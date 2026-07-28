use chrono::{DateTime, TimeZone, Utc};
use runledger_core::jobs::{
    JobDeadLetterReason, JobFailureKind, JobRetryTiming, WorkflowStepStatus,
};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, Result};

use super::super::super::errors::invalid_retry_timing_error;
use super::super::super::row_decode::parse_job_type_name;
use super::super::super::types::{
    JobFailureCompletionDisposition, JobFailureCompletionOutcome, JobFailureUpdate,
};
use super::super::super::workflows::{on_retry_scheduled, on_terminal};
use super::common::{
    COMPLETE_FAILURE_LEASE_MISMATCH_CONTEXT, INSERT_FAILED_EVENT_RETRY_CONTEXT,
    INSERT_FAILED_EVENT_TERMINAL_CONTEXT, rollback_and_return_lease_mismatch,
};

#[derive(Clone, Copy)]
struct FailureJobIdentity<'a> {
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &'a str,
}

struct FailureLookupRow {
    max_attempts: i32,
    payload_snapshot: Value,
    checkpoint_snapshot: Option<Value>,
    job_type: runledger_core::jobs::JobTypeName,
    organization_id: Option<Uuid>,
    completion_base_at: DateTime<Utc>,
}

#[derive(Clone, Copy)]
struct FailureOutcome<'a> {
    kind_db_value: &'a str,
    code: &'a str,
    message: &'a str,
    retry_timing: Option<JobRetryTiming>,
    dead_letter_reason: Option<JobDeadLetterReason>,
}

#[derive(Clone, Copy)]
enum ResolvedRetryTiming {
    After {
        retry_delay_ms: i32,
        next_run_at: DateTime<Utc>,
    },
    At {
        requested_retry_at: DateTime<Utc>,
        next_run_at: DateTime<Utc>,
    },
}

impl ResolvedRetryTiming {
    const fn retry_delay_ms(self) -> Option<i32> {
        match self {
            Self::After { retry_delay_ms, .. } => Some(retry_delay_ms),
            Self::At { .. } => None,
        }
    }

    const fn requested_retry_at(self) -> Option<DateTime<Utc>> {
        match self {
            Self::After { .. } => None,
            Self::At {
                requested_retry_at, ..
            } => Some(requested_retry_at),
        }
    }

    const fn next_run_at(self) -> DateTime<Utc> {
        match self {
            Self::After { next_run_at, .. } | Self::At { next_run_at, .. } => next_run_at,
        }
    }
}

async fn load_failure_lookup_row(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
) -> Result<Option<FailureLookupRow>> {
    let row = sqlx::query!(
        "WITH locked_job AS MATERIALIZED (
             SELECT max_attempts, payload, checkpoint, job_type, organization_id, lease_expires_at
             FROM job_queue
             WHERE id = $1
               AND run_number = $2
               AND attempt = $3
               AND worker_id = $4
               AND status = 'LEASED'
               AND lease_expires_at IS NOT NULL
             FOR UPDATE
         )
         SELECT
            max_attempts,
            payload,
            checkpoint,
            job_type,
            organization_id,
            clock_timestamp() AS \"completion_base_at!\"
         FROM locked_job
         WHERE lease_expires_at > clock_timestamp()",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        identity.worker_id,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("complete job failure lookup", error))?;

    row.map(|row| {
        Ok(FailureLookupRow {
            max_attempts: row.max_attempts,
            payload_snapshot: row.payload,
            checkpoint_snapshot: row.checkpoint,
            job_type: parse_job_type_name(row.job_type)?,
            organization_id: row.organization_id,
            completion_base_at: row.completion_base_at,
        })
    })
    .transpose()
}

fn failure_outcome<'a>(
    attempt: i32,
    max_attempts: i32,
    failure: &'a JobFailureUpdate<'a>,
) -> FailureOutcome<'a> {
    let dead_letter_reason = if matches!(
        failure.kind,
        JobFailureKind::Terminal | JobFailureKind::Panicked
    ) {
        Some(JobDeadLetterReason::FailureKindNonRetryable)
    } else if attempt >= max_attempts {
        Some(JobDeadLetterReason::AttemptsExhausted)
    } else {
        None
    };

    FailureOutcome {
        kind_db_value: failure.kind.as_db_value(),
        code: failure.code,
        message: failure.message,
        retry_timing: failure.retry_timing,
        dead_letter_reason,
    }
}

fn retry_delay_milliseconds(delay: std::time::Duration) -> Result<i32> {
    let rounded_milliseconds =
        delay.as_millis() + u128::from(delay.subsec_nanos() % 1_000_000 != 0);
    let retry_delay_ms = i32::try_from(rounded_milliseconds).map_err(|_| {
        invalid_retry_timing_error(format!(
            "handler-selected retry delay must fit in a positive 32-bit millisecond value, got {delay:?}"
        ))
    })?;
    if retry_delay_ms <= 0 {
        return Err(invalid_retry_timing_error(format!(
            "handler-selected retry delay must be greater than zero, got {delay:?}"
        )));
    }

    Ok(retry_delay_ms)
}

fn round_retry_at_to_postgres_precision(retry_at: DateTime<Utc>) -> Result<DateTime<Utc>> {
    let nanosecond_remainder = retry_at.timestamp_subsec_nanos() % 1_000;
    let retry_at = if nanosecond_remainder == 0 {
        retry_at
    } else {
        retry_at.checked_add_signed(chrono::Duration::nanoseconds(i64::from(
            1_000 - nanosecond_remainder,
        )))
        .ok_or_else(|| {
            invalid_retry_timing_error(format!(
                "handler-selected absolute retry time cannot be rounded to PostgreSQL microsecond precision: {retry_at}"
            ))
        })?
    };
    let postgres_minimum = Utc
        .with_ymd_and_hms(-4712, 1, 1, 0, 0, 0)
        .single()
        .expect("PostgreSQL's minimum timestamp must be representable by chrono");
    if retry_at < postgres_minimum {
        return Err(invalid_retry_timing_error(format!(
            "handler-selected absolute retry time is outside PostgreSQL's supported range: {retry_at}"
        )));
    }

    Ok(retry_at)
}

fn resolve_retry_timing(
    completion_base_at: DateTime<Utc>,
    retry_timing: Option<JobRetryTiming>,
) -> Result<ResolvedRetryTiming> {
    let Some(retry_timing) = retry_timing else {
        return Err(invalid_retry_timing_error(
            "retry timing is required for a retryable failure".to_owned(),
        ));
    };

    match retry_timing {
        JobRetryTiming::After(delay) => {
            let retry_delay_ms = retry_delay_milliseconds(delay)?;
            let next_run_at = completion_base_at
                .checked_add_signed(chrono::Duration::milliseconds(i64::from(retry_delay_ms)))
                .ok_or_else(|| {
                    invalid_retry_timing_error(format!(
                        "handler-selected retry delay produces an unrepresentable timestamp: base={completion_base_at}, delay={delay:?}"
                    ))
                })?;
            Ok(ResolvedRetryTiming::After {
                retry_delay_ms,
                next_run_at,
            })
        }
        JobRetryTiming::At(requested_retry_at) => {
            let requested_retry_at = round_retry_at_to_postgres_precision(requested_retry_at)?;
            Ok(ResolvedRetryTiming::At {
                requested_retry_at,
                next_run_at: requested_retry_at.max(completion_base_at),
            })
        }
        #[allow(unreachable_patterns)]
        _ => Err(invalid_retry_timing_error(
            "handler selected an unsupported retry timing variant".to_owned(),
        )),
    }
}

async fn apply_terminal_failure(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    lookup: &FailureLookupRow,
    outcome: FailureOutcome<'_>,
) -> Result<()> {
    mark_dead_lettered_job_queue_for_failure_tx(tx, identity, outcome).await?;
    upsert_job_dead_letter_for_failure_tx(tx, identity, lookup, outcome).await?;
    update_failed_attempt_terminal_tx(tx, identity, outcome).await?;
    insert_failed_event_for_failure_tx(tx, identity, outcome, INSERT_FAILED_EVENT_TERMINAL_CONTEXT)
        .await?;
    insert_dead_lettered_event_for_failure_tx(tx, identity, outcome).await?;

    on_terminal(
        tx,
        identity.job_id,
        WorkflowStepStatus::Failed,
        Some(outcome.kind_db_value),
        Some(outcome.code),
        Some(outcome.message),
        None,
    )
    .await?;

    Ok(())
}

async fn mark_dead_lettered_job_queue_for_failure_tx(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    outcome: FailureOutcome<'_>,
) -> Result<()> {
    sqlx::query!(
        "UPDATE job_queue
         SET status = 'DEAD_LETTERED',
             lease_expires_at = NULL,
             last_heartbeat_at = NULL,
             worker_id = NULL,
             finished_at = now(),
             output = NULL,
             status_reason = $5,
             last_error_code = $6,
             last_error_message = $7,
             updated_at = now()
         WHERE id = $1
           AND run_number = $2
           AND attempt = $3
           AND worker_id = $4
           AND status = 'LEASED'",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        identity.worker_id,
        outcome.kind_db_value,
        outcome.code,
        outcome.message,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("mark dead letter", error))?;

    Ok(())
}

async fn upsert_job_dead_letter_for_failure_tx(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    lookup: &FailureLookupRow,
    outcome: FailureOutcome<'_>,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_dead_letters (
            job_id,
            job_type,
            organization_id,
            run_number,
            attempt,
            error_code,
            error_message,
            payload_snapshot,
            checkpoint_snapshot,
            failed_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, now())
             ON CONFLICT (job_id)
             DO UPDATE
                SET run_number = EXCLUDED.run_number,
                    attempt = EXCLUDED.attempt,
                    error_code = EXCLUDED.error_code,
                    error_message = EXCLUDED.error_message,
                    payload_snapshot = EXCLUDED.payload_snapshot,
                    checkpoint_snapshot = EXCLUDED.checkpoint_snapshot,
                    failed_at = EXCLUDED.failed_at",
        identity.job_id,
        lookup.job_type.as_str(),
        lookup.organization_id,
        identity.run_number,
        identity.attempt,
        outcome.code,
        outcome.message,
        lookup.payload_snapshot,
        lookup.checkpoint_snapshot,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert dead letter", error))?;

    Ok(())
}

async fn update_failed_attempt_terminal_tx(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    outcome: FailureOutcome<'_>,
) -> Result<()> {
    sqlx::query!(
        "UPDATE job_attempts
         SET finished_at = now(),
             outcome = $4::text::job_failure_kind,
             error_code = $5,
             error_message = $6
         WHERE job_id = $1
           AND run_number = $2
           AND attempt = $3",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        outcome.kind_db_value,
        outcome.code,
        outcome.message,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("update failed attempt terminal", error)
    })?;

    Ok(())
}

async fn insert_failed_event_for_failure_tx(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    outcome: FailureOutcome<'_>,
    error_context: &'static str,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_events (job_id, run_number, attempt, event_type, payload)
         VALUES (
            $1,
            $2,
            $3,
            'FAILED',
            jsonb_build_object('kind', $4::text, 'error_code', $5::text, 'error_message', $6::text)
         )",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        outcome.kind_db_value,
        outcome.code,
        outcome.message,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(error_context, error))?;

    Ok(())
}

async fn insert_dead_lettered_event_for_failure_tx(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    outcome: FailureOutcome<'_>,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_events (job_id, run_number, attempt, event_type, payload)
         VALUES (
            $1,
            $2,
            $3,
            'DEAD_LETTERED',
            jsonb_build_object('kind', $4::text, 'error_code', $5::text)
         )",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        outcome.kind_db_value,
        outcome.code,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert dead lettered event", error))?;

    Ok(())
}

async fn apply_retryable_failure(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    outcome: FailureOutcome<'_>,
    retry_timing: ResolvedRetryTiming,
) -> Result<()> {
    mark_retryable_job_queue_for_failure_tx(tx, identity, outcome, retry_timing.next_run_at())
        .await?;
    update_failed_attempt_retryable_tx(tx, identity, outcome, retry_timing.retry_delay_ms())
        .await?;
    insert_failed_event_for_failure_tx(tx, identity, outcome, INSERT_FAILED_EVENT_RETRY_CONTEXT)
        .await?;
    insert_retry_scheduled_event_for_failure_tx(tx, identity, retry_timing).await?;
    on_retry_scheduled(
        tx,
        identity.job_id,
        Some(outcome.kind_db_value),
        Some(outcome.code),
        Some(outcome.message),
    )
    .await?;

    Ok(())
}

async fn mark_retryable_job_queue_for_failure_tx(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    outcome: FailureOutcome<'_>,
    next_run_at: DateTime<Utc>,
) -> Result<DateTime<Utc>> {
    sqlx::query_scalar!(
        "UPDATE job_queue
         SET status = 'PENDING',
             lease_expires_at = NULL,
             last_heartbeat_at = NULL,
             worker_id = NULL,
             next_run_at = $5,
             output = NULL,
             status_reason = $6,
             last_error_code = $7,
             last_error_message = $8,
             updated_at = now()
         WHERE id = $1
           AND run_number = $2
           AND attempt = $3
           AND worker_id = $4
           AND status = 'LEASED'
         RETURNING next_run_at",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        identity.worker_id,
        next_run_at,
        outcome.kind_db_value,
        outcome.code,
        outcome.message,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("mark retryable failure", error))
}

async fn update_failed_attempt_retryable_tx(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    outcome: FailureOutcome<'_>,
    retry_delay_ms: Option<i32>,
) -> Result<()> {
    sqlx::query!(
        "UPDATE job_attempts
         SET finished_at = now(),
             outcome = $4::text::job_failure_kind,
             error_code = $5,
             error_message = $6,
             retry_delay_ms = $7
         WHERE job_id = $1
           AND run_number = $2
           AND attempt = $3",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        outcome.kind_db_value,
        outcome.code,
        outcome.message,
        retry_delay_ms,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("update failed attempt retry", error))?;

    Ok(())
}

async fn insert_retry_scheduled_event_for_failure_tx(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    retry_timing: ResolvedRetryTiming,
) -> Result<()> {
    let retry_delay_ms = retry_timing.retry_delay_ms();
    let requested_retry_at = retry_timing.requested_retry_at();
    let next_run_at = retry_timing.next_run_at();
    sqlx::query!(
        "INSERT INTO job_events (job_id, run_number, attempt, event_type, payload)
         VALUES (
            $1,
            $2,
            $3,
            'RETRY_SCHEDULED',
            jsonb_strip_nulls(
                jsonb_build_object(
                    'retry_delay_ms', $4::int4,
                    'requested_retry_at', $5::timestamptz,
                    'next_run_at', $6::timestamptz
                )
            )
         )",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        retry_delay_ms,
        requested_retry_at,
        next_run_at,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert retry scheduled event", error))?;

    Ok(())
}

pub async fn complete_job_failure(
    pool: &DbPool,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &str,
    failure: &JobFailureUpdate<'_>,
) -> Result<()> {
    complete_job_failure_with_outcome(pool, job_id, run_number, attempt, worker_id, failure)
        .await
        .map(|_| ())
}

pub async fn complete_job_failure_with_outcome(
    pool: &DbPool,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &str,
    failure: &JobFailureUpdate<'_>,
) -> Result<JobFailureCompletionOutcome> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    let identity = FailureJobIdentity {
        job_id,
        run_number,
        attempt,
        worker_id,
    };

    let Some(lookup) = load_failure_lookup_row(&mut tx, identity).await? else {
        return rollback_and_return_lease_mismatch(tx, COMPLETE_FAILURE_LEASE_MISMATCH_CONTEXT)
            .await;
    };

    let outcome = failure_outcome(attempt, lookup.max_attempts, failure);

    let disposition = if let Some(reason) = outcome.dead_letter_reason {
        apply_terminal_failure(&mut tx, identity, &lookup, outcome).await?;
        JobFailureCompletionDisposition::DeadLettered { reason }
    } else {
        let retry_timing = resolve_retry_timing(lookup.completion_base_at, outcome.retry_timing)?;
        apply_retryable_failure(&mut tx, identity, outcome, retry_timing).await?;
        match retry_timing {
            ResolvedRetryTiming::After {
                retry_delay_ms,
                next_run_at,
            } => JobFailureCompletionDisposition::RetryScheduled {
                retry_delay_ms,
                next_run_at,
            },
            ResolvedRetryTiming::At {
                requested_retry_at,
                next_run_at,
            } => JobFailureCompletionDisposition::RetryScheduledAt {
                requested_retry_at,
                next_run_at,
            },
        }
    };

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    Ok(JobFailureCompletionOutcome {
        job_id,
        job_type: lookup.job_type,
        organization_id: lookup.organization_id,
        run_number,
        attempt,
        max_attempts: lookup.max_attempts,
        failure_kind: failure.kind,
        failure_code: failure.code.to_owned(),
        failure_message: failure.message.to_owned(),
        checkpoint: lookup.checkpoint_snapshot,
        disposition,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::{Duration as ChronoDuration, TimeZone};

    use super::*;

    fn completion_base_at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 28, 12, 0, 0)
            .single()
            .expect("valid completion base timestamp")
    }

    fn assert_invalid_retry_timing<T>(result: Result<T>) {
        let Err(error) = result else {
            panic!("expected invalid retry timing error");
        };
        let Error::QueryError(error) = error else {
            panic!("expected query error");
        };
        assert_eq!(error.code(), "job.invalid_retry_timing");
    }

    #[test]
    fn retry_delay_rounds_up_to_positive_milliseconds() {
        let max_milliseconds = u64::try_from(i32::MAX).expect("positive i32 maximum fits in u64");

        for (delay, expected_milliseconds) in [
            (Duration::from_nanos(1), 1),
            (Duration::from_millis(1), 1),
            (Duration::from_millis(1) + Duration::from_nanos(1), 2),
            (Duration::from_millis(max_milliseconds), i32::MAX),
        ] {
            assert_eq!(
                retry_delay_milliseconds(delay).expect("delay should be representable"),
                expected_milliseconds,
                "unexpected rounded value for {delay:?}"
            );
        }
    }

    #[test]
    fn retry_delay_rejects_zero_and_rounded_i32_overflow() {
        let max_milliseconds = u64::try_from(i32::MAX).expect("positive i32 maximum fits in u64");

        assert_invalid_retry_timing(retry_delay_milliseconds(Duration::ZERO));
        assert_invalid_retry_timing(retry_delay_milliseconds(
            Duration::from_millis(max_milliseconds) + Duration::from_nanos(1),
        ));
    }

    #[test]
    fn relative_retry_resolution_uses_the_database_completion_base() {
        let completion_base_at = completion_base_at();
        let resolved = resolve_retry_timing(
            completion_base_at,
            Some(JobRetryTiming::After(
                Duration::from_millis(1) + Duration::from_nanos(1),
            )),
        )
        .expect("relative retry timing should resolve");

        let ResolvedRetryTiming::After {
            retry_delay_ms,
            next_run_at,
        } = resolved
        else {
            panic!("expected relative retry timing");
        };
        assert_eq!(retry_delay_ms, 2);
        assert_eq!(
            next_run_at,
            completion_base_at + ChronoDuration::milliseconds(2)
        );
    }

    #[test]
    fn exact_absolute_retry_time_is_preserved() {
        let completion_base_at = completion_base_at();
        let requested_retry_at = completion_base_at + ChronoDuration::microseconds(2_123_456);
        let resolved = resolve_retry_timing(
            completion_base_at,
            Some(JobRetryTiming::At(requested_retry_at)),
        )
        .expect("absolute retry timing should resolve");

        let ResolvedRetryTiming::At {
            requested_retry_at: resolved_request,
            next_run_at,
        } = resolved
        else {
            panic!("expected absolute retry timing");
        };
        assert_eq!(resolved_request, requested_retry_at);
        assert_eq!(next_run_at, requested_retry_at);
    }

    #[test]
    fn sub_microsecond_absolute_retry_time_rounds_up() {
        let completion_base_at = completion_base_at();
        let requested_retry_at = completion_base_at + ChronoDuration::nanoseconds(2_123_456_001);
        let expected_retry_at = completion_base_at + ChronoDuration::nanoseconds(2_123_457_000);
        let resolved = resolve_retry_timing(
            completion_base_at,
            Some(JobRetryTiming::At(requested_retry_at)),
        )
        .expect("sub-microsecond absolute retry timing should resolve");

        let ResolvedRetryTiming::At {
            requested_retry_at,
            next_run_at,
        } = resolved
        else {
            panic!("expected absolute retry timing");
        };
        assert_eq!(requested_retry_at, expected_retry_at);
        assert_eq!(next_run_at, expected_retry_at);
    }

    #[test]
    fn past_and_equal_absolute_retry_times_are_immediately_eligible() {
        let completion_base_at = completion_base_at();

        for requested_retry_at in [
            completion_base_at - ChronoDuration::seconds(1),
            completion_base_at,
        ] {
            let resolved = resolve_retry_timing(
                completion_base_at,
                Some(JobRetryTiming::At(requested_retry_at)),
            )
            .expect("past or equal absolute retry timing should resolve");

            let ResolvedRetryTiming::At {
                requested_retry_at: resolved_request,
                next_run_at,
            } = resolved
            else {
                panic!("expected absolute retry timing");
            };
            assert_eq!(resolved_request, requested_retry_at);
            assert_eq!(next_run_at, completion_base_at);
        }
    }

    #[test]
    fn retry_resolution_rejects_timestamp_overflow() {
        assert_invalid_retry_timing(resolve_retry_timing(
            DateTime::<Utc>::MAX_UTC,
            Some(JobRetryTiming::After(Duration::from_millis(1))),
        ));
    }

    #[test]
    fn absolute_retry_rounding_rejects_max_timestamp_overflow() {
        let maximum = DateTime::<Utc>::MAX_UTC;
        assert_ne!(
            maximum.timestamp_subsec_nanos() % 1_000,
            0,
            "chrono maximum should require sub-microsecond rounding"
        );
        assert_invalid_retry_timing(round_retry_at_to_postgres_precision(maximum));
    }

    #[test]
    fn absolute_retry_resolution_rejects_timestamp_before_postgres_range() {
        assert_invalid_retry_timing(resolve_retry_timing(
            completion_base_at(),
            Some(JobRetryTiming::At(DateTime::<Utc>::MIN_UTC)),
        ));
    }
}
