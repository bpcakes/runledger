use chrono::{DateTime, TimeZone, Utc};
use runledger_core::jobs::{JobDeadLetterReason, JobRetryTiming};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, Result};

use super::super::super::errors::invalid_retry_timing_error;
use super::super::super::row_decode::parse_job_type_name;
use super::super::super::types::{
    JobFailureCompletionDisposition, JobFailureCompletionOutcome, JobFailureUpdate,
    JobLeaseIdentity,
};
use super::super::failure_transition::{
    DeadLetterSnapshot, FailureDetails, HandlerFailureTransition, ResolvedRetryTiming,
    RetryTimingSource,
};
use super::common::{COMPLETE_FAILURE_LEASE_MISMATCH_CONTEXT, rollback_and_return_lease_mismatch};

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
    details: FailureDetails<'a>,
    retry_timing: Option<JobRetryTiming>,
    policy_retry_delay_ms: Option<i32>,
    dead_letter_reason: Option<JobDeadLetterReason>,
}

async fn load_failure_lookup_row(
    tx: &mut DbTx<'_>,
    identity: JobLeaseIdentity<'_>,
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
    let dead_letter_reason = if !failure.kind.is_retryable() {
        Some(JobDeadLetterReason::FailureKindNonRetryable)
    } else if attempt >= max_attempts {
        Some(JobDeadLetterReason::AttemptsExhausted)
    } else {
        None
    };

    FailureOutcome {
        details: FailureDetails::new(failure.kind.as_db_value(), failure.code, failure.message),
        retry_timing: failure.retry_timing,
        policy_retry_delay_ms: failure.policy_retry_delay_ms,
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
    policy_retry_delay_ms: Option<i32>,
    handler_retry_timing: Option<JobRetryTiming>,
) -> Result<ResolvedRetryTiming> {
    let Some(policy_retry_delay_ms) = policy_retry_delay_ms else {
        return Err(invalid_retry_timing_error(
            "policy retry delay is required for a retryable failure".to_owned(),
        ));
    };
    if policy_retry_delay_ms <= 0 {
        return Err(invalid_retry_timing_error(format!(
            "policy retry delay must be greater than zero, got {policy_retry_delay_ms}"
        )));
    }
    let policy_next_run_at = completion_base_at
        .checked_add_signed(chrono::Duration::milliseconds(i64::from(
            policy_retry_delay_ms,
        )))
        .ok_or_else(|| {
            invalid_retry_timing_error(format!(
                "policy retry delay produces an unrepresentable timestamp: base={completion_base_at}, delay_ms={policy_retry_delay_ms}"
            ))
        })?;

    let requested_retry_not_before = match handler_retry_timing {
        None => None,
        Some(JobRetryTiming::After(delay)) => {
            if delay.is_zero() {
                None
            } else {
                let handler_delay_ms = retry_delay_milliseconds(delay)?;
                Some(
                    completion_base_at
                        .checked_add_signed(chrono::Duration::milliseconds(i64::from(
                            handler_delay_ms,
                        )))
                        .ok_or_else(|| {
                            invalid_retry_timing_error(format!(
                                "handler-selected retry delay produces an unrepresentable timestamp: base={completion_base_at}, delay={delay:?}"
                            ))
                        })?,
                )
            }
        }
        Some(JobRetryTiming::At(requested_retry_at)) => {
            match round_retry_at_to_postgres_precision(requested_retry_at) {
                Ok(requested_retry_at) => Some(requested_retry_at),
                Err(_) if requested_retry_at <= policy_next_run_at => None,
                Err(error) => return Err(error),
            }
        }
        #[allow(
            unreachable_patterns,
            reason = "reject future non-exhaustive retry timing variants until explicitly supported"
        )]
        _ => {
            return Err(invalid_retry_timing_error(
                "handler selected an unsupported retry timing variant".to_owned(),
            ));
        }
    };
    let (next_run_at, source) = requested_retry_not_before.map_or(
        (policy_next_run_at, RetryTimingSource::Policy),
        |requested_retry_not_before| {
            if requested_retry_not_before > policy_next_run_at {
                (
                    requested_retry_not_before,
                    RetryTimingSource::HandlerNotBefore,
                )
            } else {
                (policy_next_run_at, RetryTimingSource::Policy)
            }
        },
    );

    Ok(ResolvedRetryTiming {
        policy_retry_delay_ms,
        requested_retry_not_before,
        next_run_at,
        source,
    })
}

pub async fn complete_job_failure(
    pool: &DbPool,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &str,
    failure: &JobFailureUpdate<'_>,
) -> Result<()> {
    complete_job_failure_for_lease(
        pool,
        JobLeaseIdentity::new(job_id, run_number, attempt, worker_id),
        failure,
    )
    .await
}

/// Completes an exact live job lease with failure.
pub async fn complete_job_failure_for_lease(
    pool: &DbPool,
    identity: JobLeaseIdentity<'_>,
    failure: &JobFailureUpdate<'_>,
) -> Result<()> {
    complete_job_failure_with_outcome_for_lease(pool, identity, failure)
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
    complete_job_failure_with_outcome_for_lease(
        pool,
        JobLeaseIdentity::new(job_id, run_number, attempt, worker_id),
        failure,
    )
    .await
}

/// Completes an exact live job lease with failure and returns its durable outcome.
pub async fn complete_job_failure_with_outcome_for_lease(
    pool: &DbPool,
    identity: JobLeaseIdentity<'_>,
    failure: &JobFailureUpdate<'_>,
) -> Result<JobFailureCompletionOutcome> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    let Some(lookup) = load_failure_lookup_row(&mut tx, identity).await? else {
        return rollback_and_return_lease_mismatch(tx, COMPLETE_FAILURE_LEASE_MISMATCH_CONTEXT)
            .await;
    };

    let outcome = failure_outcome(identity.attempt, lookup.max_attempts, failure);
    let transition = HandlerFailureTransition::new(
        identity,
        outcome.details,
        DeadLetterSnapshot::new(
            &lookup.job_type,
            lookup.organization_id,
            &lookup.payload_snapshot,
            lookup.checkpoint_snapshot.as_ref(),
        ),
    );

    let disposition = if let Some(reason) = outcome.dead_letter_reason {
        transition.apply_terminal(&mut tx).await?;
        JobFailureCompletionDisposition::DeadLettered { reason }
    } else {
        let retry_timing = resolve_retry_timing(
            lookup.completion_base_at,
            outcome.policy_retry_delay_ms,
            outcome.retry_timing,
        )?;
        transition.apply_retry(&mut tx, retry_timing).await?;
        match (retry_timing.source, retry_timing.requested_retry_not_before) {
            (RetryTimingSource::HandlerNotBefore, Some(requested_retry_at)) => {
                JobFailureCompletionDisposition::RetryScheduledAt {
                    requested_retry_at,
                    next_run_at: retry_timing.next_run_at,
                }
            }
            _ => JobFailureCompletionDisposition::RetryScheduled {
                retry_delay_ms: retry_timing.policy_retry_delay_ms,
                next_run_at: retry_timing.next_run_at,
            },
        }
    };

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    Ok(JobFailureCompletionOutcome {
        job_id: identity.job_id,
        job_type: lookup.job_type,
        organization_id: lookup.organization_id,
        run_number: identity.run_number,
        attempt: identity.attempt,
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
    use runledger_core::jobs::JobFailureKind;

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
    fn failure_outcome_uses_core_retry_eligibility() {
        for (kind, expected_dead_letter_reason) in [
            (JobFailureKind::Retryable, None),
            (
                JobFailureKind::Terminal,
                Some(JobDeadLetterReason::FailureKindNonRetryable),
            ),
            (JobFailureKind::Timeout, None),
            (JobFailureKind::LeaseExpired, None),
            (
                JobFailureKind::Panicked,
                Some(JobDeadLetterReason::FailureKindNonRetryable),
            ),
        ] {
            let failure = JobFailureUpdate::new(kind, "job.test", "test failure", Some(1));

            assert_eq!(
                failure_outcome(1, 2, &failure).dead_letter_reason,
                expected_dead_letter_reason,
                "unexpected policy for {kind:?}"
            );
        }
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
            Some(1),
            Some(JobRetryTiming::After(
                Duration::from_millis(1) + Duration::from_nanos(1),
            )),
        )
        .expect("relative retry timing should resolve");

        assert_eq!(resolved.policy_retry_delay_ms, 1);
        assert_eq!(resolved.source, RetryTimingSource::HandlerNotBefore);
        assert_eq!(
            resolved.requested_retry_not_before,
            Some(completion_base_at + ChronoDuration::milliseconds(2))
        );
        assert_eq!(
            resolved.next_run_at,
            completion_base_at + ChronoDuration::milliseconds(2)
        );
    }

    #[test]
    fn exact_absolute_retry_time_is_preserved() {
        let completion_base_at = completion_base_at();
        let requested_retry_at = completion_base_at + ChronoDuration::microseconds(2_123_456);
        let resolved = resolve_retry_timing(
            completion_base_at,
            Some(1_000),
            Some(JobRetryTiming::At(requested_retry_at)),
        )
        .expect("absolute retry timing should resolve");

        assert_eq!(
            resolved.requested_retry_not_before,
            Some(requested_retry_at)
        );
        assert_eq!(resolved.next_run_at, requested_retry_at);
        assert_eq!(resolved.source, RetryTimingSource::HandlerNotBefore);
    }

    #[test]
    fn sub_microsecond_absolute_retry_time_rounds_up() {
        let completion_base_at = completion_base_at();
        let requested_retry_at = completion_base_at + ChronoDuration::nanoseconds(2_123_456_001);
        let expected_retry_at = completion_base_at + ChronoDuration::nanoseconds(2_123_457_000);
        let resolved = resolve_retry_timing(
            completion_base_at,
            Some(1_000),
            Some(JobRetryTiming::At(requested_retry_at)),
        )
        .expect("sub-microsecond absolute retry timing should resolve");

        assert_eq!(resolved.requested_retry_not_before, Some(expected_retry_at));
        assert_eq!(resolved.next_run_at, expected_retry_at);
    }

    #[test]
    fn policy_backoff_wins_over_past_or_equal_handler_not_before() {
        let completion_base_at = completion_base_at();

        for requested_retry_at in [
            completion_base_at - ChronoDuration::seconds(1),
            completion_base_at,
        ] {
            let resolved = resolve_retry_timing(
                completion_base_at,
                Some(1_000),
                Some(JobRetryTiming::At(requested_retry_at)),
            )
            .expect("past or equal absolute retry timing should resolve");

            assert_eq!(
                resolved.requested_retry_not_before,
                Some(requested_retry_at)
            );
            assert_eq!(
                resolved.next_run_at,
                completion_base_at + ChronoDuration::seconds(1)
            );
            assert_eq!(resolved.source, RetryTimingSource::Policy);
        }
    }

    #[test]
    fn policy_backoff_applies_without_a_handler_hint() {
        let completion_base_at = completion_base_at();
        let resolved = resolve_retry_timing(completion_base_at, Some(2_500), None)
            .expect("policy-only retry should resolve");

        assert_eq!(resolved.requested_retry_not_before, None);
        assert_eq!(resolved.source, RetryTimingSource::Policy);
        assert_eq!(
            resolved.next_run_at,
            completion_base_at + ChronoDuration::milliseconds(2_500)
        );
    }

    #[test]
    fn zero_relative_retry_hint_falls_back_to_policy() {
        let completion_base_at = completion_base_at();
        let resolved = resolve_retry_timing(
            completion_base_at,
            Some(2_500),
            Some(JobRetryTiming::After(Duration::ZERO)),
        )
        .expect("zero lower bound should fall back to policy");

        assert_eq!(resolved.requested_retry_not_before, None);
        assert_eq!(resolved.source, RetryTimingSource::Policy);
        assert_eq!(
            resolved.next_run_at,
            completion_base_at + ChronoDuration::milliseconds(2_500)
        );
    }

    #[test]
    fn retry_resolution_rejects_timestamp_overflow() {
        assert_invalid_retry_timing(resolve_retry_timing(
            DateTime::<Utc>::MAX_UTC,
            Some(1),
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
    fn absolute_retry_before_postgres_range_falls_back_to_policy() {
        let completion_base_at = completion_base_at();
        let resolved = resolve_retry_timing(
            completion_base_at,
            Some(1_000),
            Some(JobRetryTiming::At(DateTime::<Utc>::MIN_UTC)),
        )
        .expect("irrelevant past lower bound should fall back to policy");

        assert_eq!(resolved.requested_retry_not_before, None);
        assert_eq!(resolved.source, RetryTimingSource::Policy);
        assert_eq!(
            resolved.next_run_at,
            completion_base_at + ChronoDuration::seconds(1)
        );
    }
}
