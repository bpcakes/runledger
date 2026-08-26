use chrono::{DateTime, Utc};
use runledger_core::jobs::JobStage;
use sqlx::types::Uuid;

use crate::{DbPool, Error, Result};

use super::super::super::errors::{
    ensure_rejection_rollback_succeeded, invalid_continuation_delay_error,
    validate_completion_progress,
};
use super::super::super::row_decode::parse_job_type_name;
use super::super::super::types::{JobContinuationOutcome, JobContinuationUpdate, JobLeaseIdentity};
use super::super::super::workflows::on_handler_continuation;
use super::super::advance::{
    AdvanceJobToNextRun, LiveLeaseGuard, advance_live_lease_to_next_run_tx,
};
use super::super::events::{
    HANDLER_CONTINUATION_REASON, RequeuedEventPayload, RequeuedJobEvent, insert_requeued_event_tx,
};
use super::common::{
    COMPLETE_CONTINUATION_LEASE_MISMATCH_CONTEXT, cap_owned_job_lifecycle_timeouts_tx,
    coalesce_completion_progress, finish_successful_attempt_tx, lock_live_completion_lease_tx,
    rollback_and_return_lease_mismatch,
};

struct ContinuationProgressUpdate<'a> {
    progress_done: Option<i64>,
    progress_total: Option<i64>,
    checkpoint: Option<&'a serde_json::Value>,
}

fn continuation_delay_microseconds(delay: std::time::Duration) -> Result<i64> {
    let rounded_microseconds = delay.as_micros() + u128::from(delay.subsec_nanos() % 1_000 != 0);
    i64::try_from(rounded_microseconds).map_err(|_| {
        invalid_continuation_delay_error(format!(
            "continuation delay must fit in signed 64-bit microseconds, got {delay:?}"
        ))
    })
}

fn continuation_next_run_at(base_at: DateTime<Utc>, delay_us: i64) -> Result<DateTime<Utc>> {
    base_at
        .checked_add_signed(chrono::Duration::microseconds(delay_us))
        .ok_or_else(|| {
            invalid_continuation_delay_error(format!(
                "continuation from {base_at} with {delay_us} microseconds exceeds {}",
                DateTime::<Utc>::MAX_UTC
            ))
        })
}

/// Successfully closes the current attempt of a direct job and schedules the
/// same logical job for another run.
///
/// The transition applies only while the exact `(job_id, run_number, attempt,
/// worker_id)` lease is still live. Progress and checkpoint values are retained
/// across the run boundary, while the failure-attempt budget resets.
/// Workflow-managed steps that did not opt in are rejected with
/// `job.workflow_handler_continuation_not_enabled`.
pub async fn complete_job_continuation(
    pool: &DbPool,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &str,
    continuation: &JobContinuationUpdate<'_>,
) -> Result<()> {
    complete_job_continuation_for_lease(
        pool,
        JobLeaseIdentity::new(job_id, run_number, attempt, worker_id),
        continuation,
    )
    .await
}

/// Closes an exact live lease and schedules the next run of the job.
pub async fn complete_job_continuation_for_lease(
    pool: &DbPool,
    identity: JobLeaseIdentity<'_>,
    continuation: &JobContinuationUpdate<'_>,
) -> Result<()> {
    complete_job_continuation_with_outcome_for_lease(pool, identity, continuation)
        .await
        .map(|_| ())
}

pub async fn complete_job_continuation_with_outcome(
    pool: &DbPool,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &str,
    continuation: &JobContinuationUpdate<'_>,
) -> Result<JobContinuationOutcome> {
    complete_job_continuation_with_outcome_for_lease(
        pool,
        JobLeaseIdentity::new(job_id, run_number, attempt, worker_id),
        continuation,
    )
    .await
}

/// Closes an exact live lease, schedules the next run, and returns its outcome.
pub async fn complete_job_continuation_with_outcome_for_lease(
    pool: &DbPool,
    identity: JobLeaseIdentity<'_>,
    continuation: &JobContinuationUpdate<'_>,
) -> Result<JobContinuationOutcome> {
    let delay_us = continuation_delay_microseconds(continuation.delay)?;
    validate_completion_progress(continuation.progress_done, continuation.progress_total)?;
    let mut progress = ContinuationProgressUpdate {
        progress_done: continuation.progress_done,
        progress_total: continuation.progress_total,
        checkpoint: continuation.checkpoint,
    };
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    cap_owned_job_lifecycle_timeouts_tx(&mut tx, "cap job continuation lifecycle timeouts").await?;

    let Some(lookup) =
        lock_live_completion_lease_tx(&mut tx, identity, "lock job continuation").await?
    else {
        return rollback_and_return_lease_mismatch(
            tx,
            COMPLETE_CONTINUATION_LEASE_MISMATCH_CONTEXT,
        )
        .await;
    };
    if let Err(error) = on_handler_continuation(&mut tx, identity.job_id).await {
        ensure_rejection_rollback_succeeded(tx.rollback().await)?;
        return Err(error);
    }
    if let Err(validation_error) = coalesce_completion_progress(
        &mut progress.progress_done,
        &mut progress.progress_total,
        &lookup,
    ) {
        ensure_rejection_rollback_succeeded(tx.rollback().await)?;
        return Err(validation_error);
    }
    let next_run_at = match continuation_next_run_at(lookup.completion_base_at, delay_us) {
        Ok(next_run_at) => next_run_at,
        Err(validation_error) => {
            ensure_rejection_rollback_succeeded(tx.rollback().await)?;
            return Err(validation_error);
        }
    };

    let Some(next_run) = advance_live_lease_to_next_run_tx(
        &mut tx,
        &AdvanceJobToNextRun {
            job_id: identity.job_id,
            preserve_missing_resume_state: true,
            progress_done: progress.progress_done,
            progress_total: progress.progress_total,
            checkpoint: progress.checkpoint,
            next_run_at: Some(next_run_at),
            status_reason: Some(HANDLER_CONTINUATION_REASON),
        },
        LiveLeaseGuard {
            run_number: identity.run_number,
            attempt: identity.attempt,
            worker_id: identity.worker_id,
        },
        "complete job continuation",
    )
    .await?
    else {
        return rollback_and_return_lease_mismatch(
            tx,
            COMPLETE_CONTINUATION_LEASE_MISMATCH_CONTEXT,
        )
        .await;
    };

    finish_successful_attempt_tx(&mut tx, identity, "close continued job attempt").await?;
    insert_requeued_event_tx(
        &mut tx,
        RequeuedJobEvent {
            job_id: identity.job_id,
            completed_run_number: identity.run_number,
            attempt: Some(identity.attempt),
            stage: Some(JobStage::Queued.as_db_value()),
            progress_done: progress.progress_done,
            progress_total: progress.progress_total,
            payload: RequeuedEventPayload::HandlerContinuation {
                next_run_number: next_run.run_number,
                next_run_at: next_run.next_run_at,
                delay_microseconds: delay_us,
            },
        },
        "insert handler continuation event",
    )
    .await?;
    let job_type = parse_job_type_name(lookup.job_type)?;

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    Ok(JobContinuationOutcome {
        job_id: identity.job_id,
        job_type,
        organization_id: lookup.organization_id,
        completed_run_number: identity.run_number,
        next_run_number: next_run.run_number,
        attempt: identity.attempt,
        max_attempts: lookup.max_attempts,
        next_run_at: next_run.next_run_at,
        progress_done: progress.progress_done,
        progress_total: progress.progress_total,
    })
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn continuation_delay_rounds_up_to_postgres_microsecond_precision() {
        let base_at = Utc
            .with_ymd_and_hms(2026, 7, 17, 12, 0, 0)
            .single()
            .expect("valid base timestamp");

        let delay_us = continuation_delay_microseconds(std::time::Duration::from_nanos(1))
            .expect("sub-microsecond delay should round up");
        let next_run_at = continuation_next_run_at(base_at, delay_us)
            .expect("rounded delay should be schedulable");

        assert_eq!(delay_us, 1);
        assert_eq!(next_run_at, base_at + chrono::Duration::microseconds(1));
    }

    #[test]
    fn continuation_target_rejects_chrono_timestamp_overflow() {
        let error = continuation_next_run_at(DateTime::<Utc>::MAX_UTC, 1)
            .expect_err("timestamp overflow must be rejected");
        let Error::QueryError(error) = error else {
            panic!("expected invalid continuation delay query error");
        };

        assert_eq!(error.code(), "job.invalid_continuation_delay");
    }
}
