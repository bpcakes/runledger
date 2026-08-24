use chrono::{DateTime, Utc};
use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

use super::super::super::errors::{lease_owner_mismatch_error, validate_completion_progress};
use super::super::super::types::JobLeaseIdentity;

pub(super) const HEARTBEAT_LEASE_MISMATCH_CONTEXT: &str =
    "heartbeat job transaction lease mismatch";
pub(super) const UPDATE_PROGRESS_LEASE_MISMATCH_CONTEXT: &str =
    "update job progress transaction lease mismatch";
pub(super) const COMPLETE_SUCCESS_LEASE_MISMATCH_CONTEXT: &str =
    "complete job success transaction lease mismatch";
pub(super) const COMPLETE_CONTINUATION_LEASE_MISMATCH_CONTEXT: &str =
    "complete job continuation transaction lease mismatch";
pub(super) const COMPLETE_FAILURE_LEASE_MISMATCH_CONTEXT: &str =
    "complete job failure transaction missing leased row";

pub(super) struct CompletionLeaseRow {
    pub(super) job_type: String,
    pub(super) organization_id: Option<Uuid>,
    pub(super) max_attempts: i32,
    pub(super) progress_done: Option<i64>,
    pub(super) progress_total: Option<i64>,
    pub(super) completion_base_at: DateTime<Utc>,
}

pub(super) async fn lock_live_completion_lease_tx(
    tx: &mut DbTx<'_>,
    identity: JobLeaseIdentity<'_>,
    error_context: &'static str,
) -> Result<Option<CompletionLeaseRow>> {
    sqlx::query_as!(
        CompletionLeaseRow,
        r#"SELECT
            job_type AS "job_type!",
            organization_id,
            max_attempts,
            progress_done,
            progress_total,
            clock_timestamp() AS "completion_base_at!"
         FROM job_queue
         WHERE id = $1
           AND run_number = $2
           AND attempt = $3
           AND worker_id = $4
           AND status = 'LEASED'
           AND lease_expires_at IS NOT NULL
           AND lease_expires_at > clock_timestamp()
         FOR UPDATE"#,
        identity.job_id,
        identity.run_number,
        identity.attempt,
        identity.worker_id,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(error_context, error))
}

pub(super) fn coalesce_completion_progress(
    progress_done: &mut Option<i64>,
    progress_total: &mut Option<i64>,
    existing: &CompletionLeaseRow,
) -> Result<()> {
    *progress_done = progress_done.or(existing.progress_done);
    *progress_total = progress_total.or(existing.progress_total);
    validate_completion_progress(*progress_done, *progress_total)
}

pub(super) async fn finish_successful_attempt_tx(
    tx: &mut DbTx<'_>,
    identity: JobLeaseIdentity<'_>,
    error_context: &'static str,
) -> Result<()> {
    sqlx::query!(
        "UPDATE job_attempts
         SET finished_at = now(),
             outcome = NULL,
             error_code = NULL,
             error_message = NULL,
             retry_delay_ms = NULL
         WHERE job_id = $1
           AND run_number = $2
           AND attempt = $3",
        identity.job_id,
        identity.run_number,
        identity.attempt,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(error_context, error))?;

    Ok(())
}

pub(super) async fn rollback_and_return_lease_mismatch<T>(
    tx: DbTx<'_>,
    context: &'static str,
) -> Result<T> {
    if let Err(error) = tx.rollback().await {
        tracing::warn!(
            error = %error,
            lease_mismatch_context = context,
            "failed to rollback transaction due to lease mismatch"
        );
    }
    Err(lease_owner_mismatch_error())
}
