use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

use super::super::rows::JobQueueRow;
use super::super::types::JobQueueRecord;

const ADVANCE_JOB_TO_NEXT_RUN_SQL: &str = "UPDATE job_queue
     SET status = 'PENDING',
         stage = 'queued',
         progress_done = CASE
             WHEN $2::bool THEN COALESCE($3::bigint, progress_done)
             ELSE $3::bigint
         END,
         progress_total = CASE
             WHEN $2::bool THEN COALESCE($4::bigint, progress_total)
             ELSE $4::bigint
         END,
         checkpoint = CASE
             WHEN $2::bool THEN COALESCE($5::jsonb, checkpoint)
             ELSE $5::jsonb
         END,
         output = NULL,
         run_number = run_number + 1,
         attempt = 0,
         lease_expires_at = NULL,
         last_heartbeat_at = NULL,
         worker_id = NULL,
         next_run_at = COALESCE($6::timestamptz, now()),
         started_at = NULL,
         finished_at = NULL,
         status_reason = COALESCE($7::text, 'REQUEUED'),
         last_error_code = NULL,
         last_error_message = NULL,
         updated_at = now()
     WHERE id = $1";

const LIVE_LEASE_GUARD_SQL: &str = " AND run_number = $8
      AND attempt = $9
      AND worker_id = $10
      AND status = 'LEASED'
      AND lease_expires_at IS NOT NULL
      AND lease_expires_at > clock_timestamp()";

pub(crate) const JOB_QUEUE_COLUMNS_SQL: &str = "id,
        job_type,
        organization_id,
        payload,
        status::text AS status,
        priority,
        run_number,
        attempt,
        max_attempts,
        timeout_seconds,
        next_run_at,
        lease_expires_at,
        last_heartbeat_at,
        worker_id,
        started_at,
        finished_at,
        stage,
        progress_done,
        progress_total,
        progress_pct::float8 AS progress_pct,
        checkpoint,
        output,
        idempotency_key,
        status_reason,
        last_error_code,
        last_error_message,
        created_at,
        updated_at";

pub(crate) struct AdvanceJobToNextRun<'a> {
    pub(crate) job_id: Uuid,
    /// Preserve the stored value whenever the matching update field is `None`.
    pub(crate) preserve_missing_resume_state: bool,
    pub(crate) progress_done: Option<i64>,
    pub(crate) progress_total: Option<i64>,
    pub(crate) checkpoint: Option<&'a Value>,
    /// `None` uses the database transaction clock.
    pub(crate) next_run_at: Option<DateTime<Utc>>,
    pub(crate) status_reason: Option<&'a str>,
}

pub(crate) struct LiveLeaseGuard<'a> {
    pub(crate) run_number: i32,
    pub(crate) attempt: i32,
    pub(crate) worker_id: &'a str,
}

macro_rules! bound_advance_query {
    ($sql:expr, $update:expr) => {
        sqlx::query_as::<_, JobQueueRow>($sql)
            .bind($update.job_id)
            .bind($update.preserve_missing_resume_state)
            .bind($update.progress_done)
            .bind($update.progress_total)
            .bind($update.checkpoint)
            .bind($update.next_run_at)
            .bind($update.status_reason)
    };
}

/// Advances a row already locked by the caller to its next pending run.
pub(crate) async fn advance_locked_job_to_next_run_tx(
    tx: &mut DbTx<'_>,
    update: &AdvanceJobToNextRun<'_>,
    error_context: &'static str,
) -> Result<JobQueueRecord> {
    // The caller's row lock guarantees this primary-key update still has one
    // target. `fetch_one` keeps that invariant explicit instead of carrying an
    // impossible no-row branch through every admin caller.
    let sql = format!("{ADVANCE_JOB_TO_NEXT_RUN_SQL} RETURNING {JOB_QUEUE_COLUMNS_SQL}");
    let row = bound_advance_query!(&sql, update)
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| Error::from_query_sqlx_with_context(error_context, error))?;

    row.into_record()
}

/// Advances only while the exact live lease still owns the locked row.
///
/// The second lease predicate is intentional: time can pass after the initial
/// `FOR UPDATE`, so lifecycle completion must re-check expiry at mutation time.
pub(crate) async fn advance_live_lease_to_next_run_tx(
    tx: &mut DbTx<'_>,
    update: &AdvanceJobToNextRun<'_>,
    guard: LiveLeaseGuard<'_>,
    error_context: &'static str,
) -> Result<Option<JobQueueRecord>> {
    // The suffix is a fixed internal SQL fragment; request data is bound below
    // and can never alter query structure.
    let sql = format!(
        "{ADVANCE_JOB_TO_NEXT_RUN_SQL}{LIVE_LEASE_GUARD_SQL} RETURNING {JOB_QUEUE_COLUMNS_SQL}"
    );
    let row = bound_advance_query!(&sql, update)
        .bind(guard.run_number)
        .bind(guard.attempt)
        .bind(guard.worker_id)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| Error::from_query_sqlx_with_context(error_context, error))?;

    row.map(JobQueueRow::into_record).transpose()
}
