use runledger_core::jobs::{JobStage, WorkflowStepStatus};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, Result};

use super::super::super::errors::validate_completion_progress;
use super::super::super::types::JobCompletionUpdate;
use super::super::super::workflows::on_terminal;
use super::common::{COMPLETE_SUCCESS_LEASE_MISMATCH_CONTEXT, rollback_and_return_lease_mismatch};

struct SuccessProgressUpdate<'a> {
    stage: JobStage,
    progress_done: Option<i64>,
    progress_total: Option<i64>,
    checkpoint: Option<&'a Value>,
    output: Option<&'a Value>,
}

#[derive(sqlx::FromRow)]
struct ExistingSuccessProgress {
    progress_done: Option<i64>,
    progress_total: Option<i64>,
}

impl SuccessProgressUpdate<'_> {
    fn coalesce_existing_progress(&mut self, existing: ExistingSuccessProgress) -> Result<()> {
        self.progress_done = self.progress_done.or(existing.progress_done);
        self.progress_total = self.progress_total.or(existing.progress_total);
        validate_completion_progress(self.progress_done, self.progress_total)
    }
}

fn success_progress_update<'a>(
    progress: Option<&'a JobCompletionUpdate<'a>>,
) -> Result<SuccessProgressUpdate<'a>> {
    let progress_done = progress.and_then(|value| value.progress_done);
    let progress_total = progress.and_then(|value| value.progress_total);
    validate_completion_progress(progress_done, progress_total)?;

    Ok(SuccessProgressUpdate {
        stage: JobStage::Completed,
        progress_done,
        progress_total,
        checkpoint: progress.and_then(|value| value.checkpoint),
        output: progress.and_then(|value| value.output),
    })
}

async fn lock_job_success_progress_tx(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &str,
) -> Result<Option<ExistingSuccessProgress>> {
    sqlx::query_as::<_, ExistingSuccessProgress>(
        "SELECT progress_done, progress_total
         FROM job_queue
         WHERE id = $1
           AND run_number = $2
           AND attempt = $3
           AND worker_id = $4
           AND status = 'LEASED'
           AND lease_expires_at IS NOT NULL
           AND lease_expires_at > clock_timestamp()
         FOR UPDATE",
    )
    .bind(job_id)
    .bind(run_number)
    .bind(attempt)
    .bind(worker_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("lock job success progress", error))
}

async fn mark_job_succeeded_tx(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &str,
    progress: &SuccessProgressUpdate<'_>,
) -> Result<u64> {
    let rows_affected = sqlx::query(
        "UPDATE job_queue
         SET status = 'SUCCEEDED',
             lease_expires_at = NULL,
             last_heartbeat_at = NULL,
             worker_id = NULL,
             finished_at = now(),
             stage = $5,
             progress_done = COALESCE($6, progress_done),
             progress_total = COALESCE($7, progress_total),
             checkpoint = COALESCE($8::jsonb, checkpoint),
             output = $9::jsonb,
             status_reason = NULL,
             last_error_code = NULL,
             last_error_message = NULL,
             updated_at = now()
         WHERE id = $1
           AND run_number = $2
           AND attempt = $3
           AND worker_id = $4
           AND status = 'LEASED'
           AND lease_expires_at IS NOT NULL
           AND job_queue.lease_expires_at > clock_timestamp()",
    )
    .bind(job_id)
    .bind(run_number)
    .bind(attempt)
    .bind(worker_id)
    .bind(progress.stage.as_db_value())
    .bind(progress.progress_done)
    .bind(progress.progress_total)
    .bind(progress.checkpoint)
    .bind(progress.output)
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("complete job success", error))?
    .rows_affected();

    Ok(rows_affected)
}

async fn mark_job_attempt_succeeded_tx(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
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
        job_id,
        run_number,
        attempt,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("complete job success attempt", error))?;

    Ok(())
}

async fn insert_job_succeeded_event_tx(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    progress: &SuccessProgressUpdate<'_>,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_events (
            job_id,
            run_number,
            attempt,
            event_type,
            stage,
            progress_done,
            progress_total,
            payload
         )
         VALUES ($1, $2, $3, 'SUCCEEDED', $4, $5, $6, '{}'::jsonb)",
        job_id,
        run_number,
        attempt,
        progress.stage.as_db_value(),
        progress.progress_done,
        progress.progress_total,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("complete job success event", error))?;

    Ok(())
}

/// Completes a leased job successfully.
///
/// Successful completion always persists `JobStage::Completed`. The optional
/// completion update carries final progress, checkpoint, and output values.
pub async fn complete_job_success(
    pool: &DbPool,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &str,
    progress: Option<&JobCompletionUpdate<'_>>,
) -> Result<()> {
    let mut progress = success_progress_update(progress)?;
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    let Some(existing_progress) =
        lock_job_success_progress_tx(&mut tx, job_id, run_number, attempt, worker_id).await?
    else {
        return rollback_and_return_lease_mismatch(tx, COMPLETE_SUCCESS_LEASE_MISMATCH_CONTEXT)
            .await;
    };
    progress.coalesce_existing_progress(existing_progress)?;

    let updated =
        mark_job_succeeded_tx(&mut tx, job_id, run_number, attempt, worker_id, &progress).await?;

    if updated == 0 {
        return rollback_and_return_lease_mismatch(tx, COMPLETE_SUCCESS_LEASE_MISMATCH_CONTEXT)
            .await;
    }

    mark_job_attempt_succeeded_tx(&mut tx, job_id, run_number, attempt).await?;
    insert_job_succeeded_event_tx(&mut tx, job_id, run_number, attempt, &progress).await?;

    on_terminal(
        &mut tx,
        job_id,
        WorkflowStepStatus::Succeeded,
        Some("SUCCEEDED"),
        None,
        None,
        progress.output,
    )
    .await?;

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    Ok(())
}
