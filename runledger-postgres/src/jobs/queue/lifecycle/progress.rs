use runledger_core::jobs::JobStage;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, Result};

use super::super::super::types::JobProgressUpdate;
use super::common::{UPDATE_PROGRESS_LEASE_MISMATCH_CONTEXT, rollback_and_return_lease_mismatch};

async fn update_job_progress_row_tx(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &str,
    progress: &JobProgressUpdate<'_>,
) -> Result<u64> {
    let rows_affected = sqlx::query!(
        "WITH locked_job AS MATERIALIZED (
             SELECT id
             FROM job_queue
             WHERE id = $1
               AND run_number = $2
               AND attempt = $3
               AND worker_id = $4
               AND status = 'LEASED'
               AND lease_expires_at IS NOT NULL
             FOR UPDATE
         )
         UPDATE job_queue
         SET stage = COALESCE($5, stage),
             progress_done = COALESCE($6, progress_done),
             progress_total = COALESCE($7, progress_total),
             checkpoint = COALESCE($8::jsonb, checkpoint),
             updated_at = now()
         FROM locked_job
         WHERE job_queue.id = locked_job.id
           AND job_queue.lease_expires_at > clock_timestamp()",
        job_id,
        run_number,
        attempt,
        worker_id,
        progress.stage.map(|s| s.as_db_value()),
        progress.progress_done,
        progress.progress_total,
        progress.checkpoint,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("update job progress", error))?
    .rows_affected();

    Ok(rows_affected)
}

async fn insert_stage_changed_event_tx(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    stage: &str,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_events (
            job_id,
            run_number,
            attempt,
            event_type,
            stage,
            payload
         )
         VALUES ($1, $2, $3, 'STAGE_CHANGED', $4, '{}'::jsonb)",
        job_id,
        run_number,
        attempt,
        stage,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert stage changed event", error))?;

    Ok(())
}

async fn insert_progress_event_tx(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    progress_done: Option<i64>,
    progress_total: Option<i64>,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_events (
            job_id,
            run_number,
            attempt,
            event_type,
            progress_done,
            progress_total,
            payload
         )
         VALUES ($1, $2, $3, 'PROGRESS', $4, $5, '{}'::jsonb)",
        job_id,
        run_number,
        attempt,
        progress_done,
        progress_total,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert progress event", error))?;

    Ok(())
}

async fn mark_execution_started_persisted_tx(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
) -> Result<()> {
    sqlx::query!(
        "UPDATE job_attempts
         SET execution_started_persisted_at = now()
         WHERE job_id = $1
           AND run_number = $2
           AND attempt = $3
           AND execution_started_persisted_at IS NULL",
        job_id,
        run_number,
        attempt,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("mark execution started persisted", error)
    })?;

    Ok(())
}

pub async fn update_job_progress(
    pool: &DbPool,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &str,
    progress: &JobProgressUpdate<'_>,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    let updated =
        update_job_progress_row_tx(&mut tx, job_id, run_number, attempt, worker_id, progress)
            .await?;

    if updated == 0 {
        return rollback_and_return_lease_mismatch(tx, UPDATE_PROGRESS_LEASE_MISMATCH_CONTEXT)
            .await;
    }

    if progress.stage == Some(JobStage::Running) {
        mark_execution_started_persisted_tx(&mut tx, job_id, run_number, attempt).await?;
    }

    if let Some(stage) = progress.stage {
        insert_stage_changed_event_tx(&mut tx, job_id, run_number, attempt, stage.as_db_value())
            .await?;
    }

    if progress.progress_done.is_some() || progress.progress_total.is_some() {
        insert_progress_event_tx(
            &mut tx,
            job_id,
            run_number,
            attempt,
            progress.progress_done,
            progress.progress_total,
        )
        .await?;
    }

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    Ok(())
}
