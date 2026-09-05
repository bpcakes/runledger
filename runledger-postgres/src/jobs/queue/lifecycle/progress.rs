use runledger_core::jobs::{JobStage, validate_job_progress};
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, QueryError, Result};

#[allow(
    deprecated,
    reason = "the legacy stage-bearing API delegates through the shared transaction"
)]
use super::super::super::types::JobProgressUpdate;
use super::super::super::types::{JobLeaseIdentity, JobOrdinaryProgressUpdate, JobRunningUpdate};
use super::common::{
    UPDATE_PROGRESS_LEASE_MISMATCH_CONTEXT, cap_bounded_job_lifecycle_timeouts_tx,
    lock_live_job_lease_tx, rollback_and_return_lease_mismatch,
};

#[derive(Clone, Copy)]
struct ProgressMutation<'a> {
    stage: Option<JobStage>,
    progress_done: Option<i64>,
    progress_total: Option<i64>,
    checkpoint: Option<&'a serde_json::Value>,
}

impl<'a> ProgressMutation<'a> {
    const fn running(update: &JobRunningUpdate<'a>) -> Self {
        Self {
            stage: Some(JobStage::Running),
            progress_done: update.progress_done,
            progress_total: update.progress_total,
            checkpoint: update.checkpoint,
        }
    }

    const fn ordinary(update: &JobOrdinaryProgressUpdate<'a>) -> Self {
        Self {
            stage: None,
            progress_done: update.progress_done,
            progress_total: update.progress_total,
            checkpoint: update.checkpoint,
        }
    }

    #[allow(
        deprecated,
        reason = "the legacy stage-bearing API delegates through the shared transaction"
    )]
    const fn with_stage(update: &JobProgressUpdate<'a>) -> Self {
        Self {
            stage: update.stage,
            progress_done: update.progress_done,
            progress_total: update.progress_total,
            checkpoint: update.checkpoint,
        }
    }
}

async fn update_job_progress_row_tx(
    tx: &mut DbTx<'_>,
    identity: JobLeaseIdentity<'_>,
    progress: ProgressMutation<'_>,
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
        identity.job_id,
        identity.run_number,
        identity.attempt,
        identity.worker_id,
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
    identity: JobLeaseIdentity<'_>,
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
        identity.job_id,
        identity.run_number,
        identity.attempt,
        stage,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert stage changed event", error))?;

    Ok(())
}

async fn insert_progress_event_tx(
    tx: &mut DbTx<'_>,
    identity: JobLeaseIdentity<'_>,
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
        identity.job_id,
        identity.run_number,
        identity.attempt,
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
    identity: JobLeaseIdentity<'_>,
) -> Result<()> {
    sqlx::query!(
        "UPDATE job_attempts
         SET execution_started_persisted_at = now()
         WHERE job_id = $1
           AND run_number = $2
           AND attempt = $3
           AND execution_started_persisted_at IS NULL",
        identity.job_id,
        identity.run_number,
        identity.attempt,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("mark execution started persisted", error)
    })?;

    Ok(())
}

async fn persist_progress_mutation_for_lease(
    pool: &DbPool,
    identity: JobLeaseIdentity<'_>,
    progress: ProgressMutation<'_>,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    cap_bounded_job_lifecycle_timeouts_tx(&mut tx, "cap progress lifecycle timeouts").await?;

    // Partial updates can only be validated against the current locked values,
    // never the handler's invocation snapshot. Keep the original update for
    // persistence and audit events so omitted fields retain their semantics.
    let Some(existing) =
        lock_live_job_lease_tx(&mut tx, identity, "lock job progress lease").await?
    else {
        return rollback_and_return_lease_mismatch(tx, UPDATE_PROGRESS_LEASE_MISMATCH_CONTEXT)
            .await;
    };
    if let Err(error) = validate_job_progress(
        progress.progress_done.or(existing.progress_done),
        progress.progress_total.or(existing.progress_total),
    ) {
        super::super::super::errors::ensure_rejection_rollback_succeeded(tx.rollback().await)?;
        return Err(Error::QueryError(QueryError::from_invalid_progress(error)));
    }

    let updated = update_job_progress_row_tx(&mut tx, identity, progress).await?;

    if updated == 0 {
        return rollback_and_return_lease_mismatch(tx, UPDATE_PROGRESS_LEASE_MISMATCH_CONTEXT)
            .await;
    }

    if progress.stage == Some(JobStage::Running) {
        mark_execution_started_persisted_tx(&mut tx, identity).await?;
    }

    if let Some(stage) = progress.stage {
        insert_stage_changed_event_tx(&mut tx, identity, stage.as_db_value()).await?;
    }

    if progress.progress_done.is_some() || progress.progress_total.is_some() {
        insert_progress_event_tx(
            &mut tx,
            identity,
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

/// Marks a positional live lease as running and atomically persists its
/// checkpoint and progress.
///
/// Prefer [`mark_job_running_for_lease`] in custom runtimes that already hold
/// a [`JobLeaseIdentity`].
pub async fn mark_job_running(
    pool: &DbPool,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &str,
    update: &JobRunningUpdate<'_>,
) -> Result<()> {
    mark_job_running_for_lease(
        pool,
        JobLeaseIdentity::new(job_id, run_number, attempt, worker_id),
        update,
    )
    .await
}

/// Marks an exact live lease as running and atomically persists its checkpoint
/// and progress.
///
/// This is intentionally an update-taking transition rather than a zero-input
/// `mark_running` operation: a caller must commit the initial durable resume
/// state in the same transaction as `RUNNING`.
pub async fn mark_job_running_for_lease(
    pool: &DbPool,
    identity: JobLeaseIdentity<'_>,
    update: &JobRunningUpdate<'_>,
) -> Result<()> {
    persist_progress_mutation_for_lease(pool, identity, ProgressMutation::running(update)).await
}

/// Updates ordinary progress for a positional live lease without changing its
/// stage.
///
/// Prefer [`update_job_ordinary_progress_for_lease`] in custom runtimes that
/// already hold a [`JobLeaseIdentity`].
pub async fn update_job_ordinary_progress(
    pool: &DbPool,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &str,
    update: &JobOrdinaryProgressUpdate<'_>,
) -> Result<()> {
    update_job_ordinary_progress_for_lease(
        pool,
        JobLeaseIdentity::new(job_id, run_number, attempt, worker_id),
        update,
    )
    .await
}

/// Updates ordinary progress for an exact live job lease without changing its
/// stage.
pub async fn update_job_ordinary_progress_for_lease(
    pool: &DbPool,
    identity: JobLeaseIdentity<'_>,
    update: &JobOrdinaryProgressUpdate<'_>,
) -> Result<()> {
    persist_progress_mutation_for_lease(pool, identity, ProgressMutation::ordinary(update)).await
}

/// Deprecated compatibility entrypoint for callers whose progress input still
/// carries a stage.
///
/// New callers must use [`mark_job_running`] for a `RUNNING` transition and
/// [`update_job_ordinary_progress`] for ordinary progress. This entrypoint
/// preserves the historical arbitrary-stage behavior until downstream callers
/// have migrated.
#[deprecated(
    since = "0.11.0",
    note = "use mark_job_running for RUNNING, or update_job_ordinary_progress for ordinary progress"
)]
#[allow(
    deprecated,
    reason = "the compatibility function accepts the deprecated input intentionally"
)]
pub async fn update_job_progress(
    pool: &DbPool,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &str,
    update: &JobProgressUpdate<'_>,
) -> Result<()> {
    update_job_progress_for_lease(
        pool,
        JobLeaseIdentity::new(job_id, run_number, attempt, worker_id),
        update,
    )
    .await
}

/// Deprecated compatibility entrypoint for an exact live lease whose progress
/// input still carries a stage.
#[deprecated(
    since = "0.11.0",
    note = "use mark_job_running_for_lease for RUNNING, or update_job_ordinary_progress_for_lease for ordinary progress"
)]
#[allow(
    deprecated,
    reason = "the compatibility function accepts the deprecated input intentionally"
)]
pub async fn update_job_progress_for_lease(
    pool: &DbPool,
    identity: JobLeaseIdentity<'_>,
    update: &JobProgressUpdate<'_>,
) -> Result<()> {
    persist_progress_mutation_for_lease(pool, identity, ProgressMutation::with_stage(update)).await
}
