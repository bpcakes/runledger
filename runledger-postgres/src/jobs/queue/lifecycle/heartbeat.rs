use sqlx::types::Uuid;

use crate::{DbPool, Error, Result};

use super::super::super::errors::validate_positive_lease_duration;
use super::super::super::types::JobLeaseIdentity;
use super::common::{
    HEARTBEAT_LEASE_MISMATCH_CONTEXT, cap_bounded_job_lifecycle_timeouts_tx,
    rollback_and_return_lease_mismatch,
};

pub async fn heartbeat_job(
    pool: &DbPool,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &str,
    lease_duration_seconds: i32,
) -> Result<()> {
    heartbeat_job_for_lease(
        pool,
        JobLeaseIdentity::new(job_id, run_number, attempt, worker_id),
        lease_duration_seconds,
    )
    .await
}

/// Renews an exact live job lease.
pub async fn heartbeat_job_for_lease(
    pool: &DbPool,
    identity: JobLeaseIdentity<'_>,
    lease_duration_seconds: i32,
) -> Result<()> {
    validate_positive_lease_duration(lease_duration_seconds)?;

    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    cap_bounded_job_lifecycle_timeouts_tx(&mut tx, "cap heartbeat lifecycle timeouts").await?;

    let updated = sqlx::query!(
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
         SET lease_expires_at = now() + make_interval(secs => $5::int4),
             last_heartbeat_at = now(),
             updated_at = now()
         FROM locked_job
         WHERE job_queue.id = locked_job.id
           AND job_queue.lease_expires_at > clock_timestamp()",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        identity.worker_id,
        lease_duration_seconds,
    )
    .execute(&mut *tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("heartbeat job", error))?
    .rows_affected();

    if updated == 0 {
        return rollback_and_return_lease_mismatch(tx, HEARTBEAT_LEASE_MISMATCH_CONTEXT).await;
    }

    sqlx::query!(
        "INSERT INTO job_events (
            job_id,
            run_number,
            attempt,
            event_type,
            payload
         )
         VALUES (
            $1,
            $2,
            $3,
            'HEARTBEAT',
            jsonb_build_object('worker_id', $4::text)
         )",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        identity.worker_id,
    )
    .execute(&mut *tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("heartbeat job event", error))?;

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    Ok(())
}
