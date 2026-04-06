use sqlx::types::Uuid;

use crate::{DbPool, Error, Result};

use super::common::{HEARTBEAT_LEASE_MISMATCH_CONTEXT, rollback_and_return_lease_mismatch};

pub async fn heartbeat_job(
    pool: &DbPool,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &str,
    lease_duration_seconds: i32,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    let updated = sqlx::query!(
        "UPDATE job_queue
         SET lease_expires_at = now() + make_interval(secs => $5::int4),
             last_heartbeat_at = now(),
             updated_at = now()
         WHERE id = $1
           AND run_number = $2
           AND attempt = $3
           AND worker_id = $4
           AND status = 'LEASED'",
        job_id,
        run_number,
        attempt,
        worker_id,
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
        job_id,
        run_number,
        attempt,
        worker_id,
    )
    .execute(&mut *tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("heartbeat job event", error))?;

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    Ok(())
}
