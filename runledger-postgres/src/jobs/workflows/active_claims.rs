use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

pub(crate) async fn release_or_defer_workflow_active_claim_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
) -> Result<()> {
    // Lock only runs that actually own an active claim. Keeping the lock and
    // quiescence check as separate statements gives the check a fresh READ
    // COMMITTED snapshot even when acquiring the claim lock had to wait.
    let claim_run_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT workflow_run_id
         FROM workflow_active_claims
         WHERE workflow_run_id = $1
         FOR UPDATE",
    )
    .bind(workflow_run_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lock workflow active claim for release", error)
    })?;
    let Some(claim_run_id) = claim_run_id else {
        return Ok(());
    };

    let has_live_canceled_lease = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
            SELECT 1
            FROM workflow_steps ws
            JOIN job_queue jq ON jq.id = ws.job_id
            WHERE ws.workflow_run_id = $1
              AND jq.status = 'CANCELED'
              AND jq.lease_expires_at IS NOT NULL
              AND jq.lease_expires_at > clock_timestamp()
         )",
    )
    .bind(claim_run_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context(
            "check canceled workflow active claim quiescence",
            error,
        )
    })?;

    if has_live_canceled_lease {
        sqlx::query(
            "UPDATE workflow_active_claims
             SET release_pending = true
             WHERE workflow_run_id = $1",
        )
        .bind(workflow_run_id)
        .execute(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("defer canceled workflow active claim", error)
        })?;
    } else {
        sqlx::query(
            "DELETE FROM workflow_active_claims
             WHERE workflow_run_id = $1",
        )
        .bind(workflow_run_id)
        .execute(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("release workflow active claim", error)
        })?;
    }

    Ok(())
}

pub(crate) async fn release_quiesced_workflow_active_claims_tx(
    tx: &mut DbTx<'_>,
    limit: i64,
) -> Result<u64> {
    let released = sqlx::query(
        "WITH releasable AS MATERIALIZED (
            SELECT claim.scope, claim.active_key
            FROM workflow_active_claims claim
            WHERE claim.release_pending
              AND NOT EXISTS (
                  SELECT 1
                  FROM workflow_steps ws
                  JOIN job_queue jq ON jq.id = ws.job_id
                  WHERE ws.workflow_run_id = claim.workflow_run_id
                    AND jq.status = 'CANCELED'
                    AND jq.lease_expires_at IS NOT NULL
                    AND jq.lease_expires_at > clock_timestamp()
              )
            ORDER BY claim.updated_at ASC, claim.workflow_run_id ASC
            FOR UPDATE OF claim SKIP LOCKED
            LIMIT $1
         )
         DELETE FROM workflow_active_claims claim
         USING releasable
         WHERE claim.scope = releasable.scope
           AND claim.active_key = releasable.active_key",
    )
    .bind(limit)
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("release quiesced workflow active claims", error)
    })?
    .rows_affected();

    Ok(released)
}
