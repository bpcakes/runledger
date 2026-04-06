use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, Result};

use super::super::errors::lease_owner_mismatch_error;
use super::super::errors::unstarted_claim_release_not_applicable_error;
use super::super::workflows::on_claim_released;
use super::attempts::ATTEMPT_CLAIM_ORIGIN_WORKER_PRESTART;

#[derive(Clone, Copy)]
pub(crate) struct UnstartedClaimIdentity<'a> {
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: i32,
    pub worker_id: Option<&'a str>,
}

impl UnstartedClaimIdentity<'_> {
    const fn should_reset_started_at(self) -> bool {
        self.attempt == 1
    }
}

pub(crate) enum TryReleaseUnstartedClaimResult {
    Released,
    NotApplicable,
}

async fn classify_unstarted_release_not_applicable_tx(
    tx: &mut DbTx<'_>,
    identity: UnstartedClaimIdentity<'_>,
) -> Result<Error> {
    let row = sqlx::query!(
        "SELECT
            jq.status::text AS \"status?\",
            jq.worker_id
         FROM job_queue jq
         WHERE jq.id = $1
           AND jq.run_number = $2
         LIMIT 1",
        identity.job_id,
        identity.run_number,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("classify unstarted claim release miss", error)
    })?;

    let lease_owned_by_other_worker = row.as_ref().is_some_and(|row| {
        row.status.as_deref() == Some("LEASED")
            && identity.worker_id.is_some()
            && row.worker_id.as_deref() != identity.worker_id
    });

    Ok(if lease_owned_by_other_worker {
        lease_owner_mismatch_error()
    } else {
        unstarted_claim_release_not_applicable_error()
    })
}

async fn release_unstarted_job_queue_row_tx(
    tx: &mut DbTx<'_>,
    identity: UnstartedClaimIdentity<'_>,
    retry_delay_ms: i32,
) -> Result<u64> {
    let rows_affected = sqlx::query!(
        "UPDATE job_queue
         SET status = 'PENDING',
             attempt = attempt - 1,
             lease_expires_at = NULL,
             last_heartbeat_at = NULL,
             worker_id = NULL,
             next_run_at = now() + ($6::bigint * interval '1 millisecond'),
             started_at = CASE
                WHEN $5 THEN NULL
                ELSE started_at
             END,
             status_reason = NULL,
             last_error_code = NULL,
             last_error_message = NULL,
             updated_at = now()
         WHERE id = $1
           AND run_number = $2
           AND attempt = $3
           AND status = 'LEASED'
           AND ($4::text IS NULL OR worker_id = $4)
           AND EXISTS (
                SELECT 1
                FROM job_attempts ja
                WHERE ja.job_id = $1
                  AND ja.run_number = $2
                  AND ja.attempt = $3
                  AND ja.claim_origin = $7
                  AND ja.execution_started_persisted_at IS NULL
           )",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        identity.worker_id,
        identity.should_reset_started_at(),
        i64::from(retry_delay_ms.max(0)),
        ATTEMPT_CLAIM_ORIGIN_WORKER_PRESTART,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("release unstarted job claim", error))?
    .rows_affected();

    Ok(rows_affected)
}

async fn delete_attempt_row_tx(
    tx: &mut DbTx<'_>,
    identity: UnstartedClaimIdentity<'_>,
) -> Result<()> {
    sqlx::query!(
        "DELETE FROM job_attempts
         WHERE job_id = $1
           AND run_number = $2
           AND attempt = $3
           AND claim_origin = $4
           AND execution_started_persisted_at IS NULL",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        ATTEMPT_CLAIM_ORIGIN_WORKER_PRESTART,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("delete unstarted job attempt", error))?;

    Ok(())
}

async fn insert_requeued_event_tx(
    tx: &mut DbTx<'_>,
    identity: UnstartedClaimIdentity<'_>,
    reason: &str,
) -> Result<()> {
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
            'REQUEUED',
            jsonb_build_object('reason', $4::text)
         )",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        reason,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("insert unstarted-claim requeued event", error)
    })?;

    Ok(())
}

pub(crate) async fn try_release_unstarted_job_claim_tx(
    tx: &mut DbTx<'_>,
    identity: UnstartedClaimIdentity<'_>,
    reason: &str,
    retry_delay_ms: i32,
) -> Result<TryReleaseUnstartedClaimResult> {
    let updated = release_unstarted_job_queue_row_tx(tx, identity, retry_delay_ms).await?;
    if updated == 0 {
        return Ok(TryReleaseUnstartedClaimResult::NotApplicable);
    }

    on_claim_released(tx, identity.job_id, identity.should_reset_started_at()).await?;
    delete_attempt_row_tx(tx, identity).await?;
    insert_requeued_event_tx(tx, identity, reason).await?;

    Ok(TryReleaseUnstartedClaimResult::Released)
}

pub(crate) async fn release_unstarted_job_claim_tx(
    tx: &mut DbTx<'_>,
    identity: UnstartedClaimIdentity<'_>,
    reason: &str,
    retry_delay_ms: i32,
) -> Result<()> {
    if matches!(
        try_release_unstarted_job_claim_tx(tx, identity, reason, retry_delay_ms).await?,
        TryReleaseUnstartedClaimResult::Released
    ) {
        return Ok(());
    }

    Err(classify_unstarted_release_not_applicable_tx(tx, identity).await?)
}

pub async fn release_unstarted_job_claim(
    pool: &DbPool,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &str,
    reason: &str,
    retry_delay_ms: i32,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    release_unstarted_job_claim_tx(
        &mut tx,
        UnstartedClaimIdentity {
            job_id,
            run_number,
            attempt,
            worker_id: Some(worker_id),
        },
        reason,
        retry_delay_ms,
    )
    .await?;

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    Ok(())
}
