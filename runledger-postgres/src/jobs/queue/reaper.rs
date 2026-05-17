use chrono::{DateTime, Utc};
use runledger_core::jobs::WorkflowStepStatus;
use serde_json::Value;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, Result};

use super::super::row_decode::parse_job_type_name;
use super::super::types::{ReapExpiredLeasesResult, ReapedTerminalLeaseRecord};
use super::super::workflows::{on_retry_scheduled, on_terminal};
use super::attempts::ATTEMPT_CLAIM_ORIGIN_WORKER_PRESTART;
use super::release::{
    TryReleaseUnstartedClaimResult, UnstartedClaimIdentity, try_release_unstarted_job_claim_tx,
};

pub async fn reap_expired_leases(
    pool: &DbPool,
    limit: i64,
    default_retry_delay_ms: i32,
) -> Result<i64> {
    let result =
        reap_expired_leases_with_terminal_records(pool, limit, default_retry_delay_ms).await?;
    Ok(result.processed)
}

pub async fn reap_expired_leases_with_terminal_records(
    pool: &DbPool,
    limit: i64,
    default_retry_delay_ms: i32,
) -> Result<ReapExpiredLeasesResult> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    let rows = sqlx::query!(
        "SELECT
            jq.id,
            jq.run_number,
            jq.attempt,
            jq.max_attempts,
            jq.job_type,
            jq.organization_id,
            jq.payload,
            jq.checkpoint,
            jq.stage,
            jq.worker_id,
            jq.last_heartbeat_at AS \"last_heartbeat_at?\",
            jq.updated_at,
            ja.claim_origin AS \"attempt_claim_origin?\",
            ja.execution_started_persisted_at AS \"execution_started_persisted_at?\"
         FROM job_queue jq
         LEFT JOIN job_attempts ja
           ON ja.job_id = jq.id
          AND ja.run_number = jq.run_number
          AND ja.attempt = jq.attempt
         WHERE jq.status = 'LEASED'
           AND jq.lease_expires_at IS NOT NULL
           AND jq.lease_expires_at < now()
         ORDER BY jq.lease_expires_at ASC
         FOR UPDATE OF jq SKIP LOCKED
         LIMIT $1",
        limit,
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("reap expired lease lookup", error))?;

    let mut processed: i64 = 0;
    let mut terminal_dead_lettered = Vec::new();
    for db_row in rows {
        let row = ReapExpiredLeaseRow {
            job_id: db_row.id,
            run_number: db_row.run_number,
            attempt: db_row.attempt,
            max_attempts: db_row.max_attempts,
            job_type: parse_job_type_name(db_row.job_type)?,
            organization_id: db_row.organization_id,
            payload_snapshot: db_row.payload,
            checkpoint_snapshot: db_row.checkpoint,
            stage: db_row.stage,
            worker_id: db_row.worker_id,
            last_heartbeat_at: db_row.last_heartbeat_at,
            updated_at: db_row.updated_at,
            attempt_claim_origin: db_row.attempt_claim_origin,
            execution_started_persisted_at: db_row.execution_started_persisted_at,
        };
        let job_id = row.job_id;
        let run_number = row.run_number;

        sqlx::query!("SAVEPOINT reaper_row")
            .execute(&mut *tx)
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context("reap create row savepoint", error)
            })?;

        let disposition = match reap_expired_lease_row_tx(&mut tx, &row, default_retry_delay_ms)
            .await
        {
            Ok(disposition) => disposition,
            Err(_error) => {
                sqlx::query!("ROLLBACK TO SAVEPOINT reaper_row")
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| {
                        Error::from_query_sqlx_with_context("reap rollback row savepoint", error)
                    })?;
                sqlx::query!("RELEASE SAVEPOINT reaper_row")
                    .execute(&mut *tx)
                    .await
                    .map_err(|error| {
                        Error::from_query_sqlx_with_context("reap release row savepoint", error)
                    })?;

                // Push failed rows out of the immediate expired-lease window so one
                // poison row does not starve otherwise healthy work in subsequent batches.
                sqlx::query!(
                    "UPDATE job_queue
                     SET lease_expires_at = now() + ($3::bigint * interval '1 millisecond'),
                         updated_at = now()
                     WHERE id = $1
                       AND run_number = $2
                       AND status = 'LEASED'",
                    job_id,
                    run_number,
                    i64::from(default_retry_delay_ms),
                )
                .execute(&mut *tx)
                .await
                .map_err(|error| {
                    Error::from_query_sqlx_with_context("reap defer failed row", error)
                })?;

                continue;
            }
        };

        sqlx::query!("RELEASE SAVEPOINT reaper_row")
            .execute(&mut *tx)
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context("reap release row savepoint", error)
            })?;

        processed += 1;
        if matches!(
            disposition,
            ReapExpiredLeaseDisposition::DeadLetteredTerminal
        ) {
            terminal_dead_lettered.push(ReapedTerminalLeaseRecord {
                job_id: row.job_id,
                job_type: row.job_type.clone(),
                organization_id: row.organization_id,
                run_number: row.run_number,
                attempt: row.attempt,
                payload: row.payload_snapshot.clone(),
            });
        }
    }

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    Ok(ReapExpiredLeasesResult {
        processed,
        terminal_dead_lettered,
    })
}

struct ReapExpiredLeaseRow {
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    max_attempts: i32,
    job_type: runledger_core::jobs::JobTypeName,
    organization_id: Option<Uuid>,
    payload_snapshot: Value,
    checkpoint_snapshot: Option<Value>,
    stage: String,
    worker_id: Option<String>,
    last_heartbeat_at: Option<DateTime<Utc>>,
    updated_at: DateTime<Utc>,
    attempt_claim_origin: Option<String>,
    execution_started_persisted_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Copy)]
struct ReapLeaseIdentity {
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
}

const LEASE_EXPIRED_KIND: &str = "LEASE_EXPIRED";
const LEASE_EXPIRED_CODE: &str = "job.lease_expired";
const LEASE_EXPIRED_MESSAGE: &str = "Job lease expired before completion.";

enum ReapExpiredLeaseDisposition {
    ReleasedToPending,
    Retried,
    DeadLetteredTerminal,
}

fn identity_for(row: &ReapExpiredLeaseRow) -> ReapLeaseIdentity {
    ReapLeaseIdentity {
        job_id: row.job_id,
        run_number: row.run_number,
        attempt: row.attempt,
    }
}

fn is_exhausted(row: &ReapExpiredLeaseRow) -> bool {
    row.attempt >= row.max_attempts
}

fn is_worker_prestart_unstarted(row: &ReapExpiredLeaseRow) -> bool {
    row.attempt_claim_origin.as_deref() == Some(ATTEMPT_CLAIM_ORIGIN_WORKER_PRESTART)
        && row.execution_started_persisted_at.is_none()
}

fn started_without_renewal_heartbeat(row: &ReapExpiredLeaseRow) -> bool {
    row.stage == runledger_core::jobs::JobStage::Running.as_db_value()
        && row
            .last_heartbeat_at
            .is_some_and(|last_heartbeat_at| last_heartbeat_at < row.updated_at)
}

async fn update_dead_lettered_queue_row(
    tx: &mut DbTx<'_>,
    identity: ReapLeaseIdentity,
) -> Result<()> {
    sqlx::query!(
        "UPDATE job_queue
         SET status = 'DEAD_LETTERED',
             lease_expires_at = NULL,
             last_heartbeat_at = NULL,
             worker_id = NULL,
             finished_at = now(),
             status_reason = 'LEASE_EXPIRED',
             last_error_code = 'job.lease_expired',
             last_error_message = 'Job lease expired before completion.',
             updated_at = now()
         WHERE id = $1
           AND run_number = $2",
        identity.job_id,
        identity.run_number,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("reap mark dead lettered", error))?;
    Ok(())
}

async fn update_dead_lettered_attempt(
    tx: &mut DbTx<'_>,
    identity: ReapLeaseIdentity,
) -> Result<()> {
    sqlx::query!(
        "UPDATE job_attempts
         SET finished_at = now(),
             outcome = 'LEASE_EXPIRED',
             error_code = 'job.lease_expired',
             error_message = 'Job lease expired before completion.'
         WHERE job_id = $1
           AND run_number = $2
           AND attempt = $3",
        identity.job_id,
        identity.run_number,
        identity.attempt,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("reap update dead lettered attempt", error)
    })?;
    Ok(())
}

async fn insert_dead_letter_row(tx: &mut DbTx<'_>, row: &ReapExpiredLeaseRow) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_dead_letters (
            job_id,
            job_type,
            organization_id,
            run_number,
            attempt,
            error_code,
            error_message,
            payload_snapshot,
            checkpoint_snapshot,
            failed_at
         )
         VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            'job.lease_expired',
            'Job lease expired before completion.',
            $6::jsonb,
            $7::jsonb,
            now()
         )
         ON CONFLICT (job_id)
         DO UPDATE
            SET run_number = EXCLUDED.run_number,
                attempt = EXCLUDED.attempt,
                error_code = EXCLUDED.error_code,
                error_message = EXCLUDED.error_message,
                payload_snapshot = EXCLUDED.payload_snapshot,
                checkpoint_snapshot = EXCLUDED.checkpoint_snapshot,
                failed_at = EXCLUDED.failed_at",
        row.job_id,
        row.job_type.as_str(),
        row.organization_id,
        row.run_number,
        row.attempt,
        row.payload_snapshot,
        row.checkpoint_snapshot,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("reap insert dead letter row", error))?;
    Ok(())
}

async fn insert_failed_event(
    tx: &mut DbTx<'_>,
    row: &ReapExpiredLeaseRow,
    identity: ReapLeaseIdentity,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_events (job_id, run_number, attempt, event_type, payload)
         VALUES (
            $1,
            $2,
            $3,
            'FAILED',
            jsonb_build_object(
                'kind', 'LEASE_EXPIRED',
                'error_code', 'job.lease_expired',
                'error_message', 'Job lease expired before completion.',
                'started_without_renewal_heartbeat', $4::bool
            )
         )",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        started_without_renewal_heartbeat(row),
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("reap failed event", error))?;
    Ok(())
}

async fn insert_dead_lettered_event(
    tx: &mut DbTx<'_>,
    row: &ReapExpiredLeaseRow,
    identity: ReapLeaseIdentity,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_events (job_id, run_number, attempt, event_type, payload)
         VALUES (
            $1,
            $2,
            $3,
            'DEAD_LETTERED',
            jsonb_build_object(
                'kind', 'LEASE_EXPIRED',
                'error_code', 'job.lease_expired',
                'started_without_renewal_heartbeat', $4::bool
            )
         )",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        started_without_renewal_heartbeat(row),
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("reap dead lettered event", error))?;
    Ok(())
}

async fn handle_exhausted_expired_lease(
    tx: &mut DbTx<'_>,
    row: &ReapExpiredLeaseRow,
) -> Result<()> {
    let identity = identity_for(row);
    update_dead_lettered_queue_row(tx, identity).await?;
    update_dead_lettered_attempt(tx, identity).await?;
    insert_dead_letter_row(tx, row).await?;
    insert_failed_event(tx, row, identity).await?;
    insert_dead_lettered_event(tx, row, identity).await?;

    on_terminal(
        tx,
        identity.job_id,
        WorkflowStepStatus::Failed,
        Some(LEASE_EXPIRED_KIND),
        Some(LEASE_EXPIRED_CODE),
        Some(LEASE_EXPIRED_MESSAGE),
    )
    .await?;

    Ok(())
}

async fn update_retryable_queue_row(
    tx: &mut DbTx<'_>,
    identity: ReapLeaseIdentity,
    retry_delay_ms: i32,
) -> Result<DateTime<Utc>> {
    sqlx::query_scalar!(
        "UPDATE job_queue
         SET status = 'PENDING',
             lease_expires_at = NULL,
             last_heartbeat_at = NULL,
             worker_id = NULL,
             next_run_at = now() + ($2::bigint * interval '1 millisecond'),
             status_reason = 'LEASE_EXPIRED',
             last_error_code = 'job.lease_expired',
             last_error_message = 'Job lease expired before completion.',
             updated_at = now()
         WHERE id = $1
           AND run_number = $3
         RETURNING next_run_at",
        identity.job_id,
        i64::from(retry_delay_ms),
        identity.run_number,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("reap mark retryable", error))
}

async fn update_retry_attempt(
    tx: &mut DbTx<'_>,
    identity: ReapLeaseIdentity,
    retry_delay_ms: i32,
) -> Result<()> {
    sqlx::query!(
        "UPDATE job_attempts
         SET finished_at = now(),
             outcome = 'LEASE_EXPIRED',
             error_code = 'job.lease_expired',
             error_message = 'Job lease expired before completion.',
             retry_delay_ms = $4
         WHERE job_id = $1
           AND run_number = $2
           AND attempt = $3",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        retry_delay_ms,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("reap update retry attempt", error))?;
    Ok(())
}

async fn insert_retry_scheduled_event(
    tx: &mut DbTx<'_>,
    row: &ReapExpiredLeaseRow,
    identity: ReapLeaseIdentity,
    retry_delay_ms: i32,
    next_run_at: DateTime<Utc>,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_events (job_id, run_number, attempt, event_type, payload)
         VALUES (
            $1,
            $2,
            $3,
            'RETRY_SCHEDULED',
            jsonb_build_object(
                'kind', 'LEASE_EXPIRED',
                'retry_delay_ms', $4::int4,
                'next_run_at', $5::timestamptz,
                'started_without_renewal_heartbeat', $6::bool
            )
         )",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        retry_delay_ms,
        next_run_at,
        started_without_renewal_heartbeat(row),
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("reap retry event", error))?;
    Ok(())
}

async fn handle_retryable_expired_lease(
    tx: &mut DbTx<'_>,
    row: &ReapExpiredLeaseRow,
    default_retry_delay_ms: i32,
) -> Result<()> {
    let identity = identity_for(row);
    let next_run_at = update_retryable_queue_row(tx, identity, default_retry_delay_ms).await?;
    update_retry_attempt(tx, identity, default_retry_delay_ms).await?;
    insert_failed_event(tx, row, identity).await?;
    insert_retry_scheduled_event(tx, row, identity, default_retry_delay_ms, next_run_at).await?;
    on_retry_scheduled(
        tx,
        identity.job_id,
        Some(LEASE_EXPIRED_KIND),
        Some(LEASE_EXPIRED_CODE),
        Some(LEASE_EXPIRED_MESSAGE),
    )
    .await?;
    Ok(())
}

async fn reap_expired_lease_row_tx(
    tx: &mut DbTx<'_>,
    row: &ReapExpiredLeaseRow,
    default_retry_delay_ms: i32,
) -> Result<ReapExpiredLeaseDisposition> {
    if is_worker_prestart_unstarted(row) {
        // This is an optimistic read of the joined attempt row. A worker can
        // persist RUNNING after the reaper SELECT but before this branch runs;
        // try_release_unstarted_job_claim_tx re-checks
        // execution_started_persisted_at IS NULL in its UPDATE and returns
        // NotApplicable on that race, which falls through to normal reaper
        // retry/dead-letter handling.
        match try_release_unstarted_job_claim_tx(
            tx,
            UnstartedClaimIdentity {
                job_id: row.job_id,
                run_number: row.run_number,
                attempt: row.attempt,
                worker_id: row.worker_id.as_deref(),
            },
            "LEASE_EXPIRED_BEFORE_RUNNING_PERSISTED",
            0,
        )
        .await?
        {
            TryReleaseUnstartedClaimResult::Released => {
                return Ok(ReapExpiredLeaseDisposition::ReleasedToPending);
            }
            TryReleaseUnstartedClaimResult::NotApplicable => {}
        }
    }

    if is_exhausted(row) {
        handle_exhausted_expired_lease(tx, row).await?;
        return Ok(ReapExpiredLeaseDisposition::DeadLetteredTerminal);
    }

    handle_retryable_expired_lease(tx, row, default_retry_delay_ms).await?;
    Ok(ReapExpiredLeaseDisposition::Retried)
}
