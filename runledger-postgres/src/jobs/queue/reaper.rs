use chrono::{DateTime, Utc};
use runledger_core::jobs::{JobFailure, WorkflowStepStatus};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, Result};

use super::super::errors::validate_positive_retry_delay;
use super::super::row_decode::parse_job_type_name;
use super::super::types::{
    ReapExpiredLeaseCleanupError, ReapExpiredLeaseCleanupOperation, ReapExpiredLeaseDeferredError,
    ReapExpiredLeasesDetailedResult, ReapExpiredLeasesResult, ReapedLeaseDisposition,
    ReapedLeaseRecord, ReapedTerminalLeaseRecord,
};
use super::super::workflows::{
    on_retry_scheduled, on_terminal, release_quiesced_workflow_active_claims_tx,
};
use super::attempts::ATTEMPT_CLAIM_ORIGIN_WORKER_PRESTART;
use super::claim::release_expired_execution_resource_claims_tx;
use super::release::{
    TryReleaseUnstartedClaimResult, UnstartedClaimIdentity, try_release_unstarted_job_claim_tx,
};

pub async fn reap_expired_leases(
    pool: &DbPool,
    limit: i64,
    default_retry_delay_ms: i32,
) -> Result<i64> {
    validate_positive_retry_delay(default_retry_delay_ms)?;
    let result =
        reap_expired_leases_with_terminal_records(pool, limit, default_retry_delay_ms).await?;
    Ok(result.processed)
}

pub async fn reap_expired_leases_with_terminal_records(
    pool: &DbPool,
    limit: i64,
    default_retry_delay_ms: i32,
) -> Result<ReapExpiredLeasesResult> {
    let result = reap_expired_leases_with_diagnostics(pool, limit, default_retry_delay_ms).await?;
    Ok(result.summary)
}

pub async fn reap_expired_leases_with_diagnostics(
    pool: &DbPool,
    limit: i64,
    default_retry_delay_ms: i32,
) -> Result<ReapExpiredLeasesDetailedResult> {
    validate_positive_retry_delay(default_retry_delay_ms)?;

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
            ja.claim_origin AS \"attempt_claim_origin?\",
            ja.execution_started_persisted_at AS \"execution_started_persisted_at?\",
            (
                SELECT je.occurred_at
                FROM job_events je
                WHERE je.job_id = jq.id
                  AND je.run_number = jq.run_number
                  AND je.attempt = jq.attempt
                  AND je.event_type = 'STAGE_CHANGED'
                  AND je.stage = 'running'
                ORDER BY je.id ASC
                LIMIT 1
            ) AS \"legacy_execution_started_persisted_at?\"
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
    let mut reaped_leases = Vec::new();
    let mut deferred_row_error_count = 0;
    let mut deferred_row_errors = Vec::new();
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
            attempt_claim_origin: db_row.attempt_claim_origin,
            execution_started_persisted_at: db_row.execution_started_persisted_at,
            legacy_execution_started_persisted_at: db_row.legacy_execution_started_persisted_at,
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
            Err(error) => {
                log_trusted_deferred_row_error(&row, &error);

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

                record_deferred_row_error(
                    &mut deferred_row_error_count,
                    &mut deferred_row_errors,
                    &row,
                    &error,
                );

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
        reaped_leases.push(reaped_lease_record(&row, &disposition));
    }

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    let mut cleanup_errors = Vec::new();
    let workflow_active_claims_released =
        match cleanup_quiesced_workflow_active_claims(pool, limit).await {
            Ok(released) => released,
            Err(error) => {
                cleanup_errors.push(ReapExpiredLeaseCleanupError {
                    operation: ReapExpiredLeaseCleanupOperation::WorkflowActiveClaims,
                    error: error.to_string(),
                });
                0
            }
        };
    let execution_resource_claims_released =
        match cleanup_expired_execution_resource_claims(pool, limit).await {
            Ok(released) => released,
            Err(error) => {
                cleanup_errors.push(ReapExpiredLeaseCleanupError {
                    operation: ReapExpiredLeaseCleanupOperation::ExecutionResourceClaims,
                    error: error.to_string(),
                });
                0
            }
        };
    let terminal_dead_lettered = terminal_dead_lettered_from(&reaped_leases);

    Ok(ReapExpiredLeasesDetailedResult {
        summary: ReapExpiredLeasesResult {
            processed,
            terminal_dead_lettered,
        },
        reaped_leases,
        deferred_row_error_count,
        deferred_row_errors,
        workflow_active_claims_released,
        execution_resource_claims_released,
        cleanup_errors,
    })
}

async fn cleanup_quiesced_workflow_active_claims(pool: &DbPool, limit: i64) -> Result<u64> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    let released = release_quiesced_workflow_active_claims_tx(&mut tx, limit).await?;
    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    Ok(released)
}

async fn cleanup_expired_execution_resource_claims(pool: &DbPool, limit: i64) -> Result<u64> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    let released = release_expired_execution_resource_claims_tx(&mut tx, limit).await?;
    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    Ok(released)
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
    attempt_claim_origin: Option<String>,
    execution_started_persisted_at: Option<DateTime<Utc>>,
    legacy_execution_started_persisted_at: Option<DateTime<Utc>>,
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
const MAX_DEFERRED_ROW_ERRORS: usize = 16;

enum ReapExpiredLeaseDisposition {
    ReleasedToPending,
    RetryScheduled {
        retry_delay_ms: i32,
        next_run_at: DateTime<Utc>,
    },
    DeadLetteredTerminal,
}

fn reaped_lease_record(
    row: &ReapExpiredLeaseRow,
    disposition: &ReapExpiredLeaseDisposition,
) -> ReapedLeaseRecord {
    let disposition = match disposition {
        ReapExpiredLeaseDisposition::ReleasedToPending => ReapedLeaseDisposition::ReleasedToPending,
        ReapExpiredLeaseDisposition::RetryScheduled {
            retry_delay_ms,
            next_run_at,
        } => ReapedLeaseDisposition::RetryScheduled {
            retry_delay_ms: *retry_delay_ms,
            next_run_at: *next_run_at,
        },
        ReapExpiredLeaseDisposition::DeadLetteredTerminal => {
            ReapedLeaseDisposition::DeadLetteredTerminal {
                payload: row.payload_snapshot.clone(),
            }
        }
    };

    ReapedLeaseRecord {
        job_id: row.job_id,
        job_type: row.job_type.clone(),
        organization_id: row.organization_id,
        run_number: row.run_number,
        attempt: row.attempt,
        max_attempts: row.max_attempts,
        checkpoint: row.checkpoint_snapshot.clone(),
        worker_id: row.worker_id.clone(),
        started_without_renewal_heartbeat: started_without_renewal_heartbeat(row),
        failure: lease_expired_failure(),
        disposition,
    }
}

fn terminal_dead_lettered_from(
    reaped_leases: &[ReapedLeaseRecord],
) -> Vec<ReapedTerminalLeaseRecord> {
    reaped_leases
        .iter()
        .filter_map(|record| match &record.disposition {
            ReapedLeaseDisposition::DeadLetteredTerminal { payload } => {
                Some(ReapedTerminalLeaseRecord {
                    job_id: record.job_id,
                    job_type: record.job_type.clone(),
                    organization_id: record.organization_id,
                    run_number: record.run_number,
                    attempt: record.attempt,
                    payload: payload.clone(),
                })
            }
            _ => None,
        })
        .collect()
}

fn lease_expired_failure() -> JobFailure {
    JobFailure::lease_expired(LEASE_EXPIRED_CODE, LEASE_EXPIRED_MESSAGE)
}

fn identity_for(row: &ReapExpiredLeaseRow) -> ReapLeaseIdentity {
    ReapLeaseIdentity {
        job_id: row.job_id,
        run_number: row.run_number,
        attempt: row.attempt,
    }
}

fn record_deferred_row_error(
    deferred_row_error_count: &mut usize,
    deferred_row_errors: &mut Vec<ReapExpiredLeaseDeferredError>,
    row: &ReapExpiredLeaseRow,
    error: &Error,
) {
    *deferred_row_error_count += 1;

    if deferred_row_errors.len() >= MAX_DEFERRED_ROW_ERRORS {
        return;
    }

    let (error_code, error_message, sqlstate) = sanitized_deferred_row_error(error);
    deferred_row_errors.push(ReapExpiredLeaseDeferredError {
        job_id: row.job_id,
        run_number: row.run_number,
        attempt: row.attempt,
        error_code,
        error_message,
        sqlstate,
    });
}

fn log_trusted_deferred_row_error(row: &ReapExpiredLeaseRow, error: &Error) {
    match error {
        Error::QueryError(query_error) => {
            let source = query_error.source_arc();
            let source_detail = source
                .as_deref()
                .map(ToString::to_string)
                .unwrap_or_default();
            tracing::warn!(
                job_id = %row.job_id,
                run_number = row.run_number,
                attempt = row.attempt,
                error_code = query_error.code(),
                error_sqlstate = query_error.sqlstate().unwrap_or(""),
                error_internal_message = query_error.internal_message(),
                error_has_source = source.is_some(),
                error_source = source_detail.as_str(),
                "reaper deferred expired leased job after row-level query error"
            );
        }
        Error::ConfigError(_) => log_trusted_deferred_row_non_query_error(
            row,
            "ConfigError",
            "reaper.config_error",
            "reaper row processing failed with a configuration error",
        ),
        Error::ConnectionError(_) => log_trusted_deferred_row_non_query_error(
            row,
            "ConnectionError",
            "db.connection_failed",
            "reaper row processing failed with a database connection error",
        ),
        Error::MigrationError(_) => log_trusted_deferred_row_non_query_error(
            row,
            "MigrationError",
            "db.migration_failed",
            "reaper row processing failed with a database migration error",
        ),
    }
}

fn log_trusted_deferred_row_non_query_error(
    row: &ReapExpiredLeaseRow,
    variant: &'static str,
    error_code: &'static str,
    message: &'static str,
) {
    tracing::warn!(
        job_id = %row.job_id,
        run_number = row.run_number,
        attempt = row.attempt,
        error_code,
        error_sqlstate = "",
        error_variant = variant,
        error_message = message,
        error_has_source = false,
        "reaper deferred expired leased job after row-level non-query error"
    );
}

fn sanitized_deferred_row_error(error: &Error) -> (String, String, Option<String>) {
    match error {
        Error::QueryError(query_error) => (
            query_error.code().to_owned(),
            query_error.client_message().to_owned(),
            query_error.sqlstate().map(ToOwned::to_owned),
        ),
        Error::ConfigError(_) => (
            "reaper.config_error".to_owned(),
            "Reaper configuration is invalid.".to_owned(),
            None,
        ),
        Error::ConnectionError(_) => (
            "db.connection_failed".to_owned(),
            "Database connection failed.".to_owned(),
            None,
        ),
        Error::MigrationError(_) => (
            "db.migration_failed".to_owned(),
            "Database migration failed.".to_owned(),
            None,
        ),
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
    if row.stage != runledger_core::jobs::JobStage::Running.as_db_value() {
        return false;
    }

    // Older direct-claim workers persisted the RUNNING event but did not fill
    // the attempt marker. The event occurred in the same transaction as the
    // stage update, so its first timestamp is an exact rolling-deploy fallback.
    let Some(execution_started_persisted_at) = row
        .execution_started_persisted_at
        .or(row.legacy_execution_started_persisted_at)
    else {
        return false;
    };

    row.last_heartbeat_at
        .is_none_or(|last_heartbeat_at| last_heartbeat_at <= execution_started_persisted_at)
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
             output = NULL,
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
        None,
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
             output = NULL,
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
) -> Result<DateTime<Utc>> {
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
    Ok(next_run_at)
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

    let next_run_at = handle_retryable_expired_lease(tx, row, default_retry_delay_ms).await?;
    Ok(ReapExpiredLeaseDisposition::RetryScheduled {
        retry_delay_ms: default_retry_delay_ms,
        next_run_at,
    })
}
