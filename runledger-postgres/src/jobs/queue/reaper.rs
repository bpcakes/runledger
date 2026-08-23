use chrono::{DateTime, Utc};
use runledger_core::jobs::JobFailure;
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
use super::super::workflows::release_quiesced_workflow_active_claims_tx;
use super::attempts::ATTEMPT_CLAIM_ORIGIN_WORKER_PRESTART;
use super::claim::release_expired_execution_resource_claims_tx;
use super::failure_transition::{
    DeadLetterSnapshot, ExpiredLeaseTransition, FailureIdentity, LEASE_EXPIRED_FAILURE,
};
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

    let batch = reap_expired_lease_batch(pool, limit, default_retry_delay_ms).await?;
    let cleanup = cleanup_reaped_lease_coordination(pool, limit).await;

    Ok(batch.into_detailed_result(cleanup))
}

async fn reap_expired_lease_batch(
    pool: &DbPool,
    limit: i64,
    default_retry_delay_ms: i32,
) -> Result<ReapExpiredLeaseBatchOutcome> {
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

    let mut outcome = ReapExpiredLeaseBatchOutcome::default();
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
                log_sanitized_deferred_row_error(&row, &error);

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

                outcome.record_deferred_row_error(&row, &error);

                continue;
            }
        };

        sqlx::query!("RELEASE SAVEPOINT reaper_row")
            .execute(&mut *tx)
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context("reap release row savepoint", error)
            })?;

        outcome.record_reaped_lease(&row, &disposition);
    }

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    Ok(outcome)
}

async fn cleanup_reaped_lease_coordination(
    pool: &DbPool,
    limit: i64,
) -> ReapExpiredLeaseCleanupOutcome {
    let mut outcome = ReapExpiredLeaseCleanupOutcome::default();
    outcome.record_workflow_active_claim_cleanup(
        cleanup_quiesced_workflow_active_claims(pool, limit).await,
    );
    outcome.record_execution_resource_claim_cleanup(
        cleanup_expired_execution_resource_claims(pool, limit).await,
    );
    outcome
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

#[derive(Default)]
struct ReapExpiredLeaseBatchOutcome {
    processed: i64,
    reaped_leases: Vec<ReapedLeaseRecord>,
    deferred_row_error_count: usize,
    deferred_row_errors: Vec<ReapExpiredLeaseDeferredError>,
}

impl ReapExpiredLeaseBatchOutcome {
    fn record_reaped_lease(
        &mut self,
        row: &ReapExpiredLeaseRow,
        disposition: &ReapExpiredLeaseDisposition,
    ) {
        self.processed += 1;
        self.reaped_leases
            .push(reaped_lease_record(row, disposition));
    }

    fn record_deferred_row_error(&mut self, row: &ReapExpiredLeaseRow, error: &Error) {
        self.deferred_row_error_count += 1;

        if self.deferred_row_errors.len() >= MAX_DEFERRED_ROW_ERRORS {
            return;
        }

        let (error_code, error_message, sqlstate) = sanitized_deferred_row_error(error);
        self.deferred_row_errors
            .push(ReapExpiredLeaseDeferredError {
                job_id: row.job_id,
                run_number: row.run_number,
                attempt: row.attempt,
                error_code,
                error_message,
                sqlstate,
            });
    }

    fn into_detailed_result(
        self,
        cleanup: ReapExpiredLeaseCleanupOutcome,
    ) -> ReapExpiredLeasesDetailedResult {
        let Self {
            processed,
            reaped_leases,
            deferred_row_error_count,
            deferred_row_errors,
        } = self;
        let ReapExpiredLeaseCleanupOutcome {
            workflow_active_claims_released,
            execution_resource_claims_released,
            cleanup_errors,
        } = cleanup;

        ReapExpiredLeasesDetailedResult {
            summary: ReapExpiredLeasesResult {
                processed,
                terminal_dead_lettered: terminal_dead_lettered_from(&reaped_leases),
            },
            reaped_leases,
            deferred_row_error_count,
            deferred_row_errors,
            workflow_active_claims_released,
            execution_resource_claims_released,
            cleanup_errors,
        }
    }
}

#[derive(Default)]
struct ReapExpiredLeaseCleanupOutcome {
    workflow_active_claims_released: u64,
    execution_resource_claims_released: u64,
    cleanup_errors: Vec<ReapExpiredLeaseCleanupError>,
}

impl ReapExpiredLeaseCleanupOutcome {
    fn record_workflow_active_claim_cleanup(&mut self, result: Result<u64>) {
        let released = self.capture_cleanup_result(
            ReapExpiredLeaseCleanupOperation::WorkflowActiveClaims,
            result,
        );
        self.workflow_active_claims_released = released;
    }

    fn record_execution_resource_claim_cleanup(&mut self, result: Result<u64>) {
        let released = self.capture_cleanup_result(
            ReapExpiredLeaseCleanupOperation::ExecutionResourceClaims,
            result,
        );
        self.execution_resource_claims_released = released;
    }

    fn capture_cleanup_result(
        &mut self,
        operation: ReapExpiredLeaseCleanupOperation,
        result: Result<u64>,
    ) -> u64 {
        match result {
            Ok(released) => released,
            Err(error) => {
                self.cleanup_errors.push(ReapExpiredLeaseCleanupError {
                    operation,
                    error: error.to_string(),
                });
                0
            }
        }
    }
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
    JobFailure::lease_expired(
        LEASE_EXPIRED_FAILURE.code(),
        LEASE_EXPIRED_FAILURE.message(),
    )
}

fn failure_transition_for(row: &ReapExpiredLeaseRow) -> ExpiredLeaseTransition<'_> {
    ExpiredLeaseTransition::new(
        FailureIdentity::new(row.job_id, row.run_number, row.attempt),
        LEASE_EXPIRED_FAILURE,
        DeadLetterSnapshot::new(
            &row.job_type,
            row.organization_id,
            &row.payload_snapshot,
            row.checkpoint_snapshot.as_ref(),
        ),
        started_without_renewal_heartbeat(row),
    )
}

fn log_sanitized_deferred_row_error(row: &ReapExpiredLeaseRow, error: &Error) {
    match error {
        Error::QueryError(query_error) => {
            let diagnostics = query_error.sanitized_diagnostics();
            tracing::warn!(
                job_id = %row.job_id,
                run_number = row.run_number,
                attempt = row.attempt,
                error_code = diagnostics.code(),
                error_sqlstate = diagnostics.sqlstate().unwrap_or(""),
                error_constraint = diagnostics.constraint().unwrap_or(""),
                "reaper deferred expired leased job after row-level query error"
            );
        }
        Error::ConfigError(_) => log_sanitized_deferred_row_non_query_error(
            row,
            "ConfigError",
            "reaper.config_error",
            "reaper row processing failed with a configuration error",
        ),
        Error::ConnectionError(_) => log_sanitized_deferred_row_non_query_error(
            row,
            "ConnectionError",
            "db.connection_failed",
            "reaper row processing failed with a database connection error",
        ),
        Error::MigrationError(_) => log_sanitized_deferred_row_non_query_error(
            row,
            "MigrationError",
            "db.migration_failed",
            "reaper row processing failed with a database migration error",
        ),
    }
}

fn log_sanitized_deferred_row_non_query_error(
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

    let transition = failure_transition_for(row);
    if is_exhausted(row) {
        transition.apply_terminal(tx).await?;
        return Ok(ReapExpiredLeaseDisposition::DeadLetteredTerminal);
    }

    let next_run_at = transition.apply_retry(tx, default_retry_delay_ms).await?;
    Ok(ReapExpiredLeaseDisposition::RetryScheduled {
        retry_delay_ms: default_retry_delay_ms,
        next_run_at,
    })
}
