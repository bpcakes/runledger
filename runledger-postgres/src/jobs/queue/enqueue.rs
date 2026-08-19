use chrono::SecondsFormat;
use runledger_core::jobs::JobStage;
use serde::Serialize;
use serde_json::Value;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, QueryError, QueryErrorCategory, Result};

use super::super::row_decode::parse_job_status;
use super::super::transaction_isolation::{ReadCommittedTx, ensure_read_committed_tx};
use super::super::types::{JobEnqueue, JobEnqueueDisposition, JobEnqueueOutcome};
use super::events::{EnqueuedEventPayload, EnqueuedJobEvent, insert_enqueued_event_tx};

// Snapshot upgrades are a multi-version consumer migration, not a constant
// replacement: promoters must retain decoding/promotion paths for every version
// that can remain pending until that version's backlog has drained.
pub(super) const JOB_ENQUEUE_REQUEST_VERSION: i16 = 1;

#[derive(sqlx::FromRow)]
struct EnqueuedJobRow {
    id: Uuid,
    status: String,
    run_number: i32,
}

#[derive(sqlx::FromRow)]
struct ExistingIdempotentJobRow {
    id: Uuid,
    status: String,
    run_number: i32,
    enqueue_request_matches: Option<bool>,
}

#[derive(Serialize)]
struct CanonicalJobEnqueueRequest<'a> {
    payload: &'a Value,
    priority: Option<i32>,
    max_attempts: Option<i32>,
    timeout_seconds: Option<i32>,
    next_run_at: Option<String>,
    stage: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_resource_key: Option<&'a str>,
}

#[derive(Clone, Copy)]
enum ExistingJobLock {
    KeyShare,
    MutationReady,
}

pub(super) enum IntentEnqueueResolution {
    Enqueued(JobEnqueueOutcome),
    DefinitionUnavailable,
    Conflict {
        code: &'static str,
        client_message: &'static str,
    },
}

/// Enqueues a job and returns the existing job id for an identical keyed retry.
///
/// Use this for one independent unit of retried work. If the work has
/// dependencies, fan-out/fan-in, or needs to be canceled as one logical run, use
/// the workflow DAG APIs instead: build a `WorkflowRunEnqueue` with
/// `WorkflowRunEnqueueBuilder` and `WorkflowStepEnqueueBuilder`, then call
/// `enqueue_workflow_run` or `enqueue_workflow_run_tx`.
///
/// Idempotency is strict for the submitted request snapshot, including the
/// requested initial stage and schedule. Later runtime mutations of those
/// fields do not affect retries because keyed rows compare the stored original
/// request. Unkeyed rows do not store snapshots, and keyed rows without
/// snapshots are rejected by the idempotency cutover.
pub async fn enqueue_job_with_outcome_tx(
    tx: &mut DbTx<'_>,
    payload: &JobEnqueue<'_>,
) -> Result<JobEnqueueOutcome> {
    enqueue_job_with_existing_lock_tx(
        tx,
        payload,
        None,
        ExistingJobLock::MutationReady,
        EnqueuedEventPayload::Ordinary,
    )
    .await
}

/// Enqueues a job that must exclusively own one durable execution resource
/// while leased.
///
/// Jobs blocked by an existing owner remain pending and consume neither an
/// attempt nor a returned claim slot. Claim ordering is evaluated within a
/// worker's allowed job-type set, so workers with different type filters can
/// choose different heads for the same resource; the resource claim still
/// enforces mutual exclusion. Keys must be non-blank and at most 512 bytes.
/// Their scope is global across organizations; namespace them in the
/// application when tenants must not coordinate. For an idempotently keyed
/// job, the resource key is part of the canonical enqueue request and cannot
/// change on retry.
pub async fn enqueue_job_with_execution_resource_tx(
    tx: &mut DbTx<'_>,
    payload: &JobEnqueue<'_>,
    execution_resource_key: &str,
) -> Result<JobEnqueueOutcome> {
    enqueue_job_with_existing_lock_tx(
        tx,
        payload,
        Some(execution_resource_key),
        ExistingJobLock::MutationReady,
        EnqueuedEventPayload::Ordinary,
    )
    .await
}

pub(in crate::jobs) async fn enqueue_replayed_job_with_outcome_tx(
    tx: &mut DbTx<'_>,
    payload: &JobEnqueue<'_>,
    execution_resource_key: Option<&str>,
    event_payload: EnqueuedEventPayload<'_>,
) -> Result<JobEnqueueOutcome> {
    debug_assert!(payload.idempotency_key.is_none());
    debug_assert!(matches!(
        &event_payload,
        EnqueuedEventPayload::SuccessfulReplay { .. }
    ));
    enqueue_job_with_existing_lock_tx(
        tx,
        payload,
        execution_resource_key,
        ExistingJobLock::MutationReady,
        event_payload,
    )
    .await
}

pub(super) async fn enqueue_job_from_intent_tx(
    tx: &mut ReadCommittedTx<'_, '_>,
    payload: &JobEnqueue<'_>,
    execution_resource_key: Option<&str>,
) -> Result<IntentEnqueueResolution> {
    debug_assert!(payload.idempotency_key.is_some());
    let stage = payload.stage.unwrap_or(JobStage::Queued).as_db_value();
    match enqueue_idempotent_job_with_existing_lock_read_committed_tx(
        tx,
        payload,
        execution_resource_key,
        ExistingJobLock::MutationReady,
        EnqueuedEventPayload::Ordinary,
        stage,
    )
    .await
    {
        Ok(outcome) => Ok(IntentEnqueueResolution::Enqueued(outcome)),
        Err(Error::QueryError(error)) if error.code() == "job.definition_not_found_or_disabled" => {
            Ok(IntentEnqueueResolution::DefinitionUnavailable)
        }
        Err(Error::QueryError(error))
            if matches!(
                error.code(),
                "job.idempotency_conflict" | "job.legacy_idempotency_snapshot_missing"
            ) =>
        {
            Ok(IntentEnqueueResolution::Conflict {
                code: error.code(),
                client_message: error.client_message(),
            })
        }
        Err(error) => Err(error),
    }
}

async fn enqueue_job_with_existing_lock_tx(
    tx: &mut DbTx<'_>,
    payload: &JobEnqueue<'_>,
    execution_resource_key: Option<&str>,
    existing_job_lock: ExistingJobLock,
    event_payload: EnqueuedEventPayload<'_>,
) -> Result<JobEnqueueOutcome> {
    let stage = payload
        .stage
        .unwrap_or(runledger_core::jobs::JobStage::Queued)
        .as_db_value();
    if let Some(execution_resource_key) = execution_resource_key {
        validate_execution_resource_key(execution_resource_key)?;
    }
    if payload.idempotency_key.is_some() {
        let mut read_committed_tx = ensure_read_committed_tx(
            tx,
            "job idempotent enqueue",
            "job.enqueue_idempotency_unsupported_isolation",
            "Job idempotent enqueue requires READ COMMITTED transaction isolation.",
        )
        .await?;

        return enqueue_idempotent_job_with_existing_lock_read_committed_tx(
            &mut read_committed_tx,
            payload,
            execution_resource_key,
            existing_job_lock,
            event_payload,
            stage,
        )
        .await;
    }

    enqueue_job_with_existing_lock_tx_inner(
        tx,
        payload,
        execution_resource_key,
        existing_job_lock,
        event_payload,
        stage,
    )
    .await
}

async fn enqueue_idempotent_job_with_existing_lock_read_committed_tx(
    tx: &mut ReadCommittedTx<'_, '_>,
    payload: &JobEnqueue<'_>,
    execution_resource_key: Option<&str>,
    existing_job_lock: ExistingJobLock,
    event_payload: EnqueuedEventPayload<'_>,
    stage: &'static str,
) -> Result<JobEnqueueOutcome> {
    debug_assert!(payload.idempotency_key.is_some());
    enqueue_job_with_existing_lock_tx_inner(
        tx.as_tx(),
        payload,
        execution_resource_key,
        existing_job_lock,
        event_payload,
        stage,
    )
    .await
}

async fn enqueue_job_with_existing_lock_tx_inner(
    tx: &mut DbTx<'_>,
    payload: &JobEnqueue<'_>,
    execution_resource_key: Option<&str>,
    existing_job_lock: ExistingJobLock,
    event_payload: EnqueuedEventPayload<'_>,
    stage: &'static str,
) -> Result<JobEnqueueOutcome> {
    let enqueue_request = payload
        .idempotency_key
        .map(|_| canonical_job_enqueue_request_v1(payload, stage, execution_resource_key))
        .transpose()?;
    // The conflict clause is selected from static literals only; all request
    // data remains bound below. This dynamic SQL is not SQLx macro-checked, so
    // keep the returned columns and bind order aligned with EnqueuedJobRow and
    // the job_queue insert list.
    let insert_sql = format!(
        "WITH defaults AS (
            SELECT
                jd.default_priority,
                jd.max_attempts,
                jd.default_timeout_seconds
            FROM job_definitions jd
            WHERE jd.job_type = $1
              AND jd.is_enabled = true
            FOR SHARE OF jd
         )
         INSERT INTO job_queue (
            job_type,
            organization_id,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            next_run_at,
            idempotency_key,
            stage,
            enqueue_request,
            execution_resource_key
         )
         SELECT
            $1,
            $2,
            $3::jsonb,
            COALESCE($4, d.default_priority),
            COALESCE($5, d.max_attempts),
            COALESCE($6, d.default_timeout_seconds),
            COALESCE($7, now()),
            $8,
            $9,
            $10::jsonb,
            $11
         FROM defaults d
         {}
         RETURNING id, status::text AS status, run_number",
        enqueue_job_idempotency_conflict_clause(payload),
    );
    let row = sqlx::query_as::<_, EnqueuedJobRow>(&insert_sql)
        .bind(payload.job_type)
        .bind(payload.organization_id)
        .bind(payload.payload)
        .bind(payload.priority)
        .bind(payload.max_attempts)
        .bind(payload.timeout_seconds)
        .bind(payload.next_run_at)
        .bind(payload.idempotency_key)
        .bind(stage)
        .bind(enqueue_request.as_ref())
        .bind(execution_resource_key)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| Error::from_query_sqlx_with_context("enqueue job", error))?;

    let Some(row) = row else {
        let Some(enqueue_request) = enqueue_request.as_ref() else {
            return Err(job_definition_unavailable_error());
        };
        return resolve_existing_idempotent_job_tx(tx, payload, enqueue_request, existing_job_lock)
            .await;
    };

    let job_id: Uuid = row.id;
    let run_number: i32 = row.run_number;
    let status = parse_job_status(row.status)?;

    insert_enqueued_event_tx(
        tx,
        EnqueuedJobEvent {
            job_id,
            run_number,
            stage,
            job_type: payload.job_type,
            payload: event_payload,
        },
    )
    .await?;

    Ok(JobEnqueueOutcome {
        job_id,
        status,
        run_number,
        disposition: JobEnqueueDisposition::Inserted,
    })
}

/// Enqueues a job while preserving the original UUID-only API.
pub async fn enqueue_job_tx(tx: &mut DbTx<'_>, payload: &JobEnqueue<'_>) -> Result<Uuid> {
    enqueue_job_with_existing_lock_tx(
        tx,
        payload,
        None,
        ExistingJobLock::KeyShare,
        EnqueuedEventPayload::Ordinary,
    )
    .await
    .map(|outcome| outcome.job_id)
}

// Keyed enqueues compare the canonical request snapshot exactly on retry. The
// stored snapshot preserves requested initial fields such as stage and
// next_run_at, while staying independent from later stage transitions and retry
// scheduling.
async fn resolve_existing_idempotent_job_tx(
    tx: &mut DbTx<'_>,
    payload: &JobEnqueue<'_>,
    enqueue_request: &Value,
    existing_job_lock: ExistingJobLock,
) -> Result<JobEnqueueOutcome> {
    let Some(idempotency_key) = payload.idempotency_key else {
        return Err(job_definition_unavailable_error());
    };

    let Some(existing) = load_existing_idempotent_job_tx(
        tx,
        payload,
        idempotency_key,
        enqueue_request,
        existing_job_lock,
    )
    .await?
    else {
        if job_definition_available_tx(tx, payload.job_type.as_str()).await? {
            return Err(idempotent_job_missing_existing_error(
                payload.job_type.as_str(),
            ));
        }
        return Err(job_definition_unavailable_error());
    };

    validate_existing_idempotent_job(payload, &existing)?;
    Ok(JobEnqueueOutcome {
        job_id: existing.id,
        status: parse_job_status(existing.status)?,
        run_number: existing.run_number,
        disposition: JobEnqueueDisposition::Existing,
    })
}

fn job_definition_unavailable_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "job.definition_not_found_or_disabled",
        "Job type is not available.",
        "enqueue job: definition missing or disabled",
    ))
}

async fn job_definition_available_tx(tx: &mut DbTx<'_>, job_type: &str) -> Result<bool> {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
            SELECT 1
            FROM job_definitions
            WHERE job_type = $1
              AND is_enabled = true
         )",
    )
    .bind(job_type)
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("check job definition availability", error)
    })
}

async fn load_existing_idempotent_job_tx(
    tx: &mut DbTx<'_>,
    payload: &JobEnqueue<'_>,
    idempotency_key: &str,
    enqueue_request: &Value,
    existing_job_lock: ExistingJobLock,
) -> Result<Option<ExistingIdempotentJobRow>> {
    // The outcome API gives callers a mutation-ready lock. The legacy UUID-only
    // API uses KEY SHARE so identical keyed enqueues remain concurrent while a
    // later compare-and-requeue can acquire NO KEY UPDATE without a lock upgrade
    // cycle. Neither path permits the row identity to be deleted or changed.
    // Keep the nullable scope split so PostgreSQL can prove the predicate of the
    // matching partial unique index. `IS NOT DISTINCT FROM` is logically
    // equivalent here but forces a scan.
    let result = match (payload.organization_id, existing_job_lock) {
        (Some(organization_id), ExistingJobLock::MutationReady) => {
            sqlx::query_as!(
                ExistingIdempotentJobRow,
                r#"SELECT
                id,
                status::text AS "status!",
                run_number,
                enqueue_request = $4::jsonb AS "enqueue_request_matches?"
             FROM job_queue
             WHERE job_type = $1
               AND organization_id = $2
               AND idempotency_key = $3
             LIMIT 1
             FOR NO KEY UPDATE"#,
                payload.job_type as _,
                organization_id,
                idempotency_key,
                enqueue_request,
            )
            .fetch_optional(&mut **tx)
            .await
        }
        (None, ExistingJobLock::MutationReady) => {
            sqlx::query_as!(
                ExistingIdempotentJobRow,
                r#"SELECT
                id,
                status::text AS "status!",
                run_number,
                enqueue_request = $3::jsonb AS "enqueue_request_matches?"
             FROM job_queue
             WHERE job_type = $1
               AND organization_id IS NULL
               AND idempotency_key = $2
             LIMIT 1
             FOR NO KEY UPDATE"#,
                payload.job_type as _,
                idempotency_key,
                enqueue_request,
            )
            .fetch_optional(&mut **tx)
            .await
        }
        (Some(organization_id), ExistingJobLock::KeyShare) => {
            sqlx::query_as!(
                ExistingIdempotentJobRow,
                r#"SELECT
                    id,
                    status::text AS "status!",
                    run_number,
                    enqueue_request = $4::jsonb AS "enqueue_request_matches?"
                 FROM job_queue
                 WHERE job_type = $1
                   AND organization_id = $2
                   AND idempotency_key = $3
                 LIMIT 1
                 FOR KEY SHARE"#,
                payload.job_type as _,
                organization_id,
                idempotency_key,
                enqueue_request,
            )
            .fetch_optional(&mut **tx)
            .await
        }
        (None, ExistingJobLock::KeyShare) => {
            sqlx::query_as!(
                ExistingIdempotentJobRow,
                r#"SELECT
                    id,
                    status::text AS "status!",
                    run_number,
                    enqueue_request = $3::jsonb AS "enqueue_request_matches?"
                 FROM job_queue
                 WHERE job_type = $1
                   AND organization_id IS NULL
                   AND idempotency_key = $2
                 LIMIT 1
                 FOR KEY SHARE"#,
                payload.job_type as _,
                idempotency_key,
                enqueue_request,
            )
            .fetch_optional(&mut **tx)
            .await
        }
    };
    result
        .map_err(|error| Error::from_query_sqlx_with_context("load idempotent job enqueue", error))
}

fn enqueue_job_idempotency_conflict_clause(payload: &JobEnqueue<'_>) -> &'static str {
    // Keep these predicates aligned with the partial unique indexes
    // uq_job_queue_type_idempotency_org and uq_job_queue_type_idempotency_global.
    match (payload.idempotency_key, payload.organization_id) {
        (Some(_), Some(_)) => {
            "ON CONFLICT (job_type, organization_id, idempotency_key)
             WHERE idempotency_key IS NOT NULL
               AND organization_id IS NOT NULL
             DO NOTHING"
        }
        (Some(_), None) => {
            "ON CONFLICT (job_type, idempotency_key)
             WHERE idempotency_key IS NOT NULL
               AND organization_id IS NULL
             DO NOTHING"
        }
        (None, _) => "",
    }
}

fn validate_existing_idempotent_job(
    payload: &JobEnqueue<'_>,
    existing: &ExistingIdempotentJobRow,
) -> Result<()> {
    match existing.enqueue_request_matches {
        Some(true) => Ok(()),
        Some(false) => Err(idempotent_job_conflict_error(
            payload.job_type.as_str(),
            "request",
        )),
        None => Err(legacy_job_idempotency_snapshot_missing_error(
            payload.job_type.as_str(),
            existing.id,
        )),
    }
}

pub(super) fn canonical_job_enqueue_request_v1(
    payload: &JobEnqueue<'_>,
    stage: &'static str,
    execution_resource_key: Option<&str>,
) -> Result<Value> {
    serde_json::to_value(CanonicalJobEnqueueRequest {
        payload: payload.payload,
        priority: payload.priority,
        max_attempts: payload.max_attempts,
        timeout_seconds: payload.timeout_seconds,
        // Match PostgreSQL timestamptz's microsecond precision so a retry built
        // from the persisted schedule compares the same as the original request.
        next_run_at: payload
            .next_run_at
            .map(|next_run_at| next_run_at.to_rfc3339_opts(SecondsFormat::Micros, true)),
        stage,
        execution_resource_key,
    })
    .map_err(|error| {
        Error::QueryError(QueryError::from_classified(
            QueryErrorCategory::Internal,
            "job.enqueue_request_snapshot_failed",
            "Job enqueue request could not be recorded.",
            format!("failed to serialize canonical job enqueue request: {error}"),
        ))
    })
}

pub(super) fn validate_execution_resource_key(execution_resource_key: &str) -> Result<()> {
    if execution_resource_key.trim().is_empty() || execution_resource_key.len() > 512 {
        return Err(Error::QueryError(QueryError::from_classified(
            QueryErrorCategory::Validation,
            "job.invalid_execution_resource_key",
            "Execution resource key must be non-blank and at most 512 bytes.",
            "job enqueue execution_resource_key was blank or exceeded 512 bytes",
        )));
    }
    Ok(())
}

fn idempotent_job_conflict_error(job_type: &str, field: &str) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Conflict,
        "job.idempotency_conflict",
        "Job enqueue retry conflicts with the existing idempotency key.",
        format!("job enqueue idempotency conflict for job_type={job_type}: field {field} differs"),
    ))
}

fn idempotent_job_missing_existing_error(job_type: &str) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Internal,
        "job.idempotency_conflict_missing_existing",
        "Job enqueue retry could not be resolved.",
        format!(
            "job enqueue insert for job_type={job_type} conflicted but matching idempotent job was not found"
        ),
    ))
}

fn legacy_job_idempotency_snapshot_missing_error(job_type: &str, job_id: Uuid) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Conflict,
        "job.legacy_idempotency_snapshot_missing",
        "Job enqueue retry cannot be resolved because the existing idempotency key is missing its request snapshot.",
        format!(
            "job enqueue idempotency retry for job_type={job_type} matched legacy job {job_id} without enqueue_request snapshot"
        ),
    ))
}

/// Enqueues a job in its own transaction.
///
/// Use this for one independent unit of retried work. For dependent work, use
/// the workflow DAG APIs instead of polling job state or enqueueing follow-up
/// jobs from handlers.
///
/// Calls without an idempotency key always create a new job. Calls with an
/// idempotency key return the existing job id only when the canonical request
/// snapshot matches.
pub async fn enqueue_job(pool: &DbPool, payload: &JobEnqueue<'_>) -> Result<Uuid> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    let id = enqueue_job_tx(&mut tx, payload).await?;
    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    Ok(id)
}

/// Enqueues a resource-constrained job in its own transaction.
///
/// See [`enqueue_job_with_execution_resource_tx`] for key scope, validation,
/// and claim behavior.
pub async fn enqueue_job_with_execution_resource(
    pool: &DbPool,
    payload: &JobEnqueue<'_>,
    execution_resource_key: &str,
) -> Result<JobEnqueueOutcome> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    let outcome =
        enqueue_job_with_execution_resource_tx(&mut tx, payload, execution_resource_key).await?;
    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    Ok(outcome)
}

#[cfg(test)]
mod idempotency_tests {
    use chrono::{TimeZone, Utc};
    use runledger_core::jobs::{JobStage, JobType};
    use serde_json::json;

    use super::{JobEnqueue, canonical_job_enqueue_request_v1};

    #[test]
    fn canonical_job_enqueue_request_v1_matches_golden_snapshot() {
        let payload = json!({"kind": "golden"});
        let enqueue = JobEnqueue {
            job_type: JobType::new("jobs.test.golden"),
            organization_id: None,
            payload: &payload,
            priority: Some(10),
            max_attempts: Some(3),
            timeout_seconds: Some(30),
            next_run_at: Some(
                Utc.with_ymd_and_hms(2026, 5, 22, 10, 30, 45)
                    .single()
                    .expect("valid timestamp"),
            ),
            idempotency_key: Some("job-golden"),
            stage: Some(JobStage::Scheduled),
        };

        let canonical =
            canonical_job_enqueue_request_v1(&enqueue, JobStage::Scheduled.as_db_value(), None)
                .expect("canonicalize job enqueue");

        assert_eq!(
            canonical,
            json!({
                "payload": {"kind": "golden"},
                "priority": 10,
                "max_attempts": 3,
                "timeout_seconds": 30,
                "next_run_at": "2026-05-22T10:30:45.000000Z",
                "stage": "scheduled"
            })
        );
    }
}
