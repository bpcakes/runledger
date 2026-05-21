use chrono::SecondsFormat;
use runledger_core::jobs::JobType;
use serde::Serialize;
use serde_json::Value;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, QueryError, QueryErrorCategory, Result};

use super::super::row_decode::{parse_job_stage, parse_job_status, parse_job_type_name};
use super::super::transaction_isolation::ensure_read_committed_tx;
use super::super::types::{JobEnqueue, JobQueueRecord};
use super::super::workflows::on_claimed;
use super::attempts::{ATTEMPT_CLAIM_ORIGIN_DIRECT, ATTEMPT_CLAIM_ORIGIN_WORKER_PRESTART};

#[derive(sqlx::FromRow)]
struct EnqueuedJobRow {
    id: Uuid,
    run_number: i32,
}

#[derive(sqlx::FromRow)]
struct ExistingIdempotentJobRow {
    id: Uuid,
    payload_matches: bool,
    priority: i32,
    max_attempts: i32,
    timeout_seconds: i32,
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
}

#[derive(Clone, Copy)]
enum AttemptClaimOrigin {
    Direct,
    WorkerPrestart,
}

impl AttemptClaimOrigin {
    const fn as_db_value(self) -> &'static str {
        match self {
            Self::Direct => ATTEMPT_CLAIM_ORIGIN_DIRECT,
            Self::WorkerPrestart => ATTEMPT_CLAIM_ORIGIN_WORKER_PRESTART,
        }
    }
}

/// Enqueues a job and returns the existing job id for an identical keyed retry.
///
/// Idempotency is strict for the submitted request snapshot, including the
/// requested initial stage and schedule. Later runtime mutations of those
/// fields do not affect retries because keyed rows compare the stored original
/// request; unkeyed rows do not store snapshots, and legacy keyed rows without
/// snapshots can only compare durable request fields.
pub async fn enqueue_job_tx(tx: &mut DbTx<'_>, payload: &JobEnqueue<'_>) -> Result<Uuid> {
    let stage = payload
        .stage
        .unwrap_or(runledger_core::jobs::JobStage::Queued)
        .as_db_value();
    if payload.idempotency_key.is_some() {
        ensure_read_committed_tx(
            tx,
            "job idempotent enqueue",
            "job.enqueue_idempotency_unsupported_isolation",
            "Job idempotent enqueue requires READ COMMITTED transaction isolation.",
        )
        .await?;
    }
    let enqueue_request = payload
        .idempotency_key
        .map(|_| canonical_job_enqueue_request(payload, stage))
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
            enqueue_request
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
            $10::jsonb
         FROM defaults d
         {}
         RETURNING id, run_number",
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
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| Error::from_query_sqlx_with_context("enqueue job", error))?;

    let Some(row) = row else {
        let Some(enqueue_request) = enqueue_request.as_ref() else {
            return Err(job_definition_unavailable_error());
        };
        return resolve_existing_idempotent_job_tx(tx, payload, enqueue_request).await;
    };

    let job_id: Uuid = row.id;
    let run_number: i32 = row.run_number;

    sqlx::query!(
        "INSERT INTO job_events (
            job_id,
            run_number,
            event_type,
            stage,
            payload
         )
         VALUES ($1, $2, 'ENQUEUED', $3, jsonb_build_object('job_type', $4::text))",
        job_id,
        run_number,
        stage,
        payload.job_type as _,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("enqueue job event", error))?;

    Ok(job_id)
}

// Keyed enqueues compare the canonical request snapshot exactly on retry. The
// stored snapshot preserves requested initial fields such as stage and
// next_run_at, while staying independent from later stage transitions and retry
// scheduling.
async fn resolve_existing_idempotent_job_tx(
    tx: &mut DbTx<'_>,
    payload: &JobEnqueue<'_>,
    enqueue_request: &Value,
) -> Result<Uuid> {
    let Some(idempotency_key) = payload.idempotency_key else {
        return Err(job_definition_unavailable_error());
    };

    let Some(existing) =
        load_existing_idempotent_job_tx(tx, payload, idempotency_key, enqueue_request).await?
    else {
        if job_definition_available_tx(tx, payload.job_type.as_str()).await? {
            return Err(idempotent_job_missing_existing_error(
                payload.job_type.as_str(),
            ));
        }
        return Err(job_definition_unavailable_error());
    };

    validate_existing_idempotent_job(payload, &existing)?;
    Ok(existing.id)
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
) -> Result<Option<ExistingIdempotentJobRow>> {
    // FOR SHARE keeps the matched job stable until the enqueue transaction
    // returns the existing idempotent result.
    if let Some(organization_id) = payload.organization_id {
        sqlx::query_as::<_, ExistingIdempotentJobRow>(
            "SELECT
                id,
                payload = $5::jsonb AS payload_matches,
                priority,
                max_attempts,
                timeout_seconds,
                enqueue_request = $4::jsonb AS enqueue_request_matches
             FROM job_queue
             WHERE job_type = $1
               AND organization_id = $2
               AND idempotency_key = $3
             LIMIT 1
             FOR SHARE",
        )
        .bind(payload.job_type)
        .bind(organization_id)
        .bind(idempotency_key)
        .bind(enqueue_request)
        .bind(payload.payload)
        .fetch_optional(&mut **tx)
        .await
    } else {
        sqlx::query_as::<_, ExistingIdempotentJobRow>(
            "SELECT
                id,
                payload = $4::jsonb AS payload_matches,
                priority,
                max_attempts,
                timeout_seconds,
                enqueue_request = $3::jsonb AS enqueue_request_matches
             FROM job_queue
             WHERE job_type = $1
               AND organization_id IS NULL
               AND idempotency_key = $2
             LIMIT 1
             FOR SHARE",
        )
        .bind(payload.job_type)
        .bind(idempotency_key)
        .bind(enqueue_request)
        .bind(payload.payload)
        .fetch_optional(&mut **tx)
        .await
    }
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
        Some(true) => return Ok(()),
        Some(false) => {
            return Err(idempotent_job_conflict_error(
                payload.job_type.as_str(),
                "request",
            ));
        }
        None => {}
    }

    // Legacy rows created before enqueue_request existed can only be compared
    // against durable queue fields. Omitted optional fields intentionally skip
    // comparison because the original request may have relied on then-current
    // defaults, or may have supplied the same value the row now stores. Do not
    // compare lifecycle-mutated fields such as stage or next_run_at.
    if !existing.payload_matches {
        return Err(idempotent_job_conflict_error(
            payload.job_type.as_str(),
            "payload",
        ));
    }
    if let Some(priority) = payload.priority
        && existing.priority != priority
    {
        return Err(idempotent_job_conflict_error(
            payload.job_type.as_str(),
            "priority",
        ));
    }
    if let Some(max_attempts) = payload.max_attempts
        && existing.max_attempts != max_attempts
    {
        return Err(idempotent_job_conflict_error(
            payload.job_type.as_str(),
            "max_attempts",
        ));
    }
    if let Some(timeout_seconds) = payload.timeout_seconds
        && existing.timeout_seconds != timeout_seconds
    {
        return Err(idempotent_job_conflict_error(
            payload.job_type.as_str(),
            "timeout_seconds",
        ));
    }
    tracing::warn!(
        job_id = %existing.id,
        job_type = payload.job_type.as_str(),
        organization_id = ?payload.organization_id,
        "accepted legacy job idempotency retry without enqueue_request snapshot"
    );
    Ok(())
}

fn canonical_job_enqueue_request(payload: &JobEnqueue<'_>, stage: &'static str) -> Result<Value> {
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

/// Enqueues a job in its own transaction.
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

pub async fn claim_jobs(
    pool: &DbPool,
    worker_id: &str,
    lease_duration_seconds: i32,
    limit: i64,
) -> Result<Vec<JobQueueRecord>> {
    claim_jobs_inner(
        pool,
        worker_id,
        lease_duration_seconds,
        limit,
        None,
        AttemptClaimOrigin::Direct,
    )
    .await
}

pub async fn claim_jobs_for_types(
    pool: &DbPool,
    worker_id: &str,
    lease_duration_seconds: i32,
    limit: i64,
    allowed_job_types: &[JobType<'_>],
) -> Result<Vec<JobQueueRecord>> {
    if allowed_job_types.is_empty() {
        return Ok(Vec::new());
    }

    claim_jobs_inner(
        pool,
        worker_id,
        lease_duration_seconds,
        limit,
        Some(allowed_job_types),
        AttemptClaimOrigin::Direct,
    )
    .await
}

pub async fn claim_prestart_jobs(
    pool: &DbPool,
    worker_id: &str,
    lease_duration_seconds: i32,
    limit: i64,
) -> Result<Vec<JobQueueRecord>> {
    claim_jobs_inner(
        pool,
        worker_id,
        lease_duration_seconds,
        limit,
        None,
        AttemptClaimOrigin::WorkerPrestart,
    )
    .await
}

pub async fn claim_prestart_jobs_for_types(
    pool: &DbPool,
    worker_id: &str,
    lease_duration_seconds: i32,
    limit: i64,
    allowed_job_types: &[JobType<'_>],
) -> Result<Vec<JobQueueRecord>> {
    if allowed_job_types.is_empty() {
        return Ok(Vec::new());
    }

    claim_jobs_inner(
        pool,
        worker_id,
        lease_duration_seconds,
        limit,
        Some(allowed_job_types),
        AttemptClaimOrigin::WorkerPrestart,
    )
    .await
}

async fn claim_jobs_inner(
    pool: &DbPool,
    worker_id: &str,
    lease_duration_seconds: i32,
    limit: i64,
    allowed_job_types: Option<&[JobType<'_>]>,
    claim_origin: AttemptClaimOrigin,
) -> Result<Vec<JobQueueRecord>> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    let claim_ids = fetch_claim_ids(&mut tx, limit, allowed_job_types).await?;

    if claim_ids.is_empty() {
        tx.commit()
            .await
            .map_err(|error| Error::ConnectionError(error.to_string()))?;
        return Ok(Vec::new());
    }

    let rows = sqlx::query!(
        "UPDATE job_queue
         SET status = 'LEASED',
             attempt = attempt + 1,
             worker_id = $1,
             lease_expires_at = now() + make_interval(secs => $2::int4),
             last_heartbeat_at = now(),
             started_at = COALESCE(started_at, now()),
             updated_at = now()
         WHERE id = ANY($3::uuid[])
         RETURNING
            id,
            job_type,
            organization_id,
            payload,
            status::text AS \"status!\",
            priority,
            run_number,
            attempt,
            max_attempts,
            timeout_seconds,
            next_run_at,
            lease_expires_at,
            last_heartbeat_at,
            worker_id,
            started_at,
            finished_at,
            stage,
            progress_done,
            progress_total,
            progress_pct::float8 AS progress_pct,
            checkpoint,
            idempotency_key,
            status_reason,
            last_error_code,
            last_error_message,
            created_at,
            updated_at",
        worker_id,
        lease_duration_seconds,
        &claim_ids,
    )
    .fetch_all(&mut *tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("claim jobs update", error))?;

    let claimed: Vec<JobQueueRecord> = rows
        .into_iter()
        .map(|row| {
            Ok(JobQueueRecord {
                id: row.id,
                job_type: parse_job_type_name(row.job_type)?,
                organization_id: row.organization_id,
                payload: row.payload,
                status: parse_job_status(row.status)?,
                priority: row.priority,
                run_number: row.run_number,
                attempt: row.attempt,
                max_attempts: row.max_attempts,
                timeout_seconds: row.timeout_seconds,
                next_run_at: row.next_run_at,
                lease_expires_at: row.lease_expires_at,
                last_heartbeat_at: row.last_heartbeat_at,
                worker_id: row.worker_id,
                started_at: row.started_at,
                finished_at: row.finished_at,
                stage: parse_job_stage(row.stage)?,
                progress_done: row.progress_done,
                progress_total: row.progress_total,
                progress_pct: row.progress_pct,
                checkpoint: row.checkpoint,
                idempotency_key: row.idempotency_key,
                status_reason: row.status_reason,
                last_error_code: row.last_error_code,
                last_error_message: row.last_error_message,
                created_at: row.created_at,
                updated_at: row.updated_at,
            })
        })
        .collect::<Result<_>>()?;

    for job in &claimed {
        on_claimed(&mut tx, job.id).await?;

        sqlx::query!(
            "INSERT INTO job_attempts (
                job_id,
                run_number,
                attempt,
                worker_id,
                leased_at,
                started_at,
                claim_origin,
                execution_started_persisted_at
             )
             VALUES ($1, $2, $3, $4, now(), now(), $5, NULL)",
            job.id,
            job.run_number,
            job.attempt,
            worker_id,
            claim_origin.as_db_value(),
        )
        .execute(&mut *tx)
        .await
        .map_err(|error| Error::from_query_sqlx_with_context("claim jobs attempt insert", error))?;

        sqlx::query!(
            "INSERT INTO job_events (
                job_id,
                run_number,
                attempt,
                event_type,
                stage,
                payload
             )
             VALUES (
                $1,
                $2,
                $3,
                'LEASED',
                $4,
                jsonb_build_object('worker_id', $5::text, 'lease_duration_seconds', $6::int4)
             )",
            job.id,
            job.run_number,
            job.attempt,
            job.stage.as_db_value(),
            worker_id,
            lease_duration_seconds,
        )
        .execute(&mut *tx)
        .await
        .map_err(|error| Error::from_query_sqlx_with_context("claim jobs event insert", error))?;
    }

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    Ok(claimed)
}

async fn fetch_claim_ids(
    tx: &mut DbTx<'_>,
    limit: i64,
    allowed_job_types: Option<&[JobType<'_>]>,
) -> Result<Vec<Uuid>> {
    let query_result = match allowed_job_types {
        Some(allowed_job_types) => {
            let allowed_job_types = allowed_job_types
                .iter()
                .map(|job_type| job_type.as_str().to_string())
                .collect::<Vec<_>>();
            sqlx::query_scalar!(
                "SELECT id
                 FROM job_queue
                 WHERE status = 'PENDING'
                   AND next_run_at <= now()
                   AND job_type = ANY($2::text[])
                 ORDER BY priority DESC, next_run_at ASC, created_at ASC
                 FOR UPDATE SKIP LOCKED
                 LIMIT $1",
                limit,
                allowed_job_types.as_slice(),
            )
            .fetch_all(&mut **tx)
            .await
        }
        None => {
            sqlx::query_scalar!(
                "SELECT id
                 FROM job_queue
                 WHERE status = 'PENDING'
                   AND next_run_at <= now()
                 ORDER BY priority DESC, next_run_at ASC, created_at ASC
                 FOR UPDATE SKIP LOCKED
                 LIMIT $1",
                limit,
            )
            .fetch_all(&mut **tx)
            .await
        }
    };

    query_result
        .map_err(|error| Error::from_query_sqlx_with_context("claim jobs candidate list", error))
}
