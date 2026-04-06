use runledger_core::jobs::JobType;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, QueryError, QueryErrorCategory, Result};

use super::super::row_decode::{parse_job_stage, parse_job_status, parse_job_type_name};
use super::super::types::{JobEnqueue, JobQueueRecord};
use super::super::workflows::on_claimed;
use super::attempts::{ATTEMPT_CLAIM_ORIGIN_DIRECT, ATTEMPT_CLAIM_ORIGIN_WORKER_PRESTART};

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

pub async fn enqueue_job_tx(tx: &mut DbTx<'_>, payload: &JobEnqueue<'_>) -> Result<Uuid> {
    let stage = payload
        .stage
        .unwrap_or(runledger_core::jobs::JobStage::Queued)
        .as_db_value();
    let row = sqlx::query!(
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
            stage
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
            $9
         FROM defaults d
         RETURNING id, run_number",
        payload.job_type as _,
        payload.organization_id,
        payload.payload,
        payload.priority,
        payload.max_attempts,
        payload.timeout_seconds,
        payload.next_run_at,
        payload.idempotency_key,
        stage,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("enqueue job", error))?
    .ok_or_else(|| {
        Error::QueryError(QueryError::from_classified(
            QueryErrorCategory::Validation,
            "job.definition_not_found_or_disabled",
            "Job type is not available.",
            "enqueue job: definition missing or disabled",
        ))
    })?;
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
