use runledger_core::jobs::JobType;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, Result};

use super::super::errors::validate_positive_lease_duration;
use super::super::rows::JobQueueRow;
use super::super::types::JobQueueRecord;
use super::super::workflows::on_claimed;
use super::attempts::{ATTEMPT_CLAIM_ORIGIN_DIRECT, ATTEMPT_CLAIM_ORIGIN_WORKER_PRESTART};

const MIN_RESOURCE_HEAD_WINDOW: i64 = 1_024;
const MAX_RESOURCE_HEAD_WINDOW: i64 = 16_384;
const RESOURCE_HEAD_WINDOW_PER_CLAIM: i64 = 64;

#[derive(Clone, Copy)]
enum AttemptClaimOrigin {
    Direct,
    WorkerPrestart,
}

struct ClaimRequest<'a> {
    worker_id: &'a str,
    lease_duration_seconds: i32,
    limit: i64,
    allowed_job_types: Option<&'a [JobType<'a>]>,
    claim_origin: AttemptClaimOrigin,
}

impl AttemptClaimOrigin {
    const fn as_db_value(self) -> &'static str {
        match self {
            Self::Direct => ATTEMPT_CLAIM_ORIGIN_DIRECT,
            Self::WorkerPrestart => ATTEMPT_CLAIM_ORIGIN_WORKER_PRESTART,
        }
    }
}

pub async fn claim_jobs(
    pool: &DbPool,
    worker_id: &str,
    lease_duration_seconds: i32,
    limit: i64,
) -> Result<Vec<JobQueueRecord>> {
    validate_positive_lease_duration(lease_duration_seconds)?;

    claim_jobs_inner(
        pool,
        ClaimRequest {
            worker_id,
            lease_duration_seconds,
            limit,
            allowed_job_types: None,
            claim_origin: AttemptClaimOrigin::Direct,
        },
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
    validate_positive_lease_duration(lease_duration_seconds)?;

    if allowed_job_types.is_empty() {
        return Ok(Vec::new());
    }

    claim_jobs_inner(
        pool,
        ClaimRequest {
            worker_id,
            lease_duration_seconds,
            limit,
            allowed_job_types: Some(allowed_job_types),
            claim_origin: AttemptClaimOrigin::Direct,
        },
    )
    .await
}

pub async fn claim_prestart_jobs(
    pool: &DbPool,
    worker_id: &str,
    lease_duration_seconds: i32,
    limit: i64,
) -> Result<Vec<JobQueueRecord>> {
    validate_positive_lease_duration(lease_duration_seconds)?;

    claim_jobs_inner(
        pool,
        ClaimRequest {
            worker_id,
            lease_duration_seconds,
            limit,
            allowed_job_types: None,
            claim_origin: AttemptClaimOrigin::WorkerPrestart,
        },
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
    validate_positive_lease_duration(lease_duration_seconds)?;

    if allowed_job_types.is_empty() {
        return Ok(Vec::new());
    }

    claim_jobs_inner(
        pool,
        ClaimRequest {
            worker_id,
            lease_duration_seconds,
            limit,
            allowed_job_types: Some(allowed_job_types),
            claim_origin: AttemptClaimOrigin::WorkerPrestart,
        },
    )
    .await
}

async fn claim_jobs_inner(pool: &DbPool, request: ClaimRequest<'_>) -> Result<Vec<JobQueueRecord>> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    let claimed = lease_claimed_rows_tx(&mut tx, &request).await?;
    if claimed.is_empty() {
        tx.commit()
            .await
            .map_err(|error| Error::ConnectionError(error.to_string()))?;
        return Ok(claimed);
    }

    record_claim_side_effects_tx(&mut tx, &claimed, &request).await?;

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    Ok(claimed)
}

async fn lease_claimed_rows_tx(
    tx: &mut DbTx<'_>,
    request: &ClaimRequest<'_>,
) -> Result<Vec<JobQueueRecord>> {
    let claim_ids = fetch_claim_ids(tx, request).await?;
    if claim_ids.is_empty() {
        return Ok(Vec::new());
    }

    let rows = sqlx::query_as!(
        JobQueueRow,
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
            output,
            idempotency_key,
            status_reason,
            last_error_code,
            last_error_message,
            created_at,
            updated_at",
        request.worker_id,
        request.lease_duration_seconds,
        &claim_ids,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("claim jobs update", error))?;

    let claimed: Vec<JobQueueRecord> = rows
        .into_iter()
        .map(JobQueueRow::into_record)
        .collect::<Result<_>>()?;

    Ok(claimed)
}

async fn record_claim_side_effects_tx(
    tx: &mut DbTx<'_>,
    claimed: &[JobQueueRecord],
    request: &ClaimRequest<'_>,
) -> Result<()> {
    for job in claimed {
        on_claimed(tx, job.id).await?;

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
            request.worker_id,
            request.claim_origin.as_db_value(),
        )
        .execute(&mut **tx)
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
            request.worker_id,
            request.lease_duration_seconds,
        )
        .execute(&mut **tx)
        .await
        .map_err(|error| Error::from_query_sqlx_with_context("claim jobs event insert", error))?;
    }

    Ok(())
}

#[allow(
    clippy::allow_attributes_without_reason,
    reason = "SQLx query macro expansion contains generated allow attributes without lint reasons"
)]
async fn fetch_claim_ids(tx: &mut DbTx<'_>, request: &ClaimRequest<'_>) -> Result<Vec<Uuid>> {
    let resource_head_window = resource_head_window_limit(request.limit);
    let query_result = match request.allowed_job_types {
        Some(allowed_job_types) => {
            let allowed_job_types = allowed_job_types
                .iter()
                .map(|job_type| job_type.as_str().to_string())
                .collect::<Vec<_>>();
            sqlx::query_scalar!(
                r#"WITH eligible_resource_jobs AS MATERIALIZED (
                    SELECT
                        jq.id,
                        jq.execution_resource_key,
                        jq.priority,
                        jq.next_run_at,
                        jq.created_at
                    FROM job_queue jq
                    WHERE jq.status = 'PENDING'
                      AND jq.next_run_at <= now()
                      AND jq.job_type = ANY($4::text[])
                      AND jq.execution_resource_key IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM job_execution_resource_claims rc
                          WHERE rc.resource_key = jq.execution_resource_key
                      )
                    ORDER BY
                        jq.priority DESC,
                        jq.next_run_at ASC,
                        jq.created_at ASC,
                        jq.id ASC
                    LIMIT $5
                 ),
                 resource_heads AS MATERIALIZED (
                    SELECT DISTINCT ON (eligible.execution_resource_key)
                        eligible.id
                    FROM eligible_resource_jobs eligible
                    ORDER BY
                        eligible.execution_resource_key,
                        eligible.priority DESC,
                        eligible.next_run_at ASC,
                        eligible.created_at ASC,
                        eligible.id ASC
                 ),
                 candidates AS MATERIALIZED (
                    SELECT
                        jq.id,
                        jq.execution_resource_key,
                        jq.run_number,
                        jq.attempt,
                        jq.priority,
                        jq.next_run_at,
                        jq.created_at
                    FROM job_queue jq
                    WHERE jq.status = 'PENDING'
                      AND jq.next_run_at <= now()
                      AND jq.job_type = ANY($4::text[])
                      AND (
                          jq.execution_resource_key IS NULL
                          OR (
                              NOT EXISTS (
                                  SELECT 1
                                  FROM job_execution_resource_claims rc
                                  WHERE rc.resource_key = jq.execution_resource_key
                              )
                              AND jq.id IN (SELECT id FROM resource_heads)
                          )
                      )
                    ORDER BY
                        jq.priority DESC,
                        jq.next_run_at ASC,
                        jq.created_at ASC,
                        jq.id ASC
                    FOR UPDATE OF jq SKIP LOCKED
                    LIMIT $1
                 ),
                 acquired AS (
                    INSERT INTO job_execution_resource_claims (
                        resource_key,
                        job_id,
                        run_number,
                        attempt,
                        worker_id,
                        lease_expires_at
                    )
                    SELECT
                        execution_resource_key,
                        id,
                        run_number,
                        attempt + 1,
                        $2,
                        now() + make_interval(secs => $3::int4)
                    FROM candidates
                    WHERE execution_resource_key IS NOT NULL
                    ORDER BY execution_resource_key
                    ON CONFLICT DO NOTHING
                    RETURNING job_id
                 )
                 SELECT c.id AS "id!"
                 FROM candidates c
                 LEFT JOIN acquired a ON a.job_id = c.id
                 WHERE c.execution_resource_key IS NULL OR a.job_id IS NOT NULL
                 ORDER BY
                    c.priority DESC,
                    c.next_run_at ASC,
                    c.created_at ASC,
                    c.id ASC"#,
                request.limit,
                request.worker_id,
                request.lease_duration_seconds,
                allowed_job_types.as_slice(),
                resource_head_window,
            )
            .fetch_all(&mut **tx)
            .await
        }
        None => {
            sqlx::query_scalar!(
                r#"WITH eligible_resource_jobs AS MATERIALIZED (
                    SELECT
                        jq.id,
                        jq.execution_resource_key,
                        jq.priority,
                        jq.next_run_at,
                        jq.created_at
                    FROM job_queue jq
                    WHERE jq.status = 'PENDING'
                      AND jq.next_run_at <= now()
                      AND jq.execution_resource_key IS NOT NULL
                      AND NOT EXISTS (
                          SELECT 1
                          FROM job_execution_resource_claims rc
                          WHERE rc.resource_key = jq.execution_resource_key
                      )
                    ORDER BY
                        jq.priority DESC,
                        jq.next_run_at ASC,
                        jq.created_at ASC,
                        jq.id ASC
                    LIMIT $4
                 ),
                 resource_heads AS MATERIALIZED (
                    SELECT DISTINCT ON (eligible.execution_resource_key)
                        eligible.id
                    FROM eligible_resource_jobs eligible
                    ORDER BY
                        eligible.execution_resource_key,
                        eligible.priority DESC,
                        eligible.next_run_at ASC,
                        eligible.created_at ASC,
                        eligible.id ASC
                 ),
                 candidates AS MATERIALIZED (
                    SELECT
                        jq.id,
                        jq.execution_resource_key,
                        jq.run_number,
                        jq.attempt,
                        jq.priority,
                        jq.next_run_at,
                        jq.created_at
                    FROM job_queue jq
                    WHERE jq.status = 'PENDING'
                      AND jq.next_run_at <= now()
                      AND (
                          jq.execution_resource_key IS NULL
                          OR (
                              NOT EXISTS (
                                  SELECT 1
                                  FROM job_execution_resource_claims rc
                                  WHERE rc.resource_key = jq.execution_resource_key
                              )
                              AND jq.id IN (SELECT id FROM resource_heads)
                          )
                      )
                    ORDER BY
                        jq.priority DESC,
                        jq.next_run_at ASC,
                        jq.created_at ASC,
                        jq.id ASC
                    FOR UPDATE OF jq SKIP LOCKED
                    LIMIT $1
                 ),
                 acquired AS (
                    INSERT INTO job_execution_resource_claims (
                        resource_key,
                        job_id,
                        run_number,
                        attempt,
                        worker_id,
                        lease_expires_at
                    )
                    SELECT
                        execution_resource_key,
                        id,
                        run_number,
                        attempt + 1,
                        $2,
                        now() + make_interval(secs => $3::int4)
                    FROM candidates
                    WHERE execution_resource_key IS NOT NULL
                    ORDER BY execution_resource_key
                    ON CONFLICT DO NOTHING
                    RETURNING job_id
                 )
                 SELECT c.id AS "id!"
                 FROM candidates c
                 LEFT JOIN acquired a ON a.job_id = c.id
                 WHERE c.execution_resource_key IS NULL OR a.job_id IS NOT NULL
                 ORDER BY
                    c.priority DESC,
                    c.next_run_at ASC,
                    c.created_at ASC,
                    c.id ASC"#,
                request.limit,
                request.worker_id,
                request.lease_duration_seconds,
                resource_head_window,
            )
            .fetch_all(&mut **tx)
            .await
        }
    };

    query_result
        .map_err(|error| Error::from_query_sqlx_with_context("claim jobs candidate list", error))
}

fn resource_head_window_limit(claim_limit: i64) -> i64 {
    claim_limit
        .saturating_mul(RESOURCE_HEAD_WINDOW_PER_CLAIM)
        .clamp(MIN_RESOURCE_HEAD_WINDOW, MAX_RESOURCE_HEAD_WINDOW)
}

pub(super) async fn release_expired_execution_resource_claims_tx(
    tx: &mut DbTx<'_>,
    limit: i64,
) -> Result<u64> {
    let released = sqlx::query(
        "WITH expired AS MATERIALIZED (
            SELECT resource_key
            FROM job_execution_resource_claims
            WHERE (
                release_after IS NOT NULL
                AND release_after <= clock_timestamp()
            )
            OR (
                release_after IS NULL
                AND lease_expires_at <= clock_timestamp()
            )
            ORDER BY COALESCE(release_after, lease_expires_at) ASC, resource_key ASC
            FOR UPDATE SKIP LOCKED
            LIMIT $1
         )
         DELETE FROM job_execution_resource_claims claim
         USING expired
         WHERE claim.resource_key = expired.resource_key
           AND (
               claim.release_after IS NOT NULL
               OR NOT EXISTS (
                   SELECT 1
                   FROM job_queue jq
                   WHERE jq.id = claim.job_id
                     AND jq.run_number = claim.run_number
                     AND jq.attempt = claim.attempt
                     AND jq.worker_id = claim.worker_id
                     AND jq.status = 'LEASED'
               )
           )",
    )
    .bind(limit)
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("release expired execution resource claims", error)
    })?
    .rows_affected();
    Ok(released)
}

#[cfg(test)]
mod tests {
    use super::resource_head_window_limit;

    #[test]
    fn resource_head_window_scales_with_claim_size_within_fixed_bounds() {
        assert_eq!(resource_head_window_limit(1), 1_024);
        assert_eq!(resource_head_window_limit(16), 1_024);
        assert_eq!(resource_head_window_limit(100), 6_400);
        assert_eq!(resource_head_window_limit(i64::MAX), 16_384);
    }
}
