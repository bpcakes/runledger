use sqlx::types::Uuid;

use crate::{DbPool, Error, Result};

use super::errors::validate_page_limit;
use super::types::{JobLogRecord, JobLogRecordInput, JobReadScope};

pub async fn insert_job_log(pool: &DbPool, input: &JobLogRecordInput) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_logs (
            job_id,
            run_number,
            attempt,
            level,
            message,
            payload
         )
         VALUES ($1, $2, $3, $4, $5, $6)",
        input.job_id,
        input.run_number,
        input.attempt,
        input.level,
        input.message,
        input.payload,
    )
    .execute(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert job log", error))?;

    Ok(())
}

/// Legacy read: None matches logs from global and all organization-owned jobs.
/// Prefer [`list_job_logs_with_scope`] for new code.
pub async fn list_job_logs(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_id: Uuid,
    limit: i64,
    after_id: Option<i64>,
) -> Result<Vec<JobLogRecord>> {
    list_job_logs_with_scope(
        pool,
        JobReadScope::from_legacy(organization_id),
        job_id,
        limit,
        after_id,
    )
    .await
}

/// Lists logs within an application-authorized, explicit job visibility scope.
pub async fn list_job_logs_with_scope(
    pool: &DbPool,
    scope: JobReadScope,
    job_id: Uuid,
    limit: i64,
    after_id: Option<i64>,
) -> Result<Vec<JobLogRecord>> {
    let (is_admin, organization_id) = scope.visibility_predicate();
    validate_page_limit(limit)?;

    sqlx::query_as!(
        JobLogRecord,
        "SELECT
            jl.id,
            jl.job_id,
            jl.run_number,
            jl.attempt,
            jl.level,
            jl.message,
            jl.payload,
            jl.occurred_at
         FROM job_logs jl
         JOIN job_queue jq ON jq.id = jl.job_id
         WHERE jl.job_id = $1
           AND ($5::bool OR jq.organization_id IS NOT DISTINCT FROM $2::uuid)
           AND ($3::bigint IS NULL OR jl.id > $3)
         ORDER BY jl.id ASC
         LIMIT $4",
        job_id,
        organization_id,
        after_id,
        limit,
        is_admin,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("list job logs", error))
}
