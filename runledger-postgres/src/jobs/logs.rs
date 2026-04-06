use sqlx::types::Uuid;

use crate::{DbPool, Error, Result};

use super::types::{JobLogRecord, JobLogRecordInput};

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

pub async fn list_job_logs(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_id: Uuid,
    limit: i64,
    after_id: Option<i64>,
) -> Result<Vec<JobLogRecord>> {
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
           AND ($2::uuid IS NULL OR jq.organization_id = $2)
           AND ($3::bigint IS NULL OR jl.id > $3)
         ORDER BY jl.id ASC
         LIMIT $4",
        job_id,
        organization_id,
        after_id,
        limit,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("list job logs", error))
}
