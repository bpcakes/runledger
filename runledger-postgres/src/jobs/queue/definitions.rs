use crate::{DbPool, DbTx, Error, Result};
use runledger_core::jobs::JobType;

use super::super::row_decode::parse_job_type_name;
use super::super::types::{
    JobDefinitionListFilter, JobDefinitionRecord, JobDefinitionUpdate, JobDefinitionUpsert,
};

pub async fn upsert_job_definition_tx(
    tx: &mut DbTx<'_>,
    payload: &JobDefinitionUpsert<'_>,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_definitions (
            job_type,
            version,
            max_attempts,
            default_timeout_seconds,
            default_priority,
            is_enabled
         )
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (job_type)
         DO UPDATE
            SET version = EXCLUDED.version,
                max_attempts = EXCLUDED.max_attempts,
                default_timeout_seconds = EXCLUDED.default_timeout_seconds,
                default_priority = EXCLUDED.default_priority,
                is_enabled = EXCLUDED.is_enabled,
                updated_at = now()",
        payload.job_type as _,
        payload.version,
        payload.max_attempts,
        payload.default_timeout_seconds,
        payload.default_priority,
        payload.is_enabled,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("upsert job definition", error))?;

    Ok(())
}

pub async fn insert_job_definition_if_missing_tx(
    tx: &mut DbTx<'_>,
    payload: &JobDefinitionUpsert<'_>,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_definitions (
            job_type,
            version,
            max_attempts,
            default_timeout_seconds,
            default_priority,
            is_enabled
         )
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (job_type)
         DO NOTHING",
        payload.job_type as _,
        payload.version,
        payload.max_attempts,
        payload.default_timeout_seconds,
        payload.default_priority,
        payload.is_enabled,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("insert job definition if missing", error)
    })?;

    Ok(())
}

pub async fn list_job_definitions(
    pool: &DbPool,
    filter: &JobDefinitionListFilter<'_>,
) -> Result<Vec<JobDefinitionRecord>> {
    let escaped_job_type = filter.job_type.map(escape_ilike_pattern);

    let rows = sqlx::query!(
        "SELECT
            job_type,
            version,
            max_attempts,
            default_timeout_seconds,
            default_priority,
            is_enabled,
            created_at,
            updated_at
         FROM job_definitions
         WHERE ($1::text IS NULL OR job_type ILIKE '%' || $1 || '%')
         ORDER BY job_type ASC
         LIMIT $2
         OFFSET $3",
        escaped_job_type.as_deref(),
        filter.limit,
        filter.offset,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("list job definitions", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(JobDefinitionRecord {
                job_type: parse_job_type_name(row.job_type)?,
                version: row.version,
                max_attempts: row.max_attempts,
                default_timeout_seconds: row.default_timeout_seconds,
                default_priority: row.default_priority,
                is_enabled: row.is_enabled,
                created_at: row.created_at,
                updated_at: row.updated_at,
            })
        })
        .collect()
}

fn escape_ilike_pattern(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

pub async fn get_job_definition_by_type(
    pool: &DbPool,
    job_type: JobType<'_>,
) -> Result<Option<JobDefinitionRecord>> {
    let row = sqlx::query!(
        "SELECT
            job_type,
            version,
            max_attempts,
            default_timeout_seconds,
            default_priority,
            is_enabled,
            created_at,
            updated_at
         FROM job_definitions
         WHERE job_type = $1
         LIMIT 1",
        job_type as _,
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("get job definition by type", error))?;

    row.map(|row| {
        Ok(JobDefinitionRecord {
            job_type: parse_job_type_name(row.job_type)?,
            version: row.version,
            max_attempts: row.max_attempts,
            default_timeout_seconds: row.default_timeout_seconds,
            default_priority: row.default_priority,
            is_enabled: row.is_enabled,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    })
    .transpose()
}

pub async fn update_job_definition(
    pool: &DbPool,
    job_type: JobType<'_>,
    payload: &JobDefinitionUpdate,
) -> Result<Option<JobDefinitionRecord>> {
    let row = sqlx::query!(
        "UPDATE job_definitions
         SET max_attempts = COALESCE($2, max_attempts),
             default_timeout_seconds = COALESCE($3, default_timeout_seconds),
             default_priority = COALESCE($4, default_priority),
             is_enabled = COALESCE($5, is_enabled),
             updated_at = now()
         WHERE job_type = $1
         RETURNING
            job_type,
            version,
            max_attempts,
            default_timeout_seconds,
            default_priority,
            is_enabled,
            created_at,
            updated_at",
        job_type as _,
        payload.max_attempts,
        payload.default_timeout_seconds,
        payload.default_priority,
        payload.is_enabled,
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("update job definition", error))?;

    row.map(|row| {
        Ok(JobDefinitionRecord {
            job_type: parse_job_type_name(row.job_type)?,
            version: row.version,
            max_attempts: row.max_attempts,
            default_timeout_seconds: row.default_timeout_seconds,
            default_priority: row.default_priority,
            is_enabled: row.is_enabled,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    })
    .transpose()
}
