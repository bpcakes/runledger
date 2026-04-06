use crate::{DbPool, DbTx, Error, Result};
use runledger_core::jobs::JobType;

use super::errors::runtime_config_not_found_error;
use super::row_decode::parse_job_type_name;
use super::types::{JobRuntimeConfigListFilter, JobRuntimeConfigRecord, JobRuntimeConfigUpsert};

pub async fn upsert_job_runtime_config_tx(
    tx: &mut DbTx<'_>,
    payload: &JobRuntimeConfigUpsert<'_>,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_runtime_configs (
            job_type,
            schema_version,
            config,
            updated_by_user_id
         )
         VALUES ($1, $2, $3::jsonb, $4)
         ON CONFLICT (job_type)
         DO UPDATE
            SET schema_version = EXCLUDED.schema_version,
                config = EXCLUDED.config,
                updated_by_user_id = EXCLUDED.updated_by_user_id,
                updated_at = now()",
        payload.job_type as _,
        payload.schema_version,
        payload.config,
        payload.updated_by_user_id,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("upsert job runtime config", error))?;

    Ok(())
}

pub async fn upsert_job_runtime_config(
    pool: &DbPool,
    payload: &JobRuntimeConfigUpsert<'_>,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    upsert_job_runtime_config_tx(&mut tx, payload).await?;
    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    Ok(())
}

pub async fn insert_job_runtime_config_if_missing(
    pool: &DbPool,
    payload: &JobRuntimeConfigUpsert<'_>,
) -> Result<()> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    insert_job_runtime_config_if_missing_tx(&mut tx, payload).await?;
    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    Ok(())
}

pub async fn insert_job_runtime_config_if_missing_tx(
    tx: &mut DbTx<'_>,
    payload: &JobRuntimeConfigUpsert<'_>,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_runtime_configs (
            job_type,
            schema_version,
            config,
            updated_by_user_id
         )
         VALUES ($1, $2, $3::jsonb, $4)
         ON CONFLICT (job_type)
         DO NOTHING",
        payload.job_type as _,
        payload.schema_version,
        payload.config,
        payload.updated_by_user_id,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("insert job runtime config if missing", error)
    })?;

    Ok(())
}

pub async fn get_job_runtime_config_by_type(
    pool: &DbPool,
    job_type: JobType<'_>,
) -> Result<Option<JobRuntimeConfigRecord>> {
    let row = sqlx::query!(
        "SELECT
            job_type,
            schema_version,
            config,
            updated_by_user_id,
            created_at,
            updated_at
         FROM job_runtime_configs
         WHERE job_type = $1
         LIMIT 1",
        job_type as _,
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("get job runtime config by type", error)
    })?;

    row.map(|row| {
        Ok(JobRuntimeConfigRecord {
            job_type: parse_job_type_name(row.job_type)?,
            schema_version: row.schema_version,
            config: row.config,
            updated_by_user_id: row.updated_by_user_id,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    })
    .transpose()
}

pub async fn get_required_job_runtime_config_by_type(
    pool: &DbPool,
    job_type: JobType<'_>,
) -> Result<JobRuntimeConfigRecord> {
    get_job_runtime_config_by_type(pool, job_type)
        .await?
        .ok_or_else(|| runtime_config_not_found_error(job_type.as_str()))
}

pub async fn list_job_runtime_configs(
    pool: &DbPool,
    filter: &JobRuntimeConfigListFilter<'_>,
) -> Result<Vec<JobRuntimeConfigRecord>> {
    let rows = sqlx::query!(
        "SELECT
            job_type,
            schema_version,
            config,
            updated_by_user_id,
            created_at,
            updated_at
         FROM job_runtime_configs
         WHERE ($1::text IS NULL OR job_type = $1)
         ORDER BY job_type ASC
         LIMIT $2
         OFFSET $3",
        filter.job_type,
        filter.limit,
        filter.offset,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("list job runtime configs", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(JobRuntimeConfigRecord {
                job_type: parse_job_type_name(row.job_type)?,
                schema_version: row.schema_version,
                config: row.config,
                updated_by_user_id: row.updated_by_user_id,
                created_at: row.created_at,
                updated_at: row.updated_at,
            })
        })
        .collect()
}
