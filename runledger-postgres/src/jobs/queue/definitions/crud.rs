use runledger_core::jobs::JobType;

use super::super::super::errors::validate_pagination;
use super::super::super::row_decode::parse_job_type_name;
use super::super::super::schedule_definition_guard::{
    self, GuardLockContext, ScheduleDefinitionLockError,
};
use super::super::super::types::{
    JobDefinitionListFilter, JobDefinitionRecord, JobDefinitionUpdate, JobDefinitionUpsert,
};
use crate::{DbPool, DbTx, Error, Result};

/// Creates or updates a job definition inside an existing transaction.
///
/// # Errors
/// Returns an error if PostgreSQL rejects the upsert, or if disabling the
/// definition would leave an active schedule referencing this job type.
pub async fn upsert_job_definition_tx(
    tx: &mut DbTx<'_>,
    payload: &JobDefinitionUpsert<'_>,
) -> Result<()> {
    if !payload.is_enabled {
        prepare_definition_disable_update_guard_tx(tx).await?;
        reject_active_schedule_for_disabled_job_type_update_tx(tx, payload.job_type.as_str())
            .await?;
    }

    apply_job_definition_upsert_tx(tx, payload).await
}

pub(super) async fn apply_job_definition_upsert_tx(
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
                updated_at = now()
          WHERE job_definitions.version IS DISTINCT FROM EXCLUDED.version
             OR job_definitions.max_attempts IS DISTINCT FROM EXCLUDED.max_attempts
             OR job_definitions.default_timeout_seconds IS DISTINCT FROM EXCLUDED.default_timeout_seconds
             OR job_definitions.default_priority IS DISTINCT FROM EXCLUDED.default_priority
             OR job_definitions.is_enabled IS DISTINCT FROM EXCLUDED.is_enabled",
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

/// Upserts a job definition while preserving an existing row's `is_enabled`.
///
/// Inserts use `payload.is_enabled`; updates keep the stored enabled state and
/// refresh only the catalog-owned version, retry, timeout, and priority fields.
pub(super) async fn upsert_job_definition_preserving_enabled_tx(
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
                is_enabled = job_definitions.is_enabled,
                updated_at = now()
          WHERE job_definitions.version IS DISTINCT FROM EXCLUDED.version
             OR job_definitions.max_attempts IS DISTINCT FROM EXCLUDED.max_attempts
             OR job_definitions.default_timeout_seconds IS DISTINCT FROM EXCLUDED.default_timeout_seconds
             OR job_definitions.default_priority IS DISTINCT FROM EXCLUDED.default_priority",
        payload.job_type as _,
        payload.version,
        payload.max_attempts,
        payload.default_timeout_seconds,
        payload.default_priority,
        // Used only by the INSERT path; conflicts preserve the stored value.
        payload.is_enabled,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("upsert job definition preserving enabled", error)
    })?;

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
    validate_pagination(filter.limit, filter.offset)?;

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

async fn prepare_definition_disable_update_guard_tx(tx: &mut DbTx<'_>) -> Result<()> {
    schedule_definition_guard::cap_definition_disable_statement_timeout_tx(tx).await?;
    schedule_definition_guard::lock_schedules_then_definitions_tx(
        tx,
        GuardLockContext::DefinitionDisable,
    )
    .await
    .map_err(ScheduleDefinitionLockError::into_error)
}

async fn reject_active_schedule_for_disabled_job_type_update_tx(
    tx: &mut DbTx<'_>,
    job_type: &str,
) -> Result<()> {
    if let Some(reference) =
        schedule_definition_guard::find_active_schedule_for_job_type_tx(tx, job_type).await?
    {
        return Err(
            schedule_definition_guard::active_schedule_for_disabled_definition_error(&reference),
        );
    }

    Ok(())
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

/// Updates mutable operator-owned fields on a job definition.
///
/// Returns `Ok(None)` when no definition exists for `job_type`.
///
/// # Errors
/// Returns an error if a transaction cannot be opened or committed, if
/// PostgreSQL rejects the update, or if disabling the definition would leave an
/// active schedule referencing this job type.
pub async fn update_job_definition(
    pool: &DbPool,
    job_type: JobType<'_>,
    payload: &JobDefinitionUpdate,
) -> Result<Option<JobDefinitionRecord>> {
    let mut tx = pool.begin().await.map_err(|error| {
        Error::from_query_sqlx_with_context("begin job definition update transaction", error)
    })?;

    if payload.is_enabled == Some(false) {
        prepare_definition_disable_update_guard_tx(&mut tx).await?;
        reject_active_schedule_for_disabled_job_type_update_tx(&mut tx, job_type.as_str()).await?;
    }

    let record = apply_job_definition_update_tx(&mut tx, job_type, payload).await?;
    tx.commit().await.map_err(|error| {
        Error::from_query_sqlx_with_context("commit job definition update transaction", error)
    })?;

    Ok(record)
}

async fn apply_job_definition_update_tx(
    tx: &mut DbTx<'_>,
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
    .fetch_optional(&mut **tx)
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
