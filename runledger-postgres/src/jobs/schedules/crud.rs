use chrono::{DateTime, Utc};

use crate::{DbPool, DbTx, Error, Result};

use super::super::schedule_definition_guard::{self, GuardLockContext};
use super::super::types::{JobScheduleRecord, JobScheduleUpsert};
use super::row::{JobScheduleRow, job_schedule_from_row};
use super::validation::{validate_job_schedule_name, validate_job_schedule_upsert};

/// Creates or updates a cron-backed job schedule in its own transaction.
///
/// Schedules are keyed by name. On conflict, this refreshes the schedule
/// definition while preserving scheduler-managed state. `organization_id` and
/// `is_active` are insert-only. `next_fire_at` is preserved unless the cron
/// expression changes, in which case the supplied cursor is stored. Use
/// [`set_job_schedule_active`] to pause or resume an existing schedule and
/// [`set_job_schedule_next_fire_at`] to retime it without changing the
/// definition.
///
/// # Errors
/// Returns an error if a transaction cannot be opened or committed, if
/// [`JobScheduleUpsert`] validation fails, or if PostgreSQL rejects the upsert,
/// including when the referenced job definition row does not exist. Returns a
/// validation error when the upsert would leave the schedule active for a
/// disabled job definition.
pub async fn upsert_job_schedule(
    pool: &DbPool,
    payload: &JobScheduleUpsert<'_>,
) -> Result<JobScheduleRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    let schedule = upsert_job_schedule_tx(&mut tx, payload).await?;
    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    Ok(schedule)
}

/// Creates or updates a cron-backed job schedule inside an existing transaction.
///
/// This has the same conflict semantics as [`upsert_job_schedule`].
///
/// # Errors
/// Returns an error if [`JobScheduleUpsert`] validation fails or if PostgreSQL
/// rejects the upsert, including when the referenced job definition row does not
/// exist. Returns a validation error when the upsert would leave the schedule
/// active for a disabled job definition.
pub async fn upsert_job_schedule_tx(
    tx: &mut DbTx<'_>,
    payload: &JobScheduleUpsert<'_>,
) -> Result<JobScheduleRecord> {
    validate_job_schedule_upsert(payload)?;

    schedule_definition_guard::lock_job_schedules_for_guard_tx(
        tx,
        GuardLockContext::ActiveScheduleWrite,
    )
    .await?;
    if schedule_active_after_plain_upsert_tx(tx, payload.name, payload.is_active).await? {
        schedule_definition_guard::lock_job_definitions_for_guard_tx(
            tx,
            GuardLockContext::ActiveScheduleWrite,
        )
        .await?;
        schedule_definition_guard::reject_unavailable_definition_for_active_schedule_tx(
            tx,
            payload.job_type.as_str(),
        )
        .await?;
    }

    let row = sqlx::query_as::<_, JobScheduleRow>(
        "INSERT INTO job_schedules (
            name,
            job_type,
            organization_id,
            payload_template,
            cron_expr,
            timezone,
            is_active,
            next_fire_at,
            max_jitter_seconds
         )
         VALUES ($1, $2, $3, $4::jsonb, $5, 'UTC', $6, $7, $8)
         ON CONFLICT (name)
         DO UPDATE
            SET job_type = EXCLUDED.job_type,
                payload_template = EXCLUDED.payload_template,
                next_fire_at = CASE
                    WHEN job_schedules.cron_expr IS DISTINCT FROM EXCLUDED.cron_expr
                    THEN EXCLUDED.next_fire_at
                    ELSE job_schedules.next_fire_at
                END,
                cron_expr = EXCLUDED.cron_expr,
                timezone = EXCLUDED.timezone,
                max_jitter_seconds = EXCLUDED.max_jitter_seconds,
                updated_at = now()
         RETURNING
            id,
            name,
            job_type,
            organization_id,
            payload_template,
            cron_expr,
            is_active,
            max_jitter_seconds,
            next_fire_at",
    )
    .bind(payload.name)
    .bind(payload.job_type.as_str())
    .bind(payload.organization_id)
    .bind(payload.payload_template)
    .bind(payload.cron_expr)
    .bind(payload.is_active)
    .bind(payload.next_fire_at)
    .bind(payload.max_jitter_seconds)
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("upsert job schedule", error))?;

    job_schedule_from_row(row)
}

/// Loads a schedule by name.
///
/// Returns `Ok(None)` when no schedule exists for `name`.
///
/// # Errors
/// Returns an error if `name` is blank or has surrounding whitespace, if
/// PostgreSQL rejects the query, or if the stored job type cannot be decoded.
pub async fn get_job_schedule_by_name(
    pool: &DbPool,
    name: &str,
) -> Result<Option<JobScheduleRecord>> {
    validate_job_schedule_name(name)?;

    let row = sqlx::query_as::<_, JobScheduleRow>(
        "SELECT
            id,
            name,
            job_type,
            organization_id,
            payload_template,
            cron_expr,
            is_active,
            max_jitter_seconds,
            next_fire_at
         FROM job_schedules
         WHERE name = $1",
    )
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("get job schedule by name", error))?;

    row.map(job_schedule_from_row).transpose()
}

/// Activates or deactivates a schedule in its own transaction.
///
/// Returns `true` when a schedule row existed for `name`.
///
/// # Errors
/// Returns an error if `name` is blank or has surrounding whitespace, if a
/// transaction cannot be opened or committed, or if PostgreSQL rejects the
/// update. Returns a validation error when activating a schedule whose job
/// definition is disabled.
pub async fn set_job_schedule_active(pool: &DbPool, name: &str, is_active: bool) -> Result<bool> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    let updated = set_job_schedule_active_tx(&mut tx, name, is_active).await?;
    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    Ok(updated)
}

/// Activates or deactivates a schedule inside an existing transaction.
///
/// Returns `true` when a schedule row existed for `name`.
///
/// # Errors
/// Returns an error if `name` is blank or has surrounding whitespace, or if
/// PostgreSQL rejects the update. Returns a validation error when activating a
/// schedule whose job definition is disabled.
pub async fn set_job_schedule_active_tx(
    tx: &mut DbTx<'_>,
    name: &str,
    is_active: bool,
) -> Result<bool> {
    validate_job_schedule_name(name)?;

    if is_active {
        schedule_definition_guard::lock_job_schedules_for_guard_tx(
            tx,
            GuardLockContext::ActiveScheduleWrite,
        )
        .await?;
        let Some(job_type) = job_schedule_job_type_by_name_tx(tx, name).await? else {
            return Ok(false);
        };
        schedule_definition_guard::lock_job_definitions_for_guard_tx(
            tx,
            GuardLockContext::ActiveScheduleWrite,
        )
        .await?;
        schedule_definition_guard::reject_unavailable_definition_for_active_schedule_tx(
            tx, &job_type,
        )
        .await?;
    }

    let result = sqlx::query(
        "UPDATE job_schedules
         SET is_active = $2,
             updated_at = now()
         WHERE name = $1",
    )
    .bind(name)
    .bind(is_active)
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("set job schedule active", error))?;

    Ok(result.rows_affected() > 0)
}

/// Moves a schedule's next fire cursor in its own transaction.
///
/// Returns `true` when a schedule row existed for `name`.
///
/// # Errors
/// Returns an error if `name` is blank or has surrounding whitespace, if a
/// transaction cannot be opened or committed, or if PostgreSQL rejects the
/// update.
pub async fn set_job_schedule_next_fire_at(
    pool: &DbPool,
    name: &str,
    next_fire_at: DateTime<Utc>,
) -> Result<bool> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    let updated = set_job_schedule_next_fire_at_tx(&mut tx, name, next_fire_at).await?;
    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    Ok(updated)
}

/// Moves a schedule's next fire cursor inside an existing transaction.
///
/// Returns `true` when a schedule row existed for `name`.
///
/// # Errors
/// Returns an error if `name` is blank or has surrounding whitespace, or if
/// PostgreSQL rejects the update.
pub async fn set_job_schedule_next_fire_at_tx(
    tx: &mut DbTx<'_>,
    name: &str,
    next_fire_at: DateTime<Utc>,
) -> Result<bool> {
    validate_job_schedule_name(name)?;

    let result = sqlx::query(
        "UPDATE job_schedules
         SET next_fire_at = $2,
             updated_at = now()
         WHERE name = $1",
    )
    .bind(name)
    .bind(next_fire_at)
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("set job schedule next fire at", error))?;

    Ok(result.rows_affected() > 0)
}

async fn schedule_active_after_plain_upsert_tx(
    tx: &mut DbTx<'_>,
    name: &str,
    insert_is_active: bool,
) -> Result<bool> {
    // Plain upserts preserve stored is_active on conflict; only inserts use the
    // payload value. Keep this aligned with upsert_job_schedule_tx.
    let stored_is_active = sqlx::query_scalar::<_, bool>(
        "SELECT is_active
         FROM job_schedules
         WHERE name = $1",
    )
    .bind(name)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("read schedule active state before upsert", error)
    })?;

    Ok(stored_is_active.unwrap_or(insert_is_active))
}

async fn job_schedule_job_type_by_name_tx(tx: &mut DbTx<'_>, name: &str) -> Result<Option<String>> {
    sqlx::query_scalar::<_, String>(
        "SELECT job_type
         FROM job_schedules
         WHERE name = $1",
    )
    .bind(name)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("read schedule job type before activation", error)
    })
}
