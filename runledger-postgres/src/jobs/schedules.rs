use chrono::{DateTime, Utc};
use cron::Schedule;
use sqlx::types::Uuid;
use std::str::FromStr;

use crate::{DbPool, DbTx, Error, QueryError, QueryErrorCategory, Result};

use super::row_decode::parse_job_type_name;
use super::types::{
    JOB_SCHEDULE_MAX_JITTER_SECONDS, JobScheduleCatalogSyncEntry, JobScheduleCatalogSyncReport,
    JobScheduleRecord, JobScheduleUpsert,
};

const SCHEDULE_EXACT_SYNC_LOCK_TIMEOUT: &str = "5s";
const SCHEDULE_EXACT_SYNC_LOCK_TIMEOUT_MS: i64 = 5_000;
const SCHEDULE_EXACT_SYNC_STATEMENT_TIMEOUT: &str = "30s";
const SCHEDULE_EXACT_SYNC_STATEMENT_TIMEOUT_MS: i64 = 30_000;

#[derive(sqlx::FromRow)]
struct JobScheduleRow {
    id: Uuid,
    name: String,
    job_type: String,
    organization_id: Option<Uuid>,
    payload_template: serde_json::Value,
    cron_expr: String,
    is_active: bool,
    max_jitter_seconds: i32,
    next_fire_at: DateTime<Utc>,
}

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
/// including when the referenced job definition row does not exist.
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
/// exist.
pub async fn upsert_job_schedule_tx(
    tx: &mut DbTx<'_>,
    payload: &JobScheduleUpsert<'_>,
) -> Result<JobScheduleRecord> {
    validate_job_schedule_upsert(payload)?;

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
/// update.
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
/// PostgreSQL rejects the update.
pub async fn set_job_schedule_active_tx(
    tx: &mut DbTx<'_>,
    name: &str,
    is_active: bool,
) -> Result<bool> {
    validate_job_schedule_name(name)?;

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

/// Upserts catalog-owned schedules inside an existing transaction.
///
/// On conflict, catalog sync preserves insert-only fields such as
/// `organization_id`, preserves the scheduler cursor unless the cron expression
/// changes, and applies each entry's `is_active` value as the desired active
/// state. This differs from [`upsert_job_schedule_tx`], which intentionally
/// preserves the stored active state on conflict.
///
/// Entries are upserted one at a time so callers can add per-entry error context
/// around failures.
///
/// # Errors
/// Returns an error if any entry fails validation or persistence. The caller is
/// responsible for adding per-entry context if it needs to report the failing
/// schedule name.
pub async fn sync_catalog_job_schedules_tx(
    tx: &mut DbTx<'_>,
    entries: &[JobScheduleCatalogSyncEntry<'_>],
) -> Result<JobScheduleCatalogSyncReport> {
    let mut synced_schedule_names = Vec::with_capacity(entries.len());
    for entry in entries {
        let schedule = upsert_catalog_job_schedule_tx(tx, &entry.upsert).await?;
        synced_schedule_names.push(schedule.name);
    }

    Ok(JobScheduleCatalogSyncReport {
        synced_schedule_names,
    })
}

/// Applies transaction-local bounds and locks `job_schedules` for exact
/// schedule sync.
///
/// Call this before catalog exact schedule upserts and absent-schedule
/// deactivation. The lock serializes overlapping exact syncs for the same table
/// and prevents additive schedule writes from interleaving with the exact sync
/// window. Scheduler claims acquire their table-level write lock before row
/// locks, so due-schedule claims and fire-cursor updates can also wait behind
/// this lock instead of deadlocking with exact sync. It caps `lock_timeout` at
/// 5 seconds and `statement_timeout` at 30
/// seconds for the current transaction, preserving stricter caller settings.
/// The `lock_timeout` cap is restored after the table lock is acquired; the
/// `statement_timeout` cap intentionally remains active until the transaction
/// ends so the whole exact-sync critical section stays bounded. Call this in a
/// short-lived transaction dedicated to exact schedule sync.
///
/// # Errors
/// Returns an error if PostgreSQL rejects the timeout update or table lock.
pub async fn prepare_schedule_exact_sync_critical_section_tx(tx: &mut DbTx<'_>) -> Result<()> {
    cap_local_statement_timeout_tx(
        tx,
        SCHEDULE_EXACT_SYNC_STATEMENT_TIMEOUT,
        SCHEDULE_EXACT_SYNC_STATEMENT_TIMEOUT_MS,
        "set exact schedule sync statement timeout",
    )
    .await?;
    lock_job_schedules_for_schedule_exact_sync_tx(tx).await
}

async fn upsert_catalog_job_schedule_tx(
    tx: &mut DbTx<'_>,
    payload: &JobScheduleUpsert<'_>,
) -> Result<JobScheduleRecord> {
    validate_job_schedule_upsert(payload)?;

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
                is_active = EXCLUDED.is_active,
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
    .map_err(|error| Error::from_query_sqlx_with_context("sync catalog job schedule", error))?;

    job_schedule_from_row(row)
}

/// Deactivates enabled schedules whose names are in `scope_names` but absent
/// from `present_names`.
///
/// Schedules outside `scope_names` are never modified.
///
/// # Errors
/// Returns an error if PostgreSQL rejects the update.
pub async fn deactivate_schedules_absent_from_names_tx(
    tx: &mut DbTx<'_>,
    scope_names: &[String],
    present_names: &[String],
) -> Result<Vec<String>> {
    if scope_names.is_empty() {
        return Ok(Vec::new());
    }

    let mut rows = sqlx::query_scalar::<_, String>(
        "UPDATE job_schedules
         SET is_active = false,
             updated_at = now()
         WHERE is_active = true
           AND name = ANY($1::text[])
           AND name <> ALL($2::text[])
         RETURNING name",
    )
    .bind(scope_names)
    .bind(present_names)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("deactivate absent catalog schedules", error)
    })?;

    rows.sort();
    Ok(rows)
}

async fn lock_job_schedules_for_schedule_exact_sync_tx(tx: &mut DbTx<'_>) -> Result<()> {
    let previous_lock_timeout = cap_local_lock_timeout_tx(
        tx,
        SCHEDULE_EXACT_SYNC_LOCK_TIMEOUT,
        SCHEDULE_EXACT_SYNC_LOCK_TIMEOUT_MS,
        "set exact schedule sync lock timeout",
    )
    .await?;

    let lock_result = sqlx::query("LOCK TABLE job_schedules IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut **tx)
        .await;

    match lock_result {
        Ok(_) => {
            // After the table lock is held, restore the caller's lock timeout so
            // only lock acquisition gets the exact-sync cap. The transaction's
            // statement timeout still bounds the following sync statements.
            set_local_lock_timeout_tx(
                tx,
                &previous_lock_timeout,
                "restore exact schedule sync lock timeout",
            )
            .await
        }
        Err(error) => {
            // No restore is needed on the error path: the caller rolls back the
            // transaction and PostgreSQL discards the SET LOCAL lock_timeout.
            Err(Error::from_query_sqlx_with_context(
                "lock job schedules for exact schedule sync",
                error,
            ))
        }
    }
}

async fn cap_local_lock_timeout_tx(
    tx: &mut DbTx<'_>,
    lock_timeout: &str,
    lock_timeout_ms: i64,
    context: &'static str,
) -> Result<String> {
    sqlx::query_scalar::<_, String>(
        "WITH previous AS MATERIALIZED (
             SELECT
                current_setting('lock_timeout') AS lock_timeout,
                setting::bigint AS lock_timeout_ms
             FROM pg_settings
             WHERE name = 'lock_timeout'
         )
         SELECT previous.lock_timeout
         FROM previous,
              LATERAL (
                SELECT set_config(
                    'lock_timeout',
                    CASE
                        WHEN previous.lock_timeout_ms = 0 THEN $1
                        WHEN previous.lock_timeout_ms <= $2 THEN previous.lock_timeout
                        ELSE $1
                    END,
                    true
                )
              ) AS applied",
    )
    .bind(lock_timeout)
    .bind(lock_timeout_ms)
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(context, error))
}

async fn cap_local_statement_timeout_tx(
    tx: &mut DbTx<'_>,
    statement_timeout: &str,
    statement_timeout_ms: i64,
    context: &'static str,
) -> Result<()> {
    sqlx::query_scalar::<_, String>(
        "WITH previous AS MATERIALIZED (
             SELECT
                current_setting('statement_timeout') AS statement_timeout,
                setting::bigint AS statement_timeout_ms
             FROM pg_settings
             WHERE name = 'statement_timeout'
         )
         SELECT previous.statement_timeout
         FROM previous,
              LATERAL (
                SELECT set_config(
                    'statement_timeout',
                    CASE
                        WHEN previous.statement_timeout_ms = 0 THEN $1
                        WHEN previous.statement_timeout_ms <= $2 THEN previous.statement_timeout
                        ELSE $1
                    END,
                    true
                )
              ) AS applied",
    )
    .bind(statement_timeout)
    .bind(statement_timeout_ms)
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(context, error))?;

    Ok(())
}

async fn set_local_lock_timeout_tx(
    tx: &mut DbTx<'_>,
    lock_timeout: &str,
    context: &'static str,
) -> Result<()> {
    sqlx::query_scalar::<_, String>("SELECT set_config('lock_timeout', $1, true)")
        .bind(lock_timeout)
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| Error::from_query_sqlx_with_context(context, error))?;

    Ok(())
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

/// Claims due schedules for runtime materialization inside an existing transaction.
///
/// This is a low-level runtime helper used by `runledger-runtime`'s scheduler
/// loop. It selects active schedules with `next_fire_at <= now`, ordered by
/// `next_fire_at`, using `FOR UPDATE SKIP LOCKED` so concurrent scheduler loops
/// do not materialize the same schedule row.
///
/// The claim takes the scheduler's `ROW EXCLUSIVE` table lock before row locks.
/// That lock is compatible with other scheduler workers, but conflicts with
/// exact schedule sync's table lock so claims wait before holding claimed rows.
///
/// Most applications should create schedules with [`upsert_job_schedule`] and
/// run schedule materialization through `runledger_runtime::Supervisor` instead
/// of calling this helper directly.
///
/// # Errors
/// Returns an error if PostgreSQL rejects the claim query or if a claimed row
/// cannot be decoded into [`JobScheduleRecord`].
pub async fn claim_due_schedules_tx(
    tx: &mut DbTx<'_>,
    now: DateTime<Utc>,
    limit: i64,
) -> Result<Vec<JobScheduleRecord>> {
    lock_job_schedules_for_due_schedule_claim_tx(tx).await?;

    let rows = sqlx::query!(
        "SELECT
            id,
            name,
            job_type,
            organization_id,
            payload_template,
            cron_expr,
            max_jitter_seconds,
            next_fire_at
         FROM job_schedules
         WHERE is_active = true
           AND next_fire_at <= $1
         ORDER BY next_fire_at ASC
         FOR UPDATE SKIP LOCKED
         LIMIT $2",
        now,
        limit,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("claim due schedules", error))?;

    rows.into_iter()
        .map(|row| {
            job_schedule_from_row(JobScheduleRow {
                id: row.id,
                name: row.name,
                job_type: row.job_type,
                organization_id: row.organization_id,
                payload_template: row.payload_template,
                cron_expr: row.cron_expr,
                is_active: true,
                max_jitter_seconds: row.max_jitter_seconds,
                next_fire_at: row.next_fire_at,
            })
        })
        .collect::<Result<Vec<_>>>()
}

async fn lock_job_schedules_for_due_schedule_claim_tx(tx: &mut DbTx<'_>) -> Result<()> {
    sqlx::query("LOCK TABLE job_schedules IN ROW EXCLUSIVE MODE")
        .execute(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context(
                "lock job schedules before claiming due schedules",
                error,
            )
        })?;

    Ok(())
}

/// Records a successful schedule materialization inside an existing transaction.
///
/// This is a low-level runtime helper used by `runledger-runtime` after a due
/// schedule has produced its job. It updates `last_fired_at` and advances
/// `next_fire_at` to the caller-computed UTC cursor.
///
/// Pass the [`JobScheduleRecord::id`] returned by [`claim_due_schedules_tx`].
/// Returns `true` when that schedule row still existed and was updated, and
/// `false` when no row matched `schedule_id`.
///
/// Most applications should let `runledger_runtime::Supervisor` call this as
/// part of the scheduler loop instead of calling it directly.
///
/// # Errors
/// Returns an error if PostgreSQL rejects the update. A missing schedule row is
/// reported as `Ok(false)`, not as an error.
pub async fn mark_schedule_fired_tx(
    tx: &mut DbTx<'_>,
    schedule_id: Uuid,
    fired_at: DateTime<Utc>,
    next_fire_at: DateTime<Utc>,
) -> Result<bool> {
    let result = sqlx::query!(
        "UPDATE job_schedules
         SET last_fired_at = $2,
             next_fire_at = $3,
             updated_at = now()
         WHERE id = $1",
        schedule_id,
        fired_at,
        next_fire_at,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("mark schedule fired", error))?;

    Ok(result.rows_affected() > 0)
}

fn job_schedule_from_row(row: JobScheduleRow) -> Result<JobScheduleRecord> {
    Ok(JobScheduleRecord {
        id: row.id,
        name: row.name,
        job_type: parse_job_type_name(row.job_type)?,
        organization_id: row.organization_id,
        payload_template: row.payload_template,
        cron_expr: row.cron_expr,
        is_active: row.is_active,
        max_jitter_seconds: row.max_jitter_seconds,
        next_fire_at: row.next_fire_at,
    })
}

fn validate_job_schedule_upsert(payload: &JobScheduleUpsert<'_>) -> Result<()> {
    validate_job_schedule_name(payload.name)?;

    if payload.cron_expr.trim().is_empty() {
        return Err(job_schedule_validation_error(
            "job_schedule.invalid_cron",
            "Job schedule cron expression must be non-empty.",
            "job schedule cron expression is blank",
        ));
    }

    if payload.cron_expr != payload.cron_expr.trim() {
        return Err(job_schedule_validation_error(
            "job_schedule.invalid_cron",
            "Job schedule cron expression must not have surrounding whitespace.",
            "job schedule cron expression has surrounding whitespace",
        ));
    }

    if Schedule::from_str(payload.cron_expr).is_err() {
        return Err(job_schedule_validation_error(
            "job_schedule.invalid_cron",
            "Job schedule cron expression must be valid.",
            "job schedule cron expression is invalid",
        ));
    }

    if payload.max_jitter_seconds < 0 {
        return Err(job_schedule_validation_error(
            "job_schedule.invalid_jitter",
            "Job schedule jitter must be non-negative.",
            "job schedule max_jitter_seconds is negative",
        ));
    }

    if payload.max_jitter_seconds > JOB_SCHEDULE_MAX_JITTER_SECONDS {
        return Err(job_schedule_validation_error(
            "job_schedule.invalid_jitter",
            "Job schedule jitter must not exceed 86400 seconds (24h).",
            "job schedule max_jitter_seconds exceeds 86400 seconds",
        ));
    }

    Ok(())
}

fn validate_job_schedule_name(name: &str) -> Result<()> {
    if name.trim().is_empty() {
        return Err(job_schedule_validation_error(
            "job_schedule.invalid_name",
            "Job schedule name must be non-empty.",
            "job schedule name is blank",
        ));
    }

    if name != name.trim() {
        return Err(job_schedule_validation_error(
            "job_schedule.invalid_name",
            "Job schedule name must not have surrounding whitespace.",
            "job schedule name has surrounding whitespace",
        ));
    }

    Ok(())
}

fn job_schedule_validation_error(
    code: &'static str,
    client_message: &'static str,
    internal_message: impl Into<String>,
) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        code,
        client_message,
        internal_message,
    ))
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use runledger_core::jobs::JobType;
    use serde_json::json;

    use super::{JOB_SCHEDULE_MAX_JITTER_SECONDS, JobScheduleUpsert, validate_job_schedule_upsert};
    use crate::{Error, QueryErrorCategory};

    fn valid_schedule<'a>(payload_template: &'a serde_json::Value) -> JobScheduleUpsert<'a> {
        JobScheduleUpsert {
            name: "daily-refresh",
            job_type: JobType::new("jobs.refresh"),
            organization_id: None,
            payload_template,
            cron_expr: "0 0 0 * * *",
            is_active: true,
            next_fire_at: Utc::now(),
            max_jitter_seconds: 0,
        }
    }

    fn assert_validation_code(payload: JobScheduleUpsert<'_>, expected_code: &str) {
        let error = validate_job_schedule_upsert(&payload)
            .expect_err("invalid schedule payload should fail validation");

        match error {
            Error::QueryError(query_error) => {
                assert_eq!(query_error.category(), QueryErrorCategory::Validation);
                assert_eq!(query_error.code(), expected_code);
            }
            other => panic!("expected query validation error, got {other:?}"),
        }
    }

    #[test]
    fn validates_schedule_upsert_payload() {
        let payload_template = json!({});
        validate_job_schedule_upsert(&valid_schedule(&payload_template))
            .expect("valid schedule payload should pass validation");

        let mut blank_name = valid_schedule(&payload_template);
        blank_name.name = " ";
        assert_validation_code(blank_name, "job_schedule.invalid_name");

        let mut padded_name = valid_schedule(&payload_template);
        padded_name.name = " daily-refresh ";
        assert_validation_code(padded_name, "job_schedule.invalid_name");

        let mut blank_cron = valid_schedule(&payload_template);
        blank_cron.cron_expr = " ";
        assert_validation_code(blank_cron, "job_schedule.invalid_cron");

        let mut padded_cron = valid_schedule(&payload_template);
        padded_cron.cron_expr = " 0 0 0 * * * ";
        assert_validation_code(padded_cron, "job_schedule.invalid_cron");

        let mut invalid_cron = valid_schedule(&payload_template);
        invalid_cron.cron_expr = "not a cron expression";
        assert_validation_code(invalid_cron, "job_schedule.invalid_cron");

        let mut negative_jitter = valid_schedule(&payload_template);
        negative_jitter.max_jitter_seconds = -1;
        assert_validation_code(negative_jitter, "job_schedule.invalid_jitter");

        let mut excessive_jitter = valid_schedule(&payload_template);
        excessive_jitter.max_jitter_seconds = JOB_SCHEDULE_MAX_JITTER_SECONDS + 1;
        assert_validation_code(excessive_jitter, "job_schedule.invalid_jitter");
    }
}
