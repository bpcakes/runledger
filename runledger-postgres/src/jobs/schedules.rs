use chrono::{DateTime, Utc};
use cron::Schedule;
use sqlx::types::Uuid;
use std::str::FromStr;

use crate::{DbPool, DbTx, Error, QueryError, QueryErrorCategory, Result};

use super::row_decode::parse_job_type_name;
use super::types::{JobScheduleRecord, JobScheduleUpsert};

const MAX_SCHEDULE_JITTER_SECONDS: i32 = 86_400;

#[derive(sqlx::FromRow)]
struct JobScheduleRow {
    id: Uuid,
    name: String,
    job_type: String,
    organization_id: Option<Uuid>,
    payload_template: serde_json::Value,
    cron_expr: String,
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

/// Activates or deactivates a schedule in its own transaction.
///
/// Returns `true` when a schedule row existed for `name`.
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

/// Moves a schedule's next fire cursor in its own transaction.
///
/// Returns `true` when a schedule row existed for `name`.
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

pub async fn claim_due_schedules_tx(
    tx: &mut DbTx<'_>,
    now: DateTime<Utc>,
    limit: i64,
) -> Result<Vec<JobScheduleRecord>> {
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
                max_jitter_seconds: row.max_jitter_seconds,
                next_fire_at: row.next_fire_at,
            })
        })
        .collect::<Result<Vec<_>>>()
}

pub async fn mark_schedule_fired_tx(
    tx: &mut DbTx<'_>,
    schedule_id: Uuid,
    fired_at: DateTime<Utc>,
    next_fire_at: DateTime<Utc>,
) -> Result<()> {
    sqlx::query!(
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

    Ok(())
}

fn job_schedule_from_row(row: JobScheduleRow) -> Result<JobScheduleRecord> {
    Ok(JobScheduleRecord {
        id: row.id,
        name: row.name,
        job_type: parse_job_type_name(row.job_type)?,
        organization_id: row.organization_id,
        payload_template: row.payload_template,
        cron_expr: row.cron_expr,
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

    if payload.max_jitter_seconds > MAX_SCHEDULE_JITTER_SECONDS {
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

    use super::{JobScheduleUpsert, MAX_SCHEDULE_JITTER_SECONDS, validate_job_schedule_upsert};
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
        excessive_jitter.max_jitter_seconds = MAX_SCHEDULE_JITTER_SECONDS + 1;
        assert_validation_code(excessive_jitter, "job_schedule.invalid_jitter");
    }
}
