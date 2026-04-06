use chrono::{DateTime, Utc};
use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

use super::row_decode::parse_job_type_name;
use super::types::JobScheduleRecord;

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
