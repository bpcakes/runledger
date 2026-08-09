use chrono::{DateTime, Utc};
use sqlx::types::Uuid;

use crate::Result;

use super::super::row_decode::parse_job_type_name;
use super::super::types::JobScheduleRecord;

#[derive(sqlx::FromRow)]
pub(super) struct JobScheduleRow {
    pub(super) id: Uuid,
    pub(super) name: String,
    pub(super) job_type: String,
    pub(super) organization_id: Option<Uuid>,
    pub(super) payload_template: serde_json::Value,
    pub(super) cron_expr: String,
    pub(super) is_active: bool,
    pub(super) max_jitter_seconds: i32,
    pub(super) next_fire_at: DateTime<Utc>,
}

pub(super) fn job_schedule_from_row(row: JobScheduleRow) -> Result<JobScheduleRecord> {
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
