use crate::{DbTx, Error, Result};

use super::super::types::{JobScheduleRecord, JobScheduleUpsert};
use super::row::{JobScheduleRow, job_schedule_from_row};

/// Controls how an upsert handles an existing schedule's active state.
#[derive(Clone, Copy)]
pub(super) enum ScheduleActiveStatePolicy {
    /// Retain the existing active state when the schedule name conflicts.
    PreserveStored,
    /// Treat the input active state as authoritative when the schedule name conflicts.
    ApplyRequested,
}

impl ScheduleActiveStatePolicy {
    fn conflict_set_clause(self) -> &'static str {
        match self {
            Self::PreserveStored => "",
            Self::ApplyRequested => "is_active = EXCLUDED.is_active,",
        }
    }
}

/// Persists a validated schedule after the caller has performed its guards.
pub(super) async fn persist_job_schedule_tx(
    tx: &mut DbTx<'_>,
    payload: &JobScheduleUpsert<'_>,
    active_state_policy: ScheduleActiveStatePolicy,
    error_context: &str,
) -> Result<JobScheduleRecord> {
    // This fragment is selected solely by the private enum above. Omitting the
    // assignment for PreserveStored keeps the ordinary upsert's conflict SQL
    // shape intact, including UPDATE OF trigger and column-privilege behavior.
    let sql = format!(
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
                {}
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
        active_state_policy.conflict_set_clause(),
    );

    let row = sqlx::query_as::<_, JobScheduleRow>(&sql)
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
        .map_err(|error| Error::from_query_sqlx_with_context(error_context, error))?;

    job_schedule_from_row(row)
}
