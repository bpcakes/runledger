use chrono::{DateTime, Utc};
use runledger_core::jobs::{JobType, JobTypeName};
use serde_json::Value;
use sqlx::types::Uuid;

/// Maximum accepted schedule jitter, in seconds.
///
/// The scheduler treats jitter as a deterministic spread applied to future fire
/// cursors, and the persistence layer rejects larger values.
pub const JOB_SCHEDULE_MAX_JITTER_SECONDS: i32 = 86_400;

#[derive(Clone, Debug)]
pub struct JobScheduleRecord {
    /// Stable schedule row identifier.
    pub id: Uuid,
    /// Unique schedule name.
    pub name: String,
    /// Job type enqueued whenever the schedule fires.
    pub job_type: JobTypeName,
    /// Optional organization scope copied into jobs created by this schedule.
    pub organization_id: Option<Uuid>,
    /// JSON payload template copied into each scheduled job before runtime
    /// schedule metadata is merged.
    pub payload_template: Value,
    /// UTC cron expression used by the runtime scheduler.
    pub cron_expr: String,
    /// Whether the runtime scheduler may claim this schedule.
    ///
    /// Schedule upserts preserve this value for existing rows; use
    /// `set_job_schedule_active` to pause or resume a schedule intentionally.
    pub is_active: bool,
    /// Maximum deterministic jitter, in seconds, applied when computing the next
    /// fire cursor. Must not exceed [`JOB_SCHEDULE_MAX_JITTER_SECONDS`].
    pub max_jitter_seconds: i32,
    /// Next UTC instant at which this schedule is due for materialization.
    pub next_fire_at: DateTime<Utc>,
}

/// Input for creating or updating a cron-backed job schedule.
///
/// Schedules are keyed by `name`. Updating an existing schedule refreshes the
/// stored job type, payload template, cron expression, and jitter, while leaving
/// scheduler-managed state intact. `organization_id` and `is_active` apply only
/// when a new schedule row is inserted. `next_fire_at` applies on insert and
/// when the cron expression changes.
///
/// Cron expressions are interpreted in UTC and must be accepted by
/// `cron::Schedule::from_str`, the same parser used by `runledger-runtime` when
/// materializing due schedules. The upsert validator rejects blank or padded
/// schedule names, blank or padded cron expressions, invalid cron expressions,
/// negative jitter, and jitter above [`JOB_SCHEDULE_MAX_JITTER_SECONDS`].
///
/// This input does not encode a compile-time job catalog. The PostgreSQL schema
/// requires a matching job-definition row for `job_type`, but this API does not
/// prove that a worker process has registered a runtime handler for that job
/// type.
#[derive(Clone, Debug)]
pub struct JobScheduleUpsert<'a> {
    /// Stable unique schedule name without surrounding whitespace.
    pub name: &'a str,
    /// Job type to enqueue whenever the schedule fires.
    pub job_type: JobType<'a>,
    /// Optional organization scope for enqueued jobs on first insert.
    pub organization_id: Option<Uuid>,
    /// JSON payload copied into each job created by the scheduler.
    pub payload_template: &'a Value,
    /// UTC cron expression without surrounding whitespace, validated on upsert
    /// and parsed again when the schedule fires.
    pub cron_expr: &'a str,
    /// Whether the runtime scheduler should claim this schedule on first insert.
    pub is_active: bool,
    /// Initial fire cursor for the scheduler, also used when changing cron syntax.
    pub next_fire_at: DateTime<Utc>,
    /// Maximum deterministic jitter applied when materializing a due schedule,
    /// capped at [`JOB_SCHEDULE_MAX_JITTER_SECONDS`].
    pub max_jitter_seconds: i32,
}

/// One catalog-owned schedule sync entry.
#[derive(Clone, Debug)]
pub struct JobScheduleCatalogSyncEntry<'a> {
    /// Schedule definition fields to upsert. Unlike plain schedule upserts,
    /// catalog sync treats `is_active` as the authoritative desired active state
    /// for both inserts and conflicts.
    pub upsert: JobScheduleUpsert<'a>,
}

/// Result returned by [`crate::jobs::sync_catalog_job_schedules_tx`].
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct JobScheduleCatalogSyncReport {
    /// Schedule names upserted and active state applied during this sync.
    pub synced_schedule_names: Vec<String>,
}
