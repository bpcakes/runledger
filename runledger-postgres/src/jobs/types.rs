use std::time::Duration;

use chrono::{DateTime, Utc};
use runledger_core::jobs::{
    JobEventType, JobFailure, JobFailureKind, JobStage, JobStatus, JobType, JobTypeName,
};
use serde_json::Value;
use sqlx::types::Uuid;

/// Maximum accepted schedule jitter, in seconds.
///
/// The scheduler treats jitter as a deterministic spread applied to future fire
/// cursors, and the persistence layer rejects larger values.
pub const JOB_SCHEDULE_MAX_JITTER_SECONDS: i32 = 86_400;

/// Maximum page size accepted by public job and workflow list APIs.
///
/// This bounds accidental unbounded reads from admin/TUI surfaces while still
/// allowing operators to inspect a large page when needed.
pub const JOB_LIST_PAGE_LIMIT_MAX: i64 = 1_000;

#[derive(Debug, Clone)]
pub struct JobDefinitionUpsert<'a> {
    pub job_type: JobType<'a>,
    pub version: i32,
    pub max_attempts: i32,
    pub default_timeout_seconds: i32,
    pub default_priority: i32,
    pub is_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct JobDefinitionRecord {
    pub job_type: JobTypeName,
    pub version: i32,
    pub max_attempts: i32,
    pub default_timeout_seconds: i32,
    pub default_priority: i32,
    pub is_enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Schedule row that blocks a job-definition catalog sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobScheduleJobTypeReference {
    /// Active schedule name.
    pub schedule_name: String,
    /// Job type referenced by the active schedule.
    pub job_type: JobTypeName,
}

#[derive(Debug, Clone)]
pub struct JobDefinitionListFilter<'a> {
    /// Admin list query input used for escaped `ILIKE` substring matching, not a canonical
    /// persisted identifier boundary.
    pub job_type: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Clone)]
pub struct JobDefinitionUpdate {
    pub max_attempts: Option<i32>,
    pub default_timeout_seconds: Option<i32>,
    pub default_priority: Option<i32>,
    pub is_enabled: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct JobRuntimeConfigUpsert<'a> {
    pub job_type: JobType<'a>,
    pub schema_version: i32,
    pub config: &'a Value,
    pub updated_by_user_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub struct JobRuntimeConfigRecord {
    pub job_type: JobTypeName,
    pub schema_version: i32,
    pub config: Value,
    pub updated_by_user_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct JobEnqueue<'a> {
    pub job_type: JobType<'a>,
    pub organization_id: Option<Uuid>,
    pub payload: &'a Value,
    pub priority: Option<i32>,
    pub max_attempts: Option<i32>,
    pub timeout_seconds: Option<i32>,
    /// For keyed enqueues, this value is part of the stored idempotency request
    /// snapshot. Retries must pass the same scheduled time as the original
    /// enqueue instead of recomputing a fresh timestamp.
    pub next_run_at: Option<DateTime<Utc>>,
    pub idempotency_key: Option<&'a str>,
    pub stage: Option<JobStage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum JobEnqueueDisposition {
    Inserted,
    Existing,
}

/// Stable job state returned from a transactional enqueue.
///
/// Keyed existing rows are held under a mutation-ready row lock until the
/// caller's transaction ends, so `status` and `run_number` describe the row
/// protected by that transaction rather than a later unlocked lookup. This
/// lock composes with a later mutation of the same row in the transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobEnqueueOutcome {
    pub job_id: Uuid,
    pub status: JobStatus,
    pub run_number: i32,
    pub disposition: JobEnqueueDisposition,
}

/// Exact tenant scope for a job mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobScope {
    /// Match only a job whose `organization_id` is `NULL`.
    Global,
    /// Match only a job owned by this exact organization.
    Organization(Uuid),
}

impl JobScope {
    #[must_use]
    pub const fn organization_id(self) -> Option<Uuid> {
        match self {
            Self::Global => None,
            Self::Organization(organization_id) => Some(organization_id),
        }
    }
}

/// Terminal job statuses that may be recovered through compare-and-requeue.
///
/// `SUCCEEDED` is deliberately absent: replaying successful work requires a
/// separate policy decision and cannot be requested through this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequeueableJobStatus {
    DeadLettered,
    Canceled,
}

impl RequeueableJobStatus {
    #[must_use]
    pub const fn as_job_status(self) -> JobStatus {
        match self {
            Self::DeadLettered => JobStatus::DeadLettered,
            Self::Canceled => JobStatus::Canceled,
        }
    }

    #[must_use]
    pub const fn as_db_value(self) -> &'static str {
        self.as_job_status().as_db_value()
    }
}

/// Whether compare-and-requeue carries durable resume state into the new run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobRequeueStatePolicy {
    /// Keep `progress_done`, `progress_total`, and `checkpoint` so recovery can
    /// resume from the last committed position.
    PreserveProgressAndCheckpoint,
    /// Clear progress and checkpoint state so the new run starts from scratch.
    ResetProgressAndCheckpoint,
}

impl JobRequeueStatePolicy {
    #[must_use]
    pub const fn preserves_progress_and_checkpoint(self) -> bool {
        matches!(self, Self::PreserveProgressAndCheckpoint)
    }

    #[must_use]
    pub const fn as_event_value(self) -> &'static str {
        match self {
            Self::PreserveProgressAndCheckpoint => "preserve_progress_and_checkpoint",
            Self::ResetProgressAndCheckpoint => "reset_progress_and_checkpoint",
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompareAndRequeueJob<'a> {
    pub scope: JobScope,
    pub job_id: Uuid,
    pub expected_status: RequeueableJobStatus,
    pub expected_run_number: i32,
    pub state_policy: JobRequeueStatePolicy,
    pub reason: &'a str,
}

#[derive(Debug, Clone)]
#[must_use = "callers must inspect whether the expected job was requeued"]
#[non_exhaustive]
pub enum CompareAndRequeueJobOutcome {
    Requeued {
        before: Box<JobQueueRecord>,
        after: Box<JobQueueRecord>,
        event_id: i64,
    },
    ExpectationMismatch {
        actual: Box<JobQueueRecord>,
    },
    /// Cancellation fenced a live handler, but its original lease window has
    /// not passed yet. Retrying before `retry_after` could overlap the new run
    /// with the canceled handler's external side effects.
    CancellationNotQuiesced {
        actual: Box<JobQueueRecord>,
        retry_after: DateTime<Utc>,
    },
    NotFound,
}

#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
pub struct JobScheduleCatalogSyncEntry<'a> {
    /// Schedule definition fields to upsert. Unlike plain schedule upserts,
    /// catalog sync treats `is_active` as the authoritative desired active state
    /// for both inserts and conflicts.
    pub upsert: JobScheduleUpsert<'a>,
}

/// Result returned by [`super::schedules::sync_catalog_job_schedules_tx`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobScheduleCatalogSyncReport {
    /// Schedule names upserted and active state applied during this sync.
    pub synced_schedule_names: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct JobQueueRecord {
    pub id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub payload: Value,
    pub status: JobStatus,
    pub priority: i32,
    pub run_number: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    pub timeout_seconds: i32,
    pub next_run_at: DateTime<Utc>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub worker_id: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub stage: JobStage,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub progress_pct: Option<f64>,
    pub checkpoint: Option<Value>,
    pub output: Option<Value>,
    pub idempotency_key: Option<String>,
    pub status_reason: Option<String>,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct JobEventRecord {
    pub id: i64,
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: Option<i32>,
    pub event_type: JobEventType,
    pub stage: Option<JobStage>,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub payload: Value,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReapedTerminalLeaseRecord {
    pub job_id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub run_number: i32,
    pub attempt: i32,
    pub payload: Value,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReapedLeaseDisposition {
    ReleasedToPending,
    RetryScheduled {
        retry_delay_ms: i32,
        next_run_at: DateTime<Utc>,
    },
    DeadLetteredTerminal {
        payload: Value,
    },
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ReapedLeaseRecord {
    pub job_id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub run_number: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    /// Checkpoint committed on the leased run before it was reaped.
    pub checkpoint: Option<Value>,
    pub worker_id: Option<String>,
    pub started_without_renewal_heartbeat: bool,
    pub failure: JobFailure,
    pub disposition: ReapedLeaseDisposition,
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ReapExpiredLeaseDeferredError {
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: i32,
    pub error_code: String,
    pub error_message: String,
    pub sqlstate: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ReapExpiredLeasesResult {
    pub processed: i64,
    pub terminal_dead_lettered: Vec<ReapedTerminalLeaseRecord>,
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ReapExpiredLeasesDetailedResult {
    pub summary: ReapExpiredLeasesResult,
    pub reaped_leases: Vec<ReapedLeaseRecord>,
    pub deferred_row_error_count: usize,
    pub deferred_row_errors: Vec<ReapExpiredLeaseDeferredError>,
}

#[derive(Debug, Clone)]
pub struct JobMetricsRecord {
    pub job_type: JobTypeName,
    pub pending_count: i64,
    pub leased_count: i64,
    pub stale_leases: i64,
    pub succeeded_24h: i64,
    pub retryable_24h: i64,
    pub terminal_24h: i64,
    pub panicked_24h: i64,
    pub timeout_24h: i64,
    pub dead_lettered_24h: i64,
    pub p50_duration_ms_24h: Option<f64>,
    pub p95_duration_ms_24h: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct JobLogRecord {
    pub id: i64,
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: Option<i32>,
    pub level: String,
    pub message: String,
    pub payload: Value,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct JobLogRecordInput {
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: Option<i32>,
    pub level: String,
    pub message: String,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct JobProgressUpdate<'a> {
    pub stage: Option<JobStage>,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub checkpoint: Option<&'a Value>,
}

#[derive(Debug, Clone)]
pub struct JobCompletionUpdate<'a> {
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub checkpoint: Option<&'a Value>,
    pub output: Option<&'a Value>,
}

/// Progress and scheduling data for a successful handler continuation.
#[derive(Debug, Clone)]
pub struct JobContinuationUpdate<'a> {
    /// How long to wait before the next run becomes claimable. Zero means the
    /// next run is immediately eligible. Delays whose resulting timestamp is
    /// outside the persistence driver's representable range are rejected with
    /// `job.invalid_continuation_delay`.
    pub delay: Duration,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub checkpoint: Option<&'a Value>,
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct JobContinuationOutcome {
    pub job_id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    /// The run whose attempt completed successfully.
    pub completed_run_number: i32,
    /// The newly pending run number.
    pub next_run_number: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    pub next_run_at: DateTime<Utc>,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct JobSuccessCompletionOutcome {
    pub job_id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub run_number: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct JobFailureUpdate<'a> {
    pub kind: JobFailureKind,
    pub code: &'a str,
    pub message: &'a str,
    pub retry_delay_ms: Option<i32>,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobFailureCompletionDisposition {
    RetryScheduled {
        retry_delay_ms: i32,
        next_run_at: DateTime<Utc>,
    },
    DeadLettered {
        reason: runledger_core::jobs::JobDeadLetterReason,
    },
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct JobFailureCompletionOutcome {
    pub job_id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub run_number: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    pub failure_kind: JobFailureKind,
    pub failure_code: String,
    pub failure_message: String,
    /// Latest durable checkpoint observed while locking the failed attempt.
    pub checkpoint: Option<Value>,
    pub disposition: JobFailureCompletionDisposition,
}

#[derive(Debug, Clone)]
pub struct JobListFilter<'a> {
    pub organization_id: Option<Uuid>,
    pub status: Option<JobStatus>,
    /// Admin list query input used for `ILIKE` substring matching, not a canonical persisted
    /// identifier boundary.
    pub job_type: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Clone)]
pub struct JobRuntimeConfigListFilter<'a> {
    /// Admin query filter string used for listing/runtime-config lookup filters, not a canonical
    /// persisted identifier boundary.
    pub job_type: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}
