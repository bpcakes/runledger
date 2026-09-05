use chrono::{DateTime, Utc};
use runledger_core::jobs::{JobStatus, JobTypeName};
use serde_json::Value;
use sqlx::types::Uuid;

/// Maximum page size accepted by public job and workflow list APIs.
///
/// This bounds accidental unbounded reads from admin/TUI surfaces while still
/// allowing operators to inspect a large page when needed.
pub const JOB_LIST_PAGE_LIMIT_MAX: i64 = 1_000;

/// Explicit visibility for job, event, log, enqueue-intent, and metrics reads.
///
/// This selects rows, not authorization. Applications must authorize the chosen
/// scope, especially `Admin`, before calling a read API. It grants no mutation
/// or cancellation permission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobReadScope {
    /// Match only rows whose job or intent has a NULL organization.
    Global,
    /// Match only rows belonging to this exact organization.
    Organization(Uuid),
    /// Match both global and organization-owned rows.
    Admin,
}

impl JobReadScope {
    pub(in crate::jobs) const fn from_legacy(organization_id: Option<Uuid>) -> Self {
        match organization_id {
            Some(organization_id) => Self::Organization(organization_id),
            None => Self::Admin,
        }
    }

    pub(in crate::jobs) const fn visibility_predicate(self) -> (bool, Option<Uuid>) {
        match self {
            Self::Global => (false, None),
            Self::Organization(organization_id) => (false, Some(organization_id)),
            Self::Admin => (true, None),
        }
    }
}

/// Explicit-scope input for listing jobs.
#[derive(Clone, Debug)]
pub struct JobReadListFilter<'a> {
    pub scope: JobReadScope,
    pub status: Option<JobStatus>,
    /// Case-insensitive job-type substring; SQL ILIKE wildcards retain their meaning.
    pub job_type: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}

/// Authorization scope for canceling a job.
///
/// Unlike legacy cancellation APIs that use `None` as an admin wildcard, this
/// type distinguishes an exact global job from an explicit admin operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobCancellationScope {
    /// Match only a job whose `organization_id` is `NULL`.
    Global,
    /// Match only a job owned by this exact organization.
    Organization(Uuid),
    /// Match the job regardless of its organization.
    Admin,
}

#[derive(Clone, Debug)]
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

/// Continuation-specific operational signals for one job type.
///
/// Kept separate from [`JobMetricsRecord`] so adding continuation visibility
/// does not break downstream code that constructs the established metrics DTO.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct JobContinuationMetricsRecord {
    pub job_type: JobTypeName,
    /// Successful handler continuations recorded during the last 24 hours.
    pub continued_24h: i64,
    /// Pending or leased jobs whose current run was created by continuation.
    pub active_continued_count: i64,
    /// Highest current run number among those continuation-created runs.
    pub max_active_run_number: i32,
}

#[derive(Clone, Debug)]
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

#[derive(Clone, Debug)]
pub struct JobLogRecordInput {
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: Option<i32>,
    pub level: String,
    pub message: String,
    pub payload: Value,
}

#[derive(Clone, Debug)]
pub struct JobListFilter<'a> {
    /// Legacy visibility: None matches all organizations and global jobs.
    pub organization_id: Option<Uuid>,
    pub status: Option<JobStatus>,
    /// Admin list query input used for `ILIKE` substring matching, not a canonical persisted
    /// identifier boundary.
    pub job_type: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}
