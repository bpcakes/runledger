use chrono::{DateTime, Utc};
use runledger_core::jobs::{
    JobStage, JobStatus, JobTypeName, StepKeyName, WorkflowDependencyReleaseMode,
    WorkflowRunStatus, WorkflowStepExecutionKind, WorkflowStepStatus, WorkflowTypeName,
};
use serde_json::Value;
use sqlx::types::Uuid;

/// Maximum page size accepted by public job and workflow list APIs.
///
/// This bounds accidental unbounded reads from admin/TUI surfaces while still
/// allowing operators to inspect a large page when needed.
pub const JOB_LIST_PAGE_LIMIT_MAX: i64 = 1_000;

/// Lightweight operational projection for admin job lists.
///
/// Payloads, results, worker identity, idempotency keys, and diagnostic text
/// are intentionally detail-only and cannot be fetched through this shape.
#[derive(Clone, Debug)]
pub struct AdminJobSummaryRecord {
    pub id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub status: JobStatus,
    pub priority: i32,
    pub run_number: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    pub timeout_seconds: i32,
    pub next_run_at: DateTime<Utc>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub stage: JobStage,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub progress_pct: Option<f64>,
    pub last_error_code: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Lightweight operational projection for admin workflow lists.
///
/// Idempotency keys and arbitrary workflow metadata are intentionally
/// detail-only and cannot be fetched through this shape.
#[derive(Clone, Debug)]
pub struct AdminWorkflowSummaryRecord {
    pub id: Uuid,
    pub workflow_type: WorkflowTypeName,
    pub organization_id: Option<Uuid>,
    pub status: WorkflowRunStatus,
    pub result_step_key: Option<StepKeyName>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Authorization-aware workflow step projection for admin detail views.
///
/// Dependency counters describe only prerequisites visible in the requested
/// admin scope. [`Self::has_hidden_prerequisites`] makes an incomplete graph
/// explicit without disclosing hidden step identifiers or their exact count.
/// Durable workflow APIs continue to expose [`super::super::workflow_types::WorkflowStepDbRecord`]
/// with the canonical counters used by the state machine.
#[derive(Clone, Debug)]
pub struct AdminWorkflowStepRecord {
    pub id: Uuid,
    pub workflow_run_id: Uuid,
    pub step_key: StepKeyName,
    pub execution_kind: WorkflowStepExecutionKind,
    pub job_type: Option<JobTypeName>,
    pub organization_id: Option<Uuid>,
    pub payload: Value,
    pub priority: Option<i32>,
    pub max_attempts: Option<i32>,
    pub timeout_seconds: Option<i32>,
    pub stage: Option<JobStage>,
    pub allow_handler_continuation: bool,
    pub execution_resource_key: Option<String>,
    pub status: WorkflowStepStatus,
    pub job_id: Option<Uuid>,
    pub released_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub visible_dependency_count_total: i32,
    pub visible_dependency_count_pending: i32,
    pub visible_dependency_count_unsatisfied: i32,
    pub has_hidden_prerequisites: bool,
    pub status_reason: Option<String>,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub output: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Authorization-filtered workflow dependency projection for admin detail views.
#[derive(Clone, Debug)]
pub struct AdminWorkflowDependencyRecord {
    pub workflow_run_id: Uuid,
    pub prerequisite_step_id: Uuid,
    pub dependent_step_id: Uuid,
    pub release_mode: WorkflowDependencyReleaseMode,
    pub created_at: DateTime<Utc>,
}

pub struct AdminJobSummaryFilter<'a> {
    pub organization_id: Option<Uuid>,
    pub status: Option<JobStatus>,
    /// Case-insensitive literal substring, not a SQL pattern.
    pub job_type_contains: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}

pub struct AdminWorkflowSummaryFilter<'a> {
    pub organization_id: Option<Uuid>,
    pub status: Option<WorkflowRunStatus>,
    /// Case-insensitive literal substring, not a SQL pattern.
    pub workflow_type_contains: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}

/// Complete admin metrics projection for one job type.
#[derive(Clone, Debug)]
pub struct AdminJobMetricsRecord {
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
    pub continued_24h: i64,
    pub active_continued_count: i64,
    pub max_active_run_number: i32,
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
    pub organization_id: Option<Uuid>,
    pub status: Option<JobStatus>,
    /// Admin list query input used for `ILIKE` substring matching, not a canonical persisted
    /// identifier boundary.
    pub job_type: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}
