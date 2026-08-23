use chrono::{DateTime, Utc};
use runledger_core::jobs::{
    JobStage, JobStatus, JobTypeName, StepKeyName, WorkflowDependencyReleaseMode,
    WorkflowRunStatus, WorkflowStepExecutionKind, WorkflowStepStatus, WorkflowTypeName,
};
use serde_json::Value;
use sqlx::types::Uuid;

use super::enqueue::JobQueueRecord;
use crate::jobs::workflow_types::WorkflowRunDbRecord;

/// Maximum page size accepted by public job and workflow list APIs.
///
/// This bounds accidental unbounded reads from admin/TUI surfaces while still
/// allowing operators to inspect a large page when needed.
pub const JOB_LIST_PAGE_LIMIT_MAX: i64 = 1_000;

/// Whether an admin persistence read should load sensitive stored data.
///
/// This is a database projection choice, not an authorization decision. The
/// caller must derive it from an already-authorized request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdminDataProjection {
    /// Select only operational metadata columns.
    MetadataOnly,
    /// Select operational metadata and sensitive stored data.
    Full,
}

/// Sensitive data attached to an admin persistence projection.
///
/// Keeping redaction explicit in the returned type prevents downstream code
/// from confusing a field that was not selected with a stored SQL `NULL`.
#[derive(Clone, Debug)]
pub enum AdminSensitiveData<T> {
    /// The query did not select sensitive columns.
    Redacted,
    /// The query selected the complete sensitive projection.
    Full(T),
}

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

/// Sensitive fields available only in a full admin job detail projection.
#[derive(Clone, Debug)]
pub struct AdminJobSensitiveRecord {
    pub payload: Value,
    pub checkpoint: Option<Value>,
    pub output: Option<Value>,
    pub idempotency_key: Option<String>,
    pub worker_id: Option<String>,
    pub status_reason: Option<String>,
    pub last_error_message: Option<String>,
}

/// Authorization-scoped admin projection for one job detail response.
#[derive(Clone, Debug)]
pub struct AdminJobRecord {
    pub summary: AdminJobSummaryRecord,
    pub sensitive: AdminSensitiveData<AdminJobSensitiveRecord>,
}

impl AdminJobRecord {
    pub(crate) fn from_full(row: JobQueueRecord) -> Self {
        Self {
            summary: AdminJobSummaryRecord {
                id: row.id,
                job_type: row.job_type,
                organization_id: row.organization_id,
                status: row.status,
                priority: row.priority,
                run_number: row.run_number,
                attempt: row.attempt,
                max_attempts: row.max_attempts,
                timeout_seconds: row.timeout_seconds,
                next_run_at: row.next_run_at,
                lease_expires_at: row.lease_expires_at,
                last_heartbeat_at: row.last_heartbeat_at,
                started_at: row.started_at,
                finished_at: row.finished_at,
                stage: row.stage,
                progress_done: row.progress_done,
                progress_total: row.progress_total,
                progress_pct: row.progress_pct,
                last_error_code: row.last_error_code,
                created_at: row.created_at,
                updated_at: row.updated_at,
            },
            sensitive: AdminSensitiveData::Full(AdminJobSensitiveRecord {
                payload: row.payload,
                checkpoint: row.checkpoint,
                output: row.output,
                idempotency_key: row.idempotency_key,
                worker_id: row.worker_id,
                status_reason: row.status_reason,
                last_error_message: row.last_error_message,
            }),
        }
    }
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

/// Sensitive fields available only in a full admin workflow detail projection.
#[derive(Clone, Debug)]
pub struct AdminWorkflowSensitiveRecord {
    pub idempotency_key: Option<String>,
    pub metadata: Value,
}

/// Authorization-scoped admin projection for one workflow detail response.
#[derive(Clone, Debug)]
pub struct AdminWorkflowRecord {
    pub summary: AdminWorkflowSummaryRecord,
    pub sensitive: AdminSensitiveData<AdminWorkflowSensitiveRecord>,
}

impl AdminWorkflowRecord {
    pub(crate) fn from_full(row: WorkflowRunDbRecord) -> Self {
        Self {
            summary: AdminWorkflowSummaryRecord {
                id: row.id,
                workflow_type: row.workflow_type,
                organization_id: row.organization_id,
                status: row.status,
                result_step_key: row.result_step_key,
                started_at: row.started_at,
                finished_at: row.finished_at,
                created_at: row.created_at,
                updated_at: row.updated_at,
            },
            sensitive: AdminSensitiveData::Full(AdminWorkflowSensitiveRecord {
                idempotency_key: row.idempotency_key,
                metadata: row.metadata,
            }),
        }
    }
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
    pub priority: Option<i32>,
    pub max_attempts: Option<i32>,
    pub timeout_seconds: Option<i32>,
    pub stage: Option<JobStage>,
    pub allow_handler_continuation: bool,
    pub status: WorkflowStepStatus,
    pub job_id: Option<Uuid>,
    pub released_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub visible_dependency_count_total: i32,
    pub visible_dependency_count_pending: i32,
    pub visible_dependency_count_unsatisfied: i32,
    pub has_hidden_prerequisites: bool,
    pub last_error_code: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub sensitive: AdminSensitiveData<AdminWorkflowStepSensitiveRecord>,
}

/// Sensitive fields available only in a full admin workflow-step projection.
#[derive(Clone, Debug)]
pub struct AdminWorkflowStepSensitiveRecord {
    pub payload: Value,
    pub execution_resource_key: Option<String>,
    pub status_reason: Option<String>,
    pub last_error_message: Option<String>,
    pub output: Option<Value>,
}

/// Operational metadata for one job event.
#[derive(Clone, Debug)]
pub struct AdminJobEventRecord {
    pub id: i64,
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: Option<i32>,
    pub event_type: runledger_core::jobs::JobEventType,
    pub stage: Option<JobStage>,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub occurred_at: DateTime<Utc>,
    pub sensitive: AdminSensitiveData<AdminJobEventSensitiveRecord>,
}

/// Sensitive fields available only in a full admin job-event projection.
#[derive(Clone, Debug)]
pub struct AdminJobEventSensitiveRecord {
    pub payload: Value,
}

/// Operational metadata for one job log record.
#[derive(Clone, Debug)]
pub struct AdminJobLogRecord {
    pub id: i64,
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: Option<i32>,
    pub level: String,
    pub occurred_at: DateTime<Utc>,
    pub sensitive: AdminSensitiveData<AdminJobLogSensitiveRecord>,
}

/// Sensitive fields available only in a full admin job-log projection.
#[derive(Clone, Debug)]
pub struct AdminJobLogSensitiveRecord {
    pub message: String,
    pub payload: Value,
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
