use std::fmt;
use std::time::Duration;

use chrono::{DateTime, Utc};
use runledger_core::jobs::{
    JobStage, JobTypeName, StepKey, StepKeyName, WorkflowDependencyReleaseMode, WorkflowRunStatus,
    WorkflowStepEnqueue, WorkflowStepExecutionKind, WorkflowStepStatus, WorkflowTypeName,
};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::Error;

#[derive(Debug, Clone)]
pub struct WorkflowRunDbRecord {
    pub id: Uuid,
    pub workflow_type: WorkflowTypeName,
    pub organization_id: Option<Uuid>,
    pub status: WorkflowRunStatus,
    pub idempotency_key: Option<String>,
    pub result_step_key: Option<StepKeyName>,
    pub metadata: serde_json::Value,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Classified result of enqueueing with a reusable workflow active key.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum EnqueueActiveWorkflowOutcome {
    /// A new workflow run and active claim were inserted.
    Inserted(WorkflowRunDbRecord),
    /// The active key is still owned by a prior non-quiesced workflow run.
    ///
    /// This can be a terminal canceled run whose live handler lease has not
    /// quiesced yet.
    ExistingActive(WorkflowRunDbRecord),
    /// The permanent idempotency key matched an identical prior request.
    ExistingIdempotent(WorkflowRunDbRecord),
}

impl EnqueueActiveWorkflowOutcome {
    #[must_use]
    pub const fn workflow_run(&self) -> &WorkflowRunDbRecord {
        match self {
            Self::Inserted(run) | Self::ExistingActive(run) | Self::ExistingIdempotent(run) => run,
        }
    }
}

/// Recovery shape. New modes may be added without changing lineage storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WorkflowRecoveryMode {
    /// Reconstruct and enqueue the complete original DAG plus append history.
    FullReplay,
}

impl WorkflowRecoveryMode {
    #[must_use]
    pub const fn as_db_value(self) -> &'static str {
        match self {
            Self::FullReplay => "FULL_REPLAY",
        }
    }
}

/// Request for an immutable, lineage-linked workflow replay.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WorkflowRecoveryRequest<'a> {
    /// Exactly scoped terminal source run.
    pub source_run_id: Uuid,
    /// Organization scope for the source. `None` selects only a global source,
    /// not every organization.
    pub organization_id: Option<Uuid>,
    /// Optional source step that motivated this full replay. This is lineage
    /// context and does not reopen the step in place.
    pub source_step_id: Option<Uuid>,
    /// Stable identity for one intentional recovery, non-blank and at most 512
    /// bytes.
    pub request_key: &'a str,
    pub mode: WorkflowRecoveryMode,
    /// Non-blank audit reason.
    pub reason: &'a str,
}

impl<'a> WorkflowRecoveryRequest<'a> {
    /// Creates a recovery request for an exactly global source.
    ///
    /// Use [`Self::organization_id`] for an organization-owned source and
    /// [`Self::source_step_id`] to add optional audit lineage.
    #[must_use]
    pub const fn new(
        source_run_id: Uuid,
        request_key: &'a str,
        mode: WorkflowRecoveryMode,
        reason: &'a str,
    ) -> Self {
        Self {
            source_run_id,
            organization_id: None,
            source_step_id: None,
            request_key,
            mode,
            reason,
        }
    }

    /// Selects an organization-owned source instead of an exactly global one.
    #[must_use]
    pub const fn organization_id(mut self, organization_id: Uuid) -> Self {
        self.organization_id = Some(organization_id);
        self
    }

    /// Records the source step that motivated recovery without narrowing the
    /// full-workflow replay.
    #[must_use]
    pub const fn source_step_id(mut self, source_step_id: Uuid) -> Self {
        self.source_step_id = Some(source_step_id);
        self
    }
}

/// Whether a recovery request created a run or resolved idempotently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum WorkflowRecoveryDisposition {
    /// A new recovery run and lineage row were committed.
    Inserted,
    /// The same source and request key already produced this run.
    Existing,
}

/// Classified result of immutable workflow recovery.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WorkflowRecoveryOutcome {
    /// The newly inserted or previously existing recovery run.
    pub run: WorkflowRunDbRecord,
    pub disposition: WorkflowRecoveryDisposition,
}

#[derive(Debug, Clone)]
pub struct WorkflowStepDbRecord {
    pub id: Uuid,
    pub workflow_run_id: Uuid,
    pub step_key: StepKeyName,
    pub execution_kind: WorkflowStepExecutionKind,
    pub job_type: Option<JobTypeName>,
    pub organization_id: Option<Uuid>,
    pub payload: serde_json::Value,
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
    pub dependency_count_total: i32,
    pub dependency_count_pending: i32,
    pub dependency_count_unsatisfied: i32,
    pub status_reason: Option<String>,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub output: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct WorkflowStepDependencyDbRecord {
    pub workflow_run_id: Uuid,
    pub prerequisite_step_id: Uuid,
    pub dependent_step_id: Uuid,
    pub release_mode: WorkflowDependencyReleaseMode,
    pub created_at: DateTime<Utc>,
}

/// Legacy workflow-run list filter with nullable admin visibility.
///
/// `None` for `organization_id` retains its historical meaning: the caller can
/// inspect global and organization-owned runs. Prefer
/// [`WorkflowRunReadListFilter`] for new code so the visibility decision is
/// explicit.
#[derive(Debug, Clone)]
pub struct WorkflowRunListFilter<'a> {
    pub organization_id: Option<Uuid>,
    pub status: Option<WorkflowRunStatus>,
    pub workflow_type: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}

/// Legacy workflow-run count filter with nullable admin visibility.
///
/// `None` for `organization_id` retains its historical meaning: the caller can
/// inspect global and organization-owned runs. Prefer
/// [`WorkflowRunReadCountFilter`] for new code so the visibility decision is
/// explicit.
#[derive(Debug, Clone)]
pub struct WorkflowRunCountFilter<'a> {
    pub organization_id: Option<Uuid>,
    pub status: Option<WorkflowRunStatus>,
    pub workflow_type: Option<&'a str>,
}

/// Explicit visibility capability for workflow reads.
///
/// This is intentionally a read capability, not a mutation or cancellation
/// capability. `Global` matches only rows whose `organization_id` is `NULL`,
/// `Organization` matches one exact organization, and `Admin` may inspect both
/// global and organization-owned rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowRunReadScope {
    /// Match only a workflow run whose `organization_id` is `NULL`.
    Global,
    /// Match only workflow runs owned by this exact organization.
    Organization(Uuid),
    /// Match workflow runs regardless of organization ownership.
    Admin,
}

impl WorkflowRunReadScope {
    /// Returns the exact organization for [`Self::Organization`].
    ///
    /// This returns `None` for both [`Self::Global`] and [`Self::Admin`]; match
    /// on the scope when those two visibility capabilities must remain distinct.
    #[must_use]
    pub const fn organization_id(self) -> Option<Uuid> {
        match self {
            Self::Organization(organization_id) => Some(organization_id),
            Self::Global | Self::Admin => None,
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

/// Explicit-scope input for listing workflow runs.
#[derive(Debug, Clone)]
pub struct WorkflowRunReadListFilter<'a> {
    pub scope: WorkflowRunReadScope,
    pub status: Option<WorkflowRunStatus>,
    pub workflow_type: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}

/// Explicit-scope input for counting workflow runs.
#[derive(Debug, Clone)]
pub struct WorkflowRunReadCountFilter<'a> {
    pub scope: WorkflowRunReadScope,
    pub status: Option<WorkflowRunStatus>,
    pub workflow_type: Option<&'a str>,
}

pub struct CompleteExternalWorkflowStepInput<'a> {
    pub workflow_run_id: Uuid,
    pub organization_id: Option<Uuid>,
    pub step_key: StepKey<'a>,
    pub outcome: ExternalWorkflowStepTerminalOutcome<'a>,
    pub status_reason: Option<&'a str>,
    pub last_error_code: Option<&'a str>,
    pub last_error_message: Option<&'a str>,
}

#[derive(Debug, Clone, Copy)]
pub enum ExternalWorkflowStepTerminalOutcome<'a> {
    Succeeded { output: Option<&'a Value> },
    Failed,
    Canceled,
}

impl<'a> ExternalWorkflowStepTerminalOutcome<'a> {
    pub(crate) const fn status(self) -> WorkflowStepStatus {
        match self {
            Self::Succeeded { .. } => WorkflowStepStatus::Succeeded,
            Self::Failed => WorkflowStepStatus::Failed,
            Self::Canceled => WorkflowStepStatus::Canceled,
        }
    }

    pub(crate) const fn output(self) -> Option<&'a Value> {
        match self {
            Self::Succeeded { output } => output,
            Self::Failed | Self::Canceled => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AppendWorkflowStepsInput<'a> {
    pub workflow_run_id: Uuid,
    pub organization_id: Option<Uuid>,
    pub mutation_key: &'a str,
    pub mutation_metadata: &'a serde_json::Value,
    pub append_window_step_key: StepKey<'a>,
    pub steps: Vec<WorkflowStepEnqueue<'a>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppendWorkflowStepsOutcome {
    Appended,
    AlreadyApplied,
}

#[derive(Debug, Clone)]
pub struct AppendWorkflowStepsResult {
    pub workflow_run: WorkflowRunDbRecord,
    pub appended_steps: Vec<WorkflowStepDbRecord>,
    pub outcome: AppendWorkflowStepsOutcome,
}

/// Compatibility alias for the former handle-only scope name.
///
/// Handles now use [`WorkflowRunReadScope`] so their status, run, and result
/// reads share the same visibility model as the public workflow read APIs.
pub type WorkflowRunHandleScope = WorkflowRunReadScope;

#[derive(Clone)]
pub struct WorkflowRunHandle {
    pub workflow_run_id: Uuid,
    pub scope: WorkflowRunReadScope,
    pub(crate) pool: crate::DbPool,
}

impl fmt::Debug for WorkflowRunHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WorkflowRunHandle")
            .field("workflow_run_id", &self.workflow_run_id)
            .field("scope", &self.scope)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct WorkflowRunWaitOptions {
    /// Maximum time to wait for a declared result. `None` opts into waiting
    /// indefinitely. A zero timeout performs one bounded storage lookup and
    /// returns `Timeout` if the result is not already committed.
    pub timeout: Option<Duration>,
    /// Fallback polling cadence while waiting. Values below 1ms are rounded up.
    ///
    /// A waiting handle first tries to hold a dedicated PostgreSQL LISTEN
    /// connection, then falls back to polling if LISTEN cannot be established.
    /// Size connection pools for concurrent waiters, especially when opting
    /// into unbounded waits.
    pub poll_interval: Duration,
}

/// Default workflow result wait timeout used by [`WorkflowRunWaitOptions`].
pub const DEFAULT_WORKFLOW_RUN_WAIT_TIMEOUT: Duration = Duration::from_secs(300);

impl Default for WorkflowRunWaitOptions {
    fn default() -> Self {
        Self {
            timeout: Some(DEFAULT_WORKFLOW_RUN_WAIT_TIMEOUT),
            poll_interval: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Clone)]
pub struct WorkflowRunResultRecord {
    pub workflow_run_id: Uuid,
    pub workflow_type: WorkflowTypeName,
    pub organization_id: Option<Uuid>,
    pub result_step_key: StepKeyName,
    /// JSON output produced by the declared result step.
    ///
    /// If the workflow succeeds but the declared result step did not persist
    /// output, [`WorkflowRunHandle::get_result`] returns
    /// [`WorkflowRunHandleError::ResultMissing`].
    pub result: Value,
    pub finished_at: DateTime<Utc>,
}

#[derive(Debug)]
pub enum WorkflowRunHandleError {
    Storage(Error),
    NotFound,
    ResultNotDeclared,
    /// The workflow declared a result step and succeeded, but that step produced
    /// no stored output.
    ResultMissing,
    UnsuccessfulTerminal {
        status: WorkflowRunStatus,
    },
    Timeout,
}

impl WorkflowRunHandleError {
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Storage(_) => "workflow.handle_storage_error",
            Self::NotFound => "workflow.run_not_found",
            Self::ResultNotDeclared => "workflow.result_not_declared",
            Self::ResultMissing => "workflow.result_missing",
            Self::UnsuccessfulTerminal { .. } => "workflow.result_unsuccessful_terminal",
            Self::Timeout => "workflow.result_wait_timeout",
        }
    }

    #[must_use]
    pub const fn client_message(&self) -> &'static str {
        match self {
            Self::Storage(_) => "Workflow handle storage operation failed.",
            Self::NotFound => "Workflow run was not found.",
            Self::ResultNotDeclared => "Workflow does not declare a result step.",
            Self::ResultMissing => "Workflow completed without a result.",
            Self::UnsuccessfulTerminal { .. } => "Workflow did not complete successfully.",
            Self::Timeout => "Timed out waiting for workflow result.",
        }
    }
}

impl fmt::Display for WorkflowRunHandleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.client_message())
    }
}

impl std::error::Error for WorkflowRunHandleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::NotFound
            | Self::ResultNotDeclared
            | Self::ResultMissing
            | Self::UnsuccessfulTerminal { .. }
            | Self::Timeout => None,
        }
    }
}

impl From<Error> for WorkflowRunHandleError {
    fn from(error: Error) -> Self {
        Self::Storage(error)
    }
}
