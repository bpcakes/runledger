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

pub struct WorkflowRunListFilter<'a> {
    pub organization_id: Option<Uuid>,
    pub status: Option<WorkflowRunStatus>,
    pub workflow_type: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}

pub struct WorkflowRunCountFilter<'a> {
    pub organization_id: Option<Uuid>,
    pub status: Option<WorkflowRunStatus>,
    pub workflow_type: Option<&'a str>,
}

pub struct CompleteExternalWorkflowStepInput<'a> {
    pub workflow_run_id: Uuid,
    pub organization_id: Option<Uuid>,
    pub step_key: StepKey<'a>,
    pub terminal_status: WorkflowStepStatus,
    pub status_reason: Option<&'a str>,
    pub last_error_code: Option<&'a str>,
    pub last_error_message: Option<&'a str>,
    pub output: Option<&'a Value>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkflowRunHandleScope {
    Organization(Uuid),
    Global,
    Admin,
}

impl WorkflowRunHandleScope {
    #[must_use]
    pub const fn organization_id(self) -> Option<Uuid> {
        match self {
            Self::Organization(organization_id) => Some(organization_id),
            Self::Global | Self::Admin => None,
        }
    }
}

#[derive(Clone)]
pub struct WorkflowRunHandle {
    pub workflow_run_id: Uuid,
    pub scope: WorkflowRunHandleScope,
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
