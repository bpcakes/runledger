use chrono::{DateTime, Utc};
use runledger_core::jobs::{
    JobStage, JobTypeName, StepKey, StepKeyName, WorkflowDependencyReleaseMode, WorkflowRunStatus,
    WorkflowStepEnqueue, WorkflowStepExecutionKind, WorkflowStepStatus, WorkflowTypeName,
};
use sqlx::types::Uuid;

#[derive(Debug, Clone)]
pub struct WorkflowRunDbRecord {
    pub id: Uuid,
    pub workflow_type: WorkflowTypeName,
    pub organization_id: Option<Uuid>,
    pub status: WorkflowRunStatus,
    pub idempotency_key: Option<String>,
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

pub struct CompleteExternalWorkflowStepInput<'a> {
    pub workflow_run_id: Uuid,
    pub organization_id: Option<Uuid>,
    pub step_key: StepKey<'a>,
    pub terminal_status: WorkflowStepStatus,
    pub status_reason: Option<&'a str>,
    pub last_error_code: Option<&'a str>,
    pub last_error_message: Option<&'a str>,
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
