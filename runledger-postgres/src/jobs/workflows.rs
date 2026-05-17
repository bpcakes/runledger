mod enqueue;
mod hooks;
mod locking;
mod mutate;
mod read;
mod runtime;
mod validation;

pub use self::enqueue::{enqueue_workflow_run, enqueue_workflow_run_tx};
pub(crate) use self::hooks::{on_claim_released, on_claimed, on_retry_scheduled, on_terminal};
pub(crate) use self::locking::{
    lock_workflow_run_release_tx, try_lock_workflow_run_release_shared_tx,
};
pub use self::mutate::{
    append_workflow_steps, append_workflow_steps_tx, cancel_workflow_run_tx,
    list_workflow_step_keys_for_update_tx, update_workflow_step_and_pending_job_payload_tx,
};
pub use self::read::{
    get_latest_workflow_run_by_type, get_workflow_run_by_id,
    get_workflow_run_by_type_and_idempotency_key, get_workflow_run_by_type_and_idempotency_key_tx,
    get_workflow_run_id_for_job, list_workflow_runs, list_workflow_step_dependencies,
    list_workflow_steps,
};
pub use self::runtime::{complete_external_workflow_step, complete_external_workflow_step_tx};
#[cfg(feature = "test-support")]
pub mod test_support {
    pub use super::locking::test_support::workflow_run_release_lock_key;
}

use runledger_core::jobs::{
    JobStage, JobTypeName, WorkflowRunStatus, WorkflowStepExecutionKind, WorkflowStepStatus,
};
use sqlx::types::Uuid;

use crate::{Error, QueryError, QueryErrorCategory};

#[derive(Debug, Clone)]
struct JobDefinitionDefaults {
    default_priority: i32,
    max_attempts: i32,
    default_timeout_seconds: i32,
}

#[derive(Debug, Clone)]
pub(crate) struct StepReleaseCandidate {
    id: Uuid,
    workflow_run_id: Uuid,
    execution_kind: WorkflowStepExecutionKind,
    job_type: Option<JobTypeName>,
    organization_id: Option<Uuid>,
    payload: serde_json::Value,
    priority: Option<i32>,
    max_attempts: Option<i32>,
    timeout_seconds: Option<i32>,
    stage: Option<JobStage>,
}

fn workflow_definition_not_available_error(job_type: &str) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.definition_not_found_or_disabled",
        "Workflow step job type is not available.",
        format!("workflow step job definition missing or disabled: {job_type}"),
    ))
}

fn workflow_internal_state_error(message: impl Into<String>) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Internal,
        "workflow.internal_state",
        "Workflow state is invalid.",
        message,
    ))
}

fn workflow_release_conflict_error(workflow_run_id: Uuid) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Conflict,
        "workflow.release_conflict",
        "Workflow step release conflicted with another workflow mutation.",
        format!("workflow run {workflow_run_id} is locked for an exclusive mutation"),
    ))
}

fn workflow_external_step_not_found_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.external_step_not_found",
        "External workflow step was not found.",
        "external workflow step lookup failed",
    ))
}

fn workflow_external_step_not_external_error(step_key: &str) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.external_step_not_external",
        "Workflow step is not an external gate.",
        format!("workflow step '{step_key}' is not an EXTERNAL step"),
    ))
}

fn workflow_external_step_not_waiting_error(step_key: &str, status: WorkflowStepStatus) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.external_step_not_waiting",
        "External workflow step is not waiting for completion.",
        format!(
            "workflow step '{step_key}' is not waiting for external completion; current status={}",
            status.as_db_value()
        ),
    ))
}

fn workflow_external_completion_conflict_error(
    step_key: &str,
    current_status: WorkflowStepStatus,
    requested_status: WorkflowStepStatus,
) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.external_step_conflicting_completion_retry",
        "External workflow step was already completed with a different result.",
        format!(
            "workflow step '{step_key}' was already completed as {} and cannot be retried as {}",
            current_status.as_db_value(),
            requested_status.as_db_value()
        ),
    ))
}

fn workflow_external_completion_invalid_status_error(status: WorkflowStepStatus) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.external_step_invalid_terminal_status",
        "External workflow step completion status is invalid.",
        format!(
            "external workflow step completion requires SUCCEEDED, FAILED, or CANCELED; got {}",
            status.as_db_value()
        ),
    ))
}

fn workflow_run_not_found_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.run_not_found",
        "Workflow run was not found.",
        "workflow run lookup failed",
    ))
}

fn workflow_append_blank_mutation_key_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.append_mutation_key_blank",
        "Workflow append mutation key must be non-empty.",
        "workflow append mutation key must be non-empty",
    ))
}

fn workflow_append_terminal_run_error(status: WorkflowRunStatus) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.append_terminal_run",
        "Workflow steps cannot be appended to a terminal workflow run.",
        format!(
            "workflow append rejected because run is terminal: status={}",
            status.as_db_value()
        ),
    ))
}

fn workflow_append_conflicting_retry_error(mutation_key: &str) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.append_conflicting_retry",
        "Workflow append retry conflicts with the existing mutation.",
        format!(
            "workflow append mutation '{mutation_key}' does not match the already-recorded request"
        ),
    ))
}

fn workflow_append_window_missing_error(step_key: &str) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.append_window_missing",
        "Workflow append window was not found.",
        format!("workflow append window step '{step_key}' was not found"),
    ))
}

fn workflow_append_window_not_external_error(step_key: &str) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.append_window_not_external",
        "Workflow append window must be an external gate.",
        format!("workflow append window step '{step_key}' is not an EXTERNAL step"),
    ))
}

fn workflow_append_window_not_open_error(step_key: &str, status: WorkflowStepStatus) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.append_window_not_open",
        "Workflow append window is no longer open.",
        format!(
            "workflow append window step '{step_key}' is not waiting for external completion; current status={}",
            status.as_db_value()
        ),
    ))
}

pub(crate) use self::validation::workflow_dag_validation_error;
pub(crate) use self::validation::workflow_dependency_count_overflow_error;
