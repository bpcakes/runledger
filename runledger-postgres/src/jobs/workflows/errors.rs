use runledger_core::jobs::{WorkflowRunStatus, WorkflowStepStatus};
use sqlx::types::Uuid;

use crate::{Error, QueryError, QueryErrorCategory, QueryErrorKind};

pub(in crate::jobs::workflows) fn workflow_definition_not_available_error(job_type: &str) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.definition_not_found_or_disabled",
        "Workflow step job type is not available.",
        format!("workflow step job definition missing or disabled: {job_type}"),
    ))
}

pub(in crate::jobs::workflows) fn workflow_internal_state_error(
    message: impl Into<String>,
) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Internal,
        "workflow.internal_state",
        "Workflow state is invalid.",
        message,
    ))
}

pub(in crate::jobs::workflows) fn workflow_active_key_required_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.active_key_required",
        "Active workflow enqueue requires an active key.",
        "enqueue_or_get_active_workflow payload did not include active_key",
    ))
}

pub(in crate::jobs::workflows) fn workflow_active_key_api_required_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.active_key_api_required",
        "Use the active workflow enqueue API for payloads with an active key.",
        "enqueue_workflow_run cannot discard the explicit active-collision outcome",
    ))
}

pub(in crate::jobs::workflows) fn workflow_release_conflict_error(workflow_run_id: Uuid) -> Error {
    Error::QueryError(QueryError::from_kind(
        QueryErrorKind::WorkflowReleaseConflict,
        format!("workflow run {workflow_run_id} is locked for an exclusive mutation"),
    ))
}

pub(in crate::jobs::workflows) fn workflow_release_conflict_timeout_error(
    workflow_run_id: Uuid,
    source: sqlx::Error,
) -> Error {
    Error::QueryError(QueryError::from_sqlx_with_kind(
        QueryErrorKind::WorkflowReleaseConflict,
        format!("workflow run {workflow_run_id} timed out acquiring shared release advisory lock"),
        source,
    ))
}

pub(in crate::jobs::workflows) fn workflow_external_step_not_found_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.external_step_not_found",
        "External workflow step was not found.",
        "external workflow step lookup failed",
    ))
}

pub(in crate::jobs::workflows) fn workflow_external_step_not_external_error(
    step_key: &str,
) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.external_step_not_external",
        "Workflow step is not an external gate.",
        format!("workflow step '{step_key}' is not an EXTERNAL step"),
    ))
}

pub(in crate::jobs::workflows) fn workflow_external_step_not_waiting_error(
    step_key: &str,
    status: WorkflowStepStatus,
) -> Error {
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

pub(in crate::jobs::workflows) fn workflow_external_completion_conflict_error(
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

pub(in crate::jobs::workflows) fn workflow_external_completion_invalid_status_error(
    status: WorkflowStepStatus,
) -> Error {
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

pub(in crate::jobs::workflows) fn workflow_external_completion_output_invalid_error(
    status: WorkflowStepStatus,
) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.external_step_output_invalid_status",
        "External workflow step output can only be supplied with a successful completion.",
        format!(
            "external workflow step output requires SUCCEEDED terminal status; got {}",
            status.as_db_value()
        ),
    ))
}

pub(in crate::jobs::workflows) fn workflow_external_completion_output_conflict_error(
    step_key: &str,
) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Conflict,
        "workflow.external_step_conflicting_output_retry",
        "External workflow step was already completed with a different output.",
        format!("workflow step '{step_key}' was already completed with different output"),
    ))
}

pub(in crate::jobs::workflows) fn workflow_external_completion_metadata_conflict_error(
    step_key: &str,
) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Conflict,
        "workflow.external_step_conflicting_completion_retry",
        "External workflow step was already completed with a different result.",
        format!(
            "workflow step '{step_key}' was already completed with different completion metadata"
        ),
    ))
}

pub(in crate::jobs::workflows) fn workflow_run_not_found_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.run_not_found",
        "Workflow run was not found.",
        "workflow run lookup failed",
    ))
}

pub(in crate::jobs::workflows) fn workflow_append_blank_mutation_key_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.append_mutation_key_blank",
        "Workflow append mutation key must be non-empty.",
        "workflow append mutation key must be non-empty",
    ))
}

pub(in crate::jobs::workflows) fn workflow_append_terminal_run_error(
    status: WorkflowRunStatus,
) -> Error {
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

pub(in crate::jobs::workflows) fn workflow_append_conflicting_retry_error(
    mutation_key: &str,
) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Conflict,
        "workflow.append_conflicting_retry",
        "Workflow append retry conflicts with the existing mutation.",
        format!(
            "workflow append mutation '{mutation_key}' does not match the already-recorded request"
        ),
    ))
}

pub(in crate::jobs::workflows) fn workflow_enqueue_conflicting_retry_error(field: &str) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Conflict,
        "workflow.enqueue_conflicting_retry",
        "Workflow enqueue retry conflicts with the existing idempotency key.",
        format!(
            "workflow enqueue idempotency key conflicts with existing workflow run field '{field}'"
        ),
    ))
}

pub(in crate::jobs::workflows) fn workflow_legacy_idempotency_snapshot_missing_error(
    workflow_type: &str,
    workflow_run_id: Uuid,
) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Conflict,
        "workflow.legacy_idempotency_snapshot_missing",
        "Workflow enqueue retry cannot be resolved because the existing idempotency key is missing its request snapshot.",
        format!(
            "workflow enqueue idempotency retry for workflow_type={workflow_type} matched legacy workflow run {workflow_run_id} without enqueue_request snapshot"
        ),
    ))
}

pub(in crate::jobs::workflows) fn workflow_append_window_missing_error(step_key: &str) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.append_window_missing",
        "Workflow append window was not found.",
        format!("workflow append window step '{step_key}' was not found"),
    ))
}

pub(in crate::jobs::workflows) fn workflow_append_window_not_external_error(
    step_key: &str,
) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.append_window_not_external",
        "Workflow append window must be an external gate.",
        format!("workflow append window step '{step_key}' is not an EXTERNAL step"),
    ))
}

pub(in crate::jobs::workflows) fn workflow_append_window_not_open_error(
    step_key: &str,
    status: WorkflowStepStatus,
) -> Error {
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
