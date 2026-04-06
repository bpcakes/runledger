use runledger_core::jobs::{
    JobEventType, JobStage, JobStatus, JobTypeName, StepKeyName, WorkflowDependencyReleaseMode,
    WorkflowRunStatus, WorkflowStepExecutionKind, WorkflowStepStatus, WorkflowTypeName,
};

use crate::{Error, QueryError, QueryErrorCategory, Result};

fn parse_persisted_enum<T>(
    raw_value: String,
    code: &'static str,
    client_message: &'static str,
    internal_message: &'static str,
) -> Result<T>
where
    T: std::str::FromStr<Err = ()>,
{
    raw_value.parse().map_err(|_| {
        Error::QueryError(QueryError::from_classified(
            QueryErrorCategory::Internal,
            code,
            client_message,
            internal_message,
        ))
    })
}

pub(super) fn parse_job_stage(raw_stage: String) -> Result<JobStage> {
    parse_persisted_enum(
        raw_stage,
        "job.invalid_stage",
        "Job stage in persisted row is invalid.",
        "invalid job stage in row",
    )
}

fn parse_identifier<T>(
    raw_value: String,
    blank_error: &'static str,
    client_message: &'static str,
    internal_message: &'static str,
) -> Result<T>
where
    T: TryFrom<String, Error = runledger_core::jobs::IdentifierValidationError>,
{
    T::try_from(raw_value).map_err(|_| {
        Error::QueryError(QueryError::from_classified(
            QueryErrorCategory::Internal,
            blank_error,
            client_message,
            internal_message,
        ))
    })
}

pub(super) fn parse_job_type_name(raw_job_type: String) -> Result<JobTypeName> {
    parse_identifier(
        raw_job_type,
        "job.invalid_job_type",
        "Job type in persisted row is invalid.",
        "invalid job type in row",
    )
}

pub(super) fn parse_workflow_type_name(raw_workflow_type: String) -> Result<WorkflowTypeName> {
    parse_identifier(
        raw_workflow_type,
        "workflow.invalid_workflow_type",
        "Workflow type in persisted row is invalid.",
        "invalid workflow type in row",
    )
}

pub(super) fn parse_step_key_name(raw_step_key: String) -> Result<StepKeyName> {
    parse_identifier(
        raw_step_key,
        "workflow.invalid_step_key",
        "Workflow step key in persisted row is invalid.",
        "invalid workflow step key in row",
    )
}

pub(super) fn parse_job_status(raw_status: String) -> Result<JobStatus> {
    parse_persisted_enum(
        raw_status,
        "job.invalid_status",
        "Job status in persisted row is invalid.",
        "invalid job status in row",
    )
}

pub(super) fn parse_job_event_type(raw_event_type: String) -> Result<JobEventType> {
    parse_persisted_enum(
        raw_event_type,
        "job.invalid_event_type",
        "Job event type in persisted row is invalid.",
        "invalid job event type in row",
    )
}

pub(super) fn parse_workflow_run_status(raw_status: String) -> Result<WorkflowRunStatus> {
    parse_persisted_enum(
        raw_status,
        "workflow.invalid_run_status",
        "Workflow run status in persisted row is invalid.",
        "invalid workflow run status in row",
    )
}

pub(super) fn parse_workflow_step_status(raw_status: String) -> Result<WorkflowStepStatus> {
    parse_persisted_enum(
        raw_status,
        "workflow.invalid_step_status",
        "Workflow step status in persisted row is invalid.",
        "invalid workflow step status in row",
    )
}

pub(super) fn parse_workflow_step_execution_kind(
    raw_kind: String,
) -> Result<WorkflowStepExecutionKind> {
    parse_persisted_enum(
        raw_kind,
        "workflow.invalid_step_execution_kind",
        "Workflow step execution kind in persisted row is invalid.",
        "invalid workflow step execution kind in row",
    )
}

pub(super) fn parse_workflow_release_mode(
    raw_mode: String,
) -> Result<WorkflowDependencyReleaseMode> {
    parse_persisted_enum(
        raw_mode,
        "workflow.invalid_dependency_release_mode",
        "Workflow dependency release mode in persisted row is invalid.",
        "invalid workflow dependency release mode in row",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_workflow_run_status_returns_internal_error_for_invalid_value() {
        let result = parse_workflow_run_status("NOT_A_REAL_STATUS".to_string());

        match result {
            Err(crate::Error::QueryError(query_error)) => {
                assert_eq!(query_error.category(), QueryErrorCategory::Internal);
                assert_eq!(query_error.code(), "workflow.invalid_run_status");
            }
            other => panic!("expected internal parse error, got {other:?}"),
        }
    }

    #[test]
    fn parse_workflow_step_status_returns_internal_error_for_invalid_value() {
        let result = parse_workflow_step_status("NOT_A_REAL_STATUS".to_string());

        match result {
            Err(crate::Error::QueryError(query_error)) => {
                assert_eq!(query_error.category(), QueryErrorCategory::Internal);
                assert_eq!(query_error.code(), "workflow.invalid_step_status");
            }
            other => panic!("expected internal parse error, got {other:?}"),
        }
    }

    #[test]
    fn parse_workflow_release_mode_returns_internal_error_for_invalid_value() {
        let result = parse_workflow_release_mode("NOT_A_REAL_MODE".to_string());

        match result {
            Err(crate::Error::QueryError(query_error)) => {
                assert_eq!(query_error.category(), QueryErrorCategory::Internal);
                assert_eq!(
                    query_error.code(),
                    "workflow.invalid_dependency_release_mode"
                );
            }
            other => panic!("expected internal parse error, got {other:?}"),
        }
    }

    #[test]
    fn parse_workflow_step_execution_kind_returns_internal_error_for_invalid_value() {
        let result = parse_workflow_step_execution_kind("NOT_A_REAL_KIND".to_string());

        match result {
            Err(crate::Error::QueryError(query_error)) => {
                assert_eq!(query_error.category(), QueryErrorCategory::Internal);
                assert_eq!(query_error.code(), "workflow.invalid_step_execution_kind");
            }
            other => panic!("expected internal parse error, got {other:?}"),
        }
    }
}
