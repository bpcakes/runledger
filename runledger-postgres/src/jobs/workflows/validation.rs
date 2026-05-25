use std::num::TryFromIntError;

use runledger_core::jobs::WorkflowDagValidationError;

use crate::{Error, QueryError, QueryErrorCategory};

pub(crate) fn workflow_dag_validation_error(error: WorkflowDagValidationError) -> Error {
    let (code, client_message) = match error {
        WorkflowDagValidationError::EmptySteps => (
            "workflow.invalid_dag_empty",
            "Workflow must include at least one step.",
        ),
        WorkflowDagValidationError::BlankWorkflowType => (
            "workflow.invalid_dag_workflow_type_blank",
            "Workflow type must be non-empty.",
        ),
        WorkflowDagValidationError::BlankStepKey { .. } => (
            "workflow.invalid_dag_step_key_blank",
            "Workflow step key must be non-empty.",
        ),
        WorkflowDagValidationError::BlankStepJobType { .. } => (
            "workflow.invalid_dag_job_type_blank",
            "Workflow step job type must be non-empty.",
        ),
        WorkflowDagValidationError::BlankIdempotencyKey => (
            "workflow.invalid_dag_idempotency_key_blank",
            "Workflow idempotency key must be non-empty.",
        ),
        WorkflowDagValidationError::NonPositiveStepMaxAttempts { .. } => (
            "workflow.invalid_dag_step_max_attempts_non_positive",
            "Workflow step max attempts must be positive.",
        ),
        WorkflowDagValidationError::NonPositiveStepTimeoutSeconds { .. } => (
            "workflow.invalid_dag_step_timeout_seconds_non_positive",
            "Workflow step timeout seconds must be positive.",
        ),
        WorkflowDagValidationError::ExternalStepJobTypeNotAllowed { .. } => (
            "workflow.invalid_dag_external_step_job_type_not_allowed",
            "External workflow steps cannot define a job type.",
        ),
        WorkflowDagValidationError::ExternalStepQueueSettingsNotAllowed { .. } => (
            "workflow.invalid_dag_external_step_queue_settings_not_allowed",
            "External workflow steps cannot define queue settings.",
        ),
        WorkflowDagValidationError::BlankDependencyStepKey { .. } => (
            "workflow.invalid_dag_dependency_step_key_blank",
            "Workflow dependency prerequisite step key must be non-empty.",
        ),
        WorkflowDagValidationError::DuplicateStepKey { .. } => (
            "workflow.invalid_dag_duplicate_step_key",
            "Workflow step keys must be unique.",
        ),
        WorkflowDagValidationError::MissingDependency { .. } => (
            "workflow.invalid_dag_missing_dependency",
            "Workflow dependency references an unknown step.",
        ),
        WorkflowDagValidationError::SelfDependency { .. } => (
            "workflow.invalid_dag_self_dependency",
            "Workflow steps cannot depend on themselves.",
        ),
        WorkflowDagValidationError::DuplicateDependency { .. } => (
            "workflow.invalid_dag_duplicate_dependency",
            "Workflow step dependencies must be unique per prerequisite.",
        ),
        WorkflowDagValidationError::CycleDetected => (
            "workflow.invalid_dag_cycle",
            "Workflow dependencies must form an acyclic graph.",
        ),
    };

    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        code,
        client_message,
        format!("workflow DAG validation failed: {error:?}"),
    ))
}

pub(crate) fn workflow_dependency_count_overflow_error(
    step_key: &str,
    dependency_count: usize,
    source: TryFromIntError,
) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.invalid_dag_dependency_count_overflow",
        "Workflow dependency count is too large.",
        format!(
            "workflow DAG validation failed: {dependency_count} dependencies for step '{step_key}' exceed i32 range: {source}"
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QueryErrorCategory;

    fn assert_validation_error_mapping(
        dag_error: WorkflowDagValidationError,
        expected_code: &'static str,
        expected_client_message: &'static str,
    ) {
        let error = workflow_dag_validation_error(dag_error);

        let Error::QueryError(query_error) = error else {
            panic!("expected query error");
        };
        assert_eq!(query_error.category(), QueryErrorCategory::Validation);
        assert_eq!(query_error.code(), expected_code);
        assert_eq!(query_error.client_message(), expected_client_message);
    }

    #[test]
    fn maps_dag_errors_to_validation_query_errors() {
        let cases = vec![
            (
                WorkflowDagValidationError::BlankWorkflowType,
                "workflow.invalid_dag_workflow_type_blank",
                "Workflow type must be non-empty.",
            ),
            (
                WorkflowDagValidationError::DuplicateStepKey {
                    step_key: "dup.step".to_owned(),
                },
                "workflow.invalid_dag_duplicate_step_key",
                "Workflow step keys must be unique.",
            ),
            (
                WorkflowDagValidationError::MissingDependency {
                    step_key: "step.a".to_owned(),
                    prerequisite_step_key: "step.b".to_owned(),
                },
                "workflow.invalid_dag_missing_dependency",
                "Workflow dependency references an unknown step.",
            ),
            (
                WorkflowDagValidationError::SelfDependency {
                    step_key: "step.a".to_owned(),
                },
                "workflow.invalid_dag_self_dependency",
                "Workflow steps cannot depend on themselves.",
            ),
            (
                WorkflowDagValidationError::DuplicateDependency {
                    step_key: "step.a".to_owned(),
                    prerequisite_step_key: "step.b".to_owned(),
                },
                "workflow.invalid_dag_duplicate_dependency",
                "Workflow step dependencies must be unique per prerequisite.",
            ),
            (
                WorkflowDagValidationError::BlankDependencyStepKey {
                    step_key: "step.a".to_owned(),
                },
                "workflow.invalid_dag_dependency_step_key_blank",
                "Workflow dependency prerequisite step key must be non-empty.",
            ),
            (
                WorkflowDagValidationError::BlankStepJobType {
                    step_key: "step.a".to_owned(),
                },
                "workflow.invalid_dag_job_type_blank",
                "Workflow step job type must be non-empty.",
            ),
            (
                WorkflowDagValidationError::BlankIdempotencyKey,
                "workflow.invalid_dag_idempotency_key_blank",
                "Workflow idempotency key must be non-empty.",
            ),
            (
                WorkflowDagValidationError::NonPositiveStepMaxAttempts {
                    step_key: "step.a".to_owned(),
                    max_attempts: 0,
                },
                "workflow.invalid_dag_step_max_attempts_non_positive",
                "Workflow step max attempts must be positive.",
            ),
            (
                WorkflowDagValidationError::NonPositiveStepTimeoutSeconds {
                    step_key: "step.a".to_owned(),
                    timeout_seconds: 0,
                },
                "workflow.invalid_dag_step_timeout_seconds_non_positive",
                "Workflow step timeout seconds must be positive.",
            ),
            (
                WorkflowDagValidationError::ExternalStepJobTypeNotAllowed {
                    step_key: "step.a".to_owned(),
                },
                "workflow.invalid_dag_external_step_job_type_not_allowed",
                "External workflow steps cannot define a job type.",
            ),
            (
                WorkflowDagValidationError::ExternalStepQueueSettingsNotAllowed {
                    step_key: "step.a".to_owned(),
                },
                "workflow.invalid_dag_external_step_queue_settings_not_allowed",
                "External workflow steps cannot define queue settings.",
            ),
        ];

        for (dag_error, expected_code, expected_client_message) in cases {
            assert_validation_error_mapping(dag_error, expected_code, expected_client_message);
        }
    }

    #[test]
    fn maps_dependency_overflow_to_validation_query_error() {
        let dependency_count = i32::MAX as usize + 1;
        let source = i32::try_from(dependency_count).expect_err("dependency count exceeds i32");
        let source_message = source.to_string();
        let error = workflow_dependency_count_overflow_error("step.a", dependency_count, source);

        let Error::QueryError(query_error) = error else {
            panic!("expected query error");
        };
        assert_eq!(query_error.category(), QueryErrorCategory::Validation);
        assert_eq!(
            query_error.code(),
            "workflow.invalid_dag_dependency_count_overflow"
        );
        assert!(
            query_error.internal_message().contains("step.a"),
            "unexpected internal message: {}",
            query_error.internal_message()
        );
        assert!(
            query_error
                .internal_message()
                .contains(&dependency_count.to_string()),
            "unexpected internal message: {}",
            query_error.internal_message()
        );
        assert!(
            query_error.internal_message().contains(&source_message),
            "unexpected internal message: {}",
            query_error.internal_message()
        );
    }
}
