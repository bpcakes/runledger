use super::super::{WorkflowBuildError, WorkflowDagValidationError};

#[test]
fn workflow_build_error_maps_all_dag_validation_variants() {
    let mapping_cases = vec![
        (
            WorkflowDagValidationError::EmptySteps,
            WorkflowBuildError::EmptySteps,
        ),
        (
            WorkflowDagValidationError::BlankWorkflowType,
            WorkflowBuildError::BlankWorkflowType,
        ),
        (
            WorkflowDagValidationError::BlankStepKey { step_index: 7 },
            WorkflowBuildError::BlankStepKey {
                step_index: Some(7),
            },
        ),
        (
            WorkflowDagValidationError::BlankStepJobType {
                step_key: "step.a".to_owned(),
            },
            WorkflowBuildError::BlankStepJobType {
                step_key: "step.a".to_owned(),
            },
        ),
        (
            WorkflowDagValidationError::ExternalStepJobTypeNotAllowed {
                step_key: "step.a".to_owned(),
            },
            WorkflowBuildError::ExternalStepJobTypeNotAllowed {
                step_key: "step.a".to_owned(),
            },
        ),
        (
            WorkflowDagValidationError::ExternalStepQueueSettingsNotAllowed {
                step_key: "step.a".to_owned(),
            },
            WorkflowBuildError::ExternalStepQueueSettingsNotAllowed {
                step_key: "step.a".to_owned(),
            },
        ),
        (
            WorkflowDagValidationError::BlankDependencyStepKey {
                step_key: "step.a".to_owned(),
            },
            WorkflowBuildError::BlankDependencyStepKey {
                step_key: "step.a".to_owned(),
            },
        ),
        (
            WorkflowDagValidationError::DuplicateStepKey {
                step_key: "step.a".to_owned(),
            },
            WorkflowBuildError::DuplicateStepKey {
                step_key: "step.a".to_owned(),
            },
        ),
        (
            WorkflowDagValidationError::MissingDependency {
                step_key: "step.a".to_owned(),
                prerequisite_step_key: "step.b".to_owned(),
            },
            WorkflowBuildError::MissingDependency {
                step_key: "step.a".to_owned(),
                prerequisite_step_key: "step.b".to_owned(),
            },
        ),
        (
            WorkflowDagValidationError::SelfDependency {
                step_key: "step.a".to_owned(),
            },
            WorkflowBuildError::SelfDependency {
                step_key: "step.a".to_owned(),
            },
        ),
        (
            WorkflowDagValidationError::DuplicateDependency {
                step_key: "step.a".to_owned(),
                prerequisite_step_key: "step.b".to_owned(),
            },
            WorkflowBuildError::DuplicateDependency {
                step_key: "step.a".to_owned(),
                prerequisite_step_key: "step.b".to_owned(),
            },
        ),
        (
            WorkflowDagValidationError::CycleDetected,
            WorkflowBuildError::CycleDetected,
        ),
    ];

    for (validation_error, expected_build_error) in mapping_cases {
        assert_eq!(
            WorkflowBuildError::from(validation_error),
            expected_build_error
        );
    }
}
