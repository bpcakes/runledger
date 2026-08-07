use std::collections::BTreeSet;

use super::dag_validation::validate_workflow_run_enqueue;
use super::errors::WorkflowBuildError;
use super::step_validation::{
    WorkflowStepShapeValidationInput, WorkflowStepValidationError, validate_step_dependency_shape,
    validate_step_shape,
};
use super::types::{WorkflowRunEnqueue, WorkflowStepEnqueue};

impl From<WorkflowStepValidationError> for WorkflowBuildError {
    fn from(error: WorkflowStepValidationError) -> Self {
        match error {
            WorkflowStepValidationError::BlankStepKey => Self::BlankStepKey { step_index: None },
            WorkflowStepValidationError::BlankStepJobType { step_key } => {
                Self::BlankStepJobType { step_key }
            }
            WorkflowStepValidationError::NonPositiveStepMaxAttempts {
                step_key,
                max_attempts,
            } => Self::NonPositiveStepMaxAttempts {
                step_key,
                max_attempts,
            },
            WorkflowStepValidationError::NonPositiveStepTimeoutSeconds {
                step_key,
                timeout_seconds,
            } => Self::NonPositiveStepTimeoutSeconds {
                step_key,
                timeout_seconds,
            },
            WorkflowStepValidationError::InvalidStepExecutionResourceKey { step_key } => {
                Self::InvalidStepExecutionResourceKey { step_key }
            }
            WorkflowStepValidationError::ExternalStepJobTypeNotAllowed { step_key } => {
                Self::ExternalStepJobTypeNotAllowed { step_key }
            }
            WorkflowStepValidationError::ExternalStepQueueSettingsNotAllowed { step_key } => {
                Self::ExternalStepQueueSettingsNotAllowed { step_key }
            }
            WorkflowStepValidationError::BlankDependencyStepKey { step_key } => {
                Self::BlankDependencyStepKey { step_key }
            }
            WorkflowStepValidationError::DuplicateDependency {
                step_key,
                prerequisite_step_key,
            } => Self::DuplicateDependency {
                step_key,
                prerequisite_step_key,
            },
            WorkflowStepValidationError::SelfDependency { step_key } => {
                Self::SelfDependency { step_key }
            }
        }
    }
}

pub(super) fn validate_step_enqueue(
    step: &WorkflowStepEnqueue<'_>,
) -> Result<(), WorkflowStepValidationError> {
    validate_step_shape(WorkflowStepShapeValidationInput {
        step_key: step.step_key.as_str(),
        execution_kind: step.execution_kind,
        job_type: step.job_type.map(|job_type| job_type.as_str()),
        priority: step.priority,
        max_attempts: step.max_attempts,
        timeout_seconds: step.timeout_seconds,
        stage: step.stage,
        allow_handler_continuation: step.allow_handler_continuation,
        execution_resource_key: step.execution_resource_key,
    })?;

    let mut seen_dependencies: BTreeSet<&str> = BTreeSet::new();
    for dependency in &step.dependencies {
        validate_step_dependency_shape(
            step.step_key.as_str(),
            dependency.prerequisite_step_key.as_str(),
            &mut seen_dependencies,
        )?;
    }

    Ok(())
}

pub(super) fn validate_workflow_enqueue(
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<(), WorkflowBuildError> {
    validate_workflow_run_enqueue(payload).map_err(Into::into)
}
