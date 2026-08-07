use std::collections::BTreeSet;

use super::super::identifiers::{JobType, StepKey};
use super::super::status::JobStage;
use super::types::WorkflowStepExecutionKind;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum WorkflowStepValidationError {
    BlankStepKey,
    BlankStepJobType {
        step_key: String,
    },
    NonPositiveStepMaxAttempts {
        step_key: String,
        max_attempts: i32,
    },
    NonPositiveStepTimeoutSeconds {
        step_key: String,
        timeout_seconds: i32,
    },
    InvalidStepExecutionResourceKey {
        step_key: String,
    },
    ExternalStepJobTypeNotAllowed {
        step_key: String,
    },
    ExternalStepQueueSettingsNotAllowed {
        step_key: String,
    },
    BlankDependencyStepKey {
        step_key: String,
    },
    DuplicateDependency {
        step_key: String,
        prerequisite_step_key: String,
    },
    SelfDependency {
        step_key: String,
    },
}

#[derive(Debug, Clone, Copy)]
pub(super) struct WorkflowStepShapeValidationInput<'a> {
    pub(super) step_key: &'a str,
    pub(super) execution_kind: WorkflowStepExecutionKind,
    pub(super) job_type: Option<&'a str>,
    pub(super) priority: Option<i32>,
    pub(super) max_attempts: Option<i32>,
    pub(super) timeout_seconds: Option<i32>,
    pub(super) stage: Option<JobStage>,
    pub(super) allow_handler_continuation: bool,
    pub(super) execution_resource_key: Option<&'a str>,
}

pub(super) fn validate_step_shape(
    step: WorkflowStepShapeValidationInput<'_>,
) -> Result<(), WorkflowStepValidationError> {
    if StepKey::try_new(step.step_key).is_err() {
        return Err(WorkflowStepValidationError::BlankStepKey);
    }

    match step.execution_kind {
        WorkflowStepExecutionKind::Job => {
            let Some(job_type) = step.job_type else {
                return Err(WorkflowStepValidationError::BlankStepJobType {
                    step_key: step.step_key.to_owned(),
                });
            };
            if JobType::try_new(job_type).is_err() {
                return Err(WorkflowStepValidationError::BlankStepJobType {
                    step_key: step.step_key.to_owned(),
                });
            }
            if let Some(max_attempts) = step.max_attempts
                && max_attempts <= 0
            {
                return Err(WorkflowStepValidationError::NonPositiveStepMaxAttempts {
                    step_key: step.step_key.to_owned(),
                    max_attempts,
                });
            }
            if let Some(timeout_seconds) = step.timeout_seconds
                && timeout_seconds <= 0
            {
                return Err(WorkflowStepValidationError::NonPositiveStepTimeoutSeconds {
                    step_key: step.step_key.to_owned(),
                    timeout_seconds,
                });
            }
            if let Some(resource_key) = step.execution_resource_key
                && (resource_key.trim().is_empty() || resource_key.len() > 512)
            {
                return Err(
                    WorkflowStepValidationError::InvalidStepExecutionResourceKey {
                        step_key: step.step_key.to_owned(),
                    },
                );
            }
        }
        WorkflowStepExecutionKind::External => {
            if step.job_type.is_some() {
                return Err(WorkflowStepValidationError::ExternalStepJobTypeNotAllowed {
                    step_key: step.step_key.to_owned(),
                });
            }
            if step.priority.is_some()
                || step.max_attempts.is_some()
                || step.timeout_seconds.is_some()
                || step.stage.is_some()
                || step.allow_handler_continuation
                || step.execution_resource_key.is_some()
            {
                return Err(
                    WorkflowStepValidationError::ExternalStepQueueSettingsNotAllowed {
                        step_key: step.step_key.to_owned(),
                    },
                );
            }
        }
    }

    Ok(())
}

pub(super) fn validate_step_dependency_shape<'dependency>(
    step_key: &str,
    prerequisite_step_key: &'dependency str,
    seen_dependencies: &mut BTreeSet<&'dependency str>,
) -> Result<(), WorkflowStepValidationError> {
    if StepKey::try_new(prerequisite_step_key).is_err() {
        return Err(WorkflowStepValidationError::BlankDependencyStepKey {
            step_key: step_key.to_owned(),
        });
    }
    if prerequisite_step_key == step_key {
        return Err(WorkflowStepValidationError::SelfDependency {
            step_key: step_key.to_owned(),
        });
    }
    if !seen_dependencies.insert(prerequisite_step_key) {
        return Err(WorkflowStepValidationError::DuplicateDependency {
            step_key: step_key.to_owned(),
            prerequisite_step_key: prerequisite_step_key.to_owned(),
        });
    }

    Ok(())
}
