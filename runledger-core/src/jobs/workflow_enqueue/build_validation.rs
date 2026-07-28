use std::collections::BTreeSet;

use super::super::identifiers::{JobType, StepKey};
use super::dag_validation::validate_workflow_run_enqueue;
use super::errors::WorkflowBuildError;
use super::types::{WorkflowRunEnqueue, WorkflowStepEnqueue, WorkflowStepExecutionKind};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum WorkflowStepBuildValidationError {
    BlankStepKey {
        step_index: Option<usize>,
    },
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

impl From<WorkflowStepBuildValidationError> for WorkflowBuildError {
    fn from(error: WorkflowStepBuildValidationError) -> Self {
        match error {
            WorkflowStepBuildValidationError::BlankStepKey { step_index } => {
                Self::BlankStepKey { step_index }
            }
            WorkflowStepBuildValidationError::BlankStepJobType { step_key } => {
                Self::BlankStepJobType { step_key }
            }
            WorkflowStepBuildValidationError::NonPositiveStepMaxAttempts {
                step_key,
                max_attempts,
            } => Self::NonPositiveStepMaxAttempts {
                step_key,
                max_attempts,
            },
            WorkflowStepBuildValidationError::NonPositiveStepTimeoutSeconds {
                step_key,
                timeout_seconds,
            } => Self::NonPositiveStepTimeoutSeconds {
                step_key,
                timeout_seconds,
            },
            WorkflowStepBuildValidationError::InvalidStepExecutionResourceKey { step_key } => {
                Self::InvalidStepExecutionResourceKey { step_key }
            }
            WorkflowStepBuildValidationError::ExternalStepJobTypeNotAllowed { step_key } => {
                Self::ExternalStepJobTypeNotAllowed { step_key }
            }
            WorkflowStepBuildValidationError::ExternalStepQueueSettingsNotAllowed { step_key } => {
                Self::ExternalStepQueueSettingsNotAllowed { step_key }
            }
            WorkflowStepBuildValidationError::BlankDependencyStepKey { step_key } => {
                Self::BlankDependencyStepKey { step_key }
            }
            WorkflowStepBuildValidationError::DuplicateDependency {
                step_key,
                prerequisite_step_key,
            } => Self::DuplicateDependency {
                step_key,
                prerequisite_step_key,
            },
            WorkflowStepBuildValidationError::SelfDependency { step_key } => {
                Self::SelfDependency { step_key }
            }
        }
    }
}

pub(super) fn validate_step_enqueue(
    step: &WorkflowStepEnqueue<'_>,
    step_index: Option<usize>,
) -> Result<(), WorkflowStepBuildValidationError> {
    if StepKey::try_new(step.step_key.as_str()).is_err() {
        return Err(WorkflowStepBuildValidationError::BlankStepKey { step_index });
    }
    match step.execution_kind {
        WorkflowStepExecutionKind::Job => {
            let Some(job_type) = step.job_type else {
                return Err(WorkflowStepBuildValidationError::BlankStepJobType {
                    step_key: step.step_key.as_str().to_owned(),
                });
            };
            if JobType::try_new(job_type.as_str()).is_err() {
                return Err(WorkflowStepBuildValidationError::BlankStepJobType {
                    step_key: step.step_key.as_str().to_owned(),
                });
            }
            if let Some(max_attempts) = step.max_attempts
                && max_attempts <= 0
            {
                return Err(
                    WorkflowStepBuildValidationError::NonPositiveStepMaxAttempts {
                        step_key: step.step_key.as_str().to_owned(),
                        max_attempts,
                    },
                );
            }
            if let Some(timeout_seconds) = step.timeout_seconds
                && timeout_seconds <= 0
            {
                return Err(
                    WorkflowStepBuildValidationError::NonPositiveStepTimeoutSeconds {
                        step_key: step.step_key.as_str().to_owned(),
                        timeout_seconds,
                    },
                );
            }
            if let Some(resource_key) = step.execution_resource_key
                && (resource_key.trim().is_empty() || resource_key.len() > 512)
            {
                return Err(
                    WorkflowStepBuildValidationError::InvalidStepExecutionResourceKey {
                        step_key: step.step_key.as_str().to_owned(),
                    },
                );
            }
        }
        WorkflowStepExecutionKind::External => {
            if step.job_type.is_some() {
                return Err(
                    WorkflowStepBuildValidationError::ExternalStepJobTypeNotAllowed {
                        step_key: step.step_key.as_str().to_owned(),
                    },
                );
            }
            if step.priority.is_some()
                || step.max_attempts.is_some()
                || step.timeout_seconds.is_some()
                || step.stage.is_some()
                || step.allow_handler_continuation
                || step.execution_resource_key.is_some()
            {
                return Err(
                    WorkflowStepBuildValidationError::ExternalStepQueueSettingsNotAllowed {
                        step_key: step.step_key.as_str().to_owned(),
                    },
                );
            }
        }
    }
    let mut seen_dependencies: BTreeSet<&str> = BTreeSet::new();
    for dependency in &step.dependencies {
        if StepKey::try_new(dependency.prerequisite_step_key.as_str()).is_err() {
            return Err(WorkflowStepBuildValidationError::BlankDependencyStepKey {
                step_key: step.step_key.as_str().to_owned(),
            });
        }
        if dependency.prerequisite_step_key == step.step_key {
            return Err(WorkflowStepBuildValidationError::SelfDependency {
                step_key: step.step_key.as_str().to_owned(),
            });
        }
        if !seen_dependencies.insert(dependency.prerequisite_step_key.as_str()) {
            return Err(WorkflowStepBuildValidationError::DuplicateDependency {
                step_key: step.step_key.as_str().to_owned(),
                prerequisite_step_key: dependency.prerequisite_step_key.as_str().to_owned(),
            });
        }
    }

    Ok(())
}

pub(super) fn validate_workflow_enqueue(
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<(), WorkflowBuildError> {
    validate_workflow_run_enqueue(payload).map_err(Into::into)
}
