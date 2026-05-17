use std::fmt;

use super::dag_validation::WorkflowDagValidationError;

/// Error returned when building workflow enqueue payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowBuildError {
    BlankWorkflowType,
    EmptySteps,
    BlankStepKey {
        step_index: Option<usize>,
    },
    BlankStepJobType {
        step_key: String,
    },
    BlankIdempotencyKey,
    NonPositiveStepMaxAttempts {
        step_key: String,
        max_attempts: i32,
    },
    NonPositiveStepTimeoutSeconds {
        step_key: String,
        timeout_seconds: i32,
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
    DuplicateStepKey {
        step_key: String,
    },
    MissingDependency {
        step_key: String,
        prerequisite_step_key: String,
    },
    DuplicateDependency {
        step_key: String,
        prerequisite_step_key: String,
    },
    SelfDependency {
        step_key: String,
    },
    CycleDetected,
}

impl From<WorkflowDagValidationError> for WorkflowBuildError {
    fn from(error: WorkflowDagValidationError) -> Self {
        match error {
            WorkflowDagValidationError::EmptySteps => Self::EmptySteps,
            WorkflowDagValidationError::BlankWorkflowType => Self::BlankWorkflowType,
            WorkflowDagValidationError::BlankStepKey { step_index } => Self::BlankStepKey {
                step_index: Some(step_index),
            },
            WorkflowDagValidationError::BlankStepJobType { step_key } => {
                Self::BlankStepJobType { step_key }
            }
            WorkflowDagValidationError::BlankIdempotencyKey => Self::BlankIdempotencyKey,
            WorkflowDagValidationError::NonPositiveStepMaxAttempts {
                step_key,
                max_attempts,
            } => Self::NonPositiveStepMaxAttempts {
                step_key,
                max_attempts,
            },
            WorkflowDagValidationError::NonPositiveStepTimeoutSeconds {
                step_key,
                timeout_seconds,
            } => Self::NonPositiveStepTimeoutSeconds {
                step_key,
                timeout_seconds,
            },
            WorkflowDagValidationError::ExternalStepJobTypeNotAllowed { step_key } => {
                Self::ExternalStepJobTypeNotAllowed { step_key }
            }
            WorkflowDagValidationError::ExternalStepQueueSettingsNotAllowed { step_key } => {
                Self::ExternalStepQueueSettingsNotAllowed { step_key }
            }
            WorkflowDagValidationError::BlankDependencyStepKey { step_key } => {
                Self::BlankDependencyStepKey { step_key }
            }
            WorkflowDagValidationError::DuplicateStepKey { step_key } => {
                Self::DuplicateStepKey { step_key }
            }
            WorkflowDagValidationError::MissingDependency {
                step_key,
                prerequisite_step_key,
            } => Self::MissingDependency {
                step_key,
                prerequisite_step_key,
            },
            WorkflowDagValidationError::SelfDependency { step_key } => {
                Self::SelfDependency { step_key }
            }
            WorkflowDagValidationError::DuplicateDependency {
                step_key,
                prerequisite_step_key,
            } => Self::DuplicateDependency {
                step_key,
                prerequisite_step_key,
            },
            WorkflowDagValidationError::CycleDetected => Self::CycleDetected,
        }
    }
}

impl fmt::Display for WorkflowBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BlankWorkflowType => write!(f, "workflow_type must be non-empty"),
            Self::EmptySteps => write!(f, "workflow must include at least one step"),
            Self::BlankStepKey {
                step_index: Some(step_index),
            } => {
                write!(f, "step key at index {step_index} must be non-empty")
            }
            Self::BlankStepKey { step_index: None } => {
                write!(f, "step key must be non-empty")
            }
            Self::BlankStepJobType { step_key } => {
                write!(f, "job_type for step '{step_key}' must be non-empty")
            }
            Self::BlankIdempotencyKey => write!(f, "idempotency_key must be non-empty"),
            Self::NonPositiveStepMaxAttempts {
                step_key,
                max_attempts,
            } => {
                write!(
                    f,
                    "max_attempts for step '{step_key}' must be positive; got {max_attempts}"
                )
            }
            Self::NonPositiveStepTimeoutSeconds {
                step_key,
                timeout_seconds,
            } => {
                write!(
                    f,
                    "timeout_seconds for step '{step_key}' must be positive; got {timeout_seconds}"
                )
            }
            Self::ExternalStepJobTypeNotAllowed { step_key } => {
                write!(f, "external step '{step_key}' cannot define a job_type")
            }
            Self::ExternalStepQueueSettingsNotAllowed { step_key } => {
                write!(f, "external step '{step_key}' cannot define queue settings")
            }
            Self::BlankDependencyStepKey { step_key } => {
                write!(
                    f,
                    "dependency prerequisite step key for step '{step_key}' must be non-empty"
                )
            }
            Self::DuplicateStepKey { step_key } => {
                write!(f, "duplicate step key '{step_key}'")
            }
            Self::MissingDependency {
                step_key,
                prerequisite_step_key,
            } => {
                write!(
                    f,
                    "step '{step_key}' references missing prerequisite step '{prerequisite_step_key}'"
                )
            }
            Self::DuplicateDependency {
                step_key,
                prerequisite_step_key,
            } => {
                write!(
                    f,
                    "step '{step_key}' has duplicate dependency on '{prerequisite_step_key}'"
                )
            }
            Self::SelfDependency { step_key } => {
                write!(f, "step '{step_key}' cannot depend on itself")
            }
            Self::CycleDetected => write!(f, "workflow contains at least one dependency cycle"),
        }
    }
}

impl std::error::Error for WorkflowBuildError {}
