use std::fmt;

use super::dag_validation::WorkflowDagValidationError;

/// Error returned when building workflow enqueue payloads.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum WorkflowBuildError {
    /// The workflow type was blank.
    BlankWorkflowType,
    /// The workflow did not include any steps.
    EmptySteps,
    /// A step key was blank.
    BlankStepKey {
        /// The step index when the blank key came from a step list.
        step_index: Option<usize>,
    },
    /// A job step had a blank job type.
    BlankStepJobType {
        /// The step whose job type was blank.
        step_key: String,
    },
    /// The workflow idempotency key was blank.
    BlankIdempotencyKey,
    /// The reusable workflow active key was blank.
    BlankActiveKey,
    /// The reusable workflow active key exceeded 512 bytes.
    ActiveKeyTooLong,
    /// The workflow result step key was blank.
    BlankResultStepKey,
    /// The workflow result step key does not match a step in the workflow.
    UnknownResultStepKey {
        /// The missing result step key.
        step_key: String,
    },
    /// A step max-attempts override was zero or negative.
    NonPositiveStepMaxAttempts {
        /// The step with the invalid max-attempts override.
        step_key: String,
        /// The invalid max-attempts value.
        max_attempts: i32,
    },
    /// A step timeout override was zero or negative.
    NonPositiveStepTimeoutSeconds {
        /// The step with the invalid timeout override.
        step_key: String,
        /// The invalid timeout value in seconds.
        timeout_seconds: i32,
    },
    /// A job step execution-resource key was blank or exceeded 512 bytes.
    InvalidStepExecutionResourceKey {
        /// The step with the invalid resource key.
        step_key: String,
    },
    /// An external step incorrectly supplied a job type.
    ExternalStepJobTypeNotAllowed {
        /// The external step with a job type.
        step_key: String,
    },
    /// An external step incorrectly supplied queue execution settings.
    ExternalStepQueueSettingsNotAllowed {
        /// The external step with queue settings.
        step_key: String,
    },
    /// A dependency prerequisite step key was blank.
    BlankDependencyStepKey {
        /// The step that owns the blank dependency.
        step_key: String,
    },
    /// The workflow declared the same step key more than once.
    DuplicateStepKey {
        /// The duplicate step key.
        step_key: String,
    },
    /// Dependencies were attached to a step that has not been added to the builder.
    UnknownStepKey {
        /// The missing target step key.
        step_key: String,
    },
    /// A dependency references a prerequisite step that does not exist in the workflow.
    MissingDependency {
        /// The step that owns the dependency.
        step_key: String,
        /// The missing prerequisite step key.
        prerequisite_step_key: String,
    },
    /// A step declares the same prerequisite more than once.
    DuplicateDependency {
        /// The step that owns the duplicate dependency.
        step_key: String,
        /// The duplicated prerequisite step key.
        prerequisite_step_key: String,
    },
    /// A step depends on itself.
    SelfDependency {
        /// The self-dependent step key.
        step_key: String,
    },
    /// The workflow dependency graph contains a cycle.
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
            WorkflowDagValidationError::BlankActiveKey => Self::BlankActiveKey,
            WorkflowDagValidationError::ActiveKeyTooLong => Self::ActiveKeyTooLong,
            WorkflowDagValidationError::BlankResultStepKey => Self::BlankResultStepKey,
            WorkflowDagValidationError::UnknownResultStepKey { step_key } => {
                Self::UnknownResultStepKey { step_key }
            }
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
            WorkflowDagValidationError::InvalidStepExecutionResourceKey { step_key } => {
                Self::InvalidStepExecutionResourceKey { step_key }
            }
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
            Self::BlankActiveKey => write!(f, "active_key must be non-empty"),
            Self::ActiveKeyTooLong => write!(f, "active_key must be at most 512 bytes"),
            Self::BlankResultStepKey => write!(f, "result_step_key must be non-empty"),
            Self::UnknownResultStepKey { step_key } => {
                write!(f, "result step '{step_key}' must exist in the workflow")
            }
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
            Self::InvalidStepExecutionResourceKey { step_key } => {
                write!(
                    f,
                    "execution_resource_key for step '{step_key}' must be non-blank and at most 512 bytes"
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
            Self::UnknownStepKey { step_key } => {
                write!(
                    f,
                    "step '{step_key}' must be added before dependencies can be attached"
                )
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
