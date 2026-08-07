use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::super::identifiers::{JobType, StepKey, WorkflowType};
use super::super::status::JobStage;
use super::build_validation::validate_step_enqueue;
use super::step_validation::{
    WorkflowStepShapeValidationInput, WorkflowStepValidationError, validate_step_dependency_shape,
    validate_step_shape,
};
use super::types::{WorkflowRunEnqueue, WorkflowStepExecutionKind};

/// Dependency input used by [`validate_workflow_dag`].
#[derive(Debug, Clone, Copy)]
pub struct WorkflowDagDependencyValidationInput<'a> {
    /// The prerequisite step that must release before the dependent step can run.
    pub prerequisite_step_key: StepKey<'a>,
}

/// Step input used by [`validate_workflow_dag`].
///
/// This DTO lets callers validate a workflow DAG without first constructing a
/// full [`WorkflowRunEnqueue`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WorkflowDagStepValidationInput<'a> {
    /// Unique key for this step within the workflow.
    pub step_key: StepKey<'a>,
    /// Whether this step is a queued job or an external gate.
    pub execution_kind: WorkflowStepExecutionKind,
    /// Job type for queued job steps.
    ///
    /// External steps must leave this as `None`.
    pub job_type: Option<JobType<'a>>,
    /// Optional queue priority override for queued job steps.
    pub priority: Option<i32>,
    /// Optional max-attempts override for queued job steps.
    pub max_attempts: Option<i32>,
    /// Optional timeout override, in seconds, for queued job steps.
    pub timeout_seconds: Option<i32>,
    /// Initial job stage for queued job steps.
    pub stage: Option<JobStage>,
    /// Whether a queued job step may continue as another handler-owned run.
    pub allow_handler_continuation: bool,
    /// Optional single-permit resource owned while a queued job is leased.
    pub execution_resource_key: Option<&'a str>,
    /// Dependencies declared by this step.
    pub dependencies: Vec<WorkflowDagDependencyValidationInput<'a>>,
}

impl<'a> WorkflowDagStepValidationInput<'a> {
    /// Creates a lightweight DAG step with optional queue settings disabled.
    #[must_use]
    pub fn new(
        step_key: StepKey<'a>,
        execution_kind: WorkflowStepExecutionKind,
        job_type: Option<JobType<'a>>,
        dependencies: Vec<WorkflowDagDependencyValidationInput<'a>>,
    ) -> Self {
        Self {
            step_key,
            execution_kind,
            job_type,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            stage: None,
            allow_handler_continuation: false,
            execution_resource_key: None,
            dependencies,
        }
    }

    /// Sets the optional queue-priority override used by validation.
    #[must_use]
    pub const fn priority(mut self, priority: Option<i32>) -> Self {
        self.priority = priority;
        self
    }

    /// Sets the optional positive max-attempts override used by validation.
    #[must_use]
    pub const fn max_attempts(mut self, max_attempts: Option<i32>) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Sets the optional positive timeout override, in seconds.
    #[must_use]
    pub const fn timeout_seconds(mut self, timeout_seconds: Option<i32>) -> Self {
        self.timeout_seconds = timeout_seconds;
        self
    }

    /// Sets the queued job's initial stage.
    #[must_use]
    pub const fn stage(mut self, stage: Option<JobStage>) -> Self {
        self.stage = stage;
        self
    }

    /// Sets whether the job-backed step may use successful handler continuation.
    ///
    /// External steps cannot enable this setting.
    #[must_use]
    pub const fn allow_handler_continuation(mut self, allow: bool) -> Self {
        self.allow_handler_continuation = allow;
        self
    }

    /// Sets the optional non-blank, at-most-512-byte execution-resource key.
    ///
    /// External steps cannot supply this setting.
    #[must_use]
    pub const fn execution_resource_key(mut self, resource_key: Option<&'a str>) -> Self {
        self.execution_resource_key = resource_key;
        self
    }
}

/// Error returned by workflow DAG validation helpers.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum WorkflowDagValidationError {
    /// The workflow did not include any steps.
    EmptySteps,
    /// The workflow type was blank.
    BlankWorkflowType,
    /// A step key was blank.
    BlankStepKey {
        /// Index of the step with the blank key.
        step_index: usize,
    },
    /// A job step had no usable job type.
    BlankStepJobType {
        /// The step whose job type was blank or missing.
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
    /// The workflow result step key does not match any step.
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
    /// A dependency references a prerequisite step that does not exist in the workflow.
    MissingDependency {
        /// The step that owns the dependency.
        step_key: String,
        /// The missing prerequisite step key.
        prerequisite_step_key: String,
    },
    /// A step depends on itself.
    SelfDependency {
        /// The self-dependent step key.
        step_key: String,
    },
    /// A step declares the same prerequisite more than once.
    DuplicateDependency {
        /// The step that owns the duplicate dependency.
        step_key: String,
        /// The duplicated prerequisite step key.
        prerequisite_step_key: String,
    },
    /// The workflow dependency graph contains a cycle.
    CycleDetected,
}

impl WorkflowDagValidationError {
    fn from_step_validation_error(error: WorkflowStepValidationError, step_index: usize) -> Self {
        match error {
            WorkflowStepValidationError::BlankStepKey => Self::BlankStepKey { step_index },
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

fn is_acyclic(mut indegree: Vec<usize>, adjacency: &[Vec<usize>]) -> bool {
    let mut ready: VecDeque<usize> = indegree
        .iter()
        .enumerate()
        .filter_map(|(index, &count)| (count == 0).then_some(index))
        .collect();
    let mut visited = 0usize;

    while let Some(index) = ready.pop_front() {
        visited += 1;

        for &next in &adjacency[index] {
            indegree[next] -= 1;
            if indegree[next] == 0 {
                ready.push_back(next);
            }
        }
    }

    visited == indegree.len()
}

/// Validates a workflow DAG from lightweight validation inputs.
///
/// This helper checks workflow shape only. It does not check whether job types
/// have registered storage definitions or runtime handlers.
///
/// # Errors
/// Returns [`WorkflowDagValidationError`] for blank identifiers, an empty step
/// list, invalid external-step queue fields, duplicate steps, missing
/// prerequisites, duplicate dependencies, self-dependencies, or cycles.
pub fn validate_workflow_dag(
    workflow_type: WorkflowType<'_>,
    steps: &[WorkflowDagStepValidationInput<'_>],
) -> Result<(), WorkflowDagValidationError> {
    if workflow_type.as_str().trim().is_empty() {
        return Err(WorkflowDagValidationError::BlankWorkflowType);
    }
    if steps.is_empty() {
        return Err(WorkflowDagValidationError::EmptySteps);
    }

    let mut step_key_to_index: BTreeMap<&str, usize> = BTreeMap::new();
    for (step_index, step) in steps.iter().enumerate() {
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
        })
        .map_err(|error| {
            WorkflowDagValidationError::from_step_validation_error(error, step_index)
        })?;
        if step_key_to_index
            .insert(step.step_key.as_str(), step_index)
            .is_some()
        {
            return Err(WorkflowDagValidationError::DuplicateStepKey {
                step_key: step.step_key.as_str().to_owned(),
            });
        }
    }

    let mut indegree: Vec<usize> = vec![0; steps.len()];
    let mut adjacency: Vec<Vec<usize>> = vec![Vec::new(); steps.len()];

    for (dependent_index, step) in steps.iter().enumerate() {
        let mut seen_dependencies: BTreeSet<&str> = BTreeSet::new();

        for dependency in &step.dependencies {
            validate_step_dependency_shape(
                step.step_key.as_str(),
                dependency.prerequisite_step_key.as_str(),
                &mut seen_dependencies,
            )
            .map_err(|error| {
                WorkflowDagValidationError::from_step_validation_error(error, dependent_index)
            })?;

            let Some(&prerequisite_index) =
                step_key_to_index.get(dependency.prerequisite_step_key.as_str())
            else {
                return Err(WorkflowDagValidationError::MissingDependency {
                    step_key: step.step_key.as_str().to_owned(),
                    prerequisite_step_key: dependency.prerequisite_step_key.as_str().to_owned(),
                });
            };

            indegree[dependent_index] += 1;
            adjacency[prerequisite_index].push(dependent_index);
        }
    }

    if !is_acyclic(indegree, &adjacency) {
        return Err(WorkflowDagValidationError::CycleDetected);
    }

    Ok(())
}

/// Validates a complete workflow enqueue payload.
///
/// This adapts [`WorkflowRunEnqueue`] into [`WorkflowDagStepValidationInput`]
/// and then applies [`validate_workflow_dag`]. It also validates the optional
/// workflow idempotency key.
///
/// # Errors
/// Returns [`WorkflowDagValidationError`] when the payload has a blank
/// idempotency key or fails DAG validation.
pub fn validate_workflow_run_enqueue(
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<(), WorkflowDagValidationError> {
    if payload
        .idempotency_key()
        .is_some_and(|idempotency_key| idempotency_key.trim().is_empty())
    {
        return Err(WorkflowDagValidationError::BlankIdempotencyKey);
    }
    if payload
        .active_key()
        .is_some_and(|active_key| active_key.trim().is_empty())
    {
        return Err(WorkflowDagValidationError::BlankActiveKey);
    }
    if payload
        .active_key()
        .is_some_and(|active_key| active_key.len() > 512)
    {
        return Err(WorkflowDagValidationError::ActiveKeyTooLong);
    }
    if payload
        .result_step_key()
        .is_some_and(|step_key| step_key.as_str().trim().is_empty())
    {
        return Err(WorkflowDagValidationError::BlankResultStepKey);
    }

    let steps = payload
        .steps()
        .iter()
        .map(|step| {
            let dependencies = step
                .dependencies()
                .iter()
                .map(|dependency| WorkflowDagDependencyValidationInput {
                    prerequisite_step_key: dependency.prerequisite_step_key,
                })
                .collect();
            WorkflowDagStepValidationInput::new(
                step.step_key(),
                step.execution_kind(),
                step.job_type(),
                dependencies,
            )
            .priority(step.priority())
            .max_attempts(step.max_attempts())
            .timeout_seconds(step.timeout_seconds())
            .stage(step.stage())
            .allow_handler_continuation(step.allows_handler_continuation())
            .execution_resource_key(step.execution_resource_key())
        })
        .collect::<Vec<_>>();

    validate_workflow_dag(payload.workflow_type(), &steps)?;

    if let Some(result_step_key) = payload.result_step_key()
        && !payload
            .steps()
            .iter()
            .any(|step| step.step_key() == result_step_key)
    {
        return Err(WorkflowDagValidationError::UnknownResultStepKey {
            step_key: result_step_key.as_str().to_owned(),
        });
    }

    Ok(())
}

/// Validates steps that are about to be appended to an existing workflow.
///
/// `existing_step_keys` should contain the step keys already persisted for the
/// workflow. `new_steps` are checked for valid step enqueue fields, duplicate
/// keys within the append batch, and collisions with existing keys.
///
/// # Errors
/// Returns [`WorkflowDagValidationError`] when the append batch is empty, any new
/// step is invalid, a new step key duplicates another new step, or a new step key
/// already exists in the workflow.
pub fn validate_workflow_step_append(
    existing_step_keys: &BTreeSet<super::super::identifiers::StepKeyName>,
    new_steps: &[super::types::WorkflowStepEnqueue<'_>],
) -> Result<(), WorkflowDagValidationError> {
    if new_steps.is_empty() {
        return Err(WorkflowDagValidationError::EmptySteps);
    }

    let mut new_step_key_to_index: BTreeMap<&str, usize> = BTreeMap::new();
    for (build_step_index, step) in new_steps.iter().enumerate() {
        validate_step_enqueue(step).map_err(|error| {
            WorkflowDagValidationError::from_step_validation_error(error, build_step_index)
        })?;

        let step_key = step.step_key().as_str();
        if existing_step_keys.contains(step_key) {
            return Err(WorkflowDagValidationError::DuplicateStepKey {
                step_key: step_key.to_owned(),
            });
        }

        if new_step_key_to_index
            .insert(step_key, build_step_index)
            .is_some()
        {
            return Err(WorkflowDagValidationError::DuplicateStepKey {
                step_key: step_key.to_owned(),
            });
        }
    }

    let mut indegree = vec![0usize; new_steps.len()];
    let mut adjacency: Vec<Vec<usize>> = vec![Vec::new(); new_steps.len()];

    for (dependent_index, step) in new_steps.iter().enumerate() {
        for dependency in step.dependencies() {
            let prerequisite_step_key = dependency.prerequisite_step_key.as_str();
            if let Some(&prerequisite_index) = new_step_key_to_index.get(prerequisite_step_key) {
                indegree[dependent_index] += 1;
                adjacency[prerequisite_index].push(dependent_index);
                continue;
            }

            if existing_step_keys.contains(prerequisite_step_key) {
                continue;
            }

            return Err(WorkflowDagValidationError::MissingDependency {
                step_key: step.step_key().as_str().to_owned(),
                prerequisite_step_key: prerequisite_step_key.to_owned(),
            });
        }
    }

    if !is_acyclic(indegree, &adjacency) {
        return Err(WorkflowDagValidationError::CycleDetected);
    }

    Ok(())
}
