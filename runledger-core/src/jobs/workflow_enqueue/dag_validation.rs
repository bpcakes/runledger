use std::collections::{BTreeMap, BTreeSet, VecDeque};

use super::super::identifiers::{JobType, StepKey, WorkflowType};
use super::super::status::JobStage;
use super::types::{WorkflowRunEnqueue, WorkflowStepExecutionKind};

#[derive(Debug, Clone, Copy)]
pub struct WorkflowDagDependencyValidationInput<'a> {
    pub prerequisite_step_key: StepKey<'a>,
}

#[derive(Debug, Clone)]
pub struct WorkflowDagStepValidationInput<'a> {
    pub step_key: StepKey<'a>,
    pub execution_kind: WorkflowStepExecutionKind,
    pub job_type: Option<JobType<'a>>,
    pub priority: Option<i32>,
    pub max_attempts: Option<i32>,
    pub timeout_seconds: Option<i32>,
    pub stage: Option<JobStage>,
    pub dependencies: Vec<WorkflowDagDependencyValidationInput<'a>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkflowDagValidationError {
    EmptySteps,
    BlankWorkflowType,
    BlankStepKey {
        step_index: usize,
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
    SelfDependency {
        step_key: String,
    },
    DuplicateDependency {
        step_key: String,
        prerequisite_step_key: String,
    },
    CycleDetected,
}

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
        if step.step_key.as_str().trim().is_empty() {
            return Err(WorkflowDagValidationError::BlankStepKey { step_index });
        }
        match step.execution_kind {
            WorkflowStepExecutionKind::Job => {
                let Some(job_type) = step.job_type else {
                    return Err(WorkflowDagValidationError::BlankStepJobType {
                        step_key: step.step_key.as_str().to_owned(),
                    });
                };
                if job_type.as_str().trim().is_empty() {
                    return Err(WorkflowDagValidationError::BlankStepJobType {
                        step_key: step.step_key.as_str().to_owned(),
                    });
                }
                if let Some(max_attempts) = step.max_attempts
                    && max_attempts <= 0
                {
                    return Err(WorkflowDagValidationError::NonPositiveStepMaxAttempts {
                        step_key: step.step_key.as_str().to_owned(),
                        max_attempts,
                    });
                }
                if let Some(timeout_seconds) = step.timeout_seconds
                    && timeout_seconds <= 0
                {
                    return Err(WorkflowDagValidationError::NonPositiveStepTimeoutSeconds {
                        step_key: step.step_key.as_str().to_owned(),
                        timeout_seconds,
                    });
                }
            }
            WorkflowStepExecutionKind::External => {
                if step.job_type.is_some() {
                    return Err(WorkflowDagValidationError::ExternalStepJobTypeNotAllowed {
                        step_key: step.step_key.as_str().to_owned(),
                    });
                }
                if step.priority.is_some()
                    || step.max_attempts.is_some()
                    || step.timeout_seconds.is_some()
                    || step.stage.is_some()
                {
                    return Err(
                        WorkflowDagValidationError::ExternalStepQueueSettingsNotAllowed {
                            step_key: step.step_key.as_str().to_owned(),
                        },
                    );
                }
            }
        }
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
            if dependency.prerequisite_step_key.as_str().trim().is_empty() {
                return Err(WorkflowDagValidationError::BlankDependencyStepKey {
                    step_key: step.step_key.as_str().to_owned(),
                });
            }
            if dependency.prerequisite_step_key == step.step_key {
                return Err(WorkflowDagValidationError::SelfDependency {
                    step_key: step.step_key.as_str().to_owned(),
                });
            }

            if !seen_dependencies.insert(dependency.prerequisite_step_key.as_str()) {
                return Err(WorkflowDagValidationError::DuplicateDependency {
                    step_key: step.step_key.as_str().to_owned(),
                    prerequisite_step_key: dependency.prerequisite_step_key.as_str().to_owned(),
                });
            }

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

    if visited != steps.len() {
        return Err(WorkflowDagValidationError::CycleDetected);
    }

    Ok(())
}

pub fn validate_workflow_run_enqueue(
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<(), WorkflowDagValidationError> {
    if payload
        .idempotency_key()
        .is_some_and(|idempotency_key| idempotency_key.trim().is_empty())
    {
        return Err(WorkflowDagValidationError::BlankIdempotencyKey);
    }

    let steps = payload
        .steps()
        .iter()
        .map(|step| WorkflowDagStepValidationInput {
            step_key: step.step_key(),
            execution_kind: step.execution_kind(),
            job_type: step.job_type(),
            priority: step.priority(),
            max_attempts: step.max_attempts(),
            timeout_seconds: step.timeout_seconds(),
            stage: step.stage(),
            dependencies: step
                .dependencies()
                .iter()
                .map(|dependency| WorkflowDagDependencyValidationInput {
                    prerequisite_step_key: dependency.prerequisite_step_key,
                })
                .collect(),
        })
        .collect::<Vec<_>>();

    validate_workflow_dag(payload.workflow_type(), &steps)
}

pub fn validate_workflow_step_append(
    existing_step_keys: &BTreeSet<super::super::identifiers::StepKeyName>,
    new_steps: &[super::types::WorkflowStepEnqueue<'_>],
) -> Result<(), WorkflowDagValidationError> {
    if new_steps.is_empty() {
        return Err(WorkflowDagValidationError::EmptySteps);
    }

    let mut new_step_key_to_index: BTreeMap<&str, usize> = BTreeMap::new();
    for (build_step_index, step) in new_steps.iter().enumerate() {
        super::build_validation::validate_step_enqueue(step, Some(build_step_index)).map_err(
            |error| match error {
                super::errors::WorkflowBuildError::BlankWorkflowType => {
                    WorkflowDagValidationError::BlankWorkflowType
                }
                super::errors::WorkflowBuildError::EmptySteps => {
                    WorkflowDagValidationError::EmptySteps
                }
                super::errors::WorkflowBuildError::BlankStepKey { step_index } => {
                    WorkflowDagValidationError::BlankStepKey {
                        step_index: step_index.unwrap_or(build_step_index),
                    }
                }
                super::errors::WorkflowBuildError::BlankStepJobType { step_key } => {
                    WorkflowDagValidationError::BlankStepJobType { step_key }
                }
                super::errors::WorkflowBuildError::BlankIdempotencyKey => {
                    WorkflowDagValidationError::BlankIdempotencyKey
                }
                super::errors::WorkflowBuildError::NonPositiveStepMaxAttempts {
                    step_key,
                    max_attempts,
                } => WorkflowDagValidationError::NonPositiveStepMaxAttempts {
                    step_key,
                    max_attempts,
                },
                super::errors::WorkflowBuildError::NonPositiveStepTimeoutSeconds {
                    step_key,
                    timeout_seconds,
                } => WorkflowDagValidationError::NonPositiveStepTimeoutSeconds {
                    step_key,
                    timeout_seconds,
                },
                super::errors::WorkflowBuildError::ExternalStepJobTypeNotAllowed { step_key } => {
                    WorkflowDagValidationError::ExternalStepJobTypeNotAllowed { step_key }
                }
                super::errors::WorkflowBuildError::ExternalStepQueueSettingsNotAllowed {
                    step_key,
                } => WorkflowDagValidationError::ExternalStepQueueSettingsNotAllowed { step_key },
                super::errors::WorkflowBuildError::BlankDependencyStepKey { step_key } => {
                    WorkflowDagValidationError::BlankDependencyStepKey { step_key }
                }
                super::errors::WorkflowBuildError::DuplicateStepKey { step_key } => {
                    WorkflowDagValidationError::DuplicateStepKey { step_key }
                }
                super::errors::WorkflowBuildError::MissingDependency {
                    step_key,
                    prerequisite_step_key,
                } => WorkflowDagValidationError::MissingDependency {
                    step_key,
                    prerequisite_step_key,
                },
                super::errors::WorkflowBuildError::DuplicateDependency {
                    step_key,
                    prerequisite_step_key,
                } => WorkflowDagValidationError::DuplicateDependency {
                    step_key,
                    prerequisite_step_key,
                },
                super::errors::WorkflowBuildError::SelfDependency { step_key } => {
                    WorkflowDagValidationError::SelfDependency { step_key }
                }
                super::errors::WorkflowBuildError::CycleDetected => {
                    WorkflowDagValidationError::CycleDetected
                }
            },
        )?;

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

    if visited != new_steps.len() {
        return Err(WorkflowDagValidationError::CycleDetected);
    }

    Ok(())
}
