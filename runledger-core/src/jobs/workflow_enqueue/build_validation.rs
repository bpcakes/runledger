use std::collections::BTreeSet;

use super::super::identifiers::{JobType, StepKey};
use super::dag_validation::validate_workflow_run_enqueue;
use super::errors::WorkflowBuildError;
use super::types::{WorkflowRunEnqueue, WorkflowStepEnqueue, WorkflowStepExecutionKind};

pub(super) fn validate_step_enqueue(
    step: &WorkflowStepEnqueue<'_>,
    step_index: Option<usize>,
) -> Result<(), WorkflowBuildError> {
    if StepKey::try_new(step.step_key.as_str()).is_err() {
        return Err(WorkflowBuildError::BlankStepKey { step_index });
    }
    match step.execution_kind {
        WorkflowStepExecutionKind::Job => {
            let Some(job_type) = step.job_type else {
                return Err(WorkflowBuildError::BlankStepJobType {
                    step_key: step.step_key.as_str().to_owned(),
                });
            };
            if JobType::try_new(job_type.as_str()).is_err() {
                return Err(WorkflowBuildError::BlankStepJobType {
                    step_key: step.step_key.as_str().to_owned(),
                });
            }
        }
        WorkflowStepExecutionKind::External => {
            if step.job_type.is_some() {
                return Err(WorkflowBuildError::ExternalStepJobTypeNotAllowed {
                    step_key: step.step_key.as_str().to_owned(),
                });
            }
            if step.priority.is_some()
                || step.max_attempts.is_some()
                || step.timeout_seconds.is_some()
                || step.stage.is_some()
            {
                return Err(WorkflowBuildError::ExternalStepQueueSettingsNotAllowed {
                    step_key: step.step_key.as_str().to_owned(),
                });
            }
        }
    }
    let mut seen_dependencies: BTreeSet<&str> = BTreeSet::new();
    for dependency in &step.dependencies {
        if StepKey::try_new(dependency.prerequisite_step_key.as_str()).is_err() {
            return Err(WorkflowBuildError::BlankDependencyStepKey {
                step_key: step.step_key.as_str().to_owned(),
            });
        }
        if dependency.prerequisite_step_key == step.step_key {
            return Err(WorkflowBuildError::SelfDependency {
                step_key: step.step_key.as_str().to_owned(),
            });
        }
        if !seen_dependencies.insert(dependency.prerequisite_step_key.as_str()) {
            return Err(WorkflowBuildError::DuplicateDependency {
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
