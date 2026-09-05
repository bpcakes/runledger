mod batch;
pub(in crate::jobs::workflows) use batch::{
    INSERT_CHUNK_ROWS, StepInsert, insert_step_chunk_tx, insert_workflow_step_dependencies_tx,
    insert_workflow_steps_tx,
};

use std::collections::{BTreeMap, BTreeSet};

use runledger_core::jobs::{
    JobStage, WorkflowJobStepExecution, WorkflowStepEnqueue, WorkflowStepExecution,
};
use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

use super::errors::{workflow_definition_not_available_error, workflow_internal_state_error};
use super::validation::workflow_dependency_count_overflow_error;

pub(in crate::jobs::workflows) type DefaultsByJobType = BTreeMap<String, JobDefinitionDefaults>;
pub(in crate::jobs::workflows) type WorkflowStepIdsByKey = BTreeMap<String, Uuid>;

#[derive(Clone, Copy, Debug)]
pub(in crate::jobs::workflows) enum WorkflowStepDependencyWriteContext {
    InitialEnqueue,
    Append,
}

impl WorkflowStepDependencyWriteContext {
    const fn missing_dependent_step_id_error(self) -> &'static str {
        match self {
            Self::InitialEnqueue => "missing dependent workflow step id",
            Self::Append => "missing dependent appended workflow step id",
        }
    }

    const fn missing_prerequisite_step_id_error(self) -> &'static str {
        match self {
            Self::InitialEnqueue => "missing prerequisite workflow step id",
            Self::Append => "missing appended workflow prerequisite step id",
        }
    }
}

#[derive(Clone, Debug)]
pub(in crate::jobs::workflows) struct JobDefinitionDefaults {
    default_priority: i32,
    max_attempts: i32,
    default_timeout_seconds: i32,
}

pub(in crate::jobs::workflows) fn dependency_count_total(
    step: &WorkflowStepEnqueue<'_>,
) -> Result<i32> {
    let dependency_count = step.dependencies().len();
    i32::try_from(dependency_count).map_err(|error| {
        workflow_dependency_count_overflow_error(step.step_key().as_str(), dependency_count, error)
    })
}

pub(in crate::jobs::workflows) fn workflow_step_effective_organization_id(
    workflow_organization_id: Option<Uuid>,
    step: &WorkflowStepEnqueue<'_>,
) -> Option<Uuid> {
    step.organization_id().or(workflow_organization_id)
}

pub(in crate::jobs::workflows) fn workflow_step_effective_stage(
    step: &WorkflowStepEnqueue<'_>,
) -> Option<&'static str> {
    match step.execution() {
        WorkflowStepExecution::Job(execution) => {
            Some(execution.stage().unwrap_or(JobStage::Queued).as_db_value())
        }
        WorkflowStepExecution::External => None,
    }
}

pub(in crate::jobs::workflows) fn workflow_step_defaults<'a>(
    defaults_by_job_type: &'a DefaultsByJobType,
    execution: WorkflowJobStepExecution<'_>,
) -> Result<&'a JobDefinitionDefaults> {
    let job_type = execution.job_type();

    defaults_by_job_type
        .get(job_type.as_str())
        .ok_or_else(|| workflow_definition_not_available_error(job_type.as_str()))
}

pub(in crate::jobs::workflows) fn step_id_for_key(
    step_id_by_key: &WorkflowStepIdsByKey,
    step_key: &str,
    missing_error: &'static str,
) -> Result<Uuid> {
    step_id_by_key
        .get(step_key)
        .copied()
        .ok_or_else(|| workflow_internal_state_error(missing_error))
}

pub(in crate::jobs::workflows) async fn fetch_job_definition_defaults_tx(
    tx: &mut DbTx<'_>,
    steps: &[WorkflowStepEnqueue<'_>],
) -> Result<DefaultsByJobType> {
    let job_types: Vec<String> = steps
        .iter()
        .filter_map(|step| match step.execution() {
            WorkflowStepExecution::Job(execution) => Some(execution.job_type().as_str().to_owned()),
            WorkflowStepExecution::External => None,
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let rows = sqlx::query!(
        "SELECT job_type, default_priority, max_attempts, default_timeout_seconds
         FROM job_definitions jd
         WHERE is_enabled = true
           AND job_type = ANY($1::text[])
         FOR SHARE OF jd",
        &job_types,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lookup workflow step job definition defaults", error)
    })?;

    let defaults_by_job_type: DefaultsByJobType = rows
        .into_iter()
        .map(|row| {
            (
                row.job_type,
                JobDefinitionDefaults {
                    default_priority: row.default_priority,
                    max_attempts: row.max_attempts,
                    default_timeout_seconds: row.default_timeout_seconds,
                },
            )
        })
        .collect();

    if let Some(job_type) = steps.iter().find_map(|step| match step.execution() {
        WorkflowStepExecution::Job(execution)
            if !defaults_by_job_type.contains_key(execution.job_type().as_str()) =>
        {
            Some(execution.job_type().as_str())
        }
        WorkflowStepExecution::Job(_) | WorkflowStepExecution::External => None,
    }) {
        return Err(workflow_definition_not_available_error(job_type));
    }

    Ok(defaults_by_job_type)
}
