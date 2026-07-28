use std::collections::{BTreeMap, BTreeSet};

use runledger_core::jobs::{
    JobStage, WorkflowDependencyReleaseMode, WorkflowRunEnqueue, WorkflowStepEnqueue,
    WorkflowStepExecutionKind,
};
use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

use super::errors::{workflow_definition_not_available_error, workflow_internal_state_error};
use super::validation::workflow_dependency_count_overflow_error;

pub(in crate::jobs::workflows) type DefaultsByJobType = BTreeMap<String, JobDefinitionDefaults>;
pub(in crate::jobs::workflows) type WorkflowStepIdsByKey = BTreeMap<String, Uuid>;

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
    match step.execution_kind() {
        WorkflowStepExecutionKind::Job => {
            Some(step.stage().unwrap_or(JobStage::Queued).as_db_value())
        }
        WorkflowStepExecutionKind::External => None,
    }
}

pub(in crate::jobs::workflows) fn workflow_step_defaults<'a>(
    defaults_by_job_type: &'a DefaultsByJobType,
    step: &WorkflowStepEnqueue<'_>,
) -> Result<&'a JobDefinitionDefaults> {
    let job_type = step
        .job_type()
        .ok_or_else(|| workflow_internal_state_error("job workflow step missing job_type"))?;

    defaults_by_job_type
        .get(job_type.as_str())
        .ok_or_else(|| workflow_definition_not_available_error(job_type.as_str()))
}

pub(in crate::jobs::workflows) async fn insert_workflow_step_record_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    organization_id: Option<Uuid>,
    step: &WorkflowStepEnqueue<'_>,
    defaults: Option<&JobDefinitionDefaults>,
    dependency_count_pending: i32,
    dependency_count_unsatisfied: i32,
) -> Result<Uuid> {
    let dependency_count_total = dependency_count_total(step)?;
    let (job_type, priority, max_attempts, timeout_seconds, stage) = match step.execution_kind() {
        WorkflowStepExecutionKind::Job => {
            let defaults = defaults.ok_or_else(|| {
                workflow_internal_state_error("missing job definition defaults for job step")
            })?;
            let job_type = step.job_type().ok_or_else(|| {
                workflow_internal_state_error("missing job_type for job workflow step")
            })?;

            (
                Some(job_type.as_str()),
                Some(step.priority().unwrap_or(defaults.default_priority)),
                Some(step.max_attempts().unwrap_or(defaults.max_attempts)),
                Some(
                    step.timeout_seconds()
                        .unwrap_or(defaults.default_timeout_seconds),
                ),
                workflow_step_effective_stage(step),
            )
        }
        WorkflowStepExecutionKind::External => (None, None, None, None, None),
    };
    let step_id: Uuid = sqlx::query_scalar!(
        "INSERT INTO workflow_steps (
            workflow_run_id,
            step_key,
            execution_kind,
            job_type,
            organization_id,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            stage,
            allow_handler_continuation,
            execution_resource_key,
            status,
            dependency_count_total,
            dependency_count_pending,
            dependency_count_unsatisfied
         )
         VALUES (
            $1,
            $2,
            $3::text::workflow_step_execution_kind,
            $4,
            $5,
            $6::jsonb,
            $7,
            $8,
            $9,
            $10,
            $11,
            $12,
            'BLOCKED',
            $13,
            $14,
            $15
         )
         RETURNING id",
        workflow_run_id,
        step.step_key() as _,
        step.execution_kind().as_db_value(),
        job_type,
        organization_id,
        step.payload(),
        priority,
        max_attempts,
        timeout_seconds,
        stage,
        step.allows_handler_continuation(),
        step.execution_resource_key(),
        dependency_count_total,
        dependency_count_pending,
        dependency_count_unsatisfied,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert workflow step", error))?;

    Ok(step_id)
}

pub(in crate::jobs::workflows) async fn insert_workflow_steps_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
    workflow_run_id: Uuid,
    defaults_by_job_type: &DefaultsByJobType,
) -> Result<WorkflowStepIdsByKey> {
    let mut step_id_by_key = WorkflowStepIdsByKey::new();
    for step in payload.steps() {
        let defaults = match step.execution_kind() {
            WorkflowStepExecutionKind::Job => {
                Some(workflow_step_defaults(defaults_by_job_type, step)?)
            }
            WorkflowStepExecutionKind::External => None,
        };
        let step_id = insert_workflow_step_record_tx(
            tx,
            workflow_run_id,
            workflow_step_effective_organization_id(payload.organization_id(), step),
            step,
            defaults,
            dependency_count_total(step)?,
            0,
        )
        .await?;
        step_id_by_key.insert(step.step_key().as_str().to_owned(), step_id);
    }

    Ok(step_id_by_key)
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

pub(in crate::jobs::workflows) async fn insert_workflow_step_dependency_record_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    prerequisite_step_id: Uuid,
    dependent_step_id: Uuid,
    release_mode: &str,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO workflow_step_dependencies (
            workflow_run_id,
            prerequisite_step_id,
            dependent_step_id,
            release_mode
         )
         VALUES ($1, $2, $3, $4::text::workflow_dependency_release_mode)",
        workflow_run_id,
        prerequisite_step_id,
        dependent_step_id,
        release_mode,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("insert workflow step dependency", error)
    })?;

    Ok(())
}

pub(in crate::jobs::workflows) async fn insert_workflow_step_dependencies_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
    workflow_run_id: Uuid,
    step_id_by_key: &WorkflowStepIdsByKey,
) -> Result<()> {
    for step in payload.steps() {
        let dependent_step_id = step_id_for_key(
            step_id_by_key,
            step.step_key().as_str(),
            "missing dependent workflow step id",
        )?;
        for dependency in step.dependencies() {
            let prerequisite_step_id = step_id_for_key(
                step_id_by_key,
                dependency.prerequisite_step_key.as_str(),
                "missing prerequisite workflow step id",
            )?;
            let release_mode = dependency
                .release_mode
                .unwrap_or(WorkflowDependencyReleaseMode::OnTerminal)
                .as_db_value();
            insert_workflow_step_dependency_record_tx(
                tx,
                workflow_run_id,
                prerequisite_step_id,
                dependent_step_id,
                release_mode,
            )
            .await?;
        }
    }

    Ok(())
}

pub(in crate::jobs::workflows) async fn fetch_job_definition_defaults_tx(
    tx: &mut DbTx<'_>,
    steps: &[WorkflowStepEnqueue<'_>],
) -> Result<DefaultsByJobType> {
    let job_types: Vec<String> = steps
        .iter()
        .filter(|step| step.execution_kind() == WorkflowStepExecutionKind::Job)
        .map(|step| {
            step.job_type()
                .map(|job_type| job_type.as_str().to_owned())
                .ok_or_else(|| workflow_internal_state_error("job workflow step missing job_type"))
        })
        .collect::<Result<BTreeSet<_>>>()?
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

    if let Some(step) = steps
        .iter()
        .filter(|step| step.execution_kind() == WorkflowStepExecutionKind::Job)
        .find(|step| {
            step.job_type()
                .is_none_or(|job_type| !defaults_by_job_type.contains_key(job_type.as_str()))
        })
    {
        return Err(workflow_definition_not_available_error(
            step.job_type()
                .map(|job_type| job_type.as_str())
                .unwrap_or("<missing-job-type>"),
        ));
    }

    Ok(defaults_by_job_type)
}
