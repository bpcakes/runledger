use std::collections::{BTreeMap, BTreeSet};

use runledger_core::jobs::WorkflowDependencyReleaseMode;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, Result};

use super::super::row_decode::{
    parse_job_stage, parse_job_type_name, parse_workflow_run_status,
    parse_workflow_step_execution_kind, parse_workflow_type_name,
};
use super::super::workflow_types::WorkflowRunDbRecord;
use super::runtime::{recompute_workflow_run_statuses_tx, release_candidate_step_tx};
use super::{
    JobDefinitionDefaults, workflow_dag_validation_error, workflow_definition_not_available_error,
    workflow_dependency_count_overflow_error, workflow_internal_state_error,
};
use runledger_core::jobs::{
    WorkflowRunEnqueue, WorkflowStepEnqueue, WorkflowStepExecutionKind,
    validate_workflow_run_enqueue,
};

type DefaultsByJobType = BTreeMap<String, JobDefinitionDefaults>;
pub(crate) type WorkflowStepIdsByKey = BTreeMap<String, Uuid>;

pub async fn enqueue_workflow_run(
    pool: &DbPool,
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<WorkflowRunDbRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    let workflow_run = enqueue_workflow_run_tx(&mut tx, payload).await?;
    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    Ok(workflow_run)
}

pub async fn enqueue_workflow_run_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<WorkflowRunDbRecord> {
    validate_workflow_run_enqueue(payload).map_err(workflow_dag_validation_error)?;

    let defaults_by_job_type = fetch_job_definition_defaults_tx(tx, payload.steps()).await?;
    let workflow_run = insert_workflow_run_record_tx(tx, payload).await?;
    let step_id_by_key =
        insert_workflow_steps_tx(tx, payload, workflow_run.id, &defaults_by_job_type).await?;
    insert_workflow_step_dependencies_tx(tx, payload, workflow_run.id, &step_id_by_key).await?;

    enqueue_root_steps_tx(tx, workflow_run.id).await?;
    recompute_workflow_run_statuses_tx(tx, &std::collections::BTreeSet::from([workflow_run.id]))
        .await?;

    load_workflow_run_by_id_tx(tx, workflow_run.id).await
}

async fn insert_workflow_run_record_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<WorkflowRunDbRecord> {
    let run_row = sqlx::query!(
        "INSERT INTO workflow_runs (
            workflow_type,
            organization_id,
            status,
            idempotency_key,
            metadata,
            started_at
         )
         VALUES ($1, $2, 'RUNNING', $3, $4::jsonb, now())
         RETURNING
            id,
            workflow_type,
            organization_id,
            status::text AS \"status!\",
            idempotency_key,
            metadata,
            started_at,
            finished_at,
            created_at,
            updated_at",
        payload.workflow_type() as _,
        payload.organization_id(),
        payload.idempotency_key(),
        payload.metadata(),
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("enqueue workflow run", error))?;

    Ok(WorkflowRunDbRecord {
        id: run_row.id,
        workflow_type: parse_workflow_type_name(run_row.workflow_type)?,
        organization_id: run_row.organization_id,
        status: parse_workflow_run_status(run_row.status)?,
        idempotency_key: run_row.idempotency_key,
        metadata: run_row.metadata,
        started_at: run_row.started_at,
        finished_at: run_row.finished_at,
        created_at: run_row.created_at,
        updated_at: run_row.updated_at,
    })
}

pub(crate) fn dependency_count_total(step: &WorkflowStepEnqueue<'_>) -> Result<i32> {
    i32::try_from(step.dependencies().len())
        .map_err(|_| workflow_dependency_count_overflow_error(step.step_key().as_str()))
}

pub(crate) fn workflow_step_effective_organization_id(
    workflow_organization_id: Option<Uuid>,
    step: &WorkflowStepEnqueue<'_>,
) -> Option<Uuid> {
    step.organization_id().or(workflow_organization_id)
}

pub(crate) fn workflow_step_defaults<'a>(
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

pub(crate) async fn insert_workflow_step_record_tx(
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
                Some(
                    step.stage()
                        .unwrap_or(runledger_core::jobs::JobStage::Queued)
                        .as_db_value(),
                ),
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
            'BLOCKED',
            $11,
            $12,
            $13
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
        dependency_count_total,
        dependency_count_pending,
        dependency_count_unsatisfied,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert workflow step", error))?;

    Ok(step_id)
}

pub(crate) async fn insert_workflow_steps_tx(
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

pub(crate) fn step_id_for_key(
    step_id_by_key: &WorkflowStepIdsByKey,
    step_key: &str,
    missing_error: &'static str,
) -> Result<Uuid> {
    step_id_by_key
        .get(step_key)
        .copied()
        .ok_or_else(|| workflow_internal_state_error(missing_error))
}

pub(crate) async fn insert_workflow_step_dependency_record_tx(
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

pub(crate) async fn insert_workflow_step_dependencies_tx(
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

pub(crate) async fn fetch_job_definition_defaults_tx(
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
         FROM job_definitions
         WHERE is_enabled = true
           AND job_type = ANY($1::text[])",
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

pub(crate) async fn enqueue_root_steps_tx(tx: &mut DbTx<'_>, workflow_run_id: Uuid) -> Result<()> {
    let rows = sqlx::query!(
        "SELECT
            id,
            workflow_run_id,
            execution_kind::text AS \"execution_kind!\",
            job_type,
            organization_id,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            stage
         FROM workflow_steps
         WHERE workflow_run_id = $1
           AND status = 'BLOCKED'
           AND dependency_count_pending = 0
         ORDER BY created_at ASC
         FOR UPDATE",
        workflow_run_id,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lookup root workflow steps for enqueue", error)
    })?;

    for row in rows {
        let candidate = super::StepReleaseCandidate {
            id: row.id,
            workflow_run_id: row.workflow_run_id,
            execution_kind: parse_workflow_step_execution_kind(row.execution_kind)?,
            job_type: row.job_type.map(parse_job_type_name).transpose()?,
            organization_id: row.organization_id,
            payload: row.payload,
            priority: row.priority,
            max_attempts: row.max_attempts,
            timeout_seconds: row.timeout_seconds,
            stage: row.stage.map(parse_job_stage).transpose()?,
        };
        release_candidate_step_tx(tx, &candidate).await?;
    }

    Ok(())
}

pub(crate) async fn load_workflow_run_by_id_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
) -> Result<WorkflowRunDbRecord> {
    let run_row = sqlx::query!(
        "SELECT
            id,
            workflow_type,
            organization_id,
            status::text AS \"status!\",
            idempotency_key,
            metadata,
            started_at,
            finished_at,
            created_at,
            updated_at
         FROM workflow_runs
         WHERE id = $1",
        workflow_run_id,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("load workflow run after enqueue recompute", error)
    })?;

    Ok(WorkflowRunDbRecord {
        id: run_row.id,
        workflow_type: parse_workflow_type_name(run_row.workflow_type)?,
        organization_id: run_row.organization_id,
        status: parse_workflow_run_status(run_row.status)?,
        idempotency_key: run_row.idempotency_key,
        metadata: run_row.metadata,
        started_at: run_row.started_at,
        finished_at: run_row.finished_at,
        created_at: run_row.created_at,
        updated_at: run_row.updated_at,
    })
}
