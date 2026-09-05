use runledger_core::jobs::{WorkflowRunEnqueue, WorkflowStepEnqueue, WorkflowStepExecution};
use serde::Serialize;
use sqlx::types::{Json, Uuid};

use crate::{DbTx, Error, Result};

use super::{
    DefaultsByJobType, WorkflowStepDependencyWriteContext, WorkflowStepIdsByKey,
    dependency_count_total, step_id_for_key, workflow_step_defaults,
    workflow_step_effective_organization_id, workflow_step_effective_stage,
};

// Bounds serialization scratch space and work per statement independently of
// graph size. Payload bytes remain caller-controlled, just as for single inserts.
pub(in crate::jobs::workflows) const INSERT_CHUNK_ROWS: usize = 256;

#[derive(Serialize)]
pub(in crate::jobs::workflows) struct StepInsert<'a> {
    step_key: &'a str,
    execution_kind: &'static str,
    job_type: Option<&'a str>,
    organization_id: Option<Uuid>,
    payload: &'a serde_json::Value,
    priority: Option<i32>,
    max_attempts: Option<i32>,
    timeout_seconds: Option<i32>,
    stage: Option<&'static str>,
    allow_handler_continuation: bool,
    execution_resource_key: Option<&'a str>,
    dependency_count_total: i32,
    dependency_count_pending: i32,
    dependency_count_unsatisfied: i32,
}

impl<'a> StepInsert<'a> {
    pub(in crate::jobs::workflows) fn new(
        step: &'a WorkflowStepEnqueue<'_>,
        organization_id: Option<Uuid>,
        defaults: &DefaultsByJobType,
        pending: i32,
        unsatisfied: i32,
    ) -> Result<Self> {
        let (job_type, priority, max_attempts, timeout_seconds) = match step.execution() {
            WorkflowStepExecution::Job(execution) => {
                let defaults = workflow_step_defaults(defaults, execution)?;
                (
                    Some(execution.job_type().as_str()),
                    Some(execution.priority().unwrap_or(defaults.default_priority)),
                    Some(execution.max_attempts().unwrap_or(defaults.max_attempts)),
                    Some(
                        execution
                            .timeout_seconds()
                            .unwrap_or(defaults.default_timeout_seconds),
                    ),
                )
            }
            WorkflowStepExecution::External => (None, None, None, None),
        };
        Ok(Self {
            step_key: step.step_key().as_str(),
            execution_kind: step.execution_kind().as_db_value(),
            job_type,
            organization_id,
            payload: step.payload(),
            priority,
            max_attempts,
            timeout_seconds,
            stage: workflow_step_effective_stage(step),
            allow_handler_continuation: step.allows_handler_continuation(),
            execution_resource_key: step.execution_resource_key(),
            dependency_count_total: dependency_count_total(step)?,
            dependency_count_pending: pending,
            dependency_count_unsatisfied: unsatisfied,
        })
    }
}

pub(in crate::jobs::workflows) async fn insert_step_chunk_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    records: &[StepInsert<'_>],
) -> Result<WorkflowStepIdsByKey> {
    // recordset maps a JSON null field to SQL NULL. Payload is always present
    // in StepInsert, so restore the JSON null that single-row binding stored.
    let rows = sqlx::query!(
        "INSERT INTO workflow_steps (
            workflow_run_id, step_key, execution_kind, job_type, organization_id, payload,
            priority, max_attempts, timeout_seconds, stage, allow_handler_continuation,
            execution_resource_key, status, dependency_count_total,
            dependency_count_pending, dependency_count_unsatisfied
         ) SELECT $1, r.step_key, r.execution_kind::workflow_step_execution_kind,
            r.job_type, r.organization_id, COALESCE(r.payload, 'null'::jsonb), r.priority, r.max_attempts,
            r.timeout_seconds, r.stage, r.allow_handler_continuation, r.execution_resource_key,
            'BLOCKED', r.dependency_count_total, r.dependency_count_pending, r.dependency_count_unsatisfied
         FROM jsonb_to_recordset($2::jsonb) AS r(
            step_key text, execution_kind text, job_type text, organization_id uuid, payload jsonb,
            priority int, max_attempts int, timeout_seconds int, stage text,
            allow_handler_continuation bool, execution_resource_key text,
            dependency_count_total int, dependency_count_pending int, dependency_count_unsatisfied int)
         RETURNING id, step_key",
        workflow_run_id, Json(records) as _,
    ).fetch_all(&mut **tx).await
        .map_err(|error| Error::from_query_sqlx_with_context("insert workflow steps", error))?;
    Ok(rows.into_iter().map(|row| (row.step_key, row.id)).collect())
}

pub(in crate::jobs::workflows) async fn insert_workflow_steps_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
    workflow_run_id: Uuid,
    defaults: &DefaultsByJobType,
) -> Result<WorkflowStepIdsByKey> {
    let mut ids = WorkflowStepIdsByKey::new();
    for chunk in payload.steps().chunks(INSERT_CHUNK_ROWS) {
        let records = chunk
            .iter()
            .map(|step| {
                StepInsert::new(
                    step,
                    workflow_step_effective_organization_id(payload.organization_id(), step),
                    defaults,
                    dependency_count_total(step)?,
                    0,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        ids.extend(insert_step_chunk_tx(tx, workflow_run_id, &records).await?);
    }
    Ok(ids)
}

#[derive(Serialize)]
struct DependencyInsert {
    prerequisite_step_id: Uuid,
    dependent_step_id: Uuid,
    release_mode: &'static str,
}

async fn insert_dependency_chunk_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    records: &[DependencyInsert],
) -> Result<()> {
    if records.is_empty() {
        return Ok(());
    }
    sqlx::query!(
        "INSERT INTO workflow_step_dependencies (
            workflow_run_id, prerequisite_step_id, dependent_step_id, release_mode
         ) SELECT $1, r.prerequisite_step_id, r.dependent_step_id,
            r.release_mode::workflow_dependency_release_mode
         FROM jsonb_to_recordset($2::jsonb) AS r(
            prerequisite_step_id uuid, dependent_step_id uuid, release_mode text)",
        workflow_run_id,
        Json(records) as _,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("insert workflow step dependencies", error)
    })?;
    Ok(())
}

pub(in crate::jobs::workflows) async fn insert_workflow_step_dependencies_tx(
    tx: &mut DbTx<'_>,
    steps: &[WorkflowStepEnqueue<'_>],
    workflow_run_id: Uuid,
    step_id_by_key: &WorkflowStepIdsByKey,
    context: WorkflowStepDependencyWriteContext,
) -> Result<()> {
    let mut records = Vec::with_capacity(INSERT_CHUNK_ROWS);
    for step in steps {
        let dependent_step_id = step_id_for_key(
            step_id_by_key,
            step.step_key().as_str(),
            context.missing_dependent_step_id_error(),
        )?;
        for dependency in step.dependencies() {
            records.push(DependencyInsert {
                prerequisite_step_id: step_id_for_key(
                    step_id_by_key,
                    dependency.prerequisite_step_key.as_str(),
                    context.missing_prerequisite_step_id_error(),
                )?,
                dependent_step_id,
                release_mode: dependency.effective_release_mode().as_db_value(),
            });
            if records.len() == INSERT_CHUNK_ROWS {
                insert_dependency_chunk_tx(tx, workflow_run_id, &records).await?;
                records.clear();
            }
        }
    }
    insert_dependency_chunk_tx(tx, workflow_run_id, &records).await
}
