mod append;
mod cancel;
mod idempotency;

pub use self::append::{append_workflow_steps, append_workflow_steps_tx};
pub use self::cancel::cancel_workflow_run_tx;

use runledger_core::jobs::{StepKeyName, WorkflowStepExecutionKind, WorkflowStepStatus};
use serde_json::Value as JsonValue;
use sqlx::types::Uuid;

use crate::jobs::row_decode::{
    parse_step_key_name, parse_workflow_run_status, parse_workflow_step_execution_kind,
    parse_workflow_step_status,
};
use crate::jobs::workflow_types::WorkflowRunDbRecord;
use crate::{DbTx, Error, Result};

use super::enqueue::load_workflow_run_by_id_tx;
use super::{workflow_internal_state_error, workflow_run_not_found_error};

#[derive(Debug, Clone)]
struct LockedWorkflowStepState {
    id: Uuid,
    step_key: StepKeyName,
    execution_kind: WorkflowStepExecutionKind,
    organization_id: Option<Uuid>,
    status: WorkflowStepStatus,
    job_id: Option<Uuid>,
}

impl LockedWorkflowStepState {
    fn decode(
        id: Uuid,
        step_key: String,
        execution_kind: String,
        organization_id: Option<Uuid>,
        status: String,
        job_id: Option<Uuid>,
    ) -> Result<Self> {
        Ok(Self {
            id,
            step_key: parse_step_key_name(step_key)?,
            execution_kind: parse_workflow_step_execution_kind(execution_kind)?,
            organization_id,
            status: parse_workflow_step_status(status)?,
            job_id,
        })
    }
}

pub async fn list_workflow_step_keys_for_update_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    organization_id: Option<Uuid>,
) -> Result<Vec<StepKeyName>> {
    let locked_steps =
        lock_workflow_steps_for_update_tx(tx, workflow_run_id, organization_id).await?;
    let _workflow_run =
        lock_workflow_run_for_update_tx(tx, workflow_run_id, organization_id).await?;
    Ok(locked_steps.into_iter().map(|step| step.step_key).collect())
}

pub async fn update_workflow_step_and_pending_job_payload_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    organization_id: Option<Uuid>,
    workflow_step_id: Uuid,
    job_id: Uuid,
    payload: &JsonValue,
) -> Result<bool> {
    let updated = sqlx::query_scalar::<_, bool>(
        "WITH locked_job AS (
             SELECT jq.id
             FROM job_queue jq
             JOIN workflow_steps ws ON ws.job_id = jq.id
             JOIN workflow_runs wr ON wr.id = ws.workflow_run_id
             WHERE jq.id = $4
               AND jq.status = 'PENDING'
               AND ws.id = $3
               AND ws.workflow_run_id = $1
               AND ($2::uuid IS NULL OR wr.organization_id = $2)
             FOR UPDATE OF jq
         ),
         updated_step AS (
             UPDATE workflow_steps ws
             SET payload = $5::jsonb,
                 updated_at = now()
             FROM workflow_runs wr
             WHERE ws.id = $3
               AND ws.workflow_run_id = $1
               AND ws.job_id = $4
               AND wr.id = ws.workflow_run_id
               AND ($2::uuid IS NULL OR wr.organization_id = $2)
               AND EXISTS (SELECT 1 FROM locked_job)
             RETURNING ws.id
         ),
         updated_job AS (
             UPDATE job_queue
             SET payload = $5::jsonb,
                 updated_at = now()
             WHERE id = $4
               AND EXISTS (SELECT 1 FROM updated_step)
             RETURNING id
         )
         SELECT EXISTS(SELECT 1 FROM updated_job)",
    )
    .bind(workflow_run_id)
    .bind(organization_id)
    .bind(workflow_step_id)
    .bind(job_id)
    .bind(payload)
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("update workflow step and pending job payload", error)
    })?;

    Ok(updated)
}

pub(super) async fn lock_workflow_step_jobs_for_update_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    organization_id: Option<Uuid>,
) -> Result<()> {
    sqlx::query!(
        // Keep the marker in the SQL text; the lock-order regression test
        // uses it to observe this exact wait in pg_stat_activity.
        "SELECT jq.id /* runledger:lock_workflow_step_jobs_for_update */
         FROM job_queue jq
         JOIN workflow_steps ws ON ws.job_id = jq.id
         JOIN workflow_runs wr ON wr.id = ws.workflow_run_id
         WHERE ws.workflow_run_id = $1
           AND ($2::uuid IS NULL OR wr.organization_id = $2)
         ORDER BY jq.id ASC
         FOR UPDATE OF jq",
        workflow_run_id,
        organization_id,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lock workflow step jobs for mutation", error)
    })?;

    Ok(())
}

async fn lock_workflow_steps_for_update_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    organization_id: Option<Uuid>,
) -> Result<Vec<LockedWorkflowStepState>> {
    let rows = sqlx::query!(
        "SELECT
            ws.id,
            ws.step_key,
            ws.execution_kind::text AS \"execution_kind!\",
            ws.organization_id,
            ws.status::text AS \"status!\",
            ws.job_id
         FROM workflow_steps ws
         JOIN workflow_runs wr ON wr.id = ws.workflow_run_id
         WHERE ws.workflow_run_id = $1
           AND ($2::uuid IS NULL OR wr.organization_id = $2)
         ORDER BY ws.id ASC
         FOR UPDATE OF ws",
        workflow_run_id,
        organization_id,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lock workflow steps for mutation", error)
    })?;

    rows.into_iter()
        .map(|row| {
            LockedWorkflowStepState::decode(
                row.id,
                row.step_key,
                row.execution_kind,
                row.organization_id,
                row.status,
                row.job_id,
            )
        })
        .collect()
}

async fn lock_workflow_run_for_update_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    organization_id: Option<Uuid>,
) -> Result<WorkflowRunDbRecord> {
    let row = sqlx::query!(
        "SELECT id, status::text AS \"status!\"
         FROM workflow_runs
         WHERE id = $1
           AND ($2::uuid IS NULL OR organization_id = $2)
         FOR UPDATE",
        workflow_run_id,
        organization_id,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("lock workflow run for mutation", error))?
    .ok_or_else(workflow_run_not_found_error)?;

    let workflow_run = load_workflow_run_by_id_tx(tx, row.id).await?;
    let status = parse_workflow_run_status(row.status)?;
    if workflow_run.status != status {
        return Err(workflow_internal_state_error(format!(
            "workflow run {} status changed during mutation lock reload",
            workflow_run.id
        )));
    }
    Ok(workflow_run)
}
