pub use self::append::{append_workflow_steps, append_workflow_steps_tx};
pub use self::cancel::cancel_workflow_run_tx;

use runledger_core::jobs::StepKeyName;
use serde_json::Value as JsonValue;
use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

use super::locking::{lock_workflow_run_for_update_tx, lock_workflow_steps_for_update_tx};

mod append;
mod cancel;
mod idempotency;

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
