use std::collections::BTreeSet;

use runledger_core::jobs::{WorkflowRunStatus, WorkflowStepStatus};
use sqlx::types::Uuid;

use crate::jobs::admin::cancel_job_tx;
use crate::jobs::transaction_isolation::ensure_read_committed_tx;
use crate::jobs::workflow_types::{CompleteExternalWorkflowStepInput, WorkflowRunDbRecord};
use crate::{DbTx, Error, Result};

use super::super::errors::workflow_internal_state_error;
use super::super::locking::{
    LockedWorkflowStepState, lock_workflow_run_for_update_tx,
    lock_workflow_run_release_exclusive_after_jobs_tx, lock_workflow_step_jobs_for_update_tx,
    lock_workflow_steps_for_update_tx,
};
use super::super::read::load_workflow_run_by_id_tx;
use super::super::runtime::{
    complete_external_workflow_step_tx, recompute_workflow_run_statuses_tx,
    resolve_terminal_step_queue_tx,
};

pub async fn cancel_workflow_run_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    organization_id: Option<Uuid>,
    reason: Option<&str>,
    last_error_code: Option<&str>,
    last_error_message: Option<&str>,
) -> Result<WorkflowRunDbRecord> {
    ensure_read_committed_tx(
        tx,
        "workflow cancel",
        "workflow.cancel_unsupported_isolation",
        "Workflow cancellation requires READ COMMITTED transaction isolation.",
    )
    .await?;

    // Lock job rows before workflow-step rows so cancel does not wait on a
    // step while an in-flight release has already locked that step and is
    // inserting or updating its job row. After the advisory wait, that release
    // has committed or rolled back, so the second job lock catches rows that
    // became visible while cancel was waiting. Appended steps that skip release
    // because cancel owns the advisory lock remain BLOCKED and are swept by the
    // nonterminal-step reload below. This relies on PostgreSQL's default READ
    // COMMITTED isolation so the second job-lock query observes rows committed
    // while this transaction waited on the advisory lock.
    lock_workflow_step_jobs_for_update_tx(tx, workflow_run_id, organization_id).await?;
    lock_workflow_run_release_exclusive_after_jobs_tx(tx, workflow_run_id).await?;
    // Catch job rows inserted by releases that committed while cancel was
    // waiting on the advisory lock.
    lock_workflow_step_jobs_for_update_tx(tx, workflow_run_id, organization_id).await?;
    let locked_steps =
        lock_workflow_steps_for_update_tx(tx, workflow_run_id, organization_id).await?;
    let workflow_run =
        lock_workflow_run_for_update_tx(tx, workflow_run_id, organization_id).await?;

    if workflow_run.status == WorkflowRunStatus::Canceled {
        return load_workflow_run_by_id_tx(
            tx,
            workflow_run.id,
            "load already-canceled workflow run",
        )
        .await;
    }

    // Keep this status update before cancel_nonterminal_workflow_step_tx: that
    // path can reenter release logic for external steps, and the canceled run
    // status is what makes the reentrant release no-op.
    sqlx::query!(
        "UPDATE workflow_runs
         SET status = 'CANCELED',
             finished_at = COALESCE(finished_at, now()),
             updated_at = now()
         WHERE id = $1",
        workflow_run.id,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("mark workflow run canceled", error))?;

    let mut touched_run_ids = BTreeSet::from([workflow_run.id]);
    let mut pending_steps = locked_steps;
    loop {
        let mut progressed = false;
        for step in pending_steps {
            progressed |= cancel_nonterminal_workflow_step_tx(
                tx,
                &workflow_run,
                &step,
                reason,
                last_error_code,
                last_error_message,
                &mut touched_run_ids,
            )
            .await?;
        }

        pending_steps =
            load_nonterminal_workflow_steps_for_cancel_tx(tx, workflow_run.id, organization_id)
                .await?;
        if pending_steps.is_empty() {
            break;
        }
        if !progressed {
            return Err(workflow_internal_state_error(format!(
                "workflow cancel found nonterminal steps on run {} but made no progress",
                workflow_run.id
            )));
        }
    }

    recompute_workflow_run_statuses_tx(tx, &touched_run_ids).await?;
    load_workflow_run_by_id_tx(tx, workflow_run.id, "load workflow run after cancel").await
}

async fn load_nonterminal_workflow_steps_for_cancel_tx(
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
           AND ws.status IN (
               'BLOCKED'::workflow_step_status,
               'ENQUEUED'::workflow_step_status,
               'RUNNING'::workflow_step_status,
               'WAITING_FOR_EXTERNAL'::workflow_step_status
           )
         ORDER BY ws.id ASC
         FOR UPDATE OF ws",
        workflow_run_id,
        organization_id,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context(
            "load remaining nonterminal workflow steps for cancel",
            error,
        )
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

async fn cancel_nonterminal_workflow_step_tx(
    tx: &mut DbTx<'_>,
    workflow_run: &WorkflowRunDbRecord,
    step: &LockedWorkflowStepState,
    reason: Option<&str>,
    last_error_code: Option<&str>,
    last_error_message: Option<&str>,
    touched_run_ids: &mut BTreeSet<Uuid>,
) -> Result<bool> {
    match step.status {
        WorkflowStepStatus::Enqueued | WorkflowStepStatus::Running => {
            let job_id = step.job_id.ok_or_else(|| {
                workflow_internal_state_error(format!(
                    "workflow step '{}' is job-backed but missing job_id during workflow cancel",
                    step.step_key.as_str()
                ))
            })?;
            let canceled = cancel_job_tx(tx, step.organization_id, job_id, reason).await?;
            if canceled.is_none() {
                return Err(workflow_internal_state_error(format!(
                    "workflow-managed job {job_id} could not be canceled during workflow run cancel"
                )));
            }

            Ok(true)
        }
        WorkflowStepStatus::WaitingForExternal => {
            complete_external_workflow_step_tx(
                tx,
                &CompleteExternalWorkflowStepInput {
                    workflow_run_id: workflow_run.id,
                    organization_id: workflow_run.organization_id,
                    step_key: step.step_key.as_borrowed(),
                    terminal_status: WorkflowStepStatus::Canceled,
                    status_reason: reason,
                    last_error_code,
                    last_error_message,
                },
            )
            .await?;

            Ok(true)
        }
        WorkflowStepStatus::Blocked => {
            cancel_blocked_workflow_step_tx(
                tx,
                step.id,
                reason,
                last_error_code,
                last_error_message,
                touched_run_ids,
            )
            .await
        }
        WorkflowStepStatus::Succeeded
        | WorkflowStepStatus::Failed
        | WorkflowStepStatus::Canceled => Ok(false),
    }
}

async fn cancel_blocked_workflow_step_tx(
    tx: &mut DbTx<'_>,
    step_id: Uuid,
    reason: Option<&str>,
    last_error_code: Option<&str>,
    last_error_message: Option<&str>,
    touched_run_ids: &mut BTreeSet<Uuid>,
) -> Result<bool> {
    let canceled = sqlx::query!(
        "UPDATE workflow_steps
                         SET status = 'CANCELED',
                             finished_at = COALESCE(finished_at, now()),
                             status_reason = $2,
                             last_error_code = $3,
                             last_error_message = $4,
                             updated_at = now()
                         WHERE id = $1
                           AND status = 'BLOCKED'
                         RETURNING workflow_run_id",
        step_id,
        reason,
        last_error_code,
        last_error_message,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context(
            "cancel blocked workflow step during workflow cancel",
            error,
        )
    })?;

    if canceled.is_some() {
        resolve_terminal_step_queue_tx(tx, step_id, WorkflowStepStatus::Canceled, touched_run_ids)
            .await?;
        return Ok(true);
    }

    Ok(false)
}
