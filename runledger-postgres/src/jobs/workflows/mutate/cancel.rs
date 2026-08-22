use std::collections::BTreeSet;

use runledger_core::jobs::{WorkflowRunStatus, WorkflowStepStatus};
use sqlx::types::Uuid;

use crate::jobs::admin::cancel_job_with_scope_tx;
use crate::jobs::types::JobCancellationScope;
use crate::jobs::workflow_types::{CompleteExternalWorkflowStepInput, WorkflowRunDbRecord};
use crate::{DbTx, Error, Result};

use super::super::active_claims::release_or_defer_workflow_active_claim_tx;
use super::super::errors::workflow_internal_state_error;
use super::super::locking::{
    CancelWorkflowLockPhase, LockedWorkflowStepState, acquire_cancel_workflow_lock_phase_tx,
};
use super::super::read::load_workflow_run_by_id_tx;
use super::super::runtime::{
    complete_external_workflow_step_tx, notify_workflow_run_terminal_tx,
    recompute_workflow_run_statuses_tx, resolve_terminal_step_queue_tx,
};

#[derive(Clone, Copy)]
struct CancellationMetadata<'a> {
    reason: Option<&'a str>,
    last_error_code: Option<&'a str>,
    last_error_message: Option<&'a str>,
}

struct WorkflowCancellationSweep<'a> {
    workflow_run: WorkflowRunDbRecord,
    scope_organization_id: Option<Uuid>,
    metadata: CancellationMetadata<'a>,
    touched_run_ids: BTreeSet<Uuid>,
}

pub async fn cancel_workflow_run_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    organization_id: Option<Uuid>,
    reason: Option<&str>,
    last_error_code: Option<&str>,
    last_error_message: Option<&str>,
) -> Result<WorkflowRunDbRecord> {
    let metadata = CancellationMetadata {
        reason,
        last_error_code,
        last_error_message,
    };
    let lock_phase =
        acquire_cancel_workflow_lock_phase_tx(tx, workflow_run_id, organization_id).await?;

    cancel_workflow_run_after_lock_phase_tx(lock_phase, organization_id, metadata).await
}

async fn cancel_workflow_run_after_lock_phase_tx(
    lock_phase: CancelWorkflowLockPhase<'_, '_>,
    organization_id: Option<Uuid>,
    metadata: CancellationMetadata<'_>,
) -> Result<WorkflowRunDbRecord> {
    let (mut read_committed_tx, locked_steps, workflow_run) = lock_phase.into_parts();
    let tx = read_committed_tx.as_tx();

    if workflow_run.status.is_terminal() {
        return load_workflow_run_by_id_tx(
            tx,
            workflow_run.id,
            "load already-terminal workflow run for cancel",
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
             result = NULL,
             updated_at = now()
         WHERE id = $1",
        workflow_run.id,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("mark workflow run canceled", error))?;
    notify_workflow_run_terminal_tx(tx, workflow_run.id, WorkflowRunStatus::Canceled).await?;

    let mut sweep = WorkflowCancellationSweep::new(workflow_run, organization_id, metadata);
    sweep.sweep_tx(tx, locked_steps).await?;

    recompute_workflow_run_statuses_tx(tx, sweep.touched_run_ids()).await?;
    // Cancellation marks the run terminal before recompute, so recompute does
    // not observe a nonterminal-to-terminal transition for this run. Release
    // (or defer) its active claim explicitly after all jobs are canceled.
    release_or_defer_workflow_active_claim_tx(tx, sweep.workflow_run_id()).await?;
    load_workflow_run_by_id_tx(
        tx,
        sweep.workflow_run_id(),
        "load workflow run after cancel",
    )
    .await
}

impl<'a> WorkflowCancellationSweep<'a> {
    fn new(
        workflow_run: WorkflowRunDbRecord,
        scope_organization_id: Option<Uuid>,
        metadata: CancellationMetadata<'a>,
    ) -> Self {
        let workflow_run_id = workflow_run.id;

        Self {
            workflow_run,
            scope_organization_id,
            metadata,
            touched_run_ids: BTreeSet::from([workflow_run_id]),
        }
    }

    fn workflow_run_id(&self) -> Uuid {
        self.workflow_run.id
    }

    fn touched_run_ids(&self) -> &BTreeSet<Uuid> {
        &self.touched_run_ids
    }

    async fn sweep_tx(
        &mut self,
        tx: &mut DbTx<'_>,
        mut pending_steps: Vec<LockedWorkflowStepState>,
    ) -> Result<()> {
        let mut stalled_once = false;
        loop {
            let mut progressed = false;
            for step in pending_steps {
                progressed |= self.cancel_nonterminal_workflow_step_tx(tx, &step).await?;
            }

            pending_steps = self.load_nonterminal_workflow_steps_tx(tx).await?;
            if pending_steps.is_empty() {
                break;
            }
            if !progressed {
                if !stalled_once {
                    // The locked range should normally make new nonterminal rows
                    // impossible here. Allow one fresh READ COMMITTED reload per
                    // no-progress streak before treating it as state corruption, so
                    // a row that became visible between loop statements can still
                    // be swept.
                    stalled_once = true;
                    continue;
                }
                let pending_step_ids = pending_steps
                    .iter()
                    .map(|step| step.id.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(workflow_internal_state_error(format!(
                    "workflow cancel found nonterminal steps on run {} but made no progress; pending step ids: {}",
                    self.workflow_run.id, pending_step_ids
                )));
            }
            stalled_once = false;
        }

        Ok(())
    }

    async fn load_nonterminal_workflow_steps_tx(
        &self,
        tx: &mut DbTx<'_>,
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
            self.workflow_run.id,
            self.scope_organization_id,
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
        &mut self,
        tx: &mut DbTx<'_>,
        step: &LockedWorkflowStepState,
    ) -> Result<bool> {
        match step.status {
            WorkflowStepStatus::Enqueued | WorkflowStepStatus::Running => {
                let job_id = step.job_id.ok_or_else(|| {
                    workflow_internal_state_error(format!(
                        "workflow step '{}' is job-backed but missing job_id during workflow cancel",
                        step.step_key.as_str()
                    ))
                })?;
                let scope = match step.organization_id {
                    Some(organization_id) => JobCancellationScope::Organization(organization_id),
                    None => JobCancellationScope::Global,
                };
                let canceled =
                    cancel_job_with_scope_tx(tx, scope, job_id, self.metadata.reason).await?;
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
                        workflow_run_id: self.workflow_run.id,
                        organization_id: self.workflow_run.organization_id,
                        step_key: step.step_key.as_borrowed(),
                        terminal_status: WorkflowStepStatus::Canceled,
                        status_reason: self.metadata.reason,
                        last_error_code: self.metadata.last_error_code,
                        last_error_message: self.metadata.last_error_message,
                        output: None,
                    },
                )
                .await?;

                Ok(true)
            }
            WorkflowStepStatus::Blocked => self.cancel_blocked_workflow_step_tx(tx, step.id).await,
            WorkflowStepStatus::Succeeded
            | WorkflowStepStatus::Failed
            | WorkflowStepStatus::Canceled => Ok(false),
        }
    }

    async fn cancel_blocked_workflow_step_tx(
        &mut self,
        tx: &mut DbTx<'_>,
        step_id: Uuid,
    ) -> Result<bool> {
        let canceled = sqlx::query_scalar!(
            "UPDATE workflow_steps
                         SET status = 'CANCELED',
                             finished_at = COALESCE(finished_at, now()),
                             status_reason = $2,
                             last_error_code = $3,
                             last_error_message = $4,
                             output = NULL,
                             updated_at = now()
                         WHERE id = $1
                           AND status = 'BLOCKED'
                         RETURNING workflow_run_id",
            step_id,
            self.metadata.reason,
            self.metadata.last_error_code,
            self.metadata.last_error_message,
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
            resolve_terminal_step_queue_tx(
                tx,
                step_id,
                WorkflowStepStatus::Canceled,
                &mut self.touched_run_ids,
            )
            .await?;
            return Ok(true);
        }

        Ok(false)
    }
}
