use runledger_core::jobs::WorkflowStepStatus;
use sqlx::types::Uuid;

use crate::{DbTx, Result};

use super::runtime::{
    mark_workflow_step_enqueued_for_claim_release_tx, mark_workflow_step_enqueued_for_retry_tx,
    mark_workflow_step_running_for_claim_tx, process_workflow_step_terminal_by_job_id_tx,
};

pub(crate) async fn on_claimed(tx: &mut DbTx<'_>, job_id: Uuid) -> Result<()> {
    mark_workflow_step_running_for_claim_tx(tx, job_id).await
}

pub(crate) async fn on_claim_released(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    reset_started_at: bool,
) -> Result<()> {
    mark_workflow_step_enqueued_for_claim_release_tx(tx, job_id, reset_started_at).await
}

pub(crate) async fn on_retry_scheduled(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    status_reason: Option<&str>,
    last_error_code: Option<&str>,
    last_error_message: Option<&str>,
) -> Result<()> {
    mark_workflow_step_enqueued_for_retry_tx(
        tx,
        job_id,
        status_reason,
        last_error_code,
        last_error_message,
    )
    .await
}

pub(crate) async fn on_terminal(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    terminal_status: WorkflowStepStatus,
    status_reason: Option<&str>,
    last_error_code: Option<&str>,
    last_error_message: Option<&str>,
) -> Result<()> {
    process_workflow_step_terminal_by_job_id_tx(
        tx,
        job_id,
        terminal_status,
        status_reason,
        last_error_code,
        last_error_message,
    )
    .await
}
