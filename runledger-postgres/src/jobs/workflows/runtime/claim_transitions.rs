use runledger_core::jobs::{WorkflowStepExecutionKind, WorkflowStepStatus};
use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

use super::super::super::errors::workflow_handler_continuation_not_enabled_error;
use super::super::super::row_decode::{
    parse_workflow_step_execution_kind, parse_workflow_step_status,
};
use super::super::super::types::HANDLER_CONTINUATION_REASON;
use super::super::errors::workflow_internal_state_error;

pub(crate) async fn mark_workflow_step_running_for_claim_tx(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
) -> Result<()> {
    sqlx::query!(
        "UPDATE workflow_steps
         SET status = 'RUNNING',
             started_at = COALESCE(started_at, now()),
             output = NULL,
             updated_at = now()
         WHERE job_id = $1
           AND status IN ('ENQUEUED', 'RUNNING')",
        job_id,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("mark workflow step running for claim", error)
    })?;

    Ok(())
}

pub(crate) async fn mark_workflow_step_enqueued_for_claim_release_tx(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    reset_started_at: bool,
) -> Result<()> {
    sqlx::query!(
        "UPDATE workflow_steps
         SET status = 'ENQUEUED',
             started_at = CASE
                WHEN $2 THEN NULL
                ELSE started_at
             END,
             finished_at = NULL,
             status_reason = NULL,
             last_error_code = NULL,
             last_error_message = NULL,
             output = NULL,
             updated_at = now()
         WHERE job_id = $1
           AND status = 'RUNNING'",
        job_id,
        reset_started_at,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("mark workflow step enqueued for claim release", error)
    })?;

    Ok(())
}

pub(crate) async fn mark_workflow_step_enqueued_for_retry_tx(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    status_reason: Option<&str>,
    last_error_code: Option<&str>,
    last_error_message: Option<&str>,
) -> Result<()> {
    // A retry keeps started_at as the first time this workflow step began
    // executing; the next claim should not erase that history. Non-RUNNING
    // rows are intentionally left alone: non-workflow jobs have no matching
    // step, and concurrent cancellation or terminal handling must win over
    // putting a step back into ENQUEUED.
    sqlx::query!(
        "UPDATE workflow_steps
         SET status = 'ENQUEUED',
             finished_at = NULL,
             status_reason = $2,
             last_error_code = $3,
             last_error_message = $4,
             output = NULL,
             updated_at = now()
         WHERE job_id = $1
           AND status = 'RUNNING'",
        job_id,
        status_reason,
        last_error_code,
        last_error_message,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("mark workflow step enqueued for retry", error)
    })?;

    Ok(())
}

#[derive(sqlx::FromRow)]
struct WorkflowHandlerContinuationRow {
    id: Uuid,
    job_id: Option<Uuid>,
    execution_kind: String,
    status: String,
    allow_handler_continuation: bool,
}

pub(crate) async fn mark_workflow_step_enqueued_for_handler_continuation_tx(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    workflow_step_id: Uuid,
) -> Result<()> {
    // Lifecycle continuation already owns job_queue(id), preserving the
    // repository-wide job-row-before-workflow-step lock order.
    let row = sqlx::query_as::<_, WorkflowHandlerContinuationRow>(
        "SELECT
            id,
            job_id,
            execution_kind::text AS execution_kind,
            status::text AS status,
            allow_handler_continuation
         FROM workflow_steps
         WHERE id = $1
         /* runledger:lock_workflow_step_for_handler_continuation */
         FOR UPDATE",
    )
    .bind(workflow_step_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lock workflow step for handler continuation", error)
    })?
    .ok_or_else(|| {
        workflow_internal_state_error(format!(
            "workflow-managed job {job_id} links to missing workflow step {workflow_step_id}"
        ))
    })?;

    let execution_kind = parse_workflow_step_execution_kind(row.execution_kind)?;
    let status = parse_workflow_step_status(row.status)?;
    if row.id != workflow_step_id
        || row.job_id != Some(job_id)
        || execution_kind != WorkflowStepExecutionKind::Job
        || status != WorkflowStepStatus::Running
    {
        return Err(workflow_internal_state_error(format!(
            "workflow handler continuation linkage/status mismatch: job_id={job_id}, workflow_step_id={workflow_step_id}, stored_job_id={:?}, execution_kind={}, status={}",
            row.job_id,
            execution_kind.as_db_value(),
            status.as_db_value()
        )));
    }
    if !row.allow_handler_continuation {
        return Err(workflow_handler_continuation_not_enabled_error());
    }

    let updated_step_id = sqlx::query_scalar!(
        "UPDATE workflow_steps
         SET status = 'ENQUEUED',
             finished_at = NULL,
             status_reason = $3,
             last_error_code = NULL,
             last_error_message = NULL,
             output = NULL,
             updated_at = now()
         WHERE id = $1
           AND job_id = $2
           AND execution_kind = 'JOB'
           AND status = 'RUNNING'
           AND allow_handler_continuation
         RETURNING id",
        workflow_step_id,
        job_id,
        HANDLER_CONTINUATION_REASON,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context(
            "mark workflow step enqueued for handler continuation",
            error,
        )
    })?;

    if updated_step_id != Some(workflow_step_id) {
        return Err(workflow_internal_state_error(format!(
            "workflow handler continuation expected exactly one workflow step update for job_id={job_id}, workflow_step_id={workflow_step_id}"
        )));
    }

    Ok(())
}
