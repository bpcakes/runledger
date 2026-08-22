use std::collections::VecDeque;

use runledger_core::jobs::{
    WorkflowDependencyReleaseMode, WorkflowStepExecutionKind, WorkflowStepStatus,
};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::jobs::transaction_isolation::{ReadCommittedTx, ensure_read_committed_tx};
use crate::{DbTx, Error, Result};

use super::super::super::row_decode::{
    parse_job_stage, parse_job_type_name, parse_workflow_release_mode,
    parse_workflow_step_execution_kind, parse_workflow_step_status,
};
use super::super::super::rows::WorkflowStepRow;
use super::super::super::workflow_types::{
    CompleteExternalWorkflowStepInput, WorkflowStepDbRecord,
};
use super::super::errors::{
    workflow_external_completion_conflict_error, workflow_external_completion_invalid_status_error,
    workflow_external_completion_metadata_conflict_error,
    workflow_external_completion_output_conflict_error,
    workflow_external_completion_output_invalid_error, workflow_external_step_not_external_error,
    workflow_external_step_not_found_error, workflow_external_step_not_waiting_error,
    workflow_internal_state_error, workflow_release_conflict_error,
};
use super::super::locking::{
    lock_workflow_run_release_shared_tx, lock_workflow_step_rows_for_update_tx,
    try_lock_workflow_run_release_shared_tx,
};
use super::super::release::{
    StepReleaseCandidate, StepReleaseCandidateInit, release_candidate_step_tx,
};
use super::run_status::recompute_workflow_run_status_tx;

fn validate_external_completion_status(terminal_status: WorkflowStepStatus) -> Result<()> {
    if terminal_status.is_terminal() {
        return Ok(());
    }

    Err(workflow_external_completion_invalid_status_error(
        terminal_status,
    ))
}

fn validate_external_completion_output(
    terminal_status: WorkflowStepStatus,
    output: Option<&Value>,
) -> Result<()> {
    if output.is_none() || terminal_status == WorkflowStepStatus::Succeeded {
        return Ok(());
    }

    Err(workflow_external_completion_output_invalid_error(
        terminal_status,
    ))
}

fn validate_terminal_transition_status(terminal_status: WorkflowStepStatus) -> Result<()> {
    if !terminal_status.is_terminal() {
        return Err(workflow_internal_state_error(
            "workflow step terminal transition requires terminal status",
        ));
    }

    Ok(())
}

fn external_completion_metadata_matches(
    step: &WorkflowStepDbRecord,
    input: &CompleteExternalWorkflowStepInput<'_>,
) -> bool {
    step.status_reason.as_deref() == input.status_reason
        && step.last_error_code.as_deref() == input.last_error_code
        && step.last_error_message.as_deref() == input.last_error_message
}

struct WorkflowStepTerminalTransition<'reason, 'code, 'message, 'output> {
    job_id: Uuid,
    terminal_status: WorkflowStepStatus,
    status_reason: Option<&'reason str>,
    last_error_code: Option<&'code str>,
    last_error_message: Option<&'message str>,
    output: Option<&'output Value>,
}

struct LockedWorkflowStepTerminalState {
    id: Uuid,
    workflow_run_id: Uuid,
    status: WorkflowStepStatus,
}

async fn jsonb_values_match_tx(
    tx: &mut DbTx<'_>,
    left: Option<&Value>,
    right: Option<&Value>,
) -> Result<bool> {
    let matches = sqlx::query_scalar!(
        "SELECT $1::jsonb IS NOT DISTINCT FROM $2::jsonb AS \"matches!\"",
        left,
        right,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("compare workflow external step output", error)
    })?;

    Ok(matches)
}

pub(crate) async fn process_workflow_step_terminal_by_job_id_tx(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    terminal_status: WorkflowStepStatus,
    status_reason: Option<&str>,
    last_error_code: Option<&str>,
    last_error_message: Option<&str>,
    output: Option<&Value>,
) -> Result<()> {
    // Lifecycle callers already hold the job_queue row lock for `job_id`, so
    // the lookup below is reentrant. Cancellation follows the same job-row-first
    // ordering before taking the exclusive release advisory lock.
    validate_terminal_transition_status(terminal_status)?;
    let transition = WorkflowStepTerminalTransition {
        job_id,
        terminal_status,
        status_reason,
        last_error_code,
        last_error_message,
        output,
    };

    let linked_workflow_step_id: Option<Uuid> = sqlx::query_scalar!(
        "SELECT workflow_step_id FROM job_queue WHERE id = $1 FOR UPDATE",
        transition.job_id
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context(
            "lookup workflow step linkage for job integrity check",
            error,
        )
    })?
    .flatten();

    if let Some(linked_workflow_step_id) = linked_workflow_step_id {
        let mut read_committed_tx = ensure_read_committed_tx(
            tx,
            "workflow job terminal completion",
            "workflow.terminal_completion_unsupported_isolation",
            "Workflow job completion requires READ COMMITTED transaction isolation.",
        )
        .await?;

        return process_linked_workflow_step_terminal_by_job_id_read_committed_tx(
            &mut read_committed_tx,
            &transition,
            linked_workflow_step_id,
        )
        .await;
    }

    process_unlinked_workflow_step_terminal_by_job_id_tx(tx, &transition).await
}

async fn process_linked_workflow_step_terminal_by_job_id_read_committed_tx(
    tx: &mut ReadCommittedTx<'_, '_>,
    transition: &WorkflowStepTerminalTransition<'_, '_, '_, '_>,
    linked_workflow_step_id: Uuid,
) -> Result<()> {
    let tx = tx.as_tx();
    let Some(step) = lock_workflow_step_for_terminal_transition_tx(tx, transition.job_id).await?
    else {
        return Err(workflow_internal_state_error(format!(
            "workflow-managed job {} links to workflow step {linked_workflow_step_id} but workflow_steps.job_id has no matching row",
            transition.job_id,
        )));
    };

    if step.id != linked_workflow_step_id {
        return Err(workflow_internal_state_error(format!(
            "workflow step linkage mismatch for job {}: job_queue.workflow_step_id={linked_workflow_step_id}, workflow_steps.id={}",
            transition.job_id, step.id,
        )));
    }

    if step.status.is_terminal() {
        return Ok(());
    }

    sqlx::query!(
        "UPDATE workflow_steps
         SET status = $2::text::workflow_step_status,
             finished_at = COALESCE(finished_at, now()),
             status_reason = $3,
             last_error_code = $4,
             last_error_message = $5,
             output = CASE
                WHEN $2::text::workflow_step_status = 'SUCCEEDED' THEN $6::jsonb
                ELSE NULL
             END,
             updated_at = now()
         WHERE id = $1",
        step.id,
        transition.terminal_status.as_db_value(),
        transition.status_reason,
        transition.last_error_code,
        transition.last_error_message,
        transition.output,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("mark workflow step terminal", error))?;

    // Job-backed terminal completion already owns the job row. Real cancellation
    // locks job rows before taking the exclusive release lock, so cancellation
    // cannot hold the exclusive lock while also waiting on this job row. If the
    // two transactions meet in the narrow job-row/advisory-lock cycle, this
    // bounded wait turns it into workflow.release_conflict instead of an
    // unbounded deadlock. Waiting here keeps terminal persistence atomic with
    // dependency release instead of stranding dependents behind a rolled-back
    // exclusive holder.
    // Invariant: dependency release runs later in this same transaction, on this
    // same connection, so its pg_try_advisory_xact_lock_shared call is reentrant
    // after this blocking shared acquire.
    lock_workflow_run_release_shared_tx(tx, step.workflow_run_id).await?;
    resolve_terminal_step_queue_tx(
        tx,
        step.workflow_run_id,
        step.id,
        transition.terminal_status,
    )
    .await?;
    recompute_workflow_run_status_tx(tx, step.workflow_run_id).await?;

    Ok(())
}

async fn process_unlinked_workflow_step_terminal_by_job_id_tx(
    tx: &mut DbTx<'_>,
    transition: &WorkflowStepTerminalTransition<'_, '_, '_, '_>,
) -> Result<()> {
    let Some(step) = lock_workflow_step_for_terminal_transition_tx(tx, transition.job_id).await?
    else {
        return Ok(());
    };

    Err(workflow_internal_state_error(format!(
        "workflow step linkage mismatch for job {}: job_queue.workflow_step_id is NULL, workflow_steps.id={}",
        transition.job_id, step.id,
    )))
}

async fn lock_workflow_step_for_terminal_transition_tx(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
) -> Result<Option<LockedWorkflowStepTerminalState>> {
    let row = sqlx::query!(
        "SELECT id, workflow_run_id, status::text AS \"status!\"
         FROM workflow_steps
         WHERE job_id = $1
         FOR UPDATE",
        job_id,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context(
            "lock workflow step by job id for terminal update",
            error,
        )
    })?;

    row.map(|row| {
        Ok(LockedWorkflowStepTerminalState {
            id: row.id,
            workflow_run_id: row.workflow_run_id,
            status: parse_workflow_step_status(row.status)?,
        })
    })
    .transpose()
}

pub async fn complete_external_workflow_step(
    pool: &crate::DbPool,
    input: &CompleteExternalWorkflowStepInput<'_>,
) -> Result<WorkflowStepDbRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    let step = complete_external_workflow_step_tx(&mut tx, input).await?;
    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    Ok(step)
}

pub async fn complete_external_workflow_step_tx(
    tx: &mut DbTx<'_>,
    input: &CompleteExternalWorkflowStepInput<'_>,
) -> Result<WorkflowStepDbRecord> {
    validate_external_completion_status(input.terminal_status)?;
    validate_external_completion_output(input.terminal_status, input.output)?;
    let mut read_committed_tx = ensure_read_committed_tx(
        tx,
        "workflow external step completion",
        "workflow.external_completion_unsupported_isolation",
        "External workflow step completion requires READ COMMITTED transaction isolation.",
    )
    .await?;

    complete_external_workflow_step_read_committed_tx(&mut read_committed_tx, input).await
}

async fn complete_external_workflow_step_read_committed_tx(
    tx: &mut ReadCommittedTx<'_, '_>,
    input: &CompleteExternalWorkflowStepInput<'_>,
) -> Result<WorkflowStepDbRecord> {
    let tx = tx.as_tx();

    lock_workflow_step_rows_for_update_tx(tx, input.workflow_run_id, input.organization_id).await?;

    let row = sqlx::query_as!(
        WorkflowStepRow,
        "SELECT
            ws.id,
            ws.workflow_run_id,
            ws.step_key,
            ws.execution_kind::text AS \"execution_kind!\",
            ws.job_type,
            ws.organization_id,
            ws.payload,
            ws.priority,
            ws.max_attempts,
            ws.timeout_seconds,
            ws.stage,
            ws.allow_handler_continuation,
            ws.execution_resource_key,
            ws.status::text AS \"status!\",
            ws.job_id,
            ws.released_at,
            ws.started_at,
            ws.finished_at,
            ws.dependency_count_total,
            ws.dependency_count_pending,
            ws.dependency_count_unsatisfied,
            ws.status_reason,
            ws.last_error_code,
            ws.last_error_message,
            ws.output,
            ws.created_at,
            ws.updated_at
         FROM workflow_steps ws
         JOIN workflow_runs wr ON wr.id = ws.workflow_run_id
         WHERE ws.workflow_run_id = $1
           AND ws.step_key = $2
           AND ($3::uuid IS NULL OR wr.organization_id = $3)
         FOR UPDATE",
        input.workflow_run_id,
        input.step_key.as_str(),
        input.organization_id,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lock external workflow step for completion", error)
    })?
    .ok_or_else(workflow_external_step_not_found_error)?;

    let stored_output = row.output.clone();
    let step = row.into_record()?;
    if step.execution_kind != WorkflowStepExecutionKind::External {
        return Err(workflow_external_step_not_external_error(
            step.step_key.as_str(),
        ));
    }

    if step.status.is_terminal() {
        if step.status == input.terminal_status {
            if step.status == WorkflowStepStatus::Succeeded
                && !jsonb_values_match_tx(tx, stored_output.as_ref(), input.output).await?
            {
                return Err(workflow_external_completion_output_conflict_error(
                    step.step_key.as_str(),
                ));
            }

            if !external_completion_metadata_matches(&step, input) {
                return Err(workflow_external_completion_metadata_conflict_error(
                    step.step_key.as_str(),
                ));
            }

            return Ok(step);
        }

        return Err(workflow_external_completion_conflict_error(
            step.step_key.as_str(),
            step.status,
            input.terminal_status,
        ));
    }

    if step.status != WorkflowStepStatus::WaitingForExternal {
        return Err(workflow_external_step_not_waiting_error(
            step.step_key.as_str(),
            step.status,
        ));
    }

    if !try_lock_workflow_run_release_shared_tx(tx, step.workflow_run_id).await? {
        return Err(workflow_release_conflict_error(step.workflow_run_id));
    }

    let updated = sqlx::query_as!(
        WorkflowStepRow,
        "UPDATE workflow_steps
         SET status = $2::text::workflow_step_status,
             finished_at = COALESCE(finished_at, now()),
             status_reason = $3,
             last_error_code = $4,
             last_error_message = $5,
             output = CASE
                WHEN $2::text::workflow_step_status = 'SUCCEEDED' THEN $6::jsonb
                ELSE NULL
             END,
             updated_at = now()
         WHERE id = $1
         RETURNING
            id,
            workflow_run_id,
            step_key,
            execution_kind::text AS \"execution_kind!\",
            job_type,
            organization_id,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            stage,
            allow_handler_continuation,
            execution_resource_key,
            status::text AS \"status!\",
            job_id,
            released_at,
            started_at,
            finished_at,
            dependency_count_total,
            dependency_count_pending,
            dependency_count_unsatisfied,
            status_reason,
            last_error_code,
            last_error_message,
            output,
            created_at,
            updated_at",
        step.id,
        input.terminal_status.as_db_value(),
        input.status_reason,
        input.last_error_code,
        input.last_error_message,
        input.output,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("mark external workflow step terminal", error)
    })?;

    let updated = updated.into_record()?;
    // External completion already owns the workflow-step row locks. Do not wait
    // on the shared release advisory lock here: cancellation may own the
    // exclusive form while waiting on these same rows. Dependent release goes
    // through try_lock_workflow_run_release_shared_tx, so concurrent
    // cancellation returns workflow.release_conflict before this transaction can
    // release new work.
    resolve_terminal_step_queue_tx(tx, updated.workflow_run_id, updated.id, updated.status).await?;
    recompute_workflow_run_status_tx(tx, updated.workflow_run_id).await?;

    Ok(updated)
}

pub(crate) async fn resolve_terminal_step_queue_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    initial_step_id: Uuid,
    initial_terminal_status: WorkflowStepStatus,
) -> Result<()> {
    let mut terminal_queue = VecDeque::from([(initial_step_id, initial_terminal_status)]);

    while let Some((prerequisite_step_id, prerequisite_terminal_status)) =
        terminal_queue.pop_front()
    {
        let edges = sqlx::query!(
            "SELECT workflow_run_id, dependent_step_id,
                    release_mode::text AS \"release_mode!\"
             FROM workflow_step_dependencies
             WHERE prerequisite_step_id = $1",
            prerequisite_step_id,
        )
        .fetch_all(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("lookup workflow step dependency edges", error)
        })?;

        for edge in edges {
            if edge.workflow_run_id != workflow_run_id {
                return Err(workflow_internal_state_error(format!(
                    "workflow dependency edge from prerequisite step {prerequisite_step_id} belongs to run {}, expected {workflow_run_id}",
                    edge.workflow_run_id,
                )));
            }

            let dependent_step_id: Uuid = edge.dependent_step_id;
            let release_mode = parse_workflow_release_mode(edge.release_mode)?;
            let dependency_unsatisfied =
                matches!(release_mode, WorkflowDependencyReleaseMode::OnSuccess)
                    && !matches!(prerequisite_terminal_status, WorkflowStepStatus::Succeeded);

            let row = sqlx::query!(
                "UPDATE workflow_steps
                 SET dependency_count_pending = dependency_count_pending - 1,
                     dependency_count_unsatisfied = dependency_count_unsatisfied +
                        CASE WHEN $2 THEN 1 ELSE 0 END,
                     updated_at = now()
                 WHERE id = $1
                   AND workflow_run_id = $3
                 RETURNING
                    id,
                    workflow_run_id,
                    execution_kind::text AS \"execution_kind!\",
                    job_type,
                    organization_id,
                    payload,
                    priority,
                    max_attempts,
                    timeout_seconds,
                    stage,
                    execution_resource_key,
                    status::text AS \"status!\",
                    dependency_count_pending,
                    dependency_count_unsatisfied",
                dependent_step_id,
                dependency_unsatisfied,
                workflow_run_id,
            )
            .fetch_optional(&mut **tx)
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context(
                    "update workflow step dependency counters",
                    error,
                )
            })?
            .ok_or_else(|| {
                workflow_internal_state_error(format!(
                    "workflow dependency edge from prerequisite step {prerequisite_step_id} references dependent step {dependent_step_id} outside expected run {workflow_run_id}",
                ))
            })?;

            let candidate = StepReleaseCandidate::from_decoded_fields(StepReleaseCandidateInit {
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
                execution_resource_key: row.execution_resource_key,
            });
            let status = parse_workflow_step_status(row.status)?;
            let dependency_count_pending: i32 = row.dependency_count_pending;
            let dependency_count_unsatisfied: i32 = row.dependency_count_unsatisfied;
            if dependency_count_pending != 0 {
                continue;
            }

            if status != WorkflowStepStatus::Blocked {
                continue;
            }

            if dependency_count_unsatisfied == 0 {
                release_candidate_step_tx(tx, &candidate).await?;
                continue;
            }

            let canceled_row = sqlx::query_scalar!(
                "UPDATE workflow_steps
                 SET status = 'CANCELED',
                     finished_at = COALESCE(finished_at, now()),
                     status_reason = 'workflow.dependency_unsatisfied',
                     last_error_code = 'workflow.dependency_unsatisfied',
                     last_error_message = 'Step dependency requirements were not satisfied.',
                     output = NULL,
                     updated_at = now()
                 WHERE id = $1
                   AND workflow_run_id = $2
                   AND status = 'BLOCKED'
                 RETURNING id",
                candidate.id(),
                workflow_run_id,
            )
            .fetch_optional(&mut **tx)
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context("cancel blocked workflow step", error)
            })?;

            if canceled_row.is_some() {
                terminal_queue.push_back((candidate.id(), WorkflowStepStatus::Canceled));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{Error, QueryErrorCategory};

    #[test]
    fn workflow_terminal_transition_requires_terminal_status() {
        let result = super::validate_terminal_transition_status(
            runledger_core::jobs::WorkflowStepStatus::Running,
        );
        match result {
            Err(Error::QueryError(query_error)) => {
                assert_eq!(query_error.category(), QueryErrorCategory::Internal);
                assert_eq!(query_error.code(), "workflow.internal_state");
                assert!(
                    query_error
                        .internal_message()
                        .contains("workflow step terminal transition requires terminal status"),
                    "unexpected internal message: {}",
                    query_error.internal_message()
                );
            }
            other => panic!("expected internal workflow state error, got {other:?}"),
        }

        assert!(
            super::validate_terminal_transition_status(
                runledger_core::jobs::WorkflowStepStatus::Succeeded
            )
            .is_ok()
        );
        assert!(
            super::validate_terminal_transition_status(
                runledger_core::jobs::WorkflowStepStatus::Failed
            )
            .is_ok()
        );
    }
}
