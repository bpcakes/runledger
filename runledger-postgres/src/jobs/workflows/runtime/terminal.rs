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
    workflow_external_completion_conflict_error,
    workflow_external_completion_metadata_conflict_error,
    workflow_external_completion_output_conflict_error, workflow_external_step_not_external_error,
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

    let workflow_managed = sqlx::query_scalar!(
        r#"SELECT EXISTS (
                SELECT 1
                FROM workflow_steps ws
                WHERE ws.job_id = jq.id
            ) AS "workflow_managed!"
         FROM job_queue jq
         WHERE jq.id = $1
         FOR UPDATE OF jq"#,
        transition.job_id
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lookup workflow step ownership by job id", error)
    })?
    .unwrap_or(false);

    if !workflow_managed {
        return Ok(());
    }

    let mut read_committed_tx = ensure_read_committed_tx(
        tx,
        "workflow job terminal completion",
        "workflow.terminal_completion_unsupported_isolation",
        "Workflow job completion requires READ COMMITTED transaction isolation.",
    )
    .await?;

    process_linked_workflow_step_terminal_by_job_id_read_committed_tx(
        &mut read_committed_tx,
        &transition,
    )
    .await
}

async fn process_linked_workflow_step_terminal_by_job_id_read_committed_tx(
    tx: &mut ReadCommittedTx<'_, '_>,
    transition: &WorkflowStepTerminalTransition<'_, '_, '_, '_>,
) -> Result<()> {
    let tx = tx.as_tx();
    let Some(step) = lock_workflow_step_for_terminal_transition_tx(tx, transition.job_id).await?
    else {
        return Err(workflow_internal_state_error(format!(
            "workflow-managed job {} lost its workflow_steps.job_id relationship while locked",
            transition.job_id,
        )));
    };

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
    let terminal_status = input.outcome.status();
    let output = input.outcome.output();

    let (step, stored_output) = lock_external_workflow_step_for_completion_tx(tx, input).await?;
    if step.execution_kind != WorkflowStepExecutionKind::External {
        return Err(workflow_external_step_not_external_error(
            step.step_key.as_str(),
        ));
    }

    if step.status.is_terminal() {
        if step.status == terminal_status {
            if step.status == WorkflowStepStatus::Succeeded
                && !jsonb_values_match_tx(tx, stored_output.as_ref(), output).await?
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
            terminal_status,
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
        terminal_status.as_db_value(),
        input.status_reason,
        input.last_error_code,
        input.last_error_message,
        output,
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

async fn lock_external_workflow_step_for_completion_tx(
    tx: &mut DbTx<'_>,
    input: &CompleteExternalWorkflowStepInput<'_>,
) -> Result<(WorkflowStepDbRecord, Option<Value>)> {
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
    Ok((step, stored_output))
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
        apply_terminal_prerequisite(
            tx,
            workflow_run_id,
            prerequisite_step_id,
            prerequisite_terminal_status,
            &mut terminal_queue,
        )
        .await?;
    }

    Ok(())
}

async fn apply_terminal_prerequisite(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    prerequisite_step_id: Uuid,
    prerequisite_terminal_status: WorkflowStepStatus,
    terminal_queue: &mut VecDeque<(Uuid, WorkflowStepStatus)>,
) -> Result<()> {
    let direct_dependent_updates = load_direct_dependent_updates(
        tx,
        workflow_run_id,
        prerequisite_step_id,
        prerequisite_terminal_status,
    )
    .await?;
    if direct_dependent_updates.is_empty() {
        return Ok(());
    }

    let rows = lock_and_update_direct_dependents(
        tx,
        workflow_run_id,
        prerequisite_step_id,
        &direct_dependent_updates,
    )
    .await?;

    for row in rows {
        enqueue_ready_or_canceled_dependent(tx, workflow_run_id, row, terminal_queue).await?;
    }

    Ok(())
}

struct DirectDependentUpdate {
    step_id: Uuid,
    dependency_unsatisfied: bool,
}

async fn load_direct_dependent_updates(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    prerequisite_step_id: Uuid,
    prerequisite_terminal_status: WorkflowStepStatus,
) -> Result<Vec<DirectDependentUpdate>> {
    let edges = sqlx::query!(
        "SELECT workflow_run_id, dependent_step_id,
                    release_mode::text AS \"release_mode!\"
             FROM workflow_step_dependencies
             WHERE prerequisite_step_id = $1
             ORDER BY dependent_step_id ASC",
        prerequisite_step_id,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lookup workflow step dependency edges", error)
    })?;

    let mut updates = Vec::with_capacity(edges.len());
    for edge in edges {
        if edge.workflow_run_id != workflow_run_id {
            return Err(workflow_internal_state_error(format!(
                "workflow dependency edge from prerequisite step {prerequisite_step_id} belongs to run {}, expected {workflow_run_id}",
                edge.workflow_run_id,
            )));
        }

        let dependent_step_id: Uuid = edge.dependent_step_id;
        let release_mode = parse_workflow_release_mode(edge.release_mode)?;
        let is_unsatisfied = matches!(release_mode, WorkflowDependencyReleaseMode::OnSuccess)
            && !matches!(prerequisite_terminal_status, WorkflowStepStatus::Succeeded);

        updates.push(DirectDependentUpdate {
            step_id: dependent_step_id,
            dependency_unsatisfied: is_unsatisfied,
        });
    }

    Ok(updates)
}

async fn lock_and_update_direct_dependents(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    prerequisite_step_id: Uuid,
    updates: &[DirectDependentUpdate],
) -> Result<Vec<UpdatedDependent>> {
    let (dependent_step_ids, dependency_unsatisfied) = direct_dependent_update_arrays(updates);

    // Intersecting fan-outs can update the same dependent from concurrent
    // prerequisite completions. Lock the entire direct batch in stable UUID
    // order before changing any counters so every completion acquires shared
    // rows in the same order. The following UPDATE runs as a fresh READ
    // COMMITTED statement and therefore observes counters committed while
    // this lock acquisition waited.
    let locked_dependent_step_ids = sqlx::query_scalar!(
        "SELECT id
             FROM workflow_steps
             WHERE id = ANY($1::uuid[])
               AND workflow_run_id = $2
             ORDER BY id ASC
             FOR UPDATE",
        &dependent_step_ids,
        workflow_run_id,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context(
            "lock direct workflow dependents for counter update",
            error,
        )
    })?;

    if let Some(dependent_step_id) = dependent_step_ids.iter().find(|dependent_step_id| {
        locked_dependent_step_ids
            .binary_search(dependent_step_id)
            .is_err()
    }) {
        return Err(workflow_internal_state_error(format!(
            "workflow dependency edge from prerequisite step {prerequisite_step_id} references dependent step {dependent_step_id} outside expected run {workflow_run_id}",
        )));
    }

    let mut rows = sqlx::query!(
        "WITH dependency_updates AS (
                SELECT dependent_step_id, dependency_unsatisfied
                FROM unnest($1::uuid[], $2::boolean[])
                    AS direct_dependents(dependent_step_id, dependency_unsatisfied)
             )
             UPDATE workflow_steps AS ws
             SET dependency_count_pending = ws.dependency_count_pending - 1,
                 dependency_count_unsatisfied = ws.dependency_count_unsatisfied +
                    CASE WHEN dependency_updates.dependency_unsatisfied THEN 1 ELSE 0 END,
                 updated_at = now()
             FROM dependency_updates
             WHERE ws.id = dependency_updates.dependent_step_id
               AND ws.workflow_run_id = $3
             RETURNING
                ws.id,
                ws.workflow_run_id,
                ws.execution_kind::text AS \"execution_kind!\",
                ws.job_type,
                ws.organization_id,
                ws.payload,
                ws.priority,
                ws.max_attempts,
                ws.timeout_seconds,
                ws.stage,
                ws.execution_resource_key,
                ws.status::text AS \"status!\",
                ws.dependency_count_pending,
                ws.dependency_count_unsatisfied",
        &dependent_step_ids,
        &dependency_unsatisfied,
        workflow_run_id,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("batch update workflow step dependency counters", error)
    })?;
    rows.sort_by_key(|row| row.id);

    if rows
        .iter()
        .map(|row| row.id)
        .ne(dependent_step_ids.iter().copied())
    {
        return Err(workflow_internal_state_error(format!(
            "workflow dependency batch from prerequisite step {prerequisite_step_id} updated an unexpected dependent set in run {workflow_run_id}",
        )));
    }

    let mut decoded = Vec::with_capacity(rows.len());
    for row in rows {
        decoded.push(UpdatedDependent {
            candidate: StepReleaseCandidate::from_decoded_fields(StepReleaseCandidateInit {
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
            }),
            status: parse_workflow_step_status(row.status)?,
            dependency_count_pending: row.dependency_count_pending,
            dependency_count_unsatisfied: row.dependency_count_unsatisfied,
        });
    }
    Ok(decoded)
}

fn direct_dependent_update_arrays(updates: &[DirectDependentUpdate]) -> (Vec<Uuid>, Vec<bool>) {
    updates
        .iter()
        .map(|update| (update.step_id, update.dependency_unsatisfied))
        .unzip()
}

struct UpdatedDependent {
    candidate: StepReleaseCandidate,
    status: WorkflowStepStatus,
    dependency_count_pending: i32,
    dependency_count_unsatisfied: i32,
}

async fn enqueue_ready_or_canceled_dependent(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    row: UpdatedDependent,
    terminal_queue: &mut VecDeque<(Uuid, WorkflowStepStatus)>,
) -> Result<()> {
    if row.dependency_count_pending != 0 || row.status != WorkflowStepStatus::Blocked {
        return Ok(());
    }

    if row.dependency_count_unsatisfied == 0 {
        release_candidate_step_tx(tx, &row.candidate).await?;
        return Ok(());
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
        row.candidate.id(),
        workflow_run_id,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("cancel blocked workflow step", error))?;

    if canceled_row.is_some() {
        terminal_queue.push_back((row.candidate.id(), WorkflowStepStatus::Canceled));
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
