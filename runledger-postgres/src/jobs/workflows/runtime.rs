use std::collections::{BTreeSet, VecDeque};

use runledger_core::jobs::{
    WorkflowDependencyReleaseMode, WorkflowRunStatus, WorkflowStepExecutionKind, WorkflowStepStatus,
};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::jobs::transaction_isolation::ensure_read_committed_tx;
use crate::{DbTx, Error, Result};

use super::super::errors::workflow_handler_continuation_not_enabled_error;
use super::super::row_decode::{
    parse_job_stage, parse_job_type_name, parse_workflow_release_mode, parse_workflow_run_status,
    parse_workflow_step_execution_kind, parse_workflow_step_status,
};
use super::super::rows::WorkflowStepRow;
use super::super::types::HANDLER_CONTINUATION_REASON;
use super::super::workflow_types::{CompleteExternalWorkflowStepInput, WorkflowStepDbRecord};
use super::active_claims::release_or_defer_workflow_active_claim_tx;
use super::errors::{
    workflow_external_completion_conflict_error, workflow_external_completion_invalid_status_error,
    workflow_external_completion_metadata_conflict_error,
    workflow_external_completion_output_conflict_error,
    workflow_external_completion_output_invalid_error, workflow_external_step_not_external_error,
    workflow_external_step_not_found_error, workflow_external_step_not_waiting_error,
    workflow_internal_state_error, workflow_release_conflict_error,
};
use super::locking::{
    lock_workflow_run_release_shared_tx, lock_workflow_step_rows_for_update_tx,
    try_lock_workflow_run_release_shared_tx,
};
use super::release::{StepReleaseCandidate, StepReleaseCandidateInit, release_candidate_step_tx};

pub(crate) const WORKFLOW_RUN_TERMINAL_CHANNEL: &str = "runledger_workflow_run_terminal";

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

    let linked_workflow_step_id: Option<Uuid> = sqlx::query_scalar!(
        "SELECT workflow_step_id FROM job_queue WHERE id = $1 FOR UPDATE",
        job_id
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

    if linked_workflow_step_id.is_some() {
        ensure_read_committed_tx(
            tx,
            "workflow job terminal completion",
            "workflow.terminal_completion_unsupported_isolation",
            "Workflow job completion requires READ COMMITTED transaction isolation.",
        )
        .await?;
    }

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

    let Some(row) = row else {
        if let Some(workflow_step_id) = linked_workflow_step_id {
            return Err(workflow_internal_state_error(format!(
                "workflow-managed job {job_id} links to workflow step {workflow_step_id} but workflow_steps.job_id has no matching row"
            )));
        }

        return Ok(());
    };

    let step_id: Uuid = row.id;
    let workflow_run_id: Uuid = row.workflow_run_id;
    let current_status = parse_workflow_step_status(row.status)?;

    if linked_workflow_step_id != Some(step_id) {
        let message = match linked_workflow_step_id {
            Some(linked_step_id) => format!(
                "workflow step linkage mismatch for job {job_id}: job_queue.workflow_step_id={linked_step_id}, workflow_steps.id={step_id}"
            ),
            None => format!(
                "workflow step linkage mismatch for job {job_id}: job_queue.workflow_step_id is NULL, workflow_steps.id={step_id}"
            ),
        };
        return Err(workflow_internal_state_error(message));
    }

    if current_status.is_terminal() {
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
        step_id,
        terminal_status.as_db_value(),
        status_reason,
        last_error_code,
        last_error_message,
        output,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("mark workflow step terminal", error))?;

    let mut touched_run_ids = BTreeSet::from([workflow_run_id]);
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
    lock_workflow_run_release_shared_tx(tx, workflow_run_id).await?;
    resolve_terminal_step_queue_tx(tx, step_id, terminal_status, &mut touched_run_ids).await?;
    recompute_workflow_run_statuses_tx(tx, &touched_run_ids).await?;

    Ok(())
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
    ensure_read_committed_tx(
        tx,
        "workflow external step completion",
        "workflow.external_completion_unsupported_isolation",
        "External workflow step completion requires READ COMMITTED transaction isolation.",
    )
    .await?;
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
    let mut touched_run_ids = BTreeSet::from([updated.workflow_run_id]);
    // External completion already owns the workflow-step row locks. Do not wait
    // on the shared release advisory lock here: cancellation may own the
    // exclusive form while waiting on these same rows. Dependent release goes
    // through try_lock_workflow_run_release_shared_tx, so concurrent
    // cancellation returns workflow.release_conflict before this transaction can
    // release new work.
    resolve_terminal_step_queue_tx(tx, updated.id, updated.status, &mut touched_run_ids).await?;
    recompute_workflow_run_statuses_tx(tx, &touched_run_ids).await?;

    Ok(updated)
}

pub(crate) async fn resolve_terminal_step_queue_tx(
    tx: &mut DbTx<'_>,
    initial_step_id: Uuid,
    initial_terminal_status: WorkflowStepStatus,
    touched_run_ids: &mut BTreeSet<Uuid>,
) -> Result<()> {
    let mut terminal_queue = VecDeque::from([(initial_step_id, initial_terminal_status)]);

    while let Some((prerequisite_step_id, prerequisite_terminal_status)) =
        terminal_queue.pop_front()
    {
        let edges = sqlx::query!(
            "SELECT dependent_step_id, release_mode::text AS \"release_mode!\"
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
            )
            .fetch_one(&mut **tx)
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context(
                    "update workflow step dependency counters",
                    error,
                )
            })?;

            let candidate = StepReleaseCandidate::from_init(StepReleaseCandidateInit {
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
            touched_run_ids.insert(candidate.workflow_run_id());

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
                   AND status = 'BLOCKED'
                 RETURNING workflow_run_id",
                candidate.id(),
            )
            .fetch_optional(&mut **tx)
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context("cancel blocked workflow step", error)
            })?;

            if let Some(workflow_run_id) = canceled_row {
                touched_run_ids.insert(workflow_run_id);
                terminal_queue.push_back((candidate.id(), WorkflowStepStatus::Canceled));
            }
        }
    }

    Ok(())
}

pub(crate) async fn notify_workflow_run_terminal_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    status: WorkflowRunStatus,
) -> Result<()> {
    sqlx::query!(
        "SELECT pg_notify(
            $1,
            json_build_object(
                'workflow_run_id', $2::uuid::text,
                'status', $3::text
            )::text
         )",
        WORKFLOW_RUN_TERMINAL_CHANNEL,
        workflow_run_id,
        status.as_db_value(),
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("notify workflow run terminal", error))?;

    Ok(())
}

pub(crate) async fn recompute_workflow_run_statuses_tx(
    tx: &mut DbTx<'_>,
    touched_run_ids: &BTreeSet<Uuid>,
) -> Result<()> {
    for workflow_run_id in touched_run_ids {
        // Serialize status recomputation per run so concurrent step completions do not
        // leave the run stuck in RUNNING due to snapshot races.
        let run_status = sqlx::query!(
            "SELECT status::text AS \"status!\"
             FROM workflow_runs
             WHERE id = $1
             FOR UPDATE",
            *workflow_run_id
        )
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("lock workflow run for status recompute", error)
        })?;
        let Some(run_status) = run_status else {
            continue;
        };
        let previous_status = parse_workflow_run_status(run_status.status)?;

        let row = sqlx::query!(
            "SELECT
                COUNT(*) FILTER (
                    WHERE status IN ('BLOCKED', 'ENQUEUED', 'RUNNING')
                )::bigint AS \"active_steps!\",
                COUNT(*) FILTER (
                    WHERE status = 'WAITING_FOR_EXTERNAL'
                )::bigint AS \"waiting_steps!\",
                COUNT(*) FILTER (
                    WHERE status IN ('FAILED', 'CANCELED')
                )::bigint AS \"errored_steps!\"
             FROM workflow_steps
             WHERE workflow_run_id = $1",
            *workflow_run_id,
        )
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("recompute workflow run status counters", error)
        })?;

        let active_steps: i64 = row.active_steps;
        let waiting_steps: i64 = row.waiting_steps;
        let errored_steps: i64 = row.errored_steps;

        let next_status = if active_steps > 0 {
            WorkflowRunStatus::Running
        } else if waiting_steps > 0 {
            WorkflowRunStatus::WaitingForExternal
        } else if errored_steps > 0 {
            WorkflowRunStatus::CompletedWithErrors
        } else {
            WorkflowRunStatus::Succeeded
        };

        if previous_status == next_status && next_status.is_terminal() {
            continue;
        }

        let updated = sqlx::query!(
            "UPDATE workflow_runs
             SET status = $2::text::workflow_run_status,
                 finished_at = CASE
                    WHEN $2::text::workflow_run_status IN ('RUNNING', 'WAITING_FOR_EXTERNAL')
                        THEN NULL
                    ELSE COALESCE(finished_at, now())
                 END,
                 result = CASE
                    WHEN $2::text::workflow_run_status = 'SUCCEEDED' THEN (
                        SELECT ws.output
                        FROM workflow_steps ws
                        WHERE ws.workflow_run_id = workflow_runs.id
                          AND ws.step_key = workflow_runs.result_step_key
                          AND ws.status = 'SUCCEEDED'
                        LIMIT 1
                    )
                    WHEN $2::text::workflow_run_status IN ('COMPLETED_WITH_ERRORS', 'CANCELED')
                        THEN NULL
                    ELSE result
                 END,
                 updated_at = now()
             WHERE id = $1
               AND status <> 'CANCELED'",
            *workflow_run_id,
            next_status.as_db_value(),
        )
        .execute(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("update workflow run recomputed status", error)
        })?;

        if updated.rows_affected() > 0
            && next_status.is_terminal()
            && !previous_status.is_terminal()
        {
            release_or_defer_workflow_active_claim_tx(tx, *workflow_run_id).await?;
            notify_workflow_run_terminal_tx(tx, *workflow_run_id, next_status).await?;
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
