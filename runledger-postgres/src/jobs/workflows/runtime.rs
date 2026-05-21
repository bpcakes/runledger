use std::collections::{BTreeSet, VecDeque};

use chrono::{DateTime, Utc};
use runledger_core::jobs::{
    JobStage, JobTypeName, WorkflowDependencyReleaseMode, WorkflowRunStatus,
    WorkflowStepExecutionKind, WorkflowStepStatus,
};
use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

use super::super::row_decode::{
    parse_job_stage, parse_job_type_name, parse_step_key_name, parse_workflow_release_mode,
    parse_workflow_step_execution_kind, parse_workflow_step_status,
};
use super::super::workflow_types::{CompleteExternalWorkflowStepInput, WorkflowStepDbRecord};
use super::mutate::lock_workflow_step_rows_for_update_tx;
use super::{
    StepReleaseCandidate, ensure_read_committed_tx, lock_workflow_run_release_shared_tx,
    try_lock_workflow_run_release_shared_tx, workflow_external_completion_conflict_error,
    workflow_external_completion_invalid_status_error, workflow_external_step_not_external_error,
    workflow_external_step_not_found_error, workflow_external_step_not_waiting_error,
    workflow_internal_state_error, workflow_release_conflict_error,
};

#[derive(sqlx::FromRow)]
struct WorkflowStepRow {
    id: Uuid,
    workflow_run_id: Uuid,
    step_key: String,
    execution_kind: String,
    job_type: Option<String>,
    organization_id: Option<Uuid>,
    payload: serde_json::Value,
    priority: Option<i32>,
    max_attempts: Option<i32>,
    timeout_seconds: Option<i32>,
    stage: Option<String>,
    status: String,
    job_id: Option<Uuid>,
    released_at: Option<DateTime<Utc>>,
    started_at: Option<DateTime<Utc>>,
    finished_at: Option<DateTime<Utc>>,
    dependency_count_total: i32,
    dependency_count_pending: i32,
    dependency_count_unsatisfied: i32,
    status_reason: Option<String>,
    last_error_code: Option<String>,
    last_error_message: Option<String>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

fn workflow_step_db_record_from_row(row: WorkflowStepRow) -> Result<WorkflowStepDbRecord> {
    Ok(WorkflowStepDbRecord {
        id: row.id,
        workflow_run_id: row.workflow_run_id,
        step_key: parse_step_key_name(row.step_key)?,
        execution_kind: parse_workflow_step_execution_kind(row.execution_kind)?,
        job_type: row.job_type.map(parse_job_type_name).transpose()?,
        organization_id: row.organization_id,
        payload: row.payload,
        priority: row.priority,
        max_attempts: row.max_attempts,
        timeout_seconds: row.timeout_seconds,
        stage: row.stage.map(parse_job_stage).transpose()?,
        status: parse_workflow_step_status(row.status)?,
        job_id: row.job_id,
        released_at: row.released_at,
        started_at: row.started_at,
        finished_at: row.finished_at,
        dependency_count_total: row.dependency_count_total,
        dependency_count_pending: row.dependency_count_pending,
        dependency_count_unsatisfied: row.dependency_count_unsatisfied,
        status_reason: row.status_reason,
        last_error_code: row.last_error_code,
        last_error_message: row.last_error_message,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

fn validate_external_completion_status(terminal_status: WorkflowStepStatus) -> Result<()> {
    if terminal_status.is_terminal() {
        return Ok(());
    }

    Err(workflow_external_completion_invalid_status_error(
        terminal_status,
    ))
}

fn job_release_fields(
    candidate: &StepReleaseCandidate,
) -> Result<(&JobTypeName, i32, i32, i32, JobStage)> {
    let Some(job_type) = candidate.job_type.as_ref() else {
        return Err(workflow_internal_state_error(
            "job workflow step release is missing job_type",
        ));
    };
    let Some(priority) = candidate.priority else {
        return Err(workflow_internal_state_error(
            "job workflow step release is missing priority",
        ));
    };
    let Some(max_attempts) = candidate.max_attempts else {
        return Err(workflow_internal_state_error(
            "job workflow step release is missing max_attempts",
        ));
    };
    let Some(timeout_seconds) = candidate.timeout_seconds else {
        return Err(workflow_internal_state_error(
            "job workflow step release is missing timeout_seconds",
        ));
    };
    let Some(stage) = candidate.stage else {
        return Err(workflow_internal_state_error(
            "job workflow step release is missing stage",
        ));
    };

    Ok((job_type, priority, max_attempts, timeout_seconds, stage))
}

fn validate_terminal_transition_status(terminal_status: WorkflowStepStatus) -> Result<()> {
    if !terminal_status.is_terminal() {
        return Err(workflow_internal_state_error(
            "workflow step terminal transition requires terminal status",
        ));
    }

    Ok(())
}

pub(crate) async fn mark_workflow_step_running_for_claim_tx(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
) -> Result<()> {
    sqlx::query!(
        "UPDATE workflow_steps
         SET status = 'RUNNING',
             started_at = COALESCE(started_at, now()),
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
    sqlx::query(
        "UPDATE workflow_steps
         SET status = 'ENQUEUED',
             finished_at = NULL,
             status_reason = $2,
             last_error_code = $3,
             last_error_message = $4,
             updated_at = now()
         WHERE job_id = $1
           AND status = 'RUNNING'",
    )
    .bind(job_id)
    .bind(status_reason)
    .bind(last_error_code)
    .bind(last_error_message)
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("mark workflow step enqueued for retry", error)
    })?;

    Ok(())
}

pub(crate) async fn process_workflow_step_terminal_by_job_id_tx(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    terminal_status: WorkflowStepStatus,
    status_reason: Option<&str>,
    last_error_code: Option<&str>,
    last_error_message: Option<&str>,
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
             updated_at = now()
         WHERE id = $1",
        step_id,
        terminal_status.as_db_value(),
        status_reason,
        last_error_code,
        last_error_message,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("mark workflow step terminal", error))?;

    let mut touched_run_ids = BTreeSet::from([workflow_run_id]);
    // Job-backed terminal completion already owns the job row. Real cancellation
    // locks job rows before taking the exclusive release lock, so cancellation
    // cannot hold the exclusive lock while also waiting on this job row. Waiting
    // here keeps terminal persistence atomic with dependency release instead of
    // stranding dependents behind a rolled-back exclusive holder.
    // Invariant: dependency release runs later in this same transaction, on this
    // same connection, so its shared advisory try-lock is reentrant.
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
    lock_workflow_step_rows_for_update_tx(tx, input.workflow_run_id, input.organization_id).await?;

    let row = sqlx::query_as::<_, WorkflowStepRow>(
        "SELECT
            ws.id,
            ws.workflow_run_id,
            ws.step_key,
            ws.execution_kind::text AS execution_kind,
            ws.job_type,
            ws.organization_id,
            ws.payload,
            ws.priority,
            ws.max_attempts,
            ws.timeout_seconds,
            ws.stage,
            ws.status::text AS status,
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
            ws.created_at,
            ws.updated_at
         FROM workflow_steps ws
         JOIN workflow_runs wr ON wr.id = ws.workflow_run_id
         WHERE ws.workflow_run_id = $1
           AND ws.step_key = $2
           AND ($3::uuid IS NULL OR wr.organization_id = $3)
         FOR UPDATE",
    )
    .bind(input.workflow_run_id)
    .bind(input.step_key.as_str())
    .bind(input.organization_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lock external workflow step for completion", error)
    })?
    .ok_or_else(workflow_external_step_not_found_error)?;

    let step = workflow_step_db_record_from_row(row)?;
    if step.execution_kind != WorkflowStepExecutionKind::External {
        return Err(workflow_external_step_not_external_error(
            step.step_key.as_str(),
        ));
    }

    if step.status.is_terminal() {
        if step.status == input.terminal_status {
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

    let updated = sqlx::query_as::<_, WorkflowStepRow>(
        "UPDATE workflow_steps
         SET status = $2::text::workflow_step_status,
             finished_at = COALESCE(finished_at, now()),
             status_reason = $3,
             last_error_code = $4,
             last_error_message = $5,
             updated_at = now()
         WHERE id = $1
         RETURNING
            id,
            workflow_run_id,
            step_key,
            execution_kind::text AS execution_kind,
            job_type,
            organization_id,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            stage,
            status::text AS status,
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
            created_at,
            updated_at",
    )
    .bind(step.id)
    .bind(input.terminal_status.as_db_value())
    .bind(input.status_reason)
    .bind(input.last_error_code)
    .bind(input.last_error_message)
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("mark external workflow step terminal", error)
    })?;

    let updated = workflow_step_db_record_from_row(updated)?;
    let mut touched_run_ids = BTreeSet::from([updated.workflow_run_id]);
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

            let candidate = StepReleaseCandidate {
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
            };
            let status = parse_workflow_step_status(row.status)?;
            let dependency_count_pending: i32 = row.dependency_count_pending;
            let dependency_count_unsatisfied: i32 = row.dependency_count_unsatisfied;
            touched_run_ids.insert(candidate.workflow_run_id);

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

            let canceled_row = sqlx::query!(
                "UPDATE workflow_steps
                 SET status = 'CANCELED',
                     finished_at = COALESCE(finished_at, now()),
                     status_reason = 'workflow.dependency_unsatisfied',
                     last_error_code = 'workflow.dependency_unsatisfied',
                     last_error_message = 'Step dependency requirements were not satisfied.',
                     updated_at = now()
                 WHERE id = $1
                   AND status = 'BLOCKED'
                 RETURNING workflow_run_id",
                candidate.id,
            )
            .fetch_optional(&mut **tx)
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context("cancel blocked workflow step", error)
            })?;

            if let Some(canceled_row) = canceled_row {
                let workflow_run_id: Uuid = canceled_row.workflow_run_id;
                touched_run_ids.insert(workflow_run_id);
                terminal_queue.push_back((candidate.id, WorkflowStepStatus::Canceled));
            }
        }
    }

    Ok(())
}

pub(crate) async fn release_candidate_step_tx(
    tx: &mut DbTx<'_>,
    candidate: &StepReleaseCandidate,
) -> Result<()> {
    // Callers reach step release with the candidate workflow-step rows locked
    // FOR UPDATE. append_workflow_steps_tx also holds the workflow-run row lock
    // before inserting and releasing appended steps. If cancel already owns the
    // exclusive advisory lock, append/external callers must roll back rather
    // than committing consumed dependency counters without a matching
    // release/cancel sweep. Job terminal completion waits on the blocking
    // shared lock before it gets here, so this try-lock succeeds reentrantly on
    // that connection.
    if !try_lock_workflow_run_release_shared_tx(tx, candidate.workflow_run_id).await? {
        return Err(workflow_release_conflict_error(candidate.workflow_run_id));
    }

    if !workflow_run_allows_step_release_tx(tx, candidate.workflow_run_id).await? {
        // This also covers reentrant calls from cancellation: PostgreSQL
        // advisory locks are reentrant for a backend, then the canceled run
        // status rejects release here.
        return Ok(());
    }

    match candidate.execution_kind {
        WorkflowStepExecutionKind::Job => {
            let (job_type, priority, max_attempts, timeout_seconds, stage) =
                job_release_fields(candidate)?;
            let row = sqlx::query!(
                "INSERT INTO job_queue (
                    job_type,
                    organization_id,
                    payload,
                    priority,
                    max_attempts,
                    timeout_seconds,
                    next_run_at,
                    workflow_step_id,
                    stage
                 )
                 VALUES ($1, $2, $3::jsonb, $4, $5, $6, now(), $7, $8)
                 RETURNING id, run_number",
                job_type.as_str(),
                candidate.organization_id,
                &candidate.payload,
                priority,
                max_attempts,
                timeout_seconds,
                candidate.id,
                stage.as_db_value(),
            )
            .fetch_one(&mut **tx)
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context("enqueue released workflow step job", error)
            })?;

            let job_id: Uuid = row.id;
            let run_number: i32 = row.run_number;

            let updated = sqlx::query!(
                "UPDATE workflow_steps
                 SET status = 'ENQUEUED',
                     job_id = $2,
                     released_at = COALESCE(released_at, now()),
                     status_reason = NULL,
                     last_error_code = NULL,
                     last_error_message = NULL,
                     updated_at = now()
                 WHERE id = $1
                   AND status = 'BLOCKED'
                   AND job_id IS NULL
                   AND dependency_count_pending = 0
                   AND dependency_count_unsatisfied = 0",
                candidate.id,
                job_id,
            )
            .execute(&mut **tx)
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context(
                    "mark released workflow step as enqueued",
                    error,
                )
            })?
            .rows_affected();
            if updated != 1 {
                return Err(workflow_internal_state_error(
                    "workflow step release preconditions were not met",
                ));
            }

            sqlx::query!(
                "INSERT INTO job_events (
                    job_id,
                    run_number,
                    event_type,
                    stage,
                    payload
                 )
                 VALUES ($1, $2, 'ENQUEUED', $3, jsonb_build_object('job_type', $4::text))",
                job_id,
                run_number,
                stage.as_db_value(),
                job_type.as_str(),
            )
            .execute(&mut **tx)
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context(
                    "insert released workflow step enqueue event",
                    error,
                )
            })?;

            Ok(())
        }
        WorkflowStepExecutionKind::External => {
            let updated = sqlx::query!(
                "UPDATE workflow_steps
                 SET status = 'WAITING_FOR_EXTERNAL',
                     job_id = NULL,
                     released_at = COALESCE(released_at, now()),
                     started_at = NULL,
                     finished_at = NULL,
                     status_reason = NULL,
                     last_error_code = NULL,
                     last_error_message = NULL,
                     updated_at = now()
                 WHERE id = $1
                   AND status = 'BLOCKED'
                   AND job_id IS NULL
                   AND dependency_count_pending = 0
                   AND dependency_count_unsatisfied = 0",
                candidate.id,
            )
            .execute(&mut **tx)
            .await
            .map_err(|error| {
                Error::from_query_sqlx_with_context(
                    "mark released workflow step as waiting for external completion",
                    error,
                )
            })?
            .rows_affected();
            if updated != 1 {
                return Err(workflow_internal_state_error(
                    "workflow step release preconditions were not met",
                ));
            }

            Ok(())
        }
    }
}

async fn workflow_run_allows_step_release_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
) -> Result<bool> {
    // The shared row lock makes the releasable-status check stable until this
    // transaction either releases the step or exits. Release callers already
    // hold workflow-step row locks before taking this workflow-run row lock;
    // cancel takes the release advisory lock before it takes workflow-step row
    // locks, and release never blocks on that advisory lock.
    sqlx::query_scalar!(
        "SELECT status IN (
            'RUNNING'::workflow_run_status,
            'WAITING_FOR_EXTERNAL'::workflow_run_status
         ) AS \"allows_release!\"
         FROM workflow_runs
         WHERE id = $1
         FOR SHARE",
        workflow_run_id,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("check workflow run allows step release", error)
    })
}

pub(crate) async fn recompute_workflow_run_statuses_tx(
    tx: &mut DbTx<'_>,
    touched_run_ids: &BTreeSet<Uuid>,
) -> Result<()> {
    for workflow_run_id in touched_run_ids {
        // Serialize status recomputation per run so concurrent step completions do not
        // leave the run stuck in RUNNING due to snapshot races.
        sqlx::query!(
            "SELECT id FROM workflow_runs WHERE id = $1 FOR UPDATE",
            *workflow_run_id
        )
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("lock workflow run for status recompute", error)
        })?;

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

        sqlx::query!(
            "UPDATE workflow_runs
             SET status = $2::text::workflow_run_status,
                 finished_at = CASE
                    WHEN $2::text::workflow_run_status IN ('RUNNING', 'WAITING_FOR_EXTERNAL')
                        THEN NULL
                    ELSE COALESCE(finished_at, now())
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
