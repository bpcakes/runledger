use std::collections::{BTreeMap, BTreeSet};

use runledger_core::jobs::{
    StepKey, WorkflowDependencyReleaseMode, WorkflowRunStatus, WorkflowStepEnqueue,
    WorkflowStepExecutionKind, WorkflowStepStatus, validate_workflow_step_append,
};
use sqlx::types::Uuid;

use crate::jobs::row_decode::{
    parse_job_stage, parse_job_type_name, parse_workflow_step_execution_kind,
};
use crate::jobs::rows::WorkflowStepRow;
use crate::jobs::transaction_isolation::ensure_read_committed_tx;
use crate::jobs::workflow_types::{
    AppendWorkflowStepsInput as AppendWorkflowStepsInputRecord,
    AppendWorkflowStepsOutcome as AppendOutcome, AppendWorkflowStepsResult as AppendResult,
    WorkflowStepDbRecord,
};
use crate::{DbPool, DbTx, Error, Result};

use super::super::errors::{
    workflow_append_blank_mutation_key_error, workflow_append_conflicting_retry_error,
    workflow_append_terminal_run_error, workflow_append_window_missing_error,
    workflow_append_window_not_external_error, workflow_append_window_not_open_error,
    workflow_internal_state_error, workflow_release_conflict_error,
};
use super::super::locking::{
    LockedWorkflowStepState, lock_workflow_run_for_update_tx, lock_workflow_steps_for_update_tx,
    try_lock_workflow_run_release_shared_tx,
};
use super::super::read::load_workflow_run_by_id_tx;
use super::super::release::{
    StepReleaseCandidate, StepReleaseCandidateInit, release_candidate_step_tx,
};
use super::super::runtime::{recompute_workflow_run_statuses_tx, resolve_terminal_step_queue_tx};
use super::super::snapshot::{canonical_append_request, deserialize_stored_append_request};
use super::super::steps::{
    WorkflowStepIdsByKey, dependency_count_total, fetch_job_definition_defaults_tx,
    insert_workflow_step_dependency_record_tx, insert_workflow_step_record_tx, step_id_for_key,
    workflow_step_defaults, workflow_step_effective_organization_id,
};
use super::super::validation::workflow_dag_validation_error;
use super::idempotency::{
    insert_workflow_mutation_row_tx, load_existing_mutation_request_tx,
    stored_append_request_matches_tx,
};

#[derive(Debug)]
struct AppendedStepState {
    candidate: StepReleaseCandidate,
    dependency_count_pending: i32,
    dependency_count_unsatisfied: i32,
}

pub async fn append_workflow_steps(
    pool: &DbPool,
    input: &AppendWorkflowStepsInputRecord<'_>,
) -> Result<AppendResult> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    let result = append_workflow_steps_tx(&mut tx, input).await?;
    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    Ok(result)
}

pub async fn append_workflow_steps_tx(
    tx: &mut DbTx<'_>,
    input: &AppendWorkflowStepsInputRecord<'_>,
) -> Result<AppendResult> {
    if input.mutation_key.trim().is_empty() {
        return Err(workflow_append_blank_mutation_key_error());
    }
    ensure_read_committed_tx(
        tx,
        "workflow append mutation",
        "workflow.append_unsupported_isolation",
        "Workflow append mutation requires READ COMMITTED transaction isolation.",
    )
    .await?;

    let locked_steps =
        lock_workflow_steps_for_update_tx(tx, input.workflow_run_id, input.organization_id).await?;
    let workflow_run =
        lock_workflow_run_for_update_tx(tx, input.workflow_run_id, input.organization_id).await?;
    let canonical_request = canonical_append_request(
        input.append_window_step_key,
        workflow_run.organization_id,
        &input.steps,
    )?;
    let comparable_request =
        deserialize_stored_append_request(&canonical_request, workflow_run.organization_id)?;

    if let Some(existing_request) =
        load_existing_mutation_request_tx(tx, workflow_run.id, input.mutation_key).await?
    {
        if !stored_append_request_matches_tx(
            tx,
            &existing_request,
            workflow_run.organization_id,
            &comparable_request,
        )
        .await?
        {
            return Err(workflow_append_conflicting_retry_error(input.mutation_key));
        }

        return load_append_result_tx(
            tx,
            workflow_run.id,
            &input.steps,
            workflow_run.organization_id,
            AppendOutcome::AlreadyApplied,
        )
        .await;
    }

    ensure_append_window_is_open(&locked_steps, input.append_window_step_key)?;

    if matches!(
        workflow_run.status,
        WorkflowRunStatus::Succeeded
            | WorkflowRunStatus::CompletedWithErrors
            | WorkflowRunStatus::Canceled
    ) {
        return Err(workflow_append_terminal_run_error(workflow_run.status));
    }

    let existing_step_keys = locked_steps
        .iter()
        .map(|step| step.step_key.clone())
        .collect::<BTreeSet<_>>();
    validate_workflow_step_append(&existing_step_keys, &input.steps)
        .map_err(workflow_dag_validation_error)?;

    if !try_lock_workflow_run_release_shared_tx(tx, workflow_run.id).await? {
        return Err(workflow_release_conflict_error(workflow_run.id));
    }

    let defaults_by_job_type = fetch_job_definition_defaults_tx(tx, &input.steps).await?;
    let existing_statuses_by_key = locked_steps
        .iter()
        .map(|step| (step.step_key.as_str().to_owned(), step.status))
        .collect::<BTreeMap<_, _>>();
    let new_step_keys = input
        .steps
        .iter()
        .map(|step| step.step_key().as_str().to_owned())
        .collect::<BTreeSet<_>>();

    let mut step_id_by_key = locked_steps
        .iter()
        .map(|step| (step.step_key.as_str().to_owned(), step.id))
        .collect::<WorkflowStepIdsByKey>();
    let mut appended_step_ids = Vec::with_capacity(input.steps.len());

    for step in &input.steps {
        let defaults = match step.execution_kind() {
            WorkflowStepExecutionKind::Job => {
                Some(workflow_step_defaults(&defaults_by_job_type, step)?)
            }
            WorkflowStepExecutionKind::External => None,
        };
        let (dependency_count_pending, dependency_count_unsatisfied) =
            initial_dependency_counters(&existing_statuses_by_key, &new_step_keys, step)?;
        let step_id = insert_workflow_step_record_tx(
            tx,
            workflow_run.id,
            workflow_step_effective_organization_id(workflow_run.organization_id, step),
            step,
            defaults,
            dependency_count_pending,
            dependency_count_unsatisfied,
        )
        .await?;
        step_id_by_key.insert(step.step_key().as_str().to_owned(), step_id);
        appended_step_ids.push(step_id);
    }

    insert_appended_workflow_step_dependencies_tx(
        tx,
        workflow_run.id,
        &input.steps,
        &step_id_by_key,
    )
    .await?;
    insert_workflow_mutation_row_tx(
        tx,
        workflow_run.id,
        input.mutation_key,
        input.mutation_metadata,
        &canonical_request,
    )
    .await?;

    let appended_step_states = load_appended_step_states_tx(tx, &appended_step_ids).await?;
    let mut touched_run_ids = BTreeSet::from([workflow_run.id]);
    for step_id in &appended_step_ids {
        let Some(step_state) = appended_step_states.get(step_id) else {
            return Err(workflow_internal_state_error(format!(
                "missing appended workflow step state for step id {step_id}"
            )));
        };
        if step_state.dependency_count_pending != 0 {
            continue;
        }

        resolve_appended_step_state_tx(tx, step_state, &mut touched_run_ids).await?;
    }

    recompute_workflow_run_statuses_tx(tx, &touched_run_ids).await?;

    load_append_result_tx(
        tx,
        workflow_run.id,
        &input.steps,
        workflow_run.organization_id,
        AppendOutcome::Appended,
    )
    .await
}

fn ensure_append_window_is_open(
    locked_steps: &[LockedWorkflowStepState],
    append_window_step_key: StepKey<'_>,
) -> Result<()> {
    let Some(append_window) = locked_steps
        .iter()
        .find(|step| step.step_key.as_str() == append_window_step_key.as_str())
    else {
        return Err(workflow_append_window_missing_error(
            append_window_step_key.as_str(),
        ));
    };

    if append_window.execution_kind != WorkflowStepExecutionKind::External {
        return Err(workflow_append_window_not_external_error(
            append_window.step_key.as_str(),
        ));
    }

    if append_window.status != WorkflowStepStatus::WaitingForExternal {
        return Err(workflow_append_window_not_open_error(
            append_window.step_key.as_str(),
            append_window.status,
        ));
    }

    Ok(())
}

fn initial_dependency_counters(
    existing_statuses_by_key: &BTreeMap<String, WorkflowStepStatus>,
    new_step_keys: &BTreeSet<String>,
    step: &WorkflowStepEnqueue<'_>,
) -> Result<(i32, i32)> {
    let _ = dependency_count_total(step)?;
    let mut dependency_count_pending = 0i32;
    let mut dependency_count_unsatisfied = 0i32;

    for dependency in step.dependencies() {
        let release_mode = dependency
            .release_mode
            .unwrap_or(WorkflowDependencyReleaseMode::OnTerminal);
        if let Some(status) =
            existing_statuses_by_key.get(dependency.prerequisite_step_key.as_str())
        {
            if !status.is_terminal() {
                dependency_count_pending += 1;
            } else if matches!(release_mode, WorkflowDependencyReleaseMode::OnSuccess)
                && *status != WorkflowStepStatus::Succeeded
            {
                dependency_count_unsatisfied += 1;
            }
            continue;
        }

        if new_step_keys.contains(dependency.prerequisite_step_key.as_str()) {
            dependency_count_pending += 1;
            continue;
        }

        return Err(workflow_internal_state_error(format!(
            "append dependency '{}' for step '{}' was not present in the existing or new step key set",
            dependency.prerequisite_step_key.as_str(),
            step.step_key().as_str()
        )));
    }

    Ok((dependency_count_pending, dependency_count_unsatisfied))
}

async fn resolve_appended_step_state_tx(
    tx: &mut DbTx<'_>,
    step_state: &AppendedStepState,
    touched_run_ids: &mut BTreeSet<Uuid>,
) -> Result<()> {
    if step_state.dependency_count_unsatisfied == 0 {
        return release_candidate_step_tx(tx, &step_state.candidate).await;
    }

    let canceled = sqlx::query!(
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
        step_state.candidate.id(),
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("cancel born-unsatisfied appended workflow step", error)
    })?;

    if canceled.is_some() {
        resolve_terminal_step_queue_tx(
            tx,
            step_state.candidate.id(),
            WorkflowStepStatus::Canceled,
            touched_run_ids,
        )
        .await?;
    }

    Ok(())
}

async fn insert_appended_workflow_step_dependencies_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    steps: &[WorkflowStepEnqueue<'_>],
    step_id_by_key: &WorkflowStepIdsByKey,
) -> Result<()> {
    for step in steps {
        let dependent_step_id = step_id_for_key(
            step_id_by_key,
            step.step_key().as_str(),
            "missing dependent appended workflow step id",
        )?;
        for dependency in step.dependencies() {
            let prerequisite_step_id = step_id_for_key(
                step_id_by_key,
                dependency.prerequisite_step_key.as_str(),
                "missing appended workflow prerequisite step id",
            )?;
            let release_mode = dependency
                .release_mode
                .unwrap_or(WorkflowDependencyReleaseMode::OnTerminal)
                .as_db_value();
            insert_workflow_step_dependency_record_tx(
                tx,
                workflow_run_id,
                prerequisite_step_id,
                dependent_step_id,
                release_mode,
            )
            .await?;
        }
    }

    Ok(())
}

async fn load_appended_step_states_tx(
    tx: &mut DbTx<'_>,
    appended_step_ids: &[Uuid],
) -> Result<BTreeMap<Uuid, AppendedStepState>> {
    let rows = sqlx::query!(
        "SELECT
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
            dependency_count_pending,
            dependency_count_unsatisfied
         FROM workflow_steps
         WHERE id = ANY($1::uuid[])
         FOR UPDATE",
        appended_step_ids,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("load appended workflow step states", error)
    })?;

    rows.into_iter()
        .map(|row| {
            let execution_kind = parse_workflow_step_execution_kind(row.execution_kind)?;
            let job_type = row.job_type.map(parse_job_type_name).transpose()?;
            let stage = row.stage.map(parse_job_stage).transpose()?;
            Ok((
                row.id,
                AppendedStepState {
                    candidate: StepReleaseCandidate::from_init(StepReleaseCandidateInit {
                        id: row.id,
                        workflow_run_id: row.workflow_run_id,
                        execution_kind,
                        job_type,
                        organization_id: row.organization_id,
                        payload: row.payload,
                        priority: row.priority,
                        max_attempts: row.max_attempts,
                        timeout_seconds: row.timeout_seconds,
                        stage,
                        execution_resource_key: row.execution_resource_key,
                    }),
                    dependency_count_pending: row.dependency_count_pending,
                    dependency_count_unsatisfied: row.dependency_count_unsatisfied,
                },
            ))
        })
        .collect()
}

async fn load_workflow_steps_by_keys_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    input_steps: &[WorkflowStepEnqueue<'_>],
    organization_id: Option<Uuid>,
) -> Result<Vec<WorkflowStepDbRecord>> {
    let step_keys = input_steps
        .iter()
        .map(|step| step.step_key().as_str().to_owned())
        .collect::<Vec<_>>();
    let rows = sqlx::query_as!(
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
           AND ws.step_key = ANY($2::text[])
           AND ($3::uuid IS NULL OR wr.organization_id = $3)",
        workflow_run_id,
        &step_keys,
        organization_id,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("load appended workflow steps by key", error)
    })?;

    let steps_by_key = rows
        .into_iter()
        .map(|row| {
            let record = row.into_record()?;
            Ok((record.step_key.clone(), record))
        })
        .collect::<Result<BTreeMap<_, _>>>()?;

    input_steps
        .iter()
        .map(|step| {
            steps_by_key
                .get(step.step_key().as_str())
                .cloned()
                .ok_or_else(|| {
                    workflow_internal_state_error(format!(
                        "workflow append result missing step '{}'",
                        step.step_key().as_str()
                    ))
                })
        })
        .collect()
}

async fn load_append_result_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    input_steps: &[WorkflowStepEnqueue<'_>],
    organization_id: Option<Uuid>,
    outcome: AppendOutcome,
) -> Result<AppendResult> {
    let appended_steps =
        load_workflow_steps_by_keys_tx(tx, workflow_run_id, input_steps, organization_id).await?;

    Ok(AppendResult {
        workflow_run: load_workflow_run_by_id_tx(
            tx,
            workflow_run_id,
            "load workflow run after append",
        )
        .await?,
        appended_steps,
        outcome,
    })
}
