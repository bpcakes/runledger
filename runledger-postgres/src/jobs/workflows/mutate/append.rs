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
use crate::jobs::transaction_isolation::{ReadCommittedTx, ensure_read_committed_tx};
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
use super::super::runtime::{recompute_workflow_run_status_tx, resolve_terminal_step_queue_tx};
use super::super::snapshot::{CanonicalAppendRequest, canonical_append_request};
use super::super::steps::{
    WorkflowStepDependencyWriteContext, WorkflowStepIdsByKey, dependency_count_total,
    fetch_job_definition_defaults_tx, insert_workflow_step_dependencies_tx,
    insert_workflow_step_record_tx, workflow_step_effective_organization_id,
};
use super::super::validation::workflow_dag_validation_error;
use super::idempotency::{
    insert_workflow_mutation_row_tx, load_existing_mutation_request_tx,
    stored_append_request_matches_tx,
};

#[derive(Debug)]
struct ImmediatelyReadyAppendedStepCandidate {
    candidate: StepReleaseCandidate,
    dependency_count_unsatisfied: i32,
}

#[derive(Debug)]
struct InsertedAppend {
    step_id_by_key: WorkflowStepIdsByKey,
    appended_step_ids: Vec<Uuid>,
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
    let mut read_committed_tx = ensure_read_committed_tx(
        tx,
        "workflow append mutation",
        "workflow.append_unsupported_isolation",
        "Workflow append mutation requires READ COMMITTED transaction isolation.",
    )
    .await?;

    append_workflow_steps_read_committed_tx(&mut read_committed_tx, input).await
}

async fn append_workflow_steps_read_committed_tx(
    tx: &mut ReadCommittedTx<'_, '_>,
    input: &AppendWorkflowStepsInputRecord<'_>,
) -> Result<AppendResult> {
    let tx = tx.as_tx();

    let locked_steps =
        lock_workflow_steps_for_update_tx(tx, input.workflow_run_id, input.organization_id).await?;
    let workflow_run =
        lock_workflow_run_for_update_tx(tx, input.workflow_run_id, input.organization_id).await?;
    let canonical_request = canonical_append_request(
        input.append_window_step_key,
        workflow_run.organization_id,
        &input.steps,
    );

    if let Some(existing_request) =
        load_existing_mutation_request_tx(tx, workflow_run.id, input.mutation_key).await?
    {
        if !stored_append_request_matches_tx(
            tx,
            &existing_request,
            workflow_run.organization_id,
            &canonical_request,
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

    let inserted_append = insert_appended_step_records_tx(
        tx,
        workflow_run.id,
        workflow_run.organization_id,
        &locked_steps,
        &input.steps,
    )
    .await?;

    persist_appended_step_dependencies_and_mutation_tx(
        tx,
        workflow_run.id,
        input,
        &canonical_request,
        &inserted_append.step_id_by_key,
    )
    .await?;

    resolve_appended_steps_tx(tx, workflow_run.id, &inserted_append.appended_step_ids).await?;

    load_append_result_tx(
        tx,
        workflow_run.id,
        &input.steps,
        workflow_run.organization_id,
        AppendOutcome::Appended,
    )
    .await
}

async fn insert_appended_step_records_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    workflow_organization_id: Option<Uuid>,
    locked_steps: &[LockedWorkflowStepState],
    steps: &[WorkflowStepEnqueue<'_>],
) -> Result<InsertedAppend> {
    let defaults_by_job_type = fetch_job_definition_defaults_tx(tx, steps).await?;
    let existing_statuses_by_key = locked_steps
        .iter()
        .map(|step| (step.step_key.as_str().to_owned(), step.status))
        .collect::<BTreeMap<_, _>>();
    let new_step_keys = steps
        .iter()
        .map(|step| step.step_key().as_str().to_owned())
        .collect::<BTreeSet<_>>();

    let mut step_id_by_key = locked_steps
        .iter()
        .map(|step| (step.step_key.as_str().to_owned(), step.id))
        .collect::<WorkflowStepIdsByKey>();
    let mut appended_step_ids = Vec::with_capacity(steps.len());

    for step in steps {
        let (dependency_count_pending, dependency_count_unsatisfied) =
            initial_dependency_counters(&existing_statuses_by_key, &new_step_keys, step)?;
        let step_id = insert_workflow_step_record_tx(
            tx,
            workflow_run_id,
            workflow_step_effective_organization_id(workflow_organization_id, step),
            step,
            &defaults_by_job_type,
            dependency_count_pending,
            dependency_count_unsatisfied,
        )
        .await?;
        step_id_by_key.insert(step.step_key().as_str().to_owned(), step_id);
        appended_step_ids.push(step_id);
    }

    Ok(InsertedAppend {
        step_id_by_key,
        appended_step_ids,
    })
}

async fn persist_appended_step_dependencies_and_mutation_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    input: &AppendWorkflowStepsInputRecord<'_>,
    canonical_request: &CanonicalAppendRequest,
    step_id_by_key: &WorkflowStepIdsByKey,
) -> Result<()> {
    insert_workflow_step_dependencies_tx(
        tx,
        &input.steps,
        workflow_run_id,
        step_id_by_key,
        WorkflowStepDependencyWriteContext::Append,
    )
    .await?;
    insert_workflow_mutation_row_tx(
        tx,
        workflow_run_id,
        input.mutation_key,
        input.mutation_metadata,
        canonical_request,
    )
    .await?;

    Ok(())
}

async fn resolve_appended_steps_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    appended_step_ids: &[Uuid],
) -> Result<()> {
    // The IDs are collected from successful INSERT ... RETURNING calls in this
    // transaction, so the former missing-state diagnostic was unreachable. The
    // ready-candidate query intentionally returns only a subset: omitted rows
    // are pending and require neither decoding nor release work.
    let ready_candidates =
        load_immediately_ready_appended_step_candidates_tx(tx, workflow_run_id, appended_step_ids)
            .await?;
    for ready_candidate in &ready_candidates {
        resolve_immediately_ready_appended_step_candidate_tx(tx, workflow_run_id, ready_candidate)
            .await?;
    }

    recompute_workflow_run_status_tx(tx, workflow_run_id).await?;
    Ok(())
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
        let release_mode = dependency.effective_release_mode();
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

async fn resolve_immediately_ready_appended_step_candidate_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    ready_candidate: &ImmediatelyReadyAppendedStepCandidate,
) -> Result<()> {
    if ready_candidate.dependency_count_unsatisfied == 0 {
        return release_candidate_step_tx(tx, &ready_candidate.candidate).await;
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
               AND workflow_run_id = $2
               AND status = 'BLOCKED'
             RETURNING workflow_run_id",
        ready_candidate.candidate.id(),
        workflow_run_id,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("cancel born-unsatisfied appended workflow step", error)
    })?;

    if canceled.is_some() {
        resolve_terminal_step_queue_tx(
            tx,
            workflow_run_id,
            ready_candidate.candidate.id(),
            WorkflowStepStatus::Canceled,
        )
        .await?;
    }

    Ok(())
}

async fn load_immediately_ready_appended_step_candidates_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    appended_step_ids: &[Uuid],
) -> Result<Vec<ImmediatelyReadyAppendedStepCandidate>> {
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
            dependency_count_unsatisfied
         FROM workflow_steps
         WHERE workflow_run_id = $1
           AND id = ANY($2::uuid[])
           AND dependency_count_pending = 0
         ORDER BY array_position($2::uuid[], id)
         FOR UPDATE",
        workflow_run_id,
        appended_step_ids,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context(
            "load immediately ready appended workflow step candidates",
            error,
        )
    })?;

    rows.into_iter()
        .map(|row| {
            let execution_kind = parse_workflow_step_execution_kind(row.execution_kind)?;
            let job_type = row.job_type.map(parse_job_type_name).transpose()?;
            let stage = row.stage.map(parse_job_stage).transpose()?;
            Ok(ImmediatelyReadyAppendedStepCandidate {
                candidate: StepReleaseCandidate::from_decoded_fields(StepReleaseCandidateInit {
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
                dependency_count_unsatisfied: row.dependency_count_unsatisfied,
            })
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
    use serde_json::Value as JsonValue;
    use sqlx::types::Uuid;

    use crate::{DbPool, DbTx};

    use super::{load_immediately_ready_appended_step_candidates_tx, resolve_appended_steps_tx};

    async fn record_postgres_18_server_version(pool: &DbPool) {
        let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
            .fetch_one(pool)
            .await
            .expect("read PostgreSQL server_version");
        let server_version_num =
            sqlx::query_scalar::<_, i32>("SELECT current_setting('server_version_num')::int")
                .fetch_one(pool)
                .await
                .expect("read PostgreSQL server_version_num");
        eprintln!(
            "append ready-candidate regression PostgreSQL server_version={server_version}, \
             server_version_num={server_version_num}"
        );
        assert_eq!(
            server_version_num / 10_000,
            18,
            "append ready-candidate regression must run on PostgreSQL 18"
        );
    }

    async fn insert_running_workflow_run(pool: &DbPool, workflow_type: &str) -> Uuid {
        sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO workflow_runs (workflow_type)
             VALUES ($1)
             RETURNING id",
        )
        .bind(workflow_type)
        .fetch_one(pool)
        .await
        .expect("insert workflow run")
    }

    async fn insert_external_blocked_step(
        tx: &mut DbTx<'_>,
        workflow_run_id: Uuid,
        step_key: &str,
        dependency_count_total: i32,
        dependency_count_pending: i32,
        dependency_count_unsatisfied: i32,
    ) -> Uuid {
        sqlx::query_scalar::<_, Uuid>(
            "INSERT INTO workflow_steps (
                workflow_run_id,
                step_key,
                execution_kind,
                status,
                dependency_count_total,
                dependency_count_pending,
                dependency_count_unsatisfied
             )
             VALUES ($1, $2, 'EXTERNAL', 'BLOCKED', $3, $4, $5)
             RETURNING id",
        )
        .bind(workflow_run_id)
        .bind(step_key)
        .bind(dependency_count_total)
        .bind(dependency_count_pending)
        .bind(dependency_count_unsatisfied)
        .fetch_one(&mut **tx)
        .await
        .expect("insert external blocked workflow step")
    }

    async fn seed_ready_candidate_plan_noise(pool: &DbPool) {
        let noise_run_id =
            insert_running_workflow_run(pool, "workflow.test.append_ready_candidate_plan_noise")
                .await;
        sqlx::query(
            "INSERT INTO workflow_steps (workflow_run_id, step_key, execution_kind)
             SELECT $1, format('append-ready-candidate-plan-noise-%s', ordinal), 'EXTERNAL'
             FROM generate_series(1, 512) AS noise(ordinal)",
        )
        .bind(noise_run_id)
        .execute(pool)
        .await
        .expect("seed ready-candidate plan noise");
        sqlx::query("ANALYZE workflow_steps")
            .execute(pool)
            .await
            .expect("analyze workflow steps for ready-candidate plan");
    }

    fn plan_node_types(plan: &JsonValue) -> Vec<String> {
        fn visit(node: &JsonValue, node_types: &mut Vec<String>) {
            if let Some(node_type) = node["Node Type"].as_str() {
                node_types.push(node_type.to_owned());
            }
            if let Some(children) = node["Plans"].as_array() {
                for child in children {
                    visit(child, node_types);
                }
            }
        }

        let mut node_types = Vec::new();
        visit(&plan[0]["Plan"], &mut node_types);
        node_types
    }

    #[tokio::test]
    async fn ready_appended_candidates_filter_pending_preserve_input_order_and_skip_missing_rows() {
        let (pool, database) = setup_ephemeral_pool("workflow_append_ready_candidates", 4).await;
        record_postgres_18_server_version(&pool).await;
        seed_ready_candidate_plan_noise(&pool).await;

        let workflow_run_id =
            insert_running_workflow_run(&pool, "workflow.test.append_ready_candidates").await;
        let mut tx = pool.begin().await.expect("begin ready-candidate test tx");
        let ready_first =
            insert_external_blocked_step(&mut tx, workflow_run_id, "ready-first", 0, 0, 0).await;
        let pending =
            insert_external_blocked_step(&mut tx, workflow_run_id, "pending", 1, 1, 0).await;
        let born_unsatisfied =
            insert_external_blocked_step(&mut tx, workflow_run_id, "born-unsatisfied", 1, 0, 1)
                .await;
        let ready_second =
            insert_external_blocked_step(&mut tx, workflow_run_id, "ready-second", 0, 0, 0).await;
        let missing = Uuid::now_v7();
        let appended_step_ids = vec![
            ready_second,
            missing,
            pending,
            born_unsatisfied,
            ready_first,
        ];

        let ready_candidates = load_immediately_ready_appended_step_candidates_tx(
            &mut tx,
            workflow_run_id,
            &appended_step_ids,
        )
        .await
        .expect("load immediately ready appended candidates");
        assert_eq!(
            ready_candidates
                .iter()
                .map(|candidate| candidate.candidate.id())
                .collect::<Vec<_>>(),
            vec![ready_second, born_unsatisfied, ready_first],
            "the ready-only query must retain the append-input order while omitting pending and absent rows"
        );

        let plan = sqlx::query_scalar::<_, JsonValue>(
            "EXPLAIN (ANALYZE, FORMAT JSON)
             SELECT
                id,
                workflow_run_id,
                execution_kind::text,
                job_type,
                organization_id,
                payload,
                priority,
                max_attempts,
                timeout_seconds,
                stage,
                execution_resource_key,
                dependency_count_unsatisfied
             FROM workflow_steps
             WHERE workflow_run_id = $1
               AND id = ANY($2::uuid[])
               AND dependency_count_pending = 0
             ORDER BY array_position($2::uuid[], id)
             FOR UPDATE",
        )
        .bind(workflow_run_id)
        .bind(&appended_step_ids)
        .fetch_one(&mut *tx)
        .await
        .expect("explain ready appended-candidate query");
        let node_types = plan_node_types(&plan);
        eprintln!(
            "append ready-candidate EXPLAIN ANALYZE node_types={node_types:?}, planning_time_ms={}, execution_time_ms={}",
            plan[0]["Planning Time"], plan[0]["Execution Time"]
        );
        assert_eq!(
            plan[0]["Plan"]["Actual Rows"].as_f64(),
            Some(3.0),
            "only immediately ready existing rows should reach the candidate plan"
        );
        assert!(
            node_types.iter().any(|node_type| node_type == "LockRows"),
            "ready candidates remain locked before release: {plan}"
        );
        assert!(
            node_types.iter().any(|node_type| node_type == "Sort"),
            "ready candidates must sort by append-input order: {plan}"
        );
        assert!(
            node_types
                .iter()
                .any(|node_type| matches!(node_type.as_str(), "Index Scan" | "Bitmap Heap Scan")),
            "the ready-candidate lookup should remain ID-index driven: {plan}"
        );
        assert!(
            !node_types.iter().any(|node_type| node_type == "Seq Scan"),
            "the ready-candidate lookup must not scan unrelated workflow steps: {plan}"
        );

        // The normal append path obtains these IDs from successful inserts, so
        // it cannot include `missing`. Passing it here proves the deliberate
        // replacement for the old unreachable diagnostic: a non-candidate is
        // simply omitted, just like a pending appended row.
        resolve_appended_steps_tx(&mut tx, workflow_run_id, &appended_step_ids)
            .await
            .expect("resolve ready appended candidates while omitting missing row");

        let step_states = sqlx::query_as::<_, (Uuid, String, i32, i32, Option<String>)>(
            "SELECT
                id,
                status::text,
                dependency_count_pending,
                dependency_count_unsatisfied,
                last_error_code
             FROM workflow_steps
             WHERE workflow_run_id = $1",
        )
        .bind(workflow_run_id)
        .fetch_all(&mut *tx)
        .await
        .expect("load appended candidate states")
        .into_iter()
        .map(|(id, status, pending, unsatisfied, last_error_code)| {
            (id, (status, pending, unsatisfied, last_error_code))
        })
        .collect::<BTreeMap<_, _>>();
        assert_eq!(
            step_states.get(&ready_second),
            Some(&("WAITING_FOR_EXTERNAL".to_owned(), 0, 0, None))
        );
        assert_eq!(
            step_states.get(&pending),
            Some(&("BLOCKED".to_owned(), 1, 0, None))
        );
        assert_eq!(
            step_states.get(&born_unsatisfied),
            Some(&(
                "CANCELED".to_owned(),
                0,
                1,
                Some("workflow.dependency_unsatisfied".to_owned()),
            ))
        );
        assert_eq!(
            step_states.get(&ready_first),
            Some(&("WAITING_FOR_EXTERNAL".to_owned(), 0, 0, None))
        );

        tx.rollback()
            .await
            .expect("roll back ready-candidate test tx");
        teardown_ephemeral_pool(pool, database).await;
    }
}
