use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use runledger_core::jobs::{
    JobType, StepKey, WorkflowDependencyReleaseMode, WorkflowRunEnqueueBuilder,
    WorkflowStepEnqueueBuilder, WorkflowStepStatus, WorkflowType,
};
use runledger_postgres::jobs::{
    AppendWorkflowStepsInput, CompleteExternalWorkflowStepInput,
    ExternalWorkflowStepTerminalOutcome, JobDefinitionUpsert, append_workflow_steps,
    claim_jobs_for_types, complete_external_workflow_step, complete_external_workflow_step_tx,
    complete_job_success, enqueue_or_get_active_workflow, enqueue_workflow_run,
    list_workflow_step_dependencies, list_workflow_steps, upsert_job_definition_tx,
};
use runledger_postgres::{DbPool, Error, QueryErrorCategory};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::{Value, json};
use sqlx::postgres::PgListener;
use sqlx::types::Uuid;
use tokio::sync::Barrier;
use tokio::time::timeout;

const WORKFLOW_RUN_TERMINAL_CHANNEL: &str = "runledger_workflow_run_terminal";

type WorkflowRunSnapshot = (Uuid, String, Option<String>, Option<Value>);
type WorkflowStepSnapshot = (
    Uuid,
    Uuid,
    String,
    i32,
    i32,
    i32,
    Option<Uuid>,
    Option<Value>,
);
type ActiveClaimSnapshot = (String, String, Uuid, bool);

#[derive(Debug, PartialEq)]
struct DurableWorkflowSnapshot {
    runs: Vec<WorkflowRunSnapshot>,
    steps: Vec<WorkflowStepSnapshot>,
    active_claims: Vec<ActiveClaimSnapshot>,
    job_count: i64,
    event_count: i64,
}

async fn record_postgres_server_version(pool: &DbPool, diagnostic: &str) {
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
        "{diagnostic} PostgreSQL server_version={server_version}, \
         server_version_num={server_version_num}"
    );
    assert_eq!(
        server_version_num / 10_000,
        18,
        "workflow dependency regression must run on PostgreSQL 18"
    );
}

async fn register_job_definition(pool: &DbPool, job_type: JobType<'static>) {
    let mut tx = pool.begin().await.expect("begin job definition setup");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type,
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert workflow job definition");
    tx.commit().await.expect("commit job definition setup");
}

async fn durable_workflow_snapshot(pool: &DbPool) -> DurableWorkflowSnapshot {
    let runs = sqlx::query_as::<_, WorkflowRunSnapshot>(
        "SELECT id, status::text, finished_at::text, result
         FROM workflow_runs
         ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("snapshot workflow runs");
    let steps = sqlx::query_as::<_, WorkflowStepSnapshot>(
        "SELECT id, workflow_run_id, status::text, dependency_count_total,
                dependency_count_pending, dependency_count_unsatisfied, job_id, output
         FROM workflow_steps
         ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .expect("snapshot workflow steps");
    let active_claims = sqlx::query_as::<_, ActiveClaimSnapshot>(
        "SELECT scope, active_key, workflow_run_id, release_pending
         FROM workflow_active_claims
         ORDER BY scope, active_key",
    )
    .fetch_all(pool)
    .await
    .expect("snapshot workflow active claims");
    let job_count = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM job_queue")
        .fetch_one(pool)
        .await
        .expect("count workflow fixture jobs");
    let event_count = sqlx::query_scalar::<_, i64>("SELECT count(*) FROM job_events")
        .fetch_one(pool)
        .await
        .expect("count workflow fixture events");

    DurableWorkflowSnapshot {
        runs,
        steps,
        active_claims,
        job_count,
        event_count,
    }
}

async fn assert_cross_run_completion_rejected(
    pool: &DbPool,
    listener: &mut PgListener,
    source_run_id: Uuid,
    dependent_step_id: Uuid,
) {
    let before = durable_workflow_snapshot(pool).await;
    let output = json!({"must": "roll back"});
    let mut tx = pool.begin().await.expect("begin malformed completion");
    let error = complete_external_workflow_step_tx(
        &mut tx,
        &CompleteExternalWorkflowStepInput {
            workflow_run_id: source_run_id,
            organization_id: None,
            step_key: StepKey::new("source-gate"),
            outcome: ExternalWorkflowStepTerminalOutcome::Succeeded {
                output: Some(&output),
            },
            status_reason: None,
            last_error_code: None,
            last_error_message: None,
        },
    )
    .await
    .expect_err("cross-run dependency propagation must fail closed");
    match error {
        Error::QueryError(query_error) => {
            assert_eq!(query_error.category(), QueryErrorCategory::Internal);
            assert_eq!(query_error.code(), "workflow.internal_state");
        }
        other => panic!("expected internal workflow state error, got {other:?}"),
    }

    let dependent_state = sqlx::query_as::<_, (String, i32, i32, i32)>(
        "SELECT status::text, dependency_count_total,
                dependency_count_pending, dependency_count_unsatisfied
         FROM workflow_steps
         WHERE id = $1",
    )
    .bind(dependent_step_id)
    .fetch_one(&mut *tx)
    .await
    .expect("load dependent state before malformed completion rollback");
    assert_eq!(
        dependent_state,
        ("BLOCKED".to_owned(), 1, 1, 0),
        "cross-run rejection must happen before mutating the dependent"
    );

    tx.rollback()
        .await
        .expect("roll back malformed workflow completion");
    assert_eq!(
        durable_workflow_snapshot(pool).await,
        before,
        "malformed completion must roll back all durable workflow effects"
    );
    assert!(
        timeout(Duration::from_millis(100), listener.recv())
            .await
            .is_err(),
        "rolled-back malformed completion must not emit a terminal notification"
    );
}

#[tokio::test]
async fn dependency_writes_preserve_edge_orientation_and_release_modes_for_appended_steps() {
    let (pool, database) = setup_ephemeral_pool("workflow_dependency_persistence", 8).await;
    record_postgres_server_version(&pool, "workflow dependency persistence").await;

    let payload = json!({"kind": "dependency-persistence"});
    let metadata = json!({"source": "test"});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build append-window gate");
    let initial_dependent =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("initial-dependent"), &payload)
            .depends_on_terminal(&[StepKey::new("gate")])
            .try_build()
            .expect("build initial dependent");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.dependency_persistence"),
        &metadata,
    )
    .step(gate)
    .step(initial_dependent)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow");

    let appended_from_existing =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("appended-from-existing"), &payload)
            .depends_on_success(&[StepKey::new("gate")])
            .try_build()
            .expect("build step depending on existing gate");
    let appended_from_new =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("appended-from-new"), &payload)
            .depends_on_terminal(&[StepKey::new("appended-from-existing")])
            .try_build()
            .expect("build step depending on newly appended step");
    let mutation_metadata = json!({});
    append_workflow_steps(
        &pool,
        &AppendWorkflowStepsInput {
            workflow_run_id: run.id,
            organization_id: None,
            mutation_key: "append-dependency-edges",
            mutation_metadata: &mutation_metadata,
            append_window_step_key: StepKey::new("gate"),
            steps: vec![appended_from_existing, appended_from_new],
        },
    )
    .await
    .expect("append dependent workflow steps");

    let step_keys_by_id = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list workflow steps")
        .into_iter()
        .map(|step| (step.id, step.step_key.as_str().to_owned()))
        .collect::<BTreeMap<_, _>>();
    let mut persisted_edges = list_workflow_step_dependencies(&pool, None, run.id)
        .await
        .expect("list workflow step dependencies")
        .into_iter()
        .map(|dependency| {
            (
                step_keys_by_id
                    .get(&dependency.prerequisite_step_id)
                    .expect("prerequisite step id resolves")
                    .clone(),
                step_keys_by_id
                    .get(&dependency.dependent_step_id)
                    .expect("dependent step id resolves")
                    .clone(),
                dependency.release_mode,
            )
        })
        .collect::<Vec<_>>();
    persisted_edges.sort_by(|left, right| left.1.cmp(&right.1));

    assert_eq!(
        persisted_edges,
        vec![
            (
                "gate".to_owned(),
                "appended-from-existing".to_owned(),
                WorkflowDependencyReleaseMode::OnSuccess,
            ),
            (
                "appended-from-existing".to_owned(),
                "appended-from-new".to_owned(),
                WorkflowDependencyReleaseMode::OnTerminal,
            ),
            (
                "gate".to_owned(),
                "initial-dependent".to_owned(),
                WorkflowDependencyReleaseMode::OnTerminal,
            ),
        ]
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn malformed_cross_run_dependencies_fail_closed_before_dependent_mutation() {
    let (pool, database) = setup_ephemeral_pool("workflow_cross_run_rejection", 8).await;
    record_postgres_server_version(&pool, "workflow cross-run rejection").await;

    let payload = json!({"kind": "cross-run-rejection"});
    let metadata = json!({});
    let source_gate =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("source-gate"), &payload)
            .try_build()
            .expect("build source external gate");
    let source_workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.cross_run_source"),
        &metadata,
    )
    .active_key("cross-run-source")
    .result_step_key(StepKey::new("source-gate"))
    .step(source_gate)
    .try_build()
    .expect("build source workflow");
    let source_outcome = enqueue_or_get_active_workflow(&pool, &source_workflow)
        .await
        .expect("enqueue active source workflow");
    let source_run_id = source_outcome.workflow_run().id;
    let source_step_id = list_workflow_steps(&pool, None, source_run_id)
        .await
        .expect("list source steps")[0]
        .id;

    let target_gate =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("target-gate"), &payload)
            .try_build()
            .expect("build target gate");
    let target_dependent =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("target-dependent"), &payload)
            .depends_on_terminal(&[StepKey::new("target-gate")])
            .try_build()
            .expect("build target dependent");
    let target_workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.cross_run_target"),
        &metadata,
    )
    .step(target_gate)
    .step(target_dependent)
    .try_build()
    .expect("build target workflow");
    let target_run = enqueue_workflow_run(&pool, &target_workflow)
        .await
        .expect("enqueue target workflow");
    let target_steps = list_workflow_steps(&pool, None, target_run.id)
        .await
        .expect("list target steps");
    let dependent_step_id = target_steps
        .iter()
        .find(|step| step.step_key.as_str() == "target-dependent")
        .expect("target dependent exists")
        .id;

    sqlx::query(
        "ALTER TABLE workflow_step_dependencies
             DROP CONSTRAINT fk_workflow_step_dependencies_prerequisite,
             DROP CONSTRAINT fk_workflow_step_dependencies_dependent",
    )
    .execute(&pool)
    .await
    .expect("disable same-run foreign keys for malformed fixture");

    let mut listener = PgListener::connect_with(&pool)
        .await
        .expect("connect workflow terminal listener");
    listener
        .listen(WORKFLOW_RUN_TERMINAL_CHANNEL)
        .await
        .expect("listen for workflow terminal notifications");

    sqlx::query(
        "INSERT INTO workflow_step_dependencies (
            workflow_run_id, prerequisite_step_id, dependent_step_id, release_mode
         ) VALUES ($1, $2, $3, 'ON_TERMINAL')",
    )
    .bind(target_run.id)
    .bind(source_step_id)
    .bind(dependent_step_id)
    .execute(&pool)
    .await
    .expect("insert malformed prerequisite-run edge");
    assert_cross_run_completion_rejected(&pool, &mut listener, source_run_id, dependent_step_id)
        .await;

    sqlx::query(
        "DELETE FROM workflow_step_dependencies
         WHERE prerequisite_step_id = $1",
    )
    .bind(source_step_id)
    .execute(&pool)
    .await
    .expect("replace malformed prerequisite-run edge");
    sqlx::query(
        "INSERT INTO workflow_step_dependencies (
            workflow_run_id, prerequisite_step_id, dependent_step_id, release_mode
         ) VALUES ($1, $2, $3, 'ON_TERMINAL')",
    )
    .bind(source_run_id)
    .bind(source_step_id)
    .bind(dependent_step_id)
    .execute(&pool)
    .await
    .expect("insert malformed dependent-run edge");
    assert_cross_run_completion_rejected(&pool, &mut listener, source_run_id, dependent_step_id)
        .await;

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn terminal_propagation_batches_high_fan_out_and_preserves_breadth_first_cascade() {
    const FAN_OUT: usize = 64;

    let (pool, database) = setup_ephemeral_pool("workflow_dependency_high_fan_out", 8).await;
    record_postgres_server_version(&pool, "workflow dependency high fan-out").await;

    let payload = json!({"kind": "high-fan-out"});
    let metadata = json!({});
    let dependent_names = (0..FAN_OUT)
        .map(|index| format!("dependent-{index:03}"))
        .collect::<Vec<_>>();
    let dependent_keys = dependent_names
        .iter()
        .map(|name| StepKey::new(name))
        .collect::<Vec<_>>();
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build high fan-out gate");
    let mut workflow_builder = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.dependency_high_fan_out"),
        &metadata,
    )
    .step(gate);

    for dependent_key in &dependent_keys {
        let dependent = WorkflowStepEnqueueBuilder::new_external(*dependent_key, &payload)
            .depends_on_success(&[StepKey::new("gate")])
            .try_build()
            .expect("build high fan-out dependent");
        workflow_builder = workflow_builder.step(dependent);
    }

    let fan_in = WorkflowStepEnqueueBuilder::new_external(StepKey::new("fan-in"), &payload)
        .depends_on_terminal(&dependent_keys)
        .try_build()
        .expect("build high fan-in dependent");
    let workflow = workflow_builder
        .step(fan_in)
        .try_build()
        .expect("build high fan-out workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue high fan-out workflow");

    complete_external_workflow_step(
        &pool,
        &CompleteExternalWorkflowStepInput {
            workflow_run_id: run.id,
            organization_id: None,
            step_key: StepKey::new("gate"),
            outcome: ExternalWorkflowStepTerminalOutcome::Failed,
            status_reason: Some("forced high fan-out failure"),
            last_error_code: Some("workflow.test.high_fan_out_failure"),
            last_error_message: Some("forced high fan-out failure"),
        },
    )
    .await
    .expect("complete high fan-out gate");

    let steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list high fan-out workflow steps");
    assert_eq!(steps.len(), FAN_OUT + 2);

    for dependent_name in &dependent_names {
        let dependent = steps
            .iter()
            .find(|step| step.step_key.as_str() == dependent_name)
            .expect("high fan-out dependent exists");
        assert_eq!(dependent.status, WorkflowStepStatus::Canceled);
        assert_eq!(dependent.dependency_count_total, 1);
        assert_eq!(dependent.dependency_count_pending, 0);
        assert_eq!(dependent.dependency_count_unsatisfied, 1);
        assert_eq!(
            dependent.last_error_code.as_deref(),
            Some("workflow.dependency_unsatisfied")
        );
    }

    let fan_in = steps
        .iter()
        .find(|step| step.step_key.as_str() == "fan-in")
        .expect("fan-in step exists");
    assert_eq!(fan_in.status, WorkflowStepStatus::WaitingForExternal);
    assert_eq!(fan_in.dependency_count_total, FAN_OUT as i32);
    assert_eq!(fan_in.dependency_count_pending, 0);
    assert_eq!(fan_in.dependency_count_unsatisfied, 0);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_terminal_completions_update_shared_dependents_once_without_deadlock() {
    const SHARED_DEPENDENTS: usize = 16;

    let (pool, database) = setup_ephemeral_pool("workflow_dependency_concurrent_terminal", 8).await;
    record_postgres_server_version(&pool, "workflow dependency concurrent terminal").await;
    let job_type = JobType::new("jobs.test.workflow_dependency_concurrent_terminal");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"kind": "concurrent-terminal"});
    let metadata = json!({});
    let prerequisite_keys = [StepKey::new("root-a"), StepKey::new("root-b")];
    let root_a = WorkflowStepEnqueueBuilder::new(prerequisite_keys[0], job_type, &payload)
        .try_build()
        .expect("build first concurrent root");
    let root_b = WorkflowStepEnqueueBuilder::new(prerequisite_keys[1], job_type, &payload)
        .try_build()
        .expect("build second concurrent root");
    let dependent_names = (0..SHARED_DEPENDENTS)
        .map(|index| format!("shared-dependent-{index:03}"))
        .collect::<Vec<_>>();
    let mut workflow_builder = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.dependency_concurrent_terminal"),
        &metadata,
    )
    .step(root_a)
    .step(root_b);

    for dependent_name in &dependent_names {
        let dependent =
            WorkflowStepEnqueueBuilder::new_external(StepKey::new(dependent_name), &payload)
                .depends_on_success(&prerequisite_keys)
                .try_build()
                .expect("build shared concurrent dependent");
        workflow_builder = workflow_builder.step(dependent);
    }

    let workflow = workflow_builder
        .try_build()
        .expect("build concurrent terminal workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue concurrent terminal workflow");
    let claimed_jobs =
        claim_jobs_for_types(&pool, "concurrent-terminal-worker", 30, 2, &[job_type])
            .await
            .expect("claim concurrent root jobs");
    assert_eq!(claimed_jobs.len(), 2);

    let barrier = Arc::new(Barrier::new(claimed_jobs.len() + 1));
    let mut completions = Vec::with_capacity(claimed_jobs.len());
    for job in claimed_jobs {
        let pool = pool.clone();
        let barrier = Arc::clone(&barrier);
        completions.push(tokio::spawn(async move {
            let worker_id = job.worker_id.expect("claimed root has worker id");
            barrier.wait().await;
            complete_job_success(&pool, job.id, job.run_number, job.attempt, &worker_id, None).await
        }));
    }

    barrier.wait().await;
    timeout(Duration::from_secs(5), async {
        for completion in completions {
            completion
                .await
                .expect("concurrent completion task must not panic")
                .expect("concurrent root completion must succeed");
        }
    })
    .await
    .expect("concurrent terminal completions must not deadlock");

    let steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list concurrently completed workflow steps");
    for root_key in prerequisite_keys {
        let root = steps
            .iter()
            .find(|step| step.step_key.as_str() == root_key.as_str())
            .expect("concurrent root exists");
        assert_eq!(root.status, WorkflowStepStatus::Succeeded);
    }
    for dependent_name in &dependent_names {
        let dependent = steps
            .iter()
            .find(|step| step.step_key.as_str() == dependent_name)
            .expect("shared concurrent dependent exists");
        assert_eq!(dependent.status, WorkflowStepStatus::WaitingForExternal);
        assert_eq!(dependent.dependency_count_total, 2);
        assert_eq!(dependent.dependency_count_pending, 0);
        assert_eq!(dependent.dependency_count_unsatisfied, 0);
    }

    teardown_ephemeral_pool(pool, database).await;
}
