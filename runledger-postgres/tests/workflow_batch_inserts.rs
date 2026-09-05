use runledger_core::jobs::{
    JobStage, JobType, StepKey, WorkflowRunEnqueueBuilder, WorkflowStepEnqueue,
    WorkflowStepEnqueueBuilder, WorkflowType,
};
use runledger_postgres::{DbPool, jobs::*};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::{Value, json};
use sqlx::types::Uuid;

mod support;

#[tokio::test]
async fn batch_preserves_json_null_payloads_for_job_and_external_steps() {
    let (pool, database) = setup_ephemeral_pool("workflow_batch_null", 1).await;
    support::register_test_job_definition(&pool, "batch.job").await;
    let payload = Value::Null;
    let job =
        WorkflowStepEnqueueBuilder::new(StepKey::new("job"), JobType::new("batch.job"), &payload)
            .try_build()
            .expect("null job payload");
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("null gate payload");
    let request = WorkflowRunEnqueueBuilder::new(WorkflowType::new("batch.null"), &payload)
        .step(job)
        .step(gate)
        .try_build()
        .expect("run");
    enqueue_workflow_run(&pool, &request)
        .await
        .expect("enqueue null payloads");
    let payloads: Vec<Value> =
        sqlx::query_scalar("SELECT payload FROM workflow_steps ORDER BY step_key")
            .fetch_all(&pool)
            .await
            .expect("persisted JSON nulls");
    assert_eq!(payloads, vec![Value::Null, Value::Null]);
    let queued_payload: Value = sqlx::query_scalar("SELECT payload FROM job_queue")
        .fetch_one(&pool)
        .await
        .expect("queued JSON null");
    assert_eq!(queued_payload, Value::Null);
    teardown_ephemeral_pool(pool, database).await;
}

fn graph_steps<'a>(keys: &'a [String], payload: &'a Value) -> Vec<WorkflowStepEnqueue<'a>> {
    keys.iter()
        .enumerate()
        .map(|(i, key)| {
            let dependencies = keys[i.saturating_sub(2)..i]
                .iter()
                .map(|k| StepKey::new(k))
                .collect::<Vec<_>>();
            WorkflowStepEnqueueBuilder::new(StepKey::new(key), JobType::new("batch.job"), payload)
                .depends_on_success(&dependencies)
                .priority(-7)
                .max_attempts(5)
                .timeout_seconds(43)
                .stage(JobStage::Scheduled)
                .allow_handler_continuation()
                .execution_resource("batch-resource")
                .try_build()
                .expect("step")
        })
        .collect()
}

async fn counts(pool: &DbPool) -> (i64, i64, i64, i64, i64) {
    sqlx::query_as(
        "SELECT (SELECT count(*) FROM workflow_runs), (SELECT count(*) FROM workflow_steps),
        (SELECT count(*) FROM workflow_step_dependencies), (SELECT count(*) FROM job_queue),
        (SELECT count(*) FROM job_events)",
    )
    .fetch_one(pool)
    .await
    .expect("durable counts")
}

#[tokio::test]
async fn multi_chunk_graph_preserves_fields_edges_audit_and_snapshot_idempotency() {
    let (pool, database) = setup_ephemeral_pool("workflow_batch", 1).await;
    support::register_test_job_definition(&pool, "batch.job").await;
    let keys = (0..270).map(|i| format!("s{i:03}")).collect::<Vec<_>>();
    let payload = json!({"nested": [null, {"unicode": "žluťoučký", "text": "x".repeat(4000)}]});
    let tenant = Uuid::now_v7();
    let mut builder = WorkflowRunEnqueueBuilder::new(WorkflowType::new("batch.workflow"), &payload)
        .organization_id(tenant)
        .idempotency_key("batch-key");
    for step in graph_steps(&keys, &payload) {
        builder = builder.step(step);
    }
    let request = builder.try_build().expect("run");
    let run = enqueue_workflow_run(&pool, &request)
        .await
        .expect("enqueue");
    assert_eq!(counts(&pool).await, (1, 270, 537, 1, 1));
    let wrong_fields: i64 = sqlx::query_scalar("SELECT count(*) FROM workflow_steps WHERE
        organization_id IS DISTINCT FROM $1 OR payload <> $2 OR priority <> -7 OR max_attempts <> 5
        OR timeout_seconds <> 43 OR stage <> 'scheduled' OR NOT allow_handler_continuation
        OR execution_resource_key <> 'batch-resource' OR dependency_count_unsatisfied <> 0
        OR dependency_count_total <> CASE WHEN step_key='s000' THEN 0 WHEN step_key='s001' THEN 1 ELSE 2 END
        OR dependency_count_pending <> dependency_count_total")
        .bind(tenant).bind(&payload).fetch_one(&pool).await.expect("persisted fields");
    assert_eq!(wrong_fields, 0);
    let edges: Vec<(String, String, String)> = sqlx::query_as(
        "SELECT p.step_key, d.step_key, e.release_mode::text
        FROM workflow_step_dependencies e JOIN workflow_steps p ON p.id=e.prerequisite_step_id
        JOIN workflow_steps d ON d.id=e.dependent_step_id ORDER BY d.step_key, p.step_key",
    )
    .fetch_all(&pool)
    .await
    .expect("edges");
    let expected = (0..270_usize)
        .flat_map(|i| {
            (i.saturating_sub(2)..i).map(move |j| {
                (
                    format!("s{j:03}"),
                    format!("s{i:03}"),
                    "ON_SUCCESS".to_owned(),
                )
            })
        })
        .collect::<Vec<_>>();
    assert_eq!(edges, expected);
    assert_eq!(
        enqueue_workflow_run(&pool, &request)
            .await
            .expect("identical retry")
            .id,
        run.id
    );
    assert_eq!(counts(&pool).await, (1, 270, 537, 1, 1));
    // The single initial root still has one ordinary ENQUEUED audit event.
    let event: String = sqlx::query_scalar("SELECT event_type::text FROM job_events")
        .fetch_one(&pool)
        .await
        .expect("event");
    assert_eq!(event, "ENQUEUED");
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn failure_in_later_step_or_edge_chunk_rolls_back_the_owned_transaction() {
    let (pool, database) = setup_ephemeral_pool("workflow_batch_rollback", 1).await;
    support::register_test_job_definition(&pool, "batch.job").await;
    let keys = (0..270).map(|i| format!("s{i:03}")).collect::<Vec<_>>();
    let payload = json!({});
    let mut builder = WorkflowRunEnqueueBuilder::new(WorkflowType::new("batch.rollback"), &payload);
    for step in graph_steps(&keys, &payload) {
        builder = builder.step(step);
    }
    let request = builder.try_build().expect("run");
    for (table, predicate) in [
        ("workflow_steps", "NEW.step_key = 's269'"),
        (
            "workflow_step_dependencies",
            "EXISTS (SELECT 1 FROM workflow_steps WHERE id=NEW.dependent_step_id AND step_key='s269')",
        ),
    ] {
        // Fail after earlier chunks have succeeded, exercising the actual transaction boundary.
        sqlx::raw_sql(&format!("CREATE FUNCTION reject_late_row() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF {predicate} THEN RAISE EXCEPTION 'injected late row failure'; END IF; RETURN NEW; END $$;
            CREATE TRIGGER reject_late BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION reject_late_row();"))
            .execute(&pool).await.expect("install fault");
        enqueue_workflow_run(&pool, &request)
            .await
            .expect_err("late insertion fails");
        assert_eq!(counts(&pool).await, (0, 0, 0, 0, 0));
        sqlx::raw_sql(&format!(
            "DROP TRIGGER reject_late ON {table}; DROP FUNCTION reject_late_row();"
        ))
        .execute(&pool)
        .await
        .expect("remove fault");
    }
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn multi_chunk_append_preserves_input_order_and_idempotent_outcome() {
    let (pool, database) = setup_ephemeral_pool("workflow_batch_append", 1).await;
    let payload = json!({});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("gate");
    let run = enqueue_workflow_run(
        &pool,
        &WorkflowRunEnqueueBuilder::new(WorkflowType::new("batch.append"), &payload)
            .step(gate)
            .try_build()
            .expect("run"),
    )
    .await
    .expect("enqueue");
    let keys = (0..270)
        .rev()
        .map(|i| format!("s{i:03}"))
        .collect::<Vec<_>>();
    let steps = keys
        .iter()
        .map(|key| {
            WorkflowStepEnqueueBuilder::new_external(StepKey::new(key), &payload)
                .depends_on_terminal(&[StepKey::new("gate")])
                .try_build()
                .expect("external step")
        })
        .collect();
    let request = AppendWorkflowStepsInput {
        workflow_run_id: run.id,
        organization_id: None,
        mutation_key: "append",
        mutation_metadata: &payload,
        append_window_step_key: StepKey::new("gate"),
        steps,
    };
    let first = append_workflow_steps(&pool, &request)
        .await
        .expect("append");
    assert_eq!(first.outcome, AppendWorkflowStepsOutcome::Appended);
    assert_eq!(
        first
            .appended_steps
            .iter()
            .map(|s| s.step_key.as_str())
            .collect::<Vec<_>>(),
        keys
    );
    let retry = append_workflow_steps(&pool, &request)
        .await
        .expect("retry append");
    assert_eq!(retry.outcome, AppendWorkflowStepsOutcome::AlreadyApplied);
    assert_eq!(
        first
            .appended_steps
            .iter()
            .map(|s| s.id)
            .collect::<Vec<_>>(),
        retry
            .appended_steps
            .iter()
            .map(|s| s.id)
            .collect::<Vec<_>>()
    );
    assert_eq!(counts(&pool).await, (1, 271, 270, 0, 0));
    teardown_ephemeral_pool(pool, database).await;
}
