use std::sync::Arc;

use runledger_core::jobs::{
    JobType, StepKey, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder, WorkflowType,
};
use runledger_postgres::jobs::{
    AppendWorkflowStepsInput, EnqueueActiveWorkflowOutcome, JobDefinitionUpsert,
    WorkflowRecoveryDisposition, WorkflowRecoveryMode, WorkflowRecoveryRequest,
    append_workflow_steps, cancel_workflow_run_tx, enqueue_or_get_active_workflow,
    list_workflow_step_dependencies, list_workflow_steps, recover_workflow_run,
    recover_workflow_run_tx, update_workflow_step_and_pending_job_payload_tx,
    upsert_job_definition_tx,
};
use runledger_postgres::{DbPool, Error, QueryErrorCategory};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::{Value, json};
use sqlx::types::Uuid;
use tokio::sync::Barrier;

const JOB_TYPE_A: &str = "jobs.test.workflow_recovery.a";
const JOB_TYPE_B: &str = "jobs.test.workflow_recovery.b";
const WORKFLOW_TYPE: &str = "workflow.test.recovery";

async fn register_definitions(pool: &DbPool) {
    register_definitions_with_defaults(pool, 1, 3, 60, 100).await;
}

async fn register_definitions_with_defaults(
    pool: &DbPool,
    version: i32,
    max_attempts: i32,
    default_timeout_seconds: i32,
    default_priority: i32,
) {
    let mut tx = pool.begin().await.expect("begin definition transaction");
    for job_type in [JOB_TYPE_A, JOB_TYPE_B] {
        upsert_job_definition_tx(
            &mut tx,
            &JobDefinitionUpsert {
                job_type: JobType::new(job_type),
                version,
                max_attempts,
                default_timeout_seconds,
                default_priority,
                is_enabled: true,
            },
        )
        .await
        .expect("upsert definition");
    }
    tx.commit().await.expect("commit definition transaction");
}

async fn terminal_source_with_append_history(pool: &DbPool) -> (Uuid, Uuid, Value) {
    let metadata = json!({"source": "recovery-test"});
    let gate_payload = json!({"step": "gate"});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &gate_payload)
        .try_build()
        .expect("build source gate");
    let step_a_payload = json!({"step": "a"});
    let step_a = WorkflowStepEnqueueBuilder::new(
        StepKey::new("a"),
        JobType::new(JOB_TYPE_A),
        &step_a_payload,
    )
    .allow_handler_continuation()
    .execution_resource("provider-account:recovery")
    .depends_on_success(&[StepKey::new("gate")])
    .try_build()
    .expect("build source step");
    let workflow = WorkflowRunEnqueueBuilder::new(WorkflowType::new(WORKFLOW_TYPE), &metadata)
        .idempotency_key("source-once")
        .active_key("recovery-cycle")
        .result_step_key(StepKey::new("a"))
        .step(gate)
        .step(step_a)
        .try_build()
        .expect("build source workflow");
    let source = match enqueue_or_get_active_workflow(pool, &workflow)
        .await
        .expect("enqueue source")
    {
        EnqueueActiveWorkflowOutcome::Inserted(run) => run,
        other => panic!("expected inserted source, got {other:?}"),
    };
    let source_step = list_workflow_steps(pool, None, source.id)
        .await
        .expect("list source steps")
        .into_iter()
        .find(|step| step.step_key.as_str() == "a")
        .expect("source step a");

    let step_b_payload = json!({"step": "b"});
    let step_b = WorkflowStepEnqueueBuilder::new(
        StepKey::new("b"),
        JobType::new(JOB_TYPE_B),
        &step_b_payload,
    )
    .execution_resource("provider-account:recovery")
    .depends_on_success(&[StepKey::new("gate")])
    .try_build()
    .expect("build appended step");
    let mutation_metadata = json!({});
    append_workflow_steps(
        pool,
        &AppendWorkflowStepsInput {
            workflow_run_id: source.id,
            organization_id: None,
            mutation_key: "append-b",
            mutation_metadata: &mutation_metadata,
            append_window_step_key: StepKey::new("gate"),
            steps: vec![step_b],
        },
    )
    .await
    .expect("append source workflow step");

    let mut tx = pool.begin().await.expect("begin cancellation");
    cancel_workflow_run_tx(
        &mut tx,
        source.id,
        None,
        Some("prepare recovery"),
        None,
        None,
    )
    .await
    .expect("cancel source");
    tx.commit().await.expect("commit cancellation");

    let immutable_source = sqlx::query_scalar::<_, Value>(
        "SELECT to_jsonb(wr)
         FROM workflow_runs wr
         WHERE id = $1",
    )
    .bind(source.id)
    .fetch_one(pool)
    .await
    .expect("snapshot source row");
    (source.id, source_step.id, immutable_source)
}

#[tokio::test]
async fn recovery_replays_canonical_enqueue_and_append_history_without_reopening_source() {
    let (pool, database) = setup_ephemeral_pool("workflow_recovery_replay", 12).await;
    register_definitions(&pool).await;
    let (source_run_id, source_step_id, source_before) =
        terminal_source_with_append_history(&pool).await;
    register_definitions_with_defaults(&pool, 2, 9, 900, -100).await;
    let request = WorkflowRecoveryRequest::new(
        source_run_id,
        "recover-once",
        WorkflowRecoveryMode::FullReplay,
        "provider state repaired",
    )
    .source_step_id(source_step_id);

    let outcome = recover_workflow_run(&pool, &request)
        .await
        .expect("recover workflow");
    assert_eq!(outcome.disposition, WorkflowRecoveryDisposition::Inserted);
    assert_ne!(outcome.run.id, source_run_id);
    assert!(outcome.run.idempotency_key.is_none());
    for whitespace in ["\t", "\u{00a0}"] {
        let whitespace_error = sqlx::query(
            "UPDATE workflow_recoveries
             SET request_key = $2
             WHERE recovery_run_id = $1",
        )
        .bind(outcome.run.id)
        .bind(whitespace)
        .execute(&pool)
        .await
        .expect_err("database must reject whitespace-only recovery request keys");
        assert_eq!(
            whitespace_error
                .as_database_error()
                .and_then(|error| error.constraint()),
            Some("chk_workflow_recoveries_request_key")
        );
    }

    let steps = list_workflow_steps(&pool, None, outcome.run.id)
        .await
        .expect("list recovery steps");
    assert_eq!(steps.len(), 3);
    let step_a = steps
        .iter()
        .find(|step| step.step_key.as_str() == "a")
        .expect("recovered step a");
    assert!(step_a.allow_handler_continuation);
    assert_eq!(
        step_a.execution_resource_key.as_deref(),
        Some("provider-account:recovery")
    );
    assert_eq!(step_a.priority, Some(100));
    assert_eq!(step_a.max_attempts, Some(3));
    assert_eq!(step_a.timeout_seconds, Some(60));
    let step_b = steps
        .iter()
        .find(|step| step.step_key.as_str() == "b")
        .expect("recovered step b");
    assert_eq!(
        step_b.execution_resource_key.as_deref(),
        Some("provider-account:recovery")
    );
    assert_eq!(step_b.priority, Some(100));
    assert_eq!(step_b.max_attempts, Some(3));
    assert_eq!(step_b.timeout_seconds, Some(60));
    let dependencies = list_workflow_step_dependencies(&pool, None, outcome.run.id)
        .await
        .expect("list recovery dependencies");
    assert_eq!(dependencies.len(), 2);

    let lineage = sqlx::query_as::<_, (Uuid, Uuid, Option<Uuid>, String, String)>(
        "SELECT recovery_run_id, source_run_id, source_step_id, mode, reason
         FROM workflow_recoveries
         WHERE recovery_run_id = $1",
    )
    .bind(outcome.run.id)
    .fetch_one(&pool)
    .await
    .expect("load recovery lineage");
    assert_eq!(
        lineage,
        (
            outcome.run.id,
            source_run_id,
            Some(source_step_id),
            "FULL_REPLAY".to_owned(),
            "provider state repaired".to_owned()
        )
    );
    assert_eq!(
        sqlx::query_scalar::<_, Uuid>(
            "SELECT workflow_run_id
             FROM workflow_active_claims
             WHERE scope = 'global' AND active_key = 'recovery-cycle'",
        )
        .fetch_one(&pool)
        .await
        .expect("load recovery active claim"),
        outcome.run.id
    );
    let source_after = sqlx::query_scalar::<_, Value>(
        "SELECT to_jsonb(wr)
         FROM workflow_runs wr
         WHERE id = $1",
    )
    .bind(source_run_id)
    .fetch_one(&pool)
    .await
    .expect("reload source row");
    assert_eq!(source_after, source_before);

    let existing = recover_workflow_run(&pool, &request)
        .await
        .expect("repeat recovery");
    assert_eq!(existing.disposition, WorkflowRecoveryDisposition::Existing);
    assert_eq!(existing.run.id, outcome.run.id);

    let conflict = recover_workflow_run(
        &pool,
        &WorkflowRecoveryRequest::new(
            source_run_id,
            "recover-once",
            WorkflowRecoveryMode::FullReplay,
            "different reason",
        )
        .source_step_id(source_step_id),
    )
    .await
    .expect_err("conflicting request key must fail");
    match conflict {
        Error::QueryError(error) => {
            assert_eq!(error.category(), QueryErrorCategory::Conflict);
            assert_eq!(error.code(), "workflow.recovery_request_conflict");
        }
        other => panic!("expected recovery conflict, got {other:?}"),
    }

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn recovery_uses_the_source_steps_latest_persisted_payload() {
    let (pool, database) = setup_ephemeral_pool("workflow_recovery_mutated_payload", 8).await;
    register_definitions(&pool).await;

    let metadata = json!({"source": "payload-mutation"});
    let original_payload = json!({"revision": "original"});
    let source_step = WorkflowStepEnqueueBuilder::new(
        StepKey::new("mutable"),
        JobType::new(JOB_TYPE_A),
        &original_payload,
    )
    .try_build()
    .expect("build mutable source step");
    let source_enqueue =
        WorkflowRunEnqueueBuilder::new(WorkflowType::new(WORKFLOW_TYPE), &metadata)
            .idempotency_key("mutated-payload-source")
            .active_key("mutated-payload-cycle")
            .step(source_step)
            .try_build()
            .expect("build mutable source workflow");
    let source = match enqueue_or_get_active_workflow(&pool, &source_enqueue)
        .await
        .expect("enqueue mutable source")
    {
        EnqueueActiveWorkflowOutcome::Inserted(run) => run,
        other => panic!("expected inserted source, got {other:?}"),
    };
    let persisted_source_step = list_workflow_steps(&pool, None, source.id)
        .await
        .expect("list mutable source steps")
        .into_iter()
        .next()
        .expect("mutable source step");
    let source_job_id = persisted_source_step
        .job_id
        .expect("job step should have a pending job");

    let changed_payload = json!({"revision": "operator-corrected"});
    let mut mutation_tx = pool.begin().await.expect("begin payload mutation");
    assert!(
        update_workflow_step_and_pending_job_payload_tx(
            &mut mutation_tx,
            source.id,
            None,
            persisted_source_step.id,
            source_job_id,
            &changed_payload,
        )
        .await
        .expect("mutate pending workflow payload")
    );
    mutation_tx.commit().await.expect("commit payload mutation");

    let mut cancellation_tx = pool.begin().await.expect("begin source cancellation");
    cancel_workflow_run_tx(
        &mut cancellation_tx,
        source.id,
        None,
        Some("prepare corrected-payload recovery"),
        None,
        None,
    )
    .await
    .expect("cancel mutable source");
    cancellation_tx
        .commit()
        .await
        .expect("commit source cancellation");

    let idempotent_source = enqueue_or_get_active_workflow(&pool, &source_enqueue)
        .await
        .expect("retry canonical source enqueue");
    assert!(matches!(
        idempotent_source,
        EnqueueActiveWorkflowOutcome::ExistingIdempotent(run) if run.id == source.id
    ));

    let recovered = recover_workflow_run(
        &pool,
        &WorkflowRecoveryRequest::new(
            source.id,
            "recover-corrected-payload",
            WorkflowRecoveryMode::FullReplay,
            "retain operator correction",
        )
        .source_step_id(persisted_source_step.id),
    )
    .await
    .expect("recover corrected payload");
    let recovered_step = list_workflow_steps(&pool, None, recovered.run.id)
        .await
        .expect("list recovered corrected steps")
        .into_iter()
        .next()
        .expect("recovered corrected step");
    assert_eq!(recovered_step.payload, changed_payload);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn concurrent_equal_recovery_requests_create_exactly_one_new_run() {
    let (pool, database) = setup_ephemeral_pool("workflow_recovery_race", 24).await;
    register_definitions(&pool).await;
    let (source_run_id, source_step_id, _) = terminal_source_with_append_history(&pool).await;
    let barrier = Arc::new(Barrier::new(50));
    let mut tasks = Vec::new();
    for _ in 0..50 {
        let pool = pool.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            recover_workflow_run(
                &pool,
                &WorkflowRecoveryRequest::new(
                    source_run_id,
                    "concurrent-recovery",
                    WorkflowRecoveryMode::FullReplay,
                    "retry together",
                )
                .source_step_id(source_step_id),
            )
            .await
            .expect("concurrent recovery")
        }));
    }

    let mut inserted = 0;
    let mut recovery_run_id = None;
    for task in tasks {
        let outcome = task.await.expect("recovery task");
        if outcome.disposition == WorkflowRecoveryDisposition::Inserted {
            inserted += 1;
        }
        recovery_run_id.get_or_insert(outcome.run.id);
        assert_eq!(recovery_run_id, Some(outcome.run.id));
    }
    assert_eq!(inserted, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*)
             FROM workflow_recoveries
             WHERE source_run_id = $1
               AND request_key = 'concurrent-recovery'",
        )
        .bind(source_run_id)
        .fetch_one(&pool)
        .await
        .expect("count lineage rows"),
        1
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn deleting_only_the_recovery_run_cannot_erase_request_idempotency() {
    let (pool, database) = setup_ephemeral_pool("workflow_recovery_retention", 8).await;
    register_definitions(&pool).await;
    let (source_run_id, source_step_id, _) = terminal_source_with_append_history(&pool).await;
    let request = WorkflowRecoveryRequest::new(
        source_run_id,
        "retained-recovery-request",
        WorkflowRecoveryMode::FullReplay,
        "preserve recovery request identity",
    )
    .source_step_id(source_step_id);
    let inserted = recover_workflow_run(&pool, &request)
        .await
        .expect("insert recovery");

    let deletion_error = sqlx::query("DELETE FROM workflow_runs WHERE id = $1")
        .bind(inserted.run.id)
        .execute(&pool)
        .await
        .expect_err("recovery run deletion must preserve source request idempotency");
    assert_eq!(
        deletion_error
            .as_database_error()
            .and_then(|error| error.code())
            .as_deref(),
        Some("23503")
    );

    let existing = recover_workflow_run(&pool, &request)
        .await
        .expect("retry retained recovery request");
    assert_eq!(existing.disposition, WorkflowRecoveryDisposition::Existing);
    assert_eq!(existing.run.id, inserted.run.id);

    sqlx::query("DELETE FROM workflow_runs WHERE id = ANY($1::uuid[])")
        .bind(&[source_run_id, inserted.run.id][..])
        .execute(&pool)
        .await
        .expect("source-led retention can delete the complete lineage");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn oversized_recovery_request_key_is_rejected_before_storage() {
    let (pool, database) =
        setup_ephemeral_pool("workflow_recovery_request_key_validation", 2).await;
    let oversized_request_key = "x".repeat(513);
    let error = recover_workflow_run(
        &pool,
        &WorkflowRecoveryRequest::new(
            Uuid::from_u128(999),
            &oversized_request_key,
            WorkflowRecoveryMode::FullReplay,
            "validate request key",
        ),
    )
    .await
    .expect_err("oversized recovery request key must fail validation");
    let Error::QueryError(error) = error else {
        panic!("expected recovery request validation error");
    };
    assert_eq!(error.category(), QueryErrorCategory::Validation);
    assert_eq!(error.code(), "workflow.invalid_recovery_request_key");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn legacy_source_without_canonical_snapshot_is_rejected() {
    let (pool, database) = setup_ephemeral_pool("workflow_recovery_legacy", 4).await;
    let source_run_id = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO workflow_runs (
            workflow_type,
            status,
            metadata,
            enqueue_request,
            finished_at
         )
         VALUES ('workflow.test.legacy', 'CANCELED', '{}'::jsonb, NULL, now())
         RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .expect("insert legacy source");

    let error = recover_workflow_run(
        &pool,
        &WorkflowRecoveryRequest::new(
            source_run_id,
            "legacy-recovery",
            WorkflowRecoveryMode::FullReplay,
            "try legacy",
        ),
    )
    .await
    .expect_err("legacy recovery must fail");
    match error {
        Error::QueryError(error) => {
            assert_eq!(error.category(), QueryErrorCategory::Conflict);
            assert_eq!(error.code(), "workflow.recovery_snapshot_missing");
        }
        other => panic!("expected missing snapshot conflict, got {other:?}"),
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM workflow_recoveries")
            .fetch_one(&pool)
            .await
            .expect("count recoveries"),
        0
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn recovery_fails_closed_on_unknown_snapshot_fields_and_mutation_kinds() {
    let (pool, database) = setup_ephemeral_pool("workflow_recovery_fail_closed", 8).await;
    register_definitions(&pool).await;
    let (source_run_id, source_step_id, _) = terminal_source_with_append_history(&pool).await;

    let mutation_kind_error = sqlx::query(
        "UPDATE workflow_run_mutations
         SET mutation_kind = 'FUTURE_MUTATION'
         WHERE workflow_run_id = $1",
    )
    .bind(source_run_id)
    .execute(&pool)
    .await
    .expect_err("database must reject unknown workflow mutation kinds");
    assert_eq!(
        mutation_kind_error
            .as_database_error()
            .and_then(|error| error.constraint()),
        Some("chk_workflow_run_mutations_kind")
    );

    sqlx::query(
        "UPDATE workflow_runs
         SET enqueue_request = jsonb_set(
             enqueue_request,
             '{steps,0,future_execution_constraint}',
             'true'::jsonb,
             true
         )
         WHERE id = $1",
    )
    .bind(source_run_id)
    .execute(&pool)
    .await
    .expect("inject unknown canonical snapshot field");

    let error = recover_workflow_run(
        &pool,
        &WorkflowRecoveryRequest::new(
            source_run_id,
            "reject-unknown-snapshot-field",
            WorkflowRecoveryMode::FullReplay,
            "prove fail-closed recovery",
        )
        .source_step_id(source_step_id),
    )
    .await
    .expect_err("unknown recovery snapshot fields must fail closed");
    let Error::QueryError(error) = error else {
        panic!("expected unsafe recovery snapshot error");
    };
    assert_eq!(error.code(), "workflow.recovery_snapshot_unsafe");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn recovery_rejects_live_continuation_and_resource_setting_drift() {
    let (pool, database) = setup_ephemeral_pool("workflow_recovery_setting_drift", 8).await;
    register_definitions(&pool).await;
    let (source_run_id, source_step_id, _) = terminal_source_with_append_history(&pool).await;

    sqlx::query(
        "UPDATE workflow_steps
         SET allow_handler_continuation = false
         WHERE workflow_run_id = $1
           AND step_key = 'a'",
    )
    .bind(source_run_id)
    .execute(&pool)
    .await
    .expect("simulate continuation setting drift");
    let continuation_error = recover_workflow_run(
        &pool,
        &WorkflowRecoveryRequest::new(
            source_run_id,
            "reject-continuation-drift",
            WorkflowRecoveryMode::FullReplay,
            "prove continuation setting integrity",
        )
        .source_step_id(source_step_id),
    )
    .await
    .expect_err("recovery must reject continuation setting drift");
    let Error::QueryError(continuation_error) = continuation_error else {
        panic!("expected unsafe continuation setting snapshot error");
    };
    assert_eq!(
        continuation_error.code(),
        "workflow.recovery_snapshot_unsafe"
    );

    sqlx::query(
        "UPDATE workflow_steps
         SET allow_handler_continuation = true,
             execution_resource_key = 'provider-account:drifted'
         WHERE workflow_run_id = $1
           AND step_key = 'a'",
    )
    .bind(source_run_id)
    .execute(&pool)
    .await
    .expect("simulate execution resource setting drift");
    let resource_error = recover_workflow_run(
        &pool,
        &WorkflowRecoveryRequest::new(
            source_run_id,
            "reject-resource-drift",
            WorkflowRecoveryMode::FullReplay,
            "prove resource setting integrity",
        )
        .source_step_id(source_step_id),
    )
    .await
    .expect_err("recovery must reject execution resource setting drift");
    let Error::QueryError(resource_error) = resource_error else {
        panic!("expected unsafe resource setting snapshot error");
    };
    assert_eq!(resource_error.code(), "workflow.recovery_snapshot_unsafe");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn recovery_rejects_non_read_committed_transactions_up_front() {
    let (pool, database) = setup_ephemeral_pool("workflow_recovery_isolation_guard", 8).await;
    register_definitions(&pool).await;
    let (source_run_id, source_step_id, _) = terminal_source_with_append_history(&pool).await;
    let request = WorkflowRecoveryRequest::new(
        source_run_id,
        "reject-repeatable-read",
        WorkflowRecoveryMode::FullReplay,
        "unsupported isolation must be explicit",
    )
    .source_step_id(source_step_id);

    let mut tx = pool.begin().await.expect("begin recovery transaction");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await
        .expect("set repeatable-read isolation");
    let error = recover_workflow_run_tx(&mut tx, &request)
        .await
        .expect_err("repeatable-read recovery must be rejected deterministically");
    let Error::QueryError(error) = error else {
        panic!("expected isolation validation error");
    };
    assert_eq!(error.code(), "workflow.recovery_unsupported_isolation");
    tx.rollback().await.expect("rollback isolation test");

    let recovery_count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)
         FROM workflow_recoveries
         WHERE source_run_id = $1
           AND request_key = $2",
    )
    .bind(source_run_id)
    .bind(request.request_key)
    .fetch_one(&pool)
    .await
    .expect("count recoveries after rejected isolation");
    assert_eq!(recovery_count, 0);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn recovery_rejects_nonterminal_sources_foreign_steps_and_active_key_collisions() {
    let (pool, database) = setup_ephemeral_pool("workflow_recovery_rejections", 8).await;
    register_definitions(&pool).await;
    let (source_run_id, source_step_id, _) = terminal_source_with_append_history(&pool).await;

    let blocker_metadata = json!({"source": "active-key-blocker"});
    let blocker_payload = json!({"step": "blocker"});
    let blocker_step = WorkflowStepEnqueueBuilder::new(
        StepKey::new("blocker"),
        JobType::new(JOB_TYPE_A),
        &blocker_payload,
    )
    .try_build()
    .expect("build active-key blocker step");
    let blocker_enqueue =
        WorkflowRunEnqueueBuilder::new(WorkflowType::new(WORKFLOW_TYPE), &blocker_metadata)
            .active_key("recovery-cycle")
            .step(blocker_step)
            .try_build()
            .expect("build active-key blocker workflow");
    let blocker = match enqueue_or_get_active_workflow(&pool, &blocker_enqueue)
        .await
        .expect("enqueue active-key blocker")
    {
        EnqueueActiveWorkflowOutcome::Inserted(run) => run,
        other => panic!("expected inserted active-key blocker, got {other:?}"),
    };
    let blocker_step_id = list_workflow_steps(&pool, None, blocker.id)
        .await
        .expect("list active-key blocker steps")
        .into_iter()
        .next()
        .expect("active-key blocker step")
        .id;

    let nonterminal = recover_workflow_run(
        &pool,
        &WorkflowRecoveryRequest::new(
            blocker.id,
            "reject-nonterminal",
            WorkflowRecoveryMode::FullReplay,
            "source is still running",
        ),
    )
    .await
    .expect_err("nonterminal recovery source must fail");
    let Error::QueryError(nonterminal) = nonterminal else {
        panic!("expected nonterminal recovery query error");
    };
    assert_eq!(nonterminal.code(), "workflow.recovery_source_not_terminal");

    let foreign_step = recover_workflow_run(
        &pool,
        &WorkflowRecoveryRequest::new(
            source_run_id,
            "reject-foreign-step",
            WorkflowRecoveryMode::FullReplay,
            "source step belongs to another run",
        )
        .source_step_id(blocker_step_id),
    )
    .await
    .expect_err("foreign recovery source step must fail");
    let Error::QueryError(foreign_step) = foreign_step else {
        panic!("expected foreign source-step query error");
    };
    assert_eq!(
        foreign_step.code(),
        "workflow.recovery_source_step_not_found"
    );

    let active_collision = recover_workflow_run(
        &pool,
        &WorkflowRecoveryRequest::new(
            source_run_id,
            "reject-active-collision",
            WorkflowRecoveryMode::FullReplay,
            "active key is occupied",
        )
        .source_step_id(source_step_id),
    )
    .await
    .expect_err("occupied recovery active key must fail");
    let Error::QueryError(active_collision) = active_collision else {
        panic!("expected recovery active-key query error");
    };
    assert_eq!(
        active_collision.code(),
        "workflow.recovery_active_constraint"
    );

    teardown_ephemeral_pool(pool, database).await;
}
