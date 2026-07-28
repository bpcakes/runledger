use std::sync::Arc;

use runledger_core::jobs::{
    JobType, StepKey, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder, WorkflowType,
};
use runledger_postgres::Error;
use runledger_postgres::jobs::{
    EnqueueActiveWorkflowOutcome, JobDefinitionUpsert, cancel_job, cancel_workflow_run_tx,
    claim_jobs, complete_job_success, enqueue_or_get_active_workflow, enqueue_workflow_run,
    enqueue_workflow_run_handle, get_job_by_id, reap_expired_leases_with_diagnostics,
    upsert_job_definition_tx,
};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::types::Uuid;
use tokio::sync::Barrier;

const JOB_TYPE: &str = "jobs.test.workflow_active_claim";
const WORKFLOW_TYPE: &str = "workflow.test.active_claim";

async fn register_definition(pool: &sqlx::PgPool) {
    let mut tx = pool.begin().await.expect("begin definition transaction");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(JOB_TYPE),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert active-claim definition");
    tx.commit().await.expect("commit definition transaction");
}

async fn enqueue_active(
    pool: &sqlx::PgPool,
    organization_id: Option<Uuid>,
    active_key: &str,
    idempotency_key: Option<&str>,
    payload_value: usize,
) -> EnqueueActiveWorkflowOutcome {
    enqueue_active_for_type(
        pool,
        organization_id,
        WORKFLOW_TYPE,
        active_key,
        idempotency_key,
        payload_value,
    )
    .await
}

async fn enqueue_active_for_type(
    pool: &sqlx::PgPool,
    organization_id: Option<Uuid>,
    workflow_type: &str,
    active_key: &str,
    idempotency_key: Option<&str>,
    payload_value: usize,
) -> EnqueueActiveWorkflowOutcome {
    let payload = json!({"value": payload_value});
    let metadata = json!({"value": payload_value});
    let step =
        WorkflowStepEnqueueBuilder::new(StepKey::new("root"), JobType::new(JOB_TYPE), &payload)
            .try_build()
            .expect("build active workflow step");
    let mut builder = WorkflowRunEnqueueBuilder::new(WorkflowType::new(workflow_type), &metadata)
        .active_key(active_key)
        .step(step);
    if let Some(organization_id) = organization_id {
        builder = builder.organization_id(organization_id);
    }
    if let Some(idempotency_key) = idempotency_key {
        builder = builder.idempotency_key(idempotency_key);
    }
    let workflow = builder.try_build().expect("build active workflow");

    enqueue_or_get_active_workflow(pool, &workflow)
        .await
        .expect("enqueue active workflow")
}

#[tokio::test]
async fn active_key_apis_reject_payloads_for_the_wrong_enqueue_contract() {
    let (pool, database) = setup_ephemeral_pool("workflow_active_claim_api_contract", 4).await;
    let payload = json!({"value": 1});
    let metadata = json!({"value": 1});
    let regular_step =
        WorkflowStepEnqueueBuilder::new(StepKey::new("root"), JobType::new(JOB_TYPE), &payload)
            .try_build()
            .expect("build regular workflow step");
    let regular_workflow =
        WorkflowRunEnqueueBuilder::new(WorkflowType::new(WORKFLOW_TYPE), &metadata)
            .step(regular_step)
            .try_build()
            .expect("build workflow without active key");

    let missing_key_error = enqueue_or_get_active_workflow(&pool, &regular_workflow)
        .await
        .expect_err("active enqueue API must require an active key");
    let Error::QueryError(missing_key_error) = missing_key_error else {
        panic!("expected active-key validation error");
    };
    assert_eq!(missing_key_error.code(), "workflow.active_key_required");

    let active_step =
        WorkflowStepEnqueueBuilder::new(StepKey::new("root"), JobType::new(JOB_TYPE), &payload)
            .try_build()
            .expect("build active workflow step");
    let active_workflow =
        WorkflowRunEnqueueBuilder::new(WorkflowType::new(WORKFLOW_TYPE), &metadata)
            .active_key("api-contract")
            .step(active_step)
            .try_build()
            .expect("build workflow with active key");

    let wrong_api_error = enqueue_workflow_run(&pool, &active_workflow)
        .await
        .expect_err("ordinary enqueue API must preserve active collision classification");
    let Error::QueryError(wrong_api_error) = wrong_api_error else {
        panic!("expected active-key API validation error");
    };
    assert_eq!(wrong_api_error.code(), "workflow.active_key_api_required");

    let handle_api_error = enqueue_workflow_run_handle(&pool, &active_workflow)
        .await
        .expect_err("handle enqueue must reject active keys before ordinary enqueue");
    let Error::QueryError(handle_api_error) = handle_api_error else {
        panic!("expected handle active-key API validation error");
    };
    assert_eq!(handle_api_error.code(), "workflow.active_key_api_required");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM workflow_runs")
            .fetch_one(&pool)
            .await
            .expect("count workflows after rejected API calls"),
        0
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn one_hundred_concurrent_active_enqueues_create_one_run_and_explicit_collisions() {
    let (pool, database) = setup_ephemeral_pool("workflow_active_claim_concurrency", 24).await;
    register_definition(&pool).await;
    let barrier = Arc::new(Barrier::new(100));
    let mut tasks = Vec::new();
    for value in 0..100 {
        let task_pool = pool.clone();
        let task_barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            task_barrier.wait().await;
            enqueue_active(&task_pool, None, "daily-cycle", None, value).await
        }));
    }

    let mut inserted = Vec::new();
    let mut existing = Vec::new();
    for task in tasks {
        match task.await.expect("active enqueue task should not panic") {
            EnqueueActiveWorkflowOutcome::Inserted(run) => inserted.push(run.id),
            EnqueueActiveWorkflowOutcome::ExistingActive(run) => existing.push(run.id),
            EnqueueActiveWorkflowOutcome::ExistingIdempotent(_) => {
                panic!("unkeyed concurrent enqueue cannot be idempotent")
            }
            _ => panic!("unexpected future active enqueue outcome"),
        }
    }

    assert_eq!(inserted.len(), 1);
    assert_eq!(existing.len(), 99);
    assert!(existing.iter().all(|run_id| *run_id == inserted[0]));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM workflow_runs")
            .fetch_one(&pool)
            .await
            .expect("count workflow runs"),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM workflow_active_claims")
            .fetch_one(&pool)
            .await
            .expect("count workflow active claims"),
        1
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn active_key_isolated_by_scope_and_idempotent_retry_is_classified_separately() {
    let (pool, database) = setup_ephemeral_pool("workflow_active_claim_scope", 8).await;
    register_definition(&pool).await;
    let organization_a = Uuid::now_v7();
    let organization_b = Uuid::now_v7();

    let global = enqueue_active(&pool, None, "shared-key", Some("global-request"), 1).await;
    let global_id = global.workflow_run().id;
    assert!(matches!(global, EnqueueActiveWorkflowOutcome::Inserted(_)));
    for whitespace in ["\t", "\u{00a0}"] {
        let whitespace_error = sqlx::query(
            "UPDATE workflow_active_claims
             SET active_key = $2
             WHERE workflow_run_id = $1",
        )
        .bind(global_id)
        .bind(whitespace)
        .execute(&pool)
        .await
        .expect_err("database must reject whitespace-only active keys");
        assert_eq!(
            whitespace_error
                .as_database_error()
                .and_then(|error| error.constraint()),
            Some("chk_workflow_active_claims_key_not_blank")
        );
    }
    let idempotent = enqueue_active(&pool, None, "shared-key", Some("global-request"), 1).await;
    assert!(matches!(
        &idempotent,
        EnqueueActiveWorkflowOutcome::ExistingIdempotent(_)
    ));
    assert_eq!(idempotent.workflow_run().id, global_id);

    let active_collision = enqueue_active(&pool, None, "shared-key", None, 999).await;
    assert!(matches!(
        &active_collision,
        EnqueueActiveWorkflowOutcome::ExistingActive(_)
    ));
    assert_eq!(active_collision.workflow_run().id, global_id);
    let cross_type_collision = enqueue_active_for_type(
        &pool,
        None,
        "workflow.test.other_active_claim",
        "shared-key",
        None,
        1_000,
    )
    .await;
    assert!(matches!(
        &cross_type_collision,
        EnqueueActiveWorkflowOutcome::ExistingActive(_)
    ));
    assert_eq!(cross_type_collision.workflow_run().id, global_id);
    assert_eq!(
        cross_type_collision.workflow_run().workflow_type.as_str(),
        WORKFLOW_TYPE
    );
    let organization_a_run =
        enqueue_active(&pool, Some(organization_a), "shared-key", None, 2).await;
    let organization_b_run =
        enqueue_active(&pool, Some(organization_b), "shared-key", None, 3).await;
    assert!(matches!(
        &organization_a_run,
        EnqueueActiveWorkflowOutcome::Inserted(_)
    ));
    assert!(matches!(
        &organization_b_run,
        EnqueueActiveWorkflowOutcome::Inserted(_)
    ));
    let organization_a_collision =
        enqueue_active(&pool, Some(organization_a), "shared-key", None, 4).await;
    assert!(matches!(
        &organization_a_collision,
        EnqueueActiveWorkflowOutcome::ExistingActive(_)
    ));
    assert_eq!(
        organization_a_collision.workflow_run().id,
        organization_a_run.workflow_run().id
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn terminal_completion_and_active_enqueue_race_leaves_one_reusable_claim() {
    let (pool, database) = setup_ephemeral_pool("workflow_active_claim_terminal_race", 8).await;
    register_definition(&pool).await;
    let first = enqueue_active(&pool, None, "terminal-race", None, 1).await;
    let first_run_id = first.workflow_run().id;
    let mut claimed = claim_jobs(&pool, "worker-terminal-race", 30, 1)
        .await
        .expect("claim first workflow job");
    let job = claimed.pop().expect("first workflow job is claimable");
    let worker_id = job.worker_id.clone().expect("claimed job worker id");
    let barrier = Arc::new(Barrier::new(2));

    let completion_pool = pool.clone();
    let completion_barrier = barrier.clone();
    let completion = tokio::spawn(async move {
        completion_barrier.wait().await;
        complete_job_success(
            &completion_pool,
            job.id,
            job.run_number,
            job.attempt,
            &worker_id,
            None,
        )
        .await
    });
    let enqueue_pool = pool.clone();
    let enqueue_barrier = barrier.clone();
    let enqueue = tokio::spawn(async move {
        enqueue_barrier.wait().await;
        enqueue_active(&enqueue_pool, None, "terminal-race", None, 2).await
    });

    completion
        .await
        .expect("completion task should not panic")
        .expect("first workflow completion should succeed");
    let raced_outcome = enqueue.await.expect("enqueue task should not panic");
    let current_run_id = match raced_outcome {
        EnqueueActiveWorkflowOutcome::Inserted(run) => run.id,
        EnqueueActiveWorkflowOutcome::ExistingActive(run) => {
            assert_eq!(run.id, first_run_id);
            match enqueue_active(&pool, None, "terminal-race", None, 3).await {
                EnqueueActiveWorkflowOutcome::Inserted(run) => run.id,
                other => panic!("active key should be reusable after terminal commit: {other:?}"),
            }
        }
        EnqueueActiveWorkflowOutcome::ExistingIdempotent(_) => {
            panic!("unkeyed race cannot return an idempotent outcome")
        }
        _ => panic!("unexpected future active enqueue outcome"),
    };
    assert_ne!(current_run_id, first_run_id);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM workflow_active_claims WHERE active_key = 'terminal-race'",
        )
        .fetch_one(&pool)
        .await
        .expect("count terminal-race claims"),
        1
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn canceled_live_lease_holds_active_claim_until_reaper_observes_quiescence() {
    let (pool, database) = setup_ephemeral_pool("workflow_active_claim_cancel_quiescence", 8).await;
    register_definition(&pool).await;
    let first = enqueue_active(&pool, None, "cancel-cycle", None, 1).await;
    let first_run_id = first.workflow_run().id;
    let mut claimed = claim_jobs(&pool, "worker-cancel-quiescence", 30, 1)
        .await
        .expect("claim workflow job");
    let job = claimed.pop().expect("workflow job is claimable");
    let mut cancel_tx = pool.begin().await.expect("begin workflow cancellation");
    cancel_workflow_run_tx(
        &mut cancel_tx,
        first_run_id,
        None,
        Some("test.cancel"),
        None,
        None,
    )
    .await
    .expect("cancel workflow with live lease");
    cancel_tx
        .commit()
        .await
        .expect("commit workflow cancellation");

    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT release_pending
             FROM workflow_active_claims
             WHERE workflow_run_id = $1",
        )
        .bind(first_run_id)
        .fetch_one(&pool)
        .await
        .expect("load pending active claim")
    );
    let collision = enqueue_active(&pool, None, "cancel-cycle", None, 2).await;
    assert!(matches!(
        &collision,
        EnqueueActiveWorkflowOutcome::ExistingActive(_)
    ));
    assert_eq!(collision.workflow_run().id, first_run_id);

    sqlx::query(
        "UPDATE job_queue
         SET lease_expires_at = clock_timestamp() - interval '1 second'
         WHERE id = $1",
    )
    .bind(job.id)
    .execute(&pool)
    .await
    .expect("expire canceled lease marker");
    reap_expired_leases_with_diagnostics(&pool, 10, 1)
        .await
        .expect("run active-claim reaper cleanup");
    assert!(
        get_job_by_id(&pool, None, job.id)
            .await
            .expect("load canceled job")
            .is_some()
    );
    let next = enqueue_active(&pool, None, "cancel-cycle", None, 3).await;
    assert!(matches!(&next, EnqueueActiveWorkflowOutcome::Inserted(_)));
    assert_ne!(next.workflow_run().id, first_run_id);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn canceling_the_only_job_holds_active_claim_until_its_lease_quiesces() {
    let (pool, database) =
        setup_ephemeral_pool("workflow_active_claim_job_cancel_quiescence", 8).await;
    register_definition(&pool).await;
    let first = enqueue_active(&pool, None, "job-cancel-cycle", None, 1).await;
    let first_run_id = first.workflow_run().id;
    let job = claim_jobs(&pool, "worker-job-cancel-quiescence", 30, 1)
        .await
        .expect("claim workflow job")
        .pop()
        .expect("workflow job is claimable");

    cancel_job(&pool, None, job.id, Some("cancel only workflow job"))
        .await
        .expect("cancel workflow job");

    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT release_pending
             FROM workflow_active_claims
             WHERE workflow_run_id = $1",
        )
        .bind(first_run_id)
        .fetch_one(&pool)
        .await
        .expect("load deferred job-cancel active claim")
    );
    let collision = enqueue_active(&pool, None, "job-cancel-cycle", None, 2).await;
    assert!(matches!(
        &collision,
        EnqueueActiveWorkflowOutcome::ExistingActive(_)
    ));
    assert_eq!(collision.workflow_run().id, first_run_id);

    sqlx::query(
        "UPDATE job_queue
         SET lease_expires_at = clock_timestamp() - interval '1 second'
         WHERE id = $1",
    )
    .bind(job.id)
    .execute(&pool)
    .await
    .expect("expire canceled job lease marker");
    reap_expired_leases_with_diagnostics(&pool, 10, 1)
        .await
        .expect("release quiesced job-cancel active claim");

    let next = enqueue_active(&pool, None, "job-cancel-cycle", None, 3).await;
    assert!(matches!(&next, EnqueueActiveWorkflowOutcome::Inserted(_)));
    assert_ne!(next.workflow_run().id, first_run_id);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn reaper_reconciles_terminal_active_claims_marked_by_the_database_trigger() {
    let (pool, database) =
        setup_ephemeral_pool("workflow_active_claim_terminal_reconciliation", 8).await;
    register_definition(&pool).await;
    let first = enqueue_active(&pool, None, "reconcile-cycle", None, 1).await;
    let first_run_id = first.workflow_run().id;

    // Simulate a custom writer that terminalizes the run without calling the
    // Rust active-claim release hook. The migration trigger must still queue
    // the claim for bounded reaper cleanup.
    sqlx::query(
        "UPDATE workflow_runs
         SET status = 'CANCELED',
             finished_at = now()
         WHERE id = $1",
    )
    .bind(first_run_id)
    .execute(&pool)
    .await
    .expect("simulate terminal transition without active-claim hook");
    assert!(
        sqlx::query_scalar::<_, bool>(
            "SELECT release_pending
             FROM workflow_active_claims
             WHERE workflow_run_id = $1",
        )
        .bind(first_run_id)
        .fetch_one(&pool)
        .await
        .expect("load orphaned active claim")
    );

    let cleanup = reap_expired_leases_with_diagnostics(&pool, 10, 1)
        .await
        .expect("reconcile terminal active claim");
    assert_eq!(cleanup.workflow_active_claims_released, 1);
    assert_eq!(cleanup.execution_resource_claims_released, 0);
    assert!(cleanup.cleanup_errors.is_empty());

    let next = enqueue_active(&pool, None, "reconcile-cycle", None, 2).await;
    assert!(matches!(&next, EnqueueActiveWorkflowOutcome::Inserted(_)));
    assert_ne!(next.workflow_run().id, first_run_id);

    teardown_ephemeral_pool(pool, database).await;
}
