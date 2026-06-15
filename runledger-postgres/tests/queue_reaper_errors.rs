use chrono::Utc;
use runledger_core::jobs::{
    JobEventType, JobStatus, JobType, StepKey, WorkflowRunEnqueueBuilder,
    WorkflowStepEnqueueBuilder, WorkflowType,
};
use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    JobDefinitionUpsert, JobEnqueue, JobQueueRecord, claim_jobs_for_types, enqueue_job,
    enqueue_workflow_run, get_job_by_id, list_job_events, list_workflow_steps,
    reap_expired_leases_with_diagnostics, upsert_job_definition_tx,
};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::types::Uuid;

const WORKFLOW_JOB_TYPE: &str = "jobs.test.reaper_deferred_error.workflow";
const HEALTHY_JOB_TYPE: &str = "jobs.test.reaper_deferred_error.healthy";
const WORKFLOW_TYPE: &str = "workflow.test.reaper_deferred_error";
const WORKFLOW_RUN_RELEASE_LOCK_NAMESPACE: u64 = 0x7275_6e6c_9e37_79b9;

fn workflow_run_release_lock_key(workflow_run_id: Uuid) -> i64 {
    let value = workflow_run_id.as_u128();
    let folded = (value >> 64) as u64 ^ value as u64 ^ WORKFLOW_RUN_RELEASE_LOCK_NAMESPACE;
    folded as i64
}

async fn register_job_definition(pool: &DbPool, job_type: JobType<'static>, max_attempts: i32) {
    let mut tx = pool.begin().await.expect("begin setup tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type,
            version: 1,
            max_attempts,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit setup tx");
}

async fn enqueue_workflow_job(pool: &DbPool) -> (Uuid, Uuid) {
    let payload = json!({ "case": "workflow-row-error" });
    let metadata = json!({});
    let root = WorkflowStepEnqueueBuilder::new(
        StepKey::new("root"),
        JobType::new(WORKFLOW_JOB_TYPE),
        &payload,
    )
    .max_attempts(1)
    .try_build()
    .expect("build root step");
    let workflow = WorkflowRunEnqueueBuilder::new(WorkflowType::new(WORKFLOW_TYPE), &metadata)
        .step(root)
        .try_build()
        .expect("build workflow");
    let run = enqueue_workflow_run(pool, &workflow)
        .await
        .expect("enqueue workflow");
    let root_job_id = list_workflow_steps(pool, None, run.id)
        .await
        .expect("list workflow steps")
        .into_iter()
        .find(|step| step.step_key.as_str() == "root")
        .and_then(|step| step.job_id)
        .expect("root job should be released");

    (run.id, root_job_id)
}

async fn enqueue_healthy_job(pool: &DbPool) -> Uuid {
    enqueue_job(
        pool,
        &JobEnqueue {
            job_type: JobType::new(HEALTHY_JOB_TYPE),
            organization_id: None,
            payload: &json!({ "case": "healthy-row" }),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: None,
        },
    )
    .await
    .expect("enqueue healthy job")
}

async fn claim_one(pool: &DbPool, worker_id: &str, job_type: JobType<'static>) -> JobQueueRecord {
    claim_jobs_for_types(pool, worker_id, 30, 1, &[job_type])
        .await
        .expect("claim job")
        .pop()
        .expect("one job should be claimed")
}

async fn expire_leases(pool: &DbPool, failed_job_id: Uuid, healthy_job_id: Uuid) {
    sqlx::query(
        "UPDATE job_queue
         SET lease_expires_at = CASE
            WHEN id = $1 THEN now() - interval '20 seconds'
            ELSE now() - interval '10 seconds'
         END
         WHERE id IN ($1, $2)",
    )
    .bind(failed_job_id)
    .bind(healthy_job_id)
    .execute(pool)
    .await
    .expect("expire leases");
}

async fn load_job(pool: &DbPool, job_id: Uuid) -> JobQueueRecord {
    get_job_by_id(pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists")
}

async fn event_types(pool: &DbPool, job_id: Uuid) -> Vec<JobEventType> {
    list_job_events(pool, None, job_id, 10, None)
        .await
        .expect("list job events")
        .into_iter()
        .map(|event| event.event_type)
        .collect()
}

#[tokio::test]
async fn expired_lease_reaper_reports_deferred_row_error_and_continues_batch() {
    let (pool, database) = setup_ephemeral_pool("postgres_reaper_deferred_row_error", 2).await;
    register_job_definition(&pool, JobType::new(WORKFLOW_JOB_TYPE), 3).await;
    register_job_definition(&pool, JobType::new(HEALTHY_JOB_TYPE), 3).await;

    let (workflow_run_id, failed_job_id) = enqueue_workflow_job(&pool).await;
    let healthy_job_id = enqueue_healthy_job(&pool).await;
    let failed_claim = claim_one(
        &pool,
        "worker-reaper-row-error",
        JobType::new(WORKFLOW_JOB_TYPE),
    )
    .await;
    let healthy_claim = claim_one(
        &pool,
        "worker-reaper-healthy",
        JobType::new(HEALTHY_JOB_TYPE),
    )
    .await;
    assert_eq!(failed_claim.id, failed_job_id);
    assert_eq!(healthy_claim.id, healthy_job_id);
    expire_leases(&pool, failed_job_id, healthy_job_id).await;

    let mut release_lock_tx = pool.begin().await.expect("begin release lock tx");
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(workflow_run_release_lock_key(workflow_run_id))
        .execute(&mut *release_lock_tx)
        .await
        .expect("hold exclusive workflow release lock");

    let reaper_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect reaper pool");
    sqlx::query("SET lock_timeout = '100ms'")
        .execute(&reaper_pool)
        .await
        .expect("set short reaper lock timeout");

    let result = reap_expired_leases_with_diagnostics(&reaper_pool, 2, 60_000)
        .await
        .expect("reap expired leases");

    release_lock_tx
        .rollback()
        .await
        .expect("release workflow lock");
    reaper_pool.close().await;

    assert_eq!(
        result.summary.processed, 1,
        "healthy row should still be processed"
    );
    assert!(result.summary.terminal_dead_lettered.is_empty());
    assert_eq!(result.deferred_row_error_count, 1);
    let deferred_error = result
        .deferred_row_errors
        .first()
        .expect("deferred row error should be sampled");
    assert_eq!(deferred_error.job_id, failed_job_id);
    assert_eq!(deferred_error.run_number, failed_claim.run_number);
    assert_eq!(deferred_error.attempt, failed_claim.attempt);
    assert_eq!(deferred_error.error_code, "workflow.release_conflict");
    assert_eq!(
        deferred_error.error_message,
        "Workflow step release conflicted with another workflow mutation."
    );
    assert_eq!(deferred_error.sqlstate.as_deref(), Some("55P03"));

    let deferred_job = load_job(&pool, failed_job_id).await;
    assert_eq!(deferred_job.status, JobStatus::Leased);
    assert_eq!(deferred_job.attempt, failed_claim.attempt);
    assert_eq!(deferred_job.worker_id, failed_claim.worker_id);
    assert!(
        deferred_job.lease_expires_at.expect("deferred lease") > Utc::now(),
        "failed row should be pushed out of the immediate expired-lease window"
    );
    assert_eq!(
        event_types(&pool, failed_job_id).await,
        [JobEventType::Enqueued, JobEventType::Leased]
    );

    let healthy_job = load_job(&pool, healthy_job_id).await;
    assert_eq!(healthy_job.status, JobStatus::Pending);
    assert_eq!(healthy_job.attempt, healthy_claim.attempt);
    assert!(healthy_job.lease_expires_at.is_none());
    assert_eq!(
        event_types(&pool, healthy_job_id).await,
        [
            JobEventType::Enqueued,
            JobEventType::Leased,
            JobEventType::Failed,
            JobEventType::RetryScheduled,
        ]
    );

    teardown_ephemeral_pool(pool, database).await;
}
