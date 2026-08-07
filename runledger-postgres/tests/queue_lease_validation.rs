use runledger_core::jobs::{
    JobEventType, JobStatus, JobType, StepKey, WorkflowRunEnqueueBuilder,
    WorkflowStepEnqueueBuilder, WorkflowStepStatus, WorkflowType,
};
use runledger_postgres::jobs::{
    JobQueueRecord, claim_jobs, claim_jobs_for_types, claim_prestart_jobs,
    claim_prestart_jobs_for_types, enqueue_workflow_run, get_job_by_id, heartbeat_job,
    list_job_events, list_workflow_steps,
};
use runledger_postgres::{DbPool, Error, QueryErrorCategory};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::types::Uuid;

mod support;

use support::{enqueue_test_job as enqueue_shared_test_job, register_test_job_definition};

const JOB_TYPE: &str = "jobs.test.lease_validation";
const CLAIM_TRANSACTION_RESOURCE: &str = "claim-transaction-resource";

async fn enqueue_test_job(pool: &DbPool, case_name: &str) -> Uuid {
    let payload = json!({ "case": case_name });
    enqueue_shared_test_job(pool, JOB_TYPE, None, &payload).await
}

async fn load_job(pool: &DbPool, job_id: Uuid) -> JobQueueRecord {
    get_job_by_id(pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists")
}

async fn assert_job_unchanged(pool: &DbPool, job_id: Uuid, before: &JobQueueRecord) {
    let after = load_job(pool, job_id).await;
    assert_eq!(after.status, before.status);
    assert_eq!(after.attempt, before.attempt);
    assert_eq!(after.worker_id, before.worker_id);
    assert_eq!(after.lease_expires_at, before.lease_expires_at);
    assert_eq!(after.last_heartbeat_at, before.last_heartbeat_at);
    assert_eq!(after.started_at, before.started_at);
    assert_eq!(after.updated_at, before.updated_at);
}

async fn assert_jobs_unchanged(pool: &DbPool, jobs: &[(Uuid, JobQueueRecord)]) {
    for (job_id, before) in jobs {
        assert_job_unchanged(pool, *job_id, before).await;
        assert_event_types(pool, *job_id, &[JobEventType::Enqueued]).await;
    }
}

async fn assert_event_types(pool: &DbPool, job_id: Uuid, expected: &[JobEventType]) {
    let actual = list_job_events(pool, None, job_id, 10, None)
        .await
        .expect("list job events")
        .into_iter()
        .map(|event| event.event_type)
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

fn assert_invalid_lease_duration_error(error: Error) {
    match error {
        Error::QueryError(query_error) => {
            assert_eq!(query_error.category(), QueryErrorCategory::Validation);
            assert_eq!(query_error.code(), "job.invalid_lease_duration");
            assert_eq!(
                query_error.client_message(),
                "Job lease duration must be positive."
            );
        }
        other => panic!("expected validation query error, got {other:?}"),
    }
}

async fn assert_pending_unclaimed(pool: &DbPool, job_id: Uuid) -> JobQueueRecord {
    let job = load_job(pool, job_id).await;
    assert_eq!(job.status, JobStatus::Pending);
    assert_eq!(job.attempt, 0);
    assert!(job.worker_id.is_none());
    assert!(job.lease_expires_at.is_none());
    assert!(job.last_heartbeat_at.is_none());
    assert!(job.started_at.is_none());
    assert_event_types(pool, job_id, &[JobEventType::Enqueued]).await;
    job
}

#[tokio::test]
async fn claim_entrypoints_reject_non_positive_lease_duration_without_mutating_pending_jobs() {
    let (pool, database) = setup_ephemeral_pool("postgres_claim_lease_validation", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let mut pending_jobs = Vec::new();

    let job_id = enqueue_test_job(&pool, "claim_jobs_zero").await;
    let before = assert_pending_unclaimed(&pool, job_id).await;
    pending_jobs.push((job_id, before));
    assert_invalid_lease_duration_error(
        claim_jobs(&pool, "worker-claim-zero", 0, 1)
            .await
            .expect_err("zero claim lease duration should be rejected"),
    );
    assert_jobs_unchanged(&pool, &pending_jobs).await;

    let job_id = enqueue_test_job(&pool, "claim_jobs_for_types_negative").await;
    let before = assert_pending_unclaimed(&pool, job_id).await;
    pending_jobs.push((job_id, before));
    assert_invalid_lease_duration_error(
        claim_jobs_for_types(
            &pool,
            "worker-claim-types-negative",
            -1,
            1,
            &[JobType::new(JOB_TYPE)],
        )
        .await
        .expect_err("negative typed claim lease duration should be rejected"),
    );
    assert_jobs_unchanged(&pool, &pending_jobs).await;

    let job_id = enqueue_test_job(&pool, "claim_prestart_jobs_zero").await;
    let before = assert_pending_unclaimed(&pool, job_id).await;
    pending_jobs.push((job_id, before));
    assert_invalid_lease_duration_error(
        claim_prestart_jobs(&pool, "worker-prestart-zero", 0, 1)
            .await
            .expect_err("zero prestart claim lease duration should be rejected"),
    );
    assert_jobs_unchanged(&pool, &pending_jobs).await;

    let job_id = enqueue_test_job(&pool, "claim_prestart_jobs_for_types_negative").await;
    let before = assert_pending_unclaimed(&pool, job_id).await;
    pending_jobs.push((job_id, before));
    assert_invalid_lease_duration_error(
        claim_prestart_jobs_for_types(
            &pool,
            "worker-prestart-types-negative",
            -1,
            1,
            &[JobType::new(JOB_TYPE)],
        )
        .await
        .expect_err("negative typed prestart claim lease duration should be rejected"),
    );
    assert_jobs_unchanged(&pool, &pending_jobs).await;

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn claim_rolls_back_all_phase_writes_when_event_recording_fails() {
    let (pool, database) = setup_ephemeral_pool("postgres_claim_transaction_rollback", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let payload = json!({"case": "claim-transaction-rollback"});
    let metadata = json!({"case": "claim-transaction-rollback"});
    let step =
        WorkflowStepEnqueueBuilder::new(StepKey::new("root"), JobType::new(JOB_TYPE), &payload)
            .execution_resource(CLAIM_TRANSACTION_RESOURCE)
            .try_build()
            .expect("build resource-bound workflow step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.claim_transaction_rollback"),
        &metadata,
    )
    .step(step)
    .try_build()
    .expect("build claim rollback workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue claim rollback workflow");
    let initial_step = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list initial workflow step")
        .pop()
        .expect("one workflow step");
    let job_id = initial_step.job_id.expect("root job should be released");
    assert_eq!(initial_step.status, WorkflowStepStatus::Enqueued);

    sqlx::query(
        "CREATE FUNCTION assert_claim_attempt_order()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             IF NOT EXISTS (
                 SELECT 1
                 FROM job_queue
                 WHERE id = NEW.job_id
                   AND status = 'LEASED'
             ) OR NOT EXISTS (
                 SELECT 1
                 FROM workflow_steps
                 WHERE job_id = NEW.job_id
                   AND status = 'RUNNING'
             ) THEN
                 RAISE EXCEPTION 'claim attempt side effects were recorded out of order';
             END IF;
             RETURN NEW;
         END;
         $$",
    )
    .execute(&pool)
    .await
    .expect("create claim attempt order assertion");
    sqlx::query(
        "CREATE TRIGGER assert_claim_attempt_order
         BEFORE INSERT ON job_attempts
         FOR EACH ROW
         EXECUTE FUNCTION assert_claim_attempt_order()",
    )
    .execute(&pool)
    .await
    .expect("create claim attempt order trigger");
    sqlx::query(
        "CREATE FUNCTION fail_claim_event_after_order_checks()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             IF NEW.event_type = 'LEASED' THEN
                 IF NOT EXISTS (
                     SELECT 1
                     FROM job_attempts
                     WHERE job_id = NEW.job_id
                       AND run_number = NEW.run_number
                       AND attempt = NEW.attempt
                 ) THEN
                     RAISE EXCEPTION 'claim event was recorded before its attempt';
                 END IF;
                 RAISE EXCEPTION 'injected claim event failure';
             END IF;
             RETURN NEW;
         END;
         $$",
    )
    .execute(&pool)
    .await
    .expect("create claim event failure function");
    sqlx::query(
        "CREATE TRIGGER fail_claim_event_after_order_checks
         BEFORE INSERT ON job_events
         FOR EACH ROW
         EXECUTE FUNCTION fail_claim_event_after_order_checks()",
    )
    .execute(&pool)
    .await
    .expect("create claim event failure trigger");

    let error = claim_jobs(&pool, "worker-claim-transaction", 30, 1)
        .await
        .expect_err("event failure should roll back the claim transaction");
    let Error::QueryError(query_error) = error else {
        panic!("expected query error from injected event failure");
    };
    assert_eq!(
        query_error.internal_message(),
        "claim jobs event insert: injected claim event failure"
    );

    let job = load_job(&pool, job_id).await;
    assert_eq!(job.status, JobStatus::Pending);
    assert_eq!(job.attempt, 0);
    assert!(job.worker_id.is_none());
    assert!(job.lease_expires_at.is_none());
    assert!(job.last_heartbeat_at.is_none());
    assert!(job.started_at.is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*)
             FROM job_execution_resource_claims
             WHERE job_id = $1",
        )
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("count rolled-back resource claims"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*)
             FROM job_attempts
             WHERE job_id = $1",
        )
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("count rolled-back attempts"),
        0
    );
    assert_event_types(&pool, job_id, &[JobEventType::Enqueued]).await;

    let step = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list rolled-back workflow step")
        .pop()
        .expect("one workflow step");
    assert_eq!(step.status, WorkflowStepStatus::Enqueued);
    assert!(step.started_at.is_none());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn heartbeat_rejects_non_positive_lease_duration_without_mutating_lease() {
    let (pool, database) = setup_ephemeral_pool("postgres_heartbeat_lease_validation", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let job_id = enqueue_test_job(&pool, "heartbeat_invalid_ttl").await;
    let mut claimed = claim_jobs(&pool, "worker-heartbeat", 30, 1)
        .await
        .expect("claim job");
    let job = claimed.pop().expect("job should be claimed");
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");

    let before = load_job(&pool, job_id).await;
    assert_eq!(before.status, JobStatus::Leased);
    assert_eq!(before.attempt, 1);
    assert_eq!(before.worker_id.as_deref(), Some(worker_id.as_str()));
    assert!(before.lease_expires_at.is_some());
    assert!(before.last_heartbeat_at.is_some());
    assert_event_types(
        &pool,
        job_id,
        &[JobEventType::Enqueued, JobEventType::Leased],
    )
    .await;

    for lease_duration_seconds in [0, -1] {
        assert_invalid_lease_duration_error(
            heartbeat_job(
                &pool,
                job.id,
                job.run_number,
                job.attempt,
                &worker_id,
                lease_duration_seconds,
            )
            .await
            .expect_err("non-positive heartbeat lease duration should be rejected"),
        );
        assert_job_unchanged(&pool, job_id, &before).await;
        assert_event_types(
            &pool,
            job_id,
            &[JobEventType::Enqueued, JobEventType::Leased],
        )
        .await;
    }

    teardown_ephemeral_pool(pool, database).await;
}
