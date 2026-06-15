use runledger_core::jobs::{JobEventType, JobStatus, JobType};
use runledger_postgres::jobs::{
    JobDefinitionUpsert, JobEnqueue, JobQueueRecord, claim_jobs, claim_jobs_for_types,
    claim_prestart_jobs, claim_prestart_jobs_for_types, enqueue_job, get_job_by_id, heartbeat_job,
    list_job_events, upsert_job_definition_tx,
};
use runledger_postgres::{DbPool, Error, QueryErrorCategory};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::types::Uuid;

const JOB_TYPE: &str = "jobs.test.lease_validation";

async fn register_job_definition(pool: &DbPool) {
    let mut tx = pool.begin().await.expect("begin setup tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(JOB_TYPE),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit setup tx");
}

async fn enqueue_test_job(pool: &DbPool, case_name: &str) -> Uuid {
    let payload = json!({ "case": case_name });
    enqueue_job(
        pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: None,
            payload: &payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: None,
        },
    )
    .await
    .expect("enqueue test job")
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
    register_job_definition(&pool).await;
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
async fn heartbeat_rejects_non_positive_lease_duration_without_mutating_lease() {
    let (pool, database) = setup_ephemeral_pool("postgres_heartbeat_lease_validation", 4).await;
    register_job_definition(&pool).await;
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
