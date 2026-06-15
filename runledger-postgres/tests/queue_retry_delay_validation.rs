use runledger_core::jobs::{JobEventType, JobFailureKind, JobStatus, JobType};
use runledger_postgres::jobs::{
    JobDefinitionUpsert, JobEnqueue, JobFailureUpdate, JobQueueRecord, claim_jobs,
    complete_job_failure, enqueue_job, get_job_by_id, list_job_events, reap_expired_leases,
    reap_expired_leases_with_terminal_records, upsert_job_definition_tx,
};
use runledger_postgres::{DbPool, Error, QueryErrorCategory};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::types::Uuid;

const JOB_TYPE: &str = "jobs.test.retry_delay_validation";

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
    assert_eq!(after.next_run_at, before.next_run_at);
    assert_eq!(after.worker_id, before.worker_id);
    assert_eq!(after.lease_expires_at, before.lease_expires_at);
    assert_eq!(after.last_heartbeat_at, before.last_heartbeat_at);
    assert_eq!(after.started_at, before.started_at);
    assert_eq!(after.finished_at, before.finished_at);
    assert_eq!(after.status_reason, before.status_reason);
    assert_eq!(after.last_error_code, before.last_error_code);
    assert_eq!(after.last_error_message, before.last_error_message);
    assert_eq!(after.updated_at, before.updated_at);
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

fn assert_invalid_retry_delay_error(error: Error) {
    match error {
        Error::QueryError(query_error) => {
            assert_eq!(query_error.category(), QueryErrorCategory::Validation);
            assert_eq!(query_error.code(), "job.invalid_retry_delay");
            assert_eq!(
                query_error.client_message(),
                "Job retry delay must be positive."
            );
        }
        other => panic!("expected validation query error, got {other:?}"),
    }
}

async fn claim_one_job(pool: &DbPool, worker_id: &str) -> JobQueueRecord {
    claim_jobs(pool, worker_id, 30, 1)
        .await
        .expect("claim job")
        .pop()
        .expect("job should be claimed")
}

async fn expire_job_lease(pool: &DbPool, job_id: Uuid) {
    sqlx::query(
        "UPDATE job_queue SET lease_expires_at = now() - interval '10 seconds' WHERE id = $1",
    )
    .bind(job_id)
    .execute(pool)
    .await
    .expect("expire job lease");
}

#[tokio::test]
async fn retryable_failure_rejects_invalid_retry_delay_without_mutating_lease() {
    let (pool, database) = setup_ephemeral_pool("postgres_retry_delay_failure", 4).await;
    register_job_definition(&pool).await;
    let job_id = enqueue_test_job(&pool, "failure_invalid_retry_delay").await;
    let job = claim_one_job(&pool, "worker-retry-delay-failure").await;
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");

    let before = load_job(&pool, job_id).await;
    assert_eq!(before.status, JobStatus::Leased);
    assert_eq!(before.attempt, 1);
    assert_eq!(before.worker_id.as_deref(), Some(worker_id.as_str()));
    assert_event_types(
        &pool,
        job_id,
        &[JobEventType::Enqueued, JobEventType::Leased],
    )
    .await;

    for retry_delay_ms in [None, Some(0), Some(-1)] {
        assert_invalid_retry_delay_error(
            complete_job_failure(
                &pool,
                job.id,
                job.run_number,
                job.attempt,
                &worker_id,
                &JobFailureUpdate {
                    kind: JobFailureKind::Retryable,
                    code: "job.test.retry_delay_invalid",
                    message: "retryable failure should be rejected",
                    retry_delay_ms,
                },
            )
            .await
            .expect_err("invalid retry delay should be rejected"),
        );
        assert_job_unchanged(&pool, job_id, &before).await;
        assert_event_types(
            &pool,
            job_id,
            &[JobEventType::Enqueued, JobEventType::Leased],
        )
        .await;
    }

    complete_job_failure(
        &pool,
        job.id,
        job.run_number,
        job.attempt,
        &worker_id,
        &JobFailureUpdate {
            kind: JobFailureKind::Terminal,
            code: "job.test.terminal_without_retry_delay",
            message: "terminal failure does not need retry delay",
            retry_delay_ms: None,
        },
    )
    .await
    .expect("terminal failure should allow absent retry delay");

    let terminal = load_job(&pool, job_id).await;
    assert_eq!(terminal.status, JobStatus::DeadLettered);
    assert_event_types(
        &pool,
        job_id,
        &[
            JobEventType::Enqueued,
            JobEventType::Leased,
            JobEventType::Failed,
            JobEventType::DeadLettered,
        ],
    )
    .await;

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn expired_lease_reapers_reject_invalid_retry_delay_without_mutating_lease() {
    let (pool, database) = setup_ephemeral_pool("postgres_retry_delay_reaper", 4).await;
    register_job_definition(&pool).await;
    let job_id = enqueue_test_job(&pool, "reaper_invalid_retry_delay").await;
    let job = claim_one_job(&pool, "worker-retry-delay-reaper").await;
    assert_eq!(job.id, job_id);
    expire_job_lease(&pool, job_id).await;

    let before = load_job(&pool, job_id).await;
    assert_eq!(before.status, JobStatus::Leased);
    assert_eq!(before.attempt, 1);
    assert!(before.lease_expires_at.is_some());
    assert_event_types(
        &pool,
        job_id,
        &[JobEventType::Enqueued, JobEventType::Leased],
    )
    .await;

    for default_retry_delay_ms in [0, -1] {
        assert_invalid_retry_delay_error(
            reap_expired_leases(&pool, 1, default_retry_delay_ms)
                .await
                .expect_err("invalid reaper retry delay should be rejected"),
        );
        assert_job_unchanged(&pool, job_id, &before).await;
        assert_event_types(
            &pool,
            job_id,
            &[JobEventType::Enqueued, JobEventType::Leased],
        )
        .await;
    }

    for default_retry_delay_ms in [0, -1] {
        assert_invalid_retry_delay_error(
            reap_expired_leases_with_terminal_records(&pool, 1, default_retry_delay_ms)
                .await
                .expect_err("invalid terminal-record reaper retry delay should be rejected"),
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
