use runledger_core::jobs::{JobEventType, JobStatus, JobType};
use runledger_postgres::jobs::{
    JobCompletionUpdate, JobDefinitionUpsert, JobEnqueue, JobProgressUpdate, JobQueueRecord,
    claim_jobs, complete_job_success, complete_job_success_with_outcome, enqueue_job,
    get_job_by_id, list_job_events, update_job_progress, upsert_job_definition_tx,
};
use runledger_postgres::{DbPool, Error, QueryErrorCategory};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::types::Uuid;

const JOB_TYPE: &str = "jobs.test.completion_progress_validation";

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

async fn enqueue_test_job(pool: &DbPool) -> Uuid {
    let payload = json!({ "case": "invalid_completion_progress" });
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

async fn claim_one_job(pool: &DbPool, worker_id: &str) -> JobQueueRecord {
    claim_jobs(pool, worker_id, 30, 1)
        .await
        .expect("claim job")
        .pop()
        .expect("job should be claimed")
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
    assert_eq!(after.progress_done, before.progress_done);
    assert_eq!(after.progress_total, before.progress_total);
    assert_eq!(after.checkpoint, before.checkpoint);
    assert_eq!(after.output, before.output);
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

async fn succeeded_event_progress(pool: &DbPool, job_id: Uuid) -> (Option<i64>, Option<i64>) {
    let event = list_job_events(pool, None, job_id, 10, None)
        .await
        .expect("list job events")
        .into_iter()
        .find(|event| event.event_type == JobEventType::Succeeded)
        .expect("succeeded event exists");

    (event.progress_done, event.progress_total)
}

fn assert_invalid_completion_progress_error(error: Error) {
    match error {
        Error::QueryError(query_error) => {
            assert_eq!(query_error.category(), QueryErrorCategory::Validation);
            assert_eq!(query_error.code(), "job.invalid_completion_progress");
            assert_eq!(
                query_error.client_message(),
                "Job completion progress is invalid."
            );
        }
        other => panic!("expected validation query error, got {other:?}"),
    }
}

#[tokio::test]
async fn successful_completion_rejects_invalid_progress_without_mutating_lease() {
    let (pool, database) = setup_ephemeral_pool("postgres_completion_progress_validation", 4).await;
    register_job_definition(&pool).await;
    let job_id = enqueue_test_job(&pool).await;
    let job = claim_one_job(&pool, "worker-completion-progress").await;
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");

    let before = load_job(&pool, job_id).await;
    assert_eq!(before.status, JobStatus::Leased);
    assert_eq!(before.worker_id.as_deref(), Some(worker_id.as_str()));
    assert_event_types(
        &pool,
        job_id,
        &[JobEventType::Enqueued, JobEventType::Leased],
    )
    .await;

    for (progress_done, progress_total) in
        [(Some(-1), Some(1)), (Some(1), Some(-1)), (Some(2), Some(1))]
    {
        assert_invalid_completion_progress_error(
            complete_job_success(
                &pool,
                job.id,
                job.run_number,
                job.attempt,
                &worker_id,
                Some(&JobCompletionUpdate {
                    progress_done,
                    progress_total,
                    checkpoint: None,
                    output: None,
                }),
            )
            .await
            .expect_err("invalid completion progress should be rejected"),
        );
        assert_job_unchanged(&pool, job_id, &before).await;
        assert_event_types(
            &pool,
            job_id,
            &[JobEventType::Enqueued, JobEventType::Leased],
        )
        .await;
    }

    complete_job_success(
        &pool,
        job.id,
        job.run_number,
        job.attempt,
        &worker_id,
        Some(&JobCompletionUpdate {
            progress_done: Some(1),
            progress_total: None,
            checkpoint: None,
            output: None,
        }),
    )
    .await
    .expect("valid partial completion progress should be accepted");

    let completed = load_job(&pool, job_id).await;
    assert_eq!(completed.status, JobStatus::Succeeded);
    assert_eq!(completed.progress_done, Some(1));
    assert_eq!(completed.progress_total, None);
    assert_event_types(
        &pool,
        job_id,
        &[
            JobEventType::Enqueued,
            JobEventType::Leased,
            JobEventType::Succeeded,
        ],
    )
    .await;

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn complete_job_success_with_outcome_returns_coalesced_progress() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_success_outcome_coalesced_progress", 4).await;
    register_job_definition(&pool).await;

    let job_id = enqueue_test_job(&pool).await;
    let job = claim_one_job(&pool, "worker-success-outcome-existing").await;
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");
    update_job_progress(
        &pool,
        job.id,
        job.run_number,
        job.attempt,
        &worker_id,
        &JobProgressUpdate {
            stage: None,
            progress_done: Some(5),
            progress_total: Some(10),
            checkpoint: None,
        },
    )
    .await
    .expect("persist prior progress");

    let outcome = complete_job_success_with_outcome(
        &pool,
        job.id,
        job.run_number,
        job.attempt,
        &worker_id,
        None,
    )
    .await
    .expect("complete success with existing progress");

    let completed = load_job(&pool, job_id).await;
    assert_eq!(completed.status, JobStatus::Succeeded);
    assert_eq!(completed.progress_done, Some(5));
    assert_eq!(completed.progress_total, Some(10));
    assert_eq!(outcome.job_id, job_id);
    assert_eq!(outcome.progress_done, Some(5));
    assert_eq!(outcome.progress_total, Some(10));
    assert_eq!(
        succeeded_event_progress(&pool, job_id).await,
        (Some(5), Some(10))
    );

    let partial_job_id = enqueue_test_job(&pool).await;
    let partial_job = claim_one_job(&pool, "worker-success-outcome-partial").await;
    let partial_worker_id = partial_job
        .worker_id
        .clone()
        .expect("claimed partial job has worker id");
    update_job_progress(
        &pool,
        partial_job.id,
        partial_job.run_number,
        partial_job.attempt,
        &partial_worker_id,
        &JobProgressUpdate {
            stage: None,
            progress_done: Some(5),
            progress_total: Some(10),
            checkpoint: None,
        },
    )
    .await
    .expect("persist prior partial progress");

    let partial_outcome = complete_job_success_with_outcome(
        &pool,
        partial_job.id,
        partial_job.run_number,
        partial_job.attempt,
        &partial_worker_id,
        Some(&JobCompletionUpdate {
            progress_done: Some(7),
            progress_total: None,
            checkpoint: None,
            output: None,
        }),
    )
    .await
    .expect("complete success with partial progress");

    let partial_completed = load_job(&pool, partial_job_id).await;
    assert_eq!(partial_completed.status, JobStatus::Succeeded);
    assert_eq!(partial_completed.progress_done, Some(7));
    assert_eq!(partial_completed.progress_total, Some(10));
    assert_eq!(partial_outcome.job_id, partial_job_id);
    assert_eq!(partial_outcome.progress_done, Some(7));
    assert_eq!(partial_outcome.progress_total, Some(10));
    assert_eq!(
        succeeded_event_progress(&pool, partial_job_id).await,
        (Some(7), Some(10))
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn successful_completion_rejects_progress_invalid_after_existing_progress_is_applied() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_completion_stale_progress_validation", 4).await;
    register_job_definition(&pool).await;
    let job_id = enqueue_test_job(&pool).await;
    let job = claim_one_job(&pool, "worker-completion-stale-progress").await;
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");

    update_job_progress(
        &pool,
        job.id,
        job.run_number,
        job.attempt,
        &worker_id,
        &JobProgressUpdate {
            stage: None,
            progress_done: Some(5),
            progress_total: Some(10),
            checkpoint: None,
        },
    )
    .await
    .expect("persist prior progress");

    let before = load_job(&pool, job_id).await;
    assert_eq!(before.progress_done, Some(5));
    assert_eq!(before.progress_total, Some(10));

    assert_invalid_completion_progress_error(
        complete_job_success(
            &pool,
            job.id,
            job.run_number,
            job.attempt,
            &worker_id,
            Some(&JobCompletionUpdate {
                progress_done: Some(20),
                progress_total: None,
                checkpoint: None,
                output: None,
            }),
        )
        .await
        .expect_err("coalesced invalid completion progress should be rejected"),
    );

    assert_job_unchanged(&pool, job_id, &before).await;
    assert_event_types(
        &pool,
        job_id,
        &[
            JobEventType::Enqueued,
            JobEventType::Leased,
            JobEventType::Progress,
        ],
    )
    .await;

    teardown_ephemeral_pool(pool, database).await;
}
