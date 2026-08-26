mod support;

use std::time::Duration;

use runledger_core::jobs::JobFailureKind;
use runledger_postgres::jobs::{
    JobContinuationUpdate, JobFailureUpdate, complete_job_continuation, complete_job_failure,
    complete_job_success,
};
use runledger_postgres::{Error, QueryErrorCategory};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::time::timeout;

use support::{claim_one_job, enqueue_test_job, register_test_job_definition};

const JOB_TYPE: &str = "jobs.test.lifecycle_completion_timeout";

fn assert_lock_timeout_error(error: Error) {
    match error {
        Error::QueryError(query_error) => {
            assert_eq!(query_error.category(), QueryErrorCategory::Internal);
            assert_eq!(query_error.sqlstate(), Some("55P03"));
        }
        other => panic!("expected lock-timeout query error, got {other:?}"),
    }
}

async fn enqueue_and_claim(
    pool: &PgPool,
    worker_id: &str,
) -> runledger_postgres::jobs::JobQueueRecord {
    enqueue_test_job(pool, JOB_TYPE, None, &json!({"worker_id": worker_id})).await;
    claim_one_job(pool, worker_id).await
}

async fn lock_job_row<'a>(
    pool: &'a PgPool,
    job_id: sqlx::types::Uuid,
) -> sqlx::Transaction<'a, sqlx::Postgres> {
    let mut blocker = pool.begin().await.expect("begin job-row blocker");
    sqlx::query("SELECT id FROM job_queue WHERE id = $1 FOR UPDATE")
        .bind(job_id)
        .fetch_one(&mut *blocker)
        .await
        .expect("hold job row lock");
    blocker
}

#[tokio::test]
async fn completion_row_lock_waits_preserve_stricter_database_timeout() {
    let (pool, database) = setup_ephemeral_pool("postgres_completion_timeouts", 4).await;
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("read PostgreSQL server_version");
    let server_version_num =
        sqlx::query_scalar::<_, i32>("SELECT current_setting('server_version_num')::int")
            .fetch_one(&pool)
            .await
            .expect("read PostgreSQL server_version_num");
    eprintln!(
        "completion timeout regression PostgreSQL server_version={server_version}, \
         server_version_num={server_version_num}"
    );
    register_test_job_definition(&pool, JOB_TYPE).await;

    let success = enqueue_and_claim(&pool, "worker-completion-timeout-success").await;
    let failure = enqueue_and_claim(&pool, "worker-completion-timeout-failure").await;
    let continuation = enqueue_and_claim(&pool, "worker-completion-timeout-continuation").await;

    let completion_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database.url())
        .await
        .expect("connect completion pool");
    sqlx::query("SET SESSION lock_timeout = '100ms'")
        .execute(&completion_pool)
        .await
        .expect("set strict completion lock timeout");

    let success_worker_id = success.worker_id.as_deref().expect("success worker id");
    let success_blocker = lock_job_row(&pool, success.id).await;
    let success_error = timeout(
        Duration::from_secs(2),
        complete_job_success(
            &completion_pool,
            success.id,
            success.run_number,
            success.attempt,
            success_worker_id,
            None,
        ),
    )
    .await
    .expect("success completion lock wait should be bounded")
    .expect_err("success completion should report lock timeout");
    assert_lock_timeout_error(success_error);
    success_blocker
        .rollback()
        .await
        .expect("release success blocker");

    let failure_worker_id = failure.worker_id.as_deref().expect("failure worker id");
    let failure_blocker = lock_job_row(&pool, failure.id).await;
    let failure_update = JobFailureUpdate::new(
        JobFailureKind::Retryable,
        "job.test.completion_timeout",
        "completion timeout regression",
        Some(1_000),
    );
    let failure_error = timeout(
        Duration::from_secs(2),
        complete_job_failure(
            &completion_pool,
            failure.id,
            failure.run_number,
            failure.attempt,
            failure_worker_id,
            &failure_update,
        ),
    )
    .await
    .expect("failure completion lock wait should be bounded")
    .expect_err("failure completion should report lock timeout");
    assert_lock_timeout_error(failure_error);
    failure_blocker
        .rollback()
        .await
        .expect("release failure blocker");

    let continuation_worker_id = continuation
        .worker_id
        .as_deref()
        .expect("continuation worker id");
    let continuation_blocker = lock_job_row(&pool, continuation.id).await;
    let continuation_error = timeout(
        Duration::from_secs(2),
        complete_job_continuation(
            &completion_pool,
            continuation.id,
            continuation.run_number,
            continuation.attempt,
            continuation_worker_id,
            &JobContinuationUpdate {
                delay: Duration::ZERO,
                progress_done: None,
                progress_total: None,
                checkpoint: None,
            },
        ),
    )
    .await
    .expect("continuation completion lock wait should be bounded")
    .expect_err("continuation completion should report lock timeout");
    assert_lock_timeout_error(continuation_error);
    continuation_blocker
        .rollback()
        .await
        .expect("release continuation blocker");

    completion_pool.close().await;
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn completion_restores_caller_timeouts_after_job_row_acquisition() {
    let (pool, database) = setup_ephemeral_pool("postgres_completion_timeout_scope", 4).await;
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("read PostgreSQL server_version");
    let server_version_num =
        sqlx::query_scalar::<_, i32>("SELECT current_setting('server_version_num')::int")
            .fetch_one(&pool)
            .await
            .expect("read PostgreSQL server_version_num");
    eprintln!(
        "completion timeout scope regression PostgreSQL server_version={server_version}, \
         server_version_num={server_version_num}"
    );
    register_test_job_definition(&pool, JOB_TYPE).await;

    for statement in [
        "CREATE TABLE runledger_test_completion_timeout_observations (
            lock_timeout text NOT NULL,
            transaction_timeout text NOT NULL
         )",
        "CREATE FUNCTION runledger_test_observe_completion_timeouts()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             INSERT INTO runledger_test_completion_timeout_observations (
                 lock_timeout,
                 transaction_timeout
             )
             VALUES (
                 current_setting('lock_timeout'),
                 current_setting('transaction_timeout')
             );
             RETURN NEW;
         END;
         $$",
        "CREATE TRIGGER runledger_test_observe_completion_timeouts
         AFTER UPDATE OF finished_at ON job_attempts
         FOR EACH ROW
         WHEN (NEW.finished_at IS NOT NULL)
         EXECUTE FUNCTION runledger_test_observe_completion_timeouts()",
    ] {
        sqlx::query(statement)
            .execute(&pool)
            .await
            .expect("install completion timeout observation trigger");
    }

    let success = enqueue_and_claim(&pool, "worker-completion-timeout-scope-success").await;
    let failure = enqueue_and_claim(&pool, "worker-completion-timeout-scope-failure").await;
    let continuation =
        enqueue_and_claim(&pool, "worker-completion-timeout-scope-continuation").await;
    let completion_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database.url())
        .await
        .expect("connect timeout-scope completion pool");
    sqlx::query(
        "SELECT
            set_config('lock_timeout', '1min', false),
            set_config('transaction_timeout', '2min', false)",
    )
    .execute(&completion_pool)
    .await
    .expect("set caller completion timeouts");

    complete_job_success(
        &completion_pool,
        success.id,
        success.run_number,
        success.attempt,
        success.worker_id.as_deref().expect("success worker id"),
        None,
    )
    .await
    .expect("complete job after scoped row-lock timeout");

    let failure_update = JobFailureUpdate::new(
        JobFailureKind::Retryable,
        "job.test.completion_timeout_scope",
        "completion timeout scope regression",
        Some(1_000),
    );
    complete_job_failure(
        &completion_pool,
        failure.id,
        failure.run_number,
        failure.attempt,
        failure.worker_id.as_deref().expect("failure worker id"),
        &failure_update,
    )
    .await
    .expect("fail job after scoped row-lock timeout");

    complete_job_continuation(
        &completion_pool,
        continuation.id,
        continuation.run_number,
        continuation.attempt,
        continuation
            .worker_id
            .as_deref()
            .expect("continuation worker id"),
        &JobContinuationUpdate {
            delay: Duration::ZERO,
            progress_done: None,
            progress_total: None,
            checkpoint: None,
        },
    )
    .await
    .expect("continue job after scoped row-lock timeout");

    let (observed, unexpected) = sqlx::query_as::<_, (i64, i64)>(
        "SELECT
            count(*) FILTER (
                WHERE lock_timeout = '1min'
                  AND transaction_timeout = '2min'
            ),
            count(*) FILTER (
                WHERE lock_timeout <> '1min'
                   OR transaction_timeout <> '2min'
            )
         FROM runledger_test_completion_timeout_observations",
    )
    .fetch_one(&pool)
    .await
    .expect("read downstream completion timeout settings");
    assert_eq!(observed, 3);
    assert_eq!(unexpected, 0);

    completion_pool.close().await;
    teardown_ephemeral_pool(pool, database).await;
}
