use runledger_postgres::jobs::{
    CompareAndReplaySucceededJob, CompareAndReplaySucceededJobOutcome, CompareAndRequeueJob,
    CompareAndRequeueJobOutcome, JobRequeueStatePolicy, JobScope, RequeueableJobStatus,
    compare_and_replay_succeeded_job, compare_and_replay_succeeded_job_tx, compare_and_requeue_job,
    compare_and_requeue_job_tx,
};
use runledger_postgres::{Error, QueryErrorCategory};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use sqlx::postgres::PgPoolOptions;
use sqlx::types::Uuid;

fn assert_contextual_begin_error(error: Error, operation: &str) {
    let Error::QueryError(error) = error else {
        panic!("expected contextual query error for {operation}");
    };
    assert_eq!(error.category(), QueryErrorCategory::Internal);
    assert_eq!(error.code(), "db.query_failed");
    assert!(
        error.internal_message().contains(operation),
        "begin error should identify {operation}: {}",
        error.internal_message()
    );
}

fn assert_isolation_error(error: Error, expected_code: &str) {
    let Error::QueryError(error) = error else {
        panic!("expected transaction isolation validation error");
    };
    assert_eq!(error.category(), QueryErrorCategory::Validation);
    assert_eq!(error.code(), expected_code);
}

#[tokio::test]
async fn owned_transaction_wrappers_validate_before_classifying_begin_failures() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://runledger:runledger@127.0.0.1/runledger")
        .expect("create lazy test pool");
    pool.close().await;

    let requeue_error = compare_and_requeue_job(
        &pool,
        CompareAndRequeueJob {
            scope: JobScope::Global,
            job_id: Uuid::nil(),
            expected_status: RequeueableJobStatus::Canceled,
            expected_run_number: 1,
            state_policy: JobRequeueStatePolicy::ResetProgressAndCheckpoint,
            reason: "closed-pool classification",
        },
    )
    .await
    .expect_err("closed pool should reject compare-and-requeue");
    assert_contextual_begin_error(requeue_error, "compare-and-requeue");

    let invalid_replay_error = compare_and_replay_succeeded_job(
        &pool,
        CompareAndReplaySucceededJob {
            scope: JobScope::Global,
            source_job_id: Uuid::nil(),
            expected_run_number: 1,
            replay_request_key: " ",
            reason: "closed-pool validation",
        },
    )
    .await
    .expect_err("invalid replay request should fail without acquiring from the closed pool");
    let Error::QueryError(invalid_replay_error) = invalid_replay_error else {
        panic!("expected replay validation error");
    };
    assert_eq!(
        invalid_replay_error.category(),
        QueryErrorCategory::Validation
    );
    assert_eq!(invalid_replay_error.code(), "job.replay_request_key_blank");

    let replay_error = compare_and_replay_succeeded_job(
        &pool,
        CompareAndReplaySucceededJob {
            scope: JobScope::Global,
            source_job_id: Uuid::nil(),
            expected_run_number: 1,
            replay_request_key: "closed-pool-classification",
            reason: "closed-pool classification",
        },
    )
    .await
    .expect_err("closed pool should reject successful replay");
    assert_contextual_begin_error(replay_error, "successful job replay");
}

#[tokio::test]
async fn owned_wrappers_override_session_isolation_while_tx_apis_enforce_it() {
    let (pool, database) = setup_ephemeral_pool("postgres_owned_wrapper_isolation", 1).await;
    let missing_job_id = Uuid::from_u128(1);

    let mut connection = pool.acquire().await.expect("acquire sole connection");
    sqlx::query("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *connection)
        .await
        .expect("set repeatable-read session default");
    drop(connection);

    let requeue = compare_and_requeue_job(
        &pool,
        CompareAndRequeueJob {
            scope: JobScope::Global,
            job_id: missing_job_id,
            expected_status: RequeueableJobStatus::Canceled,
            expected_run_number: 1,
            state_policy: JobRequeueStatePolicy::ResetProgressAndCheckpoint,
            reason: "owned isolation override",
        },
    )
    .await
    .expect("owned requeue should establish READ COMMITTED");
    assert!(matches!(requeue, CompareAndRequeueJobOutcome::NotFound));

    let replay = compare_and_replay_succeeded_job(
        &pool,
        CompareAndReplaySucceededJob {
            scope: JobScope::Global,
            source_job_id: missing_job_id,
            expected_run_number: 1,
            replay_request_key: "owned-isolation-override",
            reason: "owned isolation override",
        },
    )
    .await
    .expect("owned replay should establish READ COMMITTED");
    assert!(matches!(
        replay,
        CompareAndReplaySucceededJobOutcome::NotFound
    ));

    let mut requeue_tx = pool.begin().await.expect("begin caller-owned requeue");
    let requeue_error = compare_and_requeue_job_tx(
        &mut requeue_tx,
        CompareAndRequeueJob {
            scope: JobScope::Global,
            job_id: missing_job_id,
            expected_status: RequeueableJobStatus::Canceled,
            expected_run_number: 1,
            state_policy: JobRequeueStatePolicy::ResetProgressAndCheckpoint,
            reason: "caller isolation enforcement",
        },
    )
    .await
    .expect_err("caller-owned requeue must reject repeatable read");
    assert_isolation_error(
        requeue_error,
        "job.compare_and_requeue_unsupported_isolation",
    );
    requeue_tx
        .rollback()
        .await
        .expect("roll back rejected requeue");

    let mut replay_tx = pool.begin().await.expect("begin caller-owned replay");
    let replay_validation_error = compare_and_replay_succeeded_job_tx(
        &mut replay_tx,
        CompareAndReplaySucceededJob {
            scope: JobScope::Global,
            source_job_id: missing_job_id,
            expected_run_number: 1,
            replay_request_key: " ",
            reason: "caller validation enforcement",
        },
    )
    .await
    .expect_err("caller-owned replay must independently validate its request");
    let Error::QueryError(replay_validation_error) = replay_validation_error else {
        panic!("expected caller-owned replay validation error");
    };
    assert_eq!(
        replay_validation_error.category(),
        QueryErrorCategory::Validation
    );
    assert_eq!(
        replay_validation_error.code(),
        "job.replay_request_key_blank"
    );

    let replay_error = compare_and_replay_succeeded_job_tx(
        &mut replay_tx,
        CompareAndReplaySucceededJob {
            scope: JobScope::Global,
            source_job_id: missing_job_id,
            expected_run_number: 1,
            replay_request_key: "caller-isolation-enforcement",
            reason: "caller isolation enforcement",
        },
    )
    .await
    .expect_err("caller-owned replay must reject repeatable read");
    assert_isolation_error(replay_error, "job.compare_and_replay_unsupported_isolation");
    replay_tx
        .rollback()
        .await
        .expect("roll back rejected replay");

    teardown_ephemeral_pool(pool, database).await;
}
