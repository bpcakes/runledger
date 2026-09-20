use runledger_postgres::{
    Error,
    jobs::{JobCancellationScope, cancel_job_with_scope},
};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use sqlx::{
    postgres::{PgConnectOptions, PgPoolOptions},
    types::Uuid,
};

mod support;

fn sqlx_source(error: &Error) -> &sqlx::Error {
    use std::error::Error as _;
    error
        .source()
        .expect("native source")
        .source()
        .expect("SQLx source")
        .downcast_ref()
        .expect("concrete SQLx error")
}

#[tokio::test]
async fn begin_failure_retains_original_sqlx_error() {
    let pool = PgPoolOptions::new().connect_lazy_with(PgConnectOptions::new());
    pool.close().await;
    let error = cancel_job_with_scope(&pool, JobCancellationScope::Global, Uuid::nil(), None)
        .await
        .expect_err("closed pool cannot begin");
    assert!(matches!(sqlx_source(&error), sqlx::Error::PoolClosed));
    let Error::QueryError(query) = &error else {
        panic!("query boundary required")
    };
    assert_eq!(
        query.kind(),
        Some(runledger_postgres::QueryErrorKind::TransactionBeginFailed)
    );
    assert_eq!(query.code(), "db.transaction_begin_failed");
    assert_eq!(
        query.category(),
        runledger_postgres::QueryErrorCategory::Internal
    );
    assert_eq!(
        query.client_message(),
        "Database transaction could not begin."
    );
}

#[tokio::test]
async fn commit_failure_retains_source_and_does_not_claim_cancellation() {
    let (pool, database) = setup_ephemeral_pool("cancellation_commit_failure", 2).await;
    support::register_test_job_definition(&pool, "jobs.test.cancellation_failure").await;
    let job = support::enqueue_test_job(
        &pool,
        "jobs.test.cancellation_failure",
        None,
        &serde_json::json!({}),
    )
    .await;
    sqlx::raw_sql(
        "CREATE FUNCTION reject_cancellation_commit() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'private deferred failure' USING ERRCODE = '23514'; END $$;
         CREATE CONSTRAINT TRIGGER reject_cancellation_commit AFTER UPDATE ON job_queue
         DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (NEW.status = 'CANCELED')
         EXECUTE FUNCTION reject_cancellation_commit();",
    )
    .execute(&pool)
    .await
    .expect("install deferred commit failure");
    let error = cancel_job_with_scope(&pool, JobCancellationScope::Global, job, None)
        .await
        .expect_err("deferred constraint rejects commit");
    let source = sqlx_source(&error)
        .as_database_error()
        .expect("database source");
    assert_eq!(source.code().as_deref(), Some("23514"));
    assert_eq!(source.message(), "private deferred failure");
    assert!(!format!("{error:?} {error}").contains("private deferred failure"));
    let (status, events): (String, i64) = sqlx::query_as(
        "SELECT status::text, (SELECT count(*) FROM job_events WHERE job_id=$1 AND event_type='CANCELED') FROM job_queue WHERE id=$1"
    ).bind(job).fetch_one(&pool).await.expect("independent durable readback");
    assert_eq!(status, "PENDING");
    assert_eq!(events, 0);
    teardown_ephemeral_pool(pool, database).await;
    // An unknown outcome is its own top-level variant: a `QueryError` match,
    // and any SQLSTATE classification behind it, can never absorb it.
    let Error::CommitUnconfirmed(unconfirmed) = &error else {
        panic!("unconfirmed commit must not be a query classification")
    };
    assert_eq!(unconfirmed.operation(), "commit job cancellation");
    assert_eq!(unconfirmed.sqlstate().as_deref(), Some("23514"));
    assert_eq!(
        runledger_postgres::CommitUnconfirmed::CODE,
        "db.transaction_commit_unconfirmed"
    );
}

#[tokio::test]
async fn terminated_mutation_retains_operation_and_rollback_errors() {
    let (pool, database) = setup_ephemeral_pool("cancellation_rollback_failure", 2).await;
    support::register_test_job_definition(&pool, "jobs.test.cancellation_failure").await;
    let job = support::enqueue_test_job(
        &pool,
        "jobs.test.cancellation_failure",
        None,
        &serde_json::json!({}),
    )
    .await;
    sqlx::raw_sql(
        "CREATE FUNCTION terminate_cancellation() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN PERFORM pg_terminate_backend(pg_backend_pid()); RETURN NEW; END $$;
         CREATE TRIGGER terminate_cancellation BEFORE UPDATE ON job_queue
         FOR EACH ROW WHEN (NEW.status = 'CANCELED') EXECUTE FUNCTION terminate_cancellation();",
    )
    .execute(&pool)
    .await
    .expect("install connection failure");
    let error = cancel_job_with_scope(&pool, JobCancellationScope::Global, job, None)
        .await
        .expect_err("backend termination prevents cancellation");
    let Error::RollbackFailure(failure) = error else {
        panic!("both failures must be retained")
    };
    assert!(matches!(
        sqlx_source(&failure.operation),
        sqlx::Error::Database(_) | sqlx::Error::Io(_)
    ));
    assert!(matches!(
        failure.rollback,
        sqlx::Error::Io(_) | sqlx::Error::Protocol(_) | sqlx::Error::Database(_)
    ));
    let status: String = sqlx::query_scalar("SELECT status::text FROM job_queue WHERE id=$1")
        .bind(job)
        .fetch_one(&pool)
        .await
        .expect("independent readback after backend death");
    assert_eq!(status, "PENDING");
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn cancellation_owns_read_committed_despite_session_default() {
    let (pool, database) = setup_ephemeral_pool("cancellation_isolation", 1).await;
    support::register_test_job_definition(&pool, "jobs.test.cancellation_failure").await;
    let job = support::enqueue_test_job(
        &pool,
        "jobs.test.cancellation_failure",
        None,
        &serde_json::json!({}),
    )
    .await;
    sqlx::raw_sql("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE;
        CREATE FUNCTION require_cancellation_isolation() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF current_setting('transaction_isolation') <> 'read committed' THEN
            RAISE EXCEPTION 'cancellation did not own its isolation';
          END IF;
          RETURN NEW;
        END $$;
        CREATE TRIGGER require_cancellation_isolation BEFORE UPDATE ON job_queue
        FOR EACH ROW WHEN (NEW.status = 'CANCELED') EXECUTE FUNCTION require_cancellation_isolation();")
        .execute(&pool).await.expect("install isolation oracle");
    let result = cancel_job_with_scope(&pool, JobCancellationScope::Global, job, None).await;
    let default: String = sqlx::query_scalar("SHOW default_transaction_isolation")
        .fetch_one(&pool)
        .await
        .expect("read session isolation after cancellation");
    teardown_ephemeral_pool(pool, database).await;
    assert_eq!(
        default, "serializable",
        "operation must not change the session default"
    );
    result.expect("owned cancellation establishes READ COMMITTED");
}
