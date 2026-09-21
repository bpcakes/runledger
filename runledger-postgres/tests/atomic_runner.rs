use runledger_core::jobs::JobType;
use runledger_postgres::jobs::{JobEnqueue, JobEnqueueIntent};
use runledger_postgres::{PgAtomicError, run_atomic};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};

mod support;

#[tokio::test]
async fn intent_then_queue_runner_commits_or_rejects_as_one_unit() {
    let (pool, database) = setup_ephemeral_pool("atomic_runner", 4).await;
    let profiled = support::profiled_database(&pool).await;
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("read PostgreSQL server version");
    let major: i32 =
        sqlx::query_scalar("SELECT current_setting('server_version_num')::int / 10000")
            .fetch_one(&pool)
            .await
            .expect("read PostgreSQL major version");
    assert_eq!(major, 18);
    eprintln!("phase-scoped atomic runner PostgreSQL {version}");
    support::register_test_job_definition(&pool, "test.atomic.runner").await;
    sqlx::query("CREATE TABLE atomic_audit (id integer)")
        .execute(&pool)
        .await
        .expect("create application audit table");
    for (id, commit) in [(1, true), (2, false)] {
        let payload = serde_json::json!({"id": id});
        let key = format!("runner-{id}");
        let request = JobEnqueue {
            job_type: JobType::new("test.atomic.runner"),
            organization_id: None,
            payload: &payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: Some(&key),
            stage: None,
        };
        let intent = JobEnqueueIntent::new(JobType::new("test.atomic.runner"), &payload, &key);
        let result = run_atomic(&profiled, async |mut scope| {
            scope
                .application(async |sql| {
                    sqlx::query("INSERT INTO atomic_audit VALUES ($1)")
                        .bind(id)
                        .execute(sql.executor())
                        .await
                })
                .await
                .expect("application write");
            let intent = scope
                .record_required_job_enqueue_intent(&intent)
                .await
                .expect("intent phase");
            let mut queue = scope.queue();
            let job = queue.enqueue_job(&request).await.expect("queue phase");
            let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM job_queue WHERE id=$1")
                .bind(job.job_id)
                .fetch_one(&pool)
                .await
                .expect("observe provisional job from another connection");
            assert_eq!(visible, 0, "enqueue output is provisional inside body");
            if commit {
                Ok((intent, job))
            } else {
                Err("domain rejected")
            }
        })
        .await;
        if commit {
            assert!(result.is_ok());
        } else {
            assert!(matches!(
                result,
                Err(PgAtomicError::Rejected("domain rejected"))
            ));
        }
        let audit: i64 = sqlx::query_scalar("SELECT count(*) FROM atomic_audit WHERE id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .expect("read committed application audit");
        let intent: i64 =
            sqlx::query_scalar("SELECT count(*) FROM job_enqueue_intents WHERE idempotency_key=$1")
                .bind(&key)
                .fetch_one(&pool)
                .await
                .expect("read committed enqueue intent");
        let job: i64 =
            sqlx::query_scalar("SELECT count(*) FROM job_queue WHERE idempotency_key=$1")
                .bind(&key)
                .fetch_one(&pool)
                .await
                .expect("read committed queue row");
        assert_eq!(
            (audit, intent, job),
            (i64::from(commit), i64::from(commit), i64::from(commit))
        );
    }
    profiled.pool().close().await;
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn required_conflicted_handoff_rolls_back_application_write() {
    use runledger_postgres::{PgScopeError, RequiredIntentError};
    let (pool, database) = setup_ephemeral_pool("required_conflict", 4).await;
    let profiled = support::profiled_database(&pool).await;
    let payload = serde_json::json!({"request": 1});
    let intent = JobEnqueueIntent::new(
        JobType::new("test.required.conflict"),
        &payload,
        "conflicted",
    );
    let recorded = run_atomic(&profiled, async |mut scope| {
        scope.record_required_job_enqueue_intent(&intent).await
    })
    .await
    .expect("seed pending intent");
    sqlx::query("UPDATE job_enqueue_intents SET status = 'CONFLICTED', promotion_attempts = 1, last_attempted_at = now(), conflicted_at = now(), last_error_code = 'fixture_conflict', last_error_message = 'persisted conflict fixture' WHERE id = $1")
        .bind(recorded.intent_id())
        .execute(&pool)
        .await
        .expect("seed persisted conflict");
    sqlx::query("CREATE TABLE required_handoff_audit (id integer)")
        .execute(&pool)
        .await
        .expect("application table");
    let result = run_atomic(&profiled, async |mut scope| {
        scope
            .application(async |sql| {
                sqlx::query("INSERT INTO required_handoff_audit VALUES (1)")
                    .execute(sql.executor())
                    .await
            })
            .await
            .expect("application write");
        scope.record_required_job_enqueue_intent(&intent).await
    })
    .await;
    assert!(matches!(
        result,
        Err(PgAtomicError::Rejected(PgScopeError::Application(
            RequiredIntentError::Conflict(_)
        )))
    ));
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM required_handoff_audit")
        .fetch_one(&pool)
        .await
        .expect("read committed audit");
    assert_eq!(
        count, 0,
        "known failed handoff must not commit application state"
    );
    profiled.pool().close().await;
    teardown_ephemeral_pool(pool, database).await;
}
