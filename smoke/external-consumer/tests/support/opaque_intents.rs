//! Consumer coverage: no native transaction escapes the wrapper to Runledger.
use super::{OpaqueConsumerTransaction, PgTransactionExecutor};
use runledger_core::jobs::JobType;
use runledger_postgres::jobs::{
    JobEnqueueIntent, JobEnqueueIntentDisposition, get_job_enqueue_intent_by_id,
    record_job_enqueue_intent_in_transaction,
};
use runledger_postgres::{DbPool, PgSessionExecutor};
use serde_json::json;

struct OpaqueSession(sqlx::pool::PoolConnection<sqlx::Postgres>);
impl PgSessionExecutor for OpaqueSession {
    fn executor(&mut self) -> impl sqlx::Executor<'_, Database = sqlx::Postgres> {
        &mut *self.0
    }
}

pub async fn verify_schema(pool: &DbPool) {
    let mut session = OpaqueSession(pool.acquire().await.expect("acquire schema session"));
    runledger_postgres::ensure_schema_compatible_after_idempotency_cutover_with_executor(
        &mut session,
    )
    .await
    .expect("schema checks accept an opaque session");
}

pub async fn atomicity_and_replay(pool: &DbPool) {
    sqlx::query("CREATE TABLE opaque_intent_audit (id integer PRIMARY KEY)")
        .execute(pool)
        .await
        .expect("create application audit");
    for (id, commit) in [(1, true), (2, false)] {
        let payload = json!({"audit_id": id});
        let key = format!("opaque-intent-{id}");
        let intent = JobEnqueueIntent::new(JobType::new("external.opaque.intent"), &payload, &key);
        let mut tx =
            OpaqueConsumerTransaction::new(pool.begin().await.expect("begin opaque intent"));
        sqlx::query("INSERT INTO opaque_intent_audit VALUES ($1)")
            .bind(id)
            .execute(tx.executor())
            .await
            .expect("write application state");
        let outcome = record_job_enqueue_intent_in_transaction(&mut tx, &intent)
            .await
            .expect("record opaque durable intent");
        assert_eq!(outcome.disposition, JobEnqueueIntentDisposition::Inserted);
        if commit {
            tx.inner.commit().await.expect("commit atomic handoff");
        } else {
            tx.inner.rollback().await.expect("rollback atomic handoff");
        }
        let retained = get_job_enqueue_intent_by_id(pool, None, outcome.intent_id)
            .await
            .expect("read authoritative intent");
        let audit: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM opaque_intent_audit WHERE id=$1)")
                .bind(id)
                .fetch_one(pool)
                .await
                .expect("read application audit");
        assert_eq!(retained.is_some(), commit);
        assert_eq!(audit, commit);
        if commit {
            assert_replay(pool, &intent, outcome.intent_id).await;
        }
    }
    assert_isolation_rejection(pool).await;
}

async fn assert_replay(pool: &DbPool, intent: &JobEnqueueIntent<'_>, expected: sqlx::types::Uuid) {
    let mut tx = OpaqueConsumerTransaction::new(pool.begin().await.expect("begin replay"));
    let replay = record_job_enqueue_intent_in_transaction(&mut tx, intent)
        .await
        .expect("exact replay");
    assert_eq!(replay.intent_id, expected);
    assert_eq!(replay.disposition, JobEnqueueIntentDisposition::Existing);
    let payload = json!({"changed": true});
    let changed = JobEnqueueIntent::new(
        JobType::new("external.opaque.intent"),
        &payload,
        "opaque-intent-1",
    );
    let error = record_job_enqueue_intent_in_transaction(&mut tx, &changed)
        .await
        .expect_err("changed replay conflicts");
    assert!(matches!(error, runledger_postgres::Error::QueryError(ref e)
        if e.code() == "job.intent_idempotency_conflict"));
    tx.inner.rollback().await.expect("rollback rejected replay");
}

async fn assert_isolation_rejection(pool: &DbPool) {
    let mut tx = OpaqueConsumerTransaction::new(pool.begin().await.expect("begin repeatable read"));
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(tx.executor())
        .await
        .expect("change transaction isolation");
    let payload = json!({"must_not_write": true});
    let intent = JobEnqueueIntent::new(
        JobType::new("external.opaque.intent"),
        &payload,
        "opaque-intent-isolation",
    );
    let error = record_job_enqueue_intent_in_transaction(&mut tx, &intent)
        .await
        .expect_err("unsupported isolation rejected");
    assert!(matches!(error, runledger_postgres::Error::QueryError(ref e)
        if e.code() == "job.intent_idempotency_unsupported_isolation"));
    tx.inner
        .commit()
        .await
        .expect("validation did not abort transaction");
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM job_enqueue_intents WHERE idempotency_key='opaque-intent-isolation'",
    )
    .fetch_one(pool)
    .await
    .expect("check absence after committed rejection");
    assert_eq!(count, 0);
}
