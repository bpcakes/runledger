//! External consumer parity for retained native-resource views.
use super::OpaqueConsumerTransaction;
use runledger_core::jobs::JobType;
use runledger_postgres::jobs::{
    JobEnqueueIntent, JobEnqueueIntentDisposition, get_job_enqueue_intent_by_id,
    record_job_enqueue_intent_in_transaction, record_job_enqueue_intent_tx,
};
use runledger_postgres::{DbPool, PgSessionView, SchemaCompatibilityError};
use serde_json::{Value, json};
use sqlx::types::Uuid;

struct OpaqueSession(sqlx::pool::PoolConnection<sqlx::Postgres>);
impl OpaqueSession {
    fn view(&mut self) -> PgSessionView<'_> {
        PgSessionView::new(&mut self.0)
    }
}

pub async fn verify_schema(pool: &DbPool) {
    let mut session = OpaqueSession(pool.acquire().await.expect("acquire schema session"));
    runledger_postgres::ensure_schema_compatible_after_idempotency_cutover_with_session(
        session.view(),
    )
    .await
    .expect("schema checks accept a retained session");
    drop(session);
    assert_schema_rejection_parity(pool).await;
}

async fn assert_schema_rejection_parity(pool: &DbPool) {
    let mut tx = pool.begin().await.expect("begin schema corruption fixture");
    let version: i64 = sqlx::query_scalar("SELECT min(version) FROM _sqlx_migrations")
        .fetch_one(&mut *tx)
        .await
        .expect("oldest native migration");
    sqlx::query("UPDATE _sqlx_migrations SET checksum = decode('00', 'hex') WHERE version=$1")
        .bind(version)
        .execute(&mut *tx)
        .await
        .expect("corrupt one history checksum");
    let native =
        runledger_postgres::ensure_schema_compatible_after_idempotency_cutover_with_connection(
            &mut tx,
        )
        .await
        .expect_err("native check rejects corrupt history");
    let opaque =
        runledger_postgres::ensure_schema_compatible_after_idempotency_cutover_with_session(
            PgSessionView::new(&mut tx),
        )
        .await
        .expect_err("view check rejects the same corrupt history");
    for error in [native, opaque] {
        assert!(
            matches!(error, SchemaCompatibilityError::Incompatible(sqlx::migrate::MigrateError::VersionMismatch(observed)) if observed == version)
        );
    }
    tx.rollback().await.expect("restore history");
}

fn request<'a>(scope: Option<Uuid>, payload: &'a Value, key: &'a str) -> JobEnqueueIntent<'a> {
    let intent = JobEnqueueIntent::new(JobType::new("external.opaque.intent"), payload, key);
    match scope {
        Some(scope) => intent.with_organization_id(scope),
        None => intent,
    }
}

pub async fn atomicity_and_replay(pool: &DbPool) {
    sqlx::query("CREATE TABLE opaque_intent_audit (id integer PRIMARY KEY)")
        .execute(pool)
        .await
        .expect("create application audit");
    for (base, scope) in [
        (0, None),
        (
            10,
            Some(Uuid::from_u128(0x4f50415155455f494e54454e545f5343)),
        ),
    ] {
        for (id, commit) in [(base + 1, true), (base + 2, false)] {
            assert_atomic_handoff(pool, scope, id, commit).await;
        }
        assert_isolation_rejection(pool, scope).await;
    }
}

async fn assert_atomic_handoff(pool: &DbPool, scope: Option<Uuid>, id: i32, commit: bool) {
    let payload = json!({"audit_id": id});
    let key = format!("opaque-intent-{id}");
    let intent = request(scope, &payload, &key);
    let mut tx = OpaqueConsumerTransaction::new(pool.begin().await.expect("begin opaque intent"));
    sqlx::query("INSERT INTO opaque_intent_audit VALUES ($1)")
        .bind(id)
        .execute(tx.executor())
        .await
        .expect("write application state");
    let outcome = record_job_enqueue_intent_in_transaction(&mut tx.view(), &intent)
        .await
        .expect("record opaque durable intent");
    assert_eq!(outcome.disposition, JobEnqueueIntentDisposition::Inserted);
    if commit {
        tx.commit().await.expect("commit atomic handoff");
    } else {
        tx.rollback().await.expect("rollback atomic handoff");
    }
    let retained = get_job_enqueue_intent_by_id(pool, scope, outcome.intent_id)
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
        assert_replay(pool, scope, &intent, &key, outcome.intent_id).await;
    }
}

async fn assert_replay(
    pool: &DbPool,
    scope: Option<Uuid>,
    intent: &JobEnqueueIntent<'_>,
    key: &str,
    expected: Uuid,
) {
    let mut native = pool.begin().await.expect("begin native replay");
    let replay = record_job_enqueue_intent_tx(&mut native, intent)
        .await
        .expect("native replay of opaque write");
    assert_eq!(replay.intent_id, expected);
    assert_eq!(replay.disposition, JobEnqueueIntentDisposition::Existing);
    native.commit().await.expect("commit native replay");
    let mut tx = OpaqueConsumerTransaction::new(pool.begin().await.expect("begin opaque replay"));
    let replay = record_job_enqueue_intent_in_transaction(&mut tx.view(), intent)
        .await
        .expect("opaque replay");
    assert_eq!(replay.intent_id, expected);
    assert_eq!(replay.disposition, JobEnqueueIntentDisposition::Existing);
    let payload = json!({"changed": true});
    let changed = request(scope, &payload, key);
    let error = record_job_enqueue_intent_in_transaction(&mut tx.view(), &changed)
        .await
        .expect_err("changed opaque replay conflicts");
    assert!(
        matches!(error, runledger_postgres::Error::QueryError(ref e) if e.code() == "job.intent_idempotency_conflict")
    );
    tx.rollback().await.expect("rollback rejected replay");
    assert_native_conflict(pool, &changed).await;
}

async fn assert_native_conflict(pool: &DbPool, changed: &JobEnqueueIntent<'_>) {
    let mut native = pool.begin().await.expect("begin native conflict");
    let error = record_job_enqueue_intent_tx(&mut native, changed)
        .await
        .expect_err("changed native replay conflicts");
    assert!(
        matches!(error, runledger_postgres::Error::QueryError(ref e) if e.code() == "job.intent_idempotency_conflict")
    );
    native.rollback().await.expect("rollback native conflict");
}

async fn assert_isolation_rejection(pool: &DbPool, scope: Option<Uuid>) {
    let payload = json!({"must_not_write": true});
    let intent = request(scope, &payload, "opaque-intent-isolation");
    let mut native = pool.begin().await.expect("begin repeatable read");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *native)
        .await
        .expect("change transaction isolation");
    let error = record_job_enqueue_intent_tx(&mut native, &intent)
        .await
        .expect_err("native isolation rejected");
    assert!(
        matches!(error, runledger_postgres::Error::QueryError(ref e) if e.code() == "job.intent_idempotency_unsupported_isolation")
    );
    let mut tx = OpaqueConsumerTransaction::new(native);
    let error = record_job_enqueue_intent_in_transaction(&mut tx.view(), &intent)
        .await
        .expect_err("opaque isolation rejected");
    assert!(
        matches!(error, runledger_postgres::Error::QueryError(ref e) if e.code() == "job.intent_idempotency_unsupported_isolation")
    );
    tx.commit()
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
