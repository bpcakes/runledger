//! External consumer coverage for consuming transaction and schema owners.
use runledger_core::jobs::JobType;
use runledger_postgres::jobs::{
    JobEnqueueIntent, JobEnqueueIntentDisposition, record_job_enqueue_intent_tx,
};
use runledger_postgres::{DbPool, PgAtomicError, PgScopeError, run_atomic};
use serde_json::{Value, json};
use sqlx::types::Uuid;

pub async fn verify_schema(pool: &DbPool) {
    let snapshot = runledger_postgres::ensure_schema_compatible_after_idempotency_cutover(pool)
        .await
        .expect("owned schema snapshot");
    assert!(snapshot.database_oid() > 0);
    let mut tx = pool.begin().await.expect("begin uncommitted corruption");
    sqlx::query("UPDATE public._sqlx_migrations SET checksum = decode('00', 'hex')")
        .execute(&mut *tx)
        .await
        .expect("corrupt caller history");
    runledger_postgres::ensure_schema_compatible_after_idempotency_cutover(pool)
        .await
        .expect("uncommitted caller state cannot influence verification");
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
    let result = run_atomic(pool, async |mut scope| {
        scope
            .application(async |sql| {
                sqlx::query("INSERT INTO opaque_intent_audit VALUES ($1)")
                    .bind(id)
                    .execute(sql.executor())
                    .await?;
                Ok::<_, sqlx::Error>(())
            })
            .await
            .expect("write application state");
        let outcome = scope
            .record_job_enqueue_intent(&intent)
            .await
            .expect("record intent");
        assert_eq!(outcome.disposition, JobEnqueueIntentDisposition::Inserted);
        if commit { Ok(outcome) } else { Err("rejected") }
    })
    .await;
    let outcome = if commit {
        Some(result.expect("acknowledged commit"))
    } else {
        assert!(matches!(result, Err(PgAtomicError::Rejected("rejected"))));
        None
    };
    let retained: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM job_enqueue_intents WHERE idempotency_key=$1)",
    )
    .bind(&key)
    .fetch_one(pool)
    .await
    .expect("read authoritative intent");
    let audit: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM opaque_intent_audit WHERE id=$1)")
            .bind(id)
            .fetch_one(pool)
            .await
            .expect("read application audit");
    assert_eq!(retained, commit);
    assert_eq!(audit, commit);
    if let Some(outcome) = outcome {
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
    let payload = json!({"changed": true});
    let changed = request(scope, &payload, key);
    let error = run_atomic(pool, async |mut scope| {
        let replay = scope
            .record_job_enqueue_intent(intent)
            .await
            .expect("opaque replay");
        assert_eq!(replay.intent_id, expected);
        assert_eq!(replay.disposition, JobEnqueueIntentDisposition::Existing);
        scope.record_job_enqueue_intent(&changed).await
    })
    .await
    .expect_err("changed opaque replay conflicts");
    assert!(
        matches!(error, PgAtomicError::Rejected(PgScopeError::Application(runledger_postgres::Error::QueryError(ref e))) if e.code() == "job.intent_idempotency_conflict")
    );
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
    native
        .commit()
        .await
        .expect("validation did not abort native transaction");
    run_atomic(pool, async |mut scope| {
        scope
            .application(async |sql| {
                sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
                    .execute(sql.executor())
                    .await?;
                Ok::<_, sqlx::Error>(())
            })
            .await
    })
    .await
    .expect_err("birth identity prevents changing transaction isolation");
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM job_enqueue_intents WHERE idempotency_key='opaque-intent-isolation'",
    )
    .fetch_one(pool)
    .await
    .expect("check absence after committed rejection");
    assert_eq!(count, 0);
}
