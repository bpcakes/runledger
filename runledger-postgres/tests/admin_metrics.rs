use runledger_core::jobs::JobType;
use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    JobDefinitionUpsert, JobEnqueue, enqueue_job, get_job_metrics, upsert_job_definition_tx,
};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::types::Uuid;

const JOB_TYPE: &str = "jobs.test.global_percentiles";

async fn register_definition(pool: &DbPool) {
    let mut tx = pool.begin().await.expect("begin definition transaction");
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
    tx.commit().await.expect("commit definition transaction");
}

async fn enqueue(pool: &DbPool, organization_id: Uuid, idempotency_key: &str) -> Uuid {
    let payload = json!({});
    enqueue_job(
        pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: Some(organization_id),
            payload: &payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: Some(idempotency_key),
            stage: None,
        },
    )
    .await
    .expect("enqueue metrics job")
}

async fn insert_finished_attempt(pool: &DbPool, job_id: Uuid, attempt: i32, duration_ms: i64) {
    sqlx::query(
        "INSERT INTO job_attempts (
            job_id,
            run_number,
            attempt,
            worker_id,
            leased_at,
            started_at,
            finished_at,
            outcome
         )
         VALUES (
            $1,
            1,
            $2,
            'global-percentile-test',
            now() - interval '1 hour',
            now() - interval '1 hour',
            now() - interval '1 hour' + $3::float8 * interval '1 millisecond',
            'TERMINAL'
         )",
    )
    .bind(job_id)
    .bind(attempt)
    .bind(duration_ms as f64)
    .execute(pool)
    .await
    .expect("insert finished attempt");
}

fn assert_metric_close(actual: Option<f64>, expected: f64) {
    let actual = actual.expect("duration percentile is present");
    let tolerance = (16.0 * f64::EPSILON * expected.abs()).max(1e-9);
    assert!(
        (actual - expected).abs() <= tolerance,
        "expected {expected}, got {actual} (tolerance {tolerance})"
    );
}

#[tokio::test]
async fn service_wide_percentiles_are_computed_from_all_attempts() {
    let (pool, database) = setup_ephemeral_pool("postgres_global_metrics", 4).await;
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("read PostgreSQL server version");
    eprintln!("global metrics regression PostgreSQL server_version={server_version}");

    register_definition(&pool).await;
    let organization_a = Uuid::now_v7();
    let organization_b = Uuid::now_v7();
    let job_a = enqueue(&pool, organization_a, "global-percentile-a").await;
    let job_b = enqueue(&pool, organization_b, "global-percentile-b").await;

    insert_finished_attempt(&pool, job_a, 1, 10).await;
    for (attempt, duration_ms) in [(1, 1_000), (2, 2_000), (3, 3_000)] {
        insert_finished_attempt(&pool, job_b, attempt, duration_ms).await;
    }

    let global = get_job_metrics(&pool, None, Some(JOB_TYPE))
        .await
        .expect("load service-wide job metrics")
        .pop()
        .expect("registered job type has metrics");
    assert_metric_close(global.p50_duration_ms_24h, 1_500.0);
    assert_metric_close(global.p95_duration_ms_24h, 2_850.0);

    let organization = get_job_metrics(&pool, Some(organization_a), Some(JOB_TYPE))
        .await
        .expect("load organization job metrics")
        .pop()
        .expect("organization job type has metrics");
    assert_metric_close(organization.p50_duration_ms_24h, 10.0);
    assert_metric_close(organization.p95_duration_ms_24h, 10.0);

    teardown_ephemeral_pool(pool, database).await;
}
