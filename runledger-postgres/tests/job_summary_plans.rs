use chrono::{TimeZone, Utc};
use runledger_postgres::jobs::*;
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::Value;
use sqlx::{Connection, types::Uuid};

mod support;

fn has_cursor_index_condition(value: &Value) -> bool {
    match value {
        Value::Object(object) => {
            object
                .get("Index Cond")
                .and_then(Value::as_str)
                .is_some_and(|condition| {
                    condition.contains("created_at")
                        && condition.contains("id")
                        && condition.contains('<')
                })
                || object.values().any(has_cursor_index_condition)
        }
        Value::Array(values) => values.iter().any(has_cursor_index_condition),
        _ => false,
    }
}

#[tokio::test]
async fn actual_summary_cursor_constrains_index_with_custom_and_generic_plans() {
    let (pool, database) = setup_ephemeral_pool("summary_plans", 1).await;
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("version");
    eprintln!("summary plans PostgreSQL {version}");
    assert!(version.starts_with("18."));
    support::register_test_job_definition(&pool, "summary.job").await;
    sqlx::raw_sql(
        "INSERT INTO job_queue (job_type, max_attempts, organization_id, created_at)
        SELECT 'summary.job', 3, CASE WHEN n % 3 = 0 THEN NULL ELSE md5((n % 3)::text)::uuid END,
            '2026-01-01'::timestamptz + n * interval '1 second'
        FROM generate_series(1, 30000) n; ANALYZE job_queue;",
    )
    .execute(&pool)
    .await
    .expect("seed");
    let tenant: Uuid = sqlx::query_scalar("SELECT md5('1')::uuid")
        .fetch_one(&pool)
        .await
        .expect("tenant");
    let created_at = Utc
        .with_ymd_and_hms(2026, 1, 1, 0, 16, 40)
        .single()
        .expect("timestamp");
    for mode in ["force_custom_plan", "force_generic_plan"] {
        sqlx::raw_sql(&format!("SET plan_cache_mode = {mode}"))
            .execute(&pool)
            .await
            .expect("plan mode");
        for scope in [
            JobReadScope::Global,
            JobReadScope::Organization(tenant),
            JobReadScope::Admin,
        ] {
            pool.acquire()
                .await
                .expect("connection")
                .clear_cached_statements()
                .await
                .expect("clear statements");
            let rows = list_job_summaries(
                &pool,
                &JobSummaryFilter {
                    scope,
                    status: None,
                    job_type: None,
                    limit: 20,
                    after: Some(JobSummaryCursor {
                        created_at,
                        id: Uuid::nil(),
                    }),
                },
            )
            .await
            .expect("page");
            assert_eq!(rows.len(), 20);
            assert!(rows.iter().all(|r| r.created_at < created_at));
            let name: String = sqlx::query_scalar(
                "SELECT name FROM pg_prepared_statements
                WHERE statement LIKE 'SELECT id, job_type,%'
          AND statement LIKE '%AND (created_at, id) < ($5, $6)%'
                  AND statement LIKE '%FROM job_queue WHERE%' LIMIT 1",
            )
            .fetch_one(&pool)
            .await
            .expect("actual public query");
            let organization = match scope {
                JobReadScope::Organization(id) => format!("'{id}'::uuid"),
                _ => "NULL".into(),
            };
            let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON, TIMING OFF)
                EXECUTE \"{}\" ({organization}, NULL, NULL, 20, '2026-01-01 00:16:40+00', '00000000-0000-0000-0000-000000000000')", name.replace('"', "\"\"")))
                .fetch_one(&pool).await.expect("explain");
            assert!(
                has_cursor_index_condition(&plan),
                "{mode} {scope:?}: {plan}"
            );
            eprintln!(
                "{mode} {scope:?}: execution={} ms, shared buffers={}",
                plan[0]["Execution Time"], plan[0]["Plan"]["Shared Hit Blocks"]
            );
        }
    }
    teardown_ephemeral_pool(pool, database).await;
}
