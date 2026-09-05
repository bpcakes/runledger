use chrono::{TimeZone, Utc};
use runledger_core::jobs::{JobStatus, JobType};
use runledger_postgres::jobs::*;
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::Value;
use sqlx::{Connection, types::Uuid};

mod support;

fn has_cursor_index_condition(value: &Value, require_id: bool) -> bool {
    match value {
        Value::Object(object) => {
            object
                .get("Index Cond")
                .and_then(Value::as_str)
                .is_some_and(|condition| {
                    condition.contains("created_at")
                        && (!require_id || condition.contains("id"))
                        && condition.contains('<')
                })
                || object
                    .values()
                    .any(|value| has_cursor_index_condition(value, require_id))
        }
        Value::Array(values) => values
            .iter()
            .any(|value| has_cursor_index_condition(value, require_id)),
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
    support::register_test_job_definition(&pool, "summary.sparse").await;
    sqlx::raw_sql(
        "INSERT INTO job_queue (job_type, max_attempts, organization_id, created_at, status)
        SELECT CASE WHEN n % 101 = 0 THEN 'summary.sparse' ELSE 'summary.job' END, 3, CASE WHEN n % 3 = 0 THEN NULL ELSE md5((n % 3)::text)::uuid END,
            '2026-01-01'::timestamptz + n * interval '1 second',
            CASE WHEN n % 97 = 0 THEN 'CANCELED' ELSE 'PENDING' END::job_status
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
            for (status, job_type) in [
                (None, None),
                (Some(JobStatus::Canceled), None),
                (None, Some(JobType::new("summary.sparse"))),
                (
                    Some(JobStatus::Canceled),
                    Some(JobType::new("summary.sparse")),
                ),
            ] {
                assert_summary_plan(&pool, mode, scope, created_at, status, job_type).await;
            }
        }
    }
    teardown_ephemeral_pool(pool, database).await;
}

async fn assert_summary_plan(
    pool: &runledger_postgres::DbPool,
    mode: &str,
    scope: JobReadScope,
    created_at: chrono::DateTime<Utc>,
    status: Option<JobStatus>,
    job_type: Option<JobType<'_>>,
) {
    pool.acquire()
        .await
        .expect("connection")
        .clear_cached_statements()
        .await
        .expect("clear statements");
    let rows = list_job_summaries(
        pool,
        &JobSummaryFilter {
            scope,
            status,
            job_type,
            limit: 20,
            after: Some(JobSummaryCursor {
                created_at,
                id: Uuid::nil(),
            }),
        },
    )
    .await
    .expect("page");
    let expected: Vec<_> = (1_i64..1000)
        .rev()
        .filter(|n| {
            let scope_matches = match scope {
                JobReadScope::Global => n % 3 == 0,
                JobReadScope::Organization(_) => n % 3 == 1,
                JobReadScope::Admin => true,
            };
            scope_matches
                && (status.is_none() || n % 97 == 0)
                && (job_type.is_none() || n % 101 == 0)
        })
        .take(20)
        .collect();
    let epoch = Utc
        .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .single()
        .expect("epoch");
    assert_eq!(
        rows.iter()
            .map(|r| (r.created_at - epoch).num_seconds())
            .collect::<Vec<_>>(),
        expected
    );
    assert!(rows.iter().all(|r| status.is_none_or(|s| r.status == s)
        && job_type.is_none_or(|t| r.job_type.as_str() == t.as_str())));
    let name: String = sqlx::query_scalar(
        "SELECT name FROM pg_prepared_statements
                WHERE statement LIKE 'SELECT id, job_type,%'
          AND statement LIKE '%AND (created_at, id) < ($5, $6)%'
                  AND statement LIKE '%FROM job_queue WHERE%' LIMIT 1",
    )
    .fetch_one(pool)
    .await
    .expect("actual public query");
    let organization = match scope {
        JobReadScope::Organization(id) => format!("'{id}'::uuid"),
        _ => "NULL".into(),
    };
    let status_sql = status.map_or_else(|| "NULL".into(), |s| format!("'{}'", s.as_db_value()));
    let type_sql = job_type.map_or_else(|| "NULL".into(), |t| format!("'{}'", t.as_str()));
    let plan: Value = sqlx::query_scalar(&format!("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON, TIMING OFF)
                EXECUTE \"{}\" ({organization}, {status_sql}, {type_sql}, 20, '2026-01-01 00:16:40+00', '00000000-0000-0000-0000-000000000000')", name.replace('"', "\"\"")))
                .fetch_one(pool).await.expect("explain");
    // A selective custom plan may use the existing type/status/time index,
    // applying the UUID tie-break as a residual filter. That is valid bounded
    // access too; the independent expected rows above verify full semantics.
    assert!(
        has_cursor_index_condition(&plan, status.is_none() && job_type.is_none()),
        "{mode} {scope:?}: {plan}"
    );
    eprintln!(
        "{mode} {scope:?} status={status:?} type={job_type:?}: execution={} ms, shared buffers={}",
        plan[0]["Execution Time"], plan[0]["Plan"]["Shared Hit Blocks"]
    );
}
