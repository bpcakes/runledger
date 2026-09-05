//! Plans for the actual SQLx-prepared public list queries, using production indexes.
use runledger_postgres::prelude::*;
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::Value;
use sqlx::Connection;
use sqlx::types::Uuid;

mod support;

const JOB_TYPE: &str = "jobs.test.read_plans";

async fn populate(pool: &DbPool) {
    support::register_test_job_definition(pool, JOB_TYPE).await;
    // Twenty rows per tenant, interleaved by creation time. Global rows are
    // deliberately old so a global ordering scan cannot get lucky with LIMIT.
    sqlx::raw_sql(
        "INSERT INTO job_queue (job_type, organization_id, max_attempts, created_at)
         SELECT 'jobs.test.read_plans',
                CASE WHEN n <= 20 THEN NULL ELSE md5((n % 1000)::text)::uuid END,
                3, now() - (20001 - n) * interval '1 second'
         FROM generate_series(1, 20000) n;
         INSERT INTO job_enqueue_intents
            (job_type, organization_id, payload, idempotency_key, enqueue_request, created_at)
         SELECT job_type, organization_id, payload, id::text, '{}'::jsonb, created_at FROM job_queue;
         ANALYZE job_queue;
         ANALYZE job_enqueue_intents;",
    ).execute(pool).await.expect("populate representative cross-tenant data");
}

fn has_scope_index_access(value: &Value, global_indexes: &[String]) -> bool {
    match value {
        Value::Object(object) => {
            object
                .get("Index Cond")
                .and_then(Value::as_str)
                .is_some_and(|condition| condition.contains("organization_id"))
                || object
                    .get("Index Name")
                    .and_then(Value::as_str)
                    .is_some_and(|name| global_indexes.iter().any(|index| index == name))
                || object
                    .values()
                    .any(|value| has_scope_index_access(value, global_indexes))
        }
        Value::Array(values) => values
            .iter()
            .any(|value| has_scope_index_access(value, global_indexes)),
        _ => false,
    }
}

async fn assert_prepared_plan(pool: &DbPool, table: &str, organization: Option<Uuid>) {
    let (name, parameter_count): (String, i32) = sqlx::query_as(
        "SELECT name, cardinality(parameter_types) FROM pg_prepared_statements
         WHERE statement LIKE '%' || $1 || '%'
           AND statement LIKE '%ORDER BY created_at DESC, id DESC%'
           AND statement LIKE '%OFFSET $5%'",
    )
    .bind(format!("FROM {table}"))
    .fetch_one(pool)
    .await
    .expect("public list prepared statement");
    // A global-only partial index also proves selective scope access, even
    // when EXPLAIN has no Index Cond because the index predicate is sufficient.
    let global_indexes: Vec<String> = if organization.is_none() {
        sqlx::query_scalar(
            "SELECT indexrelid::regclass::text FROM pg_index
             WHERE indrelid = $1::text::regclass
               AND pg_get_expr(indpred, indrelid) LIKE '%organization_id IS NULL%'",
        )
        .bind(table)
        .fetch_all(pool)
        .await
        .expect("global partial indexes")
    } else {
        Vec::new()
    };
    let organization = organization.map_or_else(|| "NULL".into(), |id| format!("'{id}'::uuid"));
    // The baseline query has a sixth admin flag; retaining support here lets
    // this regression test demonstrate failure before the fix as well.
    let admin_argument = if parameter_count == 6 { ", false" } else { "" };
    let statement = format!(
        "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON, TIMING OFF) EXECUTE \"{}\" ({organization}, NULL, NULL, 20, 0{admin_argument})",
        name.replace('"', "\"\""),
    );
    let plan: Value = sqlx::query_scalar(&statement)
        .fetch_one(pool)
        .await
        .expect("explain actual query");
    eprintln!(
        "{table}: execution={} ms, shared buffers={}",
        plan[0]["Execution Time"], plan[0]["Plan"]["Shared Hit Blocks"]
    );
    assert!(
        has_scope_index_access(&plan, &global_indexes),
        "scope must constrain an index scan: {plan}"
    );
}

#[tokio::test]
async fn selective_scopes_use_indexes_with_custom_and_generic_prepared_plans() {
    // A single connection keeps the public call and EXPLAIN on the same
    // backend, so this checks the executed query rather than a copied predicate.
    let (pool, database) = setup_ephemeral_pool("job_read_plans", 1).await;
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("version");
    eprintln!("job list plans PostgreSQL {version}");
    assert!(version.starts_with("18."));
    populate(&pool).await;
    let tenant: Uuid = sqlx::query_scalar("SELECT md5('999')::uuid")
        .fetch_one(&pool)
        .await
        .expect("tenant");
    for mode in ["force_custom_plan", "force_generic_plan"] {
        sqlx::raw_sql(&format!("SET plan_cache_mode = {mode}"))
            .execute(&pool)
            .await
            .expect("plan mode");
        for (scope, organization) in [
            (JobReadScope::Organization(tenant), Some(tenant)),
            (JobReadScope::Global, None),
        ] {
            // SQLx retains statement IDs internally, so close the prepared
            // cache through its API instead of issuing DEALLOCATE behind it.
            pool.acquire()
                .await
                .expect("connection")
                .clear_cached_statements()
                .await
                .expect("clear cache");
            let jobs = list_jobs_with_scope(
                &pool,
                &JobReadListFilter {
                    scope,
                    status: None,
                    job_type: None,
                    limit: 20,
                    offset: 0,
                },
            )
            .await
            .expect("list jobs");
            assert_eq!(jobs.len(), 20);
            assert!(jobs.iter().all(|job| job.organization_id == organization));
            assert_prepared_plan(&pool, "job_queue", organization).await;
            let intents = list_job_enqueue_intents_with_scope(
                &pool,
                &JobEnqueueIntentReadListFilter::new(scope, 20, 0),
            )
            .await
            .expect("list intents");
            assert_eq!(intents.len(), 20);
            assert!(
                intents
                    .iter()
                    .all(|intent| intent.organization_id == organization)
            );
            assert_prepared_plan(&pool, "job_enqueue_intents", organization).await;
        }
    }
    teardown_ephemeral_pool(pool, database).await;
}
