use std::time::{Duration, Instant};

use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::Value;
use sqlx::{Acquire, PgConnection, PgPool};

const UNIFIED_SQL: &str = include_str!("../src/jobs/queue/claim_ids.sql");
const NULLABLE_FILTER: &str = "($4::text[] IS NULL OR jq.job_type = ANY($4::text[]))";
const ITERATIONS: usize = 60;
const JOB_TYPES: [&str; 16] = [
    "jobs.test.claim_parity.0",
    "jobs.test.claim_parity.1",
    "jobs.test.claim_parity.2",
    "jobs.test.claim_parity.3",
    "jobs.test.claim_parity.4",
    "jobs.test.claim_parity.5",
    "jobs.test.claim_parity.6",
    "jobs.test.claim_parity.7",
    "jobs.test.claim_parity.8",
    "jobs.test.claim_parity.9",
    "jobs.test.claim_parity.10",
    "jobs.test.claim_parity.11",
    "jobs.test.claim_parity.12",
    "jobs.test.claim_parity.13",
    "jobs.test.claim_parity.14",
    "jobs.test.claim_parity.15",
];

#[derive(Clone, Copy, Debug)]
enum PlanMode {
    Custom,
    Generic,
}

impl PlanMode {
    const fn setting(self) -> &'static str {
        match self {
            Self::Custom => "force_custom_plan",
            Self::Generic => "force_generic_plan",
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum QueueShape {
    Balanced,
    Skewed,
}

impl QueueShape {
    const fn label(self) -> &'static str {
        match self {
            Self::Balanced => "balanced",
            Self::Skewed => "skewed",
        }
    }
}

fn baseline_sql(filtered: bool) -> String {
    assert_eq!(UNIFIED_SQL.matches(NULLABLE_FILTER).count(), 2);
    UNIFIED_SQL.replace(
        NULLABLE_FILTER,
        if filtered {
            "jq.job_type = ANY($4::text[])"
        } else {
            "TRUE"
        },
    )
}

async fn record_postgres_18_server_version(pool: &PgPool) {
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(pool)
        .await
        .expect("read PostgreSQL server_version");
    let server_version_num =
        sqlx::query_scalar::<_, i32>("SELECT current_setting('server_version_num')::int")
            .fetch_one(pool)
            .await
            .expect("read PostgreSQL server_version_num");
    eprintln!(
        "claim plan/throughput parity PostgreSQL server_version={server_version}, \
         server_version_num={server_version_num}"
    );
    assert_eq!(server_version_num / 10_000, 18);
}

async fn seed_queue(pool: &PgPool, shape: QueueShape) {
    sqlx::raw_sql(
        "TRUNCATE job_queue CASCADE;
         DELETE FROM job_definitions WHERE job_type LIKE 'jobs.test.claim_parity.%';",
    )
    .execute(pool)
    .await
    .expect("reset claim parity fixture");
    sqlx::query(
        "INSERT INTO job_definitions (
            job_type,
            version,
            max_attempts,
            default_timeout_seconds,
            default_priority,
            is_enabled
         )
         SELECT
            'jobs.test.claim_parity.' || ordinal::text,
            1,
            3,
            30,
            100,
            true
         FROM generate_series(0, 15) AS ordinal",
    )
    .execute(pool)
    .await
    .expect("insert claim parity definitions");

    let job_type_expression = match shape {
        QueueShape::Balanced => "'jobs.test.claim_parity.' || ((ordinal - 1) % 16)::text",
        QueueShape::Skewed => {
            "CASE WHEN ordinal % 20 = 0
                  THEN 'jobs.test.claim_parity.15'
                  ELSE 'jobs.test.claim_parity.0'
             END"
        }
    };
    let insert_sql = format!(
        "INSERT INTO job_queue (
            job_type,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            next_run_at,
            stage,
            execution_resource_key
         )
         SELECT
            {job_type_expression},
            '{{}}'::jsonb,
            100000 - ordinal,
            3,
            30,
            now() - interval '1 minute',
            'queued',
            CASE
                WHEN ordinal % 3 = 0 THEN 'resource-' || (ordinal % 500)::text
                ELSE NULL
            END
         FROM generate_series(1, 20000) AS ordinal"
    );
    sqlx::query(&insert_sql)
        .execute(pool)
        .await
        .expect("insert claim parity jobs");
    sqlx::raw_sql("ANALYZE job_queue; ANALYZE job_execution_resource_claims;")
        .execute(pool)
        .await
        .expect("analyze claim parity fixture");
}

async fn prepare_statements(conn: &mut PgConnection, mode: PlanMode, filtered: bool) {
    sqlx::query(&format!("SET plan_cache_mode = {}", mode.setting()))
        .execute(&mut *conn)
        .await
        .expect("set claim parity plan mode");
    for (name, sql) in [
        ("baseline_claim", baseline_sql(filtered)),
        ("unified_claim", UNIFIED_SQL.to_owned()),
    ] {
        sqlx::query(&format!(
            "PREPARE {name}(bigint, text, integer, text[], bigint) AS {sql}"
        ))
        .execute(&mut *conn)
        .await
        .unwrap_or_else(|error| panic!("prepare {name}: {error}"));
    }
}

fn execute_arguments(allowed_types: &[&str]) -> String {
    if allowed_types.is_empty() {
        "16, 'claim-parity-worker', 30, NULL::text[], 128".to_owned()
    } else {
        let values = allowed_types
            .iter()
            .map(|job_type| format!("'{job_type}'"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("16, 'claim-parity-worker', 30, ARRAY[{values}]::text[], 128")
    }
}

async fn explain_statement(conn: &mut PgConnection, statement: &str, arguments: &str) -> Value {
    sqlx::query_scalar::<_, Value>(&format!(
        "EXPLAIN (FORMAT JSON) EXECUTE {statement}({arguments})"
    ))
    .fetch_one(conn)
    .await
    .unwrap_or_else(|error| panic!("explain {statement}: {error}"))
}

fn total_cost(plan: &Value) -> f64 {
    plan.pointer("/0/Plan/Total Cost")
        .and_then(Value::as_f64)
        .expect("claim plan has numeric total cost")
}

fn plan_signature(plan: &Value) -> Vec<String> {
    fn collect(node: &Value, signature: &mut Vec<String>) {
        if let Some(node_type) = node.get("Node Type").and_then(Value::as_str) {
            signature.push(format!("node:{node_type}"));
        }
        if let Some(index_name) = node.get("Index Name").and_then(Value::as_str) {
            signature.push(format!("index:{index_name}"));
        }
        if let Some(join_type) = node.get("Join Type").and_then(Value::as_str) {
            signature.push(format!("join:{join_type}"));
        }
        if let Some(plans) = node.get("Plans").and_then(Value::as_array) {
            for child in plans {
                collect(child, signature);
            }
        }
    }

    let mut signature = Vec::new();
    collect(
        plan.pointer("/0/Plan").expect("claim plan has a root node"),
        &mut signature,
    );
    signature
}

async fn execute_statement(
    conn: &mut PgConnection,
    statement: &str,
    arguments: &str,
) -> (Duration, Vec<sqlx::types::Uuid>) {
    let sql = format!("EXECUTE {statement}({arguments})");
    let mut tx = conn.begin().await.expect("begin claim parity sample");
    let started = Instant::now();
    let ids = sqlx::query_scalar::<_, sqlx::types::Uuid>(&sql)
        .fetch_all(&mut *tx)
        .await
        .unwrap_or_else(|error| panic!("execute {statement}: {error}"));
    let elapsed = started.elapsed();
    assert!(!ids.is_empty(), "{statement} must find claim candidates");
    tx.rollback().await.expect("rollback claim parity sample");
    (elapsed, ids)
}

async fn measure_pair(
    conn: &mut PgConnection,
    arguments: &str,
) -> (Duration, Duration, Vec<sqlx::types::Uuid>) {
    const WARMUP_ITERATIONS: usize = 10;

    let mut baseline_samples = Vec::with_capacity(ITERATIONS);
    let mut unified_samples = Vec::with_capacity(ITERATIONS);
    let mut expected_ids = None;
    for iteration in 0..ITERATIONS + WARMUP_ITERATIONS {
        let (baseline, unified) = if iteration % 2 == 0 {
            let baseline = execute_statement(conn, "baseline_claim", arguments).await;
            let unified = execute_statement(conn, "unified_claim", arguments).await;
            (baseline, unified)
        } else {
            let unified = execute_statement(conn, "unified_claim", arguments).await;
            let baseline = execute_statement(conn, "baseline_claim", arguments).await;
            (baseline, unified)
        };
        assert_eq!(
            baseline.1, unified.1,
            "claim statements selected different IDs"
        );
        if let Some(expected_ids) = &expected_ids {
            assert_eq!(
                &baseline.1, expected_ids,
                "claim IDs changed between samples"
            );
        } else {
            expected_ids = Some(baseline.1.clone());
        }
        if iteration >= WARMUP_ITERATIONS {
            baseline_samples.push(baseline.0);
            unified_samples.push(unified.0);
        }
    }
    baseline_samples.sort_unstable();
    unified_samples.sort_unstable();
    (
        baseline_samples[baseline_samples.len() / 2],
        unified_samples[unified_samples.len() / 2],
        expected_ids.expect("claim parity samples produce IDs"),
    )
}

#[tokio::test]
#[ignore = "manual PostgreSQL 18 plan/throughput acceptance gate"]
async fn nullable_filter_claim_plan_and_throughput_match_split_statements() {
    let (pool, database) = setup_ephemeral_pool("postgres_claim_plan_parity", 6).await;
    record_postgres_18_server_version(&pool).await;

    let cardinalities = [
        Vec::<&str>::new(),
        vec![JOB_TYPES[15]],
        JOB_TYPES[..8].to_vec(),
        JOB_TYPES.to_vec(),
    ];

    for shape in [QueueShape::Balanced, QueueShape::Skewed] {
        seed_queue(&pool, shape).await;
        for allowed_types in &cardinalities {
            if matches!(shape, QueueShape::Skewed) && allowed_types.len() > 1 {
                continue;
            }
            let filtered = !allowed_types.is_empty();
            let arguments = execute_arguments(allowed_types);
            for mode in [PlanMode::Custom, PlanMode::Generic] {
                let mut conn = pool
                    .acquire()
                    .await
                    .expect("acquire claim parity connection");
                conn.close_on_drop();
                prepare_statements(&mut conn, mode, filtered).await;
                let baseline_plan =
                    explain_statement(&mut conn, "baseline_claim", &arguments).await;
                let unified_plan = explain_statement(&mut conn, "unified_claim", &arguments).await;
                let baseline_cost = total_cost(&baseline_plan);
                let unified_cost = total_cost(&unified_plan);
                assert_eq!(
                    plan_signature(&baseline_plan),
                    plan_signature(&unified_plan),
                    "claim plan operators/indexes differ for shape={} allowed_types={} mode={mode:?}",
                    shape.label(),
                    allowed_types.len(),
                );
                let (baseline_median, unified_median, _) =
                    measure_pair(&mut conn, &arguments).await;
                let cost_ratio = unified_cost / baseline_cost;
                let throughput_ratio = unified_median.as_secs_f64() / baseline_median.as_secs_f64();
                eprintln!(
                    "claim parity shape={} allowed_types={} mode={mode:?} \
                     baseline_cost={baseline_cost:.2} unified_cost={unified_cost:.2} \
                     cost_ratio={cost_ratio:.3} baseline_median_us={} unified_median_us={} \
                     latency_ratio={throughput_ratio:.3}",
                    shape.label(),
                    allowed_types.len(),
                    baseline_median.as_micros(),
                    unified_median.as_micros(),
                );
                assert!(
                    cost_ratio <= 1.05,
                    "unified claim plan cost regressed by more than 5%"
                );
                assert!(
                    throughput_ratio <= 1.15,
                    "unified claim latency regressed by more than 15%"
                );

                if allowed_types.len() <= 1 {
                    let allowed_owned = (!allowed_types.is_empty()).then(|| {
                        allowed_types
                            .iter()
                            .map(|job_type| (*job_type).to_owned())
                            .collect::<Vec<_>>()
                    });
                    let mut blocker = pool.begin().await.expect("begin claim contention blocker");
                    let locked = sqlx::query_scalar::<_, sqlx::types::Uuid>(
                        "SELECT id
                         FROM job_queue
                         WHERE status = 'PENDING'
                           AND next_run_at <= now()
                           AND ($1::text[] IS NULL OR job_type = ANY($1::text[]))
                         ORDER BY priority DESC, next_run_at, created_at, id
                         LIMIT 128
                         FOR UPDATE",
                    )
                    .bind(allowed_owned.as_deref())
                    .fetch_all(&mut *blocker)
                    .await
                    .expect("lock contended claim candidates");
                    assert!(!locked.is_empty());
                    let (baseline_contended, unified_contended, _) =
                        measure_pair(&mut conn, &arguments).await;
                    let contended_ratio =
                        unified_contended.as_secs_f64() / baseline_contended.as_secs_f64();
                    eprintln!(
                        "claim contention shape={} allowed_types={} mode={mode:?} \
                         locked={} baseline_median_us={} unified_median_us={} \
                         latency_ratio={contended_ratio:.3}",
                        shape.label(),
                        allowed_types.len(),
                        locked.len(),
                        baseline_contended.as_micros(),
                        unified_contended.as_micros(),
                    );
                    assert!(
                        contended_ratio <= 1.15,
                        "unified contended claim latency regressed by more than 15%"
                    );
                    blocker
                        .rollback()
                        .await
                        .expect("rollback claim contention blocker");
                }
            }
        }
    }

    teardown_ephemeral_pool(pool, database).await;
}
