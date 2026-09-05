//! Reproducible diagnostic, deliberately excluded from timing-sensitive CI.
use std::time::Instant;

use runledger_core::jobs::{
    JobType, StepKey, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder, WorkflowType,
};
use runledger_postgres::{DbPool, jobs::*};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;

mod support;

fn report(label: &str, mut samples: Vec<f64>) {
    samples.sort_by(f64::total_cmp);
    println!(
        "{label}: median_ms={:.3} p95_ms={:.3}",
        samples[samples.len() / 2],
        samples[(samples.len() * 95 / 100).min(samples.len() - 1)]
    );
}

async fn reads(pool: &DbPool) {
    // Deterministic, individually varied text; payload/checkpoint/output are deliberately wide.
    sqlx::raw_sql(
        "INSERT INTO job_queue (job_type, max_attempts, payload, checkpoint, output, created_at)
        SELECT 'cost.job', 3, jsonb_build_object('data', data), jsonb_build_object('data', data),
            jsonb_build_object('data', data), '2026-01-01'::timestamptz + n * interval '1 second'
        FROM generate_series(1, 10000) n
        CROSS JOIN LATERAL (SELECT string_agg(md5(n::text || ':' || k::text), '') AS data
            FROM generate_series(1, 128) k) wide;
        ANALYZE job_queue;",
    )
    .execute(pool)
    .await
    .expect("seed reads");
    for offset in [0, 9000] {
        let mut samples = Vec::new();
        let mut json_bytes = 0;
        for sample in 0..32 {
            let start = Instant::now();
            let rows = list_jobs_with_scope(
                pool,
                &JobReadListFilter {
                    scope: JobReadScope::Global,
                    status: None,
                    job_type: None,
                    limit: 100,
                    offset,
                },
            )
            .await
            .expect("full page");
            let elapsed = start.elapsed().as_secs_f64() * 1000.;
            assert_eq!(rows.len(), 100);
            if sample > 0 {
                samples.push(elapsed);
            }
            json_bytes = rows
                .iter()
                .map(|r| {
                    r.payload.to_string().len()
                        + r.checkpoint.as_ref().map_or(0, |v| v.to_string().len())
                        + r.output.as_ref().map_or(0, |v| v.to_string().len())
                })
                .sum::<usize>();
        }
        report(
            &format!("full offset={offset} json_bytes={json_bytes}"),
            samples,
        );
    }
    compact_reads(pool).await;
}

async fn compact_reads(pool: &DbPool) {
    let (created_at, id) = sqlx::query_as(
        "SELECT created_at, id FROM job_queue
        ORDER BY created_at DESC, id DESC OFFSET 8999 LIMIT 1",
    )
    .fetch_one(pool)
    .await
    .expect("cursor outside measured region");
    for after in [None, Some(JobSummaryCursor { created_at, id })] {
        let request = JobSummaryFilter {
            scope: JobReadScope::Global,
            status: None,
            job_type: None,
            limit: 100,
            after,
        };
        let mut samples = Vec::new();
        for sample in 0..32 {
            let start = Instant::now();
            let rows = list_job_summaries(pool, &request)
                .await
                .expect("compact page");
            let elapsed = start.elapsed().as_secs_f64() * 1000.;
            assert_eq!(rows.len(), 100);
            if sample > 0 {
                samples.push(elapsed);
            }
        }
        report(
            &format!("compact after={} json_bytes=0", after.is_some()),
            samples,
        );
    }
    // Compare equal projections/decoding. Derive the offset counterpart from
    // the actual prepared public cursor query, changing pagination only.
    let cursor_sql: String = sqlx::query_scalar(
        "SELECT statement FROM pg_prepared_statements
        WHERE statement LIKE 'SELECT id, job_type,%'
          AND statement LIKE '%AND (created_at, id) < ($5, $6)%'
          AND statement LIKE '%organization_id IS NULL%' LIMIT 1",
    )
    .fetch_one(pool)
    .await
    .expect("public cursor statement");
    let offset_sql = cursor_sql.replace("AND (created_at, id) < ($5, $6)", "") + " OFFSET $5";
    let mut cursor_samples = Vec::new();
    let mut offset_samples = Vec::new();
    for sample in 0..32 {
        let start = Instant::now();
        let cursor_rows = sqlx::query(&cursor_sql)
            .bind(None::<sqlx::types::Uuid>)
            .bind(None::<String>)
            .bind(None::<String>)
            .bind(100_i64)
            .bind(created_at)
            .bind(id)
            .fetch_all(pool)
            .await
            .expect("cursor raw");
        let cursor_elapsed = start.elapsed().as_secs_f64() * 1000.;
        let start = Instant::now();
        let offset_rows = sqlx::query(&offset_sql)
            .bind(None::<sqlx::types::Uuid>)
            .bind(None::<String>)
            .bind(None::<String>)
            .bind(100_i64)
            .bind(9000_i64)
            .fetch_all(pool)
            .await
            .expect("offset raw");
        let offset_elapsed = start.elapsed().as_secs_f64() * 1000.;
        use sqlx::Row;
        assert_eq!(
            cursor_rows
                .iter()
                .map(|r| r.get::<sqlx::types::Uuid, _>("id"))
                .collect::<Vec<_>>(),
            offset_rows
                .iter()
                .map(|r| r.get::<sqlx::types::Uuid, _>("id"))
                .collect::<Vec<_>>()
        );
        if sample > 0 {
            cursor_samples.push(cursor_elapsed);
            offset_samples.push(offset_elapsed);
        }
    }
    report("compact raw cursor at depth 9000", cursor_samples);
    report("compact raw offset=9000", offset_samples);
}

async fn workflows(pool: &DbPool) {
    let payload = json!({"sample": true});
    for count in [10_usize, 100, 600] {
        for width in [1, 4] {
            let keys = (0..count).map(|i| format!("s{i}")).collect::<Vec<_>>();
            let mut builder =
                WorkflowRunEnqueueBuilder::new(WorkflowType::new("cost.workflow"), &payload);
            let mut edges = 0;
            for i in 0..count {
                let deps = keys[i.saturating_sub(width)..i]
                    .iter()
                    .map(|s| StepKey::new(s))
                    .collect::<Vec<_>>();
                edges += deps.len();
                builder = builder.step(
                    WorkflowStepEnqueueBuilder::new(
                        StepKey::new(&keys[i]),
                        JobType::new("cost.job"),
                        &payload,
                    )
                    .depends_on_success(&deps)
                    .try_build()
                    .expect("step"),
                );
            }
            let run = builder.try_build().expect("run");
            let mut samples = Vec::new();
            sqlx::query("SELECT pg_stat_statements_reset()")
                .execute(pool)
                .await
                .expect("reset counters");
            for sample in 0..12 {
                let start = Instant::now();
                enqueue_workflow_run(pool, &run)
                    .await
                    .expect("enqueue workflow");
                if sample > 0 {
                    samples.push(start.elapsed().as_secs_f64() * 1000.);
                }
            }
            report(&format!("workflow V={count} E={edges}"), samples);
            let counts: Vec<(String, i64)> = sqlx::query_as("SELECT CASE WHEN query ILIKE 'INSERT INTO workflow_steps %' THEN 'step inserts'
                WHEN query ILIKE 'INSERT INTO workflow_step_dependencies %' THEN 'edge inserts' ELSE 'other statements' END,
                sum(calls)::bigint FROM pg_stat_statements WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                AND query NOT ILIKE '%pg_stat_statements%' GROUP BY 1 ORDER BY 1")
                .fetch_all(pool).await.expect("statement counts");
            println!("12 enqueues: {counts:?}");
        }
    }
}

async fn direct_jobs(pool: &DbPool) {
    let payload = json!({"sample": true});
    let request = JobEnqueue {
        job_type: JobType::new("cost.job"),
        organization_id: None,
        payload: &payload,
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        next_run_at: None,
        idempotency_key: None,
        stage: None,
    };
    for own_transaction in [true, false] {
        let mut samples = Vec::new();
        sqlx::query("SELECT pg_stat_statements_reset()")
            .execute(pool)
            .await
            .expect("reset counters");
        for sample in 0..12 {
            let start = Instant::now();
            if own_transaction {
                for _ in 0..100 {
                    enqueue_job_with_outcome(pool, &request)
                        .await
                        .expect("direct enqueue");
                }
            } else {
                let mut tx = pool.begin().await.expect("begin");
                for _ in 0..100 {
                    enqueue_job_with_outcome_tx(&mut tx, &request)
                        .await
                        .expect("direct enqueue tx");
                }
                tx.commit().await.expect("commit");
            }
            if sample > 0 {
                samples.push(start.elapsed().as_secs_f64() * 1000.);
            }
        }
        report(
            &format!("100 direct jobs own_transaction={own_transaction}"),
            samples,
        );
        let calls: i64 = sqlx::query_scalar(
            "SELECT sum(calls)::bigint FROM pg_stat_statements
            WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
            AND query NOT ILIKE '%pg_stat_statements%'",
        )
        .fetch_one(pool)
        .await
        .expect("direct statement count");
        println!("12 direct groups: total statements={calls}");
    }
}

#[tokio::test]
#[ignore = "manual PostgreSQL 18 measurement; requires shared_preload_libraries=pg_stat_statements"]
async fn measure_operational_costs() {
    let (pool, database) = setup_ephemeral_pool("operational_costs", 1).await;
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("version");
    let version_num: String = sqlx::query_scalar("SHOW server_version_num")
        .fetch_one(&pool)
        .await
        .expect("version num");
    assert!(version_num.starts_with("18"));
    println!("PostgreSQL {version}; server_version_num={version_num}; pool max=1");
    sqlx::raw_sql("CREATE EXTENSION pg_stat_statements")
        .execute(&pool)
        .await
        .expect("extension");
    support::register_test_job_definition(&pool, "cost.job").await;
    reads(&pool).await;
    workflows(&pool).await;
    direct_jobs(&pool).await;
    println!("connections={}, idle={}", pool.size(), pool.num_idle());
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
#[ignore = "manual PostgreSQL 18 measurement; requires shared_preload_libraries=pg_stat_statements"]
async fn measure_progress_costs() {
    let (pool, database) = setup_ephemeral_pool("progress_costs", 1).await;
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("version");
    assert!(version.starts_with("18."));
    println!("progress costs PostgreSQL {version}; pool max=1");
    sqlx::raw_sql("CREATE EXTENSION pg_stat_statements")
        .execute(&pool)
        .await
        .expect("extension");
    support::register_test_job_definition(&pool, "cost.progress").await;
    let id = support::enqueue_test_job(&pool, "cost.progress", None, &json!({})).await;
    let job = support::claim_one_job(&pool, "progress-cost-worker").await;
    let identity = JobLeaseIdentity::new(id, job.run_number, job.attempt, "progress-cost-worker");
    let checkpoint = json!({"page": 1});
    let update = JobOrdinaryProgressUpdate {
        progress_done: Some(1),
        progress_total: Some(10),
        checkpoint: Some(&checkpoint),
    };
    update_job_ordinary_progress_for_lease(&pool, identity, &update)
        .await
        .expect("warm up");
    sqlx::query("SELECT pg_stat_statements_reset(0, (SELECT oid FROM pg_database WHERE datname = current_database()))")
        .execute(&pool).await.expect("reset this database's statistics");
    let mut samples = Vec::new();
    for _ in 0..64 {
        let start = Instant::now();
        update_job_ordinary_progress_for_lease(&pool, identity, &update)
            .await
            .expect("progress commits");
        samples.push(start.elapsed().as_secs_f64() * 1000.);
    }
    let statements: Vec<(String, i64)> = sqlx::query_as(
        "SELECT query, sum(calls)::bigint FROM pg_stat_statements
         WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
           AND toplevel AND query NOT ILIKE '%pg_stat_statements%'
         GROUP BY query ORDER BY query",
    )
    .fetch_all(&pool)
    .await
    .expect("actual progress statements");
    let calls: i64 = statements.iter().map(|(_, calls)| calls).sum();
    println!(
        "progress: writes=64, total_statements={calls}, statements_per_write={}",
        calls / 64
    );
    assert!(
        calls <= 64 * 6,
        "ordinary progress exceeded its six-statement budget: {statements:?}"
    );
    for (query, calls) in &statements {
        println!(
            "calls={calls}: {}",
            query.split_whitespace().collect::<Vec<_>>().join(" ")
        );
    }
    report("ordinary progress with checkpoint and audit event", samples);
    let saved = get_job_by_id(&pool, None, id)
        .await
        .expect("read")
        .expect("job");
    assert_eq!(saved.checkpoint, Some(checkpoint));
    assert_eq!(
        (saved.progress_done, saved.progress_total),
        (Some(1), Some(10))
    );
    let events = list_job_events(&pool, None, id, 100, None)
        .await
        .expect("events");
    assert_eq!(
        events
            .iter()
            .filter(|event| event.event_type == runledger_core::jobs::JobEventType::Progress)
            .count(),
        65
    );
    teardown_ephemeral_pool(pool, database).await;
}
