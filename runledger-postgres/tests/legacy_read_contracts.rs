use runledger_core::jobs::JobType;
use runledger_postgres::prelude::*;
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::types::Uuid;

mod support;

const JOB_TYPE: &str = "jobs.test.legacy_reads";

#[tokio::test]
async fn metrics_none_aggregates_global_and_tenant_rows_while_payload_keys_are_tenant_local() {
    let (pool, database) = setup_ephemeral_pool("legacy_read_contracts", 3).await;
    support::register_test_job_definition(&pool, JOB_TYPE).await;
    let tenants = [Uuid::now_v7(), Uuid::now_v7()];
    let run_id = Uuid::now_v7();
    let mut rows = Vec::new();
    for organization_id in [None, Some(tenants[0]), Some(tenants[1])] {
        let payload = json!({"run_id":run_id, "organization_id":organization_id});
        let id = enqueue_job(
            &pool,
            &JobEnqueue {
                job_type: JobType::new(JOB_TYPE),
                organization_id,
                payload: &payload,
                idempotency_key: Some("same-key"),
                priority: None,
                max_attempts: None,
                timeout_seconds: None,
                next_run_at: None,
                stage: None,
            },
        )
        .await
        .expect("same key in independent scopes");
        let intent = JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "same-key");
        let intent = match organization_id {
            Some(id) => intent.with_organization_id(id),
            None => intent,
        };
        let recorded = record_job_enqueue_intent(&pool, &intent)
            .await
            .expect("intent");
        assert_eq!(recorded.status(), JobEnqueueIntentStatus::Pending);
        rows.push((id, payload));
    }
    for (organization_id, count) in [
        (None, 3),
        (Some(tenants[0]), 1),
        (Some(tenants[1]), 1),
        (Some(Uuid::now_v7()), 0),
    ] {
        let metrics = get_job_metrics(&pool, organization_id, Some(JOB_TYPE))
            .await
            .expect("job metrics");
        assert_eq!(
            metrics.len(),
            1,
            "job definitions remain visible with zero counts"
        );
        assert_eq!(metrics[0].pending_count, count);
        let filter =
            JobEnqueueIntentMetricsFilter::new(10, 0).with_job_type(JobType::new(JOB_TYPE));
        let filter = match organization_id {
            Some(id) => filter.with_organization_id(id),
            None => filter,
        };
        let metrics = get_job_enqueue_intent_metrics(&pool, &filter)
            .await
            .expect("intent metrics");
        assert_eq!(
            metrics.iter().map(|row| row.pending_count).sum::<i64>(),
            count
        );
        assert_eq!(metrics.len(), usize::from(count != 0));
    }
    for (index, tenant) in tenants.into_iter().enumerate() {
        assert_eq!(
            get_job_payload_by_idempotency_key(&pool, tenant, JobType::new(JOB_TYPE), "same-key")
                .await
                .expect("key lookup"),
            Some(rows[index + 1].clone())
        );
        assert_eq!(
            get_latest_job_payload_for_run(&pool, tenant, JobType::new(JOB_TYPE), run_id)
                .await
                .expect("run lookup"),
            Some(rows[index + 1].clone())
        );
    }
    // Neither legacy payload helper interprets a sentinel UUID as global/admin.
    assert_eq!(
        get_job_payload_by_idempotency_key(&pool, Uuid::nil(), JobType::new(JOB_TYPE), "same-key")
            .await
            .expect("absent tenant"),
        None
    );
    assert_eq!(
        get_latest_job_payload_for_run(&pool, Uuid::nil(), JobType::new(JOB_TYPE), run_id)
            .await
            .expect("absent tenant"),
        None
    );
    let global = list_jobs_with_scope(
        &pool,
        &JobReadListFilter {
            scope: JobReadScope::Global,
            status: None,
            job_type: Some(JOB_TYPE),
            limit: 10,
            offset: 0,
        },
    )
    .await
    .expect("exact global inspection");
    assert_eq!(global.len(), 1);
    assert_eq!(global[0].id, rows[0].0);
    teardown_ephemeral_pool(pool, database).await;
}

async fn enqueue_payload(
    pool: &DbPool,
    scope: JobScope,
    job_type: &str,
    key: &str,
    payload: &serde_json::Value,
) -> Uuid {
    enqueue_job(
        pool,
        &JobEnqueue {
            job_type: JobType::new(job_type),
            organization_id: scope.organization_id(),
            payload,
            idempotency_key: Some(key),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            stage: None,
        },
    )
    .await
    .expect("enqueue scoped payload")
}

#[tokio::test]
async fn exact_payload_scopes_preserve_duplicate_keys_and_latest_ordering() {
    let (pool, database) = setup_ephemeral_pool("exact_payload_scopes", 3).await;
    support::register_test_job_definition(&pool, JOB_TYPE).await;
    let other_type = "jobs.test.legacy_reads.other";
    support::register_test_job_definition(&pool, other_type).await;
    let scopes = [
        JobScope::Global,
        JobScope::Organization(Uuid::now_v7()),
        JobScope::Organization(Uuid::now_v7()),
    ];
    let run_id = Uuid::now_v7();
    let mut expected = Vec::new();
    for (index, scope) in scopes.into_iter().enumerate() {
        let payload = json!({"run_id": run_id, "scope": index, "version": "keyed"});
        let id = enqueue_payload(&pool, scope, JOB_TYPE, "shared", &payload).await;
        let mut candidates = Vec::new();
        for version in ["tie-a", "tie-b", "older-high-id"] {
            let value = json!({"run_id": run_id, "scope": index, "version": version});
            let id = enqueue_payload(&pool, scope, JOB_TYPE, version, &value).await;
            candidates.push((id, value));
        }
        // The larger UUID wins a timestamp tie, but a larger UUID cannot beat a newer timestamp.
        candidates.sort_by_key(|row| row.0);
        for (position, row) in candidates.iter().enumerate() {
            sqlx::query("UPDATE job_queue SET created_at = '2026-01-01'::timestamptz + make_interval(days => $2) WHERE id = $1")
                .bind(row.0).bind(if position == 2 { 0 } else { 10 + index as i32 }).execute(&pool).await.expect("set deterministic order");
        }
        sqlx::query("UPDATE job_queue SET created_at = '2025-01-01'::timestamptz WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .expect("age keyed row");
        // Same scope and run, wrong job type, newer than every matching row.
        enqueue_payload(
            &pool,
            scope,
            other_type,
            "shared",
            &json!({"run_id":run_id}),
        )
        .await;
        expected.push(((id, payload), candidates[1].clone()));
    }
    enqueue_payload(
        &pool,
        JobScope::Organization(Uuid::now_v7()),
        JOB_TYPE,
        "shared",
        &json!({"run_id":run_id, "scope":"newer-unrelated"}),
    )
    .await;
    for (scope, (keyed, latest)) in scopes.into_iter().zip(expected) {
        assert_eq!(
            get_job_payload_by_idempotency_key_with_scope(
                &pool,
                scope,
                JobType::new(JOB_TYPE),
                "shared"
            )
            .await
            .expect("scoped payload or metrics read succeeds"),
            Some(keyed.clone())
        );
        assert_eq!(
            get_latest_job_payload_for_run_with_scope(&pool, scope, JobType::new(JOB_TYPE), run_id)
                .await
                .expect("scoped payload or metrics read succeeds"),
            Some(latest.clone())
        );
        if let JobScope::Organization(tenant) = scope {
            assert_eq!(
                get_job_payload_by_idempotency_key(&pool, tenant, JobType::new(JOB_TYPE), "shared")
                    .await
                    .expect("scoped payload or metrics read succeeds"),
                Some(keyed)
            );
            assert_eq!(
                get_latest_job_payload_for_run(&pool, tenant, JobType::new(JOB_TYPE), run_id)
                    .await
                    .expect("scoped payload or metrics read succeeds"),
                Some(latest)
            );
        }
        assert_eq!(
            get_job_payload_by_idempotency_key_with_scope(
                &pool,
                scope,
                JobType::new(JOB_TYPE),
                "missing"
            )
            .await
            .expect("scoped payload or metrics read succeeds"),
            None
        );
        assert_eq!(
            get_latest_job_payload_for_run_with_scope(
                &pool,
                scope,
                JobType::new(JOB_TYPE),
                Uuid::nil()
            )
            .await
            .expect("scoped payload or metrics read succeeds"),
            None
        );
    }
    for tenant in [Uuid::now_v7(), Uuid::nil()] {
        assert_eq!(
            get_job_payload_by_idempotency_key_with_scope(
                &pool,
                JobScope::Organization(tenant),
                JobType::new(JOB_TYPE),
                "shared"
            )
            .await
            .expect("scoped payload or metrics read succeeds"),
            None
        );
        assert_eq!(
            get_latest_job_payload_for_run_with_scope(
                &pool,
                JobScope::Organization(tenant),
                JobType::new(JOB_TYPE),
                run_id
            )
            .await
            .expect("scoped payload or metrics read succeeds"),
            None
        );
        assert_eq!(
            get_job_payload_by_idempotency_key(&pool, tenant, JobType::new(JOB_TYPE), "shared")
                .await
                .expect("scoped payload or metrics read succeeds"),
            None
        );
        assert_eq!(
            get_latest_job_payload_for_run(&pool, tenant, JobType::new(JOB_TYPE), run_id)
                .await
                .expect("scoped payload or metrics read succeeds"),
            None
        );
    }
    assert_nil_payload_scope(&pool).await;
    teardown_ephemeral_pool(pool, database).await;
}

fn metric_values(row: &JobMetricsRecord) -> ([i64; 9], [Option<f64>; 2]) {
    (
        [
            row.pending_count,
            row.leased_count,
            row.stale_leases,
            row.succeeded_24h,
            row.retryable_24h,
            row.terminal_24h,
            row.panicked_24h,
            row.timeout_24h,
            row.dead_lettered_24h,
        ],
        [row.p50_duration_ms_24h, row.p95_duration_ms_24h],
    )
}

#[tokio::test]
async fn exact_job_metrics_preserve_counts_durations_and_empty_definitions() {
    let (pool, database) = setup_ephemeral_pool("exact_job_metrics", 3).await;
    support::register_test_job_definition(&pool, JOB_TYPE).await;
    let empty_type = "jobs.test.legacy_reads.empty";
    support::register_test_job_definition(&pool, empty_type).await;
    let tenants = [Uuid::now_v7(), Uuid::now_v7()];
    for (scope, count) in [
        (JobScope::Global, 1),
        (JobScope::Organization(tenants[0]), 2),
        (JobScope::Organization(tenants[1]), 4),
    ] {
        for index in 0..count {
            let id = enqueue_payload(
                &pool,
                scope,
                JOB_TYPE,
                &format!("pending-{index}"),
                &json!({}),
            )
            .await;
            for (attempt, outcome) in ["RETRYABLE", "TERMINAL", "PANICKED", "TIMEOUT"]
                .into_iter()
                .enumerate()
            {
                sqlx::query("INSERT INTO job_attempts (job_id, attempt, worker_id, leased_at, started_at, finished_at, outcome) VALUES ($1, $2, 'metrics', now(), now(), now() + make_interval(secs => $3), $4::job_failure_kind)")
                    .bind(id).bind(attempt as i32 + 1).bind(f64::from(count)).bind(outcome).execute(&pool).await.expect("attempt fixture");
            }
            sqlx::query("INSERT INTO job_events (job_id, event_type) VALUES ($1, 'SUCCEEDED')")
                .bind(id)
                .execute(&pool)
                .await
                .expect("success fixture");
            sqlx::query("INSERT INTO job_dead_letters (job_id, job_type, organization_id, attempt, payload_snapshot, failed_at) VALUES ($1, $2, $3, 1, '{}'::jsonb, now())")
                .bind(id).bind(JOB_TYPE).bind(scope.organization_id()).execute(&pool).await.expect("dead letter fixture");
            let leased = enqueue_payload(
                &pool,
                scope,
                JOB_TYPE,
                &format!("leased-{index}"),
                &json!({}),
            )
            .await;
            sqlx::query("UPDATE job_queue SET status = 'LEASED', lease_expires_at = now() - interval '1 hour' WHERE id = $1").bind(leased).execute(&pool).await.expect("stale lease fixture");
        }
    }
    for (scope, count, duration) in [
        (JobReadScope::Global, 1, Some(1000.0)),
        (JobReadScope::Organization(tenants[0]), 2, Some(2000.0)),
        (JobReadScope::Organization(tenants[1]), 4, Some(4000.0)),
        // Preserve averaging per-scope percentiles, rather than pooling all attempts.
        (JobReadScope::Admin, 7, Some(7000.0 / 3.0)),
        (JobReadScope::Organization(Uuid::now_v7()), 0, None),
    ] {
        let rows = get_job_metrics_with_scope(&pool, scope, Some(JOB_TYPE))
            .await
            .expect("scoped payload or metrics read succeeds");
        assert_eq!(rows.len(), 1);
        assert_eq!(metric_values(&rows[0]), ([count; 9], [duration, duration]));
        let all = get_job_metrics_with_scope(&pool, scope, None)
            .await
            .expect("scoped payload or metrics read succeeds");
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].job_type.as_str(), JOB_TYPE);
        assert_eq!(all[1].job_type.as_str(), empty_type);
        assert_eq!(metric_values(&all[1]), ([0; 9], [None; 2]));
        assert!(
            get_job_metrics_with_scope(&pool, scope, Some("legacy_reads"))
                .await
                .expect("scoped payload or metrics read succeeds")
                .is_empty(),
            "job type filter is exact"
        );
        let legacy_scope = match scope {
            JobReadScope::Global => continue,
            JobReadScope::Organization(id) => Some(id),
            JobReadScope::Admin => None,
        };
        let legacy = get_job_metrics(&pool, legacy_scope, Some(JOB_TYPE))
            .await
            .expect("scoped payload or metrics read succeeds");
        assert_eq!(legacy.len(), 1);
        assert_eq!(metric_values(&legacy[0]), metric_values(&rows[0]));
    }
    teardown_ephemeral_pool(pool, database).await;
}

async fn assert_nil_payload_scope(pool: &DbPool) {
    // Nil UUID is an exact tenant/run value when it actually has data, never a sentinel.
    let payload = json!({"run_id":Uuid::nil(), "scope":"nil-tenant"});
    let id = enqueue_payload(
        pool,
        JobScope::Organization(Uuid::nil()),
        JOB_TYPE,
        "shared",
        &payload,
    )
    .await;
    for result in [
        get_job_payload_by_idempotency_key_with_scope(
            pool,
            JobScope::Organization(Uuid::nil()),
            JobType::new(JOB_TYPE),
            "shared",
        )
        .await
        .expect("scoped payload or metrics read succeeds"),
        get_latest_job_payload_for_run_with_scope(
            pool,
            JobScope::Organization(Uuid::nil()),
            JobType::new(JOB_TYPE),
            Uuid::nil(),
        )
        .await
        .expect("scoped payload or metrics read succeeds"),
        get_job_payload_by_idempotency_key(pool, Uuid::nil(), JobType::new(JOB_TYPE), "shared")
            .await
            .expect("scoped payload or metrics read succeeds"),
        get_latest_job_payload_for_run(pool, Uuid::nil(), JobType::new(JOB_TYPE), Uuid::nil())
            .await
            .expect("scoped payload or metrics read succeeds"),
    ] {
        assert_eq!(result, Some((id, payload.clone())));
    }
}
