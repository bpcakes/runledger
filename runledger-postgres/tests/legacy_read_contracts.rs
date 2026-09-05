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
