use chrono::{TimeZone, Utc};
use runledger_core::jobs::{JobStatus, JobType};
use runledger_postgres::{DbPool, jobs::*};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::types::Uuid;

mod support;

fn filter(scope: JobReadScope) -> JobSummaryFilter<'static> {
    JobSummaryFilter {
        scope,
        status: None,
        job_type: None,
        limit: 2,
        after: None,
    }
}

#[tokio::test]
async fn summaries_and_statuses_preserve_scope_exact_filters_and_cursor_boundaries() {
    let (pool, database) = setup_ephemeral_pool("job_summaries", 1).await;
    for name in [
        "summary.job",
        "summary.job.extra",
        "SUMMARY.JOB",
        "summary.%",
    ] {
        support::register_test_job_definition(&pool, name).await;
    }
    let tenant = Uuid::from_u128(1000);
    let other_tenant = Uuid::from_u128(2000);
    let timestamp = Utc
        .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
        .single()
        .expect("timestamp");
    for (id, organization, job_type, status) in [
        (1, None, "summary.job", "PENDING"),
        (2, None, "summary.job", "PENDING"),
        (3, None, "summary.job", "CANCELED"),
        (4, Some(tenant), "summary.job", "PENDING"),
        (5, Some(other_tenant), "summary.job", "PENDING"),
        (6, None, "summary.job.extra", "PENDING"),
        (7, None, "SUMMARY.JOB", "PENDING"),
        (8, None, "summary.%", "PENDING"),
    ] {
        sqlx::query("INSERT INTO job_queue (id, organization_id, job_type, status, max_attempts,
            payload, checkpoint, output, created_at, run_number, attempt, progress_done, progress_total)
            VALUES ($1, $2, $3, $4::text::job_status, 3, $5, $5, $5, $6, 2, 1, 4, 10)")
            .bind(Uuid::from_u128(id)).bind(organization).bind(job_type).bind(status)
            .bind(json!({"wide": "x".repeat(8192)})).bind(timestamp)
            .execute(&pool).await.expect("fixture");
    }
    for (scope, expected) in [
        (JobReadScope::Global, vec![8, 7, 6, 3, 2, 1]),
        (JobReadScope::Organization(tenant), vec![4]),
        (JobReadScope::Organization(other_tenant), vec![5]),
        (JobReadScope::Organization(Uuid::nil()), vec![]),
        (JobReadScope::Admin, vec![8, 7, 6, 5, 4, 3, 2, 1]),
    ] {
        assert_scan(&pool, scope, &expected).await;
        let ids = [8, 7, 6, 5, 4, 3, 2, 1, 1, 999].map(Uuid::from_u128);
        let statuses = get_job_statuses_with_scope(&pool, scope, &ids)
            .await
            .expect("statuses");
        let mut sorted = expected;
        sorted.sort();
        assert_eq!(
            statuses.iter().map(|s| s.id.as_u128()).collect::<Vec<_>>(),
            sorted
        );
        for status in statuses {
            assert_eq!(
                status.status,
                if status.id == Uuid::from_u128(3) {
                    JobStatus::Canceled
                } else {
                    JobStatus::Pending
                }
            );
            assert_eq!((status.run_number, status.attempt), (2, 1));
        }
    }
    let mut exact = filter(JobReadScope::Global);
    exact.job_type = Some(JobType::new("summary.job"));
    exact.status = Some(JobStatus::Pending);
    let page = list_job_summaries(&pool, &exact).await.expect("exact page");
    assert_eq!(
        page.iter().map(|s| s.id.as_u128()).collect::<Vec<_>>(),
        vec![2, 1]
    );
    assert_eq!(
        (page[0].progress_done, page[0].progress_total),
        (Some(4), Some(10))
    );
    exact.job_type = Some(JobType::new("summary.%"));
    assert_eq!(
        list_job_summaries(&pool, &exact)
            .await
            .expect("literal wildcard")[0]
            .id,
        Uuid::from_u128(8)
    );
    // A cursor still works after its anchor is deleted; a newer row cannot shift the next page.
    let mut scan = filter(JobReadScope::Global);
    scan.after = Some(JobSummaryCursor {
        created_at: timestamp,
        id: Uuid::from_u128(3),
    });
    sqlx::query("DELETE FROM job_queue WHERE id = $1")
        .bind(Uuid::from_u128(3))
        .execute(&pool)
        .await
        .expect("delete anchor");
    support::enqueue_test_job(&pool, "summary.job", None, &json!({})).await;
    assert_eq!(
        list_job_summaries(&pool, &scan)
            .await
            .expect("continue")
            .iter()
            .map(|s| s.id.as_u128())
            .collect::<Vec<_>>(),
        vec![2, 1]
    );
    teardown_ephemeral_pool(pool, database).await;
}

async fn assert_scan(pool: &DbPool, scope: JobReadScope, expected: &[u128]) {
    let mut request = filter(scope);
    let mut seen = Vec::new();
    loop {
        let page = list_job_summaries(pool, &request).await.expect("page");
        let Some(last) = page.last() else {
            break;
        };
        request.after = Some(last.cursor());
        seen.extend(page.iter().map(|s| s.id.as_u128()));
        assert!(
            seen.len() <= expected.len(),
            "scan must terminate without duplicates"
        );
    }
    assert_eq!(seen, expected);
}

#[tokio::test]
async fn compact_read_bounds_are_validated_before_database_access() {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@localhost/unused")
        .expect("lazy pool");
    for limit in [0, -1, 1001, i64::MAX] {
        let mut request = filter(JobReadScope::Admin);
        request.limit = limit;
        let error = list_job_summaries(&pool, &request)
            .await
            .expect_err("invalid limit");
        assert!(
            matches!(error, runledger_postgres::Error::QueryError(e) if e.code() == "job.invalid_pagination")
        );
    }
    assert!(
        get_job_statuses_with_scope(&pool, JobReadScope::Admin, &[])
            .await
            .expect("empty input")
            .is_empty()
    );
    let error = get_job_statuses_with_scope(&pool, JobReadScope::Admin, &[Uuid::nil(); 1001])
        .await
        .expect_err("too many IDs");
    assert!(
        matches!(error, runledger_postgres::Error::QueryError(e) if e.code() == "job.invalid_pagination")
    );
}
