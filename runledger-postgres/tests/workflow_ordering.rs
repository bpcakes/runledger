use chrono::{TimeZone, Utc};
use runledger_core::jobs::WorkflowType;
use runledger_postgres::jobs::{
    WorkflowRunListFilter, get_latest_workflow_run_by_type, list_workflow_runs,
};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::types::Uuid;

#[tokio::test]
async fn workflow_run_reads_break_created_at_ties_by_id_desc() {
    let (pool, database) = setup_ephemeral_pool("postgres_workflow_ordering", 4).await;
    let workflow_type = "workflow.test.ordering";
    let created_at = Utc
        .with_ymd_and_hms(2026, 6, 18, 12, 0, 0)
        .single()
        .expect("valid timestamp");
    let older_id = Uuid::from_u128(1);
    let newer_id = Uuid::from_u128(2);

    sqlx::query(
        "INSERT INTO workflow_runs (
            id,
            workflow_type,
            metadata,
            started_at,
            created_at,
            updated_at
         )
         VALUES
            ($1, $2, $3::jsonb, $4, $4, $4),
            ($5, $2, $6::jsonb, $4, $4, $4)",
    )
    .bind(older_id)
    .bind(workflow_type)
    .bind(json!({"order": "older"}))
    .bind(created_at)
    .bind(newer_id)
    .bind(json!({"order": "newer"}))
    .execute(&pool)
    .await
    .expect("insert tied workflow runs");

    let runs = list_workflow_runs(
        &pool,
        &WorkflowRunListFilter {
            organization_id: None,
            status: None,
            workflow_type: Some(workflow_type),
            limit: 2,
            offset: 0,
        },
    )
    .await
    .expect("list workflow runs");
    assert_eq!(
        runs.into_iter().map(|run| run.id).collect::<Vec<_>>(),
        vec![newer_id, older_id]
    );

    let second_page = list_workflow_runs(
        &pool,
        &WorkflowRunListFilter {
            organization_id: None,
            status: None,
            workflow_type: Some(workflow_type),
            limit: 1,
            offset: 1,
        },
    )
    .await
    .expect("list workflow runs second page");
    assert_eq!(second_page[0].id, older_id);

    let latest = get_latest_workflow_run_by_type(&pool, None, WorkflowType::new(workflow_type))
        .await
        .expect("load latest workflow run")
        .expect("latest workflow run exists");
    assert_eq!(latest.id, newer_id);

    teardown_ephemeral_pool(pool, database).await;
}
