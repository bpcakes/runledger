use runledger_core::jobs::JobStatus;
use runledger_postgres::jobs::{JobCancellationScope, cancel_job_with_scope, get_job_by_id};
use runledger_postgres::{DbPool, Error};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::types::Uuid;

mod support;

use support::{enqueue_test_job, register_test_job_definition};

const JOB_TYPE: &str = "jobs.test.cancellation_scope";

async fn assert_pending(pool: &DbPool, organization_id: Uuid, job_id: Uuid) {
    let job = get_job_by_id(pool, Some(organization_id), job_id)
        .await
        .expect("load tenant job")
        .expect("tenant job exists");
    assert_eq!(job.status, JobStatus::Pending);
}

fn assert_error_code(error: Error, expected: &str) {
    match error {
        Error::QueryError(error) => assert_eq!(error.code(), expected),
        other => panic!("expected query error {expected}, got {other}"),
    }
}

#[tokio::test]
async fn cancellation_scope_distinguishes_exact_tenants_from_admin() {
    let (pool, database) = setup_ephemeral_pool("postgres_job_cancellation_scope", 4).await;
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("read PostgreSQL server_version");
    let server_version_num =
        sqlx::query_scalar::<_, i32>("SELECT current_setting('server_version_num')::int")
            .fetch_one(&pool)
            .await
            .expect("read PostgreSQL server_version_num");
    eprintln!(
        "job cancellation scope regression PostgreSQL server_version={server_version}, server_version_num={server_version_num}"
    );

    register_test_job_definition(&pool, JOB_TYPE).await;
    let organization_id = Uuid::from_u128(11_001);
    let wrong_organization_id = Uuid::from_u128(11_002);
    let payload = json!({"test": "cancellation-scope"});

    let admin_job_id = enqueue_test_job(&pool, JOB_TYPE, Some(organization_id), &payload).await;
    let admin_canceled = cancel_job_with_scope(
        &pool,
        JobCancellationScope::Admin,
        admin_job_id,
        Some("admin cancellation"),
    )
    .await
    .expect("admin scope cancels a tenant job");
    assert_eq!(admin_canceled.organization_id, Some(organization_id));
    assert_eq!(admin_canceled.status, JobStatus::Canceled);

    let exact_job_id = enqueue_test_job(&pool, JOB_TYPE, Some(organization_id), &payload).await;
    let exact_global_error = cancel_job_with_scope(
        &pool,
        JobCancellationScope::Global,
        exact_job_id,
        Some("must not cross global boundary"),
    )
    .await
    .expect_err("exact global scope must not cancel a tenant job");
    assert_error_code(exact_global_error, "job.not_found");
    assert_pending(&pool, organization_id, exact_job_id).await;

    let exact_canceled = cancel_job_with_scope(
        &pool,
        JobCancellationScope::Organization(organization_id),
        exact_job_id,
        Some("exact tenant cancellation"),
    )
    .await
    .expect("correct tenant scope cancels its job");
    assert_eq!(exact_canceled.organization_id, Some(organization_id));
    assert_eq!(exact_canceled.status, JobStatus::Canceled);

    let wrong_tenant_job_id =
        enqueue_test_job(&pool, JOB_TYPE, Some(organization_id), &payload).await;
    let wrong_tenant_error = cancel_job_with_scope(
        &pool,
        JobCancellationScope::Organization(wrong_organization_id),
        wrong_tenant_job_id,
        Some("must not cross tenant boundary"),
    )
    .await
    .expect_err("wrong tenant scope must not cancel another tenant's job");
    assert_error_code(wrong_tenant_error, "job.not_found");
    assert_pending(&pool, organization_id, wrong_tenant_job_id).await;

    let global_job_id = enqueue_test_job(&pool, JOB_TYPE, None, &payload).await;
    let global_canceled = cancel_job_with_scope(
        &pool,
        JobCancellationScope::Global,
        global_job_id,
        Some("exact global cancellation"),
    )
    .await
    .expect("exact global scope cancels a global job");
    assert_eq!(global_canceled.organization_id, None);
    assert_eq!(global_canceled.status, JobStatus::Canceled);

    teardown_ephemeral_pool(pool, database).await;
}
