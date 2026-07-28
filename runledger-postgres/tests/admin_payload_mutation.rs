use runledger_core::jobs::{
    JobFailureKind, JobType, StepKey, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder,
    WorkflowType,
};
use runledger_postgres::jobs::{
    JobDefinitionUpsert, JobEnqueue, JobFailureUpdate, JobPayloadUuidArrayFieldUpdate,
    JobPayloadUuidArrayFieldUpdateRejection, cancel_job, claim_jobs_for_types,
    complete_job_failure, complete_job_success, enqueue_job, enqueue_workflow_run,
    list_workflow_steps, update_job_payload_uuid_array_field, upsert_job_definition_tx,
};
use runledger_postgres::{Error, QueryErrorCategory};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::{Value, json};
use sqlx::types::Uuid;
use sqlx::{PgPool, Row};
use tokio::time::{Duration, timeout};

const PAYLOAD_FIELD: &str = "target_ids";

fn uuid(value: &str) -> Uuid {
    Uuid::parse_str(value).expect("valid uuid")
}

fn org_id() -> Uuid {
    uuid("00000000-0000-0000-0000-000000000001")
}

fn original_payload() -> Value {
    json!({
        PAYLOAD_FIELD: [uuid("00000000-0000-0000-0000-000000000010").to_string()],
        "stable": "keep"
    })
}

fn payload_with_ids(ids: &[Uuid]) -> Value {
    let ids = ids.iter().map(Uuid::to_string).collect::<Vec<_>>();
    json!({
        PAYLOAD_FIELD: ids,
        "stable": "keep"
    })
}

fn replacement_ids() -> [Uuid; 2] {
    [
        uuid("00000000-0000-0000-0000-000000000101"),
        uuid("00000000-0000-0000-0000-000000000102"),
    ]
}

fn assert_lock_timeout_error(error: Error) {
    match error {
        Error::QueryError(query_error) => {
            assert_eq!(query_error.category(), QueryErrorCategory::Internal);
            assert_eq!(query_error.sqlstate(), Some("55P03"));
            assert!(
                query_error.source_arc().is_some(),
                "lock timeout should preserve the source sqlx error"
            );
        }
        other => panic!("expected query error, got {other:?}"),
    }
}

async fn register_job_definition(pool: &PgPool, job_type: JobType<'_>) {
    let mut tx = pool.begin().await.expect("begin setup tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type,
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit setup tx");
}

async fn enqueue_direct_job(
    pool: &PgPool,
    organization_id: Uuid,
    job_type: JobType<'_>,
    payload: &Value,
    idempotency_key: Option<&str>,
) -> Uuid {
    enqueue_job(
        pool,
        &JobEnqueue {
            job_type,
            organization_id: Some(organization_id),
            payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key,
            stage: None,
        },
    )
    .await
    .expect("enqueue direct job")
}

async fn load_job_payload(pool: &PgPool, job_id: Uuid) -> Value {
    sqlx::query_scalar::<_, Value>("SELECT payload FROM job_queue WHERE id = $1")
        .bind(job_id)
        .fetch_one(pool)
        .await
        .expect("load job payload")
}

async fn load_job_payload_and_enqueue_request(
    pool: &PgPool,
    job_id: Uuid,
) -> (Value, Option<Value>) {
    let row = sqlx::query("SELECT payload, enqueue_request FROM job_queue WHERE id = $1")
        .bind(job_id)
        .fetch_one(pool)
        .await
        .expect("load job payload and enqueue request");

    (
        row.try_get("payload").expect("payload column"),
        row.try_get("enqueue_request")
            .expect("enqueue_request column"),
    )
}

async fn assert_rejected_update_preserves_payload(
    pool: &PgPool,
    organization_id: Uuid,
    job_id: Uuid,
    job_type: JobType<'_>,
    expected_payload: &Value,
    expected_reason: JobPayloadUuidArrayFieldUpdateRejection,
) {
    let replacement = replacement_ids();
    let updated = update_job_payload_uuid_array_field(
        pool,
        organization_id,
        job_id,
        job_type,
        PAYLOAD_FIELD,
        &replacement,
    )
    .await
    .expect("update payload field");

    assert_eq!(
        updated,
        JobPayloadUuidArrayFieldUpdate::Rejected {
            reason: expected_reason
        },
        "guarded job should not be mutated"
    );
    assert_eq!(
        load_job_payload(pool, job_id).await,
        expected_payload.clone()
    );
}

#[tokio::test]
async fn admin_payload_update_allows_pending_non_idempotent_direct_job() {
    let (pool, database) = setup_ephemeral_pool("admin_payload_pending", 4).await;
    let job_type = JobType::new("jobs.test.admin_payload_pending");
    let organization_id = org_id();
    register_job_definition(&pool, job_type).await;

    let payload = original_payload();
    let job_id = enqueue_direct_job(&pool, organization_id, job_type, &payload, None).await;
    let replacement = replacement_ids();

    let updated = update_job_payload_uuid_array_field(
        &pool,
        organization_id,
        job_id,
        job_type,
        PAYLOAD_FIELD,
        &replacement,
    )
    .await
    .expect("update pending non-idempotent payload");

    assert_eq!(updated, JobPayloadUuidArrayFieldUpdate::Updated);
    assert_eq!(
        load_job_payload(&pool, job_id).await,
        payload_with_ids(&replacement)
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn admin_payload_update_row_lock_wait_is_bounded_without_mutation() {
    let (pool, database) = setup_ephemeral_pool("admin_payload_lock_timeout", 4).await;
    let job_type = JobType::new("jobs.test.admin_payload_lock_timeout");
    let organization_id = org_id();
    register_job_definition(&pool, job_type).await;

    let payload = original_payload();
    let job_id = enqueue_direct_job(&pool, organization_id, job_type, &payload, None).await;

    let mut blocker = pool.begin().await.expect("begin blocker tx");
    sqlx::query("SELECT id FROM job_queue WHERE id = $1 FOR UPDATE")
        .bind(job_id)
        .fetch_one(&mut *blocker)
        .await
        .expect("hold job row lock");

    let replacement = replacement_ids();
    let error = timeout(
        Duration::from_secs(2),
        update_job_payload_uuid_array_field(
            &pool,
            organization_id,
            job_id,
            job_type,
            PAYLOAD_FIELD,
            &replacement,
        ),
    )
    .await
    .expect("row lock acquisition should be bounded")
    .expect_err("conflicting row lock should time out");

    assert_lock_timeout_error(error);
    assert_eq!(load_job_payload(&pool, job_id).await, payload);

    blocker.rollback().await.expect("release blocker tx");
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn admin_payload_update_reports_not_found_for_missing_job() {
    let (pool, database) = setup_ephemeral_pool("admin_payload_missing", 4).await;
    let job_type = JobType::new("jobs.test.admin_payload_missing");
    let organization_id = org_id();
    register_job_definition(&pool, job_type).await;

    let replacement = replacement_ids();
    let updated = update_job_payload_uuid_array_field(
        &pool,
        organization_id,
        uuid("00000000-0000-0000-0000-00000000ffff"),
        job_type,
        PAYLOAD_FIELD,
        &replacement,
    )
    .await
    .expect("update missing payload field");

    assert_eq!(updated, JobPayloadUuidArrayFieldUpdate::NotFound);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn admin_payload_update_rejects_leased_and_terminal_jobs() {
    let (pool, database) = setup_ephemeral_pool("admin_payload_state_guards", 4).await;
    let job_type = JobType::new("jobs.test.admin_payload_state_guards");
    let organization_id = org_id();
    register_job_definition(&pool, job_type).await;
    let payload = original_payload();

    let leased_job_id = enqueue_direct_job(&pool, organization_id, job_type, &payload, None).await;
    let leased_job = claim_jobs_for_types(&pool, "worker-leased", 30, 1, &[job_type])
        .await
        .expect("claim leased job")
        .pop()
        .expect("leased job claimed");
    assert_eq!(leased_job.id, leased_job_id);
    assert_rejected_update_preserves_payload(
        &pool,
        organization_id,
        leased_job_id,
        job_type,
        &payload,
        JobPayloadUuidArrayFieldUpdateRejection::NotPendingOrClaimed,
    )
    .await;

    let succeeded_job_id =
        enqueue_direct_job(&pool, organization_id, job_type, &payload, None).await;
    let succeeded_job = claim_jobs_for_types(&pool, "worker-succeeded", 30, 1, &[job_type])
        .await
        .expect("claim succeeded job")
        .pop()
        .expect("succeeded job claimed");
    assert_eq!(succeeded_job.id, succeeded_job_id);
    let succeeded_worker = succeeded_job
        .worker_id
        .as_deref()
        .expect("claimed job worker id");
    complete_job_success(
        &pool,
        succeeded_job.id,
        succeeded_job.run_number,
        succeeded_job.attempt,
        succeeded_worker,
        None,
    )
    .await
    .expect("complete succeeded job");
    assert_rejected_update_preserves_payload(
        &pool,
        organization_id,
        succeeded_job_id,
        job_type,
        &payload,
        JobPayloadUuidArrayFieldUpdateRejection::NotPendingOrClaimed,
    )
    .await;

    let canceled_job_id =
        enqueue_direct_job(&pool, organization_id, job_type, &payload, None).await;
    cancel_job(
        &pool,
        Some(organization_id),
        canceled_job_id,
        Some("test cancel"),
    )
    .await
    .expect("cancel pending job");
    assert_rejected_update_preserves_payload(
        &pool,
        organization_id,
        canceled_job_id,
        job_type,
        &payload,
        JobPayloadUuidArrayFieldUpdateRejection::NotPendingOrClaimed,
    )
    .await;

    let dead_lettered_job_id =
        enqueue_direct_job(&pool, organization_id, job_type, &payload, None).await;
    let dead_lettered_job = claim_jobs_for_types(&pool, "worker-dead-lettered", 30, 1, &[job_type])
        .await
        .expect("claim dead-lettered job")
        .pop()
        .expect("dead-lettered job claimed");
    assert_eq!(dead_lettered_job.id, dead_lettered_job_id);
    let dead_lettered_worker = dead_lettered_job
        .worker_id
        .as_deref()
        .expect("claimed job worker id");
    complete_job_failure(
        &pool,
        dead_lettered_job.id,
        dead_lettered_job.run_number,
        dead_lettered_job.attempt,
        dead_lettered_worker,
        &JobFailureUpdate::new(
            JobFailureKind::Terminal,
            "terminal",
            "terminal failure",
            None,
        ),
    )
    .await
    .expect("complete dead-lettered job");
    assert_rejected_update_preserves_payload(
        &pool,
        organization_id,
        dead_lettered_job_id,
        job_type,
        &payload,
        JobPayloadUuidArrayFieldUpdateRejection::NotPendingOrClaimed,
    )
    .await;

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn admin_payload_update_rejects_idempotent_pending_job_without_snapshot_drift() {
    let (pool, database) = setup_ephemeral_pool("admin_payload_idempotent", 4).await;
    let job_type = JobType::new("jobs.test.admin_payload_idempotent");
    let organization_id = org_id();
    register_job_definition(&pool, job_type).await;

    let payload = original_payload();
    let job_id = enqueue_direct_job(
        &pool,
        organization_id,
        job_type,
        &payload,
        Some("admin-payload-idempotent"),
    )
    .await;
    let (before_payload, before_enqueue_request) =
        load_job_payload_and_enqueue_request(&pool, job_id).await;
    assert_eq!(before_payload, payload);
    assert!(
        before_enqueue_request.is_some(),
        "keyed job should store an enqueue request snapshot"
    );

    assert_rejected_update_preserves_payload(
        &pool,
        organization_id,
        job_id,
        job_type,
        &payload,
        JobPayloadUuidArrayFieldUpdateRejection::IdempotentRequestSnapshot,
    )
    .await;

    let (after_payload, after_enqueue_request) =
        load_job_payload_and_enqueue_request(&pool, job_id).await;
    assert_eq!(after_payload, before_payload);
    assert_eq!(after_enqueue_request, before_enqueue_request);

    let retry_id = enqueue_direct_job(
        &pool,
        organization_id,
        job_type,
        &payload,
        Some("admin-payload-idempotent"),
    )
    .await;
    assert_eq!(retry_id, job_id);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn admin_payload_update_rejects_workflow_managed_pending_job() {
    let (pool, database) = setup_ephemeral_pool("admin_payload_workflow", 4).await;
    let job_type = JobType::new("jobs.test.admin_payload_workflow");
    let organization_id = org_id();
    register_job_definition(&pool, job_type).await;

    let payload = original_payload();
    let metadata = json!({"test": "admin_payload_update_rejects_workflow_managed_pending_job"});
    let step = WorkflowStepEnqueueBuilder::new(StepKey::new("step"), job_type, &payload)
        .try_build()
        .expect("build workflow step");
    let workflow =
        WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test.admin_payload"), &metadata)
            .organization_id(organization_id)
            .step(step)
            .try_build()
            .expect("build workflow");
    let workflow_run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow");
    let steps = list_workflow_steps(&pool, Some(organization_id), workflow_run.id)
        .await
        .expect("list workflow steps");
    let workflow_step = steps.first().expect("workflow step exists");
    let job_id = workflow_step.job_id.expect("workflow step released a job");

    assert_rejected_update_preserves_payload(
        &pool,
        organization_id,
        job_id,
        job_type,
        &payload,
        JobPayloadUuidArrayFieldUpdateRejection::WorkflowManaged,
    )
    .await;
    let steps_after = list_workflow_steps(&pool, Some(organization_id), workflow_run.id)
        .await
        .expect("list workflow steps after rejected update");
    assert_eq!(steps_after[0].payload, payload);

    teardown_ephemeral_pool(pool, database).await;
}
