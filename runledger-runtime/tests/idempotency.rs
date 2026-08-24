use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use runledger_core::jobs::{
    JobStage, JobType, StepKey, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder,
    WorkflowStepStatus, WorkflowType,
};
use runledger_postgres::jobs::test_support::workflow_run_release_lock_key;
use runledger_postgres::jobs::{
    AppendWorkflowStepsInput, AppendWorkflowStepsOutcome, CompleteExternalWorkflowStepInput,
    ExternalWorkflowStepTerminalOutcome, JobEnqueue, JobRunningUpdate, append_workflow_steps,
    append_workflow_steps_tx, claim_jobs_for_types, complete_external_workflow_step,
    complete_job_success, enqueue_job, enqueue_job_tx, enqueue_workflow_run,
    enqueue_workflow_run_tx, get_workflow_run_by_type_and_idempotency_key, list_job_events,
    list_workflow_steps, mark_job_running, update_workflow_step_and_pending_job_payload_tx,
};
use serde_json::{Value, json};
use sqlx::types::Uuid;
use tokio::sync::Barrier;

use runledger_test_support::{
    setup_ephemeral_pool_with_untracked_migrations as setup_ephemeral_pool, teardown_ephemeral_pool,
};
use support::{query_error_code, register_job_definition};

mod support;

async fn record_postgres_18_server_version(pool: &sqlx::PgPool, diagnostic: &str) {
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
        "{diagnostic} PostgreSQL server_version={server_version}, \
         server_version_num={server_version_num}"
    );
    assert_eq!(
        server_version_num / 10_000,
        18,
        "{diagnostic} validation must run on PostgreSQL 18"
    );
}

fn assert_error_does_not_expose(error: &runledger_postgres::Error, sensitive: &str) {
    assert!(
        !error.to_string().contains(sensitive),
        "public error display exposed sensitive value"
    );
    assert!(
        !format!("{error:?}").contains(sensitive),
        "debug error output exposed sensitive value"
    );

    if let runledger_postgres::Error::QueryError(query_error) = error {
        assert!(
            !query_error.client_message().contains(sensitive),
            "client query error message exposed sensitive value"
        );
        assert!(
            !query_error.internal_message().contains(sensitive),
            "internal query error message exposed sensitive value"
        );
    }
}

fn assert_public_error_does_not_expose(error: &runledger_postgres::Error, sensitive: &str) {
    assert!(
        !error.to_string().contains(sensitive),
        "public error display exposed sensitive value"
    );
    assert!(
        !format!("{error:?}").contains(sensitive),
        "debug error output exposed sensitive value"
    );

    if let runledger_postgres::Error::QueryError(query_error) = error {
        assert!(
            !query_error.client_message().contains(sensitive),
            "client query error message exposed sensitive value"
        );
    }
}

fn job_enqueue_with_snapshot_fields<'a>(
    job_type: JobType<'static>,
    payload: &'a Value,
    idempotency_key: &'a str,
    next_run_at: DateTime<Utc>,
) -> JobEnqueue<'a> {
    JobEnqueue {
        job_type,
        organization_id: None,
        payload,
        priority: Some(42),
        max_attempts: Some(2),
        timeout_seconds: Some(30),
        next_run_at: Some(next_run_at),
        idempotency_key: Some(idempotency_key),
        stage: Some(JobStage::Scheduled),
    }
}

async fn assert_job_idempotency_conflict(
    pool: &sqlx::PgPool,
    first: &JobEnqueue<'_>,
    changed: &JobEnqueue<'_>,
) {
    enqueue_job(pool, first)
        .await
        .expect("first enqueue succeeds");
    let error = enqueue_job(pool, changed)
        .await
        .expect_err("changed idempotent retry should be rejected");

    assert_eq!(query_error_code(&error), Some("job.idempotency_conflict"));
}

#[tokio::test]
async fn job_enqueue_idempotency_returns_existing_job_for_identical_retry() {
    let (pool, database) = setup_ephemeral_pool("idempotent_job_retry", 8).await;
    let job_type = JobType::new("jobs.test.idempotent_retry");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"kind": "retry"});
    let enqueue = JobEnqueue {
        job_type,
        organization_id: None,
        payload: &payload,
        priority: Some(42),
        max_attempts: Some(2),
        timeout_seconds: Some(30),
        next_run_at: None,
        idempotency_key: Some("same-job"),
        stage: Some(runledger_core::jobs::JobStage::Queued),
    };

    let first = enqueue_job(&pool, &enqueue)
        .await
        .expect("first enqueue succeeds");
    let second = enqueue_job(&pool, &enqueue)
        .await
        .expect("idempotent retry returns existing job");

    assert_eq!(first, second);
    let events = list_job_events(&pool, None, first, 10, None)
        .await
        .expect("list job events");
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].event_type,
        runledger_core::jobs::JobEventType::Enqueued
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn org_scoped_job_enqueue_idempotency_returns_existing_job() {
    let (pool, database) = setup_ephemeral_pool("org_idempotent_job_retry", 8).await;
    let job_type = JobType::new("jobs.test.org_idempotent_retry");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"kind": "org-retry"});
    let enqueue = JobEnqueue {
        job_type,
        organization_id: Some(Uuid::now_v7()),
        payload: &payload,
        priority: Some(42),
        max_attempts: Some(2),
        timeout_seconds: Some(30),
        next_run_at: None,
        idempotency_key: Some("same-org-job"),
        stage: Some(JobStage::Queued),
    };

    let first = enqueue_job(&pool, &enqueue)
        .await
        .expect("first org enqueue succeeds");
    let second = enqueue_job(&pool, &enqueue)
        .await
        .expect("org-scoped retry returns existing job");

    assert_eq!(first, second);
    let events = list_job_events(&pool, enqueue.organization_id, first, 10, None)
        .await
        .expect("list org job events");
    assert_eq!(events.len(), 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn job_enqueue_idempotency_keeps_global_and_org_scopes_separate() {
    let (pool, database) = setup_ephemeral_pool("job_idempotent_scope_retry", 8).await;
    let job_type = JobType::new("jobs.test.idempotent_scope_retry");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"kind": "scope-retry"});
    let organization_id = Uuid::now_v7();
    let global = JobEnqueue {
        job_type,
        organization_id: None,
        payload: &payload,
        priority: Some(42),
        max_attempts: Some(2),
        timeout_seconds: Some(30),
        next_run_at: None,
        idempotency_key: Some("same-scope-key"),
        stage: Some(JobStage::Queued),
    };
    let org_scoped = JobEnqueue {
        organization_id: Some(organization_id),
        ..global.clone()
    };

    let global_id = enqueue_job(&pool, &global)
        .await
        .expect("first global enqueue succeeds");
    let org_job_id = enqueue_job(&pool, &org_scoped)
        .await
        .expect("first org-scoped enqueue succeeds");
    assert_ne!(global_id, org_job_id);

    let global_retry = enqueue_job(&pool, &global)
        .await
        .expect("global retry returns original global job");
    let org_retry = enqueue_job(&pool, &org_scoped)
        .await
        .expect("org-scoped retry returns original org-scoped job");

    assert_eq!(global_retry, global_id);
    assert_eq!(org_retry, org_job_id);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn job_enqueue_idempotency_returns_one_job_for_concurrent_identical_retry() {
    let (pool, database) = setup_ephemeral_pool("idempotent_job_concurrent_retry", 8).await;
    let job_type_name = "jobs.test.idempotent_concurrent_retry";
    let job_type = JobType::new(job_type_name);
    register_job_definition(&pool, job_type).await;

    let barrier = Arc::new(Barrier::new(2));
    let enqueue_task = || {
        let pool = pool.clone();
        let barrier = barrier.clone();
        tokio::spawn(async move {
            let payload = json!({"kind": "concurrent-retry"});
            let enqueue = JobEnqueue {
                job_type: JobType::new(job_type_name),
                organization_id: None,
                payload: &payload,
                priority: Some(42),
                max_attempts: Some(2),
                timeout_seconds: Some(30),
                next_run_at: None,
                idempotency_key: Some("same-job-concurrent"),
                stage: Some(JobStage::Queued),
            };

            barrier.wait().await;
            enqueue_job(&pool, &enqueue).await
        })
    };

    let first_task = enqueue_task();
    let second_task = enqueue_task();
    let first = first_task
        .await
        .expect("first concurrent enqueue task should not panic")
        .expect("first concurrent enqueue succeeds");
    let second = second_task
        .await
        .expect("second concurrent enqueue task should not panic")
        .expect("second concurrent enqueue succeeds");

    assert_eq!(first, second);
    let events = list_job_events(&pool, None, first, 10, None)
        .await
        .expect("list job events");
    assert_eq!(events.len(), 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn job_enqueue_idempotency_uses_jsonb_numeric_equality() {
    let (pool, database) = setup_ephemeral_pool("idempotent_job_numeric_retry", 8).await;
    let job_type = JobType::new("jobs.test.idempotent_numeric_retry");
    register_job_definition(&pool, job_type).await;

    let first_payload = json!({"kind": "numeric", "amount": 1.0});
    let retry_payload = json!({"kind": "numeric", "amount": 1});
    let first = JobEnqueue {
        job_type,
        organization_id: None,
        payload: &first_payload,
        priority: Some(42),
        max_attempts: Some(2),
        timeout_seconds: Some(30),
        next_run_at: None,
        idempotency_key: Some("same-job-numeric"),
        stage: Some(runledger_core::jobs::JobStage::Queued),
    };
    let retry = JobEnqueue {
        payload: &retry_payload,
        ..first.clone()
    };

    let first_id = enqueue_job(&pool, &first)
        .await
        .expect("first enqueue succeeds");
    let retry_id = enqueue_job(&pool, &retry)
        .await
        .expect("numeric-equivalent retry returns existing job");

    assert_eq!(first_id, retry_id);
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn job_enqueue_idempotency_normalizes_next_run_at_to_postgres_precision() {
    let (pool, database) = setup_ephemeral_pool("idempotent_job_timestamp_precision", 8).await;
    let job_type = JobType::new("jobs.test.idempotent_timestamp_precision");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"kind": "timestamp-precision"});
    let next_run_at = DateTime::parse_from_rfc3339("2026-01-01T00:00:00.123456789Z")
        .expect("parse first timestamp")
        .with_timezone(&Utc);
    let retry_next_run_at = DateTime::parse_from_rfc3339("2026-01-01T00:00:00.123456999Z")
        .expect("parse retry timestamp")
        .with_timezone(&Utc);
    let enqueue = JobEnqueue {
        job_type,
        organization_id: None,
        payload: &payload,
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        next_run_at: Some(next_run_at),
        idempotency_key: Some("same-job-timestamp-precision"),
        stage: Some(JobStage::Queued),
    };
    let retry = JobEnqueue {
        next_run_at: Some(retry_next_run_at),
        ..enqueue.clone()
    };

    let first = enqueue_job(&pool, &enqueue)
        .await
        .expect("first enqueue succeeds");
    let second = enqueue_job(&pool, &retry)
        .await
        .expect("same microsecond retry returns existing job");

    assert_eq!(first, second);
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn legacy_job_enqueue_idempotency_snapshot_missing_is_rejected() {
    let (pool, database) = setup_ephemeral_pool("legacy_idempotent_job_rejected", 8).await;
    let job_type = JobType::new("jobs.test.legacy_idempotent_rejected");
    register_job_definition(&pool, job_type).await;

    let first_payload = json!({"kind": "legacy-snapshot-missing"});
    let first = JobEnqueue {
        job_type,
        organization_id: None,
        payload: &first_payload,
        priority: Some(42),
        max_attempts: Some(2),
        timeout_seconds: Some(30),
        next_run_at: None,
        idempotency_key: Some("legacy-same-job-missing-snapshot"),
        stage: Some(runledger_core::jobs::JobStage::Queued),
    };
    let retry = first.clone();

    let first_id = enqueue_job(&pool, &first)
        .await
        .expect("first enqueue succeeds");
    drop_job_idempotency_cutover_constraint(&pool).await;
    sqlx::query("UPDATE job_queue SET enqueue_request = NULL WHERE id = $1")
        .bind(first_id)
        .execute(&pool)
        .await
        .expect("simulate legacy job row");

    let error = enqueue_job(&pool, &retry)
        .await
        .expect_err("legacy retry without enqueue_request should be rejected");

    assert_eq!(
        query_error_code(&error),
        Some("job.legacy_idempotency_snapshot_missing")
    );
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn idempotent_job_enqueue_tx_rejects_non_read_committed_transaction() {
    let (pool, database) = setup_ephemeral_pool("idempotent_job_isolation", 8).await;
    let job_type = JobType::new("jobs.test.idempotent_isolation");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"kind": "isolation"});
    let enqueue = JobEnqueue {
        job_type,
        organization_id: None,
        payload: &payload,
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        next_run_at: None,
        idempotency_key: Some("same-job-isolation"),
        stage: Some(runledger_core::jobs::JobStage::Queued),
    };

    let mut tx = pool.begin().await.expect("begin repeatable read tx");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await
        .expect("set repeatable read isolation");
    let error = enqueue_job_tx(&mut tx, &enqueue)
        .await
        .expect_err("keyed job enqueue should reject non-read-committed isolation");
    assert_eq!(
        query_error_code(&error),
        Some("job.enqueue_idempotency_unsupported_isolation")
    );
    tx.rollback().await.expect("rollback repeatable read tx");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn idempotent_job_enqueue_tx_accepts_read_uncommitted_transaction() {
    let (pool, database) = setup_ephemeral_pool("idempotent_job_read_uncommitted", 8).await;
    let job_type = JobType::new("jobs.test.idempotent_read_uncommitted");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"kind": "read-uncommitted"});
    let enqueue = JobEnqueue {
        job_type,
        organization_id: None,
        payload: &payload,
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        next_run_at: None,
        idempotency_key: Some("same-job-read-uncommitted"),
        stage: Some(JobStage::Queued),
    };

    let mut tx = pool.begin().await.expect("begin read uncommitted tx");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
        .execute(&mut *tx)
        .await
        .expect("set read uncommitted isolation");
    let id = enqueue_job_tx(&mut tx, &enqueue)
        .await
        .expect("postgres read uncommitted behaves as read committed");
    assert_ne!(id, Uuid::nil());
    tx.rollback().await.expect("rollback read uncommitted tx");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn job_enqueue_without_idempotency_always_inserts_new_job() {
    let (pool, database) = setup_ephemeral_pool("unkeyed_job_enqueue", 8).await;
    let job_type = JobType::new("jobs.test.unkeyed_enqueue");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"kind": "unkeyed"});
    let enqueue = JobEnqueue {
        job_type,
        organization_id: None,
        payload: &payload,
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        next_run_at: None,
        idempotency_key: None,
        stage: Some(runledger_core::jobs::JobStage::Queued),
    };

    let first = enqueue_job(&pool, &enqueue)
        .await
        .expect("first enqueue succeeds");
    let second = enqueue_job(&pool, &enqueue)
        .await
        .expect("unkeyed retry inserts a new job");

    assert_ne!(first, second);
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn job_enqueue_idempotency_rejects_conflicting_retry() {
    let (pool, database) = setup_ephemeral_pool("idempotent_job_conflict", 8).await;
    let job_type = JobType::new("jobs.test.idempotent_conflict");
    register_job_definition(&pool, job_type).await;

    let first_payload = json!({"kind": "first"});
    let changed_payload = json!({"kind": "changed"});
    let first = JobEnqueue {
        job_type,
        organization_id: None,
        payload: &first_payload,
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        next_run_at: None,
        idempotency_key: Some("conflicting-job"),
        stage: Some(runledger_core::jobs::JobStage::Queued),
    };
    let changed = JobEnqueue {
        payload: &changed_payload,
        ..first.clone()
    };

    enqueue_job(&pool, &first)
        .await
        .expect("first enqueue succeeds");
    let error = enqueue_job(&pool, &changed)
        .await
        .expect_err("conflicting retry should be rejected");

    assert_eq!(query_error_code(&error), Some("job.idempotency_conflict"));
    assert_error_does_not_expose(&error, "conflicting-job");
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn job_enqueue_idempotency_rejects_changed_request_fields() {
    let (pool, database) = setup_ephemeral_pool("idempotent_job_request_fields", 8).await;
    let job_type = JobType::new("jobs.test.idempotent_request_fields");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"kind": "request-fields"});
    let next_run_at = Utc::now() + Duration::minutes(10);

    let first =
        job_enqueue_with_snapshot_fields(job_type, &payload, "changed-priority", next_run_at);
    let changed = JobEnqueue {
        priority: Some(43),
        ..first.clone()
    };
    assert_job_idempotency_conflict(&pool, &first, &changed).await;

    let first =
        job_enqueue_with_snapshot_fields(job_type, &payload, "changed-max-attempts", next_run_at);
    let changed = JobEnqueue {
        max_attempts: Some(3),
        ..first.clone()
    };
    assert_job_idempotency_conflict(&pool, &first, &changed).await;

    let first =
        job_enqueue_with_snapshot_fields(job_type, &payload, "changed-timeout", next_run_at);
    let changed = JobEnqueue {
        timeout_seconds: Some(31),
        ..first.clone()
    };
    assert_job_idempotency_conflict(&pool, &first, &changed).await;

    let first =
        job_enqueue_with_snapshot_fields(job_type, &payload, "changed-next-run", next_run_at);
    let changed = JobEnqueue {
        next_run_at: Some(next_run_at + Duration::seconds(1)),
        ..first.clone()
    };
    assert_job_idempotency_conflict(&pool, &first, &changed).await;

    let first = job_enqueue_with_snapshot_fields(job_type, &payload, "changed-stage", next_run_at);
    let changed = JobEnqueue {
        stage: Some(JobStage::Queued),
        ..first.clone()
    };
    assert_job_idempotency_conflict(&pool, &first, &changed).await;

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn job_enqueue_idempotency_survives_lifecycle_stage_mutation() {
    let (pool, database) = setup_ephemeral_pool("idempotent_job_stage_mutation", 8).await;
    let job_type = JobType::new("jobs.test.idempotent_stage_mutation");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"kind": "stage-mutation"});
    let enqueue = JobEnqueue {
        job_type,
        organization_id: None,
        payload: &payload,
        priority: Some(42),
        max_attempts: Some(2),
        timeout_seconds: Some(30),
        next_run_at: None,
        idempotency_key: Some("same-job-after-stage-change"),
        stage: Some(runledger_core::jobs::JobStage::Queued),
    };

    let job_id = enqueue_job(&pool, &enqueue)
        .await
        .expect("first enqueue succeeds");
    let mut claimed = claim_jobs_for_types(&pool, "worker-stage-mutation", 30, 1, &[job_type])
        .await
        .expect("claim job");
    let claim = claimed.pop().expect("job should be claimable");
    mark_job_running(
        &pool,
        claim.id,
        claim.run_number,
        claim.attempt,
        claim
            .worker_id
            .as_deref()
            .expect("claimed job has worker id"),
        &JobRunningUpdate {
            progress_done: None,
            progress_total: None,
            checkpoint: None,
        },
    )
    .await
    .expect("mark job running");

    let after_running = enqueue_job(&pool, &enqueue)
        .await
        .expect("retry after running stage returns existing job");
    assert_eq!(after_running, job_id);

    complete_job_success(
        &pool,
        claim.id,
        claim.run_number,
        claim.attempt,
        claim
            .worker_id
            .as_deref()
            .expect("claimed job has worker id"),
        None,
    )
    .await
    .expect("complete job");

    let after_success = enqueue_job(&pool, &enqueue)
        .await
        .expect("retry after completed stage returns existing job");
    assert_eq!(after_success, job_id);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_idempotency_lookup_keeps_global_and_org_scopes_separate() {
    let (pool, database) = setup_ephemeral_pool("workflow_idempotent_scope", 8).await;

    let workflow_type = WorkflowType::new("workflow.test.idempotent_scope");
    let payload = json!({"kind": "scope"});
    let metadata = json!({"source": "test"});
    let org_id = Uuid::now_v7();

    let org_step = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build org external step");
    let org_workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .organization_id(org_id)
        .idempotency_key("same-key")
        .step(org_step)
        .try_build()
        .expect("build org workflow");
    let org_run = enqueue_workflow_run(&pool, &org_workflow)
        .await
        .expect("enqueue org workflow");

    assert!(
        get_workflow_run_by_type_and_idempotency_key(&pool, None, workflow_type, "same-key")
            .await
            .expect("global lookup succeeds")
            .is_none(),
        "global lookup must not return an org-scoped run"
    );

    let global_step = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build global external step");
    let global_workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .idempotency_key("same-key")
        .step(global_step)
        .try_build()
        .expect("build global workflow");
    let global_run = enqueue_workflow_run(&pool, &global_workflow)
        .await
        .expect("enqueue global workflow");

    let loaded_global =
        get_workflow_run_by_type_and_idempotency_key(&pool, None, workflow_type, "same-key")
            .await
            .expect("load global workflow")
            .expect("global workflow exists");
    let loaded_org = get_workflow_run_by_type_and_idempotency_key(
        &pool,
        Some(org_id),
        workflow_type,
        "same-key",
    )
    .await
    .expect("load org workflow")
    .expect("org workflow exists");

    assert_eq!(loaded_global.id, global_run.id);
    assert_eq!(loaded_org.id, org_run.id);
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_enqueue_idempotency_returns_existing_run_for_identical_retry() {
    let (pool, database) = setup_ephemeral_pool("workflow_idempotent_retry", 8).await;

    let workflow_type = WorkflowType::new("workflow.test.idempotent_retry");
    let payload = json!({"kind": "retry"});
    let metadata = json!({"source": "test"});
    let step = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build external step");
    let workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .idempotency_key("same-workflow")
        .step(step)
        .try_build()
        .expect("build workflow");

    let first = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("first workflow enqueue succeeds");
    let second = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("idempotent workflow retry returns existing run");

    assert_eq!(first.id, second.id);
    let steps = list_workflow_steps(&pool, None, first.id)
        .await
        .expect("list workflow steps");
    assert_eq!(steps.len(), 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn org_scoped_workflow_enqueue_idempotency_returns_existing_run() {
    let (pool, database) = setup_ephemeral_pool("workflow_org_idempotent_retry", 8).await;

    let workflow_type = WorkflowType::new("workflow.test.org_idempotent_retry");
    let payload = json!({"kind": "org-retry"});
    let metadata = json!({"source": "test"});
    let org_id = Uuid::now_v7();
    let step = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build external step");
    let workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .organization_id(org_id)
        .idempotency_key("same-org-workflow")
        .step(step)
        .try_build()
        .expect("build org workflow");

    let first = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("first org workflow enqueue succeeds");
    let second = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("org-scoped workflow retry returns existing run");

    assert_eq!(first.id, second.id);
    assert_eq!(second.organization_id, Some(org_id));
    let steps = list_workflow_steps(&pool, Some(org_id), first.id)
        .await
        .expect("list org workflow steps");
    assert_eq!(steps.len(), 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_enqueue_idempotency_uses_jsonb_numeric_equality() {
    let (pool, database) = setup_ephemeral_pool("workflow_idempotent_numeric_retry", 8).await;

    let workflow_type = WorkflowType::new("workflow.test.idempotent_numeric_retry");
    let first_payload = json!({"kind": "numeric", "amount": 1.0});
    let retry_payload = json!({"kind": "numeric", "amount": 1});
    let first_metadata = json!({"source": "test", "batch": 1.0});
    let retry_metadata = json!({"source": "test", "batch": 1});
    let first_step = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &first_payload)
        .try_build()
        .expect("build first external step");
    let first_workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &first_metadata)
        .idempotency_key("same-workflow-numeric")
        .step(first_step)
        .try_build()
        .expect("build first workflow");
    let retry_step = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &retry_payload)
        .try_build()
        .expect("build retry external step");
    let retry_workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &retry_metadata)
        .idempotency_key("same-workflow-numeric")
        .step(retry_step)
        .try_build()
        .expect("build retry workflow");

    let first = enqueue_workflow_run(&pool, &first_workflow)
        .await
        .expect("first workflow enqueue succeeds");
    let retry = enqueue_workflow_run(&pool, &retry_workflow)
        .await
        .expect("numeric-equivalent workflow retry returns existing run");

    assert_eq!(first.id, retry.id);
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn legacy_workflow_enqueue_idempotency_snapshot_missing_is_rejected() {
    let (pool, database) = setup_ephemeral_pool("legacy_workflow_idempotent_rejected", 8).await;

    let workflow_type = WorkflowType::new("workflow.test.legacy_idempotent_rejected");
    let payload = json!({"kind": "legacy-snapshot-missing"});
    let metadata = json!({"source": "test"});
    let first_step = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build first external step");
    let first_workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .idempotency_key("legacy-same-workflow-missing-snapshot")
        .step(first_step)
        .try_build()
        .expect("build first workflow");
    let retry_step = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build retry external step");
    let retry_workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .idempotency_key("legacy-same-workflow-missing-snapshot")
        .step(retry_step)
        .try_build()
        .expect("build retry workflow");

    let first = enqueue_workflow_run(&pool, &first_workflow)
        .await
        .expect("first workflow enqueue succeeds");
    drop_workflow_idempotency_cutover_constraint(&pool).await;
    sqlx::query("UPDATE workflow_runs SET enqueue_request = NULL WHERE id = $1")
        .bind(first.id)
        .execute(&pool)
        .await
        .expect("simulate legacy workflow row");

    let error = enqueue_workflow_run(&pool, &retry_workflow)
        .await
        .expect_err("legacy workflow retry without enqueue_request should be rejected");

    assert_eq!(
        query_error_code(&error),
        Some("workflow.legacy_idempotency_snapshot_missing")
    );
    teardown_ephemeral_pool(pool, database).await;
}

async fn drop_job_idempotency_cutover_constraint(pool: &sqlx::PgPool) {
    sqlx::query("ALTER TABLE job_queue DROP CONSTRAINT ck_job_queue_idempotency_enqueue_request")
        .execute(pool)
        .await
        .expect("drop job_queue idempotency cutover constraint");
}

async fn drop_workflow_idempotency_cutover_constraint(pool: &sqlx::PgPool) {
    sqlx::query(
        "ALTER TABLE workflow_runs DROP CONSTRAINT ck_workflow_runs_idempotency_enqueue_request",
    )
    .execute(pool)
    .await
    .expect("drop workflow_runs idempotency cutover constraint");
}

#[tokio::test]
async fn idempotent_workflow_enqueue_tx_rejects_non_read_committed_transaction() {
    let (pool, database) = setup_ephemeral_pool("workflow_idempotent_isolation", 8).await;

    let workflow_type = WorkflowType::new("workflow.test.idempotent_isolation");
    let payload = json!({"kind": "isolation"});
    let metadata = json!({"source": "test"});
    let step = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build external step");
    let workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .idempotency_key("same-workflow-isolation")
        .step(step)
        .try_build()
        .expect("build workflow");

    let mut tx = pool.begin().await.expect("begin repeatable read tx");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await
        .expect("set repeatable read isolation");
    let error = enqueue_workflow_run_tx(&mut tx, &workflow)
        .await
        .expect_err("keyed workflow enqueue should reject non-read-committed isolation");
    assert_eq!(
        query_error_code(&error),
        Some("workflow.enqueue_idempotency_unsupported_isolation")
    );
    tx.rollback().await.expect("rollback repeatable read tx");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_enqueue_idempotency_treats_cleared_job_stage_as_queued() {
    let (pool, database) = setup_ephemeral_pool("workflow_default_stage_retry", 8).await;

    let workflow_type = WorkflowType::new("workflow.test.default_stage_retry");
    let job_type = JobType::new("jobs.test.workflow_default_stage_retry");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"kind": "default-stage"});
    let metadata = json!({"source": "test"});
    let first_step = WorkflowStepEnqueueBuilder::new(StepKey::new("root"), job_type, &payload)
        .clear_stage()
        .try_build()
        .expect("build job step with cleared stage");
    let first_workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .idempotency_key("same-workflow-default-stage")
        .step(first_step)
        .try_build()
        .expect("build first workflow");

    let retry_step = WorkflowStepEnqueueBuilder::new(StepKey::new("root"), job_type, &payload)
        .try_build()
        .expect("build job step with default queued stage");
    let retry_workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .idempotency_key("same-workflow-default-stage")
        .step(retry_step)
        .try_build()
        .expect("build retry workflow");

    let first = enqueue_workflow_run(&pool, &first_workflow)
        .await
        .expect("first workflow enqueue succeeds");
    let retry = enqueue_workflow_run(&pool, &retry_workflow)
        .await
        .expect("retry with default queued stage returns existing run");

    assert_eq!(first.id, retry.id);
    let steps = list_workflow_steps(&pool, None, first.id)
        .await
        .expect("list workflow steps");
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0].stage, Some(JobStage::Queued));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_enqueue_idempotency_matches_reordered_steps_and_dependencies() {
    let (pool, database) = setup_ephemeral_pool("workflow_reordered_enqueue", 8).await;

    let workflow_type = WorkflowType::new("workflow.test.reordered_enqueue");
    let payload = json!({"kind": "reordered"});
    let metadata = json!({"source": "test"});
    let root = WorkflowStepEnqueueBuilder::new_external(StepKey::new("root"), &payload)
        .try_build()
        .expect("build root external step");
    let alpha = WorkflowStepEnqueueBuilder::new_external(StepKey::new("alpha"), &payload)
        .depends_on_success(&[StepKey::new("root")])
        .try_build()
        .expect("build alpha external step");
    let beta = WorkflowStepEnqueueBuilder::new_external(StepKey::new("beta"), &payload)
        .depends_on_success(&[StepKey::new("root"), StepKey::new("alpha")])
        .try_build()
        .expect("build beta external step");
    let workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .idempotency_key("same-workflow-reordered")
        .step(root.clone())
        .step(alpha.clone())
        .step(beta.clone())
        .try_build()
        .expect("build first workflow");

    let reordered_beta = WorkflowStepEnqueueBuilder::new_external(StepKey::new("beta"), &payload)
        .depends_on_success(&[StepKey::new("alpha"), StepKey::new("root")])
        .try_build()
        .expect("build reordered beta external step");
    let reordered = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .idempotency_key("same-workflow-reordered")
        .step(reordered_beta)
        .step(alpha)
        .step(root)
        .try_build()
        .expect("build reordered workflow");

    let first = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("first workflow enqueue succeeds");
    let retry = enqueue_workflow_run(&pool, &reordered)
        .await
        .expect("reordered workflow retry returns existing run");

    assert_eq!(first.id, retry.id);
    let steps = list_workflow_steps(&pool, None, first.id)
        .await
        .expect("list workflow steps");
    assert_eq!(steps.len(), 3);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_enqueue_without_idempotency_always_inserts_new_run() {
    let (pool, database) = setup_ephemeral_pool("workflow_unkeyed_enqueue", 8).await;

    let workflow_type = WorkflowType::new("workflow.test.unkeyed_enqueue");
    let payload = json!({"kind": "unkeyed"});
    let metadata = json!({"source": "test"});
    let step = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build external step");
    let workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .step(step)
        .try_build()
        .expect("build workflow");

    let first = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("first workflow enqueue succeeds");
    let second = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("unkeyed workflow enqueue creates a new run");

    assert_ne!(first.id, second.id);
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_enqueue_idempotency_rejects_conflicting_retry() {
    let (pool, database) = setup_ephemeral_pool("workflow_idempotent_conflict", 8).await;

    let workflow_type = WorkflowType::new("workflow.test.idempotent_conflict");
    let first_payload = json!({"kind": "first"});
    let changed_payload = json!({"kind": "changed"});
    let metadata = json!({"source": "test"});
    let first_step = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &first_payload)
        .try_build()
        .expect("build first external step");
    let first = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .idempotency_key("conflicting-workflow")
        .step(first_step)
        .try_build()
        .expect("build first workflow");
    let changed_step =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &changed_payload)
            .try_build()
            .expect("build changed external step");
    let changed = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .idempotency_key("conflicting-workflow")
        .step(changed_step)
        .try_build()
        .expect("build changed workflow");

    enqueue_workflow_run(&pool, &first)
        .await
        .expect("first workflow enqueue succeeds");
    let error = enqueue_workflow_run(&pool, &changed)
        .await
        .expect_err("conflicting workflow retry should be rejected");

    assert_eq!(
        query_error_code(&error),
        Some("workflow.enqueue_conflicting_retry")
    );
    assert_error_does_not_expose(&error, "conflicting-workflow");
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_enqueue_idempotency_rejects_changed_result_step_key() {
    let (pool, database) =
        setup_ephemeral_pool("workflow_idempotent_result_step_conflict", 8).await;

    let workflow_type = WorkflowType::new("workflow.test.idempotent_result_step_conflict");
    let metadata = json!({"source": "test"});
    let first_payload = json!({"step": "first"});
    let second_payload = json!({"step": "second"});
    let first = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .idempotency_key("conflicting-workflow-result-step")
        .step(
            WorkflowStepEnqueueBuilder::new_external(StepKey::new("first"), &first_payload)
                .try_build()
                .expect("build first external step"),
        )
        .step(
            WorkflowStepEnqueueBuilder::new_external(StepKey::new("second"), &second_payload)
                .try_build()
                .expect("build second external step"),
        )
        .try_result_step_key("first")
        .expect("set first result step")
        .try_build()
        .expect("build first workflow");
    let changed = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .idempotency_key("conflicting-workflow-result-step")
        .step(
            WorkflowStepEnqueueBuilder::new_external(StepKey::new("first"), &first_payload)
                .try_build()
                .expect("build retry first external step"),
        )
        .step(
            WorkflowStepEnqueueBuilder::new_external(StepKey::new("second"), &second_payload)
                .try_build()
                .expect("build retry second external step"),
        )
        .try_result_step_key("second")
        .expect("set changed result step")
        .try_build()
        .expect("build changed workflow");

    enqueue_workflow_run(&pool, &first)
        .await
        .expect("first workflow enqueue succeeds");
    let error = enqueue_workflow_run(&pool, &changed)
        .await
        .expect_err("changed result step retry should be rejected");

    assert_eq!(
        query_error_code(&error),
        Some("workflow.enqueue_conflicting_retry")
    );
    assert_error_does_not_expose(&error, "conflicting-workflow-result-step");
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn append_workflow_steps_rejects_conflicting_retry() {
    let (pool, database) = setup_ephemeral_pool("workflow_append_conflicting_retry", 8).await;

    let workflow_type = WorkflowType::new("workflow.test.append_conflicting_retry");
    let payload = json!({"kind": "append-conflict"});
    let metadata = json!({"source": "test"});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build external gate");
    let workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .step(gate)
        .try_build()
        .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow");

    let first_payload = json!({"kind": "first"});
    let changed_payload = json!({"kind": "changed"});
    let first_step =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("appended"), &first_payload)
            .try_build()
            .expect("build first appended step");
    let changed_step =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("appended"), &changed_payload)
            .try_build()
            .expect("build changed appended step");
    let mutation_metadata = json!({});

    append_workflow_steps(
        &pool,
        &AppendWorkflowStepsInput {
            workflow_run_id: run.id,
            organization_id: None,
            mutation_key: "append-conflict",
            mutation_metadata: &mutation_metadata,
            append_window_step_key: StepKey::new("gate"),
            steps: vec![first_step],
        },
    )
    .await
    .expect("first append succeeds");

    let error = append_workflow_steps(
        &pool,
        &AppendWorkflowStepsInput {
            workflow_run_id: run.id,
            organization_id: None,
            mutation_key: "append-conflict",
            mutation_metadata: &mutation_metadata,
            append_window_step_key: StepKey::new("gate"),
            steps: vec![changed_step],
        },
    )
    .await
    .expect_err("conflicting append retry should be rejected");

    assert_eq!(
        query_error_code(&error),
        Some("workflow.append_conflicting_retry")
    );
    assert_public_error_does_not_expose(&error, "append-conflict");
    let runledger_postgres::Error::QueryError(query_error) = &error else {
        panic!("expected query error");
    };
    assert!(query_error.internal_message().contains("append-conflict"));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn append_workflow_steps_matches_legacy_persisted_request_shape() {
    let (pool, database) = setup_ephemeral_pool("workflow_append_legacy_retry", 8).await;
    record_postgres_18_server_version(&pool, "workflow append legacy retry").await;

    let organization_id = Uuid::now_v7();
    let workflow_type = WorkflowType::new("workflow.test.append_legacy_retry");
    let gate_payload = json!({"kind": "append-legacy-gate"});
    let metadata = json!({"source": "test"});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &gate_payload)
        .try_build()
        .expect("build external gate");
    let workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .organization_id(organization_id)
        .step(gate)
        .try_build()
        .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow");

    let append_payload = json!({"kind": "append-legacy"});
    let appended =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("appended"), &append_payload)
            .try_build()
            .expect("build appended step");
    let mutation_metadata = json!({});
    let input = AppendWorkflowStepsInput {
        workflow_run_id: run.id,
        organization_id: Some(organization_id),
        mutation_key: "append-legacy",
        mutation_metadata: &mutation_metadata,
        append_window_step_key: StepKey::new("gate"),
        steps: vec![appended],
    };

    let first = append_workflow_steps(&pool, &input)
        .await
        .expect("first append succeeds");
    assert_eq!(first.outcome, AppendWorkflowStepsOutcome::Appended);

    sqlx::query(
        "UPDATE workflow_run_mutations
         SET request = jsonb_set(
             jsonb_set(
                 (request - 'append_window_step_key') #- '{steps,0,organization_id}',
                 '{future_root_field}',
                 'true'::jsonb
             ),
             '{steps,0,future_step_field}',
             'true'::jsonb
         )
         WHERE workflow_run_id = $1
           AND mutation_key = $2",
    )
    .bind(run.id)
    .bind("append-legacy")
    .execute(&pool)
    .await
    .expect("rewrite mutation request into a legacy-compatible shape");

    let retry = append_workflow_steps(&pool, &input)
        .await
        .expect("legacy-shaped identical retry returns existing mutation");
    assert_eq!(retry.outcome, AppendWorkflowStepsOutcome::AlreadyApplied);
    assert_eq!(retry.appended_steps[0].id, first.appended_steps[0].id);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn append_workflow_steps_tx_rejects_non_read_committed_transaction() {
    let (pool, database) = setup_ephemeral_pool("workflow_append_isolation", 8).await;

    let workflow_type = WorkflowType::new("workflow.test.append_isolation");
    let gate_payload = json!({"kind": "append-isolation-gate"});
    let metadata = json!({"source": "test"});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &gate_payload)
        .try_build()
        .expect("build external gate");
    let workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .step(gate)
        .try_build()
        .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow");

    let append_payload = json!({"kind": "append-isolation"});
    let appended =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("appended"), &append_payload)
            .try_build()
            .expect("build appended step");
    let mutation_metadata = json!({});
    let input = AppendWorkflowStepsInput {
        workflow_run_id: run.id,
        organization_id: None,
        mutation_key: "append-isolation",
        mutation_metadata: &mutation_metadata,
        append_window_step_key: StepKey::new("gate"),
        steps: vec![appended],
    };

    let mut tx = pool.begin().await.expect("begin repeatable read tx");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await
        .expect("set repeatable read isolation");
    let error = append_workflow_steps_tx(&mut tx, &input)
        .await
        .expect_err("append mutation should reject non-read-committed isolation");
    assert_eq!(
        query_error_code(&error),
        Some("workflow.append_unsupported_isolation")
    );
    tx.rollback().await.expect("rollback repeatable read tx");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn append_workflow_steps_release_conflict_can_be_committed_without_partial_mutation() {
    let (pool, database) = setup_ephemeral_pool("workflow_append_release_conflict_commit", 8).await;

    let workflow_type = WorkflowType::new("workflow.test.append_release_conflict_commit");
    let gate_payload = json!({"kind": "append-release-conflict-gate"});
    let metadata = json!({"source": "test"});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &gate_payload)
        .try_build()
        .expect("build external gate");
    let workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .step(gate)
        .try_build()
        .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow");

    let append_payload = json!({"kind": "append-release-conflict"});
    let appended =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("appended"), &append_payload)
            .try_build()
            .expect("build appended step");
    let mutation_metadata = json!({});
    let input = AppendWorkflowStepsInput {
        workflow_run_id: run.id,
        organization_id: None,
        mutation_key: "append-release-conflict",
        mutation_metadata: &mutation_metadata,
        append_window_step_key: StepKey::new("gate"),
        steps: vec![appended],
    };

    let mut release_lock_tx = pool.begin().await.expect("begin release lock tx");
    sqlx::query!(
        "SELECT pg_advisory_xact_lock($1)",
        workflow_run_release_lock_key(run.id)
    )
    .execute(&mut *release_lock_tx)
    .await
    .expect("hold exclusive workflow release lock");

    let mut append_tx = pool.begin().await.expect("begin append tx");
    let error = append_workflow_steps_tx(&mut append_tx, &input)
        .await
        .expect_err("exclusive release lock should make append conflict");
    assert_eq!(query_error_code(&error), Some("workflow.release_conflict"));
    append_tx
        .commit()
        .await
        .expect("commit after conflict should not persist partial append");
    release_lock_tx
        .rollback()
        .await
        .expect("release exclusive workflow release lock");

    let steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list workflow steps");
    assert!(
        steps
            .iter()
            .all(|step| step.step_key.as_str() != "appended")
    );

    let mutation_count: i64 = sqlx::query_scalar(
        "SELECT count(*)
         FROM workflow_run_mutations
         WHERE workflow_run_id = $1
           AND mutation_key = $2",
    )
    .bind(run.id)
    .bind("append-release-conflict")
    .fetch_one(&pool)
    .await
    .expect("count append mutation rows");
    assert_eq!(mutation_count, 0);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn append_workflow_steps_idempotency_uses_jsonb_numeric_equality() {
    let (pool, database) = setup_ephemeral_pool("workflow_append_numeric_retry", 8).await;

    let workflow_type = WorkflowType::new("workflow.test.append_numeric_retry");
    let gate_payload = json!({"kind": "append-numeric-gate"});
    let metadata = json!({"source": "test"});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &gate_payload)
        .try_build()
        .expect("build external gate");
    let workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .step(gate)
        .try_build()
        .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow");

    let first_payload: Value = serde_json::from_str(
        r#"{"amount": 1.0, "items": [{"quantity": 2.0}], "fraction": 0.10, "scientific": 1e2, "negative_zero": -0}"#,
    )
    .expect("parse first numeric payload");
    let retry_payload = json!({
        "amount": 1,
        "items": [{"quantity": 2}],
        "fraction": 0.1,
        "scientific": 100,
        "negative_zero": 0,
    });
    let first_step =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("appended"), &first_payload)
            .try_build()
            .expect("build first numeric append step");
    let retry_step =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("appended"), &retry_payload)
            .try_build()
            .expect("build retry numeric append step");
    let mutation_metadata = json!({});

    append_workflow_steps(
        &pool,
        &AppendWorkflowStepsInput {
            workflow_run_id: run.id,
            organization_id: None,
            mutation_key: "append-numeric",
            mutation_metadata: &mutation_metadata,
            append_window_step_key: StepKey::new("gate"),
            steps: vec![first_step],
        },
    )
    .await
    .expect("first append succeeds");

    let retry = append_workflow_steps(
        &pool,
        &AppendWorkflowStepsInput {
            workflow_run_id: run.id,
            organization_id: None,
            mutation_key: "append-numeric",
            mutation_metadata: &mutation_metadata,
            append_window_step_key: StepKey::new("gate"),
            steps: vec![retry_step],
        },
    )
    .await
    .expect("numeric-equivalent append retry returns existing mutation");
    assert_eq!(retry.outcome, AppendWorkflowStepsOutcome::AlreadyApplied);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn append_workflow_steps_identical_retry_after_closed_window_is_already_applied() {
    let (pool, database) = setup_ephemeral_pool("workflow_append_retry_closed_window", 8).await;

    let workflow_type = WorkflowType::new("workflow.test.append_retry_closed_window");
    let gate_payload = json!({"kind": "append-closed-window-gate"});
    let metadata = json!({"source": "test"});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &gate_payload)
        .try_build()
        .expect("build external gate");
    let workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .step(gate)
        .try_build()
        .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow");

    let append_payload = json!({"kind": "append-after-closed-window"});
    let appended =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("appended"), &append_payload)
            .try_build()
            .expect("build appended step");
    let mutation_metadata = json!({});
    let input = AppendWorkflowStepsInput {
        workflow_run_id: run.id,
        organization_id: None,
        mutation_key: "append-closed-window",
        mutation_metadata: &mutation_metadata,
        append_window_step_key: StepKey::new("gate"),
        steps: vec![appended],
    };

    let first = append_workflow_steps(&pool, &input)
        .await
        .expect("first append succeeds");
    assert_eq!(first.outcome, AppendWorkflowStepsOutcome::Appended);

    let completed_gate = complete_external_workflow_step(
        &pool,
        &CompleteExternalWorkflowStepInput {
            workflow_run_id: run.id,
            organization_id: None,
            step_key: StepKey::new("gate"),
            outcome: ExternalWorkflowStepTerminalOutcome::Succeeded { output: None },
            status_reason: None,
            last_error_code: None,
            last_error_message: None,
        },
    )
    .await
    .expect("complete append window external step");
    assert_eq!(completed_gate.status, WorkflowStepStatus::Succeeded);

    let retry = append_workflow_steps(&pool, &input)
        .await
        .expect("identical append retry after closed window returns existing mutation");
    assert_eq!(retry.outcome, AppendWorkflowStepsOutcome::AlreadyApplied);
    assert_eq!(retry.appended_steps.len(), 1);
    assert_eq!(retry.appended_steps[0].id, first.appended_steps[0].id);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_enqueue_idempotency_survives_appended_steps() {
    let (pool, database) = setup_ephemeral_pool("workflow_idempotent_after_append", 8).await;

    let workflow_type = WorkflowType::new("workflow.test.idempotent_after_append");
    let payload = json!({"kind": "append"});
    let metadata = json!({"source": "test"});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build external gate");
    let workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .idempotency_key("same-workflow-after-append")
        .step(gate)
        .try_build()
        .expect("build workflow");

    let first = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("first workflow enqueue succeeds");
    let appended_payload = json!({"kind": "appended"});
    let appended =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("appended"), &appended_payload)
            .try_build()
            .expect("build appended external step");
    let mutation_metadata = json!({});
    append_workflow_steps(
        &pool,
        &AppendWorkflowStepsInput {
            workflow_run_id: first.id,
            organization_id: None,
            mutation_key: "append-1",
            mutation_metadata: &mutation_metadata,
            append_window_step_key: StepKey::new("gate"),
            steps: vec![appended],
        },
    )
    .await
    .expect("append workflow step");

    let second = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("idempotent retry after append returns existing run");
    assert_eq!(first.id, second.id);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_enqueue_idempotency_survives_pending_step_payload_mutation() {
    let (pool, database) =
        setup_ephemeral_pool("workflow_idempotent_after_payload_mutation", 8).await;

    let workflow_type = WorkflowType::new("workflow.test.idempotent_after_payload_mutation");
    let job_type = JobType::new("jobs.test.workflow_payload_mutation");
    register_job_definition(&pool, job_type).await;

    let original_payload = json!({"kind": "original"});
    let changed_payload = json!({"kind": "changed"});
    let metadata = json!({"source": "test"});
    let step = WorkflowStepEnqueueBuilder::new(StepKey::new("root"), job_type, &original_payload)
        .try_build()
        .expect("build job step");
    let workflow = WorkflowRunEnqueueBuilder::new(workflow_type, &metadata)
        .idempotency_key("same-workflow-after-payload-mutation")
        .step(step)
        .try_build()
        .expect("build workflow");

    let first = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("first workflow enqueue succeeds");
    let step = list_workflow_steps(&pool, None, first.id)
        .await
        .expect("list workflow steps")
        .into_iter()
        .next()
        .expect("workflow step exists");
    let job_id = step.job_id.expect("root job should be enqueued");
    let mut tx = pool.begin().await.expect("begin payload update tx");
    let updated = update_workflow_step_and_pending_job_payload_tx(
        &mut tx,
        first.id,
        None,
        step.id,
        job_id,
        &changed_payload,
    )
    .await
    .expect("update pending workflow step payload");
    assert!(updated);
    tx.commit().await.expect("commit payload update tx");

    let second = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("idempotent retry after payload mutation returns existing run");
    assert_eq!(first.id, second.id);

    teardown_ephemeral_pool(pool, database).await;
}
