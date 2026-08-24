use runledger_core::jobs::{
    JobType, StepKey, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder, WorkflowType,
};
use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    AdminDataProjection, AdminSensitiveData, JobDefinitionUpsert, JobEnqueue, JobLogRecordInput,
    enqueue_job, enqueue_workflow_run, get_admin_job_by_id, get_admin_workflow_by_id,
    insert_job_log, list_admin_job_events, list_admin_job_logs, list_admin_workflow_steps,
    upsert_job_definition_tx,
};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::types::Uuid;

const JOB_TYPE: &str = "jobs.admin.projection";

async fn register_definition(pool: &DbPool) {
    let mut tx = pool.begin().await.expect("begin definition transaction");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(JOB_TYPE),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit definition transaction");
}

async fn create_metadata_reader(pool: &DbPool, database_url: &str, role: &str) -> DbPool {
    let grants = format!(
        "CREATE ROLE {role};
         GRANT USAGE ON SCHEMA public TO {role};
         GRANT SELECT (
             id, job_type, organization_id, status, priority, run_number, attempt,
             max_attempts, timeout_seconds, next_run_at, lease_expires_at,
             last_heartbeat_at, started_at, finished_at, stage, progress_done,
             progress_total, progress_pct, last_error_code, created_at, updated_at
         ) ON job_queue TO {role};
         GRANT SELECT (
             id, workflow_type, organization_id, status, result_step_key,
             started_at, finished_at, created_at, updated_at
         ) ON workflow_runs TO {role};
         GRANT SELECT (
             id, workflow_run_id, step_key, execution_kind, job_type, organization_id,
             priority, max_attempts, timeout_seconds, stage, allow_handler_continuation,
             status, job_id, released_at, started_at, finished_at,
             dependency_count_total, dependency_count_pending,
             dependency_count_unsatisfied, last_error_code, created_at, updated_at
         ) ON workflow_steps TO {role};
         GRANT SELECT (
             workflow_run_id, prerequisite_step_id, dependent_step_id, release_mode,
             created_at
         ) ON workflow_step_dependencies TO {role};
         GRANT SELECT (
             id, job_id, run_number, attempt, event_type, stage, progress_done,
             progress_total, occurred_at
         ) ON job_events TO {role};
         GRANT SELECT (id, job_id, run_number, attempt, level, occurred_at)
         ON job_logs TO {role};"
    );
    sqlx::raw_sql(&grants)
        .execute(pool)
        .await
        .expect("create metadata-only database role");

    let restricted = PgPoolOptions::new()
        .max_connections(1)
        .connect(database_url)
        .await
        .expect("connect restricted metadata pool");
    sqlx::query(&format!("SET ROLE {role}"))
        .execute(&restricted)
        .await
        .expect("assume metadata-only database role");
    restricted
}

async fn assert_full_projections_are_denied(
    pool: &DbPool,
    organization_id: Uuid,
    job_id: Uuid,
    workflow_id: Uuid,
) {
    assert!(
        get_admin_job_by_id(
            pool,
            Some(organization_id),
            job_id,
            AdminDataProjection::Full,
        )
        .await
        .is_err(),
        "full job detail must require sensitive job_queue columns"
    );
    assert!(
        get_admin_workflow_by_id(
            pool,
            Some(organization_id),
            workflow_id,
            AdminDataProjection::Full,
        )
        .await
        .is_err(),
        "full workflow detail must require sensitive workflow_runs columns"
    );
    assert!(
        list_admin_workflow_steps(
            pool,
            Some(organization_id),
            workflow_id,
            10,
            0,
            AdminDataProjection::Full,
        )
        .await
        .is_err(),
        "full workflow steps must require sensitive workflow_steps columns"
    );
    assert!(
        list_admin_job_events(
            pool,
            Some(organization_id),
            job_id,
            10,
            None,
            AdminDataProjection::Full,
        )
        .await
        .is_err(),
        "full events must require job_events.payload"
    );
    assert!(
        list_admin_job_logs(
            pool,
            Some(organization_id),
            job_id,
            10,
            None,
            AdminDataProjection::Full,
        )
        .await
        .is_err(),
        "full logs must require job_logs message and payload"
    );
}

#[tokio::test]
async fn metadata_only_admin_reads_do_not_require_sensitive_column_privileges() {
    let (pool, database) = setup_ephemeral_pool("admin_projection", 4).await;
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("read PostgreSQL server version");
    eprintln!("admin projection regression PostgreSQL server_version={server_version}");

    register_definition(&pool).await;
    let organization_id = Uuid::now_v7();
    let job_payload = json!({"job_secret": "not-selected"});
    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: Some(organization_id),
            payload: &job_payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: Some("admin-projection-job"),
            stage: None,
        },
    )
    .await
    .expect("enqueue projection job");
    insert_job_log(
        &pool,
        &JobLogRecordInput {
            job_id,
            run_number: 1,
            attempt: None,
            level: "info".to_owned(),
            message: "not-selected log message".to_owned(),
            payload: json!({"log_secret": "not-selected"}),
        },
    )
    .await
    .expect("insert projection log");

    let workflow_payload = json!({"step_secret": "not-selected"});
    let step = WorkflowStepEnqueueBuilder::new(
        StepKey::new("project"),
        JobType::new(JOB_TYPE),
        &workflow_payload,
    )
    .try_build()
    .expect("build workflow step");
    let workflow_metadata = json!({"workflow_secret": "not-selected"});
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.admin.projection"),
        &workflow_metadata,
    )
    .organization_id(organization_id)
    .idempotency_key("admin-projection-workflow")
    .step(step)
    .try_build()
    .expect("build projection workflow");
    let workflow_run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue projection workflow");

    let role = format!("{}_metadata_reader", database.name());
    let restricted = create_metadata_reader(&pool, database.url(), &role).await;

    let job = get_admin_job_by_id(
        &restricted,
        Some(organization_id),
        job_id,
        AdminDataProjection::MetadataOnly,
    )
    .await
    .expect("query job metadata with restricted role")
    .expect("job is visible");
    assert!(matches!(job.sensitive, AdminSensitiveData::Redacted));

    let workflow = get_admin_workflow_by_id(
        &restricted,
        Some(organization_id),
        workflow_run.id,
        AdminDataProjection::MetadataOnly,
    )
    .await
    .expect("query workflow metadata with restricted role")
    .expect("workflow is visible");
    assert!(matches!(workflow.sensitive, AdminSensitiveData::Redacted));

    let steps = list_admin_workflow_steps(
        &restricted,
        Some(organization_id),
        workflow_run.id,
        10,
        0,
        AdminDataProjection::MetadataOnly,
    )
    .await
    .expect("query workflow step metadata with restricted role");
    assert_eq!(steps.len(), 1);
    assert!(matches!(steps[0].sensitive, AdminSensitiveData::Redacted));

    let events = list_admin_job_events(
        &restricted,
        Some(organization_id),
        job_id,
        10,
        None,
        AdminDataProjection::MetadataOnly,
    )
    .await
    .expect("query event metadata with restricted role");
    assert!(!events.is_empty());
    assert!(
        events
            .iter()
            .all(|event| matches!(event.sensitive, AdminSensitiveData::Redacted))
    );

    let logs = list_admin_job_logs(
        &restricted,
        Some(organization_id),
        job_id,
        10,
        None,
        AdminDataProjection::MetadataOnly,
    )
    .await
    .expect("query log metadata with restricted role");
    assert_eq!(logs.len(), 1);
    assert!(matches!(logs[0].sensitive, AdminSensitiveData::Redacted));

    assert_full_projections_are_denied(&restricted, organization_id, job_id, workflow_run.id).await;

    restricted.close().await;
    sqlx::raw_sql(&format!("DROP OWNED BY {role}; DROP ROLE {role};"))
        .execute(&pool)
        .await
        .expect("drop metadata-only database role");
    teardown_ephemeral_pool(pool, database).await;
}
