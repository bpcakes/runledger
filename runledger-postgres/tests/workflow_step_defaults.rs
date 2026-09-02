use runledger_core::jobs::{
    JobStage, JobType, StepKey, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder,
    WorkflowStepExecutionKind, WorkflowStepStatus, WorkflowType,
};
use runledger_postgres::jobs::{
    AppendWorkflowStepsInput, JobDefinitionUpsert, WorkflowStepDbRecord, append_workflow_steps,
    append_workflow_steps_tx, enqueue_workflow_run, enqueue_workflow_run_tx, list_workflow_steps,
    upsert_job_definition_tx,
};
use runledger_postgres::{DbPool, Error, QueryErrorCategory};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;

const DEFAULTED_JOB_TYPE: &str = "jobs.test.workflow_step_defaults";
const MISSING_JOB_TYPE: &str = "jobs.test.workflow_step_defaults_missing";
const DEFAULT_PRIORITY: i32 = 101;
const DEFAULT_MAX_ATTEMPTS: i32 = 7;
const DEFAULT_TIMEOUT_SECONDS: i32 = 63;
const OVERRIDE_PRIORITY: i32 = -41;
const OVERRIDE_MAX_ATTEMPTS: i32 = 2;
const OVERRIDE_TIMEOUT_SECONDS: i32 = 19;

async fn record_postgres_server_version(pool: &DbPool, diagnostic: &str) {
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
        "workflow step defaults regression must run on PostgreSQL 18"
    );
}

async fn register_defaulted_job_definition(pool: &DbPool) {
    let mut tx = pool.begin().await.expect("begin definition setup");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(DEFAULTED_JOB_TYPE),
            version: 1,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            default_timeout_seconds: DEFAULT_TIMEOUT_SECONDS,
            default_priority: DEFAULT_PRIORITY,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert workflow defaults definition");
    tx.commit().await.expect("commit definition setup");
}

fn step_by_key<'a>(steps: &'a [WorkflowStepDbRecord], step_key: &str) -> &'a WorkflowStepDbRecord {
    steps
        .iter()
        .find(|step| step.step_key.as_str() == step_key)
        .unwrap_or_else(|| panic!("missing workflow step '{step_key}'"))
}

fn assert_resolved_job_defaults_overrides_and_external_nullability(steps: &[WorkflowStepDbRecord]) {
    assert_eq!(steps.len(), 3);
    assert_omitted_defaults_step(step_by_key(steps, "omitted-defaults"));
    assert_explicit_overrides_step(step_by_key(steps, "explicit-overrides"));
    assert_external_step_nullability(step_by_key(steps, "external"));
}

fn assert_omitted_defaults_step(omitted: &WorkflowStepDbRecord) {
    assert_eq!(omitted.execution_kind, WorkflowStepExecutionKind::Job);
    assert_eq!(
        omitted.job_type.as_ref().map(|job_type| job_type.as_str()),
        Some(DEFAULTED_JOB_TYPE)
    );
    assert_eq!(omitted.priority, Some(DEFAULT_PRIORITY));
    assert_eq!(omitted.max_attempts, Some(DEFAULT_MAX_ATTEMPTS));
    assert_eq!(omitted.timeout_seconds, Some(DEFAULT_TIMEOUT_SECONDS));
    assert_eq!(omitted.stage, Some(JobStage::Queued));
}

fn assert_explicit_overrides_step(override_step: &WorkflowStepDbRecord) {
    assert_eq!(override_step.execution_kind, WorkflowStepExecutionKind::Job);
    assert_eq!(
        override_step
            .job_type
            .as_ref()
            .map(|job_type| job_type.as_str()),
        Some(DEFAULTED_JOB_TYPE)
    );
    assert_eq!(override_step.priority, Some(OVERRIDE_PRIORITY));
    assert_eq!(override_step.max_attempts, Some(OVERRIDE_MAX_ATTEMPTS));
    assert_eq!(
        override_step.timeout_seconds,
        Some(OVERRIDE_TIMEOUT_SECONDS)
    );
    assert_eq!(override_step.stage, Some(JobStage::Queued));
}

fn assert_external_step_nullability(external: &WorkflowStepDbRecord) {
    assert_eq!(external.execution_kind, WorkflowStepExecutionKind::External);
    assert!(external.job_type.is_none());
    assert!(external.priority.is_none());
    assert!(external.max_attempts.is_none());
    assert!(external.timeout_seconds.is_none());
    assert!(external.stage.is_none());
    assert_eq!(external.status, WorkflowStepStatus::WaitingForExternal);
}

fn assert_definition_not_available(error: Error) {
    let Error::QueryError(query_error) = error else {
        panic!("expected workflow definition validation error");
    };
    assert_eq!(query_error.category(), QueryErrorCategory::Validation);
    assert_eq!(
        query_error.code(),
        "workflow.definition_not_found_or_disabled"
    );
}

fn omitted_defaults_step(
    payload: &serde_json::Value,
) -> runledger_core::jobs::WorkflowStepEnqueue<'_> {
    WorkflowStepEnqueueBuilder::new(
        StepKey::new("omitted-defaults"),
        JobType::new(DEFAULTED_JOB_TYPE),
        payload,
    )
    .try_build()
    .expect("build job step with omitted defaults")
}

fn explicit_overrides_step(
    payload: &serde_json::Value,
) -> runledger_core::jobs::WorkflowStepEnqueue<'_> {
    WorkflowStepEnqueueBuilder::new(
        StepKey::new("explicit-overrides"),
        JobType::new(DEFAULTED_JOB_TYPE),
        payload,
    )
    .priority(OVERRIDE_PRIORITY)
    .max_attempts(OVERRIDE_MAX_ATTEMPTS)
    .timeout_seconds(OVERRIDE_TIMEOUT_SECONDS)
    .try_build()
    .expect("build job step with explicit overrides")
}

fn external_step(payload: &serde_json::Value) -> runledger_core::jobs::WorkflowStepEnqueue<'_> {
    WorkflowStepEnqueueBuilder::new_external(StepKey::new("external"), payload)
        .try_build()
        .expect("build external step")
}

#[tokio::test]
async fn initial_step_insertion_resolves_defaults_preserves_overrides_and_external_nullability() {
    let (pool, database) = setup_ephemeral_pool("workflow_initial_step_defaults", 4).await;
    record_postgres_server_version(&pool, "initial workflow step defaults regression").await;
    register_defaulted_job_definition(&pool).await;

    let metadata = json!({"path": "initial-defaults"});
    let payload = json!({"path": "initial-defaults"});
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.initial_step_defaults"),
        &metadata,
    )
    .step(omitted_defaults_step(&payload))
    .step(explicit_overrides_step(&payload))
    .step(external_step(&payload))
    .try_build()
    .expect("build initial defaults workflow");

    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue initial defaults workflow");
    let steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("load initial defaults workflow steps");
    assert_resolved_job_defaults_overrides_and_external_nullability(&steps);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn appended_step_insertion_resolves_defaults_preserves_overrides_and_external_nullability() {
    let (pool, database) = setup_ephemeral_pool("workflow_appended_step_defaults", 4).await;
    record_postgres_server_version(&pool, "append workflow step defaults regression").await;
    register_defaulted_job_definition(&pool).await;

    let metadata = json!({"path": "append-defaults"});
    let payload = json!({"path": "append-defaults"});
    let append_window =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("append-window"), &payload)
            .try_build()
            .expect("build append window");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.appended_step_defaults"),
        &metadata,
    )
    .step(append_window)
    .try_build()
    .expect("build append defaults workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue append defaults workflow");

    let mutation_metadata = json!({"path": "append-defaults"});
    let append_result = append_workflow_steps(
        &pool,
        &AppendWorkflowStepsInput {
            workflow_run_id: run.id,
            organization_id: None,
            mutation_key: "append-defaults",
            mutation_metadata: &mutation_metadata,
            append_window_step_key: StepKey::new("append-window"),
            steps: vec![
                omitted_defaults_step(&payload),
                explicit_overrides_step(&payload),
                external_step(&payload),
            ],
        },
    )
    .await
    .expect("append defaults workflow steps");
    assert_resolved_job_defaults_overrides_and_external_nullability(&append_result.appended_steps);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn definition_prevalidation_prevents_partial_initial_and_append_step_rows() {
    let (pool, database) = setup_ephemeral_pool("workflow_step_defaults_prevalidation", 4).await;
    record_postgres_server_version(&pool, "workflow step defaults prevalidation regression").await;
    register_defaulted_job_definition(&pool).await;

    let metadata = json!({"path": "definition-prevalidation"});
    let payload = json!({"path": "definition-prevalidation"});
    let missing_definition_step = WorkflowStepEnqueueBuilder::new(
        StepKey::new("missing-definition"),
        JobType::new(MISSING_JOB_TYPE),
        &payload,
    )
    .try_build()
    .expect("build missing-definition step");
    let initial_workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.initial_step_defaults_prevalidation"),
        &metadata,
    )
    .step(omitted_defaults_step(&payload))
    .step(external_step(&payload))
    .step(missing_definition_step)
    .try_build()
    .expect("build initial prevalidation workflow");

    let mut initial_tx = pool.begin().await.expect("begin initial prevalidation tx");
    assert_definition_not_available(
        enqueue_workflow_run_tx(&mut initial_tx, &initial_workflow)
            .await
            .expect_err("missing definition must reject initial workflow before step insertion"),
    );
    let initial_run_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint
         FROM workflow_runs
         WHERE workflow_type = $1",
    )
    .bind("workflow.test.initial_step_defaults_prevalidation")
    .fetch_one(&mut *initial_tx)
    .await
    .expect("count initial workflow rows after rejection");
    let initial_step_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*)::bigint FROM workflow_steps")
            .fetch_one(&mut *initial_tx)
            .await
            .expect("count initial workflow step rows after rejection");
    assert_eq!(
        initial_run_count, 1,
        "initial transaction ordering remains unchanged"
    );
    assert_eq!(
        initial_step_count, 0,
        "full definition prevalidation must reject initial input before any step row is inserted"
    );
    initial_tx
        .rollback()
        .await
        .expect("roll back initial prevalidation tx");

    let append_window =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("append-window"), &payload)
            .try_build()
            .expect("build append prevalidation window");
    let append_workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.append_step_defaults_prevalidation"),
        &metadata,
    )
    .step(append_window)
    .try_build()
    .expect("build append prevalidation workflow");
    let append_run = enqueue_workflow_run(&pool, &append_workflow)
        .await
        .expect("enqueue append prevalidation workflow");

    let missing_definition_append_step = WorkflowStepEnqueueBuilder::new(
        StepKey::new("missing-definition"),
        JobType::new(MISSING_JOB_TYPE),
        &payload,
    )
    .try_build()
    .expect("build missing-definition append step");
    let mutation_metadata = json!({"path": "append-prevalidation"});
    let mut append_tx = pool.begin().await.expect("begin append prevalidation tx");
    assert_definition_not_available(
        append_workflow_steps_tx(
            &mut append_tx,
            &AppendWorkflowStepsInput {
                workflow_run_id: append_run.id,
                organization_id: None,
                mutation_key: "append-prevalidation",
                mutation_metadata: &mutation_metadata,
                append_window_step_key: StepKey::new("append-window"),
                steps: vec![
                    omitted_defaults_step(&payload),
                    external_step(&payload),
                    missing_definition_append_step,
                ],
            },
        )
        .await
        .expect_err("missing definition must reject append before any appended row is inserted"),
    );
    let append_step_count = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint FROM workflow_steps WHERE workflow_run_id = $1",
    )
    .bind(append_run.id)
    .fetch_one(&mut *append_tx)
    .await
    .expect("count append workflow step rows after rejection");
    assert_eq!(
        append_step_count, 1,
        "full definition prevalidation must leave the append window as the only persisted step"
    );
    append_tx
        .rollback()
        .await
        .expect("roll back append prevalidation tx");

    teardown_ephemeral_pool(pool, database).await;
}
