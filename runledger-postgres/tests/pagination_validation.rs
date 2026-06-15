use std::collections::HashSet;

use runledger_core::jobs::{
    JobType, StepKey, WorkflowDependencyReleaseMode, WorkflowRunEnqueueBuilder,
    WorkflowStepEnqueueBuilder, WorkflowType,
};
use runledger_postgres::jobs::{
    JOB_LIST_PAGE_LIMIT_MAX, JobDefinitionListFilter, JobDefinitionUpsert, JobListFilter,
    JobRuntimeConfigListFilter, WorkflowRunListFilter, count_workflow_step_dependencies,
    count_workflow_steps, enqueue_workflow_run, list_job_definitions, list_job_events,
    list_job_logs, list_job_runtime_configs, list_jobs, list_workflow_runs,
    list_workflow_step_dependencies_page, list_workflow_steps, list_workflow_steps_page,
    upsert_job_definition_tx,
};
use runledger_postgres::{DbPool, Error, QueryErrorCategory};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::types::Uuid;

fn disconnected_pool() -> DbPool {
    PgPoolOptions::new()
        .max_connections(1)
        .connect_lazy("postgres://runledger:runledger@127.0.0.1:1/runledger")
        .expect("create lazy pool")
}

async fn register_job_definition(pool: &DbPool, job_type: JobType<'static>) {
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

fn assert_invalid_pagination<T>(result: runledger_postgres::Result<T>) {
    match result {
        Err(Error::QueryError(query_error)) => {
            assert_eq!(query_error.category(), QueryErrorCategory::Validation);
            assert_eq!(query_error.code(), "job.invalid_pagination");
            assert_eq!(
                query_error.client_message(),
                "Pagination limit and offset are invalid."
            );
            assert!(
                query_error.source_arc().is_none(),
                "pagination validation should run before SQL execution"
            );
        }
        Err(other) => panic!("expected pagination validation error, got {other:?}"),
        Ok(_) => panic!("expected pagination validation error, got Ok"),
    }
}

#[tokio::test]
async fn invalid_pagination_rejects_before_database_access() {
    let pool = disconnected_pool();
    let job_id = Uuid::nil();

    assert_invalid_pagination(
        list_jobs(
            &pool,
            &JobListFilter {
                organization_id: None,
                status: None,
                job_type: None,
                limit: 0,
                offset: 0,
            },
        )
        .await,
    );
    assert_invalid_pagination(
        list_jobs(
            &pool,
            &JobListFilter {
                organization_id: None,
                status: None,
                job_type: None,
                limit: -1,
                offset: 0,
            },
        )
        .await,
    );
    assert_invalid_pagination(
        list_jobs(
            &pool,
            &JobListFilter {
                organization_id: None,
                status: None,
                job_type: None,
                limit: JOB_LIST_PAGE_LIMIT_MAX + 1,
                offset: 0,
            },
        )
        .await,
    );
    assert_invalid_pagination(
        list_jobs(
            &pool,
            &JobListFilter {
                organization_id: None,
                status: None,
                job_type: None,
                limit: 1,
                offset: -1,
            },
        )
        .await,
    );

    assert_invalid_pagination(list_job_events(&pool, None, job_id, 0, None).await);
    assert_invalid_pagination(
        list_job_events(&pool, None, job_id, JOB_LIST_PAGE_LIMIT_MAX + 1, None).await,
    );
    assert_invalid_pagination(list_job_logs(&pool, None, job_id, 0, None).await);
    assert_invalid_pagination(
        list_job_logs(&pool, None, job_id, JOB_LIST_PAGE_LIMIT_MAX + 1, None).await,
    );

    assert_invalid_pagination(
        list_workflow_runs(
            &pool,
            &WorkflowRunListFilter {
                organization_id: None,
                status: None,
                workflow_type: None,
                limit: 0,
                offset: 0,
            },
        )
        .await,
    );
    assert_invalid_pagination(
        list_workflow_runs(
            &pool,
            &WorkflowRunListFilter {
                organization_id: None,
                status: None,
                workflow_type: None,
                limit: JOB_LIST_PAGE_LIMIT_MAX + 1,
                offset: 0,
            },
        )
        .await,
    );
    assert_invalid_pagination(
        list_workflow_runs(
            &pool,
            &WorkflowRunListFilter {
                organization_id: None,
                status: None,
                workflow_type: None,
                limit: 1,
                offset: -1,
            },
        )
        .await,
    );

    assert_invalid_pagination(
        list_job_definitions(
            &pool,
            &JobDefinitionListFilter {
                job_type: None,
                limit: 0,
                offset: 0,
            },
        )
        .await,
    );
    assert_invalid_pagination(
        list_job_definitions(
            &pool,
            &JobDefinitionListFilter {
                job_type: None,
                limit: JOB_LIST_PAGE_LIMIT_MAX + 1,
                offset: 0,
            },
        )
        .await,
    );
    assert_invalid_pagination(
        list_job_definitions(
            &pool,
            &JobDefinitionListFilter {
                job_type: None,
                limit: 1,
                offset: -1,
            },
        )
        .await,
    );

    assert_invalid_pagination(
        list_job_runtime_configs(
            &pool,
            &JobRuntimeConfigListFilter {
                job_type: None,
                limit: 0,
                offset: 0,
            },
        )
        .await,
    );
    assert_invalid_pagination(
        list_job_runtime_configs(
            &pool,
            &JobRuntimeConfigListFilter {
                job_type: None,
                limit: JOB_LIST_PAGE_LIMIT_MAX + 1,
                offset: 0,
            },
        )
        .await,
    );
    assert_invalid_pagination(
        list_job_runtime_configs(
            &pool,
            &JobRuntimeConfigListFilter {
                job_type: None,
                limit: 1,
                offset: -1,
            },
        )
        .await,
    );

    assert_invalid_pagination(list_workflow_steps_page(&pool, None, job_id, 0, 0).await);
    assert_invalid_pagination(
        list_workflow_steps_page(&pool, None, job_id, JOB_LIST_PAGE_LIMIT_MAX + 1, 0).await,
    );
    assert_invalid_pagination(list_workflow_steps_page(&pool, None, job_id, 1, -1).await);

    assert_invalid_pagination(
        list_workflow_step_dependencies_page(&pool, None, job_id, 0, 0).await,
    );
    assert_invalid_pagination(
        list_workflow_step_dependencies_page(&pool, None, job_id, JOB_LIST_PAGE_LIMIT_MAX + 1, 0)
            .await,
    );
    assert_invalid_pagination(
        list_workflow_step_dependencies_page(&pool, None, job_id, 1, -1).await,
    );
}

#[tokio::test]
async fn valid_pagination_limits_still_execute_list_queries() {
    let (pool, database) = setup_ephemeral_pool("postgres_pagination_validation", 4).await;
    let job_id = Uuid::nil();

    let jobs = list_jobs(
        &pool,
        &JobListFilter {
            organization_id: None,
            status: None,
            job_type: None,
            limit: 1,
            offset: 0,
        },
    )
    .await
    .expect("list jobs with valid pagination");
    assert!(jobs.is_empty());

    let events = list_job_events(&pool, None, job_id, 1, None)
        .await
        .expect("list job events with valid pagination");
    assert!(events.is_empty());

    let logs = list_job_logs(&pool, None, job_id, 1, None)
        .await
        .expect("list job logs with valid pagination");
    assert!(logs.is_empty());

    let workflow_runs = list_workflow_runs(
        &pool,
        &WorkflowRunListFilter {
            organization_id: None,
            status: None,
            workflow_type: None,
            limit: 1,
            offset: 0,
        },
    )
    .await
    .expect("list workflow runs with valid pagination");
    assert!(workflow_runs.is_empty());

    let definitions = list_job_definitions(
        &pool,
        &JobDefinitionListFilter {
            job_type: None,
            limit: 1,
            offset: 0,
        },
    )
    .await
    .expect("list job definitions with valid pagination");
    assert!(definitions.is_empty());

    let runtime_configs = list_job_runtime_configs(
        &pool,
        &JobRuntimeConfigListFilter {
            job_type: None,
            limit: 1,
            offset: 0,
        },
    )
    .await
    .expect("list runtime configs with valid pagination");
    assert!(runtime_configs.is_empty());

    let workflow_steps = list_workflow_steps_page(&pool, None, job_id, 1, 0)
        .await
        .expect("list workflow steps with valid pagination");
    assert!(workflow_steps.is_empty());

    let workflow_step_dependencies =
        list_workflow_step_dependencies_page(&pool, None, job_id, 1, 0)
            .await
            .expect("list workflow step dependencies with valid pagination");
    assert!(workflow_step_dependencies.is_empty());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_detail_page_readers_decode_populated_rows() {
    let (pool, database) = setup_ephemeral_pool("postgres_workflow_detail_page_rows", 4).await;
    let job_type = JobType::new("jobs.test.workflow_detail_page");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"case": "workflow-detail-page"});
    let root = WorkflowStepEnqueueBuilder::new(StepKey::new("root"), job_type, &payload)
        .priority(25)
        .try_build()
        .expect("build root step");
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .depends_on_success(&[StepKey::new("root")])
        .try_build()
        .expect("build gate step");
    let child = WorkflowStepEnqueueBuilder::new(StepKey::new("child"), job_type, &payload)
        .depends_on_success(&[StepKey::new("root")])
        .depends_on_terminal(&[StepKey::new("gate")])
        .try_build()
        .expect("build child step");
    let metadata = json!({"case": "workflow-detail-page"});
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.workflow_detail_page"),
        &metadata,
    )
    .step(root)
    .step(gate)
    .step(child)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow");

    let workflow_step_count = count_workflow_steps(&pool, None, run.id)
        .await
        .expect("count workflow steps");
    assert_eq!(workflow_step_count, 3);
    assert_eq!(
        count_workflow_step_dependencies(&pool, None, run.id)
            .await
            .expect("count workflow step dependencies"),
        3
    );

    let all_steps = list_workflow_steps_page(&pool, None, run.id, 3, 0)
        .await
        .expect("list workflow step page");
    assert_eq!(all_steps.len(), 3);
    let root_step = all_steps
        .iter()
        .find(|step| step.step_key.as_str() == "root")
        .expect("root step should decode");
    assert_eq!(
        root_step.job_type.as_ref().map(|value| value.as_str()),
        Some(job_type.as_str())
    );
    assert_eq!(root_step.priority, Some(25));
    let gate_step = all_steps
        .iter()
        .find(|step| step.step_key.as_str() == "gate")
        .expect("gate step should decode");
    assert!(gate_step.job_type.is_none());
    let child_step = all_steps
        .iter()
        .find(|step| step.step_key.as_str() == "child")
        .expect("child step should decode");
    assert_eq!(
        child_step.job_type.as_ref().map(|value| value.as_str()),
        Some(job_type.as_str())
    );

    let first_two_steps = list_workflow_steps_page(&pool, None, run.id, 2, 0)
        .await
        .expect("list partial workflow step page");
    assert_eq!(first_two_steps.len(), 2);

    let legacy_steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list legacy workflow steps");
    assert_eq!(
        legacy_steps.iter().map(|step| step.id).collect::<Vec<_>>(),
        all_steps.iter().map(|step| step.id).collect::<Vec<_>>(),
        "legacy and paged workflow step readers should use the same stable ordering"
    );

    let mut single_step_page_ids = Vec::new();
    for offset in 0..workflow_step_count {
        let page = list_workflow_steps_page(&pool, None, run.id, 1, offset)
            .await
            .expect("list single workflow step page");
        assert_eq!(page.len(), 1);
        single_step_page_ids.push(page[0].id);
    }
    let unique_single_step_page_ids = single_step_page_ids.iter().copied().collect::<HashSet<_>>();
    assert_eq!(
        unique_single_step_page_ids.len(),
        workflow_step_count as usize
    );
    assert_eq!(
        unique_single_step_page_ids,
        all_steps.iter().map(|step| step.id).collect::<HashSet<_>>(),
        "one-row pages should cover each workflow step exactly once"
    );

    let dependencies = list_workflow_step_dependencies_page(&pool, None, run.id, 3, 0)
        .await
        .expect("list workflow dependency page");
    assert_eq!(dependencies.len(), 3);
    let has_success = dependencies
        .iter()
        .any(|dependency| dependency.release_mode == WorkflowDependencyReleaseMode::OnSuccess);
    let has_terminal = dependencies
        .iter()
        .any(|dependency| dependency.release_mode == WorkflowDependencyReleaseMode::OnTerminal);
    assert!(has_success, "dependency page should decode OnSuccess rows");
    assert!(
        has_terminal,
        "dependency page should decode OnTerminal rows"
    );

    teardown_ephemeral_pool(pool, database).await;
}
