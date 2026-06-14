use std::time::Duration;

use runledger_core::jobs::{
    JobFailureKind, JobType, StepKey, WorkflowRunEnqueueBuilder, WorkflowRunStatus,
    WorkflowStepEnqueueBuilder, WorkflowStepStatus, WorkflowType,
};
use runledger_postgres::jobs::{
    CompleteExternalWorkflowStepInput, DEFAULT_WORKFLOW_RUN_WAIT_TIMEOUT, JobCompletionUpdate,
    JobDefinitionUpsert, JobFailureUpdate, WorkflowRunHandleError, WorkflowRunHandleScope,
    WorkflowRunWaitOptions, cancel_workflow_run_tx, claim_jobs_for_types,
    complete_external_workflow_step, complete_job_failure, complete_job_success,
    enqueue_workflow_run_handle, list_workflow_steps, retrieve_workflow_run_handle,
    upsert_job_definition_tx, workflow_run_handle,
};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::{Value, json};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use sqlx::types::Uuid;
use tokio::time::sleep;

#[test]
fn workflow_wait_options_default_is_bounded() {
    let options = WorkflowRunWaitOptions::default();

    assert_eq!(options.timeout, Some(Duration::from_secs(300)));
    assert_eq!(options.timeout, Some(DEFAULT_WORKFLOW_RUN_WAIT_TIMEOUT));
    assert_eq!(options.poll_interval, Duration::from_secs(1));
}

async fn register_job_definition(pool: &PgPool, job_type: JobType<'static>) {
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

#[tokio::test]
async fn workflow_handle_waits_for_declared_job_result() {
    let (pool, database) = setup_ephemeral_pool("workflow_handle_job_result", 8).await;
    let job_type = JobType::new("jobs.test.workflow_result");
    register_job_definition(&pool, job_type).await;

    let metadata = json!({"test": "workflow_handle_job_result"});
    let payload = json!({"work": "result"});
    let result_step = WorkflowStepEnqueueBuilder::new(StepKey::new("result"), job_type, &payload)
        .try_build()
        .expect("build result step");
    let workflow =
        WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test.result.job"), &metadata)
            .try_result_step_key("result")
            .expect("set result step")
            .step(result_step)
            .try_build()
            .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");

    let mut claimed = claim_jobs_for_types(&pool, "worker-result", 30, 1, &[job_type])
        .await
        .expect("claim result job");
    let job = claimed.pop().expect("result job should be claimable");
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");
    let expected_output = json!({"artifact_id": "artifact_123"});
    let output_for_task = expected_output.clone();
    let completion_pool = pool.clone();

    let completion = tokio::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        complete_job_success(
            &completion_pool,
            job.id,
            job.run_number,
            job.attempt,
            &worker_id,
            Some(&JobCompletionUpdate {
                progress_done: Some(1),
                progress_total: Some(1),
                checkpoint: None,
                output: Some(&output_for_task),
            }),
        )
        .await
        .expect("complete result job");
    });

    let result = handle
        .get_result(WorkflowRunWaitOptions {
            timeout: Some(Duration::from_secs(2)),
            poll_interval: Duration::from_secs(5),
        })
        .await
        .expect("workflow result should be available");
    completion.await.expect("completion task should join");

    assert_eq!(result.workflow_run_id, handle.workflow_run_id);
    assert_eq!(result.result_step_key.as_str(), "result");
    assert_eq!(result.result, expected_output);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_handle_zero_timeout_reads_committed_result() {
    let (pool, database) = setup_ephemeral_pool("workflow_handle_zero_timeout_ready", 8).await;
    let job_type = JobType::new("jobs.test.workflow_zero_timeout_ready");
    register_job_definition(&pool, job_type).await;

    let metadata = json!({"test": "workflow_handle_zero_timeout_ready"});
    let payload = json!({"work": "result"});
    let result_step = WorkflowStepEnqueueBuilder::new(StepKey::new("result"), job_type, &payload)
        .try_build()
        .expect("build result step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.zero_timeout_ready"),
        &metadata,
    )
    .try_result_step_key("result")
    .expect("set result step")
    .step(result_step)
    .try_build()
    .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");

    let mut claimed = claim_jobs_for_types(&pool, "worker-zero-timeout", 30, 1, &[job_type])
        .await
        .expect("claim result job");
    let job = claimed.pop().expect("result job should be claimable");
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");
    let expected_output = json!({"artifact_id": "artifact_zero_timeout"});
    complete_job_success(
        &pool,
        job.id,
        job.run_number,
        job.attempt,
        &worker_id,
        Some(&JobCompletionUpdate {
            progress_done: Some(1),
            progress_total: Some(1),
            checkpoint: None,
            output: Some(&expected_output),
        }),
    )
    .await
    .expect("complete result job before waiting");

    let result = handle
        .get_result(WorkflowRunWaitOptions {
            timeout: Some(Duration::ZERO),
            poll_interval: Duration::from_secs(30),
        })
        .await
        .expect("zero timeout should read an already committed result");

    assert_eq!(result.result, expected_output);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_handle_oversized_timeout_reads_committed_result() {
    let (pool, database) = setup_ephemeral_pool("workflow_handle_oversized_timeout_ready", 8).await;
    let job_type = JobType::new("jobs.test.workflow_oversized_timeout_ready");
    register_job_definition(&pool, job_type).await;

    let metadata = json!({"test": "workflow_handle_oversized_timeout_ready"});
    let payload = json!({"work": "result"});
    let result_step = WorkflowStepEnqueueBuilder::new(StepKey::new("result"), job_type, &payload)
        .try_build()
        .expect("build result step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.oversized_timeout_ready"),
        &metadata,
    )
    .try_result_step_key("result")
    .expect("set result step")
    .step(result_step)
    .try_build()
    .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");

    let mut claimed = claim_jobs_for_types(&pool, "worker-oversized-timeout", 30, 1, &[job_type])
        .await
        .expect("claim result job");
    let job = claimed.pop().expect("result job should be claimable");
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");
    let expected_output = json!({"artifact_id": "artifact_oversized_timeout"});
    complete_job_success(
        &pool,
        job.id,
        job.run_number,
        job.attempt,
        &worker_id,
        Some(&JobCompletionUpdate {
            progress_done: Some(1),
            progress_total: Some(1),
            checkpoint: None,
            output: Some(&expected_output),
        }),
    )
    .await
    .expect("complete result job before waiting");

    let result = handle
        .get_result(WorkflowRunWaitOptions {
            timeout: Some(Duration::MAX),
            poll_interval: Duration::MAX,
        })
        .await
        .expect("oversized timeout should not panic while reading committed result");

    assert_eq!(result.result, expected_output);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_handle_reports_missing_result_declaration() {
    let (pool, database) = setup_ephemeral_pool("workflow_handle_no_result", 8).await;
    let job_type = JobType::new("jobs.test.workflow_no_result");
    register_job_definition(&pool, job_type).await;

    let metadata = json!({"test": "workflow_handle_no_result"});
    let payload = json!({"work": "no_result"});
    let step = WorkflowStepEnqueueBuilder::new(StepKey::new("only"), job_type, &payload)
        .try_build()
        .expect("build step");
    let workflow =
        WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test.no_result"), &metadata)
            .step(step)
            .try_build()
            .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");

    let mut claimed = claim_jobs_for_types(&pool, "worker-no-result", 30, 1, &[job_type])
        .await
        .expect("claim job");
    let job = claimed.pop().expect("job should be claimable");
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");
    let output = json!({"ignored": true});
    complete_job_success(
        &pool,
        job.id,
        job.run_number,
        job.attempt,
        &worker_id,
        Some(&JobCompletionUpdate {
            progress_done: None,
            progress_total: None,
            checkpoint: None,
            output: Some(&output),
        }),
    )
    .await
    .expect("complete job");

    let error = handle
        .get_result(WorkflowRunWaitOptions::default())
        .await
        .expect_err("workflow without result declaration should fail");
    assert!(matches!(error, WorkflowRunHandleError::ResultNotDeclared));
    assert_eq!(error.code(), "workflow.result_not_declared");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_handle_reports_missing_result_declaration_before_terminal() {
    let (pool, database) = setup_ephemeral_pool("workflow_handle_no_result_running", 8).await;
    let job_type = JobType::new("jobs.test.workflow_no_result_running");
    register_job_definition(&pool, job_type).await;

    let metadata = json!({"test": "workflow_handle_no_result_running"});
    let payload = json!({"work": "no_result_running"});
    let step = WorkflowStepEnqueueBuilder::new(StepKey::new("only"), job_type, &payload)
        .try_build()
        .expect("build step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.no_result_running"),
        &metadata,
    )
    .step(step)
    .try_build()
    .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");

    let error = tokio::time::timeout(
        Duration::from_millis(200),
        handle.get_result(WorkflowRunWaitOptions {
            timeout: Some(Duration::from_secs(30)),
            poll_interval: Duration::from_secs(30),
        }),
    )
    .await
    .expect("missing declaration should not wait")
    .expect_err("workflow without result declaration should fail before terminal");
    assert!(matches!(error, WorkflowRunHandleError::ResultNotDeclared));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_handle_timeout_does_not_self_starve_single_connection_pool() {
    let (pool, database) = setup_ephemeral_pool("workflow_handle_timeout_single_conn", 1).await;
    let job_type = JobType::new("jobs.test.workflow_timeout_single_conn");
    register_job_definition(&pool, job_type).await;

    let metadata = json!({"test": "workflow_handle_timeout_single_conn"});
    let payload = json!({"work": "pending_result"});
    let result_step = WorkflowStepEnqueueBuilder::new(StepKey::new("result"), job_type, &payload)
        .try_build()
        .expect("build result step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.timeout_single_conn"),
        &metadata,
    )
    .try_result_step_key("result")
    .expect("set result step")
    .step(result_step)
    .try_build()
    .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");

    let error = tokio::time::timeout(
        Duration::from_secs(2),
        handle.get_result(WorkflowRunWaitOptions {
            timeout: Some(Duration::from_millis(100)),
            poll_interval: Duration::from_secs(30),
        }),
    )
    .await
    .expect("single-connection waits should not stall on listener self-starvation")
    .expect_err("pending declared result should time out");
    assert!(matches!(error, WorkflowRunHandleError::Timeout));
    assert_eq!(error.code(), "workflow.result_wait_timeout");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_handle_oversized_poll_interval_times_out_without_panic() {
    let (pool, database) = setup_ephemeral_pool("workflow_handle_oversized_poll_interval", 1).await;
    let job_type = JobType::new("jobs.test.workflow_oversized_poll_interval");
    register_job_definition(&pool, job_type).await;

    let metadata = json!({"test": "workflow_handle_oversized_poll_interval"});
    let payload = json!({"work": "pending_result"});
    let result_step = WorkflowStepEnqueueBuilder::new(StepKey::new("result"), job_type, &payload)
        .try_build()
        .expect("build result step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.oversized_poll_interval"),
        &metadata,
    )
    .try_result_step_key("result")
    .expect("set result step")
    .step(result_step)
    .try_build()
    .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");

    let error = tokio::time::timeout(
        Duration::from_secs(2),
        handle.get_result(WorkflowRunWaitOptions {
            timeout: Some(Duration::from_millis(100)),
            poll_interval: Duration::MAX,
        }),
    )
    .await
    .expect("oversized poll interval should not panic or stall beyond timeout")
    .expect_err("pending declared result should time out");
    assert!(matches!(error, WorkflowRunHandleError::Timeout));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_handle_deadline_poll_observes_result_without_listener() {
    let (pool, database) = setup_ephemeral_pool("workflow_handle_deadline_poll", 1).await;
    let job_type = JobType::new("jobs.test.workflow_deadline_poll");
    register_job_definition(&pool, job_type).await;

    let metadata = json!({"test": "workflow_handle_deadline_poll"});
    let payload = json!({"work": "pending_result"});
    let result_step = WorkflowStepEnqueueBuilder::new(StepKey::new("result"), job_type, &payload)
        .try_build()
        .expect("build result step");
    let workflow =
        WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test.deadline_poll"), &metadata)
            .try_result_step_key("result")
            .expect("set result step")
            .step(result_step)
            .try_build()
            .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");

    let mut claimed = claim_jobs_for_types(&pool, "worker-deadline-poll", 30, 1, &[job_type])
        .await
        .expect("claim result job");
    let job = claimed.pop().expect("result job should be claimable");
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");
    let expected_output = json!({"artifact_id": "artifact_deadline_poll"});
    let output_for_task = expected_output.clone();
    let completion_pool = pool.clone();
    let completion = tokio::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        complete_job_success(
            &completion_pool,
            job.id,
            job.run_number,
            job.attempt,
            &worker_id,
            Some(&JobCompletionUpdate {
                progress_done: Some(1),
                progress_total: Some(1),
                checkpoint: None,
                output: Some(&output_for_task),
            }),
        )
        .await
        .expect("complete result job before deadline");
    });

    let result = tokio::time::timeout(
        Duration::from_secs(3),
        handle.get_result(WorkflowRunWaitOptions {
            timeout: Some(Duration::from_millis(500)),
            poll_interval: Duration::from_secs(30),
        }),
    )
    .await
    .expect("deadline poll wait should not stall")
    .expect("deadline poll should observe completed result");
    completion.await.expect("completion task should join");

    assert_eq!(result.result, expected_output);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_handle_pre_deadline_poll_timeout_runs_final_probe() {
    let (pool, database) = setup_ephemeral_pool("workflow_handle_final_probe_after_poll", 1).await;
    let job_type = JobType::new("jobs.test.workflow_final_probe_after_poll");
    register_job_definition(&pool, job_type).await;

    let metadata = json!({"test": "workflow_handle_final_probe_after_poll"});
    let payload = json!({"work": "pending_result"});
    let result_step = WorkflowStepEnqueueBuilder::new(StepKey::new("result"), job_type, &payload)
        .try_build()
        .expect("build result step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.final_probe_after_poll"),
        &metadata,
    )
    .try_result_step_key("result")
    .expect("set result step")
    .step(result_step)
    .try_build()
    .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");

    let mut claimed = claim_jobs_for_types(&pool, "worker-final-probe-poll", 30, 1, &[job_type])
        .await
        .expect("claim result job");
    let job = claimed.pop().expect("result job should be claimable");
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");
    let expected_output = json!({"artifact_id": "artifact_final_probe_after_poll"});
    let output_for_task = expected_output.clone();
    let completion_pool = pool.clone();
    let completion = tokio::spawn(async move {
        sleep(Duration::from_millis(150)).await;
        complete_job_success(
            &completion_pool,
            job.id,
            job.run_number,
            job.attempt,
            &worker_id,
            Some(&JobCompletionUpdate {
                progress_done: Some(1),
                progress_total: Some(1),
                checkpoint: None,
                output: Some(&output_for_task),
            }),
        )
        .await
        .expect("complete result job before deadline");

        let held_connection = completion_pool
            .acquire()
            .await
            .expect("hold only pool connection through deadline");
        sleep(Duration::from_millis(620)).await;
        drop(held_connection);
    });

    let result = tokio::time::timeout(
        Duration::from_secs(3),
        handle.get_result(WorkflowRunWaitOptions {
            timeout: Some(Duration::from_millis(700)),
            poll_interval: Duration::from_millis(500),
        }),
    )
    .await
    .expect("final-probe wait should not stall")
    .expect("final probe should observe completed result after poll timeout");
    completion.await.expect("completion task should join");

    assert_eq!(result.result, expected_output);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_handle_initial_lookup_timeout_runs_final_probe() {
    let (pool, database) =
        setup_ephemeral_pool("workflow_handle_final_probe_initial_lookup", 1).await;
    let side_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url)
        .await
        .expect("connect side pool");
    let job_type = JobType::new("jobs.test.workflow_final_probe_initial_lookup");
    register_job_definition(&pool, job_type).await;

    let metadata = json!({"test": "workflow_handle_final_probe_initial_lookup"});
    let payload = json!({"work": "pending_result"});
    let result_step = WorkflowStepEnqueueBuilder::new(StepKey::new("result"), job_type, &payload)
        .try_build()
        .expect("build result step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.final_probe_initial_lookup"),
        &metadata,
    )
    .try_result_step_key("result")
    .expect("set result step")
    .step(result_step)
    .try_build()
    .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");

    let mut claimed = claim_jobs_for_types(&pool, "worker-final-probe-initial", 30, 1, &[job_type])
        .await
        .expect("claim result job");
    let job = claimed.pop().expect("result job should be claimable");
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");
    let expected_output = json!({"artifact_id": "artifact_final_probe_initial_lookup"});
    let output_for_task = expected_output.clone();
    let completion_pool = side_pool.clone();
    let completion = tokio::spawn(async move {
        sleep(Duration::from_millis(100)).await;
        complete_job_success(
            &completion_pool,
            job.id,
            job.run_number,
            job.attempt,
            &worker_id,
            Some(&JobCompletionUpdate {
                progress_done: Some(1),
                progress_total: Some(1),
                checkpoint: None,
                output: Some(&output_for_task),
            }),
        )
        .await
        .expect("complete result job before deadline from side pool");
    });

    let held_connection = pool
        .acquire()
        .await
        .expect("hold only handle pool connection");
    let release = tokio::spawn(async move {
        sleep(Duration::from_millis(550)).await;
        drop(held_connection);
    });

    let result = tokio::time::timeout(
        Duration::from_secs(3),
        handle.get_result(WorkflowRunWaitOptions {
            timeout: Some(Duration::from_millis(500)),
            poll_interval: Duration::from_secs(30),
        }),
    )
    .await
    .expect("initial final-probe wait should not stall")
    .expect("final probe should observe completed result after initial lookup timeout");
    completion.await.expect("completion task should join");
    release.await.expect("release task should join");

    assert_eq!(result.result, expected_output);

    side_pool.close().await;
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_handle_timeout_covers_initial_result_lookup() {
    let (pool, database) = setup_ephemeral_pool("workflow_handle_timeout_initial_lookup", 1).await;
    let job_type = JobType::new("jobs.test.workflow_timeout_initial_lookup");
    register_job_definition(&pool, job_type).await;

    let metadata = json!({"test": "workflow_handle_timeout_initial_lookup"});
    let payload = json!({"work": "pending_result"});
    let result_step = WorkflowStepEnqueueBuilder::new(StepKey::new("result"), job_type, &payload)
        .try_build()
        .expect("build result step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.timeout_initial_lookup"),
        &metadata,
    )
    .try_result_step_key("result")
    .expect("set result step")
    .step(result_step)
    .try_build()
    .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");

    let held_connection = pool.acquire().await.expect("hold only pool connection");
    let error = tokio::time::timeout(
        Duration::from_secs(2),
        handle.get_result(WorkflowRunWaitOptions {
            timeout: Some(Duration::from_millis(100)),
            poll_interval: Duration::from_secs(30),
        }),
    )
    .await
    .expect("initial lookup should be bounded by workflow wait timeout")
    .expect_err("pending declared result should time out while pool is saturated");
    assert!(matches!(error, WorkflowRunHandleError::Timeout));
    drop(held_connection);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_handle_reports_missing_result_output() {
    let (pool, database) = setup_ephemeral_pool("workflow_handle_missing_result_output", 8).await;
    let job_type = JobType::new("jobs.test.workflow_missing_result_output");
    register_job_definition(&pool, job_type).await;

    let metadata = json!({"test": "workflow_handle_missing_result_output"});
    let payload = json!({"work": "missing_output"});
    let result_step = WorkflowStepEnqueueBuilder::new(StepKey::new("result"), job_type, &payload)
        .try_build()
        .expect("build result step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.missing_result_output"),
        &metadata,
    )
    .try_result_step_key("result")
    .expect("set result step")
    .step(result_step)
    .try_build()
    .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");

    let mut claimed = claim_jobs_for_types(&pool, "worker-missing-output", 30, 1, &[job_type])
        .await
        .expect("claim result job");
    let job = claimed.pop().expect("result job should be claimable");
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");
    complete_job_success(&pool, job.id, job.run_number, job.attempt, &worker_id, None)
        .await
        .expect("complete result job without output");

    let error = handle
        .get_result(WorkflowRunWaitOptions::default())
        .await
        .expect_err("missing result output should fail");
    assert!(matches!(error, WorkflowRunHandleError::ResultMissing));
    assert_eq!(error.code(), "workflow.result_missing");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_handle_reports_unsuccessful_terminal_run() {
    let (pool, database) = setup_ephemeral_pool("workflow_handle_unsuccessful_terminal", 8).await;
    let job_type = JobType::new("jobs.test.workflow_unsuccessful_terminal");
    register_job_definition(&pool, job_type).await;

    let metadata = json!({"test": "workflow_handle_unsuccessful_terminal"});
    let payload = json!({"work": "fail"});
    let result_step = WorkflowStepEnqueueBuilder::new(StepKey::new("result"), job_type, &payload)
        .try_build()
        .expect("build result step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.unsuccessful_terminal"),
        &metadata,
    )
    .try_result_step_key("result")
    .expect("set result step")
    .step(result_step)
    .try_build()
    .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");

    let mut claimed =
        claim_jobs_for_types(&pool, "worker-unsuccessful-terminal", 30, 1, &[job_type])
            .await
            .expect("claim result job");
    let job = claimed.pop().expect("result job should be claimable");
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");
    complete_job_failure(
        &pool,
        job.id,
        job.run_number,
        job.attempt,
        &worker_id,
        &JobFailureUpdate {
            kind: JobFailureKind::Terminal,
            code: "job.test.terminal_failure",
            message: "result step failed terminally",
            retry_delay_ms: None,
        },
    )
    .await
    .expect("fail result job terminally");

    let error = handle
        .get_result(WorkflowRunWaitOptions::default())
        .await
        .expect_err("failed run should not produce a result");
    assert!(matches!(
        error,
        WorkflowRunHandleError::UnsuccessfulTerminal {
            status: WorkflowRunStatus::CompletedWithErrors,
        }
    ));
    assert_eq!(error.code(), "workflow.result_unsuccessful_terminal");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn cancel_after_success_preserves_workflow_result() {
    let (pool, database) = setup_ephemeral_pool("workflow_result_cancel_after_success", 8).await;
    let job_type = JobType::new("jobs.test.workflow_cancel_after_success");
    register_job_definition(&pool, job_type).await;

    let metadata = json!({"test": "workflow_result_cancel_after_success"});
    let payload = json!({"work": "result"});
    let result_step = WorkflowStepEnqueueBuilder::new(StepKey::new("result"), job_type, &payload)
        .try_build()
        .expect("build result step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.cancel_after_success"),
        &metadata,
    )
    .try_result_step_key("result")
    .expect("set result step")
    .step(result_step)
    .try_build()
    .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");

    let mut claimed =
        claim_jobs_for_types(&pool, "worker-cancel-after-success", 30, 1, &[job_type])
            .await
            .expect("claim result job");
    let job = claimed.pop().expect("result job should be claimable");
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");
    let output = json!({"artifact_id": "artifact_456"});
    complete_job_success(
        &pool,
        job.id,
        job.run_number,
        job.attempt,
        &worker_id,
        Some(&JobCompletionUpdate {
            progress_done: None,
            progress_total: None,
            checkpoint: None,
            output: Some(&output),
        }),
    )
    .await
    .expect("complete result job");

    let before_cancel = handle
        .get_result(WorkflowRunWaitOptions::default())
        .await
        .expect("result should be available before cancel");
    assert_eq!(before_cancel.result, output);

    let mut tx = pool.begin().await.expect("begin cancel tx");
    let cancel_result = cancel_workflow_run_tx(
        &mut tx,
        handle.workflow_run_id,
        None,
        Some("test.cancel_after_success"),
        None,
        None,
    )
    .await
    .expect("cancel terminal workflow should return existing run");
    tx.commit().await.expect("commit cancel tx");
    assert_eq!(cancel_result.status, WorkflowRunStatus::Succeeded);

    let after_cancel = handle
        .get_result(WorkflowRunWaitOptions::default())
        .await
        .expect("result should remain available after terminal cancel");
    assert_eq!(after_cancel.result, output);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn external_result_output_is_idempotent_and_conflicts_on_change() {
    let (pool, database) = setup_ephemeral_pool("workflow_external_result_output", 8).await;

    let metadata = json!({"test": "workflow_external_result_output"});
    let payload = json!({"gate": "approval"});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("approval"), &payload)
        .try_build()
        .expect("build external gate");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.external_result"),
        &metadata,
    )
    .try_result_step_key("approval")
    .expect("set result step")
    .step(gate)
    .try_build()
    .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");

    let output = json!({"approved_by": "service"});
    let changed_output = json!({"approved_by": "other"});
    let input = CompleteExternalWorkflowStepInput {
        workflow_run_id: handle.workflow_run_id,
        organization_id: None,
        step_key: StepKey::new("approval"),
        terminal_status: WorkflowStepStatus::Succeeded,
        status_reason: Some("approved"),
        last_error_code: None,
        last_error_message: None,
        output: Some(&output),
    };
    let first = complete_external_workflow_step(&pool, &input)
        .await
        .expect("first external completion succeeds");
    assert_eq!(first.status, WorkflowStepStatus::Succeeded);
    assert_eq!(first.output.as_ref(), Some(&output));

    let steps = list_workflow_steps(&pool, None, handle.workflow_run_id)
        .await
        .expect("list workflow steps after external completion");
    let approval = steps
        .iter()
        .find(|step| step.step_key.as_str() == "approval")
        .expect("approval step should be listed");
    assert_eq!(approval.output.as_ref(), Some(&output));

    complete_external_workflow_step(&pool, &input)
        .await
        .expect("identical external completion is idempotent");

    let conflicting_metadata = CompleteExternalWorkflowStepInput {
        workflow_run_id: handle.workflow_run_id,
        organization_id: None,
        step_key: StepKey::new("approval"),
        terminal_status: WorkflowStepStatus::Succeeded,
        status_reason: Some("approved-later"),
        last_error_code: None,
        last_error_message: None,
        output: Some(&output),
    };
    assert_external_completion_metadata_conflict(
        &pool,
        &conflicting_metadata,
        "changed successful status reason should conflict",
    )
    .await;

    let conflicting = CompleteExternalWorkflowStepInput {
        workflow_run_id: handle.workflow_run_id,
        organization_id: None,
        step_key: StepKey::new("approval"),
        terminal_status: WorkflowStepStatus::Succeeded,
        status_reason: Some("approved"),
        last_error_code: None,
        last_error_message: None,
        output: Some(&changed_output),
    };
    let error = complete_external_workflow_step(&pool, &conflicting)
        .await
        .expect_err("changed external output should conflict");
    assert_eq!(
        query_error_code(&error),
        Some("workflow.external_step_conflicting_output_retry")
    );

    let result = handle
        .get_result(WorkflowRunWaitOptions::default())
        .await
        .expect("external result should be materialized");
    assert_eq!(result.result, output);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn external_result_output_idempotency_uses_jsonb_semantics() {
    let (pool, database) = setup_ephemeral_pool("workflow_external_result_jsonb_output", 8).await;

    let metadata = json!({"test": "workflow_external_result_jsonb_output"});
    let payload = json!({"gate": "approval"});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("approval"), &payload)
        .try_build()
        .expect("build external gate");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.external_jsonb"),
        &metadata,
    )
    .try_result_step_key("approval")
    .expect("set result step")
    .step(gate)
    .try_build()
    .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");

    let output = json!({"score": 1});
    let equivalent_output: Value =
        serde_json::from_str(r#"{"score":1.0}"#).expect("parse equivalent json number");
    let input = CompleteExternalWorkflowStepInput {
        workflow_run_id: handle.workflow_run_id,
        organization_id: None,
        step_key: StepKey::new("approval"),
        terminal_status: WorkflowStepStatus::Succeeded,
        status_reason: Some("approved"),
        last_error_code: None,
        last_error_message: None,
        output: Some(&output),
    };
    complete_external_workflow_step(&pool, &input)
        .await
        .expect("first external completion succeeds");

    let equivalent_retry = CompleteExternalWorkflowStepInput {
        output: Some(&equivalent_output),
        ..input
    };
    complete_external_workflow_step(&pool, &equivalent_retry)
        .await
        .expect("jsonb-equivalent external output should be idempotent");

    let result = handle
        .get_result(WorkflowRunWaitOptions::default())
        .await
        .expect("external result should be materialized");
    assert_eq!(result.result, output);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn external_failed_completion_metadata_is_idempotent_and_conflicts_on_change() {
    let (pool, database) = setup_ephemeral_pool("workflow_external_failed_metadata", 8).await;

    let metadata = json!({"test": "workflow_external_failed_metadata"});
    let payload = json!({"gate": "approval"});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("approval"), &payload)
        .try_build()
        .expect("build external gate");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.external_failed_metadata"),
        &metadata,
    )
    .step(gate)
    .try_build()
    .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");

    let input = CompleteExternalWorkflowStepInput {
        workflow_run_id: handle.workflow_run_id,
        organization_id: None,
        step_key: StepKey::new("approval"),
        terminal_status: WorkflowStepStatus::Failed,
        status_reason: Some("approval.rejected"),
        last_error_code: Some("approval.rejected"),
        last_error_message: Some("Approval was rejected."),
        output: None,
    };
    let first = complete_external_workflow_step(&pool, &input)
        .await
        .expect("first failed external completion succeeds");
    assert_eq!(first.status, WorkflowStepStatus::Failed);
    assert_eq!(first.status_reason.as_deref(), Some("approval.rejected"));
    assert_eq!(first.last_error_code.as_deref(), Some("approval.rejected"));
    assert_eq!(
        first.last_error_message.as_deref(),
        Some("Approval was rejected.")
    );
    assert_eq!(first.output, None);

    complete_external_workflow_step(&pool, &input)
        .await
        .expect("identical failed external completion is idempotent");

    let changed_reason = CompleteExternalWorkflowStepInput {
        status_reason: Some("approval.denied"),
        ..input
    };
    assert_external_completion_metadata_conflict(
        &pool,
        &changed_reason,
        "changed failed status reason should conflict",
    )
    .await;

    let changed_error_code = CompleteExternalWorkflowStepInput {
        last_error_code: Some("approval.denied"),
        ..input
    };
    assert_external_completion_metadata_conflict(
        &pool,
        &changed_error_code,
        "changed failed error code should conflict",
    )
    .await;

    let changed_error_message = CompleteExternalWorkflowStepInput {
        last_error_message: Some("Approval was denied."),
        ..input
    };
    assert_external_completion_metadata_conflict(
        &pool,
        &changed_error_message,
        "changed failed error message should conflict",
    )
    .await;

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn external_canceled_completion_metadata_is_idempotent_and_conflicts_on_change() {
    let (pool, database) = setup_ephemeral_pool("workflow_external_canceled_metadata", 8).await;

    let metadata = json!({"test": "workflow_external_canceled_metadata"});
    let payload = json!({"gate": "approval"});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("approval"), &payload)
        .try_build()
        .expect("build external gate");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.external_canceled_metadata"),
        &metadata,
    )
    .step(gate)
    .try_build()
    .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");

    let input = CompleteExternalWorkflowStepInput {
        workflow_run_id: handle.workflow_run_id,
        organization_id: None,
        step_key: StepKey::new("approval"),
        terminal_status: WorkflowStepStatus::Canceled,
        status_reason: Some("approval.canceled"),
        last_error_code: Some("approval.canceled"),
        last_error_message: Some("Approval was canceled."),
        output: None,
    };
    let first = complete_external_workflow_step(&pool, &input)
        .await
        .expect("first canceled external completion succeeds");
    assert_eq!(first.status, WorkflowStepStatus::Canceled);
    assert_eq!(first.status_reason.as_deref(), Some("approval.canceled"));
    assert_eq!(first.last_error_code.as_deref(), Some("approval.canceled"));
    assert_eq!(
        first.last_error_message.as_deref(),
        Some("Approval was canceled.")
    );
    assert_eq!(first.output, None);

    complete_external_workflow_step(&pool, &input)
        .await
        .expect("identical canceled external completion is idempotent");

    let changed_reason = CompleteExternalWorkflowStepInput {
        status_reason: Some("approval.aborted"),
        ..input
    };
    assert_external_completion_metadata_conflict(
        &pool,
        &changed_reason,
        "changed canceled status reason should conflict",
    )
    .await;

    let changed_error_code = CompleteExternalWorkflowStepInput {
        last_error_code: Some("approval.aborted"),
        ..input
    };
    assert_external_completion_metadata_conflict(
        &pool,
        &changed_error_code,
        "changed canceled error code should conflict",
    )
    .await;

    let changed_error_message = CompleteExternalWorkflowStepInput {
        last_error_message: Some("Approval was aborted."),
        ..input
    };
    assert_external_completion_metadata_conflict(
        &pool,
        &changed_error_message,
        "changed canceled error message should conflict",
    )
    .await;

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_handle_scope_controls_run_visibility() {
    let (pool, database) = setup_ephemeral_pool("workflow_handle_scope_visibility", 8).await;
    let job_type = JobType::new("jobs.test.workflow_scope_visibility");
    register_job_definition(&pool, job_type).await;

    let metadata = json!({"test": "workflow_handle_scope_visibility"});
    let payload = json!({"work": "scope"});
    let result_step = WorkflowStepEnqueueBuilder::new(StepKey::new("result"), job_type, &payload)
        .try_build()
        .expect("build result step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.scope_visibility"),
        &metadata,
    )
    .try_result_step_key("result")
    .expect("set result step")
    .step(result_step)
    .try_build()
    .expect("build workflow");
    let handle = enqueue_workflow_run_handle(&pool, &workflow)
        .await
        .expect("enqueue workflow handle");
    assert_eq!(handle.scope, WorkflowRunHandleScope::Global);

    let status = handle.get_status().await.expect("load status in scope");
    assert_eq!(status, Some(WorkflowRunStatus::Running));
    let run = handle
        .get_run()
        .await
        .expect("load run in scope")
        .expect("run should be visible in its own scope");
    assert_eq!(run.id, handle.workflow_run_id);
    assert_eq!(run.status, WorkflowRunStatus::Running);
    assert_eq!(
        run.result_step_key.as_ref().map(|key| key.as_str()),
        Some("result")
    );

    let admin = workflow_run_handle(&pool, WorkflowRunHandleScope::Admin, handle.workflow_run_id);
    assert_eq!(
        admin.get_status().await.expect("load status as admin"),
        Some(WorkflowRunStatus::Running)
    );
    assert!(
        admin
            .get_run()
            .await
            .expect("load run as admin")
            .is_some_and(|run| run.id == handle.workflow_run_id)
    );

    let mismatched_scope = WorkflowRunHandleScope::Organization(Uuid::from_u128(0x5C09E));
    let mismatched = workflow_run_handle(&pool, mismatched_scope, handle.workflow_run_id);
    assert_eq!(
        mismatched
            .get_status()
            .await
            .expect("status query with mismatched scope"),
        None
    );
    assert!(
        mismatched
            .get_run()
            .await
            .expect("run query with mismatched scope")
            .is_none()
    );

    let error = retrieve_workflow_run_handle(&pool, mismatched_scope, handle.workflow_run_id)
        .await
        .expect_err("mismatched scope should not retrieve a handle");
    assert!(matches!(error, WorkflowRunHandleError::NotFound));
    assert_eq!(error.code(), "workflow.run_not_found");

    retrieve_workflow_run_handle(
        &pool,
        WorkflowRunHandleScope::Global,
        handle.workflow_run_id,
    )
    .await
    .expect("matching scope should retrieve the handle");

    teardown_ephemeral_pool(pool, database).await;
}

fn query_error_code(error: &runledger_postgres::Error) -> Option<&'static str> {
    match error {
        runledger_postgres::Error::QueryError(query_error) => Some(query_error.code()),
        runledger_postgres::Error::ConfigError(_)
        | runledger_postgres::Error::ConnectionError(_)
        | runledger_postgres::Error::MigrationError(_) => None,
    }
}

async fn assert_external_completion_metadata_conflict(
    pool: &PgPool,
    input: &CompleteExternalWorkflowStepInput<'_>,
    message: &str,
) {
    let error = complete_external_workflow_step(pool, input)
        .await
        .expect_err(message);
    assert_eq!(
        query_error_code(&error),
        Some("workflow.external_step_conflicting_completion_retry")
    );
}
