use std::time::Duration;

use runledger_core::jobs::{
    JobFailureKind, JobStatus, JobType, StepKey, WorkflowRunEnqueueBuilder,
    WorkflowStepEnqueueBuilder, WorkflowStepStatus, WorkflowType,
};
use runledger_postgres::jobs::test_support::workflow_run_release_lock_key;
use runledger_postgres::jobs::{
    CompleteExternalWorkflowStepInput, JobFailureUpdate, WorkflowStepDbRecord,
    cancel_workflow_run_tx, claim_jobs_for_types, complete_external_workflow_step_tx,
    complete_job_failure, complete_job_success, enqueue_workflow_run, get_job_by_id,
    list_workflow_steps,
};
use serde_json::json;
use sqlx::types::Uuid;
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

use support::{query_error_code, register_job_definition};
use test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};

mod support;
#[path = "../test_support.rs"]
mod test_support;

// These tests observe blocking through SQL markers in pg_stat_activity. If a
// marker changes, update the matching wait helper rather than weakening the
// production SQL comments.

async fn await_spawned_task<T>(
    handle: &mut JoinHandle<T>,
    timeout_duration: Duration,
    timeout_message: &str,
    panic_message: &str,
) -> T {
    match timeout(timeout_duration, &mut *handle).await {
        Ok(result) => result.expect(panic_message),
        Err(_) => {
            handle.abort();
            let _ = handle.await;
            panic!("{timeout_message}");
        }
    }
}

#[tokio::test]
async fn cancel_workflow_run_rejects_non_read_committed_transaction() {
    let (pool, database) = setup_ephemeral_pool("workflow_cancel_isolation", 8).await;

    let job_type = JobType::new("jobs.test.workflow_cancel_isolation");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"test": "workflow_cancel_isolation"});
    let metadata = json!({});
    let step = WorkflowStepEnqueueBuilder::new(StepKey::new("root"), job_type, &payload)
        .try_build()
        .expect("build workflow step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.cancel_isolation"),
        &metadata,
    )
    .step(step)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow run");

    let mut tx = pool.begin().await.expect("begin repeatable read tx");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await
        .expect("set repeatable read isolation");
    let error = cancel_workflow_run_tx(&mut tx, run.id, None, Some("test.cancel"), None, None)
        .await
        .expect_err("cancel should reject non-read-committed isolation");
    assert_eq!(
        query_error_code(&error),
        Some("workflow.cancel_unsupported_isolation")
    );
    tx.rollback().await.expect("rollback repeatable read tx");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_job_completion_rejects_non_read_committed_transaction() {
    let (pool, database) = setup_ephemeral_pool("workflow_terminal_isolation", 1).await;

    let job_type = JobType::new("jobs.test.workflow_terminal_isolation");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"test": "workflow_terminal_isolation"});
    let metadata = json!({});
    let step = WorkflowStepEnqueueBuilder::new(StepKey::new("root"), job_type, &payload)
        .try_build()
        .expect("build workflow step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.terminal_isolation"),
        &metadata,
    )
    .step(step)
    .try_build()
    .expect("build workflow");
    enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow run");

    let mut claimed = claim_jobs_for_types(&pool, "worker-terminal-isolation", 30, 1, &[job_type])
        .await
        .expect("claim workflow job");
    let job = claimed.pop().expect("workflow job should be claimable");
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");

    sqlx::query("SET default_transaction_isolation = 'repeatable read'")
        .execute(&pool)
        .await
        .expect("set default isolation to repeatable read");
    let error = complete_job_success(&pool, job.id, job.run_number, job.attempt, &worker_id, None)
        .await
        .expect_err("workflow terminal completion should reject non-read-committed isolation");
    assert_eq!(
        query_error_code(&error),
        Some("workflow.terminal_completion_unsupported_isolation")
    );
    sqlx::query("SET default_transaction_isolation = 'read committed'")
        .execute(&pool)
        .await
        .expect("reset default isolation to read committed");

    let persisted = get_job_by_id(&pool, None, job.id)
        .await
        .expect("load workflow job")
        .expect("workflow job exists");
    assert_eq!(persisted.status, JobStatus::Leased);
    assert_eq!(persisted.worker_id.as_deref(), Some(worker_id.as_str()));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn external_workflow_completion_rejects_non_read_committed_transaction() {
    let (pool, database) = setup_ephemeral_pool("workflow_external_completion_isolation", 8).await;

    let payload = json!({"test": "workflow_external_completion_isolation"});
    let metadata = json!({});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build external gate step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.external_completion_isolation"),
        &metadata,
    )
    .step(gate)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow run");

    let mut tx = pool.begin().await.expect("begin repeatable read tx");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await
        .expect("set repeatable read isolation");
    let error = complete_external_workflow_step_tx(
        &mut tx,
        &CompleteExternalWorkflowStepInput {
            workflow_run_id: run.id,
            organization_id: None,
            step_key: StepKey::new("gate"),
            terminal_status: WorkflowStepStatus::Succeeded,
            status_reason: None,
            last_error_code: None,
            last_error_message: None,
            output: None,
        },
    )
    .await
    .expect_err("external completion should reject non-read-committed isolation");
    assert_eq!(
        query_error_code(&error),
        Some("workflow.external_completion_unsupported_isolation")
    );
    tx.rollback().await.expect("rollback repeatable read tx");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn cancel_workflow_run_locks_job_rows_before_workflow_steps() {
    let (pool, database) = setup_ephemeral_pool("workflow_cancel_lock_order", 8).await;

    let job_type = JobType::new("jobs.test.workflow_cancel_lock_order");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"test": "workflow_cancel_lock_order"});
    let metadata = json!({});
    let step = WorkflowStepEnqueueBuilder::new(StepKey::new("root"), job_type, &payload)
        .try_build()
        .expect("build workflow step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.cancel_lock_order"),
        &metadata,
    )
    .step(step)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow run");
    let step = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list workflow steps")
        .into_iter()
        .next()
        .expect("workflow step exists");
    let job_id = step.job_id.expect("root job step should be enqueued");

    let mut held_job_tx = pool.begin().await.expect("begin held job tx");
    sqlx::query!("SELECT id FROM job_queue WHERE id = $1 FOR UPDATE", job_id)
        .fetch_one(&mut *held_job_tx)
        .await
        .expect("lock job row");

    let cancel_pool = pool.clone();
    let mut cancel_task = tokio::spawn(async move {
        let mut tx = cancel_pool.begin().await.expect("begin cancel tx");
        let result =
            cancel_workflow_run_tx(&mut tx, run.id, None, Some("test.cancel"), None, None).await;
        if result.is_ok() {
            tx.commit().await.expect("commit cancel tx");
        } else {
            tx.rollback().await.expect("rollback cancel tx");
        }
        result.map(|_| ())
    });

    wait_for_cancel_to_block_on_job_lock(&pool).await;

    let mut probe_tx = pool.begin().await.expect("begin probe tx");
    sqlx::query!("SELECT set_config('lock_timeout', '100ms', true) AS \"lock_timeout!\"")
        .fetch_one(&mut *probe_tx)
        .await
        .expect("set probe lock timeout");
    sqlx::query!(
        "SELECT id FROM workflow_steps WHERE id = $1 FOR UPDATE",
        step.id
    )
    .fetch_one(&mut *probe_tx)
    .await
    .expect("cancel must not hold workflow step locks while waiting on job rows");
    probe_tx.rollback().await.expect("rollback probe tx");

    held_job_tx.rollback().await.expect("release held job lock");
    await_spawned_task(
        &mut cancel_task,
        Duration::from_secs(5),
        "cancel task should finish after job lock release",
        "cancel task should not panic",
    )
    .await
    .expect("cancel workflow run should succeed");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn cancel_workflow_run_relocks_jobs_that_appear_while_waiting_for_release_lock() {
    let (pool, database) = setup_ephemeral_pool("workflow_cancel_late_job_lock", 8).await;

    let job_type = JobType::new("jobs.test.workflow_cancel_late_job_lock");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"test": "workflow_cancel_late_job_lock"});
    let metadata = json!({});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build external gate step");
    let dependent = WorkflowStepEnqueueBuilder::new(StepKey::new("dependent"), job_type, &payload)
        .depends_on_terminal(&[StepKey::new("gate")])
        .try_build()
        .expect("build dependent job step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.cancel_late_job_lock"),
        &metadata,
    )
    .step(gate)
    .step(dependent)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow run");
    let steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list workflow steps");
    let gate_step = steps
        .iter()
        .find(|step| step.step_key.as_str() == "gate")
        .expect("gate step exists")
        .clone();
    let dependent_step = steps
        .into_iter()
        .find(|step| step.step_key.as_str() == "dependent")
        .expect("dependent step exists");
    assert_eq!(gate_step.status, WorkflowStepStatus::WaitingForExternal);
    assert_eq!(dependent_step.status, WorkflowStepStatus::Blocked);
    assert!(dependent_step.job_id.is_none());

    let mut advisory_tx = pool.begin().await.expect("begin advisory tx");
    sqlx::query!(
        "SELECT pg_advisory_xact_lock($1)",
        workflow_run_release_lock_key(run.id)
    )
    .execute(&mut *advisory_tx)
    .await
    .expect("hold workflow release advisory lock");

    let cancel_pool = pool.clone();
    let mut cancel_task = tokio::spawn(async move {
        let mut tx = cancel_pool.begin().await.expect("begin cancel tx");
        let result =
            cancel_workflow_run_tx(&mut tx, run.id, None, Some("test.cancel"), None, None).await;
        if result.is_ok() {
            tx.commit().await.expect("commit cancel tx");
        } else {
            tx.rollback().await.expect("rollback cancel tx");
        }
        result.map(|_| ())
    });

    wait_for_cancel_to_block_on_release_lock(&pool).await;

    let late_job_id = release_blocked_job_step_for_test(&pool, &gate_step, &dependent_step).await;
    let mut held_job_tx = pool.begin().await.expect("begin held job tx");
    sqlx::query!(
        "SELECT id FROM job_queue WHERE id = $1 FOR UPDATE",
        late_job_id
    )
    .fetch_one(&mut *held_job_tx)
    .await
    .expect("lock late job row");

    advisory_tx
        .rollback()
        .await
        .expect("release workflow release advisory lock");
    wait_for_cancel_to_block_on_job_lock(&pool).await;

    let mut probe_tx = pool.begin().await.expect("begin probe tx");
    sqlx::query!("SELECT set_config('lock_timeout', '100ms', true) AS \"lock_timeout!\"")
        .fetch_one(&mut *probe_tx)
        .await
        .expect("set probe lock timeout");
    sqlx::query!(
        "SELECT id FROM workflow_steps WHERE id = $1 FOR UPDATE",
        dependent_step.id
    )
    .fetch_one(&mut *probe_tx)
    .await
    .expect("cancel must relock the late job before holding workflow step locks");
    probe_tx.rollback().await.expect("rollback probe tx");

    held_job_tx.rollback().await.expect("release held job lock");
    await_spawned_task(
        &mut cancel_task,
        Duration::from_secs(5),
        "cancel task should finish after job lock release",
        "cancel task should not panic",
    )
    .await
    .expect("cancel workflow run should succeed");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_step_release_proceeds_while_another_release_holds_shared_lock() {
    let (pool, database) = setup_ephemeral_pool("workflow_release_shared_lock", 8).await;

    let job_type = JobType::new("jobs.test.workflow_release_shared_lock");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"test": "workflow_release_shared_lock"});
    let metadata = json!({});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build external gate step");
    let dependent = WorkflowStepEnqueueBuilder::new(StepKey::new("dependent"), job_type, &payload)
        .depends_on_success(&[StepKey::new("gate")])
        .try_build()
        .expect("build dependent job step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.release_shared_lock"),
        &metadata,
    )
    .step(gate)
    .step(dependent)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow run");

    let mut shared_release_tx = pool.begin().await.expect("begin shared release tx");
    sqlx::query!(
        "SELECT pg_advisory_xact_lock_shared($1)",
        workflow_run_release_lock_key(run.id)
    )
    .execute(&mut *shared_release_tx)
    .await
    .expect("hold shared workflow release advisory lock");

    let mut tx = pool.begin().await.expect("begin external completion tx");
    complete_external_workflow_step_tx(
        &mut tx,
        &CompleteExternalWorkflowStepInput {
            workflow_run_id: run.id,
            organization_id: None,
            step_key: StepKey::new("gate"),
            terminal_status: WorkflowStepStatus::Succeeded,
            status_reason: None,
            last_error_code: None,
            last_error_message: None,
            output: None,
        },
    )
    .await
    .expect("complete external gate");
    tx.commit().await.expect("commit external completion tx");
    shared_release_tx
        .rollback()
        .await
        .expect("release shared workflow release advisory lock");

    let steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list workflow steps");
    let dependent_step = steps
        .into_iter()
        .find(|step| step.step_key.as_str() == "dependent")
        .expect("dependent step exists");
    assert_eq!(dependent_step.status, WorkflowStepStatus::Enqueued);
    assert!(dependent_step.job_id.is_some());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn job_completion_waits_for_release_lock_and_releases_after_rollback() {
    let (pool, database) = setup_ephemeral_pool("workflow_terminal_release_conflict", 8).await;

    let job_type = JobType::new("jobs.test.workflow_terminal_release_conflict");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"test": "workflow_terminal_release_conflict"});
    let metadata = json!({});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build external gate step");
    let parent = WorkflowStepEnqueueBuilder::new(StepKey::new("parent"), job_type, &payload)
        .depends_on_success(&[StepKey::new("gate")])
        .try_build()
        .expect("build parent job step");
    let child = WorkflowStepEnqueueBuilder::new(StepKey::new("child"), job_type, &payload)
        .depends_on_success(&[StepKey::new("parent")])
        .try_build()
        .expect("build child job step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.terminal_release_conflict"),
        &metadata,
    )
    .step(gate)
    .step(parent)
    .step(child)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow run");

    let mut external_tx = pool.begin().await.expect("begin external completion tx");
    complete_external_workflow_step_tx(
        &mut external_tx,
        &CompleteExternalWorkflowStepInput {
            workflow_run_id: run.id,
            organization_id: None,
            step_key: StepKey::new("gate"),
            terminal_status: WorkflowStepStatus::Succeeded,
            status_reason: None,
            last_error_code: None,
            last_error_message: None,
            output: None,
        },
    )
    .await
    .expect("complete external gate");
    external_tx.commit().await.expect("commit external gate");

    let mut claimed = claim_jobs_for_types(&pool, "worker-terminal-conflict", 30, 1, &[job_type])
        .await
        .expect("claim parent job");
    let parent_job = claimed.pop().expect("parent job should be claimable");

    let mut cancel_release_lock_tx = pool.begin().await.expect("begin cancel lock tx");
    sqlx::query!(
        "SELECT pg_advisory_xact_lock($1)",
        workflow_run_release_lock_key(run.id)
    )
    .execute(&mut *cancel_release_lock_tx)
    .await
    .expect("hold exclusive workflow release lock");

    let completion_pool = pool.clone();
    let parent_worker_id = parent_job
        .worker_id
        .clone()
        .expect("claimed job has worker id");
    let mut completion_task = tokio::spawn(async move {
        complete_job_success(
            &completion_pool,
            parent_job.id,
            parent_job.run_number,
            parent_job.attempt,
            &parent_worker_id,
            None,
        )
        .await
    });

    wait_for_completion_to_block_on_shared_release_lock(&pool).await;

    cancel_release_lock_tx
        .rollback()
        .await
        .expect("release exclusive workflow release lock");
    await_spawned_task(
        &mut completion_task,
        Duration::from_secs(5),
        "completion should finish after release lock rollback",
        "completion task should not panic",
    )
    .await
    .expect("job completion should succeed after release lock rollback");

    let completed_parent = get_job_by_id(&pool, None, parent_job.id)
        .await
        .expect("load parent job")
        .expect("parent job exists");
    assert_eq!(completed_parent.status, JobStatus::Succeeded);

    let steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list workflow steps");
    let parent_step = steps
        .iter()
        .find(|step| step.step_key.as_str() == "parent")
        .expect("parent step exists");
    let child_step = steps
        .iter()
        .find(|step| step.step_key.as_str() == "child")
        .expect("child step exists");
    assert_eq!(parent_step.status, WorkflowStepStatus::Succeeded);
    assert_eq!(child_step.status, WorkflowStepStatus::Enqueued);
    assert!(child_step.job_id.is_some());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn job_completion_release_lock_timeout_returns_release_conflict() {
    let (pool, database) = setup_ephemeral_pool("workflow_terminal_release_lock_timeout", 2).await;

    let job_type = JobType::new("jobs.test.workflow_terminal_release_lock_timeout");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"test": "workflow_terminal_release_lock_timeout"});
    let metadata = json!({});
    let root = WorkflowStepEnqueueBuilder::new(StepKey::new("root"), job_type, &payload)
        .try_build()
        .expect("build root job step");
    let dependent = WorkflowStepEnqueueBuilder::new(StepKey::new("dependent"), job_type, &payload)
        .depends_on_success(&[StepKey::new("root")])
        .try_build()
        .expect("build dependent job step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.terminal_release_lock_timeout"),
        &metadata,
    )
    .step(root)
    .step(dependent)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow run");

    let mut claimed = claim_jobs_for_types(&pool, "worker-timeout", 30, 1, &[job_type])
        .await
        .expect("claim root job");
    let root_job = claimed.pop().expect("root job should be claimable");
    let worker_id = root_job
        .worker_id
        .clone()
        .expect("claimed job has worker id");

    let mut cancel_lock_tx = pool.begin().await.expect("begin cancel lock tx");
    sqlx::query!(
        "SELECT pg_advisory_xact_lock($1)",
        workflow_run_release_lock_key(run.id)
    )
    .execute(&mut *cancel_lock_tx)
    .await
    .expect("hold exclusive workflow release lock");

    let error = timeout(
        Duration::from_secs(15),
        complete_job_success(
            &pool,
            root_job.id,
            root_job.run_number,
            root_job.attempt,
            &worker_id,
            None,
        ),
    )
    .await
    .expect("completion should return after release lock timeout")
    .expect_err("release lock timeout should return a release conflict");
    assert_eq!(query_error_code(&error), Some("workflow.release_conflict"));
    let runledger_postgres::Error::QueryError(query_error) = &error else {
        panic!("expected query error");
    };
    assert_eq!(query_error.sqlstate(), Some("55P03"));
    assert!(query_error.source_arc().is_some());
    assert!(
        query_error
            .internal_message()
            .contains("timed out acquiring shared release advisory lock"),
        "{}",
        query_error.internal_message()
    );

    let lock_timeout: String = sqlx::query_scalar("SELECT current_setting('lock_timeout')")
        .fetch_one(&pool)
        .await
        .expect("read returned completion connection lock_timeout");
    assert_eq!(lock_timeout, "0");

    cancel_lock_tx
        .rollback()
        .await
        .expect("release exclusive workflow release lock");
    let persisted = get_job_by_id(&pool, None, root_job.id)
        .await
        .expect("load root job")
        .expect("root job exists");
    assert_eq!(persisted.status, JobStatus::Leased);
    assert_eq!(persisted.worker_id.as_deref(), Some(worker_id.as_str()));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn external_completion_release_conflict_can_be_committed_without_partial_mutation() {
    let (pool, database) =
        setup_ephemeral_pool("workflow_external_release_conflict_commit", 8).await;

    let job_type = JobType::new("jobs.test.workflow_external_release_conflict_commit");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"test": "workflow_external_release_conflict_commit"});
    let metadata = json!({});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build external gate step");
    let dependent = WorkflowStepEnqueueBuilder::new(StepKey::new("dependent"), job_type, &payload)
        .depends_on_success(&[StepKey::new("gate")])
        .try_build()
        .expect("build dependent job step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.external_release_conflict_commit"),
        &metadata,
    )
    .step(gate)
    .step(dependent)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow run");

    let mut release_lock_tx = pool.begin().await.expect("begin release lock tx");
    sqlx::query!(
        "SELECT pg_advisory_xact_lock($1)",
        workflow_run_release_lock_key(run.id)
    )
    .execute(&mut *release_lock_tx)
    .await
    .expect("hold exclusive workflow release lock");

    let mut external_tx = pool.begin().await.expect("begin external tx");
    let error = complete_external_workflow_step_tx(
        &mut external_tx,
        &CompleteExternalWorkflowStepInput {
            workflow_run_id: run.id,
            organization_id: None,
            step_key: StepKey::new("gate"),
            terminal_status: WorkflowStepStatus::Succeeded,
            status_reason: None,
            last_error_code: None,
            last_error_message: None,
            output: None,
        },
    )
    .await
    .expect_err("exclusive release lock should make external completion conflict");
    assert_eq!(query_error_code(&error), Some("workflow.release_conflict"));
    external_tx
        .commit()
        .await
        .expect("commit after conflict should not persist partial mutation");
    release_lock_tx
        .rollback()
        .await
        .expect("release exclusive workflow release lock");

    let steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list workflow steps");
    let gate_step = steps
        .iter()
        .find(|step| step.step_key.as_str() == "gate")
        .expect("gate step exists");
    let dependent_step = steps
        .iter()
        .find(|step| step.step_key.as_str() == "dependent")
        .expect("dependent step exists");
    assert_eq!(gate_step.status, WorkflowStepStatus::WaitingForExternal);
    assert_eq!(dependent_step.status, WorkflowStepStatus::Blocked);
    assert!(dependent_step.job_id.is_none());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn job_completion_after_cancel_commit_does_not_reenqueue_dependents() {
    let (pool, database) = setup_ephemeral_pool("workflow_terminal_release_cancel_commit", 8).await;

    let job_type = JobType::new("jobs.test.workflow_terminal_release_cancel_commit");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"test": "workflow_terminal_release_cancel_commit"});
    let metadata = json!({});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build external gate step");
    let parent = WorkflowStepEnqueueBuilder::new(StepKey::new("parent"), job_type, &payload)
        .depends_on_success(&[StepKey::new("gate")])
        .try_build()
        .expect("build parent job step");
    let child = WorkflowStepEnqueueBuilder::new(StepKey::new("child"), job_type, &payload)
        .depends_on_success(&[StepKey::new("parent")])
        .try_build()
        .expect("build child job step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.terminal_release_cancel_commit"),
        &metadata,
    )
    .step(gate)
    .step(parent)
    .step(child)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow run");

    let mut external_tx = pool.begin().await.expect("begin external completion tx");
    complete_external_workflow_step_tx(
        &mut external_tx,
        &CompleteExternalWorkflowStepInput {
            workflow_run_id: run.id,
            organization_id: None,
            step_key: StepKey::new("gate"),
            terminal_status: WorkflowStepStatus::Succeeded,
            status_reason: None,
            last_error_code: None,
            last_error_message: None,
            output: None,
        },
    )
    .await
    .expect("complete external gate");
    external_tx.commit().await.expect("commit external gate");

    let mut claimed = claim_jobs_for_types(&pool, "worker-cancel-commit", 30, 1, &[job_type])
        .await
        .expect("claim parent job");
    let parent_job = claimed.pop().expect("parent job should be claimable");

    let mut cancel_release_lock_tx = pool.begin().await.expect("begin cancel lock tx");
    sqlx::query!(
        "SELECT pg_advisory_xact_lock($1)",
        workflow_run_release_lock_key(run.id)
    )
    .execute(&mut *cancel_release_lock_tx)
    .await
    .expect("hold exclusive workflow release lock");

    let completion_pool = pool.clone();
    let parent_worker_id = parent_job
        .worker_id
        .clone()
        .expect("claimed job has worker id");
    let mut completion_task = tokio::spawn(async move {
        complete_job_success(
            &completion_pool,
            parent_job.id,
            parent_job.run_number,
            parent_job.attempt,
            &parent_worker_id,
            None,
        )
        .await
    });

    wait_for_completion_to_block_on_shared_release_lock(&pool).await;

    sqlx::query(
        "UPDATE workflow_runs
         SET status = 'CANCELED',
             finished_at = COALESCE(finished_at, now()),
             updated_at = now()
         WHERE id = $1",
    )
    .bind(run.id)
    .execute(&mut *cancel_release_lock_tx)
    .await
    .expect("mark workflow canceled while completion waits");
    sqlx::query(
        "UPDATE workflow_steps
         SET status = 'CANCELED',
             finished_at = COALESCE(finished_at, now()),
             updated_at = now()
         WHERE workflow_run_id = $1
           AND step_key = 'child'
           AND status = 'BLOCKED'",
    )
    .bind(run.id)
    .execute(&mut *cancel_release_lock_tx)
    .await
    .expect("mark dependent canceled while completion waits");
    cancel_release_lock_tx
        .commit()
        .await
        .expect("commit simulated cancel lock transaction");

    await_spawned_task(
        &mut completion_task,
        Duration::from_secs(5),
        "completion should finish after cancel commit",
        "completion task should not panic",
    )
    .await
    .expect("job completion should succeed after cancel commit");

    let run_status: String = sqlx::query_scalar(
        "SELECT status::text
         FROM workflow_runs
         WHERE id = $1",
    )
    .bind(run.id)
    .fetch_one(&pool)
    .await
    .expect("load workflow run status");
    assert_eq!(run_status, "CANCELED");

    let completed_parent = get_job_by_id(&pool, None, parent_job.id)
        .await
        .expect("load parent job")
        .expect("parent job exists");
    assert_eq!(completed_parent.status, JobStatus::Succeeded);

    let steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list workflow steps");
    let parent_step = steps
        .iter()
        .find(|step| step.step_key.as_str() == "parent")
        .expect("parent step exists");
    let child_step = steps
        .iter()
        .find(|step| step.step_key.as_str() == "child")
        .expect("child step exists");
    assert_eq!(parent_step.status, WorkflowStepStatus::Succeeded);
    assert_eq!(child_step.status, WorkflowStepStatus::Canceled);
    assert!(child_step.job_id.is_none());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn external_completion_waits_for_ordered_step_locks_before_locking_gate() {
    let (pool, database) = setup_ephemeral_pool("workflow_external_lock_order", 8).await;

    let job_type = JobType::new("jobs.test.workflow_external_lock_order");
    register_job_definition(&pool, job_type).await;

    let run_id = Uuid::now_v7();
    let dependent_id =
        Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("valid dependent uuid");
    let gate_id = Uuid::parse_str("00000000-0000-0000-0000-000000000002").expect("valid gate uuid");

    sqlx::query(
        "INSERT INTO workflow_runs (
            id,
            workflow_type,
            status,
            metadata,
            started_at
         )
         VALUES ($1, 'workflow.test.external_lock_order', 'WAITING_FOR_EXTERNAL', '{}'::jsonb, now())",
    )
    .bind(run_id)
    .execute(&pool)
    .await
    .expect("insert workflow run");

    sqlx::query(
        "INSERT INTO workflow_steps (
            id,
            workflow_run_id,
            step_key,
            execution_kind,
            job_type,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            stage,
            status,
            dependency_count_total,
            dependency_count_pending,
            dependency_count_unsatisfied
         )
         VALUES
            ($1, $3, 'dependent', 'JOB', $4, '{}'::jsonb, 100, 3, 60, 'queued', 'BLOCKED', 1, 1, 0),
            ($2, $3, 'gate', 'EXTERNAL', NULL, '{}'::jsonb, NULL, NULL, NULL, NULL, 'WAITING_FOR_EXTERNAL', 0, 0, 0)",
    )
    .bind(dependent_id)
    .bind(gate_id)
    .bind(run_id)
    .bind(job_type.as_str())
    .execute(&pool)
    .await
    .expect("insert workflow steps");

    sqlx::query(
        "INSERT INTO workflow_step_dependencies (
            workflow_run_id,
            prerequisite_step_id,
            dependent_step_id,
            release_mode
         )
         VALUES ($1, $2, $3, 'ON_SUCCESS')",
    )
    .bind(run_id)
    .bind(gate_id)
    .bind(dependent_id)
    .execute(&pool)
    .await
    .expect("insert workflow dependency");

    let mut held_dependent_tx = pool.begin().await.expect("begin held dependent tx");
    sqlx::query!(
        "SELECT id FROM workflow_steps WHERE id = $1 FOR UPDATE",
        dependent_id
    )
    .fetch_one(&mut *held_dependent_tx)
    .await
    .expect("lock dependent step");

    let completion_pool = pool.clone();
    let mut completion_task = tokio::spawn(async move {
        let mut tx = completion_pool.begin().await.expect("begin completion tx");
        let result = complete_external_workflow_step_tx(
            &mut tx,
            &CompleteExternalWorkflowStepInput {
                workflow_run_id: run_id,
                organization_id: None,
                step_key: StepKey::new("gate"),
                terminal_status: WorkflowStepStatus::Succeeded,
                status_reason: None,
                last_error_code: None,
                last_error_message: None,
                output: None,
            },
        )
        .await;
        if result.is_ok() {
            tx.commit().await.expect("commit completion tx");
        } else {
            tx.rollback().await.expect("rollback completion tx");
        }
        result.map(|_| ())
    });

    wait_for_workflow_step_lock_wait(&pool).await;

    let mut probe_tx = pool.begin().await.expect("begin probe tx");
    sqlx::query!("SELECT set_config('lock_timeout', '100ms', true) AS \"lock_timeout!\"")
        .fetch_one(&mut *probe_tx)
        .await
        .expect("set probe lock timeout");
    sqlx::query!(
        "SELECT id FROM workflow_steps WHERE id = $1 FOR UPDATE",
        gate_id
    )
    .fetch_one(&mut *probe_tx)
    .await
    .expect("external completion must not lock the gate before earlier step rows");
    probe_tx.rollback().await.expect("rollback probe tx");

    held_dependent_tx
        .rollback()
        .await
        .expect("release dependent lock");
    await_spawned_task(
        &mut completion_task,
        Duration::from_secs(5),
        "completion should finish after dependent lock release",
        "completion task should not panic",
    )
    .await
    .expect("external completion should succeed");

    let steps = list_workflow_steps(&pool, None, run_id)
        .await
        .expect("list workflow steps");
    let dependent_step = steps
        .into_iter()
        .find(|step| step.step_key.as_str() == "dependent")
        .expect("dependent step exists");
    assert_eq!(dependent_step.status, WorkflowStepStatus::Enqueued);
    assert!(dependent_step.job_id.is_some());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn external_completion_conflicts_when_cancel_waits_on_step_rows() {
    let (pool, database) = setup_ephemeral_pool("workflow_external_cancel_step_rows", 8).await;

    let job_type = JobType::new("jobs.test.workflow_external_cancel_step_rows");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"test": "workflow_external_cancel_step_rows"});
    let metadata = json!({});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build external gate step");
    let dependent = WorkflowStepEnqueueBuilder::new(StepKey::new("dependent"), job_type, &payload)
        .depends_on_success(&[StepKey::new("gate")])
        .try_build()
        .expect("build dependent job step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.external_cancel_step_rows"),
        &metadata,
    )
    .step(gate)
    .step(dependent)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow run");
    let run_id = run.id;

    let mut external_tx = pool.begin().await.expect("begin external tx");
    sqlx::query(
        "SELECT id
         FROM workflow_steps
         WHERE workflow_run_id = $1
         ORDER BY id ASC
         FOR UPDATE",
    )
    .bind(run_id)
    .fetch_all(&mut *external_tx)
    .await
    .expect("hold workflow step rows for external completion");

    let cancel_pool = pool.clone();
    let mut cancel_task = tokio::spawn(async move {
        let mut tx = cancel_pool.begin().await.expect("begin cancel tx");
        let result =
            cancel_workflow_run_tx(&mut tx, run_id, None, Some("test.cancel"), None, None).await;
        if result.is_ok() {
            tx.commit().await.expect("commit cancel tx");
        } else {
            tx.rollback().await.expect("rollback cancel tx");
        }
        result.map(|_| ())
    });

    wait_for_cancel_to_block_on_workflow_step_lock(&pool).await;

    let error = complete_external_workflow_step_tx(
        &mut external_tx,
        &CompleteExternalWorkflowStepInput {
            workflow_run_id: run_id,
            organization_id: None,
            step_key: StepKey::new("gate"),
            terminal_status: WorkflowStepStatus::Succeeded,
            status_reason: None,
            last_error_code: None,
            last_error_message: None,
            output: None,
        },
    )
    .await
    .expect_err("cancel-owned release lock should make external completion conflict");
    assert_eq!(query_error_code(&error), Some("workflow.release_conflict"));
    external_tx
        .rollback()
        .await
        .expect("rollback external completion");

    await_spawned_task(
        &mut cancel_task,
        Duration::from_secs(5),
        "cancel should finish after external rollback",
        "cancel task should not panic",
    )
    .await
    .expect("cancel workflow run should succeed");

    let steps = list_workflow_steps(&pool, None, run_id)
        .await
        .expect("list workflow steps");
    let gate_step = steps
        .iter()
        .find(|step| step.step_key.as_str() == "gate")
        .expect("gate step exists");
    let dependent_step = steps
        .iter()
        .find(|step| step.step_key.as_str() == "dependent")
        .expect("dependent step exists");
    assert_eq!(gate_step.status, WorkflowStepStatus::Canceled);
    assert_eq!(dependent_step.status, WorkflowStepStatus::Canceled);
    assert!(dependent_step.job_id.is_none());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn queue_success_waits_for_release_lock_and_releases_dependents() {
    let (pool, database) = setup_ephemeral_pool("workflow_release_cancel_rollback", 8).await;

    let job_type = JobType::new("jobs.test.workflow_release_cancel_rollback");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"test": "workflow_release_cancel_rollback"});
    let metadata = json!({});
    let root = WorkflowStepEnqueueBuilder::new(StepKey::new("root"), job_type, &payload)
        .try_build()
        .expect("build root job step");
    let dependent = WorkflowStepEnqueueBuilder::new(StepKey::new("dependent"), job_type, &payload)
        .depends_on_success(&[StepKey::new("root")])
        .try_build()
        .expect("build dependent job step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.release_cancel_rollback"),
        &metadata,
    )
    .step(root)
    .step(dependent)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow run");

    let mut claimed = claim_jobs_for_types(&pool, "worker-root", 30, 1, &[job_type])
        .await
        .expect("claim root job");
    let root_job = claimed.pop().expect("root job should be claimable");

    let mut cancel_lock_tx = pool.begin().await.expect("begin cancel lock tx");
    sqlx::query!(
        "SELECT pg_advisory_xact_lock($1)",
        workflow_run_release_lock_key(run.id)
    )
    .execute(&mut *cancel_lock_tx)
    .await
    .expect("hold exclusive workflow release advisory lock");

    let completion_pool = pool.clone();
    let root_worker_id = root_job
        .worker_id
        .clone()
        .expect("claimed job should have worker id");
    let mut completion_task = tokio::spawn(async move {
        complete_job_success(
            &completion_pool,
            root_job.id,
            root_job.run_number,
            root_job.attempt,
            &root_worker_id,
            None,
        )
        .await
    });

    wait_for_completion_to_block_on_shared_release_lock(&pool).await;

    cancel_lock_tx
        .rollback()
        .await
        .expect("rollback cancel lock transaction");
    await_spawned_task(
        &mut completion_task,
        Duration::from_secs(5),
        "completion should finish after release lock rollback",
        "completion task should not panic",
    )
    .await
    .expect("completion should commit terminal state after release lock rollback");

    let persisted_root = get_job_by_id(&pool, None, root_job.id)
        .await
        .expect("load root job")
        .expect("root job exists");
    assert_eq!(persisted_root.status, JobStatus::Succeeded);

    let released_steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list workflow steps after release");
    let root_after_completion = released_steps
        .iter()
        .find(|step| step.step_key.as_str() == "root")
        .expect("root step exists after completion");
    assert_eq!(root_after_completion.status, WorkflowStepStatus::Succeeded);
    let dependent_after_completion = released_steps
        .iter()
        .find(|step| step.step_key.as_str() == "dependent")
        .expect("dependent step exists after release");
    assert_eq!(
        dependent_after_completion.status,
        WorkflowStepStatus::Enqueued
    );
    assert!(dependent_after_completion.job_id.is_some());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn retryable_workflow_job_failure_returns_step_to_enqueued() {
    let (pool, database) = setup_ephemeral_pool("workflow_retryable_step_enqueued", 8).await;

    let job_type = JobType::new("jobs.test.workflow_retryable_step_enqueued");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"test": "workflow_retryable_step_enqueued"});
    let metadata = json!({});
    let step = WorkflowStepEnqueueBuilder::new(StepKey::new("root"), job_type, &payload)
        .try_build()
        .expect("build workflow job step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.retryable_step_enqueued"),
        &metadata,
    )
    .step(step)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow run");

    let mut claimed = claim_jobs_for_types(&pool, "worker-retryable", 30, 1, &[job_type])
        .await
        .expect("claim workflow job");
    let job = claimed.pop().expect("workflow job should be claimable");

    let running_step = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list workflow steps")
        .into_iter()
        .next()
        .expect("workflow step exists");
    assert_eq!(running_step.status, WorkflowStepStatus::Running);

    complete_job_failure(
        &pool,
        job.id,
        job.run_number,
        job.attempt,
        job.worker_id.as_deref().expect("claimed job has worker id"),
        &JobFailureUpdate {
            kind: JobFailureKind::Retryable,
            code: "test.retryable",
            message: "retryable failure",
            retry_delay_ms: Some(1),
        },
    )
    .await
    .expect("complete job with retryable failure");

    let persisted_job = get_job_by_id(&pool, None, job.id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted_job.status, JobStatus::Pending);

    let retried_step = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list workflow steps")
        .into_iter()
        .next()
        .expect("workflow step exists");
    assert_eq!(retried_step.status, WorkflowStepStatus::Enqueued);
    assert_eq!(retried_step.job_id, Some(job.id));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn cancel_workflow_run_does_not_release_blocked_on_terminal_dependents() {
    let (pool, database) = setup_ephemeral_pool("workflow_cancel_no_release", 8).await;

    let job_type = JobType::new("jobs.test.workflow_cancel_no_release");
    register_job_definition(&pool, job_type).await;

    let payload = json!({"test": "workflow_cancel_no_release"});
    let metadata = json!({});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build external gate step");
    let dependent = WorkflowStepEnqueueBuilder::new(StepKey::new("dependent"), job_type, &payload)
        .depends_on_terminal(&[StepKey::new("gate")])
        .try_build()
        .expect("build dependent job step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.cancel_no_release"),
        &metadata,
    )
    .step(gate)
    .step(dependent)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow run");

    let mut tx = pool.begin().await.expect("begin cancel tx");
    cancel_workflow_run_tx(&mut tx, run.id, None, Some("test.cancel"), None, None)
        .await
        .expect("cancel workflow run");
    tx.commit().await.expect("commit cancel tx");

    let job_count = sqlx::query_scalar!(
        "SELECT count(*) AS \"count!\"
         FROM job_queue jq
         JOIN workflow_steps ws ON ws.job_id = jq.id
         WHERE ws.workflow_run_id = $1",
        run.id,
    )
    .fetch_one(&pool)
    .await
    .expect("count workflow jobs");
    assert_eq!(job_count, 0);

    let steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list workflow steps");
    assert_eq!(steps.len(), 2);
    assert!(
        steps
            .iter()
            .all(|step| step.status == WorkflowStepStatus::Canceled)
    );

    teardown_ephemeral_pool(pool, database).await;
}

async fn wait_for_cancel_to_block_on_job_lock(pool: &sqlx::PgPool) {
    for _ in 0..100 {
        let waiting = sqlx::query_scalar!(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_stat_activity
                 WHERE wait_event_type = 'Lock'
                   AND query LIKE '%runledger:lock_workflow_step_jobs_for_update%'
             ) AS \"waiting!\"",
        )
        .fetch_one(pool)
        .await
        .expect("query waiting cancel activity");

        if waiting {
            return;
        }

        sleep(Duration::from_millis(50)).await;
    }

    panic!("cancel workflow run did not block on the job-row lock");
}

async fn wait_for_cancel_to_block_on_release_lock(pool: &sqlx::PgPool) {
    for _ in 0..100 {
        let waiting = sqlx::query_scalar!(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_stat_activity
                 WHERE wait_event_type = 'Lock'
                   AND query LIKE '%runledger:lock_workflow_run_release%'
             ) AS \"waiting!\"",
        )
        .fetch_one(pool)
        .await
        .expect("query waiting cancel advisory activity");

        if waiting {
            return;
        }

        sleep(Duration::from_millis(50)).await;
    }

    panic!("cancel workflow run did not block on the release advisory lock");
}

async fn wait_for_cancel_to_block_on_workflow_step_lock(pool: &sqlx::PgPool) {
    for _ in 0..100 {
        let waiting = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_stat_activity
                 WHERE wait_event_type = 'Lock'
                   AND query LIKE '%runledger:lock_workflow_steps_for_update%'
                   AND query NOT LIKE '%pg_stat_activity%'
             )",
        )
        .fetch_one(pool)
        .await
        .expect("query waiting cancel workflow step activity");

        if waiting {
            return;
        }

        sleep(Duration::from_millis(50)).await;
    }

    panic!("cancel workflow run did not block on the workflow step lock");
}

async fn wait_for_completion_to_block_on_shared_release_lock(pool: &sqlx::PgPool) {
    for _ in 0..90 {
        let waiting = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_stat_activity
                 WHERE wait_event_type = 'Lock'
                   AND query LIKE '%runledger:lock_workflow_run_release_shared%'
                   AND query NOT LIKE '%pg_stat_activity%'
             )",
        )
        .fetch_one(pool)
        .await
        .expect("query waiting completion advisory activity");

        if waiting {
            return;
        }

        sleep(Duration::from_millis(50)).await;
    }

    panic!("job completion did not block on the shared release advisory lock");
}

async fn wait_for_workflow_step_lock_wait(pool: &sqlx::PgPool) {
    for _ in 0..100 {
        let waiting = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_stat_activity
                 WHERE wait_event_type = 'Lock'
                   AND query LIKE '%runledger:lock_workflow_step_rows_for_update%'
                   AND query NOT LIKE '%pg_stat_activity%'
             )",
        )
        .fetch_one(pool)
        .await
        .expect("query waiting workflow step activity");

        if waiting {
            return;
        }

        sleep(Duration::from_millis(50)).await;
    }

    panic!("workflow step mutation did not block on the held workflow step lock");
}

async fn release_blocked_job_step_for_test(
    pool: &sqlx::PgPool,
    prerequisite: &WorkflowStepDbRecord,
    step: &WorkflowStepDbRecord,
) -> Uuid {
    // This bypasses the normal release path deliberately: during the race under
    // test, cancel owns the exclusive release advisory lock, so a real release
    // would wait instead of inserting the late job row before cancel resumes.
    // Keep this insert/update in sync with release_candidate_step_tx's job-step branch.
    let job_type = step
        .job_type
        .as_ref()
        .expect("job-backed step should have job_type")
        .as_str();
    let priority = step.priority.expect("job-backed step should have priority");
    let max_attempts = step
        .max_attempts
        .expect("job-backed step should have max_attempts");
    let timeout_seconds = step
        .timeout_seconds
        .expect("job-backed step should have timeout_seconds");
    let stage = step.stage.expect("job-backed step should have stage");

    let mut tx = pool.begin().await.expect("begin late job release tx");
    let prerequisite_updated = sqlx::query!(
        "UPDATE workflow_steps
         SET status = 'CANCELED',
             finished_at = COALESCE(finished_at, now()),
             updated_at = now()
         WHERE id = $1
           AND status = 'WAITING_FOR_EXTERNAL'",
        prerequisite.id,
    )
    .execute(&mut *tx)
    .await
    .expect("mark prerequisite terminal for late release")
    .rows_affected();
    assert_eq!(prerequisite_updated, 1);

    let row = sqlx::query!(
        "INSERT INTO job_queue (
            job_type,
            organization_id,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            next_run_at,
            workflow_step_id,
            stage
         )
         VALUES ($1, $2, $3::jsonb, $4, $5, $6, now(), $7, $8)
         RETURNING id",
        job_type,
        step.organization_id,
        &step.payload,
        priority,
        max_attempts,
        timeout_seconds,
        step.id,
        stage.as_db_value(),
    )
    .fetch_one(&mut *tx)
    .await
    .expect("insert late workflow job");

    let updated = sqlx::query!(
        "UPDATE workflow_steps
         SET status = 'ENQUEUED',
             job_id = $2,
             released_at = COALESCE(released_at, now()),
             dependency_count_pending = 0,
             dependency_count_unsatisfied = 0,
             status_reason = NULL,
             last_error_code = NULL,
             last_error_message = NULL,
             updated_at = now()
         WHERE id = $1
           AND status = 'BLOCKED'
           AND job_id IS NULL",
        step.id,
        row.id,
    )
    .execute(&mut *tx)
    .await
    .expect("mark late workflow job step enqueued")
    .rows_affected();
    assert_eq!(updated, 1);

    tx.commit().await.expect("commit late job release tx");
    row.id
}
