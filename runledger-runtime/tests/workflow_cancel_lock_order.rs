use std::time::Duration;

use runledger_core::jobs::{
    JobType, StepKey, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder, WorkflowStepStatus,
    WorkflowType,
};
use runledger_postgres::jobs::test_support::workflow_run_release_lock_key;
use runledger_postgres::jobs::{
    CompleteExternalWorkflowStepInput, JobDefinitionUpsert, WorkflowStepDbRecord,
    cancel_workflow_run_tx, complete_external_workflow_step_tx, enqueue_workflow_run,
    list_workflow_steps, upsert_job_definition_tx,
};
use serde_json::json;
use sqlx::types::Uuid;
use tokio::time::{sleep, timeout};

#[path = "../test_support.rs"]
mod test_support;

use test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};

// These tests observe blocking through SQL markers in pg_stat_activity. If a
// marker changes, update the matching wait helper rather than weakening the
// production SQL comments.

async fn register_job_definition(pool: &sqlx::PgPool, job_type: JobType<'static>) {
    let mut setup_tx = pool.begin().await.expect("begin setup tx");
    upsert_job_definition_tx(
        &mut setup_tx,
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
    setup_tx.commit().await.expect("commit setup tx");
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
    let cancel_task = tokio::spawn(async move {
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
    timeout(Duration::from_secs(5), cancel_task)
        .await
        .expect("cancel task should finish after job lock release")
        .expect("cancel task should not panic")
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
    let cancel_task = tokio::spawn(async move {
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
    timeout(Duration::from_secs(5), cancel_task)
        .await
        .expect("cancel task should finish after job lock release")
        .expect("cancel task should not panic")
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

async fn release_blocked_job_step_for_test(
    pool: &sqlx::PgPool,
    prerequisite: &WorkflowStepDbRecord,
    step: &WorkflowStepDbRecord,
) -> Uuid {
    // This bypasses the normal release path deliberately: during the race under
    // test, cancel owns the exclusive release advisory lock, so a real release
    // would no-op before inserting the late job row. Keep this insert/update in
    // sync with release_candidate_step_tx's job-step branch.
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
