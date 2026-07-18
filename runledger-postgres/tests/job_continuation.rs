use std::time::Duration;

use runledger_core::jobs::{
    JobEventType, JobStage, JobStatus, JobType, StepKey, WorkflowRunEnqueueBuilder,
    WorkflowStepEnqueueBuilder, WorkflowStepStatus, WorkflowType,
};
use runledger_postgres::jobs::{
    JobContinuationUpdate, complete_job_continuation_with_outcome, enqueue_workflow_run,
    get_job_by_id, list_job_events, list_workflow_steps,
};
use runledger_postgres::{Error, QueryErrorKind};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::{Value, json};
use sqlx::Row;

mod support;

use support::{claim_one_job, enqueue_test_job, register_test_job_definition};

const JOB_TYPE: &str = "jobs.test.handler_continuation";

fn assert_lease_mismatch(error: Error) {
    match error {
        Error::QueryError(error) => {
            assert_eq!(error.kind(), Some(QueryErrorKind::JobLeaseOwnerMismatch));
            assert_eq!(error.code(), "job.lease_owner_mismatch");
        }
        other => panic!("expected lease mismatch query error, got {other:?}"),
    }
}

#[tokio::test]
async fn handler_continuation_closes_attempt_and_starts_a_fresh_run() {
    let (pool, database) = setup_ephemeral_pool("postgres_handler_continuation", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let payload = json!({"target": "continuation"});
    let job_id = enqueue_test_job(&pool, JOB_TYPE, None, &payload).await;
    let claim = claim_one_job(&pool, "worker-continuation").await;
    let worker_id = claim.worker_id.as_deref().expect("claimed worker id");
    let checkpoint = json!({"cursor": 25});

    let database_time_before =
        sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>("SELECT clock_timestamp()")
            .fetch_one(&pool)
            .await
            .expect("database time before continuation");
    let outcome = complete_job_continuation_with_outcome(
        &pool,
        claim.id,
        claim.run_number,
        claim.attempt,
        worker_id,
        &JobContinuationUpdate {
            delay: Duration::from_millis(100),
            progress_done: Some(25),
            progress_total: Some(100),
            checkpoint: Some(&checkpoint),
        },
    )
    .await
    .expect("persist continuation");

    assert_eq!(outcome.job_id, job_id);
    assert_eq!(outcome.completed_run_number, 1);
    assert_eq!(outcome.next_run_number, 2);
    assert_eq!(outcome.attempt, 1);
    assert_eq!(outcome.max_attempts, 3);
    assert_eq!(outcome.progress_done, Some(25));
    assert_eq!(outcome.progress_total, Some(100));
    assert!(outcome.next_run_at >= database_time_before + chrono::Duration::milliseconds(100));

    let pending = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load continued job")
        .expect("continued job exists");
    assert_eq!(pending.status, JobStatus::Pending);
    assert_eq!(pending.stage, JobStage::Queued);
    assert_eq!(pending.run_number, 2);
    assert_eq!(pending.attempt, 0);
    assert_eq!(pending.progress_done, Some(25));
    assert_eq!(pending.progress_total, Some(100));
    assert_eq!(pending.checkpoint, Some(checkpoint));
    assert_eq!(
        pending.status_reason.as_deref(),
        Some("HANDLER_CONTINUATION")
    );
    assert!(pending.worker_id.is_none());
    assert!(pending.lease_expires_at.is_none());
    assert!(pending.last_heartbeat_at.is_none());
    assert!(pending.started_at.is_none());
    assert!(pending.finished_at.is_none());
    assert!(pending.output.is_none());

    let attempt = sqlx::query(
        "SELECT finished_at, outcome::text AS outcome, error_code, retry_delay_ms
         FROM job_attempts
         WHERE job_id = $1 AND run_number = 1 AND attempt = 1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("load completed continuation attempt");
    assert!(
        attempt
            .try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("finished_at")
            .expect("finished_at column")
            .is_some()
    );
    assert_eq!(
        attempt
            .try_get::<Option<String>, _>("outcome")
            .expect("outcome column"),
        None
    );
    assert_eq!(
        attempt
            .try_get::<Option<String>, _>("error_code")
            .expect("error_code column"),
        None
    );
    assert_eq!(
        attempt
            .try_get::<Option<i32>, _>("retry_delay_ms")
            .expect("retry_delay_ms column"),
        None
    );

    let events = list_job_events(&pool, None, job_id, 10, None)
        .await
        .expect("list continuation events");
    assert_eq!(
        events
            .iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![
            JobEventType::Enqueued,
            JobEventType::Leased,
            JobEventType::Requeued
        ]
    );
    let event = events.last().expect("continuation event");
    assert_eq!(event.run_number, 1);
    assert_eq!(event.attempt, Some(1));
    assert_eq!(event.stage, Some(JobStage::Queued));
    assert_eq!(event.progress_done, Some(25));
    assert_eq!(event.progress_total, Some(100));
    assert_eq!(
        event.payload.get("reason").and_then(Value::as_str),
        Some("HANDLER_CONTINUATION")
    );
    assert_eq!(
        event.payload.get("next_run_number").and_then(Value::as_i64),
        Some(2)
    );
    assert_eq!(
        event
            .payload
            .get("delay_microseconds")
            .and_then(Value::as_i64),
        Some(100_000)
    );

    tokio::time::sleep(Duration::from_millis(125)).await;
    let next_claim = claim_one_job(&pool, "worker-continuation-next").await;
    assert_eq!(next_claim.id, job_id);
    assert_eq!(next_claim.run_number, 2);
    assert_eq!(next_claim.attempt, 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn handler_continuation_requires_the_exact_live_lease() {
    let (pool, database) = setup_ephemeral_pool("postgres_continuation_exact_lease", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let payload = json!({"target": "continuation"});
    let job_id = enqueue_test_job(&pool, JOB_TYPE, None, &payload).await;
    let claim = claim_one_job(&pool, "worker-continuation-owner").await;
    let before = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load leased job")
        .expect("leased job exists");
    let continuation = JobContinuationUpdate {
        delay: Duration::ZERO,
        progress_done: None,
        progress_total: None,
        checkpoint: None,
    };

    assert_lease_mismatch(
        complete_job_continuation_with_outcome(
            &pool,
            claim.id,
            claim.run_number,
            claim.attempt,
            "different-worker",
            &continuation,
        )
        .await
        .expect_err("wrong worker must not continue job"),
    );
    assert_lease_mismatch(
        complete_job_continuation_with_outcome(
            &pool,
            claim.id,
            claim.run_number + 1,
            claim.attempt,
            claim.worker_id.as_deref().expect("worker id"),
            &continuation,
        )
        .await
        .expect_err("wrong run must not continue job"),
    );

    sqlx::query("UPDATE job_queue SET lease_expires_at = clock_timestamp() WHERE id = $1")
        .bind(job_id)
        .execute(&pool)
        .await
        .expect("expire lease");
    assert_lease_mismatch(
        complete_job_continuation_with_outcome(
            &pool,
            claim.id,
            claim.run_number,
            claim.attempt,
            claim.worker_id.as_deref().expect("worker id"),
            &continuation,
        )
        .await
        .expect_err("expired lease must not continue job"),
    );

    let after = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load unchanged leased job")
        .expect("job exists");
    assert_eq!(after.status, JobStatus::Leased);
    assert_eq!(after.run_number, before.run_number);
    assert_eq!(after.attempt, before.attempt);
    assert_eq!(after.worker_id, before.worker_id);
    assert_eq!(
        list_job_events(&pool, None, job_id, 10, None)
            .await
            .expect("list events")
            .into_iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![JobEventType::Enqueued, JobEventType::Leased]
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn handler_continuation_rejects_workflow_managed_jobs_without_mutation() {
    let (pool, database) = setup_ephemeral_pool("postgres_workflow_continuation_rejected", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let payload = json!({"target": "workflow-continuation"});
    let metadata = json!({"test": "workflow-continuation-rejected"});
    let step =
        WorkflowStepEnqueueBuilder::new(StepKey::new("step"), JobType::new(JOB_TYPE), &payload)
            .try_build()
            .expect("build workflow step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.continuation-rejected"),
        &metadata,
    )
    .step(step)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow");
    let job_id = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list workflow steps")
        .into_iter()
        .next()
        .and_then(|step| step.job_id)
        .expect("workflow step job should be released");
    let claim = claim_one_job(&pool, "worker-workflow-continuation").await;
    assert_eq!(claim.id, job_id);
    let worker_id = claim.worker_id.as_deref().expect("claimed worker id");
    let checkpoint = json!({"cursor": 1});

    let error = complete_job_continuation_with_outcome(
        &pool,
        claim.id,
        claim.run_number,
        claim.attempt,
        worker_id,
        &JobContinuationUpdate {
            delay: Duration::ZERO,
            progress_done: Some(1),
            progress_total: Some(2),
            checkpoint: Some(&checkpoint),
        },
    )
    .await
    .expect_err("workflow-managed continuation must be rejected");
    match error {
        Error::QueryError(error) => {
            assert_eq!(
                error.kind(),
                Some(QueryErrorKind::JobWorkflowRequeueNotSupported)
            );
            assert_eq!(error.code(), "job.workflow_requeue_not_supported");
        }
        other => panic!("expected workflow requeue rejection, got {other:?}"),
    }

    let unchanged = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load unchanged workflow job")
        .expect("workflow job exists");
    assert_eq!(unchanged.status, JobStatus::Leased);
    assert_eq!(unchanged.run_number, 1);
    assert_eq!(unchanged.attempt, 1);
    assert!(unchanged.progress_done.is_none());
    assert!(unchanged.checkpoint.is_none());
    let steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list unchanged workflow steps");
    assert_eq!(steps[0].status, WorkflowStepStatus::Running);
    assert!(
        list_job_events(&pool, None, job_id, 20, None)
            .await
            .expect("list workflow job events")
            .iter()
            .all(|event| event.event_type != JobEventType::Requeued)
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn handler_continuation_rejects_a_delay_beyond_the_schedulable_timestamp_range() {
    let (pool, database) = setup_ephemeral_pool("postgres_continuation_delay_range", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let payload = json!({"target": "continuation"});
    let job_id = enqueue_test_job(&pool, JOB_TYPE, None, &payload).await;
    let claim = claim_one_job(&pool, "worker-continuation-delay-range").await;
    let worker_id = claim.worker_id.as_deref().expect("claimed worker id");

    let error = complete_job_continuation_with_outcome(
        &pool,
        claim.id,
        claim.run_number,
        claim.attempt,
        worker_id,
        &JobContinuationUpdate {
            // This still fits in signed 64-bit microseconds, but adding it to
            // the current time exceeds the timestamp range SQLx can decode.
            delay: Duration::from_micros(i64::MAX as u64),
            progress_done: None,
            progress_total: None,
            checkpoint: None,
        },
    )
    .await
    .expect_err("unschedulable continuation delay must be rejected");
    match error {
        Error::QueryError(error) => {
            assert_eq!(
                error.kind(),
                Some(QueryErrorKind::JobInvalidContinuationDelay)
            );
            assert_eq!(error.code(), "job.invalid_continuation_delay");
        }
        other => panic!("expected invalid continuation delay, got {other:?}"),
    }

    let unchanged = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load unchanged job")
        .expect("job exists");
    assert_eq!(unchanged.status, JobStatus::Leased);
    assert_eq!(unchanged.run_number, claim.run_number);
    assert_eq!(unchanged.attempt, claim.attempt);
    assert_eq!(unchanged.worker_id.as_deref(), Some(worker_id));
    assert_eq!(
        list_job_events(&pool, None, job_id, 10, None)
            .await
            .expect("list unchanged events")
            .into_iter()
            .map(|event| event.event_type)
            .collect::<Vec<_>>(),
        vec![JobEventType::Enqueued, JobEventType::Leased]
    );

    teardown_ephemeral_pool(pool, database).await;
}
