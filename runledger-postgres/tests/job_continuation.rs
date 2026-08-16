use std::time::Duration;

use runledger_core::jobs::{
    JobEventType, JobStage, JobStatus, JobType, StepKey, WorkflowRunEnqueueBuilder,
    WorkflowStepEnqueueBuilder, WorkflowStepStatus, WorkflowType,
};
use runledger_postgres::jobs::{
    CompareAndRequeueJob, CompareAndRequeueJobOutcome, DecodedJobEventPayload,
    DecodedRequeuedEventPayload, JobContinuationUpdate, JobRequeueStatePolicy, cancel_job,
    claim_jobs, compare_and_requeue_job, complete_job_continuation_with_outcome,
    complete_job_success, enqueue_workflow_run, get_job_by_id, get_job_continuation_metrics,
    get_workflow_run_by_id, list_job_events, list_workflow_steps,
};
use runledger_postgres::{Error, QueryErrorKind};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::{Value, json};
use sqlx::{Row, types::Uuid};

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

fn plan_contains_node_type(value: &Value, expected_node_type: &str) -> bool {
    match value {
        Value::Array(values) => values
            .iter()
            .any(|value| plan_contains_node_type(value, expected_node_type)),
        Value::Object(fields) => {
            fields.get("Node Type").and_then(Value::as_str) == Some(expected_node_type)
                || fields
                    .values()
                    .any(|value| plan_contains_node_type(value, expected_node_type))
        }
        _ => false,
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
    match event.decoded_payload() {
        DecodedJobEventPayload::Requeued(DecodedRequeuedEventPayload::HandlerContinuation {
            reason,
            next_run_number,
            next_run_at,
            delay_microseconds,
            ..
        }) => {
            assert_eq!(reason, "HANDLER_CONTINUATION");
            assert_eq!(next_run_number, 2);
            assert_eq!(next_run_at, outcome.next_run_at);
            assert_eq!(delay_microseconds, 100_000);
        }
        payload => panic!("expected decoded handler-continuation payload, got {payload:?}"),
    }
    assert_eq!(
        event.payload.get("reason").and_then(Value::as_str),
        Some("HANDLER_CONTINUATION")
    );
    assert_eq!(
        event.payload.get("requeue_kind").and_then(Value::as_str),
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

    let active_metrics = get_job_continuation_metrics(&pool, None, Some(JOB_TYPE))
        .await
        .expect("load active continuation metrics")
        .pop()
        .expect("registered job type has continuation metrics");
    assert_eq!(active_metrics.continued_24h, 1);
    assert_eq!(active_metrics.active_continued_count, 1);
    assert_eq!(active_metrics.max_active_run_number, 2);

    complete_job_success(
        &pool,
        next_claim.id,
        next_claim.run_number,
        next_claim.attempt,
        next_claim
            .worker_id
            .as_deref()
            .expect("next claim has worker id"),
        None,
    )
    .await
    .expect("complete continued job");
    let terminal_metrics = get_job_continuation_metrics(&pool, None, Some(JOB_TYPE))
        .await
        .expect("load terminal continuation metrics")
        .pop()
        .expect("registered job type has continuation metrics");
    assert_eq!(terminal_metrics.continued_24h, 1);
    assert_eq!(terminal_metrics.active_continued_count, 0);
    assert_eq!(terminal_metrics.max_active_run_number, 0);

    #[expect(
        deprecated,
        reason = "the regression test exercises the legacy admin requeue compatibility entrypoint"
    )]
    let admin_requeued = runledger_postgres::jobs::requeue_job(
        &pool,
        None,
        job_id,
        Some("ordinary admin replay after terminal success"),
    )
    .await
    .expect("legacy admin requeue continued terminal job");
    assert_eq!(admin_requeued.status, JobStatus::Pending);
    assert_eq!(admin_requeued.run_number, 3);
    let admin_events = list_job_events(&pool, None, job_id, 10, None)
        .await
        .expect("list ordinary admin requeue events");
    let admin_event = admin_events.last().expect("ordinary admin requeue event");
    match admin_event.decoded_payload() {
        DecodedJobEventPayload::Requeued(DecodedRequeuedEventPayload::Basic { reason, .. }) => {
            assert_eq!(reason, "ordinary admin replay after terminal success")
        }
        payload => panic!("expected decoded basic requeue payload, got {payload:?}"),
    }
    assert_eq!(
        admin_event
            .payload
            .get("requeue_kind")
            .and_then(Value::as_str),
        Some("BASIC")
    );
    let admin_requeue_metrics = get_job_continuation_metrics(&pool, None, Some(JOB_TYPE))
        .await
        .expect("load metrics after ordinary admin requeue")
        .pop()
        .expect("registered job type has continuation metrics");
    assert_eq!(admin_requeue_metrics.continued_24h, 1);
    assert_eq!(admin_requeue_metrics.active_continued_count, 0);
    assert_eq!(admin_requeue_metrics.max_active_run_number, 0);

    let collision_job_id = enqueue_test_job(
        &pool,
        JOB_TYPE,
        None,
        &json!({"target": "ordinary-requeue-reason-collision"}),
    )
    .await;
    cancel_job(
        &pool,
        None,
        collision_job_id,
        Some("prepare ordinary requeue"),
    )
    .await
    .expect("cancel reason-collision job");
    let canceled = get_job_by_id(&pool, None, collision_job_id)
        .await
        .expect("load reason-collision job")
        .expect("reason-collision job exists");
    let request = CompareAndRequeueJob::from_observed_job(
        &canceled,
        JobRequeueStatePolicy::ResetProgressAndCheckpoint,
        "HANDLER_CONTINUATION",
    )
    .expect("canceled reason-collision job is recoverable");
    let collision_requeue = compare_and_requeue_job(&pool, request)
        .await
        .expect("ordinary requeue may use the same free-form reason");
    let CompareAndRequeueJobOutcome::Requeued {
        after: collision_after,
        ..
    } = collision_requeue
    else {
        panic!("expected reason-collision job to be requeued");
    };
    let collision_event = list_job_events(&pool, None, collision_job_id, 10, None)
        .await
        .expect("list reason-collision events")
        .pop()
        .expect("reason-collision requeue event exists");
    match collision_event.decoded_payload() {
        DecodedJobEventPayload::Requeued(DecodedRequeuedEventPayload::CompareAndRequeue {
            reason,
            state_policy,
            ..
        }) => {
            assert_eq!(reason, "HANDLER_CONTINUATION");
            assert_eq!(
                state_policy,
                JobRequeueStatePolicy::ResetProgressAndCheckpoint
            );
        }
        payload => panic!("expected decoded compare-and-requeue payload, got {payload:?}"),
    }
    assert_eq!(
        collision_event
            .payload
            .get("requeue_kind")
            .and_then(Value::as_str),
        Some("COMPARE_AND_REQUEUE")
    );

    // A future delayed administrative requeue may carry the same schedule
    // shape. Its stable discriminator must keep it out of continuation metrics
    // even if an operator also chose the legacy continuation reason string.
    sqlx::query(
        "UPDATE job_events
         SET payload = payload || jsonb_build_object(
             'next_run_number', $2::int4,
             'next_run_at', clock_timestamp() + interval '1 hour',
             'delay_microseconds', 3600000000::bigint
         )
         WHERE id = $1",
    )
    .bind(collision_event.id)
    .bind(collision_after.run_number)
    .execute(&pool)
    .await
    .expect("shape future non-continuation requeue event");

    let collision_safe_metrics = get_job_continuation_metrics(&pool, None, Some(JOB_TYPE))
        .await
        .expect("load collision-safe continuation metrics")
        .pop()
        .expect("registered job type has continuation metrics");
    assert_eq!(collision_safe_metrics.continued_24h, 1);
    assert_eq!(collision_safe_metrics.active_continued_count, 0);
    assert_eq!(collision_safe_metrics.max_active_run_number, 0);

    teardown_ephemeral_pool(pool, database).await;
}

async fn continue_next_due_job(
    pool: &runledger_postgres::DbPool,
    worker_id: &str,
    delay: Duration,
) {
    let claim = claim_one_job(pool, worker_id).await;
    complete_job_continuation_with_outcome(
        pool,
        claim.id,
        claim.run_number,
        claim.attempt,
        claim.worker_id.as_deref().expect("claimed worker id"),
        &JobContinuationUpdate {
            delay,
            progress_done: None,
            progress_total: None,
            checkpoint: None,
        },
    )
    .await
    .expect("continue due metrics job");
}

#[tokio::test]
async fn continuation_metrics_filter_exact_tenants_and_aggregate_all_scopes() {
    let (pool, database) = setup_ephemeral_pool("postgres_continuation_metrics_scope", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let organization_a = Uuid::from_u128(601);
    let organization_b = Uuid::from_u128(602);
    let unrelated_organization = Uuid::from_u128(603);
    let long_delay = Duration::from_secs(3_600);

    enqueue_test_job(&pool, JOB_TYPE, None, &json!({"scope": "global"})).await;
    continue_next_due_job(&pool, "worker-continuation-metrics-global", long_delay).await;

    enqueue_test_job(
        &pool,
        JOB_TYPE,
        Some(organization_a),
        &json!({"scope": "organization-a"}),
    )
    .await;
    continue_next_due_job(&pool, "worker-continuation-metrics-org-a", long_delay).await;

    enqueue_test_job(
        &pool,
        JOB_TYPE,
        Some(organization_b),
        &json!({"scope": "organization-b"}),
    )
    .await;
    continue_next_due_job(
        &pool,
        "worker-continuation-metrics-org-b-run-1",
        Duration::ZERO,
    )
    .await;
    continue_next_due_job(&pool, "worker-continuation-metrics-org-b-run-2", long_delay).await;

    let organization_a_metrics =
        get_job_continuation_metrics(&pool, Some(organization_a), Some(JOB_TYPE))
            .await
            .expect("load organization A continuation metrics")
            .pop()
            .expect("registered job type has organization A metrics");
    assert_eq!(organization_a_metrics.continued_24h, 1);
    assert_eq!(organization_a_metrics.active_continued_count, 1);
    assert_eq!(organization_a_metrics.max_active_run_number, 2);

    let organization_b_metrics =
        get_job_continuation_metrics(&pool, Some(organization_b), Some(JOB_TYPE))
            .await
            .expect("load organization B continuation metrics")
            .pop()
            .expect("registered job type has organization B metrics");
    assert_eq!(organization_b_metrics.continued_24h, 2);
    assert_eq!(organization_b_metrics.active_continued_count, 1);
    assert_eq!(organization_b_metrics.max_active_run_number, 3);

    let unrelated_metrics =
        get_job_continuation_metrics(&pool, Some(unrelated_organization), Some(JOB_TYPE))
            .await
            .expect("load unrelated organization continuation metrics")
            .pop()
            .expect("registered job type has zero-valued unrelated metrics");
    assert_eq!(unrelated_metrics.continued_24h, 0);
    assert_eq!(unrelated_metrics.active_continued_count, 0);
    assert_eq!(unrelated_metrics.max_active_run_number, 0);

    let aggregate_metrics = get_job_continuation_metrics(&pool, None, Some(JOB_TYPE))
        .await
        .expect("load aggregate continuation metrics")
        .pop()
        .expect("registered job type has aggregate metrics");
    assert_eq!(aggregate_metrics.continued_24h, 4);
    assert_eq!(aggregate_metrics.active_continued_count, 3);
    assert_eq!(aggregate_metrics.max_active_run_number, 3);

    let scoped_plan = sqlx::query_scalar::<_, Value>(
        "EXPLAIN (FORMAT JSON)
         SELECT
            jd.job_type,
            COALESCE(SUM(jcmr.continued_24h), 0)::bigint AS continued_24h,
            COALESCE(SUM(jcmr.active_continued_count), 0)::bigint
                AS active_continued_count,
            COALESCE(MAX(jcmr.max_active_run_number), 0)::int4
                AS max_active_run_number
         FROM job_definitions jd
         LEFT JOIN job_continuation_metrics_rollup jcmr
           ON jcmr.job_type = jd.job_type
          AND ($1::uuid IS NULL OR jcmr.organization_id = $1)
         WHERE ($2::text IS NULL OR jd.job_type = $2)
         GROUP BY jd.job_type
         ORDER BY jd.job_type ASC",
    )
    .bind(organization_a)
    .bind(JOB_TYPE)
    .fetch_one(&pool)
    .await
    .expect("explain scoped continuation metrics query");
    assert!(
        !plan_contains_node_type(&scoped_plan, "CTE Scan"),
        "scoped continuation metrics must not materialize and rescan global aggregates: {scoped_plan}"
    );

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
                Some(QueryErrorKind::JobWorkflowHandlerContinuationNotEnabled)
            );
            assert_eq!(
                error.code(),
                "job.workflow_handler_continuation_not_enabled"
            );
            assert_eq!(
                error.client_message(),
                "Workflow step handler continuation is not enabled."
            );
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
async fn opted_in_workflow_step_continuation_atomically_requeues_job_and_step() {
    let (pool, database) = setup_ephemeral_pool("postgres_workflow_continuation_enabled", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let payload = json!({"target": "workflow-continuation"});
    let metadata = json!({"test": "workflow-continuation-enabled"});
    let step =
        WorkflowStepEnqueueBuilder::new(StepKey::new("step"), JobType::new(JOB_TYPE), &payload)
            .allow_handler_continuation()
            .try_build()
            .expect("build continuation-enabled workflow step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.continuation-enabled"),
        &metadata,
    )
    .step(step)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow");
    let initial_step = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list initial workflow steps")
        .into_iter()
        .next()
        .expect("workflow step exists");
    assert!(initial_step.allow_handler_continuation);
    let job_id = initial_step
        .job_id
        .expect("root step job should be released");
    let claim = claim_one_job(&pool, "worker-workflow-continuation-enabled").await;
    assert_eq!(claim.id, job_id);
    let checkpoint = json!({"cursor": 1});

    let outcome = complete_job_continuation_with_outcome(
        &pool,
        claim.id,
        claim.run_number,
        claim.attempt,
        claim.worker_id.as_deref().expect("claimed worker id"),
        &JobContinuationUpdate {
            delay: Duration::ZERO,
            progress_done: Some(1),
            progress_total: Some(2),
            checkpoint: Some(&checkpoint),
        },
    )
    .await
    .expect("continue opted-in workflow step");

    assert_eq!(outcome.job_id, job_id);
    assert_eq!(outcome.completed_run_number, 1);
    assert_eq!(outcome.next_run_number, 2);
    let continued = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load continued workflow job")
        .expect("workflow job exists");
    assert_eq!(continued.status, JobStatus::Pending);
    assert_eq!(continued.run_number, 2);
    assert_eq!(continued.attempt, 0);
    assert_eq!(continued.progress_done, Some(1));
    assert_eq!(continued.progress_total, Some(2));
    assert_eq!(continued.checkpoint, Some(checkpoint));

    let continued_steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list continued workflow steps");
    assert_eq!(continued_steps.len(), 1);
    assert_eq!(continued_steps[0].id, initial_step.id);
    assert_eq!(continued_steps[0].job_id, Some(job_id));
    assert_eq!(continued_steps[0].status, WorkflowStepStatus::Enqueued);
    assert!(continued_steps[0].finished_at.is_none());
    assert_eq!(
        continued_steps[0].status_reason.as_deref(),
        Some("HANDLER_CONTINUATION")
    );

    let second_claim = claim_one_job(&pool, "worker-workflow-continuation-final").await;
    assert_eq!(second_claim.id, job_id);
    assert_eq!(second_claim.run_number, 2);
    assert_eq!(second_claim.attempt, 1);
    complete_job_success(
        &pool,
        second_claim.id,
        second_claim.run_number,
        second_claim.attempt,
        second_claim
            .worker_id
            .as_deref()
            .expect("second claim worker id"),
        None,
    )
    .await
    .expect("complete continued workflow step");
    let terminal_steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list terminal workflow steps");
    assert_eq!(terminal_steps[0].status, WorkflowStepStatus::Succeeded);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn delayed_workflow_step_continuation_stays_active_and_unclaimable() {
    let (pool, database) = setup_ephemeral_pool("postgres_workflow_continuation_delayed", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let payload = json!({"target": "delayed-workflow-continuation"});
    let metadata = json!({"test": "delayed-workflow-continuation"});
    let step =
        WorkflowStepEnqueueBuilder::new(StepKey::new("step"), JobType::new(JOB_TYPE), &payload)
            .allow_handler_continuation()
            .try_build()
            .expect("build delayed continuation step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.continuation-delayed"),
        &metadata,
    )
    .step(step)
    .try_build()
    .expect("build delayed continuation workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue delayed continuation workflow");
    let job_id = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list delayed workflow steps")[0]
        .job_id
        .expect("root job should be released");
    let claim = claim_one_job(&pool, "worker-delayed-workflow-continuation").await;
    let database_time_before =
        sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>("SELECT clock_timestamp()")
            .fetch_one(&pool)
            .await
            .expect("load database time");
    let outcome = complete_job_continuation_with_outcome(
        &pool,
        claim.id,
        claim.run_number,
        claim.attempt,
        claim.worker_id.as_deref().expect("delayed worker id"),
        &JobContinuationUpdate {
            delay: Duration::from_secs(60),
            progress_done: None,
            progress_total: None,
            checkpoint: None,
        },
    )
    .await
    .expect("schedule delayed workflow continuation");

    assert!(outcome.next_run_at >= database_time_before + chrono::Duration::seconds(60));
    assert!(
        claim_jobs(&pool, "worker-too-early", 30, 1)
            .await
            .expect("attempt early claim")
            .is_empty()
    );
    assert_eq!(
        get_workflow_run_by_id(&pool, None, run.id)
            .await
            .expect("load active delayed workflow")
            .expect("workflow exists")
            .status,
        runledger_core::jobs::WorkflowRunStatus::Running
    );
    let steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list delayed workflow steps");
    assert_eq!(steps[0].status, WorkflowStepStatus::Enqueued);
    assert_eq!(
        get_job_by_id(&pool, None, job_id)
            .await
            .expect("load delayed job")
            .expect("delayed job exists")
            .status,
        JobStatus::Pending
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_step_continuation_event_failure_rolls_back_job_step_and_checkpoint() {
    let (pool, database) = setup_ephemeral_pool("postgres_workflow_continuation_rollback", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let payload = json!({"target": "workflow-continuation-rollback"});
    let metadata = json!({"test": "workflow-continuation-rollback"});
    let step =
        WorkflowStepEnqueueBuilder::new(StepKey::new("step"), JobType::new(JOB_TYPE), &payload)
            .allow_handler_continuation()
            .try_build()
            .expect("build continuation rollback step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.continuation-rollback"),
        &metadata,
    )
    .step(step)
    .try_build()
    .expect("build continuation rollback workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue continuation rollback workflow");
    let initial_step = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list rollback workflow steps")
        .into_iter()
        .next()
        .expect("rollback workflow step exists");
    let job_id = initial_step.job_id.expect("root job should be released");
    let claim = claim_one_job(&pool, "worker-workflow-continuation-rollback").await;
    sqlx::query(
        "CREATE FUNCTION fail_handler_continuation_event()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             IF NEW.event_type = 'REQUEUED' THEN
                 RAISE EXCEPTION 'injected continuation event failure';
             END IF;
             RETURN NEW;
         END;
         $$",
    )
    .execute(&pool)
    .await
    .expect("create injected failure function");
    sqlx::query(
        "CREATE TRIGGER fail_handler_continuation_event
         BEFORE INSERT ON job_events
         FOR EACH ROW
         EXECUTE FUNCTION fail_handler_continuation_event()",
    )
    .execute(&pool)
    .await
    .expect("create injected failure trigger");
    let checkpoint = json!({"cursor": "must-roll-back"});

    complete_job_continuation_with_outcome(
        &pool,
        claim.id,
        claim.run_number,
        claim.attempt,
        claim.worker_id.as_deref().expect("rollback worker id"),
        &JobContinuationUpdate {
            delay: Duration::ZERO,
            progress_done: Some(1),
            progress_total: Some(2),
            checkpoint: Some(&checkpoint),
        },
    )
    .await
    .expect_err("injected event failure must abort continuation");

    let unchanged_job = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load rolled-back job")
        .expect("job exists");
    assert_eq!(unchanged_job.status, JobStatus::Leased);
    assert_eq!(unchanged_job.run_number, 1);
    assert_eq!(unchanged_job.attempt, 1);
    assert!(unchanged_job.progress_done.is_none());
    assert!(unchanged_job.progress_total.is_none());
    assert!(unchanged_job.checkpoint.is_none());
    let unchanged_steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list rolled-back steps");
    assert_eq!(unchanged_steps[0].id, initial_step.id);
    assert_eq!(unchanged_steps[0].status, WorkflowStepStatus::Running);
    assert!(
        unchanged_steps[0].status_reason.is_none(),
        "continuation step update must roll back with event failure"
    );
    let attempt_finished_at = sqlx::query_scalar::<_, Option<chrono::DateTime<chrono::Utc>>>(
        "SELECT finished_at
             FROM job_attempts
             WHERE job_id = $1 AND run_number = 1 AND attempt = 1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("load rolled-back attempt");
    assert!(attempt_finished_at.is_none());
    assert!(
        list_job_events(&pool, None, job_id, 20, None)
            .await
            .expect("list rolled-back events")
            .iter()
            .all(|event| event.event_type != JobEventType::Requeued)
    );

    teardown_ephemeral_pool(pool, database).await;
}

async fn assert_continuing_prerequisite_releases_dependent_once(
    database_name: &str,
    release_on_success: bool,
) {
    let (pool, database) = setup_ephemeral_pool(database_name, 4).await;
    let prerequisite_job_type = "jobs.test.workflow_continuation.prerequisite";
    let dependent_job_type = "jobs.test.workflow_continuation.dependent";
    register_test_job_definition(&pool, prerequisite_job_type).await;
    register_test_job_definition(&pool, dependent_job_type).await;
    let prerequisite_payload = json!({"step": "a"});
    let dependent_payload = json!({"step": "b"});
    let metadata = json!({
        "test": "workflow-continuation-dependency",
        "release_on_success": release_on_success
    });
    let prerequisite = WorkflowStepEnqueueBuilder::new(
        StepKey::new("a"),
        JobType::new(prerequisite_job_type),
        &prerequisite_payload,
    )
    .allow_handler_continuation()
    .try_build()
    .expect("build continuing prerequisite");
    let dependent_builder = WorkflowStepEnqueueBuilder::new(
        StepKey::new("b"),
        JobType::new(dependent_job_type),
        &dependent_payload,
    );
    let dependent_builder = if release_on_success {
        dependent_builder.depends_on_success(&[StepKey::new("a")])
    } else {
        dependent_builder.depends_on_terminal(&[StepKey::new("a")])
    };
    let dependent = dependent_builder.try_build().expect("build dependent step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.continuation-dependency"),
        &metadata,
    )
    .step(prerequisite)
    .step(dependent)
    .try_build()
    .expect("build dependency workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue dependency workflow");
    let initial_steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list initial steps");
    let prerequisite_step = initial_steps
        .iter()
        .find(|step| step.step_key.as_str() == "a")
        .expect("prerequisite step exists");
    let prerequisite_step_id = prerequisite_step.id;
    let prerequisite_job_id = prerequisite_step
        .job_id
        .expect("prerequisite job should be released");
    let dependent_step = initial_steps
        .iter()
        .find(|step| step.step_key.as_str() == "b")
        .expect("dependent step exists");
    assert_eq!(dependent_step.status, WorkflowStepStatus::Blocked);
    assert_eq!(dependent_step.job_id, None);
    assert_eq!(dependent_step.dependency_count_pending, 1);

    for completed_run_number in 1..=3 {
        let claim = claim_one_job(
            &pool,
            &format!("worker-prerequisite-{completed_run_number}"),
        )
        .await;
        assert_eq!(claim.id, prerequisite_job_id);
        assert_eq!(claim.run_number, completed_run_number);
        let checkpoint = json!({"slice": completed_run_number});
        complete_job_continuation_with_outcome(
            &pool,
            claim.id,
            claim.run_number,
            claim.attempt,
            claim.worker_id.as_deref().expect("prerequisite worker id"),
            &JobContinuationUpdate {
                delay: Duration::ZERO,
                progress_done: Some(i64::from(completed_run_number)),
                progress_total: Some(4),
                checkpoint: Some(&checkpoint),
            },
        )
        .await
        .expect("continue prerequisite");

        let steps = list_workflow_steps(&pool, None, run.id)
            .await
            .expect("list steps after continuation");
        let continued_prerequisite = steps
            .iter()
            .find(|step| step.step_key.as_str() == "a")
            .expect("continued prerequisite exists");
        let blocked_dependent = steps
            .iter()
            .find(|step| step.step_key.as_str() == "b")
            .expect("blocked dependent exists");
        assert_eq!(continued_prerequisite.id, prerequisite_step_id);
        assert_eq!(continued_prerequisite.status, WorkflowStepStatus::Enqueued);
        assert_eq!(continued_prerequisite.job_id, Some(prerequisite_job_id));
        assert_eq!(blocked_dependent.status, WorkflowStepStatus::Blocked);
        assert_eq!(blocked_dependent.job_id, None);
        assert_eq!(blocked_dependent.dependency_count_pending, 1);
        assert_eq!(
            get_workflow_run_by_id(&pool, None, run.id)
                .await
                .expect("load active workflow")
                .expect("workflow exists")
                .status,
            runledger_core::jobs::WorkflowRunStatus::Running
        );
    }

    let final_claim = claim_one_job(&pool, "worker-prerequisite-final").await;
    assert_eq!(final_claim.id, prerequisite_job_id);
    assert_eq!(final_claim.run_number, 4);
    complete_job_success(
        &pool,
        final_claim.id,
        final_claim.run_number,
        final_claim.attempt,
        final_claim
            .worker_id
            .as_deref()
            .expect("final prerequisite worker id"),
        None,
    )
    .await
    .expect("complete prerequisite");

    let released_steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list released steps");
    let released_dependent = released_steps
        .iter()
        .find(|step| step.step_key.as_str() == "b")
        .expect("released dependent exists");
    assert_eq!(released_dependent.status, WorkflowStepStatus::Enqueued);
    assert_eq!(released_dependent.dependency_count_pending, 0);
    let dependent_job_id = released_dependent
        .job_id
        .expect("dependent job should be released exactly once");
    assert_eq!(
        list_job_events(&pool, None, dependent_job_id, 20, None)
            .await
            .expect("list dependent events")
            .iter()
            .filter(|event| event.event_type == JobEventType::Enqueued)
            .count(),
        1
    );

    let dependent_claim = claim_one_job(&pool, "worker-dependent").await;
    assert_eq!(dependent_claim.id, dependent_job_id);
    complete_job_success(
        &pool,
        dependent_claim.id,
        dependent_claim.run_number,
        dependent_claim.attempt,
        dependent_claim
            .worker_id
            .as_deref()
            .expect("dependent worker id"),
        None,
    )
    .await
    .expect("complete dependent");
    assert_eq!(
        get_workflow_run_by_id(&pool, None, run.id)
            .await
            .expect("load terminal workflow")
            .expect("workflow exists")
            .status,
        runledger_core::jobs::WorkflowRunStatus::Succeeded
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn continuing_prerequisite_keeps_on_success_dependent_blocked_until_final_success() {
    assert_continuing_prerequisite_releases_dependent_once(
        "postgres_workflow_continuation_on_success",
        true,
    )
    .await;
}

#[tokio::test]
async fn continuing_prerequisite_keeps_on_terminal_dependent_blocked_until_final_success() {
    assert_continuing_prerequisite_releases_dependent_once(
        "postgres_workflow_continuation_on_terminal",
        false,
    )
    .await;
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
