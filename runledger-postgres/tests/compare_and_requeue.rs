use runledger_core::jobs::{
    JobEventType, JobFailureKind, JobStatus, JobType, StepKey, WorkflowRunEnqueueBuilder,
    WorkflowStepEnqueueBuilder, WorkflowType,
};
use runledger_postgres::jobs::{
    CompareAndRequeueJob, CompareAndRequeueJobOutcome, JobEnqueue, JobEnqueueDisposition,
    JobFailureUpdate, JobOrdinaryProgressUpdate, JobRequeueStatePolicy, JobScope,
    RequeueableJobStatus, cancel_job, compare_and_requeue_job, compare_and_requeue_job_tx,
    complete_job_failure, enqueue_job_with_outcome_tx, enqueue_workflow_run, get_job_by_id,
    heartbeat_job, list_job_events, list_workflow_steps, update_job_ordinary_progress,
};
use runledger_postgres::{DbPool, Error};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::{Value, json};
use sqlx::types::Uuid;
use std::time::Duration;
use tokio::time::timeout;

mod support;

use support::{claim_one_job, enqueue_test_job, register_test_job_definition};

const JOB_TYPE: &str = "jobs.test.compare_and_requeue";

async fn enqueue_scoped_job(pool: &DbPool, organization_id: Option<Uuid>, key: &str) -> Uuid {
    let payload = json!({"key": key});
    enqueue_test_job(pool, JOB_TYPE, organization_id, &payload).await
}

async fn event_count(pool: &DbPool, job_id: Uuid) -> usize {
    list_job_events(pool, None, job_id, 20, None)
        .await
        .expect("list job events")
        .len()
}

async fn wait_until_backend_is_blocked_by(
    pool: &DbPool,
    blocked_pid: i32,
    blocker_pid: i32,
) -> Result<(), sqlx::Error> {
    let mut poll = tokio::time::interval(Duration::from_millis(10));
    loop {
        poll.tick().await;
        let blocking_pids = sqlx::query_scalar::<_, Vec<i32>>("SELECT pg_blocking_pids($1)")
            .bind(blocked_pid)
            .fetch_one(pool)
            .await?;
        if blocking_pids.contains(&blocker_pid) {
            return Ok(());
        }
    }
}

async fn assert_job_row_is_not_locked(pool: &DbPool, job_id: Uuid, context: &str) {
    let mut probe_tx = pool.begin().await.expect("begin row-lock probe");
    sqlx::query("SET LOCAL lock_timeout = '500ms'")
        .execute(&mut *probe_tx)
        .await
        .expect("set row-lock probe timeout");
    timeout(
        Duration::from_secs(1),
        sqlx::query_scalar::<_, Uuid>(
            "SELECT id
             FROM job_queue
             WHERE id = $1
             FOR UPDATE",
        )
        .bind(job_id)
        .fetch_one(&mut *probe_tx),
    )
    .await
    .unwrap_or_else(|_| panic!("{context}: row-lock probe timed out"))
    .unwrap_or_else(|error| panic!("{context}: row remained locked: {error}"));
    probe_tx.rollback().await.expect("rollback row-lock probe");
}

#[tokio::test]
async fn observed_request_preserves_exact_identity_and_rejects_nonrequeueable_statuses() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_compare_requeue_observed_request", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let organization_id = Uuid::from_u128(41);

    let pending_job_id = enqueue_scoped_job(&pool, None, "pending-observation").await;
    let mut unsupported_observation = get_job_by_id(&pool, None, pending_job_id)
        .await
        .expect("load pending job")
        .expect("pending job exists");
    for status in [JobStatus::Pending, JobStatus::Leased, JobStatus::Succeeded] {
        unsupported_observation.status = status;
        let error = CompareAndRequeueJob::from_observed_job(
            &unsupported_observation,
            JobRequeueStatePolicy::ResetProgressAndCheckpoint,
            "must reject unsupported status",
        )
        .expect_err("non-requeueable observations must not seed recovery");
        assert_eq!(error.status(), status);
    }

    let global_job_id = enqueue_scoped_job(&pool, None, "global-observation").await;
    let global = cancel_job(&pool, None, global_job_id, Some("cancel global"))
        .await
        .expect("cancel global job");
    let global_request = CompareAndRequeueJob::from_observed_job(
        &global,
        JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
        "recover global observation",
    )
    .expect("canceled global job is requeueable");
    assert_eq!(global_request.scope, JobScope::Global);
    assert_eq!(global_request.job_id, global.id);
    assert_eq!(
        global_request.expected_status,
        RequeueableJobStatus::Canceled
    );
    assert_eq!(global_request.expected_run_number, global.run_number);
    assert_eq!(
        global_request.state_policy,
        JobRequeueStatePolicy::PreserveProgressAndCheckpoint
    );
    assert_eq!(global_request.reason, "recover global observation");

    let mut dead_lettered_observation = global.clone();
    dead_lettered_observation.status = JobStatus::DeadLettered;
    let dead_lettered_request = CompareAndRequeueJob::from_observed_job(
        &dead_lettered_observation,
        JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
        "recover dead-lettered observation",
    )
    .expect("dead-lettered jobs are requeueable");
    assert_eq!(
        dead_lettered_request.expected_status,
        RequeueableJobStatus::DeadLettered
    );

    let organization_job_id =
        enqueue_scoped_job(&pool, Some(organization_id), "organization-observation").await;
    let organization = cancel_job(
        &pool,
        Some(organization_id),
        organization_job_id,
        Some("cancel organization job"),
    )
    .await
    .expect("cancel organization job");
    let organization_request = CompareAndRequeueJob::from_observed_job(
        &organization,
        JobRequeueStatePolicy::ResetProgressAndCheckpoint,
        "recover organization observation",
    )
    .expect("canceled organization job is requeueable");
    assert_eq!(
        organization_request.scope,
        JobScope::Organization(organization_id)
    );
    assert_eq!(organization_request.job_id, organization.id);
    assert_eq!(
        organization_request.expected_status,
        RequeueableJobStatus::Canceled
    );
    assert_eq!(
        organization_request.expected_run_number,
        organization.run_number
    );
    assert_eq!(
        organization_request.state_policy,
        JobRequeueStatePolicy::ResetProgressAndCheckpoint
    );
    assert_eq!(
        organization_request.reason,
        "recover organization observation"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn pool_owned_compare_and_requeue_sets_read_committed_and_commits_job_and_event() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_compare_requeue_owned_transaction", 1).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let organization_id = Uuid::from_u128(42);
    let job_id = enqueue_scoped_job(&pool, Some(organization_id), "owned-transaction").await;
    let canceled = cancel_job(
        &pool,
        Some(organization_id),
        job_id,
        Some("cancel before owned recovery"),
    )
    .await
    .expect("cancel job");
    let initial_event_count = event_count(&pool, job_id).await;

    let mut connection = pool.acquire().await.expect("acquire sole pool connection");
    sqlx::query("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *connection)
        .await
        .expect("set a non-default session transaction isolation");
    let session_isolation: String = sqlx::query_scalar("SHOW transaction_isolation")
        .fetch_one(&mut *connection)
        .await
        .expect("read session transaction isolation");
    assert_eq!(session_isolation, "repeatable read");
    drop(connection);

    let request = CompareAndRequeueJob::from_observed_job(
        &canceled,
        JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
        "owned recovery",
    )
    .expect("canceled observation is requeueable");
    let outcome = compare_and_requeue_job(&pool, request)
        .await
        .expect("owned recovery should establish READ COMMITTED");
    let CompareAndRequeueJobOutcome::Requeued {
        before,
        after,
        event_id,
    } = outcome
    else {
        panic!("expected requeued outcome");
    };
    assert_eq!(before.status, JobStatus::Canceled);
    assert_eq!(before.run_number, 1);
    assert_eq!(after.status, JobStatus::Pending);
    assert_eq!(after.run_number, 2);
    assert_eq!(after.organization_id, Some(organization_id));

    let persisted = get_job_by_id(&pool, Some(organization_id), job_id)
        .await
        .expect("load recovered job")
        .expect("recovered job exists");
    assert_eq!(persisted.status, JobStatus::Pending);
    assert_eq!(persisted.run_number, 2);
    assert_eq!(persisted.status_reason.as_deref(), Some("owned recovery"));

    let events = list_job_events(&pool, Some(organization_id), job_id, 20, None)
        .await
        .expect("list committed recovery events");
    assert_eq!(events.len(), initial_event_count + 1);
    let event = events.last().expect("committed requeue event");
    assert_eq!(event.id, event_id);
    assert_eq!(event.event_type, JobEventType::Requeued);
    assert_eq!(event.run_number, 1);
    assert_eq!(
        event.payload.get("reason").and_then(Value::as_str),
        Some("owned recovery")
    );
    assert_eq!(
        event.payload.get("state_policy").and_then(Value::as_str),
        Some("preserve_progress_and_checkpoint")
    );
    assert_eq!(
        event.payload.get("requeue_kind").and_then(Value::as_str),
        Some("COMPARE_AND_REQUEUE")
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn compare_and_requeue_uses_exact_scope_expectations_and_caller_transaction() {
    let (pool, database) = setup_ephemeral_pool("postgres_compare_and_requeue", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let organization_id = Uuid::from_u128(1);
    let other_organization_id = Uuid::from_u128(2);
    let job_id = enqueue_scoped_job(&pool, Some(organization_id), "organization-job").await;
    cancel_job(
        &pool,
        Some(organization_id),
        job_id,
        Some("initial cancellation"),
    )
    .await
    .expect("cancel job");
    let initial_event_count = event_count(&pool, job_id).await;

    for scope in [
        JobScope::Global,
        JobScope::Organization(other_organization_id),
    ] {
        let mut tx = pool.begin().await.expect("begin wrong-scope transaction");
        let outcome = compare_and_requeue_job_tx(
            &mut tx,
            CompareAndRequeueJob {
                scope,
                job_id,
                expected_status: RequeueableJobStatus::Canceled,
                expected_run_number: 1,
                state_policy: JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
                reason: "wrong scope must not mutate",
            },
        )
        .await
        .expect("wrong scope is a normal outcome");
        assert!(matches!(outcome, CompareAndRequeueJobOutcome::NotFound));
        tx.commit().await.expect("commit no-op transaction");
    }

    for (expected_status, expected_run_number) in [
        (RequeueableJobStatus::DeadLettered, 1),
        (RequeueableJobStatus::Canceled, 2),
    ] {
        let mut tx = pool.begin().await.expect("begin mismatch transaction");
        let outcome = compare_and_requeue_job_tx(
            &mut tx,
            CompareAndRequeueJob {
                scope: JobScope::Organization(organization_id),
                job_id,
                expected_status,
                expected_run_number,
                state_policy: JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
                reason: "stale expectation must not mutate",
            },
        )
        .await
        .expect("mismatch is a normal outcome");
        let CompareAndRequeueJobOutcome::ExpectationMismatch { actual } = outcome else {
            panic!("expected mismatch outcome");
        };
        assert_eq!(actual.status, JobStatus::Canceled);
        assert_eq!(actual.run_number, 1);
        tx.commit().await.expect("commit mismatch transaction");
    }
    assert_eq!(event_count(&pool, job_id).await, initial_event_count);

    let mut rollback_tx = pool.begin().await.expect("begin rollback transaction");
    let rollback_outcome = compare_and_requeue_job_tx(
        &mut rollback_tx,
        CompareAndRequeueJob {
            scope: JobScope::Organization(organization_id),
            job_id,
            expected_status: RequeueableJobStatus::Canceled,
            expected_run_number: 1,
            state_policy: JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
            reason: "rolled back recovery",
        },
    )
    .await
    .expect("matching compare-and-requeue");
    assert!(matches!(
        rollback_outcome,
        CompareAndRequeueJobOutcome::Requeued { .. }
    ));
    rollback_tx
        .rollback()
        .await
        .expect("rollback caller transaction");
    let still_canceled = get_job_by_id(&pool, Some(organization_id), job_id)
        .await
        .expect("load rolled-back job")
        .expect("job exists");
    assert_eq!(still_canceled.status, JobStatus::Canceled);
    assert_eq!(still_canceled.run_number, 1);
    assert_eq!(event_count(&pool, job_id).await, initial_event_count);

    let mut commit_tx = pool.begin().await.expect("begin commit transaction");
    let outcome = compare_and_requeue_job_tx(
        &mut commit_tx,
        CompareAndRequeueJob {
            scope: JobScope::Organization(organization_id),
            job_id,
            expected_status: RequeueableJobStatus::Canceled,
            expected_run_number: 1,
            state_policy: JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
            reason: "operator recovery",
        },
    )
    .await
    .expect("matching compare-and-requeue");
    let CompareAndRequeueJobOutcome::Requeued {
        before,
        after,
        event_id,
    } = outcome
    else {
        panic!("expected requeued outcome");
    };
    assert_eq!(before.status, JobStatus::Canceled);
    assert_eq!(before.run_number, 1);
    assert_eq!(after.status, JobStatus::Pending);
    assert_eq!(after.run_number, 2);
    assert_eq!(after.attempt, 0);
    assert_eq!(after.organization_id, Some(organization_id));
    assert_eq!(after.status_reason.as_deref(), Some("operator recovery"));
    commit_tx.commit().await.expect("commit recovery");

    let events = list_job_events(&pool, Some(organization_id), job_id, 20, None)
        .await
        .expect("list committed events");
    assert_eq!(events.len(), initial_event_count + 1);
    let event = events.last().expect("requeued event");
    assert_eq!(event.id, event_id);
    assert_eq!(event.event_type, JobEventType::Requeued);
    assert_eq!(event.run_number, 1);
    assert_eq!(
        event.payload.get("reason").and_then(|value| value.as_str()),
        Some("operator recovery")
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn global_scope_matches_only_global_jobs() {
    let (pool, database) = setup_ephemeral_pool("postgres_compare_requeue_global", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let job_id = enqueue_scoped_job(&pool, None, "global-job").await;
    cancel_job(&pool, None, job_id, Some("cancel global"))
        .await
        .expect("cancel global job");

    let mut tx = pool.begin().await.expect("begin global recovery");
    let outcome = compare_and_requeue_job_tx(
        &mut tx,
        CompareAndRequeueJob {
            scope: JobScope::Global,
            job_id,
            expected_status: RequeueableJobStatus::Canceled,
            expected_run_number: 1,
            state_policy: JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
            reason: "recover global",
        },
    )
    .await
    .expect("recover global job");
    assert!(matches!(
        outcome,
        CompareAndRequeueJobOutcome::Requeued { .. }
    ));
    tx.commit().await.expect("commit global recovery");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn keyed_enqueue_composes_with_compare_and_requeue_without_lock_upgrade_deadlock() {
    const IDEMPOTENCY_KEY: &str = "keyed-enqueue-recovery";
    const RECOVERY_REASON: &str = "second transaction recovery";

    let (pool, database) = setup_ephemeral_pool("postgres_keyed_enqueue_recovery_lock", 6).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let payload = json!({"key": "keyed-enqueue-recovery"});

    let mut seed_tx = pool.begin().await.expect("begin keyed enqueue seed");
    let seeded = enqueue_job_with_outcome_tx(
        &mut seed_tx,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: None,
            payload: &payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: Some(IDEMPOTENCY_KEY),
            stage: None,
        },
    )
    .await
    .expect("seed keyed job");
    assert_eq!(seeded.disposition, JobEnqueueDisposition::Inserted);
    seed_tx.commit().await.expect("commit keyed enqueue seed");

    let canceled = cancel_job(&pool, None, seeded.job_id, Some("prepare recovery race"))
        .await
        .expect("cancel pending keyed job");
    assert_eq!(canceled.status, JobStatus::Canceled);
    assert_eq!(canceled.run_number, 1);
    assert!(
        canceled.lease_expires_at.is_none(),
        "a pending cancellation must not retain a live lease marker"
    );
    let canceled_event = list_job_events(&pool, None, seeded.job_id, 20, None)
        .await
        .expect("list pending cancellation events")
        .into_iter()
        .find(|event| event.event_type == JobEventType::Canceled)
        .expect("pending cancellation event should exist");
    assert_eq!(
        canceled_event.payload,
        json!({"reason": "prepare recovery race"})
    );

    let mut tx_a = pool.begin().await.expect("begin transaction A");
    let pid_a = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&mut *tx_a)
        .await
        .expect("load transaction A backend pid");
    let existing_a = enqueue_job_with_outcome_tx(
        &mut tx_a,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: None,
            payload: &payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: Some(IDEMPOTENCY_KEY),
            stage: None,
        },
    )
    .await
    .expect("transaction A loads existing keyed job");
    assert_eq!(existing_a.disposition, JobEnqueueDisposition::Existing);
    assert_eq!(existing_a.status, JobStatus::Canceled);
    assert_eq!(existing_a.run_number, 1);

    let (pid_b_sender, pid_b_receiver) = tokio::sync::oneshot::channel();
    let pool_b = pool.clone();
    let payload_b = payload.clone();
    let job_id = seeded.job_id;
    let task_b = tokio::spawn(async move {
        let mut tx_b = pool_b
            .begin()
            .await
            .map_err(|error| format!("begin transaction B: {error}"))?;
        let pid_b = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
            .fetch_one(&mut *tx_b)
            .await
            .map_err(|error| format!("load transaction B backend pid: {error}"))?;
        pid_b_sender
            .send(pid_b)
            .map_err(|_| String::from("publish transaction B backend pid"))?;

        let existing_b = enqueue_job_with_outcome_tx(
            &mut tx_b,
            &JobEnqueue {
                job_type: JobType::new(JOB_TYPE),
                organization_id: None,
                payload: &payload_b,
                priority: None,
                max_attempts: None,
                timeout_seconds: None,
                next_run_at: None,
                idempotency_key: Some(IDEMPOTENCY_KEY),
                stage: None,
            },
        )
        .await
        .map_err(|error| format!("transaction B keyed enqueue: {error}"))?;
        if existing_b.disposition != JobEnqueueDisposition::Existing
            || existing_b.status != JobStatus::Canceled
            || existing_b.run_number != 1
        {
            return Err(format!(
                "transaction B saw unexpected keyed enqueue outcome: {existing_b:?}"
            ));
        }

        let outcome = compare_and_requeue_job_tx(
            &mut tx_b,
            CompareAndRequeueJob {
                scope: JobScope::Global,
                job_id,
                expected_status: RequeueableJobStatus::Canceled,
                expected_run_number: 1,
                state_policy: JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
                reason: RECOVERY_REASON,
            },
        )
        .await
        .map_err(|error| format!("transaction B compare-and-requeue: {error}"))?;
        let CompareAndRequeueJobOutcome::Requeued { before, after, .. } = outcome else {
            return Err(format!(
                "transaction B expected Requeued outcome, got {outcome:?}"
            ));
        };
        if before.organization_id.is_some()
            || before.status != JobStatus::Canceled
            || before.run_number != 1
            || after.status != JobStatus::Pending
            || after.run_number != 2
        {
            return Err(format!(
                "transaction B saw unexpected recovery transition: before={before:?}, after={after:?}"
            ));
        }
        tx_b.commit()
            .await
            .map_err(|error| format!("commit transaction B: {error}"))?;
        Ok::<(), String>(())
    });

    let pid_b = timeout(Duration::from_secs(2), pid_b_receiver)
        .await
        .expect("transaction B must publish its backend pid")
        .expect("transaction B backend pid sender must remain alive");

    // With FOR NO KEY UPDATE, B waits during its keyed lookup. The previous
    // FOR SHARE behavior let B reach compare-and-requeue while A retained its
    // shared lock; A's matching mutation then formed a lock-upgrade cycle and
    // PostgreSQL aborted one transaction with SQLSTATE 40P01.
    timeout(
        Duration::from_secs(2),
        wait_until_backend_is_blocked_by(&pool, pid_b, pid_a),
    )
    .await
    .expect("transaction B must become blocked by transaction A")
    .expect("inspect PostgreSQL blocker graph");

    let outcome_a = timeout(
        Duration::from_secs(5),
        compare_and_requeue_job_tx(
            &mut tx_a,
            CompareAndRequeueJob {
                scope: JobScope::Global,
                job_id: seeded.job_id,
                expected_status: RequeueableJobStatus::Canceled,
                expected_run_number: 1,
                state_policy: JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
                reason: "transaction A rolled-back recovery",
            },
        ),
    )
    .await
    .expect("transaction A compare-and-requeue must not hang")
    .expect("transaction A requeues without a lock-upgrade deadlock");
    assert!(matches!(
        outcome_a,
        CompareAndRequeueJobOutcome::Requeued { .. }
    ));
    tx_a.rollback()
        .await
        .expect("roll back transaction A recovery");

    timeout(Duration::from_secs(5), task_b)
        .await
        .expect("transaction B must finish after transaction A rolls back")
        .expect("transaction B task must not panic")
        .expect("transaction B must serialize and commit recovery");

    let final_job = get_job_by_id(&pool, None, seeded.job_id)
        .await
        .expect("load final keyed job")
        .expect("final keyed job exists");
    assert_eq!(final_job.status, JobStatus::Pending);
    assert_eq!(final_job.run_number, 2);

    let requeued_events = list_job_events(&pool, None, seeded.job_id, 20, None)
        .await
        .expect("list keyed recovery events")
        .into_iter()
        .filter(|event| event.event_type == JobEventType::Requeued)
        .collect::<Vec<_>>();
    assert_eq!(requeued_events.len(), 1);
    assert_eq!(requeued_events[0].run_number, 1);
    assert_eq!(
        requeued_events[0]
            .payload
            .get("reason")
            .and_then(|value| value.as_str()),
        Some(RECOVERY_REASON)
    );
    assert_eq!(
        requeued_events[0]
            .payload
            .get("state_policy")
            .and_then(|value| value.as_str()),
        Some("preserve_progress_and_checkpoint")
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn live_expectation_mismatch_does_not_block_worker_heartbeat() {
    let (pool, database) = setup_ephemeral_pool("postgres_compare_requeue_live_mismatch", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let job_id = enqueue_scoped_job(&pool, None, "live-mismatch").await;
    let claim = claim_one_job(&pool, "worker-live-mismatch").await;
    let worker_id = claim.worker_id.as_deref().expect("claimed worker id");

    let mut recovery_tx = pool.begin().await.expect("begin mismatch transaction");
    let outcome = compare_and_requeue_job_tx(
        &mut recovery_tx,
        CompareAndRequeueJob {
            scope: JobScope::Global,
            job_id,
            expected_status: RequeueableJobStatus::Canceled,
            expected_run_number: claim.run_number,
            state_policy: JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
            reason: "live row must remain unlocked",
        },
    )
    .await
    .expect("live status mismatch should be a normal outcome");
    assert!(matches!(
        outcome,
        CompareAndRequeueJobOutcome::ExpectationMismatch { .. }
    ));

    timeout(
        Duration::from_secs(1),
        heartbeat_job(
            &pool,
            claim.id,
            claim.run_number,
            claim.attempt,
            worker_id,
            30,
        ),
    )
    .await
    .expect("mismatch transaction must not block the healthy worker heartbeat")
    .expect("healthy worker heartbeat should succeed");
    recovery_tx
        .rollback()
        .await
        .expect("rollback mismatch transaction");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn compare_and_requeue_rejects_non_read_committed_transactions_up_front() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_compare_requeue_isolation_guard", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let job_id = enqueue_scoped_job(&pool, None, "isolation-guard").await;
    cancel_job(&pool, None, job_id, Some("cancel before isolation test"))
        .await
        .expect("cancel job");

    let mut tx = pool.begin().await.expect("begin recovery transaction");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await
        .expect("set repeatable-read isolation");
    let error = compare_and_requeue_job_tx(
        &mut tx,
        CompareAndRequeueJob {
            scope: JobScope::Global,
            job_id,
            expected_status: RequeueableJobStatus::Canceled,
            expected_run_number: 1,
            state_policy: JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
            reason: "unsupported isolation must be explicit",
        },
    )
    .await
    .expect_err("repeatable-read recovery must be rejected deterministically");
    let Error::QueryError(error) = error else {
        panic!("expected isolation validation error");
    };
    assert_eq!(
        error.code(),
        "job.compare_and_requeue_unsupported_isolation"
    );
    tx.rollback().await.expect("rollback isolation test");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn canceled_live_job_waits_for_its_original_lease_window_before_requeue() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_compare_requeue_cancel_quiescence", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let job_id = enqueue_scoped_job(&pool, None, "cancel-quiescence").await;
    let claim = claim_one_job(&pool, "worker-cancel-quiescence").await;
    let original_lease_expiry = claim
        .lease_expires_at
        .expect("claimed job should have a lease expiry");
    let canceled = cancel_job(&pool, None, job_id, Some("cancel live handler"))
        .await
        .expect("cancel live job");
    assert_eq!(canceled.status, JobStatus::Canceled);
    assert_eq!(canceled.lease_expires_at, Some(original_lease_expiry));
    assert!(canceled.worker_id.is_none());
    let canceled_event = list_job_events(&pool, None, job_id, 20, None)
        .await
        .expect("list leased cancellation events")
        .into_iter()
        .find(|event| event.event_type == JobEventType::Canceled)
        .expect("leased cancellation event should exist");
    let canceled_payload = canceled_event
        .payload
        .as_object()
        .expect("cancellation event payload should be an object");
    assert_eq!(canceled_payload.len(), 2);
    assert_eq!(
        canceled_payload.get("reason"),
        Some(&json!("cancel live handler"))
    );
    assert!(
        canceled_payload
            .get("lease_quiesces_at")
            .is_some_and(Value::is_string)
    );

    let mut blocked_tx = pool.begin().await.expect("begin early recovery");
    let outcome = compare_and_requeue_job_tx(
        &mut blocked_tx,
        CompareAndRequeueJob {
            scope: JobScope::Global,
            job_id,
            expected_status: RequeueableJobStatus::Canceled,
            expected_run_number: claim.run_number,
            state_policy: JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
            reason: "must wait for canceled handler quiescence",
        },
    )
    .await
    .expect("early recovery should be a normal no-mutation outcome");
    let CompareAndRequeueJobOutcome::CancellationNotQuiesced {
        actual,
        retry_after,
    } = outcome
    else {
        panic!("expected cancellation quiescence outcome");
    };
    assert_eq!(actual.status, JobStatus::Canceled);
    assert_eq!(retry_after, original_lease_expiry);
    assert_job_row_is_not_locked(&pool, job_id, "cancellation quiescence no-mutation outcome")
        .await;
    blocked_tx
        .rollback()
        .await
        .expect("rollback early recovery");

    sqlx::query(
        "UPDATE job_queue
         SET lease_expires_at = clock_timestamp() - interval '1 microsecond'
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .expect("advance cancellation quiescence marker for test");

    let mut recovery_tx = pool.begin().await.expect("begin quiesced recovery");
    let outcome = compare_and_requeue_job_tx(
        &mut recovery_tx,
        CompareAndRequeueJob {
            scope: JobScope::Global,
            job_id,
            expected_status: RequeueableJobStatus::Canceled,
            expected_run_number: claim.run_number,
            state_policy: JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
            reason: "recover after canceled handler quiescence",
        },
    )
    .await
    .expect("quiesced canceled job should be recoverable");
    let CompareAndRequeueJobOutcome::Requeued { after, .. } = outcome else {
        panic!("expected quiesced canceled job to requeue");
    };
    assert_eq!(after.status, JobStatus::Pending);
    assert_eq!(after.run_number, claim.run_number + 1);
    assert!(after.lease_expires_at.is_none());
    recovery_tx
        .commit()
        .await
        .expect("commit quiesced recovery");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn workflow_managed_job_is_rejected_from_the_locked_candidate() {
    let (pool, database) = setup_ephemeral_pool("postgres_compare_requeue_workflow", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let payload = json!({"key": "workflow-job"});
    let metadata = json!({"test": "compare-and-requeue-workflow"});
    let step =
        WorkflowStepEnqueueBuilder::new(StepKey::new("step"), JobType::new(JOB_TYPE), &payload)
            .try_build()
            .expect("build workflow step");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.compare-and-requeue"),
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
    cancel_job(&pool, None, job_id, Some("cancel workflow step job"))
        .await
        .expect("cancel workflow-managed job");
    let before = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load canceled workflow job")
        .expect("workflow job exists");
    let initial_event_count = event_count(&pool, job_id).await;

    let mut tx = pool.begin().await.expect("begin workflow recovery");
    let error = compare_and_requeue_job_tx(
        &mut tx,
        CompareAndRequeueJob {
            scope: JobScope::Global,
            job_id,
            expected_status: RequeueableJobStatus::Canceled,
            expected_run_number: before.run_number,
            state_policy: JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
            reason: "workflow recovery must be rejected",
        },
    )
    .await
    .expect_err("workflow-managed job must not be requeued directly");
    let Error::QueryError(error) = error else {
        panic!("expected workflow requeue validation error");
    };
    assert_eq!(error.code(), "job.workflow_requeue_not_supported");
    assert_eq!(
        error.client_message(),
        "Workflow-managed jobs cannot be requeued directly."
    );
    assert_job_row_is_not_locked(&pool, job_id, "workflow rejection").await;
    tx.rollback().await.expect("rollback rejected recovery");

    let after = get_job_by_id(&pool, None, job_id)
        .await
        .expect("reload workflow job")
        .expect("workflow job exists");
    assert_eq!(after.status, before.status);
    assert_eq!(after.run_number, before.run_number);
    assert_eq!(event_count(&pool, job_id).await, initial_event_count);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn dead_lettered_status_is_requeueable_with_an_exact_run_match() {
    let (pool, database) = setup_ephemeral_pool("postgres_compare_requeue_dead_letter", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let job_id = enqueue_scoped_job(&pool, None, "dead-lettered-job").await;
    let claim = claim_one_job(&pool, "worker-dead-letter").await;
    let worker_id = claim.worker_id.as_deref().expect("claimed worker id");
    let checkpoint = json!({"cursor": 900});
    update_job_ordinary_progress(
        &pool,
        claim.id,
        claim.run_number,
        claim.attempt,
        worker_id,
        &JobOrdinaryProgressUpdate {
            progress_done: Some(900),
            progress_total: Some(1_000),
            checkpoint: Some(&checkpoint),
        },
    )
    .await
    .expect("persist resumable progress before dead letter");
    complete_job_failure(
        &pool,
        claim.id,
        claim.run_number,
        claim.attempt,
        worker_id,
        &JobFailureUpdate::new(
            JobFailureKind::Terminal,
            "job.test.dead_letter",
            "dead letter before recovery",
            None,
        ),
    )
    .await
    .expect("dead-letter job");

    let mut tx = pool.begin().await.expect("begin dead-letter recovery");
    let outcome = compare_and_requeue_job_tx(
        &mut tx,
        CompareAndRequeueJob {
            scope: JobScope::Global,
            job_id,
            expected_status: RequeueableJobStatus::DeadLettered,
            expected_run_number: 1,
            state_policy: JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
            reason: "repair dead letter",
        },
    )
    .await
    .expect("requeue exact dead letter");
    let CompareAndRequeueJobOutcome::Requeued { before, after, .. } = outcome else {
        panic!("expected dead letter to be requeued");
    };
    assert_eq!(before.status, JobStatus::DeadLettered);
    assert_eq!(after.status, JobStatus::Pending);
    assert_eq!(after.run_number, 2);
    assert_eq!(after.progress_done, Some(900));
    assert_eq!(after.progress_total, Some(1_000));
    assert_eq!(after.checkpoint, Some(checkpoint.clone()));
    tx.commit().await.expect("commit dead-letter recovery");

    let second_claim = claim_one_job(&pool, "worker-dead-letter-reset").await;
    assert_eq!(second_claim.run_number, 2);
    assert_eq!(second_claim.progress_done, Some(900));
    assert_eq!(second_claim.checkpoint, Some(checkpoint));
    complete_job_failure(
        &pool,
        second_claim.id,
        second_claim.run_number,
        second_claim.attempt,
        second_claim
            .worker_id
            .as_deref()
            .expect("second claimed worker id"),
        &JobFailureUpdate::new(
            JobFailureKind::Terminal,
            "job.test.dead_letter_again",
            "dead letter before reset recovery",
            None,
        ),
    )
    .await
    .expect("dead-letter preserved recovery run");

    let mut reset_tx = pool.begin().await.expect("begin reset recovery");
    let reset_outcome = compare_and_requeue_job_tx(
        &mut reset_tx,
        CompareAndRequeueJob {
            scope: JobScope::Global,
            job_id,
            expected_status: RequeueableJobStatus::DeadLettered,
            expected_run_number: 2,
            state_policy: JobRequeueStatePolicy::ResetProgressAndCheckpoint,
            reason: "restart dead letter from scratch",
        },
    )
    .await
    .expect("reset exact dead letter");
    let CompareAndRequeueJobOutcome::Requeued { after, .. } = reset_outcome else {
        panic!("expected reset recovery to requeue");
    };
    assert_eq!(after.run_number, 3);
    assert_eq!(after.progress_done, None);
    assert_eq!(after.progress_total, None);
    assert_eq!(after.checkpoint, None);
    reset_tx.commit().await.expect("commit reset recovery");

    let requeue_events = list_job_events(&pool, None, job_id, 50, None)
        .await
        .expect("list recovery policy events")
        .into_iter()
        .filter(|event| event.event_type == JobEventType::Requeued)
        .collect::<Vec<_>>();
    assert_eq!(requeue_events.len(), 2);
    assert_eq!(
        requeue_events[0]
            .payload
            .get("state_policy")
            .and_then(|value| value.as_str()),
        Some("preserve_progress_and_checkpoint")
    );
    assert_eq!(
        requeue_events[1]
            .payload
            .get("state_policy")
            .and_then(|value| value.as_str()),
        Some("reset_progress_and_checkpoint")
    );

    teardown_ephemeral_pool(pool, database).await;
}
