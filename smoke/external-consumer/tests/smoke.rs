use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex as StdMutex};
use std::task::{Context, Poll};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use runledger_core::jobs::{
    JobCompletion, JobContext, JobDeadLetterInfo, JobEventType, JobFailure, JobStage, JobStatus,
    JobType,
};
use runledger_postgres::jobs::{
    self, CompareAndReplaySucceededJob, CompareAndReplaySucceededJobOutcome, CompareAndRequeueJob,
    CompareAndRequeueJobOutcome, JobCancellationScope, JobDefinitionUpsert, JobEnqueue,
    JobEnqueueDisposition, JobEnqueueIntent, JobEnqueueIntentStatus, JobListFilter, JobQueueRecord,
    JobRequeueStatePolicy, JobScope, cancel_job_with_scope, compare_and_replay_succeeded_job,
    compare_and_replay_succeeded_job_tx, compare_and_requeue_job, compare_and_requeue_job_tx,
    enqueue_job_with_outcome_in_transaction, get_job_by_id, get_job_continuation_metrics,
    get_job_enqueue_intent_by_id, record_job_enqueue_intent_tx, upsert_job_definition_tx,
};
use runledger_postgres::prelude::{
    DbPool, DecodedJobEventPayload, DecodedRequeuedEventPayload, JobEventRecord,
    PgTransactionExecutor, enqueue_job_with_outcome, list_job_events,
};
use runledger_runtime::catalog::JobCatalog;
use runledger_runtime::config::JobsConfig;
use runledger_runtime::registry::JobHandler;
use runledger_runtime::{
    RuntimeCallbackFailure, RuntimeSettlement, RuntimeShutdownBudget, RuntimeShutdownCause,
    RuntimeShutdownFailure, RuntimeShutdownReport, RuntimeShutdownSettlement,
    RuntimeShutdownSignal, Supervisor,
};
use runledger_test_support::{setup_unmigrated_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::{Value, json};
use sqlx::types::Uuid;
use tokio::sync::{Mutex, Notify, watch};
use tokio::time::{Instant, sleep, timeout};

#[path = "support/migration_identity.rs"]
mod migration_identity;

#[path = "support/opaque_intents.rs"]
mod opaque_intents;

#[path = "support/shutdown_signal.rs"]
mod shutdown_signal;

const SMOKE_JOB_TYPE: &str = "jobs.external.smoke";
const SMOKE_POOL_MAX_CONNECTIONS: u32 = 12;
const CONTINUATION_CHECKPOINT_VERSION: i64 = 1;
const CONTINUATION_MAX_RUNS: i64 = 2;
const HANDLER_CONTINUATION_REASON: &str = "HANDLER_CONTINUATION";
const HANDLER_RETRY_AFTER: Duration = Duration::from_millis(25);
const HANDLER_RETRY_AFTER_MS: i64 = 25;
const SMOKE_RETRY_POLICY_DELAY_MS: i32 = 1;
const RETRY_AFTER_FAILURE_CODE: &str = "smoke.provider_temporarily_unavailable";
const RETRY_AT_FAILURE_CODE: &str = "smoke.provider_rate_limited";
const REPLAY_REQUEST_KEY: &str = "external-smoke-success-replay";
const REPLAY_REASON: &str = "prove fresh successful replay from a packaged consumer";

struct SmokeRuntime {
    hang_release: Arc<Notify>,
    dead_letters: Arc<Mutex<Vec<String>>>,
    execution_count: Arc<AtomicUsize>,
    completed_continuation_slices: Arc<Mutex<HashSet<(Uuid, i64)>>>,
    stop_supervisor_tx: tokio::sync::oneshot::Sender<()>,
    supervisor_task: tokio::task::JoinHandle<RuntimeShutdownReport>,
}

struct RecoveryJobs {
    keyed: Uuid,
    transactional: Uuid,
}

struct OpaqueConsumerTransaction<'a> {
    inner: sqlx::Transaction<'a, sqlx::Postgres>,
}

impl<'a> OpaqueConsumerTransaction<'a> {
    fn new(inner: sqlx::Transaction<'a, sqlx::Postgres>) -> Self {
        Self { inner }
    }

    async fn commit(self) -> Result<(), sqlx::Error> {
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), sqlx::Error> {
        self.inner.rollback().await
    }
}

impl<'a> OpaqueConsumerTransaction<'a> {
    fn view(&mut self) -> runledger_postgres::PgTransactionView<'_, 'a> {
        runledger_postgres::PgTransactionView::new(&mut self.inner)
    }

    fn executor(&mut self) -> impl sqlx::Executor<'_, Database = sqlx::Postgres> {
        &mut *self.inner
    }
}

fn inspect_shutdown_invariants(report: &RuntimeShutdownReport) {
    match report.cause() {
        RuntimeShutdownCause::Requested
        | RuntimeShutdownCause::SignalFailed
        | RuntimeShutdownCause::LoopFailure(_)
        | RuntimeShutdownCause::DescendantFailure { .. } => {}
    }
    match report.settlement() {
        RuntimeShutdownSettlement::Settled | RuntimeShutdownSettlement::GracefulTimeout => {}
        RuntimeShutdownSettlement::AbortTimeout { unjoined } => {
            assert!(std::ptr::eq(unjoined.as_slice(), report.unjoined()));
            assert_eq!(unjoined.len().get(), report.unjoined().len());
        }
        RuntimeShutdownSettlement::Interrupted { unjoined } => {
            assert!(std::ptr::eq(unjoined.as_slice(), report.unjoined()));
            assert!(report.failure().is_some());
        }
    }
    match report.failure() {
        None
        | Some(RuntimeShutdownFailure::Signal { .. })
        | Some(RuntimeShutdownFailure::SignalPanicked)
        | Some(RuntimeShutdownFailure::LoopExitedUnexpectedly { .. })
        | Some(RuntimeShutdownFailure::LoopInvalidConfig { .. })
        | Some(RuntimeShutdownFailure::LoopJoin { .. })
        | Some(RuntimeShutdownFailure::DescendantJoin { .. })
        | Some(RuntimeShutdownFailure::CallbackInterrupted { .. })
        | Some(RuntimeShutdownFailure::EarlierCallbackInterruptions { .. })
        | Some(RuntimeShutdownFailure::GracefulTimeout)
        | Some(RuntimeShutdownFailure::UnrepresentableDeadline)
        | Some(RuntimeShutdownFailure::SettlementInterrupted) => {}
        Some(RuntimeShutdownFailure::AbortTimeout { unjoined }) => {
            assert!(unjoined.get() > 0);
        }
    }
    for failure in report.callback_failures() {
        match failure {
            RuntimeCallbackFailure::TimedOut { .. }
            | RuntimeCallbackFailure::Panicked { .. }
            | RuntimeCallbackFailure::LeaseMaintenance { .. } => {}
        }
    }
}

struct AbortBlocker {
    entered: AtomicBool,
    released: StdMutex<bool>,
    release: Condvar,
    exited: watch::Sender<bool>,
}

impl Default for AbortBlocker {
    fn default() -> Self {
        let (exited, _) = watch::channel(false);
        Self {
            entered: AtomicBool::new(false),
            released: StdMutex::new(false),
            release: Condvar::new(),
            exited,
        }
    }
}

impl AbortBlocker {
    fn release(&self) {
        *self.released.lock().expect("lock abort blocker") = true;
        self.release.notify_all();
    }

    async fn wait_for_exit(&self) {
        let mut exited = self.exited.subscribe();
        while !*exited.borrow_and_update() {
            exited
                .changed()
                .await
                .expect("abort blocker owns the exit sender");
        }
    }
}

struct BlockOnDrop(Arc<AbortBlocker>);

struct ReleaseOnDrop(Arc<AbortBlocker>);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

impl Future for BlockOnDrop {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.entered.store(true, Ordering::SeqCst);
        Poll::Pending
    }
}

impl Drop for BlockOnDrop {
    fn drop(&mut self) {
        let mut released = self.0.released.lock().expect("lock abort blocker");
        while !*released {
            released = self.0.release.wait(released).expect("wait for release");
        }
        self.0.exited.send_replace(true);
    }
}

struct AbortTimeoutHandler(Arc<AbortBlocker>);

#[async_trait]
impl JobHandler for AbortTimeoutHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(SMOKE_JOB_TYPE)
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        BlockOnDrop(Arc::clone(&self.0)).await;
        Ok(JobCompletion::success())
    }
}

struct SmokeJobs {
    success: Uuid,
    continuation: Uuid,
    terminal: Uuid,
    retry_after: Uuid,
    retry_at: Uuid,
}

async fn setup_consumer_schema_and_intent(pool: &DbPool) -> (Uuid, Value) {
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(pool)
        .await
        .expect("read smoke PostgreSQL server_version");
    let server_version_num =
        sqlx::query_scalar::<_, i32>("SELECT current_setting('server_version_num')::int")
            .fetch_one(pool)
            .await
            .expect("read smoke PostgreSQL server_version_num");
    eprintln!(
        "external consumer smoke PostgreSQL server_version={server_version} server_version_num={server_version_num}"
    );

    runledger_postgres::migrate_after_idempotency_cutover(pool)
        .await
        .expect("apply packaged migrations");
    create_consumer_audit_table(pool)
        .await
        .expect("create consumer-owned audit table");

    let intent_payload = json!({"kind": "success", "source": "transactional-intent"});
    let intent_request = JobEnqueueIntent::new(
        JobType::new(SMOKE_JOB_TYPE),
        &intent_payload,
        "external-smoke-transactional-intent",
    );
    let mut intent_tx = pool
        .begin()
        .await
        .expect("begin consumer-owned intent transaction");
    let recorded_intent = record_job_enqueue_intent_tx(&mut intent_tx, &intent_request)
        .await
        .expect("record intent before job definition");
    record_consumer_audit_tx(
        &mut intent_tx,
        "transactional-enqueue-intent",
        recorded_intent.intent_id,
        recorded_intent.intent_id,
    )
    .await
    .expect("record consumer audit beside enqueue intent");
    intent_tx
        .commit()
        .await
        .expect("commit consumer audit and enqueue intent atomically");
    assert_consumer_audit(
        pool,
        "transactional-enqueue-intent",
        recorded_intent.intent_id,
        recorded_intent.intent_id,
    )
    .await;
    (recorded_intent.intent_id, intent_payload)
}

async fn register_smoke_job_definition(pool: &DbPool) {
    let mut tx = pool.begin().await.expect("begin job definition tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(SMOKE_JOB_TYPE),
            version: 1,
            max_attempts: 1,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert smoke job definition");
    tx.commit().await.expect("commit job definition tx");
}

async fn start_smoke_runtime(pool: &DbPool) -> SmokeRuntime {
    let hang_release = Arc::new(Notify::new());
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let execution_count = Arc::new(AtomicUsize::new(0));
    let completed_continuation_slices = Arc::new(Mutex::new(HashSet::new()));

    let handler = SmokeHandler {
        execution_count: Arc::clone(&execution_count),
        hang_release: Arc::clone(&hang_release),
        dead_letters: Arc::clone(&dead_letters),
        completed_continuation_slices: Arc::clone(&completed_continuation_slices),
        continuation_canary_enabled: true,
    };

    let mut catalog = JobCatalog::new().handler(handler);
    for failure_code in [RETRY_AFTER_FAILURE_CODE, RETRY_AT_FAILURE_CODE] {
        catalog =
            catalog.retry_delay_override(SMOKE_JOB_TYPE, failure_code, SMOKE_RETRY_POLICY_DELAY_MS);
    }
    let registry = catalog.to_registry();

    let config = JobsConfig {
        worker_id: "external-consumer-smoke-worker".to_string(),
        poll_interval: Duration::from_millis(25),
        claim_batch_size: 8,
        lease_ttl_seconds: 10,
        max_global_concurrency: 8,
        reaper_interval: Duration::from_millis(100),
        schedule_poll_interval: Duration::from_millis(100),
        reaper_retry_delay_ms: 1_000,
    };

    let supervisor = Supervisor::builder(pool, config)
        .expect("supervisor builder should find active Tokio runtime")
        .with_registry(registry)
        .build()
        .expect("supervisor should build");
    let (stop_supervisor_tx, stop_supervisor_rx) = tokio::sync::oneshot::channel();
    let budget = RuntimeShutdownBudget::new(Duration::from_secs(10), Duration::from_secs(2))
        .expect("valid shutdown budget");
    let supervisor_task = tokio::spawn(supervisor.run_until_shutdown_report(
        RuntimeShutdownSignal::infallible(async move {
            let _ = stop_supervisor_rx.await;
        }),
        budget,
    ));

    SmokeRuntime {
        hang_release,
        dead_letters,
        execution_count,
        completed_continuation_slices,
        stop_supervisor_tx,
        supervisor_task,
    }
}

async fn assert_keyed_recovery(pool: &DbPool, recovery_payload: &Value) -> Uuid {
    let recovery_next_run_at = Utc::now() + ChronoDuration::hours(1);
    let recovery_request = JobEnqueue {
        job_type: JobType::new(SMOKE_JOB_TYPE),
        organization_id: None,
        payload: recovery_payload,
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        next_run_at: Some(recovery_next_run_at),
        idempotency_key: Some("external-smoke-recovery"),
        stage: None,
    };
    let inserted_recovery = enqueue_job_with_outcome(pool, &recovery_request)
        .await
        .expect("insert recovery job with outcome");
    assert_eq!(
        inserted_recovery.disposition,
        JobEnqueueDisposition::Inserted
    );
    cancel_job_with_scope(
        pool,
        JobCancellationScope::Global,
        inserted_recovery.job_id,
        Some("external smoke recovery"),
    )
    .await
    .expect("cancel recovery job");

    let existing_enqueue_tx = pool.begin().await.expect("begin existing enqueue");
    let mut existing_enqueue_tx = OpaqueConsumerTransaction::new(existing_enqueue_tx);
    let existing_recovery =
        enqueue_job_with_outcome_in_transaction(&mut existing_enqueue_tx.view(), &recovery_request)
            .await
            .expect("resolve existing recovery job through opaque transaction");
    assert_eq!(existing_recovery.job_id, inserted_recovery.job_id);
    assert_eq!(existing_recovery.status, JobStatus::Canceled);
    assert_eq!(
        existing_recovery.disposition,
        JobEnqueueDisposition::Existing
    );
    record_consumer_audit_tx(
        &mut existing_enqueue_tx.view(),
        "opaque-transactional-enqueue",
        inserted_recovery.job_id,
        existing_recovery.job_id,
    )
    .await
    .expect("record consumer audit through the same opaque transaction");
    existing_enqueue_tx
        .commit()
        .await
        .expect("commit existing enqueue");
    assert_consumer_audit(
        pool,
        "opaque-transactional-enqueue",
        inserted_recovery.job_id,
        existing_recovery.job_id,
    )
    .await;

    let observed_recovery = get_job_by_id(pool, None, inserted_recovery.job_id)
        .await
        .expect("load canceled recovery job")
        .expect("canceled recovery job exists");
    let recovery_request = CompareAndRequeueJob::from_observed_job(
        &observed_recovery,
        JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
        "external smoke compare-and-requeue",
    )
    .expect("canceled observation is recoverable");
    let recovery_outcome = compare_and_requeue_job(pool, recovery_request)
        .await
        .expect("compare and requeue recovery job");
    assert!(matches!(
        recovery_outcome,
        CompareAndRequeueJobOutcome::Requeued { .. }
    ));
    inserted_recovery.job_id
}

async fn assert_opaque_transaction_atomicity(pool: &DbPool) {
    assert_opaque_transaction_rollback(pool).await;
    assert_opaque_transaction_commit(pool).await;
}

async fn assert_opaque_transaction_rollback(pool: &DbPool) {
    let rollback_payload = json!({"kind": "success", "source": "opaque-rollback"});
    let rollback_request = JobEnqueue {
        job_type: JobType::new(SMOKE_JOB_TYPE),
        organization_id: None,
        payload: &rollback_payload,
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        next_run_at: None,
        idempotency_key: Some("external-smoke-opaque-rollback"),
        stage: None,
    };
    let rollback_tx = pool
        .begin()
        .await
        .expect("begin opaque rollback transaction");
    let mut rollback_tx = OpaqueConsumerTransaction::new(rollback_tx);
    let rolled_back =
        enqueue_job_with_outcome_in_transaction(&mut rollback_tx.view(), &rollback_request)
            .await
            .expect("enqueue through opaque rollback transaction");
    assert_eq!(rolled_back.disposition, JobEnqueueDisposition::Inserted);
    record_consumer_audit_tx(
        &mut rollback_tx.view(),
        "opaque-transactional-enqueue-rollback",
        rolled_back.job_id,
        rolled_back.job_id,
    )
    .await
    .expect("record audit through opaque rollback transaction");
    rollback_tx
        .rollback()
        .await
        .expect("roll back opaque enqueue and audit together");

    assert!(
        get_job_by_id(pool, None, rolled_back.job_id)
            .await
            .expect("check rolled-back opaque enqueue")
            .is_none(),
        "rolling back the opaque transaction must remove the queued job"
    );
    assert!(
        list_job_events(pool, None, rolled_back.job_id, 100, None)
            .await
            .expect("check rolled-back opaque enqueue event")
            .is_empty(),
        "rolling back the opaque transaction must remove its enqueue event"
    );
    assert_consumer_audit_absent(pool, "opaque-transactional-enqueue-rollback").await;
}

async fn assert_opaque_transaction_commit(pool: &DbPool) {
    let commit_payload = json!({"kind": "success", "source": "opaque-commit"});
    let commit_request = JobEnqueue {
        job_type: JobType::new(SMOKE_JOB_TYPE),
        organization_id: None,
        payload: &commit_payload,
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        next_run_at: None,
        idempotency_key: Some("external-smoke-opaque-commit"),
        stage: None,
    };
    let commit_tx = pool.begin().await.expect("begin opaque commit transaction");
    let mut commit_tx = OpaqueConsumerTransaction::new(commit_tx);
    let committed = enqueue_job_with_outcome_in_transaction(&mut commit_tx.view(), &commit_request)
        .await
        .expect("enqueue through opaque commit transaction");
    assert_eq!(committed.disposition, JobEnqueueDisposition::Inserted);
    record_consumer_audit_tx(
        &mut commit_tx.view(),
        "opaque-transactional-enqueue-commit",
        committed.job_id,
        committed.job_id,
    )
    .await
    .expect("record audit through opaque commit transaction");
    commit_tx
        .commit()
        .await
        .expect("commit opaque enqueue and audit together");

    let committed_job = get_job_by_id(pool, None, committed.job_id)
        .await
        .expect("load committed opaque enqueue")
        .expect("opaque enqueue must persist with its audit row");
    assert_eq!(committed_job.status, JobStatus::Pending);
    let committed_events = list_job_events(pool, None, committed.job_id, 100, None)
        .await
        .expect("load committed opaque enqueue event");
    assert_eq!(committed_events.len(), 1);
    assert_eq!(committed_events[0].event_type, JobEventType::Enqueued);
    assert_consumer_audit(
        pool,
        "opaque-transactional-enqueue-commit",
        committed.job_id,
        committed.job_id,
    )
    .await;
}

async fn assert_transactional_recovery(pool: &DbPool, recovery_payload: &Value) -> Uuid {
    let transactional_recovery_job_id = enqueue_payload(pool, recovery_payload).await;
    cancel_job_with_scope(
        pool,
        JobCancellationScope::Global,
        transactional_recovery_job_id,
        Some("external smoke transactional recovery"),
    )
    .await
    .expect("cancel transactional recovery job");
    let observed_transactional_recovery = get_job_by_id(pool, None, transactional_recovery_job_id)
        .await
        .expect("load transactional recovery job")
        .expect("transactional recovery job exists");
    let transactional_recovery_request = CompareAndRequeueJob::from_observed_job(
        &observed_transactional_recovery,
        JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
        "external smoke transactional compare-and-requeue",
    )
    .expect("transactional recovery observation is recoverable");
    let mut transactional_recovery_tx = pool
        .begin()
        .await
        .expect("begin consumer-owned recovery transaction");
    let transactional_recovery_outcome = compare_and_requeue_job_tx(
        &mut transactional_recovery_tx,
        transactional_recovery_request,
    )
    .await
    .expect("compare and requeue in consumer-owned transaction");
    let CompareAndRequeueJobOutcome::Requeued {
        after: transactional_recovery,
        ..
    } = transactional_recovery_outcome
    else {
        panic!("expected transactional recovery to requeue");
    };
    record_consumer_audit_tx(
        &mut transactional_recovery_tx,
        "transactional-recovery",
        transactional_recovery_job_id,
        transactional_recovery.id,
    )
    .await
    .expect("record recovery in consumer-owned transaction");
    transactional_recovery_tx
        .commit()
        .await
        .expect("commit recovery and consumer audit atomically");
    assert_consumer_audit(
        pool,
        "transactional-recovery",
        transactional_recovery_job_id,
        transactional_recovery_job_id,
    )
    .await;
    let committed_transactional_recovery = get_job_by_id(pool, None, transactional_recovery_job_id)
        .await
        .expect("reload committed transactional recovery")
        .expect("committed transactional recovery exists");
    assert_eq!(committed_transactional_recovery.status, JobStatus::Pending);
    assert_eq!(committed_transactional_recovery.run_number, 2);
    transactional_recovery_job_id
}

async fn assert_recovery_apis(pool: &DbPool) -> RecoveryJobs {
    let recovery_payload = json!({"kind": "success"});
    RecoveryJobs {
        keyed: assert_keyed_recovery(pool, &recovery_payload).await,
        transactional: assert_transactional_recovery(pool, &recovery_payload).await,
    }
}

async fn enqueue_smoke_jobs(pool: &DbPool, intent_id: Uuid, intent_payload: &Value) -> SmokeJobs {
    let success_job_id = enqueue_kind(pool, "success").await;
    let intent_job_id = wait_for_promoted_intent(pool, intent_id).await;
    let intent_job = wait_for_status(pool, intent_job_id, JobStatus::Succeeded).await;
    assert_eq!(&intent_job.payload, intent_payload);
    let continuation_job_id = enqueue_payload(
        pool,
        &json!({
            "kind": "continuation",
            "canary": true,
            "max_runs": CONTINUATION_MAX_RUNS,
        }),
    )
    .await;
    let terminal_job_id = enqueue_kind(pool, "terminal").await;
    let retry_after_job_id =
        enqueue_payload_with_max_attempts(pool, &json!({"kind": "retry-after"}), Some(2)).await;
    let retry_at_job_id =
        enqueue_payload_with_max_attempts(pool, &json!({"kind": "retry-at"}), Some(2)).await;
    insert_due_schedule(pool, "scheduled-success")
        .await
        .expect("insert due schedule");
    SmokeJobs {
        success: success_job_id,
        continuation: continuation_job_id,
        terminal: terminal_job_id,
        retry_after: retry_after_job_id,
        retry_at: retry_at_job_id,
    }
}

async fn assert_successful_replay(pool: &DbPool, success_job_id: Uuid) {
    let success_job = wait_for_status(pool, success_job_id, JobStatus::Succeeded).await;
    assert_eq!(success_job.status, JobStatus::Succeeded);

    let replay_request = CompareAndReplaySucceededJob {
        scope: JobScope::Global,
        source_job_id: success_job.id,
        expected_run_number: success_job.run_number,
        replay_request_key: REPLAY_REQUEST_KEY,
        reason: REPLAY_REASON,
    };
    let mut replay_tx = pool
        .begin()
        .await
        .expect("begin consumer-owned replay transaction");
    let replay_outcome =
        compare_and_replay_succeeded_job_tx(&mut replay_tx, replay_request.clone())
            .await
            .expect("replay successful job in consumer-owned transaction");
    let CompareAndReplaySucceededJobOutcome::Replayed { replay, .. } = replay_outcome else {
        panic!("expected successful replay outcome");
    };
    assert_eq!(replay.disposition, JobEnqueueDisposition::Inserted);
    assert_ne!(replay.job_id, success_job.id);
    record_consumer_audit_tx(
        &mut replay_tx,
        "transactional-successful-replay",
        success_job.id,
        replay.job_id,
    )
    .await
    .expect("record replay in consumer-owned transaction");
    replay_tx
        .commit()
        .await
        .expect("commit replay and consumer audit atomically");
    assert_consumer_audit(
        pool,
        "transactional-successful-replay",
        success_job.id,
        replay.job_id,
    )
    .await;

    let existing_replay = compare_and_replay_succeeded_job(pool, replay_request)
        .await
        .expect("resolve replay idempotently through pool wrapper");
    let CompareAndReplaySucceededJobOutcome::Replayed {
        replay: existing_replay,
        ..
    } = existing_replay
    else {
        panic!("expected existing successful replay outcome");
    };
    assert_eq!(existing_replay.job_id, replay.job_id);
    assert_eq!(existing_replay.disposition, JobEnqueueDisposition::Existing);

    let replayed_job = wait_for_status(pool, replay.job_id, JobStatus::Succeeded).await;
    assert_eq!(replayed_job.run_number, 1);
    let replay_events = list_job_events(pool, None, replay.job_id, 100, None)
        .await
        .expect("list successful replay events through the public prelude");
    assert_successful_replay_event(
        &replay_events,
        replay.job_id,
        success_job.id,
        success_job.run_number,
    );
    assert_eq!(
        get_job_by_id(pool, None, success_job.id)
            .await
            .expect("reload successful replay source")
            .expect("successful replay source still exists")
            .status,
        JobStatus::Succeeded
    );
}

async fn assert_continuation_smoke_job(pool: &DbPool, jobs: &SmokeJobs, runtime: &SmokeRuntime) {
    let continuation_job = wait_for_status(pool, jobs.continuation, JobStatus::Succeeded).await;
    assert_eq!(continuation_job.run_number, 2);
    assert_eq!(continuation_job.attempt, 1);
    let continuation_events = list_job_events(pool, None, jobs.continuation, 100, None)
        .await
        .expect("list continuation events through the public prelude");
    assert_handler_continuation_event(&continuation_events, jobs.continuation);
    assert_eq!(
        continuation_job.checkpoint,
        Some(json!({
            "version": CONTINUATION_CHECKPOINT_VERSION,
            "cursor": CONTINUATION_MAX_RUNS - 1,
        }))
    );
    let continuation_metrics = get_job_continuation_metrics(pool, None, Some(SMOKE_JOB_TYPE))
        .await
        .expect("load smoke continuation metrics")
        .pop()
        .expect("registered smoke job type has metrics");
    assert_eq!(continuation_metrics.continued_24h, 1);
    assert_eq!(continuation_metrics.active_continued_count, 0);
    assert_eq!(continuation_metrics.max_active_run_number, 0);
    assert_eq!(runtime.completed_continuation_slices.lock().await.len(), 2);
}

async fn assert_retry_smoke_jobs(pool: &DbPool, jobs: &SmokeJobs) {
    let retry_after_job = wait_for_status(pool, jobs.retry_after, JobStatus::Succeeded).await;
    assert_eq!(retry_after_job.run_number, 1);
    assert_eq!(retry_after_job.attempt, 2);
    let retry_after_events = list_job_events(pool, None, jobs.retry_after, 100, None)
        .await
        .expect("list handler-selected relative retry events");
    assert_relative_retry_event(&retry_after_events, jobs.retry_after);

    let retry_at_job = wait_for_status(pool, jobs.retry_at, JobStatus::Succeeded).await;
    assert_eq!(retry_at_job.run_number, 1);
    assert_eq!(retry_at_job.attempt, 2);
    let retry_at_events = list_job_events(pool, None, jobs.retry_at, 100, None)
        .await
        .expect("list handler-selected absolute retry events");
    assert_absolute_retry_event(&retry_at_events, jobs.retry_at);
}

async fn assert_recovery_terminal_and_schedule_jobs(
    pool: &DbPool,
    jobs: &SmokeJobs,
    recovery_jobs: &RecoveryJobs,
) {
    let recovered_job = wait_for_status(pool, recovery_jobs.keyed, JobStatus::Succeeded).await;
    assert_eq!(recovered_job.run_number, 2);
    let transactionally_recovered_job =
        wait_for_status(pool, recovery_jobs.transactional, JobStatus::Succeeded).await;
    assert_eq!(transactionally_recovered_job.run_number, 2);

    let terminal_job = wait_for_status(pool, jobs.terminal, JobStatus::DeadLettered).await;
    assert_eq!(terminal_job.status, JobStatus::DeadLettered);

    let scheduled_job = wait_for_kind_status(pool, "scheduled-success", JobStatus::Succeeded).await;
    assert_eq!(scheduled_job.status, JobStatus::Succeeded);
}

async fn assert_completed_smoke_jobs(
    pool: &DbPool,
    jobs: &SmokeJobs,
    recovery_jobs: &RecoveryJobs,
    runtime: &SmokeRuntime,
) {
    assert_continuation_smoke_job(pool, jobs, runtime).await;
    assert_retry_smoke_jobs(pool, jobs).await;
    assert_recovery_terminal_and_schedule_jobs(pool, jobs, recovery_jobs).await;
}

async fn assert_reaping_and_shutdown(pool: &DbPool, runtime: SmokeRuntime) {
    let hanging_job_id = enqueue_kind(pool, "hang").await;
    let hanging_job = wait_for_running(pool, hanging_job_id).await;
    assert_eq!(hanging_job.status, JobStatus::Leased);
    assert_eq!(hanging_job.stage, JobStage::Running);
    assert!(
        hanging_job.worker_id.is_some(),
        "hanging job should be claimed by the worker"
    );

    expire_job_lease(pool, hanging_job_id)
        .await
        .expect("force job lease expiration");

    let reaped_job = wait_for_status(pool, hanging_job_id, JobStatus::DeadLettered).await;
    assert_eq!(reaped_job.status, JobStatus::DeadLettered);

    wait_for_dead_letter(&runtime.dead_letters, "terminal").await;
    wait_for_dead_letter(&runtime.dead_letters, "hang").await;

    assert!(
        runtime.execution_count.load(Ordering::SeqCst) >= 13,
        "worker should execute direct, retried, continued, recovered, replayed, scheduled, and hanging jobs"
    );

    runtime.hang_release.notify_waiters();
    let _ = runtime.stop_supervisor_tx.send(());
    let report = timeout(Duration::from_secs(12), runtime.supervisor_task)
        .await
        .expect("supervisor monitor task should stop before outer timeout")
        .expect("supervisor monitor task should join");
    inspect_shutdown_invariants(&report);
    let outcome = report.classify();
    assert!(
        matches!(outcome, RuntimeSettlement::Clean(_)),
        "a clean smoke run must authorize dependency cleanup: {outcome:?}"
    );
}

async fn assert_external_abort_timeout(pool: &DbPool) {
    let blocker = Arc::new(AbortBlocker::default());
    let _release = ReleaseOnDrop(Arc::clone(&blocker));
    let config = JobsConfig {
        worker_id: "external-abort-timeout-worker".to_string(),
        poll_interval: Duration::from_millis(10),
        claim_batch_size: 1,
        lease_ttl_seconds: 10,
        max_global_concurrency: 1,
        reaper_interval: Duration::from_secs(60),
        schedule_poll_interval: Duration::from_secs(60),
        reaper_retry_delay_ms: 1_000,
    };
    let supervisor = Supervisor::builder(pool, config)
        .expect("build abort-timeout supervisor")
        .with_registry(
            JobCatalog::new()
                .handler(AbortTimeoutHandler(Arc::clone(&blocker)))
                .to_registry(),
        )
        .disable_intent_promoter()
        .disable_scheduler()
        .disable_reaper()
        .build()
        .expect("start abort-timeout supervisor");
    let job_id = enqueue_kind(pool, "abort-timeout").await;
    wait_for_running(pool, job_id).await;
    timeout(Duration::from_secs(2), async {
        while !blocker.entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("handler reaches non-yielding destruction fixture");

    let budget = RuntimeShutdownBudget::new(Duration::ZERO, Duration::from_millis(25))
        .expect("valid abort-timeout budget");
    let report = timeout(Duration::from_secs(2), supervisor.shutdown_report(budget))
        .await
        .expect("abort-timeout supervisor produces a bounded terminal report");
    inspect_shutdown_invariants(&report);
    let RuntimeShutdownSettlement::AbortTimeout { unjoined } = report.settlement() else {
        panic!("external fixture must produce AbortTimeout: {report:?}");
    };
    assert!(!unjoined.as_slice().is_empty());
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::AbortTimeout { unjoined }) if unjoined.get() > 0
    ));
    assert!(matches!(report.classify(), RuntimeSettlement::Unsettled(_)));

    blocker.release();
    timeout(Duration::from_secs(2), blocker.wait_for_exit())
        .await
        .expect("blocked handler destruction exits after release");
}

#[tokio::test]
async fn abort_fixture_exit_state_is_durable_before_waiter_subscription() {
    let blocker = AbortBlocker::default();
    blocker.exited.send_replace(true);

    timeout(Duration::from_millis(100), blocker.wait_for_exit())
        .await
        .expect("an already-recorded exit remains observable");
}

#[test]
fn abort_fixture_releases_a_blocked_destructor_when_assertions_unwind() {
    let blocker = Arc::new(AbortBlocker::default());
    let blocked = BlockOnDrop(Arc::clone(&blocker));
    let (exited, exit) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        drop(blocked);
        exited.send(()).expect("test observes destructor exit");
    });
    let unwind = std::panic::catch_unwind(|| {
        let _release = ReleaseOnDrop(Arc::clone(&blocker));
        panic!("simulated smoke assertion");
    });
    assert!(unwind.is_err());
    exit.recv_timeout(Duration::from_secs(2))
        .expect("fixture release must survive a failed assertion");
    thread.join().expect("destructor thread exits");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn packaged_crates_support_external_consumer_embedding() {
    let (pool, database) =
        setup_unmigrated_ephemeral_pool("external_consumer_smoke", SMOKE_POOL_MAX_CONNECTIONS)
            .await;
    let (intent_id, intent_payload) = setup_consumer_schema_and_intent(&pool).await;
    register_smoke_job_definition(&pool).await;
    assert_opaque_transaction_atomicity(&pool).await;
    let recovery_jobs = assert_recovery_apis(&pool).await;
    let runtime = start_smoke_runtime(&pool).await;
    let jobs = enqueue_smoke_jobs(&pool, intent_id, &intent_payload).await;
    assert_successful_replay(&pool, jobs.success).await;
    assert_completed_smoke_jobs(&pool, &jobs, &recovery_jobs, &runtime).await;
    assert_reaping_and_shutdown(&pool, runtime).await;
    assert_external_abort_timeout(&pool).await;
    teardown_ephemeral_pool(pool, database).await;
}

struct SmokeHandler {
    execution_count: Arc<AtomicUsize>,
    hang_release: Arc<Notify>,
    dead_letters: Arc<Mutex<Vec<String>>>,
    completed_continuation_slices: Arc<Mutex<HashSet<(sqlx::types::Uuid, i64)>>>,
    continuation_canary_enabled: bool,
}

impl SmokeHandler {
    async fn execute_continuation(
        &self,
        context: JobContext,
        payload: &Value,
    ) -> Result<JobCompletion, JobFailure> {
        if !self.continuation_canary_enabled
            || payload.get("canary").and_then(Value::as_bool) != Some(true)
        {
            return Err(JobFailure::terminal(
                "smoke.continuation_not_enabled",
                "Continuation is not enabled for this application canary.",
            ));
        }

        let max_runs = payload
            .get("max_runs")
            .and_then(Value::as_i64)
            .filter(|max_runs| *max_runs > 0)
            .ok_or_else(|| {
                JobFailure::terminal(
                    "smoke.invalid_continuation_limit",
                    "Continuation payload requires a positive max_runs limit.",
                )
            })?;
        if i64::from(context.run_number) > max_runs {
            return Err(JobFailure::terminal(
                "smoke.continuation_limit_exceeded",
                "Continuation exceeded its application-owned run limit.",
            ));
        }

        let cursor = match context.checkpoint.as_ref() {
            None => 0,
            Some(checkpoint) => {
                if checkpoint.get("version").and_then(Value::as_i64)
                    != Some(CONTINUATION_CHECKPOINT_VERSION)
                {
                    return Err(JobFailure::terminal(
                        "smoke.unsupported_checkpoint_version",
                        "Continuation checkpoint version is unsupported.",
                    ));
                }
                checkpoint
                    .get("cursor")
                    .and_then(Value::as_i64)
                    .filter(|cursor| *cursor >= 0)
                    .ok_or_else(|| {
                        JobFailure::terminal(
                            "smoke.invalid_checkpoint_cursor",
                            "Continuation checkpoint cursor is invalid.",
                        )
                    })?
            }
        };
        let slice = cursor + 1;
        if slice != i64::from(context.run_number) {
            return Err(JobFailure::terminal(
                "smoke.checkpoint_run_mismatch",
                "Continuation checkpoint does not match the current run.",
            ));
        }

        // A production handler would enforce this uniqueness in the same
        // datastore as its externally visible side effect. `(job_id, slice)`
        // remains stable if an attempt is retried, unlike `attempt`.
        self.completed_continuation_slices
            .lock()
            .await
            .insert((context.job_id, slice));

        if slice < max_runs {
            Ok(JobCompletion::continue_after(Duration::from_millis(25))
                .progress(slice, max_runs)
                .map_err(|error| JobFailure::terminal("smoke.invalid_progress", error.to_string()))?
                .checkpoint(json!({
                    "version": CONTINUATION_CHECKPOINT_VERSION,
                    "cursor": slice,
                })))
        } else {
            JobCompletion::success()
                .progress(slice, max_runs)
                .map_err(|error| JobFailure::terminal("smoke.invalid_progress", error.to_string()))
        }
    }
}

#[async_trait]
impl JobHandler for SmokeHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(SMOKE_JOB_TYPE)
    }

    async fn execute(
        &self,
        context: JobContext,
        payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.execution_count.fetch_add(1, Ordering::SeqCst);

        match payload_kind(&payload) {
            "success" | "scheduled-success" => Ok(JobCompletion::success()),
            "continuation" => self.execute_continuation(context, &payload).await,
            "retry-after" if context.attempt == 1 => Err(JobFailure::retryable(
                RETRY_AFTER_FAILURE_CODE,
                "Smoke provider requested a relative retry.",
            )
            .retry_not_before_delay(HANDLER_RETRY_AFTER)),
            "retry-after" => Ok(JobCompletion::success()),
            "retry-at" if context.attempt == 1 => Err(JobFailure::retryable(
                RETRY_AT_FAILURE_CODE,
                "Smoke provider supplied an absolute reset timestamp.",
            )
            .retry_not_before(Utc::now() + ChronoDuration::milliseconds(HANDLER_RETRY_AFTER_MS))),
            "retry-at" => Ok(JobCompletion::success()),
            "terminal" => Err(JobFailure::terminal(
                "smoke.terminal_failure",
                "Smoke handler returned a terminal failure.",
            )),
            "hang" => {
                self.hang_release.notified().await;
                Ok(JobCompletion::success())
            }
            other => Err(JobFailure::terminal(
                "smoke.unknown_kind",
                format!("Unsupported smoke payload kind `{other}`."),
            )),
        }
    }

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        payload: Value,
        _dead_letter: JobDeadLetterInfo,
    ) {
        self.dead_letters
            .lock()
            .await
            .push(payload_kind(&payload).to_string());
    }
}

async fn enqueue_kind(pool: &DbPool, kind: &str) -> sqlx::types::Uuid {
    enqueue_payload(pool, &json!({ "kind": kind })).await
}

async fn enqueue_payload(pool: &DbPool, payload: &Value) -> sqlx::types::Uuid {
    enqueue_payload_with_max_attempts(pool, payload, None).await
}

async fn enqueue_payload_with_max_attempts(
    pool: &DbPool,
    payload: &Value,
    max_attempts: Option<i32>,
) -> sqlx::types::Uuid {
    jobs::enqueue_job(
        pool,
        &JobEnqueue {
            job_type: JobType::new(SMOKE_JOB_TYPE),
            organization_id: None,
            payload,
            priority: None,
            max_attempts,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue smoke job")
}

fn retry_scheduled_event(
    events: &[JobEventRecord],
    expected_job_id: sqlx::types::Uuid,
) -> &JobEventRecord {
    let event = events
        .iter()
        .find(|event| event.event_type == JobEventType::RetryScheduled)
        .expect("handler-selected retry should record RETRY_SCHEDULED");
    assert_eq!(event.job_id, expected_job_id);
    event
}

fn assert_relative_retry_event(events: &[JobEventRecord], expected_job_id: sqlx::types::Uuid) {
    let event = retry_scheduled_event(events, expected_job_id);
    assert!(
        event
            .payload
            .get("retry_delay_ms")
            .and_then(Value::as_i64)
            .is_some_and(|delay_ms| delay_ms > 0),
        "relative retry audit should retain the positive policy delay"
    );
    let requested_retry_not_before = event
        .payload
        .get("requested_retry_not_before")
        .and_then(Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
        .expect("relative retry audit should retain the handler lower bound");
    let next_run_at = event
        .payload
        .get("next_run_at")
        .and_then(Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
        .expect("relative retry audit should retain next_run_at");
    assert!(
        next_run_at >= requested_retry_not_before,
        "effective relative retry time must not precede the handler lower bound"
    );
}

fn assert_absolute_retry_event(events: &[JobEventRecord], expected_job_id: sqlx::types::Uuid) {
    let event = retry_scheduled_event(events, expected_job_id);
    let requested_retry_at = event
        .payload
        .get("requested_retry_not_before")
        .and_then(Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
        .expect("absolute retry audit should retain requested_retry_at");
    let next_run_at = event
        .payload
        .get("next_run_at")
        .and_then(Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
        .expect("absolute retry audit should retain next_run_at");
    assert!(
        next_run_at >= requested_retry_at,
        "effective absolute retry time must not precede the requested provider reset"
    );
}

async fn create_consumer_audit_table(pool: &DbPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE external_consumer_operation_audit (
            operation_key text PRIMARY KEY,
            source_job_id uuid NOT NULL,
            result_job_id uuid NOT NULL
         )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn record_consumer_audit_tx<T>(
    tx: &mut T,
    operation_key: &str,
    source_job_id: sqlx::types::Uuid,
    result_job_id: sqlx::types::Uuid,
) -> Result<(), sqlx::Error>
where
    T: PgTransactionExecutor + ?Sized,
{
    sqlx::query(
        "INSERT INTO external_consumer_operation_audit (
            operation_key,
            source_job_id,
            result_job_id
         )
         VALUES ($1, $2, $3)",
    )
    .bind(operation_key)
    .bind(source_job_id)
    .bind(result_job_id)
    .execute(tx.executor())
    .await?;
    Ok(())
}

async fn assert_consumer_audit(
    pool: &DbPool,
    operation_key: &str,
    expected_source_job_id: sqlx::types::Uuid,
    expected_result_job_id: sqlx::types::Uuid,
) {
    let (source_job_id, result_job_id) =
        sqlx::query_as::<_, (sqlx::types::Uuid, sqlx::types::Uuid)>(
            "SELECT source_job_id, result_job_id
         FROM external_consumer_operation_audit
         WHERE operation_key = $1",
        )
        .bind(operation_key)
        .fetch_one(pool)
        .await
        .expect("load consumer-owned audit row");
    assert_eq!(source_job_id, expected_source_job_id);
    assert_eq!(result_job_id, expected_result_job_id);
}

async fn assert_consumer_audit_absent(pool: &DbPool, operation_key: &str) {
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
            SELECT 1
            FROM external_consumer_operation_audit
            WHERE operation_key = $1
         )",
    )
    .bind(operation_key)
    .fetch_one(pool)
    .await
    .expect("check consumer audit absence");
    assert!(!exists, "rolled-back consumer audit must not persist");
}

fn assert_successful_replay_event(
    events: &[JobEventRecord],
    expected_replay_job_id: sqlx::types::Uuid,
    expected_source_job_id: sqlx::types::Uuid,
    expected_source_run_number: i32,
) {
    let event = events
        .iter()
        .find(|event| {
            matches!(
                event.decoded_payload(),
                DecodedJobEventPayload::SuccessfulReplayEnqueued(_)
            )
        })
        .expect("successful replay should have a typed ENQUEUED payload");
    assert_eq!(event.job_id, expected_replay_job_id);

    match event.decoded_payload() {
        DecodedJobEventPayload::SuccessfulReplayEnqueued(payload) => {
            assert_eq!(payload.replayed_from_job_id, expected_source_job_id);
            assert_eq!(payload.replayed_from_run_number, expected_source_run_number);
            assert_eq!(payload.replay_request_key, REPLAY_REQUEST_KEY);
            assert_eq!(payload.reason, REPLAY_REASON);
        }
        DecodedJobEventPayload::Requeued(DecodedRequeuedEventPayload::Unknown {
            reason, ..
        }) => panic!(
            "expected successful replay payload, got unknown requeue reason {reason:?}; raw payload: {}",
            event.payload
        ),
        DecodedJobEventPayload::Requeued(_) | DecodedJobEventPayload::Other => {
            panic!(
                "expected successful replay payload; raw payload: {}",
                event.payload
            )
        }
        _ => panic!(
            "expected successful replay payload, got a future decoded variant; raw payload: {}",
            event.payload
        ),
    }
}

fn assert_handler_continuation_event(
    events: &[JobEventRecord],
    expected_job_id: sqlx::types::Uuid,
) {
    let event = events
        .iter()
        .find(|event| {
            matches!(
                event.decoded_payload(),
                DecodedJobEventPayload::Requeued(
                    DecodedRequeuedEventPayload::HandlerContinuation { .. }
                )
            )
        })
        .expect("continuation should have a typed REQUEUED payload");
    assert_eq!(event.job_id, expected_job_id);

    match event.decoded_payload() {
        DecodedJobEventPayload::Requeued(DecodedRequeuedEventPayload::HandlerContinuation {
            reason,
            next_run_number,
            next_run_at,
            delay_microseconds,
            ..
        }) => {
            assert_eq!(reason, HANDLER_CONTINUATION_REASON);
            assert_eq!(next_run_number, 2);
            assert_eq!(delay_microseconds, 25_000);

            let raw_next_run_at = event
                .payload
                .get("next_run_at")
                .and_then(Value::as_str)
                .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                .map(|value| value.with_timezone(&Utc))
                .expect("continuation raw payload should retain next_run_at");
            assert_eq!(next_run_at, raw_next_run_at);
        }
        DecodedJobEventPayload::Requeued(DecodedRequeuedEventPayload::Unknown {
            reason, ..
        }) => panic!(
            "expected handler continuation payload, got unknown requeue reason {reason:?}; raw payload: {}",
            event.payload
        ),
        DecodedJobEventPayload::Requeued(_)
        | DecodedJobEventPayload::SuccessfulReplayEnqueued(_)
        | DecodedJobEventPayload::Other => panic!(
            "expected handler continuation payload; raw payload: {}",
            event.payload
        ),
        _ => panic!(
            "expected handler continuation payload, got a future decoded variant; raw payload: {}",
            event.payload
        ),
    }
}

async fn insert_due_schedule(pool: &DbPool, kind: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO job_schedules (
            name,
            job_type,
            organization_id,
            payload_template,
            cron_expr,
            next_fire_at
         )
         VALUES ($1, $2, $3, $4::jsonb, $5, $6)",
    )
    .bind(format!("external-consumer-{kind}"))
    .bind(SMOKE_JOB_TYPE)
    .bind::<Option<sqlx::types::Uuid>>(None)
    .bind(json!({ "kind": kind }))
    .bind("0 0 0 1 1 * *")
    .bind(Utc::now() - ChronoDuration::seconds(5))
    .execute(pool)
    .await?;

    Ok(())
}

async fn expire_job_lease(pool: &DbPool, job_id: sqlx::types::Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE job_queue
         SET lease_expires_at = now() - interval '10 seconds'
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(pool)
    .await?;

    Ok(())
}

async fn wait_for_promoted_intent(
    pool: &DbPool,
    intent_id: sqlx::types::Uuid,
) -> sqlx::types::Uuid {
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        let intent = get_job_enqueue_intent_by_id(pool, None, intent_id)
            .await
            .expect("load enqueue intent")
            .expect("enqueue intent should exist");
        if intent.status() == JobEnqueueIntentStatus::Promoted {
            return intent
                .promoted_job_id()
                .expect("promoted intent has job id");
        }

        assert_eq!(intent.status(), JobEnqueueIntentStatus::Pending);
        assert!(
            Instant::now() < deadline,
            "timed out waiting for enqueue intent {intent_id} to be promoted"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_status(
    pool: &DbPool,
    job_id: sqlx::types::Uuid,
    expected: JobStatus,
) -> JobQueueRecord {
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        let job = get_job_by_id(pool, None, job_id)
            .await
            .expect("load job by id")
            .expect("job should exist");
        if job.status == expected {
            return job;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for job {job_id} to reach status {expected:?}; last status was {:?}",
            job.status
        );
        sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_running(pool: &DbPool, job_id: sqlx::types::Uuid) -> JobQueueRecord {
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        let job = get_job_by_id(pool, None, job_id)
            .await
            .expect("load running job by id")
            .expect("job should exist");
        if job.status == JobStatus::Leased && job.stage == JobStage::Running {
            return job;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for job {job_id} to start; last status/stage was {:?}/{:?}",
            job.status,
            job.stage
        );
        sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_kind_status(pool: &DbPool, kind: &str, expected: JobStatus) -> JobQueueRecord {
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        let jobs = jobs::list_jobs(
            pool,
            &JobListFilter {
                organization_id: None,
                status: None,
                job_type: Some(SMOKE_JOB_TYPE),
                limit: 32,
                offset: 0,
            },
        )
        .await
        .expect("list smoke jobs");

        if let Some(job) = jobs
            .into_iter()
            .find(|job| payload_kind(&job.payload) == kind && job.status == expected)
        {
            return job;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for smoke job kind `{kind}` to reach status {expected:?}"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_dead_letter(dead_letters: &Arc<Mutex<Vec<String>>>, kind: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        if dead_letters.lock().await.iter().any(|entry| entry == kind) {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for dead-letter hook for `{kind}`"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

fn payload_kind(payload: &Value) -> &str {
    payload
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
}

#[tokio::test]
async fn packaged_prelude_exports_explicit_metric_and_payload_scopes() {
    use runledger_postgres::prelude::{
        JobEnqueueIntentReadMetricsFilter, JobReadScope, JobScope,
        get_job_continuation_metrics_with_scope, get_job_enqueue_intent_metrics_with_scope,
        get_job_metrics_with_scope, get_job_payload_by_idempotency_key_with_scope,
        get_latest_job_payload_for_run_with_scope,
    };
    let (pool, database) = setup_unmigrated_ephemeral_pool("consumer_explicit_scopes", 2).await;
    runledger_postgres::migrate_after_idempotency_cutover(&pool)
        .await
        .expect("packaged explicit scope API succeeds");
    let tenant = Uuid::now_v7();
    for scope in [
        JobReadScope::Global,
        JobReadScope::Organization(tenant),
        JobReadScope::Admin,
    ] {
        assert!(
            get_job_metrics_with_scope(&pool, scope, Some(SMOKE_JOB_TYPE))
                .await
                .expect("packaged explicit scope API succeeds")
                .is_empty()
        );
        assert!(
            get_job_continuation_metrics_with_scope(&pool, scope, Some(SMOKE_JOB_TYPE))
                .await
                .expect("packaged explicit scope API succeeds")
                .is_empty()
        );
        assert!(
            get_job_enqueue_intent_metrics_with_scope(
                &pool,
                &JobEnqueueIntentReadMetricsFilter::new(scope, 10, 0)
                    .with_job_type(JobType::new(SMOKE_JOB_TYPE))
            )
            .await
            .expect("packaged explicit scope API succeeds")
            .is_empty()
        );
    }
    for scope in [JobScope::Global, JobScope::Organization(tenant)] {
        assert_eq!(
            get_job_payload_by_idempotency_key_with_scope(
                &pool,
                scope,
                JobType::new(SMOKE_JOB_TYPE),
                "missing"
            )
            .await
            .expect("packaged explicit scope API succeeds"),
            None
        );
        assert_eq!(
            get_latest_job_payload_for_run_with_scope(
                &pool,
                scope,
                JobType::new(SMOKE_JOB_TYPE),
                Uuid::nil()
            )
            .await
            .expect("packaged explicit scope API succeeds"),
            None
        );
    }
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn opaque_capabilities_preserve_durable_handoff() {
    let (pool, database) =
        runledger_test_support::setup_ephemeral_pool("opaque_intent_consumer", 2).await;
    opaque_intents::verify_schema(&pool).await;
    opaque_intents::atomicity_and_replay(&pool).await;
    teardown_ephemeral_pool(pool, database).await;
}
