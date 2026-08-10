use std::future::pending;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use runledger_core::jobs::{
    JobCompletion, JobContext, JobDeadLetterInfo, JobDeadLetterReason, JobEventType, JobFailure,
    JobFailureKind, JobRetryTiming, JobStage, JobStatus, JobType, JobTypeName, StepKey,
    WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder, WorkflowStepStatus, WorkflowType,
};
use runledger_postgres::jobs::{
    JobCompletionUpdate, JobDefinitionUpsert, JobEnqueue, JobFailureUpdate, JobProgressUpdate,
    claim_prestart_jobs, complete_job_failure, complete_job_success, enqueue_job,
    enqueue_workflow_run, get_job_by_id, heartbeat_job, list_job_events, list_workflow_steps,
    reap_expired_leases, release_unstarted_job_claim, update_job_progress,
    upsert_job_definition_tx,
};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};

use super::completion::{completion_persist_error_diagnostic, compute_retry_delay_ms};
use super::observers::{
    JobObserverLogContext, JobRunningNotification, RunningObserverHandle, TerminalJobObserverEvent,
    TerminalObserverTasks,
};
use super::{
    process_claimed_job, process_claimed_job_with_observer, run_worker_loop,
    run_worker_loop_with_observer,
};
use crate::RuntimeLoopExit;
use crate::config::JobsConfig;
use crate::observer::{
    JobCompletionPersistFailedEvent, JobContinuedEvent, JobFailedEvent, JobFailureDisposition,
    JobLeaseLostEvent, JobLifecycleObserver, JobLifecycleObservers, JobRunningEvent,
    JobSucceededEvent, ObservedJob,
};
use crate::registry::{JobHandler, JobRegistry};
use runledger_test_support::{
    setup_ephemeral_pool_with_untracked_migrations as setup_ephemeral_pool, teardown_ephemeral_pool,
};

struct CountingHandler {
    runs: Arc<AtomicUsize>,
}

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

#[async_trait::async_trait]
impl JobHandler for CountingHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.pre_run_lease_loss")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(JobCompletion::success())
    }
}

struct PersistenceFailureHandler {
    runs: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl JobHandler for PersistenceFailureHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.persistence_failure")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(JobCompletion::success())
    }
}

struct RetryThenSuccessHandler {
    runs: Arc<AtomicUsize>,
}

#[derive(Debug, PartialEq)]
struct ContinuationExecution {
    run_number: i32,
    attempt: i32,
    checkpoint: Option<Value>,
}

struct ContinueThenSuccessHandler {
    executions: Arc<Mutex<Vec<ContinuationExecution>>>,
}

struct ExpireLeaseThenContinueHandler {
    pool: PgPool,
}

struct ExpireLeaseThenSucceedHandler {
    pool: PgPool,
}

struct ExpireLeaseThenFailHandler {
    pool: PgPool,
}

struct InvalidContinuationDelayHandler {
    runs: Arc<AtomicUsize>,
    dead_letters: Arc<Mutex<Vec<JobDeadLetterInfo>>>,
}

struct InvalidCompletionProgressHandler {
    runs: Arc<AtomicUsize>,
    dead_letters: Arc<Mutex<Vec<JobDeadLetterInfo>>>,
}

struct PartialInvalidCompletionProgressHandler {
    runs: Arc<AtomicUsize>,
    dead_letters: Arc<Mutex<Vec<JobDeadLetterInfo>>>,
}

struct PartialInvalidContinuationProgressHandler {
    runs: Arc<AtomicUsize>,
    dead_letters: Arc<Mutex<Vec<JobDeadLetterInfo>>>,
}

struct PanickingHandler {
    runs: Arc<AtomicUsize>,
}

struct LoopSuccessHandler {
    runs: Arc<AtomicUsize>,
}

struct FixedSuccessHandler {
    job_type_name: &'static str,
    completion: JobCompletion,
    runs: Arc<AtomicUsize>,
}

struct RecordingDeadLetterHandler {
    job_type_name: &'static str,
    failure: JobFailure,
    runs: Arc<AtomicUsize>,
    dead_letters: Arc<Mutex<Vec<JobDeadLetterInfo>>>,
}

struct CheckpointingDeadLetterHandler {
    pool: PgPool,
    dead_letter_contexts: Arc<Mutex<Vec<JobContext>>>,
}

struct ControlledDeadLetterFailureHandler {
    job_type_name: &'static str,
    runs: Arc<AtomicUsize>,
    release: Arc<Notify>,
    dead_letter_notified: Arc<Notify>,
}

struct HangingDeadLetterFailureHandler {
    runs: Arc<AtomicUsize>,
    started: Arc<Notify>,
    drops: Arc<AtomicUsize>,
}

struct FailingHandler {
    job_type_name: &'static str,
    failure: JobFailure,
    runs: Arc<AtomicUsize>,
}

struct HangingHandler {
    job_type_name: &'static str,
    runs: Arc<AtomicUsize>,
}

struct SlowSuccessHandler {
    job_type_name: &'static str,
    runs: Arc<AtomicUsize>,
    sleep_for: Duration,
}

struct SlowRunningObserver {
    calls: Arc<AtomicUsize>,
}

struct OrderedLifecycleObserver {
    events: Arc<Mutex<Vec<&'static str>>>,
    running_started: Arc<Notify>,
    release_running: Arc<Notify>,
    terminal_seen: Arc<Notify>,
}

struct BlockingSucceededObserver {
    calls: Arc<AtomicUsize>,
    started: Arc<Notify>,
    release: Arc<Notify>,
}

struct HangingSucceededObserver {
    calls: Arc<AtomicUsize>,
}

struct HangingRunningSucceededObserver {
    running_started: Arc<Notify>,
    succeeded_seen: Arc<Notify>,
    succeeded_calls: Arc<AtomicUsize>,
}

struct HangingDropRunningObserver {
    started: Arc<Notify>,
    drops: Arc<AtomicUsize>,
}

struct DropNotify {
    drops: Arc<AtomicUsize>,
}

impl Drop for DropNotify {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Default)]
struct RecordingObserver {
    running: Arc<Mutex<Vec<JobRunningEvent>>>,
    continued: Arc<Mutex<Vec<JobContinuedEvent>>>,
    succeeded: Arc<Mutex<Vec<JobSucceededEvent>>>,
    failed: Arc<Mutex<Vec<JobFailedEvent>>>,
    persist_failed: Arc<Mutex<Vec<JobCompletionPersistFailedEvent>>>,
    lease_lost: Arc<Mutex<Vec<JobLeaseLostEvent>>>,
}

impl RecordingObserver {
    fn lifecycle_observers(&self) -> JobLifecycleObservers {
        JobLifecycleObservers::from_observer(self.clone())
    }

    fn running(&self) -> Vec<JobRunningEvent> {
        self.running
            .lock()
            .expect("running events lock should not be poisoned")
            .clone()
    }

    fn succeeded(&self) -> Vec<JobSucceededEvent> {
        self.succeeded
            .lock()
            .expect("succeeded events lock should not be poisoned")
            .clone()
    }

    fn continued(&self) -> Vec<JobContinuedEvent> {
        self.continued
            .lock()
            .expect("continued events lock should not be poisoned")
            .clone()
    }

    fn failed(&self) -> Vec<JobFailedEvent> {
        self.failed
            .lock()
            .expect("failed events lock should not be poisoned")
            .clone()
    }

    fn persist_failed(&self) -> Vec<JobCompletionPersistFailedEvent> {
        self.persist_failed
            .lock()
            .expect("persist-failed events lock should not be poisoned")
            .clone()
    }

    fn lease_lost(&self) -> Vec<JobLeaseLostEvent> {
        self.lease_lost
            .lock()
            .expect("lease-lost events lock should not be poisoned")
            .clone()
    }
}

#[async_trait::async_trait]
impl JobHandler for RetryThenSuccessHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.retry_then_success")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        let prior_runs = self.runs.fetch_add(1, Ordering::SeqCst);
        if prior_runs == 0 {
            return Err(JobFailure::retryable(
                "job.test.retry_once",
                "first execution should retry",
            ));
        }

        Ok(JobCompletion::success())
    }
}

#[async_trait::async_trait]
impl JobHandler for ContinueThenSuccessHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.continue_then_success")
    }

    async fn execute(
        &self,
        context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.executions
            .lock()
            .expect("continuation executions lock should not be poisoned")
            .push(ContinuationExecution {
                run_number: context.run_number,
                attempt: context.attempt,
                checkpoint: context.checkpoint,
            });

        if context.run_number == 1 {
            return Ok(JobCompletion::continue_now()
                .progress(1, 2)
                .checkpoint(json!({"cursor": 1})));
        }

        Ok(JobCompletion::success().progress(2, 2))
    }
}

#[async_trait::async_trait]
impl JobHandler for ExpireLeaseThenContinueHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.continuation_lease_loss")
    }

    async fn execute(
        &self,
        context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        expire_job_lease(&self.pool, context.job_id).await;
        Ok(JobCompletion::continue_now())
    }
}

#[async_trait::async_trait]
impl JobHandler for ExpireLeaseThenSucceedHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.success_lease_loss")
    }

    async fn execute(
        &self,
        context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        expire_job_lease(&self.pool, context.job_id).await;
        Ok(JobCompletion::success())
    }
}

#[async_trait::async_trait]
impl JobHandler for ExpireLeaseThenFailHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.failure_lease_loss")
    }

    async fn execute(
        &self,
        context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        expire_job_lease(&self.pool, context.job_id).await;
        Err(JobFailure::terminal(
            "job.test.failure_after_lease_loss",
            "failure completion should observe lease loss",
        ))
    }
}

#[async_trait::async_trait]
impl JobHandler for InvalidContinuationDelayHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.invalid_continuation_delay")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(JobCompletion::continue_after(Duration::from_micros(
            i64::MAX as u64,
        )))
    }

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        _payload: Value,
        dead_letter: JobDeadLetterInfo,
    ) {
        self.dead_letters
            .lock()
            .expect("dead-letter list lock should not be poisoned")
            .push(dead_letter);
    }
}

#[async_trait::async_trait]
impl JobHandler for InvalidCompletionProgressHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.invalid_completion_progress")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(JobCompletion::success().progress(2, 1))
    }

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        _payload: Value,
        dead_letter: JobDeadLetterInfo,
    ) {
        self.dead_letters
            .lock()
            .expect("dead-letter list lock should not be poisoned")
            .push(dead_letter);
    }
}

#[async_trait::async_trait]
impl JobHandler for PartialInvalidCompletionProgressHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.partial_invalid_completion_progress")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        let mut completion = JobCompletion::success();
        completion.progress_done = Some(20);
        Ok(completion)
    }

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        _payload: Value,
        dead_letter: JobDeadLetterInfo,
    ) {
        self.dead_letters
            .lock()
            .expect("dead-letter list lock should not be poisoned")
            .push(dead_letter);
    }
}

#[async_trait::async_trait]
impl JobHandler for PartialInvalidContinuationProgressHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.partial_invalid_continuation_progress")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        let mut completion = JobCompletion::continue_now();
        completion.progress_done = Some(20);
        Ok(completion)
    }

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        _payload: Value,
        dead_letter: JobDeadLetterInfo,
    ) {
        self.dead_letters
            .lock()
            .expect("dead-letter list lock should not be poisoned")
            .push(dead_letter);
    }
}

#[async_trait::async_trait]
impl JobHandler for PanickingHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.handler_panic")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        panic!("panic from main job handler");
    }
}

#[async_trait::async_trait]
impl JobHandler for LoopSuccessHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.handler_panic_successor")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(JobCompletion::success())
    }
}

#[async_trait::async_trait]
impl JobHandler for FixedSuccessHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(self.job_type_name)
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(self.completion.clone())
    }
}

#[async_trait::async_trait]
impl JobHandler for RecordingDeadLetterHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(self.job_type_name)
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Err(self.failure.clone())
    }

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        _payload: Value,
        dead_letter: JobDeadLetterInfo,
    ) {
        self.dead_letters
            .lock()
            .expect("dead-letter list lock should not be poisoned")
            .push(dead_letter);
    }
}

#[async_trait::async_trait]
impl JobHandler for CheckpointingDeadLetterHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.dead_letter_latest_checkpoint")
    }

    async fn execute(
        &self,
        context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        let checkpoint = json!({"cursor": "persisted-during-handler"});
        update_job_progress(
            &self.pool,
            context.job_id,
            context.run_number,
            context.attempt,
            &context.worker_id,
            &JobProgressUpdate {
                stage: None,
                progress_done: Some(1),
                progress_total: Some(2),
                checkpoint: Some(&checkpoint),
            },
        )
        .await
        .expect("persist checkpoint during handler execution");
        Err(JobFailure::terminal(
            "job.test.dead_letter_latest_checkpoint",
            "terminal failure after checkpoint update",
        ))
    }

    async fn on_dead_letter(
        &self,
        context: JobContext,
        _payload: Value,
        _dead_letter: JobDeadLetterInfo,
    ) {
        self.dead_letter_contexts
            .lock()
            .expect("dead-letter contexts lock should not be poisoned")
            .push(context);
    }
}

#[async_trait::async_trait]
impl JobHandler for ControlledDeadLetterFailureHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(self.job_type_name)
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        self.release.notified().await;
        Err(JobFailure::terminal(
            "job.test.controlled_terminal_failure",
            "terminal failure for observer ordering test",
        ))
    }

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        _payload: Value,
        _dead_letter: JobDeadLetterInfo,
    ) {
        self.dead_letter_notified.notify_one();
    }
}

#[async_trait::async_trait]
impl JobHandler for HangingDeadLetterFailureHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.hanging_dead_letter_hook")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Err(JobFailure::terminal(
            "job.test.hanging_dead_letter_hook",
            "terminal failure for hook cancellation test",
        ))
    }

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        _payload: Value,
        _dead_letter: JobDeadLetterInfo,
    ) {
        let _drop_notify = DropNotify {
            drops: self.drops.clone(),
        };
        self.started.notify_one();
        pending::<()>().await;
    }
}

#[async_trait::async_trait]
impl JobHandler for FailingHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(self.job_type_name)
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Err(self.failure.clone())
    }
}

#[async_trait::async_trait]
impl JobHandler for HangingHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(self.job_type_name)
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        pending::<Result<JobCompletion, JobFailure>>().await
    }
}

#[async_trait::async_trait]
impl JobHandler for SlowSuccessHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(self.job_type_name)
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        sleep(self.sleep_for).await;
        Ok(JobCompletion::success())
    }
}

#[async_trait::async_trait]
impl JobLifecycleObserver for SlowRunningObserver {
    async fn on_job_running(&self, _event: JobRunningEvent) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        pending::<()>().await;
    }
}

#[async_trait::async_trait]
impl JobLifecycleObserver for OrderedLifecycleObserver {
    async fn on_job_running(&self, _event: JobRunningEvent) {
        self.events
            .lock()
            .expect("ordered observer events lock should not be poisoned")
            .push("running");
        self.running_started.notify_one();
        self.release_running.notified().await;
    }

    async fn on_job_succeeded(&self, _event: JobSucceededEvent) {
        self.events
            .lock()
            .expect("ordered observer events lock should not be poisoned")
            .push("succeeded");
        self.terminal_seen.notify_one();
    }
}

#[async_trait::async_trait]
impl JobLifecycleObserver for BlockingSucceededObserver {
    async fn on_job_succeeded(&self, _event: JobSucceededEvent) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        self.release.notified().await;
    }
}

#[async_trait::async_trait]
impl JobLifecycleObserver for HangingSucceededObserver {
    async fn on_job_succeeded(&self, _event: JobSucceededEvent) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        pending::<()>().await;
    }
}

#[async_trait::async_trait]
impl JobLifecycleObserver for HangingRunningSucceededObserver {
    async fn on_job_running(&self, _event: JobRunningEvent) {
        self.running_started.notify_one();
        pending::<()>().await;
    }

    async fn on_job_succeeded(&self, _event: JobSucceededEvent) {
        self.succeeded_calls.fetch_add(1, Ordering::SeqCst);
        self.succeeded_seen.notify_one();
    }
}

#[async_trait::async_trait]
impl JobLifecycleObserver for HangingDropRunningObserver {
    async fn on_job_running(&self, _event: JobRunningEvent) {
        let _drop_notify = DropNotify {
            drops: self.drops.clone(),
        };
        self.started.notify_one();
        pending::<()>().await;
    }
}

#[async_trait::async_trait]
impl JobLifecycleObserver for RecordingObserver {
    async fn on_job_running(&self, event: JobRunningEvent) {
        self.running
            .lock()
            .expect("running events lock should not be poisoned")
            .push(event);
    }

    async fn on_job_succeeded(&self, event: JobSucceededEvent) {
        self.succeeded
            .lock()
            .expect("succeeded events lock should not be poisoned")
            .push(event);
    }

    async fn on_job_continued(&self, event: JobContinuedEvent) {
        self.continued
            .lock()
            .expect("continued events lock should not be poisoned")
            .push(event);
    }

    async fn on_job_failed(&self, event: JobFailedEvent) {
        self.failed
            .lock()
            .expect("failed events lock should not be poisoned")
            .push(event);
    }

    async fn on_job_completion_persist_failed(&self, event: JobCompletionPersistFailedEvent) {
        self.persist_failed
            .lock()
            .expect("persist-failed events lock should not be poisoned")
            .push(event);
    }

    async fn on_job_lease_lost(&self, event: JobLeaseLostEvent) {
        self.lease_lost
            .lock()
            .expect("lease-lost events lock should not be poisoned")
            .push(event);
    }
}

struct TerminalHookPanicHandler {
    runs: Arc<AtomicUsize>,
    terminal_failures: Arc<AtomicUsize>,
}

struct TerminalHookHangHandler {
    runs: Arc<AtomicUsize>,
    terminal_failures: Arc<AtomicUsize>,
}

struct RetryDelayOverrideObservation {
    status: JobStatus,
    next_run_at: DateTime<Utc>,
    retry_event_delay_ms: Option<i64>,
    retry_event_requested_retry_at: Option<DateTime<Utc>>,
    retry_event_count: usize,
    failed_event_error_code: Option<String>,
    attempt_retry_delay_ms: Option<i32>,
    default_retry_delay_ms: i32,
    db_now_before: DateTime<Utc>,
    db_now_after: DateTime<Utc>,
    runs: usize,
}

#[async_trait::async_trait]
impl JobHandler for TerminalHookHangHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.terminal_hook_hang")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Err(JobFailure::terminal(
            "job.test.terminal_failure",
            "terminal failure for timeout isolation test",
        ))
    }

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        _payload: Value,
        _dead_letter: JobDeadLetterInfo,
    ) {
        self.terminal_failures.fetch_add(1, Ordering::SeqCst);
        pending::<()>().await;
    }
}

#[async_trait::async_trait]
impl JobHandler for TerminalHookPanicHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.terminal_hook_panic")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Err(JobFailure::terminal(
            "job.test.terminal_failure",
            "terminal failure for panic isolation test",
        ))
    }

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        _payload: Value,
        _dead_letter: JobDeadLetterInfo,
    ) {
        self.terminal_failures.fetch_add(1, Ordering::SeqCst);
        panic!("panic from worker terminal failure hook");
    }
}

async fn claim_one_job(pool: &PgPool, worker_id: &str) -> runledger_postgres::jobs::JobQueueRecord {
    let mut claimed = claim_prestart_jobs(pool, worker_id, 30, 1)
        .await
        .expect("claim jobs");
    claimed.pop().expect("expected one claimed job")
}

fn observer_task_test_job() -> JobObserverLogContext {
    JobObserverLogContext {
        job_id: uuid::Uuid::nil(),
        job_type: "jobs.test.observer_task_cap".to_string(),
        run_number: 1,
        attempt: 1,
    }
}

fn observer_task_observed_job() -> ObservedJob {
    ObservedJob {
        job_id: uuid::Uuid::nil(),
        job_type: JobTypeName::from_static("jobs.test.observer_task_cap"),
        organization_id: None,
        run_number: 1,
        attempt: 1,
        max_attempts: 3,
        worker_id: "worker-observer-task-cap".to_string(),
    }
}

fn observer_task_queue_record() -> runledger_postgres::jobs::JobQueueRecord {
    let now = Utc::now();
    runledger_postgres::jobs::JobQueueRecord {
        id: uuid::Uuid::nil(),
        job_type: JobTypeName::from_static("jobs.test.observer_task_cap"),
        organization_id: None,
        payload: json!({}),
        status: JobStatus::Leased,
        priority: 100,
        run_number: 1,
        attempt: 1,
        max_attempts: 3,
        timeout_seconds: 30,
        next_run_at: now,
        lease_expires_at: Some(now + ChronoDuration::seconds(30)),
        last_heartbeat_at: Some(now),
        worker_id: Some("worker-observer-task-cap".to_string()),
        started_at: Some(now),
        finished_at: None,
        stage: JobStage::Running,
        progress_done: None,
        progress_total: None,
        progress_pct: None,
        checkpoint: None,
        output: None,
        idempotency_key: None,
        status_reason: None,
        last_error_code: None,
        last_error_message: None,
        created_at: now,
        updated_at: now,
    }
}

#[tokio::test]
async fn observer_task_owner_drops_newest_terminal_task_at_cap() {
    let tasks = TerminalObserverTasks::owned_with_max_concurrency(2);
    let job = observer_task_test_job();
    let started = Arc::new(AtomicUsize::new(0));
    let (release_tx, release_rx) = watch::channel(false);

    for _ in 0..2 {
        let started = started.clone();
        let mut release_rx = release_rx.clone();
        tasks
            .spawn_terminal(
                async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    while !*release_rx.borrow() {
                        if release_rx.changed().await.is_err() {
                            break;
                        }
                    }
                },
                &job,
            )
            .await;
    }

    assert!(
        wait_for_counter_at_least(&started, 2, Duration::from_millis(500)).await,
        "initial observer tasks should start"
    );
    assert_eq!(tasks.in_flight_count().await, 2);

    let overflow_started = Arc::new(AtomicUsize::new(0));
    let overflow_started_for_task = overflow_started.clone();
    tasks
        .spawn_terminal(
            async move {
                overflow_started_for_task.fetch_add(1, Ordering::SeqCst);
            },
            &job,
        )
        .await;

    sleep(Duration::from_millis(25)).await;
    assert_eq!(
        overflow_started.load(Ordering::SeqCst),
        0,
        "overflow terminal observer task must be dropped instead of executed"
    );
    assert_eq!(tasks.in_flight_count().await, 2);

    release_tx
        .send(true)
        .expect("release receiver should still exist");
    tasks.drain_for_shutdown().await;
}

#[tokio::test]
async fn observer_task_owner_drains_completed_tasks_before_new_admission() {
    let tasks = TerminalObserverTasks::owned_with_max_concurrency(1);
    let job = observer_task_test_job();
    let first_finished = Arc::new(AtomicUsize::new(0));
    let first_finished_for_task = first_finished.clone();

    tasks
        .spawn_terminal(
            async move {
                first_finished_for_task.fetch_add(1, Ordering::SeqCst);
            },
            &job,
        )
        .await;
    assert!(
        wait_for_counter_at_least(&first_finished, 1, Duration::from_millis(500)).await,
        "first observer task should finish"
    );
    assert_eq!(
        tasks.in_flight_count().await,
        1,
        "completed task should remain in the JoinSet until admission drains it"
    );

    let second_started = Arc::new(AtomicUsize::new(0));
    let second_started_for_task = second_started.clone();
    let (release_tx, mut release_rx) = watch::channel(false);
    tasks
        .spawn_terminal(
            async move {
                second_started_for_task.fetch_add(1, Ordering::SeqCst);
                while !*release_rx.borrow() {
                    if release_rx.changed().await.is_err() {
                        break;
                    }
                }
            },
            &job,
        )
        .await;

    assert!(
        wait_for_counter_at_least(&second_started, 1, Duration::from_millis(500)).await,
        "new observer task should be admitted after completed task is drained"
    );
    assert_eq!(tasks.in_flight_count().await, 1);

    release_tx
        .send(true)
        .expect("release receiver should still exist");
    tasks.drain_for_shutdown().await;
}

#[tokio::test]
async fn observer_task_owner_aborts_running_drain_overflow() {
    let tasks = TerminalObserverTasks::owned_with_max_concurrency(1);
    let job = observer_task_test_job();

    let first_running_started = Arc::new(AtomicUsize::new(0));
    let first_running_started_for_task = first_running_started.clone();
    let first_running_task = tokio::spawn(async move {
        first_running_started_for_task.fetch_add(1, Ordering::SeqCst);
        pending::<()>().await;
    });
    assert!(
        wait_for_counter_at_least(&first_running_started, 1, Duration::from_millis(500)).await,
        "first underlying running observer should start"
    );

    tasks
        .spawn_running(
            RunningObserverHandle::new(first_running_task, job),
            tracing::Span::current(),
        )
        .await;
    assert_eq!(tasks.in_flight_count().await, 1);

    let overflow_running_started = Arc::new(AtomicUsize::new(0));
    let overflow_running_started_for_task = overflow_running_started.clone();
    let drops = Arc::new(AtomicUsize::new(0));
    let drop_notify = DropNotify {
        drops: drops.clone(),
    };
    let overflow_running_task = tokio::spawn(async move {
        let _drop_notify = drop_notify;
        overflow_running_started_for_task.fetch_add(1, Ordering::SeqCst);
        pending::<()>().await;
    });
    assert!(
        wait_for_counter_at_least(&overflow_running_started, 1, Duration::from_millis(500)).await,
        "overflow underlying running observer should start"
    );

    tasks
        .spawn_running(
            RunningObserverHandle::new(overflow_running_task, observer_task_test_job()),
            tracing::Span::current(),
        )
        .await;

    assert!(
        wait_for_counter_at_least(&drops, 1, Duration::from_millis(500)).await,
        "dropped running drain should abort the underlying running observer task"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(tasks.in_flight_count().await, 1);

    tasks.drain_for_shutdown().await;
}

#[tokio::test]
async fn observer_task_owner_runs_terminal_when_running_drains_are_at_cap() {
    let tasks = TerminalObserverTasks::owned_with_max_concurrency(1);
    let running_job = observer_task_test_job();

    let running_started = Arc::new(AtomicUsize::new(0));
    let running_started_for_task = running_started.clone();
    let running_task = tokio::spawn(async move {
        running_started_for_task.fetch_add(1, Ordering::SeqCst);
        pending::<()>().await;
    });
    assert!(
        wait_for_counter_at_least(&running_started, 1, Duration::from_millis(500)).await,
        "underlying running observer should start"
    );

    tasks
        .spawn_running(
            RunningObserverHandle::new(running_task, running_job),
            tracing::Span::current(),
        )
        .await;
    assert_eq!(tasks.in_flight_count().await, 1);

    let terminal_started = Arc::new(AtomicUsize::new(0));
    let terminal_started_for_task = terminal_started.clone();
    tasks
        .spawn_terminal(
            async move {
                terminal_started_for_task.fetch_add(1, Ordering::SeqCst);
            },
            &observer_task_test_job(),
        )
        .await;

    assert!(
        wait_for_counter_at_least(&terminal_started, 1, Duration::from_millis(500)).await,
        "terminal observer should run even while running drains are at cap"
    );

    tasks.drain_for_shutdown().await;
}

#[tokio::test]
async fn empty_lifecycle_observers_skip_running_and_terminal_tasks() {
    let tasks = TerminalObserverTasks::owned_with_max_concurrency(1);
    let observed_job = observer_task_observed_job();
    let observers =
        JobLifecycleObservers::from_arc_observers(Vec::<Arc<dyn JobLifecycleObserver>>::new());
    let mut running_notification =
        JobRunningNotification::spawn(observers.clone(), observed_job.clone());

    assert!(
        running_notification.handle.is_none(),
        "empty observers should not create a running observer task"
    );
    assert_eq!(tasks.in_flight_count().await, 0);

    let queue_record = observer_task_queue_record();
    running_notification
        .spawn_terminal_observer(
            &tasks,
            &queue_record,
            observers,
            TerminalJobObserverEvent::Succeeded(JobSucceededEvent {
                job: observed_job,
                duration: Duration::from_millis(1),
                progress_done: None,
                progress_total: None,
            }),
        )
        .await;

    assert_eq!(
        tasks.in_flight_count().await,
        0,
        "empty observers should not create no-op terminal observer tasks"
    );
}

#[tokio::test]
async fn finished_running_observer_is_reaped_before_terminal_task_admission() {
    let tasks = TerminalObserverTasks::owned_with_max_concurrency(0);
    let observer = RecordingObserver::default();
    let observers = observer.lifecycle_observers();
    let observed_job = observer_task_observed_job();
    let mut running_notification =
        JobRunningNotification::spawn(observers.clone(), observed_job.clone());

    wait_for_observer_count(|| observer.running().len(), 1, Duration::from_millis(500)).await;
    timeout(Duration::from_millis(500), async {
        while !running_notification
            .handle
            .as_ref()
            .is_some_and(|handle| handle.is_finished())
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("running observer task should finish");

    running_notification
        .spawn_terminal_observer(
            &tasks,
            &observer_task_queue_record(),
            observers,
            TerminalJobObserverEvent::Succeeded(JobSucceededEvent {
                job: observed_job,
                duration: Duration::from_millis(1),
                progress_done: None,
                progress_total: None,
            }),
        )
        .await;

    assert!(
        running_notification.handle.is_none(),
        "a finished running callback should be joined inline even when the terminal callback is not admitted"
    );
    assert!(observer.succeeded().is_empty());
    assert_eq!(tasks.in_flight_count().await, 0);
}

#[tokio::test]
async fn terminal_observer_rejection_does_not_abort_same_job_running_observer() {
    let tasks = TerminalObserverTasks::owned_with_max_concurrency(1);
    let cap_job = observer_task_test_job();
    let cap_started = Arc::new(AtomicUsize::new(0));
    let cap_started_for_task = cap_started.clone();
    let (release_tx, mut release_rx) = watch::channel(false);
    assert!(
        tasks
            .spawn_terminal(
                async move {
                    cap_started_for_task.fetch_add(1, Ordering::SeqCst);
                    while !*release_rx.borrow() {
                        if release_rx.changed().await.is_err() {
                            break;
                        }
                    }
                },
                &cap_job,
            )
            .await,
        "initial terminal observer should be admitted"
    );
    assert!(
        wait_for_counter_at_least(&cap_started, 1, Duration::from_millis(500)).await,
        "initial terminal observer should start"
    );
    assert_eq!(tasks.in_flight_count().await, 1);

    let running_started = Arc::new(Notify::new());
    let running_drops = Arc::new(AtomicUsize::new(0));
    let observers = JobLifecycleObservers::from_observer(HangingDropRunningObserver {
        started: running_started.clone(),
        drops: running_drops.clone(),
    });
    let observed_job = observer_task_observed_job();
    let mut running_notification =
        JobRunningNotification::spawn(observers.clone(), observed_job.clone());
    timeout(Duration::from_millis(500), running_started.notified())
        .await
        .expect("running observer should start");

    running_notification
        .spawn_terminal_observer(
            &tasks,
            &observer_task_queue_record(),
            observers,
            TerminalJobObserverEvent::Succeeded(JobSucceededEvent {
                job: observed_job,
                duration: Duration::from_millis(1),
                progress_done: None,
                progress_total: None,
            }),
        )
        .await;

    assert!(
        running_notification.handle.is_some(),
        "rejected terminal admission must not consume the running observer handle"
    );
    sleep(Duration::from_millis(25)).await;
    assert_eq!(
        running_drops.load(Ordering::SeqCst),
        0,
        "rejected terminal admission must not abort the pending running observer"
    );

    release_tx
        .send(true)
        .expect("release receiver should still exist");
    tasks.drain_for_shutdown().await;
    assert_eq!(
        running_drops.load(Ordering::SeqCst),
        0,
        "terminal task cleanup should not abort the still-owned running observer"
    );

    drop(running_notification);
    assert!(
        wait_for_counter_at_least(&running_drops, 1, Duration::from_millis(500)).await,
        "dropping the running notification should still abort the pending running observer"
    );
}

#[tokio::test]
async fn terminal_observer_waits_for_running_observer_for_same_job() {
    let tasks = TerminalObserverTasks::owned_with_max_concurrency(2);
    let events = Arc::new(Mutex::new(Vec::new()));
    let running_started = Arc::new(Notify::new());
    let release_running = Arc::new(Notify::new());
    let terminal_seen = Arc::new(Notify::new());
    let observer = OrderedLifecycleObserver {
        events: events.clone(),
        running_started: running_started.clone(),
        release_running: release_running.clone(),
        terminal_seen: terminal_seen.clone(),
    };
    let observers = JobLifecycleObservers::from_observer(observer);
    let observed_job = observer_task_observed_job();
    let mut running_notification =
        JobRunningNotification::spawn(observers.clone(), observed_job.clone());

    timeout(Duration::from_millis(500), running_started.notified())
        .await
        .expect("running observer should start");

    running_notification
        .spawn_terminal_observer(
            &tasks,
            &observer_task_queue_record(),
            observers,
            TerminalJobObserverEvent::Succeeded(JobSucceededEvent {
                job: observed_job,
                duration: Duration::from_millis(1),
                progress_done: None,
                progress_total: None,
            }),
        )
        .await;

    sleep(Duration::from_millis(25)).await;
    assert_eq!(
        *events
            .lock()
            .expect("ordered observer events lock should not be poisoned"),
        vec!["running"],
        "terminal observer must not run before the running observer completes"
    );

    release_running.notify_one();
    timeout(Duration::from_millis(500), terminal_seen.notified())
        .await
        .expect("terminal observer should run after running observer completes");
    tasks.drain_for_shutdown().await;

    assert_eq!(
        *events
            .lock()
            .expect("ordered observer events lock should not be poisoned"),
        vec!["running", "succeeded"]
    );
}

#[tokio::test]
async fn terminal_observer_shutdown_waits_for_same_job_running_observer() {
    let tasks = TerminalObserverTasks::owned_with_max_concurrency(2);
    let events = Arc::new(Mutex::new(Vec::new()));
    let running_started = Arc::new(Notify::new());
    let release_running = Arc::new(Notify::new());
    let terminal_seen = Arc::new(Notify::new());
    let observer = OrderedLifecycleObserver {
        events: events.clone(),
        running_started: running_started.clone(),
        release_running: release_running.clone(),
        terminal_seen: terminal_seen.clone(),
    };
    let observers = JobLifecycleObservers::from_observer(observer);
    let observed_job = observer_task_observed_job();
    let mut running_notification =
        JobRunningNotification::spawn(observers.clone(), observed_job.clone());

    timeout(Duration::from_millis(500), running_started.notified())
        .await
        .expect("running observer should start");

    running_notification
        .spawn_terminal_observer(
            &tasks,
            &observer_task_queue_record(),
            observers,
            TerminalJobObserverEvent::Succeeded(JobSucceededEvent {
                job: observed_job,
                duration: Duration::from_millis(1),
                progress_done: None,
                progress_total: None,
            }),
        )
        .await;

    let tasks_for_shutdown = tasks.clone();
    let mut drain_task = tokio::spawn(async move {
        tasks_for_shutdown.drain_for_shutdown().await;
    });

    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        *events
            .lock()
            .expect("ordered observer events lock should not be poisoned"),
        vec!["running"],
        "shutdown must not deliver terminal observer before the same-job running observer settles"
    );

    release_running.notify_one();
    timeout(Duration::from_millis(500), terminal_seen.notified())
        .await
        .expect("terminal observer should run after running observer is released");
    await_spawned_task(
        &mut drain_task,
        Duration::from_secs(2),
        "terminal observer shutdown drain should finish after running observer release",
        "terminal observer shutdown drain should not panic",
    )
    .await;

    assert_eq!(
        *events
            .lock()
            .expect("ordered observer events lock should not be poisoned"),
        vec!["running", "succeeded"]
    );
}

#[tokio::test]
async fn terminal_observer_shutdown_delivers_after_multiple_hanging_observer_callbacks() {
    const HANGING_RUNNING_OBSERVERS: usize = 3;
    const HANGING_TERMINAL_OBSERVERS: usize = 3;

    let tasks = TerminalObserverTasks::owned_with_max_concurrency(16);
    let running_calls = Arc::new(AtomicUsize::new(0));
    let hanging_terminal_calls = Arc::new(AtomicUsize::new(0));
    let recording_observer = RecordingObserver::default();
    let mut observer_list: Vec<Arc<dyn JobLifecycleObserver>> = Vec::new();

    for _ in 0..HANGING_RUNNING_OBSERVERS {
        observer_list.push(Arc::new(SlowRunningObserver {
            calls: running_calls.clone(),
        }));
    }

    for _ in 0..HANGING_TERMINAL_OBSERVERS {
        observer_list.push(Arc::new(HangingSucceededObserver {
            calls: hanging_terminal_calls.clone(),
        }));
    }

    observer_list.push(Arc::new(recording_observer.clone()));
    let observers = JobLifecycleObservers::from_arc_observers(observer_list);
    let observed_job = observer_task_observed_job();
    let mut running_notification =
        JobRunningNotification::spawn(observers.clone(), observed_job.clone());

    assert!(
        wait_for_counter_at_least(
            &running_calls,
            HANGING_RUNNING_OBSERVERS,
            Duration::from_millis(90)
        )
        .await,
        "running observer fanout should start every hanging callback before one observer timeout"
    );

    running_notification
        .spawn_terminal_observer(
            &tasks,
            &observer_task_queue_record(),
            observers,
            TerminalJobObserverEvent::Succeeded(JobSucceededEvent {
                job: observed_job,
                duration: Duration::from_millis(1),
                progress_done: None,
                progress_total: None,
            }),
        )
        .await;

    let tasks_for_shutdown = tasks.clone();
    let mut drain_task = tokio::spawn(async move {
        tasks_for_shutdown.drain_for_shutdown().await;
    });
    await_spawned_task(
        &mut drain_task,
        Duration::from_millis(500),
        "terminal observer shutdown drain should finish within the bounded drain policy",
        "terminal observer shutdown drain should not panic",
    )
    .await;

    assert_eq!(
        hanging_terminal_calls.load(Ordering::SeqCst),
        HANGING_TERMINAL_OBSERVERS,
        "terminal observer fanout should start every hanging terminal callback"
    );
    assert_eq!(
        recording_observer.succeeded().len(),
        1,
        "recording terminal observer must be delivered before shutdown returns"
    );
}

#[tokio::test]
async fn run_worker_loop_exits_when_shutdown_sender_is_dropped() {
    // The lazy pool deliberately points at an invalid port. This test asserts
    // the loop observes a closed shutdown channel before attempting any claim.
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://postgres:postgres@127.0.0.1:1/runledger")
        .expect("construct lazy pool");
    let registry = JobRegistry::new();
    let config = JobsConfig {
        worker_id: "dropped-shutdown-worker".to_string(),
        poll_interval: Duration::from_secs(30),
        claim_batch_size: 1,
        lease_ttl_seconds: 30,
        max_global_concurrency: 1,
        reaper_interval: Duration::from_secs(30),
        schedule_poll_interval: Duration::from_secs(30),
        reaper_retry_delay_ms: 1_000,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut worker_task = tokio::spawn(run_worker_loop(pool, registry, config, shutdown_rx));

    drop(shutdown_tx);

    if timeout(Duration::from_millis(200), &mut worker_task)
        .await
        .is_err()
    {
        worker_task.abort();
        let _ = worker_task.await;
        panic!("worker should treat a closed shutdown channel as shutdown");
    }
}

#[tokio::test]
async fn run_worker_loop_waits_for_terminal_observer_on_shutdown() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_terminal_observer_shutdown", 8).await;
    let job_type = JobType::new("jobs.test.handler_panic_successor");

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type,
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type,
            organization_id: None,
            payload: &json!({"kind":"terminal-observer-shutdown"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(LoopSuccessHandler { runs: runs.clone() });

    let observer_calls = Arc::new(AtomicUsize::new(0));
    let observer_started = Arc::new(Notify::new());
    let observer_release = Arc::new(Notify::new());
    let observers = JobLifecycleObservers::from_observer(BlockingSucceededObserver {
        calls: observer_calls.clone(),
        started: observer_started.clone(),
        release: observer_release.clone(),
    });
    let config = JobsConfig {
        worker_id: "terminal-observer-shutdown-worker".to_string(),
        poll_interval: Duration::from_millis(25),
        claim_batch_size: 1,
        lease_ttl_seconds: 30,
        max_global_concurrency: 1,
        reaper_interval: Duration::from_secs(30),
        schedule_poll_interval: Duration::from_secs(30),
        reaper_retry_delay_ms: 1_000,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut worker_task = tokio::spawn(run_worker_loop_with_observer(
        pool.clone(),
        registry,
        config,
        shutdown_rx,
        observers,
    ));

    timeout(Duration::from_secs(5), observer_started.notified())
        .await
        .expect("terminal success observer should start");
    shutdown_tx
        .send(true)
        .expect("shutdown receiver should still be active");

    assert!(
        timeout(Duration::from_millis(25), &mut worker_task)
            .await
            .is_err(),
        "worker loop returned before the terminal success observer was released"
    );

    observer_release.notify_one();
    let exit = await_spawned_task(
        &mut worker_task,
        Duration::from_secs(2),
        "worker loop should return after terminal observer release",
        "worker loop should not panic",
    )
    .await;
    assert_eq!(exit, RuntimeLoopExit::Shutdown);

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after terminal observer shutdown")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Succeeded);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(observer_calls.load(Ordering::SeqCst), 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn run_worker_loop_shutdown_delivers_terminal_after_hanging_running_observer() {
    const JOB_TYPE: &str = "jobs.test.terminal_after_hanging_running_observer";

    let (pool, database) =
        setup_ephemeral_pool("jobs_worker_terminal_after_hanging_running", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(JOB_TYPE),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: None,
            payload: &json!({"kind":"terminal-after-hanging-running"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(FixedSuccessHandler {
        job_type_name: JOB_TYPE,
        completion: JobCompletion::success(),
        runs: runs.clone(),
    });

    let running_started = Arc::new(Notify::new());
    let succeeded_seen = Arc::new(Notify::new());
    let succeeded_calls = Arc::new(AtomicUsize::new(0));
    let observers = JobLifecycleObservers::from_observer(HangingRunningSucceededObserver {
        running_started: running_started.clone(),
        succeeded_seen: succeeded_seen.clone(),
        succeeded_calls: succeeded_calls.clone(),
    });
    let config = JobsConfig {
        worker_id: "terminal-after-hanging-running-worker".to_string(),
        poll_interval: Duration::from_millis(25),
        claim_batch_size: 1,
        lease_ttl_seconds: 30,
        max_global_concurrency: 1,
        reaper_interval: Duration::from_secs(30),
        schedule_poll_interval: Duration::from_secs(30),
        reaper_retry_delay_ms: 1_000,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut worker_task = tokio::spawn(run_worker_loop_with_observer(
        pool.clone(),
        registry,
        config,
        shutdown_rx,
        observers,
    ));

    timeout(Duration::from_secs(5), running_started.notified())
        .await
        .expect("running observer should start");
    wait_for_status(&pool, job_id, JobStatus::Succeeded, Duration::from_secs(5)).await;

    shutdown_tx
        .send(true)
        .expect("shutdown receiver should still be active");
    let exit = await_spawned_task(
        &mut worker_task,
        Duration::from_secs(2),
        "worker loop should wait for terminal observer delivery after running observer timeout",
        "worker loop should not panic",
    )
    .await;
    assert_eq!(exit, RuntimeLoopExit::Shutdown);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(
        succeeded_calls.load(Ordering::SeqCst),
        1,
        "shutdown must not abort the terminal observer before it runs after the running observer timeout"
    );

    timeout(Duration::from_millis(10), succeeded_seen.notified())
        .await
        .expect(
            "terminal observer notification should have been recorded before shutdown returned",
        );

    teardown_ephemeral_pool(pool, database).await;
}

async fn enqueue_and_claim_job(
    pool: &PgPool,
    job_type: JobType<'static>,
    max_attempts: i32,
    payload: Value,
    worker_id: &str,
) -> (uuid::Uuid, runledger_postgres::jobs::JobQueueRecord) {
    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type,
            version: 1,
            max_attempts,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        pool,
        &JobEnqueue {
            job_type,
            organization_id: None,
            payload: &payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let claimed_job = claim_one_job(pool, worker_id).await;
    (job_id, claimed_job)
}

async fn connect_closed_pool(database_url: &str) -> PgPool {
    let worker_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(database_url)
        .await
        .expect("connect worker pool");
    worker_pool.close().await;
    worker_pool
}

async fn expire_job_lease(pool: &PgPool, job_id: uuid::Uuid) {
    sqlx::query(
        "UPDATE job_queue
         SET lease_expires_at = now() - interval '10 seconds'
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(pool)
    .await
    .expect("expire leased job");
}

async fn wait_for_heartbeat_to_block_on_job_lock(pool: &PgPool) {
    for _ in 0..100 {
        let waiting = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_stat_activity
                 WHERE wait_event_type = 'Lock'
                   AND query LIKE '%UPDATE job_queue%'
                   AND query LIKE '%make_interval%'
                   AND query NOT LIKE '%pg_stat_activity%'
             )",
        )
        .fetch_one(pool)
        .await
        .expect("query waiting heartbeat activity");

        if waiting {
            return;
        }

        sleep(Duration::from_millis(50)).await;
    }

    panic!("heartbeat did not block on the job-row lock");
}

async fn wait_for_status(
    pool: &PgPool,
    job_id: uuid::Uuid,
    expected: JobStatus,
    timeout_after: Duration,
) -> runledger_postgres::jobs::JobQueueRecord {
    let deadline = Instant::now() + timeout_after;

    loop {
        let job = get_job_by_id(pool, None, job_id)
            .await
            .expect("load job")
            .expect("job exists");
        if job.status == expected {
            return job;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for {expected:?}; last observed status was {:?}",
            job.status
        );
        sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_counter_at_least(
    counter: &AtomicUsize,
    expected: usize,
    timeout_after: Duration,
) -> bool {
    let deadline = Instant::now() + timeout_after;

    loop {
        if counter.load(Ordering::SeqCst) >= expected {
            return true;
        }

        if Instant::now() >= deadline {
            return false;
        }

        sleep(Duration::from_millis(10)).await;
    }
}

async fn wait_for_observer_count(
    mut count_events: impl FnMut() -> usize,
    expected: usize,
    timeout_after: Duration,
) {
    let deadline = Instant::now() + timeout_after;

    loop {
        let observed = count_events();
        if observed >= expected {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for {expected} observer event(s); last observed count was {observed}"
        );
        sleep(Duration::from_millis(10)).await;
    }
}

fn query_error_code(error: &runledger_postgres::Error) -> Option<&str> {
    match error {
        runledger_postgres::Error::QueryError(query_error) => Some(query_error.code()),
        _ => None,
    }
}

#[test]
fn completion_persist_error_diagnostic_omits_internal_query_details() {
    let error =
        runledger_postgres::Error::QueryError(runledger_postgres::QueryError::from_classified(
            runledger_postgres::QueryErrorCategory::Validation,
            "job.test.persist_failed",
            "Persist failed.",
            "trusted diagnostic detail",
        ));

    let diagnostic = completion_persist_error_diagnostic(&error);

    assert!(diagnostic.contains("client_message=\"Persist failed.\""));
    assert!(diagnostic.contains("code=job.test.persist_failed"));
    assert!(!diagnostic.contains("sqlstate"));
    assert!(!diagnostic.contains("constraint"));
    assert!(!diagnostic.contains("internal_message"));
    assert!(!diagnostic.contains("trusted diagnostic detail"));
}

fn clone_dead_letters(dead_letters: &Arc<Mutex<Vec<JobDeadLetterInfo>>>) -> Vec<JobDeadLetterInfo> {
    dead_letters
        .lock()
        .expect("dead-letter list lock should not be poisoned")
        .clone()
}

async fn database_now(pool: &PgPool) -> DateTime<Utc> {
    sqlx::query_scalar::<_, DateTime<Utc>>("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
        .expect("fetch database now")
}

async fn observe_retry_delay_override_failure<F>(
    database_name: &str,
    handler_job_type: &'static str,
    failure: F,
    max_attempts: i32,
    override_registration: Option<(JobType<'static>, &'static str, i32)>,
) -> RetryDelayOverrideObservation
where
    F: FnOnce(DateTime<Utc>) -> JobFailure,
{
    let (pool, database) = setup_ephemeral_pool(database_name, 8).await;
    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new(handler_job_type),
        max_attempts,
        json!({"kind":"retry-delay-override"}),
        "worker-retry-delay-override",
    )
    .await;

    let default_retry_delay_ms = compute_retry_delay_ms(claimed_job.attempt, claimed_job.id);
    let db_now_before = database_now(&pool).await;
    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(FailingHandler {
        job_type_name: handler_job_type,
        failure: failure(db_now_before),
        runs: runs.clone(),
    });
    if let Some((job_type, failure_code, retry_delay_ms)) = override_registration {
        registry.register_retry_delay_override(job_type, failure_code, retry_delay_ms);
    }

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;
    let db_now_after = database_now(&pool).await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after failure")
        .expect("job exists");
    let events = list_job_events(&pool, None, job_id, 50, None)
        .await
        .expect("list job events");
    let retry_events = events
        .iter()
        .filter(|event| event.event_type == JobEventType::RetryScheduled)
        .collect::<Vec<_>>();
    let retry_event_delay_ms = retry_events
        .first()
        .and_then(|event| event.payload.get("retry_delay_ms"))
        .and_then(Value::as_i64);
    let retry_event_requested_retry_at = retry_events
        .first()
        .and_then(|event| event.payload.get("requested_retry_not_before"))
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<DateTime<Utc>>().ok());
    let failed_event_error_code = events
        .iter()
        .find(|event| event.event_type == JobEventType::Failed)
        .and_then(|event| event.payload.get("error_code"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let attempt_retry_delay_ms = sqlx::query_scalar::<_, Option<i32>>(
        "SELECT retry_delay_ms
         FROM job_attempts
         WHERE job_id = $1
           AND run_number = 1
           AND attempt = 1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("fetch attempt retry delay");

    let observation = RetryDelayOverrideObservation {
        status: persisted.status,
        next_run_at: persisted.next_run_at,
        retry_event_delay_ms,
        retry_event_requested_retry_at,
        retry_event_count: retry_events.len(),
        failed_event_error_code,
        attempt_retry_delay_ms,
        default_retry_delay_ms,
        db_now_before,
        db_now_after,
        runs: runs.load(Ordering::SeqCst),
    };

    teardown_ephemeral_pool(pool, database).await;
    observation
}

#[tokio::test]
async fn process_claimed_job_observer_reports_success_after_commit() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_observer_success", 8).await;
    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.handler_panic_successor"),
        3,
        json!({"kind":"observer-success"}),
        "worker-observer-success",
    )
    .await;
    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(LoopSuccessHandler { runs: runs.clone() });
    let observer = RecordingObserver::default();

    process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        30,
        observer.lifecycle_observers(),
    )
    .await;
    wait_for_observer_count(|| observer.succeeded().len(), 1, Duration::from_millis(500)).await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after success")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Succeeded);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(observer.running().len(), 1);
    let succeeded = observer.succeeded();
    assert_eq!(succeeded.len(), 1);
    assert_eq!(succeeded[0].job.job_id, job_id);
    assert_eq!(succeeded[0].job.worker_id, "worker-observer-success");
    assert!(observer.failed().is_empty());
    assert!(observer.persist_failed().is_empty());
    assert!(observer.lease_lost().is_empty());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn handler_continuation_reuses_the_job_with_a_fresh_attempt_budget() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_handler_continuation", 8).await;
    let (job_id, first_claim) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.continue_then_success"),
        3,
        json!({"kind": "continuation"}),
        "worker-continuation-first",
    )
    .await;
    let executions = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(ContinueThenSuccessHandler {
        executions: executions.clone(),
    });
    let registry = Arc::new(registry);
    let observer = RecordingObserver::default();

    process_claimed_job_with_observer(
        pool.clone(),
        registry.clone(),
        first_claim,
        30,
        observer.lifecycle_observers(),
    )
    .await;
    wait_for_observer_count(|| observer.continued().len(), 1, Duration::from_millis(500)).await;

    let continued = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load continued job")
        .expect("continued job exists");
    assert_eq!(continued.status, JobStatus::Pending);
    assert_eq!(continued.run_number, 2);
    assert_eq!(continued.attempt, 0);
    assert_eq!(continued.progress_done, Some(1));
    assert_eq!(continued.progress_total, Some(2));
    assert_eq!(continued.checkpoint, Some(json!({"cursor": 1})));
    let continued_events = observer.continued();
    assert_eq!(continued_events.len(), 1);
    assert_eq!(continued_events[0].job.job_id, job_id);
    assert_eq!(continued_events[0].job.run_number, 1);
    assert_eq!(continued_events[0].job.attempt, 1);
    assert_eq!(continued_events[0].next_run_number, 2);
    assert_eq!(continued_events[0].progress_done, Some(1));
    assert_eq!(continued_events[0].progress_total, Some(2));
    assert_eq!(observer.running().len(), 1);
    assert!(observer.succeeded().is_empty());
    assert!(observer.failed().is_empty());
    assert!(observer.persist_failed().is_empty());

    let second_claim = claim_one_job(&pool, "worker-continuation-second").await;
    assert_eq!(second_claim.id, job_id);
    assert_eq!(second_claim.run_number, 2);
    assert_eq!(second_claim.attempt, 1);
    process_claimed_job_with_observer(
        pool.clone(),
        registry,
        second_claim,
        30,
        observer.lifecycle_observers(),
    )
    .await;
    wait_for_observer_count(|| observer.succeeded().len(), 1, Duration::from_millis(500)).await;

    let succeeded = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load succeeded job")
        .expect("succeeded job exists");
    assert_eq!(succeeded.status, JobStatus::Succeeded);
    assert_eq!(succeeded.run_number, 2);
    assert_eq!(succeeded.attempt, 1);
    assert_eq!(succeeded.progress_done, Some(2));
    assert_eq!(succeeded.progress_total, Some(2));
    assert_eq!(succeeded.checkpoint, Some(json!({"cursor": 1})));
    assert_eq!(
        *executions
            .lock()
            .expect("continuation executions lock should not be poisoned"),
        vec![
            ContinuationExecution {
                run_number: 1,
                attempt: 1,
                checkpoint: None,
            },
            ContinuationExecution {
                run_number: 2,
                attempt: 1,
                checkpoint: Some(json!({"cursor": 1})),
            },
        ]
    );
    assert_eq!(observer.succeeded().len(), 1);
    assert_eq!(observer.continued().len(), 1);
    assert_eq!(observer.running().len(), 2);
    assert!(observer.failed().is_empty());

    let attempts = sqlx::query_as::<_, (i32, i32, bool, Option<String>)>(
        "SELECT run_number, attempt, finished_at IS NOT NULL, outcome::text
         FROM job_attempts
         WHERE job_id = $1
         ORDER BY run_number, attempt",
    )
    .bind(job_id)
    .fetch_all(&pool)
    .await
    .expect("load continuation attempts");
    assert_eq!(attempts, vec![(1, 1, true, None), (2, 1, true, None)]);

    let events = list_job_events(&pool, None, job_id, 20, None)
        .await
        .expect("list continuation lifecycle events");
    let continuation_event = events
        .iter()
        .find(|event| event.event_type == JobEventType::Requeued)
        .expect("handler continuation should write a requeued event");
    assert_eq!(
        continuation_event
            .payload
            .get("reason")
            .and_then(Value::as_str),
        Some("HANDLER_CONTINUATION")
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn continuation_lease_mismatch_reports_lease_loss_instead_of_persist_failure() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_continuation_lease_loss", 8).await;
    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.continuation_lease_loss"),
        3,
        json!({"kind": "continuation-lease-loss"}),
        "worker-continuation-lease-loss",
    )
    .await;
    let mut registry = JobRegistry::new();
    registry.register(ExpireLeaseThenContinueHandler { pool: pool.clone() });
    let observer = RecordingObserver::default();

    process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        30,
        observer.lifecycle_observers(),
    )
    .await;
    wait_for_observer_count(
        || observer.lease_lost().len(),
        1,
        Duration::from_millis(500),
    )
    .await;

    let lease_lost = observer.lease_lost();
    assert_eq!(lease_lost.len(), 1);
    assert_eq!(lease_lost[0].job.job_id, job_id);
    assert_eq!(lease_lost[0].failure.kind, JobFailureKind::LeaseExpired);
    assert_eq!(lease_lost[0].failure.code, "job.lease_owner_mismatch");
    assert!(observer.persist_failed().is_empty());
    assert!(observer.continued().is_empty());

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load expired continuation lease")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Leased);
    assert_eq!(persisted.run_number, 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn success_and_failure_lease_mismatches_report_lease_loss() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_terminal_lease_loss", 8).await;
    let observer = RecordingObserver::default();

    let (success_job_id, success_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.success_lease_loss"),
        3,
        json!({"kind": "success-lease-loss"}),
        "worker-success-lease-loss",
    )
    .await;
    let mut success_registry = JobRegistry::new();
    success_registry.register(ExpireLeaseThenSucceedHandler { pool: pool.clone() });
    process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(success_registry),
        success_job,
        30,
        observer.lifecycle_observers(),
    )
    .await;

    let (failure_job_id, failure_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.failure_lease_loss"),
        3,
        json!({"kind": "failure-lease-loss"}),
        "worker-failure-lease-loss",
    )
    .await;
    let mut failure_registry = JobRegistry::new();
    failure_registry.register(ExpireLeaseThenFailHandler { pool: pool.clone() });
    process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(failure_registry),
        failure_job,
        30,
        observer.lifecycle_observers(),
    )
    .await;

    wait_for_observer_count(
        || observer.lease_lost().len(),
        2,
        Duration::from_millis(500),
    )
    .await;
    let lease_lost = observer.lease_lost();
    assert_eq!(lease_lost.len(), 2);
    assert!(
        lease_lost
            .iter()
            .any(|event| event.job.job_id == success_job_id)
    );
    assert!(
        lease_lost
            .iter()
            .any(|event| event.job.job_id == failure_job_id)
    );
    assert!(
        lease_lost
            .iter()
            .all(|event| event.failure.kind == JobFailureKind::LeaseExpired)
    );
    assert!(observer.persist_failed().is_empty());
    assert!(observer.succeeded().is_empty());
    assert!(observer.failed().is_empty());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn opted_in_workflow_managed_handler_continuation_runs_again_then_succeeds() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_workflow_continuation", 8).await;
    let job_type = JobType::new("jobs.test.continue_then_success");
    let mut tx = pool.begin().await.expect("begin definition transaction");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type,
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert workflow job definition");
    tx.commit().await.expect("commit definition transaction");

    let payload = json!({"kind": "workflow-continuation"});
    let metadata = json!({"test": "workflow-continuation"});
    let step = WorkflowStepEnqueueBuilder::new(StepKey::new("step"), job_type, &payload)
        .allow_handler_continuation()
        .try_build()
        .expect("build workflow step");
    let workflow =
        WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test.continuation"), &metadata)
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

    let executions = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(ContinueThenSuccessHandler {
        executions: executions.clone(),
    });
    let registry = Arc::new(registry);
    process_claimed_job(pool.clone(), registry.clone(), claim, 30).await;

    let continued = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load workflow job")
        .expect("workflow job exists");
    assert_eq!(continued.status, JobStatus::Pending);
    assert_eq!(continued.run_number, 2);
    assert_eq!(continued.attempt, 0);
    assert_eq!(continued.progress_done, Some(1));
    assert_eq!(continued.progress_total, Some(2));
    assert_eq!(continued.checkpoint, Some(json!({"cursor": 1})));
    assert_eq!(
        *executions
            .lock()
            .expect("continuation executions lock should not be poisoned"),
        vec![ContinuationExecution {
            run_number: 1,
            attempt: 1,
            checkpoint: None,
        }]
    );
    let continued_steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list continued workflow steps");
    assert_eq!(continued_steps[0].status, WorkflowStepStatus::Enqueued);

    let second_claim = claim_one_job(&pool, "worker-workflow-continuation-final").await;
    assert_eq!(second_claim.id, job_id);
    assert_eq!(second_claim.run_number, 2);
    assert_eq!(second_claim.attempt, 1);
    process_claimed_job(pool.clone(), registry, second_claim, 30).await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load terminal workflow job")
        .expect("workflow job exists");
    assert_eq!(persisted.status, JobStatus::Succeeded);
    assert_eq!(persisted.run_number, 2);
    assert_eq!(persisted.attempt, 1);
    assert_eq!(
        *executions
            .lock()
            .expect("continuation executions lock should not be poisoned"),
        vec![
            ContinuationExecution {
                run_number: 1,
                attempt: 1,
                checkpoint: None,
            },
            ContinuationExecution {
                run_number: 2,
                attempt: 1,
                checkpoint: Some(json!({"cursor": 1})),
            },
        ]
    );
    let steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list terminal workflow steps");
    assert_eq!(steps[0].status, WorkflowStepStatus::Succeeded);
    assert_eq!(
        list_job_events(&pool, None, job_id, 20, None)
            .await
            .expect("list workflow job events")
            .iter()
            .filter(|event| event.event_type == JobEventType::Requeued)
            .count(),
        1
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_success_observer_reports_committed_coalesced_progress() {
    const JOB_TYPE: &str = "jobs.test.observer.coalesced_success_progress";

    let (pool, database) = setup_ephemeral_pool("jobs_worker_observer_success_progress", 8).await;
    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new(JOB_TYPE),
        3,
        json!({"kind":"observer-success-progress"}),
        "worker-observer-success-progress",
    )
    .await;
    let worker_id = claimed_job
        .worker_id
        .clone()
        .expect("claimed job has worker id");
    update_job_progress(
        &pool,
        claimed_job.id,
        claimed_job.run_number,
        claimed_job.attempt,
        &worker_id,
        &JobProgressUpdate {
            stage: None,
            progress_done: Some(5),
            progress_total: Some(10),
            checkpoint: None,
        },
    )
    .await
    .expect("persist existing progress before success");

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(FixedSuccessHandler {
        job_type_name: JOB_TYPE,
        completion: {
            let mut completion = JobCompletion::success();
            completion.progress_done = Some(7);
            completion
        },
        runs: runs.clone(),
    });
    let observer = RecordingObserver::default();

    process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        30,
        observer.lifecycle_observers(),
    )
    .await;
    wait_for_observer_count(|| observer.succeeded().len(), 1, Duration::from_millis(500)).await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after success")
        .expect("job exists");
    let succeeded = observer.succeeded();
    assert_eq!(succeeded.len(), 1);
    assert_eq!(persisted.status, JobStatus::Succeeded);
    assert_eq!(persisted.progress_done, Some(7));
    assert_eq!(persisted.progress_total, Some(10));
    assert_eq!(succeeded[0].progress_done, persisted.progress_done);
    assert_eq!(succeeded[0].progress_total, persisted.progress_total);
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_does_not_block_handler_or_heartbeats_on_slow_running_observers() {
    const JOB_TYPE: &str = "jobs.test.slow_running_observer_heartbeat";
    const LEASE_TTL_SECONDS: i32 = 2;
    const SLOW_RUNNING_OBSERVERS: usize = 25;

    let (pool, database) = setup_ephemeral_pool("jobs_worker_slow_running_observer", 8).await;
    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(JOB_TYPE),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 10,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: None,
            payload: &json!({"kind":"slow-running-observer-heartbeat"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");
    let mut claimed =
        claim_prestart_jobs(&pool, "worker-slow-running-observer", LEASE_TTL_SECONDS, 1)
            .await
            .expect("claim job with short lease");
    let claimed_job = claimed.pop().expect("expected one claimed job");

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(SlowSuccessHandler {
        job_type_name: JOB_TYPE,
        runs: runs.clone(),
        sleep_for: Duration::from_millis(2_200),
    });

    let running_calls = Arc::new(AtomicUsize::new(0));
    let observers: Vec<Arc<dyn JobLifecycleObserver>> = (0..SLOW_RUNNING_OBSERVERS)
        .map(|_| {
            Arc::new(SlowRunningObserver {
                calls: running_calls.clone(),
            }) as Arc<dyn JobLifecycleObserver>
        })
        .collect();
    let observers = JobLifecycleObservers::from_arc_observers(observers);

    let mut job_task = tokio::spawn(process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        LEASE_TTL_SECONDS,
        observers,
    ));

    if !wait_for_counter_at_least(&runs, 1, Duration::from_millis(500)).await {
        job_task.abort();
        let _ = job_task.await;
        teardown_ephemeral_pool(pool, database).await;
        panic!("handler should start before slow running observers serially time out");
    }

    await_spawned_task(
        &mut job_task,
        Duration::from_secs(8),
        "job processing should finish without waiting for slow running observers to time out",
        "job processing should not panic",
    )
    .await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after slow running observer test")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Succeeded);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert!(
        running_calls.load(Ordering::SeqCst) >= 1,
        "running observer fanout should have started"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_dead_letter_hook_is_not_delayed_by_slow_running_observers() {
    const JOB_TYPE: &str = "jobs.test.dead_letter_not_delayed_by_running_observer";
    const SLOW_RUNNING_OBSERVERS: usize = 8;

    let (pool, database) = setup_ephemeral_pool("jobs_worker_dead_letter_observer_order", 8).await;
    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new(JOB_TYPE),
        3,
        json!({"kind":"dead-letter-observer-order"}),
        "worker-dead-letter-observer-order",
    )
    .await;

    let runs = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let dead_letter_notified = Arc::new(Notify::new());
    let mut registry = JobRegistry::new();
    registry.register(ControlledDeadLetterFailureHandler {
        job_type_name: JOB_TYPE,
        runs: runs.clone(),
        release: release.clone(),
        dead_letter_notified: dead_letter_notified.clone(),
    });

    let running_calls = Arc::new(AtomicUsize::new(0));
    let observers: Vec<Arc<dyn JobLifecycleObserver>> = (0..SLOW_RUNNING_OBSERVERS)
        .map(|_| {
            Arc::new(SlowRunningObserver {
                calls: running_calls.clone(),
            }) as Arc<dyn JobLifecycleObserver>
        })
        .collect();
    let observers = JobLifecycleObservers::from_arc_observers(observers);

    let mut job_task = tokio::spawn(process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        30,
        observers,
    ));

    assert!(
        wait_for_counter_at_least(&runs, 1, Duration::from_millis(500)).await,
        "handler should start"
    );
    assert!(
        wait_for_counter_at_least(&running_calls, 1, Duration::from_millis(500)).await,
        "running observer should start"
    );

    release.notify_waiters();
    timeout(Duration::from_millis(500), dead_letter_notified.notified())
        .await
        .expect("dead-letter hook should not wait for running observer timeouts");
    await_spawned_task(
        &mut job_task,
        Duration::from_millis(500),
        "worker task should complete after terminal dead-letter hook",
        "worker task should not panic",
    )
    .await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_observer_reports_retryable_failure_after_commit() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_observer_retry_failure", 8).await;
    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.observer.retry_failure"),
        3,
        json!({"kind":"observer-retry-failure"}),
        "worker-observer-retry-failure",
    )
    .await;
    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(FailingHandler {
        job_type_name: "jobs.test.observer.retry_failure",
        failure: JobFailure::retryable("job.test.retry", "retryable failure"),
        runs: runs.clone(),
    });
    let observer = RecordingObserver::default();

    process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        30,
        observer.lifecycle_observers(),
    )
    .await;
    wait_for_observer_count(|| observer.failed().len(), 1, Duration::from_millis(500)).await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after retryable failure")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Pending);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(observer.running().len(), 1);
    let failed = observer.failed();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].job.job_id, job_id);
    assert_eq!(failed[0].failure.kind, JobFailureKind::Retryable);
    assert_eq!(failed[0].failure.code, "job.test.retry");
    assert!(matches!(
        failed[0].disposition,
        JobFailureDisposition::RetryScheduled { .. }
    ));
    assert!(observer.succeeded().is_empty());
    assert!(observer.persist_failed().is_empty());
    assert!(observer.lease_lost().is_empty());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_observer_reports_absolute_retry_time_after_commit() {
    let (pool, database) =
        setup_ephemeral_pool("jobs_worker_observer_absolute_retry_failure", 8).await;
    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.observer.absolute_retry_failure"),
        3,
        json!({"kind":"observer-absolute-retry-failure"}),
        "worker-observer-absolute-retry-failure",
    )
    .await;
    let requested_retry_at = database_now(&pool).await + ChronoDuration::minutes(5);
    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(FailingHandler {
        job_type_name: "jobs.test.observer.absolute_retry_failure",
        failure: JobFailure::retryable(
            "job.test.provider_rate_limited",
            "provider supplied an absolute reset time",
        )
        .retry_not_before(requested_retry_at),
        runs: runs.clone(),
    });
    let observer = RecordingObserver::default();

    process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        30,
        observer.lifecycle_observers(),
    )
    .await;
    wait_for_observer_count(|| observer.failed().len(), 1, Duration::from_millis(500)).await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after absolute retryable failure")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Pending);
    assert_eq!(persisted.next_run_at, requested_retry_at);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let failed = observer.failed();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].job.job_id, job_id);
    assert_eq!(failed[0].failure.kind, JobFailureKind::Retryable);
    assert_eq!(failed[0].failure.code, "job.test.provider_rate_limited");
    assert_eq!(
        failed[0].disposition,
        JobFailureDisposition::RetryScheduledAt {
            requested_retry_at,
            next_run_at: requested_retry_at,
        }
    );
    assert!(observer.succeeded().is_empty());
    assert!(observer.persist_failed().is_empty());
    assert!(observer.lease_lost().is_empty());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_observer_reports_dead_letter_failure_from_completion_outcome() {
    let (pool, database) =
        setup_ephemeral_pool("jobs_worker_observer_dead_letter_failure", 8).await;
    let (job_id, mut claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.observer.dead_letter_failure"),
        1,
        json!({"kind":"observer-dead-letter-failure"}),
        "worker-observer-dead-letter-failure",
    )
    .await;
    claimed_job.max_attempts = 99;

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(FailingHandler {
        job_type_name: "jobs.test.observer.dead_letter_failure",
        failure: JobFailure::retryable(
            "job.test.retryable_exhausted",
            "retryable failure should exhaust attempts",
        )
        .retry_not_before_delay(Duration::ZERO),
        runs: runs.clone(),
    });
    let observer = RecordingObserver::default();

    process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        30,
        observer.lifecycle_observers(),
    )
    .await;
    wait_for_observer_count(|| observer.failed().len(), 1, Duration::from_millis(500)).await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after dead-letter failure")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let failed = observer.failed();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].job.job_id, job_id);
    assert_eq!(failed[0].job.max_attempts, 1);
    assert_eq!(failed[0].failure.kind, JobFailureKind::Retryable);
    assert_eq!(failed[0].failure.code, "job.test.retryable_exhausted");
    assert_eq!(
        failed[0].disposition,
        JobFailureDisposition::DeadLettered {
            reason: JobDeadLetterReason::AttemptsExhausted,
        }
    );
    assert!(observer.succeeded().is_empty());
    assert!(observer.persist_failed().is_empty());
    assert!(observer.lease_lost().is_empty());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_observer_reports_timeout_failure_after_commit() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_observer_timeout_failure", 8).await;
    let job_type = JobType::new("jobs.test.observer.timeout");
    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type,
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 1,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type,
            organization_id: None,
            payload: &json!({"kind":"observer-timeout"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: Some(1),
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");
    let claimed_job = claim_one_job(&pool, "worker-observer-timeout").await;
    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(HangingHandler {
        job_type_name: "jobs.test.observer.timeout",
        runs: runs.clone(),
    });
    let observer = RecordingObserver::default();

    process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        30,
        observer.lifecycle_observers(),
    )
    .await;
    wait_for_observer_count(|| observer.failed().len(), 1, Duration::from_millis(500)).await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after timeout")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Pending);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(observer.running().len(), 1);
    let failed = observer.failed();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].job.job_id, job_id);
    assert_eq!(failed[0].failure.kind, JobFailureKind::Timeout);
    assert_eq!(failed[0].failure.code, "job.timeout_exceeded");
    assert!(matches!(
        failed[0].disposition,
        JobFailureDisposition::RetryScheduled { .. }
    ));
    assert!(observer.succeeded().is_empty());
    assert!(observer.persist_failed().is_empty());
    assert!(observer.lease_lost().is_empty());

    teardown_ephemeral_pool(pool, database).await;
}

fn assert_next_run_at_around_delay(observation: &RetryDelayOverrideObservation, delay_ms: i32) {
    let lower_bound = observation.db_now_before + ChronoDuration::milliseconds(i64::from(delay_ms));
    let upper_bound = observation.db_now_after
        + ChronoDuration::milliseconds(i64::from(delay_ms))
        + ChronoDuration::seconds(1);

    assert!(
        observation.next_run_at >= lower_bound && observation.next_run_at <= upper_bound,
        "expected next_run_at {} to be between {} and {}",
        observation.next_run_at,
        lower_bound,
        upper_bound
    );
}

#[tokio::test]
async fn process_claimed_job_uses_registered_retry_delay_override() {
    const OVERRIDE_RETRY_DELAY_MS: i32 = 120_000;

    let observation = observe_retry_delay_override_failure(
        "jobs_worker_retry_override",
        "jobs.test.retry_override",
        |_| {
            JobFailure::retryable(
                "job.test.waiting_for_external_refresh",
                "waiting for external refresh",
            )
        },
        3,
        Some((
            JobType::new("jobs.test.retry_override"),
            "job.test.waiting_for_external_refresh",
            OVERRIDE_RETRY_DELAY_MS,
        )),
    )
    .await;

    assert_eq!(observation.runs, 1);
    assert_eq!(observation.status, JobStatus::Pending);
    assert_eq!(
        observation.retry_event_delay_ms,
        Some(i64::from(OVERRIDE_RETRY_DELAY_MS))
    );
    assert_eq!(
        observation.attempt_retry_delay_ms,
        Some(OVERRIDE_RETRY_DELAY_MS)
    );
    assert_next_run_at_around_delay(&observation, OVERRIDE_RETRY_DELAY_MS);
}

#[tokio::test]
async fn process_claimed_job_handler_retry_after_cannot_shorten_registered_override() {
    const OVERRIDE_RETRY_DELAY_MS: i32 = 120_000;

    let observation = observe_retry_delay_override_failure(
        "jobs_worker_handler_retry_after",
        "jobs.test.handler_retry_after",
        |_| {
            JobFailure::retryable(
                "job.test.waiting_for_external_refresh",
                "provider supplied a relative reset delay",
            )
            .retry_not_before_delay(Duration::from_secs(45))
        },
        3,
        Some((
            JobType::new("jobs.test.handler_retry_after"),
            "job.test.waiting_for_external_refresh",
            OVERRIDE_RETRY_DELAY_MS,
        )),
    )
    .await;

    assert_eq!(observation.runs, 1);
    assert_eq!(observation.status, JobStatus::Pending);
    assert_eq!(
        observation.retry_event_delay_ms,
        Some(i64::from(OVERRIDE_RETRY_DELAY_MS))
    );
    assert!(observation.retry_event_requested_retry_at.is_some());
    assert_eq!(observation.retry_event_count, 1);
    assert_eq!(
        observation.attempt_retry_delay_ms,
        Some(OVERRIDE_RETRY_DELAY_MS)
    );
    assert_next_run_at_around_delay(&observation, OVERRIDE_RETRY_DELAY_MS);
}

#[tokio::test]
async fn process_claimed_job_handler_not_before_sets_effective_schedule_beyond_override() {
    const OVERRIDE_RETRY_DELAY_MS: i32 = 120_000;

    let observation = observe_retry_delay_override_failure(
        "jobs_worker_handler_retry_at",
        "jobs.test.handler_retry_at",
        |db_now_before| {
            JobFailure::retryable(
                "job.test.waiting_for_external_refresh",
                "provider supplied an absolute reset time",
            )
            .retry_not_before(db_now_before + ChronoDuration::minutes(5))
        },
        3,
        Some((
            JobType::new("jobs.test.handler_retry_at"),
            "job.test.waiting_for_external_refresh",
            OVERRIDE_RETRY_DELAY_MS,
        )),
    )
    .await;

    let requested_retry_at = observation
        .retry_event_requested_retry_at
        .expect("absolute retry event should record the provider reset time");
    assert_eq!(observation.runs, 1);
    assert_eq!(observation.status, JobStatus::Pending);
    assert_eq!(
        observation.retry_event_delay_ms,
        Some(i64::from(OVERRIDE_RETRY_DELAY_MS))
    );
    assert_eq!(observation.retry_event_count, 1);
    assert_eq!(
        observation.attempt_retry_delay_ms,
        Some(OVERRIDE_RETRY_DELAY_MS)
    );
    assert_eq!(observation.next_run_at, requested_retry_at);
    assert_eq!(
        requested_retry_at,
        observation.db_now_before + ChronoDuration::minutes(5)
    );
}

#[tokio::test]
async fn process_claimed_job_zero_handler_retry_bound_falls_back_to_override() {
    const OVERRIDE_RETRY_DELAY_MS: i32 = 120_000;

    let observation = observe_retry_delay_override_failure(
        "jobs_worker_invalid_handler_retry_timing",
        "jobs.test.invalid_handler_retry_timing",
        |_| {
            JobFailure::retryable(
                "job.test.waiting_for_external_refresh",
                "provider supplied an empty reset delay",
            )
            .retry_not_before_delay(Duration::ZERO)
        },
        3,
        Some((
            JobType::new("jobs.test.invalid_handler_retry_timing"),
            "job.test.waiting_for_external_refresh",
            OVERRIDE_RETRY_DELAY_MS,
        )),
    )
    .await;

    assert_eq!(observation.runs, 1);
    assert_eq!(observation.status, JobStatus::Pending);
    assert_eq!(observation.retry_event_count, 1);
    assert_eq!(
        observation.retry_event_delay_ms,
        Some(i64::from(OVERRIDE_RETRY_DELAY_MS))
    );
    assert_eq!(observation.retry_event_requested_retry_at, None);
    assert_eq!(
        observation.attempt_retry_delay_ms,
        Some(OVERRIDE_RETRY_DELAY_MS)
    );
    assert_eq!(
        observation.failed_event_error_code.as_deref(),
        Some("job.test.waiting_for_external_refresh")
    );
}

#[tokio::test]
async fn process_claimed_job_does_not_apply_override_to_other_job_type() {
    const OVERRIDE_RETRY_DELAY_MS: i32 = 120_000;

    let observation = observe_retry_delay_override_failure(
        "jobs_worker_retry_override_type",
        "jobs.test.retry_override.other",
        |_| {
            JobFailure::retryable(
                "job.test.waiting_for_external_refresh",
                "waiting for external refresh",
            )
        },
        3,
        Some((
            JobType::new("jobs.test.retry_override"),
            "job.test.waiting_for_external_refresh",
            OVERRIDE_RETRY_DELAY_MS,
        )),
    )
    .await;

    assert_eq!(observation.status, JobStatus::Pending);
    assert_ne!(
        observation.retry_event_delay_ms,
        Some(i64::from(OVERRIDE_RETRY_DELAY_MS))
    );
    assert_eq!(
        observation.retry_event_delay_ms,
        Some(i64::from(observation.default_retry_delay_ms))
    );
    assert_eq!(
        observation.attempt_retry_delay_ms,
        Some(observation.default_retry_delay_ms)
    );
}

#[tokio::test]
async fn process_claimed_job_does_not_apply_override_to_other_failure_code() {
    const OVERRIDE_RETRY_DELAY_MS: i32 = 120_000;

    let observation = observe_retry_delay_override_failure(
        "jobs_worker_retry_override_code",
        "jobs.test.retry_override",
        |_| {
            JobFailure::retryable(
                "job.test.other_waiting_reason",
                "waiting for a different reason",
            )
        },
        3,
        Some((
            JobType::new("jobs.test.retry_override"),
            "job.test.waiting_for_external_refresh",
            OVERRIDE_RETRY_DELAY_MS,
        )),
    )
    .await;

    assert_eq!(observation.status, JobStatus::Pending);
    assert_ne!(
        observation.retry_event_delay_ms,
        Some(i64::from(OVERRIDE_RETRY_DELAY_MS))
    );
    assert_eq!(
        observation.retry_event_delay_ms,
        Some(i64::from(observation.default_retry_delay_ms))
    );
    assert_eq!(
        observation.attempt_retry_delay_ms,
        Some(observation.default_retry_delay_ms)
    );
}

#[tokio::test]
async fn process_claimed_job_ignores_retry_delay_override_for_terminal_failure() {
    const OVERRIDE_RETRY_DELAY_MS: i32 = 120_000;

    let observation = observe_retry_delay_override_failure(
        "jobs_worker_retry_override_terminal",
        "jobs.test.retry_override",
        |_| {
            JobFailure::terminal(
                "job.test.waiting_for_external_refresh",
                "terminal failure with matching code",
            )
            .retry_not_before_delay(Duration::ZERO)
        },
        3,
        Some((
            JobType::new("jobs.test.retry_override"),
            "job.test.waiting_for_external_refresh",
            OVERRIDE_RETRY_DELAY_MS,
        )),
    )
    .await;

    assert_eq!(observation.runs, 1);
    assert_eq!(observation.status, JobStatus::DeadLettered);
    assert_eq!(observation.retry_event_delay_ms, None);
    assert_eq!(observation.retry_event_requested_retry_at, None);
    assert_eq!(observation.retry_event_count, 0);
    assert_eq!(
        observation.failed_event_error_code.as_deref(),
        Some("job.test.waiting_for_external_refresh")
    );
    assert_eq!(observation.attempt_retry_delay_ms, None);
}

#[tokio::test]
async fn expired_lease_rejects_worker_lifecycle_updates() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_expired_lifecycle", 8).await;
    let job_type = JobType::new("jobs.test.expired_lifecycle");

    let (heartbeat_job_id, heartbeat_claim) = enqueue_and_claim_job(
        &pool,
        job_type,
        3,
        json!({"kind":"expired-heartbeat"}),
        "worker-expired-heartbeat",
    )
    .await;
    expire_job_lease(&pool, heartbeat_job_id).await;
    let heartbeat_error = heartbeat_job(
        &pool,
        heartbeat_claim.id,
        heartbeat_claim.run_number,
        heartbeat_claim.attempt,
        heartbeat_claim
            .worker_id
            .as_deref()
            .expect("claimed job has worker id"),
        30,
    )
    .await
    .expect_err("expired lease heartbeat should fail");
    assert_eq!(
        query_error_code(&heartbeat_error),
        Some("job.lease_owner_mismatch")
    );

    let (progress_job_id, progress_claim) = enqueue_and_claim_job(
        &pool,
        job_type,
        3,
        json!({"kind":"expired-progress"}),
        "worker-expired-progress",
    )
    .await;
    expire_job_lease(&pool, progress_job_id).await;
    let progress_error = update_job_progress(
        &pool,
        progress_claim.id,
        progress_claim.run_number,
        progress_claim.attempt,
        progress_claim
            .worker_id
            .as_deref()
            .expect("claimed job has worker id"),
        &JobProgressUpdate {
            stage: Some(runledger_core::jobs::JobStage::Running),
            progress_done: None,
            progress_total: None,
            checkpoint: None,
        },
    )
    .await
    .expect_err("expired lease progress update should fail");
    assert_eq!(
        query_error_code(&progress_error),
        Some("job.lease_owner_mismatch")
    );

    let (success_job_id, success_claim) = enqueue_and_claim_job(
        &pool,
        job_type,
        3,
        json!({"kind":"expired-success"}),
        "worker-expired-success",
    )
    .await;
    expire_job_lease(&pool, success_job_id).await;
    let success_error = complete_job_success(
        &pool,
        success_claim.id,
        success_claim.run_number,
        success_claim.attempt,
        success_claim
            .worker_id
            .as_deref()
            .expect("claimed job has worker id"),
        None,
    )
    .await
    .expect_err("expired lease success completion should fail");
    assert_eq!(
        query_error_code(&success_error),
        Some("job.lease_owner_mismatch")
    );

    let (failure_job_id, failure_claim) = enqueue_and_claim_job(
        &pool,
        job_type,
        3,
        json!({"kind":"expired-failure"}),
        "worker-expired-failure",
    )
    .await;
    expire_job_lease(&pool, failure_job_id).await;
    let failure_error = complete_job_failure(
        &pool,
        failure_claim.id,
        failure_claim.run_number,
        failure_claim.attempt,
        failure_claim
            .worker_id
            .as_deref()
            .expect("claimed job has worker id"),
        &JobFailureUpdate::new(
            JobFailureKind::Retryable,
            "job.test.expired_failure",
            "expired failure should not persist",
            Some(1_000),
        )
        .with_retry_timing(JobRetryTiming::After(Duration::from_millis(1_000))),
    )
    .await
    .expect_err("expired lease failure completion should fail");
    assert_eq!(
        query_error_code(&failure_error),
        Some("job.lease_owner_mismatch")
    );

    for job_id in [
        heartbeat_job_id,
        progress_job_id,
        success_job_id,
        failure_job_id,
    ] {
        let job = get_job_by_id(&pool, None, job_id)
            .await
            .expect("load job")
            .expect("job exists");
        assert_eq!(job.status, JobStatus::Leased);
    }

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn heartbeat_rejects_lease_that_expires_while_waiting_for_job_lock() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_heartbeat_clock_expiry", 8).await;
    let job_type = JobType::new("jobs.test.heartbeat_clock_expiry");
    let (job_id, claim) = enqueue_and_claim_job(
        &pool,
        job_type,
        3,
        json!({"kind":"heartbeat-clock-expiry"}),
        "worker-heartbeat-clock-expiry",
    )
    .await;
    let worker_id = claim.worker_id.clone().expect("claimed job has worker id");

    sqlx::query(
        "UPDATE job_queue
         SET lease_expires_at = clock_timestamp() + interval '1 second'
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .expect("shorten lease before blocking heartbeat");

    let mut lock_tx = pool.begin().await.expect("begin job lock transaction");
    sqlx::query("SELECT id FROM job_queue WHERE id = $1 FOR UPDATE")
        .bind(job_id)
        .execute(&mut *lock_tx)
        .await
        .expect("hold job row lock");

    let heartbeat_pool = pool.clone();
    let mut heartbeat_task = tokio::spawn(async move {
        heartbeat_job(
            &heartbeat_pool,
            claim.id,
            claim.run_number,
            claim.attempt,
            &worker_id,
            30,
        )
        .await
    });

    wait_for_heartbeat_to_block_on_job_lock(&pool).await;
    sleep(Duration::from_millis(1_200)).await;
    lock_tx.rollback().await.expect("release job row lock");

    let error = await_spawned_task(
        &mut heartbeat_task,
        Duration::from_secs(5),
        "heartbeat should finish after row lock release",
        "heartbeat task should not panic",
    )
    .await
    .expect_err("heartbeat should reject lease expired during lock wait");
    assert_eq!(query_error_code(&error), Some("job.lease_owner_mismatch"));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn successful_completion_persists_completion_update() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_success_completion_update", 8).await;
    let job_type = JobType::new("jobs.test.success_completion_update");
    let (job_id, claim) = enqueue_and_claim_job(
        &pool,
        job_type,
        3,
        json!({"kind":"success-completion-update"}),
        "worker-success-completion-update",
    )
    .await;

    let checkpoint = json!({"cursor": "next"});
    let output = json!({"result_id": "result_123"});
    complete_job_success(
        &pool,
        claim.id,
        claim.run_number,
        claim.attempt,
        claim
            .worker_id
            .as_deref()
            .expect("claimed job has worker id"),
        Some(&JobCompletionUpdate {
            progress_done: Some(2),
            progress_total: Some(3),
            checkpoint: Some(&checkpoint),
            output: Some(&output),
        }),
    )
    .await
    .expect("success completion should persist completion update");

    let job = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(job.status, JobStatus::Succeeded);
    assert_eq!(job.stage, runledger_core::jobs::JobStage::Completed);
    assert_eq!(job.progress_done, Some(2));
    assert_eq!(job.progress_total, Some(3));
    assert_eq!(job.checkpoint, Some(checkpoint));
    assert_eq!(job.output, Some(output));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn standalone_success_completion_allows_non_read_committed_session() {
    // The session-level isolation setting must apply to the same connection
    // complete_job_success borrows from the pool.
    let (pool, database) = setup_ephemeral_pool("jobs_worker_success_repeatable_read", 1).await;
    let job_type = JobType::new("jobs.test.success_repeatable_read");
    let (job_id, claim) = enqueue_and_claim_job(
        &pool,
        job_type,
        3,
        json!({"kind":"standalone-repeatable-read-success"}),
        "worker-standalone-repeatable-read",
    )
    .await;
    let worker_id = claim.worker_id.clone().expect("claimed job has worker id");

    sqlx::query("SET default_transaction_isolation = 'repeatable read'")
        .execute(&pool)
        .await
        .expect("set default isolation to repeatable read");
    let result = complete_job_success(
        &pool,
        claim.id,
        claim.run_number,
        claim.attempt,
        &worker_id,
        None,
    )
    .await;
    sqlx::query("SET default_transaction_isolation = 'read committed'")
        .execute(&pool)
        .await
        .expect("reset default isolation to read committed");
    result.expect("standalone job completion does not require workflow isolation");

    let job = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(job.status, JobStatus::Succeeded);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_terminally_fails_invalid_continuation_delay_without_replay() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_invalid_continuation_delay", 8).await;

    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.invalid_continuation_delay"),
        3,
        json!({"kind":"invalid-continuation-delay"}),
        "worker-invalid-continuation-delay",
    )
    .await;

    let runs = Arc::new(AtomicUsize::new(0));
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(InvalidContinuationDelayHandler {
        runs: runs.clone(),
        dead_letters: dead_letters.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let dead_letters = clone_dead_letters(&dead_letters);
    assert_eq!(dead_letters.len(), 1);
    let dead_letter = &dead_letters[0];
    assert_eq!(
        dead_letter.reason,
        JobDeadLetterReason::FailureKindNonRetryable
    );
    assert_eq!(dead_letter.failure.kind, JobFailureKind::Terminal);
    assert_eq!(dead_letter.failure.code, "job.invalid_continuation_delay");
    assert_eq!(dead_letter.max_attempts, Some(3));

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);
    assert_eq!(persisted.status_reason.as_deref(), Some("TERMINAL"));
    assert_eq!(
        persisted.last_error_code.as_deref(),
        Some("job.invalid_continuation_delay")
    );
    assert!(persisted.worker_id.is_none());
    assert!(persisted.lease_expires_at.is_none());

    let events = list_job_events(&pool, None, job_id, 50, None)
        .await
        .expect("list job events");
    assert!(
        events.iter().all(|event| !matches!(
            event.event_type,
            JobEventType::Requeued | JobEventType::RetryScheduled | JobEventType::Succeeded
        )),
        "invalid continuation delay must not continue, retry, or succeed"
    );
    let failed = events
        .iter()
        .find(|event| event.event_type == JobEventType::Failed)
        .expect("failed event should exist");
    assert_eq!(failed.payload.get("kind"), Some(&json!("TERMINAL")));
    assert_eq!(
        failed.payload.get("error_code"),
        Some(&json!("job.invalid_continuation_delay"))
    );

    reap_expired_leases(&pool, 10, 1_000)
        .await
        .expect("reaper should not requeue terminal invalid continuation");
    let replay_claims = claim_prestart_jobs(&pool, "worker-invalid-continuation-replay", 30, 1)
        .await
        .expect("claim after terminal invalid continuation");
    assert!(replay_claims.is_empty());
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_terminally_fails_invalid_success_progress_without_replay() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_invalid_success_progress", 8).await;

    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.invalid_completion_progress"),
        3,
        json!({"kind":"invalid-success-progress"}),
        "worker-invalid-success-progress",
    )
    .await;

    let runs = Arc::new(AtomicUsize::new(0));
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(InvalidCompletionProgressHandler {
        runs: runs.clone(),
        dead_letters: dead_letters.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let dead_letters = clone_dead_letters(&dead_letters);
    assert_eq!(dead_letters.len(), 1);
    let dead_letter = &dead_letters[0];
    assert_eq!(
        dead_letter.reason,
        JobDeadLetterReason::FailureKindNonRetryable
    );
    assert_eq!(dead_letter.failure.kind, JobFailureKind::Terminal);
    assert_eq!(dead_letter.failure.code, "job.invalid_completion_progress");
    assert!(
        dead_letter
            .failure
            .message
            .contains("Handler returned invalid success progress:")
    );
    assert!(!dead_letter.failure.message.contains("stored progress"));
    assert_eq!(dead_letter.max_attempts, Some(3));

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);
    assert_eq!(persisted.status_reason.as_deref(), Some("TERMINAL"));
    assert_eq!(
        persisted.last_error_code.as_deref(),
        Some("job.invalid_completion_progress")
    );
    assert!(persisted.worker_id.is_none());
    assert!(persisted.lease_expires_at.is_none());

    let events = list_job_events(&pool, None, job_id, 50, None)
        .await
        .expect("list job events");
    assert!(
        events
            .iter()
            .all(|event| event.event_type != JobEventType::Succeeded),
        "invalid success completion must not write a succeeded event"
    );
    assert!(
        events
            .iter()
            .all(|event| event.event_type != JobEventType::RetryScheduled),
        "invalid success completion must not schedule a retry"
    );
    let failed = events
        .iter()
        .find(|event| event.event_type == JobEventType::Failed)
        .expect("failed event should exist");
    assert_eq!(failed.payload.get("kind"), Some(&json!("TERMINAL")));
    assert_eq!(
        failed.payload.get("error_code"),
        Some(&json!("job.invalid_completion_progress"))
    );

    reap_expired_leases(&pool, 10, 1_000)
        .await
        .expect("reaper should not requeue terminal invalid completion");
    let replay_claims = claim_prestart_jobs(&pool, "worker-invalid-success-replay", 30, 1)
        .await
        .expect("claim after terminal invalid completion");
    assert!(replay_claims.is_empty());
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_terminally_fails_stale_partial_success_progress_without_replay() {
    let (pool, database) =
        setup_ephemeral_pool("jobs_worker_stale_partial_success_progress", 8).await;

    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.partial_invalid_completion_progress"),
        3,
        json!({"kind":"stale-partial-success-progress"}),
        "worker-stale-partial-success-progress",
    )
    .await;

    let worker_id = claimed_job
        .worker_id
        .clone()
        .expect("claimed job has worker id");
    update_job_progress(
        &pool,
        claimed_job.id,
        claimed_job.run_number,
        claimed_job.attempt,
        &worker_id,
        &JobProgressUpdate {
            stage: None,
            progress_done: Some(5),
            progress_total: Some(10),
            checkpoint: None,
        },
    )
    .await
    .expect("persist prior progress");

    let runs = Arc::new(AtomicUsize::new(0));
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(PartialInvalidCompletionProgressHandler {
        runs: runs.clone(),
        dead_letters: dead_letters.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let dead_letters = clone_dead_letters(&dead_letters);
    assert_eq!(dead_letters.len(), 1);
    let dead_letter = &dead_letters[0];
    assert_eq!(
        dead_letter.reason,
        JobDeadLetterReason::FailureKindNonRetryable
    );
    assert_eq!(dead_letter.failure.kind, JobFailureKind::Terminal);
    assert_eq!(dead_letter.failure.code, "job.invalid_completion_progress");

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);
    assert_eq!(
        persisted.last_error_code.as_deref(),
        Some("job.invalid_completion_progress")
    );
    assert!(persisted.worker_id.is_none());
    assert!(persisted.lease_expires_at.is_none());

    let events = list_job_events(&pool, None, job_id, 50, None)
        .await
        .expect("list job events");
    assert!(
        events
            .iter()
            .all(|event| event.event_type != JobEventType::Succeeded),
        "invalid coalesced success completion must not write a succeeded event"
    );
    assert!(
        events
            .iter()
            .all(|event| event.event_type != JobEventType::RetryScheduled),
        "invalid coalesced success completion must not schedule a retry"
    );

    reap_expired_leases(&pool, 10, 1_000)
        .await
        .expect("reaper should not requeue terminal invalid completion");
    let replay_claims = claim_prestart_jobs(&pool, "worker-stale-partial-replay", 30, 1)
        .await
        .expect("claim after terminal invalid completion");
    assert!(replay_claims.is_empty());
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn stale_partial_continuation_progress_reports_the_continuation_path() {
    let (pool, database) =
        setup_ephemeral_pool("jobs_worker_stale_partial_continuation_progress", 8).await;
    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.partial_invalid_continuation_progress"),
        3,
        json!({"kind":"stale-partial-continuation-progress"}),
        "worker-stale-partial-continuation-progress",
    )
    .await;
    let worker_id = claimed_job
        .worker_id
        .clone()
        .expect("claimed job has worker id");
    update_job_progress(
        &pool,
        claimed_job.id,
        claimed_job.run_number,
        claimed_job.attempt,
        &worker_id,
        &JobProgressUpdate {
            stage: None,
            progress_done: Some(5),
            progress_total: Some(10),
            checkpoint: None,
        },
    )
    .await
    .expect("persist prior progress");
    let runs = Arc::new(AtomicUsize::new(0));
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(PartialInvalidContinuationProgressHandler {
        runs: runs.clone(),
        dead_letters: dead_letters.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let dead_letters = clone_dead_letters(&dead_letters);
    assert_eq!(dead_letters.len(), 1);
    assert_eq!(
        dead_letters[0].failure.code,
        "job.invalid_completion_progress"
    );
    assert!(
        dead_letters[0]
            .failure
            .message
            .contains("invalid continuation progress:")
    );
    assert!(!dead_letters[0].failure.message.contains("stored progress"));
    assert!(!dead_letters[0].failure.message.contains("invalid success"));
    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load stale continuation progress job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);
    assert!(
        list_job_events(&pool, None, job_id, 20, None)
            .await
            .expect("list stale continuation progress events")
            .iter()
            .all(|event| event.event_type != JobEventType::Requeued)
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_aborts_before_handler_when_lease_owner_changes_pre_run() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_pre_run_lease", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.pre_run_lease_loss"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.pre_run_lease_loss"),
            organization_id: None,
            payload: &json!({"kind":"pre-run-mismatch"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let claimed_job = claim_one_job(&pool, "worker-1").await;

    sqlx::query(
        "UPDATE job_queue
         SET worker_id = 'worker-2'
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .expect("switch lease ownership");

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(CountingHandler { runs: runs.clone() });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "handler must not execute if lease ownership is lost before starting"
    );

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Leased);
    assert_eq!(persisted.worker_id.as_deref(), Some("worker-2"));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_survives_terminal_failure_hook_panic() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_terminal_hook_panic", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.terminal_hook_panic"),
            version: 1,
            max_attempts: 1,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.terminal_hook_panic"),
            organization_id: None,
            payload: &json!({"kind":"terminal-hook-panic"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let claimed_job = claim_one_job(&pool, "worker-terminal-hook-panic").await;

    let runs = Arc::new(AtomicUsize::new(0));
    let terminal_failures = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(TerminalHookPanicHandler {
        runs: runs.clone(),
        terminal_failures: terminal_failures.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(terminal_failures.load(Ordering::SeqCst), 1);

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_survives_terminal_failure_hook_timeout() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_terminal_hook_hang", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.terminal_hook_hang"),
            version: 1,
            max_attempts: 1,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.terminal_hook_hang"),
            organization_id: None,
            payload: &json!({"kind":"terminal-hook-hang"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let claimed_job = claim_one_job(&pool, "worker-terminal-hook-hang").await;

    let runs = Arc::new(AtomicUsize::new(0));
    let terminal_failures = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(TerminalHookHangHandler {
        runs: runs.clone(),
        terminal_failures: terminal_failures.clone(),
    });

    timeout(
        Duration::from_secs(2),
        process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30),
    )
    .await
    .expect("process_claimed_job should return even when terminal hook hangs");

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(terminal_failures.load(Ordering::SeqCst), 1);

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_cancels_inflight_dead_letter_hook_when_parent_is_aborted() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_dead_letter_hook_cancel", 8).await;

    let (_job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.hanging_dead_letter_hook"),
        1,
        json!({"kind":"dead-letter-hook-cancel"}),
        "worker-dead-letter-hook-cancel",
    )
    .await;

    let runs = Arc::new(AtomicUsize::new(0));
    let hook_started = Arc::new(Notify::new());
    let hook_drops = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(HangingDeadLetterFailureHandler {
        runs: runs.clone(),
        started: hook_started.clone(),
        drops: hook_drops.clone(),
    });

    let job_task = tokio::spawn(process_claimed_job(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        30,
    ));

    timeout(Duration::from_millis(500), hook_started.notified())
        .await
        .expect("dead-letter hook should start");
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    job_task.abort();
    let _ = job_task.await;

    assert!(
        wait_for_counter_at_least(&hook_drops, 1, Duration::from_millis(500)).await,
        "dead-letter hook future should be dropped when the parent job task is aborted"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_reports_attempt_exhaustion_to_dead_letter_hook() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_dead_letter_attempts", 8).await;

    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.dead_letter_attempts"),
        1,
        json!({"kind":"dead-letter-attempts"}),
        "worker-dead-letter-attempts",
    )
    .await;

    let runs = Arc::new(AtomicUsize::new(0));
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(RecordingDeadLetterHandler {
        job_type_name: "jobs.test.dead_letter_attempts",
        failure: JobFailure::retryable(
            "job.test.retryable_exhausted",
            "retryable failure should exhaust attempts",
        )
        .retry_not_before_delay(Duration::ZERO),
        runs: runs.clone(),
        dead_letters: dead_letters.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let dead_letters = clone_dead_letters(&dead_letters);
    assert_eq!(dead_letters.len(), 1);
    let dead_letter = &dead_letters[0];
    assert_eq!(dead_letter.reason, JobDeadLetterReason::AttemptsExhausted);
    assert_eq!(dead_letter.failure.kind, JobFailureKind::Retryable);
    assert_eq!(dead_letter.max_attempts, Some(1));

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn worker_dead_letter_hook_receives_latest_committed_checkpoint() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_dead_letter_checkpoint", 8).await;
    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.dead_letter_latest_checkpoint"),
        3,
        json!({"kind": "dead-letter-checkpoint"}),
        "worker-dead-letter-checkpoint",
    )
    .await;
    assert!(claimed_job.checkpoint.is_none());

    let dead_letter_contexts = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(CheckpointingDeadLetterHandler {
        pool: pool.clone(),
        dead_letter_contexts: dead_letter_contexts.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    {
        let contexts = dead_letter_contexts
            .lock()
            .expect("dead-letter contexts lock should not be poisoned");
        assert_eq!(contexts.len(), 1);
        assert_eq!(
            contexts[0].checkpoint,
            Some(json!({"cursor": "persisted-during-handler"}))
        );
    }

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load dead-lettered job")
        .expect("dead-lettered job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);
    assert_eq!(
        persisted.checkpoint,
        Some(json!({"cursor": "persisted-during-handler"}))
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_reports_non_retryable_failure_to_dead_letter_hook() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_dead_letter_non_retryable", 8).await;

    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.dead_letter_non_retryable"),
        3,
        json!({"kind":"dead-letter-non-retryable"}),
        "worker-dead-letter-non-retryable",
    )
    .await;

    let runs = Arc::new(AtomicUsize::new(0));
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(RecordingDeadLetterHandler {
        job_type_name: "jobs.test.dead_letter_non_retryable",
        failure: JobFailure::terminal(
            "job.test.non_retryable",
            "terminal failure should remain non-retryable",
        )
        .retry_not_before_delay(Duration::ZERO),
        runs: runs.clone(),
        dead_letters: dead_letters.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let dead_letters = clone_dead_letters(&dead_letters);
    assert_eq!(dead_letters.len(), 1);
    let dead_letter = &dead_letters[0];
    assert_eq!(
        dead_letter.reason,
        JobDeadLetterReason::FailureKindNonRetryable
    );
    assert_eq!(dead_letter.failure.kind, JobFailureKind::Terminal);
    assert_eq!(dead_letter.max_attempts, Some(3));

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_persists_handler_failure_with_reserved_lease_code() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_reserved_lease_code", 8).await;

    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.reserved_lease_code"),
        3,
        json!({"kind":"reserved-lease-code"}),
        "worker-reserved-lease-code",
    )
    .await;

    let runs = Arc::new(AtomicUsize::new(0));
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(RecordingDeadLetterHandler {
        job_type_name: "jobs.test.reserved_lease_code",
        failure: JobFailure::terminal(
            "job.lease_owner_mismatch",
            "handler failure should not be treated as internal lease loss",
        ),
        runs: runs.clone(),
        dead_letters: dead_letters.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let dead_letters = clone_dead_letters(&dead_letters);
    assert_eq!(dead_letters.len(), 1);
    let dead_letter = &dead_letters[0];
    assert_eq!(
        dead_letter.reason,
        JobDeadLetterReason::FailureKindNonRetryable
    );
    assert_eq!(dead_letter.failure.kind, JobFailureKind::Terminal);
    assert_eq!(dead_letter.failure.code, "job.lease_owner_mismatch");

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);
    assert_eq!(
        persisted.last_error_code.as_deref(),
        Some("job.lease_owner_mismatch")
    );
    assert!(persisted.worker_id.is_none());
    assert!(persisted.lease_expires_at.is_none());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_catches_main_handler_panic() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_handler_panic", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.handler_panic"),
            version: 1,
            max_attempts: 1,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.handler_panic"),
            organization_id: None,
            payload: &json!({"kind":"handler-panic"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let claimed_job = claim_one_job(&pool, "worker-handler-panic").await;

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(PanickingHandler { runs: runs.clone() });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);
    assert_eq!(persisted.status_reason.as_deref(), Some("PANICKED"));
    assert_eq!(
        persisted.last_error_code.as_deref(),
        Some("job.handler_panic")
    );

    let outcome = sqlx::query_scalar::<_, String>(
        "SELECT outcome::text
         FROM job_attempts
         WHERE job_id = $1
           AND run_number = 1
           AND attempt = 1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("fetch attempt outcome");
    assert_eq!(outcome, "PANICKED");

    let events = list_job_events(&pool, None, job_id, 50, None)
        .await
        .expect("list job events");
    let failed = events
        .iter()
        .find(|event| event.event_type == runledger_core::jobs::JobEventType::Failed)
        .expect("failed event should exist");
    assert_eq!(failed.payload.get("kind"), Some(&json!("PANICKED")));
    assert_eq!(
        failed.payload.get("error_code"),
        Some(&json!("job.handler_panic"))
    );
    let dead_lettered = events
        .iter()
        .find(|event| event.event_type == runledger_core::jobs::JobEventType::DeadLettered)
        .expect("dead-lettered event should exist");
    assert_eq!(dead_lettered.payload.get("kind"), Some(&json!("PANICKED")));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn run_worker_loop_continues_processing_after_handler_panic() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_handler_panic_loop", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    for job_type in [
        JobType::new("jobs.test.handler_panic"),
        JobType::new("jobs.test.handler_panic_successor"),
    ] {
        upsert_job_definition_tx(
            &mut tx,
            &JobDefinitionUpsert {
                job_type,
                version: 1,
                max_attempts: 1,
                default_timeout_seconds: 30,
                default_priority: 100,
                is_enabled: true,
            },
        )
        .await
        .expect("upsert job definition");
    }
    tx.commit().await.expect("commit tx");

    let panic_job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.handler_panic"),
            organization_id: None,
            payload: &json!({"kind":"loop-panic"}),
            priority: Some(200),
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue panic job");
    let success_job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.handler_panic_successor"),
            organization_id: None,
            payload: &json!({"kind":"loop-success"}),
            priority: Some(100),
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue success job");

    let panic_runs = Arc::new(AtomicUsize::new(0));
    let success_runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(PanickingHandler {
        runs: panic_runs.clone(),
    });
    registry.register(LoopSuccessHandler {
        runs: success_runs.clone(),
    });

    let config = JobsConfig {
        worker_id: "handler-panic-loop-worker".to_string(),
        poll_interval: Duration::from_millis(25),
        claim_batch_size: 1,
        lease_ttl_seconds: 30,
        max_global_concurrency: 1,
        reaper_interval: Duration::from_secs(30),
        schedule_poll_interval: Duration::from_secs(30),
        reaper_retry_delay_ms: 1_000,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_task = tokio::spawn(run_worker_loop(pool.clone(), registry, config, shutdown_rx));

    let panic_job = wait_for_status(
        &pool,
        panic_job_id,
        JobStatus::DeadLettered,
        Duration::from_secs(5),
    )
    .await;
    let success_job = wait_for_status(
        &pool,
        success_job_id,
        JobStatus::Succeeded,
        Duration::from_secs(5),
    )
    .await;

    assert_eq!(panic_job.status_reason.as_deref(), Some("PANICKED"));
    assert_eq!(
        panic_job.last_error_code.as_deref(),
        Some("job.handler_panic")
    );
    assert_eq!(success_job.last_error_code, None);
    assert_eq!(panic_runs.load(Ordering::SeqCst), 1);
    assert_eq!(success_runs.load(Ordering::SeqCst), 1);

    let _ = shutdown_tx.send(true);
    worker_task
        .await
        .expect("worker loop should shut down cleanly");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_aborts_when_running_progress_cannot_be_persisted() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_progress_persist_fail", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.persistence_failure"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.persistence_failure"),
            organization_id: None,
            payload: &json!({"kind":"running-progress-persistence-failure"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let claimed_job = claim_one_job(&pool, "worker-persistence-failure").await;
    let worker_pool = connect_closed_pool(&database.url).await;

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(PersistenceFailureHandler { runs: runs.clone() });

    process_claimed_job(worker_pool, Arc::new(registry), claimed_job, 30).await;

    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "handler must not execute once the worker can no longer persist running state"
    );

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Leased);
    assert_eq!(
        persisted.stage,
        runledger_core::jobs::JobStage::Queued,
        "job should remain queued because running state was never durably recorded"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn release_unstarted_claim_reports_not_applicable_after_running_persists() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_release_not_applicable", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.persistence_failure"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.persistence_failure"),
            organization_id: None,
            payload: &json!({"kind":"release-not-applicable"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let claimed_job = claim_one_job(&pool, "worker-release-not-applicable").await;
    update_job_progress(
        &pool,
        claimed_job.id,
        claimed_job.run_number,
        claimed_job.attempt,
        claimed_job
            .worker_id
            .as_deref()
            .expect("worker id is set on claimed job"),
        &JobProgressUpdate {
            stage: Some(runledger_core::jobs::JobStage::Running),
            progress_done: None,
            progress_total: None,
            checkpoint: None,
        },
    )
    .await
    .expect("persist running stage");

    let error = release_unstarted_job_claim(
        &pool,
        job_id,
        claimed_job.run_number,
        claimed_job.attempt,
        "worker-release-not-applicable",
        "TEST_NOT_APPLICABLE",
        0,
    )
    .await
    .expect_err("release should no longer apply once running persists");

    assert_eq!(
        query_error_code(&error),
        Some("job.unstarted_claim_release_not_applicable")
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn release_unstarted_claim_reports_owner_mismatch_for_other_worker() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_release_owner_mismatch", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.persistence_failure"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.persistence_failure"),
            organization_id: None,
            payload: &json!({"kind":"release-owner-mismatch"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let claimed_job = claim_one_job(&pool, "worker-release-owner-a").await;
    sqlx::query(
        "UPDATE job_queue
         SET worker_id = 'worker-release-owner-b'
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .expect("switch lease ownership");

    let error = release_unstarted_job_claim(
        &pool,
        job_id,
        claimed_job.run_number,
        claimed_job.attempt,
        "worker-release-owner-a",
        "TEST_OWNER_MISMATCH",
        0,
    )
    .await
    .expect_err("release should fail when another worker owns the lease");

    assert_eq!(query_error_code(&error), Some("job.lease_owner_mismatch"));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn reaper_requeues_unstarted_claim_when_running_progress_never_persisted() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_progress_reaper_requeue", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.persistence_failure"),
            version: 1,
            max_attempts: 1,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.persistence_failure"),
            organization_id: None,
            payload: &json!({"kind":"running-progress-reaper-requeue"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let claimed_job = claim_one_job(&pool, "worker-persistence-failure").await;
    let worker_pool = connect_closed_pool(&database.url).await;

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(PersistenceFailureHandler { runs: runs.clone() });

    process_claimed_job(worker_pool, Arc::new(registry), claimed_job, 30).await;

    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "handler must not execute before the job can durably enter RUNNING"
    );

    expire_job_lease(&pool, job_id).await;

    let reaped = reap_expired_leases(&pool, 1, 1_000)
        .await
        .expect("reap expired leases");
    assert_eq!(reaped, 1, "reaper should reclaim the stranded lease");

    let recovered = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load recovered job")
        .expect("job exists");
    assert_eq!(recovered.status, JobStatus::Pending);
    assert_eq!(
        recovered.attempt, 0,
        "reaper must return the unstarted claim without consuming an attempt"
    );

    let recovered_job = claim_one_job(&pool, "worker-persistence-retry").await;
    let runs_after_recovery = Arc::new(AtomicUsize::new(0));
    let mut recovery_registry = JobRegistry::new();
    recovery_registry.register(PersistenceFailureHandler {
        runs: runs_after_recovery.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(recovery_registry), recovered_job, 30).await;

    assert_eq!(
        runs_after_recovery.load(Ordering::SeqCst),
        1,
        "job should still be executable after reaper recovery"
    );

    let completed = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load completed job")
        .expect("job exists");
    assert_eq!(completed.status, JobStatus::Succeeded);
    assert_eq!(
        completed.attempt, 1,
        "successful execution should use the first real attempt after recovery"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn reaper_does_not_burn_retry_attempt_when_later_attempt_never_started() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_retry_attempt_not_burned", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.retry_then_success"),
            version: 1,
            max_attempts: 2,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.retry_then_success"),
            organization_id: None,
            payload: &json!({"kind":"retry-attempt-pre-run-failure"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let first_runs = Arc::new(AtomicUsize::new(0));
    let mut first_registry = JobRegistry::new();
    first_registry.register(RetryThenSuccessHandler {
        runs: first_runs.clone(),
    });

    let first_claimed_job = claim_one_job(&pool, "worker-retry-1").await;
    process_claimed_job(
        pool.clone(),
        Arc::new(first_registry),
        first_claimed_job,
        30,
    )
    .await;

    assert_eq!(
        first_runs.load(Ordering::SeqCst),
        1,
        "first attempt should execute and fail retryably"
    );

    let after_first_attempt = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after first attempt")
        .expect("job exists");
    assert_eq!(after_first_attempt.status, JobStatus::Pending);
    assert_eq!(after_first_attempt.attempt, 1);

    sqlx::query(
        "UPDATE job_queue
         SET next_run_at = now()
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .expect("make second attempt claimable immediately");

    let second_claimed_job = claim_one_job(&pool, "worker-retry-2").await;
    let failing_worker_pool = connect_closed_pool(&database.url).await;

    let second_runs = Arc::new(AtomicUsize::new(0));
    let mut second_registry = JobRegistry::new();
    second_registry.register(RetryThenSuccessHandler {
        runs: second_runs.clone(),
    });

    process_claimed_job(
        failing_worker_pool,
        Arc::new(second_registry),
        second_claimed_job,
        30,
    )
    .await;

    assert_eq!(
        second_runs.load(Ordering::SeqCst),
        0,
        "second attempt must fail before handler execution"
    );

    expire_job_lease(&pool, job_id).await;

    let reaped = reap_expired_leases(&pool, 1, 1_000)
        .await
        .expect("reap expired lease");
    assert_eq!(reaped, 1);

    let after_reap = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after reap")
        .expect("job exists");
    assert_eq!(
        after_reap.status,
        JobStatus::Pending,
        "later attempt that never started should be released back to pending"
    );
    assert_eq!(
        after_reap.attempt, 1,
        "reaper should preserve the earlier consumed attempt count"
    );

    let recovery_runs = Arc::new(AtomicUsize::new(1));
    let mut recovery_registry = JobRegistry::new();
    recovery_registry.register(RetryThenSuccessHandler {
        runs: recovery_runs.clone(),
    });

    let recovered_job = claim_one_job(&pool, "worker-retry-3").await;
    process_claimed_job(pool.clone(), Arc::new(recovery_registry), recovered_job, 30).await;

    let completed = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load completed recovered job")
        .expect("job exists");
    assert_eq!(completed.status, JobStatus::Succeeded);
    assert_eq!(completed.attempt, 2);

    teardown_ephemeral_pool(pool, database).await;
}
