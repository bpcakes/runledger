use std::future::pending;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use runledger_core::jobs::{
    JobCompletion, JobContext, JobDeadLetterInfo, JobDeadLetterReason, JobEventType, JobFailure,
    JobFailureKind, JobRetryTiming, JobStatus, JobType, JobTypeName, StepKey,
    WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder, WorkflowStepStatus, WorkflowType,
};
use runledger_postgres::jobs::{
    JobCompletionUpdate, JobDefinitionUpsert, JobEnqueue, JobFailureUpdate, JobLeaseIdentity,
    JobOrdinaryProgressUpdate, JobRunningUpdate, claim_prestart_jobs, complete_job_failure,
    complete_job_success, enqueue_job, enqueue_workflow_run, get_job_by_id, heartbeat_job,
    list_job_events, list_workflow_steps, mark_job_running, reap_expired_leases,
    release_unstarted_job_claim, update_job_ordinary_progress, upsert_job_definition_tx,
};
use runledger_test_support::{
    setup_ephemeral_pool_with_untracked_migrations as setup_ephemeral_pool, teardown_ephemeral_pool,
};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};

use self::support::*;
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
    JobCompletionPersistFailedEvent, JobCompletionPersistenceOperation, JobContinuedEvent,
    JobFailedEvent, JobFailureDisposition, JobLeaseLostEvent, JobLifecycleObserver,
    JobLifecycleObservers, JobRunningEvent, JobSucceededEvent, ObservedJob,
};
use crate::registry::{JobHandler, JobRegistry};

mod capacity;
mod completion_and_retry;
mod execution_services;
mod heartbeat_progress;
mod lease_fencing;
mod observer_tasks;
mod prestart_recovery;
mod support;
mod terminal_hooks;

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

struct InvalidStoredTotalProgressHandler {
    runs: Arc<AtomicUsize>,
    dead_letters: Arc<Mutex<Vec<JobDeadLetterInfo>>>,
}

struct InvalidContinuationProgressHandler {
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

struct CommitCheckingSucceededObserver {
    pool: PgPool,
    committed_status: Arc<Mutex<Option<JobStatus>>>,
    checked: Arc<Notify>,
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
                .expect("continuation progress is valid")
                .checkpoint(json!({"cursor": 1})));
        }

        Ok(JobCompletion::success()
            .progress(2, 2)
            .expect("terminal progress is valid"))
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
        JobCompletion::success().progress(2, 1).map_err(|error| {
            JobFailure::terminal(
                "job.invalid_completion_progress",
                format!("Handler returned invalid success progress: {error}"),
            )
        })
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
impl JobHandler for InvalidStoredTotalProgressHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.partial_invalid_completion_progress")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        JobCompletion::success().progress(20, 10).map_err(|error| {
            JobFailure::terminal(
                "job.invalid_completion_progress",
                format!("Handler returned invalid success progress: {error}"),
            )
        })
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
impl JobHandler for InvalidContinuationProgressHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.partial_invalid_continuation_progress")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        JobCompletion::continue_now()
            .progress(20, 10)
            .map_err(|error| {
                JobFailure::terminal(
                    "job.invalid_completion_progress",
                    format!("Handler returned invalid continuation progress: {error}"),
                )
            })
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
        update_job_ordinary_progress(
            &self.pool,
            context.job_id,
            context.run_number,
            context.attempt,
            &context.worker_id,
            &JobOrdinaryProgressUpdate {
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

#[async_trait::async_trait]
impl JobLifecycleObserver for CommitCheckingSucceededObserver {
    async fn on_job_succeeded(&self, event: JobSucceededEvent) {
        let status = get_job_by_id(&self.pool, None, event.job.job_id)
            .await
            .expect("success observer should read committed job")
            .expect("success observer job should exist")
            .status;
        *self
            .committed_status
            .lock()
            .expect("committed status lock should not be poisoned") = Some(status);
        self.checked.notify_one();
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

async fn record_postgres_server_version(pool: &PgPool, diagnostic: &str) {
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
        "{diagnostic} PostgreSQL server_version={server_version}, server_version_num={server_version_num}"
    );
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
