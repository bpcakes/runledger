use runledger_postgres::jobs::ReapedLeaseRecord;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{info, warn};

mod observers;
mod terminal_hooks;

use self::observers::ReapedObserverTasks;
use self::terminal_hooks::notify_handlers_of_terminal_lease_expirations;
use crate::ReaperError;
use crate::RuntimeLoopExit;
use crate::config::JobsConfig;
use crate::observer::JobLifecycleObservers;
use crate::registry::JobRegistry;
use crate::shutdown;

pub async fn run_reaper_loop(
    pool: runledger_postgres::DbPool,
    registry: JobRegistry,
    config: JobsConfig,
    shutdown: watch::Receiver<bool>,
) -> RuntimeLoopExit {
    run_reaper_loop_with_observer(
        pool,
        registry,
        config,
        shutdown,
        JobLifecycleObservers::empty(),
    )
    .await
}

pub async fn run_reaper_loop_with_observer(
    pool: runledger_postgres::DbPool,
    registry: JobRegistry,
    config: JobsConfig,
    mut shutdown: watch::Receiver<bool>,
    observers: JobLifecycleObservers,
) -> RuntimeLoopExit {
    if let Err(error) = config.validate_reaper_loop() {
        warn!(%error, "invalid jobs config; stopping reaper loop");
        return RuntimeLoopExit::InvalidConfig(error);
    }

    let registry = Arc::new(registry);
    let mut reaped_observer_tasks = ReapedObserverTasks::owned();

    loop {
        reaped_observer_tasks.drain_finished();

        if shutdown::is_requested_or_closed(&shutdown) {
            return reaper_shutdown_complete(&mut reaped_observer_tasks).await;
        }

        match runledger_postgres::jobs::reap_expired_leases_with_diagnostics(
            &pool,
            config.claim_batch_size,
            config.reaper_retry_delay_ms,
        )
        .await
        {
            Ok(result) => {
                if result.summary.processed > 0 {
                    info!(
                        reaped = result.summary.processed,
                        "reaper reclaimed expired leases"
                    );
                }
                log_deferred_row_errors(&result);

                let fanout_result = notify_reaped_lease_side_effects(
                    registry.as_ref(),
                    &observers,
                    &mut reaped_observer_tasks,
                    &result.reaped_leases,
                    &mut shutdown,
                )
                .await;
                if matches!(
                    fanout_result,
                    ReaperNotificationFanoutResult::InterruptedByShutdown
                ) {
                    return reaper_shutdown_complete(&mut reaped_observer_tasks).await;
                }
            }
            Err(error) => {
                let error = ReaperError::ReapExpiredLeases {
                    batch_size: config.claim_batch_size,
                    retry_delay_ms: config.reaper_retry_delay_ms,
                    source: error,
                };
                warn!(%error, "reaper iteration failed");
            }
        }

        if shutdown::wait_for_request_or_timeout(&mut shutdown, config.reaper_interval).await {
            return reaper_shutdown_complete(&mut reaped_observer_tasks).await;
        }
    }
}

async fn notify_reaped_lease_side_effects(
    registry: &JobRegistry,
    observers: &JobLifecycleObservers,
    reaped_observer_tasks: &mut ReapedObserverTasks,
    reaped_leases: &[ReapedLeaseRecord],
    shutdown: &mut watch::Receiver<bool>,
) -> ReaperNotificationFanoutResult {
    let terminal_hook_result =
        notify_handlers_of_terminal_lease_expirations(registry, reaped_leases, shutdown).await;

    if terminal_hook_result.interrupted_by_shutdown() || shutdown::is_requested_or_closed(shutdown)
    {
        return ReaperNotificationFanoutResult::InterruptedByShutdown;
    }

    reaped_observer_tasks.spawn_batch(observers, reaped_leases);
    ReaperNotificationFanoutResult::Completed
}

fn log_deferred_row_errors(result: &runledger_postgres::jobs::ReapExpiredLeasesDetailedResult) {
    for error in &result.deferred_row_errors {
        warn!(
            job_id = %error.job_id,
            run_number = error.run_number,
            attempt = error.attempt,
            error_code = %error.error_code,
            error_message = %error.error_message,
            error_sqlstate = error.sqlstate.as_deref().unwrap_or(""),
            deferred_row_error_count = result.deferred_row_error_count,
            logged_deferred_row_errors = result.deferred_row_errors.len(),
            "reaper deferred expired leased job after row-level processing error"
        );
    }

    if result.deferred_row_error_count > result.deferred_row_errors.len() {
        warn!(
            deferred_row_error_count = result.deferred_row_error_count,
            logged_deferred_row_errors = result.deferred_row_errors.len(),
            "reaper deferred additional expired leased jobs after row-level processing errors"
        );
    }
}

async fn reaper_shutdown_complete(
    reaped_observer_tasks: &mut ReapedObserverTasks,
) -> RuntimeLoopExit {
    reaped_observer_tasks.abort_for_shutdown().await;
    info!("reaper shutdown complete");
    RuntimeLoopExit::Shutdown
}

#[derive(Debug, PartialEq, Eq)]
enum ReaperNotificationFanoutResult {
    Completed,
    InterruptedByShutdown,
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use chrono::Utc;
    use runledger_core::jobs::{
        JobCompletion, JobContext, JobDeadLetterInfo, JobDeadLetterReason, JobFailure, JobType,
    };
    use runledger_postgres::jobs::ReapedLeaseDisposition;
    use runledger_postgres::jobs::test_support::reaped_lease_record as postgres_reaped_lease_record;
    use serde_json::{Value, json};
    use sqlx::types::Uuid;
    use tokio::sync::{Notify, watch};
    use tokio::time::{sleep, timeout};

    use crate::observer::{JobLeaseReapedEvent, JobLifecycleObserver, JobLifecycleObservers};
    use crate::registry::{JobHandler, JobRegistry};

    use super::observers::ReapedObserverTasks;
    use super::terminal_hooks::{
        TerminalHookFanoutResult, notify_handlers_of_terminal_lease_expirations,
        notify_handlers_of_terminal_lease_expirations_with_before_first_hook_admission,
    };
    use super::{ReaperNotificationFanoutResult, notify_reaped_lease_side_effects};

    struct HangingTerminalHookHandler;
    struct CountingHangingTerminalHookHandler {
        started: Arc<AtomicUsize>,
    }

    struct ControlledTerminalHookHandler {
        started: Arc<Notify>,
        release: Arc<Notify>,
        completions: Arc<AtomicUsize>,
    }

    struct RecordingDeadLetterHandler {
        dead_letters: Arc<Mutex<Vec<JobDeadLetterInfo>>>,
    }

    #[derive(Clone, Default)]
    struct RecordingObserver {
        reaped: Arc<Mutex<Vec<JobLeaseReapedEvent>>>,
    }

    #[derive(Clone)]
    struct SlowReapedObserver {
        started: Arc<Notify>,
        started_count: Arc<AtomicUsize>,
    }

    #[derive(Clone)]
    struct CancellableReapedObserver {
        started: Arc<Notify>,
        started_count: Arc<AtomicUsize>,
        active_callbacks: Arc<AtomicUsize>,
    }

    struct ActiveCallbackGuard {
        active_callbacks: Arc<AtomicUsize>,
    }

    impl Drop for ActiveCallbackGuard {
        fn drop(&mut self) {
            self.active_callbacks.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl JobHandler for HangingTerminalHookHandler {
        fn job_type(&self) -> JobType<'static> {
            JobType::new("jobs.test.reaper.hook.hang")
        }

        async fn execute(
            &self,
            _context: JobContext,
            _payload: Value,
        ) -> Result<JobCompletion, JobFailure> {
            Ok(JobCompletion::success())
        }

        async fn on_dead_letter(
            &self,
            _context: JobContext,
            _payload: Value,
            _dead_letter: JobDeadLetterInfo,
        ) {
            pending::<()>().await;
        }
    }

    #[async_trait::async_trait]
    impl JobHandler for CountingHangingTerminalHookHandler {
        fn job_type(&self) -> JobType<'static> {
            JobType::new("jobs.test.reaper.hook.counting_hang")
        }

        async fn execute(
            &self,
            _context: JobContext,
            _payload: Value,
        ) -> Result<JobCompletion, JobFailure> {
            Ok(JobCompletion::success())
        }

        async fn on_dead_letter(
            &self,
            _context: JobContext,
            _payload: Value,
            _dead_letter: JobDeadLetterInfo,
        ) {
            self.started.fetch_add(1, Ordering::SeqCst);
            pending::<()>().await;
        }
    }

    #[async_trait::async_trait]
    impl JobHandler for ControlledTerminalHookHandler {
        fn job_type(&self) -> JobType<'static> {
            JobType::new("jobs.test.reaper.hook.controlled")
        }

        async fn execute(
            &self,
            _context: JobContext,
            _payload: Value,
        ) -> Result<JobCompletion, JobFailure> {
            Ok(JobCompletion::success())
        }

        async fn on_dead_letter(
            &self,
            _context: JobContext,
            _payload: Value,
            _dead_letter: JobDeadLetterInfo,
        ) {
            self.started.notify_one();
            self.release.notified().await;
            self.completions.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait::async_trait]
    impl JobHandler for RecordingDeadLetterHandler {
        fn job_type(&self) -> JobType<'static> {
            JobType::new("jobs.test.reaper.dead_letter.record")
        }

        async fn execute(
            &self,
            _context: JobContext,
            _payload: Value,
        ) -> Result<JobCompletion, JobFailure> {
            Ok(JobCompletion::success())
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
    impl JobLifecycleObserver for RecordingObserver {
        async fn on_job_lease_reaped(&self, event: JobLeaseReapedEvent) {
            self.reaped
                .lock()
                .expect("reaped events lock should not be poisoned")
                .push(event);
        }
    }

    #[async_trait::async_trait]
    impl JobLifecycleObserver for SlowReapedObserver {
        async fn on_job_lease_reaped(&self, _event: JobLeaseReapedEvent) {
            self.started_count.fetch_add(1, Ordering::SeqCst);
            self.started.notify_waiters();
            pending::<()>().await;
        }
    }

    #[async_trait::async_trait]
    impl JobLifecycleObserver for CancellableReapedObserver {
        async fn on_job_lease_reaped(&self, _event: JobLeaseReapedEvent) {
            self.started_count.fetch_add(1, Ordering::SeqCst);
            self.active_callbacks.fetch_add(1, Ordering::SeqCst);
            let _guard = ActiveCallbackGuard {
                active_callbacks: self.active_callbacks.clone(),
            };
            self.started.notify_waiters();
            pending::<()>().await;
        }
    }

    async fn wait_for_count(counter: Arc<AtomicUsize>, notify: Arc<Notify>, expected: usize) {
        while counter.load(Ordering::SeqCst) < expected {
            notify.notified().await;
        }
    }

    async fn wait_for_reaped_count(
        observer: &RecordingObserver,
        expected: usize,
        timeout_after: Duration,
    ) {
        timeout(timeout_after, async {
            loop {
                let count = observer
                    .reaped
                    .lock()
                    .expect("reaped events lock should not be poisoned")
                    .len();
                if count >= expected {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timed out waiting for reaped observer event count");
    }

    fn reaped_lease_record(job_type: &'static str) -> runledger_postgres::jobs::ReapedLeaseRecord {
        postgres_reaped_lease_record(
            Uuid::now_v7(),
            runledger_core::jobs::JobTypeName::from_static(job_type),
            None,
            1,
            1,
            3,
            Some("worker-reaped-observer".to_owned()),
            false,
            ReapedLeaseDisposition::ReleasedToPending,
        )
    }

    fn terminal_reaped_lease_record(
        job_id: Uuid,
        job_type: runledger_core::jobs::JobTypeName,
        payload: Value,
    ) -> runledger_postgres::jobs::ReapedLeaseRecord {
        postgres_reaped_lease_record(
            job_id,
            job_type.clone(),
            None,
            1,
            1,
            1,
            Some("worker-reaped-terminal".to_owned()),
            false,
            ReapedLeaseDisposition::DeadLetteredTerminal { payload },
        )
    }

    #[tokio::test]
    async fn notify_observers_reports_reaped_retryable_lease() {
        let observer = RecordingObserver::default();
        let observers = JobLifecycleObservers::from_observer(observer.clone());
        let next_run_at = Utc::now();
        let mut tasks = ReapedObserverTasks::owned();

        let job = postgres_reaped_lease_record(
            Uuid::now_v7(),
            runledger_core::jobs::JobTypeName::from_static("jobs.test.reaper.observer"),
            None,
            1,
            2,
            3,
            Some("worker-lost-lease".to_owned()),
            true,
            ReapedLeaseDisposition::RetryScheduled {
                retry_delay_ms: 1_000,
                next_run_at,
            },
        );
        tasks.spawn_batch(&observers, &[job]);
        wait_for_reaped_count(&observer, 1, Duration::from_millis(500)).await;
        tasks.abort_for_shutdown().await;

        let reaped = observer
            .reaped
            .lock()
            .expect("reaped events lock should not be poisoned")
            .clone();
        assert_eq!(reaped.len(), 1);
        assert_eq!(reaped[0].job.worker_id, "worker-lost-lease");
        assert_eq!(reaped[0].job.attempt, 2);
        assert_eq!(reaped[0].job.max_attempts, 3);
        assert_eq!(reaped[0].failure.code, "job.lease_expired");
        assert!(reaped[0].started_without_renewal_heartbeat);
        assert!(matches!(
            reaped[0].disposition,
            crate::observer::JobLeaseReapedDisposition::RetryScheduled { .. }
        ));
    }

    #[tokio::test]
    async fn reaped_observer_task_owner_drops_newest_callback_at_cap() {
        let started = Arc::new(Notify::new());
        let started_count = Arc::new(AtomicUsize::new(0));
        let active_callbacks = Arc::new(AtomicUsize::new(0));
        let observers = JobLifecycleObservers::from_observer(CancellableReapedObserver {
            started: started.clone(),
            started_count: started_count.clone(),
            active_callbacks: active_callbacks.clone(),
        });
        let jobs: Vec<_> = (0..2)
            .map(|_| reaped_lease_record("jobs.test.reaper.observer.cap"))
            .collect();
        let mut tasks = ReapedObserverTasks::owned_with_max_concurrency(1);

        tasks.spawn_batch(&observers, &jobs);
        timeout(
            Duration::from_millis(500),
            wait_for_count(started_count.clone(), started.clone(), 1),
        )
        .await
        .expect("first reaped observer callback should start");
        sleep(Duration::from_millis(25)).await;

        assert_eq!(active_callbacks.load(Ordering::SeqCst), 1);
        assert_eq!(tasks.in_flight_count(), 1);
        tasks.abort_for_shutdown().await;
        assert_eq!(active_callbacks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn notify_observers_starts_multiple_reaped_callbacks_without_serial_timeout() {
        let started = Arc::new(Notify::new());
        let started_count = Arc::new(AtomicUsize::new(0));
        let active_callbacks = Arc::new(AtomicUsize::new(0));
        let observers = JobLifecycleObservers::from_observer(CancellableReapedObserver {
            started: started.clone(),
            started_count: started_count.clone(),
            active_callbacks: active_callbacks.clone(),
        });
        let jobs: Vec<_> = (0..3)
            .map(|_| reaped_lease_record("jobs.test.reaper.concurrent.observer"))
            .collect();
        let mut tasks = ReapedObserverTasks::owned();

        tasks.spawn_batch(&observers, &jobs);
        timeout(
            Duration::from_millis(75),
            wait_for_count(started_count.clone(), started.clone(), 3),
        )
        .await
        .expect("reaped observer callbacks should not wait for serial observer timeouts to start");
        assert_eq!(active_callbacks.load(Ordering::SeqCst), 3);

        tasks.abort_for_shutdown().await;
        assert_eq!(active_callbacks.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn notify_observers_shutdown_cancels_inflight_reaped_observer() {
        let started = Arc::new(Notify::new());
        let started_count = Arc::new(AtomicUsize::new(0));
        let active_callbacks = Arc::new(AtomicUsize::new(0));
        let observers = JobLifecycleObservers::from_observer(CancellableReapedObserver {
            started: started.clone(),
            started_count: started_count.clone(),
            active_callbacks: active_callbacks.clone(),
        });
        let jobs: Vec<_> = (0..2)
            .map(|_| reaped_lease_record("jobs.test.reaper.cancel.observer"))
            .collect();
        let mut tasks = ReapedObserverTasks::owned();

        tasks.spawn_batch(&observers, &jobs);
        timeout(
            Duration::from_millis(500),
            wait_for_count(started_count, started, 2),
        )
        .await
        .expect("reaped observer callbacks should start");
        assert_eq!(active_callbacks.load(Ordering::SeqCst), 2);

        tasks.abort_for_shutdown().await;
        assert_eq!(
            active_callbacks.load(Ordering::SeqCst),
            0,
            "reaped observer callback must be cancelled before shutdown returns"
        );
    }

    #[tokio::test]
    async fn reaper_side_effects_notify_terminal_hooks_before_slow_reaped_observers() {
        let hook_started = Arc::new(Notify::new());
        let hook_release = Arc::new(Notify::new());
        let hook_completions = Arc::new(AtomicUsize::new(0));
        let mut registry = JobRegistry::new();
        registry.register(ControlledTerminalHookHandler {
            started: hook_started.clone(),
            release: hook_release.clone(),
            completions: hook_completions.clone(),
        });

        let observer_started = Arc::new(Notify::new());
        let observer_started_count = Arc::new(AtomicUsize::new(0));
        let observers = JobLifecycleObservers::from_observer(SlowReapedObserver {
            started: observer_started.clone(),
            started_count: observer_started_count.clone(),
        });
        let job_id = Uuid::now_v7();
        let job_type =
            runledger_core::jobs::JobTypeName::from_static("jobs.test.reaper.hook.controlled");
        let reaped_leases = vec![terminal_reaped_lease_record(
            job_id,
            job_type,
            json!({ "kind": "reaper-side-effect-order" }),
        )];
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let mut notification_task = tokio::spawn(async move {
            let mut reaped_observer_tasks = ReapedObserverTasks::owned();
            let result = notify_reaped_lease_side_effects(
                &registry,
                &observers,
                &mut reaped_observer_tasks,
                &reaped_leases,
                &mut shutdown_rx,
            )
            .await;
            (result, reaped_observer_tasks)
        });

        timeout(Duration::from_millis(500), hook_started.notified())
            .await
            .expect("terminal dead-letter hook should start before reaped observer fanout");
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            observer_started_count.load(Ordering::SeqCst),
            0,
            "reaped observer callback should not start while terminal hook is still running"
        );

        hook_release.notify_waiters();
        timeout(
            Duration::from_millis(500),
            wait_for_count(observer_started_count, observer_started, 1),
        )
        .await
        .expect("reaped observer fanout should start after terminal hook delivery");
        assert_eq!(hook_completions.load(Ordering::SeqCst), 1);

        let (result, mut reaped_observer_tasks) =
            timeout(Duration::from_secs(2), &mut notification_task)
                .await
                .expect("reaper notification fanout should finish after scheduling observers")
                .expect("notification task should not panic");
        assert_eq!(result, ReaperNotificationFanoutResult::Completed);
        reaped_observer_tasks.abort_for_shutdown().await;
    }

    #[tokio::test]
    async fn notify_handlers_survives_terminal_hook_timeout() {
        let mut registry = JobRegistry::new();
        registry.register(HangingTerminalHookHandler);

        let jobs = vec![terminal_reaped_lease_record(
            Uuid::now_v7(),
            runledger_core::jobs::JobTypeName::from_static("jobs.test.reaper.hook.hang"),
            json!({ "kind": "hook-timeout" }),
        )];

        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let result = timeout(
            Duration::from_secs(2),
            notify_handlers_of_terminal_lease_expirations(&registry, &jobs, &mut shutdown_rx),
        )
        .await
        .expect("notification pass should return even when terminal hook hangs");

        assert_eq!(result, TerminalHookFanoutResult::Completed { started: 1 });
    }

    #[tokio::test]
    async fn notify_handlers_shutdown_bounds_hanging_terminal_hook_batch() {
        const HOOK_COUNT: usize = 80;

        let started = Arc::new(AtomicUsize::new(0));
        let mut registry = JobRegistry::new();
        registry.register(CountingHangingTerminalHookHandler {
            started: started.clone(),
        });

        let jobs: Vec<_> = (0..HOOK_COUNT)
            .map(|_| {
                terminal_reaped_lease_record(
                    Uuid::now_v7(),
                    runledger_core::jobs::JobTypeName::from_static(
                        "jobs.test.reaper.hook.counting_hang",
                    ),
                    json!({ "kind": "bounded-shutdown-hook-drain" }),
                )
            })
            .collect();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        shutdown_tx
            .send(true)
            .expect("shutdown receiver should still be active");

        let result = timeout(
            Duration::from_millis(350),
            notify_handlers_of_terminal_lease_expirations(&registry, &jobs, &mut shutdown_rx),
        )
        .await
        .expect("shutdown hook fanout should not wait for every hanging hook timeout");

        assert_eq!(
            result,
            TerminalHookFanoutResult::InterruptedByShutdown {
                started: 8,
                skipped: HOOK_COUNT - 8
            }
        );
        assert_eq!(
            started.load(Ordering::SeqCst),
            8,
            "shutdown should admit only one bounded hook concurrency window"
        );
    }

    #[tokio::test]
    async fn notify_handlers_shutdown_after_entry_before_first_admission_uses_bounded_window() {
        const HOOK_COUNT: usize = 80;

        let started = Arc::new(AtomicUsize::new(0));
        let mut registry = JobRegistry::new();
        registry.register(CountingHangingTerminalHookHandler {
            started: started.clone(),
        });

        let jobs: Vec<_> = (0..HOOK_COUNT)
            .map(|_| {
                terminal_reaped_lease_record(
                    Uuid::now_v7(),
                    runledger_core::jobs::JobTypeName::from_static(
                        "jobs.test.reaper.hook.counting_hang",
                    ),
                    json!({ "kind": "mid-fanout-shutdown-hook-drain" }),
                )
            })
            .collect();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let result = timeout(
            Duration::from_millis(350),
            notify_handlers_of_terminal_lease_expirations_with_before_first_hook_admission(
                &registry,
                &jobs,
                &mut shutdown_rx,
                move || {
                    shutdown_tx
                        .send(true)
                        .expect("shutdown receiver should still be active");
                },
            ),
        )
        .await
        .expect("shutdown hook fanout should not skip every committed hook");

        assert_eq!(
            result,
            TerminalHookFanoutResult::InterruptedByShutdown {
                started: 8,
                skipped: HOOK_COUNT - 8
            }
        );
        assert_eq!(
            started.load(Ordering::SeqCst),
            8,
            "shutdown first observed after entry should admit one bounded hook concurrency window"
        );
    }

    #[tokio::test]
    async fn reaper_side_effects_shutdown_after_committed_terminal_batch_delivers_hook() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let completions = Arc::new(AtomicUsize::new(0));
        let mut registry = JobRegistry::new();
        registry.register(ControlledTerminalHookHandler {
            started: started.clone(),
            release: release.clone(),
            completions: completions.clone(),
        });
        let observer_started = Arc::new(Notify::new());
        let observer_started_count = Arc::new(AtomicUsize::new(0));
        let observers = JobLifecycleObservers::from_observer(SlowReapedObserver {
            started: observer_started.clone(),
            started_count: observer_started_count.clone(),
        });
        let mut reaped_observer_tasks = ReapedObserverTasks::owned();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        shutdown_tx
            .send(true)
            .expect("shutdown receiver should still be active");

        let reaped_leases = vec![terminal_reaped_lease_record(
            Uuid::now_v7(),
            runledger_core::jobs::JobTypeName::from_static("jobs.test.reaper.hook.controlled"),
            json!({ "kind": "committed-batch-shutdown" }),
        )];

        let mut notification_task = tokio::spawn(async move {
            notify_reaped_lease_side_effects(
                &registry,
                &observers,
                &mut reaped_observer_tasks,
                &reaped_leases,
                &mut shutdown_rx,
            )
            .await
        });

        timeout(Duration::from_millis(500), started.notified())
            .await
            .expect("terminal hook should start even though shutdown is already requested");
        assert_eq!(completions.load(Ordering::SeqCst), 0);
        assert!(
            timeout(Duration::from_millis(25), observer_started.notified())
                .await
                .is_err(),
            "reaped observer should not start while shutdown is requested after terminal hook delivery"
        );

        release.notify_waiters();
        let result = timeout(Duration::from_secs(2), &mut notification_task)
            .await
            .expect("notification fanout should return after terminal hook drains")
            .expect("notification task should not panic");

        assert_eq!(
            result,
            ReaperNotificationFanoutResult::InterruptedByShutdown
        );
        assert_eq!(completions.load(Ordering::SeqCst), 1);
        assert_eq!(observer_started_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn notify_handlers_delivers_terminal_hook_for_committed_batch() {
        let dead_letters = Arc::new(Mutex::new(Vec::new()));
        let mut registry = JobRegistry::new();
        registry.register(RecordingDeadLetterHandler {
            dead_letters: dead_letters.clone(),
        });

        let jobs = vec![terminal_reaped_lease_record(
            Uuid::now_v7(),
            runledger_core::jobs::JobTypeName::from_static("jobs.test.reaper.dead_letter.record"),
            json!({ "kind": "committed-terminal-hook" }),
        )];

        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let result =
            notify_handlers_of_terminal_lease_expirations(&registry, &jobs, &mut shutdown_rx).await;

        assert_eq!(result, TerminalHookFanoutResult::Completed { started: 1 });
        assert_eq!(
            dead_letters
                .lock()
                .expect("dead-letter list lock should not be poisoned")
                .len(),
            1,
            "terminal hook must still run for a committed terminal reaper batch"
        );
    }

    #[tokio::test]
    async fn reaper_side_effects_closed_shutdown_channel_does_not_start_reaped_observer() {
        let observer_started = Arc::new(Notify::new());
        let observer_started_count = Arc::new(AtomicUsize::new(0));
        let observers = JobLifecycleObservers::from_observer(SlowReapedObserver {
            started: observer_started.clone(),
            started_count: observer_started_count.clone(),
        });
        let registry = JobRegistry::new();
        let mut reaped_observer_tasks = ReapedObserverTasks::owned();
        let reaped_leases = vec![reaped_lease_record("jobs.test.reaper.closed.observer")];
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        drop(shutdown_tx);

        let result = notify_reaped_lease_side_effects(
            &registry,
            &observers,
            &mut reaped_observer_tasks,
            &reaped_leases,
            &mut shutdown_rx,
        )
        .await;

        assert_eq!(
            result,
            ReaperNotificationFanoutResult::InterruptedByShutdown
        );
        assert_eq!(reaped_observer_tasks.in_flight_count(), 0);
        assert_eq!(observer_started_count.load(Ordering::SeqCst), 0);
        assert!(
            timeout(Duration::from_millis(50), observer_started.notified())
                .await
                .is_err(),
            "reaped observer callback must not start after shutdown channel closes"
        );
    }

    #[tokio::test]
    async fn notify_handlers_drains_controlled_terminal_hook() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let completions = Arc::new(AtomicUsize::new(0));
        let mut registry = JobRegistry::new();
        registry.register(ControlledTerminalHookHandler {
            started: started.clone(),
            release: release.clone(),
            completions: completions.clone(),
        });

        let jobs = vec![terminal_reaped_lease_record(
            Uuid::now_v7(),
            runledger_core::jobs::JobTypeName::from_static("jobs.test.reaper.hook.controlled"),
            json!({ "kind": "non-shutdown-watch-update" }),
        )];

        tokio::spawn(async move {
            started.notified().await;
            sleep(Duration::from_millis(20)).await;
            release.notify_waiters();
        });

        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let result =
            notify_handlers_of_terminal_lease_expirations(&registry, &jobs, &mut shutdown_rx).await;

        assert_eq!(result, TerminalHookFanoutResult::Completed { started: 1 });
        assert_eq!(completions.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn notify_handlers_reports_lease_expiration_dead_letter_reason() {
        let dead_letters = Arc::new(Mutex::new(Vec::new()));
        let mut registry = JobRegistry::new();
        registry.register(RecordingDeadLetterHandler {
            dead_letters: dead_letters.clone(),
        });

        let jobs = vec![terminal_reaped_lease_record(
            Uuid::now_v7(),
            runledger_core::jobs::JobTypeName::from_static("jobs.test.reaper.dead_letter.record"),
            json!({ "kind": "lease-expired" }),
        )];

        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let result =
            notify_handlers_of_terminal_lease_expirations(&registry, &jobs, &mut shutdown_rx).await;

        assert_eq!(result, TerminalHookFanoutResult::Completed { started: 1 });
        let dead_letters = dead_letters
            .lock()
            .expect("dead-letter list lock should not be poisoned");
        assert_eq!(dead_letters.len(), 1);
        let dead_letter = &dead_letters[0];
        assert_eq!(dead_letter.reason, JobDeadLetterReason::LeaseExpired);
        assert_eq!(
            dead_letter.failure.kind,
            runledger_core::jobs::JobFailureKind::LeaseExpired
        );
        assert_eq!(dead_letter.max_attempts, Some(1));
    }
}
