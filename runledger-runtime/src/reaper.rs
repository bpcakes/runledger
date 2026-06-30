use runledger_core::jobs::{JobContext, JobDeadLetterInfo, JobDeadLetterReason, JobFailure};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::{Id, JoinSet};
use tokio::time::{Duration, timeout};
use tracing::{info, warn};

use crate::ReaperError;
use crate::RuntimeLoopExit;
use crate::config::JobsConfig;
use crate::registry::JobRegistry;
use crate::shutdown;

const REAPER_WORKER_ID: &str = "reaper";
const LEASE_EXPIRED_CODE: &str = "job.lease_expired";
const LEASE_EXPIRED_MESSAGE: &str = "Job lease expired before completion.";
const REAPER_TERMINAL_HOOK_MAX_CONCURRENCY: usize = 8;
#[cfg(test)]
const TERMINAL_HOOK_TIMEOUT: Duration = Duration::from_millis(100);
#[cfg(not(test))]
const TERMINAL_HOOK_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const TERMINAL_HOOK_ABORT_DRAIN_TIMEOUT: Duration = Duration::from_millis(50);
#[cfg(not(test))]
const TERMINAL_HOOK_ABORT_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);

pub async fn run_reaper_loop(
    pool: runledger_postgres::DbPool,
    registry: JobRegistry,
    config: JobsConfig,
    mut shutdown: watch::Receiver<bool>,
) -> RuntimeLoopExit {
    if let Err(error) = config.validate_reaper_loop() {
        warn!(%error, "invalid jobs config; stopping reaper loop");
        return RuntimeLoopExit::InvalidConfig(error);
    }

    let registry = Arc::new(registry);

    loop {
        if shutdown::is_requested_or_closed(&shutdown) {
            return reaper_shutdown_complete();
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

                let fanout_result = notify_handlers_of_terminal_lease_expirations(
                    registry.as_ref(),
                    &result.summary.terminal_dead_lettered,
                    &mut shutdown,
                )
                .await;
                if matches!(
                    fanout_result,
                    TerminalHookFanoutResult::InterruptedByShutdown
                ) {
                    return reaper_shutdown_complete();
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
            return reaper_shutdown_complete();
        }
    }
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

fn reaper_shutdown_complete() -> RuntimeLoopExit {
    info!("reaper shutdown complete");
    RuntimeLoopExit::Shutdown
}

async fn notify_handlers_of_terminal_lease_expirations(
    registry: &JobRegistry,
    jobs: &[runledger_postgres::jobs::ReapedTerminalLeaseRecord],
    shutdown: &mut watch::Receiver<bool>,
) -> TerminalHookFanoutResult {
    let mut in_flight = JoinSet::new();
    let mut metadata: HashMap<Id, HookMetadata> = HashMap::new();

    for job in jobs {
        if *shutdown.borrow() {
            abort_terminal_hook_fanout(&mut in_flight, &mut metadata).await;
            return TerminalHookFanoutResult::InterruptedByShutdown;
        }

        let Some(handler) = registry.get(job.job_type.as_borrowed()) else {
            continue;
        };

        let context = JobContext {
            job_id: job.job_id,
            run_number: job.run_number,
            attempt: job.attempt,
            organization_id: job.organization_id,
            worker_id: REAPER_WORKER_ID.to_string(),
        };
        let payload = job.payload.clone();
        let dead_letter = JobDeadLetterInfo::new(
            lease_expired_failure(),
            JobDeadLetterReason::LeaseExpired,
            None,
        );
        let hook_meta = HookMetadata {
            job_id: job.job_id.to_string(),
            job_type: job.job_type.to_string(),
            run_number: job.run_number,
            attempt: job.attempt,
        };

        let abort_handle = in_flight.spawn(async move {
            if tokio::time::timeout(
                TERMINAL_HOOK_TIMEOUT,
                handler.on_dead_letter(context, payload, dead_letter),
            )
            .await
            .is_ok()
            {
                HookOutcome::Completed
            } else {
                HookOutcome::TimedOut
            }
        });
        metadata.insert(abort_handle.id(), hook_meta);

        if in_flight.len() >= REAPER_TERMINAL_HOOK_MAX_CONCURRENCY
            && matches!(
                await_next_hook_result_or_shutdown(&mut in_flight, &mut metadata, shutdown).await,
                NextHookResult::InterruptedByShutdown
            )
        {
            abort_terminal_hook_fanout(&mut in_flight, &mut metadata).await;
            return TerminalHookFanoutResult::InterruptedByShutdown;
        }
    }

    while !in_flight.is_empty() {
        if matches!(
            await_next_hook_result_or_shutdown(&mut in_flight, &mut metadata, shutdown).await,
            NextHookResult::InterruptedByShutdown
        ) {
            abort_terminal_hook_fanout(&mut in_flight, &mut metadata).await;
            return TerminalHookFanoutResult::InterruptedByShutdown;
        }
    }

    if !metadata.is_empty() {
        warn!(
            stale_hook_metadata_entries = metadata.len(),
            "reaper hook metadata diverged from in-flight task set; clearing stale metadata"
        );
        metadata.clear();
    }

    TerminalHookFanoutResult::Completed
}

fn lease_expired_failure() -> JobFailure {
    JobFailure::lease_expired(LEASE_EXPIRED_CODE, LEASE_EXPIRED_MESSAGE)
}

#[derive(Debug)]
enum HookOutcome {
    Completed,
    TimedOut,
}

#[derive(Debug, PartialEq, Eq)]
enum TerminalHookFanoutResult {
    Completed,
    InterruptedByShutdown,
}

#[derive(Debug, PartialEq, Eq)]
enum NextHookResult {
    HookSettled,
    InterruptedByShutdown,
}

#[derive(Debug)]
struct HookMetadata {
    job_id: String,
    job_type: String,
    run_number: i32,
    attempt: i32,
}

type HookJoinResult = std::result::Result<(Id, HookOutcome), tokio::task::JoinError>;

async fn await_next_hook_result_or_shutdown(
    in_flight: &mut JoinSet<HookOutcome>,
    metadata: &mut HashMap<Id, HookMetadata>,
    shutdown: &mut watch::Receiver<bool>,
) -> NextHookResult {
    if *shutdown.borrow() {
        return NextHookResult::InterruptedByShutdown;
    }

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return NextHookResult::InterruptedByShutdown;
                }
            }
            result = in_flight.join_next_with_id() => {
                if let Some(result) = result {
                    handle_next_hook_result(result, metadata);
                }
                return NextHookResult::HookSettled;
            }
        }
    }
}

async fn abort_terminal_hook_fanout(
    in_flight: &mut JoinSet<HookOutcome>,
    metadata: &mut HashMap<Id, HookMetadata>,
) {
    if in_flight.is_empty() {
        if !metadata.is_empty() {
            warn!(
                stale_hook_metadata_entries = metadata.len(),
                "reaper hook metadata diverged while shutdown fanout abort had no in-flight tasks"
            );
            metadata.clear();
        }
        return;
    }

    warn!(
        in_flight_terminal_hooks = in_flight.len(),
        drain_timeout_ms = TERMINAL_HOOK_ABORT_DRAIN_TIMEOUT.as_millis(),
        "shutdown requested while terminal failure hooks are running; waiting briefly for in-flight hooks to finish"
    );

    let drain_result = timeout(
        TERMINAL_HOOK_ABORT_DRAIN_TIMEOUT,
        drain_terminal_hook_results(in_flight, metadata),
    )
    .await;
    match drain_result {
        Ok(()) => {}
        Err(_) => {
            warn!(
                remaining_in_flight_hooks = in_flight.len(),
                drain_timeout_ms = TERMINAL_HOOK_ABORT_DRAIN_TIMEOUT.as_millis(),
                "terminal hooks did not finish before shutdown deadline; aborting remaining hooks"
            );
            in_flight.abort_all();

            let abort_drain_result = timeout(
                TERMINAL_HOOK_ABORT_DRAIN_TIMEOUT,
                drain_aborted_terminal_hook_results(in_flight, metadata),
            )
            .await;

            match abort_drain_result {
                Ok(cancelled_hook_count) => {
                    if cancelled_hook_count > 0 {
                        info!(
                            cancelled_hook_count,
                            "terminal failure hooks cancelled due to shutdown request"
                        );
                    }
                }
                Err(_) => {
                    warn!(
                        remaining_in_flight_hooks = in_flight.len(),
                        undrained_hook_metadata_entries = metadata.len(),
                        drain_timeout_ms = TERMINAL_HOOK_ABORT_DRAIN_TIMEOUT.as_millis(),
                        "terminal hook abort drain timed out during shutdown; dropping unresolved hook tasks"
                    );
                }
            }
        }
    }

    if !metadata.is_empty() {
        warn!(
            stale_hook_metadata_entries = metadata.len(),
            "reaper hook metadata remains after shutdown fanout abort; clearing stale metadata"
        );
        metadata.clear();
    }
}

async fn drain_terminal_hook_results(
    in_flight: &mut JoinSet<HookOutcome>,
    metadata: &mut HashMap<Id, HookMetadata>,
) {
    while let Some(result) = in_flight.join_next_with_id().await {
        handle_next_hook_result(result, metadata);
    }
}

async fn drain_aborted_terminal_hook_results(
    in_flight: &mut JoinSet<HookOutcome>,
    metadata: &mut HashMap<Id, HookMetadata>,
) -> usize {
    let mut cancelled_hook_count: usize = 0;

    while let Some(result) = in_flight.join_next_with_id().await {
        match result {
            Err(error) if error.is_cancelled() => {
                let id = error.id();
                if metadata.remove(&id).is_none() {
                    warn!(
                        "terminal failure hook cancellation observed during shutdown; metadata missing in reaper loop"
                    );
                }
                cancelled_hook_count += 1;
            }
            other => handle_next_hook_result(other, metadata),
        }
    }

    cancelled_hook_count
}

fn handle_next_hook_result(result: HookJoinResult, metadata: &mut HashMap<Id, HookMetadata>) {
    match result {
        Ok((id, HookOutcome::Completed)) => {
            if metadata.remove(&id).is_none() {
                warn!("terminal failure hook completed; metadata missing in reaper loop");
            }
        }
        Ok((id, HookOutcome::TimedOut)) => {
            if let Some(meta) = metadata.remove(&id) {
                warn!(
                    job_id = meta.job_id,
                    job_type = meta.job_type,
                    run_number = meta.run_number,
                    attempt = meta.attempt,
                    timeout_ms = TERMINAL_HOOK_TIMEOUT.as_millis(),
                    "terminal failure hook timed out; continuing reaper loop"
                );
            } else {
                warn!(
                    timeout_ms = TERMINAL_HOOK_TIMEOUT.as_millis(),
                    "terminal failure hook timed out; metadata missing in reaper loop"
                );
            }
        }
        Err(error) => {
            let id = error.id();
            if let Some(meta) = metadata.remove(&id) {
                if error.is_panic() {
                    warn!(
                        job_id = meta.job_id,
                        job_type = meta.job_type,
                        run_number = meta.run_number,
                        attempt = meta.attempt,
                        error = %error,
                        "terminal failure hook panicked; continuing reaper loop"
                    );
                } else if error.is_cancelled() {
                    warn!(
                        job_id = meta.job_id,
                        job_type = meta.job_type,
                        run_number = meta.run_number,
                        attempt = meta.attempt,
                        error = %error,
                        "terminal failure hook was cancelled; continuing reaper loop"
                    );
                } else {
                    warn!(
                        job_id = meta.job_id,
                        job_type = meta.job_type,
                        run_number = meta.run_number,
                        attempt = meta.attempt,
                        error = %error,
                        "terminal failure hook join failed; continuing reaper loop"
                    );
                }
            } else if error.is_panic() {
                warn!(
                    error = %error,
                    "terminal failure hook panicked; metadata missing in reaper loop"
                );
            } else if error.is_cancelled() {
                warn!(
                    error = %error,
                    "terminal failure hook was cancelled; metadata missing in reaper loop"
                );
            } else {
                warn!(
                    error = %error,
                    "terminal failure hook join failed; metadata missing in reaper loop"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use runledger_core::jobs::{
        JobCompletion, JobContext, JobDeadLetterInfo, JobDeadLetterReason, JobFailure, JobType,
    };
    use runledger_postgres::jobs::ReapedTerminalLeaseRecord;
    use serde_json::{Value, json};
    use sqlx::types::Uuid;
    use tokio::sync::{Notify, watch};
    use tokio::time::{sleep, timeout};

    use crate::registry::{JobHandler, JobRegistry};

    use super::{TerminalHookFanoutResult, notify_handlers_of_terminal_lease_expirations};

    struct HangingTerminalHookHandler;
    struct ControlledTerminalHookHandler {
        started: Arc<Notify>,
        release: Arc<Notify>,
        completions: Arc<AtomicUsize>,
    }

    struct RecordingDeadLetterHandler {
        dead_letters: Arc<Mutex<Vec<JobDeadLetterInfo>>>,
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

    #[tokio::test]
    async fn notify_handlers_survives_terminal_hook_timeout() {
        let mut registry = JobRegistry::new();
        registry.register(HangingTerminalHookHandler);
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let jobs = vec![ReapedTerminalLeaseRecord {
            job_id: Uuid::now_v7(),
            job_type: runledger_core::jobs::JobTypeName::from_static("jobs.test.reaper.hook.hang"),
            organization_id: None,
            run_number: 1,
            attempt: 1,
            payload: json!({ "kind": "hook-timeout" }),
        }];

        let result = timeout(
            Duration::from_secs(2),
            notify_handlers_of_terminal_lease_expirations(&registry, &jobs, &mut shutdown_rx),
        )
        .await
        .expect("notification pass should return even when terminal hook hangs");
        assert_eq!(result, TerminalHookFanoutResult::Completed);
    }

    #[tokio::test]
    async fn notify_handlers_shutdown_interrupts_terminal_hook_fanout() {
        let mut registry = JobRegistry::new();
        registry.register(HangingTerminalHookHandler);
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let jobs: Vec<_> = (0..256)
            .map(|idx| ReapedTerminalLeaseRecord {
                job_id: Uuid::now_v7(),
                job_type: runledger_core::jobs::JobTypeName::from_static(
                    "jobs.test.reaper.hook.hang",
                ),
                organization_id: None,
                run_number: 1,
                attempt: 1,
                payload: json!({ "kind": "hook-shutdown", "index": idx }),
            })
            .collect();

        tokio::spawn(async move {
            sleep(Duration::from_millis(20)).await;
            let _ = shutdown_tx.send(true);
        });

        let result = timeout(
            Duration::from_secs(2),
            notify_handlers_of_terminal_lease_expirations(&registry, &jobs, &mut shutdown_rx),
        )
        .await
        .expect("notification pass should return quickly after shutdown signal");
        assert_eq!(result, TerminalHookFanoutResult::InterruptedByShutdown);
    }

    #[tokio::test]
    async fn notify_handlers_ignores_non_shutdown_watch_updates() {
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let completions = Arc::new(AtomicUsize::new(0));
        let mut registry = JobRegistry::new();
        registry.register(ControlledTerminalHookHandler {
            started: started.clone(),
            release: release.clone(),
            completions: completions.clone(),
        });
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let update_tx = shutdown_tx.clone();

        let jobs = vec![ReapedTerminalLeaseRecord {
            job_id: Uuid::now_v7(),
            job_type: runledger_core::jobs::JobTypeName::from_static(
                "jobs.test.reaper.hook.controlled",
            ),
            organization_id: None,
            run_number: 1,
            attempt: 1,
            payload: json!({ "kind": "non-shutdown-watch-update" }),
        }];

        tokio::spawn(async move {
            started.notified().await;
            let _ = update_tx.send(false);
            sleep(Duration::from_millis(20)).await;
            release.notify_waiters();
        });

        let result =
            notify_handlers_of_terminal_lease_expirations(&registry, &jobs, &mut shutdown_rx).await;
        drop(shutdown_tx);

        assert_eq!(result, TerminalHookFanoutResult::Completed);
        assert_eq!(completions.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn notify_handlers_reports_lease_expiration_dead_letter_reason() {
        let dead_letters = Arc::new(Mutex::new(Vec::new()));
        let mut registry = JobRegistry::new();
        registry.register(RecordingDeadLetterHandler {
            dead_letters: dead_letters.clone(),
        });
        let (_shutdown_tx, mut shutdown_rx) = watch::channel(false);

        let jobs = vec![ReapedTerminalLeaseRecord {
            job_id: Uuid::now_v7(),
            job_type: runledger_core::jobs::JobTypeName::from_static(
                "jobs.test.reaper.dead_letter.record",
            ),
            organization_id: None,
            run_number: 1,
            attempt: 1,
            payload: json!({ "kind": "lease-expired" }),
        }];

        let result =
            notify_handlers_of_terminal_lease_expirations(&registry, &jobs, &mut shutdown_rx).await;
        assert_eq!(result, TerminalHookFanoutResult::Completed);

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
        assert_eq!(dead_letter.max_attempts, None);
    }
}
