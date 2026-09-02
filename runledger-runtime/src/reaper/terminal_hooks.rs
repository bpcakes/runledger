use std::collections::HashMap;
use std::future::Future;

use runledger_core::jobs::{JobContext, JobDeadLetterInfo, JobDeadLetterReason};
use runledger_postgres::jobs::{ReapedLeaseDisposition, ReapedLeaseRecord};
use tokio::sync::watch;
use tokio::task::{Id, JoinSet};
use tokio::time::{Duration, timeout};
use tracing::{Instrument, info, info_span, warn};

use crate::dead_letter_hook::{
    DEAD_LETTER_HOOK_TIMEOUT, DeadLetterHookOutcome, invoke_dead_letter_hook,
};
use crate::registry::JobRegistry;
use crate::shutdown;

const REAPER_WORKER_ID: &str = "reaper";
const REAPER_TERMINAL_HOOK_MAX_CONCURRENCY: usize = 8;
#[cfg(test)]
const TERMINAL_HOOK_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_millis(150);
#[cfg(not(test))]
const TERMINAL_HOOK_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(test)]
const TERMINAL_HOOK_ABORT_DRAIN_TIMEOUT: Duration = Duration::from_millis(50);
#[cfg(not(test))]
const TERMINAL_HOOK_ABORT_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);

pub(super) async fn notify_handlers_of_terminal_lease_expirations(
    registry: &JobRegistry,
    jobs: &[ReapedLeaseRecord],
    shutdown: &mut watch::Receiver<bool>,
) -> TerminalHookFanoutResult {
    notify_handlers_of_terminal_lease_expirations_inner(registry, jobs, shutdown, || {}).await
}

#[cfg(test)]
pub(super) async fn notify_handlers_of_terminal_lease_expirations_with_before_first_hook_admission<
    F,
>(
    registry: &JobRegistry,
    jobs: &[ReapedLeaseRecord],
    shutdown: &mut watch::Receiver<bool>,
    before_first_hook_admission: F,
) -> TerminalHookFanoutResult
where
    F: FnOnce(),
{
    notify_handlers_of_terminal_lease_expirations_inner(
        registry,
        jobs,
        shutdown,
        before_first_hook_admission,
    )
    .await
}

async fn notify_handlers_of_terminal_lease_expirations_inner<F>(
    registry: &JobRegistry,
    jobs: &[ReapedLeaseRecord],
    shutdown: &mut watch::Receiver<bool>,
    before_first_hook_admission: F,
) -> TerminalHookFanoutResult
where
    F: FnOnce(),
{
    let mut fanout = TerminalHookFanout::new(shutdown);
    let mut before_first_hook_admission = Some(before_first_hook_admission);

    for job in jobs {
        let ReapedLeaseDisposition::DeadLetteredTerminal { payload } = &job.disposition else {
            continue;
        };

        let Some(handler) = registry.get(job.job_type.as_borrowed()) else {
            continue;
        };

        if let Some(before_first_hook_admission) = before_first_hook_admission.take() {
            before_first_hook_admission();
        }

        fanout.wait_for_capacity_or_observe_shutdown(shutdown).await;
        if !fanout.reserve_hook_admission() {
            continue;
        }

        let context = JobContext {
            job_id: job.job_id,
            run_number: job.run_number,
            attempt: job.attempt,
            organization_id: job.organization_id,
            worker_id: REAPER_WORKER_ID.to_string(),
            checkpoint: job.checkpoint.clone(),
        };
        let payload = payload.clone();
        let dead_letter = JobDeadLetterInfo::new(
            job.failure.clone(),
            JobDeadLetterReason::LeaseExpired,
            Some(job.max_attempts),
        );
        let hook_meta = HookMetadata {
            job_id: job.job_id.to_string(),
            job_type: job.job_type.to_string(),
            run_number: job.run_number,
            attempt: job.attempt,
        };
        let hook_span = info_span!(
            "reaper_terminal_hook",
            sentry.name = %job.job_type,
            sentry.op = "runledger.reaper.terminal_hook",
            job_id = %job.job_id,
            job_type = %job.job_type,
            run_number = job.run_number,
            attempt = job.attempt,
        );

        let hook = async move {
            invoke_dead_letter_hook(handler.on_dead_letter(context, payload, dead_letter)).await
        }
        .instrument(hook_span);
        fanout.spawn_hook(hook, hook_meta);
    }

    if !fanout.shutdown_observed {
        fanout
            .drain_results_until_complete_or_shutdown(shutdown)
            .await;
    }

    fanout.finish().await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TerminalHookFanoutResult {
    Completed { started: usize },
    InterruptedByShutdown { started: usize, skipped: usize },
}

impl TerminalHookFanoutResult {
    pub(super) fn interrupted_by_shutdown(self) -> bool {
        matches!(self, Self::InterruptedByShutdown { .. })
    }
}

#[derive(Debug)]
struct HookMetadata {
    job_id: String,
    job_type: String,
    run_number: i32,
    attempt: i32,
}

struct TerminalHookFanout {
    in_flight: JoinSet<DeadLetterHookOutcome>,
    metadata: HashMap<Id, HookMetadata>,
    started_hook_count: usize,
    skipped_hook_count: usize,
    shutdown_observed: bool,
    post_shutdown_admission_budget: usize,
}

impl TerminalHookFanout {
    fn new(shutdown: &watch::Receiver<bool>) -> Self {
        let shutdown_observed = shutdown::is_requested_or_closed(shutdown);
        let post_shutdown_admission_budget = if shutdown_observed {
            // If shutdown was already requested after the reaper committed a
            // terminal batch, attempt one bounded concurrency window instead of
            // silently skipping every committed hook.
            REAPER_TERMINAL_HOOK_MAX_CONCURRENCY
        } else {
            0
        };

        Self {
            in_flight: JoinSet::new(),
            metadata: HashMap::new(),
            started_hook_count: 0,
            skipped_hook_count: 0,
            shutdown_observed,
            post_shutdown_admission_budget,
        }
    }

    async fn wait_for_capacity_or_observe_shutdown(
        &mut self,
        shutdown: &mut watch::Receiver<bool>,
    ) {
        if self.shutdown_observed {
            return;
        }

        while self.in_flight.len() >= REAPER_TERMINAL_HOOK_MAX_CONCURRENCY {
            if self.wait_for_hook_capacity_or_shutdown(shutdown).await
                == HookWaitOutcome::ShutdownRequested
            {
                self.observe_shutdown_and_grant_admission_budget();
                break;
            }
        }

        if !self.shutdown_observed && shutdown::is_requested_or_closed(shutdown) {
            self.observe_shutdown_and_grant_admission_budget();
        }
    }

    fn observe_shutdown_and_grant_admission_budget(&mut self) {
        if self.shutdown_observed {
            return;
        }

        self.shutdown_observed = true;
        self.post_shutdown_admission_budget = REAPER_TERMINAL_HOOK_MAX_CONCURRENCY;
    }

    fn reserve_hook_admission(&mut self) -> bool {
        if !self.shutdown_observed {
            return true;
        }

        if self.post_shutdown_admission_budget == 0
            || self.in_flight.len() >= REAPER_TERMINAL_HOOK_MAX_CONCURRENCY
        {
            self.skipped_hook_count += 1;
            return false;
        }

        self.post_shutdown_admission_budget -= 1;
        true
    }

    fn spawn_hook<F>(&mut self, hook: F, hook_meta: HookMetadata)
    where
        F: Future<Output = DeadLetterHookOutcome> + Send + 'static,
    {
        let abort_handle = self.in_flight.spawn(hook);
        self.metadata.insert(abort_handle.id(), hook_meta);
        self.started_hook_count += 1;
    }

    async fn wait_for_hook_capacity_or_shutdown(
        &mut self,
        shutdown: &mut watch::Receiver<bool>,
    ) -> HookWaitOutcome {
        while self.in_flight.len() >= REAPER_TERMINAL_HOOK_MAX_CONCURRENCY {
            tokio::select! {
                result = self.in_flight.join_next_with_id() => {
                    if let Some(result) = result {
                        self.handle_next_hook_result(result);
                        return HookWaitOutcome::HookCompleted;
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return HookWaitOutcome::ShutdownRequested;
                    }
                }
            }
        }

        HookWaitOutcome::HookCompleted
    }

    async fn drain_results_until_complete_or_shutdown(
        &mut self,
        shutdown: &mut watch::Receiver<bool>,
    ) {
        while !self.in_flight.is_empty() {
            tokio::select! {
                result = self.in_flight.join_next_with_id() => {
                    if let Some(result) = result {
                        self.handle_next_hook_result(result);
                    }
                }
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        self.shutdown_observed = true;
                        return;
                    }
                }
            }
        }
    }

    async fn finish(mut self) -> TerminalHookFanoutResult {
        if self.shutdown_observed {
            if self.started_hook_count > 0 || self.skipped_hook_count > 0 {
                warn!(
                    started_terminal_hooks = self.started_hook_count,
                    skipped_terminal_hooks_due_to_shutdown = self.skipped_hook_count,
                    in_flight_terminal_hooks = self.in_flight.len(),
                    shutdown_drain_timeout_ms = TERMINAL_HOOK_SHUTDOWN_DRAIN_TIMEOUT.as_millis(),
                    "reaper terminal hook fanout interrupted by shutdown; draining in-flight hooks with bounded budget"
                );
            }
            self.drain_terminal_hooks_for_shutdown().await;
            self.clear_stale_hook_metadata();
            return TerminalHookFanoutResult::InterruptedByShutdown {
                started: self.started_hook_count,
                skipped: self.skipped_hook_count,
            };
        }

        self.clear_stale_hook_metadata();
        TerminalHookFanoutResult::Completed {
            started: self.started_hook_count,
        }
    }

    async fn drain_terminal_hooks_for_shutdown(&mut self) {
        if self.in_flight.is_empty() {
            return;
        }

        log_terminal_hook_shutdown_drain_start(self.in_flight.len());

        if timeout(
            TERMINAL_HOOK_SHUTDOWN_DRAIN_TIMEOUT,
            self.drain_hook_results_to_completion(),
        )
        .await
        .is_err()
        {
            self.abort_remaining_hooks_after_drain_timeout().await;
        }
    }

    async fn abort_remaining_hooks_after_drain_timeout(&mut self) {
        log_terminal_hook_shutdown_drain_timeout(self.in_flight.len());
        self.in_flight.abort_all();

        if timeout(
            TERMINAL_HOOK_ABORT_DRAIN_TIMEOUT,
            self.drain_hook_results_to_completion(),
        )
        .await
        .is_err()
        {
            log_terminal_hook_abort_drain_timeout(self.in_flight.len());
        }
    }

    async fn drain_hook_results_to_completion(&mut self) {
        while let Some(result) = self.in_flight.join_next_with_id().await {
            self.handle_next_hook_result(result);
        }
    }

    fn clear_stale_hook_metadata(&mut self) {
        if self.metadata.is_empty() {
            return;
        }

        warn!(
            stale_hook_metadata_entries = self.metadata.len(),
            "reaper hook metadata diverged from in-flight task set; clearing stale metadata"
        );
        self.metadata.clear();
    }

    fn handle_next_hook_result(&mut self, result: HookJoinResult) {
        match result {
            Ok((id, DeadLetterHookOutcome::Completed)) => {
                log_completed_hook(self.metadata.remove(&id).is_none());
            }
            Ok((id, DeadLetterHookOutcome::TimedOut)) => {
                log_timed_out_hook(self.metadata.remove(&id));
            }
            Ok((id, DeadLetterHookOutcome::Panicked(panic_message))) => {
                log_panicked_hook(self.metadata.remove(&id), &panic_message);
            }
            Err(error) => {
                log_join_failed_hook(self.metadata.remove(&error.id()), &error);
            }
        }
    }
}

fn log_terminal_hook_shutdown_drain_start(in_flight_terminal_hooks: usize) {
    info!(
        in_flight_terminal_hooks,
        timeout_ms = TERMINAL_HOOK_SHUTDOWN_DRAIN_TIMEOUT.as_millis(),
        "shutdown requested; draining in-flight reaper terminal hooks"
    );
}

fn log_terminal_hook_shutdown_drain_timeout(remaining_terminal_hooks: usize) {
    warn!(
        remaining_terminal_hooks,
        timeout_ms = TERMINAL_HOOK_SHUTDOWN_DRAIN_TIMEOUT.as_millis(),
        "reaper terminal hooks did not finish before shutdown drain deadline; aborting"
    );
}

fn log_terminal_hook_abort_drain_timeout(remaining_terminal_hooks: usize) {
    warn!(
        remaining_terminal_hooks,
        timeout_ms = TERMINAL_HOOK_ABORT_DRAIN_TIMEOUT.as_millis(),
        "reaper terminal hook abort drain timed out during shutdown; dropping unresolved tasks"
    );
}

fn log_completed_hook(metadata_missing: bool) {
    if metadata_missing {
        warn!("terminal failure hook completed; metadata missing in reaper loop");
    }
}

fn log_timed_out_hook(meta: Option<HookMetadata>) {
    match meta {
        Some(meta) => meta.warn_timed_out(),
        None => warn_timed_out_hook_missing_metadata(),
    }
}

fn log_panicked_hook(meta: Option<HookMetadata>, panic_message: &str) {
    match meta {
        Some(meta) => meta.warn_panicked(panic_message),
        None => warn_panicked_hook_missing_metadata(panic_message),
    }
}

fn log_join_failed_hook(meta: Option<HookMetadata>, error: &tokio::task::JoinError) {
    match (meta, join_error_kind(error)) {
        (Some(meta), JoinErrorKind::Panic) => meta.warn_join_panic(error),
        (Some(meta), JoinErrorKind::Cancelled) => meta.warn_join_cancelled(error),
        (Some(meta), JoinErrorKind::Other) => meta.warn_join_failed(error),
        (None, JoinErrorKind::Panic) => warn_join_panic_missing_metadata(error),
        (None, JoinErrorKind::Cancelled) => warn_join_cancelled_missing_metadata(error),
        (None, JoinErrorKind::Other) => warn_join_failed_missing_metadata(error),
    }
}

fn join_error_kind(error: &tokio::task::JoinError) -> JoinErrorKind {
    if error.is_panic() {
        JoinErrorKind::Panic
    } else if error.is_cancelled() {
        JoinErrorKind::Cancelled
    } else {
        JoinErrorKind::Other
    }
}

impl HookMetadata {
    fn warn_timed_out(&self) {
        warn!(
            job_id = self.job_id,
            job_type = self.job_type,
            run_number = self.run_number,
            attempt = self.attempt,
            timeout_ms = DEAD_LETTER_HOOK_TIMEOUT.as_millis(),
            "terminal failure hook timed out; continuing reaper loop"
        );
    }

    fn warn_panicked(&self, panic_message: &str) {
        warn!(
            job_id = self.job_id,
            job_type = self.job_type,
            run_number = self.run_number,
            attempt = self.attempt,
            panic = %panic_message,
            "terminal failure hook panicked; continuing reaper loop"
        );
    }

    fn warn_join_panic(&self, error: &tokio::task::JoinError) {
        warn!(
            job_id = self.job_id,
            job_type = self.job_type,
            run_number = self.run_number,
            attempt = self.attempt,
            error = %error,
            "terminal failure hook panicked; continuing reaper loop"
        );
    }

    fn warn_join_cancelled(&self, error: &tokio::task::JoinError) {
        warn!(
            job_id = self.job_id,
            job_type = self.job_type,
            run_number = self.run_number,
            attempt = self.attempt,
            error = %error,
            "terminal failure hook was cancelled; continuing reaper loop"
        );
    }

    fn warn_join_failed(&self, error: &tokio::task::JoinError) {
        warn!(
            job_id = self.job_id,
            job_type = self.job_type,
            run_number = self.run_number,
            attempt = self.attempt,
            error = %error,
            "terminal failure hook join failed; continuing reaper loop"
        );
    }
}

fn warn_timed_out_hook_missing_metadata() {
    warn!(
        timeout_ms = DEAD_LETTER_HOOK_TIMEOUT.as_millis(),
        "terminal failure hook timed out; metadata missing in reaper loop"
    );
}

fn warn_panicked_hook_missing_metadata(panic_message: &str) {
    warn!(
        panic = %panic_message,
        "terminal failure hook panicked; metadata missing in reaper loop"
    );
}

fn warn_join_panic_missing_metadata(error: &tokio::task::JoinError) {
    warn!(
        error = %error,
        "terminal failure hook panicked; metadata missing in reaper loop"
    );
}

fn warn_join_cancelled_missing_metadata(error: &tokio::task::JoinError) {
    warn!(
        error = %error,
        "terminal failure hook was cancelled; metadata missing in reaper loop"
    );
}

fn warn_join_failed_missing_metadata(error: &tokio::task::JoinError) {
    warn!(
        error = %error,
        "terminal failure hook join failed; metadata missing in reaper loop"
    );
}

type HookJoinResult = std::result::Result<(Id, DeadLetterHookOutcome), tokio::task::JoinError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HookWaitOutcome {
    HookCompleted,
    ShutdownRequested,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinErrorKind {
    Panic,
    Cancelled,
    Other,
}
