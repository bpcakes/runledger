use std::collections::HashMap;
use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;
use runledger_core::jobs::{JobContext, JobDeadLetterInfo, JobDeadLetterReason};
use runledger_postgres::jobs::{ReapedLeaseDisposition, ReapedLeaseRecord};
use tokio::sync::watch;
use tokio::task::{Id, JoinSet};
use tokio::time::{Duration, timeout};
use tracing::{Instrument, info, info_span, warn};

use crate::registry::JobRegistry;
use crate::shutdown;

const REAPER_WORKER_ID: &str = "reaper";
const REAPER_TERMINAL_HOOK_MAX_CONCURRENCY: usize = 8;
#[cfg(test)]
const TERMINAL_HOOK_TIMEOUT: Duration = Duration::from_millis(100);
#[cfg(not(test))]
const TERMINAL_HOOK_TIMEOUT: Duration = Duration::from_secs(10);
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
    let mut in_flight = JoinSet::new();
    let mut metadata: HashMap<Id, HookMetadata> = HashMap::new();
    let mut started_hook_count = 0;
    let mut skipped_hook_count = 0;
    let mut shutdown_observed = shutdown::is_requested_or_closed(shutdown);
    let mut post_shutdown_admission_budget = if shutdown_observed {
        // If shutdown was already requested after the reaper committed a
        // terminal batch, attempt one bounded concurrency window instead of
        // silently skipping every committed hook.
        REAPER_TERMINAL_HOOK_MAX_CONCURRENCY
    } else {
        0
    };
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

        if !shutdown_observed {
            while in_flight.len() >= REAPER_TERMINAL_HOOK_MAX_CONCURRENCY {
                if wait_for_hook_capacity_or_shutdown(&mut in_flight, &mut metadata, shutdown).await
                    == HookWaitOutcome::ShutdownRequested
                {
                    observe_shutdown_and_grant_admission_budget(
                        &mut shutdown_observed,
                        &mut post_shutdown_admission_budget,
                    );
                    break;
                }
            }

            if !shutdown_observed && shutdown::is_requested_or_closed(shutdown) {
                observe_shutdown_and_grant_admission_budget(
                    &mut shutdown_observed,
                    &mut post_shutdown_admission_budget,
                );
            }
        }

        if shutdown_observed {
            if post_shutdown_admission_budget == 0
                || in_flight.len() >= REAPER_TERMINAL_HOOK_MAX_CONCURRENCY
            {
                skipped_hook_count += 1;
                continue;
            }
            post_shutdown_admission_budget -= 1;
        }

        let context = JobContext {
            job_id: job.job_id,
            run_number: job.run_number,
            attempt: job.attempt,
            organization_id: job.organization_id,
            worker_id: REAPER_WORKER_ID.to_string(),
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

        let abort_handle = in_flight.spawn(
            async move {
                match tokio::time::timeout(
                    TERMINAL_HOOK_TIMEOUT,
                    AssertUnwindSafe(handler.on_dead_letter(context, payload, dead_letter))
                        .catch_unwind(),
                )
                .await
                {
                    Ok(Ok(())) => HookOutcome::Completed,
                    Ok(Err(panic_payload)) => {
                        HookOutcome::Panicked(panic_payload_message(&*panic_payload))
                    }
                    Err(_) => HookOutcome::TimedOut,
                }
            }
            .instrument(hook_span),
        );
        metadata.insert(abort_handle.id(), hook_meta);
        started_hook_count += 1;
    }

    if !shutdown_observed {
        shutdown_observed =
            drain_hook_results_until_complete_or_shutdown(&mut in_flight, &mut metadata, shutdown)
                .await;
    }

    if shutdown_observed {
        if started_hook_count > 0 || skipped_hook_count > 0 {
            warn!(
                started_terminal_hooks = started_hook_count,
                skipped_terminal_hooks_due_to_shutdown = skipped_hook_count,
                in_flight_terminal_hooks = in_flight.len(),
                shutdown_drain_timeout_ms = TERMINAL_HOOK_SHUTDOWN_DRAIN_TIMEOUT.as_millis(),
                "reaper terminal hook fanout interrupted by shutdown; draining in-flight hooks with bounded budget"
            );
        }
        drain_terminal_hooks_for_shutdown(&mut in_flight, &mut metadata).await;
        clear_stale_hook_metadata(&mut metadata);
        return TerminalHookFanoutResult::InterruptedByShutdown {
            started: started_hook_count,
            skipped: skipped_hook_count,
        };
    }

    clear_stale_hook_metadata(&mut metadata);
    TerminalHookFanoutResult::Completed {
        started: started_hook_count,
    }
}

fn observe_shutdown_and_grant_admission_budget(
    shutdown_observed: &mut bool,
    post_shutdown_admission_budget: &mut usize,
) {
    if *shutdown_observed {
        return;
    }

    *shutdown_observed = true;
    *post_shutdown_admission_budget = REAPER_TERMINAL_HOOK_MAX_CONCURRENCY;
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
enum HookOutcome {
    Completed,
    TimedOut,
    Panicked(String),
}

#[derive(Debug)]
struct HookMetadata {
    job_id: String,
    job_type: String,
    run_number: i32,
    attempt: i32,
}

type HookJoinResult = std::result::Result<(Id, HookOutcome), tokio::task::JoinError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HookWaitOutcome {
    HookCompleted,
    ShutdownRequested,
}

async fn wait_for_hook_capacity_or_shutdown(
    in_flight: &mut JoinSet<HookOutcome>,
    metadata: &mut HashMap<Id, HookMetadata>,
    shutdown: &mut watch::Receiver<bool>,
) -> HookWaitOutcome {
    while in_flight.len() >= REAPER_TERMINAL_HOOK_MAX_CONCURRENCY {
        tokio::select! {
            result = in_flight.join_next_with_id() => {
                if let Some(result) = result {
                    handle_next_hook_result(result, metadata);
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

async fn drain_hook_results_until_complete_or_shutdown(
    in_flight: &mut JoinSet<HookOutcome>,
    metadata: &mut HashMap<Id, HookMetadata>,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    while !in_flight.is_empty() {
        tokio::select! {
            result = in_flight.join_next_with_id() => {
                if let Some(result) = result {
                    handle_next_hook_result(result, metadata);
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return true;
                }
            }
        }
    }

    false
}

async fn drain_terminal_hooks_for_shutdown(
    in_flight: &mut JoinSet<HookOutcome>,
    metadata: &mut HashMap<Id, HookMetadata>,
) {
    if in_flight.is_empty() {
        return;
    }

    info!(
        in_flight_terminal_hooks = in_flight.len(),
        timeout_ms = TERMINAL_HOOK_SHUTDOWN_DRAIN_TIMEOUT.as_millis(),
        "shutdown requested; draining in-flight reaper terminal hooks"
    );

    match timeout(
        TERMINAL_HOOK_SHUTDOWN_DRAIN_TIMEOUT,
        drain_hook_results_to_completion(in_flight, metadata),
    )
    .await
    {
        Ok(()) => {}
        Err(_) => {
            warn!(
                remaining_terminal_hooks = in_flight.len(),
                timeout_ms = TERMINAL_HOOK_SHUTDOWN_DRAIN_TIMEOUT.as_millis(),
                "reaper terminal hooks did not finish before shutdown drain deadline; aborting"
            );
            in_flight.abort_all();

            match timeout(
                TERMINAL_HOOK_ABORT_DRAIN_TIMEOUT,
                drain_hook_results_to_completion(in_flight, metadata),
            )
            .await
            {
                Ok(()) => {}
                Err(_) => {
                    warn!(
                        remaining_terminal_hooks = in_flight.len(),
                        timeout_ms = TERMINAL_HOOK_ABORT_DRAIN_TIMEOUT.as_millis(),
                        "reaper terminal hook abort drain timed out during shutdown; dropping unresolved tasks"
                    );
                }
            }
        }
    }
}

async fn drain_hook_results_to_completion(
    in_flight: &mut JoinSet<HookOutcome>,
    metadata: &mut HashMap<Id, HookMetadata>,
) {
    while let Some(result) = in_flight.join_next_with_id().await {
        handle_next_hook_result(result, metadata);
    }
}

fn clear_stale_hook_metadata(metadata: &mut HashMap<Id, HookMetadata>) {
    if metadata.is_empty() {
        return;
    }

    warn!(
        stale_hook_metadata_entries = metadata.len(),
        "reaper hook metadata diverged from in-flight task set; clearing stale metadata"
    );
    metadata.clear();
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
        Ok((id, HookOutcome::Panicked(panic_message))) => {
            if let Some(meta) = metadata.remove(&id) {
                warn!(
                    job_id = meta.job_id,
                    job_type = meta.job_type,
                    run_number = meta.run_number,
                    attempt = meta.attempt,
                    panic = %panic_message,
                    "terminal failure hook panicked; continuing reaper loop"
                );
            } else {
                warn!(
                    panic = %panic_message,
                    "terminal failure hook panicked; metadata missing in reaper loop"
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

fn panic_payload_message(panic_payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = panic_payload.downcast_ref::<String>() {
        return message.clone();
    }

    if let Some(message) = panic_payload.downcast_ref::<&'static str>() {
        return (*message).to_string();
    }

    "non-string panic payload".to_string()
}
