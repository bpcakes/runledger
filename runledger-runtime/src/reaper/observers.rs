use std::collections::HashMap;

use runledger_core::jobs::JobDeadLetterReason;
use runledger_postgres::jobs::{ReapedLeaseDisposition, ReapedLeaseRecord};
use tokio::task::{Id, JoinSet};
use tokio::time::{Duration, timeout};
use tracing::{Instrument, info, info_span, warn};

use crate::observer::{
    JobLeaseReapedDisposition, JobLeaseReapedEvent, JobLifecycleObservers, ObservedJob,
};

const REAPER_REAPED_OBSERVER_MAX_CONCURRENCY: usize = 64;
#[cfg(test)]
const REAPED_OBSERVER_ABORT_DRAIN_TIMEOUT: Duration = Duration::from_millis(50);
#[cfg(not(test))]
const REAPED_OBSERVER_ABORT_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);

pub(super) struct ReapedObserverTasks {
    in_flight: JoinSet<()>,
    metadata: HashMap<Id, ReapedObserverMetadata>,
    max_in_flight: usize,
}

impl ReapedObserverTasks {
    pub(super) fn owned() -> Self {
        Self::with_max_concurrency(REAPER_REAPED_OBSERVER_MAX_CONCURRENCY)
    }

    #[cfg(test)]
    pub(super) fn owned_with_max_concurrency(max_in_flight: usize) -> Self {
        Self::with_max_concurrency(max_in_flight)
    }

    fn with_max_concurrency(max_in_flight: usize) -> Self {
        Self {
            in_flight: JoinSet::new(),
            metadata: HashMap::new(),
            max_in_flight,
        }
    }

    pub(super) fn spawn_batch(
        &mut self,
        observers: &JobLifecycleObservers,
        jobs: &[ReapedLeaseRecord],
    ) {
        if observers.is_empty() {
            return;
        }

        for job in jobs {
            self.drain_finished();

            let observed_job = observed_reaped_job(job);
            let observer_metadata = ReapedObserverMetadata::from(&observed_job);
            if self.in_flight.len() >= self.max_in_flight {
                warn!(
                    reaped_observer_task_cap = self.max_in_flight,
                    reaped_observer_task_current_count = self.in_flight.len(),
                    job_id = observer_metadata.job_id,
                    job_type = observer_metadata.job_type,
                    organization_id = ?observer_metadata.organization_id,
                    run_number = observer_metadata.run_number,
                    attempt = observer_metadata.attempt,
                    max_attempts = observer_metadata.max_attempts,
                    worker_id = observer_metadata.worker_id,
                    "reaper reaped-lease observer task limit reached; dropping newest best-effort observer callback"
                );
                continue;
            }

            let observer_span = reaped_observer_span(&observed_job);
            let event = JobLeaseReapedEvent {
                job: observed_job,
                failure: job.failure.clone(),
                started_without_renewal_heartbeat: job.started_without_renewal_heartbeat,
                disposition: reaped_lease_disposition(&job.disposition),
            };
            let observers = observers.clone();
            let abort_handle = self.in_flight.spawn(
                async move {
                    observers.job_lease_reaped(event).await;
                }
                .instrument(observer_span),
            );
            self.metadata.insert(abort_handle.id(), observer_metadata);
        }

        self.drain_finished();
    }

    pub(super) fn drain_finished(&mut self) {
        drain_completed_reaped_observer_notifications(&mut self.in_flight, &mut self.metadata);
        clear_stale_reaped_observer_metadata_if_idle(
            &self.in_flight,
            &mut self.metadata,
            "reaped lease observer metadata diverged from in-flight task set; clearing stale metadata",
        );
    }

    pub(super) async fn abort_for_shutdown(&mut self) {
        abort_reaped_observer_fanout(&mut self.in_flight, &mut self.metadata).await;
    }

    #[cfg(test)]
    pub(super) fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }
}

fn observed_reaped_job(job: &ReapedLeaseRecord) -> ObservedJob {
    ObservedJob {
        job_id: job.job_id,
        job_type: job.job_type.clone(),
        organization_id: job.organization_id,
        run_number: job.run_number,
        attempt: job.attempt,
        max_attempts: job.max_attempts,
        worker_id: job
            .worker_id
            .clone()
            .unwrap_or_else(|| "unknown-worker".to_owned()),
    }
}

fn reaped_observer_span(job: &ObservedJob) -> tracing::Span {
    info_span!(
        "reaped_lease_observer",
        sentry.name = %job.job_type,
        sentry.op = "runledger.reaper.reaped_lease_observer",
        job_id = %job.job_id,
        job_type = %job.job_type,
        organization_id = ?job.organization_id,
        run_number = job.run_number,
        attempt = job.attempt,
        max_attempts = job.max_attempts,
        worker_id = %job.worker_id,
    )
}

fn reaped_lease_disposition(disposition: &ReapedLeaseDisposition) -> JobLeaseReapedDisposition {
    match disposition {
        ReapedLeaseDisposition::ReleasedToPending => JobLeaseReapedDisposition::ReleasedToPending,
        ReapedLeaseDisposition::RetryScheduled {
            retry_delay_ms,
            next_run_at,
        } => JobLeaseReapedDisposition::RetryScheduled {
            retry_delay_ms: *retry_delay_ms,
            next_run_at: *next_run_at,
        },
        ReapedLeaseDisposition::DeadLetteredTerminal { .. } => {
            JobLeaseReapedDisposition::DeadLettered {
                reason: JobDeadLetterReason::LeaseExpired,
            }
        }
        #[allow(
            unreachable_patterns,
            reason = "map future non-exhaustive persistence dispositions to the public unknown variant"
        )]
        _ => JobLeaseReapedDisposition::Unknown,
    }
}

fn drain_completed_reaped_observer_notifications(
    in_flight: &mut JoinSet<()>,
    metadata: &mut HashMap<Id, ReapedObserverMetadata>,
) {
    while let Some(result) = in_flight.try_join_next_with_id() {
        handle_reaped_observer_join_result(result, metadata);
    }
}

async fn abort_reaped_observer_fanout(
    in_flight: &mut JoinSet<()>,
    metadata: &mut HashMap<Id, ReapedObserverMetadata>,
) {
    drain_completed_reaped_observer_notifications(in_flight, metadata);
    if in_flight.is_empty() {
        clear_stale_reaped_observer_metadata_if_idle(
            in_flight,
            metadata,
            "reaped lease observer metadata diverged while shutdown fanout abort had no in-flight tasks",
        );
        return;
    }

    warn!(
        in_flight_reaped_observer_callbacks = in_flight.len(),
        drain_timeout_ms = REAPED_OBSERVER_ABORT_DRAIN_TIMEOUT.as_millis(),
        "shutdown requested while reaped lease observers are running; aborting in-flight callbacks"
    );

    in_flight.abort_all();
    let drain_result = timeout(
        REAPED_OBSERVER_ABORT_DRAIN_TIMEOUT,
        drain_aborted_reaped_observer_notifications(in_flight, metadata),
    )
    .await;

    match drain_result {
        Ok(cancelled_observer_count) => {
            if cancelled_observer_count > 0 {
                info!(
                    cancelled_observer_count,
                    "reaped lease observer callbacks cancelled due to shutdown request"
                );
            }
        }
        Err(_) => {
            warn!(
                remaining_in_flight_observers = in_flight.len(),
                undrained_reaped_observer_metadata_entries = metadata.len(),
                drain_timeout_ms = REAPED_OBSERVER_ABORT_DRAIN_TIMEOUT.as_millis(),
                "reaped lease observer abort drain timed out during shutdown; dropping unresolved observer tasks"
            );
        }
    }

    if !metadata.is_empty() {
        warn!(
            stale_reaped_observer_metadata_entries = metadata.len(),
            "reaped lease observer metadata remains after shutdown fanout abort; clearing stale metadata"
        );
        metadata.clear();
    }
}

async fn drain_aborted_reaped_observer_notifications(
    in_flight: &mut JoinSet<()>,
    metadata: &mut HashMap<Id, ReapedObserverMetadata>,
) -> usize {
    let mut cancelled_observer_count = 0;

    while let Some(result) = in_flight.join_next_with_id().await {
        match result {
            Err(error) if error.is_cancelled() => {
                let id = error.id();
                if metadata.remove(&id).is_none() {
                    warn!(
                        "reaped lease observer cancellation observed during shutdown; metadata missing in reaper loop"
                    );
                }
                cancelled_observer_count += 1;
            }
            other => handle_reaped_observer_join_result(other, metadata),
        }
    }

    cancelled_observer_count
}

fn handle_reaped_observer_join_result(
    result: ReapedObserverJoinResult,
    metadata: &mut HashMap<Id, ReapedObserverMetadata>,
) {
    match result {
        Ok((id, ())) => {
            if metadata.remove(&id).is_none() {
                warn!("reaped lease observer completed; metadata missing in reaper loop");
            }
        }
        Err(error) if error.is_panic() => {
            let id = error.id();
            if let Some(meta) = metadata.remove(&id) {
                warn!(
                    job_id = meta.job_id,
                    job_type = meta.job_type,
                    organization_id = ?meta.organization_id,
                    run_number = meta.run_number,
                    attempt = meta.attempt,
                    max_attempts = meta.max_attempts,
                    worker_id = meta.worker_id,
                    error = %error,
                    "reaped lease observer task panicked after observer-level panic handling"
                );
            } else {
                warn!(
                    error = %error,
                    "reaped lease observer task panicked after observer-level panic handling; metadata missing in reaper loop"
                );
            }
        }
        Err(error) if error.is_cancelled() => {
            let id = error.id();
            if let Some(meta) = metadata.remove(&id) {
                warn!(
                    job_id = meta.job_id,
                    job_type = meta.job_type,
                    organization_id = ?meta.organization_id,
                    run_number = meta.run_number,
                    attempt = meta.attempt,
                    max_attempts = meta.max_attempts,
                    worker_id = meta.worker_id,
                    error = %error,
                    "reaped lease observer task was cancelled outside shutdown abort handling"
                );
            } else {
                warn!(
                    error = %error,
                    "reaped lease observer task was cancelled outside shutdown abort handling; metadata missing in reaper loop"
                );
            }
        }
        Err(error) => {
            let id = error.id();
            if let Some(meta) = metadata.remove(&id) {
                warn!(
                    job_id = meta.job_id,
                    job_type = meta.job_type,
                    organization_id = ?meta.organization_id,
                    run_number = meta.run_number,
                    attempt = meta.attempt,
                    max_attempts = meta.max_attempts,
                    worker_id = meta.worker_id,
                    error = %error,
                    "reaped lease observer task join failed"
                );
            } else {
                warn!(
                    error = %error,
                    "reaped lease observer task join failed; metadata missing in reaper loop"
                );
            }
        }
    }
}

fn clear_stale_reaped_observer_metadata_if_idle(
    in_flight: &JoinSet<()>,
    metadata: &mut HashMap<Id, ReapedObserverMetadata>,
    message: &'static str,
) {
    if in_flight.is_empty() && !metadata.is_empty() {
        warn!(
            stale_reaped_observer_metadata_entries = metadata.len(),
            "{}", message
        );
        metadata.clear();
    }
}

#[derive(Debug)]
struct ReapedObserverMetadata {
    job_id: String,
    job_type: String,
    organization_id: Option<uuid::Uuid>,
    run_number: i32,
    attempt: i32,
    max_attempts: i32,
    worker_id: String,
}

impl From<&ObservedJob> for ReapedObserverMetadata {
    fn from(job: &ObservedJob) -> Self {
        Self {
            job_id: job.job_id.to_string(),
            job_type: job.job_type.to_string(),
            organization_id: job.organization_id,
            run_number: job.run_number,
            attempt: job.attempt,
            max_attempts: job.max_attempts,
            worker_id: job.worker_id.clone(),
        }
    }
}

type ReapedObserverJoinResult = std::result::Result<(Id, ()), tokio::task::JoinError>;
