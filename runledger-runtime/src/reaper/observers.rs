use std::future::Future;
use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;
use runledger_core::jobs::JobDeadLetterReason;
use runledger_postgres::jobs::{ReapedLeaseDisposition, ReapedLeaseRecord};
use tokio::task::JoinSet;
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
    in_flight: JoinSet<ReapedObserverTaskResult>,
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
            self.in_flight.spawn(run_reaped_observer_task(
                observer_metadata,
                async move {
                    observers.job_lease_reaped(event).await;
                }
                .instrument(observer_span),
            ));
        }

        self.drain_finished();
    }

    pub(super) fn drain_finished(&mut self) {
        drain_completed_reaped_observer_notifications(&mut self.in_flight);
    }

    pub(super) async fn abort_for_shutdown(&mut self) {
        abort_reaped_observer_fanout(&mut self.in_flight).await;
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
    in_flight: &mut JoinSet<ReapedObserverTaskResult>,
) {
    while let Some(result) = in_flight.try_join_next() {
        handle_reaped_observer_join_result(result);
    }
}

async fn abort_reaped_observer_fanout(in_flight: &mut JoinSet<ReapedObserverTaskResult>) {
    drain_completed_reaped_observer_notifications(in_flight);
    if in_flight.is_empty() {
        return;
    }

    log_reaped_observer_abort_start(in_flight.len());
    in_flight.abort_all();
    log_reaped_observer_abort_drain(
        timeout(
            REAPED_OBSERVER_ABORT_DRAIN_TIMEOUT,
            drain_aborted_reaped_observer_notifications(in_flight),
        )
        .await,
        in_flight.len(),
    );
}

fn log_reaped_observer_abort_start(in_flight_reaped_observer_callbacks: usize) {
    warn!(
        in_flight_reaped_observer_callbacks,
        drain_timeout_ms = REAPED_OBSERVER_ABORT_DRAIN_TIMEOUT.as_millis(),
        "shutdown requested while reaped lease observers are running; aborting in-flight callbacks"
    );
}

fn log_reaped_observer_abort_drain(
    drain_result: Result<usize, tokio::time::error::Elapsed>,
    remaining_in_flight_observers: usize,
) {
    match drain_result {
        Ok(cancelled_observer_count) => {
            log_reaped_observer_cancelled(cancelled_observer_count);
        }
        Err(_) => log_reaped_observer_abort_timeout(remaining_in_flight_observers),
    }
}

fn log_reaped_observer_cancelled(cancelled_observer_count: usize) {
    if cancelled_observer_count > 0 {
        info!(
            cancelled_observer_count,
            "reaped lease observer callbacks cancelled due to shutdown request"
        );
    }
}

fn log_reaped_observer_abort_timeout(remaining_in_flight_observers: usize) {
    warn!(
        remaining_in_flight_observers,
        drain_timeout_ms = REAPED_OBSERVER_ABORT_DRAIN_TIMEOUT.as_millis(),
        "reaped lease observer abort drain timed out during shutdown; dropping unresolved observer tasks"
    );
}

async fn drain_aborted_reaped_observer_notifications(
    in_flight: &mut JoinSet<ReapedObserverTaskResult>,
) -> usize {
    let mut cancelled_observer_count = 0;

    while let Some(result) = in_flight.join_next().await {
        match result {
            Err(error) if error.is_cancelled() => {
                cancelled_observer_count += 1;
            }
            other => handle_reaped_observer_join_result(other),
        }
    }

    cancelled_observer_count
}

fn handle_reaped_observer_join_result(result: ReapedObserverJoinResult) {
    match result {
        Ok(ReapedObserverTaskResult {
            outcome: ReapedObserverTaskOutcome::Completed,
            ..
        }) => {}
        Ok(ReapedObserverTaskResult {
            metadata,
            outcome: ReapedObserverTaskOutcome::Panicked(panic_message),
        }) => log_reaped_observer_panic(&metadata, &panic_message),
        Err(error) if error.is_cancelled() => log_reaped_observer_cancelled_join(&error),
        Err(error) => log_reaped_observer_join_failure(&error),
    }
}

fn log_reaped_observer_panic(metadata: &ReapedObserverMetadata, panic_message: &str) {
    warn!(
        job_id = metadata.job_id,
        job_type = metadata.job_type,
        organization_id = ?metadata.organization_id,
        run_number = metadata.run_number,
        attempt = metadata.attempt,
        max_attempts = metadata.max_attempts,
        worker_id = metadata.worker_id,
        panic = %panic_message,
        "reaped lease observer task panicked after observer-level panic handling"
    );
}

fn log_reaped_observer_cancelled_join(error: &tokio::task::JoinError) {
    warn!(
        error = %error,
        "reaped lease observer task was cancelled outside shutdown abort handling"
    );
}

fn log_reaped_observer_join_failure(error: &tokio::task::JoinError) {
    warn!(error = %error, "reaped lease observer task join failed");
}

async fn run_reaped_observer_task<F>(
    metadata: ReapedObserverMetadata,
    notification: F,
) -> ReapedObserverTaskResult
where
    F: Future<Output = ()>,
{
    let outcome = match AssertUnwindSafe(notification).catch_unwind().await {
        Ok(()) => ReapedObserverTaskOutcome::Completed,
        Err(panic_payload) => {
            ReapedObserverTaskOutcome::Panicked(panic_payload_message(&*panic_payload))
        }
    };

    ReapedObserverTaskResult { metadata, outcome }
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

#[derive(Debug, PartialEq, Eq)]
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

#[derive(Debug, PartialEq, Eq)]
struct ReapedObserverTaskResult {
    metadata: ReapedObserverMetadata,
    outcome: ReapedObserverTaskOutcome,
}

#[derive(Debug, PartialEq, Eq)]
enum ReapedObserverTaskOutcome {
    Completed,
    Panicked(String),
}

type ReapedObserverJoinResult =
    std::result::Result<ReapedObserverTaskResult, tokio::task::JoinError>;

#[cfg(test)]
mod tests {
    use super::*;

    fn test_metadata() -> ReapedObserverMetadata {
        ReapedObserverMetadata {
            job_id: "0198d600-f47d-7e70-b3ef-161cdd42cabc".to_owned(),
            job_type: "jobs.test.reaper.observer".to_owned(),
            organization_id: None,
            run_number: 2,
            attempt: 3,
            max_attempts: 5,
            worker_id: "worker-reaped-observer".to_owned(),
        }
    }

    #[tokio::test]
    async fn observer_task_returns_metadata_on_success() {
        let metadata = test_metadata();

        let result = run_reaped_observer_task(metadata, async {}).await;

        assert_eq!(result.metadata, test_metadata());
        assert_eq!(result.outcome, ReapedObserverTaskOutcome::Completed);
    }

    #[tokio::test]
    async fn observer_task_normalizes_panic_and_returns_metadata() {
        let metadata = test_metadata();

        let result = run_reaped_observer_task(metadata, async {
            panic!("reaped observer task panic");
        })
        .await;

        assert_eq!(result.metadata, test_metadata());
        assert_eq!(
            result.outcome,
            ReapedObserverTaskOutcome::Panicked("reaped observer task panic".to_owned())
        );
    }
}
