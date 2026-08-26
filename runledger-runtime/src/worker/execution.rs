use std::any::Any;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use runledger_core::jobs::{JobCompletion, JobContext, JobFailure};
use runledger_postgres::QueryErrorKind;
use runledger_postgres::jobs::{self, JobLeaseIdentity, JobRunningUpdate};
use tokio::time::{Duration, Instant, MissedTickBehavior, sleep_until};
use tracing::{Instrument, info, info_span, warn};

use super::completion::{
    CompletionContext, CompletionObservation, complete_job_after_handler,
    complete_job_failure_after_handler,
};
use super::observers::{JobRunningNotification, TerminalJobObserverEvent, TerminalObserverTasks};
use crate::WorkerError;
use crate::observer::{JobLeaseLostEvent, JobLifecycleObservers, ObservedJob};
use crate::registry::JobRegistry;

// Kept stable for clients that already match this code; it also covers leases
// that expired before the worker's lifecycle update reached storage.
const LEASE_OWNER_MISMATCH_CODE: &str = "job.lease_owner_mismatch";
const LEASE_MAINTENANCE_FAILED_CODE: &str = "job.lease_maintenance_failed";
const HANDLER_PANIC_CODE: &str = "job.handler_panic";
const RUNNING_PROGRESS_PERSIST_FAILED_REASON: &str = "RUNNING_PROGRESS_PERSIST_FAILED";
const UNSTARTED_CLAIM_RETRY_DELAY_MS: i32 = 1_000;

enum JobExecutionFailure {
    Handler(JobFailure),
    LeaseMaintenance(JobFailure),
}

/// Executes one claimed job and owns all state that belongs to that claim.
pub(super) struct ClaimedJobExecution {
    pool: runledger_postgres::DbPool,
    registry: Arc<JobRegistry>,
    job: jobs::JobQueueRecord,
    lease_ttl_seconds: i32,
    observers: JobLifecycleObservers,
    terminal_observer_tasks: TerminalObserverTasks,
    worker_id: String,
}

impl ClaimedJobExecution {
    pub(super) fn new(
        pool: runledger_postgres::DbPool,
        registry: Arc<JobRegistry>,
        job: jobs::JobQueueRecord,
        lease_ttl_seconds: i32,
        observers: JobLifecycleObservers,
        terminal_observer_tasks: TerminalObserverTasks,
    ) -> Option<Self> {
        let Some(worker_id) = job.worker_id.clone() else {
            warn!(
                job_id = %job.id,
                run_number = job.run_number,
                attempt = job.attempt,
                "rejecting claimed job without a lease owner; leaving claim for reaper recovery"
            );
            return None;
        };

        Some(Self {
            pool,
            registry,
            job,
            lease_ttl_seconds,
            observers,
            terminal_observer_tasks,
            worker_id,
        })
    }

    pub(super) async fn execute(self) {
        let job_span = info_span!(
            "job",
            sentry.name = %self.job.job_type,
            sentry.op = "runledger.job",
            job_id = %self.job.id,
            job_type = %self.job.job_type,
            run_number = self.job.run_number,
            attempt = self.job.attempt,
            organization_id = ?self.job.organization_id,
            worker_id = %self.worker_id,
        );
        async move {
            let start = Instant::now();
            let context = self.context();
            let observed_job = self.observed_job();

            if !self.mark_job_running_or_abort().await {
                return;
            }
            let mut running_notification =
                JobRunningNotification::spawn(self.observers.clone(), observed_job.clone());

            match self.execute_job_handler_with_heartbeats(&context).await {
                Ok(completion) => {
                    complete_job_after_handler(
                        self.completion_context(&context),
                        completion,
                        CompletionObservation::new(
                            &self.observers,
                            observed_job.clone(),
                            start.elapsed(),
                            &mut running_notification,
                            &self.terminal_observer_tasks,
                        ),
                    )
                    .await;
                }
                Err(JobExecutionFailure::Handler(failure)) => {
                    complete_job_failure_after_handler(
                        self.completion_context(&context),
                        failure,
                        CompletionObservation::new(
                            &self.observers,
                            observed_job.clone(),
                            start.elapsed(),
                            &mut running_notification,
                            &self.terminal_observer_tasks,
                        ),
                    )
                    .await;
                }
                Err(JobExecutionFailure::LeaseMaintenance(failure)) => {
                    self.log_lease_maintenance_abort(&failure);
                    running_notification
                        .spawn_terminal_observer(
                            &self.terminal_observer_tasks,
                            self.observers.clone(),
                            TerminalJobObserverEvent::LeaseLost(JobLeaseLostEvent {
                                job: observed_job.clone(),
                                duration: start.elapsed(),
                                failure,
                            }),
                        )
                        .await;
                }
            }

            info!(
                job_id = %self.job.id,
                attempt = self.job.attempt,
                run_number = self.job.run_number,
                elapsed_ms = start.elapsed().as_millis(),
                "job processed"
            );
        }
        .instrument(job_span)
        .await;
    }

    fn context(&self) -> JobContext {
        JobContext {
            job_id: self.job.id,
            run_number: self.job.run_number,
            attempt: self.job.attempt,
            organization_id: self.job.organization_id,
            worker_id: self.worker_id.clone(),
            checkpoint: self.job.checkpoint.clone(),
        }
    }

    fn observed_job(&self) -> ObservedJob {
        ObservedJob {
            job_id: self.job.id,
            job_type: self.job.job_type.clone(),
            organization_id: self.job.organization_id,
            run_number: self.job.run_number,
            attempt: self.job.attempt,
            max_attempts: self.job.max_attempts,
            worker_id: self.worker_id.to_owned(),
        }
    }

    fn lease_identity(&self) -> JobLeaseIdentity<'_> {
        JobLeaseIdentity::new(
            self.job.id,
            self.job.run_number,
            self.job.attempt,
            &self.worker_id,
        )
    }

    fn completion_context<'execution, 'context>(
        &'execution self,
        context: &'context JobContext,
    ) -> CompletionContext<'execution, 'context> {
        CompletionContext::new(
            &self.pool,
            self.registry.as_ref(),
            context,
            &self.job,
            self.lease_identity(),
        )
    }

    fn log_lease_maintenance_abort(&self, failure: &JobFailure) {
        warn!(
            job_id = %self.job.id,
            attempt = self.job.attempt,
            failure_code = failure.code,
            "job processing aborted because durable lease maintenance was lost"
        );
    }

    async fn mark_job_running_or_abort(&self) -> bool {
        let running_update = JobRunningUpdate {
            progress_done: None,
            progress_total: None,
            checkpoint: None,
        };

        let Err(source) =
            jobs::mark_job_running_for_lease(&self.pool, self.lease_identity(), &running_update)
                .await
        else {
            return true;
        };

        self.handle_running_progress_persist_failure(source).await;
        false
    }

    async fn handle_running_progress_persist_failure(&self, source: runledger_postgres::Error) {
        let lease_owner_mismatch = is_lease_owner_mismatch_error(&source);
        let error = WorkerError::SetRunningProgress {
            job_id: self.job.id,
            attempt: self.job.attempt,
            source,
        };

        if lease_owner_mismatch {
            warn!(
                %error,
                job_id = %self.job.id,
                attempt = self.job.attempt,
                "aborting job before execution because lease ownership was already lost"
            );
            return;
        }

        match jobs::release_unstarted_job_claim(
            &self.pool,
            self.lease_identity(),
            RUNNING_PROGRESS_PERSIST_FAILED_REASON,
            UNSTARTED_CLAIM_RETRY_DELAY_MS,
        )
        .await
        {
            Ok(()) => {
                warn!(
                    %error,
                    job_id = %self.job.id,
                    attempt = self.job.attempt,
                    "running progress could not be persisted; released unstarted claim back to pending"
                );
            }
            Err(release_error) => {
                let no_longer_releasable =
                    is_unstarted_claim_release_not_applicable_error(&release_error);
                let release_error = WorkerError::ReleaseUnstartedClaim {
                    job_id: self.job.id,
                    attempt: self.job.attempt,
                    source: release_error,
                };
                if no_longer_releasable {
                    warn!(
                        %error,
                        %release_error,
                        job_id = %self.job.id,
                        attempt = self.job.attempt,
                        "running progress could not be persisted; unstarted release no longer applies and the job will continue under the current lease owner"
                    );
                    return;
                }

                warn!(
                    %error,
                    %release_error,
                    job_id = %self.job.id,
                    attempt = self.job.attempt,
                    "running progress could not be persisted; leaving claim for reaper recovery"
                );
            }
        }
    }

    async fn execute_job_handler_with_heartbeats(
        &self,
        context: &JobContext,
    ) -> Result<JobCompletion, JobExecutionFailure> {
        let registry = Arc::clone(&self.registry);
        let mut execution = Box::pin(
            AssertUnwindSafe(execute_job_handler(registry, context, &self.job)).catch_unwind(),
        );
        let timeout_deadline =
            Instant::now() + Duration::from_secs(self.job.timeout_seconds.max(1) as u64);
        let mut timeout = Box::pin(sleep_until(timeout_deadline));

        let heartbeat_budget = heartbeat_maintenance_budget(self.lease_ttl_seconds);
        let mut ticker = tokio::time::interval(heartbeat_budget);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticker.tick().await;
        let mut pending_heartbeats = FuturesUnordered::new();

        loop {
            tokio::select! {
                result = &mut execution => {
                    return match result {
                        Ok(result) => result.map_err(JobExecutionFailure::Handler),
                        Err(panic_payload) => {
                            Err(JobExecutionFailure::Handler(handler_panic_failure(panic_payload)))
                        }
                    };
                }
                _ = &mut timeout => {
                    return Err(JobExecutionFailure::Handler(JobFailure::timeout(
                        "job.timeout_exceeded",
                        "Job exceeded the configured timeout.",
                    )));
                }
                Some(result) = pending_heartbeats.next(), if !pending_heartbeats.is_empty() => {
                    let result = match result {
                        Ok(result) => result,
                        Err(_) => {
                            warn!(
                                job_id = %self.job.id,
                                attempt = self.job.attempt,
                                heartbeat_budget_ms = heartbeat_budget.as_millis(),
                                "aborting job because lease heartbeat exceeded its maintenance budget"
                            );
                            return Err(JobExecutionFailure::LeaseMaintenance(
                                lease_maintenance_failure(),
                            ));
                        }
                    };
                    if let Err(error) = result {
                        let lease_owner_mismatch = is_lease_owner_mismatch_error(&error);
                        let error = WorkerError::Heartbeat {
                            job_id: self.job.id,
                            attempt: self.job.attempt,
                            source: error,
                        };

                        if lease_owner_mismatch {
                            warn!(%error, job_id = %self.job.id, "job heartbeat lost lease ownership");
                            return Err(JobExecutionFailure::LeaseMaintenance(
                                lease_owner_mismatch_failure(),
                            ));
                        }

                        warn!(
                            %error,
                            job_id = %self.job.id,
                            "aborting job because lease heartbeat could not be persisted"
                        );
                        return Err(JobExecutionFailure::LeaseMaintenance(
                            lease_maintenance_failure(),
                        ));
                    }
                }
                _ = ticker.tick(), if pending_heartbeats.is_empty() => {
                    // Keep the heartbeat future in the select set instead of
                    // awaiting it inside this branch. A handler can be waiting
                    // to finish and commit a progress transaction that owns the
                    // same job-row lock the heartbeat needs.
                    // Bound the entire attempt, including pool acquisition, to
                    // one third of the lease. A timed-out SQLx transaction may
                    // still need asynchronous rollback cleanup, but handler
                    // polling stops with another third of the lease available.
                    pending_heartbeats.push(tokio::time::timeout(
                        heartbeat_budget,
                        jobs::heartbeat_job_for_lease(
                            &self.pool,
                            self.lease_identity(),
                            self.lease_ttl_seconds,
                        ),
                    ));
                }
            }
        }
    }
}

async fn execute_job_handler(
    registry: Arc<JobRegistry>,
    context: &JobContext,
    job: &jobs::JobQueueRecord,
) -> Result<JobCompletion, JobFailure> {
    let Some(handler) = registry.get(job.job_type.as_borrowed()) else {
        return Err(JobFailure::terminal(
            "job.handler_not_registered",
            "No handler is registered for this job type.",
        ));
    };

    handler.execute(context.clone(), job.payload.clone()).await
}

pub(super) fn lease_owner_mismatch_failure() -> JobFailure {
    JobFailure::lease_expired(
        LEASE_OWNER_MISMATCH_CODE,
        "Job lease ownership was lost during processing.",
    )
}

fn lease_maintenance_failure() -> JobFailure {
    JobFailure::lease_expired(
        LEASE_MAINTENANCE_FAILED_CODE,
        "Job lease could not be durably maintained during processing.",
    )
}

fn handler_panic_failure(panic_payload: Box<dyn Any + Send>) -> JobFailure {
    JobFailure::panicked(
        HANDLER_PANIC_CODE,
        format!(
            "Job handler panicked: {}",
            panic_payload_message(&*panic_payload)
        ),
    )
}

fn panic_payload_message(panic_payload: &(dyn Any + Send)) -> String {
    if let Some(message) = panic_payload.downcast_ref::<String>() {
        return message.clone();
    }

    if let Some(message) = panic_payload.downcast_ref::<&'static str>() {
        return (*message).to_string();
    }

    "non-string panic payload".to_string()
}

fn has_query_error_kind(error: &runledger_postgres::Error, expected_kind: QueryErrorKind) -> bool {
    matches!(
        error,
        runledger_postgres::Error::QueryError(query_error)
            if query_error.kind() == Some(expected_kind)
    )
}

pub(super) fn is_lease_owner_mismatch_error(error: &runledger_postgres::Error) -> bool {
    has_query_error_kind(error, QueryErrorKind::JobLeaseOwnerMismatch)
}

fn is_unstarted_claim_release_not_applicable_error(error: &runledger_postgres::Error) -> bool {
    has_query_error_kind(error, QueryErrorKind::JobUnstartedClaimReleaseNotApplicable)
}

fn heartbeat_maintenance_budget(lease_ttl_seconds: i32) -> Duration {
    // Use millisecond precision so directly constructed one- and two-second
    // configurations retain the same three-part lease budget as longer TTLs.
    let lease_ttl_millis = lease_ttl_seconds.max(1) as u64 * 1_000;
    Duration::from_millis((lease_ttl_millis / 3).max(1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn heartbeat_budget_preserves_one_third_for_short_and_default_leases() {
        assert_eq!(heartbeat_maintenance_budget(1), Duration::from_millis(333));
        assert_eq!(heartbeat_maintenance_budget(2), Duration::from_millis(666));
        assert_eq!(heartbeat_maintenance_budget(60), Duration::from_secs(20));
    }
}
