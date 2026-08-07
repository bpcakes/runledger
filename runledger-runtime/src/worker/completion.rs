use std::time::Duration;

use runledger_core::jobs::{
    JobCompletion, JobCompletionDisposition, JobContext, JobDeadLetterInfo, JobFailure,
    JobFailureKind,
};
use runledger_postgres::QueryErrorKind;
use runledger_postgres::jobs::{
    self, JobCompletionUpdate, JobContinuationUpdate, JobFailureUpdate, JobLeaseIdentity,
};
use tracing::{error, info, warn};

use super::dead_letter::notify_handler_of_dead_letter;
use super::observers::{JobRunningNotification, TerminalJobObserverEvent, TerminalObserverTasks};
use super::{is_lease_owner_mismatch_error, lease_owner_mismatch_failure};
use crate::WorkerError;
use crate::observer::{
    JobCompletionPersistFailedEvent, JobCompletionPersistenceOperation, JobContinuedEvent,
    JobFailedEvent, JobFailureDisposition, JobLeaseLostEvent, JobLifecycleObservers,
    JobSucceededEvent, ObservedJob,
};
use crate::registry::JobRegistry;

pub(super) struct CompletionObservation<'a> {
    observers: &'a JobLifecycleObservers,
    observed_job: ObservedJob,
    duration: Duration,
    running_notification: &'a mut JobRunningNotification,
    terminal_observer_tasks: &'a TerminalObserverTasks,
}

impl<'a> CompletionObservation<'a> {
    pub(super) fn new(
        observers: &'a JobLifecycleObservers,
        observed_job: ObservedJob,
        duration: Duration,
        running_notification: &'a mut JobRunningNotification,
        terminal_observer_tasks: &'a TerminalObserverTasks,
    ) -> Self {
        Self {
            observers,
            observed_job,
            duration,
            running_notification,
            terminal_observer_tasks,
        }
    }
}

#[derive(Clone, Copy)]
struct CompletionLease<'context, 'worker> {
    context: &'context JobContext,
    identity: JobLeaseIdentity<'worker>,
}

async fn handle_completion_persist_failure(
    observation: CompletionObservation<'_>,
    job: &jobs::JobQueueRecord,
    operation: JobCompletionPersistenceOperation,
    error: runledger_postgres::Error,
    log_error: impl FnOnce(runledger_postgres::Error, bool),
) {
    let lease_owner_mismatch = is_lease_owner_mismatch_error(&error);
    let terminal_event = if observation.observers.is_empty() {
        None
    } else if lease_owner_mismatch {
        Some(TerminalJobObserverEvent::LeaseLost(JobLeaseLostEvent {
            job: observation.observed_job,
            duration: observation.duration,
            failure: lease_owner_mismatch_failure(),
        }))
    } else {
        Some(TerminalJobObserverEvent::CompletionPersistFailed(
            JobCompletionPersistFailedEvent {
                job: observation.observed_job,
                duration: observation.duration,
                operation,
                error: completion_persist_error_diagnostic(&error),
            },
        ))
    };
    log_error(error, lease_owner_mismatch);

    let Some(terminal_event) = terminal_event else {
        return;
    };
    observation
        .running_notification
        .spawn_terminal_observer(
            observation.terminal_observer_tasks,
            job,
            observation.observers.clone(),
            terminal_event,
        )
        .await;
}

pub(super) async fn complete_job_after_handler(
    pool: &runledger_postgres::DbPool,
    registry: &JobRegistry,
    context: &JobContext,
    job: &jobs::JobQueueRecord,
    lease_identity: JobLeaseIdentity<'_>,
    completion: JobCompletion,
    observation: CompletionObservation<'_>,
) {
    match completion.disposition() {
        JobCompletionDisposition::Succeed => {
            complete_job_success_after_handler(
                pool,
                registry,
                context,
                job,
                lease_identity,
                completion,
                observation,
            )
            .await;
        }
        JobCompletionDisposition::ContinueAfter(delay) => {
            let lease = CompletionLease {
                context,
                identity: lease_identity,
            };
            complete_job_continuation_after_handler(
                pool,
                registry,
                job,
                lease,
                completion,
                delay,
                observation,
            )
            .await;
        }
    }
}

async fn complete_job_success_after_handler(
    pool: &runledger_postgres::DbPool,
    registry: &JobRegistry,
    context: &JobContext,
    job: &jobs::JobQueueRecord,
    lease_identity: JobLeaseIdentity<'_>,
    completion: JobCompletion,
    observation: CompletionObservation<'_>,
) {
    let completion_update = JobCompletionUpdate {
        progress_done: completion.progress_done,
        progress_total: completion.progress_total,
        checkpoint: completion.checkpoint.as_ref(),
        output: completion.output(),
    };
    match jobs::complete_job_success_with_outcome_for_lease(
        pool,
        lease_identity,
        Some(&completion_update),
    )
    .await
    {
        Err(error) => {
            if let Some(failure) = invalid_completion_progress_failure_from_error(&error, "success")
            {
                warn!(
                    job_id = %job.id,
                    attempt = job.attempt,
                    failure_code = failure.code,
                    failure_message = %failure.message,
                    "handler returned invalid success completion progress; marking job terminal"
                );
                complete_job_failure_after_handler(
                    pool,
                    registry,
                    context,
                    job,
                    lease_identity,
                    failure,
                    observation,
                )
                .await;
                return;
            }

            let release_conflict = is_workflow_release_conflict_error(&error);
            handle_completion_persist_failure(
                observation,
                job,
                JobCompletionPersistenceOperation::Success,
                error,
                |error, lease_owner_mismatch| {
                    log_completion_success_persist_error(
                        job,
                        error,
                        release_conflict,
                        lease_owner_mismatch,
                    );
                },
            )
            .await;
        }
        Ok(outcome) => {
            observation
                .running_notification
                .spawn_terminal_observer(
                    observation.terminal_observer_tasks,
                    job,
                    observation.observers.clone(),
                    TerminalJobObserverEvent::Succeeded(JobSucceededEvent {
                        job: ObservedJob {
                            job_id: outcome.job_id,
                            job_type: outcome.job_type,
                            organization_id: outcome.organization_id,
                            run_number: outcome.run_number,
                            attempt: outcome.attempt,
                            max_attempts: outcome.max_attempts,
                            worker_id: context.worker_id.clone(),
                        },
                        duration: observation.duration,
                        progress_done: outcome.progress_done,
                        progress_total: outcome.progress_total,
                    }),
                )
                .await;
        }
    }
}

async fn complete_job_continuation_after_handler(
    pool: &runledger_postgres::DbPool,
    registry: &JobRegistry,
    job: &jobs::JobQueueRecord,
    lease: CompletionLease<'_, '_>,
    completion: JobCompletion,
    delay: Duration,
    observation: CompletionObservation<'_>,
) {
    let continuation = JobContinuationUpdate {
        delay,
        progress_done: completion.progress_done,
        progress_total: completion.progress_total,
        checkpoint: completion.checkpoint.as_ref(),
    };
    match jobs::complete_job_continuation_with_outcome_for_lease(
        pool,
        lease.identity,
        &continuation,
    )
    .await
    {
        Err(error) => {
            if let Some(failure) = invalid_continuation_failure_from_error(&error) {
                warn!(
                    job_id = %job.id,
                    attempt = job.attempt,
                    failure_code = failure.code,
                    failure_message = %failure.message,
                    "handler returned an invalid continuation; marking job terminal"
                );
                complete_job_failure_after_handler(
                    pool,
                    registry,
                    lease.context,
                    job,
                    lease.identity,
                    failure,
                    observation,
                )
                .await;
                return;
            }

            handle_completion_persist_failure(
                observation,
                job,
                JobCompletionPersistenceOperation::Continuation,
                error,
                |error, lease_owner_mismatch| {
                    let error = WorkerError::CompleteContinuation {
                        job_id: job.id,
                        attempt: job.attempt,
                        source: error,
                    };
                    if lease_owner_mismatch {
                        warn!(
                            %error,
                            job_id = %job.id,
                            run_number = job.run_number,
                            attempt = job.attempt,
                            "successful handler continuation lost lease ownership before persistence"
                        );
                    } else {
                        error!(
                            %error,
                            job_id = %job.id,
                            run_number = job.run_number,
                            attempt = job.attempt,
                            "failed to persist successful handler continuation; leaving job leased for recovery"
                        );
                    }
                },
            )
            .await;
        }
        Ok(outcome) => {
            info!(
                job_id = %outcome.job_id,
                completed_run_number = outcome.completed_run_number,
                next_run_number = outcome.next_run_number,
                attempt = outcome.attempt,
                next_run_at = %outcome.next_run_at,
                "handler continuation scheduled"
            );
            observation
                .running_notification
                .spawn_terminal_observer(
                    observation.terminal_observer_tasks,
                    job,
                    observation.observers.clone(),
                    TerminalJobObserverEvent::Continued(JobContinuedEvent {
                        job: observation.observed_job,
                        duration: observation.duration,
                        next_run_number: outcome.next_run_number,
                        next_run_at: outcome.next_run_at,
                        progress_done: outcome.progress_done,
                        progress_total: outcome.progress_total,
                    }),
                )
                .await;
        }
    }
}

pub(super) async fn complete_job_failure_after_handler(
    pool: &runledger_postgres::DbPool,
    registry: &JobRegistry,
    context: &JobContext,
    job: &jobs::JobQueueRecord,
    lease_identity: JobLeaseIdentity<'_>,
    mut failure: JobFailure,
    observation: CompletionObservation<'_>,
) {
    let mut invalid_retry_timing_rewritten = false;
    loop {
        let policy_retry_delay_ms = if is_non_retryable_failure_kind(failure.kind) {
            None
        } else {
            Some(policy_retry_delay_ms_for_failure(registry, job, &failure))
        };
        let completion_result = {
            let failure_payload = JobFailureUpdate::new(
                failure.kind,
                failure.code,
                failure.message.as_ref(),
                policy_retry_delay_ms,
            );
            let failure_payload = match failure.retry_timing() {
                Some(retry_timing) => failure_payload.with_retry_timing(retry_timing),
                None => failure_payload,
            };
            jobs::complete_job_failure_with_outcome_for_lease(
                pool,
                lease_identity,
                &failure_payload,
            )
            .await
        };

        match completion_result {
            Ok(outcome) => {
                let dead_letter = match &outcome.disposition {
                    jobs::JobFailureCompletionDisposition::DeadLettered { reason } => {
                        Some(JobDeadLetterInfo::new(
                            failure.clone(),
                            *reason,
                            Some(outcome.max_attempts),
                        ))
                    }
                    jobs::JobFailureCompletionDisposition::RetryScheduled { .. }
                    | jobs::JobFailureCompletionDisposition::RetryScheduledAt { .. } => None,
                    #[allow(unreachable_patterns)]
                    _ => None,
                };
                let disposition = match outcome.disposition {
                    jobs::JobFailureCompletionDisposition::RetryScheduled {
                        retry_delay_ms,
                        next_run_at,
                    } => JobFailureDisposition::RetryScheduled {
                        retry_delay_ms,
                        next_run_at,
                    },
                    jobs::JobFailureCompletionDisposition::RetryScheduledAt {
                        requested_retry_at,
                        next_run_at,
                    } => JobFailureDisposition::RetryScheduledAt {
                        requested_retry_at,
                        next_run_at,
                    },
                    jobs::JobFailureCompletionDisposition::DeadLettered { reason } => {
                        JobFailureDisposition::DeadLettered { reason }
                    }
                    #[allow(unreachable_patterns)]
                    _ => {
                        warn!(
                            job_id = %job.id,
                            job_type = %job.job_type,
                            run_number = job.run_number,
                            attempt = job.attempt,
                            "postgres returned an unknown job failure completion disposition; reporting unknown observer disposition"
                        );
                        JobFailureDisposition::Unknown
                    }
                };
                observation
                    .running_notification
                    .spawn_terminal_observer(
                        observation.terminal_observer_tasks,
                        job,
                        observation.observers.clone(),
                        TerminalJobObserverEvent::Failed(JobFailedEvent {
                            job: ObservedJob {
                                job_id: outcome.job_id,
                                job_type: outcome.job_type,
                                organization_id: outcome.organization_id,
                                run_number: outcome.run_number,
                                attempt: outcome.attempt,
                                max_attempts: outcome.max_attempts,
                                worker_id: context.worker_id.clone(),
                            },
                            duration: observation.duration,
                            failure: failure.clone(),
                            disposition,
                        }),
                    )
                    .await;

                if let Some(dead_letter) = dead_letter {
                    warn!(
                        job_id = %job.id,
                        job_type = %job.job_type,
                        run_number = job.run_number,
                        attempt = job.attempt,
                        max_attempts = job.max_attempts,
                        organization_id = ?job.organization_id,
                        worker_id = %context.worker_id,
                        dead_letter_reason = ?dead_letter.reason,
                        failure_kind = ?dead_letter.failure.kind,
                        failure_code = dead_letter.failure.code,
                        failure_message = %dead_letter.failure.message,
                        "job dead lettered after handler failure"
                    );
                    let mut dead_letter_context = context.clone();
                    dead_letter_context.checkpoint = outcome.checkpoint;
                    notify_handler_of_dead_letter(registry, &dead_letter_context, job, dead_letter)
                        .await;
                }
                return;
            }
            Err(error) => {
                if !invalid_retry_timing_rewritten
                    && let Some(invalid_failure) = invalid_retry_timing_failure_from_error(&error)
                {
                    warn!(
                        job_id = %job.id,
                        attempt = job.attempt,
                        original_failure_code = failure.code,
                        invalid_retry_timing = ?failure.retry_timing(),
                        replacement_failure_code = invalid_failure.code,
                        replacement_failure_message = %invalid_failure.message,
                        "handler returned invalid retry timing; marking job terminal"
                    );
                    failure = invalid_failure;
                    invalid_retry_timing_rewritten = true;
                    continue;
                }

                let release_conflict = is_workflow_release_conflict_error(&error);
                handle_completion_persist_failure(
                    observation,
                    job,
                    JobCompletionPersistenceOperation::Failure,
                    error,
                    |error, lease_owner_mismatch| {
                        log_completion_failure_persist_error(
                            job,
                            error,
                            release_conflict,
                            lease_owner_mismatch,
                        );
                    },
                )
                .await;
                return;
            }
        }
    }
}

pub(super) fn completion_persist_error_diagnostic(error: &runledger_postgres::Error) -> String {
    let runledger_postgres::Error::QueryError(query_error) = error else {
        return "client_message=\"Database operation failed.\"; code=db.operation_failed"
            .to_owned();
    };

    [
        format!("client_message={:?}", query_error.client_message()),
        format!("code={}", query_error.code()),
    ]
    .join("; ")
}

pub(super) fn compute_retry_delay_ms(attempt: i32, job_id: uuid::Uuid) -> i32 {
    let exp = attempt.clamp(1, 10) as u32;
    let base_ms: i64 = 5_000;
    let raw = base_ms * (1_i64 << exp);
    let capped = raw.min(300_000);
    let jitter = (job_id.as_u128() % 1_000) as i64 - 500;
    (capped + jitter).max(1_000) as i32
}

fn invalid_completion_progress_failure_from_error(
    error: &runledger_postgres::Error,
    completion_kind: &'static str,
) -> Option<JobFailure> {
    let runledger_postgres::Error::QueryError(query_error) = error else {
        return None;
    };

    if query_error.kind() != Some(QueryErrorKind::JobInvalidCompletionProgress) {
        return None;
    }

    Some(JobFailure::terminal(
        query_error.code(),
        format!(
            "Handler returned invalid {completion_kind} progress: {}.",
            query_error.internal_message()
        ),
    ))
}

fn invalid_continuation_failure_from_error(
    error: &runledger_postgres::Error,
) -> Option<JobFailure> {
    let runledger_postgres::Error::QueryError(query_error) = error else {
        return None;
    };

    match query_error.kind() {
        Some(QueryErrorKind::JobInvalidCompletionProgress) => {
            invalid_completion_progress_failure_from_error(error, "continuation")
        }
        Some(QueryErrorKind::JobInvalidContinuationDelay) => Some(JobFailure::terminal(
            query_error.code(),
            format!(
                "Handler returned a continuation delay that cannot be persisted: {}.",
                query_error.internal_message()
            ),
        )),
        Some(QueryErrorKind::JobWorkflowHandlerContinuationNotEnabled) => {
            Some(JobFailure::terminal(
                query_error.code(),
                "Workflow step handler continuation is not enabled for this job.",
            ))
        }
        Some(
            QueryErrorKind::JobLeaseOwnerMismatch
            | QueryErrorKind::JobInvalidRetryTiming
            | QueryErrorKind::JobUnstartedClaimReleaseNotApplicable
            | QueryErrorKind::JobWorkflowRequeueNotSupported
            | QueryErrorKind::WorkflowReleaseConflict,
        )
        | None => None,
    }
}

fn invalid_retry_timing_failure_from_error(
    error: &runledger_postgres::Error,
) -> Option<JobFailure> {
    let runledger_postgres::Error::QueryError(query_error) = error else {
        return None;
    };
    if query_error.kind() != Some(QueryErrorKind::JobInvalidRetryTiming) {
        return None;
    }

    Some(JobFailure::terminal(
        query_error.code(),
        format!(
            "Handler returned retry timing that cannot be persisted: {}.",
            query_error.internal_message()
        ),
    ))
}

fn is_workflow_release_conflict_error(error: &runledger_postgres::Error) -> bool {
    matches!(
        error,
        runledger_postgres::Error::QueryError(query_error)
            if query_error.kind() == Some(QueryErrorKind::WorkflowReleaseConflict)
    )
}

fn is_non_retryable_failure_kind(kind: JobFailureKind) -> bool {
    matches!(kind, JobFailureKind::Terminal | JobFailureKind::Panicked)
}

fn policy_retry_delay_ms_for_failure(
    registry: &JobRegistry,
    job: &jobs::JobQueueRecord,
    failure: &JobFailure,
) -> i32 {
    registry
        .retry_delay_override(job.job_type.as_borrowed(), failure.code)
        .unwrap_or_else(|| compute_retry_delay_ms(job.attempt, job.id))
}

fn log_completion_success_persist_error(
    job: &jobs::JobQueueRecord,
    error: runledger_postgres::Error,
    release_conflict: bool,
    lease_owner_mismatch: bool,
) {
    let error = WorkerError::CompleteSuccess {
        job_id: job.id,
        attempt: job.attempt,
        source: error,
    };
    if lease_owner_mismatch {
        warn!(
            %error,
            job_id = %job.id,
            "successful handler completion lost lease ownership before persistence"
        );
    } else if release_conflict {
        warn!(
            %error,
            job_id = %job.id,
            "job success completion conflicted with workflow cancellation; leaving lease for reaper recovery"
        );
    } else {
        error!(%error, job_id = %job.id, "failed to mark job success");
    }
}

fn log_completion_failure_persist_error(
    job: &jobs::JobQueueRecord,
    error: runledger_postgres::Error,
    release_conflict: bool,
    lease_owner_mismatch: bool,
) {
    let error = WorkerError::CompleteFailure {
        job_id: job.id,
        attempt: job.attempt,
        source: error,
    };
    if lease_owner_mismatch {
        warn!(
            %error,
            job_id = %job.id,
            "handler failure completion lost lease ownership before persistence"
        );
    } else if release_conflict {
        warn!(
            %error,
            job_id = %job.id,
            "job failure completion conflicted with workflow cancellation; leaving lease for reaper recovery"
        );
    } else {
        error!(%error, job_id = %job.id, "failed to mark job failure");
    }
}
