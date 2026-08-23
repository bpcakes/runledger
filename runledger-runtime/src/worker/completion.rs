use std::time::Duration;

use runledger_core::jobs::{
    JobCompletion, JobCompletionDisposition, JobContext, JobDeadLetterInfo, JobFailure,
};
use runledger_postgres::QueryErrorKind;
use runledger_postgres::jobs::{
    self, JobCompletionUpdate, JobContinuationUpdate, JobFailureUpdate, JobLeaseIdentity,
};
use tracing::{error, info, warn};

use super::dead_letter::notify_handler_of_dead_letter;
use super::execution::{is_lease_owner_mismatch_error, lease_owner_mismatch_failure};
use super::observers::{JobRunningNotification, TerminalJobObserverEvent, TerminalObserverTasks};
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
pub(super) struct CompletionContext<'execution, 'context> {
    pool: &'execution runledger_postgres::DbPool,
    registry: &'execution JobRegistry,
    context: &'context JobContext,
    job: &'execution jobs::JobQueueRecord,
    lease_identity: JobLeaseIdentity<'execution>,
}

impl<'execution, 'context> CompletionContext<'execution, 'context> {
    pub(super) fn new(
        pool: &'execution runledger_postgres::DbPool,
        registry: &'execution JobRegistry,
        context: &'context JobContext,
        job: &'execution jobs::JobQueueRecord,
        lease_identity: JobLeaseIdentity<'execution>,
    ) -> Self {
        Self {
            pool,
            registry,
            context,
            job,
            lease_identity,
        }
    }
}

struct FailureCompletionPostCommitEffects {
    observer_event: JobFailedEvent,
    dead_letter: Option<JobDeadLetterInfo>,
    checkpoint: Option<serde_json::Value>,
    has_unknown_disposition: bool,
}

fn failure_update<'a>(
    registry: &JobRegistry,
    job: &jobs::JobQueueRecord,
    failure: &'a JobFailure,
) -> JobFailureUpdate<'a> {
    let policy_retry_delay_ms = if failure.kind.is_retryable() {
        Some(policy_retry_delay_ms_for_failure(registry, job, failure))
    } else {
        None
    };
    let failure_update = JobFailureUpdate::new(
        failure.kind,
        failure.code,
        failure.message.as_ref(),
        policy_retry_delay_ms,
    );

    match failure.retry_timing() {
        Some(retry_timing) => failure_update.with_retry_timing(retry_timing),
        None => failure_update,
    }
}

fn failure_completion_post_commit_effects(
    outcome: jobs::JobFailureCompletionOutcome,
    context: &JobContext,
    failure: &JobFailure,
    duration: Duration,
) -> FailureCompletionPostCommitEffects {
    let dead_letter = match &outcome.disposition {
        jobs::JobFailureCompletionDisposition::DeadLettered { reason } => Some(
            JobDeadLetterInfo::new(failure.clone(), *reason, Some(outcome.max_attempts)),
        ),
        jobs::JobFailureCompletionDisposition::RetryScheduled { .. }
        | jobs::JobFailureCompletionDisposition::RetryScheduledAt { .. } => None,
        #[allow(
            unreachable_patterns,
            reason = "future non-exhaustive dispositions cannot provide known dead-letter metadata"
        )]
        _ => None,
    };
    let (disposition, has_unknown_disposition) = match outcome.disposition {
        jobs::JobFailureCompletionDisposition::RetryScheduled {
            retry_delay_ms,
            next_run_at,
        } => (
            JobFailureDisposition::RetryScheduled {
                retry_delay_ms,
                next_run_at,
            },
            false,
        ),
        jobs::JobFailureCompletionDisposition::RetryScheduledAt {
            requested_retry_at,
            next_run_at,
        } => (
            JobFailureDisposition::RetryScheduledAt {
                requested_retry_at,
                next_run_at,
            },
            false,
        ),
        jobs::JobFailureCompletionDisposition::DeadLettered { reason } => {
            (JobFailureDisposition::DeadLettered { reason }, false)
        }
        #[allow(
            unreachable_patterns,
            reason = "map future non-exhaustive persistence dispositions to the public unknown variant"
        )]
        _ => (JobFailureDisposition::Unknown, true),
    };

    FailureCompletionPostCommitEffects {
        observer_event: JobFailedEvent {
            job: ObservedJob {
                job_id: outcome.job_id,
                job_type: outcome.job_type,
                organization_id: outcome.organization_id,
                run_number: outcome.run_number,
                attempt: outcome.attempt,
                max_attempts: outcome.max_attempts,
                worker_id: context.worker_id.clone(),
            },
            duration,
            failure: failure.clone(),
            disposition,
        },
        dead_letter,
        checkpoint: outcome.checkpoint,
        has_unknown_disposition,
    }
}

async fn notify_failure_observer(observation: CompletionObservation<'_>, event: JobFailedEvent) {
    observation
        .running_notification
        .spawn_terminal_observer(
            observation.terminal_observer_tasks,
            observation.observers.clone(),
            TerminalJobObserverEvent::Failed(event),
        )
        .await;
}

async fn notify_dead_letter_after_handler_failure(
    completion: CompletionContext<'_, '_>,
    dead_letter: JobDeadLetterInfo,
    checkpoint: Option<serde_json::Value>,
) {
    warn!(
        job_id = %completion.job.id,
        job_type = %completion.job.job_type,
        run_number = completion.job.run_number,
        attempt = completion.job.attempt,
        max_attempts = completion.job.max_attempts,
        organization_id = ?completion.job.organization_id,
        worker_id = %completion.context.worker_id,
        dead_letter_reason = ?dead_letter.reason,
        failure_kind = ?dead_letter.failure.kind,
        failure_code = dead_letter.failure.code,
        failure_message = %dead_letter.failure.message,
        "job dead lettered after handler failure"
    );
    let mut dead_letter_context = completion.context.clone();
    dead_letter_context.checkpoint = checkpoint;
    notify_handler_of_dead_letter(
        completion.registry,
        &dead_letter_context,
        completion.job,
        dead_letter,
    )
    .await;
}

async fn handle_completion_persist_failure(
    observation: CompletionObservation<'_>,
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
            observation.observers.clone(),
            terminal_event,
        )
        .await;
}

pub(super) async fn complete_job_after_handler(
    completion_context: CompletionContext<'_, '_>,
    completion: JobCompletion,
    observation: CompletionObservation<'_>,
) {
    match completion.disposition() {
        JobCompletionDisposition::Succeed => {
            complete_job_success_after_handler(completion_context, completion, observation).await;
        }
        JobCompletionDisposition::ContinueAfter(delay) => {
            complete_job_continuation_after_handler(
                completion_context,
                completion,
                delay,
                observation,
            )
            .await;
        }
    }
}

async fn complete_job_success_after_handler(
    completion_context: CompletionContext<'_, '_>,
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
        completion_context.pool,
        completion_context.lease_identity,
        Some(&completion_update),
    )
    .await
    {
        Err(error) => {
            if let Some(failure) = invalid_completion_progress_failure_from_error(&error, "success")
            {
                warn!(
                    job_id = %completion_context.job.id,
                    attempt = completion_context.job.attempt,
                    failure_code = failure.code,
                    failure_message = %failure.message,
                    "handler returned invalid success completion progress; marking job terminal"
                );
                complete_job_failure_after_handler(completion_context, failure, observation).await;
                return;
            }

            let release_conflict = is_workflow_release_conflict_error(&error);
            handle_completion_persist_failure(
                observation,
                JobCompletionPersistenceOperation::Success,
                error,
                |error, lease_owner_mismatch| {
                    log_completion_success_persist_error(
                        completion_context.job,
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
                    observation.observers.clone(),
                    TerminalJobObserverEvent::Succeeded(JobSucceededEvent {
                        job: ObservedJob {
                            job_id: outcome.job_id,
                            job_type: outcome.job_type,
                            organization_id: outcome.organization_id,
                            run_number: outcome.run_number,
                            attempt: outcome.attempt,
                            max_attempts: outcome.max_attempts,
                            worker_id: completion_context.context.worker_id.clone(),
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
    completion_context: CompletionContext<'_, '_>,
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
        completion_context.pool,
        completion_context.lease_identity,
        &continuation,
    )
    .await
    {
        Err(error) => {
            if let Some(failure) = invalid_continuation_failure_from_error(&error) {
                warn!(
                    job_id = %completion_context.job.id,
                    attempt = completion_context.job.attempt,
                    failure_code = failure.code,
                    failure_message = %failure.message,
                    "handler returned an invalid continuation; marking job terminal"
                );
                complete_job_failure_after_handler(completion_context, failure, observation).await;
                return;
            }

            handle_completion_persist_failure(
                observation,
                JobCompletionPersistenceOperation::Continuation,
                error,
                |error, lease_owner_mismatch| {
                    let error = WorkerError::CompleteContinuation {
                        job_id: completion_context.job.id,
                        attempt: completion_context.job.attempt,
                        source: error,
                    };
                    if lease_owner_mismatch {
                        warn!(
                            %error,
                            job_id = %completion_context.job.id,
                            run_number = completion_context.job.run_number,
                            attempt = completion_context.job.attempt,
                            "successful handler continuation lost lease ownership before persistence"
                        );
                    } else {
                        error!(
                            %error,
                            job_id = %completion_context.job.id,
                            run_number = completion_context.job.run_number,
                            attempt = completion_context.job.attempt,
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
    completion_context: CompletionContext<'_, '_>,
    mut failure: JobFailure,
    observation: CompletionObservation<'_>,
) {
    let mut invalid_retry_timing_rewritten = false;
    loop {
        let failure_payload = failure_update(
            completion_context.registry,
            completion_context.job,
            &failure,
        );
        let completion_result = jobs::complete_job_failure_with_outcome_for_lease(
            completion_context.pool,
            completion_context.lease_identity,
            &failure_payload,
        )
        .await;

        match completion_result {
            Ok(outcome) => {
                let effects = failure_completion_post_commit_effects(
                    outcome,
                    completion_context.context,
                    &failure,
                    observation.duration,
                );
                if effects.has_unknown_disposition {
                    warn!(
                        job_id = %completion_context.job.id,
                        job_type = %completion_context.job.job_type,
                        run_number = completion_context.job.run_number,
                        attempt = completion_context.job.attempt,
                        "postgres returned an unknown job failure completion disposition; reporting unknown observer disposition"
                    );
                }
                let FailureCompletionPostCommitEffects {
                    observer_event,
                    dead_letter,
                    checkpoint,
                    has_unknown_disposition: _,
                } = effects;
                notify_failure_observer(observation, observer_event).await;

                if let Some(dead_letter) = dead_letter {
                    notify_dead_letter_after_handler_failure(
                        completion_context,
                        dead_letter,
                        checkpoint,
                    )
                    .await;
                }
                return;
            }
            Err(error) => {
                if !invalid_retry_timing_rewritten
                    && let Some(invalid_failure) = invalid_retry_timing_failure_from_error(&error)
                {
                    warn!(
                        job_id = %completion_context.job.id,
                        attempt = completion_context.job.attempt,
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
                    JobCompletionPersistenceOperation::Failure,
                    error,
                    |error, lease_owner_mismatch| {
                        log_completion_failure_persist_error(
                            completion_context.job,
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
