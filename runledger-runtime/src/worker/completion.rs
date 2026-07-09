use std::time::Duration;

use runledger_core::jobs::{
    JobCompletion, JobContext, JobDeadLetterInfo, JobFailure, JobFailureKind,
};
use runledger_postgres::jobs::{self, JobCompletionUpdate, JobFailureUpdate};
use tracing::{error, warn};

use super::dead_letter::notify_handler_of_dead_letter;
use super::observers::{JobRunningNotification, TerminalJobObserverEvent, TerminalObserverTasks};
use super::{INVALID_COMPLETION_PROGRESS_CODE, WORKFLOW_RELEASE_CONFLICT_CODE};
use crate::WorkerError;
use crate::observer::{
    JobCompletionPersistFailedEvent, JobCompletionPersistenceOperation, JobFailedEvent,
    JobFailureDisposition, JobLifecycleObservers, JobSucceededEvent, ObservedJob,
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

pub(super) async fn complete_job_success_after_handler(
    pool: &runledger_postgres::DbPool,
    registry: &JobRegistry,
    context: &JobContext,
    job: &jobs::JobQueueRecord,
    completion: JobCompletion,
    observation: CompletionObservation<'_>,
) {
    if let Some(failure) = invalid_completion_progress_failure(&completion) {
        warn!(
            job_id = %job.id,
            attempt = job.attempt,
            failure_code = failure.code,
            failure_message = %failure.message,
            "handler returned invalid success completion; marking job terminal"
        );
        complete_job_failure_after_handler(pool, registry, context, job, failure, observation)
            .await;
        return;
    }

    let completion_update = JobCompletionUpdate {
        progress_done: completion.progress_done,
        progress_total: completion.progress_total,
        checkpoint: completion.checkpoint.as_ref(),
        output: completion.output.as_ref(),
    };
    match jobs::complete_job_success_with_outcome(
        pool,
        job.id,
        job.run_number,
        job.attempt,
        &context.worker_id,
        Some(&completion_update),
    )
    .await
    {
        Err(error) if invalid_completion_progress_failure_from_error(&error).is_some() => {
            let failure = invalid_completion_progress_failure_from_error(&error)
                .expect("guard checked invalid completion progress");
            warn!(
                job_id = %job.id,
                attempt = job.attempt,
                failure_code = failure.code,
                failure_message = %failure.message,
                "handler returned invalid success completion after stored progress was applied; marking job terminal"
            );
            complete_job_failure_after_handler(pool, registry, context, job, failure, observation)
                .await;
        }
        Err(error) => {
            let release_conflict = is_workflow_release_conflict_error(&error);
            let error_message = completion_persist_error_diagnostic(&error);
            log_completion_success_persist_error(job, error, release_conflict);
            observation
                .running_notification
                .spawn_terminal_observer(
                    observation.terminal_observer_tasks,
                    job,
                    observation.observers.clone(),
                    TerminalJobObserverEvent::CompletionPersistFailed(
                        JobCompletionPersistFailedEvent {
                            job: observation.observed_job,
                            duration: observation.duration,
                            operation: JobCompletionPersistenceOperation::Success,
                            error: error_message,
                        },
                    ),
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

pub(super) async fn complete_job_failure_after_handler(
    pool: &runledger_postgres::DbPool,
    registry: &JobRegistry,
    context: &JobContext,
    job: &jobs::JobQueueRecord,
    failure: JobFailure,
    observation: CompletionObservation<'_>,
) {
    let retry_delay_ms = if is_non_retryable_failure_kind(failure.kind) {
        None
    } else {
        Some(retry_delay_ms_for_failure(registry, job, &failure))
    };
    let failure_payload = JobFailureUpdate {
        kind: failure.kind,
        code: failure.code,
        message: failure.message.as_ref(),
        retry_delay_ms,
    };
    match jobs::complete_job_failure_with_outcome(
        pool,
        job.id,
        job.run_number,
        job.attempt,
        &context.worker_id,
        &failure_payload,
    )
    .await
    {
        Ok(outcome) => {
            let dead_letter = match &outcome.disposition {
                jobs::JobFailureCompletionDisposition::DeadLettered { reason } => Some(
                    JobDeadLetterInfo::new(failure.clone(), *reason, Some(outcome.max_attempts)),
                ),
                jobs::JobFailureCompletionDisposition::RetryScheduled { .. } => None,
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
                notify_handler_of_dead_letter(registry, context, job, dead_letter).await;
            }
        }
        Err(error) => {
            let release_conflict = is_workflow_release_conflict_error(&error);
            let error_message = completion_persist_error_diagnostic(&error);
            log_completion_failure_persist_error(job, error, release_conflict);
            observation
                .running_notification
                .spawn_terminal_observer(
                    observation.terminal_observer_tasks,
                    job,
                    observation.observers.clone(),
                    TerminalJobObserverEvent::CompletionPersistFailed(
                        JobCompletionPersistFailedEvent {
                            job: observation.observed_job,
                            duration: observation.duration,
                            operation: JobCompletionPersistenceOperation::Failure,
                            error: error_message,
                        },
                    ),
                )
                .await;
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

fn invalid_completion_progress_failure(completion: &JobCompletion) -> Option<JobFailure> {
    invalid_completion_progress_detail(completion.progress_done, completion.progress_total)
        .map(|detail| JobFailure::terminal(INVALID_COMPLETION_PROGRESS_CODE, detail))
}

fn invalid_completion_progress_failure_from_error(
    error: &runledger_postgres::Error,
) -> Option<JobFailure> {
    let runledger_postgres::Error::QueryError(query_error) = error else {
        return None;
    };

    if query_error.code() != INVALID_COMPLETION_PROGRESS_CODE {
        return None;
    }

    Some(JobFailure::terminal(
        INVALID_COMPLETION_PROGRESS_CODE,
        format!(
            "Handler returned invalid success progress after combining with stored progress: {}.",
            query_error.internal_message()
        ),
    ))
}

fn invalid_completion_progress_detail(
    progress_done: Option<i64>,
    progress_total: Option<i64>,
) -> Option<String> {
    if let Some(progress_done) = progress_done
        && progress_done < 0
    {
        return Some(format!(
            "Handler returned invalid success progress: progress_done must be greater than or equal to zero, got {progress_done}."
        ));
    }

    if let Some(progress_total) = progress_total
        && progress_total < 0
    {
        return Some(format!(
            "Handler returned invalid success progress: progress_total must be greater than or equal to zero, got {progress_total}."
        ));
    }

    if let (Some(progress_done), Some(progress_total)) = (progress_done, progress_total)
        && progress_done > progress_total
    {
        return Some(format!(
            "Handler returned invalid success progress: progress_done must not exceed progress_total, got progress_done={progress_done}, progress_total={progress_total}."
        ));
    }

    None
}

fn is_workflow_release_conflict_error(error: &runledger_postgres::Error) -> bool {
    matches!(
        error,
        runledger_postgres::Error::QueryError(query_error)
            if query_error.code() == WORKFLOW_RELEASE_CONFLICT_CODE
    )
}

fn is_non_retryable_failure_kind(kind: JobFailureKind) -> bool {
    matches!(kind, JobFailureKind::Terminal | JobFailureKind::Panicked)
}

fn retry_delay_ms_for_failure(
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
) {
    let error = WorkerError::CompleteSuccess {
        job_id: job.id,
        attempt: job.attempt,
        source: error,
    };
    if release_conflict {
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
) {
    let error = WorkerError::CompleteFailure {
        job_id: job.id,
        attempt: job.attempt,
        source: error,
    };
    if release_conflict {
        warn!(
            %error,
            job_id = %job.id,
            "job failure completion conflicted with workflow cancellation; leaving lease for reaper recovery"
        );
    } else {
        error!(%error, job_id = %job.id, "failed to mark job failure");
    }
}
