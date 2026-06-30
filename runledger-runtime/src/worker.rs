use std::any::Any;
use std::cmp::min;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures_util::FutureExt;
use runledger_core::jobs::{
    JobCompletion, JobContext, JobDeadLetterInfo, JobDeadLetterReason, JobFailure, JobFailureKind,
};
use runledger_postgres::jobs::{self, JobCompletionUpdate, JobFailureUpdate, JobProgressUpdate};
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant, MissedTickBehavior, sleep_until};
use tracing::{Instrument, error, info, info_span, warn};

use crate::RuntimeLoopExit;
use crate::WorkerError;
use crate::config::JobsConfig;
use crate::registry::JobRegistry;
use crate::shutdown;

const UNKNOWN_WORKER_ID: &str = "unknown-worker";
// Kept stable for clients that already match this code; it also covers leases
// that expired before the worker's lifecycle update reached storage.
const LEASE_OWNER_MISMATCH_CODE: &str = "job.lease_owner_mismatch";
const LEASE_MAINTENANCE_FAILED_CODE: &str = "job.lease_maintenance_failed";
const WORKFLOW_RELEASE_CONFLICT_CODE: &str = "workflow.release_conflict";
const HANDLER_PANIC_CODE: &str = "job.handler_panic";
const INVALID_COMPLETION_PROGRESS_CODE: &str = "job.invalid_completion_progress";
const RUNNING_PROGRESS_PERSIST_FAILED_REASON: &str = "RUNNING_PROGRESS_PERSIST_FAILED";
const UNSTARTED_CLAIM_RELEASE_NOT_APPLICABLE_CODE: &str =
    "job.unstarted_claim_release_not_applicable";
const UNSTARTED_CLAIM_RETRY_DELAY_MS: i32 = 1_000;
#[cfg(test)]
const TERMINAL_HOOK_TIMEOUT: Duration = Duration::from_millis(100);
#[cfg(not(test))]
const TERMINAL_HOOK_TIMEOUT: Duration = Duration::from_secs(10);

enum JobExecutionFailure {
    Handler(JobFailure),
    LeaseMaintenance(JobFailure),
}

pub async fn run_worker_loop(
    pool: runledger_postgres::DbPool,
    registry: JobRegistry,
    config: JobsConfig,
    mut shutdown: watch::Receiver<bool>,
) -> RuntimeLoopExit {
    if let Err(error) = config.validate_worker_loop() {
        warn!(%error, "invalid jobs config; stopping worker loop");
        return RuntimeLoopExit::InvalidConfig(error);
    }

    let registry = Arc::new(registry);
    let claimable_job_types = registry.registered_types();
    let semaphore = Arc::new(Semaphore::new(config.max_global_concurrency));
    let mut join_set: JoinSet<()> = JoinSet::new();

    loop {
        drain_finished_tasks(&mut join_set).await;

        if shutdown::is_requested_or_closed(&shutdown) {
            return drain_in_flight_jobs(join_set, RuntimeLoopExit::Shutdown).await;
        }

        if claimable_job_types.is_empty() {
            if shutdown::wait_for_request_or_timeout(&mut shutdown, config.poll_interval).await {
                return drain_in_flight_jobs(join_set, RuntimeLoopExit::Shutdown).await;
            }
            continue;
        }

        let available = semaphore.available_permits();
        if available == 0 {
            if shutdown::wait_for_request_or_timeout(&mut shutdown, config.poll_interval).await {
                return drain_in_flight_jobs(join_set, RuntimeLoopExit::Shutdown).await;
            }
            continue;
        }

        let claim_limit = min(available, config.claim_batch_size as usize);
        let claimed = match jobs::claim_prestart_jobs_for_types(
            &pool,
            &config.worker_id,
            config.lease_ttl_seconds,
            claim_limit as i64,
            &claimable_job_types,
        )
        .await
        {
            Ok(claimed) => claimed,
            Err(error) => {
                let error = WorkerError::ClaimJobs {
                    worker_id: config.worker_id.clone(),
                    source: error,
                };
                warn!(%error, "worker claim failed");
                Vec::new()
            }
        };

        if claimed.is_empty() {
            shutdown::wait_for_request_or_timeout(&mut shutdown, config.poll_interval).await;
            continue;
        }

        let claimed_len = claimed.len();
        for job in claimed {
            let permit = match Arc::clone(&semaphore).acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => {
                    // The worker owns this semaphore and never closes it. If
                    // this defensive branch fires, surface it as an unexpected
                    // loop completion rather than graceful shutdown.
                    warn!("worker semaphore closed; stopping worker loop");
                    return drain_in_flight_jobs(join_set, RuntimeLoopExit::Completed).await;
                }
            };
            let pool_clone = pool.clone();
            let registry_clone = Arc::clone(&registry);
            let lease_ttl_seconds = config.lease_ttl_seconds;
            join_set.spawn(async move {
                let _permit = permit;
                process_claimed_job(pool_clone, registry_clone, job, lease_ttl_seconds).await;
            });
        }

        if claimed_len == claim_limit {
            continue;
        }

        if shutdown::wait_for_request_or_timeout(&mut shutdown, config.poll_interval).await {
            return drain_in_flight_jobs(join_set, RuntimeLoopExit::Shutdown).await;
        }
    }
}

async fn drain_in_flight_jobs(mut join_set: JoinSet<()>, exit: RuntimeLoopExit) -> RuntimeLoopExit {
    if !join_set.is_empty() {
        match exit {
            RuntimeLoopExit::Shutdown => {
                info!("worker shutdown requested; draining in-flight jobs")
            }
            RuntimeLoopExit::InvalidConfig(_) => {
                warn!("worker loop rejected invalid config; draining in-flight jobs");
            }
            RuntimeLoopExit::Completed => {
                warn!("worker loop completed before shutdown; draining in-flight jobs");
            }
        }
    }
    while join_set.join_next().await.is_some() {}
    exit
}

async fn drain_finished_tasks(join_set: &mut JoinSet<()>) {
    while let Some(result) = join_set.try_join_next() {
        if let Err(error) = result {
            error!(%error, "job task crashed");
        }
    }
}

async fn process_claimed_job(
    pool: runledger_postgres::DbPool,
    registry: Arc<JobRegistry>,
    job: jobs::JobQueueRecord,
    lease_ttl_seconds: i32,
) {
    let worker_id = job
        .worker_id
        .clone()
        .unwrap_or_else(|| UNKNOWN_WORKER_ID.to_owned());

    let job_span = info_span!(
        "job",
        sentry.name = %job.job_type,
        sentry.op = "runledger.job",
        job_id = %job.id,
        job_type = %job.job_type,
        run_number = job.run_number,
        attempt = job.attempt,
        organization_id = ?job.organization_id,
        worker_id = %worker_id,
    );
    async {
        let start = Instant::now();
        let context = JobContext {
            job_id: job.id,
            run_number: job.run_number,
            attempt: job.attempt,
            organization_id: job.organization_id,
            worker_id: worker_id.clone(),
        };

        if !mark_job_running_or_abort(&pool, &context, &job).await {
            return;
        }

        match execute_job_handler_with_heartbeats(
            pool.clone(),
            Arc::clone(&registry),
            &context,
            &job,
            lease_ttl_seconds,
        )
        .await
        {
            Ok(completion) => {
                if let Some(failure) = invalid_completion_progress_failure(&completion) {
                    warn!(
                        job_id = %job.id,
                        attempt = job.attempt,
                        failure_code = failure.code,
                        failure_message = %failure.message,
                        "handler returned invalid success completion; marking job terminal"
                    );
                    complete_job_failure_after_handler(
                        &pool,
                        registry.as_ref(),
                        &context,
                        &job,
                        failure,
                    )
                    .await;
                } else {
                    let completion_update = JobCompletionUpdate {
                        progress_done: completion.progress_done,
                        progress_total: completion.progress_total,
                        checkpoint: completion.checkpoint.as_ref(),
                        output: completion.output.as_ref(),
                    };
                    if let Err(error) = jobs::complete_job_success(
                        &pool,
                        job.id,
                        job.run_number,
                        job.attempt,
                        &context.worker_id,
                        Some(&completion_update),
                    )
                    .await
                    {
                        if let Some(failure) = invalid_completion_progress_failure_from_error(&error)
                        {
                            warn!(
                                job_id = %job.id,
                                attempt = job.attempt,
                                failure_code = failure.code,
                                failure_message = %failure.message,
                                "handler returned invalid success completion after stored progress was applied; marking job terminal"
                            );
                            complete_job_failure_after_handler(
                                &pool,
                                registry.as_ref(),
                                &context,
                                &job,
                                failure,
                            )
                            .await;
                        } else {
                            let release_conflict = is_workflow_release_conflict_error(&error);
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
                    }
                }
            }
            Err(JobExecutionFailure::Handler(failure)) => {
                complete_job_failure_after_handler(&pool, registry.as_ref(), &context, &job, failure)
                    .await;
            }
            Err(JobExecutionFailure::LeaseMaintenance(failure)) => {
                log_lease_maintenance_abort(&job, &failure);
            }
        }

        info!(
            job_id = %job.id,
            attempt = job.attempt,
            run_number = job.run_number,
            elapsed_ms = start.elapsed().as_millis(),
            "job processed"
        );
    }
    .instrument(job_span)
    .await;
}

async fn complete_job_failure_after_handler(
    pool: &runledger_postgres::DbPool,
    registry: &JobRegistry,
    context: &JobContext,
    job: &jobs::JobQueueRecord,
    failure: JobFailure,
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
    let dead_letter = dead_letter_info(job, &failure);
    if let Err(error) = jobs::complete_job_failure(
        pool,
        job.id,
        job.run_number,
        job.attempt,
        &context.worker_id,
        &failure_payload,
    )
    .await
    {
        let release_conflict = is_workflow_release_conflict_error(&error);
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
    } else if let Some(dead_letter) = dead_letter {
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

fn log_lease_maintenance_abort(job: &jobs::JobQueueRecord, failure: &JobFailure) {
    warn!(
        job_id = %job.id,
        attempt = job.attempt,
        failure_code = failure.code,
        "job processing aborted because durable lease maintenance was lost"
    );
}

async fn mark_job_running_or_abort(
    pool: &runledger_postgres::DbPool,
    context: &JobContext,
    job: &jobs::JobQueueRecord,
) -> bool {
    let running_progress = JobProgressUpdate {
        stage: Some(runledger_core::jobs::JobStage::Running),
        progress_done: None,
        progress_total: None,
        checkpoint: None,
    };

    let Err(source) = jobs::update_job_progress(
        pool,
        job.id,
        job.run_number,
        job.attempt,
        &context.worker_id,
        &running_progress,
    )
    .await
    else {
        return true;
    };

    handle_running_progress_persist_failure(pool, context, job, source).await;
    false
}

async fn handle_running_progress_persist_failure(
    pool: &runledger_postgres::DbPool,
    context: &JobContext,
    job: &jobs::JobQueueRecord,
    source: runledger_postgres::Error,
) {
    let lease_owner_mismatch = is_lease_owner_mismatch_error(&source);
    let error = WorkerError::SetRunningProgress {
        job_id: job.id,
        attempt: job.attempt,
        source,
    };

    if lease_owner_mismatch {
        warn!(
            %error,
            job_id = %job.id,
            attempt = job.attempt,
            "aborting job before execution because lease ownership was already lost"
        );
        return;
    }

    match jobs::release_unstarted_job_claim(
        pool,
        job.id,
        job.run_number,
        job.attempt,
        &context.worker_id,
        RUNNING_PROGRESS_PERSIST_FAILED_REASON,
        UNSTARTED_CLAIM_RETRY_DELAY_MS,
    )
    .await
    {
        Ok(()) => {
            warn!(
                %error,
                job_id = %job.id,
                attempt = job.attempt,
                "running progress could not be persisted; released unstarted claim back to pending"
            );
        }
        Err(release_error) => {
            let no_longer_releasable =
                is_unstarted_claim_release_not_applicable_error(&release_error);
            let release_error = WorkerError::ReleaseUnstartedClaim {
                job_id: job.id,
                attempt: job.attempt,
                source: release_error,
            };
            if no_longer_releasable {
                warn!(
                    %error,
                    %release_error,
                    job_id = %job.id,
                    attempt = job.attempt,
                    "running progress could not be persisted; unstarted release no longer applies and the job will continue under the current lease owner"
                );
                return;
            }

            warn!(
                %error,
                %release_error,
                job_id = %job.id,
                attempt = job.attempt,
                "running progress could not be persisted; leaving claim for reaper recovery"
            );
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

async fn execute_job_handler_with_heartbeats(
    pool: runledger_postgres::DbPool,
    registry: Arc<JobRegistry>,
    context: &JobContext,
    job: &jobs::JobQueueRecord,
    lease_ttl_seconds: i32,
) -> Result<JobCompletion, JobExecutionFailure> {
    let mut execution =
        Box::pin(AssertUnwindSafe(execute_job_handler(registry, context, job)).catch_unwind());
    let timeout_deadline = Instant::now() + Duration::from_secs(job.timeout_seconds.max(1) as u64);
    let mut timeout = Box::pin(sleep_until(timeout_deadline));

    let mut ticker = tokio::time::interval(heartbeat_interval(lease_ttl_seconds));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;

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
            _ = ticker.tick() => {
                if let Err(error) = jobs::heartbeat_job(
                    &pool,
                    job.id,
                    job.run_number,
                    job.attempt,
                    &context.worker_id,
                    lease_ttl_seconds,
                )
                .await
                {
                    let lease_owner_mismatch = is_lease_owner_mismatch_error(&error);
                    let error = WorkerError::Heartbeat {
                        job_id: job.id,
                        attempt: job.attempt,
                        source: error,
                    };

                    if lease_owner_mismatch {
                        warn!(%error, job_id = %job.id, "job heartbeat lost lease ownership");
                        return Err(JobExecutionFailure::LeaseMaintenance(
                            lease_owner_mismatch_failure(),
                        ));
                    }

                    warn!(
                        %error,
                        job_id = %job.id,
                        "aborting job because lease heartbeat could not be persisted"
                    );
                    return Err(JobExecutionFailure::LeaseMaintenance(
                        lease_maintenance_failure(),
                    ));
                }
            }
        }
    }
}

fn lease_owner_mismatch_failure() -> JobFailure {
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

fn has_query_error_code(error: &runledger_postgres::Error, expected_code: &str) -> bool {
    matches!(
        error,
        runledger_postgres::Error::QueryError(query_error)
            if query_error.code() == expected_code
    )
}

fn is_lease_owner_mismatch_error(error: &runledger_postgres::Error) -> bool {
    has_query_error_code(error, LEASE_OWNER_MISMATCH_CODE)
}

fn is_unstarted_claim_release_not_applicable_error(error: &runledger_postgres::Error) -> bool {
    has_query_error_code(error, UNSTARTED_CLAIM_RELEASE_NOT_APPLICABLE_CODE)
}

fn is_workflow_release_conflict_error(error: &runledger_postgres::Error) -> bool {
    has_query_error_code(error, WORKFLOW_RELEASE_CONFLICT_CODE)
}

fn heartbeat_interval(lease_ttl_seconds: i32) -> Duration {
    // Renew at one-third of the lease TTL so a delayed heartbeat still leaves
    // time for subsequent renewals before the lease expires.
    let seconds = (lease_ttl_seconds.max(1) / 3).max(1) as u64;
    Duration::from_secs(seconds)
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

fn dead_letter_info(job: &jobs::JobQueueRecord, failure: &JobFailure) -> Option<JobDeadLetterInfo> {
    let reason = if is_non_retryable_failure_kind(failure.kind) {
        Some(JobDeadLetterReason::FailureKindNonRetryable)
    } else if job.attempt >= job.max_attempts {
        Some(JobDeadLetterReason::AttemptsExhausted)
    } else {
        None
    }?;

    Some(JobDeadLetterInfo::new(
        failure.clone(),
        reason,
        Some(job.max_attempts),
    ))
}

async fn notify_handler_of_dead_letter(
    registry: &JobRegistry,
    context: &JobContext,
    job: &jobs::JobQueueRecord,
    dead_letter: JobDeadLetterInfo,
) {
    let Some(handler) = registry.get(job.job_type.as_borrowed()) else {
        return;
    };
    let context = context.clone();
    let payload = job.payload.clone();

    let hook_task = tokio::spawn(async move {
        tokio::time::timeout(
            TERMINAL_HOOK_TIMEOUT,
            handler.on_dead_letter(context, payload, dead_letter),
        )
        .await
        .is_ok()
    });
    match hook_task.await {
        Ok(true) => {}
        Ok(false) => {
            warn!(
                job_id = %job.id,
                job_type = %job.job_type,
                run_number = job.run_number,
                attempt = job.attempt,
                timeout_ms = TERMINAL_HOOK_TIMEOUT.as_millis(),
                "dead-letter hook timed out; continuing worker job task"
            );
        }
        Err(error) => log_dead_letter_hook_join_error(job, error),
    }
}

fn log_dead_letter_hook_join_error(job: &jobs::JobQueueRecord, error: tokio::task::JoinError) {
    if error.is_panic() {
        warn!(
            job_id = %job.id,
            job_type = %job.job_type,
            run_number = job.run_number,
            attempt = job.attempt,
            error = %error,
            "dead-letter hook panicked; continuing worker job task"
        );
    } else if error.is_cancelled() {
        warn!(
            job_id = %job.id,
            job_type = %job.job_type,
            run_number = job.run_number,
            attempt = job.attempt,
            error = %error,
            "dead-letter hook was cancelled; continuing worker job task"
        );
    } else {
        warn!(
            job_id = %job.id,
            job_type = %job.job_type,
            run_number = job.run_number,
            attempt = job.attempt,
            error = %error,
            "dead-letter hook join failed; continuing worker job task"
        );
    }
}

fn compute_retry_delay_ms(attempt: i32, job_id: uuid::Uuid) -> i32 {
    let exp = attempt.clamp(1, 10) as u32;
    let base_ms: i64 = 5_000;
    let raw = base_ms * (1_i64 << exp);
    let capped = raw.min(300_000);
    let jitter = (job_id.as_u128() % 1_000) as i64 - 500;
    (capped + jitter).max(1_000) as i32
}

#[cfg(test)]
mod tests;
