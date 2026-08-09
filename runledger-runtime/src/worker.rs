use std::cmp::min;
use std::sync::Arc;

use runledger_postgres::jobs;
use tokio::sync::{Semaphore, watch};
use tokio::task::JoinSet;
use tracing::{error, info, warn};

mod completion;
mod dead_letter;
mod execution;
mod observers;

use self::execution::ClaimedJobExecution;
use self::observers::TerminalObserverTasks;
use crate::RuntimeLoopExit;
use crate::WorkerError;
use crate::config::JobsConfig;
use crate::observer::JobLifecycleObservers;
use crate::registry::JobRegistry;
use crate::shutdown;

pub async fn run_worker_loop(
    pool: runledger_postgres::DbPool,
    registry: JobRegistry,
    config: JobsConfig,
    shutdown: watch::Receiver<bool>,
) -> RuntimeLoopExit {
    run_worker_loop_with_observer(
        pool,
        registry,
        config,
        shutdown,
        JobLifecycleObservers::empty(),
    )
    .await
}

pub async fn run_worker_loop_with_observer(
    pool: runledger_postgres::DbPool,
    registry: JobRegistry,
    config: JobsConfig,
    mut shutdown: watch::Receiver<bool>,
    observers: JobLifecycleObservers,
) -> RuntimeLoopExit {
    if let Err(error) = config.validate_worker_loop() {
        warn!(%error, "invalid jobs config; stopping worker loop");
        return RuntimeLoopExit::InvalidConfig(error);
    }

    let registry = Arc::new(registry);
    let claimable_job_types = registry.registered_types();
    let semaphore = Arc::new(Semaphore::new(config.max_global_concurrency));
    let mut join_set: JoinSet<()> = JoinSet::new();
    let terminal_observer_tasks = TerminalObserverTasks::owned();

    loop {
        drain_finished_tasks(&mut join_set).await;
        terminal_observer_tasks.drain_finished().await;

        if shutdown::is_requested_or_closed(&shutdown) {
            return drain_worker_tasks(
                join_set,
                terminal_observer_tasks,
                RuntimeLoopExit::Shutdown,
            )
            .await;
        }

        if claimable_job_types.is_empty() {
            if shutdown::wait_for_request_or_timeout(&mut shutdown, config.poll_interval).await {
                return drain_worker_tasks(
                    join_set,
                    terminal_observer_tasks,
                    RuntimeLoopExit::Shutdown,
                )
                .await;
            }
            continue;
        }

        let available = semaphore.available_permits();
        if available == 0 {
            if shutdown::wait_for_request_or_timeout(&mut shutdown, config.poll_interval).await {
                return drain_worker_tasks(
                    join_set,
                    terminal_observer_tasks,
                    RuntimeLoopExit::Shutdown,
                )
                .await;
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
                    return drain_worker_tasks(
                        join_set,
                        terminal_observer_tasks,
                        RuntimeLoopExit::Completed,
                    )
                    .await;
                }
            };
            let pool_clone = pool.clone();
            let registry_clone = Arc::clone(&registry);
            let lease_ttl_seconds = config.lease_ttl_seconds;
            let observers = observers.clone();
            let terminal_observer_tasks = terminal_observer_tasks.clone();
            join_set.spawn(async move {
                let _permit = permit;
                process_claimed_job_with_terminal_observers(
                    pool_clone,
                    registry_clone,
                    job,
                    lease_ttl_seconds,
                    observers,
                    terminal_observer_tasks,
                )
                .await;
            });
        }

        if claimed_len == claim_limit {
            continue;
        }

        if shutdown::wait_for_request_or_timeout(&mut shutdown, config.poll_interval).await {
            return drain_worker_tasks(
                join_set,
                terminal_observer_tasks,
                RuntimeLoopExit::Shutdown,
            )
            .await;
        }
    }
}

async fn drain_worker_tasks(
    mut join_set: JoinSet<()>,
    terminal_observer_tasks: TerminalObserverTasks,
    exit: RuntimeLoopExit,
) -> RuntimeLoopExit {
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
    while let Some(result) = join_set.join_next().await {
        if let Err(error) = result {
            error!(%error, "job task crashed while draining in-flight jobs");
        }
    }
    terminal_observer_tasks.drain_for_shutdown().await;
    exit
}

async fn drain_finished_tasks(join_set: &mut JoinSet<()>) {
    while let Some(result) = join_set.try_join_next() {
        if let Err(error) = result {
            error!(%error, "job task crashed");
        }
    }
}

#[cfg(test)]
async fn process_claimed_job(
    pool: runledger_postgres::DbPool,
    registry: Arc<JobRegistry>,
    job: jobs::JobQueueRecord,
    lease_ttl_seconds: i32,
) {
    process_claimed_job_with_observer(
        pool,
        registry,
        job,
        lease_ttl_seconds,
        JobLifecycleObservers::empty(),
    )
    .await;
}

#[cfg(test)]
async fn process_claimed_job_with_observer(
    pool: runledger_postgres::DbPool,
    registry: Arc<JobRegistry>,
    job: jobs::JobQueueRecord,
    lease_ttl_seconds: i32,
    observers: JobLifecycleObservers,
) {
    process_claimed_job_with_terminal_observers(
        pool,
        registry,
        job,
        lease_ttl_seconds,
        observers,
        TerminalObserverTasks::detached(),
    )
    .await;
}

async fn process_claimed_job_with_terminal_observers(
    pool: runledger_postgres::DbPool,
    registry: Arc<JobRegistry>,
    job: jobs::JobQueueRecord,
    lease_ttl_seconds: i32,
    observers: JobLifecycleObservers,
    terminal_observer_tasks: TerminalObserverTasks,
) {
    ClaimedJobExecution::new(
        pool,
        registry,
        job,
        lease_ttl_seconds,
        observers,
        terminal_observer_tasks,
    )
    .execute()
    .await;
}

#[cfg(test)]
mod tests;
