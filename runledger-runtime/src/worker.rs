use std::cmp::min;
use std::sync::Arc;

use runledger_core::jobs::JobType;
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
    shutdown: watch::Receiver<bool>,
    observers: JobLifecycleObservers,
) -> RuntimeLoopExit {
    if let Err(error) = config.validate_worker_loop() {
        warn!(%error, "invalid jobs config; stopping worker loop");
        return RuntimeLoopExit::InvalidConfig(error);
    }

    WorkerLoop::new(pool, registry, config, shutdown, observers)
        .run()
        .await
}

struct WorkerLoop {
    // Keep the former function-local cancellation drop order: observer tasks
    // before job tasks, then the shared execution state and inputs.
    terminal_observer_tasks: TerminalObserverTasks,
    join_set: JoinSet<()>,
    semaphore: Arc<Semaphore>,
    claimable_job_types: Vec<JobType<'static>>,
    registry: Arc<JobRegistry>,
    observers: JobLifecycleObservers,
    shutdown: watch::Receiver<bool>,
    config: JobsConfig,
    pool: runledger_postgres::DbPool,
}

enum WorkerLoopControl {
    Continue,
    Drain(RuntimeLoopExit),
}

impl WorkerLoop {
    fn new(
        pool: runledger_postgres::DbPool,
        registry: JobRegistry,
        config: JobsConfig,
        shutdown: watch::Receiver<bool>,
        observers: JobLifecycleObservers,
    ) -> Self {
        let registry = Arc::new(registry);
        let claimable_job_types = registry.registered_static_types();
        let semaphore = Arc::new(Semaphore::new(config.max_global_concurrency));
        let join_set = JoinSet::new();
        let terminal_observer_tasks = TerminalObserverTasks::owned();

        Self {
            terminal_observer_tasks,
            join_set,
            semaphore,
            claimable_job_types,
            registry,
            observers,
            shutdown,
            config,
            pool,
        }
    }

    async fn run(mut self) -> RuntimeLoopExit {
        loop {
            match self.iteration().await {
                WorkerLoopControl::Continue => {}
                WorkerLoopControl::Drain(exit) => return self.drain(exit).await,
            }
        }
    }

    async fn iteration(&mut self) -> WorkerLoopControl {
        self.drain_finished_tasks().await;
        self.terminal_observer_tasks.drain_finished().await;

        if shutdown::is_requested_or_closed(&self.shutdown) {
            return WorkerLoopControl::Drain(RuntimeLoopExit::Shutdown);
        }

        if self.claimable_job_types.is_empty() {
            if self.wait_for_shutdown().await {
                return WorkerLoopControl::Drain(RuntimeLoopExit::Shutdown);
            }
            return WorkerLoopControl::Continue;
        }

        let available = self.semaphore.available_permits();
        if available == 0 {
            if self.wait_for_shutdown().await {
                return WorkerLoopControl::Drain(RuntimeLoopExit::Shutdown);
            }
            return WorkerLoopControl::Continue;
        }

        let claim_limit = min(available, self.config.claim_batch_size as usize);
        let claimed = self.claim(claim_limit).await;

        if claimed.is_empty() {
            self.wait_for_shutdown().await;
            return WorkerLoopControl::Continue;
        }

        let claimed_len = claimed.len();
        if !self.spawn_claimed_jobs(claimed).await {
            return WorkerLoopControl::Drain(RuntimeLoopExit::Completed);
        }

        if claimed_len == claim_limit {
            return WorkerLoopControl::Continue;
        }

        if self.wait_for_shutdown().await {
            return WorkerLoopControl::Drain(RuntimeLoopExit::Shutdown);
        }

        WorkerLoopControl::Continue
    }

    async fn claim(&self, claim_limit: usize) -> Vec<jobs::JobQueueRecord> {
        match jobs::claim_prestart_jobs_for_types(
            &self.pool,
            &self.config.worker_id,
            self.config.lease_ttl_seconds,
            claim_limit as i64,
            &self.claimable_job_types,
        )
        .await
        {
            Ok(claimed) => claimed,
            Err(error) => {
                let error = WorkerError::ClaimJobs {
                    worker_id: self.config.worker_id.clone(),
                    source: error,
                };
                warn!(%error, "worker claim failed");
                Vec::new()
            }
        }
    }

    async fn spawn_claimed_jobs(&mut self, claimed: Vec<jobs::JobQueueRecord>) -> bool {
        for job in claimed {
            let permit = match Arc::clone(&self.semaphore).acquire_owned().await {
                Ok(permit) => permit,
                Err(_) => {
                    // The worker owns this semaphore and never closes it. If
                    // this defensive branch fires, surface it as an unexpected
                    // loop completion rather than graceful shutdown.
                    warn!("worker semaphore closed; stopping worker loop");
                    return false;
                }
            };
            let pool_clone = self.pool.clone();
            let registry_clone = Arc::clone(&self.registry);
            let lease_ttl_seconds = self.config.lease_ttl_seconds;
            let observers = self.observers.clone();
            let terminal_observer_tasks = self.terminal_observer_tasks.clone();
            self.join_set.spawn(async move {
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

        true
    }

    async fn wait_for_shutdown(&mut self) -> bool {
        shutdown::wait_for_request_or_timeout(&mut self.shutdown, self.config.poll_interval).await
    }

    async fn drain(mut self, exit: RuntimeLoopExit) -> RuntimeLoopExit {
        if !self.join_set.is_empty() {
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
        while let Some(result) = self.join_set.join_next().await {
            if let Err(error) = result {
                error!(%error, "job task crashed while draining in-flight jobs");
            }
        }
        self.terminal_observer_tasks.drain_for_shutdown().await;
        exit
    }

    async fn drain_finished_tasks(&mut self) {
        while let Some(result) = self.join_set.try_join_next() {
            if let Err(error) = result {
                error!(%error, "job task crashed");
            }
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
