use std::cmp::min;
use std::sync::Arc;

use runledger_core::jobs::JobType;
use runledger_postgres::jobs;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tracing::{error, info, warn};

mod completion;
mod dead_letter;
mod execution;
mod execution_services;
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
    // Field order mirrors the former function locals' drop order; do not reorder.
    terminal_observer_tasks: TerminalObserverTasks,
    join_set: JoinSet<()>,
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
        let join_set = JoinSet::new();
        let terminal_observer_tasks = TerminalObserverTasks::owned();

        Self {
            terminal_observer_tasks,
            join_set,
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
            if self.wait_for_shutdown_or_poll_interval().await {
                return WorkerLoopControl::Drain(RuntimeLoopExit::Shutdown);
            }
            return WorkerLoopControl::Continue;
        }

        let available = self.available_capacity();
        if available == 0 {
            if self.wait_for_shutdown_or_poll_interval().await {
                return WorkerLoopControl::Drain(RuntimeLoopExit::Shutdown);
            }
            return WorkerLoopControl::Continue;
        }

        let claim_limit = min(available, self.config.claim_batch_size as usize);
        let claimed = self.claim(claim_limit).await;

        if claimed.is_empty() {
            self.wait_for_shutdown_or_poll_interval().await;
            return WorkerLoopControl::Continue;
        }

        let claimed_len = claimed.len();
        self.spawn_claimed_jobs(claimed);

        if claimed_len == claim_limit {
            return WorkerLoopControl::Continue;
        }

        if self.wait_for_shutdown_or_poll_interval().await {
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

    fn available_capacity(&self) -> usize {
        self.config
            .max_global_concurrency
            .saturating_sub(self.join_set.len())
    }

    fn spawn_claimed_jobs(&mut self, claimed: Vec<jobs::JobQueueRecord>) {
        debug_assert!(claimed.len() <= self.available_capacity());
        for job in claimed {
            let pool_clone = self.pool.clone();
            let registry_clone = Arc::clone(&self.registry);
            let lease_ttl_seconds = self.config.lease_ttl_seconds;
            let observers = self.observers.clone();
            let terminal_observer_tasks = self.terminal_observer_tasks.clone();
            self.join_set.spawn(async move {
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
    }

    async fn wait_for_shutdown_or_poll_interval(&mut self) -> bool {
        shutdown::wait_for_request_or_timeout(&mut self.shutdown, self.config.poll_interval).await
    }

    async fn drain(mut self, exit: RuntimeLoopExit) -> RuntimeLoopExit {
        self.log_drain_start(&exit);
        while let Some(result) = self.join_set.join_next().await {
            log_drained_job_task_result(result);
        }
        self.terminal_observer_tasks.drain_for_shutdown().await;
        exit
    }

    fn log_drain_start(&self, exit: &RuntimeLoopExit) {
        if self.join_set.is_empty() {
            return;
        }
        match exit {
            RuntimeLoopExit::Shutdown => log_worker_shutdown_drain(),
            RuntimeLoopExit::InvalidConfig(_) => log_worker_invalid_config_drain(),
            RuntimeLoopExit::Completed => log_worker_completed_drain(),
        }
    }

    async fn drain_finished_tasks(&mut self) {
        while let Some(result) = self.join_set.try_join_next() {
            log_finished_job_task_result(result);
        }
    }
}

fn log_worker_shutdown_drain() {
    info!("worker shutdown requested; draining in-flight jobs");
}

fn log_worker_invalid_config_drain() {
    warn!("worker loop rejected invalid config; draining in-flight jobs");
}

fn log_worker_completed_drain() {
    warn!("worker loop completed before shutdown; draining in-flight jobs");
}

fn log_drained_job_task_result(result: Result<(), tokio::task::JoinError>) {
    if let Err(error) = result {
        error!(%error, "job task crashed while draining in-flight jobs");
    }
}

fn log_finished_job_task_result(result: Result<(), tokio::task::JoinError>) {
    if let Err(error) = result {
        error!(%error, "job task crashed");
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
    let Some(execution) = ClaimedJobExecution::new(
        pool,
        registry,
        job,
        lease_ttl_seconds,
        observers,
        terminal_observer_tasks,
    ) else {
        return;
    };
    execution.execute().await;
}

#[cfg(test)]
mod tests;
