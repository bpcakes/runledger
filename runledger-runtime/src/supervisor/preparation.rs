use super::{INTENT_PROMOTER_TASK, REAPER_TASK, SCHEDULER_TASK, Supervisor, WORKER_TASK};
use crate::{
    config::{IntentPromoterConfig, JobsConfig},
    observer::{JobLifecycleObserver, JobLifecycleObservers},
    registry::JobRegistry,
    scheduler::run_scheduler_loop_initialized,
    shutdown::ShutdownSignal,
    task_group::TaskGroup,
};
use std::sync::Arc;
use tokio::runtime::Handle;

/// Owned, validated native configuration with no started tasks or database work.
/// Construct with [`super::SupervisorBuilder::prepare`] and transfer it to the
/// owner that will start and drive the runtime. Dropping this value never starts
/// work. Application-owned handler constructors and destructors retain their own
/// side effects; preparation does not execute handlers or observer callbacks.
///
/// ```no_run
/// # async fn example(pool: runledger_postgres::DbPool, config: runledger_runtime::config::JobsConfig) -> Result<(), runledger_runtime::RuntimeError> {
/// use runledger_runtime::{Supervisor, RuntimeShutdownBudget, registry::JobRegistry};
/// use std::time::Duration;
/// let prepared = Supervisor::builder(&pool, config)?
///     .with_registry(JobRegistry::new()).prepare()?;
/// // No loops have started. Ownership can be transferred here.
/// let native = prepared.start();
/// let budget = RuntimeShutdownBudget::new(Duration::from_secs(5), Duration::from_secs(1))?;
/// let report = native.shutdown_report(budget).await;
/// assert!(matches!(report.classify(), runledger_runtime::RuntimeSettlement::Clean(_)));
/// # Ok(()) }
/// ```
#[must_use = "preparation starts no work; transfer it to its owner or explicitly start it"]
pub struct PreparedSupervisor {
    pub(super) pool: runledger_postgres::DbPool,
    pub(super) runtime: Handle,
    pub(super) registry: JobRegistry,
    pub(super) config: JobsConfig,
    pub(super) observers: Vec<Arc<dyn JobLifecycleObserver>>,
    pub(super) worker_enabled: bool,
    pub(super) intent_promoter_enabled: bool,
    pub(super) intent_promoter_config: IntentPromoterConfig,
    pub(super) scheduler_enabled: bool,
    pub(super) reaper_enabled: bool,
}

impl PreparedSupervisor {
    fn initialization(&self) -> crate::startup::Initialization {
        let enabled = [
            (self.intent_promoter_enabled, INTENT_PROMOTER_TASK),
            (self.worker_enabled, WORKER_TASK),
            (self.scheduler_enabled, SCHEDULER_TASK),
            (self.reaper_enabled, REAPER_TASK),
        ];
        crate::startup::Initialization::new(
            enabled
                .into_iter()
                .filter_map(|(enabled, name)| enabled.then_some(name))
                .collect(),
        )
    }

    /// Start the selected native loops on the runtime captured during preparation.
    /// A lifecycle adapter should call this only after accepting ownership.
    /// This starts tasks immediately; the returned supervisor must be driven to
    /// settlement. Runtime death and arbitrary detached handler tasks are outside
    /// native settlement guarantees.
    pub fn start(self) -> Supervisor {
        let initialization = self.initialization();
        let Self {
            pool,
            runtime,
            config,
            observers,
            worker_enabled,
            intent_promoter_enabled,
            scheduler_enabled,
            reaper_enabled,
            registry,
            intent_promoter_config,
        } = self;
        let (mut shutdown, shutdown_rx) = ShutdownSignal::channel();
        shutdown.track_startup(initialization.clone());
        let descendants = crate::settlement::TaskRegistry::supervised(shutdown.clone());
        let mut tasks = TaskGroup::new();
        let observers = JobLifecycleObservers::from_arc_observers(observers)
            .with_settlement(descendants.clone());

        if intent_promoter_enabled {
            let startup = Some(initialization.loop_token(INTENT_PROMOTER_TASK));
            tasks.spawn_on(&runtime, INTENT_PROMOTER_TASK, {
                let pool = pool.clone();
                let registry = registry.clone();
                let shutdown_rx = shutdown_rx.clone();
                async move {
                    crate::intent_promoter::run_intent_promoter_loop_initialized(
                        pool,
                        registry,
                        intent_promoter_config,
                        shutdown_rx,
                        startup,
                    )
                    .await
                }
            });
        }

        if worker_enabled {
            let startup = Some(initialization.loop_token(WORKER_TASK));
            let owned_tasks = descendants.clone();
            tasks.spawn_on(&runtime, WORKER_TASK, {
                let pool = pool.clone();
                let registry = registry.clone();
                let config = config.clone();
                let shutdown_rx = shutdown_rx.clone();
                let observers = observers.clone();
                async move {
                    crate::worker::run_worker_loop_initialized(
                        pool,
                        registry,
                        config,
                        shutdown_rx,
                        observers,
                        startup,
                        owned_tasks,
                    )
                    .await
                }
            });
        }

        if scheduler_enabled {
            let startup = Some(initialization.loop_token(SCHEDULER_TASK));
            tasks.spawn_on(&runtime, SCHEDULER_TASK, {
                let pool = pool.clone();
                let config = config.clone();
                let shutdown_rx = shutdown_rx.clone();
                async move { run_scheduler_loop_initialized(pool, config, shutdown_rx, startup).await }
            });
        }

        if reaper_enabled {
            let startup = Some(initialization.loop_token(REAPER_TASK));
            let owned_tasks = descendants.clone();
            let pool = pool.clone();
            let registry = registry.clone();
            let config = config.clone();
            let shutdown_rx = shutdown_rx.clone();
            let observers = observers.clone();
            tasks.spawn_on(&runtime, REAPER_TASK, async move {
                crate::reaper::run_reaper_loop_initialized(
                    pool,
                    registry,
                    config,
                    shutdown_rx,
                    observers,
                    startup,
                    owned_tasks,
                )
                .await
            });
        }

        Supervisor {
            runtime,
            shutdown,
            tasks,
            initialization,
            descendants,
        }
    }
}
