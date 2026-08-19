use std::borrow::Borrow;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Handle;
use tracing::warn;

use crate::catalog::JobCatalog;
use crate::config::{IntentPromoterConfig, JobsConfig};
use crate::observer::{JobLifecycleObserver, JobLifecycleObservers};
use crate::registry::JobRegistry;
use crate::scheduler::run_scheduler_loop;
use crate::shutdown::{ShutdownHandle, ShutdownSignal};
use crate::task_group::TaskGroup;
use crate::{Result, RuntimeError};

const WORKER_TASK: &str = "worker";
const INTENT_PROMOTER_TASK: &str = "intent_promoter";
const SCHEDULER_TASK: &str = "scheduler";
const REAPER_TASK: &str = "reaper";

/// Supervises the Runledger runtime loops spawned for a worker process.
///
/// A supervisor owns the worker, intent promoter, scheduler, and reaper task
/// handles selected by [`SupervisorBuilder`]. Use
/// [`Self::run_until_shutdown`] for a typical worker process that should exit on
/// either an external shutdown signal or an internal runtime task failure.
///
/// Dropping a supervisor requests shutdown and detaches the task handles. Call
/// [`Self::shutdown`] or [`Self::join`] when the owning process needs to observe
/// panics or unexpected task exits.
#[must_use]
pub struct Supervisor {
    shutdown: ShutdownSignal,
    tasks: TaskGroup,
}

/// Builds a [`Supervisor`] with configurable runtime loops.
///
/// Worker execution, durable intent promotion, scheduler, and reaper loops are
/// enabled by default. Disabling the worker also disables intent promotion;
/// [`SupervisorBuilder::disable_intent_promoter`] can disable only promotion.
/// Call [`SupervisorBuilder::with_registry`] or
/// [`SupervisorBuilder::with_catalog`] before [`SupervisorBuilder::build`] when
/// worker or reaper loops remain enabled.
#[must_use]
pub struct SupervisorBuilder<'a> {
    pool: &'a runledger_postgres::DbPool,
    runtime: Handle,
    registry: Option<JobRegistry>,
    registry_source: Option<RegistrySource>,
    mixed_registry_sources: bool,
    config: JobsConfig,
    observers: Vec<Arc<dyn JobLifecycleObserver>>,
    worker_enabled: bool,
    intent_promoter_enabled: bool,
    intent_promoter_config: Option<IntentPromoterConfig>,
    scheduler_enabled: bool,
    reaper_enabled: bool,
}

/// Cloneable handle for requesting supervisor shutdown from another task.
#[derive(Clone)]
pub struct SupervisorShutdown {
    handle: ShutdownHandle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegistrySource {
    Registry,
    Catalog,
}

impl Supervisor {
    /// Returns a builder for a supervisor over a shared pool and runtime
    /// configuration.
    ///
    /// This validates that the caller is inside the Tokio runtime that will own
    /// spawned supervisor tasks.
    pub fn builder(
        pool: &runledger_postgres::DbPool,
        config: JobsConfig,
    ) -> std::result::Result<SupervisorBuilder<'_>, RuntimeError> {
        let runtime =
            Handle::try_current().map_err(|source| RuntimeError::MissingTokioRuntime { source })?;

        Ok(SupervisorBuilder {
            pool,
            runtime,
            registry: None,
            registry_source: None,
            mixed_registry_sources: false,
            config,
            observers: Vec::new(),
            worker_enabled: true,
            intent_promoter_enabled: true,
            intent_promoter_config: None,
            scheduler_enabled: true,
            reaper_enabled: true,
        })
    }

    /// Returns a cloneable shutdown handle that can request shutdown without
    /// owning the supervisor task joins.
    #[must_use]
    pub fn shutdown_handle(&self) -> SupervisorShutdown {
        SupervisorShutdown {
            handle: self.shutdown.handle(),
        }
    }

    /// Requests graceful shutdown of all supervised loops.
    pub fn request_shutdown(&self) {
        self.shutdown.request();
    }

    /// Returns whether shutdown has been requested through this supervisor or a
    /// clone of its shutdown handle.
    #[must_use]
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown.is_requested()
    }

    /// Waits for all supervised loops to exit.
    ///
    /// With the default long-running loops, this method waits until shutdown is
    /// requested through a [`SupervisorShutdown`] handle or until a task exits.
    /// If a loop exits before shutdown was requested, the remaining loops are
    /// asked to shut down and the first observed error is returned. Additional
    /// task failures observed while draining are logged. This method does not
    /// impose a deadline; use [`Self::shutdown_with_timeout`] when the caller
    /// owns shutdown and needs a bounded wait.
    pub async fn join(mut self) -> Result<()> {
        let shutdown = self.shutdown.clone();
        self.tasks.join(&shutdown).await
    }

    /// Requests graceful shutdown and waits for all supervised loops to exit.
    ///
    /// If a loop exits before shutdown was requested, the remaining loops are
    /// asked to shut down and the pre-existing task exit is reported, even when
    /// that exit is only observed after shutdown begins. This method does not
    /// impose a deadline. Use [`Self::shutdown_with_timeout`] when the owning
    /// process needs a shutdown budget; externally timing out this consuming
    /// future can detach still-running task handles.
    pub async fn shutdown(mut self) -> Result<()> {
        let shutdown = self.shutdown.clone();
        self.tasks.shutdown(&shutdown).await
    }

    /// Waits until `shutdown` resolves or a supervised task fails, then exits.
    ///
    /// If `shutdown` resolves first, graceful shutdown is requested and the
    /// supervisor waits up to `timeout` for all loops to exit. If a loop panics
    /// or exits unexpectedly before `shutdown` resolves, shutdown is requested
    /// for the remaining loops and the original task error is returned after
    /// those loops drain or a timeout is reported. If shutdown is requested
    /// through a [`SupervisorShutdown`] handle and every loop exits cleanly before
    /// `shutdown` resolves, this returns successfully.
    ///
    /// This is the preferred method for worker binaries because it observes
    /// internal task failures during normal operation while still applying a
    /// bounded shutdown budget to cooperative process termination.
    ///
    /// If `timeout` is too large to represent as a runtime deadline, this returns
    /// [`RuntimeError::ShutdownTimeoutTooLarge`] immediately. A zero timeout
    /// requests shutdown, aborts tasks without waiting for cooperative exits, and
    /// reports [`RuntimeError::ShutdownTimeout`].
    ///
    /// If the initial timeout validation fails before `shutdown` resolves, the
    /// supervisor is still dropped, so shutdown is requested, but task handles
    /// are not aborted or drained. If a deadline overflow is detected after
    /// shutdown begins, remaining tasks are aborted and drained before returning.
    pub async fn run_until_shutdown<F>(mut self, shutdown: F, timeout: Duration) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        let shutdown_signal = self.shutdown.clone();
        self.tasks
            .run_until_shutdown(shutdown, timeout, &shutdown_signal)
            .await
    }

    /// Requests graceful shutdown and waits up to `timeout` for all supervised
    /// loops to exit.
    ///
    /// If a loop had already exited before this method begins shutdown, that
    /// failure is returned after the remaining loops have had the same shutdown
    /// budget to exit cooperatively. If the timeout expires, remaining tasks are
    /// aborted and drained with a bounded cleanup attempt before a timeout error
    /// is returned. Abort cleanup can make total wall-clock time exceed `timeout`
    /// by up to one second, or `timeout`, whichever is smaller. A zero timeout
    /// requests shutdown, immediately aborts tasks that did not already finish,
    /// and reports [`RuntimeError::ShutdownTimeout`].
    ///
    /// If `timeout` is too large to represent as a runtime deadline, this returns
    /// [`RuntimeError::ShutdownTimeoutTooLarge`] immediately. The supervisor is
    /// still dropped, so shutdown is requested, but task handles are not aborted
    /// or drained.
    pub async fn shutdown_with_timeout(mut self, timeout: Duration) -> Result<()> {
        let shutdown = self.shutdown.clone();
        self.tasks.shutdown_with_timeout(timeout, &shutdown).await
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        if !self.tasks.is_empty() {
            warn!(
                task_count = self.tasks.len(),
                "dropping jobs runtime supervisor before joining tasks; tasks may continue detached after shutdown is requested and later panics will not be observed"
            );
        }
        // Drop cannot await task handles, so this only nudges loops to exit.
        self.request_shutdown();
    }
}

impl<'a> SupervisorBuilder<'a> {
    /// Registers the handlers used by worker execution and reaper terminal hooks.
    ///
    /// A registry is required when worker or reaper loops are enabled. Scheduler-only
    /// supervisors can be built without one.
    #[must_use = "builder methods return an updated builder value"]
    pub fn with_registry(mut self, registry: JobRegistry) -> Self {
        self.mixed_registry_sources |= self.registry_source == Some(RegistrySource::Catalog);
        self.registry_source = Some(RegistrySource::Registry);
        self.registry = Some(registry);
        self
    }

    /// Registers handlers from a [`JobCatalog`].
    ///
    /// This does not sync database job definitions. Call
    /// [`JobCatalog::sync_definitions`] before starting the supervisor or
    /// creating schedules. Pass `&catalog` when the caller will continue using
    /// the catalog for schedule, enqueue, or workflow helpers after building the
    /// supervisor.
    ///
    /// # Registry Source
    ///
    /// Calling this and [`Self::with_registry`] on the same builder is rejected
    /// by [`Self::build`]. Choose one registration source per builder.
    #[must_use = "builder methods return an updated builder value"]
    pub fn with_catalog(mut self, catalog: impl Borrow<JobCatalog>) -> Self {
        self.mixed_registry_sources |= self.registry_source == Some(RegistrySource::Registry);
        self.registry_source = Some(RegistrySource::Catalog);
        self.registry = Some(catalog.borrow().to_registry());
        self
    }

    /// Disables worker job claiming, execution, and durable intent promotion
    /// for this supervisor.
    #[must_use = "builder methods return an updated builder value"]
    pub fn disable_worker(mut self) -> Self {
        self.worker_enabled = false;
        self.intent_promoter_enabled = false;
        self
    }

    /// Disables durable enqueue-intent promotion while leaving ordinary worker
    /// claiming and execution enabled.
    #[must_use = "builder methods return an updated builder value"]
    pub fn disable_intent_promoter(mut self) -> Self {
        self.intent_promoter_enabled = false;
        self
    }

    /// Overrides the intent promoter's polling and batch controls.
    ///
    /// This does not enable a promoter disabled by [`Self::disable_worker`] or
    /// [`Self::disable_intent_promoter`].
    #[must_use = "builder methods return an updated builder value"]
    pub fn with_intent_promoter_config(mut self, config: IntentPromoterConfig) -> Self {
        self.intent_promoter_config = Some(config);
        self
    }

    /// Disables cron schedule materialization for this supervisor.
    #[must_use = "builder methods return an updated builder value"]
    pub fn disable_scheduler(mut self) -> Self {
        self.scheduler_enabled = false;
        self
    }

    /// Disables expired-lease reaping for this supervisor.
    #[must_use = "builder methods return an updated builder value"]
    pub fn disable_reaper(mut self) -> Self {
        self.reaper_enabled = false;
        self
    }

    /// Registers a best-effort observer for committed job lifecycle events.
    ///
    /// Observer callbacks run outside Runledger storage transactions. A callback
    /// timeout or panic is logged and does not change durable job state.
    #[must_use = "builder methods return an updated builder value"]
    pub fn with_job_lifecycle_observer(
        mut self,
        observer: impl JobLifecycleObserver + 'static,
    ) -> Self {
        self.observers.push(Arc::new(observer));
        self
    }

    /// Starts the enabled runtime loops and returns the owning supervisor.
    ///
    /// Returns an error when worker or reaper loops are enabled without a job
    /// registry.
    pub fn build(self) -> std::result::Result<Supervisor, RuntimeError> {
        let Self {
            pool,
            runtime,
            registry,
            registry_source: _,
            mixed_registry_sources,
            config,
            observers,
            worker_enabled,
            intent_promoter_enabled,
            intent_promoter_config,
            scheduler_enabled,
            reaper_enabled,
        } = self;

        config
            .validate()
            .map_err(|source| RuntimeError::InvalidJobsConfig { source })?;
        let intent_promoter_config = intent_promoter_config
            .unwrap_or_else(|| IntentPromoterConfig::from_jobs_config(&config));
        if intent_promoter_enabled {
            intent_promoter_config
                .validate()
                .map_err(|source| RuntimeError::InvalidJobsConfig { source })?;
        }

        if mixed_registry_sources {
            return Err(RuntimeError::MixedRegistrySources);
        }

        let registry = match registry {
            Some(registry) => registry,
            None if worker_enabled || reaper_enabled => {
                return Err(RuntimeError::MissingRegistry {
                    worker_enabled,
                    reaper_enabled,
                });
            }
            None => JobRegistry::new(),
        };

        let (shutdown, shutdown_rx) = ShutdownSignal::channel();
        let mut tasks = TaskGroup::new();
        let observers = JobLifecycleObservers::from_arc_observers(observers);

        if intent_promoter_enabled {
            tasks.spawn_on(&runtime, INTENT_PROMOTER_TASK, {
                let pool = pool.clone();
                let registry = registry.clone();
                let shutdown_rx = shutdown_rx.clone();
                async move {
                    crate::intent_promoter::run_intent_promoter_loop_with_config(
                        pool,
                        registry,
                        intent_promoter_config,
                        shutdown_rx,
                    )
                    .await
                }
            });
        }

        if worker_enabled {
            tasks.spawn_on(&runtime, WORKER_TASK, {
                let pool = pool.clone();
                let registry = registry.clone();
                let config = config.clone();
                let shutdown_rx = shutdown_rx.clone();
                let observers = observers.clone();
                async move {
                    crate::worker::run_worker_loop_with_observer(
                        pool,
                        registry,
                        config,
                        shutdown_rx,
                        observers,
                    )
                    .await
                }
            });
        }

        if scheduler_enabled {
            tasks.spawn_on(&runtime, SCHEDULER_TASK, {
                let pool = pool.clone();
                let config = config.clone();
                let shutdown_rx = shutdown_rx.clone();
                async move { run_scheduler_loop(pool, config, shutdown_rx).await }
            });
        }

        if reaper_enabled {
            let pool = pool.clone();
            let registry = registry.clone();
            let config = config.clone();
            let shutdown_rx = shutdown_rx.clone();
            let observers = observers.clone();
            tasks.spawn_on(&runtime, REAPER_TASK, async move {
                crate::reaper::run_reaper_loop_with_observer(
                    pool,
                    registry,
                    config,
                    shutdown_rx,
                    observers,
                )
                .await
            });
        }

        Ok(Supervisor { shutdown, tasks })
    }
}

impl SupervisorShutdown {
    /// Requests graceful shutdown of all loops watched by the supervisor.
    pub fn request_shutdown(&self) {
        self.handle.request();
    }

    /// Returns whether shutdown has been requested.
    #[must_use]
    pub fn is_shutdown_requested(&self) -> bool {
        self.handle.is_requested()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use sqlx::postgres::PgPoolOptions;
    use tokio::time::timeout;

    use super::*;

    const UNUSED_LAZY_POOL_URL: &str = "postgres://postgres:postgres@127.0.0.1:65535/runledger";

    fn lazy_pool() -> runledger_postgres::DbPool {
        PgPoolOptions::new()
            // The disable-only tests never acquire this pool; this URL is only
            // a valid PgPool value for supervisor wiring assertions.
            .connect_lazy(UNUSED_LAZY_POOL_URL)
            .expect("construct lazy pool")
    }

    fn test_config() -> JobsConfig {
        JobsConfig {
            worker_id: "supervisor-test-worker".to_string(),
            poll_interval: Duration::from_millis(25),
            claim_batch_size: 4,
            lease_ttl_seconds: 10,
            max_global_concurrency: 4,
            reaper_interval: Duration::from_millis(50),
            schedule_poll_interval: Duration::from_millis(50),
            reaper_retry_delay_ms: 1_000,
        }
    }

    fn empty_builder(pool: &runledger_postgres::DbPool) -> SupervisorBuilder<'_> {
        Supervisor::builder(pool, test_config()).expect("supervisor builder has runtime")
    }

    fn missing_registry_flags(builder: SupervisorBuilder<'_>) -> (bool, bool) {
        match builder.build() {
            Err(RuntimeError::MissingRegistry {
                worker_enabled,
                reaper_enabled,
            }) => (worker_enabled, reaper_enabled),
            Ok(_) => panic!("missing registry should be a build error"),
            Err(other) => panic!("expected missing registry error, got {other:?}"),
        }
    }

    fn task_names(supervisor: &Supervisor) -> Vec<&'static str> {
        supervisor.tasks.names_for_tests()
    }

    async fn abort_supervisor_tasks(mut supervisor: Supervisor) {
        supervisor.tasks.abort_all_for_tests().await;
    }

    #[tokio::test]
    async fn builder_defaults_enable_all_loops() {
        let pool = lazy_pool();
        let builder = empty_builder(&pool);

        assert!(builder.worker_enabled);
        assert!(builder.intent_promoter_enabled);
        assert_eq!(builder.intent_promoter_config, None);
        assert!(builder.scheduler_enabled);
        assert!(builder.reaper_enabled);
        assert!(builder.registry.is_none());
        assert_eq!(builder.registry_source, None);
        assert!(!builder.mixed_registry_sources);
    }

    #[tokio::test]
    async fn builder_accepts_registry_for_worker_and_reaper_loops() {
        let pool = lazy_pool();
        let builder = empty_builder(&pool).with_registry(JobRegistry::new());

        assert!(builder.registry.is_some());
        assert_eq!(builder.registry_source, Some(RegistrySource::Registry));
        assert!(!builder.mixed_registry_sources);
    }

    #[tokio::test]
    async fn builder_rejects_mixed_registry_sources() {
        let pool = lazy_pool();
        let registry_then_catalog = empty_builder(&pool)
            .with_registry(JobRegistry::new())
            .with_catalog(JobCatalog::new())
            .disable_worker()
            .disable_reaper()
            .build();
        let Err(registry_then_catalog) = registry_then_catalog else {
            panic!("mixed registry sources should be rejected");
        };
        assert!(matches!(
            registry_then_catalog,
            RuntimeError::MixedRegistrySources
        ));

        let catalog_then_registry = empty_builder(&pool)
            .with_catalog(JobCatalog::new())
            .with_registry(JobRegistry::new())
            .disable_worker()
            .disable_reaper()
            .build();
        let Err(catalog_then_registry) = catalog_then_registry else {
            panic!("mixed registry sources should be rejected");
        };
        assert!(matches!(
            catalog_then_registry,
            RuntimeError::MixedRegistrySources
        ));
    }

    #[tokio::test]
    async fn builder_requires_registry_when_worker_or_reaper_is_enabled() {
        let pool = lazy_pool();

        assert_eq!(missing_registry_flags(empty_builder(&pool)), (true, true));
        assert_eq!(
            missing_registry_flags(empty_builder(&pool).disable_scheduler().disable_reaper()),
            (true, false)
        );
        assert_eq!(
            missing_registry_flags(empty_builder(&pool).disable_worker().disable_scheduler()),
            (false, true)
        );
    }

    #[tokio::test]
    async fn builder_rejects_invalid_direct_config_values_before_spawning_loops() {
        let cases = [
            {
                let mut config = test_config();
                config.max_global_concurrency = 0;
                (
                    config,
                    crate::config::JobsConfigValidationError::InvalidMaxGlobalConcurrency,
                )
            },
            {
                let mut config = test_config();
                config.claim_batch_size = 0;
                (
                    config,
                    crate::config::JobsConfigValidationError::InvalidClaimBatchSize { actual: 0 },
                )
            },
            {
                let mut config = test_config();
                config.lease_ttl_seconds = 0;
                (
                    config,
                    crate::config::JobsConfigValidationError::InvalidLeaseTtlSeconds { actual: 0 },
                )
            },
        ];

        for (config, expected) in cases {
            let pool = lazy_pool();
            let result = Supervisor::builder(&pool, config)
                .expect("supervisor builder has runtime")
                .disable_worker()
                .disable_scheduler()
                .disable_reaper()
                .build();
            let Err(error) = result else {
                panic!("invalid direct config should be rejected");
            };

            match error {
                RuntimeError::InvalidJobsConfig { source } => {
                    assert_eq!(source, expected);
                }
                other => panic!("expected invalid jobs config error, got {other:?}"),
            }
        }
    }

    #[test]
    fn builder_requires_tokio_runtime_before_cloning_pool() {
        let runtime = tokio::runtime::Runtime::new().expect("construct Tokio runtime");
        let pool = runtime.block_on(async { lazy_pool() });
        let error = match Supervisor::builder(&pool, test_config()) {
            Err(error) => error,
            Ok(builder) => {
                drop(builder);
                runtime.block_on(async {
                    pool.close().await;
                });
                std::mem::forget(pool);
                panic!("missing Tokio runtime should be a builder error");
            }
        };

        // The builder was intentionally called outside a runtime to exercise
        // the pre-clone runtime check. Close and drop the pool inside the
        // temporary runtime so sqlx's own drop precondition does not contaminate
        // this assertion.
        runtime.block_on(async {
            pool.close().await;
        });
        std::mem::forget(pool);
        match error {
            RuntimeError::MissingTokioRuntime { .. } => {}
            other => panic!("expected missing Tokio runtime error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn builder_can_disable_each_loop() {
        let pool = lazy_pool();
        let builder = empty_builder(&pool)
            .disable_worker()
            .disable_scheduler()
            .disable_reaper();

        assert!(!builder.worker_enabled);
        assert!(!builder.intent_promoter_enabled);
        assert!(!builder.scheduler_enabled);
        assert!(!builder.reaper_enabled);

        let worker_without_promoter = empty_builder(&pool).disable_intent_promoter();
        assert!(worker_without_promoter.worker_enabled);
        assert!(!worker_without_promoter.intent_promoter_enabled);

        let promoter_config = IntentPromoterConfig::new(Duration::from_secs(2), 7);
        let customized = empty_builder(&pool).with_intent_promoter_config(promoter_config);
        assert_eq!(customized.intent_promoter_config, Some(promoter_config));
    }

    #[tokio::test]
    async fn builder_spawns_only_enabled_tasks() {
        let pool = lazy_pool();

        let all_disabled = empty_builder(&pool)
            .disable_worker()
            .disable_scheduler()
            .disable_reaper()
            .build()
            .expect("all-disabled supervisor should build");
        assert_eq!(task_names(&all_disabled), Vec::<&'static str>::new());
        abort_supervisor_tasks(all_disabled).await;

        let scheduler_only = empty_builder(&pool)
            .disable_worker()
            .disable_reaper()
            .build()
            .expect("scheduler-only supervisor should not require registry");
        assert_eq!(task_names(&scheduler_only), vec![SCHEDULER_TASK]);
        abort_supervisor_tasks(scheduler_only).await;

        let worker_only = empty_builder(&pool)
            .with_registry(JobRegistry::new())
            .disable_scheduler()
            .disable_reaper()
            .build()
            .expect("worker-only supervisor should build with registry");
        assert_eq!(
            task_names(&worker_only),
            vec![INTENT_PROMOTER_TASK, WORKER_TASK]
        );
        abort_supervisor_tasks(worker_only).await;

        let worker_without_promoter = empty_builder(&pool)
            .with_registry(JobRegistry::new())
            .disable_intent_promoter()
            .disable_scheduler()
            .disable_reaper()
            .build()
            .expect("worker should run without intent promotion");
        assert_eq!(task_names(&worker_without_promoter), vec![WORKER_TASK]);
        abort_supervisor_tasks(worker_without_promoter).await;

        let reaper_only = empty_builder(&pool)
            .with_registry(JobRegistry::new())
            .disable_worker()
            .disable_scheduler()
            .build()
            .expect("reaper-only supervisor should build with registry");
        assert_eq!(task_names(&reaper_only), vec![REAPER_TASK]);
        abort_supervisor_tasks(reaper_only).await;

        let all_enabled = empty_builder(&pool)
            .with_registry(JobRegistry::new())
            .build()
            .expect("all-enabled supervisor should build with registry");
        assert_eq!(
            task_names(&all_enabled),
            vec![
                INTENT_PROMOTER_TASK,
                WORKER_TASK,
                SCHEDULER_TASK,
                REAPER_TASK
            ]
        );
        abort_supervisor_tasks(all_enabled).await;
    }

    #[tokio::test]
    async fn all_disabled_supervisor_join_and_shutdown_succeed() {
        Supervisor::builder(&lazy_pool(), test_config())
            .expect("supervisor builder has runtime")
            .disable_worker()
            .disable_scheduler()
            .disable_reaper()
            .build()
            .expect("all-disabled supervisor should build")
            .join()
            .await
            .expect("all-disabled supervisor should join");

        Supervisor::builder(&lazy_pool(), test_config())
            .expect("supervisor builder has runtime")
            .disable_worker()
            .disable_scheduler()
            .disable_reaper()
            .build()
            .expect("all-disabled supervisor should build")
            .shutdown()
            .await
            .expect("all-disabled supervisor should shut down");
    }

    #[tokio::test]
    async fn shutdown_handle_can_request_shutdown_before_join() {
        let supervisor = Supervisor::builder(&lazy_pool(), test_config())
            .expect("supervisor builder has runtime")
            .disable_worker()
            .disable_scheduler()
            .disable_reaper()
            .build()
            .expect("all-disabled supervisor should build");
        let shutdown = supervisor.shutdown_handle();
        let cloned_shutdown = shutdown.clone();

        cloned_shutdown.request_shutdown();

        assert!(shutdown.is_shutdown_requested());
        assert!(supervisor.is_shutdown_requested());
        supervisor
            .join()
            .await
            .expect("supervisor should join after shutdown handle request");
    }

    #[tokio::test]
    async fn run_until_shutdown_with_no_tasks_waits_for_signal() {
        let supervisor = Supervisor::builder(&lazy_pool(), test_config())
            .expect("supervisor builder has runtime")
            .disable_worker()
            .disable_scheduler()
            .disable_reaper()
            .build()
            .expect("all-disabled supervisor should build");
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        let mut run = tokio::spawn(supervisor.run_until_shutdown(
            async move {
                signal_rx.await.expect("shutdown signal should be sent");
            },
            Duration::from_secs(1),
        ));

        assert!(
            timeout(Duration::from_millis(50), &mut run).await.is_err(),
            "all-disabled supervisor should wait for the shutdown signal"
        );

        signal_tx.send(()).expect("signal receiver should be alive");
        run.await
            .expect("run-until-shutdown task should join")
            .expect("all-disabled supervisor should complete after signal");
    }
}
