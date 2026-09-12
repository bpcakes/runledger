use std::borrow::Borrow;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Handle;
use tracing::warn;

use crate::catalog::JobCatalog;
use crate::config::{IntentPromoterConfig, JobsConfig};
use crate::observer::JobLifecycleObserver;
use crate::registry::JobRegistry;
use crate::shutdown::{ShutdownHandle, ShutdownSignal};
use crate::task_group::TaskGroup;
use crate::{Error, Result, RuntimeError};

#[path = "supervisor/preparation.rs"]
mod preparation;
pub use preparation::PreparedSupervisor;

const WORKER_TASK: &str = "worker";
const INTENT_PROMOTER_TASK: &str = "intent_promoter";
const SCHEDULER_TASK: &str = "scheduler";
const REAPER_TASK: &str = "reaper";

#[cfg(test)]
#[path = "supervisor/settlement_live.rs"]
mod settlement_live;

#[cfg(test)]
#[path = "supervisor/preparation_tests.rs"]
mod preparation_tests;

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
    initialization: crate::startup::Initialization,
    descendants: crate::settlement::TaskRegistry,
}

/// Builds a [`Supervisor`] with configurable runtime loops.
///
/// Worker execution, durable intent promotion, scheduler, and reaper loops are
/// enabled by default. Disabling the worker also disables intent promotion;
/// [`SupervisorBuilder::disable_intent_promoter`] can disable only promotion.
/// Every enabled supervisor polls independently, including when no intents are
/// pending. Deployments may tune [`IntentPromoterConfig`] or disable redundant
/// promoters, but must retain promoter coverage for every registered type that
/// can receive durable intents.
/// Call [`SupervisorBuilder::with_registry`] or
/// [`SupervisorBuilder::with_catalog`] before [`SupervisorBuilder::build`] when
/// worker or reaper loops remain enabled.
#[must_use]
pub struct SupervisorBuilder<'a> {
    pool: &'a runledger_postgres::DbPool,
    runtime: Handle,
    registry_selection: Option<RegistrySelection>,
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

enum RegistrySelection {
    Direct(JobRegistry),
    Catalog(JobRegistry),
    Mixed,
}

impl RegistrySelection {
    fn direct(current: Option<Self>, registry: JobRegistry) -> Self {
        match current {
            None | Some(Self::Direct(_)) => Self::Direct(registry),
            Some(Self::Catalog(_)) | Some(Self::Mixed) => Self::Mixed,
        }
    }

    fn catalog(current: Option<Self>, registry: JobRegistry) -> Self {
        match current {
            None | Some(Self::Catalog(_)) => Self::Catalog(registry),
            Some(Self::Direct(_)) | Some(Self::Mixed) => Self::Mixed,
        }
    }
}

impl Supervisor {
    /// Returns a supervisor builder configured from the process environment.
    ///
    /// Worker settings come from [`JobsConfig::from_env`]. Intent-promotion
    /// settings inherit the worker polling interval and batch size unless the
    /// corresponding `JOBS_INTENT_PROMOTER_*` variable is set.
    pub fn builder_from_env(
        pool: &runledger_postgres::DbPool,
    ) -> std::result::Result<SupervisorBuilder<'_>, RuntimeError> {
        let config = JobsConfig::from_env();
        let intent_promoter_config =
            IntentPromoterConfig::from_env_with_jobs_config_defaults(&config);

        Self::builder(pool, config)
            .map(|builder| builder.with_intent_promoter_config(intent_promoter_config))
    }

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
            registry_selection: None,
            config,
            observers: Vec::new(),
            worker_enabled: true,
            intent_promoter_enabled: true,
            intent_promoter_config: None,
            scheduler_enabled: true,
            reaper_enabled: true,
        })
    }

    /// Observe local initialization without taking ownership of runtime tasks.
    ///
    /// ```rust,no_run
    /// # async fn example(supervisor: runledger_runtime::Supervisor) {
    /// let startup = supervisor.startup_observer();
    /// match startup.wait_initialized().await {
    ///     Ok(()) => { /* apply application policy */ }
    ///     Err(_) => { /* startup stopped; observe the native shutdown result */ }
    /// }
    /// supervisor.shutdown().await.expect("observe shutdown");
    /// # }
    /// ```
    pub fn startup_observer(&self) -> crate::RuntimeStartupObserver {
        self.initialization.observer()
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
    /// This Result path observes loop exits and descendant failures available at
    /// completion; it does not await complete descendant settlement or authorize
    /// dependency cleanup. Use [`Self::run_until_shutdown_report`] for that contract.
    ///
    /// If a loop exits before shutdown was requested, the remaining loops are
    /// asked to shut down and the first observed error is returned. Additional
    /// task failures observed while draining are logged. This method does not
    /// impose a deadline; use [`Self::shutdown_with_timeout`] when the caller
    /// owns shutdown and needs a bounded wait.
    pub async fn join(mut self) -> Result<()> {
        let shutdown = self.shutdown.clone();
        let mut result = self
            .descendants
            .observe_while(self.tasks.join(&shutdown))
            .await;
        self.retain_descendant_failure(&mut result);
        result
    }

    /// Requests graceful shutdown and waits for all supervised loops to exit.
    ///
    /// If a loop exits before shutdown was requested, the remaining loops are
    /// asked to shut down and the pre-existing task exit is reported, even when
    /// that exit is only observed after shutdown begins. This method does not
    /// impose a deadline. It has the same descendant-settlement limits as
    /// [`Self::join`]. Use [`Self::shutdown_with_timeout`] when the owning
    /// process needs a shutdown budget; externally timing out this consuming
    /// future can detach still-running task handles.
    pub async fn shutdown(mut self) -> Result<()> {
        let shutdown = self.shutdown.clone();
        let mut result = self
            .descendants
            .observe_while(self.tasks.shutdown(&shutdown))
            .await;
        self.retain_descendant_failure(&mut result);
        result
    }

    /// Waits until `shutdown` resolves or a supervised task fails, then exits.
    ///
    /// If `shutdown` resolves or a [`SupervisorShutdown`] handle requests stop,
    /// graceful shutdown begins and the supervisor waits up to `timeout` for all
    /// loops to exit. A handle request applies this budget even if `shutdown`
    /// remains pending. If a loop panics
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
        let external_or_native_stop = async {
            tokio::select! {
                biased;
                () = shutdown_signal.requested() => {}
                () = shutdown => {}
            }
        };
        let mut result = self
            .descendants
            .observe_while(self.tasks.run_until_shutdown(
                external_or_native_stop,
                timeout,
                &shutdown_signal,
            ))
            .await;
        self.retain_descendant_failure(&mut result);
        result
    }

    /// Drive native loops and retain complete bounded settlement, including owned
    /// callback descendants. Both the external future and shutdown handles start
    /// the same non-resetting stop budget. Cancelling this consuming future still
    /// requests shutdown through Drop; an integration owner must retain the driver.
    ///
    /// ```rust,no_run
    /// # async fn example(supervisor: runledger_runtime::Supervisor) -> Result<(), runledger_runtime::RuntimeError> {
    /// use runledger_runtime::RuntimeShutdownBudget;
    /// use std::time::Duration;
    /// let budget = RuntimeShutdownBudget::new(Duration::from_secs(20), Duration::from_secs(1))?;
    /// let report = supervisor.run_until_shutdown_report(async {}, budget).await;
    /// assert!(report.is_success());
    /// # Ok(()) }
    /// ```
    pub async fn run_until_shutdown_report<F>(
        mut self,
        shutdown: F,
        budget: crate::RuntimeShutdownBudget,
    ) -> crate::RuntimeShutdownReport
    where
        F: Future<Output = ()>,
    {
        self.tasks
            .run_report(shutdown, budget, &self.shutdown, &self.descendants)
            .await
    }

    /// Requests graceful shutdown and waits up to `timeout` for all supervised
    /// loops to exit. This Result path has the descendant-settlement limits of
    /// [`Self::join`]; use [`Self::run_until_shutdown_report`] for dependency cleanup.
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
        let mut result = self
            .descendants
            .observe_while(self.tasks.shutdown_with_timeout(timeout, &shutdown))
            .await;
        self.retain_descendant_failure(&mut result);
        result
    }

    fn retain_descendant_failure(&self, result: &mut Result<()>) {
        let Some(failure) = self.descendants.first_unexpected_failure() else {
            return;
        };
        match result {
            Ok(()) => *result = Err(Error::Runtime(failure)),
            Err(Error::Runtime(RuntimeError::ShutdownTimeout { timeout })) => {
                *result = Err(Error::Runtime(
                    RuntimeError::ShutdownTimeoutAfterTaskError {
                        timeout: *timeout,
                        source: Box::new(failure),
                    },
                ));
            }
            Err(_) => {}
        }
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
        self.registry_selection =
            Some(RegistrySelection::direct(self.registry_selection, registry));
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
        self.registry_selection = Some(RegistrySelection::catalog(
            self.registry_selection,
            catalog.borrow().to_registry(),
        ));
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
    ///
    /// Use this only when another compatible promoter covers every job type
    /// that can receive intents, or when the application never records intents.
    /// Disabling all applicable promoters leaves accepted intents pending
    /// indefinitely.
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
        Ok(self.prepare()?.start())
    }

    /// Validate and own the native configuration without starting tasks or database work.
    /// The returned value can be transferred to a lifecycle adapter before launch.
    /// Dropping it releases only configuration and cloned handles.
    ///
    /// Returns the same configuration and registry errors as [`Self::build`].
    pub fn prepare(mut self) -> std::result::Result<PreparedSupervisor, RuntimeError> {
        self.config
            .validate()
            .map_err(|source| RuntimeError::InvalidJobsConfig { source })?;
        let intent_promoter_config = self
            .intent_promoter_config
            .take()
            .unwrap_or_else(|| IntentPromoterConfig::from_jobs_config(&self.config));
        if self.intent_promoter_enabled {
            intent_promoter_config
                .validate()
                .map_err(|source| RuntimeError::InvalidJobsConfig { source })?;
        }
        let registry = match self.registry_selection.take() {
            Some(RegistrySelection::Direct(registry) | RegistrySelection::Catalog(registry)) => {
                registry
            }
            Some(RegistrySelection::Mixed) => return Err(RuntimeError::MixedRegistrySources),
            None if self.worker_enabled || self.reaper_enabled => {
                return Err(RuntimeError::MissingRegistry {
                    worker_enabled: self.worker_enabled,
                    reaper_enabled: self.reaper_enabled,
                });
            }
            None => JobRegistry::new(),
        };
        Ok(PreparedSupervisor {
            pool: self.pool.clone(),
            runtime: self.runtime,
            config: self.config,
            registry,
            observers: self.observers,
            worker_enabled: self.worker_enabled,
            intent_promoter_enabled: self.intent_promoter_enabled,
            intent_promoter_config,
            scheduler_enabled: self.scheduler_enabled,
            reaper_enabled: self.reaper_enabled,
        })
    }
}

impl SupervisorShutdown {
    /// Requests graceful shutdown of all loops watched by the supervisor.
    pub fn request_shutdown(&self) {
        self.handle.request();
    }

    /// Request shutdown using an enclosing owner's already-started stop clock.
    /// Returns the earliest known native/enclosing timestamp so the enclosing
    /// owner can tighten its own deadline after a previously recorded native stop.
    /// The first native cause remains authoritative. Earlier enclosing timestamps
    /// tighten active phase deadlines in [`Supervisor::run_until_shutdown_report`];
    /// repeated later requests cannot restart that allowance. The legacy Result
    /// methods retain their separate timeout measured from driver observation;
    /// an enclosing deadline owner must use the complete-report driver.
    /// Future timestamps are clamped to the current time.
    /// This lets an adapter include scheduling delay in one parent allowance.
    ///
    /// ```no_run
    /// # async fn stop(handle: runledger_runtime::SupervisorShutdown) {
    /// let started = tokio::time::Instant::now();
    /// handle.request_shutdown_since(started);
    /// handle.requested().await;
    /// # }
    /// ```
    pub fn request_shutdown_since(&self, started: tokio::time::Instant) -> tokio::time::Instant {
        self.handle.request_since(started)
    }

    /// Observe the first native stop request independently of full settlement.
    /// Cancelling this borrowed waiter does not request or cancel shutdown.
    /// Adapters use it to drain peers promptly after a native loop failure.
    pub async fn requested(&self) {
        self.handle.requested().await;
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

    use async_trait::async_trait;
    use runledger_core::jobs::{JobCompletion, JobContext, JobFailure, JobHandler, JobType};
    use serde_json::Value;
    use sqlx::postgres::PgPoolOptions;
    use tokio::time::timeout;

    use super::*;

    const UNUSED_LAZY_POOL_URL: &str = "postgres://postgres:postgres@127.0.0.1:65535/runledger";

    struct RegistrySelectionHandler(&'static str);

    #[async_trait]
    impl JobHandler for RegistrySelectionHandler {
        fn job_type(&self) -> JobType<'static> {
            JobType::new(self.0)
        }

        async fn execute(
            &self,
            _context: JobContext,
            _payload: Value,
        ) -> std::result::Result<JobCompletion, JobFailure> {
            Ok(JobCompletion::success())
        }
    }

    fn lazy_pool() -> runledger_postgres::DbPool {
        PgPoolOptions::new()
            // The disable-only tests never acquire this pool; this URL is only
            // a valid PgPool value for supervisor wiring assertions.
            .connect_lazy(UNUSED_LAZY_POOL_URL)
            .expect("construct lazy pool")
    }

    pub(super) fn test_config() -> JobsConfig {
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

    fn registry_with(job_type: &'static str) -> JobRegistry {
        let mut registry = JobRegistry::new();
        registry.register(RegistrySelectionHandler(job_type));
        registry
    }

    fn catalog_with(job_type: &'static str) -> JobCatalog {
        JobCatalog::new().handler(RegistrySelectionHandler(job_type))
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
        assert!(builder.registry_selection.is_none());
    }

    #[tokio::test]
    async fn environment_builder_explicitly_configures_intent_promoter() {
        let pool = lazy_pool();
        let builder = Supervisor::builder_from_env(&pool).expect("build supervisor from env");

        assert!(builder.intent_promoter_config.is_some());
    }

    #[tokio::test]
    async fn builder_accepts_registry_for_worker_and_reaper_loops() {
        let pool = lazy_pool();
        let builder = empty_builder(&pool).with_registry(JobRegistry::new());

        assert!(matches!(
            builder.registry_selection,
            Some(RegistrySelection::Direct(_))
        ));
    }

    #[derive(Clone, Copy, Debug)]
    enum SelectionState {
        Unset,
        Direct,
        Catalog,
        Mixed,
    }

    #[derive(Clone, Copy, Debug)]
    enum SelectionInput {
        Direct,
        Catalog,
    }

    #[derive(Clone, Copy, Debug)]
    enum ExpectedSelection {
        Direct,
        Catalog,
        Mixed,
    }

    #[tokio::test]
    async fn registry_selection_transition_table_is_complete() {
        let pool = lazy_pool();
        let cases = [
            (
                SelectionState::Unset,
                SelectionInput::Direct,
                ExpectedSelection::Direct,
            ),
            (
                SelectionState::Unset,
                SelectionInput::Catalog,
                ExpectedSelection::Catalog,
            ),
            (
                SelectionState::Direct,
                SelectionInput::Direct,
                ExpectedSelection::Direct,
            ),
            (
                SelectionState::Direct,
                SelectionInput::Catalog,
                ExpectedSelection::Mixed,
            ),
            (
                SelectionState::Catalog,
                SelectionInput::Direct,
                ExpectedSelection::Mixed,
            ),
            (
                SelectionState::Catalog,
                SelectionInput::Catalog,
                ExpectedSelection::Catalog,
            ),
            (
                SelectionState::Mixed,
                SelectionInput::Direct,
                ExpectedSelection::Mixed,
            ),
            (
                SelectionState::Mixed,
                SelectionInput::Catalog,
                ExpectedSelection::Mixed,
            ),
        ];

        for (state, input, expected) in cases {
            let builder = match state {
                SelectionState::Unset => empty_builder(&pool),
                SelectionState::Direct => {
                    empty_builder(&pool).with_registry(registry_with("jobs.selection.previous"))
                }
                SelectionState::Catalog => {
                    empty_builder(&pool).with_catalog(catalog_with("jobs.selection.previous"))
                }
                SelectionState::Mixed => empty_builder(&pool)
                    .with_registry(registry_with("jobs.selection.previous"))
                    .with_catalog(catalog_with("jobs.selection.mixed")),
            };
            let builder = match input {
                SelectionInput::Direct => {
                    builder.with_registry(registry_with("jobs.selection.current"))
                }
                SelectionInput::Catalog => {
                    builder.with_catalog(catalog_with("jobs.selection.current"))
                }
            };

            match (&builder.registry_selection, expected) {
                (Some(RegistrySelection::Direct(registry)), ExpectedSelection::Direct)
                | (Some(RegistrySelection::Catalog(registry)), ExpectedSelection::Catalog) => {
                    assert_eq!(
                        registry.registered_types(),
                        vec![JobType::new("jobs.selection.current")],
                        "same-source selection should use the latest value for {state:?} + {input:?}"
                    );
                }
                (Some(RegistrySelection::Mixed), ExpectedSelection::Mixed) => {}
                _ => panic!(
                    "unexpected registry selection for transition {state:?} + {input:?}: expected {expected:?}"
                ),
            }
        }
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
    async fn builder_validates_config_before_rejecting_mixed_registry_sources() {
        let pool = lazy_pool();
        let mut invalid_jobs_config = test_config();
        invalid_jobs_config.claim_batch_size = 0;
        let invalid_jobs = Supervisor::builder(&pool, invalid_jobs_config)
            .expect("supervisor builder has runtime")
            .with_registry(JobRegistry::new())
            .with_catalog(JobCatalog::new())
            .build();
        assert!(matches!(
            invalid_jobs,
            Err(RuntimeError::InvalidJobsConfig {
                source: crate::config::JobsConfigValidationError::InvalidClaimBatchSize {
                    actual: 0
                }
            })
        ));

        let invalid_promoter = empty_builder(&pool)
            .with_registry(JobRegistry::new())
            .with_catalog(JobCatalog::new())
            .with_intent_promoter_config(IntentPromoterConfig::new(Duration::ZERO, 1))
            .build();
        assert!(matches!(
            invalid_promoter,
            Err(RuntimeError::InvalidJobsConfig {
                source: crate::config::JobsConfigValidationError::ZeroPollInterval
            })
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
    async fn repeated_shutdown_handle_requests_are_observable_before_join() {
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
        shutdown.request_shutdown();
        supervisor.request_shutdown();

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
    #[tokio::test]
    async fn legacy_methods_never_report_a_descendant_panic_as_success() {
        for method in 0..4 {
            let supervisor = Supervisor::builder(&lazy_pool(), test_config())
                .expect("validated supervisor construction")
                .disable_worker()
                .disable_scheduler()
                .disable_reaper()
                .build()
                .expect("validated supervisor construction");
            let failed = supervisor.descendants.spawn("escaped_job", async {
                panic!("escaped panic");
            });
            let original = failed.await.expect_err("actual descendant panic");
            let result = tokio::time::timeout(Duration::from_secs(1), async move {
                match method {
                    0 => supervisor.join().await,
                    1 => supervisor.shutdown().await,
                    2 => {
                        supervisor
                            .shutdown_with_timeout(Duration::from_secs(1))
                            .await
                    }
                    _ => {
                        supervisor
                            .run_until_shutdown(std::future::pending(), Duration::from_secs(1))
                            .await
                    }
                }
            })
            .await
            .expect("native internal stop must wake the driver");
            let error = result.expect_err("legacy result lost descendant failure");
            let mut cause: &dyn std::error::Error = &error;
            loop {
                if let Some(shared) = cause.downcast_ref::<std::sync::Arc<tokio::task::JoinError>>()
                {
                    assert!(std::sync::Arc::ptr_eq(shared, &original));
                    break;
                }
                if let Some(join) = cause.downcast_ref::<tokio::task::JoinError>() {
                    assert!(std::ptr::eq(join, &*original));
                    break;
                }
                cause = cause
                    .source()
                    .expect("original descendant join must survive");
            }
        }
    }
    #[tokio::test(start_paused = true)]
    async fn internal_descendant_failure_starts_legacy_drain_budget() {
        let mut supervisor = Supervisor::builder(&lazy_pool(), test_config())
            .expect("runtime exists")
            .disable_worker()
            .disable_scheduler()
            .disable_reaper()
            .build()
            .expect("valid supervisor");
        supervisor.tasks.spawn_on(
            &Handle::current(),
            "unresponsive_loop",
            std::future::pending(),
        );
        let original = supervisor
            .descendants
            .spawn("escaped_job", async {
                panic!("escaped panic");
            })
            .await
            .expect_err("actual descendant panic");
        let result = timeout(
            Duration::from_secs(2),
            supervisor.run_until_shutdown(std::future::pending(), Duration::from_millis(10)),
        )
        .await
        .expect("internal failure must begin bounded shutdown");
        let Err(Error::Runtime(RuntimeError::ShutdownTimeoutAfterTaskError { source, .. })) =
            result
        else {
            panic!("timeout must preserve the triggering descendant failure");
        };
        let RuntimeError::DescendantJoin { source, .. } = *source else {
            panic!("original native descendant failure is retained");
        };
        assert!(Arc::ptr_eq(&source, &original));
    }
    #[tokio::test(start_paused = true)]
    async fn legacy_driver_observes_failure_without_a_descendant_waiter() {
        use futures_util::FutureExt;
        for join_only in [true, false] {
            let mut supervisor = Supervisor::builder(&lazy_pool(), test_config())
                .expect("runtime exists")
                .disable_worker()
                .disable_scheduler()
                .disable_reaper()
                .build()
                .expect("valid supervisor");
            let shutdown = supervisor.shutdown.clone();
            let loop_stop = shutdown.clone();
            supervisor
                .tasks
                .spawn_on(&Handle::current(), "waiting_loop", async move {
                    loop_stop.requested().await;
                    crate::RuntimeLoopExit::Shutdown
                });
            let (release, released) = tokio::sync::oneshot::channel();
            let escaped = supervisor.descendants.spawn("escaped_job", async move {
                released.await.expect("fixture releases descendant");
                panic!("unobserved descendant panic");
            });
            let driver = async move {
                if join_only {
                    supervisor.join().await
                } else {
                    supervisor
                        .run_until_shutdown(std::future::pending(), Duration::from_millis(10))
                        .await
                }
            };
            tokio::pin!(driver);
            assert!(driver.as_mut().now_or_never().is_none());
            release.send(()).expect("descendant starts after driver");
            let observed = timeout(Duration::from_millis(100), driver.as_mut()).await;
            // Only after the observation boundary may the fixture harvest this join.
            let original = escaped.await.expect_err("actual descendant panic");
            let (autonomous, result) = match observed {
                Ok(result) => (true, result),
                Err(_) => (false, driver.await),
            };
            assert!(
                autonomous,
                "driver depended on external descendant observation"
            );
            let Err(Error::Runtime(RuntimeError::DescendantJoin { source, .. })) = result else {
                panic!("observed failure must survive legacy completion");
            };
            assert!(Arc::ptr_eq(&source, &original));
        }
    }
    #[tokio::test(start_paused = true)]
    async fn handle_request_applies_the_legacy_driver_shutdown_budget() {
        use futures_util::FutureExt;
        let mut supervisor = Supervisor::builder(&lazy_pool(), test_config())
            .expect("runtime exists")
            .disable_worker()
            .disable_scheduler()
            .disable_reaper()
            .build()
            .expect("valid supervisor");
        let stop = supervisor.shutdown.clone();
        let (entered, entry) = tokio::sync::oneshot::channel();
        supervisor
            .tasks
            .spawn_on(&Handle::current(), "slow_loop", async move {
                entered.send(()).expect("fixture observes loop entry");
                stop.requested().await;
                tokio::time::sleep(Duration::from_secs(30)).await;
                crate::RuntimeLoopExit::Shutdown
            });
        entry.await.expect("loop started");
        let handle = supervisor.shutdown_handle();
        let driver = supervisor.run_until_shutdown(std::future::pending(), Duration::from_secs(1));
        tokio::pin!(driver);
        assert!(driver.as_mut().now_or_never().is_none());
        handle.request_shutdown();
        let result = timeout(Duration::from_secs(3), driver)
            .await
            .expect("handle starts bounded stop");
        assert!(matches!(
            result,
            Err(Error::Runtime(RuntimeError::ShutdownTimeout { .. }))
        ));
    }
}
