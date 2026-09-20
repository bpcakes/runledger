use std::borrow::Borrow;
use std::sync::Arc;

use tokio::runtime::Handle;
use tracing::warn;

use crate::RuntimeError;
use crate::catalog::JobCatalog;
use crate::config::{IntentPromoterConfig, JobsConfig};
use crate::observer::JobLifecycleObserver;
use crate::registry::JobRegistry;
use crate::shutdown::{ShutdownHandle, ShutdownSignal};
use crate::task_group::TaskGroup;

#[path = "supervisor/preparation.rs"]
mod preparation;
pub use preparation::PreparedSupervisor;

mod driver;
pub use driver::RuntimeShutdownDriver;

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
/// handles selected by [`SupervisorBuilder`]. It has exactly two terminal
/// methods, and both return an independently owned [`RuntimeShutdownDriver`]
/// that awaits to a [`RuntimeShutdownReport`] rather than a bare success:
/// [`Self::run_until_shutdown_report`] waits for an external signal, and
/// [`Self::shutdown_report`] stops immediately. There is deliberately no terminal
/// method whose success value can be mistaken for proof that everything settled.
///
/// Dropping a supervisor requests shutdown and detaches the task handles, which
/// produces no report at all. A process that needs to observe panics, unexpected
/// task exits or descendant settlement must drive one of the two terminal
/// methods to completion.
///
/// [`RuntimeShutdownReport`]: crate::RuntimeShutdownReport
#[must_use]
pub struct Supervisor {
    runtime: Handle,
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
    /// use runledger_runtime::RuntimeShutdownBudget;
    /// use std::time::Duration;
    /// let startup = supervisor.startup_observer();
    /// match startup.wait_initialized().await {
    ///     Ok(()) => { /* apply application policy */ }
    ///     Err(_) => { /* startup stopped; observe the native shutdown report */ }
    /// }
    /// let budget = RuntimeShutdownBudget::new(Duration::from_secs(5), Duration::from_secs(1))
    ///     .expect("representable budget");
    /// let report = supervisor.shutdown_report(budget).await;
    /// assert!(matches!(
    ///     report.classify(),
    ///     runledger_runtime::RuntimeSettlement::Clean(_)
    /// ));
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

    /// Runs the supervised loops until stopped, then settles them within `budget`
    /// and reports what was actually observed.
    ///
    /// This and [`Self::shutdown_report`] are the only two ways to end a
    /// supervisor, and both hand back a report rather than a bare success,
    /// because only a report can answer the question every caller actually has:
    /// did everything this runtime owned finish, and may its dependencies now be
    /// released? Consume [`RuntimeShutdownReport::classify`] and match its three
    /// outcomes. `Clean` permits cleanup and successful process exit;
    /// `StoppedWithFailures` permits cleanup but carries a process failure;
    /// `Unsettled` cannot authorize dependency cleanup. Historical callback
    /// interruptions remain `Unsettled` even after all tracked tasks have joined.
    ///
    /// Stopping begins at whichever comes first: `shutdown` resolving, a
    /// [`SupervisorShutdown`] handle requesting it, a supervised loop failing, or
    /// a native descendant failing, or the returned waiter being dropped.
    /// All start the same non-resetting stop
    /// clock, and `budget` is measured from it — a later request cannot extend
    /// an allowance that is already running. Use
    /// [`Self::shutdown_report`] when the caller owns stop timing and has no
    /// external signal to wait on.
    ///
    /// Calling this method synchronously transfers settlement to an independent
    /// task on the Tokio runtime captured when this supervisor was prepared.
    /// The caller does not need to be inside a Tokio runtime. Cancelling or
    /// dropping the returned waiter requests stop without cancelling the owner.
    /// Await the waiter to make the cleanup decision; if it is dropped, bounded
    /// settlement continues and its unobserved report emits a redacted diagnostic.
    /// The captured runtime must remain alive and driven (including a
    /// current-thread runtime). A Tokio handle does not keep it alive. If the
    /// owner is destroyed before finishing, the report is explicitly interrupted
    /// and cannot authorize cleanup.
    ///
    /// Use [`crate::RuntimeShutdownSignal::ctrl_c`] or an explicit fallible or
    /// infallible custom signal. Its owned `Send + 'static` future runs as the
    /// tracked `shutdown_signal` descendant on the captured runtime. Returned
    /// errors are retained by [`crate::RuntimeShutdownReport::signal_error`].
    /// On Unix, a custom captured runtime used with `ctrl_c` must enable I/O or
    /// all drivers so Tokio's signal driver is available. Missing driver support
    /// is retained as signal-panic and descendant-join evidence.
    /// poll/drop panics appear as descendant join failures. Neither interrupts
    /// native settlement. A normally joined error denies success but permits
    /// cleanup. If another cause starts shutdown and the budget cancels a
    /// library-authored Ctrl-C or pending listener, its completed cancellation
    /// join is accounted because it proves guarded destruction finished. A
    /// force-aborted custom listener retains its cancellation failure and denies
    /// cleanup, as does a panic or unjoined signal. A non-triggering listener must
    /// retire within the graceful allowance; zero grace immediately escalates it.
    /// The exact initiating signal is not aborted by its own request and may use
    /// the remaining total allowance to finish destruction and join. Keep any other runtime whose
    /// I/O the signal awaits alive, and make custom signals cancellation-safe.
    ///
    /// ```rust,no_run
    /// # async fn example(supervisor: runledger_runtime::Supervisor) -> Result<(), Box<dyn std::error::Error>> {
    /// use runledger_runtime::{RuntimeSettlement, RuntimeShutdownBudget, RuntimeShutdownSignal};
    /// use std::time::Duration;
    /// async fn release_dependencies(
    ///     _permit: runledger_runtime::RuntimeShutdownCleanupPermit,
    /// ) {
    ///     // Close pools and other shared dependencies here.
    /// }
    ///
    /// let budget = RuntimeShutdownBudget::new(Duration::from_secs(20), Duration::from_secs(1))?;
    /// let report = supervisor
    ///     .run_until_shutdown_report(RuntimeShutdownSignal::ctrl_c(), budget)
    ///     .await;
    /// match report.classify() {
    ///     RuntimeSettlement::Clean(clean) => {
    ///         release_dependencies(clean.into_cleanup_permit()).await;
    ///     }
    ///     RuntimeSettlement::StoppedWithFailures(stopped) => {
    ///         let (permit, failure) = stopped.into_parts();
    ///         release_dependencies(permit).await;
    ///         return Err(failure.into());
    ///     }
    ///     RuntimeSettlement::Unsettled(unsettled) => {
    ///         return Err(unsettled.into_failure().into());
    ///     }
    /// }
    /// # Ok(()) }
    /// ```
    ///
    /// [`RuntimeShutdownReport::classify`]: crate::RuntimeShutdownReport::classify
    pub fn run_until_shutdown_report(
        self,
        shutdown: crate::RuntimeShutdownSignal,
        budget: crate::RuntimeShutdownBudget,
    ) -> RuntimeShutdownDriver {
        RuntimeShutdownDriver::start(self, Some(shutdown), budget)
    }

    /// Requests shutdown immediately, then settles within `budget` and reports
    /// what was actually observed.
    ///
    /// This is [`Self::run_until_shutdown_report`] for callers that already know
    /// it is time to stop and have no external signal to wait on. It carries the
    /// identical report contract, including that
    /// [`RuntimeShutdownReport::classify`] supplies the only payloads that can
    /// authorize releasing dependencies.
    ///
    /// A loop or descendant that had already failed still sets the shutdown
    /// cause and is retained in the report. The stop clock starts before this
    /// method returns, so scheduling delay consumes the same graceful allowance.
    ///
    /// ```rust,no_run
    /// # async fn example(supervisor: runledger_runtime::Supervisor) -> Result<(), Box<dyn std::error::Error>> {
    /// use runledger_runtime::{RuntimeSettlement, RuntimeShutdownBudget};
    /// use std::time::Duration;
    /// async fn release_dependencies(
    ///     _permit: runledger_runtime::RuntimeShutdownCleanupPermit,
    /// ) {
    ///     // Close pools and other shared dependencies here.
    /// }
    ///
    /// let budget = RuntimeShutdownBudget::new(Duration::from_secs(5), Duration::from_secs(1))?;
    /// let report = supervisor.shutdown_report(budget).await;
    /// match report.classify() {
    ///     RuntimeSettlement::Clean(clean) => {
    ///         release_dependencies(clean.into_cleanup_permit()).await;
    ///     }
    ///     RuntimeSettlement::StoppedWithFailures(stopped) => {
    ///         let (permit, failure) = stopped.into_parts();
    ///         release_dependencies(permit).await;
    ///         return Err(failure.into());
    ///     }
    ///     RuntimeSettlement::Unsettled(unsettled) => {
    ///         return Err(unsettled.into_failure().into());
    ///     }
    /// }
    /// # Ok(()) }
    /// ```
    ///
    /// [`RuntimeShutdownReport::classify`]: crate::RuntimeShutdownReport::classify
    pub fn shutdown_report(
        mut self,
        budget: crate::RuntimeShutdownBudget,
    ) -> RuntimeShutdownDriver {
        self.tasks
            .begin_immediate_shutdown(&self.shutdown, &self.descendants);
        RuntimeShutdownDriver::start(self, None, budget)
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
    /// repeated later requests cannot restart that allowance.
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

    pub(super) fn test_budget() -> crate::RuntimeShutdownBudget {
        crate::RuntimeShutdownBudget::new(Duration::from_secs(1), Duration::from_secs(1))
            .expect("valid test budget")
    }

    pub(super) fn disabled_supervisor() -> Supervisor {
        Supervisor::builder(&lazy_pool(), test_config())
            .expect("supervisor builder has runtime")
            .disable_worker()
            .disable_scheduler()
            .disable_reaper()
            .build()
            .expect("all-disabled supervisor should build")
    }

    struct NotifyOnDrop(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for NotifyOnDrop {
        fn drop(&mut self) {
            if let Some(notify) = self.0.take() {
                let _ = notify.send(());
            }
        }
    }

    pub(super) fn cancellation_budget() -> crate::RuntimeShutdownBudget {
        crate::RuntimeShutdownBudget::new(Duration::from_millis(25), Duration::from_millis(250))
            .expect("valid cancellation test budget")
    }

    #[tokio::test]
    async fn dropping_terminal_waiter_before_poll_does_not_abandon_settlement() {
        let mut supervisor = disabled_supervisor();
        let (dropped, observe_drop) = tokio::sync::oneshot::channel();
        supervisor
            .tasks
            .spawn_on(&Handle::current(), "drop_probe", async move {
                let _notify = NotifyOnDrop(Some(dropped));
                std::future::pending::<crate::RuntimeLoopExit>().await
            });

        let waiter = supervisor.shutdown_report(cancellation_budget());
        drop(waiter);

        timeout(Duration::from_secs(1), observe_drop)
            .await
            .expect("independent settlement remains bounded")
            .expect("aborting the native task drops its future");
    }

    #[tokio::test]
    async fn cancelling_terminal_waiter_during_settlement_does_not_abandon_it() {
        let mut supervisor = disabled_supervisor();
        let shutdown = supervisor.shutdown.clone();
        let (settling, observe_settling) = tokio::sync::oneshot::channel();
        let (dropped, observe_drop) = tokio::sync::oneshot::channel();
        supervisor
            .tasks
            .spawn_on(&Handle::current(), "drop_probe", async move {
                let _notify = NotifyOnDrop(Some(dropped));
                shutdown.requested().await;
                settling
                    .send(())
                    .expect("test observes graceful settlement");
                std::future::pending::<crate::RuntimeLoopExit>().await
            });

        let waiter = tokio::spawn(supervisor.shutdown_report(cancellation_budget()));
        observe_settling
            .await
            .expect("native task observes the stop request");
        waiter.abort();
        let join = waiter.await.expect_err("waiter cancellation is observed");
        assert!(join.is_cancelled());

        timeout(Duration::from_secs(1), observe_drop)
            .await
            .expect("settlement continues after waiter cancellation")
            .expect("abort escalation drops the native task future");
    }

    #[tokio::test]
    async fn an_all_disabled_supervisor_settles_through_both_terminal_methods() {
        let report = disabled_supervisor().shutdown_report(test_budget()).await;
        assert!(report.is_success());
        assert!(report.failure().is_none());

        let report = disabled_supervisor()
            .run_until_shutdown_report(
                crate::RuntimeShutdownSignal::infallible(async {}),
                test_budget(),
            )
            .await;
        assert!(report.is_success());
    }

    #[tokio::test]
    async fn shutdown_report_requests_shutdown_before_returning_its_driver() {
        let supervisor = disabled_supervisor();
        let shutdown = supervisor.shutdown_handle();

        let driver = supervisor.shutdown_report(test_budget());

        assert!(shutdown.is_shutdown_requested());
        assert!(driver.await.is_success());
    }

    #[tokio::test]
    async fn shutdown_report_preserves_an_already_failed_loop_cause() {
        let mut supervisor = disabled_supervisor();
        supervisor
            .tasks
            .spawn_on(&Handle::current(), "already_failed", async {
                crate::RuntimeLoopExit::Completed
            });
        supervisor.tasks.wait_until_finished_for_tests().await;
        let shutdown = supervisor.shutdown.clone();

        let driver = supervisor.shutdown_report(test_budget());
        assert_eq!(
            shutdown.cause(),
            crate::RuntimeShutdownCause::LoopFailure("already_failed")
        );
        let report = driver.await;
        assert_eq!(
            report.cause(),
            crate::RuntimeShutdownCause::LoopFailure("already_failed")
        );
        assert!(matches!(
            report.failure(),
            Some(crate::RuntimeShutdownFailure::LoopExitedUnexpectedly {
                task: "already_failed"
            })
        ));
    }

    #[tokio::test]
    async fn shutdown_report_preserves_an_unobserved_finished_descendant_cause() {
        let supervisor = disabled_supervisor();
        let failed = supervisor.descendants.spawn("already_failed", async {
            panic!("descendant failed before explicit shutdown");
        });
        while !failed.is_finished() {
            tokio::task::yield_now().await;
        }
        let shutdown = supervisor.shutdown.clone();

        let driver = supervisor.shutdown_report(test_budget());

        assert!(matches!(
            shutdown.cause(),
            crate::RuntimeShutdownCause::DescendantFailure {
                task: "already_failed",
                ..
            }
        ));
        let report = driver.await;
        let original = failed.await.expect_err("actual descendant panic");
        let Some(crate::RuntimeShutdownFailure::DescendantJoin { source, .. }) = report.failure()
        else {
            panic!("the already-finished descendant remains the primary failure");
        };
        assert!(Arc::ptr_eq(&source, &original));
    }

    #[tokio::test]
    async fn repeated_shutdown_handle_requests_are_observable_before_settlement() {
        let supervisor = disabled_supervisor();
        let shutdown = supervisor.shutdown_handle();
        let cloned_shutdown = shutdown.clone();

        cloned_shutdown.request_shutdown();
        shutdown.request_shutdown();
        supervisor.request_shutdown();

        assert!(shutdown.is_shutdown_requested());
        assert!(supervisor.is_shutdown_requested());
        let report = supervisor
            .run_until_shutdown_report(crate::RuntimeShutdownSignal::pending(), test_budget())
            .await;
        assert!(report.is_success());
    }

    #[tokio::test]
    async fn a_supervisor_with_no_tasks_still_waits_for_its_signal() {
        let supervisor = disabled_supervisor();
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        let mut run = tokio::spawn(supervisor.run_until_shutdown_report(
            crate::RuntimeShutdownSignal::infallible(async move {
                signal_rx.await.expect("shutdown signal should be sent");
            }),
            test_budget(),
        ));

        assert!(
            timeout(Duration::from_millis(50), &mut run).await.is_err(),
            "all-disabled supervisor should wait for the shutdown signal"
        );

        signal_tx.send(()).expect("signal receiver should be alive");
        let report = run.await.expect("report driver should join");
        assert!(report.is_success());
    }

    #[tokio::test]
    async fn neither_terminal_method_reports_a_descendant_panic_as_success() {
        for stop_now in [true, false] {
            let supervisor = disabled_supervisor();
            let failed = supervisor.descendants.spawn("escaped_job", async {
                panic!("escaped panic");
            });
            let original = failed.await.expect_err("actual descendant panic");
            let report = timeout(Duration::from_secs(1), async move {
                if stop_now {
                    supervisor.shutdown_report(test_budget()).await
                } else {
                    supervisor
                        .run_until_shutdown_report(
                            crate::RuntimeShutdownSignal::pending(),
                            test_budget(),
                        )
                        .await
                }
            })
            .await
            .expect("native internal stop must wake the driver");

            assert!(!report.is_success());
            assert!(!report.is_cooperatively_stopped());
            let Some(crate::RuntimeShutdownFailure::DescendantJoin { source, .. }) =
                report.failure()
            else {
                panic!("the descendant panic must survive settlement");
            };
            assert!(Arc::ptr_eq(&source, &original));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_internal_descendant_failure_starts_the_shutdown_budget() {
        let mut supervisor = disabled_supervisor();
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
        let budget =
            crate::RuntimeShutdownBudget::new(Duration::from_millis(10), Duration::from_millis(10))
                .expect("valid budget");
        let report = timeout(
            Duration::from_secs(2),
            supervisor.run_until_shutdown_report(crate::RuntimeShutdownSignal::pending(), budget),
        )
        .await
        .expect("internal failure must begin bounded shutdown");

        assert!(matches!(
            report.cause(),
            crate::RuntimeShutdownCause::DescendantFailure {
                task: "escaped_job",
                ..
            }
        ));
        assert!(report.graceful_timed_out());
        // The triggering descendant failure outranks the abort it caused.
        let Some(crate::RuntimeShutdownFailure::DescendantJoin { source, .. }) = report.failure()
        else {
            panic!("the triggering descendant failure is retained");
        };
        assert!(Arc::ptr_eq(&source, &original));
    }

    #[tokio::test(start_paused = true)]
    async fn the_driver_observes_a_descendant_failure_without_an_external_waiter() {
        use futures_util::FutureExt;
        let mut supervisor = disabled_supervisor();
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
        let driver = supervisor
            .run_until_shutdown_report(crate::RuntimeShutdownSignal::pending(), test_budget());
        tokio::pin!(driver);
        assert!(driver.as_mut().now_or_never().is_none());
        release.send(()).expect("descendant starts after driver");
        let observed = timeout(Duration::from_millis(100), driver.as_mut()).await;
        // Only after the observation boundary may the fixture harvest this join.
        let original = escaped.await.expect_err("actual descendant panic");
        let (autonomous, report) = match observed {
            Ok(report) => (true, report),
            Err(_) => (false, driver.await),
        };
        assert!(
            autonomous,
            "driver depended on external descendant observation"
        );
        let Some(crate::RuntimeShutdownFailure::DescendantJoin { source, .. }) = report.failure()
        else {
            panic!("observed failure must survive settlement");
        };
        assert!(Arc::ptr_eq(&source, &original));
    }

    #[tokio::test(start_paused = true)]
    async fn a_handle_request_applies_the_shutdown_budget() {
        use futures_util::FutureExt;
        let mut supervisor = disabled_supervisor();
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
        let driver = supervisor
            .run_until_shutdown_report(crate::RuntimeShutdownSignal::pending(), test_budget());
        tokio::pin!(driver);
        assert!(driver.as_mut().now_or_never().is_none());
        handle.request_shutdown();
        let report = timeout(Duration::from_secs(5), driver)
            .await
            .expect("handle starts bounded stop");
        assert!(report.graceful_timed_out());
        assert!(!report.is_cooperatively_stopped());
    }
}
