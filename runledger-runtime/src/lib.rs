//! Async runtime loops for executing Runledger jobs against a persistence
//! backend.
//!
//! Use this crate to wire the operational pieces around `runledger-core`
//! handlers and `runledger-postgres` storage:
//! - [`Supervisor`] starts, runs and settles the worker, intent promoter,
//!   scheduler, and reaper loops for a typical worker process
//! - [`Supervisor::run_until_shutdown_report`] and [`Supervisor::shutdown_report`]
//!   are its only terminal methods; both return a [`RuntimeShutdownDriver`] that
//!   awaits to a [`RuntimeShutdownReport`] carrying the evidence needed before
//!   deciding whether dependency cleanup can run
//! - [`catalog::JobCatalog`] is the preferred startup API for handler
//!   registration, definition sync, and catalog-validated enqueue helpers
//! - [`registry::JobRegistry`] stores concrete handlers directly for advanced
//!   setups that manage definitions separately
//! - [`config::JobsConfig`] centralizes poll, lease, and concurrency settings
//! - [`observer::JobLifecycleObserver`] receives best-effort post-commit
//!   running, success, continuation, failure, lease-loss, and reaper outcomes
//!
//! A typical service builds a shared PostgreSQL pool, registers handlers in a
//! [`catalog::JobCatalog`], syncs definitions during startup, and starts a
//! [`Supervisor`] with [`SupervisorBuilder::with_catalog`]. It then ends that
//! supervisor with [`Supervisor::run_until_shutdown_report`] (wait for an
//! external signal) or [`Supervisor::shutdown_report`] (stop now), both taking a
//! [`RuntimeShutdownBudget`] and returning an independently owned
//! [`RuntimeShutdownDriver`] that awaits to a [`RuntimeShutdownReport`].
//! Consume the report with [`RuntimeShutdownReport::classify`] and match
//! [`RuntimeSettlement`]: `Clean` permits cleanup and successful process exit;
//! `StoppedWithFailures` permits cleanup but retains a process failure;
//! `Unsettled` cannot authorize cleanup. Only the first two payloads yield a
//! [`RuntimeShutdownCleanupPermit`] for your dependency-release adapter.
//! An adapter transfers
//! [`SupervisorBuilder::prepare`]'s inert value before launch.
//! Dropping the driver requests stop without cancelling its independent owner.
//! Keep the captured Tokio runtime alive and driven; owner destruction yields
//! [`RuntimeShutdownSettlement::Interrupted`], which never permits cleanup.
//! A [`RuntimeShutdownSignal`] runs as a tracked descendant on that captured
//! runtime. Returned errors and panic joins remain explicit failures without
//! unwinding native settlement. Every unobserved report emits one redacted diagnostic.
//!
//! The lower-level [`worker::run_worker_loop`],
//! [`intent_promoter::run_intent_promoter_loop`],
//! [`scheduler::run_scheduler_loop`], and [`reaper::run_reaper_loop`] functions
//! remain public for custom process orchestration, but [`Supervisor`] is the
//! preferred runtime facade. Custom orchestration that uses durable enqueue
//! intents must run both the worker and intent promoter loops.
//!
//! # Copy-Paste Examples
//!
//! - [Run a worker binary](https://github.com/bpcakes/runledger/blob/master/runledger-runtime/examples/worker_binary.rs)
//! - [Enqueue one job](https://github.com/bpcakes/runledger/blob/master/runledger-postgres/examples/enqueue_job.rs)
//! - [Enqueue a workflow DAG](https://github.com/bpcakes/runledger/blob/master/runledger-postgres/examples/workflow_dag.rs)
//! - [Use an external workflow gate](https://github.com/bpcakes/runledger/blob/master/runledger-postgres/examples/external_gate.rs)
//! - [Create a scheduled job entrypoint](https://github.com/bpcakes/runledger/blob/master/runledger-postgres/examples/schedule_job.rs)
//! - [Adopt continuation, retry timing, coordination, and recovery](https://github.com/bpcakes/runledger/blob/master/docs/downstream-agent-guide.md)
//!
//! # Prelude
//!
//! ```rust
//! use runledger_runtime::prelude::*;
//! ```
//!
//! The runtime prelude exports the worker-process facade and configuration
//! types. Import `runledger_core::prelude::*` for handler contracts and
//! `runledger_postgres::prelude::*` for persistence APIs.
//!
//! # Run A Worker Process
//!
//! ```rust,no_run
//! # async fn demo(
//! #     pool: runledger_postgres::DbPool,
//! # ) -> std::result::Result<(), Box<dyn std::error::Error>> {
//! use std::time::Duration;
//!
//! use runledger_core::prelude::*;
//! use runledger_runtime::prelude::*;
//!
//! struct MyHandler;
//! # #[async_trait::async_trait]
//! # impl JobHandler for MyHandler {
//! #     fn job_type(&self) -> JobType<'static> { JobType::new("jobs.example") }
//! #     async fn execute(
//! #         &self,
//! #         _context: JobContext,
//! #         _payload: serde_json::Value,
//! #     ) -> std::result::Result<JobCompletion, JobFailure> { Ok(JobCompletion::success()) }
//! # }
//!
//! let catalog = JobCatalog::new().handler(MyHandler);
//! catalog.sync_definitions(&pool).await?;
//! async fn close_accounted_pool(
//!     _permit: RuntimeShutdownCleanupPermit,
//!     pool: &runledger_postgres::DbPool,
//! ) {
//!     pool.close().await;
//! }
//!
//! let supervisor = Supervisor::builder_from_env(&pool)?
//!     .with_catalog(&catalog)
//!     .build()?;
//!
//! let budget = RuntimeShutdownBudget::new(Duration::from_secs(30), Duration::from_secs(5))?;
//! let report = supervisor
//!     .run_until_shutdown_report(RuntimeShutdownSignal::ctrl_c(), budget)
//!     .await;
//!
//! match report.classify() {
//!     RuntimeSettlement::Clean(clean) => {
//!         close_accounted_pool(clean.into_cleanup_permit(), &pool).await;
//!     }
//!     RuntimeSettlement::StoppedWithFailures(stopped) => {
//!         let (permit, failure) = stopped.into_parts();
//!         close_accounted_pool(permit, &pool).await;
//!         return Err(failure.into());
//!     }
//!     RuntimeSettlement::Unsettled(unsettled) => {
//!         return Err(unsettled.into_failure().into());
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! The budget's graceful and abort allowances share one stop clock that starts at
//! the first stop request, whether that came from the signal, a
//! [`SupervisorShutdown`] handle, a failing loop or a failing descendant. Use the
//! lower-level loop functions only for custom process orchestration.

mod callback;
pub mod catalog;
pub mod config;
mod dead_letter_hook;
pub mod error;
pub mod intent_promoter;
pub mod observer;
mod panic_payload;
pub mod reaper;
pub mod registry;
pub mod scheduler;
mod settlement;
mod shutdown;
mod shutdown_signal;
mod startup;
pub mod supervisor;
mod task_group;
pub mod worker;

pub use error::{Error, ReaperError, Result, RuntimeError, SchedulerError, WorkerError};
pub use observer::{
    JobCompletionPersistFailedEvent, JobCompletionPersistenceOperation, JobContinuedEvent,
    JobFailedEvent, JobFailureDisposition, JobLeaseLostEvent, JobLeaseReapedDisposition,
    JobLeaseReapedEvent, JobLifecycleObserver, JobLifecycleObservers, JobRunningEvent,
    JobSucceededEvent, ObservedJob,
};
pub use settlement::{
    RuntimeCallbackFailure, RuntimeCleanSettlement, RuntimeLoopRecord, RuntimeSettlement,
    RuntimeShutdownBudget, RuntimeShutdownCause, RuntimeShutdownCleanupPermit,
    RuntimeShutdownFailure, RuntimeShutdownReport, RuntimeShutdownSettlement,
    RuntimeStoppedWithFailures, RuntimeTaskRecord, RuntimeUnsettled, UnjoinedRuntimeTasks,
    UnsettledRuntimeTask,
};
pub use shutdown_signal::{
    RuntimeShutdownSignal, RuntimeShutdownSignalError, RuntimeShutdownSignalPanic,
};
pub use startup::{RuntimeStartup, RuntimeStartupObserver, RuntimeStartupStopped};
pub use supervisor::{
    PreparedSupervisor, RuntimeShutdownDriver, Supervisor, SupervisorBuilder, SupervisorShutdown,
};

/// Common `runledger-runtime` imports for worker-process integration.
///
/// This prelude avoids generic `Result` or `Error` aliases so it can be
/// glob-imported alongside the core and PostgreSQL preludes.
pub mod prelude {
    pub use crate::catalog::{
        CatalogError, CatalogJobEnqueueInput, CatalogJobScheduleInput, CatalogJobScheduleSpec,
        CatalogWorkflowDagBuilder, JobCatalog, JobCatalogDefaults, JobCatalogDefinitionOverrides,
        JobCatalogExactSyncReport, JobCatalogScheduleSyncReport, JobCatalogScheduleSyncScope,
        JobCatalogSyncReport, JobCatalogSyncScope,
    };
    pub use crate::config::{IntentPromoterConfig, JobsConfig};
    pub use crate::error::{ReaperError, RuntimeError, SchedulerError, WorkerError};
    pub use crate::observer::{
        JobCompletionPersistFailedEvent, JobCompletionPersistenceOperation, JobContinuedEvent,
        JobFailedEvent, JobFailureDisposition, JobLeaseLostEvent, JobLeaseReapedDisposition,
        JobLeaseReapedEvent, JobLifecycleObserver, JobLifecycleObservers, JobRunningEvent,
        JobSucceededEvent, ObservedJob,
    };
    pub use crate::registry::JobRegistry;
    pub use crate::{
        PreparedSupervisor, RuntimeLoopExit, RuntimeShutdownDriver, RuntimeStartup,
        RuntimeStartupObserver, RuntimeStartupStopped, Supervisor, SupervisorBuilder,
        SupervisorShutdown,
    };
    pub use crate::{
        RuntimeCleanSettlement, RuntimeSettlement, RuntimeShutdownBudget, RuntimeShutdownCause,
        RuntimeShutdownCleanupPermit, RuntimeShutdownFailure, RuntimeShutdownReport,
        RuntimeShutdownSettlement, RuntimeShutdownSignal, RuntimeShutdownSignalError,
        RuntimeShutdownSignalPanic, RuntimeStoppedWithFailures, RuntimeUnsettled,
    };
}

/// Reason a low-level runtime loop exited.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RuntimeLoopExit {
    /// The loop observed a shutdown request or a closed shutdown channel.
    Shutdown,
    /// The loop rejected invalid runtime-loop configuration before polling.
    InvalidConfig(config::JobsConfigValidationError),
    /// The loop completed without observing shutdown. Supervisors treat this as
    /// an unexpected task exit.
    Completed,
}
