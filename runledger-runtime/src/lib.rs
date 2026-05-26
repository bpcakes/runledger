//! Async runtime loops for executing Runledger jobs against a persistence
//! backend.
//!
//! Use this crate to wire the operational pieces around `runledger-core`
//! handlers and `runledger-postgres` storage:
//! - [`Supervisor`] starts and joins the worker, scheduler, and reaper loops
//!   for a typical worker process
//! - [`registry::JobRegistry`] stores the concrete handlers a worker may claim
//! - [`config::JobsConfig`] centralizes poll, lease, and concurrency settings
//!
//! A typical service builds a shared PostgreSQL pool, registers handlers in a
//! [`registry::JobRegistry`], and starts a [`Supervisor`]. Worker processes should
//! call [`Supervisor::run_until_shutdown`] to observe task failures while still
//! applying a bounded shutdown deadline. Use [`Supervisor::shutdown_with_timeout`]
//! when shutdown is signaled externally, or [`Supervisor::shutdown`] when the
//! caller already has an external shutdown budget or knows all loops will exit promptly.
//!
//! The lower-level [`worker::run_worker_loop`], [`scheduler::run_scheduler_loop`],
//! and [`reaper::run_reaper_loop`] functions remain public for custom process
//! orchestration, but [`Supervisor`] is the preferred runtime facade.
//!
//! # Copy-Paste Examples
//!
//! - [Run a worker binary](https://github.com/featherenvy/runledger/blob/main/runledger-runtime/examples/worker_binary.rs)
//! - [Enqueue one job](https://github.com/featherenvy/runledger/blob/main/runledger-postgres/examples/enqueue_job.rs)
//! - [Enqueue a workflow DAG](https://github.com/featherenvy/runledger/blob/main/runledger-postgres/examples/workflow_dag.rs)
//! - [Use an external workflow gate](https://github.com/featherenvy/runledger/blob/main/runledger-postgres/examples/external_gate.rs)
//! - [Create a scheduled job entrypoint](https://github.com/featherenvy/runledger/blob/main/runledger-postgres/examples/schedule_job.rs)
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
//! # async fn demo(pool: runledger_postgres::DbPool) -> runledger_runtime::Result<()> {
//! use std::time::Duration;
//!
//! use runledger_runtime::prelude::*;
//!
//! let registry = JobRegistry::new();
//! let supervisor = Supervisor::builder(&pool, JobsConfig::from_env())?
//!     .with_registry(registry)
//!     .build()?;
//!
//! supervisor
//!     .run_until_shutdown(std::future::pending::<()>(), Duration::from_secs(30))
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! Use [`Supervisor::run_until_shutdown`] for ordinary worker binaries so the
//! process observes internal runtime task failures while still applying a
//! bounded shutdown deadline. Use the lower-level loop functions only for custom
//! process orchestration.

pub mod config;
pub mod error;
pub mod reaper;
pub mod registry;
pub mod scheduler;
pub mod supervisor;
pub mod worker;

pub use error::{Error, ReaperError, Result, RuntimeError, SchedulerError, WorkerError};
pub use supervisor::{Supervisor, SupervisorBuilder, SupervisorShutdown};

/// Common `runledger-runtime` imports for worker-process integration.
///
/// This prelude avoids generic `Result` or `Error` aliases so it can be
/// glob-imported alongside the core and PostgreSQL preludes.
pub mod prelude {
    pub use crate::config::JobsConfig;
    pub use crate::error::{ReaperError, RuntimeError, SchedulerError, WorkerError};
    pub use crate::registry::JobRegistry;
    pub use crate::{RuntimeLoopExit, Supervisor, SupervisorBuilder, SupervisorShutdown};
}

/// Reason a low-level runtime loop exited.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeLoopExit {
    /// The loop observed a shutdown request or a closed shutdown channel.
    Shutdown,
    /// The loop completed without observing shutdown. Supervisors treat this as
    /// an unexpected task exit.
    Completed,
}

#[cfg(test)]
#[path = "../test_support.rs"]
pub(crate) mod test_support;
