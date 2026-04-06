//! Async runtime loops for executing Runledger jobs against a persistence
//! backend.
//!
//! Use this crate to wire the operational pieces around `runledger-core`
//! handlers and `runledger-postgres` storage:
//! - [`registry::JobRegistry`] stores the concrete handlers a worker may claim
//! - [`worker::run_worker_loop`] claims jobs, executes handlers, and records
//!   progress/results
//! - [`scheduler::run_scheduler_loop`] materializes due cron schedules into
//!   runnable jobs
//! - [`reaper::run_reaper_loop`] recovers jobs whose worker lease expired
//! - [`config::JobsConfig`] centralizes poll, lease, and concurrency settings
//!
//! A typical service builds a shared PostgreSQL pool, registers handlers in a
//! [`registry::JobRegistry`], and starts the worker, scheduler, and reaper
//! loops with coordinated shutdown signals.

pub mod config;
pub mod error;
pub mod reaper;
pub mod registry;
pub mod scheduler;
pub mod worker;

pub use error::{Error, ReaperError, Result, RuntimeError, SchedulerError, WorkerError};
