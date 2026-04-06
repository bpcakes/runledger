//! Core durable-execution contracts shared by Runledger storage, runtime, and
//! application crates.
//!
//! Use this crate for the storage-agnostic pieces of the job system:
//! - [`jobs::JobHandler`] and [`jobs::JobHandlerRegistry`] for handler contracts
//! - [`jobs::JobContext`], [`jobs::JobProgress`], and [`jobs::JobFailure`] for
//!   execution-time state
//! - workflow enqueue builders and validation helpers such as
//!   [`jobs::WorkflowRunEnqueueBuilder`] and [`jobs::validate_workflow_run_enqueue`]
//!
//! This crate does not provide persistence or worker loops. Pair it with
//! `runledger-postgres` for PostgreSQL-backed storage and `runledger-runtime`
//! for worker, scheduler, and reaper execution.

pub mod jobs;
