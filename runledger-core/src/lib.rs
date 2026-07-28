//! Core durable-execution contracts shared by Runledger storage, runtime, and
//! application crates.
//!
//! Use this crate for the storage-agnostic pieces of the job system:
//! - [`jobs::JobHandler`] and [`jobs::JobHandlerRegistry`] for handler contracts
//! - [`jobs::JobContext`], [`jobs::JobCompletion`], and [`jobs::JobFailure`] for
//!   execution-time state, successful bounded continuation, and
//!   handler-selected retry lower bounds
//! - workflow enqueue builders and validation helpers such as
//!   [`jobs::WorkflowDagBuilder`], [`jobs::WorkflowRunEnqueueBuilder`], and
//!   [`jobs::validate_workflow_run_enqueue`], including active-workflow keys,
//!   execution resources, result steps, and per-step continuation opt-in
//!
//! Use workflow builders when work has dependencies, fan-out/fan-in, external
//! gates, or workflow-level idempotency. A single direct job is appropriate only
//! for one independent unit of retried work.
//!
//! This crate does not provide persistence or worker loops. Pair it with
//! `runledger-postgres` for PostgreSQL-backed storage and `runledger-runtime`
//! for worker, scheduler, and reaper execution.
//!
//! # Copy-Paste Examples
//!
//! - [Enqueue one job](https://github.com/bpcakes/runledger/blob/master/runledger-postgres/examples/enqueue_job.rs)
//! - [Enqueue a workflow DAG](https://github.com/bpcakes/runledger/blob/master/runledger-postgres/examples/workflow_dag.rs)
//! - [Use an external workflow gate](https://github.com/bpcakes/runledger/blob/master/runledger-postgres/examples/external_gate.rs)
//! - [Run a worker binary](https://github.com/bpcakes/runledger/blob/master/runledger-runtime/examples/worker_binary.rs)
//! - [Create a scheduled job entrypoint](https://github.com/bpcakes/runledger/blob/master/runledger-postgres/examples/schedule_job.rs)
//! - [Adopt continuation, coordination, replay, and recovery](https://github.com/bpcakes/runledger/blob/master/docs/downstream-agent-guide.md)
//!
//! # Prelude
//!
//! Import the common contracts with:
//!
//! ```rust
//! use runledger_core::prelude::*;
//! ```
//!
//! The prelude intentionally exports contract types and builders, not storage or
//! runtime APIs. Pair it with `runledger_postgres::prelude::*` and
//! `runledger_runtime::prelude::*` in integration crates.
//!
//! # Build A Job Handler
//!
//! ```rust
//! use runledger_core::prelude::*;
//! use serde_json::Value;
//!
//! struct SendEmail;
//!
//! #[async_trait]
//! impl JobHandler for SendEmail {
//!     fn job_type(&self) -> JobType<'static> {
//!         JobType::new("jobs.email.send")
//!     }
//!
//!     async fn execute(
//!         &self,
//!         _context: JobContext,
//!         _payload: Value,
//!     ) -> Result<JobCompletion, JobFailure> {
//!         Ok(JobCompletion::success())
//!     }
//! }
//! ```
//!
//! # Build A Workflow DAG
//!
//! ```rust
//! use runledger_core::prelude::*;
//!
//! let crawl_payload = serde_json::json!({"profile_id": "p_123"});
//! let classify_payload = serde_json::json!({"profile_id": "p_123"});
//! let metadata = serde_json::json!({"source": "api"});
//!
//! let run = WorkflowDagBuilder::new("profiles.research", &metadata)
//!     .job("crawl", "profiles.crawl", &crawl_payload)?
//!     .job("classify", "profiles.classify", &classify_payload)?
//!     .after_success("classify", ["crawl"])?
//!     .build()?;
//! # Ok::<_, WorkflowBuildError>(())
//! ```

pub mod jobs;

/// Common `runledger-core` imports for consumers.
///
/// This prelude contains storage-agnostic job and workflow contracts. It avoids
/// generic `Result` or `Error` aliases so it can be glob-imported alongside
/// `runledger_postgres::prelude::*` and `runledger_runtime::prelude::*`.
pub mod prelude {
    pub use async_trait::async_trait;

    pub use crate::jobs::{
        IdentifierValidationError, JobCompletion, JobCompletionDisposition, JobContext,
        JobDeadLetterInfo, JobDeadLetterReason, JobEventType, JobFailure, JobFailureKind,
        JobHandler, JobHandlerRegistry, JobRetryTiming, JobStage, JobStatus, JobType, JobTypeName,
        StepKey, StepKeyName, WorkflowBuildError, WorkflowDagBuilder,
        WorkflowDagDependencyValidationInput, WorkflowDagStepValidationInput,
        WorkflowDagValidationError, WorkflowDependencyReleaseMode, WorkflowRunEnqueue,
        WorkflowRunEnqueueBuilder, WorkflowRunStatus, WorkflowStepDependencySpec,
        WorkflowStepEnqueue, WorkflowStepEnqueueBuilder, WorkflowStepExecutionKind,
        WorkflowStepStatus, WorkflowType, WorkflowTypeName, validate_workflow_dag,
        validate_workflow_run_enqueue, validate_workflow_step_append,
    };
}
