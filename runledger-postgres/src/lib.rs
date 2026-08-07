//! PostgreSQL persistence layer for Runledger durable execution.
//!
//! This crate owns the SQLx-backed storage and query helpers used by the
//! runtime and application crates. The main entrypoint is the [`jobs`] module,
//! which exposes APIs for:
//! - queueing, resource-aware claiming, heartbeating, and completing jobs
//! - typed direct-job recovery, successful-job replay, events, metrics, and
//!   admin reads
//! - enqueueing, actively coordinating, querying, and immutably recovering
//!   workflow runs and steps
//! - applying or validating the bundled Runledger schema migrations
//!
//! Typical consumers share a [`DbPool`] with `runledger-runtime`, then call the
//! exported [`jobs`] functions from application setup, admin APIs, or tests.
//!
//! # Security Boundary
//!
//! This crate is a persistence layer, not an authentication or authorization
//! layer. Public job and workflow APIs that accept `organization_id`,
//! idempotency keys, workflow IDs, job IDs, or metadata expect those values to
//! come from a trusted service boundary. HTTP or RPC handlers should derive
//! organization scope from authenticated claims or server-side policy, not from
//! untrusted request parameters alone.
//!
//! [`QueryError::client_message`] and [`QueryError::code`] are the stable values
//! intended for public error responses. Detailed internal context remains
//! available through [`QueryError::internal_message`] for server-side diagnostics.
//! Public formatting keeps raw SQLx details sanitized. The standard error source
//! chain and [`QueryError::source_arc`] are available for trusted server-side
//! diagnostics.
//! Runtime lifecycle, workflow mutation, and idempotent enqueue APIs are designed
//! for PostgreSQL's default `READ COMMITTED` transaction isolation so they can
//! observe rows committed after lock waits or uniqueness conflicts.
//! Release-sensitive, workflow-append, and keyed-enqueue paths validate this
//! before running because their correctness depends on second reads after waits
//! or conflicts.
//!
//! For simple embedding, call [`migrate_after_idempotency_cutover`] during
//! startup:
//!
//! ```rust,no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let pool = sqlx::PgPool::connect("postgres://localhost/runledger").await?;
//! runledger_postgres::migrate_after_idempotency_cutover(&pool).await?;
//! # Ok(())
//! # }
//! ```
//!
//! For deployments that manage DDL elsewhere, call
//! [`ensure_schema_compatible_after_idempotency_cutover`] instead to fail fast
//! if the schema is missing, drifted, or still has keyed legacy rows without
//! idempotency request snapshots. That check is read-only, but it expects the
//! database to retain SQLx migration history in `_sqlx_migrations` and, when
//! available, Runledger-owned migration state in `runledger_migration_history`.
//!
//! # Copy-Paste Examples
//!
//! - [Enqueue one job](https://github.com/bpcakes/runledger/blob/master/runledger-postgres/examples/enqueue_job.rs)
//! - [Enqueue a workflow DAG](https://github.com/bpcakes/runledger/blob/master/runledger-postgres/examples/workflow_dag.rs)
//! - [Use an external workflow gate](https://github.com/bpcakes/runledger/blob/master/runledger-postgres/examples/external_gate.rs)
//! - [Create a scheduled job entrypoint](https://github.com/bpcakes/runledger/blob/master/runledger-postgres/examples/schedule_job.rs)
//! - [Adopt continuation, coordination, replay, and recovery](https://github.com/bpcakes/runledger/blob/master/docs/downstream-agent-guide.md)
//!
//! Import `runledger_runtime::prelude::*` and use
//! [the worker binary example](https://github.com/bpcakes/runledger/blob/master/runledger-runtime/examples/worker_binary.rs)
//! when adding a worker process for the jobs and workflows enqueued through this
//! crate.
//!
//! # Prelude
//!
//! ```rust
//! use runledger_core::prelude::*;
//! use runledger_postgres::prelude::*;
//! ```
//!
//! The PostgreSQL prelude exports persistence functions and record/input types.
//! It intentionally does not re-export core contract types, so import
//! `runledger_core::prelude::*` beside it when building job or workflow inputs.
//!
//! # Enqueue One Job
//!
//! Use direct job enqueue for one independent retried unit of work.
//!
//! ```rust,no_run
//! # async fn demo(pool: runledger_postgres::DbPool) -> Result<(), Box<dyn std::error::Error>> {
//! use runledger_core::prelude::*;
//! use runledger_postgres::prelude::*;
//!
//! let payload = serde_json::json!({"email_id": "email_123"});
//! let job = JobEnqueue {
//!     job_type: JobType::new("jobs.email.send"),
//!     organization_id: None,
//!     payload: &payload,
//!     priority: None,
//!     max_attempts: None,
//!     timeout_seconds: None,
//!     next_run_at: None,
//!     idempotency_key: Some("email:email_123:send"),
//!     stage: None,
//! };
//!
//! let _job_id = enqueue_job(&pool, &job).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Create A Scheduled Job Entrypoint
//!
//! Use [`jobs::JobScheduleUpsert`] to create or update the cron row consumed by
//! the runtime scheduler. Schedules are UTC-only. Updating an existing schedule
//! refreshes its definition while preserving `is_active` and `organization_id`;
//! `next_fire_at` is refreshed when `cron_expr` changes. Cron expressions are
//! validated with the same parser used by `runledger-runtime`. Use
//! [`jobs::set_job_schedule_active`] to pause or resume a schedule, and
//! [`jobs::set_job_schedule_next_fire_at`] to manually retime its cursor.
//!
//! ```rust,no_run
//! # async fn demo(pool: runledger_postgres::DbPool) -> Result<(), Box<dyn std::error::Error>> {
//! use chrono::Utc;
//! use runledger_core::prelude::*;
//! use runledger_postgres::prelude::*;
//!
//! let payload_template = serde_json::json!({"source": "api"});
//! let schedule = JobScheduleUpsert {
//!     name: "profile-refresh-hourly",
//!     job_type: JobType::new("profiles.refresh"),
//!     organization_id: None,
//!     payload_template: &payload_template,
//!     cron_expr: "0 0 * * * *",
//!     is_active: true,
//!     next_fire_at: Utc::now(),
//!     max_jitter_seconds: 0,
//! };
//!
//! let _schedule = upsert_job_schedule(&pool, &schedule).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Enqueue A Workflow DAG
//!
//! Use workflows when the work has step dependencies, fan-out/fan-in, external
//! gates, cancellation as one logical run, or workflow-level idempotency.
//!
//! ```rust,no_run
//! # async fn demo(pool: runledger_postgres::DbPool) -> Result<(), Box<dyn std::error::Error>> {
//! use runledger_core::prelude::*;
//! use runledger_postgres::prelude::*;
//!
//! let crawl_payload = serde_json::json!({"profile_id": "p_123"});
//! let classify_payload = serde_json::json!({"profile_id": "p_123"});
//! let metadata = serde_json::json!({"source": "api"});
//!
//! let run = WorkflowDagBuilder::new("profiles.research", &metadata)
//!     .idempotency_key("profile:p_123:research")
//!     .job("crawl", "profiles.crawl", &crawl_payload)?
//!     .job("classify", "profiles.classify", &classify_payload)?
//!     .after_success("classify", ["crawl"])?
//!     .build()?;
//!
//! let _workflow_run = enqueue_workflow_run(&pool, &run).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Use An External Workflow Gate
//!
//! Create the gate with `WorkflowStepEnqueueBuilder::new_external`, then
//! complete it from a trusted service boundary when the external condition is
//! known.
//!
//! ```rust,no_run
//! # async fn demo(
//! #     pool: runledger_postgres::DbPool,
//! #     workflow_run_id: sqlx::types::Uuid,
//! # ) -> Result<(), Box<dyn std::error::Error>> {
//! use runledger_core::prelude::*;
//! use runledger_postgres::prelude::*;
//!
//! let input = CompleteExternalWorkflowStepInput {
//!     workflow_run_id,
//!     organization_id: None,
//!     step_key: StepKey::new("approval"),
//!     terminal_status: WorkflowStepStatus::Succeeded,
//!     status_reason: Some("approved"),
//!     last_error_code: None,
//!     last_error_message: None,
//!     output: None,
//! };
//!
//! let _step = complete_external_workflow_step(&pool, &input).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Inspect Workflow State
//!
//! ```rust,no_run
//! # async fn demo(
//! #     pool: runledger_postgres::DbPool,
//! #     workflow_run_id: sqlx::types::Uuid,
//! # ) -> Result<(), Box<dyn std::error::Error>> {
//! use runledger_postgres::prelude::*;
//!
//! let _run = get_workflow_run_by_id(&pool, None, workflow_run_id).await?;
//! let _steps = list_workflow_steps(&pool, None, workflow_run_id).await?;
//! let _dependencies = list_workflow_step_dependencies(&pool, None, workflow_run_id).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Coordinate And Recover Durable Work
//!
//! - Use [`jobs::enqueue_job_with_execution_resource`] or
//!   `runledger_core::jobs::WorkflowStepEnqueueBuilder::execution_resource`
//!   for lease-scoped single-permit resources.
//! - Use [`jobs::enqueue_or_get_active_workflow`] with a workflow
//!   `runledger_core::jobs::WorkflowRunEnqueueBuilder::active_key` and inspect every
//!   [`jobs::EnqueueActiveWorkflowOutcome`].
//! - Use [`jobs::compare_and_requeue_job`] for a failed/canceled/dead-lettered
//!   direct job and [`jobs::compare_and_replay_succeeded_job`] for intentional
//!   successful replay.
//! - Use [`jobs::recover_workflow_run`] to reconstruct a terminal workflow as a
//!   new lineage-linked run rather than rewriting source history.
//!
//! The caller-transaction variants of recovery and replay require PostgreSQL
//! `READ COMMITTED`. See the
//! [downstream guide](https://github.com/bpcakes/runledger/blob/master/docs/downstream-agent-guide.md)
//! for rollout fences, request idempotency, and retention behavior.
//!
//! # Handle Errors Safely
//!
//! `QueryError::client_message` and `QueryError::code` are safe for public
//! responses. `QueryError::internal_message` is for trusted diagnostics.
//!
//! ```rust,no_run
//! # async fn demo(
//! #     pool: runledger_postgres::DbPool,
//! #     job: runledger_postgres::jobs::JobEnqueue<'_>,
//! # ) -> Result<(), runledger_postgres::Error> {
//! match runledger_postgres::jobs::enqueue_job(&pool, &job).await {
//!     Ok(_job_id) => {}
//!     Err(runledger_postgres::Error::QueryError(query_error)) => {
//!         let _public_code = query_error.code();
//!         let _public_message = query_error.client_message();
//!         let _private_diagnostic = query_error.internal_message();
//!     }
//!     Err(error) => return Err(error),
//! }
//! # Ok(())
//! # }
//! ```

use std::fmt;

mod error;
pub mod jobs;
mod migrations;

pub use error::{
    FrameworkConstraintSpec, QueryError, QueryErrorCategory, QueryErrorKind,
    classify_framework_constraint, classify_query_error,
    classify_query_error_with_constraint_classifier, has_framework_constraint_classifier,
};
pub use migrations::{
    MIGRATOR, SchemaCompatibilityError, ensure_schema_compatible_after_idempotency_cutover,
    migrate_after_idempotency_cutover,
};
#[allow(deprecated)]
pub use migrations::{ensure_schema_compatible, migrate};

/// Common `runledger-postgres` imports for integration crates.
///
/// This prelude contains persistence APIs, DB types, and database record/input
/// structs. It avoids generic `Result` or `Error` aliases and does not re-export
/// `runledger-core` contracts, so it can be glob-imported alongside
/// `runledger_core::prelude::*` and `runledger_runtime::prelude::*`.
pub mod prelude {
    #[allow(deprecated)]
    pub use crate::jobs::{
        AppendWorkflowStepsInput, AppendWorkflowStepsOutcome, AppendWorkflowStepsResult,
        CompareAndReplaySucceededJob, CompareAndReplaySucceededJobOutcome, CompareAndRequeueJob,
        CompareAndRequeueJobOutcome, CompleteExternalWorkflowStepInput,
        DEFAULT_WORKFLOW_RUN_WAIT_TIMEOUT, DecodedJobEventPayload, DecodedRequeuedEventPayload,
        EnqueueActiveWorkflowOutcome, JOB_SCHEDULE_MAX_JITTER_SECONDS, JobCompletionUpdate,
        JobContinuationMetricsRecord, JobContinuationOutcome, JobContinuationUpdate,
        JobDefinitionListFilter, JobDefinitionRecord, JobDefinitionUpdate, JobDefinitionUpsert,
        JobEnqueue, JobEnqueueDisposition, JobEnqueueOutcome, JobEventRecord,
        JobFailureCompletionDisposition, JobFailureCompletionOutcome, JobFailureUpdate,
        JobLeaseIdentity, JobListFilter, JobLogRecord, JobLogRecordInput, JobMetricsRecord,
        JobPayloadUuidArrayFieldUpdate, JobPayloadUuidArrayFieldUpdateRejection, JobProgressUpdate,
        JobQueueRecord, JobRequeueStatePolicy, JobRuntimeConfigListFilter, JobRuntimeConfigRecord,
        JobRuntimeConfigUpsert, JobScheduleRecord, JobScheduleUpsert, JobScope,
        JobSuccessCompletionOutcome, NonRequeueableJobStatusError, ReapExpiredLeaseCleanupError,
        ReapExpiredLeaseCleanupOperation, ReapExpiredLeaseDeferredError,
        ReapExpiredLeasesDetailedResult, ReapExpiredLeasesResult, ReapedLeaseDisposition,
        ReapedLeaseRecord, ReapedTerminalLeaseRecord, RequeueableJobStatus,
        SuccessfulReplayEnqueuedEventPayload, WorkflowRecoveryDisposition, WorkflowRecoveryMode,
        WorkflowRecoveryOutcome, WorkflowRecoveryRequest, WorkflowRunDbRecord, WorkflowRunHandle,
        WorkflowRunHandleError, WorkflowRunHandleScope, WorkflowRunListFilter,
        WorkflowRunResultRecord, WorkflowRunWaitOptions, WorkflowStepDbRecord,
        WorkflowStepDependencyDbRecord, append_workflow_steps, append_workflow_steps_tx,
        cancel_job, cancel_workflow_run_tx, compare_and_replay_succeeded_job,
        compare_and_replay_succeeded_job_tx, compare_and_requeue_job, compare_and_requeue_job_tx,
        complete_external_workflow_step, complete_external_workflow_step_tx,
        complete_job_continuation, complete_job_continuation_for_lease,
        complete_job_continuation_with_outcome, complete_job_continuation_with_outcome_for_lease,
        complete_job_failure, complete_job_failure_for_lease, complete_job_failure_with_outcome,
        complete_job_failure_with_outcome_for_lease, complete_job_success,
        complete_job_success_for_lease, complete_job_success_with_outcome,
        complete_job_success_with_outcome_for_lease, count_workflow_step_dependencies,
        count_workflow_steps, enqueue_job, enqueue_job_tx, enqueue_job_with_execution_resource,
        enqueue_job_with_execution_resource_tx, enqueue_job_with_outcome_tx,
        enqueue_or_get_active_workflow, enqueue_or_get_active_workflow_tx, enqueue_workflow_run,
        enqueue_workflow_run_handle, enqueue_workflow_run_tx, get_job_by_id,
        get_job_continuation_metrics, get_job_definition_by_type, get_job_metrics,
        get_job_payload_by_idempotency_key, get_job_runtime_config_by_type,
        get_job_schedule_by_name, get_latest_job_payload_for_run, get_latest_workflow_run_by_type,
        get_required_job_runtime_config_by_type, get_workflow_run_by_id,
        get_workflow_run_by_type_and_idempotency_key, get_workflow_run_id_for_job,
        heartbeat_job_for_lease, insert_job_definition_if_missing_tx, insert_job_log,
        insert_job_runtime_config_if_missing, list_job_definitions, list_job_events, list_job_logs,
        list_job_runtime_configs, list_jobs, list_workflow_runs, list_workflow_step_dependencies,
        list_workflow_step_dependencies_page, list_workflow_steps, list_workflow_steps_page,
        prepare_schedule_exact_sync_critical_section_tx, reap_expired_leases_with_diagnostics,
        recover_workflow_run, recover_workflow_run_tx, requeue_job, retrieve_workflow_run_handle,
        set_job_schedule_active, set_job_schedule_active_tx, set_job_schedule_next_fire_at,
        set_job_schedule_next_fire_at_tx, sync_catalog_job_schedules_tx, update_job_definition,
        update_job_payload_uuid_array_field, update_job_progress_for_lease,
        update_workflow_step_and_pending_job_payload_tx, upsert_job_definition_tx,
        upsert_job_runtime_config, upsert_job_runtime_config_tx, upsert_job_schedule,
        upsert_job_schedule_tx, workflow_run_handle,
    };
    pub use crate::jobs::{
        JobScheduleCatalogSyncEntry, JobScheduleCatalogSyncReport,
        deactivate_schedules_absent_from_names_tx,
    };
    pub use crate::{
        DbPool, DbTx, FrameworkConstraintSpec, MIGRATOR, QueryError, QueryErrorCategory,
        QueryErrorKind, SchemaCompatibilityError,
        ensure_schema_compatible_after_idempotency_cutover, migrate_after_idempotency_cutover,
    };
}

pub type DbPool = sqlx::PgPool;
pub type DbTx<'a> = sqlx::Transaction<'a, sqlx::Postgres>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    ConfigError(String),
    ConnectionError(String),
    MigrationError(String),
    QueryError(QueryError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConfigError(message) => write!(f, "{message}"),
            Self::ConnectionError(message) => write!(f, "{message}"),
            Self::MigrationError(message) => write!(f, "{message}"),
            Self::QueryError(query_error) => write!(f, "{query_error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::QueryError(query_error) => Some(query_error),
            Self::ConfigError(_) | Self::ConnectionError(_) | Self::MigrationError(_) => None,
        }
    }
}

impl Error {
    #[must_use]
    pub fn from_query_sqlx(error: sqlx::Error) -> Self {
        Self::QueryError(QueryError::from_sqlx(error, None))
    }

    #[must_use]
    pub fn from_query_sqlx_with_context(context: &str, error: sqlx::Error) -> Self {
        Self::QueryError(QueryError::from_sqlx(error, Some(context)))
    }
}
