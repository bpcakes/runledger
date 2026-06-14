//! Durable job, schedule, workflow, and admin persistence APIs.
//!
//! Choose the highest-level API that matches the work. Use `enqueue_job` for
//! one independent retried unit. Use workflow DAG APIs such as
//! `enqueue_workflow_run`, `WorkflowRunEnqueueBuilder`, and
//! `WorkflowStepEnqueueBuilder` when work has dependencies, fan-out/fan-in,
//! external gates, or workflow-level idempotency.
//!
//! Avoid manually orchestrating ordinary workflows by polling job state,
//! enqueueing child jobs from handlers, or storing dependency state in payloads.

mod admin;
mod errors;
mod logs;
mod queue;
mod row_decode;
mod runtime_configs;
mod schedule_definition_guard;
mod schedules;
mod transaction_isolation;
mod types;
mod workflow_types;
mod workflows;

pub use admin::{
    JobPayloadUuidArrayFieldUpdate, JobPayloadUuidArrayFieldUpdateRejection, cancel_job,
    get_job_by_id, get_job_metrics, get_job_payload_by_idempotency_key,
    get_latest_job_payload_for_run, list_job_events, list_jobs, requeue_job,
    update_job_payload_uuid_array_field,
};
pub use logs::{insert_job_log, list_job_logs};
pub use queue::{
    JobDefinitionCatalogSyncError, JobDefinitionCatalogSyncMode, JobDefinitionCatalogSyncReport,
    claim_jobs, claim_jobs_for_types, claim_prestart_jobs, claim_prestart_jobs_for_types,
    complete_job_failure, complete_job_success, enqueue_job, enqueue_job_tx,
    get_job_definition_by_type, heartbeat_job, insert_job_definition_if_missing_tx,
    list_job_definitions, reap_expired_leases, reap_expired_leases_with_terminal_records,
    release_unstarted_job_claim, sync_catalog_job_definitions_exact_tx,
    sync_catalog_job_definitions_tx, update_job_definition, update_job_progress,
    upsert_job_definition_tx,
};
pub use runtime_configs::{
    get_job_runtime_config_by_type, get_required_job_runtime_config_by_type,
    insert_job_runtime_config_if_missing, insert_job_runtime_config_if_missing_tx,
    list_job_runtime_configs, upsert_job_runtime_config, upsert_job_runtime_config_tx,
};
pub use schedules::{
    claim_due_schedules_tx, deactivate_schedules_absent_from_names_tx, get_job_schedule_by_name,
    mark_schedule_fired_tx, prepare_schedule_exact_sync_critical_section_tx,
    set_job_schedule_active, set_job_schedule_active_tx, set_job_schedule_next_fire_at,
    set_job_schedule_next_fire_at_tx, sync_catalog_job_schedules_tx, upsert_job_schedule,
    upsert_job_schedule_tx,
};
pub use types::{
    JOB_SCHEDULE_MAX_JITTER_SECONDS, JobCompletionUpdate, JobDefinitionListFilter,
    JobDefinitionRecord, JobDefinitionUpdate, JobDefinitionUpsert, JobEnqueue, JobEventRecord,
    JobFailureUpdate, JobListFilter, JobLogRecord, JobLogRecordInput, JobMetricsRecord,
    JobProgressUpdate, JobQueueRecord, JobRuntimeConfigListFilter, JobRuntimeConfigRecord,
    JobRuntimeConfigUpsert, JobScheduleCatalogSyncEntry, JobScheduleCatalogSyncReport,
    JobScheduleJobTypeReference, JobScheduleRecord, JobScheduleUpsert, ReapExpiredLeasesResult,
    ReapedTerminalLeaseRecord,
};
pub use workflow_types::{
    AppendWorkflowStepsInput, AppendWorkflowStepsOutcome, AppendWorkflowStepsResult,
    CompleteExternalWorkflowStepInput, DEFAULT_WORKFLOW_RUN_WAIT_TIMEOUT, WorkflowRunCountFilter,
    WorkflowRunDbRecord, WorkflowRunHandle, WorkflowRunHandleError, WorkflowRunHandleScope,
    WorkflowRunListFilter, WorkflowRunResultRecord, WorkflowRunWaitOptions, WorkflowStepDbRecord,
    WorkflowStepDependencyDbRecord,
};
pub use workflows::{
    append_workflow_steps, append_workflow_steps_tx, cancel_workflow_run_tx,
    complete_external_workflow_step, complete_external_workflow_step_tx, count_workflow_runs,
    enqueue_workflow_run, enqueue_workflow_run_handle, enqueue_workflow_run_tx,
    get_latest_workflow_run_by_type, get_workflow_run_by_id,
    get_workflow_run_by_type_and_idempotency_key, get_workflow_run_by_type_and_idempotency_key_tx,
    get_workflow_run_id_for_job, list_workflow_runs, list_workflow_step_dependencies,
    list_workflow_step_keys_for_update_tx, list_workflow_steps, retrieve_workflow_run_handle,
    update_workflow_step_and_pending_job_payload_tx, workflow_run_handle,
};
#[cfg(feature = "test-support")]
pub mod test_support {
    pub use super::workflows::test_support::workflow_run_release_lock_key;
}

#[deprecated(note = "Use WorkflowRunDbRecord instead.")]
pub type WorkflowRunRecord = WorkflowRunDbRecord;

#[deprecated(note = "Use WorkflowStepDbRecord instead.")]
pub type WorkflowStepRecord = WorkflowStepDbRecord;
