//! Durable job, schedule, workflow, and admin persistence APIs.
//!
//! Choose the highest-level API that matches the work. Use `enqueue_job` for
//! one independent retried unit. Use workflow DAG APIs such as
//! `enqueue_workflow_run`, `WorkflowRunEnqueueBuilder`, and
//! `WorkflowStepEnqueueBuilder` when work has dependencies, fan-out/fan-in,
//! external gates, workflow-level idempotency, active coordination, or
//! immutable workflow recovery.
//!
//! Avoid manually orchestrating ordinary workflows by polling job state,
//! enqueueing child jobs from handlers, or storing dependency state in payloads.
//! Use `enqueue_job_with_execution_resource` for lease-fenced mutual exclusion,
//! `compare_and_requeue_job` for terminal direct-job recovery,
//! `compare_and_replay_succeeded_job` for intentional successful replay, and
//! `recover_workflow_run` for a new lineage-linked workflow run.

mod admin;
mod errors;
mod logs;
mod queue;
mod replay;
mod row_decode;
mod rows;
mod runtime_configs;
mod schedule_definition_guard;
mod schedules;
mod transaction_isolation;
mod transaction_settings;
mod types;
mod workflow_types;
mod workflows;

#[allow(
    deprecated,
    reason = "deprecated admin entrypoints remain re-exported for semver compatibility"
)]
pub use admin::{
    JobPayloadUuidArrayFieldUpdate, JobPayloadUuidArrayFieldUpdateRejection, cancel_job,
    compare_and_requeue_job, compare_and_requeue_job_tx, get_admin_job_metrics_page, get_job_by_id,
    get_job_continuation_metrics, get_job_metrics, get_job_payload_by_idempotency_key,
    get_latest_job_payload_for_run, list_admin_job_summaries, list_admin_workflow_summaries,
    list_job_events, list_job_events_before, list_jobs, requeue_job,
    update_job_payload_uuid_array_field,
};
pub use logs::{insert_job_log, list_job_logs, list_job_logs_before};
pub use queue::{
    JobDefinitionCatalogSyncError, JobDefinitionCatalogSyncMode, JobDefinitionCatalogSyncReport,
    claim_jobs, claim_jobs_for_types, claim_prestart_jobs, claim_prestart_jobs_for_types,
    complete_job_continuation, complete_job_continuation_for_lease,
    complete_job_continuation_with_outcome, complete_job_continuation_with_outcome_for_lease,
    complete_job_failure, complete_job_failure_for_lease, complete_job_failure_with_outcome,
    complete_job_failure_with_outcome_for_lease, complete_job_success,
    complete_job_success_for_lease, complete_job_success_with_outcome,
    complete_job_success_with_outcome_for_lease, delete_promoted_job_enqueue_intents_before,
    delete_promoted_job_enqueue_intents_for_jobs_tx, enqueue_job, enqueue_job_tx,
    enqueue_job_with_execution_resource, enqueue_job_with_execution_resource_tx,
    enqueue_job_with_outcome_tx, get_job_definition_by_type, get_job_enqueue_intent_by_id,
    get_job_enqueue_intent_metrics, heartbeat_job, heartbeat_job_for_lease,
    insert_job_definition_if_missing_tx, list_job_definitions, list_job_enqueue_intents,
    promote_job_enqueue_intents_for_types, reap_expired_leases,
    reap_expired_leases_with_diagnostics, reap_expired_leases_with_terminal_records,
    record_job_enqueue_intent, record_job_enqueue_intent_tx, release_unstarted_job_claim,
    sync_catalog_job_definitions_exact_tx, sync_catalog_job_definitions_tx, update_job_definition,
    update_job_progress, update_job_progress_for_lease, upsert_job_definition_tx,
};
pub use replay::{
    CompareAndReplaySucceededJob, CompareAndReplaySucceededJobOutcome,
    compare_and_replay_succeeded_job, compare_and_replay_succeeded_job_tx,
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
    AdminJobMetricsRecord, AdminJobSummaryFilter, AdminJobSummaryRecord,
    AdminWorkflowSummaryFilter, AdminWorkflowSummaryRecord, CompareAndRequeueJob,
    CompareAndRequeueJobOutcome, DecodedJobEventPayload, DecodedRequeuedEventPayload,
    JOB_LIST_PAGE_LIMIT_MAX, JOB_SCHEDULE_MAX_JITTER_SECONDS, JobCompletionUpdate,
    JobContinuationMetricsRecord, JobContinuationOutcome, JobContinuationUpdate,
    JobDefinitionListFilter, JobDefinitionRecord, JobDefinitionUpdate, JobDefinitionUpsert,
    JobEnqueue, JobEnqueueDisposition, JobEnqueueIntent, JobEnqueueIntentDisposition,
    JobEnqueueIntentListFilter, JobEnqueueIntentMetricsFilter, JobEnqueueIntentMetricsRecord,
    JobEnqueueIntentOutcome, JobEnqueueIntentPromotionReport, JobEnqueueIntentRecord,
    JobEnqueueIntentStatus, JobEnqueueOutcome, JobEventRecord, JobFailureCompletionDisposition,
    JobFailureCompletionOutcome, JobFailureUpdate, JobLeaseIdentity, JobListFilter, JobLogRecord,
    JobLogRecordInput, JobMetricsRecord, JobProgressUpdate, JobQueueRecord, JobRequeueStatePolicy,
    JobRuntimeConfigListFilter, JobRuntimeConfigRecord, JobRuntimeConfigUpsert,
    JobScheduleCatalogSyncEntry, JobScheduleCatalogSyncReport, JobScheduleJobTypeReference,
    JobScheduleRecord, JobScheduleUpsert, JobScope, JobSuccessCompletionOutcome,
    NonRequeueableJobStatusError, ReapExpiredLeaseCleanupError, ReapExpiredLeaseCleanupOperation,
    ReapExpiredLeaseDeferredError, ReapExpiredLeasesDetailedResult, ReapExpiredLeasesResult,
    ReapedLeaseDisposition, ReapedLeaseRecord, ReapedTerminalLeaseRecord, RequeueableJobStatus,
    SuccessfulReplayEnqueuedEventPayload,
};
pub use workflow_types::{
    AppendWorkflowStepsInput, AppendWorkflowStepsOutcome, AppendWorkflowStepsResult,
    CompleteExternalWorkflowStepInput, DEFAULT_WORKFLOW_RUN_WAIT_TIMEOUT,
    EnqueueActiveWorkflowOutcome, WorkflowRecoveryDisposition, WorkflowRecoveryMode,
    WorkflowRecoveryOutcome, WorkflowRecoveryRequest, WorkflowRunCountFilter, WorkflowRunDbRecord,
    WorkflowRunHandle, WorkflowRunHandleError, WorkflowRunHandleScope, WorkflowRunListFilter,
    WorkflowRunResultRecord, WorkflowRunWaitOptions, WorkflowStepDbRecord,
    WorkflowStepDependencyDbRecord,
};
pub use workflows::{
    append_workflow_steps, append_workflow_steps_tx, cancel_workflow_run_tx,
    complete_external_workflow_step, complete_external_workflow_step_tx, count_workflow_runs,
    count_workflow_step_dependencies, count_workflow_steps, enqueue_or_get_active_workflow,
    enqueue_or_get_active_workflow_tx, enqueue_workflow_run, enqueue_workflow_run_handle,
    enqueue_workflow_run_tx, get_latest_workflow_run_by_type, get_workflow_run_by_id,
    get_workflow_run_by_type_and_idempotency_key, get_workflow_run_by_type_and_idempotency_key_tx,
    get_workflow_run_id_for_job, list_workflow_runs, list_workflow_step_dependencies,
    list_workflow_step_dependencies_in_organization_page, list_workflow_step_dependencies_page,
    list_workflow_step_keys_for_update_tx, list_workflow_steps,
    list_workflow_steps_in_organization_page, list_workflow_steps_page, recover_workflow_run,
    recover_workflow_run_tx, retrieve_workflow_run_handle,
    update_workflow_step_and_pending_job_payload_tx, workflow_run_handle,
};
#[cfg(feature = "test-support")]
pub mod test_support {
    use runledger_core::jobs::{JobFailure, JobTypeName};
    use serde_json::Value;
    use sqlx::types::Uuid;

    use super::types::{ReapedLeaseDisposition, ReapedLeaseRecord, ReapedTerminalLeaseRecord};

    pub use super::workflows::test_support::workflow_run_release_lock_key;

    #[expect(
        clippy::too_many_arguments,
        reason = "the test fixture mirrors the complete reaped lease record shape"
    )]
    pub fn reaped_lease_record(
        job_id: Uuid,
        job_type: JobTypeName,
        organization_id: Option<Uuid>,
        run_number: i32,
        attempt: i32,
        max_attempts: i32,
        worker_id: Option<String>,
        started_without_renewal_heartbeat: bool,
        disposition: ReapedLeaseDisposition,
    ) -> ReapedLeaseRecord {
        reaped_lease_record_with_checkpoint(
            job_id,
            job_type,
            organization_id,
            run_number,
            attempt,
            max_attempts,
            None,
            worker_id,
            started_without_renewal_heartbeat,
            disposition,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "the test fixture mirrors the complete checkpoint-bearing reaped lease record shape"
    )]
    pub fn reaped_lease_record_with_checkpoint(
        job_id: Uuid,
        job_type: JobTypeName,
        organization_id: Option<Uuid>,
        run_number: i32,
        attempt: i32,
        max_attempts: i32,
        checkpoint: Option<Value>,
        worker_id: Option<String>,
        started_without_renewal_heartbeat: bool,
        disposition: ReapedLeaseDisposition,
    ) -> ReapedLeaseRecord {
        ReapedLeaseRecord {
            job_id,
            job_type,
            organization_id,
            run_number,
            attempt,
            max_attempts,
            checkpoint,
            worker_id,
            started_without_renewal_heartbeat,
            failure: lease_expired_failure(),
            disposition,
        }
    }

    pub fn reaped_terminal_lease_record(
        job_id: Uuid,
        job_type: JobTypeName,
        organization_id: Option<Uuid>,
        run_number: i32,
        attempt: i32,
        payload: Value,
    ) -> ReapedTerminalLeaseRecord {
        ReapedTerminalLeaseRecord {
            job_id,
            job_type,
            organization_id,
            run_number,
            attempt,
            payload,
        }
    }

    fn lease_expired_failure() -> JobFailure {
        JobFailure::lease_expired("job.lease_expired", "Job lease expired before completion.")
    }
}

#[deprecated(note = "Use WorkflowRunDbRecord instead.")]
pub type WorkflowRunRecord = WorkflowRunDbRecord;

#[deprecated(note = "Use WorkflowStepDbRecord instead.")]
pub type WorkflowStepRecord = WorkflowStepDbRecord;
