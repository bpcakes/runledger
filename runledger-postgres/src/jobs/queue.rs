pub(crate) mod advance;
mod attempts;
mod claim;
mod definitions;
mod enqueue;
pub(crate) mod events;
mod failure_transition;
mod intents;
mod lifecycle;
mod lifecycle_timeouts;
mod reaper;
mod release;

pub use self::claim::{
    claim_jobs, claim_jobs_for_types, claim_prestart_jobs, claim_prestart_jobs_for_types,
};
pub use self::definitions::{
    JobDefinitionCatalogSyncError, JobDefinitionCatalogSyncMode, JobDefinitionCatalogSyncReport,
    get_job_definition_by_type, insert_job_definition_if_missing_tx, list_job_definitions,
    sync_catalog_job_definitions_exact_tx, sync_catalog_job_definitions_tx, update_job_definition,
    upsert_job_definition_tx,
};
pub(in crate::jobs) use self::enqueue::enqueue_replayed_job_with_outcome_tx;
pub use self::enqueue::{
    enqueue_job, enqueue_job_tx, enqueue_job_with_execution_resource,
    enqueue_job_with_execution_resource_tx, enqueue_job_with_outcome, enqueue_job_with_outcome_tx,
};
pub use self::intents::{
    delete_promoted_job_enqueue_intents_before, delete_promoted_job_enqueue_intents_for_jobs_tx,
    get_job_enqueue_intent_by_id, get_job_enqueue_intent_by_id_with_scope,
    get_job_enqueue_intent_metrics, list_job_enqueue_intents, list_job_enqueue_intents_with_scope,
    promote_job_enqueue_intents_for_types, record_job_enqueue_intent, record_job_enqueue_intent_tx,
};
#[allow(
    deprecated,
    reason = "deprecated stage-bearing progress wrappers remain re-exported for semver compatibility"
)]
pub use self::lifecycle::{
    complete_job_continuation, complete_job_continuation_for_lease,
    complete_job_continuation_with_outcome, complete_job_continuation_with_outcome_for_lease,
    complete_job_failure, complete_job_failure_for_lease, complete_job_failure_with_outcome,
    complete_job_failure_with_outcome_for_lease, complete_job_success,
    complete_job_success_for_lease, complete_job_success_with_outcome,
    complete_job_success_with_outcome_for_lease, heartbeat_job, heartbeat_job_for_lease,
    mark_job_running, mark_job_running_for_lease, update_job_ordinary_progress,
    update_job_ordinary_progress_for_lease, update_job_progress, update_job_progress_for_lease,
};
pub use self::reaper::{
    reap_expired_leases, reap_expired_leases_with_diagnostics,
    reap_expired_leases_with_terminal_records,
};
pub use self::release::release_unstarted_job_claim;
