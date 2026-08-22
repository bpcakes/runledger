use runledger_core::jobs::{WorkflowRunEnqueue, validate_workflow_run_enqueue};

use super::super::workflow_types::{EnqueueActiveWorkflowOutcome, WorkflowRunDbRecord};
use super::active_claims::{
    insert_workflow_active_claim_tx, load_existing_active_workflow_run_tx,
    lock_workflow_active_key_tx,
};
use super::enqueue_persistence::{
    insert_workflow_run_record_tx, try_load_existing_idempotent_workflow_run_tx,
    validate_existing_idempotent_workflow_run,
};
use super::errors::{
    workflow_active_key_api_required_error, workflow_active_key_required_error,
    workflow_internal_state_error,
};
use super::read::load_workflow_run_by_id_tx;
use super::release::enqueue_root_steps_tx;
use super::runtime::recompute_workflow_run_statuses_tx;
use super::snapshot::canonical_workflow_enqueue_request;
use super::steps::{
    WorkflowStepDependencyWriteContext, fetch_job_definition_defaults_tx,
    insert_workflow_step_dependencies_tx, insert_workflow_steps_tx,
};
use super::validation::workflow_dag_validation_error;
use crate::jobs::transaction_isolation::{ReadCommittedTx, ensure_read_committed_tx};
use crate::{DbPool, DbTx, Error, Result};

/// Enqueues a workflow run in its own transaction.
///
/// Use this API for multi-step work with dependencies, fan-out/fan-in, external
/// gates, cancellation as one logical run, or workflow-level idempotency. Build
/// the payload with `WorkflowRunEnqueueBuilder` and
/// `WorkflowStepEnqueueBuilder`.
///
/// Calls without an idempotency key always create a new workflow run. Calls
/// with an idempotency key return the existing run only when the canonical
/// enqueue request snapshot matches. Keyed rows without snapshots are rejected
/// by the idempotency cutover.
#[doc(alias = "dag")]
#[doc(alias = "orchestration")]
#[doc(alias = "dependencies")]
pub async fn enqueue_workflow_run(
    pool: &DbPool,
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<WorkflowRunDbRecord> {
    if payload.active_key().is_some() {
        return Err(workflow_active_key_api_required_error());
    }
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    let outcome = enqueue_workflow_run_classified_tx(&mut tx, payload).await?;
    let workflow_run = workflow_run_from_classified_outcome(outcome)?;
    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    Ok(workflow_run)
}

/// Enqueues a workflow run and returns the existing run for an identical keyed retry.
///
/// Use this API when composing a workflow enqueue with other database writes in
/// one transaction. For ordinary dependent work, prefer this workflow path over
/// direct-job polling or handler-chained follow-up jobs.
///
/// Idempotency is strict for the submitted request snapshot. The snapshot is
/// compared instead of live workflow step rows because steps and dependencies
/// can be legitimately appended or mutated after initial enqueue. Strict
/// workflow idempotency applies to keyed rows with an `enqueue_request`
/// snapshot. New unkeyed rows also store snapshots for workflow recovery, while
/// keyed legacy rows without snapshots are rejected by the idempotency cutover.
/// Job-step stage is part of the canonical initial request after normalizing an
/// omitted stage to `Queued`; changing the requested initial stage is treated as
/// a different enqueue request.
#[doc(alias = "dag")]
#[doc(alias = "orchestration")]
#[doc(alias = "dependencies")]
pub async fn enqueue_workflow_run_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<WorkflowRunDbRecord> {
    if payload.active_key().is_some() {
        return Err(workflow_active_key_api_required_error());
    }
    let outcome = enqueue_workflow_run_classified_tx(tx, payload).await?;
    workflow_run_from_classified_outcome(outcome)
}

fn workflow_run_from_classified_outcome(
    outcome: EnqueueActiveWorkflowOutcome,
) -> Result<WorkflowRunDbRecord> {
    match outcome {
        EnqueueActiveWorkflowOutcome::Inserted(run)
        | EnqueueActiveWorkflowOutcome::ExistingIdempotent(run) => Ok(run),
        EnqueueActiveWorkflowOutcome::ExistingActive(_) => Err(workflow_internal_state_error(
            "active workflow collision was returned for a payload without active_key",
        )),
    }
}

/// Enqueues a workflow under a reusable active key and explicitly classifies
/// insertion, active collision, and permanent idempotency collision.
///
/// Active-key scope is global when `organization_id` is absent and otherwise
/// organization-local; workflow type is not part of the scope. Claims remain
/// reserved until terminal work and canceled leases are quiescent, so
/// [`EnqueueActiveWorkflowOutcome::ExistingActive`] may carry a terminal
/// canceled run. Callers must make their decision from the outcome rather than
/// status alone.
pub async fn enqueue_or_get_active_workflow(
    pool: &DbPool,
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<EnqueueActiveWorkflowOutcome> {
    if payload.active_key().is_none() {
        return Err(workflow_active_key_required_error());
    }
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    let outcome = enqueue_workflow_run_classified_tx(&mut tx, payload).await?;
    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    Ok(outcome)
}

/// Caller-transaction counterpart to [`enqueue_or_get_active_workflow`].
///
/// The transaction must use `READ COMMITTED`. This function neither commits nor
/// rolls it back.
pub async fn enqueue_or_get_active_workflow_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<EnqueueActiveWorkflowOutcome> {
    if payload.active_key().is_none() {
        return Err(workflow_active_key_required_error());
    }
    enqueue_workflow_run_classified_tx(tx, payload).await
}

async fn enqueue_workflow_run_classified_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<EnqueueActiveWorkflowOutcome> {
    validate_workflow_run_enqueue(payload).map_err(workflow_dag_validation_error)?;
    if payload.idempotency_key().is_some() || payload.active_key().is_some() {
        let mut read_committed_tx = ensure_read_committed_tx(
            tx,
            "workflow coordinated enqueue",
            "workflow.enqueue_idempotency_unsupported_isolation",
            "Workflow idempotent or active-key enqueue requires READ COMMITTED transaction isolation.",
        )
        .await?;

        return enqueue_coordinated_workflow_run_read_committed_tx(&mut read_committed_tx, payload)
            .await;
    }

    enqueue_workflow_run_classified_tx_inner(tx, payload).await
}

async fn enqueue_coordinated_workflow_run_read_committed_tx(
    tx: &mut ReadCommittedTx<'_, '_>,
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<EnqueueActiveWorkflowOutcome> {
    debug_assert!(payload.idempotency_key().is_some() || payload.active_key().is_some());
    enqueue_workflow_run_classified_tx_inner(tx.as_tx(), payload).await
}

async fn enqueue_workflow_run_classified_tx_inner(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<EnqueueActiveWorkflowOutcome> {
    let enqueue_request = canonical_workflow_enqueue_request(payload)?;

    if let Some(active_key) = payload.active_key() {
        lock_workflow_active_key_tx(tx, payload.organization_id(), active_key).await?;

        if let Some(idempotency_key) = payload.idempotency_key() {
            if let Some(existing) = try_load_existing_idempotent_workflow_run_tx(
                tx,
                payload,
                idempotency_key,
                &enqueue_request,
            )
            .await?
            {
                validate_existing_idempotent_workflow_run(&existing)?;
                return Ok(EnqueueActiveWorkflowOutcome::ExistingIdempotent(
                    existing.into_record()?,
                ));
            }
        }

        if let Some(existing) =
            load_existing_active_workflow_run_tx(tx, payload.organization_id(), active_key).await?
        {
            return Ok(EnqueueActiveWorkflowOutcome::ExistingActive(existing));
        }
    }

    let workflow_run_insert = insert_workflow_run_record_tx(tx, payload, &enqueue_request).await?;
    let workflow_run = workflow_run_insert.record;
    if !workflow_run_insert.inserted {
        // Existing idempotent runs already have their steps and initial releases
        // committed; never replay workflow initialization for a retry.
        return Ok(EnqueueActiveWorkflowOutcome::ExistingIdempotent(
            workflow_run,
        ));
    }
    if let Some(active_key) = payload.active_key() {
        insert_workflow_active_claim_tx(tx, payload.organization_id(), active_key, workflow_run.id)
            .await?;
    }

    let defaults_by_job_type = fetch_job_definition_defaults_tx(tx, payload.steps()).await?;
    let step_id_by_key =
        insert_workflow_steps_tx(tx, payload, workflow_run.id, &defaults_by_job_type).await?;
    insert_workflow_step_dependencies_tx(
        tx,
        payload.steps(),
        workflow_run.id,
        &step_id_by_key,
        WorkflowStepDependencyWriteContext::InitialEnqueue,
    )
    .await?;

    enqueue_root_steps_tx(tx, workflow_run.id).await?;
    recompute_workflow_run_statuses_tx(tx, &std::collections::BTreeSet::from([workflow_run.id]))
        .await?;

    let workflow_run = load_workflow_run_by_id_tx(
        tx,
        workflow_run.id,
        "load workflow run after enqueue recompute",
    )
    .await?;
    Ok(EnqueueActiveWorkflowOutcome::Inserted(workflow_run))
}

#[cfg(test)]
mod tests {
    use runledger_core::jobs::{
        JobStage, JobType, StepKey, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder,
        WorkflowType,
    };
    use serde_json::json;
    use sqlx::types::Uuid;

    use super::super::snapshot::canonical_workflow_enqueue_request;

    #[test]
    fn canonical_workflow_enqueue_request_matches_golden_snapshot() {
        let run_org = Uuid::now_v7();
        let step_org = Uuid::now_v7();
        let metadata = json!({"kind": "golden"});
        let root_payload = json!({"step": "root"});
        let child_payload = json!({"step": "child"});
        let root = WorkflowStepEnqueueBuilder::new_external(StepKey::new("root"), &root_payload)
            .try_build()
            .expect("build root step");
        let child = WorkflowStepEnqueueBuilder::new(
            StepKey::new("child"),
            JobType::new("jobs.test.child"),
            &child_payload,
        )
        .organization_id(step_org)
        .priority(7)
        .max_attempts(2)
        .timeout_seconds(45)
        .stage(JobStage::Scheduled)
        .depends_on_success(&[StepKey::new("root")])
        .try_build()
        .expect("build child step");
        let workflow =
            WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test.golden"), &metadata)
                .organization_id(run_org)
                .step(child)
                .step(root)
                .try_build()
                .expect("build workflow");

        let canonical =
            canonical_workflow_enqueue_request(&workflow).expect("canonicalize workflow enqueue");

        assert_eq!(
            canonical,
            json!({
                "metadata": {"kind": "golden"},
                "steps": [
                    {
                        "step_key": "child",
                        "execution_kind": "JOB",
                        "job_type": "jobs.test.child",
                        "organization_id": step_org,
                        "payload": {"step": "child"},
                        "priority": 7,
                        "max_attempts": 2,
                        "timeout_seconds": 45,
                        "stage": "scheduled",
                        "dependencies": [
                            {
                                "prerequisite_step_key": "root",
                                "release_mode": "ON_SUCCESS"
                            }
                        ]
                    },
                    {
                        "step_key": "root",
                        "execution_kind": "EXTERNAL",
                        "job_type": null,
                        "organization_id": run_org,
                        "payload": {"step": "root"},
                        "priority": null,
                        "max_attempts": null,
                        "timeout_seconds": null,
                        "stage": null,
                        "dependencies": []
                    }
                ]
            })
        );
    }

    #[test]
    fn canonical_workflow_enqueue_request_includes_result_step_key_when_present() {
        let metadata = json!({});
        let payload = json!({"step": "result"});
        let result = WorkflowStepEnqueueBuilder::new(
            StepKey::new("result"),
            JobType::new("jobs.test.result"),
            &payload,
        )
        .try_build()
        .expect("build result step");
        let workflow =
            WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test.result"), &metadata)
                .step(result)
                .try_result_step_key("result")
                .expect("set result step key")
                .try_build()
                .expect("build workflow");

        let canonical =
            canonical_workflow_enqueue_request(&workflow).expect("canonicalize workflow enqueue");

        assert_eq!(canonical.get("result_step_key"), Some(&json!("result")));
    }
}
