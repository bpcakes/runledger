use runledger_core::jobs::{
    WorkflowDependencyReleaseMode, WorkflowRunEnqueue, validate_workflow_run_enqueue,
};
use serde::Serialize;
use serde_json::Value as JsonValue;
use sqlx::types::Uuid;

use crate::jobs::transaction_isolation::ensure_read_committed_tx;
use crate::{DbPool, DbTx, Error, Result};

use super::super::row_decode::{
    parse_job_stage, parse_job_type_name, parse_workflow_step_execution_kind,
};
use super::super::rows::WorkflowRunEnqueueRow;
use super::super::workflow_types::{EnqueueActiveWorkflowOutcome, WorkflowRunDbRecord};
use super::errors::{
    workflow_active_key_api_required_error, workflow_active_key_required_error,
    workflow_enqueue_conflicting_retry_error, workflow_internal_state_error,
    workflow_legacy_idempotency_snapshot_missing_error,
};
use super::is_false;
use super::read::load_workflow_run_by_id_tx;
use super::release::{StepReleaseCandidate, StepReleaseCandidateInit, release_candidate_step_tx};
use super::runtime::recompute_workflow_run_statuses_tx;
use super::steps::{
    fetch_job_definition_defaults_tx, insert_workflow_step_dependencies_tx,
    insert_workflow_steps_tx, workflow_step_effective_organization_id,
    workflow_step_effective_stage,
};
use super::validation::workflow_dag_validation_error;

struct WorkflowRunInsertOutcome {
    record: WorkflowRunDbRecord,
    inserted: bool,
}

#[derive(Serialize)]
struct CanonicalWorkflowRunEnqueueRequest<'a> {
    metadata: &'a JsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_key: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result_step_key: Option<&'a str>,
    steps: Vec<CanonicalWorkflowStep<'a>>,
}

#[derive(Serialize)]
struct CanonicalWorkflowStep<'a> {
    step_key: &'a str,
    execution_kind: &'static str,
    job_type: Option<&'a str>,
    organization_id: Option<Uuid>,
    payload: &'a JsonValue,
    priority: Option<i32>,
    max_attempts: Option<i32>,
    timeout_seconds: Option<i32>,
    stage: Option<&'static str>,
    #[serde(skip_serializing_if = "is_false")]
    allow_handler_continuation: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    execution_resource_key: Option<&'a str>,
    dependencies: Vec<CanonicalWorkflowDependency<'a>>,
}

#[derive(Serialize)]
struct CanonicalWorkflowDependency<'a> {
    prerequisite_step_key: &'a str,
    release_mode: &'static str,
}

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
        ensure_read_committed_tx(
            tx,
            "workflow coordinated enqueue",
            "workflow.enqueue_idempotency_unsupported_isolation",
            "Workflow idempotent or active-key enqueue requires READ COMMITTED transaction isolation.",
        )
        .await?;
    }
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
    insert_workflow_step_dependencies_tx(tx, payload, workflow_run.id, &step_id_by_key).await?;

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

fn workflow_active_claim_scope(organization_id: Option<Uuid>) -> String {
    organization_id.map_or_else(
        || "global".to_owned(),
        |organization_id| format!("organization:{organization_id}"),
    )
}

async fn lock_workflow_active_key_tx(
    tx: &mut DbTx<'_>,
    organization_id: Option<Uuid>,
    active_key: &str,
) -> Result<()> {
    let scope = workflow_active_claim_scope(organization_id);
    // Hash collisions only over-serialize; the (scope, active_key) primary key
    // remains the authoritative ownership guard.
    sqlx::query_scalar::<_, ()>("SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))")
        .bind(&scope)
        .bind(active_key)
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| Error::from_query_sqlx_with_context("lock workflow active key", error))?;
    Ok(())
}

async fn load_existing_active_workflow_run_tx(
    tx: &mut DbTx<'_>,
    organization_id: Option<Uuid>,
    active_key: &str,
) -> Result<Option<WorkflowRunDbRecord>> {
    let scope = workflow_active_claim_scope(organization_id);
    let workflow_run_id = sqlx::query_scalar::<_, Uuid>(
        "SELECT workflow_run_id
         FROM workflow_active_claims
         WHERE scope = $1
           AND active_key = $2
         FOR UPDATE",
    )
    .bind(&scope)
    .bind(active_key)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("load existing workflow active claim", error)
    })?;

    let Some(workflow_run_id) = workflow_run_id else {
        return Ok(None);
    };
    load_workflow_run_by_id_tx(tx, workflow_run_id, "load existing active workflow run")
        .await
        .map(Some)
}

async fn insert_workflow_active_claim_tx(
    tx: &mut DbTx<'_>,
    organization_id: Option<Uuid>,
    active_key: &str,
    workflow_run_id: Uuid,
) -> Result<()> {
    let scope = workflow_active_claim_scope(organization_id);
    sqlx::query(
        "INSERT INTO workflow_active_claims (
            scope,
            active_key,
            workflow_run_id,
            release_pending
         )
         VALUES ($1, $2, $3, false)",
    )
    .bind(&scope)
    .bind(active_key)
    .bind(workflow_run_id)
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert workflow active claim", error))?;
    Ok(())
}

async fn insert_workflow_run_record_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
    enqueue_request: &JsonValue,
) -> Result<WorkflowRunInsertOutcome> {
    // Every new workflow stores its canonical request so a future recovery can
    // reconstruct the original DAG safely. Idempotency still only applies when
    // an idempotency key is present.
    // The conflict clause is selected from static literals only; all request
    // data remains bound below. This dynamic SQL is not SQLx macro-checked, so
    // keep the returned columns and bind order aligned with WorkflowRunEnqueueRow
    // and the workflow_runs insert list.
    let insert_sql = format!(
        "INSERT INTO workflow_runs (
            workflow_type,
            organization_id,
            status,
            idempotency_key,
            result_step_key,
            metadata,
            enqueue_request,
            started_at
         )
         VALUES ($1, $2, 'RUNNING', $3, $4, $5::jsonb, $6::jsonb, now())
         {}
         RETURNING
            id,
            workflow_type,
            organization_id,
            status::text AS status,
            idempotency_key,
            result_step_key,
            metadata,
            NULL::boolean AS enqueue_request_matches,
            started_at,
            finished_at,
            created_at,
            updated_at",
        enqueue_workflow_run_idempotency_conflict_clause(payload),
    );
    let run_row = sqlx::query_as::<_, WorkflowRunEnqueueRow>(&insert_sql)
        .bind(payload.workflow_type())
        .bind(payload.organization_id())
        .bind(payload.idempotency_key())
        .bind(payload.result_step_key().map(|step_key| step_key.as_str()))
        .bind(payload.metadata())
        .bind(enqueue_request)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| Error::from_query_sqlx_with_context("enqueue workflow run", error))?;

    if let Some(run_row) = run_row {
        return Ok(WorkflowRunInsertOutcome {
            record: run_row.into_record()?,
            inserted: true,
        });
    }

    let Some(idempotency_key) = payload.idempotency_key() else {
        return Err(workflow_internal_state_error(
            "workflow run insert returned no row without an idempotency key conflict",
        ));
    };

    let existing =
        load_existing_idempotent_workflow_run_tx(tx, payload, idempotency_key, enqueue_request)
            .await?;
    validate_existing_idempotent_workflow_run(&existing)?;

    Ok(WorkflowRunInsertOutcome {
        record: existing.into_record()?,
        inserted: false,
    })
}

async fn load_existing_idempotent_workflow_run_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
    idempotency_key: &str,
    enqueue_request: &JsonValue,
) -> Result<WorkflowRunEnqueueRow> {
    try_load_existing_idempotent_workflow_run_tx(tx, payload, idempotency_key, enqueue_request)
        .await?
        .ok_or_else(|| {
            workflow_internal_state_error(
                "workflow run insert conflicted but matching idempotent workflow run was not found",
            )
        })
}

async fn try_load_existing_idempotent_workflow_run_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
    idempotency_key: &str,
    enqueue_request: &JsonValue,
) -> Result<Option<WorkflowRunEnqueueRow>> {
    // Lock a matched committed row while the enqueue transaction compares and
    // returns the idempotent result.
    let run = if let Some(organization_id) = payload.organization_id() {
        sqlx::query_as!(
            WorkflowRunEnqueueRow,
            r#"SELECT
                id,
                workflow_type,
                organization_id,
                status::text AS "status!",
                idempotency_key,
                result_step_key,
                metadata,
                enqueue_request = $4::jsonb AS "enqueue_request_matches?",
                started_at,
                finished_at,
                created_at,
                updated_at
             FROM workflow_runs
             WHERE workflow_type = $1
               AND organization_id = $2
               AND idempotency_key = $3
             LIMIT 1
             FOR SHARE"#,
            payload.workflow_type() as _,
            organization_id,
            idempotency_key,
            enqueue_request,
        )
        .fetch_optional(&mut **tx)
        .await
    } else {
        sqlx::query_as!(
            WorkflowRunEnqueueRow,
            r#"SELECT
                id,
                workflow_type,
                organization_id,
                status::text AS "status!",
                idempotency_key,
                result_step_key,
                metadata,
                enqueue_request = $3::jsonb AS "enqueue_request_matches?",
                started_at,
                finished_at,
                created_at,
                updated_at
             FROM workflow_runs
             WHERE workflow_type = $1
               AND organization_id IS NULL
               AND idempotency_key = $2
             LIMIT 1
             FOR SHARE"#,
            payload.workflow_type() as _,
            idempotency_key,
            enqueue_request,
        )
        .fetch_optional(&mut **tx)
        .await
    };

    run.map_err(|error| {
        Error::from_query_sqlx_with_context("load idempotent workflow enqueue", error)
    })
}

fn enqueue_workflow_run_idempotency_conflict_clause(
    payload: &WorkflowRunEnqueue<'_>,
) -> &'static str {
    // Keep these predicates aligned with the partial unique indexes
    // uq_workflow_runs_type_idempotency_org and uq_workflow_runs_type_idempotency_global.
    match (payload.idempotency_key(), payload.organization_id()) {
        (Some(_), Some(_)) => {
            "ON CONFLICT (workflow_type, organization_id, idempotency_key)
             WHERE idempotency_key IS NOT NULL
               AND organization_id IS NOT NULL
             DO NOTHING"
        }
        (Some(_), None) => {
            "ON CONFLICT (workflow_type, idempotency_key)
             WHERE idempotency_key IS NOT NULL
               AND organization_id IS NULL
             DO NOTHING"
        }
        (None, _) => "",
    }
}

fn validate_existing_idempotent_workflow_run(existing: &WorkflowRunEnqueueRow) -> Result<()> {
    match existing.enqueue_request_matches() {
        Some(true) => Ok(()),
        Some(false) => Err(workflow_enqueue_conflicting_retry_error("request")),
        None => Err(workflow_legacy_idempotency_snapshot_missing_error(
            existing.workflow_type.as_str(),
            existing.id,
        )),
    }
}

fn canonical_workflow_enqueue_request(payload: &WorkflowRunEnqueue<'_>) -> Result<JsonValue> {
    let mut steps = payload
        .steps()
        .iter()
        .map(|step| {
            let mut dependencies = step
                .dependencies()
                .iter()
                .map(|dependency| CanonicalWorkflowDependency {
                    prerequisite_step_key: dependency.prerequisite_step_key.as_str(),
                    release_mode: dependency
                        .release_mode
                        .unwrap_or(WorkflowDependencyReleaseMode::OnTerminal)
                        .as_db_value(),
                })
                .collect::<Vec<_>>();
            dependencies.sort_by(|left, right| {
                left.prerequisite_step_key
                    .cmp(right.prerequisite_step_key)
                    .then(left.release_mode.cmp(right.release_mode))
            });

            CanonicalWorkflowStep {
                step_key: step.step_key().as_str(),
                execution_kind: step.execution_kind().as_db_value(),
                job_type: step.job_type().map(|job_type| job_type.as_str()),
                organization_id: workflow_step_effective_organization_id(
                    payload.organization_id(),
                    step,
                ),
                payload: step.payload(),
                priority: step.priority(),
                max_attempts: step.max_attempts(),
                timeout_seconds: step.timeout_seconds(),
                stage: workflow_step_effective_stage(step),
                allow_handler_continuation: step.allows_handler_continuation(),
                execution_resource_key: step.execution_resource_key(),
                dependencies,
            }
        })
        .collect::<Vec<_>>();
    steps.sort_by(|left, right| left.step_key.cmp(right.step_key));

    serde_json::to_value(CanonicalWorkflowRunEnqueueRequest {
        metadata: payload.metadata(),
        active_key: payload.active_key(),
        result_step_key: payload.result_step_key().map(|step_key| step_key.as_str()),
        steps,
    })
    .map_err(|error| {
        workflow_internal_state_error(format!(
            "failed to serialize canonical workflow enqueue request: {error}"
        ))
    })
}

pub(crate) async fn enqueue_root_steps_tx(tx: &mut DbTx<'_>, workflow_run_id: Uuid) -> Result<()> {
    let rows = sqlx::query!(
        "SELECT
            id,
            workflow_run_id,
            execution_kind::text AS \"execution_kind!\",
            job_type,
            organization_id,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            stage,
            execution_resource_key
         FROM workflow_steps
         WHERE workflow_run_id = $1
           AND status = 'BLOCKED'
           AND dependency_count_pending = 0
         ORDER BY created_at ASC
         FOR UPDATE",
        workflow_run_id,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lookup root workflow steps for enqueue", error)
    })?;

    for row in rows {
        let candidate = StepReleaseCandidate::from_init(StepReleaseCandidateInit {
            id: row.id,
            workflow_run_id: row.workflow_run_id,
            execution_kind: parse_workflow_step_execution_kind(row.execution_kind)?,
            job_type: row.job_type.map(parse_job_type_name).transpose()?,
            organization_id: row.organization_id,
            payload: row.payload,
            priority: row.priority,
            max_attempts: row.max_attempts,
            timeout_seconds: row.timeout_seconds,
            stage: row.stage.map(parse_job_stage).transpose()?,
            execution_resource_key: row.execution_resource_key,
        });
        release_candidate_step_tx(tx, &candidate).await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use runledger_core::jobs::{
        JobStage, JobType, StepKey, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder,
        WorkflowType,
    };
    use serde_json::json;
    use sqlx::types::Uuid;

    use super::canonical_workflow_enqueue_request;

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
