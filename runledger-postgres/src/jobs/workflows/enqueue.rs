use std::collections::{BTreeMap, BTreeSet};

use runledger_core::jobs::WorkflowDependencyReleaseMode;
use serde::Serialize;
use serde_json::Value as JsonValue;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, Result};

use super::super::row_decode::{
    parse_job_stage, parse_job_type_name, parse_workflow_run_status,
    parse_workflow_step_execution_kind, parse_workflow_type_name,
};
use super::super::workflow_types::WorkflowRunDbRecord;
use super::runtime::{recompute_workflow_run_statuses_tx, release_candidate_step_tx};
use super::{
    JobDefinitionDefaults, ensure_read_committed_tx, workflow_dag_validation_error,
    workflow_definition_not_available_error, workflow_dependency_count_overflow_error,
    workflow_enqueue_conflicting_retry_error, workflow_internal_state_error,
};
use runledger_core::jobs::{
    JobStage, WorkflowRunEnqueue, WorkflowStepEnqueue, WorkflowStepExecutionKind,
    validate_workflow_run_enqueue,
};

type DefaultsByJobType = BTreeMap<String, JobDefinitionDefaults>;
pub(crate) type WorkflowStepIdsByKey = BTreeMap<String, Uuid>;

struct WorkflowRunInsertOutcome {
    record: WorkflowRunDbRecord,
    inserted: bool,
}

#[derive(sqlx::FromRow)]
struct WorkflowRunRow {
    id: Uuid,
    workflow_type: String,
    organization_id: Option<Uuid>,
    status: String,
    idempotency_key: Option<String>,
    metadata: JsonValue,
    enqueue_request_matches: Option<bool>,
    metadata_matches: Option<bool>,
    started_at: chrono::DateTime<chrono::Utc>,
    finished_at: Option<chrono::DateTime<chrono::Utc>>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Serialize)]
struct CanonicalWorkflowRunEnqueueRequest<'a> {
    metadata: &'a JsonValue,
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
    dependencies: Vec<CanonicalWorkflowDependency<'a>>,
}

#[derive(Serialize)]
struct CanonicalWorkflowDependency<'a> {
    prerequisite_step_key: &'a str,
    release_mode: &'static str,
}

/// Enqueues a workflow run in its own transaction.
///
/// Calls without an idempotency key always create a new workflow run. Calls
/// with an idempotency key return the existing run only when the canonical
/// enqueue request snapshot matches. Keyed legacy rows created before enqueue
/// snapshots existed cannot reconstruct their original step request, so retries
/// against those rows fall back to metadata-only comparison.
pub async fn enqueue_workflow_run(
    pool: &DbPool,
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<WorkflowRunDbRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    let workflow_run = enqueue_workflow_run_tx(&mut tx, payload).await?;
    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    Ok(workflow_run)
}

/// Enqueues a workflow run and returns the existing run for an identical keyed retry.
///
/// Idempotency is strict for the submitted request snapshot. The snapshot is
/// compared instead of live workflow step rows because steps and dependencies
/// can be legitimately appended or mutated after initial enqueue. Strict
/// workflow idempotency applies to keyed rows with an `enqueue_request`
/// snapshot; unkeyed rows do not store snapshots, and legacy keyed rows created
/// before that snapshot existed can only be metadata-checked because their
/// original step request is not recoverable after later mutations.
/// Job-step stage is part of the canonical initial request after normalizing an
/// omitted stage to `Queued`; changing the requested initial stage is treated as
/// a different enqueue request.
pub async fn enqueue_workflow_run_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<WorkflowRunDbRecord> {
    validate_workflow_run_enqueue(payload).map_err(workflow_dag_validation_error)?;
    if payload.idempotency_key().is_some() {
        ensure_read_committed_tx(
            tx,
            "workflow idempotent enqueue",
            "workflow.enqueue_idempotency_unsupported_isolation",
            "Workflow idempotent enqueue requires READ COMMITTED transaction isolation.",
        )
        .await?;
    }

    let workflow_run_insert = insert_workflow_run_record_tx(tx, payload).await?;
    let workflow_run = workflow_run_insert.record;
    if !workflow_run_insert.inserted {
        // Existing idempotent runs already have their steps and initial releases
        // committed; never replay workflow initialization for a retry.
        return Ok(workflow_run);
    }

    let defaults_by_job_type = fetch_job_definition_defaults_tx(tx, payload.steps()).await?;
    let step_id_by_key =
        insert_workflow_steps_tx(tx, payload, workflow_run.id, &defaults_by_job_type).await?;
    insert_workflow_step_dependencies_tx(tx, payload, workflow_run.id, &step_id_by_key).await?;

    enqueue_root_steps_tx(tx, workflow_run.id).await?;
    recompute_workflow_run_statuses_tx(tx, &std::collections::BTreeSet::from([workflow_run.id]))
        .await?;

    load_workflow_run_by_id_tx(tx, workflow_run.id).await
}

async fn insert_workflow_run_record_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<WorkflowRunInsertOutcome> {
    let enqueue_request = payload
        .idempotency_key()
        .map(|_| canonical_workflow_enqueue_request(payload))
        .transpose()?;
    // The conflict clause is selected from static literals only; all request
    // data remains bound below. This dynamic SQL is not SQLx macro-checked, so
    // keep the returned columns and bind order aligned with WorkflowRunRow and
    // the workflow_runs insert list.
    let insert_sql = format!(
        "INSERT INTO workflow_runs (
            workflow_type,
            organization_id,
            status,
            idempotency_key,
            metadata,
            enqueue_request,
            started_at
         )
         VALUES ($1, $2, 'RUNNING', $3, $4::jsonb, $5::jsonb, now())
         {}
         RETURNING
            id,
            workflow_type,
            organization_id,
            status::text AS status,
            idempotency_key,
            metadata,
            NULL::boolean AS enqueue_request_matches,
            TRUE AS metadata_matches,
            started_at,
            finished_at,
            created_at,
            updated_at",
        enqueue_workflow_run_idempotency_conflict_clause(payload),
    );
    let run_row = sqlx::query_as::<_, WorkflowRunRow>(&insert_sql)
        .bind(payload.workflow_type())
        .bind(payload.organization_id())
        .bind(payload.idempotency_key())
        .bind(payload.metadata())
        .bind(enqueue_request.as_ref())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| Error::from_query_sqlx_with_context("enqueue workflow run", error))?;

    if let Some(run_row) = run_row {
        return Ok(WorkflowRunInsertOutcome {
            record: workflow_run_record_from_row(run_row)?,
            inserted: true,
        });
    }

    let (Some(idempotency_key), Some(enqueue_request)) =
        (payload.idempotency_key(), enqueue_request.as_ref())
    else {
        return Err(workflow_internal_state_error(
            "workflow run insert returned no row without an idempotency key conflict",
        ));
    };

    let existing =
        load_existing_idempotent_workflow_run_tx(tx, payload, idempotency_key, enqueue_request)
            .await?;
    validate_existing_idempotent_workflow_run(&existing)?;

    Ok(WorkflowRunInsertOutcome {
        record: workflow_run_record_from_row(existing)?,
        inserted: false,
    })
}

fn workflow_run_record_from_row(run_row: WorkflowRunRow) -> Result<WorkflowRunDbRecord> {
    // The retry-match flags are only validation scratch fields for idempotent
    // conflict resolution; they are not part of the persisted public run record.
    Ok(WorkflowRunDbRecord {
        id: run_row.id,
        workflow_type: parse_workflow_type_name(run_row.workflow_type)?,
        organization_id: run_row.organization_id,
        status: parse_workflow_run_status(run_row.status)?,
        idempotency_key: run_row.idempotency_key,
        metadata: run_row.metadata,
        started_at: run_row.started_at,
        finished_at: run_row.finished_at,
        created_at: run_row.created_at,
        updated_at: run_row.updated_at,
    })
}

async fn load_existing_idempotent_workflow_run_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
    idempotency_key: &str,
    enqueue_request: &JsonValue,
) -> Result<WorkflowRunRow> {
    // FOR SHARE keeps the matched run stable until the enqueue transaction
    // returns the existing idempotent result.
    let run = if let Some(organization_id) = payload.organization_id() {
        sqlx::query_as::<_, WorkflowRunRow>(
            "SELECT
                id,
                workflow_type,
                organization_id,
                status::text AS status,
                idempotency_key,
                metadata,
                enqueue_request = $4::jsonb AS enqueue_request_matches,
                metadata = $5::jsonb AS metadata_matches,
                started_at,
                finished_at,
                created_at,
                updated_at
             FROM workflow_runs
             WHERE workflow_type = $1
               AND organization_id = $2
               AND idempotency_key = $3
             LIMIT 1
             FOR SHARE",
        )
        .bind(payload.workflow_type())
        .bind(organization_id)
        .bind(idempotency_key)
        .bind(enqueue_request)
        .bind(payload.metadata())
        .fetch_optional(&mut **tx)
        .await
    } else {
        sqlx::query_as::<_, WorkflowRunRow>(
            "SELECT
                id,
                workflow_type,
                organization_id,
                status::text AS status,
                idempotency_key,
                metadata,
                enqueue_request = $3::jsonb AS enqueue_request_matches,
                metadata = $4::jsonb AS metadata_matches,
                started_at,
                finished_at,
                created_at,
                updated_at
             FROM workflow_runs
             WHERE workflow_type = $1
               AND organization_id IS NULL
               AND idempotency_key = $2
             LIMIT 1
             FOR SHARE",
        )
        .bind(payload.workflow_type())
        .bind(idempotency_key)
        .bind(enqueue_request)
        .bind(payload.metadata())
        .fetch_optional(&mut **tx)
        .await
    };

    run.map_err(|error| {
        Error::from_query_sqlx_with_context("load idempotent workflow enqueue", error)
    })?
    .ok_or_else(|| {
        workflow_internal_state_error(
            "workflow run insert conflicted but matching idempotent workflow run was not found",
        )
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

fn validate_existing_idempotent_workflow_run(existing: &WorkflowRunRow) -> Result<()> {
    match existing.enqueue_request_matches {
        Some(true) => Ok(()),
        Some(false) => Err(workflow_enqueue_conflicting_retry_error("request")),
        None => {
            // Legacy rows created before enqueue_request existed have no stable
            // original request snapshot. Live workflow_steps are mutable through
            // append and pending-payload updates, so accepting the existing run is
            // safer than rejecting a legitimate retry against changed state.
            if existing.metadata_matches != Some(true) {
                return Err(workflow_enqueue_conflicting_retry_error("metadata"));
            }
            tracing::warn!(
                workflow_run_id = %existing.id,
                workflow_type = existing.workflow_type.as_str(),
                organization_id = ?existing.organization_id,
                "accepted legacy workflow idempotency retry without enqueue_request snapshot"
            );
            Ok(())
        }
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
                dependencies,
            }
        })
        .collect::<Vec<_>>();
    steps.sort_by(|left, right| left.step_key.cmp(right.step_key));

    serde_json::to_value(CanonicalWorkflowRunEnqueueRequest {
        metadata: payload.metadata(),
        steps,
    })
    .map_err(|error| {
        workflow_internal_state_error(format!(
            "failed to serialize canonical workflow enqueue request: {error}"
        ))
    })
}

pub(crate) fn dependency_count_total(step: &WorkflowStepEnqueue<'_>) -> Result<i32> {
    i32::try_from(step.dependencies().len())
        .map_err(|_| workflow_dependency_count_overflow_error(step.step_key().as_str()))
}

pub(crate) fn workflow_step_effective_organization_id(
    workflow_organization_id: Option<Uuid>,
    step: &WorkflowStepEnqueue<'_>,
) -> Option<Uuid> {
    step.organization_id().or(workflow_organization_id)
}

pub(crate) fn workflow_step_effective_stage(
    step: &WorkflowStepEnqueue<'_>,
) -> Option<&'static str> {
    match step.execution_kind() {
        WorkflowStepExecutionKind::Job => {
            Some(step.stage().unwrap_or(JobStage::Queued).as_db_value())
        }
        WorkflowStepExecutionKind::External => None,
    }
}

pub(crate) fn workflow_step_defaults<'a>(
    defaults_by_job_type: &'a DefaultsByJobType,
    step: &WorkflowStepEnqueue<'_>,
) -> Result<&'a JobDefinitionDefaults> {
    let job_type = step
        .job_type()
        .ok_or_else(|| workflow_internal_state_error("job workflow step missing job_type"))?;

    defaults_by_job_type
        .get(job_type.as_str())
        .ok_or_else(|| workflow_definition_not_available_error(job_type.as_str()))
}

pub(crate) async fn insert_workflow_step_record_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    organization_id: Option<Uuid>,
    step: &WorkflowStepEnqueue<'_>,
    defaults: Option<&JobDefinitionDefaults>,
    dependency_count_pending: i32,
    dependency_count_unsatisfied: i32,
) -> Result<Uuid> {
    let dependency_count_total = dependency_count_total(step)?;
    let (job_type, priority, max_attempts, timeout_seconds, stage) = match step.execution_kind() {
        WorkflowStepExecutionKind::Job => {
            let defaults = defaults.ok_or_else(|| {
                workflow_internal_state_error("missing job definition defaults for job step")
            })?;
            let job_type = step.job_type().ok_or_else(|| {
                workflow_internal_state_error("missing job_type for job workflow step")
            })?;

            (
                Some(job_type.as_str()),
                Some(step.priority().unwrap_or(defaults.default_priority)),
                Some(step.max_attempts().unwrap_or(defaults.max_attempts)),
                Some(
                    step.timeout_seconds()
                        .unwrap_or(defaults.default_timeout_seconds),
                ),
                workflow_step_effective_stage(step),
            )
        }
        WorkflowStepExecutionKind::External => (None, None, None, None, None),
    };
    let step_id: Uuid = sqlx::query_scalar!(
        "INSERT INTO workflow_steps (
            workflow_run_id,
            step_key,
            execution_kind,
            job_type,
            organization_id,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            stage,
            status,
            dependency_count_total,
            dependency_count_pending,
            dependency_count_unsatisfied
         )
         VALUES (
            $1,
            $2,
            $3::text::workflow_step_execution_kind,
            $4,
            $5,
            $6::jsonb,
            $7,
            $8,
            $9,
            $10,
            'BLOCKED',
            $11,
            $12,
            $13
         )
         RETURNING id",
        workflow_run_id,
        step.step_key() as _,
        step.execution_kind().as_db_value(),
        job_type,
        organization_id,
        step.payload(),
        priority,
        max_attempts,
        timeout_seconds,
        stage,
        dependency_count_total,
        dependency_count_pending,
        dependency_count_unsatisfied,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert workflow step", error))?;

    Ok(step_id)
}

pub(crate) async fn insert_workflow_steps_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
    workflow_run_id: Uuid,
    defaults_by_job_type: &DefaultsByJobType,
) -> Result<WorkflowStepIdsByKey> {
    let mut step_id_by_key = WorkflowStepIdsByKey::new();
    for step in payload.steps() {
        let defaults = match step.execution_kind() {
            WorkflowStepExecutionKind::Job => {
                Some(workflow_step_defaults(defaults_by_job_type, step)?)
            }
            WorkflowStepExecutionKind::External => None,
        };
        let step_id = insert_workflow_step_record_tx(
            tx,
            workflow_run_id,
            workflow_step_effective_organization_id(payload.organization_id(), step),
            step,
            defaults,
            dependency_count_total(step)?,
            0,
        )
        .await?;
        step_id_by_key.insert(step.step_key().as_str().to_owned(), step_id);
    }

    Ok(step_id_by_key)
}

pub(crate) fn step_id_for_key(
    step_id_by_key: &WorkflowStepIdsByKey,
    step_key: &str,
    missing_error: &'static str,
) -> Result<Uuid> {
    step_id_by_key
        .get(step_key)
        .copied()
        .ok_or_else(|| workflow_internal_state_error(missing_error))
}

pub(crate) async fn insert_workflow_step_dependency_record_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    prerequisite_step_id: Uuid,
    dependent_step_id: Uuid,
    release_mode: &str,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO workflow_step_dependencies (
            workflow_run_id,
            prerequisite_step_id,
            dependent_step_id,
            release_mode
         )
         VALUES ($1, $2, $3, $4::text::workflow_dependency_release_mode)",
        workflow_run_id,
        prerequisite_step_id,
        dependent_step_id,
        release_mode,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("insert workflow step dependency", error)
    })?;

    Ok(())
}

pub(crate) async fn insert_workflow_step_dependencies_tx(
    tx: &mut DbTx<'_>,
    payload: &WorkflowRunEnqueue<'_>,
    workflow_run_id: Uuid,
    step_id_by_key: &WorkflowStepIdsByKey,
) -> Result<()> {
    for step in payload.steps() {
        let dependent_step_id = step_id_for_key(
            step_id_by_key,
            step.step_key().as_str(),
            "missing dependent workflow step id",
        )?;
        for dependency in step.dependencies() {
            let prerequisite_step_id = step_id_for_key(
                step_id_by_key,
                dependency.prerequisite_step_key.as_str(),
                "missing prerequisite workflow step id",
            )?;
            let release_mode = dependency
                .release_mode
                .unwrap_or(WorkflowDependencyReleaseMode::OnTerminal)
                .as_db_value();
            insert_workflow_step_dependency_record_tx(
                tx,
                workflow_run_id,
                prerequisite_step_id,
                dependent_step_id,
                release_mode,
            )
            .await?;
        }
    }

    Ok(())
}

pub(crate) async fn fetch_job_definition_defaults_tx(
    tx: &mut DbTx<'_>,
    steps: &[WorkflowStepEnqueue<'_>],
) -> Result<DefaultsByJobType> {
    let job_types: Vec<String> = steps
        .iter()
        .filter(|step| step.execution_kind() == WorkflowStepExecutionKind::Job)
        .map(|step| {
            step.job_type()
                .map(|job_type| job_type.as_str().to_owned())
                .ok_or_else(|| workflow_internal_state_error("job workflow step missing job_type"))
        })
        .collect::<Result<BTreeSet<_>>>()?
        .into_iter()
        .collect();

    let rows = sqlx::query!(
        "SELECT job_type, default_priority, max_attempts, default_timeout_seconds
         FROM job_definitions
         WHERE is_enabled = true
           AND job_type = ANY($1::text[])",
        &job_types,
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("lookup workflow step job definition defaults", error)
    })?;

    let defaults_by_job_type: DefaultsByJobType = rows
        .into_iter()
        .map(|row| {
            (
                row.job_type,
                JobDefinitionDefaults {
                    default_priority: row.default_priority,
                    max_attempts: row.max_attempts,
                    default_timeout_seconds: row.default_timeout_seconds,
                },
            )
        })
        .collect();

    if let Some(step) = steps
        .iter()
        .filter(|step| step.execution_kind() == WorkflowStepExecutionKind::Job)
        .find(|step| {
            step.job_type()
                .is_none_or(|job_type| !defaults_by_job_type.contains_key(job_type.as_str()))
        })
    {
        return Err(workflow_definition_not_available_error(
            step.job_type()
                .map(|job_type| job_type.as_str())
                .unwrap_or("<missing-job-type>"),
        ));
    }

    Ok(defaults_by_job_type)
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
            stage
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
        let candidate = super::StepReleaseCandidate {
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
        };
        release_candidate_step_tx(tx, &candidate).await?;
    }

    Ok(())
}

pub(crate) async fn load_workflow_run_by_id_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
) -> Result<WorkflowRunDbRecord> {
    let run_row = sqlx::query!(
        "SELECT
            id,
            workflow_type,
            organization_id,
            status::text AS \"status!\",
            idempotency_key,
            metadata,
            started_at,
            finished_at,
            created_at,
            updated_at
         FROM workflow_runs
         WHERE id = $1",
        workflow_run_id,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("load workflow run after enqueue recompute", error)
    })?;

    Ok(WorkflowRunDbRecord {
        id: run_row.id,
        workflow_type: parse_workflow_type_name(run_row.workflow_type)?,
        organization_id: run_row.organization_id,
        status: parse_workflow_run_status(run_row.status)?,
        idempotency_key: run_row.idempotency_key,
        metadata: run_row.metadata,
        started_at: run_row.started_at,
        finished_at: run_row.finished_at,
        created_at: run_row.created_at,
        updated_at: run_row.updated_at,
    })
}
