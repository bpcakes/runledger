use std::collections::BTreeMap;

use runledger_core::jobs::{
    JobStage, JobType, StepKey, WorkflowDependencyReleaseMode, WorkflowRunEnqueueBuilder,
    WorkflowRunStatus, WorkflowStepEnqueue, WorkflowStepEnqueueBuilder, WorkflowStepExecutionKind,
    WorkflowType,
};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::jobs::transaction_isolation::{
    ReadCommittedTx, begin_owned_read_committed_tx, ensure_read_committed_tx,
    finish_owned_transaction,
};
use crate::{DbPool, DbTx, Error, QueryError, QueryErrorCategory, Result};

use super::super::workflow_types::{
    EnqueueActiveWorkflowOutcome, WorkflowRecoveryDisposition, WorkflowRecoveryOutcome,
    WorkflowRecoveryRequest, WorkflowRunDbRecord,
};
use super::enqueue::{enqueue_or_get_active_workflow_tx, enqueue_workflow_run_tx};
use super::read::load_workflow_run_by_id_tx;
use super::snapshot::{
    StoredCanonicalWorkflowDependency, StoredCanonicalWorkflowStep,
    deserialize_recovery_append_snapshot, deserialize_recovery_enqueue_snapshot,
};

#[derive(sqlx::FromRow)]
struct RecoverySourceRow {
    workflow_type: String,
    organization_id: Option<Uuid>,
    status: String,
    enqueue_request: Option<Value>,
}

#[derive(sqlx::FromRow)]
struct ExistingRecoveryRow {
    recovery_run_id: Uuid,
    source_step_id: Option<Uuid>,
    mode: String,
    reason: String,
}

#[derive(sqlx::FromRow)]
struct RecoveryMutationRow {
    mutation_kind: String,
    request: Value,
}

#[derive(sqlx::FromRow)]
struct RecoverySourceStepState {
    step_key: String,
    execution_kind: String,
    payload: Value,
    priority: Option<i32>,
    max_attempts: Option<i32>,
    timeout_seconds: Option<i32>,
    allow_handler_continuation: bool,
    execution_resource_key: Option<String>,
}

struct RecoveryBlueprint {
    workflow_type: String,
    organization_id: Option<Uuid>,
    metadata: Value,
    active_key: Option<String>,
    result_step_key: Option<String>,
    steps: Vec<RecoveryStep>,
}

struct RecoveryStep {
    canonical: StoredCanonicalWorkflowStep,
    source_state: RecoverySourceStepState,
}

#[derive(Debug)]
struct ValidatedRecoveryStep<'a> {
    step_key: &'a str,
    payload: &'a Value,
    organization_id: Option<Uuid>,
    stage: Option<JobStage>,
    allow_handler_continuation: bool,
    execution_resource_key: Option<&'a str>,
    dependencies: ValidatedRecoveryDependencies<'a>,
    kind: ValidatedRecoveryStepKind<'a>,
}

#[derive(Debug)]
enum ValidatedRecoveryStepKind<'a> {
    Job {
        job_type: &'a str,
        priority: i32,
        max_attempts: i32,
        timeout_seconds: i32,
    },
    External,
}

// Keeps snapshot dependency storage borrowed while proving every release mode
// is closed before enqueue construction.
#[derive(Clone, Copy, Debug)]
struct ValidatedRecoveryDependencies<'a>(&'a [StoredCanonicalWorkflowDependency]);

#[derive(Clone, Copy, Debug)]
struct ValidatedRecoveryDependency<'a> {
    prerequisite_step_key: &'a str,
    release_mode: WorkflowDependencyReleaseMode,
}

impl<'a> ValidatedRecoveryDependencies<'a> {
    fn validate(
        step_key: &str,
        dependencies: &'a [StoredCanonicalWorkflowDependency],
    ) -> Result<Self> {
        for dependency in dependencies {
            if WorkflowDependencyReleaseMode::from_db_value(&dependency.release_mode).is_none() {
                return Err(unsafe_recovery_snapshot_error(format!(
                    "unknown release mode for step {step_key}"
                )));
            }
        }
        Ok(Self(dependencies))
    }

    fn iter(self) -> impl Iterator<Item = ValidatedRecoveryDependency<'a>> {
        self.0.iter().map(|dependency| ValidatedRecoveryDependency {
            prerequisite_step_key: &dependency.prerequisite_step_key,
            release_mode: match dependency.release_mode.as_str() {
                "ON_SUCCESS" => WorkflowDependencyReleaseMode::OnSuccess,
                "ON_TERMINAL" => WorkflowDependencyReleaseMode::OnTerminal,
                _ => unreachable!(
                    "recovery dependency release modes are validated before conversion"
                ),
            },
        })
    }
}

impl RecoveryBlueprint {
    async fn load_tx(
        tx: &mut DbTx<'_>,
        source_run_id: Uuid,
        source: RecoverySourceRow,
    ) -> Result<Self> {
        let RecoverySourceRow {
            workflow_type,
            organization_id,
            status: _,
            enqueue_request,
        } = source;
        let enqueue_request = enqueue_request.ok_or_else(|| {
            recovery_snapshot_missing_error(source_run_id, "canonical enqueue request")
        })?;
        let (metadata, active_key, result_step_key, initial_steps) =
            deserialize_recovery_enqueue_snapshot(enqueue_request)
                .map_err(|error| {
                    unsafe_recovery_snapshot_error(format!("invalid enqueue snapshot: {error}"))
                })?
                .into_parts();
        let mutations = sqlx::query_as::<_, RecoveryMutationRow>(
            "SELECT mutation_kind, request
             FROM workflow_run_mutations
             WHERE workflow_run_id = $1
             ORDER BY created_at ASC, id ASC",
        )
        .bind(source_run_id)
        .fetch_all(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("load workflow recovery append history", error)
        })?;

        let mut steps_by_key = BTreeMap::new();
        for step in initial_steps {
            insert_recovery_step(&mut steps_by_key, step)?;
        }
        for mutation in mutations {
            if mutation.mutation_kind != "APPEND_STEPS" {
                return Err(unsafe_recovery_snapshot_error(format!(
                    "unsupported workflow mutation kind {}",
                    mutation.mutation_kind
                )));
            }
            let append =
                deserialize_recovery_append_snapshot(mutation.request).map_err(|error| {
                    unsafe_recovery_snapshot_error(format!("invalid append snapshot: {error}"))
                })?;
            for step in append.into_steps() {
                insert_recovery_step(&mut steps_by_key, step)?;
            }
        }

        Ok(Self {
            workflow_type,
            organization_id,
            metadata,
            active_key,
            result_step_key,
            steps: pair_recovery_steps(
                steps_by_key,
                load_recovery_source_step_state_tx(tx, source_run_id).await?,
            )?,
        })
    }

    async fn enqueue_tx(&self, tx: &mut DbTx<'_>) -> Result<WorkflowRunDbRecord> {
        let enqueue = self.build_enqueue()?;
        if self.active_key.is_some() {
            match enqueue_or_get_active_workflow_tx(tx, &enqueue).await? {
                EnqueueActiveWorkflowOutcome::Inserted(run) => Ok(run),
                EnqueueActiveWorkflowOutcome::ExistingActive(run) => {
                    Err(recovery_active_constraint_error(run.id))
                }
                EnqueueActiveWorkflowOutcome::ExistingIdempotent(_) => {
                    Err(unsafe_recovery_snapshot_error(
                        "recovery enqueue unexpectedly matched an idempotency key",
                    ))
                }
            }
        } else {
            enqueue_workflow_run_tx(tx, &enqueue).await
        }
    }

    fn build_enqueue(&self) -> Result<runledger_core::jobs::WorkflowRunEnqueue<'_>> {
        let Self {
            workflow_type,
            organization_id,
            metadata,
            active_key,
            result_step_key,
            steps,
        } = self;
        let mut replay_steps = Vec::<WorkflowStepEnqueue<'_>>::with_capacity(steps.len());
        for step in steps {
            let validated_step = validate_recovery_step(step)?;
            replay_steps.push(build_recovery_step_enqueue(validated_step)?);
        }

        let mut builder =
            WorkflowRunEnqueueBuilder::new(WorkflowType::new(workflow_type), metadata);
        if let Some(organization_id) = organization_id {
            builder = builder.organization_id(*organization_id);
        }
        if let Some(active_key) = active_key.as_deref() {
            builder = builder.active_key(active_key);
        }
        if let Some(result_step_key) = result_step_key.as_deref() {
            builder = builder.result_step_key(StepKey::new(result_step_key));
        }
        for step in replay_steps {
            builder = builder.step(step);
        }
        builder
            .try_build()
            .map_err(|error| unsafe_recovery_snapshot_error(error.to_string()))
    }
}

/// Replays a terminal workflow as a new lineage-linked run in an internally
/// owned `READ COMMITTED` transaction.
///
/// Recovery reconstructs the canonical enqueue request plus committed append
/// history, then overlays each source step's latest persisted payload and
/// resolved queue settings. Active-workflow constraints, execution resources,
/// result selection, and handler-continuation opt-ins are preserved. The new
/// run uses recovery request identity rather than the source's permanent
/// workflow idempotency key. The source run and steps are never reopened or
/// mutated.
///
/// `(source_run_id, request_key)` is an idempotency boundary. An identical
/// retry returns [`WorkflowRecoveryDisposition::Existing`]; reusing the key
/// with a different source-step context, mode, or reason is a conflict. Legacy
/// sources without a safe canonical snapshot, snapshots with unknown fields,
/// and unsupported mutation kinds are rejected.
pub async fn recover_workflow_run(
    pool: &DbPool,
    request: &WorkflowRecoveryRequest<'_>,
) -> Result<WorkflowRecoveryOutcome> {
    const OPERATION: &str = "workflow recovery";

    validate_recovery_request(request)?;
    let mut tx = begin_owned_read_committed_tx(pool, OPERATION).await?;
    let result = {
        let mut read_committed_tx = tx.as_read_committed_tx();
        recover_workflow_run_read_committed_tx(&mut read_committed_tx, request).await
    };
    finish_owned_transaction(tx, OPERATION, result).await
}

/// Transactional counterpart to [`recover_workflow_run`].
///
/// The caller transaction must use `READ COMMITTED`. This function neither
/// commits nor rolls back. Equal recovery requests that wait behind one another
/// rely on a fresh `READ COMMITTED` snapshot to load the committed lineage.
pub async fn recover_workflow_run_tx(
    tx: &mut DbTx<'_>,
    request: &WorkflowRecoveryRequest<'_>,
) -> Result<WorkflowRecoveryOutcome> {
    validate_recovery_request(request)?;
    let mut read_committed_tx = ensure_read_committed_tx(
        tx,
        "workflow recovery",
        "workflow.recovery_unsupported_isolation",
        "Workflow recovery requires READ COMMITTED transaction isolation.",
    )
    .await?;

    recover_workflow_run_read_committed_tx(&mut read_committed_tx, request).await
}

async fn recover_workflow_run_read_committed_tx(
    tx: &mut ReadCommittedTx<'_, '_>,
    request: &WorkflowRecoveryRequest<'_>,
) -> Result<WorkflowRecoveryOutcome> {
    let tx = tx.as_tx();
    // The source lock serializes equal request keys and freezes append/cancel
    // mutations while the canonical snapshots are reconstructed.
    let source = lock_recovery_source_tx(tx, request).await?;

    if let Some(existing) =
        load_existing_recovery_tx(tx, request.source_run_id, request.request_key).await?
    {
        validate_existing_recovery(request, &existing)?;
        let run = load_workflow_run_by_id_tx(
            tx,
            existing.recovery_run_id,
            "load idempotent workflow recovery run",
        )
        .await?;
        return Ok(WorkflowRecoveryOutcome {
            run,
            disposition: WorkflowRecoveryDisposition::Existing,
        });
    }

    let status = WorkflowRunStatus::from_db_value(&source.status)
        .ok_or_else(|| unsafe_recovery_snapshot_error("source workflow status is unknown"))?;
    if !status.is_terminal() {
        return Err(recovery_source_not_terminal_error(
            request.source_run_id,
            &source.status,
        ));
    }
    if let Some(source_step_id) = request.source_step_id {
        ensure_source_step_belongs_to_run_tx(tx, request.source_run_id, source_step_id).await?;
    }

    let blueprint = RecoveryBlueprint::load_tx(tx, request.source_run_id, source).await?;
    let run = blueprint.enqueue_tx(tx).await?;
    insert_recovery_lineage_tx(tx, run.id, request).await?;

    Ok(WorkflowRecoveryOutcome {
        run,
        disposition: WorkflowRecoveryDisposition::Inserted,
    })
}

async fn lock_recovery_source_tx(
    tx: &mut DbTx<'_>,
    request: &WorkflowRecoveryRequest<'_>,
) -> Result<RecoverySourceRow> {
    sqlx::query_as::<_, RecoverySourceRow>(
        "SELECT
            workflow_type,
            organization_id,
            status::text AS status,
            enqueue_request
         FROM workflow_runs
         WHERE id = $1
           AND organization_id IS NOT DISTINCT FROM $2
         FOR UPDATE",
    )
    .bind(request.source_run_id)
    .bind(request.organization_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("lock workflow recovery source", error))?
    .ok_or_else(|| recovery_source_not_found_error(request.source_run_id))
}

async fn insert_recovery_lineage_tx(
    tx: &mut DbTx<'_>,
    recovery_run_id: Uuid,
    request: &WorkflowRecoveryRequest<'_>,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO workflow_recoveries (
            recovery_run_id,
            source_run_id,
            source_step_id,
            request_key,
            mode,
            reason
         )
         VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(recovery_run_id)
    .bind(request.source_run_id)
    .bind(request.source_step_id)
    .bind(request.request_key)
    .bind(request.mode.as_db_value())
    .bind(request.reason)
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("insert workflow recovery lineage", error)
    })?;

    Ok(())
}

fn validate_recovery_step<'a>(
    recovery_step: &'a RecoveryStep,
) -> Result<ValidatedRecoveryStep<'a>> {
    let RecoveryStep {
        canonical: step,
        source_state,
    } = recovery_step;
    let execution_kind = WorkflowStepExecutionKind::from_db_value(&step.execution_kind)
        .ok_or_else(|| {
            unsafe_recovery_snapshot_error(format!(
                "unknown execution kind for step {}",
                step.step_key
            ))
        })?;
    let source_execution_kind = WorkflowStepExecutionKind::from_db_value(
        &source_state.execution_kind,
    )
    .ok_or_else(|| {
        unsafe_recovery_snapshot_error(format!(
            "source step {} has unknown execution kind {}",
            step.step_key, source_state.execution_kind
        ))
    })?;
    if source_execution_kind != execution_kind {
        return Err(unsafe_recovery_snapshot_error(format!(
            "reconstructed step {} execution kind does not match the source run",
            step.step_key
        )));
    }
    if source_state.allow_handler_continuation != step.allow_handler_continuation {
        return Err(unsafe_recovery_snapshot_error(format!(
            "reconstructed step {} handler continuation setting does not match the source run",
            step.step_key
        )));
    }
    if source_state.execution_resource_key != step.execution_resource_key {
        return Err(unsafe_recovery_snapshot_error(format!(
            "reconstructed step {} execution resource does not match the source run",
            step.step_key
        )));
    }

    let kind = validate_recovery_step_kind(step, source_state, execution_kind)?;

    let stage = step
        .stage
        .as_deref()
        .map(|stage| {
            JobStage::from_db_value(stage).ok_or_else(|| {
                unsafe_recovery_snapshot_error(format!("unknown stage for step {}", step.step_key))
            })
        })
        .transpose()?;
    let dependencies = ValidatedRecoveryDependencies::validate(&step.step_key, &step.dependencies)?;

    Ok(ValidatedRecoveryStep {
        step_key: &step.step_key,
        payload: &source_state.payload,
        organization_id: step.organization_id,
        stage,
        allow_handler_continuation: step.allow_handler_continuation,
        execution_resource_key: step.execution_resource_key.as_deref(),
        dependencies,
        kind,
    })
}

fn validate_recovery_step_kind<'a>(
    step: &'a StoredCanonicalWorkflowStep,
    source_state: &RecoverySourceStepState,
    execution_kind: WorkflowStepExecutionKind,
) -> Result<ValidatedRecoveryStepKind<'a>> {
    match execution_kind {
        WorkflowStepExecutionKind::Job => {
            let priority = resolved_source_step_setting(
                step,
                "priority",
                step.priority,
                source_state.priority,
            )?;
            let max_attempts = resolved_source_step_setting(
                step,
                "max_attempts",
                step.max_attempts,
                source_state.max_attempts,
            )?;
            let timeout_seconds = resolved_source_step_setting(
                step,
                "timeout_seconds",
                step.timeout_seconds,
                source_state.timeout_seconds,
            )?;
            let job_type = step.job_type.as_deref().ok_or_else(|| {
                unsafe_recovery_snapshot_error(format!(
                    "job type missing for step {}",
                    step.step_key
                ))
            })?;
            Ok(ValidatedRecoveryStepKind::Job {
                job_type,
                priority,
                max_attempts,
                timeout_seconds,
            })
        }
        WorkflowStepExecutionKind::External => {
            if step.priority.is_some()
                || step.max_attempts.is_some()
                || step.timeout_seconds.is_some()
                || source_state.priority.is_some()
                || source_state.max_attempts.is_some()
                || source_state.timeout_seconds.is_some()
            {
                return Err(unsafe_recovery_snapshot_error(format!(
                    "external step {} contains job queue settings",
                    step.step_key
                )));
            }
            Ok(ValidatedRecoveryStepKind::External)
        }
    }
}

fn build_recovery_step_enqueue<'a>(
    step: ValidatedRecoveryStep<'a>,
) -> Result<WorkflowStepEnqueue<'a>> {
    let ValidatedRecoveryStep {
        step_key,
        payload,
        organization_id,
        stage,
        allow_handler_continuation,
        execution_resource_key,
        dependencies,
        kind,
    } = step;
    let mut builder = match kind {
        ValidatedRecoveryStepKind::Job {
            job_type,
            priority,
            max_attempts,
            timeout_seconds,
        } => {
            WorkflowStepEnqueueBuilder::new(StepKey::new(step_key), JobType::new(job_type), payload)
                .priority(priority)
                .max_attempts(max_attempts)
                .timeout_seconds(timeout_seconds)
        }
        ValidatedRecoveryStepKind::External => {
            WorkflowStepEnqueueBuilder::new_external(StepKey::new(step_key), payload)
        }
    };
    if let Some(organization_id) = organization_id {
        builder = builder.organization_id(organization_id);
    }
    if let Some(stage) = stage {
        builder = builder.stage(stage);
    }
    if allow_handler_continuation {
        builder = builder.allow_handler_continuation();
    }
    if let Some(resource_key) = execution_resource_key {
        builder = builder.execution_resource(resource_key);
    }
    for dependency in dependencies.iter() {
        let prerequisite = [StepKey::new(dependency.prerequisite_step_key)];
        builder = match dependency.release_mode {
            WorkflowDependencyReleaseMode::OnSuccess => builder.depends_on_success(&prerequisite),
            WorkflowDependencyReleaseMode::OnTerminal => builder.depends_on_terminal(&prerequisite),
        };
    }
    builder
        .try_build()
        .map_err(|error| unsafe_recovery_snapshot_error(error.to_string()))
}

fn resolved_source_step_setting(
    step: &StoredCanonicalWorkflowStep,
    field: &str,
    requested_value: Option<i32>,
    source_value: Option<i32>,
) -> Result<i32> {
    let source_value = source_value.ok_or_else(|| {
        unsafe_recovery_snapshot_error(format!(
            "source job step {} is missing resolved {field}",
            step.step_key
        ))
    })?;
    if requested_value.is_some_and(|requested_value| requested_value != source_value) {
        return Err(unsafe_recovery_snapshot_error(format!(
            "reconstructed step {} {field} does not match the source run",
            step.step_key
        )));
    }
    Ok(source_value)
}

fn pair_recovery_steps(
    steps_by_key: BTreeMap<String, StoredCanonicalWorkflowStep>,
    mut source_step_state_by_key: BTreeMap<String, RecoverySourceStepState>,
) -> Result<Vec<RecoveryStep>> {
    // This is a load-bearing reconstruction invariant: every path that adds
    // workflow steps must also append its canonical request to
    // workflow_run_mutations. The supported append API does so atomically.
    // Keep this length check before key lookup so corrupted snapshots retain
    // the established error precedence.
    if source_step_state_by_key.len() != steps_by_key.len() {
        return Err(unsafe_recovery_snapshot_error(format!(
            "reconstructed {} workflow steps but source run stores {}",
            steps_by_key.len(),
            source_step_state_by_key.len()
        )));
    }

    steps_by_key
        .into_values()
        .map(|canonical| {
            let source_state = source_step_state_by_key
                .remove(&canonical.step_key)
                .ok_or_else(|| {
                    unsafe_recovery_snapshot_error(format!(
                        "reconstructed step {} is missing from the source run",
                        canonical.step_key
                    ))
                })?;
            Ok(RecoveryStep {
                canonical,
                source_state,
            })
        })
        .collect()
}

async fn load_recovery_source_step_state_tx(
    tx: &mut DbTx<'_>,
    source_run_id: Uuid,
) -> Result<BTreeMap<String, RecoverySourceStepState>> {
    let rows = sqlx::query_as::<_, RecoverySourceStepState>(
        "SELECT
            step_key,
            execution_kind::text AS execution_kind,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            allow_handler_continuation,
            execution_resource_key
         FROM workflow_steps
         WHERE workflow_run_id = $1",
    )
    .bind(source_run_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("load workflow recovery source step state", error)
    })?;

    let mut state_by_key = BTreeMap::new();
    for row in rows {
        let step_key = row.step_key.clone();
        if state_by_key.insert(step_key.clone(), row).is_some() {
            return Err(unsafe_recovery_snapshot_error(format!(
                "source workflow contains duplicate step key {step_key}"
            )));
        }
    }
    Ok(state_by_key)
}

fn insert_recovery_step(
    steps_by_key: &mut BTreeMap<String, StoredCanonicalWorkflowStep>,
    step: StoredCanonicalWorkflowStep,
) -> Result<()> {
    let step_key = step.step_key.clone();
    if steps_by_key.insert(step_key.clone(), step).is_some() {
        return Err(unsafe_recovery_snapshot_error(format!(
            "duplicate reconstructed step key {step_key}"
        )));
    }
    Ok(())
}

async fn load_existing_recovery_tx(
    tx: &mut DbTx<'_>,
    source_run_id: Uuid,
    request_key: &str,
) -> Result<Option<ExistingRecoveryRow>> {
    sqlx::query_as::<_, ExistingRecoveryRow>(
        "SELECT recovery_run_id, source_step_id, mode, reason
         FROM workflow_recoveries
         WHERE source_run_id = $1
           AND request_key = $2",
    )
    .bind(source_run_id)
    .bind(request_key)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("load existing workflow recovery", error))
}

fn validate_existing_recovery(
    request: &WorkflowRecoveryRequest<'_>,
    existing: &ExistingRecoveryRow,
) -> Result<()> {
    if existing.source_step_id == request.source_step_id
        && existing.mode == request.mode.as_db_value()
        && existing.reason == request.reason
    {
        return Ok(());
    }
    Err(recovery_request_conflict_error())
}

async fn ensure_source_step_belongs_to_run_tx(
    tx: &mut DbTx<'_>,
    source_run_id: Uuid,
    source_step_id: Uuid,
) -> Result<()> {
    let exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
            SELECT 1
            FROM workflow_steps
            WHERE workflow_run_id = $1
              AND id = $2
         )",
    )
    .bind(source_run_id)
    .bind(source_step_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("validate recovery source step", error))?;
    if exists {
        Ok(())
    } else {
        Err(recovery_source_step_not_found_error(source_step_id))
    }
}

fn validate_recovery_request(request: &WorkflowRecoveryRequest<'_>) -> Result<()> {
    if request.request_key.trim().is_empty() || request.request_key.len() > 512 {
        return Err(recovery_validation_error(
            "workflow.invalid_recovery_request_key",
            "Workflow recovery request key must be non-blank and at most 512 bytes.",
        ));
    }
    if request.reason.trim().is_empty() {
        return Err(recovery_validation_error(
            "workflow.invalid_recovery_reason",
            "Workflow recovery reason must be non-blank.",
        ));
    }
    Ok(())
}

fn recovery_validation_error(code: &'static str, client_message: &'static str) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        code,
        client_message,
        client_message,
    ))
}

fn recovery_source_not_found_error(source_run_id: Uuid) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.recovery_source_not_found",
        "Workflow recovery source was not found.",
        format!("workflow recovery source {source_run_id} was not found in the requested scope"),
    ))
}

fn recovery_source_step_not_found_error(source_step_id: Uuid) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "workflow.recovery_source_step_not_found",
        "Workflow recovery source step was not found.",
        format!("workflow recovery source step {source_step_id} does not belong to the source run"),
    ))
}

fn recovery_source_not_terminal_error(source_run_id: Uuid, status: &str) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Conflict,
        "workflow.recovery_source_not_terminal",
        "Only a terminal workflow run can be recovered.",
        format!("workflow recovery source {source_run_id} has nonterminal status {status}"),
    ))
}

fn recovery_snapshot_missing_error(source_run_id: Uuid, snapshot: &str) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Conflict,
        "workflow.recovery_snapshot_missing",
        "Workflow run cannot be reconstructed safely.",
        format!("workflow recovery source {source_run_id} is missing {snapshot}"),
    ))
}

fn unsafe_recovery_snapshot_error(detail: impl Into<String>) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Conflict,
        "workflow.recovery_snapshot_unsafe",
        "Workflow run cannot be reconstructed safely.",
        detail,
    ))
}

fn recovery_request_conflict_error() -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Conflict,
        "workflow.recovery_request_conflict",
        "Workflow recovery request conflicts with an existing request key.",
        "workflow recovery request reused a request key with different fields",
    ))
}

fn recovery_active_constraint_error(active_run_id: Uuid) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Conflict,
        "workflow.recovery_active_constraint",
        "Workflow recovery is blocked by an active workflow.",
        format!("workflow recovery active key is owned by run {active_run_id}"),
    ))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use super::{
        Error, RecoverySourceStepState, RecoveryStep, StoredCanonicalWorkflowDependency,
        StoredCanonicalWorkflowStep, deserialize_recovery_enqueue_snapshot, pair_recovery_steps,
        validate_recovery_step,
    };

    fn canonical_step(step_key: &str) -> StoredCanonicalWorkflowStep {
        StoredCanonicalWorkflowStep {
            step_key: step_key.to_owned(),
            execution_kind: "JOB".to_owned(),
            job_type: Some("jobs.test.recovery.child".to_owned()),
            organization_id: None,
            payload: json!({"source": "snapshot"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            stage: None,
            allow_handler_continuation: false,
            execution_resource_key: None,
            dependencies: Vec::new(),
        }
    }

    fn source_step_state(step_key: &str) -> RecoverySourceStepState {
        RecoverySourceStepState {
            step_key: step_key.to_owned(),
            execution_kind: "JOB".to_owned(),
            payload: json!({"source": step_key}),
            priority: Some(100),
            max_attempts: Some(3),
            timeout_seconds: Some(60),
            allow_handler_continuation: false,
            execution_resource_key: None,
        }
    }

    fn assert_unsafe_snapshot_error(error: Error, expected_detail: &str) {
        let Error::QueryError(error) = error else {
            panic!("expected unsafe recovery snapshot error");
        };
        assert_eq!(error.code(), "workflow.recovery_snapshot_unsafe");
        assert_eq!(error.internal_message(), expected_detail);
    }

    #[test]
    fn recovery_step_pairing_rejects_missing_source_steps_before_key_lookup() {
        let steps_by_key = BTreeMap::from([
            ("a".to_owned(), canonical_step("a")),
            ("b".to_owned(), canonical_step("b")),
        ]);
        let source_step_state_by_key = BTreeMap::from([("a".to_owned(), source_step_state("a"))]);

        let error = pair_recovery_steps(steps_by_key, source_step_state_by_key)
            .err()
            .expect("missing source state must fail before key lookup");
        assert_unsafe_snapshot_error(
            error,
            "reconstructed 2 workflow steps but source run stores 1",
        );
    }

    #[test]
    fn recovery_step_pairing_rejects_extra_source_steps_before_key_lookup() {
        let steps_by_key = BTreeMap::from([("a".to_owned(), canonical_step("a"))]);
        let source_step_state_by_key = BTreeMap::from([
            ("a".to_owned(), source_step_state("a")),
            ("b".to_owned(), source_step_state("b")),
        ]);

        let error = pair_recovery_steps(steps_by_key, source_step_state_by_key)
            .err()
            .expect("extra source state must fail before key lookup");
        assert_unsafe_snapshot_error(
            error,
            "reconstructed 1 workflow steps but source run stores 2",
        );
    }

    #[test]
    fn recovery_step_pairing_rejects_mismatched_keys_after_equal_length_check() {
        let steps_by_key = BTreeMap::from([("a".to_owned(), canonical_step("a"))]);
        let source_step_state_by_key = BTreeMap::from([("b".to_owned(), source_step_state("b"))]);

        let error = pair_recovery_steps(steps_by_key, source_step_state_by_key)
            .err()
            .expect("mismatched keys must fail after the equal-length check");
        assert_unsafe_snapshot_error(error, "reconstructed step a is missing from the source run");
    }

    #[test]
    fn recovery_step_pairing_orders_pairs_by_canonical_step_key() {
        let steps_by_key = BTreeMap::from([
            ("z".to_owned(), canonical_step("z")),
            ("a".to_owned(), canonical_step("a")),
            ("m".to_owned(), canonical_step("m")),
        ]);
        let source_step_state_by_key = BTreeMap::from([
            ("m".to_owned(), source_step_state("m")),
            ("z".to_owned(), source_step_state("z")),
            ("a".to_owned(), source_step_state("a")),
        ]);

        let steps = pair_recovery_steps(steps_by_key, source_step_state_by_key)
            .expect("pair recovery steps in deterministic order");
        assert_eq!(
            steps
                .iter()
                .map(|step| step.canonical.step_key.as_str())
                .collect::<Vec<_>>(),
            ["a", "m", "z"]
        );
        assert!(
            steps.iter().all(|step| {
                step.source_state.payload["source"] == json!(step.canonical.step_key)
            })
        );
    }

    #[test]
    fn recovery_step_pairing_accepts_legacy_snapshot_optional_fields() {
        let legacy_snapshot = json!({
            "metadata": {"source": "legacy"},
            "steps": [{
                "step_key": "legacy",
                "execution_kind": "JOB",
                "job_type": "jobs.test.recovery.legacy",
                "payload": {"source": "legacy snapshot"},
                "priority": null,
                "max_attempts": null,
                "timeout_seconds": null,
                "dependencies": [],
            }],
        });
        let (metadata, active_key, result_step_key, initial_steps) =
            deserialize_recovery_enqueue_snapshot(legacy_snapshot)
                .expect("recovery decoder must accept historical optional fields")
                .into_parts();
        assert_eq!(metadata, json!({"source": "legacy"}));
        assert!(active_key.is_none());
        assert!(result_step_key.is_none());

        let steps_by_key = initial_steps
            .into_iter()
            .map(|step| (step.step_key.clone(), step))
            .collect();
        let steps = pair_recovery_steps(
            steps_by_key,
            BTreeMap::from([("legacy".to_owned(), source_step_state("legacy"))]),
        )
        .expect("pair legacy snapshot step with source state");
        assert_eq!(steps.len(), 1);
        assert!(steps[0].canonical.organization_id.is_none());
        assert!(steps[0].canonical.stage.is_none());
        assert!(!steps[0].canonical.allow_handler_continuation);
        assert!(steps[0].canonical.execution_resource_key.is_none());
    }

    #[test]
    fn recovery_step_validation_preserves_stage_before_dependency_error_precedence() {
        let step = StoredCanonicalWorkflowStep {
            step_key: "child".to_owned(),
            execution_kind: "JOB".to_owned(),
            job_type: Some("jobs.test.recovery.child".to_owned()),
            organization_id: None,
            payload: json!({"source": "snapshot"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            stage: Some("FUTURE_STAGE".to_owned()),
            allow_handler_continuation: false,
            execution_resource_key: None,
            dependencies: vec![StoredCanonicalWorkflowDependency {
                prerequisite_step_key: "gate".to_owned(),
                release_mode: "FUTURE_RELEASE_MODE".to_owned(),
            }],
        };
        let recovery_step = RecoveryStep {
            canonical: step,
            source_state: source_step_state("child"),
        };

        let error = validate_recovery_step(&recovery_step)
            .expect_err("invalid stage must precede the later release-mode validation");
        assert_unsafe_snapshot_error(error, "unknown stage for step child");
    }
}
