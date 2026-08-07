use runledger_core::jobs::{
    JobStage, StepKey, WorkflowDependencyReleaseMode, WorkflowRunEnqueue, WorkflowStepEnqueue,
};
use serde::{Deserialize, Serialize};
use serde_json::{Map as JsonMap, Value as JsonValue};
use sqlx::types::Uuid;

use crate::Result;

use super::errors::workflow_internal_state_error;
use super::steps::{workflow_step_effective_organization_id, workflow_step_effective_stage};

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
struct CanonicalAppendRequest<'a> {
    append_window_step_key: &'a str,
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

/// The legacy-tolerant representation used when comparing persisted append
/// requests for idempotency. It intentionally has no `deny_unknown_fields` so
/// snapshots written before additive schema changes stay comparable.
#[derive(Clone, Debug, Deserialize, PartialEq)]
pub(super) struct StoredCanonicalAppendRequest {
    #[serde(default)]
    pub(super) append_window_step_key: Option<String>,
    pub(super) steps: Vec<StoredCanonicalWorkflowStep>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub(super) struct StoredCanonicalWorkflowStep {
    pub(super) step_key: String,
    pub(super) execution_kind: String,
    pub(super) job_type: Option<String>,
    #[serde(default)]
    pub(super) organization_id: Option<Uuid>,
    pub(super) payload: JsonValue,
    pub(super) priority: Option<i32>,
    pub(super) max_attempts: Option<i32>,
    pub(super) timeout_seconds: Option<i32>,
    pub(super) stage: Option<String>,
    #[serde(default)]
    pub(super) allow_handler_continuation: bool,
    #[serde(default)]
    pub(super) execution_resource_key: Option<String>,
    pub(super) dependencies: Vec<StoredCanonicalWorkflowDependency>,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub(super) struct StoredCanonicalWorkflowDependency {
    pub(super) prerequisite_step_key: String,
    pub(super) release_mode: String,
}

/// The recovery reader deliberately shares the persisted step schema with
/// append idempotency, but its entrypoints validate every structural boundary
/// before deserializing it. That preserves recovery's fail-closed contract
/// without making append retries reject legacy additive fields.
#[derive(Debug, Deserialize)]
pub(super) struct RecoveryEnqueueSnapshot {
    pub(super) metadata: JsonValue,
    #[serde(default)]
    pub(super) active_key: Option<String>,
    #[serde(default)]
    pub(super) result_step_key: Option<String>,
    pub(super) steps: Vec<StoredCanonicalWorkflowStep>,
}

#[derive(Debug, Deserialize)]
pub(super) struct RecoveryAppendSnapshot {
    #[serde(default, rename = "append_window_step_key")]
    pub(super) _append_window_step_key: Option<String>,
    pub(super) steps: Vec<StoredCanonicalWorkflowStep>,
}

pub(super) fn canonical_workflow_enqueue_request(
    payload: &WorkflowRunEnqueue<'_>,
) -> Result<JsonValue> {
    serde_json::to_value(CanonicalWorkflowRunEnqueueRequest {
        metadata: payload.metadata(),
        active_key: payload.active_key(),
        result_step_key: payload.result_step_key().map(|step_key| step_key.as_str()),
        steps: canonical_workflow_steps(payload.organization_id(), payload.steps()),
    })
    .map_err(|error| {
        workflow_internal_state_error(format!(
            "failed to serialize canonical workflow enqueue request: {error}"
        ))
    })
}

pub(super) fn canonical_append_request(
    append_window_step_key: StepKey<'_>,
    workflow_organization_id: Option<Uuid>,
    steps: &[WorkflowStepEnqueue<'_>],
) -> Result<JsonValue> {
    serde_json::to_value(CanonicalAppendRequest {
        append_window_step_key: append_window_step_key.as_str(),
        steps: canonical_workflow_steps(workflow_organization_id, steps),
    })
    .map_err(|error| {
        workflow_internal_state_error(format!(
            "failed to serialize canonical workflow append request: {error}"
        ))
    })
}

/// Decodes an append request for idempotency comparison.
///
/// This reader intentionally accepts older snapshots that omit the append
/// window, per-step organization, or job stage. It also ignores unknown fields
/// exactly as the pre-codec reader did.
pub(super) fn deserialize_stored_append_request(
    value: &JsonValue,
    workflow_organization_id: Option<Uuid>,
) -> Result<StoredCanonicalAppendRequest> {
    let request = serde_json::from_value(value.clone()).map_err(|error| {
        workflow_internal_state_error(format!(
            "failed to deserialize canonical workflow append request: {error}"
        ))
    })?;
    Ok(normalize_stored_append_request(
        request,
        workflow_organization_id,
    ))
}

/// Decodes an initial enqueue snapshot for workflow recovery.
///
/// Recovery must reject unknown structural fields even though append
/// idempotency is intentionally forward-compatible. Validate that strict
/// schema before decoding the shared owned representation.
pub(super) fn deserialize_recovery_enqueue_snapshot(
    value: JsonValue,
) -> serde_json::Result<RecoveryEnqueueSnapshot> {
    validate_strict_enqueue_snapshot(&value)?;
    serde_json::from_value(value)
}

/// Decodes an append mutation snapshot for workflow recovery under the same
/// fail-closed schema policy as the initial enqueue snapshot.
pub(super) fn deserialize_recovery_append_snapshot(
    value: JsonValue,
) -> serde_json::Result<RecoveryAppendSnapshot> {
    validate_strict_append_snapshot(&value)?;
    serde_json::from_value(value)
}

fn canonical_workflow_steps<'a>(
    workflow_organization_id: Option<Uuid>,
    steps: &[WorkflowStepEnqueue<'a>],
) -> Vec<CanonicalWorkflowStep<'a>> {
    let mut canonical_steps = steps
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
                    workflow_organization_id,
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
    canonical_steps.sort_by(|left, right| left.step_key.cmp(right.step_key));
    canonical_steps
}

fn normalize_stored_append_request(
    mut request: StoredCanonicalAppendRequest,
    workflow_organization_id: Option<Uuid>,
) -> StoredCanonicalAppendRequest {
    for step in &mut request.steps {
        step.organization_id = step.organization_id.or(workflow_organization_id);
        if step.execution_kind == "JOB" && step.stage.is_none() {
            // Older append snapshots stored an explicitly cleared job stage as
            // null, while insertion still materialized the step as queued.
            step.stage = Some(JobStage::Queued.as_db_value().to_owned());
        }
        sort_stored_dependencies(&mut step.dependencies);
    }
    request
        .steps
        .sort_by(|left, right| left.step_key.cmp(&right.step_key));
    request
}

fn sort_stored_dependencies(dependencies: &mut [StoredCanonicalWorkflowDependency]) {
    dependencies.sort_by(|left, right| {
        left.prerequisite_step_key
            .cmp(&right.prerequisite_step_key)
            .then(left.release_mode.cmp(&right.release_mode))
    });
}

fn validate_strict_enqueue_snapshot(value: &JsonValue) -> serde_json::Result<()> {
    let object = require_object(value, "enqueue snapshot")?;
    reject_unknown_fields(
        object,
        &["metadata", "active_key", "result_step_key", "steps"],
        "enqueue snapshot",
    )?;
    if let Some(steps) = object.get("steps") {
        validate_strict_steps(steps)?;
    }
    Ok(())
}

fn validate_strict_append_snapshot(value: &JsonValue) -> serde_json::Result<()> {
    let object = require_object(value, "append snapshot")?;
    reject_unknown_fields(
        object,
        &["append_window_step_key", "steps"],
        "append snapshot",
    )?;
    if let Some(steps) = object.get("steps") {
        validate_strict_steps(steps)?;
    }
    Ok(())
}

fn validate_strict_steps(value: &JsonValue) -> serde_json::Result<()> {
    let steps = value
        .as_array()
        .ok_or_else(|| strict_schema_error("snapshot steps must be an array"))?;
    for step in steps {
        let object = require_object(step, "workflow step snapshot")?;
        reject_unknown_fields(
            object,
            &[
                "step_key",
                "execution_kind",
                "job_type",
                "organization_id",
                "payload",
                "priority",
                "max_attempts",
                "timeout_seconds",
                "stage",
                "allow_handler_continuation",
                "execution_resource_key",
                "dependencies",
            ],
            "workflow step snapshot",
        )?;
        if let Some(dependencies) = object.get("dependencies") {
            validate_strict_dependencies(dependencies)?;
        }
    }
    Ok(())
}

fn validate_strict_dependencies(value: &JsonValue) -> serde_json::Result<()> {
    let dependencies = value
        .as_array()
        .ok_or_else(|| strict_schema_error("workflow step dependencies must be an array"))?;
    for dependency in dependencies {
        let object = require_object(dependency, "workflow dependency snapshot")?;
        reject_unknown_fields(
            object,
            &["prerequisite_step_key", "release_mode"],
            "workflow dependency snapshot",
        )?;
    }
    Ok(())
}

fn require_object<'a>(
    value: &'a JsonValue,
    snapshot: &str,
) -> serde_json::Result<&'a JsonMap<String, JsonValue>> {
    value
        .as_object()
        .ok_or_else(|| strict_schema_error(format!("{snapshot} must be an object")))
}

fn reject_unknown_fields(
    object: &JsonMap<String, JsonValue>,
    allowed_fields: &[&str],
    snapshot: &str,
) -> serde_json::Result<()> {
    for field in object.keys() {
        if !allowed_fields.contains(&field.as_str()) {
            return Err(strict_schema_error(format!(
                "unknown field {field} in {snapshot}"
            )));
        }
    }
    Ok(())
}

fn strict_schema_error(detail: impl Into<String>) -> serde_json::Error {
    <serde_json::Error as serde::de::Error>::custom(detail.into())
}

const fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use sqlx::types::Uuid;

    use super::{deserialize_recovery_append_snapshot, deserialize_stored_append_request};

    #[test]
    fn append_idempotency_is_legacy_tolerant_while_recovery_is_strict() {
        let workflow_organization_id = Some(Uuid::now_v7());
        let legacy_snapshot = json!({
            "steps": [
                {
                    "step_key": "child",
                    "execution_kind": "JOB",
                    "job_type": "jobs.test.child",
                    "payload": {"kind": "legacy"},
                    "priority": null,
                    "max_attempts": null,
                    "timeout_seconds": null,
                    "dependencies": [
                        {
                            "prerequisite_step_key": "gate",
                            "release_mode": "ON_TERMINAL"
                        }
                    ]
                }
            ]
        });

        let normalized =
            deserialize_stored_append_request(&legacy_snapshot, workflow_organization_id)
                .expect("legacy append idempotency snapshot should decode");
        assert_eq!(normalized.append_window_step_key, None);
        assert_eq!(
            normalized.steps[0].organization_id,
            workflow_organization_id
        );
        assert_eq!(normalized.steps[0].stage.as_deref(), Some("queued"));
        assert!(
            deserialize_recovery_append_snapshot(legacy_snapshot.clone()).is_ok(),
            "recovery still accepts the legacy optional fields"
        );

        let mut additive_snapshot = legacy_snapshot;
        additive_snapshot["steps"][0]["future_execution_constraint"] = json!(true);
        additive_snapshot["steps"][0]["dependencies"][0]["future_release_flag"] = json!(true);
        assert!(
            deserialize_stored_append_request(&additive_snapshot, workflow_organization_id).is_ok(),
            "append idempotency must continue ignoring unknown legacy/additive fields"
        );
        assert!(
            deserialize_recovery_append_snapshot(additive_snapshot).is_err(),
            "recovery must reject unknown snapshot fields"
        );
    }
}
