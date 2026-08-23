use runledger_core::jobs::{
    JobStage, StepKey, WorkflowDependencyReleaseMode, WorkflowRunEnqueue, WorkflowStepEnqueue,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
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

/// Recovery has an explicit strict view of the persisted schema. Serde derives
/// unknown-field observation from these types, while append idempotency keeps
/// using its legacy-tolerant representation above.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RecoveryEnqueueSnapshot {
    pub(super) metadata: JsonValue,
    #[serde(default)]
    pub(super) active_key: Option<String>,
    #[serde(default)]
    pub(super) result_step_key: Option<String>,
    steps: Vec<RecoveryCanonicalWorkflowStep>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RecoveryAppendSnapshot {
    #[serde(default, rename = "append_window_step_key")]
    pub(super) _append_window_step_key: Option<String>,
    steps: Vec<RecoveryCanonicalWorkflowStep>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryCanonicalWorkflowStep {
    step_key: String,
    execution_kind: String,
    job_type: Option<String>,
    #[serde(default)]
    organization_id: Option<Uuid>,
    payload: JsonValue,
    priority: Option<i32>,
    max_attempts: Option<i32>,
    timeout_seconds: Option<i32>,
    stage: Option<String>,
    #[serde(default)]
    allow_handler_continuation: bool,
    #[serde(default)]
    execution_resource_key: Option<String>,
    dependencies: Vec<RecoveryCanonicalWorkflowDependency>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveryCanonicalWorkflowDependency {
    prerequisite_step_key: String,
    release_mode: String,
}

impl RecoveryEnqueueSnapshot {
    pub(super) fn take_steps(&mut self) -> Vec<StoredCanonicalWorkflowStep> {
        std::mem::take(&mut self.steps)
            .into_iter()
            .map(Into::into)
            .collect()
    }
}

impl RecoveryAppendSnapshot {
    pub(super) fn into_steps(self) -> Vec<StoredCanonicalWorkflowStep> {
        self.steps.into_iter().map(Into::into).collect()
    }
}

impl From<RecoveryCanonicalWorkflowStep> for StoredCanonicalWorkflowStep {
    fn from(step: RecoveryCanonicalWorkflowStep) -> Self {
        let RecoveryCanonicalWorkflowStep {
            step_key,
            execution_kind,
            job_type,
            organization_id,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            stage,
            allow_handler_continuation,
            execution_resource_key,
            dependencies,
        } = step;
        Self {
            step_key,
            execution_kind,
            job_type,
            organization_id,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            stage,
            allow_handler_continuation,
            execution_resource_key,
            dependencies: dependencies.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<RecoveryCanonicalWorkflowDependency> for StoredCanonicalWorkflowDependency {
    fn from(dependency: RecoveryCanonicalWorkflowDependency) -> Self {
        Self {
            prerequisite_step_key: dependency.prerequisite_step_key,
            release_mode: dependency.release_mode,
        }
    }
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
/// idempotency is intentionally forward-compatible. The strict Serde schema
/// above applies recursively without inspecting opaque JSON values.
pub(super) fn deserialize_recovery_enqueue_snapshot(
    value: JsonValue,
) -> serde_json::Result<RecoveryEnqueueSnapshot> {
    serde_json::from_value(value)
}

/// Decodes an append mutation snapshot for workflow recovery under the same
/// fail-closed schema policy as the initial enqueue snapshot.
pub(super) fn deserialize_recovery_append_snapshot(
    value: JsonValue,
) -> serde_json::Result<RecoveryAppendSnapshot> {
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

const fn is_false(value: &bool) -> bool {
    !*value
}

#[cfg(test)]
mod tests {
    use runledger_core::jobs::{
        JobStage, JobType, StepKey, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder,
        WorkflowType,
    };
    use serde_json::json;
    use sqlx::types::Uuid;

    use super::{
        canonical_append_request, canonical_workflow_enqueue_request,
        deserialize_recovery_append_snapshot, deserialize_recovery_enqueue_snapshot,
        deserialize_stored_append_request,
    };

    #[test]
    fn canonical_snapshots_round_trip_through_strict_recovery_decoders() {
        let workflow_organization_id = Uuid::now_v7();
        let metadata = json!({"kind": "strict-round-trip"});
        let gate_payload = json!({"gate": true});
        let child_payload = json!({"child": true});
        let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &gate_payload)
            .try_build()
            .expect("build external gate");
        let child = WorkflowStepEnqueueBuilder::new(
            StepKey::new("child"),
            JobType::new("jobs.test.strict_round_trip"),
            &child_payload,
        )
        .priority(7)
        .max_attempts(2)
        .timeout_seconds(45)
        .stage(JobStage::Scheduled)
        .allow_handler_continuation()
        .execution_resource("provider-account:strict-round-trip")
        .depends_on_success(&[StepKey::new("gate")])
        .try_build()
        .expect("build job step");
        let workflow = WorkflowRunEnqueueBuilder::new(
            WorkflowType::new("workflow.test.strict_round_trip"),
            &metadata,
        )
        .organization_id(workflow_organization_id)
        .active_key("strict-round-trip")
        .result_step_key(StepKey::new("child"))
        .step(gate)
        .step(child.clone())
        .try_build()
        .expect("build workflow");

        let enqueue_snapshot = canonical_workflow_enqueue_request(&workflow)
            .expect("encode canonical workflow snapshot");
        let decoded_enqueue = deserialize_recovery_enqueue_snapshot(enqueue_snapshot)
            .expect("strict recovery decoder must accept canonical workflow snapshots");
        assert_eq!(decoded_enqueue.metadata, metadata);
        assert_eq!(
            decoded_enqueue.active_key.as_deref(),
            Some("strict-round-trip")
        );
        assert_eq!(decoded_enqueue.result_step_key.as_deref(), Some("child"));
        assert_eq!(decoded_enqueue.steps.len(), 2);
        let decoded_child = decoded_enqueue
            .steps
            .iter()
            .find(|step| step.step_key == "child")
            .expect("decoded workflow must contain child step");
        assert_eq!(
            decoded_child.organization_id,
            Some(workflow_organization_id)
        );
        assert_eq!(decoded_child.stage.as_deref(), Some("scheduled"));
        assert!(decoded_child.allow_handler_continuation);
        assert_eq!(
            decoded_child.execution_resource_key.as_deref(),
            Some("provider-account:strict-round-trip")
        );
        assert_eq!(decoded_child.dependencies.len(), 1);

        let append_snapshot = canonical_append_request(
            StepKey::new("gate"),
            Some(workflow_organization_id),
            &[child],
        )
        .expect("encode canonical append snapshot");
        let decoded_append = deserialize_recovery_append_snapshot(append_snapshot)
            .expect("strict recovery decoder must accept canonical append snapshots");
        assert_eq!(
            decoded_append._append_window_step_key.as_deref(),
            Some("gate")
        );
        assert_eq!(decoded_append.steps.len(), 1);
        assert_eq!(decoded_append.steps[0].step_key, "child");
        assert_eq!(
            decoded_append.steps[0].organization_id,
            Some(workflow_organization_id)
        );
    }

    #[test]
    fn canonical_snapshot_preserves_execution_variant_persistence_shape() {
        let metadata = json!({"source": "execution-variant"});
        let external_payload = json!({"approval": true});
        let job_payload = json!({"send": true});
        let external =
            WorkflowStepEnqueueBuilder::new_external(StepKey::new("approval"), &external_payload)
                .try_build()
                .expect("build external step");
        let job = WorkflowStepEnqueueBuilder::new(
            StepKey::new("send"),
            JobType::new("jobs.test.send"),
            &job_payload,
        )
        .priority(8)
        .max_attempts(4)
        .timeout_seconds(45)
        .stage(JobStage::Scheduled)
        .allow_handler_continuation()
        .execution_resource("provider-account:send")
        .depends_on_success(&[StepKey::new("approval")])
        .try_build()
        .expect("build job step");
        let workflow = WorkflowRunEnqueueBuilder::new(
            WorkflowType::new("workflow.test.execution_variant"),
            &metadata,
        )
        .step(job)
        .step(external)
        .try_build()
        .expect("build workflow");

        assert_eq!(
            canonical_workflow_enqueue_request(&workflow).expect("encode canonical snapshot"),
            json!({
                "metadata": {"source": "execution-variant"},
                "steps": [
                    {
                        "step_key": "approval",
                        "execution_kind": "EXTERNAL",
                        "job_type": null,
                        "organization_id": null,
                        "payload": {"approval": true},
                        "priority": null,
                        "max_attempts": null,
                        "timeout_seconds": null,
                        "stage": null,
                        "dependencies": [],
                    },
                    {
                        "step_key": "send",
                        "execution_kind": "JOB",
                        "job_type": "jobs.test.send",
                        "organization_id": null,
                        "payload": {"send": true},
                        "priority": 8,
                        "max_attempts": 4,
                        "timeout_seconds": 45,
                        "stage": "scheduled",
                        "allow_handler_continuation": true,
                        "execution_resource_key": "provider-account:send",
                        "dependencies": [{
                            "prerequisite_step_key": "approval",
                            "release_mode": "ON_SUCCESS",
                        }],
                    },
                ],
            })
        );
    }

    #[test]
    fn recovery_schema_rejects_unknown_structural_fields_and_keeps_opaque_json_open() {
        let metadata = json!({
            "future_metadata": {
                "steps": [{"unrelated": [true, {"deep": "value"}]}],
                "dependencies": {"arbitrary": null}
            }
        });
        let payload = json!({
            "future_payload": {
                "metadata": {"unrelated": [1, 2, 3]},
                "dependencies": [{"arbitrary": {"deep": false}}]
            }
        });
        let enqueue_snapshot = json!({
            "metadata": metadata.clone(),
            "steps": [{
                "step_key": "child",
                "execution_kind": "JOB",
                "job_type": "jobs.test.child",
                "payload": payload.clone(),
                "priority": null,
                "max_attempts": null,
                "timeout_seconds": null,
                "stage": null,
                "dependencies": [{
                    "prerequisite_step_key": "gate",
                    "release_mode": "ON_TERMINAL"
                }]
            }]
        });

        let decoded_enqueue = deserialize_recovery_enqueue_snapshot(enqueue_snapshot.clone())
            .expect("opaque metadata and payload fields must remain accepted");
        assert_eq!(decoded_enqueue.metadata, metadata);
        assert_eq!(decoded_enqueue.steps[0].payload, payload);
        assert!(decoded_enqueue.active_key.is_none());
        assert!(decoded_enqueue.result_step_key.is_none());
        assert!(decoded_enqueue.steps[0].organization_id.is_none());
        assert!(!decoded_enqueue.steps[0].allow_handler_continuation);
        assert!(decoded_enqueue.steps[0].execution_resource_key.is_none());

        let mut unknown_enqueue_root = enqueue_snapshot.clone();
        unknown_enqueue_root["future_enqueue_root"] = json!(true);
        let error = deserialize_recovery_enqueue_snapshot(unknown_enqueue_root)
            .expect_err("recovery must reject an unknown enqueue root field");
        assert!(error.to_string().contains("future_enqueue_root"));

        let mut unknown_step = enqueue_snapshot.clone();
        unknown_step["steps"][0]["future_step_field"] = json!(true);
        let error = deserialize_recovery_enqueue_snapshot(unknown_step)
            .expect_err("recovery must reject an unknown step field");
        assert!(error.to_string().contains("future_step_field"));

        let mut unknown_dependency = enqueue_snapshot.clone();
        unknown_dependency["steps"][0]["dependencies"][0]["future_dependency_field"] = json!(true);
        let error = deserialize_recovery_enqueue_snapshot(unknown_dependency)
            .expect_err("recovery must reject an unknown dependency field");
        assert!(error.to_string().contains("future_dependency_field"));

        let append_snapshot = json!({
            "steps": [enqueue_snapshot["steps"][0].clone()]
        });
        let decoded_append = deserialize_recovery_append_snapshot(append_snapshot.clone())
            .expect("recovery must accept historical append snapshots without the append window");
        assert!(decoded_append._append_window_step_key.is_none());
        assert_eq!(decoded_append.steps[0].payload, payload);

        let mut unknown_append_root = append_snapshot;
        unknown_append_root["future_append_root"] = json!(true);
        let error = deserialize_recovery_append_snapshot(unknown_append_root)
            .expect_err("recovery must reject an unknown append root field");
        assert!(error.to_string().contains("future_append_root"));
    }

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
