use runledger_core::jobs::{StepKey, WorkflowDependencyReleaseMode, WorkflowStepEnqueue};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

use super::super::enqueue::workflow_step_effective_organization_id;
use super::super::workflow_internal_state_error;

#[derive(Serialize)]
struct CanonicalAppendRequest<'a> {
    append_window_step_key: &'a str,
    steps: Vec<CanonicalStep<'a>>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub(super) struct StoredCanonicalAppendRequest {
    #[serde(default)]
    append_window_step_key: Option<String>,
    steps: Vec<StoredCanonicalStep>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
struct StoredCanonicalStep {
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
    dependencies: Vec<StoredCanonicalDependency>,
}

#[derive(Debug, Clone, Deserialize, PartialEq)]
struct StoredCanonicalDependency {
    prerequisite_step_key: String,
    release_mode: String,
}

#[derive(Serialize)]
struct CanonicalStep<'a> {
    step_key: &'a str,
    execution_kind: &'static str,
    job_type: Option<&'a str>,
    organization_id: Option<Uuid>,
    payload: &'a JsonValue,
    priority: Option<i32>,
    max_attempts: Option<i32>,
    timeout_seconds: Option<i32>,
    stage: Option<&'static str>,
    dependencies: Vec<CanonicalDependency<'a>>,
}

#[derive(Serialize)]
struct CanonicalDependency<'a> {
    prerequisite_step_key: &'a str,
    release_mode: &'static str,
}

pub(super) fn canonical_append_request(
    append_window_step_key: StepKey<'_>,
    workflow_organization_id: Option<Uuid>,
    steps: &[WorkflowStepEnqueue<'_>],
) -> Result<JsonValue> {
    let mut canonical_steps = steps
        .iter()
        .map(|step| {
            let mut dependencies = step
                .dependencies()
                .iter()
                .map(|dependency| CanonicalDependency {
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

            CanonicalStep {
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
                stage: step
                    .stage()
                    .map(runledger_core::jobs::JobStage::as_db_value),
                dependencies,
            }
        })
        .collect::<Vec<_>>();
    canonical_steps.sort_by(|left, right| left.step_key.cmp(right.step_key));

    let request = CanonicalAppendRequest {
        append_window_step_key: append_window_step_key.as_str(),
        steps: canonical_steps,
    };

    serde_json::to_value(request).map_err(|error| {
        workflow_internal_state_error(format!(
            "failed to serialize canonical workflow append request: {error}"
        ))
    })
}

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

fn normalize_stored_append_request(
    mut request: StoredCanonicalAppendRequest,
    workflow_organization_id: Option<Uuid>,
) -> StoredCanonicalAppendRequest {
    for step in &mut request.steps {
        step.organization_id = step.organization_id.or(workflow_organization_id);
        step.dependencies.sort_by(|left, right| {
            left.prerequisite_step_key
                .cmp(&right.prerequisite_step_key)
                .then(left.release_mode.cmp(&right.release_mode))
        });
    }
    request
        .steps
        .sort_by(|left, right| left.step_key.cmp(&right.step_key));
    request
}

pub(super) fn stored_append_request_matches(
    existing_request: &JsonValue,
    workflow_organization_id: Option<Uuid>,
    requested: &StoredCanonicalAppendRequest,
) -> Result<bool> {
    let existing = deserialize_stored_append_request(existing_request, workflow_organization_id)?;
    if existing.steps != requested.steps {
        return Ok(false);
    }

    Ok(existing
        .append_window_step_key
        .as_ref()
        .is_none_or(|stored_key| {
            Some(stored_key.as_str()) == requested.append_window_step_key.as_deref()
        }))
}

pub(super) async fn load_existing_mutation_request_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    mutation_key: &str,
) -> Result<Option<JsonValue>> {
    sqlx::query_scalar!(
        "SELECT request
         FROM workflow_run_mutations
         WHERE workflow_run_id = $1
           AND mutation_key = $2
         LIMIT 1",
        workflow_run_id,
        mutation_key,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("load workflow append mutation request", error)
    })
}

pub(super) async fn insert_workflow_mutation_row_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    mutation_key: &str,
    mutation_metadata: &JsonValue,
    request: &JsonValue,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO workflow_run_mutations (
            workflow_run_id,
            mutation_key,
            metadata,
            request
         )
         VALUES ($1, $2, $3::jsonb, $4::jsonb)",
        workflow_run_id,
        mutation_key,
        mutation_metadata,
        request,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("insert workflow append mutation row", error)
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use runledger_core::jobs::{JobType, StepKey, WorkflowStepEnqueueBuilder};
    use serde_json::json;
    use sqlx::types::Uuid;

    use super::{
        canonical_append_request, deserialize_stored_append_request, stored_append_request_matches,
    };

    #[test]
    fn workflow_append_request_matches_reordered_steps() {
        let workflow_organization_id = Some(Uuid::now_v7());
        let payload = json!({"batch": 1});
        let alpha = WorkflowStepEnqueueBuilder::new(
            StepKey::new("alpha"),
            JobType::new("jobs.test.alpha"),
            &payload,
        )
        .depends_on_terminal(&[StepKey::new("gate")])
        .try_build()
        .expect("build alpha step");
        let beta = WorkflowStepEnqueueBuilder::new(
            StepKey::new("beta"),
            JobType::new("jobs.test.beta"),
            &payload,
        )
        .depends_on_terminal(&[StepKey::new("alpha"), StepKey::new("gate")])
        .try_build()
        .expect("build beta step");

        let existing_request = canonical_append_request(
            StepKey::new("gate"),
            workflow_organization_id,
            &[alpha.clone(), beta.clone()],
        )
        .expect("build canonical append request");
        let reordered_request = canonical_append_request(
            StepKey::new("gate"),
            workflow_organization_id,
            &[beta, alpha],
        )
        .expect("build reordered append request");
        let requested =
            deserialize_stored_append_request(&reordered_request, workflow_organization_id)
                .expect("deserialize reordered append request");

        assert!(
            stored_append_request_matches(&existing_request, workflow_organization_id, &requested)
                .expect("compare reordered append request"),
            "same logical append batch should match after step reordering",
        );
    }

    #[test]
    fn workflow_append_request_matches_legacy_unsorted_steps() {
        let workflow_organization_id = Some(Uuid::now_v7());
        let legacy_request = json!({
            "append_window_step_key": "gate",
            "steps": [
                {
                    "step_key": "beta",
                    "execution_kind": "JOB",
                    "job_type": "jobs.test.beta",
                    "payload": {"batch": 1},
                    "priority": null,
                    "max_attempts": null,
                    "timeout_seconds": null,
                    "stage": "queued",
                    "dependencies": [
                        {
                            "prerequisite_step_key": "gate",
                            "release_mode": "ON_TERMINAL"
                        },
                        {
                            "prerequisite_step_key": "alpha",
                            "release_mode": "ON_TERMINAL"
                        }
                    ]
                },
                {
                    "step_key": "alpha",
                    "execution_kind": "JOB",
                    "job_type": "jobs.test.alpha",
                    "payload": {"batch": 1},
                    "priority": null,
                    "max_attempts": null,
                    "timeout_seconds": null,
                    "stage": "queued",
                    "dependencies": [
                        {
                            "prerequisite_step_key": "gate",
                            "release_mode": "ON_TERMINAL"
                        }
                    ]
                }
            ]
        });
        let payload = json!({"batch": 1});
        let alpha = WorkflowStepEnqueueBuilder::new(
            StepKey::new("alpha"),
            JobType::new("jobs.test.alpha"),
            &payload,
        )
        .depends_on_terminal(&[StepKey::new("gate")])
        .try_build()
        .expect("build alpha step");
        let beta = WorkflowStepEnqueueBuilder::new(
            StepKey::new("beta"),
            JobType::new("jobs.test.beta"),
            &payload,
        )
        .depends_on_terminal(&[StepKey::new("alpha"), StepKey::new("gate")])
        .try_build()
        .expect("build beta step");
        let reordered_request = canonical_append_request(
            StepKey::new("gate"),
            workflow_organization_id,
            &[beta, alpha],
        )
        .expect("build reordered append request");
        let requested =
            deserialize_stored_append_request(&reordered_request, workflow_organization_id)
                .expect("deserialize reordered append request");

        assert!(
            stored_append_request_matches(&legacy_request, workflow_organization_id, &requested)
                .expect("compare legacy append request"),
            "legacy stored rows with unsorted steps should still match",
        );
    }

    #[test]
    fn workflow_append_request_treats_implicit_and_explicit_run_scope_as_equal() {
        let run_organization_id = Uuid::now_v7();
        let workflow_organization_id = Some(run_organization_id);
        let payload = json!({"batch": "org-scope"});
        let implicit = WorkflowStepEnqueueBuilder::new(
            StepKey::new("child"),
            JobType::new("jobs.test.child"),
            &payload,
        )
        .try_build()
        .expect("build implicitly scoped step");
        let explicit = WorkflowStepEnqueueBuilder::new(
            StepKey::new("child"),
            JobType::new("jobs.test.child"),
            &payload,
        )
        .organization_id(run_organization_id)
        .try_build()
        .expect("build explicitly scoped step");

        let existing_request =
            canonical_append_request(StepKey::new("gate"), workflow_organization_id, &[implicit])
                .expect("build implicit canonical request");
        let explicit_request =
            canonical_append_request(StepKey::new("gate"), workflow_organization_id, &[explicit])
                .expect("build explicit canonical request");
        let requested =
            deserialize_stored_append_request(&explicit_request, workflow_organization_id)
                .expect("deserialize explicit request");

        assert!(
            stored_append_request_matches(&existing_request, workflow_organization_id, &requested)
                .expect("compare implicit and explicit requests"),
            "same effective workflow organization should compare equal",
        );
    }

    #[test]
    fn workflow_append_request_rejects_changed_step_organization_scope() {
        let workflow_organization_id = Some(Uuid::now_v7());
        let payload = json!({"batch": "org-scope"});
        let first_step = WorkflowStepEnqueueBuilder::new(
            StepKey::new("child"),
            JobType::new("jobs.test.child"),
            &payload,
        )
        .try_build()
        .expect("build first step");
        let changed_step = WorkflowStepEnqueueBuilder::new(
            StepKey::new("child"),
            JobType::new("jobs.test.child"),
            &payload,
        )
        .organization_id(Uuid::now_v7())
        .try_build()
        .expect("build changed step");

        let existing_request = canonical_append_request(
            StepKey::new("gate"),
            workflow_organization_id,
            &[first_step],
        )
        .expect("build first request");
        let changed_request = canonical_append_request(
            StepKey::new("gate"),
            workflow_organization_id,
            &[changed_step],
        )
        .expect("build changed request");
        let requested =
            deserialize_stored_append_request(&changed_request, workflow_organization_id)
                .expect("deserialize changed request");

        assert!(
            !stored_append_request_matches(&existing_request, workflow_organization_id, &requested)
                .expect("compare changed requests"),
            "changed step organization must not compare equal",
        );
    }

    #[test]
    fn workflow_append_request_matches_legacy_request_without_step_scope() {
        let workflow_organization_id = Some(Uuid::now_v7());
        let payload = json!({"batch": "legacy"});
        let legacy_request = json!({
            "append_window_step_key": "gate",
            "steps": [
                {
                    "step_key": "child",
                    "execution_kind": "JOB",
                    "job_type": "jobs.test.child",
                    "payload": payload,
                    "priority": null,
                    "max_attempts": null,
                    "timeout_seconds": null,
                    "stage": "queued",
                    "dependencies": []
                }
            ]
        });
        let current_request = canonical_append_request(
            StepKey::new("gate"),
            workflow_organization_id,
            &[WorkflowStepEnqueueBuilder::new(
                StepKey::new("child"),
                JobType::new("jobs.test.child"),
                &payload,
            )
            .try_build()
            .expect("build current step")],
        )
        .expect("build current request");
        let requested =
            deserialize_stored_append_request(&current_request, workflow_organization_id)
                .expect("deserialize current request");

        assert!(
            stored_append_request_matches(&legacy_request, workflow_organization_id, &requested)
                .expect("compare legacy request without step scope"),
            "legacy rows without step scope should match the run organization by default",
        );
    }
}
