use serde_json::Value as JsonValue;
use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

use super::super::snapshot::{
    StoredCanonicalAppendRequest, StoredCanonicalWorkflowStep, deserialize_stored_append_request,
};

pub(super) async fn stored_append_request_matches_tx(
    tx: &mut DbTx<'_>,
    existing_request: &JsonValue,
    workflow_organization_id: Option<Uuid>,
    requested: &StoredCanonicalAppendRequest,
) -> Result<bool> {
    let existing = deserialize_stored_append_request(existing_request, workflow_organization_id)?;
    if !existing
        .append_window_step_key
        .as_ref()
        .is_none_or(|stored_key| {
            Some(stored_key.as_str()) == requested.append_window_step_key.as_deref()
        })
    {
        return Ok(false);
    }

    stored_append_steps_match_tx(tx, &existing.steps, &requested.steps).await
}

#[cfg(test)]
fn stored_append_request_matches_for_test(
    existing_request: &JsonValue,
    workflow_organization_id: Option<Uuid>,
    requested: &StoredCanonicalAppendRequest,
) -> Result<bool> {
    let existing = deserialize_stored_append_request(existing_request, workflow_organization_id)?;
    Ok(
        stored_append_steps_match_for_test(&existing.steps, &requested.steps)
            && existing
                .append_window_step_key
                .as_ref()
                .is_none_or(|stored_key| {
                    Some(stored_key.as_str()) == requested.append_window_step_key.as_deref()
                }),
    )
}

#[cfg(test)]
fn stored_append_steps_match_for_test(
    left: &[StoredCanonicalWorkflowStep],
    right: &[StoredCanonicalWorkflowStep],
) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            stored_append_step_fields_match(left, right) && left.payload == right.payload
        })
}

async fn stored_append_steps_match_tx(
    tx: &mut DbTx<'_>,
    left: &[StoredCanonicalWorkflowStep],
    right: &[StoredCanonicalWorkflowStep],
) -> Result<bool> {
    if left.len() != right.len() {
        return Ok(false);
    }

    for (left, right) in left.iter().zip(right) {
        if !stored_append_step_fields_match(left, right) {
            return Ok(false);
        }
        if left.payload != right.payload
            && !jsonb_values_equal_tx(tx, &left.payload, &right.payload).await?
        {
            return Ok(false);
        }
    }

    Ok(true)
}

fn stored_append_step_fields_match(
    left: &StoredCanonicalWorkflowStep,
    right: &StoredCanonicalWorkflowStep,
) -> bool {
    left.step_key == right.step_key
        && left.execution_kind == right.execution_kind
        && left.job_type == right.job_type
        && left.organization_id == right.organization_id
        && left.priority == right.priority
        && left.max_attempts == right.max_attempts
        && left.timeout_seconds == right.timeout_seconds
        && left.stage == right.stage
        && left.allow_handler_continuation == right.allow_handler_continuation
        && left.execution_resource_key == right.execution_resource_key
        && left.dependencies == right.dependencies
}

async fn jsonb_values_equal_tx(
    tx: &mut DbTx<'_>,
    left: &JsonValue,
    right: &JsonValue,
) -> Result<bool> {
    sqlx::query_scalar::<_, bool>("SELECT $1::jsonb = $2::jsonb")
        .bind(left)
        .bind(right)
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("compare workflow append request payload", error)
        })
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
           AND mutation_kind = 'APPEND_STEPS'
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
            mutation_kind,
            metadata,
            request
         )
         VALUES ($1, $2, 'APPEND_STEPS', $3::jsonb, $4::jsonb)",
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

    use super::super::super::snapshot::{
        canonical_append_request, deserialize_stored_append_request,
    };
    use super::stored_append_request_matches_for_test;

    #[test]
    fn canonical_append_request_matches_golden_snapshot() {
        let workflow_organization_id = Some(Uuid::now_v7());
        let payload = json!({"kind": "golden"});
        let step = WorkflowStepEnqueueBuilder::new(
            StepKey::new("child"),
            JobType::new("jobs.test.child"),
            &payload,
        )
        .priority(5)
        .max_attempts(2)
        .timeout_seconds(60)
        .depends_on_terminal(&[StepKey::new("gate")])
        .try_build()
        .expect("build appended step");

        let canonical =
            canonical_append_request(StepKey::new("gate"), workflow_organization_id, &[step])
                .expect("canonicalize append request");

        assert_eq!(
            canonical,
            json!({
                "append_window_step_key": "gate",
                "steps": [
                    {
                        "step_key": "child",
                        "execution_kind": "JOB",
                        "job_type": "jobs.test.child",
                        "organization_id": workflow_organization_id,
                        "payload": {"kind": "golden"},
                        "priority": 5,
                        "max_attempts": 2,
                        "timeout_seconds": 60,
                        "stage": "queued",
                        "dependencies": [
                            {
                                "prerequisite_step_key": "gate",
                                "release_mode": "ON_TERMINAL"
                            }
                        ]
                    }
                ]
            })
        );
    }

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
            stored_append_request_matches_for_test(
                &existing_request,
                workflow_organization_id,
                &requested,
            )
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
            stored_append_request_matches_for_test(
                &legacy_request,
                workflow_organization_id,
                &requested
            )
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
            stored_append_request_matches_for_test(
                &existing_request,
                workflow_organization_id,
                &requested,
            )
            .expect("compare implicit and explicit requests"),
            "same effective workflow organization should compare equal",
        );
    }

    #[test]
    fn workflow_append_request_treats_cleared_stage_as_queued() {
        let payload = json!({"batch": "default-stage"});
        let cleared = WorkflowStepEnqueueBuilder::new(
            StepKey::new("child"),
            JobType::new("jobs.test.child"),
            &payload,
        )
        .clear_stage()
        .try_build()
        .expect("build step with cleared stage");
        let queued = WorkflowStepEnqueueBuilder::new(
            StepKey::new("child"),
            JobType::new("jobs.test.child"),
            &payload,
        )
        .try_build()
        .expect("build step with default queued stage");

        let existing_request = canonical_append_request(StepKey::new("gate"), None, &[cleared])
            .expect("build cleared-stage request");
        let queued_request = canonical_append_request(StepKey::new("gate"), None, &[queued])
            .expect("build queued-stage request");
        let requested = deserialize_stored_append_request(&queued_request, None)
            .expect("deserialize queued-stage request");

        assert!(
            stored_append_request_matches_for_test(&existing_request, None, &requested)
                .expect("compare cleared and queued stage requests"),
            "cleared job stage should compare as the inserted queued default",
        );

        let legacy_cleared_request = json!({
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
                    "stage": null,
                    "dependencies": []
                }
            ]
        });
        assert!(
            stored_append_request_matches_for_test(&legacy_cleared_request, None, &requested)
                .expect("compare legacy cleared and queued stage requests"),
            "legacy null job stage should normalize to the inserted queued default",
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
            !stored_append_request_matches_for_test(
                &existing_request,
                workflow_organization_id,
                &requested,
            )
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
            stored_append_request_matches_for_test(
                &legacy_request,
                workflow_organization_id,
                &requested
            )
            .expect("compare legacy request without step scope"),
            "legacy rows without step scope should match the run organization by default",
        );
    }
}
