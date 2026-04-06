use uuid::Uuid;

use super::super::{
    WorkflowBuildError, WorkflowDependencyReleaseMode, WorkflowRunEnqueueBuilder,
    WorkflowStepEnqueueBuilder, WorkflowStepExecutionKind,
};
use crate::jobs::{JobType, StepKey, WorkflowType};

#[test]
fn parse_workflow_dependency_release_mode_from_str_rejects_invalid_value() {
    assert!(
        "NOT_A_REAL_MODE"
            .parse::<WorkflowDependencyReleaseMode>()
            .is_err()
    );
}

#[test]
fn workflow_run_enqueue_builder_sets_scope_and_idempotency() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "builder-test"});
    let organization_id = Uuid::now_v7();
    let step = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.a"),
        JobType::new("jobs.test.a"),
        &payload,
    )
    .try_build()
    .expect("step payload should be valid");

    let enqueue = WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test"), &metadata)
        .organization_id(organization_id)
        .idempotency_key("idempotency-key")
        .step(step)
        .try_build()
        .expect("workflow payload should be valid");

    assert_eq!(enqueue.workflow_type(), WorkflowType::new("workflow.test"));
    assert_eq!(enqueue.organization_id(), Some(organization_id));
    assert_eq!(enqueue.idempotency_key(), Some("idempotency-key"));
    assert_eq!(enqueue.steps().len(), 1);
}

#[test]
fn workflow_step_enqueue_builder_defaults_stage_to_queued() {
    let payload = serde_json::json!({"test": true});
    let step = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.a"),
        JobType::new("jobs.test.a"),
        &payload,
    )
    .try_build()
    .expect("step payload should be valid");

    assert_eq!(step.stage(), Some(crate::jobs::JobStage::Queued));
    assert_eq!(step.organization_id(), None);
    assert_eq!(step.execution_kind(), WorkflowStepExecutionKind::Job);
    assert_eq!(step.job_type(), Some(JobType::new("jobs.test.a")));
}

#[test]
fn workflow_step_enqueue_builder_sets_organization_override() {
    let payload = serde_json::json!({"test": true});
    let organization_id = Uuid::now_v7();
    let step = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.a"),
        JobType::new("jobs.test.a"),
        &payload,
    )
    .organization_id(organization_id)
    .try_build()
    .expect("step payload should be valid");

    assert_eq!(step.organization_id(), Some(organization_id));
}

#[test]
fn workflow_step_enqueue_builder_defaults_organization_scope_to_none() {
    let payload = serde_json::json!({"test": true});
    let step = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.a"),
        JobType::new("jobs.test.a"),
        &payload,
    )
    .try_build()
    .expect("step payload should be valid");

    assert_eq!(step.organization_id(), None);
}

#[test]
fn workflow_run_enqueue_builder_rejects_empty_steps() {
    let metadata = serde_json::json!({"source": "builder-test"});
    let error = WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test"), &metadata)
        .try_build()
        .expect_err("empty workflow should be rejected");

    assert_eq!(error, WorkflowBuildError::EmptySteps);
}

#[test]
fn workflow_step_enqueue_builder_try_new_rejects_blank_step_key() {
    let payload = serde_json::json!({"test": true});
    let error = WorkflowStepEnqueueBuilder::try_new("", "jobs.test.a", &payload)
        .expect_err("blank step key should be rejected");

    assert_eq!(error, WorkflowBuildError::BlankStepKey { step_index: None });
}

#[test]
fn workflow_step_enqueue_builder_try_new_rejects_blank_job_type() {
    let payload = serde_json::json!({"test": true});
    let error = WorkflowStepEnqueueBuilder::try_new("step.a", "   ", &payload)
        .expect_err("blank job type should be rejected");

    assert_eq!(
        error,
        WorkflowBuildError::BlankStepJobType {
            step_key: "step.a".to_owned(),
        }
    );
}

#[test]
fn workflow_run_enqueue_builder_try_new_rejects_blank_workflow_type() {
    let metadata = serde_json::json!({"source": "builder-test"});
    let error = WorkflowRunEnqueueBuilder::try_new(" ", &metadata)
        .expect_err("blank workflow type should be rejected");

    assert_eq!(error, WorkflowBuildError::BlankWorkflowType);
}

#[test]
fn workflow_builders_try_new_accept_valid_identifiers() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "builder-test"});

    let step = WorkflowStepEnqueueBuilder::try_new("step.a", "jobs.test.a", &payload)
        .expect("valid step identifiers should build")
        .try_build()
        .expect("valid step payload should build");

    let enqueue = WorkflowRunEnqueueBuilder::try_new("workflow.test", &metadata)
        .expect("valid workflow identifier should build")
        .step(step)
        .try_build()
        .expect("workflow payload should be valid");

    assert_eq!(enqueue.workflow_type(), WorkflowType::new("workflow.test"));
    assert_eq!(enqueue.steps()[0].step_key(), StepKey::new("step.a"));
}

#[test]
fn workflow_step_enqueue_builder_supports_external_steps() {
    let payload = serde_json::json!({"test": true});

    let step = WorkflowStepEnqueueBuilder::try_new_external("step.external", &payload)
        .expect("valid external step identifier should build")
        .try_build()
        .expect("external step payload should be valid");

    assert_eq!(step.execution_kind(), WorkflowStepExecutionKind::External);
    assert_eq!(step.job_type(), None);
    assert_eq!(step.stage(), None);
}

#[test]
fn workflow_step_enqueue_builder_rejects_queue_settings_on_external_steps() {
    let payload = serde_json::json!({"test": true});

    let error = WorkflowStepEnqueueBuilder::new_external(StepKey::new("step.external"), &payload)
        .priority(10)
        .try_build()
        .expect_err("external step queue settings should be rejected");

    assert_eq!(
        error,
        WorkflowBuildError::ExternalStepQueueSettingsNotAllowed {
            step_key: "step.external".to_owned(),
        }
    );
}

#[test]
fn workflow_step_enqueue_builder_rejects_self_dependency() {
    let payload = serde_json::json!({"test": true});
    let error = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.a"),
        JobType::new("jobs.test.a"),
        &payload,
    )
    .depends_on_terminal(&[StepKey::new("step.a")])
    .try_build()
    .expect_err("self dependency should be rejected");

    assert_eq!(
        error,
        WorkflowBuildError::SelfDependency {
            step_key: "step.a".to_owned(),
        }
    );
}

#[test]
fn workflow_step_enqueue_builder_wires_dependency_release_modes() {
    let payload = serde_json::json!({"test": true});
    let step = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.a"),
        JobType::new("jobs.test.a"),
        &payload,
    )
    .depends_on_terminal(&[StepKey::new("step.b")])
    .depends_on_success(&[StepKey::new("step.c")])
    .try_build()
    .expect("step payload should be valid");

    assert_eq!(step.dependencies().len(), 2);
    assert_eq!(
        step.dependencies()[0].release_mode,
        Some(WorkflowDependencyReleaseMode::OnTerminal)
    );
    assert_eq!(
        step.dependencies()[1].release_mode,
        Some(WorkflowDependencyReleaseMode::OnSuccess)
    );
}

#[test]
fn workflow_step_enqueue_builder_preserves_dependency_append_order() {
    let payload = serde_json::json!({"test": true});
    let step = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.a"),
        JobType::new("jobs.test.a"),
        &payload,
    )
    .depends_on_terminal(&[StepKey::new("step.b"), StepKey::new("step.c")])
    .depends_on_success(&[StepKey::new("step.d")])
    .depends_on_terminal(&[StepKey::new("step.e")])
    .try_build()
    .expect("step payload should be valid");

    let dependency_pairs: Vec<(StepKey<'_>, WorkflowDependencyReleaseMode)> = step
        .dependencies()
        .iter()
        .map(|dependency| {
            (
                dependency.prerequisite_step_key,
                dependency
                    .release_mode
                    .expect("builder always sets dependency release mode"),
            )
        })
        .collect();
    assert_eq!(
        dependency_pairs,
        vec![
            (
                StepKey::new("step.b"),
                WorkflowDependencyReleaseMode::OnTerminal
            ),
            (
                StepKey::new("step.c"),
                WorkflowDependencyReleaseMode::OnTerminal
            ),
            (
                StepKey::new("step.d"),
                WorkflowDependencyReleaseMode::OnSuccess
            ),
            (
                StepKey::new("step.e"),
                WorkflowDependencyReleaseMode::OnTerminal
            ),
        ]
    );
}

#[test]
fn workflow_run_enqueue_builder_set_steps_replaces_existing_steps() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "builder-test"});
    let step_a = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.a"),
        JobType::new("jobs.test.a"),
        &payload,
    )
    .try_build()
    .expect("step payload should be valid");
    let step_b = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.b"),
        JobType::new("jobs.test.b"),
        &payload,
    )
    .try_build()
    .expect("step payload should be valid");
    let step_c = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.c"),
        JobType::new("jobs.test.c"),
        &payload,
    )
    .try_build()
    .expect("step payload should be valid");

    let enqueue = WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test"), &metadata)
        .step(step_a)
        .set_steps(vec![step_b, step_c])
        .try_build()
        .expect("workflow payload should be valid");

    assert_eq!(enqueue.steps().len(), 2);
    assert_eq!(enqueue.steps()[0].step_key(), StepKey::new("step.b"));
    assert_eq!(enqueue.steps()[1].step_key(), StepKey::new("step.c"));
}

#[test]
fn workflow_run_enqueue_builder_extend_steps_appends_steps() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "builder-test"});
    let step_a = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.a"),
        JobType::new("jobs.test.a"),
        &payload,
    )
    .try_build()
    .expect("step payload should be valid");
    let step_b = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.b"),
        JobType::new("jobs.test.b"),
        &payload,
    )
    .try_build()
    .expect("step payload should be valid");
    let step_c = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.c"),
        JobType::new("jobs.test.c"),
        &payload,
    )
    .try_build()
    .expect("step payload should be valid");

    let enqueue = WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test"), &metadata)
        .step(step_a)
        .extend_steps(vec![step_b, step_c])
        .try_build()
        .expect("workflow payload should be valid");

    assert_eq!(enqueue.steps().len(), 3);
    assert_eq!(enqueue.steps()[0].step_key(), StepKey::new("step.a"));
    assert_eq!(enqueue.steps()[1].step_key(), StepKey::new("step.b"));
    assert_eq!(enqueue.steps()[2].step_key(), StepKey::new("step.c"));
}
