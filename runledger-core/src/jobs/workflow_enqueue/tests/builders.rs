use uuid::Uuid;

use super::super::{
    WorkflowBuildError, WorkflowDagBuilder, WorkflowDependencyReleaseMode,
    WorkflowRunEnqueueBuilder, WorkflowStepDependencySpec, WorkflowStepEnqueueBuilder,
    WorkflowStepExecution, WorkflowStepExecutionKind,
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
fn workflow_run_enqueue_builder_rejects_blank_idempotency_key() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "builder-test"});
    let step = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.a"),
        JobType::new("jobs.test.a"),
        &payload,
    )
    .try_build()
    .expect("step payload should be valid");

    let result = WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test"), &metadata)
        .idempotency_key("   ")
        .step(step)
        .try_build();

    assert!(
        result.is_err(),
        "blank idempotency key should be rejected before storage"
    );
}

#[test]
fn workflow_run_enqueue_builder_sets_and_validates_active_key() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "builder-test"});
    let step = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.a"),
        JobType::new("jobs.test.a"),
        &payload,
    )
    .try_build()
    .expect("build active-key step");
    let enqueue = WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test"), &metadata)
        .active_key("daily-cycle")
        .step(step.clone())
        .try_build()
        .expect("build active-key workflow");
    assert_eq!(enqueue.active_key(), Some("daily-cycle"));

    let error = WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test"), &metadata)
        .active_key("  ")
        .step(step.clone())
        .try_build()
        .expect_err("blank active key should be rejected");
    assert_eq!(error, WorkflowBuildError::BlankActiveKey);

    let oversized_active_key = "x".repeat(513);
    let error = WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test"), &metadata)
        .active_key(&oversized_active_key)
        .step(step)
        .try_build()
        .expect_err("oversized active key should be rejected");
    assert_eq!(error, WorkflowBuildError::ActiveKeyTooLong);
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
    assert!(!step.allows_handler_continuation());
    assert_eq!(step.execution_resource_key(), None);
}

#[test]
fn workflow_step_enqueue_builder_exposes_validated_execution_variants() {
    let payload = serde_json::json!({"test": true});
    let job = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.job"),
        JobType::new("jobs.test.job"),
        &payload,
    )
    .priority(10)
    .max_attempts(3)
    .timeout_seconds(30)
    .stage(crate::jobs::JobStage::Scheduled)
    .allow_handler_continuation()
    .execution_resource("provider-account:job")
    .try_build()
    .expect("job step should build");

    match job.execution() {
        WorkflowStepExecution::Job(execution) => {
            assert_eq!(execution.job_type(), JobType::new("jobs.test.job"));
            assert_eq!(execution.priority(), Some(10));
            assert_eq!(execution.max_attempts(), Some(3));
            assert_eq!(execution.timeout_seconds(), Some(30));
            assert_eq!(execution.stage(), Some(crate::jobs::JobStage::Scheduled));
            assert!(execution.allows_handler_continuation());
            assert_eq!(
                execution.execution_resource_key(),
                Some("provider-account:job")
            );
        }
        WorkflowStepExecution::External => panic!("job builder must produce a job execution"),
    }

    let external =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("step.external"), &payload)
            .try_build()
            .expect("external step should build");
    assert!(matches!(
        external.execution(),
        WorkflowStepExecution::External
    ));
    assert_eq!(external.job_type(), None);
    assert_eq!(external.priority(), None);
    assert_eq!(external.max_attempts(), None);
    assert_eq!(external.timeout_seconds(), None);
    assert_eq!(external.stage(), None);
    assert!(!external.allows_handler_continuation());
    assert_eq!(external.execution_resource_key(), None);
}

#[test]
fn workflow_step_enqueue_builder_preserves_raw_validation_precedence() {
    let payload = serde_json::json!({"test": true});

    assert_eq!(
        WorkflowStepEnqueueBuilder::new(StepKey::new(" "), JobType::new(" "), &payload,)
            .max_attempts(0)
            .try_build()
            .expect_err("blank step key must win over other invalid job fields"),
        WorkflowBuildError::BlankStepKey { step_index: None }
    );
    assert_eq!(
        WorkflowStepEnqueueBuilder::new(StepKey::new("step.invalid"), JobType::new(" "), &payload,)
            .max_attempts(0)
            .try_build()
            .expect_err("blank job type must win over an invalid retry limit"),
        WorkflowBuildError::BlankStepJobType {
            step_key: "step.invalid".to_owned(),
        }
    );
    assert_eq!(
        WorkflowStepEnqueueBuilder::new(
            StepKey::new("step.dependency"),
            JobType::new("jobs.test.dependency"),
            &payload,
        )
        .dependency(WorkflowStepDependencySpec {
            prerequisite_step_key: StepKey::new(" "),
            release_mode: None,
        })
        .try_build()
        .expect_err("invalid dependencies must fail before a step is constructed"),
        WorkflowBuildError::BlankDependencyStepKey {
            step_key: "step.dependency".to_owned(),
        }
    );
}

#[test]
fn workflow_step_enqueue_debug_preserves_legacy_field_shape() {
    let payload = serde_json::json!({"test": true});
    let step = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.debug"),
        JobType::new("jobs.test.debug"),
        &payload,
    )
    .try_build()
    .expect("step should build");

    assert_eq!(
        format!("{step:?}"),
        "WorkflowStepEnqueue { step_key: StepKey(\"step.debug\"), execution_kind: Job, job_type: Some(JobType(\"jobs.test.debug\")), organization_id: None, payload: Object {\"test\": Bool(true)}, priority: None, max_attempts: None, timeout_seconds: None, stage: Some(Queued), allow_handler_continuation: false, execution_resource_key: None, dependencies: [] }"
    );
}

#[test]
fn workflow_step_enqueue_builder_explicitly_opts_job_step_into_handler_continuation() {
    let payload = serde_json::json!({"test": true});
    let step = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.a"),
        JobType::new("jobs.test.a"),
        &payload,
    )
    .allow_handler_continuation()
    .try_build()
    .expect("continuation-enabled job step should be valid");

    assert!(step.allows_handler_continuation());
}

#[test]
fn workflow_step_enqueue_builder_sets_and_validates_execution_resource() {
    let payload = serde_json::json!({"test": true});
    let step = WorkflowStepEnqueueBuilder::new(
        StepKey::new("step.a"),
        JobType::new("jobs.test.a"),
        &payload,
    )
    .execution_resource("provider-account:123")
    .try_build()
    .expect("resource-constrained job step should be valid");
    assert_eq!(step.execution_resource_key(), Some("provider-account:123"));

    assert_eq!(
        WorkflowStepEnqueueBuilder::new(
            StepKey::new("step.a"),
            JobType::new("jobs.test.a"),
            &payload,
        )
        .execution_resource(" ")
        .try_build()
        .expect_err("blank execution resource should be rejected"),
        WorkflowBuildError::InvalidStepExecutionResourceKey {
            step_key: "step.a".to_owned(),
        }
    );

    let oversized_resource_key = "x".repeat(513);
    assert_eq!(
        WorkflowStepEnqueueBuilder::new(
            StepKey::new("step.a"),
            JobType::new("jobs.test.a"),
            &payload,
        )
        .execution_resource(&oversized_resource_key)
        .try_build()
        .expect_err("oversized execution resource should be rejected"),
        WorkflowBuildError::InvalidStepExecutionResourceKey {
            step_key: "step.a".to_owned(),
        }
    );
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
fn workflow_step_enqueue_builder_rejects_execution_resources_on_external_steps() {
    let payload = serde_json::json!({"test": true});

    let error = WorkflowStepEnqueueBuilder::new_external(StepKey::new("step.external"), &payload)
        .execution_resource("provider-account:123")
        .try_build()
        .expect_err("external step execution resources should be rejected");

    assert_eq!(
        error,
        WorkflowBuildError::ExternalStepQueueSettingsNotAllowed {
            step_key: "step.external".to_owned(),
        }
    );
}

#[test]
fn workflow_step_enqueue_builder_rejects_handler_continuation_on_external_steps() {
    let payload = serde_json::json!({"test": true});

    let error = WorkflowStepEnqueueBuilder::new_external(StepKey::new("step.external"), &payload)
        .allow_handler_continuation()
        .try_build()
        .expect_err("external step continuation opt-in should be rejected");

    assert_eq!(
        error,
        WorkflowBuildError::ExternalStepQueueSettingsNotAllowed {
            step_key: "step.external".to_owned(),
        }
    );
}

#[test]
fn workflow_step_enqueue_builder_rejects_non_positive_execution_limits() {
    let payload = serde_json::json!({"test": true});

    assert!(
        WorkflowStepEnqueueBuilder::new(
            StepKey::new("step.zero_attempts"),
            JobType::new("jobs.test.a"),
            &payload,
        )
        .max_attempts(0)
        .try_build()
        .is_err(),
        "max_attempts must match the positive persisted workflow constraint"
    );

    assert!(
        WorkflowStepEnqueueBuilder::new(
            StepKey::new("step.zero_timeout"),
            JobType::new("jobs.test.a"),
            &payload,
        )
        .timeout_seconds(0)
        .try_build()
        .is_err(),
        "timeout_seconds must match the positive persisted workflow constraint"
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
fn workflow_dependency_effective_release_mode_defaults_only_when_omitted() {
    let omitted = WorkflowStepDependencySpec {
        prerequisite_step_key: StepKey::new("step.omitted"),
        release_mode: None,
    };
    let terminal = WorkflowStepDependencySpec {
        prerequisite_step_key: StepKey::new("step.terminal"),
        release_mode: Some(WorkflowDependencyReleaseMode::OnTerminal),
    };
    let success = WorkflowStepDependencySpec {
        prerequisite_step_key: StepKey::new("step.success"),
        release_mode: Some(WorkflowDependencyReleaseMode::OnSuccess),
    };

    assert_eq!(omitted.release_mode, None);
    assert_eq!(
        omitted.effective_release_mode(),
        WorkflowDependencyReleaseMode::OnTerminal
    );
    assert_eq!(
        terminal.effective_release_mode(),
        WorkflowDependencyReleaseMode::OnTerminal
    );
    assert_eq!(
        success.effective_release_mode(),
        WorkflowDependencyReleaseMode::OnSuccess
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

#[test]
fn workflow_dag_builder_try_new_rejects_blank_workflow_type() {
    let metadata = serde_json::json!({"source": "dag-builder-test"});

    let error = WorkflowDagBuilder::try_new("   ", &metadata)
        .expect_err("blank workflow type should be rejected");

    assert_eq!(error, WorkflowBuildError::BlankWorkflowType);
}

#[test]
fn workflow_dag_builder_job_rejects_blank_step_key() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "dag-builder-test"});

    let error = WorkflowDagBuilder::new("workflow.test", &metadata)
        .job("   ", "jobs.test.a", &payload)
        .expect_err("blank step key should be rejected");

    assert_eq!(error, WorkflowBuildError::BlankStepKey { step_index: None });
}

#[test]
fn workflow_dag_builder_job_rejects_blank_job_type() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "dag-builder-test"});

    let error = WorkflowDagBuilder::new("workflow.test", &metadata)
        .job("step.a", "   ", &payload)
        .expect_err("blank job type should be rejected");

    assert_eq!(
        error,
        WorkflowBuildError::BlankStepJobType {
            step_key: "step.a".to_owned(),
        }
    );
}

#[test]
fn workflow_dag_builder_try_build_rejects_empty_steps() {
    let metadata = serde_json::json!({"source": "dag-builder-test"});

    let error = WorkflowDagBuilder::new("workflow.test", &metadata)
        .try_build()
        .expect_err("empty workflow should be rejected at build");

    assert_eq!(error, WorkflowBuildError::EmptySteps);
}

#[test]
fn workflow_dag_builder_try_build_rejects_blank_workflow_type_from_new() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "dag-builder-test"});

    let error = WorkflowDagBuilder::new("   ", &metadata)
        .job("step.a", "jobs.test.a", &payload)
        .expect("step.a should be added")
        .try_build()
        .expect_err("blank workflow type should be rejected at build");

    assert_eq!(error, WorkflowBuildError::BlankWorkflowType);
}

#[test]
fn workflow_dag_builder_try_build_rejects_blank_idempotency_key() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "dag-builder-test"});

    let error = WorkflowDagBuilder::new("workflow.test", &metadata)
        .idempotency_key("   ")
        .job("step.a", "jobs.test.a", &payload)
        .expect("step.a should be added")
        .try_build()
        .expect_err("blank idempotency key should be rejected at build");

    assert_eq!(error, WorkflowBuildError::BlankIdempotencyKey);
}

#[test]
fn workflow_dag_builder_try_build_rejects_self_dependency() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "dag-builder-test"});

    let error = WorkflowDagBuilder::new("workflow.test", &metadata)
        .job("step.a", "jobs.test.a", &payload)
        .expect("step.a should be added")
        .after_success("step.a", ["step.a"])
        .expect("self dependency should attach before build validation")
        .try_build()
        .expect_err("self dependency should be rejected at build");

    assert_eq!(
        error,
        WorkflowBuildError::SelfDependency {
            step_key: "step.a".to_owned(),
        }
    );
}

#[test]
fn workflow_dag_builder_try_build_rejects_duplicate_dependency() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "dag-builder-test"});

    let error = WorkflowDagBuilder::new("workflow.test", &metadata)
        .job("step.a", "jobs.test.a", &payload)
        .expect("step.a should be added")
        .job("step.b", "jobs.test.b", &payload)
        .expect("step.b should be added")
        .after_success("step.b", ["step.a", "step.a"])
        .expect("duplicate dependency should attach before build validation")
        .try_build()
        .expect_err("duplicate dependency should be rejected at build");

    assert_eq!(
        error,
        WorkflowBuildError::DuplicateDependency {
            step_key: "step.b".to_owned(),
            prerequisite_step_key: "step.a".to_owned(),
        }
    );
}

#[test]
fn workflow_dag_builder_builds_success_dependency() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "dag-builder-test"});

    let run = WorkflowDagBuilder::new("workflow.test", &metadata)
        .job("crawl", "jobs.test.crawl", &payload)
        .expect("crawl step should be added")
        .job("classify", "jobs.test.classify", &payload)
        .expect("classify step should be added")
        .after_success("classify", ["crawl"])
        .expect("success dependency should attach")
        .try_build()
        .expect("workflow payload should be valid");

    assert_eq!(run.workflow_type(), WorkflowType::new("workflow.test"));
    assert_eq!(run.steps().len(), 2);
    assert_eq!(run.steps()[1].step_key(), StepKey::new("classify"));
    assert_eq!(run.steps()[1].dependencies().len(), 1);
    assert_eq!(
        run.steps()[1].dependencies()[0].release_mode,
        Some(WorkflowDependencyReleaseMode::OnSuccess)
    );
    assert_eq!(
        run.steps()[1].dependencies()[0].prerequisite_step_key,
        StepKey::new("crawl")
    );
}

#[test]
fn workflow_dag_builder_builds_terminal_dependency() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "dag-builder-test"});

    let run = WorkflowDagBuilder::new("workflow.test", &metadata)
        .job("step.a", "jobs.test.a", &payload)
        .expect("step.a should be added")
        .job("step.b", "jobs.test.b", &payload)
        .expect("step.b should be added")
        .after_terminal("step.b", ["step.a"])
        .expect("terminal dependency should attach")
        .try_build()
        .expect("workflow payload should be valid");

    assert_eq!(
        run.steps()[1].dependencies()[0].release_mode,
        Some(WorkflowDependencyReleaseMode::OnTerminal)
    );
}

#[test]
fn workflow_dag_builder_preserves_scope_and_idempotency() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "dag-builder-test"});
    let organization_id = Uuid::now_v7();

    let run = WorkflowDagBuilder::new("workflow.test", &metadata)
        .organization_id(organization_id)
        .idempotency_key("idempotency-key")
        .job("step.a", "jobs.test.a", &payload)
        .expect("step.a should be added")
        .try_build()
        .expect("workflow payload should be valid");

    assert_eq!(run.organization_id(), Some(organization_id));
    assert_eq!(run.idempotency_key(), Some("idempotency-key"));
}

#[test]
fn workflow_dag_builder_rejects_duplicate_step_keys() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "dag-builder-test"});

    let error = WorkflowDagBuilder::new("workflow.test", &metadata)
        .job("step.a", "jobs.test.a", &payload)
        .expect("first step.a should be added")
        .job("step.a", "jobs.test.b", &payload)
        .expect_err("duplicate step key should be rejected");

    assert_eq!(
        error,
        WorkflowBuildError::DuplicateStepKey {
            step_key: "step.a".to_owned(),
        }
    );
}

#[test]
fn workflow_dag_builder_rejects_unknown_dependency_target() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "dag-builder-test"});

    let error = WorkflowDagBuilder::new("workflow.test", &metadata)
        .job("crawl", "jobs.test.crawl", &payload)
        .expect("crawl step should be added")
        .after_success("classify", ["crawl"])
        .expect_err("unknown target step should be rejected");

    assert_eq!(
        error,
        WorkflowBuildError::UnknownStepKey {
            step_key: "classify".to_owned(),
        }
    );
}

#[test]
fn workflow_dag_builder_rejects_blank_target_step_key() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "dag-builder-test"});

    let error = WorkflowDagBuilder::new("workflow.test", &metadata)
        .job("crawl", "jobs.test.crawl", &payload)
        .expect("crawl step should be added")
        .after_success("   ", ["crawl"])
        .expect_err("blank target step key should be rejected");

    assert_eq!(error, WorkflowBuildError::BlankStepKey { step_index: None });
}

#[test]
fn workflow_dag_builder_rejects_blank_prerequisite_step_key() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "dag-builder-test"});

    let error = WorkflowDagBuilder::new("workflow.test", &metadata)
        .job("classify", "jobs.test.classify", &payload)
        .expect("classify step should be added")
        .after_success("classify", ["   "])
        .expect_err("blank prerequisite step key should be rejected");

    assert_eq!(
        error,
        WorkflowBuildError::BlankDependencyStepKey {
            step_key: "classify".to_owned(),
        }
    );
}

#[test]
fn workflow_dag_builder_rejects_missing_prerequisite_on_build() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "dag-builder-test"});

    let error = WorkflowDagBuilder::new("workflow.test", &metadata)
        .job("step.b", "jobs.test.b", &payload)
        .expect("step.b should be added")
        .after_success("step.b", ["missing"])
        .expect("dependency edge should attach")
        .try_build()
        .expect_err("missing prerequisite should be rejected at build");

    assert_eq!(
        error,
        WorkflowBuildError::MissingDependency {
            step_key: "step.b".to_owned(),
            prerequisite_step_key: "missing".to_owned(),
        }
    );
}

#[test]
fn workflow_dag_builder_detects_cycles_on_build() {
    let payload = serde_json::json!({"test": true});
    let metadata = serde_json::json!({"source": "dag-builder-test"});

    let error = WorkflowDagBuilder::new("workflow.test", &metadata)
        .job("a", "jobs.test.a", &payload)
        .expect("step a should be added")
        .job("b", "jobs.test.b", &payload)
        .expect("step b should be added")
        .job("c", "jobs.test.c", &payload)
        .expect("step c should be added")
        .after_success("a", ["c"])
        .expect("a -> c dependency should attach")
        .after_success("b", ["a"])
        .expect("b -> a dependency should attach")
        .after_success("c", ["b"])
        .expect("c -> b dependency should attach")
        .try_build()
        .expect_err("cycle should be rejected at build");

    assert_eq!(error, WorkflowBuildError::CycleDetected);
}
