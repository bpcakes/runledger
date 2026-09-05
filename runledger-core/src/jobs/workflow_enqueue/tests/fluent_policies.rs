use serde_json::json;
use uuid::Uuid;

use crate::jobs::{
    JobStage, StepKey, WorkflowBuildError, WorkflowDagBuilder, WorkflowDependencyReleaseMode,
    WorkflowStepDependencySpec, WorkflowStepEnqueueBuilder, WorkflowStepExecution,
};

#[test]
fn configured_steps_preserve_all_fields_and_append_edges() {
    let payload = json!({"account": "a"});
    let organization = Uuid::from_u128(42);
    let step = WorkflowStepEnqueueBuilder::try_new("work", "enrich", &payload)
        .expect("valid workflow input")
        .organization_id(organization)
        .priority(8)
        .max_attempts(3)
        .timeout_seconds(90)
        .stage(JobStage::Scheduled)
        .allow_handler_continuation()
        .execution_resource("provider")
        .dependency(WorkflowStepDependencySpec {
            prerequisite_step_key: StepKey::new("approval"),
            release_mode: None,
        })
        .try_build()
        .expect("valid workflow input");
    let run = WorkflowDagBuilder::new("workflow", &payload)
        .step(step)
        .expect("valid workflow input")
        .external("approval", &payload)
        .expect("valid workflow input")
        .job("fetch", "fetch", &payload)
        .expect("valid workflow input")
        .after_success("work", ["fetch"])
        .expect("valid workflow input")
        .result_step("work")
        .expect("valid workflow input")
        .build()
        .expect("valid workflow input");
    let step = &run.steps()[0];
    assert_eq!(step.organization_id(), Some(organization));
    assert_eq!(step.payload(), &payload);
    assert_eq!(
        step.job_type().expect("valid workflow input").as_str(),
        "enrich"
    );
    assert_eq!(step.priority(), Some(8));
    assert_eq!(step.max_attempts(), Some(3));
    assert_eq!(step.timeout_seconds(), Some(90));
    assert_eq!(step.stage(), Some(JobStage::Scheduled));
    assert!(step.allows_handler_continuation());
    assert_eq!(step.execution_resource_key(), Some("provider"));
    assert_eq!(step.dependencies().len(), 2);
    assert_eq!(step.dependencies()[0].release_mode, None);
    assert_eq!(
        step.dependencies()[0].effective_release_mode(),
        WorkflowDependencyReleaseMode::OnTerminal
    );
    assert_eq!(
        step.dependencies()[1].release_mode,
        Some(WorkflowDependencyReleaseMode::OnSuccess)
    );
    assert_eq!(run.result_step_key(), Some(StepKey::new("work")));
    assert_eq!(run.steps()[1].execution(), WorkflowStepExecution::External);
}

#[test]
fn active_keys_validate_and_clear_independently_of_idempotency() {
    let payload = json!({});
    let base = WorkflowDagBuilder::try_new("workflow", &payload)
        .expect("valid workflow input")
        .idempotency_key("request")
        .external("approval", &payload)
        .expect("valid workflow input");
    for blank in ["", "  "] {
        assert_eq!(
            base.clone()
                .active_key(blank)
                .build()
                .expect_err("invalid workflow input"),
            WorkflowBuildError::BlankActiveKey
        );
    }
    let oversized = "é".repeat(257);
    assert_eq!(
        base.clone()
            .active_key(&oversized)
            .build()
            .expect_err("invalid workflow input"),
        WorkflowBuildError::ActiveKeyTooLong
    );
    let boundary = "é".repeat(256);
    let run = base
        .clone()
        .active_key(&boundary)
        .build()
        .expect("valid workflow input");
    assert_eq!(run.active_key(), Some(boundary.as_str()));
    assert_eq!(run.idempotency_key(), Some("request"));
    let run = base
        .active_key("  ")
        .clear_active_key()
        .build()
        .expect("valid workflow input");
    assert_eq!(run.active_key(), None);
    assert_eq!(run.idempotency_key(), Some("request"));
}

#[test]
fn external_steps_support_scope_dependencies_and_result_selection() {
    let payload = json!({"ticket": 7});
    let organization = Uuid::from_u128(7);
    let external = WorkflowStepEnqueueBuilder::try_new_external("approval", &payload)
        .expect("valid workflow input")
        .organization_id(organization)
        .try_build()
        .expect("valid workflow input");
    let run = WorkflowDagBuilder::new("workflow", &payload)
        .step(external)
        .expect("valid workflow input")
        .job("work", "work", &payload)
        .expect("valid workflow input")
        .external("done", &payload)
        .expect("valid workflow input")
        .after_terminal("approval", ["work"])
        .expect("valid workflow input")
        .after_success("done", ["approval"])
        .expect("valid workflow input")
        .result_step("done")
        .expect("valid workflow input")
        .build()
        .expect("valid workflow input");
    let step = &run.steps()[0];
    assert_eq!(step.organization_id(), Some(organization));
    assert_eq!(step.payload(), &payload);
    assert_eq!(step.execution(), WorkflowStepExecution::External);
    assert_eq!(
        step.dependencies()[0].release_mode,
        Some(WorkflowDependencyReleaseMode::OnTerminal)
    );
    assert_eq!(run.result_step_key(), Some(StepKey::new("done")));
}

#[test]
fn all_entry_points_share_duplicate_key_detection() {
    let payload = json!({});
    let configured = WorkflowStepEnqueueBuilder::try_new("step", "work", &payload)
        .expect("valid workflow input")
        .try_build()
        .expect("valid workflow input");
    let bases = [
        WorkflowDagBuilder::new("workflow", &payload)
            .job("step", "work", &payload)
            .expect("valid workflow input"),
        WorkflowDagBuilder::new("workflow", &payload)
            .step(configured.clone())
            .expect("valid workflow input"),
        WorkflowDagBuilder::new("workflow", &payload)
            .external("step", &payload)
            .expect("valid workflow input"),
    ];
    for base in bases {
        for result in [
            base.clone().job("step", "work", &payload),
            base.clone().step(configured.clone()),
            base.external("step", &payload),
        ] {
            assert_eq!(
                result.expect_err("invalid workflow input"),
                WorkflowBuildError::DuplicateStepKey {
                    step_key: "step".into()
                }
            );
        }
    }
    // Preserve the existing simple-job error precedence.
    let error = WorkflowDagBuilder::new("workflow", &payload)
        .external("step", &payload)
        .expect("valid workflow input")
        .job("step", " ", &payload)
        .expect_err("invalid workflow input");
    assert!(matches!(error, WorkflowBuildError::DuplicateStepKey { .. }));
    assert!(matches!(
        WorkflowDagBuilder::new("workflow", &payload).external(" ", &payload),
        Err(WorkflowBuildError::BlankStepKey { .. })
    ));
}

#[test]
fn configured_dependencies_still_receive_complete_graph_validation() {
    let payload = json!({});
    let configured = WorkflowStepEnqueueBuilder::try_new("work", "work", &payload)
        .expect("valid workflow input")
        .depends_on_terminal(&[StepKey::new("approval")])
        .try_build()
        .expect("valid workflow input");
    let base = WorkflowDagBuilder::new("workflow", &payload)
        .step(configured)
        .expect("valid workflow input");
    assert!(matches!(
        base.clone().build(),
        Err(WorkflowBuildError::MissingDependency { .. })
    ));
    let base = base
        .external("approval", &payload)
        .expect("valid workflow input");
    assert!(base.clone().build().is_ok());
    assert!(matches!(
        base.clone()
            .after_success("work", ["approval"])
            .expect("valid workflow input")
            .build(),
        Err(WorkflowBuildError::DuplicateDependency { .. })
    ));
    assert!(matches!(
        base.clone()
            .after_terminal("approval", ["approval"])
            .expect("valid workflow input")
            .build(),
        Err(WorkflowBuildError::SelfDependency { .. })
    ));
    assert!(matches!(
        base.clone()
            .after_success("approval", ["work"])
            .expect("valid workflow input")
            .build(),
        Err(WorkflowBuildError::CycleDetected)
    ));
    assert!(matches!(
        base.result_step("missing")
            .expect("valid workflow input")
            .build(),
        Err(WorkflowBuildError::UnknownResultStepKey { .. })
    ));
}

#[test]
fn fluent_build_preserves_step_error_precedence_over_run_fields() {
    let payload = json!({});
    let error = WorkflowDagBuilder::new(" ", &payload)
        .idempotency_key(" ")
        .job("work", "work", &payload)
        .expect("valid job")
        .after_success("work", ["work"])
        .expect("deferred self dependency")
        .build()
        .expect_err("self dependency precedes invalid run fields");
    assert_eq!(
        error,
        WorkflowBuildError::SelfDependency {
            step_key: "work".into()
        }
    );
}
