use std::collections::BTreeSet;

use crate::jobs::{
    JobStage, JobType, StepKey, StepKeyName, WorkflowStepExecutionKind, WorkflowType,
};

use super::super::{
    WorkflowDagDependencyValidationInput, WorkflowDagStepValidationInput,
    WorkflowDagValidationError, WorkflowRunEnqueue, WorkflowStepDependencySpec,
    WorkflowStepEnqueue, WorkflowStepEnqueueBuilder, WorkflowStepExecution, validate_workflow_dag,
    validate_workflow_run_enqueue, validate_workflow_step_append,
};

fn dag_step<'a>(
    step_key: &'a str,
    execution_kind: WorkflowStepExecutionKind,
    job_type: Option<&'a str>,
    stage: Option<JobStage>,
    dependencies: Vec<&'a str>,
) -> WorkflowDagStepValidationInput<'a> {
    let dependencies = dependencies
        .into_iter()
        .map(
            |prerequisite_step_key| WorkflowDagDependencyValidationInput {
                prerequisite_step_key: StepKey::new(prerequisite_step_key),
            },
        )
        .collect();
    WorkflowDagStepValidationInput::new(
        StepKey::new(step_key),
        execution_kind,
        job_type.map(JobType::new),
        dependencies,
    )
    .stage(stage)
}

fn job_step<'a>(
    step_key: &'a str,
    job_type: &'a str,
    payload: &'a serde_json::Value,
    dependencies: Vec<WorkflowStepDependencySpec<'a>>,
) -> WorkflowStepEnqueue<'a> {
    WorkflowStepEnqueueBuilder::new(StepKey::new(step_key), JobType::new(job_type), payload)
        .set_dependencies(dependencies)
        .try_build()
        .expect("test step should be valid")
}

fn unchecked_external_step<'a>(
    step_key: &'a str,
    payload: &'a serde_json::Value,
    dependencies: Vec<WorkflowStepDependencySpec<'a>>,
) -> WorkflowStepEnqueue<'a> {
    WorkflowStepEnqueue {
        step_key: StepKey::new(step_key),
        execution: WorkflowStepExecution::External,
        organization_id: None,
        payload,
        dependencies,
    }
}

#[test]
fn workflow_dag_validation_rejects_missing_dependencies() {
    let result = validate_workflow_dag(
        WorkflowType::new("workflow.test"),
        &[
            dag_step(
                "a",
                WorkflowStepExecutionKind::Job,
                Some("jobs.test.a"),
                Some(JobStage::Queued),
                vec![],
            ),
            dag_step(
                "b",
                WorkflowStepExecutionKind::Job,
                Some("jobs.test.b"),
                Some(JobStage::Queued),
                vec!["missing"],
            ),
        ],
    );

    assert!(matches!(
        result,
        Err(WorkflowDagValidationError::MissingDependency {
            step_key,
            prerequisite_step_key
        }) if step_key == "b" && prerequisite_step_key == "missing"
    ));
}

#[test]
fn workflow_dag_validation_rejects_cycles() {
    let result = validate_workflow_dag(
        WorkflowType::new("workflow.test"),
        &[
            dag_step(
                "a",
                WorkflowStepExecutionKind::Job,
                Some("jobs.test.a"),
                Some(JobStage::Queued),
                vec!["c"],
            ),
            dag_step(
                "b",
                WorkflowStepExecutionKind::Job,
                Some("jobs.test.b"),
                Some(JobStage::Queued),
                vec!["a"],
            ),
            dag_step(
                "c",
                WorkflowStepExecutionKind::Job,
                Some("jobs.test.c"),
                Some(JobStage::Queued),
                vec!["b"],
            ),
        ],
    );

    assert!(matches!(
        result,
        Err(WorkflowDagValidationError::CycleDetected)
    ));
}

#[test]
fn workflow_dag_validation_rejects_blank_workflow_type() {
    let result = validate_workflow_dag(
        WorkflowType::new("  "),
        &[dag_step(
            "a",
            WorkflowStepExecutionKind::Job,
            Some("jobs.test.a"),
            Some(JobStage::Queued),
            vec![],
        )],
    );

    assert_eq!(result, Err(WorkflowDagValidationError::BlankWorkflowType));
}

#[test]
fn workflow_dag_validation_rejects_blank_step_key() {
    let result = validate_workflow_dag(
        WorkflowType::new("workflow.test"),
        &[dag_step(
            " ",
            WorkflowStepExecutionKind::Job,
            Some("jobs.test.a"),
            Some(JobStage::Queued),
            vec![],
        )],
    );

    assert_eq!(
        result,
        Err(WorkflowDagValidationError::BlankStepKey { step_index: 0 })
    );
}

#[test]
fn workflow_dag_validation_rejects_blank_step_job_type() {
    let result = validate_workflow_dag(
        WorkflowType::new("workflow.test"),
        &[dag_step(
            "a",
            WorkflowStepExecutionKind::Job,
            Some(" "),
            Some(JobStage::Queued),
            vec![],
        )],
    );

    assert_eq!(
        result,
        Err(WorkflowDagValidationError::BlankStepJobType {
            step_key: "a".to_owned(),
        })
    );
}

#[test]
fn workflow_dag_validation_rejects_blank_dependency_step_key() {
    let result = validate_workflow_dag(
        WorkflowType::new("workflow.test"),
        &[
            dag_step(
                "a",
                WorkflowStepExecutionKind::Job,
                Some("jobs.test.a"),
                Some(JobStage::Queued),
                vec![],
            ),
            dag_step(
                "b",
                WorkflowStepExecutionKind::Job,
                Some("jobs.test.b"),
                Some(JobStage::Queued),
                vec![" "],
            ),
        ],
    );

    assert_eq!(
        result,
        Err(WorkflowDagValidationError::BlankDependencyStepKey {
            step_key: "b".to_owned(),
        })
    );
}

#[test]
fn workflow_dag_validation_rejects_duplicate_step_keys() {
    let result = validate_workflow_dag(
        WorkflowType::new("workflow.test"),
        &[
            dag_step(
                "dup",
                WorkflowStepExecutionKind::Job,
                Some("jobs.test.a"),
                Some(JobStage::Queued),
                vec![],
            ),
            dag_step(
                "dup",
                WorkflowStepExecutionKind::Job,
                Some("jobs.test.b"),
                Some(JobStage::Queued),
                vec![],
            ),
        ],
    );

    assert_eq!(
        result,
        Err(WorkflowDagValidationError::DuplicateStepKey {
            step_key: "dup".to_owned(),
        })
    );
}

#[test]
fn workflow_dag_validation_checks_duplicate_step_keys_before_dependencies() {
    let result = validate_workflow_dag(
        WorkflowType::new("workflow.test"),
        &[
            dag_step(
                "duplicate",
                WorkflowStepExecutionKind::Job,
                Some("jobs.test.a"),
                Some(JobStage::Queued),
                vec!["missing"],
            ),
            dag_step(
                "duplicate",
                WorkflowStepExecutionKind::Job,
                Some("jobs.test.b"),
                Some(JobStage::Queued),
                vec![],
            ),
        ],
    );

    assert_eq!(
        result,
        Err(WorkflowDagValidationError::DuplicateStepKey {
            step_key: "duplicate".to_owned(),
        })
    );
}

#[test]
fn workflow_dag_validation_rejects_duplicate_dependencies() {
    let result = validate_workflow_dag(
        WorkflowType::new("workflow.test"),
        &[
            dag_step(
                "a",
                WorkflowStepExecutionKind::Job,
                Some("jobs.test.a"),
                Some(JobStage::Queued),
                vec![],
            ),
            dag_step(
                "b",
                WorkflowStepExecutionKind::Job,
                Some("jobs.test.b"),
                Some(JobStage::Queued),
                vec!["a", "a"],
            ),
        ],
    );

    assert_eq!(
        result,
        Err(WorkflowDagValidationError::DuplicateDependency {
            step_key: "b".to_owned(),
            prerequisite_step_key: "a".to_owned(),
        })
    );
}

#[test]
fn validate_workflow_run_enqueue_rejects_blank_workflow_type() {
    let metadata = serde_json::json!({"source": "test"});
    let payload = serde_json::json!({"ok": true});
    let enqueue = WorkflowRunEnqueue {
        workflow_type: WorkflowType::new(" "),
        organization_id: None,
        metadata: &metadata,
        idempotency_key: None,
        active_key: None,
        result_step_key: None,
        steps: vec![job_step("a", "jobs.test.a", &payload, Vec::new())],
    };

    let result = validate_workflow_run_enqueue(&enqueue);

    assert_eq!(result, Err(WorkflowDagValidationError::BlankWorkflowType));
}

#[test]
fn validate_workflow_run_enqueue_returns_step_dag_errors() {
    let metadata = serde_json::json!({"source": "test"});
    let payload = serde_json::json!({"ok": true});
    let enqueue = WorkflowRunEnqueue {
        workflow_type: WorkflowType::new("workflow.test"),
        organization_id: None,
        metadata: &metadata,
        idempotency_key: None,
        active_key: None,
        result_step_key: None,
        steps: vec![unchecked_external_step(
            "a",
            &payload,
            vec![WorkflowStepDependencySpec {
                prerequisite_step_key: StepKey::new("a"),
                release_mode: None,
            }],
        )],
    };

    let result = validate_workflow_run_enqueue(&enqueue);

    assert_eq!(
        result,
        Err(WorkflowDagValidationError::SelfDependency {
            step_key: "a".to_owned(),
        })
    );
}

#[test]
fn validate_workflow_run_enqueue_rejects_blank_result_step_key() {
    let metadata = serde_json::json!({"source": "test"});
    let payload = serde_json::json!({"ok": true});
    let enqueue = WorkflowRunEnqueue {
        workflow_type: WorkflowType::new("workflow.test"),
        organization_id: None,
        metadata: &metadata,
        idempotency_key: None,
        active_key: None,
        result_step_key: Some(StepKey::new(" ")),
        steps: vec![job_step("a", "jobs.test.a", &payload, Vec::new())],
    };

    let result = validate_workflow_run_enqueue(&enqueue);

    assert_eq!(result, Err(WorkflowDagValidationError::BlankResultStepKey));
}

#[test]
fn validate_workflow_run_enqueue_rejects_unknown_result_step_key() {
    let metadata = serde_json::json!({"source": "test"});
    let payload = serde_json::json!({"ok": true});
    let enqueue = WorkflowRunEnqueue {
        workflow_type: WorkflowType::new("workflow.test"),
        organization_id: None,
        metadata: &metadata,
        idempotency_key: None,
        active_key: None,
        result_step_key: Some(StepKey::new("missing")),
        steps: vec![job_step("a", "jobs.test.a", &payload, Vec::new())],
    };

    let result = validate_workflow_run_enqueue(&enqueue);

    assert_eq!(
        result,
        Err(WorkflowDagValidationError::UnknownResultStepKey {
            step_key: "missing".to_owned(),
        })
    );
}

#[test]
fn validate_workflow_step_append_rejects_blank_new_step_key() {
    let existing_step_keys = BTreeSet::new();
    let payload = serde_json::json!({"ok": true});
    let new_steps = vec![unchecked_external_step(" ", &payload, Vec::new())];

    let result = validate_workflow_step_append(&existing_step_keys, &new_steps);

    assert_eq!(
        result,
        Err(WorkflowDagValidationError::BlankStepKey { step_index: 0 })
    );
}

#[test]
fn validate_workflow_step_append_rejects_duplicate_existing_step_key() {
    let existing_step_keys = BTreeSet::from([StepKeyName::new("step.a").expect("valid step key")]);
    let payload = serde_json::json!({"ok": true});
    let new_steps = vec![job_step("step.a", "jobs.test.a", &payload, Vec::new())];

    let result = validate_workflow_step_append(&existing_step_keys, &new_steps);

    assert_eq!(
        result,
        Err(WorkflowDagValidationError::DuplicateStepKey {
            step_key: "step.a".to_owned(),
        })
    );
}

#[test]
fn validate_workflow_step_append_checks_each_step_before_duplicate_step_keys() {
    let existing_step_keys = BTreeSet::new();
    let payload = serde_json::json!({"ok": true});
    let new_steps = vec![
        unchecked_external_step(
            "duplicate",
            &payload,
            vec![WorkflowStepDependencySpec {
                prerequisite_step_key: StepKey::new(" "),
                release_mode: None,
            }],
        ),
        unchecked_external_step("duplicate", &payload, Vec::new()),
    ];

    let result = validate_workflow_step_append(&existing_step_keys, &new_steps);

    assert_eq!(
        result,
        Err(WorkflowDagValidationError::BlankDependencyStepKey {
            step_key: "duplicate".to_owned(),
        })
    );
}

#[test]
fn validate_workflow_step_append_rejects_cycles_within_new_steps() {
    let existing_step_keys = BTreeSet::new();
    let payload = serde_json::json!({"ok": true});
    let new_steps = vec![
        job_step(
            "a",
            "jobs.test.a",
            &payload,
            vec![WorkflowStepDependencySpec {
                prerequisite_step_key: StepKey::new("b"),
                release_mode: None,
            }],
        ),
        job_step(
            "b",
            "jobs.test.b",
            &payload,
            vec![WorkflowStepDependencySpec {
                prerequisite_step_key: StepKey::new("a"),
                release_mode: None,
            }],
        ),
    ];

    let result = validate_workflow_step_append(&existing_step_keys, &new_steps);

    assert_eq!(result, Err(WorkflowDagValidationError::CycleDetected));
}

#[test]
fn workflow_dag_validation_rejects_job_type_on_external_step() {
    let result = validate_workflow_dag(
        WorkflowType::new("workflow.test"),
        &[dag_step(
            "a",
            WorkflowStepExecutionKind::External,
            Some("jobs.test.a"),
            None,
            vec![],
        )],
    );

    assert_eq!(
        result,
        Err(WorkflowDagValidationError::ExternalStepJobTypeNotAllowed {
            step_key: "a".to_owned(),
        })
    );
}

#[test]
fn workflow_dag_validation_rejects_queue_settings_on_external_step() {
    let result = validate_workflow_dag(
        WorkflowType::new("workflow.test"),
        &[WorkflowDagStepValidationInput::new(
            StepKey::new("a"),
            WorkflowStepExecutionKind::External,
            None,
            Vec::new(),
        )
        .priority(Some(10))],
    );

    assert_eq!(
        result,
        Err(
            WorkflowDagValidationError::ExternalStepQueueSettingsNotAllowed {
                step_key: "a".to_owned(),
            }
        )
    );
}

#[test]
fn workflow_dag_validation_rejects_handler_continuation_on_external_step() {
    let result = validate_workflow_dag(
        WorkflowType::new("workflow.test"),
        &[WorkflowDagStepValidationInput::new(
            StepKey::new("a"),
            WorkflowStepExecutionKind::External,
            None,
            Vec::new(),
        )
        .allow_handler_continuation(true)],
    );

    assert_eq!(
        result,
        Err(
            WorkflowDagValidationError::ExternalStepQueueSettingsNotAllowed {
                step_key: "a".to_owned(),
            }
        )
    );
}
