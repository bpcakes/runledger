use crate::jobs::{JobStage, JobType, StepKey, WorkflowStepExecutionKind, WorkflowType};

use super::super::{
    WorkflowDagDependencyValidationInput, WorkflowDagStepValidationInput,
    WorkflowDagValidationError, WorkflowRunEnqueue, WorkflowStepDependencySpec,
    WorkflowStepEnqueue, validate_workflow_dag, validate_workflow_run_enqueue,
};

fn dag_step<'a>(
    step_key: &'a str,
    execution_kind: WorkflowStepExecutionKind,
    job_type: Option<&'a str>,
    stage: Option<JobStage>,
    dependencies: Vec<&'a str>,
) -> WorkflowDagStepValidationInput<'a> {
    WorkflowDagStepValidationInput {
        step_key: StepKey::new(step_key),
        execution_kind,
        job_type: job_type.map(JobType::new),
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        stage,
        dependencies: dependencies
            .into_iter()
            .map(
                |prerequisite_step_key| WorkflowDagDependencyValidationInput {
                    prerequisite_step_key: StepKey::new(prerequisite_step_key),
                },
            )
            .collect(),
    }
}

#[test]
fn workflow_dag_validation_rejects_missing_dependencies() {
    let result = validate_workflow_dag(
        WorkflowType::new("workflow.test"),
        &[
            WorkflowDagStepValidationInput {
                step_key: StepKey::new("a"),
                execution_kind: WorkflowStepExecutionKind::Job,
                job_type: Some(JobType::new("jobs.test.a")),
                priority: None,
                max_attempts: None,
                timeout_seconds: None,
                stage: Some(JobStage::Queued),
                dependencies: Vec::new(),
            },
            WorkflowDagStepValidationInput {
                step_key: StepKey::new("b"),
                execution_kind: WorkflowStepExecutionKind::Job,
                job_type: Some(JobType::new("jobs.test.b")),
                priority: None,
                max_attempts: None,
                timeout_seconds: None,
                stage: Some(JobStage::Queued),
                dependencies: vec![WorkflowDagDependencyValidationInput {
                    prerequisite_step_key: StepKey::new("missing"),
                }],
            },
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
fn validate_workflow_run_enqueue_returns_dag_errors() {
    let metadata = serde_json::json!({"source": "test"});
    let payload = serde_json::json!({"ok": true});
    let enqueue = WorkflowRunEnqueue {
        workflow_type: WorkflowType::new(" "),
        organization_id: None,
        metadata: &metadata,
        idempotency_key: None,
        steps: vec![WorkflowStepEnqueue {
            step_key: StepKey::new("a"),
            execution_kind: WorkflowStepExecutionKind::Job,
            job_type: Some(JobType::new("jobs.test.a")),
            organization_id: None,
            payload: &payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            stage: Some(JobStage::Queued),
            dependencies: vec![WorkflowStepDependencySpec {
                prerequisite_step_key: StepKey::new("a"),
                release_mode: None,
            }],
        }],
    };

    let result = validate_workflow_run_enqueue(&enqueue);

    assert_eq!(result, Err(WorkflowDagValidationError::BlankWorkflowType));
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
        &[WorkflowDagStepValidationInput {
            step_key: StepKey::new("a"),
            execution_kind: WorkflowStepExecutionKind::External,
            job_type: None,
            priority: Some(10),
            max_attempts: None,
            timeout_seconds: None,
            stage: None,
            dependencies: Vec::new(),
        }],
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
