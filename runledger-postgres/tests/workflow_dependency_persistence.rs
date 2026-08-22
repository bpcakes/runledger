use std::collections::BTreeMap;

use runledger_core::jobs::{
    StepKey, WorkflowDependencyReleaseMode, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder,
    WorkflowType,
};
use runledger_postgres::jobs::{
    AppendWorkflowStepsInput, append_workflow_steps, enqueue_workflow_run,
    list_workflow_step_dependencies, list_workflow_steps,
};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;

#[tokio::test]
async fn dependency_writes_preserve_edge_orientation_and_release_modes_for_appended_steps() {
    let (pool, database) = setup_ephemeral_pool("workflow_dependency_persistence", 8).await;
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("read PostgreSQL server_version");
    let server_version_num =
        sqlx::query_scalar::<_, i32>("SELECT current_setting('server_version_num')::int")
            .fetch_one(&pool)
            .await
            .expect("read PostgreSQL server_version_num");
    eprintln!(
        "workflow dependency persistence PostgreSQL server_version={server_version}, \
         server_version_num={server_version_num}"
    );
    assert_eq!(
        server_version_num / 10_000,
        18,
        "dependency persistence regression must run on PostgreSQL 18"
    );

    let payload = json!({"kind": "dependency-persistence"});
    let metadata = json!({"source": "test"});
    let gate = WorkflowStepEnqueueBuilder::new_external(StepKey::new("gate"), &payload)
        .try_build()
        .expect("build append-window gate");
    let initial_dependent =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("initial-dependent"), &payload)
            .depends_on_terminal(&[StepKey::new("gate")])
            .try_build()
            .expect("build initial dependent");
    let workflow = WorkflowRunEnqueueBuilder::new(
        WorkflowType::new("workflow.test.dependency_persistence"),
        &metadata,
    )
    .step(gate)
    .step(initial_dependent)
    .try_build()
    .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow");

    let appended_from_existing =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("appended-from-existing"), &payload)
            .depends_on_success(&[StepKey::new("gate")])
            .try_build()
            .expect("build step depending on existing gate");
    let appended_from_new =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("appended-from-new"), &payload)
            .depends_on_terminal(&[StepKey::new("appended-from-existing")])
            .try_build()
            .expect("build step depending on newly appended step");
    let mutation_metadata = json!({});
    append_workflow_steps(
        &pool,
        &AppendWorkflowStepsInput {
            workflow_run_id: run.id,
            organization_id: None,
            mutation_key: "append-dependency-edges",
            mutation_metadata: &mutation_metadata,
            append_window_step_key: StepKey::new("gate"),
            steps: vec![appended_from_existing, appended_from_new],
        },
    )
    .await
    .expect("append dependent workflow steps");

    let step_keys_by_id = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list workflow steps")
        .into_iter()
        .map(|step| (step.id, step.step_key.as_str().to_owned()))
        .collect::<BTreeMap<_, _>>();
    let mut persisted_edges = list_workflow_step_dependencies(&pool, None, run.id)
        .await
        .expect("list workflow step dependencies")
        .into_iter()
        .map(|dependency| {
            (
                step_keys_by_id
                    .get(&dependency.prerequisite_step_id)
                    .expect("prerequisite step id resolves")
                    .clone(),
                step_keys_by_id
                    .get(&dependency.dependent_step_id)
                    .expect("dependent step id resolves")
                    .clone(),
                dependency.release_mode,
            )
        })
        .collect::<Vec<_>>();
    persisted_edges.sort_by(|left, right| left.1.cmp(&right.1));

    assert_eq!(
        persisted_edges,
        vec![
            (
                "gate".to_owned(),
                "appended-from-existing".to_owned(),
                WorkflowDependencyReleaseMode::OnSuccess,
            ),
            (
                "appended-from-existing".to_owned(),
                "appended-from-new".to_owned(),
                WorkflowDependencyReleaseMode::OnTerminal,
            ),
            (
                "gate".to_owned(),
                "initial-dependent".to_owned(),
                WorkflowDependencyReleaseMode::OnTerminal,
            ),
        ]
    );

    teardown_ephemeral_pool(pool, database).await;
}
