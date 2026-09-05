use runledger_core::jobs::{
    JobCompletion, JobContext, JobFailure, JobStage, JobType, StepKey, WorkflowBuildError,
    WorkflowDagBuilder, WorkflowDependencyReleaseMode, WorkflowRunEnqueue,
    WorkflowRunEnqueueBuilder, WorkflowStepEnqueue, WorkflowStepEnqueueBuilder,
    WorkflowStepExecution, WorkflowType,
};
use runledger_runtime::catalog::{CatalogError, JobCatalog, JobCatalogDefaults};
use runledger_runtime::registry::JobHandler;
use serde_json::{Value, json};
use uuid::Uuid;

const ENRICH: &str = "buyer.enrich";

struct EnrichHandler;

#[async_trait::async_trait]
impl JobHandler for EnrichHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(ENRICH)
    }

    async fn execute(&self, _: JobContext, _: Value) -> Result<JobCompletion, JobFailure> {
        unreachable!("builder-only fixture must never execute jobs")
    }
}

fn catalog() -> JobCatalog {
    JobCatalog::new().handler(EnrichHandler)
}

// OneSales buyer_enrichment/launcher.rs: account inputs retain dynamic keys and
// payloads, each account has its own tenant, all accounts share one provider
// resource, and each account releases the next on terminal completion.
struct AccountInput {
    key: String,
    organization: Uuid,
    payload: Value,
}

fn account_step(input: &AccountInput) -> WorkflowStepEnqueueBuilder<'_> {
    WorkflowStepEnqueueBuilder::try_new(&input.key, ENRICH, &input.payload)
        .expect("account identifiers")
        .organization_id(input.organization)
        .allow_handler_continuation()
        .execution_resource("buyer:provider")
}

fn low_level_buyer<'a>(
    inputs: &'a [AccountInput],
    metadata: &'a Value,
    organization: Option<Uuid>,
    active_key: &'a str,
) -> WorkflowRunEnqueue<'a> {
    let mut run = WorkflowRunEnqueueBuilder::new(WorkflowType::new("buyer.enrichment"), metadata)
        .idempotency_key("launcher:request")
        .active_key(active_key);
    if let Some(organization) = organization {
        run = run.organization_id(organization);
    }
    for (index, input) in inputs.iter().enumerate() {
        let mut step = account_step(input);
        if index > 0 {
            step = step.depends_on_terminal(&[StepKey::new(&inputs[index - 1].key)]);
        }
        run = run.step(step.try_build().expect("account step"));
    }
    run.try_build().expect("low-level buyer graph")
}

fn assert_same_request(actual: &WorkflowRunEnqueue<'_>, expected: &WorkflowRunEnqueue<'_>) {
    assert_eq!(actual.workflow_type(), expected.workflow_type());
    assert_eq!(actual.organization_id(), expected.organization_id());
    assert_eq!(actual.metadata(), expected.metadata());
    assert_eq!(actual.idempotency_key(), expected.idempotency_key());
    assert_eq!(actual.active_key(), expected.active_key());
    assert_eq!(actual.result_step_key(), expected.result_step_key());
    assert_eq!(actual.steps().len(), expected.steps().len());
    for (actual, expected) in actual.steps().iter().zip(expected.steps()) {
        assert_eq!(actual.step_key(), expected.step_key());
        assert_eq!(actual.execution(), expected.execution());
        assert_eq!(actual.organization_id(), expected.organization_id());
        assert_eq!(actual.payload(), expected.payload());
        let dependencies = |step: &WorkflowStepEnqueue<'_>| {
            step.dependencies()
                .iter()
                .map(|edge| {
                    (
                        edge.prerequisite_step_key.as_str().to_owned(),
                        edge.release_mode,
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(dependencies(actual), dependencies(expected));
    }
}

#[test]
fn buyer_enrichment_has_identical_policies_through_both_fluent_builders() {
    let inputs = [
        AccountInput {
            key: "account:000000:a".into(),
            organization: Uuid::from_u128(1),
            payload: json!({"account_id": "a", "version": 1}),
        },
        AccountInput {
            key: "account:000001:b".into(),
            organization: Uuid::from_u128(2),
            payload: json!({"account_id": "b", "version": 1}),
        },
    ];
    let catalog = catalog();
    for (organization, active_key) in [
        (None, "buyer:active"),
        (Some(inputs[0].organization), "buyer:active"),
        (Some(inputs[0].organization), "buyer:active:account:a"),
    ] {
        let inputs = if organization.is_some() {
            &inputs[..1]
        } else {
            &inputs[..]
        };
        let metadata = json!({"account_count": inputs.len()});
        let expected = low_level_buyer(inputs, &metadata, organization, active_key);
        let mut core = WorkflowDagBuilder::new("buyer.enrichment", &metadata)
            .idempotency_key("launcher:request")
            .active_key(active_key);
        let mut checked = catalog
            .workflow_dag("buyer.enrichment", &metadata)
            .idempotency_key("launcher:request")
            .active_key(active_key);
        if let Some(organization) = organization {
            core = core.organization_id(organization);
            checked = checked.organization_id(organization);
        }
        for (index, input) in inputs.iter().enumerate() {
            core = core
                .step(account_step(input).try_build().expect("account step"))
                .expect("core step");
            checked = checked
                .step(
                    catalog
                        .workflow_step(&input.key, ENRICH, &input.payload)
                        .expect("catalog account")
                        .organization_id(input.organization)
                        .allow_handler_continuation()
                        .execution_resource("buyer:provider")
                        .try_build()
                        .expect("catalog step"),
                )
                .expect("checked step");
            if index > 0 {
                core = core
                    .after_terminal(&input.key, [inputs[index - 1].key.as_str()])
                    .expect("core edge");
                checked = checked
                    .after_terminal(&input.key, [inputs[index - 1].key.as_str()])
                    .expect("catalog edge");
            }
        }
        for actual in [
            core.build().expect("core graph"),
            checked.build().expect("catalog graph"),
        ] {
            assert_same_request(&actual, &expected);
            for (index, step) in actual.steps().iter().enumerate() {
                assert_eq!(step.organization_id(), Some(inputs[index].organization));
                assert!(step.allows_handler_continuation());
                assert_eq!(step.execution_resource_key(), Some("buyer:provider"));
            }
            assert!(actual.steps()[0].dependencies().is_empty());
            if inputs.len() > 1 {
                assert_eq!(
                    actual.steps()[1].dependencies()[0].release_mode,
                    Some(WorkflowDependencyReleaseMode::OnTerminal)
                );
            }
        }
    }
}

#[test]
fn receiving_catalog_rejects_unknown_and_disabled_configured_jobs() {
    let payload = json!({});
    let source = catalog();
    let step = source
        .workflow_step("work", ENRICH, &payload)
        .expect("source catalog step")
        .try_build()
        .expect("valid source step");
    let unknown = JobCatalog::new();
    let disabled = catalog().defaults(JobCatalogDefaults::new().enabled(false));
    assert!(matches!(
        unknown
            .workflow_dag("workflow", &payload)
            .step(step.clone()),
        Err(CatalogError::UnknownJobType { .. })
    ));
    assert!(matches!(
        disabled
            .workflow_dag("workflow", &payload)
            .step(step.clone()),
        Err(CatalogError::DisabledJobType { .. })
    ));
    assert!(
        source
            .workflow_dag("workflow", &payload)
            .step(step)
            .expect("own catalog accepts step")
            .build()
            .is_ok()
    );
    assert!(matches!(
        disabled
            .workflow_dag("workflow", &payload)
            .job("work", ENRICH, &payload),
        Err(CatalogError::DisabledJobType { .. })
    ));
}

#[test]
fn catalog_preserves_queue_settings_and_mixed_external_graph() {
    let payload = json!({"ticket": 4});
    let catalog = catalog();
    let configured = catalog
        .workflow_step("work", ENRICH, &payload)
        .expect("catalog step")
        .priority(9)
        .max_attempts(4)
        .timeout_seconds(120)
        .stage(JobStage::Scheduled)
        .depends_on_success(&[StepKey::new("approval")])
        .try_build()
        .expect("configured step");
    let external = WorkflowStepEnqueueBuilder::try_new_external("approval", &payload)
        .expect("external step")
        .organization_id(Uuid::from_u128(9))
        .try_build()
        .expect("scoped external");
    let run = catalog
        .workflow_dag("workflow", &payload)
        .step(configured)
        .expect("job")
        .step(external)
        .expect("external")
        .external("done", &payload)
        .expect("external shorthand")
        .after_terminal("done", ["work"])
        .expect("terminal edge")
        .result_step("done")
        .expect("result")
        .build()
        .expect("mixed graph");
    assert_eq!(run.steps()[0].priority(), Some(9));
    assert_eq!(run.steps()[0].max_attempts(), Some(4));
    assert_eq!(run.steps()[0].timeout_seconds(), Some(120));
    assert_eq!(run.steps()[0].stage(), Some(JobStage::Scheduled));
    assert_eq!(
        run.steps()[0].dependencies()[0].release_mode,
        Some(WorkflowDependencyReleaseMode::OnSuccess)
    );
    assert_eq!(run.steps()[1].execution(), WorkflowStepExecution::External);
    assert_eq!(run.steps()[1].organization_id(), Some(Uuid::from_u128(9)));
    assert_eq!(
        run.steps()[2].dependencies()[0].release_mode,
        Some(WorkflowDependencyReleaseMode::OnTerminal)
    );
    assert_eq!(run.result_step_key(), Some(StepKey::new("done")));
    let empty_catalog = JobCatalog::new();
    assert!(
        empty_catalog
            .workflow_dag("external-only", &payload)
            .external("approval", &payload)
            .expect("no handler required")
            .build()
            .is_ok()
    );
}

#[test]
fn catalog_forwards_active_key_validation_and_clear() {
    let payload = json!({});
    let catalog = catalog();
    let base = catalog
        .workflow_dag("workflow", &payload)
        .idempotency_key("request")
        .external("approval", &payload)
        .expect("external");
    assert!(matches!(
        base.clone().active_key(" ").build(),
        Err(CatalogError::WorkflowBuild(
            WorkflowBuildError::BlankActiveKey
        ))
    ));
    assert!(matches!(
        base.clone().active_key(&"x".repeat(513)).build(),
        Err(CatalogError::WorkflowBuild(
            WorkflowBuildError::ActiveKeyTooLong
        ))
    ));
    let run = base
        .active_key("invalid")
        .clear_active_key()
        .build()
        .expect("cleared key");
    assert_eq!(run.active_key(), None);
    assert_eq!(run.idempotency_key(), Some("request"));
}

#[test]
fn catalog_configured_external_steps_keep_core_graph_validation() {
    let payload = json!({});
    let catalog = JobCatalog::new();
    let step = WorkflowStepEnqueueBuilder::try_new_external("approval", &payload)
        .expect("external step")
        .depends_on_success(&[StepKey::new("start")])
        .try_build()
        .expect("external shape");
    let base = catalog
        .workflow_dag("workflow", &payload)
        .step(step.clone())
        .expect("configured external requires no handler");
    assert!(matches!(
        base.clone().build(),
        Err(CatalogError::WorkflowBuild(
            WorkflowBuildError::MissingDependency { .. }
        ))
    ));
    assert!(matches!(
        base.clone().step(step),
        Err(CatalogError::WorkflowBuild(
            WorkflowBuildError::DuplicateStepKey { .. }
        ))
    ));
    assert!(matches!(
        base.clone().external("approval", &payload),
        Err(CatalogError::WorkflowBuild(
            WorkflowBuildError::DuplicateStepKey { .. }
        ))
    ));
    assert!(matches!(
        base.clone().external(" ", &payload),
        Err(CatalogError::WorkflowBuild(
            WorkflowBuildError::BlankStepKey { .. }
        ))
    ));
    let base = base.external("start", &payload).expect("prerequisite");
    assert!(base.clone().build().is_ok());
    assert!(matches!(
        base.clone()
            .after_terminal("approval", ["start"])
            .expect("deferred duplicate")
            .build(),
        Err(CatalogError::WorkflowBuild(
            WorkflowBuildError::DuplicateDependency { .. }
        ))
    ));
    assert!(matches!(
        base.clone()
            .after_success("start", ["start"])
            .expect("deferred self edge")
            .build(),
        Err(CatalogError::WorkflowBuild(
            WorkflowBuildError::SelfDependency { .. }
        ))
    ));
    assert!(matches!(
        base.after_success("start", ["approval"])
            .expect("deferred cycle")
            .build(),
        Err(CatalogError::WorkflowBuild(
            WorkflowBuildError::CycleDetected
        ))
    ));
}
