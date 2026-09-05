use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use runledger_core::jobs::{
    JobCompletion, JobContext, JobContract, JobFailure, JobHandler, JobSpec, JobSpecs, JobType,
    TypedJobHandler,
};
use runledger_runtime::catalog::{CatalogError, JobBindingError, JobCatalog};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
struct Payload {
    request_id: Uuid,
    #[serde(default)]
    revision: u32,
}
struct Delivery;
impl JobContract for Delivery {
    type Payload = Payload;
    fn spec() -> JobSpec {
        JobSpec::new(JobType::new("delivery.send")).expect("static spec")
    }
}
struct Handler {
    calls: Arc<AtomicUsize>,
}
#[async_trait]
impl TypedJobHandler for Handler {
    type Contract = Delivery;
    async fn execute(
        &self,
        context: JobContext,
        payload: Payload,
    ) -> Result<JobCompletion, JobFailure> {
        assert_eq!(context.organization_id, Some(payload.request_id));
        assert_eq!(context.checkpoint, Some(json!({"resume": 2})));
        assert_eq!(payload.revision, 0);
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(JobCompletion::success())
    }
}
fn context(id: Uuid) -> JobContext {
    JobContext {
        job_id: Uuid::nil(),
        run_number: 1,
        attempt: 1,
        organization_id: Some(id),
        worker_id: "test".into(),
        checkpoint: Some(json!({"resume":2})),
    }
}
fn handler() -> Arc<dyn JobHandler> {
    Arc::new(
        Handler {
            calls: Arc::new(AtomicUsize::new(0)),
        }
        .into_job_handler(),
    )
}

#[test]
fn complete_bindings_are_required_and_metadata_matches_producer() {
    let specs = JobSpecs::new([Delivery::spec()]).expect("specs");
    assert!(matches!(
        JobCatalog::from_specs(&specs, []),
        Err(JobBindingError::MissingHandler(_))
    ));
    assert!(matches!(
        JobCatalog::from_specs(&specs, [handler(), handler()]),
        Err(JobBindingError::Catalog(
            CatalogError::DuplicateJobType { .. }
        ))
    ));
    assert!(matches!(
        JobCatalog::from_specs(&JobSpecs::default(), [handler()]),
        Err(JobBindingError::UnknownHandler(_))
    ));
    let catalog = JobCatalog::from_specs(&specs, [handler()]).expect("bound catalog");
    assert!(
        catalog
            .to_registry()
            .get(Delivery::spec().job_type())
            .is_some()
    );
}

#[tokio::test]
async fn decodes_old_json_rows_without_changing_context_or_running_invalid_payloads() {
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = Handler {
        calls: calls.clone(),
    }
    .into_job_handler();
    let id = Uuid::now_v7();
    // Unknown legacy fields are accepted unless the application opts out via serde.
    handler
        .execute(context(id), json!({"request_id":id, "legacy":true}))
        .await
        .expect("old row");
    let failure = handler
        .execute(context(id), json!({"request_id":"private-attacker-value"}))
        .await
        .expect_err("bad UUID");
    assert_eq!(failure.code, "job.invalid_payload");
    assert_eq!(failure.message, "Job payload has an invalid shape.");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

struct ApplicationFailure;
#[async_trait]
impl TypedJobHandler for ApplicationFailure {
    type Contract = Delivery;
    async fn execute(&self, _: JobContext, _: Payload) -> Result<JobCompletion, JobFailure> {
        panic!("invalid payload must not reach business logic");
    }
    fn malformed_payload(&self, source: &serde_json::Error) -> JobFailure {
        assert!(source.is_data());
        JobFailure::terminal("app.invalid_delivery", "Invalid delivery payload.")
    }
}

#[tokio::test]
async fn application_can_customize_static_failure_and_legacy_json_handlers_still_bind() {
    let error = ApplicationFailure
        .into_job_handler()
        .execute(context(Uuid::nil()), json!({}))
        .await
        .expect_err("missing field");
    assert_eq!(error.code, "app.invalid_delivery");
    assert_eq!(error.message, "Invalid delivery payload.");
    struct Legacy;
    #[async_trait]
    impl JobHandler for Legacy {
        fn job_type(&self) -> JobType<'static> {
            Delivery::spec().job_type()
        }
        async fn execute(
            &self,
            _: JobContext,
            payload: Value,
        ) -> Result<JobCompletion, JobFailure> {
            assert_eq!(payload, json!({"legacy":true}));
            Ok(JobCompletion::success())
        }
    }
    let specs = JobSpecs::new([Delivery::spec()]).expect("specs");
    let catalog = JobCatalog::from_specs(&specs, [Arc::new(Legacy) as Arc<dyn JobHandler>])
        .expect("legacy binding");
    catalog
        .to_registry()
        .get(Delivery::spec().job_type())
        .expect("legacy handler")
        .execute(context(Uuid::nil()), json!({"legacy":true}))
        .await
        .expect("legacy execute");
}

#[tokio::test]
async fn shared_settings_survive_catalog_defaults_and_disabled_specs_keep_handlers() {
    use runledger_core::jobs::JobDefinitionSettings;
    use runledger_postgres::jobs::get_job_definition_by_type;
    use runledger_runtime::catalog::JobCatalogDefaults;
    use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};

    let spec = Delivery::spec()
        .with_settings(
            JobDefinitionSettings::new()
                .version(8)
                .max_attempts(7)
                .timeout_seconds(91)
                .priority(-2)
                .enabled(false),
        )
        .expect("settings");
    let specs = JobSpecs::new([spec]).expect("specs");
    assert!(matches!(
        JobCatalog::from_specs(&specs, []),
        Err(JobBindingError::MissingHandler(_))
    ));
    let catalog = JobCatalog::from_specs(&specs, [handler()])
        .expect("disabled binding")
        .defaults(JobCatalogDefaults::new().max_attempts(20).enabled(true));
    assert!(catalog.to_registry().get(spec.job_type()).is_some());
    assert!(matches!(
        catalog.require_catalog_enabled_job_type(spec.job_type().as_str()),
        Err(CatalogError::DisabledJobType { .. })
    ));
    let (pool, database) = setup_ephemeral_pool("shared_worker_specs", 2).await;
    catalog.sync_definitions(&pool).await.expect("worker sync");
    let row = get_job_definition_by_type(&pool, spec.job_type())
        .await
        .expect("read")
        .expect("definition");
    assert_eq!(row.version, 8);
    assert_eq!(row.max_attempts, 7);
    assert_eq!(row.default_timeout_seconds, 91);
    assert_eq!(row.default_priority, -2);
    assert!(!row.is_enabled);
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn typed_dispatch_retains_execution_services_and_raw_terminal_cleanup() {
    use runledger_core::jobs::{
        JobDeadLetterInfo, JobDeadLetterReason, JobExecution, JobExecutionError,
        JobExecutionServices, JobExecutionUpdate,
    };
    use std::time::{Duration, Instant};
    struct Services(Instant);
    #[async_trait]
    impl JobExecutionServices for Services {
        fn deadline(&self) -> Instant {
            self.0
        }
        fn remaining_budget(&self) -> Duration {
            Duration::from_secs(10)
        }
        async fn persist_progress(
            &self,
            _: JobExecutionUpdate<'_>,
        ) -> Result<(), JobExecutionError> {
            Ok(())
        }
    }
    struct WithServices(Arc<AtomicUsize>);
    #[async_trait]
    impl TypedJobHandler for WithServices {
        type Contract = Delivery;
        async fn execute(&self, _: JobContext, _: Payload) -> Result<JobCompletion, JobFailure> {
            panic!("runtime dispatch must retain services");
        }
        async fn execute_with_services(
            &self,
            execution: JobExecution<'_>,
            payload: Payload,
        ) -> Result<JobCompletion, JobFailure> {
            assert_eq!(execution.remaining_budget(), Duration::from_secs(10));
            assert_eq!(
                execution.context().organization_id,
                Some(payload.request_id)
            );
            assert_eq!(
                execution.checkpoint::<Value>().expect("typed checkpoint"),
                Some(json!({"resume":2}))
            );
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(JobCompletion::success())
        }
        async fn on_dead_letter(&self, _: JobContext, payload: Value, _: JobDeadLetterInfo) {
            assert_eq!(payload, json!({"invalid":"raw"}));
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let handler = WithServices(calls.clone()).into_job_handler();
    let id = Uuid::now_v7();
    let context = context(id);
    let services = Services(Instant::now());
    handler
        .execute_with_services(
            JobExecution::new(&context, &services),
            json!({"request_id":id}),
        )
        .await
        .expect("typed service dispatch");
    let error = handler
        .execute_with_services(
            JobExecution::new(&context, &services),
            json!({"invalid":"raw"}),
        )
        .await
        .expect_err("malformed service payload");
    handler
        .on_dead_letter(
            context,
            json!({"invalid":"raw"}),
            JobDeadLetterInfo::new(error, JobDeadLetterReason::FailureKindNonRetryable, None),
        )
        .await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}
