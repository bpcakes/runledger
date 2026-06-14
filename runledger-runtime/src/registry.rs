use std::collections::HashMap;
use std::sync::Arc;

pub use runledger_core::jobs::JobHandler;
use runledger_core::jobs::{JobHandlerRegistry, JobType};

#[derive(Clone, Default)]
pub struct JobRegistry {
    handlers: HashMap<JobType<'static>, Arc<dyn JobHandler>>,
    retry_delay_overrides: HashMap<JobType<'static>, HashMap<&'static str, i32>>,
}

impl JobRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<H>(&mut self, handler: H)
    where
        H: JobHandler + 'static,
    {
        self.register_boxed(Arc::new(handler));
    }

    pub fn register_retry_delay_override(
        &mut self,
        job_type: JobType<'static>,
        failure_code: &'static str,
        retry_delay_ms: i32,
    ) {
        assert!(retry_delay_ms > 0, "retry delay override must be positive");

        self.retry_delay_overrides
            .entry(job_type)
            .or_default()
            .insert(failure_code, retry_delay_ms);
    }

    #[must_use]
    pub fn get(&self, job_type: JobType<'_>) -> Option<Arc<dyn JobHandler>> {
        self.handlers.get(job_type.as_str()).cloned()
    }

    #[must_use]
    pub fn retry_delay_override(&self, job_type: JobType<'_>, failure_code: &str) -> Option<i32> {
        self.retry_delay_overrides
            .get(job_type.as_str())
            .and_then(|overrides| overrides.get(failure_code).copied())
    }

    #[must_use]
    pub fn registered_types(&self) -> Vec<JobType<'_>> {
        let mut keys: Vec<JobType<'_>> = self.handlers.keys().copied().collect();
        keys.sort_unstable();
        keys
    }
}

impl JobHandlerRegistry for JobRegistry {
    fn register_boxed(&mut self, handler: Arc<dyn JobHandler>) {
        self.handlers.insert(handler.job_type(), handler);
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use runledger_core::jobs::{JobCompletion, JobContext, JobFailure, JobType};
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::{JobHandler, JobRegistry};

    struct ExampleHandler;

    #[async_trait]
    impl JobHandler for ExampleHandler {
        fn job_type(&self) -> JobType<'static> {
            JobType::new("jobs.example")
        }

        async fn execute(
            &self,
            _context: JobContext,
            _payload: Value,
        ) -> Result<JobCompletion, JobFailure> {
            Ok(JobCompletion::success())
        }
    }

    fn test_context() -> JobContext {
        JobContext {
            job_id: Uuid::now_v7(),
            run_number: 1,
            attempt: 1,
            organization_id: None,
            worker_id: "registry-test-worker".to_string(),
        }
    }

    #[tokio::test]
    async fn registered_handler_executes_successfully_via_trait_object() {
        let mut registry = JobRegistry::new();
        registry.register(ExampleHandler);
        let handler = registry
            .get(JobType::new("jobs.example"))
            .expect("handler exists");

        handler
            .execute(test_context(), json!({}))
            .await
            .expect("registered handler should execute");
    }

    #[test]
    fn registered_types_returns_sorted_job_types() {
        let mut registry = JobRegistry::new();
        registry.register(ExampleHandler);

        assert_eq!(
            registry.registered_types(),
            vec![JobType::new("jobs.example")]
        );
    }

    #[test]
    fn retry_delay_override_matches_job_type_and_failure_code() {
        let mut registry = JobRegistry::new();
        registry.register_retry_delay_override(
            JobType::new("jobs.example"),
            "job.example.wait",
            42,
        );

        assert_eq!(
            registry.retry_delay_override(JobType::new("jobs.example"), "job.example.wait"),
            Some(42)
        );
        assert_eq!(
            registry.retry_delay_override(JobType::new("jobs.other"), "job.example.wait"),
            None
        );
        assert_eq!(
            registry.retry_delay_override(JobType::new("jobs.example"), "job.example.other"),
            None
        );
    }

    #[test]
    #[should_panic(expected = "retry delay override must be positive")]
    fn retry_delay_override_rejects_zero_delay() {
        let mut registry = JobRegistry::new();
        registry.register_retry_delay_override(JobType::new("jobs.example"), "job.example.wait", 0);
    }
}
