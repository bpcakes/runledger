use std::collections::HashMap;
use std::sync::Arc;

pub use runledger_core::jobs::JobHandler;
use runledger_core::jobs::{JobHandlerRegistry, JobType};
use thiserror::Error;

#[derive(Debug, Clone, Error, Eq, PartialEq)]
#[non_exhaustive]
pub enum JobRegistryError {
    #[error("job handler already registered for {job_type}")]
    DuplicateJobType { job_type: JobType<'static> },
}

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
        let handler: Arc<dyn JobHandler> = Arc::new(handler);
        self.handlers.insert(handler.job_type(), handler);
    }

    pub fn try_register<H>(&mut self, handler: H) -> Result<(), JobRegistryError>
    where
        H: JobHandler + 'static,
    {
        self.try_register_boxed(Arc::new(handler))
    }

    pub fn try_register_boxed(
        &mut self,
        handler: Arc<dyn JobHandler>,
    ) -> Result<(), JobRegistryError> {
        let job_type = handler.job_type();
        if self.handlers.contains_key(job_type.as_str()) {
            return Err(JobRegistryError::DuplicateJobType { job_type });
        }

        self.handlers.insert(job_type, handler);
        Ok(())
    }

    /// Registers the policy retry delay for one job type and failure code.
    ///
    /// A lower bound attached directly to [`runledger_core::jobs::JobFailure`]
    /// may extend this delay but can never shorten it. The worker uses its
    /// exponential backoff when no matching override exists.
    ///
    /// # Panics
    ///
    /// Panics when `retry_delay_ms` is not positive.
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

    /// Returns the configured policy retry delay for an exact job type and
    /// failure code.
    ///
    /// Handler retry timing is a lower bound that may extend this policy delay
    /// but cannot shorten it.
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
    use std::sync::Arc;

    use async_trait::async_trait;
    use runledger_core::jobs::{
        JobCompletion, JobContext, JobFailure, JobHandlerRegistry, JobType,
    };
    use serde_json::{Value, json};
    use uuid::Uuid;

    use super::{JobHandler, JobRegistry, JobRegistryError};

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

    struct OutputHandler(&'static str);

    #[async_trait]
    impl JobHandler for OutputHandler {
        fn job_type(&self) -> JobType<'static> {
            JobType::new("jobs.example")
        }

        async fn execute(
            &self,
            _context: JobContext,
            _payload: Value,
        ) -> Result<JobCompletion, JobFailure> {
            Ok(JobCompletion::with_output(json!(self.0)))
        }
    }

    fn test_context() -> JobContext {
        JobContext {
            job_id: Uuid::now_v7(),
            run_number: 1,
            attempt: 1,
            organization_id: None,
            worker_id: "registry-test-worker".to_string(),
            checkpoint: None,
        }
    }

    async fn registered_output(registry: &JobRegistry) -> Value {
        registry
            .get(JobType::new("jobs.example"))
            .expect("handler exists")
            .execute(test_context(), json!({}))
            .await
            .expect("registered handler should execute")
            .output()
            .cloned()
            .expect("handler should return output")
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

    #[tokio::test]
    async fn try_register_rejects_duplicate_job_type_and_keeps_first_handler() {
        let mut registry = JobRegistry::new();
        registry
            .try_register(OutputHandler("first"))
            .expect("first handler registration should succeed");

        assert_eq!(
            registry.try_register(OutputHandler("second")),
            Err(JobRegistryError::DuplicateJobType {
                job_type: JobType::new("jobs.example"),
            })
        );
        assert_eq!(registered_output(&registry).await, json!("first"));
    }

    #[tokio::test]
    async fn try_register_boxed_rejects_duplicate_job_type_and_keeps_first_handler() {
        let mut registry = JobRegistry::new();
        registry
            .try_register_boxed(Arc::new(OutputHandler("first")))
            .expect("first handler registration should succeed");

        assert_eq!(
            registry.try_register_boxed(Arc::new(OutputHandler("second"))),
            Err(JobRegistryError::DuplicateJobType {
                job_type: JobType::new("jobs.example"),
            })
        );
        assert_eq!(registered_output(&registry).await, json!("first"));
    }

    #[tokio::test]
    async fn register_overwrites_duplicate_job_type() {
        let mut registry = JobRegistry::new();
        registry.register(OutputHandler("first"));
        registry.register(OutputHandler("second"));

        assert_eq!(registered_output(&registry).await, json!("second"));
    }

    #[tokio::test]
    async fn trait_register_boxed_overwrites_duplicate_job_type() {
        let mut registry = JobRegistry::new();
        JobHandlerRegistry::register_boxed(&mut registry, Arc::new(OutputHandler("first")));
        JobHandlerRegistry::register_boxed(&mut registry, Arc::new(OutputHandler("second")));

        assert_eq!(registered_output(&registry).await, json!("second"));
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
