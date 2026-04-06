use std::collections::HashMap;
use std::sync::Arc;

pub use runledger_core::jobs::JobHandler;
use runledger_core::jobs::{JobHandlerRegistry, JobType};

#[derive(Default, Clone)]
pub struct JobRegistry {
    handlers: HashMap<JobType<'static>, Arc<dyn JobHandler>>,
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

    #[must_use]
    pub fn get(&self, job_type: JobType<'_>) -> Option<Arc<dyn JobHandler>> {
        self.handlers.get(job_type.as_str()).cloned()
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
    use super::*;
    use async_trait::async_trait;
    use runledger_core::jobs::{JobContext, JobFailure, JobType};
    use serde_json::Value;
    use serde_json::json;
    use uuid::Uuid;

    struct ExampleHandler;

    #[async_trait]
    impl JobHandler for ExampleHandler {
        fn job_type(&self) -> JobType<'static> {
            JobType::new("jobs.example")
        }

        async fn execute(&self, _context: JobContext, _payload: Value) -> Result<(), JobFailure> {
            Ok(())
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
}
