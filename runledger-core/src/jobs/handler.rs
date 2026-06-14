use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use super::identifiers::JobType;
use super::{JobCompletion, JobContext, JobDeadLetterInfo, JobFailure};

#[async_trait]
pub trait JobHandler: Send + Sync {
    fn job_type(&self) -> JobType<'static>;
    async fn execute(
        &self,
        context: JobContext,
        payload: Value,
    ) -> Result<JobCompletion, JobFailure>;

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        _payload: Value,
        _dead_letter: JobDeadLetterInfo,
    ) {
    }
}

pub trait JobHandlerRegistry {
    fn register_boxed(&mut self, handler: Arc<dyn JobHandler>);

    fn register<H>(&mut self, handler: H)
    where
        Self: Sized,
        H: JobHandler + 'static,
    {
        self.register_boxed(Arc::new(handler));
    }
}
