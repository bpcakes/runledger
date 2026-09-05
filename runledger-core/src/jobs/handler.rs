use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value;

use super::identifiers::JobType;
use super::{JobCompletion, JobContext, JobDeadLetterInfo, JobExecution, JobFailure};

#[async_trait]
pub trait JobHandler: Send + Sync {
    /// Returns the stable identity used to register and route this handler.
    ///
    /// Implementations must return the same value for the handler's lifetime.
    fn job_type(&self) -> JobType<'static>;
    async fn execute(
        &self,
        context: JobContext,
        payload: Value,
    ) -> Result<JobCompletion, JobFailure>;

    /// Runtime dispatch with live execution services. Legacy handlers retain
    /// their existing execute contract through this default implementation.
    async fn execute_with_services(
        &self,
        execution: JobExecution<'_>,
        payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.execute(execution.context().clone(), payload).await
    }

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

/// Opt-in handler contract with runtime-owned deadline and persistence services.
///
/// Register `handler.into_job_handler()` with an existing registry or catalog.
/// Legacy `JobHandler` implementations do not need to change.
#[async_trait]
pub trait JobExecutionHandler: Send + Sync {
    fn job_type(&self) -> JobType<'static>;

    async fn execute(
        &self,
        execution: JobExecution<'_>,
        payload: Value,
    ) -> Result<JobCompletion, JobFailure>;

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        _payload: Value,
        _dead_letter: JobDeadLetterInfo,
    ) {
    }

    fn into_job_handler(self) -> ExecutionHandlerAdapter<Self>
    where
        Self: Sized,
    {
        ExecutionHandlerAdapter(self)
    }
}

/// Binds an execution-services handler to existing registries and catalogs.
///
/// Custom runtimes must call `JobHandler::execute_with_services`. Calling the
/// legacy `execute` directly returns `job.execution_services_required`; it
/// cannot fabricate a deadline or a live lease for the adapted handler.
pub struct ExecutionHandlerAdapter<H>(H);

#[async_trait]
impl<H: JobExecutionHandler> JobHandler for ExecutionHandlerAdapter<H> {
    fn job_type(&self) -> JobType<'static> {
        self.0.job_type()
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        Err(JobFailure::terminal(
            "job.execution_services_required",
            "This handler requires runtime-owned execution services.",
        ))
    }

    async fn execute_with_services(
        &self,
        execution: JobExecution<'_>,
        payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.0.execute(execution, payload).await
    }

    async fn on_dead_letter(
        &self,
        context: JobContext,
        payload: Value,
        dead_letter: JobDeadLetterInfo,
    ) {
        self.0.on_dead_letter(context, payload, dead_letter).await;
    }
}
