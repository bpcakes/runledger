use async_trait::async_trait;
use serde_json::Value;

use super::{
    JobCompletion, JobContext, JobContract, JobDeadLetterInfo, JobExecution, JobFailure,
    JobHandler, JobType,
};

/// Safe durable default: decoding details may contain input data and are never
/// included in the failure. Applications may use their own static code/message.
#[must_use]
pub fn malformed_job_payload() -> JobFailure {
    JobFailure::terminal("job.invalid_payload", "Job payload has an invalid shape.")
}

/// Opt-in typed execution over the existing JSON dispatch boundary.
#[async_trait]
pub trait TypedJobHandler: Send + Sync {
    type Contract: JobContract;

    async fn execute(
        &self,
        context: JobContext,
        payload: <Self::Contract as JobContract>::Payload,
    ) -> Result<JobCompletion, JobFailure>;

    async fn execute_with_services(
        &self,
        execution: JobExecution<'_>,
        payload: <Self::Contract as JobContract>::Payload,
    ) -> Result<JobCompletion, JobFailure> {
        self.execute(execution.context().clone(), payload).await
    }

    /// Override for application classification or safe diagnostics. The source
    /// can contain payload data; do not persist or log it without sanitizing.
    fn malformed_payload(&self, _source: &serde_json::Error) -> JobFailure {
        malformed_job_payload()
    }

    /// Retains raw JSON so cleanup also runs for undecodable durable rows.
    async fn on_dead_letter(
        &self,
        _context: JobContext,
        _payload: Value,
        _info: JobDeadLetterInfo,
    ) {
    }

    fn into_job_handler(self) -> TypedHandlerAdapter<Self>
    where
        Self: Sized,
    {
        TypedHandlerAdapter(self)
    }
}

pub struct TypedHandlerAdapter<H>(H);

#[async_trait]
impl<H: TypedJobHandler> JobHandler for TypedHandlerAdapter<H> {
    fn job_type(&self) -> JobType<'static> {
        H::Contract::spec().job_type()
    }

    async fn execute(
        &self,
        context: JobContext,
        payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        let payload =
            serde_json::from_value(payload).map_err(|error| self.0.malformed_payload(&error))?;
        self.0.execute(context, payload).await
    }

    async fn execute_with_services(
        &self,
        execution: JobExecution<'_>,
        payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        let payload =
            serde_json::from_value(payload).map_err(|error| self.0.malformed_payload(&error))?;
        self.0.execute_with_services(execution, payload).await
    }

    async fn on_dead_letter(&self, context: JobContext, payload: Value, info: JobDeadLetterInfo) {
        self.0.on_dead_letter(context, payload, info).await;
    }
}
