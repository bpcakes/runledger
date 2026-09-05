//! Optional live execution services, separate from the serializable job context.

use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;

use super::{JobContext, JobFailure, JobProgressValidationError, validate_job_progress};

/// An atomic ordinary-progress/checkpoint update. Omitted fields retain their
/// durable values. This cannot change job stage, lease identity, or final output.
#[derive(Debug, Default, Clone, Copy)]
pub struct JobExecutionUpdate<'a> {
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub checkpoint: Option<&'a Value>,
}

/// A live execution operation failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum JobExecutionError {
    /// The exact run/attempt/worker no longer holds a live lease.
    LeaseLost,
    /// The runtime's handler deadline has elapsed.
    DeadlineElapsed,
    /// Persistence did not acknowledge a successful commit.
    PersistenceFailed,
    InvalidProgress(JobProgressValidationError),
    InvalidCheckpoint(serde_json::Error),
}

impl std::fmt::Display for JobExecutionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LeaseLost => f.write_str("Job lease ownership was lost."),
            Self::DeadlineElapsed => f.write_str("Job execution deadline elapsed."),
            Self::PersistenceFailed => f.write_str("Job progress commit was not acknowledged."),
            Self::InvalidProgress(error) => error.fmt(f),
            Self::InvalidCheckpoint(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for JobExecutionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidProgress(error) => Some(error),
            Self::InvalidCheckpoint(error) => Some(error),
            _ => None,
        }
    }
}

impl From<JobExecutionError> for JobFailure {
    fn from(error: JobExecutionError) -> Self {
        match error {
            JobExecutionError::LeaseLost => {
                Self::lease_expired("job.lease_owner_mismatch", error.to_string())
            }
            JobExecutionError::DeadlineElapsed => {
                Self::timeout("job.timeout_exceeded", error.to_string())
            }
            JobExecutionError::PersistenceFailed => {
                Self::retryable("job.progress_persist_failed", error.to_string())
            }
            JobExecutionError::InvalidProgress(_) => {
                Self::terminal("job.invalid_progress", error.to_string())
            }
            JobExecutionError::InvalidCheckpoint(_) => {
                Self::terminal("job.invalid_checkpoint", error.to_string())
            }
        }
    }
}

/// Runtime implementation of services for one exact execution.
///
/// Custom runtimes must bind writes to the context's live lease, return success
/// only after commit, and use the same deadline and clock as handler timeout
/// enforcement. Implementations must not extend that deadline on progress writes.
#[async_trait]
pub trait JobExecutionServices: Send + Sync {
    fn deadline(&self) -> Instant;
    fn remaining_budget(&self) -> Duration;
    async fn persist_progress(
        &self,
        update: JobExecutionUpdate<'_>,
    ) -> Result<(), JobExecutionError>;
}

/// Borrowed services for the current handler invocation.
///
/// The checkpoint is the resume snapshot captured before handler execution.
/// Successful writes become visible to subsequent runs and dead-letter hooks;
/// they do not mutate this snapshot. The borrow prevents carrying this handle
/// into a detached task beyond the runtime-owned execution.
///
/// The runtime still enforces timeout and lease loss. These services cannot
/// cancel external effects already issued by a handler.
#[derive(Clone, Copy)]
pub struct JobExecution<'a> {
    context: &'a JobContext,
    services: &'a dyn JobExecutionServices,
}

impl<'a> JobExecution<'a> {
    /// Binds an execution snapshot to services supplied by a custom runtime.
    #[must_use]
    pub fn new(context: &'a JobContext, services: &'a dyn JobExecutionServices) -> Self {
        Self { context, services }
    }

    #[must_use]
    pub fn context(&self) -> &JobContext {
        self.context
    }

    /// The authoritative monotonic handler deadline, including time spent
    /// awaiting progress writes. Completion persistence occurs after execution.
    /// The Runledger worker accepts a handler result only when observed strictly
    /// before this instant. At or after it, timeout takes precedence over success
    /// and continuation, even when the handler and timer become ready together.
    /// Committed checkpoints and external effects are not undone by timeout.
    #[must_use]
    pub fn deadline(&self) -> Instant {
        self.services.deadline()
    }

    #[must_use]
    pub fn remaining_budget(&self) -> Duration {
        self.services.remaining_budget()
    }

    /// Time available for application work after reserving time inside the
    /// handler for its final checkpoint or cleanup. Saturates at zero.
    #[must_use]
    pub fn remaining_work_budget(&self, reserve: Duration) -> Duration {
        self.remaining_budget().saturating_sub(reserve)
    }

    #[must_use]
    pub fn checkpoint_value(&self) -> Option<&Value> {
        self.context.checkpoint.as_ref()
    }

    /// Decodes the resume snapshot. Applications still own checkpoint versions
    /// and domain validation; an absent checkpoint is distinct from malformed JSON.
    pub fn checkpoint<T: DeserializeOwned>(&self) -> Result<Option<T>, serde_json::Error> {
        self.checkpoint_value()
            .cloned()
            .map(serde_json::from_value)
            .transpose()
    }

    /// Awaits an atomic, lease-fenced commit of ordinary progress and checkpoint.
    ///
    /// Ok means the transaction committed. An error or cancellation does not
    /// prove a write was absent: the connection can fail after the server commits.
    /// Omitted fields retain existing values; a JSON null checkpoint is a value.
    /// The application must make replay of external effects safe independently.
    pub async fn persist_progress(
        &self,
        update: JobExecutionUpdate<'_>,
    ) -> Result<(), JobExecutionError> {
        validate_job_progress(update.progress_done, update.progress_total)
            .map_err(JobExecutionError::InvalidProgress)?;
        self.services.persist_progress(update).await
    }

    /// Serializes and durably commits a checkpoint without changing progress.
    pub async fn save_checkpoint<T: Serialize + Sync + ?Sized>(
        &self,
        checkpoint: &T,
    ) -> Result<(), JobExecutionError> {
        let checkpoint =
            serde_json::to_value(checkpoint).map_err(JobExecutionError::InvalidCheckpoint)?;
        self.persist_progress(JobExecutionUpdate {
            checkpoint: Some(&checkpoint),
            ..Default::default()
        })
        .await
    }
}
