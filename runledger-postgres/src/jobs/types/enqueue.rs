use std::fmt;

use chrono::{DateTime, Utc};
use runledger_core::jobs::{JobStage, JobStatus, JobType, JobTypeName};
use serde_json::Value;
use sqlx::types::Uuid;

#[derive(Clone, Debug)]
pub struct JobEnqueue<'a> {
    pub job_type: JobType<'a>,
    pub organization_id: Option<Uuid>,
    pub payload: &'a Value,
    pub priority: Option<i32>,
    pub max_attempts: Option<i32>,
    pub timeout_seconds: Option<i32>,
    /// For keyed enqueues, this value is part of the stored idempotency request
    /// snapshot. Retries must pass the same scheduled time as the original
    /// enqueue instead of recomputing a fresh timestamp.
    pub next_run_at: Option<DateTime<Utc>>,
    pub idempotency_key: Option<&'a str>,
    pub stage: Option<JobStage>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum JobEnqueueDisposition {
    Inserted,
    Existing,
}

/// Stable job state returned from a transactional enqueue.
///
/// Keyed existing rows are held under a mutation-ready row lock until the
/// caller's transaction ends, so `status` and `run_number` describe the row
/// protected by that transaction rather than a later unlocked lookup. This
/// lock composes with a later mutation of the same row in the transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobEnqueueOutcome {
    pub job_id: Uuid,
    pub status: JobStatus,
    pub run_number: i32,
    pub disposition: JobEnqueueDisposition,
}

/// Exact tenant scope for a job mutation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobScope {
    /// Match only a job whose `organization_id` is `NULL`.
    Global,
    /// Match only a job owned by this exact organization.
    Organization(Uuid),
}

impl JobScope {
    #[must_use]
    pub const fn organization_id(self) -> Option<Uuid> {
        match self {
            Self::Global => None,
            Self::Organization(organization_id) => Some(organization_id),
        }
    }
}

/// Terminal job statuses that may be recovered through compare-and-requeue.
///
/// `SUCCEEDED` is deliberately absent: replaying successful work requires a
/// separate policy decision and cannot be requested through this type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequeueableJobStatus {
    DeadLettered,
    Canceled,
}

impl RequeueableJobStatus {
    #[must_use]
    pub const fn as_job_status(self) -> JobStatus {
        match self {
            Self::DeadLettered => JobStatus::DeadLettered,
            Self::Canceled => JobStatus::Canceled,
        }
    }

    #[must_use]
    pub const fn as_db_value(self) -> &'static str {
        self.as_job_status().as_db_value()
    }
}

/// Error returned when a job observation cannot seed compare-and-requeue.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NonRequeueableJobStatusError {
    status: JobStatus,
}

impl NonRequeueableJobStatusError {
    /// The observed status that compare-and-requeue does not accept.
    #[must_use]
    pub const fn status(&self) -> JobStatus {
        self.status
    }
}

impl fmt::Display for NonRequeueableJobStatusError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "job status {} cannot be compare-and-requeued; expected CANCELED or DEAD_LETTERED",
            self.status.as_db_value()
        )
    }
}

impl std::error::Error for NonRequeueableJobStatusError {}

impl TryFrom<JobStatus> for RequeueableJobStatus {
    type Error = NonRequeueableJobStatusError;

    fn try_from(status: JobStatus) -> Result<Self, Self::Error> {
        match status {
            JobStatus::DeadLettered => Ok(Self::DeadLettered),
            JobStatus::Canceled => Ok(Self::Canceled),
            status => Err(NonRequeueableJobStatusError { status }),
        }
    }
}

/// Whether compare-and-requeue carries durable resume state into the new run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobRequeueStatePolicy {
    /// Keep `progress_done`, `progress_total`, and `checkpoint` so recovery can
    /// resume from the last committed position.
    PreserveProgressAndCheckpoint,
    /// Clear progress and checkpoint state so the new run starts from scratch.
    ResetProgressAndCheckpoint,
}

impl JobRequeueStatePolicy {
    #[must_use]
    pub const fn preserves_progress_and_checkpoint(self) -> bool {
        matches!(self, Self::PreserveProgressAndCheckpoint)
    }

    #[must_use]
    pub const fn as_event_value(self) -> &'static str {
        match self {
            Self::PreserveProgressAndCheckpoint => "preserve_progress_and_checkpoint",
            Self::ResetProgressAndCheckpoint => "reset_progress_and_checkpoint",
        }
    }

    pub(crate) fn from_event_value(value: &str) -> Option<Self> {
        match value {
            "preserve_progress_and_checkpoint" => Some(Self::PreserveProgressAndCheckpoint),
            "reset_progress_and_checkpoint" => Some(Self::ResetProgressAndCheckpoint),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct JobQueueRecord {
    pub id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub payload: Value,
    pub status: JobStatus,
    pub priority: i32,
    pub run_number: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    pub timeout_seconds: i32,
    pub next_run_at: DateTime<Utc>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub worker_id: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub stage: JobStage,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub progress_pct: Option<f64>,
    pub checkpoint: Option<Value>,
    pub output: Option<Value>,
    pub idempotency_key: Option<String>,
    pub status_reason: Option<String>,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct CompareAndRequeueJob<'a> {
    pub scope: JobScope,
    pub job_id: Uuid,
    pub expected_status: RequeueableJobStatus,
    pub expected_run_number: i32,
    pub state_policy: JobRequeueStatePolicy,
    pub reason: &'a str,
}

impl<'a> CompareAndRequeueJob<'a> {
    /// Builds a compare-and-requeue request from an observed terminal job.
    ///
    /// The job's exact tenant scope, identifier, status, and run number are
    /// copied into the request so callers cannot accidentally turn a scoped
    /// observation into a wildcard or lose the optimistic-concurrency fence.
    ///
    /// # Errors
    /// Returns [`NonRequeueableJobStatusError`] unless the observation is
    /// canceled or dead-lettered. Successful jobs require a separate replay
    /// policy and pending or leased jobs are not recovery candidates.
    pub fn from_observed_job(
        observed: &JobQueueRecord,
        state_policy: JobRequeueStatePolicy,
        reason: &'a str,
    ) -> Result<Self, NonRequeueableJobStatusError> {
        let expected_status = RequeueableJobStatus::try_from(observed.status)?;
        let scope = observed
            .organization_id
            .map_or(JobScope::Global, JobScope::Organization);

        Ok(Self {
            scope,
            job_id: observed.id,
            expected_status,
            expected_run_number: observed.run_number,
            state_policy,
            reason,
        })
    }
}

#[derive(Clone, Debug)]
#[must_use = "callers must inspect whether the expected job was requeued"]
#[non_exhaustive]
pub enum CompareAndRequeueJobOutcome {
    Requeued {
        before: Box<JobQueueRecord>,
        after: Box<JobQueueRecord>,
        event_id: i64,
    },
    ExpectationMismatch {
        actual: Box<JobQueueRecord>,
    },
    /// Cancellation fenced a live handler, but its original lease window has
    /// not passed yet. Retrying before `retry_after` could overlap the new run
    /// with the canceled handler's external side effects.
    CancellationNotQuiesced {
        actual: Box<JobQueueRecord>,
        retry_after: DateTime<Utc>,
    },
    NotFound,
}
