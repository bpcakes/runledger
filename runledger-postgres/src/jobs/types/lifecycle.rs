use std::time::Duration;

use chrono::{DateTime, Utc};
use runledger_core::jobs::{JobFailureKind, JobRetryTiming, JobStage, JobTypeName};
use serde_json::Value;
use sqlx::types::Uuid;

/// Exact identity of a live job lease.
///
/// Lifecycle mutations use every field to fence an operation to one claimed
/// attempt. Reusing this value avoids accidentally pairing a job identifier
/// with the run, attempt, or worker from another lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobLeaseIdentity<'a> {
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: i32,
    pub worker_id: &'a str,
}

impl<'a> JobLeaseIdentity<'a> {
    /// Creates an identity for one exact live job lease.
    #[must_use]
    pub const fn new(job_id: Uuid, run_number: i32, attempt: i32, worker_id: &'a str) -> Self {
        Self {
            job_id,
            run_number,
            attempt,
            worker_id,
        }
    }
}

/// The progress, checkpoint, and audit data committed with a `RUNNING`
/// transition.
///
/// [`crate::jobs::mark_job_running`] persists this update atomically with the
/// stage transition and execution-start marker. Do not split that transition
/// into a stage-only call followed by an ordinary progress update: a crash
/// between separate writes would lose the durable resume state for the started
/// attempt.
#[derive(Clone, Debug)]
pub struct JobRunningUpdate<'a> {
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub checkpoint: Option<&'a Value>,
}

/// An ordinary in-flight progress and checkpoint update.
///
/// This input intentionally cannot change a job stage. Use
/// [`JobRunningUpdate`] with [`crate::jobs::mark_job_running`] when execution
/// starts so the `RUNNING` transition and durable resume state remain one
/// transaction.
#[derive(Clone, Debug)]
pub struct JobOrdinaryProgressUpdate<'a> {
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub checkpoint: Option<&'a Value>,
}

/// Legacy stage-bearing progress input.
///
/// New callers should use [`JobRunningUpdate`] with
/// [`crate::jobs::mark_job_running`] for a `RUNNING` transition, or
/// [`JobOrdinaryProgressUpdate`] with
/// [`crate::jobs::update_job_ordinary_progress`] for ordinary progress. This
/// compatibility input preserves arbitrary historical stage writes while
/// downstream callers migrate to the typed lifecycle APIs.
#[deprecated(
    since = "0.11.0",
    note = "use JobRunningUpdate with mark_job_running for RUNNING, or JobOrdinaryProgressUpdate with update_job_ordinary_progress for ordinary progress"
)]
#[derive(Clone, Debug)]
pub struct JobProgressUpdate<'a> {
    pub stage: Option<JobStage>,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub checkpoint: Option<&'a Value>,
}

#[derive(Clone, Debug)]
pub struct JobCompletionUpdate<'a> {
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub checkpoint: Option<&'a Value>,
    pub output: Option<&'a Value>,
}

/// Progress and scheduling data for a successful handler continuation.
#[derive(Clone, Debug)]
pub struct JobContinuationUpdate<'a> {
    /// How long to wait before the next run becomes claimable. Zero means the
    /// next run is immediately eligible. Delays whose resulting timestamp is
    /// outside the persistence driver's representable range are rejected with
    /// `job.invalid_continuation_delay`.
    pub delay: Duration,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub checkpoint: Option<&'a Value>,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct JobContinuationOutcome {
    pub job_id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    /// The run whose attempt completed successfully.
    pub completed_run_number: i32,
    /// The newly pending run number.
    pub next_run_number: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    pub next_run_at: DateTime<Utc>,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct JobSuccessCompletionOutcome {
    pub job_id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub run_number: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
}

/// Failure details supplied to the persistence lifecycle.
///
/// `policy_retry_delay_ms` supplies the ordinary retry policy. `retry_timing`
/// is an optional handler lower bound. PostgreSQL commits the later schedule.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct JobFailureUpdate<'a> {
    pub kind: JobFailureKind,
    pub code: &'a str,
    pub message: &'a str,
    /// Optional handler-selected not-before lower bound.
    pub retry_timing: Option<JobRetryTiming>,
    /// Required ordinary policy backoff when the failure remains retryable.
    pub policy_retry_delay_ms: Option<i32>,
}

impl<'a> JobFailureUpdate<'a> {
    /// Creates a failure update with ordinary policy backoff and no handler
    /// lower bound.
    #[must_use]
    pub const fn new(
        kind: JobFailureKind,
        code: &'a str,
        message: &'a str,
        policy_retry_delay_ms: Option<i32>,
    ) -> Self {
        Self {
            kind,
            code,
            message,
            retry_timing: None,
            policy_retry_delay_ms,
        }
    }

    /// Adds the handler-selected retry lower bound.
    #[must_use]
    pub const fn with_retry_timing(mut self, retry_timing: JobRetryTiming) -> Self {
        self.retry_timing = Some(retry_timing);
        self
    }
}

/// Durable outcome of completing one failed attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum JobFailureCompletionDisposition {
    /// Another attempt was scheduled from a relative delay.
    RetryScheduled {
        /// Persisted positive delay, rounded up to millisecond precision.
        retry_delay_ms: i32,
        /// Effective claim time calculated from the PostgreSQL completion clock.
        next_run_at: DateTime<Utc>,
    },
    /// The handler's lower bound selected the effective retry schedule.
    RetryScheduledAt {
        /// Handler not-before time, rounded up to PostgreSQL microsecond
        /// precision when necessary.
        requested_retry_at: DateTime<Utc>,
        /// Effective claim time. This is never earlier than policy backoff.
        next_run_at: DateTime<Utc>,
    },
    DeadLettered {
        reason: runledger_core::jobs::JobDeadLetterReason,
    },
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct JobFailureCompletionOutcome {
    pub job_id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub run_number: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    pub failure_kind: JobFailureKind,
    pub failure_code: String,
    pub failure_message: String,
    /// Latest durable checkpoint observed while locking the failed attempt.
    pub checkpoint: Option<Value>,
    pub disposition: JobFailureCompletionDisposition,
}

#[cfg(test)]
mod job_lease_identity_tests {
    use super::*;

    #[test]
    fn construction_retains_each_lease_fence() {
        let identity = JobLeaseIdentity::new(Uuid::nil(), 7, 3, "worker-identity-test");

        assert_eq!(identity.job_id, Uuid::nil());
        assert_eq!(identity.run_number, 7);
        assert_eq!(identity.attempt, 3);
        assert_eq!(identity.worker_id, "worker-identity-test");
    }
}
