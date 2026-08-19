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

/// A durable request to enqueue a job after its definition becomes available.
///
/// Recording an intent does not read `job_definitions` and does not create a
/// queue row. Every intent is strictly idempotent; retries must preserve every
/// requested enqueue field.
#[derive(Clone, Debug)]
pub struct JobEnqueueIntent<'a> {
    job_type: JobType<'a>,
    organization_id: Option<Uuid>,
    payload: &'a Value,
    priority: Option<i32>,
    max_attempts: Option<i32>,
    timeout_seconds: Option<i32>,
    next_run_at: Option<DateTime<Utc>>,
    idempotency_key: &'a str,
    stage: Option<JobStage>,
    execution_resource_key: Option<&'a str>,
}

impl<'a> JobEnqueueIntent<'a> {
    #[must_use]
    pub fn new(job_type: JobType<'a>, payload: &'a Value, idempotency_key: &'a str) -> Self {
        Self {
            job_type,
            organization_id: None,
            payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key,
            stage: None,
            execution_resource_key: None,
        }
    }

    #[must_use]
    pub fn with_organization_id(mut self, organization_id: Uuid) -> Self {
        self.organization_id = Some(organization_id);
        self
    }

    #[must_use]
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = Some(priority);
        self
    }

    #[must_use]
    pub fn with_max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = Some(max_attempts);
        self
    }

    #[must_use]
    pub fn with_timeout_seconds(mut self, timeout_seconds: i32) -> Self {
        self.timeout_seconds = Some(timeout_seconds);
        self
    }

    #[must_use]
    pub fn with_next_run_at(mut self, next_run_at: DateTime<Utc>) -> Self {
        self.next_run_at = Some(next_run_at);
        self
    }

    #[must_use]
    pub fn with_stage(mut self, stage: JobStage) -> Self {
        self.stage = Some(stage);
        self
    }

    #[must_use]
    pub fn with_execution_resource(mut self, execution_resource_key: &'a str) -> Self {
        self.execution_resource_key = Some(execution_resource_key);
        self
    }

    pub(crate) fn as_job_enqueue(&self) -> JobEnqueue<'a> {
        JobEnqueue {
            job_type: self.job_type,
            organization_id: self.organization_id,
            payload: self.payload,
            priority: self.priority,
            max_attempts: self.max_attempts,
            timeout_seconds: self.timeout_seconds,
            next_run_at: self.next_run_at,
            idempotency_key: Some(self.idempotency_key),
            stage: self.stage,
        }
    }

    pub(crate) fn execution_resource_key(&self) -> Option<&'a str> {
        self.execution_resource_key
    }
}

/// Durable lifecycle state of a job enqueue intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum JobEnqueueIntentStatus {
    Pending,
    Promoted,
    Conflicted,
}

impl JobEnqueueIntentStatus {
    #[must_use]
    pub const fn as_db_value(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Promoted => "PROMOTED",
            Self::Conflicted => "CONFLICTED",
        }
    }
}

impl std::str::FromStr for JobEnqueueIntentStatus {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "PENDING" => Ok(Self::Pending),
            "PROMOTED" => Ok(Self::Promoted),
            "CONFLICTED" => Ok(Self::Conflicted),
            _ => Err(()),
        }
    }
}

/// Whether an intent record call inserted a row or resolved an existing retry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum JobEnqueueIntentDisposition {
    Inserted,
    Existing,
}

/// Stable state returned by an intent record call.
#[derive(Clone, Debug, Eq, PartialEq)]
#[must_use = "callers must inspect whether the intent is pending, promoted, or conflicted"]
#[non_exhaustive]
pub struct JobEnqueueIntentOutcome {
    pub intent_id: Uuid,
    pub status: JobEnqueueIntentStatus,
    pub promoted_job_id: Option<Uuid>,
    pub disposition: JobEnqueueIntentDisposition,
}

/// Persisted enqueue intent returned by lookup and list APIs.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct JobEnqueueIntentRecord {
    pub id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub payload: Value,
    pub priority: Option<i32>,
    pub max_attempts: Option<i32>,
    pub timeout_seconds: Option<i32>,
    pub next_run_at: Option<DateTime<Utc>>,
    pub idempotency_key: String,
    pub stage: JobStage,
    pub enqueue_request_version: i16,
    pub execution_resource_key: Option<String>,
    pub promotion_attempts: i32,
    pub next_promotion_at: DateTime<Utc>,
    pub last_attempted_at: Option<DateTime<Utc>>,
    pub status: JobEnqueueIntentStatus,
    pub promoted_job_id: Option<Uuid>,
    pub promoted_at: Option<DateTime<Utc>>,
    pub conflicted_at: Option<DateTime<Utc>>,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Bounded filters for listing durable enqueue intents.
#[derive(Clone, Debug)]
pub struct JobEnqueueIntentListFilter<'a> {
    pub(crate) organization_id: Option<Uuid>,
    pub(crate) status: Option<JobEnqueueIntentStatus>,
    pub(crate) job_type_query: Option<&'a str>,
    pub(crate) limit: i64,
    pub(crate) offset: i64,
}

impl<'a> JobEnqueueIntentListFilter<'a> {
    #[must_use]
    pub const fn new(limit: i64, offset: i64) -> Self {
        Self {
            organization_id: None,
            status: None,
            job_type_query: None,
            limit,
            offset,
        }
    }

    #[must_use]
    pub const fn with_organization_id(mut self, organization_id: Uuid) -> Self {
        self.organization_id = Some(organization_id);
        self
    }

    #[must_use]
    pub const fn with_status(mut self, status: JobEnqueueIntentStatus) -> Self {
        self.status = Some(status);
        self
    }

    /// Filters by a case-insensitive job-type substring.
    ///
    /// PostgreSQL `ILIKE` metacharacters in `job_type_query` retain their
    /// normal wildcard meaning, matching the crate's other admin filters.
    #[must_use]
    pub const fn with_job_type_query(mut self, job_type_query: &'a str) -> Self {
        self.job_type_query = Some(job_type_query);
        self
    }
}

/// Operational backlog signals for one intent job type.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct JobEnqueueIntentMetricsRecord {
    pub job_type: JobTypeName,
    pub pending_count: i64,
    pub retrying_count: i64,
    pub max_promotion_attempts: i32,
    pub conflicted_count: i64,
    pub promoted_24h: i64,
    pub oldest_pending_at: Option<DateTime<Utc>>,
}

/// Results from one bounded intent promotion pass.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct JobEnqueueIntentPromotionReport {
    pub inserted_jobs: u64,
    pub existing_jobs: u64,
    pub conflicted: u64,
    pub definition_unavailable: u64,
    pub retry_deferred: u64,
    pub total_promoted: u64,
    batch_was_full: bool,
}

impl JobEnqueueIntentPromotionReport {
    /// Returns whether the storage layer claimed its effective per-transaction
    /// limit, indicating that immediately eligible work may remain.
    #[must_use]
    pub const fn batch_was_full(&self) -> bool {
        self.batch_was_full
    }

    pub(in crate::jobs) fn mark_batch_size(&mut self, claimed: usize, limit: i64) {
        self.batch_was_full = usize::try_from(limit).is_ok_and(|limit| claimed == limit);
    }
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
