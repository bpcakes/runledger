use std::{fmt, time::Duration};

use chrono::{DateTime, Utc};
use runledger_core::jobs::{
    JobEventType, JobFailure, JobFailureKind, JobRetryTiming, JobStage, JobStatus, JobType,
    JobTypeName,
};
use serde_json::Value;
use sqlx::types::Uuid;

/// Maximum accepted schedule jitter, in seconds.
///
/// The scheduler treats jitter as a deterministic spread applied to future fire
/// cursors, and the persistence layer rejects larger values.
pub const JOB_SCHEDULE_MAX_JITTER_SECONDS: i32 = 86_400;

/// Maximum page size accepted by public job and workflow list APIs.
///
/// This bounds accidental unbounded reads from admin/TUI surfaces while still
/// allowing operators to inspect a large page when needed.
pub const JOB_LIST_PAGE_LIMIT_MAX: i64 = 1_000;

pub(crate) const BASIC_REQUEUE_KIND: &str = "BASIC";
pub(crate) const COMPARE_AND_REQUEUE_KIND: &str = "COMPARE_AND_REQUEUE";
pub(crate) const HANDLER_CONTINUATION_KIND: &str = "HANDLER_CONTINUATION";
pub(crate) const HANDLER_CONTINUATION_REASON: &str = HANDLER_CONTINUATION_KIND;

#[derive(Debug, Clone)]
pub struct JobDefinitionUpsert<'a> {
    pub job_type: JobType<'a>,
    pub version: i32,
    pub max_attempts: i32,
    pub default_timeout_seconds: i32,
    pub default_priority: i32,
    pub is_enabled: bool,
}

#[derive(Debug, Clone)]
pub struct JobDefinitionRecord {
    pub job_type: JobTypeName,
    pub version: i32,
    pub max_attempts: i32,
    pub default_timeout_seconds: i32,
    pub default_priority: i32,
    pub is_enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Schedule row that blocks a job-definition catalog sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobScheduleJobTypeReference {
    /// Active schedule name.
    pub schedule_name: String,
    /// Job type referenced by the active schedule.
    pub job_type: JobTypeName,
}

#[derive(Debug, Clone)]
pub struct JobDefinitionListFilter<'a> {
    /// Admin list query input used for escaped `ILIKE` substring matching, not a canonical
    /// persisted identifier boundary.
    pub job_type: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Clone)]
pub struct JobDefinitionUpdate {
    pub max_attempts: Option<i32>,
    pub default_timeout_seconds: Option<i32>,
    pub default_priority: Option<i32>,
    pub is_enabled: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct JobRuntimeConfigUpsert<'a> {
    pub job_type: JobType<'a>,
    pub schema_version: i32,
    pub config: &'a Value,
    pub updated_by_user_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub struct JobRuntimeConfigRecord {
    pub job_type: JobTypeName,
    pub schema_version: i32,
    pub config: Value,
    pub updated_by_user_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobEnqueueOutcome {
    pub job_id: Uuid,
    pub status: JobStatus,
    pub run_number: i32,
    pub disposition: JobEnqueueDisposition,
}

/// Exact tenant scope for a job mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequeueableJobStatus {
    DeadLettered,
    Canceled,
}

/// Error returned when a job observation cannot seed compare-and-requeue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct JobScheduleRecord {
    /// Stable schedule row identifier.
    pub id: Uuid,
    /// Unique schedule name.
    pub name: String,
    /// Job type enqueued whenever the schedule fires.
    pub job_type: JobTypeName,
    /// Optional organization scope copied into jobs created by this schedule.
    pub organization_id: Option<Uuid>,
    /// JSON payload template copied into each scheduled job before runtime
    /// schedule metadata is merged.
    pub payload_template: Value,
    /// UTC cron expression used by the runtime scheduler.
    pub cron_expr: String,
    /// Whether the runtime scheduler may claim this schedule.
    ///
    /// Schedule upserts preserve this value for existing rows; use
    /// `set_job_schedule_active` to pause or resume a schedule intentionally.
    pub is_active: bool,
    /// Maximum deterministic jitter, in seconds, applied when computing the next
    /// fire cursor. Must not exceed [`JOB_SCHEDULE_MAX_JITTER_SECONDS`].
    pub max_jitter_seconds: i32,
    /// Next UTC instant at which this schedule is due for materialization.
    pub next_fire_at: DateTime<Utc>,
}

/// Input for creating or updating a cron-backed job schedule.
///
/// Schedules are keyed by `name`. Updating an existing schedule refreshes the
/// stored job type, payload template, cron expression, and jitter, while leaving
/// scheduler-managed state intact. `organization_id` and `is_active` apply only
/// when a new schedule row is inserted. `next_fire_at` applies on insert and
/// when the cron expression changes.
///
/// Cron expressions are interpreted in UTC and must be accepted by
/// `cron::Schedule::from_str`, the same parser used by `runledger-runtime` when
/// materializing due schedules. The upsert validator rejects blank or padded
/// schedule names, blank or padded cron expressions, invalid cron expressions,
/// negative jitter, and jitter above [`JOB_SCHEDULE_MAX_JITTER_SECONDS`].
///
/// This input does not encode a compile-time job catalog. The PostgreSQL schema
/// requires a matching job-definition row for `job_type`, but this API does not
/// prove that a worker process has registered a runtime handler for that job
/// type.
#[derive(Debug, Clone)]
pub struct JobScheduleUpsert<'a> {
    /// Stable unique schedule name without surrounding whitespace.
    pub name: &'a str,
    /// Job type to enqueue whenever the schedule fires.
    pub job_type: JobType<'a>,
    /// Optional organization scope for enqueued jobs on first insert.
    pub organization_id: Option<Uuid>,
    /// JSON payload copied into each job created by the scheduler.
    pub payload_template: &'a Value,
    /// UTC cron expression without surrounding whitespace, validated on upsert
    /// and parsed again when the schedule fires.
    pub cron_expr: &'a str,
    /// Whether the runtime scheduler should claim this schedule on first insert.
    pub is_active: bool,
    /// Initial fire cursor for the scheduler, also used when changing cron syntax.
    pub next_fire_at: DateTime<Utc>,
    /// Maximum deterministic jitter applied when materializing a due schedule,
    /// capped at [`JOB_SCHEDULE_MAX_JITTER_SECONDS`].
    pub max_jitter_seconds: i32,
}

/// One catalog-owned schedule sync entry.
#[derive(Debug, Clone)]
pub struct JobScheduleCatalogSyncEntry<'a> {
    /// Schedule definition fields to upsert. Unlike plain schedule upserts,
    /// catalog sync treats `is_active` as the authoritative desired active state
    /// for both inserts and conflicts.
    pub upsert: JobScheduleUpsert<'a>,
}

/// Result returned by [`super::schedules::sync_catalog_job_schedules_tx`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobScheduleCatalogSyncReport {
    /// Schedule names upserted and active state applied during this sync.
    pub synced_schedule_names: Vec<String>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct JobEventRecord {
    pub id: i64,
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: Option<i32>,
    pub event_type: JobEventType,
    pub stage: Option<JobStage>,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub payload: Value,
    pub occurred_at: DateTime<Utc>,
}

/// Payload shapes authored by `runledger-postgres` that can be decoded without
/// exposing their JSON representation to consumers.
///
/// [`JobEventRecord::payload`] remains available so older, custom, malformed,
/// and future event payloads can still be inspected. Such payloads decode to an
/// `Unknown` or `Other` variant rather than failing the event-list query.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedJobEventPayload<'a> {
    /// An ordinary, compare-and-requeue, continuation, or unrecognized
    /// `REQUEUED` payload.
    Requeued(DecodedRequeuedEventPayload<'a>),
    /// Provenance attached to the `ENQUEUED` event for a successful-job replay.
    SuccessfulReplayEnqueued(SuccessfulReplayEnqueuedEventPayload<'a>),
    /// An event type or payload shape not decoded by this version of the
    /// persistence driver.
    Other,
}

/// Decoded payload for a `REQUEUED` event.
///
/// The decoder recognizes both current payloads with a `requeue_kind`
/// discriminator and historical kindless payloads written before that field
/// was introduced.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodedRequeuedEventPayload<'a> {
    /// A release or legacy administrative requeue.
    #[non_exhaustive]
    Basic { reason: &'a str },
    /// An optimistic compare-and-requeue recovery.
    #[non_exhaustive]
    CompareAndRequeue {
        reason: &'a str,
        state_policy: JobRequeueStatePolicy,
    },
    /// A successful handler continuation into a newly pending run.
    #[non_exhaustive]
    HandlerContinuation {
        reason: &'a str,
        next_run_number: i32,
        next_run_at: DateTime<Utc>,
        delay_microseconds: i64,
    },
    /// A malformed or future `REQUEUED` payload. The reason is retained when
    /// it is a JSON string so generic operator surfaces can still display it.
    #[non_exhaustive]
    Unknown { reason: Option<&'a str> },
}

/// Successful-job replay provenance decoded from an `ENQUEUED` event.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuccessfulReplayEnqueuedEventPayload<'a> {
    pub replayed_from_job_id: Uuid,
    pub replayed_from_run_number: i32,
    pub replay_request_key: &'a str,
    pub reason: &'a str,
}

impl JobEventRecord {
    /// Decodes payload variants whose JSON schema is owned by
    /// `runledger-postgres`.
    ///
    /// This accessor is deliberately infallible. Unknown event types,
    /// malformed JSON fields, and future discriminators remain available via
    /// [`Self::payload`] and decode to a compatibility fallback.
    #[must_use]
    pub fn decoded_payload(&self) -> DecodedJobEventPayload<'_> {
        match self.event_type {
            JobEventType::Requeued => {
                DecodedJobEventPayload::Requeued(decode_requeued_event_payload(&self.payload))
            }
            JobEventType::Enqueued => decode_successful_replay_enqueued_payload(&self.payload)
                .map(DecodedJobEventPayload::SuccessfulReplayEnqueued)
                .unwrap_or(DecodedJobEventPayload::Other),
            _ => DecodedJobEventPayload::Other,
        }
    }
}

fn decode_requeued_event_payload(payload: &Value) -> DecodedRequeuedEventPayload<'_> {
    let reason = payload.get("reason").and_then(Value::as_str);
    let unknown = || DecodedRequeuedEventPayload::Unknown { reason };
    let decode_basic = || {
        reason
            .map(|reason| DecodedRequeuedEventPayload::Basic { reason })
            .unwrap_or_else(unknown)
    };
    let decode_compare_and_requeue = || {
        let Some(reason) = reason else {
            return unknown();
        };
        let Some(state_policy) = payload
            .get("state_policy")
            .and_then(Value::as_str)
            .and_then(JobRequeueStatePolicy::from_event_value)
        else {
            return unknown();
        };
        DecodedRequeuedEventPayload::CompareAndRequeue {
            reason,
            state_policy,
        }
    };
    let decode_handler_continuation = || {
        let Some(reason) = reason else {
            return unknown();
        };
        let Some(next_run_number) = payload
            .get("next_run_number")
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok())
        else {
            return unknown();
        };
        let Some(next_run_at) = payload
            .get("next_run_at")
            .and_then(Value::as_str)
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
            .map(|value| value.with_timezone(&Utc))
        else {
            return unknown();
        };
        let Some(delay_microseconds) = payload.get("delay_microseconds").and_then(Value::as_i64)
        else {
            return unknown();
        };
        DecodedRequeuedEventPayload::HandlerContinuation {
            reason,
            next_run_number,
            next_run_at,
            delay_microseconds,
        }
    };

    let handler_schedule_keys = ["next_run_number", "next_run_at", "delay_microseconds"];
    let has_complete_handler_schedule = handler_schedule_keys
        .iter()
        .all(|key| payload.get(*key).is_some());
    let has_any_handler_schedule_field = handler_schedule_keys
        .iter()
        .any(|key| payload.get(*key).is_some());

    match payload.get("requeue_kind") {
        Some(Value::String(kind)) => match kind.as_str() {
            BASIC_REQUEUE_KIND => decode_basic(),
            COMPARE_AND_REQUEUE_KIND => decode_compare_and_requeue(),
            HANDLER_CONTINUATION_KIND => decode_handler_continuation(),
            _ => unknown(),
        },
        Some(_) => unknown(),
        None if reason == Some(HANDLER_CONTINUATION_REASON) && has_complete_handler_schedule => {
            decode_handler_continuation()
        }
        None if payload.get("state_policy").is_some() => decode_compare_and_requeue(),
        None if has_any_handler_schedule_field => unknown(),
        None => decode_basic(),
    }
}

fn decode_successful_replay_enqueued_payload(
    payload: &Value,
) -> Option<SuccessfulReplayEnqueuedEventPayload<'_>> {
    let replayed_from_job_id = payload
        .get("replayed_from_job_id")?
        .as_str()?
        .parse()
        .ok()?;
    let replayed_from_run_number =
        i32::try_from(payload.get("replayed_from_run_number")?.as_i64()?).ok()?;
    let replay_request_key = payload.get("replay_request_key")?.as_str()?;
    let reason = payload.get("reason")?.as_str()?;

    Some(SuccessfulReplayEnqueuedEventPayload {
        replayed_from_job_id,
        replayed_from_run_number,
        replay_request_key,
        reason,
    })
}

#[cfg(test)]
mod decoded_event_payload_tests {
    use serde_json::json;

    use super::*;

    fn event_record(event_type: JobEventType, payload: Value) -> JobEventRecord {
        JobEventRecord {
            id: 1,
            job_id: Uuid::nil(),
            run_number: 1,
            attempt: None,
            event_type,
            stage: None,
            progress_done: None,
            progress_total: None,
            payload,
            occurred_at: Utc::now(),
        }
    }

    #[test]
    fn kindless_requeue_payloads_preserve_legacy_decoding() {
        let basic = event_record(JobEventType::Requeued, json!({"reason": "released"}));
        assert!(matches!(
            basic.decoded_payload(),
            DecodedJobEventPayload::Requeued(DecodedRequeuedEventPayload::Basic {
                reason: "released",
                ..
            })
        ));

        let compare = event_record(
            JobEventType::Requeued,
            json!({
                "reason": "operator recovery",
                "state_policy": "reset_progress_and_checkpoint"
            }),
        );
        assert!(matches!(
            compare.decoded_payload(),
            DecodedJobEventPayload::Requeued(DecodedRequeuedEventPayload::CompareAndRequeue {
                reason: "operator recovery",
                state_policy: JobRequeueStatePolicy::ResetProgressAndCheckpoint,
                ..
            })
        ));

        let continuation = event_record(
            JobEventType::Requeued,
            json!({
                "reason": "HANDLER_CONTINUATION",
                "next_run_number": 2,
                "next_run_at": "2026-07-19T12:34:56.123456Z",
                "delay_microseconds": 250_000
            }),
        );
        assert!(matches!(
            continuation.decoded_payload(),
            DecodedJobEventPayload::Requeued(DecodedRequeuedEventPayload::HandlerContinuation {
                reason: "HANDLER_CONTINUATION",
                next_run_number: 2,
                delay_microseconds: 250_000,
                ..
            })
        ));
    }

    #[test]
    fn present_unknown_or_non_string_discriminators_are_not_legacy_payloads() {
        for requeue_kind in [json!("FUTURE_REQUEUE_KIND"), json!(42), json!(null)] {
            let payload = json!({
                "requeue_kind": requeue_kind,
                "reason": "future recovery"
            });
            let event = event_record(JobEventType::Requeued, payload.clone());

            assert!(matches!(
                event.decoded_payload(),
                DecodedJobEventPayload::Requeued(DecodedRequeuedEventPayload::Unknown {
                    reason: Some("future recovery"),
                    ..
                })
            ));
            assert_eq!(event.payload, payload);
        }
    }

    #[test]
    fn malformed_known_payloads_fall_back_without_losing_the_raw_payload() {
        let payload = json!({
            "requeue_kind": "HANDLER_CONTINUATION",
            "reason": "HANDLER_CONTINUATION",
            "next_run_number": "2",
            "next_run_at": "not-a-timestamp",
            "delay_microseconds": 250_000
        });
        let event = event_record(JobEventType::Requeued, payload.clone());

        assert!(matches!(
            event.decoded_payload(),
            DecodedJobEventPayload::Requeued(DecodedRequeuedEventPayload::Unknown {
                reason: Some("HANDLER_CONTINUATION"),
                ..
            })
        ));
        assert_eq!(event.payload, payload);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReapedTerminalLeaseRecord {
    pub job_id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub run_number: i32,
    pub attempt: i32,
    pub payload: Value,
}

#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReapedLeaseDisposition {
    ReleasedToPending,
    RetryScheduled {
        retry_delay_ms: i32,
        next_run_at: DateTime<Utc>,
    },
    DeadLetteredTerminal {
        payload: Value,
    },
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ReapedLeaseRecord {
    pub job_id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub run_number: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    /// Checkpoint committed on the leased run before it was reaped.
    pub checkpoint: Option<Value>,
    pub worker_id: Option<String>,
    pub started_without_renewal_heartbeat: bool,
    pub failure: JobFailure,
    pub disposition: ReapedLeaseDisposition,
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ReapExpiredLeaseDeferredError {
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: i32,
    pub error_code: String,
    pub error_message: String,
    pub sqlstate: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ReapExpiredLeasesResult {
    pub processed: i64,
    pub terminal_dead_lettered: Vec<ReapedTerminalLeaseRecord>,
}

#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ReapExpiredLeasesDetailedResult {
    pub summary: ReapExpiredLeasesResult,
    pub reaped_leases: Vec<ReapedLeaseRecord>,
    pub deferred_row_error_count: usize,
    pub deferred_row_errors: Vec<ReapExpiredLeaseDeferredError>,
}

#[derive(Debug, Clone)]
pub struct JobMetricsRecord {
    pub job_type: JobTypeName,
    pub pending_count: i64,
    pub leased_count: i64,
    pub stale_leases: i64,
    pub succeeded_24h: i64,
    pub retryable_24h: i64,
    pub terminal_24h: i64,
    pub panicked_24h: i64,
    pub timeout_24h: i64,
    pub dead_lettered_24h: i64,
    pub p50_duration_ms_24h: Option<f64>,
    pub p95_duration_ms_24h: Option<f64>,
}

/// Continuation-specific operational signals for one job type.
///
/// Kept separate from [`JobMetricsRecord`] so adding continuation visibility
/// does not break downstream code that constructs the established metrics DTO.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct JobContinuationMetricsRecord {
    pub job_type: JobTypeName,
    /// Successful handler continuations recorded during the last 24 hours.
    pub continued_24h: i64,
    /// Pending or leased jobs whose current run was created by continuation.
    pub active_continued_count: i64,
    /// Highest current run number among those continuation-created runs.
    pub max_active_run_number: i32,
}

#[derive(Debug, Clone)]
pub struct JobLogRecord {
    pub id: i64,
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: Option<i32>,
    pub level: String,
    pub message: String,
    pub payload: Value,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct JobLogRecordInput {
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: Option<i32>,
    pub level: String,
    pub message: String,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct JobProgressUpdate<'a> {
    pub stage: Option<JobStage>,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub checkpoint: Option<&'a Value>,
}

#[derive(Debug, Clone)]
pub struct JobCompletionUpdate<'a> {
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub checkpoint: Option<&'a Value>,
    pub output: Option<&'a Value>,
}

/// Progress and scheduling data for a successful handler continuation.
#[derive(Debug, Clone)]
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

#[non_exhaustive]
#[derive(Debug, Clone)]
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

#[non_exhaustive]
#[derive(Debug, Clone)]
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
/// `retry_timing` is a requested schedule. The returned completion disposition
/// reports the effective schedule committed by PostgreSQL.
#[derive(Debug, Clone)]
pub struct JobFailureUpdate<'a> {
    pub kind: JobFailureKind,
    pub code: &'a str,
    pub message: &'a str,
    /// Required when the failure remains retryable. Terminal, panicked, and
    /// attempts-exhausted failures ignore this value.
    pub retry_timing: Option<JobRetryTiming>,
}

/// Durable outcome of completing one failed attempt.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobFailureCompletionDisposition {
    /// Another attempt was scheduled from a relative delay.
    RetryScheduled {
        /// Persisted positive delay, rounded up to millisecond precision.
        retry_delay_ms: i32,
        /// Effective claim time calculated from the PostgreSQL completion clock.
        next_run_at: DateTime<Utc>,
    },
    /// Another attempt was scheduled from an absolute provider reset timestamp.
    RetryScheduledAt {
        /// Absolute UTC time requested by the handler, rounded up to
        /// PostgreSQL microsecond precision when necessary.
        requested_retry_at: DateTime<Utc>,
        /// Effective claim time. This equals the database completion clock when
        /// `requested_retry_at` has already passed.
        next_run_at: DateTime<Utc>,
    },
    DeadLettered {
        reason: runledger_core::jobs::JobDeadLetterReason,
    },
}

#[non_exhaustive]
#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct JobListFilter<'a> {
    pub organization_id: Option<Uuid>,
    pub status: Option<JobStatus>,
    /// Admin list query input used for `ILIKE` substring matching, not a canonical persisted
    /// identifier boundary.
    pub job_type: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Clone)]
pub struct JobRuntimeConfigListFilter<'a> {
    /// Admin query filter string used for listing/runtime-config lookup filters, not a canonical
    /// persisted identifier boundary.
    pub job_type: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}
