use chrono::{DateTime, Utc};
use runledger_core::jobs::{JobEventType, JobStage};
use serde_json::Value;
use sqlx::types::Uuid;

use super::enqueue::JobRequeueStatePolicy;

pub(crate) const BASIC_REQUEUE_KIND: &str = "BASIC";
pub(crate) const COMPARE_AND_REQUEUE_KIND: &str = "COMPARE_AND_REQUEUE";
pub(crate) const HANDLER_CONTINUATION_KIND: &str = "HANDLER_CONTINUATION";
pub(crate) const HANDLER_CONTINUATION_REASON: &str = HANDLER_CONTINUATION_KIND;

#[derive(Clone, Debug)]
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

/// Payload shapes authored by `runledger-postgres` that can be decoded without
/// exposing their JSON representation to consumers.
///
/// [`JobEventRecord::payload`] remains available so older, custom, malformed,
/// and future event payloads can still be inspected. Such payloads decode to an
/// `Unknown` or `Other` variant rather than failing the event-list query.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
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
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
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
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SuccessfulReplayEnqueuedEventPayload<'a> {
    pub replayed_from_job_id: Uuid,
    pub replayed_from_run_number: i32,
    pub replay_request_key: &'a str,
    pub reason: &'a str,
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
