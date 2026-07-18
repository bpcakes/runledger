use chrono::{DateTime, Utc};
use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

use super::super::types::JobRequeueStatePolicy;

pub(crate) const HANDLER_CONTINUATION_REASON: &str = "HANDLER_CONTINUATION";

pub(crate) enum RequeuedEventPayload<'a> {
    Basic {
        reason: &'a str,
    },
    CompareAndRequeue {
        reason: &'a str,
        state_policy: JobRequeueStatePolicy,
    },
    HandlerContinuation {
        next_run_number: i32,
        next_run_at: DateTime<Utc>,
        delay_microseconds: i64,
    },
}

pub(crate) struct RequeuedJobEvent<'a> {
    pub(crate) job_id: Uuid,
    pub(crate) completed_run_number: i32,
    pub(crate) attempt: Option<i32>,
    pub(crate) stage: Option<&'a str>,
    pub(crate) progress_done: Option<i64>,
    pub(crate) progress_total: Option<i64>,
    pub(crate) payload: RequeuedEventPayload<'a>,
}

pub(crate) async fn insert_requeued_event_tx(
    tx: &mut DbTx<'_>,
    event: RequeuedJobEvent<'_>,
    error_context: &'static str,
) -> Result<i64> {
    let (reason, state_policy, next_run_number, next_run_at, delay_microseconds) =
        match event.payload {
            RequeuedEventPayload::Basic { reason } => (reason, None, None, None, None),
            RequeuedEventPayload::CompareAndRequeue {
                reason,
                state_policy,
            } => (
                reason,
                Some(state_policy.as_event_value()),
                None,
                None,
                None,
            ),
            RequeuedEventPayload::HandlerContinuation {
                next_run_number,
                next_run_at,
                delay_microseconds,
            } => (
                HANDLER_CONTINUATION_REASON,
                None,
                Some(next_run_number),
                Some(next_run_at),
                Some(delay_microseconds),
            ),
        };

    sqlx::query_scalar!(
        "INSERT INTO job_events (
            job_id,
            run_number,
            attempt,
            event_type,
            stage,
            progress_done,
            progress_total,
            payload
         )
         VALUES (
            $1,
            $2,
            $3,
            'REQUEUED',
            $4,
            $5,
            $6,
            jsonb_strip_nulls(jsonb_build_object(
                'reason', $7::text,
                'state_policy', $8::text,
                'next_run_number', $9::int4,
                'next_run_at', $10::timestamptz,
                'delay_microseconds', $11::bigint
            ))
         )
         RETURNING id",
        event.job_id,
        event.completed_run_number,
        event.attempt,
        event.stage,
        event.progress_done,
        event.progress_total,
        reason,
        state_policy,
        next_run_number,
        next_run_at,
        delay_microseconds,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(error_context, error))
}
