use chrono::{DateTime, Utc};
use runledger_core::jobs::JobType;
use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

pub(crate) use super::super::types::HANDLER_CONTINUATION_REASON;
use super::super::types::{
    BASIC_REQUEUE_KIND, COMPARE_AND_REQUEUE_KIND, HANDLER_CONTINUATION_KIND, JobRequeueStatePolicy,
};

pub(crate) enum EnqueuedEventPayload<'a> {
    Ordinary,
    SuccessfulReplay {
        replayed_from_job_id: Uuid,
        replayed_from_run_number: i32,
        replay_request_key: &'a str,
        reason: &'a str,
    },
}

pub(crate) struct EnqueuedJobEvent<'a> {
    pub(crate) job_id: Uuid,
    pub(crate) run_number: i32,
    pub(crate) stage: &'a str,
    pub(crate) job_type: JobType<'a>,
    pub(crate) payload: EnqueuedEventPayload<'a>,
}

pub(crate) async fn insert_enqueued_event_tx(
    tx: &mut DbTx<'_>,
    event: EnqueuedJobEvent<'_>,
) -> Result<()> {
    let (replayed_from_job_id, replayed_from_run_number, replay_request_key, replay_reason) =
        match event.payload {
            EnqueuedEventPayload::Ordinary => (None, None, None, None),
            EnqueuedEventPayload::SuccessfulReplay {
                replayed_from_job_id,
                replayed_from_run_number,
                replay_request_key,
                reason,
            } => (
                Some(replayed_from_job_id),
                Some(replayed_from_run_number),
                Some(replay_request_key),
                Some(reason),
            ),
        };

    sqlx::query!(
        "INSERT INTO job_events (
            job_id,
            run_number,
            event_type,
            stage,
            payload
         )
         VALUES (
            $1,
            $2,
            'ENQUEUED',
            $3,
            jsonb_strip_nulls(jsonb_build_object(
                'job_type', $4::text,
                'replayed_from_job_id', $5::uuid,
                'replayed_from_run_number', $6::int4,
                'replay_request_key', $7::text,
                'reason', $8::text
            ))
         )",
        event.job_id,
        event.run_number,
        event.stage,
        event.job_type as _,
        replayed_from_job_id,
        replayed_from_run_number,
        replay_request_key,
        replay_reason,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("enqueue job event", error))?;

    Ok(())
}

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

impl RequeuedEventPayload<'_> {
    const fn kind(&self) -> &'static str {
        match self {
            Self::Basic { .. } => BASIC_REQUEUE_KIND,
            Self::CompareAndRequeue { .. } => COMPARE_AND_REQUEUE_KIND,
            Self::HandlerContinuation { .. } => HANDLER_CONTINUATION_KIND,
        }
    }
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
    let requeue_kind = event.payload.kind();
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
                'delay_microseconds', $11::bigint,
                'requeue_kind', $12::text
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
        requeue_kind,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(error_context, error))
}
