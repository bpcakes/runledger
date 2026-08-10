//! Durable queue, attempt, audit-event, and workflow effects for failed leases.
//!
//! Live handler failures and already-locked expired leases intentionally retain
//! separate entry points: their queue predicates, retry timestamps, audit
//! payloads, and statement ordering differ. Shared writes live here so both
//! paths evolve together without weakening either path's fencing semantics.

use chrono::{DateTime, Utc};
use runledger_core::jobs::{JobTypeName, WorkflowStepStatus};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::{DbTx, Error, Result};

use super::super::types::JobLeaseIdentity;
use super::super::workflows::{on_retry_scheduled, on_terminal};

pub(super) const LEASE_EXPIRED_FAILURE: FailureDetails<'static> = FailureDetails {
    kind_db_value: "LEASE_EXPIRED",
    code: "job.lease_expired",
    message: "Job lease expired before completion.",
};

#[derive(Clone, Copy)]
pub(super) struct FailureIdentity {
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
}

impl FailureIdentity {
    pub(super) const fn new(job_id: Uuid, run_number: i32, attempt: i32) -> Self {
        Self {
            job_id,
            run_number,
            attempt,
        }
    }

    const fn from_lease(identity: JobLeaseIdentity<'_>) -> Self {
        Self::new(identity.job_id, identity.run_number, identity.attempt)
    }
}

#[derive(Clone, Copy)]
pub(super) struct FailureDetails<'a> {
    kind_db_value: &'a str,
    code: &'a str,
    message: &'a str,
}

impl<'a> FailureDetails<'a> {
    pub(super) const fn new(kind_db_value: &'a str, code: &'a str, message: &'a str) -> Self {
        Self {
            kind_db_value,
            code,
            message,
        }
    }

    pub(super) const fn code(self) -> &'a str {
        self.code
    }

    pub(super) const fn message(self) -> &'a str {
        self.message
    }
}

#[derive(Clone, Copy)]
pub(super) struct DeadLetterSnapshot<'a> {
    job_type: &'a JobTypeName,
    organization_id: Option<Uuid>,
    payload: &'a Value,
    checkpoint: Option<&'a Value>,
}

impl<'a> DeadLetterSnapshot<'a> {
    pub(super) const fn new(
        job_type: &'a JobTypeName,
        organization_id: Option<Uuid>,
        payload: &'a Value,
        checkpoint: Option<&'a Value>,
    ) -> Self {
        Self {
            job_type,
            organization_id,
            payload,
            checkpoint,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RetryTimingSource {
    Policy,
    HandlerNotBefore,
}

impl RetryTimingSource {
    const fn as_db_value(self) -> &'static str {
        match self {
            Self::Policy => "POLICY",
            Self::HandlerNotBefore => "HANDLER_NOT_BEFORE",
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct ResolvedRetryTiming {
    pub(super) policy_retry_delay_ms: i32,
    pub(super) requested_retry_not_before: Option<DateTime<Utc>>,
    pub(super) next_run_at: DateTime<Utc>,
    pub(super) source: RetryTimingSource,
}

impl ResolvedRetryTiming {
    pub(super) const fn next_run_at(self) -> DateTime<Utc> {
        self.next_run_at
    }
}

#[derive(Clone, Copy)]
pub(super) struct HandlerFailureTransition<'a> {
    lease: JobLeaseIdentity<'a>,
    failure: FailureDetails<'a>,
    dead_letter: DeadLetterSnapshot<'a>,
}

impl<'a> HandlerFailureTransition<'a> {
    pub(super) const fn new(
        lease: JobLeaseIdentity<'a>,
        failure: FailureDetails<'a>,
        dead_letter: DeadLetterSnapshot<'a>,
    ) -> Self {
        Self {
            lease,
            failure,
            dead_letter,
        }
    }

    pub(super) async fn apply_terminal(self, tx: &mut DbTx<'_>) -> Result<()> {
        mark_handler_dead_lettered_queue(tx, self.lease, self.failure).await?;
        upsert_dead_letter(
            tx,
            FailureIdentity::from_lease(self.lease),
            self.failure,
            self.dead_letter,
            "insert dead letter",
        )
        .await?;
        finish_failed_attempt_terminal(
            tx,
            FailureIdentity::from_lease(self.lease),
            self.failure,
            "update failed attempt terminal",
        )
        .await?;
        insert_handler_failed_event(tx, self.lease, self.failure, "insert failed event terminal")
            .await?;
        insert_handler_dead_lettered_event(tx, self.lease, self.failure).await?;
        notify_workflow_terminal(tx, self.lease.job_id, self.failure).await
    }

    pub(super) async fn apply_retry(
        self,
        tx: &mut DbTx<'_>,
        retry_timing: ResolvedRetryTiming,
    ) -> Result<()> {
        mark_handler_retryable_queue(tx, self.lease, self.failure, retry_timing.next_run_at())
            .await?;
        finish_handler_retry_attempt(tx, self.lease, self.failure, retry_timing).await?;
        insert_handler_failed_event(tx, self.lease, self.failure, "insert failed event retry")
            .await?;
        insert_handler_retry_scheduled_event(tx, self.lease, retry_timing).await?;
        notify_workflow_retry(tx, self.lease.job_id, self.failure).await
    }
}

#[derive(Clone, Copy)]
pub(super) struct ExpiredLeaseTransition<'a> {
    identity: FailureIdentity,
    dead_letter: DeadLetterSnapshot<'a>,
    started_without_renewal_heartbeat: bool,
}

impl<'a> ExpiredLeaseTransition<'a> {
    pub(super) const fn new(
        identity: FailureIdentity,
        dead_letter: DeadLetterSnapshot<'a>,
        started_without_renewal_heartbeat: bool,
    ) -> Self {
        Self {
            identity,
            dead_letter,
            started_without_renewal_heartbeat,
        }
    }

    pub(super) async fn apply_terminal(self, tx: &mut DbTx<'_>) -> Result<()> {
        mark_expired_dead_lettered_queue(tx, self.identity).await?;
        finish_failed_attempt_terminal(
            tx,
            self.identity,
            LEASE_EXPIRED_FAILURE,
            "reap update dead lettered attempt",
        )
        .await?;
        upsert_dead_letter(
            tx,
            self.identity,
            LEASE_EXPIRED_FAILURE,
            self.dead_letter,
            "reap insert dead letter row",
        )
        .await?;
        insert_expired_failed_event(tx, self.identity, self.started_without_renewal_heartbeat)
            .await?;
        insert_expired_dead_lettered_event(
            tx,
            self.identity,
            self.started_without_renewal_heartbeat,
        )
        .await?;
        notify_workflow_terminal(tx, self.identity.job_id, LEASE_EXPIRED_FAILURE).await
    }

    pub(super) async fn apply_retry(
        self,
        tx: &mut DbTx<'_>,
        retry_delay_ms: i32,
    ) -> Result<DateTime<Utc>> {
        let next_run_at = mark_expired_retryable_queue(tx, self.identity, retry_delay_ms).await?;
        finish_expired_retry_attempt(tx, self.identity, retry_delay_ms).await?;
        insert_expired_failed_event(tx, self.identity, self.started_without_renewal_heartbeat)
            .await?;
        insert_expired_retry_scheduled_event(
            tx,
            self.identity,
            retry_delay_ms,
            next_run_at,
            self.started_without_renewal_heartbeat,
        )
        .await?;
        notify_workflow_retry(tx, self.identity.job_id, LEASE_EXPIRED_FAILURE).await?;
        Ok(next_run_at)
    }
}

async fn mark_handler_dead_lettered_queue(
    tx: &mut DbTx<'_>,
    identity: JobLeaseIdentity<'_>,
    failure: FailureDetails<'_>,
) -> Result<()> {
    sqlx::query!(
        "UPDATE job_queue
         SET status = 'DEAD_LETTERED',
             lease_expires_at = NULL,
             last_heartbeat_at = NULL,
             worker_id = NULL,
             finished_at = now(),
             output = NULL,
             status_reason = $5,
             last_error_code = $6,
             last_error_message = $7,
             updated_at = now()
         WHERE id = $1
           AND run_number = $2
           AND attempt = $3
           AND worker_id = $4
           AND status = 'LEASED'",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        identity.worker_id,
        failure.kind_db_value,
        failure.code,
        failure.message,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("mark dead letter", error))?;

    Ok(())
}

async fn mark_expired_dead_lettered_queue(
    tx: &mut DbTx<'_>,
    identity: FailureIdentity,
) -> Result<()> {
    sqlx::query!(
        "UPDATE job_queue
         SET status = 'DEAD_LETTERED',
             lease_expires_at = NULL,
             last_heartbeat_at = NULL,
             worker_id = NULL,
             finished_at = now(),
             output = NULL,
             status_reason = 'LEASE_EXPIRED',
             last_error_code = 'job.lease_expired',
             last_error_message = 'Job lease expired before completion.',
             updated_at = now()
         WHERE id = $1
           AND run_number = $2",
        identity.job_id,
        identity.run_number,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("reap mark dead lettered", error))?;
    Ok(())
}

async fn upsert_dead_letter(
    tx: &mut DbTx<'_>,
    identity: FailureIdentity,
    failure: FailureDetails<'_>,
    snapshot: DeadLetterSnapshot<'_>,
    error_context: &'static str,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_dead_letters (
            job_id,
            job_type,
            organization_id,
            run_number,
            attempt,
            error_code,
            error_message,
            payload_snapshot,
            checkpoint_snapshot,
            failed_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, now())
             ON CONFLICT (job_id)
             DO UPDATE
                SET run_number = EXCLUDED.run_number,
                    attempt = EXCLUDED.attempt,
                    error_code = EXCLUDED.error_code,
                    error_message = EXCLUDED.error_message,
                    payload_snapshot = EXCLUDED.payload_snapshot,
                    checkpoint_snapshot = EXCLUDED.checkpoint_snapshot,
                    failed_at = EXCLUDED.failed_at",
        identity.job_id,
        snapshot.job_type.as_str(),
        snapshot.organization_id,
        identity.run_number,
        identity.attempt,
        failure.code,
        failure.message,
        snapshot.payload,
        snapshot.checkpoint,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(error_context, error))?;

    Ok(())
}

async fn finish_failed_attempt_terminal(
    tx: &mut DbTx<'_>,
    identity: FailureIdentity,
    failure: FailureDetails<'_>,
    error_context: &'static str,
) -> Result<()> {
    sqlx::query!(
        "UPDATE job_attempts
         SET finished_at = now(),
             outcome = $4::text::job_failure_kind,
             error_code = $5,
             error_message = $6
         WHERE job_id = $1
           AND run_number = $2
           AND attempt = $3",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        failure.kind_db_value,
        failure.code,
        failure.message,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(error_context, error))?;

    Ok(())
}

async fn insert_handler_failed_event(
    tx: &mut DbTx<'_>,
    identity: JobLeaseIdentity<'_>,
    failure: FailureDetails<'_>,
    error_context: &'static str,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_events (job_id, run_number, attempt, event_type, payload)
         VALUES (
            $1,
            $2,
            $3,
            'FAILED',
            jsonb_build_object('kind', $4::text, 'error_code', $5::text, 'error_message', $6::text)
         )",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        failure.kind_db_value,
        failure.code,
        failure.message,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(error_context, error))?;

    Ok(())
}

async fn insert_expired_failed_event(
    tx: &mut DbTx<'_>,
    identity: FailureIdentity,
    started_without_renewal_heartbeat: bool,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_events (job_id, run_number, attempt, event_type, payload)
         VALUES (
            $1,
            $2,
            $3,
            'FAILED',
            jsonb_build_object(
                'kind', 'LEASE_EXPIRED',
                'error_code', 'job.lease_expired',
                'error_message', 'Job lease expired before completion.',
                'started_without_renewal_heartbeat', $4::bool
            )
         )",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        started_without_renewal_heartbeat,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("reap failed event", error))?;
    Ok(())
}

async fn insert_handler_dead_lettered_event(
    tx: &mut DbTx<'_>,
    identity: JobLeaseIdentity<'_>,
    failure: FailureDetails<'_>,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_events (job_id, run_number, attempt, event_type, payload)
         VALUES (
            $1,
            $2,
            $3,
            'DEAD_LETTERED',
            jsonb_build_object('kind', $4::text, 'error_code', $5::text)
         )",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        failure.kind_db_value,
        failure.code,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert dead lettered event", error))?;

    Ok(())
}

async fn insert_expired_dead_lettered_event(
    tx: &mut DbTx<'_>,
    identity: FailureIdentity,
    started_without_renewal_heartbeat: bool,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_events (job_id, run_number, attempt, event_type, payload)
         VALUES (
            $1,
            $2,
            $3,
            'DEAD_LETTERED',
            jsonb_build_object(
                'kind', 'LEASE_EXPIRED',
                'error_code', 'job.lease_expired',
                'started_without_renewal_heartbeat', $4::bool
            )
         )",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        started_without_renewal_heartbeat,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("reap dead lettered event", error))?;
    Ok(())
}

async fn mark_handler_retryable_queue(
    tx: &mut DbTx<'_>,
    identity: JobLeaseIdentity<'_>,
    failure: FailureDetails<'_>,
    next_run_at: DateTime<Utc>,
) -> Result<DateTime<Utc>> {
    sqlx::query_scalar!(
        "UPDATE job_queue
         SET status = 'PENDING',
             lease_expires_at = NULL,
             last_heartbeat_at = NULL,
             worker_id = NULL,
             next_run_at = $5,
             output = NULL,
             status_reason = $6,
             last_error_code = $7,
             last_error_message = $8,
             updated_at = now()
         WHERE id = $1
           AND run_number = $2
           AND attempt = $3
           AND worker_id = $4
           AND status = 'LEASED'
         RETURNING next_run_at",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        identity.worker_id,
        next_run_at,
        failure.kind_db_value,
        failure.code,
        failure.message,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("mark retryable failure", error))
}

async fn mark_expired_retryable_queue(
    tx: &mut DbTx<'_>,
    identity: FailureIdentity,
    retry_delay_ms: i32,
) -> Result<DateTime<Utc>> {
    sqlx::query_scalar!(
        "UPDATE job_queue
         SET status = 'PENDING',
             lease_expires_at = NULL,
             last_heartbeat_at = NULL,
             worker_id = NULL,
             next_run_at = now() + ($2::bigint * interval '1 millisecond'),
             output = NULL,
             status_reason = 'LEASE_EXPIRED',
             last_error_code = 'job.lease_expired',
             last_error_message = 'Job lease expired before completion.',
             updated_at = now()
         WHERE id = $1
           AND run_number = $3
         RETURNING next_run_at",
        identity.job_id,
        i64::from(retry_delay_ms),
        identity.run_number,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("reap mark retryable", error))
}

async fn finish_handler_retry_attempt(
    tx: &mut DbTx<'_>,
    identity: JobLeaseIdentity<'_>,
    failure: FailureDetails<'_>,
    retry_timing: ResolvedRetryTiming,
) -> Result<()> {
    sqlx::query!(
        "UPDATE job_attempts
         SET finished_at = now(),
             outcome = $4::text::job_failure_kind,
             error_code = $5,
             error_message = $6,
             retry_delay_ms = $7,
             requested_retry_not_before = $8,
             effective_next_run_at = $9,
             retry_timing_source = $10
         WHERE job_id = $1
           AND run_number = $2
           AND attempt = $3",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        failure.kind_db_value,
        failure.code,
        failure.message,
        retry_timing.policy_retry_delay_ms,
        retry_timing.requested_retry_not_before,
        retry_timing.next_run_at,
        retry_timing.source.as_db_value(),
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("update failed attempt retry", error))?;

    Ok(())
}

async fn finish_expired_retry_attempt(
    tx: &mut DbTx<'_>,
    identity: FailureIdentity,
    retry_delay_ms: i32,
) -> Result<()> {
    sqlx::query!(
        "UPDATE job_attempts
         SET finished_at = now(),
             outcome = 'LEASE_EXPIRED',
             error_code = 'job.lease_expired',
             error_message = 'Job lease expired before completion.',
             retry_delay_ms = $4
         WHERE job_id = $1
           AND run_number = $2
           AND attempt = $3",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        retry_delay_ms,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("reap update retry attempt", error))?;
    Ok(())
}

async fn insert_handler_retry_scheduled_event(
    tx: &mut DbTx<'_>,
    identity: JobLeaseIdentity<'_>,
    retry_timing: ResolvedRetryTiming,
) -> Result<()> {
    // Keep the requested_retry_at and next_run_at field names as aliases for
    // 0.7 consumers. retry_delay_ms intentionally carries the policy delay
    // under the new semantics; the changelog documents that value change.
    sqlx::query!(
        "INSERT INTO job_events (job_id, run_number, attempt, event_type, payload)
         VALUES (
            $1,
            $2,
            $3,
            'RETRY_SCHEDULED',
            jsonb_strip_nulls(
                jsonb_build_object(
                    'retry_delay_ms', $4::int4,
                    'requested_retry_not_before', $5::timestamptz,
                    'requested_retry_at', $5::timestamptz,
                    'effective_next_run_at', $6::timestamptz,
                    'retry_timing_source', $7::text,
                    'next_run_at', $6::timestamptz
                )
            )
         )",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        retry_timing.policy_retry_delay_ms,
        retry_timing.requested_retry_not_before,
        retry_timing.next_run_at,
        retry_timing.source.as_db_value(),
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert retry scheduled event", error))?;

    Ok(())
}

async fn insert_expired_retry_scheduled_event(
    tx: &mut DbTx<'_>,
    identity: FailureIdentity,
    retry_delay_ms: i32,
    next_run_at: DateTime<Utc>,
    started_without_renewal_heartbeat: bool,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_events (job_id, run_number, attempt, event_type, payload)
         VALUES (
            $1,
            $2,
            $3,
            'RETRY_SCHEDULED',
            jsonb_build_object(
                'kind', 'LEASE_EXPIRED',
                'retry_delay_ms', $4::int4,
                'next_run_at', $5::timestamptz,
                'started_without_renewal_heartbeat', $6::bool
            )
         )",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        retry_delay_ms,
        next_run_at,
        started_without_renewal_heartbeat,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("reap retry event", error))?;
    Ok(())
}

async fn notify_workflow_terminal(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    failure: FailureDetails<'_>,
) -> Result<()> {
    on_terminal(
        tx,
        job_id,
        WorkflowStepStatus::Failed,
        Some(failure.kind_db_value),
        Some(failure.code),
        Some(failure.message),
        None,
    )
    .await
}

async fn notify_workflow_retry(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    failure: FailureDetails<'_>,
) -> Result<()> {
    on_retry_scheduled(
        tx,
        job_id,
        Some(failure.kind_db_value),
        Some(failure.code),
        Some(failure.message),
    )
    .await
}
