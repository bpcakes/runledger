use chrono::{DateTime, Utc};
use runledger_core::jobs::{JobDeadLetterReason, JobFailureKind, WorkflowStepStatus};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, Result};

use super::super::super::errors::require_positive_retry_delay;
use super::super::super::row_decode::parse_job_type_name;
use super::super::super::types::{
    JobFailureCompletionDisposition, JobFailureCompletionOutcome, JobFailureUpdate,
};
use super::super::super::workflows::{on_retry_scheduled, on_terminal};
use super::common::{
    COMPLETE_FAILURE_LEASE_MISMATCH_CONTEXT, INSERT_FAILED_EVENT_RETRY_CONTEXT,
    INSERT_FAILED_EVENT_TERMINAL_CONTEXT, rollback_and_return_lease_mismatch,
};

#[derive(Clone, Copy)]
struct FailureJobIdentity<'a> {
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &'a str,
}

struct FailureLookupRow {
    max_attempts: i32,
    payload_snapshot: Value,
    checkpoint_snapshot: Option<Value>,
    job_type: runledger_core::jobs::JobTypeName,
    organization_id: Option<Uuid>,
}

#[derive(Clone, Copy)]
struct FailureOutcome<'a> {
    kind_db_value: &'a str,
    code: &'a str,
    message: &'a str,
    retry_delay_ms: Option<i32>,
    dead_letter_reason: Option<JobDeadLetterReason>,
}

async fn load_failure_lookup_row(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
) -> Result<Option<FailureLookupRow>> {
    let row = sqlx::query!(
        "WITH locked_job AS MATERIALIZED (
             SELECT max_attempts, payload, checkpoint, job_type, organization_id, lease_expires_at
             FROM job_queue
             WHERE id = $1
               AND run_number = $2
               AND attempt = $3
               AND worker_id = $4
               AND status = 'LEASED'
               AND lease_expires_at IS NOT NULL
             FOR UPDATE
         )
         SELECT max_attempts, payload, checkpoint, job_type, organization_id
         FROM locked_job
         WHERE lease_expires_at > clock_timestamp()",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        identity.worker_id,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("complete job failure lookup", error))?;

    row.map(|row| {
        Ok(FailureLookupRow {
            max_attempts: row.max_attempts,
            payload_snapshot: row.payload,
            checkpoint_snapshot: row.checkpoint,
            job_type: parse_job_type_name(row.job_type)?,
            organization_id: row.organization_id,
        })
    })
    .transpose()
}

fn failure_outcome<'a>(
    attempt: i32,
    max_attempts: i32,
    failure: &'a JobFailureUpdate<'a>,
) -> FailureOutcome<'a> {
    let dead_letter_reason = if matches!(
        failure.kind,
        JobFailureKind::Terminal | JobFailureKind::Panicked
    ) {
        Some(JobDeadLetterReason::FailureKindNonRetryable)
    } else if attempt >= max_attempts {
        Some(JobDeadLetterReason::AttemptsExhausted)
    } else {
        None
    };

    FailureOutcome {
        kind_db_value: failure.kind.as_db_value(),
        code: failure.code,
        message: failure.message,
        retry_delay_ms: failure.retry_delay_ms,
        dead_letter_reason,
    }
}

async fn apply_terminal_failure(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    lookup: &FailureLookupRow,
    outcome: FailureOutcome<'_>,
) -> Result<()> {
    mark_dead_lettered_job_queue_for_failure_tx(tx, identity, outcome).await?;
    upsert_job_dead_letter_for_failure_tx(tx, identity, lookup, outcome).await?;
    update_failed_attempt_terminal_tx(tx, identity, outcome).await?;
    insert_failed_event_for_failure_tx(tx, identity, outcome, INSERT_FAILED_EVENT_TERMINAL_CONTEXT)
        .await?;
    insert_dead_lettered_event_for_failure_tx(tx, identity, outcome).await?;

    on_terminal(
        tx,
        identity.job_id,
        WorkflowStepStatus::Failed,
        Some(outcome.kind_db_value),
        Some(outcome.code),
        Some(outcome.message),
        None,
    )
    .await?;

    Ok(())
}

async fn mark_dead_lettered_job_queue_for_failure_tx(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    outcome: FailureOutcome<'_>,
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
        outcome.kind_db_value,
        outcome.code,
        outcome.message,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("mark dead letter", error))?;

    Ok(())
}

async fn upsert_job_dead_letter_for_failure_tx(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    lookup: &FailureLookupRow,
    outcome: FailureOutcome<'_>,
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
        lookup.job_type.as_str(),
        lookup.organization_id,
        identity.run_number,
        identity.attempt,
        outcome.code,
        outcome.message,
        lookup.payload_snapshot,
        lookup.checkpoint_snapshot,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert dead letter", error))?;

    Ok(())
}

async fn update_failed_attempt_terminal_tx(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    outcome: FailureOutcome<'_>,
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
        outcome.kind_db_value,
        outcome.code,
        outcome.message,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("update failed attempt terminal", error)
    })?;

    Ok(())
}

async fn insert_failed_event_for_failure_tx(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    outcome: FailureOutcome<'_>,
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
        outcome.kind_db_value,
        outcome.code,
        outcome.message,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(error_context, error))?;

    Ok(())
}

async fn insert_dead_lettered_event_for_failure_tx(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    outcome: FailureOutcome<'_>,
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
        outcome.kind_db_value,
        outcome.code,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert dead lettered event", error))?;

    Ok(())
}

async fn apply_retryable_failure(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    outcome: FailureOutcome<'_>,
) -> Result<DateTime<Utc>> {
    let retry_delay_ms = require_positive_retry_delay(outcome.retry_delay_ms)?;
    let next_run_at =
        mark_retryable_job_queue_for_failure_tx(tx, identity, outcome, retry_delay_ms).await?;
    update_failed_attempt_retryable_tx(tx, identity, outcome, retry_delay_ms).await?;
    insert_failed_event_for_failure_tx(tx, identity, outcome, INSERT_FAILED_EVENT_RETRY_CONTEXT)
        .await?;
    insert_retry_scheduled_event_for_failure_tx(tx, identity, retry_delay_ms, next_run_at).await?;
    on_retry_scheduled(
        tx,
        identity.job_id,
        Some(outcome.kind_db_value),
        Some(outcome.code),
        Some(outcome.message),
    )
    .await?;

    Ok(next_run_at)
}

async fn mark_retryable_job_queue_for_failure_tx(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    outcome: FailureOutcome<'_>,
    retry_delay_ms: i32,
) -> Result<DateTime<Utc>> {
    sqlx::query_scalar!(
        "UPDATE job_queue
         SET status = 'PENDING',
             lease_expires_at = NULL,
             last_heartbeat_at = NULL,
             worker_id = NULL,
             next_run_at = now() + ($5::bigint * interval '1 millisecond'),
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
        i64::from(retry_delay_ms),
        outcome.kind_db_value,
        outcome.code,
        outcome.message,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("mark retryable failure", error))
}

async fn update_failed_attempt_retryable_tx(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    outcome: FailureOutcome<'_>,
    retry_delay_ms: i32,
) -> Result<()> {
    sqlx::query!(
        "UPDATE job_attempts
         SET finished_at = now(),
             outcome = $4::text::job_failure_kind,
             error_code = $5,
             error_message = $6,
             retry_delay_ms = $7
         WHERE job_id = $1
           AND run_number = $2
           AND attempt = $3",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        outcome.kind_db_value,
        outcome.code,
        outcome.message,
        retry_delay_ms,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("update failed attempt retry", error))?;

    Ok(())
}

async fn insert_retry_scheduled_event_for_failure_tx(
    tx: &mut DbTx<'_>,
    identity: FailureJobIdentity<'_>,
    retry_delay_ms: i32,
    next_run_at: DateTime<Utc>,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_events (job_id, run_number, attempt, event_type, payload)
         VALUES (
            $1,
            $2,
            $3,
            'RETRY_SCHEDULED',
            jsonb_build_object('retry_delay_ms', $4::int4, 'next_run_at', $5::timestamptz)
         )",
        identity.job_id,
        identity.run_number,
        identity.attempt,
        retry_delay_ms,
        next_run_at,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert retry scheduled event", error))?;

    Ok(())
}

pub async fn complete_job_failure(
    pool: &DbPool,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &str,
    failure: &JobFailureUpdate<'_>,
) -> Result<()> {
    complete_job_failure_with_outcome(pool, job_id, run_number, attempt, worker_id, failure)
        .await
        .map(|_| ())
}

pub async fn complete_job_failure_with_outcome(
    pool: &DbPool,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
    worker_id: &str,
    failure: &JobFailureUpdate<'_>,
) -> Result<JobFailureCompletionOutcome> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    let identity = FailureJobIdentity {
        job_id,
        run_number,
        attempt,
        worker_id,
    };

    let Some(lookup) = load_failure_lookup_row(&mut tx, identity).await? else {
        return rollback_and_return_lease_mismatch(tx, COMPLETE_FAILURE_LEASE_MISMATCH_CONTEXT)
            .await;
    };

    let outcome = failure_outcome(attempt, lookup.max_attempts, failure);

    let disposition = if let Some(reason) = outcome.dead_letter_reason {
        apply_terminal_failure(&mut tx, identity, &lookup, outcome).await?;
        JobFailureCompletionDisposition::DeadLettered { reason }
    } else {
        let retry_delay_ms = require_positive_retry_delay(outcome.retry_delay_ms)?;
        let next_run_at = apply_retryable_failure(&mut tx, identity, outcome).await?;
        JobFailureCompletionDisposition::RetryScheduled {
            retry_delay_ms,
            next_run_at,
        }
    };

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    Ok(JobFailureCompletionOutcome {
        job_id,
        job_type: lookup.job_type,
        organization_id: lookup.organization_id,
        run_number,
        attempt,
        max_attempts: lookup.max_attempts,
        failure_kind: failure.kind,
        failure_code: failure.code.to_owned(),
        failure_message: failure.message.to_owned(),
        checkpoint: lookup.checkpoint_snapshot,
        disposition,
    })
}
