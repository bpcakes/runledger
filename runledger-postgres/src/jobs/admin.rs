use runledger_core::jobs::{JobStatus, JobType, WorkflowStepStatus};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, Result};

use super::errors::{
    invalid_job_state_error, job_not_found_error, validate_page_limit, validate_pagination,
    workflow_requeue_not_supported_error,
};
use super::row_decode::{parse_job_event_type, parse_job_stage, parse_job_type_name};
use super::rows::JobQueueRow;
use super::types::{JobEventRecord, JobListFilter, JobMetricsRecord, JobQueueRecord};
use super::workflows::on_terminal;

const JOB_PAYLOAD_UUID_ARRAY_FIELD_UPDATE_LOCK_TIMEOUT: &str = "1s";
const JOB_PAYLOAD_UUID_ARRAY_FIELD_UPDATE_LOCK_TIMEOUT_MS: i64 = 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use = "callers must inspect Updated/NotFound/Rejected"]
#[non_exhaustive]
pub enum JobPayloadUuidArrayFieldUpdate {
    Updated,
    NotFound,
    Rejected {
        reason: JobPayloadUuidArrayFieldUpdateRejection,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum JobPayloadUuidArrayFieldUpdateRejection {
    WorkflowManaged,
    IdempotentRequestSnapshot,
    NotPendingOrClaimed,
}

async fn rollback_and_classify_missing_job_mutation(
    tx: DbTx<'_>,
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_id: Uuid,
) -> Result<Error> {
    if let Err(error) = tx.rollback().await {
        tracing::warn!(error = %error, "failed to rollback missing job mutation transaction");
    }
    let exists = get_job_by_id(pool, organization_id, job_id).await?;
    Ok(if exists.is_none() {
        job_not_found_error()
    } else {
        invalid_job_state_error()
    })
}

async fn workflow_managed_job_exists_tx(
    tx: &mut DbTx<'_>,
    job_id: Uuid,
    organization_id: Option<Uuid>,
) -> Result<bool> {
    let exists: bool = sqlx::query_scalar!(
        "SELECT EXISTS (
            SELECT 1
            FROM job_queue jq
            WHERE jq.id = $1
              AND jq.workflow_step_id IS NOT NULL
              AND ($2::uuid IS NULL OR jq.organization_id = $2)
         ) AS \"exists!\"",
        job_id,
        organization_id,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("requeue workflow-managed job check", error)
    })?;

    Ok(exists)
}

#[derive(sqlx::FromRow)]
struct JobPayloadUuidArrayFieldUpdateCandidate {
    status: String,
    worker_id: Option<String>,
    lease_expires_at: Option<chrono::DateTime<chrono::Utc>>,
    workflow_step_id: Option<Uuid>,
    idempotency_key: Option<String>,
    enqueue_request: Option<Value>,
}

pub async fn list_jobs(pool: &DbPool, filter: &JobListFilter<'_>) -> Result<Vec<JobQueueRecord>> {
    validate_pagination(filter.limit, filter.offset)?;

    let status_filter = filter.status.map(JobStatus::as_db_value);

    let rows = sqlx::query_as!(
        JobQueueRow,
        "SELECT
            id,
            job_type,
            organization_id,
            payload,
            status::text AS \"status!\",
            priority,
            run_number,
            attempt,
            max_attempts,
            timeout_seconds,
            next_run_at,
            lease_expires_at,
            last_heartbeat_at,
            worker_id,
            started_at,
            finished_at,
            stage,
            progress_done,
            progress_total,
            progress_pct::float8 AS progress_pct,
            checkpoint,
            output,
            idempotency_key,
            status_reason,
            last_error_code,
            last_error_message,
            created_at,
            updated_at
         FROM job_queue
         WHERE ($1::uuid IS NULL OR organization_id = $1)
           AND ($2::text::job_status IS NULL OR status = $2::text::job_status)
           AND ($3::text IS NULL OR job_type ILIKE '%' || $3 || '%')
         ORDER BY created_at DESC, id DESC
         LIMIT $4
         OFFSET $5",
        filter.organization_id,
        status_filter,
        filter.job_type,
        filter.limit,
        filter.offset,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("list jobs", error))?;

    rows.into_iter().map(JobQueueRow::into_record).collect()
}

pub async fn get_job_by_id(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_id: Uuid,
) -> Result<Option<JobQueueRecord>> {
    let row = sqlx::query_as!(
        JobQueueRow,
        "SELECT
            id,
            job_type,
            organization_id,
            payload,
            status::text AS \"status!\",
            priority,
            run_number,
            attempt,
            max_attempts,
            timeout_seconds,
            next_run_at,
            lease_expires_at,
            last_heartbeat_at,
            worker_id,
            started_at,
            finished_at,
            stage,
            progress_done,
            progress_total,
            progress_pct::float8 AS progress_pct,
            checkpoint,
            output,
            idempotency_key,
            status_reason,
            last_error_code,
            last_error_message,
            created_at,
            updated_at
         FROM job_queue
         WHERE id = $1
           AND ($2::uuid IS NULL OR organization_id = $2)
         LIMIT 1",
        job_id,
        organization_id,
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("get job by id", error))?;

    row.map(JobQueueRow::into_record).transpose()
}

pub async fn get_job_payload_by_idempotency_key(
    pool: &DbPool,
    organization_id: Uuid,
    job_type: JobType<'_>,
    idempotency_key: &str,
) -> Result<Option<(Uuid, serde_json::Value)>> {
    let row = sqlx::query!(
        "SELECT id, payload
         FROM job_queue
         WHERE organization_id = $1
           AND job_type = $2
           AND idempotency_key = $3
         LIMIT 1",
        organization_id,
        job_type as _,
        idempotency_key,
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("get job payload by idempotency key", error)
    })?;

    Ok(row.map(|row| (row.id, row.payload)))
}

pub async fn get_latest_job_payload_for_run(
    pool: &DbPool,
    organization_id: Uuid,
    job_type: JobType<'_>,
    run_id: Uuid,
) -> Result<Option<(Uuid, serde_json::Value)>> {
    let run_id_text = run_id.to_string();
    let row = sqlx::query!(
        "SELECT id, payload
         FROM job_queue
         WHERE organization_id = $1
           AND job_type = $2
           AND payload->>'run_id' = $3
         ORDER BY created_at DESC, id DESC
         LIMIT 1",
        organization_id,
        job_type as _,
        run_id_text,
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("get latest job payload for run", error)
    })?;

    Ok(row.map(|row| (row.id, row.payload)))
}

/// Updates one UUID-array payload field on a direct, unclaimed pending job.
///
/// Returns a classified rejection when the row is already claimed or terminal,
/// belongs to a workflow step, or has an idempotency request snapshot that this
/// API cannot keep consistent.
pub async fn update_job_payload_uuid_array_field(
    pool: &DbPool,
    organization_id: Uuid,
    job_id: Uuid,
    job_type: JobType<'_>,
    payload_field: &str,
    values: &[Uuid],
) -> Result<JobPayloadUuidArrayFieldUpdate> {
    let mut tx = pool.begin().await.map_err(|error| {
        Error::from_query_sqlx_with_context(
            "begin job payload uuid array update transaction",
            error,
        )
    })?;

    let previous_lock_timeout =
        cap_job_payload_uuid_array_field_update_lock_timeout_tx(&mut tx).await?;

    let row_result = sqlx::query_as::<_, JobPayloadUuidArrayFieldUpdateCandidate>(
        "SELECT
             status::text AS status,
             worker_id,
             lease_expires_at,
             workflow_step_id,
             idempotency_key,
             enqueue_request
           FROM job_queue
           WHERE id = $1
             AND organization_id = $2
             AND job_type = $3
           FOR UPDATE",
    )
    .bind(job_id)
    .bind(organization_id)
    .bind(job_type)
    .fetch_optional(&mut *tx)
    .await;

    let row = match row_result {
        Ok(row) => {
            set_local_lock_timeout_tx(
                &mut tx,
                &previous_lock_timeout,
                "restore job payload uuid array update lock timeout",
            )
            .await?;
            row
        }
        Err(error) => {
            return Err(Error::from_query_sqlx_with_context(
                "classify job payload uuid array update",
                error,
            ));
        }
    };

    let Some(row) = row else {
        tx.commit().await.map_err(|error| {
            Error::from_query_sqlx_with_context(
                "commit job payload uuid array update transaction",
                error,
            )
        })?;
        return Ok(JobPayloadUuidArrayFieldUpdate::NotFound);
    };

    // Order matters: workflow-managed jobs can also carry request snapshots, so
    // return the ownership rejection before the snapshot-consistency rejection.
    let rejection = if row.workflow_step_id.is_some() {
        Some(JobPayloadUuidArrayFieldUpdateRejection::WorkflowManaged)
    } else if row.idempotency_key.is_some() || row.enqueue_request.is_some() {
        Some(JobPayloadUuidArrayFieldUpdateRejection::IdempotentRequestSnapshot)
    } else if row.status != JobStatus::Pending.as_db_value()
        || row.worker_id.is_some()
        || row.lease_expires_at.is_some()
    {
        Some(JobPayloadUuidArrayFieldUpdateRejection::NotPendingOrClaimed)
    } else {
        None
    };

    if let Some(reason) = rejection {
        tx.commit().await.map_err(|error| {
            Error::from_query_sqlx_with_context(
                "commit job payload uuid array update transaction",
                error,
            )
        })?;
        return Ok(JobPayloadUuidArrayFieldUpdate::Rejected { reason });
    }

    sqlx::query!(
        "UPDATE job_queue
         SET
             payload = jsonb_set(
                 payload,
                 ARRAY[$4::text],
                 to_jsonb($5::uuid[]),
                 true
             ),
             updated_at = now()
         WHERE id = $1
           AND organization_id = $2
           AND job_type = $3",
        job_id,
        organization_id,
        job_type as _,
        payload_field,
        values,
    )
    .execute(&mut *tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("update job payload uuid array field", error)
    })?;

    tx.commit().await.map_err(|error| {
        Error::from_query_sqlx_with_context(
            "commit job payload uuid array update transaction",
            error,
        )
    })?;
    Ok(JobPayloadUuidArrayFieldUpdate::Updated)
}

async fn cap_job_payload_uuid_array_field_update_lock_timeout_tx(
    tx: &mut DbTx<'_>,
) -> Result<String> {
    sqlx::query_scalar::<_, String>(
        "WITH previous AS MATERIALIZED (
             SELECT
                current_setting('lock_timeout') AS lock_timeout,
                setting::bigint AS lock_timeout_ms
             FROM pg_settings
             WHERE name = 'lock_timeout'
         )
         SELECT previous.lock_timeout
         FROM previous,
              LATERAL (
                SELECT set_config(
                    'lock_timeout',
                    CASE
                        WHEN previous.lock_timeout_ms = 0 THEN $1
                        WHEN previous.lock_timeout_ms <= $2 THEN previous.lock_timeout
                        ELSE $1
                    END,
                    true
                )
              ) AS applied",
    )
    .bind(JOB_PAYLOAD_UUID_ARRAY_FIELD_UPDATE_LOCK_TIMEOUT)
    .bind(JOB_PAYLOAD_UUID_ARRAY_FIELD_UPDATE_LOCK_TIMEOUT_MS)
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("set job payload uuid array update lock timeout", error)
    })
}

async fn set_local_lock_timeout_tx(
    tx: &mut DbTx<'_>,
    lock_timeout: &str,
    context: &'static str,
) -> Result<()> {
    sqlx::query_scalar::<_, String>("SELECT set_config('lock_timeout', $1, true)")
        .bind(lock_timeout)
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| Error::from_query_sqlx_with_context(context, error))?;

    Ok(())
}

pub async fn list_job_events(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_id: Uuid,
    limit: i64,
    after_id: Option<i64>,
) -> Result<Vec<JobEventRecord>> {
    validate_page_limit(limit)?;

    let rows = sqlx::query!(
        "SELECT
            je.id,
            je.job_id,
            je.run_number,
            je.attempt,
            je.event_type::text AS \"event_type!\",
            je.stage,
            je.progress_done,
            je.progress_total,
            je.payload,
            je.occurred_at
         FROM job_events je
         JOIN job_queue jq ON jq.id = je.job_id
         WHERE je.job_id = $1
           AND ($2::uuid IS NULL OR jq.organization_id = $2)
           AND ($3::bigint IS NULL OR je.id > $3)
         ORDER BY je.id ASC
         LIMIT $4",
        job_id,
        organization_id,
        after_id,
        limit,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("list job events", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(JobEventRecord {
                id: row.id,
                job_id: row.job_id,
                run_number: row.run_number,
                attempt: row.attempt,
                event_type: parse_job_event_type(row.event_type)?,
                stage: row.stage.map(parse_job_stage).transpose()?,
                progress_done: row.progress_done,
                progress_total: row.progress_total,
                payload: row.payload,
                occurred_at: row.occurred_at,
            })
        })
        .collect::<Result<Vec<_>>>()
}

pub async fn get_job_metrics(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_type: Option<&str>,
) -> Result<Vec<JobMetricsRecord>> {
    let rows = sqlx::query!(
        "SELECT
            jd.job_type AS \"job_type!\",
            COALESCE(SUM(jmr.pending_count), 0)::bigint AS \"pending_count!\",
            COALESCE(SUM(jmr.leased_count), 0)::bigint AS \"leased_count!\",
            COALESCE(SUM(jmr.stale_leases), 0)::bigint AS \"stale_leases!\",
            COALESCE(SUM(jmr.succeeded_24h), 0)::bigint AS \"succeeded_24h!\",
            COALESCE(SUM(jmr.retryable_24h), 0)::bigint AS \"retryable_24h!\",
            COALESCE(SUM(jmr.terminal_24h), 0)::bigint AS \"terminal_24h!\",
            COALESCE(SUM(jmr.panicked_24h), 0)::bigint AS \"panicked_24h!\",
            COALESCE(SUM(jmr.timeout_24h), 0)::bigint AS \"timeout_24h!\",
            COALESCE(SUM(jmr.dead_lettered_24h), 0)::bigint AS \"dead_lettered_24h!\",
            AVG(jmr.p50_duration_ms_24h) AS p50_duration_ms_24h,
            AVG(jmr.p95_duration_ms_24h) AS p95_duration_ms_24h
         FROM job_definitions jd
         LEFT JOIN job_metrics_rollup jmr
           ON jmr.job_type = jd.job_type
          AND ($1::uuid IS NULL OR jmr.organization_id = $1)
         WHERE ($2::text IS NULL OR jd.job_type = $2)
         GROUP BY jd.job_type
         ORDER BY jd.job_type ASC",
        organization_id,
        job_type,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("get job metrics", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(JobMetricsRecord {
                job_type: parse_job_type_name(row.job_type)?,
                pending_count: row.pending_count,
                leased_count: row.leased_count,
                stale_leases: row.stale_leases,
                succeeded_24h: row.succeeded_24h,
                retryable_24h: row.retryable_24h,
                terminal_24h: row.terminal_24h,
                panicked_24h: row.panicked_24h,
                timeout_24h: row.timeout_24h,
                dead_lettered_24h: row.dead_lettered_24h,
                p50_duration_ms_24h: row.p50_duration_ms_24h,
                p95_duration_ms_24h: row.p95_duration_ms_24h,
            })
        })
        .collect::<Result<Vec<_>>>()
}

pub async fn cancel_job(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_id: Uuid,
    reason: Option<&str>,
) -> Result<JobQueueRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;
    let Some(record) = cancel_job_tx(&mut tx, organization_id, job_id, reason).await? else {
        return Err(
            rollback_and_classify_missing_job_mutation(tx, pool, organization_id, job_id).await?,
        );
    };

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    Ok(record)
}

pub(crate) async fn cancel_job_tx(
    tx: &mut DbTx<'_>,
    organization_id: Option<Uuid>,
    job_id: Uuid,
    reason: Option<&str>,
) -> Result<Option<JobQueueRecord>> {
    let row = sqlx::query_as!(
        JobQueueRow,
        "UPDATE job_queue
         SET status = 'CANCELED',
             lease_expires_at = NULL,
             last_heartbeat_at = NULL,
             worker_id = NULL,
             finished_at = now(),
             output = NULL,
             status_reason = COALESCE($3, 'CANCELED'),
             updated_at = now()
         WHERE id = $1
           AND ($2::uuid IS NULL OR organization_id = $2)
           AND status IN ('PENDING', 'LEASED')
         RETURNING
            id,
            job_type,
            organization_id,
            payload,
            status::text AS \"status!\",
            priority,
            run_number,
            attempt,
            max_attempts,
            timeout_seconds,
            next_run_at,
            lease_expires_at,
            last_heartbeat_at,
            worker_id,
            started_at,
            finished_at,
            stage,
            progress_done,
            progress_total,
            progress_pct::float8 AS progress_pct,
            checkpoint,
            output,
            idempotency_key,
            status_reason,
            last_error_code,
            last_error_message,
            created_at,
            updated_at",
        job_id,
        organization_id,
        reason,
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("cancel job", error))?;

    let Some(row) = row else {
        return Ok(None);
    };

    let record = row.into_record()?;

    sqlx::query!(
        "UPDATE job_attempts
         SET finished_at = now()
         WHERE job_id = $1
           AND run_number = $2
           AND attempt = $3
           AND finished_at IS NULL",
        record.id,
        record.run_number,
        record.attempt,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("close canceled attempt", error))?;

    let event_attempt = (record.attempt > 0).then_some(record.attempt);
    sqlx::query!(
        "INSERT INTO job_events (
            job_id,
            run_number,
            attempt,
            event_type,
            payload
         )
         VALUES (
            $1,
            $2,
            $3,
            'CANCELED',
            jsonb_build_object('reason', $4::text)
         )",
        record.id,
        record.run_number,
        event_attempt,
        record.status_reason.as_deref(),
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert canceled event", error))?;

    on_terminal(
        tx,
        record.id,
        WorkflowStepStatus::Canceled,
        record.status_reason.as_deref(),
        None,
        None,
        None,
    )
    .await?;

    Ok(Some(record))
}

fn ensure_workflow_requeue_rejection_rollback_succeeded(
    rollback_result: std::result::Result<(), sqlx::Error>,
) -> Result<()> {
    rollback_result.map_err(|error| Error::ConnectionError(error.to_string()))
}

pub async fn requeue_job(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_id: Uuid,
    reason: Option<&str>,
) -> Result<JobQueueRecord> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    let workflow_managed = workflow_managed_job_exists_tx(&mut tx, job_id, organization_id).await?;
    if workflow_managed {
        ensure_workflow_requeue_rejection_rollback_succeeded(tx.rollback().await)?;
        return Err(workflow_requeue_not_supported_error());
    }

    let previous_run = sqlx::query!(
        "SELECT run_number, attempt
         FROM job_queue
         WHERE id = $1
           AND ($2::uuid IS NULL OR organization_id = $2)
           AND status IN ('DEAD_LETTERED', 'CANCELED', 'SUCCEEDED')
         FOR UPDATE",
        job_id,
        organization_id,
    )
    .fetch_optional(&mut *tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("requeue job prefetch attempt", error))?;

    let Some(previous_run) = previous_run else {
        return Err(
            rollback_and_classify_missing_job_mutation(tx, pool, organization_id, job_id).await?,
        );
    };
    let previous_run_number: i32 = previous_run.run_number;
    let previous_attempt: i32 = previous_run.attempt;

    let row = sqlx::query_as!(
        JobQueueRow,
        "UPDATE job_queue
         SET status = 'PENDING',
             stage = 'queued',
             progress_done = NULL,
             progress_total = NULL,
             checkpoint = NULL,
             output = NULL,
             run_number = run_number + 1,
             attempt = 0,
             lease_expires_at = NULL,
             last_heartbeat_at = NULL,
             worker_id = NULL,
             next_run_at = now(),
             started_at = NULL,
             finished_at = NULL,
             status_reason = COALESCE($3, 'REQUEUED'),
             last_error_code = NULL,
             last_error_message = NULL,
             updated_at = now()
         WHERE id = $1
           AND ($2::uuid IS NULL OR organization_id = $2)
           AND status IN ('DEAD_LETTERED', 'CANCELED', 'SUCCEEDED')
         RETURNING
            id,
            job_type,
            organization_id,
            payload,
            status::text AS \"status!\",
            priority,
            run_number,
            attempt,
            max_attempts,
            timeout_seconds,
            next_run_at,
            lease_expires_at,
            last_heartbeat_at,
            worker_id,
            started_at,
            finished_at,
            stage,
            progress_done,
            progress_total,
            progress_pct::float8 AS progress_pct,
            checkpoint,
            output,
            idempotency_key,
            status_reason,
            last_error_code,
            last_error_message,
            created_at,
            updated_at",
        job_id,
        organization_id,
        reason,
    )
    .fetch_optional(&mut *tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("requeue job", error))?;

    let Some(row) = row else {
        return Err(
            rollback_and_classify_missing_job_mutation(tx, pool, organization_id, job_id).await?,
        );
    };

    let record = row.into_record()?;

    let event_attempt = (previous_attempt > 0).then_some(previous_attempt);
    sqlx::query!(
        "INSERT INTO job_events (
            job_id,
            run_number,
            attempt,
            event_type,
            payload
         )
         VALUES (
            $1,
            $2,
            $3,
            'REQUEUED',
            jsonb_build_object('reason', $4::text)
         )",
        record.id,
        previous_run_number,
        event_attempt,
        record.status_reason.as_deref(),
    )
    .execute(&mut *tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("insert requeued event", error))?;

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    Ok(record)
}

#[cfg(test)]
mod tests {
    use super::ensure_workflow_requeue_rejection_rollback_succeeded;
    use crate::Error;

    #[test]
    fn workflow_requeue_rejection_allows_validation_error_after_successful_rollback() {
        let result = ensure_workflow_requeue_rejection_rollback_succeeded(Ok(()));
        assert!(result.is_ok());
    }

    #[test]
    fn workflow_requeue_rejection_returns_internal_error_when_rollback_fails() {
        let result =
            ensure_workflow_requeue_rejection_rollback_succeeded(Err(sqlx::Error::PoolTimedOut));
        match result {
            Err(Error::ConnectionError(message)) => assert!(!message.is_empty()),
            other => panic!("expected connection error, got {other:?}"),
        }
    }
}
