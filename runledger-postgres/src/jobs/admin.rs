use std::future::Future;

use runledger_core::jobs::{JobStatus, JobType, WorkflowStepStatus};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Error, Result};

use super::errors::{
    cancellation_not_quiesced_error, ensure_rejection_rollback_succeeded, invalid_job_state_error,
    job_not_found_error, validate_page_limit, validate_pagination,
    workflow_requeue_not_supported_error,
};
use super::queue::advance::{
    AdvanceJobToNextRun, JOB_QUEUE_COLUMNS_SQL, advance_locked_job_to_next_run_tx,
};
use super::queue::events::{RequeuedEventPayload, RequeuedJobEvent, insert_requeued_event_tx};
use super::row_decode::{parse_job_event_type, parse_job_stage, parse_job_type_name};
use super::rows::JobQueueRow;
use super::transaction_isolation::{
    begin_owned_read_committed_tx, ensure_read_committed_tx, finish_owned_transaction,
};
use super::types::{
    CompareAndRequeueJob, CompareAndRequeueJobOutcome, JobContinuationMetricsRecord,
    JobEventRecord, JobListFilter, JobMetricsRecord, JobQueueRecord,
};
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

/// Returns continuation-specific canary and runaway-loop signals by job type.
pub async fn get_job_continuation_metrics(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_type: Option<&str>,
) -> Result<Vec<JobContinuationMetricsRecord>> {
    let rows = sqlx::query!(
        "SELECT
            jd.job_type AS \"job_type!\",
            COALESCE(SUM(jcmr.continued_24h), 0)::bigint AS \"continued_24h!\",
            COALESCE(SUM(jcmr.active_continued_count), 0)::bigint AS \"active_continued_count!\",
            COALESCE(MAX(jcmr.max_active_run_number), 0)::int4 AS \"max_active_run_number!\"
         FROM job_definitions jd
         LEFT JOIN job_continuation_metrics_rollup jcmr
           ON jcmr.job_type = jd.job_type
          AND ($1::uuid IS NULL OR jcmr.organization_id = $1)
         WHERE ($2::text IS NULL OR jd.job_type = $2)
         GROUP BY jd.job_type
         ORDER BY jd.job_type ASC",
        organization_id,
        job_type,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("get job continuation metrics", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(JobContinuationMetricsRecord {
                job_type: parse_job_type_name(row.job_type)?,
                continued_24h: row.continued_24h,
                active_continued_count: row.active_continued_count,
                max_active_run_number: row.max_active_run_number,
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
    // Preserve a live lease's original expiry as a cancellation-quiescence
    // marker. Status fencing rejects every subsequent worker write immediately,
    // while compare-and-requeue waits until this marker has passed before it
    // can start a new run. Pending jobs already have a NULL marker.
    let row = sqlx::query_as!(
        JobQueueRow,
        "UPDATE job_queue
         SET status = 'CANCELED',
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
            jsonb_strip_nulls(jsonb_build_object(
                'reason', $4::text,
                'lease_quiesces_at', $5::timestamptz
            ))
         )",
        record.id,
        record.run_number,
        event_attempt,
        record.status_reason.as_deref(),
        record.lease_expires_at,
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

#[derive(sqlx::FromRow)]
struct CompareAndRequeueCandidateRow {
    #[sqlx(flatten)]
    job: JobQueueRow,
    workflow_step_id: Option<Uuid>,
    canceled_lease_still_active: bool,
}

struct CompareAndRequeueCandidate {
    job: JobQueueRecord,
    workflow_managed: bool,
    canceled_lease_still_active: bool,
}

fn compare_and_requeue_candidate_from_row(
    row: CompareAndRequeueCandidateRow,
) -> Result<CompareAndRequeueCandidate> {
    let job = row.job.into_record()?;
    let workflow_managed = row.workflow_step_id.is_some();
    Ok(CompareAndRequeueCandidate {
        job,
        workflow_managed,
        canceled_lease_still_active: row.canceled_lease_still_active,
    })
}

async fn lock_compare_and_requeue_candidate_tx(
    tx: &mut DbTx<'_>,
    request: &CompareAndRequeueJob<'_>,
) -> Result<Option<JobQueueRecord>> {
    // Requeue never changes the job's identity, so NO KEY UPDATE is sufficient
    // and composes with the legacy keyed-enqueue path's KEY SHARE lock.
    let sql = format!(
        "SELECT
            {JOB_QUEUE_COLUMNS_SQL},
            workflow_step_id,
            (
                status = 'CANCELED'
                AND lease_expires_at IS NOT NULL
                AND lease_expires_at > clock_timestamp()
            ) AS canceled_lease_still_active
         FROM job_queue
         WHERE id = $1
           AND organization_id IS NOT DISTINCT FROM $2::uuid
           AND status::text = $3::text
           AND run_number = $4::int4
           AND workflow_step_id IS NULL
           AND NOT (
                status = 'CANCELED'
                AND lease_expires_at IS NOT NULL
                AND lease_expires_at > clock_timestamp()
           )
         FOR NO KEY UPDATE"
    );
    let row = sqlx::query_as::<_, CompareAndRequeueCandidateRow>(&sql)
        .bind(request.job_id)
        .bind(request.scope.organization_id())
        .bind(request.expected_status.as_db_value())
        .bind(request.expected_run_number)
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("lock compare-and-requeue job", error)
        })?;

    let Some(candidate) = row
        .map(compare_and_requeue_candidate_from_row)
        .transpose()?
    else {
        return Ok(None);
    };
    debug_assert!(!candidate.workflow_managed);
    debug_assert!(!candidate.canceled_lease_still_active);
    Ok(Some(candidate.job))
}

async fn load_compare_and_requeue_candidate_for_classification_tx(
    tx: &mut DbTx<'_>,
    request: &CompareAndRequeueJob<'_>,
) -> Result<Option<CompareAndRequeueCandidate>> {
    // A mismatch/no-mutation read deliberately omits row locking so a
    // caller-owned transaction cannot stall a live worker or an operator
    // acting on a rejected row.
    let sql = format!(
        "SELECT
            {JOB_QUEUE_COLUMNS_SQL},
            workflow_step_id,
            (
                status = 'CANCELED'
                AND lease_expires_at IS NOT NULL
                AND lease_expires_at > clock_timestamp()
            ) AS canceled_lease_still_active
         FROM job_queue
         WHERE id = $1
           AND organization_id IS NOT DISTINCT FROM $2::uuid"
    );
    let row = sqlx::query_as::<_, CompareAndRequeueCandidateRow>(&sql)
        .bind(request.job_id)
        .bind(request.scope.organization_id())
        .fetch_optional(&mut **tx)
        .await
        .map_err(|error| {
            Error::from_query_sqlx_with_context("read compare-and-requeue mismatch", error)
        })?;

    row.map(compare_and_requeue_candidate_from_row).transpose()
}

async fn update_compare_and_requeue_candidate_tx(
    tx: &mut DbTx<'_>,
    request: &CompareAndRequeueJob<'_>,
) -> Result<JobQueueRecord> {
    advance_locked_job_to_next_run_tx(
        tx,
        &AdvanceJobToNextRun {
            job_id: request.job_id,
            preserve_missing_resume_state: request.state_policy.preserves_progress_and_checkpoint(),
            progress_done: None,
            progress_total: None,
            checkpoint: None,
            next_run_at: None,
            status_reason: Some(request.reason),
        },
        "compare and requeue job",
    )
    .await
}

/// Atomically requeues an exactly observed canceled or dead-lettered job in an
/// internally owned `READ COMMITTED` transaction.
///
/// Prefer this convenience API when recovery does not need to compose with
/// other database changes. Use [`compare_and_requeue_job_tx`] when the mutation
/// must be part of a caller-owned transaction.
///
/// Every normal [`CompareAndRequeueJobOutcome`] is committed before it is
/// returned. Database errors are rolled back before returning to the caller.
pub async fn compare_and_requeue_job(
    pool: &DbPool,
    request: CompareAndRequeueJob<'_>,
) -> Result<CompareAndRequeueJobOutcome> {
    const OPERATION: &str = "compare-and-requeue";

    let mut tx = begin_owned_read_committed_tx(pool, OPERATION).await?;
    // `begin_owned_read_committed_tx` established the exact isolation required
    // by the operation body, so the pool-owned path need not query it again.
    let result = compare_and_requeue_job_read_committed_tx(&mut tx, request).await;
    finish_owned_transaction(tx, OPERATION, result).await
}

/// Atomically requeues an exactly scoped canceled or dead-lettered job only if
/// its terminal status and run number still match the caller's observation.
/// `state_policy` explicitly controls whether committed progress/checkpoint
/// state is carried into the new run or cleared.
///
/// The caller transaction must use `READ COMMITTED`. A mismatch against a live
/// row is read without taking a row lock. If cancellation fenced a leased
/// handler whose original lease window is still active, this returns
/// [`CompareAndRequeueJobOutcome::CancellationNotQuiesced`] instead of starting
/// an overlapping run.
///
/// The caller owns `tx`; this function neither commits nor rolls it back.
/// Missing rows, stale expectations, active cancellation fences, and workflow
/// rejections do not leave the job row locked in the caller transaction.
pub async fn compare_and_requeue_job_tx(
    tx: &mut DbTx<'_>,
    request: CompareAndRequeueJob<'_>,
) -> Result<CompareAndRequeueJobOutcome> {
    ensure_read_committed_tx(
        tx,
        "job compare-and-requeue",
        "job.compare_and_requeue_unsupported_isolation",
        "Job compare-and-requeue requires READ COMMITTED transaction isolation.",
    )
    .await?;

    compare_and_requeue_job_read_committed_tx(tx, request).await
}

async fn compare_and_requeue_job_read_committed_tx(
    tx: &mut DbTx<'_>,
    request: CompareAndRequeueJob<'_>,
) -> Result<CompareAndRequeueJobOutcome> {
    compare_and_requeue_job_read_committed_tx_inner(tx, request, || {
        std::future::ready(Ok::<(), Error>(()))
    })
    .await
}

async fn compare_and_requeue_job_read_committed_tx_inner<AfterLockMiss, AfterLockMissFuture>(
    tx: &mut DbTx<'_>,
    request: CompareAndRequeueJob<'_>,
    mut after_lock_miss: AfterLockMiss,
) -> Result<CompareAndRequeueJobOutcome>
where
    AfterLockMiss: FnMut() -> AfterLockMissFuture,
    AfterLockMissFuture: Future<Output = Result<()>>,
{
    let before = loop {
        if let Some(before) = lock_compare_and_requeue_candidate_tx(tx, &request).await? {
            break before;
        }

        after_lock_miss().await?;
        let Some(actual) =
            load_compare_and_requeue_candidate_for_classification_tx(tx, &request).await?
        else {
            return Ok(CompareAndRequeueJobOutcome::NotFound);
        };
        if actual.job.status == request.expected_status.as_job_status()
            && actual.job.run_number == request.expected_run_number
        {
            if actual.workflow_managed {
                return Err(workflow_requeue_not_supported_error());
            }

            if let (true, Some(retry_after)) = (
                actual.canceled_lease_still_active,
                actual.job.lease_expires_at,
            ) {
                return Ok(CompareAndRequeueJobOutcome::CancellationNotQuiesced {
                    actual: Box::new(actual.job),
                    retry_after,
                });
            }

            // READ COMMITTED gives each statement a fresh snapshot. The row may
            // have become mutation-eligible after the locking read missed it,
            // so retry instead of returning a contradictory mismatch whose
            // actual state equals the caller's expectation.
            continue;
        }

        return Ok(CompareAndRequeueJobOutcome::ExpectationMismatch {
            actual: Box::new(actual.job),
        });
    };
    let after = update_compare_and_requeue_candidate_tx(tx, &request).await?;

    let event_attempt = (before.attempt > 0).then_some(before.attempt);
    let event_id = insert_requeued_event_tx(
        tx,
        RequeuedJobEvent {
            job_id: before.id,
            completed_run_number: before.run_number,
            attempt: event_attempt,
            stage: None,
            progress_done: None,
            progress_total: None,
            payload: RequeuedEventPayload::CompareAndRequeue {
                reason: request.reason,
                state_policy: request.state_policy,
            },
        },
        "insert compare-and-requeue event",
    )
    .await?;

    Ok(CompareAndRequeueJobOutcome::Requeued {
        before: Box::new(before),
        after: Box::new(after),
        event_id,
    })
}

#[deprecated(
    since = "0.6.0",
    note = "use compare_and_requeue_job (or compare_and_requeue_job_tx for caller-owned transactions) with exact JobScope and RequeueableJobStatus expectations"
)]
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
        ensure_rejection_rollback_succeeded(tx.rollback().await)?;
        return Err(workflow_requeue_not_supported_error());
    }

    let previous_run = sqlx::query!(
        "SELECT
            run_number,
            attempt,
            lease_expires_at,
            (
                status = 'CANCELED'
                AND lease_expires_at IS NOT NULL
                AND lease_expires_at > clock_timestamp()
            ) AS \"canceled_lease_still_active!\"
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
    if let (true, Some(retry_after)) = (
        previous_run.canceled_lease_still_active,
        previous_run.lease_expires_at,
    ) {
        ensure_rejection_rollback_succeeded(tx.rollback().await)?;
        return Err(cancellation_not_quiesced_error(retry_after));
    }

    let record = advance_locked_job_to_next_run_tx(
        &mut tx,
        &AdvanceJobToNextRun {
            job_id,
            preserve_missing_resume_state: false,
            progress_done: None,
            progress_total: None,
            checkpoint: None,
            next_run_at: None,
            status_reason: reason,
        },
        "requeue job",
    )
    .await?;

    let event_attempt = (previous_attempt > 0).then_some(previous_attempt);
    insert_requeued_event_tx(
        &mut tx,
        RequeuedJobEvent {
            job_id: record.id,
            completed_run_number: previous_run_number,
            attempt: event_attempt,
            stage: None,
            progress_done: None,
            progress_total: None,
            payload: RequeuedEventPayload::Basic {
                reason: record.status_reason.as_deref().unwrap_or("REQUEUED"),
            },
        },
        "insert requeued event",
    )
    .await?;

    tx.commit()
        .await
        .map_err(|error| Error::ConnectionError(error.to_string()))?;

    Ok(record)
}

#[cfg(test)]
mod tests {
    use runledger_core::jobs::{JobFailureKind, JobStatus, JobType};
    use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
    use serde_json::json;

    use super::compare_and_requeue_job_read_committed_tx_inner;
    use crate::jobs::{
        CompareAndRequeueJob, CompareAndRequeueJobOutcome, JobDefinitionUpsert, JobEnqueue,
        JobFailureUpdate, JobRequeueStatePolicy, JobScope, RequeueableJobStatus, claim_jobs,
        complete_job_failure, enqueue_job, upsert_job_definition_tx,
    };

    #[tokio::test]
    async fn compare_and_requeue_retries_lock_when_later_snapshot_matches_expectation() {
        const JOB_TYPE: &str = "jobs.test.compare_requeue_snapshot_race";

        let (pool, database) =
            setup_ephemeral_pool("postgres_compare_requeue_snapshot_race", 4).await;
        let mut definition_tx = pool.begin().await.expect("begin definition transaction");
        upsert_job_definition_tx(
            &mut definition_tx,
            &JobDefinitionUpsert {
                job_type: JobType::new(JOB_TYPE),
                version: 1,
                max_attempts: 3,
                default_timeout_seconds: 60,
                default_priority: 100,
                is_enabled: true,
            },
        )
        .await
        .expect("upsert job definition");
        definition_tx.commit().await.expect("commit job definition");

        let payload = json!({ "case": "snapshot-race" });
        let job_id = enqueue_job(
            &pool,
            &JobEnqueue {
                job_type: JobType::new(JOB_TYPE),
                organization_id: None,
                payload: &payload,
                priority: None,
                max_attempts: None,
                timeout_seconds: None,
                next_run_at: None,
                idempotency_key: None,
                stage: None,
            },
        )
        .await
        .expect("enqueue job");
        let claim = claim_jobs(&pool, "worker-snapshot-race", 30, 1)
            .await
            .expect("claim job")
            .pop()
            .expect("one job should be claimed");
        let worker_id = claim
            .worker_id
            .clone()
            .expect("claimed job should have a worker id");

        let mut transition = Some((claim.id, claim.run_number, claim.attempt, worker_id));
        let transition_pool = pool.clone();
        let mut recovery_tx = pool.begin().await.expect("begin recovery transaction");
        let outcome = compare_and_requeue_job_read_committed_tx_inner(
            &mut recovery_tx,
            CompareAndRequeueJob {
                scope: JobScope::Global,
                job_id,
                expected_status: RequeueableJobStatus::DeadLettered,
                expected_run_number: claim.run_number,
                state_policy: JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
                reason: "recover terminal transition observed by later snapshot",
            },
            || {
                let transition = transition.take();
                let transition_pool = transition_pool.clone();
                async move {
                    if let Some((job_id, run_number, attempt, worker_id)) = transition {
                        complete_job_failure(
                            &transition_pool,
                            job_id,
                            run_number,
                            attempt,
                            &worker_id,
                            &JobFailureUpdate::new(
                                JobFailureKind::Terminal,
                                "job.test.snapshot_race",
                                "terminal transition between recovery reads",
                                None,
                            ),
                        )
                        .await?;
                    }
                    Ok(())
                }
            },
        )
        .await
        .expect("recovery should retry the exact lock");

        let CompareAndRequeueJobOutcome::Requeued { before, after, .. } = outcome else {
            panic!("later exact snapshot must requeue instead of returning mismatch");
        };
        assert_eq!(before.status, JobStatus::DeadLettered);
        assert_eq!(before.run_number, claim.run_number);
        assert_eq!(after.status, JobStatus::Pending);
        assert_eq!(after.run_number, claim.run_number + 1);
        recovery_tx.commit().await.expect("commit recovery");

        teardown_ephemeral_pool(pool, database).await;
    }
}
