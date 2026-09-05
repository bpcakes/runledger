use runledger_core::jobs::{JobStatus, JobType};
use sqlx::types::Uuid;

use crate::{DbPool, Error, Result};

use super::super::errors::{validate_page_limit, validate_pagination};
use super::super::row_decode::{parse_job_event_type, parse_job_stage};
use super::super::rows::JobQueueRow;
use super::super::types::{
    JobEventRecord, JobListFilter, JobQueueRecord, JobReadListFilter, JobReadScope, JobScope,
};

struct JobPayloadRow {
    id: Uuid,
    payload: serde_json::Value,
}

/// Lists jobs with legacy visibility: a None organization matches every scope.
/// Prefer [`list_jobs_with_scope`] for new code.
pub async fn list_jobs(pool: &DbPool, filter: &JobListFilter<'_>) -> Result<Vec<JobQueueRecord>> {
    list_jobs_with_scope(
        pool,
        &JobReadListFilter {
            scope: JobReadScope::from_legacy(filter.organization_id),
            status: filter.status,
            job_type: filter.job_type,
            limit: filter.limit,
            offset: filter.offset,
        },
    )
    .await
}

/// Lists jobs within an application-authorized, explicit visibility scope.
pub async fn list_jobs_with_scope(
    pool: &DbPool,
    filter: &JobReadListFilter<'_>,
) -> Result<Vec<JobQueueRecord>> {
    validate_pagination(filter.limit, filter.offset)?;

    let status_filter = filter.status.map(JobStatus::as_db_value);

    let rows = super::super::scoped_read::scoped_list!(
        JobQueueRow,
        pool,
        filter.scope,
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
         WHERE",
        "AND ($2::text::job_status IS NULL OR status = $2::text::job_status)
           AND ($3::text IS NULL OR job_type ILIKE '%' || $3 || '%')
         ORDER BY created_at DESC, id DESC
         LIMIT $4
         OFFSET $5",
        status_filter,
        filter.job_type,
        filter.limit,
        filter.offset,
    )
    .map_err(|error| Error::from_query_sqlx_with_context("list jobs", error))?;

    rows.into_iter().map(JobQueueRow::into_record).collect()
}

/// Legacy read: None matches global and organization-owned jobs.
/// Prefer [`get_job_by_id_with_scope`] for new code.
pub async fn get_job_by_id(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_id: Uuid,
) -> Result<Option<JobQueueRecord>> {
    get_job_by_id_with_scope(pool, JobReadScope::from_legacy(organization_id), job_id).await
}

/// Reads within an application-authorized, explicit job visibility scope.
pub async fn get_job_by_id_with_scope(
    pool: &DbPool,
    scope: JobReadScope,
    job_id: Uuid,
) -> Result<Option<JobQueueRecord>> {
    let (is_admin, organization_id) = scope.visibility_predicate();
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
           AND ($3::bool OR organization_id IS NOT DISTINCT FROM $2::uuid)
         LIMIT 1",
        job_id,
        organization_id,
        is_admin,
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("get job by id", error))?;

    row.map(JobQueueRow::into_record).transpose()
}

/// Tenant-only compatibility wrapper for [`get_job_payload_by_idempotency_key_with_scope`].
pub async fn get_job_payload_by_idempotency_key(
    pool: &DbPool,
    organization_id: Uuid,
    job_type: JobType<'_>,
    idempotency_key: &str,
) -> Result<Option<(Uuid, serde_json::Value)>> {
    get_job_payload_by_idempotency_key_with_scope(
        pool,
        JobScope::Organization(organization_id),
        job_type,
        idempotency_key,
    )
    .await
}

/// Looks up a payload in one exact global or tenant scope, returning `None` if absent.
/// Applications must authorize the selected [`JobScope`]; keys are not unique across scopes.
pub async fn get_job_payload_by_idempotency_key_with_scope(
    pool: &DbPool,
    scope: JobScope,
    job_type: JobType<'_>,
    idempotency_key: &str,
) -> Result<Option<(Uuid, serde_json::Value)>> {
    let row = super::super::scoped_read::scoped_lookup!(
        JobPayloadRow,
        pool,
        scope,
        "SELECT id, payload FROM job_queue WHERE",
        "AND job_type = $2
           AND idempotency_key = $3
         LIMIT 1",
        job_type as _,
        idempotency_key,
    )
    .map_err(|error| {
        Error::from_query_sqlx_with_context("get job payload by idempotency key", error)
    })?;

    Ok(row.map(|row| (row.id, row.payload)))
}

/// Tenant-only compatibility wrapper for [`get_latest_job_payload_for_run_with_scope`].
pub async fn get_latest_job_payload_for_run(
    pool: &DbPool,
    organization_id: Uuid,
    job_type: JobType<'_>,
    run_id: Uuid,
) -> Result<Option<(Uuid, serde_json::Value)>> {
    get_latest_job_payload_for_run_with_scope(
        pool,
        JobScope::Organization(organization_id),
        job_type,
        run_id,
    )
    .await
}

/// Looks up a payload in one exact global or tenant scope, returning `None` if absent.
/// Applications must authorize the selected [`JobScope`]; keys are not unique across scopes.
/// Selects the newest `created_at`, breaking timestamp ties by descending job ID.
pub async fn get_latest_job_payload_for_run_with_scope(
    pool: &DbPool,
    scope: JobScope,
    job_type: JobType<'_>,
    run_id: Uuid,
) -> Result<Option<(Uuid, serde_json::Value)>> {
    let run_id_text = run_id.to_string();
    let row = super::super::scoped_read::scoped_lookup!(
        JobPayloadRow,
        pool,
        scope,
        "SELECT id, payload FROM job_queue WHERE",
        "AND job_type = $2
           AND payload->>'run_id' = $3
         ORDER BY created_at DESC, id DESC
         LIMIT 1",
        job_type as _,
        run_id_text,
    )
    .map_err(|error| {
        Error::from_query_sqlx_with_context("get latest job payload for run", error)
    })?;

    Ok(row.map(|row| (row.id, row.payload)))
}

/// Legacy read: None matches global and organization-owned jobs.
/// Prefer [`list_job_events_with_scope`] for new code.
pub async fn list_job_events(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_id: Uuid,
    limit: i64,
    after_id: Option<i64>,
) -> Result<Vec<JobEventRecord>> {
    list_job_events_with_scope(
        pool,
        JobReadScope::from_legacy(organization_id),
        job_id,
        limit,
        after_id,
    )
    .await
}

/// Reads within an application-authorized, explicit job visibility scope.
pub async fn list_job_events_with_scope(
    pool: &DbPool,
    scope: JobReadScope,
    job_id: Uuid,
    limit: i64,
    after_id: Option<i64>,
) -> Result<Vec<JobEventRecord>> {
    let (is_admin, organization_id) = scope.visibility_predicate();
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
           AND ($5::bool OR jq.organization_id IS NOT DISTINCT FROM $2::uuid)
           AND ($3::bigint IS NULL OR je.id > $3)
         ORDER BY je.id ASC
         LIMIT $4",
        job_id,
        organization_id,
        after_id,
        limit,
        is_admin,
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
