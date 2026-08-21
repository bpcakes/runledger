use runledger_core::jobs::{JobStatus, JobType};
use sqlx::types::Uuid;

use crate::{DbPool, Error, Result};

use super::super::errors::{escape_ilike_pattern, validate_page_limit, validate_pagination};
use super::super::row_decode::{
    parse_job_event_type, parse_job_stage, parse_job_status, parse_job_type_name,
    parse_step_key_name, parse_workflow_release_mode, parse_workflow_run_status,
    parse_workflow_step_execution_kind, parse_workflow_step_status, parse_workflow_type_name,
};
use super::super::rows::JobQueueRow;
use super::super::types::{
    AdminJobSummaryFilter, AdminJobSummaryRecord, AdminWorkflowDependencyRecord,
    AdminWorkflowStepRecord, AdminWorkflowSummaryFilter, AdminWorkflowSummaryRecord,
    JobEventRecord, JobListFilter, JobQueueRecord,
};

struct AdminJobSummaryRow {
    id: Uuid,
    job_type: String,
    organization_id: Option<Uuid>,
    status: String,
    priority: i32,
    run_number: i32,
    attempt: i32,
    max_attempts: i32,
    timeout_seconds: i32,
    next_run_at: chrono::DateTime<chrono::Utc>,
    lease_expires_at: Option<chrono::DateTime<chrono::Utc>>,
    last_heartbeat_at: Option<chrono::DateTime<chrono::Utc>>,
    started_at: Option<chrono::DateTime<chrono::Utc>>,
    finished_at: Option<chrono::DateTime<chrono::Utc>>,
    stage: String,
    progress_done: Option<i64>,
    progress_total: Option<i64>,
    progress_pct: Option<f64>,
    last_error_code: Option<String>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

struct AdminWorkflowSummaryRow {
    id: Uuid,
    workflow_type: String,
    organization_id: Option<Uuid>,
    status: String,
    result_step_key: Option<String>,
    started_at: chrono::DateTime<chrono::Utc>,
    finished_at: Option<chrono::DateTime<chrono::Utc>>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

struct AdminWorkflowStepRow {
    id: Uuid,
    workflow_run_id: Uuid,
    step_key: String,
    execution_kind: String,
    job_type: Option<String>,
    organization_id: Option<Uuid>,
    payload: serde_json::Value,
    priority: Option<i32>,
    max_attempts: Option<i32>,
    timeout_seconds: Option<i32>,
    stage: Option<String>,
    allow_handler_continuation: bool,
    execution_resource_key: Option<String>,
    status: String,
    job_id: Option<Uuid>,
    released_at: Option<chrono::DateTime<chrono::Utc>>,
    started_at: Option<chrono::DateTime<chrono::Utc>>,
    finished_at: Option<chrono::DateTime<chrono::Utc>>,
    visible_dependency_count_total: i32,
    visible_dependency_count_pending: i32,
    visible_dependency_count_unsatisfied: i32,
    has_hidden_prerequisites: bool,
    status_reason: Option<String>,
    last_error_code: Option<String>,
    last_error_message: Option<String>,
    output: Option<serde_json::Value>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

#[derive(sqlx::FromRow)]
struct AdminWorkflowDependencyRow {
    workflow_run_id: Uuid,
    prerequisite_step_id: Uuid,
    dependent_step_id: Uuid,
    release_mode: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

pub async fn list_admin_job_summaries(
    pool: &DbPool,
    filter: &AdminJobSummaryFilter<'_>,
) -> Result<Vec<AdminJobSummaryRecord>> {
    validate_pagination(filter.limit, filter.offset)?;

    let status_filter = filter.status.map(JobStatus::as_db_value);
    let job_type_pattern = filter.job_type_contains.map(escape_ilike_pattern);
    let rows = match filter.organization_id {
        Some(organization_id) => {
            sqlx::query_file_as!(
                AdminJobSummaryRow,
                "src/jobs/admin/queries/list_job_summaries_for_organization.sql",
                organization_id,
                status_filter,
                job_type_pattern.as_deref(),
                filter.limit,
                filter.offset,
            )
            .fetch_all(pool)
            .await
        }
        None => {
            sqlx::query_file_as!(
                AdminJobSummaryRow,
                "src/jobs/admin/queries/list_job_summaries_global.sql",
                status_filter,
                job_type_pattern.as_deref(),
                filter.limit,
                filter.offset,
            )
            .fetch_all(pool)
            .await
        }
    }
    .map_err(|error| Error::from_query_sqlx_with_context("list admin job summaries", error))?;

    rows.into_iter().map(admin_job_summary_record).collect()
}

fn admin_job_summary_record(row: AdminJobSummaryRow) -> Result<AdminJobSummaryRecord> {
    Ok(AdminJobSummaryRecord {
        id: row.id,
        job_type: parse_job_type_name(row.job_type)?,
        organization_id: row.organization_id,
        status: parse_job_status(row.status)?,
        priority: row.priority,
        run_number: row.run_number,
        attempt: row.attempt,
        max_attempts: row.max_attempts,
        timeout_seconds: row.timeout_seconds,
        next_run_at: row.next_run_at,
        lease_expires_at: row.lease_expires_at,
        last_heartbeat_at: row.last_heartbeat_at,
        started_at: row.started_at,
        finished_at: row.finished_at,
        stage: parse_job_stage(row.stage)?,
        progress_done: row.progress_done,
        progress_total: row.progress_total,
        progress_pct: row.progress_pct,
        last_error_code: row.last_error_code,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

pub async fn list_admin_workflow_summaries(
    pool: &DbPool,
    filter: &AdminWorkflowSummaryFilter<'_>,
) -> Result<Vec<AdminWorkflowSummaryRecord>> {
    validate_pagination(filter.limit, filter.offset)?;

    let status_filter = filter.status.map(|status| status.as_db_value());
    let workflow_type_pattern = filter.workflow_type_contains.map(escape_ilike_pattern);
    let rows = match filter.organization_id {
        Some(organization_id) => {
            sqlx::query_file_as!(
                AdminWorkflowSummaryRow,
                "src/jobs/admin/queries/list_workflow_summaries_for_organization.sql",
                organization_id,
                status_filter,
                workflow_type_pattern.as_deref(),
                filter.limit,
                filter.offset,
            )
            .fetch_all(pool)
            .await
        }
        None => {
            sqlx::query_file_as!(
                AdminWorkflowSummaryRow,
                "src/jobs/admin/queries/list_workflow_summaries_global.sql",
                status_filter,
                workflow_type_pattern.as_deref(),
                filter.limit,
                filter.offset,
            )
            .fetch_all(pool)
            .await
        }
    }
    .map_err(|error| Error::from_query_sqlx_with_context("list admin workflow summaries", error))?;

    rows.into_iter()
        .map(admin_workflow_summary_record)
        .collect()
}

fn admin_workflow_summary_record(
    row: AdminWorkflowSummaryRow,
) -> Result<AdminWorkflowSummaryRecord> {
    Ok(AdminWorkflowSummaryRecord {
        id: row.id,
        workflow_type: parse_workflow_type_name(row.workflow_type)?,
        organization_id: row.organization_id,
        status: parse_workflow_run_status(row.status)?,
        result_step_key: row.result_step_key.map(parse_step_key_name).transpose()?,
        started_at: row.started_at,
        finished_at: row.finished_at,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

/// Lists workflow steps in an admin scope without changing the meaning of the
/// durable workflow-step record's dependency counters.
pub async fn list_admin_workflow_steps(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    workflow_run_id: Uuid,
    limit: i64,
    offset: i64,
) -> Result<Vec<AdminWorkflowStepRecord>> {
    validate_pagination(limit, offset)?;

    let rows = match organization_id {
        Some(organization_id) => {
            sqlx::query_file_as!(
                AdminWorkflowStepRow,
                "src/jobs/admin/queries/list_workflow_steps_for_organization.sql",
                workflow_run_id,
                organization_id,
                limit,
                offset,
            )
            .fetch_all(pool)
            .await
        }
        None => {
            sqlx::query_file_as!(
                AdminWorkflowStepRow,
                "src/jobs/admin/queries/list_workflow_steps_global.sql",
                workflow_run_id,
                limit,
                offset,
            )
            .fetch_all(pool)
            .await
        }
    }
    .map_err(|error| Error::from_query_sqlx_with_context("list admin workflow steps", error))?;

    rows.into_iter().map(admin_workflow_step_record).collect()
}

fn admin_workflow_step_record(row: AdminWorkflowStepRow) -> Result<AdminWorkflowStepRecord> {
    Ok(AdminWorkflowStepRecord {
        id: row.id,
        workflow_run_id: row.workflow_run_id,
        step_key: parse_step_key_name(row.step_key)?,
        execution_kind: parse_workflow_step_execution_kind(row.execution_kind)?,
        job_type: row.job_type.map(parse_job_type_name).transpose()?,
        organization_id: row.organization_id,
        payload: row.payload,
        priority: row.priority,
        max_attempts: row.max_attempts,
        timeout_seconds: row.timeout_seconds,
        stage: row.stage.map(parse_job_stage).transpose()?,
        allow_handler_continuation: row.allow_handler_continuation,
        execution_resource_key: row.execution_resource_key,
        status: parse_workflow_step_status(row.status)?,
        job_id: row.job_id,
        released_at: row.released_at,
        started_at: row.started_at,
        finished_at: row.finished_at,
        visible_dependency_count_total: row.visible_dependency_count_total,
        visible_dependency_count_pending: row.visible_dependency_count_pending,
        visible_dependency_count_unsatisfied: row.visible_dependency_count_unsatisfied,
        has_hidden_prerequisites: row.has_hidden_prerequisites,
        status_reason: row.status_reason,
        last_error_code: row.last_error_code,
        last_error_message: row.last_error_message,
        output: row.output,
        created_at: row.created_at,
        updated_at: row.updated_at,
    })
}

/// Lists only dependency edges whose two steps are visible in the admin scope.
pub async fn list_admin_workflow_dependencies(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    workflow_run_id: Uuid,
    limit: i64,
    offset: i64,
) -> Result<Vec<AdminWorkflowDependencyRecord>> {
    validate_pagination(limit, offset)?;

    let rows = sqlx::query_as!(
        AdminWorkflowDependencyRow,
        r#"SELECT
            dependency.workflow_run_id,
            dependency.prerequisite_step_id,
            dependency.dependent_step_id,
            dependency.release_mode::text AS "release_mode!",
            dependency.created_at
         FROM workflow_step_dependencies dependency
         JOIN workflow_runs workflow
           ON workflow.id = dependency.workflow_run_id
         JOIN workflow_steps prerequisite
           ON prerequisite.id = dependency.prerequisite_step_id
         JOIN workflow_steps dependent
           ON dependent.id = dependency.dependent_step_id
         WHERE dependency.workflow_run_id = $1
           AND (
             $2::uuid IS NULL
             OR (
               workflow.organization_id = $2
               AND prerequisite.organization_id = $2
               AND dependent.organization_id = $2
             )
           )
         ORDER BY
           dependency.prerequisite_step_id ASC,
           dependency.dependent_step_id ASC
         LIMIT $3 OFFSET $4"#,
        workflow_run_id,
        organization_id,
        limit,
        offset,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("list admin workflow dependencies", error)
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(AdminWorkflowDependencyRecord {
                workflow_run_id: row.workflow_run_id,
                prerequisite_step_id: row.prerequisite_step_id,
                dependent_step_id: row.dependent_step_id,
                release_mode: parse_workflow_release_mode(row.release_mode)?,
                created_at: row.created_at,
            })
        })
        .collect()
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

/// Reports whether a job is visible within the requested organization scope.
pub async fn job_exists_in_scope(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_id: Uuid,
) -> Result<bool> {
    sqlx::query_scalar!(
        "SELECT EXISTS (
            SELECT 1
            FROM job_queue
            WHERE id = $1
              AND ($2::uuid IS NULL OR organization_id = $2)
         ) AS \"exists!\"",
        job_id,
        organization_id,
    )
    .fetch_one(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("check job existence in scope", error))
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

/// Lists job events newest-first, optionally continuing before an event id.
pub async fn list_job_events_before(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_id: Uuid,
    limit: i64,
    before_id: Option<i64>,
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
           AND ($3::bigint IS NULL OR je.id < $3)
         ORDER BY je.id DESC
         LIMIT $4",
        job_id,
        organization_id,
        before_id,
        limit,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("list job events before id", error))?;

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
