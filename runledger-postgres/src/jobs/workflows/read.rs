use runledger_core::jobs::WorkflowType;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Result};

use super::super::errors::validate_pagination;
use super::super::row_decode::parse_workflow_release_mode;
use super::super::rows::{WorkflowRunRow, WorkflowStepRow};
use super::super::workflow_types::{
    WorkflowRunCountFilter, WorkflowRunDbRecord, WorkflowRunListFilter, WorkflowRunReadCountFilter,
    WorkflowRunReadListFilter, WorkflowRunReadScope, WorkflowStepDbRecord,
    WorkflowStepDependencyDbRecord,
};

#[derive(sqlx::FromRow)]
struct WorkflowStepDependencyLookupRow {
    workflow_run_id: Uuid,
    prerequisite_step_id: Uuid,
    dependent_step_id: Uuid,
    release_mode: String,
    created_at: chrono::DateTime<chrono::Utc>,
}

fn workflow_step_dependency_db_record_from_lookup_row(
    row: WorkflowStepDependencyLookupRow,
) -> Result<WorkflowStepDependencyDbRecord> {
    Ok(WorkflowStepDependencyDbRecord {
        workflow_run_id: row.workflow_run_id,
        prerequisite_step_id: row.prerequisite_step_id,
        dependent_step_id: row.dependent_step_id,
        release_mode: parse_workflow_release_mode(row.release_mode)?,
        created_at: row.created_at,
    })
}

const fn legacy_workflow_read_scope(organization_id: Option<Uuid>) -> WorkflowRunReadScope {
    match organization_id {
        Some(organization_id) => WorkflowRunReadScope::Organization(organization_id),
        None => WorkflowRunReadScope::Admin,
    }
}

/// Loads a workflow run using the legacy nullable visibility scope.
///
/// `None` retains its historical admin visibility across global and
/// organization-owned workflow runs. Prefer
/// [`get_workflow_run_by_id_with_scope`] for new code.
pub async fn get_workflow_run_by_id(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    workflow_run_id: Uuid,
) -> Result<Option<WorkflowRunDbRecord>> {
    get_workflow_run_by_id_with_scope(
        pool,
        legacy_workflow_read_scope(organization_id),
        workflow_run_id,
    )
    .await
}

/// Loads a workflow run within an explicit read-visibility scope.
pub async fn get_workflow_run_by_id_with_scope(
    pool: &DbPool,
    scope: WorkflowRunReadScope,
    workflow_run_id: Uuid,
) -> Result<Option<WorkflowRunDbRecord>> {
    let (is_admin, organization_id) = scope.visibility_predicate();
    let row = sqlx::query_as!(
        WorkflowRunRow,
        "SELECT
            id,
            workflow_type,
            organization_id,
            status::text AS \"status!\",
            idempotency_key,
            result_step_key,
            metadata,
            started_at,
            finished_at,
            created_at,
            updated_at
         FROM workflow_runs
         WHERE id = $1
           AND ($2::bool OR organization_id IS NOT DISTINCT FROM $3::uuid)
         LIMIT 1",
        workflow_run_id,
        is_admin,
        organization_id,
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| crate::Error::from_query_sqlx_with_context("get workflow run by id", error))?;

    row.map(WorkflowRunRow::into_record).transpose()
}

pub(in crate::jobs::workflows) async fn load_workflow_run_by_id_tx(
    tx: &mut DbTx<'_>,
    workflow_run_id: Uuid,
    context: &'static str,
) -> Result<WorkflowRunDbRecord> {
    let run_row = sqlx::query_as!(
        WorkflowRunRow,
        "SELECT
            id,
            workflow_type,
            organization_id,
            status::text AS \"status!\",
            idempotency_key,
            result_step_key,
            metadata,
            started_at,
            finished_at,
            created_at,
            updated_at
         FROM workflow_runs
         WHERE id = $1",
        workflow_run_id,
    )
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| crate::Error::from_query_sqlx_with_context(context, error))?;

    run_row.into_record()
}

/// Lists workflow steps using the legacy nullable visibility scope.
///
/// `None` retains its historical admin visibility across global and
/// organization-owned workflow runs. Prefer [`list_workflow_steps_with_scope`]
/// for new code.
pub async fn list_workflow_steps(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    workflow_run_id: Uuid,
) -> Result<Vec<WorkflowStepDbRecord>> {
    list_workflow_steps_with_scope(
        pool,
        legacy_workflow_read_scope(organization_id),
        workflow_run_id,
    )
    .await
}

/// Lists workflow steps within an explicit read-visibility scope.
pub async fn list_workflow_steps_with_scope(
    pool: &DbPool,
    scope: WorkflowRunReadScope,
    workflow_run_id: Uuid,
) -> Result<Vec<WorkflowStepDbRecord>> {
    let (is_admin, organization_id) = scope.visibility_predicate();
    let rows = sqlx::query_as::<_, WorkflowStepRow>(
        "SELECT
            ws.id,
            ws.workflow_run_id,
            ws.step_key,
            ws.execution_kind::text AS execution_kind,
            ws.job_type,
            ws.organization_id,
            ws.payload,
            ws.priority,
            ws.max_attempts,
            ws.timeout_seconds,
            ws.stage,
            ws.allow_handler_continuation,
            ws.execution_resource_key,
            ws.status::text AS status,
            ws.job_id,
            ws.released_at,
            ws.started_at,
            ws.finished_at,
            ws.dependency_count_total,
            ws.dependency_count_pending,
            ws.dependency_count_unsatisfied,
            ws.status_reason,
            ws.last_error_code,
            ws.last_error_message,
            ws.output,
            ws.created_at,
            ws.updated_at
         FROM workflow_steps ws
         JOIN workflow_runs wr ON wr.id = ws.workflow_run_id
         WHERE ws.workflow_run_id = $1
           AND ($2::bool OR wr.organization_id IS NOT DISTINCT FROM $3::uuid)
         ORDER BY ws.created_at ASC, ws.id ASC",
    )
    .bind(workflow_run_id)
    .bind(is_admin)
    .bind(organization_id)
    .fetch_all(pool)
    .await
    .map_err(|error| crate::Error::from_query_sqlx_with_context("list workflow steps", error))?;

    rows.into_iter().map(WorkflowStepRow::into_record).collect()
}

/// Lists a page of workflow steps using the legacy nullable visibility scope.
///
/// `None` retains its historical admin visibility across global and
/// organization-owned workflow runs. Prefer
/// [`list_workflow_steps_page_with_scope`] for new code.
pub async fn list_workflow_steps_page(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    workflow_run_id: Uuid,
    limit: i64,
    offset: i64,
) -> Result<Vec<WorkflowStepDbRecord>> {
    list_workflow_steps_page_with_scope(
        pool,
        legacy_workflow_read_scope(organization_id),
        workflow_run_id,
        limit,
        offset,
    )
    .await
}

/// Lists a page of workflow steps within an explicit read-visibility scope.
pub async fn list_workflow_steps_page_with_scope(
    pool: &DbPool,
    scope: WorkflowRunReadScope,
    workflow_run_id: Uuid,
    limit: i64,
    offset: i64,
) -> Result<Vec<WorkflowStepDbRecord>> {
    validate_pagination(limit, offset)?;

    let (is_admin, organization_id) = scope.visibility_predicate();
    let rows = sqlx::query_as::<_, WorkflowStepRow>(
        "SELECT
            ws.id,
            ws.workflow_run_id,
            ws.step_key,
            ws.execution_kind::text AS execution_kind,
            ws.job_type,
            ws.organization_id,
            ws.payload,
            ws.priority,
            ws.max_attempts,
            ws.timeout_seconds,
            ws.stage,
            ws.allow_handler_continuation,
            ws.execution_resource_key,
            ws.status::text AS status,
            ws.job_id,
            ws.released_at,
            ws.started_at,
            ws.finished_at,
            ws.dependency_count_total,
            ws.dependency_count_pending,
            ws.dependency_count_unsatisfied,
            ws.status_reason,
            ws.last_error_code,
            ws.last_error_message,
            ws.output,
            ws.created_at,
            ws.updated_at
         FROM workflow_steps ws
         JOIN workflow_runs wr ON wr.id = ws.workflow_run_id
         WHERE ws.workflow_run_id = $1
           AND ($2::bool OR wr.organization_id IS NOT DISTINCT FROM $3::uuid)
         ORDER BY ws.created_at ASC, ws.id ASC
         LIMIT $4 OFFSET $5",
    )
    .bind(workflow_run_id)
    .bind(is_admin)
    .bind(organization_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await
    .map_err(|error| {
        crate::Error::from_query_sqlx_with_context("list workflow steps page", error)
    })?;

    rows.into_iter().map(WorkflowStepRow::into_record).collect()
}

/// Counts workflow steps using the legacy nullable visibility scope.
///
/// `None` retains its historical admin visibility across global and
/// organization-owned workflow runs. Prefer [`count_workflow_steps_with_scope`]
/// for new code.
pub async fn count_workflow_steps(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    workflow_run_id: Uuid,
) -> Result<i64> {
    count_workflow_steps_with_scope(
        pool,
        legacy_workflow_read_scope(organization_id),
        workflow_run_id,
    )
    .await
}

/// Counts workflow steps within an explicit read-visibility scope.
pub async fn count_workflow_steps_with_scope(
    pool: &DbPool,
    scope: WorkflowRunReadScope,
    workflow_run_id: Uuid,
) -> Result<i64> {
    let (is_admin, organization_id) = scope.visibility_predicate();
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint
         FROM workflow_steps ws
         JOIN workflow_runs wr ON wr.id = ws.workflow_run_id
         WHERE ws.workflow_run_id = $1
           AND ($2::bool OR wr.organization_id IS NOT DISTINCT FROM $3::uuid)",
    )
    .bind(workflow_run_id)
    .bind(is_admin)
    .bind(organization_id)
    .fetch_one(pool)
    .await
    .map_err(|error| crate::Error::from_query_sqlx_with_context("count workflow steps", error))
}

/// Lists workflow runs using the legacy nullable visibility filter.
///
/// `filter.organization_id = None` retains its historical admin visibility
/// across global and organization-owned workflow runs. Prefer
/// [`list_workflow_runs_with_scope`] with [`WorkflowRunReadListFilter`] for
/// new code.
pub async fn list_workflow_runs(
    pool: &DbPool,
    filter: &WorkflowRunListFilter<'_>,
) -> Result<Vec<WorkflowRunDbRecord>> {
    let scoped_filter = WorkflowRunReadListFilter {
        scope: legacy_workflow_read_scope(filter.organization_id),
        status: filter.status,
        workflow_type: filter.workflow_type,
        limit: filter.limit,
        offset: filter.offset,
    };
    list_workflow_runs_with_scope(pool, &scoped_filter).await
}

/// Lists workflow runs within an explicit read-visibility scope.
pub async fn list_workflow_runs_with_scope(
    pool: &DbPool,
    filter: &WorkflowRunReadListFilter<'_>,
) -> Result<Vec<WorkflowRunDbRecord>> {
    validate_pagination(filter.limit, filter.offset)?;

    let (is_admin, organization_id) = filter.scope.visibility_predicate();
    let status_text = filter.status.map(|status| status.as_db_value());

    let rows = sqlx::query_as::<_, WorkflowRunRow>(
        "SELECT
            id,
            workflow_type,
            organization_id,
            status::text AS status,
            idempotency_key,
            result_step_key,
            metadata,
            started_at,
            finished_at,
            created_at,
            updated_at
         FROM workflow_runs
         WHERE ($1::bool OR organization_id IS NOT DISTINCT FROM $2::uuid)
           AND ($3::text IS NULL OR status = $3::text::workflow_run_status)
           AND ($4::text IS NULL OR workflow_type ILIKE '%' || $4 || '%')
         ORDER BY created_at DESC, id DESC
         LIMIT $5 OFFSET $6",
    )
    .bind(is_admin)
    .bind(organization_id)
    .bind(status_text)
    .bind(filter.workflow_type)
    .bind(filter.limit)
    .bind(filter.offset)
    .fetch_all(pool)
    .await
    .map_err(|error| crate::Error::from_query_sqlx_with_context("list workflow runs", error))?;

    rows.into_iter().map(WorkflowRunRow::into_record).collect()
}

/// Counts workflow runs using the legacy nullable visibility filter.
///
/// `filter.organization_id = None` retains its historical admin visibility
/// across global and organization-owned workflow runs. Prefer
/// [`count_workflow_runs_with_scope`] with [`WorkflowRunReadCountFilter`] for
/// new code.
pub async fn count_workflow_runs(
    pool: &DbPool,
    filter: &WorkflowRunCountFilter<'_>,
) -> Result<i64> {
    let scoped_filter = WorkflowRunReadCountFilter {
        scope: legacy_workflow_read_scope(filter.organization_id),
        status: filter.status,
        workflow_type: filter.workflow_type,
    };
    count_workflow_runs_with_scope(pool, &scoped_filter).await
}

/// Counts workflow runs within an explicit read-visibility scope.
pub async fn count_workflow_runs_with_scope(
    pool: &DbPool,
    filter: &WorkflowRunReadCountFilter<'_>,
) -> Result<i64> {
    let (is_admin, organization_id) = filter.scope.visibility_predicate();
    let status_text = filter.status.map(|status| status.as_db_value());
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint
         FROM workflow_runs
         WHERE ($1::bool OR organization_id IS NOT DISTINCT FROM $2::uuid)
           AND ($3::text IS NULL OR status = $3::text::workflow_run_status)
           AND ($4::text IS NULL OR workflow_type ILIKE '%' || $4 || '%')",
    )
    .bind(is_admin)
    .bind(organization_id)
    .bind(status_text)
    .bind(filter.workflow_type)
    .fetch_one(pool)
    .await
    .map_err(|error| crate::Error::from_query_sqlx_with_context("count workflow runs", error))
}

/// Loads the latest workflow run using the legacy nullable visibility scope.
///
/// `None` retains its historical admin visibility across global and
/// organization-owned workflow runs. Prefer
/// [`get_latest_workflow_run_by_type_with_scope`] for new code.
pub async fn get_latest_workflow_run_by_type(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    workflow_type: WorkflowType<'_>,
) -> Result<Option<WorkflowRunDbRecord>> {
    get_latest_workflow_run_by_type_with_scope(
        pool,
        legacy_workflow_read_scope(organization_id),
        workflow_type,
    )
    .await
}

/// Loads the latest workflow run within an explicit read-visibility scope.
pub async fn get_latest_workflow_run_by_type_with_scope(
    pool: &DbPool,
    scope: WorkflowRunReadScope,
    workflow_type: WorkflowType<'_>,
) -> Result<Option<WorkflowRunDbRecord>> {
    let (is_admin, organization_id) = scope.visibility_predicate();
    let row = sqlx::query_as::<_, WorkflowRunRow>(
        "SELECT
            id,
            workflow_type,
            organization_id,
            status::text AS status,
            idempotency_key,
            result_step_key,
            metadata,
            started_at,
            finished_at,
            created_at,
            updated_at
         FROM workflow_runs
         WHERE ($1::bool OR organization_id IS NOT DISTINCT FROM $2::uuid)
           AND workflow_type = $3
         ORDER BY created_at DESC, id DESC
         LIMIT 1",
    )
    .bind(is_admin)
    .bind(organization_id)
    .bind(workflow_type.as_str())
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        crate::Error::from_query_sqlx_with_context("get latest workflow run by type", error)
    })?;

    let Some(row) = row else {
        return Ok(None);
    };

    Ok(Some(row.into_record()?))
}

/// Lists workflow step dependencies using the legacy nullable visibility scope.
///
/// `None` retains its historical admin visibility across global and
/// organization-owned workflow runs. Prefer
/// [`list_workflow_step_dependencies_with_scope`] for new code.
pub async fn list_workflow_step_dependencies(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    workflow_run_id: Uuid,
) -> Result<Vec<WorkflowStepDependencyDbRecord>> {
    list_workflow_step_dependencies_with_scope(
        pool,
        legacy_workflow_read_scope(organization_id),
        workflow_run_id,
    )
    .await
}

/// Lists workflow step dependencies within an explicit read-visibility scope.
pub async fn list_workflow_step_dependencies_with_scope(
    pool: &DbPool,
    scope: WorkflowRunReadScope,
    workflow_run_id: Uuid,
) -> Result<Vec<WorkflowStepDependencyDbRecord>> {
    let (is_admin, organization_id) = scope.visibility_predicate();
    let rows = sqlx::query_as::<_, WorkflowStepDependencyLookupRow>(
        "SELECT
            wsd.workflow_run_id,
            wsd.prerequisite_step_id,
            wsd.dependent_step_id,
            wsd.release_mode::text AS release_mode,
            wsd.created_at
         FROM workflow_step_dependencies wsd
         JOIN workflow_runs wr ON wr.id = wsd.workflow_run_id
         WHERE wsd.workflow_run_id = $1
           AND ($2::bool OR wr.organization_id IS NOT DISTINCT FROM $3::uuid)
         ORDER BY
           wsd.prerequisite_step_id ASC,
           wsd.dependent_step_id ASC",
    )
    .bind(workflow_run_id)
    .bind(is_admin)
    .bind(organization_id)
    .fetch_all(pool)
    .await
    .map_err(|error| {
        crate::Error::from_query_sqlx_with_context("list workflow step dependencies", error)
    })?;

    rows.into_iter()
        .map(workflow_step_dependency_db_record_from_lookup_row)
        .collect()
}

/// Lists a page of workflow step dependencies using the legacy nullable
/// visibility scope.
///
/// `None` retains its historical admin visibility across global and
/// organization-owned workflow runs. Prefer
/// [`list_workflow_step_dependencies_page_with_scope`] for new code.
pub async fn list_workflow_step_dependencies_page(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    workflow_run_id: Uuid,
    limit: i64,
    offset: i64,
) -> Result<Vec<WorkflowStepDependencyDbRecord>> {
    list_workflow_step_dependencies_page_with_scope(
        pool,
        legacy_workflow_read_scope(organization_id),
        workflow_run_id,
        limit,
        offset,
    )
    .await
}

/// Lists a page of workflow step dependencies within an explicit
/// read-visibility scope.
pub async fn list_workflow_step_dependencies_page_with_scope(
    pool: &DbPool,
    scope: WorkflowRunReadScope,
    workflow_run_id: Uuid,
    limit: i64,
    offset: i64,
) -> Result<Vec<WorkflowStepDependencyDbRecord>> {
    validate_pagination(limit, offset)?;

    let (is_admin, organization_id) = scope.visibility_predicate();
    let rows = sqlx::query_as::<_, WorkflowStepDependencyLookupRow>(
        "SELECT
            wsd.workflow_run_id,
            wsd.prerequisite_step_id,
            wsd.dependent_step_id,
            wsd.release_mode::text AS release_mode,
            wsd.created_at
         FROM workflow_step_dependencies wsd
         JOIN workflow_runs wr ON wr.id = wsd.workflow_run_id
         WHERE wsd.workflow_run_id = $1
           AND ($2::bool OR wr.organization_id IS NOT DISTINCT FROM $3::uuid)
         ORDER BY
           wsd.prerequisite_step_id ASC,
           wsd.dependent_step_id ASC
         LIMIT $4 OFFSET $5",
    )
    .bind(workflow_run_id)
    .bind(is_admin)
    .bind(organization_id)
    .bind(limit)
    .bind(offset)
    .fetch_all(pool)
    .await
    .map_err(|error| {
        crate::Error::from_query_sqlx_with_context("list workflow step dependencies page", error)
    })?;

    rows.into_iter()
        .map(workflow_step_dependency_db_record_from_lookup_row)
        .collect()
}

/// Counts workflow step dependencies using the legacy nullable visibility
/// scope.
///
/// `None` retains its historical admin visibility across global and
/// organization-owned workflow runs. Prefer
/// [`count_workflow_step_dependencies_with_scope`] for new code.
pub async fn count_workflow_step_dependencies(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    workflow_run_id: Uuid,
) -> Result<i64> {
    count_workflow_step_dependencies_with_scope(
        pool,
        legacy_workflow_read_scope(organization_id),
        workflow_run_id,
    )
    .await
}

/// Counts workflow step dependencies within an explicit read-visibility scope.
pub async fn count_workflow_step_dependencies_with_scope(
    pool: &DbPool,
    scope: WorkflowRunReadScope,
    workflow_run_id: Uuid,
) -> Result<i64> {
    let (is_admin, organization_id) = scope.visibility_predicate();
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint
         FROM workflow_step_dependencies wsd
         JOIN workflow_runs wr ON wr.id = wsd.workflow_run_id
         WHERE wsd.workflow_run_id = $1
           AND ($2::bool OR wr.organization_id IS NOT DISTINCT FROM $3::uuid)",
    )
    .bind(workflow_run_id)
    .bind(is_admin)
    .bind(organization_id)
    .fetch_one(pool)
    .await
    .map_err(|error| {
        crate::Error::from_query_sqlx_with_context("count workflow step dependencies", error)
    })
}

pub async fn get_workflow_run_id_for_job(pool: &DbPool, job_id: Uuid) -> Result<Option<Uuid>> {
    sqlx::query_scalar!(
        "SELECT ws.workflow_run_id FROM workflow_steps ws WHERE ws.job_id = $1",
        job_id,
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        crate::Error::from_query_sqlx_with_context("get workflow run id for job", error)
    })
}

pub async fn get_workflow_run_by_type_and_idempotency_key(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    workflow_type: WorkflowType<'_>,
    idempotency_key: &str,
) -> Result<Option<WorkflowRunDbRecord>> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|error| crate::Error::ConnectionError(error.to_string()))?;
    let run = get_workflow_run_by_type_and_idempotency_key_tx(
        &mut tx,
        organization_id,
        workflow_type,
        idempotency_key,
    )
    .await?;
    tx.commit()
        .await
        .map_err(|error| crate::Error::ConnectionError(error.to_string()))?;
    Ok(run)
}

pub async fn get_workflow_run_by_type_and_idempotency_key_tx(
    tx: &mut DbTx<'_>,
    organization_id: Option<Uuid>,
    workflow_type: WorkflowType<'_>,
    idempotency_key: &str,
) -> Result<Option<WorkflowRunDbRecord>> {
    let row = if let Some(organization_id) = organization_id {
        sqlx::query_as!(
            WorkflowRunRow,
            "SELECT
                id,
                workflow_type,
                organization_id,
                status::text AS \"status!\",
                idempotency_key,
                result_step_key,
                metadata,
                started_at,
                finished_at,
                created_at,
                updated_at
             FROM workflow_runs
             WHERE workflow_type = $1
               AND idempotency_key = $2
               AND organization_id = $3
             LIMIT 1",
            workflow_type as _,
            idempotency_key,
            organization_id,
        )
        .fetch_optional(&mut **tx)
        .await
    } else {
        sqlx::query_as!(
            WorkflowRunRow,
            "SELECT
                id,
                workflow_type,
                organization_id,
                status::text AS \"status!\",
                idempotency_key,
                result_step_key,
                metadata,
                started_at,
                finished_at,
                created_at,
                updated_at
             FROM workflow_runs
             WHERE workflow_type = $1
               AND idempotency_key = $2
               AND organization_id IS NULL
             LIMIT 1",
            workflow_type as _,
            idempotency_key,
        )
        .fetch_optional(&mut **tx)
        .await
    }
    .map_err(|error| {
        crate::Error::from_query_sqlx_with_context(
            "get workflow run by type and idempotency key",
            error,
        )
    })?;

    row.map(WorkflowRunRow::into_record).transpose()
}
