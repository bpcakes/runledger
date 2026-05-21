use runledger_core::jobs::WorkflowType;
use sqlx::types::Uuid;

use crate::{DbPool, DbTx, Result};

use super::super::row_decode::{
    parse_job_stage, parse_job_type_name, parse_step_key_name, parse_workflow_release_mode,
    parse_workflow_run_status, parse_workflow_step_execution_kind, parse_workflow_step_status,
    parse_workflow_type_name,
};
use super::super::workflow_types::{
    WorkflowRunDbRecord, WorkflowRunListFilter, WorkflowStepDbRecord,
    WorkflowStepDependencyDbRecord,
};

struct WorkflowRunLookupRow {
    id: Uuid,
    workflow_type: String,
    organization_id: Option<Uuid>,
    status: String,
    idempotency_key: Option<String>,
    metadata: serde_json::Value,
    started_at: chrono::DateTime<chrono::Utc>,
    finished_at: Option<chrono::DateTime<chrono::Utc>>,
    created_at: chrono::DateTime<chrono::Utc>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

pub async fn get_workflow_run_by_id(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    workflow_run_id: Uuid,
) -> Result<Option<WorkflowRunDbRecord>> {
    let row = sqlx::query!(
        "SELECT
            id,
            workflow_type,
            organization_id,
            status::text AS \"status!\",
            idempotency_key,
            metadata,
            started_at,
            finished_at,
            created_at,
            updated_at
         FROM workflow_runs
         WHERE id = $1
           AND ($2::uuid IS NULL OR organization_id = $2)
         LIMIT 1",
        workflow_run_id,
        organization_id,
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| crate::Error::from_query_sqlx_with_context("get workflow run by id", error))?;

    row.map(|row| {
        Ok(WorkflowRunDbRecord {
            id: row.id,
            workflow_type: parse_workflow_type_name(row.workflow_type)?,
            organization_id: row.organization_id,
            status: parse_workflow_run_status(row.status)?,
            idempotency_key: row.idempotency_key,
            metadata: row.metadata,
            started_at: row.started_at,
            finished_at: row.finished_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    })
    .transpose()
}

pub async fn list_workflow_steps(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    workflow_run_id: Uuid,
) -> Result<Vec<WorkflowStepDbRecord>> {
    let rows = sqlx::query!(
        "SELECT
            ws.id,
            ws.workflow_run_id,
            ws.step_key,
            ws.execution_kind::text AS \"execution_kind!\",
            ws.job_type,
            ws.organization_id,
            ws.payload,
            ws.priority,
            ws.max_attempts,
            ws.timeout_seconds,
            ws.stage,
            ws.status::text AS \"status!\",
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
            ws.created_at,
            ws.updated_at
         FROM workflow_steps ws
         JOIN workflow_runs wr ON wr.id = ws.workflow_run_id
         WHERE ws.workflow_run_id = $1
           AND ($2::uuid IS NULL OR wr.organization_id = $2)
         ORDER BY ws.created_at ASC",
        workflow_run_id,
        organization_id,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| crate::Error::from_query_sqlx_with_context("list workflow steps", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(WorkflowStepDbRecord {
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
                status: parse_workflow_step_status(row.status)?,
                job_id: row.job_id,
                released_at: row.released_at,
                started_at: row.started_at,
                finished_at: row.finished_at,
                dependency_count_total: row.dependency_count_total,
                dependency_count_pending: row.dependency_count_pending,
                dependency_count_unsatisfied: row.dependency_count_unsatisfied,
                status_reason: row.status_reason,
                last_error_code: row.last_error_code,
                last_error_message: row.last_error_message,
                created_at: row.created_at,
                updated_at: row.updated_at,
            })
        })
        .collect()
}

pub async fn list_workflow_runs(
    pool: &DbPool,
    filter: &WorkflowRunListFilter<'_>,
) -> Result<Vec<WorkflowRunDbRecord>> {
    let status_text = filter.status.map(|status| status.as_db_value());

    let rows = sqlx::query!(
        "SELECT
            id,
            workflow_type,
            organization_id,
            status::text AS \"status!\",
            idempotency_key,
            metadata,
            started_at,
            finished_at,
            created_at,
            updated_at
         FROM workflow_runs
         WHERE ($1::uuid IS NULL OR organization_id = $1)
           AND ($2::text IS NULL OR status = $2::text::workflow_run_status)
           AND ($3::text IS NULL OR workflow_type ILIKE '%' || $3 || '%')
         ORDER BY created_at DESC
         LIMIT $4 OFFSET $5",
        filter.organization_id,
        status_text,
        filter.workflow_type,
        filter.limit,
        filter.offset,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| crate::Error::from_query_sqlx_with_context("list workflow runs", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(WorkflowRunDbRecord {
                id: row.id,
                workflow_type: parse_workflow_type_name(row.workflow_type)?,
                organization_id: row.organization_id,
                status: parse_workflow_run_status(row.status)?,
                idempotency_key: row.idempotency_key,
                metadata: row.metadata,
                started_at: row.started_at,
                finished_at: row.finished_at,
                created_at: row.created_at,
                updated_at: row.updated_at,
            })
        })
        .collect()
}

pub async fn get_latest_workflow_run_by_type(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    workflow_type: WorkflowType<'_>,
) -> Result<Option<WorkflowRunDbRecord>> {
    let row = sqlx::query!(
        "SELECT
            id,
            workflow_type,
            organization_id,
            status::text AS \"status!\",
            idempotency_key,
            metadata,
            started_at,
            finished_at,
            created_at,
            updated_at
         FROM workflow_runs
         WHERE ($1::uuid IS NULL OR organization_id = $1)
           AND workflow_type = $2
         ORDER BY created_at DESC
         LIMIT 1",
        organization_id,
        workflow_type as _,
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| {
        crate::Error::from_query_sqlx_with_context("get latest workflow run by type", error)
    })?;

    let Some(row) = row else {
        return Ok(None);
    };

    Ok(Some(WorkflowRunDbRecord {
        id: row.id,
        workflow_type: parse_workflow_type_name(row.workflow_type)?,
        organization_id: row.organization_id,
        status: parse_workflow_run_status(row.status)?,
        idempotency_key: row.idempotency_key,
        metadata: row.metadata,
        started_at: row.started_at,
        finished_at: row.finished_at,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }))
}

pub async fn list_workflow_step_dependencies(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    workflow_run_id: Uuid,
) -> Result<Vec<WorkflowStepDependencyDbRecord>> {
    let rows = sqlx::query!(
        "SELECT
            wsd.workflow_run_id,
            wsd.prerequisite_step_id,
            wsd.dependent_step_id,
            wsd.release_mode::text AS \"release_mode!\",
            wsd.created_at
         FROM workflow_step_dependencies wsd
         JOIN workflow_runs wr ON wr.id = wsd.workflow_run_id
         WHERE wsd.workflow_run_id = $1
           AND ($2::uuid IS NULL OR wr.organization_id = $2)
         ORDER BY
           wsd.prerequisite_step_id ASC,
           wsd.dependent_step_id ASC",
        workflow_run_id,
        organization_id,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| {
        crate::Error::from_query_sqlx_with_context("list workflow step dependencies", error)
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(WorkflowStepDependencyDbRecord {
                workflow_run_id: row.workflow_run_id,
                prerequisite_step_id: row.prerequisite_step_id,
                dependent_step_id: row.dependent_step_id,
                release_mode: parse_workflow_release_mode(row.release_mode)?,
                created_at: row.created_at,
            })
        })
        .collect()
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
            WorkflowRunLookupRow,
            "SELECT
                id,
                workflow_type,
                organization_id,
                status::text AS \"status!\",
                idempotency_key,
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
            WorkflowRunLookupRow,
            "SELECT
                id,
                workflow_type,
                organization_id,
                status::text AS \"status!\",
                idempotency_key,
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

    row.map(|row| {
        Ok(WorkflowRunDbRecord {
            id: row.id,
            workflow_type: parse_workflow_type_name(row.workflow_type)?,
            organization_id: row.organization_id,
            status: parse_workflow_run_status(row.status)?,
            idempotency_key: row.idempotency_key,
            metadata: row.metadata,
            started_at: row.started_at,
            finished_at: row.finished_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    })
    .transpose()
}
