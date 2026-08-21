use runledger_core::jobs::{JobStatus, WorkflowRunStatus};
use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    AdminJobSummaryFilter, AdminJobSummaryRecord, AdminWorkflowSummaryFilter,
    AdminWorkflowSummaryRecord, JobDefinitionListFilter, JobEventRecord, JobLogRecord,
    JobQueueRecord, WorkflowRunDbRecord, WorkflowStepDbRecord, WorkflowStepDependencyDbRecord,
    get_admin_job_metrics_page, get_job_by_id, get_workflow_run_by_id, job_exists_in_scope,
    list_admin_job_summaries, list_admin_workflow_summaries, list_job_definitions, list_job_events,
    list_job_events_before, list_job_logs, list_job_logs_before,
    list_workflow_step_dependencies_in_organization_page, list_workflow_steps_in_organization_page,
};
use uuid::Uuid;

use crate::dto::{
    DefinitionsQuery, DefinitionsResponse, HistoryOrder, HistoryPageDto, HistoryQuery,
    JobDefinitionDto, JobDto, JobEventDto, JobEventsResponse, JobLogDto, JobLogsResponse,
    JobMetricsDto, JobResponse, JobSummaryDto, JobsQuery, JobsResponse, MAX_PAGE_LIMIT,
    MAX_PAGE_OFFSET, MetricsQuery, MetricsResponse, PageDto, WorkflowDependencyDto, WorkflowDto,
    WorkflowQuery, WorkflowResponse, WorkflowStepDto, WorkflowSummaryDto, WorkflowsQuery,
    WorkflowsResponse,
};
use crate::{AdminAccess, AdminApiError, DataVisibility};

/// Read-only application service behind the HTTP router.
#[derive(Clone)]
pub struct AdminService {
    pool: DbPool,
}

impl AdminService {
    /// Creates an admin service over an existing Runledger PostgreSQL pool.
    #[must_use]
    pub const fn new(pool: DbPool) -> Self {
        Self { pool }
    }

    /// Loads job metrics in the authorized scope.
    pub async fn metrics(
        &self,
        access: AdminAccess,
        query: &MetricsQuery,
    ) -> Result<MetricsResponse, AdminApiError> {
        validate_page(query.limit, query.offset)?;
        validate_filter(query.job_type.as_deref())?;
        let rows = get_admin_job_metrics_page(
            &self.pool,
            access.scope().organization_id(),
            query.job_type.as_deref(),
            fetch_limit(query.limit),
            query.offset,
        )
        .await
        .map_err(|error| storage_error("load job metrics", error))?;
        let (rows, has_more) = take_page(rows, query.limit);
        let items = rows
            .into_iter()
            .map(|metric| JobMetricsDto {
                job_type: metric.job_type.as_str().to_owned(),
                pending_count: metric.pending_count,
                leased_count: metric.leased_count,
                stale_leases: metric.stale_leases,
                succeeded_24h: metric.succeeded_24h,
                retryable_24h: metric.retryable_24h,
                terminal_24h: metric.terminal_24h,
                panicked_24h: metric.panicked_24h,
                timeout_24h: metric.timeout_24h,
                dead_lettered_24h: metric.dead_lettered_24h,
                p50_duration_ms_24h: metric.p50_duration_ms_24h,
                p95_duration_ms_24h: metric.p95_duration_ms_24h,
                continued_24h: metric.continued_24h,
                active_continued_count: metric.active_continued_count,
                max_active_run_number: metric.max_active_run_number,
            })
            .collect();
        Ok(MetricsResponse {
            items,
            page: PageDto {
                limit: query.limit,
                offset: query.offset,
                max_offset: MAX_PAGE_OFFSET,
                has_more,
            },
        })
    }

    /// Lists jobs in the authorized scope.
    pub async fn jobs(
        &self,
        access: AdminAccess,
        query: &JobsQuery,
    ) -> Result<JobsResponse, AdminApiError> {
        validate_page(query.limit, query.offset)?;
        validate_filter(query.job_type.as_deref())?;
        let status = parse_job_status(query.status.as_deref())?;
        let rows = list_admin_job_summaries(
            &self.pool,
            &AdminJobSummaryFilter {
                organization_id: access.scope().organization_id(),
                status,
                job_type_contains: query.job_type.as_deref(),
                limit: fetch_limit(query.limit),
                offset: query.offset,
            },
        )
        .await
        .map_err(|error| storage_error("list jobs", error))?;
        let (rows, has_more) = take_page(rows, query.limit);
        Ok(JobsResponse {
            items: rows.into_iter().map(job_summary_dto).collect(),
            page: PageDto {
                limit: query.limit,
                offset: query.offset,
                max_offset: MAX_PAGE_OFFSET,
                has_more,
            },
        })
    }

    /// Loads one job in the authorized scope.
    pub async fn job(
        &self,
        access: AdminAccess,
        job_id: Uuid,
    ) -> Result<JobResponse, AdminApiError> {
        let row = self.find_job(access, job_id).await?;
        Ok(JobResponse {
            job: job_dto(row, access.visibility()),
        })
    }

    /// Lists events for one authorized job.
    pub async fn job_events(
        &self,
        access: AdminAccess,
        job_id: Uuid,
        query: &HistoryQuery,
    ) -> Result<JobEventsResponse, AdminApiError> {
        let cursor = validate_history_query(query)?;
        self.ensure_job_exists(access, job_id).await?;
        let rows = match query.order {
            HistoryOrder::NewestFirst => {
                list_job_events_before(
                    &self.pool,
                    access.scope().organization_id(),
                    job_id,
                    fetch_limit(query.limit),
                    cursor,
                )
                .await
            }
            HistoryOrder::OldestFirst => {
                list_job_events(
                    &self.pool,
                    access.scope().organization_id(),
                    job_id,
                    fetch_limit(query.limit),
                    cursor,
                )
                .await
            }
        }
        .map_err(|error| storage_error("list job events", error))?;
        let (rows, has_more) = take_page(rows, query.limit);
        let next_cursor = has_more.then(|| {
            rows.last()
                .expect("a page with more history has a last row")
                .id
                .to_string()
        });
        Ok(JobEventsResponse {
            items: rows
                .into_iter()
                .map(|row| job_event_dto(row, access.visibility()))
                .collect(),
            page: HistoryPageDto {
                limit: query.limit,
                cursor: query.cursor.clone(),
                next_cursor,
                order: query.order,
                has_more,
            },
        })
    }

    /// Lists application logs for one authorized job.
    pub async fn job_logs(
        &self,
        access: AdminAccess,
        job_id: Uuid,
        query: &HistoryQuery,
    ) -> Result<JobLogsResponse, AdminApiError> {
        let cursor = validate_history_query(query)?;
        self.ensure_job_exists(access, job_id).await?;
        let rows = match query.order {
            HistoryOrder::NewestFirst => {
                list_job_logs_before(
                    &self.pool,
                    access.scope().organization_id(),
                    job_id,
                    fetch_limit(query.limit),
                    cursor,
                )
                .await
            }
            HistoryOrder::OldestFirst => {
                list_job_logs(
                    &self.pool,
                    access.scope().organization_id(),
                    job_id,
                    fetch_limit(query.limit),
                    cursor,
                )
                .await
            }
        }
        .map_err(|error| storage_error("list job logs", error))?;
        let (rows, has_more) = take_page(rows, query.limit);
        let next_cursor = has_more.then(|| {
            rows.last()
                .expect("a page with more history has a last row")
                .id
                .to_string()
        });
        Ok(JobLogsResponse {
            items: rows
                .into_iter()
                .map(|row| job_log_dto(row, access.visibility()))
                .collect(),
            page: HistoryPageDto {
                limit: query.limit,
                cursor: query.cursor.clone(),
                next_cursor,
                order: query.order,
                has_more,
            },
        })
    }

    /// Lists workflow runs in the authorized scope.
    pub async fn workflows(
        &self,
        access: AdminAccess,
        query: &WorkflowsQuery,
    ) -> Result<WorkflowsResponse, AdminApiError> {
        validate_page(query.limit, query.offset)?;
        validate_filter(query.workflow_type.as_deref())?;
        let status = parse_workflow_status(query.status.as_deref())?;
        let rows = list_admin_workflow_summaries(
            &self.pool,
            &AdminWorkflowSummaryFilter {
                organization_id: access.scope().organization_id(),
                status,
                workflow_type_contains: query.workflow_type.as_deref(),
                limit: fetch_limit(query.limit),
                offset: query.offset,
            },
        )
        .await
        .map_err(|error| storage_error("list workflow runs", error))?;
        let (rows, has_more) = take_page(rows, query.limit);
        Ok(WorkflowsResponse {
            items: rows.into_iter().map(workflow_summary_dto).collect(),
            page: PageDto {
                limit: query.limit,
                offset: query.offset,
                max_offset: MAX_PAGE_OFFSET,
                has_more,
            },
        })
    }

    /// Loads a workflow, its steps, and its dependency graph.
    pub async fn workflow(
        &self,
        access: AdminAccess,
        workflow_id: Uuid,
        query: &WorkflowQuery,
    ) -> Result<WorkflowResponse, AdminApiError> {
        validate_page(query.step_limit, query.step_offset)?;
        validate_page(query.dependency_limit, query.dependency_offset)?;
        let organization_id = access.scope().organization_id();
        let workflow = get_workflow_run_by_id(&self.pool, organization_id, workflow_id)
            .await
            .map_err(|error| storage_error("load workflow run", error))?
            .ok_or_else(AdminApiError::not_found)?;
        let steps = list_workflow_steps_in_organization_page(
            &self.pool,
            organization_id,
            workflow_id,
            fetch_limit(query.step_limit),
            query.step_offset,
        )
        .await
        .map_err(|error| storage_error("list workflow steps", error))?;
        let dependencies = list_workflow_step_dependencies_in_organization_page(
            &self.pool,
            organization_id,
            workflow_id,
            fetch_limit(query.dependency_limit),
            query.dependency_offset,
        )
        .await
        .map_err(|error| storage_error("list workflow dependencies", error))?;
        let (steps, steps_has_more) = take_page(steps, query.step_limit);
        let (dependencies, dependencies_has_more) = take_page(dependencies, query.dependency_limit);

        Ok(WorkflowResponse {
            workflow: workflow_dto(workflow, access.visibility()),
            steps: steps
                .into_iter()
                .map(|row| workflow_step_dto(row, access.visibility()))
                .collect(),
            steps_page: PageDto {
                limit: query.step_limit,
                offset: query.step_offset,
                max_offset: MAX_PAGE_OFFSET,
                has_more: steps_has_more,
            },
            dependencies: dependencies
                .into_iter()
                .map(workflow_dependency_dto)
                .collect(),
            dependencies_page: PageDto {
                limit: query.dependency_limit,
                offset: query.dependency_offset,
                max_offset: MAX_PAGE_OFFSET,
                has_more: dependencies_has_more,
            },
        })
    }

    /// Lists the service-wide job definition catalog.
    pub async fn definitions(
        &self,
        access: AdminAccess,
        query: &DefinitionsQuery,
    ) -> Result<DefinitionsResponse, AdminApiError> {
        if !access.can_read_service_wide_definitions() {
            return Err(AdminApiError::forbidden());
        }
        validate_page(query.limit, query.offset)?;
        validate_filter(query.job_type.as_deref())?;
        let rows = list_job_definitions(
            &self.pool,
            &JobDefinitionListFilter {
                job_type: query.job_type.as_deref(),
                limit: fetch_limit(query.limit),
                offset: query.offset,
            },
        )
        .await
        .map_err(|error| storage_error("list job definitions", error))?;
        let (rows, has_more) = take_page(rows, query.limit);
        Ok(DefinitionsResponse {
            items: rows
                .into_iter()
                .map(|row| JobDefinitionDto {
                    job_type: row.job_type.as_str().to_owned(),
                    version: row.version,
                    max_attempts: row.max_attempts,
                    default_timeout_seconds: row.default_timeout_seconds,
                    default_priority: row.default_priority,
                    is_enabled: row.is_enabled,
                    created_at: row.created_at,
                    updated_at: row.updated_at,
                })
                .collect(),
            page: PageDto {
                limit: query.limit,
                offset: query.offset,
                max_offset: MAX_PAGE_OFFSET,
                has_more,
            },
        })
    }

    async fn find_job(
        &self,
        access: AdminAccess,
        job_id: Uuid,
    ) -> Result<JobQueueRecord, AdminApiError> {
        get_job_by_id(&self.pool, access.scope().organization_id(), job_id)
            .await
            .map_err(|error| storage_error("load job", error))?
            .ok_or_else(AdminApiError::not_found)
    }

    async fn ensure_job_exists(
        &self,
        access: AdminAccess,
        job_id: Uuid,
    ) -> Result<(), AdminApiError> {
        let exists = job_exists_in_scope(&self.pool, access.scope().organization_id(), job_id)
            .await
            .map_err(|error| storage_error("check job existence", error))?;
        exists.then_some(()).ok_or_else(AdminApiError::not_found)
    }
}

fn validate_page(limit: i64, offset: i64) -> Result<(), AdminApiError> {
    if !(1..=MAX_PAGE_LIMIT).contains(&limit) || !(0..=MAX_PAGE_OFFSET).contains(&offset) {
        return Err(AdminApiError::invalid_query());
    }
    Ok(())
}

fn validate_history_query(query: &HistoryQuery) -> Result<Option<i64>, AdminApiError> {
    validate_page(query.limit, 0)?;
    query
        .cursor
        .as_deref()
        .map(|cursor| {
            cursor
                .parse::<i64>()
                .ok()
                .filter(|cursor| *cursor >= 0)
                .ok_or_else(AdminApiError::invalid_query)
        })
        .transpose()
}

const fn fetch_limit(limit: i64) -> i64 {
    limit + 1
}

fn take_page<T>(mut rows: Vec<T>, limit: i64) -> (Vec<T>, bool) {
    let limit = usize::try_from(limit).expect("page limit is validated before querying");
    let has_more = rows.len() > limit;
    rows.truncate(limit);
    (rows, has_more)
}

fn validate_filter(filter: Option<&str>) -> Result<(), AdminApiError> {
    if filter.is_some_and(|value| value.len() > 200) {
        return Err(AdminApiError::invalid_query());
    }
    Ok(())
}

fn storage_error(operation: &'static str, error: runledger_postgres::Error) -> AdminApiError {
    let (error_kind, error_code) = match &error {
        runledger_postgres::Error::ConfigError(_) => ("config", "admin.storage_error"),
        runledger_postgres::Error::ConnectionError(_) => ("connection", "admin.storage_error"),
        runledger_postgres::Error::MigrationError(_) => ("migration", "admin.storage_error"),
        runledger_postgres::Error::QueryError(error) => ("query", error.code()),
    };
    tracing::error!(
        operation,
        error_kind,
        error_code,
        "Runledger admin storage read failed"
    );
    AdminApiError::storage()
}

fn parse_job_status(status: Option<&str>) -> Result<Option<JobStatus>, AdminApiError> {
    status
        .map(|value| JobStatus::from_db_value(value).ok_or_else(AdminApiError::invalid_query))
        .transpose()
}

fn parse_workflow_status(status: Option<&str>) -> Result<Option<WorkflowRunStatus>, AdminApiError> {
    status
        .map(|value| {
            WorkflowRunStatus::from_db_value(value).ok_or_else(AdminApiError::invalid_query)
        })
        .transpose()
}

fn redacted(visibility: DataVisibility, fields: &[&str]) -> Vec<String> {
    if visibility == DataVisibility::MetadataOnly {
        fields.iter().map(|field| (*field).to_owned()).collect()
    } else {
        Vec::new()
    }
}

fn full<T>(visibility: DataVisibility, value: T) -> Option<T> {
    (visibility == DataVisibility::Full).then_some(value)
}

fn full_optional<T>(visibility: DataVisibility, value: Option<T>) -> Option<T> {
    if visibility == DataVisibility::Full {
        value
    } else {
        None
    }
}

fn job_dto(row: JobQueueRecord, visibility: DataVisibility) -> JobDto {
    JobDto {
        id: row.id,
        job_type: row.job_type.as_str().to_owned(),
        organization_id: row.organization_id,
        status: row.status.as_db_value().to_owned(),
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
        stage: row.stage.as_db_value().to_owned(),
        progress_done: row.progress_done,
        progress_total: row.progress_total,
        progress_pct: row.progress_pct,
        last_error_code: row.last_error_code,
        created_at: row.created_at,
        updated_at: row.updated_at,
        payload: full(visibility, row.payload),
        checkpoint: full_optional(visibility, row.checkpoint),
        output: full_optional(visibility, row.output),
        idempotency_key: full_optional(visibility, row.idempotency_key),
        worker_id: full_optional(visibility, row.worker_id),
        status_reason: full_optional(visibility, row.status_reason),
        last_error_message: full_optional(visibility, row.last_error_message),
        redacted_fields: redacted(
            visibility,
            &[
                "payload",
                "checkpoint",
                "output",
                "idempotency_key",
                "worker_id",
                "status_reason",
                "last_error_message",
            ],
        ),
    }
}

fn job_summary_dto(row: AdminJobSummaryRecord) -> JobSummaryDto {
    JobSummaryDto {
        id: row.id,
        job_type: row.job_type.as_str().to_owned(),
        organization_id: row.organization_id,
        status: row.status.as_db_value().to_owned(),
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
        stage: row.stage.as_db_value().to_owned(),
        progress_done: row.progress_done,
        progress_total: row.progress_total,
        progress_pct: row.progress_pct,
        last_error_code: row.last_error_code,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn job_event_dto(row: JobEventRecord, visibility: DataVisibility) -> JobEventDto {
    JobEventDto {
        id: row.id.to_string(),
        job_id: row.job_id,
        run_number: row.run_number,
        attempt: row.attempt,
        event_type: row.event_type.as_db_value().to_owned(),
        stage: row.stage.map(|stage| stage.as_db_value().to_owned()),
        progress_done: row.progress_done,
        progress_total: row.progress_total,
        occurred_at: row.occurred_at,
        payload: full(visibility, row.payload),
        redacted_fields: redacted(visibility, &["payload"]),
    }
}

fn job_log_dto(row: JobLogRecord, visibility: DataVisibility) -> JobLogDto {
    JobLogDto {
        id: row.id.to_string(),
        job_id: row.job_id,
        run_number: row.run_number,
        attempt: row.attempt,
        level: row.level,
        occurred_at: row.occurred_at,
        message: full(visibility, row.message),
        payload: full(visibility, row.payload),
        redacted_fields: redacted(visibility, &["message", "payload"]),
    }
}

fn workflow_dto(row: WorkflowRunDbRecord, visibility: DataVisibility) -> WorkflowDto {
    WorkflowDto {
        id: row.id,
        workflow_type: row.workflow_type.as_str().to_owned(),
        organization_id: row.organization_id,
        status: row.status.as_db_value().to_owned(),
        result_step_key: row.result_step_key.map(|key| key.as_str().to_owned()),
        started_at: row.started_at,
        finished_at: row.finished_at,
        created_at: row.created_at,
        updated_at: row.updated_at,
        idempotency_key: full_optional(visibility, row.idempotency_key),
        metadata: full(visibility, row.metadata),
        redacted_fields: redacted(visibility, &["idempotency_key", "metadata"]),
    }
}

fn workflow_summary_dto(row: AdminWorkflowSummaryRecord) -> WorkflowSummaryDto {
    WorkflowSummaryDto {
        id: row.id,
        workflow_type: row.workflow_type.as_str().to_owned(),
        organization_id: row.organization_id,
        status: row.status.as_db_value().to_owned(),
        result_step_key: row.result_step_key.map(|key| key.as_str().to_owned()),
        started_at: row.started_at,
        finished_at: row.finished_at,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn workflow_step_dto(row: WorkflowStepDbRecord, visibility: DataVisibility) -> WorkflowStepDto {
    WorkflowStepDto {
        id: row.id,
        workflow_run_id: row.workflow_run_id,
        step_key: row.step_key.as_str().to_owned(),
        execution_kind: row.execution_kind.as_db_value().to_owned(),
        job_type: row.job_type.map(|value| value.as_str().to_owned()),
        organization_id: row.organization_id,
        priority: row.priority,
        max_attempts: row.max_attempts,
        timeout_seconds: row.timeout_seconds,
        stage: row.stage.map(|value| value.as_db_value().to_owned()),
        allow_handler_continuation: row.allow_handler_continuation,
        status: row.status.as_db_value().to_owned(),
        job_id: row.job_id,
        released_at: row.released_at,
        started_at: row.started_at,
        finished_at: row.finished_at,
        dependency_count_total: row.dependency_count_total,
        dependency_count_pending: row.dependency_count_pending,
        dependency_count_unsatisfied: row.dependency_count_unsatisfied,
        last_error_code: row.last_error_code,
        created_at: row.created_at,
        updated_at: row.updated_at,
        payload: full(visibility, row.payload),
        execution_resource_key: full_optional(visibility, row.execution_resource_key),
        status_reason: full_optional(visibility, row.status_reason),
        last_error_message: full_optional(visibility, row.last_error_message),
        output: full_optional(visibility, row.output),
        redacted_fields: redacted(
            visibility,
            &[
                "payload",
                "execution_resource_key",
                "status_reason",
                "last_error_message",
                "output",
            ],
        ),
    }
}

fn workflow_dependency_dto(row: WorkflowStepDependencyDbRecord) -> WorkflowDependencyDto {
    WorkflowDependencyDto {
        workflow_run_id: row.workflow_run_id,
        prerequisite_step_id: row.prerequisite_step_id,
        dependent_step_id: row.dependent_step_id,
        release_mode: row.release_mode.as_db_value().to_owned(),
        created_at: row.created_at,
    }
}
