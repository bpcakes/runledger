use runledger_core::jobs::{JobStatus, WorkflowRunStatus};
use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    AdminDataProjection, AdminJobEventRecord, AdminJobLogRecord, AdminJobRecord,
    AdminJobSummaryFilter, AdminJobSummaryRecord, AdminSensitiveData,
    AdminWorkflowDependencyRecord, AdminWorkflowRecord, AdminWorkflowStepRecord,
    AdminWorkflowSummaryFilter, AdminWorkflowSummaryRecord, JobDefinitionListFilter,
    get_admin_job_by_id, get_admin_job_metrics_page, get_admin_workflow_by_id, job_exists_in_scope,
    list_admin_job_definitions, list_admin_job_events, list_admin_job_events_before,
    list_admin_job_logs, list_admin_job_logs_before, list_admin_job_summaries,
    list_admin_workflow_dependencies, list_admin_workflow_steps, list_admin_workflow_summaries,
    workflow_exists_in_scope,
};
use uuid::Uuid;

use crate::dto::{
    DefinitionsQuery, DefinitionsResponse, ExactI64, HistoryOrder, HistoryPageDto, HistoryQuery,
    JobDefinitionDto, JobDto, JobEventDto, JobEventsResponse, JobLogDto, JobLogsResponse,
    JobMetricsDto, JobResponse, JobSummaryDto, JobsQuery, JobsResponse, MAX_PAGE_LIMIT,
    MAX_PAGE_OFFSET, MetricsQuery, MetricsResponse, PageDto, WorkflowCollectionQuery,
    WorkflowDependenciesResponse, WorkflowDependencyDto, WorkflowDto, WorkflowResponse,
    WorkflowStepDto, WorkflowStepsResponse, WorkflowSummaryDto, WorkflowsQuery, WorkflowsResponse,
};
use crate::{AdminAccess, AdminApiError, DataVisibility};

/// Read-only application service behind the HTTP router.
#[derive(Clone)]
pub struct AdminService {
    pool: DbPool,
}

impl AdminService {
    /// Creates an admin service over an existing Runledger PostgreSQL pool.
    ///
    /// The service uses exactly this pool. Cloning a worker pool does not create
    /// a separate connection budget; production polling surfaces should
    /// normally inject a dedicated pool with host-chosen connection and
    /// acquisition limits so admin reads cannot starve runtime work.
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
                pending_count: metric.pending_count.into(),
                leased_count: metric.leased_count.into(),
                stale_leases: metric.stale_leases.into(),
                succeeded_24h: metric.succeeded_24h.into(),
                retryable_24h: metric.retryable_24h.into(),
                terminal_24h: metric.terminal_24h.into(),
                panicked_24h: metric.panicked_24h.into(),
                timeout_24h: metric.timeout_24h.into(),
                dead_lettered_24h: metric.dead_lettered_24h.into(),
                p50_duration_ms_24h: metric.p50_duration_ms_24h,
                p95_duration_ms_24h: metric.p95_duration_ms_24h,
                continued_24h: metric.continued_24h.into(),
                active_continued_count: metric.active_continued_count.into(),
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
        Ok(JobResponse { job: job_dto(row) })
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
                list_admin_job_events_before(
                    &self.pool,
                    access.scope().organization_id(),
                    job_id,
                    fetch_limit(query.limit),
                    cursor,
                    projection(access.visibility()),
                )
                .await
            }
            HistoryOrder::OldestFirst => {
                list_admin_job_events(
                    &self.pool,
                    access.scope().organization_id(),
                    job_id,
                    fetch_limit(query.limit),
                    cursor,
                    projection(access.visibility()),
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
            items: rows.into_iter().map(job_event_dto).collect(),
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
                list_admin_job_logs_before(
                    &self.pool,
                    access.scope().organization_id(),
                    job_id,
                    fetch_limit(query.limit),
                    cursor,
                    projection(access.visibility()),
                )
                .await
            }
            HistoryOrder::OldestFirst => {
                list_admin_job_logs(
                    &self.pool,
                    access.scope().organization_id(),
                    job_id,
                    fetch_limit(query.limit),
                    cursor,
                    projection(access.visibility()),
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
            items: rows.into_iter().map(job_log_dto).collect(),
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

    /// Loads one workflow in the authorized scope.
    pub async fn workflow(
        &self,
        access: AdminAccess,
        workflow_id: Uuid,
    ) -> Result<WorkflowResponse, AdminApiError> {
        let workflow = self.find_workflow(access, workflow_id).await?;
        Ok(WorkflowResponse {
            workflow: workflow_dto(workflow),
        })
    }

    /// Lists one page of steps for an authorized workflow.
    pub async fn workflow_steps(
        &self,
        access: AdminAccess,
        workflow_id: Uuid,
        query: &WorkflowCollectionQuery,
    ) -> Result<WorkflowStepsResponse, AdminApiError> {
        validate_page(query.limit, query.offset)?;
        self.ensure_workflow_exists(access, workflow_id).await?;
        let organization_id = access.scope().organization_id();
        let steps = list_admin_workflow_steps(
            &self.pool,
            organization_id,
            workflow_id,
            fetch_limit(query.limit),
            query.offset,
            projection(access.visibility()),
        )
        .await
        .map_err(|error| storage_error("list workflow steps", error))?;
        let (steps, has_more) = take_page(steps, query.limit);

        Ok(WorkflowStepsResponse {
            items: steps.into_iter().map(workflow_step_dto).collect(),
            page: PageDto {
                limit: query.limit,
                offset: query.offset,
                max_offset: MAX_PAGE_OFFSET,
                has_more,
            },
        })
    }

    /// Lists one page of dependency edges for an authorized workflow.
    pub async fn workflow_dependencies(
        &self,
        access: AdminAccess,
        workflow_id: Uuid,
        query: &WorkflowCollectionQuery,
    ) -> Result<WorkflowDependenciesResponse, AdminApiError> {
        validate_page(query.limit, query.offset)?;
        self.ensure_workflow_exists(access, workflow_id).await?;
        let organization_id = access.scope().organization_id();
        let dependencies = list_admin_workflow_dependencies(
            &self.pool,
            organization_id,
            workflow_id,
            fetch_limit(query.limit),
            query.offset,
        )
        .await
        .map_err(|error| storage_error("list workflow dependencies", error))?;
        let (dependencies, has_more) = take_page(dependencies, query.limit);

        Ok(WorkflowDependenciesResponse {
            items: dependencies
                .into_iter()
                .map(workflow_dependency_dto)
                .collect(),
            page: PageDto {
                limit: query.limit,
                offset: query.offset,
                max_offset: MAX_PAGE_OFFSET,
                has_more,
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
        let rows = list_admin_job_definitions(
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
    ) -> Result<AdminJobRecord, AdminApiError> {
        get_admin_job_by_id(
            &self.pool,
            access.scope().organization_id(),
            job_id,
            projection(access.visibility()),
        )
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

    async fn find_workflow(
        &self,
        access: AdminAccess,
        workflow_id: Uuid,
    ) -> Result<AdminWorkflowRecord, AdminApiError> {
        get_admin_workflow_by_id(
            &self.pool,
            access.scope().organization_id(),
            workflow_id,
            projection(access.visibility()),
        )
        .await
        .map_err(|error| storage_error("load workflow run", error))?
        .ok_or_else(AdminApiError::not_found)
    }

    async fn ensure_workflow_exists(
        &self,
        access: AdminAccess,
        workflow_id: Uuid,
    ) -> Result<(), AdminApiError> {
        let exists =
            workflow_exists_in_scope(&self.pool, access.scope().organization_id(), workflow_id)
                .await
                .map_err(|error| storage_error("check workflow existence", error))?;
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

const fn projection(visibility: DataVisibility) -> AdminDataProjection {
    match visibility {
        DataVisibility::MetadataOnly => AdminDataProjection::MetadataOnly,
        DataVisibility::Full => AdminDataProjection::Full,
    }
}

fn redacted(fields: &[&str]) -> Vec<String> {
    fields.iter().map(|field| (*field).to_owned()).collect()
}

fn job_dto(row: AdminJobRecord) -> JobDto {
    let (
        payload,
        checkpoint,
        output,
        idempotency_key,
        worker_id,
        status_reason,
        last_error_message,
        redacted_fields,
    ) = match row.sensitive {
        AdminSensitiveData::Redacted => (
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            redacted(&[
                "payload",
                "checkpoint",
                "output",
                "idempotency_key",
                "worker_id",
                "status_reason",
                "last_error_message",
            ]),
        ),
        AdminSensitiveData::Full(sensitive) => (
            Some(sensitive.payload),
            sensitive.checkpoint,
            sensitive.output,
            sensitive.idempotency_key,
            sensitive.worker_id,
            sensitive.status_reason,
            sensitive.last_error_message,
            Vec::new(),
        ),
    };
    let row = row.summary;
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
        progress_done: row.progress_done.map(ExactI64::from),
        progress_total: row.progress_total.map(ExactI64::from),
        progress_pct: row.progress_pct,
        last_error_code: row.last_error_code,
        created_at: row.created_at,
        updated_at: row.updated_at,
        payload,
        checkpoint,
        output,
        idempotency_key,
        worker_id,
        status_reason,
        last_error_message,
        redacted_fields,
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
        progress_done: row.progress_done.map(ExactI64::from),
        progress_total: row.progress_total.map(ExactI64::from),
        progress_pct: row.progress_pct,
        last_error_code: row.last_error_code,
        created_at: row.created_at,
        updated_at: row.updated_at,
    }
}

fn job_event_dto(row: AdminJobEventRecord) -> JobEventDto {
    let (payload, redacted_fields) = match row.sensitive {
        AdminSensitiveData::Redacted => (None, redacted(&["payload"])),
        AdminSensitiveData::Full(sensitive) => (Some(sensitive.payload), Vec::new()),
    };
    JobEventDto {
        id: row.id.to_string(),
        job_id: row.job_id,
        run_number: row.run_number,
        attempt: row.attempt,
        event_type: row.event_type.as_db_value().to_owned(),
        stage: row.stage.map(|stage| stage.as_db_value().to_owned()),
        progress_done: row.progress_done.map(ExactI64::from),
        progress_total: row.progress_total.map(ExactI64::from),
        occurred_at: row.occurred_at,
        payload,
        redacted_fields,
    }
}

fn job_log_dto(row: AdminJobLogRecord) -> JobLogDto {
    let (message, payload, redacted_fields) = match row.sensitive {
        AdminSensitiveData::Redacted => (None, None, redacted(&["message", "payload"])),
        AdminSensitiveData::Full(sensitive) => {
            (Some(sensitive.message), Some(sensitive.payload), Vec::new())
        }
    };
    JobLogDto {
        id: row.id.to_string(),
        job_id: row.job_id,
        run_number: row.run_number,
        attempt: row.attempt,
        level: row.level,
        occurred_at: row.occurred_at,
        message,
        payload,
        redacted_fields,
    }
}

fn workflow_dto(row: AdminWorkflowRecord) -> WorkflowDto {
    let (idempotency_key, metadata, redacted_fields) = match row.sensitive {
        AdminSensitiveData::Redacted => (None, None, redacted(&["idempotency_key", "metadata"])),
        AdminSensitiveData::Full(sensitive) => (
            sensitive.idempotency_key,
            Some(sensitive.metadata),
            Vec::new(),
        ),
    };
    let row = row.summary;
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
        idempotency_key,
        metadata,
        redacted_fields,
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

fn workflow_step_dto(row: AdminWorkflowStepRecord) -> WorkflowStepDto {
    let (
        payload,
        execution_resource_key,
        status_reason,
        last_error_message,
        output,
        redacted_fields,
    ) = match row.sensitive {
        AdminSensitiveData::Redacted => (
            None,
            None,
            None,
            None,
            None,
            redacted(&[
                "payload",
                "execution_resource_key",
                "status_reason",
                "last_error_message",
                "output",
            ]),
        ),
        AdminSensitiveData::Full(sensitive) => (
            Some(sensitive.payload),
            sensitive.execution_resource_key,
            sensitive.status_reason,
            sensitive.last_error_message,
            sensitive.output,
            Vec::new(),
        ),
    };
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
        visible_dependency_count_total: row.visible_dependency_count_total,
        visible_dependency_count_pending: row.visible_dependency_count_pending,
        visible_dependency_count_unsatisfied: row.visible_dependency_count_unsatisfied,
        has_hidden_prerequisites: row.has_hidden_prerequisites,
        last_error_code: row.last_error_code,
        created_at: row.created_at,
        updated_at: row.updated_at,
        payload,
        execution_resource_key,
        status_reason,
        last_error_message,
        output,
        redacted_fields,
    }
}

fn workflow_dependency_dto(row: AdminWorkflowDependencyRecord) -> WorkflowDependencyDto {
    WorkflowDependencyDto {
        workflow_run_id: row.workflow_run_id,
        prerequisite_step_id: row.prerequisite_step_id,
        dependent_step_id: row.dependent_step_id,
        release_mode: row.release_mode.as_db_value().to_owned(),
        created_at: row.created_at,
    }
}
