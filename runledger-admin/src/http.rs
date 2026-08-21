use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderValue, header};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::{Extension, Json, Router};
use utoipa::openapi::{Info, OpenApiBuilder, server::Server};
use utoipa_axum::{router::OpenApiRouter, routes};
use uuid::Uuid;

use crate::dto::{
    CapabilitiesDto, DefinitionsQuery, DefinitionsResponse, HistoryQuery, JobEventsResponse,
    JobLogsResponse, JobResponse, JobsQuery, JobsResponse, MetricsQuery, MetricsResponse,
    WorkflowQuery, WorkflowResponse, WorkflowsQuery, WorkflowsResponse,
};
use crate::error::ErrorBody;
use crate::{AdminAccess, AdminApiError, AdminService};

const DEFAULT_SERVER_URL: &str = "/api/admin/runledger/v1";

/// Builds the read-only v1 route tree.
///
/// Routes are relative. A typical host nests the result at
/// `/api/admin/runledger/v1` and inserts [`AdminAccess`] after authenticating
/// each request.
pub fn router(service: AdminService) -> Router {
    documented_router()
        .with_state(service)
        .layer(middleware::from_fn(no_store))
        .into()
}

/// Returns the generated OpenAPI 3.1 contract as pretty-printed JSON.
///
/// The document describes routes relative to [`router`] and declares the
/// conventional `/api/admin/runledger/v1` mount point as its server URL.
#[must_use]
pub fn openapi_json() -> String {
    documented_router()
        .into_openapi()
        .to_pretty_json()
        .expect("the static admin OpenAPI document must serialize as JSON")
}

fn documented_router() -> OpenApiRouter<AdminService> {
    let mut info = Info::new("Runledger Admin API", crate::API_VERSION);
    info.description = Some(
        "Read-only administration contract. The embedding application owns authentication and authorization."
            .to_owned(),
    );
    let document = OpenApiBuilder::new()
        .info(info)
        .servers(Some([Server::new(DEFAULT_SERVER_URL)]))
        .build();

    OpenApiRouter::with_openapi(document)
        .routes(routes!(capabilities))
        .routes(routes!(metrics))
        .routes(routes!(jobs))
        .routes(routes!(job))
        .routes(routes!(job_events))
        .routes(routes!(job_logs))
        .routes(routes!(workflows))
        .routes(routes!(workflow))
        .routes(routes!(definitions))
}

async fn no_store(request: Request, next: Next) -> Response {
    let mut response = next.run(request).await;
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, no-store"),
    );
    response
}

fn require_access(access: Option<Extension<AdminAccess>>) -> Result<AdminAccess, AdminApiError> {
    access
        .map(|Extension(access)| access)
        .ok_or_else(AdminApiError::unauthorized)
}

fn require_query<T>(query: Result<Query<T>, QueryRejection>) -> Result<T, AdminApiError> {
    query
        .map(|Query(query)| query)
        .map_err(|_| AdminApiError::invalid_query())
}

fn parse_id(raw_id: &str) -> Result<Uuid, AdminApiError> {
    Uuid::parse_str(raw_id).map_err(|_| AdminApiError::invalid_identifier())
}

#[utoipa::path(
    get,
    path = "/capabilities",
    operation_id = "getAdminCapabilities",
    tag = "Runledger Admin",
    responses(
        (status = 200, description = "Version and effective permissions", body = CapabilitiesDto),
        (status = 401, description = "Admin access was not provided", body = ErrorBody)
    )
)]
async fn capabilities(
    access: Option<Extension<AdminAccess>>,
) -> Result<Json<CapabilitiesDto>, AdminApiError> {
    Ok(Json(require_access(access)?.into()))
}

#[utoipa::path(
    get,
    path = "/metrics",
    operation_id = "getAdminMetrics",
    tag = "Runledger Admin",
    params(MetricsQuery),
    responses(
        (status = 200, description = "Per-job-type operational metrics", body = MetricsResponse),
        (status = 400, description = "Invalid query", body = ErrorBody),
        (status = 401, description = "Admin access was not provided", body = ErrorBody),
        (status = 500, description = "Admin data could not be loaded", body = ErrorBody)
    )
)]
async fn metrics(
    State(service): State<AdminService>,
    access: Option<Extension<AdminAccess>>,
    query: Result<Query<MetricsQuery>, QueryRejection>,
) -> Result<Json<MetricsResponse>, AdminApiError> {
    let access = require_access(access)?;
    Ok(Json(service.metrics(access, &require_query(query)?).await?))
}

#[utoipa::path(
    get,
    path = "/jobs",
    operation_id = "listAdminJobs",
    tag = "Runledger Admin",
    params(JobsQuery),
    responses(
        (status = 200, description = "Page of jobs", body = JobsResponse),
        (status = 400, description = "Invalid query", body = ErrorBody),
        (status = 401, description = "Admin access was not provided", body = ErrorBody),
        (status = 500, description = "Admin data could not be loaded", body = ErrorBody)
    )
)]
async fn jobs(
    State(service): State<AdminService>,
    access: Option<Extension<AdminAccess>>,
    query: Result<Query<JobsQuery>, QueryRejection>,
) -> Result<Json<JobsResponse>, AdminApiError> {
    let access = require_access(access)?;
    Ok(Json(service.jobs(access, &require_query(query)?).await?))
}

#[utoipa::path(
    get,
    path = "/jobs/{job_id}",
    operation_id = "getAdminJob",
    tag = "Runledger Admin",
    params(("job_id" = Uuid, Path, description = "Job identifier")),
    responses(
        (status = 200, description = "Job detail", body = JobResponse),
        (status = 400, description = "Invalid job identifier", body = ErrorBody),
        (status = 401, description = "Admin access was not provided", body = ErrorBody),
        (status = 404, description = "Job was not found in the authorized scope", body = ErrorBody),
        (status = 500, description = "Admin data could not be loaded", body = ErrorBody)
    )
)]
async fn job(
    State(service): State<AdminService>,
    access: Option<Extension<AdminAccess>>,
    Path(job_id): Path<String>,
) -> Result<Json<JobResponse>, AdminApiError> {
    Ok(Json(
        service
            .job(require_access(access)?, parse_id(&job_id)?)
            .await?,
    ))
}

#[utoipa::path(
    get,
    path = "/jobs/{job_id}/events",
    operation_id = "listAdminJobEvents",
    tag = "Runledger Admin",
    params(
        ("job_id" = Uuid, Path, description = "Job identifier"),
        HistoryQuery
    ),
    responses(
        (status = 200, description = "Page of durable job events", body = JobEventsResponse),
        (status = 400, description = "Invalid identifier or query", body = ErrorBody),
        (status = 401, description = "Admin access was not provided", body = ErrorBody),
        (status = 404, description = "Job was not found in the authorized scope", body = ErrorBody),
        (status = 500, description = "Admin data could not be loaded", body = ErrorBody)
    )
)]
async fn job_events(
    State(service): State<AdminService>,
    access: Option<Extension<AdminAccess>>,
    Path(job_id): Path<String>,
    query: Result<Query<HistoryQuery>, QueryRejection>,
) -> Result<Json<JobEventsResponse>, AdminApiError> {
    Ok(Json(
        service
            .job_events(
                require_access(access)?,
                parse_id(&job_id)?,
                &require_query(query)?,
            )
            .await?,
    ))
}

#[utoipa::path(
    get,
    path = "/jobs/{job_id}/logs",
    operation_id = "listAdminJobLogs",
    tag = "Runledger Admin",
    params(
        ("job_id" = Uuid, Path, description = "Job identifier"),
        HistoryQuery
    ),
    responses(
        (status = 200, description = "Page of application-provided job logs", body = JobLogsResponse),
        (status = 400, description = "Invalid identifier or query", body = ErrorBody),
        (status = 401, description = "Admin access was not provided", body = ErrorBody),
        (status = 404, description = "Job was not found in the authorized scope", body = ErrorBody),
        (status = 500, description = "Admin data could not be loaded", body = ErrorBody)
    )
)]
async fn job_logs(
    State(service): State<AdminService>,
    access: Option<Extension<AdminAccess>>,
    Path(job_id): Path<String>,
    query: Result<Query<HistoryQuery>, QueryRejection>,
) -> Result<Json<JobLogsResponse>, AdminApiError> {
    Ok(Json(
        service
            .job_logs(
                require_access(access)?,
                parse_id(&job_id)?,
                &require_query(query)?,
            )
            .await?,
    ))
}

#[utoipa::path(
    get,
    path = "/workflows",
    operation_id = "listAdminWorkflows",
    tag = "Runledger Admin",
    params(WorkflowsQuery),
    responses(
        (status = 200, description = "Page of workflow runs", body = WorkflowsResponse),
        (status = 400, description = "Invalid query", body = ErrorBody),
        (status = 401, description = "Admin access was not provided", body = ErrorBody),
        (status = 500, description = "Admin data could not be loaded", body = ErrorBody)
    )
)]
async fn workflows(
    State(service): State<AdminService>,
    access: Option<Extension<AdminAccess>>,
    query: Result<Query<WorkflowsQuery>, QueryRejection>,
) -> Result<Json<WorkflowsResponse>, AdminApiError> {
    let access = require_access(access)?;
    Ok(Json(
        service.workflows(access, &require_query(query)?).await?,
    ))
}

#[utoipa::path(
    get,
    path = "/workflows/{workflow_id}",
    operation_id = "getAdminWorkflow",
    tag = "Runledger Admin",
    params(
        ("workflow_id" = Uuid, Path, description = "Workflow run identifier"),
        WorkflowQuery
    ),
    responses(
        (status = 200, description = "Workflow detail and paged graph collections", body = WorkflowResponse),
        (status = 400, description = "Invalid identifier or query", body = ErrorBody),
        (status = 401, description = "Admin access was not provided", body = ErrorBody),
        (status = 404, description = "Workflow was not found in the authorized scope", body = ErrorBody),
        (status = 500, description = "Admin data could not be loaded", body = ErrorBody)
    )
)]
async fn workflow(
    State(service): State<AdminService>,
    access: Option<Extension<AdminAccess>>,
    Path(workflow_id): Path<String>,
    query: Result<Query<WorkflowQuery>, QueryRejection>,
) -> Result<Json<WorkflowResponse>, AdminApiError> {
    Ok(Json(
        service
            .workflow(
                require_access(access)?,
                parse_id(&workflow_id)?,
                &require_query(query)?,
            )
            .await?,
    ))
}

#[utoipa::path(
    get,
    path = "/definitions",
    operation_id = "listAdminDefinitions",
    tag = "Runledger Admin",
    params(DefinitionsQuery),
    responses(
        (status = 200, description = "Page of registered job definitions", body = DefinitionsResponse),
        (status = 400, description = "Invalid query", body = ErrorBody),
        (status = 401, description = "Admin access was not provided", body = ErrorBody),
        (status = 403, description = "Access does not include service-wide definitions", body = ErrorBody),
        (status = 500, description = "Admin data could not be loaded", body = ErrorBody)
    )
)]
async fn definitions(
    State(service): State<AdminService>,
    access: Option<Extension<AdminAccess>>,
    query: Result<Query<DefinitionsQuery>, QueryRejection>,
) -> Result<Json<DefinitionsResponse>, AdminApiError> {
    Ok(Json(
        service
            .definitions(require_access(access)?, &require_query(query)?)
            .await?,
    ))
}
