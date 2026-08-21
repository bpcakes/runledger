use axum::extract::rejection::QueryRejection;
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderValue, header};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::get;
use axum::{Extension, Json, Router};
use uuid::Uuid;

use crate::dto::{
    CapabilitiesDto, DefinitionsQuery, DefinitionsResponse, HistoryQuery, JobEventsResponse,
    JobLogsResponse, JobResponse, JobsQuery, JobsResponse, MetricsQuery, MetricsResponse,
    WorkflowQuery, WorkflowResponse, WorkflowsQuery, WorkflowsResponse,
};
use crate::{AdminAccess, AdminApiError, AdminService};

/// Builds the read-only v1 route tree.
///
/// Routes are relative. A typical host nests the result at
/// `/api/admin/runledger/v1` and inserts [`AdminAccess`] after authenticating
/// each request.
pub fn router(service: AdminService) -> Router {
    Router::new()
        .route("/capabilities", get(capabilities))
        .route("/metrics", get(metrics))
        .route("/jobs", get(jobs))
        .route("/jobs/{job_id}", get(job))
        .route("/jobs/{job_id}/events", get(job_events))
        .route("/jobs/{job_id}/logs", get(job_logs))
        .route("/workflows", get(workflows))
        .route("/workflows/{workflow_id}", get(workflow))
        .route("/definitions", get(definitions))
        .with_state(service)
        .layer(middleware::from_fn(no_store))
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

async fn capabilities(
    access: Option<Extension<AdminAccess>>,
) -> Result<Json<CapabilitiesDto>, AdminApiError> {
    Ok(Json(require_access(access)?.into()))
}

async fn metrics(
    State(service): State<AdminService>,
    access: Option<Extension<AdminAccess>>,
    query: Result<Query<MetricsQuery>, QueryRejection>,
) -> Result<Json<MetricsResponse>, AdminApiError> {
    let access = require_access(access)?;
    Ok(Json(service.metrics(access, &require_query(query)?).await?))
}

async fn jobs(
    State(service): State<AdminService>,
    access: Option<Extension<AdminAccess>>,
    query: Result<Query<JobsQuery>, QueryRejection>,
) -> Result<Json<JobsResponse>, AdminApiError> {
    let access = require_access(access)?;
    Ok(Json(service.jobs(access, &require_query(query)?).await?))
}

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
