mod dashboard;
mod definitions;
mod job_detail;
mod jobs;
mod workflows;

pub use dashboard::DashboardData;
pub use definitions::DefinitionsData;
pub use job_detail::JobDetailData;
pub use jobs::{JobsData, QueueStatusFilter};
pub use workflows::{WorkflowDetailData, WorkflowsData};

use runledger_postgres::{DbPool, Error};

use crate::scope::Scope;

pub fn pg_error(error: Error) -> String {
    error.to_string()
}

pub async fn fetch_dashboard(pool: &DbPool, scope: Scope) -> Result<DashboardData, String> {
    dashboard::fetch(pool, scope).await.map_err(pg_error)
}

pub async fn fetch_jobs(
    pool: &DbPool,
    scope: Scope,
    filter: QueueStatusFilter,
    job_type: Option<String>,
    limit: i64,
) -> Result<JobsData, String> {
    jobs::fetch(pool, scope, filter, job_type.as_deref(), limit)
        .await
        .map_err(pg_error)
}

pub async fn fetch_job_detail(
    pool: &DbPool,
    scope: Scope,
    job_id: uuid::Uuid,
    limit: i64,
) -> Result<JobDetailData, String> {
    job_detail::fetch(pool, scope, job_id, limit).await
}

pub async fn fetch_workflows(
    pool: &DbPool,
    scope: Scope,
    workflow_type: Option<String>,
    limit: i64,
) -> Result<WorkflowsData, String> {
    workflows::fetch_runs(pool, scope, workflow_type.as_deref(), limit)
        .await
        .map_err(pg_error)
}

pub async fn fetch_workflow_detail(
    pool: &DbPool,
    scope: Scope,
    run_id: uuid::Uuid,
    limit: i64,
) -> Result<WorkflowDetailData, String> {
    workflows::fetch_detail(pool, scope, run_id, limit).await
}

pub async fn fetch_definitions(
    pool: &DbPool,
    job_type: Option<String>,
    limit: i64,
) -> Result<DefinitionsData, String> {
    definitions::fetch(pool, job_type.as_deref(), limit)
        .await
        .map_err(pg_error)
}
