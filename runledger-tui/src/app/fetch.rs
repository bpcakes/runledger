use runledger_postgres::DbPool;

use crate::data::{
    DashboardData, DefinitionsData, JobDetailData, JobsData, QueueStatusFilter, WorkflowDetailData,
    WorkflowsData, fetch_dashboard, fetch_definitions, fetch_job_detail, fetch_jobs,
    fetch_workflow_detail, fetch_workflows,
};
use crate::scope::Scope;

use super::Screen;

pub(crate) enum FetchOutcome {
    Dashboard(Result<Box<DashboardData>, String>),
    Jobs(Result<Box<JobsData>, String>),
    JobDetail(Result<Box<JobDetailData>, String>),
    Workflows(Result<Box<WorkflowsData>, String>),
    WorkflowDetail(Result<Box<WorkflowDetailData>, String>),
    Definitions(Result<Box<DefinitionsData>, String>),
}

pub(crate) struct FetchRequest {
    pub screen: Screen,
    pub scope: Scope,
    pub queue_filter: QueueStatusFilter,
    pub job_type_filter: Option<String>,
    pub workflow_type_filter: Option<String>,
    pub limit: i64,
}

pub(crate) async fn execute_fetch(pool: &DbPool, req: FetchRequest) -> FetchOutcome {
    match req.screen {
        Screen::Dashboard => {
            FetchOutcome::Dashboard(fetch_dashboard(pool, req.scope).await.map(Box::new))
        }
        Screen::Queue => FetchOutcome::Jobs(
            fetch_jobs(
                pool,
                req.scope,
                req.queue_filter,
                req.job_type_filter,
                req.limit,
            )
            .await
            .map(Box::new),
        ),
        Screen::JobDetail { job_id } => FetchOutcome::JobDetail(
            fetch_job_detail(pool, req.scope, job_id, req.limit)
                .await
                .map(Box::new),
        ),
        Screen::Workflows => FetchOutcome::Workflows(
            fetch_workflows(pool, req.scope, req.workflow_type_filter, req.limit)
                .await
                .map(Box::new),
        ),
        Screen::WorkflowDetail { run_id } => FetchOutcome::WorkflowDetail(
            fetch_workflow_detail(pool, req.scope, run_id)
                .await
                .map(Box::new),
        ),
        Screen::Definitions => FetchOutcome::Definitions(
            fetch_definitions(pool, req.job_type_filter, req.limit)
                .await
                .map(Box::new),
        ),
    }
}
