use runledger_core::jobs::WorkflowRunStatus;
use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    JobMetricsRecord, WorkflowRunListFilter, get_job_metrics, list_workflow_runs,
};

use crate::scope::Scope;

#[derive(Debug, Clone)]
pub struct DashboardData {
    pub metrics: Vec<JobMetricsRecord>,
    pub failed_workflows: usize,
    pub external_waits: usize,
}

pub async fn fetch(pool: &DbPool, scope: Scope) -> runledger_postgres::Result<DashboardData> {
    let mut metrics = get_job_metrics(pool, scope.organization_id, None).await?;
    metrics.sort_by(|a, b| {
        let a_load = a.pending_count + a.leased_count;
        let b_load = b.pending_count + b.leased_count;
        b_load
            .cmp(&a_load)
            .then_with(|| a.job_type.as_str().cmp(b.job_type.as_str()))
    });

    let failed_workflows = list_workflow_runs(
        pool,
        &WorkflowRunListFilter {
            organization_id: scope.organization_id,
            status: Some(WorkflowRunStatus::CompletedWithErrors),
            workflow_type: None,
            limit: 10_000,
            offset: 0,
        },
    )
    .await?
    .len();
    let external_waits = list_workflow_runs(
        pool,
        &WorkflowRunListFilter {
            organization_id: scope.organization_id,
            status: Some(WorkflowRunStatus::WaitingForExternal),
            workflow_type: None,
            limit: 10_000,
            offset: 0,
        },
    )
    .await?
    .len();

    Ok(DashboardData {
        metrics,
        failed_workflows,
        external_waits,
    })
}
