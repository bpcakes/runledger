use runledger_core::jobs::WorkflowRunStatus;
use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    JobMetricsRecord, WorkflowRunCountFilter, count_workflow_runs, get_job_metrics,
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

    let failed_workflows = count_to_usize(
        count_workflow_runs(
            pool,
            &WorkflowRunCountFilter {
                organization_id: scope.organization_id,
                status: Some(WorkflowRunStatus::CompletedWithErrors),
                workflow_type: None,
            },
        )
        .await?,
    );
    let external_waits = count_to_usize(
        count_workflow_runs(
            pool,
            &WorkflowRunCountFilter {
                organization_id: scope.organization_id,
                status: Some(WorkflowRunStatus::WaitingForExternal),
                workflow_type: None,
            },
        )
        .await?,
    );

    Ok(DashboardData {
        metrics,
        failed_workflows,
        external_waits,
    })
}

fn count_to_usize(count: i64) -> usize {
    usize::try_from(count).unwrap_or(usize::MAX)
}
