use runledger_postgres::DbPool;
use runledger_postgres::jobs::{JobMetricsRecord, get_job_metrics};

use crate::scope::Scope;

#[derive(Debug, Clone)]
pub struct DashboardData {
    pub metrics: Vec<JobMetricsRecord>,
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
    Ok(DashboardData { metrics })
}
