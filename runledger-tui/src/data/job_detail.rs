use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    JobEventRecord, JobLogRecord, JobQueueRecord, get_job_by_id, get_workflow_run_id_for_job,
    list_job_events, list_job_logs,
};
use uuid::Uuid;

use super::pg_error;
use crate::scope::Scope;

#[derive(Debug)]
pub struct JobDetailData {
    pub job: JobQueueRecord,
    pub events: Vec<JobEventRecord>,
    pub logs: Vec<JobLogRecord>,
    pub workflow_run_id: Option<Uuid>,
}

pub async fn fetch(
    pool: &DbPool,
    scope: Scope,
    job_id: Uuid,
    limit: i64,
) -> Result<JobDetailData, String> {
    let job = get_job_by_id(pool, scope.organization_id, job_id)
        .await
        .map_err(pg_error)?
        .ok_or_else(|| "Job not found.".to_owned())?;
    let events = list_job_events(pool, scope.organization_id, job_id, limit, None)
        .await
        .map_err(pg_error)?;
    let logs = list_job_logs(pool, scope.organization_id, job_id, limit, None)
        .await
        .map_err(pg_error)?;
    let workflow_run_id = get_workflow_run_id_for_job(pool, job_id)
        .await
        .map_err(pg_error)?;
    Ok(JobDetailData {
        job,
        events,
        logs,
        workflow_run_id,
    })
}
