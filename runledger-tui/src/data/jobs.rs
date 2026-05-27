use runledger_core::jobs::JobStatus;
use runledger_postgres::DbPool;
use runledger_postgres::jobs::{JobListFilter, JobQueueRecord, list_jobs};

use crate::scope::Scope;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueStatusFilter {
    All,
    Pending,
    Leased,
    DeadLettered,
}

impl QueueStatusFilter {
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::All => "All",
            Self::Pending => "Pending",
            Self::Leased => "Leased",
            Self::DeadLettered => "Dead letter",
        }
    }

    #[must_use]
    pub fn next(self) -> Self {
        match self {
            Self::All => Self::Pending,
            Self::Pending => Self::Leased,
            Self::Leased => Self::DeadLettered,
            Self::DeadLettered => Self::All,
        }
    }

    #[must_use]
    pub fn status(self) -> Option<JobStatus> {
        match self {
            Self::All => None,
            Self::Pending => Some(JobStatus::Pending),
            Self::Leased => Some(JobStatus::Leased),
            Self::DeadLettered => Some(JobStatus::DeadLettered),
        }
    }
}

#[derive(Debug, Clone)]
pub struct JobsData {
    pub jobs: Vec<JobQueueRecord>,
}

pub async fn fetch(
    pool: &DbPool,
    scope: Scope,
    filter: QueueStatusFilter,
    job_type: Option<&str>,
    limit: i64,
) -> runledger_postgres::Result<JobsData> {
    let list_filter = JobListFilter {
        organization_id: scope.organization_id,
        status: filter.status(),
        job_type,
        limit,
        offset: 0,
    };
    let jobs = list_jobs(pool, &list_filter).await?;
    Ok(JobsData { jobs })
}
