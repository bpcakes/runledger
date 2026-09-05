use chrono::{DateTime, Utc};
use runledger_core::jobs::{JobStage, JobStatus, JobType, JobTypeName};
use sqlx::types::Uuid;

use super::JobReadScope;

/// Exclusive position in descending `(created_at, id)` order.
/// Preserve PostgreSQL microsecond timestamp precision when transporting this value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JobSummaryCursor {
    pub created_at: DateTime<Utc>,
    pub id: Uuid,
}

/// Compact operational page. Applications must authorize `scope` and keep the
/// scope/filters unchanged between pages. A cursor is a position, not a snapshot:
/// concurrent inserts ahead of it are excluded, and mutable status filters can
/// gain or lose rows while scanning. Use detail reads for payload inspection.
#[derive(Clone, Debug)]
pub struct JobSummaryFilter<'a> {
    pub scope: JobReadScope,
    pub status: Option<JobStatus>,
    /// Exact, case-sensitive identifier; no substring or wildcard matching.
    pub job_type: Option<JobType<'a>>,
    /// Between 1 and `JOB_LIST_PAGE_LIMIT_MAX`, inclusive.
    pub limit: i64,
    pub after: Option<JobSummaryCursor>,
}

/// Operational fields without payload, checkpoint, output, or free-form errors.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct JobSummary {
    pub id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub status: JobStatus,
    pub priority: i32,
    pub run_number: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    pub next_run_at: DateTime<Utc>,
    pub stage: Option<JobStage>,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl JobSummary {
    /// Continue a scan after this row, normally the last row in a full page.
    #[must_use]
    pub const fn cursor(&self) -> JobSummaryCursor {
        JobSummaryCursor {
            created_at: self.created_at,
            id: self.id,
        }
    }
}

/// A batch status observation, not authorization or a lease/mutation fence.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct JobStatusRecord {
    pub id: Uuid,
    pub status: JobStatus,
    pub run_number: i32,
    pub attempt: i32,
    pub updated_at: DateTime<Utc>,
}
