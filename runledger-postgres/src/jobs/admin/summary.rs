use chrono::{DateTime, Utc};
use sqlx::types::Uuid;

use crate::jobs::errors::validate_page_limit;
use crate::jobs::row_decode::{parse_job_stage, parse_job_status, parse_job_type_name};
use crate::jobs::types::{JobReadScope, JobStatusRecord, JobSummary, JobSummaryFilter};
use crate::{DbPool, Error, Result};

struct SummaryRow {
    id: Uuid,
    job_type: String,
    organization_id: Option<Uuid>,
    status: String,
    priority: i32,
    run_number: i32,
    attempt: i32,
    max_attempts: i32,
    next_run_at: DateTime<Utc>,
    stage: Option<String>,
    progress_done: Option<i64>,
    progress_total: Option<i64>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

/// Reads a compact page in descending `(created_at, id)` order within an
/// application-authorized scope. Empty pages end the scan; use the last row's
/// [`JobSummary::cursor`] to continue. No count query or JSON fields are read.
#[expect(
    clippy::cognitive_complexity,
    reason = "SQLx expands six statically checked scope/cursor query branches"
)]
pub async fn list_job_summaries(
    pool: &DbPool,
    filter: &JobSummaryFilter<'_>,
) -> Result<Vec<JobSummary>> {
    validate_page_limit(filter.limit)?;
    // Separate first/subsequent-page SQL keeps the tuple comparison indexable
    // even when PostgreSQL chooses a generic prepared plan.
    macro_rules! page {
        ($cursor_predicate:literal, $($cursor_arg:expr),* $(,)?) => {
            crate::jobs::scoped_read::scoped_list!(
                SummaryRow, pool, filter.scope,
                "SELECT id, job_type, organization_id, status::text AS \"status!\",
                    priority, run_number, attempt, max_attempts, next_run_at, stage,
                    progress_done, progress_total, created_at, updated_at
                 FROM job_queue WHERE",
                "AND ($2::text::job_status IS NULL OR status = $2::text::job_status)
                   AND ($3::text IS NULL OR job_type = $3) " + $cursor_predicate +
                " ORDER BY created_at DESC, id DESC LIMIT $4",
                filter.status.map(|s| s.as_db_value()),
                filter.job_type.map(|t| t.as_str()), filter.limit, $($cursor_arg),*
            )
        };
    }
    let rows = match filter.after {
        Some(after) => page!(
            "AND (created_at, id) < ($5, $6)",
            after.created_at,
            after.id
        ),
        None => page!("",),
    }
    .map_err(|error| Error::from_query_sqlx_with_context("list job summaries", error))?;
    rows.into_iter()
        .map(|row| {
            Ok(JobSummary {
                id: row.id,
                job_type: parse_job_type_name(row.job_type)?,
                organization_id: row.organization_id,
                status: parse_job_status(row.status)?,
                priority: row.priority,
                run_number: row.run_number,
                attempt: row.attempt,
                max_attempts: row.max_attempts,
                next_run_at: row.next_run_at,
                stage: row.stage.map(parse_job_stage).transpose()?,
                progress_done: row.progress_done,
                progress_total: row.progress_total,
                created_at: row.created_at,
                updated_at: row.updated_at,
            })
        })
        .collect()
}

struct StatusRow {
    id: Uuid,
    status: String,
    run_number: i32,
    attempt: i32,
    updated_at: DateTime<Utc>,
}

/// Reads up to `JOB_LIST_PAGE_LIMIT_MAX` input IDs in one statement. Duplicate
/// IDs return one row; missing and out-of-scope IDs are both omitted. Results
/// are ordered by ID, not input order. Empty input returns without a query.
/// The application must authorize `scope`; observations confer no mutation rights.
pub async fn get_job_statuses_with_scope(
    pool: &DbPool,
    scope: JobReadScope,
    job_ids: &[Uuid],
) -> Result<Vec<JobStatusRecord>> {
    if job_ids.is_empty() {
        return Ok(Vec::new());
    }
    validate_page_limit(i64::try_from(job_ids.len()).unwrap_or(i64::MAX))?;
    let rows = crate::jobs::scoped_read::scoped_list!(
        StatusRow,
        pool,
        scope,
        "SELECT id, status::text AS \"status!\", run_number, attempt, updated_at
         FROM job_queue WHERE",
        "AND id = ANY($2::uuid[]) ORDER BY id",
        job_ids,
    )
    .map_err(|error| Error::from_query_sqlx_with_context("get job statuses", error))?;
    rows.into_iter()
        .map(|row| {
            Ok(JobStatusRecord {
                id: row.id,
                status: parse_job_status(row.status)?,
                run_number: row.run_number,
                attempt: row.attempt,
                updated_at: row.updated_at,
            })
        })
        .collect()
}
