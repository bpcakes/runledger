use sqlx::types::Uuid;

use crate::{DbPool, Error, Result};

use super::super::errors::validate_pagination;
use super::super::row_decode::parse_job_type_name;
use super::super::types::{AdminJobMetricsRecord, JobContinuationMetricsRecord, JobMetricsRecord};

/// Returns one bounded page of combined metrics for the admin HTTP boundary.
///
/// Service-wide access pages over the registered definition catalog. An
/// organization page contains only job types already visible in that
/// organization's metrics rollup, preventing catalog enumeration.
pub async fn get_admin_job_metrics_page(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_type: Option<&str>,
    limit: i64,
    offset: i64,
) -> Result<Vec<AdminJobMetricsRecord>> {
    validate_pagination(limit, offset)?;

    let rows = sqlx::query!(
        "WITH page_job_types AS MATERIALIZED (
            SELECT visible.job_type
            FROM (
                SELECT definitions.job_type
                FROM job_definitions definitions
                WHERE $1::uuid IS NULL

                UNION

                SELECT rollup.job_type
                FROM job_metrics_rollup rollup
                WHERE $1::uuid IS NOT NULL
                  AND rollup.organization_id = $1
            ) visible
            WHERE ($2::text IS NULL OR visible.job_type = $2)
            ORDER BY visible.job_type ASC
            LIMIT $3 OFFSET $4
         ), base_metrics AS (
            SELECT
                rollup.job_type,
                SUM(rollup.pending_count)::bigint AS pending_count,
                SUM(rollup.leased_count)::bigint AS leased_count,
                SUM(rollup.stale_leases)::bigint AS stale_leases,
                SUM(rollup.succeeded_24h)::bigint AS succeeded_24h,
                SUM(rollup.retryable_24h)::bigint AS retryable_24h,
                SUM(rollup.terminal_24h)::bigint AS terminal_24h,
                SUM(rollup.panicked_24h)::bigint AS panicked_24h,
                SUM(rollup.timeout_24h)::bigint AS timeout_24h,
                SUM(rollup.dead_lettered_24h)::bigint AS dead_lettered_24h,
                AVG(rollup.p50_duration_ms_24h) AS p50_duration_ms_24h,
                AVG(rollup.p95_duration_ms_24h) AS p95_duration_ms_24h
            FROM job_metrics_rollup rollup
            JOIN page_job_types page USING (job_type)
            WHERE ($1::uuid IS NULL OR rollup.organization_id = $1)
            GROUP BY rollup.job_type
         ), continuation_metrics AS (
            SELECT
                rollup.job_type,
                SUM(rollup.continued_24h)::bigint AS continued_24h,
                SUM(rollup.active_continued_count)::bigint AS active_continued_count,
                MAX(rollup.max_active_run_number)::int4 AS max_active_run_number
            FROM job_continuation_metrics_rollup rollup
            JOIN page_job_types page USING (job_type)
            WHERE ($1::uuid IS NULL OR rollup.organization_id = $1)
            GROUP BY rollup.job_type
         )
         SELECT
            page.job_type AS \"job_type!\",
            COALESCE(base.pending_count, 0)::bigint AS \"pending_count!\",
            COALESCE(base.leased_count, 0)::bigint AS \"leased_count!\",
            COALESCE(base.stale_leases, 0)::bigint AS \"stale_leases!\",
            COALESCE(base.succeeded_24h, 0)::bigint AS \"succeeded_24h!\",
            COALESCE(base.retryable_24h, 0)::bigint AS \"retryable_24h!\",
            COALESCE(base.terminal_24h, 0)::bigint AS \"terminal_24h!\",
            COALESCE(base.panicked_24h, 0)::bigint AS \"panicked_24h!\",
            COALESCE(base.timeout_24h, 0)::bigint AS \"timeout_24h!\",
            COALESCE(base.dead_lettered_24h, 0)::bigint AS \"dead_lettered_24h!\",
            base.p50_duration_ms_24h,
            base.p95_duration_ms_24h,
            COALESCE(continuation.continued_24h, 0)::bigint AS \"continued_24h!\",
            COALESCE(continuation.active_continued_count, 0)::bigint
                AS \"active_continued_count!\",
            COALESCE(continuation.max_active_run_number, 0)::int4
                AS \"max_active_run_number!\"
         FROM page_job_types page
         LEFT JOIN base_metrics base USING (job_type)
         LEFT JOIN continuation_metrics continuation USING (job_type)
         ORDER BY page.job_type ASC",
        organization_id,
        job_type,
        limit,
        offset,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("get admin job metrics page", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(AdminJobMetricsRecord {
                job_type: parse_job_type_name(row.job_type)?,
                pending_count: row.pending_count,
                leased_count: row.leased_count,
                stale_leases: row.stale_leases,
                succeeded_24h: row.succeeded_24h,
                retryable_24h: row.retryable_24h,
                terminal_24h: row.terminal_24h,
                panicked_24h: row.panicked_24h,
                timeout_24h: row.timeout_24h,
                dead_lettered_24h: row.dead_lettered_24h,
                p50_duration_ms_24h: row.p50_duration_ms_24h,
                p95_duration_ms_24h: row.p95_duration_ms_24h,
                continued_24h: row.continued_24h,
                active_continued_count: row.active_continued_count,
                max_active_run_number: row.max_active_run_number,
            })
        })
        .collect()
}

pub async fn get_job_metrics(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_type: Option<&str>,
) -> Result<Vec<JobMetricsRecord>> {
    let rows = sqlx::query!(
        "SELECT
            jd.job_type AS \"job_type!\",
            COALESCE(SUM(jmr.pending_count), 0)::bigint AS \"pending_count!\",
            COALESCE(SUM(jmr.leased_count), 0)::bigint AS \"leased_count!\",
            COALESCE(SUM(jmr.stale_leases), 0)::bigint AS \"stale_leases!\",
            COALESCE(SUM(jmr.succeeded_24h), 0)::bigint AS \"succeeded_24h!\",
            COALESCE(SUM(jmr.retryable_24h), 0)::bigint AS \"retryable_24h!\",
            COALESCE(SUM(jmr.terminal_24h), 0)::bigint AS \"terminal_24h!\",
            COALESCE(SUM(jmr.panicked_24h), 0)::bigint AS \"panicked_24h!\",
            COALESCE(SUM(jmr.timeout_24h), 0)::bigint AS \"timeout_24h!\",
            COALESCE(SUM(jmr.dead_lettered_24h), 0)::bigint AS \"dead_lettered_24h!\",
            AVG(jmr.p50_duration_ms_24h) AS p50_duration_ms_24h,
            AVG(jmr.p95_duration_ms_24h) AS p95_duration_ms_24h
         FROM job_definitions jd
         LEFT JOIN job_metrics_rollup jmr
           ON jmr.job_type = jd.job_type
          AND ($1::uuid IS NULL OR jmr.organization_id = $1)
         WHERE ($2::text IS NULL OR jd.job_type = $2)
         GROUP BY jd.job_type
         ORDER BY jd.job_type ASC",
        organization_id,
        job_type,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("get job metrics", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(JobMetricsRecord {
                job_type: parse_job_type_name(row.job_type)?,
                pending_count: row.pending_count,
                leased_count: row.leased_count,
                stale_leases: row.stale_leases,
                succeeded_24h: row.succeeded_24h,
                retryable_24h: row.retryable_24h,
                terminal_24h: row.terminal_24h,
                panicked_24h: row.panicked_24h,
                timeout_24h: row.timeout_24h,
                dead_lettered_24h: row.dead_lettered_24h,
                p50_duration_ms_24h: row.p50_duration_ms_24h,
                p95_duration_ms_24h: row.p95_duration_ms_24h,
            })
        })
        .collect::<Result<Vec<_>>>()
}

/// Returns metrics only for job types with rows in one organization scope.
///
/// Unlike [`get_job_metrics`], this does not synthesize zero-valued rows from
/// the service-wide definition catalog. HTTP tenant boundaries should use this
/// form so an organization cannot enumerate job types it has never used.
pub async fn get_job_metrics_in_organization(
    pool: &DbPool,
    organization_id: Uuid,
    job_type: Option<&str>,
) -> Result<Vec<JobMetricsRecord>> {
    let rows = sqlx::query!(
        "SELECT
            jmr.job_type AS \"job_type!\",
            SUM(jmr.pending_count)::bigint AS \"pending_count!\",
            SUM(jmr.leased_count)::bigint AS \"leased_count!\",
            SUM(jmr.stale_leases)::bigint AS \"stale_leases!\",
            SUM(jmr.succeeded_24h)::bigint AS \"succeeded_24h!\",
            SUM(jmr.retryable_24h)::bigint AS \"retryable_24h!\",
            SUM(jmr.terminal_24h)::bigint AS \"terminal_24h!\",
            SUM(jmr.panicked_24h)::bigint AS \"panicked_24h!\",
            SUM(jmr.timeout_24h)::bigint AS \"timeout_24h!\",
            SUM(jmr.dead_lettered_24h)::bigint AS \"dead_lettered_24h!\",
            AVG(jmr.p50_duration_ms_24h) AS p50_duration_ms_24h,
            AVG(jmr.p95_duration_ms_24h) AS p95_duration_ms_24h
         FROM job_metrics_rollup jmr
         WHERE jmr.organization_id = $1
           AND ($2::text IS NULL OR jmr.job_type = $2)
         GROUP BY jmr.job_type
         ORDER BY jmr.job_type ASC",
        organization_id,
        job_type,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("get job metrics in organization", error)
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(JobMetricsRecord {
                job_type: parse_job_type_name(row.job_type)?,
                pending_count: row.pending_count,
                leased_count: row.leased_count,
                stale_leases: row.stale_leases,
                succeeded_24h: row.succeeded_24h,
                retryable_24h: row.retryable_24h,
                terminal_24h: row.terminal_24h,
                panicked_24h: row.panicked_24h,
                timeout_24h: row.timeout_24h,
                dead_lettered_24h: row.dead_lettered_24h,
                p50_duration_ms_24h: row.p50_duration_ms_24h,
                p95_duration_ms_24h: row.p95_duration_ms_24h,
            })
        })
        .collect::<Result<Vec<_>>>()
}

/// Returns continuation-specific canary and runaway-loop signals by job type.
pub async fn get_job_continuation_metrics(
    pool: &DbPool,
    organization_id: Option<Uuid>,
    job_type: Option<&str>,
) -> Result<Vec<JobContinuationMetricsRecord>> {
    let rows = sqlx::query!(
        "SELECT
            jd.job_type AS \"job_type!\",
            COALESCE(SUM(jcmr.continued_24h), 0)::bigint AS \"continued_24h!\",
            COALESCE(SUM(jcmr.active_continued_count), 0)::bigint AS \"active_continued_count!\",
            COALESCE(MAX(jcmr.max_active_run_number), 0)::int4 AS \"max_active_run_number!\"
         FROM job_definitions jd
         LEFT JOIN job_continuation_metrics_rollup jcmr
           ON jcmr.job_type = jd.job_type
          AND ($1::uuid IS NULL OR jcmr.organization_id = $1)
         WHERE ($2::text IS NULL OR jd.job_type = $2)
         GROUP BY jd.job_type
         ORDER BY jd.job_type ASC",
        organization_id,
        job_type,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("get job continuation metrics", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(JobContinuationMetricsRecord {
                job_type: parse_job_type_name(row.job_type)?,
                continued_24h: row.continued_24h,
                active_continued_count: row.active_continued_count,
                max_active_run_number: row.max_active_run_number,
            })
        })
        .collect::<Result<Vec<_>>>()
}

/// Returns continuation metrics only for job types used by one organization.
///
/// Unlike [`get_job_continuation_metrics`], this does not join the service-wide
/// definition catalog. Organization-scoped HTTP boundaries should use this
/// form so the query does not enumerate and discard unrelated job types.
pub async fn get_job_continuation_metrics_in_organization(
    pool: &DbPool,
    organization_id: Uuid,
    job_type: Option<&str>,
) -> Result<Vec<JobContinuationMetricsRecord>> {
    let rows = sqlx::query!(
        "SELECT
            jcmr.job_type AS \"job_type!\",
            SUM(jcmr.continued_24h)::bigint AS \"continued_24h!\",
            SUM(jcmr.active_continued_count)::bigint AS \"active_continued_count!\",
            MAX(jcmr.max_active_run_number)::int4 AS \"max_active_run_number!\"
         FROM job_continuation_metrics_rollup jcmr
         WHERE jcmr.organization_id = $1
           AND ($2::text IS NULL OR jcmr.job_type = $2)
         GROUP BY jcmr.job_type
         ORDER BY jcmr.job_type ASC",
        organization_id,
        job_type,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("get job continuation metrics in organization", error)
    })?;

    rows.into_iter()
        .map(|row| {
            Ok(JobContinuationMetricsRecord {
                job_type: parse_job_type_name(row.job_type)?,
                continued_24h: row.continued_24h,
                active_continued_count: row.active_continued_count,
                max_active_run_number: row.max_active_run_number,
            })
        })
        .collect::<Result<Vec<_>>>()
}
