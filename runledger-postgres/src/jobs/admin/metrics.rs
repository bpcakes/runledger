use sqlx::types::Uuid;

use crate::{DbPool, Error, Result};

use super::super::row_decode::parse_job_type_name;
use super::super::types::{JobContinuationMetricsRecord, JobMetricsRecord};

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
