use std::collections::BTreeMap;
use std::future::Future;

use runledger_core::jobs::WorkflowRunStatus;
use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    JobContinuationMetricsRecord, JobMetricsRecord, WorkflowRunReadCountFilter,
    count_workflow_runs_with_scope, get_job_continuation_metrics, get_job_metrics,
};

use crate::scope::Scope;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DashboardContinuationMetrics {
    pub continued_24h: i64,
    pub active_continued_count: i64,
    pub max_active_run_number: i32,
}

impl From<JobContinuationMetricsRecord> for DashboardContinuationMetrics {
    fn from(metrics: JobContinuationMetricsRecord) -> Self {
        Self {
            continued_24h: metrics.continued_24h,
            active_continued_count: metrics.active_continued_count,
            max_active_run_number: metrics.max_active_run_number,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DashboardRow {
    fields: [String; 11],
    has_stale_leases: bool,
}

impl DashboardRow {
    pub(crate) fn into_fields(self) -> [String; 11] {
        self.fields
    }

    pub(crate) const fn has_stale_leases(&self) -> bool {
        self.has_stale_leases
    }
}

#[derive(Debug, Clone)]
pub struct DashboardData {
    pub metrics: Vec<JobMetricsRecord>,
    pub continuation_metrics: BTreeMap<String, DashboardContinuationMetrics>,
    pub failed_workflows: usize,
    pub external_waits: usize,
}

impl DashboardData {
    pub(crate) fn continuation_for(&self, job_type: &str) -> DashboardContinuationMetrics {
        self.continuation_metrics
            .get(job_type)
            .copied()
            .unwrap_or_default()
    }

    pub(crate) fn row_for(&self, metric: &JobMetricsRecord) -> DashboardRow {
        let continuation = self.continuation_for(metric.job_type.as_str());
        DashboardRow {
            fields: [
                metric.job_type.as_str().to_owned(),
                metric.pending_count.to_string(),
                metric.leased_count.to_string(),
                metric.stale_leases.to_string(),
                continuation.continued_24h.to_string(),
                continuation.active_continued_count.to_string(),
                continuation.max_active_run_number.to_string(),
                metric.succeeded_24h.to_string(),
                metric.dead_lettered_24h.to_string(),
                format_duration_metric(metric.p50_duration_ms_24h),
                format_duration_metric(metric.p95_duration_ms_24h),
            ],
            has_stale_leases: metric.stale_leases > 0,
        }
    }
}

pub async fn fetch(pool: &DbPool, scope: Scope) -> runledger_postgres::Result<DashboardData> {
    let workflow_read_scope = scope.workflow_read_scope();
    let failed_workflow_filter = WorkflowRunReadCountFilter {
        scope: workflow_read_scope,
        status: Some(WorkflowRunStatus::CompletedWithErrors),
        workflow_type: None,
    };
    let external_wait_filter = WorkflowRunReadCountFilter {
        scope: workflow_read_scope,
        status: Some(WorkflowRunStatus::WaitingForExternal),
        workflow_type: None,
    };

    let (metrics, continuation_metrics, failed_workflows, external_waits) =
        try_join_dashboard_queries(
            get_job_metrics(pool, scope.organization_id, None),
            get_job_continuation_metrics(pool, scope.organization_id, None),
            count_workflow_runs_with_scope(pool, &failed_workflow_filter),
            count_workflow_runs_with_scope(pool, &external_wait_filter),
        )
        .await?;

    let continuation_metrics = continuation_metrics
        .into_iter()
        .map(|metrics| {
            let job_type = metrics.job_type.as_str().to_owned();
            (job_type, metrics.into())
        })
        .collect();

    Ok(assemble_dashboard(
        metrics,
        continuation_metrics,
        failed_workflows,
        external_waits,
    ))
}

async fn try_join_dashboard_queries<
    Metrics,
    Continuations,
    FailedWorkflows,
    ExternalWaits,
    QueryError,
    MetricsFuture,
    ContinuationsFuture,
    FailedWorkflowsFuture,
    ExternalWaitsFuture,
>(
    metrics: MetricsFuture,
    continuations: ContinuationsFuture,
    failed_workflows: FailedWorkflowsFuture,
    external_waits: ExternalWaitsFuture,
) -> Result<(Metrics, Continuations, FailedWorkflows, ExternalWaits), QueryError>
where
    MetricsFuture: Future<Output = Result<Metrics, QueryError>>,
    ContinuationsFuture: Future<Output = Result<Continuations, QueryError>>,
    FailedWorkflowsFuture: Future<Output = Result<FailedWorkflows, QueryError>>,
    ExternalWaitsFuture: Future<Output = Result<ExternalWaits, QueryError>>,
{
    tokio::try_join!(metrics, continuations, failed_workflows, external_waits)
}

fn assemble_dashboard(
    mut metrics: Vec<JobMetricsRecord>,
    continuation_metrics: BTreeMap<String, DashboardContinuationMetrics>,
    failed_workflows: i64,
    external_waits: i64,
) -> DashboardData {
    metrics.sort_by(|a, b| {
        let a_load = a.pending_count + a.leased_count;
        let b_load = b.pending_count + b.leased_count;
        b_load
            .cmp(&a_load)
            .then_with(|| a.job_type.as_str().cmp(b.job_type.as_str()))
    });

    DashboardData {
        metrics,
        continuation_metrics,
        failed_workflows: count_to_usize(failed_workflows),
        external_waits: count_to_usize(external_waits),
    }
}

fn count_to_usize(count: i64) -> usize {
    usize::try_from(count).unwrap_or(usize::MAX)
}

fn format_duration_metric(duration_ms: Option<f64>) -> String {
    duration_ms.map_or_else(|| "—".to_owned(), |value| format!("{value:.0}"))
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use runledger_core::jobs::JobTypeName;

    use super::*;

    fn job_metrics(job_type: &str, pending_count: i64, leased_count: i64) -> JobMetricsRecord {
        JobMetricsRecord {
            job_type: JobTypeName::new(job_type).expect("valid job type"),
            pending_count,
            leased_count,
            stale_leases: 0,
            succeeded_24h: 0,
            retryable_24h: 0,
            terminal_24h: 0,
            panicked_24h: 0,
            timeout_24h: 0,
            dead_lettered_24h: 0,
            p50_duration_ms_24h: None,
            p95_duration_ms_24h: None,
        }
    }

    struct PendingUntilDropped {
        drop_count: Arc<AtomicUsize>,
    }

    impl Future for PendingUntilDropped {
        type Output = Result<&'static str, &'static str>;

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Pending
        }
    }

    impl Drop for PendingUntilDropped {
        fn drop(&mut self) {
            self.drop_count.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn dashboard_query_join_drops_pending_siblings_on_error() {
        let drop_count = Arc::new(AtomicUsize::new(0));
        let pending = || PendingUntilDropped {
            drop_count: Arc::clone(&drop_count),
        };

        let result = tokio::time::timeout(
            Duration::from_millis(100),
            try_join_dashboard_queries(
                async { Err::<&'static str, _>("metrics failed") },
                pending(),
                pending(),
                pending(),
            ),
        )
        .await
        .expect("dashboard join should fail fast");

        assert_eq!(result, Err("metrics failed"));
        assert_eq!(drop_count.load(Ordering::SeqCst), 3);

        let subsequent = try_join_dashboard_queries(
            async { Ok::<_, &'static str>("metrics") },
            async { Ok::<_, &'static str>("continuations") },
            async { Ok::<_, &'static str>(17_i64) },
            async { Ok::<_, &'static str>(23_i64) },
        )
        .await;
        assert_eq!(subsequent, Ok(("metrics", "continuations", 17, 23)));
    }

    #[tokio::test]
    async fn dashboard_query_join_preserves_all_successful_results() {
        let result = try_join_dashboard_queries(
            async { Ok::<_, &'static str>("metrics") },
            async { Ok::<_, &'static str>("continuations") },
            async { Ok::<_, &'static str>(17_i64) },
            async { Ok::<_, &'static str>(23_i64) },
        )
        .await;

        assert_eq!(result, Ok(("metrics", "continuations", 17, 23)));
    }

    #[test]
    fn dashboard_assembly_sorts_by_load_then_job_type() {
        let continuation = DashboardContinuationMetrics {
            continued_24h: 7,
            active_continued_count: 3,
            max_active_run_number: 11,
        };
        let data = assemble_dashboard(
            vec![
                job_metrics("jobs.zeta", 2, 1),
                job_metrics("jobs.low", 1, 0),
                job_metrics("jobs.alpha", 1, 2),
            ],
            BTreeMap::from([("jobs.alpha".to_owned(), continuation)]),
            17,
            23,
        );

        assert_eq!(
            data.metrics
                .iter()
                .map(|metric| metric.job_type.as_str())
                .collect::<Vec<_>>(),
            vec!["jobs.alpha", "jobs.zeta", "jobs.low"]
        );
        assert_eq!(data.continuation_for("jobs.alpha"), continuation);
        assert_eq!(data.failed_workflows, 17);
        assert_eq!(data.external_waits, 23);
    }

    #[test]
    fn dashboard_row_fields_are_the_exact_rendered_values() {
        let mut metric = job_metrics("jobs.continued", 12, 34);
        metric.stale_leases = 56;
        metric.succeeded_24h = 78;
        metric.dead_lettered_24h = 90;
        metric.p50_duration_ms_24h = Some(1234.4);
        metric.p95_duration_ms_24h = None;
        let data = DashboardData {
            metrics: vec![metric],
            continuation_metrics: BTreeMap::from([(
                "jobs.continued".to_owned(),
                DashboardContinuationMetrics {
                    continued_24h: 123,
                    active_continued_count: 456,
                    max_active_run_number: 789,
                },
            )]),
            failed_workflows: 0,
            external_waits: 0,
        };

        let row = data.row_for(&data.metrics[0]);
        assert!(row.has_stale_leases());
        assert_eq!(
            row.into_fields(),
            [
                "jobs.continued".to_owned(),
                "12".to_owned(),
                "34".to_owned(),
                "56".to_owned(),
                "123".to_owned(),
                "456".to_owned(),
                "789".to_owned(),
                "78".to_owned(),
                "90".to_owned(),
                "1234".to_owned(),
                "—".to_owned(),
            ]
        );
    }

    #[test]
    fn continuation_lookup_returns_stored_metrics_or_zeroes() {
        let expected = DashboardContinuationMetrics {
            continued_24h: 7,
            active_continued_count: 3,
            max_active_run_number: 11,
        };
        let data = DashboardData {
            metrics: Vec::new(),
            continuation_metrics: BTreeMap::from([("jobs.continued".to_owned(), expected)]),
            failed_workflows: 0,
            external_waits: 0,
        };

        assert_eq!(data.continuation_for("jobs.continued"), expected);
        assert_eq!(
            data.continuation_for("jobs.without-continuations"),
            DashboardContinuationMetrics::default()
        );
    }
}
