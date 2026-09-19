use super::Supervisor;
use crate::{
    JobLifecycleObserver, JobSucceededEvent, RuntimeShutdownBudget, catalog::JobCatalog,
    config::JobsConfig,
};
use runledger_core::jobs::{JobCompletion, JobContext, JobFailure, JobHandler, JobStatus, JobType};
use runledger_postgres::jobs::{JobEnqueue, enqueue_job, get_job_by_id};
use runledger_test_support::{
    setup_ephemeral_pool_with_untracked_migrations, teardown_ephemeral_pool,
};
use std::{
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};
use tokio::sync::Notify;

struct Succeed;
#[async_trait::async_trait]
impl JobHandler for Succeed {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.settlement")
    }
    async fn execute(
        &self,
        _: JobContext,
        _: serde_json::Value,
    ) -> Result<JobCompletion, JobFailure> {
        Ok(JobCompletion::success())
    }
}

struct HeldCallback {
    entered: Arc<Notify>,
    release: Mutex<mpsc::Receiver<()>>,
}
#[async_trait::async_trait]
impl JobLifecycleObserver for HeldCallback {
    async fn on_job_succeeded(&self, _: JobSucceededEvent) {
        self.entered.notify_one();
        // Deliberately non-yielding: neither observer timeout nor task abort can
        // finish this future before the fixture explicitly releases it.
        self.release
            .lock()
            .expect("fixture release lock")
            .recv()
            .expect("fixture releases callback");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_report_retains_terminal_callback_after_aborted_worker() {
    let (pool, database) =
        setup_ephemeral_pool_with_untracked_migrations("native_settlement", 8).await;
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("server version");
    eprintln!("native settlement PostgreSQL {version}");
    let catalog = JobCatalog::new().handler(Succeed);
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync fixture definition");
    let job = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.settlement"),
            organization_id: None,
            payload: &serde_json::json!({}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue fixture work");
    let config = JobsConfig {
        worker_id: "settlement-worker".into(),
        poll_interval: Duration::from_millis(10),
        claim_batch_size: 1,
        lease_ttl_seconds: 30,
        max_global_concurrency: 1,
        reaper_interval: Duration::from_secs(30),
        schedule_poll_interval: Duration::from_secs(30),
        reaper_retry_delay_ms: 1_000,
    };
    let entered = Arc::new(Notify::new());
    let (release, receiver) = mpsc::channel();
    let supervisor = Supervisor::builder(&pool, config)
        .expect("runtime exists")
        .with_catalog(&catalog)
        .disable_scheduler()
        .disable_reaper()
        .disable_intent_promoter()
        .with_job_lifecycle_observer(HeldCallback {
            entered: entered.clone(),
            release: Mutex::new(receiver),
        })
        .build()
        .expect("valid supervisor");
    let retained = supervisor.descendants.clone();
    let shutdown = supervisor.shutdown_handle();
    let budget = RuntimeShutdownBudget::new(Duration::from_millis(20), Duration::from_millis(20))
        .expect("valid stop budget");
    let mut driver = tokio::spawn(
        supervisor.run_until_shutdown_report(crate::RuntimeShutdownSignal::pending(), budget),
    );
    let entry = tokio::time::timeout(Duration::from_secs(5), entered.notified()).await;
    shutdown.request_shutdown();
    let result = tokio::time::timeout(Duration::from_secs(2), &mut driver).await;
    // Release the blocking fixture before assertions or any error path can unwind.
    let _ = release.send(());
    tokio::time::timeout(Duration::from_secs(2), retained.wait())
        .await
        .expect("descendant joins after release");
    entry.expect("durable-success callback entered");
    let report = result
        .expect("native report bounded")
        .expect("report driver joined");
    assert!(!report.is_success());
    assert!(!report.is_cooperatively_stopped());
    assert!(
        report
            .unjoined()
            .iter()
            .any(|task| task.task == "terminal_observer" && task.abort_requested)
    );
    let stored = get_job_by_id(&pool, None, job)
        .await
        .expect("read durable outcome")
        .expect("job exists");
    assert_eq!(stored.status, JobStatus::Succeeded);
    teardown_ephemeral_pool(pool, database).await;
}
