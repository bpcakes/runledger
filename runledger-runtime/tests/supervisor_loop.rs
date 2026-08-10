use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use runledger_core::jobs::{JobCompletion, JobContext, JobFailure, JobStatus, JobType};
use runledger_postgres::jobs::{
    JobDefinitionUpsert, JobEnqueue, enqueue_job, get_job_by_id, upsert_job_definition_tx,
};
use runledger_runtime::Supervisor;
use runledger_runtime::config::JobsConfig;
use runledger_runtime::registry::{JobHandler, JobRegistry};
use serde_json::{Value, json};
use tokio::time::{Instant, sleep};

use runledger_test_support::{
    setup_ephemeral_pool_with_untracked_migrations as setup_ephemeral_pool, teardown_ephemeral_pool,
};

const SUPERVISOR_TEST_JOB: &str = "jobs.test.supervisor";

struct CountingHandler {
    runs: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl JobHandler for CountingHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(SUPERVISOR_TEST_JOB)
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(JobCompletion::success())
    }
}

fn test_config() -> JobsConfig {
    JobsConfig {
        worker_id: "supervisor-test-worker".to_string(),
        poll_interval: Duration::from_millis(25),
        claim_batch_size: 4,
        lease_ttl_seconds: 10,
        max_global_concurrency: 4,
        reaper_interval: Duration::from_millis(50),
        schedule_poll_interval: Duration::from_millis(50),
        reaper_retry_delay_ms: 1_000,
    }
}

#[tokio::test]
async fn supervisor_processes_job_and_shuts_down() {
    let (pool, database) = setup_ephemeral_pool("runtime_supervisor_job", 8).await;

    let mut tx = pool.begin().await.expect("begin job definition tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(SUPERVISOR_TEST_JOB),
            version: 1,
            max_attempts: 1,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit job definition tx");

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(CountingHandler {
        runs: Arc::clone(&runs),
    });

    let supervisor = Supervisor::builder(&pool, test_config())
        .expect("supervisor builder has runtime")
        .with_registry(registry)
        .build()
        .expect("supervisor should build");
    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new(SUPERVISOR_TEST_JOB),
            organization_id: None,
            payload: &json!({}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue supervisor test job");

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let job = get_job_by_id(&pool, None, job_id)
            .await
            .expect("load job by id")
            .expect("job should exist");
        if job.status == JobStatus::Succeeded {
            break;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for supervisor test job; last status was {:?}",
            job.status
        );
        sleep(Duration::from_millis(25)).await;
    }

    assert_eq!(runs.load(Ordering::SeqCst), 1);

    supervisor
        .shutdown_with_timeout(Duration::from_secs(10))
        .await
        .expect("supervisor should shut down cleanly");

    teardown_ephemeral_pool(pool, database).await;
}
