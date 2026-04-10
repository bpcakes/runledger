use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use runledger_core::jobs::{JobContext, JobFailure, JobStatus, JobType};
use runledger_postgres::jobs::{
    JobDefinitionUpsert, JobEnqueue, enqueue_job, get_job_by_id, upsert_job_definition_tx,
};
use runledger_runtime::config::JobsConfig;
use runledger_runtime::registry::{JobHandler, JobRegistry};
use runledger_runtime::worker::run_worker_loop;
use serde_json::{Value, json};
use tokio::sync::{Notify, watch};
use tokio::time::{Instant, sleep, timeout};

#[path = "../test_support.rs"]
mod test_support;

use test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};

struct BlockingHandler {
    runs: Arc<AtomicUsize>,
    release: Arc<Notify>,
}

struct CountingHandler {
    job_type: JobType<'static>,
    runs: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl JobHandler for BlockingHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.shutdown_wait")
    }

    async fn execute(&self, _context: JobContext, _payload: Value) -> Result<(), JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        self.release.notified().await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl JobHandler for CountingHandler {
    fn job_type(&self) -> JobType<'static> {
        self.job_type
    }

    async fn execute(&self, _context: JobContext, _payload: Value) -> Result<(), JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn fail_stage_changed_inserts_for_job(pool: &sqlx::PgPool, job_id: uuid::Uuid) {
    let function_sql = format!(
        "CREATE OR REPLACE FUNCTION fail_stage_changed_for_job_for_tests()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             IF NEW.event_type = 'STAGE_CHANGED'::job_event_type
                AND NEW.job_id = '{job_id}'::uuid THEN
                 RAISE EXCEPTION 'forced stage-changed insert failure';
             END IF;

             RETURN NEW;
         END;
         $$"
    );

    sqlx::query(&function_sql)
        .execute(pool)
        .await
        .expect("create failing stage-changed trigger function");

    sqlx::query(
        "CREATE TRIGGER trg_fail_stage_changed_for_job_for_tests
         BEFORE INSERT ON job_events
         FOR EACH ROW
         EXECUTE FUNCTION fail_stage_changed_for_job_for_tests()",
    )
    .execute(pool)
    .await
    .expect("create failing stage-changed trigger");
}

#[tokio::test]
async fn worker_shutdown_interrupts_poll_wait_when_no_permits_available() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_shutdown_wait", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.shutdown_wait"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.shutdown_wait"),
            organization_id: None,
            payload: &json!({"kind":"shutdown-wait"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");

    let runs = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let mut registry = JobRegistry::new();
    registry.register(BlockingHandler {
        runs: runs.clone(),
        release: release.clone(),
    });

    let poll_interval = Duration::from_secs(3);
    let config = JobsConfig {
        worker_id: "shutdown-wait-worker".to_string(),
        poll_interval,
        claim_batch_size: 1,
        lease_ttl_seconds: 30,
        max_global_concurrency: 1,
        reaper_interval: Duration::from_secs(30),
        schedule_poll_interval: Duration::from_secs(30),
        reaper_retry_delay_ms: 1_000,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_task = tokio::spawn(run_worker_loop(pool.clone(), registry, config, shutdown_rx));

    let start_deadline = Instant::now() + Duration::from_secs(5);
    while runs.load(Ordering::SeqCst) == 0 {
        assert!(
            Instant::now() < start_deadline,
            "timed out waiting for worker to start job"
        );
        sleep(Duration::from_millis(25)).await;
    }

    sleep(poll_interval + Duration::from_millis(300)).await;

    let shutdown_sent_at = Instant::now();
    let _ = shutdown_tx.send(true);
    release.notify_waiters();

    let prompt_shutdown_window = Duration::from_secs(2);

    timeout(prompt_shutdown_window, worker_task)
        .await
        .expect("worker should exit promptly once shutdown is signaled while saturated")
        .expect("worker join should succeed");

    assert!(
        shutdown_sent_at.elapsed() < prompt_shutdown_window,
        "worker shutdown was delayed despite shutdown-aware saturated wait path"
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Succeeded);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn worker_claims_next_batch_without_poll_delay_when_batch_is_full() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_batch_fill", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.shutdown_wait"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let first_job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.shutdown_wait"),
            organization_id: None,
            payload: &json!({"kind":"batch-fill-1"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue first job");

    let second_job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.shutdown_wait"),
            organization_id: None,
            payload: &json!({"kind":"batch-fill-2"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue second job");

    let runs = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let mut registry = JobRegistry::new();
    registry.register(BlockingHandler {
        runs: runs.clone(),
        release: release.clone(),
    });

    let poll_interval = Duration::from_secs(2);
    let config = JobsConfig {
        worker_id: "batch-fill-worker".to_string(),
        poll_interval,
        claim_batch_size: 1,
        lease_ttl_seconds: 30,
        max_global_concurrency: 2,
        reaper_interval: Duration::from_secs(30),
        schedule_poll_interval: Duration::from_secs(30),
        reaper_retry_delay_ms: 1_000,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_task = tokio::spawn(run_worker_loop(pool.clone(), registry, config, shutdown_rx));

    let second_start_deadline = Instant::now() + Duration::from_millis(1_500);
    while runs.load(Ordering::SeqCst) < 2 {
        assert!(
            Instant::now() < second_start_deadline,
            "timed out waiting for second job; worker likely slept for poll_interval after a full claim batch"
        );
        sleep(Duration::from_millis(25)).await;
    }

    let _ = shutdown_tx.send(true);
    release.notify_waiters();

    timeout(Duration::from_secs(2), worker_task)
        .await
        .expect("worker should exit promptly after shutdown")
        .expect("worker join should succeed");

    assert_eq!(runs.load(Ordering::SeqCst), 2);

    for job_id in [first_job_id, second_job_id] {
        let persisted = get_job_by_id(&pool, None, job_id)
            .await
            .expect("load job")
            .expect("job exists");
        assert_eq!(persisted.status, JobStatus::Succeeded);
    }

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn worker_does_not_starve_other_jobs_when_running_progress_persist_keeps_failing() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_progress_starvation", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    for job_type in [
        JobType::new("jobs.test.poison_progress_failure"),
        JobType::new("jobs.test.healthy_after_poison"),
    ] {
        upsert_job_definition_tx(
            &mut tx,
            &JobDefinitionUpsert {
                job_type,
                version: 1,
                max_attempts: 3,
                default_timeout_seconds: 30,
                default_priority: 100,
                is_enabled: true,
            },
        )
        .await
        .expect("upsert job definition");
    }
    tx.commit().await.expect("commit tx");

    let poison_job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.poison_progress_failure"),
            organization_id: None,
            payload: &json!({"kind":"poison"}),
            priority: Some(200),
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: Some("poison-progress-failure"),
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue poison job");

    let healthy_job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.healthy_after_poison"),
            organization_id: None,
            payload: &json!({"kind":"healthy"}),
            priority: Some(100),
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: Some("healthy-after-poison"),
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue healthy job");

    fail_stage_changed_inserts_for_job(&pool, poison_job_id).await;

    let poison_runs = Arc::new(AtomicUsize::new(0));
    let healthy_runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(CountingHandler {
        job_type: JobType::new("jobs.test.poison_progress_failure"),
        runs: poison_runs.clone(),
    });
    registry.register(CountingHandler {
        job_type: JobType::new("jobs.test.healthy_after_poison"),
        runs: healthy_runs.clone(),
    });

    let config = JobsConfig {
        worker_id: "poison-starvation-worker".to_string(),
        poll_interval: Duration::from_secs(3),
        claim_batch_size: 1,
        lease_ttl_seconds: 30,
        max_global_concurrency: 2,
        reaper_interval: Duration::from_secs(30),
        schedule_poll_interval: Duration::from_secs(30),
        reaper_retry_delay_ms: 1_000,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_task = tokio::spawn(run_worker_loop(pool.clone(), registry, config, shutdown_rx));

    let healthy_started = timeout(Duration::from_millis(750), async {
        while healthy_runs.load(Ordering::SeqCst) == 0 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();

    let _ = shutdown_tx.send(true);
    timeout(Duration::from_secs(2), worker_task)
        .await
        .expect("worker should exit after shutdown")
        .expect("worker task should join");

    assert!(
        healthy_started,
        "healthy job should still run even if a higher-priority job keeps failing before RUNNING persists"
    );
    assert_eq!(
        poison_runs.load(Ordering::SeqCst),
        0,
        "poison job handler should never start when running progress persistence fails"
    );

    let poison = get_job_by_id(&pool, None, poison_job_id)
        .await
        .expect("load poison job")
        .expect("poison job exists");
    assert_eq!(poison.status, JobStatus::Pending);

    let healthy = get_job_by_id(&pool, None, healthy_job_id)
        .await
        .expect("load healthy job")
        .expect("healthy job exists");
    assert_eq!(healthy.status, JobStatus::Succeeded);

    teardown_ephemeral_pool(pool, database).await;
}
