use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use runledger_core::jobs::{
    JobCompletion, JobContext, JobDeadLetterInfo, JobFailure, JobStatus, JobType,
};
use runledger_postgres::jobs::{
    JobDefinitionUpsert, JobEnqueue, JobProgressUpdate, claim_jobs_for_types, enqueue_job,
    get_job_by_id, update_job_progress, upsert_job_definition_tx,
};
use runledger_runtime::config::JobsConfig;
use runledger_runtime::observer::{
    JobLeaseReapedEvent, JobLifecycleObserver, JobLifecycleObservers,
};
use runledger_runtime::reaper::{run_reaper_loop, run_reaper_loop_with_observer};
use runledger_runtime::registry::{JobHandler, JobRegistry};
use serde_json::{Value, json};
use tokio::sync::{Notify, watch};
use tokio::time::{Instant, sleep, timeout};

use runledger_test_support::{
    setup_ephemeral_pool_with_untracked_migrations as setup_ephemeral_pool, teardown_ephemeral_pool,
};

struct ShutdownAwareTerminalHookHandler {
    terminal_starts: Arc<AtomicUsize>,
    terminal_completions: Arc<AtomicUsize>,
    release: Arc<Notify>,
}

struct PendingReapedObserver {
    started: Arc<Notify>,
    started_count: Arc<AtomicUsize>,
    active_callbacks: Arc<AtomicUsize>,
}

struct ActiveObserverGuard {
    active_callbacks: Arc<AtomicUsize>,
}

impl Drop for ActiveObserverGuard {
    fn drop(&mut self) {
        self.active_callbacks.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl JobHandler for ShutdownAwareTerminalHookHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.reaper.hook.shutdown.persist")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        Ok(JobCompletion::success())
    }

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        _payload: Value,
        _dead_letter: JobDeadLetterInfo,
    ) {
        self.terminal_starts.fetch_add(1, Ordering::SeqCst);
        self.release.notified().await;
        self.terminal_completions.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl JobLifecycleObserver for PendingReapedObserver {
    async fn on_job_lease_reaped(&self, _event: JobLeaseReapedEvent) {
        self.started_count.fetch_add(1, Ordering::SeqCst);
        self.active_callbacks.fetch_add(1, Ordering::SeqCst);
        let _guard = ActiveObserverGuard {
            active_callbacks: self.active_callbacks.clone(),
        };
        self.started.notify_waiters();
        pending::<()>().await;
    }
}

async fn wait_for_observer_count(
    counter: &AtomicUsize,
    notify: &Notify,
    expected: usize,
    timeout_after: Duration,
) {
    timeout(timeout_after, async {
        while counter.load(Ordering::SeqCst) < expected {
            notify.notified().await;
        }
    })
    .await
    .expect("timed out waiting for reaped observer count");
}

async fn wait_for_status(
    pool: &sqlx::PgPool,
    job_id: sqlx::types::Uuid,
    expected: JobStatus,
    timeout_after: Duration,
) {
    timeout(timeout_after, async {
        loop {
            let persisted = get_job_by_id(pool, None, job_id)
                .await
                .expect("load job")
                .expect("job exists");
            if persisted.status == expected {
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("timed out waiting for job status");
}

#[tokio::test]
async fn run_reaper_loop_reaps_later_batches_while_reaped_observer_is_pending() {
    let (pool, database) = setup_ephemeral_pool("runtime_reaper_observer_nonblocking", 8).await;
    let job_type = JobType::new("jobs.test.reaper.observer.pending");

    let mut tx = pool.begin().await.expect("begin tx");
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
    tx.commit().await.expect("commit tx");

    let first_job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type,
            organization_id: None,
            payload: &json!({"kind":"observer-nonblocking", "index": 1}),
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
            job_type,
            organization_id: None,
            payload: &json!({"kind":"observer-nonblocking", "index": 2}),
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

    let claimed = claim_jobs_for_types(
        &pool,
        "reaper-observer-nonblocking-worker",
        60,
        2,
        &[job_type],
    )
    .await
    .expect("claim jobs for reaper observer nonblocking test");
    assert_eq!(claimed.len(), 2);

    sqlx::query(
        "UPDATE job_queue
         SET lease_expires_at = now() - interval '10 seconds'
         WHERE id = ANY($1)",
    )
    .bind(&[first_job_id, second_job_id][..])
    .execute(&pool)
    .await
    .expect("expire leased jobs");

    let observer_started = Arc::new(Notify::new());
    let observer_started_count = Arc::new(AtomicUsize::new(0));
    let active_callbacks = Arc::new(AtomicUsize::new(0));
    let observers = JobLifecycleObservers::from_observer(PendingReapedObserver {
        started: observer_started.clone(),
        started_count: observer_started_count.clone(),
        active_callbacks: active_callbacks.clone(),
    });
    let config = JobsConfig {
        worker_id: "runtime-reaper-test".to_string(),
        poll_interval: Duration::from_secs(30),
        claim_batch_size: 1,
        lease_ttl_seconds: 30,
        max_global_concurrency: 1,
        reaper_interval: Duration::from_millis(10),
        schedule_poll_interval: Duration::from_secs(30),
        reaper_retry_delay_ms: 1_000,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut task = tokio::spawn(run_reaper_loop_with_observer(
        pool.clone(),
        JobRegistry::new(),
        config,
        shutdown_rx,
        observers,
    ));

    wait_for_observer_count(
        &observer_started_count,
        &observer_started,
        1,
        Duration::from_secs(2),
    )
    .await;
    assert_eq!(active_callbacks.load(Ordering::SeqCst), 1);

    wait_for_status(
        &pool,
        second_job_id,
        JobStatus::Pending,
        Duration::from_secs(2),
    )
    .await;
    assert!(
        active_callbacks.load(Ordering::SeqCst) > 0,
        "later reaper batches should run while the earlier reaped observer callback is still active"
    );

    shutdown_tx
        .send(true)
        .expect("shutdown receiver should still be active");
    timeout(Duration::from_secs(2), &mut task)
        .await
        .expect("reaper loop should exit after shutdown")
        .expect("reaper loop joins cleanly");
    assert_eq!(active_callbacks.load(Ordering::SeqCst), 0);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn run_reaper_loop_shutdown_waits_for_inflight_terminal_hook_delivery() {
    let (pool, database) = setup_ephemeral_pool("runtime_reaper_hook_delivery", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.reaper.hook.shutdown.persist"),
            version: 1,
            max_attempts: 1,
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
            job_type: JobType::new("jobs.test.reaper.hook.shutdown.persist"),
            organization_id: None,
            payload: &json!({"kind":"hook-shutdown-persist"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue shutdown-persist job");

    let claimed = claim_jobs_for_types(
        &pool,
        "reaper-hook-shutdown-persist-worker",
        60,
        1,
        &[JobType::new("jobs.test.reaper.hook.shutdown.persist")],
    )
    .await
    .expect("claim shutdown-persist job");
    assert_eq!(claimed.len(), 1);
    let claimed_job = claimed.first().expect("claimed job exists");

    update_job_progress(
        &pool,
        claimed_job.id,
        claimed_job.run_number,
        claimed_job.attempt,
        claimed_job.worker_id.as_deref().expect("worker id is set"),
        &JobProgressUpdate {
            stage: Some(runledger_core::jobs::JobStage::Running),
            progress_done: None,
            progress_total: None,
            checkpoint: None,
        },
    )
    .await
    .expect("persist running stage before expiring lease");

    sqlx::query(
        "UPDATE job_queue
         SET lease_expires_at = now() - interval '10 seconds'
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .expect("expire leased shutdown-persist job");

    let terminal_starts = Arc::new(AtomicUsize::new(0));
    let terminal_completions = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let mut registry = JobRegistry::new();
    registry.register(ShutdownAwareTerminalHookHandler {
        terminal_starts: terminal_starts.clone(),
        terminal_completions: terminal_completions.clone(),
        release: release.clone(),
    });

    let config = JobsConfig {
        worker_id: "runtime-reaper-test".to_string(),
        poll_interval: Duration::from_secs(30),
        claim_batch_size: 1,
        lease_ttl_seconds: 30,
        max_global_concurrency: 1,
        reaper_interval: Duration::from_secs(5),
        schedule_poll_interval: Duration::from_secs(30),
        reaper_retry_delay_ms: 1_000,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(run_reaper_loop(pool.clone(), registry, config, shutdown_rx));

    let status_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let persisted = get_job_by_id(&pool, None, job_id)
            .await
            .expect("load job")
            .expect("job exists");
        if persisted.status == JobStatus::DeadLettered {
            break;
        }
        assert!(
            Instant::now() < status_deadline,
            "timed out waiting for reaper to dead-letter job"
        );
        sleep(Duration::from_millis(10)).await;
    }

    let _ = shutdown_tx.send(true);

    let hook_start_deadline = Instant::now() + Duration::from_secs(2);
    while terminal_starts.load(Ordering::SeqCst) == 0 {
        assert!(
            Instant::now() < hook_start_deadline,
            "terminal hook should start after a committed terminal reaper batch even when shutdown is requested"
        );
        sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(terminal_starts.load(Ordering::SeqCst), 1);

    release.notify_waiters();

    timeout(Duration::from_secs(2), task)
        .await
        .expect("reaper loop should exit after inflight hook completes")
        .expect("reaper loop joins cleanly");

    assert_eq!(
        terminal_completions.load(Ordering::SeqCst),
        1,
        "shutdown must not drop terminal hook delivery for already reaped jobs"
    );

    teardown_ephemeral_pool(pool, database).await;
}
