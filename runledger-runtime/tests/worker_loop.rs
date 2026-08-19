use std::future::pending;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use runledger_core::jobs::{JobCompletion, JobContext, JobFailure, JobStatus, JobType};
use runledger_postgres::jobs::{
    JobDefinitionUpsert, JobEnqueue, JobEnqueueIntent, JobEnqueueIntentStatus, enqueue_job,
    get_job_by_id, get_job_enqueue_intent_by_id, record_job_enqueue_intent,
    upsert_job_definition_tx,
};
use runledger_runtime::RuntimeLoopExit;
use runledger_runtime::config::JobsConfig;
use runledger_runtime::observer::{
    JobLifecycleObserver, JobLifecycleObservers, JobRunningEvent, JobSucceededEvent,
};
use runledger_runtime::registry::{JobHandler, JobRegistry};
use runledger_runtime::worker::{run_worker_loop, run_worker_loop_with_observer};
use serde_json::{Value, json};
use tokio::sync::{Notify, watch};
use tokio::time::{Instant, sleep, timeout};

use runledger_test_support::{
    setup_ephemeral_pool_with_untracked_migrations as setup_ephemeral_pool, teardown_ephemeral_pool,
};

struct BlockingHandler {
    runs: Arc<AtomicUsize>,
    release: Arc<Notify>,
}

struct CountingHandler {
    job_type: JobType<'static>,
    runs: Arc<AtomicUsize>,
}

struct HangingRunningObserver {
    calls: Arc<AtomicUsize>,
    started: Arc<Notify>,
}

struct SucceededObserver {
    calls: Arc<AtomicUsize>,
    notified: Arc<Notify>,
}

#[async_trait::async_trait]
impl JobHandler for BlockingHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.shutdown_wait")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        self.release.notified().await;
        Ok(JobCompletion::success())
    }
}

#[async_trait::async_trait]
impl JobHandler for CountingHandler {
    fn job_type(&self) -> JobType<'static> {
        self.job_type
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

#[async_trait::async_trait]
impl JobLifecycleObserver for HangingRunningObserver {
    async fn on_job_running(&self, _event: JobRunningEvent) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.started.notify_one();
        pending::<()>().await;
    }
}

#[async_trait::async_trait]
impl JobLifecycleObserver for SucceededObserver {
    async fn on_job_succeeded(&self, _event: JobSucceededEvent) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.notified.notify_one();
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
async fn worker_promotes_registered_intents_and_leaves_unregistered_types_pending() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_enqueue_intents", 8).await;
    let registered_type = JobType::new("jobs.test.intent_registered");
    let unregistered_type = JobType::new("jobs.test.intent_unregistered");
    let registered_payload = json!({"kind": "registered-intent"});
    let unregistered_payload = json!({"kind": "unregistered-intent"});

    let registered_intent = record_job_enqueue_intent(
        &pool,
        &JobEnqueueIntent::new(registered_type, &registered_payload, "registered-intent"),
    )
    .await
    .expect("record registered intent before definition");
    let unregistered_intent = record_job_enqueue_intent(
        &pool,
        &JobEnqueueIntent::new(
            unregistered_type,
            &unregistered_payload,
            "unregistered-intent",
        ),
    )
    .await
    .expect("record unregistered intent before definition");

    let mut tx = pool.begin().await.expect("begin definition transaction");
    for job_type in [registered_type, unregistered_type] {
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
        .expect("upsert intent job definition");
    }
    tx.commit().await.expect("commit definitions");

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(CountingHandler {
        job_type: registered_type,
        runs: Arc::clone(&runs),
    });
    let config = JobsConfig {
        worker_id: "intent-promotion-worker".to_owned(),
        poll_interval: Duration::from_millis(25),
        claim_batch_size: 2,
        lease_ttl_seconds: 30,
        max_global_concurrency: 2,
        reaper_interval: Duration::from_secs(30),
        schedule_poll_interval: Duration::from_secs(30),
        reaper_retry_delay_ms: 1_000,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_task = tokio::spawn(run_worker_loop(pool.clone(), registry, config, shutdown_rx));

    timeout(Duration::from_secs(5), async {
        while runs.load(Ordering::SeqCst) == 0 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("registered intent should be promoted and executed");

    shutdown_tx.send(true).expect("send worker shutdown");
    assert_eq!(
        timeout(Duration::from_secs(5), worker_task)
            .await
            .expect("worker shutdown timeout")
            .expect("worker task must not panic"),
        RuntimeLoopExit::Shutdown
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    let registered = get_job_enqueue_intent_by_id(&pool, None, registered_intent.intent_id)
        .await
        .expect("load registered intent")
        .expect("registered intent exists");
    assert_eq!(registered.status, JobEnqueueIntentStatus::Promoted);
    let job = get_job_by_id(
        &pool,
        None,
        registered.promoted_job_id.expect("promoted job id"),
    )
    .await
    .expect("load promoted job")
    .expect("promoted job exists");
    assert_eq!(job.status, JobStatus::Succeeded);

    let unregistered = get_job_enqueue_intent_by_id(&pool, None, unregistered_intent.intent_id)
        .await
        .expect("load unregistered intent")
        .expect("unregistered intent exists");
    assert_eq!(unregistered.status, JobEnqueueIntentStatus::Pending);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn worker_claims_existing_jobs_when_intent_promotion_fails() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_intent_promotion_failure", 8).await;
    let job_type = JobType::new("jobs.test.intent_promotion_failure");

    let mut tx = pool.begin().await.expect("begin definition transaction");
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
    tx.commit().await.expect("commit definition");

    let payload = json!({"kind": "survives-promotion-failure"});
    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type,
            organization_id: None,
            payload: &payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: Some("survives-promotion-failure"),
            stage: None,
        },
    )
    .await
    .expect("enqueue existing job");

    sqlx::query("DROP TABLE job_enqueue_intents")
        .execute(&pool)
        .await
        .expect("make intent promotion fail");

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(CountingHandler {
        job_type,
        runs: Arc::clone(&runs),
    });
    let config = JobsConfig {
        worker_id: "intent-promotion-failure-worker".to_owned(),
        poll_interval: Duration::from_millis(25),
        claim_batch_size: 1,
        lease_ttl_seconds: 30,
        max_global_concurrency: 1,
        reaper_interval: Duration::from_secs(30),
        schedule_poll_interval: Duration::from_secs(30),
        reaper_retry_delay_ms: 1_000,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_task = tokio::spawn(run_worker_loop(pool.clone(), registry, config, shutdown_rx));

    timeout(Duration::from_secs(5), async {
        while runs.load(Ordering::SeqCst) == 0 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("existing queue work should run despite promotion failure");

    shutdown_tx.send(true).expect("send worker shutdown");
    assert_eq!(
        timeout(Duration::from_secs(5), worker_task)
            .await
            .expect("worker shutdown timeout")
            .expect("worker task must not panic"),
        RuntimeLoopExit::Shutdown
    );
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let job = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(job.status, JobStatus::Succeeded);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn worker_promotes_intents_while_saturated_and_shutdown_interrupts_wait() {
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

    let saturated_intent_payload = json!({"kind": "queues-while-saturated"});
    let saturated_intent = record_job_enqueue_intent(
        &pool,
        &JobEnqueueIntent::new(
            JobType::new("jobs.test.shutdown_wait"),
            &saturated_intent_payload,
            "queues-while-saturated",
        ),
    )
    .await
    .expect("record intent while worker is saturated");

    sleep(poll_interval + Duration::from_millis(300)).await;

    let saturated_intent = get_job_enqueue_intent_by_id(&pool, None, saturated_intent.intent_id)
        .await
        .expect("load saturated intent")
        .expect("saturated intent exists");
    assert_eq!(saturated_intent.status, JobEnqueueIntentStatus::Promoted);
    assert_eq!(
        get_job_by_id(
            &pool,
            None,
            saturated_intent
                .promoted_job_id
                .expect("promoted intent links its queued job"),
        )
        .await
        .expect("load job queued while saturated")
        .expect("job queued while saturated exists")
        .status,
        JobStatus::Pending,
        "the ordinary queue, not execution permits, provides promotion backpressure"
    );

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
async fn worker_shutdown_delivers_terminal_success_when_running_observer_hangs() {
    let (pool, database) =
        setup_ephemeral_pool("jobs_worker_hung_running_terminal_success", 8).await;
    let job_type = JobType::new("jobs.test.hung_running_terminal_success");

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

    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type,
            organization_id: None,
            payload: &json!({"kind":"hung-running-terminal-success"}),
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
    let mut registry = JobRegistry::new();
    registry.register(CountingHandler {
        job_type,
        runs: runs.clone(),
    });

    let running_calls = Arc::new(AtomicUsize::new(0));
    let running_started = Arc::new(Notify::new());
    let succeeded_calls = Arc::new(AtomicUsize::new(0));
    let succeeded_notified = Arc::new(Notify::new());
    let observers = JobLifecycleObservers::from_arc_observers(vec![
        Arc::new(HangingRunningObserver {
            calls: running_calls.clone(),
            started: running_started.clone(),
        }) as Arc<dyn JobLifecycleObserver>,
        Arc::new(SucceededObserver {
            calls: succeeded_calls.clone(),
            notified: succeeded_notified.clone(),
        }) as Arc<dyn JobLifecycleObserver>,
    ]);

    let config = JobsConfig {
        worker_id: "hung-running-terminal-success-worker".to_string(),
        poll_interval: Duration::from_millis(25),
        claim_batch_size: 1,
        lease_ttl_seconds: 30,
        max_global_concurrency: 1,
        reaper_interval: Duration::from_secs(30),
        schedule_poll_interval: Duration::from_secs(30),
        reaper_retry_delay_ms: 1_000,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_task = tokio::spawn(run_worker_loop_with_observer(
        pool.clone(),
        registry,
        config,
        shutdown_rx,
        observers,
    ));

    timeout(Duration::from_secs(5), running_started.notified())
        .await
        .expect("running observer should start");

    let status_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let persisted = get_job_by_id(&pool, None, job_id)
            .await
            .expect("load job")
            .expect("job exists");
        if persisted.status == JobStatus::Succeeded {
            break;
        }
        assert!(
            Instant::now() < status_deadline,
            "timed out waiting for job to durably succeed"
        );
        sleep(Duration::from_millis(10)).await;
    }

    shutdown_tx
        .send(true)
        .expect("shutdown receiver should still be active");
    let exit = timeout(Duration::from_secs(15), worker_task)
        .await
        .expect("worker should shut down after the running observer timeout")
        .expect("worker task should not panic");
    assert_eq!(exit, RuntimeLoopExit::Shutdown);

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after shutdown")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Succeeded);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(running_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        succeeded_calls.load(Ordering::SeqCst),
        1,
        "terminal success observer should be delivered before shutdown returns"
    );

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

    let healthy_started = timeout(Duration::from_secs(2), async {
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
