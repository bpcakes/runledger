use super::*;

const JOB_TYPE: JobType<'static> = JobType::new("jobs.test.heartbeat_progress_lock_race");
const TIMEOUT_JOB_TYPE: JobType<'static> = JobType::new("jobs.test.heartbeat_lock_timeout");
const LEASE_TTL_SECONDS: i32 = 10;

struct ProgressDuringHeartbeatHandler {
    pool: PgPool,
    settle_after_progress: Duration,
}

struct PendingHeartbeatTimeoutHandler {
    started: Arc<Notify>,
    drops: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl JobHandler for ProgressDuringHeartbeatHandler {
    fn job_type(&self) -> JobType<'static> {
        JOB_TYPE
    }

    async fn execute(
        &self,
        context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        update_job_ordinary_progress(
            &self.pool,
            context.job_id,
            context.run_number,
            context.attempt,
            &context.worker_id,
            &JobOrdinaryProgressUpdate {
                progress_done: Some(1),
                progress_total: Some(2),
                checkpoint: None,
            },
        )
        .await
        .expect("persist progress while heartbeat is due");

        sleep(self.settle_after_progress).await;

        Ok(JobCompletion::success())
    }
}

#[async_trait::async_trait]
impl JobHandler for PendingHeartbeatTimeoutHandler {
    fn job_type(&self) -> JobType<'static> {
        TIMEOUT_JOB_TYPE
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        let _drop_notify = DropNotify {
            drops: Arc::clone(&self.drops),
        };
        self.started.notify_one();
        pending().await
    }
}

#[tokio::test]
async fn pending_heartbeat_keeps_polling_handler_that_owns_the_job_row_lock() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_heartbeat_progress_lock", 8).await;
    record_postgres_server_version(&pool, "heartbeat/progress row-lock regression").await;

    sqlx::query(
        "CREATE FUNCTION runledger_test_delay_progress_update()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             IF NEW.progress_done IS DISTINCT FROM OLD.progress_done
                AND NEW.progress_done = 1 THEN
                 PERFORM pg_sleep(4);
             END IF;
             RETURN NEW;
         END;
         $$",
    )
    .execute(&pool)
    .await
    .expect("create progress delay trigger function");
    sqlx::query(
        "CREATE TRIGGER runledger_test_delay_progress_update
         BEFORE UPDATE OF progress_done ON job_queue
         FOR EACH ROW
         EXECUTE FUNCTION runledger_test_delay_progress_update()",
    )
    .execute(&pool)
    .await
    .expect("create progress delay trigger");

    let (job_id, claimed_job) = enqueue_and_claim_job_with_lease_ttl(
        &pool,
        JOB_TYPE,
        3,
        json!({"kind": "heartbeat-progress-lock-race"}),
        "worker-heartbeat-progress-lock-race",
        LEASE_TTL_SECONDS,
    )
    .await;
    let mut registry = JobRegistry::new();
    registry.register(ProgressDuringHeartbeatHandler {
        pool: pool.clone(),
        settle_after_progress: Duration::ZERO,
    });

    // A ten-second lease TTL makes the first heartbeat due after three seconds,
    // while the trigger keeps the progress UPDATE holding the job-row lock for
    // four seconds. Claim and processing use the same TTL, and the
    // pg_stat_activity observation proves the heartbeat and progress operations
    // overlap on that exact lock.
    let process_pool = pool.clone();
    let mut process_task = tokio::spawn(async move {
        process_claimed_job(
            process_pool,
            Arc::new(registry),
            claimed_job,
            LEASE_TTL_SECONDS,
        )
        .await;
    });
    wait_for_heartbeat_to_block_on_job_lock(&pool).await;

    await_spawned_task(
        &mut process_task,
        Duration::from_secs(10),
        "worker self-deadlocked while progress held the heartbeat job-row lock",
        "job processing task should not panic",
    )
    .await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after heartbeat/progress race")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Succeeded);
    assert_eq!(persisted.progress_done, Some(1));
    assert_eq!(persisted.progress_total, Some(2));

    let cleanup_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let stranded_sessions = sqlx::query_scalar::<_, i64>(
            "SELECT count(*)
             FROM pg_stat_activity
             WHERE datname = current_database()
               AND pid <> pg_backend_pid()
               AND (
                    state = 'idle in transaction'
                    OR (
                        wait_event_type = 'Lock'
                        AND query LIKE '%UPDATE job_queue%'
                        AND query LIKE '%make_interval%'
                    )
               )",
        )
        .fetch_one(&pool)
        .await
        .expect("inspect heartbeat/progress cleanup");
        if stranded_sessions == 0 {
            break;
        }
        assert!(
            Instant::now() < cleanup_deadline,
            "heartbeat/progress race left {stranded_sessions} blocked or idle-in-transaction session(s)"
        );
        sleep(Duration::from_millis(20)).await;
    }

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn transient_heartbeat_lock_timeouts_retry_within_one_maintenance_budget() {
    const RETRY_TEST_LEASE_TTL_SECONDS: i32 = 3;

    let (pool, database) = setup_ephemeral_pool("jobs_worker_heartbeat_lock_retry", 8).await;
    record_postgres_server_version(&pool, "heartbeat lock-timeout retry regression").await;

    sqlx::query(
        "CREATE FUNCTION runledger_test_delay_retry_progress_update()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             IF NEW.progress_done IS DISTINCT FROM OLD.progress_done
                AND NEW.progress_done = 1 THEN
                 PERFORM pg_sleep(1.4);
             END IF;
             RETURN NEW;
         END;
         $$",
    )
    .execute(&pool)
    .await
    .expect("create retry progress delay trigger function");
    sqlx::query(
        "CREATE TRIGGER runledger_test_delay_retry_progress_update
         BEFORE UPDATE OF progress_done ON job_queue
         FOR EACH ROW
         EXECUTE FUNCTION runledger_test_delay_retry_progress_update()",
    )
    .execute(&pool)
    .await
    .expect("create retry progress delay trigger");

    let (job_id, claimed_job) = enqueue_and_claim_job_with_lease_ttl(
        &pool,
        JOB_TYPE,
        3,
        json!({"kind": "heartbeat-lock-retry"}),
        "worker-heartbeat-lock-retry",
        RETRY_TEST_LEASE_TTL_SECONDS,
    )
    .await;
    let worker_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database.url())
        .await
        .expect("connect heartbeat-retry worker pool");
    sqlx::query("SET SESSION lock_timeout = '100ms'")
        .execute(&worker_pool)
        .await
        .expect("set strict heartbeat retry lock timeout");

    let mut registry = JobRegistry::new();
    registry.register(ProgressDuringHeartbeatHandler {
        pool: pool.clone(),
        settle_after_progress: Duration::from_millis(500),
    });
    let process_pool = worker_pool.clone();
    let mut process_task = tokio::spawn(async move {
        process_claimed_job(
            process_pool,
            Arc::new(registry),
            claimed_job,
            RETRY_TEST_LEASE_TTL_SECONDS,
        )
        .await;
    });

    wait_for_heartbeat_to_block_on_job_lock(&pool).await;
    await_spawned_task(
        &mut process_task,
        Duration::from_secs(4),
        "worker did not recover from transient heartbeat lock contention",
        "heartbeat-retry worker task should not panic",
    )
    .await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after heartbeat retry")
        .expect("job exists after heartbeat retry");
    assert_eq!(persisted.status, JobStatus::Succeeded);
    assert!(
        list_job_events(&pool, None, job_id, 20, None)
            .await
            .expect("list heartbeat-retry events")
            .iter()
            .any(|event| event.event_type == JobEventType::Heartbeat),
        "a heartbeat should persist after transient lock timeouts clear"
    );

    worker_pool.close().await;
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn heartbeat_budget_aborts_handler_before_short_lease_expires() {
    const TIMEOUT_TEST_LEASE_TTL_SECONDS: i32 = 3;

    let (pool, database) = setup_ephemeral_pool("jobs_worker_heartbeat_timeout", 8).await;
    record_postgres_server_version(&pool, "heartbeat lease-budget recovery regression").await;
    let (job_id, claimed_job) = enqueue_and_claim_job_with_lease_ttl(
        &pool,
        TIMEOUT_JOB_TYPE,
        3,
        json!({"kind": "heartbeat-lock-timeout"}),
        "worker-heartbeat-lock-timeout",
        TIMEOUT_TEST_LEASE_TTL_SECONDS,
    )
    .await;

    let worker_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(database.url())
        .await
        .expect("connect heartbeat-budget worker pool");
    assert_eq!(
        sqlx::query_scalar::<_, String>("SHOW lock_timeout")
            .fetch_one(&worker_pool)
            .await
            .expect("read default worker lock timeout"),
        "0"
    );

    let started = Arc::new(Notify::new());
    let drops = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(PendingHeartbeatTimeoutHandler {
        started: Arc::clone(&started),
        drops: Arc::clone(&drops),
    });
    let observer = RecordingObserver::default();
    let process_observers = observer.lifecycle_observers();
    let process_pool = worker_pool.clone();
    let mut process_task = tokio::spawn(async move {
        process_claimed_job_with_observer(
            process_pool,
            Arc::new(registry),
            claimed_job,
            TIMEOUT_TEST_LEASE_TTL_SECONDS,
            process_observers,
        )
        .await;
    });

    timeout(Duration::from_secs(2), started.notified())
        .await
        .expect("handler should start before its first heartbeat");
    let mut lock_tx = pool.begin().await.expect("begin heartbeat blocker tx");
    sqlx::query("SELECT id FROM job_queue WHERE id = $1 FOR UPDATE")
        .bind(job_id)
        .fetch_one(&mut *lock_tx)
        .await
        .expect("hold heartbeat job-row lock");
    wait_for_heartbeat_to_block_on_job_lock(&pool).await;

    await_spawned_task(
        &mut process_task,
        Duration::from_secs(3),
        "worker should abort within the lease-aware heartbeat budget",
        "heartbeat-budget worker task should not panic",
    )
    .await;
    wait_for_observer_count(
        || observer.lease_lost().len(),
        1,
        Duration::from_millis(500),
    )
    .await;

    assert_eq!(drops.load(Ordering::SeqCst), 1, "handler should be dropped");
    let lease_lost = observer.lease_lost();
    assert_eq!(lease_lost.len(), 1);
    assert_eq!(lease_lost[0].job.job_id, job_id);
    assert_eq!(lease_lost[0].failure.kind, JobFailureKind::LeaseExpired);
    assert_eq!(lease_lost[0].failure.code, "job.lease_maintenance_failed");

    let abandoned = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after heartbeat timeout")
        .expect("job exists after heartbeat timeout");
    assert_eq!(abandoned.status, JobStatus::Leased);
    assert_eq!(abandoned.attempt, 1);
    assert!(
        abandoned.lease_expires_at.expect("abandoned lease expiry") > Utc::now(),
        "handler must stop before durable lease ownership expires"
    );

    assert_eq!(
        timeout(
            Duration::from_secs(6),
            sqlx::query_scalar::<_, String>("SHOW lock_timeout").fetch_one(&worker_pool),
        )
        .await
        .expect("timed-out heartbeat connection should become reusable while the blocker is held")
        .expect("probe recovered heartbeat worker connection"),
        "0"
    );

    lock_tx.rollback().await.expect("release heartbeat blocker");
    expire_job_lease(&pool, job_id).await;
    assert_eq!(
        reap_expired_leases(&pool, 1, 1_000)
            .await
            .expect("reap heartbeat-timeout lease"),
        1
    );

    let recovered = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load reaped heartbeat-timeout job")
        .expect("reaped job exists");
    assert_eq!(recovered.status, JobStatus::Pending);
    assert_eq!(recovered.attempt, 1, "the expired attempt remains recorded");
    let (attempt_finished, attempt_outcome, attempt_error_code) =
        sqlx::query_as::<_, (bool, Option<String>, Option<String>)>(
            "SELECT finished_at IS NOT NULL, outcome::text, error_code
             FROM job_attempts
             WHERE job_id = $1
               AND run_number = $2
               AND attempt = $3",
        )
        .bind(job_id)
        .bind(recovered.run_number)
        .bind(recovered.attempt)
        .fetch_one(&pool)
        .await
        .expect("load reaped attempt outcome");
    assert!(attempt_finished);
    assert_eq!(attempt_outcome.as_deref(), Some("LEASE_EXPIRED"));
    assert_eq!(attempt_error_code.as_deref(), Some("job.lease_expired"));

    worker_pool.close().await;
    teardown_ephemeral_pool(pool, database).await;
}
