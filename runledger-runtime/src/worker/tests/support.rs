use super::*;

pub(super) async fn enqueue_and_claim_job(
    pool: &PgPool,
    job_type: JobType<'static>,
    max_attempts: i32,
    payload: Value,
    worker_id: &str,
) -> (uuid::Uuid, runledger_postgres::jobs::JobQueueRecord) {
    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type,
            version: 1,
            max_attempts,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit tx");

    let job_id = enqueue_job(
        pool,
        &JobEnqueue {
            job_type,
            organization_id: None,
            payload: &payload,
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

    let claimed_job = claim_one_job(pool, worker_id).await;
    (job_id, claimed_job)
}

pub(super) async fn connect_closed_pool(database_url: &str) -> PgPool {
    let worker_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(database_url)
        .await
        .expect("connect worker pool");
    worker_pool.close().await;
    worker_pool
}

pub(super) async fn expire_job_lease(pool: &PgPool, job_id: uuid::Uuid) {
    sqlx::query(
        "UPDATE job_queue
         SET lease_expires_at = now() - interval '10 seconds'
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(pool)
    .await
    .expect("expire leased job");
}

pub(super) async fn wait_for_heartbeat_to_block_on_job_lock(pool: &PgPool) {
    for _ in 0..100 {
        let waiting = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_stat_activity
                 WHERE wait_event_type = 'Lock'
                   AND query LIKE '%UPDATE job_queue%'
                   AND query LIKE '%make_interval%'
                   AND query NOT LIKE '%pg_stat_activity%'
             )",
        )
        .fetch_one(pool)
        .await
        .expect("query waiting heartbeat activity");

        if waiting {
            return;
        }

        sleep(Duration::from_millis(50)).await;
    }

    panic!("heartbeat did not block on the job-row lock");
}

pub(super) async fn wait_for_status(
    pool: &PgPool,
    job_id: uuid::Uuid,
    expected: JobStatus,
    timeout_after: Duration,
) -> runledger_postgres::jobs::JobQueueRecord {
    let deadline = Instant::now() + timeout_after;

    loop {
        let job = get_job_by_id(pool, None, job_id)
            .await
            .expect("load job")
            .expect("job exists");
        if job.status == expected {
            return job;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for {expected:?}; last observed status was {:?}",
            job.status
        );
        sleep(Duration::from_millis(25)).await;
    }
}

pub(super) async fn wait_for_counter_at_least(
    counter: &AtomicUsize,
    expected: usize,
    timeout_after: Duration,
) -> bool {
    let deadline = Instant::now() + timeout_after;

    loop {
        if counter.load(Ordering::SeqCst) >= expected {
            return true;
        }

        if Instant::now() >= deadline {
            return false;
        }

        sleep(Duration::from_millis(10)).await;
    }
}

pub(super) async fn wait_for_observer_count(
    mut count_events: impl FnMut() -> usize,
    expected: usize,
    timeout_after: Duration,
) {
    let deadline = Instant::now() + timeout_after;

    loop {
        let observed = count_events();
        if observed >= expected {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for {expected} observer event(s); last observed count was {observed}"
        );
        sleep(Duration::from_millis(10)).await;
    }
}

pub(super) fn query_error_code(error: &runledger_postgres::Error) -> Option<&str> {
    match error {
        runledger_postgres::Error::QueryError(query_error) => Some(query_error.code()),
        _ => None,
    }
}

pub(super) fn clone_dead_letters(
    dead_letters: &Arc<Mutex<Vec<JobDeadLetterInfo>>>,
) -> Vec<JobDeadLetterInfo> {
    dead_letters
        .lock()
        .expect("dead-letter list lock should not be poisoned")
        .clone()
}

pub(super) async fn database_now(pool: &PgPool) -> DateTime<Utc> {
    sqlx::query_scalar::<_, DateTime<Utc>>("SELECT clock_timestamp()")
        .fetch_one(pool)
        .await
        .expect("fetch database now")
}

pub(super) async fn observe_retry_delay_override_failure<F>(
    database_name: &str,
    handler_job_type: &'static str,
    failure: F,
    max_attempts: i32,
    override_registration: Option<(JobType<'static>, &'static str, i32)>,
) -> RetryDelayOverrideObservation
where
    F: FnOnce(DateTime<Utc>) -> JobFailure,
{
    let (pool, database) = setup_ephemeral_pool(database_name, 8).await;
    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new(handler_job_type),
        max_attempts,
        json!({"kind":"retry-delay-override"}),
        "worker-retry-delay-override",
    )
    .await;

    let default_retry_delay_ms = compute_retry_delay_ms(claimed_job.attempt, claimed_job.id);
    let db_now_before = database_now(&pool).await;
    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(FailingHandler {
        job_type_name: handler_job_type,
        failure: failure(db_now_before),
        runs: runs.clone(),
    });
    if let Some((job_type, failure_code, retry_delay_ms)) = override_registration {
        registry.register_retry_delay_override(job_type, failure_code, retry_delay_ms);
    }

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;
    let db_now_after = database_now(&pool).await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after failure")
        .expect("job exists");
    let events = list_job_events(&pool, None, job_id, 50, None)
        .await
        .expect("list job events");
    let retry_events = events
        .iter()
        .filter(|event| event.event_type == JobEventType::RetryScheduled)
        .collect::<Vec<_>>();
    let retry_event_delay_ms = retry_events
        .first()
        .and_then(|event| event.payload.get("retry_delay_ms"))
        .and_then(Value::as_i64);
    let retry_event_requested_retry_at = retry_events
        .first()
        .and_then(|event| event.payload.get("requested_retry_not_before"))
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<DateTime<Utc>>().ok());
    let failed_event_error_code = events
        .iter()
        .find(|event| event.event_type == JobEventType::Failed)
        .and_then(|event| event.payload.get("error_code"))
        .and_then(Value::as_str)
        .map(str::to_owned);
    let attempt_retry_delay_ms = sqlx::query_scalar::<_, Option<i32>>(
        "SELECT retry_delay_ms
         FROM job_attempts
         WHERE job_id = $1
           AND run_number = 1
           AND attempt = 1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("fetch attempt retry delay");

    let observation = RetryDelayOverrideObservation {
        status: persisted.status,
        next_run_at: persisted.next_run_at,
        retry_event_delay_ms,
        retry_event_requested_retry_at,
        retry_event_count: retry_events.len(),
        failed_event_error_code,
        attempt_retry_delay_ms,
        default_retry_delay_ms,
        db_now_before,
        db_now_after,
        runs: runs.load(Ordering::SeqCst),
    };

    teardown_ephemeral_pool(pool, database).await;
    observation
}
