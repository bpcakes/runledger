use std::future::pending;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use runledger_core::jobs::{
    JobCompletion, JobContext, JobDeadLetterInfo, JobDeadLetterReason, JobEventType, JobFailure,
    JobFailureKind, JobStatus, JobType,
};
use runledger_postgres::jobs::{
    JobCompletionUpdate, JobDefinitionUpsert, JobEnqueue, JobFailureUpdate, JobProgressUpdate,
    claim_prestart_jobs, complete_job_failure, complete_job_success, enqueue_job, get_job_by_id,
    heartbeat_job, list_job_events, reap_expired_leases, release_unstarted_job_claim,
    update_job_progress, upsert_job_definition_tx,
};
use serde_json::{Value, json};
use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, timeout};

use super::{compute_retry_delay_ms, process_claimed_job, run_worker_loop};
use crate::config::JobsConfig;
use crate::registry::{JobHandler, JobRegistry};
use crate::test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};

struct CountingHandler {
    runs: Arc<AtomicUsize>,
}

async fn await_spawned_task<T>(
    handle: &mut JoinHandle<T>,
    timeout_duration: Duration,
    timeout_message: &str,
    panic_message: &str,
) -> T {
    match timeout(timeout_duration, &mut *handle).await {
        Ok(result) => result.expect(panic_message),
        Err(_) => {
            handle.abort();
            let _ = handle.await;
            panic!("{timeout_message}");
        }
    }
}

#[async_trait::async_trait]
impl JobHandler for CountingHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.pre_run_lease_loss")
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

struct PersistenceFailureHandler {
    runs: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl JobHandler for PersistenceFailureHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.persistence_failure")
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

struct RetryThenSuccessHandler {
    runs: Arc<AtomicUsize>,
}

struct InvalidCompletionProgressHandler {
    runs: Arc<AtomicUsize>,
    dead_letters: Arc<Mutex<Vec<JobDeadLetterInfo>>>,
}

struct PartialInvalidCompletionProgressHandler {
    runs: Arc<AtomicUsize>,
    dead_letters: Arc<Mutex<Vec<JobDeadLetterInfo>>>,
}

struct PanickingHandler {
    runs: Arc<AtomicUsize>,
}

struct LoopSuccessHandler {
    runs: Arc<AtomicUsize>,
}

struct RecordingDeadLetterHandler {
    job_type_name: &'static str,
    failure: JobFailure,
    runs: Arc<AtomicUsize>,
    dead_letters: Arc<Mutex<Vec<JobDeadLetterInfo>>>,
}

struct FailingHandler {
    job_type_name: &'static str,
    failure: JobFailure,
    runs: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl JobHandler for RetryThenSuccessHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.retry_then_success")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        let prior_runs = self.runs.fetch_add(1, Ordering::SeqCst);
        if prior_runs == 0 {
            return Err(JobFailure::retryable(
                "job.test.retry_once",
                "first execution should retry",
            ));
        }

        Ok(JobCompletion::success())
    }
}

#[async_trait::async_trait]
impl JobHandler for InvalidCompletionProgressHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.invalid_completion_progress")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(JobCompletion::success().progress(2, 1))
    }

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        _payload: Value,
        dead_letter: JobDeadLetterInfo,
    ) {
        self.dead_letters
            .lock()
            .expect("dead-letter list lock should not be poisoned")
            .push(dead_letter);
    }
}

#[async_trait::async_trait]
impl JobHandler for PartialInvalidCompletionProgressHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.partial_invalid_completion_progress")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(JobCompletion {
            progress_done: Some(20),
            progress_total: None,
            checkpoint: None,
            output: None,
        })
    }

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        _payload: Value,
        dead_letter: JobDeadLetterInfo,
    ) {
        self.dead_letters
            .lock()
            .expect("dead-letter list lock should not be poisoned")
            .push(dead_letter);
    }
}

#[async_trait::async_trait]
impl JobHandler for PanickingHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.handler_panic")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        panic!("panic from main job handler");
    }
}

#[async_trait::async_trait]
impl JobHandler for LoopSuccessHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.handler_panic_successor")
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
impl JobHandler for RecordingDeadLetterHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(self.job_type_name)
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Err(self.failure.clone())
    }

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        _payload: Value,
        dead_letter: JobDeadLetterInfo,
    ) {
        self.dead_letters
            .lock()
            .expect("dead-letter list lock should not be poisoned")
            .push(dead_letter);
    }
}

#[async_trait::async_trait]
impl JobHandler for FailingHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(self.job_type_name)
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Err(self.failure.clone())
    }
}

struct TerminalHookPanicHandler {
    runs: Arc<AtomicUsize>,
    terminal_failures: Arc<AtomicUsize>,
}

struct TerminalHookHangHandler {
    runs: Arc<AtomicUsize>,
    terminal_failures: Arc<AtomicUsize>,
}

struct RetryDelayOverrideObservation {
    status: JobStatus,
    next_run_at: DateTime<Utc>,
    retry_event_delay_ms: Option<i64>,
    attempt_retry_delay_ms: Option<i32>,
    default_retry_delay_ms: i32,
    db_now_before: DateTime<Utc>,
    db_now_after: DateTime<Utc>,
    runs: usize,
}

#[async_trait::async_trait]
impl JobHandler for TerminalHookHangHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.terminal_hook_hang")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Err(JobFailure::terminal(
            "job.test.terminal_failure",
            "terminal failure for timeout isolation test",
        ))
    }

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        _payload: Value,
        _dead_letter: JobDeadLetterInfo,
    ) {
        self.terminal_failures.fetch_add(1, Ordering::SeqCst);
        pending::<()>().await;
    }
}

#[async_trait::async_trait]
impl JobHandler for TerminalHookPanicHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.test.terminal_hook_panic")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Err(JobFailure::terminal(
            "job.test.terminal_failure",
            "terminal failure for panic isolation test",
        ))
    }

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        _payload: Value,
        _dead_letter: JobDeadLetterInfo,
    ) {
        self.terminal_failures.fetch_add(1, Ordering::SeqCst);
        panic!("panic from worker terminal failure hook");
    }
}

async fn claim_one_job(pool: &PgPool, worker_id: &str) -> runledger_postgres::jobs::JobQueueRecord {
    let mut claimed = claim_prestart_jobs(pool, worker_id, 30, 1)
        .await
        .expect("claim jobs");
    claimed.pop().expect("expected one claimed job")
}

#[tokio::test]
async fn run_worker_loop_exits_when_shutdown_sender_is_dropped() {
    // The lazy pool deliberately points at an invalid port. This test asserts
    // the loop observes a closed shutdown channel before attempting any claim.
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://postgres:postgres@127.0.0.1:1/runledger")
        .expect("construct lazy pool");
    let registry = JobRegistry::new();
    let config = JobsConfig {
        worker_id: "dropped-shutdown-worker".to_string(),
        poll_interval: Duration::from_secs(30),
        claim_batch_size: 1,
        lease_ttl_seconds: 30,
        max_global_concurrency: 1,
        reaper_interval: Duration::from_secs(30),
        schedule_poll_interval: Duration::from_secs(30),
        reaper_retry_delay_ms: 1_000,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut worker_task = tokio::spawn(run_worker_loop(pool, registry, config, shutdown_rx));

    drop(shutdown_tx);

    if timeout(Duration::from_millis(200), &mut worker_task)
        .await
        .is_err()
    {
        worker_task.abort();
        let _ = worker_task.await;
        panic!("worker should treat a closed shutdown channel as shutdown");
    }
}

async fn enqueue_and_claim_job(
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

async fn connect_closed_pool(database_url: &str) -> PgPool {
    let worker_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(database_url)
        .await
        .expect("connect worker pool");
    worker_pool.close().await;
    worker_pool
}

async fn expire_job_lease(pool: &PgPool, job_id: uuid::Uuid) {
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

async fn wait_for_heartbeat_to_block_on_job_lock(pool: &PgPool) {
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

async fn wait_for_status(
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

fn query_error_code(error: &runledger_postgres::Error) -> Option<&str> {
    match error {
        runledger_postgres::Error::QueryError(query_error) => Some(query_error.code()),
        _ => None,
    }
}

fn clone_dead_letters(dead_letters: &Arc<Mutex<Vec<JobDeadLetterInfo>>>) -> Vec<JobDeadLetterInfo> {
    dead_letters
        .lock()
        .expect("dead-letter list lock should not be poisoned")
        .clone()
}

async fn database_now(pool: &PgPool) -> DateTime<Utc> {
    sqlx::query_scalar::<_, DateTime<Utc>>("SELECT now()")
        .fetch_one(pool)
        .await
        .expect("fetch database now")
}

async fn observe_retry_delay_override_failure(
    database_name: &str,
    handler_job_type: &'static str,
    failure: JobFailure,
    max_attempts: i32,
    override_registration: Option<(JobType<'static>, &'static str, i32)>,
) -> RetryDelayOverrideObservation {
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
        failure,
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
    let retry_event_delay_ms = events
        .iter()
        .find(|event| event.event_type == JobEventType::RetryScheduled)
        .and_then(|event| event.payload.get("retry_delay_ms"))
        .and_then(Value::as_i64);
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
        attempt_retry_delay_ms,
        default_retry_delay_ms,
        db_now_before,
        db_now_after,
        runs: runs.load(Ordering::SeqCst),
    };

    teardown_ephemeral_pool(pool, database).await;
    observation
}

fn assert_next_run_at_around_delay(observation: &RetryDelayOverrideObservation, delay_ms: i32) {
    let lower_bound = observation.db_now_before + ChronoDuration::milliseconds(i64::from(delay_ms));
    let upper_bound = observation.db_now_after
        + ChronoDuration::milliseconds(i64::from(delay_ms))
        + ChronoDuration::seconds(1);

    assert!(
        observation.next_run_at >= lower_bound && observation.next_run_at <= upper_bound,
        "expected next_run_at {} to be between {} and {}",
        observation.next_run_at,
        lower_bound,
        upper_bound
    );
}

#[tokio::test]
async fn process_claimed_job_uses_registered_retry_delay_override() {
    const OVERRIDE_RETRY_DELAY_MS: i32 = 120_000;

    let observation = observe_retry_delay_override_failure(
        "jobs_worker_retry_override",
        "jobs.test.retry_override",
        JobFailure::retryable(
            "job.test.waiting_for_external_refresh",
            "waiting for external refresh",
        ),
        3,
        Some((
            JobType::new("jobs.test.retry_override"),
            "job.test.waiting_for_external_refresh",
            OVERRIDE_RETRY_DELAY_MS,
        )),
    )
    .await;

    assert_eq!(observation.runs, 1);
    assert_eq!(observation.status, JobStatus::Pending);
    assert_eq!(
        observation.retry_event_delay_ms,
        Some(i64::from(OVERRIDE_RETRY_DELAY_MS))
    );
    assert_eq!(
        observation.attempt_retry_delay_ms,
        Some(OVERRIDE_RETRY_DELAY_MS)
    );
    assert_next_run_at_around_delay(&observation, OVERRIDE_RETRY_DELAY_MS);
}

#[tokio::test]
async fn process_claimed_job_does_not_apply_override_to_other_job_type() {
    const OVERRIDE_RETRY_DELAY_MS: i32 = 120_000;

    let observation = observe_retry_delay_override_failure(
        "jobs_worker_retry_override_type",
        "jobs.test.retry_override.other",
        JobFailure::retryable(
            "job.test.waiting_for_external_refresh",
            "waiting for external refresh",
        ),
        3,
        Some((
            JobType::new("jobs.test.retry_override"),
            "job.test.waiting_for_external_refresh",
            OVERRIDE_RETRY_DELAY_MS,
        )),
    )
    .await;

    assert_eq!(observation.status, JobStatus::Pending);
    assert_ne!(
        observation.retry_event_delay_ms,
        Some(i64::from(OVERRIDE_RETRY_DELAY_MS))
    );
    assert_eq!(
        observation.retry_event_delay_ms,
        Some(i64::from(observation.default_retry_delay_ms))
    );
    assert_eq!(
        observation.attempt_retry_delay_ms,
        Some(observation.default_retry_delay_ms)
    );
}

#[tokio::test]
async fn process_claimed_job_does_not_apply_override_to_other_failure_code() {
    const OVERRIDE_RETRY_DELAY_MS: i32 = 120_000;

    let observation = observe_retry_delay_override_failure(
        "jobs_worker_retry_override_code",
        "jobs.test.retry_override",
        JobFailure::retryable(
            "job.test.other_waiting_reason",
            "waiting for a different reason",
        ),
        3,
        Some((
            JobType::new("jobs.test.retry_override"),
            "job.test.waiting_for_external_refresh",
            OVERRIDE_RETRY_DELAY_MS,
        )),
    )
    .await;

    assert_eq!(observation.status, JobStatus::Pending);
    assert_ne!(
        observation.retry_event_delay_ms,
        Some(i64::from(OVERRIDE_RETRY_DELAY_MS))
    );
    assert_eq!(
        observation.retry_event_delay_ms,
        Some(i64::from(observation.default_retry_delay_ms))
    );
    assert_eq!(
        observation.attempt_retry_delay_ms,
        Some(observation.default_retry_delay_ms)
    );
}

#[tokio::test]
async fn process_claimed_job_ignores_retry_delay_override_for_terminal_failure() {
    const OVERRIDE_RETRY_DELAY_MS: i32 = 120_000;

    let observation = observe_retry_delay_override_failure(
        "jobs_worker_retry_override_terminal",
        "jobs.test.retry_override",
        JobFailure::terminal(
            "job.test.waiting_for_external_refresh",
            "terminal failure with matching code",
        ),
        3,
        Some((
            JobType::new("jobs.test.retry_override"),
            "job.test.waiting_for_external_refresh",
            OVERRIDE_RETRY_DELAY_MS,
        )),
    )
    .await;

    assert_eq!(observation.runs, 1);
    assert_eq!(observation.status, JobStatus::DeadLettered);
    assert_eq!(observation.retry_event_delay_ms, None);
    assert_eq!(observation.attempt_retry_delay_ms, None);
}

#[tokio::test]
async fn expired_lease_rejects_worker_lifecycle_updates() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_expired_lifecycle", 8).await;
    let job_type = JobType::new("jobs.test.expired_lifecycle");

    let (heartbeat_job_id, heartbeat_claim) = enqueue_and_claim_job(
        &pool,
        job_type,
        3,
        json!({"kind":"expired-heartbeat"}),
        "worker-expired-heartbeat",
    )
    .await;
    expire_job_lease(&pool, heartbeat_job_id).await;
    let heartbeat_error = heartbeat_job(
        &pool,
        heartbeat_claim.id,
        heartbeat_claim.run_number,
        heartbeat_claim.attempt,
        heartbeat_claim
            .worker_id
            .as_deref()
            .expect("claimed job has worker id"),
        30,
    )
    .await
    .expect_err("expired lease heartbeat should fail");
    assert_eq!(
        query_error_code(&heartbeat_error),
        Some("job.lease_owner_mismatch")
    );

    let (progress_job_id, progress_claim) = enqueue_and_claim_job(
        &pool,
        job_type,
        3,
        json!({"kind":"expired-progress"}),
        "worker-expired-progress",
    )
    .await;
    expire_job_lease(&pool, progress_job_id).await;
    let progress_error = update_job_progress(
        &pool,
        progress_claim.id,
        progress_claim.run_number,
        progress_claim.attempt,
        progress_claim
            .worker_id
            .as_deref()
            .expect("claimed job has worker id"),
        &JobProgressUpdate {
            stage: Some(runledger_core::jobs::JobStage::Running),
            progress_done: None,
            progress_total: None,
            checkpoint: None,
        },
    )
    .await
    .expect_err("expired lease progress update should fail");
    assert_eq!(
        query_error_code(&progress_error),
        Some("job.lease_owner_mismatch")
    );

    let (success_job_id, success_claim) = enqueue_and_claim_job(
        &pool,
        job_type,
        3,
        json!({"kind":"expired-success"}),
        "worker-expired-success",
    )
    .await;
    expire_job_lease(&pool, success_job_id).await;
    let success_error = complete_job_success(
        &pool,
        success_claim.id,
        success_claim.run_number,
        success_claim.attempt,
        success_claim
            .worker_id
            .as_deref()
            .expect("claimed job has worker id"),
        None,
    )
    .await
    .expect_err("expired lease success completion should fail");
    assert_eq!(
        query_error_code(&success_error),
        Some("job.lease_owner_mismatch")
    );

    let (failure_job_id, failure_claim) = enqueue_and_claim_job(
        &pool,
        job_type,
        3,
        json!({"kind":"expired-failure"}),
        "worker-expired-failure",
    )
    .await;
    expire_job_lease(&pool, failure_job_id).await;
    let failure_error = complete_job_failure(
        &pool,
        failure_claim.id,
        failure_claim.run_number,
        failure_claim.attempt,
        failure_claim
            .worker_id
            .as_deref()
            .expect("claimed job has worker id"),
        &JobFailureUpdate {
            kind: JobFailureKind::Retryable,
            code: "job.test.expired_failure",
            message: "expired failure should not persist",
            retry_delay_ms: Some(1_000),
        },
    )
    .await
    .expect_err("expired lease failure completion should fail");
    assert_eq!(
        query_error_code(&failure_error),
        Some("job.lease_owner_mismatch")
    );

    for job_id in [
        heartbeat_job_id,
        progress_job_id,
        success_job_id,
        failure_job_id,
    ] {
        let job = get_job_by_id(&pool, None, job_id)
            .await
            .expect("load job")
            .expect("job exists");
        assert_eq!(job.status, JobStatus::Leased);
    }

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn heartbeat_rejects_lease_that_expires_while_waiting_for_job_lock() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_heartbeat_clock_expiry", 8).await;
    let job_type = JobType::new("jobs.test.heartbeat_clock_expiry");
    let (job_id, claim) = enqueue_and_claim_job(
        &pool,
        job_type,
        3,
        json!({"kind":"heartbeat-clock-expiry"}),
        "worker-heartbeat-clock-expiry",
    )
    .await;
    let worker_id = claim.worker_id.clone().expect("claimed job has worker id");

    sqlx::query(
        "UPDATE job_queue
         SET lease_expires_at = clock_timestamp() + interval '1 second'
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .expect("shorten lease before blocking heartbeat");

    let mut lock_tx = pool.begin().await.expect("begin job lock transaction");
    sqlx::query("SELECT id FROM job_queue WHERE id = $1 FOR UPDATE")
        .bind(job_id)
        .execute(&mut *lock_tx)
        .await
        .expect("hold job row lock");

    let heartbeat_pool = pool.clone();
    let mut heartbeat_task = tokio::spawn(async move {
        heartbeat_job(
            &heartbeat_pool,
            claim.id,
            claim.run_number,
            claim.attempt,
            &worker_id,
            30,
        )
        .await
    });

    wait_for_heartbeat_to_block_on_job_lock(&pool).await;
    sleep(Duration::from_millis(1_200)).await;
    lock_tx.rollback().await.expect("release job row lock");

    let error = await_spawned_task(
        &mut heartbeat_task,
        Duration::from_secs(5),
        "heartbeat should finish after row lock release",
        "heartbeat task should not panic",
    )
    .await
    .expect_err("heartbeat should reject lease expired during lock wait");
    assert_eq!(query_error_code(&error), Some("job.lease_owner_mismatch"));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn successful_completion_persists_completion_update() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_success_completion_update", 8).await;
    let job_type = JobType::new("jobs.test.success_completion_update");
    let (job_id, claim) = enqueue_and_claim_job(
        &pool,
        job_type,
        3,
        json!({"kind":"success-completion-update"}),
        "worker-success-completion-update",
    )
    .await;

    let checkpoint = json!({"cursor": "next"});
    let output = json!({"result_id": "result_123"});
    complete_job_success(
        &pool,
        claim.id,
        claim.run_number,
        claim.attempt,
        claim
            .worker_id
            .as_deref()
            .expect("claimed job has worker id"),
        Some(&JobCompletionUpdate {
            progress_done: Some(2),
            progress_total: Some(3),
            checkpoint: Some(&checkpoint),
            output: Some(&output),
        }),
    )
    .await
    .expect("success completion should persist completion update");

    let job = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(job.status, JobStatus::Succeeded);
    assert_eq!(job.stage, runledger_core::jobs::JobStage::Completed);
    assert_eq!(job.progress_done, Some(2));
    assert_eq!(job.progress_total, Some(3));
    assert_eq!(job.checkpoint, Some(checkpoint));
    assert_eq!(job.output, Some(output));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn standalone_success_completion_allows_non_read_committed_session() {
    // The session-level isolation setting must apply to the same connection
    // complete_job_success borrows from the pool.
    let (pool, database) = setup_ephemeral_pool("jobs_worker_success_repeatable_read", 1).await;
    let job_type = JobType::new("jobs.test.success_repeatable_read");
    let (job_id, claim) = enqueue_and_claim_job(
        &pool,
        job_type,
        3,
        json!({"kind":"standalone-repeatable-read-success"}),
        "worker-standalone-repeatable-read",
    )
    .await;
    let worker_id = claim.worker_id.clone().expect("claimed job has worker id");

    sqlx::query("SET default_transaction_isolation = 'repeatable read'")
        .execute(&pool)
        .await
        .expect("set default isolation to repeatable read");
    let result = complete_job_success(
        &pool,
        claim.id,
        claim.run_number,
        claim.attempt,
        &worker_id,
        None,
    )
    .await;
    sqlx::query("SET default_transaction_isolation = 'read committed'")
        .execute(&pool)
        .await
        .expect("reset default isolation to read committed");
    result.expect("standalone job completion does not require workflow isolation");

    let job = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(job.status, JobStatus::Succeeded);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_terminally_fails_invalid_success_progress_without_replay() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_invalid_success_progress", 8).await;

    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.invalid_completion_progress"),
        3,
        json!({"kind":"invalid-success-progress"}),
        "worker-invalid-success-progress",
    )
    .await;

    let runs = Arc::new(AtomicUsize::new(0));
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(InvalidCompletionProgressHandler {
        runs: runs.clone(),
        dead_letters: dead_letters.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let dead_letters = clone_dead_letters(&dead_letters);
    assert_eq!(dead_letters.len(), 1);
    let dead_letter = &dead_letters[0];
    assert_eq!(
        dead_letter.reason,
        JobDeadLetterReason::FailureKindNonRetryable
    );
    assert_eq!(dead_letter.failure.kind, JobFailureKind::Terminal);
    assert_eq!(dead_letter.failure.code, "job.invalid_completion_progress");
    assert_eq!(dead_letter.max_attempts, Some(3));

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);
    assert_eq!(persisted.status_reason.as_deref(), Some("TERMINAL"));
    assert_eq!(
        persisted.last_error_code.as_deref(),
        Some("job.invalid_completion_progress")
    );
    assert!(persisted.worker_id.is_none());
    assert!(persisted.lease_expires_at.is_none());

    let events = list_job_events(&pool, None, job_id, 50, None)
        .await
        .expect("list job events");
    assert!(
        events
            .iter()
            .all(|event| event.event_type != JobEventType::Succeeded),
        "invalid success completion must not write a succeeded event"
    );
    assert!(
        events
            .iter()
            .all(|event| event.event_type != JobEventType::RetryScheduled),
        "invalid success completion must not schedule a retry"
    );
    let failed = events
        .iter()
        .find(|event| event.event_type == JobEventType::Failed)
        .expect("failed event should exist");
    assert_eq!(failed.payload.get("kind"), Some(&json!("TERMINAL")));
    assert_eq!(
        failed.payload.get("error_code"),
        Some(&json!("job.invalid_completion_progress"))
    );

    reap_expired_leases(&pool, 10, 1_000)
        .await
        .expect("reaper should not requeue terminal invalid completion");
    let replay_claims = claim_prestart_jobs(&pool, "worker-invalid-success-replay", 30, 1)
        .await
        .expect("claim after terminal invalid completion");
    assert!(replay_claims.is_empty());
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_terminally_fails_stale_partial_success_progress_without_replay() {
    let (pool, database) =
        setup_ephemeral_pool("jobs_worker_stale_partial_success_progress", 8).await;

    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.partial_invalid_completion_progress"),
        3,
        json!({"kind":"stale-partial-success-progress"}),
        "worker-stale-partial-success-progress",
    )
    .await;

    let worker_id = claimed_job
        .worker_id
        .clone()
        .expect("claimed job has worker id");
    update_job_progress(
        &pool,
        claimed_job.id,
        claimed_job.run_number,
        claimed_job.attempt,
        &worker_id,
        &JobProgressUpdate {
            stage: None,
            progress_done: Some(5),
            progress_total: Some(10),
            checkpoint: None,
        },
    )
    .await
    .expect("persist prior progress");

    let runs = Arc::new(AtomicUsize::new(0));
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(PartialInvalidCompletionProgressHandler {
        runs: runs.clone(),
        dead_letters: dead_letters.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let dead_letters = clone_dead_letters(&dead_letters);
    assert_eq!(dead_letters.len(), 1);
    let dead_letter = &dead_letters[0];
    assert_eq!(
        dead_letter.reason,
        JobDeadLetterReason::FailureKindNonRetryable
    );
    assert_eq!(dead_letter.failure.kind, JobFailureKind::Terminal);
    assert_eq!(dead_letter.failure.code, "job.invalid_completion_progress");

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);
    assert_eq!(
        persisted.last_error_code.as_deref(),
        Some("job.invalid_completion_progress")
    );
    assert!(persisted.worker_id.is_none());
    assert!(persisted.lease_expires_at.is_none());

    let events = list_job_events(&pool, None, job_id, 50, None)
        .await
        .expect("list job events");
    assert!(
        events
            .iter()
            .all(|event| event.event_type != JobEventType::Succeeded),
        "invalid coalesced success completion must not write a succeeded event"
    );
    assert!(
        events
            .iter()
            .all(|event| event.event_type != JobEventType::RetryScheduled),
        "invalid coalesced success completion must not schedule a retry"
    );

    reap_expired_leases(&pool, 10, 1_000)
        .await
        .expect("reaper should not requeue terminal invalid completion");
    let replay_claims = claim_prestart_jobs(&pool, "worker-stale-partial-replay", 30, 1)
        .await
        .expect("claim after terminal invalid completion");
    assert!(replay_claims.is_empty());
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_aborts_before_handler_when_lease_owner_changes_pre_run() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_pre_run_lease", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.pre_run_lease_loss"),
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
            job_type: JobType::new("jobs.test.pre_run_lease_loss"),
            organization_id: None,
            payload: &json!({"kind":"pre-run-mismatch"}),
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

    let claimed_job = claim_one_job(&pool, "worker-1").await;

    sqlx::query(
        "UPDATE job_queue
         SET worker_id = 'worker-2'
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .expect("switch lease ownership");

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(CountingHandler { runs: runs.clone() });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "handler must not execute if lease ownership is lost before starting"
    );

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Leased);
    assert_eq!(persisted.worker_id.as_deref(), Some("worker-2"));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_survives_terminal_failure_hook_panic() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_terminal_hook_panic", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.terminal_hook_panic"),
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
            job_type: JobType::new("jobs.test.terminal_hook_panic"),
            organization_id: None,
            payload: &json!({"kind":"terminal-hook-panic"}),
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

    let claimed_job = claim_one_job(&pool, "worker-terminal-hook-panic").await;

    let runs = Arc::new(AtomicUsize::new(0));
    let terminal_failures = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(TerminalHookPanicHandler {
        runs: runs.clone(),
        terminal_failures: terminal_failures.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(terminal_failures.load(Ordering::SeqCst), 1);

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_survives_terminal_failure_hook_timeout() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_terminal_hook_hang", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.terminal_hook_hang"),
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
            job_type: JobType::new("jobs.test.terminal_hook_hang"),
            organization_id: None,
            payload: &json!({"kind":"terminal-hook-hang"}),
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

    let claimed_job = claim_one_job(&pool, "worker-terminal-hook-hang").await;

    let runs = Arc::new(AtomicUsize::new(0));
    let terminal_failures = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(TerminalHookHangHandler {
        runs: runs.clone(),
        terminal_failures: terminal_failures.clone(),
    });

    timeout(
        Duration::from_secs(2),
        process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30),
    )
    .await
    .expect("process_claimed_job should return even when terminal hook hangs");

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(terminal_failures.load(Ordering::SeqCst), 1);

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_reports_attempt_exhaustion_to_dead_letter_hook() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_dead_letter_attempts", 8).await;

    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.dead_letter_attempts"),
        1,
        json!({"kind":"dead-letter-attempts"}),
        "worker-dead-letter-attempts",
    )
    .await;

    let runs = Arc::new(AtomicUsize::new(0));
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(RecordingDeadLetterHandler {
        job_type_name: "jobs.test.dead_letter_attempts",
        failure: JobFailure::retryable(
            "job.test.retryable_exhausted",
            "retryable failure should exhaust attempts",
        ),
        runs: runs.clone(),
        dead_letters: dead_letters.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let dead_letters = clone_dead_letters(&dead_letters);
    assert_eq!(dead_letters.len(), 1);
    let dead_letter = &dead_letters[0];
    assert_eq!(dead_letter.reason, JobDeadLetterReason::AttemptsExhausted);
    assert_eq!(dead_letter.failure.kind, JobFailureKind::Retryable);
    assert_eq!(dead_letter.max_attempts, Some(1));

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_reports_non_retryable_failure_to_dead_letter_hook() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_dead_letter_non_retryable", 8).await;

    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.dead_letter_non_retryable"),
        3,
        json!({"kind":"dead-letter-non-retryable"}),
        "worker-dead-letter-non-retryable",
    )
    .await;

    let runs = Arc::new(AtomicUsize::new(0));
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(RecordingDeadLetterHandler {
        job_type_name: "jobs.test.dead_letter_non_retryable",
        failure: JobFailure::terminal(
            "job.test.non_retryable",
            "terminal failure should remain non-retryable",
        ),
        runs: runs.clone(),
        dead_letters: dead_letters.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let dead_letters = clone_dead_letters(&dead_letters);
    assert_eq!(dead_letters.len(), 1);
    let dead_letter = &dead_letters[0];
    assert_eq!(
        dead_letter.reason,
        JobDeadLetterReason::FailureKindNonRetryable
    );
    assert_eq!(dead_letter.failure.kind, JobFailureKind::Terminal);
    assert_eq!(dead_letter.max_attempts, Some(3));

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_persists_handler_failure_with_reserved_lease_code() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_reserved_lease_code", 8).await;

    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.reserved_lease_code"),
        3,
        json!({"kind":"reserved-lease-code"}),
        "worker-reserved-lease-code",
    )
    .await;

    let runs = Arc::new(AtomicUsize::new(0));
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(RecordingDeadLetterHandler {
        job_type_name: "jobs.test.reserved_lease_code",
        failure: JobFailure::terminal(
            "job.lease_owner_mismatch",
            "handler failure should not be treated as internal lease loss",
        ),
        runs: runs.clone(),
        dead_letters: dead_letters.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let dead_letters = clone_dead_letters(&dead_letters);
    assert_eq!(dead_letters.len(), 1);
    let dead_letter = &dead_letters[0];
    assert_eq!(
        dead_letter.reason,
        JobDeadLetterReason::FailureKindNonRetryable
    );
    assert_eq!(dead_letter.failure.kind, JobFailureKind::Terminal);
    assert_eq!(dead_letter.failure.code, "job.lease_owner_mismatch");

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);
    assert_eq!(
        persisted.last_error_code.as_deref(),
        Some("job.lease_owner_mismatch")
    );
    assert!(persisted.worker_id.is_none());
    assert!(persisted.lease_expires_at.is_none());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_catches_main_handler_panic() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_handler_panic", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.handler_panic"),
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
            job_type: JobType::new("jobs.test.handler_panic"),
            organization_id: None,
            payload: &json!({"kind":"handler-panic"}),
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

    let claimed_job = claim_one_job(&pool, "worker-handler-panic").await;

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(PanickingHandler { runs: runs.clone() });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);
    assert_eq!(persisted.status_reason.as_deref(), Some("PANICKED"));
    assert_eq!(
        persisted.last_error_code.as_deref(),
        Some("job.handler_panic")
    );

    let outcome = sqlx::query_scalar::<_, String>(
        "SELECT outcome::text
         FROM job_attempts
         WHERE job_id = $1
           AND run_number = 1
           AND attempt = 1",
    )
    .bind(job_id)
    .fetch_one(&pool)
    .await
    .expect("fetch attempt outcome");
    assert_eq!(outcome, "PANICKED");

    let events = list_job_events(&pool, None, job_id, 50, None)
        .await
        .expect("list job events");
    let failed = events
        .iter()
        .find(|event| event.event_type == runledger_core::jobs::JobEventType::Failed)
        .expect("failed event should exist");
    assert_eq!(failed.payload.get("kind"), Some(&json!("PANICKED")));
    assert_eq!(
        failed.payload.get("error_code"),
        Some(&json!("job.handler_panic"))
    );
    let dead_lettered = events
        .iter()
        .find(|event| event.event_type == runledger_core::jobs::JobEventType::DeadLettered)
        .expect("dead-lettered event should exist");
    assert_eq!(dead_lettered.payload.get("kind"), Some(&json!("PANICKED")));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn run_worker_loop_continues_processing_after_handler_panic() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_handler_panic_loop", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    for job_type in [
        JobType::new("jobs.test.handler_panic"),
        JobType::new("jobs.test.handler_panic_successor"),
    ] {
        upsert_job_definition_tx(
            &mut tx,
            &JobDefinitionUpsert {
                job_type,
                version: 1,
                max_attempts: 1,
                default_timeout_seconds: 30,
                default_priority: 100,
                is_enabled: true,
            },
        )
        .await
        .expect("upsert job definition");
    }
    tx.commit().await.expect("commit tx");

    let panic_job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.handler_panic"),
            organization_id: None,
            payload: &json!({"kind":"loop-panic"}),
            priority: Some(200),
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue panic job");
    let success_job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new("jobs.test.handler_panic_successor"),
            organization_id: None,
            payload: &json!({"kind":"loop-success"}),
            priority: Some(100),
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue success job");

    let panic_runs = Arc::new(AtomicUsize::new(0));
    let success_runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(PanickingHandler {
        runs: panic_runs.clone(),
    });
    registry.register(LoopSuccessHandler {
        runs: success_runs.clone(),
    });

    let config = JobsConfig {
        worker_id: "handler-panic-loop-worker".to_string(),
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

    let panic_job = wait_for_status(
        &pool,
        panic_job_id,
        JobStatus::DeadLettered,
        Duration::from_secs(5),
    )
    .await;
    let success_job = wait_for_status(
        &pool,
        success_job_id,
        JobStatus::Succeeded,
        Duration::from_secs(5),
    )
    .await;

    assert_eq!(panic_job.status_reason.as_deref(), Some("PANICKED"));
    assert_eq!(
        panic_job.last_error_code.as_deref(),
        Some("job.handler_panic")
    );
    assert_eq!(success_job.last_error_code, None);
    assert_eq!(panic_runs.load(Ordering::SeqCst), 1);
    assert_eq!(success_runs.load(Ordering::SeqCst), 1);

    let _ = shutdown_tx.send(true);
    worker_task
        .await
        .expect("worker loop should shut down cleanly");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_aborts_when_running_progress_cannot_be_persisted() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_progress_persist_fail", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.persistence_failure"),
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
            job_type: JobType::new("jobs.test.persistence_failure"),
            organization_id: None,
            payload: &json!({"kind":"running-progress-persistence-failure"}),
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

    let claimed_job = claim_one_job(&pool, "worker-persistence-failure").await;
    let worker_pool = connect_closed_pool(&database.url).await;

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(PersistenceFailureHandler { runs: runs.clone() });

    process_claimed_job(worker_pool, Arc::new(registry), claimed_job, 30).await;

    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "handler must not execute once the worker can no longer persist running state"
    );

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Leased);
    assert_eq!(
        persisted.stage,
        runledger_core::jobs::JobStage::Queued,
        "job should remain queued because running state was never durably recorded"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn release_unstarted_claim_reports_not_applicable_after_running_persists() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_release_not_applicable", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.persistence_failure"),
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
            job_type: JobType::new("jobs.test.persistence_failure"),
            organization_id: None,
            payload: &json!({"kind":"release-not-applicable"}),
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

    let claimed_job = claim_one_job(&pool, "worker-release-not-applicable").await;
    update_job_progress(
        &pool,
        claimed_job.id,
        claimed_job.run_number,
        claimed_job.attempt,
        claimed_job
            .worker_id
            .as_deref()
            .expect("worker id is set on claimed job"),
        &JobProgressUpdate {
            stage: Some(runledger_core::jobs::JobStage::Running),
            progress_done: None,
            progress_total: None,
            checkpoint: None,
        },
    )
    .await
    .expect("persist running stage");

    let error = release_unstarted_job_claim(
        &pool,
        job_id,
        claimed_job.run_number,
        claimed_job.attempt,
        "worker-release-not-applicable",
        "TEST_NOT_APPLICABLE",
        0,
    )
    .await
    .expect_err("release should no longer apply once running persists");

    assert_eq!(
        query_error_code(&error),
        Some("job.unstarted_claim_release_not_applicable")
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn release_unstarted_claim_reports_owner_mismatch_for_other_worker() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_release_owner_mismatch", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.persistence_failure"),
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
            job_type: JobType::new("jobs.test.persistence_failure"),
            organization_id: None,
            payload: &json!({"kind":"release-owner-mismatch"}),
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

    let claimed_job = claim_one_job(&pool, "worker-release-owner-a").await;
    sqlx::query(
        "UPDATE job_queue
         SET worker_id = 'worker-release-owner-b'
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .expect("switch lease ownership");

    let error = release_unstarted_job_claim(
        &pool,
        job_id,
        claimed_job.run_number,
        claimed_job.attempt,
        "worker-release-owner-a",
        "TEST_OWNER_MISMATCH",
        0,
    )
    .await
    .expect_err("release should fail when another worker owns the lease");

    assert_eq!(query_error_code(&error), Some("job.lease_owner_mismatch"));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn reaper_requeues_unstarted_claim_when_running_progress_never_persisted() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_progress_reaper_requeue", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.persistence_failure"),
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
            job_type: JobType::new("jobs.test.persistence_failure"),
            organization_id: None,
            payload: &json!({"kind":"running-progress-reaper-requeue"}),
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

    let claimed_job = claim_one_job(&pool, "worker-persistence-failure").await;
    let worker_pool = connect_closed_pool(&database.url).await;

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(PersistenceFailureHandler { runs: runs.clone() });

    process_claimed_job(worker_pool, Arc::new(registry), claimed_job, 30).await;

    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "handler must not execute before the job can durably enter RUNNING"
    );

    expire_job_lease(&pool, job_id).await;

    let reaped = reap_expired_leases(&pool, 1, 1_000)
        .await
        .expect("reap expired leases");
    assert_eq!(reaped, 1, "reaper should reclaim the stranded lease");

    let recovered = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load recovered job")
        .expect("job exists");
    assert_eq!(recovered.status, JobStatus::Pending);
    assert_eq!(
        recovered.attempt, 0,
        "reaper must return the unstarted claim without consuming an attempt"
    );

    let recovered_job = claim_one_job(&pool, "worker-persistence-retry").await;
    let runs_after_recovery = Arc::new(AtomicUsize::new(0));
    let mut recovery_registry = JobRegistry::new();
    recovery_registry.register(PersistenceFailureHandler {
        runs: runs_after_recovery.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(recovery_registry), recovered_job, 30).await;

    assert_eq!(
        runs_after_recovery.load(Ordering::SeqCst),
        1,
        "job should still be executable after reaper recovery"
    );

    let completed = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load completed job")
        .expect("job exists");
    assert_eq!(completed.status, JobStatus::Succeeded);
    assert_eq!(
        completed.attempt, 1,
        "successful execution should use the first real attempt after recovery"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn reaper_does_not_burn_retry_attempt_when_later_attempt_never_started() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_retry_attempt_not_burned", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new("jobs.test.retry_then_success"),
            version: 1,
            max_attempts: 2,
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
            job_type: JobType::new("jobs.test.retry_then_success"),
            organization_id: None,
            payload: &json!({"kind":"retry-attempt-pre-run-failure"}),
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

    let first_runs = Arc::new(AtomicUsize::new(0));
    let mut first_registry = JobRegistry::new();
    first_registry.register(RetryThenSuccessHandler {
        runs: first_runs.clone(),
    });

    let first_claimed_job = claim_one_job(&pool, "worker-retry-1").await;
    process_claimed_job(
        pool.clone(),
        Arc::new(first_registry),
        first_claimed_job,
        30,
    )
    .await;

    assert_eq!(
        first_runs.load(Ordering::SeqCst),
        1,
        "first attempt should execute and fail retryably"
    );

    let after_first_attempt = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after first attempt")
        .expect("job exists");
    assert_eq!(after_first_attempt.status, JobStatus::Pending);
    assert_eq!(after_first_attempt.attempt, 1);

    sqlx::query(
        "UPDATE job_queue
         SET next_run_at = now()
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(&pool)
    .await
    .expect("make second attempt claimable immediately");

    let second_claimed_job = claim_one_job(&pool, "worker-retry-2").await;
    let failing_worker_pool = connect_closed_pool(&database.url).await;

    let second_runs = Arc::new(AtomicUsize::new(0));
    let mut second_registry = JobRegistry::new();
    second_registry.register(RetryThenSuccessHandler {
        runs: second_runs.clone(),
    });

    process_claimed_job(
        failing_worker_pool,
        Arc::new(second_registry),
        second_claimed_job,
        30,
    )
    .await;

    assert_eq!(
        second_runs.load(Ordering::SeqCst),
        0,
        "second attempt must fail before handler execution"
    );

    expire_job_lease(&pool, job_id).await;

    let reaped = reap_expired_leases(&pool, 1, 1_000)
        .await
        .expect("reap expired lease");
    assert_eq!(reaped, 1);

    let after_reap = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after reap")
        .expect("job exists");
    assert_eq!(
        after_reap.status,
        JobStatus::Pending,
        "later attempt that never started should be released back to pending"
    );
    assert_eq!(
        after_reap.attempt, 1,
        "reaper should preserve the earlier consumed attempt count"
    );

    let recovery_runs = Arc::new(AtomicUsize::new(1));
    let mut recovery_registry = JobRegistry::new();
    recovery_registry.register(RetryThenSuccessHandler {
        runs: recovery_runs.clone(),
    });

    let recovered_job = claim_one_job(&pool, "worker-retry-3").await;
    process_claimed_job(pool.clone(), Arc::new(recovery_registry), recovered_job, 30).await;

    let completed = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load completed recovered job")
        .expect("job exists");
    assert_eq!(completed.status, JobStatus::Succeeded);
    assert_eq!(completed.attempt, 2);

    teardown_ephemeral_pool(pool, database).await;
}
