use runledger_core::jobs::{
    JobExecution, JobExecutionError, JobExecutionHandler, JobExecutionUpdate,
};

use super::*;

const JOB_TYPE: JobType<'static> = JobType::new("jobs.test.execution_services");

struct CheckpointHandler {
    committed: Arc<Notify>,
    finish: Arc<Notify>,
    pending: bool,
    dead_checkpoint: Arc<Mutex<Option<Value>>>,
}

#[async_trait::async_trait]
impl JobExecutionHandler for CheckpointHandler {
    fn job_type(&self) -> JobType<'static> {
        JOB_TYPE
    }

    async fn execute(
        &self,
        execution: JobExecution<'_>,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        if execution
            .checkpoint::<u64>()
            .expect("decode durable cursor")
            == Some(1)
        {
            return Ok(JobCompletion::success());
        }
        let configured_timeout = Duration::from_secs(if self.pending { 1 } else { 30 });
        assert!(execution.remaining_budget() <= configured_timeout);
        assert!(
            execution
                .deadline()
                .saturating_duration_since(std::time::Instant::now())
                <= configured_timeout
        );
        assert!(matches!(
            execution
                .persist_progress(JobExecutionUpdate {
                    progress_done: Some(-1),
                    ..Default::default()
                })
                .await,
            Err(JobExecutionError::InvalidProgress(_))
        ));
        let before = execution.remaining_budget();
        let deadline = execution.deadline();
        assert!(before > Duration::ZERO);
        assert_eq!(
            execution.remaining_work_budget(Duration::from_secs(60)),
            Duration::ZERO
        );
        let checkpoint = json!(1);
        execution
            .persist_progress(JobExecutionUpdate {
                progress_done: Some(1),
                progress_total: Some(3),
                checkpoint: Some(&checkpoint),
            })
            .await?;
        assert_eq!(
            execution.deadline(),
            deadline,
            "writes cannot reset the deadline"
        );
        assert!(execution.remaining_budget() < before);
        assert_eq!(
            execution.checkpoint::<u64>().expect("initial snapshot"),
            None
        );
        self.committed.notify_one();
        if self.pending {
            pending::<()>().await;
        }
        self.finish.notified().await;
        Ok(JobCompletion::continue_now())
    }

    async fn on_dead_letter(
        &self,
        context: JobContext,
        _payload: Value,
        _dead_letter: JobDeadLetterInfo,
    ) {
        *self
            .dead_checkpoint
            .lock()
            .expect("dead-letter checkpoint lock") = context.checkpoint;
    }
}

fn checkpoint_handler(pending: bool) -> CheckpointHandler {
    CheckpointHandler {
        committed: Arc::new(Notify::new()),
        finish: Arc::new(Notify::new()),
        pending,
        dead_checkpoint: Arc::new(Mutex::new(None)),
    }
}

#[tokio::test]
async fn awaited_progress_commits_before_return_and_continuation_resumes_checkpoint() {
    let (pool, database) = setup_ephemeral_pool("execution_services_checkpoint", 4).await;
    record_postgres_server_version(&pool, "execution-service checkpoint regression").await;
    let (job_id, job) =
        enqueue_and_claim_job(&pool, JOB_TYPE, 3, json!({}), "services-worker").await;
    let handler = checkpoint_handler(false);
    let committed = handler.committed.clone();
    let finish = handler.finish.clone();
    let mut registry = JobRegistry::new();
    registry
        .try_register(handler.into_job_handler())
        .expect("register opt-in handler");
    let registry = Arc::new(registry);
    let mut task = tokio::spawn(process_claimed_job(pool.clone(), registry.clone(), job, 30));
    timeout(Duration::from_secs(3), committed.notified())
        .await
        .expect("progress acknowledged");
    let saved = get_job_by_id(&pool, None, job_id)
        .await
        .expect("read durable job")
        .expect("job");
    assert_eq!(saved.status, JobStatus::Leased);
    assert_eq!(saved.checkpoint, Some(json!(1)));
    assert_eq!(
        (saved.progress_done, saved.progress_total),
        (Some(1), Some(3))
    );
    assert!(
        list_job_events(&pool, None, job_id, 100, None)
            .await
            .expect("events")
            .iter()
            .any(|event| event.event_type == JobEventType::Progress
                && event.progress_done == Some(1))
    );
    finish.notify_one();
    await_spawned_task(
        &mut task,
        Duration::from_secs(3),
        "continuation completes",
        "worker joins",
    )
    .await;
    let next = claim_prestart_jobs(&pool, "services-worker-next", 30, 1)
        .await
        .expect("claim next run")
        .pop()
        .expect("continued job");
    assert_eq!(next.run_number, 2);
    process_claimed_job(pool.clone(), registry, next, 30).await;
    let done = get_job_by_id(&pool, None, job_id)
        .await
        .expect("read result")
        .expect("job");
    assert_eq!(done.status, JobStatus::Succeeded);
    assert_eq!(done.checkpoint, Some(json!(1)));
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn runtime_deadline_cancels_handler_and_dead_letter_gets_committed_checkpoint() {
    let (pool, database) = setup_ephemeral_pool("execution_services_timeout", 4).await;
    record_postgres_server_version(&pool, "execution-service timeout regression").await;
    let (job_id, mut job) =
        enqueue_and_claim_job(&pool, JOB_TYPE, 1, json!({}), "services-worker").await;
    sqlx::query("UPDATE job_queue SET timeout_seconds = 1 WHERE id = $1")
        .bind(job_id)
        .execute(&pool)
        .await
        .expect("set short timeout");
    job.timeout_seconds = 1;
    let handler = checkpoint_handler(true);
    let committed = handler.committed.clone();
    let dead_checkpoint = handler.dead_checkpoint.clone();
    let mut registry = JobRegistry::new();
    registry.register(handler.into_job_handler());
    let started = Instant::now();
    let mut task = tokio::spawn(process_claimed_job(
        pool.clone(),
        Arc::new(registry),
        job,
        30,
    ));
    timeout(Duration::from_secs(3), committed.notified())
        .await
        .expect("progress acknowledged");
    await_spawned_task(
        &mut task,
        Duration::from_secs(3),
        "deadline terminates execution",
        "worker joins",
    )
    .await;
    assert!(started.elapsed() >= Duration::from_secs(1));
    let saved = get_job_by_id(&pool, None, job_id)
        .await
        .expect("read timed-out job")
        .expect("job");
    assert_eq!(saved.status, JobStatus::DeadLettered);
    assert_eq!(
        saved.last_error_code.as_deref(),
        Some("job.timeout_exceeded")
    );
    assert_eq!(saved.checkpoint, Some(json!(1)));
    assert_eq!(
        *dead_checkpoint.lock().expect("hook checkpoint"),
        Some(json!(1))
    );
    teardown_ephemeral_pool(pool, database).await;
}

struct ControlledWriteHandler {
    started: Arc<Notify>,
    write: Arc<Notify>,
    outcome: Arc<Mutex<Option<&'static str>>>,
    returned: Arc<Notify>,
}

#[async_trait::async_trait]
impl JobExecutionHandler for ControlledWriteHandler {
    fn job_type(&self) -> JobType<'static> {
        JOB_TYPE
    }
    async fn execute(
        &self,
        execution: JobExecution<'_>,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.started.notify_one();
        self.write.notified().await;
        let result = execution.save_checkpoint(&7_u64).await;
        let code = match result {
            Ok(()) => "committed",
            Err(JobExecutionError::LeaseLost) => "lease_lost",
            Err(JobExecutionError::PersistenceFailed) => "persistence_failed",
            Err(JobExecutionError::DeadlineElapsed) => "deadline_elapsed",
            Err(error) => panic!("unexpected progress error: {error}"),
        };
        *self.outcome.lock().expect("write outcome") = Some(code);
        self.returned.notify_one();
        // Deliberately swallow the failure: the runtime must still stop a handler
        // when its progress operation discovers lease loss.
        pending::<()>().await;
        Ok(JobCompletion::success())
    }
}

fn controlled_handler() -> ControlledWriteHandler {
    ControlledWriteHandler {
        started: Arc::new(Notify::new()),
        write: Arc::new(Notify::new()),
        outcome: Arc::new(Mutex::new(None)),
        returned: Arc::new(Notify::new()),
    }
}

#[tokio::test]
async fn progress_detects_expired_and_replaced_leases_and_aborts_even_if_error_is_swallowed() {
    let (pool, database) = setup_ephemeral_pool("execution_services_lease_loss", 4).await;
    record_postgres_server_version(&pool, "execution-service lease-loss regression").await;
    for mutation in [
        "lease_expires_at = clock_timestamp() - interval '1 second'",
        "worker_id = 'replacement-worker'",
        "attempt = attempt + 1",
        "run_number = run_number + 1",
    ] {
        let (job_id, job) =
            enqueue_and_claim_job(&pool, JOB_TYPE, 3, json!({}), "services-worker").await;
        let handler = controlled_handler();
        let started = handler.started.clone();
        let write = handler.write.clone();
        let outcome = handler.outcome.clone();
        let observer = RecordingObserver::default();
        let mut registry = JobRegistry::new();
        registry.register(handler.into_job_handler());
        let mut task = tokio::spawn(process_claimed_job_with_observer(
            pool.clone(),
            Arc::new(registry),
            job,
            30,
            observer.lifecycle_observers(),
        ));
        timeout(Duration::from_secs(3), started.notified())
            .await
            .expect("handler starts");
        sqlx::query(&format!("UPDATE job_queue SET {mutation} WHERE id = $1"))
            .bind(job_id)
            .execute(&pool)
            .await
            .expect("invalidate lease");
        write.notify_one();
        await_spawned_task(
            &mut task,
            Duration::from_secs(3),
            "progress lease loss aborts",
            "worker joins",
        )
        .await;
        assert_eq!(*outcome.lock().expect("write outcome"), Some("lease_lost"));
        wait_for_observer_count(|| observer.lease_lost().len(), 1, Duration::from_secs(2)).await;
        let saved = get_job_by_id(&pool, None, job_id)
            .await
            .expect("read abandoned job")
            .expect("job");
        assert_eq!(saved.checkpoint, None);
        assert_eq!(saved.status, JobStatus::Leased);
        assert!(saved.finished_at.is_none());
    }
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn failed_progress_transaction_does_not_acknowledge_or_persist_checkpoint() {
    let (pool, database) = setup_ephemeral_pool("execution_services_failure", 4).await;
    let (job_id, mut job) =
        enqueue_and_claim_job(&pool, JOB_TYPE, 1, json!({}), "services-worker").await;
    job.timeout_seconds = 1;
    let handler = controlled_handler();
    let started = handler.started.clone();
    let write = handler.write.clone();
    let returned = handler.returned.clone();
    let outcome = handler.outcome.clone();
    let mut registry = JobRegistry::new();
    registry.register(handler.into_job_handler());
    let mut task = tokio::spawn(process_claimed_job(
        pool.clone(),
        Arc::new(registry),
        job,
        30,
    ));
    timeout(Duration::from_secs(3), started.notified())
        .await
        .expect("handler starts");
    sqlx::raw_sql("CREATE FUNCTION reject_checkpoint() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.checkpoint IS NOT NULL THEN RAISE EXCEPTION 'injected checkpoint failure'; END IF; RETURN NEW; END $$;
        CREATE TRIGGER reject_checkpoint BEFORE UPDATE ON job_queue FOR EACH ROW EXECUTE FUNCTION reject_checkpoint();")
        .execute(&pool).await.expect("inject persistence failure");
    write.notify_one();
    timeout(Duration::from_secs(3), returned.notified())
        .await
        .expect("write returns error");
    assert_eq!(
        *outcome.lock().expect("outcome"),
        Some("persistence_failed")
    );
    let saved = get_job_by_id(&pool, None, job_id)
        .await
        .expect("read after failed update")
        .expect("job");
    assert_eq!(saved.checkpoint, None);
    await_spawned_task(
        &mut task,
        Duration::from_secs(3),
        "timeout completes",
        "worker joins",
    )
    .await;
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn handler_deadline_bounds_a_progress_write_waiting_for_a_row_lock() {
    for connections in [1, 2, 4] {
        assert_deadline_bounds_blocked_progress(connections).await;
    }
}

async fn assert_deadline_bounds_blocked_progress(connections: u32) {
    let (pool, database) =
        setup_ephemeral_pool("execution_services_blocked_write", connections).await;
    record_postgres_server_version(&pool, "blocked progress with a small pool").await;
    // An independent connection holds the contended row. Even a one-connection
    // worker pool must reclaim its cancelled progress transaction for completion.
    let lock_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(pool.connect_options().as_ref().clone())
        .await
        .expect("lock pool");
    let (job_id, mut job) =
        enqueue_and_claim_job(&pool, JOB_TYPE, 1, json!({}), "services-worker").await;
    job.timeout_seconds = 1;
    let handler = controlled_handler();
    let started = handler.started.clone();
    let write = handler.write.clone();
    let outcome = handler.outcome.clone();
    let mut registry = JobRegistry::new();
    registry.register(handler.into_job_handler());
    let mut task = tokio::spawn(process_claimed_job(
        pool.clone(),
        Arc::new(registry),
        job,
        30,
    ));
    timeout(Duration::from_secs(3), started.notified())
        .await
        .expect("handler starts");
    let mut tx = lock_pool.begin().await.expect("begin row lock");
    sqlx::query("SELECT id FROM job_queue WHERE id = $1 FOR UPDATE")
        .bind(job_id)
        .fetch_one(&mut *tx)
        .await
        .expect("hold row lock");
    write.notify_one();
    sleep(Duration::from_millis(1200)).await;
    assert_ne!(
        *outcome.lock().expect("unacknowledged write"),
        Some("committed")
    );
    tx.rollback()
        .await
        .expect("release row lock after deadline");
    await_spawned_task(
        &mut task,
        Duration::from_secs(4),
        "timeout completes after releasing row lock",
        "worker joins",
    )
    .await;
    let saved = get_job_by_id(&pool, None, job_id)
        .await
        .expect("read timed-out write")
        .expect("job");
    assert_eq!(
        saved.checkpoint, None,
        "cancelled locked update must not commit later"
    );
    assert_eq!(
        saved.last_error_code.as_deref(),
        Some("job.timeout_exceeded")
    );
    lock_pool.close().await;
    teardown_ephemeral_pool(pool, database).await;
}

struct LateCompletionHandler {
    completion: JobCompletion,
    returned: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl JobExecutionHandler for LateCompletionHandler {
    fn job_type(&self) -> JobType<'static> {
        JOB_TYPE
    }

    async fn execute(
        &self,
        execution: JobExecution<'_>,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        // Deliberately cross the cutoff without yielding. The execution branch
        // returns Ready in this poll, so the timer branch cannot enforce policy.
        // This models a slow synchronous handler section or delayed worker poll.
        while execution.remaining_budget() > Duration::ZERO {
            std::thread::sleep(execution.remaining_budget());
        }
        self.returned.fetch_add(1, Ordering::SeqCst);
        Ok(self.completion.clone())
    }
}

#[tokio::test]
async fn a_handler_returning_success_or_continuation_after_the_cutoff_is_timed_out() {
    let (pool, database) = setup_ephemeral_pool("execution_services_late_result", 1).await;
    for completion in [JobCompletion::success(), JobCompletion::continue_now()] {
        let (job_id, mut job) =
            enqueue_and_claim_job(&pool, JOB_TYPE, 1, json!({}), "late-worker").await;
        job.timeout_seconds = 1;
        let returned = Arc::new(AtomicUsize::new(0));
        let mut registry = JobRegistry::new();
        registry.register(
            LateCompletionHandler {
                completion,
                returned: returned.clone(),
            }
            .into_job_handler(),
        );
        process_claimed_job(pool.clone(), Arc::new(registry), job, 30).await;
        assert_eq!(
            returned.load(Ordering::SeqCst),
            1,
            "handler actually returned a result"
        );
        let saved = get_job_by_id(&pool, None, job_id)
            .await
            .expect("read")
            .expect("job");
        assert_eq!(saved.status, JobStatus::DeadLettered);
        assert_eq!(
            saved.run_number, 1,
            "late continuation must not schedule a run"
        );
        assert_eq!(
            saved.last_error_code.as_deref(),
            Some("job.timeout_exceeded")
        );
    }
    teardown_ephemeral_pool(pool, database).await;
}
