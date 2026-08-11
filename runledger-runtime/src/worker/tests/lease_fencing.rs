use super::*;

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
        &JobFailureUpdate::new(
            JobFailureKind::Retryable,
            "job.test.expired_failure",
            "expired failure should not persist",
            Some(1_000),
        )
        .with_retry_timing(JobRetryTiming::After(Duration::from_millis(1_000))),
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
async fn process_claimed_job_terminally_fails_invalid_continuation_delay_without_replay() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_invalid_continuation_delay", 8).await;

    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.invalid_continuation_delay"),
        3,
        json!({"kind":"invalid-continuation-delay"}),
        "worker-invalid-continuation-delay",
    )
    .await;

    let runs = Arc::new(AtomicUsize::new(0));
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(InvalidContinuationDelayHandler {
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
    assert_eq!(dead_letter.failure.code, "job.invalid_continuation_delay");
    assert_eq!(dead_letter.max_attempts, Some(3));

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);
    assert_eq!(persisted.status_reason.as_deref(), Some("TERMINAL"));
    assert_eq!(
        persisted.last_error_code.as_deref(),
        Some("job.invalid_continuation_delay")
    );
    assert!(persisted.worker_id.is_none());
    assert!(persisted.lease_expires_at.is_none());

    let events = list_job_events(&pool, None, job_id, 50, None)
        .await
        .expect("list job events");
    assert!(
        events.iter().all(|event| !matches!(
            event.event_type,
            JobEventType::Requeued | JobEventType::RetryScheduled | JobEventType::Succeeded
        )),
        "invalid continuation delay must not continue, retry, or succeed"
    );
    let failed = events
        .iter()
        .find(|event| event.event_type == JobEventType::Failed)
        .expect("failed event should exist");
    assert_eq!(failed.payload.get("kind"), Some(&json!("TERMINAL")));
    assert_eq!(
        failed.payload.get("error_code"),
        Some(&json!("job.invalid_continuation_delay"))
    );

    reap_expired_leases(&pool, 10, 1_000)
        .await
        .expect("reaper should not requeue terminal invalid continuation");
    let replay_claims = claim_prestart_jobs(&pool, "worker-invalid-continuation-replay", 30, 1)
        .await
        .expect("claim after terminal invalid continuation");
    assert!(replay_claims.is_empty());
    assert_eq!(runs.load(Ordering::SeqCst), 1);

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
    assert!(
        dead_letter
            .failure
            .message
            .contains("Handler returned invalid success progress:")
    );
    assert!(!dead_letter.failure.message.contains("stored progress"));
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
async fn stale_partial_continuation_progress_reports_the_continuation_path() {
    let (pool, database) =
        setup_ephemeral_pool("jobs_worker_stale_partial_continuation_progress", 8).await;
    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.partial_invalid_continuation_progress"),
        3,
        json!({"kind":"stale-partial-continuation-progress"}),
        "worker-stale-partial-continuation-progress",
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
    registry.register(PartialInvalidContinuationProgressHandler {
        runs: runs.clone(),
        dead_letters: dead_letters.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let dead_letters = clone_dead_letters(&dead_letters);
    assert_eq!(dead_letters.len(), 1);
    assert_eq!(
        dead_letters[0].failure.code,
        "job.invalid_completion_progress"
    );
    assert!(
        dead_letters[0]
            .failure
            .message
            .contains("invalid continuation progress:")
    );
    assert!(!dead_letters[0].failure.message.contains("stored progress"));
    assert!(!dead_letters[0].failure.message.contains("invalid success"));
    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load stale continuation progress job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);
    assert!(
        list_job_events(&pool, None, job_id, 20, None)
            .await
            .expect("list stale continuation progress events")
            .iter()
            .all(|event| event.event_type != JobEventType::Requeued)
    );

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
