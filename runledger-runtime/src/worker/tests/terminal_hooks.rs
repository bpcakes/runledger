use super::*;

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
async fn process_claimed_job_cancels_inflight_dead_letter_hook_when_parent_is_aborted() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_dead_letter_hook_cancel", 8).await;

    let (_job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.hanging_dead_letter_hook"),
        1,
        json!({"kind":"dead-letter-hook-cancel"}),
        "worker-dead-letter-hook-cancel",
    )
    .await;

    let runs = Arc::new(AtomicUsize::new(0));
    let hook_started = Arc::new(Notify::new());
    let hook_drops = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(HangingDeadLetterFailureHandler {
        runs: runs.clone(),
        started: hook_started.clone(),
        drops: hook_drops.clone(),
    });

    let job_task = tokio::spawn(process_claimed_job(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        30,
    ));

    timeout(Duration::from_millis(500), hook_started.notified())
        .await
        .expect("dead-letter hook should start");
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    job_task.abort();
    let _ = job_task.await;

    assert!(
        wait_for_counter_at_least(&hook_drops, 1, Duration::from_millis(500)).await,
        "dead-letter hook future should be dropped when the parent job task is aborted"
    );

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
        )
        .retry_not_before_delay(Duration::ZERO),
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
async fn worker_dead_letter_hook_receives_latest_committed_checkpoint() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_dead_letter_checkpoint", 8).await;
    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.dead_letter_latest_checkpoint"),
        3,
        json!({"kind": "dead-letter-checkpoint"}),
        "worker-dead-letter-checkpoint",
    )
    .await;
    assert!(claimed_job.checkpoint.is_none());

    let dead_letter_contexts = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(CheckpointingDeadLetterHandler {
        pool: pool.clone(),
        dead_letter_contexts: dead_letter_contexts.clone(),
    });

    process_claimed_job(pool.clone(), Arc::new(registry), claimed_job, 30).await;

    {
        let contexts = dead_letter_contexts
            .lock()
            .expect("dead-letter contexts lock should not be poisoned");
        assert_eq!(contexts.len(), 1);
        assert_eq!(
            contexts[0].checkpoint,
            Some(json!({"cursor": "persisted-during-handler"}))
        );
    }

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load dead-lettered job")
        .expect("dead-lettered job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);
    assert_eq!(
        persisted.checkpoint,
        Some(json!({"cursor": "persisted-during-handler"}))
    );

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
        )
        .retry_not_before_delay(Duration::ZERO),
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
