use super::*;

#[test]
fn completion_persist_error_diagnostic_omits_internal_query_details() {
    let error =
        runledger_postgres::Error::QueryError(runledger_postgres::QueryError::from_classified(
            runledger_postgres::QueryErrorCategory::Validation,
            "job.test.persist_failed",
            "Persist failed.",
            "trusted diagnostic detail",
        ));

    let diagnostic = completion_persist_error_diagnostic(&error);

    assert!(diagnostic.contains("client_message=\"Persist failed.\""));
    assert!(diagnostic.contains("code=job.test.persist_failed"));
    assert!(!diagnostic.contains("sqlstate"));
    assert!(!diagnostic.contains("constraint"));
    assert!(!diagnostic.contains("internal_message"));
    assert!(!diagnostic.contains("trusted diagnostic detail"));
}

#[tokio::test]
async fn process_claimed_job_observer_reports_success_after_commit() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_observer_success", 8).await;
    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.handler_panic_successor"),
        3,
        json!({"kind":"observer-success"}),
        "worker-observer-success",
    )
    .await;
    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(LoopSuccessHandler { runs: runs.clone() });
    let observer = RecordingObserver::default();

    process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        30,
        observer.lifecycle_observers(),
    )
    .await;
    wait_for_observer_count(|| observer.succeeded().len(), 1, Duration::from_millis(500)).await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after success")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Succeeded);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(observer.running().len(), 1);
    let succeeded = observer.succeeded();
    assert_eq!(succeeded.len(), 1);
    assert_eq!(succeeded[0].job.job_id, job_id);
    assert_eq!(succeeded[0].job.worker_id, "worker-observer-success");
    assert!(observer.failed().is_empty());
    assert!(observer.persist_failed().is_empty());
    assert!(observer.lease_lost().is_empty());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn handler_continuation_reuses_the_job_with_a_fresh_attempt_budget() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_handler_continuation", 8).await;
    let (job_id, first_claim) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.continue_then_success"),
        3,
        json!({"kind": "continuation"}),
        "worker-continuation-first",
    )
    .await;
    let executions = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(ContinueThenSuccessHandler {
        executions: executions.clone(),
    });
    let registry = Arc::new(registry);
    let observer = RecordingObserver::default();

    process_claimed_job_with_observer(
        pool.clone(),
        registry.clone(),
        first_claim,
        30,
        observer.lifecycle_observers(),
    )
    .await;
    wait_for_observer_count(|| observer.continued().len(), 1, Duration::from_millis(500)).await;

    let continued = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load continued job")
        .expect("continued job exists");
    assert_eq!(continued.status, JobStatus::Pending);
    assert_eq!(continued.run_number, 2);
    assert_eq!(continued.attempt, 0);
    assert_eq!(continued.progress_done, Some(1));
    assert_eq!(continued.progress_total, Some(2));
    assert_eq!(continued.checkpoint, Some(json!({"cursor": 1})));
    let continued_events = observer.continued();
    assert_eq!(continued_events.len(), 1);
    assert_eq!(continued_events[0].job.job_id, job_id);
    assert_eq!(continued_events[0].job.run_number, 1);
    assert_eq!(continued_events[0].job.attempt, 1);
    assert_eq!(continued_events[0].next_run_number, 2);
    assert_eq!(continued_events[0].progress_done, Some(1));
    assert_eq!(continued_events[0].progress_total, Some(2));
    assert_eq!(observer.running().len(), 1);
    assert!(observer.succeeded().is_empty());
    assert!(observer.failed().is_empty());
    assert!(observer.persist_failed().is_empty());

    let second_claim = claim_one_job(&pool, "worker-continuation-second").await;
    assert_eq!(second_claim.id, job_id);
    assert_eq!(second_claim.run_number, 2);
    assert_eq!(second_claim.attempt, 1);
    process_claimed_job_with_observer(
        pool.clone(),
        registry,
        second_claim,
        30,
        observer.lifecycle_observers(),
    )
    .await;
    wait_for_observer_count(|| observer.succeeded().len(), 1, Duration::from_millis(500)).await;

    let succeeded = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load succeeded job")
        .expect("succeeded job exists");
    assert_eq!(succeeded.status, JobStatus::Succeeded);
    assert_eq!(succeeded.run_number, 2);
    assert_eq!(succeeded.attempt, 1);
    assert_eq!(succeeded.progress_done, Some(2));
    assert_eq!(succeeded.progress_total, Some(2));
    assert_eq!(succeeded.checkpoint, Some(json!({"cursor": 1})));
    assert_eq!(
        *executions
            .lock()
            .expect("continuation executions lock should not be poisoned"),
        vec![
            ContinuationExecution {
                run_number: 1,
                attempt: 1,
                checkpoint: None,
            },
            ContinuationExecution {
                run_number: 2,
                attempt: 1,
                checkpoint: Some(json!({"cursor": 1})),
            },
        ]
    );
    assert_eq!(observer.succeeded().len(), 1);
    assert_eq!(observer.continued().len(), 1);
    assert_eq!(observer.running().len(), 2);
    assert!(observer.failed().is_empty());

    let attempts = sqlx::query_as::<_, (i32, i32, bool, Option<String>)>(
        "SELECT run_number, attempt, finished_at IS NOT NULL, outcome::text
         FROM job_attempts
         WHERE job_id = $1
         ORDER BY run_number, attempt",
    )
    .bind(job_id)
    .fetch_all(&pool)
    .await
    .expect("load continuation attempts");
    assert_eq!(attempts, vec![(1, 1, true, None), (2, 1, true, None)]);

    let events = list_job_events(&pool, None, job_id, 20, None)
        .await
        .expect("list continuation lifecycle events");
    let continuation_event = events
        .iter()
        .find(|event| event.event_type == JobEventType::Requeued)
        .expect("handler continuation should write a requeued event");
    assert_eq!(
        continuation_event
            .payload
            .get("reason")
            .and_then(Value::as_str),
        Some("HANDLER_CONTINUATION")
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn continuation_lease_mismatch_reports_lease_loss_instead_of_persist_failure() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_continuation_lease_loss", 8).await;
    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.continuation_lease_loss"),
        3,
        json!({"kind": "continuation-lease-loss"}),
        "worker-continuation-lease-loss",
    )
    .await;
    let mut registry = JobRegistry::new();
    registry.register(ExpireLeaseThenContinueHandler { pool: pool.clone() });
    let observer = RecordingObserver::default();

    process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        30,
        observer.lifecycle_observers(),
    )
    .await;
    wait_for_observer_count(
        || observer.lease_lost().len(),
        1,
        Duration::from_millis(500),
    )
    .await;

    let lease_lost = observer.lease_lost();
    assert_eq!(lease_lost.len(), 1);
    assert_eq!(lease_lost[0].job.job_id, job_id);
    assert_eq!(lease_lost[0].failure.kind, JobFailureKind::LeaseExpired);
    assert_eq!(lease_lost[0].failure.code, "job.lease_owner_mismatch");
    assert!(observer.persist_failed().is_empty());
    assert!(observer.continued().is_empty());

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load expired continuation lease")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Leased);
    assert_eq!(persisted.run_number, 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn success_and_failure_lease_mismatches_report_lease_loss() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_terminal_lease_loss", 8).await;
    let observer = RecordingObserver::default();

    let (success_job_id, success_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.success_lease_loss"),
        3,
        json!({"kind": "success-lease-loss"}),
        "worker-success-lease-loss",
    )
    .await;
    let mut success_registry = JobRegistry::new();
    success_registry.register(ExpireLeaseThenSucceedHandler { pool: pool.clone() });
    process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(success_registry),
        success_job,
        30,
        observer.lifecycle_observers(),
    )
    .await;

    let (failure_job_id, failure_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.failure_lease_loss"),
        3,
        json!({"kind": "failure-lease-loss"}),
        "worker-failure-lease-loss",
    )
    .await;
    let mut failure_registry = JobRegistry::new();
    failure_registry.register(ExpireLeaseThenFailHandler { pool: pool.clone() });
    process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(failure_registry),
        failure_job,
        30,
        observer.lifecycle_observers(),
    )
    .await;

    wait_for_observer_count(
        || observer.lease_lost().len(),
        2,
        Duration::from_millis(500),
    )
    .await;
    let lease_lost = observer.lease_lost();
    assert_eq!(lease_lost.len(), 2);
    assert!(
        lease_lost
            .iter()
            .any(|event| event.job.job_id == success_job_id)
    );
    assert!(
        lease_lost
            .iter()
            .any(|event| event.job.job_id == failure_job_id)
    );
    assert!(
        lease_lost
            .iter()
            .all(|event| event.failure.kind == JobFailureKind::LeaseExpired)
    );
    assert!(observer.persist_failed().is_empty());
    assert!(observer.succeeded().is_empty());
    assert!(observer.failed().is_empty());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn opted_in_workflow_managed_handler_continuation_runs_again_then_succeeds() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_workflow_continuation", 8).await;
    let job_type = JobType::new("jobs.test.continue_then_success");
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
    .expect("upsert workflow job definition");
    tx.commit().await.expect("commit definition transaction");

    let payload = json!({"kind": "workflow-continuation"});
    let metadata = json!({"test": "workflow-continuation"});
    let step = WorkflowStepEnqueueBuilder::new(StepKey::new("step"), job_type, &payload)
        .allow_handler_continuation()
        .try_build()
        .expect("build workflow step");
    let workflow =
        WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test.continuation"), &metadata)
            .step(step)
            .try_build()
            .expect("build workflow");
    let run = enqueue_workflow_run(&pool, &workflow)
        .await
        .expect("enqueue workflow");
    let job_id = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list workflow steps")
        .into_iter()
        .next()
        .and_then(|step| step.job_id)
        .expect("workflow step job should be released");
    let claim = claim_one_job(&pool, "worker-workflow-continuation").await;
    assert_eq!(claim.id, job_id);

    let executions = Arc::new(Mutex::new(Vec::new()));
    let mut registry = JobRegistry::new();
    registry.register(ContinueThenSuccessHandler {
        executions: executions.clone(),
    });
    let registry = Arc::new(registry);
    process_claimed_job(pool.clone(), registry.clone(), claim, 30).await;

    let continued = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load workflow job")
        .expect("workflow job exists");
    assert_eq!(continued.status, JobStatus::Pending);
    assert_eq!(continued.run_number, 2);
    assert_eq!(continued.attempt, 0);
    assert_eq!(continued.progress_done, Some(1));
    assert_eq!(continued.progress_total, Some(2));
    assert_eq!(continued.checkpoint, Some(json!({"cursor": 1})));
    assert_eq!(
        *executions
            .lock()
            .expect("continuation executions lock should not be poisoned"),
        vec![ContinuationExecution {
            run_number: 1,
            attempt: 1,
            checkpoint: None,
        }]
    );
    let continued_steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list continued workflow steps");
    assert_eq!(continued_steps[0].status, WorkflowStepStatus::Enqueued);

    let second_claim = claim_one_job(&pool, "worker-workflow-continuation-final").await;
    assert_eq!(second_claim.id, job_id);
    assert_eq!(second_claim.run_number, 2);
    assert_eq!(second_claim.attempt, 1);
    process_claimed_job(pool.clone(), registry, second_claim, 30).await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load terminal workflow job")
        .expect("workflow job exists");
    assert_eq!(persisted.status, JobStatus::Succeeded);
    assert_eq!(persisted.run_number, 2);
    assert_eq!(persisted.attempt, 1);
    assert_eq!(
        *executions
            .lock()
            .expect("continuation executions lock should not be poisoned"),
        vec![
            ContinuationExecution {
                run_number: 1,
                attempt: 1,
                checkpoint: None,
            },
            ContinuationExecution {
                run_number: 2,
                attempt: 1,
                checkpoint: Some(json!({"cursor": 1})),
            },
        ]
    );
    let steps = list_workflow_steps(&pool, None, run.id)
        .await
        .expect("list terminal workflow steps");
    assert_eq!(steps[0].status, WorkflowStepStatus::Succeeded);
    assert_eq!(
        list_job_events(&pool, None, job_id, 20, None)
            .await
            .expect("list workflow job events")
            .iter()
            .filter(|event| event.event_type == JobEventType::Requeued)
            .count(),
        1
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_success_observer_reports_committed_coalesced_progress() {
    const JOB_TYPE: &str = "jobs.test.observer.coalesced_success_progress";

    let (pool, database) = setup_ephemeral_pool("jobs_worker_observer_success_progress", 8).await;
    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new(JOB_TYPE),
        3,
        json!({"kind":"observer-success-progress"}),
        "worker-observer-success-progress",
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
    .expect("persist existing progress before success");

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(FixedSuccessHandler {
        job_type_name: JOB_TYPE,
        completion: {
            let mut completion = JobCompletion::success();
            completion.progress_done = Some(7);
            completion
        },
        runs: runs.clone(),
    });
    let observer = RecordingObserver::default();

    process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        30,
        observer.lifecycle_observers(),
    )
    .await;
    wait_for_observer_count(|| observer.succeeded().len(), 1, Duration::from_millis(500)).await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after success")
        .expect("job exists");
    let succeeded = observer.succeeded();
    assert_eq!(succeeded.len(), 1);
    assert_eq!(persisted.status, JobStatus::Succeeded);
    assert_eq!(persisted.progress_done, Some(7));
    assert_eq!(persisted.progress_total, Some(10));
    assert_eq!(succeeded[0].progress_done, persisted.progress_done);
    assert_eq!(succeeded[0].progress_total, persisted.progress_total);
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_does_not_block_handler_or_heartbeats_on_slow_running_observers() {
    const JOB_TYPE: &str = "jobs.test.slow_running_observer_heartbeat";
    const LEASE_TTL_SECONDS: i32 = 2;
    const SLOW_RUNNING_OBSERVERS: usize = 25;

    let (pool, database) = setup_ephemeral_pool("jobs_worker_slow_running_observer", 8).await;
    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(JOB_TYPE),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 10,
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
            job_type: JobType::new(JOB_TYPE),
            organization_id: None,
            payload: &json!({"kind":"slow-running-observer-heartbeat"}),
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
    let mut claimed =
        claim_prestart_jobs(&pool, "worker-slow-running-observer", LEASE_TTL_SECONDS, 1)
            .await
            .expect("claim job with short lease");
    let claimed_job = claimed.pop().expect("expected one claimed job");

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(SlowSuccessHandler {
        job_type_name: JOB_TYPE,
        runs: runs.clone(),
        sleep_for: Duration::from_millis(2_200),
    });

    let running_calls = Arc::new(AtomicUsize::new(0));
    let observers: Vec<Arc<dyn JobLifecycleObserver>> = (0..SLOW_RUNNING_OBSERVERS)
        .map(|_| {
            Arc::new(SlowRunningObserver {
                calls: running_calls.clone(),
            }) as Arc<dyn JobLifecycleObserver>
        })
        .collect();
    let observers = JobLifecycleObservers::from_arc_observers(observers);

    let mut job_task = tokio::spawn(process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        LEASE_TTL_SECONDS,
        observers,
    ));

    if !wait_for_counter_at_least(&runs, 1, Duration::from_millis(500)).await {
        job_task.abort();
        let _ = job_task.await;
        teardown_ephemeral_pool(pool, database).await;
        panic!("handler should start before slow running observers serially time out");
    }

    await_spawned_task(
        &mut job_task,
        Duration::from_secs(8),
        "job processing should finish without waiting for slow running observers to time out",
        "job processing should not panic",
    )
    .await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after slow running observer test")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Succeeded);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert!(
        running_calls.load(Ordering::SeqCst) >= 1,
        "running observer fanout should have started"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_dead_letter_hook_is_not_delayed_by_slow_running_observers() {
    const JOB_TYPE: &str = "jobs.test.dead_letter_not_delayed_by_running_observer";
    const SLOW_RUNNING_OBSERVERS: usize = 8;

    let (pool, database) = setup_ephemeral_pool("jobs_worker_dead_letter_observer_order", 8).await;
    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new(JOB_TYPE),
        3,
        json!({"kind":"dead-letter-observer-order"}),
        "worker-dead-letter-observer-order",
    )
    .await;

    let runs = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let dead_letter_notified = Arc::new(Notify::new());
    let mut registry = JobRegistry::new();
    registry.register(ControlledDeadLetterFailureHandler {
        job_type_name: JOB_TYPE,
        runs: runs.clone(),
        release: release.clone(),
        dead_letter_notified: dead_letter_notified.clone(),
    });

    let running_calls = Arc::new(AtomicUsize::new(0));
    let observers: Vec<Arc<dyn JobLifecycleObserver>> = (0..SLOW_RUNNING_OBSERVERS)
        .map(|_| {
            Arc::new(SlowRunningObserver {
                calls: running_calls.clone(),
            }) as Arc<dyn JobLifecycleObserver>
        })
        .collect();
    let observers = JobLifecycleObservers::from_arc_observers(observers);

    let mut job_task = tokio::spawn(process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        30,
        observers,
    ));

    assert!(
        wait_for_counter_at_least(&runs, 1, Duration::from_millis(500)).await,
        "handler should start"
    );
    assert!(
        wait_for_counter_at_least(&running_calls, 1, Duration::from_millis(500)).await,
        "running observer should start"
    );

    release.notify_waiters();
    timeout(Duration::from_millis(500), dead_letter_notified.notified())
        .await
        .expect("dead-letter hook should not wait for running observer timeouts");
    await_spawned_task(
        &mut job_task,
        Duration::from_millis(500),
        "worker task should complete after terminal dead-letter hook",
        "worker task should not panic",
    )
    .await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_observer_reports_retryable_failure_after_commit() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_observer_retry_failure", 8).await;
    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.observer.retry_failure"),
        3,
        json!({"kind":"observer-retry-failure"}),
        "worker-observer-retry-failure",
    )
    .await;
    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(FailingHandler {
        job_type_name: "jobs.test.observer.retry_failure",
        failure: JobFailure::retryable("job.test.retry", "retryable failure"),
        runs: runs.clone(),
    });
    let observer = RecordingObserver::default();

    process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        30,
        observer.lifecycle_observers(),
    )
    .await;
    wait_for_observer_count(|| observer.failed().len(), 1, Duration::from_millis(500)).await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after retryable failure")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Pending);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(observer.running().len(), 1);
    let failed = observer.failed();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].job.job_id, job_id);
    assert_eq!(failed[0].failure.kind, JobFailureKind::Retryable);
    assert_eq!(failed[0].failure.code, "job.test.retry");
    assert!(matches!(
        failed[0].disposition,
        JobFailureDisposition::RetryScheduled { .. }
    ));
    assert!(observer.succeeded().is_empty());
    assert!(observer.persist_failed().is_empty());
    assert!(observer.lease_lost().is_empty());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_observer_reports_absolute_retry_time_after_commit() {
    let (pool, database) =
        setup_ephemeral_pool("jobs_worker_observer_absolute_retry_failure", 8).await;
    let (job_id, claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.observer.absolute_retry_failure"),
        3,
        json!({"kind":"observer-absolute-retry-failure"}),
        "worker-observer-absolute-retry-failure",
    )
    .await;
    let requested_retry_at = database_now(&pool).await + ChronoDuration::minutes(5);
    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(FailingHandler {
        job_type_name: "jobs.test.observer.absolute_retry_failure",
        failure: JobFailure::retryable(
            "job.test.provider_rate_limited",
            "provider supplied an absolute reset time",
        )
        .retry_not_before(requested_retry_at),
        runs: runs.clone(),
    });
    let observer = RecordingObserver::default();

    process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        30,
        observer.lifecycle_observers(),
    )
    .await;
    wait_for_observer_count(|| observer.failed().len(), 1, Duration::from_millis(500)).await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after absolute retryable failure")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Pending);
    assert_eq!(persisted.next_run_at, requested_retry_at);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let failed = observer.failed();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].job.job_id, job_id);
    assert_eq!(failed[0].failure.kind, JobFailureKind::Retryable);
    assert_eq!(failed[0].failure.code, "job.test.provider_rate_limited");
    assert_eq!(
        failed[0].disposition,
        JobFailureDisposition::RetryScheduledAt {
            requested_retry_at,
            next_run_at: requested_retry_at,
        }
    );
    assert!(observer.succeeded().is_empty());
    assert!(observer.persist_failed().is_empty());
    assert!(observer.lease_lost().is_empty());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_observer_reports_dead_letter_failure_from_completion_outcome() {
    let (pool, database) =
        setup_ephemeral_pool("jobs_worker_observer_dead_letter_failure", 8).await;
    let (job_id, mut claimed_job) = enqueue_and_claim_job(
        &pool,
        JobType::new("jobs.test.observer.dead_letter_failure"),
        1,
        json!({"kind":"observer-dead-letter-failure"}),
        "worker-observer-dead-letter-failure",
    )
    .await;
    claimed_job.max_attempts = 99;

    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(FailingHandler {
        job_type_name: "jobs.test.observer.dead_letter_failure",
        failure: JobFailure::retryable(
            "job.test.retryable_exhausted",
            "retryable failure should exhaust attempts",
        )
        .retry_not_before_delay(Duration::ZERO),
        runs: runs.clone(),
    });
    let observer = RecordingObserver::default();

    process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        30,
        observer.lifecycle_observers(),
    )
    .await;
    wait_for_observer_count(|| observer.failed().len(), 1, Duration::from_millis(500)).await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after dead-letter failure")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::DeadLettered);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    let failed = observer.failed();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].job.job_id, job_id);
    assert_eq!(failed[0].job.max_attempts, 1);
    assert_eq!(failed[0].failure.kind, JobFailureKind::Retryable);
    assert_eq!(failed[0].failure.code, "job.test.retryable_exhausted");
    assert_eq!(
        failed[0].disposition,
        JobFailureDisposition::DeadLettered {
            reason: JobDeadLetterReason::AttemptsExhausted,
        }
    );
    assert!(observer.succeeded().is_empty());
    assert!(observer.persist_failed().is_empty());
    assert!(observer.lease_lost().is_empty());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn process_claimed_job_observer_reports_timeout_failure_after_commit() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_observer_timeout_failure", 8).await;
    let job_type = JobType::new("jobs.test.observer.timeout");
    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type,
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 1,
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
            payload: &json!({"kind":"observer-timeout"}),
            priority: None,
            max_attempts: None,
            timeout_seconds: Some(1),
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue job");
    let claimed_job = claim_one_job(&pool, "worker-observer-timeout").await;
    let runs = Arc::new(AtomicUsize::new(0));
    let mut registry = JobRegistry::new();
    registry.register(HangingHandler {
        job_type_name: "jobs.test.observer.timeout",
        runs: runs.clone(),
    });
    let observer = RecordingObserver::default();

    process_claimed_job_with_observer(
        pool.clone(),
        Arc::new(registry),
        claimed_job,
        30,
        observer.lifecycle_observers(),
    )
    .await;
    wait_for_observer_count(|| observer.failed().len(), 1, Duration::from_millis(500)).await;

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after timeout")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Pending);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(observer.running().len(), 1);
    let failed = observer.failed();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].job.job_id, job_id);
    assert_eq!(failed[0].failure.kind, JobFailureKind::Timeout);
    assert_eq!(failed[0].failure.code, "job.timeout_exceeded");
    assert!(matches!(
        failed[0].disposition,
        JobFailureDisposition::RetryScheduled { .. }
    ));
    assert!(observer.succeeded().is_empty());
    assert!(observer.persist_failed().is_empty());
    assert!(observer.lease_lost().is_empty());

    teardown_ephemeral_pool(pool, database).await;
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
        |_| {
            JobFailure::retryable(
                "job.test.waiting_for_external_refresh",
                "waiting for external refresh",
            )
        },
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
async fn process_claimed_job_handler_retry_after_cannot_shorten_registered_override() {
    const OVERRIDE_RETRY_DELAY_MS: i32 = 120_000;

    let observation = observe_retry_delay_override_failure(
        "jobs_worker_handler_retry_after",
        "jobs.test.handler_retry_after",
        |_| {
            JobFailure::retryable(
                "job.test.waiting_for_external_refresh",
                "provider supplied a relative reset delay",
            )
            .retry_not_before_delay(Duration::from_secs(45))
        },
        3,
        Some((
            JobType::new("jobs.test.handler_retry_after"),
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
    assert!(observation.retry_event_requested_retry_at.is_some());
    assert_eq!(observation.retry_event_count, 1);
    assert_eq!(
        observation.attempt_retry_delay_ms,
        Some(OVERRIDE_RETRY_DELAY_MS)
    );
    assert_next_run_at_around_delay(&observation, OVERRIDE_RETRY_DELAY_MS);
}

#[tokio::test]
async fn process_claimed_job_handler_not_before_sets_effective_schedule_beyond_override() {
    const OVERRIDE_RETRY_DELAY_MS: i32 = 120_000;

    let observation = observe_retry_delay_override_failure(
        "jobs_worker_handler_retry_at",
        "jobs.test.handler_retry_at",
        |db_now_before| {
            JobFailure::retryable(
                "job.test.waiting_for_external_refresh",
                "provider supplied an absolute reset time",
            )
            .retry_not_before(db_now_before + ChronoDuration::minutes(5))
        },
        3,
        Some((
            JobType::new("jobs.test.handler_retry_at"),
            "job.test.waiting_for_external_refresh",
            OVERRIDE_RETRY_DELAY_MS,
        )),
    )
    .await;

    let requested_retry_at = observation
        .retry_event_requested_retry_at
        .expect("absolute retry event should record the provider reset time");
    assert_eq!(observation.runs, 1);
    assert_eq!(observation.status, JobStatus::Pending);
    assert_eq!(
        observation.retry_event_delay_ms,
        Some(i64::from(OVERRIDE_RETRY_DELAY_MS))
    );
    assert_eq!(observation.retry_event_count, 1);
    assert_eq!(
        observation.attempt_retry_delay_ms,
        Some(OVERRIDE_RETRY_DELAY_MS)
    );
    assert_eq!(observation.next_run_at, requested_retry_at);
    assert_eq!(
        requested_retry_at,
        observation.db_now_before + ChronoDuration::minutes(5)
    );
}

#[tokio::test]
async fn process_claimed_job_zero_handler_retry_bound_falls_back_to_override() {
    const OVERRIDE_RETRY_DELAY_MS: i32 = 120_000;

    let observation = observe_retry_delay_override_failure(
        "jobs_worker_invalid_handler_retry_timing",
        "jobs.test.invalid_handler_retry_timing",
        |_| {
            JobFailure::retryable(
                "job.test.waiting_for_external_refresh",
                "provider supplied an empty reset delay",
            )
            .retry_not_before_delay(Duration::ZERO)
        },
        3,
        Some((
            JobType::new("jobs.test.invalid_handler_retry_timing"),
            "job.test.waiting_for_external_refresh",
            OVERRIDE_RETRY_DELAY_MS,
        )),
    )
    .await;

    assert_eq!(observation.runs, 1);
    assert_eq!(observation.status, JobStatus::Pending);
    assert_eq!(observation.retry_event_count, 1);
    assert_eq!(
        observation.retry_event_delay_ms,
        Some(i64::from(OVERRIDE_RETRY_DELAY_MS))
    );
    assert_eq!(observation.retry_event_requested_retry_at, None);
    assert_eq!(
        observation.attempt_retry_delay_ms,
        Some(OVERRIDE_RETRY_DELAY_MS)
    );
    assert_eq!(
        observation.failed_event_error_code.as_deref(),
        Some("job.test.waiting_for_external_refresh")
    );
}

#[tokio::test]
async fn process_claimed_job_does_not_apply_override_to_other_job_type() {
    const OVERRIDE_RETRY_DELAY_MS: i32 = 120_000;

    let observation = observe_retry_delay_override_failure(
        "jobs_worker_retry_override_type",
        "jobs.test.retry_override.other",
        |_| {
            JobFailure::retryable(
                "job.test.waiting_for_external_refresh",
                "waiting for external refresh",
            )
        },
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
        |_| {
            JobFailure::retryable(
                "job.test.other_waiting_reason",
                "waiting for a different reason",
            )
        },
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
        |_| {
            JobFailure::terminal(
                "job.test.waiting_for_external_refresh",
                "terminal failure with matching code",
            )
            .retry_not_before_delay(Duration::ZERO)
        },
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
    assert_eq!(observation.retry_event_requested_retry_at, None);
    assert_eq!(observation.retry_event_count, 0);
    assert_eq!(
        observation.failed_event_error_code.as_deref(),
        Some("job.test.waiting_for_external_refresh")
    );
    assert_eq!(observation.attempt_retry_delay_ms, None);
}
