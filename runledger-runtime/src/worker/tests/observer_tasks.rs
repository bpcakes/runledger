use super::*;

#[tokio::test]
async fn observer_task_owner_drops_newest_terminal_task_at_cap() {
    let tasks = TerminalObserverTasks::owned_with_max_concurrency(2);
    let job = observer_task_test_job();
    let started = Arc::new(AtomicUsize::new(0));
    let (release_tx, release_rx) = watch::channel(false);

    for _ in 0..2 {
        let started = started.clone();
        let mut release_rx = release_rx.clone();
        tasks
            .spawn_terminal(
                async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    while !*release_rx.borrow() {
                        if release_rx.changed().await.is_err() {
                            break;
                        }
                    }
                },
                &job,
            )
            .await;
    }

    assert!(
        wait_for_counter_at_least(&started, 2, Duration::from_millis(500)).await,
        "initial observer tasks should start"
    );
    assert_eq!(tasks.in_flight_count().await, 2);

    let overflow_started = Arc::new(AtomicUsize::new(0));
    let overflow_started_for_task = overflow_started.clone();
    tasks
        .spawn_terminal(
            async move {
                overflow_started_for_task.fetch_add(1, Ordering::SeqCst);
            },
            &job,
        )
        .await;

    sleep(Duration::from_millis(25)).await;
    assert_eq!(
        overflow_started.load(Ordering::SeqCst),
        0,
        "overflow terminal observer task must be dropped instead of executed"
    );
    assert_eq!(tasks.in_flight_count().await, 2);

    release_tx
        .send(true)
        .expect("release receiver should still exist");
    tasks.drain_for_shutdown().await;
}

#[tokio::test]
async fn observer_task_owner_drains_completed_tasks_before_new_admission() {
    let tasks = TerminalObserverTasks::owned_with_max_concurrency(1);
    let job = observer_task_test_job();
    let first_finished = Arc::new(AtomicUsize::new(0));
    let first_finished_for_task = first_finished.clone();

    tasks
        .spawn_terminal(
            async move {
                first_finished_for_task.fetch_add(1, Ordering::SeqCst);
            },
            &job,
        )
        .await;
    assert!(
        wait_for_counter_at_least(&first_finished, 1, Duration::from_millis(500)).await,
        "first observer task should finish"
    );
    assert_eq!(
        tasks.in_flight_count().await,
        1,
        "completed task should remain in the JoinSet until admission drains it"
    );

    let second_started = Arc::new(AtomicUsize::new(0));
    let second_started_for_task = second_started.clone();
    let (release_tx, mut release_rx) = watch::channel(false);
    tasks
        .spawn_terminal(
            async move {
                second_started_for_task.fetch_add(1, Ordering::SeqCst);
                while !*release_rx.borrow() {
                    if release_rx.changed().await.is_err() {
                        break;
                    }
                }
            },
            &job,
        )
        .await;

    assert!(
        wait_for_counter_at_least(&second_started, 1, Duration::from_millis(500)).await,
        "new observer task should be admitted after completed task is drained"
    );
    assert_eq!(tasks.in_flight_count().await, 1);

    release_tx
        .send(true)
        .expect("release receiver should still exist");
    tasks.drain_for_shutdown().await;
}

#[tokio::test]
async fn observer_task_owner_aborts_running_drain_overflow() {
    let tasks = TerminalObserverTasks::owned_with_max_concurrency(1);
    let job = observer_task_test_job();

    let first_running_started = Arc::new(AtomicUsize::new(0));
    let first_running_started_for_task = first_running_started.clone();
    let first_running_task = tokio::spawn(async move {
        first_running_started_for_task.fetch_add(1, Ordering::SeqCst);
        pending::<()>().await;
    });
    assert!(
        wait_for_counter_at_least(&first_running_started, 1, Duration::from_millis(500)).await,
        "first underlying running observer should start"
    );

    tasks
        .spawn_running(
            RunningObserverHandle::new(first_running_task, job),
            tracing::Span::current(),
        )
        .await;
    assert_eq!(tasks.in_flight_count().await, 1);

    let overflow_running_started = Arc::new(AtomicUsize::new(0));
    let overflow_running_started_for_task = overflow_running_started.clone();
    let drops = Arc::new(AtomicUsize::new(0));
    let drop_notify = DropNotify {
        drops: drops.clone(),
    };
    let overflow_running_task = tokio::spawn(async move {
        let _drop_notify = drop_notify;
        overflow_running_started_for_task.fetch_add(1, Ordering::SeqCst);
        pending::<()>().await;
    });
    assert!(
        wait_for_counter_at_least(&overflow_running_started, 1, Duration::from_millis(500)).await,
        "overflow underlying running observer should start"
    );

    tasks
        .spawn_running(
            RunningObserverHandle::new(overflow_running_task, observer_task_test_job()),
            tracing::Span::current(),
        )
        .await;

    assert!(
        wait_for_counter_at_least(&drops, 1, Duration::from_millis(500)).await,
        "dropped running drain should abort the underlying running observer task"
    );
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(tasks.in_flight_count().await, 1);

    tasks.drain_for_shutdown().await;
}

#[tokio::test]
async fn observer_task_owner_runs_terminal_when_running_drains_are_at_cap() {
    let tasks = TerminalObserverTasks::owned_with_max_concurrency(1);
    let running_job = observer_task_test_job();

    let running_started = Arc::new(AtomicUsize::new(0));
    let running_started_for_task = running_started.clone();
    let running_task = tokio::spawn(async move {
        running_started_for_task.fetch_add(1, Ordering::SeqCst);
        pending::<()>().await;
    });
    assert!(
        wait_for_counter_at_least(&running_started, 1, Duration::from_millis(500)).await,
        "underlying running observer should start"
    );

    tasks
        .spawn_running(
            RunningObserverHandle::new(running_task, running_job),
            tracing::Span::current(),
        )
        .await;
    assert_eq!(tasks.in_flight_count().await, 1);

    let terminal_started = Arc::new(AtomicUsize::new(0));
    let terminal_started_for_task = terminal_started.clone();
    tasks
        .spawn_terminal(
            async move {
                terminal_started_for_task.fetch_add(1, Ordering::SeqCst);
            },
            &observer_task_test_job(),
        )
        .await;

    assert!(
        wait_for_counter_at_least(&terminal_started, 1, Duration::from_millis(500)).await,
        "terminal observer should run even while running drains are at cap"
    );

    tasks.drain_for_shutdown().await;
}

#[tokio::test]
async fn empty_lifecycle_observers_skip_running_and_terminal_tasks() {
    let tasks = TerminalObserverTasks::owned_with_max_concurrency(1);
    let observed_job = observer_task_observed_job();
    let observers =
        JobLifecycleObservers::from_arc_observers(Vec::<Arc<dyn JobLifecycleObserver>>::new());
    let mut running_notification =
        JobRunningNotification::spawn(observers.clone(), observed_job.clone());

    assert!(
        running_notification.handle.is_none(),
        "empty observers should not create a running observer task"
    );
    assert_eq!(tasks.in_flight_count().await, 0);

    let queue_record = observer_task_queue_record();
    running_notification
        .spawn_terminal_observer(
            &tasks,
            &queue_record,
            observers,
            TerminalJobObserverEvent::Succeeded(JobSucceededEvent {
                job: observed_job,
                duration: Duration::from_millis(1),
                progress_done: None,
                progress_total: None,
            }),
        )
        .await;

    assert_eq!(
        tasks.in_flight_count().await,
        0,
        "empty observers should not create no-op terminal observer tasks"
    );
}

#[tokio::test]
async fn finished_running_observer_is_reaped_before_terminal_task_admission() {
    let tasks = TerminalObserverTasks::owned_with_max_concurrency(0);
    let observer = RecordingObserver::default();
    let observers = observer.lifecycle_observers();
    let observed_job = observer_task_observed_job();
    let mut running_notification =
        JobRunningNotification::spawn(observers.clone(), observed_job.clone());

    wait_for_observer_count(|| observer.running().len(), 1, Duration::from_millis(500)).await;
    timeout(Duration::from_millis(500), async {
        while !running_notification
            .handle
            .as_ref()
            .is_some_and(|handle| handle.is_finished())
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("running observer task should finish");

    running_notification
        .spawn_terminal_observer(
            &tasks,
            &observer_task_queue_record(),
            observers,
            TerminalJobObserverEvent::Succeeded(JobSucceededEvent {
                job: observed_job,
                duration: Duration::from_millis(1),
                progress_done: None,
                progress_total: None,
            }),
        )
        .await;

    assert!(
        running_notification.handle.is_none(),
        "a finished running callback should be joined inline even when the terminal callback is not admitted"
    );
    assert!(observer.succeeded().is_empty());
    assert_eq!(tasks.in_flight_count().await, 0);
}

#[tokio::test]
async fn terminal_observer_rejection_does_not_abort_same_job_running_observer() {
    let tasks = TerminalObserverTasks::owned_with_max_concurrency(1);
    let cap_job = observer_task_test_job();
    let cap_started = Arc::new(AtomicUsize::new(0));
    let cap_started_for_task = cap_started.clone();
    let (release_tx, mut release_rx) = watch::channel(false);
    assert!(
        tasks
            .spawn_terminal(
                async move {
                    cap_started_for_task.fetch_add(1, Ordering::SeqCst);
                    while !*release_rx.borrow() {
                        if release_rx.changed().await.is_err() {
                            break;
                        }
                    }
                },
                &cap_job,
            )
            .await,
        "initial terminal observer should be admitted"
    );
    assert!(
        wait_for_counter_at_least(&cap_started, 1, Duration::from_millis(500)).await,
        "initial terminal observer should start"
    );
    assert_eq!(tasks.in_flight_count().await, 1);

    let running_started = Arc::new(Notify::new());
    let running_drops = Arc::new(AtomicUsize::new(0));
    let observers = JobLifecycleObservers::from_observer(HangingDropRunningObserver {
        started: running_started.clone(),
        drops: running_drops.clone(),
    });
    let observed_job = observer_task_observed_job();
    let mut running_notification =
        JobRunningNotification::spawn(observers.clone(), observed_job.clone());
    timeout(Duration::from_millis(500), running_started.notified())
        .await
        .expect("running observer should start");

    running_notification
        .spawn_terminal_observer(
            &tasks,
            &observer_task_queue_record(),
            observers,
            TerminalJobObserverEvent::Succeeded(JobSucceededEvent {
                job: observed_job,
                duration: Duration::from_millis(1),
                progress_done: None,
                progress_total: None,
            }),
        )
        .await;

    assert!(
        running_notification.handle.is_some(),
        "rejected terminal admission must not consume the running observer handle"
    );
    sleep(Duration::from_millis(25)).await;
    assert_eq!(
        running_drops.load(Ordering::SeqCst),
        0,
        "rejected terminal admission must not abort the pending running observer"
    );

    release_tx
        .send(true)
        .expect("release receiver should still exist");
    tasks.drain_for_shutdown().await;
    assert_eq!(
        running_drops.load(Ordering::SeqCst),
        0,
        "terminal task cleanup should not abort the still-owned running observer"
    );

    drop(running_notification);
    assert!(
        wait_for_counter_at_least(&running_drops, 1, Duration::from_millis(500)).await,
        "dropping the running notification should still abort the pending running observer"
    );
}

#[tokio::test]
async fn terminal_observer_waits_for_running_observer_for_same_job() {
    let tasks = TerminalObserverTasks::owned_with_max_concurrency(2);
    let events = Arc::new(Mutex::new(Vec::new()));
    let running_started = Arc::new(Notify::new());
    let release_running = Arc::new(Notify::new());
    let terminal_seen = Arc::new(Notify::new());
    let observer = OrderedLifecycleObserver {
        events: events.clone(),
        running_started: running_started.clone(),
        release_running: release_running.clone(),
        terminal_seen: terminal_seen.clone(),
    };
    let observers = JobLifecycleObservers::from_observer(observer);
    let observed_job = observer_task_observed_job();
    let mut running_notification =
        JobRunningNotification::spawn(observers.clone(), observed_job.clone());

    timeout(Duration::from_millis(500), running_started.notified())
        .await
        .expect("running observer should start");

    running_notification
        .spawn_terminal_observer(
            &tasks,
            &observer_task_queue_record(),
            observers,
            TerminalJobObserverEvent::Succeeded(JobSucceededEvent {
                job: observed_job,
                duration: Duration::from_millis(1),
                progress_done: None,
                progress_total: None,
            }),
        )
        .await;

    sleep(Duration::from_millis(25)).await;
    assert_eq!(
        *events
            .lock()
            .expect("ordered observer events lock should not be poisoned"),
        vec!["running"],
        "terminal observer must not run before the running observer completes"
    );

    release_running.notify_one();
    timeout(Duration::from_millis(500), terminal_seen.notified())
        .await
        .expect("terminal observer should run after running observer completes");
    tasks.drain_for_shutdown().await;

    assert_eq!(
        *events
            .lock()
            .expect("ordered observer events lock should not be poisoned"),
        vec!["running", "succeeded"]
    );
}

#[tokio::test]
async fn terminal_observer_shutdown_waits_for_same_job_running_observer() {
    let tasks = TerminalObserverTasks::owned_with_max_concurrency(2);
    let events = Arc::new(Mutex::new(Vec::new()));
    let running_started = Arc::new(Notify::new());
    let release_running = Arc::new(Notify::new());
    let terminal_seen = Arc::new(Notify::new());
    let observer = OrderedLifecycleObserver {
        events: events.clone(),
        running_started: running_started.clone(),
        release_running: release_running.clone(),
        terminal_seen: terminal_seen.clone(),
    };
    let observers = JobLifecycleObservers::from_observer(observer);
    let observed_job = observer_task_observed_job();
    let mut running_notification =
        JobRunningNotification::spawn(observers.clone(), observed_job.clone());

    timeout(Duration::from_millis(500), running_started.notified())
        .await
        .expect("running observer should start");

    running_notification
        .spawn_terminal_observer(
            &tasks,
            &observer_task_queue_record(),
            observers,
            TerminalJobObserverEvent::Succeeded(JobSucceededEvent {
                job: observed_job,
                duration: Duration::from_millis(1),
                progress_done: None,
                progress_total: None,
            }),
        )
        .await;

    let tasks_for_shutdown = tasks.clone();
    let mut drain_task = tokio::spawn(async move {
        tasks_for_shutdown.drain_for_shutdown().await;
    });

    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        *events
            .lock()
            .expect("ordered observer events lock should not be poisoned"),
        vec!["running"],
        "shutdown must not deliver terminal observer before the same-job running observer settles"
    );

    release_running.notify_one();
    timeout(Duration::from_millis(500), terminal_seen.notified())
        .await
        .expect("terminal observer should run after running observer is released");
    await_spawned_task(
        &mut drain_task,
        Duration::from_secs(2),
        "terminal observer shutdown drain should finish after running observer release",
        "terminal observer shutdown drain should not panic",
    )
    .await;

    assert_eq!(
        *events
            .lock()
            .expect("ordered observer events lock should not be poisoned"),
        vec!["running", "succeeded"]
    );
}

#[tokio::test]
async fn terminal_observer_shutdown_delivers_after_multiple_hanging_observer_callbacks() {
    const HANGING_RUNNING_OBSERVERS: usize = 3;
    const HANGING_TERMINAL_OBSERVERS: usize = 3;

    let tasks = TerminalObserverTasks::owned_with_max_concurrency(16);
    let running_calls = Arc::new(AtomicUsize::new(0));
    let hanging_terminal_calls = Arc::new(AtomicUsize::new(0));
    let recording_observer = RecordingObserver::default();
    let mut observer_list: Vec<Arc<dyn JobLifecycleObserver>> = Vec::new();

    for _ in 0..HANGING_RUNNING_OBSERVERS {
        observer_list.push(Arc::new(SlowRunningObserver {
            calls: running_calls.clone(),
        }));
    }

    for _ in 0..HANGING_TERMINAL_OBSERVERS {
        observer_list.push(Arc::new(HangingSucceededObserver {
            calls: hanging_terminal_calls.clone(),
        }));
    }

    observer_list.push(Arc::new(recording_observer.clone()));
    let observers = JobLifecycleObservers::from_arc_observers(observer_list);
    let observed_job = observer_task_observed_job();
    let mut running_notification =
        JobRunningNotification::spawn(observers.clone(), observed_job.clone());

    assert!(
        wait_for_counter_at_least(
            &running_calls,
            HANGING_RUNNING_OBSERVERS,
            Duration::from_millis(90)
        )
        .await,
        "running observer fanout should start every hanging callback before one observer timeout"
    );

    running_notification
        .spawn_terminal_observer(
            &tasks,
            &observer_task_queue_record(),
            observers,
            TerminalJobObserverEvent::Succeeded(JobSucceededEvent {
                job: observed_job,
                duration: Duration::from_millis(1),
                progress_done: None,
                progress_total: None,
            }),
        )
        .await;

    let tasks_for_shutdown = tasks.clone();
    let mut drain_task = tokio::spawn(async move {
        tasks_for_shutdown.drain_for_shutdown().await;
    });
    await_spawned_task(
        &mut drain_task,
        Duration::from_millis(500),
        "terminal observer shutdown drain should finish within the bounded drain policy",
        "terminal observer shutdown drain should not panic",
    )
    .await;

    assert_eq!(
        hanging_terminal_calls.load(Ordering::SeqCst),
        HANGING_TERMINAL_OBSERVERS,
        "terminal observer fanout should start every hanging terminal callback"
    );
    assert_eq!(
        recording_observer.succeeded().len(),
        1,
        "recording terminal observer must be delivered before shutdown returns"
    );
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

#[tokio::test]
async fn run_worker_loop_waits_for_terminal_observer_on_shutdown() {
    let (pool, database) = setup_ephemeral_pool("jobs_worker_terminal_observer_shutdown", 8).await;
    let job_type = JobType::new("jobs.test.handler_panic_successor");

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
            payload: &json!({"kind":"terminal-observer-shutdown"}),
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
    registry.register(LoopSuccessHandler { runs: runs.clone() });

    let observer_calls = Arc::new(AtomicUsize::new(0));
    let observer_started = Arc::new(Notify::new());
    let observer_release = Arc::new(Notify::new());
    let observers = JobLifecycleObservers::from_observer(BlockingSucceededObserver {
        calls: observer_calls.clone(),
        started: observer_started.clone(),
        release: observer_release.clone(),
    });
    let config = JobsConfig {
        worker_id: "terminal-observer-shutdown-worker".to_string(),
        poll_interval: Duration::from_millis(25),
        claim_batch_size: 1,
        lease_ttl_seconds: 30,
        max_global_concurrency: 1,
        reaper_interval: Duration::from_secs(30),
        schedule_poll_interval: Duration::from_secs(30),
        reaper_retry_delay_ms: 1_000,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut worker_task = tokio::spawn(run_worker_loop_with_observer(
        pool.clone(),
        registry,
        config,
        shutdown_rx,
        observers,
    ));

    timeout(Duration::from_secs(5), observer_started.notified())
        .await
        .expect("terminal success observer should start");
    shutdown_tx
        .send(true)
        .expect("shutdown receiver should still be active");

    assert!(
        timeout(Duration::from_millis(25), &mut worker_task)
            .await
            .is_err(),
        "worker loop returned before the terminal success observer was released"
    );

    observer_release.notify_one();
    let exit = await_spawned_task(
        &mut worker_task,
        Duration::from_secs(2),
        "worker loop should return after terminal observer release",
        "worker loop should not panic",
    )
    .await;
    assert_eq!(exit, RuntimeLoopExit::Shutdown);

    let persisted = get_job_by_id(&pool, None, job_id)
        .await
        .expect("load job after terminal observer shutdown")
        .expect("job exists");
    assert_eq!(persisted.status, JobStatus::Succeeded);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(observer_calls.load(Ordering::SeqCst), 1);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn run_worker_loop_shutdown_delivers_terminal_after_hanging_running_observer() {
    const JOB_TYPE: &str = "jobs.test.terminal_after_hanging_running_observer";

    let (pool, database) =
        setup_ephemeral_pool("jobs_worker_terminal_after_hanging_running", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(JOB_TYPE),
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
            job_type: JobType::new(JOB_TYPE),
            organization_id: None,
            payload: &json!({"kind":"terminal-after-hanging-running"}),
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
    registry.register(FixedSuccessHandler {
        job_type_name: JOB_TYPE,
        completion: JobCompletion::success(),
        runs: runs.clone(),
    });

    let running_started = Arc::new(Notify::new());
    let succeeded_seen = Arc::new(Notify::new());
    let succeeded_calls = Arc::new(AtomicUsize::new(0));
    let observers = JobLifecycleObservers::from_observer(HangingRunningSucceededObserver {
        running_started: running_started.clone(),
        succeeded_seen: succeeded_seen.clone(),
        succeeded_calls: succeeded_calls.clone(),
    });
    let config = JobsConfig {
        worker_id: "terminal-after-hanging-running-worker".to_string(),
        poll_interval: Duration::from_millis(25),
        claim_batch_size: 1,
        lease_ttl_seconds: 30,
        max_global_concurrency: 1,
        reaper_interval: Duration::from_secs(30),
        schedule_poll_interval: Duration::from_secs(30),
        reaper_retry_delay_ms: 1_000,
    };
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut worker_task = tokio::spawn(run_worker_loop_with_observer(
        pool.clone(),
        registry,
        config,
        shutdown_rx,
        observers,
    ));

    timeout(Duration::from_secs(5), running_started.notified())
        .await
        .expect("running observer should start");
    wait_for_status(&pool, job_id, JobStatus::Succeeded, Duration::from_secs(5)).await;

    shutdown_tx
        .send(true)
        .expect("shutdown receiver should still be active");
    let exit = await_spawned_task(
        &mut worker_task,
        Duration::from_secs(2),
        "worker loop should wait for terminal observer delivery after running observer timeout",
        "worker loop should not panic",
    )
    .await;
    assert_eq!(exit, RuntimeLoopExit::Shutdown);
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert_eq!(
        succeeded_calls.load(Ordering::SeqCst),
        1,
        "shutdown must not abort the terminal observer before it runs after the running observer timeout"
    );

    timeout(Duration::from_millis(10), succeeded_seen.notified())
        .await
        .expect(
            "terminal observer notification should have been recorded before shutdown returned",
        );

    teardown_ephemeral_pool(pool, database).await;
}
