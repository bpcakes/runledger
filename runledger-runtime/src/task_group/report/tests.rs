use crate::{
    RuntimeLoopExit, RuntimeShutdownBudget, RuntimeShutdownCause, RuntimeShutdownFailure,
    RuntimeShutdownSettlement,
    settlement::TaskRegistry,
    shutdown::{self, ShutdownSignal},
    task_group::{RuntimeTask, TaskGroup},
};
use futures_util::FutureExt;
use std::{
    future::{Future, pending},
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};

fn budget() -> RuntimeShutdownBudget {
    RuntimeShutdownBudget::new(Duration::from_millis(20), Duration::from_millis(20))
        .expect("valid budget")
}

/// Construct a valid budget at the paused clock's platform-specific instant
/// boundary, then move the first-stop clock beyond that boundary.
async fn budget_that_overflows_at_shutdown() -> RuntimeShutdownBudget {
    let now = tokio::time::Instant::now();
    let (mut valid, mut upper) = (0_u64, u64::MAX);
    while valid < upper {
        let candidate = valid + (upper - valid) / 2 + 1;
        if now.checked_add(Duration::from_secs(candidate)).is_some() {
            valid = candidate;
        } else {
            upper = candidate - 1;
        }
    }
    let total = Duration::from_secs(valid);
    let budget = RuntimeShutdownBudget::new(total - Duration::from_secs(1), Duration::from_secs(1))
        .expect("budget is representable when constructed");
    tokio::time::advance(Duration::from_secs(2)).await;
    assert!(budget.deadlines(tokio::time::Instant::now()).is_none());
    budget
}

#[tokio::test(start_paused = true)]
async fn a_deadline_rejected_at_stop_uses_zero_allowances_and_retains_its_total() {
    let budget = budget_that_overflows_at_shutdown().await;
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let mut tasks =
        TaskGroup::from_tasks_for_tests(vec![RuntimeTask::spawn("stubborn", pending())]);
    let started = tokio::time::Instant::now();
    let report = tasks
        .run_report_with_signal(async {}, budget, &shutdown, &descendants)
        .now_or_never()
        .expect("rejected deadlines allow only immediately ready joins");

    assert_eq!(tokio::time::Instant::now(), started);
    assert!(matches!(
        report.deadline_error(),
        Some(crate::RuntimeError::ShutdownTimeoutTooLarge { timeout })
            if *timeout == budget.total_allowance()
    ));
    assert!(report.graceful_timed_out());
    assert!(report.abort_timed_out());
    assert_eq!(report.unjoined().len(), 1);
    assert!(report.unjoined()[0].abort_requested);
    assert!(!report.is_success());
    assert!(!report.is_cooperatively_stopped());
}

#[tokio::test(start_paused = true)]
async fn interruption_after_deadline_rejection_retains_the_rejected_total() {
    let budget = budget_that_overflows_at_shutdown().await;
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let mut tasks =
        TaskGroup::from_tasks_for_tests(vec![RuntimeTask::spawn("stubborn", pending())]);
    let reached_final_harvest = std::cell::Cell::new(false);
    let before_final_harvest = async {
        reached_final_harvest.set(true);
        pending::<()>().await;
    };
    assert!(
        tasks
            .run_report_with_final_harvest_hook(
                async {},
                budget,
                &shutdown,
                &descendants,
                before_final_harvest,
            )
            .now_or_never()
            .is_none()
    );
    assert!(reached_final_harvest.get());
    let report = tasks.interrupted_report(&shutdown, &descendants);

    assert!(matches!(
        report.deadline_error(),
        Some(crate::RuntimeError::ShutdownTimeoutTooLarge { timeout })
            if *timeout == budget.total_allowance()
    ));
    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::Interrupted { .. }
    ));
    // An interrupted report never claims a completed timeout boundary.
    assert!(!report.graceful_timed_out());
    assert!(!report.abort_timed_out());
    assert_eq!(report.unjoined().len(), 1);
    assert!(report.unjoined()[0].abort_requested);
    assert!(!report.is_success());
    assert!(!report.is_cooperatively_stopped());
}

fn descendant_failure(task: &'static str) -> RuntimeShutdownCause {
    let handle = tokio::spawn(async {});
    let id = handle.id();
    handle.abort();
    RuntimeShutdownCause::DescendantFailure { task, id }
}

struct CompleteAfterPollSignal {
    entered: Option<std::sync::mpsc::Sender<()>>,
    release: std::sync::mpsc::Receiver<()>,
    exit: RuntimeLoopExit,
}

impl Future for CompleteAfterPollSignal {
    type Output = RuntimeLoopExit;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let task = self.as_mut().get_mut();
        if let Some(entered) = task.entered.take() {
            entered.send(()).expect("test observes final loop poll");
        }
        task.release.recv().expect("test releases final loop poll");
        Poll::Ready(task.exit)
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn earlier_parent_deadline_interrupts_active_abort_observation() {
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let (release, blocked) = std::sync::mpsc::channel();
    let (entered, entry) = tokio::sync::oneshot::channel();
    drop(descendants.spawn("blocked_callback", async move {
        entered.send(()).expect("callback entry observed");
        blocked
            .recv_timeout(Duration::from_secs(10))
            .expect("callback released");
    }));
    entry.await.expect("callback is non-yielding");
    shutdown.request_with(descendant_failure("blocked_callback"));
    let native_stop = shutdown.clone();
    let owned_descendants = descendants.clone();
    let mut driver = tokio::spawn(async move {
        TaskGroup::new()
            .run_report_with_signal(
                pending(),
                RuntimeShutdownBudget::new(Duration::ZERO, Duration::from_secs(30))
                    .expect("valid budget"),
                &native_stop,
                &owned_descendants,
            )
            .await
    });
    let abort_observed = tokio::time::timeout(Duration::from_secs(1), async {
        while !descendants
            .snapshot()
            .1
            .iter()
            .any(|task| task.abort_requested)
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    shutdown
        .handle()
        .request_since(tokio::time::Instant::now() - Duration::from_secs(60));
    let observed = tokio::time::timeout(Duration::from_secs(1), &mut driver).await;
    // Always release and observe the real descendant before assertions or errors.
    release.send(()).expect("release retained callback");
    descendants.wait().await;
    let (timely, report) = match observed {
        Ok(report) => (true, report.expect("native driver joined")),
        Err(_) => (
            false,
            driver.await.expect("native driver joined after release"),
        ),
    };
    assert!(
        abort_observed.is_ok(),
        "native abort phase was never observed"
    );
    assert!(
        timely,
        "active native abort observation ignored tightened clock"
    );
    assert!(report.abort_timed_out());
    assert!(matches!(
        report.cause(),
        RuntimeShutdownCause::DescendantFailure {
            task: "blocked_callback",
            ..
        }
    ));
    assert_eq!(report.unjoined().len(), 1);
    assert!(!report.is_cooperatively_stopped());
}

#[tokio::test(start_paused = true)]
async fn earlier_parent_stop_tightens_a_running_native_settlement() {
    use futures_util::FutureExt;
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let mut tasks = TaskGroup::from_tasks_for_tests(vec![RuntimeTask::spawn("blocked", pending())]);
    let parent_started = tokio::time::Instant::now();
    tokio::time::advance(Duration::from_secs(5)).await;
    shutdown.request_with(descendant_failure("blocked"));
    let allowance = RuntimeShutdownBudget::new(Duration::from_secs(10), Duration::from_secs(2))
        .expect("valid phase budget");
    let driver = tasks.run_report_with_signal(pending(), allowance, &shutdown, &descendants);
    tokio::pin!(driver);
    assert!(driver.as_mut().now_or_never().is_none());
    tokio::time::advance(Duration::from_secs(6)).await;
    assert!(driver.as_mut().now_or_never().is_none());
    shutdown.handle().request_since(parent_started);
    let report = driver.await;
    assert!(tokio::time::Instant::now() <= parent_started + Duration::from_secs(12));
    assert!(report.graceful_timed_out());
    assert!(matches!(
        report.cause(),
        RuntimeShutdownCause::DescendantFailure {
            task: "blocked",
            ..
        }
    ));
    assert!(report.loops()[0].abort_requested);
}

#[tokio::test(start_paused = true)]
async fn retains_initial_loop_failure_and_later_shutdown_panic() {
    let (shutdown, mut receiver) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let mut tasks = TaskGroup::from_tasks_for_tests(vec![
        RuntimeTask::spawn("first", async { RuntimeLoopExit::Completed }),
        RuntimeTask::spawn("second", async move {
            shutdown::wait_for_request(&mut receiver).await;
            panic!("second failure during shutdown");
        }),
    ]);
    let report = tasks
        .run_report_with_signal(pending(), budget(), &shutdown, &descendants)
        .await;
    assert_eq!(report.cause(), RuntimeShutdownCause::LoopFailure("first"));
    assert_eq!(report.loops().len(), 2);
    assert!(
        report
            .loops()
            .iter()
            .any(|record| matches!(record.result, Ok(RuntimeLoopExit::Completed)))
    );
    assert!(
        report
            .loops()
            .iter()
            .any(|record| record.result.as_ref().is_err_and(|error| error.is_panic()))
    );
    assert!(!report.is_success());
    assert!(!report.is_cooperatively_stopped());
    assert!(report.unjoined().is_empty());
}

#[tokio::test]
async fn abandoned_descendant_join_still_initiates_stop_and_retains_its_error() {
    let (shutdown, mut receiver) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    drop(descendants.spawn("callback", async {
        panic!("unobserved by parent");
    }));
    let mut tasks =
        TaskGroup::from_tasks_for_tests(vec![RuntimeTask::spawn("worker", async move {
            shutdown::wait_for_request(&mut receiver).await;
            RuntimeLoopExit::Shutdown
        })]);
    let report = tasks
        .run_report_with_signal(pending(), budget(), &shutdown, &descendants)
        .await;
    assert!(matches!(
        report.cause(),
        RuntimeShutdownCause::DescendantFailure {
            task: "callback",
            ..
        }
    ));
    assert_eq!(report.descendants().len(), 1);
    assert!(
        report.descendants()[0]
            .error
            .as_ref()
            .expect("panic retained")
            .is_panic()
    );
    assert!(!report.is_success());
}

#[tokio::test(start_paused = true)]
async fn early_handle_request_and_later_requests_share_the_original_deadline() {
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let mut tasks = TaskGroup::from_tasks_for_tests(vec![RuntimeTask::spawn("blocked", pending())]);
    shutdown.handle().request();
    let first = shutdown.requested_at();
    tokio::time::advance(Duration::from_secs(10)).await;
    shutdown.request_with(RuntimeShutdownCause::LoopFailure("later"));
    let before = tokio::time::Instant::now();
    let report = tasks
        .run_report_with_signal(pending(), budget(), &shutdown, &descendants)
        .await;
    assert_eq!(shutdown.requested_at(), first);
    assert_eq!(report.cause(), RuntimeShutdownCause::Requested);
    assert!(report.graceful_timed_out());
    assert_eq!(tokio::time::Instant::now(), before);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_yielding_descendant_remains_unjoined_after_parent_completion() {
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let (release, blocked) = std::sync::mpsc::channel();
    let (entered, entry) = tokio::sync::oneshot::channel();
    drop(descendants.spawn("blocked_callback", async move {
        entered.send(()).expect("test observes entry");
        blocked.recv().expect("test releases callback");
    }));
    entry.await.expect("callback entered");
    let mut tasks = TaskGroup::new();
    let report = tasks
        .run_report_with_signal(async {}, budget(), &shutdown, &descendants)
        .await;
    // Release before assertions so any failed assertion cannot strand runtime teardown.
    release.send(()).expect("release owned callback");
    tokio::time::timeout(Duration::from_secs(1), descendants.wait())
        .await
        .expect("join after release");
    assert!(report.graceful_timed_out());
    assert!(report.abort_timed_out());
    assert_eq!(report.unjoined().len(), 1);
    assert_eq!(report.unjoined()[0].task, "blocked_callback");
    assert!(report.unjoined()[0].abort_requested);
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::AbortTimeout { unjoined }) if unjoined.get() == 1
    ));
    assert!(!report.is_cooperatively_stopped());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn final_harvest_classifies_a_late_success_from_its_final_evidence() {
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let (release, blocked) = std::sync::mpsc::channel();
    let (entered, entry) = tokio::sync::oneshot::channel();
    let escaped = descendants.spawn("boundary_race", async move {
        entered.send(()).expect("test observes entry");
        blocked.recv().expect("test releases descendant");
    });
    entry.await.expect("descendant entered non-yielding work");
    let before_final_harvest = async move {
        release.send(()).expect("release boundary descendant");
        while !escaped.is_finished() {
            tokio::task::yield_now().await;
        }
    };
    let zero = RuntimeShutdownBudget::new(Duration::ZERO, Duration::ZERO).expect("valid budget");
    let mut tasks = TaskGroup::new();
    let report = tasks
        .run_report_with_final_harvest_hook(
            async {},
            zero,
            &shutdown,
            &descendants,
            before_final_harvest,
        )
        .await;

    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::GracefulTimeout
    ));
    assert!(report.graceful_timed_out());
    assert!(!report.abort_timed_out());
    assert!(report.unjoined().is_empty());
    assert!(report.is_cooperatively_stopped());
    assert_eq!(report.descendants().len(), 1);
    assert!(report.descendants()[0].abort_requested);
    assert!(report.descendants()[0].error.is_none());
}

struct PanickingObserver;

struct TimedOutOnceObserver(std::sync::atomic::AtomicBool);

#[async_trait::async_trait]
impl crate::JobLifecycleObserver for TimedOutOnceObserver {
    async fn on_job_running(&self, _: crate::JobRunningEvent) {
        if !self.0.swap(true, std::sync::atomic::Ordering::SeqCst) {
            pending::<()>().await;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn earlier_observer_timeout_remains_unsettled_after_later_success_and_all_joins() {
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let observers = crate::JobLifecycleObservers::from_observer(TimedOutOnceObserver(
        std::sync::atomic::AtomicBool::new(false),
    ))
    .with_settlement(descendants.clone());
    let event = crate::JobRunningEvent::new(crate::ObservedJob::new(
        uuid::Uuid::nil(),
        "jobs.callback".try_into().expect("valid job type"),
        None,
        1,
        1,
        1,
        "worker",
    ));
    observers.job_running(event.clone()).await;
    assert!(
        !shutdown.is_requested(),
        "best-effort timeout preserves processing"
    );
    observers.job_running(event).await;
    let task = descendants.track("later_success", tokio::spawn(async {}));
    task.await.expect("later native work succeeds");
    descendants.wait().await;

    let report = TaskGroup::new()
        .run_report_with_signal(async {}, budget(), &shutdown, &descendants)
        .await;
    assert_eq!(report.prior_callback_interruptions(), 1);
    assert!(report.callback_failures().is_empty());
    assert!(report.unjoined().is_empty());
    assert!(
        report
            .descendants()
            .iter()
            .all(|record| record.error.is_none())
    );
    let crate::RuntimeSettlement::Unsettled(unsettled) = report.classify() else {
        panic!("later success must not erase interruption evidence");
    };
    assert!(matches!(
        unsettled.failure(),
        RuntimeShutdownFailure::EarlierCallbackInterruptions { count: 1 }
    ));
}

#[async_trait::async_trait]
impl crate::JobLifecycleObserver for PanickingObserver {
    async fn on_job_running(&self, _: crate::JobRunningEvent) {
        panic!("private callback cause");
    }
}

#[tokio::test]
async fn best_effort_callback_failures_keep_their_scope_and_cleanup_consequence() {
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let observers = crate::JobLifecycleObservers::from_observer(PanickingObserver)
        .with_settlement(descendants.clone());
    let event = crate::JobRunningEvent::new(crate::ObservedJob::new(
        uuid::Uuid::nil(),
        "jobs.callback".try_into().expect("valid job type"),
        None,
        1,
        1,
        1,
        "worker",
    ));
    observers.job_running(event.clone()).await;
    assert!(
        !shutdown.is_requested(),
        "best-effort callback failure does not stop normal processing"
    );
    shutdown.request();
    observers.job_running(event.clone()).await;
    observers.job_running(event).await;
    let report = TaskGroup::new()
        .run_report_with_signal(pending(), budget(), &shutdown, &descendants)
        .await;
    assert_eq!(report.prior_callback_interruptions(), 1);
    assert_eq!(report.callback_failures().len(), 2);
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::CallbackInterrupted {
            callback: "on_job_running"
        })
    ));
    assert!(!report.is_cooperatively_stopped());
    assert!(!format!("{report:?}").contains("private callback cause"));
    assert!(
        matches!(&report.callback_failures()[0], crate::RuntimeCallbackFailure::Panicked { message, .. }
        if message == "private callback cause")
    );
}

#[tokio::test]
async fn joined_configuration_failure_permits_cleanup_but_is_not_success() {
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let mut tasks = TaskGroup::from_tasks_for_tests(vec![RuntimeTask::spawn("invalid", async {
        RuntimeLoopExit::InvalidConfig(crate::config::JobsConfigValidationError::ZeroPollInterval)
    })]);
    let report = tasks
        .run_report_with_signal(pending(), budget(), &shutdown, &descendants)
        .await;
    assert!(report.is_cooperatively_stopped());
    assert!(!report.is_success());
    assert!(matches!(
        report.loops()[0].result,
        Ok(RuntimeLoopExit::InvalidConfig(_))
    ));
}

#[tokio::test]
async fn external_signal_stops_cooperative_loops_and_succeeds() {
    let (shutdown, mut receiver) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let mut tasks =
        TaskGroup::from_tasks_for_tests(vec![RuntimeTask::spawn("cooperative", async move {
            shutdown::wait_for_request(&mut receiver).await;
            RuntimeLoopExit::Shutdown
        })]);
    let report = tasks
        .run_report_with_signal(async {}, budget(), &shutdown, &descendants)
        .await;
    assert_eq!(report.cause(), RuntimeShutdownCause::Requested);
    assert!(report.is_success());
    assert!(report.is_cooperatively_stopped());
    assert!(report.failure().is_none());
}

#[tokio::test]
async fn an_empty_task_group_still_waits_for_its_stop_signal() {
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let mut tasks = TaskGroup::from_tasks_for_tests(Vec::new());
    let (signal, signalled) = tokio::sync::oneshot::channel();
    let driver = tasks.run_report_with_signal(
        async {
            signalled.await.expect("fixture sends the stop signal");
        },
        budget(),
        &shutdown,
        &descendants,
    );
    tokio::pin!(driver);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), driver.as_mut())
            .await
            .is_err(),
        "an empty group must not report before it is asked to stop"
    );
    signal.send(()).expect("driver is still waiting");
    let report = driver.await;
    assert!(report.is_success());
    assert!(report.loops().is_empty());
}

#[tokio::test]
async fn a_loop_returning_before_shutdown_is_an_unexpected_exit_failure() {
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let mut tasks = TaskGroup::from_tasks_for_tests(vec![RuntimeTask::spawn("early", async {
        RuntimeLoopExit::Completed
    })]);
    let report = tasks
        .run_report_with_signal(pending(), budget(), &shutdown, &descendants)
        .await;
    assert_eq!(report.cause(), RuntimeShutdownCause::LoopFailure("early"));
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::LoopExitedUnexpectedly { task: "early" })
    ));
}

#[tokio::test]
async fn a_panicking_loop_is_classified_as_a_join_failure() {
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let mut tasks = TaskGroup::from_tasks_for_tests(vec![RuntimeTask::spawn("panicking", async {
        panic!("loop panic");
    })]);
    let report = tasks
        .run_report_with_signal(pending(), budget(), &shutdown, &descendants)
        .await;
    let Some(RuntimeShutdownFailure::LoopJoin { task, source }) = report.failure() else {
        panic!("a panicking loop must classify as a join failure");
    };
    assert_eq!(task, "panicking");
    assert!(source.is_panic());
    assert!(!report.is_cooperatively_stopped());
}

#[tokio::test]
async fn an_invalid_configuration_exit_is_classified_without_losing_its_source() {
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let mut tasks = TaskGroup::from_tasks_for_tests(vec![RuntimeTask::spawn("invalid", async {
        RuntimeLoopExit::InvalidConfig(crate::config::JobsConfigValidationError::ZeroPollInterval)
    })]);
    let report = tasks
        .run_report_with_signal(pending(), budget(), &shutdown, &descendants)
        .await;
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::LoopInvalidConfig {
            task: "invalid",
            source: crate::config::JobsConfigValidationError::ZeroPollInterval,
        })
    ));
    // Cleanup eligibility and shutdown success are independent decisions.
    assert!(report.is_cooperatively_stopped());
}

#[tokio::test(start_paused = true)]
async fn a_zero_budget_aborts_without_waiting_and_reports_both_timeouts() {
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let mut tasks =
        TaskGroup::from_tasks_for_tests(vec![RuntimeTask::spawn("stubborn", pending())]);
    let zero = RuntimeShutdownBudget::new(Duration::ZERO, Duration::ZERO).expect("valid budget");
    let report = tasks
        .run_report_with_signal(async {}, zero, &shutdown, &descendants)
        .await;
    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::AbortTimeout { .. }
    ));
    assert!(report.graceful_timed_out());
    assert!(report.abort_timed_out());
    assert_eq!(report.unjoined().len(), 1);
    assert_eq!(report.unjoined()[0].task, "stubborn");
    assert!(report.unjoined()[0].abort_requested);
    assert!(report.loops().is_empty());
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::AbortTimeout { unjoined }) if unjoined.get() == 1
    ));
    assert!(!report.is_cooperatively_stopped());
}

#[tokio::test(start_paused = true)]
async fn a_requested_stop_reports_its_missed_budget_ahead_of_the_abort_it_caused() {
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let mut tasks =
        TaskGroup::from_tasks_for_tests(vec![RuntimeTask::spawn("stubborn", pending())]);
    let report = tasks
        .run_report_with_signal(async {}, budget(), &shutdown, &descendants)
        .await;
    assert_eq!(report.cause(), RuntimeShutdownCause::Requested);
    assert!(report.graceful_timed_out());
    // The loop was aborted because the budget ran out, so the budget is the
    // reported cause rather than the cancellation it produced.
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::GracefulTimeout)
    ));
}

#[tokio::test(start_paused = true)]
async fn a_triggering_loop_failure_outranks_what_the_drain_observed() {
    let (shutdown, mut receiver) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let mut tasks = TaskGroup::from_tasks_for_tests(vec![
        RuntimeTask::spawn("trigger", async { RuntimeLoopExit::Completed }),
        RuntimeTask::spawn("draining", async move {
            shutdown::wait_for_request(&mut receiver).await;
            RuntimeLoopExit::Shutdown
        }),
    ]);
    let report = tasks
        .run_report_with_signal(pending(), budget(), &shutdown, &descendants)
        .await;
    assert_eq!(report.cause(), RuntimeShutdownCause::LoopFailure("trigger"));
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::LoopExitedUnexpectedly { task: "trigger" })
    ));
}

#[tokio::test(start_paused = true)]
async fn a_triggering_loop_failure_outranks_a_descendant_failure_during_drain() {
    let (shutdown, mut receiver) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let escaped = descendants.spawn("draining_descendant", async move {
        shutdown::wait_for_request(&mut receiver).await;
        panic!("descendant failure during drain");
    });
    let mut tasks = TaskGroup::from_tasks_for_tests(vec![RuntimeTask::spawn("trigger", async {
        RuntimeLoopExit::Completed
    })]);
    let report = tasks
        .run_report_with_signal(pending(), budget(), &shutdown, &descendants)
        .await;
    let original = escaped.await.expect_err("actual descendant panic");

    assert_eq!(report.cause(), RuntimeShutdownCause::LoopFailure("trigger"));
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::LoopExitedUnexpectedly { task: "trigger" })
    ));
    let retained = report
        .descendants()
        .iter()
        .find(|record| record.task == "draining_descendant")
        .and_then(|record| record.error.as_ref())
        .expect("the descendant failure remains available as drain evidence");
    assert!(std::sync::Arc::ptr_eq(retained, &original));
}

#[tokio::test(start_paused = true)]
async fn a_triggering_descendant_failure_outranks_a_loop_failure_during_drain() {
    let (shutdown, mut receiver) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let mut tasks =
        TaskGroup::from_tasks_for_tests(vec![RuntimeTask::spawn("draining_loop", async move {
            shutdown::wait_for_request(&mut receiver).await;
            panic!("loop failure during drain");
        })]);
    let escaped = descendants.spawn("escaped_job", async { panic!("triggering panic") });
    let original = escaped.await.expect_err("actual descendant panic");
    let report = tasks
        .run_report_with_signal(pending(), budget(), &shutdown, &descendants)
        .await;

    assert!(matches!(
        report.cause(),
        RuntimeShutdownCause::DescendantFailure {
            task: "escaped_job",
            ..
        }
    ));
    assert!(
        report.loops()[0]
            .result
            .as_ref()
            .is_err_and(|error| error.is_panic())
    );
    let Some(RuntimeShutdownFailure::DescendantJoin { source, .. }) = report.failure() else {
        panic!("the descendant that started shutdown must remain the primary failure");
    };
    assert!(std::sync::Arc::ptr_eq(&source, &original));
}

#[tokio::test(start_paused = true)]
async fn a_successful_report_is_exactly_a_report_without_a_failure() {
    let (shutdown, mut receiver) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let mut tasks = TaskGroup::from_tasks_for_tests(vec![RuntimeTask::spawn("loop", async move {
        shutdown::wait_for_request(&mut receiver).await;
        RuntimeLoopExit::Shutdown
    })]);
    let report = tasks
        .run_report_with_signal(async {}, budget(), &shutdown, &descendants)
        .await;
    assert_eq!(report.is_success(), report.failure().is_none());
    assert!(report.is_success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_after_a_requested_stop_fails_shutdown_but_permits_cleanup() {
    let (entered, entry) = std::sync::mpsc::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let task = RuntimeTask::spawn(
        "completed_after_request",
        CompleteAfterPollSignal {
            entered: Some(entered),
            release: blocked,
            exit: RuntimeLoopExit::Completed,
        },
    );
    let mut tasks = TaskGroup::from_tasks_for_tests(vec![task]);
    entry
        .recv_timeout(Duration::from_secs(1))
        .expect("loop enters its final poll");

    shutdown.handle().request();
    release.send(()).expect("release final loop poll");
    let report = tasks
        .run_report_with_signal(pending(), budget(), &shutdown, &descendants)
        .await;

    assert_eq!(report.cause(), RuntimeShutdownCause::Requested);
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::LoopExitedUnexpectedly {
            task: "completed_after_request"
        })
    ));
    assert!(!report.is_success());
    assert!(report.is_cooperatively_stopped());
    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::Settled
    ));
    assert!(!report.loops()[0].abort_requested);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_exit_wins_a_live_loop_abort_race() {
    let (entered, entry) = std::sync::mpsc::channel();
    let (release, blocked) = std::sync::mpsc::channel();
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let task = RuntimeTask::spawn(
        "shutdown_abort_race",
        CompleteAfterPollSignal {
            entered: Some(entered),
            release: blocked,
            exit: RuntimeLoopExit::Shutdown,
        },
    );
    let finished = task.handle.abort_handle();
    let mut tasks = TaskGroup::from_tasks_for_tests(vec![task]);
    entry
        .recv_timeout(Duration::from_secs(1))
        .expect("loop enters its final poll");
    let zero = RuntimeShutdownBudget::new(Duration::ZERO, Duration::ZERO).expect("valid budget");
    let before_final_harvest = async move {
        release.send(()).expect("release final loop poll");
        while !finished.is_finished() {
            tokio::task::yield_now().await;
        }
    };

    let report = tasks
        .run_report_with_final_harvest_hook(
            async {},
            zero,
            &shutdown,
            &descendants,
            before_final_harvest,
        )
        .await;

    assert_eq!(report.cause(), RuntimeShutdownCause::Requested);
    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::GracefulTimeout
    ));
    assert!(report.graceful_timed_out());
    assert!(!report.abort_timed_out());
    assert!(report.unjoined().is_empty());
    assert_eq!(report.loops().len(), 1);
    assert!(report.loops()[0].abort_requested);
    assert!(matches!(
        report.loops()[0].result,
        Ok(RuntimeLoopExit::Shutdown)
    ));
    assert!(report.is_cooperatively_stopped());
    assert!(!report.is_success());
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::GracefulTimeout)
    ));
}

#[tokio::test]
async fn final_harvest_observes_finished_loop_without_ready_notification() {
    let (release, released) = tokio::sync::oneshot::channel();
    let task = RuntimeTask::spawn("delayed_notification", async {
        released.await.expect("test releases loop");
        RuntimeLoopExit::Shutdown
    });
    let finished = task.handle.abort_handle();
    let mut pending = super::PendingLoops::from([(
        "delayed_notification",
        super::PendingLoop {
            abort: finished.clone(),
            aborted: false,
        },
    )]);
    let mut joined = super::join_runtime_tasks(vec![task]);
    let mut records = Vec::new();
    let (shutdown, _) = ShutdownSignal::channel();
    super::collect_ready(&mut joined, &mut pending, &mut records, &shutdown);
    // Suppress the notification deterministically after the join stream has
    // registered interest. This models completion published before wake delivery.
    let task = joined.iter_mut().next().expect("one pending loop");
    assert!(
        Pin::new(&mut task.handle)
            .poll(&mut Context::from_waker(
                futures_util::task::noop_waker_ref()
            ))
            .is_pending()
    );
    release.send(()).expect("release loop");
    while !finished.is_finished() {
        tokio::task::yield_now().await;
    }
    super::collect_ready(&mut joined, &mut pending, &mut records, &shutdown);
    assert!(
        records.is_empty(),
        "ordinary collection has no notification"
    );
    super::collect_finished(&mut joined, &mut pending, &mut records, &shutdown);
    assert!(pending.is_empty());
    assert!(joined.is_empty());
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].task, "delayed_notification");
    assert!(matches!(records[0].result, Ok(RuntimeLoopExit::Shutdown)));
    assert!(!records[0].abort_requested);
}

#[tokio::test(start_paused = true)]
async fn an_abort_we_issued_does_not_outrank_the_failure_that_caused_it() {
    let (shutdown, _) = ShutdownSignal::channel();
    let descendants = TaskRegistry::supervised(shutdown.clone());
    let mut tasks =
        TaskGroup::from_tasks_for_tests(vec![RuntimeTask::spawn("unresponsive", pending())]);
    let escaped = descendants.spawn("escaped_job", async { panic!("escaped panic") });
    let original = escaped.await.expect_err("actual descendant panic");
    let report = tasks
        .run_report_with_signal(pending(), budget(), &shutdown, &descendants)
        .await;

    assert!(matches!(
        report.cause(),
        RuntimeShutdownCause::DescendantFailure {
            task: "escaped_job",
            ..
        }
    ));
    // The loop was cancelled only because we aborted it after the budget ran out.
    assert!(report.graceful_timed_out());
    assert!(report.loops()[0].cancelled_by_abort());
    let Some(RuntimeShutdownFailure::DescendantJoin { source, .. }) = report.failure() else {
        panic!("the panic that started the stop must outrank the abort it caused");
    };
    assert!(std::sync::Arc::ptr_eq(&source, &original));
}
