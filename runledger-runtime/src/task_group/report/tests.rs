use crate::{
    RuntimeLoopExit, RuntimeShutdownBudget, RuntimeShutdownCause,
    settlement::TaskRegistry,
    shutdown::{self, ShutdownSignal},
    task_group::{RuntimeTask, TaskGroup},
};
use std::{future::pending, time::Duration};

fn budget() -> RuntimeShutdownBudget {
    RuntimeShutdownBudget::new(Duration::from_millis(20), Duration::from_millis(20))
        .expect("valid budget")
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
    shutdown.request_with(RuntimeShutdownCause::DescendantFailure);
    let native_stop = shutdown.clone();
    let owned_descendants = descendants.clone();
    let mut driver = tokio::spawn(async move {
        TaskGroup::new()
            .run_report(
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
    assert!(report.abort_timed_out);
    assert_eq!(report.cause, RuntimeShutdownCause::DescendantFailure);
    assert_eq!(report.unjoined.len(), 1);
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
    shutdown.request_with(RuntimeShutdownCause::DescendantFailure);
    let allowance = RuntimeShutdownBudget::new(Duration::from_secs(10), Duration::from_secs(2))
        .expect("valid phase budget");
    let driver = tasks.run_report(pending(), allowance, &shutdown, &descendants);
    tokio::pin!(driver);
    assert!(driver.as_mut().now_or_never().is_none());
    tokio::time::advance(Duration::from_secs(6)).await;
    assert!(driver.as_mut().now_or_never().is_none());
    shutdown.handle().request_since(parent_started);
    let report = driver.await;
    assert!(tokio::time::Instant::now() <= parent_started + Duration::from_secs(12));
    assert!(report.graceful_timed_out);
    assert_eq!(report.cause, RuntimeShutdownCause::DescendantFailure);
    assert!(report.loops[0].abort_requested);
}

#[tokio::test]
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
        .run_report(pending(), budget(), &shutdown, &descendants)
        .await;
    assert_eq!(report.cause, RuntimeShutdownCause::LoopFailure("first"));
    assert_eq!(report.loops.len(), 2);
    assert!(
        report
            .loops
            .iter()
            .any(|record| matches!(record.result, Ok(RuntimeLoopExit::Completed)))
    );
    assert!(
        report
            .loops
            .iter()
            .any(|record| record.result.as_ref().is_err_and(|error| error.is_panic()))
    );
    assert!(!report.is_success());
    assert!(!report.is_cooperatively_stopped());
    assert!(report.unjoined.is_empty());
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
        .run_report(pending(), budget(), &shutdown, &descendants)
        .await;
    assert_eq!(report.cause, RuntimeShutdownCause::DescendantFailure);
    assert_eq!(report.descendants.len(), 1);
    assert!(
        report.descendants[0]
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
        .run_report(pending(), budget(), &shutdown, &descendants)
        .await;
    assert_eq!(shutdown.requested_at(), first);
    assert_eq!(report.cause, RuntimeShutdownCause::Requested);
    assert!(report.graceful_timed_out);
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
        .run_report(async {}, budget(), &shutdown, &descendants)
        .await;
    // Release before assertions so any failed assertion cannot strand runtime teardown.
    release.send(()).expect("release owned callback");
    tokio::time::timeout(Duration::from_secs(1), descendants.wait())
        .await
        .expect("join after release");
    assert!(report.graceful_timed_out);
    assert!(report.abort_timed_out);
    assert_eq!(report.unjoined.len(), 1);
    assert_eq!(report.unjoined[0].task, "blocked_callback");
    assert!(report.unjoined[0].abort_requested);
    assert!(!report.is_cooperatively_stopped());
}

struct PanickingObserver;

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
        .run_report(pending(), budget(), &shutdown, &descendants)
        .await;
    assert_eq!(report.prior_callback_interruptions, 1);
    assert_eq!(report.callback_failures.len(), 2);
    assert!(!report.is_cooperatively_stopped());
    assert!(!format!("{report:?}").contains("private callback cause"));
    assert!(
        matches!(&report.callback_failures[0], crate::RuntimeCallbackFailure::Panicked { message, .. }
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
        .run_report(pending(), budget(), &shutdown, &descendants)
        .await;
    assert!(report.is_cooperatively_stopped());
    assert!(!report.is_success());
    assert!(matches!(
        report.loops[0].result,
        Ok(RuntimeLoopExit::InvalidConfig(_))
    ));
}
