use super::*;
use crate::{
    RuntimeLoopExit, RuntimeShutdownFailure, RuntimeShutdownSettlement,
    supervisor::tests::{disabled_supervisor, test_budget},
};
use std::{
    io::{self, Write},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex},
    time::Duration,
};

mod panics;

#[derive(Clone, Default)]
struct LogBuffer(Arc<Mutex<Vec<u8>>>);

impl Write for LogBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().expect("log buffer").extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl LogBuffer {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().expect("log buffer").clone()).expect("UTF-8 tracing")
    }
}

fn capture_logs(log: &LogBuffer) -> tracing::subscriber::DefaultGuard {
    let writer = log.clone();
    tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish(),
    )
}

const UNOBSERVED: &str = "jobs runtime settlement completed after its report waiter was dropped";

fn ready_signal(fails: bool) -> RuntimeShutdownSignal {
    if fails {
        RuntimeShutdownSignal::fallible(std::future::ready(Err::<(), _>(io::Error::other(
            "signal setup failed",
        ))))
    } else {
        RuntimeShutdownSignal::infallible(std::future::ready(()))
    }
}

#[tokio::test(start_paused = true)]
async fn terminal_activation_preserves_ready_loop_failure_priority() {
    for panics in [false, true] {
        for signal_fails in [false, true] {
            let mut supervisor = disabled_supervisor();
            supervisor
                .tasks
                .spawn_on(&supervisor.runtime, "trigger", async move {
                    assert!(!panics, "native loop panic");
                    RuntimeLoopExit::Completed
                });
            supervisor.tasks.wait_until_finished_for_tests().await;
            supervisor
                .tasks
                .spawn_on(&supervisor.runtime, "stubborn", std::future::pending());
            let shutdown = supervisor.shutdown.clone();
            assert!(!shutdown.is_requested());

            let driver =
                supervisor.run_until_shutdown_report(ready_signal(signal_fails), test_budget());
            // Establish priority at the public activation boundary, independent
            // of how Tokio schedules the signal and settlement tasks afterward.
            assert_eq!(
                shutdown.cause(),
                crate::RuntimeShutdownCause::LoopFailure("trigger")
            );
            let report = driver.await;
            assert_eq!(
                report.cause(),
                crate::RuntimeShutdownCause::LoopFailure("trigger")
            );
            assert!(report.graceful_timed_out());
            if panics {
                assert!(matches!(report.failure(),
                    Some(RuntimeShutdownFailure::LoopJoin { task: "trigger", source })
                        if source.is_panic()));
            } else {
                assert!(matches!(
                    report.failure(),
                    Some(RuntimeShutdownFailure::LoopExitedUnexpectedly { task: "trigger" })
                ));
            }
            assert!(report.signal_error().is_none());
            assert!(!report.is_success());
            assert!(!report.is_cooperatively_stopped());
        }
    }
}

#[tokio::test(start_paused = true)]
async fn terminal_activation_preserves_ready_descendant_failure_priority() {
    for signal_fails in [false, true] {
        let mut supervisor = disabled_supervisor();
        let failed = supervisor.descendants.spawn("trigger", async {
            panic!("native descendant panic");
        });
        // Do not await the shared join: observing it would record the failure
        // before the terminal method and conceal the activation-order defect.
        while !failed.is_finished() {
            tokio::task::yield_now().await;
        }
        supervisor
            .tasks
            .spawn_on(&supervisor.runtime, "stubborn", std::future::pending());
        let shutdown = supervisor.shutdown.clone();
        assert!(!shutdown.is_requested());
        let driver =
            supervisor.run_until_shutdown_report(ready_signal(signal_fails), test_budget());
        assert!(matches!(
            shutdown.cause(),
            crate::RuntimeShutdownCause::DescendantFailure {
                task: "trigger",
                ..
            }
        ));
        let report = driver.await;
        assert!(matches!(
            report.cause(),
            crate::RuntimeShutdownCause::DescendantFailure {
                task: "trigger",
                ..
            }
        ));
        assert!(report.graceful_timed_out());
        assert!(matches!(report.failure(),
            Some(RuntimeShutdownFailure::DescendantJoin { task: "trigger", source })
                if source.is_panic()));
        assert!(report.signal_error().is_none());
        assert!(!report.is_success());
        assert!(!report.is_cooperatively_stopped());
    }
}

#[tokio::test]
async fn terminal_activation_without_failure_still_waits_for_signal() {
    let mut supervisor = disabled_supervisor();
    supervisor
        .tasks
        .spawn_on(&supervisor.runtime, "finished", async {
            RuntimeLoopExit::Shutdown
        });
    supervisor.tasks.wait_until_finished_for_tests().await;
    let shutdown = supervisor.shutdown.clone();
    let stopping = shutdown.clone();
    supervisor
        .tasks
        .spawn_on(&supervisor.runtime, "cooperative", async move {
            stopping.requested().await;
            RuntimeLoopExit::Shutdown
        });
    let (entered, entry) = oneshot::channel();
    let (stop, stopped) = oneshot::channel();
    let driver = supervisor.run_until_shutdown_report(
        RuntimeShutdownSignal::infallible(async move {
            entered.send(()).expect("test observes signal polling");
            stopped.await.expect("test supplies signal");
        }),
        test_budget(),
    );
    entry.await.expect("signal starts after harvest");
    assert!(!shutdown.is_requested());
    assert!(driver.report.is_empty());
    stop.send(()).expect("signal is still waiting");
    let report = driver.await;
    assert_eq!(report.cause(), crate::RuntimeShutdownCause::Requested);
    assert_eq!(report.loops().len(), 2);
    assert!(report.is_success());
    assert!(report.is_cooperatively_stopped());
}

struct BlockingDropSignal {
    entered: Option<std::sync::mpsc::Sender<()>>,
    release: std::sync::mpsc::Receiver<()>,
}

impl Future for BlockingDropSignal {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }
}

impl Drop for BlockingDropSignal {
    fn drop(&mut self) {
        if let Some(entered) = self.entered.take() {
            entered.send(()).expect("test observes signal destruction");
        }
        // Exercise blocked destruction without starving work queued on this
        // Tokio worker. Disconnection releases the fixture during test unwind.
        tokio::task::block_in_place(|| {
            let _ = self.release.recv();
        });
    }
}

struct BlockingPollSignal {
    entered: Option<std::sync::mpsc::Sender<()>>,
    release: std::sync::mpsc::Receiver<()>,
    dropped: Option<oneshot::Sender<()>>,
    panic_on_drop: bool,
}

impl Future for BlockingPollSignal {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if let Some(entered) = this.entered.take() {
            entered.send(()).expect("test observes signal polling");
        }
        this.release
            .recv()
            .expect("test releases the blocked signal poll");
        Poll::Pending
    }
}

impl Drop for BlockingPollSignal {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            dropped
                .send(())
                .expect("test observes guarded signal destruction");
        }
        if self.panic_on_drop {
            panic!("private abort-path signal destruction panic");
        }
    }
}

struct PendingDropSignal {
    entered: Option<oneshot::Sender<()>>,
    dropped: Option<oneshot::Sender<()>>,
}

impl Future for PendingDropSignal {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if let Some(entered) = self.entered.take() {
            entered.send(()).expect("test observes signal polling");
        }
        Poll::Pending
    }
}

impl Drop for PendingDropSignal {
    fn drop(&mut self) {
        if let Some(dropped) = self.dropped.take() {
            dropped
                .send(())
                .expect("test observes guarded signal destruction");
        }
    }
}

struct BlockingErrorOutputSignal {
    poll_entered: Option<std::sync::mpsc::Sender<()>>,
    release_poll: std::sync::mpsc::Receiver<()>,
    destruction_entered: Option<std::sync::mpsc::Sender<()>>,
    release_destruction: std::sync::mpsc::Receiver<()>,
}

impl Future for BlockingErrorOutputSignal {
    type Output = Result<(), io::Error>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Some(entered) = this.poll_entered.take() {
            entered.send(()).expect("test observes signal polling");
        }
        this.release_poll
            .recv()
            .expect("test releases the blocked signal poll");
        Poll::Ready(Err(io::Error::other("losing signal failed")))
    }
}

impl Drop for BlockingErrorOutputSignal {
    fn drop(&mut self) {
        if let Some(entered) = self.destruction_entered.take() {
            entered
                .send(())
                .expect("test observes losing signal destruction");
        }
        self.release_destruction
            .recv()
            .expect("test releases losing signal destruction");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn force_aborted_custom_signal_denies_cleanup_with_zero_grace() {
    let supervisor = disabled_supervisor();
    let shutdown = supervisor.shutdown.clone();
    let descendants = supervisor.descendants.clone();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let driver = supervisor.run_until_shutdown_report(
        RuntimeShutdownSignal::infallible(BlockingPollSignal {
            entered: Some(entered_tx),
            release: release_rx,
            dropped: Some(dropped_tx),
            panic_on_drop: false,
        }),
        RuntimeShutdownBudget::new(Duration::ZERO, Duration::from_secs(1))
            .expect("zero grace with an abort allowance is valid"),
    );

    tokio::task::spawn_blocking(move || {
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("signal poll blocks before external stop");
    })
    .await
    .expect("poll observer joins");
    shutdown.request();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let (_, unjoined) = descendants.snapshot();
            if unjoined
                .iter()
                .any(|task| task.task == "shutdown_signal" && task.abort_requested)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("settlement owner requests signal cancellation");
    release_tx.send(()).expect("release blocked signal poll");
    let report = driver.await;
    dropped_rx
        .await
        .expect("joined cancellation completed guarded destruction");

    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::GracefulTimeout
    ));
    assert!(report.unjoined().is_empty());
    assert!(!report.is_cooperatively_stopped());
    assert!(!report.is_success());
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::GracefulTimeout)
    ));
    let signal = report
        .descendants()
        .iter()
        .find(|record| record.task == "shutdown_signal")
        .expect("retain the custom signal cancellation");
    assert!(signal.abort_requested);
    assert!(!signal.cancelled_by_owner());
    assert!(signal.cancelled_by_abort());
    assert!(
        signal
            .error
            .as_ref()
            .is_some_and(|error| error.is_cancelled())
    );
    assert!(matches!(
        signal.failure(),
        Some(RuntimeShutdownFailure::DescendantJoin {
            task: "shutdown_signal",
            source,
        }) if source.is_cancelled()
    ));
}

#[tokio::test]
async fn runtime_authored_pending_signal_owner_cancellation_is_accounted() {
    let supervisor = disabled_supervisor();
    let shutdown = supervisor.shutdown.clone();
    let descendants = supervisor.descendants.clone();
    let driver =
        supervisor.run_until_shutdown_report(RuntimeShutdownSignal::pending(), test_budget());

    // Keep this sequence synchronous so the registered listener cannot retire
    // cooperatively before this test exercises its trusted cancellation path.
    shutdown.request();
    descendants.abort_all();
    let report = driver.await;

    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::Settled
    ));
    assert!(report.is_cooperatively_stopped());
    assert!(report.is_success());
    let signal = report
        .descendants()
        .iter()
        .find(|record| record.task == "shutdown_signal")
        .expect("retain the accounted built-in signal cancellation");
    assert!(signal.abort_requested);
    assert!(signal.cancelled_by_owner());
    assert!(signal.cancelled_by_abort());
    assert!(signal.error.is_none());
    assert!(signal.failure().is_none());
}

#[tokio::test]
async fn runtime_authored_ctrl_c_owner_cancellation_is_accounted() {
    let supervisor = disabled_supervisor();
    let shutdown = supervisor.shutdown.clone();
    let descendants = supervisor.descendants.clone();
    let driver =
        supervisor.run_until_shutdown_report(RuntimeShutdownSignal::ctrl_c(), test_budget());

    // Exercise Ctrl-C provenance without delivering a process signal: another
    // source wins first cause and the settlement owner cancels the listener.
    shutdown.request();
    descendants.abort_all();
    let report = driver.await;

    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::Settled
    ));
    assert!(report.is_cooperatively_stopped());
    assert!(report.is_success());
    let signal = report
        .descendants()
        .iter()
        .find(|record| record.task == "shutdown_signal")
        .expect("retain the accounted Ctrl-C listener cancellation");
    assert!(signal.abort_requested);
    assert!(signal.cancelled_by_owner());
    assert!(signal.cancelled_by_abort());
    assert!(signal.error.is_none());
    assert!(signal.failure().is_none());
}

#[cfg(unix)]
#[test]
fn ctrl_c_without_a_signal_driver_is_retained_as_panic_evidence() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("construct Tokio runtime without I/O");
    let report = runtime.block_on(async {
        disabled_supervisor()
            .run_until_shutdown_report(RuntimeShutdownSignal::ctrl_c(), test_budget())
            .await
    });

    assert!(matches!(
        report.signal_panic(),
        Some(crate::RuntimeShutdownSignalPanic::Poll { .. })
    ));
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::DescendantJoin {
            task: "shutdown_signal",
            source,
        }) if source.is_panic()
    ));
    assert!(!report.is_cooperatively_stopped());
    assert!(!report.is_success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_phase_signal_destruction_panic_directly_denies_cleanup() {
    let supervisor = disabled_supervisor();
    let shutdown = supervisor.shutdown.clone();
    let descendants = supervisor.descendants.clone();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let driver = supervisor.run_until_shutdown_report(
        RuntimeShutdownSignal::infallible(BlockingPollSignal {
            entered: Some(entered_tx),
            release: release_rx,
            dropped: None,
            panic_on_drop: true,
        }),
        RuntimeShutdownBudget::new(Duration::ZERO, Duration::from_secs(1))
            .expect("zero grace with an abort allowance is valid"),
    );

    tokio::task::spawn_blocking(move || {
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("signal poll blocks before external stop");
    })
    .await
    .expect("poll observer joins");
    shutdown.request();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let (_, unjoined) = descendants.snapshot();
            if unjoined
                .iter()
                .any(|task| task.task == "shutdown_signal" && task.abort_requested)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("settlement owner enters the abort phase");
    release_tx.send(()).expect("release blocked signal poll");
    let report = driver.await;

    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::GracefulTimeout
    ));
    assert!(report.unjoined().is_empty());
    assert!(
        report
            .descendants()
            .iter()
            .any(|record| { record.task == "shutdown_signal" && record.abort_requested })
    );
    assert!(matches!(report.signal_panic(),
        Some(crate::RuntimeShutdownSignalPanic::Destruction { message })
            if message == "private abort-path signal destruction panic"));
    assert!(!report.is_cooperatively_stopped());
    assert!(!report.is_success());
    assert_eq!(report.is_success(), report.failure().is_none());
}

#[tokio::test]
async fn unjoined_initiating_signal_still_denies_cleanup_without_self_abort() {
    let supervisor = disabled_supervisor();
    supervisor
        .descendants
        .spawn_initiating_shutdown_signal_on_for_tests(&supervisor.runtime, std::future::pending());
    let report = tokio::time::timeout(
        Duration::from_secs(2),
        supervisor.shutdown_report(
            RuntimeShutdownBudget::new(Duration::ZERO, Duration::ZERO)
                .expect("zero budget is valid"),
        ),
    );
    let report = report.await.expect("zero-budget settlement is bounded");

    assert!(
        matches!(
            report.settlement(),
            RuntimeShutdownSettlement::AbortTimeout { .. }
        ),
        "unexpected settlement: {:?}",
        report.settlement()
    );
    assert!(
        report
            .unjoined()
            .iter()
            .any(|task| task.task == "shutdown_signal" && !task.abort_requested)
    );
    assert!(!report.is_cooperatively_stopped());
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::AbortTimeout { .. })
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn signal_publishes_stop_before_its_blocking_destruction_joins() {
    let supervisor = disabled_supervisor();
    let shutdown = supervisor.shutdown.clone();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let driver = supervisor.run_until_shutdown_report(
        RuntimeShutdownSignal::infallible(BlockingDropSignal {
            entered: Some(entered_tx),
            release: release_rx,
        }),
        test_budget(),
    );

    tokio::task::spawn_blocking(move || {
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("signal reaches guarded destruction");
    })
    .await
    .expect("destruction observer joins");
    assert!(
        shutdown.is_requested(),
        "signal output publishes stop before guarded destruction joins"
    );
    release_tx.send(()).expect("release signal destruction");

    let report = driver.await;
    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::Settled
    ));
    assert!(report.unjoined().is_empty());
    assert!(report.is_success());
    assert!(report.is_cooperatively_stopped());
    assert!(report.descendants().iter().any(|record| {
        record.task == "shutdown_signal" && !record.abort_requested && record.error.is_none()
    }));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initiating_signal_is_not_aborted_after_grace_expires() {
    let supervisor = disabled_supervisor();
    let descendants = supervisor.descendants.clone();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let driver = supervisor.run_until_shutdown_report(
        RuntimeShutdownSignal::infallible(BlockingDropSignal {
            entered: Some(entered_tx),
            release: release_rx,
        }),
        RuntimeShutdownBudget::new(Duration::ZERO, Duration::from_secs(1))
            .expect("zero grace with an abort allowance is valid"),
    );

    tokio::task::spawn_blocking(move || {
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("initiating signal reaches guarded destruction");
    })
    .await
    .expect("destruction observer joins");
    tokio::time::timeout(Duration::from_secs(2), async {
        while !descendants.graceful_abort_started_for_tests() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("settlement enters the abort allowance");
    assert!(
        descendants
            .snapshot()
            .1
            .iter()
            .any(|task| { task.task == "shutdown_signal" && !task.abort_requested })
    );

    release_tx
        .send(())
        .expect("release initiating signal destruction");
    let report = driver.await;
    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::GracefulTimeout
    ));
    assert!(report.unjoined().is_empty());
    assert!(report.descendants().iter().any(|record| {
        record.task == "shutdown_signal" && !record.abort_requested && record.error.is_none()
    }));
    assert!(report.is_cooperatively_stopped());
    assert!(!report.is_success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn signal_output_that_loses_first_cause_remains_abortable() {
    let mut supervisor = disabled_supervisor();
    let shutdown = supervisor.shutdown.clone();
    let descendants = supervisor.descendants.clone();
    let (fail_loop, fail_requested) = oneshot::channel();
    supervisor
        .tasks
        .spawn_on(&supervisor.runtime, "trigger", async move {
            fail_requested.await.expect("test fails the native loop");
            RuntimeLoopExit::Completed
        });
    let (poll_entered_tx, poll_entered_rx) = std::sync::mpsc::channel();
    let (release_poll_tx, release_poll_rx) = std::sync::mpsc::channel();
    let (destruction_entered_tx, destruction_entered_rx) = std::sync::mpsc::channel();
    let (release_destruction_tx, release_destruction_rx) = std::sync::mpsc::channel();
    let driver = supervisor.run_until_shutdown_report(
        RuntimeShutdownSignal::fallible(BlockingErrorOutputSignal {
            poll_entered: Some(poll_entered_tx),
            release_poll: release_poll_rx,
            destruction_entered: Some(destruction_entered_tx),
            release_destruction: release_destruction_rx,
        }),
        RuntimeShutdownBudget::new(Duration::from_secs(10), Duration::from_secs(1))
            .expect("test controls graceful escalation directly"),
    );
    let report = tokio::spawn(driver);

    tokio::task::spawn_blocking(move || {
        poll_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("signal poll blocks before another cause wins");
    })
    .await
    .expect("poll observer joins");
    fail_loop.send(()).expect("release failing native loop");
    tokio::time::timeout(Duration::from_secs(2), async {
        while !matches!(
            shutdown.cause(),
            crate::RuntimeShutdownCause::LoopFailure("trigger")
        ) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("native loop wins first-cause arbitration");

    release_poll_tx.send(()).expect("release signal output");
    tokio::task::spawn_blocking(move || {
        destruction_entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("losing signal reaches guarded destruction");
    })
    .await
    .expect("destruction observer joins");
    assert!(shutdown.signal_error().is_some());

    // Exercise the exact policy transition used when grace expires. A signal
    // that merely produced output must not gain the initiator's exemption.
    descendants.abort_after_graceful_timeout();
    assert!(
        descendants
            .snapshot()
            .1
            .iter()
            .any(|task| task.task == "shutdown_signal" && task.abort_requested)
    );

    release_destruction_tx
        .send(())
        .expect("release losing signal destruction");
    let report = report.await.expect("report waiter joins");
    assert!(matches!(
        report.cause(),
        crate::RuntimeShutdownCause::LoopFailure("trigger")
    ));
    assert!(report.signal_error().is_some());
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::LoopExitedUnexpectedly { task: "trigger" })
    ));
    assert!(
        report
            .descendants()
            .iter()
            .any(|record| { record.task == "shutdown_signal" && record.abort_requested })
    );
    assert!(!report.is_success());
}

#[tokio::test]
async fn initiating_signal_uses_abort_allowance_without_self_abort() {
    let supervisor = disabled_supervisor();
    let descendants = supervisor.descendants.clone();
    let (release_tx, release_rx) = oneshot::channel();
    descendants.spawn_initiating_shutdown_signal_on_for_tests(&supervisor.runtime, async move {
        let _ = release_rx.await;
    });
    let driver = supervisor.shutdown_report(
        RuntimeShutdownBudget::new(Duration::ZERO, Duration::from_secs(1))
            .expect("zero grace with an abort allowance is valid"),
    );
    let mut report = tokio::spawn(driver);

    tokio::time::timeout(Duration::from_secs(2), async {
        while !descendants.graceful_abort_started_for_tests() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("settlement enters the abort allowance");
    assert!(
        descendants
            .snapshot()
            .1
            .iter()
            .any(|task| { task.task == "shutdown_signal" && !task.abort_requested })
    );

    release_tx.send(()).expect("release initiating signal join");
    let report = tokio::time::timeout(Duration::from_secs(2), &mut report)
        .await
        .expect("report completes inside the abort allowance")
        .expect("report waiter joins");
    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::GracefulTimeout
    ));
    assert!(report.unjoined().is_empty());
    assert!(report.is_cooperatively_stopped());
    assert!(!report.is_success());
    let signal = report
        .descendants()
        .iter()
        .find(|record| record.task == "shutdown_signal")
        .expect("initiating signal join is retained");
    assert!(!signal.abort_requested);
    assert!(signal.error.is_none());
}

#[tokio::test]
async fn fallible_ok_signal_is_a_clean_requested_trigger() {
    let report = disabled_supervisor()
        .run_until_shutdown_report(
            RuntimeShutdownSignal::fallible(std::future::ready(Ok::<(), io::Error>(()))),
            test_budget(),
        )
        .await;

    assert_eq!(report.cause(), crate::RuntimeShutdownCause::Requested);
    assert!(report.signal_error().is_none());
    assert!(report.signal_panic().is_none());
    assert!(report.is_success());
    assert!(report.is_cooperatively_stopped());
}

#[tokio::test]
async fn application_signal_that_loses_requested_cause_retires_cleanly() {
    let supervisor = disabled_supervisor();
    let shutdown = supervisor.shutdown.clone();
    let (entered, entry) = oneshot::channel();
    let (dropped, destruction) = oneshot::channel();
    let driver = supervisor.run_until_shutdown_report(
        RuntimeShutdownSignal::infallible(PendingDropSignal {
            entered: Some(entered),
            dropped: Some(dropped),
        }),
        test_budget(),
    );

    entry.await.expect("application signal is being polled");
    shutdown.request();
    destruction
        .await
        .expect("losing signal is destroyed cooperatively");
    let report = driver.await;

    assert_eq!(report.cause(), crate::RuntimeShutdownCause::Requested);
    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::Settled
    ));
    assert!(report.signal_error().is_none());
    assert!(report.is_success());
    assert!(report.is_cooperatively_stopped());
    let signal = report
        .descendants()
        .iter()
        .find(|record| record.task == "shutdown_signal")
        .expect("retain the losing signal join");
    assert!(!signal.abort_requested);
    assert!(signal.error.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn losing_application_signal_retires_within_nonzero_grace_on_multithread_runtime() {
    let supervisor = disabled_supervisor();
    let shutdown = supervisor.shutdown.clone();
    let (entered, entry) = oneshot::channel();
    let (dropped, destruction) = oneshot::channel();
    let driver = supervisor.run_until_shutdown_report(
        RuntimeShutdownSignal::infallible(PendingDropSignal {
            entered: Some(entered),
            dropped: Some(dropped),
        }),
        RuntimeShutdownBudget::new(Duration::from_millis(250), Duration::from_secs(1))
            .expect("nonzero grace and abort allowance are valid"),
    );

    entry.await.expect("application signal is being polled");
    shutdown.request();
    destruction
        .await
        .expect("losing signal is destroyed within graceful allowance");
    let report = tokio::time::timeout(Duration::from_secs(3), driver)
        .await
        .expect("native settlement completes within the test allowance");

    assert_eq!(report.cause(), crate::RuntimeShutdownCause::Requested);
    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::Settled
    ));
    assert!(report.is_success());
    assert!(report.is_cooperatively_stopped());
    let signal = report
        .descendants()
        .iter()
        .find(|record| record.task == "shutdown_signal")
        .expect("retain the losing signal join");
    assert!(!signal.abort_requested);
    assert!(signal.error.is_none());
    assert!(matches!(
        report.classify(),
        crate::RuntimeSettlement::Clean(_)
    ));
}

async fn wait_for_queued_report(driver: &RuntimeShutdownDriver) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while driver.report.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owner queues a report");
}

#[tokio::test]
async fn dropping_before_owner_poll_requests_stop_and_settles() {
    let log = LogBuffer::default();
    let _capture = capture_logs(&log);
    let mut supervisor = disabled_supervisor();
    let shutdown = supervisor.shutdown.clone();
    let original = shutdown.clone();
    let (stopped, stop) = oneshot::channel();
    supervisor
        .tasks
        .spawn_on(&supervisor.runtime, "cooperative", async move {
            shutdown.requested().await;
            stopped.send(()).expect("test observes cooperative stop");
            RuntimeLoopExit::Shutdown
        });
    let driver =
        supervisor.run_until_shutdown_report(RuntimeShutdownSignal::pending(), test_budget());
    drop(driver); // Current-thread runtime has not polled the owner yet.
    assert!(original.is_requested());
    tokio::time::timeout(Duration::from_secs(1), stop)
        .await
        .expect("abandonment starts stop")
        .expect("native work settles");
    tokio::time::timeout(Duration::from_secs(1), async {
        while !log.text().contains(UNOBSERVED) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("unobserved settlement diagnostic");
    assert_eq!(log.text().matches(UNOBSERVED).count(), 1);
}

#[tokio::test]
async fn dropping_while_waiting_for_signal_keeps_first_stop_clock() {
    let log = LogBuffer::default();
    let _capture = capture_logs(&log);
    let supervisor = disabled_supervisor();
    let shutdown = supervisor.shutdown.clone();
    let (entered, entry) = oneshot::channel();
    let (dropped, destruction) = oneshot::channel();
    let driver = supervisor.run_until_shutdown_report(
        RuntimeShutdownSignal::infallible(PendingDropSignal {
            entered: Some(entered),
            dropped: Some(dropped),
        }),
        RuntimeShutdownBudget::new(Duration::from_millis(25), Duration::from_secs(5))
            .expect("test cancellation budget is valid"),
    );
    entry.await.expect("owner is waiting for external signal");
    let first = tokio::time::Instant::now() - Duration::from_millis(50);
    shutdown.request_since(first, crate::RuntimeShutdownCause::Requested);
    drop(driver);
    destruction
        .await
        .expect("abandoned waiter does not cancel signal settlement");
    tokio::time::timeout(Duration::from_secs(1), async {
        while !log.text().contains(UNOBSERVED) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owner emits the settled unobserved report");
    assert_eq!(shutdown.requested_at(), Some(first));
    let diagnostic = log.text();
    assert_eq!(diagnostic.matches(UNOBSERVED).count(), 1);
    assert!(diagnostic.contains("cause=Requested"));
    assert!(
        diagnostic.contains("settlement=Settled")
            || diagnostic.contains("settlement=GracefulTimeout"),
        "unexpected diagnostic: {diagnostic}"
    );
    assert!(!diagnostic.contains("settlement=AbortTimeout"));
    assert!(!diagnostic.contains("settlement=Interrupted"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_driver_during_abort_join_keeps_owner_settlement_alive() {
    let supervisor = disabled_supervisor();
    let shutdown = supervisor.shutdown.clone();
    let descendants = supervisor.descendants.clone();
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let (dropped_tx, dropped_rx) = oneshot::channel();
    let driver = supervisor.run_until_shutdown_report(
        RuntimeShutdownSignal::infallible(BlockingPollSignal {
            entered: Some(entered_tx),
            release: release_rx,
            dropped: Some(dropped_tx),
            panic_on_drop: false,
        }),
        RuntimeShutdownBudget::new(Duration::ZERO, Duration::from_secs(1))
            .expect("zero grace with an abort allowance is valid"),
    );

    tokio::task::spawn_blocking(move || {
        entered_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("signal poll blocks before external stop");
    })
    .await
    .expect("poll observer joins");

    shutdown.request();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let (_, unjoined) = descendants.snapshot();
            if unjoined
                .iter()
                .any(|task| task.task == "shutdown_signal" && task.abort_requested)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("settlement enters abort/join before driver drop");

    let report = driver.drop_and_retain_report_for_tests();
    release_tx
        .send(())
        .expect("release abort-phase signal destruction");
    dropped_rx
        .await
        .expect("driver drop does not cancel guarded destruction");
    let report = report
        .await
        .expect("settlement owner survives driver drop")
        .observe();

    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::GracefulTimeout
    ));
    let signal = report
        .descendants()
        .iter()
        .find(|record| record.task == "shutdown_signal")
        .expect("retain the abort-phase signal join");
    assert!(signal.abort_requested);
    assert!(
        signal
            .error
            .as_ref()
            .is_some_and(|error| error.is_cancelled())
    );
    assert!(!report.is_cooperatively_stopped());
}

#[tokio::test]
async fn queued_report_abandonment_emits_exactly_one_diagnostic() {
    let log = LogBuffer::default();
    let _capture = capture_logs(&log);
    let driver = disabled_supervisor().shutdown_report(test_budget());
    wait_for_queued_report(&driver).await;
    assert!(!log.text().contains(UNOBSERVED));
    drop(driver);
    assert_eq!(log.text().matches(UNOBSERVED).count(), 1);
}

#[tokio::test]
async fn observing_a_report_disarms_its_diagnostic() {
    let log = LogBuffer::default();
    let _capture = capture_logs(&log);
    let driver = disabled_supervisor().shutdown_report(test_budget());
    wait_for_queued_report(&driver).await;
    assert!(driver.await.is_success());
    assert!(!log.text().contains(UNOBSERVED));
}

#[tokio::test]
async fn signal_panic_preserves_evidence_and_allows_native_cooperative_settlement() {
    let mut supervisor = disabled_supervisor();
    supervisor
        .tasks
        .spawn_on(&supervisor.runtime, "observed", async {
            RuntimeLoopExit::Shutdown
        });
    supervisor.tasks.wait_until_finished_for_tests().await;
    let shutdown = supervisor.shutdown.clone();
    supervisor
        .tasks
        .spawn_on(&supervisor.runtime, "cooperative", async move {
            shutdown.requested().await;
            tokio::task::yield_now().await;
            RuntimeLoopExit::Shutdown
        });
    let report = supervisor
        .run_until_shutdown_report(
            RuntimeShutdownSignal::infallible(async {
                panic!("private signal panic");
            }),
            test_budget(),
        )
        .await;
    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::Settled
    ));
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::DescendantJoin { task: "shutdown_signal", source })
            if source.is_panic()
    ));
    assert!(
        report
            .loops()
            .iter()
            .any(|record| record.task == "observed")
    );
    assert!(report.loops().iter().any(|task| task.task == "cooperative"
        && matches!(task.result, Ok(RuntimeLoopExit::Shutdown))
        && !task.abort_requested));
    assert!(report.unjoined().is_empty());
    assert!(!report.is_cooperatively_stopped());
    assert!(!report.is_success());
    assert!(!report.graceful_timed_out());
    assert!(!report.abort_timed_out());
    assert!(matches!(report.signal_panic(),
        Some(crate::RuntimeShutdownSignalPanic::Poll { message })
            if message == "private signal panic"));
}

struct PanicOnDropSignal;

impl Future for PanicOnDropSignal {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }
}

impl Drop for PanicOnDropSignal {
    fn drop(&mut self) {
        panic!("private signal destructor panic");
    }
}

#[tokio::test]
async fn signal_destruction_cannot_erase_collected_loop_evidence() {
    let mut supervisor = disabled_supervisor();
    supervisor
        .tasks
        .spawn_on(&supervisor.runtime, "observed", async {
            RuntimeLoopExit::Shutdown
        });
    supervisor.tasks.wait_until_finished_for_tests().await;
    let report = supervisor
        .run_until_shutdown_report(
            RuntimeShutdownSignal::infallible(PanicOnDropSignal),
            test_budget(),
        )
        .await;
    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::Settled
    ));
    assert!(
        report
            .loops()
            .iter()
            .any(|record| record.task == "observed")
    );
    assert!(!report.is_cooperatively_stopped());
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::DescendantJoin { task: "shutdown_signal", source })
            if source.is_panic()
    ));
}

struct PendingPanicOnDropSignal(Option<oneshot::Sender<()>>);

impl Future for PendingPanicOnDropSignal {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        if let Some(entered) = self.0.take() {
            let _ = entered.send(());
        }
        Poll::Pending
    }
}

impl Drop for PendingPanicOnDropSignal {
    fn drop(&mut self) {
        panic!("private pending signal destructor panic");
    }
}

#[tokio::test]
async fn cancelling_a_pending_signal_contains_its_destructor_panic() {
    let mut supervisor = disabled_supervisor();
    let stop = supervisor.shutdown.clone();
    let task_stop = stop.clone();
    supervisor
        .tasks
        .spawn_on(&supervisor.runtime, "cooperative", async move {
            task_stop.requested().await;
            RuntimeLoopExit::Shutdown
        });
    let (entered, entry) = oneshot::channel();
    let driver = supervisor.run_until_shutdown_report(
        RuntimeShutdownSignal::infallible(PendingPanicOnDropSignal(Some(entered))),
        test_budget(),
    );
    entry.await.expect("signal polled before requesting stop");
    stop.request();
    let report = driver.await;
    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::Settled
    ));
    assert!(matches!(
        report.cause(),
        crate::RuntimeShutdownCause::Requested
    ));
    assert!(
        report
            .loops()
            .iter()
            .all(|record| matches!(record.result, Ok(RuntimeLoopExit::Shutdown)))
    );
    assert!(matches!(report.failure(),
        Some(RuntimeShutdownFailure::DescendantJoin { task: "shutdown_signal", source }) if source.is_panic()));
    assert!(!report.is_cooperatively_stopped());
    assert_eq!(report.is_success(), report.failure().is_none());
}

struct ErrorThenPanicOnDropSignal;

impl Future for ErrorThenPanicOnDropSignal {
    type Output = Result<(), io::Error>;

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(Err(io::Error::other("private signal setup failure")))
    }
}

impl Drop for ErrorThenPanicOnDropSignal {
    fn drop(&mut self) {
        panic!("private failed signal destructor panic");
    }
}

#[tokio::test]
async fn signal_error_survives_a_second_failure_during_signal_destruction() {
    let report = disabled_supervisor()
        .run_until_shutdown_report(
            RuntimeShutdownSignal::fallible(ErrorThenPanicOnDropSignal),
            test_budget(),
        )
        .await;
    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::Settled
    ));
    assert!(matches!(
        report.cause(),
        crate::RuntimeShutdownCause::SignalFailed
    ));
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::Signal { .. })
    ));
    let error = report
        .signal_error()
        .expect("retain returned error before destruction");
    assert!(
        std::error::Error::source(error)
            .expect("retain original signal error")
            .downcast_ref::<io::Error>()
            .is_some()
    );
    assert!(
        report
            .descendants()
            .iter()
            .any(|record| record.task == "shutdown_signal"
                && record.error.as_ref().is_some_and(|error| error.is_panic()))
    );
    assert!(!report.is_cooperatively_stopped());
    assert!(!report.is_success());
    assert!(!format!("{report:?}").contains("private"));
    assert!(matches!(report.signal_panic(),
        Some(crate::RuntimeShutdownSignalPanic::Destruction { message })
            if message == "private failed signal destructor panic"));
}

#[tokio::test]
async fn unavailable_report_retains_live_signal_observations() {
    let mut supervisor = disabled_supervisor();
    supervisor
        .tasks
        .spawn_on(&supervisor.runtime, "pending", std::future::pending());
    let shutdown = supervisor.shutdown.clone();
    let mut driver = supervisor.run_until_shutdown_report(
        RuntimeShutdownSignal::fallible(ErrorThenPanicOnDropSignal),
        RuntimeShutdownBudget::new(Duration::from_secs(60), Duration::ZERO)
            .expect("long graceful allowance is valid"),
    );

    tokio::time::timeout(Duration::from_secs(2), async {
        while shutdown.signal_error().is_none() || shutdown.signal_panic().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("signal observations are recorded before channel closure");
    driver.report.close();
    let report = driver.await;

    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::Interrupted { .. }
    ));
    assert_eq!(report.cause(), crate::RuntimeShutdownCause::SignalFailed);
    assert!(report.signal_error().is_some());
    assert!(matches!(
        report.signal_panic(),
        Some(crate::RuntimeShutdownSignalPanic::Destruction { message })
            if message == "private failed signal destructor panic"
    ));
    assert!(!report.is_success());
    assert!(!report.is_cooperatively_stopped());
}

#[test]
fn never_polled_signal_destructor_cannot_unwind_the_settlement_owner() {
    use futures_util::FutureExt;
    let owner = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("construct captured runtime");
    let driver = {
        let _entered = owner.enter();
        disabled_supervisor().run_until_shutdown_report(
            RuntimeShutdownSignal::infallible(PanicOnDropSignal),
            test_budget(),
        )
    };
    drop(owner);
    let report = driver
        .now_or_never()
        .expect("owner destruction delivers partial report");
    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::Interrupted { .. }
    ));
    assert!(!report.is_cooperatively_stopped());
    assert!(!report.is_success());
}

#[test]
fn terminal_call_after_captured_runtime_stops_contains_signal_destructor_panic() {
    use futures_util::FutureExt;
    let owner = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("construct captured runtime");
    let supervisor = {
        let _entered = owner.enter();
        disabled_supervisor()
    };
    drop(owner);

    let driver = catch_unwind(AssertUnwindSafe(|| {
        supervisor.run_until_shutdown_report(
            RuntimeShutdownSignal::infallible(PanicOnDropSignal),
            test_budget(),
        )
    }))
    .expect("terminal call contains never-polled signal destruction panic");
    let report = driver
        .now_or_never()
        .expect("stopped owner reports without polling");
    assert!(matches!(
        report.signal_panic(),
        Some(crate::RuntimeShutdownSignalPanic::Destruction { message })
            if message == "private signal destructor panic"
    ));
    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::Interrupted { .. }
    ));
    assert!(!report.is_cooperatively_stopped());
    assert!(!report.is_success());
}

#[test]
fn owner_loss_retains_an_already_observed_signal_error() {
    use futures_util::FutureExt;
    let owner = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("construct captured runtime");
    let (entered, entry) = oneshot::channel();
    let driver = {
        let _entered = owner.enter();
        let mut supervisor = disabled_supervisor();
        supervisor
            .tasks
            .spawn_on(&supervisor.runtime, "pending", std::future::pending());
        supervisor.run_until_shutdown_report(
            RuntimeShutdownSignal::fallible(async move {
                entered.send(()).expect("test observes signal error");
                Err::<(), _>(io::Error::other("private retained error"))
            }),
            test_budget(),
        )
    };
    owner.block_on(entry).expect("signal polled to completion");
    drop(owner);
    let report = driver.now_or_never().expect("owner loss delivers evidence");
    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::Interrupted { .. }
    ));
    assert!(report.signal_error().is_some());
    assert!(matches!(
        report.cause(),
        crate::RuntimeShutdownCause::SignalFailed
    ));
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::Signal { .. })
    ));
    assert!(!report.is_success());
    assert!(!report.is_cooperatively_stopped());
}

#[tokio::test]
async fn unobserved_failure_diagnostic_redacts_panic_contents() {
    let log = LogBuffer::default();
    let _capture = capture_logs(&log);
    let mut supervisor = disabled_supervisor();
    supervisor
        .tasks
        .spawn_on(&supervisor.runtime, "panicking", async {
            panic!("private native panic payload");
        });
    supervisor.tasks.wait_until_finished_for_tests().await;
    let driver = supervisor.shutdown_report(test_budget());
    wait_for_queued_report(&driver).await;
    drop(driver);
    let text = log.text();
    assert_eq!(text.matches(UNOBSERVED).count(), 1);
    assert!(!text.contains("private native panic payload"));
    assert!(text.contains("successful=false"));
}
