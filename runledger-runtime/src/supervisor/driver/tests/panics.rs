use super::*;
use crate::RuntimeShutdownSignalPanic;
use std::{
    panic::{AssertUnwindSafe, catch_unwind, panic_any},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
};

struct DoublePanicSignal {
    drops: Arc<AtomicUsize>,
    opaque_payload: bool,
}

impl Future for DoublePanicSignal {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        if self.opaque_payload {
            panic_any(PanickingPayload);
        }
        panic!("private poll panic");
    }
}

impl Drop for DoublePanicSignal {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        panic!("private destruction panic");
    }
}

struct PanickingPayload;

struct BlockingPanicSignal {
    poll_entered: Option<oneshot::Sender<()>>,
    poll_release: std::sync::mpsc::Receiver<()>,
    drop_entered: Option<oneshot::Sender<()>>,
    drop_release: std::sync::mpsc::Receiver<()>,
    drops: Arc<AtomicUsize>,
    panic_on_drop: bool,
}

impl Future for BlockingPanicSignal {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
        let _ = self.poll_entered.take().expect("one poll").send(());
        tokio::task::block_in_place(|| {
            let _ = self.poll_release.recv();
        });
        panic!("blocked signal poll panic");
    }
}

impl Drop for BlockingPanicSignal {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        let _ = self.drop_entered.take().expect("one destruction").send(());
        // Disconnection also releases the task if a test assertion fails.
        // Keep the executor schedulable while this synchronous destructor is
        // held open: this tests stop ordering, not Tokio worker starvation.
        tokio::task::block_in_place(|| {
            let _ = self.drop_release.recv();
        });
        assert!(!self.panic_on_drop, "blocked signal destruction panic");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poll_panic_starts_settlement_before_blocking_destruction() {
    for earlier_stop in [false, true] {
        for (panic_on_drop, later_panics) in
            [(false, false), (false, true), (true, false), (true, true)]
        {
            let mut supervisor = disabled_supervisor();
            let shutdown = supervisor.shutdown.clone();
            let descendants = supervisor.descendants.clone();
            let stopping = shutdown.clone();
            let draining = shutdown.clone();
            supervisor
                .tasks
                .spawn_on(&supervisor.runtime, "later", async move {
                    draining.requested().await;
                    assert!(!later_panics, "later native loop panic");
                    std::future::pending::<RuntimeLoopExit>().await
                });
            let (native_stopped, native_stop) = oneshot::channel();
            supervisor
                .tasks
                .spawn_on(&supervisor.runtime, "cooperative", async move {
                    stopping.requested().await;
                    let _ = native_stopped.send(());
                    RuntimeLoopExit::Shutdown
                });
            let (poll_entered, poll_entry) = oneshot::channel();
            let (poll_release, poll_gate) = std::sync::mpsc::channel();
            let (drop_entered, drop_entry) = oneshot::channel();
            let (drop_release, drop_gate) = std::sync::mpsc::channel();
            let drops = Arc::new(AtomicUsize::new(0));
            let driver = supervisor.run_until_shutdown_report(
                RuntimeShutdownSignal::infallible(BlockingPanicSignal {
                    poll_entered: Some(poll_entered),
                    poll_release: poll_gate,
                    drop_entered: Some(drop_entered),
                    drop_release: drop_gate,
                    drops: drops.clone(),
                    panic_on_drop,
                }),
                RuntimeShutdownBudget::new(Duration::from_millis(100), Duration::from_millis(20))
                    .expect("bounded settlement"),
            );
            tokio::time::timeout(Duration::from_secs(2), poll_entry)
                .await
                .expect("signal polled")
                .expect("poll entry");
            if earlier_stop {
                shutdown.request();
            }
            let earlier_clock = shutdown.requested_at();
            poll_release.send(()).expect("release signal poll");
            tokio::time::timeout(Duration::from_secs(2), drop_entry)
                .await
                .expect("signal destruction entered")
                .expect("drop entry");

            assert!(
                shutdown.is_requested(),
                "caught panic must publish stop before Drop"
            );
            assert!(matches!(
                shutdown.signal_panic(),
                Some(RuntimeShutdownSignalPanic::Poll { .. })
            ));
            tokio::time::timeout(Duration::from_secs(2), native_stop)
                .await
                .expect("native loop observes stop while Drop blocks")
                .expect("native stopped");
            let report = tokio::time::timeout(Duration::from_secs(2), driver)
                .await
                .expect("shutdown budget runs while destruction is blocked");
            assert!(report.abort_timed_out());
            assert!(!report.is_success());
            assert!(!report.is_cooperatively_stopped());
            let outcome = report.classify();
            assert!(matches!(outcome, crate::RuntimeSettlement::Unsettled(_)));
            let report = outcome.report();
            assert!(
                report
                    .loops()
                    .iter()
                    .any(|record| record.task == "cooperative"
                        && matches!(record.result, Ok(RuntimeLoopExit::Shutdown)))
            );
            let signal = report
                .unjoined()
                .iter()
                .find(|task| task.task == "shutdown_signal")
                .expect("destruction is still an unsettled obligation");
            assert_eq!(signal.abort_requested, earlier_stop);
            let later = report
                .loops()
                .iter()
                .find(|record| record.task == "later")
                .expect("the later failure or owner-issued cancellation is retained");
            let error = later
                .result
                .as_ref()
                .expect_err("later loop must fail or be aborted");
            assert_eq!(error.is_panic(), later_panics);
            assert_eq!(error.is_cancelled(), !later_panics);
            if earlier_stop {
                assert_eq!(report.cause(), crate::RuntimeShutdownCause::Requested);
                assert_eq!(shutdown.requested_at(), earlier_clock);
                assert!(matches!(
                    report.failure(),
                    Some(RuntimeShutdownFailure::AbortTimeout { .. })
                ));
            } else {
                assert!(
                    matches!(
                        report.failure(),
                        Some(RuntimeShutdownFailure::SignalPanicked)
                    ),
                    "the observed initiating panic outranks later native failure or cancellation: {:?}",
                    report.failure()
                );
                assert_eq!(
                    report.cause(),
                    crate::RuntimeShutdownCause::DescendantFailure {
                        task: "shutdown_signal",
                        id: signal.id,
                    }
                );
            }

            drop_release.send(()).expect("release signal destruction");
            assert_released_signal_panic_joins(&descendants).await;
            assert_eq!(drops.load(Ordering::SeqCst), 1);
            assert!(
                matches!(
                    shutdown.signal_panic(),
                    Some(RuntimeShutdownSignalPanic::PollAndDestruction { .. })
                ) == panic_on_drop
            );
        }
    }
}

async fn assert_released_signal_panic_joins(descendants: &crate::settlement::TaskRegistry) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let (records, unjoined) = descendants.snapshot();
            if unjoined.is_empty() {
                let record = records
                    .iter()
                    .find(|record| record.task == "shutdown_signal")
                    .expect("retain fatal signal join");
                let error = record.error.as_ref().expect("panic is a join failure");
                assert!(error.is_panic());
                assert!(
                    error.to_string().contains("blocked signal poll panic"),
                    "the poll panic remains primary even if destruction also panics"
                );
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("released destruction joins");
}

impl Drop for PanickingPayload {
    fn drop(&mut self) {
        panic!("opaque panic payload destruction must not execute");
    }
}

fn assert_both_panics(report: &RuntimeShutdownReport, opaque_payload: bool) {
    let Some(RuntimeShutdownSignalPanic::PollAndDestruction {
        poll_message,
        destruction_message,
    }) = report.signal_panic()
    else {
        panic!("both signal panic observations must survive");
    };
    assert_eq!(
        poll_message,
        if opaque_payload {
            "non-string panic payload"
        } else {
            "private poll panic"
        }
    );
    assert_eq!(destruction_message, "private destruction panic");
    assert!(!report.is_success());
    assert!(!report.is_cooperatively_stopped());
    assert!(report.signal_error().is_none());
    assert!(!format!("{report:?}").contains("private"));
    assert!(!format!("{:?}", report.signal_panic()).contains("private"));
}

fn submitted_signal_case(opaque_payload: bool, lose_owner: bool) {
    use futures_util::FutureExt;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("test runtime");
    let drops = Arc::new(AtomicUsize::new(0));
    let (driver, shutdown) = {
        let _entered = runtime.enter();
        let mut supervisor = disabled_supervisor();
        let shutdown = supervisor.shutdown.clone();
        let stopping = shutdown.clone();
        supervisor
            .tasks
            .spawn_on(&supervisor.runtime, "native", async move {
                if lose_owner {
                    std::future::pending::<()>().await;
                }
                stopping.requested().await;
                RuntimeLoopExit::Shutdown
            });
        let driver = supervisor.run_until_shutdown_report(
            RuntimeShutdownSignal::infallible(DoublePanicSignal {
                drops: drops.clone(),
                opaque_payload,
            }),
            test_budget(),
        );
        (driver, shutdown)
    };
    let report = if lose_owner {
        runtime.block_on(async {
            while shutdown.signal_panic().is_none() {
                tokio::task::yield_now().await;
            }
        });
        drop(runtime);
        let report = driver.now_or_never().expect("interrupted owner reports");
        assert!(matches!(
            report.settlement(),
            RuntimeShutdownSettlement::Interrupted { .. }
        ));
        report
    } else {
        let report = runtime.block_on(driver);
        assert!(matches!(
            report.settlement(),
            RuntimeShutdownSettlement::Settled
        ));
        assert!(report.loops().iter().any(|record| record.task == "native"
            && matches!(record.result, Ok(RuntimeLoopExit::Shutdown))));
        assert!(matches!(report.failure(),
            Some(RuntimeShutdownFailure::DescendantJoin { task: "shutdown_signal", source })
                if source.is_panic()));
        report
    };
    assert_both_panics(&report, opaque_payload);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

fn unsubmitted_signal_case() {
    let drops = Arc::new(AtomicUsize::new(0));
    let log = LogBuffer::default();
    let _capture = capture_logs(&log);
    drop(RuntimeShutdownSignal::infallible(DoublePanicSignal {
        drops: drops.clone(),
        opaque_payload: false,
    }));
    let original = catch_unwind(AssertUnwindSafe(|| {
        let _signal = RuntimeShutdownSignal::infallible(DoublePanicSignal {
            drops: drops.clone(),
            opaque_payload: false,
        });
        panic!("original application unwind");
    }))
    .expect_err("the original unwind is preserved");
    assert_eq!(
        original.downcast_ref::<&str>(),
        Some(&"original application unwind")
    );
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert_eq!(
        log.text()
            .matches("shutdown signal destruction panicked")
            .count(),
        2
    );
    assert!(!log.text().contains("private"));
}

struct ChildGuard(Option<Child>);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

#[test]
fn signal_panic_containment_subprocess() {
    const CHILD: &str = "RUNLEDGER_SIGNAL_PANIC_CHILD";
    if let Ok(case) = std::env::var(CHILD) {
        // Expected caught panics should not fill the child's output pipes.
        std::panic::set_hook(Box::new(|_| {}));
        match case.as_str() {
            "combined" => submitted_signal_case(false, false),
            "opaque" => submitted_signal_case(true, false),
            "owner_loss" => submitted_signal_case(false, true),
            "unsubmitted" => unsubmitted_signal_case(),
            _ => panic!("unknown child case"),
        }
        println!("SIGNAL_PANIC_CHILD_OK");
        return;
    }
    for case in ["combined", "opaque", "owner_loss", "unsubmitted"] {
        let child = Command::new(std::env::current_exe().expect("test executable"))
            .args([
                "--exact",
                "supervisor::driver::tests::panics::signal_panic_containment_subprocess",
                "--nocapture",
            ])
            .env(CHILD, case)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn signal panic child");
        let mut child = ChildGuard(Some(child));
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        while child
            .0
            .as_mut()
            .expect("child exists")
            .try_wait()
            .expect("child status")
            .is_none()
        {
            assert!(
                std::time::Instant::now() < deadline,
                "{case} child timed out"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        let output = child
            .0
            .take()
            .expect("child exists")
            .wait_with_output()
            .expect("child output");
        assert!(
            output.status.success(),
            "{case}: {:?}\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("SIGNAL_PANIC_CHILD_OK"),
            "{case}: child must execute its assertions"
        );
    }
}
