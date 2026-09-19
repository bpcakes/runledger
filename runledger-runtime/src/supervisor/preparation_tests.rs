use super::{PreparedSupervisor, RuntimeShutdownDriver, Supervisor, tests::test_config};
use crate::{
    RuntimeError, RuntimeShutdownBudget, RuntimeShutdownReport, RuntimeShutdownSignal,
    registry::JobRegistry,
};
use futures_util::FutureExt;
use sqlx::postgres::PgPoolOptions;
use std::time::{Duration, Instant};

fn owner_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("construct owner runtime")
}

fn prepared_disabled_supervisor(runtime: &tokio::runtime::Runtime) -> PreparedSupervisor {
    let _entered = runtime.enter();
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .expect("valid inert pool URL");
    Supervisor::builder(&pool, test_config())
        .expect("owner runtime is entered during preparation")
        .disable_worker()
        .disable_scheduler()
        .disable_reaper()
        .prepare()
        .expect("all-disabled supervisor prepares")
}

fn receive_report_outside_runtime(mut driver: RuntimeShutdownDriver) -> RuntimeShutdownReport {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        if let Some(report) = (&mut driver).now_or_never() {
            return report;
        }
        assert!(
            Instant::now() < deadline,
            "owner runtime did not complete terminal settlement"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn owner_teardown_before_first_poll_returns_interruption() {
    let owner = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("construct unpolled owner runtime");
    let supervisor = prepared_disabled_supervisor(&owner).start();
    let driver = supervisor.run_until_shutdown_report(
        RuntimeShutdownSignal::pending(),
        RuntimeShutdownBudget::new(Duration::from_secs(1), Duration::from_secs(1))
            .expect("valid budget"),
    );
    drop(owner);

    let report = receive_report_outside_runtime(driver);
    assert!(matches!(
        report.settlement(),
        crate::RuntimeShutdownSettlement::Interrupted { .. }
    ));
    assert!(matches!(
        report.failure(),
        Some(crate::RuntimeShutdownFailure::SettlementInterrupted)
    ));
    assert!(!report.is_cooperatively_stopped());
    assert!(!report.is_success());
}

#[test]
fn terminal_call_after_owner_teardown_returns_interruption() {
    for immediate in [false, true] {
        let owner = owner_runtime();
        let supervisor = prepared_disabled_supervisor(&owner).start();
        drop(owner);

        let budget = RuntimeShutdownBudget::new(Duration::from_secs(1), Duration::from_secs(1))
            .expect("valid budget");
        let driver = if immediate {
            supervisor.shutdown_report(budget)
        } else {
            supervisor.run_until_shutdown_report(RuntimeShutdownSignal::pending(), budget)
        };
        let report = receive_report_outside_runtime(driver);
        assert!(matches!(
            report.settlement(),
            crate::RuntimeShutdownSettlement::Interrupted { .. }
        ));
        assert!(matches!(
            report.failure(),
            Some(crate::RuntimeShutdownFailure::SettlementInterrupted)
        ));
        if !immediate {
            assert!(
                report
                    .descendants()
                    .iter()
                    .any(|record| record.task == "shutdown_signal"
                        && record
                            .error
                            .as_ref()
                            .is_some_and(|error| error.is_cancelled())),
                "owner-controlled cancellation must remain visible as join evidence"
            );
        }
        assert!(!report.is_cooperatively_stopped());
        assert!(!report.is_success());
    }
}

#[test]
fn owner_teardown_during_settlement_retains_observations() {
    // Dropping a current-thread runtime cannot race a worker that completes
    // settlement before cancellation reaches the owner.
    let owner = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("construct owner runtime");
    let mut supervisor = prepared_disabled_supervisor(&owner).start();
    supervisor
        .tasks
        .spawn_on(owner.handle(), "observed", async {
            crate::RuntimeLoopExit::Shutdown
        });
    let (entered, entry) = tokio::sync::oneshot::channel();
    owner.block_on(async {
        supervisor.tasks.wait_until_finished_for_tests().await;
    });
    supervisor
        .tasks
        .spawn_on(owner.handle(), "pending", std::future::pending());
    let driver = supervisor.run_until_shutdown_report(
        RuntimeShutdownSignal::infallible(async move {
            entered.send(()).expect("test observes graceful drain");
        }),
        RuntimeShutdownBudget::new(Duration::from_secs(1), Duration::from_secs(1))
            .expect("valid budget"),
    );
    owner.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), entry)
            .await
            .expect("owner polls signal")
            .expect("signal starts graceful drain");
    });
    drop(owner);

    let report = receive_report_outside_runtime(driver);
    assert!(matches!(
        report.settlement(),
        crate::RuntimeShutdownSettlement::Interrupted { .. }
    ));
    assert!(
        report
            .loops()
            .iter()
            .any(|record| record.task == "observed")
    );
    assert!(report.unjoined().iter().any(|task| task.task == "pending"));
    assert!(!report.is_cooperatively_stopped());
}

#[test]
fn off_runtime_shutdown_collects_finished_loops_and_descendants() {
    let owner = owner_runtime();
    let mut supervisor = prepared_disabled_supervisor(&owner).start();
    supervisor
        .tasks
        .spawn_on(owner.handle(), "finished_loop", async {
            crate::RuntimeLoopExit::Completed
        });
    let descendant = {
        let _entered = owner.enter();
        supervisor.descendants.spawn("finished_descendant", async {
            panic!("private descendant panic");
        })
    };
    owner.block_on(async {
        supervisor.tasks.wait_until_finished_for_tests().await;
        while !descendant.is_finished() {
            tokio::task::yield_now().await;
        }
    });
    let driver = supervisor.shutdown_report(
        RuntimeShutdownBudget::new(Duration::from_secs(1), Duration::from_secs(1))
            .expect("valid budget"),
    );
    let report = receive_report_outside_runtime(driver);
    assert_eq!(
        report.cause(),
        crate::RuntimeShutdownCause::LoopFailure("finished_loop")
    );
    assert!(
        report
            .loops()
            .iter()
            .any(|record| record.task == "finished_loop")
    );
    assert!(
        report
            .descendants()
            .iter()
            .any(|record| record.task == "finished_descendant")
    );
    assert!(!report.is_success());
    assert!(!report.is_cooperatively_stopped());
}

#[tokio::test]
async fn dropping_preparation_starts_no_native_tasks() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .expect("valid inert pool URL");
    pool.close().await;
    tokio::task::yield_now().await;
    let runtime = tokio::runtime::Handle::current();
    let before = runtime.metrics().num_alive_tasks();
    let prepared = Supervisor::builder(&pool, test_config())
        .expect("test runtime exists")
        .with_registry(JobRegistry::new())
        .prepare()
        .expect("valid empty registry");
    tokio::task::yield_now().await;
    assert_eq!(runtime.metrics().num_alive_tasks(), before);
    drop(prepared);
    tokio::task::yield_now().await;
    assert_eq!(runtime.metrics().num_alive_tasks(), before);
}

#[tokio::test]
async fn preparation_owns_its_pool_and_selected_loops() {
    let prepared = {
        let pool = PgPoolOptions::new()
            .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
            .expect("valid inert pool URL");
        pool.close().await;
        Supervisor::builder(&pool, test_config())
            .expect("test runtime exists")
            .with_registry(JobRegistry::new())
            .disable_scheduler()
            .disable_reaper()
            .prepare()
            .expect("valid empty registry")
    };
    let native = prepared.start();
    native
        .startup_observer()
        .wait_initialized()
        .await
        .expect("selected loops initialize locally");
    let report = native
        .run_until_shutdown_report(
            RuntimeShutdownSignal::infallible(async {}),
            RuntimeShutdownBudget::new(Duration::from_secs(1), Duration::from_secs(1))
                .expect("valid bounded settlement"),
        )
        .await;
    assert!(report.is_success());
    let mut loops: Vec<_> = report.loops().iter().map(|record| record.task).collect();
    loops.sort_unstable();
    assert_eq!(loops, ["intent_promoter", "worker"]);
}

#[tokio::test]
async fn preparation_rejects_missing_registry_without_starting_tasks() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .expect("valid inert pool URL");
    pool.close().await;
    tokio::task::yield_now().await;
    let runtime = tokio::runtime::Handle::current();
    let before = runtime.metrics().num_alive_tasks();
    assert!(matches!(
        Supervisor::builder(&pool, test_config())
            .expect("test runtime exists")
            .prepare(),
        Err(RuntimeError::MissingRegistry { .. })
    ));
    tokio::task::yield_now().await;
    assert_eq!(runtime.metrics().num_alive_tasks(), before);
}

#[test]
fn both_terminal_methods_settle_on_the_captured_runtime_outside_a_runtime_context() {
    let owner = owner_runtime();

    let supervisor = prepared_disabled_supervisor(&owner).start();
    assert!(tokio::runtime::Handle::try_current().is_err());
    let report = receive_report_outside_runtime(
        supervisor.shutdown_report(
            RuntimeShutdownBudget::new(Duration::from_secs(1), Duration::from_secs(1))
                .expect("valid bounded settlement"),
        ),
    );
    assert!(report.is_success());

    let supervisor = prepared_disabled_supervisor(&owner).start();
    assert!(tokio::runtime::Handle::try_current().is_err());
    let report = receive_report_outside_runtime(
        supervisor.run_until_shutdown_report(
            RuntimeShutdownSignal::infallible(async {}),
            RuntimeShutdownBudget::new(Duration::from_secs(1), Duration::from_secs(1))
                .expect("valid bounded settlement"),
        ),
    );
    assert!(report.is_success());
}

#[test]
fn terminal_settlement_is_not_owned_by_an_unrelated_caller_runtime() {
    let owner = owner_runtime();
    let supervisor = prepared_disabled_supervisor(&owner).start();
    let caller = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .expect("construct unrelated caller runtime");
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();

    let driver = {
        let _entered = caller.enter();
        supervisor.run_until_shutdown_report(
            RuntimeShutdownSignal::infallible(async move {
                entered_tx
                    .send(())
                    .expect("fixture observes shutdown future polling");
                release_rx.await.expect("fixture releases shutdown signal");
            }),
            RuntimeShutdownBudget::new(Duration::from_secs(1), Duration::from_secs(1))
                .expect("valid bounded settlement"),
        )
    };
    entered_rx
        .recv_timeout(Duration::from_secs(1))
        .expect("captured owner runtime polls terminal settlement");
    caller.shutdown_background();
    release_tx
        .send(())
        .expect("owner still owns shutdown future");

    let report = receive_report_outside_runtime(driver);
    assert!(report.is_success());
}
