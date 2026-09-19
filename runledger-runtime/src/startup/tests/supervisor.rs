use crate::{
    RuntimeStartup, RuntimeStartupStopped, Supervisor, config::JobsConfig, registry::JobRegistry,
};
use sqlx::postgres::PgPoolOptions;
use std::{future::ready, time::Duration};
use tokio::time::timeout;

async fn closed_pool() -> runledger_postgres::DbPool {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .expect("valid lazy pool");
    // No runtime loop can complete any database operation against this pool.
    pool.close().await;
    pool
}

fn config() -> JobsConfig {
    JobsConfig {
        worker_id: "initialization-test".to_owned(),
        poll_interval: Duration::from_secs(1),
        claim_batch_size: 1,
        lease_ttl_seconds: 10,
        max_global_concurrency: 1,
        reaper_interval: Duration::from_secs(1),
        schedule_poll_interval: Duration::from_secs(1),
        reaper_retry_delay_ms: 1_000,
    }
}

fn test_budget() -> crate::RuntimeShutdownBudget {
    crate::RuntimeShutdownBudget::new(Duration::from_secs(1), Duration::from_secs(1))
        .expect("valid test budget")
}

#[tokio::test]
async fn initialization_does_not_require_database_success_or_queue_activity() {
    let pool = closed_pool().await;
    let supervisor = Supervisor::builder(&pool, config())
        .expect("runtime exists")
        .with_registry(JobRegistry::new())
        .build()
        .expect("valid supervisor");
    let observer = supervisor.startup_observer();
    assert!(matches!(
        observer.snapshot(),
        RuntimeStartup::Starting { .. }
    ));
    // Cancelling this first observation does not cancel startup or lose updates.
    tokio::select! {
        biased;
        state = observer.wait_initialized() => panic!("loops have not been polled yet: {state:?}"),
        () = ready(()) => {}
    }
    assert_eq!(
        timeout(Duration::from_secs(1), observer.wait_initialized())
            .await
            .expect("initialization bounded"),
        Ok(())
    );
    assert!(
        supervisor.shutdown_report(test_budget()).await.is_success(),
        "loops stop"
    );
    assert_eq!(
        observer.wait_initialized().await,
        Err(RuntimeStartupStopped)
    );
}

#[tokio::test]
async fn stop_before_first_poll_cannot_be_revived_by_loop_initialization() {
    let pool = closed_pool().await;
    let supervisor = Supervisor::builder(&pool, config())
        .expect("runtime exists")
        .with_registry(JobRegistry::new())
        .build()
        .expect("valid supervisor");
    let observer = supervisor.startup_observer();
    supervisor.shutdown_handle().request_shutdown();
    assert_eq!(
        observer.wait_initialized().await,
        Err(RuntimeStartupStopped)
    );
    assert!(
        supervisor.shutdown_report(test_budget()).await.is_success(),
        "loops stop"
    );
    assert_eq!(
        observer.wait_initialized().await,
        Err(RuntimeStartupStopped)
    );
}

#[tokio::test]
async fn disabled_loops_do_not_block_initialization() {
    let pool = closed_pool().await;
    let supervisor = Supervisor::builder(&pool, config())
        .expect("runtime exists")
        .with_registry(JobRegistry::new())
        .disable_scheduler()
        .disable_reaper()
        .disable_intent_promoter()
        .build()
        .expect("valid supervisor");
    let observer = supervisor.startup_observer();
    assert_eq!(
        observer.snapshot(),
        RuntimeStartup::Starting {
            pending: vec!["worker"]
        }
    );
    assert_eq!(
        timeout(Duration::from_secs(1), observer.wait_initialized())
            .await
            .expect("initialization bounded"),
        Ok(())
    );
    assert!(
        supervisor.shutdown_report(test_budget()).await.is_success(),
        "loop stops"
    );
}

#[tokio::test]
async fn invalid_configuration_and_unpolled_future_loss_stop_startup() {
    for poll_invalid_configuration in [false, true] {
        let pool = closed_pool().await;
        let initialization = crate::startup::Initialization::new(vec!["scheduler"]);
        let observer = initialization.observer();
        let mut invalid = config();
        invalid.claim_batch_size = 0;
        let (_, shutdown) = crate::shutdown::ShutdownSignal::channel();
        let future = crate::scheduler::run_scheduler_loop_initialized(
            pool,
            invalid,
            shutdown,
            Some(initialization.loop_token("scheduler")),
        );
        if poll_invalid_configuration {
            assert!(matches!(
                future.await,
                crate::RuntimeLoopExit::InvalidConfig(_)
            ));
        } else {
            drop(future);
        }
        assert_eq!(
            observer.wait_initialized().await,
            Err(RuntimeStartupStopped)
        );
    }
}

#[tokio::test]
async fn native_handle_drives_the_report_without_an_external_signal() {
    let pool = closed_pool().await;
    let supervisor = Supervisor::builder(&pool, config())
        .expect("runtime exists")
        .with_registry(JobRegistry::new())
        .disable_scheduler()
        .disable_reaper()
        .build()
        .expect("valid supervisor");
    let startup = supervisor.startup_observer();
    startup
        .wait_initialized()
        .await
        .expect("empty worker initializes");
    supervisor.shutdown_handle().request_shutdown();
    let budget = crate::RuntimeShutdownBudget::new(Duration::from_secs(1), Duration::from_secs(1))
        .expect("valid budget");
    let report = supervisor
        .run_until_shutdown_report(crate::RuntimeShutdownSignal::pending(), budget)
        .await;
    assert!(report.is_success(), "{report:?}");
    assert_eq!(report.loops().len(), 2);
    assert_eq!(startup.snapshot(), RuntimeStartup::Stopped);
}
