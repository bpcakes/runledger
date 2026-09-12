use super::{Supervisor, tests::test_config};
use crate::{RuntimeError, RuntimeShutdownBudget, registry::JobRegistry};
use sqlx::postgres::PgPoolOptions;
use std::time::Duration;

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
            async {},
            RuntimeShutdownBudget::new(Duration::from_secs(1), Duration::from_secs(1))
                .expect("valid bounded settlement"),
        )
        .await;
    assert!(report.is_success());
    let mut loops: Vec<_> = report.loops.iter().map(|record| record.task).collect();
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
