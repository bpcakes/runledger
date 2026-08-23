use std::future::pending;
use std::sync::Arc;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use tokio::sync::{Notify, watch};
use tokio::time::timeout;

use super::super::WorkerLoop;
use crate::RuntimeLoopExit;
use crate::config::JobsConfig;
use crate::observer::JobLifecycleObservers;
use crate::registry::JobRegistry;

const UNUSED_LAZY_POOL_URL: &str =
    "postgres://postgres:postgres@127.0.0.1:65535/runledger_worker_capacity_test";

fn worker_with_capacity(max_global_concurrency: usize) -> WorkerLoop {
    let pool = PgPoolOptions::new()
        .connect_lazy(UNUSED_LAZY_POOL_URL)
        .expect("construct lazy capacity-test pool");
    let (_, shutdown) = watch::channel(false);
    WorkerLoop::new(
        pool,
        JobRegistry::new(),
        JobsConfig {
            worker_id: "worker-capacity-test".to_owned(),
            poll_interval: Duration::from_millis(10),
            claim_batch_size: 4,
            lease_ttl_seconds: 30,
            max_global_concurrency,
            reaper_interval: Duration::from_secs(30),
            schedule_poll_interval: Duration::from_secs(30),
            reaper_retry_delay_ms: 1_000,
        },
        shutdown,
        JobLifecycleObservers::empty(),
    )
}

#[tokio::test]
async fn join_set_occupancy_is_worker_capacity() {
    let mut worker = worker_with_capacity(2);
    assert_eq!(worker.available_capacity(), 2);

    worker.join_set.spawn(pending());
    assert_eq!(worker.available_capacity(), 1);
    worker.join_set.spawn(pending());
    assert_eq!(worker.available_capacity(), 0);

    worker.join_set.abort_all();
    while worker.join_set.join_next().await.is_some() {}
    assert_eq!(worker.available_capacity(), 2);
}

#[tokio::test]
async fn crashed_job_task_is_drained_and_restores_capacity() {
    let mut worker = worker_with_capacity(1);
    worker
        .join_set
        .spawn(async { panic!("capacity test panic") });
    assert_eq!(worker.available_capacity(), 0);

    for _ in 0..100 {
        worker.drain_finished_tasks().await;
        if worker.join_set.is_empty() {
            break;
        }
        tokio::task::yield_now().await;
    }

    assert!(worker.join_set.is_empty());
    assert_eq!(worker.available_capacity(), 1);
}

#[tokio::test]
async fn shutdown_drain_waits_for_join_set_tasks() {
    let mut worker = worker_with_capacity(1);
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let task_started = Arc::clone(&started);
    let task_release = Arc::clone(&release);
    worker.join_set.spawn(async move {
        task_started.notify_one();
        task_release.notified().await;
    });
    started.notified().await;

    let mut drain = tokio::spawn(worker.drain(RuntimeLoopExit::Shutdown));
    assert!(
        timeout(Duration::from_millis(25), &mut drain)
            .await
            .is_err(),
        "worker drain returned before its JoinSet task completed"
    );

    release.notify_waiters();
    assert_eq!(
        timeout(Duration::from_secs(1), drain)
            .await
            .expect("worker drain should finish after release")
            .expect("worker drain task should not panic"),
        RuntimeLoopExit::Shutdown
    );
}
