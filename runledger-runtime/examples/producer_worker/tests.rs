use super::*;
use runledger_core::jobs::JobStatus;
use runledger_postgres::jobs::{JobReadScope, enqueue_job_tx, get_job_by_id_with_scope};
use runledger_runtime::config::JobsConfig;
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use shared::request;

#[tokio::test]
async fn shared_contract_transaction_and_worker_round_trip() {
    let (pool, database) = setup_ephemeral_pool("producer_worker_example", 5).await;
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("server version");
    eprintln!("producer/worker example PostgreSQL {version}");
    let catalog = JobCatalog::new().handler(PrintGreeting);
    catalog.sync_definitions(&pool).await.expect("definitions");
    let payload = serde_json::to_value(Greeting { name: "Ada".into() }).expect("payload");

    let mut tx = pool.begin().await.expect("transaction");
    let rolled_back = enqueue_job_tx(&mut tx, &request(&payload, "rolled-back"))
        .await
        .expect("enqueue before rollback");
    tx.rollback().await.expect("rollback");
    assert!(
        get_job_by_id_with_scope(&pool, JobReadScope::Global, rolled_back)
            .await
            .expect("read rolled back job")
            .is_none()
    );

    let mut tx = pool.begin().await.expect("transaction");
    let job_id = enqueue_job_tx(&mut tx, &request(&payload, "greeting:1"))
        .await
        .expect("enqueue");
    tx.commit().await.expect("commit");
    let mut tx = pool.begin().await.expect("retry transaction");
    let retry_id = enqueue_job_tx(&mut tx, &request(&payload, "greeting:1"))
        .await
        .expect("idempotent retry");
    tx.commit().await.expect("retry commit");
    assert_eq!(retry_id, job_id);

    let config = JobsConfig {
        worker_id: "example-test-worker".into(),
        poll_interval: Duration::from_millis(25),
        claim_batch_size: 1,
        lease_ttl_seconds: 30,
        max_global_concurrency: 1,
        reaper_interval: Duration::from_secs(1),
        schedule_poll_interval: Duration::from_secs(1),
        reaper_retry_delay_ms: 100,
    };
    let supervisor = Supervisor::builder(&pool, config)
        .expect("builder")
        .with_catalog(&catalog)
        .build()
        .expect("supervisor");
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(supervisor.run_until_shutdown(
        async {
            let _ = stop_rx.await;
        },
        Duration::from_secs(10),
    ));
    let completed = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let job = get_job_by_id_with_scope(&pool, JobReadScope::Global, job_id)
                .await
                .expect("read job")
                .expect("committed job");
            if job.status == JobStatus::Succeeded {
                break job;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await;
    stop_tx.send(()).expect("request shutdown");
    task.await
        .expect("supervisor task")
        .expect("graceful shutdown");
    let job = completed.expect("job completes");
    assert_eq!(job.payload, payload);
    assert_eq!(job.progress_done, Some(1));
    assert_eq!(job.progress_total, Some(1));
    teardown_ephemeral_pool(pool, database).await;
}
