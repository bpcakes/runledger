use super::*;
use runledger_core::jobs::JobStatus;
use runledger_postgres::jobs::{JobReadScope, get_job_by_id_with_scope};
use runledger_postgres::{PgAtomicError, run_atomic};
use runledger_runtime::config::JobsConfig;
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use shared::request;

#[tokio::test]
async fn shared_contract_transaction_and_worker_round_trip() {
    let (pool, database) = setup_ephemeral_pool("producer_worker_example", 5).await;
    let options = pool.connect_options();
    let login = options.get_username();
    let profile = runledger_postgres::PgSessionProfile::new(
        login,
        login,
        vec!["public".into()],
        Duration::ZERO,
        Duration::ZERO,
    )
    .expect("test policy");
    let profiled = runledger_postgres::RunledgerDatabase::connect((*options).clone(), profile, 1)
        .await
        .expect("test database");
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("server version");
    eprintln!("producer/worker example PostgreSQL {version}");
    let catalog = JobCatalog::new().handler(PrintGreeting);
    catalog.sync_definitions(&pool).await.expect("definitions");
    let payload = serde_json::to_value(Greeting { name: "Ada".into() }).expect("payload");

    let rejected = run_atomic(&profiled, async |scope| {
        scope
            .queue()
            .enqueue_job(&request(&payload, "rolled-back"))
            .await
            .expect("enqueue before rejection");
        Err::<(), _>("reject")
    })
    .await;
    assert!(matches!(rejected, Err(PgAtomicError::Rejected("reject"))));
    let rolled_back: i64 =
        sqlx::query_scalar("SELECT count(*) FROM job_queue WHERE idempotency_key='rolled-back'")
            .fetch_one(&pool)
            .await
            .expect("read rolled back job");
    assert_eq!(rolled_back, 0);

    let outcome = run_atomic(&profiled, async |scope| {
        scope
            .queue()
            .enqueue_job(&request(&payload, "greeting:1"))
            .await
    })
    .await
    .expect("committed enqueue");
    let job_id = outcome.job_id;
    let retry = run_atomic(&profiled, async |scope| {
        scope
            .queue()
            .enqueue_job(&request(&payload, "greeting:1"))
            .await
    })
    .await
    .expect("committed retry");
    assert_eq!(retry.job_id, job_id);

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
    let budget = RuntimeShutdownBudget::new(Duration::from_secs(10), Duration::from_secs(1))
        .expect("valid shutdown budget");
    let task = tokio::spawn(supervisor.run_until_shutdown_report(
        RuntimeShutdownSignal::infallible(async move {
            let _ = stop_rx.await;
        }),
        budget,
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
    let report = task.await.expect("supervisor task");
    assert!(
        matches!(report.classify(), RuntimeSettlement::Clean(_)),
        "graceful shutdown"
    );
    let job = completed.expect("job completes");
    assert_eq!(job.payload, payload);
    assert_eq!(job.progress_done, Some(1));
    assert_eq!(job.progress_total, Some(1));
    profiled.pool().close().await;
    teardown_ephemeral_pool(pool, database).await;
}
