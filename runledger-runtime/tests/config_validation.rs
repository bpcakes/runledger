use std::future::Future;
use std::time::Duration;

use runledger_runtime::config::{
    IntentPromoterConfig, JOBS_CLAIM_BATCH_SIZE_MAX, JobsConfig, JobsConfigValidationError,
};
use runledger_runtime::intent_promoter::{
    run_intent_promoter_loop, run_intent_promoter_loop_with_config,
};
use runledger_runtime::reaper::run_reaper_loop;
use runledger_runtime::registry::JobRegistry;
use runledger_runtime::scheduler::run_scheduler_loop;
use runledger_runtime::worker::run_worker_loop;
use runledger_runtime::{RuntimeError, RuntimeLoopExit, Supervisor};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::watch;
use tokio::time::timeout;

// Invalid config guards must return before any database work; this URL should
// never be contacted by these tests.
const UNUSED_LAZY_POOL_URL: &str = "postgres://postgres:postgres@127.0.0.1:65535/runledger";

fn lazy_pool() -> runledger_postgres::DbPool {
    PgPoolOptions::new()
        .connect_lazy(UNUSED_LAZY_POOL_URL)
        .expect("construct lazy pool")
}

fn test_config() -> JobsConfig {
    JobsConfig {
        worker_id: "runtime-loop-validation-test-worker".to_string(),
        poll_interval: Duration::from_millis(25),
        claim_batch_size: 4,
        lease_ttl_seconds: 10,
        max_global_concurrency: 4,
        reaper_interval: Duration::from_millis(25),
        schedule_poll_interval: Duration::from_millis(25),
        reaper_retry_delay_ms: 1_000,
    }
}

async fn assert_invalid_config_quickly(
    future: impl Future<Output = RuntimeLoopExit>,
    expected: JobsConfigValidationError,
) {
    let exit = timeout(Duration::from_secs(1), future)
        .await
        .expect("invalid config should return before polling or using the database");
    assert_eq!(exit, RuntimeLoopExit::InvalidConfig(expected));
}

async fn assert_shutdown_quickly(future: impl Future<Output = RuntimeLoopExit>) {
    let exit = timeout(Duration::from_secs(1), future)
        .await
        .expect("pre-shutdown loop should return before polling or using the database");
    assert_eq!(exit, RuntimeLoopExit::Shutdown);
}

fn assert_supervisor_build_rejects_config(config: JobsConfig, expected: JobsConfigValidationError) {
    let pool = lazy_pool();
    let result = Supervisor::builder(&pool, config)
        .expect("supervisor builder has runtime")
        .disable_worker()
        .disable_scheduler()
        .disable_reaper()
        .build();

    match result {
        Err(RuntimeError::InvalidJobsConfig { source }) => assert_eq!(source, expected),
        Err(other) => panic!("expected invalid config error, got {other:?}"),
        Ok(_) => panic!("invalid config should prevent supervisor build"),
    }
}

#[tokio::test]
async fn supervisor_build_rejects_any_invalid_config_field() {
    let cases = [
        {
            let mut config = test_config();
            config.worker_id = "  ".to_string();
            (config, JobsConfigValidationError::EmptyWorkerId)
        },
        {
            let mut config = test_config();
            config.poll_interval = Duration::ZERO;
            (config, JobsConfigValidationError::ZeroPollInterval)
        },
        {
            let mut config = test_config();
            config.claim_batch_size = 0;
            (
                config,
                JobsConfigValidationError::InvalidClaimBatchSize { actual: 0 },
            )
        },
        {
            let mut config = test_config();
            config.claim_batch_size = JOBS_CLAIM_BATCH_SIZE_MAX + 1;
            (
                config,
                JobsConfigValidationError::InvalidClaimBatchSize {
                    actual: JOBS_CLAIM_BATCH_SIZE_MAX + 1,
                },
            )
        },
        {
            let mut config = test_config();
            config.lease_ttl_seconds = 0;
            (
                config,
                JobsConfigValidationError::InvalidLeaseTtlSeconds { actual: 0 },
            )
        },
        {
            let mut config = test_config();
            config.max_global_concurrency = 0;
            (
                config,
                JobsConfigValidationError::InvalidMaxGlobalConcurrency,
            )
        },
        {
            let mut config = test_config();
            config.reaper_interval = Duration::ZERO;
            (config, JobsConfigValidationError::ZeroReaperInterval)
        },
        {
            let mut config = test_config();
            config.schedule_poll_interval = Duration::ZERO;
            (config, JobsConfigValidationError::ZeroSchedulePollInterval)
        },
        {
            let mut config = test_config();
            config.reaper_retry_delay_ms = 0;
            (
                config,
                JobsConfigValidationError::InvalidReaperRetryDelayMs { actual: 0 },
            )
        },
    ];

    for (config, expected) in cases {
        assert_supervisor_build_rejects_config(config, expected);
    }
}

#[tokio::test]
async fn worker_loop_rejects_zero_global_concurrency() {
    let mut config = test_config();
    config.max_global_concurrency = 0;
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    assert_invalid_config_quickly(
        run_worker_loop(lazy_pool(), JobRegistry::new(), config, shutdown_rx),
        JobsConfigValidationError::InvalidMaxGlobalConcurrency,
    )
    .await;
}

#[tokio::test]
async fn worker_loop_does_not_reject_scheduler_or_reaper_only_fields() {
    let mut config = test_config();
    config.reaper_interval = Duration::ZERO;
    config.schedule_poll_interval = Duration::ZERO;
    config.reaper_retry_delay_ms = 0;
    let (_shutdown_tx, shutdown_rx) = watch::channel(true);

    assert_shutdown_quickly(run_worker_loop(
        lazy_pool(),
        JobRegistry::new(),
        config,
        shutdown_rx,
    ))
    .await;
}

#[tokio::test]
async fn worker_loop_rejects_nonpositive_lease_ttl() {
    let mut config = test_config();
    config.lease_ttl_seconds = 0;
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    assert_invalid_config_quickly(
        run_worker_loop(lazy_pool(), JobRegistry::new(), config, shutdown_rx),
        JobsConfigValidationError::InvalidLeaseTtlSeconds { actual: 0 },
    )
    .await;
}

#[tokio::test]
async fn intent_promoter_rejects_zero_poll_interval() {
    let mut config = test_config();
    config.poll_interval = Duration::ZERO;
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    assert_invalid_config_quickly(
        run_intent_promoter_loop(lazy_pool(), JobRegistry::new(), config, shutdown_rx),
        JobsConfigValidationError::ZeroPollInterval,
    )
    .await;
}

#[tokio::test]
async fn independently_configured_intent_promoter_rejects_invalid_values() {
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    assert_invalid_config_quickly(
        run_intent_promoter_loop_with_config(
            lazy_pool(),
            JobRegistry::new(),
            IntentPromoterConfig::new(Duration::from_millis(1), 0),
            shutdown_rx,
        ),
        JobsConfigValidationError::InvalidClaimBatchSize { actual: 0 },
    )
    .await;
}

#[tokio::test]
async fn supervisor_rejects_invalid_independent_intent_promoter_config() {
    let pool = lazy_pool();
    let result = Supervisor::builder(&pool, test_config())
        .expect("supervisor builder has runtime")
        .with_registry(JobRegistry::new())
        .with_intent_promoter_config(IntentPromoterConfig::new(Duration::ZERO, 1))
        .disable_scheduler()
        .disable_reaper()
        .build();

    match result {
        Err(RuntimeError::InvalidJobsConfig { source }) => {
            assert_eq!(source, JobsConfigValidationError::ZeroPollInterval);
        }
        Err(other) => panic!("expected invalid config error, got {other:?}"),
        Ok(_) => panic!("invalid intent promoter config should prevent supervisor build"),
    }
}

#[tokio::test]
async fn intent_promoter_does_not_reject_execution_or_other_loop_fields() {
    let mut config = test_config();
    config.worker_id = "  ".to_string();
    config.lease_ttl_seconds = 0;
    config.max_global_concurrency = 0;
    config.reaper_interval = Duration::ZERO;
    config.schedule_poll_interval = Duration::ZERO;
    config.reaper_retry_delay_ms = 0;
    let (_shutdown_tx, shutdown_rx) = watch::channel(true);

    assert_shutdown_quickly(run_intent_promoter_loop(
        lazy_pool(),
        JobRegistry::new(),
        config,
        shutdown_rx,
    ))
    .await;
}

#[tokio::test]
async fn scheduler_loop_rejects_zero_claim_batch_size() {
    let mut config = test_config();
    config.claim_batch_size = 0;
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    assert_invalid_config_quickly(
        run_scheduler_loop(lazy_pool(), config, shutdown_rx),
        JobsConfigValidationError::InvalidClaimBatchSize { actual: 0 },
    )
    .await;
}

#[tokio::test]
async fn runtime_loops_reject_oversized_claim_batch_size() {
    let mut config = test_config();
    config.claim_batch_size = JOBS_CLAIM_BATCH_SIZE_MAX + 1;
    let expected = JobsConfigValidationError::InvalidClaimBatchSize {
        actual: JOBS_CLAIM_BATCH_SIZE_MAX + 1,
    };

    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    assert_invalid_config_quickly(
        run_worker_loop(lazy_pool(), JobRegistry::new(), config.clone(), shutdown_rx),
        expected,
    )
    .await;

    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    assert_invalid_config_quickly(
        run_intent_promoter_loop(lazy_pool(), JobRegistry::new(), config.clone(), shutdown_rx),
        expected,
    )
    .await;

    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    assert_invalid_config_quickly(
        run_scheduler_loop(lazy_pool(), config.clone(), shutdown_rx),
        expected,
    )
    .await;

    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    assert_invalid_config_quickly(
        run_reaper_loop(lazy_pool(), JobRegistry::new(), config, shutdown_rx),
        expected,
    )
    .await;
}

#[tokio::test]
async fn scheduler_loop_does_not_reject_worker_or_reaper_only_fields() {
    let mut config = test_config();
    config.worker_id = "  ".to_string();
    config.poll_interval = Duration::ZERO;
    config.lease_ttl_seconds = 0;
    config.max_global_concurrency = 0;
    config.reaper_interval = Duration::ZERO;
    config.reaper_retry_delay_ms = 0;
    let (_shutdown_tx, shutdown_rx) = watch::channel(true);

    assert_shutdown_quickly(run_scheduler_loop(lazy_pool(), config, shutdown_rx)).await;
}

#[tokio::test]
async fn reaper_loop_rejects_zero_retry_delay() {
    let mut config = test_config();
    config.reaper_retry_delay_ms = 0;
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);

    assert_invalid_config_quickly(
        run_reaper_loop(lazy_pool(), JobRegistry::new(), config, shutdown_rx),
        JobsConfigValidationError::InvalidReaperRetryDelayMs { actual: 0 },
    )
    .await;
}

#[tokio::test]
async fn reaper_loop_does_not_reject_worker_or_scheduler_only_fields() {
    let mut config = test_config();
    config.worker_id = "  ".to_string();
    config.poll_interval = Duration::ZERO;
    config.lease_ttl_seconds = 0;
    config.max_global_concurrency = 0;
    config.schedule_poll_interval = Duration::ZERO;
    let (_shutdown_tx, shutdown_rx) = watch::channel(true);

    assert_shutdown_quickly(run_reaper_loop(
        lazy_pool(),
        JobRegistry::new(),
        config,
        shutdown_rx,
    ))
    .await;
}
