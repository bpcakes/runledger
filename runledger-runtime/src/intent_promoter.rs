use runledger_core::jobs::JobType;
use runledger_postgres::jobs;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::RuntimeLoopExit;
use crate::config::{IntentPromoterConfig, JobsConfig};
use crate::registry::JobRegistry;
use crate::shutdown;

/// Promotes durable enqueue intents for registered job types until shutdown.
///
/// Promotion is deliberately independent from ordinary queue claiming so a
/// slow or contended intent pass cannot delay execution of already-queued
/// jobs. The loop derives its allowlist from `registry`, promotes at most
/// [`JobsConfig::claim_batch_size`] intents per pass. Full passes continue
/// immediately; partial or empty passes wait for either shutdown or
/// [`JobsConfig::poll_interval`].
///
/// [`crate::Supervisor`] starts this loop automatically whenever its worker is
/// enabled. Custom process orchestration that runs [`crate::worker::run_worker_loop`]
/// directly must also run this loop if it uses durable enqueue intents.
pub async fn run_intent_promoter_loop(
    pool: runledger_postgres::DbPool,
    registry: JobRegistry,
    config: JobsConfig,
    shutdown: watch::Receiver<bool>,
) -> RuntimeLoopExit {
    run_intent_promoter_loop_with_config(
        pool,
        registry,
        IntentPromoterConfig::from_jobs_config(&config),
        shutdown,
    )
    .await
}

/// Promotes durable enqueue intents with intent-specific polling controls.
///
/// A full storage batch is followed immediately by another pass so a backlog is
/// not rate-limited by the idle polling cadence. The loop yields between full
/// batches and checks shutdown before each pass.
pub async fn run_intent_promoter_loop_with_config(
    pool: runledger_postgres::DbPool,
    registry: JobRegistry,
    config: IntentPromoterConfig,
    mut shutdown: watch::Receiver<bool>,
) -> RuntimeLoopExit {
    if let Err(error) = config.validate() {
        warn!(%error, "invalid jobs config; stopping intent promoter loop");
        return RuntimeLoopExit::InvalidConfig(error);
    }

    let promotable_job_types = registry.registered_static_types();

    loop {
        if shutdown::is_requested_or_closed(&shutdown) {
            return intent_promoter_shutdown_complete();
        }

        if promotion_pass_should_wait(&pool, &promotable_job_types, config.batch_size()).await {
            if shutdown::wait_for_request_or_timeout(&mut shutdown, config.poll_interval()).await {
                return intent_promoter_shutdown_complete();
            }
        } else {
            tokio::task::yield_now().await;
        }
    }
}

async fn promotion_pass_should_wait(
    pool: &runledger_postgres::DbPool,
    promotable_job_types: &[JobType<'static>],
    limit: i64,
) -> bool {
    if promotable_job_types.is_empty() {
        return true;
    }

    match jobs::promote_job_enqueue_intents_for_types(pool, promotable_job_types, limit).await {
        Ok(report) => !report.batch_was_full(),
        Err(error) => {
            warn!(%error, "job enqueue intent promotion failed");
            true
        }
    }
}

fn intent_promoter_shutdown_complete() -> RuntimeLoopExit {
    info!("intent promoter shutdown complete");
    RuntimeLoopExit::Shutdown
}
