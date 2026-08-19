use runledger_core::jobs::JobType;
use runledger_postgres::jobs;
use tokio::sync::watch;
use tracing::{info, warn};

use crate::RuntimeLoopExit;
use crate::config::JobsConfig;
use crate::registry::JobRegistry;
use crate::shutdown;

/// Promotes durable enqueue intents for registered job types until shutdown.
///
/// Promotion is deliberately independent from ordinary queue claiming so a
/// slow or contended intent pass cannot delay execution of already-queued
/// jobs. The loop derives its allowlist from `registry`, promotes at most
/// [`JobsConfig::claim_batch_size`] intents per pass, then waits for either
/// shutdown or [`JobsConfig::poll_interval`].
///
/// [`crate::Supervisor`] starts this loop automatically whenever its worker is
/// enabled. Custom process orchestration that runs [`crate::worker::run_worker_loop`]
/// directly must also run this loop if it uses durable enqueue intents.
pub async fn run_intent_promoter_loop(
    pool: runledger_postgres::DbPool,
    registry: JobRegistry,
    config: JobsConfig,
    mut shutdown: watch::Receiver<bool>,
) -> RuntimeLoopExit {
    if let Err(error) = config.validate_intent_promoter_loop() {
        warn!(%error, "invalid jobs config; stopping intent promoter loop");
        return RuntimeLoopExit::InvalidConfig(error);
    }

    let promotable_job_types = registry.registered_static_types();

    loop {
        if shutdown::is_requested_or_closed(&shutdown) {
            return intent_promoter_shutdown_complete();
        }

        promote_intents(&pool, &promotable_job_types, config.claim_batch_size).await;

        if shutdown::wait_for_request_or_timeout(&mut shutdown, config.poll_interval).await {
            return intent_promoter_shutdown_complete();
        }
    }
}

async fn promote_intents(
    pool: &runledger_postgres::DbPool,
    promotable_job_types: &[JobType<'static>],
    limit: i64,
) {
    if promotable_job_types.is_empty() {
        return;
    }

    if let Err(error) =
        jobs::promote_job_enqueue_intents_for_types(pool, promotable_job_types, limit).await
    {
        warn!(%error, "job enqueue intent promotion failed");
    }
}

fn intent_promoter_shutdown_complete() -> RuntimeLoopExit {
    info!("intent promoter shutdown complete");
    RuntimeLoopExit::Shutdown
}
