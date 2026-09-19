pub mod shared;

use std::time::Duration;

use runledger_core::jobs::{JobCompletion, JobContext, JobFailure, JobType};
use runledger_core::prelude::async_trait;
use runledger_runtime::{
    RuntimeShutdownBudget, RuntimeShutdownCleanupDecision, RuntimeShutdownCleanupPermit,
    RuntimeShutdownSignal, Supervisor, catalog::JobCatalog, registry::JobHandler,
};
use serde_json::Value;
use shared::{GREETING_JOB, Greeting};
use sqlx::postgres::PgPoolOptions;

struct PrintGreeting;

async fn close_accounted_pool(_permit: RuntimeShutdownCleanupPermit, pool: &sqlx::PgPool) {
    pool.close().await;
}

#[async_trait]
impl JobHandler for PrintGreeting {
    fn job_type(&self) -> JobType<'static> {
        GREETING_JOB
    }

    async fn execute(
        &self,
        _context: JobContext,
        payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        let greeting: Greeting = serde_json::from_value(payload)
            .map_err(|_| JobFailure::terminal("greeting.invalid_payload", "Expected a name."))?;
        println!("Hello, {}!", greeting.name);
        JobCompletion::success().progress(1, 1).map_err(|_| {
            JobFailure::terminal("greeting.invalid_progress", "Invalid completion counts.")
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPoolOptions::new()
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;
    // For a fresh database. Existing deployments must follow the migration runbook.
    runledger_postgres::migrate_after_idempotency_cutover(&pool).await?;
    let catalog = JobCatalog::new().handler(PrintGreeting);
    catalog.sync_definitions(&pool).await?;
    println!("worker ready; producers can now enqueue greetings");

    let supervisor = Supervisor::builder_from_env(&pool)?
        .with_catalog(&catalog)
        .build()?;
    let budget = RuntimeShutdownBudget::new(Duration::from_secs(30), Duration::from_secs(5))?;
    let report = supervisor
        .run_until_shutdown_report(RuntimeShutdownSignal::ctrl_c(), budget)
        .await;
    // The adapter cannot release the pool without the report-derived permit.
    if let RuntimeShutdownCleanupDecision::Allowed(permit) = report.cleanup_decision() {
        close_accounted_pool(permit, &pool).await;
    }
    if let Some(failure) = report.failure() {
        return Err(failure.into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
