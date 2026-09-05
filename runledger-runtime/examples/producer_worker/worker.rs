pub mod shared;

use std::time::Duration;

use runledger_core::jobs::{JobCompletion, JobContext, JobFailure, JobType};
use runledger_core::prelude::async_trait;
use runledger_runtime::{Supervisor, catalog::JobCatalog, registry::JobHandler};
use serde_json::Value;
use shared::{GREETING_JOB, Greeting};
use sqlx::postgres::PgPoolOptions;

struct PrintGreeting;

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
    let shutdown_result = supervisor
        .run_until_shutdown(
            async {
                if let Err(error) = tokio::signal::ctrl_c().await {
                    eprintln!("failed to listen for shutdown signal: {error}");
                }
            },
            Duration::from_secs(30),
        )
        .await;
    pool.close().await;
    shutdown_result?;
    Ok(())
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
