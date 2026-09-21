#[path = "../support/database.rs"]
mod database;

pub mod shared;

use std::time::Duration;

use runledger_core::jobs::{JobCompletion, JobContext, JobFailure, JobType};
use runledger_core::prelude::async_trait;
use runledger_runtime::{
    RuntimeSettlement, RuntimeShutdownBudget, RuntimeShutdownCleanupPermit, RuntimeShutdownSignal,
    Supervisor, catalog::JobCatalog, registry::JobHandler,
};
use serde_json::Value;
use shared::{GREETING_JOB, Greeting};

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
    let database = database::connect(&std::env::var("DATABASE_URL")?).await?;
    let pool = database.pool().clone();
    // For a fresh database. Existing deployments must follow the migration runbook.
    runledger_postgres::migrate_after_idempotency_cutover(&database).await?;
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
    match report.classify() {
        RuntimeSettlement::Clean(clean) => {
            close_accounted_pool(clean.into_cleanup_permit(), &pool).await;
        }
        RuntimeSettlement::StoppedWithFailures(stopped) => {
            let (permit, failure) = stopped.into_parts();
            close_accounted_pool(permit, &pool).await;
            return Err(failure.into());
        }
        RuntimeSettlement::Unsettled(unsettled) => {
            return Err(unsettled.into_failure().into());
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
