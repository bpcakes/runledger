use std::time::Duration;

use runledger_core::prelude::*;
use runledger_postgres::prelude::*;
use runledger_runtime::prelude::*;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;

struct SendEmail;

#[async_trait]
impl JobHandler for SendEmail {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.email.send")
    }

    async fn execute(&self, _context: JobContext, _payload: Value) -> Result<(), JobFailure> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new().connect(&database_url).await?;

    ensure_schema_compatible_after_idempotency_cutover(&pool).await?;

    let mut registry = JobRegistry::new();
    registry.register(SendEmail);

    let supervisor = Supervisor::builder(&pool, JobsConfig::from_env())?
        .with_registry(registry)
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
