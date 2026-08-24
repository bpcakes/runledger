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

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        Ok(JobCompletion::success())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Copied worker binaries need tracing-subscriber or another subscriber setup.
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPoolOptions::new().connect(&database_url).await?;

    ensure_schema_compatible_after_idempotency_cutover(&pool).await?;

    let catalog = JobCatalog::new().handler(SendEmail);
    // Optional catalog-owned schedules. Register schedules on the builder
    // before calling sync_schedules or schedule_sync_scope. Uncomment the whole
    // shadowing binding so later startup code uses the scheduled catalog.
    // use runledger_runtime::catalog::CatalogJobScheduleSpec;
    // let catalog = catalog.schedule(CatalogJobScheduleSpec {
    //     name: "jobs.email.send.hourly",
    //     job_type: "jobs.email.send",
    //     cron_expr: "0 0 * * * *",
    //     payload_template: &serde_json::json!({}),
    //     is_active: true,
    //     organization_id: None,
    //     max_jitter_seconds: 0,
    //     next_fire_at: None,
    // });

    catalog.sync_definitions(&pool).await?;
    // let scope = catalog.schedule_sync_scope()?;
    // catalog.sync_schedules_exact(&pool, &scope).await?;
    // For additive schedule sync, use:
    // catalog.sync_schedules(&pool).await?;

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
