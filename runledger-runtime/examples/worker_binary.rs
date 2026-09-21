#[path = "support/database.rs"]
mod database;

use std::time::Duration;

use runledger_core::prelude::*;
use runledger_postgres::prelude::*;
use runledger_runtime::prelude::*;
use serde_json::Value;

struct SendEmail;

async fn close_accounted_pool(_permit: RuntimeShutdownCleanupPermit, pool: &sqlx::PgPool) {
    pool.close().await;
}

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
    let database = database::connect(&database_url).await?;
    let pool = database.pool().clone();

    ensure_schema_compatible_after_idempotency_cutover(&database).await?;

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
    // Graceful time for the loops to stop on their own, then a shorter allowance
    // to abort and join whatever did not. Both share one clock that starts at the
    // first stop request, whichever source raised it.
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
            eprintln!("jobs runtime did not settle cooperatively; leaving the pool open");
            return Err(unsettled.into_failure().into());
        }
    }
    Ok(())
}
