use chrono::{Duration as ChronoDuration, Utc};
use runledger_core::prelude::*;
use runledger_postgres::prelude::*;
use serde_json::json;

const REFRESH_JOB: &str = "profiles.refresh";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let schedule_name = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "profile-refresh-hourly".to_owned());

    let pool = DbPool::connect(&database_url).await?;
    ensure_schema_compatible_after_idempotency_cutover(&pool).await?;
    ensure_job_definition(&pool).await?;

    let job_type = JobType::new(REFRESH_JOB);
    let payload_template = json!({ "source": "schedule_job_example" });
    let cron_expr = "0 0 * * * *";
    let next_fire_at = Utc::now() + ChronoDuration::minutes(5);

    // Re-running this setup refreshes the schedule definition but preserves
    // the existing row's is_active and organization_id. A cron change stores
    // the next_fire_at cursor supplied here.
    let schedule = upsert_job_schedule(
        &pool,
        &JobScheduleUpsert {
            name: &schedule_name,
            job_type,
            organization_id: None,
            payload_template: &payload_template,
            cron_expr,
            is_active: true,
            next_fire_at,
            max_jitter_seconds: 0,
        },
    )
    .await?;

    println!(
        "schedule_id={} name={} job_type={} next_fire_at={}",
        schedule.id,
        schedule.name,
        schedule.job_type.as_str(),
        schedule.next_fire_at
    );

    Ok(())
}

async fn ensure_job_definition(pool: &DbPool) -> Result<(), Box<dyn std::error::Error>> {
    let mut tx = pool.begin().await?;
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(REFRESH_JOB),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 300,
            default_priority: 0,
            is_enabled: true,
        },
    )
    .await?;
    tx.commit().await?;
    Ok(())
}
