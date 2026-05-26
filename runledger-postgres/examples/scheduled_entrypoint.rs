use chrono::{Duration as ChronoDuration, Utc};
use runledger_core::prelude::*;
use runledger_postgres::prelude::*;
use serde_json::json;
use sqlx::Row;

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
    let payload_template = json!({ "source": "scheduled_entrypoint_example" });
    let cron_expr = "0 0 * * * *";
    let next_fire_at = Utc::now() + ChronoDuration::minutes(5);
    let row = sqlx::query(
        "INSERT INTO job_schedules (
            name,
            job_type,
            payload_template,
            cron_expr,
            timezone,
            is_active,
            next_fire_at,
            max_jitter_seconds
         )
         VALUES ($1, $2, $3::jsonb, $4, 'UTC', true, $5, $6)
         ON CONFLICT (name)
         DO UPDATE
            SET job_type = EXCLUDED.job_type,
                payload_template = EXCLUDED.payload_template,
                cron_expr = EXCLUDED.cron_expr,
                timezone = EXCLUDED.timezone,
                is_active = true,
                next_fire_at = EXCLUDED.next_fire_at,
                max_jitter_seconds = EXCLUDED.max_jitter_seconds,
                updated_at = now()
         RETURNING id",
    )
    .bind(&schedule_name)
    .bind(job_type.as_str())
    .bind(&payload_template)
    .bind(cron_expr)
    .bind(next_fire_at)
    .bind(0_i32)
    .fetch_one(&pool)
    .await?;
    let schedule_id: sqlx::types::Uuid = row.try_get("id")?;

    println!(
        "schedule_id={} name={} job_type={} next_fire_at={}",
        schedule_id,
        schedule_name,
        job_type.as_str(),
        next_fire_at
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
