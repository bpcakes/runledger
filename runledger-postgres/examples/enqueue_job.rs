use runledger_core::prelude::*;
use runledger_postgres::prelude::*;
use serde_json::json;

const SEND_EMAIL_JOB: &str = "jobs.email.send";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let email_id = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "email_123".to_owned());

    let pool = DbPool::connect(&database_url).await?;
    ensure_schema_compatible_after_idempotency_cutover(&pool).await?;
    ensure_job_definition(&pool).await?;

    let payload = json!({ "email_id": email_id });
    let idempotency_key = format!("email:{email_id}:send");
    let job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new(SEND_EMAIL_JOB),
            organization_id: None,
            payload: &payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: Some(&idempotency_key),
            stage: None,
        },
    )
    .await?;

    println!("enqueued job_id={job_id} job_type={SEND_EMAIL_JOB}");
    // This application enqueues global jobs, so it selects exact global visibility.
    if let Some(job) = get_job_by_id_with_scope(&pool, JobReadScope::Global, job_id).await? {
        println!("job status={:?}", job.status);
    }

    Ok(())
}

async fn ensure_job_definition(pool: &DbPool) -> Result<(), Box<dyn std::error::Error>> {
    let mut tx = pool.begin().await?;
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(SEND_EMAIL_JOB),
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
