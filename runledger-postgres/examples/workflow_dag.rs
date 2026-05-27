use runledger_core::jobs::{JobType, WorkflowDagBuilder};
use runledger_postgres::DbPool;
use runledger_postgres::jobs::{JobDefinitionUpsert, WorkflowRunDbRecord};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let profile_id = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "p_123".to_owned());

    let pool = DbPool::connect(&database_url).await?;
    runledger_postgres::ensure_schema_compatible_after_idempotency_cutover(&pool).await?;
    ensure_job_definitions(&pool).await?;

    let workflow_run = enqueue_profile_research_workflow(&pool, &profile_id).await?;
    println!(
        "enqueued workflow_run_id={} workflow_type={} status={:?}",
        workflow_run.id, workflow_run.workflow_type, workflow_run.status
    );

    Ok(())
}

async fn ensure_job_definitions(pool: &DbPool) -> Result<(), Box<dyn std::error::Error>> {
    let mut tx = pool.begin().await?;
    for job_type in [
        "profiles.crawl",
        "profiles.classify",
        "profiles.score",
        "profiles.persist",
    ] {
        let definition = JobDefinitionUpsert {
            job_type: JobType::new(job_type),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 300,
            default_priority: 0,
            is_enabled: true,
        };
        runledger_postgres::jobs::upsert_job_definition_tx(&mut tx, &definition).await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn enqueue_profile_research_workflow(
    pool: &DbPool,
    profile_id: &str,
) -> Result<WorkflowRunDbRecord, Box<dyn std::error::Error>> {
    let metadata = json!({
        "source": "workflow_dag_example",
        "profile_id": profile_id,
    });
    let crawl_payload = json!({ "profile_id": profile_id });
    let classify_payload = json!({ "profile_id": profile_id });
    let score_payload = json!({ "profile_id": profile_id });
    let persist_payload = json!({ "profile_id": profile_id });
    let idempotency_key = format!("profile:{profile_id}:research");

    let run = WorkflowDagBuilder::new("profiles.research", &metadata)
        .idempotency_key(&idempotency_key)
        .job("crawl", "profiles.crawl", &crawl_payload)?
        .job("classify", "profiles.classify", &classify_payload)?
        .after_success("classify", ["crawl"])?
        .job("score", "profiles.score", &score_payload)?
        .after_success("score", ["crawl"])?
        .job("persist", "profiles.persist", &persist_payload)?
        .after_success("persist", ["classify", "score"])?
        .build()?;

    let workflow_run = runledger_postgres::jobs::enqueue_workflow_run(pool, &run).await?;
    Ok(workflow_run)
}
