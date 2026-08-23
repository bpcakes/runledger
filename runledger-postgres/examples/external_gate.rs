use runledger_core::prelude::*;
use runledger_postgres::prelude::*;
use serde_json::json;

const PERSIST_JOB: &str = "profiles.persist";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let profile_id = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "p_123".to_owned());

    let pool = DbPool::connect(&database_url).await?;
    ensure_schema_compatible_after_idempotency_cutover(&pool).await?;
    ensure_job_definition(&pool, PERSIST_JOB).await?;

    let workflow_run = enqueue_approval_workflow(&pool, &profile_id).await?;
    let approved_step = complete_external_workflow_step(
        &pool,
        &CompleteExternalWorkflowStepInput {
            workflow_run_id: workflow_run.id,
            organization_id: None,
            step_key: StepKey::new("approval"),
            outcome: ExternalWorkflowStepTerminalOutcome::Succeeded { output: None },
            status_reason: Some("approved by trusted service"),
            last_error_code: None,
            last_error_message: None,
        },
    )
    .await?;

    println!(
        "workflow_run_id={} completed_external_step={} status={:?}",
        workflow_run.id, approved_step.step_key, approved_step.status
    );

    Ok(())
}

async fn ensure_job_definition(
    pool: &DbPool,
    job_type: &'static str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut tx = pool.begin().await?;
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(job_type),
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

async fn enqueue_approval_workflow(
    pool: &DbPool,
    profile_id: &str,
) -> Result<WorkflowRunDbRecord, Box<dyn std::error::Error>> {
    let approval_payload = json!({ "profile_id": profile_id, "gate": "human_approval" });
    let persist_payload = json!({ "profile_id": profile_id });
    let metadata = json!({ "source": "external_gate_example" });
    let request_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let idempotency_key = format!("profile:{profile_id}:approval:{request_suffix}");

    let approval =
        WorkflowStepEnqueueBuilder::new_external(StepKey::new("approval"), &approval_payload)
            .try_build()?;
    let persist = WorkflowStepEnqueueBuilder::new(
        StepKey::new("persist"),
        JobType::new(PERSIST_JOB),
        &persist_payload,
    )
    .depends_on_success(&[StepKey::new("approval")])
    .try_build()?;

    let run = WorkflowRunEnqueueBuilder::new(WorkflowType::new("profiles.approval"), &metadata)
        .idempotency_key(&idempotency_key)
        .extend_steps([approval, persist])
        .try_build()?;

    Ok(enqueue_workflow_run(pool, &run).await?)
}
