use runledger_core::prelude::*;
use runledger_postgres::prelude::*;
use serde_json::json;

const ENRICH_JOB: &str = "profiles.enrich";
const PERSIST_JOB: &str = "profiles.persist";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let profile_id = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "p_123".to_owned());

    let pool = DbPool::connect(&database_url).await?;
    ensure_schema_compatible_after_idempotency_cutover(&pool).await?;
    ensure_job_definitions(&pool).await?;

    let workflow_run = enqueue_appendable_workflow(&pool, &profile_id).await?;
    complete_external_workflow_step(
        &pool,
        &CompleteExternalWorkflowStepInput {
            workflow_run_id: workflow_run.id,
            organization_id: None,
            step_key: StepKey::new("seed"),
            terminal_status: WorkflowStepStatus::Succeeded,
            status_reason: Some("seed accepted"),
            last_error_code: None,
            last_error_message: None,
        },
    )
    .await?;

    let enrich_payload = json!({ "profile_id": profile_id });
    let persist_payload = json!({ "profile_id": profile_id });
    let mutation_metadata = json!({
        "source": "append_workflow_steps_example",
        "reason": "late profile enrichment requested",
    });
    let enrich = WorkflowStepEnqueueBuilder::new(
        StepKey::new("enrich"),
        JobType::new(ENRICH_JOB),
        &enrich_payload,
    )
    .depends_on_success(&[StepKey::new("seed")])
    .try_build()?;
    let persist = WorkflowStepEnqueueBuilder::new(
        StepKey::new("persist"),
        JobType::new(PERSIST_JOB),
        &persist_payload,
    )
    .depends_on_success(&[StepKey::new("seed")])
    .try_build()?;

    let append_result = append_workflow_steps(
        &pool,
        &AppendWorkflowStepsInput {
            workflow_run_id: workflow_run.id,
            organization_id: None,
            mutation_key: "append:profile-enrichment:v1",
            mutation_metadata: &mutation_metadata,
            append_window_step_key: StepKey::new("append_window"),
            steps: vec![enrich, persist],
        },
    )
    .await?;

    println!(
        "workflow_run_id={} append_outcome={:?} appended_steps={}",
        append_result.workflow_run.id,
        append_result.outcome,
        append_result.appended_steps.len()
    );

    Ok(())
}

async fn ensure_job_definitions(pool: &DbPool) -> Result<(), Box<dyn std::error::Error>> {
    let mut tx = pool.begin().await?;
    for job_type in [ENRICH_JOB, PERSIST_JOB] {
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
    }
    tx.commit().await?;
    Ok(())
}

async fn enqueue_appendable_workflow(
    pool: &DbPool,
    profile_id: &str,
) -> Result<WorkflowRunDbRecord, Box<dyn std::error::Error>> {
    let seed_payload = json!({ "profile_id": profile_id, "gate": "seed" });
    let append_window_payload = json!({ "profile_id": profile_id, "gate": "append_window" });
    let metadata = json!({ "source": "append_workflow_steps_example" });
    let request_suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let idempotency_key = format!("profile:{profile_id}:append-window:{request_suffix}");

    let seed = WorkflowStepEnqueueBuilder::new_external(StepKey::new("seed"), &seed_payload)
        .try_build()?;
    let append_window = WorkflowStepEnqueueBuilder::new_external(
        StepKey::new("append_window"),
        &append_window_payload,
    )
    .try_build()?;
    let run = WorkflowRunEnqueueBuilder::new(WorkflowType::new("profiles.appendable"), &metadata)
        .idempotency_key(&idempotency_key)
        .extend_steps([seed, append_window])
        .try_build()?;

    Ok(enqueue_workflow_run(pool, &run).await?)
}
