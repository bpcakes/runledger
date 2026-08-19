use std::time::Duration;

use runledger_core::prelude::*;
use runledger_postgres::prelude::*;
use runledger_runtime::prelude::*;
use serde_json::{Value, json};
use sqlx::{postgres::PgPoolOptions, types::Uuid};

const UNUSED_LAZY_POOL_URL: &str = "postgres://postgres:postgres@127.0.0.1:65535/runledger";

struct PreludeHandler;

#[async_trait]
impl JobHandler for PreludeHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.prelude.smoke")
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        Ok(JobCompletion::success())
    }
}

#[tokio::test]
async fn all_preludes_can_be_glob_imported_together() {
    let payload = json!({ "profile_id": "p_123" });
    let _job = JobEnqueue {
        job_type: JobType::new("jobs.prelude.smoke"),
        organization_id: None,
        payload: &payload,
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        next_run_at: None,
        idempotency_key: Some("prelude:smoke"),
        stage: Some(JobStage::Queued),
    };
    let _intent = JobEnqueueIntent::new(
        JobType::new("jobs.prelude.smoke"),
        &payload,
        "prelude:intent",
    )
    .with_stage(JobStage::Queued);

    let metadata = json!({ "source": "prelude_smoke" });
    let first = WorkflowStepEnqueueBuilder::new(
        StepKey::new("first"),
        JobType::new("jobs.prelude.smoke"),
        &payload,
    )
    .try_build()
    .expect("first workflow step should build");
    let second = WorkflowStepEnqueueBuilder::new(
        StepKey::new("second"),
        JobType::new("jobs.prelude.smoke"),
        &payload,
    )
    .depends_on_success(&[StepKey::new("first")])
    .try_build()
    .expect("second workflow step should build");
    let _workflow = WorkflowRunEnqueueBuilder::new(WorkflowType::new("prelude.smoke"), &metadata)
        .extend_steps([first, second])
        .try_build()
        .expect("workflow should build");

    let pool: DbPool = PgPoolOptions::new()
        .connect_lazy(UNUSED_LAZY_POOL_URL)
        .expect("construct lazy pool");
    let lease_identity = JobLeaseIdentity::new(Uuid::nil(), 1, 1, "prelude-smoke");
    heartbeat_job_for_lease(&pool, lease_identity, 0)
        .await
        .expect_err("zero lease duration must be rejected before using the lazy pool");
    let catalog = JobCatalog::new().job("jobs.prelude.smoke", PreludeHandler);

    let supervisor = Supervisor::builder(
        &pool,
        JobsConfig {
            worker_id: "prelude-smoke".to_owned(),
            poll_interval: Duration::from_millis(10),
            claim_batch_size: 1,
            lease_ttl_seconds: 10,
            max_global_concurrency: 1,
            reaper_interval: Duration::from_millis(10),
            schedule_poll_interval: Duration::from_millis(10),
            reaper_retry_delay_ms: 1_000,
        },
    )
    .expect("supervisor builder has runtime")
    .with_catalog(&catalog)
    .disable_worker()
    .disable_scheduler()
    .disable_reaper()
    .build()
    .expect("disabled supervisor should build without acquiring pool");

    supervisor
        .shutdown()
        .await
        .expect("shutdown should succeed");
}
