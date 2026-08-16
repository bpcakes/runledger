#![allow(
    dead_code,
    reason = "each integration-test crate uses a different subset of this shared support module"
)]

use runledger_core::jobs::JobType;
use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    JobDefinitionUpsert, JobEnqueue, JobQueueRecord, claim_jobs, enqueue_job,
    upsert_job_definition_tx,
};
use serde_json::Value;
use sqlx::types::Uuid;

pub async fn register_test_job_definition(pool: &DbPool, job_type: &str) {
    let mut tx = pool.begin().await.expect("begin definition transaction");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(job_type),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert definition");
    tx.commit().await.expect("commit definition");
}

pub async fn enqueue_test_job(
    pool: &DbPool,
    job_type: &str,
    organization_id: Option<Uuid>,
    payload: &Value,
) -> Uuid {
    enqueue_job(
        pool,
        &JobEnqueue {
            job_type: JobType::new(job_type),
            organization_id,
            payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: None,
        },
    )
    .await
    .expect("enqueue test job")
}

pub async fn claim_one_job(pool: &DbPool, worker_id: &str) -> JobQueueRecord {
    claim_jobs(pool, worker_id, 30, 1)
        .await
        .expect("claim job")
        .pop()
        .expect("one job should be claimable")
}
