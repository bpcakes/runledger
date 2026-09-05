use runledger_core::jobs::JobType;
use runledger_postgres::jobs::JobEnqueue;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const GREETING_JOB: JobType<'static> = JobType::new("jobs.greeting.print");

#[derive(Serialize, Deserialize)]
pub struct Greeting {
    pub name: String,
}

pub fn request<'a>(payload: &'a Value, key: &'a str) -> JobEnqueue<'a> {
    JobEnqueue {
        job_type: GREETING_JOB,
        organization_id: None,
        payload,
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        next_run_at: None,
        idempotency_key: Some(key),
        stage: None,
    }
}
