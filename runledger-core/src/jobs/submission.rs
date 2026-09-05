use chrono::{DateTime, Utc};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::Value;
use uuid::Uuid;

use super::{JobSpec, JobSpecError, JobStage, JobType};

/// Associates a durable identity with its payload; no runtime clients are needed.
/// Serde attributes and application decoding policy define the wire format.
pub trait JobContract {
    type Payload: Serialize + DeserializeOwned + Send;

    fn spec() -> JobSpec;

    fn submit(payload: &Self::Payload) -> Result<JobSubmission, JobSubmissionError> {
        let spec = Self::spec();
        spec.require_enabled().map_err(JobSubmissionError::Spec)?;
        Ok(JobSubmission::new(
            spec.job_type(),
            serde_json::to_value(payload).map_err(JobSubmissionError::Serialize)?,
        ))
    }
}

#[derive(Debug)]
pub enum JobSubmissionError {
    Spec(JobSpecError),
    Serialize(serde_json::Error),
}

impl std::fmt::Display for JobSubmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spec(error) => error.fmt(f),
            Self::Serialize(_) => f.write_str("could not serialize job payload"),
        }
    }
}

impl std::error::Error for JobSubmissionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(match self {
            Self::Spec(error) => error,
            Self::Serialize(error) => error,
        })
    }
}

/// Owned direct-job request. Only explicitly selected overrides enter the
/// idempotency snapshot; definition defaults are resolved by storage on insert.
#[derive(Debug, Clone)]
pub struct JobSubmission {
    pub job_type: JobType<'static>,
    pub payload: Value,
    pub organization_id: Option<Uuid>,
    pub priority: Option<i32>,
    pub max_attempts: Option<i32>,
    pub timeout_seconds: Option<i32>,
    pub next_run_at: Option<DateTime<Utc>>,
    pub idempotency_key: Option<String>,
    pub stage: Option<JobStage>,
}

impl JobSubmission {
    #[must_use]
    pub fn new(job_type: JobType<'static>, payload: Value) -> Self {
        Self {
            job_type,
            payload,
            organization_id: None,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: None,
        }
    }

    #[must_use]
    pub fn organization_id(mut self, id: Uuid) -> Self {
        self.organization_id = Some(id);
        self
    }
    #[must_use]
    pub fn priority(mut self, value: i32) -> Self {
        self.priority = Some(value);
        self
    }
    #[must_use]
    pub fn max_attempts(mut self, value: i32) -> Self {
        self.max_attempts = Some(value);
        self
    }
    #[must_use]
    pub fn timeout_seconds(mut self, value: i32) -> Self {
        self.timeout_seconds = Some(value);
        self
    }
    /// Keyed retries must reuse the original timestamp.
    #[must_use]
    pub fn next_run_at(mut self, value: DateTime<Utc>) -> Self {
        self.next_run_at = Some(value);
        self
    }
    #[must_use]
    pub fn idempotency_key(mut self, value: impl Into<String>) -> Self {
        self.idempotency_key = Some(value.into());
        self
    }
    #[must_use]
    pub fn stage(mut self, value: JobStage) -> Self {
        self.stage = Some(value);
        self
    }
}
