use std::borrow::Cow;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{JobFailureKind, JobStage};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobProgress {
    pub stage: JobStage,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub checkpoint: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobFailure {
    pub kind: JobFailureKind,
    pub code: &'static str,
    pub message: Cow<'static, str>,
}

impl JobFailure {
    #[must_use]
    pub fn new(
        kind: JobFailureKind,
        code: &'static str,
        message: impl Into<Cow<'static, str>>,
    ) -> Self {
        Self {
            kind,
            code,
            message: message.into(),
        }
    }

    #[must_use]
    pub fn retryable(code: &'static str, message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(JobFailureKind::Retryable, code, message)
    }

    #[must_use]
    pub fn terminal(code: &'static str, message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(JobFailureKind::Terminal, code, message)
    }

    #[must_use]
    pub fn timeout(code: &'static str, message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(JobFailureKind::Timeout, code, message)
    }

    #[must_use]
    pub fn lease_expired(code: &'static str, message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(JobFailureKind::LeaseExpired, code, message)
    }

    #[must_use]
    pub fn panicked(code: &'static str, message: impl Into<Cow<'static, str>>) -> Self {
        Self::new(JobFailureKind::Panicked, code, message)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobDeadLetterReason {
    FailureKindNonRetryable,
    AttemptsExhausted,
    LeaseExpired,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct JobDeadLetterInfo {
    pub failure: JobFailure,
    pub reason: JobDeadLetterReason,
    pub max_attempts: Option<i32>,
}

impl JobDeadLetterInfo {
    #[must_use]
    pub fn new(
        failure: JobFailure,
        reason: JobDeadLetterReason,
        max_attempts: Option<i32>,
    ) -> Self {
        Self {
            failure,
            reason,
            max_attempts,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobContext {
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: i32,
    pub organization_id: Option<Uuid>,
    pub worker_id: String,
}
