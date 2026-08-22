use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStage {
    Queued,
    Scheduled,
    Running,
    Completed,
}

impl JobStage {
    #[must_use]
    pub fn from_db_value(raw_stage: &str) -> Option<Self> {
        match raw_stage {
            "queued" => Some(Self::Queued),
            "scheduled" => Some(Self::Scheduled),
            "running" => Some(Self::Running),
            "completed" => Some(Self::Completed),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_db_value(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Scheduled => "scheduled",
            Self::Running => "running",
            Self::Completed => "completed",
        }
    }
}

impl FromStr for JobStage {
    type Err = ();

    fn from_str(raw_stage: &str) -> Result<Self, Self::Err> {
        Self::from_db_value(raw_stage).ok_or(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobStatus {
    Pending,
    Leased,
    Succeeded,
    DeadLettered,
    Canceled,
}

impl JobStatus {
    #[must_use]
    pub fn from_db_value(raw_status: &str) -> Option<Self> {
        match raw_status {
            "PENDING" => Some(Self::Pending),
            "LEASED" => Some(Self::Leased),
            "SUCCEEDED" => Some(Self::Succeeded),
            "DEAD_LETTERED" => Some(Self::DeadLettered),
            "CANCELED" => Some(Self::Canceled),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_db_value(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Leased => "LEASED",
            Self::Succeeded => "SUCCEEDED",
            Self::DeadLettered => "DEAD_LETTERED",
            Self::Canceled => "CANCELED",
        }
    }
}

impl FromStr for JobStatus {
    type Err = ();

    fn from_str(raw_status: &str) -> Result<Self, Self::Err> {
        Self::from_db_value(raw_status).ok_or(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobEventType {
    Enqueued,
    Leased,
    Heartbeat,
    StageChanged,
    Progress,
    RetryScheduled,
    Succeeded,
    Failed,
    DeadLettered,
    Canceled,
    Requeued,
}

impl JobEventType {
    #[must_use]
    pub fn from_db_value(raw_type: &str) -> Option<Self> {
        match raw_type {
            "ENQUEUED" => Some(Self::Enqueued),
            "LEASED" => Some(Self::Leased),
            "HEARTBEAT" => Some(Self::Heartbeat),
            "STAGE_CHANGED" => Some(Self::StageChanged),
            "PROGRESS" => Some(Self::Progress),
            "RETRY_SCHEDULED" => Some(Self::RetryScheduled),
            "SUCCEEDED" => Some(Self::Succeeded),
            "FAILED" => Some(Self::Failed),
            "DEAD_LETTERED" => Some(Self::DeadLettered),
            "CANCELED" => Some(Self::Canceled),
            "REQUEUED" => Some(Self::Requeued),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_db_value(self) -> &'static str {
        match self {
            Self::Enqueued => "ENQUEUED",
            Self::Leased => "LEASED",
            Self::Heartbeat => "HEARTBEAT",
            Self::StageChanged => "STAGE_CHANGED",
            Self::Progress => "PROGRESS",
            Self::RetryScheduled => "RETRY_SCHEDULED",
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::DeadLettered => "DEAD_LETTERED",
            Self::Canceled => "CANCELED",
            Self::Requeued => "REQUEUED",
        }
    }
}

impl FromStr for JobEventType {
    type Err = ();

    fn from_str(raw_type: &str) -> Result<Self, Self::Err> {
        Self::from_db_value(raw_type).ok_or(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobFailureKind {
    Retryable,
    Terminal,
    Timeout,
    LeaseExpired,
    Panicked,
}

impl JobFailureKind {
    #[must_use]
    pub const fn is_retryable(self) -> bool {
        match self {
            Self::Retryable | Self::Timeout | Self::LeaseExpired => true,
            Self::Terminal | Self::Panicked => false,
        }
    }

    #[must_use]
    pub const fn as_db_value(self) -> &'static str {
        match self {
            Self::Retryable => "RETRYABLE",
            Self::Terminal => "TERMINAL",
            Self::Timeout => "TIMEOUT",
            Self::LeaseExpired => "LEASE_EXPIRED",
            Self::Panicked => "PANICKED",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkflowRunStatus {
    Running,
    WaitingForExternal,
    Succeeded,
    CompletedWithErrors,
    Canceled,
}

impl WorkflowRunStatus {
    #[must_use]
    pub fn from_db_value(raw_status: &str) -> Option<Self> {
        match raw_status {
            "RUNNING" => Some(Self::Running),
            "WAITING_FOR_EXTERNAL" => Some(Self::WaitingForExternal),
            "SUCCEEDED" => Some(Self::Succeeded),
            "COMPLETED_WITH_ERRORS" => Some(Self::CompletedWithErrors),
            "CANCELED" => Some(Self::Canceled),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_db_value(self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::WaitingForExternal => "WAITING_FOR_EXTERNAL",
            Self::Succeeded => "SUCCEEDED",
            Self::CompletedWithErrors => "COMPLETED_WITH_ERRORS",
            Self::Canceled => "CANCELED",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::CompletedWithErrors | Self::Canceled
        )
    }
}

impl FromStr for WorkflowRunStatus {
    type Err = ();

    fn from_str(raw_status: &str) -> Result<Self, Self::Err> {
        Self::from_db_value(raw_status).ok_or(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkflowStepStatus {
    Blocked,
    WaitingForExternal,
    Enqueued,
    Running,
    Succeeded,
    Failed,
    Canceled,
}

impl WorkflowStepStatus {
    #[must_use]
    pub fn from_db_value(raw_status: &str) -> Option<Self> {
        match raw_status {
            "BLOCKED" => Some(Self::Blocked),
            "WAITING_FOR_EXTERNAL" => Some(Self::WaitingForExternal),
            "ENQUEUED" => Some(Self::Enqueued),
            "RUNNING" => Some(Self::Running),
            "SUCCEEDED" => Some(Self::Succeeded),
            "FAILED" => Some(Self::Failed),
            "CANCELED" => Some(Self::Canceled),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_db_value(self) -> &'static str {
        match self {
            Self::Blocked => "BLOCKED",
            Self::WaitingForExternal => "WAITING_FOR_EXTERNAL",
            Self::Enqueued => "ENQUEUED",
            Self::Running => "RUNNING",
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::Canceled => "CANCELED",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Succeeded | Self::Failed | Self::Canceled)
    }
}

impl FromStr for WorkflowStepStatus {
    type Err = ();

    fn from_str(raw_status: &str) -> Result<Self, Self::Err> {
        Self::from_db_value(raw_status).ok_or(())
    }
}

#[cfg(test)]
mod tests {
    use super::{JobFailureKind, JobStage, WorkflowRunStatus, WorkflowStepStatus};
    use proptest::prelude::*;

    #[test]
    fn parse_job_stage_from_str_rejects_invalid_value() {
        assert!("NOT_A_REAL_STAGE".parse::<JobStage>().is_err());
    }

    #[test]
    fn parse_workflow_run_status_from_str_rejects_invalid_value() {
        assert!("NOT_A_REAL_STATUS".parse::<WorkflowRunStatus>().is_err());
    }

    #[test]
    fn parse_workflow_step_status_from_str_rejects_invalid_value() {
        assert!("NOT_A_REAL_STATUS".parse::<WorkflowStepStatus>().is_err());
    }

    #[test]
    fn job_failure_kind_retry_eligibility_is_exhaustive() {
        for (kind, expected) in [
            (JobFailureKind::Retryable, true),
            (JobFailureKind::Terminal, false),
            (JobFailureKind::Timeout, true),
            (JobFailureKind::LeaseExpired, true),
            (JobFailureKind::Panicked, false),
        ] {
            assert_eq!(
                kind.is_retryable(),
                expected,
                "unexpected policy for {kind:?}"
            );
        }
    }

    proptest! {
        #[test]
        fn job_stage_roundtrips_db_value(
            stage in prop_oneof![
                Just(JobStage::Queued),
                Just(JobStage::Scheduled),
                Just(JobStage::Running),
                Just(JobStage::Completed),
            ]
        ) {
            let raw = stage.as_db_value();

            prop_assert_eq!(JobStage::from_db_value(raw), Some(stage));
            prop_assert_eq!(raw.parse::<JobStage>().ok(), Some(stage));
        }

        #[test]
        fn workflow_step_status_roundtrips_db_value(
            status in prop_oneof![
                Just(WorkflowStepStatus::Blocked),
                Just(WorkflowStepStatus::WaitingForExternal),
                Just(WorkflowStepStatus::Enqueued),
                Just(WorkflowStepStatus::Running),
                Just(WorkflowStepStatus::Succeeded),
                Just(WorkflowStepStatus::Failed),
                Just(WorkflowStepStatus::Canceled),
            ]
        ) {
            let raw = status.as_db_value();

            prop_assert_eq!(WorkflowStepStatus::from_db_value(raw), Some(status));
            prop_assert_eq!(raw.parse::<WorkflowStepStatus>().ok(), Some(status));
        }

        #[test]
        fn workflow_run_status_roundtrips_db_value(
            status in prop_oneof![
                Just(WorkflowRunStatus::Running),
                Just(WorkflowRunStatus::WaitingForExternal),
                Just(WorkflowRunStatus::Succeeded),
                Just(WorkflowRunStatus::CompletedWithErrors),
                Just(WorkflowRunStatus::Canceled),
            ]
        ) {
            let raw = status.as_db_value();

            prop_assert_eq!(WorkflowRunStatus::from_db_value(raw), Some(status));
            prop_assert_eq!(raw.parse::<WorkflowRunStatus>().ok(), Some(status));
        }
    }
}
