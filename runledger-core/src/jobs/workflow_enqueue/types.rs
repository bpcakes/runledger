use serde::{Deserialize, Serialize};
use std::str::FromStr;
use uuid::Uuid;

use super::super::identifiers::{JobType, StepKey, WorkflowType};
use super::super::status::JobStage;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkflowStepExecutionKind {
    Job,
    External,
}

impl WorkflowStepExecutionKind {
    #[must_use]
    pub fn from_db_value(raw_kind: &str) -> Option<Self> {
        match raw_kind {
            "JOB" => Some(Self::Job),
            "EXTERNAL" => Some(Self::External),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_db_value(self) -> &'static str {
        match self {
            Self::Job => "JOB",
            Self::External => "EXTERNAL",
        }
    }
}

impl FromStr for WorkflowStepExecutionKind {
    type Err = ();

    fn from_str(raw_kind: &str) -> Result<Self, Self::Err> {
        Self::from_db_value(raw_kind).ok_or(())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkflowDependencyReleaseMode {
    OnTerminal,
    OnSuccess,
}

impl WorkflowDependencyReleaseMode {
    #[must_use]
    pub fn from_db_value(raw_mode: &str) -> Option<Self> {
        match raw_mode {
            "ON_TERMINAL" => Some(Self::OnTerminal),
            "ON_SUCCESS" => Some(Self::OnSuccess),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_db_value(self) -> &'static str {
        match self {
            Self::OnTerminal => "ON_TERMINAL",
            Self::OnSuccess => "ON_SUCCESS",
        }
    }
}

impl FromStr for WorkflowDependencyReleaseMode {
    type Err = ();

    fn from_str(raw_mode: &str) -> Result<Self, Self::Err> {
        Self::from_db_value(raw_mode).ok_or(())
    }
}

#[derive(Debug, Clone)]
pub struct WorkflowRunEnqueue<'a> {
    pub(super) workflow_type: WorkflowType<'a>,
    pub(super) organization_id: Option<Uuid>,
    pub(super) metadata: &'a serde_json::Value,
    pub(super) idempotency_key: Option<&'a str>,
    pub(super) steps: Vec<WorkflowStepEnqueue<'a>>,
}

impl<'a> WorkflowRunEnqueue<'a> {
    #[must_use]
    pub const fn workflow_type(&self) -> WorkflowType<'a> {
        self.workflow_type
    }

    #[must_use]
    pub const fn organization_id(&self) -> Option<Uuid> {
        self.organization_id
    }

    #[must_use]
    pub const fn metadata(&self) -> &'a serde_json::Value {
        self.metadata
    }

    #[must_use]
    pub const fn idempotency_key(&self) -> Option<&'a str> {
        self.idempotency_key
    }

    #[must_use]
    pub fn steps(&self) -> &[WorkflowStepEnqueue<'a>] {
        &self.steps
    }
}

#[derive(Debug, Clone)]
pub struct WorkflowStepEnqueue<'a> {
    pub(super) step_key: StepKey<'a>,
    pub(super) execution_kind: WorkflowStepExecutionKind,
    pub(super) job_type: Option<JobType<'a>>,
    pub(super) organization_id: Option<Uuid>,
    pub(super) payload: &'a serde_json::Value,
    pub(super) priority: Option<i32>,
    pub(super) max_attempts: Option<i32>,
    pub(super) timeout_seconds: Option<i32>,
    pub(super) stage: Option<JobStage>,
    pub(super) dependencies: Vec<WorkflowStepDependencySpec<'a>>,
}

impl<'a> WorkflowStepEnqueue<'a> {
    #[must_use]
    pub const fn step_key(&self) -> StepKey<'a> {
        self.step_key
    }

    #[must_use]
    pub const fn execution_kind(&self) -> WorkflowStepExecutionKind {
        self.execution_kind
    }

    #[must_use]
    pub const fn job_type(&self) -> Option<JobType<'a>> {
        self.job_type
    }

    #[must_use]
    pub const fn organization_id(&self) -> Option<Uuid> {
        self.organization_id
    }

    #[must_use]
    pub const fn payload(&self) -> &'a serde_json::Value {
        self.payload
    }

    #[must_use]
    pub const fn priority(&self) -> Option<i32> {
        self.priority
    }

    #[must_use]
    pub const fn max_attempts(&self) -> Option<i32> {
        self.max_attempts
    }

    #[must_use]
    pub const fn timeout_seconds(&self) -> Option<i32> {
        self.timeout_seconds
    }

    #[must_use]
    pub const fn stage(&self) -> Option<JobStage> {
        self.stage
    }

    #[must_use]
    pub fn dependencies(&self) -> &[WorkflowStepDependencySpec<'a>] {
        &self.dependencies
    }
}

#[derive(Debug, Clone)]
pub struct WorkflowStepDependencySpec<'a> {
    pub prerequisite_step_key: StepKey<'a>,
    pub release_mode: Option<WorkflowDependencyReleaseMode>,
}
