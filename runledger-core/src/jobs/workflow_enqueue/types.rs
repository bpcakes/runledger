use serde::{Deserialize, Serialize};
use std::fmt;
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

/// Validated execution configuration for a workflow step.
///
/// This copyable view is returned by [`WorkflowStepEnqueue::execution`]. A
/// job step always carries its required job type and may carry queue settings;
/// an external step cannot carry either.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkflowStepExecution<'a> {
    /// A step backed by a queued job.
    Job(WorkflowJobStepExecution<'a>),
    /// A step completed by an external actor.
    External,
}

impl WorkflowStepExecution<'_> {
    /// Returns the durable execution-kind discriminator.
    #[must_use]
    pub const fn kind(self) -> WorkflowStepExecutionKind {
        match self {
            Self::Job(_) => WorkflowStepExecutionKind::Job,
            Self::External => WorkflowStepExecutionKind::External,
        }
    }
}

/// Validated queued-job configuration for a workflow step.
///
/// Instances are exposed through [`WorkflowStepExecution::Job`]. Their fields
/// remain private so the builder validation boundary owns construction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkflowJobStepExecution<'a> {
    job_type: JobType<'a>,
    priority: Option<i32>,
    max_attempts: Option<i32>,
    timeout_seconds: Option<i32>,
    stage: Option<JobStage>,
    allow_handler_continuation: bool,
    execution_resource_key: Option<&'a str>,
}

impl<'a> WorkflowJobStepExecution<'a> {
    pub(super) const fn new(
        job_type: JobType<'a>,
        priority: Option<i32>,
        max_attempts: Option<i32>,
        timeout_seconds: Option<i32>,
        stage: Option<JobStage>,
        allow_handler_continuation: bool,
        execution_resource_key: Option<&'a str>,
    ) -> Self {
        Self {
            job_type,
            priority,
            max_attempts,
            timeout_seconds,
            stage,
            allow_handler_continuation,
            execution_resource_key,
        }
    }

    /// The required queued-job type.
    #[must_use]
    pub const fn job_type(self) -> JobType<'a> {
        self.job_type
    }

    /// Optional queued-job priority override.
    #[must_use]
    pub const fn priority(self) -> Option<i32> {
        self.priority
    }

    /// Optional queued-job retry-attempt override.
    #[must_use]
    pub const fn max_attempts(self) -> Option<i32> {
        self.max_attempts
    }

    /// Optional queued-job timeout override, in seconds.
    #[must_use]
    pub const fn timeout_seconds(self) -> Option<i32> {
        self.timeout_seconds
    }

    /// Optional initial queued-job stage.
    #[must_use]
    pub const fn stage(self) -> Option<JobStage> {
        self.stage
    }

    /// Whether a handler may continue this queued-job step after success.
    #[must_use]
    pub const fn allows_handler_continuation(self) -> bool {
        self.allow_handler_continuation
    }

    /// The optional single-permit resource owned while this job is leased.
    #[must_use]
    pub const fn execution_resource_key(self) -> Option<&'a str> {
        self.execution_resource_key
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
    pub(super) active_key: Option<&'a str>,
    pub(super) result_step_key: Option<StepKey<'a>>,
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
    pub const fn active_key(&self) -> Option<&'a str> {
        self.active_key
    }

    #[must_use]
    pub const fn result_step_key(&self) -> Option<StepKey<'a>> {
        self.result_step_key
    }

    #[must_use]
    pub fn steps(&self) -> &[WorkflowStepEnqueue<'a>] {
        &self.steps
    }
}

#[derive(Clone)]
pub struct WorkflowStepEnqueue<'a> {
    pub(super) step_key: StepKey<'a>,
    pub(super) execution: WorkflowStepExecution<'a>,
    pub(super) organization_id: Option<Uuid>,
    pub(super) payload: &'a serde_json::Value,
    pub(super) dependencies: Vec<WorkflowStepDependencySpec<'a>>,
}

impl fmt::Debug for WorkflowStepEnqueue<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkflowStepEnqueue")
            .field("step_key", &self.step_key)
            .field("execution_kind", &self.execution_kind())
            .field("job_type", &self.job_type())
            .field("organization_id", &self.organization_id)
            .field("payload", &self.payload)
            .field("priority", &self.priority())
            .field("max_attempts", &self.max_attempts())
            .field("timeout_seconds", &self.timeout_seconds())
            .field("stage", &self.stage())
            .field(
                "allow_handler_continuation",
                &self.allows_handler_continuation(),
            )
            .field("execution_resource_key", &self.execution_resource_key())
            .field("dependencies", &self.dependencies)
            .finish()
    }
}

impl<'a> WorkflowStepEnqueue<'a> {
    #[must_use]
    pub const fn step_key(&self) -> StepKey<'a> {
        self.step_key
    }

    #[must_use]
    pub const fn execution_kind(&self) -> WorkflowStepExecutionKind {
        self.execution.kind()
    }

    /// Returns the validated execution configuration for this step.
    #[must_use]
    pub const fn execution(&self) -> WorkflowStepExecution<'a> {
        self.execution
    }

    #[must_use]
    pub const fn job_type(&self) -> Option<JobType<'a>> {
        match self.execution {
            WorkflowStepExecution::Job(execution) => Some(execution.job_type()),
            WorkflowStepExecution::External => None,
        }
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
        match self.execution {
            WorkflowStepExecution::Job(execution) => execution.priority(),
            WorkflowStepExecution::External => None,
        }
    }

    #[must_use]
    pub const fn max_attempts(&self) -> Option<i32> {
        match self.execution {
            WorkflowStepExecution::Job(execution) => execution.max_attempts(),
            WorkflowStepExecution::External => None,
        }
    }

    #[must_use]
    pub const fn timeout_seconds(&self) -> Option<i32> {
        match self.execution {
            WorkflowStepExecution::Job(execution) => execution.timeout_seconds(),
            WorkflowStepExecution::External => None,
        }
    }

    #[must_use]
    pub const fn stage(&self) -> Option<JobStage> {
        match self.execution {
            WorkflowStepExecution::Job(execution) => execution.stage(),
            WorkflowStepExecution::External => None,
        }
    }

    /// Whether this job-backed step may return a successful handler continuation.
    #[must_use]
    pub const fn allows_handler_continuation(&self) -> bool {
        match self.execution {
            WorkflowStepExecution::Job(execution) => execution.allows_handler_continuation(),
            WorkflowStepExecution::External => false,
        }
    }

    /// The single-permit resource this job step must own while leased.
    #[must_use]
    pub const fn execution_resource_key(&self) -> Option<&'a str> {
        match self.execution {
            WorkflowStepExecution::Job(execution) => execution.execution_resource_key(),
            WorkflowStepExecution::External => None,
        }
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
