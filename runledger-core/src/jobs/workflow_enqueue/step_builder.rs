use uuid::Uuid;

use super::super::identifiers::{JobType, StepKey};
use super::super::status::JobStage;
use super::build_validation::{WorkflowStepEnqueueInput, validate_step_enqueue_input};
use super::errors::WorkflowBuildError;
use super::types::{
    WorkflowDependencyReleaseMode, WorkflowJobStepExecution, WorkflowStepDependencySpec,
    WorkflowStepEnqueue, WorkflowStepExecution, WorkflowStepExecutionKind,
};

/// Builder for [`WorkflowStepEnqueue`].
///
/// Use this builder to configure optional per-step execution settings and
/// dependencies. Use [`Self::depends_on_success`] and
/// [`Self::depends_on_terminal`] to model workflow DAG edges instead of
/// manually polling jobs or enqueueing child jobs from handlers.
///
/// Defaults:
/// - `organization_id`: `None`
/// - `priority`: `None`
/// - `max_attempts`: `None`
/// - `timeout_seconds`: `None`
/// - `stage`: `Some(JobStage::Queued)`
/// - `allow_handler_continuation`: `false`
/// - `execution_resource_key`: `None`
/// - `dependencies`: empty
///
/// # Examples
/// ```rust
/// use runledger_core::jobs::{JobStage, JobType, StepKey, WorkflowStepEnqueueBuilder};
///
/// let payload = serde_json::json!({"profile_id": "p_123"});
/// let step = WorkflowStepEnqueueBuilder::new(StepKey::new("c14"), JobType::new("seller.profile.research.c14.pain_mappings"), &payload)
///     .depends_on_success(&[StepKey::new("c6"), StepKey::new("c7")])
///     .try_build()
///     .expect("step payload should be valid");
///
/// assert_eq!(step.stage(), Some(JobStage::Queued));
/// assert_eq!(step.dependencies().len(), 2);
/// ```
#[doc(alias = "dag")]
#[doc(alias = "dependency")]
#[doc(alias = "dependencies")]
#[derive(Debug, Clone)]
pub struct WorkflowStepEnqueueBuilder<'a> {
    step_key: StepKey<'a>,
    execution_kind: WorkflowStepExecutionKind,
    job_type: Option<JobType<'a>>,
    organization_id: Option<Uuid>,
    payload: &'a serde_json::Value,
    priority: Option<i32>,
    max_attempts: Option<i32>,
    timeout_seconds: Option<i32>,
    stage: Option<JobStage>,
    allow_handler_continuation: bool,
    execution_resource_key: Option<&'a str>,
    dependencies: Vec<WorkflowStepDependencySpec<'a>>,
}

impl<'a> WorkflowStepEnqueueBuilder<'a> {
    /// Creates a new step builder with required fields and default options.
    ///
    /// The default stage is `Some(JobStage::Queued)`.
    ///
    /// # Examples
    /// ```rust
    /// use runledger_core::jobs::{JobStage, JobType, StepKey, WorkflowStepEnqueueBuilder};
    ///
    /// let payload = serde_json::json!({});
    /// let step = WorkflowStepEnqueueBuilder::new(StepKey::new("step.a"), JobType::new("jobs.test.a"), &payload)
    ///     .try_build()
    ///     .expect("step payload should be valid");
    ///
    /// assert_eq!(step.stage(), Some(JobStage::Queued));
    /// ```
    #[must_use]
    pub fn new(
        step_key: StepKey<'a>,
        job_type: JobType<'a>,
        payload: &'a serde_json::Value,
    ) -> Self {
        Self {
            step_key,
            execution_kind: WorkflowStepExecutionKind::Job,
            job_type: Some(job_type),
            organization_id: None,
            payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            stage: Some(JobStage::Queued),
            allow_handler_continuation: false,
            execution_resource_key: None,
            dependencies: Vec::new(),
        }
    }

    /// Creates a new step builder from raw identifier strings with checked validation.
    ///
    /// # Errors
    /// Returns [`WorkflowBuildError`] when `step_key` or `job_type` is blank.
    pub fn try_new(
        step_key: &'a str,
        job_type: &'a str,
        payload: &'a serde_json::Value,
    ) -> Result<Self, WorkflowBuildError> {
        let step_key = StepKey::try_new(step_key)
            .map_err(|_| WorkflowBuildError::BlankStepKey { step_index: None })?;
        let job_type =
            JobType::try_new(job_type).map_err(|_| WorkflowBuildError::BlankStepJobType {
                step_key: step_key.as_str().to_owned(),
            })?;

        Ok(Self::new(step_key, job_type, payload))
    }

    /// Creates a new external step builder with required fields and no queue settings.
    #[must_use]
    pub fn new_external(step_key: StepKey<'a>, payload: &'a serde_json::Value) -> Self {
        Self {
            step_key,
            execution_kind: WorkflowStepExecutionKind::External,
            job_type: None,
            organization_id: None,
            payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            stage: None,
            allow_handler_continuation: false,
            execution_resource_key: None,
            dependencies: Vec::new(),
        }
    }

    /// Sets an execution-organization override for this step.
    #[must_use]
    pub fn organization_id(mut self, organization_id: Uuid) -> Self {
        self.organization_id = Some(organization_id);
        self
    }

    /// Clears any previously configured execution-organization override.
    #[must_use]
    pub fn clear_organization_id(mut self) -> Self {
        self.organization_id = None;
        self
    }

    /// Requires exclusive ownership of one durable resource while this step's
    /// job is leased.
    ///
    /// Keys must be non-blank and at most 512 bytes. Resource scope is global
    /// across workflow and organization boundaries, so callers should
    /// namespace keys when independent tenants must not contend. External
    /// steps cannot own execution resources and are rejected at
    /// [`Self::try_build`].
    #[must_use]
    pub fn execution_resource(mut self, resource_key: &'a str) -> Self {
        self.execution_resource_key = Some(resource_key);
        self
    }

    /// Clears a previously configured execution resource.
    #[must_use]
    pub fn clear_execution_resource(mut self) -> Self {
        self.execution_resource_key = None;
        self
    }

    /// Creates a new external step builder from a raw step key with checked validation.
    ///
    /// # Errors
    /// Returns [`WorkflowBuildError`] when `step_key` is blank.
    pub fn try_new_external(
        step_key: &'a str,
        payload: &'a serde_json::Value,
    ) -> Result<Self, WorkflowBuildError> {
        let step_key = StepKey::try_new(step_key)
            .map_err(|_| WorkflowBuildError::BlankStepKey { step_index: None })?;

        Ok(Self::new_external(step_key, payload))
    }

    /// Sets job priority override for this step.
    ///
    /// # Examples
    /// ```rust
    /// use runledger_core::jobs::{JobType, StepKey, WorkflowStepEnqueueBuilder};
    ///
    /// let payload = serde_json::json!({});
    /// let step = WorkflowStepEnqueueBuilder::new(StepKey::new("step.a"), JobType::new("jobs.test.a"), &payload)
    ///     .priority(10)
    ///     .try_build()
    ///     .expect("step payload should be valid");
    ///
    /// assert_eq!(step.priority(), Some(10));
    /// ```
    #[must_use]
    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Clears any previously configured priority override.
    #[must_use]
    pub fn clear_priority(mut self) -> Self {
        self.priority = None;
        self
    }

    /// Sets max retry attempts override for this step.
    ///
    /// The value must be positive (`> 0`); this is enforced when [`Self::try_build`]
    /// is called.
    ///
    /// # Examples
    /// ```rust
    /// use runledger_core::jobs::{JobType, StepKey, WorkflowStepEnqueueBuilder};
    ///
    /// let payload = serde_json::json!({});
    /// let step = WorkflowStepEnqueueBuilder::new(StepKey::new("step.a"), JobType::new("jobs.test.a"), &payload)
    ///     .max_attempts(5)
    ///     .try_build()
    ///     .expect("step payload should be valid");
    ///
    /// assert_eq!(step.max_attempts(), Some(5));
    /// ```
    #[must_use]
    pub fn max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = Some(max_attempts);
        self
    }

    /// Clears any previously configured max-attempts override.
    #[must_use]
    pub fn clear_max_attempts(mut self) -> Self {
        self.max_attempts = None;
        self
    }

    /// Sets timeout override (in seconds) for this step.
    ///
    /// The value must be positive (`> 0`); this is enforced when [`Self::try_build`]
    /// is called.
    ///
    /// # Examples
    /// ```rust
    /// use runledger_core::jobs::{JobType, StepKey, WorkflowStepEnqueueBuilder};
    ///
    /// let payload = serde_json::json!({});
    /// let step = WorkflowStepEnqueueBuilder::new(StepKey::new("step.a"), JobType::new("jobs.test.a"), &payload)
    ///     .timeout_seconds(300)
    ///     .try_build()
    ///     .expect("step payload should be valid");
    ///
    /// assert_eq!(step.timeout_seconds(), Some(300));
    /// ```
    #[must_use]
    pub fn timeout_seconds(mut self, timeout_seconds: i32) -> Self {
        self.timeout_seconds = Some(timeout_seconds);
        self
    }

    /// Clears any previously configured timeout override.
    #[must_use]
    pub fn clear_timeout_seconds(mut self) -> Self {
        self.timeout_seconds = None;
        self
    }

    /// Sets the initial step stage.
    ///
    /// # Examples
    /// ```rust
    /// use runledger_core::jobs::{JobStage, JobType, StepKey, WorkflowStepEnqueueBuilder};
    ///
    /// let payload = serde_json::json!({});
    /// let step = WorkflowStepEnqueueBuilder::new(StepKey::new("step.a"), JobType::new("jobs.test.a"), &payload)
    ///     .stage(JobStage::Scheduled)
    ///     .try_build()
    ///     .expect("step payload should be valid");
    ///
    /// assert_eq!(step.stage(), Some(JobStage::Scheduled));
    /// ```
    #[must_use]
    pub fn stage(mut self, stage: JobStage) -> Self {
        self.stage = Some(stage);
        self
    }

    /// Clears any previously configured step stage.
    #[must_use]
    pub fn clear_stage(mut self) -> Self {
        self.stage = None;
        self
    }

    /// Explicitly allows this job-backed step to return
    /// [`JobCompletion::continue_after`](crate::jobs::JobCompletion::continue_after).
    ///
    /// Continuation is disabled by default so a handler cannot accidentally
    /// keep a workflow active indefinitely. The opt-in is persisted with the
    /// step; external steps cannot enable it.
    #[must_use]
    pub fn allow_handler_continuation(mut self) -> Self {
        self.allow_handler_continuation = true;
        self
    }

    /// Disables handler continuation after a previous opt-in.
    #[must_use]
    pub fn disallow_handler_continuation(mut self) -> Self {
        self.allow_handler_continuation = false;
        self
    }

    /// Replaces all previously configured dependencies with `dependencies`.
    #[must_use]
    pub fn set_dependencies(
        mut self,
        dependencies: impl IntoIterator<Item = WorkflowStepDependencySpec<'a>>,
    ) -> Self {
        self.dependencies = dependencies.into_iter().collect();
        self
    }

    /// Appends one dependency specification.
    #[must_use]
    pub fn dependency(mut self, dependency: WorkflowStepDependencySpec<'a>) -> Self {
        self.dependencies.push(dependency);
        self
    }

    /// Adds dependencies released when prerequisite steps reach a terminal state.
    ///
    /// Appends to any existing dependencies in call order.
    ///
    /// # Examples
    /// ```rust
    /// use runledger_core::jobs::{StepKey, JobType, WorkflowDependencyReleaseMode, WorkflowStepEnqueueBuilder};
    ///
    /// let payload = serde_json::json!({});
    /// let step = WorkflowStepEnqueueBuilder::new(StepKey::new("step.b"), JobType::new("jobs.test.b"), &payload)
    ///     .depends_on_terminal(&[StepKey::new("step.a")])
    ///     .try_build()
    ///     .expect("step payload should be valid");
    ///
    /// assert_eq!(
    ///     step.dependencies()[0].release_mode,
    ///     Some(WorkflowDependencyReleaseMode::OnTerminal)
    /// );
    /// ```
    #[must_use]
    pub fn depends_on_terminal(self, prerequisite_step_keys: &[StepKey<'a>]) -> Self {
        self.depends_on(
            prerequisite_step_keys,
            WorkflowDependencyReleaseMode::OnTerminal,
        )
    }

    /// Adds dependencies released only when prerequisite steps succeed.
    ///
    /// Appends to any existing dependencies in call order.
    ///
    /// # Examples
    /// ```rust
    /// use runledger_core::jobs::{StepKey, JobType, WorkflowDependencyReleaseMode, WorkflowStepEnqueueBuilder};
    ///
    /// let payload = serde_json::json!({});
    /// let step = WorkflowStepEnqueueBuilder::new(StepKey::new("step.b"), JobType::new("jobs.test.b"), &payload)
    ///     .depends_on_success(&[StepKey::new("step.a")])
    ///     .try_build()
    ///     .expect("step payload should be valid");
    ///
    /// assert_eq!(
    ///     step.dependencies()[0].release_mode,
    ///     Some(WorkflowDependencyReleaseMode::OnSuccess)
    /// );
    /// ```
    #[must_use]
    pub fn depends_on_success(self, prerequisite_step_keys: &[StepKey<'a>]) -> Self {
        self.depends_on(
            prerequisite_step_keys,
            WorkflowDependencyReleaseMode::OnSuccess,
        )
    }

    fn depends_on(
        mut self,
        prerequisite_step_keys: &[StepKey<'a>],
        release_mode: WorkflowDependencyReleaseMode,
    ) -> Self {
        self.dependencies
            .extend(
                prerequisite_step_keys
                    .iter()
                    .copied()
                    .map(|prerequisite_step_key| WorkflowStepDependencySpec {
                        prerequisite_step_key,
                        release_mode: Some(release_mode),
                    }),
            );
        self
    }

    /// Finalizes the builder and returns a validated [`WorkflowStepEnqueue`].
    ///
    /// # Errors
    /// Returns [`WorkflowBuildError`] if required fields are empty or
    /// dependencies are invalid (blank, duplicate, or self-referential).
    ///
    /// # Examples
    /// ```rust
    /// use runledger_core::jobs::{JobType, StepKey, WorkflowStepEnqueueBuilder};
    ///
    /// let payload = serde_json::json!({});
    /// let step = WorkflowStepEnqueueBuilder::new(StepKey::new("step.a"), JobType::new("jobs.test.a"), &payload)
    ///     .try_build()
    ///     .expect("step payload should be valid");
    ///
    /// assert_eq!(step.step_key(), StepKey::new("step.a"));
    /// ```
    pub fn try_build(self) -> Result<WorkflowStepEnqueue<'a>, WorkflowBuildError> {
        let input = WorkflowStepEnqueueInput {
            step_key: self.step_key,
            execution_kind: self.execution_kind,
            job_type: self.job_type,
            organization_id: self.organization_id,
            payload: self.payload,
            priority: self.priority,
            max_attempts: self.max_attempts,
            timeout_seconds: self.timeout_seconds,
            stage: self.stage,
            allow_handler_continuation: self.allow_handler_continuation,
            execution_resource_key: self.execution_resource_key,
            dependencies: self.dependencies,
        };
        validate_step_enqueue_input(&input)?;

        let WorkflowStepEnqueueInput {
            step_key,
            execution_kind,
            job_type,
            organization_id,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            stage,
            allow_handler_continuation,
            execution_resource_key,
            dependencies,
        } = input;
        let execution = match execution_kind {
            WorkflowStepExecutionKind::Job => {
                // Shape validation above guarantees this branch has a job type.
                // Keep the checked fallback so this conversion remains panic-free
                // if that validation contract changes later.
                let Some(job_type) = job_type else {
                    return Err(WorkflowBuildError::BlankStepJobType {
                        step_key: step_key.as_str().to_owned(),
                    });
                };
                WorkflowStepExecution::Job(WorkflowJobStepExecution::new(
                    job_type,
                    priority,
                    max_attempts,
                    timeout_seconds,
                    stage,
                    allow_handler_continuation,
                    execution_resource_key,
                ))
            }
            WorkflowStepExecutionKind::External => WorkflowStepExecution::External,
        };

        Ok(WorkflowStepEnqueue {
            step_key,
            execution,
            organization_id,
            payload,
            dependencies,
        })
    }
}
