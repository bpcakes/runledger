use uuid::Uuid;

use super::super::identifiers::{StepKey, WorkflowType};
use super::build_validation::validate_step_enqueue;
use super::errors::WorkflowBuildError;
use super::run_builder::WorkflowRunEnqueueBuilder;
use super::step_builder::WorkflowStepEnqueueBuilder;
use super::types::{
    WorkflowDependencyReleaseMode, WorkflowRunEnqueue, WorkflowStepDependencySpec,
    WorkflowStepEnqueue,
};

/// High-level builder for workflow DAG enqueue payloads.
///
/// Compose jobs, external steps, and dependency edges in a fluent chain. Use
/// [`Self::step`] with a validated [`WorkflowStepEnqueueBuilder`] result for
/// per-step organizations, queue settings, continuations, execution resources,
/// or hand-authored dependencies. The lower-level builders remain available.
///
/// This helper accepts raw string identifiers for ergonomics. It validates the
/// workflow shape before enqueueing, but it does not prove at compile time that
/// a job type is registered with storage or with a runtime handler. Use the
/// [`Self::step`] with the low-level step builder for explicit [`StepKey`]
/// and [`JobType`](crate::jobs::JobType) values.
///
/// # Validation Timing
///
/// | Call | Fails immediately | Deferred until [`Self::build`] / [`Self::try_build`] |
/// | --- | --- | --- |
/// | [`Self::new`] | never | blank workflow type |
/// | [`Self::try_new`] | blank workflow type | empty step list and dependency graph errors |
/// | [`Self::job`] | blank step key, blank job type, duplicate step key | job type registration is not checked by this builder |
/// | [`Self::after_success`] / [`Self::after_terminal`] | blank target step key, blank prerequisite step key, unknown target step | missing prerequisite step, self-dependency, duplicate dependency, cycle |
/// | [`Self::step`] / [`Self::external`] | invalid or duplicate step key (configured steps are already shape-validated) | dependency graph errors |
/// | [`Self::idempotency_key`] | never | blank idempotency key |
/// | [`Self::active_key`] | never | blank key or key longer than 512 bytes |
///
/// # Examples
/// ```rust
/// use runledger_core::jobs::WorkflowDagBuilder;
///
/// let metadata = serde_json::json!({"source": "api"});
/// let crawl_payload = serde_json::json!({"profile_id": "p_123"});
/// let classify_payload = serde_json::json!({"profile_id": "p_123"});
///
/// let run = WorkflowDagBuilder::new("profiles.research", &metadata)
///     .idempotency_key("profile:p_123:research")
///     .job("crawl", "profiles.crawl", &crawl_payload)?
///     .job("classify", "profiles.classify", &classify_payload)?
///     .after_success("classify", ["crawl"])?
///     .build()?;
///
/// assert_eq!(run.workflow_type().as_str(), "profiles.research");
/// assert_eq!(run.steps().len(), 2);
/// # Ok::<_, runledger_core::jobs::WorkflowBuildError>(())
/// ```
#[doc(alias = "dag")]
#[doc(alias = "orchestration")]
#[doc(alias = "dependencies")]
#[derive(Debug, Clone)]
pub struct WorkflowDagBuilder<'a> {
    workflow_type: WorkflowType<'a>,
    organization_id: Option<Uuid>,
    metadata: &'a serde_json::Value,
    idempotency_key: Option<&'a str>,
    active_key: Option<&'a str>,
    result_step_key: Option<StepKey<'a>>,
    steps: Vec<WorkflowStepEnqueue<'a>>,
}

impl<'a> WorkflowDagBuilder<'a> {
    /// Creates a new workflow DAG builder with the required fields.
    ///
    /// Blank `workflow_type` values are rejected when [`Self::build`] or
    /// [`Self::try_build`] is called.
    #[must_use]
    pub fn new(workflow_type: &'a str, metadata: &'a serde_json::Value) -> Self {
        Self {
            workflow_type: WorkflowType::new(workflow_type),
            organization_id: None,
            metadata,
            idempotency_key: None,
            active_key: None,
            result_step_key: None,
            steps: Vec::new(),
        }
    }

    /// Creates a new workflow DAG builder with checked workflow-type validation.
    ///
    /// # Errors
    /// Returns [`WorkflowBuildError::BlankWorkflowType`] when `workflow_type` is blank.
    pub fn try_new(
        workflow_type: &'a str,
        metadata: &'a serde_json::Value,
    ) -> Result<Self, WorkflowBuildError> {
        let workflow_type = WorkflowType::try_new(workflow_type)
            .map_err(|_| WorkflowBuildError::BlankWorkflowType)?;
        Ok(Self {
            workflow_type,
            organization_id: None,
            metadata,
            idempotency_key: None,
            active_key: None,
            result_step_key: None,
            steps: Vec::new(),
        })
    }

    /// Sets the workflow organization scope.
    #[must_use]
    pub fn organization_id(mut self, organization_id: Uuid) -> Self {
        self.organization_id = Some(organization_id);
        self
    }

    /// Clears any previously configured workflow organization scope.
    #[must_use]
    pub fn clear_organization_id(mut self) -> Self {
        self.organization_id = None;
        self
    }

    /// Sets a deduplication key for idempotent enqueue behavior.
    #[must_use]
    pub fn idempotency_key(mut self, idempotency_key: &'a str) -> Self {
        self.idempotency_key = Some(idempotency_key);
        self
    }

    /// Clears any previously configured idempotency key.
    #[must_use]
    pub fn clear_idempotency_key(mut self) -> Self {
        self.idempotency_key = None;
        self
    }

    /// Sets a reusable coordination key for one active workflow cycle.
    ///
    /// The key is shared across workflow types in the same organization/global
    /// scope. It must be non-blank and at most 512 bytes, checked at build time.
    /// Use the request with `enqueue_or_get_active_workflow`; this key is
    /// independent of permanent request idempotency.
    #[must_use]
    pub fn active_key(mut self, active_key: &'a str) -> Self {
        self.active_key = Some(active_key);
        self
    }

    /// Clears the reusable active workflow key.
    #[must_use]
    pub fn clear_active_key(mut self) -> Self {
        self.active_key = None;
        self
    }

    /// Declares the step whose successful output becomes the workflow result.
    ///
    /// # Errors
    /// Returns [`WorkflowBuildError::BlankResultStepKey`] when the step key is blank.
    pub fn result_step(mut self, step_key: &'a str) -> Result<Self, WorkflowBuildError> {
        self.result_step_key =
            Some(StepKey::try_new(step_key).map_err(|_| WorkflowBuildError::BlankResultStepKey)?);
        Ok(self)
    }

    /// Clears any previously configured result step.
    #[must_use]
    pub fn clear_result_step(mut self) -> Self {
        self.result_step_key = None;
        self
    }

    /// Adds a job step to the workflow.
    ///
    /// `step_key` and `job_type` are raw string identifiers. This method rejects
    /// blank values and duplicate step keys, but it does not check whether
    /// `job_type` has a registered job definition or runtime handler.
    ///
    /// # Errors
    /// Returns [`WorkflowBuildError`] when the step key or job type is blank, or
    /// when `step_key` was already added.
    pub fn job(
        mut self,
        step_key: &'a str,
        job_type: &'a str,
        payload: &'a serde_json::Value,
    ) -> Result<Self, WorkflowBuildError> {
        self.check_new_step_key(step_key)?;
        let step = WorkflowStepEnqueueBuilder::try_new(step_key, job_type, payload)?.try_build()?;
        self.steps.push(step);
        Ok(self)
    }

    /// Adds a configured job or external step built with [`WorkflowStepEnqueueBuilder`].
    ///
    /// Preserves all step settings and dependencies. More edges can be appended
    /// with [`Self::after_success`] or [`Self::after_terminal`]. Prerequisites
    /// may be added later; the complete graph is validated at build time.
    ///
    /// # Errors
    /// Returns [`WorkflowBuildError::DuplicateStepKey`] if the step already exists.
    ///
    /// # Examples
    /// ```rust
    /// use runledger_core::jobs::{WorkflowDagBuilder, WorkflowStepEnqueueBuilder};
    /// let payload = serde_json::json!({"account": "a"});
    /// let run = WorkflowDagBuilder::new("enrichment", &payload)
    ///     .active_key("enrichment:active")
    ///     .step(WorkflowStepEnqueueBuilder::try_new("account", "enrich", &payload)?
    ///         .allow_handler_continuation()
    ///         .execution_resource("provider")
    ///         .try_build()?)?
    ///     .external("approval", &payload)?
    ///     .after_success("approval", ["account"])?
    ///     .build()?;
    /// assert!(run.steps()[0].allows_handler_continuation());
    /// # Ok::<_, runledger_core::jobs::WorkflowBuildError>(())
    /// ```
    pub fn step(mut self, step: WorkflowStepEnqueue<'a>) -> Result<Self, WorkflowBuildError> {
        self.check_new_step_key(step.step_key().as_str())?;
        self.steps.push(step);
        Ok(self)
    }

    /// Adds a step completed by an external actor, without queued-job settings.
    ///
    /// Use [`Self::step`] to supply a configured external step.
    ///
    /// # Errors
    /// Returns [`WorkflowBuildError`] for a blank or duplicate step key.
    pub fn external(
        self,
        step_key: &'a str,
        payload: &'a serde_json::Value,
    ) -> Result<Self, WorkflowBuildError> {
        self.step(WorkflowStepEnqueueBuilder::try_new_external(step_key, payload)?.try_build()?)
    }

    fn check_new_step_key(&self, step_key: &str) -> Result<(), WorkflowBuildError> {
        let step_key = StepKey::try_new(step_key)
            .map_err(|_| WorkflowBuildError::BlankStepKey { step_index: None })?;
        if self.steps.iter().any(|step| step.step_key() == step_key) {
            return Err(WorkflowBuildError::DuplicateStepKey {
                step_key: step_key.as_str().to_owned(),
            });
        }
        Ok(())
    }

    /// Adds success-only dependencies to an existing step.
    ///
    /// The target `step_key` must already have been added.
    /// Prerequisite step keys may be added later in the chain, but every
    /// prerequisite must exist before [`Self::build`] or [`Self::try_build`]
    /// succeeds.
    ///
    /// # Errors
    /// Returns [`WorkflowBuildError`] when the target or any prerequisite step key
    /// is blank, when the target step has not been added, or when
    /// dependency validation fails at build time.
    pub fn after_success<I>(
        self,
        step_key: &'a str,
        prerequisites: I,
    ) -> Result<Self, WorkflowBuildError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.after(
            step_key,
            prerequisites,
            WorkflowDependencyReleaseMode::OnSuccess,
        )
    }

    /// Adds terminal-state dependencies to an existing step.
    ///
    /// The target `step_key` must already have been added.
    /// Prerequisite step keys may be added later in the chain, but every
    /// prerequisite must exist before [`Self::build`] or [`Self::try_build`]
    /// succeeds.
    ///
    /// # Errors
    /// Returns [`WorkflowBuildError`] when the target or any prerequisite step key
    /// is blank, when the target step has not been added, or when
    /// dependency validation fails at build time.
    pub fn after_terminal<I>(
        self,
        step_key: &'a str,
        prerequisites: I,
    ) -> Result<Self, WorkflowBuildError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.after(
            step_key,
            prerequisites,
            WorkflowDependencyReleaseMode::OnTerminal,
        )
    }

    fn after<I>(
        mut self,
        step_key: &'a str,
        prerequisites: I,
        release_mode: WorkflowDependencyReleaseMode,
    ) -> Result<Self, WorkflowBuildError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let target_step_key = StepKey::try_new(step_key)
            .map_err(|_| WorkflowBuildError::BlankStepKey { step_index: None })?;
        let target_step_key_string = target_step_key.as_str().to_owned();

        let prerequisite_step_keys = prerequisites
            .into_iter()
            .map(|prerequisite_step_key| {
                StepKey::try_new(prerequisite_step_key).map_err(|_| {
                    WorkflowBuildError::BlankDependencyStepKey {
                        step_key: target_step_key_string.clone(),
                    }
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        let step = self
            .steps
            .iter_mut()
            .find(|step| step.step_key() == target_step_key)
            .ok_or(WorkflowBuildError::UnknownStepKey {
                step_key: target_step_key_string,
            })?;

        step.dependencies.extend(
            prerequisite_step_keys
                .into_iter()
                .map(|prerequisite_step_key| WorkflowStepDependencySpec {
                    prerequisite_step_key,
                    release_mode: Some(release_mode),
                }),
        );
        Ok(self)
    }

    /// Finalizes the builder and returns a validated [`WorkflowRunEnqueue`].
    ///
    /// This validates the workflow type, idempotency and active keys, non-empty step list,
    /// per-step enqueue fields, missing prerequisite steps, duplicate
    /// dependencies, self-dependencies, and cycles. It does not check whether job
    /// types have registered storage definitions or runtime handlers.
    ///
    /// # Errors
    /// Returns [`WorkflowBuildError`] if any required field is empty, dependency
    /// keys are invalid, dependencies reference missing steps, or the dependency
    /// graph contains a cycle.
    pub fn build(self) -> Result<WorkflowRunEnqueue<'a>, WorkflowBuildError> {
        self.try_build()
    }

    /// Finalizes the builder and returns a validated [`WorkflowRunEnqueue`].
    ///
    /// This validates the workflow type, idempotency and active keys, non-empty step list,
    /// per-step enqueue fields, missing prerequisite steps, duplicate
    /// dependencies, self-dependencies, and cycles. It does not check whether job
    /// types have registered storage definitions or runtime handlers.
    ///
    /// # Errors
    /// Returns [`WorkflowBuildError`] if any required field is empty, dependency
    /// keys are invalid, dependencies reference missing steps, or the dependency
    /// graph contains a cycle.
    pub fn try_build(self) -> Result<WorkflowRunEnqueue<'a>, WorkflowBuildError> {
        // Preserve per-step error precedence before validating run fields.
        for step in &self.steps {
            validate_step_enqueue(step)?;
        }

        let mut run_builder = WorkflowRunEnqueueBuilder::new(self.workflow_type, self.metadata);
        if let Some(organization_id) = self.organization_id {
            run_builder = run_builder.organization_id(organization_id);
        }
        if let Some(idempotency_key) = self.idempotency_key {
            run_builder = run_builder.idempotency_key(idempotency_key);
        }
        if let Some(active_key) = self.active_key {
            run_builder = run_builder.active_key(active_key);
        }
        if let Some(result_step_key) = self.result_step_key {
            run_builder = run_builder.result_step_key(result_step_key);
        }

        run_builder.extend_steps(self.steps).try_build()
    }
}
