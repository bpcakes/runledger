use uuid::Uuid;

use super::super::identifiers::{StepKey, WorkflowType};
use super::errors::WorkflowBuildError;
use super::run_builder::WorkflowRunEnqueueBuilder;
use super::step_builder::WorkflowStepEnqueueBuilder;
use super::types::WorkflowRunEnqueue;

#[derive(Debug, Clone)]
struct StepSlot<'a> {
    step_key: StepKey<'a>,
    builder: Option<WorkflowStepEnqueueBuilder<'a>>,
}

/// High-level builder for workflow DAG enqueue payloads.
///
/// Use this builder for common workflows expressed as a fluent chain of jobs and
/// dependency edges. For per-step priority, attempts, timeout, stage, external
/// steps, or hand-authored dependency specs, use
/// [`WorkflowStepEnqueueBuilder`] and [`WorkflowRunEnqueueBuilder`] instead.
///
/// This helper accepts raw string identifiers for ergonomics. It validates the
/// workflow shape before enqueueing, but it does not prove at compile time that
/// a job type is registered with storage or with a runtime handler. Use the
/// lower-level builders when you want call sites to pass explicit [`StepKey`]
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
/// | [`Self::idempotency_key`] | never | blank idempotency key |
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
    result_step_key: Option<StepKey<'a>>,
    steps: Vec<StepSlot<'a>>,
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
        let validated_step_key = StepKey::try_new(step_key)
            .map_err(|_| WorkflowBuildError::BlankStepKey { step_index: None })?;
        if self
            .steps
            .iter()
            .any(|slot| slot.step_key == validated_step_key)
        {
            return Err(WorkflowBuildError::DuplicateStepKey {
                step_key: validated_step_key.as_str().to_owned(),
            });
        }

        let builder = WorkflowStepEnqueueBuilder::try_new(step_key, job_type, payload)?;
        self.steps.push(StepSlot {
            step_key: validated_step_key,
            builder: Some(builder),
        });
        Ok(self)
    }

    /// Adds success-only dependencies to an existing step.
    ///
    /// The target `step_key` must already have been added with [`Self::job`].
    /// Prerequisite step keys may be added later in the chain, but every
    /// prerequisite must exist before [`Self::build`] or [`Self::try_build`]
    /// succeeds.
    ///
    /// # Errors
    /// Returns [`WorkflowBuildError`] when the target or any prerequisite step key
    /// is blank, when the target step was not added with [`Self::job`], or when
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
            WorkflowStepEnqueueBuilder::depends_on_success,
        )
    }

    /// Adds terminal-state dependencies to an existing step.
    ///
    /// The target `step_key` must already have been added with [`Self::job`].
    /// Prerequisite step keys may be added later in the chain, but every
    /// prerequisite must exist before [`Self::build`] or [`Self::try_build`]
    /// succeeds.
    ///
    /// # Errors
    /// Returns [`WorkflowBuildError`] when the target or any prerequisite step key
    /// is blank, when the target step was not added with [`Self::job`], or when
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
            WorkflowStepEnqueueBuilder::depends_on_terminal,
        )
    }

    fn after<I, F>(
        mut self,
        step_key: &'a str,
        prerequisites: I,
        attach: F,
    ) -> Result<Self, WorkflowBuildError>
    where
        I: IntoIterator<Item = &'a str>,
        F: FnOnce(WorkflowStepEnqueueBuilder<'a>, &[StepKey<'a>]) -> WorkflowStepEnqueueBuilder<'a>,
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

        let slot = self
            .steps
            .iter_mut()
            .find(|slot| slot.step_key == target_step_key)
            .ok_or(WorkflowBuildError::UnknownStepKey {
                step_key: target_step_key_string,
            })?;

        let builder = slot
            .builder
            .take()
            .expect("step builder is present until try_build consumes it");
        slot.builder = Some(attach(builder, &prerequisite_step_keys));
        Ok(self)
    }

    /// Finalizes the builder and returns a validated [`WorkflowRunEnqueue`].
    ///
    /// This validates the workflow type, idempotency key, non-empty step list,
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
    /// This validates the workflow type, idempotency key, non-empty step list,
    /// per-step enqueue fields, missing prerequisite steps, duplicate
    /// dependencies, self-dependencies, and cycles. It does not check whether job
    /// types have registered storage definitions or runtime handlers.
    ///
    /// # Errors
    /// Returns [`WorkflowBuildError`] if any required field is empty, dependency
    /// keys are invalid, dependencies reference missing steps, or the dependency
    /// graph contains a cycle.
    pub fn try_build(self) -> Result<WorkflowRunEnqueue<'a>, WorkflowBuildError> {
        let mut built_steps = Vec::with_capacity(self.steps.len());
        for mut slot in self.steps {
            let builder = slot
                .builder
                .take()
                .expect("step builder is present until try_build consumes it");
            built_steps.push(builder.try_build()?);
        }

        let mut run_builder = WorkflowRunEnqueueBuilder::new(self.workflow_type, self.metadata);
        if let Some(organization_id) = self.organization_id {
            run_builder = run_builder.organization_id(organization_id);
        }
        if let Some(idempotency_key) = self.idempotency_key {
            run_builder = run_builder.idempotency_key(idempotency_key);
        }
        if let Some(result_step_key) = self.result_step_key {
            run_builder = run_builder.result_step_key(result_step_key);
        }

        run_builder.extend_steps(built_steps).try_build()
    }
}
