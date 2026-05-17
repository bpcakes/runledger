use uuid::Uuid;

use super::super::identifiers::WorkflowType;
use super::build_validation::validate_workflow_enqueue;
use super::errors::WorkflowBuildError;
use super::types::{WorkflowRunEnqueue, WorkflowStepEnqueue};

/// Builder for [`WorkflowRunEnqueue`].
///
/// Use this builder to assemble a workflow enqueue payload incrementally.
///
/// Defaults:
/// - `organization_id`: `None`
/// - `idempotency_key`: `None`
/// - `steps`: empty
///
/// # Examples
/// ```rust
/// use runledger_core::jobs::{
///     JobType, StepKey, WorkflowType,
///     WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder,
/// };
///
/// let payload = serde_json::json!({"profile_id": "p_123"});
/// let metadata = serde_json::json!({"source": "api"});
///
/// let step = WorkflowStepEnqueueBuilder::new(
///     StepKey::new("crawl"),
///     JobType::new("seller.profile.research.c2.website_crawler"),
///     &payload,
/// )
/// .try_build()
/// .expect("step payload should be valid");
///
/// let run = WorkflowRunEnqueueBuilder::new(WorkflowType::new("seller.profile.research.initial"), &metadata)
///     .idempotency_key("profile:p_123:initial")
///     .step(step)
///     .try_build()
///     .expect("workflow payload should be valid");
///
/// assert_eq!(run.workflow_type(), WorkflowType::new("seller.profile.research.initial"));
/// assert_eq!(run.steps().len(), 1);
/// ```
#[derive(Debug, Clone)]
pub struct WorkflowRunEnqueueBuilder<'a> {
    workflow_type: WorkflowType<'a>,
    organization_id: Option<Uuid>,
    metadata: &'a serde_json::Value,
    idempotency_key: Option<&'a str>,
    steps: Vec<WorkflowStepEnqueue<'a>>,
}

impl<'a> WorkflowRunEnqueueBuilder<'a> {
    /// Creates a new run builder with the required fields.
    ///
    /// `workflow_type` identifies the workflow definition to enqueue.
    /// `metadata` is persisted with the run for tracing and auditing.
    ///
    /// # Examples
    /// ```rust
    /// use runledger_core::jobs::{JobType, StepKey, WorkflowType, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder};
    ///
    /// let payload = serde_json::json!({});
    /// let metadata = serde_json::json!({"source": "api"});
    /// let step = WorkflowStepEnqueueBuilder::new(StepKey::new("step.a"), JobType::new("jobs.test.a"), &payload)
    ///     .try_build()
    ///     .expect("step payload should be valid");
    ///
    /// let run = WorkflowRunEnqueueBuilder::new(WorkflowType::new("seller.profile.research.initial"), &metadata)
    ///     .step(step)
    ///     .try_build()
    ///     .expect("workflow payload should be valid");
    /// assert_eq!(run.workflow_type(), WorkflowType::new("seller.profile.research.initial"));
    /// ```
    #[must_use]
    pub fn new(workflow_type: WorkflowType<'a>, metadata: &'a serde_json::Value) -> Self {
        Self {
            workflow_type,
            organization_id: None,
            metadata,
            idempotency_key: None,
            steps: Vec::new(),
        }
    }

    /// Creates a new run builder from a raw workflow identifier string with checked validation.
    ///
    /// # Errors
    /// Returns [`WorkflowBuildError`] when `workflow_type` is blank.
    pub fn try_new(
        workflow_type: &'a str,
        metadata: &'a serde_json::Value,
    ) -> Result<Self, WorkflowBuildError> {
        let workflow_type = WorkflowType::try_new(workflow_type)
            .map_err(|_| WorkflowBuildError::BlankWorkflowType)?;
        Ok(Self::new(workflow_type, metadata))
    }

    /// Sets the workflow organization scope.
    ///
    /// # Examples
    /// ```rust
    /// use uuid::Uuid;
    /// use runledger_core::jobs::{JobType, StepKey, WorkflowType, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder};
    ///
    /// let payload = serde_json::json!({});
    /// let metadata = serde_json::json!({});
    /// let step = WorkflowStepEnqueueBuilder::new(StepKey::new("step.a"), JobType::new("jobs.test.a"), &payload)
    ///     .try_build()
    ///     .expect("step payload should be valid");
    /// let run = WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test"), &metadata)
    ///     .organization_id(Uuid::nil())
    ///     .step(step)
    ///     .try_build()
    ///     .expect("workflow payload should be valid");
    ///
    /// assert_eq!(run.organization_id(), Some(Uuid::nil()));
    /// ```
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
    ///
    /// The key must be non-blank after trimming; this is enforced when
    /// [`Self::try_build`] is called.
    ///
    /// # Examples
    /// ```rust
    /// use runledger_core::jobs::{JobType, StepKey, WorkflowType, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder};
    ///
    /// let payload = serde_json::json!({});
    /// let metadata = serde_json::json!({});
    /// let step = WorkflowStepEnqueueBuilder::new(StepKey::new("step.a"), JobType::new("jobs.test.a"), &payload)
    ///     .try_build()
    ///     .expect("step payload should be valid");
    /// let run = WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test"), &metadata)
    ///     .idempotency_key("org:123:workflow.test")
    ///     .step(step)
    ///     .try_build()
    ///     .expect("workflow payload should be valid");
    ///
    /// assert_eq!(run.idempotency_key(), Some("org:123:workflow.test"));
    /// ```
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

    /// Appends a single step to the workflow.
    ///
    /// Step order is preserved and used as provided.
    ///
    /// # Examples
    /// ```rust
    /// use runledger_core::jobs::{JobType, StepKey, WorkflowType, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder};
    ///
    /// let payload = serde_json::json!({});
    /// let metadata = serde_json::json!({});
    /// let step = WorkflowStepEnqueueBuilder::new(StepKey::new("step.a"), JobType::new("jobs.test.a"), &payload)
    ///     .try_build()
    ///     .expect("step payload should be valid");
    ///
    /// let run = WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test"), &metadata)
    ///     .step(step)
    ///     .try_build()
    ///     .expect("workflow payload should be valid");
    ///
    /// assert_eq!(run.steps().len(), 1);
    /// ```
    #[must_use]
    pub fn step(mut self, step: WorkflowStepEnqueue<'a>) -> Self {
        self.steps.push(step);
        self
    }

    /// Replaces all previously configured steps with `steps`.
    ///
    /// This is a replacement setter, not an append operation.
    ///
    /// # Examples
    /// ```rust
    /// use runledger_core::jobs::{JobType, StepKey, WorkflowType, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder};
    ///
    /// let payload = serde_json::json!({});
    /// let metadata = serde_json::json!({});
    ///
    /// let a = WorkflowStepEnqueueBuilder::new(StepKey::new("step.a"), JobType::new("jobs.test.a"), &payload)
    ///     .try_build()
    ///     .expect("step payload should be valid");
    /// let b = WorkflowStepEnqueueBuilder::new(StepKey::new("step.b"), JobType::new("jobs.test.b"), &payload)
    ///     .try_build()
    ///     .expect("step payload should be valid");
    ///
    /// let run = WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test"), &metadata)
    ///     .step(a)
    ///     .set_steps(vec![b])
    ///     .try_build()
    ///     .expect("workflow payload should be valid");
    ///
    /// assert_eq!(run.steps().len(), 1);
    /// assert_eq!(run.steps()[0].step_key(), StepKey::new("step.b"));
    /// ```
    #[must_use]
    pub fn set_steps(mut self, steps: impl IntoIterator<Item = WorkflowStepEnqueue<'a>>) -> Self {
        self.steps = steps.into_iter().collect();
        self
    }

    /// Appends multiple steps to the workflow.
    ///
    /// This is an append operation; existing steps are preserved.
    ///
    /// # Examples
    /// ```rust
    /// use runledger_core::jobs::{JobType, StepKey, WorkflowType, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder};
    ///
    /// let payload = serde_json::json!({});
    /// let metadata = serde_json::json!({});
    /// let a = WorkflowStepEnqueueBuilder::new(StepKey::new("step.a"), JobType::new("jobs.test.a"), &payload)
    ///     .try_build()
    ///     .expect("step payload should be valid");
    /// let b = WorkflowStepEnqueueBuilder::new(StepKey::new("step.b"), JobType::new("jobs.test.b"), &payload)
    ///     .try_build()
    ///     .expect("step payload should be valid");
    ///
    /// let run = WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test"), &metadata)
    ///     .step(a)
    ///     .extend_steps(vec![b])
    ///     .try_build()
    ///     .expect("workflow payload should be valid");
    ///
    /// assert_eq!(run.steps().len(), 2);
    /// ```
    #[must_use]
    pub fn extend_steps(
        mut self,
        steps: impl IntoIterator<Item = WorkflowStepEnqueue<'a>>,
    ) -> Self {
        self.steps.extend(steps);
        self
    }

    /// Finalizes the builder and returns a validated [`WorkflowRunEnqueue`].
    ///
    /// # Errors
    /// Returns [`WorkflowBuildError`] if any required field is empty, dependency
    /// keys are invalid, dependencies reference missing steps, or the dependency
    /// graph contains a cycle.
    ///
    /// # Examples
    /// ```rust
    /// use runledger_core::jobs::{JobType, StepKey, WorkflowType, WorkflowRunEnqueueBuilder, WorkflowStepEnqueueBuilder};
    ///
    /// let payload = serde_json::json!({});
    /// let metadata = serde_json::json!({});
    /// let step = WorkflowStepEnqueueBuilder::new(StepKey::new("step.a"), JobType::new("jobs.test.a"), &payload)
    ///     .try_build()
    ///     .expect("step payload should be valid");
    /// let run = WorkflowRunEnqueueBuilder::new(WorkflowType::new("workflow.test"), &metadata)
    ///     .step(step)
    ///     .try_build()
    ///     .expect("workflow payload should be valid");
    ///
    /// assert_eq!(run.steps().len(), 1);
    /// ```
    pub fn try_build(self) -> Result<WorkflowRunEnqueue<'a>, WorkflowBuildError> {
        let workflow = WorkflowRunEnqueue {
            workflow_type: self.workflow_type,
            organization_id: self.organization_id,
            metadata: self.metadata,
            idempotency_key: self.idempotency_key,
            steps: self.steps,
        };
        validate_workflow_enqueue(&workflow)?;
        Ok(workflow)
    }
}
