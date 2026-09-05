use runledger_core::jobs::{WorkflowDagBuilder, WorkflowRunEnqueue, WorkflowStepEnqueue};
use serde_json::Value;
use uuid::Uuid;

use super::{CatalogError, JobCatalog};

/// Workflow DAG builder that validates job types against a [`JobCatalog`].
///
/// # Examples
/// ```rust
/// use runledger_core::jobs::WorkflowStepEnqueueBuilder;
/// use runledger_runtime::catalog::JobCatalog;
/// use uuid::Uuid;
///
/// let catalog = JobCatalog::new();
/// let payload = serde_json::json!({"ticket": "review"});
/// let approval = WorkflowStepEnqueueBuilder::try_new_external("approval", &payload)?
///     .organization_id(Uuid::nil())
///     .try_build()?;
/// let run = catalog.workflow_dag("review", &payload)
///     .active_key("review:active")
///     .step(approval)?
///     .external("receipt", &payload)?
///     .after_success("receipt", ["approval"])?
///     .result_step("receipt")?
///     .build()?;
/// assert_eq!(run.steps().len(), 2);
/// # Ok::<_, runledger_runtime::catalog::CatalogError>(())
/// ```
#[derive(Debug, Clone)]
pub struct CatalogWorkflowDagBuilder<'a, 'catalog> {
    pub(super) catalog: &'catalog JobCatalog,
    pub(super) inner: WorkflowDagBuilder<'a>,
}

impl JobCatalog {
    /// Starts a workflow DAG builder that validates step job types against the catalog.
    #[must_use]
    pub fn workflow_dag<'a>(
        &self,
        workflow_type: &'a str,
        metadata: &'a Value,
    ) -> CatalogWorkflowDagBuilder<'a, '_> {
        CatalogWorkflowDagBuilder {
            catalog: self,
            inner: WorkflowDagBuilder::new(workflow_type, metadata),
        }
    }
}

impl<'a, 'catalog> CatalogWorkflowDagBuilder<'a, 'catalog> {
    /// Sets the organization scope for the workflow run and its steps by default.
    #[must_use]
    pub fn organization_id(mut self, organization_id: Uuid) -> Self {
        self.inner = self.inner.organization_id(organization_id);
        self
    }

    /// Clears the workflow-level organization scope.
    #[must_use]
    pub fn clear_organization_id(mut self) -> Self {
        self.inner = self.inner.clear_organization_id();
        self
    }

    /// Sets the workflow idempotency key.
    #[must_use]
    pub fn idempotency_key(mut self, idempotency_key: &'a str) -> Self {
        self.inner = self.inner.idempotency_key(idempotency_key);
        self
    }

    /// Clears the workflow idempotency key.
    #[must_use]
    pub fn clear_idempotency_key(mut self) -> Self {
        self.inner = self.inner.clear_idempotency_key();
        self
    }

    /// Sets a reusable active-cycle key, independent of request idempotency.
    ///
    /// Shared across workflow types in the same organization/global scope.
    /// Checked for non-blank content and a maximum of 512 bytes at build time.
    /// Use the request with `enqueue_or_get_active_workflow`.
    #[must_use]
    pub fn active_key(mut self, active_key: &'a str) -> Self {
        self.inner = self.inner.active_key(active_key);
        self
    }

    /// Clears the reusable active workflow key.
    #[must_use]
    pub fn clear_active_key(mut self) -> Self {
        self.inner = self.inner.clear_active_key();
        self
    }

    /// Declares the step whose successful output becomes the workflow result.
    ///
    /// # Errors
    /// Returns [`CatalogError::WorkflowBuild`] when the step key is blank.
    pub fn result_step(mut self, step_key: &'a str) -> Result<Self, CatalogError> {
        self.inner = self
            .inner
            .result_step(step_key)
            .map_err(CatalogError::WorkflowBuild)?;
        Ok(self)
    }

    /// Clears the workflow result step.
    #[must_use]
    pub fn clear_result_step(mut self) -> Self {
        self.inner = self.inner.clear_result_step();
        self
    }

    /// Adds a job step after validating `job_type_name` against enabled catalog entries.
    ///
    /// # Errors
    /// Returns [`CatalogError`] when the job type is unknown or disabled, or when
    /// the underlying workflow builder rejects the step.
    pub fn job(
        mut self,
        step_key: &'a str,
        job_type_name: &str,
        payload: &'a Value,
    ) -> Result<Self, CatalogError> {
        let job_type = self
            .catalog
            .require_catalog_enabled_job_type(job_type_name)?;
        self.inner = self
            .inner
            .job(step_key, job_type.as_str(), payload)
            .map_err(CatalogError::WorkflowBuild)?;
        Ok(self)
    }

    /// Adds a configured step, checking job steps against this catalog.
    ///
    /// Use [`JobCatalog::workflow_step`] or the core step builder to configure
    /// step policies, then pass its `try_build()` result here. Even steps from
    /// another catalog must be enabled in this catalog. External steps require
    /// no job registration. All settings and dependencies are preserved.
    ///
    /// # Errors
    /// Returns [`CatalogError`] for unknown/disabled job types or duplicate keys.
    pub fn step(mut self, step: WorkflowStepEnqueue<'a>) -> Result<Self, CatalogError> {
        if let Some(job_type) = step.job_type() {
            self.catalog
                .require_catalog_enabled_job_type(job_type.as_str())?;
        }
        self.inner = self.inner.step(step).map_err(CatalogError::WorkflowBuild)?;
        Ok(self)
    }

    /// Adds a step completed by an external actor; no catalog job is required.
    ///
    /// # Errors
    /// Returns [`CatalogError::WorkflowBuild`] for a blank or duplicate key.
    pub fn external(mut self, step_key: &'a str, payload: &'a Value) -> Result<Self, CatalogError> {
        self.inner = self
            .inner
            .external(step_key, payload)
            .map_err(CatalogError::WorkflowBuild)?;
        Ok(self)
    }

    /// Adds success dependencies to an existing workflow step.
    ///
    /// # Errors
    /// Returns [`CatalogError::WorkflowBuild`] when the underlying workflow
    /// builder rejects the dependency edge.
    pub fn after_success<I>(self, step_key: &'a str, prerequisites: I) -> Result<Self, CatalogError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.inner
            .after_success(step_key, prerequisites)
            .map_err(CatalogError::WorkflowBuild)
            .map(|inner| Self {
                catalog: self.catalog,
                inner,
            })
    }

    /// Adds terminal-state dependencies to an existing workflow step.
    ///
    /// # Errors
    /// Returns [`CatalogError::WorkflowBuild`] when the underlying workflow
    /// builder rejects the dependency edge.
    pub fn after_terminal<I>(
        self,
        step_key: &'a str,
        prerequisites: I,
    ) -> Result<Self, CatalogError>
    where
        I: IntoIterator<Item = &'a str>,
    {
        self.inner
            .after_terminal(step_key, prerequisites)
            .map_err(CatalogError::WorkflowBuild)
            .map(|inner| Self {
                catalog: self.catalog,
                inner,
            })
    }

    /// Builds the workflow enqueue payload.
    ///
    /// This is an alias for [`Self::try_build`].
    ///
    /// # Errors
    /// Returns [`CatalogError::WorkflowBuild`] when final workflow validation fails.
    pub fn build(self) -> Result<WorkflowRunEnqueue<'a>, CatalogError> {
        self.try_build()
    }

    /// Builds the workflow enqueue payload.
    ///
    /// # Errors
    /// Returns [`CatalogError::WorkflowBuild`] when final workflow validation fails.
    pub fn try_build(self) -> Result<WorkflowRunEnqueue<'a>, CatalogError> {
        self.inner.try_build().map_err(CatalogError::WorkflowBuild)
    }
}
