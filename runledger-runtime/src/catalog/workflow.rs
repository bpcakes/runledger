use runledger_core::jobs::{WorkflowDagBuilder, WorkflowRunEnqueue};
use serde_json::Value;
use uuid::Uuid;

use super::{CatalogError, JobCatalog};

/// Workflow DAG builder that validates job types against a [`JobCatalog`].
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
