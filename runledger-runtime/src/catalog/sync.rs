use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    JobDefinitionCatalogSyncMode, sync_catalog_job_definitions_exact_tx,
    sync_catalog_job_definitions_tx,
};

use super::{
    CatalogError, JobCatalog, JobCatalogExactSyncReport, JobCatalogSyncReport, JobCatalogSyncScope,
};

impl JobCatalog {
    /// Upserts every catalog job into `job_definitions`.
    ///
    /// The catalog owns the synced definition fields: repeated syncs overwrite
    /// `version`, `max_attempts`, `default_timeout_seconds`, and
    /// `default_priority` for registered jobs. Enabled catalogs preserve an
    /// existing disabled row so operator pauses survive worker restarts; disabled
    /// catalogs explicitly write `is_enabled = false`.
    /// Safe to call repeatedly. Does not delete or disable definitions absent from the catalog.
    /// Use [`Self::sync_definitions_exact`] with an explicit scope when removed
    /// catalog entries should be disabled.
    ///
    /// Disabled catalog sync briefly locks `job_schedules` and `job_definitions`
    /// so active schedule checks and definition disables are evaluated against a
    /// stable write boundary.
    ///
    /// # Errors
    /// Returns [`CatalogError`] when defaults are invalid or persistence fails.
    pub async fn sync_definitions(
        &self,
        pool: &DbPool,
    ) -> Result<JobCatalogSyncReport, CatalogError> {
        self.validate_defaults()?;

        let mut tx = pool.begin().await.map_err(|error| {
            CatalogError::SyncFailure(Box::new(
                runledger_postgres::Error::from_query_sqlx_with_context(
                    "begin job catalog definition sync",
                    error,
                ),
            ))
        })?;

        let definitions = self.catalog_definitions();
        let mode = if self.defaults.is_enabled {
            JobDefinitionCatalogSyncMode::PreserveExistingEnabledForEnabledDefinitions
        } else {
            JobDefinitionCatalogSyncMode::RestoreCatalogEnabledState
        };
        let report = sync_catalog_job_definitions_tx(&mut tx, &definitions, mode)
            .await
            .map_err(CatalogError::from_definition_catalog_sync_error)?;

        tx.commit().await.map_err(CatalogError::CommitFailure)?;

        Ok(JobCatalogSyncReport {
            disabled_catalog_job_types: report.disabled_catalog_job_types,
        })
    }

    /// Upserts catalog jobs, then disables enabled `job_definitions` rows in
    /// `scope` whose job type is absent from the catalog.
    ///
    /// This is the stricter startup mode for applications that want the catalog
    /// to be the active job-definition source of truth. It never deletes rows,
    /// never operates outside the supplied owned job-type set, and rejects active
    /// schedules that still reference a job type it would disable. Unlike
    /// [`Self::sync_definitions`], exact sync restores catalog entries'
    /// `is_enabled` value from catalog defaults.
    ///
    /// Exact sync briefly locks `job_schedules` and `job_definitions` before it
    /// checks active schedules or disables definitions, so it is heavier than the
    /// additive sync path.
    ///
    /// # Errors
    /// Returns [`CatalogError`] when the scope is invalid, the catalog is empty,
    /// a catalog job is outside the scope, active schedules still reference
    /// absent scoped jobs, or persistence fails.
    pub async fn sync_definitions_exact(
        &self,
        pool: &DbPool,
        scope: &JobCatalogSyncScope,
    ) -> Result<JobCatalogExactSyncReport, CatalogError> {
        self.validate_defaults()?;
        self.validate_exact_sync_scope(scope)?;

        let mut tx = pool.begin().await.map_err(|error| {
            CatalogError::SyncFailure(Box::new(
                runledger_postgres::Error::from_query_sqlx_with_context(
                    "begin exact job catalog definition sync",
                    error,
                ),
            ))
        })?;

        let scope_job_types = scope.job_types_for_storage();
        let definitions = self.catalog_definitions();
        let report = sync_catalog_job_definitions_exact_tx(&mut tx, &definitions, &scope_job_types)
            .await
            .map_err(CatalogError::from_definition_catalog_sync_error)?;

        tx.commit().await.map_err(CatalogError::CommitFailure)?;

        Ok(JobCatalogExactSyncReport {
            disabled_absent_job_types: report.disabled_absent_job_types,
            disabled_catalog_job_types: report.disabled_catalog_job_types,
        })
    }

    fn catalog_definitions(&self) -> Vec<runledger_postgres::jobs::JobDefinitionUpsert<'static>> {
        self.jobs
            .values()
            .map(|entry| self.materialize_definition(entry))
            .collect()
    }
}
