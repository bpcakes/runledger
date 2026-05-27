use std::collections::BTreeMap;
use std::sync::Arc;

use runledger_core::jobs::{JobHandler, JobHandlerRegistry, JobType, JobTypeName};
use runledger_postgres::jobs::JobDefinitionUpsert;

use crate::registry::JobRegistry;

use super::types::CatalogJob;
use super::{CatalogError, JobCatalog, JobCatalogDefaults, JobCatalogSyncScope};

impl JobCatalog {
    /// Creates an empty catalog with default definition values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            defaults: JobCatalogDefaults::default(),
            jobs: BTreeMap::new(),
        }
    }

    /// Replaces the definition defaults used by subsequent sync operations.
    #[must_use]
    pub fn defaults(mut self, defaults: JobCatalogDefaults) -> Self {
        self.defaults = defaults;
        self
    }

    /// Registers a handler after validating declared and handler job types match.
    ///
    /// # Errors
    /// Returns [`CatalogError`] when job types are blank, mismatched, or duplicated.
    pub fn try_job<H>(mut self, job_type: &'static str, handler: H) -> Result<Self, CatalogError>
    where
        H: JobHandler + 'static,
    {
        let declared = Self::validate_declared_job_type(job_type)?;
        let handler_type = Self::validate_handler_job_type(&handler)?;
        if declared != handler_type {
            return Err(CatalogError::HandlerJobTypeMismatch {
                declared: declared.as_str().to_owned(),
                handler: handler_type.as_str().to_owned(),
            });
        }

        let key = JobTypeName::new(job_type).map_err(|source| CatalogError::InvalidJobType {
            job_type: job_type.to_owned(),
            source,
        })?;
        if self.jobs.contains_key(&key) {
            return Err(CatalogError::DuplicateJobType {
                job_type: job_type.to_owned(),
            });
        }

        self.jobs.insert(
            key,
            CatalogJob {
                job_type: declared,
                handler: Arc::new(handler),
                retry_delay_overrides: BTreeMap::new(),
            },
        );
        Ok(self)
    }

    /// Registers a handler, panicking when validation fails.
    #[must_use]
    pub fn job<H>(self, job_type: &'static str, handler: H) -> Self
    where
        H: JobHandler + 'static,
    {
        self.try_job(job_type, handler).unwrap_or_else(|error| {
            panic!("invalid job catalog registration for {job_type:?}: {error}");
        })
    }

    /// Registers a retry-delay override for a catalog job type.
    ///
    /// # Errors
    /// Returns [`CatalogError`] when the job type is unknown or override values are invalid.
    pub fn try_retry_delay_override(
        mut self,
        job_type: &str,
        failure_code: &'static str,
        retry_delay_ms: i32,
    ) -> Result<Self, CatalogError> {
        let key = self.require_job_key(job_type)?;
        Self::validate_failure_code(failure_code)?;
        Self::validate_retry_delay(retry_delay_ms)?;
        self.jobs
            .get_mut(&key)
            .expect("job key validated")
            .retry_delay_overrides
            .insert(failure_code, retry_delay_ms);
        Ok(self)
    }

    /// Registers a retry-delay override, panicking when validation fails.
    #[must_use]
    pub fn retry_delay_override(
        self,
        job_type: &str,
        failure_code: &'static str,
        retry_delay_ms: i32,
    ) -> Self {
        self.try_retry_delay_override(job_type, failure_code, retry_delay_ms)
            .unwrap_or_else(|error| {
                panic!(
                    "invalid retry delay override for job type {job_type:?}, failure code {failure_code:?}: {error}"
                );
            })
    }

    /// Converts the catalog into a runtime [`JobRegistry`].
    ///
    /// Disabled catalog defaults still register handlers so workers can process
    /// already-queued work and dead-letter hooks.
    #[must_use]
    pub fn to_registry(&self) -> JobRegistry {
        let mut registry = JobRegistry::new();
        for entry in self.jobs.values() {
            registry.register_boxed(Arc::clone(&entry.handler));
            for (failure_code, retry_delay_ms) in &entry.retry_delay_overrides {
                registry.register_retry_delay_override(
                    entry.job_type,
                    failure_code,
                    *retry_delay_ms,
                );
            }
        }
        registry
    }

    /// Returns whether the catalog has a registered job type.
    #[must_use]
    pub fn contains(&self, job_type: JobType<'_>) -> bool {
        self.jobs.contains_key(job_type.as_str())
    }

    /// Returns a catalog job type when it is registered.
    ///
    /// # Errors
    /// Returns [`CatalogError::UnknownJobType`] when the name is not in the catalog.
    pub fn require_job_type(&self, job_type: &str) -> Result<JobType<'static>, CatalogError> {
        let key = self.require_job_key(job_type)?;
        Ok(self.jobs.get(&key).expect("job key validated").job_type)
    }

    /// Returns a catalog job type when it is registered and catalog-enabled.
    ///
    /// This checks catalog configuration only. It does not read `job_definitions`;
    /// operator-disabled database rows are enforced later by persistence APIs.
    /// Catalog defaults' enabled flag applies to every catalog entry; per-job
    /// enabled overrides are not modeled yet.
    ///
    /// # Errors
    /// Returns [`CatalogError::UnknownJobType`] or [`CatalogError::DisabledJobType`].
    pub fn require_catalog_enabled_job_type(
        &self,
        job_type: &str,
    ) -> Result<JobType<'static>, CatalogError> {
        let job_type = self.require_job_type(job_type)?;
        if !self.defaults.is_enabled {
            return Err(CatalogError::DisabledJobType {
                job_type: job_type.as_str().to_owned(),
            });
        }
        Ok(job_type)
    }

    pub(super) fn validate_defaults(&self) -> Result<(), CatalogError> {
        if self.defaults.version <= 0 {
            return Err(CatalogError::InvalidDefinitionValue { field: "version" });
        }
        if self.defaults.max_attempts <= 0 {
            return Err(CatalogError::InvalidDefinitionValue {
                field: "max_attempts",
            });
        }
        if self.defaults.default_timeout_seconds <= 0 {
            return Err(CatalogError::InvalidDefinitionValue {
                field: "default_timeout_seconds",
            });
        }
        // default_priority intentionally accepts zero and negative values.
        Ok(())
    }

    pub(super) fn validate_exact_sync_scope(
        &self,
        scope: &JobCatalogSyncScope,
    ) -> Result<(), CatalogError> {
        if self.jobs.is_empty() {
            return Err(CatalogError::EmptyExactSyncCatalog);
        }

        for entry in self.jobs.values() {
            if !scope.contains(entry.job_type) {
                return Err(CatalogError::JobTypeOutsideExactSyncScope {
                    job_type: entry.job_type.as_str().to_owned(),
                });
            }
        }

        Ok(())
    }

    pub(super) fn materialize_definition(
        &self,
        entry: &CatalogJob,
    ) -> JobDefinitionUpsert<'static> {
        JobDefinitionUpsert {
            job_type: entry.job_type,
            version: self.defaults.version,
            max_attempts: self.defaults.max_attempts,
            default_timeout_seconds: self.defaults.default_timeout_seconds,
            default_priority: self.defaults.default_priority,
            is_enabled: self.defaults.is_enabled,
        }
    }

    pub(super) fn require_job_key(&self, job_type: &str) -> Result<JobTypeName, CatalogError> {
        let key = JobTypeName::new(job_type).map_err(|source| CatalogError::InvalidJobType {
            job_type: job_type.to_owned(),
            source,
        })?;
        if self.jobs.contains_key(&key) {
            Ok(key)
        } else {
            Err(CatalogError::UnknownJobType {
                job_type: job_type.to_owned(),
            })
        }
    }

    fn validate_declared_job_type(
        job_type: &'static str,
    ) -> Result<JobType<'static>, CatalogError> {
        JobType::try_new(job_type).map_err(|source| CatalogError::InvalidJobType {
            job_type: job_type.to_owned(),
            source,
        })
    }

    fn validate_handler_job_type<H: JobHandler + ?Sized>(
        handler: &H,
    ) -> Result<JobType<'static>, CatalogError> {
        let handler_job_type = handler.job_type();
        JobType::try_new(handler_job_type.as_str()).map_err(|source| {
            CatalogError::InvalidHandlerJobType {
                handler_job_type: handler_job_type.as_str().to_owned(),
                source,
            }
        })
    }

    fn validate_failure_code(failure_code: &str) -> Result<(), CatalogError> {
        if failure_code.trim().is_empty() {
            Err(CatalogError::InvalidFailureCode)
        } else {
            Ok(())
        }
    }

    fn validate_retry_delay(retry_delay_ms: i32) -> Result<(), CatalogError> {
        if retry_delay_ms <= 0 {
            Err(CatalogError::InvalidRetryDelay)
        } else {
            Ok(())
        }
    }
}

impl Default for JobCatalog {
    fn default() -> Self {
        Self::new()
    }
}
