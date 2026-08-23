use std::collections::BTreeMap;
use std::sync::Arc;

use runledger_core::jobs::{JobHandler, JobHandlerRegistry, JobType, JobTypeName};
use runledger_postgres::jobs::JobDefinitionUpsert;

use crate::registry::JobRegistry;

use super::schedule_spec::{CatalogJobScheduleSpec, StoredCatalogJobScheduleSpec};
use super::types::CatalogJob;
use super::{
    CatalogError, JobCatalog, JobCatalogDefaults, JobCatalogDefinitionOverrides,
    JobCatalogSyncScope,
};

impl JobCatalog {
    /// Creates an empty catalog with default definition values.
    #[must_use]
    pub fn new() -> Self {
        Self {
            defaults: JobCatalogDefaults::default(),
            jobs: BTreeMap::new(),
            schedules: Vec::new(),
        }
    }

    /// Replaces the definition defaults used by subsequent sync operations.
    #[must_use]
    pub fn defaults(mut self, defaults: JobCatalogDefaults) -> Self {
        self.defaults = defaults;
        self
    }

    /// Registers a handler using the job type returned by the handler.
    ///
    /// # Errors
    /// Returns [`CatalogError`] when the handler job type is invalid or duplicated.
    pub fn try_handler<H>(self, handler: H) -> Result<Self, CatalogError>
    where
        H: JobHandler + 'static,
    {
        let handler_type = Self::validate_handler_job_type(&handler)?;
        self.insert_handler(handler_type, Arc::new(handler))
    }

    /// Registers a handler using the job type returned by the handler, panicking
    /// when validation fails.
    #[must_use]
    pub fn handler<H>(self, handler: H) -> Self
    where
        H: JobHandler + 'static,
    {
        self.try_handler(handler).unwrap_or_else(|error| {
            panic!("invalid job catalog handler registration: {error}");
        })
    }

    /// Registers a handler after validating that the declared job type matches
    /// the handler-provided identity.
    ///
    /// New code should use [`Self::try_handler`], which has no parallel declared
    /// identity.
    ///
    /// # Errors
    /// Returns [`CatalogError`] when job types are blank, mismatched, or duplicated.
    #[deprecated(
        since = "0.11.0",
        note = "use JobCatalog::try_handler(handler); the handler now supplies the catalog identity"
    )]
    pub fn try_job<H>(self, job_type: &'static str, handler: H) -> Result<Self, CatalogError>
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

        self.insert_handler(handler_type, Arc::new(handler))
    }

    /// Registers a handler after validating a redundant declared job type,
    /// panicking when validation fails.
    ///
    /// New code should use [`Self::handler`], which has no parallel declared
    /// identity.
    #[must_use]
    #[deprecated(
        since = "0.11.0",
        note = "use JobCatalog::handler(handler); the handler now supplies the catalog identity"
    )]
    pub fn job<H>(self, job_type: &'static str, handler: H) -> Self
    where
        H: JobHandler + 'static,
    {
        #[allow(
            deprecated,
            reason = "compatibility wrapper delegates to legacy validation"
        )]
        self.try_job(job_type, handler).unwrap_or_else(|error| {
            panic!("invalid job catalog registration for {job_type:?}: {error}");
        })
    }

    /// Registers a handler with job-specific definition overrides, using the
    /// job type returned by the handler.
    ///
    /// # Errors
    /// Returns [`CatalogError`] when the handler job type or overrides are
    /// invalid, or when the job type is duplicated.
    pub fn try_handler_with_definition_overrides<H>(
        self,
        handler: H,
        overrides: JobCatalogDefinitionOverrides,
    ) -> Result<Self, CatalogError>
    where
        H: JobHandler + 'static,
    {
        let job_type = Self::validate_handler_job_type(&handler)?;
        self.insert_handler(job_type, Arc::new(handler))?
            .try_definition_overrides(job_type.as_str(), overrides)
    }

    /// Registers a handler with job-specific definition overrides using the
    /// handler-provided identity, panicking when validation fails.
    #[must_use]
    pub fn handler_with_definition_overrides<H>(
        self,
        handler: H,
        overrides: JobCatalogDefinitionOverrides,
    ) -> Self
    where
        H: JobHandler + 'static,
    {
        self.try_handler_with_definition_overrides(handler, overrides)
            .unwrap_or_else(|error| {
                panic!(
                    "invalid job catalog handler registration with definition overrides: {error}"
                );
            })
    }

    /// Registers a handler with job-specific definition overrides after
    /// validating a redundant declared job type.
    ///
    /// New code should use [`Self::try_handler_with_definition_overrides`],
    /// which has no parallel declared identity.
    ///
    /// # Errors
    /// Returns [`CatalogError`] when job types are blank, mismatched, or
    /// duplicated, or when the overrides are invalid.
    #[deprecated(
        since = "0.11.0",
        note = "use JobCatalog::try_handler_with_definition_overrides(handler, overrides); the handler now supplies the catalog identity"
    )]
    pub fn try_job_with_definition_overrides<H>(
        self,
        job_type: &'static str,
        handler: H,
        overrides: JobCatalogDefinitionOverrides,
    ) -> Result<Self, CatalogError>
    where
        H: JobHandler + 'static,
    {
        #[allow(
            deprecated,
            reason = "compatibility wrapper preserves mismatch diagnostics"
        )]
        self.try_job(job_type, handler)?
            .try_definition_overrides(job_type, overrides)
    }

    /// Registers a handler with job-specific definition overrides after
    /// validating a redundant declared job type, panicking when validation
    /// fails.
    ///
    /// New code should use [`Self::handler_with_definition_overrides`], which
    /// has no parallel declared identity.
    #[must_use]
    #[deprecated(
        since = "0.11.0",
        note = "use JobCatalog::handler_with_definition_overrides(handler, overrides); the handler now supplies the catalog identity"
    )]
    pub fn job_with_definition_overrides<H>(
        self,
        job_type: &'static str,
        handler: H,
        overrides: JobCatalogDefinitionOverrides,
    ) -> Self
    where
        H: JobHandler + 'static,
    {
        #[allow(deprecated, reason = "compatibility wrapper delegates to fallible legacy API")]
        self.try_job_with_definition_overrides(job_type, handler, overrides)
            .unwrap_or_else(|error| {
                panic!(
                    "invalid job catalog registration with definition overrides for {job_type:?}: {error}"
                );
            })
    }

    /// Replaces the definition overrides for one registered catalog job.
    ///
    /// # Errors
    /// Returns [`CatalogError::UnknownJobType`] when the job type is not registered.
    pub fn try_definition_overrides(
        mut self,
        job_type: &str,
        overrides: JobCatalogDefinitionOverrides,
    ) -> Result<Self, CatalogError> {
        let key = self.require_job_key(job_type)?;
        overrides
            .validate()
            .map_err(|field| CatalogError::InvalidJobDefinitionValue {
                job_type: job_type.to_owned(),
                field,
            })?;
        self.jobs
            .get_mut(&key)
            .expect("job key validated")
            .definition_overrides = overrides;
        Ok(self)
    }

    /// Replaces the definition overrides for one registered catalog job,
    /// panicking when validation fails.
    #[must_use]
    pub fn definition_overrides(
        self,
        job_type: &str,
        overrides: JobCatalogDefinitionOverrides,
    ) -> Self {
        self.try_definition_overrides(job_type, overrides)
            .unwrap_or_else(|error| {
                panic!("invalid definition overrides for job type {job_type:?}: {error}");
            })
    }

    /// Registers a policy retry-delay override for a catalog job type.
    ///
    /// A lower bound attached directly to a handler's
    /// [`runledger_core::jobs::JobFailure`] may extend this delay but cannot
    /// shorten it.
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

    /// Registers a policy retry-delay override, panicking when validation
    /// fails.
    ///
    /// A lower bound attached directly to a handler's
    /// [`runledger_core::jobs::JobFailure`] may extend this delay but cannot
    /// shorten it.
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

    /// Registers a catalog-owned schedule after validating its shape and job type.
    ///
    /// Registered schedules are used by [`Self::sync_schedules`] and
    /// [`Self::sync_schedules_exact`]. The referenced job must already be
    /// registered on this builder and effectively enabled.
    ///
    /// # Errors
    /// Returns [`CatalogError`] when the schedule spec is invalid, the job type
    /// is unknown or disabled, or the schedule name is already registered.
    pub fn try_schedule(mut self, spec: CatalogJobScheduleSpec<'_>) -> Result<Self, CatalogError> {
        spec.validate_shape()
            .map_err(|field| CatalogError::InvalidScheduleSpec {
                name: spec.name.to_owned(),
                field,
            })?;
        self.require_catalog_enabled_job_type(spec.job_type)?;

        if self.schedules.iter().any(|stored| stored.name == spec.name) {
            return Err(CatalogError::DuplicateScheduleName {
                name: spec.name.to_owned(),
            });
        }

        self.schedules
            .push(StoredCatalogJobScheduleSpec::from(&spec));
        Ok(self)
    }

    /// Registers a catalog-owned schedule, panicking when validation fails.
    ///
    /// Use [`Self::try_schedule`] when registration data is not static or should
    /// be reported as a recoverable startup error.
    #[must_use]
    pub fn schedule(self, spec: CatalogJobScheduleSpec<'_>) -> Self {
        self.try_schedule(spec).unwrap_or_else(|error| {
            panic!("invalid job catalog schedule registration: {error}");
        })
    }

    /// Converts the catalog into a runtime [`JobRegistry`].
    ///
    /// Disabled catalog jobs still register handlers so workers can process
    /// already-queued work and dead-letter hooks.
    #[must_use]
    pub fn to_registry(&self) -> JobRegistry {
        let mut registry = JobRegistry::new();
        for entry in self.jobs.values() {
            registry.register_boxed(Arc::clone(&entry.handler));
            for (failure_code, retry_delay_ms) in &entry.retry_delay_overrides {
                registry.register_retry_delay_override(
                    entry.job_type(),
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
        Ok(self.jobs.get(&key).expect("job key validated").job_type())
    }

    /// Returns a catalog job type when it is registered and catalog-enabled.
    ///
    /// This checks catalog configuration only. It does not read `job_definitions`;
    /// operator-disabled database rows are enforced later by persistence APIs.
    /// Job-specific definition overrides take precedence over the catalog
    /// default enabled flag when present.
    ///
    /// # Errors
    /// Returns [`CatalogError::UnknownJobType`] or [`CatalogError::DisabledJobType`].
    pub fn require_catalog_enabled_job_type(
        &self,
        job_type: &str,
    ) -> Result<JobType<'static>, CatalogError> {
        let key = self.require_job_key(job_type)?;
        let entry = self.jobs.get(&key).expect("job key validated");
        if !self.effective_defaults(entry).is_enabled {
            return Err(CatalogError::DisabledJobType {
                job_type: entry.job_type().as_str().to_owned(),
            });
        }
        Ok(entry.job_type())
    }

    pub(super) fn validate_defaults(&self) -> Result<(), CatalogError> {
        self.defaults
            .validate()
            .map_err(|field| CatalogError::InvalidDefinitionValue { field })?;

        for entry in self.jobs.values() {
            self.effective_defaults(entry).validate().map_err(|field| {
                CatalogError::InvalidJobDefinitionValue {
                    job_type: entry.job_type().as_str().to_owned(),
                    field,
                }
            })?;
        }

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
            if !scope.contains(entry.job_type()) {
                return Err(CatalogError::JobTypeOutsideExactSyncScope {
                    job_type: entry.job_type().as_str().to_owned(),
                });
            }
        }

        Ok(())
    }

    pub(super) fn materialize_definition(
        &self,
        entry: &CatalogJob,
    ) -> JobDefinitionUpsert<'static> {
        let defaults = self.effective_defaults(entry);
        JobDefinitionUpsert {
            job_type: entry.job_type(),
            version: defaults.version,
            max_attempts: defaults.max_attempts,
            default_timeout_seconds: defaults.default_timeout_seconds,
            default_priority: defaults.default_priority,
            is_enabled: defaults.is_enabled,
        }
    }

    pub(super) fn effective_defaults(&self, entry: &CatalogJob) -> JobCatalogDefaults {
        entry.definition_overrides.apply_to(self.defaults)
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

    fn insert_handler(
        mut self,
        job_type: JobType<'static>,
        handler: Arc<dyn JobHandler>,
    ) -> Result<Self, CatalogError> {
        let key = JobTypeName::new(job_type.as_str()).map_err(|source| {
            CatalogError::InvalidHandlerJobType {
                handler_job_type: job_type.as_str().to_owned(),
                source,
            }
        })?;
        if self.jobs.contains_key(&key) {
            return Err(CatalogError::DuplicateJobType {
                job_type: job_type.as_str().to_owned(),
            });
        }

        self.jobs.insert(
            key,
            CatalogJob {
                handler,
                definition_overrides: JobCatalogDefinitionOverrides::new(),
                retry_delay_overrides: BTreeMap::new(),
            },
        );
        Ok(self)
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
