use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use runledger_core::jobs::{JobHandler, JobType, JobTypeName};

use super::CatalogError;

/// Default values applied when syncing catalog jobs to `job_definitions`.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobCatalogDefaults {
    /// Definition version written for catalog jobs.
    pub version: i32,
    /// Default maximum attempts written for catalog jobs.
    pub max_attempts: i32,
    /// Default execution timeout, in seconds, written for catalog jobs.
    pub default_timeout_seconds: i32,
    /// Default queue priority written for catalog jobs.
    pub default_priority: i32,
    /// Whether catalog jobs should be synced as enabled.
    ///
    /// This flag applies to every job in the catalog. Use separate catalogs or
    /// lower-level definition APIs if different job types need different enabled
    /// states at startup.
    pub is_enabled: bool,
}

impl Default for JobCatalogDefaults {
    fn default() -> Self {
        Self {
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 300,
            default_priority: 0,
            is_enabled: true,
        }
    }
}

impl JobCatalogDefaults {
    /// Creates the default catalog definition values.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the definition version written for catalog jobs.
    #[must_use]
    pub fn version(mut self, version: i32) -> Self {
        self.version = version;
        self
    }

    /// Sets the default maximum attempts written for catalog jobs.
    #[must_use]
    pub fn max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Sets the default execution timeout, in seconds, written for catalog jobs.
    #[must_use]
    pub fn timeout_seconds(mut self, default_timeout_seconds: i32) -> Self {
        self.default_timeout_seconds = default_timeout_seconds;
        self
    }

    /// Sets the default queue priority written for catalog jobs.
    #[must_use]
    pub fn priority(mut self, default_priority: i32) -> Self {
        self.default_priority = default_priority;
        self
    }

    /// Sets whether catalog jobs should be synced as enabled.
    #[must_use]
    pub fn enabled(mut self, is_enabled: bool) -> Self {
        self.is_enabled = is_enabled;
        self
    }
}

/// Single-source registry of job handlers, definition defaults, and enqueue helpers.
#[derive(Debug, Clone)]
pub struct JobCatalog {
    pub(super) defaults: JobCatalogDefaults,
    pub(super) jobs: BTreeMap<JobTypeName, CatalogJob>,
}

#[derive(Clone)]
pub(super) struct CatalogJob {
    pub(super) job_type: JobType<'static>,
    pub(super) handler: Arc<dyn JobHandler>,
    pub(super) retry_delay_overrides: BTreeMap<&'static str, i32>,
}

impl fmt::Debug for CatalogJob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogJob")
            .field("job_type", &self.job_type)
            .field("retry_delay_overrides", &self.retry_delay_overrides)
            .finish_non_exhaustive()
    }
}

/// Explicit owned job-type scope used by [`JobCatalog::sync_definitions_exact`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobCatalogSyncScope {
    pub(super) job_types: BTreeSet<JobTypeName>,
}

/// Result returned by [`JobCatalog::sync_definitions`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobCatalogSyncReport {
    /// Catalog job types that this additive sync forced disabled through catalog
    /// defaults. Already-disabled catalog rows are not included.
    pub disabled_catalog_job_types: Vec<JobTypeName>,
}

/// Result returned by [`JobCatalog::sync_definitions_exact`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobCatalogExactSyncReport {
    /// Enabled definitions inside the exact-sync scope that were absent from the
    /// catalog and changed to disabled by this sync.
    pub disabled_absent_job_types: Vec<JobTypeName>,
    /// Catalog job types that this sync forced disabled through catalog
    /// defaults, including both newly inserted disabled rows and existing
    /// enabled rows changed to disabled. Already-disabled catalog rows are not
    /// included.
    pub disabled_catalog_job_types: Vec<JobTypeName>,
}

impl JobCatalogSyncScope {
    /// Builds a scope containing one owned job type.
    ///
    /// # Errors
    /// Returns [`CatalogError::InvalidExactSyncScopeJobType`] when `job_type` is invalid.
    pub fn job_type(job_type: impl Into<String>) -> Result<Self, CatalogError> {
        Self::job_types([job_type])
    }

    /// Builds a scope containing one or more owned job types.
    ///
    /// # Errors
    /// Returns [`CatalogError::InvalidExactSyncScope`] when no job type is
    /// provided, or [`CatalogError::InvalidExactSyncScopeJobType`] when any job
    /// type is invalid.
    pub fn job_types<I, S>(job_types: I) -> Result<Self, CatalogError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut parsed = BTreeSet::new();
        for job_type in job_types {
            let job_type = job_type.into();
            parsed.insert(JobTypeName::new(&job_type).map_err(|source| {
                CatalogError::InvalidExactSyncScopeJobType {
                    job_type: job_type.clone(),
                    source,
                }
            })?);
        }

        if parsed.is_empty() {
            return Err(CatalogError::InvalidExactSyncScope);
        }

        Ok(Self { job_types: parsed })
    }

    pub(super) fn contains(&self, job_type: JobType<'_>) -> bool {
        self.job_types.contains(job_type.as_str())
    }

    pub(super) fn job_types_for_storage(&self) -> Vec<JobTypeName> {
        self.job_types.iter().cloned().collect()
    }
}
