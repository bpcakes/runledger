use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use runledger_core::jobs::{JobHandler, JobType, JobTypeName};

use super::CatalogError;

pub use runledger_core::jobs::JobDefinitionSettings as JobCatalogDefaults;

/// Per-job definition values that override [`JobCatalogDefaults`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JobCatalogDefinitionOverrides {
    /// Optional definition version override.
    version: Option<i32>,
    /// Optional maximum-attempts override.
    max_attempts: Option<i32>,
    /// Optional execution timeout override, in seconds.
    default_timeout_seconds: Option<i32>,
    /// Optional queue priority override.
    default_priority: Option<i32>,
    /// Optional enabled-state override.
    is_enabled: Option<bool>,
}

impl JobCatalogDefinitionOverrides {
    /// Creates empty job-specific definition overrides.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Overrides the definition version written for this job.
    #[must_use]
    pub fn version(mut self, version: i32) -> Self {
        self.version = Some(version);
        self
    }

    /// Overrides the default maximum attempts written for this job.
    #[must_use]
    pub fn max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = Some(max_attempts);
        self
    }

    /// Overrides the default execution timeout, in seconds, written for this job.
    #[must_use]
    pub fn timeout_seconds(mut self, default_timeout_seconds: i32) -> Self {
        self.default_timeout_seconds = Some(default_timeout_seconds);
        self
    }

    /// Overrides the default queue priority written for this job.
    #[must_use]
    pub fn priority(mut self, default_priority: i32) -> Self {
        self.default_priority = Some(default_priority);
        self
    }

    /// Overrides whether this job should be synced as enabled.
    #[must_use]
    pub fn enabled(mut self, is_enabled: bool) -> Self {
        self.is_enabled = Some(is_enabled);
        self
    }

    pub(super) fn validate(self) -> Result<(), &'static str> {
        if matches!(self.version, Some(version) if version <= 0) {
            return Err("version");
        }
        if matches!(self.max_attempts, Some(max_attempts) if max_attempts <= 0) {
            return Err("max_attempts");
        }
        if matches!(
            self.default_timeout_seconds,
            Some(default_timeout_seconds) if default_timeout_seconds <= 0
        ) {
            return Err("default_timeout_seconds");
        }
        // default_priority intentionally accepts zero and negative values.
        Ok(())
    }

    pub(super) fn apply_to(self, defaults: JobCatalogDefaults) -> JobCatalogDefaults {
        defaults
            .version(self.version.unwrap_or(defaults.version))
            .max_attempts(self.max_attempts.unwrap_or(defaults.max_attempts))
            .timeout_seconds(
                self.default_timeout_seconds
                    .unwrap_or(defaults.default_timeout_seconds),
            )
            .priority(self.default_priority.unwrap_or(defaults.default_priority))
            .enabled(self.is_enabled.unwrap_or(defaults.is_enabled))
    }
}

/// Single-source registry of job handlers, definition defaults, schedules, and enqueue helpers.
#[derive(Debug, Clone)]
pub struct JobCatalog {
    pub(super) defaults: JobCatalogDefaults,
    pub(super) jobs: BTreeMap<JobTypeName, CatalogJob>,
    pub(super) schedules: Vec<super::schedule_spec::StoredCatalogJobScheduleSpec>,
}

#[derive(Clone)]
pub(super) struct CatalogJob {
    pub(super) job_type: JobType<'static>,
    pub(super) handler: Arc<dyn JobHandler>,
    pub(super) definition_overrides: JobCatalogDefinitionOverrides,
    pub(super) retry_delay_overrides: BTreeMap<&'static str, i32>,
}

impl CatalogJob {
    pub(super) fn job_type(&self) -> JobType<'static> {
        self.job_type
    }
}

impl fmt::Debug for CatalogJob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CatalogJob")
            .field("job_type", &self.job_type())
            .field("definition_overrides", &self.definition_overrides)
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
    /// Catalog job types that this additive sync forced disabled through
    /// effective catalog definition values. Already-disabled catalog rows are
    /// not included.
    pub disabled_catalog_job_types: Vec<JobTypeName>,
}

/// Result returned by [`JobCatalog::sync_definitions_exact`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobCatalogExactSyncReport {
    /// Enabled definitions inside the exact-sync scope that were absent from the
    /// catalog and changed to disabled by this sync.
    pub disabled_absent_job_types: Vec<JobTypeName>,
    /// Catalog job types that this sync forced disabled through effective
    /// catalog definition values, including both newly inserted disabled rows
    /// and existing enabled rows changed to disabled. Already-disabled catalog
    /// rows are not included.
    pub disabled_catalog_job_types: Vec<JobTypeName>,
}

/// Explicit owned schedule-name scope used by exact catalog schedule sync.
///
/// Exact sync only deactivates schedules whose names are in this scope, and it
/// rejects synced specs whose names are outside the scope. Use this to describe
/// the deployment-owned schedule names that may be made inactive when omitted
/// from the current catalog.
///
/// # Example
/// ```rust
/// use runledger_runtime::catalog::JobCatalogScheduleSyncScope;
///
/// let scope = JobCatalogScheduleSyncScope::schedule_names([
///     "profiles.refresh.hourly",
///     "profiles.refresh.daily",
/// ])?;
///
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobCatalogScheduleSyncScope {
    pub(super) schedule_names: BTreeSet<String>,
}

/// Result returned by catalog schedule sync methods.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobCatalogScheduleSyncReport {
    /// Schedule names upserted and active state applied during this sync, in the
    /// order they were supplied by the catalog or caller.
    pub synced_schedule_names: Vec<String>,
    /// Enabled schedules inside the exact-sync scope that were absent from the
    /// synced spec set and changed to inactive by this sync. Empty for additive
    /// sync methods. Names are returned in sorted order.
    pub deactivated_absent_schedule_names: Vec<String>,
}

impl JobCatalogScheduleSyncScope {
    /// Builds a scope containing one owned schedule name.
    ///
    /// # Errors
    /// Returns [`CatalogError::InvalidExactScheduleSyncScopeName`] when `name`
    /// is blank or has surrounding whitespace.
    pub fn schedule_name(name: impl Into<String>) -> Result<Self, CatalogError> {
        Self::schedule_names([name])
    }

    /// Builds a scope containing one or more owned schedule names.
    ///
    /// Duplicate names are rejected instead of silently coalesced so callers can
    /// catch ownership-list mistakes during startup.
    /// Whitespace-only names are reported as invalid names rather than as an
    /// empty scope.
    ///
    /// # Errors
    /// Returns [`CatalogError::InvalidExactScheduleSyncScope`] when no schedule
    /// name is provided, or [`CatalogError::InvalidExactScheduleSyncScopeName`]
    /// when any name is blank or has surrounding whitespace.
    pub fn schedule_names<I, S>(schedule_names: I) -> Result<Self, CatalogError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut parsed = BTreeSet::new();
        for name in schedule_names {
            let name = name.into();
            Self::validate_schedule_name(&name)?;
            if !parsed.insert(name.clone()) {
                return Err(CatalogError::DuplicateExactScheduleSyncScopeName { name });
            }
        }

        if parsed.is_empty() {
            return Err(CatalogError::InvalidExactScheduleSyncScope);
        }

        Ok(Self {
            schedule_names: parsed,
        })
    }

    pub(super) fn schedule_names_for_storage(&self) -> Vec<String> {
        self.schedule_names.iter().cloned().collect()
    }

    pub(super) fn contains(&self, schedule_name: &str) -> bool {
        self.schedule_names.contains(schedule_name)
    }

    fn validate_schedule_name(name: &str) -> Result<(), CatalogError> {
        if name.trim().is_empty() {
            return Err(CatalogError::InvalidExactScheduleSyncScopeName {
                name: name.to_owned(),
            });
        }
        if name != name.trim() {
            return Err(CatalogError::InvalidExactScheduleSyncScopeName {
                name: name.to_owned(),
            });
        }
        Ok(())
    }
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
