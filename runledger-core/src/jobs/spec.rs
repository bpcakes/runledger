use std::collections::BTreeMap;

use super::{JobType, JobTypeName};

/// Default values applied when syncing catalog jobs to `job_definitions`.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobDefinitionSettings {
    /// Definition version written for catalog jobs.
    pub version: i32,
    /// Default maximum attempts written for catalog jobs.
    pub max_attempts: i32,
    /// Default execution timeout, in seconds, written for catalog jobs.
    pub default_timeout_seconds: i32,
    /// Default queue priority written for catalog jobs.
    pub default_priority: i32,
    /// Whether catalog jobs should be synced as enabled.
    pub is_enabled: bool,
}

impl Default for JobDefinitionSettings {
    fn default() -> Self {
        Self::new()
    }
}

impl JobDefinitionSettings {
    /// Creates the default catalog definition values.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 300,
            default_priority: 0,
            is_enabled: true,
        }
    }

    /// Sets the definition version written for catalog jobs.
    #[must_use]
    pub const fn version(mut self, version: i32) -> Self {
        self.version = version;
        self
    }

    /// Sets the default maximum attempts written for catalog jobs.
    #[must_use]
    pub const fn max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Sets the default execution timeout, in seconds, written for catalog jobs.
    #[must_use]
    pub const fn timeout_seconds(mut self, default_timeout_seconds: i32) -> Self {
        self.default_timeout_seconds = default_timeout_seconds;
        self
    }

    /// Sets the default queue priority written for catalog jobs.
    #[must_use]
    pub const fn priority(mut self, default_priority: i32) -> Self {
        self.default_priority = default_priority;
        self
    }

    /// Sets whether catalog jobs should be synced as enabled.
    #[must_use]
    pub const fn enabled(mut self, is_enabled: bool) -> Self {
        self.is_enabled = is_enabled;
        self
    }

    pub fn validate(self) -> Result<(), &'static str> {
        if self.version <= 0 {
            return Err("version");
        }
        if self.max_attempts <= 0 {
            return Err("max_attempts");
        }
        if self.default_timeout_seconds <= 0 {
            return Err("default_timeout_seconds");
        }
        // default_priority intentionally accepts zero and negative values.
        Ok(())
    }
}

/// Storage-independent definition shared by producers and worker bindings.
///
/// Definition version and settings are operational metadata. They are never
/// injected into payloads or enqueue request snapshots. Applications own any
/// durable payload versioning and must retain decoding for queued older rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JobSpec {
    job_type: JobType<'static>,
    settings: JobDefinitionSettings,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JobSpecError {
    InvalidJobType,
    InvalidSetting(&'static str),
    DuplicateJobType(String),
    DisabledJobType(String),
}

impl std::fmt::Display for JobSpecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidJobType => f.write_str("job type must be non-empty"),
            Self::InvalidSetting(field) => write!(f, "invalid job definition setting: {field}"),
            Self::DuplicateJobType(name) => write!(f, "duplicate job specification: {name}"),
            Self::DisabledJobType(name) => write!(f, "job specification is disabled: {name}"),
        }
    }
}

impl std::error::Error for JobSpecError {}

impl JobSpec {
    pub fn new(job_type: JobType<'static>) -> Result<Self, JobSpecError> {
        JobTypeName::new(job_type.as_str()).map_err(|_| JobSpecError::InvalidJobType)?;
        Ok(Self {
            job_type,
            settings: JobDefinitionSettings::default(),
        })
    }

    pub fn with_settings(mut self, settings: JobDefinitionSettings) -> Result<Self, JobSpecError> {
        settings.validate().map_err(JobSpecError::InvalidSetting)?;
        self.settings = settings;
        Ok(self)
    }

    #[must_use]
    pub fn job_type(&self) -> JobType<'static> {
        self.job_type
    }

    #[must_use]
    pub fn settings(&self) -> JobDefinitionSettings {
        self.settings
    }

    /// Builds a JSON request without implicitly snapshotting definition settings.
    pub fn submit(&self, payload: serde_json::Value) -> Result<super::JobSubmission, JobSpecError> {
        self.require_enabled()?;
        Ok(super::JobSubmission::new(self.job_type, payload))
    }

    /// Checks code configuration; persistence still enforces operator disables.
    pub fn require_enabled(&self) -> Result<(), JobSpecError> {
        if self.settings.is_enabled {
            Ok(())
        } else {
            Err(JobSpecError::DisabledJobType(
                self.job_type.as_str().to_owned(),
            ))
        }
    }
}

/// A validated producer-side collection requiring no handler or provider clients.
#[derive(Debug, Clone, Default)]
pub struct JobSpecs(BTreeMap<JobTypeName, JobSpec>);

impl JobSpecs {
    pub fn new(specs: impl IntoIterator<Item = JobSpec>) -> Result<Self, JobSpecError> {
        let mut entries = BTreeMap::new();
        for spec in specs {
            let key = JobTypeName::new(spec.job_type.as_str())
                .map_err(|_| JobSpecError::InvalidJobType)?;
            if entries.insert(key, spec).is_some() {
                return Err(JobSpecError::DuplicateJobType(
                    spec.job_type.as_str().to_owned(),
                ));
            }
        }
        Ok(Self(entries))
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = &JobSpec> {
        self.0.values()
    }

    pub fn get(&self, job_type: JobType<'_>) -> Option<&JobSpec> {
        self.0.get(job_type.as_str())
    }
}
