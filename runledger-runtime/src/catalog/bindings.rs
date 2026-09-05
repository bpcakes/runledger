use std::{collections::BTreeSet, sync::Arc};

use runledger_core::jobs::{JobHandler, JobSpec, JobSpecs};

use super::{CatalogError, JobCatalog, JobCatalogDefinitionOverrides};

/// Worker startup failure when bindings do not match the shared specifications.
#[derive(Debug)]
pub enum JobBindingError {
    Catalog(CatalogError),
    MissingHandler(String),
    UnknownHandler(String),
}

impl std::fmt::Display for JobBindingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Catalog(error) => error.fmt(f),
            Self::MissingHandler(name) => {
                write!(f, "missing handler for job specification: {name}")
            }
            Self::UnknownHandler(name) => write!(f, "handler has no job specification: {name}"),
        }
    }
}

impl std::error::Error for JobBindingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Catalog(error) => Some(error),
            _ => None,
        }
    }
}

impl JobCatalog {
    /// Adds a legacy or typed-adapted handler using shared operational metadata.
    pub fn try_handler_for_spec<H: JobHandler + 'static>(
        self,
        spec: &JobSpec,
        handler: H,
    ) -> Result<Self, CatalogError> {
        self.insert_spec_handler(spec, Arc::new(handler))
    }

    fn insert_spec_handler(
        self,
        spec: &JobSpec,
        handler: Arc<dyn JobHandler>,
    ) -> Result<Self, CatalogError> {
        let job_type = spec.job_type();
        let handler_type = handler.job_type();
        if handler_type != job_type {
            return Err(CatalogError::HandlerJobTypeMismatch {
                declared: job_type.as_str().to_owned(),
                handler: handler_type.as_str().to_owned(),
            });
        }
        let settings = spec.settings();
        self.insert_handler(job_type, handler)?
            .try_definition_overrides(
                job_type.as_str(),
                JobCatalogDefinitionOverrides::new()
                    .version(settings.version)
                    .max_attempts(settings.max_attempts)
                    .timeout_seconds(settings.default_timeout_seconds)
                    .priority(settings.default_priority)
                    .enabled(settings.is_enabled),
            )
    }

    /// Binds exactly one handler per shared spec. Disabled specs still require
    /// handlers to service already queued work and terminal cleanup.
    /// No partially bound catalog is returned on missing or duplicate handlers.
    pub fn from_specs(
        specs: &JobSpecs,
        handlers: impl IntoIterator<Item = Arc<dyn JobHandler>>,
    ) -> Result<Self, JobBindingError> {
        let mut catalog = Self::new();
        let mut bound = BTreeSet::new();
        for handler in handlers {
            let job_type = handler.job_type();
            let spec = specs
                .get(job_type)
                .ok_or_else(|| JobBindingError::UnknownHandler(job_type.as_str().to_owned()))?;
            catalog = catalog
                .insert_spec_handler(spec, handler)
                .map_err(JobBindingError::Catalog)?;
            bound.insert(job_type);
        }
        for spec in specs.iter() {
            if !bound.contains(&spec.job_type()) {
                return Err(JobBindingError::MissingHandler(
                    spec.job_type().as_str().to_owned(),
                ));
            }
        }
        Ok(catalog)
    }
}
