use runledger_core::jobs::{IdentifierValidationError, WorkflowBuildError};
use runledger_postgres::jobs::JobDefinitionCatalogSyncError;
use thiserror::Error;

/// Error returned by [`super::JobCatalog`] validation, sync, and helper methods.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum CatalogError {
    /// A caller supplied an invalid catalog job type.
    #[error("job type {job_type:?} is invalid: {source}")]
    InvalidJobType {
        /// Invalid job type value supplied by the caller.
        job_type: String,
        /// Identifier validation failure reported by `runledger-core`.
        #[source]
        source: IdentifierValidationError,
    },
    /// A registered handler returned an invalid job type.
    #[error("handler job type {handler_job_type:?} is invalid: {source}")]
    InvalidHandlerJobType {
        /// Invalid job type returned by the handler.
        handler_job_type: String,
        /// Identifier validation failure reported by `runledger-core`.
        #[source]
        source: IdentifierValidationError,
    },
    /// The declared catalog job type did not match the handler's job type.
    #[error("job type {declared} does not match handler job type {handler}")]
    HandlerJobTypeMismatch {
        /// Job type declared at the catalog registration site.
        declared: String,
        /// Job type returned by the handler.
        handler: String,
    },
    /// The catalog already contains the requested job type.
    #[error("job type {job_type} is already registered in the catalog")]
    DuplicateJobType {
        /// Duplicate job type.
        job_type: String,
    },
    /// A catalog definition default failed validation.
    #[error("catalog defaults are invalid: {field} must be positive")]
    InvalidDefinitionValue {
        /// Name of the invalid defaults field.
        field: &'static str,
    },
    /// Retry-delay override failure codes must be non-empty.
    #[error("failure code must be non-empty")]
    InvalidFailureCode,
    /// Retry-delay override values must be positive.
    #[error("retry delay override must be positive")]
    InvalidRetryDelay,
    /// Exact sync requires a non-empty owned job-type scope.
    #[error("exact sync scope must include at least one job type")]
    InvalidExactSyncScope,
    /// Exact sync scope construction received an invalid job type.
    #[error("exact sync scope job type {job_type:?} is invalid: {source}")]
    InvalidExactSyncScopeJobType {
        /// Invalid job type supplied for the exact-sync scope.
        job_type: String,
        /// Identifier validation failure reported by `runledger-core`.
        #[source]
        source: IdentifierValidationError,
    },
    /// Exact sync cannot run against an empty catalog.
    #[error("exact sync requires at least one catalog job")]
    EmptyExactSyncCatalog,
    /// A catalog job was not included in the exact-sync scope.
    #[error("catalog job type {job_type} is outside the exact sync scope")]
    JobTypeOutsideExactSyncScope {
        /// Catalog job type missing from the exact-sync scope.
        job_type: String,
    },
    /// An active schedule still references an enabled definition absent from the catalog.
    #[error("active schedule {schedule_name} still references absent catalog job type {job_type}")]
    ActiveScheduleForAbsentJobType {
        /// Active schedule name that blocks disabling the absent definition.
        schedule_name: String,
        /// Absent catalog job type referenced by the active schedule.
        job_type: String,
    },
    /// An active schedule still references a catalog job that would be disabled.
    #[error(
        "active schedule {schedule_name} still references disabled catalog job type {job_type}"
    )]
    ActiveScheduleForDisabledJobType {
        /// Active schedule name that blocks disabling the catalog definition.
        schedule_name: String,
        /// Catalog job type referenced by the active schedule.
        job_type: String,
    },
    /// The requested job type is not registered in the catalog.
    #[error("job type {job_type} is not registered in the catalog")]
    UnknownJobType {
        /// Missing job type.
        job_type: String,
    },
    /// The requested job type is disabled in catalog defaults.
    #[error("job type {job_type} is disabled in the catalog")]
    DisabledJobType {
        /// Disabled job type.
        job_type: String,
    },
    /// Workflow enqueue construction failed.
    #[error(transparent)]
    WorkflowBuild(#[from] WorkflowBuildError),
    /// Starting a catalog sync transaction failed.
    #[error("failed to start job definition sync transaction: {0}")]
    SyncFailure(#[source] Box<runledger_postgres::Error>),
    /// A persistence-layer catalog sync failure had no runtime-specific mapping.
    #[error("failed to sync job definitions with an unmapped persistence error: {0}")]
    DefinitionCatalogSyncFailure(#[source] Box<dyn std::error::Error + Send + Sync>),
    /// Syncing a specific catalog job definition failed.
    #[error("failed to sync job definition {job_type}: {source}")]
    DefinitionSyncFailure {
        /// Catalog job type whose definition failed to sync.
        job_type: String,
        /// Persistence-layer failure that occurred while syncing the definition.
        #[source]
        source: Box<runledger_postgres::Error>,
    },
    /// Committing a catalog sync transaction failed.
    #[error("failed to commit job definition sync transaction: {0}")]
    CommitFailure(#[source] sqlx::Error),
    /// Applying the transaction-local bounds for definition-disabling sync failed.
    #[error("failed to bound job definition sync critical section: {0}")]
    CriticalSectionTimeoutFailure(#[source] Box<runledger_postgres::Error>),
    /// Catalog definition sync input failed persistence-layer validation.
    #[error("job definition sync input is invalid: {0}")]
    DefinitionSyncValidationFailure(#[source] Box<runledger_postgres::Error>),
    /// Locking schedules before disabling definitions failed.
    #[error("failed to lock job schedules before disabling job definitions: {0}")]
    ScheduleLockFailure(#[source] Box<runledger_postgres::Error>),
    /// Locking definitions before checking and disabling definitions failed.
    #[error("failed to lock job definitions before disabling job definitions: {0}")]
    DefinitionLockFailure(#[source] Box<runledger_postgres::Error>),
    /// Checking active schedules before disabling definitions failed.
    #[error("failed to check active schedules before disabling job definitions: {0}")]
    ScheduleCheckFailure(#[source] Box<runledger_postgres::Error>),
    /// Inspecting existing job definitions before sync failed.
    #[error("failed to inspect job definitions before syncing catalog: {0}")]
    DefinitionInspectFailure(#[source] Box<runledger_postgres::Error>),
    /// Disabling absent job definitions failed.
    #[error("failed to disable absent job definitions: {0}")]
    DisableAbsentFailure(#[source] Box<runledger_postgres::Error>),
}

impl CatalogError {
    pub(crate) fn from_definition_catalog_sync_error(error: JobDefinitionCatalogSyncError) -> Self {
        match error {
            JobDefinitionCatalogSyncError::ActiveScheduleForAbsentJobType(reference) => {
                Self::ActiveScheduleForAbsentJobType {
                    schedule_name: reference.schedule_name,
                    job_type: reference.job_type.to_string(),
                }
            }
            JobDefinitionCatalogSyncError::ActiveScheduleForDisabledJobType(reference) => {
                Self::ActiveScheduleForDisabledJobType {
                    schedule_name: reference.schedule_name,
                    job_type: reference.job_type.to_string(),
                }
            }
            JobDefinitionCatalogSyncError::CriticalSectionTimeoutFailure(source) => {
                Self::CriticalSectionTimeoutFailure(source)
            }
            JobDefinitionCatalogSyncError::ScheduleLockFailure(source) => {
                Self::ScheduleLockFailure(source)
            }
            JobDefinitionCatalogSyncError::DefinitionLockFailure(source) => {
                Self::DefinitionLockFailure(source)
            }
            JobDefinitionCatalogSyncError::ScheduleCheckFailure(source) => {
                Self::ScheduleCheckFailure(source)
            }
            JobDefinitionCatalogSyncError::ValidationFailure(source) => {
                Self::DefinitionSyncValidationFailure(source)
            }
            JobDefinitionCatalogSyncError::DefinitionInspectFailure(source) => {
                Self::DefinitionInspectFailure(source)
            }
            JobDefinitionCatalogSyncError::DefinitionSyncFailure { job_type, source } => {
                Self::DefinitionSyncFailure { job_type, source }
            }
            JobDefinitionCatalogSyncError::DisableAbsentFailure(source) => {
                Self::DisableAbsentFailure(source)
            }
            // Fallback for future #[non_exhaustive] variants that this runtime
            // version cannot map to a more specific CatalogError.
            _ => Self::DefinitionCatalogSyncFailure(Box::new(error)),
        }
    }
}
