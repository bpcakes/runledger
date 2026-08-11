use std::{error::Error as StdError, fmt};

use crate::{DbTx, Error, QueryError, QueryErrorCategory, Result};
use runledger_core::jobs::JobTypeName;

use super::super::super::row_decode::parse_job_type_name;
use super::super::super::schedule_definition_guard::{
    self, GuardLockContext, ScheduleDefinitionLockError,
};
use super::super::super::types::{JobDefinitionUpsert, JobScheduleJobTypeReference};
use super::crud::{apply_job_definition_upsert_tx, upsert_job_definition_preserving_enabled_tx};

/// Summary of definition rows changed by a catalog sync.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobDefinitionCatalogSyncReport {
    /// Enabled definitions in the exact-sync scope that were absent from the
    /// catalog and changed to disabled.
    pub disabled_absent_job_types: Vec<JobTypeName>,
    /// Catalog definitions changed to disabled because the catalog synced them
    /// with `is_enabled = false`.
    pub disabled_catalog_job_types: Vec<JobTypeName>,
}

/// Enabled-state handling for additive catalog definition sync.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobDefinitionCatalogSyncMode {
    /// Preserve stored `is_enabled` for enabled catalog definitions on conflict.
    ///
    /// This keeps operator pauses in place when a worker restarts with the same
    /// enabled catalog. Disabled definitions in the payload still write
    /// `is_enabled = false`.
    PreserveExistingEnabledForEnabledDefinitions,
    /// Write each payload's `is_enabled` value on insert and conflict.
    RestoreCatalogEnabledState,
}

/// Error returned while applying a catalog-owned job-definition sync.
#[derive(Debug)]
#[non_exhaustive]
pub enum JobDefinitionCatalogSyncError {
    /// An active schedule references an enabled scoped definition absent from
    /// the catalog.
    ActiveScheduleForAbsentJobType(JobScheduleJobTypeReference),
    /// An active schedule references a catalog definition that would be disabled.
    ActiveScheduleForDisabledJobType(JobScheduleJobTypeReference),
    /// Applying transaction-local statement timeout bounds failed.
    CriticalSectionTimeoutFailure(Box<Error>),
    /// Locking `job_schedules` before disabling definitions failed.
    ScheduleLockFailure(Box<Error>),
    /// Locking `job_definitions` before disabling definitions failed.
    DefinitionLockFailure(Box<Error>),
    /// Checking active schedules before disabling definitions failed.
    ScheduleCheckFailure(Box<Error>),
    /// Sync input failed validation before any catalog writes.
    ValidationFailure(Box<Error>),
    /// Inspecting existing definitions before sync failed.
    DefinitionInspectFailure(Box<Error>),
    /// Syncing one catalog definition failed.
    DefinitionSyncFailure {
        /// Job type whose definition failed to sync.
        job_type: String,
        /// Persistence-layer failure returned by the definition write.
        source: Box<Error>,
    },
    /// Disabling absent scoped definitions failed.
    DisableAbsentFailure(Box<Error>),
}

impl fmt::Display for JobDefinitionCatalogSyncError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ActiveScheduleForAbsentJobType(reference) => write!(
                f,
                "active schedule {} still references absent catalog job type {}",
                reference.schedule_name, reference.job_type
            ),
            Self::ActiveScheduleForDisabledJobType(reference) => write!(
                f,
                "active schedule {} still references disabled catalog job type {}",
                reference.schedule_name, reference.job_type
            ),
            Self::CriticalSectionTimeoutFailure(error) => {
                write!(
                    f,
                    "failed to bound job definition disable critical section: {error}"
                )
            }
            Self::ScheduleLockFailure(error) => write!(
                f,
                "failed to lock job schedules before disabling job definitions: {error}"
            ),
            Self::DefinitionLockFailure(error) => write!(
                f,
                "failed to lock job definitions before disabling job definitions: {error}"
            ),
            Self::ScheduleCheckFailure(error) => write!(
                f,
                "failed to check active schedules before disabling job definitions: {error}"
            ),
            Self::ValidationFailure(error) => {
                write!(f, "job definition catalog sync input is invalid: {error}")
            }
            Self::DefinitionInspectFailure(error) => {
                write!(
                    f,
                    "failed to inspect job definitions before catalog sync: {error}"
                )
            }
            Self::DefinitionSyncFailure { job_type, source } => {
                write!(f, "failed to sync job definition {job_type}: {source}")
            }
            Self::DisableAbsentFailure(error) => {
                write!(f, "failed to disable absent job definitions: {error}")
            }
        }
    }
}

impl StdError for JobDefinitionCatalogSyncError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::CriticalSectionTimeoutFailure(error)
            | Self::ScheduleLockFailure(error)
            | Self::DefinitionLockFailure(error)
            | Self::ScheduleCheckFailure(error)
            | Self::ValidationFailure(error)
            | Self::DefinitionInspectFailure(error)
            | Self::DefinitionSyncFailure { source: error, .. }
            | Self::DisableAbsentFailure(error) => Some(error.as_ref()),
            Self::ActiveScheduleForAbsentJobType(_) | Self::ActiveScheduleForDisabledJobType(_) => {
                None
            }
        }
    }
}

enum ValidatedDefinitionCatalogSyncScope<'scope> {
    Additive {
        mode: JobDefinitionCatalogSyncMode,
    },
    Exact {
        scope_job_types: &'scope [JobTypeName],
        catalog_job_types: Vec<JobTypeName>,
        has_absent_scope_job_types: bool,
    },
}

struct DefinitionCatalogSync<'definitions, 'payload, 'scope> {
    definitions: &'definitions [JobDefinitionUpsert<'payload>],
    disabled_job_types: Vec<JobTypeName>,
    scope: ValidatedDefinitionCatalogSyncScope<'scope>,
}

impl<'definitions, 'payload, 'scope> DefinitionCatalogSync<'definitions, 'payload, 'scope> {
    fn additive(
        definitions: &'definitions [JobDefinitionUpsert<'payload>],
        mode: JobDefinitionCatalogSyncMode,
    ) -> std::result::Result<Self, JobDefinitionCatalogSyncError> {
        let disabled_job_types = definition_job_type_names(
            definitions
                .iter()
                .filter(|definition| !definition.is_enabled),
        )?;

        Ok(Self {
            definitions,
            disabled_job_types,
            scope: ValidatedDefinitionCatalogSyncScope::Additive { mode },
        })
    }

    fn exact(
        definitions: &'definitions [JobDefinitionUpsert<'payload>],
        scope_job_types: &'scope [JobTypeName],
    ) -> std::result::Result<Self, JobDefinitionCatalogSyncError> {
        let catalog_job_types = definition_job_type_names(definitions.iter())?;
        validate_non_empty_job_types("exact catalog sync job definitions", &catalog_job_types)
            .map_err(|error| JobDefinitionCatalogSyncError::ValidationFailure(Box::new(error)))?;
        validate_non_empty_job_types("exact catalog sync scope", scope_job_types)
            .map_err(|error| JobDefinitionCatalogSyncError::ValidationFailure(Box::new(error)))?;

        let disabled_job_types = definition_job_type_names(
            definitions
                .iter()
                .filter(|definition| !definition.is_enabled),
        )?;
        let has_absent_scope_job_types = scope_job_types
            .iter()
            .any(|job_type| !catalog_job_types.contains(job_type));

        Ok(Self {
            definitions,
            disabled_job_types,
            scope: ValidatedDefinitionCatalogSyncScope::Exact {
                scope_job_types,
                catalog_job_types,
                has_absent_scope_job_types,
            },
        })
    }

    async fn execute(
        &self,
        tx: &mut DbTx<'_>,
    ) -> std::result::Result<JobDefinitionCatalogSyncReport, JobDefinitionCatalogSyncError> {
        // Preserve the disable protocol's lock and validation order: acquire
        // the guard, reject unsafe schedule references, write definitions, and
        // only then disable exact-scope rows absent from the catalog.
        if self.requires_disable_guard() {
            prepare_definition_disable_critical_section_tx(tx).await?;
        }

        let disabled_catalog_job_types = self.inspect_disabled_catalog_definitions_tx(tx).await?;
        self.reject_active_schedules_for_absent_definitions_tx(tx)
            .await?;
        self.upsert_definitions_tx(tx).await?;
        let disabled_absent_job_types = self.disable_absent_definitions_tx(tx).await?;

        Ok(JobDefinitionCatalogSyncReport {
            disabled_absent_job_types,
            disabled_catalog_job_types,
        })
    }

    fn requires_disable_guard(&self) -> bool {
        !self.disabled_job_types.is_empty()
            || matches!(
                &self.scope,
                ValidatedDefinitionCatalogSyncScope::Exact {
                    has_absent_scope_job_types: true,
                    ..
                }
            )
    }

    async fn inspect_disabled_catalog_definitions_tx(
        &self,
        tx: &mut DbTx<'_>,
    ) -> std::result::Result<Vec<JobTypeName>, JobDefinitionCatalogSyncError> {
        if self.disabled_job_types.is_empty() {
            return Ok(Vec::new());
        }

        reject_active_schedules_for_disabled_job_types_tx(tx, &self.disabled_job_types).await?;
        // Report rows that this sync will newly create as disabled or change
        // from enabled to disabled. Already-disabled rows are intentionally
        // omitted from the report.
        list_job_types_missing_or_enabled_definitions_tx(tx, &self.disabled_job_types)
            .await
            .map_err(|error| {
                JobDefinitionCatalogSyncError::DefinitionInspectFailure(Box::new(error))
            })
    }

    async fn reject_active_schedules_for_absent_definitions_tx(
        &self,
        tx: &mut DbTx<'_>,
    ) -> std::result::Result<(), JobDefinitionCatalogSyncError> {
        let ValidatedDefinitionCatalogSyncScope::Exact {
            scope_job_types,
            catalog_job_types,
            has_absent_scope_job_types: true,
        } = &self.scope
        else {
            return Ok(());
        };

        if let Some(reference) =
            schedule_definition_guard::find_active_schedule_for_enabled_absent_job_types_tx(
                tx,
                catalog_job_types,
                scope_job_types,
            )
            .await
            .map_err(|error| JobDefinitionCatalogSyncError::ScheduleCheckFailure(Box::new(error)))?
        {
            return Err(JobDefinitionCatalogSyncError::ActiveScheduleForAbsentJobType(reference));
        }

        Ok(())
    }

    async fn upsert_definitions_tx(
        &self,
        tx: &mut DbTx<'_>,
    ) -> std::result::Result<(), JobDefinitionCatalogSyncError> {
        // Exact sync restores catalog enabled state. Re-enabling cannot orphan
        // an active schedule, so it does not independently require the disable
        // guard when no definition is being disabled.
        for definition in self.definitions {
            let upsert_result = match (self.upsert_mode(), definition.is_enabled) {
                (
                    JobDefinitionCatalogSyncMode::PreserveExistingEnabledForEnabledDefinitions,
                    true,
                ) => upsert_job_definition_preserving_enabled_tx(tx, definition).await,
                (
                    JobDefinitionCatalogSyncMode::PreserveExistingEnabledForEnabledDefinitions,
                    false,
                )
                | (JobDefinitionCatalogSyncMode::RestoreCatalogEnabledState, _) => {
                    apply_job_definition_upsert_tx(tx, definition).await
                }
            };
            upsert_result.map_err(|source| {
                JobDefinitionCatalogSyncError::DefinitionSyncFailure {
                    job_type: definition.job_type.as_str().to_owned(),
                    source: Box::new(source),
                }
            })?;
        }

        Ok(())
    }

    fn upsert_mode(&self) -> JobDefinitionCatalogSyncMode {
        match self.scope {
            ValidatedDefinitionCatalogSyncScope::Additive { mode } => mode,
            ValidatedDefinitionCatalogSyncScope::Exact { .. } => {
                JobDefinitionCatalogSyncMode::RestoreCatalogEnabledState
            }
        }
    }

    async fn disable_absent_definitions_tx(
        &self,
        tx: &mut DbTx<'_>,
    ) -> std::result::Result<Vec<JobTypeName>, JobDefinitionCatalogSyncError> {
        let ValidatedDefinitionCatalogSyncScope::Exact {
            scope_job_types,
            catalog_job_types,
            has_absent_scope_job_types: true,
        } = &self.scope
        else {
            return Ok(Vec::new());
        };

        disable_enabled_job_definitions_except_tx(tx, catalog_job_types, scope_job_types)
            .await
            .map_err(|error| JobDefinitionCatalogSyncError::DisableAbsentFailure(Box::new(error)))
    }
}

pub async fn sync_catalog_job_definitions_tx(
    tx: &mut DbTx<'_>,
    definitions: &[JobDefinitionUpsert<'_>],
    mode: JobDefinitionCatalogSyncMode,
) -> std::result::Result<JobDefinitionCatalogSyncReport, JobDefinitionCatalogSyncError> {
    DefinitionCatalogSync::additive(definitions, mode)?
        .execute(tx)
        .await
}

pub async fn sync_catalog_job_definitions_exact_tx(
    tx: &mut DbTx<'_>,
    definitions: &[JobDefinitionUpsert<'_>],
    scope_job_types: &[JobTypeName],
) -> std::result::Result<JobDefinitionCatalogSyncReport, JobDefinitionCatalogSyncError> {
    DefinitionCatalogSync::exact(definitions, scope_job_types)?
        .execute(tx)
        .await
}

async fn list_job_types_missing_or_enabled_definitions_tx(
    tx: &mut DbTx<'_>,
    job_types: &[JobTypeName],
) -> Result<Vec<JobTypeName>> {
    let job_types = job_type_strings(job_types);
    let rows = sqlx::query_scalar!(
        "SELECT catalog.job_type as \"job_type!\"
         FROM unnest($1::text[]) AS catalog(job_type)
         LEFT JOIN job_definitions
            ON job_definitions.job_type = catalog.job_type
         WHERE job_definitions.job_type IS NULL
            OR job_definitions.is_enabled = true",
        job_types.as_slice(),
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("list missing or enabled job definitions", error)
    })?;

    parse_job_type_rows(rows)
}

async fn disable_enabled_job_definitions_except_tx(
    tx: &mut DbTx<'_>,
    keep_job_types: &[JobTypeName],
    scope_job_types: &[JobTypeName],
) -> Result<Vec<JobTypeName>> {
    validate_non_empty_job_types("disable enabled job definitions keep list", keep_job_types)?;
    validate_non_empty_job_types("disable enabled job definitions scope", scope_job_types)?;

    let keep_job_types = job_type_strings(keep_job_types);
    let scope_job_types = job_type_strings(scope_job_types);
    let rows = sqlx::query_scalar!(
        "UPDATE job_definitions
         SET is_enabled = false,
             updated_at = now()
         WHERE is_enabled = true
           AND job_type <> ALL($1::text[])
           AND job_type = ANY($2::text[])
         RETURNING job_type",
        keep_job_types.as_slice(),
        scope_job_types.as_slice(),
    )
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("disable enabled job definitions except list", error)
    })?;

    parse_job_type_rows(rows)
}

fn job_type_strings(job_types: &[JobTypeName]) -> Vec<String> {
    job_types
        .iter()
        .map(|job_type| job_type.as_str().to_owned())
        .collect()
}

fn definition_job_type_names<'definition, 'payload, I>(
    definitions: I,
) -> std::result::Result<Vec<JobTypeName>, JobDefinitionCatalogSyncError>
where
    'payload: 'definition,
    I: IntoIterator<Item = &'definition JobDefinitionUpsert<'payload>>,
{
    // JobType::new is intentionally lightweight, so the catalog sync boundary
    // revalidates names before using them in scope comparisons or reports.
    let mut job_types = definitions
        .into_iter()
        .map(|definition| parse_job_type_name(definition.job_type.as_str().to_owned()))
        .collect::<Result<Vec<_>>>()
        .map_err(|error| {
            JobDefinitionCatalogSyncError::DefinitionInspectFailure(Box::new(error))
        })?;
    job_types.sort();
    Ok(job_types)
}

async fn prepare_definition_disable_critical_section_tx(
    tx: &mut DbTx<'_>,
) -> std::result::Result<(), JobDefinitionCatalogSyncError> {
    schedule_definition_guard::cap_definition_disable_statement_timeout_tx(tx)
        .await
        .map_err(|error| {
            JobDefinitionCatalogSyncError::CriticalSectionTimeoutFailure(Box::new(error))
        })?;
    schedule_definition_guard::lock_schedules_then_definitions_tx(
        tx,
        GuardLockContext::DefinitionDisable,
    )
    .await
    .map_err(|error| match error {
        ScheduleDefinitionLockError::Schedule(error) => {
            JobDefinitionCatalogSyncError::ScheduleLockFailure(Box::new(error))
        }
        ScheduleDefinitionLockError::Definition(error) => {
            JobDefinitionCatalogSyncError::DefinitionLockFailure(Box::new(error))
        }
    })
}

async fn reject_active_schedules_for_disabled_job_types_tx(
    tx: &mut DbTx<'_>,
    job_types: &[JobTypeName],
) -> std::result::Result<(), JobDefinitionCatalogSyncError> {
    if let Some(reference) =
        schedule_definition_guard::find_active_schedule_for_job_types_tx(tx, job_types)
            .await
            .map_err(|error| JobDefinitionCatalogSyncError::ScheduleCheckFailure(Box::new(error)))?
    {
        return Err(JobDefinitionCatalogSyncError::ActiveScheduleForDisabledJobType(reference));
    }

    Ok(())
}

fn parse_job_type_rows(rows: Vec<String>) -> Result<Vec<JobTypeName>> {
    let mut job_types = rows
        .into_iter()
        .map(parse_job_type_name)
        .collect::<Result<Vec<_>>>()?;
    job_types.sort();
    Ok(job_types)
}

fn validate_non_empty_job_types(context: &'static str, job_types: &[JobTypeName]) -> Result<()> {
    if job_types.is_empty() {
        return Err(Error::QueryError(QueryError::from_classified(
            QueryErrorCategory::Validation,
            "job_definition.empty_job_type_list",
            "Job type list must not be empty.",
            format!("{context}: job type list must not be empty"),
        )));
    }
    Ok(())
}
