use std::{error::Error as StdError, fmt};

use crate::{DbPool, DbTx, Error, QueryError, QueryErrorCategory, Result};
use runledger_core::jobs::{JobType, JobTypeName};

use super::super::row_decode::parse_job_type_name;
use super::super::types::{
    JobDefinitionListFilter, JobDefinitionRecord, JobDefinitionUpdate, JobDefinitionUpsert,
    JobScheduleJobTypeReference,
};

const DEFINITION_DISABLE_LOCK_TIMEOUT: &str = "5s";
const DEFINITION_DISABLE_LOCK_TIMEOUT_MS: i64 = 5_000;
const DEFINITION_DISABLE_STATEMENT_TIMEOUT: &str = "30s";
const DEFINITION_DISABLE_STATEMENT_TIMEOUT_MS: i64 = 30_000;

/// Summary of definition rows changed by a catalog sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobDefinitionCatalogSyncReport {
    /// Enabled definitions in the exact-sync scope that were absent from the
    /// catalog and changed to disabled.
    pub disabled_absent_job_types: Vec<JobTypeName>,
    /// Catalog definitions changed to disabled because the catalog synced them
    /// with `is_enabled = false`.
    pub disabled_catalog_job_types: Vec<JobTypeName>,
}

/// Enabled-state handling for additive catalog definition sync.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[non_exhaustive]
#[derive(Debug)]
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

pub async fn sync_catalog_job_definitions_tx(
    tx: &mut DbTx<'_>,
    definitions: &[JobDefinitionUpsert<'_>],
    mode: JobDefinitionCatalogSyncMode,
) -> std::result::Result<JobDefinitionCatalogSyncReport, JobDefinitionCatalogSyncError> {
    let disabled_job_types = definition_job_type_names(
        definitions
            .iter()
            .filter(|definition| !definition.is_enabled),
    )?;
    let disabled_catalog_job_types = if disabled_job_types.is_empty() {
        Vec::new()
    } else {
        prepare_definition_disable_critical_section_tx(tx).await?;
        reject_active_schedules_for_disabled_job_types_tx(tx, &disabled_job_types).await?;
        // Report rows that this sync will newly create as disabled or change
        // from enabled to disabled. Already-disabled rows are intentionally
        // omitted from the report.
        list_job_types_missing_or_enabled_definitions_tx(tx, &disabled_job_types)
            .await
            .map_err(|error| {
                JobDefinitionCatalogSyncError::DefinitionInspectFailure(Box::new(error))
            })?
    };

    for definition in definitions {
        let upsert_result = match (mode, definition.is_enabled) {
            (JobDefinitionCatalogSyncMode::PreserveExistingEnabledForEnabledDefinitions, true) => {
                upsert_job_definition_preserving_enabled_tx(tx, definition).await
            }
            (JobDefinitionCatalogSyncMode::PreserveExistingEnabledForEnabledDefinitions, false)
            | (JobDefinitionCatalogSyncMode::RestoreCatalogEnabledState, _) => {
                upsert_job_definition_tx(tx, definition).await
            }
        };
        upsert_result.map_err(
            |source| JobDefinitionCatalogSyncError::DefinitionSyncFailure {
                job_type: definition.job_type.as_str().to_owned(),
                source: Box::new(source),
            },
        )?;
    }

    Ok(JobDefinitionCatalogSyncReport {
        disabled_absent_job_types: Vec::new(),
        disabled_catalog_job_types,
    })
}

pub async fn sync_catalog_job_definitions_exact_tx(
    tx: &mut DbTx<'_>,
    definitions: &[JobDefinitionUpsert<'_>],
    scope_job_types: &[JobTypeName],
) -> std::result::Result<JobDefinitionCatalogSyncReport, JobDefinitionCatalogSyncError> {
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
    let requires_disable_guard = !disabled_job_types.is_empty() || has_absent_scope_job_types;
    if requires_disable_guard {
        prepare_definition_disable_critical_section_tx(tx).await?;
    }

    let disabled_catalog_job_types = if disabled_job_types.is_empty() {
        Vec::new()
    } else {
        reject_active_schedules_for_disabled_job_types_tx(tx, &disabled_job_types).await?;
        list_job_types_missing_or_enabled_definitions_tx(tx, &disabled_job_types)
            .await
            .map_err(|error| {
                JobDefinitionCatalogSyncError::DefinitionInspectFailure(Box::new(error))
            })?
    };

    if has_absent_scope_job_types {
        if let Some(reference) = find_active_schedule_for_enabled_absent_job_types_tx(
            tx,
            &catalog_job_types,
            scope_job_types,
        )
        .await
        .map_err(|error| JobDefinitionCatalogSyncError::ScheduleCheckFailure(Box::new(error)))?
        {
            return Err(JobDefinitionCatalogSyncError::ActiveScheduleForAbsentJobType(reference));
        }
    }

    // Re-enabling catalog definitions does not need the disable guard because it
    // cannot orphan active schedules or authorize work for a row being disabled.
    for definition in definitions {
        upsert_job_definition_tx(tx, definition)
            .await
            .map_err(
                |source| JobDefinitionCatalogSyncError::DefinitionSyncFailure {
                    job_type: definition.job_type.as_str().to_owned(),
                    source: Box::new(source),
                },
            )?;
    }

    let disabled_absent_job_types = if has_absent_scope_job_types {
        disable_enabled_job_definitions_except_tx(tx, &catalog_job_types, scope_job_types)
            .await
            .map_err(|error| JobDefinitionCatalogSyncError::DisableAbsentFailure(Box::new(error)))?
    } else {
        Vec::new()
    };

    Ok(JobDefinitionCatalogSyncReport {
        disabled_absent_job_types,
        disabled_catalog_job_types,
    })
}

pub async fn upsert_job_definition_tx(
    tx: &mut DbTx<'_>,
    payload: &JobDefinitionUpsert<'_>,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_definitions (
            job_type,
            version,
            max_attempts,
            default_timeout_seconds,
            default_priority,
            is_enabled
         )
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (job_type)
         DO UPDATE
            SET version = EXCLUDED.version,
                max_attempts = EXCLUDED.max_attempts,
                default_timeout_seconds = EXCLUDED.default_timeout_seconds,
                default_priority = EXCLUDED.default_priority,
                is_enabled = EXCLUDED.is_enabled,
                updated_at = now()
          WHERE job_definitions.version IS DISTINCT FROM EXCLUDED.version
             OR job_definitions.max_attempts IS DISTINCT FROM EXCLUDED.max_attempts
             OR job_definitions.default_timeout_seconds IS DISTINCT FROM EXCLUDED.default_timeout_seconds
             OR job_definitions.default_priority IS DISTINCT FROM EXCLUDED.default_priority
             OR job_definitions.is_enabled IS DISTINCT FROM EXCLUDED.is_enabled",
        payload.job_type as _,
        payload.version,
        payload.max_attempts,
        payload.default_timeout_seconds,
        payload.default_priority,
        payload.is_enabled,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("upsert job definition", error))?;

    Ok(())
}

/// Upserts a job definition while preserving an existing row's `is_enabled`.
///
/// Inserts use `payload.is_enabled`; updates keep the stored enabled state and
/// refresh only the catalog-owned version, retry, timeout, and priority fields.
async fn upsert_job_definition_preserving_enabled_tx(
    tx: &mut DbTx<'_>,
    payload: &JobDefinitionUpsert<'_>,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_definitions (
            job_type,
            version,
            max_attempts,
            default_timeout_seconds,
            default_priority,
            is_enabled
         )
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (job_type)
         DO UPDATE
            SET version = EXCLUDED.version,
                max_attempts = EXCLUDED.max_attempts,
                default_timeout_seconds = EXCLUDED.default_timeout_seconds,
                default_priority = EXCLUDED.default_priority,
                is_enabled = job_definitions.is_enabled,
                updated_at = now()
          WHERE job_definitions.version IS DISTINCT FROM EXCLUDED.version
             OR job_definitions.max_attempts IS DISTINCT FROM EXCLUDED.max_attempts
             OR job_definitions.default_timeout_seconds IS DISTINCT FROM EXCLUDED.default_timeout_seconds
             OR job_definitions.default_priority IS DISTINCT FROM EXCLUDED.default_priority",
        payload.job_type as _,
        payload.version,
        payload.max_attempts,
        payload.default_timeout_seconds,
        payload.default_priority,
        // Used only by the INSERT path; conflicts preserve the stored value.
        payload.is_enabled,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("upsert job definition preserving enabled", error)
    })?;

    Ok(())
}

async fn lock_job_schedules_for_definition_disable_tx(tx: &mut DbTx<'_>) -> Result<()> {
    let previous_lock_timeout = cap_local_lock_timeout_tx(
        tx,
        DEFINITION_DISABLE_LOCK_TIMEOUT,
        DEFINITION_DISABLE_LOCK_TIMEOUT_MS,
        "set job definition disable schedule lock timeout",
    )
    .await?;

    let lock_result = sqlx::query!("LOCK TABLE job_schedules IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut **tx)
        .await;

    match lock_result {
        Ok(_) => {
            set_local_lock_timeout_tx(
                tx,
                &previous_lock_timeout,
                "restore job definition disable schedule lock timeout",
            )
            .await
        }
        Err(error) => {
            // No restore is needed on the error path: the caller rolls back the
            // transaction and PostgreSQL discards the SET LOCAL lock_timeout.
            Err(Error::from_query_sqlx_with_context(
                "lock job schedules before disabling job definitions",
                error,
            ))
        }
    }
}

async fn lock_job_definitions_for_definition_disable_tx(tx: &mut DbTx<'_>) -> Result<()> {
    let previous_lock_timeout = cap_local_lock_timeout_tx(
        tx,
        DEFINITION_DISABLE_LOCK_TIMEOUT,
        DEFINITION_DISABLE_LOCK_TIMEOUT_MS,
        "set job definition disable definition lock timeout",
    )
    .await?;

    let lock_result = sqlx::query("LOCK TABLE job_definitions IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut **tx)
        .await;

    match lock_result {
        Ok(_) => {
            set_local_lock_timeout_tx(
                tx,
                &previous_lock_timeout,
                "restore job definition disable definition lock timeout",
            )
            .await
        }
        Err(error) => {
            // No restore is needed on the error path: the caller rolls back the
            // transaction and PostgreSQL discards the SET LOCAL lock_timeout.
            Err(Error::from_query_sqlx_with_context(
                "lock job definitions before disabling job definitions",
                error,
            ))
        }
    }
}

async fn find_active_schedule_for_job_types_tx(
    tx: &mut DbTx<'_>,
    job_types: &[JobTypeName],
) -> Result<Option<JobScheduleJobTypeReference>> {
    let job_types = job_type_strings(job_types);
    let row = sqlx::query!(
        "SELECT name, job_type
         FROM job_schedules
         WHERE is_active = true
           AND job_type = ANY($1::text[])
         ORDER BY name ASC
         LIMIT 1",
        job_types.as_slice(),
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("find active schedule for job definitions", error)
    })?;

    row.map(|row| parse_schedule_job_type_reference(row.name, row.job_type))
        .transpose()
}

async fn find_active_schedule_for_enabled_absent_job_types_tx(
    tx: &mut DbTx<'_>,
    catalog_job_types: &[JobTypeName],
    scope_job_types: &[JobTypeName],
) -> Result<Option<JobScheduleJobTypeReference>> {
    let catalog_job_types = job_type_strings(catalog_job_types);
    let scope_job_types = job_type_strings(scope_job_types);
    let row = sqlx::query!(
        "SELECT job_schedules.name, job_schedules.job_type
         FROM job_schedules
         INNER JOIN job_definitions
            ON job_definitions.job_type = job_schedules.job_type
         WHERE job_schedules.is_active = true
           AND job_schedules.job_type <> ALL($1::text[])
           AND job_schedules.job_type = ANY($2::text[])
           AND job_definitions.is_enabled = true
         ORDER BY job_schedules.name ASC
         LIMIT 1",
        catalog_job_types.as_slice(),
        scope_job_types.as_slice(),
    )
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context(
            "find active schedule for enabled absent job definitions",
            error,
        )
    })?;

    row.map(|row| parse_schedule_job_type_reference(row.name, row.job_type))
        .transpose()
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

pub async fn insert_job_definition_if_missing_tx(
    tx: &mut DbTx<'_>,
    payload: &JobDefinitionUpsert<'_>,
) -> Result<()> {
    sqlx::query!(
        "INSERT INTO job_definitions (
            job_type,
            version,
            max_attempts,
            default_timeout_seconds,
            default_priority,
            is_enabled
         )
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (job_type)
         DO NOTHING",
        payload.job_type as _,
        payload.version,
        payload.max_attempts,
        payload.default_timeout_seconds,
        payload.default_priority,
        payload.is_enabled,
    )
    .execute(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("insert job definition if missing", error)
    })?;

    Ok(())
}

pub async fn list_job_definitions(
    pool: &DbPool,
    filter: &JobDefinitionListFilter<'_>,
) -> Result<Vec<JobDefinitionRecord>> {
    let escaped_job_type = filter.job_type.map(escape_ilike_pattern);

    let rows = sqlx::query!(
        "SELECT
            job_type,
            version,
            max_attempts,
            default_timeout_seconds,
            default_priority,
            is_enabled,
            created_at,
            updated_at
         FROM job_definitions
         WHERE ($1::text IS NULL OR job_type ILIKE '%' || $1 || '%')
         ORDER BY job_type ASC
         LIMIT $2
         OFFSET $3",
        escaped_job_type.as_deref(),
        filter.limit,
        filter.offset,
    )
    .fetch_all(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("list job definitions", error))?;

    rows.into_iter()
        .map(|row| {
            Ok(JobDefinitionRecord {
                job_type: parse_job_type_name(row.job_type)?,
                version: row.version,
                max_attempts: row.max_attempts,
                default_timeout_seconds: row.default_timeout_seconds,
                default_priority: row.default_priority,
                is_enabled: row.is_enabled,
                created_at: row.created_at,
                updated_at: row.updated_at,
            })
        })
        .collect()
}

fn escape_ilike_pattern(input: &str) -> String {
    input
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
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
    cap_local_statement_timeout_tx(
        tx,
        DEFINITION_DISABLE_STATEMENT_TIMEOUT,
        DEFINITION_DISABLE_STATEMENT_TIMEOUT_MS,
        "set job definition disable statement timeout",
    )
    .await
    .map_err(|error| {
        JobDefinitionCatalogSyncError::CriticalSectionTimeoutFailure(Box::new(error))
    })?;
    lock_job_schedules_for_definition_disable_tx(tx)
        .await
        .map_err(|error| JobDefinitionCatalogSyncError::ScheduleLockFailure(Box::new(error)))?;
    lock_job_definitions_for_definition_disable_tx(tx)
        .await
        .map_err(|error| JobDefinitionCatalogSyncError::DefinitionLockFailure(Box::new(error)))
}

async fn reject_active_schedules_for_disabled_job_types_tx(
    tx: &mut DbTx<'_>,
    job_types: &[JobTypeName],
) -> std::result::Result<(), JobDefinitionCatalogSyncError> {
    if let Some(reference) = find_active_schedule_for_job_types_tx(tx, job_types)
        .await
        .map_err(|error| JobDefinitionCatalogSyncError::ScheduleCheckFailure(Box::new(error)))?
    {
        return Err(JobDefinitionCatalogSyncError::ActiveScheduleForDisabledJobType(reference));
    }

    Ok(())
}

fn parse_schedule_job_type_reference(
    schedule_name: String,
    job_type: String,
) -> Result<JobScheduleJobTypeReference> {
    Ok(JobScheduleJobTypeReference {
        schedule_name,
        job_type: parse_job_type_name(job_type)?,
    })
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

async fn cap_local_lock_timeout_tx(
    tx: &mut DbTx<'_>,
    lock_timeout: &str,
    lock_timeout_ms: i64,
    context: &'static str,
) -> Result<String> {
    // Preserve PostgreSQL's reported GUC text so restore keeps units and special
    // values such as "0" exactly as the connection reported them.
    sqlx::query_scalar::<_, String>(
        "WITH previous AS MATERIALIZED (
             SELECT
                current_setting('lock_timeout') AS lock_timeout,
                setting::bigint AS lock_timeout_ms
             FROM pg_settings
             WHERE name = 'lock_timeout'
         )
         SELECT previous.lock_timeout
         FROM previous,
              LATERAL (
                SELECT set_config(
                    'lock_timeout',
                    CASE
                        WHEN previous.lock_timeout_ms = 0 THEN $1
                        WHEN previous.lock_timeout_ms <= $2 THEN previous.lock_timeout
                        ELSE $1
                    END,
                    true
                )
              ) AS applied",
    )
    .bind(lock_timeout)
    .bind(lock_timeout_ms)
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(context, error))
}

async fn cap_local_statement_timeout_tx(
    tx: &mut DbTx<'_>,
    statement_timeout: &str,
    statement_timeout_ms: i64,
    context: &'static str,
) -> Result<String> {
    // Cap statement_timeout while the transaction holds definition-disable locks
    // so a stalled check, upsert, or disable statement cannot hold table locks
    // indefinitely. Preserve stricter caller settings.
    sqlx::query_scalar::<_, String>(
        "WITH previous AS MATERIALIZED (
             SELECT
                current_setting('statement_timeout') AS statement_timeout,
                setting::bigint AS statement_timeout_ms
             FROM pg_settings
             WHERE name = 'statement_timeout'
         )
         SELECT previous.statement_timeout
         FROM previous,
              LATERAL (
                SELECT set_config(
                    'statement_timeout',
                    CASE
                        WHEN previous.statement_timeout_ms = 0 THEN $1
                        WHEN previous.statement_timeout_ms <= $2 THEN previous.statement_timeout
                        ELSE $1
                    END,
                    true
                )
              ) AS applied",
    )
    .bind(statement_timeout)
    .bind(statement_timeout_ms)
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context(context, error))
}

async fn set_local_lock_timeout_tx(
    tx: &mut DbTx<'_>,
    lock_timeout: &str,
    context: &'static str,
) -> Result<()> {
    sqlx::query_scalar::<_, String>("SELECT set_config('lock_timeout', $1, true)")
        .bind(lock_timeout)
        .fetch_one(&mut **tx)
        .await
        .map_err(|error| Error::from_query_sqlx_with_context(context, error))?;

    Ok(())
}

pub async fn get_job_definition_by_type(
    pool: &DbPool,
    job_type: JobType<'_>,
) -> Result<Option<JobDefinitionRecord>> {
    let row = sqlx::query!(
        "SELECT
            job_type,
            version,
            max_attempts,
            default_timeout_seconds,
            default_priority,
            is_enabled,
            created_at,
            updated_at
         FROM job_definitions
         WHERE job_type = $1
         LIMIT 1",
        job_type as _,
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("get job definition by type", error))?;

    row.map(|row| {
        Ok(JobDefinitionRecord {
            job_type: parse_job_type_name(row.job_type)?,
            version: row.version,
            max_attempts: row.max_attempts,
            default_timeout_seconds: row.default_timeout_seconds,
            default_priority: row.default_priority,
            is_enabled: row.is_enabled,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    })
    .transpose()
}

pub async fn update_job_definition(
    pool: &DbPool,
    job_type: JobType<'_>,
    payload: &JobDefinitionUpdate,
) -> Result<Option<JobDefinitionRecord>> {
    let row = sqlx::query!(
        "UPDATE job_definitions
         SET max_attempts = COALESCE($2, max_attempts),
             default_timeout_seconds = COALESCE($3, default_timeout_seconds),
             default_priority = COALESCE($4, default_priority),
             is_enabled = COALESCE($5, is_enabled),
             updated_at = now()
         WHERE job_type = $1
         RETURNING
            job_type,
            version,
            max_attempts,
            default_timeout_seconds,
            default_priority,
            is_enabled,
            created_at,
            updated_at",
        job_type as _,
        payload.max_attempts,
        payload.default_timeout_seconds,
        payload.default_priority,
        payload.is_enabled,
    )
    .fetch_optional(pool)
    .await
    .map_err(|error| Error::from_query_sqlx_with_context("update job definition", error))?;

    row.map(|row| {
        Ok(JobDefinitionRecord {
            job_type: parse_job_type_name(row.job_type)?,
            version: row.version,
            max_attempts: row.max_attempts,
            default_timeout_seconds: row.default_timeout_seconds,
            default_priority: row.default_priority,
            is_enabled: row.is_enabled,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    })
    .transpose()
}
