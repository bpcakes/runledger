use runledger_core::jobs::JobTypeName;

use crate::{DbTx, Error, QueryError, QueryErrorCategory, Result};

use super::row_decode::parse_job_type_name;
use super::transaction_settings::{cap_local_lock_timeout_tx, set_local_lock_timeout_tx};
use super::types::JobScheduleJobTypeReference;

// All cross-table guards acquire job_schedules before job_definitions. Keep new
// callers in that order so active-schedule writes and definition disables cannot
// wait on the same tables in opposite directions.
const SCHEDULE_DEFINITION_GUARD_LOCK_TIMEOUT: &str = "5s";
const SCHEDULE_DEFINITION_GUARD_LOCK_TIMEOUT_MS: i64 = 5_000;
const DEFINITION_DISABLE_STATEMENT_TIMEOUT: &str = "30s";
const DEFINITION_DISABLE_STATEMENT_TIMEOUT_MS: i64 = 30_000;

#[derive(Clone, Copy)]
pub(in crate::jobs) enum GuardLockContext {
    ActiveScheduleWrite,
    DefinitionDisable,
}

pub(in crate::jobs) enum ScheduleDefinitionLockError {
    Schedule(Error),
    Definition(Error),
}

impl ScheduleDefinitionLockError {
    pub(in crate::jobs) fn into_error(self) -> Error {
        match self {
            Self::Schedule(error) | Self::Definition(error) => error,
        }
    }
}

impl GuardLockContext {
    fn set_schedule_lock_timeout_context(self) -> &'static str {
        match self {
            Self::ActiveScheduleWrite => "set active schedule guard schedule lock timeout",
            Self::DefinitionDisable => "set job definition disable schedule lock timeout",
        }
    }

    fn restore_schedule_lock_timeout_context(self) -> &'static str {
        match self {
            Self::ActiveScheduleWrite => "restore active schedule guard schedule lock timeout",
            Self::DefinitionDisable => "restore job definition disable schedule lock timeout",
        }
    }

    fn schedule_lock_context(self) -> &'static str {
        match self {
            Self::ActiveScheduleWrite => {
                "lock job schedules before active schedule definition check"
            }
            Self::DefinitionDisable => "lock job schedules before disabling job definitions",
        }
    }

    fn set_definition_lock_timeout_context(self) -> &'static str {
        match self {
            Self::ActiveScheduleWrite => "set active schedule guard definition lock timeout",
            Self::DefinitionDisable => "set job definition disable definition lock timeout",
        }
    }

    fn restore_definition_lock_timeout_context(self) -> &'static str {
        match self {
            Self::ActiveScheduleWrite => "restore active schedule guard definition lock timeout",
            Self::DefinitionDisable => "restore job definition disable definition lock timeout",
        }
    }

    fn definition_lock_context(self) -> &'static str {
        match self {
            Self::ActiveScheduleWrite => {
                "lock job definitions before active schedule definition check"
            }
            Self::DefinitionDisable => "lock job definitions before disabling job definitions",
        }
    }
}

pub(in crate::jobs) async fn cap_definition_disable_statement_timeout_tx(
    tx: &mut DbTx<'_>,
) -> Result<()> {
    cap_local_statement_timeout_tx(
        tx,
        DEFINITION_DISABLE_STATEMENT_TIMEOUT,
        DEFINITION_DISABLE_STATEMENT_TIMEOUT_MS,
        "set job definition disable statement timeout",
    )
    .await?;
    Ok(())
}

pub(in crate::jobs) async fn lock_schedules_then_definitions_tx(
    tx: &mut DbTx<'_>,
    context: GuardLockContext,
) -> std::result::Result<(), ScheduleDefinitionLockError> {
    lock_job_schedules_for_guard_tx(tx, context)
        .await
        .map_err(ScheduleDefinitionLockError::Schedule)?;
    lock_job_definitions_for_guard_tx(tx, context)
        .await
        .map_err(ScheduleDefinitionLockError::Definition)
}

pub(in crate::jobs) async fn lock_job_schedules_for_guard_tx(
    tx: &mut DbTx<'_>,
    context: GuardLockContext,
) -> Result<()> {
    let previous_lock_timeout = cap_local_lock_timeout_tx(
        tx,
        SCHEDULE_DEFINITION_GUARD_LOCK_TIMEOUT,
        SCHEDULE_DEFINITION_GUARD_LOCK_TIMEOUT_MS,
        context.set_schedule_lock_timeout_context(),
    )
    .await?;

    let lock_result = sqlx::query("LOCK TABLE job_schedules IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut **tx)
        .await;

    match lock_result {
        Ok(_) => {
            set_local_lock_timeout_tx(
                tx,
                &previous_lock_timeout,
                context.restore_schedule_lock_timeout_context(),
            )
            .await
        }
        Err(error) => Err(Error::from_query_sqlx_with_context(
            context.schedule_lock_context(),
            error,
        )),
    }
}

pub(in crate::jobs) async fn lock_job_definitions_for_guard_tx(
    tx: &mut DbTx<'_>,
    context: GuardLockContext,
) -> Result<()> {
    let previous_lock_timeout = cap_local_lock_timeout_tx(
        tx,
        SCHEDULE_DEFINITION_GUARD_LOCK_TIMEOUT,
        SCHEDULE_DEFINITION_GUARD_LOCK_TIMEOUT_MS,
        context.set_definition_lock_timeout_context(),
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
                context.restore_definition_lock_timeout_context(),
            )
            .await
        }
        Err(error) => Err(Error::from_query_sqlx_with_context(
            context.definition_lock_context(),
            error,
        )),
    }
}

pub(in crate::jobs) async fn reject_unavailable_definition_for_active_schedule_tx(
    tx: &mut DbTx<'_>,
    job_type: &str,
) -> Result<()> {
    if enabled_job_definition_exists_tx(tx, job_type).await? {
        return Ok(());
    }

    Err(active_schedule_definition_unavailable_error(job_type))
}

async fn enabled_job_definition_exists_tx(tx: &mut DbTx<'_>, job_type: &str) -> Result<bool> {
    let is_enabled = sqlx::query_scalar::<_, bool>(
        "SELECT is_enabled
         FROM job_definitions
         WHERE job_type = $1",
    )
    .bind(job_type)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context(
            "check job definition before active schedule write",
            error,
        )
    })?;

    Ok(is_enabled == Some(true))
}

pub(in crate::jobs) async fn find_active_schedule_for_job_type_tx(
    tx: &mut DbTx<'_>,
    job_type: &str,
) -> Result<Option<JobScheduleJobTypeReference>> {
    let row = sqlx::query_as::<_, (String, String)>(
        "SELECT name, job_type
         FROM job_schedules
         WHERE is_active = true
           AND job_type = $1
         ORDER BY name ASC
         LIMIT 1",
    )
    .bind(job_type)
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("find active schedule for job definition", error)
    })?;

    row.map(|(name, job_type)| parse_schedule_job_type_reference(name, job_type))
        .transpose()
}

pub(in crate::jobs) async fn find_active_schedule_for_job_types_tx(
    tx: &mut DbTx<'_>,
    job_types: &[JobTypeName],
) -> Result<Option<JobScheduleJobTypeReference>> {
    if job_types.is_empty() {
        return Ok(None);
    }

    let job_types = job_type_strings(job_types);
    let row = sqlx::query_as::<_, (String, String)>(
        "SELECT name, job_type
         FROM job_schedules
         WHERE is_active = true
           AND job_type = ANY($1::text[])
         ORDER BY name ASC
         LIMIT 1",
    )
    .bind(job_types.as_slice())
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("find active schedule for job definitions", error)
    })?;

    row.map(|(name, job_type)| parse_schedule_job_type_reference(name, job_type))
        .transpose()
}

pub(in crate::jobs) async fn find_active_schedule_for_enabled_absent_job_types_tx(
    tx: &mut DbTx<'_>,
    catalog_job_types: &[JobTypeName],
    scope_job_types: &[JobTypeName],
) -> Result<Option<JobScheduleJobTypeReference>> {
    if scope_job_types.is_empty() {
        return Ok(None);
    }

    let catalog_job_types = job_type_strings(catalog_job_types);
    let scope_job_types = job_type_strings(scope_job_types);
    let row = sqlx::query_as::<_, (String, String)>(
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
    )
    .bind(catalog_job_types.as_slice())
    .bind(scope_job_types.as_slice())
    .fetch_optional(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context(
            "find active schedule for enabled absent job definitions",
            error,
        )
    })?;

    row.map(|(name, job_type)| parse_schedule_job_type_reference(name, job_type))
        .transpose()
}

fn active_schedule_definition_unavailable_error(job_type: &str) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "job_schedule.definition_not_found_or_disabled",
        "Active job schedules require an enabled job definition.",
        format!("active job schedule references missing or disabled job definition: {job_type}"),
    ))
}

pub(in crate::jobs) fn active_schedule_for_disabled_definition_error(
    reference: &JobScheduleJobTypeReference,
) -> Error {
    Error::QueryError(QueryError::from_classified(
        QueryErrorCategory::Validation,
        "job_definition.active_schedule_exists",
        "Job definition cannot be disabled while active schedules reference it.",
        format!(
            "active schedule {} still references job type {}",
            reference.schedule_name, reference.job_type
        ),
    ))
}

async fn cap_local_statement_timeout_tx(
    tx: &mut DbTx<'_>,
    statement_timeout: &str,
    statement_timeout_ms: i64,
    context: &'static str,
) -> Result<String> {
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

fn job_type_strings(job_types: &[JobTypeName]) -> Vec<String> {
    job_types
        .iter()
        .map(|job_type| job_type.as_str().to_owned())
        .collect()
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
