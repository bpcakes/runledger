use crate::{DbTx, Error, Result};

use super::super::schedule_definition_guard::{self, GuardLockContext};
use super::super::types::{
    JobScheduleCatalogSyncEntry, JobScheduleCatalogSyncReport, JobScheduleRecord, JobScheduleUpsert,
};
use super::persistence::{ScheduleActiveStatePolicy, persist_job_schedule_tx};
use super::validation::validate_job_schedule_upsert;

/// Upserts catalog-owned schedules inside an existing transaction.
///
/// On conflict, catalog sync preserves insert-only fields such as
/// `organization_id`, preserves the scheduler cursor unless the cron expression
/// changes, and applies each entry's `is_active` value as the desired active
/// state. This differs from [`crate::jobs::upsert_job_schedule_tx`], which intentionally
/// preserves the stored active state on conflict.
///
/// Entries are upserted one at a time so callers can add per-entry error context
/// around failures.
///
/// # Errors
/// Returns an error if any entry fails validation or persistence. The caller is
/// responsible for adding per-entry context if it needs to report the failing
/// schedule name.
pub async fn sync_catalog_job_schedules_tx(
    tx: &mut DbTx<'_>,
    entries: &[JobScheduleCatalogSyncEntry<'_>],
) -> Result<JobScheduleCatalogSyncReport> {
    let mut synced_schedule_names = Vec::with_capacity(entries.len());
    for entry in entries {
        let schedule = upsert_catalog_job_schedule_tx(tx, &entry.upsert).await?;
        synced_schedule_names.push(schedule.name);
    }

    Ok(JobScheduleCatalogSyncReport {
        synced_schedule_names,
    })
}

async fn upsert_catalog_job_schedule_tx(
    tx: &mut DbTx<'_>,
    payload: &JobScheduleUpsert<'_>,
) -> Result<JobScheduleRecord> {
    validate_job_schedule_upsert(payload)?;

    if payload.is_active {
        schedule_definition_guard::lock_schedules_then_definitions_tx(
            tx,
            GuardLockContext::ActiveScheduleWrite,
        )
        .await
        .map_err(schedule_definition_guard::ScheduleDefinitionLockError::into_error)?;
        schedule_definition_guard::reject_unavailable_definition_for_active_schedule_tx(
            tx,
            payload.job_type.as_str(),
        )
        .await?;
    }

    persist_job_schedule_tx(
        tx,
        payload,
        ScheduleActiveStatePolicy::ApplyRequested,
        "sync catalog job schedule",
    )
    .await
}

/// Deactivates enabled schedules whose names are in `scope_names` but absent
/// from `present_names`.
///
/// Schedules outside `scope_names` are never modified.
///
/// # Errors
/// Returns an error if PostgreSQL rejects the update.
pub async fn deactivate_schedules_absent_from_names_tx(
    tx: &mut DbTx<'_>,
    scope_names: &[String],
    present_names: &[String],
) -> Result<Vec<String>> {
    if scope_names.is_empty() {
        return Ok(Vec::new());
    }

    let mut rows = sqlx::query_scalar::<_, String>(
        "UPDATE job_schedules
         SET is_active = false,
             updated_at = now()
         WHERE is_active = true
           AND name = ANY($1::text[])
           AND name <> ALL($2::text[])
         RETURNING name",
    )
    .bind(scope_names)
    .bind(present_names)
    .fetch_all(&mut **tx)
    .await
    .map_err(|error| {
        Error::from_query_sqlx_with_context("deactivate absent catalog schedules", error)
    })?;

    rows.sort();
    Ok(rows)
}
