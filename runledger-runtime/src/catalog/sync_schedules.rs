use chrono::Utc;
use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    JobScheduleCatalogSyncEntry, JobScheduleUpsert, deactivate_schedules_absent_from_names_tx,
    prepare_schedule_exact_sync_critical_section_tx, sync_catalog_job_schedules_tx,
};

use super::schedule_spec::CatalogJobScheduleSpec;
use super::{CatalogError, JobCatalog, JobCatalogScheduleSyncReport, JobCatalogScheduleSyncScope};

enum ScheduleSyncMode<'a> {
    Additive,
    Exact {
        scope: &'a JobCatalogScheduleSyncScope,
    },
}

impl JobCatalog {
    /// Builds an exact-sync scope from schedules registered with [`Self::schedule`].
    ///
    /// Use this with [`Self::sync_schedules_exact`] when the registered
    /// schedules are the source of truth for their owned name set. The helper
    /// avoids duplicating schedule names in startup code.
    ///
    /// # Errors
    /// Returns [`CatalogError::InvalidExactScheduleSyncScope`] when the catalog
    /// has no registered schedules.
    pub fn schedule_sync_scope(&self) -> Result<JobCatalogScheduleSyncScope, CatalogError> {
        JobCatalogScheduleSyncScope::schedule_names(
            self.schedules.iter().map(|stored| stored.name.clone()),
        )
    }

    /// Upserts every catalog-registered schedule and applies each spec's
    /// `is_active` value.
    ///
    /// Use this for static schedules registered with [`Self::schedule`] next to
    /// their handler registration. It does not sync caller-provided startup
    /// specs; use [`Self::sync_schedules_with`] for those.
    ///
    /// Call [`Self::sync_definitions`] before this method so referenced job
    /// definitions exist. Safe to call repeatedly on worker startup. Existing
    /// rows keep their `organization_id`, keep `next_fire_at` unless the cron
    /// expression changes, and have `is_active` overwritten from the registered
    /// spec on every sync. Use lower-level schedule APIs for schedules whose
    /// active state should be owned by an admin pause/resume workflow instead of
    /// startup catalog sync.
    ///
    /// Sync applies one schedule upsert at a time so persistence errors can be
    /// reported with the schedule name that failed. Keep catalog-owned schedule
    /// sets on the order of dozens, not thousands; use a custom lower-level
    /// setup path for very large schedule inventories.
    ///
    /// # Errors
    /// Returns [`CatalogError`] when schedule validation fails, a schedule
    /// references an unknown or disabled catalog job type, or persistence fails.
    pub async fn sync_schedules(
        &self,
        pool: &DbPool,
    ) -> Result<JobCatalogScheduleSyncReport, CatalogError> {
        let specs: Vec<CatalogJobScheduleSpec<'_>> = self
            .schedules
            .iter()
            .map(|stored| stored.as_spec())
            .collect();
        self.sync_schedules_impl(pool, &specs, ScheduleSyncMode::Additive)
            .await
    }

    /// Upserts caller-provided schedule specs and applies each spec's
    /// `is_active` value.
    ///
    /// Use this for schedule specs assembled at startup from config, feature
    /// flags, tenants, or another source outside the builder chain. This syncs
    /// only `specs`; it does not include schedules registered with
    /// [`Self::schedule`]. Existing rows keep their `organization_id`, and keep
    /// `next_fire_at` unless the cron expression changes. Existing rows have
    /// `is_active` overwritten from the provided spec on every sync.
    ///
    /// Use lower-level schedule APIs for schedules whose active state should be
    /// owned by an admin pause/resume workflow instead of startup catalog sync.
    /// Sync applies one schedule upsert at a time so persistence errors can be
    /// reported with the schedule name that failed. Keep catalog-owned schedule
    /// sets on the order of dozens, not thousands; use a custom lower-level
    /// setup path for very large schedule inventories.
    ///
    /// # Errors
    /// Returns [`CatalogError`] when schedule validation fails, a schedule
    /// references an unknown or disabled catalog job type, duplicate names are
    /// supplied, or persistence fails.
    pub async fn sync_schedules_with<'a>(
        &self,
        pool: &DbPool,
        specs: &[CatalogJobScheduleSpec<'a>],
    ) -> Result<JobCatalogScheduleSyncReport, CatalogError> {
        self.sync_schedules_impl(pool, specs, ScheduleSyncMode::Additive)
            .await
    }

    /// Upserts catalog-registered schedules, then deactivates enabled schedules
    /// in `scope` whose names are absent from the synced spec set.
    ///
    /// Use this when registered catalog schedules are the active source of truth
    /// for a bounded deployment-owned schedule-name scope.
    ///
    /// Every registered schedule name must be included in `scope`. Exact sync
    /// takes a bounded PostgreSQL lock around the upsert/deactivation window so
    /// overlapping exact syncs cannot interleave their active schedule sets.
    /// Scheduler claims and fire-cursor updates can also wait briefly behind
    /// this lock, bounded by the exact-sync transaction settings.
    /// Exact sync still applies each present schedule's `is_active` value on
    /// every sync; absent active schedules inside `scope` are deactivated.
    /// Keep ownership scopes deployment-stable during rolling deploys. For a
    /// feature-flagged schedule, prefer registering the schedule with
    /// `is_active: false` over omitting it from the catalog, so exact sync still
    /// owns and can deactivate the row.
    ///
    /// # Errors
    /// Returns [`CatalogError`] when any schedule is outside `scope`, schedule
    /// validation fails, a schedule references an unknown or disabled catalog
    /// job type, lock acquisition fails, or persistence fails.
    pub async fn sync_schedules_exact(
        &self,
        pool: &DbPool,
        scope: &JobCatalogScheduleSyncScope,
    ) -> Result<JobCatalogScheduleSyncReport, CatalogError> {
        let specs: Vec<CatalogJobScheduleSpec<'_>> = self
            .schedules
            .iter()
            .map(|stored| stored.as_spec())
            .collect();
        self.sync_schedules_impl(pool, &specs, ScheduleSyncMode::Exact { scope })
            .await
    }

    /// Upserts caller-provided schedule specs, then deactivates enabled
    /// schedules in `scope` whose names are absent from the synced spec set.
    ///
    /// Use this when startup-provided specs are the active source of truth for a
    /// bounded deployment-owned schedule-name scope.
    ///
    /// This exact syncs only `specs`; it does not include schedules registered
    /// with [`Self::schedule`]. Every provided schedule name must be included in
    /// `scope`. Passing an empty `specs` slice deactivates every enabled
    /// schedule in the owned scope. Exact sync still applies each present
    /// schedule's `is_active` value on every sync. Scheduler claims and
    /// fire-cursor updates can wait briefly behind the exact-sync table lock,
    /// bounded by the exact-sync transaction settings.
    /// Keep ownership scopes deployment-stable during rolling deploys; when a
    /// dynamic source disables a schedule, keep its name in `scope` if exact
    /// sync should deactivate the stored row.
    ///
    /// # Errors
    /// Returns [`CatalogError`] when any schedule is outside `scope`, schedule
    /// validation fails, a schedule references an unknown or disabled catalog
    /// job type, duplicate names are supplied, lock acquisition fails, or
    /// persistence fails.
    pub async fn sync_schedules_exact_with<'a>(
        &self,
        pool: &DbPool,
        scope: &JobCatalogScheduleSyncScope,
        specs: &[CatalogJobScheduleSpec<'a>],
    ) -> Result<JobCatalogScheduleSyncReport, CatalogError> {
        self.sync_schedules_impl(pool, specs, ScheduleSyncMode::Exact { scope })
            .await
    }

    async fn sync_schedules_impl<'a>(
        &self,
        pool: &DbPool,
        specs: &[CatalogJobScheduleSpec<'a>],
        mode: ScheduleSyncMode<'_>,
    ) -> Result<JobCatalogScheduleSyncReport, CatalogError> {
        let entries = self.materialize_schedule_sync_entries(specs)?;
        if let ScheduleSyncMode::Exact { scope } = &mode {
            Self::validate_exact_schedule_sync_scope(scope, &entries)?;
        }

        // Exact sync with empty specs still needs a transaction so it can
        // deactivate every active schedule in the owned scope.
        if entries.is_empty() && matches!(&mode, ScheduleSyncMode::Additive) {
            return Ok(JobCatalogScheduleSyncReport {
                synced_schedule_names: Vec::new(),
                deactivated_absent_schedule_names: Vec::new(),
            });
        }

        let mut tx = pool.begin().await.map_err(|error| {
            CatalogError::ScheduleSyncFailure(Box::new(
                runledger_postgres::Error::from_query_sqlx_with_context(
                    "begin job catalog schedule sync",
                    error,
                ),
            ))
        })?;

        if matches!(&mode, ScheduleSyncMode::Exact { .. }) {
            prepare_schedule_exact_sync_critical_section_tx(&mut tx)
                .await
                .map_err(|error| {
                    CatalogError::ScheduleSyncCriticalSectionFailure(Box::new(error))
                })?;
        }

        let mut synced_schedule_names = Vec::with_capacity(entries.len());
        for entry in &entries {
            // Keep per-entry upserts so persistence failures can name the
            // schedule that failed. Catalog schedule sets are expected to stay
            // small enough for startup-time one-by-one sync.
            let report = sync_catalog_job_schedules_tx(&mut tx, std::slice::from_ref(entry))
                .await
                .map_err(|error| CatalogError::ScheduleSyncEntryFailure {
                    name: entry.upsert.name.to_owned(),
                    source: Box::new(error),
                })?;
            synced_schedule_names.extend(report.synced_schedule_names);
        }

        let deactivated_absent_schedule_names = match mode {
            ScheduleSyncMode::Additive => Vec::new(),
            ScheduleSyncMode::Exact { scope } => deactivate_schedules_absent_from_names_tx(
                &mut tx,
                &scope.schedule_names_for_storage(),
                &synced_schedule_names,
            )
            .await
            .map_err(|error| CatalogError::DeactivateAbsentSchedulesFailure(Box::new(error)))?,
        };

        tx.commit().await.map_err(|error| {
            CatalogError::ScheduleSyncCommitFailure(Box::new(
                runledger_postgres::Error::from_query_sqlx_with_context(
                    "commit job catalog schedule sync",
                    error,
                ),
            ))
        })?;

        Ok(JobCatalogScheduleSyncReport {
            synced_schedule_names,
            deactivated_absent_schedule_names,
        })
    }

    fn validate_exact_schedule_sync_scope(
        scope: &JobCatalogScheduleSyncScope,
        entries: &[JobScheduleCatalogSyncEntry<'_>],
    ) -> Result<(), CatalogError> {
        for entry in entries {
            if !scope.contains(entry.upsert.name) {
                return Err(CatalogError::ScheduleNameOutsideExactSyncScope {
                    name: entry.upsert.name.to_owned(),
                });
            }
        }

        Ok(())
    }

    fn materialize_schedule_sync_entries<'a>(
        &self,
        specs: &[CatalogJobScheduleSpec<'a>],
    ) -> Result<Vec<JobScheduleCatalogSyncEntry<'a>>, CatalogError> {
        let mut seen_names = std::collections::BTreeSet::new();
        let now = Utc::now();
        let mut entries = Vec::with_capacity(specs.len());

        for spec in specs {
            spec.validate_shape()
                .map_err(|field| CatalogError::InvalidScheduleSpec {
                    name: spec.name.to_owned(),
                    field,
                })?;

            if !seen_names.insert(spec.name) {
                return Err(CatalogError::DuplicateScheduleNameInSyncBatch {
                    name: spec.name.to_owned(),
                });
            }

            let job_type = self.require_catalog_enabled_job_type(spec.job_type)?;
            let next_fire_at = spec.next_fire_at.unwrap_or(now);

            entries.push(JobScheduleCatalogSyncEntry {
                upsert: JobScheduleUpsert {
                    name: spec.name,
                    job_type,
                    organization_id: spec.organization_id,
                    payload_template: spec.payload_template,
                    cron_expr: spec.cron_expr,
                    is_active: spec.is_active,
                    next_fire_at,
                    max_jitter_seconds: spec.max_jitter_seconds,
                },
            });
        }

        Ok(entries)
    }
}
