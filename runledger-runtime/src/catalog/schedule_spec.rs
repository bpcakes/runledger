use std::str::FromStr;

use chrono::{DateTime, Utc};
use cron::Schedule;
use runledger_postgres::jobs::JOB_SCHEDULE_MAX_JITTER_SECONDS;
use serde_json::Value;
use uuid::Uuid;

/// Catalog-owned cron schedule definition synced to `job_schedules` at startup.
///
/// Specs are validated before database writes. Names and cron expressions must
/// be non-empty without surrounding whitespace, cron expressions must parse as
/// UTC cron schedules, and `max_jitter_seconds` must be in the inclusive range
/// `0..=`[`JOB_SCHEDULE_MAX_JITTER_SECONDS`].
///
/// # Example
/// ```rust
/// use runledger_runtime::catalog::CatalogJobScheduleSpec;
///
/// let payload = serde_json::json!({ "kind": "refresh" });
/// let spec = CatalogJobScheduleSpec {
///     name: "profiles.refresh.hourly",
///     job_type: "profiles.refresh",
///     cron_expr: "0 0 * * * *",
///     payload_template: &payload,
///     is_active: true,
///     organization_id: None,
///     max_jitter_seconds: 0,
///     next_fire_at: None,
/// };
///
/// assert_eq!(spec.name, "profiles.refresh.hourly");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogJobScheduleSpec<'a> {
    /// Unique schedule name. This is the global `job_schedules.name` key and
    /// must be non-empty without surrounding whitespace.
    pub name: &'a str,
    /// Catalog job type enqueued when the schedule fires. The job type must be
    /// registered and effectively enabled in the [`super::JobCatalog`] used for
    /// schedule registration or sync.
    pub job_type: &'a str,
    /// UTC cron expression without surrounding whitespace.
    ///
    /// On conflict, catalog sync preserves the existing `next_fire_at` cursor
    /// when this expression is unchanged. When this expression changes, the
    /// synced `next_fire_at` value becomes the stored cursor.
    pub cron_expr: &'a str,
    /// JSON payload template copied into scheduled jobs.
    pub payload_template: &'a Value,
    /// Desired active state after sync. Applications can map feature flags to
    /// this value before calling catalog schedule sync.
    pub is_active: bool,
    /// Optional organization scope copied into jobs for a new schedule.
    pub organization_id: Option<Uuid>,
    /// Maximum deterministic jitter, in seconds, applied to future fire times.
    /// Must be between `0` and [`JOB_SCHEDULE_MAX_JITTER_SECONDS`], inclusive.
    pub max_jitter_seconds: i32,
    /// Next UTC instant at which the schedule is due. Defaults to `Utc::now()`
    /// when unset at sync time.
    ///
    /// This value is used for new rows and for existing rows whose cron
    /// expression changed. Existing rows with the same cron expression keep
    /// their stored cursor.
    pub next_fire_at: Option<DateTime<Utc>>,
}

impl<'a> CatalogJobScheduleSpec<'a> {
    pub(super) fn validate_shape(&self) -> Result<(), &'static str> {
        if self.name.trim().is_empty() {
            return Err("name");
        }
        if self.name != self.name.trim() {
            return Err("name");
        }
        if self.job_type.trim().is_empty() {
            return Err("job_type");
        }
        if self.cron_expr.trim().is_empty() {
            return Err("cron_expr");
        }
        if self.cron_expr != self.cron_expr.trim() {
            return Err("cron_expr");
        }
        if Schedule::from_str(self.cron_expr).is_err() {
            return Err("cron_expr");
        }
        if self.max_jitter_seconds < 0 {
            return Err("max_jitter_seconds");
        }
        if self.max_jitter_seconds > JOB_SCHEDULE_MAX_JITTER_SECONDS {
            return Err("max_jitter_seconds");
        }
        Ok(())
    }
}

/// Owned schedule spec stored on [`super::JobCatalog`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct StoredCatalogJobScheduleSpec {
    pub name: String,
    pub job_type: String,
    pub cron_expr: String,
    pub payload_template: Value,
    pub is_active: bool,
    pub organization_id: Option<Uuid>,
    pub max_jitter_seconds: i32,
    pub next_fire_at: Option<DateTime<Utc>>,
}

impl<'a> From<&CatalogJobScheduleSpec<'a>> for StoredCatalogJobScheduleSpec {
    fn from(spec: &CatalogJobScheduleSpec<'a>) -> Self {
        Self {
            name: spec.name.to_owned(),
            job_type: spec.job_type.to_owned(),
            cron_expr: spec.cron_expr.to_owned(),
            payload_template: spec.payload_template.clone(),
            is_active: spec.is_active,
            organization_id: spec.organization_id,
            max_jitter_seconds: spec.max_jitter_seconds,
            next_fire_at: spec.next_fire_at,
        }
    }
}

impl StoredCatalogJobScheduleSpec {
    pub(super) fn as_spec(&self) -> CatalogJobScheduleSpec<'_> {
        CatalogJobScheduleSpec {
            name: &self.name,
            job_type: &self.job_type,
            cron_expr: &self.cron_expr,
            payload_template: &self.payload_template,
            is_active: self.is_active,
            organization_id: self.organization_id,
            max_jitter_seconds: self.max_jitter_seconds,
            next_fire_at: self.next_fire_at,
        }
    }
}
