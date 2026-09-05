use chrono::{DateTime, Utc};
use runledger_core::jobs::{JobType, JobTypeName};
use serde_json::Value;
use sqlx::types::Uuid;

#[derive(Clone, Debug)]
pub struct JobDefinitionUpsert<'a> {
    pub job_type: JobType<'a>,
    pub version: i32,
    pub max_attempts: i32,
    pub default_timeout_seconds: i32,
    pub default_priority: i32,
    pub is_enabled: bool,
}

impl From<&runledger_core::jobs::JobSpec> for JobDefinitionUpsert<'static> {
    fn from(spec: &runledger_core::jobs::JobSpec) -> Self {
        let settings = spec.settings();
        Self {
            job_type: spec.job_type(),
            version: settings.version,
            max_attempts: settings.max_attempts,
            default_timeout_seconds: settings.default_timeout_seconds,
            default_priority: settings.default_priority,
            is_enabled: settings.is_enabled,
        }
    }
}

#[derive(Clone, Debug)]
pub struct JobDefinitionRecord {
    pub job_type: JobTypeName,
    pub version: i32,
    pub max_attempts: i32,
    pub default_timeout_seconds: i32,
    pub default_priority: i32,
    pub is_enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Schedule row that blocks a job-definition catalog sync.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobScheduleJobTypeReference {
    /// Active schedule name.
    pub schedule_name: String,
    /// Job type referenced by the active schedule.
    pub job_type: JobTypeName,
}

#[derive(Clone, Debug)]
pub struct JobDefinitionListFilter<'a> {
    /// Admin list query input used for escaped `ILIKE` substring matching, not a canonical
    /// persisted identifier boundary.
    pub job_type: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Clone, Debug)]
pub struct JobDefinitionUpdate {
    pub max_attempts: Option<i32>,
    pub default_timeout_seconds: Option<i32>,
    pub default_priority: Option<i32>,
    pub is_enabled: Option<bool>,
}

#[derive(Clone, Debug)]
pub struct JobRuntimeConfigUpsert<'a> {
    pub job_type: JobType<'a>,
    pub schema_version: i32,
    pub config: &'a Value,
    pub updated_by_user_id: Option<Uuid>,
}

#[derive(Clone, Debug)]
pub struct JobRuntimeConfigRecord {
    pub job_type: JobTypeName,
    pub schema_version: i32,
    pub config: Value,
    pub updated_by_user_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct JobRuntimeConfigListFilter<'a> {
    /// Admin query filter string used for listing/runtime-config lookup filters, not a canonical
    /// persisted identifier boundary.
    pub job_type: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}
