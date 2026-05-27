use chrono::{DateTime, Utc};
use runledger_core::jobs::{JobStage, WorkflowStepEnqueueBuilder};
use runledger_postgres::jobs::{JobEnqueue, JobScheduleUpsert};
use serde_json::Value;
use uuid::Uuid;

use super::{CatalogError, JobCatalog};

/// Input for building a [`JobEnqueue`] from a catalog-backed job type.
#[derive(Debug, Clone)]
pub struct CatalogJobEnqueueInput<'a> {
    /// Catalog job type to enqueue.
    pub job_type: &'a str,
    /// Optional organization scope copied to the queued job.
    pub organization_id: Option<Uuid>,
    /// JSON payload stored with the queued job.
    pub payload: &'a Value,
    /// Optional queue priority override.
    pub priority: Option<i32>,
    /// Optional maximum-attempts override.
    pub max_attempts: Option<i32>,
    /// Optional execution timeout override, in seconds.
    pub timeout_seconds: Option<i32>,
    /// Optional future time before which the job should not run.
    pub next_run_at: Option<DateTime<Utc>>,
    /// Optional idempotency key for duplicate enqueue protection.
    pub idempotency_key: Option<&'a str>,
    /// Optional initial job stage.
    pub stage: Option<JobStage>,
}

/// Input for building a [`JobScheduleUpsert`] from a catalog-backed job type.
#[derive(Debug, Clone)]
pub struct CatalogJobScheduleInput<'a> {
    /// Unique schedule name.
    pub name: &'a str,
    /// Catalog job type enqueued when the schedule fires.
    pub job_type: &'a str,
    /// Optional organization scope copied into jobs for a new schedule.
    ///
    /// Existing schedules preserve their stored organization scope on conflict.
    pub organization_id: Option<Uuid>,
    /// JSON payload template copied into scheduled jobs.
    pub payload_template: &'a Value,
    /// Cron expression used by the runtime scheduler.
    pub cron_expr: &'a str,
    /// Whether a new schedule should be active.
    ///
    /// Existing schedules preserve their stored active state on conflict.
    pub is_active: bool,
    /// Next UTC instant at which the schedule is due.
    pub next_fire_at: DateTime<Utc>,
    /// Maximum deterministic jitter, in seconds, applied to future fire times.
    pub max_jitter_seconds: i32,
}

impl JobCatalog {
    /// Builds a [`JobEnqueue`] after validating the job type is registered and enabled.
    ///
    /// This checks catalog configuration only. Operator-disabled database rows
    /// are still enforced by `runledger-postgres` when the job is enqueued.
    /// Catalog defaults' enabled flag applies to every catalog entry; per-job
    /// enabled overrides are not modeled yet.
    pub fn job_enqueue<'a>(
        &self,
        input: &CatalogJobEnqueueInput<'a>,
    ) -> Result<JobEnqueue<'a>, CatalogError> {
        let job_type = self.require_catalog_enabled_job_type(input.job_type)?;
        Ok(JobEnqueue {
            job_type,
            organization_id: input.organization_id,
            payload: input.payload,
            priority: input.priority,
            max_attempts: input.max_attempts,
            timeout_seconds: input.timeout_seconds,
            next_run_at: input.next_run_at,
            idempotency_key: input.idempotency_key,
            stage: input.stage,
        })
    }

    /// Builds a [`JobScheduleUpsert`] after validating the job type is registered and enabled.
    ///
    /// This checks catalog configuration only. Operator-disabled database rows
    /// are still enforced by `runledger-postgres` when schedule-created jobs are
    /// materialized.
    /// Catalog defaults' enabled flag applies to every catalog entry; per-job
    /// enabled overrides are not modeled yet.
    pub fn job_schedule<'a>(
        &self,
        input: &CatalogJobScheduleInput<'a>,
    ) -> Result<JobScheduleUpsert<'a>, CatalogError> {
        let job_type = self.require_catalog_enabled_job_type(input.job_type)?;
        Ok(JobScheduleUpsert {
            name: input.name,
            job_type,
            organization_id: input.organization_id,
            payload_template: input.payload_template,
            cron_expr: input.cron_expr,
            is_active: input.is_active,
            next_fire_at: input.next_fire_at,
            max_jitter_seconds: input.max_jitter_seconds,
        })
    }

    /// Builds a workflow step after validating the job type is registered and enabled.
    pub fn workflow_step<'a>(
        &self,
        step_key: &'a str,
        job_type_name: &str,
        payload: &'a Value,
    ) -> Result<WorkflowStepEnqueueBuilder<'a>, CatalogError> {
        let job_type = self.require_catalog_enabled_job_type(job_type_name)?;
        WorkflowStepEnqueueBuilder::try_new(step_key, job_type.as_str(), payload)
            .map_err(CatalogError::WorkflowBuild)
    }
}
