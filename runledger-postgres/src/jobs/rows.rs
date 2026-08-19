use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::Result;

use super::row_decode::{
    parse_job_stage, parse_job_status, parse_job_type_name, parse_step_key_name,
    parse_workflow_run_status, parse_workflow_step_execution_kind, parse_workflow_step_status,
    parse_workflow_type_name,
};
use super::types::{JobEnqueueIntentRecord, JobEnqueueIntentStatus, JobQueueRecord};
use super::workflow_types::{WorkflowRunDbRecord, WorkflowStepDbRecord};

#[derive(sqlx::FromRow)]
pub(in crate::jobs) struct JobQueueRow {
    pub(in crate::jobs) id: Uuid,
    pub(in crate::jobs) job_type: String,
    pub(in crate::jobs) organization_id: Option<Uuid>,
    pub(in crate::jobs) payload: Value,
    pub(in crate::jobs) status: String,
    pub(in crate::jobs) priority: i32,
    pub(in crate::jobs) run_number: i32,
    pub(in crate::jobs) attempt: i32,
    pub(in crate::jobs) max_attempts: i32,
    pub(in crate::jobs) timeout_seconds: i32,
    pub(in crate::jobs) next_run_at: DateTime<Utc>,
    pub(in crate::jobs) lease_expires_at: Option<DateTime<Utc>>,
    pub(in crate::jobs) last_heartbeat_at: Option<DateTime<Utc>>,
    pub(in crate::jobs) worker_id: Option<String>,
    pub(in crate::jobs) started_at: Option<DateTime<Utc>>,
    pub(in crate::jobs) finished_at: Option<DateTime<Utc>>,
    pub(in crate::jobs) stage: String,
    pub(in crate::jobs) progress_done: Option<i64>,
    pub(in crate::jobs) progress_total: Option<i64>,
    pub(in crate::jobs) progress_pct: Option<f64>,
    pub(in crate::jobs) checkpoint: Option<Value>,
    pub(in crate::jobs) output: Option<Value>,
    pub(in crate::jobs) idempotency_key: Option<String>,
    pub(in crate::jobs) status_reason: Option<String>,
    pub(in crate::jobs) last_error_code: Option<String>,
    pub(in crate::jobs) last_error_message: Option<String>,
    pub(in crate::jobs) created_at: DateTime<Utc>,
    pub(in crate::jobs) updated_at: DateTime<Utc>,
}

impl JobQueueRow {
    pub(in crate::jobs) fn into_record(self) -> Result<JobQueueRecord> {
        self.try_into()
    }
}

impl TryFrom<JobQueueRow> for JobQueueRecord {
    type Error = crate::Error;

    fn try_from(row: JobQueueRow) -> Result<Self> {
        Ok(Self {
            id: row.id,
            job_type: parse_job_type_name(row.job_type)?,
            organization_id: row.organization_id,
            payload: row.payload,
            status: parse_job_status(row.status)?,
            priority: row.priority,
            run_number: row.run_number,
            attempt: row.attempt,
            max_attempts: row.max_attempts,
            timeout_seconds: row.timeout_seconds,
            next_run_at: row.next_run_at,
            lease_expires_at: row.lease_expires_at,
            last_heartbeat_at: row.last_heartbeat_at,
            worker_id: row.worker_id,
            started_at: row.started_at,
            finished_at: row.finished_at,
            stage: parse_job_stage(row.stage)?,
            progress_done: row.progress_done,
            progress_total: row.progress_total,
            progress_pct: row.progress_pct,
            checkpoint: row.checkpoint,
            output: row.output,
            idempotency_key: row.idempotency_key,
            status_reason: row.status_reason,
            last_error_code: row.last_error_code,
            last_error_message: row.last_error_message,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

pub(in crate::jobs) struct JobEnqueueIntentOutcomeRow {
    pub(in crate::jobs) id: Uuid,
    pub(in crate::jobs) status: String,
    pub(in crate::jobs) promoted_job_id: Option<Uuid>,
    pub(in crate::jobs) enqueue_request_matches: bool,
}

pub(in crate::jobs) struct JobEnqueueIntentPromotionRow {
    pub(in crate::jobs) id: Uuid,
    pub(in crate::jobs) job_type: String,
    pub(in crate::jobs) organization_id: Option<Uuid>,
    pub(in crate::jobs) payload: Value,
    pub(in crate::jobs) priority: Option<i32>,
    pub(in crate::jobs) max_attempts: Option<i32>,
    pub(in crate::jobs) timeout_seconds: Option<i32>,
    pub(in crate::jobs) next_run_at: Option<DateTime<Utc>>,
    pub(in crate::jobs) idempotency_key: String,
    pub(in crate::jobs) stage: String,
    pub(in crate::jobs) enqueue_request_version: i16,
    pub(in crate::jobs) enqueue_request: Value,
    pub(in crate::jobs) execution_resource_key: Option<String>,
}

pub(in crate::jobs) struct JobEnqueueIntentRecordRow {
    pub(in crate::jobs) id: Uuid,
    pub(in crate::jobs) job_type: String,
    pub(in crate::jobs) organization_id: Option<Uuid>,
    pub(in crate::jobs) payload: Value,
    pub(in crate::jobs) priority: Option<i32>,
    pub(in crate::jobs) max_attempts: Option<i32>,
    pub(in crate::jobs) timeout_seconds: Option<i32>,
    pub(in crate::jobs) next_run_at: Option<DateTime<Utc>>,
    pub(in crate::jobs) idempotency_key: String,
    pub(in crate::jobs) stage: String,
    pub(in crate::jobs) enqueue_request_version: i16,
    pub(in crate::jobs) execution_resource_key: Option<String>,
    pub(in crate::jobs) promotion_attempts: i32,
    pub(in crate::jobs) next_promotion_at: DateTime<Utc>,
    pub(in crate::jobs) last_attempted_at: Option<DateTime<Utc>>,
    pub(in crate::jobs) status: String,
    pub(in crate::jobs) promoted_job_id: Option<Uuid>,
    pub(in crate::jobs) promoted_at: Option<DateTime<Utc>>,
    pub(in crate::jobs) conflicted_at: Option<DateTime<Utc>>,
    pub(in crate::jobs) last_error_code: Option<String>,
    pub(in crate::jobs) last_error_message: Option<String>,
    pub(in crate::jobs) created_at: DateTime<Utc>,
    pub(in crate::jobs) updated_at: DateTime<Utc>,
}

impl JobEnqueueIntentRecordRow {
    pub(in crate::jobs) fn into_record(self) -> Result<JobEnqueueIntentRecord> {
        let status = self
            .status
            .parse::<JobEnqueueIntentStatus>()
            .map_err(|()| {
                crate::Error::QueryError(crate::QueryError::from_classified(
                    crate::QueryErrorCategory::Internal,
                    "job.intent_invalid_status",
                    "Job enqueue intent status in persisted row is invalid.",
                    "invalid job enqueue intent status in row",
                ))
            })?;

        Ok(JobEnqueueIntentRecord {
            id: self.id,
            job_type: parse_job_type_name(self.job_type)?,
            organization_id: self.organization_id,
            payload: self.payload,
            priority: self.priority,
            max_attempts: self.max_attempts,
            timeout_seconds: self.timeout_seconds,
            next_run_at: self.next_run_at,
            idempotency_key: self.idempotency_key,
            stage: parse_job_stage(self.stage)?,
            enqueue_request_version: self.enqueue_request_version,
            execution_resource_key: self.execution_resource_key,
            promotion_attempts: self.promotion_attempts,
            next_promotion_at: self.next_promotion_at,
            last_attempted_at: self.last_attempted_at,
            status,
            promoted_job_id: self.promoted_job_id,
            promoted_at: self.promoted_at,
            conflicted_at: self.conflicted_at,
            last_error_code: self.last_error_code,
            last_error_message: self.last_error_message,
            created_at: self.created_at,
            updated_at: self.updated_at,
        })
    }
}

#[derive(sqlx::FromRow)]
pub(in crate::jobs) struct WorkflowRunRow {
    pub(in crate::jobs) id: Uuid,
    pub(in crate::jobs) workflow_type: String,
    pub(in crate::jobs) organization_id: Option<Uuid>,
    pub(in crate::jobs) status: String,
    pub(in crate::jobs) idempotency_key: Option<String>,
    pub(in crate::jobs) result_step_key: Option<String>,
    pub(in crate::jobs) metadata: Value,
    pub(in crate::jobs) started_at: DateTime<Utc>,
    pub(in crate::jobs) finished_at: Option<DateTime<Utc>>,
    pub(in crate::jobs) created_at: DateTime<Utc>,
    pub(in crate::jobs) updated_at: DateTime<Utc>,
}

impl WorkflowRunRow {
    pub(in crate::jobs) fn into_record(self) -> Result<WorkflowRunDbRecord> {
        self.try_into()
    }
}

impl TryFrom<WorkflowRunRow> for WorkflowRunDbRecord {
    type Error = crate::Error;

    fn try_from(row: WorkflowRunRow) -> Result<Self> {
        Ok(Self {
            id: row.id,
            workflow_type: parse_workflow_type_name(row.workflow_type)?,
            organization_id: row.organization_id,
            status: parse_workflow_run_status(row.status)?,
            idempotency_key: row.idempotency_key,
            result_step_key: row.result_step_key.map(parse_step_key_name).transpose()?,
            metadata: row.metadata,
            started_at: row.started_at,
            finished_at: row.finished_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[derive(sqlx::FromRow)]
pub(in crate::jobs) struct WorkflowRunEnqueueRow {
    pub(in crate::jobs) id: Uuid,
    pub(in crate::jobs) workflow_type: String,
    pub(in crate::jobs) organization_id: Option<Uuid>,
    pub(in crate::jobs) status: String,
    pub(in crate::jobs) idempotency_key: Option<String>,
    pub(in crate::jobs) result_step_key: Option<String>,
    pub(in crate::jobs) metadata: Value,
    pub(in crate::jobs) enqueue_request_matches: Option<bool>,
    pub(in crate::jobs) started_at: DateTime<Utc>,
    pub(in crate::jobs) finished_at: Option<DateTime<Utc>>,
    pub(in crate::jobs) created_at: DateTime<Utc>,
    pub(in crate::jobs) updated_at: DateTime<Utc>,
}

impl WorkflowRunEnqueueRow {
    pub(in crate::jobs) fn into_record(self) -> Result<WorkflowRunDbRecord> {
        WorkflowRunRow::from(self).into_record()
    }

    pub(in crate::jobs) fn enqueue_request_matches(&self) -> Option<bool> {
        self.enqueue_request_matches
    }
}

impl From<WorkflowRunEnqueueRow> for WorkflowRunRow {
    fn from(row: WorkflowRunEnqueueRow) -> Self {
        Self {
            id: row.id,
            workflow_type: row.workflow_type,
            organization_id: row.organization_id,
            status: row.status,
            idempotency_key: row.idempotency_key,
            result_step_key: row.result_step_key,
            metadata: row.metadata,
            started_at: row.started_at,
            finished_at: row.finished_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        }
    }
}

#[derive(sqlx::FromRow)]
pub(in crate::jobs) struct WorkflowStepRow {
    pub(in crate::jobs) id: Uuid,
    pub(in crate::jobs) workflow_run_id: Uuid,
    pub(in crate::jobs) step_key: String,
    pub(in crate::jobs) execution_kind: String,
    pub(in crate::jobs) job_type: Option<String>,
    pub(in crate::jobs) organization_id: Option<Uuid>,
    pub(in crate::jobs) payload: Value,
    pub(in crate::jobs) priority: Option<i32>,
    pub(in crate::jobs) max_attempts: Option<i32>,
    pub(in crate::jobs) timeout_seconds: Option<i32>,
    pub(in crate::jobs) stage: Option<String>,
    pub(in crate::jobs) allow_handler_continuation: bool,
    pub(in crate::jobs) execution_resource_key: Option<String>,
    pub(in crate::jobs) status: String,
    pub(in crate::jobs) job_id: Option<Uuid>,
    pub(in crate::jobs) released_at: Option<DateTime<Utc>>,
    pub(in crate::jobs) started_at: Option<DateTime<Utc>>,
    pub(in crate::jobs) finished_at: Option<DateTime<Utc>>,
    pub(in crate::jobs) dependency_count_total: i32,
    pub(in crate::jobs) dependency_count_pending: i32,
    pub(in crate::jobs) dependency_count_unsatisfied: i32,
    pub(in crate::jobs) status_reason: Option<String>,
    pub(in crate::jobs) last_error_code: Option<String>,
    pub(in crate::jobs) last_error_message: Option<String>,
    pub(in crate::jobs) output: Option<Value>,
    pub(in crate::jobs) created_at: DateTime<Utc>,
    pub(in crate::jobs) updated_at: DateTime<Utc>,
}

impl WorkflowStepRow {
    pub(in crate::jobs) fn into_record(self) -> Result<WorkflowStepDbRecord> {
        self.try_into()
    }
}

impl TryFrom<WorkflowStepRow> for WorkflowStepDbRecord {
    type Error = crate::Error;

    fn try_from(row: WorkflowStepRow) -> Result<Self> {
        Ok(Self {
            id: row.id,
            workflow_run_id: row.workflow_run_id,
            step_key: parse_step_key_name(row.step_key)?,
            execution_kind: parse_workflow_step_execution_kind(row.execution_kind)?,
            job_type: row.job_type.map(parse_job_type_name).transpose()?,
            organization_id: row.organization_id,
            payload: row.payload,
            priority: row.priority,
            max_attempts: row.max_attempts,
            timeout_seconds: row.timeout_seconds,
            stage: row.stage.map(parse_job_stage).transpose()?,
            allow_handler_continuation: row.allow_handler_continuation,
            execution_resource_key: row.execution_resource_key,
            status: parse_workflow_step_status(row.status)?,
            job_id: row.job_id,
            released_at: row.released_at,
            started_at: row.started_at,
            finished_at: row.finished_at,
            dependency_count_total: row.dependency_count_total,
            dependency_count_pending: row.dependency_count_pending,
            dependency_count_unsatisfied: row.dependency_count_unsatisfied,
            status_reason: row.status_reason,
            last_error_code: row.last_error_code,
            last_error_message: row.last_error_message,
            output: row.output,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}
