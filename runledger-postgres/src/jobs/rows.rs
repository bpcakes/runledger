use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::types::Uuid;

use crate::Result;

use super::row_decode::{
    parse_job_stage, parse_job_status, parse_job_type_name, parse_step_key_name,
    parse_workflow_run_status, parse_workflow_step_execution_kind, parse_workflow_step_status,
    parse_workflow_type_name,
};
use super::types::{
    JobEnqueueIntentPromotionError, JobEnqueueIntentRecord, JobEnqueueIntentState, JobQueueRecord,
};
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

/// Promotion input selected behind the current snapshot-version predicate.
///
/// Widening version support requires a separate decoder for each accepted
/// version before the selection query admits those rows.
pub(in crate::jobs) struct SupportedJobEnqueueIntentPromotionRow {
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

struct JobEnqueueIntentStateRow {
    promotion_attempts: i32,
    last_attempted_at: Option<DateTime<Utc>>,
    status: String,
    promoted_job_id: Option<Uuid>,
    promoted_at: Option<DateTime<Utc>>,
    conflicted_at: Option<DateTime<Utc>>,
    last_error_code: Option<String>,
    last_error_message: Option<String>,
}

impl JobEnqueueIntentStateRow {
    fn into_state(self) -> Result<JobEnqueueIntentState> {
        let Self {
            promotion_attempts,
            last_attempted_at,
            status,
            promoted_job_id,
            promoted_at,
            conflicted_at,
            last_error_code,
            last_error_message,
        } = self;

        match status.as_str() {
            "PENDING"
                if promotion_attempts == 0
                    && last_attempted_at.is_none()
                    && promoted_job_id.is_none()
                    && promoted_at.is_none()
                    && conflicted_at.is_none()
                    && last_error_code.is_none()
                    && last_error_message.is_none() =>
            {
                Ok(JobEnqueueIntentState::InitialPending)
            }
            "PENDING" if promotion_attempts > 0 => {
                let (Some(last_attempted_at), None, None, None, Some(code), Some(message)) = (
                    last_attempted_at,
                    promoted_job_id,
                    promoted_at,
                    conflicted_at,
                    last_error_code,
                    last_error_message,
                ) else {
                    return Err(invalid_job_enqueue_intent_state_error(status));
                };
                let error = decode_job_enqueue_intent_promotion_error(code, message, &status)?;

                Ok(JobEnqueueIntentState::RetryPending {
                    promotion_attempts,
                    last_attempted_at,
                    error,
                })
            }
            "PROMOTED" if promotion_attempts > 0 => {
                let (Some(last_attempted_at), Some(job_id), Some(promoted_at), None, None, None) = (
                    last_attempted_at,
                    promoted_job_id,
                    promoted_at,
                    conflicted_at,
                    last_error_code,
                    last_error_message,
                ) else {
                    return Err(invalid_job_enqueue_intent_state_error(status));
                };

                Ok(JobEnqueueIntentState::Promoted {
                    promotion_attempts,
                    last_attempted_at,
                    job_id,
                    promoted_at,
                })
            }
            "CONFLICTED" if promotion_attempts > 0 => {
                let (
                    Some(last_attempted_at),
                    None,
                    None,
                    Some(conflicted_at),
                    Some(code),
                    Some(message),
                ) = (
                    last_attempted_at,
                    promoted_job_id,
                    promoted_at,
                    conflicted_at,
                    last_error_code,
                    last_error_message,
                )
                else {
                    return Err(invalid_job_enqueue_intent_state_error(status));
                };
                let error = decode_job_enqueue_intent_promotion_error(code, message, &status)?;

                Ok(JobEnqueueIntentState::Conflicted {
                    promotion_attempts,
                    last_attempted_at,
                    conflicted_at,
                    error,
                })
            }
            _ => Err(invalid_job_enqueue_intent_state_error(status)),
        }
    }
}

fn decode_job_enqueue_intent_promotion_error(
    code: String,
    message: String,
    status: &str,
) -> Result<JobEnqueueIntentPromotionError> {
    if code.trim().is_empty() || message.trim().is_empty() {
        return Err(invalid_job_enqueue_intent_state_error(status));
    }

    Ok(JobEnqueueIntentPromotionError::new(code, message))
}

fn invalid_job_enqueue_intent_state_error(status: impl AsRef<str>) -> crate::Error {
    crate::Error::QueryError(crate::QueryError::from_classified(
        crate::QueryErrorCategory::Internal,
        "job.intent_invalid_persisted_row",
        "Job enqueue intent contains invalid persisted state.",
        format!(
            "job enqueue intent persisted lifecycle fields do not match status {}",
            status.as_ref()
        ),
    ))
}

impl JobEnqueueIntentRecordRow {
    pub(in crate::jobs) fn into_record(self) -> Result<JobEnqueueIntentRecord> {
        let Self {
            id,
            job_type,
            organization_id,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            next_run_at,
            idempotency_key,
            stage,
            enqueue_request_version,
            execution_resource_key,
            promotion_attempts,
            next_promotion_at,
            last_attempted_at,
            status,
            promoted_job_id,
            promoted_at,
            conflicted_at,
            last_error_code,
            last_error_message,
            created_at,
            updated_at,
        } = self;
        let state = JobEnqueueIntentStateRow {
            promotion_attempts,
            last_attempted_at,
            status,
            promoted_job_id,
            promoted_at,
            conflicted_at,
            last_error_code,
            last_error_message,
        }
        .into_state()?;

        Ok(JobEnqueueIntentRecord {
            id,
            job_type: parse_job_type_name(job_type)?,
            organization_id,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            next_run_at,
            idempotency_key,
            stage: parse_job_stage(stage)?,
            enqueue_request_version,
            execution_resource_key,
            next_promotion_at,
            state,
            created_at,
            updated_at,
        })
    }
}

#[cfg(test)]
mod enqueue_intent_state_tests {
    use super::*;

    fn initial_pending_row() -> JobEnqueueIntentStateRow {
        JobEnqueueIntentStateRow {
            promotion_attempts: 0,
            last_attempted_at: None,
            status: "PENDING".into(),
            promoted_job_id: None,
            promoted_at: None,
            conflicted_at: None,
            last_error_code: None,
            last_error_message: None,
        }
    }

    fn assert_invalid(row: JobEnqueueIntentStateRow) {
        let error = row.into_state().expect_err("row must be rejected");
        let crate::Error::QueryError(error) = error else {
            panic!("expected query error");
        };
        assert_eq!(error.code(), "job.intent_invalid_persisted_row");
    }

    #[test]
    fn decodes_and_serializes_every_enqueue_intent_state() {
        let attempted_at = Utc::now();
        let terminal_at = attempted_at + chrono::Duration::seconds(1);
        let job_id = Uuid::now_v7();

        let initial = initial_pending_row()
            .into_state()
            .expect("decode initial pending state");
        assert_eq!(
            serde_json::to_value(initial).expect("serialize initial pending state"),
            serde_json::json!({"state": "initial_pending"})
        );

        let retry = JobEnqueueIntentStateRow {
            promotion_attempts: 1,
            last_attempted_at: Some(attempted_at),
            status: "PENDING".into(),
            promoted_job_id: None,
            promoted_at: None,
            conflicted_at: None,
            last_error_code: Some("job.definition_not_found".into()),
            last_error_message: Some("definition unavailable".into()),
        }
        .into_state()
        .expect("decode retry pending state");
        assert_eq!(
            serde_json::to_value(retry).expect("serialize retry pending state"),
            serde_json::json!({
                "state": "retry_pending",
                "promotion_attempts": 1,
                "last_attempted_at": attempted_at,
                "error": {
                    "code": "job.definition_not_found",
                    "message": "definition unavailable"
                }
            })
        );

        let promoted = JobEnqueueIntentStateRow {
            promotion_attempts: 2,
            last_attempted_at: Some(attempted_at),
            status: "PROMOTED".into(),
            promoted_job_id: Some(job_id),
            promoted_at: Some(terminal_at),
            conflicted_at: None,
            last_error_code: None,
            last_error_message: None,
        }
        .into_state()
        .expect("decode promoted state");
        assert_eq!(
            serde_json::to_value(promoted).expect("serialize promoted state"),
            serde_json::json!({
                "state": "promoted",
                "promotion_attempts": 2,
                "last_attempted_at": attempted_at,
                "job_id": job_id,
                "promoted_at": terminal_at
            })
        );

        let conflicted = JobEnqueueIntentStateRow {
            promotion_attempts: 3,
            last_attempted_at: Some(attempted_at),
            status: "CONFLICTED".into(),
            promoted_job_id: None,
            promoted_at: None,
            conflicted_at: Some(terminal_at),
            last_error_code: Some("job.intent_idempotency_conflict".into()),
            last_error_message: Some("request differs".into()),
        }
        .into_state()
        .expect("decode conflicted state");
        assert_eq!(
            serde_json::to_value(conflicted).expect("serialize conflicted state"),
            serde_json::json!({
                "state": "conflicted",
                "promotion_attempts": 3,
                "last_attempted_at": attempted_at,
                "conflicted_at": terminal_at,
                "error": {
                    "code": "job.intent_idempotency_conflict",
                    "message": "request differs"
                }
            })
        );
    }

    #[test]
    fn rejects_impossible_enqueue_intent_state_rows() {
        let timestamp = Utc::now();

        let mut unknown = initial_pending_row();
        unknown.status = "UNKNOWN".into();
        assert_invalid(unknown);

        let mut attempted_initial = initial_pending_row();
        attempted_initial.last_attempted_at = Some(timestamp);
        assert_invalid(attempted_initial);

        let mut incomplete_retry = initial_pending_row();
        incomplete_retry.promotion_attempts = 1;
        incomplete_retry.last_attempted_at = Some(timestamp);
        assert_invalid(incomplete_retry);

        let mut incomplete_promoted = initial_pending_row();
        incomplete_promoted.status = "PROMOTED".into();
        incomplete_promoted.promotion_attempts = 1;
        incomplete_promoted.last_attempted_at = Some(timestamp);
        assert_invalid(incomplete_promoted);

        let mut crossed_terminal_fields = initial_pending_row();
        crossed_terminal_fields.status = "CONFLICTED".into();
        crossed_terminal_fields.promotion_attempts = 1;
        crossed_terminal_fields.last_attempted_at = Some(timestamp);
        crossed_terminal_fields.promoted_job_id = Some(Uuid::now_v7());
        crossed_terminal_fields.promoted_at = Some(timestamp);
        crossed_terminal_fields.conflicted_at = Some(timestamp);
        crossed_terminal_fields.last_error_code = Some("code".into());
        crossed_terminal_fields.last_error_message = Some("message".into());
        assert_invalid(crossed_terminal_fields);

        let mut blank_error = initial_pending_row();
        blank_error.promotion_attempts = 1;
        blank_error.last_attempted_at = Some(timestamp);
        blank_error.last_error_code = Some(" ".into());
        blank_error.last_error_message = Some("message".into());
        assert_invalid(blank_error);
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
