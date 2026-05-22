use chrono::{DateTime, Utc};
use runledger_core::jobs::{
    JobEventType, JobFailureKind, JobStage, JobStatus, JobType, JobTypeName,
};
use serde_json::Value;
use sqlx::types::Uuid;

#[derive(Debug, Clone)]
pub struct JobDefinitionUpsert<'a> {
    pub job_type: JobType<'a>,
    pub version: i32,
    pub max_attempts: i32,
    pub default_timeout_seconds: i32,
    pub default_priority: i32,
    pub is_enabled: bool,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct JobDefinitionListFilter<'a> {
    /// Admin list query input used for escaped `ILIKE` substring matching, not a canonical
    /// persisted identifier boundary.
    pub job_type: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Clone)]
pub struct JobDefinitionUpdate {
    pub max_attempts: Option<i32>,
    pub default_timeout_seconds: Option<i32>,
    pub default_priority: Option<i32>,
    pub is_enabled: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct JobRuntimeConfigUpsert<'a> {
    pub job_type: JobType<'a>,
    pub schema_version: i32,
    pub config: &'a Value,
    pub updated_by_user_id: Option<Uuid>,
}

#[derive(Debug, Clone)]
pub struct JobRuntimeConfigRecord {
    pub job_type: JobTypeName,
    pub schema_version: i32,
    pub config: Value,
    pub updated_by_user_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct JobEnqueue<'a> {
    pub job_type: JobType<'a>,
    pub organization_id: Option<Uuid>,
    pub payload: &'a Value,
    pub priority: Option<i32>,
    pub max_attempts: Option<i32>,
    pub timeout_seconds: Option<i32>,
    /// For keyed enqueues, this value is part of the stored idempotency request
    /// snapshot. Retries must pass the same scheduled time as the original
    /// enqueue instead of recomputing a fresh timestamp.
    pub next_run_at: Option<DateTime<Utc>>,
    pub idempotency_key: Option<&'a str>,
    pub stage: Option<JobStage>,
}

#[derive(Debug, Clone)]
pub struct JobScheduleRecord {
    pub id: Uuid,
    pub name: String,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub payload_template: Value,
    pub cron_expr: String,
    pub max_jitter_seconds: i32,
    pub next_fire_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct JobQueueRecord {
    pub id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub payload: Value,
    pub status: JobStatus,
    pub priority: i32,
    pub run_number: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    pub timeout_seconds: i32,
    pub next_run_at: DateTime<Utc>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub worker_id: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub stage: JobStage,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub progress_pct: Option<f64>,
    pub checkpoint: Option<Value>,
    pub idempotency_key: Option<String>,
    pub status_reason: Option<String>,
    pub last_error_code: Option<String>,
    pub last_error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct JobEventRecord {
    pub id: i64,
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: Option<i32>,
    pub event_type: JobEventType,
    pub stage: Option<JobStage>,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub payload: Value,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ReapedTerminalLeaseRecord {
    pub job_id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub run_number: i32,
    pub attempt: i32,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct ReapExpiredLeasesResult {
    pub processed: i64,
    pub terminal_dead_lettered: Vec<ReapedTerminalLeaseRecord>,
}

#[derive(Debug, Clone)]
pub struct JobMetricsRecord {
    pub job_type: JobTypeName,
    pub pending_count: i64,
    pub leased_count: i64,
    pub stale_leases: i64,
    pub succeeded_24h: i64,
    pub retryable_24h: i64,
    pub terminal_24h: i64,
    pub panicked_24h: i64,
    pub timeout_24h: i64,
    pub dead_lettered_24h: i64,
    pub p50_duration_ms_24h: Option<f64>,
    pub p95_duration_ms_24h: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct JobLogRecord {
    pub id: i64,
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: Option<i32>,
    pub level: String,
    pub message: String,
    pub payload: Value,
    pub occurred_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct JobLogRecordInput {
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: Option<i32>,
    pub level: String,
    pub message: String,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct JobProgressUpdate<'a> {
    pub stage: Option<JobStage>,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub checkpoint: Option<&'a Value>,
}

#[derive(Debug, Clone)]
pub struct JobFailureUpdate<'a> {
    pub kind: JobFailureKind,
    pub code: &'a str,
    pub message: &'a str,
    pub retry_delay_ms: Option<i32>,
}

#[derive(Debug, Clone)]
pub struct JobListFilter<'a> {
    pub organization_id: Option<Uuid>,
    pub status: Option<JobStatus>,
    /// Admin list query input used for `ILIKE` substring matching, not a canonical persisted
    /// identifier boundary.
    pub job_type: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}

#[derive(Debug, Clone)]
pub struct JobRuntimeConfigListFilter<'a> {
    /// Admin query filter string used for listing/runtime-config lookup filters, not a canonical
    /// persisted identifier boundary.
    pub job_type: Option<&'a str>,
    pub limit: i64,
    pub offset: i64,
}
