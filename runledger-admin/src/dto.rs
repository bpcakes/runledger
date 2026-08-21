use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::{AdminAccess, AdminScope, DataVisibility};

/// Default number of records returned by list endpoints.
pub const DEFAULT_PAGE_LIMIT: i64 = 50;

/// Maximum number of records returned by one admin request.
pub const MAX_PAGE_LIMIT: i64 = 200;

fn default_page_limit() -> i64 {
    DEFAULT_PAGE_LIMIT
}

/// Query accepted by `GET /metrics`.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct MetricsQuery {
    /// Exact job type to return.
    pub job_type: Option<String>,
}

/// Query accepted by `GET /jobs`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct JobsQuery {
    pub status: Option<String>,
    /// Case-insensitive literal substring matched against the job type.
    pub job_type: Option<String>,
    #[serde(default = "default_page_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

impl Default for JobsQuery {
    fn default() -> Self {
        Self {
            status: None,
            job_type: None,
            limit: DEFAULT_PAGE_LIMIT,
            offset: 0,
        }
    }
}

/// Ordering for job event and log history.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HistoryOrder {
    /// Return the newest records first. This is the operator-view default.
    #[default]
    NewestFirst,
    /// Return the oldest records first, which is useful for forward tailing.
    OldestFirst,
}

/// Query accepted by event and log endpoints.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct HistoryQuery {
    #[serde(default = "default_page_limit")]
    pub limit: i64,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub order: HistoryOrder,
}

impl Default for HistoryQuery {
    fn default() -> Self {
        Self {
            limit: DEFAULT_PAGE_LIMIT,
            cursor: None,
            order: HistoryOrder::default(),
        }
    }
}

/// Query accepted by `GET /workflows`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct WorkflowsQuery {
    pub status: Option<String>,
    /// Case-insensitive literal substring matched against the workflow type.
    pub workflow_type: Option<String>,
    #[serde(default = "default_page_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

impl Default for WorkflowsQuery {
    fn default() -> Self {
        Self {
            status: None,
            workflow_type: None,
            limit: DEFAULT_PAGE_LIMIT,
            offset: 0,
        }
    }
}

/// Pagination accepted by `GET /workflows/{id}` for the two graph collections.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub struct WorkflowQuery {
    #[serde(default = "default_page_limit")]
    pub step_limit: i64,
    #[serde(default)]
    pub step_offset: i64,
    #[serde(default = "default_page_limit")]
    pub dependency_limit: i64,
    #[serde(default)]
    pub dependency_offset: i64,
}

impl Default for WorkflowQuery {
    fn default() -> Self {
        Self {
            step_limit: DEFAULT_PAGE_LIMIT,
            step_offset: 0,
            dependency_limit: DEFAULT_PAGE_LIMIT,
            dependency_offset: 0,
        }
    }
}

/// Query accepted by `GET /definitions`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct DefinitionsQuery {
    /// Case-insensitive literal substring matched against the job type.
    pub job_type: Option<String>,
    #[serde(default = "default_page_limit")]
    pub limit: i64,
    #[serde(default)]
    pub offset: i64,
}

impl Default for DefinitionsQuery {
    fn default() -> Self {
        Self {
            job_type: None,
            limit: DEFAULT_PAGE_LIMIT,
            offset: 0,
        }
    }
}

/// Effective request scope reported by the capabilities endpoint.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScopeDto {
    All,
    Organization { organization_id: Uuid },
}

/// Version and permissions discovered by the frontend.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CapabilitiesDto {
    pub api_version: String,
    pub scope: ScopeDto,
    pub visibility: DataVisibility,
    pub actions: Vec<String>,
    pub resources: Vec<String>,
}

impl From<AdminAccess> for CapabilitiesDto {
    fn from(access: AdminAccess) -> Self {
        let scope = match access.scope() {
            AdminScope::All => ScopeDto::All,
            AdminScope::Organization(organization_id) => ScopeDto::Organization { organization_id },
        };
        let mut resources = vec!["metrics", "jobs", "job_events", "job_logs", "workflows"];
        if access.can_read_service_wide_definitions() {
            resources.push("definitions");
        }
        Self {
            api_version: crate::API_VERSION.to_owned(),
            scope,
            visibility: access.visibility(),
            actions: Vec::new(),
            resources: resources.into_iter().map(str::to_owned).collect(),
        }
    }
}

/// Offset pagination echoed in list responses.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PageDto {
    pub limit: i64,
    pub offset: i64,
    pub has_more: bool,
}

/// Cursor information returned with event and log history.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HistoryPageDto {
    pub limit: i64,
    pub cursor: Option<String>,
    pub next_cursor: Option<String>,
    pub order: HistoryOrder,
    pub has_more: bool,
}

/// Per-job-type operational metrics.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JobMetricsDto {
    pub job_type: String,
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
    pub continued_24h: i64,
    pub active_continued_count: i64,
    pub max_active_run_number: i32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MetricsResponse {
    pub items: Vec<JobMetricsDto>,
}

/// Safe job representation used by list and detail endpoints.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JobDto {
    pub id: Uuid,
    pub job_type: String,
    pub organization_id: Option<Uuid>,
    pub status: String,
    pub priority: i32,
    pub run_number: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    pub timeout_seconds: i32,
    pub next_run_at: DateTime<Utc>,
    pub lease_expires_at: Option<DateTime<Utc>>,
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub stage: String,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub progress_pct: Option<f64>,
    pub last_error_code: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_message: Option<String>,
    pub redacted_fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JobsResponse {
    pub items: Vec<JobDto>,
    pub page: PageDto,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JobResponse {
    pub job: JobDto,
}

/// One durable job event.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JobEventDto {
    pub id: String,
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: Option<i32>,
    pub event_type: String,
    pub stage: Option<String>,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
    pub occurred_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    pub redacted_fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JobEventsResponse {
    pub items: Vec<JobEventDto>,
    pub page: HistoryPageDto,
}

/// One application-provided job log entry.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JobLogDto {
    pub id: String,
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: Option<i32>,
    pub level: String,
    pub occurred_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    pub redacted_fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct JobLogsResponse {
    pub items: Vec<JobLogDto>,
    pub page: HistoryPageDto,
}

/// Safe workflow run representation.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkflowDto {
    pub id: Uuid,
    pub workflow_type: String,
    pub organization_id: Option<Uuid>,
    pub status: String,
    pub result_step_key: Option<String>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<Value>,
    pub redacted_fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkflowsResponse {
    pub items: Vec<WorkflowDto>,
    pub page: PageDto,
}

/// Safe workflow step representation.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkflowStepDto {
    pub id: Uuid,
    pub workflow_run_id: Uuid,
    pub step_key: String,
    pub execution_kind: String,
    pub job_type: Option<String>,
    pub organization_id: Option<Uuid>,
    pub priority: Option<i32>,
    pub max_attempts: Option<i32>,
    pub timeout_seconds: Option<i32>,
    pub stage: Option<String>,
    pub allow_handler_continuation: bool,
    pub status: String,
    pub job_id: Option<Uuid>,
    pub released_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub dependency_count_total: i32,
    pub dependency_count_pending: i32,
    pub dependency_count_unsatisfied: i32,
    pub last_error_code: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub execution_resource_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    pub redacted_fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkflowDependencyDto {
    pub workflow_run_id: Uuid,
    pub prerequisite_step_id: Uuid,
    pub dependent_step_id: Uuid,
    pub release_mode: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkflowResponse {
    pub workflow: WorkflowDto,
    pub steps: Vec<WorkflowStepDto>,
    pub steps_page: PageDto,
    pub dependencies: Vec<WorkflowDependencyDto>,
    pub dependencies_page: PageDto,
}

/// Registered job definition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct JobDefinitionDto {
    pub job_type: String,
    pub version: i32,
    pub max_attempts: i32,
    pub default_timeout_seconds: i32,
    pub default_priority: i32,
    pub is_enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DefinitionsResponse {
    pub items: Vec<JobDefinitionDto>,
    pub page: PageDto,
}
