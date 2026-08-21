use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::openapi::schema::{Object, ObjectBuilder, Type};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::{AdminAccess, AdminScope, DataVisibility};

/// Default number of records returned by list endpoints.
pub const DEFAULT_PAGE_LIMIT: i64 = 50;

/// Maximum number of records returned by one admin request.
pub const MAX_PAGE_LIMIT: i64 = 200;

/// Maximum number of records an offset-paged admin request may skip.
///
/// Operators should narrow filters instead of requesting pages beyond this
/// bounded window. History endpoints use cursor pagination and are unaffected.
pub const MAX_PAGE_OFFSET: i64 = 10_000;

fn default_page_limit() -> i64 {
    DEFAULT_PAGE_LIMIT
}

/// Query accepted by `GET /metrics`.
#[derive(Clone, Debug, Deserialize, Eq, IntoParams, PartialEq)]
#[into_params(parameter_in = Query)]
pub struct MetricsQuery {
    /// Exact job type to return.
    #[param(value_type = String, required = false)]
    pub job_type: Option<String>,
    #[serde(default = "default_page_limit")]
    #[param(default = 50, minimum = 1, maximum = 200)]
    pub limit: i64,
    #[serde(default)]
    #[param(default = 0, minimum = 0, maximum = 10000)]
    pub offset: i64,
}

impl Default for MetricsQuery {
    fn default() -> Self {
        Self {
            job_type: None,
            limit: DEFAULT_PAGE_LIMIT,
            offset: 0,
        }
    }
}

/// Query accepted by `GET /jobs`.
#[derive(Clone, Debug, Deserialize, Eq, IntoParams, PartialEq)]
#[into_params(parameter_in = Query)]
pub struct JobsQuery {
    #[param(value_type = String, required = false)]
    pub status: Option<String>,
    /// Case-insensitive literal substring matched against the job type.
    #[param(value_type = String, required = false)]
    pub job_type: Option<String>,
    #[serde(default = "default_page_limit")]
    #[param(default = 50, minimum = 1, maximum = 200)]
    pub limit: i64,
    #[serde(default)]
    #[param(default = 0, minimum = 0, maximum = 10000)]
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
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HistoryOrder {
    /// Return the newest records first. This is the operator-view default.
    #[default]
    NewestFirst,
    /// Return the oldest records first, which is useful for forward tailing.
    OldestFirst,
}

/// Query accepted by event and log endpoints.
#[derive(Clone, Debug, Deserialize, Eq, IntoParams, PartialEq)]
#[into_params(parameter_in = Query)]
pub struct HistoryQuery {
    #[serde(default = "default_page_limit")]
    #[param(default = 50, minimum = 1, maximum = 200)]
    pub limit: i64,
    #[serde(default)]
    #[param(value_type = String, required = false)]
    pub cursor: Option<String>,
    #[serde(default)]
    #[param(default = "newest_first")]
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
#[derive(Clone, Debug, Deserialize, Eq, IntoParams, PartialEq)]
#[into_params(parameter_in = Query)]
pub struct WorkflowsQuery {
    #[param(value_type = String, required = false)]
    pub status: Option<String>,
    /// Case-insensitive literal substring matched against the workflow type.
    #[param(value_type = String, required = false)]
    pub workflow_type: Option<String>,
    #[serde(default = "default_page_limit")]
    #[param(default = 50, minimum = 1, maximum = 200)]
    pub limit: i64,
    #[serde(default)]
    #[param(default = 0, minimum = 0, maximum = 10000)]
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, IntoParams, PartialEq)]
#[into_params(parameter_in = Query)]
pub struct WorkflowQuery {
    #[serde(default = "default_page_limit")]
    #[param(default = 50, minimum = 1, maximum = 200)]
    pub step_limit: i64,
    #[serde(default)]
    #[param(default = 0, minimum = 0, maximum = 10000)]
    pub step_offset: i64,
    #[serde(default = "default_page_limit")]
    #[param(default = 50, minimum = 1, maximum = 200)]
    pub dependency_limit: i64,
    #[serde(default)]
    #[param(default = 0, minimum = 0, maximum = 10000)]
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
#[derive(Clone, Debug, Deserialize, Eq, IntoParams, PartialEq)]
#[into_params(parameter_in = Query)]
pub struct DefinitionsQuery {
    /// Case-insensitive literal substring matched against the job type.
    #[param(value_type = String, required = false)]
    pub job_type: Option<String>,
    #[serde(default = "default_page_limit")]
    #[param(default = 50, minimum = 1, maximum = 200)]
    pub limit: i64,
    #[serde(default)]
    #[param(default = 0, minimum = 0, maximum = 10000)]
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
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[schema(as = AdminScope)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScopeDto {
    All,
    Organization { organization_id: Uuid },
}

/// Version and permissions discovered by the frontend.
fn api_version_schema() -> Object {
    ObjectBuilder::new()
        .schema_type(Type::String)
        .enum_values(Some([crate::API_VERSION]))
        .build()
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[schema(as = Capabilities)]
pub struct CapabilitiesDto {
    #[schema(schema_with = api_version_schema)]
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
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[schema(as = Page)]
pub struct PageDto {
    pub limit: i64,
    pub offset: i64,
    pub max_offset: i64,
    pub has_more: bool,
}

/// Cursor information returned with event and log history.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[schema(as = HistoryPage)]
pub struct HistoryPageDto {
    pub limit: i64,
    #[schema(required = true)]
    pub cursor: Option<String>,
    #[schema(required = true)]
    pub next_cursor: Option<String>,
    pub order: HistoryOrder,
    pub has_more: bool,
}

/// Per-job-type operational metrics.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
#[schema(as = JobMetrics)]
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
    #[schema(required = true)]
    pub p50_duration_ms_24h: Option<f64>,
    #[schema(required = true)]
    pub p95_duration_ms_24h: Option<f64>,
    pub continued_24h: i64,
    pub active_continued_count: i64,
    pub max_active_run_number: i32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct MetricsResponse {
    pub items: Vec<JobMetricsDto>,
    pub page: PageDto,
}

/// Lightweight job representation used by list endpoints.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
#[schema(as = JobSummary)]
pub struct JobSummaryDto {
    pub id: Uuid,
    pub job_type: String,
    #[schema(required = true)]
    pub organization_id: Option<Uuid>,
    pub status: String,
    pub priority: i32,
    pub run_number: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    pub timeout_seconds: i32,
    pub next_run_at: DateTime<Utc>,
    #[schema(required = true)]
    pub lease_expires_at: Option<DateTime<Utc>>,
    #[schema(required = true)]
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    #[schema(required = true)]
    pub started_at: Option<DateTime<Utc>>,
    #[schema(required = true)]
    pub finished_at: Option<DateTime<Utc>>,
    pub stage: String,
    #[schema(required = true)]
    pub progress_done: Option<i64>,
    #[schema(required = true)]
    pub progress_total: Option<i64>,
    #[schema(required = true)]
    pub progress_pct: Option<f64>,
    #[schema(required = true)]
    pub last_error_code: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Job detail representation. Sensitive and potentially large fields are
/// available only from the single-job endpoint and remain visibility-gated.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
#[schema(as = Job)]
pub struct JobDto {
    pub id: Uuid,
    pub job_type: String,
    #[schema(required = true)]
    pub organization_id: Option<Uuid>,
    pub status: String,
    pub priority: i32,
    pub run_number: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    pub timeout_seconds: i32,
    pub next_run_at: DateTime<Utc>,
    #[schema(required = true)]
    pub lease_expires_at: Option<DateTime<Utc>>,
    #[schema(required = true)]
    pub last_heartbeat_at: Option<DateTime<Utc>>,
    #[schema(required = true)]
    pub started_at: Option<DateTime<Utc>>,
    #[schema(required = true)]
    pub finished_at: Option<DateTime<Utc>>,
    pub stage: String,
    #[schema(required = true)]
    pub progress_done: Option<i64>,
    #[schema(required = true)]
    pub progress_total: Option<i64>,
    #[schema(required = true)]
    pub progress_pct: Option<f64>,
    #[schema(required = true)]
    pub last_error_code: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Value)]
    pub payload: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Value)]
    pub checkpoint: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Value)]
    pub output: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String)]
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String)]
    pub worker_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String)]
    pub status_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String)]
    pub last_error_message: Option<String>,
    pub redacted_fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct JobsResponse {
    pub items: Vec<JobSummaryDto>,
    pub page: PageDto,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct JobResponse {
    pub job: JobDto,
}

/// One durable job event.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
#[schema(as = JobEvent)]
pub struct JobEventDto {
    pub id: String,
    pub job_id: Uuid,
    pub run_number: i32,
    #[schema(required = true)]
    pub attempt: Option<i32>,
    pub event_type: String,
    #[schema(required = true)]
    pub stage: Option<String>,
    #[schema(required = true)]
    pub progress_done: Option<i64>,
    #[schema(required = true)]
    pub progress_total: Option<i64>,
    pub occurred_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Value)]
    pub payload: Option<Value>,
    pub redacted_fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct JobEventsResponse {
    pub items: Vec<JobEventDto>,
    pub page: HistoryPageDto,
}

/// One application-provided job log entry.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
#[schema(as = JobLog)]
pub struct JobLogDto {
    pub id: String,
    pub job_id: Uuid,
    pub run_number: i32,
    #[schema(required = true)]
    pub attempt: Option<i32>,
    pub level: String,
    pub occurred_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String)]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Value)]
    pub payload: Option<Value>,
    pub redacted_fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct JobLogsResponse {
    pub items: Vec<JobLogDto>,
    pub page: HistoryPageDto,
}

/// Lightweight workflow run representation used by list endpoints.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
#[schema(as = WorkflowSummary)]
pub struct WorkflowSummaryDto {
    pub id: Uuid,
    pub workflow_type: String,
    #[schema(required = true)]
    pub organization_id: Option<Uuid>,
    pub status: String,
    #[schema(required = true)]
    pub result_step_key: Option<String>,
    pub started_at: DateTime<Utc>,
    #[schema(required = true)]
    pub finished_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Workflow detail representation. Sensitive and potentially large fields are
/// available only from the single-workflow endpoint and remain visibility-gated.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
#[schema(as = Workflow)]
pub struct WorkflowDto {
    pub id: Uuid,
    pub workflow_type: String,
    #[schema(required = true)]
    pub organization_id: Option<Uuid>,
    pub status: String,
    #[schema(required = true)]
    pub result_step_key: Option<String>,
    pub started_at: DateTime<Utc>,
    #[schema(required = true)]
    pub finished_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String)]
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Value)]
    pub metadata: Option<Value>,
    pub redacted_fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct WorkflowsResponse {
    pub items: Vec<WorkflowSummaryDto>,
    pub page: PageDto,
}

/// Safe workflow step representation.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
#[schema(as = WorkflowStep)]
pub struct WorkflowStepDto {
    pub id: Uuid,
    pub workflow_run_id: Uuid,
    pub step_key: String,
    pub execution_kind: String,
    #[schema(required = true)]
    pub job_type: Option<String>,
    #[schema(required = true)]
    pub organization_id: Option<Uuid>,
    #[schema(required = true)]
    pub priority: Option<i32>,
    #[schema(required = true)]
    pub max_attempts: Option<i32>,
    #[schema(required = true)]
    pub timeout_seconds: Option<i32>,
    #[schema(required = true)]
    pub stage: Option<String>,
    pub allow_handler_continuation: bool,
    pub status: String,
    #[schema(required = true)]
    pub job_id: Option<Uuid>,
    #[schema(required = true)]
    pub released_at: Option<DateTime<Utc>>,
    #[schema(required = true)]
    pub started_at: Option<DateTime<Utc>>,
    #[schema(required = true)]
    pub finished_at: Option<DateTime<Utc>>,
    /// Number of prerequisites visible in the caller's authorization scope.
    pub visible_dependency_count_total: i32,
    /// Number of visible prerequisites that have not reached a terminal state.
    pub visible_dependency_count_pending: i32,
    /// Number of visible `ON_SUCCESS` prerequisites that failed or were canceled.
    pub visible_dependency_count_unsatisfied: i32,
    /// Whether one or more prerequisite steps are outside the caller's scope.
    pub has_hidden_prerequisites: bool,
    #[schema(required = true)]
    pub last_error_code: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Value)]
    pub payload: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String)]
    pub execution_resource_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String)]
    pub status_reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = String)]
    pub last_error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Value)]
    pub output: Option<Value>,
    pub redacted_fields: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[schema(as = WorkflowDependency)]
pub struct WorkflowDependencyDto {
    pub workflow_run_id: Uuid,
    pub prerequisite_step_id: Uuid,
    pub dependent_step_id: Uuid,
    pub release_mode: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize, ToSchema)]
pub struct WorkflowResponse {
    pub workflow: WorkflowDto,
    pub steps: Vec<WorkflowStepDto>,
    pub steps_page: PageDto,
    pub dependencies: Vec<WorkflowDependencyDto>,
    pub dependencies_page: PageDto,
}

/// Registered job definition.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
#[schema(as = JobDefinition)]
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize, ToSchema)]
pub struct DefinitionsResponse {
    pub items: Vec<JobDefinitionDto>,
    pub page: PageDto,
}
