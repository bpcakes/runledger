use chrono::{DateTime, Utc};
use runledger_core::jobs::{JobFailure, JobTypeName};
use serde_json::Value;
use sqlx::types::Uuid;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReapedTerminalLeaseRecord {
    pub job_id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub run_number: i32,
    pub attempt: i32,
    pub payload: Value,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReapedLeaseDisposition {
    ReleasedToPending,
    RetryScheduled {
        retry_delay_ms: i32,
        next_run_at: DateTime<Utc>,
    },
    DeadLetteredTerminal {
        payload: Value,
    },
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ReapedLeaseRecord {
    pub job_id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub run_number: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    /// Checkpoint committed on the leased run before it was reaped.
    pub checkpoint: Option<Value>,
    pub worker_id: Option<String>,
    pub started_without_renewal_heartbeat: bool,
    pub failure: JobFailure,
    pub disposition: ReapedLeaseDisposition,
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ReapExpiredLeaseDeferredError {
    pub job_id: Uuid,
    pub run_number: i32,
    pub attempt: i32,
    pub error_code: String,
    pub error_message: String,
    pub sqlstate: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ReapExpiredLeaseCleanupOperation {
    WorkflowActiveClaims,
    ExecutionResourceClaims,
}

impl ReapExpiredLeaseCleanupOperation {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::WorkflowActiveClaims => "workflow_active_claims",
            Self::ExecutionResourceClaims => "execution_resource_claims",
        }
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ReapExpiredLeaseCleanupError {
    /// Bounded cleanup operation that failed after lease transitions committed.
    pub operation: ReapExpiredLeaseCleanupOperation,
    /// Persistence error text for trusted operator diagnostics.
    pub error: String,
}

#[derive(Clone, Debug)]
pub struct ReapExpiredLeasesResult {
    pub processed: i64,
    pub terminal_dead_lettered: Vec<ReapedTerminalLeaseRecord>,
}

/// Detailed lease-reaper outcome, including post-commit coordination cleanup.
///
/// Lease transitions commit before active/resource claim cleanup. A non-empty
/// `cleanup_errors` therefore does not roll back the reaped jobs in `summary`.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ReapExpiredLeasesDetailedResult {
    pub summary: ReapExpiredLeasesResult,
    pub reaped_leases: Vec<ReapedLeaseRecord>,
    pub deferred_row_error_count: usize,
    pub deferred_row_errors: Vec<ReapExpiredLeaseDeferredError>,
    /// Quiesced reusable workflow claims removed in the bounded cleanup pass.
    pub workflow_active_claims_released: u64,
    /// Stale execution-resource claims removed in the bounded cleanup pass.
    pub execution_resource_claims_released: u64,
    pub cleanup_errors: Vec<ReapExpiredLeaseCleanupError>,
}
