use thiserror::Error;

use crate::config::JobsConfigValidationError;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    Scheduler(#[from] SchedulerError),
    #[error(transparent)]
    Worker(#[from] WorkerError),
    #[error(transparent)]
    Reaper(#[from] ReaperError),
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
}

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("failed to begin scheduler transaction")]
    BeginTransaction { source: runledger_postgres::Error },
    #[error("failed to commit scheduler transaction")]
    CommitTransaction { source: runledger_postgres::Error },
    #[error("failed to claim due schedules")]
    ClaimDueSchedules { source: runledger_postgres::Error },
    #[error("failed to create savepoint using `{statement}`")]
    SavepointCreate {
        statement: &'static str,
        source: runledger_postgres::Error,
    },
    #[error("failed to rollback savepoint using `{statement}`")]
    SavepointRollback {
        statement: &'static str,
        source: runledger_postgres::Error,
    },
    #[error("failed to release savepoint using `{statement}`")]
    SavepointRelease {
        statement: &'static str,
        source: runledger_postgres::Error,
    },
    #[error("failed deferring failed schedule `{schedule_id}`")]
    DeferFailedSchedule {
        schedule_id: uuid::Uuid,
        source: runledger_postgres::Error,
    },
    #[error(
        "invalid cron expression for schedule `{schedule_name}` ({schedule_id}): `{cron_expr}`"
    )]
    InvalidCronExpression {
        schedule_id: uuid::Uuid,
        schedule_name: String,
        cron_expr: String,
    },
    #[error("failed enqueueing scheduled job `{job_type}` from schedule `{schedule_id}`")]
    EnqueueScheduledJob {
        schedule_id: uuid::Uuid,
        job_type: String,
        source: runledger_postgres::Error,
    },
    #[error("failed marking schedule `{schedule_id}` as fired")]
    MarkScheduleFired {
        schedule_id: uuid::Uuid,
        source: runledger_postgres::Error,
    },
    /// A schedule row returned by the runtime claim query disappeared before the
    /// scheduler could advance or defer its next fire cursor.
    #[error("claimed schedule `{schedule_id}` was missing while {operation}")]
    ClaimedScheduleMissing {
        schedule_id: uuid::Uuid,
        operation: &'static str,
    },
}

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("failed claiming jobs for worker `{worker_id}`")]
    ClaimJobs {
        worker_id: String,
        source: runledger_postgres::Error,
    },
    #[error("failed setting running progress for job `{job_id}` attempt `{attempt}`")]
    SetRunningProgress {
        job_id: uuid::Uuid,
        attempt: i32,
        source: runledger_postgres::Error,
    },
    #[error("failed releasing unstarted claim for job `{job_id}` attempt `{attempt}`")]
    ReleaseUnstartedClaim {
        job_id: uuid::Uuid,
        attempt: i32,
        source: runledger_postgres::Error,
    },
    #[error("failed completing job `{job_id}` attempt `{attempt}` as success")]
    CompleteSuccess {
        job_id: uuid::Uuid,
        attempt: i32,
        source: runledger_postgres::Error,
    },
    #[error("failed continuing job `{job_id}` attempt `{attempt}` after successful handler slice")]
    CompleteContinuation {
        job_id: uuid::Uuid,
        attempt: i32,
        source: runledger_postgres::Error,
    },
    #[error("failed completing job `{job_id}` attempt `{attempt}` as failure")]
    CompleteFailure {
        job_id: uuid::Uuid,
        attempt: i32,
        source: runledger_postgres::Error,
    },
    #[error("failed heartbeat for job `{job_id}` attempt `{attempt}`")]
    Heartbeat {
        job_id: uuid::Uuid,
        attempt: i32,
        source: runledger_postgres::Error,
    },
}

#[derive(Debug, Error)]
pub enum ReaperError {
    #[error(
        "failed reaping expired leases with batch_size `{batch_size}` and retry_delay_ms `{retry_delay_ms}`"
    )]
    ReapExpiredLeases {
        batch_size: i64,
        retry_delay_ms: i32,
        source: runledger_postgres::Error,
    },
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum RuntimeError {
    /// The supervisor builder received an invalid [`JobsConfig`] value. This
    /// usually means the config was constructed directly instead of through
    /// [`JobsConfig::from_env`]. Validation is strict for the whole supervisor
    /// config, even if the loop that would use an invalid field is disabled.
    ///
    /// [`JobsConfig`]: crate::config::JobsConfig
    /// [`JobsConfig::from_env`]: crate::config::JobsConfig::from_env
    #[error("invalid jobs runtime configuration: {source}")]
    InvalidJobsConfig {
        #[source]
        source: JobsConfigValidationError,
    },
    /// The supervisor builder received both direct registry and catalog
    /// registration sources. Choose either [`SupervisorBuilder::with_registry`]
    /// or [`SupervisorBuilder::with_catalog`] for a single builder.
    ///
    /// [`SupervisorBuilder::with_catalog`]: crate::supervisor::SupervisorBuilder::with_catalog
    /// [`SupervisorBuilder::with_registry`]: crate::supervisor::SupervisorBuilder::with_registry
    #[error(
        "supervisor builder received both a job registry and a job catalog; choose one registration source"
    )]
    MixedRegistrySources,
    /// The supervisor builder requires a job registry when the worker or reaper
    /// loop is enabled, but none was provided. Call
    /// [`SupervisorBuilder::with_registry`] before [`SupervisorBuilder::build`].
    ///
    /// [`SupervisorBuilder::with_registry`]: crate::supervisor::SupervisorBuilder::with_registry
    /// [`SupervisorBuilder::build`]: crate::supervisor::SupervisorBuilder::build
    #[error(
        "supervisor builder requires a job registry when worker or reaper loops are enabled \
         (worker_enabled={worker_enabled}, reaper_enabled={reaper_enabled})"
    )]
    MissingRegistry {
        worker_enabled: bool,
        reaper_enabled: bool,
    },
    /// The supervisor builder must be called from within an active Tokio runtime
    /// context. Ensure you are calling it inside a `#[tokio::main]`, `#[tokio::test]`,
    /// or within a spawned task inside an existing runtime.
    #[error("supervisor builder requires an active Tokio runtime")]
    MissingTokioRuntime {
        #[source]
        source: tokio::runtime::TryCurrentError,
    },
    /// The requested shutdown allowance is too large to represent as a deadline.
    /// Use a smaller [`RuntimeShutdownBudget`]. Per-task and per-loop outcomes
    /// are reported by [`RuntimeShutdownFailure`], not by this type.
    ///
    /// [`RuntimeShutdownBudget`]: crate::RuntimeShutdownBudget
    /// [`RuntimeShutdownFailure`]: crate::RuntimeShutdownFailure
    #[error("jobs runtime shutdown timeout {timeout:?} is too large to represent")]
    ShutdownTimeoutTooLarge { timeout: std::time::Duration },
    /// The graceful and abort allowances cannot be added as a Duration.
    /// Both inputs are retained because no representable total exists.
    #[error("jobs runtime shutdown budget overflows: graceful {graceful:?}, abort {abort:?}")]
    ShutdownBudgetOverflow {
        graceful: std::time::Duration,
        abort: std::time::Duration,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scheduler_invalid_cron_variant_contains_schedule_metadata() {
        let schedule_id = uuid::Uuid::nil();
        let error = SchedulerError::InvalidCronExpression {
            schedule_id,
            schedule_name: "nightly sync".to_string(),
            cron_expr: "not cron".to_string(),
        };

        match error {
            SchedulerError::InvalidCronExpression {
                schedule_id: actual_id,
                schedule_name,
                cron_expr,
            } => {
                assert_eq!(actual_id, schedule_id);
                assert_eq!(schedule_name, "nightly sync");
                assert_eq!(cron_expr, "not cron");
            }
            other => panic!("expected invalid cron variant, got: {other:?}"),
        }
    }
}
