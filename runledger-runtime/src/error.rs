use thiserror::Error;

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
pub enum RuntimeError {
    #[error("jobs runtime task `{task}` exited unexpectedly before shutdown")]
    TaskExitedUnexpectedly { task: &'static str },
    #[error("failed joining jobs runtime task `{task}`")]
    TaskJoin {
        task: &'static str,
        #[source]
        source: tokio::task::JoinError,
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
