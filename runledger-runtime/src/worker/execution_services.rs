use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use runledger_core::jobs::{JobExecutionError, JobExecutionServices, JobExecutionUpdate};
use runledger_postgres::jobs::{
    JobLeaseIdentity, JobOrdinaryProgressUpdate, update_job_ordinary_progress_for_lease,
};
use runledger_postgres::{DbPool, Error};
use tokio::sync::Notify;
use tokio::time::{Duration, Instant, timeout_at};

use super::execution::is_lease_owner_mismatch_error;

/// Borrowed only for the worker-owned handler future; it cannot outlive it.
pub(super) struct LeaseExecutionServices<'a> {
    pool: &'a DbPool,
    identity: JobLeaseIdentity<'a>,
    deadline: Instant,
    lease_lost: AtomicBool,
    lease_lost_notification: Notify,
}

impl<'a> LeaseExecutionServices<'a> {
    pub(super) fn new(pool: &'a DbPool, identity: JobLeaseIdentity<'a>, deadline: Instant) -> Self {
        Self {
            pool,
            identity,
            deadline,
            lease_lost: AtomicBool::new(false),
            lease_lost_notification: Notify::new(),
        }
    }

    pub(super) fn lease_was_lost(&self) -> bool {
        self.lease_lost.load(Ordering::Acquire)
    }

    pub(super) async fn wait_for_lease_loss(&self) {
        self.lease_lost_notification.notified().await;
    }
}

#[async_trait]
impl JobExecutionServices for LeaseExecutionServices<'_> {
    fn deadline(&self) -> std::time::Instant {
        self.deadline.into_std()
    }

    fn remaining_budget(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    async fn persist_progress(
        &self,
        update: JobExecutionUpdate<'_>,
    ) -> Result<(), JobExecutionError> {
        if self.lease_was_lost() {
            return Err(JobExecutionError::LeaseLost);
        }
        if Instant::now() >= self.deadline {
            return Err(JobExecutionError::DeadlineElapsed);
        }
        let result = timeout_at(
            self.deadline,
            update_job_ordinary_progress_for_lease(
                self.pool,
                self.identity,
                &JobOrdinaryProgressUpdate {
                    progress_done: update.progress_done,
                    progress_total: update.progress_total,
                    checkpoint: update.checkpoint,
                },
            ),
        )
        .await;
        match result {
            Ok(Ok(())) => Ok(()),
            Err(_) => Err(JobExecutionError::DeadlineElapsed),
            Ok(Err(error)) if is_lease_owner_mismatch_error(&error) => {
                self.lease_lost.store(true, Ordering::Release);
                self.lease_lost_notification.notify_one();
                Err(JobExecutionError::LeaseLost)
            }
            Ok(Err(error)) => {
                if let Error::QueryError(query_error) = &error
                    && let Some(progress_error) = query_error.progress_validation_error()
                {
                    return Err(JobExecutionError::InvalidProgress(progress_error));
                }
                tracing::warn!(
                    job_id = %self.identity.job_id,
                    run_number = self.identity.run_number,
                    attempt = self.identity.attempt,
                    %error,
                    "handler progress persistence failed"
                );
                Err(JobExecutionError::PersistenceFailed)
            }
        }
    }
}
