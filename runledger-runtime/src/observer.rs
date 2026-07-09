use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use runledger_core::jobs::{JobDeadLetterReason, JobFailure, JobTypeName};
use tracing::warn;
use uuid::Uuid;

#[cfg(test)]
const OBSERVER_TIMEOUT: Duration = Duration::from_millis(100);
#[cfg(not(test))]
const OBSERVER_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ObservedJob {
    pub job_id: Uuid,
    pub job_type: JobTypeName,
    pub organization_id: Option<Uuid>,
    pub run_number: i32,
    pub attempt: i32,
    pub max_attempts: i32,
    pub worker_id: String,
}

impl ObservedJob {
    #[must_use]
    pub fn new(
        job_id: Uuid,
        job_type: JobTypeName,
        organization_id: Option<Uuid>,
        run_number: i32,
        attempt: i32,
        max_attempts: i32,
        worker_id: impl Into<String>,
    ) -> Self {
        Self {
            job_id,
            job_type,
            organization_id,
            run_number,
            attempt,
            max_attempts,
            worker_id: worker_id.into(),
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct JobRunningEvent {
    pub job: ObservedJob,
}

impl JobRunningEvent {
    #[must_use]
    pub fn new(job: ObservedJob) -> Self {
        Self { job }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct JobSucceededEvent {
    pub job: ObservedJob,
    pub duration: Duration,
    pub progress_done: Option<i64>,
    pub progress_total: Option<i64>,
}

impl JobSucceededEvent {
    #[must_use]
    pub fn new(
        job: ObservedJob,
        duration: Duration,
        progress_done: Option<i64>,
        progress_total: Option<i64>,
    ) -> Self {
        Self {
            job,
            duration,
            progress_done,
            progress_total,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum JobFailureDisposition {
    RetryScheduled {
        retry_delay_ms: i32,
        next_run_at: DateTime<Utc>,
    },
    DeadLettered {
        reason: JobDeadLetterReason,
    },
    Unknown,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct JobFailedEvent {
    pub job: ObservedJob,
    pub duration: Duration,
    pub failure: JobFailure,
    pub disposition: JobFailureDisposition,
}

impl JobFailedEvent {
    #[must_use]
    pub fn new(
        job: ObservedJob,
        duration: Duration,
        failure: JobFailure,
        disposition: JobFailureDisposition,
    ) -> Self {
        Self {
            job,
            duration,
            failure,
            disposition,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum JobCompletionPersistenceOperation {
    Success,
    Failure,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct JobCompletionPersistFailedEvent {
    pub job: ObservedJob,
    pub duration: Duration,
    pub operation: JobCompletionPersistenceOperation,
    pub error: String,
}

impl JobCompletionPersistFailedEvent {
    #[must_use]
    pub fn new(
        job: ObservedJob,
        duration: Duration,
        operation: JobCompletionPersistenceOperation,
        error: impl Into<String>,
    ) -> Self {
        Self {
            job,
            duration,
            operation,
            error: error.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum JobLeaseReapedDisposition {
    ReleasedToPending,
    RetryScheduled {
        retry_delay_ms: i32,
        next_run_at: DateTime<Utc>,
    },
    DeadLettered {
        reason: JobDeadLetterReason,
    },
    Unknown,
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct JobLeaseLostEvent {
    pub job: ObservedJob,
    pub duration: Duration,
    pub failure: JobFailure,
}

impl JobLeaseLostEvent {
    #[must_use]
    pub fn new(job: ObservedJob, duration: Duration, failure: JobFailure) -> Self {
        Self {
            job,
            duration,
            failure,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct JobLeaseReapedEvent {
    pub job: ObservedJob,
    pub failure: JobFailure,
    pub started_without_renewal_heartbeat: bool,
    pub disposition: JobLeaseReapedDisposition,
}

impl JobLeaseReapedEvent {
    #[must_use]
    pub fn new(
        job: ObservedJob,
        failure: JobFailure,
        started_without_renewal_heartbeat: bool,
        disposition: JobLeaseReapedDisposition,
    ) -> Self {
        Self {
            job,
            failure,
            started_without_renewal_heartbeat,
            disposition,
        }
    }
}

#[async_trait]
pub trait JobLifecycleObserver: Send + Sync {
    async fn on_job_running(&self, _event: JobRunningEvent) {}

    async fn on_job_succeeded(&self, _event: JobSucceededEvent) {}

    async fn on_job_failed(&self, _event: JobFailedEvent) {}

    async fn on_job_completion_persist_failed(&self, _event: JobCompletionPersistFailedEvent) {}

    async fn on_job_lease_lost(&self, _event: JobLeaseLostEvent) {}

    async fn on_job_lease_reaped(&self, _event: JobLeaseReapedEvent) {}
}

#[derive(Clone, Default)]
pub struct JobLifecycleObservers {
    observers: Arc<Vec<Arc<dyn JobLifecycleObserver>>>,
}

impl JobLifecycleObservers {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn from_observer(observer: impl JobLifecycleObserver + 'static) -> Self {
        Self {
            observers: Arc::new(vec![Arc::new(observer)]),
        }
    }

    #[must_use]
    pub fn from_arc_observers(observers: Vec<Arc<dyn JobLifecycleObserver>>) -> Self {
        Self {
            observers: Arc::new(observers),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.observers.is_empty()
    }

    pub(crate) async fn job_running(&self, event: JobRunningEvent) {
        let job = event.job.clone();
        self.notify_all_observers(
            "on_job_running",
            event,
            &job,
            |observer, event| async move {
                observer.on_job_running(event).await;
            },
        )
        .await;
    }

    pub(crate) async fn job_succeeded(&self, event: JobSucceededEvent) {
        let job = event.job.clone();
        self.notify_all_observers(
            "on_job_succeeded",
            event,
            &job,
            |observer, event| async move {
                observer.on_job_succeeded(event).await;
            },
        )
        .await;
    }

    pub(crate) async fn job_failed(&self, event: JobFailedEvent) {
        let job = event.job.clone();
        self.notify_all_observers("on_job_failed", event, &job, |observer, event| async move {
            observer.on_job_failed(event).await;
        })
        .await;
    }

    pub(crate) async fn job_completion_persist_failed(
        &self,
        event: JobCompletionPersistFailedEvent,
    ) {
        let job = event.job.clone();
        self.notify_all_observers(
            "on_job_completion_persist_failed",
            event,
            &job,
            |observer, event| async move {
                observer.on_job_completion_persist_failed(event).await;
            },
        )
        .await;
    }

    pub(crate) async fn job_lease_lost(&self, event: JobLeaseLostEvent) {
        let job = event.job.clone();
        self.notify_all_observers(
            "on_job_lease_lost",
            event,
            &job,
            |observer, event| async move {
                observer.on_job_lease_lost(event).await;
            },
        )
        .await;
    }

    pub(crate) async fn job_lease_reaped(&self, event: JobLeaseReapedEvent) {
        let job = event.job.clone();
        self.notify_all_observers(
            "on_job_lease_reaped",
            event,
            &job,
            |observer, event| async move {
                observer.on_job_lease_reaped(event).await;
            },
        )
        .await;
    }

    async fn notify_all_observers<E, F, Fut>(
        &self,
        callback_name: &'static str,
        event: E,
        job: &ObservedJob,
        notify: F,
    ) where
        E: Clone,
        F: Fn(Arc<dyn JobLifecycleObserver>, E) -> Fut,
        Fut: Future<Output = ()> + Send,
    {
        let mut pending = FuturesUnordered::new();

        for observer in self.observers.iter() {
            let observer = Arc::clone(observer);
            let job = ObserverJobLogContext::from(job);
            let event = event.clone();
            pending.push(notify_observer(callback_name, job, notify(observer, event)));
        }

        while pending.next().await.is_some() {}
    }
}

#[derive(Debug)]
struct ObserverJobLogContext {
    job_id: Uuid,
    job_type: String,
    organization_id: Option<Uuid>,
    run_number: i32,
    attempt: i32,
    max_attempts: i32,
    worker_id: String,
}

impl From<&ObservedJob> for ObserverJobLogContext {
    fn from(job: &ObservedJob) -> Self {
        Self {
            job_id: job.job_id,
            job_type: job.job_type.to_string(),
            organization_id: job.organization_id,
            run_number: job.run_number,
            attempt: job.attempt,
            max_attempts: job.max_attempts,
            worker_id: job.worker_id.clone(),
        }
    }
}

async fn notify_observer<F>(callback_name: &'static str, job: ObserverJobLogContext, future: F)
where
    F: Future<Output = ()> + Send,
{
    match tokio::time::timeout(OBSERVER_TIMEOUT, AssertUnwindSafe(future).catch_unwind()).await {
        Ok(Ok(())) => {}
        Ok(Err(panic_payload)) => {
            let panic_message = panic_payload_message(&*panic_payload);
            warn!(
                callback_name,
                job_id = %job.job_id,
                job_type = %job.job_type,
                organization_id = ?job.organization_id,
                run_number = job.run_number,
                attempt = job.attempt,
                max_attempts = job.max_attempts,
                worker_id = %job.worker_id,
                panic = %panic_message,
                "job lifecycle observer panicked"
            );
        }
        Err(_) => {
            warn!(
                callback_name,
                job_id = %job.job_id,
                job_type = %job.job_type,
                organization_id = ?job.organization_id,
                run_number = job.run_number,
                attempt = job.attempt,
                max_attempts = job.max_attempts,
                worker_id = %job.worker_id,
                timeout_ms = OBSERVER_TIMEOUT.as_millis(),
                "job lifecycle observer timed out"
            );
        }
    }
}

fn panic_payload_message(panic_payload: &(dyn Any + Send)) -> String {
    if let Some(message) = panic_payload.downcast_ref::<String>() {
        return message.clone();
    }

    if let Some(message) = panic_payload.downcast_ref::<&'static str>() {
        return (*message).to_string();
    }

    "non-string panic payload".to_string()
}
