use std::future::Future;
use std::sync::Arc;

use runledger_postgres::jobs;
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Duration, timeout};
use tracing::{Instrument, error, info, warn};
use uuid::Uuid;

use crate::observer::{
    JobCompletionPersistFailedEvent, JobContinuedEvent, JobFailedEvent, JobLeaseLostEvent,
    JobLifecycleObservers, JobRunningEvent, JobSucceededEvent, ObservedJob,
};

#[cfg(test)]
const TERMINAL_OBSERVER_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);
#[cfg(not(test))]
// Allow one observer timeout for a same-job running callback to settle, then a
// separate observer timeout for terminal callback delivery.
const TERMINAL_OBSERVER_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(20);
#[cfg(test)]
const TERMINAL_OBSERVER_ABORT_DRAIN_TIMEOUT: Duration = Duration::from_millis(100);
#[cfg(not(test))]
const TERMINAL_OBSERVER_ABORT_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);
const WORKER_OBSERVER_TASK_MAX_CONCURRENCY: usize = 64;

#[derive(Clone)]
pub(super) enum TerminalObserverTasks {
    #[cfg(test)]
    Detached,
    Owned {
        tasks: Arc<AsyncMutex<ObserverTaskSets>>,
    },
}

pub(super) struct ObserverTaskSets {
    terminal: JoinSet<()>,
    running: JoinSet<()>,
    terminal_max_in_flight: usize,
    running_max_in_flight: usize,
}

impl Default for ObserverTaskSets {
    fn default() -> Self {
        Self::new(WORKER_OBSERVER_TASK_MAX_CONCURRENCY)
    }
}

impl ObserverTaskSets {
    fn new(max_in_flight: usize) -> Self {
        Self::new_with_limits(max_in_flight, max_in_flight)
    }

    fn new_with_limits(terminal_max_in_flight: usize, running_max_in_flight: usize) -> Self {
        Self {
            terminal: JoinSet::new(),
            running: JoinSet::new(),
            terminal_max_in_flight,
            running_max_in_flight,
        }
    }

    fn drain_finished(&mut self) {
        drain_finished_terminal_observer_tasks(&mut self.terminal);
        drain_finished_running_observer_tasks(&mut self.running);
    }

    fn in_flight_count(&self) -> usize {
        self.terminal.len() + self.running.len()
    }

    fn try_admit(&mut self, kind: ObserverTaskKind, job: &JobObserverLogContext) -> bool {
        self.drain_finished();

        let current_count = self.in_flight_count_for(kind);
        let max_in_flight = self.max_in_flight_for(kind);
        if current_count < max_in_flight {
            return true;
        }

        warn!(
            observer_task_kind = kind.as_str(),
            observer_task_cap = max_in_flight,
            observer_task_current_count = current_count,
            observer_task_total_count = self.in_flight_count(),
            job_id = %job.job_id,
            job_type = %job.job_type,
            run_number = job.run_number,
            attempt = job.attempt,
            "worker observer task limit reached; dropping best-effort observer task"
        );
        false
    }

    fn in_flight_count_for(&self, kind: ObserverTaskKind) -> usize {
        match kind {
            ObserverTaskKind::Terminal => self.terminal.len(),
            #[cfg(test)]
            ObserverTaskKind::RunningDrain => self.running.len(),
        }
    }

    fn max_in_flight_for(&self, kind: ObserverTaskKind) -> usize {
        match kind {
            ObserverTaskKind::Terminal => self.terminal_max_in_flight,
            #[cfg(test)]
            ObserverTaskKind::RunningDrain => self.running_max_in_flight,
        }
    }
}

#[derive(Clone, Copy)]
enum ObserverTaskKind {
    Terminal,
    #[cfg(test)]
    RunningDrain,
}

impl ObserverTaskKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Terminal => "terminal",
            #[cfg(test)]
            Self::RunningDrain => "running_drain",
        }
    }
}

impl TerminalObserverTasks {
    #[cfg(test)]
    pub(super) fn detached() -> Self {
        Self::Detached
    }

    pub(super) fn owned() -> Self {
        Self::owned_with_task_sets(ObserverTaskSets::default())
    }

    #[cfg(test)]
    pub(super) fn owned_with_max_concurrency(max_in_flight: usize) -> Self {
        Self::owned_with_task_sets(ObserverTaskSets::new(max_in_flight))
    }

    fn owned_with_task_sets(task_sets: ObserverTaskSets) -> Self {
        Self::Owned {
            tasks: Arc::new(AsyncMutex::new(task_sets)),
        }
    }

    #[cfg(test)]
    pub(super) async fn spawn_terminal<F>(&self, future: F, job: &JobObserverLogContext) -> bool
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_terminal_if_admitted(job, || future).await
    }

    async fn spawn_terminal_if_admitted<F, Fut>(
        &self,
        job: &JobObserverLogContext,
        future: F,
    ) -> bool
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = ()> + Send + 'static,
    {
        match self {
            #[cfg(test)]
            Self::Detached => {
                drop(tokio::spawn(future()));
                true
            }
            Self::Owned { tasks } => {
                let mut tasks = tasks.lock().await;
                if tasks.try_admit(ObserverTaskKind::Terminal, job) {
                    let _ = tasks.terminal.spawn(future());
                    true
                } else {
                    false
                }
            }
        }
    }

    #[cfg(test)]
    pub(super) async fn spawn_running(
        &self,
        running_handle: RunningObserverHandle,
        span: tracing::Span,
    ) {
        match self {
            #[cfg(test)]
            Self::Detached => {
                drop(tokio::spawn(async move {
                    running_handle.drain().instrument(span).await;
                }));
            }
            Self::Owned { tasks } => {
                let mut tasks = tasks.lock().await;
                if tasks.try_admit(ObserverTaskKind::RunningDrain, &running_handle.job) {
                    let _ = tasks
                        .running
                        .spawn(async move { running_handle.drain().instrument(span).await });
                }
            }
        }
    }

    pub(super) async fn drain_finished(&self) {
        match self {
            #[cfg(test)]
            Self::Detached => {}
            Self::Owned { tasks } => {
                tasks.lock().await.drain_finished();
            }
        }
    }

    #[cfg(test)]
    pub(super) async fn in_flight_count(&self) -> usize {
        match self {
            Self::Detached => 0,
            Self::Owned { tasks } => tasks.lock().await.in_flight_count(),
        }
    }

    pub(super) async fn drain_for_shutdown(&self) {
        let tasks = match self {
            #[cfg(test)]
            Self::Detached => return,
            Self::Owned { tasks } => tasks,
        };
        let mut tasks = {
            let mut guard = tasks.lock().await;
            let replacement = ObserverTaskSets::new_with_limits(
                guard.terminal_max_in_flight,
                guard.running_max_in_flight,
            );
            std::mem::replace(&mut *guard, replacement)
        };

        drain_terminal_observers_for_shutdown(&mut tasks.terminal).await;
        abort_running_observers_for_shutdown(&mut tasks.running).await;
    }
}

fn drain_finished_terminal_observer_tasks(tasks: &mut JoinSet<()>) {
    while let Some(result) = tasks.try_join_next() {
        if let Err(error) = result {
            error!(%error, "terminal observer task crashed");
        }
    }
}

fn drain_finished_running_observer_tasks(tasks: &mut JoinSet<()>) {
    while let Some(result) = tasks.try_join_next() {
        if let Err(error) = result {
            error!(%error, "running observer drain task crashed");
        }
    }
}

async fn drain_terminal_observers_for_shutdown(tasks: &mut JoinSet<()>) {
    if tasks.is_empty() {
        return;
    }

    info!(
        terminal_observer_tasks = tasks.len(),
        timeout_ms = TERMINAL_OBSERVER_SHUTDOWN_DRAIN_TIMEOUT.as_millis(),
        "worker shutdown requested; draining terminal observer tasks"
    );

    match timeout(
        TERMINAL_OBSERVER_SHUTDOWN_DRAIN_TIMEOUT,
        drain_terminal_observer_tasks(tasks),
    )
    .await
    {
        Ok(()) => {}
        Err(_) => {
            warn!(
                remaining_terminal_observer_tasks = tasks.len(),
                timeout_ms = TERMINAL_OBSERVER_SHUTDOWN_DRAIN_TIMEOUT.as_millis(),
                "terminal observer tasks did not finish before worker shutdown deadline; aborting"
            );
            tasks.abort_all();

            match timeout(
                TERMINAL_OBSERVER_ABORT_DRAIN_TIMEOUT,
                drain_aborted_terminal_observer_tasks(tasks),
            )
            .await
            {
                Ok(cancelled_count) => {
                    if cancelled_count > 0 {
                        info!(
                            cancelled_terminal_observer_tasks = cancelled_count,
                            "terminal observer tasks cancelled during worker shutdown"
                        );
                    }
                }
                Err(_) => {
                    warn!(
                        remaining_terminal_observer_tasks = tasks.len(),
                        timeout_ms = TERMINAL_OBSERVER_ABORT_DRAIN_TIMEOUT.as_millis(),
                        "terminal observer abort drain timed out during worker shutdown; dropping unresolved tasks"
                    );
                }
            }
        }
    }
}

async fn drain_terminal_observer_tasks(tasks: &mut JoinSet<()>) {
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result {
            error!(%error, "terminal observer task crashed while draining worker shutdown");
        }
    }
}

async fn drain_aborted_terminal_observer_tasks(tasks: &mut JoinSet<()>) -> usize {
    let mut cancelled_count = 0;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(()) => {}
            Err(error) if error.is_cancelled() => {
                cancelled_count += 1;
            }
            Err(error) => {
                error!(%error, "terminal observer task failed after worker shutdown abort");
            }
        }
    }
    cancelled_count
}

async fn abort_running_observers_for_shutdown(tasks: &mut JoinSet<()>) {
    if tasks.is_empty() {
        return;
    }

    info!(
        running_observer_tasks = tasks.len(),
        timeout_ms = TERMINAL_OBSERVER_ABORT_DRAIN_TIMEOUT.as_millis(),
        "worker shutdown requested; aborting running observer tasks"
    );
    tasks.abort_all();

    match timeout(
        TERMINAL_OBSERVER_ABORT_DRAIN_TIMEOUT,
        drain_aborted_running_observer_tasks(tasks),
    )
    .await
    {
        Ok(cancelled_count) => {
            if cancelled_count > 0 {
                info!(
                    cancelled_running_observer_tasks = cancelled_count,
                    "running observer tasks cancelled during worker shutdown"
                );
            }
        }
        Err(_) => {
            warn!(
                remaining_running_observer_tasks = tasks.len(),
                timeout_ms = TERMINAL_OBSERVER_ABORT_DRAIN_TIMEOUT.as_millis(),
                "running observer abort drain timed out during worker shutdown; dropping unresolved tasks"
            );
        }
    }
}

async fn drain_aborted_running_observer_tasks(tasks: &mut JoinSet<()>) -> usize {
    let mut cancelled_count = 0;
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(()) => {}
            Err(error) if error.is_cancelled() => {
                cancelled_count += 1;
            }
            Err(error) => {
                error!(%error, "running observer drain task failed after worker shutdown abort");
            }
        }
    }
    cancelled_count
}

pub(super) enum TerminalJobObserverEvent {
    Continued(JobContinuedEvent),
    Succeeded(JobSucceededEvent),
    Failed(JobFailedEvent),
    CompletionPersistFailed(JobCompletionPersistFailedEvent),
    LeaseLost(JobLeaseLostEvent),
}

impl TerminalJobObserverEvent {
    async fn notify(self, observers: &JobLifecycleObservers) {
        match self {
            Self::Continued(event) => observers.job_continued(event).await,
            Self::Succeeded(event) => observers.job_succeeded(event).await,
            Self::Failed(event) => observers.job_failed(event).await,
            Self::CompletionPersistFailed(event) => {
                observers.job_completion_persist_failed(event).await;
            }
            Self::LeaseLost(event) => observers.job_lease_lost(event).await,
        }
    }
}

#[derive(Clone)]
pub(super) struct JobObserverLogContext {
    pub(super) job_id: Uuid,
    pub(super) job_type: String,
    pub(super) run_number: i32,
    pub(super) attempt: i32,
}

impl JobObserverLogContext {
    pub(super) fn from_job(job: &jobs::JobQueueRecord) -> Self {
        Self {
            job_id: job.id,
            job_type: job.job_type.to_string(),
            run_number: job.run_number,
            attempt: job.attempt,
        }
    }
}

pub(super) struct JobRunningNotification {
    #[cfg(test)]
    pub(super) handle: Option<JoinHandle<()>>,
    #[cfg(not(test))]
    handle: Option<JoinHandle<()>>,
}

impl JobRunningNotification {
    pub(super) fn spawn(observers: JobLifecycleObservers, job: ObservedJob) -> Self {
        if observers.is_empty() {
            return Self { handle: None };
        }

        let span = tracing::Span::current();
        let handle = tokio::spawn(
            async move {
                observers.job_running(JobRunningEvent { job }).await;
            }
            .instrument(span),
        );
        Self {
            handle: Some(handle),
        }
    }

    pub(super) async fn spawn_terminal_observer(
        &mut self,
        terminal_observer_tasks: &TerminalObserverTasks,
        job: &jobs::JobQueueRecord,
        observers: JobLifecycleObservers,
        event: TerminalJobObserverEvent,
    ) {
        if observers.is_empty() {
            return;
        }

        let span = tracing::Span::current();
        let job_log_context = JobObserverLogContext::from_job(job);
        if let Some(handle) = self.handle.take_if(|handle| handle.is_finished()) {
            RunningObserverHandle::new(handle, job_log_context.clone())
                .drain()
                .await;
        }
        terminal_observer_tasks
            .spawn_terminal_if_admitted(&job_log_context, || {
                let running_handle = self
                    .handle
                    .take()
                    .map(|handle| RunningObserverHandle::new(handle, job_log_context.clone()));
                async move {
                    if let Some(running_handle) = running_handle {
                        running_handle.drain().await;
                    }
                    event.notify(&observers).await;
                }
                .instrument(span)
            })
            .await;
    }
}

impl Drop for JobRunningNotification {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

pub(super) struct RunningObserverHandle {
    handle: Option<JoinHandle<()>>,
    pub(super) job: JobObserverLogContext,
}

impl RunningObserverHandle {
    pub(super) fn new(handle: JoinHandle<()>, job: JobObserverLogContext) -> Self {
        Self {
            handle: Some(handle),
            job,
        }
    }

    async fn drain(mut self) {
        let Some(handle) = self.handle.take() else {
            return;
        };
        let mut guard = RunningObserverJoinGuard::new(handle, self.job.clone());
        let result = (&mut guard.handle).await;
        guard.completed = true;

        if let Err(error) = result {
            log_running_notification_join_error(&self.job, error);
        }
    }
}

impl Drop for RunningObserverHandle {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            warn!(
                job_id = %self.job.job_id,
                job_type = %self.job.job_type,
                run_number = self.job.run_number,
                attempt = self.job.attempt,
                "job running observer task dropped before completion; aborting"
            );
            handle.abort();
        }
    }
}

struct RunningObserverJoinGuard {
    handle: JoinHandle<()>,
    job: JobObserverLogContext,
    completed: bool,
}

impl RunningObserverJoinGuard {
    fn new(handle: JoinHandle<()>, job: JobObserverLogContext) -> Self {
        Self {
            handle,
            job,
            completed: false,
        }
    }
}

impl Drop for RunningObserverJoinGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }

        warn!(
            job_id = %self.job.job_id,
            job_type = %self.job.job_type,
            run_number = self.job.run_number,
            attempt = self.job.attempt,
            "job running observer task dropped before completion; aborting"
        );
        self.handle.abort();
    }
}

fn log_running_notification_join_error(job: &JobObserverLogContext, error: tokio::task::JoinError) {
    if error.is_panic() {
        warn!(
            job_id = %job.job_id,
            job_type = %job.job_type,
            run_number = job.run_number,
            attempt = job.attempt,
            error = %error,
            "job running observer task panicked; continuing worker job task"
        );
    } else if error.is_cancelled() {
        warn!(
            job_id = %job.job_id,
            job_type = %job.job_type,
            run_number = job.run_number,
            attempt = job.attempt,
            error = %error,
            "job running observer task was cancelled; continuing worker job task"
        );
    } else {
        warn!(
            job_id = %job.job_id,
            job_type = %job.job_type,
            run_number = job.run_number,
            attempt = job.attempt,
            error = %error,
            "job running observer task join failed; continuing worker job task"
        );
    }
}
