use std::borrow::Borrow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::runtime::Handle;
use tokio::sync::watch;
use tokio::task::{AbortHandle, JoinError, JoinHandle};
use tokio::time::Instant;
use tracing::{Instrument, debug, error, info_span, warn};

use crate::catalog::JobCatalog;
use crate::config::JobsConfig;
use crate::reaper::run_reaper_loop;
use crate::registry::JobRegistry;
use crate::scheduler::run_scheduler_loop;
use crate::worker::run_worker_loop;
use crate::{Error, Result, RuntimeError, RuntimeLoopExit};

const WORKER_TASK: &str = "worker";
const SCHEDULER_TASK: &str = "scheduler";
const REAPER_TASK: &str = "reaper";
const MAX_ABORT_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

/// Supervises the Runledger runtime loops spawned for a worker process.
///
/// A supervisor owns the worker, scheduler, and reaper task handles selected by
/// [`SupervisorBuilder`]. Use [`Self::run_until_shutdown`] for a typical worker
/// process that should exit on either an external shutdown signal or an internal
/// runtime task failure.
///
/// Dropping a supervisor requests shutdown and detaches the task handles. Call
/// [`Self::shutdown`] or [`Self::join`] when the owning process needs to observe
/// panics or unexpected task exits.
#[must_use]
pub struct Supervisor {
    shutdown_tx: watch::Sender<bool>,
    shutdown_requested: Arc<AtomicBool>,
    tasks: Vec<RuntimeTask>,
}

/// Builds a [`Supervisor`] with configurable runtime loops.
///
/// Worker, scheduler, and reaper loops are enabled by default. Call
/// [`Self::with_registry`] or [`Self::with_catalog`] before [`Self::build`] when
/// worker or reaper loops remain enabled.
#[must_use]
pub struct SupervisorBuilder<'a> {
    pool: &'a runledger_postgres::DbPool,
    runtime: Handle,
    registry: Option<JobRegistry>,
    registry_source: Option<RegistrySource>,
    mixed_registry_sources: bool,
    config: JobsConfig,
    worker_enabled: bool,
    scheduler_enabled: bool,
    reaper_enabled: bool,
}

/// Cloneable handle for requesting supervisor shutdown from another task.
#[derive(Clone)]
pub struct SupervisorShutdown {
    shutdown_tx: watch::Sender<bool>,
    shutdown_requested: Arc<AtomicBool>,
}

struct RuntimeTask {
    name: &'static str,
    handle: JoinHandle<RuntimeTaskExit>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RegistrySource {
    Registry,
    Catalog,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeTaskExit {
    Completed,
    Shutdown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DrainResult {
    Drained,
    TimedOut,
}

struct RuntimeTaskFuture {
    name: &'static str,
    future: Pin<Box<dyn Future<Output = RuntimeTaskExit> + Send>>,
    started: bool,
}

type RuntimeTaskJoinResult = std::result::Result<RuntimeTaskExit, JoinError>;
type JoinedRuntimeTask = (&'static str, RuntimeTaskJoinResult);

impl Supervisor {
    /// Returns a builder for a supervisor over a shared pool and runtime
    /// configuration.
    ///
    /// This validates that the caller is inside the Tokio runtime that will own
    /// spawned supervisor tasks.
    pub fn builder(
        pool: &runledger_postgres::DbPool,
        config: JobsConfig,
    ) -> std::result::Result<SupervisorBuilder<'_>, RuntimeError> {
        let runtime =
            Handle::try_current().map_err(|source| RuntimeError::MissingTokioRuntime { source })?;

        Ok(SupervisorBuilder {
            pool,
            runtime,
            registry: None,
            registry_source: None,
            mixed_registry_sources: false,
            config,
            worker_enabled: true,
            scheduler_enabled: true,
            reaper_enabled: true,
        })
    }

    /// Returns a cloneable shutdown handle that can request shutdown without
    /// owning the supervisor task joins.
    #[must_use]
    pub fn shutdown_handle(&self) -> SupervisorShutdown {
        SupervisorShutdown {
            shutdown_tx: self.shutdown_tx.clone(),
            shutdown_requested: Arc::clone(&self.shutdown_requested),
        }
    }

    /// Requests graceful shutdown of all supervised loops.
    pub fn request_shutdown(&self) {
        request_shutdown_signal(&self.shutdown_tx, self.shutdown_requested.as_ref());
    }

    /// Returns whether shutdown has been requested through this supervisor or a
    /// clone of its shutdown handle.
    #[must_use]
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
    }

    /// Waits for all supervised loops to exit.
    ///
    /// With the default long-running loops, this method waits until shutdown is
    /// requested through a [`SupervisorShutdown`] handle or until a task exits.
    /// If a loop exits before shutdown was requested, the remaining loops are
    /// asked to shut down and the first observed error is returned. Additional
    /// task failures observed while draining are logged. This method does not
    /// impose a deadline; use [`Self::shutdown_with_timeout`] when the caller
    /// owns shutdown and needs a bounded wait.
    pub async fn join(mut self) -> Result<()> {
        let tasks = std::mem::take(&mut self.tasks);
        self.join_tasks(tasks).await
    }

    /// Requests graceful shutdown and waits for all supervised loops to exit.
    ///
    /// If a loop exits before shutdown was requested, the remaining loops are
    /// asked to shut down and the pre-existing task exit is reported, even when
    /// that exit is only observed after shutdown begins. This method does not
    /// impose a deadline. Use [`Self::shutdown_with_timeout`] when the owning
    /// process needs a shutdown budget; externally timing out this consuming
    /// future can detach still-running task handles.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(error) = join_pre_shutdown_finished_tasks(&mut self.tasks).await {
            self.request_shutdown();
            let tasks = std::mem::take(&mut self.tasks);
            drain_tasks(tasks).await;
            return Err(Error::Runtime(error));
        }

        self.request_shutdown();
        let tasks = std::mem::take(&mut self.tasks);
        self.join_tasks(tasks).await
    }

    /// Waits until `shutdown` resolves or a supervised task fails, then exits.
    ///
    /// If `shutdown` resolves first, graceful shutdown is requested and the
    /// supervisor waits up to `timeout` for all loops to exit. If a loop panics
    /// or exits unexpectedly before `shutdown` resolves, shutdown is requested
    /// for the remaining loops and the original task error is returned after
    /// those loops drain or a timeout is reported. If shutdown is requested
    /// through a [`SupervisorShutdown`] handle and every loop exits cleanly before
    /// `shutdown` resolves, this returns successfully.
    ///
    /// This is the preferred method for worker binaries because it observes
    /// internal task failures during normal operation while still applying a
    /// bounded shutdown budget to cooperative process termination.
    ///
    /// If `timeout` is too large to represent as a runtime deadline, this returns
    /// [`RuntimeError::ShutdownTimeoutTooLarge`] immediately. A zero timeout
    /// requests shutdown, aborts tasks without waiting for cooperative exits, and
    /// reports [`RuntimeError::ShutdownTimeout`].
    ///
    /// If the initial timeout validation fails before `shutdown` resolves, the
    /// supervisor is still dropped, so shutdown is requested, but task handles
    /// are not aborted or drained. If a deadline overflow is detected after
    /// shutdown begins, remaining tasks are aborted and drained before returning.
    pub async fn run_until_shutdown<F>(mut self, shutdown: F, timeout: Duration) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        // Validate the shutdown budget before waiting on a signal, then
        // recompute the deadline when shutdown actually begins.
        let _ = shutdown_deadline(timeout)?;
        let tasks = std::mem::take(&mut self.tasks);
        if tasks.is_empty() {
            shutdown.await;
            self.request_shutdown();
            return Ok(());
        }

        let mut abort_handles = Some(task_abort_handles(&tasks));
        let mut joined = join_runtime_tasks(tasks);
        let mut shutdown = std::pin::pin!(shutdown);

        loop {
            tokio::select! {
                _ = shutdown.as_mut() => {
                    self.request_shutdown();
                    let abort_handles = abort_handles.take().expect("abort handles are consumed on return");
                    // The budget was validated before waiting; recompute here so
                    // the timeout starts when shutdown begins, and abort instead
                    // of detaching if this late recompute somehow overflows.
                    let deadline = match shutdown_deadline(timeout) {
                        Ok(deadline) => deadline,
                        Err(error) => {
                            abort_and_drain_joined_tasks_or_log(
                                &mut joined,
                                abort_handles,
                                abort_drain_timeout(timeout),
                            )
                            .await;
                            return Err(error.into());
                        }
                    };
                    return self
                        .join_joined_tasks_with_timeout(
                            &mut joined,
                            abort_handles,
                            timeout,
                            deadline,
                        )
                        .await;
                }
                joined_result = joined.next() => {
                    let Some((task, result)) = joined_result else {
                        return Ok(());
                    };
                    let Some(error) = classify_task_result(task, result) else {
                        continue;
                    };

                    self.request_shutdown();
                    let abort_handles = abort_handles.take().expect("abort handles are consumed on return");
                    // Start the drain budget when shutdown begins. If this
                    // pathological recompute overflows, abort already-running
                    // tasks rather than dropping their handles detached.
                    let deadline = match shutdown_deadline(timeout) {
                        Ok(deadline) => deadline,
                        Err(error) => {
                            abort_and_drain_joined_tasks_or_log(
                                &mut joined,
                                abort_handles,
                                abort_drain_timeout(timeout),
                            )
                            .await;
                            return Err(error.into());
                        }
                    };
                    return drain_after_task_error_with_timeout(
                        &mut joined,
                        abort_handles,
                        timeout,
                        deadline,
                        error,
                    )
                    .await;
                }
            }
        }
    }

    /// Requests graceful shutdown and waits up to `timeout` for all supervised
    /// loops to exit.
    ///
    /// If a loop had already exited before this method begins shutdown, that
    /// failure is returned after the remaining loops have had the same shutdown
    /// budget to exit cooperatively. If the timeout expires, remaining tasks are
    /// aborted and drained with a bounded cleanup attempt before a timeout error
    /// is returned. Abort cleanup can make total wall-clock time exceed `timeout`
    /// by up to one second, or `timeout`, whichever is smaller. A zero timeout
    /// requests shutdown, immediately aborts tasks that did not already finish,
    /// and reports [`RuntimeError::ShutdownTimeout`].
    ///
    /// If `timeout` is too large to represent as a runtime deadline, this returns
    /// [`RuntimeError::ShutdownTimeoutTooLarge`] immediately. The supervisor is
    /// still dropped, so shutdown is requested, but task handles are not aborted
    /// or drained.
    pub async fn shutdown_with_timeout(mut self, timeout: Duration) -> Result<()> {
        let deadline = shutdown_deadline(timeout)?;

        if let Some(error) = join_pre_shutdown_finished_tasks(&mut self.tasks).await {
            self.request_shutdown();
            let tasks = std::mem::take(&mut self.tasks);
            let abort_handles = task_abort_handles(&tasks);
            let mut joined = join_runtime_tasks(tasks);

            return drain_after_task_error_with_timeout(
                &mut joined,
                abort_handles,
                timeout,
                deadline,
                error,
            )
            .await;
        }

        self.request_shutdown();
        let tasks = std::mem::take(&mut self.tasks);
        self.join_tasks_with_timeout(tasks, timeout, deadline).await
    }

    async fn join_tasks(&self, tasks: Vec<RuntimeTask>) -> Result<()> {
        let mut joined = join_runtime_tasks(tasks);

        while let Some((task, result)) = joined.next().await {
            if let Some(error) = classify_task_result(task, result) {
                self.request_shutdown();
                drain_joined_tasks(&mut joined).await;
                return Err(Error::Runtime(error));
            }
        }

        Ok(())
    }

    async fn join_tasks_with_timeout(
        &self,
        tasks: Vec<RuntimeTask>,
        timeout: Duration,
        deadline: Instant,
    ) -> Result<()> {
        let abort_handles = task_abort_handles(&tasks);
        let mut joined = join_runtime_tasks(tasks);

        self.join_joined_tasks_with_timeout(&mut joined, abort_handles, timeout, deadline)
            .await
    }

    async fn join_joined_tasks_with_timeout(
        &self,
        joined: &mut FuturesUnordered<impl Future<Output = JoinedRuntimeTask>>,
        abort_handles: Vec<AbortHandle>,
        timeout: Duration,
        deadline: Instant,
    ) -> Result<()> {
        loop {
            match tokio::time::timeout_at(deadline, joined.next()).await {
                Ok(Some((task, result))) => {
                    if let Some(error) = classify_task_result(task, result) {
                        self.request_shutdown();
                        return drain_after_task_error_with_timeout(
                            joined,
                            abort_handles,
                            timeout,
                            deadline,
                            error,
                        )
                        .await;
                    }
                }
                Ok(None) => return Ok(()),
                Err(_) => {
                    abort_and_drain_joined_tasks_or_log(
                        joined,
                        abort_handles,
                        abort_drain_timeout(timeout),
                    )
                    .await;
                    return Err(Error::Runtime(RuntimeError::ShutdownTimeout { timeout }));
                }
            }
        }
    }

    #[cfg(test)]
    fn from_tasks_for_tests(tasks: Vec<RuntimeTask>) -> Self {
        // These synthetic tasks do not receive this channel; tests using this
        // helper either finish without shutdown or exercise timeout abort paths.
        let (shutdown_tx, _) = watch::channel(false);
        Self {
            shutdown_tx,
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            tasks,
        }
    }
}

impl Drop for Supervisor {
    fn drop(&mut self) {
        if !self.tasks.is_empty() {
            warn!(
                task_count = self.tasks.len(),
                "dropping jobs runtime supervisor before joining tasks; tasks may continue detached after shutdown is requested and later panics will not be observed"
            );
        }
        // Drop cannot await task handles, so this only nudges loops to exit.
        self.request_shutdown();
    }
}

impl<'a> SupervisorBuilder<'a> {
    /// Registers the handlers used by worker execution and reaper terminal hooks.
    ///
    /// A registry is required when worker or reaper loops are enabled. Scheduler-only
    /// supervisors can be built without one.
    #[must_use = "builder methods return an updated builder value"]
    pub fn with_registry(mut self, registry: JobRegistry) -> Self {
        self.mixed_registry_sources |= self.registry_source == Some(RegistrySource::Catalog);
        self.registry_source = Some(RegistrySource::Registry);
        self.registry = Some(registry);
        self
    }

    /// Registers handlers from a [`JobCatalog`].
    ///
    /// This does not sync database job definitions. Call
    /// [`JobCatalog::sync_definitions`] before starting the supervisor or
    /// creating schedules. Pass `&catalog` when the caller will continue using
    /// the catalog for schedule, enqueue, or workflow helpers after building the
    /// supervisor.
    ///
    /// # Registry Source
    ///
    /// Calling this and [`Self::with_registry`] on the same builder is rejected
    /// by [`Self::build`]. Choose one registration source per builder.
    #[must_use = "builder methods return an updated builder value"]
    pub fn with_catalog(mut self, catalog: impl Borrow<JobCatalog>) -> Self {
        self.mixed_registry_sources |= self.registry_source == Some(RegistrySource::Registry);
        self.registry_source = Some(RegistrySource::Catalog);
        self.registry = Some(catalog.borrow().to_registry());
        self
    }

    /// Disables worker job claiming and execution for this supervisor.
    #[must_use = "builder methods return an updated builder value"]
    pub fn disable_worker(mut self) -> Self {
        self.worker_enabled = false;
        self
    }

    /// Disables cron schedule materialization for this supervisor.
    #[must_use = "builder methods return an updated builder value"]
    pub fn disable_scheduler(mut self) -> Self {
        self.scheduler_enabled = false;
        self
    }

    /// Disables expired-lease reaping for this supervisor.
    #[must_use = "builder methods return an updated builder value"]
    pub fn disable_reaper(mut self) -> Self {
        self.reaper_enabled = false;
        self
    }

    /// Starts the enabled runtime loops and returns the owning supervisor.
    ///
    /// Returns an error when worker or reaper loops are enabled without a job
    /// registry.
    pub fn build(self) -> std::result::Result<Supervisor, RuntimeError> {
        let Self {
            pool,
            runtime,
            registry,
            registry_source: _,
            mixed_registry_sources,
            config,
            worker_enabled,
            scheduler_enabled,
            reaper_enabled,
        } = self;

        if mixed_registry_sources {
            return Err(RuntimeError::MixedRegistrySources);
        }

        let registry = match registry {
            Some(registry) => registry,
            None if worker_enabled || reaper_enabled => {
                return Err(RuntimeError::MissingRegistry {
                    worker_enabled,
                    reaper_enabled,
                });
            }
            None => JobRegistry::new(),
        };

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let mut tasks = Vec::new();

        if worker_enabled {
            tasks.push(RuntimeTask::spawn_on(&runtime, WORKER_TASK, {
                let pool = pool.clone();
                let registry = registry.clone();
                let config = config.clone();
                let shutdown_rx = shutdown_rx.clone();
                async move { run_worker_loop(pool, registry, config, shutdown_rx).await }
            }));
        }

        if scheduler_enabled {
            tasks.push(RuntimeTask::spawn_on(&runtime, SCHEDULER_TASK, {
                let pool = pool.clone();
                let config = config.clone();
                let shutdown_rx = shutdown_rx.clone();
                async move { run_scheduler_loop(pool, config, shutdown_rx).await }
            }));
        }

        if reaper_enabled {
            let pool = pool.clone();
            let registry = registry.clone();
            let config = config.clone();
            let shutdown_rx = shutdown_rx.clone();
            tasks.push(RuntimeTask::spawn_on(&runtime, REAPER_TASK, async move {
                run_reaper_loop(pool, registry, config, shutdown_rx).await
            }));
        }

        Ok(Supervisor {
            shutdown_tx,
            shutdown_requested,
            tasks,
        })
    }
}

impl SupervisorShutdown {
    /// Requests graceful shutdown of all loops watched by the supervisor.
    pub fn request_shutdown(&self) {
        request_shutdown_signal(&self.shutdown_tx, self.shutdown_requested.as_ref());
    }

    /// Returns whether shutdown has been requested.
    #[must_use]
    pub fn is_shutdown_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
    }
}

impl RuntimeTask {
    fn spawn_on<F>(runtime: &Handle, name: &'static str, future: F) -> Self
    where
        F: Future<Output = RuntimeLoopExit> + Send + 'static,
    {
        let span = info_span!("runledger_runtime_supervisor_task", task = name);
        Self {
            name,
            handle: runtime.spawn(
                RuntimeTaskFuture::new(name, async move { future.await.into() }).instrument(span),
            ),
        }
    }

    #[cfg(test)]
    fn spawn<F>(name: &'static str, future: F) -> Self
    where
        F: Future<Output = RuntimeTaskExit> + Send + 'static,
    {
        Self {
            name,
            handle: tokio::spawn(RuntimeTaskFuture::new(name, future)),
        }
    }

    async fn await_result(self) -> RuntimeTaskJoinResult {
        self.handle.await
    }
}

impl RuntimeTaskFuture {
    fn new<F>(name: &'static str, future: F) -> Self
    where
        F: Future<Output = RuntimeTaskExit> + Send + 'static,
    {
        Self {
            name,
            future: Box::pin(future),
            started: false,
        }
    }
}

impl Future for RuntimeTaskFuture {
    type Output = RuntimeTaskExit;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let task = self.as_mut().get_mut();
        if !task.started {
            task.started = true;
            debug!(task = task.name, "supervised runtime task started");
        }

        match task.future.as_mut().poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(exit) => {
                debug!(task = task.name, ?exit, "supervised runtime task completed");
                Poll::Ready(exit)
            }
        }
    }
}

impl From<RuntimeLoopExit> for RuntimeTaskExit {
    fn from(exit: RuntimeLoopExit) -> Self {
        match exit {
            RuntimeLoopExit::Shutdown => Self::Shutdown,
            RuntimeLoopExit::Completed => Self::Completed,
        }
    }
}

fn request_shutdown_signal(shutdown_tx: &watch::Sender<bool>, shutdown_requested: &AtomicBool) {
    if !shutdown_requested.swap(true, Ordering::SeqCst) {
        // Mark the observable flag before notifying receivers so status readers
        // agree that shutdown has begun as soon as request_shutdown returns.
        // Safe to ignore: send failure means every shutdown receiver was dropped,
        // so there are no supervised loops left to notify.
        let _ = shutdown_tx.send(true);
    }
}

fn take_finished_tasks(tasks: &mut Vec<RuntimeTask>) -> Vec<RuntimeTask> {
    let mut finished = Vec::new();
    let mut index = 0;
    while index < tasks.len() {
        if tasks[index].handle.is_finished() {
            // Preserving order is not required for draining, and swap_remove
            // avoids shifting still-running task handles.
            finished.push(tasks.swap_remove(index));
        } else {
            index += 1;
        }
    }
    finished
}

async fn join_pre_shutdown_finished_tasks(tasks: &mut Vec<RuntimeTask>) -> Option<RuntimeError> {
    let finished = take_finished_tasks(tasks);
    let mut first_error = None;

    for task in finished {
        let task_name = task.name;
        let result = task.await_result().await;
        let Some(error) = classify_task_result(task_name, result) else {
            continue;
        };

        if first_error.is_none() {
            first_error = Some(error);
        } else {
            log_drained_task_error(error);
        }
    }

    first_error
}

fn join_runtime_tasks(
    tasks: Vec<RuntimeTask>,
) -> FuturesUnordered<impl Future<Output = JoinedRuntimeTask>> {
    tasks
        .into_iter()
        .map(|task| async move {
            let name = task.name;
            (name, task.await_result().await)
        })
        .collect()
}

async fn drain_tasks(tasks: Vec<RuntimeTask>) {
    let mut joined = join_runtime_tasks(tasks);
    drain_joined_tasks(&mut joined).await;
}

fn task_abort_handles(tasks: &[RuntimeTask]) -> Vec<AbortHandle> {
    tasks
        .iter()
        .map(|task| task.handle.abort_handle())
        .collect()
}

fn shutdown_deadline(timeout: Duration) -> std::result::Result<Instant, RuntimeError> {
    Instant::now()
        .checked_add(timeout)
        .ok_or(RuntimeError::ShutdownTimeoutTooLarge { timeout })
}

fn abort_drain_timeout(timeout: Duration) -> Duration {
    timeout.min(MAX_ABORT_DRAIN_TIMEOUT)
}

async fn drain_joined_tasks(
    joined: &mut FuturesUnordered<impl Future<Output = JoinedRuntimeTask>>,
) {
    while let Some((task, result)) = joined.next().await {
        if let Some(error) = classify_task_result(task, result) {
            log_drained_task_error(error);
        }
    }
}

async fn drain_joined_tasks_until_deadline(
    joined: &mut FuturesUnordered<impl Future<Output = JoinedRuntimeTask>>,
    deadline: Instant,
) -> DrainResult {
    loop {
        match tokio::time::timeout_at(deadline, joined.next()).await {
            Ok(Some((task, result))) => {
                if let Some(error) = classify_task_result(task, result) {
                    log_drained_task_error(error);
                }
            }
            Ok(None) => return DrainResult::Drained,
            Err(_) => return DrainResult::TimedOut,
        }
    }
}

async fn drain_after_task_error_with_timeout(
    joined: &mut FuturesUnordered<impl Future<Output = JoinedRuntimeTask>>,
    abort_handles: Vec<AbortHandle>,
    timeout: Duration,
    deadline: Instant,
    error: RuntimeError,
) -> Result<()> {
    if matches!(
        drain_joined_tasks_until_deadline(joined, deadline).await,
        DrainResult::Drained
    ) {
        return Err(error.into());
    }

    abort_and_drain_joined_tasks_or_log(joined, abort_handles, abort_drain_timeout(timeout)).await;
    Err(RuntimeError::ShutdownTimeoutAfterTaskError {
        timeout,
        source: Box::new(error),
    }
    .into())
}

async fn abort_and_drain_joined_tasks_with_timeout(
    joined: &mut FuturesUnordered<impl Future<Output = JoinedRuntimeTask>>,
    abort_handles: Vec<AbortHandle>,
    timeout: Duration,
) -> DrainResult {
    for abort_handle in abort_handles {
        abort_handle.abort();
    }

    match tokio::time::timeout(timeout, drain_aborted_joined_tasks(joined)).await {
        Ok(()) => DrainResult::Drained,
        Err(_) => DrainResult::TimedOut,
    }
}

async fn abort_and_drain_joined_tasks_or_log(
    joined: &mut FuturesUnordered<impl Future<Output = JoinedRuntimeTask>>,
    abort_handles: Vec<AbortHandle>,
    timeout: Duration,
) {
    if matches!(
        abort_and_drain_joined_tasks_with_timeout(joined, abort_handles, timeout).await,
        DrainResult::TimedOut
    ) {
        log_abort_drain_timeout(timeout);
    }
}

async fn drain_aborted_joined_tasks(
    joined: &mut FuturesUnordered<impl Future<Output = JoinedRuntimeTask>>,
) {
    while let Some((task, result)) = joined.next().await {
        match result {
            Ok(_) => {}
            Err(source) if source.is_cancelled() => {
                // Cancellation is expected after this helper aborts the
                // remaining tasks; the caller returns the timeout or earlier task
                // failure that triggered the abort.
            }
            Err(source) => {
                log_drained_task_error(RuntimeError::TaskJoin { task, source });
            }
        }
    }
}

fn log_drained_task_error(error: RuntimeError) {
    error!(
        %error,
        "supervised runtime task failed while draining after an earlier failure"
    );
}

fn log_abort_drain_timeout(timeout: Duration) {
    warn!(
        ?timeout,
        "timed out draining aborted supervisor tasks; later task failures may be unobserved"
    );
}

fn classify_task_result(task: &'static str, result: RuntimeTaskJoinResult) -> Option<RuntimeError> {
    match result {
        Ok(RuntimeTaskExit::Shutdown) => {
            debug!(task, "supervised runtime task joined after shutdown");
            None
        }
        Ok(RuntimeTaskExit::Completed) => {
            debug!(task, "supervised runtime task exited before shutdown");
            Some(RuntimeError::TaskExitedUnexpectedly { task })
        }
        Err(source) => {
            debug!(
                task,
                is_cancelled = source.is_cancelled(),
                is_panic = source.is_panic(),
                "supervised runtime task join failed"
            );
            Some(RuntimeError::TaskJoin { task, source })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use sqlx::postgres::PgPoolOptions;
    use tokio::time::timeout;

    use super::*;
    use crate::Error;

    const UNUSED_LAZY_POOL_URL: &str = "postgres://postgres:postgres@127.0.0.1:65535/runledger";

    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct CompleteAfterPollSignal {
        entered_tx: Option<std::sync::mpsc::Sender<()>>,
        release_rx: std::sync::mpsc::Receiver<()>,
        exit: RuntimeTaskExit,
    }

    impl Future for CompleteAfterPollSignal {
        type Output = RuntimeTaskExit;

        fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            let task = self.as_mut().get_mut();
            if let Some(entered_tx) = task.entered_tx.take() {
                entered_tx
                    .send(())
                    .expect("completion poll entry signal should be received");
            }
            task.release_rx
                .recv()
                .expect("completion poll should be released");
            Poll::Ready(task.exit)
        }
    }

    fn lazy_pool() -> runledger_postgres::DbPool {
        PgPoolOptions::new()
            // The disable-only tests never acquire this pool; this URL is only
            // a valid PgPool value for supervisor wiring assertions.
            .connect_lazy(UNUSED_LAZY_POOL_URL)
            .expect("construct lazy pool")
    }

    fn test_config() -> JobsConfig {
        JobsConfig {
            worker_id: "supervisor-test-worker".to_string(),
            poll_interval: Duration::from_millis(25),
            claim_batch_size: 4,
            lease_ttl_seconds: 10,
            max_global_concurrency: 4,
            reaper_interval: Duration::from_millis(50),
            schedule_poll_interval: Duration::from_millis(50),
            reaper_retry_delay_ms: 1_000,
        }
    }

    fn empty_builder(pool: &runledger_postgres::DbPool) -> SupervisorBuilder<'_> {
        Supervisor::builder(pool, test_config()).expect("supervisor builder has runtime")
    }

    fn missing_registry_flags(builder: SupervisorBuilder<'_>) -> (bool, bool) {
        match builder.build() {
            Err(RuntimeError::MissingRegistry {
                worker_enabled,
                reaper_enabled,
            }) => (worker_enabled, reaper_enabled),
            Ok(_) => panic!("missing registry should be a build error"),
            Err(other) => panic!("expected missing registry error, got {other:?}"),
        }
    }

    fn test_task<F>(name: &'static str, future: F) -> RuntimeTask
    where
        F: Future<Output = ()> + Send + 'static,
    {
        test_task_with_exit(name, RuntimeTaskExit::Completed, future)
    }

    fn test_shutdown_task<F>(name: &'static str, future: F) -> RuntimeTask
    where
        F: Future<Output = ()> + Send + 'static,
    {
        test_task_with_exit(name, RuntimeTaskExit::Shutdown, future)
    }

    fn test_task_with_exit<F>(name: &'static str, exit: RuntimeTaskExit, future: F) -> RuntimeTask
    where
        F: Future<Output = ()> + Send + 'static,
    {
        RuntimeTask::spawn(name, async move {
            future.await;
            exit
        })
    }

    fn supervisor_from_shutdown_channel(
        shutdown_tx: watch::Sender<bool>,
        shutdown_requested: Arc<AtomicBool>,
        tasks: Vec<RuntimeTask>,
    ) -> Supervisor {
        Supervisor {
            shutdown_tx,
            shutdown_requested,
            tasks,
        }
    }

    fn task_names(supervisor: &Supervisor) -> Vec<&'static str> {
        supervisor.tasks.iter().map(|task| task.name).collect()
    }

    async fn abort_supervisor_tasks(mut supervisor: Supervisor) {
        let tasks = std::mem::take(&mut supervisor.tasks);
        for task in tasks {
            task.handle.abort();
            let _ = task.handle.await;
        }
    }

    #[tokio::test]
    async fn builder_defaults_enable_all_loops() {
        let pool = lazy_pool();
        let builder = empty_builder(&pool);

        assert!(builder.worker_enabled);
        assert!(builder.scheduler_enabled);
        assert!(builder.reaper_enabled);
        assert!(builder.registry.is_none());
        assert_eq!(builder.registry_source, None);
        assert!(!builder.mixed_registry_sources);
    }

    #[tokio::test]
    async fn builder_accepts_registry_for_worker_and_reaper_loops() {
        let pool = lazy_pool();
        let builder = empty_builder(&pool).with_registry(JobRegistry::new());

        assert!(builder.registry.is_some());
        assert_eq!(builder.registry_source, Some(RegistrySource::Registry));
        assert!(!builder.mixed_registry_sources);
    }

    #[tokio::test]
    async fn builder_rejects_mixed_registry_sources() {
        let pool = lazy_pool();
        let registry_then_catalog = empty_builder(&pool)
            .with_registry(JobRegistry::new())
            .with_catalog(JobCatalog::new())
            .disable_worker()
            .disable_reaper()
            .build();
        let Err(registry_then_catalog) = registry_then_catalog else {
            panic!("mixed registry sources should be rejected");
        };
        assert!(matches!(
            registry_then_catalog,
            RuntimeError::MixedRegistrySources
        ));

        let catalog_then_registry = empty_builder(&pool)
            .with_catalog(JobCatalog::new())
            .with_registry(JobRegistry::new())
            .disable_worker()
            .disable_reaper()
            .build();
        let Err(catalog_then_registry) = catalog_then_registry else {
            panic!("mixed registry sources should be rejected");
        };
        assert!(matches!(
            catalog_then_registry,
            RuntimeError::MixedRegistrySources
        ));
    }

    #[tokio::test]
    async fn builder_requires_registry_when_worker_or_reaper_is_enabled() {
        let pool = lazy_pool();

        assert_eq!(missing_registry_flags(empty_builder(&pool)), (true, true));
        assert_eq!(
            missing_registry_flags(empty_builder(&pool).disable_scheduler().disable_reaper()),
            (true, false)
        );
        assert_eq!(
            missing_registry_flags(empty_builder(&pool).disable_worker().disable_scheduler()),
            (false, true)
        );
    }

    #[test]
    fn builder_requires_tokio_runtime_before_cloning_pool() {
        let runtime = tokio::runtime::Runtime::new().expect("construct Tokio runtime");
        let pool = runtime.block_on(async { lazy_pool() });
        let error = match Supervisor::builder(&pool, test_config()) {
            Err(error) => error,
            Ok(builder) => {
                drop(builder);
                runtime.block_on(async {
                    pool.close().await;
                });
                std::mem::forget(pool);
                panic!("missing Tokio runtime should be a builder error");
            }
        };

        // The builder was intentionally called outside a runtime to exercise
        // the pre-clone runtime check. Close and drop the pool inside the
        // temporary runtime so sqlx's own drop precondition does not contaminate
        // this assertion.
        runtime.block_on(async {
            pool.close().await;
        });
        std::mem::forget(pool);
        match error {
            RuntimeError::MissingTokioRuntime { .. } => {}
            other => panic!("expected missing Tokio runtime error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn builder_can_disable_each_loop() {
        let pool = lazy_pool();
        let builder = empty_builder(&pool)
            .disable_worker()
            .disable_scheduler()
            .disable_reaper();

        assert!(!builder.worker_enabled);
        assert!(!builder.scheduler_enabled);
        assert!(!builder.reaper_enabled);
    }

    #[tokio::test]
    async fn builder_spawns_only_enabled_tasks() {
        let pool = lazy_pool();

        let all_disabled = empty_builder(&pool)
            .disable_worker()
            .disable_scheduler()
            .disable_reaper()
            .build()
            .expect("all-disabled supervisor should build");
        assert_eq!(task_names(&all_disabled), Vec::<&'static str>::new());
        abort_supervisor_tasks(all_disabled).await;

        let scheduler_only = empty_builder(&pool)
            .disable_worker()
            .disable_reaper()
            .build()
            .expect("scheduler-only supervisor should not require registry");
        assert_eq!(task_names(&scheduler_only), vec![SCHEDULER_TASK]);
        abort_supervisor_tasks(scheduler_only).await;

        let worker_only = empty_builder(&pool)
            .with_registry(JobRegistry::new())
            .disable_scheduler()
            .disable_reaper()
            .build()
            .expect("worker-only supervisor should build with registry");
        assert_eq!(task_names(&worker_only), vec![WORKER_TASK]);
        abort_supervisor_tasks(worker_only).await;

        let reaper_only = empty_builder(&pool)
            .with_registry(JobRegistry::new())
            .disable_worker()
            .disable_scheduler()
            .build()
            .expect("reaper-only supervisor should build with registry");
        assert_eq!(task_names(&reaper_only), vec![REAPER_TASK]);
        abort_supervisor_tasks(reaper_only).await;

        let all_enabled = empty_builder(&pool)
            .with_registry(JobRegistry::new())
            .build()
            .expect("all-enabled supervisor should build with registry");
        assert_eq!(
            task_names(&all_enabled),
            vec![WORKER_TASK, SCHEDULER_TASK, REAPER_TASK]
        );
        abort_supervisor_tasks(all_enabled).await;
    }

    #[tokio::test]
    async fn all_disabled_supervisor_join_and_shutdown_succeed() {
        Supervisor::builder(&lazy_pool(), test_config())
            .expect("supervisor builder has runtime")
            .disable_worker()
            .disable_scheduler()
            .disable_reaper()
            .build()
            .expect("all-disabled supervisor should build")
            .join()
            .await
            .expect("all-disabled supervisor should join");

        Supervisor::builder(&lazy_pool(), test_config())
            .expect("supervisor builder has runtime")
            .disable_worker()
            .disable_scheduler()
            .disable_reaper()
            .build()
            .expect("all-disabled supervisor should build")
            .shutdown()
            .await
            .expect("all-disabled supervisor should shut down");
    }

    #[tokio::test]
    async fn shutdown_handle_can_request_shutdown_before_join() {
        let supervisor = Supervisor::builder(&lazy_pool(), test_config())
            .expect("supervisor builder has runtime")
            .disable_worker()
            .disable_scheduler()
            .disable_reaper()
            .build()
            .expect("all-disabled supervisor should build");
        let shutdown = supervisor.shutdown_handle();
        let cloned_shutdown = shutdown.clone();

        cloned_shutdown.request_shutdown();

        assert!(shutdown.is_shutdown_requested());
        assert!(supervisor.is_shutdown_requested());
        supervisor
            .join()
            .await
            .expect("supervisor should join after shutdown handle request");
    }

    #[tokio::test]
    async fn shutdown_after_shutdown_handle_request_allows_clean_task_exit() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let supervisor = supervisor_from_shutdown_channel(
            shutdown_tx,
            Arc::clone(&shutdown_requested),
            vec![test_shutdown_task("cooperative-loop", async move {
                while !*shutdown_rx.borrow() {
                    if shutdown_rx.changed().await.is_err() {
                        break;
                    }
                }
            })],
        );
        let shutdown = supervisor.shutdown_handle();

        shutdown.request_shutdown();

        supervisor
            .shutdown()
            .await
            .expect("clean exit after requested shutdown should succeed");
    }

    #[tokio::test]
    async fn run_until_shutdown_requests_shutdown_when_signal_resolves() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        let supervisor = supervisor_from_shutdown_channel(
            shutdown_tx,
            Arc::clone(&shutdown_requested),
            vec![test_shutdown_task(
                "run-until-cooperative-loop",
                async move {
                    while !*shutdown_rx.borrow() {
                        if shutdown_rx.changed().await.is_err() {
                            break;
                        }
                    }
                },
            )],
        );

        signal_tx.send(()).expect("signal receiver should be alive");

        supervisor
            .run_until_shutdown(
                async move {
                    signal_rx.await.expect("shutdown signal should be sent");
                },
                Duration::from_secs(1),
            )
            .await
            .expect("resolved shutdown signal should shut down cleanly");
        assert!(shutdown_requested.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn run_until_shutdown_with_no_tasks_waits_for_signal() {
        let supervisor = Supervisor::from_tasks_for_tests(Vec::new());
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        let mut run = tokio::spawn(supervisor.run_until_shutdown(
            async move {
                signal_rx.await.expect("shutdown signal should be sent");
            },
            Duration::from_secs(1),
        ));

        assert!(
            timeout(Duration::from_millis(50), &mut run).await.is_err(),
            "all-disabled supervisor should wait for the shutdown signal"
        );

        signal_tx.send(()).expect("signal receiver should be alive");
        run.await
            .expect("run-until-shutdown task should join")
            .expect("all-disabled supervisor should complete after signal");
    }

    #[tokio::test]
    async fn run_until_shutdown_reports_task_exit_before_signal() {
        let supervisor =
            Supervisor::from_tasks_for_tests(vec![test_task("run-until-early-loop", async {})]);

        let error = timeout(
            Duration::from_secs(1),
            supervisor.run_until_shutdown(std::future::pending::<()>(), Duration::from_secs(1)),
        )
        .await
        .expect("task exit should be reported before external signal")
        .expect_err("early task exit should fail run-until shutdown");

        match error {
            Error::Runtime(RuntimeError::TaskExitedUnexpectedly { task }) => {
                assert_eq!(task, "run-until-early-loop");
            }
            other => panic!("expected unexpected task exit, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn run_until_shutdown_times_out_and_aborts_after_signal() {
        let dropped = Arc::new(AtomicBool::new(false));
        let drop_flag = DropFlag(Arc::clone(&dropped));
        let supervisor =
            Supervisor::from_tasks_for_tests(vec![test_task("run-until-stubborn-loop", async {
                let _drop_flag = drop_flag;
                std::future::pending::<()>().await;
            })]);

        let error = supervisor
            .run_until_shutdown(async {}, Duration::from_millis(50))
            .await
            .expect_err("stubborn task should time out after shutdown signal");

        match error {
            Error::Runtime(RuntimeError::ShutdownTimeout { timeout }) => {
                assert_eq!(timeout, Duration::from_millis(50));
            }
            other => panic!("expected shutdown timeout error, got {other:?}"),
        }
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn run_until_shutdown_reports_task_exit_after_signal_before_deadline() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let supervisor = supervisor_from_shutdown_channel(
            shutdown_tx,
            Arc::clone(&shutdown_requested),
            vec![test_task("run-until-bad-shutdown-loop", async move {
                while !*shutdown_rx.borrow() {
                    if shutdown_rx.changed().await.is_err() {
                        break;
                    }
                }
            })],
        );

        let error = supervisor
            .run_until_shutdown(async {}, Duration::from_secs(1))
            .await
            .expect_err("task completion after signal should still be reported");

        match error {
            Error::Runtime(RuntimeError::TaskExitedUnexpectedly { task }) => {
                assert_eq!(task, "run-until-bad-shutdown-loop");
            }
            other => panic!("expected unexpected task exit, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dropping_supervisor_requests_shutdown_signal() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let mut observed_shutdown = shutdown_rx.clone();
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let supervisor = supervisor_from_shutdown_channel(
            shutdown_tx,
            Arc::clone(&shutdown_requested),
            vec![test_shutdown_task("drop-shutdown-loop", async move {
                while !*shutdown_rx.borrow() {
                    if shutdown_rx.changed().await.is_err() {
                        break;
                    }
                }
            })],
        );

        drop(supervisor);

        timeout(Duration::from_secs(1), observed_shutdown.changed())
            .await
            .expect("drop should promptly notify shutdown")
            .expect("shutdown sender should notify before closing");
        assert!(*observed_shutdown.borrow());
        assert!(shutdown_requested.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn join_reports_task_that_exited_before_late_shutdown_request() {
        let (shutdown_tx, _) = watch::channel(false);
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let supervisor = supervisor_from_shutdown_channel(
            shutdown_tx,
            Arc::clone(&shutdown_requested),
            vec![test_task("early-before-late-signal", async {})],
        );

        while !supervisor.tasks[0].handle.is_finished() {
            tokio::task::yield_now().await;
        }

        let shutdown = supervisor.shutdown_handle();
        shutdown.request_shutdown();

        let error = supervisor
            .join()
            .await
            .expect_err("task exit before shutdown request should still be reported");

        match error {
            Error::Runtime(RuntimeError::TaskExitedUnexpectedly { task }) => {
                assert_eq!(task, "early-before-late-signal");
            }
            other => panic!("expected unexpected task exit, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn join_reports_task_exit_when_shutdown_races_completion_poll() {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (shutdown_tx, _) = watch::channel(false);
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let supervisor = supervisor_from_shutdown_channel(
            shutdown_tx,
            Arc::clone(&shutdown_requested),
            vec![RuntimeTask::spawn(
                "race-completion",
                CompleteAfterPollSignal {
                    entered_tx: Some(entered_tx),
                    release_rx,
                    exit: RuntimeTaskExit::Completed,
                },
            )],
        );
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("task should enter its completion poll");
        let shutdown = supervisor.shutdown_handle();

        shutdown.request_shutdown();
        release_tx
            .send(())
            .expect("completion poll release should be received");

        let error = supervisor
            .join()
            .await
            .expect_err("task exit that began before shutdown should be reported");

        match error {
            Error::Runtime(RuntimeError::TaskExitedUnexpectedly { task }) => {
                assert_eq!(task, "race-completion");
            }
            other => panic!("expected unexpected task exit, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn join_allows_shutdown_exit_when_shutdown_races_completion_poll() {
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let (shutdown_tx, _) = watch::channel(false);
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let supervisor = supervisor_from_shutdown_channel(
            shutdown_tx,
            Arc::clone(&shutdown_requested),
            vec![RuntimeTask::spawn(
                "shutdown-race-completion",
                CompleteAfterPollSignal {
                    entered_tx: Some(entered_tx),
                    release_rx,
                    exit: RuntimeTaskExit::Shutdown,
                },
            )],
        );
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("task should enter its completion poll");
        let shutdown = supervisor.shutdown_handle();

        shutdown.request_shutdown();
        release_tx
            .send(())
            .expect("completion poll release should be received");

        supervisor
            .join()
            .await
            .expect("task that reports shutdown should join cleanly");
    }

    #[tokio::test]
    async fn panic_after_shutdown_request_is_reported() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let supervisor = supervisor_from_shutdown_channel(
            shutdown_tx,
            Arc::clone(&shutdown_requested),
            vec![test_shutdown_task("panic-after-shutdown", async move {
                while !*shutdown_rx.borrow() {
                    if shutdown_rx.changed().await.is_err() {
                        return;
                    }
                }
                panic!("forced post-shutdown panic");
            })],
        );
        let shutdown = supervisor.shutdown_handle();

        shutdown.request_shutdown();

        let error = supervisor
            .shutdown()
            .await
            .expect_err("panic after requested shutdown should fail");

        match error {
            Error::Runtime(RuntimeError::TaskJoin { task, source }) => {
                assert_eq!(task, "panic-after-shutdown");
                assert!(source.is_panic());
            }
            other => panic!("expected task join error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn early_normal_task_exit_is_unexpected() {
        let supervisor = Supervisor::from_tasks_for_tests(vec![test_task("test-loop", async {})]);

        let error = supervisor
            .join()
            .await
            .expect_err("early normal exit should fail");

        match error {
            Error::Runtime(RuntimeError::TaskExitedUnexpectedly { task }) => {
                assert_eq!(task, "test-loop");
            }
            other => panic!("expected unexpected task exit, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn shutdown_reports_task_that_exited_before_shutdown_request() {
        let supervisor = Supervisor::from_tasks_for_tests(vec![test_task("early-loop", async {})]);

        while !supervisor.tasks[0].handle.is_finished() {
            tokio::task::yield_now().await;
        }

        let error = supervisor
            .shutdown()
            .await
            .expect_err("pre-shutdown task exit should fail");

        match error {
            Error::Runtime(RuntimeError::TaskExitedUnexpectedly { task }) => {
                assert_eq!(task, "early-loop");
            }
            other => panic!("expected unexpected task exit, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn pre_shutdown_sweep_consumes_all_already_finished_tasks() {
        let mut tasks = vec![
            test_task("finished-a", async {}),
            test_task("pending", async {
                std::future::pending::<()>().await;
            }),
            test_task("finished-b", async {}),
        ];

        while tasks
            .iter()
            .filter(|task| task.name != "pending")
            .any(|task| !task.handle.is_finished())
        {
            tokio::task::yield_now().await;
        }

        let error = join_pre_shutdown_finished_tasks(&mut tasks)
            .await
            .expect("finished tasks should produce a pre-shutdown error");

        match error {
            RuntimeError::TaskExitedUnexpectedly { task } => {
                assert!(
                    matches!(task, "finished-a" | "finished-b"),
                    "unexpected first finished task: {task}"
                );
            }
            other => panic!("expected unexpected task exit, got {other:?}"),
        }
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].name, "pending");

        let pending = tasks.pop().expect("pending task remains");
        pending.handle.abort();
        // The task was deliberately aborted; this test only needs the join
        // handle drained before returning.
        let _ = pending.handle.await;
    }

    #[tokio::test]
    async fn pre_shutdown_sweep_allows_explicit_shutdown_exit() {
        let mut tasks = vec![test_shutdown_task("finished-after-signal", async {})];
        while !tasks[0].handle.is_finished() {
            tokio::task::yield_now().await;
        }

        let error = join_pre_shutdown_finished_tasks(&mut tasks).await;

        assert!(error.is_none());
        assert!(tasks.is_empty());
    }

    #[tokio::test]
    async fn shutdown_with_timeout_aborts_and_drains_stubborn_task() {
        let dropped = Arc::new(AtomicBool::new(false));
        let drop_flag = DropFlag(Arc::clone(&dropped));
        let supervisor =
            Supervisor::from_tasks_for_tests(vec![test_task("stubborn-loop", async move {
                let _drop_flag = drop_flag;
                std::future::pending::<()>().await;
            })]);

        let error = supervisor
            .shutdown_with_timeout(Duration::from_millis(50))
            .await
            .expect_err("stubborn task should time out shutdown");

        match error {
            Error::Runtime(RuntimeError::ShutdownTimeout { timeout }) => {
                assert_eq!(timeout, Duration::from_millis(50));
            }
            other => panic!("expected shutdown timeout error, got {other:?}"),
        }
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn shutdown_with_timeout_rejects_unrepresentable_deadline() {
        let error = Supervisor::from_tasks_for_tests(Vec::new())
            .shutdown_with_timeout(Duration::MAX)
            .await
            .expect_err("unrepresentable timeout should fail instead of panicking");

        match error {
            Error::Runtime(RuntimeError::ShutdownTimeoutTooLarge { timeout }) => {
                assert_eq!(timeout, Duration::MAX);
            }
            other => panic!("expected oversized timeout error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn shutdown_with_zero_timeout_aborts_immediately() {
        let supervisor =
            Supervisor::from_tasks_for_tests(vec![test_task("zero-timeout-pending-loop", async {
                std::future::pending::<()>().await;
            })]);

        let error = supervisor
            .shutdown_with_timeout(Duration::ZERO)
            .await
            .expect_err("zero timeout should report an immediate shutdown timeout");

        match error {
            Error::Runtime(RuntimeError::ShutdownTimeout { timeout }) => {
                assert_eq!(timeout, Duration::ZERO);
            }
            other => panic!("expected shutdown timeout error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn shutdown_with_timeout_succeeds_when_task_exits_cooperatively() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let supervisor = supervisor_from_shutdown_channel(
            shutdown_tx,
            Arc::clone(&shutdown_requested),
            vec![test_shutdown_task("cooperative-timeout-loop", async move {
                while !*shutdown_rx.borrow() {
                    if shutdown_rx.changed().await.is_err() {
                        break;
                    }
                }
            })],
        );

        supervisor
            .shutdown_with_timeout(Duration::from_secs(1))
            .await
            .expect("cooperative task should shut down before timeout");
    }

    #[tokio::test]
    async fn shutdown_with_timeout_pre_shutdown_error_allows_remaining_task_to_exit() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let drop_flag = DropFlag(Arc::clone(&dropped));
        let tasks = vec![
            test_task("finished-before-shutdown", async {}),
            test_shutdown_task("cooperative-after-error", async move {
                let _drop_flag = drop_flag;
                while !*shutdown_rx.borrow() {
                    if shutdown_rx.changed().await.is_err() {
                        break;
                    }
                }
            }),
        ];

        while !tasks[0].handle.is_finished() {
            tokio::task::yield_now().await;
        }

        let supervisor =
            supervisor_from_shutdown_channel(shutdown_tx, Arc::clone(&shutdown_requested), tasks);
        let error = supervisor
            .shutdown_with_timeout(Duration::from_secs(1))
            .await
            .expect_err("pre-shutdown task exit should fail");

        match error {
            Error::Runtime(RuntimeError::TaskExitedUnexpectedly { task }) => {
                assert_eq!(task, "finished-before-shutdown");
            }
            other => panic!("expected pre-shutdown task exit, got {other:?}"),
        }
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn shutdown_with_timeout_reports_timeout_after_pre_shutdown_error() {
        let tasks = vec![
            test_task("finished-before-shutdown", async {}),
            test_task("pending-after-error", async {
                std::future::pending::<()>().await;
            }),
        ];

        while !tasks[0].handle.is_finished() {
            tokio::task::yield_now().await;
        }

        let error = Supervisor::from_tasks_for_tests(tasks)
            .shutdown_with_timeout(Duration::from_millis(1))
            .await
            .expect_err("pre-shutdown task exit with stuck drain should time out");

        match error {
            Error::Runtime(RuntimeError::ShutdownTimeoutAfterTaskError { timeout, source }) => {
                assert_eq!(timeout, Duration::from_millis(1));
                match *source {
                    RuntimeError::TaskExitedUnexpectedly { task } => {
                        assert_eq!(task, "finished-before-shutdown");
                    }
                    other => panic!("expected pre-shutdown task exit source, got {other:?}"),
                }
            }
            other => panic!("expected shutdown timeout after task error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn shutdown_with_timeout_reports_task_error_when_remaining_task_misses_deadline() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let shutdown_requested = Arc::new(AtomicBool::new(false));
        let supervisor = supervisor_from_shutdown_channel(
            shutdown_tx,
            Arc::clone(&shutdown_requested),
            vec![
                test_shutdown_task("panic-after-timeout-shutdown", async move {
                    while !*shutdown_rx.borrow() {
                        if shutdown_rx.changed().await.is_err() {
                            return;
                        }
                    }
                    panic!("forced live shutdown panic");
                }),
                test_shutdown_task("pending-after-timeout-panic", async {
                    std::future::pending::<()>().await;
                }),
            ],
        );

        let error = supervisor
            .shutdown_with_timeout(Duration::from_millis(50))
            .await
            .expect_err("task failure with stuck drain should preserve task error source");

        match error {
            Error::Runtime(RuntimeError::ShutdownTimeoutAfterTaskError { timeout, source }) => {
                assert_eq!(timeout, Duration::from_millis(50));
                match *source {
                    RuntimeError::TaskJoin { task, source } => {
                        assert_eq!(task, "panic-after-timeout-shutdown");
                        assert!(source.is_panic());
                    }
                    other => panic!("expected task join source, got {other:?}"),
                }
            }
            other => panic!("expected timeout after task join error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn panicked_task_maps_to_task_join_error() {
        let supervisor = Supervisor::from_tasks_for_tests(vec![test_task("panic-loop", async {
            panic!("forced supervisor test panic");
        })]);

        let error = supervisor
            .join()
            .await
            .expect_err("panicked task should fail");

        match error {
            Error::Runtime(RuntimeError::TaskJoin { task, source }) => {
                assert_eq!(task, "panic-loop");
                assert!(source.is_panic());
            }
            other => panic!("expected task join error, got {other:?}"),
        }
    }
}
