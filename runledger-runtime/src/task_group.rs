use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use tokio::runtime::Handle;
use tokio::task::{AbortHandle, JoinError, JoinHandle};
use tokio::time::Instant;
use tracing::{Instrument, debug, error, info_span, warn};

use crate::config::JobsConfigValidationError;
use crate::shutdown::ShutdownSignal;
use crate::{Error, Result, RuntimeError, RuntimeLoopExit};

const MAX_ABORT_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);

#[must_use]
pub(crate) struct TaskGroup {
    tasks: Vec<RuntimeTask>,
}

struct RuntimeTask {
    name: &'static str,
    handle: JoinHandle<RuntimeTaskExit>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeTaskExit {
    Completed,
    InvalidConfig(JobsConfigValidationError),
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

impl TaskGroup {
    pub(crate) fn new() -> Self {
        Self { tasks: Vec::new() }
    }

    pub(crate) fn len(&self) -> usize {
        self.tasks.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    pub(crate) fn spawn_on<F>(&mut self, runtime: &Handle, name: &'static str, future: F)
    where
        F: Future<Output = RuntimeLoopExit> + Send + 'static,
    {
        self.tasks
            .push(RuntimeTask::spawn_on(runtime, name, future));
    }

    pub(crate) async fn join(&mut self, shutdown: &ShutdownSignal) -> Result<()> {
        let tasks = std::mem::take(&mut self.tasks);
        join_tasks(tasks, shutdown).await
    }

    pub(crate) async fn shutdown(&mut self, shutdown: &ShutdownSignal) -> Result<()> {
        if let Some(error) = join_pre_shutdown_finished_tasks(&mut self.tasks).await {
            shutdown.request();
            let tasks = std::mem::take(&mut self.tasks);
            drain_tasks(tasks).await;
            return Err(Error::Runtime(error));
        }

        shutdown.request();
        let tasks = std::mem::take(&mut self.tasks);
        join_tasks(tasks, shutdown).await
    }

    pub(crate) async fn shutdown_with_timeout(
        &mut self,
        timeout: Duration,
        shutdown: &ShutdownSignal,
    ) -> Result<()> {
        let deadline = shutdown_deadline(timeout)?;

        if let Some(error) = join_pre_shutdown_finished_tasks(&mut self.tasks).await {
            shutdown.request();
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

        shutdown.request();
        let tasks = std::mem::take(&mut self.tasks);
        join_tasks_with_timeout(tasks, timeout, deadline, shutdown).await
    }

    pub(crate) async fn run_until_shutdown<F>(
        &mut self,
        external_shutdown: F,
        timeout: Duration,
        shutdown: &ShutdownSignal,
    ) -> Result<()>
    where
        F: Future<Output = ()>,
    {
        // Validate the shutdown budget before waiting on a signal, then
        // recompute the deadline when shutdown actually begins.
        let _ = shutdown_deadline(timeout)?;
        let tasks = std::mem::take(&mut self.tasks);
        if tasks.is_empty() {
            external_shutdown.await;
            shutdown.request();
            return Ok(());
        }

        let mut abort_handles = Some(task_abort_handles(&tasks));
        let mut joined = join_runtime_tasks(tasks);
        let mut external_shutdown = std::pin::pin!(external_shutdown);

        loop {
            tokio::select! {
                _ = external_shutdown.as_mut() => {
                    shutdown.request();
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
                    return join_joined_tasks_with_timeout(
                        &mut joined,
                        abort_handles,
                        timeout,
                        deadline,
                        shutdown,
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

                    shutdown.request();
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

    #[cfg(test)]
    fn from_tasks_for_tests(tasks: Vec<RuntimeTask>) -> Self {
        Self { tasks }
    }

    #[cfg(test)]
    pub(crate) fn names_for_tests(&self) -> Vec<&'static str> {
        self.tasks.iter().map(|task| task.name).collect()
    }

    #[cfg(test)]
    pub(crate) async fn abort_all_for_tests(&mut self) {
        let tasks = std::mem::take(&mut self.tasks);
        for task in tasks {
            task.handle.abort();
            let _ = task.handle.await;
        }
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
            RuntimeLoopExit::InvalidConfig(source) => Self::InvalidConfig(source),
            RuntimeLoopExit::Completed => Self::Completed,
        }
    }
}

async fn join_tasks(tasks: Vec<RuntimeTask>, shutdown: &ShutdownSignal) -> Result<()> {
    let mut joined = join_runtime_tasks(tasks);

    while let Some((task, result)) = joined.next().await {
        if let Some(error) = classify_task_result(task, result) {
            shutdown.request();
            drain_joined_tasks(&mut joined).await;
            return Err(Error::Runtime(error));
        }
    }

    Ok(())
}

async fn join_tasks_with_timeout(
    tasks: Vec<RuntimeTask>,
    timeout: Duration,
    deadline: Instant,
    shutdown: &ShutdownSignal,
) -> Result<()> {
    let abort_handles = task_abort_handles(&tasks);
    let mut joined = join_runtime_tasks(tasks);

    join_joined_tasks_with_timeout(&mut joined, abort_handles, timeout, deadline, shutdown).await
}

async fn join_joined_tasks_with_timeout(
    joined: &mut FuturesUnordered<impl Future<Output = JoinedRuntimeTask>>,
    abort_handles: Vec<AbortHandle>,
    timeout: Duration,
    deadline: Instant,
    shutdown: &ShutdownSignal,
) -> Result<()> {
    loop {
        match tokio::time::timeout_at(deadline, joined.next()).await {
            Ok(Some((task, result))) => {
                if let Some(error) = classify_task_result(task, result) {
                    shutdown.request();
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
        Ok(RuntimeTaskExit::InvalidConfig(source)) => {
            debug!(
                task,
                "supervised runtime task rejected invalid config after build validation"
            );
            Some(RuntimeError::InvalidJobsConfig { source })
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
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use tokio::time::timeout;

    use super::*;
    use crate::Error;

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

    fn test_group(tasks: Vec<RuntimeTask>) -> TaskGroup {
        TaskGroup::from_tasks_for_tests(tasks)
    }

    #[tokio::test]
    async fn shutdown_after_repeated_handle_requests_allows_clean_task_exit() {
        let (shutdown, mut shutdown_rx) = ShutdownSignal::channel();
        let mut group = test_group(vec![test_shutdown_task("cooperative-loop", async move {
            while !*shutdown_rx.borrow() {
                if shutdown_rx.changed().await.is_err() {
                    break;
                }
            }
        })]);
        let shutdown_handle = shutdown.handle();

        shutdown_handle.request();
        shutdown_handle.request();

        group
            .shutdown(&shutdown)
            .await
            .expect("clean exit after requested shutdown should succeed");
    }

    #[tokio::test]
    async fn run_until_shutdown_requests_shutdown_when_signal_resolves() {
        let (shutdown, mut shutdown_rx) = ShutdownSignal::channel();
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        let mut group = test_group(vec![test_shutdown_task(
            "run-until-cooperative-loop",
            async move {
                while !*shutdown_rx.borrow() {
                    if shutdown_rx.changed().await.is_err() {
                        break;
                    }
                }
            },
        )]);

        signal_tx.send(()).expect("signal receiver should be alive");

        group
            .run_until_shutdown(
                async move {
                    signal_rx.await.expect("shutdown signal should be sent");
                },
                Duration::from_secs(1),
                &shutdown,
            )
            .await
            .expect("resolved shutdown signal should shut down cleanly");
        assert!(shutdown.is_requested());
    }

    #[tokio::test]
    async fn run_until_shutdown_with_no_tasks_waits_for_signal() {
        let (shutdown, _) = ShutdownSignal::channel();
        let mut group = TaskGroup::new();
        let (signal_tx, signal_rx) = tokio::sync::oneshot::channel();
        let mut run = tokio::spawn(async move {
            group
                .run_until_shutdown(
                    async move {
                        signal_rx.await.expect("shutdown signal should be sent");
                    },
                    Duration::from_secs(1),
                    &shutdown,
                )
                .await
        });

        assert!(
            timeout(Duration::from_millis(50), &mut run).await.is_err(),
            "empty task group should wait for the shutdown signal"
        );

        signal_tx.send(()).expect("signal receiver should be alive");
        run.await
            .expect("run-until-shutdown task should join")
            .expect("empty task group should complete after signal");
    }

    #[tokio::test]
    async fn run_until_shutdown_reports_task_exit_before_signal() {
        let (shutdown, _) = ShutdownSignal::channel();
        let mut group = test_group(vec![test_task("run-until-early-loop", async {})]);

        let error = timeout(
            Duration::from_secs(1),
            group.run_until_shutdown(
                std::future::pending::<()>(),
                Duration::from_secs(1),
                &shutdown,
            ),
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
        let (shutdown, _) = ShutdownSignal::channel();
        let dropped = Arc::new(AtomicBool::new(false));
        let drop_flag = DropFlag(Arc::clone(&dropped));
        let mut group = test_group(vec![test_task("run-until-stubborn-loop", async {
            let _drop_flag = drop_flag;
            std::future::pending::<()>().await;
        })]);

        let error = group
            .run_until_shutdown(async {}, Duration::from_millis(50), &shutdown)
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
        let (shutdown, mut shutdown_rx) = ShutdownSignal::channel();
        let mut group = test_group(vec![test_task("run-until-bad-shutdown-loop", async move {
            while !*shutdown_rx.borrow() {
                if shutdown_rx.changed().await.is_err() {
                    break;
                }
            }
        })]);

        let error = group
            .run_until_shutdown(async {}, Duration::from_secs(1), &shutdown)
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
    async fn join_reports_task_that_exited_before_late_shutdown_request() {
        let (shutdown, _) = ShutdownSignal::channel();
        let mut group = test_group(vec![test_task("early-before-late-signal", async {})]);

        while !group.tasks[0].handle.is_finished() {
            tokio::task::yield_now().await;
        }

        let shutdown_handle = shutdown.handle();
        shutdown_handle.request();

        let error = group
            .join(&shutdown)
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
        let (shutdown, _) = ShutdownSignal::channel();
        let mut group = test_group(vec![RuntimeTask::spawn(
            "race-completion",
            CompleteAfterPollSignal {
                entered_tx: Some(entered_tx),
                release_rx,
                exit: RuntimeTaskExit::Completed,
            },
        )]);
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("task should enter its completion poll");
        let shutdown_handle = shutdown.handle();

        shutdown_handle.request();
        release_tx
            .send(())
            .expect("completion poll release should be received");

        let error = group
            .join(&shutdown)
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
        let (shutdown, _) = ShutdownSignal::channel();
        let mut group = test_group(vec![RuntimeTask::spawn(
            "shutdown-race-completion",
            CompleteAfterPollSignal {
                entered_tx: Some(entered_tx),
                release_rx,
                exit: RuntimeTaskExit::Shutdown,
            },
        )]);
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("task should enter its completion poll");
        let shutdown_handle = shutdown.handle();

        shutdown_handle.request();
        release_tx
            .send(())
            .expect("completion poll release should be received");

        group
            .join(&shutdown)
            .await
            .expect("task that reports shutdown should join cleanly");
    }

    #[tokio::test]
    async fn panic_after_shutdown_request_is_reported() {
        let (shutdown, mut shutdown_rx) = ShutdownSignal::channel();
        let mut group = test_group(vec![test_shutdown_task(
            "panic-after-shutdown",
            async move {
                while !*shutdown_rx.borrow() {
                    if shutdown_rx.changed().await.is_err() {
                        return;
                    }
                }
                panic!("forced post-shutdown panic");
            },
        )]);
        let shutdown_handle = shutdown.handle();

        shutdown_handle.request();

        let error = group
            .shutdown(&shutdown)
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
        let (shutdown, _) = ShutdownSignal::channel();
        let mut group = test_group(vec![test_task("test-loop", async {})]);

        let error = group
            .join(&shutdown)
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
    async fn invalid_config_task_exit_preserves_validation_source() {
        let (shutdown, _) = ShutdownSignal::channel();
        let expected = JobsConfigValidationError::InvalidClaimBatchSize { actual: 0 };
        let mut group = test_group(vec![test_task_with_exit(
            "invalid-config-loop",
            RuntimeTaskExit::InvalidConfig(expected),
            async {},
        )]);

        let error = group
            .join(&shutdown)
            .await
            .expect_err("invalid config task exit should fail");

        match error {
            Error::Runtime(RuntimeError::InvalidJobsConfig { source }) => {
                assert_eq!(source, expected);
            }
            other => panic!("expected invalid jobs config error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn shutdown_reports_task_that_exited_before_shutdown_request() {
        let (shutdown, _) = ShutdownSignal::channel();
        let mut group = test_group(vec![test_task("early-loop", async {})]);

        while !group.tasks[0].handle.is_finished() {
            tokio::task::yield_now().await;
        }

        let error = group
            .shutdown(&shutdown)
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
        let (shutdown, _) = ShutdownSignal::channel();
        let dropped = Arc::new(AtomicBool::new(false));
        let drop_flag = DropFlag(Arc::clone(&dropped));
        let mut group = test_group(vec![test_task("stubborn-loop", async move {
            let _drop_flag = drop_flag;
            std::future::pending::<()>().await;
        })]);

        let error = group
            .shutdown_with_timeout(Duration::from_millis(50), &shutdown)
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
        let (shutdown, _) = ShutdownSignal::channel();
        let mut group = TaskGroup::new();
        let error = group
            .shutdown_with_timeout(Duration::MAX, &shutdown)
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
        let (shutdown, _) = ShutdownSignal::channel();
        let mut group = test_group(vec![test_task("zero-timeout-pending-loop", async {
            std::future::pending::<()>().await;
        })]);

        let error = group
            .shutdown_with_timeout(Duration::ZERO, &shutdown)
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
        let (shutdown, mut shutdown_rx) = ShutdownSignal::channel();
        let mut group = test_group(vec![test_shutdown_task(
            "cooperative-timeout-loop",
            async move {
                while !*shutdown_rx.borrow() {
                    if shutdown_rx.changed().await.is_err() {
                        break;
                    }
                }
            },
        )]);

        group
            .shutdown_with_timeout(Duration::from_secs(1), &shutdown)
            .await
            .expect("cooperative task should shut down before timeout");
    }

    #[tokio::test]
    async fn shutdown_with_timeout_pre_shutdown_error_allows_remaining_task_to_exit() {
        let (shutdown, mut shutdown_rx) = ShutdownSignal::channel();
        let dropped = Arc::new(AtomicBool::new(false));
        let drop_flag = DropFlag(Arc::clone(&dropped));
        let mut group = test_group(vec![
            test_task("finished-before-shutdown", async {}),
            test_shutdown_task("cooperative-after-error", async move {
                let _drop_flag = drop_flag;
                while !*shutdown_rx.borrow() {
                    if shutdown_rx.changed().await.is_err() {
                        break;
                    }
                }
            }),
        ]);

        while !group.tasks[0].handle.is_finished() {
            tokio::task::yield_now().await;
        }

        let error = group
            .shutdown_with_timeout(Duration::from_secs(1), &shutdown)
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
        let (shutdown, _) = ShutdownSignal::channel();
        let mut group = test_group(vec![
            test_task("finished-before-shutdown", async {}),
            test_task("pending-after-error", async {
                std::future::pending::<()>().await;
            }),
        ]);

        while !group.tasks[0].handle.is_finished() {
            tokio::task::yield_now().await;
        }

        let error = group
            .shutdown_with_timeout(Duration::from_millis(1), &shutdown)
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
        let (shutdown, mut shutdown_rx) = ShutdownSignal::channel();
        let mut group = test_group(vec![
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
        ]);

        let error = group
            .shutdown_with_timeout(Duration::from_millis(50), &shutdown)
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
        let (shutdown, _) = ShutdownSignal::channel();
        let mut group = test_group(vec![test_task("panic-loop", async {
            panic!("forced supervisor test panic");
        })]);

        let error = group
            .join(&shutdown)
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
