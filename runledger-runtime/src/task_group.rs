use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::stream::FuturesUnordered;
use tokio::runtime::Handle;
use tokio::task::{JoinError, JoinHandle};
use tracing::{Instrument, debug, info_span};

use crate::RuntimeLoopExit;

mod report;

#[must_use]
pub(crate) struct TaskGroup {
    tasks: Vec<RuntimeTask>,
    records: Vec<crate::RuntimeLoopRecord>,
    pending: report::PendingLoops,
    shutdown_budget: Option<report::ShutdownBudgetDecision>,
}

struct RuntimeTask {
    name: &'static str,
    handle: JoinHandle<RuntimeLoopExit>,
}

struct RuntimeTaskFuture {
    name: &'static str,
    future: Pin<Box<dyn Future<Output = RuntimeLoopExit> + Send>>,
    started: bool,
}

type RuntimeTaskJoinResult = std::result::Result<RuntimeLoopExit, JoinError>;
type JoinedRuntimeTask = (&'static str, RuntimeTaskJoinResult);

impl TaskGroup {
    pub(crate) fn new() -> Self {
        Self {
            tasks: Vec::new(),
            records: Vec::new(),
            pending: report::PendingLoops::new(),
            shutdown_budget: None,
        }
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

    #[cfg(test)]
    fn from_tasks_for_tests(tasks: Vec<RuntimeTask>) -> Self {
        Self {
            tasks,
            records: Vec::new(),
            pending: report::PendingLoops::new(),
            shutdown_budget: None,
        }
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

    #[cfg(test)]
    pub(crate) async fn wait_until_finished_for_tests(&self) {
        while self.tasks.iter().any(|task| !task.handle.is_finished()) {
            tokio::task::yield_now().await;
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
            handle: runtime.spawn(RuntimeTaskFuture::new(name, future).instrument(span)),
        }
    }

    #[cfg(test)]
    fn spawn<F>(name: &'static str, future: F) -> Self
    where
        F: Future<Output = RuntimeLoopExit> + Send + 'static,
    {
        Self {
            name,
            handle: tokio::spawn(RuntimeTaskFuture::new(name, future)),
        }
    }
}

impl Future for RuntimeTask {
    type Output = JoinedRuntimeTask;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let task = self.get_mut();
        Pin::new(&mut task.handle)
            .poll(cx)
            .map(|result| (task.name, result))
    }
}

impl RuntimeTaskFuture {
    fn new<F>(name: &'static str, future: F) -> Self
    where
        F: Future<Output = RuntimeLoopExit> + Send + 'static,
    {
        Self {
            name,
            future: Box::pin(future),
            started: false,
        }
    }
}

impl Future for RuntimeTaskFuture {
    type Output = RuntimeLoopExit;

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

fn join_runtime_tasks(tasks: Vec<RuntimeTask>) -> FuturesUnordered<RuntimeTask> {
    tasks.into_iter().collect()
}
