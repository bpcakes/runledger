//! Shared observation of native-owned descendant joins.

use futures_util::{
    FutureExt, StreamExt,
    future::{BoxFuture, Shared},
    stream::FuturesUnordered,
    task::{ArcWake, noop_waker_ref, waker_ref},
};
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
use tokio::task::{AbortHandle, Id, JoinError, JoinHandle};

use crate::shutdown::ShutdownSignal;

mod report;
pub use report::{
    RuntimeCallbackFailure, RuntimeLoopRecord, RuntimeShutdownBudget, RuntimeShutdownCause,
    RuntimeShutdownReport,
};

/// One observed native task exit. Error contents are available only by explicit access.
#[derive(Clone)]
pub struct RuntimeTaskRecord {
    pub task: &'static str,
    pub id: Id,
    /// A request was issued; a successful join means completion won the race.
    pub abort_requested: bool,
    pub error: Option<Arc<JoinError>>,
}

impl std::fmt::Debug for RuntimeTaskRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeTaskRecord")
            .field("task", &self.task)
            .field("id", &self.id)
            .field("abort_requested", &self.abort_requested)
            .field("failed", &self.error.is_some())
            .finish()
    }
}

/// A native descendant whose join has not completed at the report boundary.
#[derive(Clone, Debug)]
pub struct UnsettledRuntimeTask {
    pub task: &'static str,
    pub id: Id,
    pub abort_requested: bool,
}

type SharedResult<T> = Shared<BoxFuture<'static, Result<T, Arc<JoinError>>>>;

struct Entry {
    task: &'static str,
    wake: Arc<EntryWake>,
    abort: AbortHandle,
    aborted: bool,
    result: BoxFuture<'static, Result<(), Arc<JoinError>>>,
}

#[derive(Default)]
struct State {
    entries: HashMap<Id, Entry>,
    records: Vec<RuntimeTaskRecord>,
    shutdown: Option<ShutdownSignal>,
    aborting: bool,
    callback_failures: Vec<RuntimeCallbackFailure>,
    prior_callback_interruptions: u64,
}

/// The supervisor retains this registry independently of individual loop futures.
#[derive(Clone, Default)]
pub(crate) struct TaskRegistry(Arc<RegistryInner>);

#[derive(Default)]
struct RegistryInner {
    state: Mutex<State>,
    signal: Arc<RegistrySignal>,
}

// A Shared join invokes registered wakers under its notifier lock. Its waker
// must not own the registry: dropping the last registry there would drop that
// same Shared join and try to acquire its notifier lock recursively.
#[derive(Default)]
struct RegistrySignal {
    changed: tokio::sync::Notify,
    ready: Mutex<VecDeque<Weak<EntryWake>>>,
}

// Each notification identifies one join. Neither a task waker nor the ready
// queue owns registry state or a Shared future. Weak queue entries also prevent
// a signal -> queued waker -> signal retention cycle at registry destruction.
struct EntryWake {
    id: Id,
    signal: Arc<RegistrySignal>,
    queued: AtomicBool,
}

impl ArcWake for EntryWake {
    fn wake_by_ref(inner: &Arc<Self>) {
        if !inner.queued.swap(true, Ordering::AcqRel) {
            inner
                .signal
                .ready
                .lock()
                .expect("ready queue is not poisoned")
                .push_back(Arc::downgrade(inner));
            inner.signal.changed.notify_waiters();
        }
    }
}

impl TaskRegistry {
    pub(crate) fn record_hook(
        &self,
        name: &'static str,
        outcome: &crate::dead_letter_hook::DeadLetterHookOutcome,
    ) {
        use crate::dead_letter_hook::DeadLetterHookOutcome;
        match outcome {
            DeadLetterHookOutcome::Completed => {}
            DeadLetterHookOutcome::TimedOut => {
                self.record_callback(RuntimeCallbackFailure::TimedOut { callback: name })
            }
            DeadLetterHookOutcome::Panicked(message) => {
                self.record_callback(RuntimeCallbackFailure::Panicked {
                    callback: name,
                    message: message.clone(),
                })
            }
        }
    }
    pub(crate) fn record_callback(&self, failure: RuntimeCallbackFailure) {
        let mut state = self.0.state.lock().expect("task registry is not poisoned");
        let Some(shutdown) = &state.shutdown else {
            return;
        };
        if shutdown.is_requested() {
            state.callback_failures.push(failure);
        } else {
            state.prior_callback_interruptions =
                state.prior_callback_interruptions.saturating_add(1);
        }
    }

    pub(crate) fn callback_snapshot(&self) -> (Vec<RuntimeCallbackFailure>, u64) {
        let state = self.0.state.lock().expect("task registry is not poisoned");
        (
            state.callback_failures.clone(),
            state.prior_callback_interruptions,
        )
    }
    pub(crate) fn supervised(shutdown: ShutdownSignal) -> Self {
        Self(Arc::new(RegistryInner {
            state: Mutex::new(State {
                shutdown: Some(shutdown),
                ..State::default()
            }),
            signal: Arc::default(),
        }))
    }

    pub(crate) fn track<T>(&self, task: &'static str, handle: JoinHandle<T>) -> SharedJoin<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        let abort = handle.abort_handle();
        let id = abort.id();
        let wake = Arc::new(EntryWake {
            id,
            signal: self.0.signal.clone(),
            queued: AtomicBool::new(false),
        });
        let result = async move { handle.await.map_err(Arc::new) }
            .boxed()
            .shared();
        let observed = result.clone();
        self.0
            .state
            .lock()
            .expect("task registry is not poisoned")
            .entries
            .insert(
                id,
                Entry {
                    task,
                    wake: wake.clone(),
                    abort: abort.clone(),
                    aborted: false,
                    result: async move { observed.await.map(|_| ()) }.boxed(),
                },
            );
        EntryWake::wake_by_ref(&wake);
        SharedJoin {
            result,
            abort,
            registry: Arc::downgrade(&self.0),
        }
    }

    pub(crate) fn spawn<T, F>(&self, task: &'static str, future: F) -> SharedJoin<T>
    where
        T: Clone + Send + Sync + 'static,
        F: Future<Output = T> + Send + 'static,
    {
        let (start, started) = tokio::sync::oneshot::channel();
        let join = self.track(
            task,
            tokio::spawn(async move {
                // Registration owns the task before any application future is polled.
                if started.await.is_err() {
                    std::future::pending::<()>().await;
                }
                future.await
            }),
        );
        let mut state = self.0.state.lock().expect("task registry is not poisoned");
        if state.aborting {
            if let Some(entry) = state.entries.get_mut(&join.abort.id()) {
                entry.aborted = true;
            }
            join.abort.abort();
        } else {
            let _ = start.send(());
        }
        join
    }

    pub(crate) fn collect_ready(&self) {
        self.collect(false);
    }

    fn collect(&self, include_finished: bool) {
        // Poll only join handles, never application futures. A depleted Tokio
        // cooperative budget must not hide a join that is already available.
        let harvest = std::future::poll_fn(|cx| self.poll_empty(cx, include_finished));
        let harvest = tokio::task::unconstrained(harvest);
        tokio::pin!(harvest);
        let _ = harvest
            .as_mut()
            .poll(&mut Context::from_waker(noop_waker_ref()));
    }

    fn ready_batch(&self, state: &State, include_finished: bool) -> VecDeque<Weak<EntryWake>> {
        let mut ready = std::mem::take(
            &mut *self
                .0
                .signal
                .ready
                .lock()
                .expect("ready queue is not poisoned"),
        );
        if include_finished {
            let queued: std::collections::HashSet<_> = ready
                .iter()
                .filter_map(Weak::upgrade)
                .map(|wake| Arc::as_ptr(&wake))
                .collect();
            ready.extend(
                state
                    .entries
                    .values()
                    .filter(|entry| {
                        entry.abort.is_finished() && !queued.contains(&Arc::as_ptr(&entry.wake))
                    })
                    .map(|entry| Arc::downgrade(&entry.wake)),
            );
        }
        ready
    }

    fn poll_empty(&self, _: &mut Context<'_>, include_finished: bool) -> Poll<()> {
        // Serialize collectors before taking notifications. Shared polling only
        // locks the separate ready queue, never registry state. Boundary checks
        // inspect each finished handle once; ordinary observation stays event-driven.
        let mut state = self.0.state.lock().expect("task registry is not poisoned");
        let ready = self.ready_batch(&state, include_finished);
        let mut completed = Vec::new();
        for wake in ready.into_iter().filter_map(|wake| wake.upgrade()) {
            let Some(entry) = state.entries.get_mut(&wake.id) else {
                continue;
            };
            if !Arc::ptr_eq(&entry.wake, &wake) {
                continue;
            }
            wake.queued.store(false, Ordering::Release);
            let waker = waker_ref(&wake);
            if let Poll::Ready(result) =
                entry.result.as_mut().poll(&mut Context::from_waker(&waker))
            {
                completed.push((wake.id, result));
            }
        }
        let changed = !completed.is_empty();
        for (id, result) in completed {
            let entry = state
                .entries
                .remove(&id)
                .expect("completed task is registered");
            let unexpected_failure = result
                .as_ref()
                .is_err_and(|error| error.is_panic() || !entry.aborted);
            if unexpected_failure && let Some(shutdown) = &state.shutdown {
                shutdown.request_with(crate::RuntimeShutdownCause::DescendantFailure);
            }
            let stopping = state
                .shutdown
                .as_ref()
                .is_some_and(ShutdownSignal::is_requested);
            if state.shutdown.is_some() && !stopping && entry.aborted && result.is_err() {
                // Routine best-effort aborts must neither stop processing nor
                // accumulate an unbounded history. They still forbid claiming
                // that application descendants settled cooperatively.
                state.prior_callback_interruptions =
                    state.prior_callback_interruptions.saturating_add(1);
            } else if state.shutdown.is_some() && (stopping || result.is_err()) {
                state.records.push(RuntimeTaskRecord {
                    task: entry.task,
                    id,
                    abort_requested: entry.aborted,
                    error: result.err(),
                });
            }
        }
        if changed {
            self.0.signal.changed.notify_waiters();
        }
        if state.entries.is_empty() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    pub(crate) async fn wait(&self) {
        loop {
            let notified = self.0.signal.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.collect_ready();
            if self
                .0
                .state
                .lock()
                .expect("task registry is not poisoned")
                .entries
                .is_empty()
            {
                return;
            }
            notified.await;
        }
    }

    pub(crate) async fn observe_while<F: Future>(&self, future: F) -> F::Output {
        tokio::pin!(future);
        tokio::select! {
            biased;
            result = &mut future => result,
            () = self.observe_until_stop() => future.await,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.collect(true);
        self.0
            .state
            .lock()
            .expect("task registry is not poisoned")
            .entries
            .is_empty()
    }

    pub(crate) async fn observe_until_stop(&self) {
        loop {
            let notified = self.0.signal.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            self.collect_ready();
            if self
                .0
                .state
                .lock()
                .expect("task registry is not poisoned")
                .shutdown
                .as_ref()
                .is_some_and(ShutdownSignal::is_requested)
            {
                return;
            }
            notified.await;
        }
    }

    pub(crate) fn abort_all(&self) {
        self.collect(true);
        let mut state = self.0.state.lock().expect("task registry is not poisoned");
        state.aborting = true;
        for entry in state.entries.values_mut() {
            if !entry.abort.is_finished() {
                entry.aborted = true;
                entry.abort.abort();
            }
        }
    }

    pub(crate) fn snapshot(&self) -> (Vec<RuntimeTaskRecord>, Vec<UnsettledRuntimeTask>) {
        self.collect(true);
        let state = self.0.state.lock().expect("task registry is not poisoned");
        let pending = state
            .entries
            .iter()
            .map(|(id, entry)| UnsettledRuntimeTask {
                task: entry.task,
                id: *id,
                abort_requested: entry.aborted,
            })
            .collect();
        (state.records.clone(), pending)
    }

    pub(crate) fn first_unexpected_failure(&self) -> Option<crate::RuntimeError> {
        self.snapshot().0.into_iter().find_map(|record| {
            record.error.and_then(|source| {
                (source.is_panic() || !record.abort_requested).then_some(
                    crate::RuntimeError::DescendantJoin {
                        task: record.task,
                        source,
                    },
                )
            })
        })
    }
}

impl<T: Clone + Send + Sync + 'static> From<JoinHandle<T>> for SharedJoin<T> {
    fn from(handle: JoinHandle<T>) -> Self {
        TaskRegistry::default().track("native_task", handle)
    }
}

pub(crate) struct SharedJoin<T: Clone> {
    result: SharedResult<T>,
    abort: AbortHandle,
    registry: Weak<RegistryInner>,
}

impl<T: Clone> SharedJoin<T> {
    pub(crate) fn is_finished(&self) -> bool {
        self.abort.is_finished()
    }
    pub(crate) fn abort(&self) {
        if self.abort.is_finished() {
            return;
        }
        if let Some(registry) = self.registry.upgrade() {
            let mut state = registry
                .state
                .lock()
                .expect("task registry is not poisoned");
            if let Some(entry) = state.entries.get_mut(&self.abort.id()) {
                entry.aborted = true;
            }
        }
        self.abort.abort();
    }
}

impl<T: Clone> Future for SharedJoin<T> {
    type Output = Result<T, Arc<JoinError>>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result = Pin::new(&mut self.result).poll(cx);
        if result.is_ready()
            && let Some(registry) = self.registry.upgrade()
        {
            TaskRegistry(registry).collect_ready();
        }
        result
    }
}

type Joined<T> = (Id, Result<T, Arc<JoinError>>);

struct TaskWait<T: Clone> {
    id: Id,
    join: SharedJoin<T>,
}
impl<T: Clone> Future for TaskWait<T> {
    type Output = Joined<T>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let id = self.id;
        Pin::new(&mut self.join).poll(cx).map(|result| (id, result))
    }
}

/// Native loop-local admission and waiting, with joins independently retained above it.
pub(crate) struct TaskSet<T: Clone + Send + Sync + 'static> {
    registry: TaskRegistry,
    name: &'static str,
    pending: FuturesUnordered<TaskWait<T>>,
    aborts: HashMap<Id, AbortHandle>,
}

impl<T: Clone + Send + Sync + 'static> TaskSet<T> {
    pub(crate) fn new() -> Self {
        Self::with_registry(TaskRegistry::default(), "native_task")
    }
    pub(crate) fn with_registry(registry: TaskRegistry, name: &'static str) -> Self {
        Self {
            registry,
            name,
            pending: FuturesUnordered::new(),
            aborts: HashMap::new(),
        }
    }
    pub(crate) fn registry(&self) -> TaskRegistry {
        self.registry.clone()
    }
    pub(crate) fn spawn<F>(&mut self, future: F) -> AbortHandle
    where
        F: Future<Output = T> + Send + 'static,
    {
        let join = self.registry.spawn(self.name, future);
        let abort = join.abort.clone();
        let id = abort.id();
        self.pending.push(TaskWait { id, join });
        self.aborts.insert(id, abort.clone());
        abort
    }
    pub(crate) fn len(&self) -> usize {
        self.aborts.len()
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.aborts.is_empty()
    }
    pub(crate) fn abort_all(&self) {
        let mut state = self
            .registry
            .0
            .state
            .lock()
            .expect("task registry is not poisoned");
        for (id, abort) in &self.aborts {
            if !abort.is_finished() {
                if let Some(entry) = state.entries.get_mut(id) {
                    entry.aborted = true;
                }
                abort.abort();
            }
        }
    }
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Joined<T>>> {
        let result = self.pending.poll_next_unpin(cx);
        if let Poll::Ready(Some((id, _))) = &result {
            self.aborts.remove(id);
        }
        result
    }
    pub(crate) async fn join_next(&mut self) -> Option<Result<T, Arc<JoinError>>> {
        std::future::poll_fn(|cx| self.poll_next(cx))
            .await
            .map(|(_, result)| result)
    }
    pub(crate) async fn join_next_with_id(&mut self) -> Option<Result<(Id, T), Arc<JoinError>>> {
        std::future::poll_fn(|cx| self.poll_next(cx))
            .await
            .map(|(id, result)| result.map(|value| (id, value)))
    }
    pub(crate) fn try_join_next(&mut self) -> Option<Result<T, Arc<JoinError>>> {
        let next = std::future::poll_fn(|cx| self.poll_next(cx));
        let next = tokio::task::unconstrained(next);
        tokio::pin!(next);
        match next.poll(&mut Context::from_waker(noop_waker_ref())) {
            Poll::Ready(result) => result.map(|(_, result)| result),
            Poll::Pending => None,
        }
    }
}

impl<T: Clone + Send + Sync + 'static> Drop for TaskSet<T> {
    fn drop(&mut self) {
        self.abort_all();
    }
}

#[cfg(test)]
mod tests;
