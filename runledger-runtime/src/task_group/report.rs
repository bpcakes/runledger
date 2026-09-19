use super::{JoinedRuntimeTask, RuntimeTask, TaskGroup, join_runtime_tasks};
use crate::{
    RuntimeError, RuntimeLoopExit, RuntimeLoopRecord, RuntimeShutdownBudget, RuntimeShutdownCause,
    RuntimeShutdownReport, RuntimeShutdownSettlement, UnsettledRuntimeTask,
    settlement::{RuntimeShutdownObservations, TaskRegistry},
    shutdown::ShutdownSignal,
};
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
#[cfg(test)]
use std::future::Future;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::task::AbortHandle;
use tracing::debug;

pub(super) struct PendingLoop {
    abort: AbortHandle,
    aborted: bool,
}
pub(super) type PendingLoops = HashMap<&'static str, PendingLoop>;

/// Keep the decision that controls settlement on its owner so interruption cannot
/// erase a rejected deadline after the zero-allowance fallback has been used.
pub(super) enum ShutdownBudgetDecision {
    Representable(RuntimeShutdownBudget),
    Unrepresentable { total: Duration },
}

impl ShutdownBudgetDecision {
    fn resolve(budget: RuntimeShutdownBudget, started: tokio::time::Instant) -> Self {
        if budget.deadlines(started).is_some() {
            Self::Representable(budget)
        } else {
            Self::Unrepresentable {
                total: budget.total_allowance(),
            }
        }
    }

    fn allowances(&self) -> (Duration, Duration) {
        match self {
            Self::Representable(budget) => (budget.graceful_allowance(), budget.total_allowance()),
            Self::Unrepresentable { .. } => (Duration::ZERO, Duration::ZERO),
        }
    }

    fn deadline_error(&self) -> Option<RuntimeError> {
        match self {
            Self::Representable(_) => None,
            Self::Unrepresentable { total } => {
                Some(RuntimeError::ShutdownTimeoutTooLarge { timeout: *total })
            }
        }
    }
}

impl TaskGroup {
    /// Owner destruction is not a settlement boundary. Preserve observations
    /// already made, request cancellation of remaining native work, and explicitly
    /// deny cleanup even when there are no known outstanding tasks.
    pub(crate) fn interrupted_report(
        &mut self,
        shutdown: &ShutdownSignal,
        descendants: &TaskRegistry,
    ) -> RuntimeShutdownReport {
        self.begin_immediate_shutdown(shutdown, descendants);
        descendants.abort_all();
        let (descendant_records, mut unjoined) = descendants.snapshot();
        let (callback_failures, prior_callback_interruptions) = descendants.callback_snapshot();
        for task in self.tasks.drain(..) {
            let abort_requested = !task.handle.is_finished();
            if abort_requested {
                task.handle.abort();
            }
            unjoined.push(UnsettledRuntimeTask {
                task: task.name,
                id: task.handle.id(),
                abort_requested,
            });
        }
        for (task, mut entry) in self.pending.drain() {
            if !entry.abort.is_finished() {
                entry.aborted = true;
                entry.abort.abort();
            }
            unjoined.push(UnsettledRuntimeTask {
                task,
                id: entry.abort.id(),
                abort_requested: entry.aborted,
            });
        }
        RuntimeShutdownReport::new(
            shutdown.cause(),
            std::mem::take(&mut self.records),
            descendant_records,
            RuntimeShutdownSettlement::Interrupted { unjoined },
            RuntimeShutdownObservations::from_shutdown(
                shutdown,
                self.shutdown_budget
                    .as_ref()
                    .and_then(ShutdownBudgetDecision::deadline_error),
            ),
            callback_failures,
            prior_callback_interruptions,
        )
    }

    pub(crate) fn begin_immediate_shutdown(
        &mut self,
        shutdown: &ShutdownSignal,
        descendants: &TaskRegistry,
    ) {
        self.collect_finished(shutdown, descendants);
        shutdown.request();
    }

    /// Observe existing native outcomes before enabling another stop source.
    /// Only join handles are polled; application futures stay on their tasks.
    pub(crate) fn collect_finished(
        &mut self,
        shutdown: &ShutdownSignal,
        descendants: &TaskRegistry,
    ) {
        let tasks = std::mem::take(&mut self.tasks);
        self.tasks.reserve(tasks.len());
        for task in tasks {
            if task.handle.is_finished() {
                let name = task.name;
                let result = tokio::task::unconstrained(task.handle)
                    .now_or_never()
                    .expect("a finished runtime loop join is ready");
                record_result(name, result, false, &mut self.records, shutdown);
            } else {
                self.tasks.push(task);
            }
        }
        descendants.collect_finished();
    }

    pub(crate) async fn run_report(
        &mut self,
        budget: RuntimeShutdownBudget,
        shutdown: &ShutdownSignal,
        descendants: &TaskRegistry,
    ) -> RuntimeShutdownReport {
        self.run_report_inner(
            budget,
            shutdown,
            descendants,
            #[cfg(test)]
            std::future::ready(()),
        )
        .await
    }

    #[cfg(test)]
    pub(crate) async fn run_report_with_signal<F>(
        &mut self,
        external: F,
        budget: RuntimeShutdownBudget,
        shutdown: &ShutdownSignal,
        descendants: &TaskRegistry,
    ) -> RuntimeShutdownReport
    where
        F: Future<Output = ()>,
    {
        self.run_report_with_final_harvest_hook(
            external,
            budget,
            shutdown,
            descendants,
            std::future::ready(()),
        )
        .await
    }

    #[cfg(test)]
    async fn run_report_with_final_harvest_hook<F, H>(
        &mut self,
        external: F,
        budget: RuntimeShutdownBudget,
        shutdown: &ShutdownSignal,
        descendants: &TaskRegistry,
        before_final_harvest: H,
    ) -> RuntimeShutdownReport
    where
        F: Future<Output = ()>,
        H: Future<Output = ()>,
    {
        // Fixture signals stay outside the production native settlement future.
        let report = self.run_report_inner(budget, shutdown, descendants, before_final_harvest);
        tokio::pin!(report);
        tokio::select! {
            biased;
            completed = &mut report => return completed,
            () = shutdown.requested() => {},
            () = external => shutdown.request(),
        }
        report.await
    }

    async fn run_report_inner(
        &mut self,
        budget: RuntimeShutdownBudget,
        shutdown: &ShutdownSignal,
        descendants: &TaskRegistry,
        #[cfg(test)] before_final_harvest: impl Future<Output = ()>,
    ) -> RuntimeShutdownReport {
        let tasks = std::mem::take(&mut self.tasks);
        self.pending = tasks
            .iter()
            .map(|task| {
                (
                    task.name,
                    PendingLoop {
                        abort: task.handle.abort_handle(),
                        aborted: false,
                    },
                )
            })
            .collect();
        let mut joined = join_runtime_tasks(tasks);
        // These observations belong to the owner, not this cancellable future.
        let pending = &mut self.pending;
        let records = &mut self.records;
        loop {
            collect_ready(&mut joined, pending, records, shutdown);
            descendants.collect_ready();
            if shutdown.is_requested() {
                break;
            }
            tokio::select! {
                biased;
                result = joined.next(), if !pending.is_empty() => {
                    if let Some(result) = result { record(result, pending, records, shutdown); }
                }
                () = shutdown.requested() => {}
                () = descendants.observe_until_stop() => {}
            }
        }
        let started = shutdown
            .requested_at()
            .expect("a stop request sets the clock");
        let decision = self
            .shutdown_budget
            .insert(ShutdownBudgetDecision::resolve(budget, started));
        let (graceful_allowance, total_allowance) = decision.allowances();
        let graceful = settle_until(
            &mut joined,
            pending,
            records,
            shutdown,
            descendants,
            graceful_allowance,
        )
        .await;
        if !graceful {
            descendants.abort_after_graceful_timeout();
            for task in pending.values_mut() {
                if !task.abort.is_finished() {
                    task.aborted = true;
                    task.abort.abort();
                }
            }
        }
        if !graceful {
            let _ = settle_until(
                &mut joined,
                pending,
                records,
                shutdown,
                descendants,
                total_allowance,
            )
            .await;
        }
        #[cfg(test)]
        before_final_harvest.await;
        collect_finished(&mut joined, pending, records, shutdown);
        let (descendant_records, mut unjoined) = descendants.snapshot();
        let (callback_failures, prior_callback_interruptions) = descendants.callback_snapshot();
        unjoined.extend(pending.iter().map(|(task, entry)| UnsettledRuntimeTask {
            task,
            id: entry.abort.id(),
            abort_requested: entry.aborted,
        }));
        let settlement = if graceful {
            RuntimeShutdownSettlement::Settled
        } else {
            RuntimeShutdownSettlement::after_graceful_timeout(unjoined)
        };
        RuntimeShutdownReport::new(
            shutdown.cause(),
            records.clone(),
            descendant_records,
            settlement,
            RuntimeShutdownObservations::from_shutdown(
                shutdown,
                self.shutdown_budget
                    .as_ref()
                    .and_then(ShutdownBudgetDecision::deadline_error),
            ),
            callback_failures,
            prior_callback_interruptions,
        )
    }
}

#[cfg(test)]
mod tests;

fn record(
    (name, result): JoinedRuntimeTask,
    pending: &mut PendingLoops,
    records: &mut Vec<RuntimeLoopRecord>,
    shutdown: &ShutdownSignal,
) {
    let task = pending.remove(name).expect("every loop has metadata");
    record_result(name, result, task.aborted, records, shutdown);
}

fn record_result(
    name: &'static str,
    result: std::result::Result<RuntimeLoopExit, tokio::task::JoinError>,
    abort_requested: bool,
    records: &mut Vec<RuntimeLoopRecord>,
    shutdown: &ShutdownSignal,
) {
    if !matches!(result, Ok(RuntimeLoopExit::Shutdown)) {
        if let Err(source) = &result {
            // A panicking loop never reaches RuntimeTaskFuture's completion log,
            // so this is the only place the join cause is recorded.
            debug!(
                task = name,
                is_cancelled = source.is_cancelled(),
                is_panic = source.is_panic(),
                "supervised runtime loop join failed"
            );
        }
        shutdown.request_with(RuntimeShutdownCause::LoopFailure(name));
    }
    records.push(RuntimeLoopRecord {
        task: name,
        result: result.map_err(Arc::new),
        abort_requested,
    });
}

fn collect_ready(
    joined: &mut FuturesUnordered<RuntimeTask>,
    pending: &mut PendingLoops,
    records: &mut Vec<RuntimeLoopRecord>,
    shutdown: &ShutdownSignal,
) {
    // These futures await native joins only; no application polling is unbounded.
    while let Some(Some(result)) = tokio::task::unconstrained(joined.next()).now_or_never() {
        record(result, pending, records, shutdown);
    }
}

fn collect_finished(
    joined: &mut FuturesUnordered<RuntimeTask>,
    pending: &mut PendingLoops,
    records: &mut Vec<RuntimeLoopRecord>,
    shutdown: &ShutdownSignal,
) {
    // A completed join can precede delivery of its ready notification. At a
    // settlement boundary inspect the bounded native-loop set directly; ordinary
    // waiting remains driven by FuturesUnordered's ready notifications.
    for task in std::mem::take(joined) {
        if task.handle.is_finished() {
            let name = task.name;
            let result = tokio::task::unconstrained(task.handle)
                .now_or_never()
                .expect("a finished runtime loop join is ready");
            record((name, result), pending, records, shutdown);
        } else {
            joined.push(task);
        }
    }
}

async fn settle_until(
    joined: &mut FuturesUnordered<RuntimeTask>,
    pending: &mut PendingLoops,
    records: &mut Vec<RuntimeLoopRecord>,
    shutdown: &ShutdownSignal,
    descendants: &TaskRegistry,
    allowance: Duration,
) -> bool {
    loop {
        collect_ready(joined, pending, records, shutdown);
        descendants.collect_ready();
        if pending.is_empty() && descendants.is_empty() {
            return true;
        }
        tokio::select! {
            biased;
            () = shutdown.phase_elapsed(allowance) => {
                collect_finished(joined, pending, records, shutdown);
                descendants.collect_finished();
                return pending.is_empty() && descendants.is_empty();
            }
            result = joined.next(), if !pending.is_empty() => {
                record(result.expect("pending loop metadata matches joins"), pending, records, shutdown);
            }
            () = descendants.wait(), if pending.is_empty() => return true,
        }
    }
}
