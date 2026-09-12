use super::{JoinedRuntimeTask, TaskGroup, join_runtime_tasks};
use crate::{
    RuntimeError, RuntimeLoopExit, RuntimeLoopRecord, RuntimeShutdownBudget, RuntimeShutdownCause,
    RuntimeShutdownReport, UnsettledRuntimeTask, settlement::TaskRegistry,
    shutdown::ShutdownSignal,
};
use futures_util::{FutureExt, StreamExt, stream::FuturesUnordered};
use std::{collections::HashMap, future::Future, sync::Arc, time::Duration};
use tokio::task::AbortHandle;

struct PendingLoop {
    abort: AbortHandle,
    aborted: bool,
}
type PendingLoops = HashMap<&'static str, PendingLoop>;

impl TaskGroup {
    pub(crate) async fn run_report<F>(
        &mut self,
        external: F,
        budget: RuntimeShutdownBudget,
        shutdown: &ShutdownSignal,
        descendants: &TaskRegistry,
    ) -> RuntimeShutdownReport
    where
        F: Future<Output = ()>,
    {
        let tasks = std::mem::take(&mut self.tasks);
        let mut pending: PendingLoops = tasks
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
        let mut records = Vec::new();
        tokio::pin!(external);
        loop {
            collect_ready(&mut joined, &mut pending, &mut records, shutdown);
            descendants.collect_ready();
            if shutdown.is_requested() {
                break;
            }
            tokio::select! {
                biased;
                result = joined.next(), if !pending.is_empty() => {
                    if let Some(result) = result { record(result, &mut pending, &mut records, shutdown); }
                }
                () = shutdown.requested() => {}
                () = descendants.observe_until_stop() => {}
                () = &mut external => shutdown.request(),
            }
        }
        let started = shutdown
            .requested_at()
            .expect("a stop request sets the clock");
        let deadlines = budget.deadlines(started);
        let deadline_error = deadlines
            .is_none()
            .then_some(RuntimeError::ShutdownTimeoutTooLarge {
                timeout: budget.total_allowance(),
            });
        let graceful = settle_until(
            &mut joined,
            &mut pending,
            &mut records,
            shutdown,
            descendants,
            if deadline_error.is_some() {
                Duration::ZERO
            } else {
                budget.graceful_allowance()
            },
        )
        .await;
        if !graceful {
            descendants.abort_all();
            for task in pending.values_mut() {
                if !task.abort.is_finished() {
                    task.aborted = true;
                    task.abort.abort();
                }
            }
        }
        let settled = graceful
            || settle_until(
                &mut joined,
                &mut pending,
                &mut records,
                shutdown,
                descendants,
                if deadline_error.is_some() {
                    Duration::ZERO
                } else {
                    budget.total_allowance()
                },
            )
            .await;
        collect_ready(&mut joined, &mut pending, &mut records, shutdown);
        let (descendant_records, mut unjoined) = descendants.snapshot();
        let (callback_failures, prior_callback_interruptions) = descendants.callback_snapshot();
        unjoined.extend(
            pending
                .into_iter()
                .map(|(task, entry)| UnsettledRuntimeTask {
                    task,
                    id: entry.abort.id(),
                    abort_requested: entry.aborted,
                }),
        );
        RuntimeShutdownReport {
            cause: shutdown.cause(),
            loops: records,
            descendants: descendant_records,
            unjoined,
            graceful_timed_out: !graceful,
            abort_timed_out: !settled,
            deadline_error,
            callback_failures,
            prior_callback_interruptions,
        }
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
    if !matches!(result, Ok(RuntimeLoopExit::Shutdown)) {
        shutdown.request_with(RuntimeShutdownCause::LoopFailure(name));
    }
    records.push(RuntimeLoopRecord {
        task: name,
        result: result.map_err(Arc::new),
        abort_requested: task.aborted,
    });
}

fn collect_ready(
    joined: &mut FuturesUnordered<impl Future<Output = JoinedRuntimeTask>>,
    pending: &mut PendingLoops,
    records: &mut Vec<RuntimeLoopRecord>,
    shutdown: &ShutdownSignal,
) {
    // These futures await native joins only; no application polling is unbounded.
    while let Some(Some(result)) = tokio::task::unconstrained(joined.next()).now_or_never() {
        record(result, pending, records, shutdown);
    }
}

async fn settle_until(
    joined: &mut FuturesUnordered<impl Future<Output = JoinedRuntimeTask>>,
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
                collect_ready(joined, pending, records, shutdown);
                descendants.collect_ready();
                return pending.is_empty() && descendants.is_empty();
            }
            result = joined.next(), if !pending.is_empty() => {
                record(result.expect("pending loop metadata matches joins"), pending, records, shutdown);
            }
            () = descendants.wait(), if pending.is_empty() => return true,
        }
    }
}
