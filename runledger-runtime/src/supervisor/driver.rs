use super::Supervisor;
use crate::{
    RuntimeShutdownBudget, RuntimeShutdownReport, RuntimeShutdownSignal, shutdown::ShutdownSignal,
};
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::sync::oneshot;
use tracing::warn;

/// Waiter for an independently owned terminal settlement operation.
///
/// Dropping this value requests stop on the original clock without cancelling
/// settlement. Each completed or interrupted report is either returned to the
/// waiter or emitted as one redacted diagnostic when its delivery is abandoned.
/// The captured Tokio runtime must remain alive and driven for bounded settlement
/// to execute. Owner destruction produces an interrupted report, never cleanup
/// permission or a channel-closure panic.
#[must_use = "await the terminal driver to inspect settlement and cleanup eligibility"]
pub struct RuntimeShutdownDriver {
    report: oneshot::Receiver<UnobservedReport>,
    shutdown: ShutdownSignal,
}

impl RuntimeShutdownDriver {
    pub(super) fn start(
        mut supervisor: Supervisor,
        signal: Option<RuntimeShutdownSignal>,
        budget: RuntimeShutdownBudget,
    ) -> Self {
        let (sender, report) = oneshot::channel();
        let shutdown = supervisor.shutdown.clone();
        let runtime = supervisor.runtime.clone();
        if let Some(signal) = signal {
            // Existing native failures must establish the stop cause before a
            // ready signal can run, even on another runtime worker thread.
            supervisor
                .tasks
                .collect_finished(&shutdown, &supervisor.descendants);
            // The registry owns the signal before its first poll. Its private
            // future owner separates poll and destruction unwinds before Tokio
            // catches the primary panic; native settlement only observes joins.
            supervisor
                .descendants
                .spawn_shutdown_signal_on(&runtime, signal.into_task(shutdown.clone()));
        }
        // Construct before spawn: a runtime may reject/drop a never-polled task.
        let owner = SettlementOwner {
            supervisor,
            sender: Some(sender),
        };
        runtime.spawn(owner.run(budget));
        Self { report, shutdown }
    }

    #[cfg(test)]
    fn drop_and_retain_report_for_tests(mut self) -> oneshot::Receiver<UnobservedReport> {
        // Preserve the owner's original delivery only so a test can inspect it
        // after exercising the driver's real Drop implementation. Ordinary
        // abandonment diagnostics are covered separately.
        let (_sender, replacement) = oneshot::channel();
        let report = std::mem::replace(&mut self.report, replacement);
        drop(self);
        report
    }
}

impl Future for RuntimeShutdownDriver {
    type Output = RuntimeShutdownReport;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let driver = self.get_mut();
        match Pin::new(&mut driver.report).poll(cx) {
            Poll::Ready(Ok(report)) => Poll::Ready(report.observe()),
            Poll::Ready(Err(_)) => {
                // Even an internal failure while constructing the interruption
                // report cannot turn missing evidence into successful cleanup.
                driver.shutdown.request();
                Poll::Ready(RuntimeShutdownReport::unavailable(&driver.shutdown))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for RuntimeShutdownDriver {
    fn drop(&mut self) {
        self.shutdown.request();
    }
}

/// Owns the obligation to produce a report independently of async polling.
struct SettlementOwner {
    supervisor: Supervisor,
    sender: Option<oneshot::Sender<UnobservedReport>>,
}

impl SettlementOwner {
    async fn run(mut self, budget: RuntimeShutdownBudget) {
        let supervisor = &mut self.supervisor;
        let report = supervisor
            .tasks
            .run_report(budget, &supervisor.shutdown, &supervisor.descendants)
            .await;
        self.deliver(report);
    }

    fn deliver(&mut self, report: RuntimeShutdownReport) {
        if let Some(sender) = self.sender.take() {
            // A failed send drops the envelope; a successful send transfers the
            // same obligation to the receiver. Sending alone is not observation.
            let _ = sender.send(UnobservedReport(Some(report)));
        }
    }
}

impl Drop for SettlementOwner {
    fn drop(&mut self) {
        if self.sender.is_some() {
            let supervisor = &mut self.supervisor;
            let report = supervisor
                .tasks
                .interrupted_report(&supervisor.shutdown, &supervisor.descendants);
            self.deliver(report);
        }
    }
}

/// Linear delivery: consuming the value acknowledges observation; every other
/// destruction path owns exactly one diagnostic.
struct UnobservedReport(Option<RuntimeShutdownReport>);

impl UnobservedReport {
    fn observe(mut self) -> RuntimeShutdownReport {
        self.0.take().expect("a report envelope is consumed once")
    }
}

impl Drop for UnobservedReport {
    fn drop(&mut self) {
        if let Some(report) = &self.0 {
            warn!(
                cause = ?report.cause(),
                settlement = ?report.settlement(),
                successful = report.is_success(),
                cooperatively_stopped = report.is_cooperatively_stopped(),
                "jobs runtime settlement completed after its report waiter was dropped"
            );
        }
    }
}

#[cfg(test)]
mod tests;
