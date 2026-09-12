use super::{RuntimeTaskRecord, UnsettledRuntimeTask};

/// A handler, observer or hook interrupted by native timeout, panic or lease
/// maintenance handling. A completed handler result rejected by deadline or
/// lease fencing is not an interruption. Payload access is explicit; automatic
/// formatting does not reveal panic contents.
#[derive(Clone)]
pub enum RuntimeCallbackFailure {
    TimedOut {
        callback: &'static str,
    },
    Panicked {
        callback: &'static str,
        message: String,
    },
    /// Handler execution was interrupted after lease loss or failure to maintain it.
    LeaseMaintenance {
        callback: &'static str,
    },
}

impl std::fmt::Debug for RuntimeCallbackFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimedOut { callback } => f.debug_tuple("TimedOut").field(callback).finish(),
            Self::Panicked { callback, .. } => f.debug_tuple("Panicked").field(callback).finish(),
            Self::LeaseMaintenance { callback } => {
                f.debug_tuple("LeaseMaintenance").field(callback).finish()
            }
        }
    }
}
use crate::{RuntimeError, RuntimeLoopExit};
use std::{sync::Arc, time::Duration};
use tokio::{task::JoinError, time::Instant};

/// Validated graceful and abort/join intervals. Both consume one first-stop clock.
#[derive(Clone, Copy, Debug)]
pub struct RuntimeShutdownBudget {
    graceful: Duration,
    abort: Duration,
}

impl RuntimeShutdownBudget {
    pub(crate) fn graceful_allowance(self) -> Duration {
        self.graceful
    }

    /// Validate the complete allowance before starting native work. Zero graceful
    /// time requests immediate escalation; zero abort time permits ready joins only.
    /// An unrepresentable sum returns [`RuntimeError::ShutdownBudgetOverflow`]
    /// with both inputs. A representable sum that cannot form an instant returns
    /// [`RuntimeError::ShutdownTimeoutTooLarge`] with that total.
    pub fn new(graceful: Duration, abort: Duration) -> Result<Self, RuntimeError> {
        let total = graceful
            .checked_add(abort)
            .ok_or(RuntimeError::ShutdownBudgetOverflow { graceful, abort })?;
        Instant::now()
            .checked_add(total)
            .ok_or(RuntimeError::ShutdownTimeoutTooLarge { timeout: total })?;
        Ok(Self { graceful, abort })
    }

    pub fn total_allowance(self) -> Duration {
        self.graceful + self.abort
    }

    pub(crate) fn deadlines(self, start: Instant) -> Option<(Instant, Instant)> {
        Some((
            start.checked_add(self.graceful)?,
            start.checked_add(self.total_allowance())?,
        ))
    }
}

/// The first observed reason to start stopping this runtime instance.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RuntimeShutdownCause {
    Requested,
    LoopFailure(&'static str),
    DescendantFailure,
}

/// An observed top-level native loop result, independent of other loop outcomes.
#[derive(Clone)]
pub struct RuntimeLoopRecord {
    pub task: &'static str,
    pub result: Result<RuntimeLoopExit, Arc<JoinError>>,
    /// A request was issued; a successful join means completion won the race.
    pub abort_requested: bool,
}

impl std::fmt::Debug for RuntimeLoopRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeLoopRecord")
            .field("task", &self.task)
            .field("abort_requested", &self.abort_requested)
            .field("joined", &true)
            .field("failed", &self.failed())
            .finish()
    }
}

impl RuntimeLoopRecord {
    pub fn failed(&self) -> bool {
        !matches!(self.result, Ok(RuntimeLoopExit::Shutdown))
    }
}

/// Complete bounded native settlement evidence. This is an internal report, not
/// a wire payload. Retains every loop outcome and descendant failure observed in
/// settlement; ordinary business outcomes remain durable job records.
#[must_use = "inspect native settlement before releasing its dependencies"]
pub struct RuntimeShutdownReport {
    pub cause: RuntimeShutdownCause,
    pub loops: Vec<RuntimeLoopRecord>,
    pub descendants: Vec<RuntimeTaskRecord>,
    pub unjoined: Vec<UnsettledRuntimeTask>,
    pub graceful_timed_out: bool,
    pub abort_timed_out: bool,
    pub deadline_error: Option<RuntimeError>,
    /// Callback interruptions observed after stopping began, with every cause retained.
    pub callback_failures: Vec<RuntimeCallbackFailure>,
    /// Earlier handler/observer/hook interruption facts, not unique callbacks.
    /// One callback can contribute several observed causes. This count bounds
    /// retained history; it never expires and permanently disqualifies cooperative
    /// cleanup for this runtime instance.
    pub prior_callback_interruptions: u64,
}

impl RuntimeShutdownReport {
    /// Whether shutdown succeeded and cooperative dependency cleanup is justified.
    /// This is not durable job health: even a historical callback interruption
    /// disqualifies success because its detached descendants cannot be accounted for.
    /// Conversely, a joined configuration failure can permit cleanup but fail here.
    pub fn is_success(&self) -> bool {
        self.is_cooperatively_stopped()
            && !self.graceful_timed_out
            && self.deadline_error.is_none()
            && self.loops.iter().all(|record| !record.failed())
            && self.descendants.iter().all(|record| record.error.is_none())
    }

    /// Conservative dependency-cleanup eligibility. An abort or failed join does
    /// not prove that arbitrary descendants created by application callbacks stopped.
    /// Normal callback return relies on the application having settled its own
    /// children; this report does not discover arbitrary detached application tasks.
    /// An abort request that loses to successful task completion is retained as
    /// evidence, but does not imply interruption or prevent cleanup.
    pub fn is_cooperatively_stopped(&self) -> bool {
        self.prior_callback_interruptions == 0
            && self.callback_failures.is_empty()
            && self.unjoined.is_empty()
            && !self.abort_timed_out
            && self.loops.iter().all(|record| record.result.is_ok())
            && self.descendants.iter().all(|record| record.error.is_none())
    }
}

impl std::fmt::Debug for RuntimeShutdownReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeShutdownReport")
            .field("cause", &self.cause)
            .field("loops", &self.loops)
            .field("descendants", &self.descendants)
            .field("unjoined", &self.unjoined)
            .field("graceful_timed_out", &self.graceful_timed_out)
            .field("abort_timed_out", &self.abort_timed_out)
            .field("deadline_failed", &self.deadline_error.is_some())
            .field("callback_failures", &self.callback_failures)
            .field(
                "prior_callback_interruptions",
                &self.prior_callback_interruptions,
            )
            .finish()
    }
}

#[cfg(test)]
mod tests;
