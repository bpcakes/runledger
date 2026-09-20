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

impl RuntimeCallbackFailure {
    /// The interrupted callback's static name, whatever the interruption cause.
    #[must_use]
    pub fn callback(&self) -> &'static str {
        match self {
            Self::TimedOut { callback }
            | Self::Panicked { callback, .. }
            | Self::LeaseMaintenance { callback } => callback,
        }
    }
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
use crate::config::JobsConfigValidationError;
use crate::{RuntimeError, RuntimeLoopExit, RuntimeShutdownSignalError};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use thiserror::Error;
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

    /// Validate the complete allowance before starting native work. A signal
    /// listener that did not initiate stop must retire within the graceful
    /// allowance. Zero graceful time immediately escalates any such registered
    /// listener. The exact initiating listener is not force-aborted at that
    /// boundary and may use the remaining total allowance to finish guarded
    /// destruction and join. Zero abort time permits ready joins only.
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
pub enum RuntimeShutdownCause {
    Requested,
    /// The external stop input returned an error and requested shutdown.
    SignalFailed,
    LoopFailure(&'static str),
    DescendantFailure {
        task: &'static str,
        id: tokio::task::Id,
    },
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

    /// Whether this loop's only failure is the cancellation the supervisor asked
    /// for. Such a record is a consequence of stopping, not a reason for it.
    #[must_use]
    pub fn cancelled_by_abort(&self) -> bool {
        self.abort_requested
            && self
                .result
                .as_ref()
                .is_err_and(|source| source.is_cancelled())
    }

    /// This loop's outcome as a classified failure, or `None` when it exited on
    /// shutdown as expected.
    #[must_use]
    pub fn failure(&self) -> Option<RuntimeShutdownFailure> {
        match &self.result {
            Ok(RuntimeLoopExit::Shutdown) => None,
            Ok(RuntimeLoopExit::Completed) => {
                Some(RuntimeShutdownFailure::LoopExitedUnexpectedly { task: self.task })
            }
            Ok(RuntimeLoopExit::InvalidConfig(source)) => {
                Some(RuntimeShutdownFailure::LoopInvalidConfig {
                    task: self.task,
                    source: *source,
                })
            }
            Err(source) => Some(RuntimeShutdownFailure::LoopJoin {
                task: self.task,
                source: Arc::clone(source),
            }),
        }
    }
}

/// The primary retained reason a [`RuntimeShutdownReport`] is not a success.
///
/// This is classification for logs, alerts and process exit codes. It is not an
/// authorization: [`RuntimeShutdownReport::classify`] can allow cleanup
/// while this is present, and only that typed decision decides whether dependency
/// cleanup may run. The originating report retains every other
/// observed reason; this names one. Automatic debug formatting retains the
/// classification and safe metadata but redacts join sources, whose panic
/// payloads remain available only through explicit variant matching.
#[derive(Clone, Error)]
pub enum RuntimeShutdownFailure {
    /// The external stop input returned an error retained by the report.
    #[error("jobs runtime shutdown signal failed")]
    Signal {
        #[source]
        source: RuntimeShutdownSignalError,
    },
    /// The external stop input panicked while it was polled or destroyed.
    /// Detailed, redacted-by-default evidence remains on the report.
    #[error("jobs runtime shutdown signal panicked")]
    SignalPanicked,
    /// A supervised loop returned `Completed` instead of the expected shutdown exit.
    #[error("jobs runtime loop `{task}` completed unexpectedly")]
    LoopExitedUnexpectedly { task: &'static str },
    /// A supervised loop rejected its configuration after build validation.
    #[error("jobs runtime loop `{task}` rejected invalid configuration")]
    LoopInvalidConfig {
        task: &'static str,
        #[source]
        source: JobsConfigValidationError,
    },
    /// A supervised loop panicked or failed to join.
    #[error("failed joining jobs runtime loop `{task}`")]
    LoopJoin {
        task: &'static str,
        #[source]
        source: Arc<JoinError>,
    },
    /// A native-owned descendant panicked or was cancelled unexpectedly.
    #[error("failed joining jobs runtime descendant `{task}`")]
    DescendantJoin {
        task: &'static str,
        #[source]
        source: Arc<JoinError>,
    },
    /// A handler, observer or hook was interrupted while stopping, so
    /// descendants it may have created cannot be accounted for.
    #[error("jobs runtime callback `{callback}` was interrupted while stopping")]
    CallbackInterrupted { callback: &'static str },
    /// Callbacks were interrupted before stopping began. This count never
    /// expires and permanently disqualifies cooperative cleanup.
    #[error("jobs runtime observed {count} callback interruption(s) before stopping began")]
    EarlierCallbackInterruptions { count: u64 },
    /// Native work had not settled when the graceful allowance elapsed.
    #[error("jobs runtime native work did not settle within the graceful allowance")]
    GracefulTimeout,
    /// Tasks did not settle within the total allowance after aborting.
    #[error(
        "jobs runtime tasks did not settle within the abort allowance ({unjoined} task(s) unjoined)"
    )]
    AbortTimeout { unjoined: NonZeroUsize },
    /// The budget could not be represented as a deadline, so no settlement time
    /// was allowed. [`RuntimeShutdownReport::deadline_error`] retains the
    /// rejected allowance. Validation at construction cannot prove that the
    /// same duration fits a stop instant reached much later.
    #[error("jobs runtime shutdown budget could not be represented as a deadline")]
    UnrepresentableDeadline,
    /// The settlement owner ended before establishing a final join boundary.
    /// Retained evidence is partial and never authorizes dependency cleanup.
    #[error("jobs runtime settlement owner ended before completing observation")]
    SettlementInterrupted,
}

impl std::fmt::Debug for RuntimeShutdownFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Signal { source } => f.debug_struct("Signal").field("source", source).finish(),
            Self::SignalPanicked => f.write_str("SignalPanicked"),
            Self::LoopExitedUnexpectedly { task } => f
                .debug_struct("LoopExitedUnexpectedly")
                .field("task", task)
                .finish(),
            Self::LoopInvalidConfig { task, source } => f
                .debug_struct("LoopInvalidConfig")
                .field("task", task)
                .field("source", source)
                .finish(),
            Self::LoopJoin { task, source } => f
                .debug_struct("LoopJoin")
                .field("task", task)
                .field("cancelled", &source.is_cancelled())
                .field("panicked", &source.is_panic())
                .finish(),
            Self::DescendantJoin { task, source } => f
                .debug_struct("DescendantJoin")
                .field("task", task)
                .field("cancelled", &source.is_cancelled())
                .field("panicked", &source.is_panic())
                .finish(),
            Self::CallbackInterrupted { callback } => f
                .debug_struct("CallbackInterrupted")
                .field("callback", callback)
                .finish(),
            Self::EarlierCallbackInterruptions { count } => f
                .debug_struct("EarlierCallbackInterruptions")
                .field("count", count)
                .finish(),
            Self::GracefulTimeout => f.write_str("GracefulTimeout"),
            Self::AbortTimeout { unjoined } => f
                .debug_struct("AbortTimeout")
                .field("unjoined", unjoined)
                .finish(),
            Self::UnrepresentableDeadline => f.write_str("UnrepresentableDeadline"),
            Self::SettlementInterrupted => f.write_str("SettlementInterrupted"),
        }
    }
}

/// The mutually exclusive terminal state of bounded native settlement.
///
/// A graceful timeout means native work missed the graceful boundary and the
/// runtime entered its abort phase, but every native task was joined within the
/// total allowance. Per-task evidence records whether an abort was actually
/// requested. An abort timeout retains the exact tasks that were still unjoined
/// at the report boundary.
#[derive(Clone, Debug)]
pub enum RuntimeShutdownSettlement {
    Settled,
    GracefulTimeout,
    AbortTimeout {
        unjoined: UnjoinedRuntimeTasks,
    },
    /// The owner was cancelled or unwound before its final settlement boundary.
    /// These are known unjoined tasks, not a complete inventory. Even an empty
    /// list cannot prove cooperative settlement.
    Interrupted {
        unjoined: Vec<UnsettledRuntimeTask>,
    },
}

/// A nonempty collection of native tasks still unjoined at a report boundary.
///
/// Values are created only from a runtime boundary observation that contains at
/// least one task. The private representation prevents consumers from inventing
/// an empty [`RuntimeShutdownSettlement::AbortTimeout`] while preserving
/// read-only access to every retained task.
#[derive(Clone, Debug)]
pub struct UnjoinedRuntimeTasks {
    tasks: Vec<UnsettledRuntimeTask>,
}

impl UnjoinedRuntimeTasks {
    pub(crate) fn new(tasks: Vec<UnsettledRuntimeTask>) -> Option<Self> {
        (!tasks.is_empty()).then_some(Self { tasks })
    }

    /// Every task that was still unjoined at the report boundary.
    #[must_use]
    pub fn as_slice(&self) -> &[UnsettledRuntimeTask] {
        &self.tasks
    }

    /// The nonzero number of tasks that remained unjoined.
    #[must_use]
    pub fn len(&self) -> NonZeroUsize {
        NonZeroUsize::new(self.tasks.len()).expect("unjoined runtime tasks are nonempty")
    }
}

impl RuntimeShutdownSettlement {
    pub(crate) fn after_graceful_timeout(unjoined: Vec<UnsettledRuntimeTask>) -> Self {
        match UnjoinedRuntimeTasks::new(unjoined) {
            Some(unjoined) => Self::AbortTimeout { unjoined },
            None => Self::GracefulTimeout,
        }
    }

    fn unjoined(&self) -> &[UnsettledRuntimeTask] {
        match self {
            Self::AbortTimeout { unjoined } => unjoined.as_slice(),
            Self::Interrupted { unjoined } => unjoined,
            Self::Settled | Self::GracefulTimeout => &[],
        }
    }

    fn graceful_timed_out(&self) -> bool {
        matches!(self, Self::GracefulTimeout | Self::AbortTimeout { .. })
    }

    fn abort_timed_out(&self) -> bool {
        matches!(self, Self::AbortTimeout { .. })
    }
}

/// The consuming, report-derived classification of shutdown and cleanup authority.
///
/// Match all three outcomes at the application shutdown boundary. The distinct,
/// opaque payloads cannot be constructed, relabeled, or separated from their
/// evidence by downstream code. Only the two cleanup-safe payloads can yield a
/// permit, and doing so consumes the payload. Neither reports nor payloads clone.
///
/// ```compile_fail
/// use runledger_runtime::{RuntimeSettlement, RuntimeStoppedWithFailures};
/// fn relabel(failed: RuntimeStoppedWithFailures) -> RuntimeSettlement {
///     RuntimeSettlement::Clean(failed)
/// }
/// ```
#[derive(Debug)]
#[must_use = "match the settlement before releasing dependencies or choosing process status"]
pub enum RuntimeSettlement {
    /// Shutdown succeeded and dependency cleanup is permitted.
    Clean(RuntimeCleanSettlement),
    /// Shutdown failed, but dependency cleanup is permitted.
    StoppedWithFailures(RuntimeStoppedWithFailures),
    /// Cleanup cannot be proven safe. Tracked tasks may all have joined, but an
    /// earlier interrupted callback can have left untracked application children.
    Unsettled(RuntimeUnsettled),
}

impl RuntimeSettlement {
    /// Retained evidence for diagnostics. Borrowing cannot reclassify the report
    /// or issue additional permits.
    pub fn report(&self) -> &RuntimeShutdownReport {
        match self {
            Self::Clean(outcome) => outcome.report(),
            Self::StoppedWithFailures(outcome) => outcome.report(),
            Self::Unsettled(outcome) => outcome.report(),
        }
    }
}

/// Proof of successful shutdown, with one owned cleanup capability.
///
/// ```compile_fail
/// use runledger_runtime::RuntimeCleanSettlement;
/// fn extract_twice(clean: RuntimeCleanSettlement) {
///     let first = clean.into_cleanup_permit();
///     let second = clean.into_cleanup_permit();
/// }
/// ```
///
/// ```compile_fail
/// use runledger_runtime::{RuntimeCleanSettlement, RuntimeShutdownReport, RuntimeShutdownCleanupPermit};
/// fn forge(report: RuntimeShutdownReport, cleanup: RuntimeShutdownCleanupPermit) -> RuntimeCleanSettlement {
///     RuntimeCleanSettlement { report, cleanup }
/// }
/// ```
#[derive(Debug)]
#[must_use = "consume the clean settlement at the dependency cleanup boundary"]
pub struct RuntimeCleanSettlement {
    report: RuntimeShutdownReport,
    cleanup: RuntimeShutdownCleanupPermit,
}

impl RuntimeCleanSettlement {
    pub fn report(&self) -> &RuntimeShutdownReport {
        &self.report
    }

    /// Transfer the sole cleanup capability for this report to an application
    /// adapter. Inspect or log the borrowed evidence before consuming this value.
    pub fn into_cleanup_permit(self) -> RuntimeShutdownCleanupPermit {
        self.cleanup
    }
}

/// A failed shutdown whose dependency cleanup has nevertheless been proven safe.
///
/// ```compile_fail
/// use runledger_runtime::RuntimeStoppedWithFailures;
/// fn duplicate(stopped: RuntimeStoppedWithFailures) {
///     let copy = stopped.clone();
/// }
/// ```
#[derive(Debug)]
#[must_use = "retain the failure and consume the cleanup capability"]
pub struct RuntimeStoppedWithFailures {
    report: RuntimeShutdownReport,
    failure: RuntimeShutdownFailure,
    cleanup: RuntimeShutdownCleanupPermit,
}

impl RuntimeStoppedWithFailures {
    pub fn report(&self) -> &RuntimeShutdownReport {
        &self.report
    }

    /// The primary process failure; inspect the report for all retained evidence.
    #[must_use]
    pub fn failure(&self) -> &RuntimeShutdownFailure {
        &self.failure
    }

    /// Consume this outcome into its cleanup authority and process failure.
    /// The required failure cannot be lost through a success-shaped return value.
    pub fn into_parts(self) -> (RuntimeShutdownCleanupPermit, RuntimeShutdownFailure) {
        (self.cleanup, self.failure)
    }
}

/// Evidence insufficient to authorize dependency cleanup. No operation on this
/// payload yields a cleanup permit or an owned report that could be reclassified.
///
/// ```compile_fail
/// use runledger_runtime::RuntimeUnsettled;
/// fn release(outcome: RuntimeUnsettled) {
///     let permit = outcome.into_cleanup_permit();
/// }
/// ```
#[derive(Debug)]
#[must_use = "inspect the unsettled evidence and retain dependencies"]
pub struct RuntimeUnsettled {
    report: RuntimeShutdownReport,
    failure: RuntimeShutdownFailure,
}

impl RuntimeUnsettled {
    pub fn report(&self) -> &RuntimeShutdownReport {
        &self.report
    }

    #[must_use]
    pub fn failure(&self) -> &RuntimeShutdownFailure {
        &self.failure
    }

    /// Consume the evidence into its primary process failure, without authorizing
    /// dependency cleanup.
    #[must_use]
    pub fn into_failure(self) -> RuntimeShutdownFailure {
        self.failure
    }
}

/// An externally unforgeable capability to release dependencies after shutdown.
///
/// Values originate only from consuming [`RuntimeShutdownReport::classify`]
/// and extracting a cleanup-safe outcome. This type is neither `Clone` nor
/// `Copy`. An application cleanup adapter must require and consume the permit.
/// The permit proves this runtime's settlement; it does not identify a pool,
/// prove other runtimes stopped, or prevent direct calls to external resources.
///
/// ```compile_fail
/// use runledger_runtime::RuntimeShutdownCleanupPermit;
///
/// let _ = RuntimeShutdownCleanupPermit { _private: () };
/// ```
///
/// ```compile_fail
/// use runledger_runtime::RuntimeShutdownCleanupPermit;
/// fn duplicate(permit: RuntimeShutdownCleanupPermit) {
///     let copy = permit.clone();
/// }
/// ```
#[derive(Debug)]
#[must_use = "pass the cleanup permit to the dependency-release boundary"]
pub struct RuntimeShutdownCleanupPermit {
    _private: (),
}

/// Bounded native settlement evidence, explicitly partial when interrupted.
/// This is an internal report, not
/// a wire payload. Retains every loop outcome and descendant failure observed in
/// settlement; ordinary business outcomes remain durable job records.
#[must_use = "inspect native settlement before releasing its dependencies"]
pub struct RuntimeShutdownReport {
    cause: RuntimeShutdownCause,
    loops: Vec<RuntimeLoopRecord>,
    descendants: Vec<RuntimeTaskRecord>,
    settlement: RuntimeShutdownSettlement,
    observations: RuntimeShutdownObservations,
    /// Callback interruptions observed after stopping began, with every cause retained.
    callback_failures: Vec<RuntimeCallbackFailure>,
    /// Earlier handler/observer/hook interruption facts, not unique callbacks.
    /// One callback can contribute several observed causes. This count bounds
    /// retained history; it never expires and permanently disqualifies cooperative
    /// cleanup for this runtime instance.
    prior_callback_interruptions: u64,
}

/// Owner-held observations shared by every report path. Requiring the bundle
/// at construction prevents final/interrupted paths from silently omitting a
/// newly retained observation.
#[derive(Default)]
pub(crate) struct RuntimeShutdownObservations {
    pub(crate) deadline_error: Option<RuntimeError>,
    pub(crate) signal_error: Option<RuntimeShutdownSignalError>,
    pub(crate) signal_panic: Option<crate::RuntimeShutdownSignalPanic>,
}

impl RuntimeShutdownObservations {
    pub(crate) fn from_shutdown(
        shutdown: &crate::shutdown::ShutdownSignal,
        deadline_error: Option<RuntimeError>,
    ) -> Self {
        Self {
            deadline_error,
            signal_error: shutdown.signal_error(),
            signal_panic: shutdown.signal_panic(),
        }
    }
}

impl RuntimeShutdownReport {
    pub(crate) fn unavailable(shutdown: &crate::shutdown::ShutdownSignal) -> Self {
        Self::new(
            shutdown.cause(),
            Vec::new(),
            Vec::new(),
            RuntimeShutdownSettlement::Interrupted {
                unjoined: Vec::new(),
            },
            RuntimeShutdownObservations::from_shutdown(shutdown, None),
            Vec::new(),
            0,
        )
    }

    pub(crate) fn new(
        cause: RuntimeShutdownCause,
        loops: Vec<RuntimeLoopRecord>,
        descendants: Vec<RuntimeTaskRecord>,
        settlement: RuntimeShutdownSettlement,
        observations: RuntimeShutdownObservations,
        callback_failures: Vec<RuntimeCallbackFailure>,
        prior_callback_interruptions: u64,
    ) -> Self {
        Self {
            cause,
            loops,
            descendants,
            settlement,
            observations,
            callback_failures,
            prior_callback_interruptions,
        }
    }

    /// The first observed reason native shutdown began.
    #[must_use]
    pub fn cause(&self) -> RuntimeShutdownCause {
        self.cause.clone()
    }

    /// Every observed top-level loop outcome, in observation order.
    #[must_use]
    pub fn loops(&self) -> &[RuntimeLoopRecord] {
        &self.loops
    }

    /// Every observed native descendant outcome, in observation order.
    #[must_use]
    pub fn descendants(&self) -> &[RuntimeTaskRecord] {
        &self.descendants
    }

    /// The mutually exclusive terminal settlement state.
    #[must_use]
    pub fn settlement(&self) -> &RuntimeShutdownSettlement {
        &self.settlement
    }

    /// Known native tasks still unjoined at the report boundary. Interrupted
    /// settlement has no complete final inventory; an empty slice is not proof
    /// that native work stopped.
    #[must_use]
    pub fn unjoined(&self) -> &[UnsettledRuntimeTask] {
        self.settlement.unjoined()
    }

    /// Whether the graceful allowance elapsed before native settlement.
    #[must_use]
    pub fn graceful_timed_out(&self) -> bool {
        self.settlement.graceful_timed_out()
    }

    /// Whether native tasks remained unsettled after the abort allowance.
    #[must_use]
    pub fn abort_timed_out(&self) -> bool {
        self.settlement.abort_timed_out()
    }

    /// A shutdown-budget deadline construction error, when one occurred.
    #[must_use]
    pub fn deadline_error(&self) -> Option<&RuntimeError> {
        self.observations.deadline_error.as_ref()
    }

    /// The original returned signal error, when observed. It fails shutdown but,
    /// after normal destruction and a successful tracked join, does not by itself
    /// deny dependency cleanup. A signal panic, custom-signal cancellation, or
    /// unjoined signal instead denies cleanup. Cancellation of a runtime-authored
    /// listener explicitly requested by the live settlement owner is accounted
    /// only after its tracked join proves guarded destruction completed. Signal
    /// failure never stops native settlement.
    #[must_use]
    pub fn signal_error(&self) -> Option<&RuntimeShutdownSignalError> {
        self.observations.signal_error.as_ref()
    }

    /// Observed signal polling and/or destruction panics, including both when
    /// they occurred on one future. The primary panic remains a fatal
    /// `shutdown_signal` descendant join; interrupted reports may only retain
    /// observations made before their owner was lost.
    #[must_use]
    pub fn signal_panic(&self) -> Option<&crate::RuntimeShutdownSignalPanic> {
        self.observations.signal_panic.as_ref()
    }

    /// Callback interruptions observed after shutdown began.
    #[must_use]
    pub fn callback_failures(&self) -> &[RuntimeCallbackFailure] {
        &self.callback_failures
    }

    /// Number of callback interruptions observed before shutdown began.
    #[must_use]
    pub fn prior_callback_interruptions(&self) -> u64 {
        self.prior_callback_interruptions
    }

    /// Consume this report into the single authoritative shutdown outcome.
    /// Historical callback interruptions remain `Unsettled`, even if every
    /// tracked task subsequently joins: untracked children cannot be accounted for.
    ///
    /// ```compile_fail
    /// use runledger_runtime::RuntimeShutdownReport;
    /// fn classify_twice(report: RuntimeShutdownReport) {
    ///     let first = report.classify();
    ///     let second = report.classify();
    /// }
    /// ```
    ///
    /// ```compile_fail
    /// use runledger_runtime::RuntimeShutdownReport;
    /// fn duplicate_report(report: RuntimeShutdownReport) {
    ///     let copy = report.clone();
    /// }
    /// ```
    ///
    /// ```compile_fail
    /// use runledger_runtime::RuntimeSettlement;
    /// fn reclassify(outcome: RuntimeSettlement) {
    ///     let duplicate = outcome.report().classify();
    /// }
    /// ```
    pub fn classify(self) -> RuntimeSettlement {
        match (self.permits_dependency_cleanup(), self.failure()) {
            (true, None) => RuntimeSettlement::Clean(RuntimeCleanSettlement {
                report: self,
                cleanup: RuntimeShutdownCleanupPermit { _private: () },
            }),
            (true, Some(failure)) => {
                RuntimeSettlement::StoppedWithFailures(RuntimeStoppedWithFailures {
                    report: self,
                    failure,
                    cleanup: RuntimeShutdownCleanupPermit { _private: () },
                })
            }
            (false, failure) => RuntimeSettlement::Unsettled(RuntimeUnsettled {
                failure: failure.expect("denied cleanup retains a shutdown failure"),
                report: self,
            }),
        }
    }

    /// Whether shutdown succeeded.
    ///
    /// Success implies dependency cleanup eligibility, but the typed
    /// [`Self::classify`] remains the authority to use at that boundary.
    /// This is not durable job health: even a historical callback interruption
    /// disqualifies success because its detached descendants cannot be accounted for.
    /// Conversely, a joined configuration failure can permit cleanup but fail here.
    pub(crate) fn is_success(&self) -> bool {
        self.permits_dependency_cleanup()
            && matches!(&self.settlement, RuntimeShutdownSettlement::Settled)
            && self.deadline_error().is_none()
            && self.signal_error().is_none()
            && self.loops.iter().all(|record| !record.failed())
            && self.descendants.iter().all(|record| record.error.is_none())
    }

    /// The primary retained reason this report is not a success, classified for
    /// logs, alerts and process exit codes. This is `None` exactly when
    /// classification would produce [`RuntimeSettlement::Clean`].
    ///
    /// A failure here does not by itself forbid dependency cleanup, and its
    /// absence is not what authorizes cleanup: match
    /// [`Self::classify`] for that decision. Precedence follows
    /// the recorded first cause:
    ///
    /// - `SignalFailed` reports the returned signal error before settlement or
    ///   later task failures.
    /// - `LoopFailure` and `DescendantFailure` report that triggering join or
    ///   exit before failures observed while draining.
    /// - `Requested` reports an interrupted/unrepresentable/timeout settlement
    ///   before a signal, callback, or task failure observed afterward.
    ///
    /// Thus an earlier requested stop that misses its budget reports the timeout
    /// even if a signal later returns an error or a task fails while draining.
    /// Inspect the report's observations and task records for every failure; this
    /// single classification is not a complete incident log.
    #[must_use]
    pub fn failure(&self) -> Option<RuntimeShutdownFailure> {
        match &self.cause {
            RuntimeShutdownCause::SignalFailed => self
                .signal_failure()
                .or_else(|| self.settlement_failure())
                .or_else(|| self.triggering_failure()),
            RuntimeShutdownCause::LoopFailure(_)
            | RuntimeShutdownCause::DescendantFailure { .. } => self
                .triggering_failure()
                .or_else(|| self.settlement_failure()),
            RuntimeShutdownCause::Requested => self
                .settlement_failure()
                .or_else(|| self.triggering_failure()),
        }
    }

    /// A loop or descendant outcome that was itself a failure, independent of
    /// whether the shutdown budget was met.
    ///
    /// A task we aborted ourselves reports a cancellation, which is a
    /// consequence of stopping rather than a reason for it. Those are considered
    /// only once no task has failed on its own.
    fn triggering_failure(&self) -> Option<RuntimeShutdownFailure> {
        let triggering = match &self.cause {
            RuntimeShutdownCause::LoopFailure(task) => self
                .loops
                .iter()
                .find(|record| record.task == *task)
                .and_then(RuntimeLoopRecord::failure),
            RuntimeShutdownCause::DescendantFailure { id, task } => self
                .descendants
                .iter()
                .find(|record| record.id == *id)
                .and_then(RuntimeTaskRecord::failure)
                .or_else(|| {
                    // Signal polling publishes its fatal cause before Drop or
                    // join. Its retained panic is already triggering evidence;
                    // unrelated failures while draining must not displace it.
                    use crate::RuntimeShutdownSignalPanic::{Poll, PollAndDestruction};
                    match (*task, self.signal_panic()) {
                        (
                            crate::shutdown_signal::TASK_NAME,
                            Some(Poll { .. } | PollAndDestruction { .. }),
                        ) => Some(RuntimeShutdownFailure::SignalPanicked),
                        _ => None,
                    }
                }),
            RuntimeShutdownCause::Requested | RuntimeShutdownCause::SignalFailed => None,
        };
        triggering
            .or_else(|| self.task_failure(true))
            .or_else(|| self.task_failure(false))
            // The normal production path also retains a failed descendant join.
            // This fallback keeps independently retained panic evidence fatal if
            // join normalization or an interrupted producer omits that record.
            .or_else(|| self.signal_panic().map(|_| RuntimeShutdownFailure::SignalPanicked))
    }

    fn task_failure(&self, exclude_own_aborts: bool) -> Option<RuntimeShutdownFailure> {
        let keep = |cancelled_by_abort: bool| !(exclude_own_aborts && cancelled_by_abort);
        self.loops
            .iter()
            .filter(|record| keep(record.cancelled_by_abort()))
            .find_map(RuntimeLoopRecord::failure)
            .or_else(|| {
                self.descendants
                    .iter()
                    .filter(|record| keep(record.cancelled_by_abort()))
                    .find_map(RuntimeTaskRecord::failure)
            })
    }

    /// Something that went wrong while stopping, rather than a task outcome.
    fn settlement_failure(&self) -> Option<RuntimeShutdownFailure> {
        if matches!(
            self.settlement,
            RuntimeShutdownSettlement::Interrupted { .. }
        ) {
            return Some(RuntimeShutdownFailure::SettlementInterrupted);
        }
        if self.deadline_error().is_some() {
            return Some(RuntimeShutdownFailure::UnrepresentableDeadline);
        }
        match &self.settlement {
            RuntimeShutdownSettlement::Settled => {}
            RuntimeShutdownSettlement::GracefulTimeout => {
                return Some(RuntimeShutdownFailure::GracefulTimeout);
            }
            RuntimeShutdownSettlement::AbortTimeout { unjoined } => {
                return Some(RuntimeShutdownFailure::AbortTimeout {
                    unjoined: unjoined.len(),
                });
            }
            RuntimeShutdownSettlement::Interrupted { .. } => unreachable!("handled above"),
        }
        if let Some(failure) = self.signal_failure() {
            return Some(failure);
        }
        if let Some(interrupted) = self.callback_failures.first() {
            return Some(RuntimeShutdownFailure::CallbackInterrupted {
                callback: interrupted.callback(),
            });
        }
        if self.prior_callback_interruptions > 0 {
            return Some(RuntimeShutdownFailure::EarlierCallbackInterruptions {
                count: self.prior_callback_interruptions,
            });
        }
        None
    }

    fn signal_failure(&self) -> Option<RuntimeShutdownFailure> {
        self.signal_error()
            .cloned()
            .map(|source| RuntimeShutdownFailure::Signal { source })
    }

    /// Internal diagnostic projection; public callers consume `classify`.
    #[must_use]
    pub(crate) fn is_cooperatively_stopped(&self) -> bool {
        self.permits_dependency_cleanup()
    }

    /// Conservative dependency-cleanup eligibility. A failed join does not prove
    /// that arbitrary descendants created by application callbacks stopped.
    /// Normal callback return relies on the application having settled its own
    /// children; this report does not discover arbitrary detached application tasks.
    /// An abort request that loses to successful task completion is retained as
    /// evidence, but does not imply interruption or prevent cleanup. The tracked
    /// runtime-authored signal's owner-requested cancellation is likewise
    /// accounted only after its join proves guarded destruction completed. A
    /// force-aborted custom signal retains its cancelled join and denies cleanup.
    /// Independently retained signal panic evidence always denies cleanup, even
    /// if its correlated join record was normalized or unavailable.
    fn permits_dependency_cleanup(&self) -> bool {
        self.prior_callback_interruptions == 0
            && self.callback_failures.is_empty()
            && self.signal_panic().is_none()
            && matches!(
                self.settlement,
                RuntimeShutdownSettlement::Settled | RuntimeShutdownSettlement::GracefulTimeout
            )
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
            .field("settlement", &self.settlement)
            .field("deadline_failed", &self.deadline_error().is_some())
            .field("signal_failed", &self.signal_error().is_some())
            .field("signal_panic", &self.signal_panic())
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
