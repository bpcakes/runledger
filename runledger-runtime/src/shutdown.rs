use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::sleep;

#[derive(Clone)]
pub(crate) struct ShutdownSignal {
    shutdown_tx: watch::Sender<bool>,
    startup: Option<crate::startup::Initialization>,
    state: Arc<Mutex<ShutdownState>>,
}

#[derive(Default)]
struct ShutdownState {
    request: Option<(tokio::time::Instant, crate::RuntimeShutdownCause)>,
    signal_error: Option<crate::RuntimeShutdownSignalError>,
    signal_panic: Option<crate::RuntimeShutdownSignalPanic>,
}

#[derive(Clone)]
pub(crate) struct ShutdownHandle {
    signal: ShutdownSignal,
}

impl ShutdownSignal {
    pub(crate) fn channel() -> (Self, watch::Receiver<bool>) {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        (
            Self {
                shutdown_tx,
                startup: None,
                state: Arc::new(Mutex::new(ShutdownState::default())),
            },
            shutdown_rx,
        )
    }

    pub(crate) fn track_startup(&mut self, startup: crate::startup::Initialization) {
        self.startup = Some(startup);
    }

    pub(crate) fn handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            signal: self.clone(),
        }
    }

    pub(crate) fn request(&self) {
        self.request_with(crate::RuntimeShutdownCause::Requested);
    }

    /// Commit one terminal signal poll as one arbitration event.
    ///
    /// Error/panic evidence, first cause, and initiating-task identity are published
    /// under the same lock. There is deliberately no separate error-recording
    /// operation that could expose a half-committed signal trigger.
    pub(crate) fn request_after_signal_trigger(
        &self,
        task_state: &crate::shutdown_signal::ShutdownSignalTaskState,
        trigger: crate::shutdown_signal::SignalTrigger,
    ) {
        use crate::shutdown_signal::SignalTrigger;
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        let cause = match trigger {
            SignalTrigger::Output(None) => crate::RuntimeShutdownCause::Requested,
            SignalTrigger::Output(Some(error)) => {
                debug_assert!(
                    state.signal_error.is_none(),
                    "one signal produces one output"
                );
                state.signal_error.get_or_insert(error);
                crate::RuntimeShutdownCause::SignalFailed
            }
            SignalTrigger::PollPanicked { id, message } => {
                state.signal_panic = Some(crate::RuntimeShutdownSignalPanic::Poll { message });
                crate::RuntimeShutdownCause::DescendantFailure {
                    task: crate::shutdown_signal::TASK_NAME,
                    id,
                }
            }
        };
        if state.request.is_none() {
            task_state.mark_initiating();
            state.request = Some((tokio::time::Instant::now(), cause));
        }
        drop(state);
        self.publish_request();
    }

    pub(crate) fn signal_error(&self) -> Option<crate::RuntimeShutdownSignalError> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .signal_error
            .clone()
    }

    pub(crate) fn signal_destruction_panicked(&self, message: String) {
        use crate::RuntimeShutdownSignalPanic;
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.signal_panic = Some(match state.signal_panic.take() {
            Some(RuntimeShutdownSignalPanic::Poll {
                message: poll_message,
            }) => RuntimeShutdownSignalPanic::PollAndDestruction {
                poll_message,
                destruction_message: message,
            },
            None => RuntimeShutdownSignalPanic::Destruction { message },
            Some(observed) => observed,
        });
    }

    pub(crate) fn signal_panic(&self) -> Option<crate::RuntimeShutdownSignalPanic> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .signal_panic
            .clone()
    }

    pub(crate) fn request_with(&self, cause: crate::RuntimeShutdownCause) {
        self.request_since(tokio::time::Instant::now(), cause);
    }

    pub(crate) fn request_since(
        &self,
        started: tokio::time::Instant,
        cause: crate::RuntimeShutdownCause,
    ) {
        let started = started.min(tokio::time::Instant::now());
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            match &mut state.request {
                Some((earliest, _)) => *earliest = (*earliest).min(started),
                None => {
                    state.request = Some((started, cause));
                }
            }
        }
        self.publish_request();
    }

    fn publish_request(&self) {
        if let Some(startup) = &self.startup {
            startup.stop();
        }
        self.shutdown_tx.send_replace(true);
    }

    pub(crate) fn is_requested(&self) -> bool {
        self.requested_at().is_some()
    }

    pub(crate) fn requested_at(&self) -> Option<tokio::time::Instant> {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .request
            .as_ref()
            .map(|(instant, _)| *instant)
    }

    pub(crate) fn cause(&self) -> crate::RuntimeShutdownCause {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .request
            .as_ref()
            .map_or(crate::RuntimeShutdownCause::Requested, |(_, cause)| {
                cause.clone()
            })
    }

    // Read the clock after subscribing, and keep listening after the first stop.
    // An enclosing owner can tighten a deadline while settlement is already waiting.
    pub(crate) async fn phase_elapsed(&self, allowance: Duration) {
        let mut updates = self.shutdown_tx.subscribe();
        loop {
            updates.borrow_and_update();
            let started = self
                .requested_at()
                .expect("phase wait follows a stop request");
            let deadline = started
                .checked_add(allowance)
                .unwrap_or_else(tokio::time::Instant::now);
            tokio::select! {
                biased;
                () = tokio::time::sleep_until(deadline) => return,
                _ = updates.changed() => {},
            }
        }
    }

    pub(crate) async fn requested(&self) {
        wait_for_request(&mut self.shutdown_tx.subscribe()).await;
    }
}

impl ShutdownHandle {
    pub(crate) fn request(&self) {
        self.signal.request();
    }

    pub(crate) fn is_requested(&self) -> bool {
        self.signal.is_requested()
    }

    pub(crate) fn request_since(&self, started: tokio::time::Instant) -> tokio::time::Instant {
        self.signal
            .request_since(started, crate::RuntimeShutdownCause::Requested);
        self.signal
            .requested_at()
            .expect("request records its clock")
    }

    pub(crate) async fn requested(&self) {
        self.signal.requested().await;
    }
}

pub(crate) fn is_requested_or_closed(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow() || shutdown.has_changed().is_err()
}

pub(crate) async fn wait_for_request(shutdown: &mut watch::Receiver<bool>) {
    while !is_requested_or_closed(shutdown) {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

pub(crate) async fn wait_for_request_or_timeout(
    shutdown: &mut watch::Receiver<bool>,
    timeout: Duration,
) -> bool {
    tokio::select! {
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
        _ = sleep(timeout) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::{Barrier, watch};

    use super::*;

    fn test_task_id() -> tokio::task::Id {
        let handle = tokio::spawn(async {});
        let id = handle.id();
        handle.abort();
        id
    }

    #[test]
    fn signal_error_trigger_commits_one_legal_winning_state() {
        let (signal, _) = ShutdownSignal::channel();
        let task_state = crate::shutdown_signal::ShutdownSignalTaskState::pending_for_tests();

        signal.request_after_signal_trigger(
            &task_state,
            crate::shutdown_signal::SignalTrigger::Output(Some(
                crate::RuntimeShutdownSignalError::new(io::Error::other("signal failed")),
            )),
        );

        assert_eq!(signal.cause(), crate::RuntimeShutdownCause::SignalFailed);
        assert!(signal.signal_error().is_some());
        assert!(task_state.is_initiating());
    }

    #[test]
    fn signal_error_trigger_commits_one_legal_losing_state() {
        let (signal, _) = ShutdownSignal::channel();
        let task_state = crate::shutdown_signal::ShutdownSignalTaskState::pending_for_tests();
        signal.request_with(crate::RuntimeShutdownCause::LoopFailure("first"));

        signal.request_after_signal_trigger(
            &task_state,
            crate::shutdown_signal::SignalTrigger::Output(Some(
                crate::RuntimeShutdownSignalError::new(io::Error::other("later signal failure")),
            )),
        );

        assert_eq!(
            signal.cause(),
            crate::RuntimeShutdownCause::LoopFailure("first")
        );
        assert!(signal.signal_error().is_some());
        assert!(!task_state.is_initiating());
    }

    #[tokio::test(start_paused = true)]
    async fn tightening_wakes_a_phase_that_is_already_awaiting_its_old_timer() {
        let (signal, _) = ShutdownSignal::channel();
        let earlier = tokio::time::Instant::now();
        tokio::time::advance(Duration::from_secs(5)).await;
        signal.request();
        let waiting = signal.clone();
        let (entered, entry) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            entered.send(()).expect("waiter entry observed");
            waiting.phase_elapsed(Duration::from_secs(3)).await;
        });
        entry.await.expect("waiter has polled");
        assert!(!task.is_finished());
        signal.handle().request_since(earlier);
        tokio::task::yield_now().await;
        assert!(task.is_finished(), "tightened native timer did not wake");
        task.await.expect("native phase waiter joined");
    }

    #[tokio::test(start_paused = true)]
    async fn earlier_parent_tightens_active_phase_wait_without_replacing_native_cause() {
        use futures_util::FutureExt;
        let (signal, _) = ShutdownSignal::channel();
        let parent_started = tokio::time::Instant::now();
        tokio::time::advance(Duration::from_secs(5)).await;
        signal.request_with(crate::RuntimeShutdownCause::LoopFailure("first"));
        let phase = signal.phase_elapsed(Duration::from_secs(12));
        tokio::pin!(phase);
        assert!(phase.as_mut().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(8)).await;
        assert!(phase.as_mut().now_or_never().is_none());
        signal.handle().request_since(parent_started);
        assert!(phase.as_mut().now_or_never().is_some());
        assert_eq!(signal.requested_at(), Some(parent_started));
        assert_eq!(
            signal.cause(),
            crate::RuntimeShutdownCause::LoopFailure("first")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn later_requests_do_not_extend_an_active_phase_wait() {
        use futures_util::FutureExt;
        let (signal, _) = ShutdownSignal::channel();
        let first = tokio::time::Instant::now();
        signal.request();
        let phase = signal.phase_elapsed(Duration::from_secs(2));
        tokio::pin!(phase);
        assert!(phase.as_mut().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(1)).await;
        signal.request_with(crate::RuntimeShutdownCause::DescendantFailure {
            task: "later",
            id: test_task_id(),
        });
        assert!(phase.as_mut().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(phase.as_mut().now_or_never().is_some());
        assert_eq!(signal.requested_at(), Some(first));
        assert_eq!(signal.cause(), crate::RuntimeShutdownCause::Requested);
    }

    #[tokio::test(start_paused = true)]
    async fn enclosing_stop_clock_is_retained_without_restarting_native_allowance() {
        let (signal, _) = ShutdownSignal::channel();
        let started = tokio::time::Instant::now();
        tokio::time::advance(Duration::from_secs(3)).await;
        signal.handle().request_since(started);
        assert_eq!(signal.requested_at(), Some(started));
        signal.handle().request();
        signal.handle().requested().await;
        assert_eq!(signal.requested_at(), Some(started));
    }

    #[tokio::test(start_paused = true)]
    async fn future_parent_clock_cannot_delay_shutdown_and_native_cause_stays_first() {
        let (signal, _) = ShutdownSignal::channel();
        let now = tokio::time::Instant::now();
        signal.handle().request_since(now + Duration::from_secs(30));
        assert_eq!(signal.requested_at(), Some(now));
        let (native, _) = ShutdownSignal::channel();
        let descendant = crate::RuntimeShutdownCause::DescendantFailure {
            task: "native",
            id: test_task_id(),
        };
        native.request_with(descendant.clone());
        native.handle().request_since(now);
        assert_eq!(native.cause(), descendant);
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_stop_observer_never_requests_native_shutdown() {
        let (signal, _) = ShutdownSignal::channel();
        let handle = signal.handle();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), handle.requested())
                .await
                .is_err()
        );
        assert!(!handle.is_requested());
        handle.request();
        handle.requested().await;
    }

    #[test]
    fn repeated_requests_keep_watch_state_observable() {
        let (shutdown, mut receiver) = ShutdownSignal::channel();

        shutdown.request();

        assert!(shutdown.is_requested());
        assert!(receiver.has_changed().expect("receiver should see request"));
        assert!(*receiver.borrow_and_update());

        shutdown.request();

        assert!(shutdown.is_requested());
        assert!(
            receiver
                .has_changed()
                .expect("receiver should see repeated request")
        );
        assert!(*receiver.borrow_and_update());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_requests_converge_on_watch_state() {
        const REQUESTERS: usize = 8;

        let (shutdown, mut receiver) = ShutdownSignal::channel();
        let barrier = Arc::new(Barrier::new(REQUESTERS + 1));
        let mut requesters = Vec::with_capacity(REQUESTERS);

        for _ in 0..REQUESTERS {
            let handle = shutdown.handle();
            let barrier = Arc::clone(&barrier);
            requesters.push(tokio::spawn(async move {
                barrier.wait().await;
                handle.request();
            }));
        }

        barrier.wait().await;
        for requester in requesters {
            requester.await.expect("shutdown requester must not panic");
        }

        assert!(shutdown.is_requested());
        assert!(receiver.has_changed().expect("receiver should see request"));
        assert!(*receiver.borrow_and_update());
    }

    #[test]
    fn request_is_retained_after_all_receivers_are_dropped() {
        let (shutdown, receiver) = ShutdownSignal::channel();
        drop(receiver);

        shutdown.request();

        assert!(shutdown.is_requested());
        let replacement_receiver = shutdown.shutdown_tx.subscribe();
        assert!(*replacement_receiver.borrow());
    }

    #[test]
    fn sender_and_receiver_clones_keep_channel_open_until_the_last_drop() {
        let (shutdown, receiver) = ShutdownSignal::channel();
        let handle = shutdown.handle();
        let surviving_receiver = receiver.clone();

        drop(shutdown);
        drop(receiver);

        assert!(!is_requested_or_closed(&surviving_receiver));

        handle.request();

        assert!(handle.is_requested());
        assert!(is_requested_or_closed(&surviving_receiver));
    }

    #[test]
    fn requested_or_closed_detects_request_before_sender_close() {
        let (shutdown_tx, shutdown_rx) = watch::channel(true);
        drop(shutdown_tx);

        assert!(is_requested_or_closed(&shutdown_rx));
    }

    #[test]
    fn requested_or_closed_detects_sender_close_before_request() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        drop(shutdown_tx);

        assert!(is_requested_or_closed(&shutdown_rx));
    }

    #[tokio::test]
    async fn wait_for_request_or_timeout_returns_false_on_timeout_or_false_update() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        assert!(
            !wait_for_request_or_timeout(&mut shutdown_rx, Duration::from_millis(1)).await,
            "poll timeout should not be reported as shutdown"
        );

        shutdown_tx
            .send(false)
            .expect("receiver should remain active");
        assert!(
            !wait_for_request_or_timeout(&mut shutdown_rx, Duration::from_secs(1)).await,
            "non-shutdown watch updates should only wake the waiter"
        );
    }

    #[tokio::test]
    async fn wait_for_request_or_timeout_returns_true_on_request_or_closed_sender() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        shutdown_tx
            .send(true)
            .expect("receiver should remain active");
        assert!(wait_for_request_or_timeout(&mut shutdown_rx, Duration::from_secs(1)).await);

        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        drop(shutdown_tx);
        assert!(wait_for_request_or_timeout(&mut shutdown_rx, Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn wait_for_request_ignores_false_updates() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let waiter = tokio::spawn(async move {
            wait_for_request(&mut shutdown_rx).await;
        });

        shutdown_tx
            .send(false)
            .expect("receiver should remain active");
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());

        shutdown_tx
            .send(true)
            .expect("receiver should remain active");
        waiter.await.expect("request waiter must not panic");
    }
}
