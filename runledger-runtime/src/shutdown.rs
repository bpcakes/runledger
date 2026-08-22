use std::time::Duration;

use tokio::sync::watch;
use tokio::time::sleep;

#[derive(Clone)]
pub(crate) struct ShutdownSignal {
    shutdown_tx: watch::Sender<bool>,
}

#[derive(Clone)]
pub(crate) struct ShutdownHandle {
    signal: ShutdownSignal,
}

impl ShutdownSignal {
    pub(crate) fn channel() -> (Self, watch::Receiver<bool>) {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        (Self { shutdown_tx }, shutdown_rx)
    }

    pub(crate) fn handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            signal: self.clone(),
        }
    }

    pub(crate) fn request(&self) {
        self.shutdown_tx.send_replace(true);
    }

    pub(crate) fn is_requested(&self) -> bool {
        *self.shutdown_tx.borrow()
    }
}

impl ShutdownHandle {
    pub(crate) fn request(&self) {
        self.signal.request();
    }

    pub(crate) fn is_requested(&self) -> bool {
        self.signal.is_requested()
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
    use std::sync::Arc;
    use std::time::Duration;

    use tokio::sync::{Barrier, watch};

    use super::*;

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
