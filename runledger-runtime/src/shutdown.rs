use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::watch;
use tokio::time::sleep;

#[derive(Clone)]
pub(crate) struct ShutdownSignal {
    shutdown_tx: watch::Sender<bool>,
    shutdown_requested: Arc<AtomicBool>,
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
                shutdown_requested: Arc::new(AtomicBool::new(false)),
            },
            shutdown_rx,
        )
    }

    pub(crate) fn handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            signal: self.clone(),
        }
    }

    pub(crate) fn request(&self) {
        if !self.shutdown_requested.swap(true, Ordering::SeqCst) {
            // Mark the observable flag before notifying receivers so status
            // readers agree that shutdown has begun as soon as this returns.
            // Safe to ignore: send failure means every receiver was dropped.
            let _ = self.shutdown_tx.send(true);
        }
    }

    pub(crate) fn is_requested(&self) -> bool {
        self.shutdown_requested.load(Ordering::SeqCst)
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
    use std::time::Duration;

    use tokio::sync::watch;

    use super::*;

    #[test]
    fn request_sets_observable_flag() {
        let (shutdown, mut receiver) = ShutdownSignal::channel();

        shutdown.request();

        assert!(shutdown.is_requested());
        assert!(receiver.has_changed().expect("receiver should see request"));
        assert!(*receiver.borrow_and_update());
    }

    #[test]
    fn requested_or_closed_detects_true_value_and_closed_sender() {
        let (shutdown_tx, shutdown_rx) = watch::channel(true);
        assert!(is_requested_or_closed(&shutdown_rx));

        let (shutdown_tx_closed, shutdown_rx_closed) = watch::channel(false);
        drop(shutdown_tx_closed);
        assert!(is_requested_or_closed(&shutdown_rx_closed));

        drop(shutdown_tx);
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
