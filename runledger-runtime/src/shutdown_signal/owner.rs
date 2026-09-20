//! The raw application future never belongs to an async polling frame.
//!
//! Like CallbackGuard, this owner catches a borrowed poll and separately takes
//! the future before destruction. Its policy differs: supervised signals resume
//! a normalized primary panic into their tracked task instead of reporting a
//! best-effort callback outcome.

use std::{
    any::Any,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use super::{RuntimeShutdownSignalError, ShutdownSignalTaskState, SignalTrigger};
use crate::shutdown::ShutdownSignal;

/// Unsubmitted futures can only be transferred or destroyed, never polled.
pub(super) struct SignalFuture<F> {
    future: Option<Pin<Box<F>>>,
}

/// Polling requires the complete stop-publication capability. Both completion
/// and panic commit a trigger here before application destruction can run.
pub(super) struct SupervisedSignalFuture<F, M> {
    raw: SignalFuture<F>,
    shutdown: ShutdownSignal,
    state: Arc<ShutdownSignalTaskState>,
    error: Option<M>,
}

impl<F> SignalFuture<F> {
    pub(super) fn new(future: F) -> Self {
        Self {
            future: Some(Box::pin(future)),
        }
    }

    pub(super) fn supervise<M>(
        self,
        shutdown: ShutdownSignal,
        state: Arc<ShutdownSignalTaskState>,
        error: M,
    ) -> SupervisedSignalFuture<F, M> {
        SupervisedSignalFuture {
            raw: self,
            shutdown,
            state,
            error: Some(error),
        }
    }

    fn destroy(&mut self) -> Option<String> {
        // Taking first makes repeated destruction impossible, including when
        // unwinding resumes and Rust subsequently drops this owner.
        let future = self.future.take();
        catch_unwind(AssertUnwindSafe(|| drop(future)))
            .err()
            .map(panic_message)
    }
}

impl<F, M> SupervisedSignalFuture<F, M> {
    fn destroy(&mut self) -> Option<String> {
        self.raw.destroy().inspect(|message| {
            self.shutdown.signal_destruction_panicked(message.clone());
        })
    }
}

// Only the separately pinned application future is structurally pinned.
impl<F, M> Unpin for SupervisedSignalFuture<F, M> {}

impl<F, M> Future for SupervisedSignalFuture<F, M>
where
    F: Future,
    M: FnOnce(F::Output) -> Option<RuntimeShutdownSignalError>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            this.raw
                .future
                .as_mut()
                .expect("completed signal is not polled again")
                .as_mut()
                .poll(cx)
        }));
        match outcome {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(result)) => {
                let error = this.error.take().expect("one signal produces one output");
                this.shutdown.request_after_signal_trigger(
                    &this.state,
                    SignalTrigger::Output(error(result)),
                );
                Poll::Ready(())
            }
            Err(payload) => {
                let message = panic_message(payload);
                this.shutdown.request_after_signal_trigger(
                    &this.state,
                    SignalTrigger::PollPanicked {
                        id: tokio::task::id(),
                        message: message.clone(),
                    },
                );
                // Poll unwinding has ended. A destructor can now panic without
                // causing a destructor-during-unwind process abort.
                let _ = this.destroy();
                resume_unwind(Box::new(message));
            }
        }
    }
}

impl<F, M> Drop for SupervisedSignalFuture<F, M> {
    fn drop(&mut self) {
        if let Some(message) = self.destroy() {
            if !std::thread::panicking() {
                // Preserve fatal join semantics on normal completion,
                // cancellation and never-polled task destruction.
                resume_unwind(Box::new(message));
            }
            // During an unrelated unwind, retain evidence without starting a
            // second unwind.
            tracing::warn!("shutdown signal destruction panicked");
        }
    }
}

impl<F> Drop for SignalFuture<F> {
    fn drop(&mut self) {
        if self.destroy().is_some() {
            // An unsubmitted value has no supervisor/report recipient.
            tracing::warn!("shutdown signal destruction panicked");
        }
    }
}

fn panic_message(payload: Box<dyn Any + Send>) -> String {
    // Only known string payloads have a destructor we can safely run. An opaque
    // panic_any payload may itself panic on Drop, even recursively. Do not run
    // that additional application code while containing a signal failure.
    // This intentionally retains the allocation for non-string panic payloads.
    let payload = match payload.downcast::<String>() {
        Ok(message) => return *message,
        Err(payload) => payload,
    };
    match payload.downcast::<&'static str>() {
        Ok(message) => (*message).to_owned(),
        Err(payload) => {
            std::mem::forget(payload);
            "non-string panic payload".to_owned()
        }
    }
}
