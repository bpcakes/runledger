//! Best-effort callback polling and synchronous future-destruction boundary.
//!
//! Catching only polls misses destruction by timeout or task abortion. This
//! owner contains both. It does not finalize resources or join application tasks;
//! retained interruption evidence continues to prohibit cooperative cleanup.

use std::{
    any::Any,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    task::{Context, Poll},
};

use crate::{
    RuntimeCallbackFailure, panic_payload::panic_payload_message, settlement::TaskRegistry,
};

type Panic = Box<dyn Any + Send + 'static>;

pub(crate) struct CallbackGuard<F: Future<Output = ()>> {
    future: Option<Pin<Box<F>>>,
    callback: &'static str,
    registry: Option<TaskRegistry>,
}

impl<F: Future<Output = ()>> CallbackGuard<F> {
    pub(crate) fn new(future: F, callback: &'static str, registry: Option<TaskRegistry>) -> Self {
        Self {
            future: Some(Box::pin(future)),
            callback,
            registry,
        }
    }

    fn destroy(&mut self) -> Result<(), Panic> {
        let future = self.future.take();
        catch_unwind(AssertUnwindSafe(|| drop(future)))
    }

    fn record_destruction_panic(&self, panic: &Panic) {
        if let Some(registry) = &self.registry {
            registry.record_callback(RuntimeCallbackFailure::Panicked {
                callback: self.callback,
                message: panic_payload_message(&**panic),
            });
        }
        tracing::warn!(
            callback_name = self.callback,
            "best-effort callback destruction panicked"
        );
    }
}

impl<F: Future<Output = ()>> Future for CallbackGuard<F> {
    type Output = Result<(), Panic>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let outcome = catch_unwind(AssertUnwindSafe(|| {
            this.future
                .as_mut()
                .expect("completed callback is not polled again")
                .as_mut()
                .poll(cx)
        }));
        match outcome {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(())) => Poll::Ready(this.destroy()),
            Err(primary) => {
                if let Err(secondary) = this.destroy() {
                    this.record_destruction_panic(&secondary);
                }
                Poll::Ready(Err(primary))
            }
        }
    }
}

impl<F: Future<Output = ()>> Drop for CallbackGuard<F> {
    fn drop(&mut self) {
        if let Err(panic) = self.destroy() {
            self.record_destruction_panic(&panic);
        }
    }
}
