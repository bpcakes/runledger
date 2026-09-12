//! Observation of local runtime-loop initialization, separate from database health.

use tokio::sync::watch;

/// Current initialization state of one supervisor instance.
///
/// Initialization establishes local loop state, not database connectivity, handler
/// success or durable execution. A stopped instance never becomes initialized again.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RuntimeStartup {
    /// These enabled loops have not yet acknowledged their local initialization.
    Starting { pending: Vec<&'static str> },
    /// Every enabled loop acknowledged; no stop or loop exit has been observed.
    Initialized,
    /// Shutdown was requested or an enabled loop exited, including by cancellation.
    Stopped,
}

/// Startup cannot complete because shutdown was requested or a runtime loop exited.
/// The supervisor's termination result owns the underlying cause.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("runtime startup stopped")]
pub struct RuntimeStartupStopped;

/// Cloneable startup observation. Dropping or cancelling a waiter changes no ownership.
#[derive(Clone, Debug)]
pub struct RuntimeStartupObserver {
    state: watch::Receiver<RuntimeStartup>,
}

impl RuntimeStartupObserver {
    /// Read the current state. A later stop can immediately obsolete this snapshot.
    pub fn snapshot(&self) -> RuntimeStartup {
        self.state.borrow().clone()
    }

    /// Wait for initialized or stopped state. Cancel and resume without losing state.
    ///
    /// A successful observation is not permanent readiness; applications must also
    /// observe runtime termination and apply their own dependency and business policy.
    pub async fn wait_initialized(&self) -> Result<(), RuntimeStartupStopped> {
        let mut state = self.state.clone();
        loop {
            let current = state.borrow_and_update().clone();
            match current {
                RuntimeStartup::Initialized => return Ok(()),
                RuntimeStartup::Stopped => return Err(RuntimeStartupStopped),
                RuntimeStartup::Starting { .. } => {}
            }
            if state.changed().await.is_err() {
                return Err(RuntimeStartupStopped);
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct Initialization {
    state: watch::Sender<RuntimeStartup>,
}

impl Initialization {
    pub(crate) fn new(pending: Vec<&'static str>) -> Self {
        let initial = if pending.is_empty() {
            RuntimeStartup::Initialized
        } else {
            RuntimeStartup::Starting { pending }
        };
        Self {
            state: watch::channel(initial).0,
        }
    }

    pub(crate) fn observer(&self) -> RuntimeStartupObserver {
        RuntimeStartupObserver {
            state: self.state.subscribe(),
        }
    }

    pub(crate) fn loop_token(&self, name: &'static str) -> LoopStartup {
        LoopStartup {
            initialization: self.clone(),
            name: Some(name),
        }
    }

    pub(crate) fn stop(&self) {
        self.state.send_replace(RuntimeStartup::Stopped);
    }
}

/// Constructed before spawn so even a never-polled loop records its destruction.
pub(crate) struct LoopStartup {
    initialization: Initialization,
    name: Option<&'static str>,
}

pub(crate) fn acknowledge(startup: &mut Option<LoopStartup>) {
    if let Some(startup) = startup {
        startup.acknowledge();
    }
}

impl LoopStartup {
    fn acknowledge(&mut self) {
        let Some(name) = self.name.take() else {
            return;
        };
        // Acknowledgement and stopping serialize on the same watch state. No
        // read/check/write gap can publish Initialized after a preceding stop.
        self.initialization.state.send_if_modified(|state| {
            let RuntimeStartup::Starting { pending } = state else {
                return false;
            };
            pending.retain(|entry| *entry != name);
            if pending.is_empty() {
                *state = RuntimeStartup::Initialized;
            }
            true
        });
    }
}

impl Drop for LoopStartup {
    fn drop(&mut self) {
        self.initialization.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::{Initialization, RuntimeStartup, RuntimeStartupStopped};

    mod supervisor;

    #[tokio::test]
    async fn every_enabled_loop_must_acknowledge() {
        let initialization = Initialization::new(vec!["worker", "scheduler"]);
        let observer = initialization.observer();
        let mut worker = initialization.loop_token("worker");
        let mut scheduler = initialization.loop_token("scheduler");
        worker.acknowledge();
        worker.acknowledge();
        assert_eq!(
            observer.snapshot(),
            RuntimeStartup::Starting {
                pending: vec!["scheduler"]
            }
        );
        scheduler.acknowledge();
        assert_eq!(observer.wait_initialized().await, Ok(()));
        drop(worker);
        assert_eq!(
            observer.wait_initialized().await,
            Err(RuntimeStartupStopped)
        );
    }

    #[tokio::test]
    async fn shutdown_and_unpolled_destruction_prevent_late_acknowledgement() {
        for explicit_stop in [false, true] {
            let initialization = Initialization::new(vec!["worker", "scheduler"]);
            let observer = initialization.observer();
            let worker = initialization.loop_token("worker");
            let mut scheduler = initialization.loop_token("scheduler");
            if explicit_stop {
                initialization.stop();
            }
            drop(worker);
            scheduler.acknowledge();
            assert_eq!(
                observer.wait_initialized().await,
                Err(RuntimeStartupStopped)
            );
        }
    }

    #[test]
    fn instances_are_independent_and_empty_selection_is_initialized() {
        let first = Initialization::new(Vec::new());
        let second = Initialization::new(Vec::new());
        first.stop();
        assert_eq!(first.observer().snapshot(), RuntimeStartup::Stopped);
        assert_eq!(second.observer().snapshot(), RuntimeStartup::Initialized);
    }
}
