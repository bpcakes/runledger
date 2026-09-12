use std::future::Future;

use tokio::time::Duration;

use crate::panic_payload::panic_payload_message;

#[cfg(test)]
pub(crate) const DEAD_LETTER_HOOK_TIMEOUT: Duration = Duration::from_millis(100);
#[cfg(not(test))]
pub(crate) const DEAD_LETTER_HOOK_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DeadLetterHookOutcome {
    Completed,
    TimedOut,
    Panicked(String),
}

pub(crate) async fn invoke_dead_letter_hook<F>(
    hook: F,
    callback: &'static str,
    settlement: Option<crate::settlement::TaskRegistry>,
) -> DeadLetterHookOutcome
where
    F: Future<Output = ()>,
{
    match tokio::time::timeout(
        DEAD_LETTER_HOOK_TIMEOUT,
        crate::callback::CallbackGuard::new(hook, callback, settlement),
    )
    .await
    {
        Ok(Ok(())) => DeadLetterHookOutcome::Completed,
        Ok(Err(panic_payload)) => {
            DeadLetterHookOutcome::Panicked(panic_payload_message(&*panic_payload))
        }
        Err(_) => DeadLetterHookOutcome::TimedOut,
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use super::*;

    #[tokio::test]
    async fn hook_policy_reports_completion() {
        assert_eq!(
            invoke_dead_letter_hook(async {}, "test_hook", None).await,
            DeadLetterHookOutcome::Completed
        );
    }

    #[tokio::test]
    async fn hook_policy_normalizes_panic() {
        assert_eq!(
            invoke_dead_letter_hook(
                async {
                    panic!("dead-letter hook panic");
                },
                "test_hook",
                None
            )
            .await,
            DeadLetterHookOutcome::Panicked("dead-letter hook panic".to_owned())
        );
    }

    #[tokio::test]
    async fn hook_policy_bounds_execution_time() {
        assert_eq!(
            invoke_dead_letter_hook(pending(), "test_hook", None).await,
            DeadLetterHookOutcome::TimedOut
        );
    }
}

#[cfg(test)]
mod destruction_tests {
    use super::*;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };

    struct FailingFuture {
        panic_on_poll: bool,
    }
    impl Future for FailingFuture {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            if self.panic_on_poll {
                panic!("hook poll failure");
            }
            Poll::Pending
        }
    }
    impl Drop for FailingFuture {
        fn drop(&mut self) {
            panic!("hook destruction failure");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn hook_poll_and_destruction_failures_are_both_retained() {
        let (shutdown, _) = crate::shutdown::ShutdownSignal::channel();
        let registry = crate::settlement::TaskRegistry::supervised(shutdown.clone());
        shutdown.request();
        let outcome = invoke_dead_letter_hook(
            FailingFuture {
                panic_on_poll: true,
            },
            "test_hook",
            Some(registry.clone()),
        )
        .await;
        registry.record_hook("test_hook", &outcome);
        assert_eq!(
            outcome,
            DeadLetterHookOutcome::Panicked("hook poll failure".into())
        );
        let (failures, _) = registry.callback_snapshot();
        assert_eq!(failures.len(), 2);
        for expected in ["hook poll failure", "hook destruction failure"] {
            assert!(failures.iter().any(|failure| matches!(failure,
                crate::RuntimeCallbackFailure::Panicked { message, .. } if message == expected)));
            assert!(!format!("{failures:?}").contains(expected));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn hook_timeout_and_destruction_failures_are_both_retained() {
        let (shutdown, _) = crate::shutdown::ShutdownSignal::channel();
        let registry = crate::settlement::TaskRegistry::supervised(shutdown.clone());
        shutdown.request();
        let outcome = invoke_dead_letter_hook(
            FailingFuture {
                panic_on_poll: false,
            },
            "test_hook",
            Some(registry.clone()),
        )
        .await;
        registry.record_hook("test_hook", &outcome);
        assert_eq!(outcome, DeadLetterHookOutcome::TimedOut);
        let (failures, _) = registry.callback_snapshot();
        assert_eq!(failures.len(), 2);
        assert!(
            failures
                .iter()
                .any(|failure| matches!(failure, crate::RuntimeCallbackFailure::TimedOut { .. }))
        );
        assert!(failures.iter().any(|failure| matches!(failure, crate::RuntimeCallbackFailure::Panicked { message, .. } if message == "hook destruction failure")));
    }
}
