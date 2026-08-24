use std::future::Future;
use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;
use tokio::time::Duration;

#[cfg(test)]
pub(crate) const DEAD_LETTER_HOOK_TIMEOUT: Duration = Duration::from_millis(100);
#[cfg(not(test))]
pub(crate) const DEAD_LETTER_HOOK_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum DeadLetterHookOutcome {
    Completed,
    TimedOut,
    Panicked(String),
}

pub(crate) async fn invoke_dead_letter_hook<F>(hook: F) -> DeadLetterHookOutcome
where
    F: Future<Output = ()>,
{
    match tokio::time::timeout(
        DEAD_LETTER_HOOK_TIMEOUT,
        AssertUnwindSafe(hook).catch_unwind(),
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

fn panic_payload_message(panic_payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(message) = panic_payload.downcast_ref::<String>() {
        return message.clone();
    }

    if let Some(message) = panic_payload.downcast_ref::<&'static str>() {
        return (*message).to_string();
    }

    "non-string panic payload".to_string()
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use super::*;

    #[tokio::test]
    async fn hook_policy_reports_completion() {
        assert_eq!(
            invoke_dead_letter_hook(async {}).await,
            DeadLetterHookOutcome::Completed
        );
    }

    #[tokio::test]
    async fn hook_policy_normalizes_panic() {
        assert_eq!(
            invoke_dead_letter_hook(async {
                panic!("dead-letter hook panic");
            })
            .await,
            DeadLetterHookOutcome::Panicked("dead-letter hook panic".to_owned())
        );
    }

    #[tokio::test]
    async fn hook_policy_bounds_execution_time() {
        assert_eq!(
            invoke_dead_letter_hook(pending()).await,
            DeadLetterHookOutcome::TimedOut
        );
    }
}
