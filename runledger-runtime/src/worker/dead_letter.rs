use std::any::Any;
use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;
use runledger_core::jobs::{JobContext, JobDeadLetterInfo};
use runledger_postgres::jobs;
use tokio::time::Duration;
use tracing::warn;

use crate::registry::JobRegistry;

#[cfg(test)]
const TERMINAL_HOOK_TIMEOUT: Duration = Duration::from_millis(100);
#[cfg(not(test))]
const TERMINAL_HOOK_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) async fn notify_handler_of_dead_letter(
    registry: &JobRegistry,
    context: &JobContext,
    job: &jobs::JobQueueRecord,
    dead_letter: JobDeadLetterInfo,
) {
    let Some(handler) = registry.get(job.job_type.as_borrowed()) else {
        return;
    };
    let context = context.clone();
    let payload = job.payload.clone();

    match tokio::time::timeout(
        TERMINAL_HOOK_TIMEOUT,
        AssertUnwindSafe(handler.on_dead_letter(context, payload, dead_letter)).catch_unwind(),
    )
    .await
    {
        Ok(Ok(())) => {}
        Ok(Err(panic_payload)) => {
            let panic_message = panic_payload_message(&*panic_payload);
            warn!(
                job_id = %job.id,
                job_type = %job.job_type,
                run_number = job.run_number,
                attempt = job.attempt,
                panic = %panic_message,
                "dead-letter hook panicked; continuing worker job task"
            );
        }
        Err(_) => {
            warn!(
                job_id = %job.id,
                job_type = %job.job_type,
                run_number = job.run_number,
                attempt = job.attempt,
                timeout_ms = TERMINAL_HOOK_TIMEOUT.as_millis(),
                "dead-letter hook timed out; continuing worker job task"
            );
        }
    }
}

fn panic_payload_message(panic_payload: &(dyn Any + Send)) -> String {
    if let Some(message) = panic_payload.downcast_ref::<String>() {
        return message.clone();
    }

    if let Some(message) = panic_payload.downcast_ref::<&'static str>() {
        return (*message).to_string();
    }

    "non-string panic payload".to_string()
}
