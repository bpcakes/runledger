use runledger_core::jobs::{JobContext, JobDeadLetterInfo};
use runledger_postgres::jobs;
use tracing::warn;

use crate::dead_letter_hook::{
    DEAD_LETTER_HOOK_TIMEOUT, DeadLetterHookOutcome, invoke_dead_letter_hook,
};
use crate::registry::JobRegistry;

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

    match invoke_dead_letter_hook(handler.on_dead_letter(context, payload, dead_letter)).await {
        DeadLetterHookOutcome::Completed => {}
        DeadLetterHookOutcome::Panicked(panic_message) => {
            warn!(
                job_id = %job.id,
                job_type = %job.job_type,
                run_number = job.run_number,
                attempt = job.attempt,
                panic = %panic_message,
                "dead-letter hook panicked; continuing worker job task"
            );
        }
        DeadLetterHookOutcome::TimedOut => {
            warn!(
                job_id = %job.id,
                job_type = %job.job_type,
                run_number = job.run_number,
                attempt = job.attempt,
                timeout_ms = DEAD_LETTER_HOOK_TIMEOUT.as_millis(),
                "dead-letter hook timed out; continuing worker job task"
            );
        }
    }
}
