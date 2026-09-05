use runledger_core::prelude::*;
use serde_json::{Value, json};
use std::time::Duration;

struct CheckpointedCounter;

#[async_trait]
impl JobExecutionHandler for CheckpointedCounter {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.example.counter")
    }

    async fn execute(
        &self,
        execution: JobExecution<'_>,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        let mut cursor = execution
            .checkpoint::<u64>()
            .map_err(|_| {
                JobFailure::terminal("counter.invalid_checkpoint", "Invalid counter checkpoint.")
            })?
            .unwrap_or(0);
        while cursor < 10 {
            if execution
                .remaining_work_budget(Duration::from_secs(1))
                .is_zero()
            {
                return Ok(JobCompletion::continue_now());
            }
            cursor += 1;
            let checkpoint = json!(cursor);
            execution
                .persist_progress(JobExecutionUpdate {
                    progress_done: Some(cursor as i64),
                    progress_total: Some(10),
                    checkpoint: Some(&checkpoint),
                })
                .await?;
        }
        Ok(JobCompletion::success())
    }
}

fn main() {
    let _catalog = runledger_runtime::catalog::JobCatalog::new()
        .handler(CheckpointedCounter.into_job_handler());
}
