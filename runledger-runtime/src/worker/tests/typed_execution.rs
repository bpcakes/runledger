use runledger_core::jobs::{
    JobContract, JobExecution, JobExecutionUpdate, JobSpec, TypedJobHandler,
};
use serde::{Deserialize, Serialize};

use super::*;

const JOB_TYPE: JobType<'static> = JobType::new("jobs.test.typed_execution");

#[derive(Deserialize, Serialize)]
struct ProgressPayload {
    done: Option<i64>,
    total: Option<i64>,
}

struct ProgressContract;

impl JobContract for ProgressContract {
    type Payload = ProgressPayload;

    fn spec() -> JobSpec {
        JobSpec::new(JOB_TYPE).expect("static spec")
    }
}

struct ProgressHandler {
    dead_payload: Arc<Mutex<Option<Value>>>,
}

#[async_trait::async_trait]
impl TypedJobHandler for ProgressHandler {
    type Contract = ProgressContract;

    async fn execute(
        &self,
        _: JobContext,
        _: ProgressPayload,
    ) -> Result<JobCompletion, JobFailure> {
        panic!("worker must dispatch typed execution with live services")
    }

    async fn execute_with_services(
        &self,
        execution: JobExecution<'_>,
        payload: ProgressPayload,
    ) -> Result<JobCompletion, JobFailure> {
        assert!(execution.remaining_budget() > Duration::ZERO);
        if execution.checkpoint::<u64>().expect("resume checkpoint") == Some(2) {
            return Ok(JobCompletion::success());
        }
        execution
            .persist_progress(JobExecutionUpdate {
                progress_done: Some(1),
                progress_total: Some(3),
                checkpoint: Some(&json!(1)),
            })
            .await?;
        execution
            .persist_progress(JobExecutionUpdate {
                progress_done: payload.done,
                progress_total: payload.total,
                checkpoint: Some(&json!(2)),
            })
            .await?;
        Ok(JobCompletion::continue_now())
    }

    async fn on_dead_letter(&self, _: JobContext, payload: Value, _: JobDeadLetterInfo) {
        *self.dead_payload.lock().expect("dead payload") = Some(payload);
    }
}

fn registry(dead_payload: Arc<Mutex<Option<Value>>>) -> Arc<JobRegistry> {
    let mut registry = JobRegistry::new();
    registry.register(ProgressHandler { dead_payload }.into_job_handler());
    Arc::new(registry)
}

#[tokio::test]
async fn invalid_partial_progress_is_terminal_and_preserves_the_last_commit() {
    let (pool, database) = setup_ephemeral_pool("typed_invalid_progress", 4).await;
    record_postgres_server_version(&pool, "typed partial progress validation").await;
    for payload in [json!({"done":4}), json!({"total":0})] {
        let (id, job) =
            enqueue_and_claim_job(&pool, JOB_TYPE, 3, payload.clone(), "typed-worker").await;
        let dead_payload = Arc::new(Mutex::new(None));
        process_claimed_job(pool.clone(), registry(dead_payload.clone()), job, 30).await;
        let saved = get_job_by_id(&pool, None, id)
            .await
            .expect("read")
            .expect("job");
        assert_eq!(
            saved.status,
            JobStatus::DeadLettered,
            "must not retry invalid progress"
        );
        assert_eq!(saved.attempt, 1);
        assert_eq!(
            saved.last_error_code.as_deref(),
            Some("job.invalid_progress")
        );
        assert_eq!(
            (saved.progress_done, saved.progress_total),
            (Some(1), Some(3))
        );
        assert_eq!(
            saved.checkpoint,
            Some(json!(1)),
            "rejected checkpoint is atomic with progress"
        );
        assert_eq!(*dead_payload.lock().expect("dead payload"), Some(payload));
        let events = list_job_events(&pool, None, id, 100, None)
            .await
            .expect("events");
        assert_eq!(
            events
                .iter()
                .filter(|event| event.event_type == JobEventType::Progress)
                .count(),
            1
        );
    }
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn typed_worker_persists_partial_progress_and_resumes_a_continuation() {
    let (pool, database) = setup_ephemeral_pool("typed_continuation", 4).await;
    let (id, job) =
        enqueue_and_claim_job(&pool, JOB_TYPE, 3, json!({"done":2}), "typed-worker").await;
    let registry = registry(Arc::new(Mutex::new(None)));
    process_claimed_job(pool.clone(), registry.clone(), job, 30).await;
    let next = claim_prestart_jobs(&pool, "typed-next", 30, 1)
        .await
        .expect("claim")
        .pop()
        .expect("continuation");
    assert_eq!(next.run_number, 2);
    assert_eq!(
        (next.progress_done, next.progress_total),
        (Some(2), Some(3))
    );
    assert_eq!(next.checkpoint, Some(json!(2)));
    process_claimed_job(pool.clone(), registry, next, 30).await;
    assert_eq!(
        get_job_by_id(&pool, None, id)
            .await
            .expect("read")
            .expect("job")
            .status,
        JobStatus::Succeeded
    );
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn typed_worker_rejects_malformed_payload_before_progress_and_passes_raw_json_to_cleanup() {
    let (pool, database) = setup_ephemeral_pool("typed_malformed", 4).await;
    let payload = json!({"done":"private-input"});
    let (id, job) =
        enqueue_and_claim_job(&pool, JOB_TYPE, 3, payload.clone(), "typed-worker").await;
    let dead_payload = Arc::new(Mutex::new(None));
    process_claimed_job(pool.clone(), registry(dead_payload.clone()), job, 30).await;
    let saved = get_job_by_id(&pool, None, id)
        .await
        .expect("read")
        .expect("job");
    assert_eq!(saved.status, JobStatus::DeadLettered);
    assert_eq!(
        saved.last_error_code.as_deref(),
        Some("job.invalid_payload")
    );
    assert_eq!(
        saved.last_error_message.as_deref(),
        Some("Job payload has an invalid shape.")
    );
    assert_eq!(saved.checkpoint, None);
    assert_eq!(*dead_payload.lock().expect("dead payload"), Some(payload));
    teardown_ephemeral_pool(pool, database).await;
}
