use chrono::{DateTime, Utc};
use runledger_core::jobs::{JobEventType, JobStatus, JobType};
use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    JobDefinitionUpsert, JobEnqueue, JobQueueRecord, ReapedLeaseDisposition, claim_jobs_for_types,
    claim_prestart_jobs_for_types, enqueue_job, get_job_by_id, list_job_events,
    reap_expired_leases_with_diagnostics, upsert_job_definition_tx,
};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::{Value, json};
use sqlx::types::Uuid;

const TERMINAL_JOB_TYPE: &str = "jobs.test.lease_expiry_metadata.terminal";
const RETRY_JOB_TYPE: &str = "jobs.test.lease_expiry_metadata.retry";
const PRESTART_JOB_TYPE: &str = "jobs.test.lease_expiry_metadata.prestart";
const LEASE_EXPIRY_KIND: &str = "LEASE_EXPIRED";
const LEASE_EXPIRY_CODE: &str = "job.lease_expired";
const LEASE_EXPIRY_MESSAGE: &str = "Job lease expired before completion.";
const PRESTART_RELEASE_REASON: &str = "LEASE_EXPIRED_BEFORE_RUNNING_PERSISTED";
const RETRY_DELAY_MS: i32 = 1_234;

type AttemptFailureMetadata = (Option<String>, Option<String>, Option<String>, Option<i32>);

async fn record_postgres_server_version(pool: &DbPool) {
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(pool)
        .await
        .expect("read PostgreSQL server_version");
    let server_version_num =
        sqlx::query_scalar::<_, i32>("SELECT current_setting('server_version_num')::int")
            .fetch_one(pool)
            .await
            .expect("read PostgreSQL server_version_num");
    eprintln!(
        "lease-expiry metadata parity PostgreSQL server_version={server_version}, \
         server_version_num={server_version_num}"
    );
    assert_eq!(
        server_version_num / 10_000,
        18,
        "lease-expiry metadata parity must run on PostgreSQL 18"
    );
}

async fn register_job_definition(pool: &DbPool, job_type: JobType<'static>) {
    let mut tx = pool
        .begin()
        .await
        .expect("begin definition setup transaction");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type,
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit()
        .await
        .expect("commit definition setup transaction");
}

async fn enqueue_job_with_max_attempts(
    pool: &DbPool,
    job_type: JobType<'static>,
    max_attempts: i32,
) -> Uuid {
    let payload = json!({ "job_type": job_type.as_str() });
    enqueue_job(
        pool,
        &JobEnqueue {
            job_type,
            organization_id: None,
            payload: &payload,
            priority: None,
            max_attempts: Some(max_attempts),
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: None,
        },
    )
    .await
    .expect("enqueue job")
}

async fn claim_direct_job(
    pool: &DbPool,
    worker_id: &str,
    job_type: JobType<'static>,
) -> JobQueueRecord {
    claim_jobs_for_types(pool, worker_id, 30, 1, &[job_type])
        .await
        .expect("claim direct job")
        .pop()
        .expect("one direct job should be claimed")
}

async fn claim_prestart_job(
    pool: &DbPool,
    worker_id: &str,
    job_type: JobType<'static>,
) -> JobQueueRecord {
    claim_prestart_jobs_for_types(pool, worker_id, 30, 1, &[job_type])
        .await
        .expect("claim prestart job")
        .pop()
        .expect("one prestart job should be claimed")
}

async fn expire_lease(pool: &DbPool, job_id: Uuid) {
    let result = sqlx::query(
        "UPDATE job_queue
         SET lease_expires_at = now() - interval '10 seconds'
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(pool)
    .await
    .expect("expire lease");
    assert_eq!(result.rows_affected(), 1);
}

async fn load_job(pool: &DbPool, job_id: Uuid) -> JobQueueRecord {
    get_job_by_id(pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists")
}

async fn load_attempt_failure_metadata(
    pool: &DbPool,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
) -> AttemptFailureMetadata {
    sqlx::query_as(
        "SELECT outcome::text, error_code, error_message, retry_delay_ms
         FROM job_attempts
         WHERE job_id = $1
           AND run_number = $2
           AND attempt = $3",
    )
    .bind(job_id)
    .bind(run_number)
    .bind(attempt)
    .fetch_one(pool)
    .await
    .expect("load expired attempt failure metadata")
}

async fn event_payload(pool: &DbPool, job_id: Uuid, event_type: JobEventType) -> Value {
    list_job_events(pool, None, job_id, 20, None)
        .await
        .expect("list job events")
        .into_iter()
        .find(|event| event.event_type == event_type)
        .unwrap_or_else(|| panic!("{event_type:?} event should exist"))
        .payload
}

fn assert_failure_metadata_parity(
    queue: &JobQueueRecord,
    attempt: &AttemptFailureMetadata,
    failed_event_payload: &Value,
) {
    assert_eq!(
        failed_event_payload,
        &json!({
            "kind": LEASE_EXPIRY_KIND,
            "error_code": LEASE_EXPIRY_CODE,
            "error_message": LEASE_EXPIRY_MESSAGE,
            "started_without_renewal_heartbeat": false,
        })
    );
    assert_eq!(queue.status_reason.as_deref(), Some(LEASE_EXPIRY_KIND));
    assert_eq!(attempt.0.as_deref(), Some(LEASE_EXPIRY_KIND));
    assert_eq!(queue.last_error_code.as_deref(), Some(LEASE_EXPIRY_CODE));
    assert_eq!(attempt.1.as_deref(), Some(LEASE_EXPIRY_CODE));
    assert_eq!(
        queue.last_error_message.as_deref(),
        Some(LEASE_EXPIRY_MESSAGE)
    );
    assert_eq!(attempt.2.as_deref(), Some(LEASE_EXPIRY_MESSAGE));
}

#[tokio::test]
async fn lease_expiry_metadata_stays_in_parity_across_queue_attempt_and_audit_rows() {
    let (pool, database) = setup_ephemeral_pool("postgres_lease_expiry_metadata", 6).await;
    record_postgres_server_version(&pool).await;
    for job_type in [TERMINAL_JOB_TYPE, RETRY_JOB_TYPE, PRESTART_JOB_TYPE] {
        register_job_definition(&pool, JobType::new(job_type)).await;
    }

    let terminal_job_id =
        enqueue_job_with_max_attempts(&pool, JobType::new(TERMINAL_JOB_TYPE), 1).await;
    let retry_job_id = enqueue_job_with_max_attempts(&pool, JobType::new(RETRY_JOB_TYPE), 2).await;
    let prestart_job_id =
        enqueue_job_with_max_attempts(&pool, JobType::new(PRESTART_JOB_TYPE), 2).await;

    let terminal_claim = claim_direct_job(
        &pool,
        "worker-lease-expiry-terminal",
        JobType::new(TERMINAL_JOB_TYPE),
    )
    .await;
    let retry_claim = claim_direct_job(
        &pool,
        "worker-lease-expiry-retry",
        JobType::new(RETRY_JOB_TYPE),
    )
    .await;
    let prestart_claim = claim_prestart_job(
        &pool,
        "worker-lease-expiry-prestart",
        JobType::new(PRESTART_JOB_TYPE),
    )
    .await;
    expire_lease(&pool, terminal_job_id).await;
    expire_lease(&pool, retry_job_id).await;
    expire_lease(&pool, prestart_job_id).await;

    let result = reap_expired_leases_with_diagnostics(&pool, 3, RETRY_DELAY_MS)
        .await
        .expect("reap every expired lease transition");
    assert_eq!(result.summary.processed, 3);
    assert_eq!(result.reaped_leases.len(), 3);

    let terminal_disposition = &result
        .reaped_leases
        .iter()
        .find(|lease| lease.job_id == terminal_job_id)
        .expect("terminal lease should be reaped")
        .disposition;
    assert!(matches!(
        terminal_disposition,
        ReapedLeaseDisposition::DeadLetteredTerminal { .. }
    ));
    let retry_disposition = &result
        .reaped_leases
        .iter()
        .find(|lease| lease.job_id == retry_job_id)
        .expect("retryable lease should be reaped")
        .disposition;
    let ReapedLeaseDisposition::RetryScheduled {
        retry_delay_ms,
        next_run_at,
    } = retry_disposition
    else {
        panic!("retryable lease should schedule a retry");
    };
    assert_eq!(*retry_delay_ms, RETRY_DELAY_MS);
    let prestart_disposition = &result
        .reaped_leases
        .iter()
        .find(|lease| lease.job_id == prestart_job_id)
        .expect("unstarted prestart lease should be reaped")
        .disposition;
    assert!(matches!(
        prestart_disposition,
        ReapedLeaseDisposition::ReleasedToPending
    ));

    let terminal_queue = load_job(&pool, terminal_job_id).await;
    assert_eq!(terminal_queue.status, JobStatus::DeadLettered);
    let terminal_attempt = load_attempt_failure_metadata(
        &pool,
        terminal_job_id,
        terminal_claim.run_number,
        terminal_claim.attempt,
    )
    .await;
    assert_eq!(terminal_attempt.3, None);
    let terminal_failed_event = event_payload(&pool, terminal_job_id, JobEventType::Failed).await;
    assert_failure_metadata_parity(&terminal_queue, &terminal_attempt, &terminal_failed_event);
    assert_eq!(
        event_payload(&pool, terminal_job_id, JobEventType::DeadLettered).await,
        json!({
            "kind": LEASE_EXPIRY_KIND,
            "error_code": LEASE_EXPIRY_CODE,
            "started_without_renewal_heartbeat": false,
        })
    );
    let terminal_dead_letter = sqlx::query_as::<_, (String, String)>(
        "SELECT error_code, error_message
         FROM job_dead_letters
         WHERE job_id = $1",
    )
    .bind(terminal_job_id)
    .fetch_one(&pool)
    .await
    .expect("load terminal dead letter metadata");
    assert_eq!(
        terminal_dead_letter,
        (
            LEASE_EXPIRY_CODE.to_owned(),
            LEASE_EXPIRY_MESSAGE.to_owned()
        )
    );

    let retry_queue = load_job(&pool, retry_job_id).await;
    assert_eq!(retry_queue.status, JobStatus::Pending);
    assert_eq!(*next_run_at, retry_queue.next_run_at);
    let retry_attempt = load_attempt_failure_metadata(
        &pool,
        retry_job_id,
        retry_claim.run_number,
        retry_claim.attempt,
    )
    .await;
    assert_eq!(retry_attempt.3, Some(RETRY_DELAY_MS));
    let retry_failed_event = event_payload(&pool, retry_job_id, JobEventType::Failed).await;
    assert_failure_metadata_parity(&retry_queue, &retry_attempt, &retry_failed_event);
    let retry_scheduled_event =
        event_payload(&pool, retry_job_id, JobEventType::RetryScheduled).await;
    assert_eq!(
        retry_scheduled_event.get("kind").and_then(Value::as_str),
        Some(LEASE_EXPIRY_KIND)
    );
    assert_eq!(
        retry_scheduled_event
            .get("retry_delay_ms")
            .and_then(Value::as_i64),
        Some(i64::from(RETRY_DELAY_MS))
    );
    assert_eq!(
        retry_scheduled_event.get("started_without_renewal_heartbeat"),
        Some(&json!(false))
    );
    assert!(
        retry_scheduled_event.get("error_code").is_none()
            && retry_scheduled_event.get("error_message").is_none(),
        "retry-scheduled payload deliberately retains its historical kind-only failure metadata"
    );
    let retry_event_next_run_at = retry_scheduled_event
        .get("next_run_at")
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<DateTime<Utc>>().ok())
        .expect("retry-scheduled event should include a PostgreSQL timestamp");
    assert_eq!(retry_event_next_run_at, retry_queue.next_run_at);

    let prestart_queue = load_job(&pool, prestart_job_id).await;
    assert_eq!(prestart_queue.status, JobStatus::Pending);
    assert_eq!(prestart_queue.attempt, 0);
    assert!(prestart_queue.status_reason.is_none());
    assert!(prestart_queue.last_error_code.is_none());
    assert!(prestart_queue.last_error_message.is_none());
    let prestart_attempt_count = sqlx::query_scalar::<_, i64>(
        "SELECT count(*)
         FROM job_attempts
         WHERE job_id = $1
           AND run_number = $2
           AND attempt = $3",
    )
    .bind(prestart_job_id)
    .bind(prestart_claim.run_number)
    .bind(prestart_claim.attempt)
    .fetch_one(&pool)
    .await
    .expect("count released prestart attempts");
    assert_eq!(prestart_attempt_count, 0);
    assert_eq!(
        event_payload(&pool, prestart_job_id, JobEventType::Requeued).await,
        json!({
            "reason": PRESTART_RELEASE_REASON,
            "requeue_kind": "BASIC",
        })
    );

    teardown_ephemeral_pool(pool, database).await;
}
