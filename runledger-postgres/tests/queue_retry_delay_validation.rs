use std::collections::BTreeSet;
use std::time::Duration;

use chrono::{DateTime, Timelike, Utc};
use runledger_core::jobs::{
    JobDeadLetterReason, JobEventType, JobFailureKind, JobRetryTiming, JobStatus, JobType,
};
use runledger_postgres::jobs::{
    JobDefinitionUpsert, JobEnqueue, JobFailureCompletionDisposition, JobFailureUpdate,
    JobQueueRecord, claim_jobs, complete_job_failure, complete_job_failure_with_outcome,
    enqueue_job, get_job_by_id, list_job_events, reap_expired_leases,
    reap_expired_leases_with_terminal_records, upsert_job_definition_tx,
};
use runledger_postgres::{DbPool, Error, QueryErrorCategory, QueryErrorKind};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::{Value, json};
use sqlx::{Row, types::Uuid};

const JOB_TYPE: &str = "jobs.test.retry_delay_validation";

async fn register_job_definition(pool: &DbPool) {
    let mut tx = pool.begin().await.expect("begin setup tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(JOB_TYPE),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit setup tx");
}

async fn enqueue_test_job_with_max_attempts(
    pool: &DbPool,
    case_name: &str,
    max_attempts: Option<i32>,
) -> Uuid {
    let payload = json!({ "case": case_name });
    enqueue_job(
        pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: None,
            payload: &payload,
            priority: None,
            max_attempts,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: None,
        },
    )
    .await
    .expect("enqueue test job")
}

async fn enqueue_test_job(pool: &DbPool, case_name: &str) -> Uuid {
    enqueue_test_job_with_max_attempts(pool, case_name, None).await
}

async fn load_job(pool: &DbPool, job_id: Uuid) -> JobQueueRecord {
    get_job_by_id(pool, None, job_id)
        .await
        .expect("load job")
        .expect("job exists")
}

async fn assert_job_unchanged(pool: &DbPool, job_id: Uuid, before: &JobQueueRecord) {
    let after = load_job(pool, job_id).await;
    assert_eq!(after.status, before.status);
    assert_eq!(after.attempt, before.attempt);
    assert_eq!(after.next_run_at, before.next_run_at);
    assert_eq!(after.worker_id, before.worker_id);
    assert_eq!(after.lease_expires_at, before.lease_expires_at);
    assert_eq!(after.last_heartbeat_at, before.last_heartbeat_at);
    assert_eq!(after.started_at, before.started_at);
    assert_eq!(after.finished_at, before.finished_at);
    assert_eq!(after.status_reason, before.status_reason);
    assert_eq!(after.last_error_code, before.last_error_code);
    assert_eq!(after.last_error_message, before.last_error_message);
    assert_eq!(after.updated_at, before.updated_at);
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct JobAttemptMutationSnapshot {
    finished_at: Option<DateTime<Utc>>,
    outcome: Option<String>,
    error_code: Option<String>,
    error_message: Option<String>,
    retry_delay_ms: Option<i32>,
    requested_retry_not_before: Option<DateTime<Utc>>,
    effective_next_run_at: Option<DateTime<Utc>>,
    retry_timing_source: Option<String>,
}

async fn load_attempt_mutation_snapshot(
    pool: &DbPool,
    job_id: Uuid,
    run_number: i32,
    attempt: i32,
) -> JobAttemptMutationSnapshot {
    let row = sqlx::query(
        "SELECT
            finished_at,
            outcome::text AS outcome,
            error_code,
            error_message,
            retry_delay_ms,
            requested_retry_not_before,
            effective_next_run_at,
            retry_timing_source
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
    .expect("load job attempt mutation snapshot");

    JobAttemptMutationSnapshot {
        finished_at: row.try_get("finished_at").expect("finished_at column"),
        outcome: row.try_get("outcome").expect("outcome column"),
        error_code: row.try_get("error_code").expect("error_code column"),
        error_message: row.try_get("error_message").expect("error_message column"),
        retry_delay_ms: row
            .try_get("retry_delay_ms")
            .expect("retry_delay_ms column"),
        requested_retry_not_before: row
            .try_get("requested_retry_not_before")
            .expect("requested_retry_not_before column"),
        effective_next_run_at: row
            .try_get("effective_next_run_at")
            .expect("effective_next_run_at column"),
        retry_timing_source: row
            .try_get("retry_timing_source")
            .expect("retry_timing_source column"),
    }
}

async fn assert_event_types(pool: &DbPool, job_id: Uuid, expected: &[JobEventType]) {
    let actual = list_job_events(pool, None, job_id, 10, None)
        .await
        .expect("list job events")
        .into_iter()
        .map(|event| event.event_type)
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

fn assert_event_payload_keys(payload: &Value, expected: &[&str]) {
    let actual = payload
        .as_object()
        .expect("event payload should be an object")
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(actual, expected);
}

fn event_timestamp(payload: &Value, key: &str) -> DateTime<Utc> {
    payload
        .get(key)
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<DateTime<Utc>>().ok())
        .unwrap_or_else(|| panic!("event {key} should be an RFC 3339 timestamp"))
}

async fn record_postgres_server_version(pool: &DbPool) {
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(pool)
        .await
        .expect("load PostgreSQL server version");
    eprintln!("retry timing regression PostgreSQL server_version={server_version}");
}

fn assert_invalid_retry_delay_error(error: Error) {
    match error {
        Error::QueryError(query_error) => {
            assert_eq!(query_error.category(), QueryErrorCategory::Validation);
            assert_eq!(query_error.code(), "job.invalid_retry_delay");
            assert_eq!(
                query_error.client_message(),
                "Job retry delay must be positive."
            );
        }
        other => panic!("expected validation query error, got {other:?}"),
    }
}

fn assert_invalid_retry_timing_error(error: Error) {
    match error {
        Error::QueryError(query_error) => {
            assert_eq!(query_error.category(), QueryErrorCategory::Validation);
            assert_eq!(
                query_error.kind(),
                Some(QueryErrorKind::JobInvalidRetryTiming)
            );
            assert_eq!(query_error.code(), "job.invalid_retry_timing");
            assert_eq!(query_error.client_message(), "Job retry timing is invalid.");
        }
        other => panic!("expected retry timing validation error, got {other:?}"),
    }
}

async fn claim_one_job(pool: &DbPool, worker_id: &str) -> JobQueueRecord {
    claim_jobs(pool, worker_id, 30, 1)
        .await
        .expect("claim job")
        .pop()
        .expect("job should be claimed")
}

async fn expire_job_lease(pool: &DbPool, job_id: Uuid) {
    sqlx::query(
        "UPDATE job_queue SET lease_expires_at = now() - interval '10 seconds' WHERE id = $1",
    )
    .bind(job_id)
    .execute(pool)
    .await
    .expect("expire job lease");
}

#[tokio::test]
async fn retryable_failure_rejects_invalid_retry_timing_without_mutating_lease() {
    let (pool, database) = setup_ephemeral_pool("postgres_retry_delay_failure", 4).await;
    register_job_definition(&pool).await;
    let job_id = enqueue_test_job(&pool, "failure_invalid_retry_delay").await;
    let job = claim_one_job(&pool, "worker-retry-delay-failure").await;
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");

    let before = load_job(&pool, job_id).await;
    let attempt_before =
        load_attempt_mutation_snapshot(&pool, job.id, job.run_number, job.attempt).await;
    assert_eq!(before.status, JobStatus::Leased);
    assert_eq!(before.attempt, 1);
    assert_eq!(before.worker_id.as_deref(), Some(worker_id.as_str()));
    assert_eq!(
        attempt_before,
        JobAttemptMutationSnapshot {
            finished_at: None,
            outcome: None,
            error_code: None,
            error_message: None,
            retry_delay_ms: None,
            requested_retry_not_before: None,
            effective_next_run_at: None,
            retry_timing_source: None,
        }
    );
    assert_event_types(
        &pool,
        job_id,
        &[JobEventType::Enqueued, JobEventType::Leased],
    )
    .await;

    let too_large_delay = Duration::from_millis(i32::MAX as u64 + 1);
    for (retry_timing, policy_retry_delay_ms) in [
        (None, None),
        (None, Some(0)),
        (Some(JobRetryTiming::After(too_large_delay)), Some(1_000)),
    ] {
        assert_invalid_retry_timing_error(
            complete_job_failure(
                &pool,
                job.id,
                job.run_number,
                job.attempt,
                &worker_id,
                &match retry_timing {
                    Some(retry_timing) => JobFailureUpdate::new(
                        JobFailureKind::Retryable,
                        "job.test.retry_timing_invalid",
                        "retryable failure timing should be rejected",
                        policy_retry_delay_ms,
                    )
                    .with_retry_timing(retry_timing),
                    None => JobFailureUpdate::new(
                        JobFailureKind::Retryable,
                        "job.test.retry_timing_invalid",
                        "retryable failure timing should be rejected",
                        policy_retry_delay_ms,
                    ),
                },
            )
            .await
            .expect_err("invalid retry timing should be rejected"),
        );
        assert_job_unchanged(&pool, job_id, &before).await;
        assert_eq!(
            load_attempt_mutation_snapshot(&pool, job.id, job.run_number, job.attempt).await,
            attempt_before
        );
        assert_event_types(
            &pool,
            job_id,
            &[JobEventType::Enqueued, JobEventType::Leased],
        )
        .await;
    }

    complete_job_failure(
        &pool,
        job.id,
        job.run_number,
        job.attempt,
        &worker_id,
        &JobFailureUpdate::new(
            JobFailureKind::Terminal,
            "job.test.terminal_without_retry_delay",
            "terminal failure does not need retry delay",
            None,
        ),
    )
    .await
    .expect("terminal failure should allow absent retry delay");

    let terminal = load_job(&pool, job_id).await;
    assert_eq!(terminal.status, JobStatus::DeadLettered);
    assert_event_types(
        &pool,
        job_id,
        &[
            JobEventType::Enqueued,
            JobEventType::Leased,
            JobEventType::Failed,
            JobEventType::DeadLettered,
        ],
    )
    .await;

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn terminal_panicked_and_exhausted_failures_ignore_invalid_retry_timing() {
    let (pool, database) = setup_ephemeral_pool("postgres_retry_timing_ignored", 4).await;
    record_postgres_server_version(&pool).await;
    register_job_definition(&pool).await;

    let cases = [
        (
            "terminal_invalid_timing",
            JobFailureKind::Terminal,
            3,
            "job.test.terminal_original",
            JobDeadLetterReason::FailureKindNonRetryable,
        ),
        (
            "panicked_invalid_timing",
            JobFailureKind::Panicked,
            3,
            "job.test.panicked_original",
            JobDeadLetterReason::FailureKindNonRetryable,
        ),
        (
            "exhausted_invalid_timing",
            JobFailureKind::Retryable,
            1,
            "job.test.retryable_original",
            JobDeadLetterReason::AttemptsExhausted,
        ),
    ];

    for (case_name, kind, max_attempts, failure_code, expected_reason) in cases {
        let job_id = enqueue_test_job_with_max_attempts(&pool, case_name, Some(max_attempts)).await;
        let worker_id = format!("worker-{case_name}");
        let job = claim_one_job(&pool, &worker_id).await;
        assert_eq!(job.id, job_id);

        let outcome = complete_job_failure_with_outcome(
            &pool,
            job.id,
            job.run_number,
            job.attempt,
            &worker_id,
            &JobFailureUpdate::new(
                kind,
                failure_code,
                "original failure should survive ignored invalid timing",
                None,
            )
            .with_retry_timing(JobRetryTiming::After(Duration::ZERO)),
        )
        .await
        .expect("terminal or exhausted failure should bypass retry timing validation");

        assert_eq!(outcome.failure_kind, kind);
        assert_eq!(outcome.failure_code, failure_code);
        assert_eq!(outcome.max_attempts, max_attempts);
        assert_eq!(
            outcome.disposition,
            JobFailureCompletionDisposition::DeadLettered {
                reason: expected_reason,
            }
        );

        let persisted = load_job(&pool, job_id).await;
        assert_eq!(persisted.status, JobStatus::DeadLettered);
        assert_eq!(persisted.status_reason.as_deref(), Some(kind.as_db_value()));
        assert_eq!(persisted.last_error_code.as_deref(), Some(failure_code));
        assert!(persisted.worker_id.is_none());
        assert!(persisted.lease_expires_at.is_none());

        let attempt =
            load_attempt_mutation_snapshot(&pool, job.id, job.run_number, job.attempt).await;
        assert!(attempt.finished_at.is_some());
        assert_eq!(attempt.outcome.as_deref(), Some(kind.as_db_value()));
        assert_eq!(attempt.error_code.as_deref(), Some(failure_code));
        assert_eq!(
            attempt.error_message.as_deref(),
            Some("original failure should survive ignored invalid timing")
        );
        assert_eq!(attempt.retry_delay_ms, None);

        assert_event_types(
            &pool,
            job_id,
            &[
                JobEventType::Enqueued,
                JobEventType::Leased,
                JobEventType::Failed,
                JobEventType::DeadLettered,
            ],
        )
        .await;
    }

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn shorter_handler_retry_hint_is_audited_while_policy_backoff_wins() {
    let (pool, database) = setup_ephemeral_pool("postgres_retry_timing_relative", 4).await;
    register_job_definition(&pool).await;
    let job_id = enqueue_test_job(&pool, "relative_retry_timing").await;
    let job = claim_one_job(&pool, "worker-relative-retry-timing").await;
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");

    let outcome = complete_job_failure_with_outcome(
        &pool,
        job.id,
        job.run_number,
        job.attempt,
        &worker_id,
        &JobFailureUpdate::new(
            JobFailureKind::Retryable,
            "job.test.relative_retry_timing",
            "retry after a positive sub-millisecond delay",
            Some(1_000),
        )
        .with_retry_timing(JobRetryTiming::After(Duration::from_nanos(1))),
    )
    .await
    .expect("persist relative retry timing");

    let JobFailureCompletionDisposition::RetryScheduled {
        retry_delay_ms,
        next_run_at,
    } = outcome.disposition
    else {
        panic!("expected relative retry disposition");
    };
    assert_eq!(retry_delay_ms, 1_000);
    assert_eq!(load_job(&pool, job_id).await.next_run_at, next_run_at);

    let attempt = load_attempt_mutation_snapshot(&pool, job.id, job.run_number, job.attempt).await;
    assert_eq!(attempt.retry_delay_ms, Some(1_000));
    assert!(attempt.requested_retry_not_before.is_some());
    assert_eq!(attempt.effective_next_run_at, Some(next_run_at));
    assert_eq!(attempt.retry_timing_source.as_deref(), Some("POLICY"));
    assert!(attempt.requested_retry_not_before < attempt.effective_next_run_at);

    let event = list_job_events(&pool, None, job_id, 10, None)
        .await
        .expect("list retry events")
        .into_iter()
        .find(|event| event.event_type == JobEventType::RetryScheduled)
        .expect("retry event exists");
    assert_event_payload_keys(
        &event.payload,
        &[
            "effective_next_run_at",
            "next_run_at",
            "requested_retry_at",
            "requested_retry_not_before",
            "retry_delay_ms",
            "retry_timing_source",
        ],
    );
    assert_eq!(
        event.payload.get("retry_delay_ms").and_then(Value::as_i64),
        Some(1_000)
    );
    assert_eq!(event_timestamp(&event.payload, "next_run_at"), next_run_at);
    assert_eq!(
        event
            .payload
            .get("retry_timing_source")
            .and_then(Value::as_str),
        Some("POLICY")
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn absolute_retry_timing_preserves_provider_reset_time_without_a_delay_limit() {
    let (pool, database) = setup_ephemeral_pool("postgres_retry_timing_absolute", 4).await;
    register_job_definition(&pool).await;
    let job_id = enqueue_test_job(&pool, "absolute_retry_timing").await;
    let job = claim_one_job(&pool, "worker-absolute-retry-timing").await;
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");
    let database_now = sqlx::query_scalar::<_, DateTime<Utc>>("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .expect("load database clock");
    let requested_retry_at =
        database_now + chrono::Duration::days(30) + chrono::Duration::nanoseconds(1);
    let persisted_retry_at =
        database_now + chrono::Duration::days(30) + chrono::Duration::microseconds(1);

    let outcome = complete_job_failure_with_outcome(
        &pool,
        job.id,
        job.run_number,
        job.attempt,
        &worker_id,
        &JobFailureUpdate::new(
            JobFailureKind::Retryable,
            "provider.rate_limited",
            "retry at provider reset",
            Some(1_000),
        )
        .with_retry_timing(JobRetryTiming::At(requested_retry_at)),
    )
    .await
    .expect("persist absolute retry timing");

    let JobFailureCompletionDisposition::RetryScheduledAt {
        requested_retry_at,
        next_run_at,
    } = outcome.disposition
    else {
        panic!("expected absolute retry disposition");
    };
    assert_eq!(requested_retry_at, persisted_retry_at);
    assert_eq!(next_run_at, persisted_retry_at);
    assert_eq!(
        load_job(&pool, job_id).await.next_run_at,
        persisted_retry_at
    );

    let attempt = load_attempt_mutation_snapshot(&pool, job.id, job.run_number, job.attempt).await;
    assert_eq!(attempt.retry_delay_ms, Some(1_000));
    assert_eq!(attempt.requested_retry_not_before, Some(persisted_retry_at));
    assert_eq!(attempt.effective_next_run_at, Some(persisted_retry_at));
    assert_eq!(
        attempt.retry_timing_source.as_deref(),
        Some("HANDLER_NOT_BEFORE")
    );

    let event = list_job_events(&pool, None, job_id, 10, None)
        .await
        .expect("list absolute retry events")
        .into_iter()
        .find(|event| event.event_type == JobEventType::RetryScheduled)
        .expect("absolute retry event exists");
    assert_event_payload_keys(
        &event.payload,
        &[
            "effective_next_run_at",
            "next_run_at",
            "requested_retry_at",
            "requested_retry_not_before",
            "retry_delay_ms",
            "retry_timing_source",
        ],
    );
    assert_eq!(
        event_timestamp(&event.payload, "requested_retry_not_before"),
        persisted_retry_at
    );
    assert_eq!(
        event_timestamp(&event.payload, "requested_retry_at"),
        persisted_retry_at
    );
    assert_eq!(
        event_timestamp(&event.payload, "next_run_at"),
        persisted_retry_at
    );
    assert_eq!(
        event
            .payload
            .get("retry_timing_source")
            .and_then(Value::as_str),
        Some("HANDLER_NOT_BEFORE")
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn past_handler_not_before_cannot_shorten_policy_backoff() {
    let (pool, database) = setup_ephemeral_pool("postgres_retry_timing_past_absolute", 4).await;
    register_job_definition(&pool).await;
    let job_id = enqueue_test_job(&pool, "past_absolute_retry_timing").await;
    let job = claim_one_job(&pool, "worker-past-absolute-retry-timing").await;
    let worker_id = job.worker_id.clone().expect("claimed job has worker id");
    let requested_retry_at = Utc::now()
        .with_nanosecond(0)
        .expect("whole-second timestamp")
        - chrono::Duration::hours(1);
    let database_before = sqlx::query_scalar::<_, DateTime<Utc>>("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .expect("load database clock before completion");

    let outcome = complete_job_failure_with_outcome(
        &pool,
        job.id,
        job.run_number,
        job.attempt,
        &worker_id,
        &JobFailureUpdate::new(
            JobFailureKind::Retryable,
            "provider.reset_already_passed",
            "provider reset has already passed",
            Some(1_000),
        )
        .with_retry_timing(JobRetryTiming::At(requested_retry_at)),
    )
    .await
    .expect("persist past absolute retry timing");
    let database_after = sqlx::query_scalar::<_, DateTime<Utc>>("SELECT clock_timestamp()")
        .fetch_one(&pool)
        .await
        .expect("load database clock after completion");

    let JobFailureCompletionDisposition::RetryScheduled {
        retry_delay_ms,
        next_run_at,
    } = outcome.disposition
    else {
        panic!("expected policy retry disposition");
    };
    assert_eq!(retry_delay_ms, 1_000);
    assert!(next_run_at >= database_before + chrono::Duration::seconds(1));
    assert!(next_run_at <= database_after + chrono::Duration::seconds(1));
    let pending = load_job(&pool, job_id).await;
    assert_eq!(pending.status, JobStatus::Pending);
    assert_eq!(pending.next_run_at, next_run_at);
    assert!(
        claim_jobs(&pool, "worker-past-absolute-retry-claim", 30, 1)
            .await
            .expect("attempt claim before policy backoff")
            .is_empty()
    );
    let attempt = load_attempt_mutation_snapshot(&pool, job.id, job.run_number, job.attempt).await;
    assert_eq!(attempt.requested_retry_not_before, Some(requested_retry_at));
    assert_eq!(attempt.effective_next_run_at, Some(next_run_at));
    assert_eq!(attempt.retry_timing_source.as_deref(), Some("POLICY"));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn expired_lease_reapers_reject_invalid_retry_delay_without_mutating_lease() {
    let (pool, database) = setup_ephemeral_pool("postgres_retry_delay_reaper", 4).await;
    register_job_definition(&pool).await;
    let job_id = enqueue_test_job(&pool, "reaper_invalid_retry_delay").await;
    let job = claim_one_job(&pool, "worker-retry-delay-reaper").await;
    assert_eq!(job.id, job_id);
    expire_job_lease(&pool, job_id).await;

    let before = load_job(&pool, job_id).await;
    assert_eq!(before.status, JobStatus::Leased);
    assert_eq!(before.attempt, 1);
    assert!(before.lease_expires_at.is_some());
    assert_event_types(
        &pool,
        job_id,
        &[JobEventType::Enqueued, JobEventType::Leased],
    )
    .await;

    for default_retry_delay_ms in [0, -1] {
        assert_invalid_retry_delay_error(
            reap_expired_leases(&pool, 1, default_retry_delay_ms)
                .await
                .expect_err("invalid reaper retry delay should be rejected"),
        );
        assert_job_unchanged(&pool, job_id, &before).await;
        assert_event_types(
            &pool,
            job_id,
            &[JobEventType::Enqueued, JobEventType::Leased],
        )
        .await;
    }

    for default_retry_delay_ms in [0, -1] {
        assert_invalid_retry_delay_error(
            reap_expired_leases_with_terminal_records(&pool, 1, default_retry_delay_ms)
                .await
                .expect_err("invalid terminal-record reaper retry delay should be rejected"),
        );
        assert_job_unchanged(&pool, job_id, &before).await;
        assert_event_types(
            &pool,
            job_id,
            &[JobEventType::Enqueued, JobEventType::Leased],
        )
        .await;
    }

    teardown_ephemeral_pool(pool, database).await;
}
