use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use runledger_core::jobs::{JobStage, JobStatus, JobType};
use runledger_postgres::jobs::{
    JobDefinitionUpdate, JobEnqueue, JobEnqueueIntent, JobEnqueueIntentDisposition,
    JobEnqueueIntentListFilter, JobEnqueueIntentMetricsFilter, JobEnqueueIntentStatus,
    delete_promoted_job_enqueue_intents_before, delete_promoted_job_enqueue_intents_for_jobs_tx,
    enqueue_job, get_job_by_id, get_job_enqueue_intent_by_id, get_job_enqueue_intent_metrics,
    list_job_enqueue_intents, list_job_events, promote_job_enqueue_intents_for_types,
    record_job_enqueue_intent, record_job_enqueue_intent_tx, update_job_definition,
};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::types::Uuid;
use tokio::time::{sleep, timeout};

mod support;

use support::register_test_job_definition;

const JOB_TYPE: &str = "jobs.test.enqueue_intent";

fn query_error_code(error: &runledger_postgres::Error) -> Option<&str> {
    match error {
        runledger_postgres::Error::QueryError(error) => Some(error.code()),
        _ => None,
    }
}

async fn record_postgres_server_version(pool: &sqlx::PgPool, diagnostic: &str) {
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
        "{diagnostic} PostgreSQL server_version={server_version}, server_version_num={server_version_num}"
    );
}

#[tokio::test]
async fn records_without_definition_and_enforces_strict_transactional_idempotency() {
    let (pool, database) = setup_ephemeral_pool("postgres_enqueue_intent_record", 4).await;
    let payload = json!({"event": "analytics.capture", "value": 1});
    let intent = JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "analytics:1")
        .with_stage(JobStage::Scheduled);

    let mut tx = pool.begin().await.expect("begin application transaction");
    let inserted = record_job_enqueue_intent_tx(&mut tx, &intent)
        .await
        .expect("record intent before definition");
    assert_eq!(inserted.disposition, JobEnqueueIntentDisposition::Inserted);
    assert_eq!(inserted.status, JobEnqueueIntentStatus::Pending);
    assert_eq!(inserted.promoted_job_id, None);
    tx.commit().await.expect("commit application transaction");

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM job_queue")
            .fetch_one(&pool)
            .await
            .expect("count queue rows"),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM job_events")
            .fetch_one(&pool)
            .await
            .expect("count job events"),
        0
    );

    let existing = record_job_enqueue_intent(&pool, &intent)
        .await
        .expect("retry identical intent");
    assert_eq!(existing.intent_id, inserted.intent_id);
    assert_eq!(existing.disposition, JobEnqueueIntentDisposition::Existing);

    let changed_payload = json!({"event": "analytics.capture", "value": 2});
    let changed = JobEnqueueIntent::new(JobType::new(JOB_TYPE), &changed_payload, "analytics:1")
        .with_stage(JobStage::Scheduled);
    let error = record_job_enqueue_intent(&pool, &changed)
        .await
        .expect_err("changed retry must conflict");
    assert_eq!(
        query_error_code(&error),
        Some("job.intent_idempotency_conflict")
    );

    let rollback_payload = json!({"event": "rolled-back"});
    let rollback_intent = JobEnqueueIntent::new(
        JobType::new(JOB_TYPE),
        &rollback_payload,
        "analytics:rollback",
    );
    let mut rollback_tx = pool.begin().await.expect("begin rollback transaction");
    let rolled_back = record_job_enqueue_intent_tx(&mut rollback_tx, &rollback_intent)
        .await
        .expect("record rolled-back intent");
    rollback_tx.rollback().await.expect("rollback intent");
    assert!(
        get_job_enqueue_intent_by_id(&pool, None, rolled_back.intent_id)
            .await
            .expect("lookup rolled-back intent")
            .is_none()
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn jsonb_numeric_normalization_preserves_idempotency_and_promotion() {
    let (pool, database) = setup_ephemeral_pool("postgres_enqueue_intent_jsonb_numeric", 4).await;
    let exponent_payload = json!({"value": 1.7e18});
    let exponent_intent =
        JobEnqueueIntent::new(JobType::new(JOB_TYPE), &exponent_payload, "numeric-jsonb");
    let inserted = record_job_enqueue_intent(&pool, &exponent_intent)
        .await
        .expect("record exponent-form JSON number");

    let integer_payload = json!({"value": 1_700_000_000_000_000_000_u64});
    let integer_intent =
        JobEnqueueIntent::new(JobType::new(JOB_TYPE), &integer_payload, "numeric-jsonb");
    let existing = record_job_enqueue_intent(&pool, &integer_intent)
        .await
        .expect("JSONB-equivalent numeric retry");
    assert_eq!(existing.intent_id, inserted.intent_id);
    assert_eq!(existing.disposition, JobEnqueueIntentDisposition::Existing);

    register_test_job_definition(&pool, JOB_TYPE).await;
    let report = promote_job_enqueue_intents_for_types(&pool, &[JobType::new(JOB_TYPE)], 10)
        .await
        .expect("promote normalized JSONB request");
    assert_eq!(report.inserted_jobs, 1);
    assert_eq!(report.conflicted, 0);
    assert_eq!(
        get_job_enqueue_intent_by_id(&pool, None, inserted.intent_id)
            .await
            .expect("load normalized JSONB intent")
            .expect("normalized JSONB intent exists")
            .status,
        JobEnqueueIntentStatus::Promoted
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn validates_inputs_and_does_not_wait_on_job_definition_locks() {
    let (pool, database) = setup_ephemeral_pool("postgres_enqueue_intent_validation", 4).await;
    let payload = json!({"event": "validation"});

    for (intent, expected_code) in [
        (
            JobEnqueueIntent::new(JobType::new("\t"), &payload, "invalid-job-type-tab"),
            "job.invalid_job_type",
        ),
        (
            JobEnqueueIntent::new(JobType::new("\u{a0}"), &payload, "invalid-job-type-nbsp"),
            "job.invalid_job_type",
        ),
        (
            JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "   "),
            "job.intent_invalid_idempotency_key",
        ),
        (
            JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "invalid-attempts")
                .with_max_attempts(0),
            "job.intent_invalid_max_attempts",
        ),
        (
            JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "invalid-timeout")
                .with_timeout_seconds(0),
            "job.intent_invalid_timeout",
        ),
        (
            JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "invalid-resource")
                .with_execution_resource("   "),
            "job.invalid_execution_resource_key",
        ),
    ] {
        let error = record_job_enqueue_intent(&pool, &intent)
            .await
            .expect_err("invalid intent must be rejected");
        assert_eq!(query_error_code(&error), Some(expected_code));
    }

    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM job_enqueue_intents")
            .fetch_one(&pool)
            .await
            .expect("count intents after input validation"),
        0
    );

    for invalid_job_type in ["\t", "\u{a0}"] {
        let error = sqlx::query(
            "INSERT INTO job_enqueue_intents (
                job_type, payload, idempotency_key, enqueue_request
             )
             VALUES ($1, '{}'::jsonb, 'direct-invalid-job-type', '{}'::jsonb)",
        )
        .bind(invalid_job_type)
        .execute(&pool)
        .await
        .expect_err("database constraint must reject Unicode-blank job types");
        let database_error = error
            .as_database_error()
            .expect("constraint failure must be a database error");
        assert_eq!(
            database_error.constraint(),
            Some("chk_job_enqueue_intents_job_type_not_blank")
        );
    }

    let mut isolation_tx = pool.begin().await.expect("begin isolation transaction");
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *isolation_tx)
        .await
        .expect("set repeatable read");
    let isolation_intent =
        JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "repeatable-read");
    let error = record_job_enqueue_intent_tx(&mut isolation_tx, &isolation_intent)
        .await
        .expect_err("repeatable read must be rejected");
    assert_eq!(
        query_error_code(&error),
        Some("job.intent_idempotency_unsupported_isolation")
    );
    isolation_tx
        .rollback()
        .await
        .expect("rollback isolation test");

    let mut lock_tx = pool.begin().await.expect("begin definition lock");
    sqlx::query("LOCK TABLE job_definitions IN ACCESS EXCLUSIVE MODE")
        .execute(&mut *lock_tx)
        .await
        .expect("lock definitions");
    let pool_clone = pool.clone();
    let record_task = tokio::spawn(async move {
        let payload = json!({"event": "not-blocked"});
        let intent =
            JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "definition-lock-free");
        record_job_enqueue_intent(&pool_clone, &intent).await
    });
    let outcome = timeout(Duration::from_secs(1), record_task)
        .await
        .expect("intent recording must not wait on job_definitions")
        .expect("record task must not panic")
        .expect("record intent while definitions are locked");
    assert_eq!(outcome.status, JobEnqueueIntentStatus::Pending);
    lock_tx.rollback().await.expect("release definition lock");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn database_rejects_unicode_blank_intent_keys_and_stages() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_enqueue_intent_blank_constraints", 2).await;

    for (idempotency_key, stage, expected_constraint) in [
        (
            "\t",
            "queued",
            "chk_job_enqueue_intents_idempotency_key_not_blank",
        ),
        (
            "\u{a0}",
            "queued",
            "chk_job_enqueue_intents_idempotency_key_not_blank",
        ),
        (
            "valid-key-tab-stage",
            "\t",
            "chk_job_enqueue_intents_stage_not_blank",
        ),
        (
            "valid-key-nbsp-stage",
            "\u{a0}",
            "chk_job_enqueue_intents_stage_not_blank",
        ),
    ] {
        let error = sqlx::query(
            "INSERT INTO job_enqueue_intents (
                job_type, payload, idempotency_key, stage, enqueue_request
             )
             VALUES ($1, '{}'::jsonb, $2, $3, '{}'::jsonb)",
        )
        .bind(JOB_TYPE)
        .bind(idempotency_key)
        .bind(stage)
        .execute(&pool)
        .await
        .expect_err("database constraint must reject Unicode-blank intent fields");
        let database_error = error
            .as_database_error()
            .expect("constraint failure must be a database error");
        assert_eq!(database_error.constraint(), Some(expected_constraint));
    }

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn duplicate_recording_does_not_wait_on_a_promoters_row_lock() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_enqueue_intent_lock_compatibility", 4).await;
    let payload = json!({"event": "lock-compatible"});
    let recorded = record_job_enqueue_intent(
        &pool,
        &JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "lock-compatible"),
    )
    .await
    .expect("record intent before simulated promotion claim");

    let mut promotion_tx = pool.begin().await.expect("begin simulated promotion");
    sqlx::query("SELECT id FROM job_enqueue_intents WHERE id = $1 FOR NO KEY UPDATE")
        .bind(recorded.intent_id)
        .fetch_one(&mut *promotion_tx)
        .await
        .expect("hold the row lock used by promotion");

    let pool_clone = pool.clone();
    let retry = tokio::spawn(async move {
        let payload = json!({"event": "lock-compatible"});
        let intent = JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "lock-compatible");
        record_job_enqueue_intent(&pool_clone, &intent).await
    });
    let outcome = timeout(Duration::from_secs(1), retry)
        .await
        .expect("duplicate recording must not wait on a promoter's row lock")
        .expect("duplicate recording task must not panic")
        .expect("record duplicate intent during promotion claim");
    assert_eq!(outcome.intent_id, recorded.intent_id);
    assert_eq!(outcome.disposition, JobEnqueueIntentDisposition::Existing);

    promotion_tx
        .rollback()
        .await
        .expect("release simulated promotion lock");
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn concurrent_uncommitted_recorders_converge_after_unique_key_wait() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_enqueue_intent_concurrent_recorders", 5).await;
    record_postgres_server_version(&pool, "concurrent intent recorder regression").await;
    let payload = json!({"event": "concurrent-recorder"});
    let intent = JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "concurrent-recorder-key");

    let mut first_tx = pool.begin().await.expect("begin first recorder");
    let first = record_job_enqueue_intent_tx(&mut first_tx, &intent)
        .await
        .expect("record uncommitted winning intent");
    assert_eq!(first.disposition, JobEnqueueIntentDisposition::Inserted);

    let second_pool = pool.clone();
    let second_recorder = tokio::spawn(async move {
        let payload = json!({"event": "concurrent-recorder"});
        let intent =
            JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "concurrent-recorder-key");
        let mut tx = second_pool.begin().await.expect("begin second recorder");
        let outcome = record_job_enqueue_intent_tx(&mut tx, &intent).await;
        if outcome.is_ok() {
            tx.commit().await.expect("commit second recorder");
        }
        outcome
    });

    timeout(Duration::from_secs(2), async {
        loop {
            let waiting = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                    SELECT 1
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND wait_event_type = 'Lock'
                      AND query LIKE '%INSERT INTO job_enqueue_intents%'
                 )",
            )
            .fetch_one(&pool)
            .await
            .expect("inspect recorder unique-key wait");
            if waiting {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("second recorder should wait for the uncommitted unique-key winner");

    first_tx.commit().await.expect("commit first recorder");
    let second = timeout(Duration::from_secs(2), second_recorder)
        .await
        .expect("second recorder should converge after the winner commits")
        .expect("second recorder task must not panic")
        .expect("record the existing intent");
    assert_eq!(second.intent_id, first.intent_id);
    assert_eq!(second.disposition, JobEnqueueIntentDisposition::Existing);
    assert_eq!(second.status, JobEnqueueIntentStatus::Pending);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM job_enqueue_intents
             WHERE job_type = $1
               AND organization_id IS NULL
               AND idempotency_key = 'concurrent-recorder-key'",
        )
        .bind(JOB_TYPE)
        .fetch_one(&pool)
        .await
        .expect("count converged intent rows"),
        1
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn promotion_creates_ordinary_job_event_metrics_and_cleanup_state() {
    let (pool, database) = setup_ephemeral_pool("postgres_enqueue_intent_promote", 4).await;
    let payload = json!({"event": "analytics.capture", "account_id": "acct_1"});
    let organization_id = Uuid::from_u128(42);
    let intent = JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "analytics:acct_1")
        .with_organization_id(organization_id)
        .with_priority(250)
        .with_max_attempts(5)
        .with_timeout_seconds(45)
        .with_stage(JobStage::Scheduled)
        .with_execution_resource("analytics:account:acct_1");
    let recorded = record_job_enqueue_intent(&pool, &intent)
        .await
        .expect("record intent");

    let metrics = get_job_enqueue_intent_metrics(
        &pool,
        &JobEnqueueIntentMetricsFilter::new(10, 0).with_organization_id(organization_id),
    )
    .await
    .expect("read pending metrics");
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].pending_count, 1);
    assert_eq!(metrics[0].retrying_count, 0);
    assert_eq!(metrics[0].max_promotion_attempts, 0);
    assert_eq!(metrics[0].conflicted_count, 0);
    assert!(metrics[0].oldest_pending_at.is_some());

    register_test_job_definition(&pool, JOB_TYPE).await;
    let report = promote_job_enqueue_intents_for_types(&pool, &[JobType::new(JOB_TYPE)], 10)
        .await
        .expect("promote intent");
    assert_eq!(report.inserted_jobs, 1);
    assert_eq!(report.existing_jobs, 0);
    assert_eq!(report.total_promoted, 1);

    let promoted = get_job_enqueue_intent_by_id(&pool, Some(organization_id), recorded.intent_id)
        .await
        .expect("load promoted intent")
        .expect("promoted intent exists");
    assert_eq!(promoted.status, JobEnqueueIntentStatus::Promoted);
    assert_eq!(promoted.enqueue_request_version, 1);
    assert_eq!(promoted.promotion_attempts, 1);
    assert!(promoted.last_attempted_at.is_some());
    let job_id = promoted.promoted_job_id.expect("promoted job id");
    let job = get_job_by_id(&pool, Some(organization_id), job_id)
        .await
        .expect("load promoted job")
        .expect("promoted job exists");
    assert_eq!(job.status, JobStatus::Pending);
    assert_eq!(job.priority, 250);
    assert_eq!(job.max_attempts, 5);
    assert_eq!(job.timeout_seconds, 45);
    assert_eq!(job.stage, JobStage::Scheduled);
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>(
            "SELECT execution_resource_key FROM job_queue WHERE id = $1"
        )
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("read promoted execution resource"),
        Some("analytics:account:acct_1".to_owned())
    );
    assert_eq!(
        list_job_events(&pool, Some(organization_id), job_id, 10, None)
            .await
            .expect("list enqueue events")
            .len(),
        1
    );

    let metrics = get_job_enqueue_intent_metrics(
        &pool,
        &JobEnqueueIntentMetricsFilter::new(10, 0).with_organization_id(organization_id),
    )
    .await
    .expect("read promoted metrics");
    assert_eq!(metrics[0].pending_count, 0);
    assert_eq!(metrics[0].retrying_count, 0);
    assert_eq!(metrics[0].max_promotion_attempts, 0);
    assert_eq!(metrics[0].promoted_24h, 1);
    assert_eq!(metrics[0].oldest_pending_at, None);

    sqlx::query(
        "UPDATE job_enqueue_intents
         SET promoted_at = now() - interval '25 hours'
         WHERE id = $1",
    )
    .bind(recorded.intent_id)
    .execute(&pool)
    .await
    .expect("age promoted intent beyond the metrics window");
    assert!(
        get_job_enqueue_intent_metrics(
            &pool,
            &JobEnqueueIntentMetricsFilter::new(10, 0).with_organization_id(organization_id),
        )
        .await
        .expect("read metrics after recent-promotion window")
        .is_empty(),
        "retained promoted history outside the metrics window must not create zero-valued groups"
    );

    let listed = list_job_enqueue_intents(
        &pool,
        &JobEnqueueIntentListFilter::new(10, 0)
            .with_organization_id(organization_id)
            .with_status(JobEnqueueIntentStatus::Promoted)
            .with_job_type_query("enqueue_intent"),
    )
    .await
    .expect("list promoted intents");
    assert_eq!(listed.len(), 1);

    assert_eq!(
        delete_promoted_job_enqueue_intents_before(
            &pool,
            Utc::now() + ChronoDuration::seconds(1),
            10,
        )
        .await
        .expect("delete promoted intent"),
        1
    );
    assert!(
        get_job_enqueue_intent_by_id(&pool, None, recorded.intent_id)
            .await
            .expect("lookup deleted intent")
            .is_none()
    );
    assert!(
        get_job_by_id(&pool, None, job_id)
            .await
            .expect("load job")
            .is_some()
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn global_and_organization_scopes_keep_the_same_key_independent() {
    let (pool, database) = setup_ephemeral_pool("postgres_enqueue_intent_scope", 4).await;
    let payload = json!({"event": "same-key-different-scope"});
    let organization_id = Uuid::from_u128(77);
    let global = JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "shared-key");
    let organization = JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "shared-key")
        .with_organization_id(organization_id);

    let global_outcome = record_job_enqueue_intent(&pool, &global)
        .await
        .expect("record global intent");
    let organization_outcome = record_job_enqueue_intent(&pool, &organization)
        .await
        .expect("record organization intent");
    assert_ne!(global_outcome.intent_id, organization_outcome.intent_id);
    assert_eq!(
        record_job_enqueue_intent(&pool, &global)
            .await
            .expect("retry global intent")
            .intent_id,
        global_outcome.intent_id
    );
    assert_eq!(
        record_job_enqueue_intent(&pool, &organization)
            .await
            .expect("retry organization intent")
            .intent_id,
        organization_outcome.intent_id
    );
    assert!(
        get_job_enqueue_intent_by_id(&pool, Some(organization_id), global_outcome.intent_id)
            .await
            .expect("scope global lookup")
            .is_none()
    );
    assert_eq!(
        list_job_enqueue_intents(
            &pool,
            &JobEnqueueIntentListFilter::new(10, 0).with_organization_id(organization_id),
        )
        .await
        .expect("list organization intents")
        .into_iter()
        .map(|record| record.id)
        .collect::<Vec<_>>(),
        vec![organization_outcome.intent_id]
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn intent_metrics_are_bounded_and_page_in_stable_job_type_order() {
    let (pool, database) = setup_ephemeral_pool("postgres_enqueue_intent_metrics_page", 4).await;
    let organization_id = Uuid::from_u128(88);
    let payload = json!({"event": "metrics-page"});
    let job_types = [
        "jobs.test.intent_metrics.a",
        "jobs.test.intent_metrics.b",
        "jobs.test.intent_metrics.c",
    ];

    for job_type in job_types {
        let outcome = record_job_enqueue_intent(
            &pool,
            &JobEnqueueIntent::new(JobType::new(job_type), &payload, job_type)
                .with_organization_id(organization_id),
        )
        .await
        .expect("record organization-scoped metrics intent");
        assert_eq!(outcome.status, JobEnqueueIntentStatus::Pending);
    }
    let global = record_job_enqueue_intent(
        &pool,
        &JobEnqueueIntent::new(
            JobType::new("jobs.test.intent_metrics.global"),
            &payload,
            "metrics-global",
        ),
    )
    .await
    .expect("record out-of-scope global metrics intent");
    assert_eq!(global.status, JobEnqueueIntentStatus::Pending);

    let first_page = get_job_enqueue_intent_metrics(
        &pool,
        &JobEnqueueIntentMetricsFilter::new(2, 0).with_organization_id(organization_id),
    )
    .await
    .expect("read first metrics page");
    assert_eq!(
        first_page
            .iter()
            .map(|record| record.job_type.as_str())
            .collect::<Vec<_>>(),
        &job_types[..2]
    );

    let second_page = get_job_enqueue_intent_metrics(
        &pool,
        &JobEnqueueIntentMetricsFilter::new(2, 2).with_organization_id(organization_id),
    )
    .await
    .expect("read second metrics page");
    assert_eq!(
        second_page
            .iter()
            .map(|record| record.job_type.as_str())
            .collect::<Vec<_>>(),
        &job_types[2..]
    );

    let exact_type = get_job_enqueue_intent_metrics(
        &pool,
        &JobEnqueueIntentMetricsFilter::new(10, 0)
            .with_organization_id(organization_id)
            .with_job_type(JobType::new(job_types[1])),
    )
    .await
    .expect("read exact-type metrics page");
    assert_eq!(exact_type.len(), 1);
    assert_eq!(exact_type[0].job_type.as_str(), job_types[1]);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn recording_retries_when_a_conflicting_intent_disappears_before_lookup() {
    let (pool, database) = setup_ephemeral_pool("postgres_enqueue_intent_disappearing", 4).await;
    let payload = json!({"event": "disappearing-winner"});
    let intent = JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "disappearing-intent-key");
    let original = record_job_enqueue_intent(&pool, &intent)
        .await
        .expect("record original intent");

    sqlx::query("CREATE TABLE delete_conflicting_intent_once (armed boolean PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("create one-shot delete control");
    sqlx::query("INSERT INTO delete_conflicting_intent_once VALUES (true)")
        .execute(&pool)
        .await
        .expect("arm one-shot delete control");
    sqlx::query(
        "CREATE FUNCTION delete_conflicting_intent_once_for_test()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             IF EXISTS (SELECT 1 FROM delete_conflicting_intent_once WHERE armed) THEN
                 DELETE FROM job_enqueue_intents
                 WHERE idempotency_key = 'disappearing-intent-key';
                 DELETE FROM delete_conflicting_intent_once WHERE armed;
             END IF;
             RETURN NULL;
         END;
         $$",
    )
    .execute(&pool)
    .await
    .expect("create one-shot disappearing-intent function");
    sqlx::query(
        "CREATE TRIGGER trg_delete_conflicting_intent_once_for_test
         AFTER INSERT ON job_enqueue_intents
         FOR EACH STATEMENT
         EXECUTE FUNCTION delete_conflicting_intent_once_for_test()",
    )
    .execute(&pool)
    .await
    .expect("create one-shot disappearing-intent trigger");

    let retried = record_job_enqueue_intent(&pool, &intent)
        .await
        .expect("retry after conflicting intent disappears");
    assert_eq!(retried.disposition, JobEnqueueIntentDisposition::Inserted);
    assert_ne!(retried.intent_id, original.intent_id);
    assert_eq!(retried.status, JobEnqueueIntentStatus::Pending);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM job_enqueue_intents
             WHERE idempotency_key = 'disappearing-intent-key'"
        )
        .fetch_one(&pool)
        .await
        .expect("count replacement intent"),
        1
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn unavailable_definitions_stay_pending_and_direct_enqueues_converge_or_conflict() {
    let (pool, database) = setup_ephemeral_pool("postgres_enqueue_intent_conflicts", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    update_job_definition(
        &pool,
        JobType::new(JOB_TYPE),
        &JobDefinitionUpdate {
            max_attempts: None,
            default_timeout_seconds: None,
            default_priority: None,
            is_enabled: Some(false),
        },
    )
    .await
    .expect("disable definition");

    let pending_payload = json!({"event": "pending"});
    let pending_intent =
        JobEnqueueIntent::new(JobType::new(JOB_TYPE), &pending_payload, "pending-key");
    let pending = record_job_enqueue_intent(&pool, &pending_intent)
        .await
        .expect("record pending intent");
    assert_eq!(
        promote_job_enqueue_intents_for_types(&pool, &[JobType::new(JOB_TYPE)], 10)
            .await
            .expect("skip disabled definition"),
        Default::default()
    );
    assert_eq!(
        get_job_enqueue_intent_by_id(&pool, None, pending.intent_id)
            .await
            .expect("load pending intent")
            .expect("pending intent exists")
            .status,
        JobEnqueueIntentStatus::Pending
    );

    update_job_definition(
        &pool,
        JobType::new(JOB_TYPE),
        &JobDefinitionUpdate {
            max_attempts: None,
            default_timeout_seconds: None,
            default_priority: None,
            is_enabled: Some(true),
        },
    )
    .await
    .expect("enable definition");

    let equivalent_payload = json!({"event": "equivalent"});
    let equivalent_intent = JobEnqueueIntent::new(
        JobType::new(JOB_TYPE),
        &equivalent_payload,
        "equivalent-key",
    );
    let equivalent = record_job_enqueue_intent(&pool, &equivalent_intent)
        .await
        .expect("record equivalent intent");
    let existing_job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: None,
            payload: &equivalent_payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: Some("equivalent-key"),
            stage: None,
        },
    )
    .await
    .expect("direct equivalent enqueue");
    sqlx::query("UPDATE job_queue SET created_at = now() - interval '1 year' WHERE id = $1")
        .bind(existing_job_id)
        .execute(&pool)
        .await
        .expect("age existing job beyond a typical queue retention cutoff");

    let conflicting_intent_payload = json!({"event": "intent-version"});
    let conflicting_intent = JobEnqueueIntent::new(
        JobType::new(JOB_TYPE),
        &conflicting_intent_payload,
        "conflicting-key",
    );
    let conflicting = record_job_enqueue_intent(&pool, &conflicting_intent)
        .await
        .expect("record conflicting intent");
    let direct_payload = json!({"event": "direct-version"});
    enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: None,
            payload: &direct_payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: Some("conflicting-key"),
            stage: None,
        },
    )
    .await
    .expect("direct conflicting enqueue");

    let report = promote_job_enqueue_intents_for_types(&pool, &[JobType::new(JOB_TYPE)], 10)
        .await
        .expect("promote pending intents");
    assert_eq!(report.inserted_jobs, 1);
    assert_eq!(report.existing_jobs, 1);
    assert_eq!(report.conflicted, 1);
    assert_eq!(report.total_promoted, 2);

    let equivalent = get_job_enqueue_intent_by_id(&pool, None, equivalent.intent_id)
        .await
        .expect("load equivalent intent")
        .expect("equivalent intent exists");
    assert_eq!(equivalent.status, JobEnqueueIntentStatus::Promoted);
    assert_eq!(equivalent.promoted_job_id, Some(existing_job_id));
    let delete_error = sqlx::query("DELETE FROM job_queue WHERE id = $1")
        .bind(existing_job_id)
        .execute(&pool)
        .await
        .expect_err("promoted intent must retain its linked job");
    let database_error = delete_error
        .as_database_error()
        .expect("retention fence must be a database error");
    assert_eq!(database_error.code().as_deref(), Some("23001"));
    assert_eq!(
        database_error.constraint(),
        Some("fk_job_enqueue_intents_promoted_job")
    );
    assert_eq!(
        delete_promoted_job_enqueue_intents_before(
            &pool,
            Utc::now() - ChronoDuration::days(1),
            10,
        )
        .await
        .expect("time-based cleanup must retain the new intent"),
        0
    );

    let mut retention_tx = pool
        .begin()
        .await
        .expect("begin exact retention transaction");
    assert_eq!(
        delete_promoted_job_enqueue_intents_for_jobs_tx(&mut retention_tx, &[])
            .await
            .expect("empty exact retention cleanup"),
        0
    );
    assert_eq!(
        delete_promoted_job_enqueue_intents_for_jobs_tx(&mut retention_tx, &[existing_job_id],)
            .await
            .expect("delete intent linked to existing job"),
        1
    );
    assert_eq!(
        sqlx::query("DELETE FROM job_queue WHERE id = $1")
            .bind(existing_job_id)
            .execute(&mut *retention_tx)
            .await
            .expect("delete linked job after exact intent cleanup")
            .rows_affected(),
        1
    );
    retention_tx
        .commit()
        .await
        .expect("commit exact retention transaction");
    assert!(
        get_job_enqueue_intent_by_id(&pool, None, equivalent.id)
            .await
            .expect("check exact retention cleanup")
            .is_none()
    );

    let conflicted = get_job_enqueue_intent_by_id(&pool, None, conflicting.intent_id)
        .await
        .expect("load conflicted intent")
        .expect("conflicted intent exists");
    assert_eq!(conflicted.status, JobEnqueueIntentStatus::Conflicted);
    assert_eq!(
        conflicted.last_error_code.as_deref(),
        Some("job.idempotency_conflict")
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn retention_cleanup_locks_existing_job_and_defers_concurrent_promotion() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_enqueue_intent_retention_promotion_race", 6).await;
    record_postgres_server_version(&pool, "intent retention/promotion lock regression").await;
    register_test_job_definition(&pool, JOB_TYPE).await;

    let payload = json!({"event": "retention-promotion-race"});
    let recorded = record_job_enqueue_intent(
        &pool,
        &JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "retention-race-key"),
    )
    .await
    .expect("record intent before retention race");
    let old_job_id = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: None,
            payload: &payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: Some("retention-race-key"),
            stage: None,
        },
    )
    .await
    .expect("enqueue existing job before retention race");

    let mut retention_tx = pool.begin().await.expect("begin retention transaction");
    assert_eq!(
        delete_promoted_job_enqueue_intents_for_jobs_tx(&mut retention_tx, &[old_job_id])
            .await
            .expect("clean promoted intents and lock retained job"),
        0
    );

    let promotion_pool = pool.clone();
    let promotion = tokio::spawn(async move {
        promote_job_enqueue_intents_for_types(&promotion_pool, &[JobType::new(JOB_TYPE)], 1).await
    });

    timeout(Duration::from_secs(2), async {
        loop {
            let waiting = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                    SELECT 1
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND wait_event_type = 'Lock'
                      AND query LIKE '%FROM job_queue%'
                      AND query LIKE '%FOR NO KEY UPDATE%'
                 )",
            )
            .fetch_one(&pool)
            .await
            .expect("inspect promotion job-lock wait");
            if waiting {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("promotion should wait on the pre-locked existing job");

    assert_eq!(
        sqlx::query("DELETE FROM job_queue WHERE id = $1")
            .bind(old_job_id)
            .execute(&mut *retention_tx)
            .await
            .expect("delete pre-locked existing job")
            .rows_affected(),
        1
    );
    retention_tx
        .commit()
        .await
        .expect("commit retention transaction");

    let first_report = timeout(Duration::from_secs(2), promotion)
        .await
        .expect("promotion must not deadlock with retention")
        .expect("promotion task must not panic")
        .expect("promotion race should remain recoverable");
    assert_eq!(first_report.retry_deferred, 1);
    assert_eq!(first_report.total_promoted, 0);

    let deferred = get_job_enqueue_intent_by_id(&pool, None, recorded.intent_id)
        .await
        .expect("load deferred retention-race intent")
        .expect("deferred retention-race intent exists");
    assert_eq!(deferred.status, JobEnqueueIntentStatus::Pending);
    assert_eq!(deferred.promotion_attempts, 1);
    sqlx::query("UPDATE job_enqueue_intents SET next_promotion_at = now() WHERE id = $1")
        .bind(recorded.intent_id)
        .execute(&pool)
        .await
        .expect("make retention-race intent immediately retryable");

    let retry_report = promote_job_enqueue_intents_for_types(&pool, &[JobType::new(JOB_TYPE)], 1)
        .await
        .expect("retry intent after retained job deletion");
    assert_eq!(retry_report.inserted_jobs, 1);
    let promoted = get_job_enqueue_intent_by_id(&pool, None, recorded.intent_id)
        .await
        .expect("load recovered retention-race intent")
        .expect("recovered retention-race intent exists");
    assert_eq!(promoted.status, JobEnqueueIntentStatus::Promoted);
    assert_ne!(
        promoted.promoted_job_id.expect("replacement job id"),
        old_job_id
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn inverse_existing_job_order_exposes_promotion_retention_deadlock() {
    const PROMOTION_BLOCKER_LOCK: i64 = 0x7275_6e6c_7465_7374;

    let (pool, database) =
        setup_ephemeral_pool("postgres_enqueue_intent_inverse_retention_order", 8).await;
    record_postgres_server_version(&pool, "inverse intent retention lock characterization").await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let payload = json!({"event": "inverse-retention-order"});

    let job_a = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: None,
            payload: &payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: Some("inverse-order-a"),
            stage: None,
        },
    )
    .await
    .expect("enqueue first inverse-order job");
    let job_b = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new(JOB_TYPE),
            organization_id: None,
            payload: &payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: Some("inverse-order-b"),
            stage: None,
        },
    )
    .await
    .expect("enqueue second inverse-order job");
    let (low_job_id, low_key, high_job_id, high_key) = if job_a < job_b {
        (job_a, "inverse-order-a", job_b, "inverse-order-b")
    } else {
        (job_b, "inverse-order-b", job_a, "inverse-order-a")
    };

    let high_intent = record_job_enqueue_intent(
        &pool,
        &JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, high_key),
    )
    .await
    .expect("record high-job intent first");
    let low_intent = record_job_enqueue_intent(
        &pool,
        &JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, low_key),
    )
    .await
    .expect("record low-job intent second");
    sqlx::query(
        "UPDATE job_enqueue_intents
         SET created_at = CASE
             WHEN id = $1 THEN now() - interval '2 seconds'
             ELSE now() - interval '1 second'
         END
         WHERE id = ANY($2::uuid[])",
    )
    .bind(high_intent.intent_id)
    .bind(&[high_intent.intent_id, low_intent.intent_id])
    .execute(&pool)
    .await
    .expect("force inverse intent promotion order");

    sqlx::query(
        "CREATE FUNCTION block_inverse_intent_promotion() RETURNS trigger
         LANGUAGE plpgsql AS $function$
         BEGIN
             IF NEW.status = 'PROMOTED' THEN
                 PERFORM pg_advisory_xact_lock(8247619704687260532);
             END IF;
             RETURN NEW;
         END
         $function$",
    )
    .execute(&pool)
    .await
    .expect("create inverse-order promotion blocker function");
    sqlx::query(
        "CREATE TRIGGER trg_block_inverse_intent_promotion
         BEFORE UPDATE ON job_enqueue_intents
         FOR EACH ROW
         EXECUTE FUNCTION block_inverse_intent_promotion()",
    )
    .execute(&pool)
    .await
    .expect("create inverse-order promotion blocker trigger");

    let mut blocker_tx = pool.begin().await.expect("begin promotion blocker");
    let blocker_pid = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
        .fetch_one(&mut *blocker_tx)
        .await
        .expect("load promotion blocker pid");
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(PROMOTION_BLOCKER_LOCK)
        .execute(&mut *blocker_tx)
        .await
        .expect("lock promotion trigger blocker");

    let promotion_pool = pool.clone();
    let promotion = tokio::spawn(async move {
        promote_job_enqueue_intents_for_types(&promotion_pool, &[JobType::new(JOB_TYPE)], 2).await
    });

    timeout(Duration::from_secs(2), async {
        loop {
            let blocked = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                    SELECT 1
                    FROM pg_stat_activity activity
                    WHERE $1 = ANY(pg_blocking_pids(activity.pid))
                      AND activity.query LIKE '%UPDATE job_enqueue_intents%'
                 )",
            )
            .bind(blocker_pid)
            .fetch_one(&pool)
            .await
            .expect("inspect promotion blocker graph");
            if blocked {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("promotion must hold the high job while blocked by the trigger");

    let retention_pool = pool.clone();
    let (retention_pid_tx, retention_pid_rx) = tokio::sync::oneshot::channel();
    let retention = tokio::spawn(async move {
        let mut retention_tx = retention_pool
            .begin()
            .await
            .expect("begin inverse-order retention");
        let retention_pid = sqlx::query_scalar::<_, i32>("SELECT pg_backend_pid()")
            .fetch_one(&mut *retention_tx)
            .await
            .expect("load inverse-order retention pid");
        retention_pid_tx
            .send(retention_pid)
            .expect("publish inverse-order retention pid");

        delete_promoted_job_enqueue_intents_for_jobs_tx(
            &mut retention_tx,
            &[low_job_id, high_job_id],
        )
        .await?;
        sqlx::query("DELETE FROM job_queue WHERE id = ANY($1::uuid[])")
            .bind(&[low_job_id, high_job_id])
            .execute(&mut *retention_tx)
            .await
            .expect("delete inverse-order retained jobs");
        retention_tx
            .commit()
            .await
            .expect("commit inverse-order retention");
        Ok::<(), runledger_postgres::Error>(())
    });
    let retention_pid = timeout(Duration::from_secs(2), retention_pid_rx)
        .await
        .expect("retention must publish its backend pid")
        .expect("retention pid sender must remain alive");

    timeout(Duration::from_secs(2), async {
        loop {
            let blocked =
                sqlx::query_scalar::<_, bool>("SELECT cardinality(pg_blocking_pids($1)) > 0")
                    .bind(retention_pid)
                    .fetch_one(&pool)
                    .await
                    .expect("inspect retention blocker graph");
            if blocked {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("retention must hold the low job while waiting for the high job");

    blocker_tx
        .rollback()
        .await
        .expect("release promotion trigger blocker");
    let (promotion_result, retention_result) = tokio::join!(
        timeout(Duration::from_secs(5), promotion),
        timeout(Duration::from_secs(5), retention),
    );
    let promotion_result = promotion_result
        .expect("promotion must resolve the forced deadlock")
        .expect("promotion task must not panic");
    let retention_result = retention_result
        .expect("retention must resolve the forced deadlock")
        .expect("retention task must not panic");

    match (promotion_result, retention_result) {
        (Ok(report), Ok(())) => {
            assert_eq!(report.existing_jobs, 1);
            assert_eq!(report.retry_deferred, 1);
        }
        (Ok(report), Err(runledger_postgres::Error::QueryError(error))) => {
            assert_eq!(report.existing_jobs, 2);
            assert_eq!(report.retry_deferred, 0);
            assert_eq!(error.sqlstate(), Some("40P01"));
        }
        (promotion_result, retention_result) => panic!(
            "forced inverse-order cycle had unexpected outcomes: promotion={promotion_result:?}, retention={retention_result:?}"
        ),
    }

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn concurrent_promoters_create_one_job() {
    let (pool, database) = setup_ephemeral_pool("postgres_enqueue_intent_concurrent", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let payload = json!({"event": "concurrent"});
    let intent = JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "concurrent-key");
    let recorded = record_job_enqueue_intent(&pool, &intent)
        .await
        .expect("record concurrent intent");

    let first_pool = pool.clone();
    let second_pool = pool.clone();
    let (first, second) = tokio::join!(
        async move {
            promote_job_enqueue_intents_for_types(&first_pool, &[JobType::new(JOB_TYPE)], 1).await
        },
        async move {
            promote_job_enqueue_intents_for_types(&second_pool, &[JobType::new(JOB_TYPE)], 1).await
        }
    );
    let first = first.expect("first promoter");
    let second = second.expect("second promoter");
    assert_eq!(first.total_promoted + second.total_promoted, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM job_queue WHERE idempotency_key = 'concurrent-key'"
        )
        .fetch_one(&pool)
        .await
        .expect("count concurrently promoted jobs"),
        1
    );
    assert_eq!(
        get_job_enqueue_intent_by_id(&pool, None, recorded.intent_id)
            .await
            .expect("load concurrent intent")
            .expect("concurrent intent exists")
            .status,
        JobEnqueueIntentStatus::Promoted
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn snapshot_drift_defers_without_starving_and_promotes_after_repair() {
    let (pool, database) = setup_ephemeral_pool("postgres_enqueue_intent_poison_row", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let poison_payload = json!({"event": "poison"});
    let healthy_payload = json!({"event": "healthy"});
    let poison = record_job_enqueue_intent(
        &pool,
        &JobEnqueueIntent::new(JobType::new(JOB_TYPE), &poison_payload, "poison-key"),
    )
    .await
    .expect("record poison candidate");
    let healthy = record_job_enqueue_intent(
        &pool,
        &JobEnqueueIntent::new(JobType::new(JOB_TYPE), &healthy_payload, "healthy-key"),
    )
    .await
    .expect("record healthy intent");
    let original_snapshot = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT enqueue_request FROM job_enqueue_intents WHERE id = $1",
    )
    .bind(poison.intent_id)
    .fetch_one(&pool)
    .await
    .expect("load canonical snapshot before simulating drift");
    sqlx::query("UPDATE job_enqueue_intents SET enqueue_request = '{}'::jsonb WHERE id = $1")
        .bind(poison.intent_id)
        .execute(&pool)
        .await
        .expect("corrupt canonical snapshot for poison-row regression");

    let report = promote_job_enqueue_intents_for_types(&pool, &[JobType::new(JOB_TYPE)], 10)
        .await
        .expect("process poison and healthy intents");
    assert_eq!(report.retry_deferred, 1);
    assert_eq!(report.conflicted, 0);
    assert_eq!(report.inserted_jobs, 1);
    assert_eq!(report.total_promoted, 1);

    let poison = get_job_enqueue_intent_by_id(&pool, None, poison.intent_id)
        .await
        .expect("load poison intent")
        .expect("poison intent exists");
    assert_eq!(poison.status, JobEnqueueIntentStatus::Pending);
    assert_eq!(poison.promotion_attempts, 1);
    assert!(poison.last_attempted_at.is_some());
    assert_eq!(
        poison.last_error_code.as_deref(),
        Some("job.intent_snapshot_mismatch")
    );
    let healthy = get_job_enqueue_intent_by_id(&pool, None, healthy.intent_id)
        .await
        .expect("load healthy intent")
        .expect("healthy intent exists");
    assert_eq!(healthy.status, JobEnqueueIntentStatus::Promoted);
    assert!(poison.next_promotion_at > poison.last_attempted_at.expect("attempt timestamp"));

    sqlx::query(
        "UPDATE job_enqueue_intents
         SET enqueue_request = $2,
             next_promotion_at = now()
         WHERE id = $1",
    )
    .bind(poison.id)
    .bind(original_snapshot)
    .execute(&pool)
    .await
    .expect("repair canonical snapshot and make intent eligible");

    let recovered = promote_job_enqueue_intents_for_types(&pool, &[JobType::new(JOB_TYPE)], 10)
        .await
        .expect("promote repaired snapshot");
    assert_eq!(recovered.inserted_jobs, 1);
    let poison = get_job_enqueue_intent_by_id(&pool, None, poison.id)
        .await
        .expect("reload repaired intent")
        .expect("repaired intent exists");
    assert_eq!(poison.status, JobEnqueueIntentStatus::Promoted);
    assert_eq!(poison.promotion_attempts, 2);
    assert!(poison.last_error_code.is_none());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn worker_version_skips_newer_snapshot_versions_without_mutating_them() {
    let (pool, database) = setup_ephemeral_pool("postgres_enqueue_intent_newer_version", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let newer_payload = json!({"event": "newer-version"});
    let healthy_payload = json!({"event": "current-version"});
    let newer = record_job_enqueue_intent(
        &pool,
        &JobEnqueueIntent::new(JobType::new(JOB_TYPE), &newer_payload, "newer-version"),
    )
    .await
    .expect("record future-version candidate");
    let healthy = record_job_enqueue_intent(
        &pool,
        &JobEnqueueIntent::new(JobType::new(JOB_TYPE), &healthy_payload, "current-version"),
    )
    .await
    .expect("record current-version intent");

    sqlx::query(
        "ALTER TABLE job_enqueue_intents
         DROP CONSTRAINT chk_job_enqueue_intents_enqueue_request_version",
    )
    .execute(&pool)
    .await
    .expect("simulate a migration that introduces a newer snapshot version");
    sqlx::query("UPDATE job_enqueue_intents SET enqueue_request_version = 2 WHERE id = $1")
        .bind(newer.intent_id)
        .execute(&pool)
        .await
        .expect("mark intent as written by a newer producer");

    let report = promote_job_enqueue_intents_for_types(&pool, &[JobType::new(JOB_TYPE)], 10)
        .await
        .expect("promote only supported snapshot versions");
    assert_eq!(report.inserted_jobs, 1);
    assert_eq!(report.conflicted, 0);
    let newer = get_job_enqueue_intent_by_id(&pool, None, newer.intent_id)
        .await
        .expect("load newer-version intent")
        .expect("newer-version intent exists");
    assert_eq!(newer.status, JobEnqueueIntentStatus::Pending);
    assert_eq!(newer.promotion_attempts, 0);
    assert_eq!(
        get_job_enqueue_intent_by_id(&pool, None, healthy.intent_id)
            .await
            .expect("load current-version intent")
            .expect("current-version intent exists")
            .status,
        JobEnqueueIntentStatus::Promoted
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn promotion_keeps_savepoint_headroom_below_postgres_subtransaction_cache() {
    let (pool, database) = setup_ephemeral_pool("postgres_enqueue_intent_batch_cap", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let payload = json!({"event": "bounded-promotion"});

    for index in 0..25 {
        let key = format!("bounded-promotion-{index}");
        let outcome = record_job_enqueue_intent(
            &pool,
            &JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, &key),
        )
        .await
        .expect("record bounded promotion intent");
        assert_eq!(outcome.status, JobEnqueueIntentStatus::Pending);
    }

    sqlx::query("UPDATE job_enqueue_intents SET enqueue_request = '{}'::jsonb")
        .execute(&pool)
        .await
        .expect("drift snapshots so every row rolls back to its savepoint");

    let first = promote_job_enqueue_intents_for_types(&pool, &[JobType::new(JOB_TYPE)], 1_000)
        .await
        .expect("process capped batch of failing rows");
    assert!(first.batch_was_full());
    assert_eq!(first.retry_deferred, 24);
    assert_eq!(first.conflicted, 0);
    assert_eq!(first.total_promoted, 0);
    let metrics = get_job_enqueue_intent_metrics(
        &pool,
        &JobEnqueueIntentMetricsFilter::new(10, 0).with_job_type(JobType::new(JOB_TYPE)),
    )
    .await
    .expect("read state after capped batch");
    assert_eq!(metrics[0].pending_count, 25);
    assert_eq!(metrics[0].retrying_count, 24);
    assert_eq!(metrics[0].conflicted_count, 0);

    let second = promote_job_enqueue_intents_for_types(&pool, &[JobType::new(JOB_TYPE)], 1_000)
        .await
        .expect("process final failing row");
    assert!(!second.batch_was_full());
    assert_eq!(second.retry_deferred, 1);
    assert_eq!(second.conflicted, 0);
    assert_eq!(second.total_promoted, 0);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn definition_disabled_after_eligibility_leaves_intent_pending() {
    let (pool, database) = setup_ephemeral_pool("postgres_enqueue_intent_disable_race", 6).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let payload = json!({"event": "definition-disable-race"});
    let recorded = record_job_enqueue_intent(
        &pool,
        &JobEnqueueIntent::new(JobType::new(JOB_TYPE), &payload, "disable-race-key"),
    )
    .await
    .expect("record race intent");

    let mut disable_tx = pool.begin().await.expect("begin definition disable");
    sqlx::query("UPDATE job_definitions SET is_enabled = false WHERE job_type = $1")
        .bind(JOB_TYPE)
        .execute(&mut *disable_tx)
        .await
        .expect("hold uncommitted definition disable");

    let promotion_pool = pool.clone();
    let promotion = tokio::spawn(async move {
        promote_job_enqueue_intents_for_types(&promotion_pool, &[JobType::new(JOB_TYPE)], 1).await
    });

    timeout(Duration::from_secs(2), async {
        loop {
            let waiting = sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (
                    SELECT 1
                    FROM pg_stat_activity
                    WHERE datname = current_database()
                      AND wait_event_type = 'Lock'
                      AND query LIKE '%INSERT INTO job_queue%'
                 )",
            )
            .fetch_one(&pool)
            .await
            .expect("inspect promotion lock wait");
            if waiting {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("promotion should reach definition lock after eligibility selection");

    disable_tx
        .commit()
        .await
        .expect("commit definition disable");
    let report = promotion
        .await
        .expect("promotion task must not panic")
        .expect("promotion should classify disabled definition");
    assert_eq!(report.definition_became_unavailable, 1);
    assert_eq!(report.total_promoted, 0);
    assert_eq!(
        get_job_enqueue_intent_by_id(&pool, None, recorded.intent_id)
            .await
            .expect("load race intent")
            .expect("race intent exists")
            .status,
        JobEnqueueIntentStatus::Pending
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn enqueue_event_failure_defers_only_failed_intent_and_rolls_back_its_job() {
    let (pool, database) = setup_ephemeral_pool("postgres_enqueue_intent_event_rollback", 4).await;
    register_test_job_definition(&pool, JOB_TYPE).await;
    let failing_payload = json!({"event": "rollback-enqueued-event"});
    let failing_intent = JobEnqueueIntent::new(
        JobType::new(JOB_TYPE),
        &failing_payload,
        "event-rollback-key",
    );
    let failing = record_job_enqueue_intent(&pool, &failing_intent)
        .await
        .expect("record intent before forced event failure");
    let healthy_payload = json!({"event": "healthy-enqueued-event"});
    let healthy_intent = JobEnqueueIntent::new(
        JobType::new(JOB_TYPE),
        &healthy_payload,
        "event-healthy-key",
    );
    let healthy = record_job_enqueue_intent(&pool, &healthy_intent)
        .await
        .expect("record healthy intent after forced event failure candidate");

    sqlx::query(
        "CREATE FUNCTION fail_enqueue_event_for_intent_test()
         RETURNS trigger
         LANGUAGE plpgsql
         AS $$
         BEGIN
             IF NEW.event_type = 'ENQUEUED'::job_event_type
                AND (
                    SELECT payload ->> 'event'
                    FROM job_queue
                    WHERE id = NEW.job_id
                ) = 'rollback-enqueued-event'
             THEN
                 RAISE EXCEPTION 'forced enqueue event failure';
             END IF;
             RETURN NEW;
         END;
         $$",
    )
    .execute(&pool)
    .await
    .expect("create failing enqueue event function");
    sqlx::query(
        "CREATE TRIGGER trg_fail_enqueue_event_for_intent_test
         BEFORE INSERT ON job_events
         FOR EACH ROW
         EXECUTE FUNCTION fail_enqueue_event_for_intent_test()",
    )
    .execute(&pool)
    .await
    .expect("create failing enqueue event trigger");

    let report = promote_job_enqueue_intents_for_types(&pool, &[JobType::new(JOB_TYPE)], 10)
        .await
        .expect("promotion should defer the failed row and continue");
    assert_eq!(report.retry_deferred, 1);
    assert_eq!(report.inserted_jobs, 1);
    assert_eq!(report.total_promoted, 1);
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM job_queue WHERE idempotency_key = 'event-rollback-key'"
        )
        .fetch_one(&pool)
        .await
        .expect("count rolled-back queue rows"),
        0
    );
    let failing = get_job_enqueue_intent_by_id(&pool, None, failing.intent_id)
        .await
        .expect("load deferred intent")
        .expect("deferred intent remains");
    assert_eq!(failing.status, JobEnqueueIntentStatus::Pending);
    assert_eq!(failing.promotion_attempts, 1);
    assert!(
        failing.next_promotion_at
            > failing
                .last_attempted_at
                .expect("deferred intent records its attempt time")
    );
    assert!(failing.last_error_code.is_some());
    let metrics = get_job_enqueue_intent_metrics(
        &pool,
        &JobEnqueueIntentMetricsFilter::new(10, 0).with_job_type(JobType::new(JOB_TYPE)),
    )
    .await
    .expect("read retrying intent metrics");
    assert_eq!(metrics[0].retrying_count, 1);
    assert_eq!(metrics[0].max_promotion_attempts, 1);
    assert_eq!(
        get_job_enqueue_intent_by_id(&pool, None, healthy.intent_id)
            .await
            .expect("load healthy intent")
            .expect("healthy intent remains")
            .status,
        JobEnqueueIntentStatus::Promoted
    );

    assert_eq!(
        promote_job_enqueue_intents_for_types(&pool, &[JobType::new(JOB_TYPE)], 10)
            .await
            .expect("immediate retry should skip deferred intent"),
        Default::default()
    );

    sqlx::query("DROP TRIGGER trg_fail_enqueue_event_for_intent_test ON job_events")
        .execute(&pool)
        .await
        .expect("drop failing enqueue event trigger");
    sqlx::query("UPDATE job_enqueue_intents SET next_promotion_at = now() WHERE id = $1")
        .bind(failing.id)
        .execute(&pool)
        .await
        .expect("make deferred intent eligible after fixing database failure");
    let recovered = promote_job_enqueue_intents_for_types(&pool, &[JobType::new(JOB_TYPE)], 10)
        .await
        .expect("retry deferred intent after database repair");
    assert_eq!(recovered.inserted_jobs, 1);
    let failing = get_job_enqueue_intent_by_id(&pool, None, failing.id)
        .await
        .expect("load recovered intent")
        .expect("recovered intent remains");
    assert_eq!(failing.status, JobEnqueueIntentStatus::Promoted);
    assert_eq!(failing.promotion_attempts, 2);
    assert!(failing.last_error_code.is_none());

    teardown_ephemeral_pool(pool, database).await;
}
