use super::*;
use chrono::Duration as ChronoDuration;
use runledger_core::jobs::JobType;
use runledger_postgres::jobs::upsert_job_definition_tx;
use serde_json::json;
use sqlx::types::Uuid;

use crate::test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};

#[tokio::test]
async fn materialize_due_schedules_ignores_disabled_job_definition() {
    let (pool, database) = setup_ephemeral_pool("jobs_sched_ignores_disabled", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &runledger_postgres::jobs::JobDefinitionUpsert {
            job_type: JobType::new("jobs.enabled"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert enabled definition");
    upsert_job_definition_tx(
        &mut tx,
        &runledger_postgres::jobs::JobDefinitionUpsert {
            job_type: JobType::new("jobs.disabled"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: false,
        },
    )
    .await
    .expect("upsert disabled definition");
    tx.commit().await.expect("commit tx");

    let good_next_fire_at = Utc::now() - ChronoDuration::minutes(10);
    let bad_next_fire_at = Utc::now() - ChronoDuration::minutes(5);

    sqlx::query(
        "INSERT INTO job_schedules (
            name,
            job_type,
            organization_id,
            payload_template,
            cron_expr,
            next_fire_at
         )
         VALUES ($1, $2, $3, $4::jsonb, $5, $6)",
    )
    .bind("good-schedule")
    .bind("jobs.enabled")
    .bind::<Option<Uuid>>(None)
    .bind(json!({"kind": "good"}))
    .bind("*/1 * * * * * *")
    .bind(good_next_fire_at)
    .execute(&pool)
    .await
    .expect("insert good schedule");

    sqlx::query(
        "INSERT INTO job_schedules (
            name,
            job_type,
            organization_id,
            payload_template,
            cron_expr,
            next_fire_at
         )
         VALUES ($1, $2, $3, $4::jsonb, $5, $6)",
    )
    .bind("bad-schedule")
    .bind("jobs.disabled")
    .bind::<Option<Uuid>>(None)
    .bind(json!({"kind": "bad"}))
    .bind("*/1 * * * * * *")
    .bind(bad_next_fire_at)
    .execute(&pool)
    .await
    .expect("insert bad schedule");

    materialize_due_schedules(&pool, 10)
        .await
        .expect("due schedules materialization");

    let queued_jobs = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint
         FROM job_queue
         WHERE job_type = $1",
    )
    .bind("jobs.enabled")
    .fetch_one(&pool)
    .await
    .expect("count enqueued jobs");
    assert_eq!(queued_jobs, 1);

    let good_next_fire_after = sqlx::query_scalar::<_, DateTime<Utc>>(
        "SELECT next_fire_at
         FROM job_schedules
         WHERE name = $1",
    )
    .bind("good-schedule")
    .fetch_one(&pool)
    .await
    .expect("load good schedule");
    assert!(good_next_fire_after > good_next_fire_at);

    let bad_next_fire_after = sqlx::query_scalar::<_, DateTime<Utc>>(
        "SELECT next_fire_at
         FROM job_schedules
         WHERE name = $1",
    )
    .bind("bad-schedule")
    .fetch_one(&pool)
    .await
    .expect("load bad schedule");
    assert!(
        bad_next_fire_after > bad_next_fire_at,
        "disabled schedule should be deferred after a failed materialization attempt"
    );

    let from = Utc::now();
    let jittered_next = compute_next_fire_at_utc("*/1 * * * * * *", from, Uuid::nil(), 30)
        .expect("jittered schedule");
    let base_schedule = Schedule::from_str("*/1 * * * * * *").expect("schedule parse");
    let base_next = base_schedule
        .upcoming(Utc)
        .find(|next| *next > from)
        .expect("base next schedule");
    assert!(jittered_next >= base_next);
    assert!(jittered_next <= base_next + ChronoDuration::seconds(30));
    assert_eq!(
        compute_next_fire_at_utc("*/1 * * * * * *", from, Uuid::nil(), 0)
            .expect("non jittered schedule"),
        base_next
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn materialize_due_schedules_skips_invalid_cron_without_enqueuing() {
    let (pool, database) = setup_ephemeral_pool("jobs_sched_skips_invalid_cron", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &runledger_postgres::jobs::JobDefinitionUpsert {
            job_type: JobType::new("jobs.valid.cron"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert valid-cron definition");
    upsert_job_definition_tx(
        &mut tx,
        &runledger_postgres::jobs::JobDefinitionUpsert {
            job_type: JobType::new("jobs.invalid.cron"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert invalid-cron definition");
    tx.commit().await.expect("commit tx");

    let valid_next_fire_at = Utc::now() - ChronoDuration::minutes(10);
    let invalid_next_fire_at = Utc::now() - ChronoDuration::minutes(5);

    sqlx::query(
        "INSERT INTO job_schedules (
            name,
            job_type,
            organization_id,
            payload_template,
            cron_expr,
            next_fire_at
         )
         VALUES ($1, $2, $3, $4::jsonb, $5, $6)",
    )
    .bind("valid-cron-schedule")
    .bind("jobs.valid.cron")
    .bind::<Option<Uuid>>(None)
    .bind(json!({"kind": "valid"}))
    .bind("*/1 * * * * * *")
    .bind(valid_next_fire_at)
    .execute(&pool)
    .await
    .expect("insert valid cron schedule");

    sqlx::query(
        "INSERT INTO job_schedules (
            name,
            job_type,
            organization_id,
            payload_template,
            cron_expr,
            next_fire_at
         )
         VALUES ($1, $2, $3, $4::jsonb, $5, $6)",
    )
    .bind("invalid-cron-schedule")
    .bind("jobs.invalid.cron")
    .bind::<Option<Uuid>>(None)
    .bind(json!({"kind": "invalid"}))
    .bind("this is not a cron expression")
    .bind(invalid_next_fire_at)
    .execute(&pool)
    .await
    .expect("insert invalid cron schedule");

    materialize_due_schedules(&pool, 10)
        .await
        .expect("due schedules materialization");

    let valid_enqueued_jobs = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint
         FROM job_queue
         WHERE job_type = $1",
    )
    .bind("jobs.valid.cron")
    .fetch_one(&pool)
    .await
    .expect("count valid enqueued jobs");
    assert_eq!(valid_enqueued_jobs, 1);

    let invalid_enqueued_jobs = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint
         FROM job_queue
         WHERE job_type = $1",
    )
    .bind("jobs.invalid.cron")
    .fetch_one(&pool)
    .await
    .expect("count invalid enqueued jobs");
    assert_eq!(invalid_enqueued_jobs, 0);

    let invalid_next_fire_after = sqlx::query_scalar::<_, DateTime<Utc>>(
        "SELECT next_fire_at
         FROM job_schedules
         WHERE name = $1",
    )
    .bind("invalid-cron-schedule")
    .fetch_one(&pool)
    .await
    .expect("load invalid cron schedule");
    assert!(
        invalid_next_fire_after > invalid_next_fire_at,
        "invalid-cron schedule should be deferred after a failed materialization attempt"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn materialize_due_schedules_releases_savepoint_after_failed_materialization() {
    let (pool, database) = setup_ephemeral_pool("jobs_sched_releases_savepoint", 8).await;

    let mut setup_tx = pool.begin().await.expect("begin setup tx");
    upsert_job_definition_tx(
        &mut setup_tx,
        &runledger_postgres::jobs::JobDefinitionUpsert {
            job_type: JobType::new("jobs.savepoint.regression"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert savepoint regression definition");
    setup_tx.commit().await.expect("commit setup tx");

    let invalid_next_fire_at = Utc::now() - ChronoDuration::minutes(5);
    sqlx::query(
        "INSERT INTO job_schedules (
            name,
            job_type,
            organization_id,
            payload_template,
            cron_expr,
            next_fire_at
         )
         VALUES ($1, $2, $3, $4::jsonb, $5, $6)",
    )
    .bind("savepoint-regression-invalid-cron")
    .bind("jobs.savepoint.regression")
    .bind::<Option<Uuid>>(None)
    .bind(json!({"kind": "invalid"}))
    .bind("this is not a cron expression")
    .bind(invalid_next_fire_at)
    .execute(&pool)
    .await
    .expect("insert invalid cron schedule");

    let mut tx = pool.begin().await.expect("begin tx");
    materialize_due_schedules_tx(&mut tx, Utc::now(), 10)
        .await
        .expect("due schedules materialization in tx");

    let release_error = sqlx::query("RELEASE SAVEPOINT materialize_due_schedule")
        .execute(&mut *tx)
        .await
        .expect_err("failure path should release savepoint after rollback");
    let release_error_code = release_error
        .as_database_error()
        .and_then(|error| error.code().map(|code| code.to_string()));
    assert_eq!(
        release_error_code.as_deref(),
        Some("3B001"),
        "unexpected release error: {release_error}"
    );

    tx.rollback().await.expect("rollback tx");
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn materialize_due_schedules_defers_failures_to_avoid_starving_valid_work() {
    let (pool, database) = setup_ephemeral_pool("jobs_sched_defers_failures", 8).await;

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &runledger_postgres::jobs::JobDefinitionUpsert {
            job_type: JobType::new("jobs.starve.valid"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert valid definition");
    upsert_job_definition_tx(
        &mut tx,
        &runledger_postgres::jobs::JobDefinitionUpsert {
            job_type: JobType::new("jobs.starve.disabled"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: false,
        },
    )
    .await
    .expect("upsert disabled definition");
    tx.commit().await.expect("commit tx");

    let now = Utc::now();
    sqlx::query(
        "INSERT INTO job_schedules (
            name,
            job_type,
            organization_id,
            payload_template,
            cron_expr,
            next_fire_at
         )
         VALUES ($1, $2, $3, $4::jsonb, $5, $6)",
    )
    .bind("starve-failing-1")
    .bind("jobs.starve.disabled")
    .bind::<Option<Uuid>>(None)
    .bind(json!({"kind":"failing-1"}))
    .bind("*/1 * * * * * *")
    .bind(now - ChronoDuration::minutes(20))
    .execute(&pool)
    .await
    .expect("insert first failing schedule");

    sqlx::query(
        "INSERT INTO job_schedules (
            name,
            job_type,
            organization_id,
            payload_template,
            cron_expr,
            next_fire_at
         )
         VALUES ($1, $2, $3, $4::jsonb, $5, $6)",
    )
    .bind("starve-failing-2")
    .bind("jobs.starve.disabled")
    .bind::<Option<Uuid>>(None)
    .bind(json!({"kind":"failing-2"}))
    .bind("*/1 * * * * * *")
    .bind(now - ChronoDuration::minutes(15))
    .execute(&pool)
    .await
    .expect("insert second failing schedule");

    sqlx::query(
        "INSERT INTO job_schedules (
            name,
            job_type,
            organization_id,
            payload_template,
            cron_expr,
            next_fire_at
         )
         VALUES ($1, $2, $3, $4::jsonb, $5, $6)",
    )
    .bind("starve-valid")
    .bind("jobs.starve.valid")
    .bind::<Option<Uuid>>(None)
    .bind(json!({"kind":"valid"}))
    .bind("*/1 * * * * * *")
    .bind(now - ChronoDuration::minutes(10))
    .execute(&pool)
    .await
    .expect("insert valid schedule");

    materialize_due_schedules(&pool, 2)
        .await
        .expect("first due schedules materialization");

    let valid_jobs_after_first_pass = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint
         FROM job_queue
         WHERE job_type = $1",
    )
    .bind("jobs.starve.valid")
    .fetch_one(&pool)
    .await
    .expect("count valid jobs after first pass");
    assert_eq!(
        valid_jobs_after_first_pass, 0,
        "first pass should be consumed by failing schedules because of batch limit"
    );

    materialize_due_schedules(&pool, 2)
        .await
        .expect("second due schedules materialization");

    let valid_jobs_after_second_pass = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint
         FROM job_queue
         WHERE job_type = $1",
    )
    .bind("jobs.starve.valid")
    .fetch_one(&pool)
    .await
    .expect("count valid jobs after second pass");
    assert_eq!(
        valid_jobs_after_second_pass, 1,
        "valid schedule should be materialized once failing schedules are deferred"
    );

    teardown_ephemeral_pool(pool, database).await;
}
