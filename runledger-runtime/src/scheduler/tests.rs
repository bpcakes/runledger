use std::cell::Cell;
use std::str::FromStr;
use std::time::Duration as StdDuration;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use cron::Schedule;
use runledger_core::jobs::JobType;
use runledger_postgres::jobs::{claim_due_schedules_tx, upsert_job_definition_tx};
use serde_json::json;
use sqlx::types::Uuid;
use tokio::sync::watch;
use tokio::time::{sleep, timeout};

use super::{
    SCHEDULE_STALE_CATCHUP_JITTER_SEARCH_LIMIT, apply_schedule_jitter, compute_next_fire_at_utc,
    compute_next_fire_at_with_stale_coalescing_utc, first_jittered_fire_after_candidates_utc,
    materialize_claimed_schedules_tx, materialize_due_schedules, materialize_due_schedules_tx,
    run_scheduler_loop,
};
use crate::config::JobsConfig;
use crate::{Error, RuntimeLoopExit, SchedulerError};
use runledger_test_support::{
    setup_ephemeral_pool_with_untracked_migrations as setup_ephemeral_pool, teardown_ephemeral_pool,
};

fn scheduler_loop_test_config() -> JobsConfig {
    JobsConfig {
        worker_id: "scheduler-loop-test-worker".to_string(),
        poll_interval: StdDuration::from_millis(25),
        claim_batch_size: 1,
        lease_ttl_seconds: 10,
        max_global_concurrency: 1,
        reaper_interval: StdDuration::from_millis(25),
        schedule_poll_interval: StdDuration::from_secs(60),
        reaper_retry_delay_ms: 1_000,
    }
}

fn reference_first_jittered_fire_after_utc(
    schedule: &Schedule,
    from: DateTime<Utc>,
    after: DateTime<Utc>,
    schedule_id: Uuid,
    max_jitter_seconds: i32,
) -> Option<DateTime<Utc>> {
    reference_first_jittered_fire_after_candidates_utc(
        schedule
            .after(&from)
            .take(SCHEDULE_STALE_CATCHUP_JITTER_SEARCH_LIMIT),
        after,
        schedule_id,
        max_jitter_seconds,
    )
}

fn reference_first_jittered_fire_after_candidates_utc(
    candidates: impl Iterator<Item = DateTime<Utc>>,
    after: DateTime<Utc>,
    schedule_id: Uuid,
    max_jitter_seconds: i32,
) -> Option<DateTime<Utc>> {
    candidates
        .map(|next| apply_schedule_jitter(schedule_id, next, max_jitter_seconds))
        .filter(|next| *next > after)
        .min()
}

#[test]
fn compute_next_fire_at_utc_applies_deterministic_jitter() {
    let from = DateTime::parse_from_rfc3339("2026-05-26T12:00:00Z")
        .expect("fixed from timestamp")
        .with_timezone(&Utc);
    let schedule_id =
        Uuid::parse_str("018fa1f8-0000-7000-8000-000000000123").expect("fixed schedule id");
    let expected = DateTime::parse_from_rfc3339("2026-05-26T12:01:03Z")
        .expect("fixed expected timestamp")
        .with_timezone(&Utc);

    let first =
        compute_next_fire_at_utc("0 * * * * *", from, schedule_id, 30).expect("jittered fire time");
    let second =
        compute_next_fire_at_utc("0 * * * * *", from, schedule_id, 30).expect("jittered fire time");
    let unjittered = compute_next_fire_at_utc("0 * * * * *", from, schedule_id, 0)
        .expect("unjittered fire time");

    assert_eq!(
        first, second,
        "jitter must be stable for the same schedule id and base fire time"
    );
    assert_eq!(first, expected, "jitter derivation should remain stable");
    assert_ne!(
        first, unjittered,
        "nonzero jitter must move the next fire time for this fixture"
    );
}

#[test]
fn compute_next_fire_at_with_stale_coalescing_moves_backlog_cursor_after_now() {
    let scheduled_for = DateTime::parse_from_rfc3339("2026-02-01T12:00:00Z")
        .expect("fixed scheduled_for timestamp")
        .with_timezone(&Utc);
    let now = DateTime::parse_from_rfc3339("2026-02-01T12:05:00Z")
        .expect("fixed now timestamp")
        .with_timezone(&Utc);
    let expected = DateTime::parse_from_rfc3339("2026-02-01T12:06:00Z")
        .expect("fixed expected timestamp")
        .with_timezone(&Utc);

    let next = compute_next_fire_at_with_stale_coalescing_utc(
        "0 * * * * * *",
        scheduled_for,
        now,
        Uuid::nil(),
        0,
    )
    .expect("coalesced next fire");

    assert_eq!(
        next, expected,
        "stale schedules should replay one due fire, then move the cursor after now"
    );
}

#[test]
fn compute_next_fire_at_with_stale_coalescing_keeps_future_jittered_fire() {
    let scheduled_for = DateTime::parse_from_rfc3339("2026-02-01T12:00:00Z")
        .expect("fixed scheduled_for timestamp")
        .with_timezone(&Utc);
    let now = DateTime::parse_from_rfc3339("2026-02-01T12:05:30Z")
        .expect("fixed now timestamp")
        .with_timezone(&Utc);
    // Uuid::nil() with this base fire and 60s max jitter yields +54s.
    let expected = DateTime::parse_from_rfc3339("2026-02-01T12:05:54Z")
        .expect("fixed expected timestamp")
        .with_timezone(&Utc);

    let next = compute_next_fire_at_with_stale_coalescing_utc(
        "0 * * * * * *",
        scheduled_for,
        now,
        Uuid::nil(),
        60,
    )
    .expect("coalesced next fire");

    assert_eq!(
        next, expected,
        "coalescing should keep a cron base before now when jitter moves it into the future"
    );
}

#[test]
fn compute_next_fire_at_with_stale_coalescing_chooses_earliest_jittered_fire() {
    let scheduled_for = DateTime::parse_from_rfc3339("2026-02-01T11:59:50Z")
        .expect("fixed scheduled_for timestamp")
        .with_timezone(&Utc);
    let now = DateTime::parse_from_rfc3339("2026-02-01T12:00:02Z")
        .expect("fixed now timestamp")
        .with_timezone(&Utc);
    // Uuid::nil() with 5s max jitter reorders nearby bases:
    // 12:00:01 -> 12:00:05, while 12:00:03 -> 12:00:03.
    let expected = DateTime::parse_from_rfc3339("2026-02-01T12:00:03Z")
        .expect("fixed expected timestamp")
        .with_timezone(&Utc);

    let next = compute_next_fire_at_with_stale_coalescing_utc(
        "*/1 * * * * * *",
        scheduled_for,
        now,
        Uuid::nil(),
        5,
    )
    .expect("coalesced next fire");

    assert_eq!(
        next, expected,
        "coalescing should pick the earliest actual future fire when jitter reorders cron bases"
    );
}

#[test]
fn bounded_stale_jitter_search_matches_reference_property_matrix() {
    let base = DateTime::parse_from_rfc3339("2026-02-01T12:00:00Z")
        .expect("fixed matrix timestamp")
        .with_timezone(&Utc);
    let schedule_ids = [
        Uuid::nil(),
        Uuid::from_u128(1),
        Uuid::from_u128(u128::MAX / 3),
        Uuid::from_u128(u128::MAX),
    ];

    for gap_seconds in [1, 2, 7, 60] {
        for schedule_id in schedule_ids {
            for max_jitter_seconds in [1, 2, 30, 60, 300, 3_600] {
                for after_offset_seconds in [0, 1, 29, 59, 60, 3_599] {
                    let after = base + ChronoDuration::seconds(after_offset_seconds);
                    let from = after - ChronoDuration::seconds(i64::from(max_jitter_seconds) + 1);
                    let candidates = (1..=512)
                        .map(|index| from + ChronoDuration::seconds(index * gap_seconds))
                        .collect::<Vec<_>>();

                    assert_eq!(
                        first_jittered_fire_after_candidates_utc(
                            candidates.iter().copied(),
                            after,
                            schedule_id,
                            max_jitter_seconds,
                        ),
                        reference_first_jittered_fire_after_candidates_utc(
                            candidates.into_iter(),
                            after,
                            schedule_id,
                            max_jitter_seconds,
                        ),
                        "optimized search diverged for gap={gap_seconds}, schedule_id={schedule_id}, jitter={max_jitter_seconds}, offset={after_offset_seconds}"
                    );
                }
            }
        }
    }
}

#[test]
fn maximum_jitter_sparse_backlog_stops_at_the_best_candidate_bound() {
    let after = DateTime::parse_from_rfc3339("2026-02-01T12:00:00Z")
        .expect("fixed maximum-jitter timestamp")
        .with_timezone(&Utc);
    let max_jitter_seconds = runledger_postgres::jobs::JOB_SCHEDULE_MAX_JITTER_SECONDS;
    let from = after - ChronoDuration::seconds(i64::from(max_jitter_seconds) + 1);
    let schedule = Schedule::from_str("0 * * * * * *").expect("per-minute schedule must parse");
    let scanned = Cell::new(0);
    let candidates = schedule
        .after(&from)
        .take(SCHEDULE_STALE_CATCHUP_JITTER_SEARCH_LIMIT)
        .inspect(|_| scanned.set(scanned.get() + 1));

    let actual = first_jittered_fire_after_candidates_utc(
        candidates,
        after,
        Uuid::nil(),
        max_jitter_seconds,
    );

    assert_eq!(
        actual,
        reference_first_jittered_fire_after_utc(
            &schedule,
            from,
            after,
            Uuid::nil(),
            max_jitter_seconds,
        )
    );
    assert!(
        scanned.get() < SCHEDULE_STALE_CATCHUP_JITTER_SEARCH_LIMIT / 10,
        "sparse maximum-jitter backlog should stop near the best candidate, scanned {} of {}",
        scanned.get(),
        SCHEDULE_STALE_CATCHUP_JITTER_SEARCH_LIMIT
    );
}

#[test]
fn maximum_jitter_dense_backlog_retains_the_global_occurrence_cap() {
    let after = DateTime::parse_from_rfc3339("2026-02-01T12:00:00Z")
        .expect("fixed dense-backlog timestamp")
        .with_timezone(&Utc);
    let max_jitter_seconds = runledger_postgres::jobs::JOB_SCHEDULE_MAX_JITTER_SECONDS;
    let from = after - ChronoDuration::seconds(i64::from(max_jitter_seconds) + 1);
    let schedule = Schedule::from_str("*/1 * * * * * *").expect("per-second schedule must parse");
    let scanned = Cell::new(0);
    let candidates = schedule
        .after(&from)
        .take(SCHEDULE_STALE_CATCHUP_JITTER_SEARCH_LIMIT)
        .inspect(|_| scanned.set(scanned.get() + 1));

    let actual = first_jittered_fire_after_candidates_utc(
        candidates,
        after,
        Uuid::nil(),
        max_jitter_seconds,
    );

    assert_eq!(
        actual,
        reference_first_jittered_fire_after_utc(
            &schedule,
            from,
            after,
            Uuid::nil(),
            max_jitter_seconds,
        )
    );
    assert_eq!(
        scanned.get(),
        SCHEDULE_STALE_CATCHUP_JITTER_SEARCH_LIMIT,
        "the defensive cap must still bound a dense pathological backlog"
    );
}

#[tokio::test]
async fn materialize_claimed_schedules_rolls_back_enqueue_when_schedule_missing() {
    let (pool, database) = setup_ephemeral_pool("jobs_sched_claimed_missing_rollback", 8).await;

    let mut setup_tx = pool.begin().await.expect("begin setup tx");
    upsert_job_definition_tx(
        &mut setup_tx,
        &runledger_postgres::jobs::JobDefinitionUpsert {
            job_type: JobType::new("jobs.claimed.missing"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert claimed-missing definition");
    setup_tx.commit().await.expect("commit setup tx");

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
    .bind("claimed-missing")
    .bind("jobs.claimed.missing")
    .bind::<Option<Uuid>>(None)
    .bind(json!({"kind": "claimed-missing"}))
    .bind("*/1 * * * * * *")
    .bind(now - ChronoDuration::minutes(5))
    .execute(&pool)
    .await
    .expect("insert schedule");

    let mut claim_tx = pool.begin().await.expect("begin claim tx");
    let schedules = claim_due_schedules_tx(&mut claim_tx, now, 1)
        .await
        .expect("claim due schedule");
    claim_tx.commit().await.expect("commit claim tx");

    assert_eq!(schedules.len(), 1, "expected one claimed schedule");
    let schedule_id = schedules[0].id;

    sqlx::query("DELETE FROM job_schedules WHERE id = $1")
        .bind(schedule_id)
        .execute(&pool)
        .await
        .expect("delete claimed schedule");

    let mut materialize_tx = pool.begin().await.expect("begin materialize tx");
    let error = materialize_claimed_schedules_tx(&mut materialize_tx, now, schedules)
        .await
        .expect_err("missing claimed schedule should fail materialization");
    match error {
        Error::Scheduler(SchedulerError::ClaimedScheduleMissing {
            schedule_id: actual_schedule_id,
            operation,
        }) => {
            assert_eq!(actual_schedule_id, schedule_id);
            assert_eq!(operation, "marking schedule as fired");
        }
        other => panic!("expected claimed schedule missing error, got {other:?}"),
    }
    materialize_tx
        .commit()
        .await
        .expect("commit materialization tx after savepoint rollback");

    let queued_jobs = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint
         FROM job_queue
         WHERE job_type = $1",
    )
    .bind("jobs.claimed.missing")
    .fetch_one(&pool)
    .await
    .expect("count enqueued jobs");
    assert_eq!(
        queued_jobs, 0,
        "savepoint rollback should discard the enqueue for the missing schedule"
    );

    let schedule_rows = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*)::bigint
         FROM job_schedules
         WHERE id = $1",
    )
    .bind(schedule_id)
    .fetch_one(&pool)
    .await
    .expect("count schedule rows");
    assert_eq!(
        schedule_rows, 0,
        "test fixture should keep schedule missing"
    );

    teardown_ephemeral_pool(pool, database).await;
}

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
        .after(&from)
        .next()
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
async fn run_scheduler_loop_shutdown_is_bounded_behind_schedule_guard_lock() {
    let (pool, database) = setup_ephemeral_pool("jobs_sched_shutdown_guard_lock", 4).await;

    let mut blocker = pool.begin().await.expect("begin blocker transaction");
    sqlx::query("LOCK TABLE job_schedules IN SHARE ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold conflicting active schedule guard lock");

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let scheduler_task = tokio::spawn(run_scheduler_loop(
        pool.clone(),
        scheduler_loop_test_config(),
        shutdown_rx,
    ));

    sleep(StdDuration::from_millis(100)).await;
    shutdown_tx
        .send(true)
        .expect("scheduler shutdown receiver should be alive");

    let exit = timeout(StdDuration::from_secs(3), scheduler_task)
        .await
        .expect("scheduler should return after bounded due-schedule claim lock wait")
        .expect("scheduler task should not panic");

    blocker.rollback().await.expect("release blocker lock");
    assert_eq!(exit, RuntimeLoopExit::Shutdown);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn materialize_due_schedules_includes_scheduled_for_metadata() {
    let (pool, database) = setup_ephemeral_pool("jobs_sched_metadata", 8).await;
    const SCHEDULED_FOR_RFC3339: &str = "2026-02-01T00:00:00Z";

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &runledger_postgres::jobs::JobDefinitionUpsert {
            job_type: JobType::new("jobs.schedule.metadata"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert metadata definition");
    tx.commit().await.expect("commit tx");

    let scheduled_for = DateTime::parse_from_rfc3339(SCHEDULED_FOR_RFC3339)
        .expect("fixed scheduled_for")
        .with_timezone(&Utc);
    let schedule_id = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO job_schedules (
            name,
            job_type,
            organization_id,
            payload_template,
            cron_expr,
            next_fire_at
         )
         VALUES ($1, $2, $3, $4::jsonb, $5, $6)
         RETURNING id",
    )
    .bind("metadata-schedule")
    .bind("jobs.schedule.metadata")
    .bind::<Option<Uuid>>(None)
    .bind(json!({"kind": "metadata"}))
    .bind("0 0 0 * * * *")
    .bind(scheduled_for)
    .fetch_one(&pool)
    .await
    .expect("insert metadata schedule");

    materialize_due_schedules(&pool, 10)
        .await
        .expect("due schedules materialization");

    let payload = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT payload
         FROM job_queue
         WHERE job_type = $1",
    )
    .bind("jobs.schedule.metadata")
    .fetch_one(&pool)
    .await
    .expect("load enqueued payload");

    assert_eq!(payload["kind"], json!("metadata"));
    assert_eq!(payload["_schedule"]["schedule_id"], json!(schedule_id));
    assert_eq!(
        payload["_schedule"]["schedule_name"],
        json!("metadata-schedule")
    );
    assert_eq!(
        payload["_schedule"]["scheduled_for"],
        json!(SCHEDULED_FOR_RFC3339)
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn materialize_due_schedules_coalesces_stale_cursor_after_one_catchup() {
    let (pool, database) = setup_ephemeral_pool("jobs_sched_stale_cursor_catchup", 8).await;
    const FIRST_FIRE_RFC3339: &str = "2026-02-01T12:00:00Z";
    const NEXT_FUTURE_FIRE_RFC3339: &str = "2026-02-01T12:06:00Z";

    let scheduler_now = DateTime::parse_from_rfc3339("2026-02-01T12:05:00Z")
        .expect("fixed scheduler now")
        .with_timezone(&Utc);
    let first_fire = DateTime::parse_from_rfc3339(FIRST_FIRE_RFC3339)
        .expect("fixed first fire")
        .with_timezone(&Utc);
    let next_future_fire = DateTime::parse_from_rfc3339(NEXT_FUTURE_FIRE_RFC3339)
        .expect("fixed next future fire")
        .with_timezone(&Utc);

    let mut tx = pool.begin().await.expect("begin tx");
    upsert_job_definition_tx(
        &mut tx,
        &runledger_postgres::jobs::JobDefinitionUpsert {
            job_type: JobType::new("jobs.schedule.catchup"),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 60,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert catchup definition");
    tx.commit().await.expect("commit tx");

    let schedule_id = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO job_schedules (
            name,
            job_type,
            organization_id,
            payload_template,
            cron_expr,
            next_fire_at
         )
         VALUES ($1, $2, $3, $4::jsonb, $5, $6)
         RETURNING id",
    )
    .bind("catchup-schedule")
    .bind("jobs.schedule.catchup")
    .bind::<Option<Uuid>>(None)
    .bind(json!({"kind": "catchup"}))
    .bind("0 * * * * * *")
    .bind(first_fire)
    .fetch_one(&pool)
    .await
    .expect("insert catchup schedule");

    let mut materialize_tx = pool.begin().await.expect("begin first materialize tx");
    materialize_due_schedules_tx(&mut materialize_tx, scheduler_now, 10)
        .await
        .expect("first due schedules materialization");
    materialize_tx
        .commit()
        .await
        .expect("commit first materialize tx");

    let next_fire_after_first = sqlx::query_scalar::<_, DateTime<Utc>>(
        "SELECT next_fire_at
         FROM job_schedules
         WHERE id = $1",
    )
    .bind(schedule_id)
    .fetch_one(&pool)
    .await
    .expect("load catchup schedule after first pass");
    assert_eq!(
        next_fire_after_first, next_future_fire,
        "stale schedule should coalesce its cursor to the next occurrence after scheduler now"
    );
    assert!(
        next_fire_after_first > scheduler_now,
        "stale schedule should not remain due after one catch-up materialization"
    );

    let first_payload = sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT payload
         FROM job_queue
         WHERE job_type = $1",
    )
    .bind("jobs.schedule.catchup")
    .fetch_one(&pool)
    .await
    .expect("load first catchup payload");
    assert_eq!(
        first_payload["_schedule"]["scheduled_for"],
        json!(FIRST_FIRE_RFC3339)
    );

    let mut materialize_tx = pool.begin().await.expect("begin second materialize tx");
    materialize_due_schedules_tx(&mut materialize_tx, scheduler_now, 10)
        .await
        .expect("second due schedules materialization");
    materialize_tx
        .commit()
        .await
        .expect("commit second materialize tx");

    let next_fire_after_second = sqlx::query_scalar::<_, DateTime<Utc>>(
        "SELECT next_fire_at
         FROM job_schedules
         WHERE id = $1",
    )
    .bind(schedule_id)
    .fetch_one(&pool)
    .await
    .expect("load catchup schedule after second pass");
    assert_eq!(next_fire_after_second, next_future_fire);

    let scheduled_for_values = sqlx::query_scalar::<_, String>(
        "SELECT payload #>> '{_schedule,scheduled_for}'
         FROM job_queue
         WHERE job_type = $1
         ORDER BY payload #>> '{_schedule,scheduled_for}'",
    )
    .bind("jobs.schedule.catchup")
    .fetch_all(&pool)
    .await
    .expect("load catchup scheduled_for values");
    assert_eq!(scheduled_for_values, vec![FIRST_FIRE_RFC3339.to_string()]);

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
