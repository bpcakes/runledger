use chrono::{DateTime, Utc};
use runledger_core::jobs::JobType;
use runledger_postgres::jobs::{
    JobDefinitionUpsert, JobScheduleUpsert, mark_schedule_fired_tx, set_job_schedule_active,
    upsert_job_definition_tx, upsert_job_schedule,
};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::types::Uuid;

const SCHEDULE_JOB: &str = "jobs.schedule.upsert_state";
const SCHEDULE_NAME: &str = "schedule-upsert-state";

fn fixed_utc(input: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(input)
        .expect("valid fixed timestamp")
        .with_timezone(&Utc)
}

#[tokio::test]
async fn schedule_upsert_returns_active_state_preserved_on_conflict() {
    let (pool, database) = setup_ephemeral_pool("postgres_schedule_upsert_state", 4).await;

    let mut tx = pool.begin().await.expect("begin job definition tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(SCHEDULE_JOB),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 300,
            default_priority: 0,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit job definition tx");

    let first_payload = json!({ "version": 1 });
    let first_next_fire_at = fixed_utc("2026-05-26T12:00:00Z");
    let inserted = upsert_job_schedule(
        &pool,
        &JobScheduleUpsert {
            name: SCHEDULE_NAME,
            job_type: JobType::new(SCHEDULE_JOB),
            organization_id: None,
            payload_template: &first_payload,
            cron_expr: "0 0 * * * *",
            is_active: false,
            next_fire_at: first_next_fire_at,
            max_jitter_seconds: 0,
        },
    )
    .await
    .expect("insert inactive schedule");

    assert!(
        !inserted.is_active,
        "first insert should return requested active state"
    );

    assert!(
        set_job_schedule_active(&pool, SCHEDULE_NAME, true)
            .await
            .expect("activate schedule"),
        "schedule should exist when activating"
    );

    let second_payload = json!({ "version": 2 });
    let second_next_fire_at = fixed_utc("2026-05-26T13:00:00Z");
    let active_after_conflict = upsert_job_schedule(
        &pool,
        &JobScheduleUpsert {
            name: SCHEDULE_NAME,
            job_type: JobType::new(SCHEDULE_JOB),
            organization_id: None,
            payload_template: &second_payload,
            cron_expr: "0 30 * * * *",
            is_active: false,
            next_fire_at: second_next_fire_at,
            max_jitter_seconds: 0,
        },
    )
    .await
    .expect("conflict upsert should preserve active state");

    assert!(
        active_after_conflict.is_active,
        "conflict upsert should report preserved active state, not input state"
    );
    assert_eq!(active_after_conflict.payload_template, second_payload);
    assert_eq!(active_after_conflict.next_fire_at, second_next_fire_at);

    assert!(
        set_job_schedule_active(&pool, SCHEDULE_NAME, false)
            .await
            .expect("pause schedule"),
        "schedule should exist when pausing"
    );

    let third_payload = json!({ "version": 3 });
    let paused_after_conflict = upsert_job_schedule(
        &pool,
        &JobScheduleUpsert {
            name: SCHEDULE_NAME,
            job_type: JobType::new(SCHEDULE_JOB),
            organization_id: None,
            payload_template: &third_payload,
            cron_expr: "0 30 * * * *",
            is_active: true,
            next_fire_at: fixed_utc("2026-05-26T14:00:00Z"),
            max_jitter_seconds: 0,
        },
    )
    .await
    .expect("conflict upsert should preserve paused state");

    assert!(
        !paused_after_conflict.is_active,
        "conflict upsert should expose preserved paused state"
    );
    assert_eq!(paused_after_conflict.payload_template, third_payload);
    assert_eq!(
        paused_after_conflict.next_fire_at, second_next_fire_at,
        "same-cron upsert should not retime the schedule cursor"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn mark_schedule_fired_reports_whether_row_existed() {
    let (pool, database) = setup_ephemeral_pool("postgres_schedule_mark_fired", 4).await;

    let mut tx = pool.begin().await.expect("begin job definition tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(SCHEDULE_JOB),
            version: 1,
            max_attempts: 3,
            default_timeout_seconds: 300,
            default_priority: 0,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert job definition");
    tx.commit().await.expect("commit job definition tx");

    let payload = json!({ "version": 1 });
    let inserted = upsert_job_schedule(
        &pool,
        &JobScheduleUpsert {
            name: "schedule-mark-fired",
            job_type: JobType::new(SCHEDULE_JOB),
            organization_id: None,
            payload_template: &payload,
            cron_expr: "0 0 * * * *",
            is_active: true,
            next_fire_at: fixed_utc("2026-05-26T12:00:00Z"),
            max_jitter_seconds: 0,
        },
    )
    .await
    .expect("insert schedule");

    let fired_at = fixed_utc("2026-05-26T12:00:01Z");
    let next_fire_at = fixed_utc("2026-05-26T13:00:00Z");
    let mut tx = pool.begin().await.expect("begin mark fired tx");
    let existing_updated = mark_schedule_fired_tx(&mut tx, inserted.id, fired_at, next_fire_at)
        .await
        .expect("mark existing schedule fired");
    let missing_id =
        Uuid::parse_str("018fa1f8-0000-7000-8000-000000000999").expect("fixed missing id");
    let missing_updated = mark_schedule_fired_tx(&mut tx, missing_id, fired_at, next_fire_at)
        .await
        .expect("mark missing schedule fired");
    tx.commit().await.expect("commit mark fired tx");

    assert!(
        existing_updated,
        "existing schedule id should report an updated row"
    );
    assert!(
        !missing_updated,
        "missing schedule id should report no updated row"
    );

    let (last_fired_at, stored_next_fire_at): (Option<DateTime<Utc>>, DateTime<Utc>) =
        sqlx::query_as(
            "SELECT last_fired_at, next_fire_at
             FROM job_schedules
             WHERE id = $1",
        )
        .bind(inserted.id)
        .fetch_one(&pool)
        .await
        .expect("load updated schedule cursors");

    assert_eq!(last_fired_at, Some(fired_at));
    assert_eq!(stored_next_fire_at, next_fire_at);

    teardown_ephemeral_pool(pool, database).await;
}
