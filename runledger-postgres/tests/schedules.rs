use chrono::{DateTime, Utc};
use runledger_core::jobs::JobType;
use runledger_postgres::jobs::{
    JobDefinitionUpsert, JobScheduleUpsert, set_job_schedule_active, upsert_job_definition_tx,
    upsert_job_schedule,
};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;

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
