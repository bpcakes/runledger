use chrono::{DateTime, Utc};
use runledger_core::jobs::{JobType, JobTypeName, WorkflowDagBuilder};
use runledger_postgres::jobs::{
    JobDefinitionCatalogSyncError, JobDefinitionCatalogSyncMode, JobDefinitionUpdate,
    JobDefinitionUpsert, JobEnqueue, JobScheduleCatalogSyncEntry, JobScheduleUpsert,
    claim_due_schedules_tx, deactivate_schedules_absent_from_names_tx, enqueue_job,
    enqueue_workflow_run, get_job_definition_by_type, get_job_schedule_by_name,
    mark_schedule_fired_tx, prepare_schedule_exact_sync_critical_section_tx,
    set_job_schedule_active, sync_catalog_job_definitions_exact_tx,
    sync_catalog_job_definitions_tx, sync_catalog_job_schedules_tx, update_job_definition,
    upsert_job_definition_tx, upsert_job_schedule, upsert_job_schedule_tx,
};
use runledger_postgres::{Error, QueryError, QueryErrorCategory};
use runledger_test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};
use serde_json::json;
use sqlx::types::Uuid;
use tokio::time::{Duration, timeout};

const SCHEDULE_JOB: &str = "jobs.schedule.upsert_state";
const SCHEDULE_NAME: &str = "schedule-upsert-state";
const DEFINITION_DISABLE_JOB: &str = "jobs.definition.disable_guard";
const ENQUEUE_LOCK_JOB: &str = "jobs.definition.enqueue_lock";
const WORKFLOW_LOCK_JOB: &str = "jobs.definition.workflow_lock";

fn fixed_utc(input: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(input)
        .expect("valid fixed timestamp")
        .with_timezone(&Utc)
}

async fn read_schedule_state(
    pool: &runledger_postgres::DbPool,
    name: &str,
) -> (bool, DateTime<Utc>) {
    let schedule = get_job_schedule_by_name(pool, name)
        .await
        .expect("read schedule state")
        .expect("schedule exists");
    (schedule.is_active, schedule.next_fire_at)
}

fn disabled_definition_upsert() -> JobDefinitionUpsert<'static> {
    definition_upsert(DEFINITION_DISABLE_JOB, false)
}

fn definition_upsert(job_type: &'static str, is_enabled: bool) -> JobDefinitionUpsert<'static> {
    JobDefinitionUpsert {
        job_type: JobType::new(job_type),
        version: 1,
        max_attempts: 3,
        default_timeout_seconds: 300,
        default_priority: 0,
        is_enabled,
    }
}

fn assert_definition_sync_validation_error(error: JobDefinitionCatalogSyncError) {
    match error {
        JobDefinitionCatalogSyncError::ValidationFailure(source) => match *source {
            Error::QueryError(query_error) => {
                assert_eq!(query_error.category(), QueryErrorCategory::Validation);
                assert_eq!(query_error.code(), "job_definition.empty_job_type_list");
            }
            other => panic!("expected validation query error, got {other:?}"),
        },
        other => panic!("expected validation query error, got {other:?}"),
    }
}

fn assert_lock_timeout_query_error(query_error: &QueryError) {
    assert_eq!(query_error.category(), QueryErrorCategory::Internal);
    assert_eq!(query_error.sqlstate(), Some("55P03"));
    assert!(
        query_error.source_arc().is_some(),
        "lock timeout should preserve the source sqlx error"
    );
}

fn assert_validation_code(error: Error, expected_code: &str) {
    match error {
        Error::QueryError(query_error) => {
            assert_eq!(query_error.category(), QueryErrorCategory::Validation);
            assert_eq!(query_error.code(), expected_code);
        }
        other => panic!("expected validation query error, got {other:?}"),
    }
}

#[tokio::test]
async fn exact_catalog_definition_sync_rejects_empty_catalog_and_scope() {
    let (pool, database) = setup_ephemeral_pool("postgres_definition_disable_guard", 4).await;

    let mut tx = pool.begin().await.expect("begin job definition tx");
    let scope = [JobTypeName::new(DEFINITION_DISABLE_JOB).expect("valid job type")];
    let empty_catalog_error = sync_catalog_job_definitions_exact_tx(&mut tx, &[], &scope)
        .await
        .expect_err("empty catalog should be rejected");
    assert_definition_sync_validation_error(empty_catalog_error);

    let definition = JobDefinitionUpsert {
        job_type: JobType::new(DEFINITION_DISABLE_JOB),
        version: 1,
        max_attempts: 3,
        default_timeout_seconds: 300,
        default_priority: 0,
        is_enabled: true,
    };
    let empty_scope_error = sync_catalog_job_definitions_exact_tx(&mut tx, &[definition], &[])
        .await
        .expect_err("empty scope should be rejected");
    assert_definition_sync_validation_error(empty_scope_error);
    tx.commit().await.expect("commit definition tx");

    let definition = get_job_definition_by_type(&pool, JobType::new(DEFINITION_DISABLE_JOB))
        .await
        .expect("load job definition");
    assert!(
        definition.is_none(),
        "invalid exact sync should not write definitions"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn definition_disable_schedule_lock_uses_bounded_lock_wait() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_definition_disable_lock_timeout", 4).await;

    let mut blocker = pool.begin().await.expect("begin blocker transaction");
    sqlx::query("LOCK TABLE job_schedules IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold conflicting schedule table lock");

    let mut tx = pool.begin().await.expect("begin lock timeout transaction");
    sqlx::query_scalar::<_, String>("SELECT set_config('lock_timeout', '100ms', true)")
        .fetch_one(&mut *tx)
        .await
        .expect("set stricter test lock timeout");

    let error = timeout(
        Duration::from_secs(2),
        sync_catalog_job_definitions_tx(
            &mut tx,
            &[disabled_definition_upsert()],
            JobDefinitionCatalogSyncMode::RestoreCatalogEnabledState,
        ),
    )
    .await
    .expect("schedule lock timeout should be bounded")
    .expect_err("conflicting schedule lock should time out");
    match error {
        JobDefinitionCatalogSyncError::ScheduleLockFailure(source) => match *source {
            Error::QueryError(query_error) => {
                assert_lock_timeout_query_error(&query_error);
            }
            other => panic!("expected query error, got {other:?}"),
        },
        other => panic!("expected query error, got {other:?}"),
    }

    tx.rollback()
        .await
        .expect("rollback timed-out lock transaction");
    blocker.rollback().await.expect("release blocker lock");
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn definition_disable_definition_lock_uses_bounded_lock_wait() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_definition_disable_definition_lock_timeout", 4).await;

    let mut blocker = pool.begin().await.expect("begin blocker transaction");
    sqlx::query("LOCK TABLE job_definitions IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold conflicting definition table lock");

    let mut tx = pool.begin().await.expect("begin lock timeout transaction");
    sqlx::query_scalar::<_, String>("SELECT set_config('lock_timeout', '100ms', true)")
        .fetch_one(&mut *tx)
        .await
        .expect("set stricter test lock timeout");

    let error = timeout(
        Duration::from_secs(2),
        sync_catalog_job_definitions_tx(
            &mut tx,
            &[disabled_definition_upsert()],
            JobDefinitionCatalogSyncMode::RestoreCatalogEnabledState,
        ),
    )
    .await
    .expect("definition lock timeout should be bounded")
    .expect_err("conflicting definition lock should time out");
    match error {
        JobDefinitionCatalogSyncError::DefinitionLockFailure(source) => match *source {
            Error::QueryError(query_error) => {
                assert_lock_timeout_query_error(&query_error);
            }
            other => panic!("expected query error, got {other:?}"),
        },
        other => panic!("expected query error, got {other:?}"),
    }

    tx.rollback()
        .await
        .expect("rollback timed-out lock transaction");
    blocker.rollback().await.expect("release blocker lock");
    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn active_schedule_upsert_rejects_disabled_job_definition() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_schedule_rejects_disabled_definition", 4).await;

    let mut tx = pool.begin().await.expect("begin disabled definition tx");
    upsert_job_definition_tx(&mut tx, &definition_upsert(DEFINITION_DISABLE_JOB, false))
        .await
        .expect("upsert disabled job definition");
    tx.commit().await.expect("commit disabled definition tx");

    let payload = json!({ "disabled": true });
    let next_fire_at = fixed_utc("2026-05-26T12:00:00Z");
    let active_schedule = JobScheduleUpsert {
        name: "schedule-disabled-definition-active",
        job_type: JobType::new(DEFINITION_DISABLE_JOB),
        organization_id: None,
        payload_template: &payload,
        cron_expr: "0 0 * * * *",
        is_active: true,
        next_fire_at,
        max_jitter_seconds: 0,
    };
    let mut tx = pool.begin().await.expect("begin schedule upsert tx");
    assert_validation_code(
        upsert_job_schedule_tx(&mut tx, &active_schedule)
            .await
            .expect_err("active schedule should require enabled definition"),
        "job_schedule.definition_not_found_or_disabled",
    );
    tx.rollback()
        .await
        .expect("rollback rejected schedule upsert tx");

    let missing = get_job_schedule_by_name(&pool, "schedule-disabled-definition-active")
        .await
        .expect("read rejected schedule");
    assert!(
        missing.is_none(),
        "rejected active schedule should not be persisted"
    );

    let inactive_schedule = JobScheduleUpsert {
        name: "schedule-disabled-definition-inactive",
        is_active: false,
        ..active_schedule
    };
    let inserted = upsert_job_schedule(&pool, &inactive_schedule)
        .await
        .expect("inactive schedule may reference a disabled definition");
    assert!(
        !inserted.is_active,
        "inactive schedule should stay inactive"
    );

    assert_validation_code(
        set_job_schedule_active(&pool, "schedule-disabled-definition-inactive", true)
            .await
            .expect_err("activating schedule should require enabled definition"),
        "job_schedule.definition_not_found_or_disabled",
    );
    let (is_active, _) = read_schedule_state(&pool, "schedule-disabled-definition-inactive").await;
    assert!(
        !is_active,
        "failed activation should leave the schedule inactive"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn update_job_definition_rejects_disable_with_active_schedule() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_definition_update_active_schedule", 4).await;

    let mut tx = pool.begin().await.expect("begin enabled definition tx");
    upsert_job_definition_tx(&mut tx, &definition_upsert(DEFINITION_DISABLE_JOB, true))
        .await
        .expect("upsert enabled job definition");
    tx.commit().await.expect("commit enabled definition tx");

    let payload = json!({ "active": true });
    upsert_job_schedule(
        &pool,
        &JobScheduleUpsert {
            name: "schedule-blocks-definition-disable",
            job_type: JobType::new(DEFINITION_DISABLE_JOB),
            organization_id: None,
            payload_template: &payload,
            cron_expr: "0 0 * * * *",
            is_active: true,
            next_fire_at: fixed_utc("2026-05-26T12:00:00Z"),
            max_jitter_seconds: 0,
        },
    )
    .await
    .expect("insert active schedule");

    let disable = JobDefinitionUpdate {
        max_attempts: None,
        default_timeout_seconds: None,
        default_priority: None,
        is_enabled: Some(false),
    };
    assert_validation_code(
        update_job_definition(&pool, JobType::new(DEFINITION_DISABLE_JOB), &disable)
            .await
            .expect_err("active schedule should block definition disable"),
        "job_definition.active_schedule_exists",
    );

    let definition = get_job_definition_by_type(&pool, JobType::new(DEFINITION_DISABLE_JOB))
        .await
        .expect("load definition")
        .expect("definition exists");
    assert!(
        definition.is_enabled,
        "rejected disable should leave definition enabled"
    );

    assert!(
        set_job_schedule_active(&pool, "schedule-blocks-definition-disable", false)
            .await
            .expect("deactivate schedule"),
        "schedule should exist"
    );
    let disabled = update_job_definition(&pool, JobType::new(DEFINITION_DISABLE_JOB), &disable)
        .await
        .expect("inactive schedule should allow definition disable")
        .expect("definition exists");
    assert!(
        !disabled.is_enabled,
        "definition should be disabled after active schedules are gone"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn public_upsert_job_definition_rejects_disable_with_active_schedule() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_definition_upsert_active_schedule", 4).await;

    let mut tx = pool.begin().await.expect("begin enabled definition tx");
    upsert_job_definition_tx(&mut tx, &definition_upsert(DEFINITION_DISABLE_JOB, true))
        .await
        .expect("upsert enabled job definition");
    tx.commit().await.expect("commit enabled definition tx");

    let payload = json!({ "active": true });
    upsert_job_schedule(
        &pool,
        &JobScheduleUpsert {
            name: "schedule-blocks-definition-upsert-disable",
            job_type: JobType::new(DEFINITION_DISABLE_JOB),
            organization_id: None,
            payload_template: &payload,
            cron_expr: "0 0 * * * *",
            is_active: true,
            next_fire_at: fixed_utc("2026-05-26T12:00:00Z"),
            max_jitter_seconds: 0,
        },
    )
    .await
    .expect("insert active schedule");

    let mut disable_tx = pool.begin().await.expect("begin disabled definition tx");
    assert_validation_code(
        upsert_job_definition_tx(
            &mut disable_tx,
            &definition_upsert(DEFINITION_DISABLE_JOB, false),
        )
        .await
        .expect_err("active schedule should block public definition upsert disable"),
        "job_definition.active_schedule_exists",
    );
    disable_tx
        .rollback()
        .await
        .expect("rollback rejected disable upsert tx");

    let definition = get_job_definition_by_type(&pool, JobType::new(DEFINITION_DISABLE_JOB))
        .await
        .expect("load definition")
        .expect("definition exists");
    assert!(
        definition.is_enabled,
        "rejected public upsert disable should leave definition enabled"
    );

    assert!(
        set_job_schedule_active(&pool, "schedule-blocks-definition-upsert-disable", false)
            .await
            .expect("deactivate schedule"),
        "schedule should exist"
    );

    let mut disable_tx = pool
        .begin()
        .await
        .expect("begin allowed disabled definition tx");
    upsert_job_definition_tx(
        &mut disable_tx,
        &definition_upsert(DEFINITION_DISABLE_JOB, false),
    )
    .await
    .expect("inactive schedule should allow public definition upsert disable");
    disable_tx
        .commit()
        .await
        .expect("commit allowed disable upsert tx");

    let definition = get_job_definition_by_type(&pool, JobType::new(DEFINITION_DISABLE_JOB))
        .await
        .expect("load disabled definition")
        .expect("definition exists");
    assert!(
        !definition.is_enabled,
        "definition should be disabled after active schedules are gone"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn enqueue_job_waits_for_concurrent_definition_disable() {
    let (pool, database) = setup_ephemeral_pool("postgres_enqueue_definition_lock", 4).await;

    let mut seed_tx = pool.begin().await.expect("begin seed tx");
    upsert_job_definition_tx(&mut seed_tx, &definition_upsert(ENQUEUE_LOCK_JOB, true))
        .await
        .expect("upsert job definition");
    seed_tx.commit().await.expect("commit seed tx");

    let mut blocker = pool.begin().await.expect("begin blocker transaction");
    sqlx::query("UPDATE job_definitions SET is_enabled = false WHERE job_type = $1")
        .bind(ENQUEUE_LOCK_JOB)
        .execute(&mut *blocker)
        .await
        .expect("disable definition without commit");

    let enqueue_pool = pool.clone();
    let mut enqueue_task = tokio::spawn(async move {
        let payload = json!({ "locked": true });
        let enqueue = JobEnqueue {
            job_type: JobType::new(ENQUEUE_LOCK_JOB),
            organization_id: None,
            payload: &payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: None,
        };
        enqueue_job(&enqueue_pool, &enqueue).await
    });

    timeout(Duration::from_millis(150), &mut enqueue_task)
        .await
        .expect_err("enqueue should wait for the definition row lock");
    blocker.commit().await.expect("commit definition disable");

    let result = timeout(Duration::from_secs(5), enqueue_task)
        .await
        .expect("enqueue should finish after definition disable commits")
        .expect("enqueue task should not panic");
    assert_validation_code(
        result.expect_err("disabled definition should reject enqueue"),
        "job.definition_not_found_or_disabled",
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn enqueue_workflow_waits_for_concurrent_definition_disable() {
    let (pool, database) = setup_ephemeral_pool("postgres_workflow_definition_lock", 4).await;

    let mut seed_tx = pool.begin().await.expect("begin seed tx");
    upsert_job_definition_tx(&mut seed_tx, &definition_upsert(WORKFLOW_LOCK_JOB, true))
        .await
        .expect("upsert job definition");
    seed_tx.commit().await.expect("commit seed tx");

    let mut blocker = pool.begin().await.expect("begin blocker transaction");
    sqlx::query("UPDATE job_definitions SET is_enabled = false WHERE job_type = $1")
        .bind(WORKFLOW_LOCK_JOB)
        .execute(&mut *blocker)
        .await
        .expect("disable definition without commit");

    let workflow_pool = pool.clone();
    let mut workflow_task = tokio::spawn(async move {
        let metadata = json!({ "locked": true });
        let payload = json!({ "step": true });
        let workflow = WorkflowDagBuilder::new("workflow.definition-lock", &metadata)
            .job("locked-step", WORKFLOW_LOCK_JOB, &payload)
            .expect("workflow step")
            .build()
            .expect("workflow build");
        enqueue_workflow_run(&workflow_pool, &workflow).await
    });

    timeout(Duration::from_millis(150), &mut workflow_task)
        .await
        .expect_err("workflow enqueue should wait for the definition row lock");
    blocker.commit().await.expect("commit definition disable");

    let result = timeout(Duration::from_secs(5), workflow_task)
        .await
        .expect("workflow enqueue should finish after definition disable commits")
        .expect("workflow task should not panic");
    assert_validation_code(
        result.expect_err("disabled definition should reject workflow enqueue"),
        "workflow.definition_not_found_or_disabled",
    );

    teardown_ephemeral_pool(pool, database).await;
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
async fn get_job_schedule_by_name_reads_schedule_without_mutation() {
    let (pool, database) = setup_ephemeral_pool("postgres_schedule_get_by_name", 4).await;

    let mut tx = pool.begin().await.expect("begin job definition tx");
    upsert_job_definition_tx(&mut tx, &definition_upsert(SCHEDULE_JOB, true))
        .await
        .expect("upsert job definition");
    tx.commit().await.expect("commit job definition tx");

    let payload = json!({ "version": 1 });
    let next_fire_at = fixed_utc("2026-05-26T12:00:00Z");
    upsert_job_schedule(
        &pool,
        &JobScheduleUpsert {
            name: SCHEDULE_NAME,
            job_type: JobType::new(SCHEDULE_JOB),
            organization_id: None,
            payload_template: &payload,
            cron_expr: "0 0 * * * *",
            is_active: false,
            next_fire_at,
            max_jitter_seconds: 7,
        },
    )
    .await
    .expect("insert schedule");

    let schedule = get_job_schedule_by_name(&pool, SCHEDULE_NAME)
        .await
        .expect("read schedule")
        .expect("schedule exists");
    assert_eq!(schedule.name, SCHEDULE_NAME);
    assert_eq!(schedule.job_type.as_str(), SCHEDULE_JOB);
    assert_eq!(schedule.payload_template, payload);
    assert_eq!(schedule.cron_expr, "0 0 * * * *");
    assert!(!schedule.is_active);
    assert_eq!(schedule.next_fire_at, next_fire_at);
    assert_eq!(schedule.max_jitter_seconds, 7);

    let missing = get_job_schedule_by_name(&pool, "schedule-missing")
        .await
        .expect("read missing schedule");
    assert!(missing.is_none());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_catalog_job_schedules_tx_applies_is_active_on_conflict() {
    let (pool, database) = setup_ephemeral_pool("postgres_schedule_catalog_sync_active", 4).await;

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
    let next_fire_at = fixed_utc("2026-05-26T12:00:00Z");
    let inserted = upsert_job_schedule(
        &pool,
        &JobScheduleUpsert {
            name: SCHEDULE_NAME,
            job_type: JobType::new(SCHEDULE_JOB),
            organization_id: None,
            payload_template: &payload,
            cron_expr: "0 0 * * * *",
            is_active: false,
            next_fire_at,
            max_jitter_seconds: 0,
        },
    )
    .await
    .expect("insert inactive schedule");
    assert!(!inserted.is_active);

    let mut tx = pool.begin().await.expect("begin schedule sync tx");
    sync_catalog_job_schedules_tx(
        &mut tx,
        &[JobScheduleCatalogSyncEntry {
            upsert: JobScheduleUpsert {
                name: SCHEDULE_NAME,
                job_type: JobType::new(SCHEDULE_JOB),
                organization_id: None,
                payload_template: &payload,
                cron_expr: "0 0 * * * *",
                is_active: true,
                next_fire_at,
                max_jitter_seconds: 0,
            },
        }],
    )
    .await
    .expect("sync catalog schedule");
    tx.commit().await.expect("commit schedule sync tx");

    let (active_after_sync, _) = read_schedule_state(&pool, SCHEDULE_NAME).await;
    assert!(
        active_after_sync,
        "catalog schedule sync should activate an existing inactive schedule"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_catalog_job_schedules_tx_deactivates_when_is_active_is_false() {
    let (pool, database) = setup_ephemeral_pool("postgres_schedule_catalog_sync_inactive", 4).await;

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
    let next_fire_at = fixed_utc("2026-05-26T12:00:00Z");
    upsert_job_schedule(
        &pool,
        &JobScheduleUpsert {
            name: SCHEDULE_NAME,
            job_type: JobType::new(SCHEDULE_JOB),
            organization_id: None,
            payload_template: &payload,
            cron_expr: "0 0 * * * *",
            is_active: true,
            next_fire_at,
            max_jitter_seconds: 0,
        },
    )
    .await
    .expect("insert active schedule");

    let mut tx = pool.begin().await.expect("begin schedule sync tx");
    sync_catalog_job_schedules_tx(
        &mut tx,
        &[JobScheduleCatalogSyncEntry {
            upsert: JobScheduleUpsert {
                name: SCHEDULE_NAME,
                job_type: JobType::new(SCHEDULE_JOB),
                organization_id: None,
                payload_template: &payload,
                cron_expr: "0 0 * * * *",
                is_active: false,
                next_fire_at,
                max_jitter_seconds: 0,
            },
        }],
    )
    .await
    .expect("sync catalog schedule inactive");
    tx.commit().await.expect("commit schedule sync tx");

    let (inactive_after_sync, _) = read_schedule_state(&pool, SCHEDULE_NAME).await;
    assert!(
        !inactive_after_sync,
        "catalog schedule sync should deactivate an existing active schedule"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn deactivate_schedules_absent_from_names_tx_is_scope_bound() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_schedule_catalog_exact_deactivate", 4).await;

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
    let next_fire_at = fixed_utc("2026-05-26T12:00:00Z");
    for name in ["schedule-in-scope", "schedule-out-of-scope"] {
        upsert_job_schedule(
            &pool,
            &JobScheduleUpsert {
                name,
                job_type: JobType::new(SCHEDULE_JOB),
                organization_id: None,
                payload_template: &payload,
                cron_expr: "0 0 * * * *",
                is_active: true,
                next_fire_at,
                max_jitter_seconds: 0,
            },
        )
        .await
        .expect("insert active schedule");
    }

    let mut tx = pool.begin().await.expect("begin exact deactivate tx");
    let deactivated = deactivate_schedules_absent_from_names_tx(
        &mut tx,
        &["schedule-in-scope".to_owned()],
        &["schedule-in-scope".to_owned()],
    )
    .await
    .expect("deactivate absent schedules");
    tx.commit().await.expect("commit exact deactivate tx");

    assert!(
        deactivated.is_empty(),
        "present in-scope schedules should not be deactivated"
    );

    let mut tx = pool.begin().await.expect("begin exact deactivate tx");
    let deactivated =
        deactivate_schedules_absent_from_names_tx(&mut tx, &["schedule-in-scope".to_owned()], &[])
            .await
            .expect("deactivate absent in-scope schedule");
    tx.commit().await.expect("commit exact deactivate tx");

    assert_eq!(deactivated, vec!["schedule-in-scope".to_owned()]);

    let (in_scope, _) = read_schedule_state(&pool, "schedule-in-scope").await;
    assert!(!in_scope);

    let (out_of_scope, _) = read_schedule_state(&pool, "schedule-out-of-scope").await;
    assert!(
        out_of_scope,
        "schedules outside the exact-sync scope should remain active"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn deactivate_schedules_absent_from_names_tx_returns_names_sorted() {
    let (pool, database) =
        setup_ephemeral_pool("postgres_schedule_catalog_exact_deactivate_sorted", 4).await;

    let mut tx = pool.begin().await.expect("begin job definition tx");
    upsert_job_definition_tx(&mut tx, &definition_upsert(SCHEDULE_JOB, true))
        .await
        .expect("upsert job definition");
    tx.commit().await.expect("commit job definition tx");

    let payload = json!({ "version": 1 });
    let next_fire_at = fixed_utc("2026-05-26T12:00:00Z");
    for name in [
        "schedule-sorted-z",
        "schedule-sorted-a",
        "schedule-sorted-m",
    ] {
        upsert_job_schedule(
            &pool,
            &JobScheduleUpsert {
                name,
                job_type: JobType::new(SCHEDULE_JOB),
                organization_id: None,
                payload_template: &payload,
                cron_expr: "0 0 * * * *",
                is_active: true,
                next_fire_at,
                max_jitter_seconds: 0,
            },
        )
        .await
        .expect("insert active schedule");
    }

    let mut tx = pool.begin().await.expect("begin exact deactivate tx");
    let deactivated = deactivate_schedules_absent_from_names_tx(
        &mut tx,
        &[
            "schedule-sorted-z".to_owned(),
            "schedule-sorted-a".to_owned(),
            "schedule-sorted-m".to_owned(),
        ],
        &[],
    )
    .await
    .expect("deactivate absent schedules");
    tx.commit().await.expect("commit exact deactivate tx");

    assert_eq!(
        deactivated,
        vec![
            "schedule-sorted-a".to_owned(),
            "schedule-sorted-m".to_owned(),
            "schedule-sorted-z".to_owned()
        ]
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn claim_due_schedules_waits_for_exact_sync_lock() {
    let (pool, database) = setup_ephemeral_pool("postgres_schedule_claim_exact_lock", 4).await;

    let mut tx = pool.begin().await.expect("begin job definition tx");
    upsert_job_definition_tx(&mut tx, &definition_upsert(SCHEDULE_JOB, true))
        .await
        .expect("upsert job definition");
    tx.commit().await.expect("commit job definition tx");

    let payload = json!({ "version": 1 });
    let next_fire_at = fixed_utc("2026-05-26T12:00:00Z");
    upsert_job_schedule(
        &pool,
        &JobScheduleUpsert {
            name: "schedule-claim-exact-lock",
            job_type: JobType::new(SCHEDULE_JOB),
            organization_id: None,
            payload_template: &payload,
            cron_expr: "0 0 * * * *",
            is_active: true,
            next_fire_at,
            max_jitter_seconds: 0,
        },
    )
    .await
    .expect("insert due schedule");

    let mut exact_sync_tx = pool.begin().await.expect("begin exact sync tx");
    prepare_schedule_exact_sync_critical_section_tx(&mut exact_sync_tx)
        .await
        .expect("hold exact sync lock");

    let claim_pool = pool.clone();
    let mut claim_task = tokio::spawn(async move {
        let mut tx = claim_pool.begin().await.expect("begin claim tx");
        let schedules =
            claim_due_schedules_tx(&mut tx, fixed_utc("2026-05-26T12:00:01Z"), 1).await?;
        tx.commit()
            .await
            .map_err(|error| Error::ConnectionError(error.to_string()))?;
        Ok::<_, Error>(schedules)
    });

    timeout(Duration::from_millis(150), &mut claim_task)
        .await
        .expect_err("claim should wait for exact sync lock before row claims");

    exact_sync_tx
        .rollback()
        .await
        .expect("release exact sync lock");
    let claimed = timeout(Duration::from_secs(5), claim_task)
        .await
        .expect("claim should finish after exact sync releases the table lock")
        .expect("claim task should not panic")
        .expect("claim due schedules");

    assert_eq!(claimed.len(), 1);
    assert_eq!(claimed[0].name, "schedule-claim-exact-lock");

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
