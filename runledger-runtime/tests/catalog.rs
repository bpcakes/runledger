use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use chrono::{DateTime, Utc};
use runledger_core::jobs::{
    JobCompletion, JobContext, JobFailure, JobStage, JobStatus, JobType, WorkflowBuildError,
};
use runledger_postgres::jobs::{
    JobDefinitionUpdate, enqueue_job, get_job_by_id, get_job_definition_by_type,
    get_job_schedule_by_name, update_job_definition, upsert_job_schedule,
};
use runledger_runtime::Supervisor;
use runledger_runtime::catalog::{
    CatalogError, CatalogJobEnqueueInput, CatalogJobScheduleInput, CatalogJobScheduleSpec,
    JobCatalog, JobCatalogDefaults, JobCatalogDefinitionOverrides, JobCatalogScheduleSyncScope,
    JobCatalogSyncScope,
};
use runledger_runtime::config::JobsConfig;
use runledger_runtime::registry::JobHandler;
use serde_json::{Value, json};
use tokio::time::{Instant, sleep, timeout};
use uuid::Uuid;

#[path = "../test_support.rs"]
mod test_support;

use test_support::{setup_ephemeral_pool, teardown_ephemeral_pool};

const CATALOG_TEST_JOB: &str = "jobs.test.catalog";
const CATALOG_OTHER_JOB: &str = "jobs.catalog.other";
const CATALOG_THIRD_JOB: &str = "jobs.catalog.third";

struct CountingHandler {
    runs: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl JobHandler for CountingHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(CATALOG_TEST_JOB)
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.runs.fetch_add(1, Ordering::SeqCst);
        Ok(JobCompletion::success())
    }
}

struct HandlerReturningOtherType;

#[async_trait::async_trait]
impl JobHandler for HandlerReturningOtherType {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(CATALOG_OTHER_JOB)
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        Ok(JobCompletion::success())
    }
}

struct HandlerReturningThirdType;

#[async_trait::async_trait]
impl JobHandler for HandlerReturningThirdType {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(CATALOG_THIRD_JOB)
    }

    async fn execute(
        &self,
        _context: JobContext,
        _payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        Ok(JobCompletion::success())
    }
}

fn test_config() -> JobsConfig {
    JobsConfig {
        worker_id: "catalog-test-worker".to_string(),
        poll_interval: Duration::from_millis(25),
        claim_batch_size: 4,
        lease_ttl_seconds: 10,
        max_global_concurrency: 4,
        reaper_interval: Duration::from_millis(50),
        schedule_poll_interval: Duration::from_millis(50),
        reaper_retry_delay_ms: 1_000,
    }
}

#[tokio::test]
async fn sync_definitions_creates_readable_job_definition_with_defaults() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_sync", 4).await;

    let catalog = JobCatalog::new()
        .job(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
        )
        .defaults(
            JobCatalogDefaults::new()
                .version(2)
                .max_attempts(5)
                .timeout_seconds(120)
                .priority(10),
        );
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync definitions");

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load definition")
        .expect("definition exists");
    assert_eq!(definition.version, 2);
    assert_eq!(definition.max_attempts, 5);
    assert_eq!(definition.default_timeout_seconds, 120);
    assert_eq!(definition.default_priority, 10);
    assert!(definition.is_enabled);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn job_before_defaults_uses_later_defaults_on_sync() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_defaults_order", 4).await;

    let catalog = JobCatalog::new()
        .job(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
        )
        .defaults(
            JobCatalogDefaults::new()
                .max_attempts(7)
                .timeout_seconds(90),
        );
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync definitions");

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load definition")
        .expect("definition exists");
    assert_eq!(definition.max_attempts, 7);
    assert_eq!(definition.default_timeout_seconds, 90);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_uses_job_specific_definition_overrides() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_job_defaults", 4).await;

    let catalog = JobCatalog::new()
        .defaults(
            JobCatalogDefaults::new()
                .version(10)
                .max_attempts(10)
                .timeout_seconds(10)
                .priority(10),
        )
        .job_with_definition_overrides(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
            JobCatalogDefinitionOverrides::new()
                .timeout_seconds(60)
                .priority(11),
        )
        .job_with_definition_overrides(
            CATALOG_OTHER_JOB,
            HandlerReturningOtherType,
            JobCatalogDefinitionOverrides::new()
                .version(3)
                .max_attempts(7)
                .timeout_seconds(600)
                .priority(22),
        );
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync definitions");

    let test_definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load test definition")
        .expect("test definition exists");
    assert_eq!(test_definition.version, 10);
    assert_eq!(test_definition.max_attempts, 10);
    assert_eq!(test_definition.default_timeout_seconds, 60);
    assert_eq!(test_definition.default_priority, 11);

    let other_definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_OTHER_JOB))
        .await
        .expect("load other definition")
        .expect("other definition exists");
    assert_eq!(other_definition.version, 3);
    assert_eq!(other_definition.max_attempts, 7);
    assert_eq!(other_definition.default_timeout_seconds, 600);
    assert_eq!(other_definition.default_priority, 22);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_is_idempotent() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_idempotent", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    catalog.sync_definitions(&pool).await.expect("first sync");
    catalog.sync_definitions(&pool).await.expect("second sync");

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load definition")
        .expect("definition exists");
    assert_eq!(definition.max_attempts, 3);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_overwrites_owned_fields_but_preserves_operator_disabled_state() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_owns_fields", 4).await;

    let catalog = JobCatalog::new()
        .job(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
        )
        .defaults(
            JobCatalogDefaults::new()
                .max_attempts(5)
                .timeout_seconds(120)
                .priority(10),
        );
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync definition");
    update_job_definition(
        &pool,
        JobType::new(CATALOG_TEST_JOB),
        &JobDefinitionUpdate {
            max_attempts: Some(9),
            default_timeout_seconds: Some(600),
            default_priority: Some(99),
            is_enabled: Some(false),
        },
    )
    .await
    .expect("operator update");

    catalog
        .sync_definitions(&pool)
        .await
        .expect("resync definition");

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load definition")
        .expect("definition exists");
    assert_eq!(definition.max_attempts, 5);
    assert_eq!(definition.default_timeout_seconds, 120);
    assert_eq!(definition.default_priority, 10);
    assert!(
        !definition.is_enabled,
        "enabled catalog sync should preserve an operator-disabled row"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_preserves_operator_enabled_state_on_resync() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_preserve_enabled", 4).await;

    let catalog = JobCatalog::new()
        .job(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
        )
        .defaults(JobCatalogDefaults::new().max_attempts(5));
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync definition");
    update_job_definition(
        &pool,
        JobType::new(CATALOG_TEST_JOB),
        &JobDefinitionUpdate {
            max_attempts: Some(9),
            default_timeout_seconds: None,
            default_priority: None,
            is_enabled: Some(true),
        },
    )
    .await
    .expect("operator update");

    catalog
        .sync_definitions(&pool)
        .await
        .expect("resync definition");

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load definition")
        .expect("definition exists");
    assert_eq!(definition.max_attempts, 5);
    assert!(
        definition.is_enabled,
        "enabled catalog sync should preserve an operator-enabled row"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_preserves_operator_disabled_state_for_enabled_job_override() {
    let (pool, database) =
        setup_ephemeral_pool("runtime_catalog_override_preserve_disabled", 4).await;

    let catalog = JobCatalog::new()
        .defaults(JobCatalogDefaults::new().enabled(false))
        .job_with_definition_overrides(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
            JobCatalogDefinitionOverrides::new().enabled(true),
        );
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync enabled override");
    update_job_definition(
        &pool,
        JobType::new(CATALOG_TEST_JOB),
        &JobDefinitionUpdate {
            max_attempts: None,
            default_timeout_seconds: None,
            default_priority: None,
            is_enabled: Some(false),
        },
    )
    .await
    .expect("operator disable");

    catalog
        .sync_definitions(&pool)
        .await
        .expect("resync enabled override");

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load definition")
        .expect("definition exists");
    assert!(
        !definition.is_enabled,
        "enabled job override should still preserve operator-disabled rows"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_writes_disabled_catalog_definition_without_active_schedules() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_disabled_additive", 4).await;

    let catalog = JobCatalog::new()
        .job(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
        )
        .defaults(JobCatalogDefaults::new().enabled(false));
    let report = catalog
        .sync_definitions(&pool)
        .await
        .expect("sync disabled definition");
    assert_eq!(
        report.disabled_catalog_job_types,
        vec![CATALOG_TEST_JOB],
        "disabled additive sync should report newly disabled catalog jobs"
    );

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load definition")
        .expect("definition exists");
    assert!(
        !definition.is_enabled,
        "disabled catalog default should write a disabled definition"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_writes_disabled_job_override_without_active_schedules() {
    let (pool, database) =
        setup_ephemeral_pool("runtime_catalog_disabled_override_additive", 4).await;

    let catalog = JobCatalog::new().job_with_definition_overrides(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
        JobCatalogDefinitionOverrides::new().enabled(false),
    );
    let report = catalog
        .sync_definitions(&pool)
        .await
        .expect("sync disabled override");
    assert_eq!(
        report.disabled_catalog_job_types,
        vec![CATALOG_TEST_JOB],
        "disabled additive sync should report newly disabled job overrides"
    );

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load definition")
        .expect("definition exists");
    assert!(
        !definition.is_enabled,
        "disabled job override should write a disabled definition"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_disables_existing_enabled_job_override_without_active_schedules() {
    let (pool, database) =
        setup_ephemeral_pool("runtime_catalog_disable_existing_override_additive", 4).await;

    let enabled_catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    enabled_catalog
        .sync_definitions(&pool)
        .await
        .expect("sync enabled definition");

    let disabled_catalog = JobCatalog::new().job_with_definition_overrides(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
        JobCatalogDefinitionOverrides::new().enabled(false),
    );
    let report = disabled_catalog
        .sync_definitions(&pool)
        .await
        .expect("sync disabled override");
    assert_eq!(
        report.disabled_catalog_job_types,
        vec![CATALOG_TEST_JOB],
        "disabled additive sync should report enabled rows changed to disabled"
    );

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load definition")
        .expect("definition exists");
    assert!(
        !definition.is_enabled,
        "disabled job override should disable an existing enabled definition"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_handles_mixed_enabled_and_disabled_overrides() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_mixed_override_additive", 4).await;

    let catalog = JobCatalog::new()
        .defaults(JobCatalogDefaults::new().enabled(false))
        .job_with_definition_overrides(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
            JobCatalogDefinitionOverrides::new().enabled(true),
        )
        .job(CATALOG_OTHER_JOB, HandlerReturningOtherType);
    let report = catalog
        .sync_definitions(&pool)
        .await
        .expect("sync mixed effective enabled states");
    assert_eq!(
        report.disabled_catalog_job_types,
        vec![CATALOG_OTHER_JOB],
        "effectively disabled jobs should be reported when first written disabled"
    );

    update_job_definition(
        &pool,
        JobType::new(CATALOG_TEST_JOB),
        &JobDefinitionUpdate {
            max_attempts: None,
            default_timeout_seconds: None,
            default_priority: None,
            is_enabled: Some(false),
        },
    )
    .await
    .expect("operator disable enabled override");

    catalog
        .sync_definitions(&pool)
        .await
        .expect("resync mixed effective enabled states");

    let enabled_override = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load enabled override definition")
        .expect("enabled override definition exists");
    assert!(
        !enabled_override.is_enabled,
        "effectively enabled job should preserve operator-disabled state"
    );

    let disabled_default = get_job_definition_by_type(&pool, JobType::new(CATALOG_OTHER_JOB))
        .await
        .expect("load disabled default definition")
        .expect("disabled default definition exists");
    assert!(
        !disabled_default.is_enabled,
        "effectively disabled job should remain disabled"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_exact_disables_absent_job_definitions() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_exact", 4).await;

    let third_catalog = JobCatalog::new().job(CATALOG_THIRD_JOB, HandlerReturningThirdType);
    third_catalog
        .sync_definitions(&pool)
        .await
        .expect("sync third definition");
    let old_catalog = JobCatalog::new().job(CATALOG_OTHER_JOB, HandlerReturningOtherType);
    old_catalog
        .sync_definitions(&pool)
        .await
        .expect("sync old definition");

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    let report = catalog
        .sync_definitions_exact(
            &pool,
            &JobCatalogSyncScope::job_types([
                CATALOG_TEST_JOB,
                CATALOG_OTHER_JOB,
                CATALOG_THIRD_JOB,
            ])
            .expect("valid scope"),
        )
        .await
        .expect("exact sync definitions");
    assert_eq!(
        report.disabled_absent_job_types,
        vec![CATALOG_OTHER_JOB, CATALOG_THIRD_JOB]
    );
    assert!(report.disabled_catalog_job_types.is_empty());

    let old_definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_OTHER_JOB))
        .await
        .expect("load old definition")
        .expect("old definition remains");
    assert!(
        !old_definition.is_enabled,
        "absent definition should be disabled"
    );

    let current_definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load current definition")
        .expect("current definition exists");
    assert!(
        current_definition.is_enabled,
        "catalog definition should stay enabled"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_exact_waits_for_schedule_table_lock_before_disabling() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_exact_lock", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    let scope =
        JobCatalogSyncScope::job_types([CATALOG_TEST_JOB, CATALOG_OTHER_JOB]).expect("valid scope");

    let mut blocker = pool.begin().await.expect("begin blocker transaction");
    sqlx::query("LOCK TABLE job_schedules IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold conflicting schedule table lock");

    let blocked = timeout(
        Duration::from_millis(150),
        catalog.sync_definitions_exact(&pool, &scope),
    )
    .await;
    assert!(
        blocked.is_err(),
        "exact sync should wait for the schedule table lock"
    );

    blocker.rollback().await.expect("release blocker lock");
    timeout(
        Duration::from_secs(5),
        catalog.sync_definitions_exact(&pool, &scope),
    )
    .await
    .expect("exact sync should complete promptly after releasing blocker")
    .expect("exact sync after releasing blocker");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_exact_rejects_empty_catalog() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_exact_empty", 4).await;

    let error = JobCatalog::new()
        .sync_definitions_exact(
            &pool,
            &JobCatalogSyncScope::job_type(CATALOG_TEST_JOB).expect("valid scope"),
        )
        .await
        .expect_err("empty exact catalog");
    assert!(matches!(error, CatalogError::EmptyExactSyncCatalog));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_exact_rejects_catalog_jobs_outside_scope() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_exact_scope", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    let error = catalog
        .sync_definitions_exact(
            &pool,
            &JobCatalogSyncScope::job_type(CATALOG_OTHER_JOB).expect("valid scope"),
        )
        .await
        .expect_err("out of scope catalog job");
    assert!(matches!(
        error,
        CatalogError::JobTypeOutsideExactSyncScope { .. }
    ));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_exact_rejects_active_schedules_for_absent_job_types() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_exact_active_schedule", 4).await;

    let old_catalog = JobCatalog::new().job(CATALOG_OTHER_JOB, HandlerReturningOtherType);
    old_catalog
        .sync_definitions(&pool)
        .await
        .expect("sync old definition");

    let schedule_payload = json!({});
    let old_schedule = old_catalog
        .job_schedule(&CatalogJobScheduleInput {
            name: "catalog-old-schedule",
            job_type: CATALOG_OTHER_JOB,
            organization_id: None,
            payload_template: &schedule_payload,
            cron_expr: "0 * * * * *",
            is_active: true,
            next_fire_at: Utc::now(),
            max_jitter_seconds: 0,
        })
        .expect("old schedule input");
    upsert_job_schedule(&pool, &old_schedule)
        .await
        .expect("upsert old schedule");

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    let error = catalog
        .sync_definitions_exact(
            &pool,
            &JobCatalogSyncScope::job_types([CATALOG_TEST_JOB, CATALOG_OTHER_JOB])
                .expect("valid scope"),
        )
        .await
        .expect_err("active schedule should block exact sync");
    assert!(matches!(
        error,
        CatalogError::ActiveScheduleForAbsentJobType {
            schedule_name,
            job_type,
        } if schedule_name == "catalog-old-schedule" && job_type == CATALOG_OTHER_JOB
    ));

    let old_definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_OTHER_JOB))
        .await
        .expect("load old definition")
        .expect("old definition remains");
    assert!(
        old_definition.is_enabled,
        "blocked exact sync should not disable active scheduled jobs"
    );
    let current_definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load catalog definition");
    assert!(
        current_definition.is_none(),
        "blocked exact sync should roll back catalog job upserts"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_exact_allows_active_schedules_for_already_disabled_absent_job_types() {
    let (pool, database) =
        setup_ephemeral_pool("runtime_catalog_exact_disabled_absent_schedule", 4).await;

    let old_catalog = JobCatalog::new().job(CATALOG_OTHER_JOB, HandlerReturningOtherType);
    old_catalog
        .sync_definitions(&pool)
        .await
        .expect("sync old definition");
    update_job_definition(
        &pool,
        JobType::new(CATALOG_OTHER_JOB),
        &JobDefinitionUpdate {
            max_attempts: None,
            default_timeout_seconds: None,
            default_priority: None,
            is_enabled: Some(false),
        },
    )
    .await
    .expect("disable old definition");

    let schedule_payload = json!({});
    let old_schedule = old_catalog
        .job_schedule(&CatalogJobScheduleInput {
            name: "catalog-disabled-absent-schedule",
            job_type: CATALOG_OTHER_JOB,
            organization_id: None,
            payload_template: &schedule_payload,
            cron_expr: "0 * * * * *",
            is_active: true,
            next_fire_at: Utc::now(),
            max_jitter_seconds: 0,
        })
        .expect("old schedule input");
    upsert_job_schedule(&pool, &old_schedule)
        .await
        .expect("upsert old schedule");

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    let report = catalog
        .sync_definitions_exact(
            &pool,
            &JobCatalogSyncScope::job_types([CATALOG_TEST_JOB, CATALOG_OTHER_JOB])
                .expect("valid scope"),
        )
        .await
        .expect("already-disabled absent schedules should not block exact sync");
    assert!(report.disabled_absent_job_types.is_empty());
    assert!(report.disabled_catalog_job_types.is_empty());

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_exact_reenables_reintroduced_catalog_jobs() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_exact_reintroduced", 4).await;

    let old_catalog = JobCatalog::new().job(CATALOG_OTHER_JOB, HandlerReturningOtherType);
    old_catalog
        .sync_definitions(&pool)
        .await
        .expect("sync old definition");
    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    catalog
        .sync_definitions_exact(
            &pool,
            &JobCatalogSyncScope::job_types([CATALOG_TEST_JOB, CATALOG_OTHER_JOB])
                .expect("valid scope"),
        )
        .await
        .expect("exact sync disables old definition");

    let reintroduced = JobCatalog::new().job(CATALOG_OTHER_JOB, HandlerReturningOtherType);
    let report = reintroduced
        .sync_definitions_exact(
            &pool,
            &JobCatalogSyncScope::job_type(CATALOG_OTHER_JOB).expect("valid scope"),
        )
        .await
        .expect("exact sync reintroduces old definition");
    assert!(report.disabled_absent_job_types.is_empty());
    assert!(report.disabled_catalog_job_types.is_empty());

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_OTHER_JOB))
        .await
        .expect("load definition")
        .expect("definition exists");
    assert!(
        definition.is_enabled,
        "exact sync should re-enable a reintroduced catalog job"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_exact_restores_operator_disabled_catalog_jobs() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_exact_restores_pause", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync definition");
    update_job_definition(
        &pool,
        JobType::new(CATALOG_TEST_JOB),
        &JobDefinitionUpdate {
            max_attempts: None,
            default_timeout_seconds: None,
            default_priority: None,
            is_enabled: Some(false),
        },
    )
    .await
    .expect("operator disables definition");

    catalog
        .sync_definitions_exact(
            &pool,
            &JobCatalogSyncScope::job_type(CATALOG_TEST_JOB).expect("valid scope"),
        )
        .await
        .expect("exact sync restores catalog enabled state");

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load definition")
        .expect("definition exists");
    assert!(
        definition.is_enabled,
        "exact sync should restore an operator-disabled catalog job from catalog defaults"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_exact_restores_enabled_job_override() {
    let (pool, database) =
        setup_ephemeral_pool("runtime_catalog_exact_restores_enabled_override", 4).await;

    let catalog = JobCatalog::new()
        .defaults(JobCatalogDefaults::new().enabled(false))
        .job_with_definition_overrides(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
            JobCatalogDefinitionOverrides::new().enabled(true),
        );
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync enabled override");
    update_job_definition(
        &pool,
        JobType::new(CATALOG_TEST_JOB),
        &JobDefinitionUpdate {
            max_attempts: None,
            default_timeout_seconds: None,
            default_priority: None,
            is_enabled: Some(false),
        },
    )
    .await
    .expect("operator disables definition");

    let report = catalog
        .sync_definitions_exact(
            &pool,
            &JobCatalogSyncScope::job_type(CATALOG_TEST_JOB).expect("valid scope"),
        )
        .await
        .expect("exact sync restores enabled override");
    assert!(report.disabled_absent_job_types.is_empty());
    assert!(report.disabled_catalog_job_types.is_empty());

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load definition")
        .expect("definition exists");
    assert!(
        definition.is_enabled,
        "exact sync should restore enabled state from job-specific overrides"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_rejects_disabled_catalog_job_with_active_schedule() {
    let (pool, database) =
        setup_ephemeral_pool("runtime_catalog_disabled_active_schedule", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync enabled definition");

    let schedule_payload = json!({});
    let schedule = catalog
        .job_schedule(&CatalogJobScheduleInput {
            name: "catalog-disabled-schedule",
            job_type: CATALOG_TEST_JOB,
            organization_id: None,
            payload_template: &schedule_payload,
            cron_expr: "0 * * * * *",
            is_active: true,
            next_fire_at: Utc::now(),
            max_jitter_seconds: 0,
        })
        .expect("schedule input");
    upsert_job_schedule(&pool, &schedule)
        .await
        .expect("upsert schedule");

    let disabled_catalog = JobCatalog::new()
        .job(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
        )
        .defaults(JobCatalogDefaults::new().enabled(false));
    let error = disabled_catalog
        .sync_definitions(&pool)
        .await
        .expect_err("active schedule should block disabled sync");
    assert!(matches!(
        error,
        CatalogError::ActiveScheduleForDisabledJobType { .. }
    ));

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load definition")
        .expect("definition exists");
    assert!(
        definition.is_enabled,
        "blocked disabled sync should leave definition enabled"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_rejects_disabled_job_override_with_active_schedule() {
    let (pool, database) =
        setup_ephemeral_pool("runtime_catalog_disabled_override_active_schedule", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync enabled definition");

    let schedule_payload = json!({});
    let schedule = catalog
        .job_schedule(&CatalogJobScheduleInput {
            name: "catalog-disabled-override-schedule",
            job_type: CATALOG_TEST_JOB,
            organization_id: None,
            payload_template: &schedule_payload,
            cron_expr: "0 * * * * *",
            is_active: true,
            next_fire_at: Utc::now(),
            max_jitter_seconds: 0,
        })
        .expect("schedule input");
    upsert_job_schedule(&pool, &schedule)
        .await
        .expect("upsert schedule");

    let disabled_catalog = JobCatalog::new().job_with_definition_overrides(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
        JobCatalogDefinitionOverrides::new().enabled(false),
    );
    let error = disabled_catalog
        .sync_definitions(&pool)
        .await
        .expect_err("active schedule should block disabled override sync");
    assert!(matches!(
        error,
        CatalogError::ActiveScheduleForDisabledJobType { .. }
    ));

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load definition")
        .expect("definition exists");
    assert!(
        definition.is_enabled,
        "blocked disabled override sync should leave definition enabled"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_exact_rejects_disabled_catalog_job_with_active_schedule() {
    let (pool, database) =
        setup_ephemeral_pool("runtime_catalog_exact_disabled_active_schedule", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync enabled definition");

    let schedule_payload = json!({});
    let schedule = catalog
        .job_schedule(&CatalogJobScheduleInput {
            name: "catalog-exact-disabled-schedule",
            job_type: CATALOG_TEST_JOB,
            organization_id: None,
            payload_template: &schedule_payload,
            cron_expr: "0 * * * * *",
            is_active: true,
            next_fire_at: Utc::now(),
            max_jitter_seconds: 0,
        })
        .expect("schedule input");
    upsert_job_schedule(&pool, &schedule)
        .await
        .expect("upsert schedule");

    let disabled_catalog = JobCatalog::new()
        .job(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
        )
        .defaults(JobCatalogDefaults::new().enabled(false));
    let error = disabled_catalog
        .sync_definitions_exact(
            &pool,
            &JobCatalogSyncScope::job_type(CATALOG_TEST_JOB).expect("valid scope"),
        )
        .await
        .expect_err("active schedule should block exact disabled sync");
    assert!(matches!(
        error,
        CatalogError::ActiveScheduleForDisabledJobType { .. }
    ));

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load definition")
        .expect("definition exists");
    assert!(
        definition.is_enabled,
        "blocked exact disabled sync should leave definition enabled"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_exact_can_disable_catalog_jobs_without_active_schedules() {
    let (pool, database) =
        setup_ephemeral_pool("runtime_catalog_exact_disabled_no_schedule", 4).await;

    let old_catalog = JobCatalog::new().job(CATALOG_OTHER_JOB, HandlerReturningOtherType);
    old_catalog
        .sync_definitions(&pool)
        .await
        .expect("sync old definition");

    let disabled_catalog = JobCatalog::new()
        .job(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
        )
        .job(CATALOG_OTHER_JOB, HandlerReturningOtherType)
        .job(CATALOG_THIRD_JOB, HandlerReturningThirdType)
        .defaults(JobCatalogDefaults::new().enabled(false));
    let third_catalog = JobCatalog::new().job(CATALOG_THIRD_JOB, HandlerReturningThirdType);
    third_catalog
        .sync_definitions(&pool)
        .await
        .expect("sync third definition");
    update_job_definition(
        &pool,
        JobType::new(CATALOG_THIRD_JOB),
        &JobDefinitionUpdate {
            max_attempts: None,
            default_timeout_seconds: None,
            default_priority: None,
            is_enabled: Some(false),
        },
    )
    .await
    .expect("pre-disable third definition");

    let report = disabled_catalog
        .sync_definitions_exact(
            &pool,
            &JobCatalogSyncScope::job_types([
                CATALOG_TEST_JOB,
                CATALOG_OTHER_JOB,
                CATALOG_THIRD_JOB,
            ])
            .expect("valid scope"),
        )
        .await
        .expect("exact disabled sync without active schedules");
    assert!(report.disabled_absent_job_types.is_empty());
    assert_eq!(
        report.disabled_catalog_job_types,
        vec![CATALOG_OTHER_JOB, CATALOG_TEST_JOB]
    );

    let catalog_definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load catalog definition")
        .expect("catalog definition exists");
    assert!(
        !catalog_definition.is_enabled,
        "disabled catalog sync should disable its registered definition"
    );

    let absent_definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_OTHER_JOB))
        .await
        .expect("load previously enabled definition")
        .expect("previously enabled definition remains");
    assert!(
        !absent_definition.is_enabled,
        "exact disabled sync should disable previously enabled catalog definitions"
    );

    let already_disabled_definition =
        get_job_definition_by_type(&pool, JobType::new(CATALOG_THIRD_JOB))
            .await
            .expect("load already disabled definition")
            .expect("already disabled definition remains");
    assert!(
        !already_disabled_definition.is_enabled,
        "exact disabled sync should leave already disabled catalog definitions disabled"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_exact_writes_disabled_job_override_without_active_schedules() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_exact_disabled_override", 4).await;

    let old_catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    old_catalog
        .sync_definitions(&pool)
        .await
        .expect("sync enabled definition");

    let disabled_catalog = JobCatalog::new().job_with_definition_overrides(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
        JobCatalogDefinitionOverrides::new().enabled(false),
    );
    let report = disabled_catalog
        .sync_definitions_exact(
            &pool,
            &JobCatalogSyncScope::job_type(CATALOG_TEST_JOB).expect("valid scope"),
        )
        .await
        .expect("exact sync disabled override without active schedules");
    assert!(report.disabled_absent_job_types.is_empty());
    assert_eq!(
        report.disabled_catalog_job_types,
        vec![CATALOG_TEST_JOB],
        "exact sync should report enabled rows changed to disabled by job overrides"
    );

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load definition")
        .expect("definition exists");
    assert!(
        !definition.is_enabled,
        "exact sync should write disabled state from job-specific overrides"
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_rejects_invalid_defaults_before_database_write() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_invalid_defaults", 4).await;

    let catalog = JobCatalog::new()
        .job(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
        )
        .defaults(JobCatalogDefaults::new().timeout_seconds(0));
    let error = catalog
        .sync_definitions(&pool)
        .await
        .expect_err("invalid defaults");
    assert!(matches!(
        error,
        CatalogError::InvalidDefinitionValue {
            field: "default_timeout_seconds"
        }
    ));

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load definition");
    assert!(definition.is_none(), "invalid sync should not write rows");

    teardown_ephemeral_pool(pool, database).await;
}

#[test]
fn try_job_with_definition_overrides_rejects_invalid_timeout_override() {
    let error = try_catalog_with_definition_overrides(
        JobCatalogDefinitionOverrides::new().timeout_seconds(0),
    );
    assert!(matches!(
        error,
        CatalogError::InvalidJobDefinitionValue {
            job_type,
            field: "default_timeout_seconds"
        } if job_type == CATALOG_TEST_JOB
    ));
}

#[test]
fn try_job_with_definition_overrides_rejects_invalid_version_override() {
    let error =
        try_catalog_with_definition_overrides(JobCatalogDefinitionOverrides::new().version(0));
    assert!(matches!(
        error,
        CatalogError::InvalidJobDefinitionValue {
            job_type,
            field: "version"
        } if job_type == CATALOG_TEST_JOB
    ));
}

#[test]
fn try_job_with_definition_overrides_rejects_invalid_max_attempts_override() {
    let error =
        try_catalog_with_definition_overrides(JobCatalogDefinitionOverrides::new().max_attempts(0));
    assert!(matches!(
        error,
        CatalogError::InvalidJobDefinitionValue {
            job_type,
            field: "max_attempts"
        } if job_type == CATALOG_TEST_JOB
    ));
}

fn try_catalog_with_definition_overrides(overrides: JobCatalogDefinitionOverrides) -> CatalogError {
    JobCatalog::new()
        .try_job_with_definition_overrides(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
            overrides,
        )
        .expect_err("invalid definition overrides")
}

#[tokio::test]
async fn sync_definitions_rejects_zero_version_before_database_write() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_invalid_version", 4).await;

    let catalog = JobCatalog::new()
        .job(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
        )
        .defaults(JobCatalogDefaults::new().version(0));
    let error = catalog
        .sync_definitions(&pool)
        .await
        .expect_err("invalid version");
    assert!(matches!(
        error,
        CatalogError::InvalidDefinitionValue { field: "version" }
    ));

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load definition");
    assert!(definition.is_none(), "invalid sync should not write rows");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_definitions_rejects_zero_max_attempts_before_database_write() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_invalid_attempts", 4).await;

    let catalog = JobCatalog::new()
        .job(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
        )
        .defaults(JobCatalogDefaults::new().max_attempts(0));
    let error = catalog
        .sync_definitions(&pool)
        .await
        .expect_err("invalid max attempts");
    assert!(matches!(
        error,
        CatalogError::InvalidDefinitionValue {
            field: "max_attempts"
        }
    ));

    let definition = get_job_definition_by_type(&pool, JobType::new(CATALOG_TEST_JOB))
        .await
        .expect("load definition");
    assert!(definition.is_none(), "invalid sync should not write rows");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn supervisor_with_catalog_processes_enqueued_job_after_sync() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_supervisor", 8).await;

    let runs = Arc::new(AtomicUsize::new(0));
    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::clone(&runs),
        },
    );
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync definitions");

    let enqueue_payload = json!({});
    let enqueue = catalog
        .job_enqueue(&CatalogJobEnqueueInput {
            job_type: CATALOG_TEST_JOB,
            organization_id: None,
            payload: &enqueue_payload,
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(JobStage::Queued),
        })
        .expect("catalog enqueue input");

    let supervisor = Supervisor::builder(&pool, test_config())
        .expect("supervisor builder has runtime")
        .with_catalog(&catalog)
        .build()
        .expect("supervisor should build");

    let job_id = enqueue_job(&pool, &enqueue)
        .await
        .expect("enqueue catalog job");

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let job = get_job_by_id(&pool, None, job_id)
            .await
            .expect("load job")
            .expect("job exists");
        if job.status == JobStatus::Succeeded {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for catalog job; last status {:?}",
            job.status
        );
        sleep(Duration::from_millis(25)).await;
    }

    assert_eq!(runs.load(Ordering::SeqCst), 1);
    supervisor
        .shutdown_with_timeout(Duration::from_secs(10))
        .await
        .expect("supervisor shutdown");

    teardown_ephemeral_pool(pool, database).await;
}

#[test]
fn try_job_rejects_handler_job_type_mismatch_before_database() {
    let error = JobCatalog::new()
        .try_job("jobs.catalog.expected", HandlerReturningOtherType)
        .expect_err("handler mismatch");
    assert!(matches!(error, CatalogError::HandlerJobTypeMismatch { .. }));
}

#[test]
fn enqueue_helper_forwards_all_input_fields() {
    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    let organization_id =
        Uuid::parse_str("018fa1f8-0000-7000-8000-000000000111").expect("fixed uuid");
    let payload = json!({ "field": "value" });
    let next_run_at = Utc::now();

    let enqueue = catalog
        .job_enqueue(&CatalogJobEnqueueInput {
            job_type: CATALOG_TEST_JOB,
            organization_id: Some(organization_id),
            payload: &payload,
            priority: Some(7),
            max_attempts: Some(5),
            timeout_seconds: Some(90),
            next_run_at: Some(next_run_at),
            idempotency_key: Some("catalog:idempotency"),
            stage: Some(JobStage::Scheduled),
        })
        .expect("enqueue input");

    assert_eq!(enqueue.job_type, JobType::new(CATALOG_TEST_JOB));
    assert_eq!(enqueue.organization_id, Some(organization_id));
    assert_eq!(enqueue.payload, &payload);
    assert_eq!(enqueue.priority, Some(7));
    assert_eq!(enqueue.max_attempts, Some(5));
    assert_eq!(enqueue.timeout_seconds, Some(90));
    assert_eq!(enqueue.next_run_at, Some(next_run_at));
    assert_eq!(enqueue.idempotency_key, Some("catalog:idempotency"));
    assert_eq!(enqueue.stage, Some(JobStage::Scheduled));
}

#[test]
fn schedule_helper_forwards_all_input_fields() {
    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    let organization_id =
        Uuid::parse_str("018fa1f8-0000-7000-8000-000000000222").expect("fixed uuid");
    let payload_template = json!({ "template": true });
    let next_fire_at = Utc::now();

    let schedule = catalog
        .job_schedule(&CatalogJobScheduleInput {
            name: "catalog-forward-schedule",
            job_type: CATALOG_TEST_JOB,
            organization_id: Some(organization_id),
            payload_template: &payload_template,
            cron_expr: "0 */5 * * * *",
            is_active: false,
            next_fire_at,
            max_jitter_seconds: 17,
        })
        .expect("schedule input");

    assert_eq!(schedule.name, "catalog-forward-schedule");
    assert_eq!(schedule.job_type, JobType::new(CATALOG_TEST_JOB));
    assert_eq!(schedule.organization_id, Some(organization_id));
    assert_eq!(schedule.payload_template, &payload_template);
    assert_eq!(schedule.cron_expr, "0 */5 * * * *");
    assert!(!schedule.is_active);
    assert_eq!(schedule.next_fire_at, next_fire_at);
    assert_eq!(schedule.max_jitter_seconds, 17);
}

#[test]
fn workflow_step_helper_forwards_job_step_fields() {
    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    let organization_id =
        Uuid::parse_str("018fa1f8-0000-7000-8000-000000000333").expect("fixed uuid");
    let payload = json!({ "step": true });

    let step = catalog
        .workflow_step("catalog-step", CATALOG_TEST_JOB, &payload)
        .expect("workflow step builder")
        .organization_id(organization_id)
        .priority(12)
        .max_attempts(4)
        .timeout_seconds(300)
        .stage(JobStage::Scheduled)
        .try_build()
        .expect("workflow step");

    assert_eq!(step.step_key().as_str(), "catalog-step");
    assert_eq!(step.job_type(), Some(JobType::new(CATALOG_TEST_JOB)));
    assert_eq!(step.organization_id(), Some(organization_id));
    assert_eq!(step.payload(), &payload);
    assert_eq!(step.priority(), Some(12));
    assert_eq!(step.max_attempts(), Some(4));
    assert_eq!(step.timeout_seconds(), Some(300));
    assert_eq!(step.stage(), Some(JobStage::Scheduled));
}

#[test]
fn schedule_helper_rejects_unknown_job_type() {
    let catalog = JobCatalog::new();
    let error = catalog
        .job_schedule(&CatalogJobScheduleInput {
            name: "catalog-schedule",
            job_type: CATALOG_TEST_JOB,
            organization_id: None,
            payload_template: &json!({}),
            cron_expr: "0 * * * * *",
            is_active: true,
            next_fire_at: Utc::now(),
            max_jitter_seconds: 0,
        })
        .expect_err("unknown job type");
    assert!(matches!(error, CatalogError::UnknownJobType { .. }));
}

#[test]
fn schedule_helper_rejects_disabled_catalog_job_type() {
    let catalog = JobCatalog::new()
        .job(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
        )
        .defaults(JobCatalogDefaults::new().enabled(false));
    let error = catalog
        .job_schedule(&CatalogJobScheduleInput {
            name: "catalog-schedule",
            job_type: CATALOG_TEST_JOB,
            organization_id: None,
            payload_template: &json!({}),
            cron_expr: "0 * * * * *",
            is_active: true,
            next_fire_at: Utc::now(),
            max_jitter_seconds: 0,
        })
        .expect_err("disabled job type");
    assert!(matches!(error, CatalogError::DisabledJobType { .. }));
}

#[test]
fn enqueue_helper_rejects_unknown_job_type_before_enqueue_job() {
    let catalog = JobCatalog::new();
    let error = catalog
        .job_enqueue(&CatalogJobEnqueueInput {
            job_type: CATALOG_TEST_JOB,
            organization_id: None,
            payload: &json!({}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: None,
        })
        .expect_err("unknown job type");
    assert!(matches!(error, CatalogError::UnknownJobType { .. }));
}

#[test]
fn workflow_dag_helper_rejects_unknown_job_type() {
    let catalog = JobCatalog::new();
    let error = catalog
        .workflow_dag("workflow.catalog", &json!({}))
        .job("step", CATALOG_TEST_JOB, &json!({}))
        .expect_err("unknown job type");
    assert!(matches!(error, CatalogError::UnknownJobType { .. }));
}

#[test]
fn workflow_step_helper_rejects_disabled_job_type() {
    let catalog = JobCatalog::new()
        .job(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
        )
        .defaults(JobCatalogDefaults::new().enabled(false));
    let error = catalog
        .workflow_step("append-step", CATALOG_TEST_JOB, &json!({}))
        .expect_err("disabled job type");
    assert!(matches!(error, CatalogError::DisabledJobType { .. }));
}

#[test]
fn workflow_step_helper_rejects_unknown_job_type() {
    let catalog = JobCatalog::new();
    let error = catalog
        .workflow_step("append-step", CATALOG_TEST_JOB, &json!({}))
        .expect_err("unknown job type");
    assert!(matches!(error, CatalogError::UnknownJobType { .. }));
}

#[test]
fn catalog_helpers_do_not_check_operator_disabled_database_rows() {
    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );

    assert!(
        catalog
            .workflow_step("append-step", CATALOG_TEST_JOB, &json!({}))
            .is_ok(),
        "catalog helpers validate catalog state only; database definition pauses are enforced by persistence APIs"
    );
}

#[test]
fn workflow_step_helper_rejects_blank_step_key() {
    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    let error = catalog
        .workflow_step("   ", CATALOG_TEST_JOB, &json!({}))
        .expect_err("blank step key");
    assert!(matches!(
        error,
        CatalogError::WorkflowBuild(WorkflowBuildError::BlankStepKey { .. })
    ));
}

#[tokio::test]
async fn schedule_helper_builds_valid_upsert_for_enabled_catalog_job() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_schedule", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync definitions");

    let schedule_payload = json!({ "source": "catalog" });
    let schedule = catalog
        .job_schedule(&CatalogJobScheduleInput {
            name: "catalog-schedule",
            job_type: CATALOG_TEST_JOB,
            organization_id: None,
            payload_template: &schedule_payload,
            cron_expr: "0 * * * * *",
            is_active: true,
            next_fire_at: Utc::now(),
            max_jitter_seconds: 0,
        })
        .expect("catalog schedule input");
    let persisted = upsert_job_schedule(&pool, &schedule)
        .await
        .expect("upsert schedule");
    assert_eq!(persisted.name, "catalog-schedule");
    assert_eq!(persisted.job_type, JobType::new(CATALOG_TEST_JOB));
    assert_eq!(persisted.payload_template, schedule_payload);
    assert_eq!(persisted.cron_expr, "0 * * * * *");
    assert!(persisted.is_active);
    assert_eq!(persisted.max_jitter_seconds, 0);

    teardown_ephemeral_pool(pool, database).await;
}

#[test]
fn workflow_dag_helper_propagates_build_errors() {
    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );

    let blank_step = catalog
        .workflow_dag("workflow.catalog", &json!({}))
        .job("   ", CATALOG_TEST_JOB, &json!({}))
        .expect_err("blank step key");
    assert!(matches!(
        blank_step,
        CatalogError::WorkflowBuild(WorkflowBuildError::BlankStepKey { .. })
    ));

    let unknown_dependency = catalog
        .workflow_dag("workflow.catalog", &json!({}))
        .job("first", CATALOG_TEST_JOB, &json!({}))
        .expect("first step")
        .after_success("missing", ["first"])
        .expect_err("unknown dependency");
    assert!(matches!(
        unknown_dependency,
        CatalogError::WorkflowBuild(WorkflowBuildError::UnknownStepKey { .. })
    ));

    let empty_workflow = catalog
        .workflow_dag("workflow.catalog", &json!({}))
        .try_build()
        .expect_err("empty workflow");
    assert!(matches!(
        empty_workflow,
        CatalogError::WorkflowBuild(WorkflowBuildError::EmptySteps)
    ));
}

static CATALOG_SYNC_PAYLOAD: LazyLock<Value> =
    LazyLock::new(|| json!({ "source": "catalog-sync" }));

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

fn catalog_schedule_spec(name: &'static str, is_active: bool) -> CatalogJobScheduleSpec<'static> {
    CatalogJobScheduleSpec {
        name,
        job_type: CATALOG_TEST_JOB,
        cron_expr: "0 * * * * *",
        payload_template: &CATALOG_SYNC_PAYLOAD,
        is_active,
        organization_id: None,
        max_jitter_seconds: 0,
        next_fire_at: None,
    }
}

#[tokio::test]
async fn sync_schedules_with_activates_schedule_after_definitions_sync() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_sync_schedules", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync definitions");

    let spec = catalog_schedule_spec("catalog-sync-schedule", true);
    catalog
        .sync_schedules_with(&pool, &[spec])
        .await
        .expect("sync schedules");

    let (is_active, _) = read_schedule_state(&pool, "catalog-sync-schedule").await;
    assert!(is_active);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_schedules_uses_catalog_registered_specs() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_sync_registered", 4).await;

    let catalog = JobCatalog::new()
        .job(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
        )
        .schedule(catalog_schedule_spec("catalog-registered-schedule", true));
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync definitions");
    let report = catalog
        .sync_schedules(&pool)
        .await
        .expect("sync registered schedules");
    assert_eq!(
        report.synced_schedule_names,
        vec!["catalog-registered-schedule"]
    );

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_schedules_exact_uses_registered_specs_and_derived_scope() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_sync_registered_exact", 4).await;

    let catalog = JobCatalog::new()
        .job(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
        )
        .schedule(catalog_schedule_spec(
            "catalog-registered-exact-schedule",
            true,
        ));
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync definitions");

    let scope = catalog
        .schedule_sync_scope()
        .expect("derive registered schedule scope");
    let report = catalog
        .sync_schedules_exact(&pool, &scope)
        .await
        .expect("exact sync registered schedules");
    assert_eq!(
        report.synced_schedule_names,
        vec!["catalog-registered-exact-schedule"]
    );
    assert!(report.deactivated_absent_schedule_names.is_empty());

    let (is_active, _) = read_schedule_state(&pool, "catalog-registered-exact-schedule").await;
    assert!(is_active);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_schedules_exact_with_rejects_dynamic_specs_outside_derived_scope() {
    let (pool, database) =
        setup_ephemeral_pool("runtime_catalog_sync_derived_scope_extra_spec", 4).await;

    let catalog = JobCatalog::new()
        .job(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
        )
        .schedule(catalog_schedule_spec(
            "catalog-registered-scope-schedule",
            true,
        ));

    let scope = catalog
        .schedule_sync_scope()
        .expect("derive registered schedule scope");
    let error = catalog
        .sync_schedules_exact_with(
            &pool,
            &scope,
            &[
                catalog_schedule_spec("catalog-registered-scope-schedule", true),
                catalog_schedule_spec("catalog-dynamic-outside-derived-scope", true),
            ],
        )
        .await
        .expect_err("dynamic spec outside derived scope");

    assert!(matches!(
        error,
        CatalogError::ScheduleNameOutsideExactSyncScope {
            name
        } if name == "catalog-dynamic-outside-derived-scope"
    ));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_schedules_with_rejects_unknown_job_type_before_database() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_sync_unknown_job", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    let payload = json!({});
    let error = catalog
        .sync_schedules_with(
            &pool,
            &[CatalogJobScheduleSpec {
                name: "missing-job-schedule",
                job_type: "jobs.missing",
                cron_expr: "0 * * * * *",
                payload_template: &payload,
                is_active: true,
                organization_id: None,
                max_jitter_seconds: 0,
                next_fire_at: None,
            }],
        )
        .await
        .expect_err("unknown job type");
    assert!(matches!(error, CatalogError::UnknownJobType { .. }));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_schedules_with_rejects_disabled_job_type_before_database() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_sync_disabled_job", 4).await;

    let catalog = JobCatalog::new()
        .job(
            CATALOG_TEST_JOB,
            CountingHandler {
                runs: Arc::new(AtomicUsize::new(0)),
            },
        )
        .defaults(JobCatalogDefaults::new().enabled(false));
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync definitions");

    let error = catalog
        .sync_schedules_with(
            &pool,
            &[catalog_schedule_spec("disabled-job-schedule", true)],
        )
        .await
        .expect_err("disabled job type");
    assert!(matches!(error, CatalogError::DisabledJobType { .. }));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_schedules_rejects_duplicate_names_in_sync_batch() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_sync_duplicate_batch", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    let first = catalog_schedule_spec("duplicate-batch-schedule", true);
    let second = catalog_schedule_spec("duplicate-batch-schedule", false);
    let error = catalog
        .sync_schedules_with(&pool, &[first, second])
        .await
        .expect_err("duplicate schedule name in sync batch");
    assert!(matches!(
        error,
        CatalogError::DuplicateScheduleNameInSyncBatch { .. }
    ));

    teardown_ephemeral_pool(pool, database).await;
}

#[test]
fn sync_schedules_rejects_duplicate_names_on_catalog() {
    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    let spec = catalog_schedule_spec("duplicate-schedule", true);
    let error = catalog
        .try_schedule(spec)
        .and_then(|catalog| {
            catalog.try_schedule(catalog_schedule_spec("duplicate-schedule", false))
        })
        .expect_err("duplicate schedule name on catalog");
    assert!(matches!(error, CatalogError::DuplicateScheduleName { .. }));
}

#[tokio::test]
async fn sync_schedules_exact_deactivates_absent_scoped_schedules() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_sync_schedules_exact", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync definitions");

    catalog
        .sync_schedules_with(
            &pool,
            &[
                catalog_schedule_spec("catalog-present-schedule", true),
                catalog_schedule_spec("catalog-absent-schedule", true),
            ],
        )
        .await
        .expect("seed schedules");

    let scope = JobCatalogScheduleSyncScope::schedule_names([
        "catalog-present-schedule",
        "catalog-absent-schedule",
    ])
    .expect("schedule scope");
    let report = catalog
        .sync_schedules_exact_with(
            &pool,
            &scope,
            &[catalog_schedule_spec("catalog-present-schedule", true)],
        )
        .await
        .expect("exact schedule sync");
    assert_eq!(
        report.deactivated_absent_schedule_names,
        vec!["catalog-absent-schedule"]
    );

    let (is_active, _) = read_schedule_state(&pool, "catalog-absent-schedule").await;
    assert!(!is_active);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_schedules_exact_waits_for_schedule_table_lock() {
    let (pool, database) =
        setup_ephemeral_pool("runtime_catalog_sync_schedule_exact_lock", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    let scope = JobCatalogScheduleSyncScope::schedule_name("catalog-lock-schedule")
        .expect("schedule scope");

    let mut blocker = pool.begin().await.expect("begin blocker transaction");
    sqlx::query("LOCK TABLE job_schedules IN ROW EXCLUSIVE MODE")
        .execute(&mut *blocker)
        .await
        .expect("hold conflicting schedule table lock");

    let blocked = timeout(
        Duration::from_millis(150),
        catalog.sync_schedules_exact_with(&pool, &scope, &[]),
    )
    .await;
    assert!(
        blocked.is_err(),
        "exact schedule sync should wait for the schedule table lock"
    );

    blocker.rollback().await.expect("release blocker lock");
    timeout(
        Duration::from_secs(5),
        catalog.sync_schedules_exact_with(&pool, &scope, &[]),
    )
    .await
    .expect("exact schedule sync should complete promptly after releasing blocker")
    .expect("exact schedule sync after releasing blocker");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_schedules_exact_with_empty_specs_deactivates_scope() {
    let (pool, database) =
        setup_ephemeral_pool("runtime_catalog_sync_schedules_exact_empty_specs", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync definitions");
    catalog
        .sync_schedules_with(
            &pool,
            &[
                catalog_schedule_spec("catalog-empty-exact-b", true),
                catalog_schedule_spec("catalog-empty-exact-a", true),
            ],
        )
        .await
        .expect("seed schedules");

    let scope = JobCatalogScheduleSyncScope::schedule_names([
        "catalog-empty-exact-a",
        "catalog-empty-exact-b",
    ])
    .expect("schedule scope");
    let report = catalog
        .sync_schedules_exact_with(&pool, &scope, &[])
        .await
        .expect("exact schedule sync");
    assert_eq!(
        report.deactivated_absent_schedule_names,
        vec!["catalog-empty-exact-a", "catalog-empty-exact-b"]
    );

    let (first_active, _) = read_schedule_state(&pool, "catalog-empty-exact-a").await;
    let (second_active, _) = read_schedule_state(&pool, "catalog-empty-exact-b").await;
    assert!(!first_active);
    assert!(!second_active);

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_schedules_exact_rejects_specs_outside_scope() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_sync_schedules_scope", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    let scope = JobCatalogScheduleSyncScope::schedule_name("catalog-present-schedule")
        .expect("schedule scope");
    let error = catalog
        .sync_schedules_exact_with(
            &pool,
            &scope,
            &[catalog_schedule_spec("catalog-out-of-scope-schedule", true)],
        )
        .await
        .expect_err("out-of-scope schedule");
    assert!(matches!(
        error,
        CatalogError::ScheduleNameOutsideExactSyncScope { .. }
    ));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_schedules_reports_failing_schedule_name() {
    let (pool, database) =
        setup_ephemeral_pool("runtime_catalog_sync_schedule_error_name", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    let error = catalog
        .sync_schedules_with(
            &pool,
            &[catalog_schedule_spec(
                "catalog-missing-definition-schedule",
                true,
            )],
        )
        .await
        .expect_err("missing database definition");

    match error {
        CatalogError::ScheduleSyncEntryFailure { name, .. } => {
            assert_eq!(name, "catalog-missing-definition-schedule");
        }
        other => panic!("expected schedule entry sync failure, got {other:?}"),
    }

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_schedules_preserves_next_fire_at_when_cron_is_unchanged() {
    let (pool, database) =
        setup_ephemeral_pool("runtime_catalog_sync_schedules_preserve_cursor", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync definitions");

    let mut spec = catalog_schedule_spec("catalog-cursor-schedule", true);
    spec.next_fire_at = Some(fixed_utc("2026-05-26T12:00:00Z"));
    catalog
        .sync_schedules_with(&pool, std::slice::from_ref(&spec))
        .await
        .expect("first schedule sync");

    spec.next_fire_at = Some(fixed_utc("2026-05-26T13:00:00Z"));
    catalog
        .sync_schedules_with(&pool, std::slice::from_ref(&spec))
        .await
        .expect("second schedule sync");

    let (_, next_fire_at) = read_schedule_state(&pool, "catalog-cursor-schedule").await;
    assert_eq!(next_fire_at, fixed_utc("2026-05-26T12:00:00Z"));

    spec.cron_expr = "0 30 * * * *";
    spec.next_fire_at = Some(fixed_utc("2026-05-26T13:00:00Z"));
    catalog
        .sync_schedules_with(&pool, std::slice::from_ref(&spec))
        .await
        .expect("third schedule sync");

    let (_, next_fire_at) = read_schedule_state(&pool, "catalog-cursor-schedule").await;
    assert_eq!(next_fire_at, fixed_utc("2026-05-26T13:00:00Z"));

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_schedules_is_idempotent() {
    let (pool, database) =
        setup_ephemeral_pool("runtime_catalog_sync_schedules_idempotent", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync definitions");

    let spec = catalog_schedule_spec("catalog-idempotent-schedule", true);
    catalog
        .sync_schedules_with(&pool, std::slice::from_ref(&spec))
        .await
        .expect("first schedule sync");
    catalog
        .sync_schedules_with(&pool, std::slice::from_ref(&spec))
        .await
        .expect("second schedule sync");

    teardown_ephemeral_pool(pool, database).await;
}

#[tokio::test]
async fn sync_schedules_with_applies_is_active_false() {
    let (pool, database) = setup_ephemeral_pool("runtime_catalog_sync_schedules_inactive", 4).await;

    let catalog = JobCatalog::new().job(
        CATALOG_TEST_JOB,
        CountingHandler {
            runs: Arc::new(AtomicUsize::new(0)),
        },
    );
    catalog
        .sync_definitions(&pool)
        .await
        .expect("sync definitions");

    catalog
        .sync_schedules_with(
            &pool,
            &[catalog_schedule_spec("catalog-flagged-schedule", true)],
        )
        .await
        .expect("activate schedule");
    catalog
        .sync_schedules_with(
            &pool,
            &[catalog_schedule_spec("catalog-flagged-schedule", false)],
        )
        .await
        .expect("deactivate schedule");

    let (is_active, _) = read_schedule_state(&pool, "catalog-flagged-schedule").await;
    assert!(!is_active);

    teardown_ephemeral_pool(pool, database).await;
}
