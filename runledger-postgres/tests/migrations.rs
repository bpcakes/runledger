use std::borrow::Cow;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use runledger_postgres::{
    MIGRATOR, SchemaCompatibilityError, ensure_schema_compatible_after_idempotency_cutover,
    migrate_after_idempotency_cutover,
};
use runledger_test_support::{TestDbConnectionBudgetPermit, acquire_test_db_connection_budget};
use serde_json::{Value, json};
use sqlx::migrate::{Migrate, MigrateError, Migrator};
use sqlx::{PgPool, postgres::PgPoolOptions};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::ContainerPort, runners::AsyncRunner,
};

const DEFAULT_POSTGRES_IMAGE: &str = "postgres:18";
const POSTGRES_USER: &str = "runledger";
const POSTGRES_PASSWORD: &str = "runledger";
const POSTGRES_DB: &str = "postgres";
const TEST_ADMIN_DATABASE_URL_ENV: &str = "RUNLEDGER_TEST_ADMIN_DATABASE_URL";
const MAX_POSTGRES_BOOTSTRAP_ATTEMPTS: u8 = 40;
const MAX_PORT_RESOLVE_ATTEMPTS: u8 = 10;
const ENQUEUE_REQUEST_CUTOVER_VERSION: i64 = 202605220001;
const V0_6_LATEST_MIGRATION_VERSION: i64 = 202606030001;
const REPLAY_METRICS_MIGRATION_VERSION: i64 = 202607190001;
const COMPATIBILITY_FENCE_EXEMPT_MIGRATION_VERSIONS: &[i64] = &[REPLAY_METRICS_MIGRATION_VERSION];
const TEST_HARNESS_POOL_CONNECTIONS: u32 = 4;
const TEST_HARNESS_ADMIN_CONNECTIONS: u32 = 1;

static DATABASE_COUNTER: AtomicU64 = AtomicU64::new(1);

struct TestHarness {
    admin_url: String,
    database_name: String,
    pool: PgPool,
    _connection_budget: TestDbConnectionBudgetPermit,
    _container: Option<ContainerAsync<GenericImage>>,
}

impl TestHarness {
    async fn fresh(prefix: &str) -> Self {
        let connection_budget = acquire_test_db_connection_budget(
            TEST_HARNESS_POOL_CONNECTIONS + TEST_HARNESS_ADMIN_CONNECTIONS,
        )
        .await;
        let (admin_url, container) =
            if let Ok(admin_url) = std::env::var(TEST_ADMIN_DATABASE_URL_ENV) {
                wait_for_postgres(&admin_url).await;
                (admin_url, None)
            } else {
                let image_ref = std::env::var("RUNLEDGER_TEST_PG_IMAGE")
                    .unwrap_or_else(|_| DEFAULT_POSTGRES_IMAGE.into());
                let (repository, tag) = parse_image_ref(&image_ref);
                let container = GenericImage::new(repository, tag)
                    .with_exposed_port(ContainerPort::Tcp(5432))
                    .with_env_var("POSTGRES_USER", POSTGRES_USER)
                    .with_env_var("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
                    .with_env_var("POSTGRES_DB", POSTGRES_DB)
                    .start()
                    .await
                    .expect("start postgres container");

                let port = resolve_host_port(&container, 5432).await;
                let admin_url = postgres_admin_url(port);
                wait_for_postgres(&admin_url).await;
                (admin_url, Some(container))
            };

        let database_name = build_database_name(prefix);
        let admin_pool = connect_admin_pool(&admin_url)
            .await
            .expect("connect admin postgres");
        let create_sql = format!("CREATE DATABASE {database_name}");
        sqlx::raw_sql(&create_sql)
            .execute(&admin_pool)
            .await
            .expect("create ephemeral database");
        admin_pool.close().await;

        let database_url = with_database_name(&admin_url, &database_name);
        let pool = PgPoolOptions::new()
            .max_connections(TEST_HARNESS_POOL_CONNECTIONS)
            .connect(&database_url)
            .await
            .expect("connect postgres");

        Self {
            admin_url,
            database_name,
            pool,
            _connection_budget: connection_budget,
            _container: container,
        }
    }

    async fn teardown(self) {
        self.pool.close().await;

        let admin_pool = connect_admin_pool(&self.admin_url)
            .await
            .expect("connect admin postgres");
        sqlx::query(
            "SELECT pg_terminate_backend(pid)
             FROM pg_stat_activity
             WHERE datname = $1
               AND pid <> pg_backend_pid()",
        )
        .bind(&self.database_name)
        .fetch_all(&admin_pool)
        .await
        .expect("terminate database backends");

        let drop_sql = format!("DROP DATABASE IF EXISTS {}", self.database_name);
        sqlx::raw_sql(&drop_sql)
            .execute(&admin_pool)
            .await
            .expect("drop ephemeral database");
        admin_pool.close().await;
    }
}

#[tokio::test]
async fn migrate_applies_bundled_schema_to_fresh_database() {
    let harness = TestHarness::fresh("runledger_pg_migrate").await;

    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply migrations");
    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("repeat migrate after constraints are validated");
    ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect("schema should validate after migrate");
    assert!(
        idempotency_cutover_constraints_valid(&harness.pool).await,
        "migrate should validate idempotency cutover constraints after legacy check passes"
    );

    let migrations_row_count =
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM _sqlx_migrations")
            .fetch_one(&harness.pool)
            .await
            .expect("count applied migrations");
    assert_eq!(
        migrations_row_count,
        runledger_migration_versions().len() as i64
    );

    let recorded_runledger_versions = sqlx::query_scalar::<_, i64>(
        "SELECT version FROM runledger_migration_history ORDER BY version",
    )
    .fetch_all(&harness.pool)
    .await
    .expect("list recorded runledger migrations");
    assert_eq!(
        recorded_runledger_versions,
        expected_compatibility_fence_versions()
    );

    let metrics_view_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
             SELECT 1
             FROM information_schema.views
             WHERE table_schema = 'public'
               AND table_name = 'job_metrics_rollup'
         )",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("query metrics view");
    assert!(metrics_view_exists);

    let continuation_metrics_view_exists = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS (
             SELECT 1
             FROM information_schema.views
             WHERE table_schema = 'public'
               AND table_name = 'job_continuation_metrics_rollup'
         )",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("query continuation metrics view");
    assert!(continuation_metrics_view_exists);
    assert_eq!(
        sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass('job_replays')::text")
            .fetch_one(&harness.pool)
            .await
            .expect("query job replay table"),
        Some("job_replays".to_owned())
    );

    harness.teardown().await;
}

#[tokio::test]
async fn replay_metrics_upgrade_preserves_data_and_exposes_raw_v0_6_rollback_boundary() {
    let harness = TestHarness::fresh("runledger_pg_replay_metrics_upgrade").await;
    let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
        .fetch_one(&harness.pool)
        .await
        .expect("read exact PostgreSQL version for migration compatibility regression");
    eprintln!("migration compatibility regression PostgreSQL server_version={server_version}");
    apply_runledger_migrations_through(&harness.pool, V0_6_LATEST_MIGRATION_VERSION).await;

    sqlx::query(
        "INSERT INTO job_definitions (
            job_type,
            version,
            max_attempts,
            default_timeout_seconds,
            default_priority,
            is_enabled
         )
         VALUES ('jobs.test.preexisting_continuation', 1, 3, 30, 100, true)",
    )
    .execute(&harness.pool)
    .await
    .expect("insert preexisting continuation definition");
    let job_id = sqlx::query_scalar::<_, sqlx::types::Uuid>(
        "INSERT INTO job_queue (
            job_type,
            payload,
            status,
            run_number,
            max_attempts,
            timeout_seconds,
            next_run_at
         )
         VALUES (
            'jobs.test.preexisting_continuation',
            '{}'::jsonb,
            'PENDING',
            2,
            3,
            30,
            clock_timestamp() + interval '1 hour'
         )
         RETURNING id",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("insert preexisting continued job");
    sqlx::query(
        "INSERT INTO job_events (
            job_id,
            run_number,
            event_type,
            payload
         )
         VALUES (
            $1,
            1,
            'REQUEUED',
            jsonb_build_object(
                'reason', 'HANDLER_CONTINUATION',
                'next_run_number', 2,
                'next_run_at', '2026-07-19T12:00:00Z',
                'delay_microseconds', 1000000
            )
         )",
    )
    .bind(job_id)
    .execute(&harness.pool)
    .await
    .expect("insert preexisting continuation event");

    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("upgrade v0.6 schema to replay and metrics migration");
    ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect("latest schema guard accepts upgraded database");

    let metrics = sqlx::query_as::<_, (i64, i64, i32)>(
        "SELECT continued_24h, active_continued_count, max_active_run_number
         FROM job_continuation_metrics_rollup
         WHERE organization_id IS NULL
           AND job_type = 'jobs.test.preexisting_continuation'",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("load upgraded continuation metrics");
    assert_eq!(metrics, (1, 1, 2));
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM job_queue WHERE id = $1")
            .bind(job_id)
            .fetch_one(&harness.pool)
            .await
            .expect("count preserved preexisting job"),
        1
    );

    let recorded_runledger_versions = sqlx::query_scalar::<_, i64>(
        "SELECT version FROM runledger_migration_history ORDER BY version",
    )
    .fetch_all(&harness.pool)
    .await
    .expect("list compatibility-fence migration history");
    assert_eq!(
        recorded_runledger_versions,
        expected_compatibility_fence_versions(),
        "additive migration must not make released v0.6.0 guards reject the schema"
    );

    let sqlx_history = sqlx::query_as::<_, (i64, Vec<u8>, bool)>(
        "SELECT version, checksum, success
         FROM _sqlx_migrations
         ORDER BY version",
    )
    .fetch_all(&harness.pool)
    .await
    .expect("load SQLx history after additive upgrade");
    assert!(
        sqlx_history
            .iter()
            .any(|(version, _, success)| *version == REPLAY_METRICS_MIGRATION_VERSION && *success)
    );
    for migration in MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .filter(|migration| migration.version <= V0_6_LATEST_MIGRATION_VERSION)
    {
        let (_, checksum, success) = sqlx_history
            .iter()
            .find(|(version, _, _)| *version == migration.version)
            .unwrap_or_else(|| panic!("missing v0.6 migration {}", migration.version));
        assert!(*success, "v0.6 migration {} is dirty", migration.version);
        assert_eq!(
            checksum.as_slice(),
            migration.checksum.as_ref(),
            "v0.6 migration {} checksum changed",
            migration.version
        );
    }

    let v0_6_migrator = raw_v0_6_migrator();
    let mut raw_connection = harness
        .pool
        .acquire()
        .await
        .expect("acquire connection for raw v0.6 migrator");
    let raw_error = v0_6_migrator
        .run(&mut *raw_connection)
        .await
        .expect_err("raw v0.6 SQLx migrator must reject newer applied history");
    assert!(
        matches!(
            &raw_error,
            MigrateError::VersionMissing(version)
                if *version == REPLAY_METRICS_MIGRATION_VERSION
        ),
        "unexpected raw v0.6 migration error: {raw_error}"
    );
    // SQLx's raw Migrator returns before unlocking on this validation error.
    // Discard the session so the advisory lock cannot wedge later startup.
    raw_connection
        .close()
        .await
        .expect("close raw v0.6 migration connection after VersionMissing");

    ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect("Runledger's filtered startup guard accepts the additive migration");

    MIGRATOR
        .undo(&harness.pool, V0_6_LATEST_MIGRATION_VERSION)
        .await
        .expect("current migrator can revert the post-v0.6 additive migration");
    for object in ["job_replays", "job_continuation_metrics_rollup"] {
        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass($1)::text")
                .bind(object)
                .fetch_one(&harness.pool)
                .await
                .unwrap_or_else(|error| panic!("query reverted object {object}: {error}")),
            None,
            "{object} must be absent after reverting to the raw v0.6 migration set"
        );
    }

    v0_6_migrator
        .run(&harness.pool)
        .await
        .expect("raw v0.6 migrator runs after newer history is reverted");
    let v0_6_versions = v0_6_migrator
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .map(|migration| migration.version)
        .collect::<Vec<_>>();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&harness.pool)
            .await
            .expect("list raw v0.6 SQLx history"),
        v0_6_versions
    );

    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("current migrator reapplies the additive migration after rollback");
    ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect("current guard accepts the reapplied schema");
    for object in ["job_replays", "job_continuation_metrics_rollup"] {
        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass($1)::text")
                .bind(object)
                .fetch_one(&harness.pool)
                .await
                .unwrap_or_else(|error| panic!("query reapplied object {object}: {error}")),
            Some(object.to_owned()),
            "{object} must be restored after reapplying current migrations"
        );
    }
    assert_eq!(
        sqlx::query_as::<_, (i64, i64, i32)>(
            "SELECT continued_24h, active_continued_count, max_active_run_number
             FROM job_continuation_metrics_rollup
             WHERE organization_id IS NULL
               AND job_type = 'jobs.test.preexisting_continuation'",
        )
        .fetch_one(&harness.pool)
        .await
        .expect("load continuation metrics after down/up cycle"),
        (1, 1, 2)
    );

    harness.teardown().await;
}

#[tokio::test]
async fn replay_metrics_down_drops_lineage_objects_but_preserves_queue_rows() {
    let harness = TestHarness::fresh("runledger_pg_replay_metrics_down").await;
    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply all migrations before replay down test");
    seed_legacy_job_definition(&harness.pool).await;

    let source_job_id = sqlx::query_scalar::<_, sqlx::types::Uuid>(
        "INSERT INTO job_queue (
            job_type, payload, status, max_attempts, timeout_seconds, finished_at
         )
         VALUES (
            'jobs.test.legacy_cutover', '{}'::jsonb, 'SUCCEEDED', 3, 30, now()
         )
         RETURNING id",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("insert replay source for down test");
    let replay_job_id = sqlx::query_scalar::<_, sqlx::types::Uuid>(
        "INSERT INTO job_queue (
            job_type, payload, status, max_attempts, timeout_seconds
         )
         VALUES (
            'jobs.test.legacy_cutover', '{}'::jsonb, 'PENDING', 3, 30
         )
         RETURNING id",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("insert replay job for down test");
    sqlx::query(
        "INSERT INTO job_replays (
            source_job_id,
            source_run_number,
            replay_request_key,
            replay_job_id,
            reason
         )
         VALUES ($1, 1, 'down-test', $2, 'verify destructive down boundary')",
    )
    .bind(source_job_id)
    .bind(replay_job_id)
    .execute(&harness.pool)
    .await
    .expect("insert replay lineage for down test");

    let replay_delete_policies = sqlx::query_as::<_, (String, String)>(
        "SELECT conname, confdeltype::text
         FROM pg_constraint
         WHERE conname IN (
            'fk_job_replays_source_job',
            'fk_job_replays_replay_job'
         )
         ORDER BY conname",
    )
    .fetch_all(&harness.pool)
    .await
    .expect("inspect replay lineage delete policy");
    assert_eq!(
        replay_delete_policies,
        vec![
            ("fk_job_replays_replay_job".to_owned(), "a".to_owned()),
            ("fk_job_replays_source_job".to_owned(), "c".to_owned()),
        ],
        "replay-only deletion must retain the idempotency guard while source deletion removes its lineage"
    );

    let down_migration = MIGRATOR
        .iter()
        .find(|migration| {
            migration.migration_type.is_down_migration()
                && migration.version == REPLAY_METRICS_MIGRATION_VERSION
        })
        .expect("replay and metrics down migration exists");
    let mut conn = harness
        .pool
        .acquire()
        .await
        .expect("acquire revert connection");
    (*conn)
        .revert(down_migration)
        .await
        .expect("revert replay and metrics migration");
    drop(conn);

    for object in ["job_replays", "job_continuation_metrics_rollup"] {
        assert_eq!(
            sqlx::query_scalar::<_, Option<String>>("SELECT to_regclass($1)::text")
                .bind(object)
                .fetch_one(&harness.pool)
                .await
                .unwrap_or_else(|error| panic!("query reverted object {object}: {error}")),
            None,
            "{object} must be removed by the down migration"
        );
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM job_queue WHERE id = ANY($1::uuid[])",)
            .bind(vec![source_job_id, replay_job_id])
            .fetch_one(&harness.pool)
            .await
            .expect("count queue rows after lineage down migration"),
        2,
        "down migration must not delete source or replay queue rows"
    );

    harness.teardown().await;
}

#[tokio::test]
async fn migrate_ignores_unrelated_sqlx_history() {
    let harness = TestHarness::fresh("runledger_pg_migrate_shared").await;
    seed_unrelated_sqlx_migration(&harness.pool, 202401010001, false).await;

    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply runledger migrations alongside app migrations");
    ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect("schema should validate when unrelated migrations are present");

    let migration_versions =
        sqlx::query_scalar::<_, i64>("SELECT version FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&harness.pool)
            .await
            .expect("list applied migrations");
    assert!(migration_versions.contains(&202401010001));
    for version in runledger_migration_versions() {
        assert!(migration_versions.contains(&version));
    }

    harness.teardown().await;
}

#[tokio::test]
async fn ensure_schema_compatible_rejects_fresh_database_without_migrations() {
    let harness = TestHarness::fresh("runledger_pg_validate").await;

    let error = ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect_err("validation should fail before migrations are applied");
    match &error {
        SchemaCompatibilityError::MissingMigrationHistory {
            required_first_migration_version,
        } => {
            assert_eq!(*required_first_migration_version, 202603280001);
        }
        other => panic!("unexpected migration validation error: {other}"),
    }
    assert!(
        error.to_string().contains("_sqlx_migrations"),
        "missing history error should explain the required table"
    );

    let migrations_table_exists =
        sqlx::query_scalar::<_, bool>("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
            .fetch_one(&harness.pool)
            .await
            .expect("check for migrations table");
    assert!(
        !migrations_table_exists,
        "schema compatibility validation must not create _sqlx_migrations"
    );

    harness.teardown().await;
}

#[tokio::test]
async fn ensure_schema_compatible_ignores_unrelated_sqlx_history() {
    let harness = TestHarness::fresh("runledger_pg_validate_shared").await;
    seed_unrelated_sqlx_migration(&harness.pool, 202401010001, false).await;
    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply migrations");

    ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect("validation should ignore unrelated migration versions");

    harness.teardown().await;
}

#[tokio::test]
async fn ensure_schema_compatible_ignores_unrelated_sqlx_history_with_runledger_description() {
    let harness = TestHarness::fresh("runledger_pg_validate_shared_named").await;
    seed_sqlx_migration(
        &harness.pool,
        202401010001,
        "runledger host app schema",
        true,
        vec![1_u8, 2, 3, 4],
    )
    .await;
    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply migrations");

    ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect("validation should ignore unrelated descriptions");

    harness.teardown().await;
}

#[test]
fn vendored_migration_copies_match_root_migrations() {
    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("postgres crate should live under workspace root");
    let root_migrations = workspace_root.join("migrations");

    assert_migration_dir_matches(
        &root_migrations,
        &workspace_root.join("runledger-postgres/migrations"),
    );
    assert_migration_dir_matches(
        &root_migrations,
        &workspace_root.join("runledger-test-support/migrations"),
    );
}

#[test]
fn compatibility_fence_exemptions_are_explicit_bundled_migrations() {
    let bundled_versions = runledger_migration_versions()
        .into_iter()
        .collect::<BTreeSet<_>>();
    let exempt_versions = COMPATIBILITY_FENCE_EXEMPT_MIGRATION_VERSIONS
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();

    assert_eq!(
        exempt_versions.len(),
        COMPATIBILITY_FENCE_EXEMPT_MIGRATION_VERSIONS.len(),
        "compatibility-fence exemptions must be unique"
    );
    assert!(
        exempt_versions.is_subset(&bundled_versions),
        "every compatibility-fence exemption must name a bundled up migration"
    );
    assert_eq!(
        expected_compatibility_fence_versions().len() + exempt_versions.len(),
        bundled_versions.len(),
        "every bundled up migration must be fenced unless explicitly exempted"
    );
}

#[tokio::test]
async fn ensure_schema_compatible_rejects_legacy_idempotency_rows() {
    let harness = TestHarness::fresh("runledger_pg_validate_legacy").await;
    apply_runledger_migrations_before_cutover(&harness.pool).await;
    seed_legacy_idempotency_rows(&harness.pool).await;
    apply_enqueue_request_cutover_migration(&harness.pool).await;
    apply_runledger_migrations_after_cutover(&harness.pool).await;

    let error = ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect_err("validation should reject legacy keyed rows without snapshots");
    assert_legacy_idempotency_snapshot_error(&error, 1, 1);

    harness.teardown().await;
}

#[tokio::test]
async fn migrate_after_idempotency_cutover_rejects_legacy_idempotency_rows() {
    let harness = TestHarness::fresh("runledger_pg_migrate_legacy").await;
    apply_runledger_migrations_before_cutover(&harness.pool).await;
    seed_legacy_idempotency_rows(&harness.pool).await;

    let error = migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect_err("migrate should reject legacy keyed rows without snapshots");
    assert_legacy_idempotency_snapshot_error(&error, 1, 1);

    harness.teardown().await;
}

#[tokio::test]
async fn workflow_results_migration_preserves_existing_enqueue_request_snapshots() {
    let harness = TestHarness::fresh("runledger_pg_workflow_results_preserve").await;
    apply_runledger_migrations_before_cutover(&harness.pool).await;
    apply_enqueue_request_cutover_migration(&harness.pool).await;
    insert_workflow_row_with_pre_result_snapshot(&harness.pool).await;

    apply_runledger_migrations_after_cutover(&harness.pool).await;

    let snapshot = sqlx::query_scalar::<_, Value>(
        "SELECT enqueue_request
         FROM workflow_runs
         WHERE idempotency_key = 'legacy-result-snapshot'
         LIMIT 1",
    )
    .fetch_one(&harness.pool)
    .await
    .expect("load preserved workflow snapshot");
    assert_eq!(
        snapshot,
        json!({
            "metadata": {},
            "steps": []
        })
    );
    assert!(snapshot.get("result_step_key").is_none());

    harness.teardown().await;
}

#[tokio::test]
async fn enqueue_request_cutover_constraints_reject_new_legacy_idempotency_rows() {
    let harness = TestHarness::fresh("runledger_pg_cutover_constraints").await;
    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply migrations");

    seed_legacy_job_definition(&harness.pool).await;

    let job_error = insert_legacy_job_row(&harness.pool, "legacy-job-after-cutover")
        .await
        .expect_err("job_queue constraint should reject keyed rows without enqueue_request");
    assert!(
        job_error
            .to_string()
            .contains("ck_job_queue_idempotency_enqueue_request"),
        "unexpected job constraint error: {job_error}"
    );

    let workflow_error = insert_legacy_workflow_row(&harness.pool, "legacy-workflow-after-cutover")
        .await
        .expect_err("workflow_runs constraint should reject keyed rows without enqueue_request");
    assert!(
        workflow_error
            .to_string()
            .contains("ck_workflow_runs_idempotency_enqueue_request"),
        "unexpected workflow constraint error: {workflow_error}"
    );

    harness.teardown().await;
}

#[tokio::test]
async fn migrate_rejects_conflicting_sqlx_version_namespace() {
    let harness = TestHarness::fresh("runledger_pg_migrate_conflict").await;
    let conflicting_version = runledger_migration_versions()
        .into_iter()
        .next()
        .expect("runledger should include at least one up migration");
    seed_unrelated_sqlx_migration(&harness.pool, conflicting_version, true).await;

    let error = migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect_err("migrate should reject conflicting version namespace");
    assert!(
        matches!(
            &error,
            SchemaCompatibilityError::Incompatible(MigrateError::VersionMismatch(version))
                if *version == conflicting_version
        ),
        "unexpected migration error: {error}"
    );

    harness.teardown().await;
}

#[tokio::test]
async fn migrate_rejects_newer_runledger_migration_history() {
    let harness = TestHarness::fresh("runledger_pg_migrate_newer").await;
    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply current migrations");

    let newer_version = runledger_migration_versions()
        .into_iter()
        .max()
        .expect("runledger should include at least one up migration")
        + 1;
    seed_runledger_migration_history(&harness.pool, newer_version).await;

    let error = migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect_err("migrate should reject newer runledger history");
    assert!(
        matches!(
            &error,
            SchemaCompatibilityError::Incompatible(MigrateError::VersionMissing(version))
                if *version == newer_version
        ),
        "unexpected migration error: {error}"
    );

    harness.teardown().await;
}

#[tokio::test]
async fn ensure_schema_compatible_rejects_conflicting_sqlx_version_namespace() {
    let harness = TestHarness::fresh("runledger_pg_validate_conflict").await;
    let conflicting_version = runledger_migration_versions()
        .into_iter()
        .next()
        .expect("runledger should include at least one up migration");
    seed_unrelated_sqlx_migration(&harness.pool, conflicting_version, true).await;

    let error = ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect_err("validation should reject conflicting version namespace");
    assert!(
        matches!(
            &error,
            SchemaCompatibilityError::Incompatible(MigrateError::VersionMismatch(version))
                if *version == conflicting_version
        ),
        "unexpected schema compatibility error: {error}"
    );

    harness.teardown().await;
}

#[tokio::test]
async fn ensure_schema_compatible_rejects_newer_runledger_migration_history() {
    let harness = TestHarness::fresh("runledger_pg_validate_newer").await;
    migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply current migrations");

    let newer_version = runledger_migration_versions()
        .into_iter()
        .max()
        .expect("runledger should include at least one up migration")
        + 1;
    seed_runledger_migration_history(&harness.pool, newer_version).await;

    let error = ensure_schema_compatible_after_idempotency_cutover(&harness.pool)
        .await
        .expect_err("validation should reject newer runledger history");
    assert!(
        matches!(
            &error,
            SchemaCompatibilityError::Incompatible(MigrateError::VersionMissing(version))
                if *version == newer_version
        ),
        "unexpected schema compatibility error: {error}"
    );

    harness.teardown().await;
}

async fn connect_admin_pool(admin_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_url)
        .await
}

async fn resolve_host_port(container: &ContainerAsync<GenericImage>, internal_port: u16) -> u16 {
    for attempt in 1..=MAX_PORT_RESOLVE_ATTEMPTS {
        match container.get_host_port_ipv4(internal_port).await {
            Ok(port) => return port,
            Err(err) => {
                if attempt == MAX_PORT_RESOLVE_ATTEMPTS {
                    panic!(
                        "resolve mapped postgres port after {MAX_PORT_RESOLVE_ATTEMPTS} attempts: {err}"
                    );
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }

    unreachable!()
}

async fn wait_for_postgres(admin_url: &str) {
    for attempt in 1..=MAX_POSTGRES_BOOTSTRAP_ATTEMPTS {
        match connect_admin_pool(admin_url).await {
            Ok(pool) => {
                let version_result = sqlx::query_scalar::<_, i32>(
                    "SELECT current_setting('server_version_num')::int",
                )
                .fetch_one(&pool)
                .await;
                pool.close().await;
                match version_result {
                    Ok(server_version_num) if server_version_num >= 180_000 => return,
                    Ok(server_version_num) => panic!(
                        "Runledger migration tests require PostgreSQL 18 or later; connected server_version_num was {server_version_num}"
                    ),
                    Err(error) if attempt == MAX_POSTGRES_BOOTSTRAP_ATTEMPTS => panic!(
                        "read PostgreSQL server_version_num after {MAX_POSTGRES_BOOTSTRAP_ATTEMPTS} attempts: {error}"
                    ),
                    Err(_) => {}
                }
            }
            Err(err) => {
                if attempt == MAX_POSTGRES_BOOTSTRAP_ATTEMPTS {
                    panic!(
                        "connect to PostgreSQL 18+ after {MAX_POSTGRES_BOOTSTRAP_ATTEMPTS} attempts: {err}"
                    );
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    panic!("unexpected PostgreSQL 18 bootstrap loop termination");
}

fn postgres_admin_url(port: u16) -> String {
    format!("postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@127.0.0.1:{port}/{POSTGRES_DB}")
}

fn parse_image_ref(image_ref: &str) -> (String, String) {
    let (name_and_tag, digest) = image_ref
        .split_once('@')
        .map_or((image_ref, None), |(name_and_tag, digest)| {
            (name_and_tag, Some(digest))
        });

    let last_slash = name_and_tag.rfind('/');
    let split_tag = name_and_tag
        .rfind(':')
        .filter(|index| last_slash.is_none_or(|slash| *index > slash));

    let (repository, mut tag) = split_tag.map_or_else(
        || (name_and_tag.to_owned(), String::from("latest")),
        |index| {
            (
                name_and_tag[..index].to_owned(),
                name_and_tag[index + 1..].to_owned(),
            )
        },
    );

    if let Some(digest) = digest {
        tag.push('@');
        tag.push_str(digest);
    }

    (repository, tag)
}

fn build_database_name(prefix: &str) -> String {
    let sanitized_prefix = sanitize_identifier(prefix);
    let compact_prefix = if sanitized_prefix.len() > 24 {
        sanitized_prefix[..24].to_string()
    } else {
        sanitized_prefix
    };
    let index = DATABASE_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}_{}_{}", compact_prefix, std::process::id(), index)
}

fn with_database_name(admin_url: &str, database_name: &str) -> String {
    let (base, _) = admin_url
        .rsplit_once('/')
        .expect("DATABASE_URL must include database name");
    format!("{base}/{database_name}")
}

fn sanitize_identifier(input: &str) -> String {
    let mut normalized = String::with_capacity(input.len() + 3);
    let mut previous_was_underscore = false;

    for ch in input.chars() {
        let mapped = if ch.is_ascii_alphanumeric() || ch == '_' {
            ch.to_ascii_lowercase()
        } else {
            '_'
        };

        if mapped == '_' {
            if !previous_was_underscore {
                normalized.push(mapped);
            }
            previous_was_underscore = true;
        } else {
            normalized.push(mapped);
            previous_was_underscore = false;
        }
    }

    if normalized.is_empty() {
        normalized.push_str("db");
    }
    if normalized
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_digit())
    {
        normalized.insert_str(0, "db_");
    }

    normalized
}

fn runledger_migration_versions() -> Vec<i64> {
    MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .map(|migration| migration.version)
        .collect()
}

fn raw_v0_6_migrator() -> Migrator {
    let migrations = MIGRATOR
        .iter()
        .filter(|migration| migration.version <= V0_6_LATEST_MIGRATION_VERSION)
        .cloned()
        .collect::<Vec<_>>();
    assert!(
        migrations.iter().any(|migration| {
            migration.version == V0_6_LATEST_MIGRATION_VERSION
                && migration.migration_type.is_up_migration()
        }),
        "raw v0.6 migration fixture must include its final up migration"
    );

    Migrator {
        migrations: Cow::Owned(migrations),
        ..Migrator::DEFAULT
    }
}

fn expected_compatibility_fence_versions() -> Vec<i64> {
    runledger_migration_versions()
        .into_iter()
        .filter(|version| !COMPATIBILITY_FENCE_EXEMPT_MIGRATION_VERSIONS.contains(version))
        .collect()
}

fn assert_migration_dir_matches(expected_dir: &Path, actual_dir: &Path) {
    let expected_names = migration_file_names(expected_dir);
    let actual_names = migration_file_names(actual_dir);
    assert_eq!(
        actual_names,
        expected_names,
        "migration filenames in {} must match {}",
        actual_dir.display(),
        expected_dir.display()
    );

    for name in expected_names {
        let expected = fs::read(expected_dir.join(&name)).unwrap_or_else(|error| {
            panic!(
                "read expected migration {} from {}: {error}",
                name,
                expected_dir.display()
            )
        });
        let actual = fs::read(actual_dir.join(&name)).unwrap_or_else(|error| {
            panic!(
                "read actual migration {} from {}: {error}",
                name,
                actual_dir.display()
            )
        });
        assert_eq!(
            actual,
            expected,
            "migration {} in {} must match {}",
            name,
            actual_dir.display(),
            expected_dir.display()
        );
    }
}

fn migration_file_names(dir: &Path) -> BTreeSet<String> {
    fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("read migration directory {}: {error}", dir.display()))
        .map(|entry| {
            let entry = entry.unwrap_or_else(|error| {
                panic!("read migration entry in {}: {error}", dir.display())
            });
            entry
                .file_name()
                .to_str()
                .unwrap_or_else(|| {
                    panic!(
                        "migration file name is not valid UTF-8 in {}",
                        dir.display()
                    )
                })
                .to_owned()
        })
        .filter(|name| name.ends_with(".sql"))
        .collect()
}

async fn apply_runledger_migrations_before_cutover(pool: &PgPool) {
    apply_runledger_migrations_through(pool, ENQUEUE_REQUEST_CUTOVER_VERSION - 1).await;
}

async fn apply_runledger_migrations_through(pool: &PgPool, latest_version: i64) {
    let mut conn = pool.acquire().await.expect("acquire migration connection");
    (*conn)
        .ensure_migrations_table()
        .await
        .expect("create sqlx migrations table");

    for migration in MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .filter(|migration| migration.version <= latest_version)
    {
        (*conn).apply(migration).await.unwrap_or_else(|error| {
            panic!(
                "apply Runledger migration {} through {latest_version}: {error}",
                migration.version
            )
        });
    }
}

async fn apply_enqueue_request_cutover_migration(pool: &PgPool) {
    let mut conn = pool.acquire().await.expect("acquire migration connection");
    let migration = MIGRATOR
        .iter()
        .find(|migration| {
            migration.migration_type.is_up_migration()
                && migration.version == ENQUEUE_REQUEST_CUTOVER_VERSION
        })
        .expect("enqueue request cutover migration should exist");

    (*conn)
        .apply(migration)
        .await
        .expect("apply enqueue request cutover migration");
}

async fn apply_runledger_migrations_after_cutover(pool: &PgPool) {
    let mut conn = pool.acquire().await.expect("acquire migration connection");

    for migration in MIGRATOR
        .iter()
        .filter(|migration| migration.migration_type.is_up_migration())
        .filter(|migration| migration.version > ENQUEUE_REQUEST_CUTOVER_VERSION)
    {
        (*conn).apply(migration).await.unwrap_or_else(|error| {
            panic!(
                "apply post-cutover Runledger migration {}: {error}",
                migration.version
            )
        });
    }
}

fn assert_legacy_idempotency_snapshot_error(
    error: &SchemaCompatibilityError,
    job_count: i64,
    workflow_count: i64,
) {
    assert!(
        matches!(
            error,
            SchemaCompatibilityError::LegacyIdempotencySnapshotsMissing {
                job_count: actual_job_count,
                workflow_count: actual_workflow_count,
            } if *actual_job_count == job_count && *actual_workflow_count == workflow_count
        ),
        "unexpected schema compatibility error: {error}"
    );
}

async fn seed_runledger_migration_history(pool: &PgPool, version: i64) {
    sqlx::query(
        r#"
CREATE TABLE IF NOT EXISTS runledger_migration_history (
    version BIGINT PRIMARY KEY,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
        "#,
    )
    .execute(pool)
    .await
    .expect("create runledger migration history table");

    sqlx::query(
        r#"
INSERT INTO runledger_migration_history (version)
VALUES ($1)
        "#,
    )
    .bind(version)
    .execute(pool)
    .await
    .expect("insert runledger migration history");
}

async fn idempotency_cutover_constraints_valid(pool: &PgPool) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT COUNT(*) FILTER (WHERE c.convalidated) = 2
         FROM pg_constraint c
         JOIN pg_class t ON t.oid = c.conrelid
         WHERE (t.relname, c.conname) IN (
             ('job_queue', 'ck_job_queue_idempotency_enqueue_request'),
             ('workflow_runs', 'ck_workflow_runs_idempotency_enqueue_request')
         )",
    )
    .fetch_one(pool)
    .await
    .expect("check idempotency cutover constraint validation")
}

async fn seed_legacy_idempotency_rows(pool: &PgPool) {
    seed_legacy_job_definition(pool).await;
    insert_legacy_job_row(pool, "legacy-job")
        .await
        .expect("insert legacy job row");
    insert_legacy_workflow_row(pool, "legacy-workflow")
        .await
        .expect("insert legacy workflow row");
}

async fn seed_legacy_job_definition(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO job_definitions (
            job_type,
            version,
            max_attempts,
            default_timeout_seconds,
            default_priority,
            is_enabled
         )
         VALUES ('jobs.test.legacy_cutover', 1, 3, 30, 100, true)",
    )
    .execute(pool)
    .await
    .expect("insert job definition");
}

async fn insert_legacy_job_row(
    pool: &PgPool,
    idempotency_key: &str,
) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    sqlx::query(
        "INSERT INTO job_queue (
            job_type,
            payload,
            priority,
            max_attempts,
            timeout_seconds,
            idempotency_key
         )
         VALUES (
            'jobs.test.legacy_cutover',
            '{}'::jsonb,
            100,
            3,
            30,
            $1
         )",
    )
    .bind(idempotency_key)
    .execute(pool)
    .await
}

async fn insert_legacy_workflow_row(
    pool: &PgPool,
    idempotency_key: &str,
) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    sqlx::query(
        "INSERT INTO workflow_runs (
            workflow_type,
            idempotency_key,
            metadata
         )
         VALUES (
            'workflow.test.legacy_cutover',
            $1,
            '{}'::jsonb
         )",
    )
    .bind(idempotency_key)
    .execute(pool)
    .await
}

async fn insert_workflow_row_with_pre_result_snapshot(pool: &PgPool) {
    sqlx::query(
        "INSERT INTO workflow_runs (
            workflow_type,
            idempotency_key,
            metadata,
            enqueue_request
         )
         VALUES (
            'workflow.test.pre_result_snapshot',
            'legacy-result-snapshot',
            '{}'::jsonb,
            $1::jsonb
         )",
    )
    .bind(json!({
        "metadata": {},
        "steps": []
    }))
    .execute(pool)
    .await
    .expect("insert pre-result workflow snapshot");
}

async fn seed_unrelated_sqlx_migration(pool: &PgPool, version: i64, success: bool) {
    seed_sqlx_migration(
        pool,
        version,
        "host app schema",
        success,
        vec![1_u8, 2, 3, 4],
    )
    .await;
}

async fn seed_sqlx_migration(
    pool: &PgPool,
    version: i64,
    description: &str,
    success: bool,
    checksum: Vec<u8>,
) {
    sqlx::query(
        r#"
CREATE TABLE IF NOT EXISTS _sqlx_migrations (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    installed_on TIMESTAMPTZ NOT NULL DEFAULT now(),
    success BOOLEAN NOT NULL,
    checksum BYTEA NOT NULL,
    execution_time BIGINT NOT NULL
)
        "#,
    )
    .execute(pool)
    .await
    .expect("create shared sqlx migrations table");

    sqlx::query(
        r#"
INSERT INTO _sqlx_migrations (version, description, success, checksum, execution_time)
VALUES ($1, $2, $3, $4, 0)
        "#,
    )
    .bind(version)
    .bind(description)
    .bind(success)
    .bind(checksum)
    .execute(pool)
    .await
    .expect("insert sqlx migration history");
}
