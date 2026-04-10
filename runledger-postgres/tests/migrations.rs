use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use runledger_postgres::{MIGRATOR, SchemaCompatibilityError, ensure_schema_compatible, migrate};
use sqlx::migrate::MigrateError;
use sqlx::{PgPool, postgres::PgPoolOptions};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::ContainerPort, runners::AsyncRunner,
};

const DEFAULT_POSTGRES_IMAGE: &str = "postgres:18";
const POSTGRES_USER: &str = "runledger";
const POSTGRES_PASSWORD: &str = "runledger";
const POSTGRES_DB: &str = "postgres";
const MAX_POSTGRES_BOOTSTRAP_ATTEMPTS: u8 = 40;
const MAX_PORT_RESOLVE_ATTEMPTS: u8 = 10;

static DATABASE_COUNTER: AtomicU64 = AtomicU64::new(1);

struct TestHarness {
    admin_url: String,
    database_name: String,
    pool: PgPool,
    _container: ContainerAsync<GenericImage>,
}

impl TestHarness {
    async fn fresh(prefix: &str) -> Self {
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
            .max_connections(4)
            .connect(&database_url)
            .await
            .expect("connect postgres");

        Self {
            admin_url,
            database_name,
            pool,
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

    migrate(&harness.pool).await.expect("apply migrations");
    ensure_schema_compatible(&harness.pool)
        .await
        .expect("schema should validate after migrate");

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
    assert_eq!(recorded_runledger_versions, runledger_migration_versions());

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

    harness.teardown().await;
}

#[tokio::test]
async fn migrate_ignores_unrelated_sqlx_history() {
    let harness = TestHarness::fresh("runledger_pg_migrate_shared").await;
    seed_unrelated_sqlx_migration(&harness.pool, 202401010001, false).await;

    migrate(&harness.pool)
        .await
        .expect("apply runledger migrations alongside app migrations");
    ensure_schema_compatible(&harness.pool)
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

    let error = ensure_schema_compatible(&harness.pool)
        .await
        .expect_err("validation should fail before migrations are applied");
    match error {
        SchemaCompatibilityError::MissingMigrationHistory {
            required_first_migration_version,
        } => {
            assert_eq!(required_first_migration_version, 202603280001);
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
    migrate(&harness.pool).await.expect("apply migrations");

    ensure_schema_compatible(&harness.pool)
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
    migrate(&harness.pool).await.expect("apply migrations");

    ensure_schema_compatible(&harness.pool)
        .await
        .expect("validation should ignore unrelated descriptions");

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

    let error = migrate(&harness.pool)
        .await
        .expect_err("migrate should reject conflicting version namespace");
    assert!(
        matches!(error, MigrateError::VersionMismatch(version) if version == conflicting_version),
        "unexpected migration error: {error}"
    );

    harness.teardown().await;
}

#[tokio::test]
async fn migrate_rejects_newer_runledger_migration_history() {
    let harness = TestHarness::fresh("runledger_pg_migrate_newer").await;
    migrate(&harness.pool)
        .await
        .expect("apply current migrations");

    let newer_version = runledger_migration_versions()
        .into_iter()
        .max()
        .expect("runledger should include at least one up migration")
        + 1;
    seed_runledger_migration_history(&harness.pool, newer_version).await;

    let error = migrate(&harness.pool)
        .await
        .expect_err("migrate should reject newer runledger history");
    assert!(
        matches!(error, MigrateError::VersionMissing(version) if version == newer_version),
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

    let error = ensure_schema_compatible(&harness.pool)
        .await
        .expect_err("validation should reject conflicting version namespace");
    assert!(
        matches!(
            error,
            SchemaCompatibilityError::Incompatible(MigrateError::VersionMismatch(version))
                if version == conflicting_version
        ),
        "unexpected schema compatibility error: {error}"
    );

    harness.teardown().await;
}

#[tokio::test]
async fn ensure_schema_compatible_rejects_newer_runledger_migration_history() {
    let harness = TestHarness::fresh("runledger_pg_validate_newer").await;
    migrate(&harness.pool)
        .await
        .expect("apply current migrations");

    let newer_version = runledger_migration_versions()
        .into_iter()
        .max()
        .expect("runledger should include at least one up migration")
        + 1;
    seed_runledger_migration_history(&harness.pool, newer_version).await;

    let error = ensure_schema_compatible(&harness.pool)
        .await
        .expect_err("validation should reject newer runledger history");
    assert!(
        matches!(
            error,
            SchemaCompatibilityError::Incompatible(MigrateError::VersionMissing(version))
                if version == newer_version
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
                if sqlx::query_scalar::<_, i64>("SELECT 1")
                    .fetch_one(&pool)
                    .await
                    .is_ok()
                {
                    pool.close().await;
                    return;
                }
                pool.close().await;
            }
            Err(err) => {
                if attempt == MAX_POSTGRES_BOOTSTRAP_ATTEMPTS {
                    panic!(
                        "connect postgres after {MAX_POSTGRES_BOOTSTRAP_ATTEMPTS} attempts: {err}"
                    );
                }
            }
        }

        tokio::time::sleep(Duration::from_millis(250)).await;
    }
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
