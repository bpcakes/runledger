use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::postgres_container::admin_database_url;

const TEST_DB_CONNECTION_BUDGET_ENV: &str = "RUNLEDGER_TEST_DB_CONNECTION_BUDGET";
const DEFAULT_TEST_DB_CONNECTION_BUDGET: usize = 64;
const DEFAULT_CREATE_DATABASE_PERMITS: u32 = 9;

static DATABASE_COUNTER: AtomicU64 = AtomicU64::new(1);
static EPHEMERAL_DB_CONNECTION_BUDGET: OnceLock<Arc<Semaphore>> = OnceLock::new();

#[derive(Debug)]
pub struct TestDbConnectionBudgetPermit {
    _permit: OwnedSemaphorePermit,
}

pub async fn setup_ephemeral_pool(
    prefix: &str,
    max_connections: u32,
) -> (PgPool, EphemeralDatabase) {
    let (pool, database) = setup_unmigrated_ephemeral_pool(prefix, max_connections).await;

    let migrations_dir = runledger_migrations_dir();
    let migrator = sqlx::migrate::Migrator::new(migrations_dir.as_path())
        .await
        .expect("load migrations");
    migrator.run(&pool).await.expect("run migrations");
    (pool, database)
}

pub async fn setup_ephemeral_pool_with_untracked_migrations(
    prefix: &str,
    max_connections: u32,
) -> (PgPool, EphemeralDatabase) {
    let (pool, database) = setup_unmigrated_ephemeral_pool(prefix, max_connections).await;
    apply_untracked_runledger_migrations(&pool)
        .await
        .expect("run migrations");
    (pool, database)
}

pub async fn setup_unmigrated_ephemeral_pool(
    prefix: &str,
    max_connections: u32,
) -> (PgPool, EphemeralDatabase) {
    let permit = acquire_ephemeral_db_permit(max_connections.saturating_add(1)).await;
    let database = create_ephemeral_database_with_permit(prefix, permit)
        .await
        .expect("create ephemeral database");
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(database.url())
        .await
        .expect("connect postgres");
    (pool, database)
}

async fn apply_untracked_runledger_migrations(
    pool: &PgPool,
) -> Result<(), Box<dyn std::error::Error>> {
    for migration_path in runledger_migration_paths()? {
        let sql = std::fs::read_to_string(&migration_path)?;
        sqlx::raw_sql(&sql).execute(pool).await?;
    }
    Ok(())
}

fn runledger_migration_paths() -> Result<Vec<PathBuf>, std::io::Error> {
    let mut paths = std::fs::read_dir(runledger_migrations_dir())?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<Result<Vec<_>, _>>()?;

    paths.retain(|path| path.extension().is_some_and(|ext| ext == "sql"));
    paths.retain(|path| {
        path.file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".up.sql"))
    });
    paths.sort();
    Ok(paths)
}

fn runledger_migrations_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations")
}

pub async fn teardown_ephemeral_pool(pool: PgPool, database: EphemeralDatabase) {
    pool.close().await;
    database.teardown().await.expect("drop ephemeral database");
}

#[derive(Debug)]
pub struct EphemeralDatabase {
    identity: EphemeralDatabaseIdentity,
    permit: Option<OwnedSemaphorePermit>,
}

#[derive(Debug)]
struct EphemeralDatabaseIdentity {
    name: String,
    url: String,
}

impl EphemeralDatabase {
    #[must_use]
    pub fn name(&self) -> &str {
        &self.identity.name
    }

    #[must_use]
    pub fn url(&self) -> &str {
        &self.identity.url
    }

    pub async fn teardown(mut self) -> Result<(), sqlx::Error> {
        drop_database(self.name()).await?;
        drop(self.permit.take());
        Ok(())
    }
}

impl Drop for EphemeralDatabase {
    fn drop(&mut self) {
        let Some(permit) = self.permit.take() else {
            return;
        };
        let name = self.identity.name.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _permit = permit;
                let _ = drop_database(&name).await;
            });
        }
    }
}

pub async fn create_ephemeral_database(prefix: &str) -> Result<EphemeralDatabase, sqlx::Error> {
    let permit = acquire_ephemeral_db_permit(DEFAULT_CREATE_DATABASE_PERMITS).await;
    create_ephemeral_database_with_permit(prefix, permit).await
}

async fn create_ephemeral_database_with_permit(
    prefix: &str,
    permit: OwnedSemaphorePermit,
) -> Result<EphemeralDatabase, sqlx::Error> {
    let admin_url = admin_database_url().await;
    let name = build_database_name(prefix);
    let admin_pool = connect_admin_pool(admin_url).await?;

    let create_sql = format!("CREATE DATABASE {name}");
    sqlx::raw_sql(&create_sql).execute(&admin_pool).await?;
    admin_pool.close().await;

    Ok(EphemeralDatabase {
        identity: EphemeralDatabaseIdentity {
            url: with_database_name(admin_url, &name),
            name,
        },
        permit: Some(permit),
    })
}

pub async fn acquire_test_db_connection_budget(
    requested_permits: u32,
) -> TestDbConnectionBudgetPermit {
    TestDbConnectionBudgetPermit {
        _permit: acquire_ephemeral_db_permit(requested_permits).await,
    }
}

async fn acquire_ephemeral_db_permit(requested_permits: u32) -> OwnedSemaphorePermit {
    let budget = ephemeral_db_connection_budget();
    let budget_size = ephemeral_db_connection_budget_size();
    let permits = requested_permits.max(1);
    assert!(
        permits as usize <= budget_size,
        "ephemeral database requested {permits} connection permits, exceeding {TEST_DB_CONNECTION_BUDGET_ENV}={budget_size}"
    );
    budget
        .acquire_many_owned(permits)
        .await
        .expect("ephemeral database connection budget should not be closed")
}

fn ephemeral_db_connection_budget() -> Arc<Semaphore> {
    EPHEMERAL_DB_CONNECTION_BUDGET
        .get_or_init(|| Arc::new(Semaphore::new(ephemeral_db_connection_budget_size())))
        .clone()
}

fn ephemeral_db_connection_budget_size() -> usize {
    std::env::var(TEST_DB_CONNECTION_BUDGET_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_TEST_DB_CONNECTION_BUDGET)
}

pub async fn drop_database(database_name: &str) -> Result<(), sqlx::Error> {
    let admin_url = admin_database_url().await;
    let normalized = sanitize_identifier(database_name);
    let admin_pool = connect_admin_pool(admin_url).await?;

    sqlx::query(
        "SELECT pg_terminate_backend(pid)
         FROM pg_stat_activity
         WHERE datname = $1
           AND pid <> pg_backend_pid()",
    )
    .bind(&normalized)
    .fetch_all(&admin_pool)
    .await?;

    let drop_sql = format!("DROP DATABASE IF EXISTS {normalized}");
    sqlx::raw_sql(&drop_sql).execute(&admin_pool).await?;
    admin_pool.close().await;

    Ok(())
}

async fn connect_admin_pool(admin_url: &str) -> Result<PgPool, sqlx::Error> {
    PgPoolOptions::new()
        .max_connections(1)
        .connect(admin_url)
        .await
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

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const CLEANUP_ATTEMPTS: usize = 100;
    const CLEANUP_POLL_INTERVAL: Duration = Duration::from_millis(50);
    static LIFECYCLE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test]
    async fn setup_modes_preserve_migration_history_semantics() {
        let _guard = LIFECYCLE_TEST_LOCK.lock().await;
        let (pool, database) = setup_unmigrated_ephemeral_pool("support_unmigrated", 1).await;
        assert!(!relation_exists(&pool, "job_queue").await);
        assert!(!relation_exists(&pool, "_sqlx_migrations").await);
        teardown_ephemeral_pool(pool, database).await;

        let (pool, database) =
            setup_ephemeral_pool_with_untracked_migrations("support_untracked", 1).await;
        assert!(relation_exists(&pool, "job_queue").await);
        assert!(!relation_exists(&pool, "_sqlx_migrations").await);
        teardown_ephemeral_pool(pool, database).await;

        let (pool, database) = setup_ephemeral_pool("support_tracked", 1).await;
        assert!(relation_exists(&pool, "job_queue").await);
        assert!(relation_exists(&pool, "_sqlx_migrations").await);
        teardown_ephemeral_pool(pool, database).await;
    }

    #[tokio::test]
    async fn explicit_teardown_preserves_identity_and_releases_exact_permit_count() {
        let _guard = LIFECYCLE_TEST_LOCK.lock().await;
        let available_before = ephemeral_db_connection_budget().available_permits();
        let max_connections = 3;
        let reserved_permits = max_connections as usize + 1;
        assert!(available_before >= reserved_permits);

        let (pool, database) =
            setup_unmigrated_ephemeral_pool("support_explicit_teardown", max_connections).await;
        let name = database.name().to_owned();
        let url = database.url().to_owned();

        assert_eq!(
            ephemeral_db_connection_budget().available_permits(),
            available_before - reserved_permits
        );
        assert_eq!(database.name(), name);
        assert_eq!(database.url(), url);
        assert!(url.ends_with(&format!("/{name}")));
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT current_database()")
                .fetch_one(&pool)
                .await
                .expect("read current database"),
            name
        );
        record_postgres_version(&pool).await;

        teardown_ephemeral_pool(pool, database).await;

        assert!(!database_exists(&name).await);
        assert_eq!(
            ephemeral_db_connection_budget().available_permits(),
            available_before
        );
    }

    #[tokio::test]
    async fn drop_fallback_cleans_database_and_releases_exact_permit_count() {
        let _guard = LIFECYCLE_TEST_LOCK.lock().await;
        let available_before = ephemeral_db_connection_budget().available_permits();
        let max_connections = 2;
        let reserved_permits = max_connections as usize + 1;
        let (pool, database) =
            setup_unmigrated_ephemeral_pool("support_drop_fallback", max_connections).await;
        let name = database.name().to_owned();

        assert_eq!(
            ephemeral_db_connection_budget().available_permits(),
            available_before - reserved_permits
        );
        pool.close().await;
        drop(database);

        wait_for_database_cleanup_and_permits(&name, available_before).await;
    }

    #[tokio::test]
    async fn panic_path_runs_drop_fallback_without_leaking_permits() {
        let _guard = LIFECYCLE_TEST_LOCK.lock().await;
        let available_before = ephemeral_db_connection_budget().available_permits();
        let max_connections = 1;
        let reserved_permits = max_connections as usize + 1;
        let (pool, database) =
            setup_unmigrated_ephemeral_pool("support_panic_fallback", max_connections).await;
        let name = database.name().to_owned();

        assert_eq!(
            ephemeral_db_connection_budget().available_permits(),
            available_before - reserved_permits
        );
        let panic_task = tokio::spawn(async move {
            let _pool = pool;
            let _database = database;
            panic!("intentional lifecycle panic");
        });
        assert!(panic_task.await.expect_err("task should panic").is_panic());

        wait_for_database_cleanup_and_permits(&name, available_before).await;
    }

    async fn record_postgres_version(pool: &PgPool) {
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
            "db lifecycle PostgreSQL server_version={server_version} server_version_num={server_version_num}"
        );
        assert!(
            server_version_num >= 180_000,
            "db lifecycle tests require PostgreSQL 18+, got {server_version} ({server_version_num})"
        );
    }

    async fn wait_for_database_cleanup_and_permits(
        database_name: &str,
        expected_available_permits: usize,
    ) {
        for _ in 0..CLEANUP_ATTEMPTS {
            if !database_exists(database_name).await
                && ephemeral_db_connection_budget().available_permits()
                    == expected_available_permits
            {
                return;
            }
            tokio::time::sleep(CLEANUP_POLL_INTERVAL).await;
        }

        panic!(
            "database {database_name} or its permits were not cleaned up; database_exists={} available_permits={} expected_available_permits={expected_available_permits}",
            database_exists(database_name).await,
            ephemeral_db_connection_budget().available_permits(),
        );
    }

    async fn database_exists(database_name: &str) -> bool {
        let admin_pool = connect_admin_pool(admin_database_url().await)
            .await
            .expect("connect admin pool");
        let exists = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)",
        )
        .bind(database_name)
        .fetch_one(&admin_pool)
        .await
        .expect("query database existence");
        admin_pool.close().await;
        exists
    }

    async fn relation_exists(pool: &PgPool, name: &str) -> bool {
        sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
            .bind(name)
            .fetch_one(pool)
            .await
            .expect("query relation existence")
    }
}
