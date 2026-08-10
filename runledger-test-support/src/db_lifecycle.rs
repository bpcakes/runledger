use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use sqlx::{PgPool, postgres::PgPoolOptions};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::postgres_container::admin_database_url;

const TEST_DB_CONNECTION_BUDGET_ENV: &str = "RUNLEDGER_TEST_DB_CONNECTION_BUDGET";
const DEFAULT_TEST_DB_CONNECTION_BUDGET: usize = 64;
const DEFAULT_CREATE_DATABASE_PERMITS: u32 = 9;

static DATABASE_COUNTER: AtomicU64 = AtomicU64::new(1);
static EPHEMERAL_DB_CONNECTION_BUDGET: OnceLock<Arc<Semaphore>> = OnceLock::new();
static EPHEMERAL_DB_PERMITS: OnceLock<Mutex<HashMap<String, OwnedSemaphorePermit>>> =
    OnceLock::new();

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
        .connect(&database.url)
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
    drop_database(&database.name)
        .await
        .expect("drop ephemeral database");
    release_ephemeral_db_permit(&database.name);
    std::mem::forget(database);
}

#[derive(Debug)]
pub struct EphemeralDatabase {
    pub name: String,
    pub url: String,
}

impl Drop for EphemeralDatabase {
    fn drop(&mut self) {
        let name = self.name.clone();
        let permit = take_ephemeral_db_permit(&name);
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
    retain_ephemeral_db_permit(&name, permit);

    Ok(EphemeralDatabase {
        url: with_database_name(admin_url, &name),
        name,
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

fn retain_ephemeral_db_permit(database_name: &str, permit: OwnedSemaphorePermit) {
    let previous = ephemeral_db_permits()
        .lock()
        .expect("ephemeral database permit registry should not be poisoned")
        .insert(database_name.to_owned(), permit);
    debug_assert!(
        previous.is_none(),
        "ephemeral database names should be unique"
    );
}

fn release_ephemeral_db_permit(database_name: &str) {
    let _permit = take_ephemeral_db_permit(database_name);
}

fn take_ephemeral_db_permit(database_name: &str) -> Option<OwnedSemaphorePermit> {
    ephemeral_db_permits()
        .lock()
        .expect("ephemeral database permit registry should not be poisoned")
        .remove(database_name)
}

fn ephemeral_db_permits() -> &'static Mutex<HashMap<String, OwnedSemaphorePermit>> {
    EPHEMERAL_DB_PERMITS.get_or_init(|| Mutex::new(HashMap::new()))
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
    use super::*;

    #[tokio::test]
    async fn setup_modes_preserve_migration_history_semantics() {
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

    async fn relation_exists(pool: &PgPool, name: &str) -> bool {
        sqlx::query_scalar::<_, bool>("SELECT to_regclass($1) IS NOT NULL")
            .bind(name)
            .fetch_one(pool)
            .await
            .expect("query relation existence")
    }
}
