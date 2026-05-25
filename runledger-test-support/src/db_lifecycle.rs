use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use sqlx::{PgPool, postgres::PgPoolOptions};

use crate::postgres_container::admin_database_url;

static DATABASE_COUNTER: AtomicU64 = AtomicU64::new(1);

pub async fn setup_ephemeral_pool(
    prefix: &str,
    max_connections: u32,
) -> (PgPool, EphemeralDatabase) {
    let database = create_ephemeral_database(prefix)
        .await
        .expect("create ephemeral database");
    let pool = PgPoolOptions::new()
        .max_connections(max_connections)
        .connect(&database.url)
        .await
        .expect("connect postgres");

    let migrations_dir = runledger_migrations_dir();
    let migrator = sqlx::migrate::Migrator::new(migrations_dir.as_path())
        .await
        .expect("load migrations");
    migrator.run(&pool).await.expect("run migrations");
    (pool, database)
}

fn runledger_migrations_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("migrations")
}

pub async fn teardown_ephemeral_pool(pool: PgPool, database: EphemeralDatabase) {
    pool.close().await;
    drop_database(&database.name)
        .await
        .expect("drop ephemeral database");
}

#[derive(Debug)]
pub struct EphemeralDatabase {
    pub name: String,
    pub url: String,
}

impl Drop for EphemeralDatabase {
    fn drop(&mut self) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let name = self.name.clone();
            handle.spawn(async move {
                let _ = drop_database(&name).await;
            });
        }
    }
}

pub async fn create_ephemeral_database(prefix: &str) -> Result<EphemeralDatabase, sqlx::Error> {
    let admin_url = admin_database_url().await;
    let name = build_database_name(prefix);
    let admin_pool = connect_admin_pool(admin_url).await?;

    let create_sql = format!("CREATE DATABASE {name}");
    sqlx::raw_sql(&create_sql).execute(&admin_pool).await?;
    admin_pool.close().await;

    Ok(EphemeralDatabase {
        url: with_database_name(admin_url, &name),
        name,
    })
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
