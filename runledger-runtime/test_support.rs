use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

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
static SHARED_ADMIN_URL: OnceLock<String> = OnceLock::new();
static SHARED_HARNESS_INIT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

    apply_runledger_migrations(&pool)
        .await
        .expect("run migrations");
    (pool, database)
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

async fn apply_runledger_migrations(pool: &PgPool) -> Result<(), Box<dyn std::error::Error>> {
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
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../migrations")
}

async fn admin_database_url() -> &'static str {
    if let Some(admin_url) = SHARED_ADMIN_URL.get() {
        return admin_url;
    }

    let _init_guard = SHARED_HARNESS_INIT_LOCK.lock().await;
    if let Some(admin_url) = SHARED_ADMIN_URL.get() {
        return admin_url;
    }

    let image_ref =
        std::env::var("RUNLEDGER_TEST_PG_IMAGE").unwrap_or_else(|_| DEFAULT_POSTGRES_IMAGE.into());
    let (repository, tag) = parse_image_ref(&image_ref);

    let image = GenericImage::new(repository, tag)
        .with_exposed_port(ContainerPort::Tcp(5432))
        .with_env_var("POSTGRES_USER", POSTGRES_USER)
        .with_env_var("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
        .with_env_var("POSTGRES_DB", POSTGRES_DB);
    let container = image.start().await.expect("start postgres container");

    let port = resolve_host_port(&container, 5432).await;
    let admin_url = postgres_admin_url(port);

    wait_for_postgres(&admin_url).await;
    retain_container_for_process_lifetime(container);

    SHARED_ADMIN_URL
        .set(admin_url)
        .expect("shared postgres admin URL must be set once");
    SHARED_ADMIN_URL
        .get()
        .expect("shared postgres admin URL must be initialized")
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

fn postgres_admin_url(port: u16) -> String {
    format!("postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@127.0.0.1:{port}/{POSTGRES_DB}")
}

fn retain_container_for_process_lifetime(container: ContainerAsync<GenericImage>) {
    let _leaked_container: &'static mut ContainerAsync<GenericImage> =
        Box::leak(Box::new(container));
}

async fn wait_for_postgres(admin_url: &str) {
    for attempt in 1..=MAX_POSTGRES_BOOTSTRAP_ATTEMPTS {
        if let Ok(pool) = PgPoolOptions::new()
            .max_connections(1)
            .connect(admin_url)
            .await
        {
            let uuidv7_check = sqlx::query_scalar::<_, String>("SELECT uuidv7()::text")
                .fetch_one(&pool)
                .await;
            pool.close().await;

            if let Err(err) = uuidv7_check {
                panic!(
                    "postgres is reachable but `uuidv7()` failed ({err}). Ensure RUNLEDGER_TEST_PG_IMAGE points to a runtime with uuidv7 support."
                );
            }
            return;
        }

        tokio::time::sleep(Duration::from_millis(250)).await;

        if attempt == MAX_POSTGRES_BOOTSTRAP_ATTEMPTS {
            panic!(
                "failed to connect to postgres test container after {attempt} attempts ({admin_url})"
            );
        }
    }

    panic!("unexpected postgres bootstrap loop termination");
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
