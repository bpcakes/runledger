use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::ContainerPort, runners::AsyncRunner,
};

use crate::container_lifecycle::{
    PROCESS_OWNER_LABEL, ProcessContainer, process_owner_label_value,
};

const DEFAULT_POSTGRES_IMAGE: &str = "postgres:18";
const POSTGRES_USER: &str = "runledger";
const POSTGRES_PASSWORD: &str = "runledger";
const POSTGRES_DB: &str = "postgres";
const TEST_ADMIN_DATABASE_URL_ENV: &str = "RUNLEDGER_TEST_ADMIN_DATABASE_URL";
const TEST_PG_IMAGE_ENV: &str = "RUNLEDGER_TEST_PG_IMAGE";
const MAX_POSTGRES_BOOTSTRAP_ATTEMPTS: u8 = 40;
const MAX_PORT_RESOLVE_ATTEMPTS: u8 = 10;

static SHARED_POSTGRES: tokio::sync::OnceCell<SharedPostgres> = tokio::sync::OnceCell::const_new();

struct SharedPostgres {
    admin_url: String,
    _container: Option<ProcessContainer>,
}

#[derive(Debug, Eq, PartialEq)]
enum PostgresSource {
    ExternalAdmin(String),
    Image(String),
}

pub async fn admin_database_url() -> &'static str {
    shared_admin_database_url().await
}

async fn shared_admin_database_url() -> &'static str {
    &SHARED_POSTGRES
        .get_or_init(initialize_shared_postgres)
        .await
        .admin_url
}

async fn initialize_shared_postgres() -> SharedPostgres {
    initialize_postgres(configured_postgres_source()).await
}

fn configured_postgres_source() -> PostgresSource {
    match std::env::var(TEST_ADMIN_DATABASE_URL_ENV) {
        Ok(admin_url) => PostgresSource::ExternalAdmin(admin_url),
        Err(_) => PostgresSource::Image(
            std::env::var(TEST_PG_IMAGE_ENV).unwrap_or_else(|_| DEFAULT_POSTGRES_IMAGE.to_owned()),
        ),
    }
}

async fn initialize_postgres(source: PostgresSource) -> SharedPostgres {
    match source {
        PostgresSource::ExternalAdmin(admin_url) => initialize_external_postgres(admin_url).await,
        PostgresSource::Image(image_ref) => initialize_owned_postgres(&image_ref).await,
    }
}

async fn initialize_external_postgres(admin_url: String) -> SharedPostgres {
    wait_for_postgres(&admin_url).await;
    SharedPostgres {
        admin_url,
        _container: None,
    }
}

async fn initialize_owned_postgres(image_ref: &str) -> SharedPostgres {
    let (repository, tag) = parse_image_ref(image_ref);

    let image = GenericImage::new(repository, tag)
        .with_exposed_port(ContainerPort::Tcp(5432))
        .with_env_var("POSTGRES_USER", POSTGRES_USER)
        .with_env_var("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
        .with_env_var("POSTGRES_DB", POSTGRES_DB)
        .with_label(PROCESS_OWNER_LABEL, process_owner_label_value());
    let container = image.start().await.expect("start postgres container");
    let process_container = ProcessContainer::new(container).await;

    let port = resolve_host_port(process_container.container(), 5432).await;
    let admin_url = postgres_admin_url(port);

    wait_for_postgres(&admin_url).await;
    SharedPostgres {
        admin_url,
        _container: Some(process_container),
    }
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

async fn wait_for_postgres(admin_url: &str) {
    for attempt in 1..=MAX_POSTGRES_BOOTSTRAP_ATTEMPTS {
        if let Ok(pool) = PgPoolOptions::new()
            .max_connections(1)
            .connect(admin_url)
            .await
        {
            let server_version_num =
                sqlx::query_scalar::<_, i32>("SELECT current_setting('server_version_num')::int")
                    .fetch_one(&pool)
                    .await
                    .unwrap_or_else(|error| {
                        panic!("failed to read PostgreSQL server_version_num: {error}")
                    });
            assert!(
                server_version_num >= 180_000,
                "Runledger requires PostgreSQL 18 or later; connected server_version_num was {server_version_num}"
            );
            let uuidv7_check = sqlx::query_scalar::<_, String>("SELECT uuidv7()::text")
                .fetch_one(&pool)
                .await;
            pool.close().await;

            if let Err(err) = uuidv7_check {
                panic!(
                    "postgres is reachable but `uuidv7()` failed ({err}). Runledger requires PostgreSQL 18 or later; ensure RUNLEDGER_TEST_PG_IMAGE or RUNLEDGER_TEST_ADMIN_DATABASE_URL points to PostgreSQL 18+."
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use tokio::sync::{Barrier, OnceCell, oneshot};

    use super::*;
    use crate::ScopedEnv;

    const CONCURRENT_CALLERS: usize = 8;

    async fn postgres_18_admin_url(test_name: &str) -> String {
        let admin_url = admin_database_url().await.to_owned();
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(&admin_url)
            .await
            .expect("connect to shared PostgreSQL for initializer test");
        let server_version = sqlx::query_scalar::<_, String>("SHOW server_version")
            .fetch_one(&pool)
            .await
            .expect("read PostgreSQL server_version");
        let server_version_num =
            sqlx::query_scalar::<_, i32>("SELECT current_setting('server_version_num')::int")
                .fetch_one(&pool)
                .await
                .expect("read PostgreSQL server_version_num");
        pool.close().await;

        eprintln!(
            "{test_name}: PostgreSQL server_version={server_version}, server_version_num={server_version_num}"
        );
        assert!(
            server_version_num >= 180_000,
            "initializer test requires PostgreSQL 18+, got {server_version} ({server_version_num})"
        );
        admin_url
    }

    #[tokio::test]
    async fn concurrent_first_access_initializes_once_on_postgres_18() {
        let admin_url = postgres_18_admin_url("concurrent first access").await;
        let cell = Arc::new(OnceCell::<SharedPostgres>::new());
        let barrier = Arc::new(Barrier::new(CONCURRENT_CALLERS));
        let attempts = Arc::new(AtomicUsize::new(0));
        let mut callers = Vec::with_capacity(CONCURRENT_CALLERS);

        for _ in 0..CONCURRENT_CALLERS {
            let cell = Arc::clone(&cell);
            let barrier = Arc::clone(&barrier);
            let attempts = Arc::clone(&attempts);
            let admin_url = admin_url.clone();
            callers.push(tokio::spawn(async move {
                barrier.wait().await;
                cell.get_or_init(|| {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    initialize_external_postgres(admin_url)
                })
                .await
                .admin_url
                .clone()
            }));
        }

        for caller in callers {
            assert_eq!(
                caller.await.expect("concurrent initializer caller joins"),
                admin_url
            );
        }
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn failed_initialization_can_retry_on_postgres_18() {
        let admin_url = postgres_18_admin_url("failed initialization retry").await;
        let cell = Arc::new(OnceCell::<SharedPostgres>::new());
        let failed_cell = Arc::clone(&cell);
        let failed_url = admin_url.clone();
        let failure = tokio::spawn(async move {
            failed_cell
                .get_or_init(|| async move {
                    wait_for_postgres(&failed_url).await;
                    panic!("intentional shared PostgreSQL initialization failure");
                })
                .await;
        })
        .await
        .expect_err("failed initializer task must panic");

        assert!(failure.is_panic());
        assert!(cell.get().is_none());
        let initialized = cell
            .get_or_init(|| initialize_external_postgres(admin_url.clone()))
            .await;
        assert_eq!(initialized.admin_url, admin_url);
    }

    #[tokio::test]
    async fn cancelled_initialization_can_retry_on_postgres_18() {
        let admin_url = postgres_18_admin_url("cancelled initialization retry").await;
        let cell = Arc::new(OnceCell::<SharedPostgres>::new());
        let cancelled_cell = Arc::clone(&cell);
        let cancelled_url = admin_url.clone();
        let (started_tx, started_rx) = oneshot::channel();
        let initializer = tokio::spawn(async move {
            cancelled_cell
                .get_or_init(|| async move {
                    wait_for_postgres(&cancelled_url).await;
                    started_tx
                        .send(())
                        .expect("cancellation test receiver remains alive");
                    std::future::pending().await
                })
                .await;
        });

        started_rx
            .await
            .expect("initializer reaches cancellable point");
        initializer.abort();
        let cancellation = initializer
            .await
            .expect_err("aborted initializer task must be cancelled");
        assert!(cancellation.is_cancelled());
        assert!(cell.get().is_none());

        let initialized = cell
            .get_or_init(|| initialize_external_postgres(admin_url.clone()))
            .await;
        assert_eq!(initialized.admin_url, admin_url);
    }

    #[tokio::test]
    async fn external_admin_url_mode_uses_postgres_18_without_owning_a_container() {
        let admin_url = postgres_18_admin_url("external admin URL mode").await;
        let source = {
            let _env = ScopedEnv::set(&[
                (TEST_ADMIN_DATABASE_URL_ENV, Some(admin_url.as_str())),
                (TEST_PG_IMAGE_ENV, Some("unused.invalid/postgres:0")),
            ]);
            configured_postgres_source()
        };
        assert_eq!(source, PostgresSource::ExternalAdmin(admin_url.clone()));

        let cell = OnceCell::<SharedPostgres>::new();
        let initialized = cell.get_or_init(|| initialize_postgres(source)).await;
        assert_eq!(initialized.admin_url, admin_url);
        assert!(initialized._container.is_none());
    }
}
