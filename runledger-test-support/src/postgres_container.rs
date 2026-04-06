use std::sync::OnceLock;
use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::ContainerPort, runners::AsyncRunner,
};

const DEFAULT_POSTGRES_IMAGE: &str = "postgres:18";
const POSTGRES_USER: &str = "runledger";
const POSTGRES_PASSWORD: &str = "runledger";
const POSTGRES_DB: &str = "postgres";
const MAX_POSTGRES_BOOTSTRAP_ATTEMPTS: u8 = 40;
const MAX_PORT_RESOLVE_ATTEMPTS: u8 = 10;

static SHARED_ADMIN_URL: OnceLock<String> = OnceLock::new();
static SHARED_HARNESS_INIT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub async fn admin_database_url() -> &'static str {
    shared_admin_database_url().await
}

async fn shared_admin_database_url() -> &'static str {
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
