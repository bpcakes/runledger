use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use runledger_core::jobs::{
    JobContext, JobDeadLetterInfo, JobFailure, JobStage, JobStatus, JobType,
};
use runledger_postgres::DbPool;
use runledger_postgres::jobs::{
    self, JobDefinitionUpsert, JobEnqueue, JobListFilter, JobQueueRecord, get_job_by_id,
    upsert_job_definition_tx,
};
use runledger_runtime::config::JobsConfig;
use runledger_runtime::reaper::run_reaper_loop;
use runledger_runtime::registry::{JobHandler, JobRegistry};
use runledger_runtime::scheduler::run_scheduler_loop;
use runledger_runtime::worker::run_worker_loop;
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::ContainerPort, runners::AsyncRunner,
};
use tokio::sync::{Mutex, Notify, watch};
use tokio::time::{Instant, sleep, timeout};

const SMOKE_JOB_TYPE: &str = "jobs.external.smoke";
const POSTGRES_USER: &str = "runledger";
const POSTGRES_PASSWORD: &str = "runledger";
const POSTGRES_DB: &str = "postgres";
const DEFAULT_POSTGRES_IMAGE: &str = "postgres:18";
const MAX_POSTGRES_BOOTSTRAP_ATTEMPTS: u8 = 40;
const MAX_PORT_RESOLVE_ATTEMPTS: u8 = 10;

#[tokio::test]
async fn packaged_crates_support_external_consumer_embedding() {
    let harness = PostgresHarness::start().await;
    runledger_postgres::migrate(&harness.pool)
        .await
        .expect("apply packaged migrations");

    let hang_release = Arc::new(Notify::new());
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let execution_count = Arc::new(AtomicUsize::new(0));

    let handler = SmokeHandler {
        execution_count: execution_count.clone(),
        hang_release: hang_release.clone(),
        dead_letters: dead_letters.clone(),
    };

    let mut registry = JobRegistry::new();
    registry.register(handler);

    let mut tx = harness.pool.begin().await.expect("begin job definition tx");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(SMOKE_JOB_TYPE),
            version: 1,
            max_attempts: 1,
            default_timeout_seconds: 30,
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert smoke job definition");
    tx.commit().await.expect("commit job definition tx");

    let config = JobsConfig {
        worker_id: "external-consumer-smoke-worker".to_string(),
        poll_interval: Duration::from_millis(25),
        claim_batch_size: 8,
        lease_ttl_seconds: 10,
        max_global_concurrency: 8,
        reaper_interval: Duration::from_millis(100),
        schedule_poll_interval: Duration::from_millis(100),
        reaper_retry_delay_ms: 1_000,
    };

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let worker_task = tokio::spawn(run_worker_loop(
        harness.pool.clone(),
        registry.clone(),
        config.clone(),
        shutdown_rx.clone(),
    ));
    let scheduler_task = tokio::spawn(run_scheduler_loop(
        harness.pool.clone(),
        config.clone(),
        shutdown_rx.clone(),
    ));
    let reaper_task = tokio::spawn(run_reaper_loop(
        harness.pool.clone(),
        registry.clone(),
        config,
        shutdown_rx,
    ));

    let success_job_id = enqueue_kind(&harness.pool, "success").await;
    let terminal_job_id = enqueue_kind(&harness.pool, "terminal").await;
    insert_due_schedule(&harness.pool, "scheduled-success")
        .await
        .expect("insert due schedule");

    let success_job = wait_for_status(&harness.pool, success_job_id, JobStatus::Succeeded).await;
    assert_eq!(success_job.status, JobStatus::Succeeded);

    let terminal_job =
        wait_for_status(&harness.pool, terminal_job_id, JobStatus::DeadLettered).await;
    assert_eq!(terminal_job.status, JobStatus::DeadLettered);

    let scheduled_job =
        wait_for_kind_status(&harness.pool, "scheduled-success", JobStatus::Succeeded).await;
    assert_eq!(scheduled_job.status, JobStatus::Succeeded);

    let hanging_job_id = enqueue_kind(&harness.pool, "hang").await;
    let hanging_job = wait_for_running(&harness.pool, hanging_job_id).await;
    assert_eq!(hanging_job.status, JobStatus::Leased);
    assert_eq!(hanging_job.stage, JobStage::Running);
    assert!(
        hanging_job.worker_id.is_some(),
        "hanging job should be claimed by the worker"
    );

    expire_job_lease(&harness.pool, hanging_job_id)
        .await
        .expect("force job lease expiration");

    let reaped_job = wait_for_status(&harness.pool, hanging_job_id, JobStatus::DeadLettered).await;
    assert_eq!(reaped_job.status, JobStatus::DeadLettered);

    wait_for_dead_letter(&dead_letters, "terminal").await;
    wait_for_dead_letter(&dead_letters, "hang").await;

    assert!(
        execution_count.load(Ordering::SeqCst) >= 4,
        "worker should execute direct, scheduled, and hanging jobs"
    );

    hang_release.notify_waiters();
    let _ = shutdown_tx.send(true);

    timeout(Duration::from_secs(3), worker_task)
        .await
        .expect("worker should stop")
        .expect("worker task should join cleanly");
    timeout(Duration::from_secs(3), scheduler_task)
        .await
        .expect("scheduler should stop")
        .expect("scheduler task should join cleanly");
    timeout(Duration::from_secs(3), reaper_task)
        .await
        .expect("reaper should stop")
        .expect("reaper task should join cleanly");

    harness.teardown().await;
}

struct SmokeHandler {
    execution_count: Arc<AtomicUsize>,
    hang_release: Arc<Notify>,
    dead_letters: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl JobHandler for SmokeHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(SMOKE_JOB_TYPE)
    }

    async fn execute(&self, _context: JobContext, payload: Value) -> Result<(), JobFailure> {
        self.execution_count.fetch_add(1, Ordering::SeqCst);

        match payload_kind(&payload) {
            "success" | "scheduled-success" => Ok(()),
            "terminal" => Err(JobFailure::terminal(
                "smoke.terminal_failure",
                "Smoke handler returned a terminal failure.",
            )),
            "hang" => {
                self.hang_release.notified().await;
                Ok(())
            }
            other => Err(JobFailure::terminal(
                "smoke.unknown_kind",
                format!("Unsupported smoke payload kind `{other}`."),
            )),
        }
    }

    async fn on_dead_letter(
        &self,
        _context: JobContext,
        payload: Value,
        _dead_letter: JobDeadLetterInfo,
    ) {
        self.dead_letters
            .lock()
            .await
            .push(payload_kind(&payload).to_string());
    }
}

struct PostgresHarness {
    _container: ContainerAsync<GenericImage>,
    pool: DbPool,
}

impl PostgresHarness {
    async fn start() -> Self {
        let image_ref = std::env::var("RUNLEDGER_TEST_PG_IMAGE")
            .unwrap_or_else(|_| DEFAULT_POSTGRES_IMAGE.into());
        let (repository, tag) = parse_image_ref(&image_ref);
        let image = GenericImage::new(repository, tag)
            .with_exposed_port(ContainerPort::Tcp(5432))
            .with_env_var("POSTGRES_USER", POSTGRES_USER)
            .with_env_var("POSTGRES_PASSWORD", POSTGRES_PASSWORD)
            .with_env_var("POSTGRES_DB", POSTGRES_DB);
        let container = image.start().await.expect("start postgres container");
        let port = resolve_host_port(&container, 5432).await;
        let database_url = format!(
            "postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@127.0.0.1:{port}/{POSTGRES_DB}"
        );

        wait_for_postgres(&database_url).await;

        let pool = PgPoolOptions::new()
            .max_connections(12)
            .connect(&database_url)
            .await
            .expect("connect smoke pool");

        Self {
            _container: container,
            pool,
        }
    }

    async fn teardown(self) {
        self.pool.close().await;
    }
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

async fn resolve_host_port(container: &ContainerAsync<GenericImage>, internal_port: u16) -> u16 {
    for attempt in 1..=MAX_PORT_RESOLVE_ATTEMPTS {
        match container.get_host_port_ipv4(internal_port).await {
            Ok(port) => return port,
            Err(error) => {
                if attempt == MAX_PORT_RESOLVE_ATTEMPTS {
                    panic!(
                        "resolve mapped postgres port after {MAX_PORT_RESOLVE_ATTEMPTS} attempts: {error}"
                    );
                }
                sleep(Duration::from_millis(250)).await;
            }
        }
    }

    unreachable!("host port resolution loop should always return or panic");
}

async fn wait_for_postgres(database_url: &str) {
    for attempt in 1..=MAX_POSTGRES_BOOTSTRAP_ATTEMPTS {
        if let Ok(pool) = PgPoolOptions::new()
            .max_connections(1)
            .connect(database_url)
            .await
        {
            let uuidv7_check = sqlx::query_scalar::<_, String>("SELECT uuidv7()::text")
                .fetch_one(&pool)
                .await;
            pool.close().await;

            match uuidv7_check {
                Ok(_) => return,
                Err(error) => {
                    panic!(
                        "postgres is reachable but `uuidv7()` failed ({error}). Ensure RUNLEDGER_TEST_PG_IMAGE points to a runtime with uuidv7 support."
                    );
                }
            }
        }

        sleep(Duration::from_millis(250)).await;

        if attempt == MAX_POSTGRES_BOOTSTRAP_ATTEMPTS {
            panic!("failed to connect to postgres smoke container after {attempt} attempts");
        }
    }
}

async fn enqueue_kind(pool: &DbPool, kind: &str) -> sqlx::types::Uuid {
    jobs::enqueue_job(
        pool,
        &JobEnqueue {
            job_type: JobType::new(SMOKE_JOB_TYPE),
            organization_id: None,
            payload: &json!({ "kind": kind }),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue smoke job")
}

async fn insert_due_schedule(pool: &DbPool, kind: &str) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO job_schedules (
            name,
            job_type,
            organization_id,
            payload_template,
            cron_expr,
            next_fire_at
         )
         VALUES ($1, $2, $3, $4::jsonb, $5, $6)",
    )
    .bind(format!("external-consumer-{kind}"))
    .bind(SMOKE_JOB_TYPE)
    .bind::<Option<sqlx::types::Uuid>>(None)
    .bind(json!({ "kind": kind }))
    .bind("0 0 0 1 1 * *")
    .bind(Utc::now() - ChronoDuration::seconds(5))
    .execute(pool)
    .await
    .map(|_| ())
}

async fn expire_job_lease(pool: &DbPool, job_id: sqlx::types::Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE job_queue
         SET lease_expires_at = now() - interval '10 seconds'
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(pool)
    .await
    .map(|_| ())
}

async fn wait_for_status(
    pool: &DbPool,
    job_id: sqlx::types::Uuid,
    expected: JobStatus,
) -> JobQueueRecord {
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        let job = get_job_by_id(pool, None, job_id)
            .await
            .expect("load job by id")
            .expect("job should exist");
        if job.status == expected {
            return job;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for job {job_id} to reach status {expected:?}; last status was {:?}",
            job.status
        );
        sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_running(pool: &DbPool, job_id: sqlx::types::Uuid) -> JobQueueRecord {
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        let job = get_job_by_id(pool, None, job_id)
            .await
            .expect("load running job by id")
            .expect("job should exist");
        if job.status == JobStatus::Leased && job.stage == JobStage::Running {
            return job;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for job {job_id} to start; last status/stage was {:?}/{:?}",
            job.status,
            job.stage
        );
        sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_kind_status(pool: &DbPool, kind: &str, expected: JobStatus) -> JobQueueRecord {
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        let jobs = jobs::list_jobs(
            pool,
            &JobListFilter {
                organization_id: None,
                status: None,
                job_type: Some(SMOKE_JOB_TYPE),
                limit: 32,
                offset: 0,
            },
        )
        .await
        .expect("list smoke jobs");

        if let Some(job) = jobs
            .into_iter()
            .find(|job| payload_kind(&job.payload) == kind && job.status == expected)
        {
            return job;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for smoke job kind `{kind}` to reach status {expected:?}"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_dead_letter(dead_letters: &Arc<Mutex<Vec<String>>>, kind: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        if dead_letters.lock().await.iter().any(|entry| entry == kind) {
            return;
        }

        assert!(
            Instant::now() < deadline,
            "timed out waiting for dead-letter hook for `{kind}`"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

fn payload_kind(payload: &Value) -> &str {
    payload
        .get("kind")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
}
