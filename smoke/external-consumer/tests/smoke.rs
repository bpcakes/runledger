use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use runledger_admin::{AdminAccess, AdminService, DataVisibility};
use runledger_core::jobs::{
    JobCompletion, JobContext, JobDeadLetterInfo, JobEventType, JobFailure, JobStage, JobStatus,
    JobType,
};
use runledger_postgres::jobs::{
    self, CompareAndReplaySucceededJob, CompareAndReplaySucceededJobOutcome, CompareAndRequeueJob,
    CompareAndRequeueJobOutcome, JobDefinitionUpsert, JobEnqueue, JobEnqueueDisposition,
    JobEnqueueIntent, JobEnqueueIntentStatus, JobListFilter, JobQueueRecord, JobRequeueStatePolicy,
    JobScope, compare_and_replay_succeeded_job, compare_and_replay_succeeded_job_tx,
    compare_and_requeue_job, compare_and_requeue_job_tx, enqueue_job_with_outcome_tx,
    get_job_by_id, get_job_continuation_metrics, get_job_enqueue_intent_by_id,
    record_job_enqueue_intent_tx, upsert_job_definition_tx,
};
use runledger_postgres::prelude::{
    DbPool, DecodedJobEventPayload, DecodedRequeuedEventPayload, JobEventRecord, list_job_events,
};
use runledger_runtime::Supervisor;
use runledger_runtime::config::JobsConfig;
use runledger_runtime::registry::{JobHandler, JobRegistry};
use serde_json::{Value, json};
use sqlx::postgres::PgPoolOptions;
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt, core::ContainerPort, runners::AsyncRunner,
};
use tokio::sync::{Mutex, Notify};
use tokio::time::{Instant, sleep, timeout};

const SMOKE_JOB_TYPE: &str = "jobs.external.smoke";
const POSTGRES_USER: &str = "runledger";
const POSTGRES_PASSWORD: &str = "runledger";
const POSTGRES_DB: &str = "postgres";
const DEFAULT_POSTGRES_IMAGE: &str = "postgres:18";
const MAX_POSTGRES_BOOTSTRAP_ATTEMPTS: u8 = 40;
const MAX_PORT_RESOLVE_ATTEMPTS: u8 = 10;
const CONTINUATION_CHECKPOINT_VERSION: i64 = 1;
const CONTINUATION_MAX_RUNS: i64 = 2;
const HANDLER_CONTINUATION_REASON: &str = "HANDLER_CONTINUATION";
const HANDLER_RETRY_AFTER: Duration = Duration::from_millis(25);
const HANDLER_RETRY_AFTER_MS: i64 = 25;
const SMOKE_RETRY_POLICY_DELAY_MS: i32 = 1;
const RETRY_AFTER_FAILURE_CODE: &str = "smoke.provider_temporarily_unavailable";
const RETRY_AT_FAILURE_CODE: &str = "smoke.provider_rate_limited";
const REPLAY_REQUEST_KEY: &str = "external-smoke-success-replay";
const REPLAY_REASON: &str = "prove fresh successful replay from a packaged consumer";

#[tokio::test]
async fn packaged_crates_support_external_consumer_embedding() {
    let harness = PostgresHarness::start().await;
    let _admin_router = runledger_admin::router(AdminService::new(harness.pool.clone()));
    let _admin_access = AdminAccess::all(DataVisibility::MetadataOnly);
    let _admin_job_schema =
        <runledger_admin::JobDto as runledger_admin::utoipa::PartialSchema>::schema();
    runledger_postgres::migrate_after_idempotency_cutover(&harness.pool)
        .await
        .expect("apply packaged migrations");
    create_consumer_audit_table(&harness.pool)
        .await
        .expect("create consumer-owned audit table");

    let intent_payload = json!({"kind": "success", "source": "transactional-intent"});
    let intent_request = JobEnqueueIntent::new(
        JobType::new(SMOKE_JOB_TYPE),
        &intent_payload,
        "external-smoke-transactional-intent",
    );
    let mut intent_tx = harness
        .pool
        .begin()
        .await
        .expect("begin consumer-owned intent transaction");
    let recorded_intent = record_job_enqueue_intent_tx(&mut intent_tx, &intent_request)
        .await
        .expect("record intent before job definition");
    record_consumer_audit_tx(
        &mut intent_tx,
        "transactional-enqueue-intent",
        recorded_intent.intent_id,
        recorded_intent.intent_id,
    )
    .await
    .expect("record consumer audit beside enqueue intent");
    intent_tx
        .commit()
        .await
        .expect("commit consumer audit and enqueue intent atomically");
    assert_consumer_audit(
        &harness.pool,
        "transactional-enqueue-intent",
        recorded_intent.intent_id,
        recorded_intent.intent_id,
    )
    .await;

    let hang_release = Arc::new(Notify::new());
    let dead_letters = Arc::new(Mutex::new(Vec::new()));
    let execution_count = Arc::new(AtomicUsize::new(0));
    let completed_continuation_slices = Arc::new(Mutex::new(HashSet::new()));

    let handler = SmokeHandler {
        execution_count: Arc::clone(&execution_count),
        hang_release: Arc::clone(&hang_release),
        dead_letters: Arc::clone(&dead_letters),
        completed_continuation_slices: Arc::clone(&completed_continuation_slices),
        continuation_canary_enabled: true,
    };

    let mut registry = JobRegistry::new();
    registry.register(handler);
    for failure_code in [RETRY_AFTER_FAILURE_CODE, RETRY_AT_FAILURE_CODE] {
        registry.register_retry_delay_override(
            JobType::new(SMOKE_JOB_TYPE),
            failure_code,
            SMOKE_RETRY_POLICY_DELAY_MS,
        );
    }

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

    let recovery_payload = json!({"kind": "success"});
    let recovery_next_run_at = Utc::now() + ChronoDuration::hours(1);
    let recovery_request = JobEnqueue {
        job_type: JobType::new(SMOKE_JOB_TYPE),
        organization_id: None,
        payload: &recovery_payload,
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        next_run_at: Some(recovery_next_run_at),
        idempotency_key: Some("external-smoke-recovery"),
        stage: None,
    };
    let mut recovery_enqueue_tx = harness.pool.begin().await.expect("begin recovery enqueue");
    let inserted_recovery =
        enqueue_job_with_outcome_tx(&mut recovery_enqueue_tx, &recovery_request)
            .await
            .expect("insert recovery job with outcome");
    assert_eq!(
        inserted_recovery.disposition,
        JobEnqueueDisposition::Inserted
    );
    recovery_enqueue_tx
        .commit()
        .await
        .expect("commit recovery enqueue");
    jobs::cancel_job(
        &harness.pool,
        None,
        inserted_recovery.job_id,
        Some("external smoke recovery"),
    )
    .await
    .expect("cancel recovery job");

    let mut existing_enqueue_tx = harness.pool.begin().await.expect("begin existing enqueue");
    let existing_recovery =
        enqueue_job_with_outcome_tx(&mut existing_enqueue_tx, &recovery_request)
            .await
            .expect("resolve existing recovery job");
    assert_eq!(existing_recovery.job_id, inserted_recovery.job_id);
    assert_eq!(existing_recovery.status, JobStatus::Canceled);
    assert_eq!(
        existing_recovery.disposition,
        JobEnqueueDisposition::Existing
    );
    existing_enqueue_tx
        .commit()
        .await
        .expect("commit existing enqueue");

    let observed_recovery = get_job_by_id(&harness.pool, None, inserted_recovery.job_id)
        .await
        .expect("load canceled recovery job")
        .expect("canceled recovery job exists");
    let recovery_request = CompareAndRequeueJob::from_observed_job(
        &observed_recovery,
        JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
        "external smoke compare-and-requeue",
    )
    .expect("canceled observation is recoverable");
    let recovery_outcome = compare_and_requeue_job(&harness.pool, recovery_request)
        .await
        .expect("compare and requeue recovery job");
    assert!(matches!(
        recovery_outcome,
        CompareAndRequeueJobOutcome::Requeued { .. }
    ));

    let transactional_recovery_job_id = enqueue_payload(&harness.pool, &recovery_payload).await;
    jobs::cancel_job(
        &harness.pool,
        None,
        transactional_recovery_job_id,
        Some("external smoke transactional recovery"),
    )
    .await
    .expect("cancel transactional recovery job");
    let observed_transactional_recovery =
        get_job_by_id(&harness.pool, None, transactional_recovery_job_id)
            .await
            .expect("load transactional recovery job")
            .expect("transactional recovery job exists");
    let transactional_recovery_request = CompareAndRequeueJob::from_observed_job(
        &observed_transactional_recovery,
        JobRequeueStatePolicy::PreserveProgressAndCheckpoint,
        "external smoke transactional compare-and-requeue",
    )
    .expect("transactional recovery observation is recoverable");
    let mut transactional_recovery_tx = harness
        .pool
        .begin()
        .await
        .expect("begin consumer-owned recovery transaction");
    let transactional_recovery_outcome = compare_and_requeue_job_tx(
        &mut transactional_recovery_tx,
        transactional_recovery_request,
    )
    .await
    .expect("compare and requeue in consumer-owned transaction");
    let CompareAndRequeueJobOutcome::Requeued {
        after: transactional_recovery,
        ..
    } = transactional_recovery_outcome
    else {
        panic!("expected transactional recovery to requeue");
    };
    record_consumer_audit_tx(
        &mut transactional_recovery_tx,
        "transactional-recovery",
        transactional_recovery_job_id,
        transactional_recovery.id,
    )
    .await
    .expect("record recovery in consumer-owned transaction");
    transactional_recovery_tx
        .commit()
        .await
        .expect("commit recovery and consumer audit atomically");
    assert_consumer_audit(
        &harness.pool,
        "transactional-recovery",
        transactional_recovery_job_id,
        transactional_recovery_job_id,
    )
    .await;
    let committed_transactional_recovery =
        get_job_by_id(&harness.pool, None, transactional_recovery_job_id)
            .await
            .expect("reload committed transactional recovery")
            .expect("committed transactional recovery exists");
    assert_eq!(committed_transactional_recovery.status, JobStatus::Pending);
    assert_eq!(committed_transactional_recovery.run_number, 2);

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

    let supervisor = Supervisor::builder(&harness.pool, config)
        .expect("supervisor builder should find active Tokio runtime")
        .with_registry(registry)
        .build()
        .expect("supervisor should build");
    let (stop_supervisor_tx, stop_supervisor_rx) = tokio::sync::oneshot::channel();
    let supervisor_task = tokio::spawn(supervisor.run_until_shutdown(
        async move {
            let _ = stop_supervisor_rx.await;
        },
        Duration::from_secs(10),
    ));

    let success_job_id = enqueue_kind(&harness.pool, "success").await;
    let intent_job_id = wait_for_promoted_intent(&harness.pool, recorded_intent.intent_id).await;
    let intent_job = wait_for_status(&harness.pool, intent_job_id, JobStatus::Succeeded).await;
    assert_eq!(intent_job.payload, intent_payload);
    let continuation_job_id = enqueue_payload(
        &harness.pool,
        &json!({
            "kind": "continuation",
            "canary": true,
            "max_runs": CONTINUATION_MAX_RUNS,
        }),
    )
    .await;
    let terminal_job_id = enqueue_kind(&harness.pool, "terminal").await;
    let retry_after_job_id =
        enqueue_payload_with_max_attempts(&harness.pool, &json!({"kind": "retry-after"}), Some(2))
            .await;
    let retry_at_job_id =
        enqueue_payload_with_max_attempts(&harness.pool, &json!({"kind": "retry-at"}), Some(2))
            .await;
    insert_due_schedule(&harness.pool, "scheduled-success")
        .await
        .expect("insert due schedule");

    let success_job = wait_for_status(&harness.pool, success_job_id, JobStatus::Succeeded).await;
    assert_eq!(success_job.status, JobStatus::Succeeded);

    let replay_request = CompareAndReplaySucceededJob {
        scope: JobScope::Global,
        source_job_id: success_job.id,
        expected_run_number: success_job.run_number,
        replay_request_key: REPLAY_REQUEST_KEY,
        reason: REPLAY_REASON,
    };
    let mut replay_tx = harness
        .pool
        .begin()
        .await
        .expect("begin consumer-owned replay transaction");
    let replay_outcome =
        compare_and_replay_succeeded_job_tx(&mut replay_tx, replay_request.clone())
            .await
            .expect("replay successful job in consumer-owned transaction");
    let CompareAndReplaySucceededJobOutcome::Replayed { replay, .. } = replay_outcome else {
        panic!("expected successful replay outcome");
    };
    assert_eq!(replay.disposition, JobEnqueueDisposition::Inserted);
    assert_ne!(replay.job_id, success_job.id);
    record_consumer_audit_tx(
        &mut replay_tx,
        "transactional-successful-replay",
        success_job.id,
        replay.job_id,
    )
    .await
    .expect("record replay in consumer-owned transaction");
    replay_tx
        .commit()
        .await
        .expect("commit replay and consumer audit atomically");
    assert_consumer_audit(
        &harness.pool,
        "transactional-successful-replay",
        success_job.id,
        replay.job_id,
    )
    .await;

    let existing_replay = compare_and_replay_succeeded_job(&harness.pool, replay_request)
        .await
        .expect("resolve replay idempotently through pool wrapper");
    let CompareAndReplaySucceededJobOutcome::Replayed {
        replay: existing_replay,
        ..
    } = existing_replay
    else {
        panic!("expected existing successful replay outcome");
    };
    assert_eq!(existing_replay.job_id, replay.job_id);
    assert_eq!(existing_replay.disposition, JobEnqueueDisposition::Existing);

    let replayed_job = wait_for_status(&harness.pool, replay.job_id, JobStatus::Succeeded).await;
    assert_eq!(replayed_job.run_number, 1);
    let replay_events = list_job_events(&harness.pool, None, replay.job_id, 100, None)
        .await
        .expect("list successful replay events through the public prelude");
    assert_successful_replay_event(
        &replay_events,
        replay.job_id,
        success_job.id,
        success_job.run_number,
    );
    assert_eq!(
        get_job_by_id(&harness.pool, None, success_job.id)
            .await
            .expect("reload successful replay source")
            .expect("successful replay source still exists")
            .status,
        JobStatus::Succeeded
    );

    let continuation_job =
        wait_for_status(&harness.pool, continuation_job_id, JobStatus::Succeeded).await;
    assert_eq!(continuation_job.run_number, 2);
    assert_eq!(continuation_job.attempt, 1);
    let continuation_events = list_job_events(&harness.pool, None, continuation_job_id, 100, None)
        .await
        .expect("list continuation events through the public prelude");
    assert_handler_continuation_event(&continuation_events, continuation_job_id);
    assert_eq!(
        continuation_job.checkpoint,
        Some(json!({
            "version": CONTINUATION_CHECKPOINT_VERSION,
            "cursor": CONTINUATION_MAX_RUNS - 1,
        }))
    );
    let continuation_metrics =
        get_job_continuation_metrics(&harness.pool, None, Some(SMOKE_JOB_TYPE))
            .await
            .expect("load smoke continuation metrics")
            .pop()
            .expect("registered smoke job type has metrics");
    assert_eq!(continuation_metrics.continued_24h, 1);
    assert_eq!(continuation_metrics.active_continued_count, 0);
    assert_eq!(continuation_metrics.max_active_run_number, 0);
    assert_eq!(completed_continuation_slices.lock().await.len(), 2);

    let retry_after_job =
        wait_for_status(&harness.pool, retry_after_job_id, JobStatus::Succeeded).await;
    assert_eq!(retry_after_job.run_number, 1);
    assert_eq!(retry_after_job.attempt, 2);
    let retry_after_events = list_job_events(&harness.pool, None, retry_after_job_id, 100, None)
        .await
        .expect("list handler-selected relative retry events");
    assert_relative_retry_event(&retry_after_events, retry_after_job_id);

    let retry_at_job = wait_for_status(&harness.pool, retry_at_job_id, JobStatus::Succeeded).await;
    assert_eq!(retry_at_job.run_number, 1);
    assert_eq!(retry_at_job.attempt, 2);
    let retry_at_events = list_job_events(&harness.pool, None, retry_at_job_id, 100, None)
        .await
        .expect("list handler-selected absolute retry events");
    assert_absolute_retry_event(&retry_at_events, retry_at_job_id);

    let recovered_job = wait_for_status(
        &harness.pool,
        inserted_recovery.job_id,
        JobStatus::Succeeded,
    )
    .await;
    assert_eq!(recovered_job.run_number, 2);
    let transactionally_recovered_job = wait_for_status(
        &harness.pool,
        transactional_recovery_job_id,
        JobStatus::Succeeded,
    )
    .await;
    assert_eq!(transactionally_recovered_job.run_number, 2);

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
        execution_count.load(Ordering::SeqCst) >= 13,
        "worker should execute direct, retried, continued, recovered, replayed, scheduled, and hanging jobs"
    );

    hang_release.notify_waiters();
    let _ = stop_supervisor_tx.send(());
    let shutdown_result = timeout(Duration::from_secs(12), supervisor_task)
        .await
        .expect("supervisor monitor task should stop before outer timeout")
        .expect("supervisor monitor task should join");
    shutdown_result.expect("supervisor tasks should stop and join cleanly");

    harness.teardown().await;
}

struct SmokeHandler {
    execution_count: Arc<AtomicUsize>,
    hang_release: Arc<Notify>,
    dead_letters: Arc<Mutex<Vec<String>>>,
    completed_continuation_slices: Arc<Mutex<HashSet<(sqlx::types::Uuid, i64)>>>,
    continuation_canary_enabled: bool,
}

impl SmokeHandler {
    async fn execute_continuation(
        &self,
        context: JobContext,
        payload: &Value,
    ) -> Result<JobCompletion, JobFailure> {
        if !self.continuation_canary_enabled
            || payload.get("canary").and_then(Value::as_bool) != Some(true)
        {
            return Err(JobFailure::terminal(
                "smoke.continuation_not_enabled",
                "Continuation is not enabled for this application canary.",
            ));
        }

        let max_runs = payload
            .get("max_runs")
            .and_then(Value::as_i64)
            .filter(|max_runs| *max_runs > 0)
            .ok_or_else(|| {
                JobFailure::terminal(
                    "smoke.invalid_continuation_limit",
                    "Continuation payload requires a positive max_runs limit.",
                )
            })?;
        if i64::from(context.run_number) > max_runs {
            return Err(JobFailure::terminal(
                "smoke.continuation_limit_exceeded",
                "Continuation exceeded its application-owned run limit.",
            ));
        }

        let cursor = match context.checkpoint.as_ref() {
            None => 0,
            Some(checkpoint) => {
                if checkpoint.get("version").and_then(Value::as_i64)
                    != Some(CONTINUATION_CHECKPOINT_VERSION)
                {
                    return Err(JobFailure::terminal(
                        "smoke.unsupported_checkpoint_version",
                        "Continuation checkpoint version is unsupported.",
                    ));
                }
                checkpoint
                    .get("cursor")
                    .and_then(Value::as_i64)
                    .filter(|cursor| *cursor >= 0)
                    .ok_or_else(|| {
                        JobFailure::terminal(
                            "smoke.invalid_checkpoint_cursor",
                            "Continuation checkpoint cursor is invalid.",
                        )
                    })?
            }
        };
        let slice = cursor + 1;
        if slice != i64::from(context.run_number) {
            return Err(JobFailure::terminal(
                "smoke.checkpoint_run_mismatch",
                "Continuation checkpoint does not match the current run.",
            ));
        }

        // A production handler would enforce this uniqueness in the same
        // datastore as its externally visible side effect. `(job_id, slice)`
        // remains stable if an attempt is retried, unlike `attempt`.
        self.completed_continuation_slices
            .lock()
            .await
            .insert((context.job_id, slice));

        if slice < max_runs {
            Ok(JobCompletion::continue_after(Duration::from_millis(25))
                .progress(slice, max_runs)
                .checkpoint(json!({
                    "version": CONTINUATION_CHECKPOINT_VERSION,
                    "cursor": slice,
                })))
        } else {
            Ok(JobCompletion::success().progress(slice, max_runs))
        }
    }
}

#[async_trait]
impl JobHandler for SmokeHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(SMOKE_JOB_TYPE)
    }

    async fn execute(
        &self,
        context: JobContext,
        payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        self.execution_count.fetch_add(1, Ordering::SeqCst);

        match payload_kind(&payload) {
            "success" | "scheduled-success" => Ok(JobCompletion::success()),
            "continuation" => self.execute_continuation(context, &payload).await,
            "retry-after" if context.attempt == 1 => Err(JobFailure::retryable(
                RETRY_AFTER_FAILURE_CODE,
                "Smoke provider requested a relative retry.",
            )
            .retry_not_before_delay(HANDLER_RETRY_AFTER)),
            "retry-after" => Ok(JobCompletion::success()),
            "retry-at" if context.attempt == 1 => Err(JobFailure::retryable(
                RETRY_AT_FAILURE_CODE,
                "Smoke provider supplied an absolute reset timestamp.",
            )
            .retry_not_before(Utc::now() + ChronoDuration::milliseconds(HANDLER_RETRY_AFTER_MS))),
            "retry-at" => Ok(JobCompletion::success()),
            "terminal" => Err(JobFailure::terminal(
                "smoke.terminal_failure",
                "Smoke handler returned a terminal failure.",
            )),
            "hang" => {
                self.hang_release.notified().await;
                Ok(JobCompletion::success())
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
        // Missing override intentionally falls back to the default smoke image.
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
        // Connection failures are expected while the container is still booting;
        // retry until the bootstrap deadline before failing the smoke test.
        if let Ok(pool) = PgPoolOptions::new()
            .max_connections(1)
            .connect(database_url)
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

            match uuidv7_check {
                Ok(_) => return,
                Err(error) => {
                    panic!(
                        "postgres is reachable but `uuidv7()` failed ({error}). Runledger requires PostgreSQL 18 or later; ensure RUNLEDGER_TEST_PG_IMAGE points to PostgreSQL 18+."
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
    enqueue_payload(pool, &json!({ "kind": kind })).await
}

async fn enqueue_payload(pool: &DbPool, payload: &Value) -> sqlx::types::Uuid {
    enqueue_payload_with_max_attempts(pool, payload, None).await
}

async fn enqueue_payload_with_max_attempts(
    pool: &DbPool,
    payload: &Value,
    max_attempts: Option<i32>,
) -> sqlx::types::Uuid {
    jobs::enqueue_job(
        pool,
        &JobEnqueue {
            job_type: JobType::new(SMOKE_JOB_TYPE),
            organization_id: None,
            payload,
            priority: None,
            max_attempts,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue smoke job")
}

fn retry_scheduled_event(
    events: &[JobEventRecord],
    expected_job_id: sqlx::types::Uuid,
) -> &JobEventRecord {
    let event = events
        .iter()
        .find(|event| event.event_type == JobEventType::RetryScheduled)
        .expect("handler-selected retry should record RETRY_SCHEDULED");
    assert_eq!(event.job_id, expected_job_id);
    event
}

fn assert_relative_retry_event(events: &[JobEventRecord], expected_job_id: sqlx::types::Uuid) {
    let event = retry_scheduled_event(events, expected_job_id);
    assert!(
        event
            .payload
            .get("retry_delay_ms")
            .and_then(Value::as_i64)
            .is_some_and(|delay_ms| delay_ms > 0),
        "relative retry audit should retain the positive policy delay"
    );
    let requested_retry_not_before = event
        .payload
        .get("requested_retry_not_before")
        .and_then(Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
        .expect("relative retry audit should retain the handler lower bound");
    let next_run_at = event
        .payload
        .get("next_run_at")
        .and_then(Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
        .expect("relative retry audit should retain next_run_at");
    assert!(
        next_run_at >= requested_retry_not_before,
        "effective relative retry time must not precede the handler lower bound"
    );
}

fn assert_absolute_retry_event(events: &[JobEventRecord], expected_job_id: sqlx::types::Uuid) {
    let event = retry_scheduled_event(events, expected_job_id);
    let requested_retry_at = event
        .payload
        .get("requested_retry_not_before")
        .and_then(Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
        .expect("absolute retry audit should retain requested_retry_at");
    let next_run_at = event
        .payload
        .get("next_run_at")
        .and_then(Value::as_str)
        .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
        .map(|value| value.with_timezone(&Utc))
        .expect("absolute retry audit should retain next_run_at");
    assert!(
        next_run_at >= requested_retry_at,
        "effective absolute retry time must not precede the requested provider reset"
    );
}

async fn create_consumer_audit_table(pool: &DbPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        "CREATE TABLE external_consumer_operation_audit (
            operation_key text PRIMARY KEY,
            source_job_id uuid NOT NULL,
            result_job_id uuid NOT NULL
         )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

async fn record_consumer_audit_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    operation_key: &str,
    source_job_id: sqlx::types::Uuid,
    result_job_id: sqlx::types::Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "INSERT INTO external_consumer_operation_audit (
            operation_key,
            source_job_id,
            result_job_id
         )
         VALUES ($1, $2, $3)",
    )
    .bind(operation_key)
    .bind(source_job_id)
    .bind(result_job_id)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn assert_consumer_audit(
    pool: &DbPool,
    operation_key: &str,
    expected_source_job_id: sqlx::types::Uuid,
    expected_result_job_id: sqlx::types::Uuid,
) {
    let (source_job_id, result_job_id) =
        sqlx::query_as::<_, (sqlx::types::Uuid, sqlx::types::Uuid)>(
            "SELECT source_job_id, result_job_id
         FROM external_consumer_operation_audit
         WHERE operation_key = $1",
        )
        .bind(operation_key)
        .fetch_one(pool)
        .await
        .expect("load consumer-owned audit row");
    assert_eq!(source_job_id, expected_source_job_id);
    assert_eq!(result_job_id, expected_result_job_id);
}

fn assert_successful_replay_event(
    events: &[JobEventRecord],
    expected_replay_job_id: sqlx::types::Uuid,
    expected_source_job_id: sqlx::types::Uuid,
    expected_source_run_number: i32,
) {
    let event = events
        .iter()
        .find(|event| {
            matches!(
                event.decoded_payload(),
                DecodedJobEventPayload::SuccessfulReplayEnqueued(_)
            )
        })
        .expect("successful replay should have a typed ENQUEUED payload");
    assert_eq!(event.job_id, expected_replay_job_id);

    match event.decoded_payload() {
        DecodedJobEventPayload::SuccessfulReplayEnqueued(payload) => {
            assert_eq!(payload.replayed_from_job_id, expected_source_job_id);
            assert_eq!(payload.replayed_from_run_number, expected_source_run_number);
            assert_eq!(payload.replay_request_key, REPLAY_REQUEST_KEY);
            assert_eq!(payload.reason, REPLAY_REASON);
        }
        DecodedJobEventPayload::Requeued(DecodedRequeuedEventPayload::Unknown {
            reason, ..
        }) => panic!(
            "expected successful replay payload, got unknown requeue reason {reason:?}; raw payload: {}",
            event.payload
        ),
        DecodedJobEventPayload::Requeued(_) | DecodedJobEventPayload::Other => {
            panic!(
                "expected successful replay payload; raw payload: {}",
                event.payload
            )
        }
        _ => panic!(
            "expected successful replay payload, got a future decoded variant; raw payload: {}",
            event.payload
        ),
    }
}

fn assert_handler_continuation_event(
    events: &[JobEventRecord],
    expected_job_id: sqlx::types::Uuid,
) {
    let event = events
        .iter()
        .find(|event| {
            matches!(
                event.decoded_payload(),
                DecodedJobEventPayload::Requeued(
                    DecodedRequeuedEventPayload::HandlerContinuation { .. }
                )
            )
        })
        .expect("continuation should have a typed REQUEUED payload");
    assert_eq!(event.job_id, expected_job_id);

    match event.decoded_payload() {
        DecodedJobEventPayload::Requeued(DecodedRequeuedEventPayload::HandlerContinuation {
            reason,
            next_run_number,
            next_run_at,
            delay_microseconds,
            ..
        }) => {
            assert_eq!(reason, HANDLER_CONTINUATION_REASON);
            assert_eq!(next_run_number, 2);
            assert_eq!(delay_microseconds, 25_000);

            let raw_next_run_at = event
                .payload
                .get("next_run_at")
                .and_then(Value::as_str)
                .and_then(|value| chrono::DateTime::parse_from_rfc3339(value).ok())
                .map(|value| value.with_timezone(&Utc))
                .expect("continuation raw payload should retain next_run_at");
            assert_eq!(next_run_at, raw_next_run_at);
        }
        DecodedJobEventPayload::Requeued(DecodedRequeuedEventPayload::Unknown {
            reason, ..
        }) => panic!(
            "expected handler continuation payload, got unknown requeue reason {reason:?}; raw payload: {}",
            event.payload
        ),
        DecodedJobEventPayload::Requeued(_)
        | DecodedJobEventPayload::SuccessfulReplayEnqueued(_)
        | DecodedJobEventPayload::Other => panic!(
            "expected handler continuation payload; raw payload: {}",
            event.payload
        ),
        _ => panic!(
            "expected handler continuation payload, got a future decoded variant; raw payload: {}",
            event.payload
        ),
    }
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
    .await?;

    Ok(())
}

async fn expire_job_lease(pool: &DbPool, job_id: sqlx::types::Uuid) -> Result<(), sqlx::Error> {
    sqlx::query(
        "UPDATE job_queue
         SET lease_expires_at = now() - interval '10 seconds'
         WHERE id = $1",
    )
    .bind(job_id)
    .execute(pool)
    .await?;

    Ok(())
}

async fn wait_for_promoted_intent(
    pool: &DbPool,
    intent_id: sqlx::types::Uuid,
) -> sqlx::types::Uuid {
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        let intent = get_job_enqueue_intent_by_id(pool, None, intent_id)
            .await
            .expect("load enqueue intent")
            .expect("enqueue intent should exist");
        if intent.status == JobEnqueueIntentStatus::Promoted {
            return intent.promoted_job_id.expect("promoted intent has job id");
        }

        assert_eq!(intent.status, JobEnqueueIntentStatus::Pending);
        assert!(
            Instant::now() < deadline,
            "timed out waiting for enqueue intent {intent_id} to be promoted"
        );
        sleep(Duration::from_millis(25)).await;
    }
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
