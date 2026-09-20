use super::*;
use runledger_core::jobs::{JobExecution, JobExecutionError, JobExecutionHandler};
use runledger_runtime::{RuntimeCallbackFailure, RuntimeShutdownBudget, RuntimeShutdownSignal};
use tokio::sync::Notify;

const JOB: &str = "jobs.test.completed_handler_settlement";
const SHUTDOWN_GRACE: Duration = Duration::from_secs(20);
const SHUTDOWN_ABORT: Duration = Duration::from_secs(5);
const REPORT_DEADLOCK_GUARD: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
enum Outcome {
    Success,
    BusinessFailure,
    Continuation,
    LeaseLost,
    Panic,
}

struct CompletedHandler {
    outcome: Outcome,
    entered: Arc<Notify>,
    proceed: Arc<Notify>,
    returned: Arc<Notify>,
}

#[async_trait::async_trait]
impl JobExecutionHandler for CompletedHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(JOB)
    }

    async fn execute(
        &self,
        execution: JobExecution<'_>,
        _: Value,
    ) -> Result<JobCompletion, JobFailure> {
        // The handler owns and joins its application child before returning.
        tokio::spawn(async {})
            .await
            .expect("application child joined");
        self.entered.notify_one();
        self.proceed.notified().await;
        if matches!(self.outcome, Outcome::LeaseLost) {
            assert!(matches!(
                execution.save_checkpoint(&json!({"done": true})).await,
                Err(JobExecutionError::LeaseLost)
            ));
        } else {
            // Cross the real handler deadline in one poll. A timer cannot cancel
            // this poll; the worker receives an actual completed result (or panic).
            std::thread::sleep(
                execution
                    .deadline()
                    .saturating_duration_since(std::time::Instant::now())
                    + Duration::from_millis(10),
            );
        }
        self.returned.notify_one();
        match self.outcome {
            Outcome::Success | Outcome::LeaseLost => Ok(JobCompletion::success()),
            Outcome::BusinessFailure => Err(JobFailure::terminal("test.denied", "denied")),
            Outcome::Continuation => Ok(JobCompletion::continue_now()),
            Outcome::Panic => panic!("late handler panic"),
        }
    }
}

async fn exercise(outcome: Outcome, during_shutdown: bool) {
    let (pool, database) = setup_ephemeral_pool("runtime_completed_handler", 6).await;
    let version: String = sqlx::query_scalar("SHOW server_version")
        .fetch_one(&pool)
        .await
        .expect("PostgreSQL version");
    eprintln!("completed handler settlement: PostgreSQL {version}");
    let mut tx = pool.begin().await.expect("definition transaction");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(JOB),
            version: 1,
            max_attempts: 1,
            default_timeout_seconds: if matches!(outcome, Outcome::LeaseLost) {
                30
            } else {
                1
            },
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("register definition");
    tx.commit().await.expect("commit definition");
    let job = enqueue_job(
        &pool,
        &JobEnqueue {
            job_type: JobType::new(JOB),
            organization_id: None,
            payload: &json!({}),
            priority: None,
            max_attempts: None,
            timeout_seconds: None,
            next_run_at: None,
            idempotency_key: None,
            stage: Some(runledger_core::jobs::JobStage::Queued),
        },
    )
    .await
    .expect("enqueue");
    let entered = Arc::new(Notify::new());
    let proceed = Arc::new(Notify::new());
    let returned = Arc::new(Notify::new());
    let mut registry = JobRegistry::new();
    registry.register(
        CompletedHandler {
            outcome,
            entered: entered.clone(),
            proceed: proceed.clone(),
            returned: returned.clone(),
        }
        .into_job_handler(),
    );
    let mut config = test_config();
    // Keep heartbeats out of the controlled progress-write lease-loss path.
    config.lease_ttl_seconds = 60;
    let supervisor = Supervisor::builder(&pool, config)
        .expect("runtime")
        .with_registry(registry)
        .disable_scheduler()
        .disable_reaper()
        .build()
        .expect("supervisor");
    let shutdown = supervisor.shutdown_handle();
    let driver = tokio::spawn(supervisor.run_until_shutdown_report(
        RuntimeShutdownSignal::pending(),
        // Handler progress is controlled by explicit gates. Keep the runtime
        // shutdown budget as a deadlock guard, not a competing timing assertion.
        RuntimeShutdownBudget::new(SHUTDOWN_GRACE, SHUTDOWN_ABORT).expect("budget"),
    ));
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("handler entered");
    if matches!(outcome, Outcome::LeaseLost) {
        sqlx::query("UPDATE job_queue SET worker_id = 'replacement-worker' WHERE id = $1")
            .bind(job)
            .execute(&pool)
            .await
            .expect("replace lease owner");
    }
    if during_shutdown {
        shutdown.request_shutdown();
    }
    proceed.notify_one();
    tokio::time::timeout(Duration::from_secs(5), returned.notified())
        .await
        .expect("handler returned");
    shutdown.request_shutdown();
    let report = tokio::time::timeout(REPORT_DEADLOCK_GUARD, driver)
        .await
        .expect("bounded report")
        .expect("report driver joined");
    let record = get_job_by_id(&pool, None, job)
        .await
        .expect("read durable outcome")
        .expect("job exists");
    teardown_ephemeral_pool(pool, database).await;

    assert_durable_outcome(outcome, &record);
    assert_settlement(outcome, report);
}

fn assert_durable_outcome(outcome: Outcome, record: &runledger_postgres::jobs::JobQueueRecord) {
    if matches!(outcome, Outcome::LeaseLost) {
        assert_eq!(record.worker_id.as_deref(), Some("replacement-worker"));
        assert_eq!(
            record.status,
            JobStatus::Leased,
            "stale completion must remain fenced"
        );
    } else {
        assert_eq!(
            record.last_error_code.as_deref(),
            Some("job.timeout_exceeded")
        );
        assert_eq!(record.status, JobStatus::DeadLettered);
    }
}

fn assert_settlement(outcome: Outcome, report: runledger_runtime::RuntimeShutdownReport) {
    assert!(report.unjoined().is_empty());
    if matches!(outcome, Outcome::Panic) {
        assert_eq!(report.callback_failures().len(), 1);
        assert!(matches!(
            &report.callback_failures()[0],
            RuntimeCallbackFailure::Panicked { .. }
        ));
        assert!(matches!(
            report.classify(),
            runledger_runtime::RuntimeSettlement::Unsettled(_)
        ));
    } else {
        assert!(
            report.callback_failures().is_empty(),
            "completed handler is not interrupted"
        );
        assert_eq!(report.prior_callback_interruptions(), 0);
        assert!(matches!(
            report.classify(),
            runledger_runtime::RuntimeSettlement::Clean(_)
        ));
    }
}

#[tokio::test]
async fn late_success_still_times_out_but_permits_cleanup() {
    exercise(Outcome::Success, true).await;
}

#[tokio::test]
async fn earlier_late_business_failure_does_not_poison_cleanup_history() {
    exercise(Outcome::BusinessFailure, false).await;
}

#[tokio::test]
async fn late_continuation_still_times_out_but_permits_cleanup() {
    exercise(Outcome::Continuation, true).await;
}

#[tokio::test]
async fn completed_handler_after_lease_loss_permits_cleanup_and_remains_fenced() {
    exercise(Outcome::LeaseLost, true).await;
}

#[tokio::test]
async fn late_panic_still_prevents_cleanup() {
    exercise(Outcome::Panic, true).await;
}
