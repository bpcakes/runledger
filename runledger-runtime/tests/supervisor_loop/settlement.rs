use super::*;
use runledger_runtime::{RuntimeShutdownBudget, RuntimeShutdownReport, RuntimeShutdownSignal};
use std::{future::pending, sync::Mutex};
use tokio::sync::{Notify, oneshot};

const JOB: &str = "jobs.test.handler_settlement";
const SHUTDOWN_GRACE: Duration = Duration::from_secs(20);
const SHUTDOWN_ABORT: Duration = Duration::from_secs(5);
const REPORT_DEADLOCK_GUARD: Duration = Duration::from_secs(30);

#[derive(Clone, Copy)]
enum Exit {
    Timeout,
    Panic,
    LeaseLoss,
    Business,
}

struct DropNotice(Arc<Notify>);
impl Drop for DropNotice {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

struct Handler {
    exit: Exit,
    entered: Arc<Notify>,
    proceed: Arc<Notify>,
    destroyed: Arc<Notify>,
    child: Mutex<Option<oneshot::Sender<tokio::task::JoinHandle<()>>>>,
    release_child: Mutex<Option<oneshot::Receiver<()>>>,
}

#[async_trait::async_trait]
impl JobHandler for Handler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(JOB)
    }
    async fn execute(&self, _: JobContext, _: Value) -> Result<JobCompletion, JobFailure> {
        let _destroyed = DropNotice(self.destroyed.clone());
        let released = self
            .release_child
            .lock()
            .expect("handler child-release state")
            .take()
            .expect("one handler invocation");
        let child = tokio::spawn(async move {
            released.await.expect("fixture releases actual child");
        });
        if matches!(self.exit, Exit::Business) {
            self.entered.notify_one();
            child.await.expect("handler child joined");
            return Err(JobFailure::terminal("job.test.denied", "business denial"));
        }
        self.child
            .lock()
            .expect("handler child-handoff state")
            .take()
            .expect("one handoff")
            .send(child)
            .expect("child handed to fixture");
        self.entered.notify_one();
        self.proceed.notified().await;
        if matches!(self.exit, Exit::Panic) {
            panic!("controlled handler panic");
        }
        pending().await
    }
}

async fn enqueue_case(pool: &sqlx::PgPool, exit: Exit) -> sqlx::types::Uuid {
    let mut tx = pool.begin().await.expect("begin definition");
    upsert_job_definition_tx(
        &mut tx,
        &JobDefinitionUpsert {
            job_type: JobType::new(JOB),
            version: 1,
            max_attempts: 1,
            default_timeout_seconds: if matches!(exit, Exit::Timeout) { 1 } else { 30 },
            default_priority: 100,
            is_enabled: true,
        },
    )
    .await
    .expect("upsert definition");
    tx.commit().await.expect("commit definition");
    enqueue_job(
        pool,
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
    .expect("enqueue job")
}

async fn exercise(exit: Exit, during_shutdown: bool) {
    let (pool, database) = setup_ephemeral_pool("runtime_handler_settlement", 6).await;
    let job = enqueue_case(&pool, exit).await;
    let entered = Arc::new(Notify::new());
    let proceed = Arc::new(Notify::new());
    let destroyed = Arc::new(Notify::new());
    let (child_tx, child_rx) = oneshot::channel();
    let (release_child, child_release) = oneshot::channel();
    let mut registry = JobRegistry::new();
    registry.register(Handler {
        exit,
        entered: entered.clone(),
        proceed: proceed.clone(),
        destroyed: destroyed.clone(),
        child: Mutex::new(Some(child_tx)),
        release_child: Mutex::new(Some(child_release)),
    });
    let mut config = test_config();
    config.lease_ttl_seconds = 3;
    let supervisor = Supervisor::builder(&pool, config)
        .expect("runtime present")
        .with_registry(registry)
        .disable_scheduler()
        .disable_reaper()
        .build()
        .expect("build supervisor");
    let shutdown = supervisor.shutdown_handle();
    let driver = tokio::spawn(supervisor.run_until_shutdown_report(
        RuntimeShutdownSignal::pending(),
        // Fixture gates establish the interruption mode. The runtime budget
        // is only a deadlock guard and must not race loaded database work.
        RuntimeShutdownBudget::new(SHUTDOWN_GRACE, SHUTDOWN_ABORT).expect("validated budget"),
    ));
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .expect("handler entered");
    if during_shutdown {
        shutdown.request_shutdown();
    }
    let child = if matches!(exit, Exit::Business) {
        release_child
            .send(())
            .expect("release joined business child");
        None
    } else {
        let child = child_rx.await.expect("receive retained child");
        proceed.notify_one();
        if matches!(exit, Exit::LeaseLoss) {
            sqlx::query("UPDATE job_queue SET worker_id = 'replacement-worker' WHERE id=$1")
                .bind(job)
                .execute(&pool)
                .await
                .expect("replace lease owner");
        }
        Some((child, release_child))
    };
    tokio::time::timeout(Duration::from_secs(5), destroyed.notified())
        .await
        .expect("handler future destroyed");
    shutdown.request_shutdown();
    let report = tokio::time::timeout(REPORT_DEADLOCK_GUARD, driver)
        .await
        .expect("report within deadline")
        .expect("report driver joined");
    // Always settle the independently retained application child before assertions.
    let child_was_alive = if let Some((child, release)) = child {
        let alive = !child.is_finished();
        release.send(()).expect("release detached child");
        child.await.expect("detached child joined");
        alive
    } else {
        false
    };
    teardown_ephemeral_pool(pool, database).await;
    assert_report(report, exit, during_shutdown, child_was_alive);
}

fn assert_report(report: RuntimeShutdownReport, exit: Exit, during: bool, child_alive: bool) {
    assert!(report.unjoined().is_empty());
    assert!(report.loops().iter().all(|record| record.result.is_ok()));
    assert!(
        report
            .descendants()
            .iter()
            .all(|record| record.error.is_none())
    );
    if matches!(exit, Exit::Business) {
        assert!(matches!(
            report.classify(),
            runledger_runtime::RuntimeSettlement::Clean(_)
        ));
    } else {
        assert!(
            child_alive,
            "the native joins did not settle the application child"
        );
        if during {
            assert!(!report.callback_failures().is_empty());
        } else {
            assert!(report.prior_callback_interruptions() > 0);
        }
        assert!(
            matches!(
                report.classify(),
                runledger_runtime::RuntimeSettlement::Unsettled(_)
            ),
            "interrupted handler approved cleanup"
        );
    }
}

#[tokio::test]
async fn timeout_during_shutdown_prevents_cleanup() {
    exercise(Exit::Timeout, true).await;
}
#[tokio::test]
async fn panic_during_shutdown_prevents_cleanup() {
    exercise(Exit::Panic, true).await;
}
#[tokio::test]
async fn lease_loss_during_shutdown_prevents_cleanup() {
    exercise(Exit::LeaseLoss, true).await;
}
#[tokio::test]
async fn earlier_timeout_prevents_cleanup() {
    exercise(Exit::Timeout, false).await;
}
#[tokio::test]
async fn earlier_panic_prevents_cleanup() {
    exercise(Exit::Panic, false).await;
}
#[tokio::test]
async fn earlier_lease_loss_prevents_cleanup() {
    exercise(Exit::LeaseLoss, false).await;
}
#[tokio::test]
async fn business_failure_with_joined_child_permits_cleanup() {
    exercise(Exit::Business, true).await;
}

struct DestructorPanicHandler;

#[async_trait::async_trait]
impl JobHandler for DestructorPanicHandler {
    fn job_type(&self) -> JobType<'static> {
        JobType::new(JOB)
    }
    async fn execute(&self, _: JobContext, _: Value) -> Result<JobCompletion, JobFailure> {
        struct PanickingDrop;
        impl Drop for PanickingDrop {
            fn drop(&mut self) {
                panic!("worker handler destruction escaped catch boundary");
            }
        }
        let _guard = PanickingDrop;
        pending().await
    }
}

// The panic happens while the worker is running a claimed job, so the driver has
// to be the one that waits: `shutdown_report` would stop the loops before the job
// was ever claimed. Its own coverage lives in the supervisor unit tests.
#[tokio::test]
async fn an_escaped_worker_destructor_panic_fails_the_shutdown_report() {
    let (pool, database) = setup_ephemeral_pool("runtime_escaped_worker_panic", 6).await;
    enqueue_case(&pool, Exit::Timeout).await;
    let mut registry = JobRegistry::new();
    registry.register(DestructorPanicHandler);
    let supervisor = Supervisor::builder(&pool, test_config())
        .expect("runtime present")
        .with_registry(registry)
        .disable_scheduler()
        .disable_reaper()
        .build()
        .expect("valid supervisor");
    let budget = RuntimeShutdownBudget::new(Duration::from_secs(2), Duration::from_secs(1))
        .expect("valid shutdown budget");
    let report = tokio::time::timeout(
        Duration::from_secs(10),
        supervisor.run_until_shutdown_report(RuntimeShutdownSignal::pending(), budget),
    )
    .await
    .expect("worker panic initiates native stop");
    teardown_ephemeral_pool(pool, database).await;

    let Some(runledger_runtime::RuntimeShutdownFailure::DescendantJoin { task, source }) =
        report.failure()
    else {
        panic!("escaped worker panic must be retained by the report");
    };
    assert_eq!(task, "worker_job");
    assert!(source.is_panic());
    assert!(
        matches!(
            report.classify(),
            runledger_runtime::RuntimeSettlement::Unsettled(_)
        ),
        "an escaped worker panic cannot authorize dependency cleanup"
    );
}
