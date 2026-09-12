use super::*;
use runledger_runtime::{RuntimeShutdownBudget, RuntimeShutdownReport};
use std::{future::pending, sync::Mutex};
use tokio::sync::{Notify, oneshot};

const JOB: &str = "jobs.test.handler_settlement";

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
    let driver = tokio::spawn(
        supervisor.run_until_shutdown_report(
            pending(),
            RuntimeShutdownBudget::new(Duration::from_secs(5), Duration::from_secs(1))
                .expect("validated budget"),
        ),
    );
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
    let report = tokio::time::timeout(Duration::from_secs(8), driver)
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
    assert!(report.unjoined.is_empty());
    assert!(report.loops.iter().all(|record| record.result.is_ok()));
    assert!(
        report
            .descendants
            .iter()
            .all(|record| record.error.is_none())
    );
    if matches!(exit, Exit::Business) {
        assert!(report.is_cooperatively_stopped());
        assert!(report.is_success());
    } else {
        assert!(
            child_alive,
            "the native joins did not settle the application child"
        );
        assert!(
            !report.is_cooperatively_stopped(),
            "interrupted handler approved cleanup"
        );
        assert!(!report.is_success());
        if during {
            assert!(!report.callback_failures.is_empty());
        } else {
            assert!(report.prior_callback_interruptions > 0);
        }
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

async fn escaped_worker_failure(join_only: bool) {
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
    let result = tokio::time::timeout(Duration::from_secs(10), async {
        if join_only {
            supervisor.join().await
        } else {
            supervisor
                .run_until_shutdown(pending(), Duration::from_secs(2))
                .await
        }
    })
    .await;
    teardown_ephemeral_pool(pool, database).await;
    let Err(runledger_runtime::Error::Runtime(runledger_runtime::RuntimeError::DescendantJoin {
        task,
        source,
    })) = result.expect("worker panic initiates native stop")
    else {
        panic!("escaped worker panic must fail the legacy driver");
    };
    assert_eq!(task, "worker_job");
    assert!(source.is_panic());
}

#[tokio::test]
async fn supervised_worker_destructor_panic_fails_join() {
    escaped_worker_failure(true).await;
}

#[tokio::test]
async fn supervised_worker_destructor_panic_fails_run_until_shutdown() {
    escaped_worker_failure(false).await;
}
