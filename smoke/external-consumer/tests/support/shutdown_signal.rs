use std::error::Error;
use std::time::Duration;

use runledger_runtime::prelude::*;
use sqlx::postgres::PgPoolOptions;

async fn close_accounted_pool(_permit: RuntimeShutdownCleanupPermit, pool: &sqlx::PgPool) {
    pool.close().await;
}

#[tokio::test]
async fn signal_error_retains_source_fails_shutdown_and_authorizes_accounted_cleanup() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .expect("valid inert pool URL");
    pool.close().await;
    let supervisor = Supervisor::builder(
        &pool,
        JobsConfig {
            worker_id: "external-signal-error".into(),
            poll_interval: Duration::from_millis(10),
            claim_batch_size: 1,
            lease_ttl_seconds: 10,
            max_global_concurrency: 1,
            reaper_interval: Duration::from_secs(1),
            schedule_poll_interval: Duration::from_secs(1),
            reaper_retry_delay_ms: 100,
        },
    )
    .expect("runtime present")
    .with_registry(JobRegistry::new())
    .disable_scheduler()
    .disable_reaper()
    .build()
    .expect("start native worker and intent promoter");
    supervisor
        .startup_observer()
        .wait_initialized()
        .await
        .expect("native loops initialized");
    let signal = RuntimeShutdownSignal::fallible(async {
        Err::<(), _>(std::io::Error::other("private signal listener detail"))
    });
    let budget = RuntimeShutdownBudget::new(Duration::from_secs(1), Duration::from_secs(1))
        .expect("valid budget");
    let report = tokio::time::timeout(
        Duration::from_secs(3),
        supervisor.run_until_shutdown_report(signal, budget),
    )
    .await
    .expect("native settlement completes");

    assert_eq!(report.cause(), RuntimeShutdownCause::SignalFailed);
    assert!(matches!(
        report.settlement(),
        RuntimeShutdownSettlement::Settled
    ));
    assert!(report.unjoined().is_empty());
    assert_eq!(report.loops().len(), 2);
    assert!(
        report
            .loops()
            .iter()
            .all(|record| matches!(record.result, Ok(RuntimeLoopExit::Shutdown)))
    );
    assert!(
        report
            .descendants()
            .iter()
            .any(|record| { record.task == "shutdown_signal" && record.error.is_none() })
    );
    assert!(!report.is_success());
    assert!(report.is_cooperatively_stopped());
    match report.cleanup_decision() {
        RuntimeShutdownCleanupDecision::Allowed(permit) => {
            close_accounted_pool(permit, &pool).await;
        }
        RuntimeShutdownCleanupDecision::Denied => {
            panic!("joined signal error still permits accounted cleanup");
        }
    }
    assert!(report.signal_panic().is_none());
    let signal_error: &RuntimeShutdownSignalError =
        report.signal_error().expect("retain typed signal error");
    let original = signal_error
        .source()
        .expect("retain source")
        .downcast_ref::<std::io::Error>()
        .expect("preserve original error type");
    assert_eq!(original.to_string(), "private signal listener detail");
    let failure = report.failure().expect("failed shutdown");
    assert!(matches!(&failure, RuntimeShutdownFailure::Signal { .. }));
    assert!(format!("{failure:?}").contains("RuntimeShutdownSignalError"));
    for formatted in [
        format!("{signal_error}"),
        format!("{signal_error:?}"),
        format!("{failure}"),
        format!("{failure:?}"),
        format!("{report:?}"),
    ] {
        assert!(!formatted.contains("private signal listener detail"));
    }
}

#[tokio::test]
async fn signal_panic_evidence_is_public_and_redacted() {
    let pool = PgPoolOptions::new()
        .connect_lazy("postgres://unused:unused@127.0.0.1:1/unused")
        .expect("valid inert pool URL");
    let supervisor = Supervisor::builder(
        &pool,
        JobsConfig {
            worker_id: "external-signal-panic".into(),
            poll_interval: Duration::from_millis(10),
            claim_batch_size: 1,
            lease_ttl_seconds: 10,
            max_global_concurrency: 1,
            reaper_interval: Duration::from_secs(1),
            schedule_poll_interval: Duration::from_secs(1),
            reaper_retry_delay_ms: 100,
        },
    )
    .expect("runtime present")
    .disable_worker()
    .disable_scheduler()
    .disable_reaper()
    .build()
    .expect("disabled supervisor");
    let report = supervisor
        .run_until_shutdown_report(
            RuntimeShutdownSignal::infallible(async { panic!("private external panic") }),
            RuntimeShutdownBudget::new(Duration::from_secs(1), Duration::from_secs(1))
                .expect("valid budget"),
        )
        .await;
    let panic: &RuntimeShutdownSignalPanic = report.signal_panic().expect("panic evidence");
    match panic {
        RuntimeShutdownSignalPanic::Poll { message } => {
            assert_eq!(message, "private external panic");
        }
        RuntimeShutdownSignalPanic::Destruction { .. }
        | RuntimeShutdownSignalPanic::PollAndDestruction { .. } => {
            panic!("this signal has no panicking destructor");
        }
    }
    assert!(matches!(report.failure(),
        Some(RuntimeShutdownFailure::DescendantJoin { task: "shutdown_signal", source })
            if source.is_panic()));
    assert!(!report.is_success());
    assert!(!report.is_cooperatively_stopped());
    assert!(matches!(
        report.cleanup_decision(),
        RuntimeShutdownCleanupDecision::Denied
    ));
    assert!(!format!("{panic:?}").contains("private"));
    assert!(!format!("{report:?}").contains("private"));
}
