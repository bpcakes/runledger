use super::*;
use crate::{RuntimeLoopExit, RuntimeLoopRecord, RuntimeShutdownCause, RuntimeShutdownReport};

// Hold a real task inside its last poll so every abort entry point observes it
// unfinished. It then returns Ready despite the request, as Tokio permits.
async fn completed_abort_race(mode: u8, stopping: bool) {
    let (shutdown, _) = crate::shutdown::ShutdownSignal::channel();
    let registry = TaskRegistry::supervised(shutdown.clone());
    let mut tasks = TaskSet::with_registry(registry.clone(), "callback");
    let (entered, started) = tokio::sync::oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let callback = async move {
        entered.send(()).expect("test waits for final poll");
        released
            .recv_timeout(Duration::from_secs(5))
            .expect("test releases final poll");
    };
    let join = if mode == 2 {
        tasks.spawn(callback);
        None
    } else {
        Some(registry.spawn("callback", callback))
    };
    tokio::time::timeout(Duration::from_secs(5), started)
        .await
        .expect("task starts")
        .expect("task entered final poll");
    if stopping {
        shutdown.request();
    }
    match mode {
        0 => join.as_ref().expect("individual join").abort(),
        1 => registry.abort_all(),
        _ => tasks.abort_all(),
    }
    release.send(()).expect("task is still in its final poll");
    if let Some(join) = join {
        join.await.expect("abort lost to normal completion");
    } else {
        tasks
            .join_next()
            .await
            .expect("registered task")
            .expect("abort lost to normal completion");
    }
    registry.wait().await;
    let (descendants, unjoined) = registry.snapshot();
    let (callback_failures, prior_callback_interruptions) = registry.callback_snapshot();
    assert_eq!(
        prior_callback_interruptions, 0,
        "mode {mode}: request alone is not interruption"
    );
    assert_eq!(descendants.len(), usize::from(stopping));
    if stopping {
        assert!(
            descendants[0].abort_requested,
            "retain the actual request as evidence"
        );
        assert!(descendants[0].error.is_none());
    }
    let report = RuntimeShutdownReport {
        cause: RuntimeShutdownCause::Requested,
        loops: Vec::new(),
        descendants,
        unjoined,
        graceful_timed_out: false,
        abort_timed_out: false,
        deadline_error: None,
        callback_failures,
        prior_callback_interruptions,
    };
    assert!(
        report.is_cooperatively_stopped(),
        "mode {mode}: completed callback permits cleanup"
    );
    assert!(report.is_success());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_race_before_shutdown_does_not_poison_history() {
    for mode in 0..3 {
        completed_abort_race(mode, false).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_race_during_shutdown_retains_request_and_permits_cleanup() {
    for mode in 0..3 {
        completed_abort_race(mode, true).await;
    }
}

#[test]
fn completed_loop_with_abort_request_permits_cleanup() {
    let mut report = RuntimeShutdownReport {
        cause: RuntimeShutdownCause::Requested,
        loops: vec![RuntimeLoopRecord {
            task: "worker",
            result: Ok(RuntimeLoopExit::Shutdown),
            abort_requested: true,
        }],
        descendants: Vec::new(),
        unjoined: Vec::new(),
        graceful_timed_out: false,
        abort_timed_out: false,
        deadline_error: None,
        callback_failures: Vec::new(),
        prior_callback_interruptions: 0,
    };
    assert!(report.is_cooperatively_stopped());
    assert!(report.is_success());
    report.graceful_timed_out = true;
    assert!(report.is_cooperatively_stopped());
    assert!(
        !report.is_success(),
        "a missed graceful budget still fails overall shutdown"
    );
}
