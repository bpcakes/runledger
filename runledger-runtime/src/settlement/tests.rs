use super::{CancellationPolicy, RuntimeTaskDisposition, TaskRegistry, TaskSet};

mod abort_race;

fn registry() -> TaskRegistry {
    TaskRegistry::supervised(crate::shutdown::ShutdownSignal::channel().0)
}

#[tokio::test]
async fn cancellation_policy_classifies_each_failed_join_once() {
    let cancelled = tokio::spawn(std::future::pending::<()>());
    cancelled.abort();
    let cancelled = Err(Arc::new(
        cancelled
            .await
            .expect_err("aborted task produces cancellation evidence"),
    ));

    let owner_cancelled = CancellationPolicy::WithOwner.classify(true, &cancelled);
    assert_eq!(
        owner_cancelled.disposition,
        RuntimeTaskDisposition::OwnerCancelled
    );
    assert!(!owner_cancelled.requests_shutdown);

    let unexpected_owner_loss = CancellationPolicy::WithOwner.classify(false, &cancelled);
    assert_eq!(
        unexpected_owner_loss.disposition,
        RuntimeTaskDisposition::Observed
    );
    assert!(!unexpected_owner_loss.requests_shutdown);

    let requested_ordinary_abort = CancellationPolicy::Unexpected.classify(true, &cancelled);
    assert_eq!(
        requested_ordinary_abort.disposition,
        RuntimeTaskDisposition::Observed
    );
    assert!(!requested_ordinary_abort.requests_shutdown);

    let panicked = Err(Arc::new(
        tokio::spawn(async { panic!("private classification panic") })
            .await
            .expect_err("panicking task produces panic evidence"),
    ));
    let panicked_after_abort = CancellationPolicy::WithOwner.classify(true, &panicked);
    assert_eq!(
        panicked_after_abort.disposition,
        RuntimeTaskDisposition::Observed
    );
    assert!(panicked_after_abort.requests_shutdown);
}

#[tokio::test]
async fn join_notification_does_not_own_the_registry_it_notifies() {
    let registry = registry();
    let weak = Arc::downgrade(&registry.0);
    let (release, released) = tokio::sync::oneshot::channel();
    let join = registry.spawn("callback", async move {
        released.await.expect("test releases actual callback");
    });
    let completion = join.abort.clone();
    registry.collect_ready();
    drop(join);
    drop(registry);
    let retained = weak.upgrade();
    let cycle = retained.is_some();
    release.send(()).expect("callback remains alive");
    // Keep an observed old registry alive while finishing the task, so the
    // regression fails without itself deadlocking inside Shared's notifier lock.
    if let Some(inner) = retained {
        tokio::time::timeout(Duration::from_secs(1), TaskRegistry(inner).wait())
            .await
            .expect("finish callback before asserting owner cycle");
    } else {
        tokio::time::timeout(Duration::from_secs(1), async {
            while !completion.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("actual callback completes after registry drop");
    }
    assert!(
        !cycle,
        "join notification must retain only a wake signal, not its registry"
    );
}
use std::{
    future::pending,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

#[tokio::test]
async fn dropped_parent_retains_the_actual_child_join() {
    let (shutdown, _) = crate::shutdown::ShutdownSignal::channel();
    let registry = TaskRegistry::supervised(shutdown.clone());
    let mut parent = TaskSet::with_registry(registry.clone(), "callback");
    parent.spawn(pending::<()>());
    shutdown.request();
    drop(parent);
    tokio::time::timeout(Duration::from_secs(1), registry.wait())
        .await
        .expect("observe abort completion");
    let (records, pending) = registry.snapshot();
    assert!(pending.is_empty());
    assert_eq!(records.len(), 1);
    assert!(records[0].abort_requested);
    assert!(
        records[0]
            .error
            .as_ref()
            .expect("cancelled join")
            .is_cancelled()
    );
}

#[tokio::test]
async fn caller_and_settlement_observe_the_same_panic() {
    let registry = registry();
    let task = registry.spawn("callback", async {
        panic!("callback panic");
    });
    registry.wait().await;
    let error = task.await.expect_err("callback failed");
    let (records, pending) = registry.snapshot();
    assert!(pending.is_empty());
    assert!(Arc::ptr_eq(
        &error,
        records[0]
            .error
            .as_ref()
            .expect("retained original join cause")
    ));
}

#[tokio::test]
async fn forced_stop_prevents_later_application_future_polling() {
    let registry = registry();
    registry.abort_all();
    let polled = Arc::new(AtomicBool::new(false));
    let observed = polled.clone();
    let task = registry.spawn("late_callback", async move {
        observed.store(true, Ordering::SeqCst);
    });
    assert!(
        task.await
            .expect_err("late admission aborted")
            .is_cancelled()
    );
    assert!(!polled.load(Ordering::SeqCst));
    registry.wait().await;
}

#[tokio::test]
async fn completion_does_not_repoll_unrelated_pending_joins() {
    use futures_util::FutureExt;
    use std::sync::atomic::AtomicUsize;
    let registry = registry();
    let mut joins = Vec::new();
    for _ in 0..128 {
        joins.push(registry.spawn("pending", pending::<()>()));
    }
    tokio::task::yield_now().await;
    registry.collect_ready();
    let polls = Arc::new(AtomicUsize::new(0));
    {
        let mut state = registry.0.state.lock().expect("registry is not poisoned");
        for entry in state.entries.values_mut() {
            let mut original = std::mem::replace(&mut entry.result, pending().boxed());
            let count = polls.clone();
            entry.result = std::future::poll_fn(move |cx| {
                count.fetch_add(1, Ordering::Relaxed);
                original.as_mut().poll(cx)
            })
            .boxed();
        }
    }
    registry
        .spawn("completed", async {})
        .await
        .expect("actual task completes");
    let unrelated_polls = polls.load(Ordering::Relaxed);
    registry.abort_all();
    registry.wait().await;
    drop(joins);
    assert_eq!(
        unrelated_polls, 0,
        "one completion rescanned unrelated pending joins"
    );
}

#[tokio::test]
async fn try_join_next_harvests_ready_tasks_with_exhausted_cooperative_budget() {
    use std::{future::Future, task::Poll};
    let mut tasks = TaskSet::new();
    let mut aborts = Vec::new();
    for _ in 0..256 {
        aborts.push(tasks.spawn(async {}));
    }
    while !aborts.iter().all(tokio::task::AbortHandle::is_finished) {
        tokio::task::yield_now().await;
    }
    std::future::poll_fn(|cx| {
        loop {
            let budget = tokio::task::consume_budget();
            tokio::pin!(budget);
            if budget.poll(cx).is_pending() {
                return Poll::Ready(());
            }
        }
    })
    .await;
    let mut harvested = 0;
    while let Some(result) = tasks.try_join_next() {
        result.expect("successful task");
        harvested += 1;
    }
    assert_eq!(
        harvested, 256,
        "cooperative scheduling hid already completed tasks"
    );
}

#[tokio::test]
async fn shutdown_boundaries_harvest_finished_joins_without_ready_notifications() {
    for boundary in 0..3 {
        let (shutdown, _) = crate::shutdown::ShutdownSignal::channel();
        let registry = TaskRegistry::supervised(shutdown.clone());
        let task = registry.spawn("finished", async {});
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
        // Model a completion whose notification is held by another collector
        // or has not yet reached the ready queue. The actual join is finished.
        let held = std::mem::take(
            &mut *registry
                .0
                .signal
                .ready
                .lock()
                .expect("ready queue is not poisoned"),
        );
        shutdown.request();
        match boundary {
            0 => assert!(registry.snapshot().1.is_empty()),
            1 => assert!(registry.is_empty()),
            _ => registry.abort_all(),
        }
        let (records, pending) = registry.snapshot();
        assert!(pending.is_empty());
        assert_eq!(records.len(), 1);
        assert!(!records[0].abort_requested);
        assert!(records[0].error.is_none());
        // Delayed notification delivery must not duplicate a completed record.
        registry
            .0
            .signal
            .ready
            .lock()
            .expect("ready queue is not poisoned")
            .extend(held);
        task.await.expect("same successful join remains available");
        assert_eq!(registry.snapshot().0.len(), 1);
    }
}

#[tokio::test]
async fn boundary_harvest_retains_original_failure_without_notification() {
    let registry = registry();
    let task = registry.spawn("failed", async { panic!("native task failed") });
    while !task.is_finished() {
        tokio::task::yield_now().await;
    }
    let held = std::mem::take(
        &mut *registry
            .0
            .signal
            .ready
            .lock()
            .expect("ready queue is not poisoned"),
    );
    let (records, pending) = registry.snapshot();
    registry
        .0
        .signal
        .ready
        .lock()
        .expect("ready queue is not poisoned")
        .extend(held);
    let original = task.await.expect_err("actual task panicked");
    assert!(pending.is_empty());
    assert_eq!(records.len(), 1);
    assert!(Arc::ptr_eq(
        records[0]
            .error
            .as_ref()
            .expect("original failure retained"),
        &original
    ));
    assert_eq!(registry.snapshot().0.len(), 1);
}
