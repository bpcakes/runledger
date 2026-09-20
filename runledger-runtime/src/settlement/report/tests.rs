use super::*;

#[test]
fn budget_sum_overflow_retains_both_operands() {
    for (graceful, abort) in [
        (Duration::from_secs(1), Duration::MAX),
        (Duration::MAX, Duration::from_nanos(1)),
        (Duration::new(u64::MAX, 0), Duration::from_secs(1)),
    ] {
        let error = RuntimeShutdownBudget::new(graceful, abort).expect_err("sum overflows");
        assert!(matches!(error, RuntimeError::ShutdownBudgetOverflow {
            graceful: actual_graceful, abort: actual_abort,
        } if actual_graceful == graceful && actual_abort == abort));
        assert_eq!(
            error.to_string(),
            format!(
                "jobs runtime shutdown budget overflows: graceful {graceful:?}, abort {abort:?}"
            )
        );
    }
}

#[test]
fn representable_sum_with_unrepresentable_deadline_reports_the_total() {
    let graceful = Duration::from_secs(1);
    let abort = Duration::MAX - graceful;
    let error = RuntimeShutdownBudget::new(graceful, abort).expect_err("instant overflows");
    assert!(
        matches!(error, RuntimeError::ShutdownTimeoutTooLarge { timeout } if timeout == Duration::MAX)
    );
}

#[test]
fn valid_budget_preserves_both_phases_including_zero() {
    for (graceful, abort) in [
        (Duration::ZERO, Duration::ZERO),
        (Duration::ZERO, Duration::from_secs(1)),
        (Duration::from_secs(1), Duration::ZERO),
        (Duration::from_secs(2), Duration::from_secs(3)),
    ] {
        let budget = RuntimeShutdownBudget::new(graceful, abort).expect("valid budget");
        let start = Instant::now();
        assert_eq!(budget.graceful_allowance(), graceful);
        assert_eq!(budget.total_allowance(), graceful + abort);
        assert_eq!(
            budget.deadlines(start),
            Some((start + graceful, start + graceful + abort))
        );
    }
}

fn report_with(
    settlement: RuntimeShutdownSettlement,
    deadline_error: Option<RuntimeError>,
    callback_failures: Vec<RuntimeCallbackFailure>,
    prior_callback_interruptions: u64,
) -> RuntimeShutdownReport {
    RuntimeShutdownReport::new(
        RuntimeShutdownCause::Requested,
        Vec::new(),
        Vec::new(),
        settlement,
        RuntimeShutdownObservations {
            deadline_error,
            signal_error: None,
            signal_panic: None,
        },
        callback_failures,
        prior_callback_interruptions,
    )
}

#[tokio::test]
async fn classification_tracks_cleanup_and_process_failure_across_report_states() {
    let unjoined = tokio::spawn(std::future::pending::<()>());
    let descendant = tokio::spawn(async { panic!("private descendant panic") });
    let descendant_id = descendant.id();
    let descendant_error = Arc::new(
        descendant
            .await
            .expect_err("descendant panic produces join evidence"),
    );
    let reports = vec![
        (
            "settled",
            report_with(RuntimeShutdownSettlement::Settled, None, Vec::new(), 0),
            true,
        ),
        (
            "graceful timeout",
            report_with(
                RuntimeShutdownSettlement::GracefulTimeout,
                None,
                Vec::new(),
                0,
            ),
            true,
        ),
        (
            "abort timeout",
            report_with(
                RuntimeShutdownSettlement::AbortTimeout {
                    unjoined: UnjoinedRuntimeTasks::new(vec![UnsettledRuntimeTask {
                        task: "unjoined",
                        id: unjoined.id(),
                        abort_requested: true,
                    }])
                    .expect("abort timeout evidence is nonempty"),
                },
                None,
                Vec::new(),
                0,
            ),
            false,
        ),
        (
            "interrupted",
            report_with(
                RuntimeShutdownSettlement::Interrupted {
                    unjoined: Vec::new(),
                },
                None,
                Vec::new(),
                0,
            ),
            false,
        ),
        (
            "callback interruption",
            report_with(
                RuntimeShutdownSettlement::Settled,
                None,
                vec![RuntimeCallbackFailure::TimedOut {
                    callback: "handler",
                }],
                0,
            ),
            false,
        ),
        (
            "earlier callback interruption after all joins",
            report_with(RuntimeShutdownSettlement::Settled, None, Vec::new(), 1),
            false,
        ),
        (
            "joined unexpected loop exit",
            RuntimeShutdownReport::new(
                RuntimeShutdownCause::LoopFailure("worker"),
                vec![RuntimeLoopRecord {
                    task: "worker",
                    result: Ok(RuntimeLoopExit::Completed),
                    abort_requested: false,
                }],
                Vec::new(),
                RuntimeShutdownSettlement::Settled,
                RuntimeShutdownObservations::default(),
                Vec::new(),
                0,
            ),
            true,
        ),
        (
            "joined signal error",
            RuntimeShutdownReport::new(
                RuntimeShutdownCause::SignalFailed,
                Vec::new(),
                Vec::new(),
                RuntimeShutdownSettlement::Settled,
                signal_observations(),
                Vec::new(),
                0,
            ),
            true,
        ),
        (
            "signal panic",
            RuntimeShutdownReport::new(
                RuntimeShutdownCause::Requested,
                Vec::new(),
                Vec::new(),
                RuntimeShutdownSettlement::Settled,
                signal_panic_observations(),
                Vec::new(),
                0,
            ),
            false,
        ),
        (
            "descendant failure",
            RuntimeShutdownReport::new(
                RuntimeShutdownCause::DescendantFailure {
                    task: "descendant",
                    id: descendant_id,
                },
                Vec::new(),
                vec![RuntimeTaskRecord {
                    task: "descendant",
                    id: descendant_id,
                    abort_requested: false,
                    error: Some(descendant_error),
                    disposition: super::super::RuntimeTaskDisposition::Observed,
                }],
                RuntimeShutdownSettlement::Settled,
                RuntimeShutdownObservations::default(),
                Vec::new(),
                0,
            ),
            false,
        ),
    ];

    for (state, report, expected_allowed) in reports {
        assert_classification(state, report, expected_allowed);
    }

    unjoined.abort();
    assert!(
        unjoined
            .await
            .expect_err("pending task is cancelled")
            .is_cancelled()
    );
}

fn assert_classification(state: &str, report: RuntimeShutdownReport, expected_allowed: bool) {
    let outcome = report.classify();
    assert_eq!(
        matches!(
            outcome,
            RuntimeSettlement::Clean(_) | RuntimeSettlement::StoppedWithFailures(_)
        ),
        expected_allowed,
        "{state} cleanup decision"
    );
    assert_eq!(
        matches!(outcome, RuntimeSettlement::Clean(_)),
        state == "settled",
        "{state} process classification"
    );
    match outcome {
        RuntimeSettlement::Clean(clean) => {
            assert!(clean.report().failure().is_none());
            let _permit = clean.into_cleanup_permit();
        }
        RuntimeSettlement::StoppedWithFailures(stopped) => {
            assert_eq!(
                stopped.failure().to_string(),
                stopped
                    .report()
                    .failure()
                    .expect("retained failure")
                    .to_string()
            );
            let (_permit, _failure) = stopped.into_parts();
        }
        RuntimeSettlement::Unsettled(unsettled) => {
            assert_eq!(
                unsettled.failure().to_string(),
                unsettled
                    .report()
                    .failure()
                    .expect("retained failure")
                    .to_string()
            );
            let _failure = unsettled.into_failure();
        }
    }
}

#[test]
fn callback_name_is_available_for_every_interruption_cause() {
    for (failure, expected) in [
        (
            RuntimeCallbackFailure::TimedOut {
                callback: "timed_out",
            },
            "timed_out",
        ),
        (
            RuntimeCallbackFailure::Panicked {
                callback: "panicked",
                message: "private".to_owned(),
            },
            "panicked",
        ),
        (
            RuntimeCallbackFailure::LeaseMaintenance {
                callback: "lease_maintenance",
            },
            "lease_maintenance",
        ),
    ] {
        assert_eq!(failure.callback(), expected);
    }
}

#[tokio::test]
async fn every_settlement_state_has_one_reachable_classification() {
    let settled = report_with(RuntimeShutdownSettlement::Settled, None, Vec::new(), 0);
    assert!(settled.failure().is_none());

    let graceful = report_with(
        RuntimeShutdownSettlement::GracefulTimeout,
        None,
        Vec::new(),
        0,
    );
    assert!(matches!(
        graceful.failure(),
        Some(RuntimeShutdownFailure::GracefulTimeout)
    ));

    let handle = tokio::spawn(async {});
    let id = handle.id();
    handle.abort();
    let abort = report_with(
        RuntimeShutdownSettlement::AbortTimeout {
            unjoined: UnjoinedRuntimeTasks::new(vec![UnsettledRuntimeTask {
                task: "callback",
                id,
                abort_requested: true,
            }])
            .expect("abort timeout evidence is nonempty"),
        },
        None,
        Vec::new(),
        0,
    );
    assert!(matches!(
        abort.failure(),
        Some(RuntimeShutdownFailure::AbortTimeout { unjoined }) if unjoined.get() == 1
    ));

    for report in [settled, graceful, abort] {
        assert_eq!(report.is_success(), report.failure().is_none());
    }
}

#[test]
fn abort_timeout_evidence_rejects_an_empty_collection() {
    assert!(UnjoinedRuntimeTasks::new(Vec::new()).is_none());
    assert!(matches!(
        RuntimeShutdownSettlement::after_graceful_timeout(Vec::new()),
        RuntimeShutdownSettlement::GracefulTimeout
    ));
}

#[test]
fn interrupted_settlement_with_no_known_tasks_never_authorizes_cleanup() {
    let (shutdown, _receiver) = crate::shutdown::ShutdownSignal::channel();
    shutdown.request();
    let report = RuntimeShutdownReport::unavailable(&shutdown);
    assert!(report.unjoined().is_empty());
    assert!(report.loops().is_empty());
    assert!(report.descendants().is_empty());
    assert!(!report.graceful_timed_out());
    assert!(!report.abort_timed_out());
    assert!(!report.is_cooperatively_stopped());
    assert!(!report.is_success());
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::SettlementInterrupted)
    ));
    assert_eq!(report.is_success(), report.failure().is_none());
}

#[tokio::test]
async fn requested_timeout_remains_primary_while_retaining_a_drain_panic() {
    let panic = Arc::new(
        tokio::spawn(async { panic!("private drain failure") })
            .await
            .expect_err("retain panic join evidence"),
    );
    let report = RuntimeShutdownReport::new(
        RuntimeShutdownCause::Requested,
        vec![RuntimeLoopRecord {
            task: "drain_panic",
            result: Err(Arc::clone(&panic)),
            abort_requested: false,
        }],
        Vec::new(),
        RuntimeShutdownSettlement::GracefulTimeout,
        RuntimeShutdownObservations::default(),
        Vec::new(),
        0,
    );
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::GracefulTimeout)
    ));
    assert!(
        matches!(report.loops()[0].failure(), Some(RuntimeShutdownFailure::LoopJoin { source, .. })
        if Arc::ptr_eq(&source, &panic))
    );
    assert!(!report.is_cooperatively_stopped());
    assert_eq!(report.is_success(), report.failure().is_none());
}

fn signal_observations() -> RuntimeShutdownObservations {
    RuntimeShutdownObservations {
        signal_error: Some(crate::RuntimeShutdownSignalError::new(
            std::io::Error::other("private signal error"),
        )),
        ..RuntimeShutdownObservations::default()
    }
}

fn signal_panic_observations() -> RuntimeShutdownObservations {
    RuntimeShutdownObservations {
        signal_panic: Some(crate::RuntimeShutdownSignalPanic::PollAndDestruction {
            poll_message: "later signal poll panic".to_owned(),
            destruction_message: "later signal destruction panic".to_owned(),
        }),
        ..RuntimeShutdownObservations::default()
    }
}

#[tokio::test]
async fn first_cause_defines_failure_precedence_across_timeout_states() {
    let descendant = tokio::spawn(async { panic!("private descendant failure") });
    let descendant_id = descendant.id();
    let descendant_error = Arc::new(
        descendant
            .await
            .expect_err("descendant panic produces join evidence"),
    );
    let unjoined = tokio::spawn(std::future::pending::<()>());
    let settlements = [
        RuntimeShutdownSettlement::GracefulTimeout,
        RuntimeShutdownSettlement::AbortTimeout {
            unjoined: UnjoinedRuntimeTasks::new(vec![UnsettledRuntimeTask {
                task: "unjoined",
                id: unjoined.id(),
                abort_requested: true,
            }])
            .expect("abort timeout evidence is nonempty"),
        },
    ];

    for settlement in settlements {
        let requested = RuntimeShutdownReport::new(
            RuntimeShutdownCause::Requested,
            Vec::new(),
            Vec::new(),
            settlement.clone(),
            signal_observations(),
            Vec::new(),
            0,
        );
        match settlement {
            RuntimeShutdownSettlement::GracefulTimeout => assert!(matches!(
                requested.failure(),
                Some(RuntimeShutdownFailure::GracefulTimeout)
            )),
            RuntimeShutdownSettlement::AbortTimeout { .. } => assert!(matches!(
                requested.failure(),
                Some(RuntimeShutdownFailure::AbortTimeout { .. })
            )),
            RuntimeShutdownSettlement::Settled | RuntimeShutdownSettlement::Interrupted { .. } => {
                unreachable!()
            }
        }

        let signal = RuntimeShutdownReport::new(
            RuntimeShutdownCause::SignalFailed,
            Vec::new(),
            Vec::new(),
            settlement.clone(),
            signal_observations(),
            Vec::new(),
            0,
        );
        assert!(matches!(
            signal.failure(),
            Some(RuntimeShutdownFailure::Signal { .. })
        ));

        for observations in [signal_observations, signal_panic_observations] {
            let loop_failure = RuntimeShutdownReport::new(
                RuntimeShutdownCause::LoopFailure("loop"),
                vec![RuntimeLoopRecord {
                    task: "loop",
                    result: Ok(RuntimeLoopExit::Completed),
                    abort_requested: false,
                }],
                Vec::new(),
                settlement.clone(),
                observations(),
                Vec::new(),
                0,
            );
            assert!(matches!(
                loop_failure.failure(),
                Some(RuntimeShutdownFailure::LoopExitedUnexpectedly { task: "loop" })
            ));

            let descendant_failure = RuntimeShutdownReport::new(
                RuntimeShutdownCause::DescendantFailure {
                    task: "descendant",
                    id: descendant_id,
                },
                Vec::new(),
                vec![RuntimeTaskRecord {
                    task: "descendant",
                    id: descendant_id,
                    abort_requested: false,
                    error: Some(Arc::clone(&descendant_error)),
                    disposition: super::super::RuntimeTaskDisposition::Observed,
                }],
                settlement.clone(),
                observations(),
                Vec::new(),
                0,
            );
            assert!(matches!(descendant_failure.failure(),
            Some(RuntimeShutdownFailure::DescendantJoin { task: "descendant", source })
                if Arc::ptr_eq(&source, &descendant_error)));
        }
    }
    unjoined.abort();
    assert!(
        unjoined
            .await
            .expect_err("pending task is cancelled")
            .is_cancelled()
    );
}

#[test]
fn joined_signal_error_fails_shutdown_but_permits_cleanup() {
    let report = RuntimeShutdownReport::new(
        RuntimeShutdownCause::SignalFailed,
        Vec::new(),
        Vec::new(),
        RuntimeShutdownSettlement::Settled,
        signal_observations(),
        Vec::new(),
        0,
    );

    assert!(report.is_cooperatively_stopped());
    assert!(!report.is_success());
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::Signal { .. })
    ));
}

#[test]
fn retained_signal_panic_directly_denies_cleanup_without_a_join_failure() {
    let report = RuntimeShutdownReport::new(
        RuntimeShutdownCause::Requested,
        Vec::new(),
        Vec::new(),
        RuntimeShutdownSettlement::Settled,
        RuntimeShutdownObservations {
            signal_panic: Some(crate::RuntimeShutdownSignalPanic::Destruction {
                message: "private signal destruction panic".to_owned(),
            }),
            ..RuntimeShutdownObservations::default()
        },
        Vec::new(),
        0,
    );

    assert!(report.descendants().is_empty());
    assert!(report.signal_panic().is_some());
    assert!(!report.is_cooperatively_stopped());
    assert!(!report.is_success());
    assert!(matches!(
        report.failure(),
        Some(RuntimeShutdownFailure::SignalPanicked)
    ));
    assert_eq!(report.is_success(), report.failure().is_none());
    assert!(!format!("{report:?}").contains("private"));
}

#[tokio::test]
async fn deadline_and_callback_classifications_are_ordered_and_named() {
    let task = tokio::spawn(std::future::pending::<()>());
    let abort_timeout =
        RuntimeShutdownSettlement::after_graceful_timeout(vec![UnsettledRuntimeTask {
            task: "unjoined",
            id: task.id(),
            abort_requested: true,
        }]);
    for settlement in [
        RuntimeShutdownSettlement::Settled,
        RuntimeShutdownSettlement::GracefulTimeout,
        abort_timeout,
    ] {
        let deadline = report_with(
            settlement.clone(),
            Some(RuntimeError::ShutdownTimeoutTooLarge {
                timeout: Duration::MAX,
            }),
            vec![RuntimeCallbackFailure::TimedOut {
                callback: "later_callback",
            }],
            1,
        );
        assert!(matches!(
            deadline.failure(),
            Some(RuntimeShutdownFailure::UnrepresentableDeadline)
        ));

        let callback = report_with(
            settlement.clone(),
            None,
            vec![RuntimeCallbackFailure::LeaseMaintenance {
                callback: "handler",
            }],
            1,
        );
        if matches!(settlement, RuntimeShutdownSettlement::Settled) {
            assert!(matches!(
                callback.failure(),
                Some(RuntimeShutdownFailure::CallbackInterrupted {
                    callback: "handler"
                })
            ));
        } else {
            assert_eq!(
                callback.failure().map(|failure| failure.to_string()),
                report_with(settlement.clone(), None, Vec::new(), 0)
                    .failure()
                    .map(|failure| failure.to_string())
            );
        }
        assert_eq!(callback.callback_failures().len(), 1);
        assert_eq!(callback.prior_callback_interruptions(), 1);

        let earlier = report_with(settlement.clone(), None, Vec::new(), 2);
        if matches!(settlement, RuntimeShutdownSettlement::Settled) {
            assert!(matches!(
                earlier.failure(),
                Some(RuntimeShutdownFailure::EarlierCallbackInterruptions { count: 2 })
            ));
        } else {
            assert_eq!(
                earlier.failure().map(|failure| failure.to_string()),
                report_with(settlement.clone(), None, Vec::new(), 0)
                    .failure()
                    .map(|failure| failure.to_string())
            );
        }
        assert!(earlier.callback_failures().is_empty());
        assert_eq!(earlier.prior_callback_interruptions(), 2);

        for report in [deadline, callback, earlier] {
            assert_eq!(report.is_success(), report.failure().is_none());
            assert!(!report.is_success());
            assert!(!report.is_cooperatively_stopped());
            assert_eq!(
                report.graceful_timed_out(),
                !matches!(settlement, RuntimeShutdownSettlement::Settled)
            );
            assert_eq!(
                report.abort_timed_out(),
                matches!(settlement, RuntimeShutdownSettlement::AbortTimeout { .. })
            );
            if let RuntimeShutdownSettlement::AbortTimeout { .. } = settlement {
                assert_eq!(report.unjoined().len(), 1);
                assert_eq!(report.unjoined()[0].id, task.id());
                assert!(report.unjoined()[0].abort_requested);
            }
        }
    }
    task.abort();
    assert!(
        task.await
            .expect_err("pending task is cancelled")
            .is_cancelled()
    );
}

#[test]
fn requested_shutdown_completed_loop_diagnostic_does_not_claim_pre_request_timing() {
    let report = RuntimeShutdownReport::new(
        RuntimeShutdownCause::Requested,
        vec![RuntimeLoopRecord {
            task: "completed_after_request",
            result: Ok(RuntimeLoopExit::Completed),
            abort_requested: false,
        }],
        Vec::new(),
        RuntimeShutdownSettlement::Settled,
        RuntimeShutdownObservations::default(),
        Vec::new(),
        0,
    );

    let failure = report
        .failure()
        .expect("completed loop remains a shutdown failure");
    assert_eq!(
        failure.to_string(),
        "jobs runtime loop `completed_after_request` completed unexpectedly"
    );
    assert!(matches!(
        failure,
        RuntimeShutdownFailure::LoopExitedUnexpectedly {
            task: "completed_after_request"
        }
    ));
    assert!(report.is_cooperatively_stopped());
    assert!(!report.is_success());
}

#[tokio::test]
async fn shutdown_failure_debug_redacts_join_panic_payloads() {
    let loop_source = Arc::new(
        tokio::spawn(async { panic!("private loop panic payload") })
            .await
            .expect_err("loop task panics"),
    );
    let descendant_source = Arc::new(
        tokio::spawn(async { panic!("private descendant panic payload") })
            .await
            .expect_err("descendant task panics"),
    );

    for (failure, variant, task, payload) in [
        (
            RuntimeShutdownFailure::LoopJoin {
                task: "worker_loop",
                source: loop_source,
            },
            "LoopJoin",
            "worker_loop",
            "private loop panic payload",
        ),
        (
            RuntimeShutdownFailure::DescendantJoin {
                task: "handler_descendant",
                source: descendant_source,
            },
            "DescendantJoin",
            "handler_descendant",
            "private descendant panic payload",
        ),
    ] {
        let rendered = format!("{failure:?}");
        assert!(rendered.contains(variant));
        assert!(rendered.contains(task));
        assert!(rendered.contains("panicked: true"));
        assert!(!rendered.contains(payload));
    }
}
