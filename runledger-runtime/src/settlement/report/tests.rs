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
