# Settlement review follow-up

The cleanup finding was a boundary mistake: the handler result mapper both
classified durable outcomes and recorded execution interruption. A late return
therefore became a permanent cleanup failure. The same mistake appeared in the
completed-handler branch when a progress write had reported lease loss.

The shutdown-budget finding was a smaller diagnostic design mismatch. An error
with one `timeout` field was reused for failure to add two durations, even though
neither input alone describes the failed operation.

## Contract and research

Rust defines `Poll::Ready` as completed execution. Tokio documents that a
non-yielding future can complete after its timeout. Runledger deliberately
rejects results observed at or after its handler deadline, as specified by
`JobExecution::deadline`; this is a durable outcome policy. It does not mean the
future was cancelled. See [Rust Future](https://doc.rust-lang.org/std/future/trait.Future.html)
and [Tokio timeout](https://docs.rs/tokio/latest/tokio/time/fn.timeout.html).

The native report remains conservative about actual interruption: timeout or
lease maintenance cancelling a pending handler, panic, and task abortion all
prevent cleanup. Those events cannot establish that application children
stopped. Normal return relies on the handler settling its own children; this
change does not claim ownership of arbitrary detached application tasks.

## Applied fix and prevention

- The durable handler result mapper no longer receives a settlement registry.
  Panic evidence is recorded at the execution boundary before outcome mapping;
  pending-handler cancellation records remain at their cancellation branches.
  Completed lease-loss results retain fencing without claiming cancellation.
- `ShutdownBudgetOverflow` retains both input durations. A representable total
  that exceeds the instant range still reports that actual total through
  `ShutdownTimeoutTooLarge`. The containing public error enum is non-exhaustive.
- Integration tests jointly assert durable outcome and cleanup eligibility for
  late success, business failure, continuation, lease loss, and panic. The five
  tests reproduced the old behavior on PostgreSQL 18.6
  (Debian 18.6-1.pgdg13+2) before the production fix.
- Budget tests cover subsecond carry, both operand positions, representable
  totals outside the instant range, and valid zero/nonzero phases.
- The runtime agent guide and consumer documentation now state the distinction
  so future outcome policies do not silently acquire settlement side effects.

The durable long-term boundary is execution ownership versus result acceptance.
A broader application-child registry would require an explicit ownership API
and adoption by handlers; inferring it from timeout codes or expiring historical
interruption counts would not establish safe cleanup. No such inference is
needed for these fixes.

## Validation

- `cargo test -p runledger-runtime --lib --offline`: 257 passed.
- `cargo test -p runledger-runtime --test supervisor_loop --offline`: 15 passed,
  including the five new regressions and existing actual-interruption cases.
- `cargo clippy -p runledger-runtime --all-targets --offline -- -D warnings`: passed.
- `cargo fmt --all -- --check` and `git diff --check`: passed.
