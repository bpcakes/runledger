# runledger-runtime agent guide

## Purpose
Generic runtime for durable execution: worker loop, scheduler loop, lease reaper, runtime config, and handler registry.

## Key entrypoints
- `src/lib.rs`: crate API surface.
- `src/worker.rs` and `src/worker/*`: claim/execute/heartbeat loop, completion
  persistence, dead-letter hooks, and observer dispatch.
- `src/scheduler.rs`: schedule claim and enqueue loop.
- `src/reaper.rs` and `src/reaper/*`: stale lease reaper loop, terminal hooks,
  and observer dispatch.
- `src/registry.rs`: handler registry.
- `src/callback.rs`: best-effort callback polling and synchronous future-destruction
  boundary; observers and dead-letter hooks share it.
- `src/settlement.rs` and `src/settlement/`: native descendant registration, shared
  join observation, callback interruption evidence and cleanup eligibility.
- `src/shutdown_signal.rs` and `src/shutdown_signal/owner.rs`: guarded external
  stop input, signal provenance, typed error/panic evidence, and separate
  polling/destruction containment.
- `src/supervisor.rs` and `src/supervisor/`: the worker-process facade, its
  builder and the two terminal report methods.
- `src/task_group.rs` and `src/task_group/report.rs`: loop spawning and the
  single shutdown driver that produces every report.
- `src/config.rs`: runtime configuration.

## Edit here for X
- Worker claim/execute/heartbeat semantics: `src/worker.rs`.
- Worker completion persistence: `src/worker/completion.rs`.
- Reaper terminal hook fanout: `src/reaper/terminal_hooks.rs`.
- Scheduler cadence/jitter logic: `src/scheduler.rs`.
- Lease cleanup runtime behavior: `src/reaper.rs`.
- Handler registration container behavior: `src/registry.rs`.
- Shutdown signal construction, provenance and typed evidence:
  `src/shutdown_signal.rs`; raw-future polling/destruction containment:
  `src/shutdown_signal/owner.rs`.

## Invariants
- Prefer direct cutovers only for internal runtime refactors within one coordinated deploy. Changes that alter runtime-config semantics or behavior relied on by persisted jobs or cross-deploy workers require backward compatibility or an explicit staged rollout; update dependents in the same change.
- Keep runtime orchestration generic; app-specific handlers and catalogs belong outside this crate.
- Worker/scheduler/reaper loops must remain cancellation-safe and shutdown-safe.
- Best-effort callbacks must use the shared callback owner. A polling-only
  `catch_unwind` does not contain future destruction during timeout or task abort.
  Retain every observed interruption; containment does not prove that application
  children stopped or permit dependency cleanup.
- Main job-task and native loop failures remain fatal. `Supervisor` has exactly
  two terminal methods, `run_until_shutdown_report` and `shutdown_report`, and
  both return an independently owned `RuntimeShutdownDriver`; awaiting the
  driver yields a `RuntimeShutdownReport`. Do not add a terminal method whose
  success value could be read as proof that everything settled; dependency
  cleanup is gated on consuming `classify()` and its unforgeable permit.
  `RuntimeSettlement` has distinct opaque `Clean`, `StoppedWithFailures`, and
  `Unsettled` payloads. Only the first two can yield one owned cleanup permit.
  Retained reports are borrowed for diagnostics and cannot be recovered for
  reclassification. Boolean projections are crate-private diagnostics only.
- The terminal owner must produce either final or explicitly interrupted
  evidence even if never polled. Keep collected evidence on that owner across
  cancellation. Waiter destruction requests stop without cancelling settlement;
  report delivery must distinguish consumption from queued-value abandonment.
  Interrupted evidence never authorizes cleanup, even with no known tasks.
- Accept shutdown input only through `RuntimeShutdownSignal`. Poll and destroy
  its future as the tracked `shutdown_signal` descendant on the captured runtime;
  signal polling or destruction panic must remain a descendant join failure
  while native settlement continues. Retain returned typed errors separately
  through `signal_error()`, `SignalFailed`, and `Signal { source }`. A normally
  destroyed and joined returned error denies success but not cleanup. If the
  registry cancels a runtime-authored `ctrl_c` or `pending` signal after another
  shutdown cause, a completed cancellation join is accounted because it proves
  guarded destruction. Cancellation of an application-supplied `fallible` or
  `infallible` signal remains unexpected and denies cleanup; panic, unjoined and
  interrupted evidence also deny cleanup. Only a supervised signal owner may
  poll the raw future. Publish every terminal poll observation (output or caught
  panic) through the same arbitration event, committing evidence, stop clock,
  and initiating identity before guarded destruction. There must be no separate
  record-only polling-panic operation. Retain destruction and join as settlement
  obligations. Structurally identify only the signal that won first-cause
  arbitration so graceful escalation cannot abort it; preserve earlier causes.
  Custom signal futures must be cancellation-safe: dropping them must not leave
  detached application children. Error formatting must redact source details.
  Guard the raw signal future from construction through destruction: catch a
  borrowed poll before dropping it, and take the future before attempting Drop.
  A task-level catch alone cannot contain combined poll/Drop panic. Preserve
  both phases through signal_panic(); unsubmitted destruction has no report
  recipient and emits only a redacted diagnostic. Keep fatal signal policy
  distinct from best-effort callback policy.
- Keep durable result classification separate from execution-interruption
  evidence. A completed handler rejected by deadline or lease fencing was not
  cancelled. Record interruption at the pending-future cancellation or panic
  boundary; test both durable outcomes and cleanup eligibility together.
- Keep ordinary descendant collection driven by ready notifications. Shutdown
  boundary checks also inspect finished handles once: a delayed notification or
  concurrent collector must not hide an already-completed join. Do not replace
  this bounded check with an unbounded drain of newly arriving notifications.

## Common commands
- `cargo check -p runledger-runtime`
- `cargo test -p runledger-runtime`
- `cargo clippy -p runledger-runtime --all-targets -- -D warnings`
