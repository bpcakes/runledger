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
- `src/config.rs`: runtime configuration.

## Edit here for X
- Worker claim/execute/heartbeat semantics: `src/worker.rs`.
- Worker completion persistence: `src/worker/completion.rs`.
- Reaper terminal hook fanout: `src/reaper/terminal_hooks.rs`.
- Scheduler cadence/jitter logic: `src/scheduler.rs`.
- Lease cleanup runtime behavior: `src/reaper.rs`.
- Handler registration container behavior: `src/registry.rs`.

## Invariants
- Prefer direct cutovers only for internal runtime refactors within one coordinated deploy. Changes that alter runtime-config semantics or behavior relied on by persisted jobs or cross-deploy workers require backward compatibility or an explicit staged rollout; update dependents in the same change.
- Keep runtime orchestration generic; app-specific handlers and catalogs belong outside this crate.
- Worker/scheduler/reaper loops must remain cancellation-safe and shutdown-safe.
- Best-effort callbacks must use the shared callback owner. A polling-only
  `catch_unwind` does not contain future destruction during timeout or task abort.
  Retain every observed interruption; containment does not prove that application
  children stopped or permit dependency cleanup.
- Main job-task and native loop failures remain fatal. Use the complete shutdown
  report for dependency-cleanup decisions; legacy Result methods observe loops.
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
