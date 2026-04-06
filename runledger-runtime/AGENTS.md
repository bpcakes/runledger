# runledger-runtime agent guide

## Purpose
Generic runtime for durable execution: worker loop, scheduler loop, lease reaper, runtime config, and handler registry.

## Key entrypoints
- `src/lib.rs`: crate API surface.
- `src/worker.rs`: claim/execute/heartbeat loop.
- `src/scheduler.rs`: schedule claim and enqueue loop.
- `src/reaper.rs`: stale lease reaper loop.
- `src/registry.rs`: handler registry.
- `src/config.rs`: runtime configuration.

## Edit here for X
- Worker lifecycle/heartbeat semantics: `src/worker.rs`.
- Scheduler cadence/jitter logic: `src/scheduler.rs`.
- Lease cleanup runtime behavior: `src/reaper.rs`.
- Handler registration container behavior: `src/registry.rs`.

## Invariants
- Prefer direct cutovers only for internal runtime refactors within one coordinated deploy. Changes that alter runtime-config semantics or behavior relied on by persisted jobs or cross-deploy workers require backward compatibility or an explicit staged rollout; update dependents in the same change.
- Keep runtime orchestration generic; app-specific handlers and catalogs belong outside this crate.
- Worker/scheduler/reaper loops must remain cancellation-safe and shutdown-safe.

## Common commands
- `cargo check -p runledger-runtime`
- `cargo test -p runledger-runtime`
- `cargo clippy -p runledger-runtime --all-targets -- -D warnings`
