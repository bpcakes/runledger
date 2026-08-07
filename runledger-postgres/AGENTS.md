# runledger-postgres agent guide

## Purpose
PostgreSQL persistence for durable execution: queue lifecycle, workflow DAG state machine, schedules, runtime configs, and logs.

## Key entrypoints
- `src/lib.rs`: crate API and shared DB error/result types.
- `src/jobs.rs`: public jobs/workflow DB API exports.
- `src/jobs/queue/*`: enqueue/claim/lifecycle/reaper paths.
- `src/jobs/workflows/*`: workflow run/step persistence and dependency resolution.
- `src/jobs/runtime_configs.rs`: per-job runtime config persistence.
- `src/jobs/logs.rs`: job log persistence.

## Edit here for X
- Job enqueue/claim/retry/dead-letter behavior: `src/jobs/queue/*`.
- Workflow runtime/dependency transitions: `src/jobs/workflows/runtime/*`.
- Workflow run creation/read APIs: `src/jobs/workflows/enqueue.rs`, `src/jobs/workflows/read.rs`.
- DB error categorization: `src/error.rs`, `src/error/classify/*`.

## Invariants
- Prefer direct cutovers only for internal persistence-layer refactors within one coordinated deploy. Changes to persisted queue, workflow, runtime-config, or log/event contracts require backward compatibility or an explicit staged rollout; update dependents in the same change.
- Preserve audit/event writes when altering queue lifecycle.
- Keep app/domain logic out of this crate.

## Common commands
- `cargo check -p runledger-postgres`
- `cargo test -p runledger-postgres`
- `cargo clippy -p runledger-postgres --all-targets -- -D warnings`
