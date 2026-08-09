# runledger-postgres agent guide

## Purpose
PostgreSQL persistence for durable execution: queue lifecycle, workflow DAG state machine, schedules, runtime configs, and logs.

## Key entrypoints
- `src/lib.rs`: crate API and shared DB error/result types.
- `src/jobs.rs`: public jobs/workflow DB API exports.
- `src/jobs/queue/{enqueue,claim,lifecycle,reaper}.rs` and
  `src/jobs/queue/lifecycle/*`: queue persistence paths.
- `src/jobs/workflows/*`: workflow run/step persistence and dependency resolution.
- `src/jobs/admin/*`: admin reads, payload mutation, metrics, and direct-job recovery.
- `src/jobs/runtime_configs.rs`: per-job runtime config persistence.
- `src/jobs/logs.rs`: job log persistence.

## Edit here for X
- Job enqueue/claim behavior: `src/jobs/queue/{enqueue,claim}.rs`.
- Heartbeat/progress/completion behavior: `src/jobs/queue/lifecycle/*`.
- Retry/dead-letter and lease cleanup behavior: `src/jobs/queue/lifecycle/failure.rs`,
  `src/jobs/queue/reaper.rs`.
- Workflow runtime/dependency transitions: `src/jobs/workflows/runtime/*`.
- Workflow snapshot encoding/decoding: `src/jobs/workflows/snapshot.rs`.
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
