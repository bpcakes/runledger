# runledger-core agent guide

## Purpose
Shared durable-execution contracts: handler traits, job/workflow enums, runtime types, and workflow enqueue/build validation.

## Key entrypoints
- `src/lib.rs`: crate API surface.
- `src/jobs.rs`: public jobs/workflow exports.
- `src/jobs/handler.rs`: handler traits and registry trait.
- `src/jobs/runtime_types.rs`: `JobContext`, `JobFailure`, `JobProgress`.
- `src/jobs/status.rs`: persisted status/event enums.
- `src/jobs/workflow_enqueue/*`: workflow enqueue builders and DAG validation.

## Edit here for X
- Shared job/domain enums or constants: `src/jobs/status.rs`.
- Handler trait contracts: `src/jobs/handler.rs`.
- Workflow enqueue shapes/builders/validation: `src/jobs/workflow_enqueue/*`.

## Invariants
- Prefer direct cutovers only for internal code-only refactors within one coordinated deploy. Changes to durable job/workflow contracts, enums, or payloads that can outlive a deploy require backward compatibility or an explicit staged rollout; update dependents in the same change.
- Keep this crate free of storage, transport, and app-specific logic.
- Prefer explicit validation errors over implicit panics.

## Common commands
- `cargo check -p runledger-core`
- `cargo test -p runledger-core`
- `cargo clippy -p runledger-core --all-targets -- -D warnings`
