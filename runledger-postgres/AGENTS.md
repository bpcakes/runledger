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
- Scoped cancellation preserves original SQLx begin/commit errors. Explicit
  rollback failure retains the operation error and rollback error together;
  neither default formatting nor automatic logging prints their contents. A
  failed commit does not establish whether cancellation happened or permit replay.
- Map every owned `tx.commit()` error with `Error::commit_unconfirmed(operation, error)`.
  Never use `ConnectionError` or `from_query_sqlx*` there: an unconfirmed commit is
  an unknown outcome, and SQLSTATE classification must not absorb it. `operation`
  is fixed text with no request data.

- Opaque durable-intent recording and native transaction callers share the same
  READ COMMITTED witness and SQL implementation. Do not add a second intent
  implementation or expose raw SQLx identity through the capability path.
- `PgTransactionExecutor` is sealed to native SQLx transactions and
  `PgTransactionView`. Never accept downstream executor providers as transaction
  evidence. Views must borrow actual native resources with private fields;
  READ COMMITTED validation stays tied to that retained transaction.
- `PgSessionView` consumes one connection borrow for the complete native schema
  check. Do not restore routing executors or per-query provider selection.
  Ownership and cancellation disposition stay with the caller's adapter.

## Common commands
- `cargo check -p runledger-postgres`
- `cargo test -p runledger-postgres`
- `cargo clippy -p runledger-postgres --all-targets -- -D warnings`
