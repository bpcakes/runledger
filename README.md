# Runledger

Runledger is a standalone Rust workspace for durable job execution and workflow orchestration on PostgreSQL.

This repository was extracted from a larger application and scoped down to the Runledger-specific crates, migrations, and test utilities needed to build and evolve the job system independently.

## Workspace

The workspace contains four crates:

- `runledger-core`
  Storage-agnostic contracts: job handler traits, runtime types, statuses, identifiers, and workflow enqueue/build validation.
- `runledger-postgres`
  SQLx-backed PostgreSQL persistence for the queue, job lifecycle, schedules, workflow DAG state machine, runtime configs, logs, and admin reads/mutations.
- `runledger-runtime`
  Async worker, scheduler, and reaper loops plus runtime configuration and handler registry.
- `runledger-test-support`
  Local test-only helpers for ephemeral PostgreSQL databases and scoped environment-variable overrides.

The root workspace manifest is [Cargo.toml](/Users/aa/Documents/runledger/Cargo.toml).

## What This Repo Includes

- Rust crates for the Runledger contracts, runtime, and PostgreSQL persistence layer
- A Runledger-only SQL migration history in [migrations](/Users/aa/Documents/runledger/migrations)
- Local test support for DB-backed tests using `testcontainers`
- SQLx offline metadata in `.sqlx/` so the macro-based queries compile without a live database during normal builds

## What This Repo Does Not Include

- Application-specific handlers
- API servers, CLIs, or binaries
- Non-Runledger product schema from the original application
- Domain models owned by a larger app

You are expected to embed these crates inside your own service and supply:

- concrete job handlers
- process bootstrapping
- database provisioning
- application-level auth/admin surfaces

## Crate Responsibilities

### `runledger-core`

Use `runledger-core` for the public contracts shared across the rest of the workspace:

- `JobHandler` and `JobHandlerRegistry`
- `JobContext`, `JobProgress`, and `JobFailure`
- job status and event enums
- workflow enqueue builders and DAG validation

This crate intentionally has no persistence or async loop logic.

### `runledger-postgres`

Use `runledger-postgres` when you need durable state in PostgreSQL.

Key capabilities:

- enqueue, claim, heartbeat, retry, succeed, cancel, dead-letter, and requeue jobs
- materialize and update cron schedules
- persist job logs and runtime configs
- create, read, mutate, and advance workflow runs and steps
- query operator/admin views over queue and workflow state

The crate assumes the matching Runledger schema has already been migrated into the target database.

### `runledger-runtime`

Use `runledger-runtime` to run the operational loops around the storage layer:

- `worker::run_worker_loop`
- `scheduler::run_scheduler_loop`
- `reaper::run_reaper_loop`
- `registry::JobRegistry`
- `config::JobsConfig`

The runtime is generic. It does not know about your application-specific job catalog beyond the handlers you register.

### `runledger-test-support`

This crate exists only to support tests inside the workspace.

It provides:

- `setup_ephemeral_pool`
- `teardown_ephemeral_pool`
- `ScopedEnv`

It starts a disposable PostgreSQL container, creates per-test databases, and runs the local Runledger migrations against them.

## Database Model

The standalone schema is intentionally limited to Runledger-owned objects.

Major schema areas:

- queue and lifecycle tables
  `job_definitions`, `job_queue`, `job_attempts`, `job_events`, `job_dead_letters`, `job_schedules`
- workflow orchestration tables
  `workflow_runs`, `workflow_steps`, `workflow_step_dependencies`, `workflow_run_mutations`
- operational support tables
  `job_logs`, `job_runtime_configs`
- derived operational view
  `job_metrics_rollup`

Notable schema features:

- idempotent queueing via `idempotency_key`
- cron-backed schedule materialization
- workflow DAG execution with dependency counters
- external workflow gates via `WAITING_FOR_EXTERNAL`
- append-only workflow mutation tracking
- panic-aware job metrics rollups

## Schema Scope Difference From The Original App

This repository no longer ships the original product schema.

A few columns remain for integration flexibility, but their original foreign keys were intentionally removed in the standalone migration set:

- `organization_id`
- `created_by_user_id`
- `updated_by_user_id`

These values are now treated as opaque UUIDs from the perspective of Runledger. If your host application wants referential integrity, it should add that in its own schema layer or wrap these migrations with app-owned extensions.

## Migrations

The migration set lives in [migrations](/Users/aa/Documents/runledger/migrations).

This repo now uses a single flattened baseline migration:

- `202603280001_runledger_baseline`
  creates the full current Runledger schema directly, including:
  helper functions, queue tables, workflow DAG tables, logs, runtime configs, workflow mutations, external workflow gates, panic-aware attempt outcomes, and the final metrics rollup view

The historical standalone migration chain was intentionally collapsed because this repository now targets fresh standalone deployments rather than preserving every intermediate extraction-era cutover step.

If you already created databases from the older multi-file standalone migration history, treat this baseline as a new-from-scratch schema definition, not as an in-place upgrade path.

Apply these migrations before using `runledger-postgres` or running DB-backed tests.

## Runtime Configuration

`runledger-runtime` exposes `JobsConfig::from_env()` in [runledger-runtime/src/config.rs](/Users/aa/Documents/runledger/runledger-runtime/src/config.rs).

Supported environment variables:

- `JOBS_WORKER_ID`
- `JOBS_POLL_INTERVAL_MS`
- `JOBS_CLAIM_BATCH_SIZE`
- `JOBS_LEASE_TTL_SECONDS`
- `JOBS_MAX_GLOBAL_CONCURRENCY`
- `JOBS_REAPER_INTERVAL_SECONDS`
- `JOBS_SCHEDULE_POLL_INTERVAL_SECONDS`
- `JOBS_REAPER_RETRY_DELAY_MS`

Default behavior:

- blank `JOBS_WORKER_ID` falls back to `worker-<uuidv7>`
- interval and concurrency values are clamped to safe minimums
- lease TTL is clamped to at least `10` seconds

## Building

Common commands:

```bash
cargo check
cargo test --workspace --no-run
cargo test -p runledger-core
cargo test -p runledger-postgres
cargo test -p runledger-runtime
```

The standalone workspace has been validated with:

```bash
cargo check
cargo test --workspace --no-run
```

## SQLx Offline Mode

This repo uses `sqlx::query!` and related macros extensively.

To keep normal builds self-contained:

- `.cargo/config.toml` sets `SQLX_OFFLINE=true`
- `.sqlx/` contains the cached query metadata used by SQLx macro expansion

If you change SQL queries or the schema, refresh the cache before committing.

Typical workflow:

1. bring up a PostgreSQL database with the current Runledger migrations applied
2. point `DATABASE_URL` at that database
3. run `cargo sqlx prepare --workspace`

If the cache and schema drift apart, `cargo check` will fail during macro expansion.

## Testing

There are two main categories of tests:

- pure Rust unit tests
  these do not require PostgreSQL
- DB-backed tests
  these use `runledger-test-support` and `testcontainers`

The DB-backed tests:

- start a shared PostgreSQL container
- create isolated ephemeral databases per test
- apply the local Runledger migrations

The default test image is `postgres:18`.

Override it with:

```bash
export RUNLEDGER_TEST_PG_IMAGE=postgres:18
```

The test harness expects the database image to support `uuidv7()`.

## PostgreSQL Assumptions

Runledger expects PostgreSQL semantics and features consistent with the migration set and SQLx queries in this repo.

In particular:

- `uuidv7()` must be available
- transactional DDL behavior must support the baseline migration as written
- the target DB must be migrated before runtime code uses it

## Typical Integration Shape

A host application will generally:

1. apply the Runledger migrations to its PostgreSQL database
2. create a shared `sqlx::PgPool`
3. register concrete handlers in `runledger_runtime::registry::JobRegistry`
4. start worker, scheduler, and reaper loops with coordinated shutdown
5. call `runledger_postgres::jobs::*` APIs from its own admin/API surfaces

At a high level:

```rust
use runledger_runtime::config::JobsConfig;
use runledger_runtime::registry::JobRegistry;

let pool = /* sqlx PgPool */;
let mut registry = JobRegistry::new();
// registry.register(MyHandler);

let config = JobsConfig::from_env();
// spawn worker/scheduler/reaper loops with the shared pool and registry
```

This workspace deliberately stops at the library boundary; it does not prescribe your process model or handler packaging.

## Repository Layout

```text
.
├── Cargo.toml
├── README.md
├── migrations/
├── runledger-core/
├── runledger-postgres/
├── runledger-runtime/
└── runledger-test-support/
```

## Development Notes

- Prefer keeping contracts in `runledger-core`, runtime orchestration in `runledger-runtime`, and SQL/state-machine logic in `runledger-postgres`.
- Treat the migration set as the canonical persisted contract for queue and workflow behavior.
- When schema semantics change, update Rust types, SQL, tests, and `.sqlx` metadata together.
- The repo may compile offline, but DB-backed behavior still needs migration-compatible PostgreSQL for execution.

## License

No license file is included in this extraction. Add one at the repository root if this workspace is intended for redistribution or open-source use.
