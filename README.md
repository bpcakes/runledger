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
  Published test utilities for ephemeral PostgreSQL databases and scoped environment-variable overrides.

The root workspace manifest is [Cargo.toml](Cargo.toml).

## What This Repo Includes

- Rust crates for the Runledger contracts, runtime, and PostgreSQL persistence layer
- A Runledger-only SQL migration history in [migrations](migrations)
- Vendored copies of those migrations in [runledger-postgres/migrations](runledger-postgres/migrations) and [runledger-test-support/migrations](runledger-test-support/migrations) so packaged crates can apply schemas without relying on repo-relative paths
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

## Choosing The API

Use the highest-level Runledger API that matches the shape of the work. This is
especially important for agents and generated integrations: a workflow DAG is a
first-class Runledger feature, not something consumers should recreate by
polling jobs or chaining handlers manually.

For a shorter prompt-facing version, see
[llms.txt](https://github.com/featherenvy/runledger/blob/master/llms.txt).
For a slightly longer guide, see
[docs/downstream-agent-guide.md](https://github.com/featherenvy/runledger/blob/master/docs/downstream-agent-guide.md).

Common integration imports:

```rust
use runledger_core::prelude::*;
use runledger_postgres::prelude::*;
use runledger_runtime::prelude::*;
```

| Need | Prefer |
| --- | --- |
| One independent retried unit of work | `runledger_postgres::jobs::enqueue_job` |
| Multi-step work with dependencies | `WorkflowDagBuilder` (simple DAGs) or `WorkflowRunEnqueueBuilder` / `WorkflowStepEnqueueBuilder` (advanced), then `enqueue_workflow_run` |
| Fan-out, fan-in, or ordered stages | `WorkflowDagBuilder::after_success` / `after_terminal`, or lower-level `depends_on_success` / `depends_on_terminal` |
| Human/API approval or another external gate | External workflow steps and `complete_external_workflow_step` |
| Delayed or recurring entrypoint | `JobScheduleUpsert` and `upsert_job_schedule` |
| Worker process lifecycle | `runledger_runtime::Supervisor::run_until_shutdown` |
| Admin/status views | `runledger_postgres::jobs` read/list APIs |

Avoid manual workflow orchestration unless you are intentionally building a
custom orchestrator outside Runledger. For ordinary dependent work, do not poll
`get_job_by_id` in a loop, enqueue dependent jobs from parent handlers, encode
dependency state in job payload JSON, or add app-owned tables to track workflow
edges. Model the run as a workflow DAG instead.

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
- create, materialize, and update cron schedules
- persist job logs and runtime configs
- create, read, mutate, and advance workflow runs and steps
- query operator/admin views over queue and workflow state

The crate assumes the matching Runledger schema has already been migrated into the target database.

For consumer setup there are two supported modes:

- call `runledger_postgres::migrate_after_idempotency_cutover(&pool)` to apply the bundled schema during startup and reject keyed legacy rows without enqueue snapshots
- call `runledger_postgres::ensure_schema_compatible_after_idempotency_cutover(&pool)` to perform a read-only validation that an existing `_sqlx_migrations` history matches the bundled migrations, with explicit errors for missing history, incompatible history, legacy idempotency rows, or PostgreSQL query/connectivity failures

Operational API notes:

- `QueryError::Display` and `Debug` are safe for public surfaces and omit internal database context; use `QueryError::internal_message()` for server-side diagnostics.
- Worker lifecycle updates reject expired leases with the stable `job.lease_owner_mismatch` code, even when the lease was lost by time rather than by another worker; once `lease_expires_at` has passed there is no owner grace period.
- `complete_job_success` persists `JobStage::Completed`; passing any other success stage is rejected as a caller error.
- Workflow-backed job completion waits for an in-flight workflow cancellation to commit or roll back instead of returning a transient `workflow.release_conflict`; append and external-step release paths may still return `workflow.release_conflict` while cancellation owns the exclusive release lock.
- Retry conflicts such as `workflow.append_conflicting_retry` are reported as conflict-category query errors; clients should prefer stable error codes over broad categories for exact branching.
- Release-sensitive workflow operations, workflow append mutations, and keyed enqueue retries require PostgreSQL `READ COMMITTED` semantics. PostgreSQL's `READ UNCOMMITTED` mode is accepted because PostgreSQL implements it as read committed.
- Keyed rows created before enqueue snapshots existed cannot be safely reconstructed. The idempotency cutover rejects keyed job and workflow rows with `enqueue_request IS NULL` during startup/schema validation, and keyed retries against such rows return dedicated conflict errors instead of falling back to mutable state comparisons.

### `runledger-runtime`

Use `runledger-runtime` to run the operational loops around the storage layer:

- `Supervisor`
- `catalog::JobCatalog` for single-source handler registration, definition sync, and validated enqueue helpers
- `registry::JobRegistry` for advanced setups that manage handlers separately from definitions
- `config::JobsConfig`

The runtime is generic. It does not embed application-specific job lists; applications build their own [`JobCatalog`](runledger-runtime/src/catalog/mod.rs) at startup. `Supervisor` is the preferred facade for worker processes; `worker::run_worker_loop`, `scheduler::run_scheduler_loop`, and `reaper::run_reaper_loop` remain available as low-level building blocks for custom orchestration. Those low-level loops return `RuntimeLoopExit`; custom orchestrators that type their join handles explicitly should use `JoinHandle<RuntimeLoopExit>`.

### `runledger-test-support`

This crate provides shared testing utilities for Runledger crates and downstream integration tests.

It provides:

- `setup_ephemeral_pool`
- `teardown_ephemeral_pool`
- `ScopedEnv`

It starts a disposable PostgreSQL container, creates per-test databases, and runs its vendored Runledger migrations against them. It is published so package tests in `runledger-postgres` can depend on the same harness that workspace tests use.

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

The migration set lives in [migrations](migrations).

This repo uses a flattened baseline plus forward migrations:

- `202603280001_runledger_baseline`
  creates the standalone Runledger schema baseline, including:
  helper functions, queue tables, workflow DAG tables, logs, runtime configs, workflow mutations, external workflow gates, panic-aware attempt outcomes, and the final metrics rollup view
- `202604100001_runledger_migration_history`
  creates `runledger_migration_history` and records the standalone baseline and history-table migration versions
- `202605180001_add_enqueue_request_snapshots`
  adds `enqueue_request` snapshots to `job_queue` and `workflow_runs` so keyed enqueue retries can compare the original request instead of mutable runtime state
- `202605220001_enforce_enqueue_request_snapshots`
  blocks new keyed queue/workflow rows without snapshots while startup validation rejects pre-cutover legacy rows; the application migration API validates the cutover constraints after legacy-row validation passes

The historical standalone migration chain was intentionally collapsed because this repository now targets fresh standalone deployments rather than preserving every intermediate extraction-era cutover step.

If you already created databases from the older multi-file standalone migration history, treat the flattened baseline as a new-from-scratch schema definition, not as an in-place upgrade path. Apply later forward migrations normally.

The workspace-root migration directory remains the canonical schema source for repo development and review.

For consumers using the published crate:

- `runledger_postgres::MIGRATOR` embeds the vendored `runledger-postgres/migrations/` copy
- `runledger-test-support` embeds its own `runledger-test-support/migrations/` copy for packaged test harnesses
- `runledger_postgres::migrate_after_idempotency_cutover(&pool)` applies those migrations and rejects keyed legacy rows without snapshots
- `runledger_postgres::ensure_schema_compatible_after_idempotency_cutover(&pool)` validates that an existing `_sqlx_migrations` history matches them without running DDL and returns Runledger-specific errors for missing history, incompatible history, legacy idempotency rows, or PostgreSQL query/connectivity failures; externally managed DDL can validate the `NOT VALID` cutover constraints after this check passes
- `runledger-postgres/build.rs` fails local builds if the vendored crate copy drifts from the canonical workspace-root `migrations/` directory

Apply these migrations, or call `runledger_postgres::migrate_after_idempotency_cutover(&pool)`, before using `runledger-postgres` or running DB-backed tests.

For the enqueue-request snapshot cutover, apply the bundled migrations first,
then run either startup API. If it returns
`SchemaCompatibilityError::LegacyIdempotencySnapshotsMissing`, inspect the
legacy rows with the `idx_job_queue_missing_enqueue_request_snapshot` and
`idx_workflow_runs_missing_enqueue_request_snapshot` partial indexes, remediate
or drain those keyed rows, and retry startup. Prefer natural drain or clearing
the stale `idempotency_key` where retry identity no longer matters; only backfill
`enqueue_request` when you have the original canonical enqueue request, not from
mutable live queue/workflow state. `migrate_after_idempotency_cutover` validates
the cutover constraints once no legacy rows remain; that first validation scans
`job_queue` and `workflow_runs` and may briefly delay startup on large tables
without blocking ordinary DML. The cutover migration also builds helper indexes
for locating legacy rows; on large tables, apply it during a maintenance window
appropriate for your write volume.

## Runtime Configuration

`runledger-runtime` exposes `JobsConfig::from_env()` in [runledger-runtime/src/config.rs](runledger-runtime/src/config.rs).

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
./scripts/run-external-consumer-smoke.sh
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
- the workspace-root `.sqlx/` directory is the source cache generated by `cargo sqlx prepare --workspace`
- each publishable crate that uses SQLx checked macros also carries its own `.sqlx/` directory so `cargo publish` can verify the packaged tarball in isolation

If you change SQL queries or the schema, refresh the cache before committing.

Typical workflow:

1. bring up a PostgreSQL database with the current Runledger migrations applied
2. point `DATABASE_URL` at that database
3. run `./scripts/refresh-sqlx-cache.sh`

What the script does:

- regenerates the workspace root `.sqlx/` cache
- syncs that cache into `runledger-postgres/.sqlx/` and `runledger-runtime/.sqlx/`
- syncs the workspace-root `migrations/` directory into `runledger-postgres/migrations/`
- runs `cargo check --workspace`
- confirms the publishable crate tarballs include their per-crate SQLx cache

Do not update only the workspace root `.sqlx/` directory. `cargo publish` verifies each crate from its packaged tarball, so publishable crates must include their own SQLx cache.

If the cache and schema drift apart, `cargo check` will fail during macro expansion.

## Publishing

Prepare a release with the repository script:

```bash
./scripts/prepare-release.sh 0.3.0
```

The preparation script:

- requires a clean working tree
- bumps publishable crate versions and root workspace dependency versions
- refreshes SQLx offline metadata
- runs workspace tests and the packaged external-consumer smoke test
- runs a publish dry-run for `runledger-core` and packages the dependent crates locally

Before publishing this release line, call out observable contract changes in release notes:

- published crates require Rust 1.88+
- `runledger-runtime` adds `Supervisor`
- low-level runtime loops now return `RuntimeLoopExit`
- `runledger-postgres` adds `JobScheduleUpsert`, `upsert_job_schedule`, `set_job_schedule_active`, and `set_job_schedule_next_fire_at`; conflict updates refresh the schedule definition while preserving `is_active` and `organization_id`, refresh `next_fire_at` when cron syntax changes, and validate cron syntax plus name/jitter bounds
- low-level `runledger-postgres::jobs::mark_schedule_fired_tx` now returns `Result<bool>` so runtime internals can distinguish a successful cursor advance from a missing schedule row
- `JobScheduleRecord` exposes `is_active` so setup code can observe preserved pause/resume state after schedule upserts
- schedules are UTC-only; schedule upserts store `timezone = 'UTC'`, and accepted cron expressions use the same parser as `runledger-runtime`
- `QueryError::Display` now returns client-safe messages
- expired leases have no owner grace period for heartbeat/progress/success/failure writes
- the `job.lease_owner_mismatch` message now covers time-based loss of ownership
- success completion rejects non-`Completed` stages
- workflow-backed job completion waits on in-flight cancellation instead of returning `workflow.release_conflict`
- append/external release can still return `workflow.release_conflict`
- workflow append mutations require read-committed transaction isolation
- idempotent enqueue adds new conflict/isolation error codes
- `workflow.append_conflicting_retry` is now a conflict-category error

If publishing manually, run `./scripts/refresh-sqlx-cache.sh` before publishing `runledger-postgres` or `runledger-runtime` and commit any resulting `.sqlx/` changes.

After reviewing and committing the prepared diff, publish with:

```bash
./scripts/publish-release.sh 0.3.0
```

The publish script publishes crates in dependency order, dry-runs each crate once its workspace dependencies are indexed, creates a `v0.3.0` tag, and pushes the current branch and tag. Set `PUBLISH_REMOTE` to override the git remote used for the final push.

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

The packaged external-consumer smoke test:

- packages `runledger-core`, `runledger-postgres`, and `runledger-runtime`
- extracts those `.crate` archives locally
- builds a standalone host crate against the packaged manifests via `[patch.crates-io]`
- runs migrations, starts the runtime supervisor, enqueues jobs, and asserts terminal states

Run it with:

```bash
./scripts/run-external-consumer-smoke.sh
```

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

1. either call `runledger_postgres::migrate_after_idempotency_cutover(&pool)` or apply the Runledger migrations with your own deployment tooling and then call `runledger_postgres::ensure_schema_compatible_after_idempotency_cutover(&pool)`
2. create a shared `sqlx::PgPool`
3. register concrete handlers in a `runledger_runtime::catalog::JobCatalog` (or directly in `runledger_runtime::registry::JobRegistry` for advanced setups)
4. start `runledger_runtime::Supervisor` in a worker process
5. call `runledger_postgres::jobs::*` APIs from its own admin/API surfaces

At a high level:

```rust
use std::time::Duration;

use runledger_runtime::Supervisor;
use runledger_runtime::catalog::JobCatalog;
use runledger_runtime::config::JobsConfig;

let pool = /* sqlx PgPool */;
runledger_postgres::migrate_after_idempotency_cutover(&pool).await?;

let catalog = JobCatalog::new()
    .job("jobs.example", MyHandler);

catalog.sync_definitions(&pool).await?;

let config = JobsConfig::from_env();
let supervisor = Supervisor::builder(&pool, config)?
    .with_catalog(&catalog)
    .build()?;
supervisor
    .run_until_shutdown(
        async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                eprintln!("failed to listen for shutdown signal: {error}");
            }
        },
        Duration::from_secs(30),
    )
    .await?;
```

Production worker binaries should still close their pool after supervisor
shutdown; the worker example below keeps cleanup independent from shutdown
errors.

See [runledger-runtime/examples/worker_binary.rs](https://github.com/featherenvy/runledger/blob/master/runledger-runtime/examples/worker_binary.rs)
for a compile-checked worker binary skeleton.

This workspace deliberately stops at the library boundary; it does not prescribe your process model or handler packaging.

## Workflow DAG Recipe

When work has dependencies, model those dependencies directly in the workflow
enqueue request. The workflow engine persists the run, validates the DAG,
enqueues root steps, releases dependents when prerequisites finish, and keeps the
run status coherent with cancellation and external gates.

```rust
use runledger_core::jobs::WorkflowDagBuilder;

let metadata = serde_json::json!({"source": "api"});
let crawl_payload = serde_json::json!({"profile_id": "p_123"});
let classify_payload = serde_json::json!({"profile_id": "p_123"});
let score_payload = serde_json::json!({"profile_id": "p_123"});
let persist_payload = serde_json::json!({"profile_id": "p_123"});

let run = WorkflowDagBuilder::new("profiles.research", &metadata)
    .idempotency_key("profile:p_123:research")
    .job("crawl", "profiles.crawl", &crawl_payload)?
    .job("classify", "profiles.classify", &classify_payload)?
    .after_success("classify", ["crawl"])?
    .job("score", "profiles.score", &score_payload)?
    .after_success("score", ["crawl"])?
    .job("persist", "profiles.persist", &persist_payload)?
    .after_success("persist", ["classify", "score"])?
    .build()?;

let workflow_run = runledger_postgres::jobs::enqueue_workflow_run(&pool, &run).await?;
```

`WorkflowDagBuilder` accepts raw string identifiers for readable call sites. It
validates the workflow shape before enqueueing, but it does not prove at compile
time that a job type has a registered job definition or runtime handler. Use
`WorkflowRunEnqueueBuilder` and `WorkflowStepEnqueueBuilder` when you need
per-step priority, attempts, timeout, stage, external steps, hand-authored
dependency specs, or call sites that pass explicit `StepKey` and `JobType`
values.

Validation timing:

| Call | Fails immediately | Deferred until `.build()` / `.try_build()` |
| --- | --- | --- |
| `WorkflowDagBuilder::new(...)` | never | blank workflow type |
| `WorkflowDagBuilder::try_new(...)` | blank workflow type | empty step list and dependency graph errors |
| `.job(step, job_type, payload)` | blank step key, blank job type, duplicate step key | job type registration is not checked by this builder |
| `.after_success(step, prerequisites)` / `.after_terminal(...)` | blank target step key, blank prerequisite step key, unknown target step | missing prerequisite step, self-dependency, duplicate dependency, cycle |
| `.idempotency_key(...)` | never | blank idempotency key |

The target of `.after_success(...)` or `.after_terminal(...)` must already have
been added with `.job(...)`. Prerequisite steps may be added later in the chain,
as long as every referenced step exists before `.build()` succeeds.

See [runledger-postgres/examples/workflow_dag.rs](https://github.com/featherenvy/runledger/blob/master/runledger-postgres/examples/workflow_dag.rs)
for a compile-checked example that shows a fan-out/fan-in DAG.

## Worker Binary

Downstream services commonly run a web/API process and a separate worker
process against the same PostgreSQL database. The web process enqueues jobs and
workflows. The worker process registers handlers and runs the supervisor:

```rust
use std::time::Duration;

use runledger_core::jobs::{JobContext, JobFailure, JobType};
use runledger_core::prelude::async_trait;
use runledger_runtime::Supervisor;
use runledger_runtime::catalog::JobCatalog;
use runledger_runtime::config::JobsConfig;
use runledger_runtime::registry::JobHandler;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;

struct SendEmail;

#[async_trait]
impl JobHandler for SendEmail {
    fn job_type(&self) -> JobType<'static> {
        JobType::new("jobs.email.send")
    }

    async fn execute(&self, _context: JobContext, _payload: Value) -> Result<(), JobFailure> {
        Ok(())
    }
}

async fn run_worker() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPoolOptions::new()
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;

    runledger_postgres::ensure_schema_compatible_after_idempotency_cutover(&pool).await?;

    let catalog = JobCatalog::new().job("jobs.email.send", SendEmail);
    catalog.sync_definitions(&pool).await?;

    let supervisor = Supervisor::builder(&pool, JobsConfig::from_env())?
        .with_catalog(&catalog)
        .build()?;
    let shutdown_result = supervisor
        .run_until_shutdown(
            async {
                if let Err(error) = tokio::signal::ctrl_c().await {
                    eprintln!("failed to listen for shutdown signal: {error}");
                }
            },
            Duration::from_secs(30),
        )
        .await;

    // Keep pool cleanup independent from the shutdown result.
    pool.close().await;
    shutdown_result?;
    Ok(())
}
```

Treat a `run_until_shutdown()`, `shutdown()`, or `shutdown_with_timeout()` error
as fatal for the worker process: it means a supervised loop panicked, exited
cleanly before shutdown was requested, or did not observe shutdown within the
process deadline. `run_until_shutdown()` is the preferred method for worker
binaries because it observes internal task failures while still applying a
shutdown deadline; when it times out, remaining supervised tasks are aborted
and in-flight handler futures are dropped. Size the shutdown timeout to cover
handler drain time, worker pool concurrency, and database capacity. A useful
starting point is your per-handler high-percentile latency under
`JobsConfig::max_global_concurrency`. The worker example stores the shutdown
result before closing the pool so cleanup still runs when shutdown reports an error.

For schedules and workflows, build persistence inputs from the same catalog so
job types stay aligned with synced `job_definitions` and registered handlers:

```rust
use runledger_runtime::catalog::{CatalogJobScheduleInput, JobCatalog};

let payload = serde_json::json!({});
let schedule = catalog.job_schedule(&CatalogJobScheduleInput {
    name: "profiles.refresh.hourly",
    job_type: "profiles.refresh",
    organization_id: None,
    payload_template: &payload,
    cron_expr: "0 0 * * * *",
    is_active: true,
    next_fire_at: chrono::Utc::now(),
    max_jitter_seconds: 0,
})?;
runledger_postgres::jobs::upsert_job_schedule(&pool, &schedule).await?;
```

Catalog sync owns the definition fields it writes: `version`, retry limits,
timeout, and priority are restored to catalog defaults on each startup sync.
Enabled catalog sync preserves an existing disabled row so operator pauses
survive worker restarts; a catalog with `enabled(false)` explicitly disables its
registered definitions. `sync_definitions` is additive: removed catalog entries
are not deleted or disabled. Use `sync_definitions_exact` with a
`JobCatalogSyncScope` when deployment startup should also disable enabled
`job_definitions` rows that are absent from the catalog but inside an explicit
owned job-type set. Exact sync returns the disabled job types and refuses to
disable definitions while active schedules still reference them. Unlike additive
sync, exact sync restores catalog entries' enabled state from catalog defaults.
Catalog helper builders validate catalog membership and catalog defaults only;
operator-disabled database rows are enforced later by persistence APIs such as
job enqueue, schedule materialization, and workflow enqueue.

Lower-level `JobEnqueue`, `JobScheduleUpsert`, `WorkflowDagBuilder`, and
`WorkflowStepEnqueueBuilder` APIs remain available when you do not use a catalog.

Additional compile-checked integration examples:

- [Enqueue one job](https://github.com/featherenvy/runledger/blob/master/runledger-postgres/examples/enqueue_job.rs)
- [Workflow DAG](https://github.com/featherenvy/runledger/blob/master/runledger-postgres/examples/workflow_dag.rs)
- [External workflow gate](https://github.com/featherenvy/runledger/blob/master/runledger-postgres/examples/external_gate.rs)
- [Append workflow steps](https://github.com/featherenvy/runledger/blob/master/runledger-postgres/examples/append_workflow_steps.rs)
- [Scheduled job entrypoint](https://github.com/featherenvy/runledger/blob/master/runledger-postgres/examples/schedule_job.rs)

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
