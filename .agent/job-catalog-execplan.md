# Add a single-source job catalog

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

No `.agent/PLANS.md` or repository-root `PLANS.md` file is checked into this repository as of 2026-05-27. Maintain this document according to the ExecPlan rules in the authoring prompt and the crate-local guidance in `runledger-core/AGENTS.md`, `runledger-postgres/AGENTS.md`, and `runledger-runtime/AGENTS.md`.

## Purpose / Big Picture

Runledger currently asks users to repeat the same job identity in several places: a `JobHandler::job_type()` implementation, a `job_definitions` database row, a `JobRegistry` registration, cron schedule setup, direct enqueue calls, initial workflow steps, and appended workflow steps. That creates a split-brain footgun where a worker can register one job type while the database, schedule, or workflow references another.

After this change, an application can build one generic `JobCatalog`, use it to register handlers, sync database job definitions, validate enqueue/schedule/workflow references, and start `Supervisor` from the same source. A human can see the improvement by running tests that use a catalog to sync `job_definitions`, build a supervisor, enqueue a job, and observe that the job succeeds without separately hand-writing a `JobRegistry` or `JobDefinitionUpsert`.

## Progress

- [x] (2026-05-27T07:16:42Z) Researched the existing job-definition, registry, supervisor, schedule, and workflow enqueue boundaries.
- [x] (2026-05-27T07:16:42Z) Improved the plan against current repository code and crate guidance.
- [x] (2026-05-27T07:16:42Z) Re-checked added plan symbols against append workflow inputs, preludes, test harnesses, SQLx offline config, README commands, and CI commands.
- [x] (2026-05-27T07:16:42Z) Re-checked workflow builder signatures and errors, and tightened catalog wrapper API shapes.
- [x] (2026-05-27T07:50:00Z) Added generic catalog types and unit tests in `runledger-runtime/src/catalog.rs`.
- [x] (2026-05-27T07:50:00Z) Wired `SupervisorBuilder::with_catalog` and exported catalog types from `lib.rs` / `runledger_runtime::prelude`.
- [x] (2026-05-27T07:50:00Z) Added catalog helpers for direct enqueue, schedules, workflow DAG, workflow steps, and retry-delay overrides.
- [x] (2026-05-27T07:50:00Z) Added DB-backed integration tests in `runledger-runtime/tests/catalog.rs` and updated `prelude_smoke.rs`.
- [x] (2026-05-27T07:50:00Z) Updated `worker_binary.rs` example and README integration/worker/schedule snippets.
- [x] (2026-05-27) Ran focused runtime/postgres tests, workspace no-run validation, clippy, rustdoc checks, and refreshed SQLx offline cache files.

## Surprises & Discoveries

- Observation: There is no checked-in `.agent/PLANS.md` or root `PLANS.md`.
  Evidence: `rg --files -g 'PLANS.md' -g '.agent/PLANS.md'` returned no plan standards file.

- Observation: `runledger-runtime/AGENTS.md` says app-specific handlers and catalogs belong outside the runtime crate.
  Evidence: The runtime guide says to keep runtime orchestration generic and keep app-specific handlers and catalogs outside this crate. This plan must therefore add a generic catalog API only, not hard-code application job lists.

- Observation: `JobRegistry::register_boxed` is supplied by the `runledger_core::jobs::JobHandlerRegistry` trait, not as an inherent method on `JobRegistry`.
  Evidence: `runledger-runtime/src/registry.rs` implements `JobHandlerRegistry for JobRegistry`; `register_boxed` is declared in `runledger-core/src/jobs/handler.rs`.

- Observation: `WorkflowDagBuilder` and `WorkflowStepEnqueueBuilder` return `WorkflowBuildError` for blank step keys, blank job types, dependency errors, empty workflows, and other shape errors.
  Evidence: `runledger-core/src/jobs/workflow_enqueue/dag_builder.rs`, `step_builder.rs`, and `errors.rs` define these fallible builder methods and error variants.

- Observation: Runtime workers only claim job types present in `JobRegistry::registered_types`.
  Evidence: `runledger-runtime/src/worker.rs` computes `claimable_job_types = registry.registered_types()` before calling `claim_prestart_jobs_for_types`.

- Observation: Direct job enqueue, initial workflow job steps, appended workflow job steps, and job schedules all depend on `job_definitions`, but not all at the same time.
  Evidence: `runledger-postgres/src/jobs/queue/dispatch.rs` selects enabled defaults from `job_definitions`; `runledger-postgres/src/jobs/workflows/steps.rs` fetches enabled defaults for workflow steps; `runledger-postgres/src/jobs/workflows/mutate/append.rs` calls the same defaults helper for appended steps; `migrations/202603280001_runledger_baseline.up.sql` gives `job_schedules.job_type` a foreign key to `job_definitions`.

- Observation: A schedule can be upserted for an existing but disabled job definition, then fail later when the scheduler tries to enqueue it.
  Evidence: `runledger-postgres/src/jobs/schedules.rs` relies on the foreign key during `upsert_job_schedule_tx`, while `runledger-runtime/src/scheduler.rs` materializes schedules through `enqueue_job_tx`, which only selects enabled definitions. `runledger-runtime/src/scheduler/tests.rs` has `materialize_due_schedules_ignores_disabled_job_definition`.

- Observation: The repository uses SQLx offline metadata by default.
  Evidence: `.cargo/config.toml` sets `SQLX_OFFLINE = "true"`, README says the workspace `.sqlx/` cache is the source cache, and CI sets `SQLX_OFFLINE: "true"`.

- Observation: `get_job_definition_by_type` takes `JobType<'_>`, not `&str`.
  Evidence: `runledger-postgres/src/jobs/queue/definitions.rs`; integration tests were written with `JobType::new(...)`.

## Decision Log

- Decision: Put the first catalog API in `runledger-runtime` as a generic runtime integration helper, not as an application-specific catalog.
  Rationale: The catalog must hold concrete `JobHandler` trait objects, build a `JobRegistry`, and call PostgreSQL definition upserts. `runledger-runtime` already depends on both `runledger-core` and `runledger-postgres`. This respects `runledger-runtime/AGENTS.md` only if the module remains generic and never contains app-owned job lists.
  Date/Author: 2026-05-27 / Codex

- Decision: Make sync idempotent and non-destructive.
  Rationale: Removing a job from a process catalog must not delete or disable durable `job_definitions` rows because existing queued jobs, workflow steps, dead letters, and schedules may still reference them. The initial `sync_definitions` should upsert catalog entries only.
  Date/Author: 2026-05-27 / Codex

- Decision: Support both ergonomic and fallible job registration.
  Rationale: The user-facing sketch wants `JobCatalog::new().job("profiles.refresh", RefreshHandler)`, but tests and generated code benefit from explicit error handling. Add `try_job` returning a catalog error and `job` as a thin panic-on-invalid convenience with a clear message.
  Date/Author: 2026-05-27 / Codex

- Decision: Validate enabled catalog entries before creating new work references.
  Rationale: Direct enqueue and workflow step insertion require an enabled job definition. Schedule upsert accepts any existing definition by foreign key, but scheduler materialization later fails if the definition is disabled. Catalog helpers for new jobs, schedules, and workflow steps should therefore use an enabled-job check, while registry conversion may still register disabled jobs so old queued work and dead-letter hooks can be handled.
  Date/Author: 2026-05-27 / Codex

- Decision: Store catalog defaults centrally and materialize effective definitions at sync time.
  Rationale: The intended usage places `.defaults(...)` after `.job(...)`. If registration eagerly copied default values into each entry, that call order would silently ignore later defaults. Storing defaults centrally keeps the fluent API predictable and still maps cleanly to `JobDefinitionUpsert`.
  Date/Author: 2026-05-27 / Codex

- Decision: `require_enabled_job_type` checks `JobCatalogDefaults::is_enabled` rather than per-job flags.
  Rationale: The initial catalog stores one defaults block for all synced definitions; there is no per-entry enabled override yet. Helpers reject new work when defaults are disabled, matching what `sync_definitions` would write.
  Date/Author: 2026-05-27T07:50:00Z / Cursor Agent

## Outcomes & Retrospective

Implemented a generic `runledger_runtime::catalog` module with `JobCatalog`, `JobCatalogDefaults`, `CatalogError`, enqueue/schedule input helpers, `CatalogWorkflowDagBuilder`, and `workflow_step`. Applications can register handlers once, call `sync_definitions`, build validated `JobEnqueue` / `JobScheduleUpsert` / workflow payloads, and start `Supervisor` via `with_catalog` without duplicating registry and definition setup.

Compatibility: existing `JobRegistry`, `JobEnqueue`, `WorkflowDagBuilder`, and postgres examples are unchanged; `with_registry` remains. Postgres examples stay persistence-only; README documents the catalog path alongside lower-level APIs.

Remaining: none known. Follow-up review fixes added bounded definition-table locking for exact/disabled sync, documented the heavier lock boundary, added the `JobCatalogDefaults::version` builder, and refreshed the checked-in SQLx offline cache files generated by the implementation.

## Context and Orientation

This Rust workspace has three primary crates. `runledger-core` contains shared contracts such as `JobType`, `JobTypeName`, `JobHandler`, workflow enqueue builders, job status types, and identifier validation. `runledger-postgres` contains SQLx-backed persistence functions and input structs such as `JobDefinitionUpsert`, `JobEnqueue`, `JobScheduleUpsert`, and `AppendWorkflowStepsInput`. `runledger-runtime` contains async worker, scheduler, reaper, `JobRegistry`, and `Supervisor`.

A job type is a string identifier such as `profiles.refresh`. The database table `job_definitions` stores defaults for each job type: version, max attempts, default timeout seconds, default priority, and enabled state. A job handler is Rust code implementing `runledger_core::jobs::JobHandler`; it executes a queued job. A registry is the in-memory map from job type to handler used by runtime workers. A schedule is a database row in `job_schedules` that periodically enqueues a job. A workflow is a durable directed acyclic graph, meaning a persisted set of steps with dependencies where later steps wait for earlier steps.

The current important files are:

- `runledger-core/src/jobs/handler.rs`: defines `JobHandler` and `JobHandlerRegistry`.
- `runledger-core/src/jobs/identifiers.rs` and `runledger-core/src/jobs/identifier_macros.rs`: define borrowed `JobType<'a>` and owned `JobTypeName`.
- `runledger-core/src/jobs/workflow_enqueue/dag_builder.rs`: provides `WorkflowDagBuilder`, which accepts raw job type strings.
- `runledger-core/src/jobs/workflow_enqueue/step_builder.rs`: provides `WorkflowStepEnqueueBuilder`, which is used for advanced initial workflow steps and appended workflow steps.
- `runledger-core/src/jobs/workflow_enqueue/errors.rs`: defines `WorkflowBuildError`.
- `runledger-postgres/src/jobs/types.rs`: defines `JobDefinitionUpsert`, `JobEnqueue`, `JobScheduleUpsert`, and related records.
- `runledger-postgres/src/jobs/workflow_types.rs`: defines `AppendWorkflowStepsInput`.
- `runledger-postgres/src/jobs/queue/definitions.rs`: upserts and reads `job_definitions`.
- `runledger-postgres/src/jobs/queue/dispatch.rs`: enqueues direct jobs using enabled job definitions.
- `runledger-postgres/src/jobs/schedules.rs`: upserts schedules and validates schedule syntax.
- `runledger-postgres/src/jobs/workflows/steps.rs`: fetches enabled job-definition defaults when inserting workflow job steps.
- `runledger-postgres/src/jobs/workflows/mutate/append.rs`: appends workflow steps and also fetches enabled job-definition defaults.
- `runledger-runtime/src/registry.rs`: stores handlers and retry-delay overrides.
- `runledger-runtime/src/supervisor.rs`: builds worker, scheduler, and reaper loops from a `JobRegistry`.
- `runledger-runtime/src/scheduler.rs`: materializes schedules into `JobEnqueue` rows.
- `runledger-runtime/tests/prelude_smoke.rs`: proves the public preludes can be glob-imported together.
- `runledger-runtime/test_support.rs`: provides `setup_ephemeral_pool` and `teardown_ephemeral_pool` for DB-backed runtime integration tests.
- `runledger-runtime/examples/worker_binary.rs`, `runledger-postgres/examples/enqueue_job.rs`, `runledger-postgres/examples/schedule_job.rs`, and `runledger-postgres/examples/workflow_dag.rs`: examples that currently duplicate job-type setup.

## Plan of Work

First add `runledger-runtime/src/catalog.rs` and export it from `runledger-runtime/src/lib.rs` and `runledger_runtime::prelude`. Define `JobCatalog`, `JobCatalogDefaults`, and `CatalogError`. The module must stay generic: it may provide reusable library machinery, but it must not include any application-specific job list.

`JobCatalog` should store entries keyed by an owned validated `JobTypeName`, while each entry also stores the original `JobType<'static>` from registration. This avoids returning borrowed values from a map-owned `String` when helpers need a `JobType` for existing API structs. Each entry stores an `Arc<dyn JobHandler>` and retry-delay overrides. Effective `JobDefinitionUpsert` values are materialized from the catalog’s current `JobCatalogDefaults` at sync time, so `.job(...).defaults(...)` behaves as users expect.

`JobCatalogDefaults` should default to version `1`, max attempts `3`, default timeout seconds `300`, default priority `0`, and enabled `true`. Existing database defaults in the baseline migration are different, but the examples in this repository repeatedly use max attempts `3`, timeout `300`, and priority `0`; the catalog defaults should match the examples because the goal is to remove example and application setup duplication. Validate catalog defaults before touching the database: version, max attempts, and timeout seconds must be positive because `job_definitions` has check constraints for those fields.

Implement `try_job(job_type: &'static str, handler: impl JobHandler + 'static) -> Result<Self, CatalogError>`. It validates that the declared string is non-blank using `JobType::try_new`, validates that `handler.job_type().as_str()` is non-blank, compares the two values, and returns `CatalogError::HandlerJobTypeMismatch` if they differ. Implement `job(...) -> Self` as a convenience wrapper around `try_job(...).expect(...)` with a clear message. If the same job type is added twice, return `CatalogError::DuplicateJobType`.

Implement retry override registration on the catalog with an API shaped like `try_retry_delay_override(job_type: &str, failure_code: &'static str, retry_delay_ms: i32) -> Result<Self, CatalogError>` and `retry_delay_override(...) -> Self`. It should validate that the job type exists in the catalog, the failure code is not blank, and retry delay is positive. This mirrors `JobRegistry::register_retry_delay_override`, which asserts positive retry delays and stores overrides by job type and static failure code.

Implement `sync_definitions(&self, pool: &runledger_postgres::DbPool) -> Result<(), CatalogError>`. It validates defaults first, opens one transaction, and commits all catalog definition writes atomically. Enabled additive sync inserts missing rows with catalog defaults and updates catalog-owned fields while preserving an existing row's `is_enabled` value so operator pauses survive worker restarts. Disabled additive sync explicitly writes catalog rows as disabled after locking schedules and definitions and rejecting active schedules for those catalog job types. This function must be safe to call repeatedly. It must not delete or disable job definitions absent from the catalog.

Implement `to_registry(&self) -> JobRegistry`. In `catalog.rs`, import `runledger_core::jobs::JobHandlerRegistry` so the code can call the trait method `registry.register_boxed(Arc::clone(&entry.handler))`. Register retry-delay overrides through existing `JobRegistry::register_retry_delay_override`.

Add `SupervisorBuilder::with_catalog(mut self, catalog: JobCatalog) -> Self` in `runledger-runtime/src/supervisor.rs`. It simply calls `self.registry = Some(catalog.to_registry())`. It does not sync definitions automatically because `build` is synchronous while database sync is async. Document that applications should call `catalog.sync_definitions(&pool).await?` during startup before starting a supervisor or creating schedules.

Add catalog reference helpers. Implement `contains(job_type: JobType<'_>) -> bool`, `require_job_type(name: &str) -> Result<JobType<'static>, CatalogError>`, and `require_enabled_job_type(name: &str) -> Result<JobType<'static>, CatalogError>`. Helpers that create new work should use `require_enabled_job_type` because direct enqueue and workflow step creation need enabled definitions, and schedules for disabled definitions fail during later materialization.

Add small helper input structs to reduce schedule and workflow mistakes. Keep their fields aligned with existing public persistence structs:

    pub struct CatalogJobEnqueueInput<'a> {
        pub job_type: &'a str,
        pub organization_id: Option<Uuid>,
        pub payload: &'a serde_json::Value,
        pub priority: Option<i32>,
        pub max_attempts: Option<i32>,
        pub timeout_seconds: Option<i32>,
        pub next_run_at: Option<DateTime<Utc>>,
        pub idempotency_key: Option<&'a str>,
        pub stage: Option<JobStage>,
    }

    pub struct CatalogJobScheduleInput<'a> {
        pub name: &'a str,
        pub job_type: &'a str,
        pub organization_id: Option<Uuid>,
        pub payload_template: &'a serde_json::Value,
        pub cron_expr: &'a str,
        pub is_active: bool,
        pub next_fire_at: DateTime<Utc>,
        pub max_jitter_seconds: i32,
    }

Then implement:

- `catalog.job_enqueue(input) -> Result<JobEnqueue<'a>, CatalogError>`, validating the job type against enabled catalog entries and filling `JobEnqueue`.
- `catalog.job_schedule(input) -> Result<JobScheduleUpsert<'a>, CatalogError>`, validating the schedule job type against enabled catalog entries and filling `JobScheduleUpsert`. This helper should not duplicate cron/name/jitter validation; `upsert_job_schedule` already owns that validation.
- `CatalogWorkflowDagBuilder<'a, 'catalog>` wrapping `WorkflowDagBuilder<'a>`. Its `.job(step_key: &'a str, job_type_name: &str, payload: &'a serde_json::Value) -> Result<Self, CatalogError>` should validate `job_type_name` against enabled catalog entries, then delegate to the core builder using the stored static job type string. Its dependency and build methods should return `Result<Self, CatalogError>` or `Result<WorkflowRunEnqueue<'a>, CatalogError>` by wrapping `WorkflowBuildError`.
- `catalog.workflow_step<'a>(&self, step_key: &'a str, job_type_name: &str, payload: &'a serde_json::Value) -> Result<WorkflowStepEnqueueBuilder<'a>, CatalogError>`, validating an enabled job type and returning a `WorkflowStepEnqueueBuilder<'a>`. This covers advanced initial workflows and `append_workflow_steps`, not only the simple `WorkflowDagBuilder` path.

Keep these helpers additive. Do not change existing `JobEnqueue`, `JobScheduleUpsert`, `WorkflowDagBuilder`, `WorkflowStepEnqueueBuilder`, `AppendWorkflowStepsInput`, or `JobRegistry` APIs. Existing consumers must compile unchanged.

Update examples. In `runledger-runtime/examples/worker_binary.rs`, build a catalog with the sample handler, call `catalog.sync_definitions(&pool).await?`, and use `.with_catalog(catalog)`. In `runledger-postgres/examples/enqueue_job.rs`, `runledger-postgres/examples/schedule_job.rs`, and `runledger-postgres/examples/workflow_dag.rs`, either keep persistence-only examples as lower-level examples or add paired catalog-driven snippets in `README.md`. Do not make `runledger-postgres` depend on `runledger-runtime`; that would invert the current crate dependency direction. Update `README.md` worker and schedule snippets to show the catalog path while leaving lower-level registry APIs documented as advanced escape hatches.

## Concrete Steps

From `/home/aa/Documents/runledger`, create `runledger-runtime/src/catalog.rs`. Add the module to `runledger-runtime/src/lib.rs`:

    pub mod catalog;

Export the main types:

    pub mod prelude {
        pub use crate::catalog::{CatalogError, JobCatalog, JobCatalogDefaults};
        ...
    }

Implement catalog storage using standard library maps and existing runtime/core types. The target public shape should be close to:

    pub struct JobCatalog {
        defaults: JobCatalogDefaults,
        jobs: BTreeMap<JobTypeName, CatalogJob>,
    }

    pub struct JobCatalogDefaults {
        pub version: i32,
        pub max_attempts: i32,
        pub default_timeout_seconds: i32,
        pub default_priority: i32,
        pub is_enabled: bool,
    }

    struct CatalogJob {
        job_type: JobType<'static>,
        handler: Arc<dyn JobHandler>,
        retry_delay_overrides: BTreeMap<&'static str, i32>,
    }

    impl JobCatalog {
        pub fn new() -> Self;
        pub fn defaults(self, defaults: JobCatalogDefaults) -> Self;
        pub fn try_job<H>(self, job_type: &'static str, handler: H) -> Result<Self, CatalogError>
            where H: JobHandler + 'static;
        pub fn job<H>(self, job_type: &'static str, handler: H) -> Self
            where H: JobHandler + 'static;
        pub fn try_retry_delay_override(
            self,
            job_type: &str,
            failure_code: &'static str,
            retry_delay_ms: i32,
        ) -> Result<Self, CatalogError>;
        pub fn retry_delay_override(
            self,
            job_type: &str,
            failure_code: &'static str,
            retry_delay_ms: i32,
        ) -> Self;
        pub async fn sync_definitions(&self, pool: &runledger_postgres::DbPool) -> Result<(), CatalogError>;
        pub fn to_registry(&self) -> JobRegistry;
        pub fn contains(&self, job_type: JobType<'_>) -> bool;
        pub fn require_job_type(&self, job_type: &str) -> Result<JobType<'static>, CatalogError>;
        pub fn require_enabled_job_type(&self, job_type: &str) -> Result<JobType<'static>, CatalogError>;
    }

Use `JobTypeName::new(...)` for map keys and retain the static `JobType<'static>` in `CatalogJob` for return values. Do not attempt to return `JobType<'static>` borrowed from a `String` owned by the map.

Add `CatalogError` with `thiserror::Error`. Include variants for invalid declared job type, invalid handler job type, duplicate job type, handler job type mismatch, invalid definition value, invalid failure code, invalid retry delay, unknown job type, disabled job type, workflow build error, PostgreSQL sync failure, and transaction commit failure. For commit failure, follow the existing repository style by converting the SQLx commit error into `runledger_postgres::Error::ConnectionError(error.to_string())` before wrapping it.

Add unit tests in `runledger-runtime/src/catalog.rs` for validation and conversion. Add integration tests in a new `runledger-runtime/tests/catalog.rs` using `#[path = "../test_support.rs"] mod test_support;` and `setup_ephemeral_pool`, matching existing runtime integration tests. Tests should prove:

- a catalog rejects a blank declared job type with `CatalogError::InvalidJobType`;
- a catalog rejects a handler whose own `job_type()` is blank;
- a catalog rejects handler/job-type mismatch;
- a catalog rejects duplicate job types;
- invalid positive-definition fields are rejected before database sync;
- `sync_definitions` creates a readable `job_definitions` row with catalog defaults via `get_job_definition_by_type`;
- `.job(...).defaults(...)` uses the later defaults when syncing definitions;
- calling `sync_definitions` twice succeeds and leaves one definition row;
- `to_registry` preserves handler registration and retry-delay overrides;
- retry-delay override registration rejects unknown job types, blank failure codes, and non-positive delays;
- `Supervisor::builder(...).with_catalog(catalog).build()` processes an enqueued job after catalog sync;
- schedule helper rejects an unknown or disabled job type before touching the database;
- direct enqueue helper rejects an unknown or disabled job type before calling `enqueue_job`;
- workflow DAG helper rejects an unknown or disabled workflow step job type before enqueue;
- workflow DAG helper propagates `WorkflowBuildError` for blank step keys, unknown dependency targets, and empty workflow builds;
- `workflow_step` helper rejects an unknown or disabled job type for append-step construction.

Update `runledger-runtime/src/supervisor.rs` by importing `JobCatalog` and adding:

    pub fn with_catalog(mut self, catalog: JobCatalog) -> Self {
        self.registry = Some(catalog.to_registry());
        self
    }

Keep `with_registry` unchanged.

Update `runledger-runtime/tests/prelude_smoke.rs` so the prelude import test references the new catalog types and verifies `.with_catalog(JobCatalog::new())` can build when worker and reaper loops are disabled, just as the existing smoke test builds with an empty `JobRegistry`.

Run formatting and focused tests:

    cd /home/aa/Documents/runledger
    cargo fmt
    cargo check -p runledger-runtime
    cargo test -p runledger-runtime catalog
    cargo test -p runledger-runtime prelude
    cargo test -p runledger-runtime supervisor_processes_job_and_shuts_down

The repository uses SQLx offline mode through `.cargo/config.toml`, and this plan should not require new checked SQL. If the implementation adds or changes `sqlx::query!` calls anyway, run `./scripts/refresh-sqlx-cache.sh` before final validation and commit the resulting `.sqlx/` and vendored migration changes.

If Docker or another testcontainers-compatible container runtime is available, run the broader crate and workspace tests. The repository’s DB-backed tests start PostgreSQL containers, defaulting to `postgres:18` unless `RUNLEDGER_TEST_PG_IMAGE` is set.

    cargo test -p runledger-runtime
    cargo test -p runledger-postgres schedules
    cargo test --workspace --no-run
    cargo test --workspace

Before completion, run the crate guidance and CI-equivalent lint command if the local toolchain has clippy available:

    cargo clippy -p runledger-runtime --all-targets -- -D warnings
    cargo clippy --workspace --all-targets -- -D warnings

Expected successful output is the usual Cargo summary ending with `test result: ok`. The exact number of tests may change as new tests are added.

## Validation and Acceptance

The feature is accepted when a new integration test demonstrates this complete behavior: build one catalog containing a `CountingHandler`, call `catalog.sync_definitions(&pool).await`, start `Supervisor::builder(&pool, test_config()).with_catalog(catalog).build()`, enqueue a job of the same type without manually upserting `JobDefinitionUpsert` or manually building `JobRegistry`, and observe the job reaches `JobStatus::Succeeded`.

The behavior must fail early for split-brain mistakes. A test with `try_job("jobs.catalog.expected", HandlerReturningOtherType)` must return a catalog error before any worker or database operation starts. A schedule helper, direct enqueue helper, initial workflow helper, or append-step helper given an unregistered or disabled job type must return a catalog error before calling `upsert_job_schedule`, `enqueue_job`, `enqueue_workflow_run`, or `append_workflow_steps`.

Existing lower-level APIs must still work. The current tests that manually call `upsert_job_definition_tx`, `JobRegistry::new`, `WorkflowDagBuilder::job`, `WorkflowStepEnqueueBuilder::new`, and `AppendWorkflowStepsInput` should continue passing.

Public API compatibility is accepted when `runledger-runtime/tests/prelude_smoke.rs` still proves that `runledger_core::prelude::*`, `runledger_postgres::prelude::*`, and `runledger_runtime::prelude::*` can be glob-imported together without ambiguous `Result` or `Error` aliases.

## Idempotence and Recovery

`sync_definitions` is safe to retry. Enabled additive sync preserves existing `is_enabled` values while refreshing catalog-owned defaults and does not create duplicates. Disabled additive sync intentionally writes catalog rows disabled, after the active-schedule guard passes. If the process crashes after some upserts but before commit, PostgreSQL rolls back the transaction and the next startup can retry. If it crashes after commit, the next startup repeats the same deterministic writes.

Additive `sync_definitions` must not delete or disable job definitions for jobs absent from the catalog. That avoids breaking durable queued jobs, workflow steps, dead letters, or schedules created by previous deployments. Exact sync is implemented separately as an explicit opt-in through `sync_definitions_exact` and `JobCatalogSyncScope`; it never deletes rows, only disables enabled rows inside the caller-supplied owned scope after locking `job_schedules` and `job_definitions` and rejecting active schedules that would still reference disabled definitions.

Do not change migrations for this feature. The catalog is an API-level helper over existing tables and functions. There is no durable payload format change, no checkpoint change, and no schema rollout step.

A disabled catalog entry should still be convertible into a runtime registry entry. Existing pending jobs of that type may still exist, and the reaper may still need the handler’s dead-letter hook. New work helpers should reject disabled entries because direct enqueue and workflow insertion require enabled definitions, and scheduled jobs fail later if materialized against disabled definitions.

## Artifacts and Notes

Current split-brain setup in `runledger-postgres/examples/schedule_job.rs` manually repeats `REFRESH_JOB` in both schedule setup and job-definition upsert:

    const REFRESH_JOB: &str = "profiles.refresh";
    ...
    upsert_job_definition_tx(&mut tx, &JobDefinitionUpsert {
        job_type: JobType::new(REFRESH_JOB),
        version: 1,
        max_attempts: 3,
        default_timeout_seconds: 300,
        default_priority: 0,
        is_enabled: true,
    }).await?;

Target usage after implementation should be close to:

    let catalog = JobCatalog::new()
        .job("profiles.refresh", RefreshHandler)
        .defaults(JobCatalogDefaults::new().max_attempts(3).timeout_seconds(300));

    catalog.sync_definitions(&pool).await?;

    let supervisor = Supervisor::builder(&pool, JobsConfig::from_env())?
        .with_catalog(catalog)
        .build()?;

Because Rust does not support named arguments like `defaults(max_attempts: 3, timeout: 300)`, use builder methods or struct update syntax on `JobCatalogDefaults`.

## Interfaces and Dependencies

Use existing dependencies already present in `runledger-runtime`: `chrono`, `runledger-core`, `runledger-postgres`, `serde_json`, `sqlx`, `thiserror`, `tokio`, and `uuid`. No migration is required.

The new public module is `runledger_runtime::catalog`. It must expose:

    pub struct JobCatalog;
    pub struct JobCatalogDefaults;
    pub enum CatalogError;

`CatalogError` should use `thiserror::Error` and include variants for invalid job type, duplicate job type, handler job type mismatch, invalid definition values, invalid retry override values, unknown job type, disabled job type, workflow build error, and database sync failure.

`JobCatalog::sync_definitions` must create or update durable `job_definitions` rows while preserving operator-disabled rows for enabled additive catalogs. Disabled additive catalogs use the full-definition upsert path to write catalog entries as disabled after schedule/definition locks and active-schedule checks. Exact sync uses the full-definition upsert path because it is the opt-in source-of-truth mode and restores catalog entries' enabled state from catalog defaults before disabling absent enabled definitions inside the explicit scope. The execute path is runtime worker claiming by `JobRegistry::registered_types`. The direct enqueue path is `enqueue_job_tx`, which reads enabled definitions from `job_definitions`. The schedule setup path is `upsert_job_schedule`, whose database foreign key requires a definition row; the schedule execution path is `runledger-runtime/src/scheduler.rs`, which later calls `enqueue_job_tx` and therefore also needs the definition to be enabled. The initial workflow path is `enqueue_workflow_run`, whose step insertion fetches enabled job-definition defaults. The appended workflow path is `append_workflow_steps`, which also fetches enabled job-definition defaults.

The retry, resume, lease-expiry, terminal-failure, and dead-letter paths for existing queued jobs and workflow steps remain unchanged; the catalog affects future startup sync, new work construction, and worker claim scope, not existing row mutation logic. Worker retry-delay override behavior must remain implemented through `JobRegistry::retry_delay_override`, used in `runledger-runtime/src/worker.rs`.

Revision note: Initial ExecPlan created on 2026-05-27 after repository inspection. Improved on 2026-05-27 to account for crate-local `AGENTS.md` guidance, trait-method import requirements, database positive-field constraints, enabled-definition behavior, schedule materialization behavior, appended workflow steps, prelude smoke coverage, and actual repository test/container commands. Improved again on 2026-05-27 to make default materialization order explicit, add catalog retry-delay override APIs, spell out helper input fields from existing persistence structs, mention SQLx offline/cache behavior, and align validation commands with README and CI. Improved again on 2026-05-27 to add explicit workflow builder error propagation, helper lifetimes, and tests for catalog wrapper delegation to existing workflow builders. Saved from chat into `.agent/job-catalog-execplan.md` on 2026-05-27 so Cursor Agent could implement it from a concrete repository file. Implementation and follow-up review fixes were completed on 2026-05-27, including local validation commands and SQLx cache refresh.
