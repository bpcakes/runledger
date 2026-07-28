# Runledger

Runledger is a durable job queue and workflow engine for Rust, backed by PostgreSQL.

You bring concrete job handlers and a Postgres database; Runledger gives you a
persistent queue, a worker runtime with leasing and retries, cron schedules, and
a first-class workflow DAG for multi-step work with dependencies, fan-out/fan-in,
and external (human or API) approval gates. State lives entirely in your
database, so there is no broker to run and nothing to lose on restart.

The crates are libraries: you embed them in your own service and supply the
handlers, process model, and admin surface.

## Features

- **Durable Postgres-backed queue** — enqueue, claim, heartbeat, retry with
  provider-directed timing, succeed, cancel, dead-letter, and requeue jobs.
  Survives restarts; no separate broker.
- **Worker runtime** — a `Supervisor` that runs worker, scheduler, and reaper
  loops with lease-based ownership, lease expiry recovery, and graceful shutdown.
- **Workflow DAGs** — model dependent work declaratively. The engine validates
  the graph, enqueues root steps, releases dependents as prerequisites finish,
  and keeps run status coherent across cancellation and external gates.
- **External gates** — pause a workflow on a human approval or third-party
  callback and resume it with `complete_external_workflow_step`.
- **Cron schedules** — recurring, UTC, idempotently materialized entrypoints.
- **Idempotent enqueue** — keyed jobs and workflow runs deduplicate against the
  original enqueue request.
- **Catalog-driven setup** — register handlers, sync job definitions, and
  declare schedules from one source of truth at startup.
- **Operator TUI** — a read-only terminal dashboard for queue metrics, jobs,
  workflows, and definitions.
- **Offline builds** — SQLx compile-time-checked queries with a committed
  `.sqlx/` cache, so the workspace builds without a live database.

## Contents

- [Workspace crates](#workspace-crates)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Core concepts](#core-concepts)
  - [Choosing the right API](#choosing-the-right-api)
  - [Workflow DAGs](#workflow-dags)
  - [Schedules](#schedules)
  - [Job definition catalog](#job-definition-catalog)
- [Examples](#examples)
- [Operator TUI](#operator-tui)
- [Configuration](#configuration)
- [Database schema and migrations](#database-schema-and-migrations)
- [Operational notes](#operational-notes)
- [PostgreSQL requirements](#postgresql-requirements)
- [Working in this repository](#working-in-this-repository)
- [Releasing](#releasing)
- [Repository layout](#repository-layout)
- [License](#license)

## Workspace crates

| Crate | Role |
| --- | --- |
| [`runledger-core`](runledger-core) | Storage-agnostic contracts: handler traits, runtime types, statuses, identifiers, and workflow enqueue/DAG validation. No persistence or async loops. |
| [`runledger-postgres`](runledger-postgres) | SQLx-backed PostgreSQL persistence: queue and job lifecycle, schedules, the workflow DAG state machine, runtime configs, logs, and admin reads/mutations. |
| [`runledger-runtime`](runledger-runtime) | The async runtime: `Supervisor`, worker/scheduler/reaper loops, the job catalog, the handler registry, and runtime configuration. |
| [`runledger-tui`](runledger-tui) | Read-only terminal UI for monitoring queue metrics, jobs, workflows, and definitions. |
| [`runledger-test-support`](runledger-test-support) | Published test utilities for ephemeral PostgreSQL databases and scoped environment overrides. |

`runledger-core`, `runledger-postgres`, and `runledger-runtime` are the
libraries you depend on. Keep the layering intact: contracts in `core`, runtime
orchestration in `runtime`, and SQL/state-machine logic in `postgres`.

## Installation

Add the libraries to your service:

```toml
[dependencies]
runledger-core = "0.7"
runledger-postgres = "0.7"
runledger-runtime = "0.7"

[dev-dependencies]
runledger-test-support = "0.7"
```

The published crates require **Rust 1.88+** and **PostgreSQL 18+**. Older
PostgreSQL releases are not supported, even when an extension supplies an
equivalent `uuidv7()` function. See [PostgreSQL requirements](#postgresql-requirements).

Common imports:

```rust
use runledger_core::prelude::*;
use runledger_postgres::prelude::*;
use runledger_runtime::prelude::*;
```

## Quick start

Downstream services typically run a web/API process that enqueues work and a
separate worker process that runs handlers against the same database. A minimal
worker:

```rust
use std::time::Duration;

use runledger_core::jobs::{JobCompletion, JobContext, JobFailure, JobType};
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

    async fn execute(&self, _context: JobContext, _payload: Value) -> Result<JobCompletion, JobFailure> {
        // do the work
        Ok(JobCompletion::success())
    }
}

async fn run_worker() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPoolOptions::new()
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;

    // Apply the bundled schema (or validate it; see "Database schema and migrations").
    runledger_postgres::migrate_after_idempotency_cutover(&pool).await?;

    // Register handlers and sync their job definitions.
    let catalog = JobCatalog::new().job("jobs.email.send", SendEmail);
    catalog.sync_definitions(&pool).await?;

    // Run the supervisor until Ctrl-C, with a 30s shutdown drain deadline.
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

From anywhere else (such as your API), enqueue a job against the same pool:

```rust
let job = runledger_postgres::jobs::enqueue_job(&pool, /* JobEnqueue */).await?;
```

Notes on the worker lifecycle:

- `run_until_shutdown()` is the preferred facade for worker binaries: it observes
  internal task failures while still applying a shutdown deadline. When the
  deadline is hit, remaining supervised tasks are aborted and in-flight handler
  futures are dropped.
- Treat any error from `run_until_shutdown()`, `shutdown()`, or
  `shutdown_with_timeout()` as **fatal** for the process — a supervised loop
  panicked, exited before shutdown was requested, or did not observe shutdown
  within the deadline.
- Size the shutdown timeout to cover handler drain time under
  `JobsConfig::max_global_concurrency` and your database capacity. A per-handler
  high-percentile latency is a reasonable starting point.
- Capture the shutdown result *before* closing the pool, so cleanup runs even
  when shutdown reports an error.
- `worker::run_worker_loop`, `scheduler::run_scheduler_loop`, and
  `reaper::run_reaper_loop` remain available as low-level building blocks for
  custom orchestration; they return `RuntimeLoopExit`
  (`JoinHandle<RuntimeLoopExit>` if you type join handles explicitly).

A typical host application:

1. Either call `migrate_after_idempotency_cutover(&pool)` to apply the bundled
   schema, or apply migrations with your own tooling and then call
   `ensure_schema_compatible_after_idempotency_cutover(&pool)` to validate it.
2. Create a shared `sqlx::PgPool`.
3. Register handlers in a `JobCatalog` (or directly in a `JobRegistry` for
   advanced setups).
4. Run the `Supervisor` in a worker process.
5. Call `runledger_postgres::jobs::*` from your own admin/API surfaces.

This workspace deliberately stops at the library boundary; it does not prescribe
your process model or handler packaging. A compile-checked worker skeleton lives
at
[`runledger-runtime/examples/worker_binary.rs`](runledger-runtime/examples/worker_binary.rs).

## Core concepts

A **job** is one independent, retried unit of work, identified by a job type and
carrying a JSON payload. A **workflow run** is a DAG of steps (each step is a
job) with dependency edges; the engine drives it to completion. A **schedule**
is a cron entrypoint that materializes jobs over time. An **external step** is a
workflow step that blocks until something outside the system completes it.

### Choosing the right API

Use the highest-level API that matches the shape of the work. This matters
especially for agents and generated integrations: a workflow DAG is a built-in
feature, not something to recreate by polling jobs or chaining handlers by hand.

| Need | Prefer |
| --- | --- |
| One independent retried unit of work | `runledger_postgres::jobs::enqueue_job` |
| Multi-step work with dependencies | `WorkflowDagBuilder` (simple DAGs), or `WorkflowRunEnqueueBuilder` / `WorkflowStepEnqueueBuilder` (advanced), then `enqueue_workflow_run` |
| Multi-step work with a durable JSON result | Declare a result step, enqueue with `enqueue_workflow_run_handle`, then call `WorkflowRunHandle::get_result` |
| Fan-out, fan-in, or ordered stages | `WorkflowDagBuilder::after_success` / `after_terminal` (or lower-level `depends_on_success` / `depends_on_terminal`) |
| Human/API approval or another external gate | External workflow steps and `complete_external_workflow_step` |
| Delayed or recurring entrypoint | `JobScheduleUpsert` and `upsert_job_schedule` (or catalog schedules) |
| Worker process lifecycle | `runledger_runtime::Supervisor::run_until_shutdown` |
| Admin/status views | `runledger_postgres::jobs` read/list/count APIs, including `count_workflow_runs` |

For ordinary dependent work, **do not** poll `get_job_by_id` in a loop, enqueue
dependent jobs from parent handlers, encode dependency state in payload JSON, or
add app-owned tables to track workflow edges. Model the run as a workflow DAG
instead. Hand-rolled orchestration is only appropriate when you are
intentionally building an orchestrator outside Runledger.

For prompt-facing summaries, see
[`llms.txt`](llms.txt) (short) and
[`docs/downstream-agent-guide.md`](docs/downstream-agent-guide.md) (longer).

### Workflow DAGs

Model dependencies directly in the enqueue request. The engine persists the run,
validates the DAG, enqueues root steps, releases dependents as prerequisites
finish, and keeps run status coherent with cancellation and external gates.

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

`WorkflowDagBuilder` takes raw string identifiers for readable call sites and
validates the workflow shape before enqueueing — but it does **not** prove at
compile time that a job type has a registered definition or handler. Reach for
`WorkflowRunEnqueueBuilder` / `WorkflowStepEnqueueBuilder` when you need per-step
priority, attempts, timeout, or stage; external steps; hand-authored dependency
specs; or explicit `StepKey` / `JobType` values.

Validation happens in two stages — some errors surface at the call site,
the rest at `.build()` / `.try_build()`:

| Call | Fails immediately | Deferred until build |
| --- | --- | --- |
| `WorkflowDagBuilder::new(...)` | never | blank workflow type |
| `WorkflowDagBuilder::try_new(...)` | blank workflow type | empty step list, dependency graph errors |
| `.job(step, job_type, payload)` | blank step key, blank job type, duplicate step key | job-type registration is not checked here |
| `.after_success(step, prereqs)` / `.after_terminal(...)` | blank target/prerequisite key, unknown target step | missing prerequisite, self-dependency, duplicate dependency, cycle |
| `.idempotency_key(...)` | never | blank idempotency key |

### Workflow results and handles

Workflows can declare one DAG step as the durable result step. A handler
returns a compact JSON result with `JobCompletion::with_output(...)`; when the
run reaches `SUCCEEDED`, Runledger materializes that step output as the workflow
result.

```rust
let run = WorkflowDagBuilder::new("profiles.research", &metadata)
    .idempotency_key("profile:p_123:research")
    .job("crawl", "profiles.crawl", &crawl_payload)?
    .job("persist", "profiles.persist", &persist_payload)?
    .after_success("persist", ["crawl"])?
    .result_step("persist")?
    .build()?;

let handle = runledger_postgres::jobs::enqueue_workflow_run_handle(&pool, &run).await?;
let result = handle.get_result(Default::default()).await?;
```

The handle is scoped when created or retrieved: organization workflows use
`WorkflowRunHandleScope::Organization`, global workflows use `Global`, and
trusted operator surfaces can use `Admin`. Use `get_status` for a cheap status
probe, `get_run` to load the scoped run record, and `get_result` to wait for or
read the declared result. Notifications wake waiters quickly, but polling
remains the correctness path. `WorkflowRunWaitOptions::default()` waits up to
five minutes by default; set `timeout: None` only when the caller intentionally
wants to wait indefinitely. Each active waiter may hold a PostgreSQL `LISTEN`
connection until the result is ready, so size pools accordingly and use shorter
explicit timeouts for high fan-out callers.
Keep outputs compact: result JSON is persisted on the job, step, and workflow
run rows; store large artifacts externally and return references. Workflows
without a declared result still run normally; `get_result` returns
`workflow.result_not_declared`. Other handle error codes include
`workflow.handle_storage_error`, `workflow.run_not_found`,
`workflow.result_missing`, `workflow.result_unsuccessful_terminal`, and
`workflow.result_wait_timeout`.

External workflow steps can also provide result output when completed
successfully:

```rust
use runledger_core::jobs::{StepKey, WorkflowStepStatus};
use runledger_postgres::jobs::CompleteExternalWorkflowStepInput;

let approval_output = serde_json::json!({ "approved_by": "ops" });

runledger_postgres::jobs::complete_external_workflow_step(
    &pool,
    &CompleteExternalWorkflowStepInput {
        workflow_run_id,
        organization_id: None,
        step_key: StepKey::new("approval"),
        terminal_status: WorkflowStepStatus::Succeeded,
        status_reason: Some("approved"),
        last_error_code: None,
        last_error_message: None,
        output: Some(&approval_output),
    },
)
.await?;
```

`output` is valid only with `WorkflowStepStatus::Succeeded`; failed or canceled
external completions must pass `None`. Retrying completion for an already
terminal external step is idempotent only when the terminal status,
`status_reason`, `last_error_code`, and `last_error_message` match; changed
metadata returns `workflow.external_step_conflicting_completion_retry`. For
successful completions, output must also match, or Runledger returns
`workflow.external_step_conflicting_output_retry`.

Breaking API note: `JobHandler::execute` returns
`Result<JobCompletion, JobFailure>`. The old stage-bearing `JobProgress`
completion type was removed; use `JobCompletion::success()` or
`JobCompletion::with_output(...)`. In-flight progress reporting still uses
`JobProgressUpdate`. Completion disposition and final output are intentionally
private; inspect them with `disposition()` / `output()` and use constructors
rather than struct literals.

### Handler-selected retry timing

When a provider supplies a dynamic reset time, a handler can attach either a
relative delay or an absolute UTC timestamp to a retryable failure:

```rust
use std::time::Duration;

let transport_failure = JobFailure::retryable(
    "provider.temporarily_unavailable",
    "Provider asked the client to retry later.",
)
.retry_after(Duration::from_secs(30));

let rate_limit_failure = JobFailure::retryable(
    "provider.rate_limited",
    "Provider rate limit reached.",
)
.retry_at(provider_reset_at);
```

This is a failed attempt, not an attempt-neutral defer or a successful
continuation. It consumes the current run's attempt budget, keeps the same
`run_number`, and does not release workflow dependents. If the failure remains
retryable, handler-selected timing takes precedence over a registered
job-type/failure-code retry-delay override, which takes precedence over
Runledger's exponential backoff. Terminal and panicked failures, and failures
that exhaust `max_attempts`, are dead-lettered without applying or validating
the timing.

`retry_after` is measured from the PostgreSQL completion clock; positive
sub-millisecond values round up to one millisecond. Zero or unrepresentable
relative delays become the terminal `job.invalid_retry_timing` handler failure.
`retry_at` uses the supplied provider timestamp directly, rounded up only when
needed for PostgreSQL microsecond precision. A timestamp at or before the
database completion time makes the job immediately eligible, avoiding a
worker-clock conversion. Timestamps outside PostgreSQL's supported range become
the terminal `job.invalid_retry_timing` handler failure.

Relative retries retain `retry_delay_ms` in the attempt and `RETRY_SCHEDULED`
event. Absolute retries leave the attempt's relative-delay field empty and
record `requested_retry_at` plus the effective `next_run_at` in the event.
Lifecycle observers likewise distinguish
`JobFailureDisposition::RetryScheduled` from `RetryScheduledAt`; the
disposition is the committed schedule, while `JobFailure::retry_timing()` is the
handler's request.

### Bounded job continuation

A direct-job handler that has successfully finished one bounded slice but still
has more work for the same logical job can return
`JobCompletion::continue_now()` or `JobCompletion::continue_after(delay)`.
Continuation is an opt-in handler disposition, not a Runledger-wide feature
flag. An application may upgrade to 0.6 and never adopt continuation; handlers
that keep returning terminal success or failure retain their existing behavior.
Progress and checkpoints can be carried into the next run with the existing
builders:

```rust
use std::time::Duration;

let completion = JobCompletion::continue_after(Duration::from_secs(5))
    .progress(processed, total)
    .checkpoint(serde_json::json!({ "cursor": next_cursor }));
```

On the next claim, the handler reads that committed value from
`context.checkpoint`; the original payload remains unchanged. A first run, or a
run without committed resume state, receives `None`.

Runledger closes the current attempt successfully, changes the exact live lease
back to `PENDING`, increments `run_number`, resets `attempt` to zero, releases
the worker/lease, and writes a `REQUEUED` event whose reason and stable
`requeue_kind` are `HANDLER_CONTINUATION`. The job ID and payload stay the same.
A later claim of the next run starts at attempt one with a fresh failure-attempt
budget. If the
continuation write does not commit, the durable row remains leased and normal
lease recovery retries the idempotent slice while attempts remain or
dead-letters an exhausted run. State from an uncommitted slice, including its
new checkpoint, cannot be recovered. Final output is valid only for terminal
success; continuation-plus-output cannot be constructed or deserialized.
Workflow-managed handlers must use
normal workflow step completion rather than continuation.

Successful continuations deliberately have no implicit run cap and do not
consume the per-run `max_attempts` failure budget. The handler owns its terminal
condition; use a nonzero delay for polling-style work and do not return
`continue_now()` forever. Production handlers should version their checkpoint
shape, make every slice idempotent, enforce a logical deadline or run limit,
canary activation by job type or tenant, and alert on continuation rate and run
depth. `get_job_continuation_metrics` returns a
`JobContinuationMetricsRecord` per job type with `continued_24h`,
`active_continued_count`, and `max_active_run_number` for canary and runaway-loop
alerts. Active counts include only jobs whose current run was created by a
handler continuation; a later admin recovery is not mislabeled as active
continuation. The packaged external-consumer
[`smoke` test](smoke/external-consumer/tests/smoke.rs) is a compile-checked
continuation, recovery, successful-replay, and metrics example; the
[`downstream agent guide`](docs/downstream-agent-guide.md) contains the full
adoption checklist and PostgreSQL 18 operational queries.

Lifecycle observers receive `on_job_continued` after the same run's
`on_job_running` callback settles, with the completed run identity, duration,
next run number/time, and committed progress. Observers are best-effort and
different run numbers may be observed concurrently or out of order; correlate
by `(job_id, run_number)` and use durable job events for authoritative history.

#### Pre-0.6 to 0.6 continuation and recovery rollout

Continuation and the new transactional recovery path require a two-phase
runtime rollout even though 0.6.0 added no schema migration:

1. Deploy 0.6 with continuation emission and new recovery calls disabled to
   every process that can participate in job lifecycle state: workers, reapers,
   and admin, API, or repair processes that cancel or requeue jobs. Keep those
   code paths unused until every pre-0.6 process has stopped and leases created
   by old workers have quiesced. There is no global continuation switch:
   "disabled" means no deployed handler returns a continuation disposition.
2. Only after the fleet is wholly on 0.6, enable handlers that return
   `continue_now()` / `continue_after(...)` and callers of the new recovery API.
   These can be activated independently. Continuation remains optional; callers
   that still use deprecated `requeue_job` must plan a typed recovery migration.

Once phase 2 is active, do not partially roll back or run pre-0.6 and 0.6
job-state writers together. A coordinated rollback must first disable new
continuations and new recovery calls, drain every `PENDING` or `LEASED` job
whose current run was created by a handler continuation to a terminal state,
and wait for retained live-lease markers on canceled jobs to quiesce. Then stop
every 0.6 worker, reaper, and admin/API/repair writer before starting any
pre-0.6 process. Follow the copyable activation and rollback runbook in the
[`downstream agent guide`](docs/downstream-agent-guide.md#pre-06-to-06-activation-and-rollback-runbook).

The target of `.after_success(...)` / `.after_terminal(...)` must already have
been added with `.job(...)`; prerequisite steps may be added later in the chain,
as long as every referenced step exists before `.build()` succeeds.

### Schedules

Schedules are UTC-only. Choose an API by who owns the schedule definition:

- `.schedule(...)` + `sync_schedules` — static schedules registered in the
  worker catalog next to their handler.
- `sync_schedules_with` — schedule specs assembled at startup from config,
  feature flags, or tenants (outside the builder chain).
- `sync_schedules_exact` / `sync_schedules_exact_with` — when this deployment
  owns a bounded schedule-name scope and missing schedules in that scope should
  be deactivated. Exact sync takes a bounded table lock so overlapping startup
  syncs do not interleave their active sets. Scheduler claims and fire-cursor
  updates can briefly wait behind the same lock; during rolling deploys, keep
  scopes narrow enough that old and new workers do not deactivate each other's
  schedules unintentionally. Keep owned scopes deployment-stable:
  feature-flagged schedules should usually stay registered with
  `is_active: false` instead of disappearing from the scope.
- `job_schedule` + `upsert_job_schedule` — one-off setup, migrations, admin
  tools, or schedules that should not be catalog-owned. Call
  `set_job_schedule_active` separately to change active state on an existing
  lower-level schedule.

```rust
use runledger_runtime::catalog::{CatalogJobScheduleSpec, JobCatalog};

let catalog = JobCatalog::new()
    .job("profiles.refresh", RefreshHandler)
    .schedule(CatalogJobScheduleSpec {
        name: "profiles.refresh.hourly",
        job_type: "profiles.refresh",
        cron_expr: "0 0 * * * *",
        payload_template: &serde_json::json!({}),
        is_active: flags.hourly_refresh,
        organization_id: None,
        max_jitter_seconds: 0,
        next_fire_at: None,
    });

catalog.sync_definitions(&pool).await?;
catalog.sync_schedules(&pool).await?;
```

Register a schedule's `.job(...)` before its `.schedule(...)` — schedule
registration validates the referenced catalog job type immediately. Sync
preserves an existing `next_fire_at` cursor while the cron expression is
unchanged; changing `cron_expr` stores the spec's `next_fire_at`, or `Utc::now()`
when it is `None`.
Catalog schedule sync applies each spec's `is_active` value on every sync, so an
admin pause made with `set_job_schedule_active(false)` is overwritten when the
catalog spec still says `is_active: true`. Use the lower-level `job_schedule` +
`upsert_job_schedule` path for schedules whose active state should be owned by
admin pause/resume workflows; that path sets `is_active` on first insert, then
preserves the stored active state on conflict.

Active schedules require enabled job definitions. Creating, syncing, or
activating a schedule for a missing or disabled definition returns
`job_schedule.definition_not_found_or_disabled`; disabling a job definition that
still has active schedules returns `job_definition.active_schedule_exists`.
During scheduler catch-up after downtime, Runledger materializes at most one
stale fire with its original `scheduled_for` metadata, then coalesces
`next_fire_at` to the first future cron fire instead of replaying every missed
tick.

For exact sync of registered schedules, derive the owned scope from the catalog
to avoid repeating names:

```rust
let scope = catalog.schedule_sync_scope()?;
catalog.sync_schedules_exact(&pool, &scope).await?;
```

If a deployment needs both registered schedules and dynamic startup specs in one
exact source-of-truth set, build one explicit spec list and
`JobCatalogScheduleSyncScope` for `sync_schedules_exact_with`; Runledger does
not provide an implicit union helper because that can hide ownership mistakes.

### Job definition catalog

`sync_definitions` is **additive**: it owns the definition fields it writes
(`version`, retry limits, timeout, priority), restoring them to effective catalog
values on each startup. It preserves an existing *disabled* row, so operator
pauses survive restarts; an explicit `enabled(false)` default or per-job override
disables a definition. Removed catalog entries are **not** deleted or disabled.

Use `sync_definitions_exact` with a `JobCatalogSyncScope` when startup should
also disable enabled `job_definitions` rows that are absent from the catalog but
inside an explicit owned job-type set. Exact sync returns the disabled job types,
refuses to disable definitions still referenced by active schedules, and (unlike
additive sync) restores catalog entries' enabled state from catalog defaults.

Override individual definitions with `job_with_definition_overrides` /
`definition_overrides`:

```rust
let catalog = JobCatalog::new()
    .job_with_definition_overrides(
        "documents.extract",
        ExtractDocuments,
        JobCatalogDefinitionOverrides::new()
            .timeout_seconds(600)
            .priority(20),
    )
    .job_with_definition_overrides(
        "auth.cleanup",
        CleanupAuth,
        JobCatalogDefinitionOverrides::new()
            .timeout_seconds(60)
            .priority(0),
    );
```

Overrides take precedence over `JobCatalogDefaults` for only the fields they set:
`version`, `max_attempts`, `timeout_seconds`, `priority`, and `enabled`. Version,
attempts, and timeout values must be positive; priority may be zero or negative.
An `enabled(true)` override can keep one job effectively enabled under disabled
catalog defaults, while `enabled(false)` disables that job during sync. Additive
sync still preserves an already-disabled database row for effectively enabled
jobs so operator pauses survive restarts; exact sync restores enabled state from
the effective catalog value.

Catalog helper builders validate catalog membership and effective enabled state;
operator-disabled database rows are enforced later by persistence APIs (job
enqueue, schedule materialization, workflow enqueue). The lower-level
`JobEnqueue`, `JobScheduleUpsert`, `WorkflowDagBuilder`, and
`WorkflowStepEnqueueBuilder` APIs remain available when you do not use a catalog.

## Examples

Each example is compile-checked:

- [Enqueue one job](runledger-postgres/examples/enqueue_job.rs)
- [Workflow DAG (fan-out / fan-in)](runledger-postgres/examples/workflow_dag.rs)
- [External workflow gate](runledger-postgres/examples/external_gate.rs)
- [Append workflow steps](runledger-postgres/examples/append_workflow_steps.rs)
- [Scheduled job entrypoint](runledger-postgres/examples/schedule_job.rs)
- [Worker binary skeleton](runledger-runtime/examples/worker_binary.rs)
- [Packaged continuation, recovery, and replay smoke test](smoke/external-consumer/tests/smoke.rs)

## Admin reads

The `runledger_postgres::jobs` admin surface exposes job/workflow detail, list,
and count helpers for operator UIs and service-owned dashboards. Use
`list_workflow_runs` with `WorkflowRunListFilter` when rendering workflow tables,
and `count_workflow_runs` with `WorkflowRunCountFilter` for status counters such
as failed workflows or runs waiting for external completion. These helpers use
the same optional organization scope and workflow-type substring filtering as
the TUI.

Use `get_job_continuation_metrics` for continuation canaries and runaway-loop
alerts. Each `JobContinuationMetricsRecord` reports the prior 24 hours' successful
continuations, the number of pending/leased jobs whose current run was created
by continuation, and the highest current run number among those active jobs.
Passing no organization filter aggregates all scopes; it does not mean exact
global scope.

`update_job_payload_uuid_array_field` is intentionally narrow: it mutates one
UUID-array payload field only for direct jobs that are still pending and
unclaimed. It returns `JobPayloadUuidArrayFieldUpdate::Updated`, `NotFound`, or
`Rejected` with a reason. Rejections distinguish workflow-managed jobs,
idempotent request snapshots that cannot be kept consistent, and jobs that are
already claimed or terminal.

## Operator TUI

`runledger-tui` is a read-only terminal UI for operators and local development.
It connects to the same database as your workers and surfaces dashboard metrics,
the job queue, workflow runs, and job definitions through the existing
`runledger-postgres` admin read APIs.

The dashboard includes continuation volume over 24 hours (`Cont 24h`), active
continued jobs (`Cont now`), maximum active run depth (`Max run`), and a total
active-continuation KPI. A selected `REQUEUED` event shows its reason and, for
handler continuation, the next run number/time and exact microsecond delay. A
selected successful-replay `ENQUEUED` event shows its source job/run, request
key, and reason.

By default it uses **global** scope (`organization_id = NULL`) so rows from all
organizations are visible. Pass `--org <uuid>` at startup, or press `o` at
runtime, to scope to one organization.

```bash
export DATABASE_URL=postgres://user:pass@localhost/runledger
cargo run -p runledger-tui

# optional org scope
cargo run -p runledger-tui -- --org 00000000-0000-0000-0000-000000000001
```

`DATABASE_URL` must point at a database with the Runledger schema already
migrated. The binary runs `ensure_schema_compatible_after_idempotency_cutover`
on startup unless `--skip-schema-check` is set.

Keys: `1`–`4` or `Tab` switch screens · `Shift+Tab` moves backward · `j`/`k`
or Up/Down move selection · `g`/`G` jump to first/last row · `PgUp`/`PgDn` page
selection · `Enter`/`l` open job/workflow detail · `h`/`Esc` go back · `[`/`]`
or Left/Right switch job-detail panes · `/` searches the current table · `t`
edits the job/workflow type filter · `w` edits the workflow type filter from the
workflows screen · `f` cycles queue status filters · `c` clears contextual
filters · `v` toggles payload wrapping · `R` toggles raw/pretty payload mode ·
`y` copies the selected ID · `p` pauses auto-refresh · `:` opens the command
palette · `r`/`.` refresh · `o` edits org scope · `?` help · `q` quit.

## Configuration

`runledger-runtime` reads worker settings from the environment via
`JobsConfig::from_env()` (see
[`runledger-runtime/src/config.rs`](runledger-runtime/src/config.rs)):

| Variable | Purpose |
| --- | --- |
| `JOBS_WORKER_ID` | Worker identity; blank falls back to `worker-<uuidv7>` |
| `JOBS_POLL_INTERVAL_MS` | Queue poll interval |
| `JOBS_CLAIM_BATCH_SIZE` | Jobs claimed per poll |
| `JOBS_LEASE_TTL_SECONDS` | Lease duration; clamped to at least `10` |
| `JOBS_MAX_GLOBAL_CONCURRENCY` | Max concurrent handler executions |
| `JOBS_REAPER_INTERVAL_SECONDS` | Reaper sweep interval |
| `JOBS_SCHEDULE_POLL_INTERVAL_SECONDS` | Schedule materialization interval |
| `JOBS_REAPER_RETRY_DELAY_MS` | Delay before reaped jobs become claimable |

Interval and concurrency values are clamped to safe minimums.
`JobsConfig::from_env()` produces a valid config; if you construct `JobsConfig`
directly, call `validate()` before starting runtime loops. Supervisor builders
reject invalid configs with `RuntimeError::InvalidJobsConfig`, and low-level
loops can return `RuntimeLoopExit::InvalidConfig`.

## Database schema and migrations

The schema is limited to Runledger-owned objects:

- **Queue and lifecycle:** `job_definitions`, `job_queue`, `job_attempts`,
  `job_events`, `job_dead_letters`, `job_schedules`
- **Workflow orchestration:** `workflow_runs`, `workflow_steps`,
  `workflow_step_dependencies`, `workflow_run_mutations`
- **Operational support:** `job_logs`, `job_runtime_configs`, `job_replays`
- **Derived views:** `job_metrics_rollup`, `job_continuation_metrics_rollup`

Notable features: idempotent queueing via `idempotency_key`, cron-backed
schedule materialization, workflow DAG execution with dependency counters,
external gates via `WAITING_FOR_EXTERNAL`, append-only workflow mutation
tracking, and panic-aware metrics rollups.

A few columns — `organization_id`, `created_by_user_id`, `updated_by_user_id` —
are kept for integration flexibility but carry no foreign keys; Runledger treats
them as opaque UUIDs. Add referential integrity in your own schema layer if you
need it.

### Migration set

Migrations live in [`migrations/`](migrations) as a flattened baseline plus
forward migrations:

- `202603280001_runledger_baseline` — the standalone schema baseline (helper
  functions, queue tables, workflow DAG tables, logs, runtime configs, workflow
  mutations, external gates, panic-aware attempt outcomes, metrics rollup view).
- `202604100001_runledger_migration_history` — creates
  `runledger_migration_history` and records the baseline and history-table
  versions.
- `202605180001_add_enqueue_request_snapshots` — adds `enqueue_request`
  snapshots to `job_queue` and `workflow_runs` so keyed enqueue retries compare
  the original request instead of mutable runtime state.
- `202605220001_enforce_enqueue_request_snapshots` — blocks new keyed rows
  without snapshots; startup validation rejects pre-cutover legacy rows.
- `202606030001_workflow_results` — adds job/step output storage and workflow
  result handles. Absent result steps are omitted from canonical workflow
  idempotency snapshots so existing no-result snapshots keep matching.
- `202607190001_job_replays_and_continuation_metrics` — adds durable successful
  job replay lineage and a dedicated continuation metrics rollup. SQLx records
  and checksum-validates this additive migration in `_sqlx_migrations`; it
  deliberately does not add a
  compatibility-fence row to `runledger_migration_history`, allowing
  Runledger's filtered released 0.6.0 startup and schema guards to coexist
  during expand-first rollout and code rollback. This does not make a raw
  `MIGRATOR.run(...)` from that exact release tolerate the newer SQLx history
  row.
- `202607250001_harden_continuation_metrics_payload_validation` — replaces the
  continuation rollup view so only well-typed, internally consistent handler
  continuation events contribute to its 24-hour and active-run metrics. This
  view-only correction also relies on SQLx history without advancing the
  compatibility-fence history, allowing filtered 0.7.0 startup paths to coexist
  during patch rollout.

Treat the flattened baseline as a from-scratch schema definition, not an
in-place upgrade from the older multi-file standalone history; apply later
forward migrations normally. The workspace-root `migrations/` directory is the
canonical source for development and review.

### Applying or validating the schema

Two supported startup modes:

- `migrate_after_idempotency_cutover(&pool)` — applies the bundled schema and
  rejects keyed legacy rows without enqueue snapshots.
- `ensure_schema_compatible_after_idempotency_cutover(&pool)` — read-only
  validation that an existing `_sqlx_migrations` history matches the bundled
  migrations, with explicit errors for missing history, incompatible history,
  legacy idempotency rows, or PostgreSQL query/connectivity failures.
  Externally managed DDL can validate the `NOT VALID` cutover constraints after
  this check passes.

For consumers of the published crates:

- `runledger_postgres::MIGRATOR` embeds the vendored
  `runledger-postgres/migrations/` copy for expert inspection, checksum
  comparison, and migration-manifest synchronization. Iterating it is
  supported; directly invoking its raw `run` or `undo` methods against a shared
  pool is not the supported startup path.
- `runledger-test-support` embeds its own `runledger-test-support/migrations/` copy for packaged test harnesses.
- `runledger-postgres/build.rs` fails local builds if the vendored copy drifts
  from the canonical workspace-root `migrations/` directory.

Call `migrate_after_idempotency_cutover` to apply migrations, or
`ensure_schema_compatible_after_idempotency_cutover` when DDL is managed
externally, before using `runledger-postgres` or running DB-backed tests. SQLx
0.8 can return early from a raw migration-history rejection without releasing
its session advisory lock. A process that unavoidably executes a raw migrator
must use a disposable connection or pool and close it after any error rather
than retrying with the possibly locked pool.

Apply `202607190001_job_replays_and_continuation_metrics` before deploying code
that calls successful-replay or continuation-metrics APIs, and apply
`202607250001_harden_continuation_metrics_payload_validation` for corrected
continuation metrics. Older job-state code remains data-plane compatible with
the additive table and views, so these are expand-first changes, but their
startup path matters. Choose one of these code rollback strategies:

1. Recommended: leave the migration applied and start the rollback binary with
   `migrate_after_idempotency_cutover` or
   `ensure_schema_compatible_after_idempotency_cutover`. These Runledger paths
   filter SQLx history to the migrations embedded in that release, preserving
   replay lineage and idempotency state. If the binary directly calls raw
   `MIGRATOR.run(...)`, patch its startup to use a filtered path first.
2. An exact older raw `MIGRATOR.run(...)` cannot tolerate the newer SQLx history
   row. If startup cannot be patched, use the newer release artifact to run its
   down migration before starting the older binary. This deletes relational
   `job_replays` lineage and replay-idempotency state while leaving
   replay-created queue rows and their lineage-bearing `ENQUEUED` events in
   place. Use this destructive path after replay activation only with explicit
   acceptance of that data loss.

### Enqueue-request snapshot cutover

Apply the bundled migrations, then run one of the startup APIs. If it returns
`SchemaCompatibilityError::LegacyIdempotencySnapshotsMissing`:

1. Inspect legacy rows with the
   `idx_job_queue_missing_enqueue_request_snapshot` and
   `idx_workflow_runs_missing_enqueue_request_snapshot` partial indexes.
2. Remediate or drain those keyed rows, then retry startup.

Prefer natural drain, or clearing the stale `idempotency_key` where retry
identity no longer matters. Only backfill `enqueue_request` when you have the
original canonical enqueue request — never reconstruct it from mutable live
queue/workflow state; keyed rows created before snapshots existed cannot be
safely reconstructed, and keyed retries against them return dedicated conflict
errors. `migrate_after_idempotency_cutover` validates the cutover constraints
once no legacy rows remain; that first validation scans `job_queue` and
`workflow_runs` and may briefly delay startup on large tables without blocking
ordinary DML. The cutover migration also builds helper indexes — on large
tables, apply it during a maintenance window appropriate for your write volume.

## Operational notes

Stable behaviors worth knowing when integrating against `runledger-postgres`:

- **Client-safe errors.** `QueryError`'s `Display` and `Debug` omit internal
  database context and are safe for public surfaces; use
  `QueryError::internal_message()` for server-side diagnostics.
- **Lease ownership.** Worker lifecycle updates reject expired leases with the
  stable `job.lease_owner_mismatch` code, even when the lease was lost by time
  rather than to another worker. Once `lease_expires_at` passes there is no
  owner grace period for heartbeat/progress/success/failure/continuation writes.
- **Transactional enqueue state.** Use `enqueue_job_with_outcome_tx` when the
  caller needs the job ID together with its locked `status`, `run_number`, and
  `Inserted`/`Existing` disposition. That API takes a mutation-ready lock on an
  existing keyed row. `enqueue_job_tx` remains the UUID-only compatibility API
  and retains key-share concurrency between identical keyed enqueues while
  composing safely with same-transaction compare-and-requeue.
- **Compare-and-requeue.** Use pool-owning `compare_and_requeue_job` for a
  standalone recovery or `compare_and_requeue_job_tx` when recovery must compose
  atomically with application writes. Build an exact request from an observed
  `JobQueueRecord` with `CompareAndRequeueJob::from_observed_job`, or provide the
  expectations explicitly. `JobScope::Global` matches
  only a global row, `JobScope::Organization(id)` matches only that tenant, and
  `RequeueableJobStatus` deliberately cannot represent `SUCCEEDED`. Stale status
  or run expectations and missing rows are returned as no-mutation outcomes
  without locking a live worker row. The caller transaction must use
  `READ COMMITTED`; other isolation levels return
  `job.compare_and_requeue_unsupported_isolation` before lookup. Canceling a
  leased job preserves its original expiry as a quiescence marker; recovery
  returns `CancellationNotQuiesced { retry_after, .. }` until that marker passes,
  preventing a healthy canceled handler from overlapping the replacement run.
  Every request must choose `JobRequeueStatePolicy::PreserveProgressAndCheckpoint`
  to resume from committed state or `ResetProgressAndCheckpoint` to restart
  from scratch; the selected policy is recorded in the `REQUEUED` event.
  The older pool-owning `requeue_job` API is deprecated for 0.6 compatibility;
  its `organization_id: None` is an unconstrained lookup, not
  `JobScope::Global`, and its behavior corresponds to
  `ResetProgressAndCheckpoint`. Migrate every compatibility caller deliberately
  and handle `NotFound`, `ExpectationMismatch`, and
  `CancellationNotQuiesced` as no-mutation outcomes. The typed API intentionally
  does not replay `SUCCEEDED` jobs in place; use the separate successful-replay
  API described below.
  When a canceled handler's retained lease is still active, the compatibility
  API returns the
  conflict code `job.cancellation_not_quiesced` so compatibility callers know
  the recovery may be retried after lease expiry.
- **Successful replay.** `compare_and_replay_succeeded_job` creates an
  idempotent fresh job from an exactly scoped successful direct-job run;
  `compare_and_replay_succeeded_job_tx` composes the same operation with a
  caller-owned `READ COMMITTED` transaction. The required
  `replay_request_key` identifies one replay action and the required reason is
  audited. Reusing the same source run, key, and reason returns the existing
  replay; reusing the key with a different reason returns
  `job.replay_idempotency_conflict`. The source row and output remain unchanged.
  The replay starts at run one with a new ID, copied payload/effective execution
  settings, no copied progress/checkpoint/output/original idempotency key, and
  lineage in `job_replays` plus its `ENQUEUED` event. Inspect
  `CompareAndReplaySucceededJobOutcome::Replayed.replay.disposition` to
  distinguish insertion from an idempotent retry; `ExpectationMismatch` and
  `NotFound` do not create a job. Queue retention cannot delete only the replay
  row while its source remains, because that would erase the idempotency guard.
  Deleting the source cascades its lineage; a single retention statement may
  delete both source and replay rows together.
- **Success stage.** `complete_job_success` persists `JobStage::Completed`; any
  other success stage is rejected as a caller error.
- **Workflow release conflicts.** Workflow-backed job completion waits for an
  in-flight workflow cancellation to commit or roll back instead of returning a
  transient `workflow.release_conflict`. Append and external-step release paths
  may still return `workflow.release_conflict` while cancellation holds the
  exclusive release lock.
- **Workflow-managed jobs.** Jobs created for workflow steps cannot be requeued
  or replayed directly with these job-level APIs; that returns
  `job.workflow_requeue_not_supported` so the workflow DAG cannot be bypassed.
  Use workflow cancellation, external completion, or append APIs for
  workflow-level recovery.
- **Stable error codes.** Conflicts such as `workflow.append_conflicting_retry`
  are conflict-category errors; branch on the stable code rather than the broad
  category.
- **Isolation.** Release-sensitive workflow operations, workflow append
  mutations, and keyed enqueue retries require PostgreSQL `READ COMMITTED`
  semantics. `READ UNCOMMITTED` is accepted because PostgreSQL implements it as
  read committed.

Migration note for 0.3.x: catalog sync error variants that carry persistence
errors now box them as `Box<runledger_postgres::Error>` to keep
`Result<_, CatalogError>` and `Result<_, JobDefinitionCatalogSyncError>` small.
Downstream code matching those variants should dereference the boxed source
before matching the inner persistence error.

## PostgreSQL requirements

Runledger requires PostgreSQL 18 or later. PostgreSQL 18 is the authoritative
baseline for production support, diagnostics, reproductions, DB-backed tests,
migration verification, and SQLx metadata. In particular:

- Native `uuidv7()` support from PostgreSQL 18+ is required; adding an
  equivalent function to an older server does not make that server supported.
- Transactional DDL must support the baseline migration as written.
- The target database must be migrated before runtime code uses it.

## Working in this repository

### Build and test

```bash
cargo check
cargo test --workspace --no-run
cargo test -p runledger-core
cargo test -p runledger-postgres
cargo test -p runledger-runtime
cargo check -p runledger-tui
```

Tests fall into two categories:

- **Pure Rust unit tests** — no PostgreSQL required.
- **DB-backed tests** — use `runledger-test-support` and `testcontainers`. They
  start a shared PostgreSQL container, create an isolated ephemeral database per
  test, and apply the local Runledger migrations.

The packaged external-consumer smoke test packages `runledger-core`,
`runledger-test-support`, `runledger-postgres`, and `runledger-runtime`,
extracts the `.crate` archives, builds a standalone host crate against the
packaged manifests via `[patch.crates-io]` and its checked-in lockfile, then
runs migrations, starts the supervisor, enqueues jobs, and asserts terminal
states:

```bash
./scripts/run-external-consumer-smoke.sh
```

The default test image is `postgres:18`. `RUNLEDGER_TEST_PG_IMAGE` may select a
different PostgreSQL 18+ image for an explicit environment or compatibility
test, but an override does not change the supported baseline. Results from an
older major version are provisional until reproduced on PostgreSQL 18.

```bash
export RUNLEDGER_TEST_PG_IMAGE=postgres:18
```

### SQLx offline mode

The repo uses `sqlx::query!` and friends extensively, and builds offline:

- `.cargo/config.toml` sets `SQLX_OFFLINE=true`.
- The workspace-root `.sqlx/` directory is the source cache, generated by
  `cargo sqlx prepare --workspace`.
- Each publishable crate that uses checked macros also carries its own `.sqlx/`
  so `cargo publish` can verify the packaged tarball in isolation.

If you change SQL or the schema, refresh the cache before committing:

1. Bring up a PostgreSQL 18 database with the current migrations applied.
2. Point `DATABASE_URL` at it.
3. Run `./scripts/refresh-sqlx-cache.sh`.

The script regenerates the root `.sqlx/`, syncs it into
`runledger-postgres/.sqlx/` and `runledger-runtime/.sqlx/`, syncs the root
`migrations/` into `runledger-postgres/migrations/`, runs `cargo check
--workspace`, and confirms the publishable tarballs include their per-crate
cache. Do **not** update only the root `.sqlx/` — `cargo publish` verifies each
crate from its packaged tarball. If the cache and schema drift apart,
`cargo check` fails during macro expansion.

### Development conventions

- Keep contracts in `runledger-core`, runtime orchestration in
  `runledger-runtime`, and SQL/state-machine logic in `runledger-postgres`.
- Treat the migration set as the canonical persisted contract for queue and
  workflow behavior.
- When schema semantics change, update Rust types, SQL, tests, and `.sqlx`
  metadata together.
- The repo compiles offline, but DB-backed behavior still needs PostgreSQL 18+
  with the current migrations applied.

## Releasing

Prepare a release:

```bash
./scripts/prepare-release.sh 0.7.0
```

The preparation script starts from a clean working tree or resumes an existing
generated release diff whose manifests are already at the requested version.
It rejects changes outside the files it generates. The script bumps publishable
crate and root workspace dependency versions, refreshes the root and standalone
smoke lockfiles plus SQLx offline metadata, runs workspace tests and the locked
packaged smoke test, dry-runs `runledger-core`, packages the library crates, and
build-verifies the packaged `runledger-tui` binary. If publishing manually, run
`./scripts/refresh-sqlx-cache.sh` before publishing `runledger-postgres` or
`runledger-runtime` and commit any resulting `.sqlx/` changes.

After reviewing and committing the prepared diff:

```bash
./scripts/publish-release.sh 0.7.0
```

Before publishing any crate, the publish script confirms that the release tag
is absent locally and remotely, fetches the same-named remote branch and
requires it to be an ancestor of `HEAD`, and dry-runs the branch and tag push.
It then publishes crates in dependency order, dry-runs each once its workspace
dependencies are indexed, creates a `v0.7.0` tag, and atomically pushes the
current branch and tag. Set `PUBLISH_REMOTE` to override the git remote for the
final push.

Observable contract changes to call out in release notes for this line:

- `compare_and_requeue_job` adds a pool-owning typed recovery path, while
  `CompareAndRequeueJob::from_observed_job` derives exact recovery expectations
  from an observed job.
- Successful direct jobs can be replayed idempotently with compare-and-create
  APIs that preserve the source job and persist replay lineage.
- Continuation metrics, typed event-payload decoding, and TUI replay and
  continuation visibility support production rollout and diagnosis.
- Release 0.7.0 adds migration
  `202607190001_job_replays_and_continuation_metrics`; apply it before using
  successful replay or continuation-metrics APIs.

See [`CHANGELOG.md`](CHANGELOG.md) for the full history.

## Repository layout

```text
.
├── Cargo.toml                # workspace manifest
├── README.md
├── CHANGELOG.md
├── llms.txt                  # prompt-facing summary
├── migrations/               # canonical schema source
├── docs/                     # downstream agent guide and notes
├── scripts/                  # release, SQLx cache, and smoke-test scripts
├── smoke/                    # external-consumer smoke test crate
├── runledger-core/
├── runledger-postgres/
├── runledger-runtime/
├── runledger-tui/
└── runledger-test-support/
```

## License

The crates are published under the **MIT** license, as declared in each crate's
`Cargo.toml`. Note that no `LICENSE` file is currently checked in at the
repository root — add one to make the license explicit for the repository as a
whole.
