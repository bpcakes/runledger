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
- **Worker runtime** — a `Supervisor` that runs worker, durable intent
  promoter, scheduler, and reaper loops with lease-based ownership, lease
  expiry recovery, and graceful shutdown.
- **Workflow DAGs** — model dependent work declaratively. The engine validates
  the graph, enqueues root steps, releases dependents as prerequisites finish,
  and keeps run status coherent across cancellation and external gates.
- **Bounded continuation and replay** — continue successful work in resumable
  slices, recover canceled or dead-lettered direct jobs, replay successful
  direct jobs without mutating their history, and recover terminal workflows as
  new lineage-linked runs.
- **Durable coordination** — reusable active-workflow keys and lease-fenced
  single-permit execution resources coordinate work across workers and
  organizations.
- **Workflow results** — designate a result step, persist compact JSON output,
  and read or wait for it through a scoped workflow handle.
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
  - [Workflow results and handles](#workflow-results-and-handles)
  - [Handler-selected retry timing](#handler-selected-retry-timing)
  - [Bounded job continuation](#bounded-job-continuation)
  - [Active workflow keys](#active-workflow-keys)
  - [Durable execution resources](#durable-execution-resources)
  - [Workflow recovery](#workflow-recovery)
  - [Upgrade map for releases 0.6 through 0.12](#upgrade-map-for-releases-06-through-012)
  - [0.7 to 0.8 activation and rollback](#07-to-08-activation-and-rollback)
  - [Schedules](#schedules)
  - [Job definition catalog](#job-definition-catalog)
- [Examples](#examples)
- [Admin reads](#admin-reads)
- [Operator TUI](#operator-tui)
- [Configuration](#configuration)
- [Database schema and migrations](#database-schema-and-migrations)
- [Operational notes](#operational-notes)
- [Platform support](#platform-support)
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
| [`runledger-runtime`](runledger-runtime) | The async runtime: `Supervisor`, worker/intent-promoter/scheduler/reaper loops, the job catalog, the handler registry, and runtime configuration. |
| [`runledger-tui`](runledger-tui) | Read-only terminal UI for monitoring queue metrics, jobs, workflows, and definitions. |
| [`runledger-test-support`](runledger-test-support) | Published test utilities for ephemeral PostgreSQL databases and scoped environment overrides. |

`runledger-core`, `runledger-postgres`, and `runledger-runtime` are the
libraries you depend on. Keep the layering intact: contracts in `core`, runtime
orchestration in `runtime`, and SQL/state-machine logic in `postgres`.

## Installation

Add the libraries to your service:

```toml
[dependencies]
runledger-core = "0.12.0"
runledger-postgres = "0.12.0"
runledger-runtime = "0.12.0"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
sqlx = { version = "0.8.6", features = ["runtime-tokio", "postgres"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }

[dev-dependencies]
runledger-test-support = "0.12.0"
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

Run a producer and a worker as separate processes against the same PostgreSQL 18
database. This example prints a greeting, using one shared job identity and typed
payload. It needs only the dependencies above. For a new service, create the
following files under `src/bin/`, with the shared module at
`src/bin/shared/mod.rs` (so Cargo does not treat it as another binary).

Shared contract (`src/bin/shared/mod.rs`):

<!-- quick-start-source: runledger-runtime/examples/producer_worker/shared.rs -->
```rust
use runledger_core::jobs::JobType;
use runledger_postgres::jobs::JobEnqueue;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const GREETING_JOB: JobType<'static> = JobType::new("jobs.greeting.print");

#[derive(Serialize, Deserialize)]
pub struct Greeting {
    pub name: String,
}

pub fn request<'a>(payload: &'a Value, key: &'a str) -> JobEnqueue<'a> {
    JobEnqueue {
        job_type: GREETING_JOB,
        organization_id: None,
        payload,
        priority: None,
        max_attempts: None,
        timeout_seconds: None,
        next_run_at: None,
        idempotency_key: Some(key),
        stage: None,
    }
}
```

Worker (`src/bin/worker.rs`):

<!-- quick-start-source: runledger-runtime/examples/producer_worker/worker.rs -->
```rust
pub mod shared;

use std::time::Duration;

use runledger_core::jobs::{JobCompletion, JobContext, JobFailure, JobType};
use runledger_core::prelude::async_trait;
use runledger_runtime::{Supervisor, catalog::JobCatalog, registry::JobHandler};
use serde_json::Value;
use shared::{GREETING_JOB, Greeting};
use sqlx::postgres::PgPoolOptions;

struct PrintGreeting;

#[async_trait]
impl JobHandler for PrintGreeting {
    fn job_type(&self) -> JobType<'static> {
        GREETING_JOB
    }

    async fn execute(
        &self,
        _context: JobContext,
        payload: Value,
    ) -> Result<JobCompletion, JobFailure> {
        let greeting: Greeting = serde_json::from_value(payload)
            .map_err(|_| JobFailure::terminal("greeting.invalid_payload", "Expected a name."))?;
        println!("Hello, {}!", greeting.name);
        JobCompletion::success().progress(1, 1).map_err(|_| {
            JobFailure::terminal("greeting.invalid_progress", "Invalid completion counts.")
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPoolOptions::new()
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;
    // For a fresh database. Existing deployments must follow the migration runbook.
    runledger_postgres::migrate_after_idempotency_cutover(&pool).await?;
    let catalog = JobCatalog::new().handler(PrintGreeting);
    catalog.sync_definitions(&pool).await?;
    println!("worker ready; producers can now enqueue greetings");

    let supervisor = Supervisor::builder_from_env(&pool)?
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
    pool.close().await;
    shutdown_result?;
    Ok(())
}
```

Producer (`src/bin/producer.rs`):

<!-- quick-start-source: runledger-runtime/examples/producer_worker/producer.rs -->
```rust
pub mod shared;

use runledger_postgres::jobs::enqueue_job_tx;
use shared::{Greeting, request};
use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let name = std::env::args()
        .nth(1)
        .ok_or("usage: producer <name> <request-key>")?;
    let key = std::env::args().nth(2).ok_or("missing request-key")?;
    let pool = PgPoolOptions::new()
        .connect(&std::env::var("DATABASE_URL")?)
        .await?;
    runledger_postgres::ensure_schema_compatible_after_idempotency_cutover(&pool).await?;

    let payload = serde_json::to_value(Greeting { name })?;
    let mut tx = pool.begin().await?;
    // Persist application changes with this same transaction when needed.
    let job_id = enqueue_job_tx(&mut tx, &request(&payload, &key)).await?;
    tx.commit().await?;
    println!("enqueued {job_id}");
    pool.close().await;
    Ok(())
}
```

Start the worker first; it applies the schema to a fresh database and syncs the
job definition. Existing deployments should follow the
[migration runbook](#database-schema-and-migrations) before starting this worker.
Wait for `worker ready`, then submit from a second terminal using the same
`DATABASE_URL`:

```bash
# Terminal 1
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/runledger
cargo run --bin worker

# Terminal 2
export DATABASE_URL=postgres://postgres:postgres@localhost:5432/runledger
cargo run --bin producer -- Ada greeting:1
```

Inside this repository, use `cargo run -p runledger-runtime --example worker`
and `cargo run -p runledger-runtime --example producer -- Ada greeting:1` instead.
The worker prints `Hello, Ada!` and persists completion progress of 1/1. Press
Ctrl-C to drain and stop it. Execution is at least once, so even this print can
repeat after an interrupted attempt; real external effects need their own
idempotency protection.

The producer commits the enqueue before reporting success. Application writes
can share that transaction: rolling it back also removes the enqueue. Reuse the
same request key and payload to retry a submission; use a new key for new work.
A changed payload with the same key is an idempotency conflict. This direct
submission requires an enabled job definition. If the producer must commit
before worker registration, use the [durable transactional handoff](#durable-transactional-handoff).

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
| Commit application state and a future job request before its definition exists | `JobEnqueueIntent` and `record_job_enqueue_intent_tx` |
| Multi-step work with dependencies | `WorkflowDagBuilder` (simple DAGs), or `WorkflowRunEnqueueBuilder` / `WorkflowStepEnqueueBuilder` (advanced), then `enqueue_workflow_run` |
| Multi-step work with a durable JSON result | Declare a result step, enqueue with `enqueue_workflow_run_handle`, then call `WorkflowRunHandle::get_result` |
| Fan-out, fan-in, or ordered stages | `WorkflowDagBuilder::after_success` / `after_terminal` (or lower-level `depends_on_success` / `depends_on_terminal`) |
| Human/API approval or another external gate | External workflow steps and `complete_external_workflow_step` |
| Delayed or recurring entrypoint | `JobScheduleUpsert` and `upsert_job_schedule` (or catalog schedules) |
| Provider-directed retry lower bound | `JobFailure::retry_not_before_delay` or `retry_not_before` |
| More work after one successful bounded slice | `JobCompletion::continue_now` or `continue_after`; workflow steps must opt in |
| At most one active workflow for an application key | `WorkflowRunEnqueueBuilder::active_key` and `enqueue_or_get_active_workflow` |
| Mutual exclusion for jobs sharing one external resource | `enqueue_job_with_execution_resource` or `WorkflowStepEnqueueBuilder::execution_resource` |
| Cancel a pending or leased job | `cancel_job_with_scope` with an explicit `JobCancellationScope::{Global, Organization, Admin}` capability |
| Recover a canceled or dead-lettered direct job | `compare_and_requeue_job` with exact observed state |
| Intentionally repeat a successful direct job | `compare_and_replay_succeeded_job` |
| Recover a terminal workflow without rewriting history | `recover_workflow_run` |
| Worker process lifecycle | `runledger_runtime::Supervisor::run_until_shutdown` |
| Admin/status views | `runledger_postgres::jobs` read/list/count APIs, including `count_workflow_runs` |

### Durable transactional handoff

Use `record_job_enqueue_intent_tx` when an application mutation and its request
for background work must commit in the same PostgreSQL transaction, but the job
definition may not exist yet. Recording an intent does not read or lock
`job_definitions` and does not create a `job_queue` row. Every intent requires an
idempotency key; retrying with the same canonical request returns the existing
intent, while changing the payload or another enqueue field returns
`job.intent_idempotency_conflict`.

Concurrent transactions recording the same `(job_type, organization_id,
idempotency_key)` may wait for the transaction that first claimed the unique
key to commit or roll back. Include that wait in the caller-owned transaction's
lock ordering and timeout budget. Record the intent before any operation in the
same transaction that can lock a `job_queue` row; do not enqueue, recover, or
explicitly lock a job first. Queue retention uses the canonical intent-before-
job order, and a job-first recorder can create an inverse lock cycle with it.

```rust
let payload = serde_json::json!({"invoice_id": "invoice_123"});
let intent = runledger_postgres::jobs::JobEnqueueIntent::new(
    runledger_core::jobs::JobType::new("billing.invoice.capture"),
    &payload,
    "invoice:invoice_123:capture",
);

let mut tx = pool.begin().await?;
// Persist the application's business or audit mutation with this same `tx`.
let outcome = runledger_postgres::jobs::record_job_enqueue_intent_tx(
    &mut tx,
    &intent,
).await?;
if outcome.status == runledger_postgres::jobs::JobEnqueueIntentStatus::Conflicted {
    return Err("the existing durable handoff is conflicted".into());
}
tx.commit().await?;
```

The returned status is a point-in-time observation, not a promotion guarantee.
An existing intent may be promoted or become conflicted concurrently, including
while a caller-owned record transaction remains open. Treat an observed
`CONFLICTED` state as terminal, but continue monitoring pending age and
`conflicted_24h` after accepting a pending handoff.

A standard Runledger supervisor promotes pending intents only for its
registered handler types once their definitions are enabled. Promotion uses
ordinary enqueue semantics and creates the normal `ENQUEUED` audit event.
Missing, disabled, and unregistered types remain pending without consuming
attempts. A dedicated promoter loop runs independently of queue claiming and
execution permits. It waits on its polling cadence after a partial or empty
pass, but immediately continues after a full batch so backlog draining is not
artificially rate-limited. Custom runtime orchestration that calls
`run_worker_loop` directly must also run `run_intent_promoter_loop` when it uses
durable enqueue intents. Use `IntentPromoterConfig` with
`run_intent_promoter_loop_with_config` for an independent cadence, or configure
and disable the supervisor promoter through `SupervisorBuilder`. Promotion is
enabled by default so recorded intents cannot silently strand after a worker
upgrade: every enabled supervisor issues its own promotion query after each
partial or empty polling interval. Capacity plans should include that aggregate
idle query rate. Increase the independent promoter interval or disable
redundant promoter instances when needed, but keep at least one promoter whose
registry covers every job type that can receive intents.
Promoted work waits in the ordinary queue, where priority, scheduling, queue
metrics, and worker concurrency provide the normal backpressure.
Database-level promotion failures roll back only the affected queue/event
writes, leave that intent pending with bounded jittered exponential backoff,
and allow later intents in the batch to continue. Lookup/list records expose
`promotion_attempts`, `next_promotion_at`, `last_attempted_at`, and the sanitized
last error. One public promotion pass is capped at 24 rows to keep worst-case
failed-savepoint headroom
below PostgreSQL's cached-subtransaction threshold.
Each transaction remains capped at 24 rows; full transactions continue after a
shutdown check and cooperative task yield.
Database failures remain pending and retry indefinitely: Runledger must not
turn a long outage into silently lost work. Alert on pending age and
`promotion_attempts`; repair the rejecting database policy. A mismatch between
an intent's canonical snapshot and its redundant enqueue columns is likewise
deferred, so an operator can restore consistency and retry without replacing
the durable request. Runledger keeps
conflicted intents as immutable evidence; if replacement work is safe, the
application must submit it deliberately under a new idempotency key.
Query `get_job_enqueue_intent_metrics_with_scope` with a
`JobEnqueueIntentReadMetricsFilter` selecting the authorized `JobReadScope` to
alert on oldest pending age,
`retrying_count`, `max_promotion_attempts`, and increases in `conflicted_24h`.
The retry count and maximum attempt count describe only intents that are still
pending, so resolved promoted or conflicted history cannot inflate the active
backlog signal.
Its promoted and conflicted populations are limited to the reported 24-hour
window, so retained terminal history older than that window is not scanned
merely to emit zeroes. Full conflicted evidence remains available through the
intent read and list APIs.
Pending and conflicted rows remain idempotency/audit evidence and have no
generic delete or cancel API: correcting or abandoning unexecuted application
work requires an application-owned authorization and audit policy rather than
a library-wide data-loss operation. Retention is
application policy: use the bounded cutoff cleanup API after audit requirements
are met, and remediate conflicted rows explicitly rather than deleting them
automatically. A promoted intent retains its job with `ON DELETE RESTRICT`.
When purging selected `job_queue` rows, call
`delete_promoted_job_enqueue_intents_for_jobs_tx` for those exact job IDs first
and delete the jobs in the same transaction. Select candidate IDs without row
locks, then invoke the helper as the transaction's first lock-taking operation.
The transaction must use `READ COMMITTED`; stronger isolation levels return
`job.intent_retention_unsupported_isolation` before the helper acquires the
promotion fence.
It waits for active promotions, fences new promotions, deletes promoted-intent
links, then locks the selected jobs until the transaction finishes. This
canonical intent-before-job order composes with duplicate recorders without an
inverse lock cycle. Keep that transaction short and commit promptly. Each
promotion transaction is capped at twenty-five seconds. The
retention helper therefore waits up to thirty seconds for the exclusive fence,
then caps each job- or intent-row lock wait at five seconds and each statement
at thirty-five seconds. Stricter caller timeouts are preserved. A timeout or
deadlock aborts the transaction; roll back and retry the complete retention
transaction. A newly promoted intent may link to a much
older existing job, so independent time windows cannot guarantee the required
ordering. Deploy that
exact-ID retention path to every queue-retention caller before enabling intent
writers; workers may be upgraded earlier while no intents exist to promote.

Intent payloads and idempotency keys have the same trust boundary as queue
payloads and keys. Treat them as sensitive application data and do not log them
casually.

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
`WorkflowRunReadScope::Organization`, global workflows use `Global`, and
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
use runledger_core::jobs::StepKey;
use runledger_postgres::jobs::{
    CompleteExternalWorkflowStepInput, ExternalWorkflowStepTerminalOutcome,
};

let approval_output = serde_json::json!({ "approved_by": "ops" });

runledger_postgres::jobs::complete_external_workflow_step(
    &pool,
    &CompleteExternalWorkflowStepInput {
        workflow_run_id,
        organization_id: None,
        step_key: StepKey::new("approval"),
        outcome: ExternalWorkflowStepTerminalOutcome::Succeeded {
            output: Some(&approval_output),
        },
        status_reason: Some("approved"),
        last_error_code: None,
        last_error_message: None,
    },
)
.await?;
```

Only the `Succeeded` outcome can carry `output`. Retrying completion for an already
terminal external step is idempotent only when the terminal outcome,
`status_reason`, `last_error_code`, and `last_error_message` match; changed
metadata returns `workflow.external_step_conflicting_completion_retry`. For
successful completions, output must also match, or Runledger returns
`workflow.external_step_conflicting_output_retry`.

Breaking API note: `JobHandler::execute` returns
`Result<JobCompletion, JobFailure>`. The old stage-bearing `JobProgress`
completion type was removed; use `JobCompletion::success()` or
`JobCompletion::with_output(...)`. In-flight lifecycle writes now use
`JobRunningUpdate` with `mark_job_running` for the atomic `RUNNING`
transition, and `JobOrdinaryProgressUpdate` with
`update_job_ordinary_progress` for stage-free progress. The older
stage-bearing `JobProgressUpdate` and `update_job_progress` remain deprecated
compatibility APIs while callers migrate. Completion disposition and final
output are intentionally private; inspect them with `disposition()` /
`output()` and use constructors rather than struct literals.

### Live handler execution services

Implement `JobExecutionHandler` when a handler needs the runtime's deadline or
durable progress writes. Register `handler.into_job_handler()` with the usual
`JobRegistry` or `JobCatalog`. Existing `JobHandler` implementations continue
to work unchanged; the worker dispatches through the new default
`execute_with_services` method. See the compiling
[counter example](runledger-runtime/examples/checkpointed_counter.rs).

`JobExecution::deadline()` is the same monotonic deadline the worker enforces,
starting after the running transition succeeds. `remaining_budget()` includes
time spent awaiting progress writes. `remaining_work_budget(reserve)` subtracts
an application-selected reserve for its final checkpoint or cleanup and
saturates at zero. Runtime completion persistence happens after the handler
returns; this reserve does not impose a deadline on that persistence.

`checkpoint::<T>()` decodes the claimed resume snapshot; applications still
validate their checkpoint versions and domain invariants.
`persist_progress(JobExecutionUpdate { .. })` atomically commits ordinary
progress and a checkpoint using the exact live lease, without a queue reread
or caller-supplied worker/run/attempt arguments. `save_checkpoint(&value)`
serializes and commits only a checkpoint. Both operations must be awaited.
Omitted fields retain their durable values; successful writes do not mutate
the invocation's resume snapshot.

The handle borrows runtime services, so it cannot escape into a detached task.
`JobExecutionError` distinguishes lease loss, deadline expiry, persistence
failure, and invalid input, and converts to `JobFailure` for `?` propagation.
The worker stops polling a handler when its progress write discovers lease loss,
even if the handler ignores that error. Successful writes acknowledge commit;
an error or cancellation can leave an indeterminate commit outcome.
External effects still require application idempotency.

Custom runtimes must supply `JobExecutionServices` and invoke
`JobHandler::execute_with_services`. Calling legacy `execute` directly on
an adapted execution-services handler returns `job.execution_services_required`.
SQLx pools and persistence errors remain outside the serializable `JobContext`.
Validated [OneSales and IdentityPro migration patches](docs/execution-services-migrations/README.md)
show how to replace existing execution-state reconstruction.

### Handler-selected retry timing

When a provider supplies a dynamic reset time, a handler can attach either a
relative delay or an absolute UTC timestamp to a retryable failure:

```rust
use std::time::Duration;

let transport_failure = JobFailure::retryable(
    "provider.temporarily_unavailable",
    "Provider asked the client to retry later.",
)
.retry_not_before_delay(Duration::from_secs(30));

let rate_limit_failure = JobFailure::retryable(
    "provider.rate_limited",
    "Provider rate limit reached.",
)
.retry_not_before(provider_reset_at);
```

In 0.8, `JobFailure` gained private retry-timing state and can no longer be
constructed with a struct literal. Use `JobFailure::new`, `retryable`,
`terminal`, `timeout`, `lease_expired`, or `panicked`, then add a lower bound
with the methods above. The older `retry_after` and `retry_at` names are
deprecated because the requested time never overrides a later policy backoff.
Low-level persistence integrations must construct the now non-exhaustive
`JobFailureUpdate` with `JobFailureUpdate::new(...)` and optionally
`.with_retry_timing(...)`.

This is a failed attempt, not an attempt-neutral defer or a successful
continuation. It consumes the current run's attempt budget, keeps the same
`run_number`, and does not release workflow dependents. If the failure remains
retryable, Runledger computes ordinary policy backoff from the registered
job-type/failure-code override or exponential fallback, then schedules the later
of that policy time and the handler's lower bound. Terminal and panicked
failures, and failures that exhaust `max_attempts`, are dead-lettered without
applying or validating timing.

`retry_not_before_delay` is measured from the PostgreSQL completion clock; positive
sub-millisecond values round up to one millisecond. Zero and absolute times
before PostgreSQL's range supply no additional lower bound. A winning hint that
cannot be represented becomes the terminal `job.invalid_retry_timing` handler failure.
`retry_not_before` uses the supplied provider timestamp, rounded up only when
needed for PostgreSQL microsecond precision. A past hint cannot shorten the
ordinary policy delay. Future timestamps outside PostgreSQL's supported range
become the terminal `job.invalid_retry_timing` handler failure.

Retry attempts retain the policy delay in `retry_delay_ms` and record
`requested_retry_not_before`, `effective_next_run_at`, and
`retry_timing_source` (`POLICY` or `HANDLER_NOT_BEFORE`). The same audit fields
are written to the `RETRY_SCHEDULED` event, which retains
`requested_retry_at` and `next_run_at` as legacy aliases. Observer dispositions
report the committed effective schedule, while `JobFailure::retry_timing()`
remains the handler's request.

### Bounded job continuation

A handler that has successfully finished one bounded slice but still has more
work for the same logical job can return
`JobCompletion::continue_now()` or `JobCompletion::continue_after(delay)`.
Direct jobs may return this disposition immediately. Workflow job steps require
an explicit, persisted enqueue-time opt-in:

```rust
let step = WorkflowStepEnqueueBuilder::new(step_key, job_type, &payload)
    .allow_handler_continuation()
    .try_build()?;
```

The workflow-step default is `false`, and external steps cannot opt in. This
keeps rollout scoped and prevents an accidental handler continuation from
creating an indefinitely active workflow. Handlers that keep returning
terminal success or failure retain their existing behavior.
Progress and checkpoints can be carried into the next run with the existing
builders:

```rust
use std::time::Duration;

let completion = JobCompletion::continue_after(Duration::from_secs(5))
    .progress(processed, total)?
    .checkpoint(serde_json::json!({ "cursor": next_cursor }));
```

Progress construction rejects negative counts and `processed > total`. A
continuation delay is rounded up to microseconds for persistence and must fit
in signed 64-bit microseconds; the resulting PostgreSQL timestamp must also be
representable. The runtime terminally dead-letters an out-of-range delay with
`job.invalid_continuation_delay`.

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
For an opted-in workflow step, the same transaction returns the step from
`RUNNING` to `ENQUEUED`. The workflow run remains active and dependencies stay
blocked; only a later terminal success, failure, or cancellation releases
dependency edges and recomputes terminal workflow state. A workflow step
without the persisted opt-in still converts an accidental continuation into
the terminal `job.workflow_handler_continuation_not_enabled` handler failure.

A mixed 0.7/0.8 worker fleet is unsafe after continuation-enabled workflow
steps are emitted: a 0.7 worker can claim one and terminalize it when the
handler returns continuation. Deploy all workers, reapers, schedulers, and
administrative processes, then wait for old processes and leases to quiesce
before enabling a canary workflow job type.

Successful continuations deliberately have no implicit run cap and do not
consume the per-run `max_attempts` failure budget. The handler owns its terminal
condition; use a nonzero delay for polling-style work and do not return
`continue_now()` forever. Production handlers should version their checkpoint
shape, make every slice idempotent, enforce a logical deadline or run limit,
canary activation by job type or tenant, and alert on continuation rate and run
depth. `get_job_continuation_metrics_with_scope` takes an explicit
`JobReadScope` and returns a
`JobContinuationMetricsRecord` per job type with `continued_24h`,
`active_continued_count`, and `max_active_run_number` for canary and runaway-loop
alerts. Active counts include only jobs whose current run was created by a
handler continuation; a later admin recovery is not mislabeled as active
continuation. The packaged external-consumer
[`smoke` test](smoke/external-consumer/tests/smoke.rs) is a compile-checked
continuation, recovery, successful-replay, and metrics example; the
[`downstream agent guide`](docs/downstream-agent-guide.md) contains the full
adoption checklist and PostgreSQL 18 operational queries.

Lifecycle observers receive `on_job_continued(JobContinuedEvent)` after the
same run's `on_job_running` callback settles, with the completed run identity,
duration, next run number/time, and committed progress. A failed continuation
write is reported through `JobCompletionPersistFailedEvent` with
`JobCompletionPersistenceOperation::Continuation`. Observers are best-effort
and different run numbers may be observed concurrently or out of order;
correlate by `(job_id, run_number)` and use durable job events for authoritative
history.

### Active workflow keys

Use `WorkflowRunEnqueueBuilder::active_key(...)` with
`enqueue_or_get_active_workflow` when only one active cycle may exist in a
global or organization scope. Always match the explicit
`EnqueueActiveWorkflowOutcome`: `Inserted`, `ExistingActive`, or
`ExistingIdempotent`. Scope does not include workflow type: namespace active
keys by workflow type unless different workflow types should deliberately
coordinate, because `ExistingActive` may otherwise return the other type's run.
An active claim is durable and is not reusable until the prior workflow is
terminal and any canceled live lease has quiesced. Active keys must be
non-blank and at most 512 bytes. Deferred cancellation release is performed by
the lease reaper; if the reaper is disabled or stopped, the key remains
reserved until reaping resumes. `ExistingActive` can therefore carry a terminal
canceled run; treat the outcome, rather than terminal status alone, as the
reuse decision. `enqueue_workflow_run_handle` intentionally rejects active-key
payloads because a handle alone would discard that classification. After
matching the active enqueue outcome, create a handle with `workflow_run_handle`
using the returned run ID and matching scope.

### Durable execution resources

For one-permit concurrency across otherwise unrelated jobs, enqueue a direct
job with `enqueue_job_with_execution_resource` or configure a workflow job step
with `.execution_resource("provider-account:123")`. Resource reservation occurs
atomically before lease creation. Blocked jobs stay `PENDING` at attempt zero
and do not consume a returned worker claim slot. Ownership is fenced to the
exact run, attempt, worker, and lease, and releases on success, failure,
continuation, prestart claim release, reaping, or quiesced cancellation.
Resource keys must contain a non-whitespace character and are limited to 512
bytes. The direct-job API returns `JobEnqueueOutcome`; when the job also has an
`idempotency_key`, its execution resource is part of the canonical enqueue
request, so retrying that request with a different resource is a conflict.
Execution resource keys are global across organizations: namespace keys in the
application when tenants must not contend, and reuse a key across organizations
only when they intentionally share one external capacity limit. Keys guarantee
mutual exclusion, not global FIFO: a type-restricted worker selects the oldest
eligible job within its allowed types. Mixing filtered and unfiltered workers,
or workers with different type filters, can therefore reorder contenders for a
shared key without weakening mutual exclusion. A requested batch size is an
upper bound, not a fullness guarantee: concurrent workers that race for the
same keys can return short batches even while unrelated work exists. Each poll
examines a bounded resource-head window (1,024–16,384 eligible keyed jobs,
scaled by requested batch size), so an unusually dense same-key prefix can also
return a short batch instead of scanning an unbounded backlog. Resource inserts
use consistent key order to reduce cross-filter deadlock risk.
Exclusivity is lease-scoped: the reaper releases an expired owner when it
transitions the owning job, so provider-side operations must still be fenced or
idempotent in case a handler outlives its lease after heartbeat loss. Successful
direct-job replay preserves the source execution resource. If the reaper is
disabled or stopped, expired claims remain reserved until it resumes; cleanup
is bounded by the configured reaper batch limit. Lease transitions commit
before coordination-claim cleanup, so a cleanup failure cannot roll back
successfully reaped jobs. `ReapExpiredLeasesDetailedResult` reports released
active and resource claim counts plus typed cleanup errors; the runtime logs
failures and warns when either cleanup reaches its batch limit. Heartbeating a
resource owner also renews its durable claim, adding one keyed-row update per
heartbeat. Continuation releases the claim between slices, so the next slice
re-contends for the resource instead of retaining it across runs.

### Workflow recovery

`recover_workflow_run` never reopens terminal steps. It creates a distinct run
and a `workflow_recoveries` lineage row, reconstructing the DAG from the
source's canonical enqueue snapshot plus committed append history. Reusing the
same `(source_run_id, request_key)` with identical fields returns the existing
recovery; conflicting reuse fails. Recovery reacquires the source active key
and preserves per-step execution resources, handler-continuation opt-ins, and
the source's resolved priority, attempt limit, and timeout even if
job-definition defaults have changed. It uses each source step's latest
persisted payload, so an operator correction made through the pending-step
payload API is not replaced by the older canonical enqueue payload. The new
run does not reuse the source's permanent workflow idempotency key; the
recovery request key is its separate replay identity. Runs created before
canonical snapshots were available, snapshots with unknown fields, and
unsupported mutation kinds are rejected instead of being replayed ambiguously.
Recovery request keys must be non-blank and at most 512 bytes. Retention cannot
delete only a recovery run while its source remains, because doing so would
erase the request's idempotency guard; a source-led statement may delete the
complete lineage together. Every new workflow stores its canonical enqueue
snapshot, including step payloads, so budget for that additional JSON storage
and keep large artifacts behind references.

Construct the non-exhaustive request through its constructor. Omitting
`.organization_id(...)` means an exactly global source; `source_step_id` is
optional audit context and does not limit the full replay:

```rust
let request = WorkflowRecoveryRequest::new(
    source_run_id,
    recovery_request_key,
    WorkflowRecoveryMode::FullReplay,
    "operator-approved retry",
)
.organization_id(organization_id)
.source_step_id(failed_step_id);

let outcome = recover_workflow_run(&pool, &request).await?;
match outcome.disposition {
    WorkflowRecoveryDisposition::Inserted => { /* new recovery run */ }
    WorkflowRecoveryDisposition::Existing => { /* idempotent request retry */ }
    _ => { /* future disposition */ }
}
```

Use `recover_workflow_run_tx` only when recovery must compose with other
application writes; the caller-owned transaction must be `READ COMMITTED` and
the function neither commits nor rolls back it. A source must be terminal.
Recovery of an active-key workflow can remain blocked until its old claim is
quiescent, or while another run owns that key.

### Upgrade map for releases 0.6 through 0.12

The `0.12` release line includes the contracts introduced in the preceding
releases. When skipping versions, preserve each release's schema and
runtime fence:

| Release | Schema requirement | Activation requirement |
| --- | --- | --- |
| `0.6` | No new migration. | Deploy every job-state writer with continuation and typed recovery unused; wait for every pre-0.6 process and live lease to quiesce before activating either path. Migrate deprecated `requeue_job` callers to exact typed outcomes. |
| `0.7` | Apply `202607190001_job_replays_and_continuation_metrics` before replay or metrics callers. | Successful replay and continuation metrics are additive. Use Runledger's filtered migration/schema helpers for expand-first deployment and code rollback. |
| `0.8` | Apply `202607250001_harden_continuation_metrics_payload_validation` and `202607280001` through `202607280005` before any 0.8 runtime loop or persistence API runs. | Deploy every 0.8 writer with new paths unused, quiesce all older processes and leases, then canary workflow continuation, active keys, resources, retry hints, and workflow recovery. |
| `0.9` | No migration after 0.8.0. | Custom runtimes may adopt `JobLeaseIdentity` and its `_for_lease` lifecycle APIs without a coordinated schema or source migration; the positional functions remain available. |
| `0.10` | Apply `202608180001_job_enqueue_intents` before any process records or promotes enqueue intents. | Deploy exact-ID retention cleanup to every queue-retention caller before enabling intent writers. Keep at least one promoter for every intent type, and budget for each enabled supervisor's independent idle polling. |
| `0.11` | Apply `202608240001_expand_workflow_step_job_link`, deploy 0.11, drain every 0.10 writer and lease, then apply `202608240002_contract_workflow_step_job_link`. | Migrate every removed `requeue_job` call before compiling 0.11: use exact-scope compare-and-requeue for canceled/dead-lettered jobs and fresh-job successful replay for `SUCCEEDED`. The contract migration is the 0.10 rollback boundary. |
| `0.12` | No migration after 0.11.0. | Update exhaustive `QueryErrorKind` matches for `PostgresLockNotAvailable`. Runtime workers now bound and retry contended heartbeats within the lease-maintenance budget; complete the 0.11 workflow-link rollout before deployment. |

For 0.8 source upgrades, construct `WorkflowDagStepValidationInput` with
`WorkflowDagStepValidationInput::new(...)` and its option setters; it is now
non-exhaustive and includes handler-continuation and execution-resource
settings. `WorkflowStepDbRecord` exposes the matching persisted fields, so
direct struct-literal consumers must update. Construct the non-exhaustive
workflow recovery request through `WorkflowRecoveryRequest::new(...)` as shown
above, and keep wildcard arms when matching non-exhaustive recovery outcomes.

The next section summarizes the current transition. The
[downstream activation and rollback runbook](docs/downstream-agent-guide.md#07-to-08-activation-and-rollback-runbook)
contains the PostgreSQL 18 gates and the earlier direct-job recovery migration
details.

### 0.7 to 0.8 activation and rollback

The 0.8 features use additive migrations and require a two-phase rollout:

1. Apply `202607250001_harden_continuation_metrics_payload_validation` and the
   migrations through `202607280005_workflow_recoveries`. Deploy 0.8 with
   workflow continuation emission, active-key enqueue, execution resources,
   retry hints, and recovery calls disabled to every process that can
   participate in job lifecycle state: workers, reapers, and admin, API, CLI,
   or repair processes that cancel or requeue jobs. Keep those paths unused
   until every 0.7 process has stopped and old leases have quiesced. If an
   upgrade skips releases, this means every pre-0.8 process.
2. Enable opted-in workflow continuation and the new enqueue/recovery paths by
   canary job type or tenant.

After resource-constrained jobs are emitted, a PostgreSQL trigger rejects any
lease that lacks the exact durable resource claim. A 0.7 worker therefore fails
loudly instead of silently violating mutual exclusion, but it can repeatedly
roll back claim batches that encounter constrained work. The activation fence
above remains mandatory for availability as well as protocol compatibility.

Before rollback, disable new 0.8 writes, drain continuation-created and
resource-constrained work, wait for retained cancellation leases to quiesce,
then stop all 0.8 writers before starting 0.7 processes. Starting a 0.7 worker
while any resource-constrained job remains `PENDING` or `LEASED` causes its
lease transaction to fail at the database fence.

Leave the additive 0.8 schema applied when the rollback binary uses
Runledger's filtered startup helpers. A raw 0.7 `MIGRATOR.run(...)` rejects the
newer SQLx history and requires the destructive down-migration path documented
in the full runbook.

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
    .handler(RefreshHandler)
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

Register a schedule's `.handler(...)` before its `.schedule(...)` — schedule
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

Producer processes can share provider-free `JobSpec` / `JobContract` definitions
with workers. Use `JobCatalog::from_specs` to validate complete worker bindings,
`TypedJobHandler` for opt-in payload decoding, and `JobContract::submit` for typed
direct requests. The pool-owning `enqueue_job_with_outcome` distinguishes new
work from identical retries. See the [shared contracts guide](docs/shared-job-specs.md)
for synchronization, wire compatibility, and migration examples.

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

Override individual definitions with `handler_with_definition_overrides` /
`definition_overrides`:

```rust
let catalog = JobCatalog::new()
    .handler_with_definition_overrides(
        ExtractDocuments,
        JobCatalogDefinitionOverrides::new()
            .timeout_seconds(600)
            .priority(20),
    )
    .handler_with_definition_overrides(
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

These examples and integration references are compile-checked:

- [Enqueue one job](runledger-postgres/examples/enqueue_job.rs)
- [Workflow DAG (fan-out / fan-in)](runledger-postgres/examples/workflow_dag.rs)
- [External workflow gate](runledger-postgres/examples/external_gate.rs)
- [Append workflow steps](runledger-postgres/examples/append_workflow_steps.rs)
- [Scheduled job entrypoint](runledger-postgres/examples/schedule_job.rs)
- [Shared producer/worker quick start](runledger-runtime/examples/producer_worker/)
- [Worker binary skeleton](runledger-runtime/examples/worker_binary.rs)
- [Packaged continuation, retry timing, direct recovery, replay, and metrics smoke test](smoke/external-consumer/tests/smoke.rs)
- [Active-workflow key integration reference](runledger-postgres/tests/workflow_active_claims.rs)
- [Execution-resource integration reference](runledger-postgres/tests/job_execution_resources.rs)
- [Workflow-recovery integration reference](runledger-postgres/tests/workflow_recovery.rs)

## Admin reads

The `runledger_postgres::jobs` admin surface exposes job/workflow detail, list,
and count helpers for operator UIs and service-owned dashboards. Use
`list_workflow_runs_with_scope` with `WorkflowRunReadListFilter` when rendering
workflow tables, and `count_workflow_runs_with_scope` with
`WorkflowRunReadCountFilter` for status counters such as failed workflows or
runs waiting for external completion. Set `WorkflowRunReadScope::Global` for
exact global rows, `Organization(id)` for one tenant, or `Admin` only for a
trusted all-tenant surface. The legacy nullable read helpers remain available:
their `None` scope retains the historical admin wildcard. These helpers use the
same workflow-type substring filtering as the TUI.

Use `get_job_metrics_with_scope` and `get_job_continuation_metrics_with_scope`
with `JobReadScope::{Global, Organization, Admin}` for queue counters and
continuation canaries. Registered job types remain visible with zero counts
when the selected scope has no matching rows. Job duration metrics retain the
average of per-scope percentiles when aggregating scopes. Each
`JobContinuationMetricsRecord` reports the prior 24 hours' successful
continuations, the number of pending/leased jobs whose current run was created
by continuation, and the highest current run number among those active jobs.

Use `get_job_enqueue_intent_metrics_with_scope` with
`JobEnqueueIntentReadMetricsFilter::new(scope, limit, offset)` for intent metrics.
Its optional `with_job_type` filter is exact. Backlog, retries, and oldest age
include pending intents only; promoted and conflicted counts cover the last
24 hours. Types with only older terminal history are omitted. Results are
ordered by job type, with limits of 1–1000 and nonnegative offsets.
All three legacy metric APIs retain `None` (or no organization filter) as
all scopes and `Some(id)` as exactly that tenant.

For payload reads, use `get_job_payload_by_idempotency_key_with_scope` or
`get_latest_job_payload_for_run_with_scope` with `JobScope::Global` or
`JobScope::Organization(id)`. Keys and JSON `run_id` values may repeat across
scopes, so these single-result lookups have no admin wildcard. The latest
lookup orders by `created_at DESC, id DESC`. Both return `None` for an absent
match; nil UUIDs are ordinary UUID values, not global/admin sentinels. Their
legacy counterparts still require a tenant UUID. These APIs and the new intent
filter are exported through both `jobs` and `prelude`. Applications must
authorize every selected read scope, including exact payload scopes.

Use `JobReadScope::Global` for jobs and intents with no organization,
`JobReadScope::Organization(id)` for one tenant, and `JobReadScope::Admin`
for visibility across all tenants and global rows. Applications must authorize
the selected scope; the enum only controls row filtering.

Use `get_job_by_id_with_scope`, `list_jobs_with_scope` (with
`JobReadListFilter`), `list_job_events_with_scope`, and
`list_job_logs_with_scope` for job inspection. Intent inspection uses
`get_job_enqueue_intent_by_id_with_scope` and
`list_job_enqueue_intents_with_scope` (with
`JobEnqueueIntentReadListFilter::new(scope, limit, offset)`).
The legacy APIs retain their existing behavior: an absent organization filter
means unrestricted visibility, including organization-owned rows.

Durable event consumers should call `list_job_events_with_scope` and prefer
`JobEventRecord::decoded_payload()` for Runledger-authored continuation,
administrative requeue, and successful-replay payloads. The decoded enums are
non-exhaustive: keep wildcard arms and retain `JobEventRecord::payload` as the
raw fallback for historical, malformed, custom, or future shapes instead of
hand-parsing `requeue_kind` or replay lineage fields.

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

By default it uses an unfiltered **global admin** scope, so rows from all
organizations and rows without an organization are visible. Passing no
organization filter is not the same as filtering stored rows to
`organization_id IS NULL`. Pass `--org <uuid>` at startup, or press `o` at
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

`Supervisor::builder_from_env()` reads the complete standard runtime
configuration from the environment. Worker settings come from
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

The environment-aware supervisor inherits ordinary worker polling settings for
intent promotion unless either intent-specific variable is set:

| Variable | Purpose |
| --- | --- |
| `JOBS_INTENT_PROMOTER_POLL_INTERVAL_MS` | Delay after partial or empty promotion passes |
| `JOBS_INTENT_PROMOTER_BATCH_SIZE` | Intents requested per promotion transaction; storage caps each pass at 24 |

Intent promotion is enabled by default on every worker-enabled supervisor, so
an idle deployment runs one non-locking eligibility query per supervisor per
configured promoter interval; it does not open a transaction or acquire the
retention fence unless eligible work is visible. Tune the intent-specific
interval or call `disable_intent_promoter` on redundant instances to fit the
database query budget. Do not disable every compatible promoter while
applications can record intents; that would leave accepted work pending
indefinitely.

Code that already owns a `JobsConfig` should continue to use
`Supervisor::builder`; it deliberately derives promoter settings from that
explicit config and does not inspect the environment. For custom composition,
pass `IntentPromoterConfig::from_env()` to
`SupervisorBuilder::with_intent_promoter_config`.

## Database schema and migrations

The schema is limited to Runledger-owned objects:

- **Queue and lifecycle:** `job_definitions`, `job_queue`,
  `job_enqueue_intents`, `job_attempts`,
  `job_events`, `job_dead_letters`, `job_schedules`,
  `job_execution_resource_claims`
- **Workflow orchestration:** `workflow_runs`, `workflow_steps`,
  `workflow_step_dependencies`, `workflow_run_mutations`,
  `workflow_active_claims`, `workflow_recoveries`
- **Operational support:** `job_logs`, `job_runtime_configs`, `job_replays`
- **Derived views:** `job_metrics_rollup`, `job_continuation_metrics_rollup`

Notable features: idempotent queueing via `idempotency_key`, cron-backed
schedule materialization, workflow DAG execution with dependency counters,
external gates via `WAITING_FOR_EXTERNAL`, append-only workflow mutation
tracking, handler continuation and retry audit data, active-workflow and
execution-resource claims, immutable replay/recovery lineage, and panic-aware
metrics rollups.

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
  during the 0.8 rollout.
- `202607280001_workflow_step_handler_continuation` — persists the explicit
  per-step handler-continuation opt-in and prevents external steps from enabling
  it.
- `202607280002_workflow_active_claims` — adds reusable global or
  organization-scoped active keys, terminal release-pending tracking, and the
  bounded cleanup index used by the reaper.
- `202607280003_handler_retry_not_before_audit` — records requested handler
  not-before bounds, effective retry times, and whether policy or the handler
  selected the committed schedule.
- `202607280004_job_execution_resources` — adds resource keys to direct jobs
  and workflow steps, durable lease-fenced resource claims, claim-order indexes,
  and database triggers that enforce and release exact ownership.
- `202607280005_workflow_recoveries` — identifies append mutation records and
  adds immutable workflow-recovery lineage plus request idempotency.
- `202608180001_job_enqueue_intents` — adds durable, strictly idempotent
  transactional handoff records, promotion/conflict state, backlog indexes,
  and promoted-row retention support. It is additive and intentionally remains
  outside the custom compatibility fence during the expand-first rollout.
- `202608240001_expand_workflow_step_job_link` — audits and backfills reciprocal
  workflow-step/job links, then makes `workflow_steps.job_id` authoritative
  while maintaining `job_queue.workflow_step_id` as a trigger-backed projection
  for mixed-version writers and readers. It is intentionally outside the custom
  compatibility fence during the rolling-deployment window.
- `202608240002_contract_workflow_step_job_link` — re-runs both relationship
  anti-joins, removes the compatibility triggers, reciprocal FK/unique
  constraint, and `job_queue.workflow_step_id`, then advances the custom
  compatibility fence so pre-contract binaries refuse the destructive schema.

Every forward migration from
`202607190001_job_replays_and_continuation_metrics` through
`202608240001_expand_workflow_step_job_link` is recorded and checksum-validated in
`_sqlx_migrations` but deliberately omitted from the custom
`runledger_migration_history` compatibility fence. This lets released filtered
startup helpers coexist during the documented expand-first windows; it does
not make a raw migrator from an older crate tolerate unknown SQLx history rows.

The workflow-step/job cutover is deliberately staged for rolling deployments:

1. Apply migrations through `202608240001_expand_workflow_step_job_link` with
   externally managed DDL. Keep both relationship anti-joins empty.
2. Roll every application instance to code that reads and writes ownership only
   through `workflow_steps.job_id`. The expand triggers keep the deprecated
   projection usable by any not-yet-replaced instance.
3. After the old instances are drained, apply
   `202608240002_contract_workflow_step_job_link`. This step takes exclusive
   locks on `job_queue` and `workflow_steps`, removes the projection, and is the
   breaking rollback boundary. Do not start a pre-contract binary afterward.

`migrate_after_idempotency_cutover` applies the complete bundled set, including
the contract migration. Deployments that need a mixed-version window must use
externally managed DDL plus `ensure_schema_compatible_after_idempotency_cutover`
during the expand phase, then apply the contract explicitly after the drain.

Treat the flattened baseline as a from-scratch schema definition, not an
in-place upgrade from the older multi-file standalone history; apply later
forward migrations normally. The workspace-root `migrations/` directory is the
canonical source for development and review.

### Migration identity and bundle manifest

`runledger_postgres::migration_bundle()` inspects the compiled crate without
opening a database or reading workspace files:

```rust
let bundle = runledger_postgres::migration_bundle();
let library_version = bundle.library_version(); // also RUNLEDGER_POSTGRES_VERSION
let content: [u8; 32] = bundle.bundle_fingerprint();
let pipeline: [u8; 32] = bundle.pipeline_fingerprint();
for migration in bundle.migrations() {
    // version, description, migration_type, checksum, no_tx, and exact SQL
    println!("{} {}", migration.version, migration.description);
}
```

The manifest includes up and down entries, sorted by version then direction
(Simple, ReversibleUp, ReversibleDown). The content fingerprint hashes their
metadata and raw SQLx checksums. The pipeline fingerprint additionally includes
the compiled `runledger-postgres` version, so a new library release invalidates
cached templates even when its SQL is unchanged. Rustdoc specifies the versioned,
length-framed SHA-256 encoding; fingerprints are 32 raw bytes, not hex strings.

Use the pipeline fingerprint as one input to your application's template/schema
fingerprint. Retain the host pipeline revision, other libraries' inputs, and host
migration ordering. Helper-only changes in same-version path/patched builds
require an additional host-owned source revision. Neither fingerprint proves a
live database is compatible or includes your application's SQLx history.

The [composition example](runledger-postgres/examples/migration_identity.rs)
shows how to combine these inputs. The [consumer guide](docs/migration-identity/README.md)
includes an IdentityPro adapter patch and validation against HOCR's historical
vendored SQL. Keep application migration ordering and cutover decisions in the
application, and use the startup helpers below to apply or validate live state.

### Applying or validating the schema

Two supported startup modes:

- `migrate_after_idempotency_cutover(&pool)` — applies the bundled schema and
  rejects keyed legacy rows without enqueue snapshots.
- `ensure_schema_compatible_after_idempotency_cutover(&pool)` — read-only
  validation that an existing `_sqlx_migrations` history matches the bundled
  migrations, with explicit errors for missing history, incompatible history,
  legacy idempotency rows, invalid expand-window triggers, or PostgreSQL
  query/connectivity failures. Trigger failures identify the expected public
  table and trigger plus typed problems such as missing function wiring,
  disabled origin writes, or incorrect constraint deferral.
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

Release 0.8 requires the complete migration set through
`202607280005_workflow_recoveries` before any 0.8 runtime loop or persistence
API runs. `migrate_after_idempotency_cutover` may apply it during process
startup before those paths begin. In particular,
`202607190001_job_replays_and_continuation_metrics` is required before replay
or continuation-metrics calls, and
`202607250001_harden_continuation_metrics_payload_validation` supplies the
corrected metrics contract. The five `20260728000*` migrations supply columns,
tables, triggers, and constraints referenced by 0.8 runtime paths.

For a 0.8-to-0.7 code rollback, choose one migration-history strategy only
after completing the runtime drain in the
[activation and rollback runbook](docs/downstream-agent-guide.md#07-to-08-activation-and-rollback-runbook):

1. Recommended: leave every 0.8 migration applied and start the 0.7 binary with
   `migrate_after_idempotency_cutover` or
   `ensure_schema_compatible_after_idempotency_cutover`. These Runledger paths
   filter SQLx history to migrations embedded in that release. Patch the
   rollback binary first if it calls raw `MIGRATOR.run(...)`.
2. A raw 0.7 `MIGRATOR.run(...)` rejects
   `202607250001_harden_continuation_metrics_payload_validation` and the five
   `20260728000*` rows because they are absent from its bundle. SQLx 0.8 may
   leave that failed session's advisory migration lock held, so close the
   disposable connection or pool rather than retrying it. If startup cannot be
   patched, use the 0.8 artifact to revert those six migrations in reverse
   order before starting 0.7.

The raw down-migration path discards 0.8 state: workflow-recovery lineage and
request idempotency, persisted active claims, execution-resource keys and
claims, retry-timing audit columns, and workflow-step continuation opt-ins.
The workflow/job rows themselves remain, which can leave recovery-created runs
without lineage and erase resource constraints from retained work. Reverting
further to a pre-0.7 raw bundle also requires reverting
`202607190001_job_replays_and_continuation_metrics`; that additionally deletes
relational successful-replay lineage and replay-request idempotency while
leaving replay-created queue rows and their lineage-bearing `ENQUEUED` events.
Use either destructive path only with explicit acceptance of those losses.

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
  `QueryError::internal_message()` for server-side diagnostics. Branch on
  `QueryError::kind()` only for the small, compile-checked set of cross-crate
  runtime policy decisions represented by `QueryErrorKind`; application and
  protocol handling should normally use the stable string returned by
  `QueryError::code()`.
- **Lease ownership.** Worker lifecycle updates reject expired leases with the
  stable `job.lease_owner_mismatch` code, even when the lease was lost by time
  rather than to another worker. Once `lease_expires_at` passes there is no
  owner grace period for heartbeat/progress/success/failure/continuation writes.
  The built-in runtime continues polling handlers while a heartbeat is waiting
  on a concurrent progress write. It schedules and bounds each complete
  heartbeat attempt at one third of the configured lease TTL, including pool
  acquisition, so directly configured one- and two-second leases retain time
  to stop handler polling before ownership expires. Heartbeat and progress
  transactions also cap job-row lock waits at five seconds and total
  transaction lifetime at thirty seconds. Handler completion applies the
  five-second cap only while acquiring its initial `job_queue` row, then
  restores the embedding service's lock policy before atomic workflow
  propagation; it does not impose a library transaction deadline on workflow
  size. Stricter caller settings remain in force throughout. If a heartbeat
  still cannot be persisted, the runtime stops the handler and reports lease
  loss rather than continuing without durable ownership. The live lease remains
  for ordinary reaper retry or dead-letter recovery, and an attempt that reached
  `RUNNING` remains consumed.
  Release 0.9.0 added `JobLeaseIdentity` for typed lifecycle lease fencing.
  In 0.11.0 and later, use `mark_job_running_for_lease` with
  `JobRunningUpdate` to commit `RUNNING` and its initial checkpoint/progress
  atomically, then use `update_job_ordinary_progress_for_lease` with
  `JobOrdinaryProgressUpdate` for stage-free progress. Reuse one identity
  derived from the claimed row and worker ID so lifecycle lease fences cannot
  be mixed across jobs. The older stage-bearing
  `update_job_progress_for_lease` remains a deprecated compatibility wrapper.
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
  Version 0.11 removes the deprecated pool-owning `requeue_job` API. Its
  `organization_id: None` was an unconstrained lookup, not `JobScope::Global`,
  so migration requires observing and authorizing the row's exact scope.
  `ResetProgressAndCheckpoint` matches its old state-reset behavior; callers
  must also handle `NotFound`, `ExpectationMismatch`, and
  `CancellationNotQuiesced` as normal no-mutation outcomes. The typed recovery
  API intentionally does not accept `SUCCEEDED`; use successful replay below.
- **Successful replay.** `compare_and_replay_succeeded_job` creates an
  idempotent fresh job from an exactly scoped successful direct-job run;
  `compare_and_replay_succeeded_job_tx` composes the same operation with a
  caller-owned `READ COMMITTED` transaction. The required
  `replay_request_key` identifies one replay action and the required reason is
  audited. Keys must be non-blank and at most 512 bytes, and reasons must be
  non-blank. Reusing the same source run, key, and reason returns the existing
  replay; reusing the key with a different reason returns
  `job.replay_idempotency_conflict`. The source row and output remain unchanged.
  The replay starts at run one with a new ID, copied payload/effective execution
  settings including any execution resource, no copied
  progress/checkpoint/output/original idempotency key, and lineage in
  `job_replays` plus its `ENQUEUED` event. Inspect
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

## Platform support

Runledger supports Unix-like operating systems only. Windows is not supported,
tested, or accepted as a compatibility target. The workspace and its published
crates may rely on Unix process and filesystem APIs without conditional Windows
implementations.

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
- **DB-backed tests** — use `runledger-test-support` and `testcontainers`. Each
  Rust test process starts one shared PostgreSQL container, creates an isolated
  ephemeral database per test, and applies the local Runledger migrations.

When a compatible Docker CLI is available, the shared-container harness starts
a separate process-liveness reaper and removes the container and its anonymous
volumes after normal process exit or abrupt test process termination. Set
`RUNLEDGER_TEST_DOCKER_CLI` to an alternate CLI path when `docker` is not on
`PATH`. If the optional CLI is unavailable, DB-backed tests still run through
the configured Testcontainers daemon and emit a warning that the additional
process-liveness cleanup is disabled. `TESTCONTAINERS_COMMAND=keep`
deliberately disables cleanup for debugging. Supplying
`RUNLEDGER_TEST_ADMIN_DATABASE_URL` uses that external PostgreSQL instance and
does not start a container or reaper.

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

The script prints `server_version` and `server_version_num` and refuses a
server other than PostgreSQL 18 or one with pending Runledger migrations. It
then regenerates the root `.sqlx/`, syncs it into
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
./scripts/prepare-release.sh 0.12.0
```

The preparation script starts from a clean working tree or resumes an existing
generated release diff whose manifests are already at the requested version.
It rejects changes outside the files it generates. The script bumps publishable
crates through their shared workspace package version, updates the explicit
published workspace dependency pins and README installation/release versions, refreshes the root and standalone smoke
lockfiles plus SQLx offline metadata, runs workspace tests and the locked
packaged smoke test, dry-runs `runledger-core`, packages the library crates,
and build-verifies the packaged `runledger-tui` binary. It also verifies that
every crate archive contains the repository license. If publishing manually,
run `./scripts/refresh-sqlx-cache.sh` before publishing `runledger-postgres`
or `runledger-runtime` and commit any resulting `.sqlx/` changes.

`python3 scripts/check-readme.py` (Python 3.11+) checks current installation and release command
versions against `Cargo.toml` and checks the quick-start snippets against the
compiled example sources. CI and both release scripts run this check; historical
upgrade notes retain their original versions. The PostgreSQL example test runs
with `cargo test -p runledger-runtime --example worker`.

After reviewing and committing the prepared diff:

```bash
./scripts/publish-release.sh 0.12.0
```

Before publishing any crate, the publish script confirms that the release tag
is absent from the selected remote, requires the same-named remote branch to
point at the exact local commit, and verifies that commit's completed GitHub
Actions `CI` run and every job succeeded. It then dry-runs the branch and tag
push, publishes crates in dependency order, and dry-runs each once its
workspace dependencies are indexed. Finally, it atomically pushes `HEAD` as
both the current branch and remote `v0.12.0` tag, then creates or reconciles the
local lightweight tag. A local-only tag left by an older failed release does
not block a retry. The publication preflight requires an authenticated GitHub
CLI. Set `PUBLISH_REMOTE` to override the git remote for the final push.

Observable contract changes to call out in release notes for this line:

- `QueryErrorKind` adds the `PostgresLockNotAvailable` variant. Update
  exhaustive downstream matches before compiling 0.12.
- Runtime heartbeats remain cancellation-safe while progress owns the same job
  row, retry transient PostgreSQL lock contention within a fixed lease-aware
  deadline, and stop handler polling before lease ownership expires.
- No new database migration is required after 0.11. Complete the 0.11
  workflow-step/job-link expand/drain/contract rollout before deploying 0.12.

See [`CHANGELOG.md`](CHANGELOG.md) for the full history.

## Repository layout

```text
.
├── Cargo.toml                # workspace manifest
├── README.md
├── LICENSE
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
`Cargo.toml`. See [`LICENSE`](LICENSE) for the repository license text.
