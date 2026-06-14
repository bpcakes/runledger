# Downstream Agent Guide

This guide is for agents integrating Runledger into another application. It is
not an instruction file for agents maintaining this repository.

## Choose The Highest-Level API

Use the highest-level Runledger API that matches the shape of the work before
composing lower-level primitives.

Common integration imports:

```rust
use runledger_core::prelude::*;
use runledger_postgres::prelude::*;
use runledger_runtime::prelude::*;
```

The preludes avoid generic `Result` and `Error` aliases so they can be imported
together.

| Need | Use |
| --- | --- |
| One independent retried unit of work | `runledger_postgres::jobs::enqueue_job` |
| Multi-step work with dependencies | Workflow DAG APIs |
| Multi-step work with a durable JSON result | Workflow result-step and handle APIs |
| Fan-out, fan-in, or ordered stages | Workflow DAG APIs |
| Human/API approval or another external gate | External workflow steps |
| Delayed or recurring entrypoint | `runledger_postgres::jobs::upsert_job_schedule` |
| Worker process lifecycle | `runledger_runtime::Supervisor::run_until_shutdown` |
| Admin/status views | `runledger_postgres::jobs` read/list/count APIs |

Use `runledger_postgres::jobs::set_job_schedule_active` to pause or resume a
schedule, and `runledger_postgres::jobs::set_job_schedule_next_fire_at` to
retime the schedule cursor.

Use `runledger_postgres::jobs::count_workflow_runs` with
`WorkflowRunCountFilter` for workflow status counters instead of fetching pages
of runs just to count them.

## Workflow DAG Rule

If the task requires step dependencies, build a workflow DAG:

1. Prefer `WorkflowDagBuilder` with `.job(...)`, `.after_success(...)`, and `.build()`.
2. Use `WorkflowStepEnqueueBuilder` and `WorkflowRunEnqueueBuilder` for advanced per-step
   settings, external steps, hand-authored dependency specs, or call sites that
   pass explicit `StepKey` and `JobType` values.
3. Persist the run with `runledger_postgres::jobs::enqueue_workflow_run`.

If callers need a durable workflow result, declare one DAG step as the
result step with `WorkflowDagBuilder::result_step(...)` or
`WorkflowRunEnqueueBuilder::try_result_step_key(...)`, enqueue with
`runledger_postgres::jobs::enqueue_workflow_run_handle`, then use
`WorkflowRunHandle::get_status`, `get_run`, or `get_result` depending on whether
the caller needs a status probe, scoped run record, or durable result. Job
handlers return `JobCompletion`; use `JobCompletion::with_output(...)` for
compact JSON result metadata. If the run succeeds but the declared result step
stores no output, `get_result` returns `WorkflowRunHandleError::ResultMissing`.
Its stable code is `workflow.result_missing`. Other handle error codes include
`workflow.handle_storage_error`, `workflow.run_not_found`,
`workflow.result_not_declared`, `workflow.result_unsuccessful_terminal`, and
`workflow.result_wait_timeout`. `WorkflowRunWaitOptions::default()` waits up to
five minutes; set `timeout: None` only when the caller intentionally wants to
wait indefinitely and can afford the pending PostgreSQL listener connection.

For external workflow steps, `CompleteExternalWorkflowStepInput::output` can be
set only when `terminal_status` is `WorkflowStepStatus::Succeeded`. Failed or
canceled external completions must leave output unset. Repeating an already
terminal external completion is idempotent only when terminal status,
`status_reason`, `last_error_code`, and `last_error_message` match. Changed
metadata returns `workflow.external_step_conflicting_completion_retry`; for
successful completions, changed output returns
`workflow.external_step_conflicting_output_retry`.

`WorkflowDagBuilder` validates workflow shape, not job registration. `.job(...)`
rejects blank identifiers and duplicate step keys immediately, but it does not
prove that a job type has a registered definition or runtime handler.
`.after_success(...)` and `.after_terminal(...)` reject blank identifiers and an
unknown target step immediately. Missing prerequisite steps, self-dependencies,
duplicate dependencies, cycles, blank workflow type from `new(...)`, blank
idempotency keys, and empty step lists fail when `.build()` / `.try_build()` is
called.

Do not recreate ordinary workflow orchestration by polling job status,
enqueueing dependent jobs from parent handlers, storing dependency state in
payload JSON, or adding app-owned workflow edge tables. Use those approaches
only when the task explicitly requires a custom orchestrator outside
Runledger's workflow model.

Do not call `requeue_job` on workflow-managed jobs. Runledger rejects direct
requeue with `job.workflow_requeue_not_supported` so workflow step state cannot
be bypassed; use workflow cancellation, external completion, or append APIs for
workflow-level recovery.

Use `update_job_payload_uuid_array_field` only for direct pending jobs whose
payload can be safely mutated. Inspect `JobPayloadUuidArrayFieldUpdate`; rejected
updates distinguish workflow-managed jobs, idempotent request snapshots, and jobs
that are no longer pending or unclaimed.

## Catalog Rule

Use `JobCatalog` as the worker startup source of truth when handlers,
definitions, schedules, and workflow builders should stay aligned. Use
`JobCatalogDefinitionOverrides` only for per-job definition differences from
catalog defaults. Overrides take precedence for the fields they set; version,
attempts, and timeout values must be positive, while priority may be zero or
negative.

Catalog schedules are appropriate when startup owns schedule definitions. The
catalog schedule sync APIs apply each spec's `is_active` value on every sync.
Use lower-level `job_schedule` + `upsert_job_schedule` for schedules whose active
state should be owned by admin pause/resume workflows.
Active schedules require enabled job definitions; do not disable definitions
until referencing schedules are inactive. Scheduler catch-up after downtime
materializes at most one stale fire, then advances the cursor to the first future
fire.

## Worker Runtime Rule

Use `runledger_runtime::Supervisor::run_until_shutdown` for ordinary worker
processes. The lower-level worker, scheduler, and reaper loops are escape
hatches for custom process orchestration.

Prefer `JobsConfig::from_env()` for runtime configuration. If code constructs
`JobsConfig` directly, validate it before startup; supervisors return
`RuntimeError::InvalidJobsConfig` and low-level loops can return
`RuntimeLoopExit::InvalidConfig` for invalid values.

## Example

See
[`runledger-postgres/examples/workflow_dag.rs`](../runledger-postgres/examples/workflow_dag.rs)
for a compile-checked fan-out/fan-in workflow DAG.

Other compile-checked examples:

- [`runledger-postgres/examples/enqueue_job.rs`](../runledger-postgres/examples/enqueue_job.rs)
- [`runledger-postgres/examples/external_gate.rs`](../runledger-postgres/examples/external_gate.rs)
- [`runledger-postgres/examples/append_workflow_steps.rs`](../runledger-postgres/examples/append_workflow_steps.rs)
- [`runledger-postgres/examples/schedule_job.rs`](../runledger-postgres/examples/schedule_job.rs)
- [`runledger-runtime/examples/worker_binary.rs`](../runledger-runtime/examples/worker_binary.rs)
