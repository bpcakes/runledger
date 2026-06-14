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
| Admin/status views | `runledger_postgres::jobs` read/list APIs |

Use `runledger_postgres::jobs::set_job_schedule_active` to pause or resume a
schedule, and `runledger_postgres::jobs::set_job_schedule_next_fire_at` to
retime the schedule cursor.

## Workflow DAG Rule

If the task requires step dependencies, build a workflow DAG:

1. Prefer `WorkflowDagBuilder` with `.job(...)`, `.after_success(...)`, and `.build()`.
2. Use `WorkflowStepEnqueueBuilder` and `WorkflowRunEnqueueBuilder` for advanced per-step
   settings, external steps, hand-authored dependency specs, or call sites that
   pass explicit `StepKey` and `JobType` values.
3. Persist the run with `runledger_postgres::jobs::enqueue_workflow_run`.

If callers need a durable workflow result, declare one initial DAG step as the
result step with `WorkflowDagBuilder::result_step(...)` or
`WorkflowRunEnqueueBuilder::try_result_step_key(...)`, enqueue with
`runledger_postgres::jobs::enqueue_workflow_run_handle`, and read through
`WorkflowRunHandle::get_result`. Job handlers return `JobCompletion`; use
`JobCompletion::with_output(...)` for compact JSON result metadata. If the run
succeeds but the declared result step stores no output, `get_result` returns
`WorkflowRunHandleError::ResultMissing`. `WorkflowRunWaitOptions::default()`
waits up to five minutes; set `timeout: None` only when the caller intentionally
wants to wait indefinitely and can afford the pending PostgreSQL listener
connection.

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

## Worker Runtime Rule

Use `runledger_runtime::Supervisor::run_until_shutdown` for ordinary worker
processes. The lower-level worker, scheduler, and reaper loops are escape
hatches for custom process orchestration.

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
