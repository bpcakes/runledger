# Downstream Agent Guide

This guide is for agents integrating Runledger into another application. It is
not an instruction file for agents maintaining this repository.

This guide targets Runledger 0.8.0 and retains the operational contracts added
in 0.6.0 and 0.7.0. Runledger requires Rust 1.88 or later and PostgreSQL 18 or
later. An older PostgreSQL server with an extension-provided `uuidv7()`
function is not a supported substitute.

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
| Provider-directed failure retry/reset time | `JobFailure::retry_not_before_delay(...)` or `JobFailure::retry_not_before(...)` |
| Another successful bounded slice | `JobCompletion::continue_now()` or `continue_after(...)` |
| One active workflow per application key | `WorkflowRunEnqueueBuilder::active_key(...)` and `enqueue_or_get_active_workflow` |
| One job at a time per external resource | `enqueue_job_with_execution_resource` or step `.execution_resource(...)` |
| Recover a canceled or dead-lettered direct job | `compare_and_requeue_job` |
| Repeat a successful direct job | `compare_and_replay_succeeded_job` |
| Recover a terminal workflow | `recover_workflow_run` |
| Worker process lifecycle | `runledger_runtime::Supervisor::run_until_shutdown` |
| Admin/status views | `runledger_postgres::jobs` read/list/count APIs |

Use `runledger_postgres::jobs::set_job_schedule_active` to pause or resume a
schedule, and `runledger_postgres::jobs::set_job_schedule_next_fire_at` to
retime the schedule cursor.

Use `runledger_postgres::jobs::count_workflow_runs` with
`runledger_postgres::jobs::WorkflowRunCountFilter` for workflow status counters
instead of fetching pages of runs just to count them.

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

## Handler-Selected Retry Timing

Use `JobFailure::retry_not_before_delay(delay)` when the provider returns a relative
`Retry-After` value, or `JobFailure::retry_not_before(reset_at)` when it returns an
absolute UTC reset timestamp:

```rust
use std::time::Duration;

let relative = JobFailure::retryable(
    "provider.temporarily_unavailable",
    "Provider asked the client to retry later.",
)
.retry_not_before_delay(Duration::from_secs(retry_after_seconds));

let absolute = JobFailure::retryable(
    "provider.rate_limited",
    "Provider rate limit reached.",
)
.retry_not_before(provider_reset_at);
```

Runledger 0.8 made the optional retry timing private, so construct
`JobFailure` with `new`, `retryable`, `terminal`, `timeout`, `lease_expired`, or
`panicked` instead of a struct literal. The deprecated `retry_after` and
`retry_at` aliases still mean a lower bound and should be migrated to the names
above. Low-level persistence integrations must construct the non-exhaustive
`JobFailureUpdate` with `JobFailureUpdate::new(...)` and optional
`.with_retry_timing(...)`.

The worker first resolves ordinary policy from the registry override for the
exact job type and failure code or Runledger's exponential fallback. PostgreSQL
then schedules the later of that policy time and the handler's not-before hint.

Timing is consulted only when another attempt will actually be scheduled.
Terminal and panicked failures, and retryable failures that exhaust
`max_attempts`, are dead-lettered without applying or validating it. A relative
delay is measured from PostgreSQL's completion clock; positive sub-millisecond
values round up to one millisecond, while zero supplies no additional lower
bound. An absolute time is rounded up only to PostgreSQL microsecond precision.
If it is at or before the database completion time, it cannot shorten ordinary
policy delay; a past value outside PostgreSQL's range is likewise ignored. A
winning relative delay or future timestamp that cannot be represented becomes
`job.invalid_retry_timing`.

This API controls another failed attempt; it is not successful continuation:

| Handler result | Durable effect |
| --- | --- |
| `Err(JobFailure::retryable(...).retry_not_before_delay(...))` or `.retry_not_before(...)` | Closes the attempt as failed, keeps `run_number`, consumes attempt budget, and leaves workflow dependents blocked. |
| `Ok(JobCompletion::continue_after(...))` | Closes a slice successfully, advances `run_number`, and starts the new run with a fresh attempt budget. Workflow job steps require persisted opt-in. |

`job_attempts.retry_delay_ms` retains the ordinary policy delay. Attempts and
`RETRY_SCHEDULED` events also record `requested_retry_not_before`,
`effective_next_run_at`, and `retry_timing_source`. Observer dispositions are
authoritative for the effective schedule; `JobFailure::retry_timing()` reports
what the handler requested. `RetryScheduledAt.requested_retry_at` contains the
database-clock boundary for either an absolute hint or a relative hint converted
to an absolute timestamp. Event payloads also retain `requested_retry_at` and
`next_run_at` as legacy aliases for consumers that have not migrated to the
clarified field names.

## Bounded Job Continuation

When one logical job must process bounded slices, return
`JobCompletion::continue_now()` or `JobCompletion::continue_after(delay)` from
the handler instead of enqueueing ordinal successor jobs. Add `.progress(...)`
and `.checkpoint(...)` when the next run needs durable position metadata.
The next handler invocation receives that committed value as
`JobContext::checkpoint`; its original payload is unchanged.
Runledger keeps the job ID, closes the current attempt successfully, advances
`run_number`, and gives the next run a fresh attempt budget. The handler slice
must remain idempotent because a crash before continuation persistence leaves
the lease for normal recovery; an exhausted lease is dead-lettered, and a new
checkpoint from an uncommitted slice is unavailable. Continuations cannot carry
final output.

Direct jobs need no enqueue setting. A workflow job step must explicitly opt in:

```rust
let step = WorkflowStepEnqueueBuilder::new(step_key, job_type, &payload)
    .allow_handler_continuation()
    .try_build()?;
```

The persisted default is disabled, and external steps cannot opt in. A
committed workflow continuation returns only that step from `RUNNING` to
`ENQUEUED`; it does not release dependency edges, decrement counters, invoke
terminal callbacks, or recompute terminal workflow state. The workflow stays
active, and a delayed continuation remains unclaimable until `next_run_at`.
Only final success, failure, or cancellation terminalizes the step.

Do not emit continuation-enabled workflow steps until every 0.7 process and
live lease has quiesced. A 0.7 worker does not understand the persisted opt-in
and can terminalize the step when its handler returns continuation.
Successful continuations are not bounded by `max_attempts`; handlers must own a
terminal condition and should use a nonzero delay for polling-style work.

Continuation has no global Runledger switch. It is enabled only when application
code returns a continuation disposition, so upgrading to 0.6 does not require
adopting it. Treat a production handler as a protocol with these invariants:

- Version the checkpoint object and reject unknown versions as a terminal
  handler error. A checkpoint is durable resume state, not a replacement for
  the immutable job payload.
- Make each slice idempotent. Deduplicate externally visible work by stable
  logical item or cursor identity (often together with `job_id`), never by
  `attempt`; a retry can rerun a slice whose continuation did not commit.
- Put a logical deadline or maximum slice/run count in immutable input or
  another durable application record. `max_attempts` limits failures within one
  run and does not cap successful continuations.
- Use `continue_after(...)` for polling and other work that should yield. Reserve
  `continue_now()` for bounded throughput where an immediate fresh claim cannot
  create a hot loop.
- Canary by job type or tenant before broad activation. Alert on continuation
  event rate, high continuation count/run depth, dead letters, and continuation
  persistence failures.

A typical checkpoint decoder makes its schema decision explicit:

```rust
#[derive(serde::Deserialize, serde::Serialize)]
struct SliceCheckpoint {
    version: u32,
    cursor: String,
}

let checkpoint = context
    .checkpoint
    .map(serde_json::from_value::<SliceCheckpoint>)
    .transpose()
    .map_err(|_| JobFailure::terminal(
        "job.invalid_checkpoint",
        "The persisted checkpoint could not be decoded.",
    ))?;

if checkpoint.as_ref().is_some_and(|value| value.version != 1) {
    return Err(JobFailure::terminal(
        "job.unsupported_checkpoint_version",
        "The persisted checkpoint version is not supported.",
    ));
}
```

The packaged external-consumer
[`smoke` test](../smoke/external-consumer/tests/smoke.rs) is the
compile-checked reference for returning a checkpointed continuation, reading it
on the next run, and completing typed recovery.

### Continuation operational queries

Use `get_job_continuation_metrics` for service dashboards and alerts. It returns
one `JobContinuationMetricsRecord` per job type:

- `continued_24h` is the number of committed handler continuations in the last
  24 hours;
- `active_continued_count` is the number of `PENDING` or `LEASED` jobs whose
  current run was created by a handler continuation;
- `max_active_run_number` is the highest current run number among those
  continuation-created runs. A later admin recovery is deliberately excluded,
  so this is a focused runaway-depth signal rather than lifetime ancestry.

The optional organization argument follows the admin-metrics convention:
`None` aggregates every scope rather than selecting only global jobs. Choose
alert thresholds from the expected slice size and schedule; a fixed global
threshold is usually misleading.

The following read-only PostgreSQL 18 queries use the same durable event log for
ad-hoc investigation. The API above is the stable application integration.
Each query requires the continuation schedule's three structural payload keys
(`next_run_number`, `next_run_at`, and `delay_microseconds`). New events are
classified by the stable `requeue_kind = HANDLER_CONTINUATION` discriminator.
The reason-based branch applies only when `requeue_kind` is absent, preserving
events written by 0.6.0 and mixed-version deployments without allowing a future
administrative requeue with the same payload shape to be misclassified.

Continuation volume by job type over the last 15 minutes:

```sql
SELECT jq.job_type,
       count(*) AS continuations,
       count(DISTINCT je.job_id) AS jobs
FROM job_events AS je
JOIN job_queue AS jq ON jq.id = je.job_id
WHERE je.event_type = 'REQUEUED'
  AND je.payload ?& ARRAY[
      'next_run_number',
      'next_run_at',
      'delay_microseconds'
  ]
  AND (
      je.payload ->> 'requeue_kind' = 'HANDLER_CONTINUATION'
      OR (
          NOT (je.payload ? 'requeue_kind')
          AND je.payload ->> 'reason' = 'HANDLER_CONTINUATION'
      )
  )
  AND je.occurred_at >= clock_timestamp() - interval '15 minutes'
GROUP BY jq.job_type
ORDER BY continuations DESC, jq.job_type;
```

Jobs with the deepest continuation history:

```sql
SELECT jq.id,
       jq.job_type,
       jq.organization_id,
       jq.status,
       jq.run_number,
       count(*) AS continuation_count,
       max(je.occurred_at) AS last_continued_at
FROM job_events AS je
JOIN job_queue AS jq ON jq.id = je.job_id
WHERE je.event_type = 'REQUEUED'
  AND je.payload ?& ARRAY[
      'next_run_number',
      'next_run_at',
      'delay_microseconds'
  ]
  AND (
      je.payload ->> 'requeue_kind' = 'HANDLER_CONTINUATION'
      OR (
          NOT (je.payload ? 'requeue_kind')
          AND je.payload ->> 'reason' = 'HANDLER_CONTINUATION'
      )
  )
GROUP BY jq.id, jq.job_type, jq.organization_id, jq.status, jq.run_number
ORDER BY continuation_count DESC, last_continued_at DESC
LIMIT 50;
```

The depth query aggregates the full continuation event history. On a large
event table, run it on a read replica or add an application-appropriate
`occurred_at` / job-type restriction.

`runledger-tui` and `list_job_events` expose the same `REQUEUED` reason,
`requeue_kind`, `next_run_number`, `next_run_at`, and `delay_microseconds`
fields for an individual job. Replay `ENQUEUED` details also expose the source
job/run, replay request key, and reason. The TUI dashboard shows `Cont 24h`,
`Cont now`, and `Max run` per job type plus an active-continuation KPI.
Lifecycle observers are the appropriate source for alerts on continuation
persistence failures; events exist only after a transition commits.

Application event consumers should prefer `JobEventRecord::decoded_payload()`
to reading Runledger-authored JSON fields directly. The decoded enums and their
struct variants are non-exhaustive: use `..` in struct-variant patterns and a
final wildcard at every enum level. Keep `JobEventRecord::payload` as the raw
fallback for malformed, historical, custom, and future payloads:

```rust
use runledger_postgres::prelude::{
    DecodedJobEventPayload, DecodedRequeuedEventPayload,
    SuccessfulReplayEnqueuedEventPayload, list_job_events,
};

let events = list_job_events(&pool, organization_id, job_id, 200, None).await?;

for event in events {
    match event.decoded_payload() {
        DecodedJobEventPayload::Requeued(requeued) => match requeued {
            DecodedRequeuedEventPayload::HandlerContinuation {
                reason,
                next_run_number,
                next_run_at,
                delay_microseconds,
                ..
            } => {
                println!(
                    "{reason}: run {next_run_number} at {next_run_at} after {delay_microseconds}us"
                );
            }
            DecodedRequeuedEventPayload::Unknown { reason, .. } => {
                eprintln!(
                    "unknown requeue payload ({reason:?}); raw payload: {}",
                    event.payload
                );
            }
            _ => {
                eprintln!("other requeue; raw payload: {}", event.payload);
            }
        },
        DecodedJobEventPayload::SuccessfulReplayEnqueued(
            SuccessfulReplayEnqueuedEventPayload {
                replayed_from_job_id,
                replayed_from_run_number,
                replay_request_key,
                reason,
                ..
            },
        ) => {
            println!(
                "replayed {} run {} with key {}: {}",
                replayed_from_job_id,
                replayed_from_run_number,
                replay_request_key,
                reason
            );
        }
        DecodedJobEventPayload::Other => {
            eprintln!("undecoded event; raw payload: {}", event.payload);
        }
        _ => {
            eprintln!("future decoded event; raw payload: {}", event.payload);
        }
    }
}
```

## Active Keys, Execution Resources, And Workflow Recovery

- Use `.active_key(...)` only with `enqueue_or_get_active_workflow`, and handle
  `Inserted`, `ExistingActive`, and `ExistingIdempotent` explicitly. Scope is
  global when `organization_id` is absent and organization-local otherwise.
  Workflow type is not part of the scope; namespace keys by type unless
  cross-type coordination is intentional. Active keys must be non-blank and at
  most 512 bytes. A canceled live lease is released by the lease reaper after
  quiescence; if reaping stops, the key remains reserved until it resumes.
  `ExistingActive` may therefore contain a terminal canceled run or a different
  workflow type using the same unnamespaced key; do not retry solely because
  `run.status` is terminal. `enqueue_workflow_run_handle` rejects active-key
  payloads so it cannot erase collision classification. Match the active
  enqueue outcome, then create a handle with `workflow_run_handle` from the
  returned run ID and scope.
- Use `enqueue_job_with_execution_resource` for direct jobs or
  `WorkflowStepEnqueueBuilder::execution_resource` for job steps. A blocked
  resource leaves work pending at attempt zero and does not occupy a worker
  claim slot. Keys must be non-blank and at most 512 bytes. Resource keys are
  global across organizations, so namespace them in the application when
  tenants must not contend. They guarantee mutual exclusion rather than global
  FIFO; type-restricted workers order only the eligible types. Mixing claim
  APIs or workers with different type filters can therefore reorder contenders
  for one key without weakening mutual exclusion.
  The direct enqueue returns `JobEnqueueOutcome`. For a keyed job, the resource
  is part of its canonical enqueue snapshot; an idempotent retry cannot silently
  change it.
  A requested batch is only an upper bound: concurrent resource races and the
  bounded 1,024–16,384-job resource-head window can return short batches,
  especially behind a dense same-key prefix. Exclusivity is lease-scoped: after
  heartbeat loss, the reaper releases an expired owner even if the old handler
  has not physically exited.
  Fence or make provider-side operations idempotent so a late handler cannot
  conflict with its successor. Successful direct-job replay preserves the source
  resource key. If reaping stops, expired claims stay reserved until it resumes;
  each pass bounds cleanup by the reaper batch limit. Reaped job transitions
  commit before coordination-claim cleanup, and the detailed reaper result
  reports released active/resource claim counts plus cleanup errors. The runtime
  logs cleanup failures and warns when cleanup reaches its batch limit. A
  resource owner's heartbeat renews the claim row, and continuation releases
  the claim between slices so the next slice must re-contend.
- Use `recover_workflow_run` for a terminal workflow. It creates a new run and
  durable lineage; it never reopens source steps. Recovery requires a canonical
  enqueue snapshot and replays committed append history while preserving active
  and resource constraints, handler-continuation opt-ins, and each source
  step's resolved priority, attempt limit, and timeout. It uses the latest
  payload persisted on each source step, including a committed pending-step
  payload correction. It does not reuse the source workflow's permanent
  idempotency key. Unknown canonical fields and unsupported mutation kinds are
  rejected. Every new workflow retains a canonical snapshot containing step
  payloads; account for the duplicated JSON in retention planning and store
  large artifacts externally.
- Treat recovery `request_key` as an idempotency boundary. Identical retries
  return the existing recovery; changed source-step, mode, or reason fields
  conflict. Keys must be non-blank and at most 512 bytes. Deleting only the
  recovery run is blocked while the source remains so retention cannot erase
  that idempotency guard; delete the complete lineage in a source-led
  statement.

Construct recovery inputs through the non-exhaustive request's constructor:

```rust
let request = WorkflowRecoveryRequest::new(
    source_run_id,
    request_key,
    WorkflowRecoveryMode::FullReplay,
    "operator-approved retry",
)
.organization_id(organization_id)
.source_step_id(failed_step_id);

let outcome = recover_workflow_run(&pool, &request).await?;
match outcome.disposition {
    WorkflowRecoveryDisposition::Inserted => { /* new run */ }
    WorkflowRecoveryDisposition::Existing => { /* idempotent retry */ }
    _ => { /* future disposition */ }
}
```

Omit `.organization_id(...)` only for an exactly global source.
`source_step_id` is optional lineage context; recovery still replays the full
workflow. Use `recover_workflow_run_tx` when recovery must share a transaction
with application writes. That transaction must be `READ COMMITTED`, and the
function neither commits nor rolls it back. An active-key recovery can remain
blocked until the source claim is quiescent, or while another run owns the key.

## Release Upgrade Map: 0.6 Through 0.8

When an integration skips versions, preserve every intermediate runtime and
schema boundary:

| Release | Required schema | Runtime/source migration |
| --- | --- | --- |
| 0.6.0 | No migration. | Deploy all job-state writers with direct continuation and typed compare-and-requeue unused. Quiesce every pre-0.6 process and live lease before activation. Move deprecated `requeue_job` callers to exact scope, typed no-mutation outcomes, and an explicit state policy. |
| 0.7.0 | `202607190001_job_replays_and_continuation_metrics` before successful replay or metrics calls. | Replay creates a fresh lineage-linked job rather than mutating a successful source. Prefer decoded event payloads and Runledger's filtered schema helpers during expand-first rollout. |
| 0.8.0 | `202607250001_harden_continuation_metrics_payload_validation` plus `202607280001` through `202607280005` before any 0.8 runtime loop or persistence API runs. | Deploy every writer with workflow continuation, active keys, resources, retry hints, and workflow recovery unused; quiesce all pre-0.8 processes and leases, then canary each path. |

For 0.8 source compatibility, construct the now non-exhaustive
`WorkflowDagStepValidationInput` through
`WorkflowDagStepValidationInput::new(...)` and its option setters. Its new
handler-continuation and execution-resource settings are also present on
`WorkflowStepDbRecord`, so direct struct-literal consumers must update.
Construct workflow recovery requests and match recovery outcomes as shown
above, retaining wildcard arms for non-exhaustive enums.

## 0.7 To 0.8 Activation And Rollback Runbook

The 0.8 persisted continuation, active-key, execution-resource, retry-audit,
and workflow-recovery paths require a two-phase runtime rollout. "Disabled"
means deployed handlers do not return workflow continuation dispositions and
application callers do not invoke the new coordinated enqueue or recovery
APIs; Runledger has no global switch.

For activation:

1. Apply
   `202607250001_harden_continuation_metrics_payload_validation` and the
   additive migrations through `202607280005_workflow_recoveries`, including
   workflow continuation, active-claim, retry-audit, execution-resource, and
   workflow-recovery state. Existing 0.7 binaries remain data-plane compatible
   only while the new paths are unused.
   Existing older binaries remain data-plane compatible with the
   additive table and views when startup uses Runledger's filtered migration or
   schema-compatibility helper. SQLx records the migrations, while their
   compatibility-fence history deliberately remains at the 0.6.0 set so those
   released guards continue to pass. A raw `MIGRATOR.run(...)` from an exact
   older release is different: SQLx rejects a newer migration-history row
   because that version is absent from the embedded migration set. SQLx 0.8 can
   also leave the raw migrator's session advisory lock held on this error, so
   close that disposable connection or pool instead of retrying it. Do not
   deploy replay or metrics callers before these expand-first migrations are
   present.
2. Deploy 0.8 to every worker, reaper, admin, API, CLI, and repair process that
   writes job lifecycle state. Keep the new paths unused.
3. Prove from deployment inventory or process telemetry that every 0.7
   process has stopped. If the upgrade skips releases, prove that every
   pre-0.8 process has stopped. Runledger rows do not record a process's crate
   version, so a database query alone cannot prove this condition.
4. Record the authoritative server version:

   ```sql
   SHOW server_version;
   SHOW server_version_num;
   ```

   The supported baseline is PostgreSQL 18 or later.
5. Briefly pause new claims and wait until this query returns no rows. This is a
   deliberately strict, version-independent proof that old worker leases and
   canceled-handler fences have quiesced:

   ```sql
   SELECT id,
          job_type,
          organization_id,
          status,
          run_number,
          worker_id,
          lease_expires_at
   FROM job_queue
   WHERE status IN ('LEASED', 'CANCELED')
     AND lease_expires_at > clock_timestamp()
   ORDER BY lease_expires_at, id;
   ```

6. Resume 0.8 claims. Activate workflow continuation, active keys, execution
   resources, retry hints, and workflow recovery independently, beginning with
   a canary job type or tenant.

A 0.7 worker does not create a `job_execution_resource_claims` row before it
tries to lease a resource-constrained job. The database trigger added by the
resource migration rejects that lease, preserving mutual exclusion but causing
the old worker's claim transaction to fail loudly. It may repeatedly roll back
batches that encounter constrained work. Steps 2–6 are therefore a mandatory
availability and protocol fence, not only a schema rollout precaution.

After a new path has committed state, do not start a 0.7 writer as a
partial rollback. For a coordinated rollback:

1. First deploy or configure application code so no handler emits another
   continuation, no caller starts another typed recovery, and no caller enqueues
   another resource-constrained job.
2. Drain all nonterminal jobs whose current run was created by a handler
   continuation. This query must return no rows before a 0.7 writer starts:

   ```sql
   SELECT jq.id,
          jq.job_type,
          jq.organization_id,
          jq.status,
          jq.run_number,
          jq.next_run_at,
          jq.lease_expires_at
   FROM job_queue AS jq
   WHERE jq.status IN ('PENDING', 'LEASED')
     AND EXISTS (
         SELECT 1
         FROM job_events AS je
         WHERE je.job_id = jq.id
           AND je.run_number = jq.run_number - 1
           AND je.event_type = 'REQUEUED'
           AND je.payload ?& ARRAY[
               'next_run_number',
               'next_run_at',
               'delay_microseconds'
           ]
           AND (
               je.payload ->> 'requeue_kind' = 'HANDLER_CONTINUATION'
               OR (
                   NOT (je.payload ? 'requeue_kind')
                   AND je.payload ->> 'reason' = 'HANDLER_CONTINUATION'
               )
           )
           AND je.payload -> 'next_run_number' = to_jsonb(jq.run_number)
     )
   ORDER BY jq.next_run_at, jq.id;
   ```

   The run-number predicates prove that the matching event completed the
   immediately preceding run and created the job's current run. Older
   continuation history therefore does not keep a job in this rollback gate
   after a later admin recovery.

3. Drain resource-constrained work. A 0.7 worker cannot acquire the durable
   resource claim, so the database rejects its lease and rolls back the claim
   transaction. This query must return no rows:

   ```sql
   SELECT id,
          job_type,
          organization_id,
          status,
          run_number,
          execution_resource_key,
          next_run_at,
          lease_expires_at
   FROM job_queue
   WHERE execution_resource_key IS NOT NULL
     AND status IN ('PENDING', 'LEASED')
   ORDER BY next_run_at, id;
   ```

4. Wait for every canceled live-handler fence to expire; this query must also
   return no rows:

   ```sql
   SELECT id,
          job_type,
          organization_id,
          run_number,
          lease_expires_at
   FROM job_queue
   WHERE status = 'CANCELED'
     AND lease_expires_at > clock_timestamp()
   ORDER BY lease_expires_at, id;
   ```

5. Stop every 0.8 worker, reaper, admin, API, CLI, and repair writer. Confirm
   process shutdown from deployment inventory, pause claims, and rerun the
   active-lease query from activation until it returns no rows. Only then start
   any 0.7 process.

Choose one migration-history strategy before starting the rollback binary:

1. Recommended: leave every migration through
   `202607280005_workflow_recoveries` applied and use
   `migrate_after_idempotency_cutover` or
   `ensure_schema_compatible_after_idempotency_cutover` at startup. These
   Runledger paths filter SQLx history to the migrations embedded in that
   release. Patch the rollback binary's startup first if it directly calls raw
   `MIGRATOR.run(...)`.
2. A raw 0.7 `MIGRATOR.run(...)` rejects
   `202607250001_harden_continuation_metrics_payload_validation` and all five
   `20260728000*` history rows because they are absent from its bundle. If it
   cannot be patched, close the failed connection or pool, then use the 0.8
   artifact to revert those six migrations in reverse order before starting
   0.7.

The raw down-migration path erases 0.8 state: workflow-recovery lineage and
request idempotency, active claims, execution-resource keys and claims,
retry-timing audit columns, and workflow-step continuation opt-ins. It leaves
the underlying workflow and job rows, so recovery-created runs lose lineage and
retained work loses its resource constraint. A further rollback to pre-0.7 raw
code also requires reverting
`202607190001_job_replays_and_continuation_metrics`; that deletes relational
replay lineage and replay-request idempotency while leaving replay-created jobs
and their lineage-bearing `ENQUEUED` events. Use either destructive path only
with explicit acceptance of these losses.

Use these SQL statements only for diagnosis and rollout gates. Drain or mutate
jobs through Runledger APIs; do not repair `job_queue` or `job_events` with
ad-hoc SQL.

## Workflow Completion And Validation

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

## Direct-Job Recovery

Use pool-owning `compare_and_requeue_job` for one standalone recovery. Use
`compare_and_requeue_job_tx` when recovery must commit atomically with an
application audit record or another mutation; the caller owns that transaction
and it must use `READ COMMITTED`. Both forms require an exact `JobScope`,
expected `RequeueableJobStatus`, expected run number, state policy, and reason.
`CompareAndRequeueJob::from_observed_job` copies the exact scope, ID, terminal
status, and run number from a `JobQueueRecord` and rejects a record whose status
is not recoverable. The PostgreSQL 18 integration coverage in
[`compare_and_requeue.rs`](../runledger-postgres/tests/compare_and_requeue.rs)
exercises both the constructor and pool-owning wrapper.

Always inspect the typed outcome:

- `Requeued` is the only mutation-success outcome.
- `ExpectationMismatch` and `NotFound` are normal no-mutation results. Refresh
  the operator view instead of retrying a stale request blindly.
- `CancellationNotQuiesced` means cancellation fenced a live handler. Retry no
  earlier than its `retry_after`; an immediate new run could overlap the old
  handler's external side effects.

Do not call either requeue API on workflow-managed jobs. Runledger rejects
direct requeue with `job.workflow_requeue_not_supported` so workflow step state
cannot be bypassed; use workflow cancellation, external completion, or append
APIs for workflow-level recovery. Live status mismatches are read without
retaining a row lock.

Choose `JobRequeueStatePolicy::PreserveProgressAndCheckpoint` when recovery
should resume from the last committed checkpoint, or
`ResetProgressAndCheckpoint` for an explicit restart. The policy is written to
the `REQUEUED` event for auditability.

### Migrating deprecated `requeue_job`

The compatibility API remains for the staged 0.6 rollout, not as the final
recovery contract. Audit every admin endpoint, CLI, and repair sweep that calls
it, then map behavior deliberately:

| Deprecated behavior | Typed migration |
| --- | --- |
| `organization_id: Some(id)` | Use `JobScope::Organization(id)` and verify it matches the observed record. |
| `organization_id: None` | This was an unconstrained lookup. Do **not** translate it to `JobScope::Global`; observe the row, authorize its actual tenant, and derive exact scope. |
| Implicitly clears progress/checkpoint | Choose `ResetProgressAndCheckpoint` for legacy parity, or explicitly approve `PreserveProgressAndCheckpoint` as a behavior change. |
| Missing row error | Handle `CompareAndRequeueJobOutcome::NotFound`. |
| Wrong terminal state or stale state | Handle `ExpectationMismatch` and refresh the observation. |
| `job.cancellation_not_quiesced` error | Handle `CancellationNotQuiesced` and schedule retry for `retry_after`. |
| Accepted `SUCCEEDED` | Do not use typed recovery; use compare-and-replay below. |

Database and workflow-policy failures still return `Err`; typed no-mutation
outcomes are not errors and the pool wrapper commits them before returning.

### Replaying successful work

`RequeueableJobStatus` deliberately excludes `SUCCEEDED`. Do not add successful
jobs to typed recovery and do not reset a successful source row in place: that
can erase its only durable output and weakens the historical record.

Use `compare_and_replay_succeeded_job` for a standalone authorized replay, or
`compare_and_replay_succeeded_job_tx` when the replay and an application audit
mutation must commit together. The transaction form requires `READ COMMITTED`.
Both take `CompareAndReplaySucceededJob` with an exact source scope and observed
run number, a required `replay_request_key`, and a required reason. Apply
`202607190001_job_replays_and_continuation_metrics` before using either API.

```rust
use runledger_postgres::jobs::{
    CompareAndReplaySucceededJob, CompareAndReplaySucceededJobOutcome,
    JobEnqueueDisposition, JobScope, compare_and_replay_succeeded_job,
};

// Authorize `source.organization_id` before deriving its exact mutation scope.
let scope = source
    .organization_id
    .map_or(JobScope::Global, JobScope::Organization);

let outcome = compare_and_replay_succeeded_job(
    &pool,
    CompareAndReplaySucceededJob {
        scope,
        source_job_id: source.id,
        expected_run_number: source.run_number,
        replay_request_key: replay_action_key,
        reason: replay_reason,
    },
)
.await?;

match outcome {
    CompareAndReplaySucceededJobOutcome::Replayed { replay, .. } => {
        match replay.disposition {
            JobEnqueueDisposition::Inserted => { /* newly created replay */ }
            JobEnqueueDisposition::Existing => { /* idempotent request retry */ }
            _ => {}
        }
    }
    CompareAndReplaySucceededJobOutcome::ExpectationMismatch {
        actual: _actual,
    } => {
        // Refresh the operator view; the observed successful run is stale.
    }
    CompareAndReplaySucceededJobOutcome::NotFound => {
        // Return an exact-scope not-found result.
    }
    _ => {}
}
```

The replay contract preserves fresh-job semantics:

- the successful source row, events, checkpoint, and output remain unchanged;
- the replay gets a new job ID, starts at run one, and copies the source job
  type, tenant, payload, priority, attempt budget, timeout, and execution
  resource;
- progress, checkpoint, output, terminal timestamps, workflow ownership, the
  original idempotency key, and the source's old schedule time are not copied;
- Runledger records the source ID/run, replay ID, request key, and reason in
  `job_replays`, and records the same lineage in the replay's `ENQUEUED` event;
- deleting only the replay queue row is blocked while the successful source
  remains, preserving the replay request's idempotency guard;
- deleting the source cascades its lineage. A retention statement that selects
  both source and replay rows can delete them together without leaving lineage
  behind.

Choose one stable `replay_request_key` for one intentional replay action. Retry
that action with the same source run, key, and reason; the `Replayed` outcome's
`replay.disposition` is `Existing`. Use a different key for another intentional
replay. Reusing a key with a different reason returns
`job.replay_idempotency_conflict`. Blank keys, keys over 512 bytes, and blank
reasons are rejected. `ExpectationMismatch` and `NotFound` create nothing.
Workflow-managed successful jobs remain workflow-owned and return
`job.workflow_requeue_not_supported` from this direct-job API.
See the PostgreSQL 18
[`succeeded_job_replay` integration test](../runledger-postgres/tests/succeeded_job_replay.rs)
for compile-checked inserted, idempotent, stale, exact-scope, transactional, and
workflow-rejection cases.

When a transactional keyed enqueue needs to branch on the durable job state,
use `enqueue_job_with_outcome_tx`. Its `JobEnqueueOutcome` contains the job ID,
status, run number, and `Inserted`/`Existing` disposition under the enqueue
transaction's mutation-ready row lock; do not query `job_queue` directly.

Use `update_job_payload_uuid_array_field` only for direct pending jobs whose
payload can be safely mutated. Inspect `JobPayloadUuidArrayFieldUpdate`; rejected
updates distinguish workflow-managed jobs, idempotent request snapshots, and jobs
that are no longer pending or unclaimed.

## Error Boundary

Expose `QueryError::code()` and `QueryError::client_message()` at public
boundaries. `Display` and `Debug` are sanitized; use
`QueryError::internal_message()` and `source_arc()` only for trusted
diagnostics. `QueryError::kind()` covers a deliberately small set of
compile-checked cross-crate runtime decisions. Application and protocol logic
should normally branch on the stable string code, not broad categories or
database text.

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

Other compile-checked examples and integration references:

- [`runledger-postgres/examples/enqueue_job.rs`](../runledger-postgres/examples/enqueue_job.rs)
- [`runledger-postgres/examples/external_gate.rs`](../runledger-postgres/examples/external_gate.rs)
- [`runledger-postgres/examples/append_workflow_steps.rs`](../runledger-postgres/examples/append_workflow_steps.rs)
- [`runledger-postgres/examples/schedule_job.rs`](../runledger-postgres/examples/schedule_job.rs)
- [`runledger-runtime/examples/worker_binary.rs`](../runledger-runtime/examples/worker_binary.rs)
- [`smoke/external-consumer/tests/smoke.rs`](../smoke/external-consumer/tests/smoke.rs)
- [`runledger-postgres/tests/succeeded_job_replay.rs`](../runledger-postgres/tests/succeeded_job_replay.rs)
- [`runledger-postgres/tests/workflow_active_claims.rs`](../runledger-postgres/tests/workflow_active_claims.rs)
- [`runledger-postgres/tests/job_execution_resources.rs`](../runledger-postgres/tests/job_execution_resources.rs)
- [`runledger-postgres/tests/workflow_recovery.rs`](../runledger-postgres/tests/workflow_recovery.rs)
