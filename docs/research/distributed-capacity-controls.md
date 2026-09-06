# Distributed capacity controls: investigation

Date: 2026-09-06. Initial source baseline: `ffe770e` (Runledger 0.12.0 workspace).
Rechecked relevant upstream changes through `4645bc3` before publication: worker
capacity, resource admission SQL, and resource lifecycle triggers are unchanged.
New submission adapters and workflow batching/builders add propagation surfaces
described below.
Tracking: `runledger-1iv`.

## Reinvestigation and epic plan (2026-09-06)

Reinvestigated against `7cdb9cb`, including the complete current claim, workflow
lock, submission, intent, catalog schedule, recovery, and migration contracts.
The executable implementation specification is now
[`docs/distributed-capacity-plan.md`](../distributed-capacity-plan.md).
Its task catalog separates concurrency delivery from rolling admission rates.
Implementation epic: `runledger-distributed-capacity-bp2`, with 21 tasks and 31
internal blocking dependencies. The first implementation task is
`runledger-distributed-capacity-bp2.1`.
The original investigation below remains useful background; the execution plan
supersedes its illustrative API, storage, transaction, and rollout choices.

The substantive changes are:

- Preserve one atomic outer claim transaction with candidate savepoints. The
  proposed engine uses nonblocking job and workflow-step prelocks, then sorted
  policy `FOR NO KEY UPDATE NOWAIT` locks and a separate occupancy command.
  This avoids introducing partial committed batch results. The workflow-step
  prelock breaks a reverse-order cycle with external workflow transitions.
- Bound lock-acquiring savepoints at 24 initially, following the current intent
  promoter's subtransaction rationale. A larger read window and a continuing
  keyset cursor are separate bounds. Alternate head and continuation passes;
  do not reset traversal after each successful claim.
- Carry a fresh admission UUID through constrained lifecycle writes, including
  prestart release. The current attempt tuple can be reused, so UUIDs cannot
  be limited to rate-history deduplication. New claim/token wrappers preserve
  public literals. Custom runtimes adopt those APIs; legacy claimers explicitly
  skip bound work and legacy tuple mutations reject it.
- Freeze exact globally scoped keys, unit costs, at most eight requirements,
  revisioned policy changes, empty-field canonical compatibility, dual intent
  request versions, and explicit schedule options. Preserve bindings on legacy
  schedule upserts. Use a maintenance cutover because compatibility history can
  prevent old binaries from restarting after the schema change.

The new [protocol probe](probes/capacity_protocol.py) creates its own disposable
`postgres:18` container and applies all **17 current upward migration files**.
It then layers a diagnostic capacity protocol over real Runledger queue,
workflow, execution-resource, attempt, and event tables. It does not execute
the Rust APIs or SQLx's migration-history validator.

Recorded server: **18.4 (Debian 18.4-1.pgdg13+1)**.
The [machine-readable results](probes/capacity-protocol-result-2026-09-06.json)
record the exact source commit and tested assertions.

| Reinvestigation experiment | Observed result |
| --- | --- |
| 24 independent sessions, two policies of capacity three, resources and workflow jobs | Three claims, six permits, three resources, and three attempt rows. |
| Unexpected error after two candidate successes | Entire batch and audit writes rolled back. |
| Later candidate denied by a held policy lock | Earlier candidate committed; denied job stayed pending. |
| Policy lock while another session creates an FK reference | `NO KEY UPDATE` allowed reference creation. |
| Uncommitted legacy resource conflict with disjoint policies | Candidate rolled back under the bounded uniqueness wait. |
| Reverse workflow step ordering with a blocked competing session | NOWAIT step prelock broke the modeled cycle. |
| Live cancellation followed by queue deletion | Retained permits prevented deletion and the legacy-resource cascade. |
| Reused run/attempt/worker with a stale admission UUID | Stale update affected zero rows; successor permits remained. |
| Expiry/reaper and heartbeat-first orderings | Ordinary expired permits remained until owner transition; renewal kept matching deadlines. |
| Old-style lease write without capacity proof | Database guard rejected it. |
| 300 saturated-prefix jobs before an unrelated eligible job | Cursor reached and admitted the unrelated job in 13 batches capped at 24 candidate savepoints. |
| New higher-priority job during traversal | A separate head probe observed it. |

Reproduce from the repository with:

```sh
uv run docs/research/probes/capacity_protocol.py
```

This is protocol-model evidence, not a production integration or performance
claim. Full Rust workflow cancellation/recovery, cursor deletion/rollback,
multi-process runtime behavior, rate history, and fault/throughput measurements
are explicit implementation gates. Proposed old/new canonical examples are in
[capacity-contract-fixtures.json](capacity-contract-fixtures.json); P01/P04/P06
turn those planning fixtures into executable tests.

## Recommendation

Add PostgreSQL-backed admission policies, beginning with distributed counting
semaphores. Keep local worker capacity as an independent protection for each
worker loop. Follow with an explicitly named rolling-window admission limiter
for workloads that need a strict count over a time interval.

Acquire all of a job's requirements in its lease transaction, before incrementing
the attempt or returning it to a worker. Persist policy references at enqueue,
including on workflow steps and enqueue intents. Release semaphore permits through
the existing fenced lease lifecycle. Rate admissions are durable consumption and
are not released when the job completes.

The following sections preserve the initial investigation and its original
single-policy experiment. Consult the linked execution plan and its review record
for the revised protocol, complete implementation tasks, and actual review status.
No complete feature implementation or full lifecycle proof is claimed here.

## What the repository does today

| Concern | Current behavior and evidence |
| --- | --- |
| Worker capacity | [`WorkerLoop::available_capacity`](../../runledger-runtime/src/worker.rs) subtracts that loop's `JoinSet` length from `max_global_concurrency`. Multiple loops do not share this count, even within one process. |
| Configuration | [`JobsConfig`](../../runledger-runtime/src/config.rs) exposes a public field and reads `JOBS_MAX_GLOBAL_CONCURRENCY`. Its default is 32. |
| Distributed resources | [`job_execution_resource_claims`](../../migrations/202607280004_job_execution_resources.up.sql) has `resource_key` as its primary key and a unique `job_id`: one owner per key, one legacy resource per job. |
| Admission | [`claim_ids.sql`](../../runledger-postgres/src/jobs/queue/claim_ids.sql) finds resource heads, locks pending queue rows with `SKIP LOCKED`, and inserts durable claims with conflict suppression. Unsuccessful contenders remain pending. |
| Claim atomicity | [`claim_jobs_inner`](../../runledger-postgres/src/jobs/queue/claim.rs) leases rows, updates workflow state, inserts attempts and events, then commits one transaction. Direct and worker-prestart claim APIs share this path. |
| Ownership fence | Resource claims carry `(job_id, run_number, attempt, worker_id)`. A database trigger requires the matching claim when a resource-constrained job transitions to `LEASED`. |
| Release | Migration triggers release on lease exit, renew on heartbeat, and retain a canceled live owner's resource until its former lease deadline. |
| Reaping | [`release_expired_execution_resource_claims_tx`](../../runledger-postgres/src/jobs/queue/claim.rs) does not reclaim a plain expired claim while its exact queue owner remains `LEASED`. Reaping must transition that owner first. |
| Prestart recovery | [`release.rs`](../../runledger-postgres/src/jobs/queue/release.rs) can decrement the attempt and delete its attempt row when execution never started. An attempt number can therefore be reused. |
| Handler start | [`execution.rs`](../../runledger-runtime/src/worker/execution.rs) persists running progress before invoking the handler. Admission and invocation are separate events. |
| Idempotency | [`canonical_job_enqueue_request_v1`](../../runledger-postgres/src/jobs/queue/enqueue.rs) includes the execution resource. Changing a resource on an idempotent enqueue is a conflict. |
| Recovery | [`workflow snapshots`](../../runledger-postgres/src/jobs/workflows/snapshot.rs), [`recovery`](../../runledger-postgres/src/jobs/workflows/recovery.rs), and [`direct replay`](../../runledger-postgres/src/jobs/replay.rs) preserve execution-resource settings. Recovery decoders reject unknown fields. |
| Schedules | [`materialize_schedule_tx`](../../runledger-runtime/src/scheduler.rs) constructs a plain `JobEnqueue`; new capacity bindings need an explicit schedule integration. |

The proposed feature is not a change to `max_global_concurrency` alone. Shared
state belongs at admission, where Runledger already enforces distributed
resources, and must cover custom runtimes using the public PostgreSQL claim APIs.

## Define the guarantees before choosing an algorithm

| User requirement | Proposed meaning |
| --- | --- |
| 20 concurrent operations per customer | At most 20 outstanding admitted job leases/retained permits bound to that customer's concurrency policy. |
| 100 requests per minute | If each admission represents one request, at most 100 admission decisions in any trailing 60 seconds of database time. Request execution time is a separate boundary. |
| Provider account plus customer limits | A job references both policies; admission succeeds only if both have room. |
| Fleet limit | Attach a shared fleet policy to every participating job. A limit is global across participating workers using the same database, not across independent databases. |
| Worker limit | The existing per-loop count continues to bound local claimed tasks, including execution and completion persistence. |

The concurrency guarantee is lease-scoped. A handler can outlive its lease during
a partition or delayed cancellation. Runledger already documents this limitation
for resource keys in the [downstream guide](../downstream-agent-guide.md).
Provider fencing or idempotency is still necessary where overlapping external
effects are unacceptable. Permits do not prove that a remote operation stopped.

A handler that performs ten HTTP calls consumes one queue admission unless those
calls are separate jobs. Even one call per job can be delayed after admission,
so actual requests may bunch later. A strict provider-time request ceiling needs
an operation-level gate near dispatch, or enforcement by the receiving service.
Do not label the queue API as a general HTTP rate limiter.

### Scope and defaults

- Use explicitly named, globally scoped keys, such as
  `customer:123:operations`, `provider:abc:requests`, and `fleet:main`.
  This follows existing resource-key scope; organization IDs do not silently
  namespace them. The embedding application controls who may choose keys.
- Store limits centrally. Jobs reference policy identities, not a caller-supplied
  number that can differ between workers. Updating a limit affects future
  admission decisions without rewriting the backlog.
- Require all referenced policies. Unknown, paused, unsupported, or invalid
  policies fail closed. Do not create an unlimited policy implicitly.
- Use unit-cost admissions initially: one permit or rate event per requirement.
  Weighted requests, recursive hierarchies, wildcard matching, and per-customer
  fair scheduling are separate extensions.
- Keep concurrency and rate as separate policy kinds with one shared admission
  boundary. They have different release and retention rules.
- Provision a concrete policy per customer initially. Automatic templates and
  family-level defaults need a separate consistency and lifecycle design.

Explicit attachment makes the first API predictable, but is not automatic fleet
enforcement: a producer that omits the binding creates unconstrained work. The
first release must say this directly. Mandatory job-type bindings could later
close that omission path; they must specify behavior for already-pending jobs.

## Public API direction

These are illustrative names, not APIs implemented in this repository:

```rust,ignore
let customer = CapacityPolicyKey::new("customer:123:operations")?;
let provider = CapacityPolicyKey::new("provider:abc:requests")?;

ensure_capacity_policy(&pool, &customer, ConcurrencyLimit::new(20)?).await?;
ensure_capacity_policy(
    &pool,
    &provider,
    RollingWindowLimit::new(100, Duration::from_secs(60))?,
).await?;

let requirements = JobCapacityRequirements::new([customer, provider])?;
enqueue_job_with_options_tx(
    &mut tx,
    &job,
    &JobEnqueueOptions::new().capacity(requirements),
).await?;
```

`ensure` should verify an existing definition matches, rather than let worker
startup overwrite policy changes. An explicit update operation should require
the expected revision and record the actor/reason. A stale update must fail.

`JobEnqueue` is a public struct used through literals. Adding a required field
breaks downstream compilation. Prefer a new options-based API while retaining
existing enqueue functions as empty-capacity adapters. Its options also need to
compose with the existing execution resource; avoid a matrix of functions for
each combination of constraints.

Add builder methods to workflow job steps and enqueue intents. Reject capacity
on non-job steps. Normalize and sort keys, reject duplicates, and cap the number
of requirements per job to bound database work. Use the existing non-blank,
512-byte resource-key convention unless the contract review identifies a reason
to differ. The exact requirement-count cap needs the contention probe.

The upstream API additions also expose an owned
[`JobSubmission`](../../runledger-core/src/jobs/submission.rs), its PostgreSQL
`JobEnqueue` adapter, and fluent workflow step configuration through
[`WorkflowDagBuilder`](../../runledger-core/src/jobs/workflow_enqueue/dag_builder.rs).
Capacity options must compose with these entrypoints. Check both owned submission
and borrowed enqueue struct-literal compatibility rather than adding fields to
only the persistence type. Workflow step persistence now uses
[`steps/batch.rs`](../../runledger-postgres/src/jobs/workflows/steps/batch.rs);
preserve requirements in that batched write path as well as snapshot encoding.

Keep `max_global_concurrency` behavior intact. Document it as per worker loop now;
use `max_worker_concurrency` in a future breaking API change. An environment alias
can be additive, but precedence and conflicting-value behavior must be documented.
Do not add two independent public fields that can disagree.

## Storage direction

Use new tables; preserve `job_execution_resource_claims` unchanged initially.
Capacity-one policies and resource keys may share internal helpers later, but
silently mapping legacy keys into a new namespace would change durable behavior.
A job using both mechanisms must acquire both atomically.

| Proposed relation | Essential contents and rationale |
| --- | --- |
| `job_capacity_policies` | Stable ID, unique key, kind, positive limit, optional fixed period, paused flag, revision, update audit metadata. This is the admission serialization row. |
| `job_capacity_requirements` | Job ID plus policy ID, unique per pair. Immutable for an existing execution request; indexed both ways. |
| `workflow_step_capacity_requirements` | Step-to-policy references, copied into queue requirements on release. Keeps unreleased steps constrained. |
| Intent and schedule bindings | Durable references/serialized keys retained before queue creation. Follow their existing canonical request and catalog update rules. |
| `job_capacity_permits` | Policy ID, admission UUID, exact lease identity, lease deadline, optional `release_after`, acquisition time. Indexed by policy and by owner. |
| `job_capacity_rate_admissions` | Policy ID, admission UUID, admitted timestamp, policy revision, optional job audit linkage. Indexed by `(policy_id, admitted_at)`. |

Use permit rows as the source of occupancy initially. Under the policy lock,
count all retained permit rows for the policy. Do not exclude rows merely because
the lease deadline passed: a live queue owner must be reaped first, and a canceled
owner must reach its retained deadline. Delayed cleanup reduces utilization but
does not allow extra owners.

This avoids a mutable `in_use` counter whose correctness would depend on every
heartbeat, release, cancellation, reaper, replay, and administrative path. A
serialized counter or explicit slots remain alternatives if measurements justify
their additional reconciliation burden.

Rate history must survive job/attempt deletion until outside its policy window.
Do not cascade rate consumption away with queue retention or prestart recovery.
Use an admission UUID independent of `(job_id, run_number, attempt)`, because
prestart recovery can reuse that tuple. Job audit linkage may become null when
retention removes the source; the consumption itself remains.

Do not delete a policy while any queue, workflow, intent, schedule, permit, or
retained rate record references it. Pause first; explicit cleanup can follow.
Never reuse a deleted policy identity as though its rate history were empty.

## Admission transaction and locking

PostgreSQL's default READ COMMITTED isolation gives each command a new snapshot;
it does not make an unprotected count-and-insert mutually exclusive. The
recommendation is to serialize admission on stable policy rows, then read
occupancy in a separate command. This design follows the documented
[snapshot behavior](https://www.postgresql.org/docs/18/transaction-iso.html).

Proposed sequence for one capacity-constrained candidate:

1. Select and lock an eligible pending job using current ordering and type filters.
2. Read its immutable requirements; use a bounded acquisition path for its legacy
   execution resource if present.
3. Lock every required policy in canonical ID order. Use nonblocking row-lock
   acquisition for the queue path. If any cannot be locked, roll back the
   candidate and try other work rather than occupy a worker slot waiting.
4. In a separate statement after all locks are held, read current permits and
   rate admissions. Read policy status/limits from the locked rows. Missing or
   unsupported requirements reject admission.
5. Sample database time after locking, evaluate every policy, and only then write
   the semaphore permits and rate events with one fresh admission UUID.
6. Transition the job to `LEASED`, increment its attempt, and write the ordinary
   workflow transitions, attempt row, and event. All effects commit together.
7. Return the leased job only after commit. Any error rolls back all capacity
   writes for that candidate.

The first prototype should use one candidate transaction at a time for constrained
jobs. Keeping earlier policy locks while acquiring later jobs in a batch can
invert lifecycle lock order. An optimized batched protocol needs its own proof;
sorting policies within each job is not a proof across the entire transaction.
Retain the current fast path for unconstrained jobs, with an explicit predicate
excluding any job with capacity requirements.

There is an additional batching API question: the existing claim call commits
its whole batch atomically. If a replacement commits candidates individually,
it must return already-committed claims when a later candidate fails, never lose
them behind an `Err`. This return/error contract is a prototype gate, not an
unannounced implementation change. Cancellation during a call can still leave a
committed prestart lease; recovery must handle it as today.

Do not combine the lock and occupancy check into a clever single CTE and assume
the post-wait snapshot is fresh. The probe uses separate statements. A production
function or statement must establish equivalent semantics explicitly.

Add a database lease guard, following the existing resource guard, that verifies
all required permits/rate admission records and the exact lease identity before
leasing constrained work. Supported claim APIs must share the admission path;
an older writer must fail closed rather than ignore requirements. This is not a
security boundary against a database owner who can manufacture coordination rows
or disable triggers.

Release and heartbeat should mutate only the exact owner's permit rows; they
need not acquire the shared policy row when occupancy is derived by counting.
Concurrent release can make a count conservative. New permit creation always
requires the policy lock. Policy updates must not acquire job locks while holding
policy locks. Verify the full order against workflow cancellation, retention,
resource acquisition, and lifecycle triggers before committing to this protocol.

### Candidate scanning and fairness

Skipping a blocked candidate is necessary but insufficient: repeatedly scanning
the same dense customer prefix can starve unrelated jobs. Preserve the existing
priority/date/ID ordering, but continue through a bounded keyset window and test
dense prefixes exceeding that window. Any persistent cursor must specify reset
behavior so new high-priority jobs are considered promptly.

Do not mutate `next_run_at` to represent capacity denial. It already represents
application scheduling and retry timing. A later optimization can add a separate
admission eligibility hint; it must be invalidated by policy changes and early
permit releases. A denial hint is never authority for granting admission.

There is no strict FIFO or starvation-freedom guarantee in the initial proposal.
Polling remains the correctness fallback. Notifications may reduce latency, but
must not be the only way blocked work wakes. Bound scans, transaction duration,
and retry work independently of the number of pending jobs.

## Rate algorithm choice

| Algorithm | Semantics and tradeoff |
| --- | --- |
| Fixed window | Simple count per calendar bucket; can admit 100 just before a boundary and 100 just after it. Does not implement 100 in every trailing minute. |
| Token bucket | Explicit refill rate plus burst allowance. Constant-sized state, but a bucket initially containing 100 tokens with refill 100/minute can admit more than 100 in a trailing minute. |
| Exact rolling window | Keep admitted timestamps and allow a new event only while fewer than 100 fall in `(now - 60 seconds, now]`. Matches the strict reading of the example; costs history and indexed reads. |
| GCRA/pacing | Compact scheduling state; useful for smoothing, but burst tolerance is part of its contract and must be stated. Defer until demanded. |

Token buckets are a valid distinct product feature. The
[API Gateway documentation](https://docs.aws.amazon.com/apigateway/latest/developerguide/api-gateway-request-throttling.html)
likewise separates refill rate and burst capacity. Do not silently translate a
strict rolling-minute policy into those two different parameters.

For the first rate feature, recommend exact rolling admissions with unit cost.
Under the same policy lock as concurrency admission, count unexpired timestamps
and insert only after all requirements pass. Use one sampled time for every rate
requirement on a candidate. PostgreSQL distinguishes transaction-start `now()`
from actual current time via `clock_timestamp()`; sample the latter after
acquiring locks. See the [PostgreSQL 18 time functions](https://www.postgresql.org/docs/18/functions-datetime.html).

The guarantee is expressed in database admission time, not transaction commit
time or handler execution time. Long transactions must be bounded; timestamps
from delayed commits should never be presented as precise dispatch timestamps.
Clock discontinuities and database failover clock skew need explicit tests and
documentation. A per-policy nondecreasing time floor can make backward movement
conservative; it cannot establish physical elapsed time through arbitrary forward
clock jumps.

Consume rate at admission for both direct and prestart claims, including claims
that crash before execution. Do not refund on failure, cancellation, or prestart
release. This may underutilize a provider quota, but has one auditable boundary
and avoids proving whether an external effect happened. A retried job or continued
slice needs a new admission and pays again.

Keep policy kind and window duration immutable initially. Changing a window from
one minute to one hour after minute-old history was deleted cannot enforce the
larger trailing window retrospectively. Limit reductions pause new admissions
until occupancy/history fits the new limit; they do not revoke running work or
retroactively change past decisions. Raising a limit opens capacity on the next
poll. Rate-history cleanup uses the immutable period and bounded indexed batches.

## Lifecycle matrix

| Transition | Concurrency | Rate consumption |
| --- | --- | --- |
| Denied admission | No permit, no attempt increment, no handler slot | No event |
| Transaction rollback | All candidate writes roll back | All candidate writes roll back |
| Claim succeeds | Reserve for exact lease owner | Insert once for this admission |
| Heartbeat | Renew exact permit deadline with job lease | Unchanged |
| Handler succeeds/fails | Release through committed lease transition | Retain until window expiry |
| Retry | Release old permits; reacquire on next claim | New admission consumes again |
| Continuation | Release between slices; preserve requirements | New slice consumes again |
| Unstarted claim release | Release exact permits despite attempt decrement | Retain consumed admission |
| Live cancellation | Hold until previous lease deadline, as legacy resources do | Retain consumed admission |
| Worker crash/lease expiry | Reap queue owner before releasing its permits | Retain consumed admission |
| Stale worker heartbeat/completion | Cannot renew or release successor permits | Cannot erase history |
| Replay/workflow recovery | Preserve references, acquire fresh permits | New admissions consume again |
| Queue retention | Must not prematurely remove canceled retained permits | Must not remove unexpired history |

Retained cancellation permits make queue deletion a correctness concern. Do not
copy an `ON DELETE CASCADE` blindly: either the existing retention contract forbids
deletion until quiescence and is enforced for these rows, or a new guard must
preserve them. Verify admin deletion paths as well as normal cleanup.

## Durable request propagation and rollout

Capacity references must survive direct enqueue, idempotent enqueue, enqueue
intent recording and promotion, workflow enqueue/append/release, continuation,
direct replay, workflow recovery, and schedule firing. Release policies when a
job's lease ends, not when a workflow run ends; whole-workflow quotas would be a
different feature from per-operation limits.

Include the normalized requirement set in canonical enqueue/append snapshots and
conflict comparisons. Persist policy identity/key membership; do not freeze its
mutable numeric limit into every job. Record the policy revision at admission
for audit. A replay preserves membership while using current policy limits.

Legacy snapshots without capacity must decode as an empty requirement set. Direct
enqueue intents already carry request-version machinery; update it deliberately
with old-version fixtures. Workflow recovery has strict unknown-field decoders,
so every recovery binary must understand the new field before it is emitted.
Appending fields to JSON is not sufficient rolling-deployment compatibility.

Suggested rollout:

1. Apply additive schema and compatibility/lease guards across the three migration
   copies (`migrations/`, `runledger-postgres/migrations/`, and
   `runledger-test-support/migrations/`). Existing empty-capacity work remains valid.
2. Deploy all claimers, producers, promoters, schedulers, replay/recovery callers,
   lifecycle writers, and reapers with support present but capacity unused.
3. Quiesce old binaries and leases using the repository's existing staged rollout
   practice. Old claimers can otherwise encounter constrained work, fail a guard,
   and repeatedly roll back a mixed batch, harming unrelated work.
4. Enable a small set of concurrency policies; then broaden after metrics show
   correct admission and release. Activate rate policies as a later feature gate.
5. For code rollback, retain the schema and policies, pause constrained producers,
   and drain/reconcile constrained state before admitting old workers. Do not
   strip bindings or down-migrate active coordination state to make rollback pass.

Refresh SQLx metadata only against PostgreSQL 18 with current migrations applied;
keep `.sqlx/`, `runledger-postgres/.sqlx/`, and `runledger-runtime/.sqlx/` synchronized.
Schema compatibility helpers and migration-history behavior also need coverage.
The upstream [`migration identity API`](../../runledger-postgres/src/migration_identity.rs)
exposes bundle/pipeline identity; additive capacity migrations must be reflected
in those identities and their validation fixtures too.

## Observability and operational cost

Expose policy limit/revision/status, outstanding permits, retained cancellation
permits, rate-window usage, and blocked-job samples. Distinguish capacity exhausted,
policy paused, lock contention, missing policy, and internal admission failure.
Denial is expected scheduling behavior, not a failed job attempt.

Use aggregate metrics by policy kind/reason; avoid unbounded customer-key metric
labels. Make specific keys available through paginated diagnostic reads and
structured logs. Avoid writing a durable job event on every denied poll. Preserve
the existing ordinary lease audit writes and add admission identity linkage.

Each required concurrency policy adds a permit row and lifecycle work. A globally
shared policy serializes admissions across the fleet. Rolling windows add one
history row per policy per successful admission within their retention periods.
These are workload-dependent costs, not throughput claims. Measure before
choosing a counter, slot table, token bucket, or external limiter.

Redis would introduce another service and a cross-store reservation/lease failure
boundary. PostgreSQL advisory locks alone are transaction/session-owned rather
than durable lease ownership. Explicit slot rows can work for fixed capacities,
but resize and retained-owner behavior become more complicated. None is the
default recommendation without evidence that the simpler permit-row model is
insufficient.

## Experiment performed

Ran [`probes/capacity_locking.py`](probes/capacity_locking.py) against an isolated
`postgres:18` container. The script asserts the server major version and builds
only a small diagnostic schema; it does not migrate or exercise Runledger.

Recorded server version: **18.4 (Debian 18.4-1.pgdg13+1)**.

| Experiment | Observed result |
| --- | --- |
| 32 concurrent sessions read occupancy before a barrier, then independently insert against a limit of 20 | 32 permits: unsafe count-and-insert oversubscribed. |
| 32 concurrent sessions lock the policy row, then count/insert in a separate READ COMMITTED command | 20 permits. |
| Insert an additional permit then roll back | Count remains 20. |

The advisory lock in the unsafe test is solely a deterministic test barrier. It
is not proposed as the production capacity mechanism. The safe test uses blocking
policy locks to force serialization; the queue's proposed nonblocking behavior,
multiple requirements, workflow locks, and lifecycle triggers remain untested.

Reproduce using a fresh disposable container:

```sh
docker run --detach --rm --name runledger-capacity-probe \
  -e POSTGRES_HOST_AUTH_METHOD=trust postgres:18
# Wait for pg_isready to report accepting connections.
docker exec runledger-capacity-probe pg_isready -U postgres
python3 docs/research/probes/capacity_locking.py runledger-capacity-probe
docker stop runledger-capacity-probe
```

The probe intentionally replaces its own `capacity_probe` schema. Never point it
at a shared database container. No port is published by the commands above.

## Dependency-aware implementation outline

These stages describe the recommended sequence, not ready-to-claim implementation
tickets. Each needs a self-contained implementation contract after the prototype
gates below. Concurrency can ship before rate support.

| Stage | Work and acceptance | Depends on | Unblocks |
| --- | --- | --- | --- |
| A | Define policy/key API, exact admission guarantees, missing-policy behavior, update rules, binding scope, and compatibility fixtures. Explain per-loop worker capacity in public docs. | Investigation | B, C |
| B | PostgreSQL 18 prototype combining real claim, workflow, resource, heartbeat, cancellation, reaper, and retention paths. Resolve batch error contract, lock order, scan bounds, and clock handling; record server version. | A | D |
| C | Specify versioned durable references through direct jobs, intents, workflows, schedules, continuation, replay, and recovery. Verify old snapshots decode empty and changed bindings conflict. | A | D |
| D | Implement additive schema, policy management, requirement persistence, guards, and compatibility validation. Synchronize migrations and metadata. | B, C | E |
| E | Implement unit concurrency admission, exact-owner lifecycle cleanup, runtime integration, diagnostics, and legacy resource composition. | D | F |
| F | Prove concurrency release/cancellation/crash correctness and bounded claim behavior under contention; validate old-worker rejection and rollback runbook. Ship concurrency. | E | G |
| G | Add rolling rate admissions, durable history/cleanup, time-boundary tests, no-refund semantics, and rate-specific diagnostics. | F | H |
| H | Validate combined limits, backlog workloads, schedules/recovery, fault injection, performance comparison, and staged rate activation. | G | Completed expansion |

### Required integration cases before calling the design proven

- Many workers sharing customer A never hold more than 20 A permits; customer B
  can progress while A is saturated. Use independent pools/processes as well as
  concurrent tasks, and sample retained permits throughout execution.
- Opposite-order multi-policy jobs acquire all requirements or none, including
  interaction with legacy resource keys. A denied rate requirement cannot leak
  a concurrency permit, and a denied concurrency requirement consumes no rate.
- Race heartbeat, completion, cancellation, replay, policy reduction, and reaper.
  Include a canceled owner whose handler is still running and a stale heartbeat
  arriving after a new lease is admitted.
- Inject failure before lease update, after admission writes, during attempt/event
  writes, and at ambiguous commit. Verify audit/permit consistency and prestart
  recovery, including reuse of the same attempt number and worker ID.
- Exhaust a 100/60-second rolling policy; reject the next admission until exactly
  the relevant boundary. Verify no fixed-window boundary burst. Test backward
  clock handling, no refunds, paused policies, and rate-history retention.
- Feed dense blocked prefixes larger than the scanner window; verify unrelated
  tenants and newly inserted higher-priority jobs progress within the documented
  scheduling contract. Inspect generic/custom plans, as existing claim parity
  tests do, and compare unconstrained-job behavior against the baseline.
- Exercise old workers/writers against the new guard, strict recovery decoders,
  unknown policy kinds, legacy idempotent requests, and all migration copies.
- Cover every durable producer and recovery route. Do not ship schedule support
  that silently fires unconstrained jobs from a constrained schedule definition.

No throughput target is asserted. Record admission latency distributions,
successful claims/second, scanned candidates/success, lock contention, heartbeat
latency, cleanup backlog, and database write load for unconstrained, dispersed,
and single-hot-policy workloads before making performance commitments.

### Planning gates and implementation handoff

1. `runledger-cod` is complete: the PostgreSQL 18 protocol model and its evidence
   resolve the planning questions; full Rust integration remains P14/P19 work.
2. `runledger-ze4` is complete: the execution plan and proposed old/new fixtures
   specify durable APIs, compatibility, explicit attachment, and rollout.
3. `runledger-bva` tracks the reviewed plan and conversion. The implementation
   epic is `runledger-distributed-capacity-bp2`; its dependency graph preserves
   concurrency release before rate implementation. Consult Beads for live status.

This sequence keeps the immediate product improvement focused: shared customer
concurrency first, explicit admission rates second, and request-level dispatch
control only if the application needs a stronger boundary than job admission.
