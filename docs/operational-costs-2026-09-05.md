# Operational reads and workflow enqueue costs

This records the API-006 measurement/design work tracked by
`runledger-runledger-simplification-audit-kqm`. The implementation adds scoped
compact pages, exact job-type filtering, batch status lookup, and set-based
initial/append graph inserts. Direct-job `enqueue_many_tx` remains a design
below, as requested by the audit's investigation scope. No new wrapper merely
hides the existing per-job loop, and no shared result listener is introduced.

## Measurements

Server: **PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2)**,
`server_version_num=180006`, official `postgres:18` container. All current
migrations were applied. Client: Rust debug test profile, SQLx 0.8.6, local TCP,
one pool connection, sequential requests, default PostgreSQL durability settings,
`pg_stat_statements` enabled. This shared development machine was not isolated
from other workloads. These are synthetic latency measurements, not production
throughput or capacity estimates.

Baseline persistence source was commit `4844fe3`, before the new indexes and
batching. The diagnostic added only fixture/measurement code. Each read has
one warmup and 31 measured samples; each write case has one warmup and 11
measured samples, including transaction commit. Fixture construction, workflow
builders, and cursor discovery are outside the measured regions. Workflow
validation, snapshot creation, graph writes, and root release are inside them.
The reported p95 is the sorted sample at `floor(n * .95)`; for 11 samples this
is the maximum, not a statistically robust tail estimate.

Raw evidence: [baseline](measurements/operational-costs-2026-09-05/baseline.txt),
[first after run](measurements/operational-costs-2026-09-05/first-after.txt),
[second after run](measurements/operational-costs-2026-09-05/second-after.txt),
[final after run](measurements/operational-costs-2026-09-05/after.txt), and
[actual prepared query plans](measurements/operational-costs-2026-09-05/plans.txt).
The first after run includes a 737.573 ms small-graph outlier; it is retained,
not discarded. The final run includes the direct-job experiment, JSON-null preservation in the
shipped INSERT, and a prepared-statement selector that cannot accidentally select
its own inspection query. Its 100/390 graph sample also has a 275.887 ms tail outlier.

### Reads

The fixture has 10,000 global jobs, each with a 4,096-character deterministic
varied string in each of payload, checkpoint, and output. Pages have 100 jobs.
No status/type filter is applied for the timing comparison.

| API/path | Baseline median / p95 ms | Final median / p95 ms |
| --- | ---: | ---: |
| Full page, offset 0 | 20.422 / 23.199 | 14.819 / 18.150 |
| Full page, offset 9,000 | 20.490 / 22.162 | 18.316 / 21.799 |
| Compact first page | — | 2.250 / 2.809 |
| Compact cursor at depth 9,000 | — | 1.912 / 2.758 |
| Compact raw cursor at depth 9,000 | — | 1.481 / 1.796 |
| Compact raw offset 9,000 | — | 4.679 / 5.539 |

The raw comparison derives its SQL from the actual public prepared cursor
statement, replaces only pagination, fetches both into the same SQLx row type,
and asserts identical IDs. This separates cursor access from projection/typed
decode differences. The final compact first page is about 6.6 times faster
than the final full first page on this fixture; the raw deep cursor is about
3.2 times faster than its offset counterpart. Wider/narrower real payloads,
selective filters, cache state, and network distance change these ratios.

Full pages materialize **1,232,100 JSON bytes** per page, calculated by compact
serialization of the three returned JSON fields outside the timed region.
Compact pages read **zero JSON fields/bytes**. This is not a PostgreSQL protocol
byte count or an allocator profile: both paths still allocate row vectors and
identifier strings. No claim about total heap allocation counts is made.
Each public page and nonempty batch status lookup executes one SELECT and uses
one query connection; an empty status lookup executes none. The diagnostic pool
remained at one connection. It does not measure concurrent connection pressure.

The plan regression uses 30,000 rows spread across global and two tenant scopes.
It EXPLAINs the public prepared statement under both `force_custom_plan` and
`force_generic_plan`. All three scopes constrain an index with the cursor tuple;
the recorded plans use 3–5 shared buffer hits for a 20-row page near the end.
Optional exact type and status predicates may remain residual filters, especially
with generic plans. No claim is made that every filter distribution has constant
scan cost; specialized covering/filter indexes need workload evidence.

### Graph writes

Graphs have one root job. Each later step depends on the previous one or up to
four preceding steps, exercising chains and denser DAGs. Every request is new
and unkeyed; each root retains its ordinary queue insertion and ENQUEUED event.

| Steps / edges | Baseline median / p95 ms | Final median / p95 ms | Graph INSERT executions before → after |
| --- | ---: | ---: | ---: |
| 10 / 9 | 13.449 / 16.268 | 13.083 / 57.564 | 19 → 2 |
| 10 / 30 | 17.232 / 20.165 | 13.475 / 33.997 | 40 → 2 |
| 100 / 99 | 70.956 / 80.737 | 23.513 / 51.518 | 199 → 2 |
| 100 / 390 | 142.869 / 153.638 | 42.835 / 275.887 | 490 → 3 |
| 600 / 599 | 378.741 / 409.424 | 147.887 / 203.130 | 1,199 → 6 |
| 600 / 2,390 | 816.635 / 859.004 | 335.263 / 453.667 | 2,990 → 13 |

Statement counts are measured with `pg_stat_statements` over all 12 executions,
then divided by 12. They count awaited SQL executions, not TCP packets or
statements executed inside triggers. Each case retains 14 other top-level
statements per enqueue, including transaction control. The graph portion changes
from `V + E` to `ceil(V / 256) + ceil(E / 256)`. Empty edge sets execute no
dependency INSERT. Small graph improvements are inconsistent across runs: the second after run
had a 14.975 ms median for 10/9, slower than the 13.449 ms baseline. The largest graph improved about 2.4 times despite far fewer
statements, because graph validation, JSON serialization, constraints, indexes,
triggers, snapshot work, and root release remain.

The same batched writers serve append, whose existing dependency-counter and
mutation-outcome logic is preserved. Append correctness is tested across chunk
boundaries; the timing table measures initial enqueue only. Fanout graphs with
many ready job roots still pay per-root queue/audit costs. The new queue indexes
also add storage and maintenance work to queue writes; this experiment does
not isolate that overhead or measure index-build duration on a production table.

### Independent direct jobs

The final diagnostic also enqueues 100 new, unkeyed jobs using existing APIs:

| Transaction ownership | Median / p95 ms | SQL executions per 100 jobs |
| --- | ---: | ---: |
| Each `enqueue_job_with_outcome` owns its transaction | 895.000 / 1124.848 | 400 |
| 100 `enqueue_job_with_outcome_tx` calls, one caller transaction | 77.239 / 94.182 | 202 |

This isolates transaction amortization on this machine. It changes the failure
boundary: individually committed jobs can survive a later failure, whereas the
caller must roll back the whole group on error for atomic submission. Both
still execute one queue INSERT and one event INSERT per new job. It is evidence
for deliberate transaction composition today, not a measured set-based direct
enqueue implementation. Keyed contention, disabled definitions, duplicates,
payload sizes, and application-side row locking were not timed in this case.

## Shipped contracts and compatibility

`list_job_summaries(pool, &JobSummaryFilter)` requires an application-authorized
`JobReadScope`, optional status, optional exact case-sensitive `JobType`, limit
1–1,000, and optional exclusive `(created_at, id)` cursor. Wildcard characters
in a job type are literal. The compact record includes identity, scope, status,
priority, run/attempt counters, retry time, stage/progress, and timestamps, without
payload/checkpoint/output or free-form errors. Callers retain the detail API.
Cursor timestamps must retain PostgreSQL microsecond precision. Cursors need no
live anchor row; deleting the previous page's last row does not invalidate them.
Keep scope/filters fixed, and treat pages as changing observations: a status
transition can enter/leave a filter, and new rows ahead of a cursor are excluded.
Application ownership joins and authorization remain application-owned.

`get_job_statuses_with_scope(pool, scope, ids)` accepts at most 1,000 IDs, including
duplicates in that bound. Empty input performs no query. Each visible ID appears
once, in ascending ID order; absent and out-of-scope IDs are indistinguishably
omitted. Status/run/attempt observations do not grant lease or recovery authority.
Legacy offset/substring/detail APIs keep their contracts.

Graph writes serialize at most 256 borrowed step records or dependency records
per statement into `jsonb_to_recordset`, with SQLx-checked columns and bound JSON.
The row bound limits scratch records and statement size growth with graph count;
it is **not a hard byte limit on caller payloads**. Existing callers can still
submit large individual payloads. IDs remain database-generated and are mapped
by step key; no dependence on RETURNING row order is introduced. Append results
remain in input order. The entire graph, root jobs, snapshots, mutation record,
and events share the existing transaction. Owned APIs commit all or roll back
on error; `_tx` callers retain transaction ownership and must roll back on error.
There are no per-chunk commits, partial success outcomes, or concurrent tasks
sharing a transaction. Initial keyed retries still reuse the original run only
after canonical snapshot equality, and append still reports `Appended` or
`AlreadyApplied`. Active-key outcomes remain unchanged.

Definition validation/locks, run coordination locks, append step locks,
dependency orientation/release modes, per-step tenant/default/override policies,
continuation/resource fields, immutable snapshots, ordinary audit writes, lease
fences, and terminal propagation use the existing paths. Only graph INSERT
execution is grouped. No claim/recovery/fencing SQL is changed.

Migration `202609050001_job_summary_pagination` adds
`(organization_id, created_at DESC, id DESC)` and `(created_at DESC, id DESC)`
indexes. It is additive and omitted from `runledger_migration_history` so older
filtered startup helpers can coexist; SQLx history still tracks/checksums it.
Apply it before deploying this build: the current startup guard requires it,
including during an expand-only workflow/job-link rollout. These are ordinary
transactional CREATE INDEX statements and block queue writes until commit;
schedule an appropriate deployment window. The down migration drops only these
indexes. The independently calculated manifest fingerprint is updated, and root,
packaged migrations, and all three SQLx caches are synchronized on PostgreSQL 18.

## Bounded direct-job batch design

The audit asked to investigate `enqueue_many_tx`, not commit to a new concurrency
protocol. The graph improvement above is shipped. The following is the concrete
contract/design for a subsequent direct-job API; it is not exported in this change.

1. Accept at most 256 indexed entries and 1 MiB of encoded canonical requests
   per call, validating the full input before any writes. Include optional
   execution-resource keys in the canonical request using the existing snapshot
   format. Empty input succeeds without SQL. Bound violations are validation
   errors; do not silently split one atomic request into separately committed work.
2. Require READ COMMITTED. The owned wrapper commits all entries together.
   The `_tx` wrapper creates a savepoint, releases it on complete success, and
   rolls back to it on any per-item validation/conflict/SQL failure. It returns
   the first failing input index with a classified error, not a partially
   successful vector. Transaction/rollback failure makes the outer transaction
   unusable and must be reported. This is an explicit stronger atomic-call
   guarantee than merely looping the current `_tx` helper.
3. Return `Vec<JobEnqueueOutcome>` in input order only on whole-batch success.
   Repeated identical keyed requests share one row: first occurrence is Inserted
   if new and later occurrences Existing. Different snapshots for the same
   `(scope, job_type, key)` reject the whole call before writes. Every unkeyed
   entry creates its own job. Existing means the observed status/run number,
   not a payload refresh or requeue.
4. Deduplicate keys before insertion. Acquire definition SHARE locks in job-type
   order, insert in a consistent `(global/tenant, organization UUID, job type,
   key, input ordinal)` order, and acquire existing job mutation-ready locks in
   a consistent order. Use the existing global/tenant partial unique indexes
   as the authority shared with single-job writers. Do not invent an advisory
   key scheme respected only by batch callers. This reduces batch-to-batch
   inversions; it cannot eliminate deadlocks from locks already held by application
   code or differently ordered single-item loops. Retry the whole transaction
   on a classified deadlock, never an unknown partial subset.
5. Use bounded set-based queue inserts and event inserts from returned new IDs.
   After `ON CONFLICT DO NOTHING`, resolve conflicts in a **second statement**
   so READ COMMITTED can see a row committed while the unique insert waited.
   A same-statement INSERT/SELECT CTE fallback can miss that row. Compare immutable
   `enqueue_request`, never mutable payload/checkpoint/live options. Preserve the
   current ability to return an identical existing keyed job even if its definition
   was subsequently disabled; missing/disabled definitions only reject entries
   that cannot resolve an existing request. Missing legacy snapshots still fail.
6. Emit exactly one ordinary ENQUEUED event for each newly inserted job and none
   for retries. Preserve execution-resource fields and normal downstream claiming;
   outcomes must never bypass lease identity or typed compare-and-requeue fences.

Before implementing/exporting this design, benchmark it against the measured
202-statement caller-transaction loop and test mixed new/existing/duplicate
entries, opposite-order concurrent batches, single/batch races, a conflict in
the final item, commit failure, application locks, disabled definitions, and
rollback retaining unrelated caller writes. The source behavior requiring these
rules is in `queue/enqueue.rs` (`enqueue_job_with_existing_lock_tx_inner`,
`resolve_existing_idempotent_job_tx`, and `load_existing_idempotent_job_tx`).
OneSales/IdentityPro loop sites in the [source audit](api-audit-2026-09-05.md)
are candidate consumers; no measured production enqueue rate is inferred from them.

Shared workflow-result listeners remain secondary. The source audit found no
sampled production wait consumer; this work neither changes LISTEN/poll fallback
behavior nor claims reduced waiter connection use.

## Reproduction and validation

Start an owned diagnostic server and pass its URL to the ignored diagnostic.
The test helper creates, migrates, and drops an isolated database; the extension
and statistics reset require the diagnostic server's administrator role.

```sh
docker run -d --name runledger-costs-pg18 \
  -e POSTGRES_USER=runledger -e POSTGRES_PASSWORD=runledger \
  -e POSTGRES_DB=postgres -p 127.0.0.1::5432 \
  postgres:18 -c shared_preload_libraries=pg_stat_statements
docker port runledger-costs-pg18 5432
# Substitute the reported local port below.
RUNLEDGER_TEST_ADMIN_DATABASE_URL=postgres://runledger:runledger@127.0.0.1:PORT/postgres \
  cargo test -p runledger-postgres --test operational_costs -- --ignored --nocapture
docker rm -f runledger-costs-pg18
```

The baseline can be reproduced in a detached checkout of `4844fe3`: copy the
current `operational_costs.rs` test into that checkout and remove the
`compact_reads` and `direct_jobs` function definitions and their calls. The
remaining read/graph fixture, warmups, sample counts, and timers are the baseline
harness. Use the same server/profile and avoid concurrent test workloads during
timing. SQLx preparation uses the ordinary refresh script against a separate
PostgreSQL 18 database with the current root migrations applied.

Behavior tests cover exact scopes/types/statuses, literal wildcards, tied
timestamps, deleted anchors, concurrent newer insertion, input bounds, duplicate
and missing IDs, and custom/generic cursor plans. Graph tests cross 256-row
step/edge boundaries, preserve JSON null payloads through recordset conversion,
assert edge orientation/fields/audit/snapshot reuse and
append outcomes/order, and inject later-chunk failures to prove owned rollback.
Existing default/nullability, dependency propagation, concurrent completion,
active-key, recovery, resource, and idempotency tests remain part of the workspace
regression suite. Timing is a manual diagnostic, not a flaky CI speed assertion.

Validation completed: the workspace suite passed 909 tests (four intentionally
ignored). The final JSON-null SQL change passed 12 focused workflow tests; the
final upgrade fixture passed all 21 migration tests. `scripts/lint.sh` and the
PostgreSQL 18 SQLx refresh/package-cache checks passed. See the
[verification record](measurements/operational-costs-2026-09-05/verification.txt).
