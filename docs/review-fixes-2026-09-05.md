# Branch review fixes and scope decisions

This follow-up addresses the review of `3ed1404..b724223`. It covers scoped
list reads, handler progress persistence, typed worker integration, and the
producer prelude. Rust 1.88/edition 2024 and PostgreSQL 18 remain the baseline.
Public signatures, persisted formats, lease fencing, and legacy scope meanings
remain compatible. Generated SQLx metadata is refreshed from current migrations.

## Open-question research

- API-004 in `api-audit-2026-09-05.md` explicitly proposed scoped job, event,
  log, and intent inspection. The implemented surface matches that list.
  `get_job_metrics` and `get_job_continuation_metrics` apply optional organization
  filtering to rollup views; `get_job_enqueue_intent_metrics` filters three
  lifecycle populations. For all three, `None` intentionally aggregates global
  and tenant rows. The TUI dashboard uses that legacy aggregation contract.
  Exact-global metrics are an additive coverage gap, not a reason to change
  existing callers' results.
- Both payload helpers require a tenant UUID. The queue has separate tenant
  and global idempotency indexes; the same key can legally exist in multiple
  scopes. An admin wildcard on a single-result key lookup would be ambiguous.
  A future global lookup needs an exact scope contract without an admin variant.
  The latest-payload helper's `run_id` is a JSON field, not a globally unique
  queue identity. Legacy signatures and behavior stay intact. Follow-up:
  `runledger-runledger-simplification-audit-60g`.
- The producer outcome helper was exported through `jobs` but omitted from
  `prelude` when introduced in `b724223`. There is no documented exclusion,
  and the transaction counterpart is already in the prelude. Exporting it is
  additive; both the producer integration test and packaged consumer now use it.

## Design diagnosis and changes

### Scope predicates: a translation mistake exposed by weak performance coverage

The `JobReadScope` enum is an appropriate closed set. Translating it into a
boolean plus nullable UUID hid the distinct SQL access paths in
`jobs/admin/read.rs` and `jobs/queue/intents.rs`. `IS NOT DISTINCT FROM` gives
correct visibility but did not constrain the organization index in the
PostgreSQL 18 diagnostic. Merely replacing it with equality inside a wildcard
OR would still depend on custom-plan simplification.

The fix selects one statement per enum variant: equality for a tenant,
`IS NULL` for global scope, and unrestricted admin filtering. A small private
`scoped_list` macro owns that choice while preserving SQLx's literal-SQL checks
and keeping projections/filters at their existing call sites. It is restricted
to these list reads; point lookups already narrow by unique identity.
The first bind position stays reserved for the organization in all variants.

Fowler move: Remove Flag Argument and Extract Function, expressed as a private
macro because SQLx needs literal SQL. Impact medium; confidence high;
scope internal/cross-module; risk medium (plan and visibility behavior).
The DTOs, public wrappers, and handler traits are not refactoring targets.

PostgreSQL documents B-tree support for equality and `IS NULL`:
<https://www.postgresql.org/docs/18/indexes-types.html>.

### Progress: invariant ownership and discarded domain errors

The core execution validator checked only values present in a request.
The database correctly merged omissions with durable values, but its CHECK
failure was reduced to a generic retryable persistence error. A validated
request alone cannot solve this: another progress writer can change the row.

The shared `validate_job_progress` function now owns the numerical rule.
Completion construction, execution prevalidation, completion persistence, and
ordinary progress use it. The existing live-lease lock helper was renamed for
reuse, then ordinary progress gained locked-state validation before mutation.
The SQL write still rechecks lease expiry. Original partial values remain in
audit events, and checkpoint writes remain atomic with progress.

The private query-error classification retains `JobProgressValidationError`;
an additive accessor lets the runtime return terminal `InvalidProgress` without
parsing strings. Existing public exhaustive error enums were not extended.
Actual connection/commit failures remain retryable. Rejected validation awaits
rollback so the lease row is released before returning. Existing completion
error codes and diagnostic wording are retained.

Fowler moves: Extract Function, Move Function, and preserve a typed recoverable
error. Impact medium; confidence high; scope cross-crate; risk medium
(transaction ordering and error policy). This is a bug behavior change after
the behavior-preserving validator extraction and lock-helper rename.

## Verification sequence

1. Baseline formatting, core tests/doctests, and existing scope tests passed.
   The heuristic scan covered 35 changed Rust files; its 143 candidates were
   treated as hints, not additional findings or cleanup work.
2. Added a worker regression before changing behavior. It failed with `Pending`
   instead of `DeadLettered`; typed continuation and malformed-payload tests
   already passed. Validator extraction passed core tests; lock-helper rename
   passed the PostgreSQL compile check.
3. Added an actual-prepared-query plan test against production indexes and
   20,000 interleaved rows. Before the fix, the job query rejected 19,980 rows
   in a sequential scan (1.833 ms, 385 shared buffers). The test covers tenant
   and global scopes under custom and generic plans, including legitimate
   global-only partial indexes. Timing is diagnostic, not a flaky threshold.
4. Added competing partial-update validation, rejected-write atomicity/audit
   assertions, terminal worker classification, typed continuation resume, raw
   malformed-payload cleanup, legacy metrics aggregation, tenant-local payload
   lookup, and packaged prelude usage.
5. All four downstream patches passed `git apply --cached --check` against their
   documented base commits using temporary indexes. This checks applicability;
   downstream application suites are not rerun by this check.

Database diagnostics use PostgreSQL `18.6 (Debian 18.6-1.pgdg13+2)`
(`server_version_num=180006`).

Final checks:

- `cargo test --workspace`: 879 passed, three explicitly ignored entries,
  using `RUNLEDGER_TEST_ADMIN_DATABASE_URL` for an isolated PostgreSQL 18.6
  container. Container-lifecycle parent tests return early under that setting.
  The ignored entries are a manual claim throughput benchmark, a slow promoter
  transaction-timeout test, and the lifecycle child-process entrypoint.
- `scripts/lint.sh`: passed formatting, workspace/all-target/all-feature Clippy,
  packaged-consumer Clippy, migration-info checks, and warning-free rustdoc.
- `scripts/refresh-sqlx-cache.sh`: passed on PostgreSQL 18.6 with all migrations;
  all three cache directories contain the same 148 query records. Offline
  workspace compilation and packaged metadata checks passed.
- `scripts/check-package-licenses.sh` and `cargo deny check`: passed.
- `scripts/run-external-consumer-smoke.sh`: passed from packaged crates against
  PostgreSQL 18.6, including the prelude outcome helper.
- Standalone container-lifecycle tests, rerun without the external database
  override: two passed and two failed before PostgreSQL readiness (40 exhausted
  connection attempts). The failed cases cover normal exit and forced
  termination; missing/stalled reaper CLI cases pass. The test-support source,
  migrations, lockfile, and toolchain are unchanged by this patch. This remains
  an infrastructure-test limitation; no cause or fix is claimed here.
- The four downstream patch applicability checks passed. Downstream application
  suites and the CI semver action were not rerun (the local semver CLI is absent).

The tenant job-list plan in the fixed 20,000-row fixture used 22 shared buffers
and 0.073 ms versus the baseline's 385 buffers and 1.833 ms. Both list APIs
passed tenant/global custom/generic plan checks. These are fixture diagnostics,
not production latency guarantees.

## Remaining risks and deliberately separate work

Progress now performs an additional locked read in its existing transaction.
It reuses existing timeout bounds and holds the lock through validation/write,
which prevents concurrent partial updates from validating against stale state.
Ordinary-progress throughput was not benchmarked; no throughput claim is made.
There are no migration, serialization, authorization, unsafe, dependency, or
runtime-dispatch changes. Exact-global metrics and payload APIs are tracked
separately rather than redefining legacy optional arguments.
