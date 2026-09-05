# Review follow-up: scoped reads, progress, and deadline policy

This follow-up addresses the comprehensive review of `3ed1404..c30fffe`.
Database diagnostics use PostgreSQL **18.6 (Debian 18.6-1.pgdg13+2)**,
`server_version_num=180006`, with the current migrations. Public signatures,
persisted formats, error categories, and timeout precedence are preserved.

## Research before implementation

The handler cutoff is intentional. `JobExecution::deadline` exposes the
worker's authoritative monotonic deadline, and the worker already explicitly
checks it after polling the handler. Tokio 1.53.1, the locked dependency,
[randomizes `select!` branch polling](https://docs.rs/tokio/1.53.1/tokio/macro.select.html#fairness).
Removing the result-side check would let polling order decide whether an
overdue result wins. Moreover, Tokio
[`timeout_at`](https://docs.rs/tokio/1.53.1/tokio/time/fn.timeout_at.html)
allows an immediately ready future to succeed irrespective of the deadline;
it is not by itself a strict result-acceptance policy.

Keep the existing rule: the worker must observe a result strictly before the
deadline. An equal or later result is a timeout, including success and
continuation. Database completion persistence occurs afterward. Committed
checkpoints and external effects cannot be undone by discarding a late result.
The public rustdoc and README now state this rule.

The migration question also has a concrete answer:
`ensure_schema_compatible_after_idempotency_cutover` checks every required
bundled up migration against `_sqlx_migrations`. The custom
`runledger_migration_history` fence is a separate compatibility mechanism.
The pagination indexes are required by current startup even though they do not
advance that fence. A regression now proves rejection before application and
acceptance afterward, without a custom fence entry.

## Diagnosis and design

### Preserve scope variants when constructing SQL

The scope enum is sound. The problem was collapsing it to a nullable UUID and
an `OR` expression before PostgreSQL planned the query. A
[generic prepared plan](https://www.postgresql.org/docs/18/sql-prepare.html)
cannot simplify it using one tenant's bound value, and
[partial-index applicability](https://www.postgresql.org/docs/18/indexes-partial.html)
depends on what the planner can prove about the predicate.

The new exact-scope lookup macro selects equality or `IS NULL` statements.
It lives beside the existing list-query mechanism in `jobs/scoped_read.rs`,
retains SQLx literal-SQL checking, and accepts only `JobScope`. An admin
wildcard cannot enter a payload lookup whose key is only scope-local.

Fowler move: Remove Flag Argument / preserve the closed enum at the SQL
boundary. Impact medium; confidence high; scope internal; risk medium because
visibility and plans must both remain correct. No new indexes are needed for
the reproduced generic-plan regression.

### Separate lock acquisition from completion timeout policy

The shared live-lease helper bundled two responsibilities: acquiring the row
and temporarily applying a completion transaction's lock-timeout policy.
Progress had already installed its whole-transaction caps, so reusing that
wrapper added redundant cap/restore statements. Its following update also
contained another locking CTE.

Extracting the row acquisition lets completion retain its existing wrapper
while progress uses the caps it already owns. Progress locks once, validates
the current row through the shared Rust validator, then updates and audits in
the same transaction. The update still rechecks wall-clock lease expiry.

The locked read remains deliberate. Replacing it with optimistic validation
would reintroduce a race between partial updates. Duplicating the numerical
validation in SQL or parsing PostgreSQL CHECK-error text would add another
source of truth or a fragile error protocol. The existing typed validation
errors and awaited rejection rollback remain intact.

Fowler move: Extract Function / separate acquisition from policy. Impact
medium; confidence high; scope internal; risk medium because cancellation,
lease fencing, and transaction ordering matter. The initial extraction passed
the existing progress race and rollback tests before progress adopted it.

### Make the rollout requirement visible

The ordinary index builds are an existing, documented deployment choice.
Changing an already checked-in migration or introducing a separate concurrent
DDL protocol would create additional compatibility work. The concrete omission
was in the Unreleased upgrade notes: they now identify the migration, its
write-lock window, and the difference between SQLx history and the custom fence.

## Measurements and regression coverage

The payload fixture uses 20,000 rows with repeated keys/run IDs across scopes,
twenty busy tenants, and old global rows. It explains the actual SQLx-prepared
public queries under forced custom and generic plans on one backend.

| Measurement | Before | After |
| --- | ---: | ---: |
| Generic global run lookup: unrelated rows rejected | 19,980 | 0 |
| Same lookup: shared buffers | 868 | 4 |
| Same lookup: diagnostic execution time | 3.437 ms | 0.043 ms |
| Ordinary progress: statements per write, including BEGIN/COMMIT | 8 | 6 |
| Progress: local median over 64 writes | 7.286 ms | 6.701 ms |
| Progress: local p95 | 10.515 ms | 8.230 ms |

The review's “4× statement count” description was incorrect for the complete
progress operation. The measured reduction is 25%. Timing is diagnostic;
the manual progress test asserts the six-statement ceiling and verifies durable
progress, checkpoint, and audit events. It does not assert a latency threshold.
The idempotency-key lookup remained efficient in the fixture; the severe
reproduced regression was the run lookup. A final repeat measured 6.868 ms
median and 10.594 ms p95 for progress while retaining exactly six statements
per write, illustrating why the timing samples are not a throughput guarantee.

The scope change does not establish a universal performance bound for JSON
searches within a large scope. A separate exploratory fixture with 10,000 old
global rows also induced a broad ordering scan under a custom plan, where the
scope predicate was already simplified. Choosing payload-expression indexes
requires workload evidence; this follow-up does not claim to resolve that
different access-pattern limitation.
It is tracked as `runledger-runledger-simplification-audit-0ju`.

Additional tests cover:

- Exact deadline equality and one nanosecond after it with a paused clock;
  success and continuation before the cutoff remain accepted.
- A real worker handler that returns success or continuation after a
  non-yielding poll crosses the cutoff, proving timer-branch selection alone
  cannot enforce the rule.
- Cancellation of blocked progress with one-, two-, and four-connection worker
  pools; cancelled writes cannot commit after the holder releases the row.
- A live lease expiring while progress waits for its row lock, with no progress
  or checkpoint/audit write surviving.
- Sparse status and job-type filters in summary pagination under custom and
  generic plans, with independent fixture-derived expected rows. Selective
  custom plans may use the existing type/status/time index and apply the UUID
  tie-break as a residual filter; requiring one particular index would reject
  valid plans.
- Current-startup enforcement of the additive index migration independently of
  the custom compatibility fence.

## Verification

Focused query-plan, progress, deadline, small-pool cancellation, and migration
tests passed. SQLx metadata was refreshed against PostgreSQL 18.6 with current
migrations and synchronized across all three cache directories.

The first full workspace build exhausted local disk space. Cargo's supported
profile cleanup removed regenerable development artifacts before retrying with
incremental compilation disabled.

- `cargo test --workspace`: 915 passed, zero failed, five explicitly ignored.
  The ignored entries are two manual cost diagnostics, a claim benchmark, a
  slow promoter transaction-timeout test, and a lifecycle child entrypoint.
  External PostgreSQL mode does not independently exercise owned-container
  lifecycle teardown; its parent tests return early in that mode.
- `scripts/lint.sh`: passed workspace/all-target/all-feature Clippy, standalone
  consumer Clippy, formatting, README checks, migration-info checks, and
  warning-free rustdoc.
- `scripts/run-external-consumer-smoke.sh`: four tests passed from packaged
  crates on PostgreSQL 18.6. Downstream application suites were not rerun.
- `scripts/refresh-sqlx-cache.sh`: passed PostgreSQL 18/current-migration checks,
  offline compilation, and packaged-cache checks; all three directories contain
  the same 159 query records.
- The ignored progress diagnostic was also run explicitly: its six-statement
  ceiling and durable-state/audit assertions passed. The strengthened deadline
  result-preservation test and final runtime Clippy check passed separately.

The focused diagnostics can be rerun with an isolated PostgreSQL 18 server
configured with `shared_preload_libraries=pg_stat_statements`, exporting its
administrative URL as `RUNLEDGER_TEST_ADMIN_DATABASE_URL`:

```bash
cargo test -p runledger-postgres --test job_read_plans -- --nocapture
cargo test -p runledger-postgres --test job_summary_plans -- --nocapture
cargo test -p runledger-postgres --test operational_costs measure_progress_costs -- --ignored --nocapture
cargo test -p runledger-postgres --test progress_validation -- --nocapture
cargo test -p runledger-runtime --lib execution_services -- --nocapture
cargo test -p runledger-runtime --lib success_and_continuation_must_be_observed_strictly_before_the_deadline
cargo test -p runledger-postgres --test migrations summary_indexes -- --nocapture
```
