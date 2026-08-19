# Add durable job enqueue intents

This ExecPlan is a living document. The sections `Progress`, `Surprises & Discoveries`, `Decision Log`, and `Outcomes & Retrospective` must be kept up to date as work proceeds.

No `.agent/PLANS.md` or repository-root `PLANS.md` is checked into this repository as of 2026-08-18. Maintain this document according to the repository guidance in `AGENTS.md`, `runledger-postgres/AGENTS.md`, and `runledger-runtime/AGENTS.md`. The repository may contain unrelated working-tree edits; preserve them and do not rewrite or revert them while executing this plan.

## Purpose / Big Picture

An application sometimes needs to commit business state and a request for background work in the same PostgreSQL transaction before that job's Runledger definition has been synchronized. The existing `enqueue_job_tx` deliberately requires an enabled definition and locks that definition while resolving queue defaults. That is correct for immediate enqueue, but it is the wrong contract for a durable handoff after an external side effect such as a payment: a temporarily absent or contended catalog row must not invalidate the application transaction.

After this change, an application can call `record_job_enqueue_intent_tx` in its existing transaction. The function records a durable, strictly idempotent job request without reading or locking `job_definitions`. A normal Runledger worker later promotes eligible intents for its registered handler types after an enabled definition exists. Promotion uses the ordinary enqueue machinery, so the resulting `job_queue` row receives the current definition defaults, execution-resource behavior, and one normal `ENQUEUED` audit event. Missing or disabled definitions leave intents pending and consume no job attempts.

The behavior is visible in three ways. A PostgreSQL integration test records and commits an intent before any definition exists, then proves no job exists until promotion. A runtime integration test synchronizes the definition, starts a standard worker, and observes the handler execute exactly once. Public backlog metrics report pending and retrying counts, maximum promotion attempts, conflicted count, promotions in the last 24 hours, and oldest pending age so applications can alert on a stuck handoff instead of treating durability as invisible.

## Progress

- [x] (2026-08-18) Read the original queue, worker, migration, test, and SQLx-cache contracts and drafted the initial ownership design.
- [x] (2026-08-18) Re-validated the plan after the repository advanced to `53d0172` / version `0.9.1` and updated all paths, gates, semver constraints, lifecycle coverage, and runtime integration points.
- [x] (2026-08-18) Confirmed that unrelated working-tree edits are limited to the shared PostgreSQL test-container lifecycle and its README explanation; implementation must preserve those edits.
- [x] (2026-08-18) Added the additive, forward-only intent migration and its migration compatibility tests.
- [x] (2026-08-18) Added shared canonical enqueue preparation plus public intent record, read, metrics, promotion, and bounded cleanup APIs in `runledger-postgres`.
- [x] (2026-08-18) Integrated bounded promotion at the current `WorkerLoop::iteration` boundary without changing existing public runtime error enums.
- [x] (2026-08-18) Added PostgreSQL, runtime, prelude, migration, concurrency, audit rollback, and packaged-consumer regression coverage.
- [x] (2026-08-18) Updated public documentation, the unreleased changelog, schema inventory, migration inventory, and downstream rollout guidance while preserving the user's existing README edits.
- [x] (2026-08-18) Refreshed SQLx metadata against PostgreSQL 18.4 and passed the focused intent, migration, worker, and test-container regression targets.
- [x] (2026-08-18) Ran the requested Claude Code Opus plus native Codex review, then fixed its actionable isolation, retry, batch-bound, read-path, and observability findings.
- [x] (2026-08-18) Repeated the full workspace lint, test, documentation, package, and external-consumer smoke gates after the review fixes.
- [x] (2026-08-18) Repeated the final comprehensive Opus/native review, fixed its production retention-index finding and operational documentation issues, and completed the final workspace gates.

## Surprises & Discoveries

- Observation: The refreshed repository split the old `runledger-postgres/src/jobs/queue/dispatch.rs` into `queue/enqueue.rs` and `queue/claim.rs`, and split public enqueue DTOs into `jobs/types/enqueue.rs`.
  Evidence: `runledger-postgres/AGENTS.md` now routes enqueue and claim work to those files, and both modules exist at `53d0172`.

- Observation: The ordinary enqueue path still owns the exact behavior an intent must reuse: canonical request serialization, execution-resource validation, strict keyed retry comparison, enabled-definition defaults, queue insertion, and `ENQUEUED` event creation.
  Evidence: `enqueue_job_with_existing_lock_tx` and `canonical_job_enqueue_request` in `runledger-postgres/src/jobs/queue/enqueue.rs` perform those operations in one transaction.

- Observation: Caller-owned keyed operations now encode their isolation precondition with the private `ReadCommittedTx` capability instead of relying only on convention.
  Evidence: `runledger-postgres/src/jobs/transaction_isolation.rs` defines `ReadCommittedTx`, `OwnedReadCommittedTx`, `ensure_read_committed_tx`, and owned-transaction finish/rollback helpers. Intent record and promotion paths should use these existing capabilities.

- Observation: The refreshed worker loop has a narrow iteration boundary and a stable, sorted list of registered static job types.
  Evidence: `runledger-runtime/src/worker.rs` stores `claimable_job_types` on `WorkerLoop` and performs one bounded claim in `WorkerLoop::iteration`.

- Observation: The workspace now denies all Rust warnings and broad Clippy categories, runs packaged-consumer lint and smoke tests, and checks patch-level API compatibility against 0.9.0.
  Evidence: root `Cargo.toml`, `scripts/lint.sh`, and `.github/workflows/ci.yml`. In particular, adding a variant to the public exhaustive `WorkerError` enum risks failing the semver gate, so promotion failures must be logged without extending that enum in this patch.

- Observation: An intent that becomes terminal needs an explicit retention story, and a queue-key conflict needs an explicit non-automatic recovery story.
  Evidence: The initial plan preserved promoted and conflicted rows but did not define cleanup or safe remediation. This revision adds bounded cleanup for promoted rows and treats conflicted rows as immutable evidence that requires a deliberately new application idempotency key if replacement work is safe.

- Observation: The current uncommitted README edit documents the user's shared-container lifecycle work in the testing section.
  Evidence: `git diff -- README.md` changes only the DB-backed test-harness explanation. Feature documentation must be inserted elsewhere or merged without removing that text.

- Observation: The ordinary enqueue transaction already rolls back queue and
  `ENQUEUED` event state together, so intent promotion can extend the same atomic
  boundary without a second event-writing path.
  Evidence: The forced event-write failure regression leaves both the queue row
  and intent transition absent, while the intent remains pending for retry.

- Observation: An embedder-owned trigger or later queue constraint can reject a
  valid persisted intent even when the intent table itself is internally
  consistent.
  Evidence: A forced `job_events` trigger failure reproduces a PostgreSQL error
  after the queue insert. Per-row savepoint rollback plus durable retry metadata
  is required to prevent the oldest failing intent from starving the batch.

- Observation: PostgreSQL's cached-subtransaction boundary is lower than the
  generic 1,000-row administrative page limit.
  Evidence: Promotion now caps a pass at 24 and an integration test requests
  1,000 across 25 terminally failing intents, observing 24 then one. Even the
  worst case assigns at most 48 subtransaction IDs, leaving 16 IDs of headroom
  below PostgreSQL's 64 cached-subtransaction threshold.

- Observation: A type-leading pending index forces PostgreSQL to sort the full
  eligible backlog before applying the promotion limit.
  Evidence: `EXPLAIN (ANALYZE, BUFFERS)` on PostgreSQL 18.4 with 100,000 pending
  rows took about 29 ms and spilled a 5.2 MB external merge sort. Ordering the
  partial index by `next_promotion_at, created_at, id` with `job_type` included
  removed the sort and returned 32 rows in about 0.08 ms on the same dataset.

- Observation: Neither one pending-index order nor one retention cutoff serves
  every operational distribution.
  Evidence: The global-order pending index avoids sorting a healthy 100,000-row
  eligible backlog, while a complementary type-leading partial index lets the
  planner constrain registered types when stale unregistered rows dominate.
  Exact-job retention and the referencing-side foreign-key check use a separate
  `promoted_job_id` partial index rather than scanning retained intent history.

- Observation: PostgreSQL row-lock compatibility lets duplicate intent retries
  stay concurrent with promotion without weakening promoter exclusion.
  Evidence: Promoters claim with `FOR NO KEY UPDATE`, exact retry lookups use
  `FOR KEY SHARE`, and an integration test completes the retry while the
  promoter-compatible lock is held.

## Decision Log

- Decision: Put durable intent storage, state transitions, metrics, and promotion in `runledger-postgres`; have `runledger-runtime` invoke promotion from the generic worker loop.
  Rationale: Queue persistence and migrations belong to `runledger-postgres`. The runtime already owns generic polling and has the exact registered-type allowlist required to promote only work the process can execute. CreditKit remains responsible only for when to record the handoff and what payload/idempotency key it carries.
  Date/Author: 2026-08-18 / Codex

- Decision: Keep `enqueue_job_tx` strict and unchanged; add an explicit `record_job_enqueue_intent_tx` API.
  Rationale: Immediate enqueue and durable deferred handoff have different readiness guarantees. One API must not silently acquire two meanings based on catalog state.
  Date/Author: 2026-08-18 / Codex

- Decision: Require every intent to have a nonblank idempotency key and persist the same canonical request snapshot used by ordinary keyed enqueue.
  Rationale: Application transactions and worker promotion are both retry boundaries. Mandatory strict idempotency makes a replay return the original intent and makes changed payload, scheduling, retry, stage, or execution-resource fields a visible conflict.
  Date/Author: 2026-08-18 / Codex

- Decision: Give the intent its own UUID and store `promoted_job_id` separately.
  Rationale: A direct keyed enqueue may race with or precede promotion. If its canonical request is identical, the intent should converge on that existing job rather than create a duplicate. Requiring the intent UUID to become the job UUID would make that convergence impossible.
  Date/Author: 2026-08-18 / Codex

- Decision: Model intent state as checked text values `PENDING`, `PROMOTED`, and `CONFLICTED`, not as a PostgreSQL enum.
  Rationale: A checked text column keeps the additive migration and any future state expansion simpler while Rust still exposes a typed, non-exhaustive status enum. Database constraints will enforce the required companion fields for each state.
  Date/Author: 2026-08-18 / Codex

- Decision: Missing or disabled definitions remain `PENDING`; an equivalent existing queue job transitions to `PROMOTED`; a mismatched or legacy-snapshot queue-key collision transitions to `CONFLICTED` with a stable diagnostic code.
  Rationale: Catalog unavailability is expected rollout state, not a failed attempt. A mismatched key is deterministic and must not hot-loop. Conflicted rows are immutable evidence; Runledger must not guess whether changing a key and executing replacement work is safe after an external side effect.
  Date/Author: 2026-08-18 / Codex

- Decision: Provide backlog metrics, bounded cutoff deletion, and exact-job transactional deletion of promoted intents, but do not automatically delete or retry conflicted intents.
  Rationale: Pending age and conflict count are required reliability signals. Promoted rows can be retained according to an application policy and removed independently in bounded batches. Queue retention must instead remove links for its exact selected job IDs in the same transaction, because a fresh intent can converge on an older existing job. Conflict remediation requires domain knowledge and therefore remains an explicit application/operator decision.
  Date/Author: 2026-08-18 / Codex

- Decision: Promote on the standard worker poll cadence before the ordinary claim, independently of execution permits, using `claim_batch_size` as the requested bound and the persistence layer's 24-row safety cap.
  Rationale: Promotion is a database-only handoff into `job_queue`; the ordinary queue's priority, scheduling, metrics, and worker concurrency are the correct backpressure boundary. Coupling promotion to free permits could leave unrelated intents outside the observable queue for the full duration of saturated handlers. This still adds no second scheduler, configuration channel, or shutdown path and still limits promotion to registered handler types.
  Date/Author: 2026-08-18 / Codex

- Decision: Compare canonical enqueue snapshots with PostgreSQL JSONB equality at both retry and promotion boundaries, and have v1 promoters claim only snapshot version 1.
  Rationale: PostgreSQL normalizes JSONB numeric representations, so decoded Rust `Value` equality can reject semantically equal exponent/integer forms. Version filtering also prevents an older mixed-fleet worker from terminalizing a row written under a future canonicalization version.
  Date/Author: 2026-08-18 / Codex

- Decision: Retain both global-order and type-leading pending partial indexes,
  plus a referencing-side `promoted_job_id` partial index.
  Rationale: The two pending indexes let PostgreSQL choose between bounded
  global ordering for healthy eligible backlogs and early type filtering when
  unregistered rows dominate. The promoted-job index prevents exact retention
  cleanup and `ON DELETE RESTRICT` checks from scanning the child table.
  Date/Author: 2026-08-18 / Codex

- Decision: Keep deterministic malformed snapshot/stage/resource rows terminal,
  but defer PostgreSQL database errors on the individual pending intent with
  exponential backoff capped at five minutes.
  Rationale: A malformed durable request cannot become valid by retrying, while
  triggers, constraints, and other database policy can be repaired. Rolling
  back to a per-row savepoint preserves queue/event atomicity; advancing
  `next_promotion_at` lets later intents continue without hiding sanitized
  failure metadata.
  Date/Author: 2026-08-18 / Codex

- Decision: Retry database-level promotion failures indefinitely rather than
  auto-transitioning them to `CONFLICTED` after an arbitrary attempt count.
  Rationale: An outage can outlast any fixed retry budget; terminalizing the
  intent would convert recoverable infrastructure failure into silently lost
  work. Backoff prevents starvation, while pending age, attempt count, and
  sanitized diagnostics provide the operator signal for repair or an explicit
  domain decision.
  Date/Author: 2026-08-18 / Codex

- Decision: After a row savepoint rollback succeeds, defer every non-terminal
  `QueryError`; keep non-query connection, configuration, and migration errors
  batch-fatal.
  Rationale: Successful rollback proves the transaction can persist bounded
  backoff metadata, so a newly introduced row-level query error must not roll
  back earlier healthy promotions or starve later rows. If the connection or
  transaction is unusable, rollback itself fails before classification.
  Date/Author: 2026-08-18 / Codex

- Decision: Cap every public promotion transaction at 24 intents even when a
  caller requests a larger valid administrative page limit.
  Rationale: `limit` remains an upper bound. A failed row assigns two
  subtransaction IDs, so 24 keeps the worst case below PostgreSQL's
  cached-subtransaction boundary with 16 IDs of explicit headroom.
  Date/Author: 2026-08-18 / Codex

- Decision: Do not add a `WorkerError` variant for promotion failures in this release.
  Rationale: `WorkerError` is currently a public exhaustive enum and CI checks patch compatibility against 0.9.0. The worker will log the sanitized Runledger error and continue to its normal poll wait/claim behavior, while the durable pending row remains the retry source.
  Date/Author: 2026-08-18 / Codex

- Decision: Treat the new migration as additive during rolling deployment and omit it from `runledger_migration_history` while retaining normal SQLx history/checksum tracking.
  Rationale: Older 0.9 workers and non-retention writers ignore the new table and remain compatible. A new worker can be rolled back without corrupting existing queue state; pending intents wait until a compatible worker returns. Once promotion begins, the intentional `ON DELETE RESTRICT` link means queue retention must delete promoted intents before linked jobs. The migration test exemption list and migration comment name that boundary explicitly.
  Date/Author: 2026-08-18 / Codex

## Outcomes & Retrospective

The persistence, promotion, runtime integration, packaged consumer story, and
public documentation are implemented. Repeated Opus and native reviews found no
remaining correctness bug; follow-up fixes added exact-job retention cleanup,
referencing-side and complementary pending indexes, worst-case savepoint
headroom, consistent safe diagnostics, and explicit rollout/version/throughput
contracts. The final shape passes 14 focused intent scenarios, three
intent-classification unit scenarios, 19 migration scenarios, four lifecycle
parent scenarios, the full workspace suite, lint/docs, packaged-license checks,
and the external consumer smoke test against PostgreSQL 18.4. A generic
pending/conflicted deletion API remains deliberately out of scope because
abandoning unexecuted durable work requires an application-owned authorization,
audit, and replacement policy.

## Context and Orientation

Runledger is a Rust workspace. `runledger-core` contains storage-independent job contracts. `runledger-postgres` owns PostgreSQL tables and queue state transitions. `runledger-runtime` owns the long-running worker, scheduler, and lease-reaper loops. `runledger-test-support` creates PostgreSQL 18 test databases. The root `migrations/` directory is canonical; the same files are vendored into `runledger-postgres/migrations/` and `runledger-test-support/migrations/` for published crates and tests.

An immediate enqueue is a `JobEnqueue` passed to `enqueue_job` or `enqueue_job_tx`. In `runledger-postgres/src/jobs/queue/enqueue.rs`, the insert first reads an enabled `job_definitions` row to resolve priority, attempt, and timeout defaults. A keyed enqueue stores `enqueue_request`, a canonical JSON snapshot of the caller-requested fields, and only returns an existing queue row when that snapshot matches exactly.

An enqueue intent is a new durable outbox row. “Outbox” means a record committed in the same database transaction as application state and processed asynchronously afterward. It is not a second job queue: no handler may claim it, it has no attempts or lease, and it emits no job lifecycle event. Promotion is the one-way transition that resolves an intent through the existing enqueue implementation and atomically stores the resulting job ID.

The current relevant files are `runledger-postgres/src/jobs/queue/enqueue.rs`, `runledger-postgres/src/jobs/queue/claim.rs`, `runledger-postgres/src/jobs/queue.rs`, `runledger-postgres/src/jobs/types/enqueue.rs`, `runledger-postgres/src/jobs/types.rs`, `runledger-postgres/src/jobs.rs`, `runledger-postgres/src/lib.rs`, `runledger-runtime/src/worker.rs`, `runledger-runtime/src/worker/tests/mod.rs`, `runledger-runtime/tests/worker_loop.rs`, `runledger-runtime/tests/prelude_smoke.rs`, `runledger-postgres/tests/migrations.rs`, and `smoke/external-consumer/tests/smoke.rs`.

## Milestone 1: Persist and inspect durable intents

At the end of this milestone an application transaction can record an intent before a job definition exists, retry the exact same request, inspect the durable row, query backlog metrics, and clean up old promoted rows. No runtime behavior changes yet.

Create `migrations/202608180001_job_enqueue_intents.up.sql` and its matching down migration. The up migration creates `job_enqueue_intents` with these columns: UUID `id` using `uuidv7()`, text `job_type` with deliberately no definition foreign key, nullable UUID `organization_id`, JSONB `payload`, nullable requested `priority`, `max_attempts`, `timeout_seconds`, and `next_run_at`, required text `idempotency_key`, required text `stage`, required JSONB `enqueue_request`, nullable text `execution_resource_key`, required text `status` defaulting to `PENDING`, nullable UUID `promoted_job_id` referencing `job_queue(id)` with delete restriction, nullable promotion/conflict timestamps and diagnostics, and created/updated timestamps. Reuse `set_updated_at_timestamp()` for the update trigger.

Database checks must mirror queue input validity: job type, idempotency key, and stage are nonblank; optional max attempts and timeout are positive; an execution-resource key is either null or nonblank and at most 512 bytes; status is one of the three values; and state companion fields are coherent. `PENDING` has no job ID or terminal diagnostics. `PROMOTED` has a job ID and promotion timestamp but no conflict fields. `CONFLICTED` has a conflict timestamp and error code but no job ID. Add separate global and organization-scoped unique indexes on `(job_type, idempotency_key)` and `(job_type, organization_id, idempotency_key)`, matching `job_queue` scope. Add a pending promotion index ordered by `created_at, id`, including `job_type`, and an index suitable for terminal retention cleanup.

Do not insert version `202608180001` into `runledger_migration_history`; add it to `COMPATIBILITY_FENCE_EXEMPT_MIGRATION_VERSIONS` in `runledger-postgres/tests/migrations.rs` with a comment explaining the additive rollout. Extend migration tests to assert the new table, state check, uniqueness, promotion index shape, foreign key, and canonical/vendored migration parity. The down migration drops the intent trigger and table only.

Add intent DTOs to `runledger-postgres/src/jobs/types/enqueue.rs`, re-export them through `jobs/types.rs`, `jobs.rs`, and both PostgreSQL public/prelude surfaces. Use new non-exhaustive enums and constructor/builder methods rather than requiring downstream struct literals for a newly evolving API. The input starts from `JobEnqueueIntent::new(job_type, payload, idempotency_key)` and offers scoped setters for organization, priority, max attempts, timeout, absolute next run, stage, and execution resource.

Create `runledger-postgres/src/jobs/queue/intents.rs`. Implement `record_job_enqueue_intent_tx` for a caller transaction and `record_job_enqueue_intent` for an owned transaction. Both require `READ COMMITTED` using the existing transaction capability helpers. Validate all values before insertion, create the canonical enqueue snapshot through shared code in `queue/enqueue.rs`, insert without touching `job_definitions`, and resolve uniqueness conflicts with a `KEY SHARE` lock compatible with the promoter's `NO KEY UPDATE` claim. Return it only when PostgreSQL JSONB equality says the snapshot matches; otherwise return stable code `job.intent_idempotency_conflict`. An identical replay returns current intent status and `promoted_job_id`, even after promotion.

Implement `get_job_enqueue_intent_by_id`, `list_job_enqueue_intents`, and `get_job_enqueue_intent_metrics`. Follow existing admin API scope semantics: an organization ID filters to that tenant and `None` is an administrator-wide query, not proof of authentication. Metrics group by intent job type rather than joining definitions, because absent definitions are precisely what must remain visible. Report pending and retrying counts, maximum promotion attempts, conflicted count, promoted count in the last 24 hours, and oldest pending timestamp. Validate list pagination with the existing bounded helpers.

Implement `delete_promoted_job_enqueue_intents_before(pool, cutoff, limit)` as a bounded `FOR UPDATE SKIP LOCKED` delete that can be called repeatedly. Also implement `delete_promoted_job_enqueue_intents_for_jobs_tx(tx, job_ids)` so an application retention transaction can remove links for its exact selected jobs before deleting those queue rows. Both APIs must leave pending and conflicted rows untouched. Document that Runledger does not automatically schedule retention; cutoff cleanup is independent policy, while queue deletion must use exact IDs because an intent can converge on an older existing job.

Add `runledger-postgres/tests/job_enqueue_intents.rs`. Prove commit and rollback with no definition, strict same-request replay, changed-request rejection, organization/global uniqueness, input validation, `READ COMMITTED` enforcement, list/lookup scope, metrics including missing definitions, and promoted-only cleanup. Also prove that holding an incompatible lock on `job_definitions` does not block intent recording; this is the key behavioral difference from immediate enqueue.

Run from `/Users/aa/Documents/runledger`:

    cargo fmt --all
    cargo test -p runledger-postgres --test job_enqueue_intents
    cargo test -p runledger-postgres --test migrations
    cargo check -p runledger-postgres --all-features

The new tests should pass on PostgreSQL 18. Before implementation, the intent test target does not exist; after this milestone it should show all tests passing and should leave no `job_queue` row for an unpromoted intent.

## Milestone 2: Promote through the existing queue contract

At the end of this milestone, a low-level caller can promote eligible intents concurrently and receive a typed report. Promotion creates ordinary queue/event state exactly once, leaves unavailable work pending, converges with identical direct enqueues, and turns deterministic queue-key mismatches into durable conflicts.

Refactor only the private internals of `runledger-postgres/src/jobs/queue/enqueue.rs`. Make canonical request construction and execution-resource validation reusable from the sibling intent module. Add one private intent-facing enqueue adapter that returns an internal typed resolution: ordinary `JobEnqueueOutcome`, definition unavailable, or deterministic idempotency conflict. Keep public direct enqueue functions and their errors unchanged. Classify `job.idempotency_conflict` and `job.legacy_idempotency_snapshot_missing` inside `queue/enqueue.rs`, where those errors originate; do not make `intents.rs` duplicate public error-code string matching. Unexpected SQL, connection, serialization, or impossible missing-existing errors remain batch errors.

Implement `promote_job_enqueue_intents_for_types(pool, allowed_job_types, limit)` in `queue/intents.rs`. Empty allowed types return an empty report. Validate a positive bounded limit. Open an explicitly `READ COMMITTED` owned transaction, select only `PENDING` intents whose type and snapshot version are supported and whose definition is currently enabled, and lock only intent rows with `FOR NO KEY UPDATE SKIP LOCKED` in retry/creation order. Reconstruct `JobEnqueue` from each row, compare its canonical snapshot with PostgreSQL JSONB equality, invoke the private intent-facing enqueue adapter with its optional execution resource, then update the intent in the same transaction.

An inserted or identical existing job sets `PROMOTED`, `promoted_job_id`, and `promoted_at`. A deterministic mismatch sets `CONFLICTED`, `conflicted_at`, `last_error_code`, and a sanitized diagnostic message. A definition disabled between eligibility selection and enqueue remains `PENDING`. No path increments a job attempt. If a database statement fails for one row, roll back to that row's savepoint, leave the intent pending with bounded exponential backoff and sanitized retry metadata, then continue the batch. Connection or transaction failures still roll back the batch. Deterministic malformed snapshots, stages, or resource keys become terminal conflicts so they cannot hot-loop.

Return a `JobEnqueueIntentPromotionReport` with inserted-job, existing-job, conflicted, and total-promoted counts. Add tracing at batch completion only when at least one row changes, with counts and no payload or idempotency key fields.

Extend `runledger-postgres/tests/job_enqueue_intents.rs` to prove missing and disabled definitions remain pending, current definition defaults are applied at promotion, requested overrides and execution resources survive, exactly one `ENQUEUED` event is created, identical direct enqueue converges on its job, changed direct enqueue marks the intent conflicted, and two concurrent promoters cannot duplicate work. Test the race where a definition becomes disabled after eligibility and confirm the row remains pending. Test a forced event-write failure and confirm both queue insertion and intent transition roll back.

Run:

    cargo fmt --all
    cargo test -p runledger-postgres --test job_enqueue_intents
    cargo test -p runledger-postgres --test enqueue_outcome
    cargo test -p runledger-postgres --test job_execution_resources
    cargo check -p runledger-postgres --all-features

Expect the new tests plus existing strict enqueue and execution-resource tests to pass unchanged.

## Milestone 3: Drive promotion from standard workers

At the end of this milestone, users of `run_worker_loop`, `run_worker_loop_with_observer`, or `Supervisor` need no separate dispatcher. A compatible standard worker promotes and then claims intents for its registered handler types.

In `runledger-runtime/src/worker.rs`, add a private `WorkerLoop::promote_intents(limit)` method and call it in `WorkerLoop::iteration` after confirming there is at least one registered type, immediately before the permit check and ordinary `claim`. Use `claim_batch_size`; the persistence API applies its 24-row safety cap. A promotion error is logged with the worker ID and sanitized source, then the iteration proceeds to claim existing queue work. Do not add a public `WorkerError` variant. Existing shutdown checks, saturated-worker wait behavior, task draining, and claim behavior must remain unchanged; a saturated worker must still promote the handoff into the ordinary queue.

Add focused unit coverage under `runledger-runtime/src/worker/tests/` for empty registries, saturation, promotion errors, and shutdown. Add or extend `runledger-runtime/tests/worker_loop.rs` to record an intent before its definition exists, synchronize/register the handler, start the worker, and observe exactly one successful execution and a `PROMOTED` intent linked to the succeeded job. Also prove that an intent for an unregistered type is not promoted by that worker.

Update `runledger-runtime/tests/prelude_smoke.rs` so the new PostgreSQL types/functions remain compatible with simultaneous glob imports. Extend `smoke/external-consumer/tests/smoke.rs` with the actual embedding story: in one consumer-owned transaction insert a consumer audit/business row and call `record_job_enqueue_intent_tx`, commit, then let the packaged standard worker promote and execute it. This proves the capability is present in packaged crates rather than only workspace paths.

Run:

    cargo fmt --all
    cargo test -p runledger-runtime worker
    cargo test -p runledger-runtime --test worker_loop
    cargo test -p runledger-runtime --test prelude_smoke
    cargo check -p runledger-runtime --all-features
    ./scripts/run-external-consumer-smoke.sh

Expect the handler count to be one, the intent status to be `PROMOTED`, the linked job status to be `SUCCEEDED`, and the consumer-owned row and intent to have committed atomically.

## Milestone 4: Document, release-check, and review

At the end of this milestone, downstream teams can adopt the feature through a migration-first rollout, the checked-in SQLx metadata matches PostgreSQL 18, every repository gate passes, and the requested independent reviews have been reconciled.

Update `runledger-postgres/src/lib.rs` crate documentation with a short transactional intent example. Update `README.md` near direct enqueue and the database schema/migration inventory, preserving the existing shared-container text. Update `docs/downstream-agent-guide.md` with the ownership boundary and rollout sequence. Add an `Unreleased / Added` changelog entry. Explain that intent payloads and idempotency keys follow the same trust boundary as queue payloads, and that applications must not log them casually.

Document this deployment order: apply migration `202608180001`; deploy a compatible Runledger worker so promotion is active; then switch application writers to `record_job_enqueue_intent_tx`; alert on oldest pending age, retrying count, maximum attempts, and conflicts. During rollback, stop new intent writers first, allow a compatible worker to drain pending rows, then roll back worker code if necessary. An older worker safely ignores the additive table, but it cannot drain rows accumulated while the new worker is absent. Once promotion starts, queue retention must delete promoted intents before linked jobs. Do not drop the migration during an ordinary code rollback.

Refresh SQLx metadata only against PostgreSQL 18 with all migrations applied. Use a disposable PostgreSQL 18 database, record `SHOW server_version`, set `DATABASE_URL`, and run the repository script. The script intentionally synchronizes root migrations into both vendored directories and refreshes `.sqlx/`, `runledger-postgres/.sqlx/`, and `runledger-runtime/.sqlx/`.

Then run, from the repository root:

    cargo fmt --all -- --check
    cargo test -p runledger-postgres
    cargo test -p runledger-runtime
    cargo test --workspace
    ./scripts/lint.sh
    ./scripts/check-package-licenses.sh
    cargo check --workspace
    cargo test --workspace --no-run
    ./scripts/run-external-consumer-smoke.sh

Run the repository's patch-semver check against 0.9.0 using the same package set as CI, or capture the GitHub semver job if the local cargo-semver-checks tool is unavailable. Review packaged crate file lists to confirm the migration and SQLx metadata are included.

Finally run the requested `$comprehensive-review --model opus` over the exact working-tree implementation scope. The comprehensive review must run Claude Code in the foreground with Opus and an independent native Codex review, deduplicate their actionable findings, and report them by severity and source. The review skill is review-only; do not silently fix findings during that review pass. If the user asks for fixes afterward, update this ExecPlan and implement them as a distinct follow-up.

## Validation and Acceptance

The feature is accepted only when all of these observable behaviors hold.

An application transaction records an intent for a job type absent from `job_definitions` and commits. Repeating the same type, scope, key, and canonical request returns the same intent ID. Changing any canonical field returns code `job.intent_idempotency_conflict`. Rolling back the application transaction leaves no intent. Recording an intent does not wait on a contended `job_definitions` lock.

Before promotion there is no `job_queue` row, job attempt, or job event. Missing, disabled, and unregistered types stay pending. Once an enabled definition and a registered handler exist, a standard worker creates or resolves exactly one job, writes exactly one ordinary `ENQUEUED` event for a newly inserted job, marks the intent promoted, and runs the handler once. Two promoters and a concurrent direct enqueue do not duplicate the job.

A different direct request using the same queue idempotency key produces one conflicted intent with a stable diagnostic and no duplicate job. Pending age, retrying count, maximum attempts, and conflict count are queryable even when no definition exists. Bounded cleanup removes only promoted rows older than its cutoff and leaves pending/conflicted evidence untouched.

Existing direct enqueue, workflow, worker shutdown, execution-resource, and idempotency tests pass; saturation coverage proves promotion now hands work into the ordinary queue independently of execution permits. Canonical and vendored migrations match. SQLx offline builds pass. Workspace lint, documentation, package, and smoke gates pass; the patch-semver gate remains CI-only because `cargo-semver-checks` is unavailable locally. The final merged review contains no unresolved critical or high-severity correctness finding before release handoff.

## Idempotence and Recovery

Migration application is forward-only through SQLx migration history. Never edit an already-applied migration; add a later migration if implementation discovers a schema correction after publication. The additive table is intentionally ignored by older binaries.

Intent recording is safe to retry only with the identical canonical request. Promotion locks rows with `SKIP LOCKED` and commits queue insertion/event creation and intent transition atomically. Cancellation or transaction failure leaves the intent pending and creates no partial job. A disabled definition or absent worker can delay delivery indefinitely without consuming an attempt; backlog metrics make that condition alertable.

Promoted cleanup is bounded and repeatable. It is safe to rerun with the same cutoff. Conflicted intents are never automatically retried or deleted. If an operator determines replacement work is safe, the application must record a new intent with a deliberately new idempotency key and retain the original conflict long enough for audit. Runledger must not mutate the original request or infer that an external side effect is repeatable.

## Artifacts and Notes

The central state transition is:

    application transaction
        -> PENDING intent (no definition lookup, no job attempt)
        -> PROMOTED intent + ordinary job/event in one promotion transaction
        -> normal worker claim and lifecycle

The only alternate terminal path is:

    PENDING intent
        -> CONFLICTED when the queue key already names a different canonical request
        -> explicit application/operator decision; never automatic replay

The expected promotion report invariant is:

    total_promoted == inserted_jobs + existing_jobs

The most important rollout invariant is that the schema is expanded before any writer calls the new API. A compatible worker should be live before writers switch so delivery begins immediately, but starting writers first is still durable: rows remain pending until a compatible worker is restored.

## Interfaces and Dependencies

In `runledger-postgres/src/jobs/types/enqueue.rs`, define and publicly re-export these API shapes with documentation and constructors:

    pub struct JobEnqueueIntent<'a> { /* private fields */ }
    pub enum JobEnqueueIntentStatus { Pending, Promoted, Conflicted }
    pub enum JobEnqueueIntentDisposition { Inserted, Existing }
    pub struct JobEnqueueIntentOutcome { /* intent id, state, job id, disposition */ }
    pub struct JobEnqueueIntentRecord { /* persisted request and lifecycle fields */ }
    pub struct JobEnqueueIntentListFilter<'a> { /* scope, status, job type, page */ }
    pub struct JobEnqueueIntentMetricsRecord { /* type, counts, oldest pending */ }
    pub struct JobEnqueueIntentPromotionReport { /* inserted, existing, conflicts */ }

The evolving input and filter types keep their fields private and expose these consuming builders. Outcome, record, metrics, and report fields are public for observation, and their structs are non-exhaustive so later additive fields remain patch-compatible.

    impl<'a> JobEnqueueIntent<'a> {
        pub fn new(
            job_type: JobType<'a>,
            payload: &'a Value,
            idempotency_key: &'a str,
        ) -> Self;
        pub fn with_organization_id(self, organization_id: Uuid) -> Self;
        pub fn with_priority(self, priority: i32) -> Self;
        pub fn with_max_attempts(self, max_attempts: i32) -> Self;
        pub fn with_timeout_seconds(self, timeout_seconds: i32) -> Self;
        pub fn with_next_run_at(self, next_run_at: DateTime<Utc>) -> Self;
        pub fn with_stage(self, stage: JobStage) -> Self;
        pub fn with_execution_resource(self, key: &'a str) -> Self;
    }

    impl<'a> JobEnqueueIntentListFilter<'a> {
        pub fn new(limit: i64, offset: i64) -> Self;
        pub fn with_organization_id(self, organization_id: Uuid) -> Self;
        pub fn with_status(self, status: JobEnqueueIntentStatus) -> Self;
        pub fn with_job_type_query(self, job_type: &'a str) -> Self;
    }

In `runledger-postgres/src/jobs/queue/intents.rs`, define and publicly re-export:

    pub async fn record_job_enqueue_intent_tx(
        tx: &mut DbTx<'_>,
        intent: &JobEnqueueIntent<'_>,
    ) -> Result<JobEnqueueIntentOutcome>;

    pub async fn record_job_enqueue_intent(
        pool: &DbPool,
        intent: &JobEnqueueIntent<'_>,
    ) -> Result<JobEnqueueIntentOutcome>;

    pub async fn get_job_enqueue_intent_by_id(
        pool: &DbPool,
        organization_id: Option<Uuid>,
        intent_id: Uuid,
    ) -> Result<Option<JobEnqueueIntentRecord>>;

    pub async fn list_job_enqueue_intents(
        pool: &DbPool,
        filter: &JobEnqueueIntentListFilter<'_>,
    ) -> Result<Vec<JobEnqueueIntentRecord>>;

    pub async fn get_job_enqueue_intent_metrics(
        pool: &DbPool,
        organization_id: Option<Uuid>,
        job_type: Option<JobType<'_>>,
    ) -> Result<Vec<JobEnqueueIntentMetricsRecord>>;

    pub async fn promote_job_enqueue_intents_for_types(
        pool: &DbPool,
        allowed_job_types: &[JobType<'_>],
        limit: i64,
    ) -> Result<JobEnqueueIntentPromotionReport>;

    pub async fn delete_promoted_job_enqueue_intents_before(
        pool: &DbPool,
        cutoff: DateTime<Utc>,
        limit: i64,
    ) -> Result<u64>;

    pub async fn delete_promoted_job_enqueue_intents_for_jobs_tx(
        tx: &mut DbTx<'_>,
        job_ids: &[Uuid],
    ) -> Result<u64>;

Use only existing workspace dependencies: `chrono`, `serde`, `serde_json`, `sqlx`, `tokio`, `tracing`, `runledger-core`, and the current test-support crate. Do not introduce a second queue, notification service, encryption layer, background scheduler, runtime configuration flag, CreditKit-specific type, or compatibility shim.

Revision note (2026-08-18): Re-verified and comprehensively rewrote the initial plan after the repository updated from the earlier 0.8-era layout to commit `53d0172` / 0.9.1. The revision updates module paths and test entrypoints, adopts the new checked transaction capabilities and worker iteration structure, accounts for strict lint/package/semver gates, adds backlog metrics and safe promoted-row retention, defines conflict lifecycle and recovery limits, and makes the additive rollout and rollback sequence explicit.
