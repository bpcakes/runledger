# Changelog

All notable changes to this workspace are documented here.

## [Unreleased]

### Added

- Add durable job enqueue intents for atomically recording application state
  and future background work before a job definition exists. Standard workers
  promote registered types through ordinary enqueue semantics; public APIs
  expose strict idempotency, status lookup, backlog metrics, and bounded cleanup
  of old promoted intents. Per-intent savepoints and bounded exponential retry
  metadata isolate persistent database failures without starving later work.

### Changed

- Declare Unix-like operating systems as the only supported platform and remove
  the unused Windows container-reaper implementation.
- Drain full enqueue-intent promotion batches without an idle poll delay, and
  add independent promoter configuration plus a supervisor opt-out for worker
  processes that do not use enqueue intents.
- Promoted enqueue intents now retain linked `job_queue` rows with
  `ON DELETE RESTRICT`. Applications can remove links for exact retained job
  IDs inside the same caller-owned transaction before deleting queue rows.
  Promotion and exact-ID retention use a transaction-scoped shared/exclusive
  fence so their queue-row lock orders cannot deadlock; cutoff-based intent
  cleanup remains available for independent retention.
- Keep the shared PostgreSQL 18 test container process-scoped and add optional
  process-liveness cleanup for normal exit, aborts, and forced termination.
  Reaper startup is bounded and degrades without blocking database-backed tests
  when the Docker CLI is unavailable or unresponsive.
- Sanitize deferred reaper query-error logs. The structured fields
  `error_internal_message`, `error_source`, and `error_has_source` are replaced
  by `error_constraint`; update log-based alerts and dashboards when upgrading.

### Upgrade notes

- Apply migration `202608180001_job_enqueue_intents` before deploying workers
  with intent promotion. Once an application adopts enqueue intents, promoted
  rows intentionally fence deletion of their linked `job_queue` rows. Queue
  retention must delete exact promoted-intent links with
  `delete_promoted_job_enqueue_intents_for_jobs_tx` in the same transaction
  before deleting those jobs. Select candidate IDs without row locks and call
  the helper as the transaction's first lock-taking operation. Deploy that
  retention path to every retention caller before enabling intent writers.
  Deployments that do not record intents have no linked rows and their existing
  retention behavior is unchanged.
- Intent promotion is enabled by default for every worker-enabled supervisor so
  durable requests self-drain. Each supervisor independently polls even when no
  intents are pending. Idle passes use one non-locking eligibility query and do
  not open a transaction or acquire the retention fence. Capacity plans should
  include that query rate; use the intent-specific polling configuration or
  disable redundant promoters, while retaining promoter coverage for every type
  that can receive intents.

## [0.9.1] - 2026-08-12
[Compare changes](https://github.com/bpcakes/runledger/compare/v0.9.0...v0.9.1)

### Added

- Add the validated `WorkflowStepExecution` and `WorkflowJobStepExecution`
  views, exposed through `WorkflowStepEnqueue::execution()`. Existing workflow
  builders, getters, persistence shapes, and execution behavior remain
  unchanged.

### Changed

- Split schedule persistence, job-definition storage, workflow enqueue and
  recovery, failure transitions, claimed-job execution, worker tests, TUI input
  state, and test support into focused internal modules without changing their
  public behavior.
- Include the repository's MIT license text in every published crate archive.
- Harden release verification with packaged-license and semantic-versioning
  checks, current GitHub Actions dependencies, and a publication preflight that
  requires the exact remote commit's CI run to be successful.

### Testing

- Stabilize the packaged retry smoke test and expand regression coverage for
  workflow-step validation, cancellation lock ordering, worker execution,
  search state, and module-boundary refactors.

No schema migrations have been added since 0.8.0.

## [0.9.0] - 2026-08-09
[Compare changes](https://github.com/bpcakes/runledger/compare/v0.8.0...v0.9.0)

### Added

- Add `JobLeaseIdentity` and typed `_for_lease` variants for heartbeat,
  progress, success, failure, and continuation lifecycle writes. The value
  keeps the job ID, run number, attempt, and worker ID together as one exact
  lease fence; the runtime now uses that identity throughout heartbeat and
  completion persistence. Existing positional lifecycle functions remain
  source-compatible wrappers.

### Changed

- Consolidate workflow step validation and persisted workflow snapshot codecs
  so DAG building, append validation, append idempotency, and strict recovery
  decoding share their structural rules without changing their public error
  contracts.
- Split queue claim/enqueue, job admin, workflow runtime/release/recovery,
  worker completion, reaper terminal-hook, and workflow-result waiting logic
  into focused internal phases. Checked `READ COMMITTED` capabilities and
  explicit workflow lock/release phase types now encode transaction and lock
  invariants while preserving external behavior.
- Replace remaining nested `mod.rs` roots with named module files and isolate
  lock-wait timing observations in the runtime cancellation regression tests.

### Documentation

- Refresh the README, downstream integration guide, LLM summary, public API
  documentation, and maintainer routing guides for the 0.8 contracts and the
  0.9 lease-identity API.

No schema migrations have been added since 0.8.0.

## [0.8.0] - 2026-07-28
[Compare changes](https://github.com/bpcakes/runledger/compare/v0.7.0...v0.8.0)

### Added

- Add explicit, persisted workflow-step handler continuation through
  `WorkflowStepEnqueueBuilder::allow_handler_continuation()`. The default
  remains disabled, external steps cannot opt in, and a committed continuation
  atomically advances the same job to a fresh run while returning its workflow
  step from `RUNNING` to `ENQUEUED` without releasing dependencies or
  recomputing terminal workflow state.
- Add handler-selected retry timing with `JobRetryTiming`,
  `JobFailure::retry_not_before_delay`, and
  `JobFailure::retry_not_before`. Handler timing is a lower bound: PostgreSQL
  schedules the later of ordinary policy backoff and the handler hint, using the
  database clock. Attempts and events persist the requested not-before time,
  effective time, policy delay, and winning source.
- Add reusable workflow active keys with explicit `Inserted`,
  `ExistingActive`, and `ExistingIdempotent` enqueue outcomes. Claims are
  scoped globally or by organization (not by workflow type) and release only
  after terminal work is quiescent; collision classification remains atomic
  across terminal handoff, and keys are limited to 512 bytes.
- Add durable single-permit execution resources. A job acquires its optional
  resource atomically before leasing; blocked jobs consume neither an attempt
  nor a worker claim slot, and exact run/attempt/worker ownership is released
  across every lease-ending lifecycle path. Keys coordinate globally across
  organizations, claim batches deduplicate queued heads per key before applying
  their limit, and successful direct-job replay preserves the source resource
  constraint. Resource-head discovery uses a bounded window rather than an
  unbounded backlog scan; concurrent claimers or a dense same-key window may
  therefore return a short batch. PostgreSQL rejects a constrained lease that
  lacks its exact durable claim, so an older worker fails loudly rather than
  silently bypassing mutual exclusion.
- Add immutable workflow recovery through `recover_workflow_run`. Recovery
  creates a new run, records durable source lineage, replays the canonical
  enqueue snapshot plus append history, preserves active/resource constraints,
  retains resolved per-step queue settings when job-definition defaults change,
  uses each source step's latest persisted payload, rejects legacy runs that
  cannot be reconstructed safely, bounds request keys, and prevents
  recovery-only retention from erasing request idempotency.
- Add a forward continuation-metrics view migration that rejects malformed
  continuation event payloads while preserving both kindless 0.6 and
  discriminated 0.7 event compatibility.
- Add regression coverage for SQLx's raw-migrator advisory-lock failure mode
  and document `MIGRATOR` as an inspection surface rather than the supported
  startup migration path.

### Changed

- Breaking for direct struct-literal consumers: `JobFailure` now carries private
  optional retry timing and must be built with its constructors, while
  non-exhaustive `JobFailureUpdate` carries both `policy_retry_delay_ms` and the
  optional handler `retry_timing`; construct it with `JobFailureUpdate::new`
  and `with_retry_timing`. Exhausted, terminal, and panicked failures ignore
  timing.
- Breaking for direct struct-literal consumers:
  `WorkflowDagStepValidationInput` adds handler-continuation and execution-
  resource fields, while `WorkflowStepDbRecord` adds the corresponding
  persisted fields. `WorkflowDagStepValidationInput` is now non-exhaustive and
  constructed with `WorkflowDagStepValidationInput::new`.
- `WorkflowRecoveryRequest` and `WorkflowRecoveryOutcome` are non-exhaustive;
  construct requests with `WorkflowRecoveryRequest::new` and its scope/source
  step setters.
- Deprecate `JobFailure::retry_after` and `JobFailure::retry_at` in favor of
  `JobFailure::retry_not_before_delay` and `JobFailure::retry_not_before`,
  making the lower-bound scheduling semantics a compile-time migration signal.
- `RetryScheduledAt` now represents any handler-selected not-before lower
  bound, including a relative delay converted to its database-clock timestamp;
  `requested_retry_at` carries that resolved absolute boundary.
- `job_attempts.retry_delay_ms` and the matching retry-event field now retain
  ordinary policy delay even when a handler not-before bound wins. Existing
  dashboards must use `effective_next_run_at` and `retry_timing_source` instead
  of deriving the committed schedule from `retry_delay_ms`. Retry events retain
  `requested_retry_at` and `next_run_at` as legacy aliases alongside the
  clarified `requested_retry_not_before` and `effective_next_run_at` fields.
- Harmless zero or pre-PostgreSQL-range handler lower bounds now fall back to
  ordinary retry policy instead of turning an otherwise retryable failure into
  a terminal invalid-timing failure.
- Every new workflow now persists a canonical enqueue snapshot for safe
  recovery, duplicating step payload JSON in `workflow_runs.enqueue_request`;
  high-volume operators should account for the additional retained storage.
- Workflow recovery now rejects unknown canonical snapshot fields, and
  `workflow_run_mutations.mutation_kind` enforces the currently supported
  `APPEND_STEPS` history shape.
- Harden release preparation and publication with resumable version
  preparation, locked external-consumer resolution, packaged TUI verification,
  remote branch/tag preflights, and a dry-run push before crate publication.
- Point crate metadata, generated changelog links, and source documentation at
  the current `bpcakes/runledger` repository.

### Fixed

- Require caller-owned workflow recovery transactions to use `READ COMMITTED`
  so a retry that waits behind an equal request reloads the committed lineage
  instead of reaching the recovery uniqueness constraint with a stale snapshot.
  The pool-owning recovery API now establishes that isolation explicitly.
- Preserve workflow dependency blocking, attempt history, checkpoint/progress,
  lease fencing, and job-row-before-step-row lock order across handler
  continuation, including continuation/cancellation races in both lock orders.
- Keep reusable workflow active keys reserved when a single leased workflow job
  is canceled, deferring release until the canceled handler's lease quiesces.
- Reconcile terminal workflow active claims during bounded reaper sweeps even
  when a custom writer does not call the Rust release hook. A database trigger
  marks terminal claims release-pending so idle sweeps use the partial cleanup
  index instead of scanning every live active claim.
- Fill execution-resource claim batches after per-key deduplication and reclaim
  stale resource ownership when its fenced lease has expired. Resource-claim
  inserts use consistent resource-key order to reduce cross-filter deadlock
  risk, resource-head windows use queue-order index scans rather than sorting
  the complete keyed backlog, already-owned dense keys do not starve unrelated
  resources, and cleanup does not get ahead of an owning job row awaiting its
  reaper turn. Reaped lease transitions now commit before bounded
  coordination-claim cleanup; detailed results report released-claim counts and
  cleanup errors, while the runtime logs failures and batch saturation.
- Reject Unicode-whitespace-only active, execution-resource, and recovery keys
  in database constraints as well as Rust validation.
- Mirror handler-continuation restrictions in both workflow-step and lightweight
  DAG validation.
- Exclude null, wrong-typed, and otherwise malformed handler-continuation event
  payloads from 24-hour and active-continuation metrics.
- Show compact-table and workflow-detail keyboard hints only when the selected
  row actually opens a detail view.

Release 0.8.0 requires
`202607250001_harden_continuation_metrics_payload_validation` and migrations
`202607280001_workflow_step_handler_continuation` through
`202607280005_workflow_recoveries` before any 0.8 runtime loop or persistence
API runs. Keep the new write paths unused until the documented 0.7-to-0.8
writer and lease quiescence fence has completed.

## [0.7.0] - 2026-07-25
[Compare changes](https://github.com/bpcakes/runledger/compare/v0.6.0...v0.7.0)

### Added

- Add pool-owning `compare_and_requeue_job` for standalone typed recovery; it
  owns a `READ COMMITTED` transaction and commits every normal typed outcome.
  Add `CompareAndRequeueJob::from_observed_job`,
  `NonRequeueableJobStatusError`, and `TryFrom<JobStatus>` for
  `RequeueableJobStatus` so callers can derive exact scope, terminal status, and
  run expectations from an observed job without hand-copying them.
- Add first-class, idempotent successful-job replay through
  `CompareAndReplaySucceededJob`, `CompareAndReplaySucceededJobOutcome`,
  `compare_and_replay_succeeded_job`, and
  `compare_and_replay_succeeded_job_tx`. Replay preserves the successful source
  and output, creates a fresh run-1 direct job, requires a stable replay request
  key and reason, and persists source/replay lineage.
- Add `get_job_continuation_metrics` and `JobContinuationMetricsRecord` with
  per-job-type 24-hour continuation volume, current continuation-created-run
  count, and maximum active run-depth signals. Current-run correlation lets the
  active lookup use the existing job/run event index.
- Add migration `202607190001_job_replays_and_continuation_metrics`, which
  creates `job_replays` and the dedicated `job_continuation_metrics_rollup`
  view. It remains compatible with Runledger's filtered released 0.6.0 startup
  and schema guards during expand-first rollout by relying on SQLx history
  without advancing the custom compatibility-fence history.
- Add a PostgreSQL 18 activation and rollback runbook for the 0.6 continuation
  and recovery fence, including copyable lease, cancellation-quiescence,
  current-run continuation-drain, continuation-rate, and run-depth queries.
- Add a production continuation adoption guide covering versioned checkpoints,
  idempotent slices, application-owned deadlines, canary rollout, and alerts,
  backed by a compile-checked external-consumer smoke test that also exercises
  typed recovery, successful replay, and continuation metrics.
- Show each `REQUEUED` event's reason in the TUI and show the selected
  continuation's next run number, timestamp, and microsecond delay; event search
  now includes payload fields. Add searchable dashboard continuation volume,
  current continuation-created-run count, and maximum active run depth, with
  filtering and Enter navigation sharing the same fields.
- Show successful-replay `ENQUEUED` provenance in the TUI, and add a stable
  `requeue_kind` discriminator to new `REQUEUED` payloads while retaining the
  kindless 0.6.0 continuation fallback during mixed-version rollout.
- Add `JobEventRecord::decoded_payload()` with typed, non-exhaustive views of
  Runledger-authored requeue and successful-replay payloads. Raw JSON remains
  available, while malformed and future payloads degrade to compatibility
  fallbacks instead of failing event reads.

### Changed

- Clarify that the 0.6 rollout fence applies to every pre-0.6 job-state writer,
  that continuation has no global feature switch and remains optional, and that
  applications using deprecated `requeue_job` must still complete a typed
  recovery migration after quiescence.
- Document the exact legacy recovery migration: an omitted organization was a
  wildcard rather than global scope, reset state is the compatibility policy,
  and legacy error cases become typed no-mutation outcomes.
- Document successful replay as a compare-and-create operation with exact source
  expectations, required idempotency metadata, source-preserving semantics, a
  fresh replay job, and Runledger-managed lineage.
- Document the two additive-migration code-rollback choices: preferably leave it
  applied and use Runledger's filtered startup or schema guard; an exact older
  raw `MIGRATOR.run(...)` rejects the newer SQLx history row and therefore needs
  patched startup or the destructive newer down migration with explicit
  acceptance of replay-lineage and replay-idempotency loss.
- Standardize the pool-owned compare/requeue and successful-replay transaction
  lifecycle so begin/isolation/commit failures use contextual query errors and
  rejected operations are explicitly rolled back.

### Fixed

- Preserve replay idempotency during queue retention by blocking deletion of a
  replay job while its successful source remains; source deletion and one-shot
  retention of both endpoints still remove lineage safely.
- Avoid global continuation-event aggregation for exact-scope metrics queries by
  using a predicate-pushable rollup on PostgreSQL 18.
- Keep TUI event filtering, selection, and rendering on one search predicate,
  and avoid constructing searchable fields or serializing payloads while search
  is inactive. Keep dashboard rendering, search, and Enter navigation on one
  exact formatted row, and admit whole optional column tiers only when their
  widths, spacing, borders, and selection marker fit the terminal.
- Reduce dashboard-refresh latency by running its independent reads concurrently
  with fail-fast sibling cancellation, and avoid rechecking transaction
  isolation after pool-owned recovery wrappers have already established
  `READ COMMITTED`.
- Validate pool-owned successful-replay request identity before acquiring a
  connection, while retaining the same independent validation for caller-owned
  transactions.

Release 0.7.0 requires migration
`202607190001_job_replays_and_continuation_metrics` before successful-replay or
continuation-metrics APIs are used.

## [0.6.0] - 2026-07-18
[Compare changes](https://github.com/bpcakes/runledger/compare/v0.5.0...v0.6.0)

### Added

- Add successful bounded job continuation through
  `JobCompletion::continue_now()` and `JobCompletion::continue_after(...)`.
  Direct-job continuation closes the current attempt successfully, retains
  optional progress/checkpoint state, advances the same job to a fresh pending
  run, and records a `REQUEUED` handler-continuation event only for the exact
  live lease.
- Add caller-transaction compare-and-requeue through
  `compare_and_requeue_job_tx`, `CompareAndRequeueJob`,
  `CompareAndRequeueJobOutcome`, exact `JobScope`, and
  `RequeueableJobStatus`. Missing jobs and stale expectations are explicit
  no-mutation outcomes, and successful jobs are not representable as a recovery
  expectation.
- Add explicit `JobRequeueStatePolicy` selection so compare-and-requeue callers
  can preserve committed progress/checkpoints or deliberately reset them. The
  selected policy is included in the recovery event.
- Add `enqueue_job_with_outcome_tx`, `JobEnqueueOutcome`, and
  `JobEnqueueDisposition` so transactional callers receive the job ID, locked
  status/run snapshot, and inserted-versus-existing result without querying
  Runledger tables directly.
- Add `QueryErrorKind` for the small set of PostgreSQL errors that drive
  compile-checked worker policy; stable string codes remain available for
  external diagnostics.

### Changed

- Breaking: `JobCompletion` now has a continuation disposition and keeps its
  disposition/final output private so `ContinueAfter + output` is
  unrepresentable. Use the supplied constructors plus `disposition()` and
  `output()` instead of struct literals or direct field access.
- Breaking: `JobContext` now exposes the committed `checkpoint` for the claimed
  run so continuation handlers can resume from state persisted by the previous
  run. Older serialized contexts still deserialize with `checkpoint: None`.
- `JobCompletionPersistenceOperation` adds `Continuation` for failed
  continuation persistence observer events. The enum remains non-exhaustive.
- Add `JobContinuedEvent` and `JobLifecycleObserver::on_job_continued` so every
  successful continuation has a typed, post-commit observer outcome ordered
  after that run's running callback.
- Preserve deserialization of pre-0.6 `JobCompletion` values by treating a
  missing disposition as terminal success.
- Deprecate the pool-owning `requeue_job` compatibility API, whose optional
  organization argument can mean an unconstrained lookup and whose accepted
  terminal states include `SUCCEEDED`. New recovery code should use the exact,
  typed transactional API.
- The deprecated `requeue_job` compatibility API now reports an active canceled
  lease as the retryable conflict `job.cancellation_not_quiesced` instead of the
  permanent-looking `job.invalid_state_transition`; callers may retry after the
  retained lease expiry.
- Keep `enqueue_job_tx` as the UUID-only compatibility API with key-share
  concurrency for existing keyed rows; it now composes safely with
  compare-and-requeue. The enriched outcome API deliberately takes a
  mutation-ready lock for same-transaction recovery.
- Require `READ COMMITTED` for compare-and-requeue, return live mismatches
  without retaining a row lock, and return `CancellationNotQuiesced` while a
  canceled handler's original lease window is still active.
- Establish PostgreSQL 18 as the minimum supported and authoritative baseline
  for production use, diagnostics, DB-backed tests, and SQLx metadata; an
  equivalent function supplied by an extension on an older server is not a
  supported substitute.
- Document the required two-phase pre-0.6 to 0.6 rollout: deploy 0.6 to workers,
  reapers, and admin/API/repair writers with continuation and new recovery
  disabled; wait for all pre-0.6 processes and old leases to quiesce; then
  enable. After activation, rollback must disable those features, drain
  pending/leased jobs whose current run was created by a handler continuation to
  terminal, quiesce canceled live lease markers, and stop every 0.6 writer
  before any pre-0.6 process starts.

### Fixed

- Treat continuation delays outside the persisted timestamp range as terminal
  handler errors instead of persistence failures that replay the same
  deterministic invalid completion. Continuation timestamps are calculated
  exactly at PostgreSQL's microsecond precision without floating-point interval
  conversion.
- Reject workflow-managed continuation as a terminal handler error, and make
  continuation output unrepresentable through `JobCompletion` constructors or
  deserialization instead of silently discarding it.
- Preserve operation-specific, source-neutral wording when completion progress
  is invalid, with PostgreSQL as the single authoritative validator after
  durable fields are coalesced.
- Restore organization-specific idempotency lookup predicates so PostgreSQL can
  use the matching partial unique index instead of scanning a job type.
- Report success, failure, and continuation completion lease mismatches through
  `on_job_lease_lost` instead of generic persistence-failure events.
- Prevent compare-and-requeue from returning an expectation mismatch whose
  reported status and run exactly equal the caller's expectation when the job
  becomes terminal between `READ COMMITTED` statements.
- Deliver the last committed checkpoint to both worker- and reaper-originated
  dead-letter hooks.
- Consolidate the shared advance-to-next-pending-run reset into one internal
  transition while retaining the continuation path's mutation-time lease-expiry
  recheck.
- Centralize `REQUEUED` event payload construction, rejection rollback policy,
  and completion-persistence-failure observer delivery so all lifecycle paths
  retain the same field names and error semantics.
- Prevent keyed enqueue followed by compare-and-requeue in one transaction from
  deadlocking with another recovery transaction: the enriched enqueue path
  takes `FOR NO KEY UPDATE` from the outset, while the legacy UUID path uses
  `FOR KEY SHARE`, which is compatible with recovery's `FOR NO KEY UPDATE`.
- Avoid retaining a row lock after compare-and-requeue reports an active
  cancellation fence or rejects a workflow-managed job.
- Omit a null `lease_quiesces_at` field from pending-job `CANCELED` events while
  preserving the timestamp for cancellation of a live lease.

Release 0.6.0 requires no database migration.

## [0.5.0] - 2026-07-09
[Compare changes](https://github.com/bpcakes/runledger/compare/v0.4.0...v0.5.0)

### Added

- Add job lifecycle observers for worker and reaper transitions through
  `JobLifecycleObserver`, `JobLifecycleObservers`,
  `SupervisorBuilder::with_job_lifecycle_observer`,
  `run_worker_loop_with_observer`, and `run_reaper_loop_with_observer`.
- Add typed observer events for running, success, failure, completion
  persistence failure, lease loss, and lease reaping.
- Add outcome-returning persistence APIs with
  `complete_job_success_with_outcome`, `complete_job_failure_with_outcome`,
  `JobSuccessCompletionOutcome`, and `JobFailureCompletionOutcome`.
- Add per-lease reaper diagnostics through `ReapedLeaseRecord`,
  `ReapedLeaseDisposition`, and
  `ReapExpiredLeasesDetailedResult::reaped_leases`.

### Changed

- Deliver terminal observer events only after the corresponding database
  transaction commits, preserve per-job running-before-terminal ordering, and
  bound observer concurrency and shutdown draining.
- Isolate dead-letter and reaper terminal hooks from panics, timeouts, and
  shutdown races while preserving committed terminal deliveries.
- Serialize successful completion with concurrent progress writes and validate
  the coalesced persisted progress before committing.
- Persist execution-start markers for direct claims and use historical running
  events as a rolling-deploy fallback when diagnosing expired leases without a
  renewal heartbeat.
- Move shared PostgreSQL row decoders and runtime shutdown/task-group logic into
  dedicated internal modules.

### Fixed

- Avoid treating progress written after a renewal heartbeat as evidence that a
  running lease never renewed.
- Prevent observer callbacks and terminal hooks from starving later worker or
  reaper batches or causing unbounded shutdown.
- Stabilize database-backed worker and reaper timing regressions under loaded
  CI hosts.

## [0.4.0] - 2026-06-15
[Compare changes](https://github.com/bpcakes/runledger/compare/v0.3.0...v0.4.0)

### Added

- Add durable workflow result handles:
  `WorkflowDagBuilder::result_step`,
  `WorkflowRunEnqueueBuilder::try_result_step_key`,
  `enqueue_workflow_run_handle`, `retrieve_workflow_run_handle`,
  `workflow_run_handle`, `WorkflowRunHandle::get_status`,
  `WorkflowRunHandle::get_run`, `WorkflowRunHandle::get_result`, and
  `WorkflowRunWaitOptions`.
- Expose successful job completion output on `JobQueueRecord`.
- Add `count_workflow_runs` and `WorkflowRunCountFilter` for dashboard/admin
  workflow counters.
- Add classified job-payload UUID array mutation outcomes with
  `JobPayloadUuidArrayFieldUpdate` and
  `JobPayloadUuidArrayFieldUpdateRejection`.
- Add catalog job definition overrides with
  `JobCatalogDefinitionOverrides`, `job_with_definition_overrides`, and
  `definition_overrides`.
- Add catalog-owned schedule registration and sync APIs:
  `CatalogJobScheduleSpec`, `JobCatalog::schedule`,
  `sync_schedules`, `sync_schedules_with`, `sync_schedules_exact`,
  `sync_schedules_exact_with`, and `JobCatalogScheduleSyncScope`.
- Expand `runledger-tui` keyboard handling with search, type filters,
  command palette, paging, payload view toggles, ID copy, and refresh pause.

### Changed

- Breaking: `JobHandler::execute` now returns `JobCompletion`, allowing handlers
  to persist compact workflow result output with `JobCompletion::with_output(...)`.
- Breaking: the stage-bearing `JobProgress` completion type was removed; use
  `JobCompletion::success()` or `JobCompletion::with_output(...)`. In-flight
  progress updates still use `JobProgressUpdate`.
- Breaking: low-level completion inputs gained output fields:
  `JobCompletionUpdate::output` and `CompleteExternalWorkflowStepInput::output`;
  set them to `None` when no workflow result output is returned.
- Breaking: public read DTOs now expose result metadata through
  `JobQueueRecord::output`, `WorkflowStepDbRecord::output`, and
  `WorkflowRunDbRecord::result_step_key`; manual construction and exhaustive
  struct patterns must account for the new fields.
- Breaking: `cancel_workflow_run_tx` is now a no-op for any already-terminal workflow run
  (`SUCCEEDED`, `COMPLETED_WITH_ERRORS`, or `CANCELED`) and returns the existing
  run unchanged; previously only already-canceled runs were skipped, so
  canceling a succeeded run would flip it to `CANCELED`.
- `WorkflowRunWaitOptions::default()` now waits up to five minutes by default;
  set `timeout: None` to opt into waiting indefinitely.
- Direct `requeue_job` calls now reject workflow-managed jobs with
  `job.workflow_requeue_not_supported`; use workflow APIs for workflow step
  recovery or append/cancel flows instead.
- Breaking: `update_job_payload_uuid_array_field` now returns
  `JobPayloadUuidArrayFieldUpdate` instead of `bool`, so callers can distinguish
  `Updated`, `NotFound`, and rejected mutations for workflow-managed jobs,
  idempotent request snapshots, and jobs that are no longer pending or claimed.
- `JobsConfig::validate` rejects directly constructed invalid runtime configs;
  supervisor construction returns `RuntimeError::InvalidJobsConfig`, and
  low-level loops can exit with `RuntimeLoopExit::InvalidConfig`.
- Catalog schedule sync applies each spec's `is_active` value on every sync,
  while lower-level `upsert_job_schedule` preserves existing active state on
  conflict.
- Active schedules require enabled job definitions. Schedule writes/activation
  can return `job_schedule.definition_not_found_or_disabled`, and disabling a
  definition referenced by an active schedule returns
  `job_definition.active_schedule_exists`.
- The scheduler materializes at most one stale fire after downtime, then
  coalesces `next_fire_at` to the first future fire instead of replaying every
  missed cron tick.

## [0.3.0] - 2026-05-27
[Compare changes](https://github.com/bpcakes/runledger/compare/v0.2.1...v0.3.0)

### Added

- Expose schedule active state
- Add workflow DAG builder
- Add job catalog sync API

### Documentation

- Add task-oriented copy-paste examples
- Fix review findings for agent docs and CI
- Improve schedule API docs and missing-row handling

### CI

- Add GitHub CI with pinned toolchain and security checks.

  Introduce a consolidated PR workflow for linting, testing, and cargo-deny, plus Dependabot and MSRV pinning so CI matches local development.
- Fix cargo deny CI runner

## [0.2.1] - 2026-05-25
[Compare changes](https://github.com/bpcakes/runledger/compare/v0.1.2...v0.2.1)

### Added

- Address review findings for idempotency cutover
- Publish test support crate

### Fixed

- Fix workflow transaction consistency
- Bound workflow release lock waits
- Fix external consumer smoke version pinning

### Changed

- Refactor workflow transaction consistency
- Enforce enqueue request snapshot cutover

### Documentation

- Improve runtime supervisor and agent-facing docs

## [0.1.2] - 2026-05-19
[Compare changes](https://github.com/bpcakes/runledger/compare/v0.1.1...v0.1.2)

### Added

- Add retry delay overrides

### Fixed

- Restore scheduled fire time metadata

## [0.1.1] - 2026-05-17

### Added

- Add crate metadata and explicit workspace dependency versions
- Add MIT license metadata to all crate Cargo.toml files
- Add migration history tracking and external consumer smoke testing

  - Create runledger_migration_history table to track applied migrations
  - Vendor migrations into runledger-postgres/migrations/ for packaged crate consumption
  - Add build.rs to enforce sync between workspace-root and vendored migrations
  - Implement runledger_postgres::migrate() and ensure_schema_compatible() APIs
  - Add external consumer smoke test to validate packaged crate functionality
  - Exclude smoke test crate from workspace default members
  - Update documentation with consumer setup modes and testing guidance
  - Exclude smoke test lockfile from version control
- Add automated release workflow scripts
- Add validation for max_attempts, timeout_seconds, and idempotency key

### Changed

- Initialize SQLx query cache and Rust project configuration
- Refresh SQLx offline cache
- Implement workflow cancellation locking and refresh SQLx cache

  - Add locking module for workflow state management with advisory locks
  - Update runtime to coordinate cancellations with proper lock ordering
  - Add test for workflow cancel lock order validation
  - Refresh SQLx offline mode cache for new query signatures

### Documentation

- Add SQLx cache and update publishing documentation

  - Document SQLx cache strategy with per-crate directories
  - Add refresh-sqlx-cache.sh script guidance
  - Include publishing workflow and dependency order
  - Add runledger-postgres SQLx query cache
