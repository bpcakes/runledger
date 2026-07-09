# Changelog

All notable changes to this workspace are documented here.

## [Unreleased]

## [0.5.0] - 2026-07-09
[Compare changes](https://github.com/featherenvy/runledger/compare/v0.4.0...v0.5.0)

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
[Compare changes](https://github.com/featherenvy/runledger/compare/v0.3.0...v0.4.0)

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
[Compare changes](https://github.com/featherenvy/runledger/compare/v0.2.1...v0.3.0)

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
[Compare changes](https://github.com/featherenvy/runledger/compare/v0.1.2...v0.2.1)

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
[Compare changes](https://github.com/featherenvy/runledger/compare/v0.1.1...v0.1.2)

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
