# Changelog

All notable changes to this workspace are documented here.

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

