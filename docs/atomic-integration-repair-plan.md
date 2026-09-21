# Atomic integration contracts (PR 20 follow-up)

Owning issue: `runledger-runledger-simplification-audit-l8p`.

## Outcome and scope

Atomic application writes, named queue operations and schema inspection use one
declared database authority. A required handoff never returns a known conflict
as accepted. The development dependency graph is reproducible and honestly
unpublished. Keep persistence migrations unchanged; do not publish or merge.

## Evidence and decisions

- Fact: `src/atomic.rs::run_atomic` accepts a raw pool and delegates to a runner
  that normalizes with DISCARD ALL; no profile restoration occurs.
- Fact: `src/migrations.rs` migrates on caller search_path but verifies public.
- Fact: the crate-level handoff example checks conflict after runner completion.
- Fact: `PgQueryExecutor` admits native idle connections and read-only scopes to
  mutation helpers. A separate private transaction/write marker can reject them.
- Fact: the foundation path dependency is unpublished, and archive smoke applies
  source patches. It is not publication evidence.
- Decision: use an explicitly unpublished coordinated-development graph, not
  unauthorized foundation publication. Clean-checkout CI must enforce its source
  prerequisites; archive checks must label patched-source evidence accurately.
- Decision (user approved): support custom schemas and SET ROLE with a declared,
  validated profile shared across paths. Do not infer policy from arbitrary pool
  hooks. No silent public-only downgrade.
- External contracts: PostgreSQL 18 DISCARD resets session authorization and GUCs
  (https://www.postgresql.org/docs/18/sql-discard.html); Cargo path+version uses the
  registry after packaging (https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#multiple-locations).

## Execution graph

### T-01 — Establish consistent authority before consumer SQL
- Outcome: ordinary, atomic and verification paths agree on role/schema/parameters.
- Changes: database profile, atomic runner setup, migration and runtime boundaries.
- Depends on: none
- Verify: PostgreSQL 18 downgraded login, custom schema, timeout and cross-path tests.
- Recovery: reject unsupported/mismatched profiles before consumer SQL; no stored-data rewrite.
- Done when: profile policy is explicit and all affected paths enforce it.

### T-02 — Make required handoff reject known conflict
- Outcome: canonical intent success contains only accepted observations.
- Changes: atomic intent method/result types, crate rustdoc, guides and consumers.
- Depends on: none
- Verify: seed conflicted intent, insert application row, require handoff, assert
  acknowledged rejection and absence of the application row; compile contracts.
- Recovery: source-level hard cutover; preserve low-level observation semantics.
- Done when: canonical callers need no success-status filter to reject a conflict.

### T-03 — Make coordinated development reproducible and unpublished
- Outcome: clean checkout has an enforced immutable dependency arrangement.
- Changes: package manifests, setup/packaging/license scripts, CI and installation docs.
- Depends on: none
- Verify: clean checkout without sibling, explicit bootstrap/pin checks, reject
  publication, distinguish unpatched package verification from archive-source checks.
- Recovery: no publication; preserve existing source archives and lockfile provenance.
- Done when: no installation or packaging claim implies a nonexistent registry dependency.

### T-04 — Restrict private mutation capability
- Outcome: idle/native and read-only execution cannot enter transaction mutation helpers.
- Changes: transaction_executor, ReadCommittedExecutor, enqueue/intents/event bounds.
- Depends on: none
- Verify: compiler-negative checks for raw connection/read-only scope plus existing tests.
- Recovery: internal source-only change; no stored-data effects.
- Done when: only DbTx and PgScopedSql implement the private write/transaction marker.

### T-05 — Validate and deliver the complete integration
- Outcome: reviewed source pair and exact-head hosted CI pass.
- Changes: tests, companion consumers if authorized, documentation and PR evidence.
- Depends on: T-01, T-02, T-03, T-04
- Verify: PostgreSQL 18 exact version, lint, compile-fail, clean source/package
  contracts, paired downstream checks, exact-head hosted CI.
- Recovery: do not merge or publish; report authority/companion scope blockers.
- Done when: all approval criteria have executed evidence or an explicit user decision.

## Risks and decision gates

## Implementation evidence (2026-09-21)

- T-01 implemented: Batter profiles are re-established after reset, before BEGIN,
  and retained for boundary validation. `RunledgerDatabase` owns mandatory hooks;
  ordinary APIs/runtime use its pool. The declared schema is customisable, but
  Runledger allows one ordinary schema (no fallback search-path resolution).
  Migration roles can differ from serving roles while naming the same schema.
- T-02 implemented: accepted intent states exclude known conflict; the required
  method returns a typed rejection. Low-level observation remains explicitly named.
- T-03 implemented: all workspace packages are unpublished; bootstrap/build guard
  verifies the pinned foundation inputs locally. Companion-only Batter descendants
  are allowed when those inputs are identical, avoiding circular repository pins.
  The archive smoke remains patched source evidence; unpatched packaging currently
  rejects the unavailable batter-sqlx registry dependency, as explicitly tested.
- T-04 implemented: mutation dispatch requires the private transaction/write
  marker. Compile-time positive/negative assertions cover all four executors.
- PostgreSQL 18.6: full workspace suite passed; custom quoted schema, SET ROLE,
  tenant settings, timeout equality, pool contamination, authority tampering,
  required-conflict rollback and all 25 migration regressions passed. Full lint
  and rustdoc passed. Nine patched archive consumer tests and license archives
  passed. A clean checkout with no sibling bootstrapped the pinned foundation,
  built all targets and observed the required unpatched-package rejection.
- Final build-contract inspection restored the pre-existing migration-copy guard
  alongside the new foundation-pin guard. The isolated negative control now
  rejects both foundation drift and an added unsynchronized migration file;
  workspace clippy and README/source checks passed after that repair.
- Hosted clean-checkout, cargo-deny and semver jobs passed at `039badd`; final-head
  hosted results remain a separate delivery check, not implied by local receipts.
- Batter full verification passed on Rust 1.94.0 and 1.98.1. Its SQLx live suite,
  Runledger adapter probe and all 66 reference cases plus both private library
  probes passed against PostgreSQL 18.6. All five HTTP profiles passed on each
  toolchain; all five companion Jig targets passed against frozen sources.
- The exact fresh-agent source and first-pass compile evidence are retained in
  `docs/evidence/profile-consumer-2026-09-21.*`. It used the canonical API and
  exhaustive outcomes; the README's leftover manual-first example was corrected.

Local implementation and delivery preparation are complete. Final exact-head
hosted CI is tracked in PR 20 and the companion PR checks, not inferred from
these receipts. No merge/publication has been performed.

API assessment: profiles declare policy, not perpetual remote authority. A pool
cannot be promoted from arbitrary hooks. Setup happens before consumer access;
failure never yields a scope. Raw SQL may cause irreversible effects before a
boundary check and remains a documented escape hatch. Existing native pool/tx
APIs do not gain an atomic-result or lock-phase guarantee. Lazy construction
preserves native background maintenance; application startup registers close
before yielding. No persisted migration or SQLx query metadata changed.

## Remaining delivery risks

The profile changes the database trust boundary; pool hooks cannot be assumed to
survive normalization. No application query may precede establishment/validation.
Arbitrary SQL can deliberately change remote state; do not claim it is sandboxed.
Custom schema support cannot be implemented by unsafe SQL identifier interpolation
or rewriting shipped migrations. The merged Batter consumer may require a new
coordinated PR, not additional commits to its already-merged feature PR.

The package alternative intentionally does not prove publishability. Do not make
an unpatched publication check green by suppressing dependency resolution errors.
Retain exact failure causes and provisional output when setup or cleanup fails.
