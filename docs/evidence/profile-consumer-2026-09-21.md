# Compile-only fresh consumer exercise

Retained source: [exact agent output](profile-consumer-2026-09-21.rs) and
[manifest](profile-consumer-2026-09-21.toml). The report below is the agent's
original account. Its README friction was subsequently repaired: the canonical
profile/runner section now precedes Quick start and the handoff example uses the
required-intent method. This is one compile-only exercise, not usability or
database proof. The temporary Cargo lockfile is not a repository dependency lock.

Source: `/tmp/runledger-profile-consumer-w6Caur/src/lib.rs`.
Manifest: `/tmp/runledger-profile-consumer-w6Caur/Cargo.toml`.
Cargo-generated lockfile: `/tmp/runledger-profile-consumer-w6Caur/Cargo.lock`.

Read local Runledger README installation, API selection, durable handoff and owned
transaction sections; runledger-postgres crate docs, database and atomic public
APIs; followed reexports for PgSessionProfile, PgScopedSql and exhaustive outcome
type docs in sibling Batter. No repository review or repair was performed.

Command:

```sh
SQLX_OFFLINE=true CARGO_TARGET_DIR=/home/aa/Documents/runledger/target cargo check --manifest-path /tmp/runledger-profile-consumer-w6Caur/Cargo.toml
```

Result: exit 0, first compile attempt; final output:

```text
    Checking runledger-profile-consumer-exercise v0.0.0 (/tmp/runledger-profile-consumer-w6Caur)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 14.61s
```

`cargo fmt --manifest-path /tmp/runledger-profile-consumer-w6Caur/Cargo.toml -- --check`
also returned exit 0.

Toolchain: rustc 1.94.1 (e408947bf 2026-03-25), cargo 1.94.1
(29ea6fb6a 2026-03-24). Resolved SQLx 0.9.0.
Observed Runledger HEAD: 73e1d3eb639725decc9d74c101edc4fe561660ad.
Observed Batter HEAD: 4dc0889792d48d89c0abe773c573897cd876b0bb.
Path dependencies consume the current working files; HEADs alone do not identify
uncommitted source changes.

The function declares login/effective-role policy (SET ROLE application_writer),
custom schema application_jobs, and 30-second statement / 5-second lock timeouts.
It executes a parameterized application INSERT via scope.application, requires
an accepted durable followup intent, consumes scope.queue(), and enqueues an
immediate process job within run_atomic. Every returned atomic disposition is
matched without a wildcard. Only Ok constructs CommittedReceipt; all uncertainty
retains its provisional output/error and original cause. No automatic replay is
performed. Typed required-intent and other operation failures remain intact.

Documentation friction: the README API-selection table and Durable transactional
handoff section still lead to record_job_enqueue_intent_tx and manual commit;
the canonical owned-scope section is after License. The crate docs clearly direct
atomic work to run_atomic, which resolved the choice. Finding the precise
PgScopedSql executor and exhaustive uncertainty fields required following
Runledger reexports into Batter public type definitions. No code/API compile
friction occurred.

Limits: compilation only. No function was called; no PostgreSQL connection,
database work, schema provisioning, migration, lock/policy verification, live
rollback/commit uncertainty, cancellation or durable promotion was exercised.
Application SQL uses a runtime SQLx query, so its table/column validity is not
compile checked. The function documents required PostgreSQL 18 objects and job
registration; it does not provision them. Successful intent acceptance remains
a point-in-time observation and does not guarantee future promotion. Source and
lockfile remain in this temporary directory for retention by the parent agent.
Neither repository was edited, staged, committed, or switched by this exercise;
Cargo reused the authorized Runledger target directory.
