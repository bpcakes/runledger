# Execution-services migration pilots

These patches migrate real downstream handlers for Runledger issue
`runledger-runledger-simplification-audit-4a2` (audit AP-RUST-001). They were
applied and tested in isolated worktrees using this Runledger workspace through
Cargo path overrides. They are integration pilots, not application releases:
neither application was deployed or had its main checkout's code changed.
Downstream maintainers can apply them once they select a Runledger release
containing these APIs, then regenerate the application lockfile.

## OneSales CRM account sync

[onesales.patch](onesales.patch) applies to OneSales
`c5beb8f1bcd6345df181cbd79f07622bf346522e`. It migrates
`CrmSyncAccountsHandler`, including Salesforce and HubSpot, to
`JobExecutionHandler` and registers its `into_job_handler()` adapter.

The account checkpoint loader now parses the supplied resume snapshot, derives
its page quota from the remaining runtime budget, and uses the supplied absolute
deadline. Its progress writes call `JobExecution::persist_progress`. The
production account path no longer calls `get_job_by_id`, compares lease fields,
copies a worker/run/attempt tuple, or reconstructs a deadline from handler start
time. Checkpoint versions, source-job lineage, provider validation, and
continuation policies remain application-owned.

The shared Salesforce page-runner state still supports legacy contact sync.
Account states select runtime-managed progress; accidentally routing one through
the positional legacy writer fails explicitly. Contact sync is not migrated by
this pilot.

Existing direct-invocation tests now supply a test execution-service driver.
Provider-only tests without a queue record retain their explicit no-persistence
fixture; queued checkpoint tests use PostgreSQL-fenced writes. Two resume
fixtures now seed checkpoints before claiming, so their invocation snapshot
contains the intended state. The driver can read a queue row to simulate the
runtime's timeout input; the migrated production handler cannot.

Verification: all 79 CRM account tests passed, including bounded continuation
and checkpoint resume. The resume test recorded PostgreSQL
`18.6 (Debian 18.6-1.pgdg13+2)`. HTTP CRM providers are mocked by the existing
test suite; this is not evidence of live Salesforce or HubSpot execution.
`cargo check -p onesales-jobs --all-targets` also passed.

## IdentityPro bounded protection-enrollment recovery

[identitypro.patch](identitypro.patch) applies to IdentityPro
`ee655fd0a40c6fd047bafa19ea3a1a3ade1e3e38`. It migrates
`ProtectionEnrollmentRecoveryHandler` to the execution-services interface and
registers its adapter in the catalog.

The recovery service receives the runtime's absolute deadline minus the
existing 15-second application reserve. It no longer starts a new 130-second
budget when execution reaches the service call. Definition policy, ownership
identity passed to the domain service, continuation versus failure decisions,
and durable payloads remain unchanged.

All eight protection-enrollment tests passed. The new test supplies a
25-second invocation deadline and asserts that the recovery service receives
that exact deadline minus 15 seconds, so reconstructing the old fixed timeout
fails the assertion. These are domain-service unit tests; runtime timeout and
database lease behavior are exercised separately in Runledger's PostgreSQL tests.
`cargo check -p identitypro-jobs --features worker,test-support --all-targets`
also passed. The public test-support helper retains its legacy direct invocation
contract through an explicit test execution driver.

## Reproduction

Start from clean worktrees at the revisions above. From each application root,
run `git apply --check /path/to/patch`, followed by `git apply /path/to/patch`.
Both patches passed that applicability check against the original checkouts.

Create a temporary Cargo configuration file, replacing the paths below with
the location of this Runledger checkout:

    [patch.crates-io]
    runledger-core = { path = "/path/to/runledger/runledger-core" }
    runledger-postgres = { path = "/path/to/runledger/runledger-postgres" }
    runledger-runtime = { path = "/path/to/runledger/runledger-runtime" }

From OneSales:

    SQLX_OFFLINE=true cargo check -p onesales-jobs --all-targets --config /path/to/overrides.toml
    SQLX_OFFLINE=true cargo test -p onesales-jobs --lib crm_accounts_sync::tests --config /path/to/overrides.toml
    SQLX_OFFLINE=true cargo test -p onesales-jobs --lib crm_accounts_sync::tests::checkpoint_resume_tests::sync_resumes_from_checkpoint_after_page_cap_continuation --config /path/to/overrides.toml -- --nocapture

The database test harness uses Docker and PostgreSQL 18. The last command prints
the exact server version.

From IdentityPro:

    SQLX_OFFLINE=true cargo check -p identitypro-jobs --features worker,test-support --all-targets --config /path/to/overrides.toml
    SQLX_OFFLINE=true cargo test -p identitypro-jobs --features worker,test-support protection_enrollment --config /path/to/overrides.toml

The patches also replace older test accesses to private `JobCompletion` fields
with the existing public accessors required by this Runledger checkout.
Assertions retain their original values. Local Cargo lockfile changes caused
by path overrides are intentionally not included.

## Runledger verification

`cargo test --workspace --all-features` passed: 864 tests, zero failures, and
three existing ignored test entries (two manual diagnostics and a child-process
entrypoint exercised by its parent tests). Database tests used PostgreSQL 18.6.
The five new runtime tests prove commit acknowledgement before handler return,
checkpoint resume across continuation, timeout with durable checkpoint delivery
to the dead-letter hook, rejection of expired/replaced run/attempt/worker leases,
typed persistence failure, and cancellation of a progress write blocked on a row
lock. Lease loss stops handler polling even when the handler swallows the error.

`scripts/lint.sh` and `cargo check -p runledger-core --no-default-features`
passed. No SQL queries or migrations changed; the existing three SQLx cache
directories remain identical. This was a solo implementation and verification;
no independent reviewer or production rollout is claimed.
