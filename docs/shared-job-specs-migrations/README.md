# Shared job-spec migration pilots

These patches migrate real IdentityPro and CreditKit adapters for
`runledger-runledger-simplification-audit-tor` (AP-RUST-002 and API-003).
They are integration pilots applied in isolated worktrees against this Runledger
workspace. Application releases and dependency updates remain separate; neither
application's main checkout was modified or deployed.

## IdentityPro

`identitypro.patch` applies to `ee655fd0a40c6fd047bafa19ea3a1a3ade1e3e38`.
Producer definition upserts and worker registration now consume the same
`JobSpec` conversion. The local compile-time timeout constants, schedule
activation, and capability policies remain application-owned. The duplicate
runtime definition conversion and manual handler identity assertion are removed;
Runledger validates the shared specification binding.

Billing renewal uses `JobContract::submit` and `TypedJobHandler`. Its durable
payload, unknown-field rejection, tenant check, static malformed-payload code,
and explicitly snapshotted attempts/timeout are preserved. The dispatcher counts
only inserted jobs. Shape diagnostics no longer print deserializer input values.

## CreditKit

`creditkit.patch` applies to `808bf397f8729f862f11646ad1dc6f5bff488727`.
The billing crate owns the shared renewal contract, payload, and operational
settings. Worker registration consumes the spec and its typed adapter; business
logic retains tenant validation and provider-error classification. The old UUID
and timestamp parsing block is removed from the renewal handler. Unknown fields
remain accepted. A contract serializer preserves the legacy `+00:00` timestamp
spelling instead of changing existing request snapshots to `Z`.

The producer preserves its explicit one-attempt/120-second request overrides and
counts only `Inserted`. The existing database duplicate-scheduler test now
requires counts of one followed by zero while still asserting one durable row
and the exact payload fields.

## Compatibility with this Runledger revision

Both patches include mechanical replacements of older test accesses to private
`JobCompletion` fields with public accessors, preserving expected values.
CreditKit also acknowledges the fallible progress builder for its fixed five-stage
mapping. These adjustments are needed by APIs already present in this checkout.
They do not relax progress or checkpoint assertions.

## Reproduce

From clean worktrees at the revisions above, apply the appropriate patch with
`git apply --check` followed by `git apply`. Create a temporary Cargo config:

```toml
[patch.crates-io]
runledger-core = { path = "/path/to/runledger/runledger-core" }
runledger-postgres = { path = "/path/to/runledger/runledger-postgres" }
runledger-runtime = { path = "/path/to/runledger/runledger-runtime" }
```

Pass `--config /path/to/overrides.toml` to the commands below and set
`SQLX_OFFLINE=true`. Cargo lockfile changes from local overrides are excluded
from the patches. For an application release, select a published Runledger
version or pinned Git revision containing these APIs and regenerate its lockfile.

IdentityPro:

```sh
cargo check -p identitypro-jobs --no-default-features --features storage
cargo check -p identitypro-jobs --features worker,test-support --all-targets
cargo test -p identitypro-jobs --features worker,test-support --lib
```

CreditKit:

```sh
cargo check -p creditkit -p creditkit-billing --features test-support --all-targets
cargo test -p creditkit-billing --features test-support --lib renewal -- --nocapture
cargo test -p creditkit --features test-support --lib jobs::tests
```

Database tests use the application's PostgreSQL 18 harness. Providers are mocked
by the existing suites; these pilots do not establish live payment-provider
behavior or replace downstream release gates.

## Verification results

IdentityPro: storage-only construction compiled without worker/provider clients;
worker/test-support all-target checking and Clippy passed. All 84 job unit tests
passed, including typed billing serialization, malformed input, unknown-field
policy, tenant mismatch, and shared definition/catalog validation.

CreditKit: all-target checking for `creditkit` and `creditkit-billing` passed.
All 47 renewal-related billing tests and all 59 application job tests passed.
The duplicate-scheduler test recorded PostgreSQL
`18.6 (Debian 18.6-1.pgdg13+2)`, preserved the old payload spelling, and verified
that an identical retry returns zero new work. Application job tests include
real PostgreSQL enqueue-to-handler execution, stale renewal replay, tenant
checks, malformed timestamps, and catalog synchronization.

Both patches passed `git apply --check` against their source checkouts. This was
solo implementation and verification; no independent reviewer or production
rollout is claimed.

Runledger verification passed: the core suite and doctests; catalog unit tests;
63 catalog integration tests; 32 idempotency tests; three enqueue-outcome tests;
and the new core, API-only PostgreSQL, and runtime shared-spec tests. The latter
cover missing/duplicate/unknown bindings, disabled specs, legacy JSON decoding,
custom safe failures, execution-service and terminal-hook forwarding, metadata
parity, strict snapshots across definition changes, and operator disables.
The PostgreSQL acceptance test recorded the same 18.6 server version.
`scripts/lint.sh`, final workspace all-target/all-feature Clippy, warning-free
rustdoc, and a storage-free `runledger-core --no-default-features` check passed.
No SQL or migration changes were needed; all three SQLx cache directories remain
identical.
