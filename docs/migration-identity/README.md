# Composing migration identity

Runledger issue `runledger-runledger-simplification-audit-7cy` adds
`RUNLEDGER_POSTGRES_VERSION`, `migration_bundle()`, and the `MigrationBundle`
manifest. These APIs are available without feature flags, runtime setup, or a
database. The manifest borrows the same SQLx entries as `MIGRATOR`; all existing
migration and compatibility helpers remain exported.

| Input | Meaning | Use |
| --- | --- | --- |
| `library_version()` | Compiled `runledger-postgres` package version | Diagnostics; conservative release identity |
| `migrations()` | All embedded up/down entries in documented order, including exact SQL and SQLx checksums | Vendoring and individual-entry inspection |
| `bundle_fingerprint()` | SHA-256 of ordered versions, descriptions, directions, checksums, and transaction modes | Exact bundle metadata identity |
| `pipeline_fingerprint()` | SHA-256 of library version and bundle fingerprint | One component of a host template/schema fingerprint |

The v1 encoding is specified in the API rustdoc. Fingerprints are 32 raw bytes;
frame or label them when adding them to a host hash. The content identity omits
the library version; the pipeline identity conservatively changes on every
release, including releases without schema changes. It identifies released
helper behavior through the release version, not by hashing Rust source or the
dependency graph. Same-version helper patches, host configuration, dependency
overrides, and changes to application ordering need a host-owned revision input.

## IdentityPro template adapter

[identitypro.patch](identitypro.patch) is a narrow adaptation of local IdentityPro
commit `5a74dfa3bdbb90d04cc17f029282e71f0cd90788`, in
`crates/identitypro-db/src/migrations/bundle.rs`. Apply it only after selecting a
Runledger release or local dependency that includes this API; the original
published 0.12.0 dependency does not gain APIs when this repository changes.

The patch removes the duplicated Runledger version literal and its raw migrator
loop, replacing both with the exported pipeline fingerprint. It preserves
IdentityPro's `identitypro-db-migration-pipeline-v16` domain, Runlimit helper
version and metadata, application migration metadata, and the existing
`FingerprintBuilder` adapter. Existing template identities change once because
the Runledger input changes representation. The host remains responsible for
template retention and cleanup.

The host coordinator in `crates/identitypro-db/src/migrations.rs` still owns
interrupted-index recovery and the Runledger → Runlimit → IdentityPro execution
sequence. Revise the host pipeline domain when that behavior changes. An identity
API does not authorize reordering it or replacing its history validation.

The packaged external-consumer test uses IdentityPro's actual
`postgres-test-harness` 0.2.0 `FingerprintBuilder`, with small host and Runlimit
migration fixtures. It checks repeatability and invalidation for changes to host
domain, host SQL, Runlimit SQL/version, owner ordering, and omission of Runledger.
This validates the composition interface, not IdentityPro's full migration
coordinator or live database behavior.

## HOCR historical vendoring

HOCR commit `4fb17323497dcbb98e09822cdab7b3b2e926f5b8` pins Runledger 0.5.0.
Its `apps/hocr-migrate/src/main.rs` test
`launch_migrations_vendor_the_exact_runledger_0_5_0_history` independently expects
five versions and compares vendored SQL and checksums. Keep that test while the
application is pinned to 0.5.0.

The external-consumer fixture under
`smoke/external-consumer/tests/fixtures/hocr-runledger-0.5.0/` copies those five
exact SQL files from HOCR. Their bytes match the corresponding current Runledger
up migrations. The test uses the new manifest to verify this historical prefix,
with the original five-version expectation, and explicitly checks that the
current manifest has newer entries. It rejects changed SQL, SQL changed without
updating its checksum, checksum-only corruption, a missing vendored entry, and a
missing upstream entry.

After an explicit dependency upgrade, a host may inspect `bundle.migrations()`
and select its historical versions to verify that prefix. It must separately
plan and validate every later migration and cutover. The current full bundle's
fingerprint is not the fingerprint of the old 0.5.0 bundle. The fixture checks
historical content preservation; it does not claim to upgrade HOCR or apply the
new migrations to its launch history.

## Shared SQLx history and startup

The [standalone example](../../runledger-postgres/examples/migration_identity.rs)
composes Runledger identity with a host pipeline domain and host SQLx migration
metadata, using length framing. Replace its empty demonstration migrator with
the application's existing migrator. Include other libraries at the positions
chosen by the host. Add non-SQL initialization inputs as needed.

Host history rows and bundled metadata are different things: the manifest is
available content, while `_sqlx_migrations` records application of that content.
Keep using `migrate_after_idempotency_cutover` when Runledger should apply all
pending migrations, or `ensure_schema_compatible_after_idempotency_cutover` when
the host manages staged DDL. Both retain their existing shared-history behavior.
The host must allow other owners' versions in its own history validation while
checking its own checksums and append-only version set. Do not replace the host
coordinator with raw `MIGRATOR.run()` on a shared application pool.

## Verification commands

```sh
cargo test -p runledger-postgres --lib migration_identity --locked
cargo test -p runledger-postgres --doc --locked
cargo run -p runledger-postgres --example migration_identity --locked
cargo test --manifest-path smoke/external-consumer/Cargo.toml --test smoke migration_identity --locked
scripts/run-external-consumer-smoke.sh
scripts/lint.sh
```

The first four commands need no database. The packaged smoke script also runs
the existing database-backed consumer tests and requires PostgreSQL 18. Unit
tests pin independently computed SHA-256 vectors and an explicit complete
up/down version set, test individual manifest-field mutations, and establish
release invalidation with unchanged SQL. Database verification results belong
to the implementation completion record, not these command instructions.

## Implementation verification, 2026-09-05

- Four focused unit tests passed, including the independent full-bundle content
  snapshot `77005335e2e12fbcc96bd95c50d8a9c75b56293a0b91afee05b3a433ec96271c`.
- All ten `runledger-postgres` doctests passed, and the standalone composition
  example ran without a database.
- Both local consumer identity tests passed. The packaged external-consumer
  harness then passed all four tests against extracted `.crate` archives,
  including both identity tests and the existing database embedding tests.
  The server reported `18.6 (Debian 18.6-1.pgdg13+2)`, version number `180006`.
- `scripts/lint.sh` passed: README checks, formatting, migration-info parser
  checks, workspace and external-consumer Clippy, and warning-free workspace
  rustdoc. Focused Clippy was repeated after adding the full-bundle snapshot.
- The IdentityPro patch passed `git apply --check` against the source revision
  cited above. The full downstream application was not built or migrated.

No migration SQL, query SQL, SQLx cache, startup-helper implementation, or
compatibility-helper implementation changed. The runtime SHA-256 dependency
uses the 0.10.9 version already present through SQLx; the real IdentityPro
fingerprint builder is a dev dependency of the separate consumer test workspace.
