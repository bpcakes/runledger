# Migration pipeline identity audit

Implementation follow-up: [migration identity and consumer composition](migration-identity/README.md).
The findings below record the source state at the audited commit.

Audited `runledger-runledger-simplification-audit-7cy` on 2026-09-05 against
Runledger commit `d10037a647afc7f7ea73747c93be48ea7c4a5369` and the current local
consumer sources. The issue remains justified at P2 and is not implemented.
This report audits the requested feature; it does not change migration behavior.

## Findings

1. **P2: downstream callers still maintain migration-helper identity manually.**
   The crate exports `MIGRATOR` and startup/compatibility functions, but no public
   library version or pipeline identity
   ([exports](../runledger-postgres/src/lib.rs#L334)). IdentityPro explicitly
   duplicates `"0.12.0"` in
   [bundle.rs](/home/aa/Documents/identitypro/crates/identitypro-db/src/migrations/bundle.rs:249)
   and includes it in the test-template fingerprint. This is currently consistent
   with its exact dependency pin, not evidence of an existing stale template.
   The maintenance hazard is real: SQL checksums alone cannot identify changes
   to Rust history filtering, legacy-row rejection, or constraint validation in
   [the helper](../runledger-postgres/src/migrations.rs#L285).
   Publish crate-owned identity and document when it changes. A library version
   is sufficient for conservative invalidation across immutable releases;
   same-version path dependencies or patched builds require an explicit policy
   if they are also expected to invalidate templates.

2. **P2: bundle identity and helper identity need distinct contracts.**
   Raw metadata is already available through the documented, public
   [`MIGRATOR`](../runledger-postgres/src/migrations.rs#L8). Do not describe this
   as missing access to migration versions or checksums. What remains absent is
   the promised documented manifest/identity contract and composition example.
   IdentityPro consumes version, description, migration type, checksum, and
   `no_tx`, in iteration order
   ([adapter](/home/aa/Documents/identitypro/crates/identitypro-db/src/migrations/bundle.rs:282)).
   HOCR's
   [vendoring test](/home/aa/Documents/hocr-next/apps/hocr-migrate/src/main.rs:678)
   requires the exact five up-migration versions in Runledger 0.5.0 and compares
   both SQL and checksums. Its manifest must describe that pinned historical
   bundle, not the latest Runledger schema. A combined opaque pipeline hash
   alone cannot replace that verification. Retain access to individual entries
   and SQL through `MIGRATOR`; specify ordering, direction, checksum encoding,
   transaction metadata, and identity-format version for any new interface.

3. **P2: consumer composition remains unproven.**
   The [existing runbook](../README.md#applying-or-validating-the-schema) explains
   supported startup helpers and migration inspection, but provides no example
   composing the proposed identity with host migration inputs. IdentityPro
   combines Runledger, Runlimit, and application migrators under its own
   `identitypro-db-migration-pipeline-v16` domain, then uses those inputs to
   [select a template](/home/aa/Documents/identitypro/crates/identitypro-test-support/src/db.rs:226).
   Its [coordinator](/home/aa/Documents/identitypro/crates/identitypro-db/src/migrations.rs:100)
   also owns recovery and library/application execution ordering. An exported
   Runledger identity must replace only the duplicated Runledger input, while
   retaining the host domain and other owners' inputs. Identity equality is a
   cache identity, not proof of a live database's compatibility. Keep the
   existing compatibility checker and host ordering decisions.

## Requirement assessment

| Issue requirement | Current evidence | Assessment |
| --- | --- | --- |
| Export library/migration-pipeline identity | No such item in crate exports or migration implementation; IdentityPro duplicates a version string | Missing |
| Bundle metadata usable in downstream fingerprints | `MIGRATOR` already supplies the fields used by IdentityPro and HOCR | Existing foundation; documented identity/manifest contract still needed |
| Document composition with host SQLx history | Startup/compatibility runbook exists; no identity composition example | Incomplete |
| Validate IdentityPro integration | Manual version and metadata adapter inspected; no replacement API or consumer fixture | Demand confirmed; implementation validation missing |
| Validate HOCR vendored-bundle use | Exact 0.5.0 version, SQL, and checksum assertions inspected | Demand confirmed; implementation validation missing |
| Retain migration and compatibility helpers | Explicit cutover helpers, `MIGRATOR`, and deprecated aliases remain exported | Satisfied by current source; preserve during implementation |
| Keep application ordering and cutovers application-owned | Existing helpers document staged external DDL; consumers own orchestration | Preserve this boundary |

## Acceptance checks for implementation

These are proposed checks to make the issue executable, not claims of completed
validation or mandatory API names.

1. Provide a public identity available to ordinary downstream crate consumers,
   without runtime setup, database access, or a source-checkout dependency.
   Define library release identity, helper behavior identity, and bundle content
   identity explicitly. If release version represents helper identity, state its
   conservative invalidation and same-version development limitations.
2. Document the manifest's complete inputs and deterministic representation.
   Cover up/down entries and transaction mode. If exposing a digest, specify a
   versioned, unambiguous encoding with stable field boundaries. Verify changes
   to relevant inputs change identity, while identical input reproduces it.
   Test the helper identity independently of SQL changes. Do not treat a digest
   derived from the same entries on both sides as proof of bundle completeness.
3. Compile an IdentityPro-shaped external consumer that replaces the local
   Runledger version literal, preserves host/Runlimit inputs and the host
   pipeline domain, and feeds the existing fingerprint builder. Demonstrate
   that host ordering/domain changes still invalidate the composed identity.
4. Exercise an HOCR-shaped vendored-bundle fixture with an independently pinned
   expected version set and SQL/checksum comparisons. Include a changed SQL
   entry and a missing expected entry as failures. Preserve the historical
   0.5.0 check; adopting a new metadata API requires an explicit dependency
   upgrade or a separately described historical adapter, not silently expecting
   the current bundle to equal 0.5.0.
5. Check the packaged crate through the external-consumer harness so metadata
   does not accidentally depend on workspace files. Keep existing helpers and
   aliases usable. Do not add application orchestration to the metadata API.
6. If migration execution or compatibility behavior changes, run the relevant
   existing migration tests on PostgreSQL 18 and record the exact server
   version. Metadata-only behavior can be tested without a database. Do not
   refresh SQLx caches merely for adding identity metadata.

## Evidence and limits

Inspected local IdentityPro commit
`5a74dfa3bdbb90d04cc17f029282e71f0cd90788` (dependency `=0.12.0`) and HOCR commit
`4fb17323497dcbb98e09822cdab7b3b2e926f5b8` (dependency `=0.5.0`). These observations
describe local source, not deployed or registry state.

Existing Runledger tests cover fresh migration application, unrelated shared
SQLx history, conflicting versions, compatibility fences, and equality of
vendored migration copies
([migration tests](../runledger-postgres/tests/migrations.rs#L1811)). They do not
establish the absent identity contract. Test bodies and assertions were
inspected; no Cargo tests or database experiments were run for this source
audit. No PostgreSQL-version-dependent behavior was experimentally asserted.
The proposed API and consumer adaptations remain future implementation work.
