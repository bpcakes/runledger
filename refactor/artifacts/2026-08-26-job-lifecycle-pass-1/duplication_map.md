# Duplication Map — 2026-08-26-job-lifecycle-pass-1

Generated: 2026-08-26 13:06 UTC
Tools run: (none installed)
Raw outputs: refactor/artifacts/2026-08-26-job-lifecycle-pass-1/scans/

| ID  | Kind | Locations | LOC each | × | Type | Notes |
|-----|------|-----------|----------|---|------|-------|
| — | — | `runledger-postgres/src/jobs/queue` manual census | — | — | — | No behavior-preserving duplicate collapse with positive LOC savings was found. |

## Structural observation

The timeout constants and helpers lived under `queue/lifecycle/common.rs`, even
though unstarted-claim release is a sibling queue lifecycle transition. That
ownership boundary made the release fallback easy to omit from the boundedness
policy. The task-required extraction to `queue/lifecycle_timeouts.rs` was kept
as a separate isomorphic commit (`3ef2900`); it adds eight source lines and is
therefore not represented as a LOC-saving duplication candidate.
