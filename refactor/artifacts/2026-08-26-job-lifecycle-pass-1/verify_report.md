# Verification Report — 2026-08-26-job-lifecycle-pass-1

Captured: 2026-08-26
Final implementation commit: `ae017d8`

## Root-cause classification

- Unstarted release hang: a localized omission enabled by fragmented timeout ownership. Boundedness was attached to selected functions instead of the lifecycle transition policy.
- Heartbeat abort on `55P03`: a deeper error-model and maintenance-state abstraction gap. Definite lease loss, transient lock contention, and deadline exhaustion all collapsed into generic persistence failure.

## Long-term shape implemented

- One queue-level lifecycle timeout policy shared by completion/progress/heartbeat and strict unstarted release.
- Strict release caps only initial job-row acquisition and restores the caller setting before workflow propagation.
- PostgreSQL lock-not-available is a compile-checked `QueryErrorKind` while category, code, client message, SQLSTATE, and source remain stable.
- Heartbeat maintenance retries only typed transient contention with backoff inside one fixed one-third-lease deadline.
- Lease mismatch and unrelated persistence errors still fail closed immediately.

## Gates

- `cargo test --workspace`: PASS — 845 passed, 0 failed, 3 explicitly ignored/manual.
- `cargo clippy --workspace --all-targets -- -D warnings`: PASS — zero warnings.
- `git diff --check 036404d..ae017d8`: PASS.
- PostgreSQL: 18.4, `server_version_num=180004`.
- Goldens: N/A; no deterministic binary-output corpus applies to these database/runtime transitions.
