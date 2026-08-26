# Refactor Ledger — 2026-08-26-job-lifecycle-pass-1

| Order | ID | Commit | Scope | Source Δ | Verification |
|-------|----|--------|-------|----------|--------------|
| 1 | S1 | `3ef2900` | Queue lifecycle timeout policy extraction | +8 | PostgreSQL 18 regression and all-target Clippy passed |

## Intentional behavior and proof slices

| Order | Commit | Purpose |
|-------|--------|---------|
| 2 | `608eac8` | Bound unstarted-claim release row-lock waits and restore caller timeout before propagation. |
| 3 | `3c2d4d8` | Type PostgreSQL `55P03` without changing existing public error metadata. |
| 4 | `f8815cd` | Retry transient heartbeat contention within one immutable maintenance deadline. |
| 5 | `ae017d8` | Prove default caps, active timer rearming, and max-one-pool cleanup. |
