# Rejections — 2026-08-26-job-lifecycle-pass-1

| Candidate | Why rejected as an isomorphic simplification | Action taken |
|-----------|----------------------------------------------|--------------|
| Typed `55P03` handling | Changes the semantic classification exposed to runtime policy. | Shipped separately in `3c2d4d8` with stable category/code/message and regression coverage. |
| Heartbeat retry loop | Intentionally changes behavior under transient lock contention. | Shipped separately in `f8815cd` with a fixed deadline and PostgreSQL 18 integration test. |
| Release lock cap | Intentionally changes an unbounded wait to a bounded wait. | Shipped separately in `608eac8`, scoped only to the initial job-row acquisition. |

No other LOC-saving duplicate candidates were found in the scoped queue census.
