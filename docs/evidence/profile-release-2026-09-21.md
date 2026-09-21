# Profile restoration before idle admission

Owning issue: `runledger-runledger-simplification-audit-z5o`.
Reviewed baseline: `23dd1880f3be395ca6e53f93adc07442f43c9f78`.
Batter foundation remains `d7728e633b3b767edde8aea40daf358ad4917ea4`;
no foundation or transaction API change is needed.

## Boundary and change

SQLx 0.9.0's `Pool::try_acquire` directly removes an idle connection without
calling `before_acquire`; `try_begin` and `try_begin_with` use that path too.
The old no-op release hook therefore let one borrower leave role, search path,
tenant and timeout settings for the next borrower.

The mandatory `after_release` hook now awaits the existing profile's complete
reset/apply/verify operation before allowing idle admission. SQLx's release code
hard-closes on hook error. Existing acquisition hooks remain intact. Profile
errors retain their redacted default formatting and deliberate native-cause
access. Fast acquisition can return `None` until release cleanup finishes.

Source trace: the installed `sqlx-core-0.9.0/src/pool/mod.rs` fast methods,
`pool/connection.rs` return-to-pool hook/error branches, and `pool/inner.rs`
idle publication/minimum-connection paths. An independent investigator and a
single read-only candidate reviewer found no remaining bypass in these paths.
Cancellation was assessed through SQLx's owned connection/guard Drop semantics,
not a new executable cancellation test; it does not prove server termination.

## Regression evidence

Server: PostgreSQL **18.6 (Debian 18.6-1.pgdg13+2)**, confirmed by server queries.
`runledger-postgres/tests/database_profile/fast_paths.rs` uses independent
one-connection fixtures with a privileged login, downgraded serving role and
quoted custom schema. There is no asynchronous acquisition between contamination
and the fast-path observation.

- Before the production fix, all three fast-path tests inherited `pg_temp` instead
  of the declared search path, and the failed-restoration test timed out waiting
  for the contaminated connection to leave the pool (four failures).
- After the fix, all six `database_profile` tests passed. Each fast path checks
  role, schema, exact path, tenant and both timeouts across idle, open-transaction
  and aborted-transaction returns. Same-PID assertions prove successful sessions
  remain reusable. `try_begin_with` also preserves requested read-only mode.
- Revoking schema USAGE makes restoration fail on a healthy socket. No fast path
  can acquire it; restoring the grant permits a correctly profiled replacement
  with a different PID and restored pool capacity.
- Existing ordinary/atomic/snapshot parity and safe-display/native-cause tests
  continue to pass. The Batter adapter suite passed without foundation changes.

Focused commands, with `SQLX_OFFLINE=true` and an externally supplied disposable
PostgreSQL 18 `RUNLEDGER_TEST_ADMIN_DATABASE_URL`:

```sh
cargo test -p runledger-postgres --test database_profile --no-run --locked
cargo test -p runledger-postgres --test database_profile fast_paths:: --locked -- --nocapture
cargo test -p runledger-postgres --test database_profile --locked -- --nocapture
```

The first fast-path execution was the deliberate pre-fix failing control. The
post-fix full target passed, including after a test-only helper extraction and
Send-bound correction required by strict lint. Final local checks also passed:

```sh
cargo test -p runledger-core -p runledger-postgres -p runledger-runtime --locked
bash scripts/lint.sh
bash scripts/bootstrap-batter.sh
# From the unchanged Batter sibling:
cargo test -p batter-runledger --locked
```

The lint script includes workspace/external-consumer clippy and strict rustdoc.
No lints or semantic assertions were relaxed. Exact-head hosted outcomes are
recorded in the PR follow-up; unchanged source pins are not publication evidence.
