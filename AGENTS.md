# Runledger repository agent guide

## PostgreSQL baseline

- PostgreSQL 18 is the minimum supported and authoritative database baseline for
  this repository.
- Use PostgreSQL 18 for database diagnostics, bug reproductions, migration and
  locking investigations, database-backed tests, SQLx metadata refreshes, and
  disposable diagnostic containers. The default diagnostic image is
  `postgres:18`.
- Evidence from another PostgreSQL major version is provisional until the same
  behavior is reproduced on PostgreSQL 18. An explicit compatibility experiment
  or `RUNLEDGER_TEST_PG_IMAGE` override does not redefine the repository
  baseline.
- When database version could affect a result, record the exact server version
  (`SHOW server_version` or `SHOW server_version_num`) with the diagnostic.
- Refresh SQLx metadata only against PostgreSQL 18 with the current migrations
  applied, then keep `.sqlx/`, `runledger-postgres/.sqlx/`, and
  `runledger-runtime/.sqlx/` synchronized.

More specific `AGENTS.md` files under individual crates add crate-local
instructions; they do not override this PostgreSQL baseline.
