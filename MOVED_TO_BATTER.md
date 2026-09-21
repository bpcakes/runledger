# Runledger development has moved to Batter

Runledger is maintained in the [`runledger/` directory of Batter](https://github.com/bpcakes/batter/tree/master/runledger).
Use [Batter issues](https://github.com/bpcakes/batter/issues) and
[Batter pull requests](https://github.com/bpcakes/batter/pulls) for new work.
This standalone repository retains its source, tags and historical documentation.

## What moved

[Batter PR #7](https://github.com/bpcakes/batter/pull/7) imports standalone revision
[`46b5cd085d011e597de9552dfebbed4c19416453`](https://github.com/bpcakes/runledger/commit/46b5cd085d011e597de9552dfebbed4c19416453).
The five packages retain their names: `runledger-core`, `runledger-postgres`,
`runledger-runtime`, `runledger-test-support` and `runledger-tui`. Rust imports
continue to use those crate names. Native queue, workflow, persistence and worker
responsibilities remain in Runledger; consuming the `batter` facade is optional.

The import preserves that source revision's runtime behavior and migration SQL.
Moving the repository alone does not require resetting a database, changing
queued job types, or rewriting applied migrations. Consumers upgrading from an
older revision or published release must still review the intervening API and
schema changes. See the [current native README](https://github.com/bpcakes/batter/blob/master/runledger/README.md)
and [import provenance](https://github.com/bpcakes/batter/blob/master/runledger/IMPORT.md).

## Existing consumers

Existing version-pinned dependencies and immutable Git revisions do not follow
this move automatically. They continue to select their original source. The
Batter workspace packages have publishing disabled; their retained version
numbers do not announce a replacement crates.io release.

To follow ongoing development, use a complete Batter checkout. A sibling
application can select its native packages with paths such as:

```toml
[dependencies]
runledger-core = { path = "../batter/runledger/runledger-core" }
runledger-postgres = { path = "../batter/runledger/runledger-postgres" }
runledger-runtime = { path = "../batter/runledger/runledger-runtime" }
```

For Git dependencies, use `https://github.com/bpcakes/batter.git` and pin related
packages to the same full commit from Batter's `master` containing the migration.
Keep the whole workspace when using paths; copying one crate alone loses its
workspace dependencies and build assets. Follow Batter's
[compatibility guidance](https://github.com/bpcakes/batter/blob/master/docs/reference-compatibility.md),
regenerate the application's lockfile with Cargo, and validate its dependency
graph and application tests before deploying an upgrade.

Current build, test, SQLx metadata and migration instructions live in
[Batter's testing guide](https://github.com/bpcakes/batter/blob/master/docs/testing.md).
The standalone release scripts and instructions retained here describe the
historical repository, not the release policy of the Batter workspace.
