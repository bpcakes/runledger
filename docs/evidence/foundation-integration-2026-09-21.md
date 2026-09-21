# Foundation integration across parallel streams

Owning Bead: `runledger-hcf`. Review base: `b83abe4`.

Preserves the session-profile release restoration in `b83abe4`. The new paired
Batter foundation combines session profiles and operation-context atomic
completion. Native queue/profile behavior is unchanged by this integration.

`build.rs` now consumes Cargo dependency metadata from the actual SQLx package,
which forwards its actual core package directory. Both must belong to the same
reviewed source tree. Tracked and untracked foundation input drift is rejected.
Inherited workspace dependency requirements, package settings, lints and resolver
remain checked semantically. Adapter dependencies and comments are not foundation
inputs. The immutable pin and migration-copy check remain enforced on ordinary
Cargo builds; a separately selected clean sibling cannot bless patched code.

The check does not freeze the application's third-party lockfile, features or
Cargo configuration. As before, consumers own those inputs and must validate
their final graph. Cargo's foundation `links` identities reject duplicate core
or SQLx implementations. All packages remain unpublished.

Regression coverage includes clean sources, adapter pin changes, inherited
settings drift, tracked and ignored-untracked source drift, split core/SQLx roots,
and independent migration drift. Final executed validation is recorded below.
