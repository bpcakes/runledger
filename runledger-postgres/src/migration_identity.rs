use std::sync::LazyLock;

use sha2::{Digest, Sha256};
use sqlx::migrate::{Migration, MigrationType};

use crate::MIGRATOR;

/// The version of the compiled `runledger-postgres` crate, not the host crate.
pub const RUNLEDGER_POSTGRES_VERSION: &str = env!("CARGO_PKG_VERSION");

static BUNDLE: LazyLock<MigrationBundle> = LazyLock::new(|| {
    let mut migrations: Vec<_> = MIGRATOR.iter().collect();
    migrations.sort_by_key(|migration| {
        (
            migration.version,
            migration_type_tag(migration.migration_type),
        )
    });
    let bundle_fingerprint = fingerprint_bundle(&migrations);
    MigrationBundle {
        pipeline_fingerprint: fingerprint_pipeline(RUNLEDGER_POSTGRES_VERSION, &bundle_fingerprint),
        bundle_fingerprint,
        migrations,
    }
});

/// Inspect the compiled migration bundle without a database or filesystem access.
///
/// Use its pipeline fingerprint as one input to an application-owned schema or
/// test-template fingerprint. Keep the host's own pipeline revision, migration
/// inputs, and ordering policy in that composition. The fingerprint does not
/// validate a live database; retain [`crate::ensure_schema_compatible_after_idempotency_cutover`].
///
/// ```
/// let bundle = runledger_postgres::migration_bundle();
/// assert_eq!(bundle.library_version(), runledger_postgres::RUNLEDGER_POSTGRES_VERSION);
/// let pipeline_input: [u8; 32] = bundle.pipeline_fingerprint();
/// for migration in bundle.migrations() {
///     // Compare version, migration_type, checksum, and sql with a vendored bundle.
///     assert!(!migration.checksum.is_empty());
/// }
/// ```
#[must_use]
pub fn migration_bundle() -> &'static MigrationBundle {
    &BUNDLE
}

/// A read-only manifest of the exact migrations embedded in this crate.
///
/// Includes both up and down migrations. It describes available content, not
/// applied database history or a rollout plan. Existing [`MIGRATOR`] inspection
/// and startup helpers remain available.
#[derive(Debug)]
pub struct MigrationBundle {
    migrations: Vec<&'static Migration>,
    bundle_fingerprint: [u8; 32],
    pipeline_fingerprint: [u8; 32],
}

impl MigrationBundle {
    /// Release identity, conservatively including changes unrelated to migrations.
    #[must_use]
    pub const fn library_version(&self) -> &'static str {
        RUNLEDGER_POSTGRES_VERSION
    }

    /// Entries sorted by version ascending, then Simple, ReversibleUp, ReversibleDown.
    ///
    /// Each SQLx entry exposes `version`, `description`, `migration_type`, raw
    /// `checksum` bytes (SQLx SHA-384 of the SQL), `no_tx`, and exact `sql` text.
    /// This ordering is for identity/inspection; it does not prescribe host DDL
    /// ordering. Filter with `migration_type.is_up_migration()` for forward DDL.
    pub fn migrations(&self) -> impl ExactSizeIterator<Item = &'static Migration> + Clone + '_ {
        self.migrations.iter().copied()
    }

    /// SHA-256 identity of the complete bundle, independent of library version.
    ///
    /// Format v1 hashes, in order:
    /// - literal bytes `runledger-postgres:migration-bundle:v1\0`;
    /// - entry count as a big-endian u64;
    /// - each sorted entry's version (big-endian i64), framed UTF-8 description,
    ///   type tag (one byte: Simple=0, ReversibleUp=1, ReversibleDown=2), framed
    ///   raw checksum, and `no_tx` (one byte: false=0, true=1).
    ///
    /// `\0` denotes one NUL byte. A frame is a big-endian u64 byte length followed
    /// by those bytes. SQL is
    /// represented by its SQLx checksum; timestamps, paths, and crate version
    /// are excluded. Any future encoding change uses a new domain version.
    #[must_use]
    pub const fn bundle_fingerprint(&self) -> [u8; 32] {
        self.bundle_fingerprint
    }

    /// SHA-256 identity of the released migration pipeline, including its bundle.
    ///
    /// Format v1 hashes literal bytes `runledger-postgres:migration-pipeline:v1\0`,
    /// the framed UTF-8 library version, then the 32 raw bundle fingerprint bytes.
    /// Framing is the same as [`Self::bundle_fingerprint`].
    ///
    /// The crate release version identifies Rust helper behavior beyond SQL.
    /// Every new release invalidates this identity, even with unchanged SQL.
    /// Helper-only edits in same-version path/patched builds are **not** detected:
    /// add a host-owned source revision to the composed fingerprint for those
    /// builds. Also include host migration ordering, configuration, and other
    /// dependencies that affect template creation. This is not a build hash,
    /// compatibility guarantee, or replacement for checking live schema state.
    #[must_use]
    pub const fn pipeline_fingerprint(&self) -> [u8; 32] {
        self.pipeline_fingerprint
    }
}

fn fingerprint_bundle(migrations: &[&Migration]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"runledger-postgres:migration-bundle:v1\0");
    digest.update((migrations.len() as u64).to_be_bytes());
    for migration in migrations {
        digest.update(migration.version.to_be_bytes());
        add_frame(&mut digest, migration.description.as_bytes());
        digest.update([migration_type_tag(migration.migration_type)]);
        add_frame(&mut digest, &migration.checksum);
        digest.update([u8::from(migration.no_tx)]);
    }
    digest.finalize().into()
}

fn fingerprint_pipeline(version: &str, bundle_fingerprint: &[u8; 32]) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"runledger-postgres:migration-pipeline:v1\0");
    add_frame(&mut digest, version.as_bytes());
    digest.update(bundle_fingerprint);
    digest.finalize().into()
}

fn migration_type_tag(migration_type: MigrationType) -> u8 {
    match migration_type {
        MigrationType::Simple => 0,
        MigrationType::ReversibleUp => 1,
        MigrationType::ReversibleDown => 2,
    }
}

fn add_frame(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}

#[cfg(test)]
mod tests;
