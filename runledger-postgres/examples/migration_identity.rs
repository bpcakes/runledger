//! Compose Runledger identity with an application-owned SQLx migration pipeline.
//! Run with `cargo run -p runledger-postgres --example migration_identity`.
//! No database is opened; replace the empty host migrator with your existing one.

use sha2::{Digest, Sha256};
use sqlx::migrate::Migrator;

fn add_frame(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}

fn template_identity(host_pipeline_revision: &str, host: &Migrator) -> [u8; 32] {
    let mut digest = Sha256::new();
    // The host owns this domain and revises it when ordering, recovery,
    // configuration, or other non-SQL template initialization changes.
    add_frame(&mut digest, host_pipeline_revision.as_bytes());
    add_frame(&mut digest, b"runledger-postgres:pipeline-fingerprint");
    add_frame(
        &mut digest,
        &runledger_postgres::migration_bundle().pipeline_fingerprint(),
    );
    // Include each additional library's helper identity and migration inputs
    // here, in the host's chosen order, before the host migration inputs.
    for migration in host.iter() {
        add_frame(&mut digest, b"application:migration");
        add_frame(&mut digest, &migration.version.to_be_bytes());
        add_frame(&mut digest, migration.description.as_bytes());
        add_frame(&mut digest, migration.migration_type.suffix().as_bytes());
        add_frame(&mut digest, &migration.checksum);
        add_frame(&mut digest, &[u8::from(migration.no_tx)]);
    }
    digest.finalize().into()
}

fn main() {
    // In a host application: use its existing sqlx::migrate!("./migrations").
    let host = Migrator::DEFAULT;
    let fingerprint = template_identity("my-application:migration-pipeline:v1", &host);
    println!("composed template identity: {fingerprint:02x?}");
}
