use std::borrow::Cow;

use super::*;

fn vector_entries() -> [Migration; 2] {
    [
        Migration {
            version: -7,
            description: Cow::Borrowed("alpha\0β"),
            migration_type: MigrationType::Simple,
            sql: Cow::Borrowed(""),
            checksum: Cow::Borrowed(&[0, 255, 128]),
            no_tx: false,
        },
        Migration {
            version: 42,
            description: Cow::Borrowed("down"),
            migration_type: MigrationType::ReversibleDown,
            sql: Cow::Borrowed(""),
            checksum: Cow::Borrowed(&[1, 2]),
            no_tx: true,
        },
    ]
}

fn hex(bytes: [u8; 32]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[test]
fn encoding_matches_independently_computed_sha256_vectors() {
    // Computed with Python hashlib/struct from the documented wire format,
    // including a negative i64, UTF-8, NUL, binary checksum, and down migration.
    let entries = vector_entries();
    let fingerprint = fingerprint_bundle(&entries.iter().collect::<Vec<_>>());
    assert_eq!(
        hex(fingerprint),
        "e1c5a7c71d47180cf5ac552e5534ddccc95b37a6e324dd673e230d4f00518209"
    );
    assert_eq!(
        hex(fingerprint_pipeline("9.8.7-test", &fingerprint)),
        "3fcc9cbe7a2c75e504fae7e5404b21d5f8c82bcf133bd2a27b2ec36cd9fd0f98"
    );
}

#[test]
fn every_manifest_field_and_entry_affects_bundle_identity() {
    let entries = vector_entries();
    let original = fingerprint_bundle(&[&entries[0], &entries[1]]);
    let mutations: [fn(&mut Migration); 6] = [
        |entry| entry.version += 1,
        |entry| entry.description = Cow::Borrowed("changed"),
        |entry| entry.migration_type = MigrationType::ReversibleUp,
        |entry| entry.migration_type = MigrationType::ReversibleDown,
        |entry| entry.checksum = Cow::Borrowed(&[0, 255, 129]),
        |entry| entry.no_tx = true,
    ];
    for mutate in mutations {
        let mut changed = entries[0].clone();
        mutate(&mut changed);
        assert_ne!(original, fingerprint_bundle(&[&changed, &entries[1]]));
    }
    assert_ne!(original, fingerprint_bundle(&[&entries[0]]));
    assert_ne!(original, fingerprint_bundle(&[]));
    let mut changed_down = entries[1].clone();
    changed_down.checksum = Cow::Borrowed(&[3, 4]);
    assert_ne!(original, fingerprint_bundle(&[&entries[0], &changed_down]));
}

#[test]
fn release_identity_changes_without_sql_changes() {
    let entries = vector_entries();
    let content = fingerprint_bundle(&[&entries[0], &entries[1]]);
    assert_ne!(
        fingerprint_pipeline("0.12.0", &content),
        fingerprint_pipeline("0.12.1", &content)
    );
    assert_eq!(
        fingerprint_pipeline("0.12.0", &content),
        fingerprint_pipeline("0.12.0", &content)
    );
    let changed_content = fingerprint_bundle(&[&entries[0]]);
    assert_ne!(
        fingerprint_pipeline("0.12.0", &content),
        fingerprint_pipeline("0.12.0", &changed_content)
    );
}

#[test]
fn compiled_manifest_has_the_independently_pinned_up_and_down_history() {
    let expected_versions = [
        202603280001,
        202604100001,
        202605180001,
        202605220001,
        202606030001,
        202607190001,
        202607250001,
        202607280001,
        202607280002,
        202607280003,
        202607280004,
        202607280005,
        202608180001,
        202608230001,
        202608240001,
        202608240002,
    ];
    let actual: Vec<_> = migration_bundle()
        .migrations()
        .map(|entry| (entry.version, entry.migration_type))
        .collect();
    let expected: Vec<_> = expected_versions
        .into_iter()
        .flat_map(|version| {
            [
                (version, MigrationType::ReversibleUp),
                (version, MigrationType::ReversibleDown),
            ]
        })
        .collect();
    assert_eq!(actual, expected);
    // Independently calculated from root migration files with Python SHA-384
    // checksums and the documented v1 SHA-256 encoding. Updating the migration
    // history requires reviewing this content snapshot as well as the versions.
    assert_eq!(
        hex(migration_bundle().bundle_fingerprint()),
        "77005335e2e12fbcc96bd95c50d8a9c75b56293a0b91afee05b3a433ec96271c"
    );
    assert_eq!(
        migration_bundle().pipeline_fingerprint(),
        fingerprint_pipeline(
            RUNLEDGER_POSTGRES_VERSION,
            &migration_bundle().bundle_fingerprint()
        )
    );
}
