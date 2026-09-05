use postgres_test_harness::{FingerprintBuilder, TemplateFingerprint};
use runledger_postgres::{RUNLEDGER_POSTGRES_VERSION, migration_bundle};
use sqlx::migrate::{Migration, MigrationType};

const HISTORICAL_VERSIONS: [i64; 5] = [
    202603280001,
    202604100001,
    202605180001,
    202605220001,
    202606030001,
];

// The exact adapter shape used by IdentityPro, with small host/Runlimit inputs.
// The real FingerprintBuilder is used, without starting its database harness.
fn add_migrator_inputs(
    mut builder: FingerprintBuilder,
    namespace: &str,
    migrations: &[Migration],
) -> FingerprintBuilder {
    for migration in migrations {
        builder = builder
            .add(
                format!("{namespace}:version"),
                migration.version.to_be_bytes(),
            )
            .add(
                format!("{namespace}:description"),
                migration.description.as_bytes(),
            )
            .add(
                format!("{namespace}:type"),
                migration.migration_type.suffix(),
            )
            .add(format!("{namespace}:checksum"), &migration.checksum)
            .add(format!("{namespace}:no_tx"), [u8::from(migration.no_tx)]);
    }
    builder
}

fn composed_identity(
    host_domain: &str,
    owner_order: &[&str],
    host_sql: &'static str,
    runlimit_sql: &'static str,
    runlimit_version: &str,
) -> TemplateFingerprint {
    let mut builder = FingerprintBuilder::new(host_domain);
    for &owner in owner_order {
        builder = match owner {
            "runledger" => builder.add(
                "runledger-postgres:pipeline-fingerprint",
                migration_bundle().pipeline_fingerprint(),
            ),
            "runlimit" => add_migrator_inputs(
                builder.add("runlimit-postgres:pipeline-version", runlimit_version),
                owner,
                &[Migration::new(
                    1,
                    "limit".into(),
                    MigrationType::Simple,
                    runlimit_sql.into(),
                    false,
                )],
            ),
            "identitypro" => add_migrator_inputs(
                builder,
                owner,
                &[Migration::new(
                    2,
                    "host".into(),
                    MigrationType::Simple,
                    host_sql.into(),
                    false,
                )],
            ),
            _ => panic!("unexpected fixture owner"),
        };
    }
    builder.finish()
}

#[test]
fn identitypro_composition_retains_host_and_other_library_inputs() {
    let order = ["runledger", "runlimit", "identitypro"];
    let fingerprint = composed_identity("host-v16", &order, "SELECT 1", "SELECT 2", "0.3.0");
    assert_eq!(
        fingerprint,
        composed_identity("host-v16", &order, "SELECT 1", "SELECT 2", "0.3.0")
    );
    let changed = [
        composed_identity("host-v17", &order, "SELECT 1", "SELECT 2", "0.3.0"),
        composed_identity("host-v16", &order, "SELECT 3", "SELECT 2", "0.3.0"),
        composed_identity("host-v16", &order, "SELECT 1", "SELECT 3", "0.3.0"),
        composed_identity("host-v16", &order, "SELECT 1", "SELECT 2", "0.3.1"),
        composed_identity(
            "host-v16",
            &["runlimit", "runledger", "identitypro"],
            "SELECT 1",
            "SELECT 2",
            "0.3.0",
        ),
        composed_identity(
            "host-v16",
            &["runlimit", "identitypro"],
            "SELECT 1",
            "SELECT 2",
            "0.3.0",
        ),
    ];
    for candidate in changed {
        assert_ne!(fingerprint, candidate);
    }
    assert_eq!(
        migration_bundle().library_version(),
        RUNLEDGER_POSTGRES_VERSION
    );
    assert_ne!(RUNLEDGER_POSTGRES_VERSION, env!("CARGO_PKG_VERSION"));
}

fn verify_historical_bundle(
    vendored: &[Migration],
    upstream: &[&Migration],
) -> Result<(), &'static str> {
    if vendored
        .iter()
        .map(|entry| entry.version)
        .collect::<Vec<_>>()
        != HISTORICAL_VERSIONS
        || upstream
            .iter()
            .map(|entry| entry.version)
            .collect::<Vec<_>>()
            != HISTORICAL_VERSIONS
    {
        return Err("historical version set differs");
    }
    for (vendored, upstream) in vendored.iter().zip(upstream) {
        if vendored.checksum != upstream.checksum || vendored.sql != upstream.sql {
            return Err("historical SQL or checksum differs");
        }
    }
    Ok(())
}

#[test]
fn hocr_historical_bundle_matches_published_manifest_and_detects_drift() {
    // Independent snapshot copied from HOCR, not read from the Runledger tree.
    let vendored = sqlx::migrate!("tests/fixtures/hocr-runledger-0.5.0");
    let originals: Vec<_> = vendored.iter().cloned().collect();
    // This is the historical prefix check during an upgrade. The current bundle
    // also has newer entries: it must never be described as the entire 0.5 bundle.
    let upstream: Vec<_> = migration_bundle()
        .migrations()
        .filter(|entry| entry.migration_type.is_up_migration() && entry.version <= 202606030001)
        .collect();
    assert!(
        migration_bundle()
            .migrations()
            .any(|entry| entry.version > 202606030001)
    );
    assert_eq!(verify_historical_bundle(&originals, &upstream), Ok(()));

    let mut changed = originals.clone();
    changed[0] = Migration::new(
        changed[0].version,
        changed[0].description.clone(),
        changed[0].migration_type,
        "SELECT 'modified historical SQL'".into(),
        changed[0].no_tx,
    );
    assert_eq!(
        verify_historical_bundle(&changed, &upstream),
        Err("historical SQL or checksum differs")
    );
    changed = originals.clone();
    changed[0].sql = "SELECT 'tampered without updating checksum'".into();
    assert_eq!(
        verify_historical_bundle(&changed, &upstream),
        Err("historical SQL or checksum differs")
    );
    changed = originals.clone();
    changed[0].checksum = vec![0; 48].into();
    assert_eq!(
        verify_historical_bundle(&changed, &upstream),
        Err("historical SQL or checksum differs")
    );
    assert_eq!(
        verify_historical_bundle(&originals[1..], &upstream),
        Err("historical version set differs")
    );
    assert_eq!(
        verify_historical_bundle(&originals, &upstream[1..]),
        Err("historical version set differs")
    );
}
