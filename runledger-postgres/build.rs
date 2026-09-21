//! Enforce the unpublished coordinated source graph for local builds too.
use std::{
    collections::BTreeMap,
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
};

pub mod foundation_source;

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed=batter-revision");
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let vendored_dir = manifest.join("migrations");
    let canonical_dir = manifest.join("../migrations");
    println!("cargo:rerun-if-changed={}", vendored_dir.display());
    if canonical_dir.exists() {
        println!("cargo:rerun-if-changed={}", canonical_dir.display());
        enforce_migration_sync(&canonical_dir, &vendored_dir);
    }
    // Cargo's direct dependency metadata describes the code actually compiled.
    // An unrelated clean sibling or RUNLEDGER_BATTER_SOURCE cannot bless it.
    let sqlx = PathBuf::from(env::var("DEP_BATTER_SQLX_FOUNDATION_SOURCE")?);
    let core = PathBuf::from(env::var("DEP_BATTER_SQLX_FOUNDATION_CORE_SOURCE")?);
    foundation_source::verify(&sqlx, &core, include_str!("batter-revision").trim())?;
    Ok(())
}

pub fn enforce_migration_sync(canonical_dir: &Path, vendored_dir: &Path) {
    let canonical = load_sql_files(canonical_dir);
    let vendored = load_sql_files(vendored_dir);

    if canonical != vendored {
        panic!(
            "runledger-postgres/migrations is out of sync with the canonical root migrations/ directory; run ./scripts/refresh-sqlx-cache.sh"
        );
    }
}

fn load_sql_files(dir: &Path) -> BTreeMap<String, String> {
    let mut files = BTreeMap::new();

    let entries = fs::read_dir(dir)
        .unwrap_or_else(|err| panic!("read migration directory {}: {err}", dir.display()));
    for entry in entries {
        let entry =
            entry.unwrap_or_else(|err| panic!("read migration entry in {}: {err}", dir.display()));
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("sql") {
            continue;
        }

        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_else(|| panic!("migration file name is not valid UTF-8: {}", path.display()))
            .to_owned();
        let contents = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("read migration file {}: {err}", path.display()));
        files.insert(file_name, contents);
    }

    files
}
