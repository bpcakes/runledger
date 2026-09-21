//! Enforce the unpublished coordinated source graph for local builds too.
use std::{
    collections::BTreeMap,
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

const PIN: &str = include_str!("batter-revision");
const INPUTS: &[&str] = &["Cargo.toml", "crates/batter-core", "crates/batter-sqlx"];

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed=batter-revision");
    println!("cargo:rerun-if-env-changed=RUNLEDGER_BATTER_SOURCE");
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let vendored_dir = manifest.join("migrations");
    let canonical_dir = manifest.join("../migrations");
    println!("cargo:rerun-if-changed={}", vendored_dir.display());
    if canonical_dir.exists() {
        println!("cargo:rerun-if-changed={}", canonical_dir.display());
        enforce_migration_sync(&canonical_dir, &vendored_dir);
    }
    let source = env::var_os("RUNLEDGER_BATTER_SOURCE")
        .map_or_else(|| manifest.join("../../batter"), PathBuf::from);
    let revision = PIN.trim();
    if revision.len() != 40 || !revision.bytes().all(|c| c.is_ascii_hexdigit()) {
        return Err("invalid immutable Batter source revision".into());
    }
    for input in INPUTS {
        println!("cargo:rerun-if-changed={}", source.join(input).display());
    }
    // Accept companion-only descendants, but require identical foundation code,
    // manifests and untracked-source state. This avoids circular whole-repo pins.
    let mut args = vec!["diff", "--quiet", revision, "--"];
    args.extend_from_slice(INPUTS);
    if !git(&source, &args)?.status.success() {
        return Err("Batter foundation differs from the coordinated pin; run scripts/bootstrap-batter.sh or deliberately update the reviewed pin".into());
    }
    let unknown = git(
        &source,
        &[
            "ls-files",
            "--others",
            "--exclude-standard",
            "--",
            "crates/batter-core",
            "crates/batter-sqlx",
        ],
    )?;
    if !unknown.status.success() || !unknown.stdout.is_empty() {
        return Err(
            "untracked Batter foundation inputs are not part of the pinned source graph".into(),
        );
    }
    Ok(())
}

fn enforce_migration_sync(canonical_dir: &Path, vendored_dir: &Path) {
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

fn git(source: &Path, args: &[&str]) -> Result<std::process::Output, std::io::Error> {
    Command::new("git")
        .arg("-C")
        .arg(source)
        .args(args)
        .output()
}
