//! These fixtures mutate disposable clones, never either development stream.
#[path = "../build.rs"]
#[allow(
    dead_code,
    reason = "exercise build-time migration and source checks without running main"
)]
mod build_guard;
use build_guard::foundation_source;

use std::{fs, path::PathBuf, process::Command};

const PIN: &str = include_str!("../batter-revision");

struct Fixture {
    _temp: tempfile::TempDir,
    root: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let temp = tempfile::tempdir().expect("source-pin fixture operation");
        let root = temp.path().join("batter");
        let source = std::env::var_os("RUNLEDGER_BATTER_SOURCE")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../batter"));
        assert!(
            Command::new("git")
                .args(["clone", "--quiet", "--shared"])
                .arg(source)
                .arg(&root)
                .status()
                .expect("source-pin fixture operation")
                .success()
        );
        assert!(
            Command::new("git")
                .arg("-C")
                .arg(&root)
                .args(["checkout", "--quiet", "--detach", PIN.trim()])
                .status()
                .expect("source-pin fixture operation")
                .success()
        );
        Self { _temp: temp, root }
    }

    fn check(&self) -> Result<(), Box<dyn std::error::Error>> {
        foundation_source::verify(
            &self.root.join("crates/batter-sqlx"),
            &self.root.join("crates/batter-core"),
            PIN.trim(),
        )
    }

    fn change(&self, path: &str, from: &str, to: &str) {
        let path = self.root.join(path);
        let original = fs::read_to_string(&path).expect("source-pin fixture operation");
        assert!(original.contains(from));
        fs::write(path, original.replace(from, to)).expect("source-pin fixture operation");
    }
}

#[test]
fn companion_pin_and_formatting_changes_do_not_change_foundation() {
    let fixture = Fixture::new();
    fixture.check().expect("source-pin fixture operation");
    fixture.change(
        "Cargo.toml",
        "bfc949bbc32fb2cc5731fb743632b2e432d1f5ae",
        &"f".repeat(40),
    );
    fixture.change(
        "Cargo.toml",
        "[workspace]",
        "# adapter-only coordination\n[workspace]",
    );
    fixture.check().expect("source-pin fixture operation");
}

#[test]
fn inherited_dependency_and_package_settings_remain_guarded() {
    for (from, to) in [
        ("1.53.1", "1.53.2"),
        ("edition = \"2024\"", "edition = \"2021\""),
    ] {
        let fixture = Fixture::new();
        fixture.change("Cargo.toml", from, to);
        assert!(
            fixture
                .check()
                .expect_err("fixture drift must be rejected")
                .to_string()
                .contains("manifest inputs differ")
        );
    }
}

#[test]
fn tracked_and_ignored_untracked_foundation_drift_is_rejected() {
    let fixture = Fixture::new();
    fixture.change("crates/batter-core/src/lib.rs", "//!", "//! changed ");
    assert!(
        fixture
            .check()
            .expect_err("fixture drift must be rejected")
            .to_string()
            .contains("foundation differs")
    );
    let fixture = Fixture::new();
    fs::write(
        fixture.root.join("crates/batter-sqlx/unreviewed.rs"),
        "// drift",
    )
    .expect("source-pin fixture operation");
    fs::write(fixture.root.join(".gitignore"), "unreviewed.rs\n")
        .expect("source-pin fixture operation");
    assert!(
        fixture
            .check()
            .expect_err("fixture drift must be rejected")
            .to_string()
            .contains("untracked actual")
    );
}

#[test]
fn clean_sibling_cannot_bless_another_compiled_core() {
    let fixture = Fixture::new();
    let other = Fixture::new();
    let error = foundation_source::verify(
        &fixture.root.join("crates/batter-sqlx"),
        &other.root.join("crates/batter-core"),
        PIN.trim(),
    )
    .expect_err("fixture drift must be rejected");
    assert!(
        error
            .to_string()
            .contains("different Batter foundation source roots")
    );
}

#[test]
fn migration_sync_remains_independent_of_source_selection() {
    let temp = tempfile::tempdir().expect("source-pin fixture operation");
    let canonical = temp.path().join("canonical");
    let vendored = temp.path().join("vendored");
    fs::create_dir(&canonical).expect("source-pin fixture operation");
    fs::create_dir(&vendored).expect("source-pin fixture operation");
    fs::write(canonical.join("001.sql"), "SELECT 1;").expect("source-pin fixture operation");
    fs::write(vendored.join("001.sql"), "SELECT 1;").expect("source-pin fixture operation");
    build_guard::enforce_migration_sync(&canonical, &vendored);
    fs::write(vendored.join("001.sql"), "SELECT 2;").expect("source-pin fixture operation");
    assert!(
        std::panic::catch_unwind(|| build_guard::enforce_migration_sync(&canonical, &vendored))
            .is_err()
    );
}
