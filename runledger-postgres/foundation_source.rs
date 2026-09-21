//! Build-time checks for the actual unpublished Cargo foundation.
use std::{collections::BTreeSet, error::Error, fs, path::Path, process::Command};

const INPUTS: &[&str] = &["crates/batter-core", "crates/batter-sqlx"];

pub fn verify(sqlx: &Path, core: &Path, revision: &str) -> Result<(), Box<dyn Error>> {
    if revision.len() != 40 || !revision.bytes().all(|c| c.is_ascii_hexdigit()) {
        return Err("invalid immutable Batter source revision".into());
    }
    let sqlx = sqlx.canonicalize()?;
    let core = core.canonicalize()?;
    let root = sqlx
        .parent()
        .and_then(Path::parent)
        .ok_or("missing Batter workspace")?;
    if root.join(INPUTS[0]).canonicalize()? != core || root.join(INPUTS[1]).canonicalize()? != sqlx
    {
        return Err("Cargo selected different Batter foundation source roots".into());
    }
    for input in INPUTS.iter().copied().chain(["Cargo.toml"]) {
        println!("cargo:rerun-if-changed={}", root.join(input).display());
    }
    let mut args = vec!["diff", "--quiet", revision, "--"];
    args.extend_from_slice(INPUTS);
    if !git(root, &args)?.status.success() {
        return Err("actual Batter foundation differs from the coordinated pin".into());
    }
    let mut args = vec!["ls-files", "--others", "--"];
    args.extend_from_slice(INPUTS);
    let unknown = git(root, &args)?;
    if !unknown.status.success() || !unknown.stdout.is_empty() {
        return Err("untracked actual Batter foundation inputs are not reviewed".into());
    }
    let expected = git(root, &["show", &format!("{revision}:Cargo.toml")])?;
    if !expected.status.success() {
        return Err("reviewed Batter revision is unavailable in the actual source checkout".into());
    }
    let expected: toml::Table = String::from_utf8(expected.stdout)?.parse()?;
    let actual: toml::Table = fs::read_to_string(root.join("Cargo.toml"))?.parse()?;
    let mut inherited = BTreeSet::new();
    for input in INPUTS {
        let manifest: toml::Table =
            fs::read_to_string(root.join(input).join("Cargo.toml"))?.parse()?;
        inherited_dependencies(&manifest, &mut inherited);
    }
    if projection(&actual, &inherited) != projection(&expected, &inherited) {
        return Err(
            "inherited Batter foundation manifest inputs differ from the coordinated pin".into(),
        );
    }
    Ok(())
}

// Include dependencies under target-specific and build/dev tables as well.
fn inherited_dependencies(table: &toml::Table, names: &mut BTreeSet<String>) {
    for (key, value) in table {
        let Some(table) = value.as_table() else {
            continue;
        };
        if matches!(
            key.as_str(),
            "dependencies" | "build-dependencies" | "dev-dependencies"
        ) {
            for (name, dependency) in table {
                if dependency.get("workspace").and_then(toml::Value::as_bool) == Some(true) {
                    names.insert(name.clone());
                }
            }
        } else {
            inherited_dependencies(table, names);
        }
    }
}

fn projection(root: &toml::Table, inherited: &BTreeSet<String>) -> toml::Table {
    let mut result = toml::Table::new();
    if let Some(workspace) = root.get("workspace").and_then(toml::Value::as_table) {
        for key in ["package", "lints", "resolver"] {
            if let Some(value) = workspace.get(key) {
                result.insert(key.into(), value.clone());
            }
        }
        if let Some(dependencies) = workspace
            .get("dependencies")
            .and_then(toml::Value::as_table)
        {
            let selected = dependencies
                .iter()
                .filter(|(name, _)| inherited.contains(*name))
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect();
            result.insert("dependencies".into(), toml::Value::Table(selected));
        }
    }
    result
}

fn git(source: &Path, args: &[&str]) -> Result<std::process::Output, std::io::Error> {
    Command::new("git")
        .arg("-C")
        .arg(source)
        .args(args)
        .output()
}
