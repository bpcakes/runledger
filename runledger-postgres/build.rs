//! Enforce the unpublished coordinated source graph for local builds too.
use std::{
    env,
    error::Error,
    path::{Path, PathBuf},
    process::Command,
};

const PIN: &str = include_str!("batter-revision");
const INPUTS: &[&str] = &["Cargo.toml", "crates/batter-core", "crates/batter-sqlx"];

fn main() -> Result<(), Box<dyn Error>> {
    println!("cargo:rerun-if-changed=batter-revision");
    println!("cargo:rerun-if-env-changed=RUNLEDGER_BATTER_SOURCE");
    let manifest = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
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

fn git(source: &Path, args: &[&str]) -> Result<std::process::Output, std::io::Error> {
    Command::new("git")
        .arg("-C")
        .arg(source)
        .args(args)
        .output()
}
