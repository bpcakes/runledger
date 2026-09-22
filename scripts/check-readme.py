#!/usr/bin/env python3
"""Check release versions and compiled quick-start sources; --write updates versions."""

import argparse
from pathlib import Path
import re
import sys
import tomllib


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def check(root: Path, write: bool) -> None:
    manifest = tomllib.loads((root / "Cargo.toml").read_text())
    version = manifest["workspace"]["package"]["version"]
    crates = {
        name for name in manifest["workspace"]["dependencies"]
        if name.startswith("runledger-")
    }
    path = root / "README.md"
    readme = path.read_text()
    if write:
        # Only current dependency recommendations and release commands change.
        # Historical upgrade guidance and compatibility examples remain intact.
        installation_start = readme.index("## Installation\n")
        installation_end = readme.index("\n## ", installation_start)
        installation = re.sub(
            r'^(runledger-[\w-]+ = ")[^"]+("\s*)$',
            lambda match: match[1] + version + match[2],
            readme[installation_start:installation_end], flags=re.MULTILINE,
        )
        readme = readme[:installation_start] + installation + readme[installation_end:]
        readme = re.sub(
            r'^(\./scripts/(?:prepare|publish)-release\.sh )\S+$',
            lambda match: match[1] + version,
            readme, flags=re.MULTILINE,
        )

    installation = readme.split("## Installation\n", 1)[1].split("\n## ", 1)[0]
    block = re.search(r"```toml\n(.*?)\n```", installation, re.DOTALL)
    require(block is not None, "missing installation manifest")
    dependencies = tomllib.loads(block[1])
    native = {
        name: value
        for section in ("dependencies", "dev-dependencies")
        for name, value in dependencies.get(section, {}).items()
        if name.startswith("runledger-")
    }
    require(set(native) == crates, "installation must include all development packages")
    revisions = set()
    for name, value in native.items():
        require(
            isinstance(value, dict) and set(value) == {"git", "rev"},
            f"{name} must use an explicit Git URL and immutable revision",
        )
        require(
            value["git"] == "https://github.com/bpcakes/batter.git",
            f"{name} must come from the Batter repository",
        )
        revision = value["rev"]
        require(
            isinstance(revision, str) and re.fullmatch(r"[0-9a-f]{40}", revision),
            f"{name} must pin a full commit SHA",
        )
        revisions.add(revision)
        package = tomllib.loads((root / name / "Cargo.toml").read_text())["package"]
        require(package.get("publish") is False, f"{name} must remain unpublished")
    require(len(revisions) == 1, "Runledger packages must use the same Batter revision")
    for command in ("prepare", "publish"):
        versions = re.findall(
            rf"^\./scripts/{command}-release\.sh (\S+)$", readme, re.MULTILINE,
        )
        require(versions == [version], f"{command}-release command must use {version}")

    if write:
        readme = re.sub(
            r"(<!-- quick-start-source: ([^\n]+) -->\n```rust\n).*?(\n```)",
            lambda match: match[1] + (root / match[2]).read_text().split("\n#[cfg(test)]", 1)[0].rstrip() + match[3],
            readme, flags=re.DOTALL,
        )
    snippets = re.findall(
        r"<!-- quick-start-source: ([^\n]+) -->\n```rust\n(.*?)\n```",
        readme, re.DOTALL,
    )
    expected = {
        f"runledger-runtime/examples/producer_worker/{name}.rs"
        for name in ("shared", "producer", "worker")
    }
    expected.add("runledger-runtime/examples/support/database.rs")
    require(
        len(snippets) == len(expected) and {p for p, _ in snippets} == expected,
        "quick start must include shared, producer, and worker source blocks"
    )
    for source, snippet in snippets:
        code = (root / source).read_text().split("\n#[cfg(test)]", 1)[0].rstrip()
        require(snippet == code, f"quick-start snippet differs from {source}")

    if write:
        path.write_text(readme)
    print(f"README versions ({version}) and quick-start sources verified.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()
    try:
        check(Path(__file__).resolve().parent.parent, args.write)
    except (KeyError, IndexError, ValueError) as error:
        sys.exit(f"error: {error}")
