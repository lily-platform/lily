#!/usr/bin/env python3
"""Build and audit real release archives with Cargo 1.96.1; never publish them."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import tarfile
import time
import tomllib
import uuid

from package_dependencies import audit

ROOT = Path(__file__).resolve().parents[2]
CARGO = ["cargo", "+1.96.1"]


def inspect_archive(path, directory, document, expected_names):
    name = document["package"]["name"]
    version = document["package"]["version"]
    prefix = f"{name}-{version}/"
    with tarfile.open(path, "r:gz") as archive:
        files = {}
        for member in archive.getmembers():
            if not member.name.startswith(prefix) or not member.isfile():
                raise ValueError(f"{path.name}: unexpected archive member {member.name}")
            relative = member.name.removeprefix(prefix)
            parts = PurePosixPath(relative).parts
            if ".." in parts or any(p in (".vscode", "wip", "fuzz", "target") for p in parts):
                raise ValueError(f"{path.name}: forbidden archive path {relative}")
            if relative in files:
                raise ValueError(f"{path.name}: duplicate file {relative}")
            files[relative] = archive.extractfile(member).read()
    required = {"Cargo.toml", "Cargo.toml.orig", "Cargo.lock", "README.md",
                "LICENSE-MIT", "LICENSE-APACHE", ".cargo_vcs_info.json"}
    if missing := required - files.keys():
        raise ValueError(f"{path.name}: missing {sorted(missing)}")
    for filename in ("README.md", "LICENSE-MIT", "LICENSE-APACHE"):
        if files[filename] != (directory / filename).read_bytes():
            raise ValueError(f"{path.name}: {filename} differs from the source")
    if files["Cargo.toml.orig"] != (directory / "Cargo.toml").read_bytes():
        raise ValueError(f"{path.name}: stale original manifest")
    generated = {"Cargo.toml", "Cargo.toml.orig", "Cargo.lock", ".cargo_vcs_info.json"}
    for filename in files.keys() - generated:
        source = directory / filename
        if not source.is_file() or files[filename] != source.read_bytes():
            raise ValueError(f"{path.name}: archived source differs: {filename}")
    normalized = tomllib.loads(files["Cargo.toml"].decode())
    package = normalized["package"]
    for key, value in {"name": name, "version": "0.1.0", "edition": "2024",
                       "rust-version": "1.96.1", "readme": "README.md",
                       "license": "MIT OR Apache-2.0",
                       "repository": "https://github.com/lily-platform/lilyrs",
                       "homepage": "https://lilyrs.com"}.items():
        if package.get(key) != value:
            raise ValueError(f"{path.name}: unexpected normalized package.{key}")
    if any(key in normalized for key in ("workspace", "patch", "replace")):
        raise ValueError(f"{path.name}: workspace overrides leaked into the archive")
    if normalized.get("features", {}) != document.get("features", {}):
        raise ValueError(f"{path.name}: feature definitions changed during packaging")
    dependencies = []
    for table in [normalized, *normalized.get("target", {}).values()]:
        for kind in ("dependencies", "build-dependencies", "dev-dependencies"):
            for alias, dependency in table.get(kind, {}).items():
                if any(key in dependency for key in ("path", "git", "workspace")):
                    raise ValueError(f"{path.name}: {kind}.{alias} is not a registry dependency")
                actual = dependency.get("package", alias)
                if actual in expected_names:
                    if dependency.get("version") not in ("0.1.0", "=0.1.0"):
                        raise ValueError(f"{path.name}: invalid internal version for {actual}")
                    if dependency.get("registry") or dependency.get("registry-index"):
                        raise ValueError(f"{path.name}: internal dependency redirects {actual}")
                    dependencies.append(actual)
    lock = tomllib.loads(files["Cargo.lock"].decode())
    for dependency in lock["package"]:
        if dependency["name"] in expected_names and dependency["name"] != name:
            if dependency.get("source") != "registry+https://github.com/rust-lang/crates.io-index":
                raise ValueError(f"{path.name}: lockfile retains a local internal dependency")
    return {"name": name, "version": version, "file_count": len(files),
            "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
            "bytes": path.stat().st_size, "internal_dependencies": sorted(set(dependencies)),
            "vcs": json.loads(files[".cargo_vcs_info.json"])}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", action="append", choices=("default", "documentation"),
                        help="Repeat to select profiles; defaults to both")
    args = parser.parse_args()
    profiles = args.profile or ["default", "documentation"]
    workspace = tomllib.loads((ROOT / "Cargo.toml").read_text())
    packages = {}
    for member in workspace["workspace"]["members"]:
        directory = ROOT / member
        document = tomllib.loads((directory / "Cargo.toml").read_text())
        if document["package"].get("publish") is not False:
            packages[document["package"]["name"]] = (directory, document)
    target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")).resolve()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    evidence = target / "release-archives" / run_id
    evidence.mkdir(parents=True)
    env = dict(os.environ, CARGO_TARGET_DIR=str(target), CARGO_INCREMENTAL="0",
               CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0", CARGO_TERM_COLOR="never")
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "passed": False,
              "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT)),
              "rustc": subprocess.check_output(["rustc", "+1.96.1", "--version"], text=True).strip(),
              "profiles": [], "commands": [], "publication_graph": audit(ROOT)}

    def save():
        (evidence / "report.json").write_text(json.dumps(report, indent=2) + "\n")

    print(f"Evidence: {evidence}", flush=True)
    save()
    try:
        for profile in profiles:
            names = set(packages)
            command = [*CARGO, "package", "--workspace", "--locked", "--offline", "--allow-dirty"]
            if profile == "documentation":
                # lily_log uses the default ClickHouse singleton. It is already
                # built in the default profile and cannot join the factory union.
                names.remove("lily_log")
                command += ["--exclude", "lily_log", "--no-default-features"]
                features = [f"{name}/{feature}" for name in sorted(names)
                            for feature in packages[name][1]["package"]["metadata"]["docs"]["rs"]["features"]]
                command += ["--features", ",".join(features)]
            print(f"RUN {profile}: Cargo packages and verifies {len(names)} archives", flush=True)
            log = evidence / f"{profile}.log"
            started = time.monotonic()
            with log.open("w") as handle:
                completed = subprocess.run(command, cwd=ROOT, env=env, stdout=handle,
                                           stderr=subprocess.STDOUT, timeout=3600)
            report["commands"].append({"command": command, "exit_code": completed.returncode,
                                       "seconds": round(time.monotonic() - started, 3), "log": log.name})
            save()
            if completed.returncode:
                raise ValueError(f"{profile}: Cargo verification failed; see {log}")
            output = log.read_text()
            verified = re.findall(r"^\s*Verifying (\S+) v0\.1\.0(?:\s|$)", output, re.M)
            if sorted(verified) != sorted(names):
                raise ValueError(f"{profile}: Cargo did not verify exactly the selected archives")
            if re.search(r"is yanked in registry", output):
                raise ValueError(f"{profile}: packaged lockfile still selects a yanked dependency")
            destination = evidence / profile
            destination.mkdir()
            entry = {"name": profile, "packages": [], "warnings":
                     [line for line in output.splitlines() if line.startswith("warning:")]}
            for name in sorted(names):
                directory, document = packages[name]
                archive = target / "package" / f"{name}-{document['package']['version']}.crate"
                entry["packages"].append(inspect_archive(archive, directory, document, packages))
                shutil.copyfile(archive, destination / archive.name)
            report["profiles"].append(entry)
            save()
            print(f"PASS {profile}: {len(names)} built, normalized manifests and source bytes verified", flush=True)
        report["passed"] = True
        return 0
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        report["error"] = str(error)
        print(f"FAIL {error}", file=sys.stderr)
        return 1
    finally:
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        save()
        print(f"Report: {evidence / 'report.json'}", flush=True)


if __name__ == "__main__":
    sys.exit(main())
