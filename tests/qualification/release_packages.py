#!/usr/bin/env python3
"""Audit publishable package contents and build each package's docs.rs profile."""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time
import tomllib
import uuid

ROOT = Path(__file__).resolve().parents[2]
CARGO = ["cargo", "+1.96.1"]
FILE_REFERENCE = re.compile(r'(?:include(?:_str|_bytes)?!\(\s*|#\[path\s*=\s*)"([^"\n]+)"')
DOC_PAGES = {
    "lily_consumer": ["type.ConsumerAsyncApiService.html"],
    "lily_queue": ["struct.PostgresTransaction.html", "struct.MongoTransaction.html"],
    "lily_clickhouse": ["struct.DatabaseService.html", "struct.ClickhouseFactory.html"],
    "lily_mongodb": ["struct.DatabaseService.html", "struct.MongoFactory.html", "derive.CrudService.html"],
    "lily_postgresql": ["struct.PgDatabaseService.html", "struct.PgDbContext.html", "struct.PgFactory.html"],
    "lily_queue_client": ["struct.QueueClientService.html", "struct.QueueClientFactory.html"],
    "lily_redis": ["struct.CacheService.html", "struct.CacheFactory.html"],
    "lily_websocket_client": ["struct.TokioWsClient.html", "struct.WebSocketClientService.html", "struct.WebSocketClientFactory.html"],
}


def manifest(path):
    return tomllib.loads(path.read_text())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docs", action="store_true", help="Build the selected metadata profiles with Rust 1.96.1")
    parser.add_argument("--snapshot", action="store_true", help="Copy only Cargo-listed source files into an isolated workspace for archive-content checks")
    parser.add_argument("--package", action="append", default=[], help="Limit checks to a workspace package; repeatable")
    args = parser.parse_args()
    if args.snapshot and args.package:
        parser.error("--snapshot needs the complete workspace; omit --package")
    workspace = manifest(ROOT / "Cargo.toml")
    packages = {}
    for member in workspace["workspace"]["members"]:
        directory = ROOT / member
        document = manifest(directory / "Cargo.toml")
        if document["package"].get("publish") is not False:
            packages[document["package"]["name"]] = (directory, document)
    unknown = set(args.package) - packages.keys()
    if unknown:
        parser.error("unknown packages: " + ", ".join(sorted(unknown)))
    selected = sorted(set(args.package) or packages)
    target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")).resolve()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    evidence = target / "release-packages" / run_id
    evidence.mkdir(parents=True)
    environment = dict(os.environ, CARGO_TARGET_DIR=str(target), CARGO_INCREMENTAL="0",
                       CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0")
    report = {"commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT)),
              "packages": [], "commands": [], "passed": False}

    def save():
        (evidence / "report.json").write_text(json.dumps(report, indent=2) + "\n")

    def run(label, command, env=None):
        started = time.monotonic()
        output = subprocess.run(command, cwd=ROOT, env=env or environment, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        (evidence / (label + ".log")).write_text(output.stdout)
        report["commands"].append({"label": label, "command": command, "exit_code": output.returncode,
                                   "seconds": round(time.monotonic() - started, 3)})
        save()
        if output.returncode:
            raise ValueError(f"{label} failed; see {evidence / (label + '.log')}")
        return output.stdout

    try:
        rustc = run("rustc", ["rustc", "+1.96.1", "-vV"])
        host = re.search(r"^host: (.+)$", rustc, re.M)[1]
        report["rustc"] = rustc
        snapshot = evidence / "package-source"
        if args.snapshot:
            snapshot.mkdir()
            for filename in ("Cargo.toml", "Cargo.lock"):
                shutil.copyfile(ROOT / filename, snapshot / filename)
            report["source_snapshot"] = str(snapshot)

        for name in selected:
            directory, document = packages[name]
            package = document["package"]
            metadata = package.get("metadata", {}).get("docs", {}).get("rs")
            if not isinstance(metadata, dict):
                raise ValueError(f"{name}: missing docs.rs profile")
            features = metadata.get("features", [])
            if metadata.get("all-features", False) or metadata.get("no-default-features") is not True:
                raise ValueError(f"{name}: use explicit documentation features without defaults or --all-features")
            if not isinstance(features, list) or len(features) != len(set(features)):
                raise ValueError(f"{name}: invalid documentation feature list")
            if set(features) - document.get("features", {}).keys():
                raise ValueError(f"{name}: unknown documentation feature")
            if any("fuzzing" in f or "test-support" in f for f in features):
                raise ValueError(f"{name}: test implementation APIs must not select the documentation profile")
            for feature in features:
                counterpart = feature.removesuffix("-factory") if feature.endswith("-factory") else None
                if (feature == "factory" and "single" in features) or (counterpart and counterpart in features):
                    raise ValueError(f"{name}: conflicting documentation features")
            doc_target = metadata.get("default-target")
            if doc_target != "x86_64-unknown-linux-gnu" or metadata.get("targets") != []:
                raise ValueError(f"{name}: documentation must select the qualified Linux target only")

            # Quiet output is strictly the archive's filenames, not Cargo diagnostics.
            files = set(run(name + "-files", [*CARGO, "package", "-p", name, "--list", "--quiet",
                                               "--allow-dirty", "--locked", "--offline"]).splitlines())
            required = {"README.md", "LICENSE-MIT", "LICENSE-APACHE", "Cargo.toml"}
            if package.get("readme") != "README.md" or required - files:
                raise ValueError(f"{name}: incomplete package documentation/license files")
            for filename in ("LICENSE-MIT", "LICENSE-APACHE"):
                if (directory / filename).read_bytes() != (ROOT / filename).read_bytes():
                    raise ValueError(f"{name}: {filename} differs from the workspace license")
            for filename in files:
                if any(part in (".vscode", "wip", "fuzz", "target") for part in Path(filename).parts):
                    raise ValueError(f"{name}: development artifact is packaged: {filename}")
            for filename in sorted(files):
                source = directory / filename
                if source.suffix != ".rs" or not source.is_file():
                    continue
                for match in FILE_REFERENCE.finditer(source.read_text()):
                    referenced = (source.parent / match[1]).resolve()
                    # Inline Rust modules can change #[path] resolution. Cargo/rustc
                    # checks those; here we validate literal paths resolved on disk.
                    if not referenced.is_file():
                        continue
                    if not referenced.is_relative_to(directory) or str(referenced.relative_to(directory)) not in files:
                        raise ValueError(f"{name}: {filename} references an unpackaged file: {match[1]}")
            if args.snapshot:
                for filename in sorted(files):
                    source = directory / filename
                    if source.is_file():
                        copied = snapshot / directory.relative_to(ROOT) / filename
                        copied.parent.mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(source, copied)
            entry = {"name": name, "features": features, "file_count": len(files),
                     "file_list_sha256": hashlib.sha256("\n".join(sorted(files)).encode()).hexdigest()}
            report["packages"].append(entry)
            if args.docs:
                command = [*CARGO, "doc", "-p", name, "--lib", "--no-deps", "--no-default-features", "--locked", "--offline"]
                if features:
                    command += ["--features", ",".join(features)]
                if doc_target != host:
                    command += ["--target", doc_target]
                flags = environment.get("RUSTDOCFLAGS", "") + " --cfg docsrs -D rustdoc::broken_intra_doc_links"
                doc_environment = dict(environment, RUSTDOCFLAGS=flags.strip(), DOCS_RS="1")
                doc_environment.pop("CARGO_ENCODED_RUSTDOCFLAGS", None)
                run(name + "-docs", command, doc_environment)
                doc_root = target / ("doc" if doc_target == host else doc_target + "/doc") / name
                for page in ["index.html", *DOC_PAGES.get(name, [])]:
                    if not (doc_root / page).is_file():
                        raise ValueError(f"{name}: expected public API page was not generated: {page}")
                    if page != "index.html" and page not in (doc_root / "index.html").read_text():
                        raise ValueError(f"{name}: expected API is absent from the current crate index: {page}")
                entry["documentation_verified"] = True
            print(f"PASS {name}: package contents" + (", docs.rs profile" if args.docs else ""), flush=True)
            save()

        # WebSocket carries a package-local copy so its released tests do not
        # depend on the source checkout of another crate. Keep the helper exact.
        for filename in ("collector.rs", "collector.yaml"):
            left = ROOT / "crates/framework/lily_websocket/tests/support" / filename
            right = ROOT / "crates/integrations/lily_trace/tests/support" / filename
            if left.read_bytes() != right.read_bytes():
                raise ValueError(f"shared Collector helper drift: {filename}; update both copies")
        report["passed"] = True
        save()
        print(f"PASS {len(selected)} packages; report: {evidence / 'report.json'}", flush=True)
        return 0
    except (ValueError, OSError, KeyError, tomllib.TOMLDecodeError) as error:
        report["error"] = str(error)
        save()
        print(f"FAIL {error}\nReport: {evidence / 'report.json'}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
