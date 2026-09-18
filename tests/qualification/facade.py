#!/usr/bin/env python3
"""Qualify facade contracts, regressions and examples without combining DI modes.

Python 3.11+, Rust 1.96.1. All Cargo commands are locked and offline.
Use --live for an additional disposable Docker Compose example run.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import tomllib
import uuid

ROOT = Path(__file__).resolve().parents[2]
CARGO = ["cargo", "+1.96.1"]
STAGES = ("umbrella", "components", "di", "macros", "downstream", "workspace", "examples", "docs")
EXAMPLE_PACKAGES = ("lily-example-models", "lily-example-shared", "lily-example-http",
                    "lily-example-websocket", "lily-example-consumer")


def manifest(path):
    return tomllib.loads((ROOT / path).read_text())


def fixture_members(workspace):
    for member in manifest(workspace)["workspace"]["members"]:
        yield manifest(Path(workspace).parent / member / "Cargo.toml")


class Qualification:
    def __init__(self, stages, live):
        self.env = dict(os.environ)
        self.env.pop("PYTHONOPTIMIZE", None)  # Assertions in existing fixture scripts must remain active.
        target = Path(self.env.get("CARGO_TARGET_DIR", ROOT / "target")).resolve()
        self.env.update(CARGO_TARGET_DIR=str(target), CARGO_INCREMENTAL="0",
                        CARGO_PROFILE_DEV_DEBUG="0", CARGO_PROFILE_TEST_DEBUG="0")
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
        self.directory = target / "facade-qualification" / run_id
        self.directory.mkdir(parents=True)
        self.report = {
            "started_at": datetime.now(timezone.utc).isoformat(),
            "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "dirty_tree": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT)),
            "rustc": subprocess.check_output(["rustc", "+1.96.1", "--version"], text=True).strip(),
            "stages": stages, "live_requested": live, "commands": [], "passed": False,
            "scope": "Facade compatibility and connected examples; not the full V1 service/TLS/fault matrix.",
        }
        self.save()
        print(f"Evidence: {self.directory}", flush=True)

    def save(self):
        (self.directory / "report.json").write_text(json.dumps(self.report, indent=2) + "\n")

    def run(self, label, command, *, env=None, timeout=3600):
        log = self.directory / (label + ".log")
        print(f"RUN {label}", flush=True)
        started = time.monotonic()
        with log.open("w") as handle:
            try:
                result = subprocess.run(command, cwd=ROOT, env=env or self.env,
                                        stdout=handle, stderr=subprocess.STDOUT, timeout=timeout)
                code = result.returncode
            except subprocess.TimeoutExpired:
                code = "timeout"
        content = log.read_text(errors="replace")
        counts = re.findall(r"test result: (?:ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored;", content)
        entry = {"label": label, "command": list(map(str, command)), "exit_code": code,
                 "duration_seconds": round(time.monotonic() - started, 3), "log": log.name,
                 "test_summary": {key: sum(int(row[index]) for row in counts)
                                  for index, key in enumerate(("passed", "failed", "ignored"))}}
        self.report["commands"].append(entry)
        self.save()
        if code != 0:
            raise RuntimeError(f"{label} failed ({code}); {log}\n{content[-7000:]}")
        print(f"PASS {label} ({entry['duration_seconds']}s)", flush=True)
        return content

    def cargo(self, label, *arguments):
        return self.run(label, [*CARGO, *arguments, "--offline", "--locked"])

    def umbrella(self):
        self.run("umbrella", [sys.executable, str(ROOT / "tests/fixtures/umbrella_facades/verify.py")])
        # Retain the feature/negative-diagnostic logs next to this run's report.
        source = Path(self.env["CARGO_TARGET_DIR"]) / "umbrella-validation"
        destination = self.directory / "umbrella"
        destination.mkdir()
        for log in source.glob("*.log"):
            (destination / log.name).write_bytes(log.read_bytes())

    def components(self):
        workspace = "tests/fixtures/component_facades/Cargo.toml"
        for package in fixture_members(workspace):
            name = package["package"]["name"]
            base = ["test", "--manifest-path", workspace, "-p", name]
            self.cargo(name, *base)
            if "factory" in package.get("features", {}):
                self.cargo(name + "-factory", *base, "--no-default-features", "--features", "factory")
            if "asyncapi" in package.get("features", {}):
                self.cargo(name + "-asyncapi", *base, "--features", "asyncapi")

    def di(self):
        workspace = "tests/fixtures/di_facades/Cargo.toml"
        for package in fixture_members(workspace):
            name = package["package"]["name"]
            self.cargo(name, "test", "--manifest-path", workspace, "-p", name)

    def macros(self):
        self.run("package-dependencies", [sys.executable, str(ROOT / "tests/qualification/package_dependencies.py")])
        workspace = "tests/fixtures/macro_contracts/Cargo.toml"
        for package in fixture_members(workspace):
            name = package["package"]["name"]
            base = ["test", "--manifest-path", workspace, "-p", name]
            self.cargo(name, *base)
            if "factory" in package.get("features", {}):
                self.cargo(name + "-factory", *base, "--no-default-features", "--features", "factory")
        # Default proc-macro unit tests remain in the workspace stage. These
        # feature-specific unit tests exercise their additional code paths.
        self.cargo("macro-mongodb-factory", "test", "-p", "lily_mongodb_derive", "--no-default-features", "--features", "factory")
        self.cargo("macro-queue-asyncapi", "test", "-p", "lily_queue_derive", "--features", "asyncapi")
        self.cargo("macro-injection-example", "run", "--manifest-path", workspace,
                   "-p", "lily-macro-contracts-injection", "--example", "simple_centralized_api")

    def downstream(self):
        for fixture in ("downstream_injectable", "downstream_http", "downstream_struct_controller",
                        "downstream_consumer", "downstream_websocket_client", "mongodb_derive_contract",
                        "postgresql_entity_contract", "clickhouse_derive_contract"):
            path = f"tests/fixtures/{fixture}/Cargo.toml"
            self.cargo(fixture, "test", "--manifest-path", path)
            features = manifest(path).get("features", {})
            if "factory" in features:
                self.cargo(fixture + "-factory", "test", "--manifest-path", path,
                           "--no-default-features", "--features", "factory")
            if "mongodb-adapter" in features:
                self.cargo(fixture + "-mongodb", "test", "--manifest-path", path, "--features", "mongodb-adapter")
        self.cargo("standalone-di-run", "run", "--manifest-path", "tests/fixtures/downstream_injectable/Cargo.toml")
        golden = "tests/qualification/golden/Cargo.toml"
        for package in fixture_members(golden):
            name = package["package"]["name"]
            self.cargo(name, "check", "--manifest-path", golden, "-p", name, "--all-targets")

    def workspace(self):
        # Selecting every member in one Cargo invocation unifies their defaults:
        # e.g. queue-client/single registers a publisher in Consumer's deliberately
        # transport-free test container. Test each member's own default graph.
        # Each invocation includes unit, integration, UI and documentation tests.
        packages = [package["package"]["name"] for package in fixture_members("Cargo.toml")]
        self.report["workspace_packages"] = packages
        self.save()
        for name in packages:
            self.cargo("workspace-" + name, "test", "-p", name)

    def examples(self):
        workspace = "examples/Cargo.toml"
        # A future direct component/derive dependency must not weaken facade examples.
        packages = [*fixture_members(workspace), manifest("examples/clients/Cargo.toml")]
        for package in packages:
            for key, dependency in package.get("dependencies", {}).items():
                actual = dependency.get("package", key) if isinstance(dependency, dict) else key
                if actual.startswith("lily_"):
                    raise RuntimeError(f"{package['package']['name']} bypasses the facade with {actual}")
        self.cargo("example-tests", "test", "--manifest-path", workspace, "--workspace")
        for package in EXAMPLE_PACKAGES:
            self.cargo(package, "check", "--manifest-path", workspace, "-p", package, "--all-targets")
        client = "examples/clients/Cargo.toml"
        self.cargo("client-tests", "test", "--manifest-path", client)
        self.cargo("client-check", "check", "--manifest-path", client, "--all-targets")
        # Do not pass fmt --all: it would format local dependency crates as well.
        selected = [argument for package in EXAMPLE_PACKAGES for argument in ("-p", package)]
        self.run("example-format", [*CARGO, "fmt", "--manifest-path", workspace, *selected, "--check"])
        self.run("client-format", [*CARGO, "fmt", "--manifest-path", client, "--check"])

    def docs(self):
        self.cargo("workspace-docs", "doc", "--workspace", "--no-deps")
        # Root-workspace defaults do not enable any public umbrella component.
        for mode in ("single", "factory"):
            features = ["consumer-asyncapi", "http-api", "websocket", "trace", "config", "injection",
                        "http-client", "error", "background-service", "cancellation", "websocket-redis"]
            features += [name + "-" + mode for name in ("mongodb", "postgresql", "clickhouse", "redis", "queue-client", "websocket-client")]
            self.cargo("facade-docs-" + mode, "doc", "-p", "lilyrs", "--no-deps", "--no-default-features", "--features", ",".join(features))

    def live(self):
        project = "lily-facade-q-" + uuid.uuid4().hex[:12]
        environment = dict(self.env, COMPOSE_PROJECT_NAME=project, EXAMPLE_IMAGE=project + ":local")
        for service in ("HTTP", "WS", "POSTGRES", "MONGO", "REDIS", "AMQP", "RABBIT_MANAGEMENT"):
            environment[f"EXAMPLE_{service}_PORT"] = "0"
        compose = ["docker", "compose", "-f", str(ROOT / "examples/compose.yaml")]
        # Fail before acquiring ownership if this randomly chosen project already exists.
        existing = subprocess.check_output([*compose, "ps", "-a", "-q"], cwd=ROOT, env=environment, text=True)
        if existing.strip():
            raise RuntimeError(f"Compose project {project} already exists; refusing to reuse it")
        self.report["compose_project"] = project
        self.save()
        try:
            config = self.run("compose-config", [*compose, "config", "--format", "json"], env=environment)
            self.report["compose_config_sha256"] = hashlib.sha256(config.encode()).hexdigest()
            self.run("compose-build", [*compose, "build", "http-api"], env=environment)
            self.run("compose-up", [*compose, "up", "-d", "--no-build", "--wait", "--wait-timeout", "120"], env=environment, timeout=180)
            ids = self.run("compose-images", [*compose, "images", "-q"], env=environment)
            self.run("image-identities", ["docker", "image", "inspect", *sorted(set(ids.splitlines())), "--format", "{{.Id}} {{json .RepoDigests}}"], env=environment)
            verified = self.run("live-examples", [sys.executable, str(ROOT / "examples/verify.py")], env=environment, timeout=240)
            artifact = next(Path(line.removeprefix("Artifacts: ")) for line in verified.splitlines() if line.startswith("Artifacts: "))
            result = json.loads((artifact / "result.json").read_text())
            if result.get("passed") is not True:
                raise RuntimeError("Example verifier did not report a successful result")
            self.report["live_evidence"] = str(artifact)
            self.report["live_result"] = result
            self.save()
        finally:
            # Only resources of the newly created, randomly named project are removed.
            self.run("compose-down", [*compose, "down", "--volumes"], env=environment, timeout=120)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", action="append", choices=STAGES, help="Repeat to select stages; defaults to all offline stages")
    parser.add_argument("--live", action="store_true", help="Also build and verify an isolated Compose example stack")
    args = parser.parse_args()
    stages = args.stage or list(STAGES)
    qualification = Qualification(stages, args.live)
    try:
        for stage in stages:
            getattr(qualification, stage)()
        if args.live:
            qualification.live()
        qualification.report["passed"] = True
    finally:
        qualification.report["finished_at"] = datetime.now(timezone.utc).isoformat()
        qualification.save()
    print(f"PASS selected stages; report: {qualification.directory / 'report.json'}")


if __name__ == "__main__":
    main()
