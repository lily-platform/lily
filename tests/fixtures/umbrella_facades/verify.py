#!/usr/bin/env python3
"""Verify umbrella consumers and Cargo feature contracts without live services."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tomllib

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
CARGO = ["cargo", "+1.96.1"]
HOST = next(line.removeprefix("host: ") for line in subprocess.check_output(["rustc", "+1.96.1", "-vV"], text=True).splitlines() if line.startswith("host: "))
ENV = dict(os.environ)
ENV.setdefault("CARGO_TARGET_DIR", str(ROOT / "target"))
ENV.setdefault("CARGO_INCREMENTAL", "0")
ENV.setdefault("CARGO_PROFILE_TEST_DEBUG", "0")
LOGS = Path(ENV["CARGO_TARGET_DIR"]) / "umbrella-validation"


def run(arguments, label, expected_error=None, metadata=False):
    completed = subprocess.run(CARGO + arguments + ["--offline", "--locked"],
                               cwd=ROOT, env=ENV, text=True, capture_output=True)
    LOGS.mkdir(parents=True, exist_ok=True)
    (LOGS / (label + ".log")).write_text(completed.stdout + completed.stderr)
    if expected_error is None:
        assert completed.returncode == 0, label + "\n" + completed.stdout[-6000:] + completed.stderr[-12000:]
    else:
        assert completed.returncode != 0, label + " unexpectedly compiled"
        diagnostics = []
        for line in completed.stdout.splitlines():
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if value.get("reason") == "compiler-message" and value["message"]["level"] == "error":
                diagnostics.append(value["message"])
        assert any(expected_error in d["message"] for d in diagnostics), (
            label + " failed for an unexpected reason\n" + completed.stderr[-8000:]
            + "\n" + repr(diagnostics)
        )
    if metadata:
        return json.loads(completed.stdout)
    print("PASS", label, flush=True)


def validate_graph():
    facade = tomllib.loads((ROOT / "crates/lilyrs/Cargo.toml").read_text())
    public = {f for f in facade["features"] if f != "default" and not f.startswith("__")}
    # These expectations come from each component manifest, not from the forwarding
    # values being tested. A newly added child feature must get an umbrella mapping.
    expected = {}
    for package, dependency in facade["dependencies"].items():
        component = package.removeprefix("lily_").replace("_", "-")
        native = tomllib.loads((ROOT / "crates/lilyrs" / dependency["path"] / "Cargo.toml").read_text())
        defaults = set(native.get("features", {}).get("default", []))
        expected[component] = (package, defaults)
        for feature in native.get("features", {}):
            if feature != "default":
                expected[component + "-" + feature] = (package, {feature})
    assert public == set(expected), (public - set(expected), set(expected) - public)
    matrix = HERE / "matrix/Cargo.toml"
    assert set(tomllib.loads(matrix.read_text())["features"]) - {"default"} == public
    for feature in [None, *sorted(public)]:
        args = ["metadata", "--format-version=1", "--filter-platform", HOST, "--manifest-path", str(matrix), "--no-default-features"]
        if feature:
            args += ["--features", feature]
        graph = run(args, "graph-" + (feature or "empty"), metadata=True)
        packages = {p["id"]: p for p in graph["packages"]}
        nodes = {packages[n["id"]]["name"]: n for n in graph["resolve"]["nodes"]}
        if feature is None:
            assert nodes["lilyrs"]["deps"] == [], "empty facade pulls optional dependencies"
            assert set(nodes) == {"umbrella-feature-matrix", "lilyrs"}
            continue
        if feature == "cancellation":
            assert {name for name in nodes if name.startswith("lily_")} == {"lily_cancellation"}
        if feature == "websocket-redis":
            assert set(nodes["lilyrs"]["features"]) == {"websocket-redis"}
            assert {dep["name"] for dep in nodes["lilyrs"]["deps"]} == {"lily_websocket_redis"}
            assert "lily_websocket" in nodes
            assert "lily_redis" not in nodes, "backplane must not activate cache DI"
        package, required = expected[feature]
        assert package in nodes, (feature, package)
        selected = set(nodes[package]["features"])
        assert required <= selected, (feature, required, selected)
        for name, node in nodes.items():
            if name.startswith("lily_"):
                assert not {"single", "factory"} <= set(node["features"]), (feature, name)
        # Low-level helper features must not introduce a DI composition mode.
        if feature.endswith(("-factory-api", "-test-support", "-di")):
            assert not {"single", "factory"} & selected, (feature, selected)
        if feature.startswith("queue-transactional-inbox-"):
            db = "lily_mongodb" if "mongodb" in feature else "lily_postgresql"
            assert not {"single", "factory"} & set(nodes[db]["features"]), feature
        if feature.startswith("consumer-transactional-inbox-"):
            db = "lily_mongodb" if "mongodb" in feature else "lily_postgresql"
            mode = "factory" if feature.endswith("-factory") else "single"
            assert mode in nodes[db]["features"], feature
        if feature in ("config-transactional-inbox-mongodb", "config-transactional-inbox-postgresql"):
            assert "lily_mongodb" not in nodes and "lily_postgresql" not in nodes
    print(f"PASS feature graphs: empty facade and {len(public)} public features", flush=True)


def validate_consumers():
    workspace = tomllib.loads((HERE / "Cargo.toml").read_text())
    for member in workspace["workspace"]["members"]:
        package = tomllib.loads((HERE / member / "Cargo.toml").read_text())
        lily_dependencies = [(name, value) for name, value in package["dependencies"].items()
                             if name.startswith("lily") or isinstance(value, dict) and value.get("package", "").startswith("lily")]
        assert len(lily_dependencies) == 1 and lily_dependencies[0][1]["package"] == "lilyrs", member
        command = ["test", "--lib", "--manifest-path", str(HERE / "Cargo.toml"), "-p", package["package"]["name"]]
        # Separate invocations are intentional. Building the whole workspace at
        # once could hide a missing feature behind another consumer's selection.
        run(command, member)
        if "factory" in package.get("features", {}):
            run(command + ["--no-default-features", "--features", "factory"], member + "-factory")
        if member.startswith("integrations"):
            run(command + ["--features", "asyncapi"], member + "-asyncapi")
        if member.startswith("consumer"):
            run(command + ["--features", "asyncapi"], member + "-asyncapi")


def validate_builds():
    matrix = HERE / "matrix/Cargo.toml"
    features = tomllib.loads(matrix.read_text())["features"]
    base = ["check", "--lib", "--manifest-path", str(matrix), "--no-default-features"]
    # Metadata alone cannot catch a cfg-gated source file that no longer compiles.
    run(base, "build-empty")
    for feature in features:
        if feature != "default":
            run(base + ["--features", feature], "build-" + feature)
    for mode in ("single", "factory"):
        combined = ["consumer-asyncapi", "http-api", "websocket", "trace", "config", "injection",
                    "http-client", "error", "background-service", "cancellation", "websocket-redis"]
        combined += [c + "-" + mode for c in ("mongodb", "postgresql", "clickhouse", "redis", "queue-client", "websocket-client")]
        run(base + ["--features", ",".join(combined)], "build-combined-" + mode)


def validate_rejections():
    base = ["check", "--manifest-path", str(HERE / "matrix/Cargo.toml"), "--no-default-features", "--message-format=json"]
    run(base + ["--bin", "missing_injection"], "missing-feature", "unresolved import `lilyrs::injection`")
    run(base + ["--bin", "missing_injection", "--features", "config"], "config-does-not-export-di", "unresolved import `lilyrs::injection`")
    run(base + ["--bin", "ws_lifecycle_payload", "--features", "websocket"], "websocket-lifecycle-payload", "WebSocket message-only extractor `TextPayload` cannot be used")
    run(base + ["--bin", "queue_asyncapi_disabled", "--features", "queue"], "queue-asyncapi-disabled", "queue AsyncAPI metadata requires enabling")
    for component in ("mongodb", "postgresql", "clickhouse", "redis", "queue-client", "websocket-client"):
        run(base + ["--lib", "--features", component + "," + component + "-factory"],
            "conflict-" + component, "features `single` and `factory` are mutually exclusive")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("stage", choices=("all", "graph", "consumers", "builds", "rejections"), default="all", nargs="?")
    stage = parser.parse_args().stage
    for name, action in (("graph", validate_graph), ("consumers", validate_consumers),
                         ("builds", validate_builds), ("rejections", validate_rejections)):
        if stage in ("all", name):
            action()
