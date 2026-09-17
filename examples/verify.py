#!/usr/bin/env python3
"""Verify the example stack against real infrastructure, including graceful shutdown.

Default: verify the already-running Compose apps, then stop those three apps.
--local: build/start owned host processes against the Compose infrastructure.
Artifacts and traces are retained; database volumes are never deleted here.
"""
import argparse
import json
import math
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import uuid

ROOT = Path(__file__).resolve().parent
COMPOSE = ["docker", "compose", "-f", str(ROOT / "compose.yaml")]
APPS = ["http-api", "websocket", "consumer"]


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def run(command, *, env=None, timeout=30):
    result = subprocess.run(command, cwd=ROOT, env=env, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"Command failed ({result.returncode}): {' '.join(map(str, command))}\n{result.stdout}\n{result.stderr}")
    return result.stdout.strip()


def records(path):
    require(path.is_file(), f"Missing trace file: {path}")
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def verify_traces(directory, job_id):
    data = {name: records(directory / f"{name}.jsonl") for name in ["http", "websocket", "consumer"]}
    for name, entries in data.items():
        lifecycles = {}
        for entry in entries:
            fields = entry.get("fields", {})
            if fields.get("lily.operation", "").startswith("example.") and "lily.lifecycle" in fields:
                lifecycles.setdefault(entry.get("span_id"), []).append(fields["lily.lifecycle"])
        require(lifecycles and all(phases == ["started", "completed"] for phases in lifecycles.values()),
                f"Missing, duplicated or interrupted method lifecycle in {name}")
        terminal = [e for e in entries if e.get("fields", {}).get("lily.lifecycle") == "completed"
                    and e.get("span", {}).get("name", "").startswith("example.")]
        require(terminal, f"No completed example method spans in {name}")
        for entry in terminal:
            fields = entry["fields"]
            duration = fields.get("lily.duration_ms")
            require(isinstance(duration, (float, int)) and not isinstance(duration, bool)
                    and math.isfinite(duration) and duration >= 0, f"Invalid method duration in {name}")
            require(re.fullmatch(r"[0-9a-f]{32}", entry.get("trace_id", ""))
                    and int(entry["trace_id"], 16) != 0, f"Missing trace identity in {name}")
            require(re.fullmatch(r"[0-9a-f]{16}", entry.get("span_id", ""))
                    and int(entry["span_id"], 16) != 0, f"Missing span identity in {name}")

    processed = [e for e in data["consumer"] if e.get("fields", {}).get("message") == "Job processed"
                 and e["fields"].get("job_id") == job_id]
    require(len(processed) == 2, f"Expected both original and duplicate deliveries, got {len(processed)}")
    terminals = [e for e in data["http"] if e.get("fields", {}).get("lily.event") == "http.server.terminal"]
    require(terminals, "HTTP terminal events are missing")
    http_traces = {e.get("trace_id") for e in terminals}
    require(all(e.get("trace_id") in http_traces for e in processed),
            "HTTP -> RabbitMQ -> Consumer trace identity was not propagated")
    for code in ["CONFLICT", "INVALID_INPUT", "NOT_FOUND"]:
        matches = [e for e in terminals if e["fields"].get("lily.error_code") == code]
        require(matches, f"Missing HTTP rejection for {code}")
        for entry in matches:
            require(entry["fields"].get("lily.outcome") == "rejected", f"{code} lost its rejection classification")
            require(entry.get("span", {}).get("otel.status_code") == "UNSET", f"{code} marked as technical error")
    require(all(e["fields"].get("lily.outcome") in ["success", "rejected"] for e in terminals),
            "Smoke requests produced a transport/technical failure")
    ws_rejections = [e for e in data["websocket"] if e.get("fields", {}).get("lily.error_code") == "INVALID_INPUT"
                     and e.get("fields", {}).get("lily.lifecycle") == "completed"]
    require(ws_rejections and all(e["fields"].get("lily.outcome") == "rejected"
                                 and e.get("span", {}).get("otel.status_code") == "OK" for e in ws_rejections),
            "The WebSocket service lost the application's rejection classification")
    for name in ["http", "websocket"]:
        require(any(e.get("span", {}).get("name") == "example.worker.summary"
                    and e.get("fields", {}).get("lily.outcome") == "success" for e in data[name]),
                f"No successful background scope iteration in {name}")
        require(any(e.get("fields", {}).get("message") == "Summary worker stopped cooperatively"
                    for e in data[name]), f"Worker failed to stop cooperatively in {name}")
    return {name: len(entries) for name, entries in data.items()}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local", action="store_true")
    parser.add_argument("--skip-build", action="store_true", help="Use existing local binaries")
    parser.add_argument("--target-dir", type=Path, default=ROOT.parent / "target")
    args = parser.parse_args()
    artifact = ROOT / "artifacts" / f"verify-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
    artifact.mkdir(parents=True)
    trace_dir = artifact / "traces"
    print(f"Artifacts: {artifact}", flush=True)
    env = dict(os.environ)
    processes = []
    handles = []
    smoke_result = None
    stopped_cleanly = False

    def client(*arguments, timeout=15):
        if args.local:
            return run([str(args.target_dir.resolve() / "debug/lily-example-client"), *arguments], env=env, timeout=timeout)
        return run([*COMPOSE, "run", "--rm", "--no-deps", "-T", "client", "lily-example-client", *arguments], timeout=timeout)

    try:
        if args.local:
            run([*COMPOSE, "up", "-d", "--wait", "postgres", "mongodb", "redis", "rabbitmq"], timeout=120)
            env.update(LILY_CONFIG_PATH=str(ROOT / "lily.toml"), LILY_CONFIG_MODE="development",
                       EXAMPLE_LOG_DIR=str(trace_dir), EXAMPLE_TRACE_OUTPUT="file",
                       EXAMPLE_HTTP_BIND="127.0.0.1:58100", EXAMPLE_WS_BIND="127.0.0.1:58101",
                       EXAMPLE_HTTP_URL="http://127.0.0.1:58100", EXAMPLE_WS_URL="ws://127.0.0.1:58101/ws",
                       EXAMPLE_RABBIT_URL="http://127.0.0.1:55673")
            # Keep verification on the isolated demo databases despite a developer's shell overrides.
            env.update({"LILY__POSTGRESQL__CONNECTION_STRING": "postgres://lily:lily_example@127.0.0.1:55432/lily_examples",
                        "LILY__DATABASE__CONNECTION_STRING": "mongodb://127.0.0.1:57017/",
                        "LILY__CACHE__REDIS_URL": "redis://127.0.0.1:56379/",
                        "LILY__QUEUE_CLIENT__HOSTNAME": "127.0.0.1", "LILY__QUEUE_CLIENT__PORT": "55672",
                        "LILY__RABBITMQ__CONSUMER__HOSTNAME": "127.0.0.1", "LILY__RABBITMQ__CONSUMER__PORT": "55672"})
            if not args.skip_build:
                build_env = dict(env, CARGO_TARGET_DIR=str(args.target_dir.resolve()), CARGO_INCREMENTAL="0", CARGO_PROFILE_DEV_DEBUG="0")
                build = run(["cargo", "+1.96.1", "build", "--workspace", "--bins", "--locked"], env=build_env, timeout=900)
                client_build = run(["cargo", "+1.96.1", "build", "--manifest-path", "clients/Cargo.toml", "--locked"], env=build_env, timeout=900)
                (artifact / "build.log").write_text(build + "\n" + client_build)
            for name in ["consumer", "http", "websocket"]:
                handle = (artifact / f"{name}.log").open("w")
                handles.append(handle)
                process = subprocess.Popen([str(args.target_dir.resolve() / f"debug/lily-example-{name}")],
                                           cwd=ROOT, env=env, stdout=handle, stderr=subprocess.STDOUT)
                processes.append((name, process))
        for command in ["consumer-health", "health", "ws-health"]:
            deadline = time.monotonic() + 60
            while True:
                for name, process in processes:
                    require(process.poll() is None, f"{name} exited during startup; see {artifact / (name + '.log')}")
                try:
                    client(command)
                    break
                except (RuntimeError, subprocess.TimeoutExpired):
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(0.2)
        baseline = json.loads(client("queue-status")).get("message_stats", {}).get("ack", 0)
        output = client("smoke", timeout=75)
        (artifact / "smoke.json").write_text(output + "\n")
        smoke_result = json.loads(output.splitlines()[-1])
        require(smoke_result["smoke"] == "passed", "Client smoke failed")
        job_id = str(uuid.UUID(smoke_result["job_id"]))
        note_id = smoke_result["deleted_note_id"]
        require(re.fullmatch(r"[0-9a-f]{24}", note_id), "Invalid note ID in test output")

        deadline = time.monotonic() + 30
        while True:
            queue = json.loads(client("queue-status"))
            if (queue.get("message_stats", {}).get("ack", 0) >= baseline + 2
                    and queue.get("messages_ready") == 0 and queue.get("messages_unacknowledged") == 0):
                break
            require(time.monotonic() < deadline, "Original and duplicate deliveries did not settle")
            time.sleep(0.5)
        sql = (f"SELECT (SELECT COUNT(*) FROM example_jobs WHERE id='{job_id}'), "
               f"(SELECT COUNT(*) FROM example_processed_events WHERE job_id='{job_id}');")
        counts = run([*COMPOSE, "exec", "-T", "postgres", "psql", "-U", "lily", "-d", "lily_examples", "-At", "-c", sql])
        require(counts == "1|1", f"Duplicate delivery created unexpected records: {counts}")
        ttl = int(run([*COMPOSE, "exec", "-T", "redis", "redis-cli", "TTL", f"lily_examples:job:{job_id}"]))
        require(0 < ttl <= 60, f"Completed job was not cached with a bounded TTL: {ttl}")
        remaining = run([*COMPOSE, "exec", "-T", "mongodb", "mongosh", "lily_examples", "--quiet", "--eval",
                         f'db.example_notes.countDocuments({{_id: ObjectId("{note_id}")}})'])
        require(remaining == "0", "Deleted MongoDB note is still present")
        (artifact / "storage.json").write_text(json.dumps({"job_rows": 1, "processed_rows": 1, "cache_ttl": ttl, "note_rows": 0}, indent=2))
    finally:
        # Only terminate processes launched here, or the explicitly selected Compose apps.
        if args.local:
            deadline = time.monotonic() + 22
            for _, process in processes:
                if process.poll() is None:
                    process.send_signal(signal.SIGTERM)
            exits = {}
            for name, process in processes:
                try:
                    exits[name] = process.wait(timeout=max(0.1, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    exits[name] = "forced-kill"
            for handle in handles:
                handle.close()
            stopped_cleanly = len(exits) == 3 and all(value == 0 for value in exits.values())
        else:
            run([*COMPOSE, "stop", "--timeout", "22", *APPS], timeout=35)
            exits = {}
            for name in APPS:
                container = run([*COMPOSE, "ps", "-a", "-q", name])
                exits[name] = int(run(["docker", "inspect", "--format", "{{.State.ExitCode}}", container]))
            stopped_cleanly = all(value == 0 for value in exits.values())
            trace_dir.mkdir()
            run([*COMPOSE, "cp", "http-api:/work/logs/.", str(trace_dir)])
            (artifact / "compose.log").write_text(run([*COMPOSE, "logs", "--no-color", *APPS]))
        (artifact / "shutdown.json").write_text(json.dumps(exits, indent=2))
    require(stopped_cleanly, f"An application failed graceful shutdown: {exits}")
    require(smoke_result is not None, "Smoke did not complete")
    traces = verify_traces(trace_dir, smoke_result["job_id"])
    (artifact / "result.json").write_text(json.dumps({"passed": True, "job_id": smoke_result["job_id"], "trace_records": traces}, indent=2))
    print("PASS: HTTP/WS contracts, queue acknowledgements, idempotent PostgreSQL writes, Redis TTL, MongoDB CRUD, tracing and graceful shutdown.")
    print("The Compose infrastructure and its data volumes remain available. Use docker compose down when finished.")


if __name__ == "__main__":
    main()
