#!/usr/bin/env python3
"""Prepare, qualify and remove one explicitly disposable Consumer fixture."""

import argparse
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import signal
import subprocess
import time

ROOT = Path(__file__).resolve().parents[3]
COMPOSE = Path(__file__).with_name("consumer.compose.yml")
IMAGES = ("rabbitmq", "postgresql", "mongodb")
SUITES = {
    "rabbitmq": ([], ["cap_q_06g_rabbitmq_lifecycle", "canonical_rabbitmq_e2e"]),
    "postgresql": (["transactional-inbox-postgresql"], ["postgresql_transactional_consumer_e2e"]),
    "mongodb": (["transactional-inbox-mongodb"], ["mongodb_transactional_consumer_e2e"]),
    "mongodb-factory": (["transactional-inbox-mongodb-factory"], ["mongodb_transactional_consumer_e2e"]),
}


def run(command, **kwargs):
    return subprocess.run(command, check=True, cwd=ROOT, **kwargs)


def compose(state, *args, **kwargs):
    project = (state / "project").read_text().strip()
    if not re.fullmatch(r"lily-consumer-live-[a-z0-9]+", project):
        raise ValueError("state is not a dedicated Consumer qualification project")
    return run(["docker", "compose", "-p", project, "--env-file",
                str(state / "services.env"), "-f", str(COMPOSE), *args], **kwargs)


def settings(state):
    return dict(line.split("=", 1) for line in (state / "services.env").read_text().splitlines())


def environment(state):
    config = settings(state)
    ports = {}
    services = compose(state, "ps", "--format", "json", capture_output=True, text=True)
    for line in services.stdout.splitlines():
        service = json.loads(line)
        for port in service.get("Publishers") or []:
            if port["PublishedPort"]:
                if port["URL"] != "127.0.0.1":
                    raise ValueError("qualification ports must be loopback-only")
                ports[port["TargetPort"]] = port["PublishedPort"]
    password = config["LILY_CONSUMER_TEST_PASSWORD"]
    rabbit = f"amqp://lily_qualification:{password}@127.0.0.1:{ports[5672]}/%2f"
    postgres = f"postgresql://lily_qualification:{password}@127.0.0.1:{ports[5432]}/lily_consumer_qualification"
    mongo = f"mongodb://127.0.0.1:{ports[27017]}/?replicaSet=lily-qualification&directConnection=true"
    env = {
        "CARGO_INCREMENTAL": "0",
        "LILY_TEST_RABBITMQ_DISPOSABLE": "1", "LILY_TEST_RABBITMQ_URL": rabbit,
        "LILY_TEST_RABBITMQ_MANAGEMENT_URL": f"http://127.0.0.1:{ports[15672]}",
        "LILY_TEST_RABBITMQ_MANAGEMENT_USERNAME": "lily_qualification",
        "LILY_TEST_RABBITMQ_MANAGEMENT_PASSWORD": password,
        "LILY_TEST_RABBITMQ_VHOST": "/", "LILY_CAP08_DISPOSABLE": "1",
        "LILY_CAP08_POSTGRES_URL": postgres, "LILY_CAP08_RABBITMQ_URL": rabbit,
        "LILY_CAP081_DISPOSABLE": "1", "LILY_CAP081_MONGODB_URL": mongo,
        "LILY_CAP081_RABBITMQ_URL": rabbit,
    }
    env_file = state / "test.env"
    env_file.write_text("".join(f"export {k}={shlex.quote(v)}\n" for k, v in env.items()))
    env_file.chmod(0o600)
    return env


def up(args, state):
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    if not (state / "project").exists():
        selected = {}
        for service in IMAGES:
            image = getattr(args, f"{service}_image")
            if not image or not re.fullmatch(r"[^\s=]+@sha256:[a-f0-9]{64}", image):
                raise ValueError(f"--{service}-image must be an immutable name@sha256 digest")
            selected[f"LILY_CONSUMER_{service.upper()}_IMAGE"] = image
        selected["LILY_CONSUMER_TEST_PASSWORD"] = secrets.token_hex(20)
        env_file = state / "services.env"
        env_file.write_text("".join(f"{k}={v}\n" for k, v in selected.items()))
        env_file.chmod(0o600)
        (state / "project").write_text("lily-consumer-live-" + secrets.token_hex(4))
    compose(state, "up", "-d", "--wait", "--wait-timeout", "120")
    compose(state, "exec", "-T", "mongodb", "mongosh", "--quiet", "--eval",
            'try { rs.status() } catch(e) { if(e.code !== 94) throw e; '
            'rs.initiate({_id:"lily-qualification",members:[{_id:0,host:"127.0.0.1:27017"}]}) }',
            capture_output=True, text=True)
    deadline = time.monotonic() + 60
    while True:
        hello = compose(state, "exec", "-T", "mongodb", "mongosh", "--quiet", "--eval",
                        "print(db.hello().isWritablePrimary)", capture_output=True, text=True)
        if hello.stdout.strip() == "true":
            break
        if time.monotonic() >= deadline:
            raise TimeoutError("test replica set did not elect a primary")
        time.sleep(1)
    environment(state)
    image_evidence = {k: v for k, v in settings(state).items() if k.endswith("_IMAGE")}
    (state / "images.json").write_text(json.dumps(image_evidence, indent=2) + "\n")
    print(f"Disposable services ready. Private environment and logs: {state}", flush=True)


def qualify(args, state):
    env = os.environ.copy()
    env.update(environment(state))
    suites = SUITES if args.suite == "all" else [args.suite]
    for suite in suites:
        features, targets = SUITES[suite]
        # Separate profiles: enabling both backends eagerly initializes both DI
        # services, while each fixture deliberately supplies only its backend.
        for target in targets:
            command = ["cargo", args.toolchain, "test", "--offline", "-p", "lily_consumer"]
            if features:
                command += ["--features", ",".join(features)]
            command += ["--test", target, "-j", "2", "--", "--ignored", "--test-threads=1"]
            log = state / f"{suite}-{target}.log"
            print(f"Running {suite}/{target}; log: {log}", flush=True)
            with log.open("w") as output:
                child = subprocess.Popen(command, cwd=ROOT, env=env, stdout=output,
                                         stderr=subprocess.STDOUT, start_new_session=True)
                try:
                    code = child.wait(timeout=900)
                except BaseException:
                    # A watchdog must not strand Cargo's test/fixture children.
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
                    raise
                if code:
                    raise subprocess.CalledProcessError(code, command)
            for line in log.read_text().splitlines():
                if line.startswith("test result:"):
                    print(line, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["up", "run", "down"])
    parser.add_argument("state", type=Path, help="private directory retained for this attempt")
    for service in IMAGES:
        parser.add_argument(f"--{service}-image")
    parser.add_argument("--suite", choices=["all", *SUITES], default="all")
    parser.add_argument("--toolchain", default="+1.96.1")
    args = parser.parse_args()
    state = args.state.resolve()
    if args.action == "up":
        up(args, state)
    elif args.action == "run":
        qualify(args, state)
    else:
        compose(state, "down", "--volumes", "--remove-orphans")
        print(f"Only this project's services removed; evidence retained in {state}")


if __name__ == "__main__":
    main()
