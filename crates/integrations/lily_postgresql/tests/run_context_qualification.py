#!/usr/bin/env python3
"""Run the single-mode PgDbContext live suite without silently missing coverage."""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parents[4]
PREFIX = "database::context_tests::"
# A stale executable, narrower filter, or removed critical test must fail discovery.
# Additional tests are discovered and required in every round automatically.
REQUIRED = {
    PREFIX + name
    for name in """
application_errors::acquisition_errors_convert_without_entering_application_work
application_errors::cancellation_disposal_and_panic_convert_after_rollback
application_errors::cleanup_timeout_overrides_custom_error_without_releasing_a_busy_lease
application_errors::commit_failure_converts_and_closes_context_despite_successful_work
application_errors::connection::cancellation_and_disposal_convert_after_discarding_the_connection
application_errors::connection::connection_acquisition_errors_convert_and_release_admission
application_errors::connection::custom_callback_preserves_payload_and_releases_the_pool
application_errors::connection::panicking_connection_conversion_runs_after_releasing_lease_and_admission
application_errors::connection::propagated_mapped_and_swallowed_custom_callbacks_all_roll_back
application_errors::connection::synchronous_and_async_callback_panics_convert_without_poisoning_context
application_errors::custom_error_preserves_payload_and_rolls_back_while_success_commits
application_errors::mapped_propagated_and_swallowed_query_errors_all_prevent_commit
application_errors::panicking_error_conversion_cannot_interrupt_rollback_or_poison_context
application_errors::rollback_failure_overrides_custom_error_and_retains_cleanup_failure
abandoned_unpolled_query_retains_dirty_lease_until_dropped
busy_context_rejects_overlap_and_dropped_query_forces_rollback
commit_is_not_interrupted_and_disposal_deadline_starts_at_disposal
context_transaction_cancels_pool_wait_and_rolls_back_running_sql
database_and_context_cancel_running_queries_and_discard_connections
di::injected_repositories_share_scope_and_owner_preserves_process_context
disposal_joins_owner_and_closes_only_its_context
dropped_transaction_caller_keeps_owner_alive_until_rollback
failed_cancel_transport_bounds_rollback_and_permanently_closes_context
failed_native_rollback_overrides_work_error_and_closes_context
lease_owns_accounting_and_cancelled_waiters_release_reservations
panics_rollback_and_native_state_corruption_closes_the_context
production::aborting_caller_during_blocked_sql_rolls_back_and_discards_the_backend
production::acquisition_timeout_releases_admission_without_invoking_work
production::caller_abort_after_commit_started_preserves_committed_data_and_shutdown_accounting
production::cancellation_during_native_begin_awaits_begin_then_rolls_back_without_entering_work
production::cancelling_ordinary_work_preserves_already_committed_statements_and_forwards_the_actual_view
production::closing_one_di_scope_rolls_back_its_sql_and_unblocks_an_independent_waiting_scope
production::concurrent_ordinary_queries_admit_exactly_one_callback_and_release_the_lease
production::concurrent_transaction_queries_share_backend_and_xid_with_exact_commit_and_rollback_images
production::deferred_constraint_failure_during_commit_returns_exact_error_and_never_reports_success
production::dropping_di_scope_automatically_disposes_context_while_sql_is_blocked
production::live_repository_token_cannot_cancel_a_transaction_and_selected_view_tracks_its_owner
repositories_share_one_transaction_and_errors_cannot_be_swallowed_into_commit
transaction_token_including_none_overrides_repository_token
""".split()
}


class QualificationFailure(Exception):
    pass


def positive_integer(value):
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def run(command, log, timeout):
    environment = dict(os.environ, CARGO_TERM_COLOR="never")
    with log.open("w", encoding="utf-8") as output:
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            stdout=output,
            stderr=subprocess.STDOUT,
            env=environment,
            start_new_session=os.name == "posix",
        )
        try:
            returncode = process.wait(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt) as error:
            # Stop the owned Cargo/test process group, including a stuck test binary.
            if os.name == "posix":
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            else:
                process.kill()
            process.wait()
            if isinstance(error, KeyboardInterrupt):
                raise
            raise QualificationFailure(f"{log.name} exceeded {timeout}s") from error
    if returncode != 0:
        raise QualificationFailure(f"{log.name} exited with status {returncode}")
    return log.read_text(encoding="utf-8", errors="replace")


def discover(output):
    names = re.findall(r"^(database::context_tests::\S+): test$", output, re.MULTILINE)
    tests = set(names)
    summary = re.findall(r"^(\d+) tests?, (\d+) benchmarks?$", output, re.MULTILINE)
    if not tests or len(names) != len(tests) or summary != [(str(len(tests)), "0")]:
        raise QualificationFailure("test discovery was empty, duplicated, or inconsistent")
    missing = REQUIRED - tests
    if missing:
        raise QualificationFailure("missing required scenarios: " + ", ".join(sorted(missing)))
    return tests


def verify_round(output, expected):
    passed = re.findall(r"^test (database::context_tests::\S+) \.\.\. ok$", output, re.MULTILINE)
    summary = re.findall(
        r"^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured;",
        output,
        re.MULTILINE,
    )
    if set(passed) != expected or len(passed) != len(expected):
        raise QualificationFailure("executed scenarios differ from the discovered suite")
    if summary != [(str(len(expected)), "0", "0", "0")]:
        raise QualificationFailure("the suite did not execute every scenario successfully")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=positive_integer, default=3)
    parser.add_argument("--test-threads", type=positive_integer, default=4)
    parser.add_argument("--timeout-secs", type=positive_integer, default=300,
                        help="wall-clock limit per Cargo invocation, including compilation")
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--report-dir", type=Path,
                        default=ROOT / "target" / "pg-context-qualification")
    args = parser.parse_args()
    if not os.environ.get("LILY_PG_TRANSACTION_TEST_URL", "").strip():
        parser.error("set LILY_PG_TRANSACTION_TEST_URL to a dedicated test database")
    ca = os.environ.get("LILY_PG_TRANSACTION_TEST_CA", "")
    if not ca or not Path(ca).is_file():
        parser.error("set LILY_PG_TRANSACTION_TEST_CA to an existing PEM CA bundle")

    args.report_dir.mkdir(parents=True, exist_ok=True)
    report = Path(tempfile.mkdtemp(
        prefix=datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ-"),
        dir=args.report_dir,
    )).resolve()
    print(f"Qualification logs: {report}", flush=True)
    command = ["cargo", "test", "-p", "lily_postgresql", "--no-default-features",
               "--features", "single,test-support", "--lib", "database::context_tests"]
    if args.offline:
        command.append("--offline")
    summary = {
        "status": "running",
        "features": ["single", "test-support"],
        "test_threads": args.test_threads,
        "requested_rounds": args.rounds,
        "timeout_secs": args.timeout_secs,
        "rounds": [],
    }
    exit_code = 0
    try:
        tests = discover(run(command + ["--", "--ignored", "--list"],
                             report / "discovery.log", args.timeout_secs))
        summary["tests"] = sorted(tests)
        print(f"Discovered {len(tests)} required live scenarios; running {args.rounds} rounds.",
              flush=True)
        for index in range(1, args.rounds + 1):
            log = report / f"round-{index}.log"
            result = {"round": index, "log": log.name, "status": "running"}
            summary["rounds"].append(result)
            started = time.monotonic()
            output = run(command + ["--", "--ignored", "--format", "pretty", "--color", "never",
                                    "--test-threads", str(args.test_threads)],
                         log, args.timeout_secs)
            verify_round(output, tests)
            result.update(status="passed", passed=len(tests),
                          elapsed_secs=round(time.monotonic() - started, 3))
            print(f"Round {index}: {len(tests)}/{len(tests)} passed", flush=True)
        summary["status"] = "passed"
    except (QualificationFailure, OSError, KeyboardInterrupt) as error:
        summary["status"] = "failed"
        summary["error"] = str(error) or "interrupted"
        for result in summary["rounds"]:
            if result["status"] == "running":
                result["status"] = "failed"
        print(f"Qualification stopped: {summary['error']}. See {report}", file=sys.stderr)
        exit_code = 130 if isinstance(error, KeyboardInterrupt) else 1
    finally:
        (report / "summary.json").write_text(json.dumps(summary, indent=2) + "\n",
                                             encoding="utf-8")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
