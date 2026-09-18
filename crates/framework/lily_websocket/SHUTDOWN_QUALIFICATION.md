# Shutdown qualification matrix

## Background services

The [background lifecycle tests](src/app/background_tests.rs) execute the real
builder, listener and shutdown owner. They use explicit barriers, exact join
and scope accounting, and event ordering; a timeout alone never proves cleanup.

| Scenario | Required evidence |
| --- | --- |
| Duplicate registration and multiple workers | One instance per concrete type; independent scope IDs; one shared original deadline. |
| Never started, already-cancelled start, failed bind | Constructor observed; execution count zero; prepared worker joins before DI disposal. |
| Required/optional backplane readiness | Required subscription gates execution; optional subscription does not; failure/cancellation never releases prepared work. |
| Worker error, panic, normal completion | Error/panic stop the host and remain failures; one-shot completion leaves readiness intact. |
| Cooperative scope cleanup and finalization | Correct `ProcessContext`, exact scoped identity, finalization scopes admitted, disposal before backplane/DI. |
| Cancelled build/start/close waiters; abandoned app | Retained cleanup continues; owned DI disposed once; external container remains usable. |
| Force while another component is waiting | Worker actually drops before the original cooperative cutoff. |
| Cleanup error or timeout | Failed scope receipt retained in incomplete report despite eventual joins. |
| Blocking worker destructor | Real OS-thread gate; incomplete frozen report; DI remains open until actual join; watchdog joined before fixture teardown. |
| External container with unrelated scope | Only the background factory's scopes close; external scope and container remain alive. |

[background_isolation.rs](tests/background_isolation.rs) uses an actual client
and listener. A real message deadline fires and its cooperative response is
received; a subsequent message succeeds. Peer Close is acknowledged. The worker
signal stays active throughout and is cancelled only by application shutdown.

[background_telemetry.rs](tests/background_telemetry.rs) runs the real file
exporter in four isolated processes (cooperative, forced, cleanup error and
cleanup timeout). It checks one terminal worker event, numeric duration, job and
cleanup trace/span identities, exact cleanup ordering, and the pre-flush
background counters. This test does not exercise an OTLP Collector.

```sh
cargo test -p lily_websocket --lib app::background::tests --offline --locked
cargo test -p lily_websocket --test background_isolation --test background_telemetry --offline --locked
```

Background adapter verification (2026-09-16): the combined WebSocket, derive,
background service, HTTP, DI and shutdown run passed **1,158 tests** with no
failures, including the 14 new lifecycle tests and two new integration tests.
The exporter integration runs four isolated scenarios. The existing ignored
HTTP loopback/port-conflict test was also run explicitly and passed. The
remaining ignored checks are 15 documentation examples, four Docker-dependent
Collector/Autobahn tests, and one OS-signal qualification test. They were not
run for this change. Clippy and formatting checks passed; Clippy reports only
the five previously documented WebSocket warnings.

```sh
cargo test -p lily_websocket -p lily_websocket_derive -p lily_background_service -p lily_injection -p lily_shutdown -p lily_http_api --offline --locked -- --test-threads=4
cargo test -p lily_http_api --lib port_in_use_preserves_addr_in_use_and_listener_context --offline --locked -- --ignored
cargo clippy -p lily_websocket -p lily_background_service --all-targets --features lily_websocket/fuzzing --offline --locked
cargo fmt -p lily_websocket -p lily_background_service --check
```

## Connection and message lifecycle

The tests below run against the implemented owner/executor/coordinator paths.
Virtual-time cases verify deadline arithmetic; duplex and real WS/WSS cases
verify transport and connection boundaries. Test names are exact Rust filters;
source links identify their containing modules.

| Scenario | Executable evidence | Invariant |
| --- | --- | --- |
| Empty / never-started graceful shutdown | [empty_shutdown_report_replays_once_and_preserves_container_ownership](src/app/reporting_tests.rs) | Single immutable report, no invented owners, actual root join, caller-owned DI preserved. |
| Idle running server | [running_close_and_a_dropped_start_waiter_share_one_supervised_terminal](src/app/mod.rs) | Dropping a start waiter enters the retained canonical root; close joins it. |
| Active connection graceful shutdown | [real_loopback_ws_reconciles_connections_cleanup_tasks_and_permits](src/app/mod.rs), [real_loopback_wss_reconciles_connections_cleanup_tasks_and_permits](src/app/mod.rs) | Close handshake, worker joins, cleanup and capacity reconcile for both transports. |
| Active action graceful shutdown / next dispatch | [graceful_shutdown_drains_started_action_before_close_and_rejects_next_message](src/app/mod.rs) | Accepted action, normal reverse exit and scope finish; queued/new application work is not dispatched. |
| Graceful deadline / explicit force | [force_at_before_guard_action_or_after_retains_reverse_termination_and_di_receipt](src/app/message_scope_qualification_tests.rs) | Before/guard/action/after interruption drops only execution; ledger and DI receipts survive. |
| Bounded cooperative cancellation | [middleware_guard_and_action_observe_execution_cancellation_before_return_and_reverse_cleanup](src/app/message_scope_qualification_tests.rs), [forced_execution_can_observe_cancellation_and_return_before_slot_abort](src/app/ownership.rs) | Cancellation is signalled first and user execution can yield and return before abort. |
| Forced connection/controller lifecycle | [real_connection_startup_failures_force_and_peer_close_obey_terminal_barriers](src/app/tests/connection_shutdown.rs) | Admission/open/connected failure, force and task abort preserve framework cleanup; disconnected follows real prerequisites. |
| Connected arming | [connected_success_arms_disconnect_before_pending_scope_close_and_survives_drop](src/app/message_scope_qualification_tests.rs), [disconnected_only_controller_is_not_implicitly_armed](src/app/tests/connection_shutdown.rs) | User obligation is armed by actual callback success, independent of later scope disposal. |
| Nested middleware reverse ordering | [interrupted_normal_exit_terminates_before_outer_and_is_not_retried](src/middleware/typed_tests/message_termination.rs) | Interrupted inner normal exit terminates before outer exits; replay does not rerun callbacks. |
| Forever pending cleanup / sibling isolation | [pending_termination_has_a_local_share_and_does_not_cancel_its_sibling](src/middleware/typed_tests/message_termination.rs) | Pending inner cleanup has a bounded share; outer callback receives independent authority; partial-poll evidence is correct. |
| Hook panic / destructor panic / returned error | [hook_panic_destructor_panic_and_returned_error_do_not_skip_outer_cleanup](src/middleware/typed_tests/message_termination.rs), [synchronous_disconnect_panic_does_not_skip_remaining_reverse_cleanup](src/app/mod.rs) | Contained failure cannot skip eligible outer cleanup; application error differs from framework interruption. |
| Cancellation while disconnect hook runs | [disconnected_evidence_survives_stage_drop_and_retains_unstarted_siblings](src/app/reporting_tests.rs) | Started cancellation and unstarted sibling remain distinguishable after the scoped callback future drops. |
| Peer disconnect plus shutdown race, many connections, late connection | [root_shutdown_reconciles_multiple_connections_peer_race_and_late_admission](src/app/tests/shutdown_qualification.rs) | Eight connections reconcile once through the full root; late Upgrade fails; all task groups and permits reconcile; report replay is stable. |
| New/escaped outbound work during drain | [admitted_callbacks_send_during_drain_and_forced_termination_uses_cleanup_authority](src/app/tests/outbound_shutdown.rs), [outbound admission cases](src/backplane/tests/outbound_shutdown.rs) | Only active callback authority grants drain continuation; cleanup sends have independent bounded authority. |
| Force cleanup expiry before first poll | [expired_root_records_not_started_without_polling_user_cleanup](src/middleware/typed_tests/message_termination.rs) | No arbitrary poll-once; three eligible timeouts remain not-started, never completed or partially polled. |
| Root shortening during cleanup | [shortened_root_stops_pending_hook_and_leaves_outer_not_started](src/middleware/typed_tests/message_termination.rs) | Later absolute shortening applies to current and future hooks without resetting time. |
| Nested budget arithmetic | [expired_budget_does_not_poll_a_new_hook_and_nested_caps_do_not_add](src/shutdown.rs), [force_splits_the_existing_cap_without_restarting_it](src/shutdown.rs) | Per-hook/phase waits do not multiply the root deadline. |
| Pending DI disposal | [root_cleanup_deadline_aborts_pending_di_disposal_and_observes_its_receipt](src/app/message_scope_qualification_tests.rs) | DI owns abort and exact-generation termination; scope receipt persists. |
| Scope ID reuse | [scope_receipt_survives_execution_drop_and_never_attaches_to_a_reused_id](src/app/ownership.rs) | A receipt never proves termination using another scope generation. |
| Dependency timeout / joined failure | [never_started_pending_provider_uses_one_deadline_and_disposes_di_only_after_join](src/app/reconciliation_tests.rs), [terminal_provider_failure_allows_di_disposal_but_remains_a_failure](src/app/reconciliation_tests.rs) | Failed cleanup and quiescent tasks are separate; failure cannot become success on retry. |
| Prerequisite still outstanding | [unjoined_lifecycle_owner_blocks_backplane_di_and_telemetry_cleanup](src/app/reconciliation_tests.rs) | Unconfirmed task termination does not authorize dependency disposal. |
| Late join after root expiry | [expired_root_keeps_an_immutable_incomplete_report_after_a_late_owner_join](src/app/reporting_tests.rs) | Frozen attempt remains incomplete, receipts remain observable, no new deadline/automatic success on repeated close. |
| Abort request versus confirmation | [task_abort_requests_and_confirmed_joins_are_distinct_in_aggregate_evidence](src/app/reporting_tests.rs), [abort_before_first_poll_is_not_confirmed_until_join_and_is_replayable](src/tasks.rs) | Request counter does not imply cancelled join; cloned handle requests are counted once. |
| Blocking destructor at deadline | [blocked_drop_keeps_the_real_join_owned_after_the_absolute_deadline](src/tasks.rs) | Tokio cannot preempt a destructor; retained join remains outstanding until actual termination. |
| Lost receivers / detached framework task regression | [concurrent_message_joins_retain_accounting_after_receivers_and_entries_disappear](src/app/reporting_tests.rs), [dropping_close_waiters_preserves_one_root_and_joins_provider_receipt_drivers](src/app/reconciliation_tests.rs) | Dropped waiters retain owners/drivers; 32 concurrent owner completions remain counted exactly once after map removal. |
| Root publication / concurrent close | [concurrent_close_callers_observe_an_installed_and_joined_root](src/app/reconciliation_tests.rs) | Sixteen callers cannot observe root completion without its installed and joined receipt. |
| Owner panic replay | [a_joined_message_owner_panic_is_not_erased_by_reconciliation_retries](src/app/reconciliation_tests.rs) | Compact registry retirement cannot erase a failed task result. |
| Diagnostic subscriber panic | [panicking_diagnostics_cannot_prevent_terminal_result_publication](src/app/reporting_tests.rs) | Reporting failure cannot strand terminal waiters or replace actual cleanup evidence. |
| Callback migration / wrong-phase extractor | [derive contracts](../../../tests/fixtures/macro_contracts/websocket/tests/compile_contracts.rs), [UI cases](../../../tests/fixtures/macro_contracts/websocket/tests/ui), [downstream fixture](../../../tests/fixtures/downstream_websocket_client) | Read-only wrappers and execution/cleanup extractor restrictions compile for supported use and reject unsupported signatures. |

## Message deadline and cooperative result qualification

The three message-timeout sessions use the final contract in
[MESSAGE_TIMEOUT.md](MESSAGE_TIMEOUT.md). The following scenarios extend the
shutdown matrix above; tests use the working tree's actual runtime and root
coordinator. No HTTP behavior is assumed or changed.

| Scenario | Executable evidence | Invariant |
| --- | --- | --- |
| Shared normal deadline across all eight stages | [every_normal_stage_and_deadline_extractor_observe_one_instant](src/app/tests/message_deadline.rs), [elapsed_forward_work_is_not_reset_at_any_later_stage](src/app/tests/message_deadline.rs) | Before/guard/extractor/action/normal after use one instant and one cooperative cutoff. |
| Whole-pipeline cooperative completion or ordinary error | [every_stage_can_observe_timeout_or_force_and_complete_the_entire_normal_pipeline](src/app/tests/message_deadline.rs), [an_ordinary_application_error_returned_during_cooperation_is_not_overwritten](src/app/tests/message_deadline.rs) | Timeout and force preserve actual complete results; callbacks can proceed through remaining normal stages without resetting the window. |
| Ignored token and partial normal exit | [ignoring_the_signal_is_allowed_only_until_the_shared_cooperative_cutoff](src/app/tests/message_deadline.rs) | A complete pipeline result is retained even without explicitly observing the token; handler-only completion cannot rescue a pending after. |
| Timeout first, force first, equal observation boundary; responsive and pending execution | [timeout_force_races_keep_the_first_cause_and_never_restart_cooperation](src/app/ownership.rs) | Six virtual-time cases retain one first cause, dynamically clip the same window, confirm slot termination and preserve owner state. |
| Repeated observers, parent/sibling isolation and late views | [local_timeout_notifies_only_its_message_and_never_restarts_cooperation](src/extractor/cancellation.rs), [completed_execution_freezes_deadline_evidence_and_retained_view_does_not_fire_a_late_timeout](src/extractor/cancellation.rs) | Child timeout cannot cancel its parent or siblings; repeated observation never restarts time or mutates completed evidence. |
| Three real connections, three sequential messages per connection | [three_connections_nine_messages_keep_local_timeout_isolated_and_later_shutdown_graceful](src/app/tests/message_cooperation.rs) | One local timeout does not end its connection or another message; all nine owners/results reconcile through graceful root shutdown. |
| Real root explicit force and graceful deadline, late dispatch | [real_root_explicit_force_and_graceful_expiry_preserve_results_and_reject_late_dispatch](src/app/tests/message_cooperation.rs) | Three active executions produce a preserved result, a preserved application error and one forced stop; queued fourth messages are not dispatched; output precedes Close where transport permits. |
| Local deadline followed by graceful shutdown, with and without peer Close | [local_timeout_then_graceful_shutdown_preserves_result_but_peer_close_suppresses_delivery](src/app/tests/message_cooperation.rs) | Actual pipeline result and reverse completion survive; a terminal peer suppresses the frame without a fabricated queue/delivery success. |
| Forced outbound continuation including backplane | [accepted_message_can_publish_after_cancellation_until_its_shared_cutoff](src/backplane/tests/outbound_shutdown.rs) | Accepted sends retain bounded authority during cooperation; expired authority cannot start another local or remote side effect. |
| Prepared output, full queue, shortened root transport deadline | [root_shortens_pending_terminal_queue_admission_without_claiming_a_write](src/app/message_reporting.rs) | Terminal queue admission has independent bounded authority; timeout releases the pending admission and reports interruption, with no detached send. |
| Transport absent, unpolled output, attempt drop/panic | [missing_transport_is_a_failed_attempt_and_unpolled_output_is_suppressed](src/app/message_reporting.rs), [dropped_and_panicked_attempts_are_terminal_even_when_an_observer_clone_survives](src/app/message_reporting.rs) | Failure, suppression, interruption and panic are distinct; none produces queue acceptance. |
| DI disposal pending while execution is already stopped | [force_at_before_guard_action_or_after_retains_reverse_termination_and_di_receipt](src/app/message_scope_qualification_tests.rs), [connection_cleanup_waits_for_session_late_message_owner_and_di_termination](src/app/message_scope_qualification_tests.rs) | Output attempts remain zero before DI/owner termination; releasing a dropped prepared decision records suppression. |
| Joined owner with retained output at shutdown | [joined_owner_with_unpublished_output_blocks_dependencies_and_freezes_incomplete_report](src/app/reporting_tests.rs) | Owner join alone cannot authorize dependencies; late suppression does not rewrite the immutable incomplete report. |
| Aggregate arithmetic and lost receivers | [inconsistent_message_output_and_cooperative_counts_cannot_reconcile](src/app/reporting_tests.rs), [concurrent_message_joins_retain_accounting_after_receivers_and_entries_disappear](src/app/reporting_tests.rs) | Impossible result/attempt/cause combinations fail reconciliation; 32 concurrent retired owners keep exact counters without detached workers. |
| Server panic recovery with a connection still completing output/cleanup | [crashed_server_children_retain_transport_tail_after_execution_cancellation](src/app/reconciliation_tests.rs) | Orphan recovery retains the transport tail after the execution cutoff, requires actual joins and preserves the server panic as failure. |

Message output counters cover prepared decisions and local terminal admission,
not individual transport writes or client delivery. Existing WS/WSS slow-reader,
write-failure, protocol-control, middleware panic, cleanup timeout and scoped
disposer tests above continue to qualify the corresponding resource boundaries.

## Release checks

Use the repository's working Rust toolchain and locked dependencies. The
qualified environment uses Rust 1.96.1:

```sh
cargo +1.96.1 test -p lily_websocket -p lily_websocket_derive -p lily_shutdown -p lily_injection --offline --locked
cargo +1.96.1 test -p lily_websocket -p lily_websocket_derive -p lily_config --all-targets --features lily_websocket/fuzzing --offline --locked
cargo +1.96.1 test -p lily_websocket --lib --features fuzzing --offline --locked
cargo +1.96.1 clippy -p lily_websocket -p lily_websocket_derive -p lily_shutdown -p lily_injection --all-targets --features lily_websocket/fuzzing --offline --locked
cargo +1.96.1 fmt -p lily_websocket -p lily_injection -p lily_shutdown --check
cargo +1.96.1 check --manifest-path tests/fixtures/downstream_websocket_client/Cargo.toml --target-dir target --offline --locked
cargo +1.96.1 check --manifest-path crates/framework/lily_websocket/fuzz/Cargo.toml --target-dir target --bin websocket_lifecycle --bin websocket_message_chain --offline --locked
git diff --check
```

Real WS/WSS tests need loopback socket access. These tests do not send OS
termination signals to the development process. The pre-existing manually run
signal/ignored documentation cases remain excluded. Fuzz smoke tests and fuzz
target compilation qualify harness compatibility, not a sustained fuzz campaign.

Message-timeout Session 3 verification (2026-09-07): the WebSocket, derive and
configuration all-target run passed **658 tests**, including **539 WebSocket
library/fuzz smoke tests** and the derive runner's **22 trybuild UI cases**.
The one existing ignored Docker/Autobahn test remained ignored. Nine tests were
added in this session; existing DI, output, concurrent-owner and recovery tests
gained terminal evidence assertions. The strengthened server-panic recovery test
failed at the premature execution cutoff before the fix; the complete WebSocket
library/fuzz run passed again after the fix (**539 passed, one ignored**).

Session 3 also passed downstream fixture compilation, both standalone fuzz
target checks, Clippy, changed-file formatting and `git diff --check`. Clippy
retains five existing warnings: unused `protocol`, `send_protocol`,
`send_raw_to_connection`, `let_and_return` in the connection registry and the
test-only `manual_async_fn` suggestion. No new public API, dependency version or
lockfile change was introduced in this session.

Phase 9 verification (2026-09-05): the combined four-crate run passed **623
tests across 32 test/doc groups**, plus **22 trybuild UI cases**. Four existing
ignored cases remained ignored. The final WebSocket library run with `fuzzing`
passed **486 tests** (480 WebSocket tests plus six fuzz smoke cases), with one
existing ignored case. Seven tests were added in Phase 9; existing ownership,
scope and termination tests gained aggregate evidence assertions. The root
admission/peer-close regression runs eight rounds of eight connections.

Clippy, formatting, `git diff --check`, downstream fixture compilation and both
standalone fuzz targets passed. Clippy reports only the four pre-existing
warnings: unused `protocol`, `send_protocol`, `send_raw_to_connection`, and the
test-only `manual_async_fn` suggestion. No dependency version or lockfile changed
in Phase 9. Shared `--target-dir target` avoids duplicating build artifacts for
the separate fixture/fuzz workspaces.

This matrix does not claim preemption of blocking application code, delivery of
all outbound messages, cleanup of raw application-spawned tasks, or guaranteed
completion/export of user cleanup/telemetry. Those boundaries are explicit in
the [migration contract](SHUTDOWN_MIGRATION.md).

## Macro contract location

Runtime-dependent derive UI tests now run from the unpublished macro contract
workspace. The `cargo test -p lily_websocket_derive` commands above cover the
implementation unit tests and gateway doctests; also run:

```sh
python3 tests/qualification/facade.py --stage macros
```
