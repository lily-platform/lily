# HTTP shutdown qualification matrix

**Phases 1–9 have executable evidence below.** Phase 1 tests private records;
Phase 2 exercises retained root/task owners, real Hyper workers and deadlines.
Phase 3 adds request owners, isolated dispatch destruction and exact request DI
termination receipts through real HTTP/1 and HTTP/2 paths. Phase 4 qualifies
atomic admission and cooperative execution through the real callback paths.
Phase 5 qualifies lazy-body/input lifetime, source destruction, file helper joins
and the graceful write watchdog. Phase 6 qualifies retained middleware
obligations, reverse abnormal cleanup and isolated bounded cleanup authority.
Phase 7 qualifies root dependency/monitor/telemetry and rollback ownership.
Phase 8 qualifies immutable HTTP aggregate reporting and health observations.
Phase 9 combines the actual managed listener, generated callbacks, body/input,
scope/dependency teardown and final report. The final evidence map identifies
which cases use loopback protocols and which require controlled owner faults.

See [architecture](SHUTDOWN_ARCHITECTURE.md) and
[phase dependencies](SHUTDOWN_ROADMAP.md).

## Implemented Phase 1 contract tests

Every test is in [src/lifecycle/tests.rs](src/lifecycle/tests.rs).

| Exact Rust test filter (prefix `lifecycle::tests::`) | Invariant qualified |
| --- | --- |
| `cancellation_preserves_first_reason_and_does_not_prove_termination` | First cancellation reason is stable; a pending execution remains nonterminal and can still return normally after the signal. |
| `unpolled_drop_is_terminal_without_inventing_execution` | A future cannot return before first poll; confirmed unpolled destruction is recorded without claiming the user body ran. |
| `terminal_failure_cannot_be_replaced_with_success` | Replayed completion cannot erase observed error/panic or restart a terminal slot. |
| `abort_request_requires_a_join_and_does_not_predict_its_result` | Repeated abort requests do not count as joins; actual completion/error/cancellation/panic wins over the requested outcome. |
| `execution_completion_and_body_eof_do_not_authorize_scope_close` | Handler return, head handoff and producer EOF cannot replace source release, bridge detachment or the DI receipt. |
| `response_outcome_and_resource_release_are_independent_of_delivery` | Explicit body disposition and resource release are required independently; none invents header delivery. |
| `pending_input_or_unjoined_helper_blocks_parent_cleanup` | A retained input user or unjoined helper, including an abort-requested helper, prevents same-scope parent cleanup. |
| `pending_execution_or_interrupted_normal_exit_remains_a_barrier` | Signal alone does not unlock termination; unresolved normal-after cleanup blocks scope close; a dropped execution can still have successful cleanup. |
| `settled_cleanup_failure_is_terminal_but_never_successful` | Failed/timed-out/cancelled/panicked/unstarted/unknown cleanup can be adjudicated and terminal without becoming successful. |
| `no_scope_created_and_scope_receipt_missing_are_different` | Explicit absence of a scope is not interchangeable with a created scope whose receipt is outstanding. |

Run the focused contracts with the working Rust toolchain and locked dependencies:

```sh
cargo +1.96.1 test -p lily_http_api --lib lifecycle::tests:: --offline --locked
cargo +1.96.1 clippy -p lily_http_api --lib --tests --offline --locked
cargo +1.96.1 fmt -p lily_http_api --check
cargo +1.96.1 doc -p lily_http_api --no-deps --offline --locked
git diff --check
```

Phase 1 verification (2026-09-06): **10 contract tests passed**, with 189
existing library tests outside the focused filter. Clippy (library and test
targets), formatting, documentation generation and whitespace checks passed.
Local documentation links and all scenario/test references were checked. No
runtime integration suite is claimed by this Phase 1 result; no dependency or
lockfile changed.

The focused tests require no listener, signal, network connection or new timeout
windows. They do not prove real cancellation polling, task ownership, exact DI
generation capture, middleware reverse scheduling or streaming resource release;
those require the integration cases below.

## Implemented Phase 2 ownership tests

The following 22 HTTP tests supplement the 10 Phase 1 contract tests. Test names
are exact suffixes; the source links identify the owning test modules.

| Test | Evidence qualified |
| --- | --- |
| `first_poll_sees_both_local_and_application_receipts` | Both actual join receipts exist before a worker's first user poll. |
| `dropping_every_local_waiter_does_not_cancel_or_detach_work` | A pending task remains owned without any local waiters. |
| `abort_before_first_poll_stays_outstanding_until_the_actual_join` | Abort records intent; only joined cancellation proves destruction. |
| `dropping_completion_stream_retains_cancelled_task_receipts` | Dropping a local task set cannot erase the root's joins. |
| `sealed_parent_rejects_unstarted_children_without_spawning` | Rejected late work is never polled/spawned outside the inventory. |
| `joined_panic_and_returned_error_survive_reaping_and_replay` | Joined panic and returned error remain distinct and replayable. |
| `concurrent_waiters_and_retirement_count_each_task_once` | Multiple receipts and retirement reconcile each task once. |
| `blocked_destructor_preserves_the_join_after_the_root_cutoff` | A destructor held across abort/deadline remains outstanding until released and actually joined. |
| `cutoffs_are_monotonic_within_one_root_including_zero_and_overflow` | All phase cutoffs fit H, with zero/overflow handled without extending it. |
| `repeated_begin_and_clones_never_restart_the_shutdown_attempt` | Repeated observation shares the original attempt. |
| `installing_root_deadline_clamps_an_existing_wait_without_dropping_its_task` | A pre-existing waiter adopts H while the actual task remains retained. |
| `nested_receipt_waits_cannot_add_another_budget` | Nested phase waits cannot restart the total timeout. |
| `delayed_native_signal_observation_uses_the_source_timestamp` | Delayed observation of a published shutdown state uses its original source time, including later force. |
| `native_signal_bounds_a_waiter_even_when_the_root_cannot_observe_it` | A waiting root receipt is clamped independently of its stalled worker after state publication. |
| `concurrent_close_without_start_uses_one_root_and_never_binds` | Sixteen close callers share one root and owned DI close; occupied listener address is never bound. |
| `dropped_start_waiter_requests_shutdown_and_preserves_all_root_joins` | Real running listener survives caller drop long enough to close; host token is not mutated; root/transport/monitor joins remain observable. |
| `dropped_close_waiter_keeps_pending_work_owned_and_preserves_caller_di` | An injected pending listener receipt survives close-waiter loss; caller DI remains open. |
| `listener_join_observes_a_signal_before_trigger_notification_is_polled` | A real listener joins after a durable signal while a controlled notification remains pending; root cleanup succeeds instead of reporting an unexpected listener stop. |
| `listener_join_racing_with_a_durable_shutdown_signal_is_clean` | 128 start/shutdown cycles on two runtime workers reconcile the durable signal and actual joins; this publishes shutdown state directly, not OS signals. |
| `listener_join_without_a_shutdown_signal_remains_a_failure` | Stopping listener admission without a lifecycle signal preserves the unexpected-stop error and its replay. |
| `failed_bind_result_is_replayed_after_confirmed_root_join` | Real bind failure keeps its I/O kind/message after root cleanup and repeated close. |
| `simultaneous_starts_share_one_claim_without_cancelling_the_winner` | Only one start wins; losing callers do not cancel the shared root. |
| `late_root_join_cannot_rewrite_an_already_frozen_timeout` | Late actual completion cannot replace the original deadline failure. |
| `actual_http2_workers_are_registered_in_connection_and_root_before_user_poll` | Three concurrent streams through the production tracked executor register both inventories before service polling and drain to actual worker joins. |
| `protocol_panic_is_terminal_evidence_but_not_clean_shutdown` | Joined worker panic permits terminal reconciliation but fails shutdown; terminal users permit owned DI close. |

Sources: [task tests](src/tasks/tests.rs), [deadline tests](src/shutdown.rs),
[App lifecycle tests](src/app/lifecycle_tests.rs), and
[tracked protocol tests](src/server/task_ownership_tests.rs). HTTP/2 uses Hyper
over Tokio duplex I/O with the production executor, not a socket-counter mock.
Native-signal deadline tests publish `ShutdownState` deterministically; they do
not claim to send OS signals to a subprocess.

Shared `lily_shutdown` regressions also cover
`existing_absolute_deadlines_do_not_restart_at_coordinator_entry`,
`expired_absolute_deadline_cannot_start_a_normal_callback`, and
`live_signal_monitor_transfer_does_not_abort_its_task` in
[framework.rs](../../foundation/lily_shutdown/src/framework.rs) and
[signal_handler.rs](../../foundation/lily_shutdown/src/signal_handler.rs).

Phase 2 verification (2026-09-06): the combined locked/offline HTTP, shutdown and
WebSocket test run passed. HTTP: **220 library tests**, **42 integration tests**
and **3 doctests** passed. Shared shutdown: **22 library tests** and **1 doctest**
passed. WebSocket regression: **486 library tests**, **3 integration tests** and
**8 doctests** passed on the working tree. Existing ignored cases remain: one
HTTP library test, one shutdown signal-subprocess test, one WebSocket library
test and 14 HTTP documentation examples. They are not included as passed tests.
Loopback tests ran with local port access. The WebSocket build emitted three
dead-code warnings from its separately modified working-tree sources.
HTTP/shutdown Clippy (all targets), formatting, rustdoc generation and whitespace
checks passed. All 33 local documentation links, exact HTTP test references and
Q01–Q36 scenario IDs were checked.

```sh
cargo +1.96.1 test -p lily_http_api -p lily_shutdown -p lily_websocket --offline --locked -- --quiet
cargo +1.96.1 clippy -p lily_http_api -p lily_shutdown --all-targets --offline --locked
cargo +1.96.1 fmt -p lily_http_api -p lily_shutdown --check
cargo +1.96.1 doc -p lily_http_api -p lily_shutdown --no-deps --offline --locked
git diff --check
```

Tokio's existing workspace dependency enables `test-util` only for HTTP dev
tests (paused time); no package or lockfile version change is introduced.

## Implemented Phase 3 request ownership tests

The following 18 HTTP tests are in
[request_lifecycle/tests.rs](src/request_lifecycle/tests.rs). Names are exact
suffixes under `request_lifecycle::tests::`; each uses the production request
registry, execution slot and DI owner. The protocol cases additionally use the
managed server with actual loopback HTTP/1 or HTTP/2 connections.

| Test | Evidence qualified |
| --- | --- |
| `request_identity_and_exact_scope_are_published_before_user_poll` | Request identity and the concrete DI generation exist before middleware polls; execution and request state release precede scope disposal with the original process metadata. |
| `dropped_service_waiter_stops_only_execution_and_retains_pending_di` | Losing the service waiter destroys only dispatch; its owner, pending scope receipt and parent dependency barrier survive. |
| `dropping_an_unpolled_service_waiter_does_not_enter_user_execution` | A stopped, unpolled dispatch creates no scope and runs no middleware body; confirmed destruction is distinct from first poll. |
| `execution_panic_is_contained_before_request_and_scope_release` | A dispatch panic is contained; actual slot destruction precedes retained request release and DI cleanup. |
| `returned_middleware_error_remains_a_normal_execution_return` | A typed middleware error remains a returned error, preserving its normal HTTP error behavior rather than inventing cancellation. |
| `request_timeout_preserves_its_scope_receipt_after_slot_destruction` | The original execution deadline stops dispatch; a separate bounded local cleanup allowance permits disposal without moving the application deadline. |
| `missing_scope_receipt_blocks_reconciliation_instead_of_becoming_no_scope` | A created scope with a missing receipt remains outstanding; it cannot be reclassified as an absent scope or authorize parent disposal. |
| `a_late_scope_observer_cannot_attach_to_a_reused_process_id` | HTTP retains the original scope generation even after textual identity reuse; closing HTTP does not close the caller's replacement scope/container. |
| `concurrent_request_owners_reconcile_distinct_scopes_and_contexts` | Thirty-two concurrent owners preserve distinct contexts/generations and retire each join and scope exactly once. |
| `http1_peer_disconnect_keeps_request_cleanup_owned_after_transport_join` | Real HTTP/1 connection loss can join transport while the request's pending DI cleanup remains independently owned. |
| `pending_http_request_body_disconnect_cannot_lose_its_scope` | Disconnect while consuming an incomplete input body cannot erase the request owner or exact scope cleanup receipt. Phase 5 adds transferred input/output coverage below. |
| `http2_connection_loss_retains_each_concurrent_request_owner` | Closing the actual TCP socket with two active HTTP/2 streams retains both request owners and their independent DI cleanup obligations. |
| `forced_transport_stop_waits_for_request_scope_before_owned_dependencies` | Transport force stops dispatch but cannot discard the request owner; owned DI waits for the pending request scope and actual owner join. |
| `root_cleanup_cutoff_stops_only_the_retained_di_generation_and_preserves_failure` | U clamps pending disposal, R bounds receipt observation; actual termination and the original timeout/failure remain distinct, including repeated root observation. |
| `scope_disposer_panic_is_terminal_failure_even_with_caller_owned_di` | Confirmed disposer termination after panic remains cleanup failure; HTTP does not close the caller container. |
| `request_capacity_is_retained_until_scope_and_owner_join_are_terminal` | The existing capacity permit covers pending DI cleanup and real owner join, not only dispatch return. |
| `an_in_progress_execution_destructor_blocks_scope_close_and_terminal_evidence` | A synchronously blocked dispatch destructor prevents terminal evidence and DI disposal until destruction actually finishes; stop request alone is insufficient. |
| `external_scope_close_is_terminal_without_inventing_a_disposal_result` | When another owner claims disposal, HTTP can prove the original generation terminated but reports the unavailable disposal result as unknown rather than successful. |

The shared DI regression
`scope_handle_observation_and_close_stay_bound_to_the_created_generation` in
[application_container.rs](../../foundation/lily_injection/src/application_container.rs)
verifies that both late observation and close through an old `ApplicationScope`
handle remain bound to its original generation. That handle cannot close a new
scope which reuses the process ID. The hidden close-result adapter distinguishes
an observed cleanup result from an already-claimed cleanup ticket; existing
public `ApplicationScope::close` behavior is preserved.

Phase 3 verification (2026-09-06): the combined locked/offline HTTP, DI, shared
shutdown and WebSocket run passed **893 tests**, with **0 failures** and **19
ignored cases**. HTTP: **238 library**, **42 integration**, **3 documentation**
tests passed. DI: **23 library**, **56 integration**, **2 documentation** tests
passed. Shared shutdown: **22 library**, **1 documentation** test passed.
WebSocket: **495 library**, **3 integration**, **8 documentation** tests passed
on the concurrently modified working tree. This includes the existing CORS
timeout/error-shaping regressions. Ignored cases are one HTTP library test, one
shutdown signal-subprocess test, one WebSocket library test, 14 HTTP examples
and two DI examples; none is counted as passed. Loopback tests ran with local
port access. Three existing WebSocket dead-code warnings remain outside this
phase's modified HTTP/DI sources.

HTTP/DI Clippy (all targets), formatting, rustdoc generation, local documentation
links, exact test references and whitespace checks passed. No new dependency,
lockfile version or public HTTP callback signature was introduced by Phase 3.

```sh
cargo +1.96.1 test -p lily_http_api -p lily_injection -p lily_shutdown -p lily_websocket --offline --locked -- --quiet
cargo +1.96.1 test -p lily_http_api --lib request_lifecycle::tests:: --offline --locked
cargo +1.96.1 clippy -p lily_http_api -p lily_injection --all-targets --offline --locked
cargo +1.96.1 fmt -p lily_http_api -p lily_injection --check
cargo +1.96.1 doc -p lily_http_api -p lily_injection --no-deps --offline --locked
git diff --check
```

The registry counts service candidates, including capacity/route rejections. Phase 4 adds separate accepted/rejected
admission counters; final report aggregation remains Phase 8. It
proves dispatch/owner/scope boundaries only. Request scope still closes before
lazy response execution; a returned response or retired owner is not body or
network-delivery evidence. The Phase 3 tests alone do not qualify middleware termination or cooperative
polling; Phase 4 evidence follows.

## Implemented Phase 4 admission and cooperative execution tests

Seventeen tests in [request_lifecycle/cancellation.rs](src/request_lifecycle/cancellation.rs)
run under `request_lifecycle::tests::cancellation::`. They use the production
request registry/owner, with paused time for deadlines and real Hyper connections
for the last three protocol cases.

| Test | Evidence qualified |
| --- | --- |
| `graceful_gate_closure_does_not_cancel_accepted_execution` | Closing admission and starting the root deadline leave accepted execution and all its views uncancelled before G. |
| `admitted_deadline_includes_time_before_dispatch_and_preserves_a_returned_error` | Time between acceptance and dispatch counts toward the same deadline; an application error returned after cancellation is preserved. |
| `an_expired_admitted_deadline_cannot_restart_or_first_poll_execution` | An accepted slot that has not started by its deadline cannot start user work or create a scope. |
| `actual_request_deadline_then_force_keeps_the_first_cooperative_cutoff` | The real request timer followed by force/root installation keeps the original reason and 250ms window; scope cleanup remains independent. |
| `graceful_cutoff_signals_the_same_future_and_all_context_views` | At G, the same invocation sees the signal and returns; first poll/destruction occur once and middleware/exchange/request views agree. |
| `force_notifies_before_drop_and_allows_a_controlled_normal_return` | Force is observable while execution and scope are still live; a controlled normal return within the window is preserved. |
| `ignored_signal_stops_only_the_slot_and_leaves_di_cleanup_independent` | Pending execution stops at the 250ms cap; retained DI remains pending until separately released, despite the cancelled execution view. |
| `repeated_stop_requests_cannot_extend_the_cooperative_window_or_reason` | Later force, timeout and root installation cannot restart the first local window or overwrite its first reason. |
| `delayed_cancellation_observation_cannot_run_past_root_c` | Observing G just before C leaves only the actual remaining root time, never a fresh full cooperative grant. |
| `a_spent_root_window_does_not_poll_an_unstarted_callback` | A previously registered but unpolled slot cannot first-enter user code after its root cutoff; no scope is created. |
| `normal_after_code_observes_execution_cancellation_and_can_return` | The around middleware's pending normal after-code receives execution cancellation and can finish normally. |
| `a_panic_during_cooperative_polling_is_contained_before_di_cleanup` | Panic after observing cancellation remains a panic; actual slot release precedes scope cleanup. |
| `gate_rejects_registered_candidates_and_late_work_without_user_execution` | Capacity denial, a candidate losing admission and a post-closure arrival run no user code and open no DI scope. |
| `concurrent_admission_closure_balances_accepted_and_rejected_identities` | Thirty-two concurrent arrivals racing gate closure each get one accepted/rejected disposition; all capacity and owner receipts reconcile. |
| `http1_keep_alive_and_new_connections_cannot_bypass_closed_admission` | Real keep-alive and new TCP requests hit the same application gate, return 503/Connection-close and create no additional scope. Closing the App also closes the listener. |
| `http2_late_stream_is_denied_while_an_accepted_stream_can_finish` | A late real HTTP/2 stream is denied while its previously admitted sibling finishes normally with an uncancelled execution view. |
| `actual_app_close_drains_an_accepted_request_without_signalling_it` | Real App close waits for accepted request execution/cleanup; it does not cancel that execution at graceful admission closure. |

The real integration test
`generated_action_guard_middleware_and_dynamic_cors_share_read_only_cancellation`
in [tests/execution_cancellation.rs](tests/execution_cancellation.rs) exercises
before, guard, custom extractor, generated controller action, normal after,
`IntoResponse`, dynamic CORS and a cancellation-ignoring action through actual
HTTP. Cooperative returns preserve response bytes, normal-after headers and
the action's own error status. Only the ignoring action receives **504** and
does not run normal after-code.
Request-local clear preserves the signal, and a later request gets an independent
uncancelled authority. The generated adapter remains generic; no macro-specific
runtime token bypass is used.

`pending_request_body_shares_the_deadline_and_can_finish_in_the_cooperative_window`
uploads a partial body over real HTTP/1.1 to a generated `RawBody` action. If the
remaining bytes arrive after cancellation but inside the window, the original
response is sent and normal after-code runs. A permanently incomplete upload
selects 504 without entering the action. No independent frame-read timer emits
408 or restarts the accepted deadline. These tests qualify execution through
response creation; the Session 3 tests below also qualify response writing.

Shared [view tests](../../foundation/lily_web_core/src/cancellation.rs) qualify
`views_replay_cancellation_without_owning_its_source`,
`independent_sources_and_inactive_requests_do_not_cross_cancel` and
`request_local_clear_cannot_remove_execution_authority`. Four compile-fail
examples reject callback-side cancellation, default construction, child-token
creation and source-field access. The existing
[typed action compile fixture](tests/ui/struct_controller/pass_typed_action.rs)
also accepts an `ExecutionCancellation` type alias as the terminal extractor.
The unknown-extractor negative fixture still fails compilation; its expected
Rust diagnostic now includes the new supported extractor.

The Phase 3 blocked-destructor regression still proves that a stop request
cannot authorize scope disposal or become terminal evidence. Its pending-join
assertion now polls the retained receipt directly: a test deliberately blocking
a runtime worker must not depend on a new timer tick to release its own barrier.
A bounded test-only watchdog prevents the fixture from hanging the test process.

Phase 4 verification (2026-09-06): **1,146 tests passed, 0 failed, 19 ignored**
in the combined locked/offline HTTP, web-core, middleware, HTTP macros, DI,
shutdown and WebSocket run. HTTP: **252 library**, **43 integration** and
**3 documentation** tests passed; web-core: **164 library**, **7 documentation**;
middleware: **50 library**; HTTP macros: **17 library**. DI, shutdown and
WebSocket also passed their full suites. Existing ignored cases and the three
WebSocket dead-code warnings remain unchanged from the Phase 3 inventory.

HTTP/web-core/middleware Clippy (all targets), formatting, rustdoc generation,
whitespace checks, local links and exact test references passed. The external
HTTP consumer and middleware example compiled together in an isolated temporary
manifest using the current working-tree crate paths and an offline lock derived
from the workspace lock. The checked-in consumer's older lock required refresh
and the example is not a workspace member; neither repository lockfiles nor
workspace membership were changed to perform this qualification. No new package
or dependency version was introduced by Phase 4.

```sh
cargo +1.96.1 test -p lily_http_api -p lily_web_core -p lily_middleware -p lily_http_api_macros -p lily_injection -p lily_shutdown -p lily_websocket --offline --locked -- --quiet
cargo +1.96.1 clippy -p lily_http_api -p lily_web_core -p lily_middleware --all-targets --offline --locked
cargo +1.96.1 fmt -p lily_http_api -p lily_web_core -p lily_middleware --check
cargo +1.96.1 doc -p lily_http_api -p lily_web_core -p lily_middleware --no-deps --offline --locked
git diff --check
```

This Phase 4 evidence qualifies dispatch through response creation. Phase 5
producer/input/helper coverage follows below; middleware termination
eligibility and reverse cleanup remain Phase 6, and complete helper/dependency/
telemetry/report reconciliation remains Phases 7–8. Existing task abort-vs-join
and exact DI-receipt tests still apply; no user cleanup completion or preemption
of blocking execution is promised.

## Implemented Phase 5 response/input/helper qualification

Twelve new cases in [request body owner tests](src/request_lifecycle/body_tests.rs)
exercise the retained request task, body bridge and exact DI generation. The two
wire tests use the managed listener and actual HTTP/1 or HTTP/2 clients; other
cases target production ownership components with deterministic sources and
paused time or destructor barriers.

| Test | Evidence |
| --- | --- |
| `handoff_preserves_scope_and_source_backpressure_until_actual_release` | Head handoff and execution return leave scope/source outstanding; one demand yields at most one bounded frame, and source release precedes disposal. An inert retained bridge does not retain the scope. |
| `pending_producer_observes_signal_without_another_hyper_poll` | A pending source observes the same execution signal and returns cooperatively while Hyper is no longer polled; DI stays live during that window. |
| `force_stops_unpolled_and_ignoring_sources_independently_of_bridge_polls` | Unstarted sources remain unpolled/NotStarted; started ignoring sources are Interrupted at the first-signal cap, with actual release before DI. |
| `body_uses_the_admitted_deadline_and_shutdown_cannot_restart_its_window` | The original request deadline signals the producer; later root/force cannot renew the first-signal window or replace its reason. |
| `producer_poll_panic_releases_source_before_scope_and_reports_panicked` | A contained source poll panic preserves Panicked evidence and releases captures before disposal. |
| `eof_and_blocked_source_destructor_cannot_authorize_di_cleanup` | EOF is recorded while a destructor is deliberately blocked; source terminal evidence, DI close and owner join remain unavailable until real release. |
| `input_ownership_follows_reader_into_returned_body_until_source_drop` | A transferred reader remains outstanding after handler return and while output waits for input; bridge loss signals/stops the source and releases input before DI. |
| `escaped_input_receipt_blocks_scope_and_preserves_wait_failure` | Escaped framework input prevents DI close/retirement. Late release and repeated exact-receipt observation do not erase the original timeout/failure. |
| `sse_keep_alive_remains_bounded_and_releases_its_captured_source` | Valid SSE keep-alive is lazy and frame-bounded; force independently drops the source before DI. |
| `protocol_bytes_cannot_retain_scoped_user_destructors` | A custom `Bytes::from_owner` destructor runs under request context before protocol bytes survive DI close; destructor panic instead blocks scope disposal and leaves the request incomplete. |
| `http1_suppressed_sources_release_unpolled_before_scope_on_keep_alive` | Real HEAD/204/304 responses over one keep-alive connection never poll the source and release each scope independently. |
| `http2_body_failure_is_stream_local_and_request_scopes_release_independently` | A failed body resets only its HTTP/2 stream; healthy and pending siblings retain their own scope lifetimes. |

The additional
[`unbound_graceful_drain_keeps_connection_fallback_and_accounts_for_cancelled_h2_sibling`](src/server/http2_transport_tests.rs)
case exercises raw codec services without managed request receipt binding:
real in-memory HTTP/2 flow control, two unconsumed responses,
graceful drain, a write timeout, and distinct timed-out/cancelled accounting for
the two streams after the unbound connection-wide fallback. Managed request
isolation is now qualified by the timeout Session 2 cases below. Existing live HTTP/1
streaming, SSE, static-file GET/HEAD/range/conditional, frame/length/error limits
and local/TLS ownership tests continue to pass.

Five new cases in [shared HTTP resource tests](../../foundation/lily_web_core/src/http_resources/tests.rs):

| Test | Evidence |
| --- | --- |
| `cancelled_caller_retains_started_blocking_join` | Caller abort joins separately; requesting helper abort does not stop started blocking work. The retained helper join stays pending until explicit worker release. |
| `abandoned_helper_result_is_destroyed_before_actual_join` | An abandoned typed file/helper result is destroyed in the worker before its actual join becomes terminal. |
| `actual_static_file_open_and_lazy_reads_retain_helper_joins` | Real static-file open and three bounded reads register four actual joins; no speculative read starts at response construction. |
| `input_release_distinguishes_handler_unwind_from_actual_destructor_failure` | Handler panic can still release input successfully; an actual input destructor panic leaves failed/unconfirmed release evidence. |
| `helper_panic_is_observed_and_does_not_become_success` | A worker panic is joined terminal evidence with a separate failure count. |

Validation against the working tree: **1,169 passed, 0 failed, 19 intentionally
ignored** across HTTP, web-core, middleware, HTTP macros, DI, shutdown and
WebSocket unit/integration/doc tests. The 18 new Phase 5 cases are included.
The WebSocket working tree had three unrelated existing dead-code warnings.
Phase 5 HTTP/web-core whitespace checks are clean. The global working-tree
check also encountered trailing whitespace in concurrently changed WebSocket
code outside this phase; those changes were preserved.
Local HTTP/TLS tests require loopback permission; the sandbox-only run cannot
bind listeners. No test expectation was relaxed for transport permission errors.

```sh
cargo +1.96.1 test -p lily_http_api -p lily_web_core -p lily_middleware -p lily_http_api_macros -p lily_injection -p lily_shutdown -p lily_websocket --offline --locked -- --quiet
cargo +1.96.1 clippy -p lily_http_api -p lily_web_core -p lily_middleware --all-targets --offline --locked
cargo +1.96.1 fmt -p lily_http_api -p lily_web_core -p lily_middleware --check
cargo +1.96.1 doc -p lily_http_api -p lily_web_core -p lily_middleware --no-deps --offline --locked
git diff --check -- crates/framework/lily_http_api crates/foundation/lily_web_core
```

This phase does not claim socket delivery, preemption of a blocked destructor
or started blocking operation, middleware termination hooks, complete late
dependency/telemetry reconciliation, or the Phase 8 aggregate report. Source
outcomes/input/helper receipts and existing transport counters are separate
observations. The final production qualification still belongs to Phase 9.

## Implemented Phase 6 middleware qualification

Seventeen new [request middleware tests](src/request_lifecycle/middleware_tests.rs)
exercise the actual request owner, DI generation and production chain adapter.
The routed case uses App dispatch plus generated application/controller/action
middleware plans for eight concurrent requests. The others isolate the same
production owner/chain with controlled pending/panicking callbacks and resources.

| Test | Evidence |
| --- | --- |
| `actual_application_controller_action_chains_share_one_ledger_per_concurrent_request` | Eight retained request owners have 24 distinct invocation obligations. The same object at application/controller positions is constructed once but finalized separately; each request observes `2 -> 1 -> 0`, actual scope termination and owner retirement. |
| `force_unwinds_same_instance_invocations_in_reverse_after_actual_execution_drop` | Three positions sharing one object retain distinct state outside `Request.local.clear`. Actual stack release precedes hooks; repeat finalization does not invoke them again. |
| `interrupted_before_only_arms_the_entered_prefix` | A pending middle before prevents inner entry; only the two entered invocations terminate, with Before/Delegating stage evidence. |
| `partial_normal_after_does_not_reopen_the_returned_inner_frame` | The inner normal return is preserved while the middle after and outer next frames unwind in reverse. |
| `typed_errors_short_circuit_and_normal_completion_do_not_invoke_termination` | Normal success, short-circuit, returned before error and returned after error are normal returns for eligibility. |
| `contained_execution_panic_keeps_its_entered_obligations` | Poll panic retains the entered prefix and original scope; eligible cleanup runs with bounded ExecutionPanicked evidence. |
| `application_abandoned_next_is_not_mistaken_for_an_inner_normal_return` | An application that drops pending next does not manufacture an inner normal return; only the inner obligation terminates. Framework-retained parent state releases after inner cleanup. |
| `hook_timeout_panic_and_error_do_not_cancel_or_skip_outer_invocations` | Each failure settles once; confirmed destruction allows outer hooks. Sibling views remain independent and the request retains cleanup failure despite terminal DI/joins. |
| `cleanup_signal_has_a_bounded_cooperative_tail_and_matching_context_authority` | The same hook continues after cleanup notification and can return within its existing cap. Parameter/context signals and deadlines agree; execution cancellation does not poison them. |
| `exhausted_root_cleanup_budget_never_polls_hooks_or_renews_sibling_budgets` | Spent U yields three NotStarted obligations with zero termination polls and no fresh budget. |
| `request_timeout_keeps_cleanup_independent_and_waiter_loss_does_not_abort_hooks` | Local execution timeout leaves cleanup authority usable. Dropping the service waiter during a cooperative hook does not abort the retained owner/finalizer. |
| `pending_hooks_share_one_owner_cutoff_and_report_unstarted_outer_frame` | Two pending hooks exhaust one local owner deadline; the outer invocation is NotStarted, without extending the total. DI deadline failure remains failure. |
| `cleanup_future_drop_panic_blocks_parent_hooks_and_scope_disposal` | An actual pending callback destructor panic leaves release unconfirmed; outer callbacks, HTTP DI disposal and retirement remain blocked. HTTP close reports incomplete. |
| `retained_input_receipt_blocks_all_termination_hooks_until_actual_release` | Even after execution destruction, a live transferred input receipt prevents every hook and DI disposal until real release. |
| `response_source_release_is_required_before_eligible_inner_cleanup` | An eligible interrupted inner invocation waits for a later retained response source to release; the bridge cannot authorize early cleanup. |
| `later_body_interruption_never_rearms_normally_returned_middleware` | Body force after three normal middleware returns creates no abnormal callback and retains state through source release. |
| `cleanup_file_helpers_have_retained_joins_outside_the_sealed_execution_inventory` | Actual static-file mount opens inside termination register three separate cleanup helper joins; execution inventory sealing does not detach or reject those owned helpers. |

The eighteenth HTTP case,
[`finalizer_waiter_drop_retains_the_same_hook_and_adopts_a_later_root_deadline`](src/request_lifecycle/middleware.rs),
drops a polled finalizer observer, proves the callback is still retained and not
destroyed, then installs a shorter root deadline. A new observer polls the same
callback through cancellation; the context/view reflects U, and repeated finalization
keeps exactly one call and one destruction.

The shared
[`cleanup_authorities_are_read_only_isolated_and_only_shorten_deadlines`](../../foundation/lily_web_core/src/cancellation.rs)
case verifies independent execution/cleanup/sibling signals, replay and monotonic
cutoffs. Four compile-fail examples deny cancellation, construction, child creation
and execution-to-cleanup conversion. The external
[downstream HTTP fixture](../../../tests/fixtures/downstream_http/src/main.rs)
implements the hook and retains/takes typed state using only facade imports.

This phase adds **19 executable scenarios**, plus four compile-fail authority
examples. Working-tree validation passed **1,196 tests, 0 failed, 19 existing
ignored** across HTTP, web-core, middleware, HTTP macros, DI, shutdown and
WebSocket unit/integration/doc suites. The WebSocket working tree still emits
three unrelated pre-existing dead-code warnings. The three changed runtime
crates pass Clippy with `-D warnings`, formatting and rustdoc generation.
After the final change that releases the evidence lock before tracing, all
**62 request lifecycle tests** passed again, including all 18 new HTTP cases.

The downstream fixture passed compilation and formatting with only facade
imports. Its existing local lockfile was stale, so validation used a temporary
manifest pointing directly to the working-tree fixture source and path crates,
seeded from the workspace lockfile. All resolved package versions match that
workspace lockfile; no repository lockfile was changed. All 57 local Markdown
links checked across the phase documents/READMEs exist; all 17 test-table names
resolve to actual tests. Scoped whitespace checks are clean.

```sh
cargo +1.96.1 test -p lily_http_api -p lily_web_core -p lily_middleware -p lily_http_api_macros -p lily_injection -p lily_shutdown -p lily_websocket --offline --locked -- --quiet
cargo +1.96.1 test -p lily_http_api request_lifecycle:: --lib --offline --locked -- --quiet
cargo +1.96.1 clippy -p lily_http_api -p lily_middleware -p lily_web_core --all-targets --offline --locked -- -D warnings
cargo +1.96.1 fmt -p lily_http_api -p lily_middleware -p lily_web_core --check
cargo +1.96.1 doc -p lily_http_api -p lily_web_core -p lily_middleware --no-deps --offline --locked
# Temporary fixture manifest/lockfile; actual source remains in the workspace:
cargo +1.96.1 check --manifest-path /tmp/lily_http_phase6_downstream/Cargo.toml --target-dir target --offline
cargo +1.96.1 fmt --manifest-path /tmp/lily_http_phase6_downstream/Cargo.toml --check
git diff --check -- crates/framework/lily_http_api crates/foundation/lily_middleware crates/foundation/lily_web_core tests/fixtures/downstream_http
```

Live HTTP/TLS tests require loopback permission; the initial restricted-sandbox
listener failures passed with that permission, without relaxing expectations.
No test requires preemption of blocking code, inferred rollback, automatic
adoption of raw spawned work or completion of every hook. Missing child release
remains outstanding; a terminal but failed cleanup remains failure. The complete
immutable HTTP report remains Phase 8; Phase 7 below adds dependency/telemetry reconciliation.

## Implemented Phase 7 dependency qualification

Twenty new cases exercise dependency boundaries (fourteen HTTP, five tracing,
one DI). Two existing cases now assert registry-driven late scope finalization
and actual tracing worker joins as well.

| Exact test suffix | Invariant qualified |
| --- | --- |
| `force_before_any_component_still_closes_owned_di_once` | Pre-existing force cannot bypass owned DI or invoke its disposer twice. |
| `transport_receipt_is_a_barrier_before_owned_di_close` | An actual pending protocol task keeps DI open until its join. |
| `outstanding_transport_blocks_di_and_replay_cannot_start_it_after_h` | R/H expiry leaves DI unstarted; a later close replays failure without a new attempt. |
| `joined_monitor_panic_is_failure_but_allows_dependency_cleanup` | Joined failure permits safe parent cleanup and remains a failed shutdown. |
| `monitor_future_drops_before_di_begins` | Monitor destruction and join precede DI disposal. |
| `abort_request_cannot_authorize_di_while_monitor_destruction_is_pending` | A blocked real destructor preserves outstanding join evidence after abort; DI cannot start. |
| `pending_root_disposer_uses_d_and_never_replays_as_success` | Pending root disposal is bounded by D; later release cannot retry or erase failure. |
| `cancelled_di_waiter_and_force_share_the_original_close_receipt` | Dropping the normal observer leaves exactly the same DI task/result for force. |
| `returned_disposal_error_is_terminal_and_stays_failed_on_replay` | Disposal failure is distinct from unknown termination and survives replay. |
| `dropped_build_rollback_waiter_keeps_the_dependency_receipt` | Build failure cleanup owns its joins after the build caller stops waiting. |
| `failed_build_rollback_cannot_multiply_the_total_budget` | DI and outer build rollback use one absolute total allowance. |
| `dropping_pending_http_build_preserves_owned_di_rollback` | Cancelling a real pending middleware constructor transfers the container to tracked rollback. |
| `cancelled_build_guard_retains_the_exact_pending_di_transaction` | A pending DI initializer is cancelled before its exact rollback transaction/join is awaited. |
| `owned_di_final_event_is_flushed_before_http_reports_terminal` | Real owned DI final event reaches the JSONL file before HTTP close returns success. |
| `blocking_worker_timeout_keeps_its_real_join_until_release` | Actual started blocking work survives abort/observer loss in the production tracing inventory; join is confirmed only after release. |
| `closing_file_queue_does_not_claim_a_blocked_thread_join` | Closing all writer clones cannot manufacture thread termination while file I/O is blocked. |
| `evidence_alone_retains_the_join_after_every_component_waiter_is_dropped` | Report publication needs no detached driver; the actual owner join remains independently observable. |
| `metric_ack_timeout_does_not_claim_the_worker_thread_join` | A returned SDK flush timeout is not termination of a pending exporter thread. |
| `owned_metric_reader_collects_real_sdk_metrics_and_joins_on_shutdown` | Real SDK aggregation still exports metrics; shutdown acknowledges and actually joins the owned thread. |
| `adapter_rollback_reserve_uses_the_original_failure_timestamp` | DI rollback starts at failure/cancellation, reserves its outer tail and exposes quiescence only after its join. |

HTTP sources: [root tests](src/app/lifecycle_tests.rs),
[real telemetry integration](tests/shutdown_dependencies.rs), and
[body/input tests](src/request_lifecycle/body_tests.rs) (the strengthened
`escaped_input_receipt_blocks_scope_and_preserves_wait_failure` now relies on
`RequestRegistry::wait`, without manually calling the context finalizer).
Shared sources: [tracing lifecycle](../../integrations/lily_trace/src/lifecycle.rs),
[worker receipts](../../integrations/lily_trace/src/runtime/tasks.rs),
[file worker](../../integrations/lily_trace/src/runtime/file_worker.rs),
[metrics adapter](../../integrations/lily_trace/src/runtime/metric_reader.rs),
[DI rollback](../../foundation/lily_injection/tests/build_rollback_deadline.rs), and
[tracing owner integration](../../integrations/lily_trace/tests/tracing_runtime_owner.rs).

The previous caller-DI, exact scope generation, pending disposer, HTTP/2 child
join, static-file and interrupted middleware suites remain part of regression.
No remote collector export is claimed by the metric adapter tests: the SDK
aggregation is real, and the exporter is controlled for deterministic blocking.
The live OTLP collector test remains explicitly ignored unless its environment
is provided. Arbitrary blocking code and thread-local destructors cannot be
preempted. SDK/network internals and client/collector delivery are not inferred
from worker joins.

Phase 7 verification commands (locked/offline, Rust 1.96.1):

```sh
cargo +1.96.1 test -p lily_http_api -p lily_trace -p lily_injection -p lily_shutdown -p lily_websocket -p lily_web_core -p lily_middleware --offline --locked
cargo +1.96.1 clippy -p lily_http_api -p lily_trace -p lily_injection --all-targets --offline --locked -- -D warnings
cargo +1.96.1 fmt --check
cargo +1.96.1 doc -p lily_http_api -p lily_trace -p lily_injection --no-deps --offline --locked
```

Phase 7 verification (2026-09-06): **1,246 passed, 0 failed, 22 ignored**
across 66 suites in the seven crates above. The ignored cases retain their existing
collector/environment/benchmark restrictions. Three pre-existing WebSocket
unused-code warnings remain outside this phase. The three changed crates passed
Clippy with `-D warnings`, workspace formatting, rustdoc generation and whitespace
checks. All 64 local shutdown-document links and all 20 new test-name references
were checked.

The downstream HTTP fixture compiled with its original facade-only source and
the previously normalized temporary manifest/lockfile. Its repository lockfile
remains unchanged; the temporary graph resolves the same package versions as
the workspace lockfile.

The SDK custom-reader feature is the only dependency configuration change;
package versions and the workspace lockfile are unchanged. Phase 8 report/health
aggregation is qualified below; Phase 9 complete production qualification remains pending.

## Implemented Phase 8 report qualification

Thirteen new HTTP cases qualify aggregate accounting, immutable attempt
publication and bounded health observation. Existing middleware/scope/root/
tracing and file-export tests gained report assertions as described below.

| Exact new test suffix | Invariant qualified |
| --- | --- |
| `retired_failures_stay_in_lifetime_diagnostics_outside_the_attempt` | A disposed historical request failure remains visible but does not poison a later HTTP shutdown. |
| `active_failed_scope_is_terminal_failed_not_outstanding_or_http_error` | Cohort disposal failure remains failed with zero outstanding owners; handler return is a separate result. |
| `concurrent_observers_move_each_cohort_identity_to_totals_once` | Thirty-two requests and eight concurrent observers preserve identity counts, scope joins and normal returns across retirement. |
| `candidate_and_rejected_admission_are_not_double_counted_as_requests` | A candidate rejected after the gate closes and a pre-registration refusal do not manufacture accepted execution or extra owners. |
| `caller_scopes_never_enter_the_http_attempt_inventory` | An unrelated caller scope remains open and is absent from exact HTTP scope/dependency counts. |
| `handler_return_does_not_imply_its_scope_is_terminal` | Pending actual disposal remains outstanding after execution returns. |
| `report_keeps_handler_return_body_handoff_and_source_termination_separate` | A polled streaming source remains outstanding after handoff; force reports interruption, actual release and scope cleanup independently. |
| `report_records_an_unpolled_forced_source_as_not_started` | An unpolled source is released without claiming partial execution or completed production. |
| `http_report_distinguishes_root_abort_request_from_its_confirmed_join` | Abort-requested root stays outstanding until actual join; cancelled join is terminal failure. |
| `health_observer_finalizes_a_join_after_the_close_waiter_and_app_are_gone` | Health retains values and the real root receipt without keeping the completed App/DI graph alive; no receipt driver is spawned. |
| `completed_root_health_receipt_can_be_observed_without_a_tokio_context` | A synchronous external health reader observes completed values/join without polling live cleanup timers. |
| `report_and_health_keep_failed_disposal_separate_from_terminal_resources` | Failed owned DI is distinct from completed owner task/quiescence; safe reason codes and health generation survive replay. |
| `a_first_late_health_observation_cannot_claim_a_join_within_the_deadline` | A first join observation after H cannot manufacture timely completion; terminal failure and timeout remain frozen. |

Sources: [request report tests](src/request_lifecycle/report_tests.rs),
[body report tests](src/request_lifecycle/body_tests.rs), and
[root/health report tests](src/app/lifecycle_tests.rs).

Strengthened existing evidence:

- `partial_normal_after_does_not_reopen_the_returned_inner_frame` asserts
  normal-returned, interrupted-after and reverse termination counts separately.
- `root_cleanup_cutoff_stops_only_the_retained_di_generation_and_preserves_failure`
  asserts a terminal-but-timed-out exact scope in the frozen report.
- `late_root_join_cannot_rewrite_an_already_frozen_timeout` compares the whole
  frozen report before/after late join and checks that health stays incomplete.
- `owned_di_final_event_is_flushed_before_http_reports_terminal` checks real
  JSONL telemetry's preliminary checkpoint, final health reasons and replay.
- Tracing `evidence_alone_retains_the_join_after_every_component_waiter_is_dropped`
  and `owner_panic_is_replayed_without_false_success` assert actual returned/
  panicked adapter joins independently of exporter report success.

Historical request-local failure tests now expect a clean later HTTP close
when the failed request fully retired before its shutdown cohort was fixed.
They still assert the original failed/timed-out/unknown scope or middleware
result; active-cohort and frozen-attempt failures remain failures. Owned DI's
own canonical close result is never discarded by this HTTP cohort rule.

Reports contain fixed-size counts/codes, not request URLs, IDs, provider errors
or user labels. No `headers_sent` or remote-delivery evidence is invented.
Raw user tasks and unrelated caller scopes are outside the HTTP inventory.
Owned telemetry cannot export its final root-join result after closing; the
pre-close event is explicitly preliminary and final evidence stays in health.

Phase 8 verification commands (Rust 1.96.1, locked/offline):

```sh
cargo +1.96.1 test -p lily_http_api -p lily_trace -p lily_injection -p lily_shutdown -p lily_websocket -p lily_web_core -p lily_middleware --offline --locked
cargo +1.96.1 clippy -p lily_http_api -p lily_trace --all-targets --offline --locked -- -D warnings
cargo +1.96.1 fmt --check
cargo +1.96.1 doc -p lily_http_api -p lily_trace --no-deps --offline --locked
```

Phase 8 verification (2026-09-06): **1,259 passed, 0 failed, 22 ignored**
across 66 suites in the seven crates above, including all **13 new HTTP tests**.
The final HTTP library suite has **309 passed and 1 existing ignored case**.
The ignored cases retain their existing collector/environment/benchmark
restrictions. No live OTLP collector export is claimed. The three pre-existing
WebSocket unused-code warnings remain outside this phase.

HTTP/tracing Clippy with `-D warnings`, workspace formatting, rustdoc generation,
whitespace checks and the facade-only downstream HTTP fixture all passed.
The downstream check reuses the previously normalized temporary manifest and
lockfile; repository/workspace lockfiles and package versions remain unchanged.
All 70 local shutdown-document links and all 13 new test names were checked.
Local HTTP/1, HTTP/2 and TLS tests ran with socket access. Disk exhaustion during
an earlier build was resolved by cleaning only reproducible HTTP target outputs;
no source or working-tree changes were removed.

The final seven-crate run and verification logs for this workspace session are
`/tmp/lily_http_phase8_qualified_final.log`, `/tmp/lily_http_phase8_clippy_final.log`,
`/tmp/lily_http_phase8_doc_final.log`, `/tmp/lily_http_phase8_downstream.log` and
`/tmp/lily_http_phase8_fmt_final.log`. This is the historical Phase 8 checkpoint.

## Production scenario contract

Phase 2 qualifies the task/root/deadline portions of Q01, Q10–Q11, Q24–Q25,
Q28–Q29 and Q32 above. Phase 3 qualifies the request/DI ownership portions of
Q03–Q04, Q10, Q13, Q19, Q21–Q22, Q24–Q26, Q29, Q32–Q33 and Q35, including
pending/panicked disposal, caller DI and real connection loss. Phase 4 adds
admission/execution coverage for Q03–Q05, Q09–Q16, Q21–Q23 and Q36, with real
protocol and generated callback tests above. Phase 5 adds body/input/helper and
protocol evidence for Q02, Q04–Q10, Q20–Q21, Q29–Q31 and Q34 as described above.
Phase 6 adds middleware-specific Q14–Q19, Q22, Q30, Q33 and Q36 evidence above,
including actual route composition, independent retained cleanup and body/input
prerequisites. Phase 8 adds report/health evidence for Q05–Q06, Q24–Q29 and
Q32–Q33 through the sources above. Phase 9 combines these boundaries through
the managed runtime. The following table preserves the scenario requirements;
the Phase 9 evidence map below supplies the executable tests and their limits.
An entire family of possible schedules is not proved by one loopback test.

| ID | Production scenario | Required invariant / evidence | Primary phases |
| --- | --- | --- | --- |
| Q01 | Idle server graceful shutdown | Listener, monitor, root and owned dependency receipts terminate once; empty request inventory is real. | 2, 7, 8 |
| Q02 | Active buffered response | Last frame, body source release and request scope close are separate; protocol bytes do not retain scoped callbacks. | 5, 7 |
| Q03 | Active action at graceful shutdown | Accepted execution continues normally within the graceful cutoff; request owner remains registered. | 3, 4 |
| Q04 | Request body pending | Input wait observes cancellation; moved input readers remain owned until released; no parent cleanup races input users. | 3–5 |
| Q05 | Response head not handed off | Interrupted creation cannot be reported as sent/completed; any unsent response follows the actual protocol seam. | 4, 5, 8 |
| Q06 | Head handed off, body pending | Body truncation is reported separately; handoff is not header commit/client delivery; no replacement response is emitted. | 5, 8 |
| Q07 | Finite/infinite streaming response | Owner survives handler return; bounded demand/backpressure; graceful cutoff then cooperative producer stop before scope disposal. | 5, 7 |
| Q08 | SSE and bounded SSE channel | Supported SSE drains only within existing write/root cutoffs; keep-alive does not extend them; no implicit adoption of user producers. | 5 |
| Q09 | HTTP/1.1 keep-alive/pipelining | New post-gate requests do not run middleware/actions or open scopes; accepted response can drain; unrelated future requests are rejected/closed. | 4, 5 |
| Q10 | HTTP/2 concurrent streams and GOAWAY | Late streams hit the application gate; admitted streams are independent; every actual Hyper executor task is joined/retained. | 2, 4, 5, 7 |
| Q11 | Graceful deadline expires | One H; graceful cutoff leaves cooperative, stop, cleanup, dependency and final-receipt reserves; no per-request restart. | 2, 4, 7 |
| Q12 | Handler responds to cancellation | Signal arrives before stop, same future is polled and returns in the cooperative window, normal around return remains possible. | 4 |
| Q13 | Handler ignores cancellation | Only its slot stops after the cooperative cutoff; lifecycle owner/ledger/scope receipt survive. Include a non-yielding case reported outstanding. | 3, 4 |
| Q14 | Force during middleware before | Unpolled future creates no enter; first-polled entered prefix is retained; cleanup is serial reverse after prerequisites. | 4, 6 |
| Q15 | Force during handler | Guard/extractor/action execution terminates before eligible middleware termination; no invented guard/action exit callbacks. | 4, 6 |
| Q16 | Force during normal after | Already returned inner frames are skipped; interrupted frame and pending outer frames terminate in reverse without duplicate side effects promised. | 4, 6 |
| Q17 | Termination hook pending forever | One child cap is bounded by owner/root; next eligible sibling has independent authority and remaining time; zero budget leaves NotStarted. | 6 |
| Q18 | Termination hook or destructor panic | Contained failure remains a failed outcome; eligible outer hooks still run if resource termination is proven and budget remains. | 6 |
| Q19 | Request-scope disposer pending | Exact-generation receipt survives timed-out waiter; actual disposal termination and disposal success are distinct. | 3, 6, 7 |
| Q20 | Transport/write pending during drain | Write watchdog remains effective; producer termination can be requested without another data poll; HTTP/2 fallback reports sibling impact. | 5, 7 |
| Q21 | Client disconnect plus server shutdown | One owner finalization, stable observed reason, no race between live execution and cleanup; transport loss cannot destroy lifecycle evidence. | 3–7 |
| Q22 | Ordinary request timeout plus shutdown | Local deadline is clamped, never reset; signal/stop/cleanup paths converge once; typed timeout response and actual cleanup outcome remain separate. | 4, 6 |
| Q23 | New request versus admission close | Either atomically registered as accepted or rejected with no scope/user execution; test TCP and existing HTTP/1/HTTP/2 connections. | 2, 4 |
| Q24 | Many concurrent requests | Bounded owner/task/body inventories and permits; retired records reconcile exactly once; helper admission is tracked before execution. | 2, 5, 8 |
| Q25 | Abort request versus confirmed join | Real Tokio task held pending across abort; outstanding until actual join, completion-winning-abort race preserved, no detached handle. | 2–4, 7, 8 |
| Q26 | Caller-owned DI container | HTTP closes/tracks its exact scopes but does not close the caller container or count unrelated caller scopes. | 3, 7 |
| Q27 | Owned/external telemetry shutdown | Shutdown is attempted only after actual users terminate; blocking exporter/file worker timeout retains its join; external ownership preserved. | 7, 8 |
| Q28 | Shutdown report reconciliation | Identities/counters balance, terminal-but-failed differs from outstanding, late join cannot rewrite frozen attempt or erase previous failure. | 8 |
| Q29 | Detached framework task leak | Listener, connection, Hyper, request owner, helper, DI and trace receipt drivers are all reconciled/retained. Raw user tasks explicitly excluded. | 2, 5, 7–9 |
| Q30 | HEAD/204/304 and unpolled source | Source can be disposed without first poll; NotStarted is explicit; source release is still required before scope close. | 5, 6 |
| Q31 | Static-file blocking work | Open/read helper result/termination is retained when request waiter/producer drops; already started blocking work is not reported aborted by request alone. | 5, 7 |
| Q32 | Dropped start/close waiter, never-started App, failed start | Canonical root/rollback receipt survives; one deadline, dependency ownership and final joins hold for every supported path. | 2, 7, 8 |
| Q33 | Typed guard/extractor/action/middleware error | Normal returned error is distinct from framework interruption; normal middleware error shaping is preserved; no spurious termination replay. | 6, 8 |
| Q34 | Input body transferred into output stream | Ownership follows the reader into the source; input/header handler completion cannot authorize early scope disposal. | 5 |
| Q35 | Scope identity reuse | Receipt is captured before cancellation and stays bound to its concrete generation, including late cleanup and reused textual ID. | 3, 7 |
| Q36 | Nested invocation and callback compile contracts | Same middleware instance at multiple chain positions gets distinct entries; reverse order/duplicate protection; read-only views and correct phase extraction compile. | 4, 6 |

Protocol tests must use actual Hyper HTTP/1.1 and HTTP/2 execution paths, not
invented connection counters. SSE and static files are supported and belong in
the matrix. Use explicit barriers and paused time for deterministic deadline
races; keep real loopback coverage for framing, flow control and disconnects.
Do not require an unsupported HTTP feature just to mirror WebSocket tests.

Phase 9 release checks include affected shared-crate and downstream
compile fixtures, not just `lily_http_api`. A green contract matrix does not
guarantee preemption of blocking application code, rollback of side effects,
raw user-task cleanup, successful network delivery or telemetry export.

## Phase 9 combined runtime evidence

M01–M13 are exact test names in
[src/app/qualification_tests.rs](src/app/qualification_tests.rs). They build an
App, bind a real local listener, use generated controllers/guards/extractors and
middleware, initiate its canonical shutdown and observe the actual joined root
report. No test creates fake request counts or equates a client task abort with
server termination. Every client/observer join is consumed. Explicit barriers
coordinate entry, headers, body progress and cleanup; HTTP/2 flow control holds
the file source live without assuming kernel buffer sizes.

| ID | Exact test name | Combined evidence |
| --- | --- | --- |
| M01 | `managed_idle_http1_and_http2_reconcile_actual_root_and_owned_di` | Both protocol listeners, empty request cohort, real listener/root/monitor joins, owned DI; normal owner Drop preserves graceful classification. |
| M02 | `managed_graceful_action_drains_buffered_response_before_application_di` | Gate closure leaves accepted action uncancelled; response bytes, reverse normal after, ledger release, scope then application disposal and final report. |
| M03 | `managed_graceful_deadline_polls_the_same_cooperative_action_to_return` | G signals the same generated action; it returns and normal after runs inside the original H; no spurious termination callback. |
| M04 | `managed_force_preserves_generated_before_guard_extractor_action_and_after_ledgers` | Five interruption positions through real HTTP; signal before slot release; reverse entered-prefix termination; returned inner after skipped; cleanup before scope/DI. |
| M05 | `managed_cleanup_timeout_and_panic_keep_outer_authority_and_report_terminal_failure` | Pending and panicked inner hook, two independent outer invocations, actual resource joins, terminal failure retained on replay. |
| M06 | `managed_finite_stream_keeps_scope_after_headers_then_drains_normally` | Handler returned, real headers/first frame arrived, source/scope still live; graceful EOF/source release precedes DI. |
| M07 | `managed_force_truncates_stream_and_sse_without_replaying_normal_middleware` | Actual HTTP/1 stream/SSE truncation after headers; no replacement response or repeated normal middleware; body interruption distinct from resource terminal evidence. |
| M08 | `managed_http2_concurrent_actions_drain_and_late_stream_cannot_enter` | Sixteen admitted streams on one connection, late stream denied by GOAWAY or application gate, independent scopes, actual Hyper worker joins and exact aggregate counts. |
| M09 | `managed_pending_input_and_input_moved_into_output_release_before_scope` | Incomplete real request input while extracting and after moving its reader into response production; force releases both before scope. |
| M10 | `managed_pending_scope_disposer_times_out_with_confirmed_release_and_terminal_failure` | Root U expires a pending disposer; actual destructor/join permits parent close but the original scope timeout remains `TerminalFailed`, never successful disposal. |
| M11 | `managed_caller_container_tracks_only_http_scopes_and_leaves_unrelated_scope_open` | Real request scope closes; unrelated caller scope and caller container stay open and outside the HTTP report. |
| M12 | `managed_static_file_http2_drain_reconciles_real_open_and_read_helpers` | File response remains active under HTTP/2 flow control when admission closes; actual open/read helper joins and scope release precede application close. |
| M13 | `managed_peer_disconnect_racing_shutdown_finalizes_each_entered_frame_once` | Real peer loss overlaps root shutdown; one request/cancellation/DI identity and one reverse termination per entered frame. |

M14 is the strengthened isolated-process
[`owned_di_final_event_is_flushed_before_http_reports_terminal`](tests/shutdown_dependencies.rs).
It drains an active real request, reads JSONL evidence of request disposal before
application disposal before the preliminary checkpoint, and observes final
health only after owned telemetry shutdown. It does not use a live OTLP collector.

The managed tests discovered and fixed completed-server Drop changing graceful
intent, and HTTP force waits being capped by the generic D/T tail before C/U/R.
The [shared coordinator tests](../../foundation/lily_shutdown/src/framework.rs)
`owner_phase_force_cutoff_preserves_an_early_cooperative_window` and
`owner_force_cutoff_is_absolute_clamped_and_default_policy_stays_bounded` verify
absolute owner reservations, hard clamping, zero-budget NotStarted, replay and
unchanged default policy. The graceful-G path now preserves its actual reason.

The regression run also exposed an existing DI test fixture race: the main
worker could call `notify_waiters` before the disposer registered its wait.
[The fixture](../../foundation/lily_injection/tests/container_shutdown.rs) now enables that
wait before announcing entry. The DI production implementation is unchanged by
this fixture correction.

## Final Q01–Q36 evidence map

The M cases above run through the full managed application. Named supporting
tests below isolate the actual owning boundary where a particular fault must
be controlled; they are not substitutes for protocol coverage. This distinction
is especially necessary for non-yielding destructors, exact scope-ID reuse,
expired deadlines and blocking OS/provider workers.

| Scenario | Managed / protocol evidence | Controlled boundary evidence and qualification limit |
| --- | --- | --- |
| Q01 | M01 | Root/monitor joins and explicit never-started close in [app lifecycle tests](src/app/lifecycle_tests.rs). |
| Q02 | M02 | [Body tests](src/request_lifecycle/body_tests.rs): `protocol_bytes_cannot_retain_scoped_user_destructors`; copied bytes may remain in transport after source/scope release, with transport joins still required. |
| Q03 | M02 | M08 adds concurrent generated actions; no cancellation solely at admission closure. |
| Q04 | M09 | `escaped_input_receipt_blocks_scope_and_preserves_wait_failure` in body tests covers an escaped input receipt that deliberately blocks parent cleanup. |
| Q05 | M04 | `report_records_an_unpolled_forced_source_as_not_started` in body tests; request return/body outcome and head handoff are separate counters. |
| Q06 | M07 | `report_keeps_handler_return_body_handoff_and_source_termination_separate` in body tests; neither handoff nor collected test bytes is a general delivery guarantee. |
| Q07 | M06–M07 | `body_uses_the_admitted_deadline_and_shutdown_cannot_restart_its_window` and `pending_producer_observes_signal_without_another_hyper_poll` in body tests exercise deadline/backpressure control. |
| Q08 | M07 | `sse_keep_alive_remains_bounded_and_releases_its_captured_source` plus [SSE codec/channel tests](../../foundation/lily_web_core/src/response/sse.rs); raw user producers remain outside ownership. |
| Q09 | M02 and real `http1_keep_alive_and_new_connections_cannot_bypass_closed_admission` in [cancellation tests](src/request_lifecycle/cancellation.rs) | [HTTP/1 conformance](src/server/http1_conformance_tests.rs) covers keep-alive/pipelined parsing. Protocol shutdown and the atomic gate are separately exercised; no promise to accept a pipelined successor during drain. |
| Q10 | M08, M12 | [HTTP/2 tests](src/server/http2_transport_tests.rs): `graceful_shutdown_sends_goaway_and_drains_the_accepted_stream`; [executor ownership](src/server/task_ownership_tests.rs): `actual_http2_workers_are_registered_in_connection_and_root_before_user_poll`. |
| Q11 | M03, M10 | [Deadline tests](src/shutdown.rs) and the two new shared-coordinator tests qualify exact cutoffs with paused time. |
| Q12 | M03 | `force_notifies_before_drop_and_allows_a_controlled_normal_return` in cancellation tests covers explicit force returning during C. |
| Q13 | M04 | [Request owner tests](src/request_lifecycle/tests.rs): `an_in_progress_execution_destructor_blocks_scope_close_and_terminal_evidence`; controlled non-yielding release and [task tests](src/tasks/tests.rs) prove outstanding receipts, not preemption. |
| Q14 | M04 before case | [Middleware tests](src/request_lifecycle/middleware_tests.rs): `interrupted_before_only_arms_the_entered_prefix`; cancellation tests also cover an entirely unpolled callback after a spent window. |
| Q15 | M04 guard/extractor/action cases | Generated callback signatures are also exercised by [public integration tests](tests/execution_cancellation.rs); no guard/action cleanup hook is invented. |
| Q16 | M04 after case | `partial_normal_after_does_not_reopen_the_returned_inner_frame` in middleware tests checks aggregate stage evidence. |
| Q17 | M05 pending case | `pending_hooks_share_one_owner_cutoff_and_report_unstarted_outer_frame` and `exhausted_root_cleanup_budget_never_polls_hooks_or_renews_sibling_budgets` in middleware tests. |
| Q18 | M05 panic case | `cleanup_future_drop_panic_blocks_parent_hooks_and_scope_disposal` in middleware tests controls the unconfirmed destructor boundary; async body panic and destructor panic differ. |
| Q19 | M10 | `root_cleanup_cutoff_stops_only_the_retained_di_generation_and_preserves_failure` in request owner tests; a pending close waiter alone cannot classify the underlying disposer. |
| Q20 | M07, M12 | Managed stream-local deadline/stop and pending final flush are covered by the timeout Session 2 tests below; `unbound_graceful_drain_keeps_connection_fallback_and_accounts_for_cancelled_h2_sibling` and `unbound_service_write_deadline_uses_connection_fallback_without_leaking_the_stream` separately qualify raw codec fallback. |
| Q21 | M13 | `http2_connection_loss_retains_each_concurrent_request_owner` in request owner tests and cancellation first-reason tests cover multiplexing and stable reason. |
| Q22 | [Public timeout integration](tests/execution_cancellation.rs) | `repeated_stop_requests_cannot_extend_the_cooperative_window_or_reason` in cancellation tests and `request_timeout_keeps_cleanup_independent_and_waiter_loss_does_not_abort_hooks` in middleware tests qualify convergent stop/cleanup. |
| Q23 | M08 and HTTP/1 gate test from Q09 | `concurrent_admission_closure_balances_accepted_and_rejected_identities` in cancellation tests proves atomic publication/capacity under controlled races. |
| Q24 | M08 | [Report tests](src/request_lifecycle/report_tests.rs): `concurrent_observers_move_each_cohort_identity_to_totals_once`; owners/permits remain retained until actual terminal evidence. |
| Q25 | Actual joins asserted by every M case | `abort_before_first_poll_stays_outstanding_until_the_actual_join` and `blocked_destructor_preserves_the_join_after_the_root_cutoff` in task tests; app lifecycle tests cover root abort versus observed join. |
| Q26 | M11 | Report tests prove unrelated caller scope IDs never enter the attempt inventory. |
| Q27 | M14 | [Tracing runtime owner test](../../integrations/lily_trace/tests/tracing_runtime_owner.rs), [runtime task receipts](../../integrations/lily_trace/src/runtime/tasks.rs), [file worker](../../integrations/lily_trace/src/runtime/file_worker.rs) and [metric reader](../../integrations/lily_trace/src/runtime/metric_reader.rs) qualify retained actual workers; live collector/network export remains unqualified. |
| Q28 | M01–M14, including terminal failures | [App report tests](src/app/lifecycle_tests.rs) and request report tests cover frozen replay, late observations, health and historical/cohort separation. |
| Q29 | M08 actual Hyper children, M12 actual file helpers, M14 actual telemetry, all M final inventories | Task/DI/trace fault tests retain blocked receipts. Zero detached framework work is asserted for terminal runs; arbitrary blocked work may remain owned and reported outstanding. Raw user tasks are excluded. |
| Q30 | Real `http1_suppressed_sources_release_unpolled_before_scope_on_keep_alive` in body tests | [Server tests](src/server/server.rs) cover response suppression and framing; an unpolled source need not be first-polled for cleanup. |
| Q31 | M12 | [Resource tests](../../foundation/lily_web_core/src/http_resources/tests.rs): `cancelled_caller_retains_started_blocking_join`; actual OS scheduling is controlled at the helper receipt, not inferred from a dropped request. |
| Q32 | Actual App tests in [app lifecycle tests](src/app/lifecycle_tests.rs) | Concurrent/unstarted close, failed bind, dropped start/close, pending build rollback, late join and health without an observer/runtime all retain the canonical receipt and result. |
| Q33 | [Typed request integration](tests/typed_parts_executor.rs) and [generated cancellation integration](tests/execution_cancellation.rs) | `typed_errors_short_circuit_and_normal_completion_do_not_invoke_termination` in middleware tests and retired-error report tests preserve normal error semantics. |
| Q34 | M09 echo case | `input_ownership_follows_reader_into_returned_body_until_source_drop` in body tests isolates the moved-reader barrier. |
| Q35 | M11 exact HTTP scope ownership | `a_late_scope_observer_cannot_attach_to_a_reused_process_id` in request owner tests controls generation reuse unavailable through arbitrary wire IDs. |
| Q36 | M04 and M08 generated three-position middleware | `actual_application_controller_action_chains_share_one_ledger_per_concurrent_request` in middleware tests, [controller UI tests](tests/struct_controller_ui.rs), [read-only cancellation compile tests](../../foundation/lily_web_core/src/cancellation.rs), and the [downstream facade](../../../tests/fixtures/downstream_http/src/main.rs). Duplicate controller/action middleware types remain a build error. |

No row claims preemption of a blocking poll/destructor, rollback, arbitrary async
cleanup completion, delivery of every response byte, cleanup of raw user tasks
or live collector availability. Outstanding resources remain owned/reportable;
their parents cannot be marked safely disposed merely because a deadline or
abort request fired. Stress/production scheduling and remote exporter operation
remain deployment-level validation, not an expanded library guarantee.

## Phase 9 verification result

Verified on **2026-09-07**, using Rust **1.96.1** and the current working tree:
**1,293 passed, 0 failed, 22 existing ignored cases across 68 suite summaries**.
The HTTP library result is **322 passed, 0 failed, 1 existing ignored case**.
The new coverage comprises 13 managed HTTP tests (with multiple stage/protocol
cases), two shared-coordinator tests, and the strengthened active-request
telemetry and downstream/DI fixtures. The eight-crate command was:

```sh
cargo +1.96.1 test -p lily_http_api -p lily_http_api_macros -p lily_trace \
  -p lily_injection -p lily_shutdown -p lily_websocket -p lily_web_core \
  -p lily_middleware --offline --locked
```

Clippy passed with `--all-targets -- -D warnings` for HTTP, HTTP macros,
shutdown, tracing, DI, middleware and web-core. Rustdoc for HTTP, HTTP macros,
shutdown and tracing passed with `RUSTDOCFLAGS='-D warnings'`. Workspace format,
downstream-source format and `git diff --check` passed. The facade-only
downstream fixture passed against its existing normalized temporary manifest
and lockfile; it compiles the repository's current source directly. No package
version or repository lockfile changed. All 106 local links in the four shutdown
documents and all 50 named evidence filters in the final matrix resolve.

The final regression log is `/tmp/lily_http_phase9_qualification_final.log`;
supporting logs are `/tmp/lily_http_phase9_clippy.log`,
`/tmp/lily_http_phase9_doc.log`, `/tmp/lily_http_phase9_downstream.log` and
`/tmp/lily_http_phase9_fmt_check.log`. Earlier failing runs are retained as
diagnostics; their failures are not hidden by this final result.

The 22 ignored tests retain their existing environment/collector/benchmark or
documentation restrictions. Local HTTP/1, HTTP/2 and TLS tests ran with socket
access. Three pre-existing WebSocket unused-code warnings remain outside the
changed components. Live OTLP export and arbitrary blocking application code
are not qualified as bounded completion guarantees. Phase 9 is complete.

## HTTP timeout Session 2 — commit and protocol stop control

The [response control tests](src/server/response_control_tests.rs) use the
registered executor and real Hyper HTTP/1 or HTTP/2 codecs. Stream association
comes from the actual worker receipt, including the real managed App/DI test.
An in-memory codec test is not treated as proof of remote TCP delivery.

| Test | Evidence |
| --- | --- |
| `h2_write_timeout_aborts_only_its_worker_and_preserves_active_and_later_siblings` | Actual cancelled worker join, active sibling completion and a later request on the same H2 connection. |
| `h2_eof_handoff_does_not_remove_the_watchdog_while_the_last_frame_waits_for_capacity` | Both zero-byte and one-byte initial windows keep the final payload controlled; a whole chunk queued with partial capacity cannot silently finish the worker. |
| `h2_response_worker_stays_abortable_while_final_io_flush_is_pending` | Both empty and non-empty responses keep a worker through pending I/O flush; abort is joined and a healthy stream works after transport progress resumes. |
| `h2_stop_before_commit_cannot_send_a_second_response_or_claim_an_abort_join` | Stop before response creation forbids commit, repeated reasons do not reopen it, requested abort is initially unconfirmed, then the actual join is observed. |
| `h2_graceful_drain_keeps_local_stop_isolated_from_accepted_siblings` | The watchdog remains active during GOAWAY/drain without cancelling an accepted sibling. |
| `http1_last_frame_still_needs_flush_and_a_stop_releases_the_actual_connection` | A blocked final write leaves flush incomplete; write timeout, request timeout and force each release actual I/O and join the connection task. Their reasons are counted separately; partial bytes are not a complete response. |
| `http1_completed_flush_retires_watchdog_without_closing_keep_alive` | Successful response flush is independent of connection-task termination; an expired old control cannot stop a subsequent request. |
| `managed_h2_worker_abort_keeps_execution_owner_and_scope_alive_until_cooperative_return` | Real localhost App: service-worker abort signals but does not abort the request owner; user execution and scope remain live until cooperative return; siblings and final inventories reconcile. |

The raw codec cases inject final stop timers to test protocol mechanics. Managed
requests now use the shared request deadline qualified in Session 3 below;
aggregate shutdown integration remains separate qualification work.

Verified on **2026-09-07**, Rust **1.96.1**: **381 passed, 0 failed,
15 existing ignored cases** (333 library, 45 integration, 3 documentation
passes). Eight new regression tests include multiple window/response/reason
cases. `clippy --all-targets --all-features -- -D warnings`, package format,
local documentation links and `git diff --check` passed. Verification commands:

```sh
CARGO_INCREMENTAL=0 cargo +1.96.1 test -p lily_http_api --all-features -j 2
CARGO_INCREMENTAL=0 cargo +1.96.1 clippy -p lily_http_api --all-targets --all-features -j 2 -- -D warnings
cargo +1.96.1 fmt -p lily_http_api -- --check
```

Final logs: `/tmp/lily_http_timeout_session2_tests_verified.log` and
`/tmp/lily_http_timeout_session2_clippy_verified.log`. The initial full test build
ran out of disk while linking; old generated HTTP test executables were removed
before the successful runs. No repository source or configuration outside
`lily_http_api` changed in this session.

## HTTP timeout Session 3 — one request and response clock

The [response deadline tests](src/server/response_deadline_tests.rs) use managed
App/DI request owners, the production connection service and actual registered
Hyper workers over in-memory I/O. Paused time verifies absolute boundaries;
these managed cases do not inject per-response write timers. Direct commit/root
tests separately cover the final selection race and late budget installation.

| Test | Evidence |
| --- | --- |
| `a_cooperative_pipeline_can_start_and_finish_its_returned_stream_in_the_same_window` | HTTP/1 and HTTP/2, timeout and force: a stream returned after notification starts and completes within the remaining shared window; the real result survives. |
| `lazy_production_uses_the_spent_handler_budget_and_keeps_its_cooperative_result` | Both protocols: 60ms dispatch plus later production share a 100ms request deadline; notification is at 100ms and the original body completes at 140ms. |
| `body_cannot_restart_the_window_already_spent_in_dispatch` | A pipeline consuming 140ms of cooperation cannot grant its new producer another 250ms. Both protocols stop the incomplete response at the original 350ms cutoff. |
| `incomplete_committed_stream_and_sse_are_truncated_with_protocol_local_scope` | Streaming and SSE retain their committed status and fail the body at the cutoff. Sources/scopes release; HTTP/1 closes, HTTP/2 permits a later healthy request. |
| `buffered_response_clock_survives_owner_retirement_and_zero_h2_capacity` | Handler/scope already terminal, buffered bytes blocked by zero H2 capacity: the original token fires at 100ms and the actual stream worker is aborted/joined at 350ms. |
| `buffered_response_flush_can_finish_after_signal_without_losing_success` | HTTP/1 and HTTP/2 buffered responses blocked at flush finish after cancellation inside the shared window. No abort; later healthy requests survive the old deadline. |
| `incomplete_uncommitted_request_gets_504_or_shutdown_503_and_can_finish_writing` | Both protocols: ignored execution receives bounded 504/503 with a complete error body. A local timeout does not close a healthy connection after the fallback succeeds. |
| `fallback_body_also_has_one_bounded_finalization_attempt` | Zero H2 capacity allows 504 headers but blocks its body: only 100ms finalization is granted, then the actual worker is aborted and joined. |
| `late_uncommitted_selection_keeps_resolved_cors_and_replaces_the_original_response` | Delayed service selection replaces uncommitted 200/body/application headers with minimal 504, preserving resolved CORS/Vary without replaying middleware. Commit and EOF still do not prove flush. |
| `uncommitted_fallback_is_clipped_to_root_and_reselection_cannot_renew_it` | Root transport stop clips the single fallback allowance; repeated selection cannot extend it, and a stop request is not driver release evidence. Session 4 also verifies the P-to-R join reserve. |
| `a_later_shutdown_root_shortens_the_same_pending_response_window` | Local timeout first, later shutdown: C shortens the same response window while the first reason remains RequestTimeout. |

The independent protocol framing/delivery limits documented for Session 2 still
apply. Full timeout/shutdown cohort reconciliation belongs to Sessions 4 and 5;
this session does not interpret frame handoff or abort requests as terminal joins.

Verified on **2026-09-07**, Rust **1.96.1**: **442 passed, 0 failed,
15 existing ignored cases** across HTTP and middleware (344 HTTP library,
45 HTTP integration, 3 HTTP documentation and 50 middleware passes). Eleven
new tests cover multiple protocol/response/cancellation variants. Local socket
HTTP/1, HTTP/2 and TLS tests ran with socket access. Commands:

```sh
CARGO_INCREMENTAL=0 cargo +1.96.1 test -p lily_http_api -p lily_middleware --all-features -j 2
CARGO_INCREMENTAL=0 cargo +1.96.1 clippy -p lily_http_api -p lily_middleware --all-targets --all-features -j 2 -- -D warnings
cargo +1.96.1 fmt -p lily_http_api -p lily_middleware -- --check
git diff --check
```

Final logs: `/tmp/lily_http_timeout_session3_tests_final.log` and
`/tmp/lily_http_timeout_session3_clippy_final.log`. Changes are confined to HTTP
implementation/tests/docs and the middleware interruption variant. Pre-existing
working-tree changes in other crates remain intact. Session 3 is complete;
the current follow-up status is recorded in the roadmap and the next section.

## HTTP timeout Session 4 — shutdown response and resource barriers

The [response shutdown qualification](src/server/shutdown_response_tests.rs)
uses actual managed request owners, DI scopes, Hyper HTTP/1 and HTTP/2 codecs,
registered connection workers and the production listener drain function.
In-memory I/O allows exact paused-time assertions. A separate localhost case
exercises actual `ManagedHttpServer` handle Drop. The
[root recovery tests](src/app/lifecycle_tests.rs) exercise the exact recovery
method used by the panic path and real task-destructor/join barriers.

| Test | Required evidence |
| --- | --- |
| `forced_drain_preserves_buffered_tail_after_request_scope_retirement` | Both protocols: the request scope is already terminal, but blocked output keeps the connection live and DI unstarted. Releasing flush within cooperation delivers all 512KiB with zero connection aborts. |
| `graceful_deadline_and_late_force_preserve_source_and_dependency_order` | Both protocols: ordinary graceful completion, G expiry, and force arriving during drain preserve the complete source result. Exact event order is normal return → source release → scope disposal → application disposal; no abnormal hook replay. |
| `forced_uncommitted_fallback_keeps_its_write_window_and_original_timeout_reason` | Both protocols: real socket writes held after admission remain alive 40ms into fallback finalization. Complete 503 or 504 is required; timeout-then-shutdown retains the original reason and 350ms cutoff, with zero premature aborts. |
| `completed_fallback_transport_cannot_bypass_a_pending_exact_scope_receipt` | Both protocols: a complete 503 and all transport joins still leave one exact scope receipt outstanding. Application disposal and close completion remain blocked until that disposer is released. |
| `three_connections_must_all_join_before_application_dependency_disposal` | Both protocols under one App/root: three buffered responses remain live after scope retirement. The first two actual connection joins cannot authorize parent disposal; only the third join permits it. All three complete payloads and zero connection aborts are required. |
| `exhausted_fallback_and_blocked_connection_stop_before_the_root_join_reserve` | A zero H2 stream window blocks the 503 body; 100ms finalization ends with the exact worker's cancelled join. Blocked connection flush still prevents DI. Final connection abort occurs at P, its join precedes R, and the body is incomplete. |
| `dropped_managed_server_signals_but_does_not_abort_the_response_owner` | Actual localhost HTTP/1 and HTTP/2: dropping the unfinished server handle signals a source that returns cooperatively. Its complete response and listener/connection joins are required, with zero premature listener or connection abort requests. |
| `root_panic_recovery_preserves_cooperation_and_reserves_actual_transport_joins` | Recovery allows an owned cooperative task to return at 40ms without abort. An ignoring task remains un-aborted immediately before P, then supplies an actual cancelled join before R. No dependency close is started by a stop request. |
| `protocol_abort_pending_destructor_blocks_dependencies_and_preserves_incomplete_report` | A real protocol-task destructor is held after abort across R. The task remains outstanding with zero confirmed cancellations; DI is unstarted and the attempt is incomplete. Its later join cannot rewrite that report or start missed disposal. |

The early-abort regression was first run against the original drain code and
failed because application disposal had already started while output was held;
the unchanged ordering assertions passed after the fix. The I/O gate covers
both actual writes and shutdown flushing, rather than assuming that a client
which already received a complete body must keep its connection alive. Test
probes are armed per App despite process-wide injectable discovery. Every armed
probe requires exactly one application disposal and checks actual transport joins
inside that disposer. No test injects a successful join or substitutes a timeout
for confirmed resource termination.

The blocked-destructor case runs its real registered worker on a dedicated test
runtime and retains its real join receipt in the same application inventory.
This isolates the join/dependency barrier from executor starvation; production
does not gain another runtime. An external three-second watchdog only releases
the deliberately blocked destructor to prevent a hung test process: using that
release fails the test. Before the normal release, recovery and close must both
fail with one outstanding task, no confirmed cancellation and no DI disposal.

Async deadlines require the control executor to keep progressing. Raw blocking
user code/destructors cannot be preempted and can also prevent that executor's
timers from advancing. The isolated case qualifies truthful incomplete evidence
when control can progress, not a wall-clock shutdown guarantee for a starved
runtime. Hyper framing, local flush, task join and peer delivery remain distinct.
The final aggregate reporting review and cross-path qualification are timeout
Session 5.

Verified on **2026-09-07**, Rust **1.96.1**: **451 passed, 0 failed,
15 existing ignored cases across 28 suite summaries** (353 HTTP library,
45 HTTP integration, 3 HTTP documentation and 50 middleware passes). All nine
new Session 4 tests ran, including their protocol and cancellation variants;
none is ignored. Local HTTP/1, HTTP/2 and TLS tests ran with socket access.
Clippy with warnings denied, package formatting, `git diff --check`, all 122
local links in README/shutdown documents and the nine new matrix references
passed. Commands:

```sh
CARGO_INCREMENTAL=0 cargo +1.96.1 test -p lily_http_api -p lily_middleware --all-features -j 2
CARGO_INCREMENTAL=0 cargo +1.96.1 clippy -p lily_http_api -p lily_middleware --all-targets --all-features -j 2 -- -D warnings
cargo +1.96.1 fmt -p lily_http_api -p lily_middleware -- --check
git diff --check
```

Final logs: `/tmp/lily_http_timeout_session4_tests_verified2.log` and
`/tmp/lily_http_timeout_session4_clippy_verified2.log`. The initial red regression
is retained in `/tmp/lily_http_timeout_session4_red.log`. Earlier fixture/lint
failures and the interrupted blocking-destructor run are diagnostic logs, not
passing verification. Session 4 changes are confined to HTTP implementation,
tests and documentation; pre-existing changes in other crates remain intact.
Session 4 is complete. Session 5 remains pending.
