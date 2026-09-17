# Consumer lifecycle qualification

The suite exercises production owners and adapters. Transport-free fixtures
replace broker/database I/O at the boundary; they do not replace lifecycle,
deadline, coordinator or receipt logic. Virtual time tests use fixed absolute
cutoffs. Concurrency tests use explicit entry/release/drop barriers. Wall-clock
timeouts are watchdogs, not substitutes for ordering or completion evidence.

## Deterministic qualification

| Scenario | Required evidence | Test location |
| --- | --- | --- |
| Readiness/admission race, partial registration | No published owner before successful open; cancelled registration closes the acquired prefix exactly once | `consumer_runtime_qualification_tests.rs`, queue `queue_service_qualification_tests.rs` |
| Graceful drain with active ACK and buffered deliveries | Basic.Cancel does not close the channel while settlement runs; exact ACK and pending-redelivery conservation | queue `consumer_drain_tests.rs::graceful_cancel_does_not_close_channel_before_accepted_ack_and_buffer_release` |
| Force with mixed concurrency | Three queue generations, nine active executions, six buffered deliveries; cooperative successes ACK, remaining deliveries classified exactly; actual child joins before channel close under one root | queue `consumer_drain_tests.rs::concurrent_queue_generations_share_one_force_budget_and_account_for_every_delivery` |
| Cancellation during before/handler/normal after | Same future keeps polling; completed actual result preserved; sibling deliveries unaffected | queue `delivery_cooperative_qualification_tests.rs` |
| Local timeout overlaps shutdown | First cancellation reason retained; later root only shortens the original pipeline/cleanup limits | `shutdown_during_local_timeout_cooperation_keeps_first_reason_and_shrinks_same_pipeline` |
| Forced execution interruption | Execution drop precedes reverse termination and exact DI disposal; no ACK/retry/DLQ invented | queue `delivery_cooperative_qualification_tests.rs`, `consumer_drain_tests.rs::returned_framework_interruption_is_never_acked_and_only_controlled_force_proves_redelivery` |
| Partial before/after, hook panic, hook pending forever | Only entered prefix eligible; completed after not replayed; sibling cleanup authority survives; panic/timeout/not-started never success | queue `delivery_cooperative_qualification_tests.rs` |
| Cancelled cleanup/drain/force observer | Same ledger, scope receipt and task handles retained; second observer gets no renewed deadline | queue `delivery_lifecycle_tests.rs`, `consumer_drain_tests.rs`, Consumer runtime qualification |
| Transport failure and recovery | Old generation joins before replacement; channel-close failure survives even with joined children | queue `consumer_drain_tests.rs` |
| Task abort vs actual termination | Abort request alone does not open barrier; panic/cancel joins retained after parent/waiter loss | queue `owned_tasks.rs`, Consumer `consumer_owned_tasks.rs` and `consumer_dependencies.rs` |
| Transactions/heartbeat/relay | Same root limits owner and heartbeat; active=0 is insufficient; confirmed publish interrupted during durable marking remains uncertain | queue `owned_tasks.rs`, `outbox_relay.rs`, transactional storage test modules |
| Owned DI and startup rollback | Failed broker close withholds parent disposal; cancelled DI observer retains original receipt; root includes initializer drop and partial disposal | Consumer runtime/build qualification |
| Telemetry and signal failure | Actual signal/telemetry-owner/worker joins; known joined signal error stays failed but cannot prevent safe telemetry cleanup | Consumer `consumer_dependencies.rs` isolated trace test and signal tests |
| Report timing and repeat observers | No final report before runtime join; cancelled public waiter loses no evidence; repeated reads/shutdown cannot rerun cleanup | `stage6_report_waits_for_real_close_and_survives_cancelled_public_observer` |
| Automatic force reporting | Exact 750ms graceful cutoff inside a 1s root; original timeout and successful force both visible | `stage6_deadline_force_is_public_and_retains_original_timeout_evidence` |
| Error vs incomplete reporting | Joined panic remains failed; failed broker close leaves owned DI unstarted; late recovery cannot rewrite report | `stage6_joined_cleanup_panic_remains_failed_after_successful_force`, `stage6_failed_broker_close_reports_unstarted_owned_di_and_incomplete_shutdown`, `shutdown_report.rs` |
| Bounded diagnostics | 12,288 completed deliveries retain at most 4096 details with exact aggregate totals and dropped count; reader lock cannot block settlement | queue `telemetry.rs` qualification tests |

## Live qualification

These fixtures are ignored by default and require explicitly disposable
infrastructure. They must be executed successfully before claiming live
RabbitMQ/PostgreSQL/MongoDB qualification. A compiled fixture is not a broker
ACK, remote commit, transport-close or redelivery receipt.

| Fixture | Production evidence |
| --- | --- |
| `cap_q_06g_rabbitmq_lifecycle` partial startup/recovery | RabbitMQ management consumer/channel/connection counts return to the exact baseline; the same queues can restart without orphan consumers |
| `cap_q_06g_rabbitmq_lifecycle` paused settlement | Consumer count reaches zero while accepted ACK is held; public shutdown observer can be cancelled; post-admission event is never handled and remains on the broker |
| `cap_q_06g_rabbitmq_lifecycle` local timeout | Actual cooperative result ACKs; exact scope cleanup; same subscription subsequently handles another event |
| `cap_q_06g_rabbitmq_lifecycle` shutdown deadline | Cooperative result survives through real ACK and disposal; report includes force and actual queue-close proof |
| `cap_q_06g_rabbitmq_lifecycle` uncooperative shutdown | Cancellation observed, execution dropped, scope disposed in order; no ACK/retry; exact original event redelivered by the broker |
| `canonical_rabbitmq_e2e` | Exact success/retry/poison/panic/timeout routing and DLQ identities/cardinality; buffered/active shutdown redelivery; all scopes already disposed when shutdown returns |
| `postgresql_transactional_consumer_e2e` | Actual transactional inbox/outbox and relay persistence/restart scenarios; retained terminal report proves queue/transaction/relay barrier before caller DI closes |
| `mongodb_transactional_consumer_e2e` | Replica-set transaction/replay/unknown-commit/crash scenarios; shutdown-active case uses local timeout longer than root, verifies ShutdownDeadline then execution drop and scope disposal, with exact broker redelivery |

Best-effort diagnostic details are not authoritative broker evidence. Concurrent
fixtures assert exact `retained + dropped` conservation and validate each retained
identity/outcome, while broker messages and fixed counters independently prove
the business result. Quiescent single-delivery detail checks remain exact.

## Executed live Consumer qualification

The 2026-09-17 working-tree run used dedicated RabbitMQ **4.3.4**, PostgreSQL
**17.10** and MongoDB **8.2.11** (single-member replica set), each pinned to its
recorded image digest and exposed only on dynamically allocated loopback ports.

| Consumer live profile | Passed | Failed | Ignored |
| --- | ---: | ---: | ---: |
| RabbitMQ lifecycle/ownership | 6 | 0 | 0 |
| Canonical RabbitMQ dispatch/settlement | 3 | 0 | 0 |
| PostgreSQL two-process transactional | 1 | 0 | 0 |
| MongoDB two-process transactional, single DI | 1 | 0 | 0 |
| MongoDB two-process transactional, factory DI | 1 | 0 | 0 |

The first canonical run exposed a fixture assertion error: sanitized poison
handoffs preserve the canonical `x-lily-event-id` header but need not preserve
the optional AMQP `message_id`. The corrected assertion verifies both the exact
canonical event identity and original payload bytes, and the entire three-test
target passed afterward. The original failure log is retained alongside the
successful run evidence.

This qualifies the Consumer targets above on these selected versions. It does
not claim the full minimum/maximum support matrix or every ignored storage-level
fault fixture ran. Use the [disposable environment helper](../../../tests/qualification/environments/README.md)
for repeatable setup, separate feature-profile execution and teardown.

## Running

Run from the workspace root with the repository toolchain. Examples for an
already populated offline dependency cache:

```sh
CARGO_INCREMENTAL=0 cargo +1.96.1 test --offline -p lily_consumer --lib -j 2
CARGO_INCREMENTAL=0 cargo +1.96.1 test --offline -p lily_queue --features test-support --lib -j 2
CARGO_INCREMENTAL=0 cargo +1.96.1 test --offline -p lily_queue --features test-support,transactional-inbox-postgresql,transactional-inbox-mongodb --lib -j 2
CARGO_INCREMENTAL=0 cargo +1.96.1 test --offline -p lily_consumer --features transactional-inbox-postgresql-factory,transactional-inbox-mongodb-factory --lib -j 2
CARGO_INCREMENTAL=0 cargo +1.96.1 test --offline -p lily_consumer --test cap_q_06g_rabbitmq_lifecycle -- --ignored --test-threads=1
```

Run queue and Consumer transactional profiles as separate Cargo invocations:
Consumer's database DI features otherwise unify into queue unit fixtures which
intentionally do not configure a live database service. The fixture module
headers specify the required disposable URLs, configuration paths, management
credentials and opt-in flags. Do not substitute a production service. Inspect
the feature-specific live fixture header before executing ignored tests.

Qualification limits remain explicit: blocking/non-yielding user code and
blocking destructors cannot be preempted by an asynchronous deadline; raw user
tasks are not framework-owned. Local joins do not prove remote commit/ACK after
an interrupted operation. Incomplete owners retain their resources and original
failure evidence rather than pretending dependency disposal succeeded.
