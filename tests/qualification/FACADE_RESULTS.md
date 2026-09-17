# Facade qualification — 2026-09-17

Validated on Linux with Rust 1.96.1, using the `bc43fd4` checkout plus the
qualification changes and the completed HTTP shutdown changes in the working
tree. No crate was published. See [the runner guide](FACADE.md) to reproduce the
checks.

## Completed checks

| Area | Result |
| --- | --- |
| Umbrella graph | Empty facade and all 52 public features passed |
| Umbrella compilation | 55 profiles passed: empty, 52 individual features, combined single and factory |
| Umbrella consumers | 30 canonical/renamed/profile runs; 94 Rust tests passed |
| Negative facade contracts | All 10 expected compiler diagnostics matched |
| Component facades | 18 runs; 28 Rust tests passed |
| DI facades and boundaries | 11 runs; 15 Rust tests passed |
| Existing downstream/derive/golden fixtures | 16 compilation/execution checks passed |
| Root workspace, packages selected individually | All 39 packages passed; 2,590 Rust tests passed and 121 remained ignored |
| Connected examples and clients | 4 unit tests passed; all six packages checked; formatting passed |
| API docs | Workspace, combined single facade and combined factory facade builds passed |
| Live example stack | HTTP, WebSocket, Consumer, PostgreSQL, MongoDB, Redis and RabbitMQ checks passed |

The root-package totals include 437 HTTP tests, 563 WebSocket tests and 97
Consumer tests. Their ignored counts are 14, 3 and 9 respectively. Totals count
Rust harness results, including documentation tests; they are not counts of
individual assertions or every UI fixture inside a harness test.

The isolated live run observed two acknowledged deliveries of the same job and
exactly one PostgreSQL job row and processed-event row. Redis TTL was 59 seconds;
the deleted MongoDB note was absent. Trace propagation and lifecycle checks
passed over 216 HTTP, 58 WebSocket and 42 Consumer JSONL records. All three
applications exited with code 0 and the HTTP/WS workers stopped cooperatively.
The temporary Compose project's containers, network and volumes were removed.

## Findings and corrections

- Repaired 55 stale local dependency paths in migrated fixtures and their
  lockfiles. Remaining `lily_base_repository` fixture references now use
  `lily_mongo_repository`. A manifest audit found no missing paths across the
  106 tracked manifests.
- Fixed two ambiguous rustdoc links in `lily_trace`: the links now explicitly
  target the `lily_trace` attribute macro. The original docs build failed;
  all three documentation profiles passed after the correction.
- The initial `cargo test --workspace` invocation failed 40 Consumer unit tests.
  Selecting all members unifies their features and enables the QueueClientService
  singleton in transport-free Consumer fixtures without publisher configuration.
  The unchanged Consumer unit suite passed all 95 tests when selected alone.
  The runner now tests every workspace member individually; no assertions,
  time limits or ignore flags were relaxed. A single combined workspace test
  invocation remains a known limitation, not a passing result.

## Evidence and boundaries

Raw local evidence is retained under ignored directories:

- `target/umbrella-validation/`: graph, consumer, build and negative diagnostics;
- `target/facade-qualification/20260917T150352Z-facc0c81/`: successful component,
  DI and downstream checks, plus the failed combined workspace invocation;
- `target/facade-qualification/20260917T151818Z-e939e718/`: successful per-package
  workspace and example checks, plus the original rustdoc failure;
- `target/facade-qualification/20260917T153603Z-f3c7de05/`: passing documentation
  and isolated live run, resolved Compose config, image identities and cleanup;
- `examples/artifacts/verify-20260917-183755-4120d8/`: client responses, storage,
  shutdown evidence and JSONL traces.

These logs are local artifacts, not published repository contents. Interrupted
runs retain their failed status; the table above combines their completed stages
with the subsequent successful runs. Ignored tests remain ignored. Live OTLP,
TLS/mTLS, transactional-inbox, fault-injection and service-version qualification
profiles were not executed by this facade gate. ClickHouse remains a compilation
and contract check, outside the live examples.
