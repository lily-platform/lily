# Facade qualification

This is the final compatibility and example gate for `lilyrs` and its component
facades. The [2026-09-17 qualification results](FACADE_RESULTS.md) record the
completed checks and remaining limits. Run from the repository root with
Python 3.11+ and Rust 1.96.1:

```sh
python3 tests/qualification/facade.py
# Also build and run the connected applications against real infrastructure:
python3 tests/qualification/facade.py --live
```

The first command needs no live database, broker or collector. Cargo commands
use committed lockfiles and `--offline`; fetch dependencies first on a fresh
machine. Fetch for the root workspace, `examples`, `examples/clients`, the
fixture workspaces under `tests/fixtures`, `umbrella_facades/matrix`, and
`tests/qualification/golden`, each with
`cargo +1.96.1 fetch --locked --manifest-path PATH`. These independent workspaces deliberately do not inherit
the root's complete dependency/feature selection.

`--live` additionally needs Docker and Docker Compose. Docker image and build
dependency downloads may use the network even though host Cargo checks are
offline. The run uses a unique `lily-facade-q-*` project, its own application image
tag, dynamically assigned loopback ports, and new data/trace volumes. The runner
removes only that project's containers, network and volumes in `finally`; it
never uses or stops the normal `lily-examples` project or unrelated databases.
Build images/cache are retained for subsequent runs.

## Stages

| Stage | Checks |
| --- | --- |
| `umbrella` | Empty facade, every native-to-public feature mapping, isolated `lilyrs`/renamed consumers, independent public feature builds, combined single/factory builds, expected compiler diagnostics for disabled APIs and conflicting modes |
| `components` | Direct component facades and aliases, each package tested separately; MongoDB/PostgreSQL/ClickHouse single and factory, Queue AsyncAPI |
| `di` | Standalone DI and framework-root re-exports, aliases, scoped identity/disposal, configuration composition, compile-fail public API boundaries |
| `macros` | Internal dependency publication graph, isolated macro/runtime UI and integration contracts, shared DI rustdoc examples, MongoDB factory and Queue AsyncAPI macro tests |
| `downstream` | Existing external-consumer and derive ABI fixtures, database modes, standalone DI execution, individually compiled golden HTTP/WS/Consumer applications |
| `workspace` | Every root workspace member's unit, integration, UI and documentation tests, selected individually with its own defaults |
| `examples` | Example unit tests, per-package checks, independent client workspace, direct Lily dependency boundary and example-only formatting checks |
| `docs` | Workspace API documentation and facade docs with the supported combined single and factory profiles |
| `--live` | Real HTTP/WS clients, queue confirmation/ACK, duplicate delivery, PostgreSQL records, Redis TTL, MongoDB CRUD, lifecycle/duration/trace propagation, rejection classification and graceful shutdown |

Select stages without repeating the complete run:

```sh
python3 tests/qualification/facade.py --stage components --stage di
python3 tests/qualification/facade.py --stage examples --live
```

The default is all eight offline stages. `--stage` can be repeated; `--live` adds
the live check after the selected stages. A failed command stops the run with a
nonzero exit code. Negative checks pass only when their intended compiler
diagnostic appears. Tests and fixtures are not weakened to accommodate missing
exports or dependency paths.

`--all-features` is intentionally not used: single/factory DI modes are mutually
exclusive. Standalone packages and the two combined modes cover those branches
without accidentally relying on Cargo workspace feature unification. Likewise,
the client example has a separate workspace so its optional client DI
registration cannot become a server startup requirement.

The `macros` stage runs the [macro contract fixtures](../fixtures/macro_contracts/README.md).
The five runtime-dependent suites live in a separate, unpublished workspace so
macro packages do not depend on the runtime that re-exports them. Their unit
tests still belong to the root packages. DI doctests share the same Markdown
with the published API documentation and execute in the injection fixture.

`python3 tests/qualification/package_dependencies.py` also checks the publication
graph independently. It includes optional features, all target tables and
versioned development dependencies, and reports a dependency-first order.

The `workspace` stage likewise runs `cargo test -p PACKAGE` for every member,
rather than `cargo test --workspace`. The latter enables other members' defaults
in the same dependency graph: for example `lily_queue_client/single` registers a
publisher in Consumer's transport-free test container, which has no publisher
configuration. During this qualification, the combined invocation failed 40
Consumer unit tests for that configuration/initialization boundary. This is
recorded as a failed run, not a passing workspace-wide invocation. Individual
selection preserves each crate's intended test environment without ignoring
tests, adding a broker to unit tests, or relaxing their assertions. Combined
facade compilation and the configured multi-service live run are separate gates.

## Evidence

Every invocation creates a directory under
`target/facade-qualification/<UTC timestamp>-<id>/`, or under the selected
`CARGO_TARGET_DIR`. It includes:

- `report.json`: source commit, dirty-tree marker, Rust version, selected stages,
  exact commands, exit codes, durations, Rust test totals and final status;
- a log for every command, plus the detailed umbrella feature/diagnostic logs;
- for live runs, resolved Compose configuration/hash, image identities and the
  path to `examples/artifacts/verify-*` with response, storage, shutdown and JSONL
  evidence.

The runner never updates lockfiles or applies formatter changes. To format just
the examples, select their packages explicitly; `cargo fmt --all` also traverses
local path dependencies.

## Scope limits

Passing this gate proves the current checkout's facade compatibility and the
selected example stack. It does not certify all version combinations in the V1
service support matrix. Explicitly ignored TLS, mTLS, fault-injection, live OTLP,
transactional-inbox and service-version tests remain separate qualification
profiles; their ignored counts remain visible in the reports. ClickHouse stays a
compile/contract check and is not started by the live examples. The example uses
normal at-least-once queue delivery with application idempotency, not the
transactional-inbox profile.

Current path-dependency fixtures validate the current version. They do not
establish compatibility with an unpublished previous release, nor do these
commands publish any crate.
