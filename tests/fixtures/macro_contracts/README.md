# Macro/runtime contracts

This independent workspace owns runtime-dependent integration and `trybuild`
tests for the published macro packages. All five consumer packages set
`publish = false`; none is a dependency of a published crate. The original test
bodies and pass/fail expectations are retained. Proc-macro parser and expansion
unit tests remain in their original packages.

| Consumer | Implementation package | Preserved checks |
| --- | --- | --- |
| `lily-macro-contracts-injection` | `lily_injectable_derive` | 19 UI cases, registration and initialization cleanup, three shared rustdoc examples, DI example |
| `lily-macro-contracts-mongodb` | `lily_mongodb_derive` | 10 UI cases and three CRUD runtime tests, default and factory profiles |
| `lily-macro-contracts-trace` | `lily_trace_macros` | 13 UI cases including runtime alias resolution, Result classification and async-trait |
| `lily-macro-contracts-queue` | `lily_queue_derive` | 28 UI cases and three generated-handler runtime tests |
| `lily-macro-contracts-websocket` | `lily_websocket_derive` | 22 controller/gateway, middleware and cancellation UI cases |

Run from the repository root with Rust 1.96.1 and Python 3.11+:

```sh
python3 tests/qualification/facade.py --stage macros
# Included automatically by the complete qualification command:
python3 tests/qualification/facade.py
```

The runner selects each package separately to preserve its dependency features.
The Queue negative tests require AsyncAPI to be disabled; its parser/expansion
unit tests additionally run with AsyncAPI enabled. MongoDB profiles are selected
separately. Do not use `--workspace --all-features` for these contracts.

For one suite:

```sh
cargo +1.96.1 test --manifest-path tests/fixtures/macro_contracts/Cargo.toml \
  -p lily-macro-contracts-trace --locked --offline
```

On a fresh machine, fetch this workspace's locked dependencies first. These
checks require no running database, RabbitMQ or OTLP collector.

DI documentation is stored once under
`crates/foundation/lily_injectable_derive/src/docs/`. The macro package includes
it in its public API docs; the injection fixture includes the same files for
rustdoc tests. Disabling the implementation package's own doctest harness avoids
the `lily_injection -> lily_injectable_derive -> lily_injection` publication
cycle without ignoring its examples.

The legacy empty `worker_tests` case is retained as part of the move, but does
not establish a worker contract. The explicit UI, cleanup, registration and
CRUD assertions provide the executable coverage listed above.

Snapshots normalize external workspace paths through dependency aliases. Path
changes caused by moving the suites must not change expected diagnostics.
