# Umbrella facade contracts

Every consumer has `lily` as its **only** direct Lily dependency. Each contract
also runs under the renamed dependency `platform`. These are compile/runtime
contracts. The [connected examples](../../../examples/README.md) provide runnable
HTTP, WebSocket and Consumer applications. The [final qualification command](../../qualification/FACADE.md)
also runs direct component and DI regressions.

Run from the repository root (Python 3.11+, Rust 1.96.1):

```sh
python3 tests/fixtures/umbrella_facades/verify.py
```

No database, broker or collector is required. The script uses the committed
lockfiles and cached Cargo dependencies (`--locked --offline`). On a fresh machine,
fetch dependencies first with `cargo fetch` for both this workspace and
`matrix/Cargo.toml` (add `--locked` to preserve the recorded versions).

The validator runs each package separately so Cargo feature unification cannot
hide missing dependencies. It checks:

- all 16 component modules, their base defaults and every native feature mapping;
- no optional dependencies for the empty facade;
- single/factory separation and low-level adapter features without DI registration;
- all three framework macros and their DI root re-exports with no explicit
  `injection` feature, plus standalone DI identity, scope and disposal contracts;
- MongoDB, PostgreSQL, ClickHouse, Trace and Queue macros, including renamed
  dependencies, nested Queue/AsyncAPI markers and singleton/factory modes;
- isolated MongoDB/PostgreSQL/ClickHouse/Trace derives with only their component
  feature enabled, so a second facade feature cannot mask an expansion dependency;
- every public feature compiles on its own, and both combined application modes;
- expected compilation failures for disabled APIs, invalid WebSocket lifecycle
  extractors, disabled Queue AsyncAPI and all six conflicting composition modes.

Use `graph`, `consumers`, `builds`, or `rejections` as the final command argument
to run one stage. Logs are written to `target/umbrella-validation` (or the chosen
`CARGO_TARGET_DIR`). Negative checks require the expected compiler diagnostic,
not just any failed command. Component contracts in `../component_facades` remain
separate regression coverage for direct component dependencies.
