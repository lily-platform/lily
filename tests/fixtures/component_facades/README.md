# Component facade contracts

These isolated consumer packages deliberately depend on only one Lily component.
Each contract runs with the canonical Cargo name and with a renamed dependency.
MongoDB/PostgreSQL/ClickHouse cover both mutually exclusive DI modes. Serde,
Diesel (`table!` requires a direct driver dependency), and ClickHouse's Row derive
are application-owned third-party dependencies.

The packages test macro/runtime boundaries; they are not runnable application
examples. HTTP/WebSocket/Consumer examples belong to the later umbrella-facade
stage. ClickHouse is included only as a compile/contract check, not an example.

From the repository root:

```sh
cargo test --manifest-path tests/fixtures/component_facades/Cargo.toml --workspace
cargo test --manifest-path tests/fixtures/component_facades/Cargo.toml --workspace --no-default-features --features factory
cargo test --manifest-path tests/fixtures/component_facades/Cargo.toml -p component-facade-queue -p component-facade-queue-renamed --features asyncapi
```

No database or message broker is needed: these contracts exercise generated code,
typed validation, metadata, registry identity and acquisition-free paths. Live
integration semantics are covered separately by the component integration suites.
