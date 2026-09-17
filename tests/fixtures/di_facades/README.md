# DI dependency and public API contracts

This workspace is independent of the repository workspace. Eight packages
have exactly one normal dependency: `lily_injection`, `lily_http_api`,
`lily_websocket`, or `lily_consumer`. Each is tested under its canonical Cargo
name and under an alias. Two configuration fixtures use explicit
`lily_config` and `lily_injection` dependencies, also with and without aliases.
None of these positive fixtures declares `linkme`, `lily_error`,
`lily_injection_registry`, or `lily_injectable_derive` directly.

Run from the repository root:

```sh
cargo test --manifest-path tests/fixtures/di_facades/Cargo.toml --workspace --locked
```

The ten positive fixtures compile concrete and trait-object constructor
injection, lifecycle hooks, a nested-module scoped service, default transient lifetime, mixed
default/injected state, and disabled registration. Tests assert that factory,
route, and disposer registrations reach the actual runtime registry exactly
once, with the correct lifetimes and dependency types.

The configuration fixtures also verify that a scoped application service can
inject the singleton `ConfigService` through their shared DI registry.

The `boundaries` package uses compile-fail tests with exact diagnostic snapshots
to verify that configuration, background service and queue crates do not export
DI types. It also checks that configuration exposes no hidden runtime bridge,
and that importing the derive directly still requires one of the four supported
DI dependencies. It deliberately has no direct DI runtime or host dependency.

The two standalone runtime packages also build and close a real container.
They verify singleton/interface identity, scoped isolation, transient identity,
default state, initialization/disposal counts, dependency disposal ordering,
scope requirements, and both scope APIs. Tokio is a dev dependency used only
to drive these tests. Framework facade tests inspect their linked registrations
without starting framework-owned services requiring broker/configuration I/O.
Framework lifecycle behavior is covered by the respective crate test suites.

The existing derive tests retain direct `lily_injectable_derive` imports as
backward-compatibility coverage.
