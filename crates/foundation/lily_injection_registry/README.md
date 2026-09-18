# lily_injection_registry

Link-time registration support shared by Lilyrs's DI derive and container.
It stores generated service, interface-route and disposal metadata and validates
dependency graphs before the runtime constructs services.

## Application entry point

Use `lily_injection::{Injectable, ServiceTrait}` or the DI re-exports of
`lily_http_api`, `lily_websocket`, `lily_consumer` or `lilyrs::injection`.
Those facades provide the support paths needed by generated code. An application
does not need to depend on this registry directly.

`ServiceLifetime` describes `Singleton`, `Scoped` and `Transient` ownership and
is also available through `lily_injection`. The other registration symbols are
public so proc-macro output can reference them from consuming crates; they are
hidden implementation interfaces. Do not construct `ServiceMetadata`, mutate
distributed slices or manually register factories in application code.

This crate does not own runtime instances, configuration, scopes or shutdown.
Those responsibilities belong to `lily_injection` and the application host.
There are no optional Cargo features.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_injection_registry](https://docs.rs/lily_injection_registry).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
