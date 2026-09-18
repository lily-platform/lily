# lily_injectable_derive

Implementation of Lilyrs's `Injectable` derive. It generates constructor
injection and link-time registration metadata consumed by `lily_injection`.
Application code normally imports the derive from that runtime facade:

```toml
[dependencies]
lily_injection = "0.1.0"
```

```rust
use lily_injection::{Injectable, ServiceTrait};

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct RequestService;

impl ServiceTrait for RequestService {}
```

## Derive contract

- `#[inject]` marks an `Arc<T>` or `Arc<dyn Interface>` field.
- `#[service(lifetime = "Singleton" | "Scoped" | "Transient")]` selects ownership;
  omitting it selects `Transient`.
- `#[service(interface = dyn Interface)]` publishes one trait-object binding.
- `#[service(disabled)]` (or `enabled = false`) disables registration.
- Non-injected runtime fields require the struct to implement `Default`.
- Generic services, tuple structs, enums and unions are rejected.
- The application supplies `ServiceTrait`; its hooks define initialization and
  disposal. DI graph validation and actual instances belong to the runtime.

Expansion supports direct and Cargo-renamed runtime dependencies, framework-root
DI re-exports, and the `lilyrs` facade. Applications do not add the registry or
macro helper crates merely to use this derive.

`CrudService` belongs to `lily_mongodb_derive` and is re-exported by `lily_mongodb`;
it is not a DI derive. This implementation crate has no optional features.

Runtime-dependent compile tests and shared documentation examples live in the
repository's unpublished `tests/fixtures/macro_contracts/injection` package.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_injectable_derive](https://docs.rs/lily_injectable_derive).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
