# lily_injection

Application-owned dependency injection for Lilyrs. One container owns service
construction, request/job scopes and asynchronous disposal. This is the complete
DI dependency: it re-exports `Injectable`, `ServiceTrait`, `InjectionError`,
`Extensions`, `ProcessContext` and the scope APIs.

```toml
[dependencies]
lily_injection = "0.1.0"
```

The umbrella path is `lilyrs::injection` with feature `injection`. HTTP, WebSocket
and Consumer also re-export the application DI APIs at their roots. No separate
derive, registry or `linkme` dependency is needed.

## Constructor injection

```rust
use std::sync::Arc;
use lily_injection::{Injectable, ServiceTrait};

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct Clock;
impl ServiceTrait for Clock {}

#[derive(Injectable)]
#[service(lifetime = "Scoped")]
struct OrderService {
    #[inject]
    clock: Arc<Clock>,
}
impl ServiceTrait for OrderService {}
```

`#[inject]` fields use `Arc<T>` or `Arc<dyn Trait>`. A dependency-only struct does
not need `Default`; structs with additional runtime state do. Interface bindings
use `#[service(interface = dyn Interface, lifetime = "Scoped")]` and share the
same instance and lifecycle as their concrete service.

| Lifetime | Ownership |
| --- | --- |
| `Singleton` | Eagerly initialized once per application container |
| `Scoped` | Lazily initialized once per managed request/job scope |
| `Transient` | Created for each resolution; disposed by its scope or root owner |

The default lifetime is `Transient`. Container startup validates missing
dependencies, duplicate bindings, cycles and incompatible lifetime capture.
A singleton cannot capture a scoped dependency. A scoped service can inject a
singleton.

## Hosting and scopes

Framework application builders own their container. Resolve from that provider
instead of constructing a second container. `Extensions::get_service::<T>(None)`
uses the current task-local process context; scoped services require a live
managed scope.

Standalone composition roots can use `ApplicationContainer::build()` and await
`close()` at shutdown. Tokio with its time driver is needed for asynchronous
lifecycle deadlines. For jobs, share `Arc<ApplicationScopeFactory>`, create a
fresh scope with `create_scope(ProcessContext::new())`, then call its single-use
`run(|extensions| Box::pin(async move { ... }))`. The callback receives the
provider inside the correct context, and disposal is awaited before returning.
Cleanup failure takes precedence over the callback error. Do not retain scoped
services for use after their scope closes.

`ServiceTrait::initialize` and `dispose` define instance lifecycle hooks.
Failed or cancelled container construction retains rollback ownership while the
Tokio runtime remains alive. `LILY_INJECTION_BUILD_ROLLBACK_TIMEOUT_SECS` controls
the aggregate build rollback budget; the default is 30 seconds. Normal application
shutdown has its own host-selected deadline.

This crate has no optional Cargo features. The registry's hidden macro support
paths are implementation details, not manual registration APIs.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_injection](https://docs.rs/lily_injection).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
