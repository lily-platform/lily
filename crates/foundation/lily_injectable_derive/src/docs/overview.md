Derive macros for Lily's dependency-injection composition model.

Application services normally import `Injectable` and `ServiceTrait` from
`lily_injection`. The derive publishes link-time registration
metadata and generates constructor injection; the trait defines the
instance lifecycle.

# Canonical service

```
use std::sync::Arc;
use lily_injection::{Injectable, ServiceTrait};

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct Clock;

impl Clock {
    fn now(&self) -> &'static str {
        "now"
    }
}

impl ServiceTrait for Clock {}

// Every field marked #[inject] must be Arc<T> or Arc<dyn Trait>.
// A dependency-only named struct does not need Default.
#[derive(Injectable)]
#[service(lifetime = "Singleton")]
struct AuditService {
    #[inject]
    clock: Arc<Clock>,
}

impl AuditService {
    fn timestamp(&self) -> &'static str {
        self.clock.now()
    }
}

impl ServiceTrait for AuditService {}
```

No manual registration call is required. `Injectable` emits metadata into
`lily_injection_registry`, and the application-owned
`lily_injection::ApplicationContainer` discovers and validates the complete
graph before starting singleton services.

# Construction rules

- `#[inject]` accepts only `Arc<T>` and `Arc<dyn Interface>` fields.
- If every named field is injected, the derive constructs the struct
  directly and `Default` is not required.
- If any named field is not injected, the struct must implement `Default`;
  the derive creates that default state and then replaces injected fields.
- Generic services, tuple structs, enums and unions are rejected.
- The service must implement `ServiceTrait + Send + Sync + 'static`.

# Lifetimes

`#[service(lifetime = "Singleton")]`, `"Scoped"` and `"Transient"` are
supported. Omitting the attribute selects `Transient`; production code
should state the intended lifetime explicitly. A singleton cannot inject a
scoped dependency. The complete graph is checked before startup.

# Interface resolution

A concrete service can publish one trait-object route:

```
use lily_injection::{Injectable, ServiceTrait};

trait ClockApi: Send + Sync {
    fn name(&self) -> &'static str;
}

#[derive(Injectable)]
#[service(interface = dyn ClockApi, lifetime = "Singleton")]
struct SystemClock;

impl ClockApi for SystemClock {
    fn name(&self) -> &'static str {
        "system"
    }
}

impl ServiceTrait for SystemClock {}
```

Both `SystemClock` and `dyn ClockApi` resolve to the same allocation. More
than one active implementation for the same interface is rejected while
the container is built.

`#[service(disabled)]` suppresses link-time registration and resolution. It
is intended for compile-time candidate selection; a disabled service cannot
satisfy another service's dependency.

Only `lily_injection` is required as a DI dependency. `lily_http_api`,
`lily_websocket` and `lily_consumer` also expose `Injectable`
and the DI APIs at their roots. Expansion locates the direct runtime or
one of these facades, including Cargo-renamed dependencies. Its hidden
registration bridge removes the need for application dependencies on
`lily_error`, `lily_injection_registry` or `linkme` solely for DI.
Existing direct imports of this crate's `Injectable` remain supported.
