# lily_monitoring

`lily_monitoring` provides bounded, application-owned health snapshots. It
stores observations; it does not run dependency probes, create an HTTP health
endpoint, or own database/broker clients.

## Canonical use

Create one `HealthRegistry` with the exact `ShutdownState` used by the
application, register stable check names during startup, and update them from
the services that already own the relevant dependency or resource:

```rust
use std::sync::Arc;

use lily_monitoring::{
    HealthCheckKind, HealthCriticality, HealthRegistry, HealthStatus,
};
use lily_shutdown::ShutdownState;

let shutdown = Arc::new(ShutdownState::new());
let health = HealthRegistry::new(shutdown);

health.register(
    "postgres.primary",
    HealthCheckKind::Dependency,
    HealthCriticality::Critical,
)?;
health.update("postgres.primary", HealthStatus::Healthy, "connected")?;

let snapshot = health.snapshot()?;
assert!(snapshot.live);
assert!(snapshot.ready);
# Ok::<(), lily_monitoring::HealthRegistryError>(())
```

Only safe, bounded reason codes belong in a health snapshot. Put raw provider
errors, connection strings, and other sensitive diagnostics in private logs or
traces. Critical checks block readiness until they are `Healthy`; non-critical
checks remain observable without blocking traffic.

`ProcessResourceSnapshot::capture` exposes real process values when the host
supports them and an explicit `Unsupported` signal otherwise. Framework-owned
task and scope counts should be recorded from their real ledgers rather than
estimated from Tokio.
