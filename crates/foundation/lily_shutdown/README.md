# lily_shutdown

`lily_shutdown` provides the application-owned lifecycle state, signal
monitor, in-flight work guards, and ordered shutdown coordinator used by Lily
adapters.

## Canonical use

Most Lily HTTP, WebSocket, consumer, and telemetry builders install their own
shutdown components. At a custom composition root, create one shared
`ShutdownState`, track admitted work with RAII guards, and register real
resource handles with `FrameworkShutdownCoordinator`:

```rust
use std::sync::Arc;
use std::time::Duration;

use lily_shutdown::{
    FrameworkShutdownCoordinator, FrameworkShutdownPhase, ShutdownAction,
    ShutdownSignal, ShutdownState,
};

# async fn example() {
let state = Arc::new(ShutdownState::new_not_ready());
let mut shutdown = FrameworkShutdownCoordinator::new(
    Arc::clone(&state),
    Duration::from_secs(30),
);

shutdown.register(ShutdownAction::new(
    "application.client",
    FrameworkShutdownPhase::DisposeDependencies,
    Duration::from_secs(5),
    || async { Ok(()) },
));

state.publish_ready().expect("startup still owns readiness");
let report = shutdown.execute_report(ShutdownSignal::Manual).await;
assert!(report.is_terminal_complete());
# }
```

Use `connection_guard` or `job_guard` for every admitted unit of work. Their
destructors reconcile counters during ordinary completion, cancellation, and
panic. Do not mutate lifecycle atomics or counters directly.

`SignalHandler::install` implements the process policy: the first signal
initiates graceful shutdown, a second signal requests the bounded force path,
and `SIGQUIT` enters that path immediately. The library never terminates the
process from a background task.
