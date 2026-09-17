# Background services

`lily_background_service` provides host-owned asynchronous workers. It depends
on Lily DI and cancellation, not on HTTP. The first host adapter is
`lily_http_api::AppBuilder`; `lily_websocket::WsAppBuilder` supports the same
registration method and worker trait:

```rust,ignore
let app = AppBuilder::default()
    .add_background_service::<ProcessingWorker>()
    .build()
    .await?;
app.start().await?;
```

The WebSocket adapter waits for listener bind and required backplane readiness
before execution. Its root re-exports the shared signal as
`BackgroundCancellation`, preserving the separate WebSocket message
`ExecutionCancellation` API. See the [WebSocket worker guide](../../framework/lily_websocket/README.md#background-services).

The same concrete worker type is registered once, even if it is added twice.
Different types have independent worker instances. This is one instance per
application, not distributed scheduling or a cross-process singleton.

## Worker contract

```rust,ignore
use std::sync::Arc;
use lily_background_service::{BackgroundServiceTrait, ExecutionCancellation};
use lily_injection::{ApplicationScopeFactory, ProcessContext};

struct ProcessingWorker {
    scopes: Arc<ApplicationScopeFactory>,
}

#[async_trait::async_trait]
impl BackgroundServiceTrait for ProcessingWorker {
    type Error = WorkerError; // Your error implements Error + Send + Sync.

    async fn new(scopes: Arc<ApplicationScopeFactory>) -> Result<Self, Self::Error> {
        Ok(Self { scopes })
    }

    async fn execute_async(
        &mut self,
        stopping: ExecutionCancellation,
    ) -> Result<(), Self::Error> {
        while !stopping.is_cancelled() {
            let cancellation = stopping.clone();
            self.scopes
                .create_scope(ProcessContext::new())?
                .run(move |extensions| {
                    Box::pin(async move {
                        // RunExecutor is your registered scoped service.
                        let executor = extensions.get_service::<RunExecutor>(None).await?;
                        executor.process_next(cancellation).await?;
                        Ok::<(), WorkerError>(())
                    })
                })
                .await?;

            tokio::select! {
                _ = stopping.cancelled() => break,
                _ = tokio::time::sleep(std::time::Duration::from_secs(1)) => {}
            }
        }
        Ok(())
    }
}
```

DI scope types come from an explicit `lily_injection` dependency, or from the
HTTP, WebSocket or consumer host's root re-exports.

`WorkerError` also implements `From<lily_injection::InjectionError>` to use `?`
for scope creation, service resolution and disposal. A worker does not need to
derive `Injectable`. Resolve options or singleton services in `new` using a
short-lived scope when needed. Keep scoped services inside their job scope.
The example assumes `process_next` returns `Ok(())` for a graceful stop; if it
uses a cancellation error, handle that specific variant in the worker.

`new` runs asynchronously during build, in registration order. Each constructor
and prepared worker already has a retained task owner. Build does not execute
the jobs. Successful listener bind releases prepared workers once, before HTTP
readiness is published. Readiness does not promise their first iteration has
completed. Failed bind, close-before-start and abandoned build/App paths stop
prepared tasks without invoking `execute_async`.

`execute_async` is invoked once for each started worker. The application owns
its loop, retry policy, recovery gates and polling delay. `Ok(())` is a normal
completion, including an intentionally disabled worker. An unhandled `Err` or
panic initiates HTTP host shutdown. No automatic restart or retry is performed.
An error returned during cancellation is still an error: handle expected
shutdown cancellation explicitly and return `Ok(())` when appropriate.

## Scopes

There is one shared `Arc<ApplicationScopeFactory>` per background runtime. The
factory itself does not implement `Clone`; each worker receives an Arc handle.
Creating a scope briefly clones that handle for scope ownership. It does not
clone the container, its services, or a database connection.

`create_scope(ProcessContext::new())` returns a single-use `ServiceScope`.
`run` consumes it, invokes the closure inside the scope's task-local context,
and supplies `&Extensions`. `get_service(None)` therefore resolves against the
correct scope. Existing `ApplicationScope::run` is unchanged.

Both success and application error await disposal before returning. A disposal
error takes precedence over an application error. Panics are resumed after
cleanup. Dropping/aborting the run schedules container-owned cleanup; the
background runtime retains the exact scope generation until cleanup has joined.
Completed scope records are retired during normal traffic, so tracking does not
grow with the number of historical jobs.

Create a fresh scope per independent operation, claim or unit of work. A long
worker loop should not keep one scoped database context forever. The borrowed
provider discourages resolution outside `run`, but Rust still allows an
`Arc<ScopedService>` to escape: the application must not use it after scope close.
Do not detach tasks that use scoped dependencies. `tokio::spawn` does not inherit
the scope's task-local context.

## Shutdown

The execution token is read-only and is cancelled only by host shutdown. HTTP
requests, request timeouts, client disconnection and polling timers do not
cancel it. Worker failure may itself be the reason the host begins shutdown.

The HTTP adapter projects its existing absolute shutdown cutoffs into the
background runtime. All workers receive cancellation together. They share the
root cooperative window (not the short per-request cooperative cap), followed by
execution abort/join, exact scope cleanup, DI disposal and telemetry flush. A
worker may create a finalization scope while cooperating with shutdown. Scope
admission closes when execution ends or forced cancellation begins.

The runtime uses the first shutdown deadlines throughout. Cancelling a close
waiter or calling close again does not cancel the owner or grant another budget.
Scope disposal errors remain failures even if every task eventually joins.
Startup failures are distinguished from rollback failures.

Tokio abort stops a yielding async task; it cannot forcibly terminate a
non-yielding CPU loop, a blocking destructor or an already-running
`spawn_blocking` job. In that situation HTTP reports incomplete shutdown and
retains DI/telemetry dependencies while execution is unconfirmed. Timer expiry
is never treated as proof that the task stopped. Applications needing hard
termination of untrusted/blocking work should isolate it in another process.

## Other hosts

Use `BackgroundServices::default()`, `add::<T>()`, and
`into_runtime(&container)` to obtain a retained runtime before awaiting
`initialize()`. Call `start()` after the host's resources are ready. Observe
`wait_for_failure()` alongside the host's normal shutdown signals.

On startup failure or shutdown, call `begin_shutdown(BackgroundShutdownDeadlines)`
with the host's **original absolute** cooperative, execution-stop, cleanup and
reconcile cutoffs. `force_stop()` escalates that shutdown. Await
`wait_stopped_before(original_reconcile_deadline)`, then inspect `snapshot()`.
`is_terminal()` proves termination; `succeeded()` includes application failures;
`cleanup_succeeded()` describes resource cleanup independently. Do not close DI
or telemetry while termination remains unconfirmed. The runtime never closes
the container itself. Dropping a waiter alone is not a standalone host shutdown
request; that host must call `begin_shutdown`.

## Telemetry and verification

Worker start/terminal events contain the concrete service type, terminal reason
and numeric elapsed milliseconds. Arbitrary error strings are not included in
host health or these events. Workers should record their own safe error context.
For a trace per job, put `#[lily_trace(...)]` on a job method that creates and runs
its scope; scope cleanup preserves that job's tracing context.

Tests cover delayed construction, duplicate registration, one-shot completion,
errors/panics, cancellation-insensitive workers, dropped waiters, shared absolute
deadlines, post-cancellation finalization scopes, cleanup timeout and exact joins.
HTTP tests also cover bind failure, external-container isolation, blocked real
destructors, frozen incomplete evidence and flushed file-exporter records with
matching job/cleanup trace identities.
Executed commands and coverage are recorded in [QUALIFICATION.md](QUALIFICATION.md).

The lifecycle follows the separation in [.NET BackgroundService](https://github.com/dotnet/runtime/blob/v10.0.0/src/libraries/Microsoft.Extensions.Hosting.Abstractions/src/BackgroundService.cs)
and the host's default [StopHost failure behavior](https://github.com/dotnet/runtime/blob/v10.0.0/src/libraries/Microsoft.Extensions.Hosting/src/HostOptions.cs),
with Lily's explicit task/scope joins and existing shutdown budget.
