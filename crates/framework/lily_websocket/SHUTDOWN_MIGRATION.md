# Shutdown migration

Phases 1–9 implement the shutdown contract described in
[SHUTDOWN_ARCHITECTURE.md](SHUTDOWN_ARCHITECTURE.md). This guide collects the
source changes an application needs; the [qualification matrix](SHUTDOWN_QUALIFICATION.md)
maps the guarantees to executable tests.

## Callback signatures

Use Lily's read-only views. They expose `is_cancelled()`, `cancelled().await`,
`Clone` and `Debug`. They do not expose `cancel()`, `child_token()`, a tuple
field, `Deref` or `into_inner()`. The deprecated `Cancellation` alias means
`ExecutionCancellation` only. It cannot be extracted in `#[disconnected]`.

| Callback | New final argument / extractor | Meaning |
| --- | --- | --- |
| Handshake `handle` | `ExecutionCancellation` | Stop the accepted upgrade operation cooperatively. |
| Identity `identify` | `ExecutionCancellation` | Stop the accepted identity operation cooperatively. |
| Connection `admit`, `opened` | `ExecutionCancellation` | Stop admission/open execution cooperatively. |
| Message `before_message`, `after_message` | `ExecutionCancellation` | Stop this message's normal execution cooperatively. |
| Guard `can_activate` (global/controller/action) | `ExecutionCancellation` | Stop the guard evaluation cooperatively. |
| Controller message action, `#[connected]` | Optional `ExecutionCancellation` extractor | Observe cancellation of accepted execution. |
| Message `on_message_termination` | `CleanupCancellation` | Observe this cleanup invocation's authority ending. |
| Connection `closed` | `CleanupCancellation` | Observe this terminal invocation's authority ending. |
| Controller `#[disconnected]` | Optional `CleanupCancellation` extractor | Observe bounded disconnect cleanup cancellation. |

The parameter and the corresponding invocation/exchange/context view refer to
the same authority. A wrapper clone does not extend its lifetime. Public
`start_with_cancellation(tokio_util::sync::CancellationToken)` still accepts an
application-owned shutdown source; this is distinct from callback parameters.

## Separate normal completion from interrupted cleanup

These are the current message middleware signatures (inside the existing
`#[async_trait] impl WsMessageMiddleware`):

```rust,ignore
async fn after_message(
    &self,
    exchange: &mut WsMessageExchange,
    outcome: WsMessageOutcome,
    cancellation: ExecutionCancellation,
) -> Result<WsMessageDecision, WsMiddlewareError> {
    // Normal reverse processing; may still affect the message outcome.
    Ok(WsMessageDecision::Continue)
}

async fn on_message_termination(
    &self,
    context: WsMessageTerminationContext<'_>,
    cancellation: CleanupCancellation,
) -> Result<(), WsMiddlewareError> {
    // Best-effort cleanup of this successfully entered middleware.
    // Inspect context.normal_exit() before repeating an earlier side effect.
    Ok(())
}
```

Keep normal response transformation and normal exit work in `after_message`.
Put interruption-specific cleanup in `on_message_termination`. The default
termination callback is a no-op: overriding it is necessary only when your
middleware owns application cleanup that requires this notification.

Termination runs for entered middleware whose normal exit never started or
was interrupted, including ordinary operation timeout. It is not restricted to
an explicit server force signal. An already completed, failed or panicked normal
exit is not invoked again as termination. An interrupted normal exit can have
partial side effects; `normal_exit()` reports whether its first poll occurred,
not what application state changed. Make cleanup safe for that partial state.

Neither first poll nor completion is guaranteed when the cleanup budget is
exhausted. There is no automatic rollback and no async cleanup through `Drop`.
Use RAII for local synchronous release and DI disposal for DI-owned resources.

## Controller eligibility and terminal context

`#[disconnected]` is armed only after an actual `#[connected]` returns `Ok(())`,
before its DI scope closes. If you previously defined only `#[disconnected]`,
add a successful connected method when the terminal notification is required:

```rust,ignore
#[connected]
async fn connected(&self, _cancellation: ExecutionCancellation)
    -> Result<(), WebSocketLifecycleError>
{
    Ok(())
}

#[disconnected]
async fn disconnected(&self, cancellation: CleanupCancellation)
    -> Result<(), WebSocketLifecycleError>
{
    // Await application cleanup only while its bounded authority permits it.
    Ok(())
}
```

An `opened` failure or unsuccessful connected callback does not invoke user
`#[disconnected]`. Framework middleware/manager/DI obligations still remain.
Before disconnected starts, the connection session/transport must have been
destroyed, message owners actually joined and their exact DI generations closed.
If those prerequisites cannot be confirmed, the eligible callback stays
unstarted; Lily reports incomplete cleanup.

The closing caller is a terminal outbound target. Other healthy clients/rooms
and DI services may still be used within the cleanup authority. No new incoming
message is admitted on the closing connection.

## Drain, cancellation and outbound errors

Graceful drain stops new connections and message dispatch without cancelling
the accepted action. When force begins or the graceful deadline expires, Lily
signals execution cancellation and keeps polling during a bounded cooperative
window. Only then can it drop the execution slot. Lifecycle owners, entered
ledgers and DI receipts survive that drop.

Cleanup uses independent per-invocation signals. One hook's local timeout
cannot cancel its siblings. Every local cap is shortened by the owner/root
absolute deadline; a callback cannot extend shutdown by starting a new timeout.

During drain, awaited sends inside admitted callbacks may attempt outbound
admission. Execution cancellation revokes execution sends; cleanup callbacks
have their own authority. Saved contexts and raw spawned tasks do not acquire
drain privileges. Once dispatcher dependency-close starts, all new sends stop.
SignalR-informed semantics are documented with pinned source references in
[Phase 7](SHUTDOWN_ARCHITECTURE.md#phase-7-bounded-outbound-continuation-during-drain).

Handle `ConnectionError::DispatchInterrupted` in exhaustive matches. It means
local dispatch was interrupted and may already have partial queue effects;
there is no complete local receipt. A backplane publish failure retains the
available local receipt in `BackplanePublish { local, source }`. A successful
send means admission/acceptance, not delivery to a socket or client. Do not
automatically retry a partially accepted broadcast without application-level
duplicate handling.

## Resource ownership and close results

Controller/middleware/guard instances are created once per app and may retain
singleton `Arc<T>` DI services. Resolve scoped/transient services through the
active invocation's Extensions/exchange. Build-time enforcement of incorrectly
retained scopes remains deferred; this release adds no async disposal contract
for these application-lived instances.

Directly constructed resources and raw `tokio::spawn` jobs are application-owned
and absent from Lily shutdown accounting. Register app-lifetime async resources
as DI-managed services. A caller-supplied container is closed by its caller;
Lily still owns the scopes it creates in that container.

Await `app.close()` (or the running `start` future) and handle `Err`. Dropping a
waiter does not cancel the retained root. Concurrent and repeated close calls
observe the same result and actual root join. `Err` can mean cleanup failed
even though all tasks stopped, or that some task termination is still
unconfirmed. It never means a fresh cleanup budget is granted on retry.

Diagnostics are internal in Phase 9; no public report API or dependency is
added. The `lily_websocket::shutdown` tracing target emits a pre-telemetry-flush
evidence checkpoint, a frozen shutdown outcome, and an actual root-join event
when a caller observes that join. The final outcome may require an
application-owned tracing subscriber because Lily's own telemetry has already
closed. Export delivery itself is not guaranteed. See the report field contract
in [Phase 9](SHUTDOWN_ARCHITECTURE.md#phase-9-aggregate-evidence-and-release-qualification).

A non-yielding future poll or blocking destructor cannot be preempted by Tokio.
An expired root records unconfirmed work and retains its receipts; late
termination does not rewrite the frozen shutdown attempt as success. Finite
shutdown with absolutely no remaining work requires those application
cooperation assumptions or process-level termination outside Lily.
