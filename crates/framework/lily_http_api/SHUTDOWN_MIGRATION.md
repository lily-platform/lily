# HTTP shutdown migration contract

**Phases 1–9 implement and qualify `App::close`, retained root/task/request/body ownership, exact DI
receipts, atomic admission, cooperative execution cancellation and independent
middleware termination.** Middleware/guard callback signatures changed in Phase 4.
Phase 6 adds a default callback and retained state without another breaking
change to `handle`. Phase 7 adds force-safe dependency close and real telemetry
worker joins. Phase 8 adds internal immutable HTTP reporting and health reasons.
Phase 9 tests the combined managed runtime and finalizes the migration below.
See [implementation status](SHUTDOWN_ARCHITECTURE.md) and the
[phase roadmap](SHUTDOWN_ROADMAP.md) before migrating application code.

## Application migration steps

1. Add the final `ExecutionCancellation` parameter to middleware `handle` and
   guard `can_activate`. Normal around after-code keeps that execution view.
   Actions can extract it; custom extractors/response conversion read it from
   the request. Keep the host's stop source separate from callback views.
2. Keep normal response shaping around `next.run`. Override
   `on_request_termination` only for abnormal cleanup, and explicitly retain
   required data in `termination_state_mut()` before cancellable awaits.
   Handle partially initialized state and interrupted normal after-code. Use
   only the supplied independent cleanup view; do not restart local timers.
3. Resolve scoped/transient services through the active `Extensions` context.
   Expect the request scope to remain live through lazy body/input release and
   owned helper joins. A returned streaming response still owns work; observe
   cancellation in its source and allow it to be dropped when its cap expires.
4. Await managed `start`, `start_with_cancellation` or `close` after every
   successful build. Keep an App clone for host-directed close. Replace
   `drop(app); container.close().await` for an App-owned container with
   `app.close().await`. Close a caller-owned container only after its HTTP and
   other users have actually terminated.
5. Treat `close` errors and `http.shutdown` reasons as evidence. A forced
   completion may truncate responses; terminal failure may include a timed-out
   disposer whose future was actually released. `Incomplete` still has
   unconfirmed resources. A late join never changes the first attempt result.

The facade-only [downstream fixture](../../../tests/fixtures/downstream_http/src/main.rs)
compiles both callback signatures, retained state, cleanup deadline access and
managed start/close without direct middleware/web-core/macro dependencies.
The [managed qualification tests](src/app/qualification_tests.rs) execute the
generated action/guard/extractor path, same-instance application/controller
middleware, action middleware and actual HTTP/1 and HTTP/2 transports.
Controller/action attributes reject a duplicate middleware type in their merged
route plan; supported application/controller reuse receives distinct ledger
entries. Qualification does not relax that build-time contract.

## Available now: explicit application close

`App::close(&self).await` closes a built but unstarted App without opening a
listener, or requests shutdown of the same root already used by `start` or
`start_with_cancellation`. Keep an App clone when the host needs a separate close
handle. Repeated/concurrent close shares the original absolute deadline and
root result. A second start is rejected with `AlreadyExists`.

After a start future's first poll installs the root, dropping its waiter requests
shutdown; the root keeps ownership. Dropping a polled close waiter likewise
cannot discard cleanup. Async methods remain lazy: dropping an entirely unpolled
future is not a shutdown request. Await start/close for every successful build;
synchronous `Drop` is not an async disposal path.

The host cancellation token is observed, never cancelled by Lily. Caller-owned
containers and external tracing stay caller-owned. At a root timeout `close`
returns an error and keeps unconfirmed task receipts; late completion does not
rewrite that frozen error as success. Phase 8 freezes the internal HTTP aggregate
and root result together, including middleware/dependency/telemetry evidence.

## Available now: dependency and telemetry barriers

Phase 7 adds no public callback changes. Owned DI close is attempted on normal
and force paths after transport, request scope and monitor termination; telemetry
follows DI. A failed disposer remains a failure even if all tasks joined. A
pending child blocks parent disposal and makes shutdown incomplete.

The shared tracing adapter now retains actual file/metrics thread joins and
provider blocking-task receipts. T/H remain within the original total deadline;
waiting again cannot retry an exporter or turn its previous timeout into success.
The SDK custom-reader feature is enabled internally without a version/lockfile
change. User-created tasks and externally configured DI/tracing remain outside
Lily's ownership.

Dropping a pending build now preserves its owned DI/tracing rollback. Explicit
failure and cancellation reuse a single rollback deadline; constructing DI uses
the captured bootstrap rollback budget with a reserved tail for telemetry. Raw
`tokio::spawn` in user constructors/hooks is still application-owned.

## Available now: request owner and DI observation

Phase 3 separates framework orchestration and retained request/response/scope
state from the dispatch future. A dropped service waiter or transport task no
longer destroys that owner. The owner records actual slot destruction before
releasing request captures and closing its scope. HTTP follows that exact scope
generation and original close result through waiter loss, including inside a
caller-owned container. Another owner's unavailable disposal result is `Unknown`,
not successful cleanup; repeated observation cannot invent a new close result.

Phase 3 introduced no callback signature change. Phase 4 now supplies the
read-only execution view and cooperative window described next.
Buffered scope cleanup stays within the original request deadline. Interrupted
execution has one separate local cleanup allowance equal to the configured
request timeout, always clamped to the root cleanup/reconciliation cutoffs.
Request capacity remains held through actual owner/scope termination.

## Available now: read-only execution cancellation

`ExecutionCancellation` now exposes
observation (`is_cancelled()`, `cancelled().await`), cloning and bounded `Debug`.
It exposes no deadline setter or cancellation source. The framework remains the
only owner of cutoffs and cancellation. `CleanupCancellation` is the separate
Phase 6 view; termination/DI cleanup is never given this execution signal.
There is no
`cancel()`, raw-token conversion, public constructor/source, tuple field,
`Deref`, child authority or execution/cleanup type conversion. Cloning a view
cannot extend its deadline; dropping a view or its `cancelled()` waiter cannot
cancel its source.

The view is defined in the transport-independent `lily_cancellation` crate and
re-exported as `lily_web_core::ExecutionCancellation` and
`lily_http_api::ExecutionCancellation`. Hidden construction seams belong to the
framework; application code cannot recover the source from a received view.
Request storage is separate from `Request.local`, so clearing local values does
not clear the signal. Low-level requests constructed outside the managed server
have an inactive view. There is no middleware-to-HTTP dependency or WebSocket
token import.

| Execution boundary | Signal/access and implementation status | Meaning |
| --- | --- | --- |
| Dynamic CORS user evaluation | Execution view in its request context | Stop this accepted policy evaluation cooperatively. |
| `HttpMiddleware::handle`, before **and after** `next.run` | Execution parameter; exchange exposes the same authority | Stop normal request processing, including response post-processing. |
| Registered route guard evaluation | Execution parameter and same request view | Stop this accepted guard evaluation. No new guard exit hook is introduced. |
| Custom extractors / `IntoResponse` | Read-only request view | Observe the same request execution cancellation. Existing request parameters can carry it without inventing separate phase tokens. |
| Controller action | Optional typed `ExecutionCancellation` extractor | Stop accepted action work cooperatively. |
| Lazy response producer / SSE | Captured view; retained producer ownership implemented in Phase 5 | Stop production cooperatively after graceful drain. Cloning the view does not authorize unrelated background work. |
| `on_request_termination` (Phase 6) | Independent `CleanupCancellation` parameter and same termination-context view | Stop this best-effort cleanup invocation within its remaining allowance. |
| Constructors / synchronous descriptors | No request signal | Application build has a separate startup/rollback lifetime. |

Unlike WebSocket's separate after callback, HTTP normal after-code lives inside
the around `handle` future. It therefore uses **execution**, not cleanup,
cancellation. Only abnormal termination uses a cleanup authority. `HttpMiddleware::handle`
and `GuardTrait::can_activate` now require an additional final
`ExecutionCancellation` argument. Use an underscore binding when observation is
unnecessary. Existing applications must update these two implementations.
Dynamic CORS and custom extractor/response signatures stay unchanged; read the
view with `context.execution_cancellation()` or `request.execution_cancellation()`.
Actions opt in with a typed `ExecutionCancellation` argument; generic generated
extraction also supports a type alias. No controller constructor token is added.

Admission closes before new user/CORS work or scope creation. Accepted execution
can drain without its signal being cancelled at gate closure. Force, graceful
cutoff, ordinary request timeout or service-waiter loss then signals execution.
The same pinned future is polled for at most **250ms from that first signal**,
clipped to the original root C cutoff. This is an internal policy and a maximum,
not a minimum grant. Repeated stop cannot restart it. Only a still-pending slot
is dropped, preserving owner state and DI cleanup. A completed pipeline keeps
its actual response or application error even after timeout/shutdown cancellation.
A local request timeout selects **504 Gateway Timeout** only for execution still
incomplete at the cooperative cutoff. The absolute execution deadline starts at
admission, including request-body consumption; dispatch and input chunks do not
restart it. Lazy response production and protocol writing use that same deadline
and the remaining first-signal window. If an uncommitted response cannot finish,
local timeout selects 504 and shutdown selects 503, with one internal 100ms
finalization cap clipped to root P (transport stop), leaving R for actual joins.
A committed incomplete response is stopped at its protocol boundary; its status
cannot be replaced. Completing production
or a local write is not proof that the client received the complete response.

`App::start_with_cancellation(tokio_util::sync::CancellationToken)` still
accepts an application-owned stop source. Keeping that host API is compatible
with hiding all framework callback sources.

## Normal response work and abnormal cleanup

Retain normal response transformation and normal return work around `next.run`.
The implemented middleware execution and termination signatures are:

```rust,ignore
// Implemented in Phase 4: normal around execution.
async fn handle(
    &self,
    exchange: &mut HttpExchange<'_>,
    next: HttpNext<'_>,
    cancellation: ExecutionCancellation,
) -> Result<(), HttpMiddlewareError>;

// Implemented in Phase 6: independent abnormal cleanup (default no-op).
async fn on_request_termination(
    &self,
    context: &mut HttpRequestTerminationContext<'_>,
    cancellation: CleanupCancellation,
) -> Result<(), HttpMiddlewareError> {
    Ok(())
}
```

Phase 6 retains one obligation per first-polled `handle` invocation.
Only an entered frame that did not return normally qualifies for termination.
A typed returned error is still a normal return for eligibility; it is not
silently changed into a forced interruption. A contained panic before normal
return is eligible when the execution/resource barrier has been proven.
Never enter/poll a future just to manufacture a cleanup obligation.

Termination is relevant to framework interruptions such as request timeout,
disconnect or forced shutdown, not just a server-wide force flag. It starts
only after execution and same-scope body/helper users stop. Calls are serial in
reverse invocation order. A normal-returned inner frame is not repeated when
an outer after section is interrupted. A termination invocation is not retried
after it fails, panics or runs out of time.

`HttpRequestTerminationContext` is a bounded cleanup view: request metadata,
the observed interruption, invocation identity/stage, retained per-invocation
state, cleanup deadline/view and DI access. It does not borrow the interrupted
execution stack, expose `HttpNext`, consume an interrupted request body or allow
a second transport response. Middleware must explicitly put state needed by
this hook into owner-retained invocation storage before it can be interrupted.
`Request.local` alone is not a framework ledger; application code can clear it.

Use `exchange.termination_state_mut()` to retain an aggregate cleanup value
before awaiting cancellable work, then inspect/take it through
`context.state_mut()` in the hook. These are distinct stores per invocation,
including repeated use of the same middleware instance. `invocation_id()` is
request-local. Managed callbacks always get storage; an ownerless direct call
returns `None`. Handle absent/partially initialized state in cleanup.

```rust,ignore
// Inside handle, before a cancellable await:
if let Some(state) = exchange.termination_state_mut() {
    state.insert(MyCleanupState { /* explicitly retained data */ });
}
// Keep normal response post-processing around next.run(exchange).await.

// Inside on_request_termination:
if let Some(state) = context.state_mut().remove::<MyCleanupState>() {
    state.finish(cancellation).await?;
}
```

The hook is abnormal notification, not an always-run async destructor. Normal
returns release retained state via Drop without invoking this hook. Do not put
input readers or response producers in the store: their release must precede
cleanup, so retaining them there leaves the request outstanding. Put body-lifetime
cleanup in its source or a DI-managed service. The cleanup context intentionally
cannot read input, run `next`, write another response or cancel its source.

The default termination hook is a no-op. Override it only when the middleware
requires this cleanup notification. Moving normal post-processing there would
change normal behavior. Adding a default callback cannot automatically finish
arbitrary code after an interrupted `await` or recover dropped async locals.

Each hook gets an independent child cleanup authority. Its timeout cannot
cancel sibling hooks, but the shared root cutoff may leave them unstarted.
The callback can be never started, partially polled, failed or panicked; none
is reported as completed. Partial-poll evidence describes framework progress,
not exactly which side effects occurred. Make cleanup safe for partial state.

The initial internal maximum is 250ms per hook, clipped to the existing request
cleanup deadline and root U; it does not restart the chain's budget. The signal
is requested up to 25ms before that same cutoff and the same future keeps being
polled during the remainder. Read the current cutoff from
`cancellation.deadline()`; the context's `cancellation()` observes the same
authority. A later root deadline may shorten it. No remaining budget means no
invocation. These caps cannot preempt blocking user code or synchronous Drop.
Cleanup-created framework file helpers retain separate real joins; missing
termination blocks outer callbacks and DI. Raw `tokio::spawn` stays user-owned.

## Request scope and response lifetime

Phase 3 replaces HTTP's `run_scoped` lifetime with an explicit retained owner.
Phase 5 now retains that scope through response-source lifetime, including
SSE, generic streams, transferred request body readers and framework-owned file
operations. This intentionally lengthens scope lifetime for streaming responses.
Plan connection limits and scoped-resource capacity accordingly.

Streaming/SSE action signatures do not change. Capture the accepted
`ExecutionCancellation` view when cooperative producer shutdown is useful. The
producer is polled only on protocol demand and continues polling its same
pending future after cancellation, bounded by the first-signal cooperative
window and root C. Its control path works without another Hyper poll. The
non-exhaustive `ResponseBodyError` adds `StreamInterrupted`, whose bounded code
is `RESPONSE_STREAM_INTERRUPTED`; it is a body failure, not a replacement HTTP
status after handoff.

After source release, streaming cleanup gets one local cap equal to the existing
request timeout; it still shares root U/R. Buffered cleanup keeps its original
request deadline. Dispose errors observed after head handoff are retained and
traced rather than replacing that response. Transport body metrics do not prove
scope cleanup or delivery; Phase 8 reports those boundaries separately.

Hyper receives copied, bounded frames and independent buffered bytes. This
preserves backpressure and prevents custom `Bytes` owners from extending scoped
resource lifetime into protocol buffers, at the cost of copying those bytes.
Request scopes can close while independent bytes drain or unrelated HTTP/2
streams remain open. Actual transport joins still gate parent cleanup.

Framework static-file open/seek/read operations retain real blocking joins even
when their caller disappears. Already-started work cannot be preempted by
`abort`; unconfirmed input/helper users block DI disposal and request retirement.
Calls made outside managed execution/body context and raw user tasks are outside
this tracking contract. No automatic migration of user-created background work
is introduced.

EOF alone is insufficient: captured scoped service references and helpers must
be released/terminal before scope cleanup. No async work is promised from
`Drop`. HEAD/204/304 or forced interruption may discard a source without its
first poll. Use RAII for safe synchronous release, retained termination state
for interrupted middleware, and DI disposal for DI-owned async resources.

A stream that fails after all middleware returned normally does not re-arm
those middleware hooks. Body-lifetime resources belong to the body owner or
request DI scope. Normal response return, body production and network delivery
remain separate outcomes. Graceful drain does not guarantee all later writes;
force may truncate an already handed-off response.

## Application-owned resources and guarantee boundaries

Controller, middleware and guard objects remain application-lived. Retain
singleton services in them; resolve scoped/transient dependencies in the
active request context. Directly created resource clients and raw
`tokio::spawn` tasks remain the application's cleanup responsibility and are
excluded from Lily's task/report inventory. Register application-lifetime async
resources as DI-managed services when Lily should own their disposal.

HTTP must observe every scope it creates even inside a caller-owned container,
but must not close that container or include unrelated scopes in its inventory.
The caller also owns externally configured tracing shutdown.

There is one total absolute shutdown deadline. Cancellation is a request;
abort is a request; only real return/drop/join/DI observations prove termination.
Hooks are best effort and bounded. A non-yielding poll/destructor or started
blocking task can remain outstanding after the budget, block dependency
cleanup and produce an immutable `Incomplete` attempt. No successful-delivery,
arbitrary user cleanup completion or zero-outstanding-at-any-deadline promise
is introduced by this migration.

## Available now: HTTP shutdown evidence and health

Phase 8 changes no callback signatures and adds no public report/handle API.
The existing `HttpHealthService::snapshot()` includes `http.shutdown`; after
shutdown its stable reason is `graceful_completed`, `forced_completed`,
`terminal_failed` or `incomplete`. DI uses `caller_owned`, `disposed`,
`disposal_failed` or `termination_unconfirmed`; owned telemetry likewise reports
its shutdown result separately. Readiness remains closed during shutdown.

A successful HTTP status or returned handler does not mean body/source, scope
or task cleanup completed. `terminal_failed` proves resources terminal while
retaining an attempt/cleanup failure; `incomplete` leaves termination or
accounting unconfirmed. Forced completion permits interrupted body/execution,
not failed required cleanup or unconfirmed joins. No client delivery is promised.

Fully retired historical request failures remain lifetime diagnostics and do
not fail a later HTTP shutdown. Owners/scopes still outstanding at admission
closure remain in that shutdown cohort. A separate failed owned-DI close still
fails shutdown. Caller-owned unrelated scopes and raw user tasks are excluded.

Repeated close and late joins preserve the first frozen report/error. Health
can observe the same root receipt after waiters disappear; it creates no new
task. With no observer, final join evidence is pending until the next observer;
a late observation cannot prove timely completion retroactively. Await the
managed lifecycle to obtain bounded terminal evidence. After owned telemetry
closes the final report stays in memory/health; the exported pre-close event is
explicitly preliminary. It cannot claim the root has already joined.
