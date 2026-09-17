# HTTP shutdown architecture and implementation status

**Phases 1–9 are implemented and qualified: evidence contracts, retained root/task/request
ownership, tracked Hyper execution, exact DI generation receipts, atomic
application admission and cooperative execution cancellation under one absolute
deadline, retained response producers, input release, file-helper joins and
reverse abnormal middleware termination, dependency barriers and actual telemetry
worker reconciliation, immutable HTTP attempt reporting and health integration.**
Phase 9 exercises the combined managed lifecycle and fixes integration failures
found by those tests. The flows below describe the implementation; the
qualification matrix distinguishes real protocol tests, controlled owner faults
and the limits those tests cannot remove.

See the [phase roadmap](SHUTDOWN_ROADMAP.md),
[migration contract](SHUTDOWN_MIGRATION.md), and
[qualification matrix](SHUTDOWN_QUALIFICATION.md). Testable Phase 1 evidence
predicates live in [src/lifecycle.rs](src/lifecycle.rs). Phase 2 connects task
join evidence through [src/tasks.rs](src/tasks.rs) and root cutoffs through
[src/shutdown.rs](src/shutdown.rs). Phase 3 supplies execution and scope evidence
through [src/request_lifecycle.rs](src/request_lifecycle.rs). Phase 4 attaches
read-only [execution views](../../foundation/lily_web_core/src/cancellation.rs) to the real
callback paths and linearizes accepted admission in that registry. Phase 5
connects [body ownership](src/request_lifecycle/body.rs) and
[input/helper receipts](../../foundation/lily_web_core/src/http_resources.rs).
Phase 6 connects [invocation ownership and termination](src/request_lifecycle/middleware.rs)
to the application/controller/action around chains.

## Current implementation: the source boundary

The audit used the working tree, including the completed WebSocket lifecycle
work. The current HTTP path, including Phase 8, is:

```text
AppBuilder::build
  -> optional App-owned tracing and DI
  -> application-lifetime controller / middleware / guard instances
App::{start, start_with_cancellation, close} (after first poll)
  -> AppLifecycleState -> one retained root receipt / run_http_lifecycle
  -> ManagedHttpServer -> retained listener receipt -> connection TaskSet
  -> TCP / optional TLS -> Hyper auto HTTP/1.1 or HTTP/2 connection
       -> tracked Hyper executor / connection and root join inventories
  -> serve_request -> RequestRegistry -> retained HttpRequestLifecycleOwner
       -> atomic shutdown gate + capacity decision + accepted identity
            (permit stays with the owner until its actual retirement)
       -> replaceable execution slot / original request deadline
            -> on stop: signal, then poll the same future until return or cutoff
            -> CORS transport policy
            -> dispatch_request -> Request with a bounded Incoming body reader
            -> App::call_with_outcome
                 -> publish owner-held request/response/ApplicationScope
                 -> capture generation observation directly from that scope
                 -> scope.run: middleware / guards / extractors / action
                      -> per-invocation first-poll ledger outside execution
                 -> normal middleware returns in reverse order
            -> response transport conversion
       -> confirm execution future destruction/return
       -> hand off response head and independent bytes / pull-driven BodyBridge
       -> retained producer polls stream / SSE / static file on demand
            -> signal then bounded cooperative polling, independent of Hyper polls
            -> body outcome, actual source destruction, bridge detachment
       -> release retained request and remaining partial response state
       -> confirm transferred input release and actual file helper joins
       -> reverse middleware finalization under the original scope
            -> normally returned: release retained state, no abnormal callback
            -> interrupted: bounded on_request_termination with isolated authority
            -> confirm hook destruction / helper joins / retained state release
       -> one retained scope-close result + actual generation termination
       -> owner join (independent of the head waiter)
  -> BoundedResponseBody in Hyper holds independent bytes and bridge channels
  -> protocol / transport completion
```

| Source | Established behavior and limitation |
| --- | --- |
| [app/app.rs](src/app/app.rs), `install_root`, `run_http_lifecycle`, `HttpServerLifecycleHandle` | Start/close callers observe one separately owned root. Listener/drain, post-build failure cleanup, DI and tracing waits inherit root cutoffs. Request/scope/helper reconciliation supplies the dependency barrier. Releasing an already joined server owner cannot trigger another force request. |
| [server/server.rs](src/server/server.rs), `run`, `drive_connection`, `drain_connection_tasks_before` | Listener and connection actual joins are retained. Graceful asks Hyper to drain until G. Escalation signals execution first; connection abort waits for slot termination or S. Owners retain cleanup and joins even if a blocked destructor prevents confirmation. Peer loss can terminate transport earlier without discarding request ownership. |
| Same file, `TrackedHttpExecutor`, `connection_builder_with_executor` | HTTP/1.1 keep-alive and HTTP/2 multiplexing remain supported. Managed-runtime Hyper workers register actual joins in both their connection and application inventories before protocol/user work is polled. Connection joins alone are not accepted as worker proof. The hidden standalone builder remains a conformance seam, not the managed lifecycle entry point. |
| Same file, `serve_request`, `serve_owned_request` | Candidate ownership precedes application work. Capacity and accepted admission are decided under the gate lock; a rejected request creates no scope and runs no CORS/user callback. An accepted slot observes cancellation and continues polling within one bounded cooperative window, retaining lifecycle state after stop. |
| [app/middleware_executor.rs](src/app/middleware_executor.rs), `HttpMiddlewareChain::run`; [HttpMiddleware](../../foundation/lily_middleware/src/http.rs) | Normal `handle` keeps around semantics and execution cancellation. The retained invocation ledger records first poll, next/after boundaries and normal return separately from the stack observation guard. Interrupted frames run `on_request_termination` in reverse order; normal-returned frames never reopen on later body failure. |
| [request_lifecycle.rs](src/request_lifecycle.rs); [body owner](src/request_lifecycle/body.rs); [DI implementation](../../foundation/lily_injection/src/application_container.rs), `ApplicationScope` | Request state, scope, exact observation and original close future survive execution/service-waiter loss. Lazy source destruction, detached scope-free bridge, input releases and actual helper joins precede scope disposal. Unconfirmed release blocks retirement and parent cleanup. |
| [server/server.rs](src/server/server.rs), `BoundedResponseBody`, `RequestTaskGuard` | Managed Hyper bodies contain copied bytes and demand/result channels, not user sources. Transport body counters are not request/scope terminal evidence. Write watchdog stays active in graceful/idle drain; connection-wide fallback distinguishes its timed-out stream and cancelled siblings. Socket delivery/flush is not observable at this body seam. |
| [response sources](../../foundation/lily_web_core/src/response), [resource receipts](../../foundation/lily_web_core/src/http_resources.rs) | Generic streams, SSE and transferred input readers share retained request ownership. Static-file open/seek/read blocking work publishes a real join before its execution gate opens. Abandoned results are destroyed in that worker before its join; started work is not assumed aborted. |

Current graceful flow is `close application admission + stop TCP admission
-> accepted execution and Hyper graceful drain
-> connection, protocol and retained request/scope joins
-> eligible dependency / tracing phases -> final reconciliation -> root join`.
On G expiry or force it becomes `signal accepted execution -> keep polling the
same slot within its cooperative allowance -> confirm actual slot destruction
(or retain outstanding evidence at S) -> connection abort / retained request
finalization -> bounded actual joins -> eligible remaining phases`.
Application admission applies equally to new TCP requests, HTTP/1 keep-alive
and HTTP/2 streams; protocol shutdown may reject/close them before the service
runs. HTTP/1 application-gate rejection includes `Connection: close`; HTTP/2
rejection affects the late stream without inserting a forbidden connection header.

Both flows retain body/input/helper and middleware prerequisites. The force-safe
DI adapter reuses the same typed close receipt; it cannot skip an owned obligation
because the normal callback was bypassed. Reconciliation runs before dependencies,
including actual monitor joins. Owned tracing additionally requires owned DI
quiescence. The root checks real dependency/exporter joins, not just coordinator
return. The full HTTP-specific immutable aggregate is published after root join
or the bounded final observer cutoff.

## Implemented Phase 2 owner boundary

```text
start / close waiter(s)
  -> AppLifecycleState
       -> canonical root TaskReceipt (one claim, repeatable result)
       -> root-owned task inventories
            -> listener -> connection TaskSet
                 -> connection-specific Hyper worker inventory
                      -> same actual joins retained in root protocol inventory
            -> cancellation / signal monitor receipt
       -> first shutdown timestamp -> shared G..H cutoffs
```

`TaskRegistry` registers the real `JoinHandle` before opening a publication gate.
There is no spawned receipt driver. Native `SignalMonitor` is the exception to
that gate: its installer already owns the task, then transfers the live handle
directly to the HTTP monitor inventory. No join handle is discarded during that
transfer. Task completion streams and timeout waiters only clone retained
receipts; dropping one does not destroy the registry's observation.

`abort_requested` is separate from joined completion/cancellation/panic. Registry
retirement observes the actual join and counts it once; an outer joined return
is not automatically a successful returned `io::Result`. Protocol panic is
terminal evidence but makes HTTP shutdown fail. Root result replay also keeps
failure: a later join can be observed without overwriting an already frozen
timeout. The root never attempts to join itself; the lifecycle waiter observes
that final receipt.

A successful listener join reconciles the durable initial shutdown signal before
being classified as an unexpected stop. A different lifecycle observer can close
admission after the trigger branch was polled, allowing the listener to join
before the root observes its notification. Signal-driven completion follows the
normal cleanup path; a join without a recorded signal or a returned listener
error remains a failure.

Once a start future has been polled and installed its root, dropping that waiter
requests shutdown and leaves the root running. `App::close().await` starts the
same root for a built but unstarted App without binding, or observes the existing
root. Concurrent close calls cannot create another shutdown attempt. An entirely
unpolled async start/close future installs nothing; synchronous `Drop` still does
not promise async cleanup. Caller-owned DI and external tracing remain external.

If a task or destructor does not yield, the absolute wait may expire while the
actual join remains outstanding. The registry retains it and transport-dependent
cleanup is blocked. Pending connection/protocol futures retain the App's
dependencies, but this does **not** repair the early request-scope close shown
above. The next section describes the Phase 3 request boundary.

## Implemented Phase 3 request boundary

```text
Hyper service waiter -- drop --> internal request stop request
       | response + actual owner join
       v
root RequestRegistry (survives connection/worker drop)
  -> request identity + actual owner task receipt
  -> HttpRequestLifecycleOwner
       -> replaceable dispatch slot
            -> CORS / around middleware / guard / extractor / action
            -> borrows owner-held request/response/scope
       -> RequestExecutionContext / retained resources and evidence
            -> ApplicationScope + ProcessContext
            -> creation-bound DI observation
            -> original shared close future and result
       -> release request captures after execution is terminal
       -> observe scope disposal result AND generation termination
       -> owner join / capacity permit release
```

`RequestRegistry::registered` counts service candidates, including capacity
rejections that never create a scope or enter a dispatch slot. Phase 4 adds separate `admitted` and `admission_rejected` observations;
`registered` remains a candidate/owner count, not an accepted-request count. Both the task join and request identity are published before a factory can
run. CORS dispatch carries the same internal owner explicitly; it cannot open an
independent scope behind the registry.

The service waiter can request a stop but cannot abort the owner. A timeout,
transport force or lost waiter signals execution; Phase 4 gives a bounded
cooperative window before destroying a pending dispatch slot. Contained poll
panic is also recorded. Terminal evidence is recorded **after** that actual
future's destructor returns. A blocked or panicking destructor leaves the
request/scope evidence outstanding and prevents parent disposal. Context and
request captures are released under the original `ProcessContext` before DI
close. Phase 3 supplies storage ownership; Phase 6 adds the retained entered
middleware ledger and termination callbacks on this boundary.

The exact observation comes from the `ApplicationScope` returned by creation,
not a subsequent lookup by process ID. Its late close also cannot close a
successor generation. The shared close future is installed once, survives waiter
expiry and separately records its original result and deadline-stop result.
Actual DI task joins must also finish before the receipt becomes terminal.
If another DI owner already claimed the close ticket, termination may be known
while the disposal result remains `Unknown`; HTTP does not label that success.

Normal scope close uses the original request deadline. An interrupted execution
gets one independent local cleanup allowance of the configured request timeout,
starting only after confirmed slot destruction. Both are clamped by U/R from the
existing root attempt. No request receives another full application shutdown
timeout. Neither the execution signal nor expiration of its cooperative window
cancels scope cleanup. Phase 4 also grants this separate local cleanup allowance
when signalled execution returns normally; an expired request deadline cannot
immediately kill that disposal.

When transport producers join, the root seals the request registry, observes its
owner joins and exact DI generations, then permits dependent cleanup. Capacity
permits remain retained until this per-request evidence is terminal. Caller-owned
DI stays open and unrelated caller scopes are excluded. A terminal disposal
failure remains a failed cleanup outcome even after the owner task joined.

Phase 3 alone supplied no response terminal proof. Phase 5 now retains the scope
through source/bridge/helper release; Phase 7 completes dependency/telemetry
reconciliation and Phase 8 provides the full HTTP aggregate report.

## Implemented Phase 4 admission and cancellation boundary

```text
service candidate -> retained owner receipt (before any user work)
  -> registry gate lock: shutdown gate + capacity + accepted identity
       -> rejected: bounded 503, no scope/user/CORS execution
       -> accepted: one ExecutionCancellation authority
            -> CORS context -> request/exchange -> middleware/guards/extractors/action
            -> same signal through normal after-code and IntoResponse
                 -> return normally before G/local expiry
                 OR
                 -> force / G / local request timeout / service-waiter loss
                      -> publish first stop reason + read-only signal
                      -> poll the same pinned dispatch future
                           -> returned/error/panicked: preserve actual result
                           -> pending at min(first signal + 250ms, root C): drop slot
                      -> confirm destruction -> independent retained DI cleanup
```

The registry distinguishes a framework-only candidate from an accepted request.
Gate closure and accepted publication share its mutex. A candidate registered
before closure can still lose admission; no user future has been entered yet.
Late candidates are rejected before spawning. A permit remains with an accepted
owner through actual join/scope retirement. `App::close`, lifecycle stopping,
force and transport shutdown close the gate; admission also checks durable root
stopping state under the same lock. Closing admission alone never cancels an
accepted execution.

The execution view lives in `lily_web_core`, below middleware and HTTP. Request
storage is independent of `Request.local`; clearing local values cannot remove
the signal. Callback parameter and request/exchange context views come from the
same owner. Dynamic CORS receives it before request scope creation. Generated
action adapters use the existing generic `FromRequestParts` path, including
aliases; no WebSocket token or artificial HTTP connection hook is introduced.

The 250ms cap is an internal initial policy, not a minimum grant or a new total
shutdown timeout. First signal time is fixed; repeated cancellation, force or
root observation cannot restart it. A newly installed root can shorten an
existing local window. A delayed scheduler or already-spent C can leave no
cooperative polling time. Unstarted, already-stopped slots are dropped without
first-polling callbacks. The framework never polls arbitrary hooks merely to
manufacture invocation evidence.

Cancellation request and execution result remain independent. Cooperative code
may finish its ordinary reverse middleware return, preserving its actual response
or application error for either timeout or shutdown cancellation. A local request
timeout selects **504 Gateway Timeout** only when execution remains incomplete
at the cooperative cutoff. Its absolute execution deadline is published once at
accepted admission; dispatch and individual request-body reads do not restart it.
Input readers defer cancellation and the polling window to that owner, so they
cannot replace its managed deadline with a separate body-read timeout response.
Lazy response production and transport writing share the admitted deadline and
the remaining first-signal cooperative window. A pipeline can return a stream
after notification; that source can start and finish within the remaining
window, without receiving a fresh timer. The resource-free clock stays with
protocol control even after independent buffered bytes let the request owner
retire. See [response deadline policy](#one-admitted-deadline-through-response-writing).
Scope cleanup has its own retained deadline/receipt. User callbacks may ignore the
signal, block, panic or have partial side effects; no preemption or rollback is
promised. Raw user tasks remain outside the inventory.

Phase 4 controls dispatch through response creation. Phase 5 extends the same
read-only view to retained producers. Phase 6 adds `on_request_termination`
under independent cleanup authority. Phase 7 adds dependency/telemetry reconciliation;
Phase 8 supplies the immutable HTTP aggregate below.

## Implemented Phase 5 body, input and helper boundary

`RequestWaiter::handoff` observes the response independently of the retained
request task join. `HttpRequestLifecycleOwner` then drives its producer slot
and exact scope cleanup. Losing the service future or Hyper body can signal
the owner, but cannot destroy its producer, request resources or DI receipt.

```text
execution returns a representation
  -> buffered: copy independent bytes; production resources are released
  -> streaming: retain ResponseBodyStream in the request owner
       Hyper BodyBridge -> at most one demand -> one bounded copied frame
       no demand -> no speculative source poll
       source EOF/error/stop -> observed disposition
         -> destroy source/captures under the original ProcessContext
         -> bridge can no longer invoke scoped user code
  -> destroy retained request/partial response; observe input release
  -> seal helper producer inventory; observe actual blocking joins
  -> exact scope close result and disposal termination
  -> request owner join / retirement
```

The bridge has no user stream, request, scope or callback. Even
`Bytes::from_owner` captures are detached by copying each bounded transport
frame; buffered representations are copied at handoff too. This adds bounded
allocation/copy overhead. Backpressure still limits the bridge to one outstanding
demand/result; `ResponseBodyStream` retains its existing source-chunk, total-byte
and exact-length limits. Buffered production completion is distinct from its
later transport-frame accounting and socket delivery.

Force/G expiry, peer loss and the original admitted request deadline signal the
same read-only execution view captured by the source. The same pinned producer
future continues under demand for at most the remaining first-signal 250ms cap,
clipped to C. Control timers run in the owner even when Hyper never polls again.
Unstarted sources need not be polled; suppressed HEAD/204/304 representations
are explicitly `NotStarted`. Poll panic, source error, interrupted production
and actual source release remain distinct observations. A blocked or panicking
source destructor cannot authorize DI close. A later body failure does not
reopen normally returned middleware (Phase 6).

The input receipt follows the concrete reader when `BodyStream` moves into an
output source. EOF alone is not reader release. A reader escaped to application
storage can keep the scope outstanding; Lily does not adopt an associated raw
user task. Input destructor failure stays unconfirmed; an enclosing handler
panic alone does not make a successfully released input fail.

Static-file open and each bounded seek/read operation use the internal
`HttpResources` inventory. The blocking task waits for its actual join receipt
to be published before running. Caller cancellation abandons only the typed
result receiver. A late result is destroyed inside the worker before its join;
queued abort requests and actual cancelled joins are counted separately.
Started blocking work remains outstanding until its real join is observed.
No `tokio::fs::File` hidden read task or detached cleanup pump remains on this
managed path. File operations outside a managed request context remain owned
by their caller. The inventory does not propagate into raw `tokio::spawn`.

`request_timeout` includes time before streaming begins. No producer or protocol
stage can restart it or the first-signal cooperative window. After producer
release, one local DI/helper wait allowance equal to that setting
is anchored once; U/R still clamp every cleanup/observation. No body, read or
helper installs another root timeout. Failure to observe children keeps the
scope and dependency keepalive retained and produces an incomplete result.

The response watchdog remains polled during graceful and idle drain. Deadline
expiry first signals the request clock; an incomplete committed response stops
at the shared cooperative cutoff. HTTP/1 stops the connection; HTTP/2 stops the
actual request worker, allowing healthy siblings to continue. Genuine socket
failure can affect every stream. Body guards observe production/handoff; the
separate protocol controls observe local flush and retained task joins. None of
these observations is a delivery guarantee.

After head handoff, scope failures are retained and traced, never rewritten into
a second HTTP response. Phase 5 emits source disposition/release diagnostics and
retains helper/input observations. The complete immutable HTTP aggregate report
is implemented in Phase 8 below; owned dependency/telemetry and late reconciliation are implemented
in Phase 7. Middleware termination is integrated in Phase 6 below.

## Implemented Phase 6 middleware termination boundary

The [request-owned ledger](src/request_lifecycle/middleware.rs) survives dispatch
drop. The production [chain adapter](src/app/middleware_executor.rs) registers a
distinct invocation for every application/controller/action position, including
repeated use of the same application-wide instance. Its first poll records entry
before calling user code. A task-local binding connects nested chain adapters to
the same request ledger; it is not stored in `Request.local` and is not propagated
to raw user-spawned tasks. Merely creating a future does not enter a callback.

Normal `handle` still wraps `next`. `Before`, `DelegatingToNext` and `After`
describe observed await boundaries, not precise side effects. Any returned
`Result`, including a typed error, marks `NormalReturned` before framework error
materialization. That frame is never reopened by subsequent response failure.
Only entered frames without a normal return qualify for the separate default
no-op `on_request_termination`. The owner may invoke it after request timeout,
peer loss, force, a contained poll panic or application abandonment of an inner
`next`; it does not attempt to replay normal response processing.

```text
execution future actually returns / drops
  -> retained source actually releases / protocol bridge becomes scope-free
  -> original Request and partial Response release
  -> input receipts and execution helper joins confirm termination
  -> reverse invocation finalization, under original ProcessContext
       -> normal-returned: release retained state; no abnormal hook
       -> eligible: independent bounded on_request_termination
            -> actual hook future destruction
            -> cleanup helper joins
            -> retained invocation state release
       -> next outer invocation
  -> exact DI close receipt and generation termination
  -> request owner join and retirement
```

`HttpExchange::termination_state_mut()` is a per-invocation typed store outside
the execution future. `invocation_id()` identifies that store within the request.
Cleanup receives only bounded metadata (method 32 bytes, path 2048 UTF-8 bytes),
identity/stage/interruption, that retained state, original provider access and
the same `CleanupCancellation` view as its parameter. It receives no input,
response or next capability. Do not retain execution input readers or response
producers in this store: their release is a prerequisite. Escaping such a
resource blocks cleanup and is reported outstanding, not bypassed. State and
framework helpers remain owned even if a cleanup waiter disappears.

One retained shared finalizer serializes the chain without spawning cleanup
tasks. Each invocation has an initial internal **250ms maximum**, shortened to
the existing owner cleanup deadline and root U. It signals its own read-only
authority up to **25ms before that same cutoff**, keeping the same hook polled
through the remaining time. These are caps, not guaranteed grants or new public
timeout settings. Installing H later shortens the live timer and published view.
Expiry never cancels a sibling, and zero remaining time records `NotStarted`
without creating/polling its callback. Hook polling ends by U; actual helper/DI
receipt observation may use the existing R reserve. The chain and DI share U; many hooks can
exhaust it and leave later hooks/disposal incomplete.

Completed, failed, panicked, timed-out and unstarted dispositions are retained
per invocation and counted in the request registry across retirement. Bounded
structured tracing accompanies them; Phase 8 aggregates them in the immutable
HTTP report below. A failed callback can be terminal without making cleanup successful.
Poll panic/error/timeout continues to the next outer frame only after destruction
and helper receipts confirm release. A blocked or panicking destructor, or an
unjoined helper, keeps parent hooks, DI and owner retirement blocked. Retrying
receipt observation after late child release preserves original failure and
never retries a settled hook. Arbitrary blocking code, panic-abort and double
panic during unwinding cannot be preempted or made recoverable by this model.

See [Phase 6 qualification](SHUTDOWN_QUALIFICATION.md#implemented-phase-6-middleware-qualification)
and [callback migration](SHUTDOWN_MIGRATION.md#normal-response-work-and-abnormal-cleanup).
Phase 7 below completes the force-safe dependency close, telemetry join and
late reconciliation obligations tracked by F008/F013.

## Implemented Phase 7 dependency boundary

```text
server drain (normal or forced)
  -> listener join -> connection joins -> protocol/helper joins
  -> request owner + exact scope generation reconciliation
  -> stop monitors -> confirmed monitor joins                   [R]
  -> one owned DI close receipt (normal and force share it)      [D]
  -> DI quiescence, including admitted resolutions/cleanup jobs
  -> owned tracing close                                       [T]
       -> log/span joins, bounded provider work
       -> owned metric OS worker + file OS worker joins
  -> original dependency/telemetry receipts reconciled          [H]
  -> root actual join / retained failed attempt
```

[HttpDependencies](src/app/dependencies.rs) owns the typed container result and
actual task join; neither a timed-out waiter nor a force transition consumes
that obligation. `HttpReconciliationHandle` runs after server drain and before
DI. Terminal failures permit safe parent cleanup but remain failures; outstanding
children block parents. Caller-owned DI is not closed and unrelated caller scopes
are not included. External tracing is not observed or shut down.

A returned request owner can leave a retained finalizer behind an input/helper
barrier. `RequestRegistry::wait` now resumes that same finalizer after late
release. It retains hook outcomes and the original U/R cutoffs, never invoking
settled hooks again. Repeated `close` observes late receipts without starting
another dependency close or rewriting the frozen root result.

[Build ownership](src/app/build_lifecycle.rs) keeps startup on its original task.
Dropping a pending build hands its exact DI transaction/container and tracing
owner to a tracked rollback task. DI initialization is cancelled before reverse
rollback; telemetry waits for its actual quiescence. The process registry owns
the rollback join even when no build waiter remains. Explicit failure rollback
uses the same owner/deadline policy. As elsewhere, entirely unpolled async build
creates no resources and synchronous/non-yielding destructors cannot be preempted.

[Tracing receipts](../../integrations/lily_trace/src/lifecycle.rs) retain the adapter join after
all component waiters disappear; no detached receipt driver is spawned. Exporter
and started provider blocking tasks retain their real joins. The JSONL worker
uses a bounded queue whose shared gate closes all writers, then drains and joins
the actual thread. The metrics adapter uses the SDK `ManualReader` for aggregation
and a framework-owned periodic thread; an SDK acknowledgement is not its join.
Its configured interval/OTLP exporter remain in effect. The private adapter enables
`experimental_metrics_custom_reader` on the already pinned SDK version.

T expiry records an incomplete attempt; H only observes remaining joins, without
retrying export/disposal or opening another budget. Started blocking I/O, metric
callbacks and synchronous destructors may remain outstanding; their owners are
retained. File queue closure prevents later writers from using disposed output.
SDK/network implementation details and remote collector delivery are not inferred
from these joins. Owned dependency cleanup that cannot safely start also retains
its owner instead of triggering a new Drop fallback timeout.

Phase 8 below aggregates these receipts with request/body/scope/task evidence.

## Implemented Phase 8 report and health boundary

The [HTTP report](src/shutdown_report.rs) contains values, never resource
handles. [Request accounting](src/request_lifecycle/report.rs) retains two
separate aggregates:

- Lifetime diagnostics preserve all retired request observations, including errors.
- The shutdown cohort consists of owners still outstanding when the request
  admission gate first closes. Under that same mutex, already-terminal records
  retire before the cohort is fixed. Outstanding scopes/helpers remain included
  even if their handler or owner task already returned. Candidates count as
  registered owners without inventing accepted execution; admission refusals
  are separate counters, not a second owner disposition.

This is the actual gate-close observation, not a reconstructed native-signal
history. Native signals still anchor the original root deadline. Later observers
cannot add a new cohort or extend that deadline. An ordinary failure fully
retired before this gate-close does not fail HTTP shutdown; an outstanding
cleanup failure in the cohort does. A failed owned-DI close is independently a
shutdown failure, including any prior failures its canonical DI receipt retains.

Each identity contributes exactly once to `registered = retired + outstanding`.
A single observation moves its complete counters into retired totals and
releases its capacity/App keepalive. No unbounded history of retired request
IDs, routes, user labels or errors is retained. Per-request execution outcomes,
first stop-reason counts, middleware interrupted stages and cleanup outcomes,
body disposition/source release/bridge detachment, exact scope outcomes and
actual helper/owner joins reconcile independently. `head_handed_off` is the
service handoff boundary; no `headers_sent` or client-delivery counter is invented.
Streaming/SSE source outstanding is distinct from handler completion.

The canonical listener/connection/protocol/monitor task registries retain
**lifetime** task facts. Their local copies are not summed into the application
inventory. An ordinary HTTP error is not a task panic; observed framework task
panics remain lifecycle failures. DI result, DI quiescence, adapter joins and
tracing worker joins are separate. `NotOwned` means this HTTP composition root
has no cleanup authority; it does not claim a caller's DI/tracing is closed.
Only exact HTTP-created scopes enter request accounting with caller-owned DI.

```text
Gate closes -> fix outstanding request cohort
  -> drain / cooperative stop / cleanup / actual receipts
  -> preliminary checkpoint before owned telemetry closes
  -> root finishes (pre-join values retained)
  -> actual root join observed OR absolute observer cutoff
  -> publish one immutable { io::Result, HttpShutdownReport }
  -> existing HttpHealthService snapshot
```

[Root publication](src/app/report.rs) uses one shared `OnceLock` for result and
report. A late root/DI/helper join can reconcile retained resources, but cannot
rewrite the frozen attempt. Health can observe the same actual root receipt
when start/close waiters disappear. Its fallback retains values and the root
receipt, with weak App/health references, so it neither retains the completed
DI graph nor creates a detached receipt-driver task. There is no background
report timer: with no observer the pre-join checkpoint remains pending; the next
start/close/health observer finalizes evidence. A first observation after H does
not retroactively prove a timely join. Outside a Tokio context health can use
completed pre-join values and a real ready root receipt, without polling live
cleanup or starting timers.

`HttpHealthService` adds the bounded `http.shutdown` check with final reasons
`graceful_completed`, `forced_completed`, `terminal_failed` or `incomplete`.
DI and owned telemetry checks distinguish disposal/termination failure from
successful cleanup. Replays preserve the first result and health generation.
Shutdown keeps readiness closed. The detailed report remains internal; there
is no new public callback, token or report API. Structured diagnostics use fixed
fields and codes. Owned telemetry exports only the preliminary checkpoint;
final evidence is retained in memory/health after it closes, avoiding new export
loss caused by announcing that close. External telemetry may receive the final
event. No final-export or remote-delivery guarantee is inferred.

## Phase 9 managed-runtime qualification

[Managed tests](src/app/qualification_tests.rs) connect the actual listener,
generated controller/guard/extractor/middleware path, body/input/file source,
request scope, application DI and final joined report. They include HTTP/2 flow
control and concurrent streams, SSE truncation, pending cleanup, caller-owned DI
and disconnect races. The isolated [telemetry test](tests/shutdown_dependencies.rs)
checks request disposal -> application disposal -> preliminary checkpoint ->
actual file-worker shutdown and final health.

These tests exposed two integration failures:

- `ManagedHttpServer::drop` unconditionally requested force even after `wait`
  had consumed its actual join. Completed owners now release without changing
  shutdown intent. The premature-drop fallback still requests cancellation and
  preserves task receipts.
- The generic coordinator's default short force reserve reduced HTTP's early
  force wait to the D/T tail (3% of H), cutting off the coordinator before the
  accepted execution's cooperative window and middleware cleanup could finish.
  HTTP components now provide their original absolute R/D/T force cutoffs.
  The coordinator clamps these to its hard deadline and configured component
  cap. Components without an owner cutoff retain their existing default policy;
  WebSocket behavior is unchanged. No hook receives a new timeout.

Graceful-G escalation also records `GracefulDeadline` before broadcasting the
transport force signal; an explicit force request keeps `ForcedShutdown`.
The first observed request reason remains immutable.

## Scope and ownership contract

The ownership graph is HTTP-specific. Names below describe responsibility
boundaries; the root, transport tasks, request execution/resources, body/input/
helper users, middleware invocation ledgers and exact DI receipts currently
have runtime ownership, as do the Phase 7 dependencies and telemetry workers.
Phase 8 retains the immutable aggregate alongside the root result.

```text
App lifecycle control block (survives dropped start/close waiters)
  -> canonical root task receipt and immutable shutdown-attempt report
  -> HttpApplicationOwner / shutdown coordinator
       -> listener task receipt
       -> connection registry
            -> HttpConnectionSupervisor
                 -> TCP/TLS/Hyper transport slot
                 -> tracked Hyper executor task receipts
                 -> associated request identities
       -> request registry (retained independently of connection futures)
            -> HttpRequestLifecycleOwner
                 -> RequestExecutionSlot
                 -> entered middleware invocation ledger
                 -> retained metadata and per-invocation cleanup state
                 -> request-body ownership evidence
                 -> optional response producer slot
                 -> response transport bridge receipt
                 -> ApplicationScope and exact-generation cleanup receipt
                 -> framework execution/body helper receipts
       -> remaining framework task receipts
       -> owned DI close receipt
       -> owned telemetry close receipt
```

| Responsibility | Exclusive owner / required observation |
| --- | --- |
| Application and request admission | Root admission authority; registration and closing admission have one linearization point. TCP acceptance alone is not accepted application execution. |
| Accepted execution | Request lifecycle owner holds the execution slot. Neither a service waiter nor the connection future may destroy its ledger/scope authority. |
| Body source | Request owner retains the producer and its captures until source destruction is observed. A pull-driven bridge preserves protocol backpressure. |
| Middleware cleanup invocation | Request owner retains one entry per actual invocation and executes eligible termination callbacks serially in reverse order. |
| Execution cancellation | Root sets phase limits; the request owner signals its execution authority for shutdown or a local interruption. The signal alone never drops the slot. |
| Cleanup cancellation | Independent root cleanup authority; the request owner creates a child authority per termination invocation. User views cannot cancel it. |
| DI scope disposal | DI runs disposal; HTTP retains and observes the exact scope generation's receipt and the disposal result. Caller-owned DI does not remove this HTTP obligation. |
| Final abort | Coordinator authorizes escalation within the root budget; the appropriate slot/task owner requests it and retains its confirmation. Only confirmed slot destruction or an actual joined task is terminal evidence. |
| Shutdown completion | Root reconciles inventories and dependency receipts. The lifecycle control block observes the canonical root join; the root cannot claim to have joined itself. |

Constructors still produce application-lifetime controller, guard and middleware
objects. Singleton service references may be retained there; scoped/transient
services belong to active DI contexts. Directly created clients and raw
`tokio::spawn` work remain application-owned. They are outside Lily's shutdown
inventory. Only resources actually owned through DI, lifecycle scopes or
framework tracking are covered; do not introduce a new implicit application
object async-disposal contract.

## What “request terminal” means

Admission, user execution, response production, resource release and cleanup
success are distinct dimensions. A request owner can finish when all of these
are accounted for:

1. Its execution slot has returned, unwound under containment, or been fully
   dropped. Any separately spawned execution task has an actual join receipt.
2. Its input reader and any transferred input producer no longer use the scope.
3. The response producer has a final disposition **and** its source/captures
   have been released. No transport bridge can invoke scoped user code again.
4. Every entered middleware obligation has a final normal or eligible
   termination disposition. A skipped/timed-out hook is a failure disposition,
   not successful cleanup.
5. Every execution/body helper has actually joined. Started blocking work is
   outstanding until its result/join can be observed.
6. Its exact DI scope generation is terminal; the disposer result remains a
   separate success/failure observation.

The root additionally requires the request-owner task join and the appropriate
connection/protocol/helper joins. A request does **not** wait for the whole
HTTP/1.1 keep-alive connection or unrelated HTTP/2 streams before closing its
scope. Its barrier is the absence of remaining scoped users, not TCP closure.
Independent encoded bytes may still belong to the protocol. Their disposition
belongs to transport reconciliation, not a fabricated client-delivery result.

`ResponseHeadEvidence` records `NotHandedOff` or `HandedOffToService` only:
the owner has returned a response to the service adapter. Final selection for
Hyper's encoder is a later, separate `ResponseCommit::CommittedToProtocol`
observation. Neither observation proves headers reached the socket or client.
`ResponseBodyOutcome::Completed` means production ended; EOF does not imply
source destruction. `NotStarted` is an explicit disposition for suppressed or
discarded unpolled sources, never a default assumption about missing work.

`RequestTerminationEvidence` checks these barriers over a coherent, complete
owner snapshot. It deliberately has no task spawning, registry, timeout or
public report implementation. Its caller must retain every actual receipt;
an empty/incomplete inventory cannot be used to manufacture terminal proof.
`cleanup_succeeded()` concerns middleware/DI cleanup only, not HTTP status,
delivery or the aggregate shutdown outcome.

## Request-linked response transport control

Each managed request binds a `ResponseTransportControl` when Hyper first polls
its service future. The binding uses a task-local receipt published before the
actual task starts; it does not infer stream identity from spawn order, request
order or Hyper's private future types. The request owner retains this control
independently of the service waiter and response body.

```text
owner returns response -> HandedOffToService
  -> service selects final response under the transport control
  -> CommittedToProtocol -> Hyper encodes headers / polls bounded frames
       -> HTTP/1: final frame + successful I/O flush
       -> HTTP/2: queued DATA release + subsequent I/O flush
                    -> release body EOF -> observe actual stream-worker join
```

Commit and a final transport stop are serialized. A stopped transport cannot
commit another response. Once committed, this adapter cannot substitute a
different 504/503 response, even if Hyper has only buffered the original head.
The conservative commit boundary is encoder handoff, **not** `headers_sent`.

The final stop authority is protocol-specific:

- **HTTP/1.1:** stop its connection driver. Driver destruction releases I/O;
  the retained connection task must still actually join. A successfully flushed
  response retires its write watchdog while keep-alive stays open. Its stale
  control cannot later close the connection for a subsequent request.
  The selected stop reason is retained: request timeout or explicit force is
  not classified as an independent response-write timeout.
- **HTTP/2:** request abort of that request's actual stream worker. The
  connection driver keeps running to service other streams and protocol resets.
  `abort_requested` is separate from a joined cancellation; the lifecycle owner,
  execution slot, middleware ledger and DI receipt are not part of that worker.
  Dropping its service waiter signals the retained execution owner, which still
  applies its cooperative window and resource prerequisites.

Hyper can enqueue a whole DATA chunk after obtaining only partial stream
capacity. Therefore handing over the last frame is insufficient: the body keeps
EOF pending until h2 releases all queued DATA owners and a later I/O flush
succeeds. Empty responses also wait for a flush after the body is first polled,
when Hyper has queued their head. Framework-only bounded byte owners carry this evidence. They contain
no user callback, DI scope or request context. The registered stream worker stays
abortable under a zero/partial flow-control window and a blocked final flush.
The body-tail wait creates no task and restarts no timeout. It uses the same
admitted request clock as execution and production.

These facts remain distinct: source EOF, frame handoff, queued DATA release,
local I/O flush, stream-worker join, connection-task join, and remote delivery.
In particular Hyper's H2 worker returns `()` even on an internally handled
protocol error; an ordinary task join alone cannot mean successful delivery.
Final protocol framing (including HTTP/2 END_STREAM) may still be buffered after
the body worker joins; the connection driver retains those independent bytes.
This per-request receipt does not claim that every final framing byte was sent.
Reset emission also requires progress on the shared connection. A failed or
blocked socket can prevent the peer from observing it. HTTP/2 stream-local stop
does not imply isolation from genuine connection-wide transport failure or final
application shutdown.

Live request identities retain controls until their transport boundary is
observed. Completed identities are replaced with fixed counters; there is no
per-request historical event list. The connection trace includes these
transport observations separately from existing frame-handoff counters. The
application task inventory remains the authority for unique actual task joins.
Protocol-buffer metadata is bounded by live frames and existing send limits.

## One admitted deadline through response writing

`HttpTransportConfig::request_timeout` starts at accepted admission and includes
request input, CORS, middleware, guards, extraction, the action, normal return
work, lazy source production and protocol writing. JSON/buffered responses,
static files, streaming bodies and SSE all use this policy. Header-read and
connection-idle limits still protect their separate transport stages.

```text
admission publishes request deadline
  -> execution / request input / normal middleware return
  -> returned representation / lazy production / protocol writing
       each stage observes the same clock
  -> local expiry or force/G: first reason + execution signal
       continue the same work until min(first signal + 250ms, root C)
       completed -> preserve result; retire the response control
       pending   -> stop unfinished execution/production slots
                    retain lifecycle owner, ledger and scope cleanup
                    uncommitted -> select 504 (local) / 503 (shutdown)
                         one finalization attempt, at most 100ms and root P
                         completed -> normal protocol completion
                         pending -> protocol stop
                    committed -> protocol stop, no replacement response
```

The finalization allowance is a single absolute deadline, normally anchored at
the exhausted cooperative cutoff. It is not another execution window or a
configurable response timeout. Framework admission rejections and failures with
no accepted execution anchor it when their fallback is selected. P always clips
it, including a root installed later. Repeated selection cannot renew it. No
budget means no promised 504/503 delivery. A fallback is framework-owned work;
it cannot restart middleware or lazy user production.

Final response selection rechecks the cutoff after owner handoff. A delayed
service poll can therefore replace a still-uncommitted response with a minimal
fallback. It keeps the already resolved CORS headers without rerunning user
origin predicates; incompatible application headers/body are discarded. Once
selected for Hyper, a response is conservatively committed even if no body byte
has reached the socket. A committed incomplete HTTP/1 response closes that
connection; HTTP/2 aborts its exact worker and requires actual join evidence.
A successfully written 504 does not itself close a healthy keep-alive connection.

The watchdog is polled inside the retained connection driver. It creates no new
task and holds only a resource-free request clock and protocol receipts. Sources
and scope disposal retain their existing owners and independent cleanup budgets.
Task abort is still only a request, not confirmation. Structured stop diagnostics
retain `RequestTimeout`, shutdown causes and `ResponseFinalizationTimeout`
separately. Frame counters do not prove final flush, task termination or delivery.
The root transport/resource barriers below are implemented by timeout Session 4.
The final cross-path aggregate reporting qualification remains Session 5.

## Shutdown response drain and dependency barriers

The managed listener no longer treats `wait_for_executions()` as permission to
abort all connections immediately. That receipt covers dispatch and producer
release. A buffered response may already have released its request scope while
its independent bytes still wait in Hyper or socket I/O. A newly selected
504/503 also needs its bounded finalization opportunity.

```text
close admission; stop listener acceptance
  -> drain actual connections until G or explicit force
  -> signal accepted execution (first reason/window remains fixed)
  -> observe execution/producer release, bounded by S
  -> continue connection drivers and per-response controls
       completed response -> actual connection/worker join
       pending committed response -> request-local stop at cooperative cutoff
       uncommitted fallback -> at most 100ms, clipped to P
  -> at P: abort only remaining connection tasks
  -> through R: confirm actual listener/connection/protocol joins
  -> confirm retained request owners, eligible middleware unwind, exact scopes
  -> owned dependencies D -> telemetry T -> final evidence H
```

These are dependency barriers, not a requirement to serialize all cleanup.
Normal around-middleware returns inside dispatch. Abnormal middleware cleanup
still runs serially in reverse order after its same-scope execution, source and
helpers terminate. Scope disposal can precede protocol completion once only
independent bytes remain. Application-owned dependency disposal requires both
branches: actual transport joins **and** every HTTP-owned request/scope receipt.
A completed error response cannot bypass a pending disposer. One completed
connection cannot authorize disposal while another connection remains live.

Response controls survive request-owner retirement inside their connection's
existing inventory. No duplicate root response registry or detached cleanup
task is necessary. Final abort requests are followed by real join observation;
blocked destructors remain outstanding and prevent parent disposal. An expired
R returns incomplete evidence while retaining receipts. A later actual join
does not rewrite the frozen attempt or secretly start a missed dependency close.

An unfinished `ManagedHttpServer` handle's Drop now publishes shutdown/force
and the root budget while leaving the listener task retained. Aborting that
listener directly would destroy its connection `TaskSet` before response drain.
Root-panic recovery likewise signals first, waits for the existing owners until
P, then requests remaining transport aborts and observes their joins through R.
The original root panic remains a failure even if recovery releases resources.
No new public callback or timeout setting is introduced.

## Implemented state machine and graceful flow

```text
Running
  -> AdmissionClosed
  -> Draining accepted requests and responses
       -> all children reconciled -----------------------------+
       -> graceful cutoff / explicit force                     |
            -> CancellingExecutions                            |
            -> bounded cooperative polling                     |
            -> StoppingExecutionSlots + termination proof      |
            -> ForcedLifecycleCleanup                          |
            -> request / transport / helper reconciliation ----+
  -> ClosingOwnedDependencies (only after their users are terminal)
  -> ClosingOwnedTelemetry (only after its users are terminal)
  -> final joins / frozen attempt report
       -> GracefulCompleted / ForcedCompleted / TerminalFailed / Incomplete
```

Normal graceful request order is:

```text
request admitted and registered
  -> optional scope creation, accepted execution
  -> middleware forward / guards / extractors / action / IntoResponse
  -> normal around-middleware return in reverse order
  -> response body production under backpressure
  -> source + input resources released / bridge detached / helpers joined
  -> release normally returned middleware state in reverse (no abnormal hook)
  -> request DI scope cleanup and exact receipt
  -> request owner join
```

The root closes the listener and application admission before draining. HTTP/1
keep-alive/pipelined requests and HTTP/2 streams that reach the service after
the gate closes must not execute application code or open request scopes.
Hyper's HTTP/1 graceful close and HTTP/2 GOAWAY remain protocol mechanisms.
GOAWAY alone is not the application gate: in-flight new streams may still
arrive during protocol convergence. Already registered executions are allowed
to finish within the shared graceful cutoff.

All responses, including SSE, remain subject to existing body limits,
backpressure, disconnects and the original admitted request deadline plus its
bounded cooperative window. SSE keep-alive does not reset that clock. Graceful
shutdown grants remaining drain time, not guaranteed delivery of all later
chunks or unlimited stream lifetime.

## Forced execution, cleanup and transport ordering

```text
close admission
  -> signal execution_cancel for accepted execution / active producers
  -> keep the same pending futures polled during the cooperative window
  -> pending at cutoff: destroy only execution / producer slots
  -> retain request owner, ledger, context and DI receipt
  -> confirm same-scope children terminal and bridge detached
  -> eligible on_request_termination callbacks, serial reverse order
  -> exact DI cleanup receipt
  -> request owner / transport / protocol / helper reconciliation
  -> owned DI, then telemetry, while prerequisites and time permit
  -> freeze the actual result
```

Transport failure or peer disconnect may occur before this sequence begins.
The execution/producer slots are polled inside the retained owner; the framework
does not abort that request owner to stop its slots. Transport or helper task
abort requests require their actual joins and cannot erase request ownership.
Conversely, the last transport bytes may
drain after source release if they no longer retain scoped user work. Stop
execution, release/detach scoped transport users and observe children before
running cleanup that changes those resources. Never wait for unrelated streams
just to close one request scope.

The request slot includes normal around-middleware processing, guards,
extractors, action and `IntoResponse`. Admitted dynamic CORS work also needs
execution ownership even though it currently precedes request DI. Constructors
are startup work, not request slots.

A lazy response source needs an independently stoppable producer slot after
handler completion. It need not be a separate Tokio task. Its shutdown control
cannot depend exclusively on Hyper polling for another data frame. Use bounded,
demand-driven handoff; register any required pump/helper before it starts and
retain its actual receipt. User-provided streams are owned while stored in that
slot; raw tasks started by their users remain application-owned.

A precreated but unpolled body is allowed to be dropped without polling it,
including HEAD/204/304 suppression or interruption during normal middleware
return. This is `NotStarted`, not a failed promise to invoke arbitrary user code.
A response whose head was handed off and body was cut is an interrupted
response with unknown delivery/commit evidence; do not replace it with a
second success/error response. HTTP/2 stream-local termination should be used
where supported by the adapter; a necessary connection-wide fallback must
report every affected sibling request.

## Around-middleware obligation contract

Keep the current around API for normal execution. Code before/after `next.run`
belongs to the same execution slot and uses the execution signal. Introduce a
separate `on_request_termination` for interrupted eligible invocations, implemented
in Phase 6; it cannot safely be replaced by rerunning normal response processing.

```text
registered future -> first poll / Entered
  -> Before -> DelegatingToNext -> After -> NormalReturned
                    (or early normal return / typed error)
  -> interruption or contained panic before NormalReturned
       -> retained termination obligation
       -> final termination disposition, at most once
```

This is an around callback, so there is no separate observable “before returned
successfully” boundary. Entry means first poll of that specific invocation,
recorded before user code can run. Merely constructing an async future does not
enter it. Guard/extractor/action callbacks have no invented matching exit hook.

Use invocation identity, not middleware `TypeId`; the same instance can appear
at different chain positions. The framework ledger must survive execution drop
and cannot live in user-clearable `Request.local`. Values needed after stack
destruction must be explicitly retained in owner-managed invocation state;
arbitrary async locals cannot be recovered.

If C returned normally, B is interrupted in its after code and A is awaiting
`next`, termination runs `B -> A`. C is not repeated. Normal returned typed
errors are response/execution outcomes, not evidence of cancellation. A panic
before normal return remains an eligible interrupted frame when containment
and resource prerequisites permit cleanup. A termination panic/error/timeout
settles that invocation once and does not skip the next eligible outer frame.

A completed around callback is not reopened merely because a later response
stream fails. Body-lifetime cleanup belongs to the body source or DI. The
framework does not infer which user after-code side effects occurred, nor fix
an application's own timeout/drop of `next` by inventing a successful exit.

Cleanup context must expose bounded interruption and invocation-stage evidence,
retained cleanup state, metadata and DI access. It must not offer another
`HttpNext`, permit a second response or consume the interrupted input body.
Partial normal processing is observable only at framework await boundaries;
cleanup must tolerate partial user side effects. Invocation/first poll and
completion are both best effort under the remaining budget.

## Authorities and absolute deadlines

Callbacks receive separate read-only `ExecutionCancellation` and
`CleanupCancellation` views. No raw token, cancellation source, `Deref`, child
authority or conversion between phases is public. Parameter and context views
must reference the same authority. Dropping/cloning a view does not cancel or
extend the operation. The host-supplied `start_with_cancellation` source remains
distinct from these callback views.

One shutdown attempt fixes `H = first_shutdown_start + total_budget`. Use
absolute cutoffs within H:

```text
G: graceful drain
C: cooperative execution cancellation
S: execution/producer stop confirmation
U: forced middleware and request-scope cleanup
P: final response/connection stop
R: request/transport/helper reconciliation
D: owned DI close
T: owned telemetry close
H: final receipts and report
```

The budget validates `G <= C <= S <= U <= P <= R <= D <= T <= H`, including zero
and overflow-safe budgets. Its internal cutoff policy is respectively
60%, 70%, 75%, 85%, 88%, 90%, 95%, 98% and 100% of the total. These are latest cutoffs,
not minimum time grants or a public percentage configuration. Earlier completion
advances work; explicit force may shorten it. Phase 4 uses C for cooperative
polling and S for execution confirmation. Timeout Session 4 keeps response drain
alive after that confirmation and uses P for the last connection abort fallback,
leaving P-to-R for actual join observation. Phase 3 uses
U/R for retained DI close and request-owner reconciliation. Forced middleware
cleanup consumes the same U in Phase 6.

The first `ShutdownState` initiation records the source timestamp, including OS
signals; delayed observation, repeated close and entry into a nested coordinator
cannot restart H. The HTTP coordinator receives existing D/T deadlines through
`FrameworkShutdownCoordinator::before`. HTTP components supply an absolute
`force_deadline` of R for transport/reconciliation, D for DI and T for telemetry;
the generic short force reserve cannot cut their cooperative/cleanup window
short. The coordinator still clamps to its hard deadline and component cap.
Listener graceful drain uses G, final transport stop P, join reconciliation R,
dependencies D, telemetry T
and final receipt observation H. Request/body/DI/helper owners retain the same
cutoffs. Build rollback before an App is
returned now has its own retained owner, anchored at the first failure or explicit
build cancellation. It uses the same D/T/H ratios. During DI construction the
bootstrap rollback timeout is captured once, with its final 5% reserved for outer
cleanup; the later telemetry phase cannot start another full timeout.

Every effective wait uses `min(local absolute cap, owner cutoff, root cutoff)`.
An ordinary request/body timeout keeps its existing deadline and never creates
a fresh total shutdown budget. A local interruption outside application shutdown
needs a bounded request-owner termination budget; if application shutdown then
begins, clamp that existing budget to H and its phase cutoffs, never extend it.

Each termination invocation gets a child authority capped by its own local
allowance, owner cleanup cutoff and root cleanup cutoff. Expiring one child
does not cancel a sibling. Exhausting the shared root can of course leave all
remaining callbacks `NotStarted`. Do not create or poll a new cleanup future
after its allowed start time; no blanket `poll_once` policy or detached cleanup.

## Reporting, failure and guarantee limits

The report must reconcile admitted request identities, not just transport
counters. Preserve independently:

- Execution first poll, first cancellation reason and actual return/drop.
- Task abort request and joined result, including completion winning an abort
  race. A dropped waiter, finished flag or task handle is not a join.
- Head handoff; producer completed/not-started/interrupted/failed/panicked;
  source release and bridge detachment. No invented client-delivery evidence.
- Entered middleware, normal return, interrupted stage, termination outcome and
  not-started obligations. Never relabel partial side effects as rolled back.
- Scope generation created, actual disposal termination and separate disposal
  success/failure/unknown; caller-owned scopes still count when HTTP created them.
- Request-owner, connection, protocol and helper joins; owned dependency and
  telemetry receipts; remaining work at the absolute cutoff.

`GracefulCompleted` requires clean required cleanup and confirmed terminal
owned work without forced escalation. `ForcedCompleted` permits forced
execution/body stops, but still requires successful required cleanup and real
termination proof. Neither promises successful HTTP delivery. `TerminalFailed` means
**termination proven but cleanup or the attempt failed**; `Incomplete` means
**termination not proven or accounting/admission prerequisites unresolved**. Normal historical HTTP errors do not poison an
unrelated later shutdown attempt.

Freeze the attempt report at its deadline/finish. A late join may update retained
diagnostics but cannot rewrite an incomplete attempt as a success or grant a
new budget on repeated close. Phase 8 publishes the root `io::Result` and
HTTP report together, after an actual root join or a bounded observer timeout.

Correct implementation still cannot preempt a non-yielding poll/destructor,
abort already started blocking work, guarantee async user cleanup completion,
roll back arbitrary side effects or prove remote receipt of bytes. Retain the
unconfirmed work, skip dependency disposal while its users remain, and report
`Incomplete`. Bounded waiting and “all work definitely gone at H” cannot both
be guaranteed for arbitrary in-process user code. Runtime/process termination
is outside the async cleanup guarantee.

## WebSocket reference boundary

**Reuse the principles:** owner/slot separation, cooperative cancellation,
read-only signal authorities, reverse entered-prefix cleanup, one root
deadline, exact DI receipts, joined-task evidence and truthful incomplete reports.

**Adapt to HTTP:** message ownership becomes request ownership; outbound
continuation becomes lazy body/transport ownership; connection handling follows
HTTP/1 keep-alive and HTTP/2 streams, with no connection DI scope invented.

**Do not import:** connected/disconnected eligibility, room/group cleanup,
WebSocket backplane, manager publication, a message/connection cleanup stack
without an HTTP equivalent, or `DispatchLease` solely because WebSocket uses it.
