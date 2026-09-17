# HTTP shutdown implementation roadmap

**Phases 1–9 are complete.** Each phase was implemented and reported separately. The
[architecture contract](SHUTDOWN_ARCHITECTURE.md) defines ownership and the
[qualification matrix](SHUTDOWN_QUALIFICATION.md) assigns evidence to each phase.

“Implementation risk” means a mistake the implementation/tests must prevent.
“Remaining guarantee limit” means a boundary that persists even in a correct
implementation. These are deliberately separate.

## Phase 1 — HTTP lifecycle and evidence contracts

- **Aim:** Establish HTTP admission, execution, body, scope and transport
  boundaries before changing their scheduling.
- **Components:** Architecture, migration and qualification documents; private
  `lifecycle` evidence records and request barrier predicates.
- **New invariant:** Cancellation/abort requests are separate from terminal
  evidence. Handler return and producer EOF cannot substitute for source,
  bridge, scope or child termination. Failed/unknown cleanup is not success.
- **Public API impact:** None in this phase. The migration contract defines the
  planned read-only token roles, termination hook and longer request scope.
- **Implementation risks:** Conflating facts from different owners; treating
  missing receipts as absent resources; presenting contract-only tests as
  proof of runtime fixes.
- **Remaining guarantee limits:** Records validate supplied observations; only
  actual runtime owners can supply real drop/join/DI evidence. Side effects
  and client delivery cannot be inferred from a first poll or handler return.
- **Qualification:** The executable Phase 1 cases in
  [src/lifecycle/tests.rs](src/lifecycle/tests.rs), plus crate compilation,
  formatting and documentation checks. Runtime scenarios remain pending.

## Phase 2 — Root ownership, task receipts and deadline foundation (implemented)

- **Aim:** Retain the canonical lifecycle independently of start/close waiters;
  establish one absolute shutdown attempt and real task inventories.
- **Components:** `AppLifecycleState`, `run_http_lifecycle`,
  `HttpLifecycleTrigger`, `ManagedHttpServer`, connection supervisors, tracked
  Hyper executor, private task/receipt and deadline modules.
- **New invariant:** Root, listener, connection and Hyper work register actual
  joins before a gate permits their first user/protocol poll. Native monitor
  installation retains its own handle until direct adoption by HTTP. A dropped
  waiter cannot erase receipts. HTTP/2 joins belong to connection/root inventories;
  abort request is never confirmed termination. Cutoffs derive from the first
  shutdown source timestamp; repeated observation cannot extend H.
- **Public API impact:** Added `App::close(&self).await` for built or running
  Apps; preserved start entry points and caller-owned container policy.
  Report aggregation and callback signatures remain later phases.
- **Implementation risks:** Publication races, root self-join, clone/drop
  triggering a second root, untracked Hyper tasks, receiver loss discarding a
  join, allocating a new deadline on repeated observation.
- **Remaining guarantee limits:** A non-yielding task, destructor or started
  blocking operation may stay outstanding beyond H. Ownership is retained;
  termination is not assumed. Async start/close must first be polled to install
  the root. Connection force still aborts its execution stack; request owners,
  cancellation, scope/body cleanup and hidden helper/telemetry joins need later
  phases. The public result is not yet the full HTTP aggregate report.
- **Qualification:** Implemented task/close/deadline portions of Q01, Q10–Q11,
  Q24–Q25, Q28–Q29 and Q32, with exact tests in the qualification document.
  Actual concurrent Hyper HTTP/2 workers are exercised, not only synthetic
  counters. Other prerequisites in these scenarios remain planned.

## Phase 3 — Retained request owner and execution slot (implemented)

- **Aim:** Preserve request state, scope authority and future cleanup ledger
  when execution or the service waiter is interrupted.
- **Components:** `serve_request`/`serve_owned_request`, `dispatch_request`,
  `App::call_with_outcome`, `RequestRegistry`, `HttpRequestLifecycleOwner`,
  `RequestExecutionContext`, the replaceable dispatch slot, retained
  `ApplicationScope`/process context and creation-bound generation observation.
  Root transport/dependency barriers include these request and DI receipts.
- **New invariant:** Request registration precedes user execution; only the
  execution slot can be dropped. Its owner and DI observation remain reachable
  from the root even after connection failure. Request scope lifetime can no
  longer be equated to the `run_scoped` handler call.
- **Public API impact:** Internal owner changes; no artificial HTTP connection
  hooks. Scope lifetime becomes explicitly tied to the owned request resources;
  streaming guarantees require the body integration in Phase 5.
- **Implementation risks:** Borrowed request/response state escaping a dropped
  slot, losing process context, looking up a receipt after scope-ID reuse,
  registering only after the first user await, owner panic losing children.
- **Remaining guarantee limits:** Slot destruction cannot recover arbitrary
  user async locals or adopt raw application-spawned work. Phase 3 alone retained the immediate stop boundary; Phase 4 now adds
  signalling and cooperative polling before dispatch destruction. Scope
  close still precedes lazy response-source release (Phase 5), and the entered
  middleware ledger/termination callbacks remain Phase 6. A non-yielding
  destructor or unknown disposal result can leave a truthful incomplete attempt.
- **Qualification:** 18 HTTP ownership/transport tests plus the DI generation
  regression in the qualification document. Q03–Q04, Q21, Q25–Q26, Q32 and Q35
  now have request/DI ownership evidence; full body/cleanup/report semantics
  still depend on later phases. Real HTTP/1 disconnect, pending input, HTTP/2
  concurrent connection loss and forced transport/parent disposal are covered.

## Phase 4 — Admission and cooperative execution cancellation (implemented)

- **Aim:** Stop new application work while allowing accepted work to drain,
  then signal and poll pending execution during a bounded cooperative window.
- **Components:** `RequestRegistry` gate + accepted publication/capacity, root
  and transport escalation, `RequestExecutionContext::execute`, root-aware
  control timers, `lily_web_core::ExecutionCancellation`, `Request`/`HttpExchange`
  and CORS contexts, middleware/guard parameters and the typed action extractor.
  Existing generic generated adapters support the new extractor and aliases.
  HTTP fixtures, CSRF adapters and the middleware example have migrated.
- **New invariant:** Gate closure and request registration are linearized;
  rejection never creates a scope or runs user work. Force/expiry signals
  execution first. Only after the cooperative cutoff may the slot stop; its
  owner remains. The same pinned future is polled for at most 250ms from the
  first stop request, clamped by C. Repeated stop cannot restart it. Dispatch
  return/panic/drop and the stop reason remain independent. A cooperative return
  keeps the actual response or application error; a local request timeout selects
  HTTP 504 only for incomplete execution. The deadline starts once at admission
  and covers request-body reads without independent per-frame timers.
- **Public API impact:** `HttpMiddleware::handle(exchange, next, cancellation)`
  and `GuardTrait::can_activate(request, cancellation)` require the new
  `ExecutionCancellation` argument. `Request`, `HttpExchange` and
  `CorsOriginContext` expose the same read-only view; controller actions may
  extract it by value. Host `CancellationToken` APIs remain distinct. Cleanup
  views and termination hooks are Phase 6, not an execution token conversion.
- **Implementation risks:** Check/register race, cancelling accepted requests
  at admission close, dropping a signalled future without polling it again,
  context/parameter authority mismatch, CORS bypassing ownership.
- **Remaining guarantee limits:** Cooperative code may ignore cancellation.
  The cap is not a guaranteed amount of CPU/poll time; a delayed scheduler or
  spent C can leave no window. Progress requires yielding polls; peer failures
  can defeat delivery even while execution returns. Lazy body ownership,
  termination hooks and complete parent cleanup are supplied by later phases.
- **Qualification:** 14 request/admission/protocol cases, a real generated
  callback integration covering eight interruption sites, shared view unit and
  compile-fail contracts, and the action-alias compile fixture. See the exact
  evidence in the qualification document. Q03–Q05, Q09–Q16, Q21–Q23 and Q36 gain
  admission/execution coverage; body/termination/report prerequisites remain.

## Phase 5 — Response, input and transport-resource ownership (implemented)

- **Aim:** Keep lazy body code and its resources under the request owner after
  action completion and preserve backpressure while stopping producers safely.
- **Components:** `BoundedResponseBody`, `ResponseBodyStream`, request body
  transfer/reader, SSE and static-file adapters, body bridge and helper tracking,
  `drive_connection` watchdog and HTTP/2 failure scope.
- **New invariant:** EOF, source release and bridge detachment are separate.
  Body cancellation/control is not dependent on another Hyper data poll.
  Producer/helper users stop before same-scope termination cleanup. A request
  scope can close without waiting for unrelated keep-alive/HTTP/2 activity.
- **Public API impact:** Accepted streaming work shares execution cancellation
  through the captured read-only view. Existing typed streaming/SSE shapes and
  byte limits remain; DI remains live throughout owned source lifetime. The
  non-exhaustive `ResponseBodyError` adds `StreamInterrupted`. Scope disposal
  errors after head handoff are retained/logged, never encoded as a second response.
- **Implementation risks:** Unbounded/eager body pumps, losing input ownership
  when moved into output, reporting handoff as delivery, dropping an unjoined
  file operation, disabling write watchdogs during graceful drain, cutting
  unrelated HTTP/2 streams without accounting for them.
- **Remaining guarantee limits:** Network delivery is not guaranteed. Started
  blocking work can outlive the wait budget. Managed requests now bind an exact
  protocol control (timeout Session 2): write stop is stream-local for HTTP/2,
  and final frame handoff is distinguished from DATA release/flush and actual
  joins. Raw codec services without that binding retain a connection-wide
  watchdog fallback. Retained transport joins remain subject to the root deadline. A blocked
  destructor or escaped framework input can prevent cleanup and yield Incomplete.
  Protocol frames copy bounded bytes so `Bytes::from_owner` cannot retain scoped
  destructors in Hyper; this adds allocation/copy cost. Generic application
  tasks remain untracked. Full termination/report/dependency work is Phases 6–8.
- **Qualification:** 18 new cases: 12 request/source/protocol ownership tests,
  one HTTP/2 graceful write-watchdog/sibling test and five shared resource/file
  tests. Q02, Q04–Q10, Q20–Q21, Q29–Q31, Q34 gain source destructor, bounded
  demand, SSE, HEAD/204/304, transferred input, late failure and real blocking
  join evidence. Existing static-file GET/HEAD/range/conditional, stream limit,
  byte framing, admission, DI and transport cases remain regression coverage.
  See the qualification document for exact test names and commands.

## Phase 6 — Around-middleware termination ledger and reverse unwind (implemented)

- **Aim:** Separate normal response post-processing from abnormal cleanup with
  retained per-invocation obligations.
- **Components:** `HttpMiddleware`, `HttpMiddlewareChain`, owner ledger and
  invocation storage, termination context, independent cleanup views/executor,
  built-in middleware and downstream middleware fixtures.
- **New invariant:** First-poll entry is recorded outside the execution stack.
  Only entered, not-normally-returned frames qualify; they terminate once in
  reverse order after all scoped execution/body prerequisites are terminal.
  Each invocation has an isolated child authority under the remaining budget.
- **Public API impact:** Add default no-op `on_request_termination` and its
  bounded context. Normal `handle` retains its around semantics and execution
  signal. Application cleanup that needs interruption notification opts in.
- **Implementation risks:** Using `TypeId` instead of invocation identity,
  losing ledger through `Request.local.clear`, duplicate cleanup after partial
  normal return, normal body failure reopening completed middleware, sibling
  token cancellation, simultaneous cleanup of one nested chain.
- **Remaining guarantee limits:** No automatic rollback; local async variables
  need explicit retained state/RAII. Hooks may be unstarted or interrupted when
  their budget ends. A default no-op cannot repair previous user code.
- **Qualification:** Q14–Q19, Q22, Q30, Q33, Q36: entered-prefix, partial-after,
  same-instance different positions, error/panic, per-hook cap isolation,
  zero-budget no poll and repeat-finalization protection.
- **Implemented:** Request-owned per-invocation storage and ledger are separate
  from execution and `Request.local`. Application/controller/action chains bind
  the same owner. First poll, normal return and next/after boundaries determine
  eligibility. The retained serial finalizer follows execution/body/input/helper
  receipts and precedes DI; cleanup helper joins and state destruction precede
  the next outer frame. Same-instance invocations do not collapse.
- **Authority/budget:** `CleanupCancellation` is read-only and independent of
  execution and siblings. The internal per-hook cap is 250ms with up to a 25ms
  cancellation notice tail inside it, always clipped by the existing owner
  deadline and U. Exhausted budget leaves callbacks `NotStarted`, never polled.
- **Evidence:** Per-invocation outcomes survive finalizer waiter loss and
  request retirement; a settled hook is not retried. Failures prevent clean
  shutdown, while missing release/helper joins also block parent cleanup.
  Eighteen HTTP cases and one shared authority case qualify the phase, plus
  compile-fail API examples and downstream facade compilation. See the exact
  test names and validation in the qualification document.

## Phase 7 — DI, transport and dependency reconciliation (implemented)

- **Aim:** Enforce child-before-parent barriers across every exit path, including
  failed start, force, caller-owned containers and telemetry shutdown.
- **Components:** Request/connection finalizers, exact DI observations and
  disposal results, HTTP shutdown adapters, root DI close, tracing receipt and
  blocking-worker adapters, signal/monitor cleanup.
- **New invariant:** Waiter timeout never consumes a receipt or implies
  disposal. Connection/helper joins and HTTP-owned scope generations reconcile
  before their dependencies close. Force cannot skip an owned DI obligation.
  Remaining waits use the existing root cutoffs; no unbounded final join.
- **Public API impact:** Preserve externally owned DI/tracing responsibility.
  Shared internal receipt seams may expand; hidden ABI changes need both HTTP
  and WebSocket regression checks.
- **Implementation risks:** Closing shared/caller DI, substituting another scope
  generation, disposing dependencies while users remain, relative timeout
  multiplication, detaching tracing/file/provider workers at waiter expiry.
- **Remaining guarantee limits:** A terminal disposer can fail. An outstanding
  child blocks dependency cleanup, leaving an explicitly incomplete attempt;
  telemetry cannot promise successful external export.
- **Qualification:** Q19–Q21, Q25–Q29, Q31–Q32, Q35: pending disposers/helpers,
  root expiry, joined failure, caller container reuse and tracing late results.
- **Implemented result:** A retained `HttpDependencies` owns the original typed
  DI result, tracing adapter/receipt and failure retention. Both normal and force
  handles use it. A preceding reconciliation component joins transport/request
  resources and monitors before DI; telemetry additionally requires DI
  quiescence. Failed bind/panic recovery use the same obligations. Late child
  observations cannot restart cleanup deadlines or overwrite the root result.
  Build cancellation now retains its in-progress DI transaction; post-build and
  pre-DI rollback share the first failure/cancellation deadline with telemetry.
  Shared tracing retains owner/log/span/provider receipts; bounded file queues
  and metric collection use owned OS threads with actual joins. No receipt
  driver or provider/file join handle is detached on timeout.
  The SDK custom-reader feature is enabled for the internal metrics adapter;
  package versions, lockfile and public callback signatures are unchanged.


## Phase 8 — HTTP report, health and immutable attempt evidence (implemented)

- **Aim:** Aggregate request/response/scope/task/dependency facts without
  confusing HTTP status, cleanup success and actual termination.
- **Components:** HTTP-specific internal report, root reconciliation, health
  adapter, bounded structured diagnostics and replayable attempt snapshot.
- **New invariant:** Every registered identity has a terminal or outstanding
  disposition exactly once. Abort requests, joins and source releases remain
  distinct; late success cannot overwrite a failed/frozen attempt.
- **Public API impact:** Prefer internal report + existing health/diagnostic
  integration initially; add public report exposure only if its contract is
  needed and stable. No raw token or dependency handles in reports.
- **Implementation risks:** Double-counting retired records, false header-sent
  evidence, erasing cleanup failures on replay, including unrelated caller scopes
  or historical ordinary request failures in the shutdown result.
- **Remaining guarantee limits:** Report only observed boundaries; partial user
  side effects, remote delivery and externally owned work remain unknown.
- **Qualification:** Q05–Q06, Q24–Q29, Q32–Q33: aggregate reconciliation,
  terminal-but-failed vs outstanding, frozen reports, root join and safe labels.

Phase 8 disposition:

- Fixed-size request/response/middleware/scope/helper/owner accounting now
  preserves lifetime observations separately from the admission-close cohort.
  Each identity retires once; historical terminal request failures do not
  contaminate a later HTTP attempt. Outstanding cleanup remains in scope.
- HTTP report and root result freeze atomically after actual join/observer
  cutoff. Late reconciliation cannot overwrite either. Terminal failure is
  distinct from outstanding work; source handoff is not header delivery.
- DI receipts, quiescence, tracing adapter/worker joins, exporter loss and
  generic coordinator outcomes have separate evidence. The shared tracing
  adapter exposes a hidden read-only actual join disposition.
- The existing health service publishes bounded shutdown/dependency reasons.
  Its weak observer retains the actual root receipt after caller loss without
  retaining the completed App graph or spawning a receipt driver. Owned
  telemetry receives a preliminary checkpoint; final evidence remains in
  memory/health after telemetry closes.
- No callback signatures, package versions or lockfiles change. Thirteen new
  HTTP cases and strengthened body/middleware/DI/tracing tests qualify these
  boundaries; Phase 9 below completes combined production qualification.

## Phase 9 — Production qualification and completed migration (implemented)

- **Aim:** Qualify all supported protocol/lifecycle paths and document only
  runtime guarantees demonstrated by their real owners.
- **Components:** HTTP/1.1 and HTTP/2 transport suites, lifecycle/body/DI tests,
  paused-time tests, compile/downstream fixtures, documentation and shared-crate
  regressions. Include SSE and static files because they are supported today.
- **New invariant:** Every promised guarantee maps to executable evidence;
  unqualified cases and actual remaining work cannot be labelled completed.
- **Public API impact:** Finalize the migration guide against the implemented
  facade and callback signatures; no speculative compatibility promises.
- **Implementation risks:** Testing only counters/mocks, treating an abort
  request as success, timing sleeps instead of deterministic barriers, excluding
  real HTTP/2 child tasks or blocking file operations from leak checks.
- **Remaining guarantee limits:** Qualification does not prove preemption,
  rollback, arbitrary user cleanup completion or successful client delivery.
- **Qualification:** All Q01–Q36 through the actual managed runtime, plus
  affected `lily_web_core`, `lily_middleware`, HTTP macros, DI, shutdown, tracing
  and WebSocket/downstream regression suites.

Implemented in Phase 9:

- Thirteen managed-runtime tests connect real HTTP/1 and HTTP/2 requests to
  generated callbacks, reverse cleanup, body/input/static-file release, exact
  scope termination, application DI and immutable joined reports. Looped cases
  cover additional protocol, callback-stage and cleanup-failure combinations.
- The isolated owned-telemetry test now drains a real active request and proves
  request disposal -> application disposal -> exported preliminary checkpoint
  before observing final worker shutdown/health.
- Qualification found and fixed completed-server Drop changing graceful into
  forced, and the generic short force cap expiring HTTP reconciliation before
  its cooperative/cleanup windows. HTTP supplies existing absolute force phase
  cutoffs; two paused-time shared-coordinator tests retain root clamping,
  expired-cutoff behavior, replay and the default policy for other consumers.
- Graceful cutoff escalation preserves its actual request stop reason. A
  pending request disposer can be terminal-but-failed after its DI owner
  confirms destruction; it cannot be reported as successful disposal.
- Migration/docs and downstream compile fixtures use the implemented public
  facade, read-only views and managed `App::close` ownership. The final
  qualification matrix maps Q01–Q36 to real protocol and controlled fault
  evidence, explicitly retaining non-preemption and delivery limits.

## Audit finding dependencies

These IDs preserve the original audit's issue boundaries. The table retains
the original problem description; the phase dispositions below distinguish
the historical foundation from the completed phase dispositions below.

| Finding | Severity | Original audited problem | Owning phases |
| --- | --- | --- | --- |
| HTTP-SHUTDOWN-F001 | Major | Capacity admission is not atomic shutdown admission; accept/HTTP/2 arrival races. | 2, 4 |
| HTTP-SHUTDOWN-F002 | Blocker | Dispatch timeout/connection abort cuts execution without cooperative signalling or retained lifecycle separation. | 3, 4, 5 |
| HTTP-SHUTDOWN-F003 | Major | Around cleanup obligations disappear with the future stack. | 3, 6 |
| HTTP-SHUTDOWN-F004 | Blocker | Request DI closes before lazy response body execution. | 3, 5, 7 |
| HTTP-SHUTDOWN-F005 | Blocker | Hyper HTTP/2 task joins are outside the connection/root inventory. | 2, 7 |
| HTTP-SHUTDOWN-F006 | Major | HTTP lacks retained exact scope receipts, particularly with caller-owned DI. | 3, 7 |
| HTTP-SHUTDOWN-F007 | Major | Nested relative budgets and unbounded final joins; separate full rollback timeouts. | 2, 4, 7 |
| HTTP-SHUTDOWN-F008 | Blocker | Graceful-only DI action may be skipped on force; dependency users lack a full barrier. | 7 |
| HTTP-SHUTDOWN-F009 | Major | Response/guard counters do not prove source, scope or transport completion and can misclassify drops. | 5, 8 |
| HTTP-SHUTDOWN-F010 | Major | Graceful branch stops polling the write watchdog; connection-wide HTTP/2 timeout needs explicit impact accounting. | 5 |
| HTTP-SHUTDOWN-F011 | Major | Static-file blocking/I/O helper termination is not owned by the HTTP request/root. | 5, 7 |
| HTTP-SHUTDOWN-F012 | Major relative to target | Caller waiter drop can lose the root cleanup path; close-without-start is currently documented as unsupported. | 2, 7 |
| HTTP-SHUTDOWN-F013 | Major | Tracing receipt drivers/provider/file work may survive timeout without retained actual join proof. Existing timeout reporting does not prove worker termination. | 7, 8 |

Historical Phase 2 dispositions (see Phase 7 updates below):

- **F005:** Managed-runtime Hyper execution now retains actual connection/root
  joins and panic evidence. Full dependent scope/body barriers still require
  Phases 3, 5 and 7.
- **F012:** Supported built/running `close`, retained root, post-first-poll
  waiter-drop behavior and failed-bind result replay are implemented. Full
  request/dependency/telemetry cleanup proof remains Phase 7.
- **F001:** TCP accept checks stop again before connection registration.
  Atomic request admission on existing HTTP/1 and HTTP/2 connections is Phase 4.
- **F007:** One source-anchored H now bounds the root, listener/connection joins,
  post-build failure cleanup and dependency/telemetry adapter waits. Local
  request/body deadlines, pre-build rollback and hidden worker receipt paths
  still need Phases 4, 5 and 7.
- **F008/F013:** Transport task barriers now protect owned dependencies; tracing
  also checks owned DI quiescence. Force cannot yet guarantee the graceful-only
  DI action is attempted, and actual tracing helper joins remain Phase 7 work.
- **Other findings:** No request/body/middleware fix is claimed by Phase 2.

Historical Phase 3 dispositions:

- **F002:** Execution and lifecycle storage are separated. Timeout and service
  waiter/transport loss stop the dispatch slot while request state, scope and
  receipts remain root-owned. Cooperative notification/polling is still Phase 4.
- **F006:** HTTP captures the generation directly from the created scope and
  retains both close-result and real termination receipts, including caller DI.
  Missing receipts and another owner's unavailable disposal result do not become
  success. Parent cleanup waits for request-owner joins and exact scope receipts.
- **F007:** Interrupted execution gets a bounded local cleanup allowance instead
  of passing its expired execution deadline to DI. U/R always clamp this work;
  the original application deadline does not move.
- **F008:** Transport force cannot discard the separate request owner, and
  dependencies now also require its join/scope barriers. The graceful-only root
  DI action can still be skipped on a forced/exhausted generic phase (Phase 7).
- **F003/F004/F009–F011/F013:** Entered middleware cleanup, lazy body lifetime,
  body/watchdog evidence and remaining helper/telemetry reconciliation are not
  claimed fixed by this phase.

Historical Phase 4 dispositions:

- **F001:** Accepted admission and gate closure share one lock. Registered
  candidates can still be denied before scope/user work. Existing HTTP/1
  keep-alive and HTTP/2 streams use the same gate as new connections.
- **F002:** Force, G expiry, request timeout and waiter loss notify execution;
  the same slot continues polling until return or the bounded cooperative cap.
  Connection escalation waits for actual execution termination or S; it cannot
  destroy the separate request owner. Phase 5 supplies lazy producer ownership.
- **F007:** One first-signal timestamp fixes the local cooperative window;
  root C can only shorten it. The original request timeout and U/R cleanup
  cutoffs remain separate and no new total shutdown attempt is created.
- **F003/F004/F008–F011/F013:** No middleware termination, body/helper ownership,
  force-safe root DI or complete telemetry/report fix is claimed by this phase.

Historical Phase 5 dispositions:

- **F002/F004:** Streaming execution is an independently controlled slot in the
  retained request task. Head handoff no longer waits for owner termination.
  Sources, transferred input and actual file-helper joins precede DI close.
  Source/byte-owner destructor failure blocks cleanup rather than manufacturing
  a successful release. Graceful and forced producers share the existing root
  cutoffs and the first-signal cooperative window.
- **F009:** Runtime body disposition, source release and bridge detachment are
  separate from handler return and transport counters. Protocol bytes cannot
  retain scoped user destructors. Full aggregate/reconciliation reporting is
  still Phase 8; no client-delivery observation is invented.
- **F010:** Write watchdog remains polled during graceful/idle drain. HTTP/2
  body failure is stream-local; connection-wide timeout fallback separately
  accounts for the timed-out stream and cancelled siblings. Final independent
  bytes/socket flush remain subject to transport joins and the root deadline.
- **F011:** Managed static-file helpers publish actual joins before execution;
  abandoning their caller cannot erase joins or late-result destruction.
  Started blocking work remains outstanding until actually joined. Full root
  reconciliation and other dependency/telemetry helpers remain Phase 7.
- **F003/F008/F013:** Middleware termination, force-safe owned DI shutdown and
  complete tracing ownership were unchanged by Phase 5. Phase 6 below implements
  the entered middleware ledger and abnormal reverse unwind.

Historical Phase 6 dispositions:

- **F003:** Entered obligations survive execution cancellation/drop. Normal
  returned errors and body failures do not reopen normal frames. Eligible
  abnormal callbacks run serially in reverse order under independent authority.
- **F002/F006/F011:** Request owner retirement and DI disposal include retained
  middleware state, callback destruction and actual cleanup-helper joins.
  Missing child termination prevents outer cleanup; failed but terminal hooks
  are distinct from unconfirmed resource release.
- **F007/F009:** Hook cutoffs share owner/root deadlines, including later root
  installation. Per-hook timeout cannot cancel siblings. Outcome/first-poll
  evidence and registry totals distinguish completion, failure, panic, timeout,
  unstarted and outstanding work. Full immutable HTTP reporting remains Phase 8.
- **F008/F013:** Force-safe owned DI close, complete telemetry ownership and
  late root reconciliation were deferred to Phase 7 at that checkpoint.

Phase 7 dispositions:

- **F004–F006/F011:** Root disposal now requires transport/monitor joins and exact
  HTTP request-scope retirement. Late input/helper release resumes the retained
  finalizer; failed cleanup remains failed. Caller DI and unrelated generations
  remain outside the owned-container obligation.
- **F007/F012:** Shared D/T/H now covers normal/forced/failed-start cleanup and
  build rollback. Dropped build/start/close waiters cannot discard the respective
  receipt; never-started Apps still close without binding. Expired replay cannot
  start another close attempt.
- **F008:** Both normal and force components invoke the same owned DI operation.
  A missing prerequisite or spent budget is explicitly incomplete, with ownership
  retained rather than silently skipping or racing disposal.
- **F013:** Real tracing owner/exporter/provider/file/metrics worker receipts are
  retained. Report return/timeout and worker join are independent; SDK metric
  acknowledgements alone cannot yield successful shutdown. Full HTTP aggregation
  of these facts is Phase 8.

Historical checkpoint: Phase 7 completed and was reported before Phase 8.

Historical Phase 8 finding dispositions:

- **F009:** Body/source/bridge and handler outcomes now reconcile separately
  from exact scopes and real task joins. HTTP status and body handoff are not
  interpreted as resource termination or client delivery.
- **F013:** Phase 7 retained actual telemetry workers; Phase 8 aggregates those
  joins separately from exporter reports and freezes the attempt/health result.
  Late joins remain observable without rewriting a failed attempt.
- **Phase 9 disposition:** Combined runtime qualification and final migration
  review are complete. No arbitrary blocking-code preemption or user-task
  ownership is added. Verification details and boundaries are in the final
  [qualification matrix](SHUTDOWN_QUALIFICATION.md).

## Follow-up HTTP timeout sessions

The shutdown phases above remain distinct from the subsequent end-to-end
request-timeout work:

1. **Completed — admitted execution deadline and cooperative result.** One
   execution deadline is published at admission. A pipeline returning in the
   cooperative window keeps its actual response/error; incomplete local timeout
   selects 504. Input reads no longer introduce independent per-frame timers.
2. **Completed — response commit and protocol control.** The retained request
   owner binds its exact Hyper worker/connection receipt. Encoder commit is
   distinct from service handoff and remote delivery. HTTP/2 stop targets that
   worker, including pending final DATA/flush; HTTP/1 stop releases its actual
   connection. Abort requests require real join observation. See the
   [transport contract](SHUTDOWN_ARCHITECTURE.md#request-linked-response-transport-control)
   and [eight regression tests](SHUTDOWN_QUALIFICATION.md#http-timeout-session-2--commit-and-protocol-stop-control).
3. **Completed — admitted deadline through response writing.** Body/transport
   share the admitted clock and the remaining first-signal cooperative window.
   `request_timeout` covers the entire response; there is no separate response
   write setting. Uncommitted fallback selection/finalization has one internal
   100ms cap clipped to root transport stop. Committed incomplete responses stop
   through their actual protocol control; completed cooperative results remain intact. See
   [deadline policy](SHUTDOWN_ARCHITECTURE.md#one-admitted-deadline-through-response-writing)
   and [Session 3 qualification](SHUTDOWN_QUALIFICATION.md#http-timeout-session-3--one-request-and-response-clock).
4. **Completed — shutdown and resource prerequisites.** Execution/source
   termination no longer triggers premature connection abort. Existing response
   controls retain their cooperative/fallback opportunity, then P (88%) bounds
   final transport stop and R (90%) bounds actual join observation within the
   same H. Managed-server Drop and root-panic recovery signal/drain before this
   fallback. Actual transport joins and exact request/scope receipts jointly
   guard dependency disposal. Qualification includes pending real I/O, three
   connections, blocked disposal and a blocked protocol-task destructor; see
   [the barriers](SHUTDOWN_ARCHITECTURE.md#shutdown-response-drain-and-dependency-barriers)
   and [Session 4 tests](SHUTDOWN_QUALIFICATION.md#http-timeout-session-4--shutdown-response-and-resource-barriers).
5. **Pending — final reporting and qualification.** Reconcile HTTP execution,
   response, scope and actual task evidence across timeout/shutdown races and
   publish the final end-to-end contract.

Session 3 keeps callback signatures unchanged. Its public configuration surface
uses `HttpTransportConfig::request_timeout` for the whole admitted response;
the typed middleware interruption cause for an exhausted fallback write is
`HttpRequestInterruption::ResponseFinalizationTimeout`. The internal request
clock owns no user resource and adds no background task.
