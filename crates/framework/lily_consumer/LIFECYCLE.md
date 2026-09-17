# Consumer lifecycle contract and implementation stages

`ConsumerRuntimeOwner` owns the application shutdown attempt. `lily_queue`
owns broker subscriptions, delivery tasks and settlement. A broker delivery
waiting in the bounded buffer is distinct from an execution which the
dispatcher has started: graceful shutdown releases buffered work for broker
redelivery and drains executions already started.

## Authorities

| Authority | Owner | Meaning |
| --- | --- | --- |
| Admission | Queue runtime | Stop registration, broker intake and new delivery execution. Closing admission does not cancel accepted execution. |
| Execution | Queue runtime, with an isolated source per delivery | Notify accepted user work of timeout, forced shutdown or runtime failure. Local cancellation never cancels sibling deliveries. |
| Cleanup | Framework lifecycle owner | Bound lifecycle unwind and scope termination independently of execution cancellation. Invocation signals must be sibling children; cancelling one must not cancel the root or later hooks. |
| Settlement | Queue runtime | Authorize ACK, NACK and retry/DLQ handoff. Execution cancellation does not itself revoke settlement authority. Actual forced drain may subsequently stop settlement. |

Callbacks and extractors receive the read-only `DeliveryCancellation` view.
`cancelled()` and `is_cancelled()` observe notification; `reason()` exposes the
first recorded `DeliveryCancellationReason`. Reasons distinguish delivery
timeout, graceful deadline exhaustion, explicit force, runtime failure, and
underlying runtime/transaction cancellation. A later shutdown cannot replace
an already recorded delivery timeout. Reasons do not prove termination or
select a broker disposition by themselves.

Only the framework holds cancellation sources. Application cancellation is
still expressed through the handler's typed result. Raw `tokio::spawn` tasks
and resources created directly by application components remain
application-owned. Lily owns resources adopted by its DI scopes and framework
task owners. User callback completion cannot be guaranteed.

## One application deadline

The root deadline is anchored to the first shutdown initiation timestamp,
including any delay before the supervisor observes it. Startup rollback has
its own single attempt, anchored before waiting for pending startup activity.
Passing the same attempt to another owner never starts a new timeout.

`QueueShutdownDeadlines` is a hidden composition adapter contract, not an
application configuration surface. It carries absolute graceful and hard
cutoffs. A shared queue budget may be shortened but never renewed. An expired
root remains expired. The coordinator reserves `min(total / 4, 2 seconds)` inside that root for
forced completion. Cooperation, termination hooks, scope cleanup and final joins
consume remaining time from this same attempt; they do not allocate a new root.

Cancellation and abort requests are observations of intent. Terminal evidence
requires actual execution termination, scope cleanup receipts, broker
settlement evidence, and owned-task joins as appropriate. An unpolled forced
cleanup future is not successful cleanup, even if execution was synchronously
notified before that future was constructed.

## Implementation sequence

| Stage | Scope | Status |
| --- | --- | --- |
| 1 | Independent admission/execution/cleanup/settlement authorities, typed cancellation reasons, absolute root budget contract | Implemented |
| 2 | Delivery lifecycle owner, execution slot, retained middleware ledger and DI receipt | Implemented |
| 3 | Common pipeline deadline, bounded cooperative execution, normal/termination unwind, preservation of completed pipeline results | Implemented |
| 4 | Subscription/channel drain and settlement barriers | Implemented |
| 5 | Transaction/outbox, DI, telemetry and fallback task reconciliation | Implemented |
| 6 | Reporting, public observation and full qualification | Implemented; selected live Consumer profiles passed (see QUALIFICATION.md) |

Stages 1–5 establish independent authorities, retained delivery ownership,
bounded cooperative execution/unwind and subscription/channel settlement barriers.
Transaction/outbox, dependency and fallback owners also retain their exact joins.
These boundaries do not establish end-to-end broker ACK or transaction commit
guarantees during force.

Stage 6 exposes the original application evidence through the managed handle.
Deterministic ownership/qualification tests and strengthened live broker/database
fixtures are tracked in [QUALIFICATION.md](QUALIFICATION.md). Compiling an ignored
fixture is not a successful live qualification run.

## Delivery ownership

Each admitted delivery attempt has a `DeliveryLifecycleOwner` outside its
replaceable execution future. The owner retains the invocation (including
delivery-local values), immutable middleware plan, successfully entered prefix,
normal-exit ledger and exact DI scope-generation receipt. Guards, extractors,
handler resolution, handler work and normal reverse middleware exit borrow
that state through `DeliveryExecutionSlot`.

```text
Delivery/transaction task
  DeliveryLifecycleOwner
    invocation + entered middleware/exit ledger
    exact scope handle + original close result + terminal receipt
    borrowed ExecutionSlot: before -> guards/extraction/handler -> after reverse
      |
      +-- returns/drops -> execution release evidence
                             |
                             v
                        reverse termination (if eligible) -> exact DI cleanup + join
                             |
                             v
                        delivery result / settlement
```

Normal execution does not create an additional Tokio task. If an enclosing
delivery or transaction future is dropped, its borrowed execution is dropped
first. The lifecycle owner then transfers the **same** resources to a cleanup
task whose actual join is retained by the registered handler's
`DeliveryScopeTracker`. Aborting execution cannot erase the successfully
entered prefix or the original scope-close result.

Each entered middleware has a distinct normal-exit state: not started,
running, completed, failed, interrupted or panicked. Successful enter is recorded before
another hook can be polled. Dropping execution during normal exit marks that
exit interrupted; completed and interrupted normal exits are not replayed.
Only unfinished normal exits are eligible for `on_delivery_termination`. A
normal after panic stops the normal reverse loop: termination of that inner
entry must precede any outer exit.

Scope receipts are captured from the scope handle, never re-looked-up by a
reusable process ID. Cancelling a close waiter retains the original result
future and its exact terminal observation. A terminal scope with a missing
original result is not reported as successful cleanup. Disposer failure can
be terminal and joined; an abandoned observation or failed task join cannot
prove reconciliation and therefore blocks dependent disposal.

Transferred cleanup respects the existing delivery cutoff and, when execution
was cancelled, the existing 100 ms abandonment cap. Both are capped by the
root deadline, including a shorter root published after cleanup has started.
Cancelling a drain waiter leaves the real cleanup join in the tracker.
Completed task receipts are removed on subsequent admission, reconciliation
or drain; the tracker does not accumulate a per-delivery history. Middleware
state is bounded by the effective plan's existing 64-entry limit.

This owner boundary also surrounds each transactional delivery attempt. Stage 5
retains the enclosing database owner and heartbeat joins through finalization.
Synchronous user work and
destructors which do not yield cannot be preempted by a Tokio deadline.

## Stage 1 qualification

- The production dispatcher can finish an already started handler and mock
  ACK after admission closes; the next buffered delivery is released without
  executing its handler.
- Local timeout is isolated from sibling deliveries, cleanup and settlement.
- Local timeout and runtime cancellation retain one stable first reason,
  including concurrent requests.
- Force notification precedes dropping the execution task; the retained task
  set is joined before reporting completion.
- Expired, shortened or repeatedly published deadlines never grow.
- A delayed Consumer supervisor cannot restart an already expired budget.
- An exhausted coordinator can publish cancellation without polling forced
  cleanup, and reports that cleanup as incomplete.

These are deterministic tests using fake broker I/O. Stage 4 also supplies a
separate live RabbitMQ channel/settlement qualification fixture.

## Stage 2 qualification

- Stopping only execution preserves invocation/local state, entered prefix and
  the live DI scope until the owner explicitly closes it.
- Outer abort during `before_delivery` retains only successful enters; abort
  during reverse exit preserves completed inner exits and marks the pending
  one interrupted without replaying callbacks.
- Execution abort request is distinct from observed future release, including
  never-polled futures, poll panic and destructor panic.
- A cancelled scope-close waiter preserves the original disposer failure;
  disposal runs once. Reusing a process ID cannot redirect a receipt.
- A root published after cleanup has started shortens its existing wait,
  stops a pending disposer and observes actual termination.
- Scope observation completion does not replace cleanup-task join evidence.
  Cancelled drain waiters, task panic and runtime cancellation cannot produce
  false successful reconciliation.
- Completed cleanup receipts are reaped during ongoing admission, rather than
  accumulating until application shutdown.
- Existing generated handler, reverse middleware, scoped/transient DI,
  caller-owned container and scope-before-settlement qualifications still run.

## Common pipeline budget and cooperative stop

`delivery_execution_timeout_millis` remains the aggregate delivery execution /
cleanup budget. Let `H` be its original absolute cutoff, and `S` the instant the
compiled delivery future is constructed. Compute once:

```text
R = min((H - S) / 4, 1 second)
P = H - R  (one normal-pipeline cutoff, exposed as DeliveryDeadline)
```

Before middleware, guards, DI handler resolution, extractors, handler and all
normal after hooks share `P`. A transaction can shorten this budget but cannot
renew it. Normal exit has no additional per-hook timer.

At `P`, or the earlier root graceful cutoff, Lily records the cancellation cause
and its timestamp and signals execution. Explicit force also signals execution.
The pipeline remains owned and polled. Local timeout uses `H` as its owner
cutoff. Other cancellation uses the earlier of `H` and a forced owner cutoff,
anchored to the **first** cancellation time: at most one second and at most
three quarters of the root time remaining at that instant. The final quarter
remains for reconciliation/dependency work. All cutoffs are capped by the root.

The cooperative window is `min(250 ms, remaining owner budget / 4)`. Scheduling
delay consumes that window; late observation or repeated cancellation cannot
restart it. A shortened root wakes both execution and cleanup waiters.

```text
Running before -> guards -> extraction/handler -> after reverse
    |
local timeout / graceful cutoff / explicit force
    v
execution cancellation notification (first cause retained)
    v
same normal future remains polled within cooperative cutoff
    +-- full pipeline returns -> retain its actual success / typed failure
    |                            -> exact scope cleanup -> delivery result
    |
    +-- window expires -> drop only execution -> confirm release
                           -> eligible termination hooks reverse
                           -> exact scope cleanup + receipt
                           -> delivery interruption result
```

Returning from the handler is not full-pipeline completion. If normal after is
still pending when cooperation expires, the pipeline is interrupted. An
application error code that resembles a framework cancellation code remains an
application error. Private execution-stop evidence controls the framework
cancellation path; notification alone does not select it. Scope cleanup failure
remains a separate failure and may prevent a successful delivery from settling
as success. It is not rewritten as an execution timeout.

## Termination eligibility and cleanup authority

Each entered middleware has an independent termination state: not started,
running, completed, failed, panicked, timed out, interrupted, or budget exhausted
before invocation. Only a normal exit that never started, was interrupted or
panicked is eligible. Normal exits that returned `Ok` or `Err` are terminal.
An uncompleted before callback never arms an exit obligation; its future-owned
partial resources must be cancellation-safe.

`on_delivery_termination(&mut QueueDeliveryTerminationContext)` has a default
no-op implementation. The context exposes the reason, normal-exit evidence,
still-live scope services and delivery-local values. It has no mutable outcome
or settlement authority. `DeliveryCleanupCancellation` is a read-only,
invocation-specific sibling signal; its deadline and the context deadline are
the same authority. Execution cancellation cannot poison this signal.

Cleanup runs sequentially in reverse. The owner retains at most the smaller of
a quarter of its remaining time or 250 ms for DI cleanup. Eligible hooks share
the remaining chain deadline, with a fair share cap per invocation. A hook
cannot extend the chain or cancel an outer sibling. Expired hooks are dropped
before the next hook starts. Panics and returned failures are recorded and
outer cleanup continues when time remains. A cancelled cleanup waiter transfers
the same ledger, scope receipt and cutoffs; running termination is marked
interrupted and is never replayed. No new full timeout is started.

The dispatcher first notifies execution, then waits for retained delivery owners
within its forced cutoff before final abort. Final abort is followed by real
JoinSet joins; requesting abort alone is never terminal evidence. Synchronous
non-yielding code cannot be preempted, and a failed/unobserved join cannot be
reported as successful shutdown. Stage 4 adds the channel/settlement barrier
below; stage 5 adds database commit/rollback owner reconciliation.

## Stage 3 qualification

Transport-free tests exercise the real execution slot and lifecycle owner,
generated handler/extractor adapters, middleware registration, scope receipts
and dispatcher JoinSet paths. Deterministic paused-clock scenarios cover:

- local timeout, graceful cutoff and explicit force, including completed success
  and typed failure after cancellation;
- the same deadline through before, guard, extractor, handler and normal after;
- cancellation during before/handler/after, interrupted entered prefixes, and
  handler success followed by an unfinished normal after;
- execution drop before reverse termination, DI disposal after termination,
  partial normal exit, normal-after panic, and duplicate-invocation prevention;
- forever-pending/panicking termination hooks, sibling isolation, cleanup waiter
  abandonment, exhausted-root NotStarted evidence and original cleanup failures;
- first-cause timestamp retention, delayed polling, later root shortening,
  concurrent delivery isolation, bounded final abort and confirmed task joins.

Feature compilation is not evidence of live broker or database I/O behavior.

## Subscription generations and settlement barriers

The engine retains each physical subscription generation's dedicated channel
and Lapin consumer handle independently of the receiver future. Retaining the
consumer's drop guard prevents receiver abort from initiating an implicit,
unowned Basic.Cancel. A registration supervisor owns the receiver, dispatcher
and their real joins. The dispatcher owns and joins its delivery/settlement
tasks. Task reconciliation evidence is separate from the first operational
error: a later dispatcher panic cannot be hidden by an earlier receiver error.

Graceful drain:

```text
seal new registrations, stop intake and new execution
  |
  +-- bounded Basic.Cancel (stop broker subscription)
  |
  +-- release buffered, never-started work for broker redelivery
      finish accepted pipeline -> exact scope cleanup -> ACK/NACK/handoff
      join each delivery task -> join receiver and dispatcher
  |
both branches observed
  v
bounded dedicated-channel close -> prove Closed/Error
  v
provider scope/task reconciliation -> parent broker connection cleanup
```

Basic.Cancel does not release the channel while accepted work still needs it.
If cancellation is unconfirmed, a proven channel close can establish transport
termination after child drain. An unconfirmed close retains the resource and a
registration tombstone, reports failure, and prevents a replacement subscription.
Parent connection cleanup can later retire retained channels once task/scope
barriers permit it; a failed/unobserved task join withholds that parent disposal.

Forced drain first notifies execution and closes admission. On a healthy channel,
settlement remains authorized during bounded delivery completion: a pipeline
which returns in its cooperative window may still be ACKed or materialize its
typed failure. If the forced delivery cutoff expires, the dispatcher revokes
that generation's settlement authority, requests abort of remaining delivery
tasks, and consumes their actual joins. Only then can the channel close start.
Pipeline success is not proof of ACK, and an ACK interrupted after a confirmed
retry/DLQ publish retains both facts: confirmed handoff and unresolved original
delivery. It is never replayed as a second ACK/NACK for the same attempt.

Transport loss instead immediately stops execution and settlement for the lost
generation. Recovery waits for its task joins and channel termination before
opening a replacement. The replacement has a fresh buffer and fresh child
execution/settlement signals. Recoverable transport loss does not cancel sibling
subscriptions. A fatal task/settlement error closes application admission and
notifies sibling queues promptly, without waiting for the failing channel close.
Recovery reacquires the same registration gate as startup and rechecks shutdown
sealing before publishing readiness; shutdown cannot resurrect admission.

Dedicated consumer channel acquisition, subscription open/cancel/close and settlement waits
clamp their existing local caps to the shared absolute root deadline. A root
installed or shortened during I/O wakes the same owned future. Handoff followed
by ACK or fallback NACK cannot allocate another root budget. Expired operations
do not start broker I/O. Startup cancel/close share one local cleanup cutoff;
Basic.Cancel also leaves up to `min(root_remaining / 4, 250 ms)` for channel
close. The forced delivery cutoff retains its existing reconciliation reserve.

This follows RabbitMQ's distinction between [consumer cancellation](https://www.rabbitmq.com/docs/consumers)
and [delivery acknowledgements / publisher confirms](https://www.rabbitmq.com/docs/confirms).
Consumer ACK is not a broker-confirmed exactly-once receipt; connection loss can
still leave delivery outcome uncertain. Applications must tolerate redelivery.
No public callback signature or configuration field changes in this stage.

## Stage 4 qualification

- The real dispatcher preserves accepted execution/ACK while Basic.Cancel runs;
  buffered deliveries do not start, and the channel closes only after real joins.
- Force preserves a cooperative success through ACK; an uncooperative delivery
  is notified, bounded, settlement-revoked, aborted and joined before close.
- Cancelling a drain waiter retains the registration task and channel owner.
- A pending/unconfirmed Basic.Cancel cannot serialize accepted execution or
  extend the root; channel-close failure remains a failure despite joined tasks.
- An earlier receiver failure cannot hide a later dispatcher panic. Missing
  child proof prevents channel and dependency disposal.
- Recoverable generation cancellation is isolated; replacement requires old
  join proof and starts fresh cancellation authorities. Fatal failures notify
  sibling queues before channel cleanup completes.
- Expired roots start no ACK/NACK/handoff; late/shortened roots stop the same
  pending operation. Confirmed handoff plus timed-out ACK remains unresolved,
  and neither replay nor a fresh fallback timeout can extend the root.

`cap_q_06g_rabbitmq_lifecycle::basic_cancel_retains_the_active_delivery_channel_until_settlement_finishes`
checks the actual channel identity in RabbitMQ's management API while a completed
pipeline is paused before ACK. After release it requires successful settlement,
no redelivered original, and exact return to the connection/channel baseline.
Like the existing startup rollback and recovery fixtures, it is explicitly
ignored without a disposable broker configuration; compilation alone does not
qualify this live boundary.

## Transaction, outbox and dependency reconciliation

The root is published synchronously to every existing transactional runtime and
relay. Concurrent registrations inherit it before publication. The admission
seal and execution notification reach the relay/transaction owners before an
asynchronous engine drain can block. Repeated graceful/force observers cannot
renew the attempt. Backend-local shutdown durations only provide a standalone
fallback when no application root exists.

```text
admission sealed
    -> accepted delivery/transaction work drains
force notification (if needed)
    -> same delivery pipeline polls cooperatively
    -> actual completed result retained
    -> transaction commit/rollback owner retains its remaining bounded budget
    -> interrupted owner releases execution; delivery ledger/scope owner survives
engine + transaction/outbox owner joins (including MongoDB heartbeat joins)
    -> exact delivery scope receipt reconciliation
    -> broker connection close confirmation
    -> owned DI close receipt + no outstanding scopes/resolutions/cleanup users
    -> signal monitor stop + actual join
    -> owned telemetry shutdown receipt + exporter/file worker joins
    -> terminal result or incomplete evidence
```

PostgreSQL's queue adapter owns the actual driver task. The hidden database ABI
runs inside that task, without an additional detached finalizer. An abandoned
waiter requests rollback; the existing delivery deadline and later application
root cap the same finalization future. Dropping an unfinished driver operation
discards its pooled connection. It does **not** prove remote commit/rollback or
broker acknowledgement. The retained join and the active counter are separate
requirements.

MongoDB retains each transaction task and each lease heartbeat in the same
runtime task registry. Heartbeat abort is followed by join reconciliation even
if its parent is dropped. Execution notification is distinct from driver
cancellation: a force request first installs a fixed owner cutoff, preserving
the delivery's cooperative window. An actual waiter disappearance can cancel
that owner's driver operation. Known commit results and unknown commit outcomes
remain distinct; lease expiry and redelivery can still be needed.

A caught owner panic or finalization interruption remains a termination failure
even after its task has joined. A full handler pipeline result cannot substitute
for a database commit receipt. Confirmed RabbitMQ publish followed by an
interrupted durable delivered mark increments `uncertain_after_publish`; the
claim is retained for lease expiry, not rewritten as a failed publish.

Outbox and transaction waiters never remove a join before awaiting it. Terminal
flags and zero active counts alone cannot open the dependency disposal barrier.
After engine and database owners join, scope reconciliation also observes any
cleanup transferred by their final drop. Framework-owned DI requires both that
barrier and confirmed broker connection close. Caller-owned containers remain
caller-owned; the delivery scopes created through them are still reconciled.

Dependency and telemetry reserves are taken from the quarter of the force
reserve left by bounded delivery owners. They remain absolute cutoffs inside
the original root, including short roots. Telemetry cannot start while queue,
DI or signal-monitor users remain unconfirmed. A telemetry report alone is not
a worker join. Missing reports, failed joins and outstanding work cannot produce
successful shutdown.

Runtime supervisors, managed observers and fallback cleanup tasks retain real
shared join receipts independently of external waiters. Completed receipts are
reaped on task adoption/observation; there is no per-delivery fallback history
and no extra task merely to drive receipts. An abandoned final observer may
leave its completed receipt until the next application adoption/observation.
Incomplete dependency/startup owners stay retained instead of triggering a new
Drop-based cleanup timeout or disposing dependencies with live users. This
quarantine is intentionally distinct from successful cleanup and is bounded by
outstanding/incomplete application attempts, not by completed messages. A live
Tokio runtime is required; non-yielding code and blocking destructors remain
outside asynchronous timeout preemption guarantees.

Startup rollback adopts the DI rollback budget before polling initialization.
Initializer cancellation, pending activity observation, reverse disposal and
telemetry consume the same attempt. If a rollback observer expires, the actual
build owner remains retained. The cancelled initializer is dropped before its
partial service is disposed.

## Stage 5 qualification

- Cancelled drain observers retain the same relay/transaction joins; zero active
  transactions with a pending task tail remains unreconciled.
- Force notification permits real completion; repeated force and late root
  publication cannot renew the original cutoff.
- Actual abort joins, panic joins and MongoDB heartbeat joins remain observable
  after parent/observer cancellation.
- Completed transaction receipts are reaped during ongoing admission.
- A confirmed publish interrupted during marking is uncertain, not delivered or
  a new publish failure; the same claim is not retried by shutdown.
- Engine/relay reconciliation precedes transferred scope drain. Connection-close
  failure with a completed delivery drain still withholds DI disposal.
- A cancelled DI observer reuses one close receipt and disposes exactly once.
- Pending startup disposal consumes the root budget, including the cancelled
  initializer's cleanup; disposal termination is observed.
- Isolated telemetry qualification verifies the prerequisite barrier, actual
  tracing-owner join, worker joins and the original report.
- Dropping all external cleanup waiters leaves an observable actual join in the
  fallback registry; returned panic evidence is replayed from the same receipt.

## Final report and observation contract

The runtime owner captures one immutable, payload-free report after coordinator
and dependency reconciliation. It retains original action statuses; a completed
force operation cannot erase a graceful timeout/panic. Publication to users is
gated by the actual managed runtime task join. `ManagedConsumer::shutdown_report()`
and `snapshot().shutdown.report` expose the same evidence after either success
or failure. A snapshot samples terminal state once, so it cannot mix a running
runtime classification with a newly joined terminal report.

Queue drain and broker close are separate receipts. Owned DI, telemetry and
signal monitor reports distinguish ownership, cleanup start, actual termination
and success. Signal task failure remains an error, but a real join proves the
signal task no longer blocks telemetry. Abort without join cannot open that
barrier. A failed disposer can be terminal; pending disposal cannot be success.
`GracefulCompleted`, `ForcedCompleted`, `Failed` and `Incomplete` keep these
distinctions. Late resource termination never rewrites an original incomplete
action receipt. Deadline escalation is visible as `forced`, including when no
caller explicitly requested force.

Reporting never starts cleanup or renews a deadline. There is one bounded list
of at most five application actions. Delivery aggregates and transactional
ledgers are fixed counters. The separate queue settlement-detail history has
a 4096-entry cap and may retain event IDs, never message bodies. Detail recording
uses a nonblocking lock; an unavailable/full/poisoned history increases `dropped`
after the primary settlement counter was already committed. Correctness and
shutdown barriers use owner receipts, not this best-effort history. Outstanding
owners/receipts can retain resources after an incomplete attempt; no automatic
post-report resource disposal is claimed.

Returned framework cancellation and dropped execution share one settlement
classification: controlled force with no settlement started means pending broker
redelivery; absent force or already-started settlement remains unresolved. Neither
case claims an ACK, NACK, actual redelivery or completed remote transaction.
