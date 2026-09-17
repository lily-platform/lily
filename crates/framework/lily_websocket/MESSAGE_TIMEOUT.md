# Message execution deadline and cancellation contract

Each routed message receives one absolute deadline before its first user
execution callback. The effective timeout is selected in this order:

1. Message method `#[timeout(seconds = N)]`.
2. Controller `#[timeout(seconds = N)]`.
3. `ServerConfig::message_timeout_secs` (default: 30 seconds).

Configuration and attribute values accept 1..=300 seconds. The selected timeout
covers the entire normal pipeline:

```text
Select route and create message scope
  -> establish message_deadline = now + effective_timeout
  -> before_message (global -> controller -> action)
  -> guards
  -> extract arguments
  -> invoke action
  -> prepare response
  -> after_message (reverse entered prefix)
  -> normal execution terminal
```

No stage gets a fresh timeout. `WsMessageExchange::deadline()`, the invocation
deadline and the `MessageDeadline` extractor expose the same `Instant`. A later
shutdown may shorten execution authority; it never moves this local deadline
forward. Raw messages waiting in the inbound queue have not entered this
pipeline. Every selected message execution gets its own deadline.

Scope disposal, transport writes and termination cleanup are separate owned
operations. Finishing this pipeline alone proves neither disposal nor delivery.
The framework checks its timer at asynchronous scheduling boundaries; it cannot
preempt a blocking poll, synchronous codec or destructor.

## Independent lifecycle caps

| Setting | Default | Scope |
| --- | --- | --- |
| `message_timeout_secs` | 30 s | Whole normal message pipeline |
| `message_cleanup_timeout_secs` | 10 s | Aggregate message termination tail |
| `connection_middleware_timeout_secs` | 10 s | Handshake/identity/admit/opened invocations and aggregate closed chain |
| `connection_lifecycle_timeout_secs` | 30 s | Default connected/disconnected invocation cap |

These settings accept 1..=300 seconds. A timeout on a connected/disconnected
method applies to that invocation; it does not create a message deadline.
Controller message defaults do not apply to connection hooks.
The HTTP Upgrade, heartbeat, transport write and DI cleanup policies retain
their own resource bounds. During shutdown every dependent operation remains
subject to the existing single absolute root deadline.

After execution is confirmed stopped, the retained message owner runs eligible
`on_message_termination` hooks serially in reverse entered order. Its aggregate
cap starts with termination and is clipped by the root cleanup deadline. Each
invocation has an independent cleanup signal and a share of the remaining cap.
A local message expiry does not consume all cleanup opportunity. Completed
normal exits are not retried as termination hooks. Exact DI receipts outlive
execution, and unconfirmed cleanup cannot be reported as complete.

## Cooperative result contract

The cancellation policy applies to both message timeout and forced
shutdown:

```text
Local deadline expires / forced execution cancellation requested
  -> signal this accepted message's ExecutionCancellation
  -> keep polling the same normal pipeline during its cooperative window
       -> pipeline returns: retain its actual result
       -> window ends while pending: stop only the execution slot
  -> confirm execution termination
  -> retained lifecycle owner reconciles remaining middleware and DI obligations
```

The local cooperative window is 250 ms, shortened by the current root execution
limit. A later cancellation cause cannot restart or extend that window. Graceful
admission closure alone does not cancel accepted execution.

Each message has its own child execution source; local expiry cannot cancel
its connection, another message or a cleanup invocation. Callback parameters,
exchange/context accessors and the action extractor share that message's view.
The first **recorded** cancellation cause remains immutable. If both sources
first become observable in the same poll, connection cancellation wins; an
elapsed local deadline still clips the cooperative window. Later local expiry
is recorded separately and cannot replace the earlier cause. This is observation
order, not a wall-clock ordering guarantee for simultaneous external signals.

Deadline exceeded, cancellation request/reason, execution disposition and the
actual pipeline result are separate facts. A returned success or ordinary
application error within the cooperative window is retained for both timeout
and shutdown; a cancellation signal alone does not replace it with a timeout
response. A handler return still requires the rest of the normal pipeline,
including reverse exits, to finish. An interrupted pipeline has no completed
result to preserve.

Accepted outbound authority remains usable within its bounded window. The
framework's terminal reply has its own bounded authority and at most one
terminal decision is published. Prepared, queued and written are distinct
evidence; none guarantees peer delivery.

The reader continues handling Ping/Pong/Close while accepted execution
cooperates. Execution cancellation alone is not transport failure. After normal
unwind, DI close and the exact owner join, Lily attempts the one terminal reply,
then any shutdown Close. Queue admission uses the existing queue cap and current
root transport cutoff, independently of the cancelled execution signal. A peer,
write or protocol failure can still prevent this attempt or delivery.

Forced shutdown reserves transport time after the execution cutoff and before
connection cleanup/final reconciliation. These are boundaries within the same
root deadline; see the [budget policy](SHUTDOWN_ARCHITECTURE.md#phase-4-absolute-deadlines-and-cooperative-execution-cancellation).
Recovery after a server task panic retains the same bounded connection tail.
It still requires actual child joins and preserves the server failure.

A local message timeout alone does not require closing a healthy connection.
After message cleanup and DI termination are confirmed, it may process the next
message. The timeout reply is one keep-open `MESSAGE_TIMEOUT` error frame,
encoded through the selected action codecs. Normal response preparation does
not start a second timeout. A codec failure, transport/protocol failure,
application shutdown or missing required cleanup/DI/join evidence may still
require connection termination.

## Reporting and completion evidence

Shutdown diagnostics retain bounded counters across the application's lifetime.
They contain no message payload, arbitrary error text or per-message history.
The cancellation source and result remain independent observations:

| Evidence | Meaning |
| --- | --- |
| `deadline_exceeded` | Normal message deadline elapsed, including pipelines which subsequently completed. |
| `timeout_cancellation` / `connection_cancellation` | First recorded cancellation source; each message contributes to at most one. Connection cancellation can come from shutdown or connection termination. |
| `execution.completed` / `timed_out` / `aborted` / `panicked` | Observed execution disposition, independently of cancellation/abort requests. |
| `completed_after_cancellation` / `completed_after_deadline` | A terminal pipeline result was retained after the respective notification/deadline. |
| `pipeline.handled` / `rejected` / `closed` / `failed` | Actual complete pipeline result, before termination cleanup, DI and transport. An ordinary application error can be a completed, rejected pipeline. |
| `pipeline.not_returned` / `unobserved` | Execution did not return a pipeline result, or no pipeline-result observation exists. Neither counts as success. |
| Middleware and DI evidence | Entered prefix, normal/termination exits, exact scope receipts, disposal failure and outstanding owners remain separately tracked. |

Message output decisions have a separate observation transferred from the
message owner to its connection. Its lifetime continues after the owner join:

```text
Owner registered
  -> final decision prepared after normal/termination unwind
  -> DI scope termination + actual owner join
  -> one terminal attempt
       -> application frame accepted by queue
       -> Close request completed (possibly coalesced with an existing Close)
       -> NoReply decision completed
       -> failed / interrupted / panicked

A decision dropped before its attempt -> suppressed
An owner ending without a prepared decision -> not_prepared
Any still-live preparation/attempt -> outstanding
```

`output.prepared_frames`, `prepared_closes` and `prepared_no_reply` do not imply
an attempt. `output.queued_frames` proves local queue acceptance only; it proves
neither socket write/flush nor peer delivery. `close_requests_completed` does
not prove that this particular Close won the transport's first-writer decision,
or that the peer acknowledged it. Side-effect sends and pre-route protocol
rejections are outside these **routed message terminal** counters; dispatcher
receipts and existing outbound metrics describe their own acceptance boundaries.
No delivered or per-message written counter is inferred.

Dropping an unpolled terminal future records suppression; dropping an attempted
send records interruption. These observations survive registry retirement and
lost waiters without new tasks or an unbounded output registry. An outstanding
output prevents dependency disposal and a successful aggregate shutdown result.
A late outcome updates live evidence but cannot rewrite the immutable shutdown
attempt report.

All exclusive outcomes must reconcile with their owner/decision totals; a
queued result requires both preparation and an attempt. A `GracefulCompleted`
shutdown describes framework termination and cleanup, not success or delivery
of every historical message. For example, nine accepted messages may include
one interrupted local timeout, one completed after its deadline and nine queued
terminal replies; a later shutdown can still complete gracefully. A peer closing
while a pipeline cooperates can leave `pipeline.handled = 1` and
`output.suppressed = 1` without fabricating a successful send.

The executable [qualification matrix](SHUTDOWN_QUALIFICATION.md#message-deadline-and-cooperative-result-qualification)
covers these distinctions, including the complete root coordinator and real
WebSocket connections. Timing guarantees apply at asynchronous polling
boundaries; blocking application polls/destructors remain non-preemptible.

## Implementation sessions

| Session | Status and delivery |
| --- | --- |
| 1 | Implemented: whole-pipeline absolute deadline, configuration/metadata separation, immutable deadline views, distinct execution timeout outcome, entered-prefix cleanup regression tests. |
| 2 | Implemented: per-message timeout cancellation, 250 ms cooperation, first-stop arbitration, full-pipeline result preservation during timeout/shutdown, bounded outbound continuation and healthy connection reuse after local timeout. |
| 3 | Implemented: complete race/wire/concurrency qualification and reporting of cancellation, actual result, outbound, cleanup and resource evidence. |
