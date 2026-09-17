# Lily RabbitMQ Consumer Architecture

## Supported model

Lily has one built-in queue transport: RabbitMQ. `provider`, `broker_type` and
second-provider abstractions are intentionally absent from the application
surface. `lily_queue_client` owns direct, non-transactional publishing. In the
optional PostgreSQL profile, application handlers insert durable events through
`PostgresTransaction::enqueue` and the framework-owned outbox relay publishes
them after commit. This crate owns consumer delivery and settlement in both
profiles.

```text
#[queue_service] metadata
          +
[[rabbitmq.topology.queues]]
          |
          v
lily_consumer validation plan
          |
          v
QueueService -> RabbitMQ runtime -> fresh DI scope -> typed handler
          |
          v
ACK / confirmed retry handoff / confirmed DLQ handoff / NACK-requeue
```

Generated metadata identifies the queue, service type, method, schema version
and content kind. Configuration is the only runtime policy authority. Each
configured queue must have at least one handler and every
`(queue, schema version, content kind)` key must be unique. One RabbitMQ
consumer pipeline per configured queue selects from an immutable local
dispatch table before DI scope creation. A missing handler, duplicate exact
contract or invalid metadata fails startup. Extra linked handlers whose queue
is not configured remain inactive.

Canonical event ID, schema-version and content-kind headers are mandatory;
there is no headerless fallback. Unknown version/content keys are permanent
rejections before application execution. RabbitMQ does not inspect those
headers when selecting a competing consumer, so all replicas sharing one
physical queue must support the full set publishers can emit. Incompatible
version-specific binaries require separate queues/routing bindings.

## Delivery bounds

- `prefetch_count` is RabbitMQ's maximum unacknowledged delivery window.
- `delivery_buffer_capacity` bounds the local receiver-to-dispatch buffer and
  cannot exceed prefetch.
- `concurrency` bounds simultaneous handler futures.
- These values are independent and are never multiplied into a hidden limit.
- Message size, handler duration, retry attempts and all main/retry/DLQ
  retention dimensions are validated before startup.
- Retry jitter is explicit and disabled by default. A non-zero bounded ratio
  expands each nominal exponential delay into at most three predeclared
  queue-level TTL buckets. Event ID plus retry attempt selects the bucket
  deterministically; no handler sleeps or per-message expiration are used.
- Retry topology is bounded to 32 physical buckets per logical queue and 4096
  across one compiled topology before any RabbitMQ operation.

Manual ACK is a fixed framework behavior. Successful handlers ACK directly.
Retryable failures publish into a bounded topology-v2 delay bucket and ACK the
original only after publisher confirmation. Permanent or exhausted failures
use the same confirmation rule for the dead-letter handoff. If handoff cannot
be proven, Lily NACKs/requeues or records the unresolved shutdown outcome; it
does not silently ACK.

## Scope and shutdown

The production macro path resolves the handler service inside a fresh
`ApplicationContainer` scope for every message. Scope cleanup runs on success,
error and cancellation.

Shutdown order is fixed:

1. stop broker delivery admission and, when configured, stop new outbox claims
   and new transactional inbox admission;
2. drain receivers, dispatchers, handler futures, settlements and delivery
   scopes;
3. when the one application deadline enters its force reserve, cancel
   settlement authority and make each dispatcher abort and join its nested
   handler/retry/settlement task set;
4. stop and join every outbox relay worker, then wait for the PostgreSQL owners
   of admitted transactions to finish their actual commit or rollback;
5. prove queue supervisors, delivery scopes, relay workers and transaction
   owners reconciled;
6. close RabbitMQ channels and connections;
7. let the process composition root dispose DI and tracing owners.

An outbox row committed after relay admission stops remains durable for the
next process startup; shutdown does not publish it through a detached task.
RabbitMQ connection close and DI disposal are forbidden until the relevant
delivery, relay and transaction owners have reconciled or shutdown reports an
incomplete result.

The application-wide shutdown coordinator is the only deadline authority.
Queue task handles and RabbitMQ connection handles remain owned across caller
cancellation, so a forced attempt cannot observe false success after losing a
handle. Completed drain results are replayed to concurrent observers.

Connection close and application-root DI disposal are gated on proven queue
task reconciliation. A non-yielding CPU-bound user future cannot be forcibly
preempted by Tokio; if it exceeds the force budget, shutdown is reported as
`Incomplete` and Lily does not claim that broker or DI resources were safely
disposed. Cooperative asynchronous handlers are abort-and-joined without a
framework-owned detached task. When force-drain aborts an otherwise cooperative
delivery future, its container-owned scope cleanup receives one final bounded
100 ms reconciliation window; a stuck disposer is then aborted and joined and
the incomplete cleanup remains visible to shutdown.

Sampling-independent snapshots expose aggregate reconciliation and a bounded
set of event-level terminal observations. Payloads and credentials are never
retained in that evidence.

## Reliability boundary

RabbitMQ delivery is at-least-once by default. The opt-in PostgreSQL
transactional inbox/outbox profile deduplicates one logical handler/event pair
and commits inbox completion, application mutations made through the typed
transaction context, and durable outbox insertion in one database transaction.
RabbitMQ and PostgreSQL are not joined by 2PC: a confirmed outbox publish may
be repeated after a crash before its delivered mark, using the same event ID.
Independent database connections and external side effects remain outside this
effectively-once boundary.

The logical handler identity is generated as
`module_path::ServiceType::method`. Moving or renaming any of those parts creates
a new deduplication namespace, so handler refactors are data-compatibility
changes. Completed rows deduplicate only while retained; after
`inbox_retention_secs` cleanup, an older replay may execute again even when the
handler identity did not change.

See `QUEUE_TOPOLOGY_MIGRATION_V2.md` before deploying topology v2 over existing
RabbitMQ resources.
