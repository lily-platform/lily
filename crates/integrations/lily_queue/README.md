# Lily Queue

`lily_queue` provides Lily's RabbitMQ consumer runtime, typed handler macros,
delivery context and opt-in PostgreSQL and MongoDB transactional inbox/outbox
adapters. Applications continue to own their database clients and business
schemas. Lily owns only its bounded, namespaced persistence components,
explicit migrators, runtime transaction boundary and outbox relay.

The canonical application path is:

1. define an injectable service;
2. put `#[queue_service]` on its inherent impl;
3. put an exact `#[queue("name", version = 1..=65535, content = "...")]`
   contract on each typed handler;
4. declare the same queue under `[[rabbitmq.topology.queues]]`;
5. run `lily_consumer::Consumer`.

Handlers may combine body-free delivery metadata and DI extractors with one
terminal `Json<T>`, `TextPayload`, `BinaryPayload`, `RawDelivery`, or custom
`FromDelivery` payload extractor. `E` must convert into
`QueueHandlerError`, whose explicit retryable/permanent class controls the
bounded settlement path. Every delivery gets a fresh Lily DI scope, so scoped
and transient dependencies retain their registered lifecycle.

Application-defined middleware and guards are optional. Global types are
registered with `Consumer::builder().middleware::<M>()` and `.guard::<G>()`;
repeatable `#[middleware(Type)]` and `#[guard(Type)]` markers below the outer
`#[queue_service]` attribute apply at service or handler level. Effective
order is global, service, handler. All middleware enters before guards; only
the successfully entered prefix unwinds in reverse before delivery-scope
cleanup and broker settlement. Guards return an explicit retryable/permanent
`QueueHandlerError`, not a boolean or ACK/NACK decision.

Implement `QueueMiddleware` or `QueueGuard` with the
`lily_queue::async_trait` re-export. Constructors receive
`Arc<lily_injection::Extensions>` (also exposed as `lily_consumer::Extensions`)
and run once per Consumer build. Retain only
application-lifetime dependencies there; resolve scoped/transient dependencies
per delivery through `QueueDeliveryExchange::service`.

Lily polls those constructors directly in the Consumer composition task under
one aggregate initialization deadline. A constructor error, panic, timeout or
outer build cancellation first drops the currently pending constructor future,
then releases every already initialized component exactly once in reverse
constructor order. There is no framework-owned detached constructor task.
Application code should keep future-owned partial state cancellation-safe;
tasks explicitly spawned by a constructor remain application-owned.

Link-time handler metadata is the only consumer-registration authority. Direct,
non-transactional publishing belongs to `lily_queue_client`. A transactional
handler instead inserts durable events with `PostgresTransaction::enqueue`; the
framework-owned relay publishes those rows after the business transaction
commits.

Queue-provider startup is cancellation-safe. Lily adopts the provider before
polling its first RabbitMQ startup operation. A failed, panicking, or cancelled
DI build therefore drops the pending startup future before the same retained
provider is stopped through DI rollback. Successful startup transfers that
single cleanup authority to normal Consumer shutdown; applications neither
start nor stop the internal provider directly.

Physical RabbitMQ registration is also cancellation-safe. Lily adopts one
engine-owned supervisor task before dedicated-channel acquisition or
`Basic.Consume` begins; the `create_queue` caller only observes its bounded
startup result. Dropping that observer cancels the pending registration but
does not detach its owner. An accepted consumer is reconciled with
`Basic.Cancel` followed by dedicated-channel close, and a queue whose cleanup
cannot be proven is not registered a second time in the same runtime. The
delivery-scope tracker is provider-owned before this cancellable boundary, so
shutdown can still join scoped DI cleanup after a registration caller exits.

Canonical deliveries require `x-lily-event-id`,
`x-lily-schema-version` and `x-lily-content-kind`. There is no headerless or
content-sniffing fallback. A configured physical queue may have multiple
handlers, but every `(queue name, schema version, content kind)` key must be
unique. Lily creates one RabbitMQ consumer pipeline for that queue and selects
the exact handler from an immutable process-local dispatch table before it
creates a delivery DI scope or enters application middleware. An unknown
version or content kind is therefore a typed permanent rejection, not a
best-effort decode attempt. Duplicate exact contracts fail startup before
broker admission.

Built-in payload contracts are `json` with `Json<T>`, `text` with
`TextPayload`, and `binary` with `BinaryPayload`. `RawDelivery` preserves the
bounded bytes and metadata for an explicitly declared raw or application
content token. An application-defined terminal `FromDelivery` extractor owns
decoding for its exact custom token. A handler without a payload still declares
an exact token, conventionally `none`; content identity is never inferred from
its Rust signature.

## Optional AsyncAPI handler metadata

The default build has no AsyncAPI model, schema pointer or documentation
metadata. Enabling `lily_queue/asyncapi` adds the facade-owned `#[asyncapi]`
marker and re-exports the exact `schemars` version used by Lily. Put the marker
beneath the outer `#[queue_service]` attribute, either on the impl for inherited
defaults or on an individual `#[queue]` method. The generated metadata remains
part of the one canonical `QueueHandlerMetadata` record; it is not a second
handler registry.

`Json<T>`, `TextPayload` and `BinaryPayload` provide their canonical payload
contract automatically. Raw/custom/body-unobserved handlers must select an
explicit `schema = Type, content_type = "..."` or
`opaque, content_type = "..."` contract. `#[asyncapi(skip)]` is the explicit
exclusion. The `lily_consumer/asyncapi` composition feature validates and
publishes the complete document after runtime plan acceptance; this low-level
crate does not build or attach a standalone document.

`delivery_execution_timeout_millis` is one aggregate delivery budget. Before
middleware, guards, extraction, handler work and normal reverse `after_delivery`
share one `DeliveryDeadline`. The smaller of one quarter of the original
remaining budget or one second is reserved for cooperation, abnormal cleanup
and DI disposal; normal after hooks do not start separate timeouts.

On local timeout, graceful deadline exhaustion or force, Lily signals the
read-only `DeliveryCancellation` and continues polling the entire normal
pipeline in a cooperative window of at most 250 ms, shortened by the remaining
delivery/root budget. If the pipeline really returns, its success or typed
failure is retained. Otherwise only execution is dropped; the lifecycle owner
remains alive. Handler success alone is insufficient if normal after is pending.

`QueueMiddleware::on_delivery_termination` is the separate best-effort abnormal
hook. It runs in reverse for the successfully entered prefix whose normal exit
was not started, interrupted or panicked. Normal exits which returned `Ok` or
`Err` are not retried. A panicked normal after stops normal unwind so its inner
termination precedes outer cleanup. The termination context supplies DI/local
state and a fresh read-only `DeliveryCleanupCancellation` with its absolute
deadline, independent of execution cancellation and sibling hooks. A callback
cannot change the pipeline result or issue ACK/NACK. Partial normal/termination
effects must be cancellation-safe; termination callbacks are never replayed.
Hook timeouts and panics are retained as cleanup failures, not success.

Each delivery attempt retains its invocation, entered middleware ledger and
exact DI scope receipt outside the user execution future. Aborting the outer
delivery task first drops execution, then transfers those resources to tracked
cleanup. Provider drain waits for both scope termination and the cleanup task's
real join; cancelling a waiter cannot lose the original disposer result.
Cleanup uses the remaining delivery/root budget, including later root
shortening. Completed cleanup receipts are reaped during normal operation.
Pipeline completion does not prove transaction commit or broker settlement.
ACK/NACK materialization retains its own local cap, clamped to the same shutdown
root, including later root shortening. Basic.Cancel stops the subscription while
the engine retains its channel for accepted work. The channel closes only after
delivery/settlement tasks and the receiver/dispatcher have actually joined.
Force notification does not immediately revoke healthy-channel settlement:
completed cooperative work may still ACK within the remaining bound. Final
abort first revokes settlement and then confirms all task joins.

Each physical subscription generation retains its channel and consumer drop
guard outside the receiver. Recovery waits for old-generation joins and channel
termination before publishing a fresh subscription under the registration gate.
Unproven joins prevent parent disposal; unproven channel close is reported and
retained for parent connection cleanup. Confirmed handoff plus incomplete ACK
remains unresolved, never successful or replayed. Transaction/outbox dependency
reconciliation follows the ownership and completion rules in the
[Consumer lifecycle contract](../../framework/lily_consumer/LIFECYCLE.md).

Retry delay jitter is opt-in through `retry_jitter_ratio` and defaults to
`0.0`. Lily keeps RabbitMQ queue-level TTL buckets: a non-zero finite ratio up
to `0.5` predeclares at most lower/nominal/upper delays and chooses one with a
stable event-ID/attempt hash. No arbitrary per-message TTL, handler sleep,
delayed-message plugin or dynamic queue is introduced. The compiled plan fails
before broker I/O above 32 retry buckets per logical queue or 4096 in total.

## Version rollout boundary

Multiple identical Consumer replicas are normal RabbitMQ competing consumers:
each delivery is handled by one replica. RabbitMQ does not route a delivery to
a process by Lily's schema/content headers. Consequently, every binary that
consumes the same physical queue must support every contract that publishers
may emit. Deploy all consumers with both the old and new handlers, remove the
old-only fleet, and only then publish the new version. Keep incompatible
version-specific binaries on different physical queues/routing bindings;
putting a V1-only and V2-only binary on the same queue is not safe load
distribution.

## PostgreSQL transactional inbox/outbox

This capability is opt-in. Enable one matching feature family on the Consumer,
the directly imported queue facade and PostgreSQL adapter. The direct
`lily_queue` dependency is required because handlers import its macros,
extractors and outbox types; `uuid` is required by the example's application
event-ID generation.

Single-database applications use:

```toml
lily_consumer = { version = "0.1.0", features = ["transactional-inbox-postgresql"] }
lily_queue = { version = "0.1.0", features = ["transactional-inbox-postgresql"] }
lily_postgresql = { version = "0.1.0" }
uuid = { version = "1", features = ["v4"] }
```

Applications that already own named PostgreSQL cells use:

```toml
lily_consumer = { version = "0.1.0", features = ["transactional-inbox-postgresql-factory"] }
lily_queue = { version = "0.1.0", features = ["transactional-inbox-postgresql-factory"] }
lily_postgresql = { version = "0.1.0", default-features = false, features = ["factory"] }
uuid = { version = "1", features = ["v4"] }
```

The single and factory feature families are mutually exclusive; neither
transactional capability is enabled by default.

The low-level `lily_queue` features compile the PostgreSQL adapter API but do
not register a database service. The canonical `lily_consumer` features enable
the selected `lily_postgresql` single/factory DI mode. A standalone
`QueueService` composition root must enable and own that mode explicitly.

Mark only handlers whose database work must share Lily's inbox transaction:

For direct use, depend on `lily_queue = "0.1.0"`; the facade path is
`lilyrs::queue` with feature `queue` (also exposed by `consumer`). Handler
macros and their registry support are re-exported, so no separate derive or
registry dependency is needed.

```rust,ignore
use lily_queue::{
    Json, PostgresTransaction, PublishContentKind, QueueHandlerError,
    TransactionalOutboxMessage, queue, queue_service,
};
use lily_postgresql::{diesel, diesel_async};

#[queue_service]
impl OrderWorker {
    #[queue(
        "orders.created",
        version = 1,
        content = "json",
        delivery_guarantee = "transactional_inbox"
    )]
    async fn created(
        &self,
        transaction: PostgresTransaction,
        Json(event): Json<OrderCreated>,
    ) -> Result<(), QueueHandlerError> {
        transaction
            .with_connection(move |connection| Box::pin(async move {
                // Run ordinary diesel-async business mutations on this exact
                // connection. A separate pool acquisition is outside the
                // effectively-once transaction boundary.
                persist_order(connection, event).await
            }))
            .await?;

        transaction
            .enqueue(TransactionalOutboxMessage::try_new(
                uuid::Uuid::new_v4(),
                "domain.events",
                "orders.persisted",
                1,
                PublishContentKind::Json,
                br#"{"status":"persisted"}"#.as_slice(),
            )?)
            .await?;
        Ok(())
    }
}
```

The matching `[[rabbitmq.topology.queues]]` entry must contain a strict
`[rabbitmq.topology.queues.transactional_inbox]` policy. In factory mode its
`database_cell` is mandatory; in single mode it is forbidden. Handler intent,
queue binding and installed schema are three separate authorities. A missing,
stale or contradictory authority fails before RabbitMQ listener admission and
never falls back to at-least-once processing.

A complete single-mode PostgreSQL and queue binding looks like this (the
RabbitMQ connection and exchange definitions are omitted only for brevity):

```toml
[postgresql]
mode = "single"
connection_string = "${secret:postgresql.primary_url}"

[[rabbitmq.topology.queues]]
name = "orders.created"
exchange_name = "orders"
routing_key = "orders.created"
retry_attempts = 3

[rabbitmq.topology.queues.transactional_inbox]
backend = "postgresql"
```

The equivalent factory-mode binding names the same exact PostgreSQL cell in
both configuration authorities:

```toml
[postgresql]
mode = "factory"

[[postgresql.cells]]
name = "primary"
connection_string = "${secret:postgresql.primary_url}"

[[rabbitmq.topology.queues]]
name = "orders.created"
exchange_name = "orders"
routing_key = "orders.created"
retry_attempts = 3

[rabbitmq.topology.queues.transactional_inbox]
backend = "postgresql"
database_cell = "primary"
```

The application imports Diesel operations from its direct `lily_postgresql`
dependency; `lily_consumer` does not expose PostgreSQL as an accidental facade.
If several physical queues use the same PostgreSQL cell, their complete relay
policy must be identical because that database has one outbox table and one
framework-owned relay worker. A contradictory policy is rejected before
listener admission.

A concurrent duplicate is classified as a retryable `InProgress` delivery and
uses the queue's existing bounded retry/DLQ policy. It never enters the handler
and never ACKs as completed. Configure a non-zero retry budget when concurrent
delivery of the same event identity is expected; the original uncommitted
delivery remains broker-owned if its transaction aborts.

Install Lily's schema explicitly in a deployment migration job. In single
mode, resolve the canonical database service:

```rust,ignore
let database = container.resolve::<lily_postgresql::PgDatabaseService>(None).await?;
let report = lily_queue::PostgresInboxOutboxMigrator::new(database)
    .migrate()
    .await?;
```

In factory mode, select the same exact cell named by
`transactional_inbox.database_cell`:

```rust,ignore
let factory = container.resolve::<lily_postgresql::PgFactory>(None).await?;
let database = factory.get("primary")?;
let report = lily_queue::PostgresInboxOutboxMigrator::new(database)
    .migrate()
    .await?;
```

Run application migrations first, the Lily migrator second, and deploy the
Consumer only after both succeed. Consumer startup performs a read-only schema
version check and never runs DDL. The Lily tables and independent migration
ledger live under the `lily_queue` PostgreSQL schema.

The guarantee is deliberately narrow: inbox claim, mutations made through
`PostgresTransaction`, inbox completion and outbox insertion commit in one
PostgreSQL transaction. RabbitMQ ACK happens only after that commit. The relay
publishes a durable outbox row with mandatory routing and publisher confirms,
then marks it delivered. A crash between publish confirmation and the delivered
mark may publish the same event ID again; a downstream transactional inbox
deduplicates it. Lily does not claim broker/database 2PC, global exactly-once,
or exactly-once behavior for HTTP, email, files, independently acquired
database connections or other external side effects.

The deduplication key contains the generated fully qualified handler identity
(`module_path::ServiceType::method`) and the delivery event ID. Moving the
service to another module or renaming its type or method therefore creates a
new logical handler identity. Treat such a refactor as a data-compatibility
change: an inbox row written under the old identity will not deduplicate a
delivery handled under the new identity.

`inbox_retention_secs` is also the explicit deduplication horizon. Once a
completed inbox row is removed by bounded cleanup, a delivery older than that
horizon can be processed again. Choose retention from the broker's maximum
redelivery/backup replay window rather than treating the seven-day default as
an unlimited guarantee.

RabbitMQ connection managers, channels, retry machinery, generated registry
metadata and mutable runtime settings are deliberately not public application
APIs.

## MongoDB transactional inbox/outbox

MongoDB support is also default-off and uses the same storage-neutral
`delivery_guarantee = "transactional_inbox"` handler metadata. A canonical
Consumer application selects exactly one mode:

```toml
# Single MongoDB service
lily_consumer = { version = "0.1.0", features = ["transactional-inbox-mongodb"] }
lily_queue = { version = "0.1.0", features = ["transactional-inbox-mongodb"] }
lily_mongodb = { version = "0.1.0" }
tokio-util = "0.7"
```

```toml
# Named MongoDB cells
lily_consumer = { version = "0.1.0", features = ["transactional-inbox-mongodb-factory"] }
lily_queue = { version = "0.1.0", features = ["transactional-inbox-mongodb-factory"] }
lily_mongodb = { version = "0.1.0", default-features = false, features = ["factory"] }
tokio-util = "0.7"
```

The `lily_queue` features compile the storage adapter API but do not register a
database service by themselves. `lily_consumer` is the composition authority
which enables the selected `lily_mongodb` single/factory DI mode. A standalone
`QueueService` composition root must enable and own the matching
`lily_mongodb` mode explicitly.

Use `MongoInboxOutboxMigrator` in an explicit deployment/migration program.
Normal Consumer startup only verifies the framework component migration and
MongoDB transaction capability; it never creates collections or indexes. The
single-mode binding names no cell:

```toml
[database]
mode = "single"
database_type = "mongodb"
connection_string = "${secret:mongodb.primary_url}"
database_name = "orders"

[[rabbitmq.topology.queues]]
name = "orders.created"
exchange_name = "orders"
routing_key = "orders.created"

[rabbitmq.topology.queues.transactional_inbox]
backend = "mongodb"

[rabbitmq.topology.queues.transactional_inbox.mongodb]
max_transaction_attempts = 3
retry_initial_backoff_millis = 10
retry_max_backoff_millis = 1000
commit_retry_timeout_millis = 10000
```

Apply the component migration with the canonical single service:

```rust,ignore
use tokio_util::sync::CancellationToken;

let database = container
    .resolve::<lily_mongodb::DatabaseService>(None)
    .await?;
let operation = database.operation_context(CancellationToken::new())?;
let report = lily_queue::MongoInboxOutboxMigrator::new(database.as_ref())?
    .apply(&operation)
    .await?;
```

Factory mode requires the exact same cell in both configuration authorities:

```toml
[database]
mode = "factory"

[[database.cells]]
name = "primary"
database_type = "mongodb"
connection_string = "${secret:mongodb.primary_url}"
database_name = "orders"

[[rabbitmq.topology.queues]]
name = "orders.created"
exchange_name = "orders"
routing_key = "orders.created"

[rabbitmq.topology.queues.transactional_inbox]
backend = "mongodb"
database_cell = "primary"

[rabbitmq.topology.queues.transactional_inbox.mongodb]
max_transaction_attempts = 3
retry_initial_backoff_millis = 10
retry_max_backoff_millis = 1000
commit_retry_timeout_millis = 10000
```

The migration job selects that exact factory cell:

```rust,ignore
use tokio_util::sync::CancellationToken;

let factory = container.resolve::<lily_mongodb::MongoFactory>(None).await?;
let database = factory.get("primary").expect("configured MongoDB cell");
let operation = database.operation_context(CancellationToken::new())?;
let report = lily_queue::MongoInboxOutboxMigrator::new(database.as_ref())?
    .apply(&operation)
    .await?;
```

Lily never guesses a cell. The selected deployment must support multi-document
transactions (a replica set or supported sharded deployment). An incompatible
topology fails before RabbitMQ listener admission—there is no at-least-once
fallback.

Transactional handlers extract `MongoTransaction`. Every covered collection
or repository mutation must use `MongoTransaction::operation_context()`, and
covered outgoing events must use `MongoTransaction::enqueue`. Resolving the
same database separately and writing without that operation context is outside
the effectively-once boundary. The handle is valid only while that handler body
is running. Do not move a clone into a detached task: Lily cancels every escaped
clone when the body returns, and later operation-context or enqueue calls fail
closed instead of becoming post-commit writes.

MongoDB may label an error `TransientTransactionError`; Lily then replays the
entire delivery pipeline in a fresh DI delivery scope with a fresh session.
Middleware, guards, extractors and handler code can therefore run more than
once before one commit. They must not perform irreversible network, file or
other transaction-external side effects. `UnknownTransactionCommitResult`
retries only commit-result reconciliation within its configured bound. If the
result remains ambiguous, Lily retains the external lease until expiry and
directly requeues the original RabbitMQ delivery; it does not consume the
application retry budget or route the message to the DLQ.

An external MongoDB lease serializes concurrent deliveries of the same
generated handler identity and event ID across Consumer processes. A retained
delivery polls with MongoDB server time inside its absolute delivery budget,
rechecks the durable inbox, and either observes `AlreadyCompleted` or acquires
an expired lease. Budget exhaustion uses one direct broker requeue; it does not
consume the configured application retry budget and cannot enter the DLQ via
that contention path. Post-commit lease deletion is advisory: failure is
recorded in the payload-free transactional inbox snapshot but cannot replace a
committed `Applied`/`AlreadyCompleted` result.

`QueueService::transactional_inbox_snapshot()` and the canonical Consumer
operational snapshot expose saturating, sampling-independent counts for active
owners, body/commit attempts, transaction/commit retries, exhausted retries,
unknown commit outcomes, lease contention and post-commit release failures.
Only a stable bounded failure code is retained; payloads, event IDs, handler
names, database cells, endpoints and credentials are never included.

## Forced lifecycle phases

`DeliveryCancellation` is a read-only extractor and middleware/guard view.
Graceful admission closure alone leaves it active. Delivery timeout and
framework execution cancellation notify it; `reason()` returns the first
recorded `DeliveryCancellationReason`. A local timeout cannot cancel another
delivery, cleanup or settlement. A cancellation reason is not proof that the
handler, DI cleanup or ACK has completed.

The Consumer composition root publishes one absolute graceful/hard deadline
pair to the queue runtime. Repeated publication can shorten, never renew, the
attempt. Cleanup has an independent framework authority. Its invocation-level
wiring and bounded cooperative execution are tracked in the
[Consumer lifecycle stages](../../framework/lily_consumer/LIFECYCLE.md).

Queue lifecycle registration keeps admission, in-flight work and connection
disposal as three distinct shutdown phases. A pre-existing force request does
not collapse those phases: `StopAdmission` calls `stop_admission_async`,
`DrainInFlight` calls `force_drain_async`, and `DisposeDependencies` calls
`close_async`. The canonical pre-forced order is therefore stop admission,
force drain, then close, exactly once per phase. A deterministic
`register_queue_lifecycle` regression freezes that order and requires a
`ForcedCompleted` report whose accounting reconciles.

Transaction owners and outbox workers adopt the Consumer application's absolute
shutdown deadline. Force first notifies delivery execution and preserves its
bounded cooperative result; driver finalization has only the remaining owner
budget. PostgreSQL queue work uses an integration-owned driver task, and MongoDB
retains both transaction and lease-heartbeat joins. An interrupted transaction
owner can be joined without confirmed database finalization: that is reported
as incomplete, not as a successful commit/rollback. A confirmed outbox publish
whose delivered mark is interrupted remains `uncertain_after_publish` and is
left for claim expiry. Cancelling shutdown observers cannot detach these owners
or allow DI disposal before their real joins and delivery-scope receipts.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_queue](https://docs.rs/lily_queue).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
