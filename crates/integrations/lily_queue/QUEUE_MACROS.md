# Queue Handler Macros

Use `#[queue_service]` on an implementation and declare the exact schema and
content contract on each typed handler:

```rust,ignore
use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;
use lily_queue::{
    BinaryPayload, Json, QueueHandlerError, guard, middleware, queue, queue_service,
};

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct OrderWorker;

impl ServiceTrait for OrderWorker {}

#[queue_service]
impl OrderWorker {
    #[queue("orders.created", version = 1, content = "json")]
    async fn created(
        &self,
        Json(order): Json<OrderCreated>,
    ) -> Result<(), QueueHandlerError> {
        self.store(order)
            .await
            .map_err(|_| QueueHandlerError::retryable("ORDER_STORE_UNAVAILABLE"))
    }
}
```

The macro records one immutable typed extraction plan and handler identity.
JSON/text/binary/raw/custom decoding is performed by the runtime extractor
contract, not by a second registration path. The macro does not own delivery
policy. The impl owner must be registered in Lily DI; a missing owner fails
Consumer startup before broker admission.

Middleware and guards are repeatable sibling markers consumed by the outer
`#[queue_service]` macro:

```rust,ignore
#[queue_service]
#[middleware(ServiceAudit)]
#[guard(ServiceAdmission)]
impl OrderWorker {
    #[queue("orders.created", version = 1, content = "json")]
    #[middleware(HandlerMetrics)]
    #[guard(CanCreateOrder)]
    async fn created(
        &self,
        Json(order): Json<OrderCreated>,
    ) -> Result<(), QueueHandlerError> {
        self.store(order).await
    }
}
```

The outer macro must precede these markers. Middleware order and guard order
are each preserved within their category; all effective middleware runs before
every guard. Within each category the level order is global, service, handler.
Global declarations come from `ConsumerBuilder`. Repeating one concrete type
in the same effective plan is a startup error.

Configure the matching queue through the canonical config:

```toml
[rabbitmq.consumer]
connection_string = "amqps://consumer:secret@rabbit.internal/%2f"
use_tls = true
confirm_timeout_secs = 10

[[rabbitmq.topology.queues]]
name = "orders.created"
exchange_name = "OrderWorker"
routing_key = "orders.created"
retention = { main_max_messages = 100000, main_max_bytes = 1073741824, retry_bucket_max_messages = 10000, retry_bucket_max_bytes = 268435456, dead_letter_max_messages = 10000, dead_letter_max_bytes = 268435456 }
concurrency = 4
prefetch_count = 20
delivery_buffer_capacity = 20
retry_attempts = 3
retry_backoff_millis = 1000
max_retry_backoff_millis = 60000
retry_jitter_ratio = 0.20
delivery_execution_timeout_millis = 30000
settlement_timeout_millis = 5000
max_message_size_bytes = 1048576
durable = true
exclusive = false
auto_delete = false
```

`name` selects the generated handler. `exchange_name` and `routing_key` define
the RabbitMQ binding; they do not select a Rust service or DI type. The runtime
requires at least one linked handler for each configured queue. Multiple
handlers may share that queue when every `(schema version, content kind)` pair
is distinct. An exact duplicate contract, invalid metadata/bound or unresolved
specialized worker component fails startup before consumption begins. Linked
handlers without a matching configured queue remain inactive.

Exact duplicate keys are rejected across the linked binary even when that
queue is inactive. This keeps the registry unambiguous for runtime dispatch
and later AsyncAPI projection; configuration controls activation, not metadata
identity.

One topology entry still produces only one RabbitMQ consumer pipeline per
process. After receiving a canonical envelope, Lily selects one exact handler
from an immutable local table:

```rust,ignore
#[queue_service]
impl OrderWorker {
    #[queue("orders.events", version = 1, content = "json")]
    async fn event_v1(&self, Json(event): Json<OrderEventV1>) -> Result<(), QueueHandlerError> {
        self.apply_v1(event).await
    }

    #[queue("orders.events", version = 2, content = "json")]
    async fn event_v2(&self, Json(event): Json<OrderEventV2>) -> Result<(), QueueHandlerError> {
        self.apply_v2(event).await
    }

    #[queue("orders.events", version = 2, content = "binary")]
    async fn event_v2_binary(&self, body: BinaryPayload) -> Result<(), QueueHandlerError> {
        self.apply_v2_binary(body).await
    }
}
```

The example requires one `[[rabbitmq.topology.queues]]` entry named
`orders.events`, not three entries. Unknown versions and content kinds are
permanent typed rejections before DI scope creation, middleware, guards,
extraction or application code.

Schema conversion remains application-owned. A V1 handler may inject and call
an application upcaster before forwarding to shared business logic; Lily does
not guess or register business conversions. Likewise, a custom terminal
`FromDelivery` extractor is the explicit decoder contract for an
application-specific content token.

`concurrency` is the exact handler permit count. `prefetch_count` is RabbitMQ
QoS. `delivery_buffer_capacity` is an independent bounded in-process queue and
cannot exceed prefetch. `retry_attempts` counts retries after the first handler
attempt. Messages larger than `max_message_size_bytes` never reach a handler.
`delivery_execution_timeout_millis` is the aggregate extraction, handler and
per-delivery DI cleanup budget. Lily exposes an earlier `DeliveryDeadline` to
application work and reserves the smaller of one quarter of the remaining
aggregate budget or one second for mandatory scope cleanup. A plan containing
middleware reserves another bounded tail within the application budget for
reverse unwind. Lily drops a
timed-out handler future, cancels that delivery's cooperative token, awaits
scope cleanup, and only then returns a retryable outcome to the broker
settlement owner.
`settlement_timeout_millis` independently bounds each original-delivery ACK or
NACK operation; it is not added to the handler budget and a timed-out
settlement is never blindly repeated.

Every accepted delivery must carry canonical `x-lily-event-id`,
`x-lily-schema-version` and `x-lily-content-kind` headers. Schema versions are
positive `u16` application-message versions, not Lily framework releases. The
exact content token must match a linked handler before DI or extraction begins.
Built-in mappings are `json` -> `Json<T>`, `text` -> `TextPayload`, and
`binary` -> `BinaryPayload`; `RawDelivery` and an application-defined
`FromDelivery` extractor support explicit raw/custom contracts. A body-free
handler still declares an exact token such as `none`. Headerless legacy JSON
and the former `handler_timeout_millis` name are rejected; there is no
compatibility alias.

For a rolling schema change, first deploy a fleet in which every process that
consumes the physical queue supports both old and new keys. Publish the new key
only after every old-only process has left the fleet. RabbitMQ chooses among
competing consumers without inspecting Lily headers, so V1-only and V2-only
binaries cannot safely share one physical queue. Use separate queues/routing
bindings when binaries intentionally support incompatible contract sets.

Every retention value is mandatory. Main and dead-letter bounds apply to their
single physical queue. Retry bounds apply independently to each distinct
fixed-delay bucket. All framework-owned queues use `reject-publish`; Lily never
silently drops the oldest message to make room.

Retries use RabbitMQ-native `v2` queue-level TTL buckets. The default
`retry_jitter_ratio = 0.0` preserves exact exponential delays: for `1s` base
and `8s` cap the physical retry TTLs are `1s`, `2s`, `4s`, and `8s`; later
capped attempts reuse the `8s` bucket. An explicit finite ratio within
`0.0..=0.5` predeclares at most lower, nominal and upper buckets for each
attempt. Lily deterministically selects one from the event ID plus retry
attempt using FNV-1a 64 over the canonical ID bytes followed by the attempt as
four-byte big-endian data, so every node and repeated handoff uses the same
route. Jitter never uses
per-message expiration, sleeping handler tasks, a delayed-message plugin or
dynamically created queues. At most 32 retry buckets may belong to one logical
queue and at most 4096 may belong to the compiled topology.
`retry_attempts = 0` creates no retry exchange or bucket.

Receiver and dispatcher tasks are supervised as a queue pair. An unexpected
exit, panic, or settlement operation failure marks runtime readiness failed,
cancels all consumer queue pairs, and reaches `Consumer::run*` or
`ManagedConsumer::wait()` as a Consumer runtime error. The broker layer retains
the typed `BROKER_CONSUMER_TASK_FAILED` diagnostic internally. Normal RabbitMQ
connection recovery remains local and bounded.

See [QUEUE_TOPOLOGY_MIGRATION_V2.md](./QUEUE_TOPOLOGY_MIGRATION_V2.md) before
starting against a broker that contains Lily `v1` queues.
