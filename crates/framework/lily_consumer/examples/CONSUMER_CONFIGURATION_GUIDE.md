# Lily Consumer Configuration Guide

Lily consumers use RabbitMQ. Queue identity and wire contract come from
`#[queue("name", version = 1..=65535, content = "...")]`;
delivery policy comes only from `[[rabbitmq.topology.queues]]`.

```toml
[rabbitmq.consumer]
connection_string = "amqps://consumer:secret@rabbit.internal/%2f"
use_tls = true
pool_size = 5
connection_timeout_secs = 10
confirm_timeout_secs = 10
heartbeat_secs = 30
max_reconnect_attempts = 5
reconnect_backoff_millis = 250

[[rabbitmq.topology.queues]]
name = "product.save.create"
exchange_name = "ProductSaveWorker"
routing_key = "product.save.create"
retention = { main_max_messages = 100000, main_max_bytes = 1073741824, retry_bucket_max_messages = 10000, retry_bucket_max_bytes = 268435456, dead_letter_max_messages = 10000, dead_letter_max_bytes = 268435456 }
concurrency = 4
prefetch_count = 20
delivery_buffer_capacity = 20
retry_attempts = 3
retry_backoff_millis = 1000
max_retry_backoff_millis = 60000
retry_jitter_ratio = 0.0
delivery_execution_timeout_millis = 30000
settlement_timeout_millis = 5000
max_message_size_bytes = 1048576
durable = true
exclusive = false
auto_delete = false
dead_letter_exchange = "product.dlx"
dead_letter_routing_key = "product.save.create.failed"
```

`name` is the handler matching key. `exchange_name` and `routing_key` define
the exact RabbitMQ binding; neither selects a Rust service or DI type.

```rust,ignore
#[queue_service]
impl ProductSaveWorker {
    #[queue("product.save.create", version = 1, content = "json")]
    async fn create(
        &self,
        Json(request): Json<ProductCreate>,
    ) -> Result<(), QueueHandlerError> {
        self.save(request)
            .await
            .map_err(|_| QueueHandlerError::retryable("PRODUCT_SAVE_UNAVAILABLE"))
    }
}

Consumer::builder().run().await?;
```

The configuration contains one entry per physical queue, not one entry per
schema version. Several exact handlers can share that entry:

```rust,ignore
#[queue_service]
impl ProductSaveWorker {
    #[queue("product.save.create", version = 1, content = "json")]
    async fn create_v1(
        &self,
        Json(request): Json<ProductCreateV1>,
    ) -> Result<(), QueueHandlerError> {
        self.save_v1(request).await
    }

    #[queue("product.save.create", version = 2, content = "json")]
    async fn create_v2(
        &self,
        Json(request): Json<ProductCreateV2>,
    ) -> Result<(), QueueHandlerError> {
        self.save_v2(request).await
    }
}
```

The exact key is `(queue name, schema version, content kind)`. Duplicate keys
fail startup. A missing event ID/version/content header, or an unsupported
version/content pair, is rejected without a headerless or content-sniffing
fallback. Built-in tokens are `json`, `text`, and `binary`; raw/custom payload
extractors use their explicitly declared token. A body-free handler commonly
uses `content = "none"`.

## Rolling deployments and replicas

Identical Consumer replicas may compete on the same RabbitMQ queue to spread
load. RabbitMQ does not inspect Lily headers before choosing a replica. Before
a publisher emits a new `(version, content)` key, every process consuming that
physical queue must therefore support it. The safe sequence is:

1. deploy consumers containing both old and new handlers;
2. wait until all old-only replicas have stopped;
3. enable publication of the new key;
4. remove the old handler only after old publication and queued old messages
   have ended.

V1-only and V2-only binaries must not share one queue as a way to distribute
work. Give intentionally incompatible binaries different physical queues and
routing bindings.

## Policy semantics

- `concurrency`: exact maximum number of simultaneously running handlers.
- `prefetch_count`: RabbitMQ QoS/unacked delivery limit.
- `delivery_buffer_capacity`: bounded local queue; omitted means prefetch.
- `retry_attempts`: retries after the first attempt; omitted means zero.
- `retry_jitter_ratio`: optional deterministic queue-bucket jitter. It defaults
  to `0.0` and accepts only finite values in `0.0..=0.5`; Lily never applies a
  per-message TTL or handler sleep.
- `delivery_execution_timeout_millis`: one aggregate middleware, guard,
  extraction, handler, reverse-unwind and delivery-scope cleanup budget.
  `DeliveryDeadline` exposes the current bounded stage cutoff. Lily reserves
  the smaller of one quarter of the remaining aggregate budget or one second
  for mandatory DI cleanup; a plan containing middleware reserves a second
  bounded tail inside the application budget for reverse unwind.
- `settlement_timeout_millis`: independent bound for one original-delivery ACK
  or NACK operation; it is not added to the execution budget.
- `max_message_size_bytes`: checked before body clone or decode.
- `retention`: mandatory main, per-retry-bucket and dead-letter message/byte
  limits. The framework uses `reject-publish`, never silent head-drop.
- ACK is always framework-controlled manual ACK.
- Batch delivery is not part of the Lily queue contract.

Retry delays are exact exponential fixed-delay buckets with a configured cap.
The runtime does not use per-message TTL or hidden jitter. A bucket has one
queue-level TTL, so a short retry cannot wait behind a longer retry in the same
queue. When a delay reaches the cap, subsequent attempts reuse that bucket.

Receiver and dispatcher tasks are supervised. A framework task exit/panic or
failed ACK/handoff settlement fails the complete Consumer runtime with queue
and task-role evidence. Broker connection loss still uses the configured
recovery loop.

During forced shutdown, an active delivery abandoned before settlement starts
is recorded as pending broker redelivery. If ACK/NACK/handoff I/O has already
started, Lily reports `Unresolved` instead of claiming a broker result it
cannot prove.

Starting `lily_consumer::Consumer` without `[rabbitmq.consumer]` is a startup
error. An application which does not need queue consumption should not start
the Consumer composition root.

## Topology v2 migration

Retry and dead-letter names are versioned as `v2`. Lily does not delete, drain,
or move `v1` queues. Existing main queues must also have declaration arguments
equivalent to the configured retention contract. Drain and remove incompatible
queues under an operator-controlled maintenance procedure before rollout; an
argument mismatch fails startup instead of silently changing broker data.
