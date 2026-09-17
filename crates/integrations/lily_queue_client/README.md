# lily_queue_client

`lily_queue_client` is Lily's RabbitMQ-only producer. It provides validated
`amqp`/`amqps` configuration, bounded application-owned connection pools,
publisher confirms, mandatory Return handling, W3C trace propagation, and a
versioned event envelope.

Starting a publisher or opening a channel never creates, mutates, or verifies
an exchange, queue, or binding. `publish(exchange, routing_key, ...)` targets
topology that already exists; a mandatory unroutable publish is returned as a
typed error. Broker topology must be supplied by the selected explicit
topology bootstrap path or by deployment infrastructure.

Connection startup is cancellation-safe: active, partially opened, and
retired pool generations remain manager-owned until asynchronous close has
completed successfully. If a startup or shutdown future is cancelled, a later
owned cleanup attempt can still reach every retained connection. Failed close
attempts retain every non-terminal handle, including Lapin `Closing` and
`Reconnecting`; only observed `Closed` or `Error` state releases ownership. No
low-level pool handle is exposed to application code.

## Choose exactly one composition mode

- `single` is the default. It registers one injectable `QueueClientService`.
- `factory` registers one `QueueClientFactory` containing multiple named
  RabbitMQ clients.

These Cargo features are mutually exclusive. “Factory” means multiple
RabbitMQ connections, not multiple broker providers:

```toml
# Default single mode
lily_queue_client = "0.1.0"

# Factory mode
lily_queue_client = { version = "0.1.0", default-features = false, features = ["factory"] }
```

## Single mode

Configure the publisher in `lily.toml`:

```toml
[queue_client]
mode = "single"
connection_string = "amqps://publisher:secret@rabbit.internal/%2f"
use_tls = true
pool_size = 5
connection_timeout_secs = 10
confirm_timeout_secs = 10
heartbeat_secs = 30
max_reconnect_attempts = 5
reconnect_backoff_millis = 250
persistence_enabled = true
```

`QueueClientService` is registered by its `Injectable` derive and is normally
injected into an application service:

```rust,ignore
use std::sync::Arc;
use async_trait::async_trait;
use lily_error::injection::InjectionError;
use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;
use lily_queue_client::QueueClientService;

#[derive(Injectable, Default)]
#[service(lifetime = "Singleton")]
pub struct ApplicationService {
    #[inject]
    queue_client: Arc<QueueClientService>,
}

#[async_trait]
impl ServiceTrait for ApplicationService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        Ok(())
    }
}
```

- `publish` serializes JSON and creates a fresh V1 event ID.
- `publish_raw` publishes binary content and creates a fresh V1 event ID.
- `publish_with_event_id` is the V1 JSON compatibility path for a
  caller-owned identity.
- `publish_json`, `publish_text`, and `publish_binary` require a positive
  `PublishSchemaVersion` and create a fresh event ID.
- Their `_with_metadata` variants accept validated `PublishMetadata`; an
  outbox persists and reuses its event ID and schema version on every retry.
- `publish_custom` and `publish_custom_with_metadata` accept bytes together
  with one bounded `CustomPublishContent` token/MIME pair. Built-in `json`,
  `text`, and `binary` names cannot be shadowed through this escape hatch.
- `publish_terminal_snapshot` returns bounded, sampling-independent outcome
  counters without destinations, event IDs, payloads, or credentials.

Calling these methods before successful DI initialization returns the typed
`BROKER_NOT_INITIALIZED` error.

Built-in methods make the wire representation non-ambiguous:

| Method | `x-lily-content-kind` | AMQP `content_type` |
| --- | --- | --- |
| `publish_json*` | `json` | `application/json` |
| `publish_text*` | `text` | `text/plain; charset=utf-8` |
| `publish_binary*` | `binary` | `application/octet-stream` |

```rust,ignore
use lily_queue_client::{PublishMetadata, PublishSchemaVersion};

let version = PublishSchemaVersion::try_new(3)?;
publisher
    .publish_json("orders", "orders.created", version, &event)
    .await?;

// An outbox stores both values and reuses them for every handoff attempt.
let metadata = PublishMetadata::try_new(persisted_event_id, version)?;
publisher
    .publish_json_with_metadata(
        "orders",
        "orders.created",
        metadata,
        &persisted_event,
    )
    .await?;
```

## Explicit topology bootstrap

Publisher initialization never creates broker resources. A publisher-only
process that owns `FrameworkManaged` topology must opt in at its composition
root before it starts accepting work:

```rust,ignore
use lily_queue_client::RabbitMqTopologyBootstrap;
use tokio_util::sync::CancellationToken;

let config = config_service.get_lily_config().await;
let publisher = config.queue_client.as_ref().ok_or("missing queue_client")?;
let bootstrap = RabbitMqTopologyBootstrap::from_client(
    publisher,
    &config.rabbitmq.topology,
)?;
let report = bootstrap.run(CancellationToken::new()).await?;
```

The accepted topology comes from the shared `[[rabbitmq.topology.queues]]`
section. `QueueClientConfig` owns publisher connection/factory settings only;
it does not contain or duplicate topology. Managed resources are declared
exactly in exchange, queue, then binding order.
`External` resources are never created or bound: exchanges and queues are
checked with passive declarations, while
`report.external_bindings_unverified()` records the bindings that remain the
deployment operator's responsibility. Passive verification proves resource
existence, not queue arguments or binding identity.

The bootstrap uses one dedicated connection and closes it before returning.
Its channel creation and complete declare/verify/bind sequence share the
validated `connection_timeout_secs` deadline; cancellation remains immediately
observable and cleanup uses the same bounded timeout. Because an exclusive
queue belongs to its declaring connection, Lily's pooled bootstrap and runtime
connection lifecycle does not support `exclusive = true`; the canonical plan
rejects it before any broker I/O for both ownership modes.

Calling `publish` later only publishes to the requested existing exchange and
routing key; it cannot silently repair or mutate topology.

## Factory mode

Factory mode uses `[[queue_client.cells]]` entries. Every cell is validated
before broker I/O and the completed set becomes visible atomically:

```toml
[queue_client]
mode = "factory"

[[queue_client.cells]]
name = "commands"
connection_string = "amqps://commands:secret@rabbit.internal/%2f"
use_tls = true
confirm_timeout_secs = 10

[[queue_client.cells]]
name = "events"
connection_string = "amqps://events:secret@rabbit.internal/%2f"
use_tls = true
confirm_timeout_secs = 15
```

```rust,ignore
let factory = container.resolve::<QueueClientFactory>(None).await?;
let commands = factory.get("commands").ok_or("missing commands cell")?;
commands
    .publish("commands", "order.create", &command)
    .await?;
```

## Delivery contract

A publish succeeds only when RabbitMQ sends an ACK and does not return the
mandatory message as unroutable. NACK, Return, timeout, cancellation, and
transport failure are errors. This is an at-least-once handoff contract, not
automatic exactly-once processing.

Applications use the service/factory API. Low-level connection managers,
transport traits, envelopes, and RabbitMQ engine structs are hidden
cross-crate ABI used by `lily_queue` and qualification fixtures.

Broker-independent regression tests run locally. The ignored
`tests/live_rabbitmq.rs` suite requires a disposable broker selected with
`LILY_TEST_RABBITMQ_URL`.
