# RabbitMQ Queue Client Modes

`lily_queue_client` supports one broker: RabbitMQ. The `single` and `factory`
features select composition style, not broker/provider type.

## Single mode

Single mode is the default and registers one injectable `QueueClientService`.

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

When the default `QueueClientService` is linked into the application,
`[queue_client]` is required and omission fails DI initialization with a typed
configuration error. Applications that do not publish messages should not
include `lily_queue_client`; there is no provider selector.

## Factory mode

Factory mode creates multiple named RabbitMQ clients. Build the crate with
`--no-default-features --features factory`.

```toml
[queue_client]
mode = "factory"

[[queue_client.cells]]
name = "commands"
connection_string = "amqps://commands:secret@rabbit.internal/%2f"
use_tls = true
pool_size = 5
connection_timeout_secs = 10
confirm_timeout_secs = 10
heartbeat_secs = 30
max_reconnect_attempts = 5
reconnect_backoff_millis = 250
persistence_enabled = true

[[queue_client.cells]]
name = "events"
connection_string = "amqps://events:secret@rabbit.internal/%2f"
use_tls = true
confirm_timeout_secs = 15
```

```rust,ignore
let factory = container.resolve::<QueueClientFactory>(None).await?;
let commands = factory.get("commands").ok_or("missing commands cell")?;
commands.publish("commands", "order.create", payload).await?;
```

Every cell is validated before broker I/O. Names must be unique and non-empty;
factory mode requires at least one cell. `confirm_timeout_secs` is the only
publish-confirm deadline authority.

Starting a cell only establishes its bounded connection pool and a
publisher-confirm channel. It never declares an exchange, queue, or binding.
Every publish therefore targets topology created through an explicit
bootstrap path or by deployment infrastructure.

For publisher-only topology ownership, the composition root can compile the
same immutable topology for an individual cell before publishing:

```rust,ignore
let bootstrap = RabbitMqTopologyBootstrap::from_cell(
    cell_config,
    &config.rabbitmq.topology,
)?;
let report = bootstrap.run(CancellationToken::new()).await?;
```

This is explicit and per cell. `QueueClientConfig` and its cells own publisher
connections only; the topology remains the single shared
`rabbitmq.topology` authority. Creating or starting `QueueClientFactory` never
performs topology bootstrap.

## Initialization safety

The injectable single service returns `BROKER_NOT_INITIALIZED` if accessed
before initialization. Factory-created services require a provider in their
constructor, so an uninitialized factory cell cannot be constructed.
