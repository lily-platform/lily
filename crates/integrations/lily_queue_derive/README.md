# Lily Queue Derive

Implementation crate for
`lily_queue::{queue_service, queue, middleware, guard}`. Application code
should use the macros through `lily_queue`, not depend on this package
directly.

`#[queue_service]` accepts a non-generic inherent impl. A handler must:

- use exactly one `#[queue("name", version = N, content = "...",
  delivery_guarantee = "...")]` marker, where `N` is a concrete positive
  `u16` literal in `1..=65535` and `delivery_guarantee` is optional;
- be `async fn` with `&self` and at most sixteen owned typed extractors;
- place its optional, sole payload extractor after every parts extractor;
- return `Result<(), E>` where `E` converts into `QueueHandlerError`;
- produce a `Send` future.

The impl owner must also be registered in the Lily DI container (normally with
`#[derive(Injectable)]`, `#[service(...)]` and the required `ServiceTrait`
implementation). The macro emits metadata; it does not instantiate the owner
or register directly with RabbitMQ. `lily_consumer` selects the configured
metadata and rejects a missing owner registration before broker admission.

`#[queue_service]` is the outermost implementation attribute. Repeatable
`#[middleware(Type)]` and `#[guard(Type)]` sibling markers below it apply to
the service in source order. The same single-type markers beside one
`#[queue(...)]` method apply only to that handler. The outer macro consumes
all four markers and emits one immutable metadata authority; standalone marker
use is rejected.

Omitting `delivery_guarantee` is exactly equivalent to
`delivery_guarantee = "at_least_once"`. The only other accepted value is
`"transactional_inbox"`. That value records storage-neutral intent in handler
metadata; it does not select a database, create schema, or weaken startup
validation. A consumer with no compatible, configured transactional backend
must reject the handler before broker admission.

The annotation supplies inbox claim and deduplication; it does not implicitly
route arbitrary repository or pool operations through PostgreSQL's active
transaction. A handler puts the `PostgresTransaction` parts extractor before
its optional payload extractor and performs every covered business mutation or
outbox insertion through that value:

```rust,ignore
#[queue(
    "orders.created",
    version = 1,
    content = "json",
    delivery_guarantee = "transactional_inbox",
)]
async fn created(
    &self,
    transaction: lily_queue::PostgresTransaction,
    lily_queue::Json(event): lily_queue::Json<OrderCreated>,
) -> Result<(), lily_queue::QueueHandlerError> {
    transaction
        .with_connection(move |connection| Box::pin(async move {
            persist_order(connection, event).await
        }))
        .await?;
    Ok(())
}
```

A separately acquired pool connection, including one hidden behind an injected
repository, is outside that atomic boundary. Extracting `PostgresTransaction`
from an at-least-once handler is invalid and fails with the stable
`QUEUE_POSTGRES_TRANSACTION_CONTEXT_UNAVAILABLE` rejection.

The macro emits a type-erased typed-executor adapter plus one exact
`(queue, schema version, content kind)` dispatch contract and its input
metadata. Multiple handlers may declare distinct contracts for the same
physical queue. The consumer builds the immutable local dispatch table and
rejects duplicate exact contracts before broker admission. Runtime queue
policy remains exclusively in `[[rabbitmq.topology.queues]]`. Generated
support paths are routed through `lily_queue::__private`; applications do not
need to understand or enumerate the registry.

For transactional handlers, the generated
`module_path::ServiceType::method` name is part of the durable inbox identity.
Moving or renaming the module, service or method starts a new deduplication
namespace. Treat such a refactor, and reducing the configured inbox retention
horizon, as a data-compatibility change.
