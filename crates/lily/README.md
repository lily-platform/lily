# Lily

A feature-selected facade over Lily's application frameworks and reusable components.
There are no default features. Enable only the components your package uses:

```toml
[dependencies]
lily = { version = "0.1", features = ["http-api", "postgresql", "trace"] }
```

Import through component modules, for example `lily::http_api::AppBuilder`,
`lily::postgresql::PgDbContext`, and `lily::trace::lily_trace`. The re-exports
preserve the component types; they do not create wrappers or another DI container.
Macros discover both `lily` and Cargo-renamed dependencies automatically. A direct
component dependency takes precedence if both forms are present.

## Components

| Feature | Import path | Component default |
| --- | --- | --- |
| `consumer` | `lily::consumer` | Queue handlers are also available at `lily::queue` |
| `http-api` | `lily::http_api` | HTTP types, controller macros and root DI API |
| `websocket` | `lily::websocket` | WebSocket types, controller macros and root DI API |
| `clickhouse` | `lily::clickhouse` | `single` |
| `mongodb` | `lily::mongodb` | `single` |
| `postgresql` | `lily::postgresql` | `single` |
| `queue` | `lily::queue` | No additional feature |
| `queue-client` | `lily::queue_client` | `single` |
| `redis` | `lily::redis` | `single` |
| `trace` | `lily::trace` | `console` |
| `config` | `lily::config` | No additional feature |
| `websocket-client` | `lily::websocket_client` | `single` |
| `injection` | `lily::injection` | DI types and `Injectable` |
| `http-client` | `lily::http_client` | No additional feature |
| `error` | `lily::error` | No additional feature |
| `background-service` | `lily::background_service` | No additional feature |

Framework users can keep imports such as `lily::http_api::{Injectable, ServiceTrait}`.
A service-only package enables `injection` and imports `lily::injection` instead.
There is no separate `di` namespace. `config` does not re-export DI.

## Feature forwarding

Child features use a component prefix, for example `consumer-asyncapi` and
`queue-asyncapi`. Enabling a child feature also exposes its component module.
The complete public forwarding surface is:

- **consumer**: `consumer-asyncapi`, `consumer-fuzzing`, `consumer-transactional-inbox-mongodb`, `consumer-transactional-inbox-mongodb-factory`, `consumer-transactional-inbox-postgresql`, `consumer-transactional-inbox-postgresql-factory`.
- **http-api**: `http-api-fuzzing`.
- **websocket**: `websocket-fuzzing`.
- **clickhouse**: `clickhouse-single`, `clickhouse-factory`.
- **mongodb**: `mongodb-single`, `mongodb-factory-api`, `mongodb-factory`, `mongodb-test-support`.
- **postgresql**: `postgresql-factory-api`, `postgresql-single`, `postgresql-factory`, `postgresql-test-support`.
- **queue**: `queue-asyncapi`, `queue-test-support`, `queue-fuzzing`, `queue-transactional-inbox-mongodb`, `queue-transactional-inbox-mongodb-factory`, `queue-transactional-inbox-postgresql`, `queue-transactional-inbox-postgresql-factory`.
- **queue-client**: `queue-client-single`, `queue-client-factory`.
- **redis**: `redis-single`, `redis-factory`.
- **trace**: `trace-console`.
- **config**: `config-transactional-inbox-mongodb`, `config-transactional-inbox-postgresql`.
- **websocket-client**: `websocket-client-single`, `websocket-client-factory`, `websocket-client-di`.
- **error**: `error-mongodb-driver`.

`trace-console` forwards the existing console feature. OTLP and file exporters
are configured through the trace runtime; they do not have separate Cargo features.
`fuzzing` and `test-support` features expose the components' existing test APIs.
Features beginning with `__` only coordinate dependency activation and macro
expansion; they are implementation details, not application configuration.

### Singleton and factory modes

`mongodb`, `postgresql`, `clickhouse`, `redis`, `queue-client`, and
`websocket-client` enable their component's default `single` mode. Select
`<component>-factory` **instead of** the base feature to choose factory mode:

```toml
lily = { version = "0.1", features = ["http-api", "postgresql-factory", "redis-factory"] }
```

Features are additive across the entire dependency graph. Combining a base/single
feature with the same component's factory feature is an error, including when a
second package enables that mode. `--all-features` is therefore not a valid build.
`mongodb-factory-api`, `postgresql-factory-api`, and `websocket-client-di` forward
lower-level capabilities without implicitly registering either mode.

### Transactional inbox

`consumer-transactional-inbox-postgresql` selects Consumer, its queue adapter and
PostgreSQL singleton registration. Use the `-factory` suffix for factory
registration. The MongoDB variants follow the same rules. `consumer-asyncapi`
also enables Queue's AsyncAPI support.

Queue-only transactional inbox features preserve Queue's lower-level contract:
they expose the database module and adapter without selecting DI registration.
Their `-factory` variants expose the database factory API only. Select a database
mode explicitly if your application needs DI registration. Configuration-only
features likewise do not activate database adapters.

## Macro ownership

MongoDB exports `MongoCollection`, `Repository` and `CrudService`; PostgreSQL
exports `PgRepository`; Trace exports `lily_trace`; Queue exports its handler
macros and registry support. The umbrella uses those component facades without
requiring separate Lily derive, registry or helper dependencies. Application
libraries such as Serde and Diesel remain normal direct dependencies when their
own macros require them.

## Validation and examples

`tests/fixtures/umbrella_facades` contains isolated downstream contracts for this
facade, including renamed dependencies, framework-only builds, singleton/factory
modes and negative compile checks. Run its documented validation command from
the repository. Runnable HTTP, WebSocket and Consumer examples are a subsequent
stage; ClickHouse is tested as a facade contract only.

## Runnable examples

The [connected examples](../../examples/README.md) use only the `lily` facade
as their direct Lily dependency. They include shared services, HTTP/WebSocket/Consumer
hosts, real clients, Docker Compose and end-to-end verification.
