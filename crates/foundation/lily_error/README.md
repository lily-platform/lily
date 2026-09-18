# lily_error

Shared typed error contracts for Lilyrs configuration, DI, application and
transport boundaries. Applications normally use the error exported by the
component that owns the operation. A direct dependency is useful for libraries
that implement those shared contracts without a framework host.

```toml
[dependencies]
lily_error = "0.1.0"
```

The facade alternative is `lilyrs` with feature `error`, imported through
`lilyrs::error`. There are no default features. `mongodb-driver` adds conversions
from MongoDB driver errors; the MongoDB integration enables it when needed.

## Canonical imports

| Operation | Public type |
| --- | --- |
| Built-in HTTP application errors | `lily_http_api::HttpApiError` |
| Failure to construct an HTTP response | `lily_http_api::ResponseWriteError` |
| HTTP client calls | `lily_http_client::HttpClientError` |
| Dependency injection and lifecycle | `lily_injection::InjectionError` |
| Configuration | `lily_config::ConfigError` |
| MongoDB access | `lily_mongodb::MongoDbError` |
| MongoDB CRUD service helpers | `lily_mongodb::BaseServiceError` |
| PostgreSQL access | `lily_postgresql::PgError` |
| Consumer lifecycle | `lily_consumer::ConsumerError` |
| Queue handler return value | `lily_queue::QueueHandlerError` |
| RabbitMQ runtime extension code | `lily_error::application::MessageBrokerError` |

This table lists operation owners; not every type is defined in `lily_error`.
Queue handlers can return their own error with an explicit
`Into<QueueHandlerError>` conversion. The application chooses retryable or
permanent failure; Lily does not infer it from a generic `ServiceError`.

## HTTP error boundary

`HttpApiError` is a provided application error, not a required base type.
An HTTP application can implement `IntoResponse` on its own error and return
`Result<T, ApplicationError>`. Successfully rendering that error returns
`Ok(ResponseWriteOutcome)`; failure to build its response returns
`Err(ResponseWriteError)`. Application failure metadata and response-writing
failure have distinct responsibilities.

`HttpApiError` stores an internal diagnostic string. It is not a public response
body. `public_body()` and `to_public_json()` expose a stable code and a generic,
secret-safe message:

```rust
use lily_error::application::http_api::HttpApiError;

let error = HttpApiError::Unauthorized("token signature verification failed".into());
assert_eq!(error.http_status().0, 401);
assert_eq!(error.public_body().code, "UNAUTHORIZED");
```

Localization catalogs are immutable application-owned snapshots configured
through the HTTP builder. This crate does not install a process-global catalog.

## Ownership rules

- Transport-specific errors stay with their transport crate. For example,
  `lily_http_client::HttpClientError` converts into `HttpApiError`; it is not
  duplicated here.
- Non-HTTP operation errors do not choose HTTP status or response JSON. That
  policy belongs to the application's HTTP boundary.
- Diagnostic strings must not contain credentials, tokens or unredacted
  configuration values.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_error](https://docs.rs/lily_error).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
