# Lily Error

`lily_error` contains the typed error contracts shared between Lily framework
crates. Most applications should import an error from the crate that owns the
operation instead of depending on this crate directly.

## Canonical imports

| Operation | Canonical public type |
| --- | --- |
| HTTP handlers, guards, middleware, and extractors | `lily_http_api::HttpApiError` |
| HTTP client calls | `lily_http_client::HttpClientError` |
| Dependency injection and lifecycle | `lily_injection::InjectionError` |
| Configuration | `lily_config::ConfigError` |
| MongoDB access | `lily_mongodb::MongoDbError` |
| CRUD service helpers | `lily_base_service::BaseServiceError` |
| Consumer lifecycle | `lily_consumer::ConsumerError` |
| Queue handler return value | `lily_error::application::service::ServiceError` |
| RabbitMQ runtime extension code | `lily_error::application::MessageBrokerError` |

A direct `lily_error` dependency is appropriate when a queue handler returns
`ServiceError`, or when framework extension code implements a trait whose
signature explicitly names another shared contract.

## HTTP error boundary

`HttpApiError` stores an internal diagnostic string so logs and traces can
describe the cause. That string is not a public response body. Use
`HttpApiError::public_body` or `HttpApiError::to_public_json` at the HTTP
boundary; both expose a stable code and a generic, secret-safe message.

```rust
use lily_http_api::HttpApiError;

fn reject() -> HttpApiError {
    HttpApiError::Unauthorized("token signature verification failed".into())
}

let error = reject();
assert_eq!(error.http_status().0, 401);
assert_eq!(error.public_body().code, "UNAUTHORIZED");
```

Localization catalogs are immutable application-owned snapshots. Configure
them through the HTTP application builder; `lily_error` does not install a
process-global catalog.

## Ownership rules

- Transport-specific errors remain owned by their transport crate. For
  example, `lily_http_client::HttpClientError` converts directly into
  `HttpApiError`; there is no second HTTP-client error enum in this crate.
- Non-HTTP errors do not choose HTTP status codes or serialize response JSON.
  The HTTP boundary performs that policy decision.
- Error payload strings are diagnostics. Do not place credentials, tokens, or
  unredacted configuration values in them.
