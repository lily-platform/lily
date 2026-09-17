# Lily middleware contracts

`lily_middleware` contains the typed policy and middleware contracts used by
Lily's HTTP and WebSocket runtimes. It is framework infrastructure rather than
a standalone application dependency.

HTTP applications should depend on `lily_http_api` and import the re-exported
types from there:

```rust,ignore
use lily_http_api::{
    CorsPolicy, CsrfPolicy, HttpMiddleware, HttpMiddlewareRejection,
};
```

WebSocket applications obtain the shared middleware descriptor and bounded
diagnostic types through `lily_websocket::middleware`. Applications should not
add a direct `lily_middleware` dependency for either transport.

The supported application-facing groups are:

- `HttpMiddleware`, `HttpExchange`, `HttpNext` and bounded middleware errors;
- `CorsPolicy`, `CorsPolicyProvider` and optional dynamic origin resolvers;
- `CsrfPolicy`, session bindings and application-provided token stores.

HTTP `handle` retains normal around semantics and read-only execution
cancellation. Its default no-op `on_request_termination` receives a separate
read-only cleanup view and a bounded `HttpRequestTerminationContext`. The HTTP
owner invokes it only for first-polled, not-normally-returned invocations in
reverse order. Per-invocation `HttpExchange::termination_state_mut()` storage
survives execution drop; it is independent of the request-local map. State must
not retain execution body readers/producers. A later body failure does not
reopen normally returned middleware. See the
[HTTP migration contract](../../framework/lily_http_api/SHUTDOWN_MIGRATION.md) for eligibility,
deadlines and the guarantees that remain bounded or best effort.

Important configuration bounds are part of the public contract: CORS policy
metadata is limited to 32 KiB and preflight `max_age` to 24 hours; CSRF secrets
use 32..=128 bytes, token TTL uses whole seconds from 1 second through 7 days,
and synchronizer-store operations can be bounded up to 30 seconds. Individual
builder methods document their exact count and byte limits.

The `__private` module is a cross-crate transport SPI used by
`lily_http_api`. Its types may change without becoming part of the application
contract. Applications must not construct Tower adapters or compiled CSRF
runtimes directly.
