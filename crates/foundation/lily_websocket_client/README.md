# Lily WebSocket Client

`lily_websocket_client` is Lily's bounded outbound WebSocket client. One
supervised task owns the socket, application writes use a bounded queue, and a
successful send means the socket writer accepted and flushed the frame.

## Choose one ownership mode

- `default-features = false`: application-owned `TokioWsClient` only.
- default `single` feature: one DI-managed `WebSocketClientService` configured
  through `lily.toml`.
- `default-features = false, features = ["factory"]`: one DI-managed
  `WebSocketClientFactory` containing named client cells.

`single` and `factory` are mutually exclusive. A direct client remains
available in every mode.

## Direct client

```toml
[dependencies]
lily_websocket_client = { version = "0.1.0", default-features = false }
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
tokio-util = "0.7"
```

```rust,no_run
use lily_websocket_client::{TokioWsClient, WebSocketClientConfig, WsClient};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = TokioWsClient::with_config(WebSocketClientConfig {
        url: "wss://events.example.com/ws".into(),
        namespace: Some("chat".into()),
        ..WebSocketClientConfig::default()
    })?;

    client.on_async("chat:received", |event, payload| async move {
        println!("{event}: {} bytes", payload.len());
    });

    let application_cancel = CancellationToken::new();
    client.connect(application_cancel.clone()).await?;
    client
        .send("chat:send", serde_json::json!({ "text": "hello" }))
        .await?;
    let _reply = client
        .request(
            "chat:save",
            serde_json::json!({ "text": "persist me" }),
            Duration::from_secs(2),
        )
        .await?;
    client.disconnect().await?;
    Ok(())
}
```

`send` creates a strict Lily v2 JSON event envelope. `send_text_event` and
`send_binary_event` create v2 text and binary payload envelopes. `send_text` and `send_binary`
send raw application frames through the same queue and limits. They do not
change inbound handling: incoming application frames are still expected to be
canonical Lily v2 envelopes.

`on` is for short synchronous work. `on_async` is canonical for I/O because
callback queue capacity, concurrency, timeout, panic containment, and task join
outcomes remain under client control. Registering the same event twice replaces
the earlier listener. `*` is the wildcard listener and `off` removes a listener.

Inbound Lily v2 `event` envelopes enter their exact event callback. An
uncorrelated server `error` enters the reserved `on_error` callback and does not
masquerade as the envelope's action event. `request` allocates one fresh
`ack_id` and returns exactly `WebSocketReply::Acknowledgement` for the matching
`ack`, or `WebSocketReply::Rejection` for the matching correlated `error`.
Pending waiters are bounded by `outbound_queue_capacity` and are removed on
success, rejection, timeout, caller cancellation, or disconnect. Lily never
automatically retries a request. An unknown or late `ack`/correlated `error` is
a protocol failure and closes the socket rather than entering an ordinary
callback.

## DI single mode

The default feature registers `WebSocketClientService` as a singleton. Resolve
it from `ApplicationContainer`; do not construct its generated `Default` value.
The service is published only after its configured socket connects.

```toml
[websocket_client]
mode = "single"
url = "wss://events.example.com/ws"
namespace = "chat"
subprotocols = ["lily.v2"]
require_subprotocol = true
authorization_bearer_file = "/run/secrets/websocket-token"
```

```rust,ignore
let client = container
    .resolve::<WebSocketClientService>(None)
    .await?;
client.send("chat:status", serde_json::Value::Null).await?;
let reply = client
    .request(
        "chat:save",
        serde_json::json!({ "status": "ready" }),
        std::time::Duration::from_secs(2),
    )
    .await?;
client.send_text_event("chat:typing", "active".into()).await?;
client.send_binary_event("files:chunk", vec![0, 1, 2, 255]).await?;
```

Initialization completes the first connection before callers can register
callbacks. Register `on_connect` on the underlying direct client before
`connect` when the initial connection notification itself must be observed.
Listeners registered through the DI service observe later events and reconnects.

## DI factory mode

Build with `default-features = false, features = ["factory"]` and configure
named cells:

```toml
[websocket_client]
mode = "factory"

[[websocket_client.cells]]
name = "primary"
url = "wss://primary.example.com/ws"
namespace = "events"

[[websocket_client.cells]]
name = "backup"
url = "wss://backup.example.com/ws"
namespace = "events"
```

Resolve `WebSocketClientFactory`, then call `get("primary")`. Initialization is
atomic from the application's perspective: duplicate/empty names fail before
I/O, and a later connection failure closes cells opened earlier.

## Authentication and TLS

- `AuthHeaderProvider` runs before the initial handshake and every reconnect.
- `StaticAuthHeaders` supports externally managed fixed credentials.
- `BearerTokenFile` re-reads a bounded token file on every handshake, supporting
  atomic secret rotation.
- Credentials in URL user-info or sensitive query parameters are rejected.
- `wss://` uses Rustls, public WebPKI roots, hostname validation, and URL-host
  SNI. `additional_ca_bundle` may append public CA certificates but cannot
  disable verification or contain private keys.

## Lifecycle and delivery

- Initial handshake, reconnect backoff, Ping/Pong, idle detection, send
  acknowledgement, Close handshake, and total shutdown have separate budgets.
- Failed writes are not replayed automatically, avoiding ambiguous duplicate
  delivery.
- `disconnect` is idempotent and joins the supervised runtime.
- `WebSocketClientShutdownHandle` adapts an application-owned direct client to
  Lily's framework shutdown coordinator.
- `metrics_snapshot` exposes bounded counters without URLs, credentials, event
  names, or payloads.

Custom client certificates, CONNECT/SOCKS proxies, insecure certificate
verification, and RFC 8441 are not supported.

## Verification

```bash
cargo run -p lily_websocket_client --example direct_client --no-default-features
cargo run -p lily_websocket_client --example basic_client
cargo run -p lily_websocket_client --example factory_example \
  --no-default-features --features factory
cargo test -p lily_websocket_client
```

License: MIT OR Apache-2.0.
