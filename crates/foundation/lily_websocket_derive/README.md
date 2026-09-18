# lily_websocket_derive

This proc-macro package implements Lily's struct-based server controller macros
and its outbound CRUD gateway derive. Server applications should import the
controller macros from the `lily_websocket` facade rather than depending on
this implementation crate directly.

## Server controllers

```toml
[dependencies]
lily_websocket = "0.1.0"
serde = { version = "1", features = ["derive"] }
```

```rust
use std::sync::Arc;

use lily_websocket::{
    Ack, Extensions, Payload, Service, WebSocketActionError, WebSocketContext,
    WebSocketController, WebSocketControllerInitError, WebSocketControllerTrait,
    WebSocketLifecycleError, async_trait, websocket_controller,
};

#[derive(serde::Deserialize)]
struct SendMessage;
#[derive(serde::Serialize)]
struct SendReceipt;
trait AuditService: Send + Sync {}

#[derive(WebSocketController)]
#[namespace("chat")]
struct ChatController;

#[async_trait]
impl WebSocketControllerTrait for ChatController {
    async fn new(
        _extensions: Arc<Extensions>,
    ) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

#[websocket_controller]
impl ChatController {
    #[connected]
    async fn connected(
        &self,
        _context: WebSocketContext,
    ) -> Result<(), WebSocketLifecycleError> {
        Ok(())
    }

    #[message("send")]
    async fn send(
        &self,
        _context: WebSocketContext,
        Payload(_input): Payload<SendMessage>,
        _audit: Service<dyn AuditService>,
    ) -> Result<Ack<SendReceipt>, WebSocketActionError> {
        Ok(Ack::new(SendReceipt))
    }

    #[disconnected]
    async fn disconnected(
        &self,
        _context: WebSocketContext,
    ) -> Result<(), WebSocketLifecycleError> {
        Ok(())
    }
}
```

Register an implementation of `AuditService` in the application's DI container
before serving this action. The example defines its extraction contract only.

The derive registers immutable controller metadata. `WsAppBuilder::build`
constructs each registered controller once from the app DI container and binds
all of its operations to that same `Arc<Controller>`. Registration never
stores a live controller in process-global state.

The namespace is required, exact, and limited to 128 ASCII bytes containing
letters, digits, `-`, `_`, or `.`. A controller may declare at most one
`#[connected]` and one `#[disconnected]` hook. Message event names obey the
same character rules, may contain at most 256 bytes, and must be unique inside
the impl. App materialization additionally enforces the 256-byte combined
`namespace:event` wire-route bound.

Controller metadata accepts `#[handshake_middleware(...)]`,
`#[connection_middleware(...)]`, `#[message_middleware(...)]`, `#[guard(...)]`,
`#[frame_codec(Type)]`, `#[payload_codec(Type)]`, `#[timeout(seconds = N)]`,
and `#[asyncapi(...)]`. Message operations accept message middleware, guard,
payload-codec, timeout, and AsyncAPI metadata. Frame codecs are selected at
the controller boundary and therefore cannot be action metadata. Lifecycle
hooks cannot carry either a payload codec, message guards, or message
middleware. Every scalar codec authority is optional and may be declared only
once. Guards and codecs use real fallible app-build constructors.

Every handshake, connection, and message middleware type must respectively
implement `WebSocketHandshakeMiddleware`, `WsConnectionMiddleware`, and
`WsMessageMiddleware`; every guard type must implement `WsGuard`. The macros
emit typed registrations rather than ready instances. `WsAppBuilder::build`
constructs each concrete type once through its async `new(Arc<Extensions>)`
contract and composes immutable effective plans. Handshake and connection
middleware are controller-level authorities and cannot be attached to a
message operation. Message middleware and guards compose as global,
controller, then action. Repeating the same concrete `TypeId` inside one
effective plan is a build error rather than an implicit deduplication rule.

Message action parameters are owned extractors and are limited to sixteen per
operation. Lily resolves their body-free/payload roles through runtime traits,
not type-name matching inside the proc macro. This preserves custom extractor
support and permits signatures such as `Payload<T>` followed by `Service<S>`;
all body-free extraction still completes before the single payload authority.
Two payload consumers, payload extraction in lifecycle hooks, unsupported
references, and raw request/invocation access fail at compile time.

Message actions return `Result<O, WebSocketActionError>`, where `O` is an
explicit `Ack<T>`, `Emit<T>`, `NoReply`, or `CloseConnection` outcome. Bare
`()` and `Result<(), _>` do not select an implicit protocol behavior.
`#[connected]` and `#[disconnected]` use their payload-free lifecycle
extractor plan and return `Result<(), WebSocketLifecycleError>`.

## Outbound gateway derive

`BaseGateway` derives an outbound CRUD notification gateway backed by the
canonical `lily_websocket_client::TokioWsClient`. It is independent from the
server controller surface.

### Installation

```toml
[dependencies]
async-trait = "0.1"
lily_error = "0.1.0"
lily_injection = "0.1.0"
lily_websocket_client = "0.1.0"
lily_websocket_derive = "0.1.0"
serde = { version = "1", features = ["derive"] }
tokio-util = "0.7"
```

### Usage

```rust,no_run
use lily_websocket_client::TokioWsClient;
use lily_websocket_derive::BaseGateway;
use serde::Serialize;

#[derive(Serialize)]
struct ApplicationDto {
    id: String,
    name: String,
}

#[derive(BaseGateway)]
#[gateway(url = "wss://events.example.com/socket", namespace = "application")]
#[dto_type(ApplicationDto)]
struct ApplicationGateway {
    #[gateway_client]
    client: TokioWsClient,
    entity_name: String,
}

let gateway = ApplicationGateway::new();
assert_eq!(gateway.entity_name, "application");
```

The macro generates:

- `new` and `Default`;
- `notify_created`, `notify_created_many`, `notify_updated`, `notify_deleted`, `notify_deleted_id`, and `notify_deleted_many`;
- `client`, `is_connected`, and `disconnect`;
- application-owned DI `ServiceTrait` initialization/disposal.

Notification names are `{entity_name}:created`, `createdMany`, `updated`, `deleted`, and `deletedMany`. Every send uses the client's real bounded wire path and returns `WebSocketError`; success is not fabricated when the outbound queue or socket fails.

The URL must be a literal absolute `ws://` or `wss://` URL. `#[dto_type(...)]` and exactly one `#[gateway_client]` field are required. Compile-pass and compile-fail behavior is covered by persistent `trybuild` fixtures.

The derived struct currently has an exact two-field contract: one named
`#[gateway_client]` field and one `entity_name: String` field. Extra fields are
rejected because `new()` owns complete construction. The namespace is a
non-empty bounded ASCII route token and must match a `namespace` query value if
the URL already contains one. Bulk notification methods accept slices rather
than requiring `Vec` ownership.

### Lifecycle

DI initialization registers lifecycle callbacks before connecting. Disposal awaits the client's bounded Close handshake and runtime shutdown. For manual construction, call `WsClient::connect` with your application cancellation token before sending and call `disconnect` during shutdown.

Public deployments normally terminate WSS at an edge and use HTTP/1.1 Upgrade to the Lily server. The client can also connect directly to public-CA `wss://` endpoints with Rustls/WebPKI certificate, hostname, and SNI validation. RFC 8441 is not implied.

## Documentation and license

Full documentation and canonical application examples: [lilyrs.com](https://lilyrs.com).
Published API reference: [docs.rs/lily_websocket_derive](https://docs.rs/lily_websocket_derive).

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
