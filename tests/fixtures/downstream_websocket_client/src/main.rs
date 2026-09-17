use lily_injection::ApplicationContainer;
use lily_websocket::guard::{GuardInitializationError, WebSocketGuardRejection, WsGuard};
use lily_websocket::middleware::{
    MiddlewareDescriptor, MiddlewareKind, WebSocketHandshakeMiddleware, WebSocketIdentity,
    WebSocketIdentityMiddleware, WsConnectionMiddleware, WsHandshakeExchange, WsHandshakeRejection,
    WsMessageDecision, WsMessageExchange, WsMessageMiddleware, WsMessageOutcome, WsMiddlewareError,
    WsMiddlewareInitError,
};
use lily_websocket::{
    async_trait, websocket_controller, Extensions, NoReply, Payload, ServerConfig, Service,
    WebSocketActionError, WebSocketController, WebSocketControllerInitError,
    WebSocketControllerTrait, WebSocketErrorCode, WebSocketMessageContext, WsAppBuilder,
};
use lily_websocket_client::{
    TokioWsClient, WebSocketClientConfig, WebSocketClientService, WebSocketError, WsClient,
    LILY_WEBSOCKET_SUBPROTOCOL,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

struct CompileGlobalHandshakeMiddleware;

#[async_trait]
impl WebSocketHandshakeMiddleware for CompileGlobalHandshakeMiddleware {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Ok(Self)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new(
            "fixture_global_handshake",
            MiddlewareKind::WebSocketHandshake,
        )
    }

    async fn handle(
        &self,
        _exchange: &mut WsHandshakeExchange,
        _cancellation: lily_websocket::ExecutionCancellation,
    ) -> Result<(), WsHandshakeRejection> {
        Ok(())
    }
}

struct CompileControllerHandshakeMiddleware;

#[async_trait]
impl WebSocketHandshakeMiddleware for CompileControllerHandshakeMiddleware {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Ok(Self)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new(
            "fixture_controller_handshake",
            MiddlewareKind::WebSocketHandshake,
        )
    }

    async fn handle(
        &self,
        exchange: &mut WsHandshakeExchange,
        _cancellation: lily_websocket::ExecutionCancellation,
    ) -> Result<(), WsHandshakeRejection> {
        let _ = exchange.request().namespace();
        Ok(())
    }
}

struct CompileIdentityMiddleware;

#[async_trait]
impl WebSocketIdentityMiddleware for CompileIdentityMiddleware {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Ok(Self)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("fixture_identity", MiddlewareKind::WebSocketHandshake)
    }

    async fn identify(
        &self,
        _exchange: &mut WsHandshakeExchange,
        _cancellation: lily_websocket::ExecutionCancellation,
    ) -> Result<WebSocketIdentity, WsHandshakeRejection> {
        Ok(WebSocketIdentity::anonymous())
    }
}

struct CompileGlobalConnectionMiddleware;

#[async_trait]
impl WsConnectionMiddleware for CompileGlobalConnectionMiddleware {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Ok(Self)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new(
            "fixture_global_connection",
            MiddlewareKind::WebSocketConnection,
        )
    }
}

struct CompileControllerConnectionMiddleware;

#[async_trait]
impl WsConnectionMiddleware for CompileControllerConnectionMiddleware {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Ok(Self)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new(
            "fixture_controller_connection",
            MiddlewareKind::WebSocketConnection,
        )
    }
}

struct CompileMessageMiddleware;

#[async_trait]
impl WsMessageMiddleware for CompileMessageMiddleware {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Ok(Self)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("fixture_message", MiddlewareKind::WebSocketMessage)
    }

    async fn after_message(
        &self,
        _exchange: &mut WsMessageExchange,
        _outcome: WsMessageOutcome,
        _cancellation: lily_websocket::ExecutionCancellation,
    ) -> Result<WsMessageDecision, WsMiddlewareError> {
        Ok(WsMessageDecision::Continue)
    }

    async fn on_message_termination(
        &self,
        context: lily_websocket::middleware::WsMessageTerminationContext<'_>,
        cancellation: lily_websocket::CleanupCancellation,
    ) -> Result<(), WsMiddlewareError> {
        let _ = (context.normal_exit(), context.reason(), context.deadline());
        let _same_authority: lily_websocket::CleanupCancellation = context.cancellation().clone();
        let _ = cancellation.is_cancelled();
        Ok(())
    }
}

struct CompileGuard;

#[async_trait]
impl WsGuard for CompileGuard {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, GuardInitializationError> {
        Ok(Self)
    }

    async fn can_activate(
        &self,
        exchange: &mut WsMessageExchange,
        _cancellation: lily_websocket::ExecutionCancellation,
    ) -> Result<(), WebSocketGuardRejection> {
        if exchange.request().event().is_empty() {
            let code = WebSocketErrorCode::new("FIXTURE_GUARD_DENIED")
                .expect("the fixture guard code is valid");
            return Err(
                WebSocketGuardRejection::error(code, "Fixture guard denied the message.")
                    .expect("the fixture guard rejection is valid"),
            );
        }

        Ok(())
    }
}

#[derive(WebSocketController)]
#[namespace("fixture")]
#[handshake_middleware(CompileControllerHandshakeMiddleware)]
#[connection_middleware(CompileControllerConnectionMiddleware)]
struct CompileController;

trait CompileDependency: Send + Sync {
    fn label(&self) -> &'static str;
}

#[async_trait]
impl WebSocketControllerTrait for CompileController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

#[websocket_controller]
impl CompileController {
    #[message("notification")]
    async fn notification(
        &self,
        _context: WebSocketMessageContext,
        Payload(_notification): Payload<Notification>,
        dependency: Service<dyn CompileDependency>,
    ) -> Result<NoReply, WebSocketActionError> {
        let _ = dependency.label();
        Ok(NoReply)
    }
}

#[derive(Deserialize, Serialize)]
struct Notification {
    message: String,
}

/// Type-check both the canonical WebSocket transport facade and its default `single`
/// DI service without connecting to a server or loading application config.
#[allow(dead_code)]
fn compile_websocket_application(
    container: &ApplicationContainer,
) -> Result<TokioWsClient, WebSocketError> {
    let resolve_service = container.resolve::<WebSocketClientService>(None);
    drop(resolve_service);

    let mut config = WebSocketClientConfig {
        url: "wss://ws.example.test/events".to_owned(),
        namespace: Some("notifications".to_owned()),
        ..WebSocketClientConfig::default()
    };
    config
        .subprotocols
        .push(LILY_WEBSOCKET_SUBPROTOCOL.to_owned());

    let client = TokioWsClient::with_config(config)?;
    client.on("notifications:notification", |_event, _payload| {});

    let send = client.send(
        "notifications:notification",
        Notification {
            message: "compile-only".to_owned(),
        },
    );
    drop(send);

    Ok(client)
}

/// Type-check the public WebSocket server composition path without polling the
/// build future, loading config, constructing DI state, or binding a socket.
#[allow(dead_code)]
fn compile_websocket_server() {
    let build = WsAppBuilder::new("127.0.0.1:8081")
        .config(ServerConfig {
            allowed_origins: vec!["https://fixture.example".to_owned()],
            ..ServerConfig::default()
        })
        .handshake_middleware::<CompileGlobalHandshakeMiddleware>()
        .identity_middleware::<CompileIdentityMiddleware>()
        .connection_middleware::<CompileGlobalConnectionMiddleware>()
        .message_middleware::<CompileMessageMiddleware>()
        .guard::<CompileGuard>()
        .build();
    drop(build);
}

fn main() {}
