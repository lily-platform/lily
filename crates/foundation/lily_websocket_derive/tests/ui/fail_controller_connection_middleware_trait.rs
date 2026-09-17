use std::sync::Arc;

use lily_websocket::middleware::{
    MiddlewareDescriptor, MiddlewareKind, WebSocketHandshakeMiddleware, WsHandshakeExchange,
    WsHandshakeRejection, WsMiddlewareInitError,
};
use lily_websocket::{
    Extensions, WebSocketController, WebSocketControllerInitError, WebSocketControllerTrait,
    async_trait,
};

struct NotConnectionMiddleware;

#[async_trait]
impl WebSocketHandshakeMiddleware for NotConnectionMiddleware {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Ok(Self)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("wrong_handshake_trait", MiddlewareKind::WebSocketHandshake)
    }

    async fn handle(
        &self,
        _exchange: &mut WsHandshakeExchange,
        _cancellation: lily_websocket::ExecutionCancellation,
    ) -> Result<(), WsHandshakeRejection> {
        Ok(())
    }
}

#[derive(WebSocketController)]
#[namespace("chat")]
#[connection_middleware(NotConnectionMiddleware)]
struct ChatController;

#[async_trait]
impl WebSocketControllerTrait for ChatController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

fn main() {}
