use std::sync::Arc;

use lily_websocket::middleware::{
    MiddlewareDescriptor, MiddlewareKind, WsConnectionMiddleware, WsMiddlewareInitError,
};
use lily_websocket::{
    Extensions, WebSocketContext, WebSocketController, WebSocketControllerInitError,
    WebSocketControllerTrait, async_trait,
};

struct NotHandshakeMiddleware;

#[async_trait]
impl WsConnectionMiddleware for NotHandshakeMiddleware {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Ok(Self)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new(
            "wrong_connection_trait",
            MiddlewareKind::WebSocketConnection,
        )
    }

    async fn opened(
        &self,
        _context: Arc<WebSocketContext>,
        _cancellation: lily_websocket::ExecutionCancellation,
    ) -> Result<(), lily_websocket::middleware::WsMiddlewareError> {
        Ok(())
    }
}

#[derive(WebSocketController)]
#[namespace("chat")]
#[handshake_middleware(NotHandshakeMiddleware)]
struct ChatController;

#[async_trait]
impl WebSocketControllerTrait for ChatController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

fn main() {}
