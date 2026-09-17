use std::sync::Arc;

use lily_websocket::middleware::{
    MiddlewareDescriptor, MiddlewareKind, WebSocketHandshakeMiddleware, WsConnectionMiddleware,
    WsHandshakeExchange, WsHandshakeRejection, WsMiddlewareError, WsMiddlewareInitError,
};
use lily_websocket::{
    Extensions, WebSocketContext, WebSocketController, WebSocketControllerInitError,
    WebSocketControllerTrait, async_trait,
};

struct RequestAdmission;
struct ConnectionAudit;

#[async_trait]
impl WebSocketHandshakeMiddleware for RequestAdmission {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Ok(Self)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("request_admission", MiddlewareKind::WebSocketHandshake)
    }

    async fn handle(
        &self,
        _exchange: &mut WsHandshakeExchange,
        _cancellation: lily_websocket::ExecutionCancellation,
    ) -> Result<(), WsHandshakeRejection> {
        Ok(())
    }
}

#[async_trait]
impl WsConnectionMiddleware for ConnectionAudit {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Ok(Self)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("connection_audit", MiddlewareKind::WebSocketConnection)
    }

    async fn opened(&self, _context: Arc<WebSocketContext>,
        _cancellation: lily_websocket::ExecutionCancellation,
    ) -> Result<(), WsMiddlewareError> {
        Ok(())
    }
}

#[derive(WebSocketController)]
#[namespace("chat")]
#[handshake_middleware(RequestAdmission)]
#[connection_middleware(ConnectionAudit)]
struct ChatController;

#[async_trait]
impl WebSocketControllerTrait for ChatController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

fn main() {}
