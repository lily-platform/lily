use std::sync::Arc;

use lily_websocket::{
    Extensions, NoReply, WebSocketActionError, WebSocketControllerInitError,
    WebSocketControllerTrait, async_trait, websocket_controller,
};

struct NotMessageMiddleware;
struct ChatController;

#[async_trait]
impl WebSocketControllerTrait for ChatController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

#[websocket_controller]
impl ChatController {
    #[message("send")]
    #[message_middleware(NotMessageMiddleware)]
    async fn send(&self) -> Result<NoReply, WebSocketActionError> {
        Ok(NoReply)
    }
}

fn main() {}
