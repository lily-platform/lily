use std::sync::Arc;

use lily_websocket::{
    Extensions, WebSocketController, WebSocketControllerInitError, WebSocketControllerTrait,
    async_trait,
};

struct NotMessageMiddleware;

#[derive(WebSocketController)]
#[namespace("chat")]
#[message_middleware(NotMessageMiddleware)]
struct ChatController;

#[async_trait]
impl WebSocketControllerTrait for ChatController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

fn main() {}
