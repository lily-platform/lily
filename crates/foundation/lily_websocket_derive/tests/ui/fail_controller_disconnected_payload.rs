use lily_websocket::{
    Extensions, WebSocketControllerInitError, WebSocketControllerTrait, async_trait,
    websocket_controller,
};
use std::sync::Arc;

struct DisconnectedPayloadController;

#[async_trait]
impl WebSocketControllerTrait for DisconnectedPayloadController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

#[websocket_controller]
impl DisconnectedPayloadController {
    #[disconnected]
    async fn disconnected(
        &self,
        _payload: lily_websocket::BinaryPayload,
    ) -> Result<(), lily_websocket::WebSocketLifecycleError> {
        Ok(())
    }
}

fn main() {}
