use lily_websocket::{
    Extensions, WebSocketControllerInitError, WebSocketControllerTrait, async_trait,
    websocket_controller,
};
use std::sync::Arc;

struct ConnectedPayloadController;

#[async_trait]
impl WebSocketControllerTrait for ConnectedPayloadController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

#[websocket_controller]
impl ConnectedPayloadController {
    #[connected]
    async fn connected(
        &self,
        _payload: lily_websocket::Payload<String>,
    ) -> Result<(), lily_websocket::WebSocketLifecycleError> {
        Ok(())
    }
}

fn main() {}
