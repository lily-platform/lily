use lily_websocket::{
    BinaryPayload, Extensions, NoReply, Payload, WebSocketActionError,
    WebSocketControllerInitError, WebSocketControllerTrait, async_trait, websocket_controller,
};
use std::sync::Arc;

struct TwoPayloadConsumersController;

#[async_trait]
impl WebSocketControllerTrait for TwoPayloadConsumersController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

#[websocket_controller]
impl TwoPayloadConsumersController {
    #[message("send")]
    async fn send(
        &self,
        _json: Payload<String>,
        _binary: BinaryPayload,
    ) -> Result<NoReply, WebSocketActionError> {
        Ok(NoReply)
    }
}

fn main() {}
