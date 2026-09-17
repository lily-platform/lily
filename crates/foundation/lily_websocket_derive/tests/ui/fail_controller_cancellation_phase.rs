use std::sync::Arc;
use lily_websocket::{
    CleanupCancellation, ExecutionCancellation, Extensions, NoReply, WebSocketActionError,
    WebSocketController, WebSocketControllerInitError, WebSocketControllerTrait,
    WebSocketLifecycleError, async_trait, websocket_controller,
};

#[derive(WebSocketController)]
#[namespace("wrong-cancellation-phase")]
struct Controller;

#[async_trait]
impl WebSocketControllerTrait for Controller {
    async fn new(_: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> { Ok(Self) }
}

#[websocket_controller]
impl Controller {
    #[connected]
    async fn connected(&self, _: CleanupCancellation) -> Result<(), WebSocketLifecycleError> { Ok(()) }

    #[disconnected]
    async fn disconnected(&self, _: ExecutionCancellation) -> Result<(), WebSocketLifecycleError> { Ok(()) }

    #[message("send")]
    async fn send(&self, _: CleanupCancellation) -> Result<NoReply, WebSocketActionError> { Ok(NoReply) }
}

fn main() {}
