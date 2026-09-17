use std::sync::Arc;
use lily_websocket::{
    CleanupCancellation, ExecutionCancellation, Extensions, NoReply, WebSocketActionError,
    WebSocketController, WebSocketControllerInitError, WebSocketControllerTrait,
    WebSocketLifecycleError, WebSocketMessageContext, async_trait, websocket_controller,
};

#[derive(WebSocketController)]
#[namespace("cancellation")]
struct Controller;

#[async_trait]
impl WebSocketControllerTrait for Controller {
    async fn new(_: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> { Ok(Self) }
}

#[websocket_controller]
impl Controller {
    #[connected]
    async fn connected(&self, cancellation: ExecutionCancellation) -> Result<(), WebSocketLifecycleError> {
        let _ = cancellation.is_cancelled();
        Ok(())
    }

    #[disconnected]
    async fn disconnected(&self, cancellation: CleanupCancellation) -> Result<(), WebSocketLifecycleError> {
        let _ = cancellation.is_cancelled();
        Ok(())
    }

    #[message("send")]
    async fn send(&self, cancellation: ExecutionCancellation, context: WebSocketMessageContext) -> Result<NoReply, WebSocketActionError> {
        let _ = (cancellation.is_cancelled(), context.cancellation().is_cancelled());
        Ok(NoReply)
    }
}

fn main() {}
