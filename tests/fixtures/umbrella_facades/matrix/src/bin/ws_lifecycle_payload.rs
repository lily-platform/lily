use lily::websocket::{
    Extensions, WebSocketControllerInitError, WebSocketControllerTrait, async_trait,
    websocket_controller,
};
use std::sync::Arc;
struct Controller;
#[async_trait]
impl WebSocketControllerTrait for Controller {
    async fn new(_: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}
#[websocket_controller]
impl Controller {
    #[connected]
    async fn connected(
        &self,
        _payload: lily::websocket::TextPayload,
    ) -> Result<(), lily::websocket::WebSocketLifecycleError> {
        Ok(())
    }
}
fn main() {}
