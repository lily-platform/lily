use lilyrs::websocket::{
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
        _payload: lilyrs::websocket::TextPayload,
    ) -> Result<(), lilyrs::websocket::WebSocketLifecycleError> {
        Ok(())
    }
}
fn main() {}
