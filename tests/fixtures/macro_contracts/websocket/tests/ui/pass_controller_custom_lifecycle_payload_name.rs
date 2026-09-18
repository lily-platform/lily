use std::{future::ready, sync::Arc};

use lily_websocket::{
    Extensions, WebSocketControllerInitError, WebSocketControllerTrait, WebSocketLifecycleError,
    async_trait, websocket_controller,
};

mod custom {
    use super::*;
    use lily_websocket::{
        Connected, FromWebSocketLifecycleParts, WebSocketExtractionError,
        WebSocketLifecycleInvocation,
    };

    pub struct Payload;

    impl FromWebSocketLifecycleParts<Connected> for Payload {
        type Rejection = WebSocketExtractionError;

        fn from_lifecycle_parts(
            _invocation: &mut WebSocketLifecycleInvocation,
        ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
            ready(Ok(Self))
        }
    }
}

struct ChatController;

#[async_trait]
impl WebSocketControllerTrait for ChatController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

#[websocket_controller]
impl ChatController {
    #[connected]
    async fn connected(&self, _payload: custom::Payload) -> Result<(), WebSocketLifecycleError> {
        Ok(())
    }
}

fn main() {}
