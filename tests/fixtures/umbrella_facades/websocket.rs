use provider::{
    Extensions, NoReply, WebSocketActionError, WebSocketController, WebSocketControllerInitError,
    WebSocketControllerTrait, WebSocketLifecycleError, websocket_controller,
};
use std::sync::Arc;
#[derive(WebSocketController)]
#[namespace("umbrella")]
struct WsController;
#[lifecycle]
impl WebSocketControllerTrait for WsController {
    async fn new(_: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}
#[websocket_controller]
impl WsController {
    #[connected]
    async fn connected(&self) -> Result<(), WebSocketLifecycleError> {
        Ok(())
    }
    #[message("ping")]
    async fn ping(&self, _text: provider::TextPayload) -> Result<NoReply, WebSocketActionError> {
        Ok(NoReply)
    }
}
#[test]
fn websocket_macros_share_the_facades_controller_registry() {
    let controllers = provider::__private::WEBSOCKET_CONTROLLER_REGISTRATIONS
        .iter()
        .map(|register| register())
        .collect::<Vec<_>>();
    assert_eq!(controllers.len(), 1);
    assert_eq!(
        controllers[0].type_id(),
        std::any::TypeId::of::<WsController>()
    );
    assert_eq!(controllers[0].namespace(), "umbrella");
    let operations = provider::__private::PENDING_WEBSOCKET_OPERATION_REGISTRATIONS
        .iter()
        .map(|register| register())
        .collect::<Vec<_>>();
    assert_eq!(operations.len(), 2);
    let mut actual: Vec<_> = operations
        .iter()
        .map(|operation| (operation.kind().as_str(), operation.event()))
        .collect();
    actual.sort();
    assert_eq!(actual, [("connected", None), ("message", Some("ping"))]);
}
