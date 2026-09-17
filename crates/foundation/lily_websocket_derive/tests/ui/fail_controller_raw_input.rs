use lily_websocket_derive::websocket_controller;

struct WsRequest;
struct NoReply;
struct WebSocketActionError;
struct RawInputController;

#[websocket_controller]
impl RawInputController {
    #[message("send")]
    async fn send(
        &self,
        _request: std::sync::Arc<WsRequest>,
    ) -> Result<NoReply, WebSocketActionError> {
        Ok(NoReply)
    }
}

fn main() {}
