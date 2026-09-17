use lily_websocket_derive::websocket_controller;

struct WebSocketContext;
struct NoReply;
struct WebSocketActionError;
struct ReferenceController;

#[websocket_controller]
impl ReferenceController {
    #[message("send")]
    async fn send(
        &self,
        _context: &WebSocketContext,
    ) -> Result<NoReply, WebSocketActionError> {
        Ok(NoReply)
    }
}

fn main() {}
