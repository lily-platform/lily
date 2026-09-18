use lily_websocket_derive::websocket_controller;

struct PayloadCodec;
struct WebSocketContext;
struct WebSocketLifecycleError;
struct LifecyclePayloadCodecController;

#[websocket_controller]
impl LifecyclePayloadCodecController {
    #[connected]
    #[payload_codec(PayloadCodec)]
    async fn connected(
        &self,
        _context: WebSocketContext,
    ) -> Result<(), WebSocketLifecycleError> {
        Ok(())
    }
}

fn main() {}
