use lily_websocket_derive::websocket_controller;

struct FrameCodec;
struct Payload;
struct NoReply;
struct WebSocketActionError;
struct ActionFrameCodecController;

#[websocket_controller]
impl ActionFrameCodecController {
    #[message("send")]
    #[frame_codec(FrameCodec)]
    async fn send(&self, _payload: Payload) -> Result<NoReply, WebSocketActionError> {
        Ok(NoReply)
    }
}

fn main() {}
