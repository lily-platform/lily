use lily_websocket_derive::websocket_controller;

struct Extractor;
struct NoReply;
struct WebSocketActionError;
struct WideController;

#[websocket_controller]
impl WideController {
    #[message("send")]
    async fn send(
        &self,
        _00: Extractor,
        _01: Extractor,
        _02: Extractor,
        _03: Extractor,
        _04: Extractor,
        _05: Extractor,
        _06: Extractor,
        _07: Extractor,
        _08: Extractor,
        _09: Extractor,
        _10: Extractor,
        _11: Extractor,
        _12: Extractor,
        _13: Extractor,
        _14: Extractor,
        _15: Extractor,
        _16: Extractor,
    ) -> Result<NoReply, WebSocketActionError> {
        Ok(NoReply)
    }
}

fn main() {}
