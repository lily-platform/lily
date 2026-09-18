use lily_websocket_derive::websocket_controller;

struct InvalidSignatureController;

#[websocket_controller]
impl InvalidSignatureController {
    #[message("send")]
    async fn send(&self) -> Result<(), ()> {
        Ok(())
    }
}

fn main() {}
