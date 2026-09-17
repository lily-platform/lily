use std::sync::Arc;

use lily_websocket::{
    Extensions, NoReply, Payload, ServerConfig, WebSocketActionError, WebSocketContext,
    WebSocketController, WebSocketControllerInitError, WebSocketControllerTrait, WsAppBuilder,
    async_trait, websocket_controller,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
struct SendMessage {
    text: String,
}

#[derive(WebSocketController)]
#[namespace("chat")]
struct ChatController;

#[async_trait]
impl WebSocketControllerTrait for ChatController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

#[websocket_controller]
impl ChatController {
    #[message("send")]
    async fn send(
        &self,
        context: WebSocketContext,
        Payload(input): Payload<SendMessage>,
    ) -> Result<NoReply, WebSocketActionError> {
        context
            .clients()
            .others()
            .send("chat:received", &input)
            .await
            .map_err(WebSocketActionError::internal)?;
        Ok(NoReply)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let app = WsAppBuilder::new("127.0.0.1:8080")
        .config(ServerConfig {
            endpoint_path: "/ws".into(),
            allowed_origins: vec!["http://localhost:3000".into()],
            ..ServerConfig::default()
        })
        .build()
        .await?;

    app.start().await?;
    Ok(())
}
