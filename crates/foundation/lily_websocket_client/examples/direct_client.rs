use lily_websocket_client::{TokioWsClient, WebSocketClientConfig, WsClient};
use serde_json::json;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = TokioWsClient::with_config(WebSocketClientConfig {
        url: "wss://events.example.com/socket".into(),
        namespace: Some("chat".into()),
        ..Default::default()
    })?;

    client.on_async("chat:status", |event, payload| async move {
        println!("received {event} ({} bytes)", payload.len());
    });

    let application_cancel = CancellationToken::new();
    client.connect(application_cancel.clone()).await?;
    client
        .send("chat:get_status", json!({ "request_id": "example-1" }))
        .await?;
    client.disconnect().await?;
    Ok(())
}
