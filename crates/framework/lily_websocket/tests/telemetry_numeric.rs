//! Verify the wire message length through a real listener and OTel span layer.
use std::{sync::Arc, time::Duration};

use futures_util::{SinkExt, StreamExt};
use lily_websocket::{
    Emit, Extensions, ServerConfig, WebSocketActionError, WebSocketController,
    WebSocketControllerInitError, WebSocketControllerTrait, WsAppBuilder, WsMessageBody,
};
use opentelemetry::{Value, trace::TracerProvider as _};
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use serde_json::json;
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use tracing_subscriber::prelude::*;

#[derive(WebSocketController)]
#[namespace("numeric")]
struct Endpoints;
#[async_trait::async_trait]
impl WebSocketControllerTrait for Endpoints {
    async fn new(_: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}
#[lily_websocket::websocket_controller]
impl Endpoints {
    #[message("check")]
    async fn check(&self) -> Result<Emit<serde_json::Value>, WebSocketActionError> {
        Ok(Emit::new("numeric:done", json!({"ok": true}))?)
    }
}

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(15), future)
        .await
        .expect("bounded WebSocket test")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn message_span_exports_utf8_wire_bytes_as_a_single_integer() {
    let exports = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exports.clone())
        .build();
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("ws-numeric-test"))),
    )
    .unwrap();
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = socket.local_addr().unwrap();
    drop(socket);
    let app = Arc::new(
        WsAppBuilder::new(&address.to_string())
            .config(ServerConfig {
                allowed_origins: vec!["https://numeric.test".into()],
                ..Default::default()
            })
            .tracing_external()
            .build()
            .await
            .unwrap(),
    );
    let running = app.clone();
    let server = tokio::spawn(async move { running.start().await });
    bounded(async {
        while !app.health_snapshot().unwrap().accepting_new_work {
            assert!(!server.is_finished(), "server failed before readiness");
            tokio::task::yield_now().await;
        }
    })
    .await;
    let mut request = format!("ws://{address}/ws?namespace=numeric")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("origin", "https://numeric.test".parse().unwrap());
    let (mut client, response) = bounded(tokio_tungstenite::connect_async(request))
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 101);
    let mut expected = Vec::new();
    for text in ["", "çığ😀", "private-message-canary"] {
        let message = WsMessageBody::try_new("numeric:check", json!({"text": text}))
            .unwrap()
            .with_namespace("numeric".into())
            .to_message()
            .unwrap();
        expected.push(message.len() as i64);
        client.send(message).await.unwrap();
        let reply = bounded(async {
            loop {
                match client.next().await.unwrap().unwrap() {
                    Message::Text(text) => break text,
                    Message::Ping(bytes) => client.send(Message::Pong(bytes)).await.unwrap(),
                    other => panic!("unexpected frame: {other:?}"),
                }
            }
        })
        .await;
        let reply: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(reply["event"], "numeric:done");
    }
    client.close(None).await.unwrap();
    bounded(async {
        while let Some(frame) = client.next().await {
            if matches!(frame, Ok(Message::Close(_))) {
                break;
            }
        }
    })
    .await;
    drop(client);
    bounded(app.close()).await.unwrap();
    bounded(server).await.unwrap().unwrap();
    provider.force_flush().unwrap();
    let spans = exports.get_finished_spans().unwrap();
    let messages: Vec<_> = spans
        .iter()
        .filter(|span| span.name == "websocket.message")
        .collect();
    assert_eq!(messages.len(), expected.len());
    for (span, bytes) in messages.iter().zip(expected) {
        let sizes: Vec<_> = span
            .attributes
            .iter()
            .filter(|kv| kv.key.as_str() == "lily.message_size")
            .collect();
        assert_eq!(
            sizes.len(),
            1,
            "raw OTLP attributes must not repeat message size"
        );
        assert_eq!(sizes[0].value, Value::I64(bytes));
        assert_eq!(span.dropped_attributes_count, 0);
        assert_eq!(span.events.dropped_count, 0);
        assert!(!format!("{span:?}").contains("private-message-canary"));
    }
    provider.shutdown().unwrap();
}
