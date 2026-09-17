//! Bounded outbound WebSocket client for Lily applications.
//!
//! [`TokioWsClient`] is the canonical transport. One supervised task owns the
//! socket, all application writes pass through a bounded queue, and a send is
//! successful only after Tungstenite accepts and flushes the frame. Use
//! [`WsClient::on_async`] for callbacks that wait on I/O.
//!
//! # Direct client
//!
//! ```no_run
//! use lily_websocket_client::{TokioWsClient, WebSocketClientConfig, WsClient};
//! use tokio_util::sync::CancellationToken;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = TokioWsClient::with_config(WebSocketClientConfig {
//!     url: "wss://events.example.com/ws".into(),
//!     namespace: Some("chat".into()),
//!     ..WebSocketClientConfig::default()
//! })?;
//!
//! client.on_async("chat:received", |event, payload| async move {
//!     println!("{event}: {} bytes", payload.len());
//! });
//!
//! client.connect(CancellationToken::new()).await?;
//! client
//!     .send("chat:send", serde_json::json!({ "text": "hello" }))
//!     .await?;
//! client.disconnect().await?;
//! # Ok(())
//! # }
//! ```
//!
//! The default `single` feature also exposes one DI-managed
//! `WebSocketClientService`. Disable default features for a direct-only
//! client. Enable only `factory` to expose a DI-managed set of named clients;
//! `single` and `factory` are mutually exclusive.
#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]

#[cfg(all(feature = "single", feature = "factory"))]
compile_error!(
    "lily_websocket_client features `single` and `factory` are mutually exclusive; disable default features before enabling `factory`"
);

mod lifecycle;
mod providers;
mod traits;
mod types;

#[cfg(any(feature = "single", feature = "factory"))]
mod service;

// Re-exports
pub use types::{
    auth::{AuthHeaderProvider, BearerTokenFile, StaticAuthHeaders},
    config::{ReconnectionConfig, WebSocketClientConfig},
    error::WebSocketError,
    message::{
        ContentEncoding, ContentKind, DecodedPayload, MessageType, WebSocketReply, WsMessage,
        BINARY_CONTENT_TYPE, JSON_CONTENT_TYPE, LILY_WEBSOCKET_PROTOCOL_VERSION,
        LILY_WEBSOCKET_SUBPROTOCOL, TEXT_CONTENT_TYPE,
    },
    state::ConnectionState,
};

pub use traits::client_trait::WsClient;

pub use lifecycle::WebSocketClientShutdownHandle;
pub use providers::{TokioWsClient, WebSocketClientMetricSnapshot};

#[cfg(any(feature = "single", feature = "factory"))]
pub use service::websocket_client_service::WebSocketClientService;

#[cfg(feature = "factory")]
pub use service::websocket_client_factory::WebSocketClientFactory;
