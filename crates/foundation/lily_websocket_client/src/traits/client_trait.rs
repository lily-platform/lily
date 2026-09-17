// =============================================================================
// WsClient Trait - High-Level Client Interface
// =============================================================================

use crate::{ReconnectionConfig, WebSocketError, WebSocketReply};
use async_trait::async_trait;
use serde::Serialize;
use std::future::Future;
use tokio_util::sync::CancellationToken;

/// High-level contract implemented by Lily's outbound WebSocket transport.
///
/// Event sends use the strict Lily v2 JSON envelope. Raw text and binary
/// methods bypass that envelope but still use the same bounded writer,
/// message-size limit, acknowledgement, and timeout behavior.
#[async_trait]
pub trait WsClient: Send + Sync {
    /// Connects and starts the single supervised socket runtime.
    ///
    /// Cancelling `ct` closes the runtime. Calling `connect` while already
    /// connected is idempotent; calling it while a connection attempt is
    /// still active returns [`WebSocketError::AlreadyConnected`].
    async fn connect(&self, ct: CancellationToken) -> Result<(), WebSocketError>;

    /// Performs a bounded Close handshake and joins the supervised runtime.
    /// Calling this on a disconnected client succeeds without side effects.
    async fn disconnect(&self) -> Result<(), WebSocketError>;

    /// Serializes and flushes one canonical Lily v2 JSON event envelope.
    ///
    /// Success means the socket writer accepted and flushed the frame, not
    /// merely that the message entered the application queue.
    async fn send<T: Serialize + Send>(&self, event: &str, data: T) -> Result<(), WebSocketError>;

    /// Sends one event with a fresh correlation authority and waits for the
    /// matching `ack` or correlated `error` envelope.
    ///
    /// The waiter consumes one slot from a table bounded by
    /// `outbound_queue_capacity`. Lily does not retry automatically: a timeout,
    /// cancellation or disconnect removes the waiter and a later reply is a
    /// protocol error rather than a normal callback.
    async fn request<T: Serialize + Send>(
        &self,
        event: &str,
        data: T,
        acknowledgement_timeout: std::time::Duration,
    ) -> Result<WebSocketReply, WebSocketError>;

    /// Serializes and flushes one Lily v2 UTF-8 text event envelope.
    async fn send_text_event(&self, event: &str, text: String) -> Result<(), WebSocketError>;

    /// Serializes and flushes one Lily v2 Base64 binary event envelope.
    async fn send_binary_event(&self, event: &str, data: Vec<u8>) -> Result<(), WebSocketError>;

    /// Flushes one raw application Binary frame without a Lily envelope.
    async fn send_binary(&self, data: Vec<u8>) -> Result<(), WebSocketError>;

    /// Flushes one raw application Text frame without a Lily event envelope.
    async fn send_text(&self, text: String) -> Result<(), WebSocketError>;

    /// Registers a synchronous listener for an exact event or `*` wildcard.
    ///
    /// Registering the same key again replaces its previous listener. The
    /// callback must not block a Tokio worker; use [`WsClient::on_async`] for
    /// work that waits on I/O.
    fn on<F>(&self, event: &str, callback: F)
    where
        F: Fn(String, Vec<u8>) + Send + Sync + 'static;

    /// Registers an asynchronous listener for an exact event or `*` wildcard.
    ///
    /// The client enforces configured callback queue, concurrency, and
    /// execution budgets. Registering the same key again replaces its
    /// previous listener.
    fn on_async<F, Fut>(&self, event: &str, callback: F)
    where
        F: Fn(String, Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static;

    /// Returns whether the supervised runtime currently owns an established socket.
    fn is_connected(&self) -> bool;

    /// Replaces the reconnect policy observed by future reconnect attempts.
    ///
    /// The new policy is validated synchronously and does not restart the
    /// current connection.
    fn set_reconnection(&self, config: ReconnectionConfig) -> Result<(), WebSocketError>;
}
