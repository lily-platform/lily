//! Tokio/Tungstenite production transport.
//!
//! One supervised task owns the socket for its entire lifetime. Both inbound
//! and outbound work are driven by that task, which prevents duplicate reader
//! tasks and makes cancellation, reconnect and close ordering deterministic.

#[cfg(test)]
use crate::DecodedPayload;
use crate::{
    AuthHeaderProvider, ConnectionState, MessageType, ReconnectionConfig, WebSocketClientConfig,
    WebSocketError, WebSocketReply, WsClient, WsMessage, LILY_WEBSOCKET_PROTOCOL_VERSION,
};
use async_trait::async_trait;
use dashmap::{mapref::entry::Entry, DashMap};
use futures_util::{SinkExt, StreamExt};
use opentelemetry::{
    global,
    metrics::{Counter, Histogram, UpDownCounter},
    KeyValue,
};
use rustls_pki_types::{
    pem::{PemObject as _, SectionKind},
    CertificateDer,
};
use serde::Serialize;
use serde_json::Value;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot, Mutex, OwnedSemaphorePermit, RwLock, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep, timeout, Instant, MissedTickBehavior};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::header::{HeaderName, HeaderValue};
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, WebSocketConfig};
use tokio_tungstenite::tungstenite::{protocol::frame::coding::CloseCode, Message};
use tokio_tungstenite::{
    connect_async_tls_with_config, Connector, MaybeTlsStream, WebSocketStream,
};
use tokio_util::sync::CancellationToken;
use tracing::Instrument;
use url::Url;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type CallbackFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type SyncEventCallback = Arc<dyn Fn(String, Vec<u8>) + Send + Sync>;
type AsyncEventCallback = Arc<dyn Fn(String, Vec<u8>) -> CallbackFuture + Send + Sync>;
type PendingAcknowledgements = DashMap<String, PendingAcknowledgement>;

struct PendingAcknowledgement {
    sender: oneshot::Sender<Result<WebSocketReply, WebSocketError>>,
    _permit: OwnedSemaphorePermit,
}

struct PendingAcknowledgementGuard {
    acknowledgement_id: String,
    pending: Arc<PendingAcknowledgements>,
}

impl Drop for PendingAcknowledgementGuard {
    fn drop(&mut self) {
        self.pending.remove(&self.acknowledgement_id);
    }
}

struct PendingAcknowledgementDrain(Arc<PendingAcknowledgements>);

impl Drop for PendingAcknowledgementDrain {
    fn drop(&mut self) {
        TokioWsClient::fail_pending_acknowledgements(&self.0);
    }
}

#[derive(Clone)]
enum EventCallback {
    Sync(SyncEventCallback),
    Async(AsyncEventCallback),
}

struct CallbackEvent {
    event: String,
    payload: Vec<u8>,
    parent: tracing::Span,
}

const STATE_DISCONNECTED: u8 = 0;
const STATE_CONNECTING: u8 = 1;
const STATE_CONNECTED: u8 = 2;
const STATE_RECONNECTING: u8 = 3;
const STATE_FAILED: u8 = 4;

/// Bounded, monotonic terminal ledger for callback and client-runtime work.
/// Every field is safe to expose to metrics; no URL, credential, event name,
/// or payload is retained.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WebSocketClientMetricSnapshot {
    /// Callbacks that returned before their deadline.
    pub callback_completed: u64,
    /// Callback tasks that panicked and were contained.
    pub callback_panicked: u64,
    /// Callback tasks cancelled after exceeding their execution budget.
    pub callback_timed_out: u64,
    /// Callback tasks whose Tokio join failed.
    pub callback_join_failed: u64,
    /// Inbound callbacks rejected because the bounded dispatcher was full or closed.
    pub callback_rejected: u64,
    /// WebSocket Close-frame writes or acknowledgements that failed.
    pub close_failed: u64,
    /// Supervised client runtime tasks whose Tokio join failed.
    pub runtime_join_failed: u64,
    /// Outbound acknowledgements whose caller was no longer waiting.
    pub acknowledgement_abandoned: u64,
    /// Initial connection attempts started.
    pub connect_attempted: u64,
    /// Initial connection attempts completed successfully.
    pub connect_succeeded: u64,
    /// Initial connection attempts that failed.
    pub connect_failed: u64,
    /// Reconnection attempts started after a previously established socket closed.
    pub reconnect_attempted: u64,
    /// Reconnection attempts completed successfully.
    pub reconnect_succeeded: u64,
    /// Reconnection attempts that failed.
    pub reconnect_failed: u64,
    /// Application frames flushed by the socket writer.
    pub outbound_messages: u64,
    /// Application frames accepted from the peer.
    pub inbound_messages: u64,
    /// Sends rejected because the bounded outbound queue was full.
    pub outbound_queue_full: u64,
    /// Sends rejected because the outbound runtime was already closed.
    pub outbound_queue_closed: u64,
    /// Client runtimes that completed a graceful shutdown.
    pub graceful_closes: u64,
    /// Client runtimes that required forced or failed shutdown.
    pub forced_closes: u64,
    /// Inbound messages rejected by the Lily envelope contract.
    pub protocol_failures: u64,
    /// Handshakes rejected because credentials could not be produced or applied.
    pub authentication_failures: u64,
    /// Operations terminated by application or runtime cancellation.
    pub cancellations: u64,
    /// Operations terminated after a bounded deadline elapsed.
    pub timeouts: u64,
}

struct ClientMetrics {
    callback_completed: AtomicU64,
    callback_panicked: AtomicU64,
    callback_timed_out: AtomicU64,
    callback_join_failed: AtomicU64,
    callback_rejected: AtomicU64,
    close_failed: AtomicU64,
    runtime_join_failed: AtomicU64,
    acknowledgement_abandoned: AtomicU64,
    connect_attempted: AtomicU64,
    connect_succeeded: AtomicU64,
    connect_failed: AtomicU64,
    reconnect_attempted: AtomicU64,
    reconnect_succeeded: AtomicU64,
    reconnect_failed: AtomicU64,
    outbound_messages: AtomicU64,
    inbound_messages: AtomicU64,
    outbound_queue_full: AtomicU64,
    outbound_queue_closed: AtomicU64,
    graceful_closes: AtomicU64,
    forced_closes: AtomicU64,
    protocol_failures: AtomicU64,
    authentication_failures: AtomicU64,
    cancellations: AtomicU64,
    timeouts: AtomicU64,
    operations: Counter<u64>,
    active_connections: UpDownCounter<i64>,
    connection_duration: Histogram<f64>,
    message_duration: Histogram<f64>,
    queue_wait: Histogram<f64>,
    queue_depth: Histogram<u64>,
}

struct ActiveClientConnectionGuard {
    metrics: Arc<ClientMetrics>,
}

impl ActiveClientConnectionGuard {
    fn new(metrics: Arc<ClientMetrics>) -> Self {
        metrics.active_connections.add(1, &[]);
        Self { metrics }
    }
}

impl Drop for ActiveClientConnectionGuard {
    fn drop(&mut self) {
        self.metrics.active_connections.add(-1, &[]);
    }
}

impl Default for ClientMetrics {
    fn default() -> Self {
        let meter = global::meter("lily_websocket_client");
        Self {
            callback_completed: AtomicU64::new(0),
            callback_panicked: AtomicU64::new(0),
            callback_timed_out: AtomicU64::new(0),
            callback_join_failed: AtomicU64::new(0),
            callback_rejected: AtomicU64::new(0),
            close_failed: AtomicU64::new(0),
            runtime_join_failed: AtomicU64::new(0),
            acknowledgement_abandoned: AtomicU64::new(0),
            connect_attempted: AtomicU64::new(0),
            connect_succeeded: AtomicU64::new(0),
            connect_failed: AtomicU64::new(0),
            reconnect_attempted: AtomicU64::new(0),
            reconnect_succeeded: AtomicU64::new(0),
            reconnect_failed: AtomicU64::new(0),
            outbound_messages: AtomicU64::new(0),
            inbound_messages: AtomicU64::new(0),
            outbound_queue_full: AtomicU64::new(0),
            outbound_queue_closed: AtomicU64::new(0),
            graceful_closes: AtomicU64::new(0),
            forced_closes: AtomicU64::new(0),
            protocol_failures: AtomicU64::new(0),
            authentication_failures: AtomicU64::new(0),
            cancellations: AtomicU64::new(0),
            timeouts: AtomicU64::new(0),
            operations: meter.u64_counter("websocket.client.operations").build(),
            active_connections: meter
                .i64_up_down_counter("websocket.client.connections.active")
                .build(),
            connection_duration: meter
                .f64_histogram("websocket.client.connection.duration")
                .with_unit("s")
                .build(),
            message_duration: meter
                .f64_histogram("websocket.client.message.duration")
                .with_unit("s")
                .build(),
            queue_wait: meter
                .f64_histogram("websocket.client.outbound.queue.wait.duration")
                .with_unit("s")
                .build(),
            queue_depth: meter
                .u64_histogram("websocket.client.outbound.queue.depth")
                .build(),
        }
    }
}

impl ClientMetrics {
    fn snapshot(&self) -> WebSocketClientMetricSnapshot {
        WebSocketClientMetricSnapshot {
            callback_completed: self.callback_completed.load(Ordering::Acquire),
            callback_panicked: self.callback_panicked.load(Ordering::Acquire),
            callback_timed_out: self.callback_timed_out.load(Ordering::Acquire),
            callback_join_failed: self.callback_join_failed.load(Ordering::Acquire),
            callback_rejected: self.callback_rejected.load(Ordering::Acquire),
            close_failed: self.close_failed.load(Ordering::Acquire),
            runtime_join_failed: self.runtime_join_failed.load(Ordering::Acquire),
            acknowledgement_abandoned: self.acknowledgement_abandoned.load(Ordering::Acquire),
            connect_attempted: self.connect_attempted.load(Ordering::Acquire),
            connect_succeeded: self.connect_succeeded.load(Ordering::Acquire),
            connect_failed: self.connect_failed.load(Ordering::Acquire),
            reconnect_attempted: self.reconnect_attempted.load(Ordering::Acquire),
            reconnect_succeeded: self.reconnect_succeeded.load(Ordering::Acquire),
            reconnect_failed: self.reconnect_failed.load(Ordering::Acquire),
            outbound_messages: self.outbound_messages.load(Ordering::Acquire),
            inbound_messages: self.inbound_messages.load(Ordering::Acquire),
            outbound_queue_full: self.outbound_queue_full.load(Ordering::Acquire),
            outbound_queue_closed: self.outbound_queue_closed.load(Ordering::Acquire),
            graceful_closes: self.graceful_closes.load(Ordering::Acquire),
            forced_closes: self.forced_closes.load(Ordering::Acquire),
            protocol_failures: self.protocol_failures.load(Ordering::Acquire),
            authentication_failures: self.authentication_failures.load(Ordering::Acquire),
            cancellations: self.cancellations.load(Ordering::Acquire),
            timeouts: self.timeouts.load(Ordering::Acquire),
        }
    }
}

struct OutboundMessage {
    message: Message,
    acknowledgement: oneshot::Sender<Result<(), OutboundFailure>>,
}

#[derive(Debug)]
enum OutboundFailure {
    Cancelled,
    Timeout(Duration),
    Transport(String),
}

struct RuntimeHandle {
    cancellation: CancellationToken,
    outbound: mpsc::Sender<OutboundMessage>,
    join: JoinHandle<Result<(), WebSocketError>>,
}

struct RuntimeContext {
    config: WebSocketClientConfig,
    state: Arc<AtomicU8>,
    reconnect_attempt: Arc<AtomicUsize>,
    reconnection: Arc<StdRwLock<ReconnectionConfig>>,
    callbacks: Arc<DashMap<String, EventCallback>>,
    pending_acknowledgements: Arc<PendingAcknowledgements>,
    auth_headers: Arc<RwLock<Option<Arc<dyn AuthHeaderProvider>>>>,
    #[cfg(feature = "di")]
    trace_headers: Arc<std::collections::HashMap<String, String>>,
    cancellation: CancellationToken,
    metrics: Arc<ClientMetrics>,
}

/// Tokio-Tungstenite WebSocket client facade.
pub struct TokioWsClient {
    config: WebSocketClientConfig,
    state: Arc<AtomicU8>,
    reconnect_attempt: Arc<AtomicUsize>,
    reconnection_config: Arc<StdRwLock<ReconnectionConfig>>,
    event_callbacks: Arc<DashMap<String, EventCallback>>,
    pending_acknowledgements: Arc<PendingAcknowledgements>,
    pending_acknowledgement_permits: Arc<Semaphore>,
    next_acknowledgement_id: AtomicU64,
    auth_headers: Arc<RwLock<Option<Arc<dyn AuthHeaderProvider>>>>,
    runtime: StdMutex<Option<RuntimeHandle>>,
    lifecycle: Mutex<()>,
    metrics: Arc<ClientMetrics>,
}

impl TokioWsClient {
    /// Synchronously closes runtime admission and wakes the supervisor. The
    /// application lifecycle must still await [`WsClient::disconnect`] so the
    /// runtime task is joined and its terminal outcome is observed.
    pub(crate) fn request_shutdown(&self) {
        if let Ok(runtime) = self.runtime.lock() {
            if let Some(runtime) = runtime.as_ref() {
                runtime.cancellation.cancel();
            }
        }
    }

    /// Create a client whose URL already carries one exact `namespace` query
    /// parameter. Network and TLS validation happen in `connect`.
    ///
    /// Prefer [`Self::with_namespace`] when the namespace is not already part
    /// of the URL.
    pub fn new(url: &str) -> Result<Self, WebSocketError> {
        Self::with_config(WebSocketClientConfig {
            url: url.to_string(),
            ..Default::default()
        })
    }

    /// Create a namespaced client using the Lily v2 message envelope.
    pub fn with_namespace(url: &str, namespace: &str) -> Result<Self, WebSocketError> {
        Self::with_config(WebSocketClientConfig {
            url: url.to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        })
    }

    /// Create a new WebSocket client with validated resource limits.
    pub fn with_config(config: WebSocketClientConfig) -> Result<Self, WebSocketError> {
        config
            .validate()
            .map_err(|reason| WebSocketError::InvalidConfiguration(reason.to_string()))?;
        Ok(Self::from_validated_config(config))
    }

    /// Construction hook used by `lily_websocket_derive` after it has validated
    /// the literal URL at compile time.
    #[doc(hidden)]
    pub fn from_compile_time_config(url: &'static str, namespace: &'static str) -> Self {
        Self::with_config(WebSocketClientConfig {
            url: url.to_string(),
            namespace: Some(namespace.to_string()),
            ..Default::default()
        })
        .expect("lily_websocket_derive emitted an invalid compile-time WebSocket configuration")
    }

    fn from_validated_config(config: WebSocketClientConfig) -> Self {
        let reconnection_config = config.reconnection.clone();
        let pending_acknowledgement_capacity = config.outbound_queue_capacity;

        Self {
            config,
            state: Arc::new(AtomicU8::new(STATE_DISCONNECTED)),
            reconnect_attempt: Arc::new(AtomicUsize::new(0)),
            reconnection_config: Arc::new(StdRwLock::new(reconnection_config)),
            event_callbacks: Arc::new(DashMap::new()),
            pending_acknowledgements: Arc::new(DashMap::new()),
            pending_acknowledgement_permits: Arc::new(Semaphore::new(
                pending_acknowledgement_capacity,
            )),
            next_acknowledgement_id: AtomicU64::new(rand::random()),
            auth_headers: Arc::new(RwLock::new(None)),
            runtime: StdMutex::new(None),
            lifecycle: Mutex::new(()),
            metrics: Arc::new(ClientMetrics::default()),
        }
    }

    /// Snapshot callback/runtime terminal outcomes without exposing message
    /// contents or connection credentials.
    pub fn metrics_snapshot(&self) -> WebSocketClientMetricSnapshot {
        self.metrics.snapshot()
    }

    /// Install a credential provider. It is called before every handshake,
    /// including reconnects, so expiring tokens are never captured forever.
    pub async fn set_auth_header_provider(&self, provider: Arc<dyn AuthHeaderProvider>) {
        *self.auth_headers.write().await = Some(provider);
    }

    /// Remove the dynamic credential provider. Static configured headers are
    /// not affected.
    pub async fn clear_auth_header_provider(&self) {
        *self.auth_headers.write().await = None;
    }

    /// Current lifecycle state without acquiring an async lock.
    pub fn connection_state(&self) -> ConnectionState {
        match self.state.load(Ordering::Acquire) {
            STATE_CONNECTING => ConnectionState::Connecting,
            STATE_CONNECTED => ConnectionState::Connected,
            STATE_RECONNECTING => ConnectionState::Reconnecting {
                attempt: self.reconnect_attempt.load(Ordering::Acquire),
            },
            STATE_FAILED => ConnectionState::Failed,
            _ => ConnectionState::Disconnected,
        }
    }

    /// Remove a callback.
    pub fn off(&self, event: &str) {
        self.event_callbacks.remove(event);
    }

    async fn send_envelope<T: Serialize + Send>(
        &self,
        mut envelope: WsMessage<T>,
    ) -> Result<(), WebSocketError> {
        envelope.namespace = self.config.namespace.clone();
        if !envelope.is_dispatchable_event() {
            return Err(WebSocketError::InvalidEnvelope);
        }
        let text = serde_json::to_string(&envelope)
            .map_err(|error| WebSocketError::SerializationError(error.to_string()))?;
        self.send_frame(Message::Text(text)).await
    }

    /// Send one JSON event envelope. Success means Tungstenite accepted and
    /// flushed the frame, not merely that it entered the application queue.
    async fn send_event<T: Serialize + Send>(
        &self,
        event: &str,
        data: T,
    ) -> Result<(), WebSocketError> {
        self.send_envelope(WsMessage::new_event(event.to_string(), data))
            .await
    }

    async fn send_frame(&self, message: Message) -> Result<(), WebSocketError> {
        let message_size = message.len();
        let span = tracing::info_span!(
            "websocket.message",
            otel.kind = "client",
            lily.direction = "outbound",
            lily.message_size = message_size,
            lily.outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            lily.queue_wait_ms = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let started = Instant::now();
        let result = self
            .send_frame_inner(message)
            .instrument(span.clone())
            .await;
        match &result {
            Ok(()) => {
                span.record("lily.outcome", "success");
                self.metrics
                    .outbound_messages
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics.operations.add(
                    1,
                    &[
                        KeyValue::new("lily.operation", "send"),
                        KeyValue::new("lily.outcome", "success"),
                    ],
                );
            }
            Err(error) => {
                let outcome = if error.is_timeout() {
                    self.metrics.timeouts.fetch_add(1, Ordering::Relaxed);
                    "timeout"
                } else if matches!(error, WebSocketError::Cancelled) {
                    self.metrics.cancellations.fetch_add(1, Ordering::Relaxed);
                    "cancelled"
                } else {
                    "error"
                };
                span.record("lily.outcome", outcome);
                span.record("lily.error_code", error.error_code());
                span.record("otel.status_code", "ERROR");
                self.metrics.operations.add(
                    1,
                    &[
                        KeyValue::new("lily.operation", "send"),
                        KeyValue::new("lily.outcome", outcome),
                    ],
                );
            }
        }
        self.metrics.message_duration.record(
            started.elapsed().as_secs_f64(),
            &[KeyValue::new("lily.direction", "outbound")],
        );
        result
    }

    async fn send_frame_inner(&self, message: Message) -> Result<(), WebSocketError> {
        let actual = message.len();
        if actual > self.config.max_message_size {
            return Err(WebSocketError::MessageTooLarge {
                actual,
                maximum: self.config.max_message_size,
            });
        }
        if !self.is_connected() {
            return Err(WebSocketError::NotConnected);
        }

        let sender = {
            let runtime = self
                .runtime
                .lock()
                .map_err(|_| WebSocketError::ConnectionError("runtime lock poisoned".into()))?;
            runtime
                .as_ref()
                .map(|runtime| runtime.outbound.clone())
                .ok_or(WebSocketError::NotConnected)?
        };
        let (acknowledgement, result) = oneshot::channel();
        let admission_started = Instant::now();
        let admission = sender.try_send(OutboundMessage {
            message,
            acknowledgement,
        });
        let wait = admission_started.elapsed();
        let depth = sender.max_capacity().saturating_sub(sender.capacity());
        self.metrics.queue_wait.record(wait.as_secs_f64(), &[]);
        self.metrics
            .queue_depth
            .record(u64::try_from(depth).unwrap_or(u64::MAX), &[]);
        tracing::Span::current().record("lily.queue_wait_ms", wait.as_secs_f64() * 1_000.0);
        admission.map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => {
                self.metrics
                    .outbound_queue_full
                    .fetch_add(1, Ordering::Relaxed);
                WebSocketError::Backpressure {
                    capacity: self.config.outbound_queue_capacity,
                }
            }
            mpsc::error::TrySendError::Closed(_) => {
                self.metrics
                    .outbound_queue_closed
                    .fetch_add(1, Ordering::Relaxed);
                WebSocketError::NotConnected
            }
        })?;

        let send_timeout = Duration::from_secs(self.config.send_timeout_secs);
        match timeout(send_timeout, result).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(OutboundFailure::Cancelled))) => Err(WebSocketError::Cancelled),
            Ok(Ok(Err(OutboundFailure::Timeout(duration)))) => {
                Err(WebSocketError::SendTimeout(duration))
            }
            Ok(Ok(Err(OutboundFailure::Transport(error)))) => Err(WebSocketError::SendError(error)),
            Ok(Err(_)) => Err(WebSocketError::NotConnected),
            Err(_) => Err(WebSocketError::SendTimeout(send_timeout)),
        }
    }

    fn queue_callback(
        callbacks: &DashMap<String, EventCallback>,
        sender: &mpsc::Sender<CallbackEvent>,
        capacity: usize,
        event: &str,
        payload: Vec<u8>,
    ) -> Result<(), WebSocketError> {
        if !callbacks.contains_key(event) && (event == "*" || !callbacks.contains_key("*")) {
            return Ok(());
        }

        sender
            .try_send(CallbackEvent {
                event: event.to_string(),
                payload,
                parent: tracing::Span::current(),
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => {
                    WebSocketError::CallbackBackpressure { capacity }
                }
                mpsc::error::TrySendError::Closed(_) => WebSocketError::Cancelled,
            })
    }

    fn set_state(state: &AtomicU8, value: u8) {
        state.store(value, Ordering::Release);
    }

    fn handshake_url(config: &WebSocketClientConfig) -> Result<Url, WebSocketError> {
        let mut url = Url::parse(&config.url)
            .map_err(|error| WebSocketError::InvalidConfiguration(error.to_string()))?;
        if let Some(namespace) = config.namespace.as_deref() {
            let has_namespace = url.query_pairs().any(|(name, _)| name == "namespace");
            if !has_namespace {
                url.query_pairs_mut().append_pair("namespace", namespace);
            }
        }
        Ok(url)
    }

    async fn establish_connection(context: &RuntimeContext) -> Result<Socket, WebSocketError> {
        let span = tracing::info_span!(
            "websocket.handshake",
            otel.kind = "client",
            lily.outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            lily.timeout_category = tracing::field::Empty,
            lily.cancellation_category = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let started = Instant::now();
        let result = Self::establish_connection_inner(context)
            .instrument(span.clone())
            .await;
        let outcome = match &result {
            Ok(_) => "success",
            Err(error) if error.is_timeout() => {
                context.metrics.timeouts.fetch_add(1, Ordering::Relaxed);
                span.record("lily.timeout_category", "handshake");
                "timeout"
            }
            Err(WebSocketError::Cancelled) => {
                context
                    .metrics
                    .cancellations
                    .fetch_add(1, Ordering::Relaxed);
                span.record("lily.cancellation_category", "shutdown");
                "cancelled"
            }
            Err(error) => {
                if matches!(
                    error,
                    WebSocketError::InvalidEnvelope
                        | WebSocketError::UnsupportedProtocolVersion { .. }
                        | WebSocketError::UnexpectedAcknowledgement
                        | WebSocketError::UnsupportedSubprotocol(_)
                        | WebSocketError::SubprotocolRequired
                ) {
                    context
                        .metrics
                        .protocol_failures
                        .fetch_add(1, Ordering::Relaxed);
                }
                if matches!(error, WebSocketError::Authentication(_)) {
                    context
                        .metrics
                        .authentication_failures
                        .fetch_add(1, Ordering::Relaxed);
                }
                "error"
            }
        };
        span.record("lily.outcome", outcome);
        if let Err(error) = &result {
            span.record("lily.error_code", error.error_code());
            span.record("otel.status_code", "ERROR");
        }
        context.metrics.operations.add(
            1,
            &[
                KeyValue::new("lily.operation", "handshake"),
                KeyValue::new("lily.outcome", outcome),
            ],
        );
        context.metrics.connection_duration.record(
            started.elapsed().as_secs_f64(),
            &[
                KeyValue::new("lily.phase", "handshake"),
                KeyValue::new("lily.outcome", outcome),
            ],
        );
        result
    }

    async fn establish_connection_inner(
        context: &RuntimeContext,
    ) -> Result<Socket, WebSocketError> {
        let url = Self::handshake_url(&context.config)?;
        let mut request = url.as_str().into_client_request()?;
        for (name, value) in &context.config.headers {
            Self::insert_header(&mut request, name, value)?;
        }

        if let Some(provider) = context.auth_headers.read().await.clone() {
            let auth_span = tracing::info_span!(
                "websocket.client.authentication",
                lily.outcome = tracing::field::Empty,
                lily.error_code = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
            );
            let provider_timeout = Duration::from_secs(context.config.connect_timeout_secs);
            let headers_result = async {
                tokio::select! {
                    _ = context.cancellation.cancelled() => Err(WebSocketError::Cancelled),
                    result = timeout(provider_timeout, provider.headers()) => match result {
                        Ok(result) => result,
                        Err(_) => Err(WebSocketError::Authentication(format!(
                            "credential provider timed out after {provider_timeout:?}"
                        ))),
                    }
                }
            }
            .instrument(auth_span.clone())
            .await;
            let headers = match headers_result {
                Ok(headers) => {
                    auth_span.record("lily.outcome", "success");
                    headers
                }
                Err(error) => {
                    auth_span.record("lily.outcome", "error");
                    auth_span.record("lily.error_code", error.error_code());
                    auth_span.record("otel.status_code", "ERROR");
                    return Err(error);
                }
            };
            if headers.len() > 64 {
                return Err(WebSocketError::Authentication(
                    "credential provider returned more than 64 headers".into(),
                ));
            }
            for (name, value) in headers {
                if value.len() > 8 * 1024 {
                    return Err(WebSocketError::Authentication(format!(
                        "credential header `{name}` exceeds the 8 KiB hard safety limit"
                    )));
                }
                Self::insert_header(&mut request, &name, &value)?;
            }
        }

        // The connect call captures one immutable W3C parent. Reconnects reuse
        // that same handshake parent instead of depending on whichever task
        // happens to be current inside the supervisor later.
        #[cfg(feature = "di")]
        Self::apply_trace_headers(&mut request, &context.trace_headers)?;

        if !context.config.subprotocols.is_empty() {
            let protocols = context.config.subprotocols.join(", ");
            request.headers_mut().insert(
                "sec-websocket-protocol",
                HeaderValue::from_str(&protocols).map_err(|error| {
                    WebSocketError::InvalidHeader {
                        name: "sec-websocket-protocol".into(),
                        reason: error.to_string(),
                    }
                })?,
            );
        }

        let write_buffer_size = context.config.max_frame_size.min(64 * 1024);
        let ws_config = WebSocketConfig {
            write_buffer_size,
            max_write_buffer_size: write_buffer_size
                .saturating_add(context.config.max_message_size),
            max_message_size: Some(context.config.max_message_size),
            max_frame_size: Some(context.config.max_frame_size),
            ..Default::default()
        };
        let connect_timeout = Duration::from_secs(context.config.connect_timeout_secs);
        let connector = Self::tls_connector(&context.config).await?;
        let connection = connect_async_tls_with_config(request, Some(ws_config), true, connector);
        let (socket, response) = tokio::select! {
            _ = context.cancellation.cancelled() => return Err(WebSocketError::Cancelled),
            result = timeout(connect_timeout, connection) => match result {
                Ok(result) => result.map_err(WebSocketError::from)?,
                Err(_) => return Err(WebSocketError::ConnectTimeout(connect_timeout)),
            },
        };

        let selected = response
            .headers()
            .get("sec-websocket-protocol")
            .map(|value| {
                value
                    .to_str()
                    .map_err(|error| WebSocketError::InvalidHeader {
                        name: "sec-websocket-protocol".into(),
                        reason: error.to_string(),
                    })
            })
            .transpose()?;
        if let Some(selected) = selected {
            if !context
                .config
                .subprotocols
                .iter()
                .any(|supported| supported == selected)
            {
                return Err(WebSocketError::UnsupportedSubprotocol(selected.to_string()));
            }
        } else if context.config.require_subprotocol {
            return Err(WebSocketError::SubprotocolRequired);
        }

        Ok(socket)
    }

    async fn tls_connector(
        config: &WebSocketClientConfig,
    ) -> Result<Option<Connector>, WebSocketError> {
        let Some(path) = config.additional_ca_bundle.as_ref() else {
            return Ok(None);
        };
        const MAX_CA_BUNDLE_BYTES: u64 = 4 * 1024 * 1024;
        let metadata = tokio::fs::metadata(path).await.map_err(|_| {
            WebSocketError::TlsValidation("additional CA bundle unavailable".into())
        })?;
        if !metadata.is_file() || metadata.len() == 0 || metadata.len() > MAX_CA_BUNDLE_BYTES {
            return Err(WebSocketError::TlsValidation(
                "additional CA bundle size or type is invalid".into(),
            ));
        }
        let bytes = tokio::fs::read(path).await.map_err(|_| {
            WebSocketError::TlsValidation("additional CA bundle unavailable".into())
        })?;
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let mut additional = 0usize;
        for item in <(SectionKind, Vec<u8>)>::pem_slice_iter(&bytes) {
            match item.map_err(|_| {
                WebSocketError::TlsValidation("additional CA bundle is malformed".into())
            })? {
                (SectionKind::Certificate, certificate) => {
                    roots.add(CertificateDer::from(certificate)).map_err(|_| {
                        WebSocketError::TlsValidation(
                            "additional CA bundle contains an invalid certificate".into(),
                        )
                    })?;
                    additional += 1;
                }
                (SectionKind::RsaPrivateKey, _)
                | (SectionKind::PrivateKey, _)
                | (SectionKind::EcPrivateKey, _) => {
                    return Err(WebSocketError::TlsValidation(
                        "additional CA bundle must not contain a private key".into(),
                    ));
                }
                _ => {
                    return Err(WebSocketError::TlsValidation(
                        "additional CA bundle may contain certificates only".into(),
                    ));
                }
            }
        }
        if additional == 0 {
            return Err(WebSocketError::TlsValidation(
                "additional CA bundle contains no certificates".into(),
            ));
        }
        let client = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        Ok(Some(Connector::Rustls(Arc::new(client))))
    }

    async fn reconnect_after(
        context: &RuntimeContext,
        attempt: usize,
        delay: Duration,
    ) -> Result<Socket, WebSocketError> {
        let span = tracing::info_span!(
            "websocket.client.reconnect",
            lily.delivery_attempt = u64::try_from(attempt).unwrap_or(u64::MAX),
            lily.backoff_ms = delay.as_secs_f64() * 1_000.0,
            lily.outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        context
            .metrics
            .reconnect_attempted
            .fetch_add(1, Ordering::Relaxed);
        let result = async {
            tokio::select! {
                _ = context.cancellation.cancelled() => Err(WebSocketError::Cancelled),
                _ = sleep(delay) => Self::establish_connection(context).await,
            }
        }
        .instrument(span.clone())
        .await;
        match &result {
            Ok(_) => {
                context
                    .metrics
                    .reconnect_succeeded
                    .fetch_add(1, Ordering::Relaxed);
                span.record("lily.outcome", "success");
            }
            Err(error) => {
                context
                    .metrics
                    .reconnect_failed
                    .fetch_add(1, Ordering::Relaxed);
                span.record(
                    "lily.outcome",
                    if matches!(error, WebSocketError::Cancelled) {
                        "cancelled"
                    } else if error.is_timeout() {
                        "timeout"
                    } else {
                        "error"
                    },
                );
                span.record("lily.error_code", error.error_code());
                span.record("otel.status_code", "ERROR");
            }
        }
        context.metrics.operations.add(
            1,
            &[
                KeyValue::new("lily.operation", "reconnect"),
                KeyValue::new(
                    "lily.outcome",
                    if result.is_ok() { "success" } else { "failure" },
                ),
            ],
        );
        result
    }

    async fn establish_initial_connection(
        context: &RuntimeContext,
    ) -> Result<Socket, WebSocketError> {
        let reconnect = context
            .reconnection
            .read()
            .map_err(|_| WebSocketError::ConnectionError("reconnection lock poisoned".into()))?
            .clone();
        let mut retry = 0usize;

        loop {
            let result = if retry > 0 {
                Self::set_state(&context.state, STATE_RECONNECTING);
                context.reconnect_attempt.store(retry, Ordering::Release);
                let delay = reconnect.calculate_delay_with_jitter(retry - 1);
                Self::reconnect_after(context, retry, delay).await
            } else {
                Self::establish_connection(context).await
            };

            match result {
                Ok(socket) => return Ok(socket),
                Err(WebSocketError::Cancelled) => return Err(WebSocketError::Cancelled),
                Err(error) if !reconnect.enabled => return Err(error),
                Err(error) => {
                    if retry >= reconnect.max_retries {
                        return Err(WebSocketError::ReconnectionFailed(error.to_string()));
                    }
                    retry += 1;
                }
            }
        }
    }

    fn insert_header(
        request: &mut tokio_tungstenite::tungstenite::handshake::client::Request,
        name: &str,
        value: &str,
    ) -> Result<(), WebSocketError> {
        let lower = name.to_ascii_lowercase();
        if matches!(
            lower.as_str(),
            "connection"
                | "host"
                | "upgrade"
                | "sec-websocket-key"
                | "sec-websocket-version"
                | "sec-websocket-protocol"
                | "sec-websocket-extensions"
        ) {
            return Err(WebSocketError::InvalidHeader {
                name: name.to_string(),
                reason: "reserved WebSocket handshake header".into(),
            });
        }
        let name = HeaderName::from_bytes(name.as_bytes()).map_err(|error| {
            WebSocketError::InvalidHeader {
                name: name.to_string(),
                reason: error.to_string(),
            }
        })?;
        let value =
            HeaderValue::from_str(value).map_err(|error| WebSocketError::InvalidHeader {
                name: name.to_string(),
                reason: error.to_string(),
            })?;
        request.headers_mut().insert(name, value);
        Ok(())
    }

    #[cfg(feature = "di")]
    fn capture_trace_headers() -> Arc<std::collections::HashMap<String, String>> {
        let mut headers = std::collections::HashMap::new();
        lily_trace::inject_current_context(&mut headers);
        Arc::new(headers)
    }

    #[cfg(feature = "di")]
    fn apply_trace_headers(
        request: &mut tokio_tungstenite::tungstenite::handshake::client::Request,
        headers: &std::collections::HashMap<String, String>,
    ) -> Result<(), WebSocketError> {
        for (name, value) in headers {
            if matches!(name.as_str(), "traceparent" | "tracestate") {
                Self::insert_header(request, name, value)?;
            }
        }
        Ok(())
    }

    async fn invoke_callback(
        callback: EventCallback,
        event: String,
        payload: Vec<u8>,
        permit: tokio::sync::OwnedSemaphorePermit,
        callback_timeout: Duration,
        metrics: Arc<ClientMetrics>,
    ) {
        let started = Instant::now();
        let outcome = match callback {
            EventCallback::Sync(callback) => {
                // The permit is moved into the blocking task. If the callback
                // exceeds its budget the task cannot be forcefully stopped,
                // but it continues to consume one of the configured callback
                // slots. This prevents repeated slow callbacks from creating
                // an unbounded blocking-task leak.
                let task = tokio::task::spawn_blocking(move || {
                    let _permit = permit;
                    catch_unwind(AssertUnwindSafe(|| callback(event, payload))).is_err()
                });
                match timeout(callback_timeout, task).await {
                    Ok(Ok(false)) => {
                        metrics.callback_completed.fetch_add(1, Ordering::Relaxed);
                        "success"
                    }
                    Ok(Ok(true)) => {
                        metrics.callback_panicked.fetch_add(1, Ordering::Relaxed);
                        "panic"
                    }
                    Ok(Err(_)) => {
                        metrics.callback_join_failed.fetch_add(1, Ordering::Relaxed);
                        "join_error"
                    }
                    Err(_) => {
                        metrics.callback_timed_out.fetch_add(1, Ordering::Relaxed);
                        "timeout"
                    }
                }
            }
            EventCallback::Async(callback) => {
                let mut task = tokio::spawn(async move {
                    let _permit = permit;
                    callback(event, payload).await;
                });
                match timeout(callback_timeout, &mut task).await {
                    Ok(Ok(())) => {
                        metrics.callback_completed.fetch_add(1, Ordering::Relaxed);
                        "success"
                    }
                    Ok(Err(_)) => {
                        metrics.callback_join_failed.fetch_add(1, Ordering::Relaxed);
                        "join_error"
                    }
                    Err(_) => {
                        metrics.callback_timed_out.fetch_add(1, Ordering::Relaxed);
                        task.abort();
                        if let Err(error) = task.await {
                            if !error.is_cancelled() {
                                metrics.callback_join_failed.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        "timeout"
                    }
                }
            }
        };
        metrics.message_duration.record(
            started.elapsed().as_secs_f64(),
            &[
                KeyValue::new("lily.direction", "inbound"),
                KeyValue::new("lily.outcome", outcome),
            ],
        );
        metrics.operations.add(
            1,
            &[
                KeyValue::new("lily.operation", "handler"),
                KeyValue::new("lily.outcome", outcome),
            ],
        );
        let span = tracing::Span::current();
        span.record("lily.outcome", outcome);
        if outcome != "success" {
            span.record("lily.error_code", "CALLBACK_ERROR");
            span.record("otel.status_code", "ERROR");
        }
    }

    async fn run_callback_dispatcher(
        mut receiver: mpsc::Receiver<CallbackEvent>,
        callbacks: Arc<DashMap<String, EventCallback>>,
        concurrency: usize,
        callback_timeout: Duration,
        cancellation: CancellationToken,
        metrics: Arc<ClientMetrics>,
    ) {
        let permits = Arc::new(Semaphore::new(concurrency));
        let mut tasks = tokio::task::JoinSet::new();

        'dispatcher: loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break,
                completed = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(Err(_)) = completed {
                        metrics.callback_join_failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                event = receiver.recv() => {
                    let Some(event) = event else { break; };
                    let mut event_callbacks = Vec::with_capacity(2);
                    if let Some(callback) = callbacks.get(&event.event) {
                        event_callbacks.push(callback.clone());
                    }
                    if event.event != "*" {
                        if let Some(callback) = callbacks.get("*") {
                            event_callbacks.push(callback.clone());
                        }
                    }

                    for callback in event_callbacks {
                        let permit = tokio::select! {
                            _ = cancellation.cancelled() => break 'dispatcher,
                            permit = permits.clone().acquire_owned() => match permit {
                                Ok(permit) => permit,
                                Err(_) => break 'dispatcher,
                            },
                        };
                        let handler_span = tracing::info_span!(
                            parent: &event.parent,
                            "websocket.message.handler",
                            lily.direction = "inbound",
                            lily.outcome = tracing::field::Empty,
                            lily.error_code = tracing::field::Empty,
                            otel.status_code = tracing::field::Empty,
                        );
                        tasks.spawn(
                            Self::invoke_callback(
                                callback,
                                event.event.clone(),
                                event.payload.clone(),
                                permit,
                                callback_timeout,
                                Arc::clone(&metrics),
                            )
                            .instrument(handler_span),
                        );
                    }
                }
            }
        }

        tasks.abort_all();
        while let Some(result) = tasks.join_next().await {
            if let Err(error) = result {
                if !error.is_cancelled() {
                    metrics.callback_join_failed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    async fn run_supervisor(
        socket: Socket,
        outbound: mpsc::Receiver<OutboundMessage>,
        context: RuntimeContext,
    ) -> Result<(), WebSocketError> {
        let _pending_acknowledgement_drain =
            PendingAcknowledgementDrain(Arc::clone(&context.pending_acknowledgements));
        let (callback_sender, callback_receiver) =
            mpsc::channel(context.config.callback_queue_capacity);
        let callback_cancellation = context.cancellation.child_token();
        let callback_timeout = Duration::from_secs(context.config.callback_timeout_secs);
        let mut callback_task = tokio::spawn(Self::run_callback_dispatcher(
            callback_receiver,
            Arc::clone(&context.callbacks),
            context.config.callback_concurrency,
            callback_timeout,
            callback_cancellation.clone(),
            Arc::clone(&context.metrics),
        ));

        let mut result =
            Self::run_connection_supervisor(socket, outbound, &context, &callback_sender).await;
        drop(callback_sender);
        callback_cancellation.cancel();

        let shutdown_timeout = Duration::from_secs(context.config.shutdown_timeout_secs);
        match timeout(shutdown_timeout, &mut callback_task).await {
            Err(_) => {
                callback_task.abort();
                if let Err(error) = callback_task.await {
                    if !error.is_cancelled() {
                        context
                            .metrics
                            .callback_join_failed
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                if result.is_ok() {
                    result = Err(WebSocketError::ShutdownTimeout(shutdown_timeout));
                }
            }
            Ok(Err(error)) if result.is_ok() => {
                result = Err(WebSocketError::ConnectionError(format!(
                    "callback dispatcher join failed: {error}"
                )));
            }
            Ok(_) => {}
        }

        Self::set_state(
            &context.state,
            if context.cancellation.is_cancelled() || result.is_ok() {
                STATE_DISCONNECTED
            } else {
                STATE_FAILED
            },
        );
        result
    }

    async fn run_connection_supervisor(
        mut socket: Socket,
        mut outbound: mpsc::Receiver<OutboundMessage>,
        context: &RuntimeContext,
        callback_sender: &mpsc::Sender<CallbackEvent>,
    ) -> Result<(), WebSocketError> {
        let mut attempt = 0usize;
        loop {
            let connected_at = Instant::now();
            let active_connection = ActiveClientConnectionGuard::new(Arc::clone(&context.metrics));
            Self::set_state(&context.state, STATE_CONNECTED);
            context.reconnect_attempt.store(0, Ordering::Release);
            if let Err(error) = Self::queue_callback(
                &context.callbacks,
                callback_sender,
                context.config.callback_queue_capacity,
                "on_connect",
                Vec::new(),
            ) {
                let _close_result = Self::graceful_close_recorded(
                    &mut socket,
                    CloseFrame {
                        code: CloseCode::Error,
                        reason: "callback overload".into(),
                    },
                    Duration::from_secs(context.config.close_timeout_secs),
                    &context.metrics,
                )
                .await;
                return Err(error);
            }
            let result =
                Self::run_socket(&mut socket, &mut outbound, context, callback_sender).await;
            Self::fail_pending_acknowledgements(&context.pending_acknowledgements);
            drop(active_connection);
            context.metrics.connection_duration.record(
                connected_at.elapsed().as_secs_f64(),
                &[KeyValue::new(
                    "lily.outcome",
                    if result.is_ok() { "closed" } else { "error" },
                )],
            );
            // A successful handshake followed by an immediate disconnect must
            // not reset the retry budget. Otherwise a flapping peer can keep a
            // client in an infinite reconnect loop despite `max_retries`.
            if connected_at.elapsed() >= Duration::from_secs(context.config.ping_interval_secs) {
                attempt = 0;
            }
            Self::set_state(&context.state, STATE_DISCONNECTED);
            if Self::queue_callback(
                &context.callbacks,
                callback_sender,
                context.config.callback_queue_capacity,
                "on_disconnect",
                Vec::new(),
            )
            .is_err()
            {
                context
                    .metrics
                    .callback_rejected
                    .fetch_add(1, Ordering::Relaxed);
            }

            if context.cancellation.is_cancelled() {
                return result;
            }

            if let Err(error) = &result {
                if Self::queue_callback(
                    &context.callbacks,
                    callback_sender,
                    context.config.callback_queue_capacity,
                    "on_error",
                    error.to_string().into_bytes(),
                )
                .is_err()
                {
                    context
                        .metrics
                        .callback_rejected
                        .fetch_add(1, Ordering::Relaxed);
                }
            }

            let reconnect = context
                .reconnection
                .read()
                .map_err(|_| WebSocketError::ConnectionError("reconnection lock poisoned".into()))?
                .clone();
            if !reconnect.enabled {
                Self::set_state(
                    &context.state,
                    if result.is_err() {
                        STATE_FAILED
                    } else {
                        STATE_DISCONNECTED
                    },
                );
                return result;
            }

            let mut last_error = result.err().map(|error| error.to_string());
            loop {
                if attempt >= reconnect.max_retries {
                    Self::set_state(&context.state, STATE_FAILED);
                    context.reconnect_attempt.store(attempt, Ordering::Release);
                    return Err(WebSocketError::ReconnectionFailed(
                        last_error.unwrap_or_else(|| {
                            format!(
                                "peer closed and retry limit ({}) was reached",
                                reconnect.max_retries
                            )
                        }),
                    ));
                }

                Self::set_state(&context.state, STATE_RECONNECTING);
                context
                    .reconnect_attempt
                    .store(attempt + 1, Ordering::Release);
                let delay = reconnect.calculate_delay_with_jitter(attempt);
                attempt += 1;
                match Self::reconnect_after(context, attempt, delay).await {
                    Ok(new_socket) => {
                        socket = new_socket;
                        break;
                    }
                    Err(WebSocketError::Cancelled) => return Ok(()),
                    Err(error) => {
                        last_error = Some(error.to_string());
                        if Self::queue_callback(
                            &context.callbacks,
                            callback_sender,
                            context.config.callback_queue_capacity,
                            "on_error",
                            error.to_string().into_bytes(),
                        )
                        .is_err()
                        {
                            context
                                .metrics
                                .callback_rejected
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        }
    }

    async fn graceful_close<S>(
        socket: &mut WebSocketStream<S>,
        frame: CloseFrame<'static>,
        close_timeout: Duration,
    ) -> Result<(), WebSocketError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let handshake = async {
            socket.send(Message::Close(Some(frame))).await?;
            loop {
                match socket.next().await {
                    Some(Ok(Message::Close(_))) | None => return Ok(()),
                    Some(Ok(_)) => {}
                    Some(Err(
                        tokio_tungstenite::tungstenite::Error::ConnectionClosed
                        | tokio_tungstenite::tungstenite::Error::AlreadyClosed,
                    )) => return Ok(()),
                    Some(Err(error)) => return Err(WebSocketError::from(error)),
                }
            }
        };

        timeout(close_timeout, handshake)
            .await
            .map_err(|_| WebSocketError::CloseTimeout(close_timeout))?
    }

    async fn graceful_close_recorded<S>(
        socket: &mut WebSocketStream<S>,
        frame: CloseFrame<'static>,
        close_timeout: Duration,
        metrics: &ClientMetrics,
    ) -> Result<(), WebSocketError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let result = Self::graceful_close(socket, frame, close_timeout).await;
        if result.is_err() {
            metrics.close_failed.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    async fn send_control<S>(
        socket: &mut WebSocketStream<S>,
        message: Message,
        send_timeout: Duration,
    ) -> Result<(), WebSocketError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        timeout(send_timeout, socket.send(message))
            .await
            .map_err(|_| WebSocketError::SendTimeout(send_timeout))??;
        Ok(())
    }

    async fn run_socket<S>(
        socket: &mut WebSocketStream<S>,
        outbound: &mut mpsc::Receiver<OutboundMessage>,
        context: &RuntimeContext,
        callback_sender: &mpsc::Sender<CallbackEvent>,
    ) -> Result<(), WebSocketError>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut heartbeat = interval(Duration::from_secs(context.config.ping_interval_secs));
        heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
        heartbeat.tick().await;
        let idle_timeout = Duration::from_secs(context.config.idle_timeout_secs);
        let pong_timeout = Duration::from_secs(context.config.pong_timeout_secs);
        let send_timeout = Duration::from_secs(context.config.send_timeout_secs);
        let close_timeout = Duration::from_secs(context.config.close_timeout_secs);
        let mut idle_deadline = Box::pin(tokio::time::sleep(idle_timeout));
        let mut pong_deadline = Box::pin(tokio::time::sleep(pong_timeout));
        let mut expected_pong: Option<Vec<u8>> = None;

        loop {
            tokio::select! {
                _ = context.cancellation.cancelled() => {
                    return Self::graceful_close_recorded(socket, CloseFrame {
                        code: CloseCode::Normal,
                        reason: "client shutdown".into(),
                    }, close_timeout, &context.metrics).await;
                }
                command = outbound.recv() => match command {
                    Some(command) => {
                        // The caller may have reached its send deadline while
                        // this command waited behind flow control. Do not write
                        // a frame after the observable operation has failed.
                        if command.acknowledgement.is_closed() {
                            continue;
                        }
                        let write = tokio::select! {
                            _ = context.cancellation.cancelled() => {
                                Err(OutboundFailure::Cancelled)
                            }
                            result = timeout(send_timeout, socket.send(command.message)) => {
                                match result {
                                    Ok(Ok(())) => Ok(()),
                                    Ok(Err(error)) => Err(OutboundFailure::Transport(error.to_string())),
                                    Err(_) => Err(OutboundFailure::Timeout(send_timeout)),
                                }
                            }
                        };
                        let terminal_error = match &write {
                            Err(OutboundFailure::Cancelled) => Some(WebSocketError::Cancelled),
                            Err(OutboundFailure::Timeout(duration)) => Some(WebSocketError::SendTimeout(*duration)),
                            Err(OutboundFailure::Transport(error)) => Some(WebSocketError::SendError(error.clone())),
                            Ok(()) => None,
                        };
                        if command.acknowledgement.send(write).is_err() {
                            context
                                .metrics
                                .acknowledgement_abandoned
                                .fetch_add(1, Ordering::Relaxed);
                        }
                        if let Some(error) = terminal_error {
                            if context.cancellation.is_cancelled() {
                                return Self::graceful_close_recorded(socket, CloseFrame {
                                    code: CloseCode::Normal,
                                    reason: "client shutdown".into(),
                                }, close_timeout, &context.metrics).await;
                            }
                            return Err(error);
                        }
                    }
                    None => return Ok(()),
                },
                incoming = socket.next() => match incoming {
                    Some(Ok(message)) => {
                        idle_deadline.as_mut().reset(Instant::now() + idle_timeout);
                        match message {
                            Message::Text(text) => {
                                if let Err(error) = Self::dispatch_envelope(
                                    &context.callbacks,
                                    &context.pending_acknowledgements,
                                    callback_sender,
                                    context.config.callback_queue_capacity,
                                    text.as_bytes(),
                                    &context.metrics,
                                ) {
                                    let (code, reason) = if matches!(
                                        &error,
                                        WebSocketError::UnsupportedProtocolVersion { .. }
                                            | WebSocketError::InvalidEnvelope
                                            | WebSocketError::UnexpectedAcknowledgement
                                    ) {
                                        (CloseCode::Protocol, "invalid lily.v2 envelope")
                                    } else {
                                        (CloseCode::Error, "callback dispatch failure")
                                    };
                                    context
                                        .metrics
                                        .callback_rejected
                                        .fetch_add(1, Ordering::Relaxed);
                                    let _close_result = Self::graceful_close_recorded(
                                        socket,
                                        CloseFrame {
                                            code,
                                            reason: reason.into(),
                                        },
                                        close_timeout,
                                        &context.metrics,
                                    )
                                    .await;
                                    return Err(error);
                                }
                            }
                            Message::Binary(data) => {
                                if let Err(error) = Self::dispatch_envelope(
                                    &context.callbacks,
                                    &context.pending_acknowledgements,
                                    callback_sender,
                                    context.config.callback_queue_capacity,
                                    &data,
                                    &context.metrics,
                                ) {
                                    let (code, reason) = if matches!(
                                        &error,
                                        WebSocketError::UnsupportedProtocolVersion { .. }
                                            | WebSocketError::InvalidEnvelope
                                            | WebSocketError::UnexpectedAcknowledgement
                                    ) {
                                        (CloseCode::Protocol, "invalid lily.v2 envelope")
                                    } else {
                                        (CloseCode::Error, "callback dispatch failure")
                                    };
                                    context
                                        .metrics
                                        .callback_rejected
                                        .fetch_add(1, Ordering::Relaxed);
                                    let _close_result = Self::graceful_close_recorded(
                                        socket,
                                        CloseFrame {
                                            code,
                                            reason: reason.into(),
                                        },
                                        close_timeout,
                                        &context.metrics,
                                    )
                                    .await;
                                    return Err(error);
                                }
                            }
                            Message::Ping(data) => Self::send_control(
                                socket,
                                Message::Pong(data),
                                send_timeout,
                            ).await?,
                            Message::Pong(payload) => {
                                if expected_pong.as_ref() == Some(&payload) {
                                    expected_pong = None;
                                }
                            }
                            Message::Close(_) => {
                                return match timeout(close_timeout, socket.flush()).await {
                                    Ok(Ok(())) => Ok(()),
                                    Ok(Err(error)) => Err(WebSocketError::from(error)),
                                    Err(_) => Err(WebSocketError::CloseTimeout(close_timeout)),
                                };
                            }
                            Message::Frame(_) => {}
                        }
                    }
                    Some(Err(error)) => return Err(WebSocketError::ReceiveError(error.to_string())),
                    None => return Ok(()),
                },
                _ = &mut idle_deadline => {
                    let _close_result = Self::graceful_close_recorded(socket, CloseFrame {
                        code: CloseCode::Away,
                        reason: "idle timeout".into(),
                    }, close_timeout, &context.metrics).await;
                    return Err(WebSocketError::IdleTimeout(idle_timeout));
                }
                _ = &mut pong_deadline, if expected_pong.is_some() => {
                    let _close_result = Self::graceful_close_recorded(socket, CloseFrame {
                        code: CloseCode::Away,
                        reason: "pong timeout".into(),
                    }, close_timeout, &context.metrics).await;
                    return Err(WebSocketError::PongTimeout(pong_timeout));
                }
                _ = heartbeat.tick() => {
                    if expected_pong.is_none() {
                        let payload = rand::random::<u64>().to_be_bytes().to_vec();
                        Self::send_control(socket, Message::Ping(payload.clone()), send_timeout).await?;
                        expected_pong = Some(payload);
                        pong_deadline.as_mut().reset(Instant::now() + pong_timeout);
                    }
                }
            }
        }
    }

    fn dispatch_envelope(
        callbacks: &DashMap<String, EventCallback>,
        pending_acknowledgements: &PendingAcknowledgements,
        callback_sender: &mpsc::Sender<CallbackEvent>,
        callback_capacity: usize,
        bytes: &[u8],
        metrics: &ClientMetrics,
    ) -> Result<(), WebSocketError> {
        let span = tracing::info_span!(
            "websocket.message",
            lily.direction = "inbound",
            lily.message_size = bytes.len(),
            lily.message_id = tracing::field::Empty,
            lily.protocol_version = tracing::field::Empty,
            lily.namespace = tracing::field::Empty,
            lily.action = tracing::field::Empty,
            lily.outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let started = Instant::now();
        let result = span.in_scope(|| {
            let envelope = serde_json::from_slice::<WsMessage<Value>>(bytes)
                .map_err(|_| WebSocketError::InvalidEnvelope)?;
            span.record(
                "lily.protocol_version",
                u64::from(envelope.protocol_version),
            );
            if envelope.protocol_version != LILY_WEBSOCKET_PROTOCOL_VERSION {
                return Err(WebSocketError::UnsupportedProtocolVersion {
                    actual: envelope.protocol_version,
                    expected: LILY_WEBSOCKET_PROTOCOL_VERSION,
                });
            }
            if !envelope.is_canonical() {
                return Err(WebSocketError::InvalidEnvelope);
            }
            if let Some(message_id) = envelope.message_id.as_deref() {
                span.record("lily.message_id", message_id);
            }
            if let Some(namespace) = envelope.namespace.as_deref() {
                span.record("lily.namespace", namespace);
            }
            span.record("lily.action", envelope.event.as_str());
            match envelope.msg_type {
                MessageType::Event => {
                    let payload = envelope
                        .decoded_payload()
                        .and_then(|payload| payload.into_callback_bytes())
                        .ok_or(WebSocketError::InvalidEnvelope)?;
                    Self::queue_callback(
                        callbacks,
                        callback_sender,
                        callback_capacity,
                        &envelope.event,
                        payload,
                    )
                }
                MessageType::Ack => {
                    let acknowledgement_id = envelope
                        .ack_id
                        .as_deref()
                        .ok_or(WebSocketError::InvalidEnvelope)?;
                    let payload = envelope
                        .decoded_payload()
                        .ok_or(WebSocketError::InvalidEnvelope)?;
                    Self::resolve_pending_acknowledgement(
                        pending_acknowledgements,
                        acknowledgement_id,
                        WebSocketReply::Acknowledgement(payload),
                    )
                }
                MessageType::Error => {
                    // Keep-open server errors use the client's existing
                    // lifecycle/error callback instead of masquerading as the
                    // action event named by the envelope.
                    if let Some(acknowledgement_id) = envelope.ack_id.as_deref() {
                        let payload = envelope
                            .decoded_payload()
                            .ok_or(WebSocketError::InvalidEnvelope)?;
                        Self::resolve_pending_acknowledgement(
                            pending_acknowledgements,
                            acknowledgement_id,
                            WebSocketReply::Rejection(payload),
                        )
                    } else {
                        let payload = envelope
                            .decoded_payload()
                            .and_then(|payload| payload.into_callback_bytes())
                            .ok_or(WebSocketError::InvalidEnvelope)?;
                        Self::queue_callback(
                            callbacks,
                            callback_sender,
                            callback_capacity,
                            "on_error",
                            payload,
                        )
                    }
                }
                MessageType::Connect | MessageType::Disconnect => {
                    Err(WebSocketError::InvalidEnvelope)
                }
            }
        });
        let outcome = match &result {
            Ok(()) => {
                metrics.inbound_messages.fetch_add(1, Ordering::Relaxed);
                "success"
            }
            Err(error) => {
                if matches!(
                    error,
                    WebSocketError::InvalidEnvelope
                        | WebSocketError::UnsupportedProtocolVersion { .. }
                ) {
                    metrics.protocol_failures.fetch_add(1, Ordering::Relaxed);
                }
                span.record("lily.error_code", error.error_code());
                span.record("otel.status_code", "ERROR");
                if matches!(error, WebSocketError::CallbackBackpressure { .. }) {
                    "backpressure"
                } else {
                    "error"
                }
            }
        };
        span.record("lily.outcome", outcome);
        metrics.message_duration.record(
            started.elapsed().as_secs_f64(),
            &[
                KeyValue::new("lily.direction", "inbound"),
                KeyValue::new("lily.outcome", outcome),
            ],
        );
        metrics.operations.add(
            1,
            &[
                KeyValue::new("lily.operation", "receive"),
                KeyValue::new("lily.outcome", outcome),
            ],
        );
        result
    }

    fn resolve_pending_acknowledgement(
        pending: &PendingAcknowledgements,
        acknowledgement_id: &str,
        reply: WebSocketReply,
    ) -> Result<(), WebSocketError> {
        let Some((_, pending)) = pending.remove(acknowledgement_id) else {
            return Err(WebSocketError::UnexpectedAcknowledgement);
        };
        pending
            .sender
            .send(Ok(reply))
            .map_err(|_| WebSocketError::UnexpectedAcknowledgement)
    }

    fn fail_pending_acknowledgements(pending: &PendingAcknowledgements) {
        let acknowledgement_ids = pending
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for acknowledgement_id in acknowledgement_ids {
            if let Some((_, pending)) = pending.remove(&acknowledgement_id) {
                let _ = pending
                    .sender
                    .send(Err(WebSocketError::AcknowledgementConnectionLost));
            }
        }
    }
}

#[async_trait]
impl WsClient for TokioWsClient {
    async fn connect(&self, ct: CancellationToken) -> Result<(), WebSocketError> {
        let span = tracing::info_span!(
            "websocket.client.connect",
            otel.kind = "client",
            lily.outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            lily.timeout_category = tracing::field::Empty,
            lily.cancellation_category = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        self.metrics
            .connect_attempted
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.operations.add(
            1,
            &[
                KeyValue::new("lily.operation", "connect"),
                KeyValue::new("lily.outcome", "attempted"),
            ],
        );
        let result = async {
            let _lifecycle = self.lifecycle.lock().await;
            if ct.is_cancelled() {
                return Err(WebSocketError::Cancelled);
            }

            let stale = {
                let mut runtime = self
                    .runtime
                    .lock()
                    .map_err(|_| WebSocketError::ConnectionError("runtime lock poisoned".into()))?;
                if runtime
                    .as_ref()
                    .is_some_and(|runtime| !runtime.join.is_finished())
                {
                    return if self.is_connected() {
                        Ok(())
                    } else {
                        Err(WebSocketError::AlreadyConnected)
                    };
                }
                runtime.take()
            };
            if let Some(stale) = stale {
                match stale.join.await {
                    Ok(Ok(())) => {}
                    Ok(Err(_)) | Err(_) => {
                        self.metrics
                            .runtime_join_failed
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            Self::set_state(&self.state, STATE_CONNECTING);
            let cancellation = ct.child_token();
            let context = RuntimeContext {
                config: self.config.clone(),
                state: Arc::clone(&self.state),
                reconnect_attempt: Arc::clone(&self.reconnect_attempt),
                reconnection: Arc::clone(&self.reconnection_config),
                callbacks: Arc::clone(&self.event_callbacks),
                pending_acknowledgements: Arc::clone(&self.pending_acknowledgements),
                auth_headers: Arc::clone(&self.auth_headers),
                #[cfg(feature = "di")]
                trace_headers: Self::capture_trace_headers(),
                cancellation: cancellation.clone(),
                metrics: Arc::clone(&self.metrics),
            };
            let socket = match Self::establish_initial_connection(&context).await {
                Ok(socket) => socket,
                Err(error) => {
                    Self::set_state(&self.state, STATE_FAILED);
                    return Err(error);
                }
            };
            let (outbound, receiver) = mpsc::channel(self.config.outbound_queue_capacity);
            let (start, started) = oneshot::channel();
            let connection_span = tracing::info_span!(
                "websocket.connection",
                otel.kind = "client",
                lily.outcome = tracing::field::Empty,
                lily.close_category = tracing::field::Empty,
                lily.error_code = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
            );
            let join = tokio::spawn(
                async move {
                    if started.await.is_err() {
                        return Err(WebSocketError::Cancelled);
                    }
                    Self::run_supervisor(socket, receiver, context).await
                }
                .instrument(connection_span),
            );
            *self
                .runtime
                .lock()
                .map_err(|_| WebSocketError::ConnectionError("runtime lock poisoned".into()))? =
                Some(RuntimeHandle {
                    cancellation,
                    outbound,
                    join,
                });
            Self::set_state(&self.state, STATE_CONNECTED);
            if start.send(()).is_err() {
                let runtime = self
                    .runtime
                    .lock()
                    .map_err(|_| WebSocketError::ConnectionError("runtime lock poisoned".into()))?
                    .take();
                if let Some(runtime) = runtime {
                    match runtime.join.await {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) | Err(_) => {
                            self.metrics
                                .runtime_join_failed
                                .fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                Self::set_state(&self.state, STATE_FAILED);
                return Err(WebSocketError::ConnectionError(
                    "WebSocket runtime stopped before startup publication".into(),
                ));
            }
            Ok(())
        }
        .instrument(span.clone())
        .await;
        match &result {
            Ok(()) => {
                span.record("lily.outcome", "success");
                self.metrics
                    .connect_succeeded
                    .fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                let outcome = if error.is_timeout() {
                    self.metrics.timeouts.fetch_add(1, Ordering::Relaxed);
                    span.record("lily.timeout_category", "connect");
                    "timeout"
                } else if matches!(error, WebSocketError::Cancelled) {
                    self.metrics.cancellations.fetch_add(1, Ordering::Relaxed);
                    span.record("lily.cancellation_category", "caller_cancelled");
                    "cancelled"
                } else {
                    "error"
                };
                if matches!(error, WebSocketError::Authentication(_)) {
                    self.metrics
                        .authentication_failures
                        .fetch_add(1, Ordering::Relaxed);
                }
                self.metrics.connect_failed.fetch_add(1, Ordering::Relaxed);
                span.record("lily.outcome", outcome);
                span.record("lily.error_code", error.error_code());
                span.record("otel.status_code", "ERROR");
            }
        }
        result
    }

    async fn disconnect(&self) -> Result<(), WebSocketError> {
        let span = tracing::info_span!(
            "websocket.client.shutdown",
            lily.outcome = tracing::field::Empty,
            lily.close_category = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let _lifecycle = self.lifecycle.lock().await;
        let runtime = self
            .runtime
            .lock()
            .map_err(|_| WebSocketError::ConnectionError("runtime lock poisoned".into()))?
            .take();
        let Some(mut runtime) = runtime else {
            Self::set_state(&self.state, STATE_DISCONNECTED);
            span.record("lily.outcome", "already_closed");
            return Ok(());
        };
        runtime.cancellation.cancel();
        let shutdown_timeout = Duration::from_secs(self.config.shutdown_timeout_secs);
        let result = match timeout(shutdown_timeout, &mut runtime.join)
            .instrument(span.clone())
            .await
        {
            Ok(Ok(Ok(()))) => {
                Self::set_state(&self.state, STATE_DISCONNECTED);
                Ok(())
            }
            Ok(Ok(Err(error))) => {
                Self::set_state(&self.state, STATE_DISCONNECTED);
                Err(error)
            }
            Ok(Err(error)) => {
                Self::set_state(&self.state, STATE_DISCONNECTED);
                Err(WebSocketError::ConnectionError(format!(
                    "WebSocket runtime join failed: {error}"
                )))
            }
            Err(_) => {
                runtime.join.abort();
                if let Err(error) = runtime.join.await {
                    if !error.is_cancelled() {
                        self.metrics
                            .runtime_join_failed
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                Self::set_state(&self.state, STATE_DISCONNECTED);
                Err(WebSocketError::ShutdownTimeout(shutdown_timeout))
            }
        };
        match &result {
            Ok(()) => {
                self.metrics.graceful_closes.fetch_add(1, Ordering::Relaxed);
                span.record("lily.outcome", "success");
                span.record("lily.close_category", "graceful");
            }
            Err(error) => {
                self.metrics.forced_closes.fetch_add(1, Ordering::Relaxed);
                span.record("lily.outcome", "error");
                span.record("lily.close_category", "forced");
                span.record("lily.error_code", error.error_code());
                span.record("otel.status_code", "ERROR");
            }
        }
        result
    }

    async fn send<T: Serialize + Send>(&self, event: &str, data: T) -> Result<(), WebSocketError> {
        self.send_event(event, data).await
    }

    async fn request<T: Serialize + Send>(
        &self,
        event: &str,
        data: T,
        acknowledgement_timeout: Duration,
    ) -> Result<WebSocketReply, WebSocketError> {
        if acknowledgement_timeout.is_zero() {
            return Err(WebSocketError::InvalidAcknowledgementTimeout);
        }

        let permit = Arc::clone(&self.pending_acknowledgement_permits)
            .try_acquire_owned()
            .map_err(|_| WebSocketError::PendingAcknowledgementLimit {
                capacity: self.config.outbound_queue_capacity,
            })?;
        let (acknowledgement_id, receiver) = loop {
            let sequence = self.next_acknowledgement_id.fetch_add(1, Ordering::Relaxed);
            let acknowledgement_id = format!("lily-{sequence:016x}");
            match self
                .pending_acknowledgements
                .entry(acknowledgement_id.clone())
            {
                Entry::Vacant(entry) => {
                    let (sender, receiver) = oneshot::channel();
                    entry.insert(PendingAcknowledgement {
                        sender,
                        _permit: permit,
                    });
                    break (acknowledgement_id, receiver);
                }
                Entry::Occupied(_) => continue,
            }
        };
        let _pending_guard = PendingAcknowledgementGuard {
            acknowledgement_id: acknowledgement_id.clone(),
            pending: Arc::clone(&self.pending_acknowledgements),
        };

        let envelope = WsMessage::new_event(event.to_owned(), data).with_ack_id(acknowledgement_id);
        self.send_envelope(envelope).await?;

        match timeout(acknowledgement_timeout, receiver).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(WebSocketError::AcknowledgementWaiterClosed),
            Err(_) => Err(WebSocketError::AcknowledgementTimeout(
                acknowledgement_timeout,
            )),
        }
    }

    async fn send_text_event(&self, event: &str, text: String) -> Result<(), WebSocketError> {
        self.send_envelope(WsMessage::new_text_event(event.to_string(), text))
            .await
    }

    async fn send_binary_event(&self, event: &str, data: Vec<u8>) -> Result<(), WebSocketError> {
        self.send_envelope(WsMessage::new_binary_event(event.to_string(), data))
            .await
    }

    async fn send_binary(&self, data: Vec<u8>) -> Result<(), WebSocketError> {
        self.send_frame(Message::Binary(data)).await
    }

    async fn send_text(&self, text: String) -> Result<(), WebSocketError> {
        self.send_frame(Message::Text(text)).await
    }

    fn on<F>(&self, event: &str, callback: F)
    where
        F: Fn(String, Vec<u8>) + Send + Sync + 'static,
    {
        self.event_callbacks
            .insert(event.to_string(), EventCallback::Sync(Arc::new(callback)));
    }

    fn on_async<F, Fut>(&self, event: &str, callback: F)
    where
        F: Fn(String, Vec<u8>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.event_callbacks.insert(
            event.to_string(),
            EventCallback::Async(Arc::new(move |event, payload| {
                Box::pin(callback(event, payload))
            })),
        );
    }

    fn is_connected(&self) -> bool {
        self.state.load(Ordering::Acquire) == STATE_CONNECTED
    }

    fn set_reconnection(&self, config: ReconnectionConfig) -> Result<(), WebSocketError> {
        config
            .validate()
            .map_err(WebSocketError::InvalidConfiguration)?;
        let mut reconnection = self
            .reconnection_config
            .write()
            .map_err(|_| WebSocketError::ConnectionError("reconnection lock poisoned".into()))?;
        *reconnection = config;
        Ok(())
    }
}

impl Drop for TokioWsClient {
    fn drop(&mut self) {
        if let Ok(runtime) = self.runtime.get_mut() {
            if let Some(runtime) = runtime.take() {
                runtime.cancellation.cancel();
                runtime.join.abort();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::DuplexStream;
    use tokio_tungstenite::tungstenite::protocol::Role;

    struct NeverReturnsCredentials;

    #[async_trait]
    impl AuthHeaderProvider for NeverReturnsCredentials {
        async fn headers(&self) -> Result<Vec<(String, String)>, WebSocketError> {
            std::future::pending().await
        }
    }

    async fn socket_pair() -> (WebSocketStream<DuplexStream>, WebSocketStream<DuplexStream>) {
        let (client_io, server_io) = tokio::io::duplex(64 * 1024);
        tokio::join!(
            WebSocketStream::from_raw_socket(client_io, Role::Client, None),
            WebSocketStream::from_raw_socket(server_io, Role::Server, None),
        )
    }

    fn test_config() -> WebSocketClientConfig {
        WebSocketClientConfig {
            url: "wss://example.test/socket".into(),
            namespace: Some("test".into()),
            reconnection: ReconnectionConfig {
                enabled: false,
                ..Default::default()
            },
            ping_interval_secs: 30,
            pong_timeout_secs: 5,
            idle_timeout_secs: 60,
            connect_timeout_secs: 1,
            send_timeout_secs: 1,
            close_timeout_secs: 1,
            shutdown_timeout_secs: 2,
            ..Default::default()
        }
    }

    fn runtime_context(client: &TokioWsClient, cancellation: CancellationToken) -> RuntimeContext {
        RuntimeContext {
            config: client.config.clone(),
            state: Arc::clone(&client.state),
            reconnect_attempt: Arc::clone(&client.reconnect_attempt),
            reconnection: Arc::clone(&client.reconnection_config),
            callbacks: Arc::clone(&client.event_callbacks),
            pending_acknowledgements: Arc::clone(&client.pending_acknowledgements),
            auth_headers: Arc::clone(&client.auth_headers),
            #[cfg(feature = "di")]
            trace_headers: TokioWsClient::capture_trace_headers(),
            cancellation,
            metrics: Arc::clone(&client.metrics),
        }
    }

    #[tokio::test]
    async fn callback_panic_is_reconciled_in_metrics() {
        let metrics = Arc::new(ClientMetrics::default());
        let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
        TokioWsClient::invoke_callback(
            EventCallback::Sync(Arc::new(|_, _| panic!("callback failure"))),
            "event".into(),
            Vec::new(),
            permit,
            Duration::from_secs(1),
            Arc::clone(&metrics),
        )
        .await;

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.callback_panicked, 1);
        assert_eq!(snapshot.callback_completed, 0);
    }

    #[tokio::test]
    async fn callback_timeout_is_reconciled_in_metrics() {
        let metrics = Arc::new(ClientMetrics::default());
        let permit = Arc::new(Semaphore::new(1)).acquire_owned().await.unwrap();
        TokioWsClient::invoke_callback(
            EventCallback::Async(Arc::new(|_, _| Box::pin(std::future::pending()))),
            "event".into(),
            Vec::new(),
            permit,
            Duration::from_millis(1),
            Arc::clone(&metrics),
        )
        .await;

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.callback_timed_out, 1);
        assert_eq!(snapshot.callback_completed, 0);
    }

    async fn attach_socket(client: &TokioWsClient, mut socket: WebSocketStream<DuplexStream>) {
        let cancellation = CancellationToken::new();
        let context = runtime_context(client, cancellation.clone());
        let (outbound, mut receiver) = mpsc::channel(client.config.outbound_queue_capacity);
        let (callback_sender, _callback_receiver) =
            mpsc::channel(client.config.callback_queue_capacity);
        TokioWsClient::set_state(&client.state, STATE_CONNECTED);
        let join = tokio::spawn(async move {
            TokioWsClient::run_socket(&mut socket, &mut receiver, &context, &callback_sender).await
        });
        *client.runtime.lock().unwrap() = Some(RuntimeHandle {
            cancellation,
            outbound,
            join,
        });
    }

    #[cfg(feature = "di")]
    #[test]
    fn immutable_trace_headers_override_untrusted_handshake_values() {
        let mut request = "ws://localhost/socket".into_client_request().unwrap();
        request.headers_mut().insert(
            "traceparent",
            HeaderValue::from_static("00-11111111111111111111111111111111-2222222222222222-01"),
        );
        let headers = std::collections::HashMap::from([
            (
                "traceparent".to_string(),
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string(),
            ),
            (
                "authorization".to_string(),
                "must-not-be-copied".to_string(),
            ),
        ]);

        TokioWsClient::apply_trace_headers(&mut request, &headers).unwrap();

        assert_eq!(
            request.headers()["traceparent"],
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        );
        assert!(!request.headers().contains_key("authorization"));
    }

    #[tokio::test]
    async fn text_binary_and_close_frames_use_the_owned_wire() {
        let client = TokioWsClient::with_config(test_config()).unwrap();
        let (client_socket, mut server_socket) = socket_pair().await;
        attach_socket(&client, client_socket).await;

        let server = tokio::spawn(async move {
            assert_eq!(
                server_socket.next().await.unwrap().unwrap(),
                Message::Text("hello".into())
            );
            assert_eq!(
                server_socket.next().await.unwrap().unwrap(),
                Message::Binary(vec![0, 1, 2, 255])
            );
            let close = server_socket.next().await.unwrap().unwrap();
            assert!(
                matches!(close, Message::Close(Some(frame)) if frame.code == CloseCode::Normal)
            );
            server_socket.flush().await.unwrap();
        });

        client.send_text("hello".into()).await.unwrap();
        client.send_binary(vec![0, 1, 2, 255]).await.unwrap();
        client.disconnect().await.unwrap();
        server.await.unwrap();
        assert_eq!(client.connection_state(), ConnectionState::Disconnected);
    }

    #[tokio::test]
    async fn unsupported_envelope_version_closes_with_protocol_error() {
        let client = TokioWsClient::with_config(test_config()).unwrap();
        let (mut socket, mut peer) = socket_pair().await;
        let context = runtime_context(&client, CancellationToken::new());
        let (_outbound, mut receiver) = mpsc::channel(1);
        let (callback_sender, _callback_receiver) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            TokioWsClient::run_socket(&mut socket, &mut receiver, &context, &callback_sender).await
        });

        peer.send(Message::Text(
            serde_json::json!({
                "protocol_version": 999,
                "msg_type": "event",
                "event": "chat:status",
                "content_kind": "json",
                "content_type": "application/json",
                "encoding": "identity",
                "data": null
            })
            .to_string(),
        ))
        .await
        .unwrap();
        let close = peer.next().await.unwrap().unwrap();
        assert!(matches!(
            close,
            Message::Close(Some(frame)) if frame.code == CloseCode::Protocol
        ));
        peer.flush().await.unwrap();

        assert!(matches!(
            task.await.unwrap(),
            Err(WebSocketError::UnsupportedProtocolVersion {
                actual: 999,
                expected: LILY_WEBSOCKET_PROTOCOL_VERSION,
            })
        ));
    }

    #[test]
    fn text_and_binary_dispatch_require_the_same_canonical_envelope() {
        let callbacks = DashMap::new();
        let pending = DashMap::new();
        let (sender, _receiver) = mpsc::channel(1);
        let metrics = ClientMetrics::default();
        let golden = br#"{"protocol_version":2,"msg_type":"event","event":"chat:status","content_kind":"json","content_type":"application/json","encoding":"identity","data":null,"timestamp":42}"#;
        assert!(TokioWsClient::dispatch_envelope(
            &callbacks, &pending, &sender, 1, golden, &metrics
        )
        .is_ok());
        assert!(matches!(
            TokioWsClient::dispatch_envelope(
                &callbacks,
                &pending,
                &sender,
                1,
                b"raw binary",
                &metrics,
            ),
            Err(WebSocketError::InvalidEnvelope)
        ));
        assert!(matches!(
            TokioWsClient::dispatch_envelope(
                &callbacks,
                &pending,
                &sender,
                1,
                br#"{"event":"chat:status","data":null}"#,
                &metrics,
            ),
            Err(WebSocketError::InvalidEnvelope)
        ));
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.inbound_messages, 1);
        assert_eq!(snapshot.protocol_failures, 2);
    }

    #[test]
    fn event_ack_and_error_envelopes_have_distinct_receive_owners() {
        let callbacks = DashMap::new();
        let pending = DashMap::new();
        callbacks.insert(
            "chat:message".into(),
            EventCallback::Sync(Arc::new(|_, _| {})),
        );
        callbacks.insert("on_error".into(), EventCallback::Sync(Arc::new(|_, _| {})));
        let (sender, mut receiver) = mpsc::channel(4);
        let metrics = ClientMetrics::default();

        let text = serde_json::to_vec(&WsMessage::new_text_event(
            "chat:message".into(),
            "hello".into(),
        ))
        .unwrap();
        TokioWsClient::dispatch_envelope(&callbacks, &pending, &sender, 4, &text, &metrics)
            .unwrap();
        assert_eq!(receiver.try_recv().unwrap().payload, b"hello");

        let binary = serde_json::to_vec(&WsMessage::new_binary_event(
            "chat:message".into(),
            [0, 1, 2, 255],
        ))
        .unwrap();
        TokioWsClient::dispatch_envelope(&callbacks, &pending, &sender, 4, &binary, &metrics)
            .unwrap();
        assert_eq!(receiver.try_recv().unwrap().payload, [0, 1, 2, 255]);

        let ack = WsMessage::new_ack("chat:message".into(), "ack-7".into(), Value::Null);
        let permit = Arc::new(Semaphore::new(1)).try_acquire_owned().unwrap();
        let (ack_sender, mut ack_receiver) = oneshot::channel();
        pending.insert(
            "ack-7".into(),
            PendingAcknowledgement {
                sender: ack_sender,
                _permit: permit,
            },
        );
        TokioWsClient::dispatch_envelope(
            &callbacks,
            &pending,
            &sender,
            4,
            &serde_json::to_vec(&ack).unwrap(),
            &metrics,
        )
        .unwrap();
        assert!(receiver.try_recv().is_err());
        assert!(matches!(
            ack_receiver.try_recv().unwrap().unwrap(),
            WebSocketReply::Acknowledgement(DecodedPayload::Json(Value::Null))
        ));

        let mut error = WsMessage::new_event(
            "chat:message".into(),
            serde_json::json!({
                "code": "ORDER_REJECTED",
                "message": "Order was rejected."
            }),
        );
        error.msg_type = MessageType::Error;
        TokioWsClient::dispatch_envelope(
            &callbacks,
            &pending,
            &sender,
            4,
            &serde_json::to_vec(&error).unwrap(),
            &metrics,
        )
        .unwrap();
        let dispatched_error = receiver.try_recv().unwrap();
        assert_eq!(dispatched_error.event, "on_error");
        let error_payload: Value = serde_json::from_slice(&dispatched_error.payload).unwrap();
        assert_eq!(error_payload["code"], "ORDER_REJECTED");

        let missing_authority = WsMessage {
            ack_id: None,
            ..ack
        };
        assert!(matches!(
            TokioWsClient::dispatch_envelope(
                &callbacks,
                &pending,
                &sender,
                4,
                &serde_json::to_vec(&missing_authority).unwrap(),
                &metrics,
            ),
            Err(WebSocketError::InvalidEnvelope)
        ));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.inbound_messages, 4);
        assert_eq!(snapshot.protocol_failures, 1);
    }

    #[test]
    fn correlated_rejection_resolves_once_and_late_reply_is_rejected() {
        let callbacks = DashMap::new();
        let pending = DashMap::new();
        let (callback_sender, _callback_receiver) = mpsc::channel(1);
        let metrics = ClientMetrics::default();
        let permits = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&permits).try_acquire_owned().unwrap();
        let (reply_sender, mut reply_receiver) = oneshot::channel();
        pending.insert(
            "ack-rejected".into(),
            PendingAcknowledgement {
                sender: reply_sender,
                _permit: permit,
            },
        );

        let mut rejection = WsMessage::new_event(
            "orders:create".into(),
            serde_json::json!({ "code": "ORDER_REJECTED" }),
        )
        .with_ack_id("ack-rejected".into());
        rejection.msg_type = MessageType::Error;
        let encoded = serde_json::to_vec(&rejection).unwrap();
        TokioWsClient::dispatch_envelope(
            &callbacks,
            &pending,
            &callback_sender,
            1,
            &encoded,
            &metrics,
        )
        .unwrap();
        assert!(matches!(
            reply_receiver.try_recv().unwrap().unwrap(),
            WebSocketReply::Rejection(DecodedPayload::Json(payload))
                if payload["code"] == "ORDER_REJECTED"
        ));
        assert_eq!(permits.available_permits(), 1);

        assert!(matches!(
            TokioWsClient::dispatch_envelope(
                &callbacks,
                &pending,
                &callback_sender,
                1,
                &encoded,
                &metrics,
            ),
            Err(WebSocketError::UnexpectedAcknowledgement)
        ));
    }

    #[test]
    fn disconnect_drains_pending_waiters_and_releases_capacity() {
        let pending = DashMap::new();
        let permits = Arc::new(Semaphore::new(1));
        let permit = Arc::clone(&permits).try_acquire_owned().unwrap();
        let (reply_sender, mut reply_receiver) = oneshot::channel();
        pending.insert(
            "ack-disconnect".into(),
            PendingAcknowledgement {
                sender: reply_sender,
                _permit: permit,
            },
        );

        TokioWsClient::fail_pending_acknowledgements(&pending);

        assert!(pending.is_empty());
        assert_eq!(permits.available_permits(), 1);
        assert!(matches!(
            reply_receiver.try_recv().unwrap(),
            Err(WebSocketError::AcknowledgementConnectionLost)
        ));
    }

    #[tokio::test]
    async fn request_waiters_are_bounded_and_cancel_or_timeout_without_replay() {
        let mut config = test_config();
        config.outbound_queue_capacity = 1;
        let client = Arc::new(TokioWsClient::with_config(config).unwrap());
        let cancellation = CancellationToken::new();
        let (outbound, mut receiver) = mpsc::channel(1);
        let runtime_join =
            tokio::spawn(async { std::future::pending::<Result<(), WebSocketError>>().await });
        TokioWsClient::set_state(&client.state, STATE_CONNECTED);
        *client.runtime.lock().unwrap() = Some(RuntimeHandle {
            cancellation,
            outbound,
            join: runtime_join,
        });

        let first = tokio::spawn({
            let client = Arc::clone(&client);
            async move {
                client
                    .request("test:first", Value::Null, Duration::from_secs(10))
                    .await
            }
        });
        timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("first correlated request must enter the writer")
            .unwrap()
            .acknowledgement
            .send(Ok(()))
            .unwrap();
        tokio::task::yield_now().await;
        assert_eq!(client.pending_acknowledgements.len(), 1);
        assert!(matches!(
            client
                .request("test:overflow", Value::Null, Duration::from_secs(1))
                .await,
            Err(WebSocketError::PendingAcknowledgementLimit { capacity: 1 })
        ));
        assert!(receiver.try_recv().is_err());

        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        tokio::task::yield_now().await;
        assert!(client.pending_acknowledgements.is_empty());
        assert_eq!(
            client.pending_acknowledgement_permits.available_permits(),
            1
        );

        let timed_out = tokio::spawn({
            let client = Arc::clone(&client);
            async move {
                client
                    .request("test:timeout", Value::Null, Duration::from_millis(10))
                    .await
            }
        });
        timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("second correlated request must enter the writer")
            .unwrap()
            .acknowledgement
            .send(Ok(()))
            .unwrap();
        assert!(matches!(
            timed_out.await.unwrap(),
            Err(WebSocketError::AcknowledgementTimeout(duration))
                if duration == Duration::from_millis(10)
        ));
        assert!(client.pending_acknowledgements.is_empty());
        assert_eq!(
            client.pending_acknowledgement_permits.available_permits(),
            1
        );
        assert!(receiver.try_recv().is_err());

        if let Some(runtime) = client.runtime.lock().unwrap().take() {
            runtime.join.abort();
        };
    }

    #[tokio::test]
    async fn a_full_outbound_queue_returns_typed_backpressure() {
        let mut config = test_config();
        config.outbound_queue_capacity = 1;
        let client = TokioWsClient::with_config(config).unwrap();
        let cancellation = CancellationToken::new();
        let (outbound, receiver) = mpsc::channel(1);
        let (acknowledgement, _result) = oneshot::channel();
        outbound
            .try_send(OutboundMessage {
                message: Message::Text("occupies-capacity".into()),
                acknowledgement,
            })
            .unwrap();
        let join = tokio::spawn(async move {
            let _receiver = receiver;
            std::future::pending::<Result<(), WebSocketError>>().await
        });
        TokioWsClient::set_state(&client.state, STATE_CONNECTED);
        *client.runtime.lock().unwrap() = Some(RuntimeHandle {
            cancellation,
            outbound,
            join,
        });

        assert!(matches!(
            client.send_text("rejected".into()).await,
            Err(WebSocketError::Backpressure { capacity: 1 })
        ));
    }

    #[test]
    fn configured_namespace_is_encoded_in_the_handshake_url_once() {
        let mut config = test_config();
        config.namespace = Some("tenant chat/özel".into());
        let url = TokioWsClient::handshake_url(&config).unwrap();
        let namespaces = url
            .query_pairs()
            .filter_map(|(name, value)| (name == "namespace").then_some(value.into_owned()))
            .collect::<Vec<_>>();
        assert_eq!(namespaces, vec!["tenant chat/özel"]);

        config.url = url.to_string();
        let url = TokioWsClient::handshake_url(&config).unwrap();
        assert_eq!(
            url.query_pairs()
                .filter(|(name, _)| name == "namespace")
                .count(),
            1
        );
    }

    #[test]
    fn rustls_failures_remain_a_typed_tls_terminal_result() {
        let source = tokio_tungstenite::tungstenite::Error::Tls(
            tokio_tungstenite::tungstenite::error::TlsError::Rustls(rustls::Error::General(
                "certificate validation failed".into(),
            )),
        );
        assert!(matches!(
            WebSocketError::from(source),
            WebSocketError::TlsValidation(message)
                if message.contains("certificate validation failed")
        ));
    }

    #[test]
    fn canonical_client_spans_do_not_declare_payload_secret_or_error_text_fields() {
        let source = include_str!("client.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("production source");
        for forbidden in [
            "otel.status_message =",
            "websocket.message.payload =",
            "authorization =",
            "cookie =",
            "token =",
            "password =",
        ] {
            assert!(
                !source.contains(forbidden),
                "forbidden telemetry field: {forbidden}"
            );
        }
        assert!(source.contains("\"websocket.handshake\""));
        assert!(source.contains("\"websocket.connection\""));
        assert!(source.contains("\"websocket.message\""));
    }

    #[test]
    fn active_connection_metric_uses_the_same_series_for_increment_and_decrement() {
        let source = include_str!("client.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("production source");
        assert!(source.contains("active_connections.add(1, &[]);"));
        assert!(source.contains("active_connections.add(-1, &[]);"));
    }

    #[tokio::test(start_paused = true)]
    async fn credential_refresh_is_bounded_before_network_io() {
        let client = TokioWsClient::with_config(test_config()).unwrap();
        client
            .set_auth_header_provider(Arc::new(NeverReturnsCredentials))
            .await;
        let context = runtime_context(&client, CancellationToken::new());
        let task = tokio::spawn(async move { TokioWsClient::establish_connection(&context).await });

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;

        assert!(matches!(
            task.await.unwrap(),
            Err(WebSocketError::Authentication(message)) if message.contains("timed out")
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn pong_timeout_uses_its_own_deadline_and_is_not_overwritten() {
        let mut config = test_config();
        config.ping_interval_secs = 2;
        config.pong_timeout_secs = 1;
        config.idle_timeout_secs = 10;
        let client = TokioWsClient::with_config(config).unwrap();
        let (mut socket, _unresponsive_peer) = socket_pair().await;
        let cancellation = CancellationToken::new();
        let context = runtime_context(&client, cancellation);
        let (_outbound, mut receiver) = mpsc::channel(1);
        let (callback_sender, _callback_receiver) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            TokioWsClient::run_socket(&mut socket, &mut receiver, &context, &callback_sender).await
        });

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;

        assert!(matches!(
            task.await.unwrap(),
            Err(WebSocketError::PongTimeout(duration)) if duration == Duration::from_secs(1)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_has_a_bounded_close_handshake() {
        let client = TokioWsClient::with_config(test_config()).unwrap();
        let (mut socket, _unresponsive_peer) = socket_pair().await;
        let cancellation = CancellationToken::new();
        let context = runtime_context(&client, cancellation.clone());
        let (_outbound, mut receiver) = mpsc::channel(1);
        let (callback_sender, _callback_receiver) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            TokioWsClient::run_socket(&mut socket, &mut receiver, &context, &callback_sender).await
        });

        cancellation.cancel();
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;

        assert!(matches!(
            task.await.unwrap(),
            Err(WebSocketError::CloseTimeout(duration)) if duration == Duration::from_secs(1)
        ));
    }
}
