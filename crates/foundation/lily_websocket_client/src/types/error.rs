// =============================================================================
// WebSocket Error Types
// =============================================================================

use thiserror::Error;

/// WebSocket client errors
#[derive(Debug, Error)]
pub enum WebSocketError {
    /// A URL, resource limit, header, subprotocol, or reconnect policy is invalid.
    #[error("Invalid WebSocket configuration: {0}")]
    InvalidConfiguration(String),

    /// The client already owns an active or connecting runtime.
    #[error("A WebSocket runtime is already connected")]
    AlreadyConnected,

    /// Application cancellation stopped the current operation.
    #[error("Operation was cancelled")]
    Cancelled,

    /// The TCP, TLS, and WebSocket handshake exceeded its deadline.
    #[error("Connection attempt timed out after {0:?}")]
    ConnectTimeout(std::time::Duration),

    /// The bounded application-to-socket queue had no remaining capacity.
    #[error("Outbound queue is full (capacity: {capacity})")]
    Backpressure {
        /// Configured queue capacity.
        capacity: usize,
    },

    /// The bounded inbound callback queue had no remaining capacity.
    #[error("Inbound callback queue is full (capacity: {capacity})")]
    CallbackBackpressure {
        /// Configured callback queue capacity.
        capacity: usize,
    },

    /// The bounded request/reply correlation table has no remaining slot.
    #[error("Pending WebSocket acknowledgement limit was reached (capacity: {capacity})")]
    PendingAcknowledgementLimit {
        /// Maximum concurrent requests awaiting a correlated terminal response.
        capacity: usize,
    },

    /// A request supplied a zero acknowledgement deadline.
    #[error("WebSocket acknowledgement timeout must be greater than zero")]
    InvalidAcknowledgementTimeout,

    /// A correlated acknowledgement or rejection did not arrive in time.
    #[error("WebSocket acknowledgement timed out after {0:?}")]
    AcknowledgementTimeout(std::time::Duration),

    /// The owning transport disconnected before a correlated reply arrived.
    #[error("WebSocket disconnected before the acknowledgement arrived")]
    AcknowledgementConnectionLost,

    /// The peer sent an acknowledgement or correlated error with no live waiter.
    #[error("WebSocket peer sent an unknown or late acknowledgement")]
    UnexpectedAcknowledgement,

    /// The correlation waiter ended without publishing a terminal result.
    #[error("WebSocket acknowledgement waiter closed unexpectedly")]
    AcknowledgementWaiterClosed,

    /// An application frame exceeded the configured message limit.
    #[error("Message is too large ({actual} bytes, maximum {maximum} bytes)")]
    MessageTooLarge {
        /// Observed frame size in bytes.
        actual: usize,
        /// Configured maximum frame size in bytes.
        maximum: usize,
    },

    /// A queued write was not acknowledged by the socket owner before its deadline.
    #[error("Send acknowledgement timed out after {0:?}")]
    SendTimeout(std::time::Duration),

    /// Graceful client shutdown and runtime reconciliation exceeded their total deadline.
    #[error("Graceful shutdown timed out after {0:?}")]
    ShutdownTimeout(std::time::Duration),

    /// The peer did not complete the WebSocket Close handshake in time.
    #[error("WebSocket closing handshake timed out after {0:?}")]
    CloseTimeout(std::time::Duration),

    /// No inbound frame arrived within the configured connection-idle budget.
    #[error("No inbound frame was received within the idle budget of {0:?}")]
    IdleTimeout(std::time::Duration),

    /// The peer did not answer a client Ping before the Pong deadline.
    #[error("The peer did not answer a Ping within {0:?}")]
    PongTimeout(std::time::Duration),

    /// Rustls certificate, hostname, SNI, or handshake validation failed.
    #[error("TLS handshake or certificate/hostname validation failed: {0}")]
    TlsValidation(String),

    /// A configured or dynamically produced handshake header is invalid.
    #[error("Invalid handshake header `{name}`: {reason}")]
    InvalidHeader {
        /// Rejected header name.
        name: String,
        /// Bounded diagnostic reason.
        reason: String,
    },

    /// The server selected a subprotocol that the client did not offer.
    #[error("Server selected unsupported subprotocol `{0}`")]
    UnsupportedSubprotocol(String),

    /// Strict subprotocol negotiation was requested but the server selected none.
    #[error("Server did not select a required WebSocket subprotocol")]
    SubprotocolRequired,

    /// An inbound Lily envelope declared an unsupported protocol version.
    #[error("Unsupported Lily WebSocket protocol version {actual}; expected {expected}")]
    UnsupportedProtocolVersion {
        /// Version received from the peer.
        actual: u16,
        /// Version supported by this client.
        expected: u16,
    },

    /// An inbound or outbound event failed the canonical Lily v2 envelope contract.
    #[error("Invalid Lily WebSocket v2 envelope")]
    InvalidEnvelope,

    /// Dynamic authentication headers could not be refreshed or validated.
    #[error("Authentication header refresh failed: {0}")]
    Authentication(String),

    /// Client lifecycle state or internal synchronization failed.
    #[error("Connection error: {0}")]
    ConnectionError(String),

    /// The operation requires an established socket, but none exists.
    #[error("Not connected")]
    NotConnected,

    /// A DI service was used before it received its configured client.
    #[error("WebSocket client service has not been initialized")]
    NotInitialized,

    /// An application value could not be serialized into an event envelope.
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// The socket writer rejected or failed to flush an outbound frame.
    #[error("Message send error: {0}")]
    SendError(String),

    /// The socket reader failed while receiving an inbound frame.
    #[error("Message receive error: {0}")]
    ReceiveError(String),

    /// Automatic reconnect attempts were exhausted or disabled after a failure.
    #[error("Reconnection failed: {0}")]
    ReconnectionFailed(String),

    /// Tungstenite reported a transport or protocol error not represented by a narrower variant.
    #[error("Tungstenite error: {0}")]
    TungsteniteError(#[source] Box<tokio_tungstenite::tungstenite::Error>),
}

impl WebSocketError {
    /// Returns a stable, bounded diagnostic code suitable for telemetry and
    /// application error mapping.
    pub const fn error_code(&self) -> &'static str {
        match self {
            Self::InvalidConfiguration(_) => "INVALID_CONFIGURATION",
            Self::AlreadyConnected => "ALREADY_CONNECTED",
            Self::Cancelled => "CANCELLED",
            Self::ConnectTimeout(_) => "CONNECT_TIMEOUT",
            Self::Backpressure { .. } => "OUTBOUND_QUEUE_FULL",
            Self::CallbackBackpressure { .. } => "CALLBACK_QUEUE_FULL",
            Self::PendingAcknowledgementLimit { .. } => "PENDING_ACKNOWLEDGEMENT_LIMIT",
            Self::InvalidAcknowledgementTimeout => "INVALID_ACKNOWLEDGEMENT_TIMEOUT",
            Self::AcknowledgementTimeout(_) => "ACKNOWLEDGEMENT_TIMEOUT",
            Self::AcknowledgementConnectionLost => "ACKNOWLEDGEMENT_CONNECTION_LOST",
            Self::UnexpectedAcknowledgement => "UNEXPECTED_ACKNOWLEDGEMENT",
            Self::AcknowledgementWaiterClosed => "ACKNOWLEDGEMENT_WAITER_CLOSED",
            Self::MessageTooLarge { .. } => "MESSAGE_TOO_LARGE",
            Self::SendTimeout(_) => "SEND_TIMEOUT",
            Self::ShutdownTimeout(_) => "SHUTDOWN_TIMEOUT",
            Self::CloseTimeout(_) => "CLOSE_TIMEOUT",
            Self::IdleTimeout(_) => "IDLE_TIMEOUT",
            Self::PongTimeout(_) => "PONG_TIMEOUT",
            Self::TlsValidation(_) => "TLS_VALIDATION_ERROR",
            Self::InvalidHeader { .. } => "INVALID_HEADER",
            Self::UnsupportedSubprotocol(_) => "UNSUPPORTED_SUBPROTOCOL",
            Self::SubprotocolRequired => "SUBPROTOCOL_REQUIRED",
            Self::UnsupportedProtocolVersion { .. } => "UNSUPPORTED_PROTOCOL_VERSION",
            Self::InvalidEnvelope => "INVALID_ENVELOPE",
            Self::Authentication(_) => "AUTHENTICATION_ERROR",
            Self::ConnectionError(_) => "CONNECTION_ERROR",
            Self::NotConnected => "NOT_CONNECTED",
            Self::NotInitialized => "NOT_INITIALIZED",
            Self::SerializationError(_) => "SERIALIZATION_ERROR",
            Self::SendError(_) => "SEND_ERROR",
            Self::ReceiveError(_) => "RECEIVE_ERROR",
            Self::ReconnectionFailed(_) => "RECONNECTION_FAILED",
            Self::TungsteniteError(_) => "TRANSPORT_ERROR",
        }
    }

    /// Returns `true` when the error represents an elapsed bounded deadline.
    pub const fn is_timeout(&self) -> bool {
        matches!(
            self,
            Self::ConnectTimeout(_)
                | Self::SendTimeout(_)
                | Self::ShutdownTimeout(_)
                | Self::CloseTimeout(_)
                | Self::IdleTimeout(_)
                | Self::PongTimeout(_)
                | Self::AcknowledgementTimeout(_)
        )
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for WebSocketError {
    fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
        match error {
            tokio_tungstenite::tungstenite::Error::Tls(error) => {
                Self::TlsValidation(error.to_string())
            }
            error => Self::TungsteniteError(Box::new(error)),
        }
    }
}
