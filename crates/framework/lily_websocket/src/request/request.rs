use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use lily_web_core::RequestConnectionInfo;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
#[cfg(any(test, feature = "fuzzing"))]
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

use crate::codec::{
    DecodedWebSocketMessage, EncodedWebSocketPayloadData, WebSocketContentKind, WebSocketFrameKind,
    WebSocketInboundMessageKind,
};
use crate::server::WsTransportSecurity;

use super::{
    WsBodyError, WsContentEncoding, WsContentKind, WsHeaderError, WsHeaders, WsMessageBody,
    WsWireFormat,
};

#[derive(Clone)]
struct OriginalWsFrame(Arc<[u8]>);

impl fmt::Debug for OriginalWsFrame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OriginalWsFrame")
            .field("bytes", &self.0.len())
            .field("payload", &"[REDACTED]")
            .finish()
    }
}

/// Immutable decoded application message and trusted connection context
/// supplied to controller actions and message middleware.
#[derive(Debug, Clone)]
pub struct WsRequest {
    // CACHE LINE 1 (64 bytes): HOT PATH - Most frequently accessed fields
    /// Unique connection identifier - accessed on every request operation
    pub connection_id: Uuid,
    /// Parsed message body - accessed on every message processing operation
    pub message_body: WsMessageBody,
    /// Transport frame kind, independent from the application content kind.
    wire_format: WsWireFormat,
    /// Exact bounded transport-frame bytes retained for `RawEnvelope` and
    /// custom-codec ownership without exposing them through Debug output.
    original_frame: OriginalWsFrame,
    /// Connection state - frequently checked for routing decisions
    pub connection_state: ConnectionState,

    // CACHE LINE 2: WARM PATH - Moderately accessed fields
    /// WebSocket headers from handshake - accessed during connection setup and validation
    pub headers: WsHeaders,
    /// Typed socket-peer and effective-client identity accepted by transport.
    connection_info: RequestConnectionInfo,
    /// Server-owned WS/WSS transport classification.
    transport_security: WsTransportSecurity,

    // CACHE LINE 3: COLD PATH - Less frequently accessed fields
    /// Timestamp when this decoded message request was materialized.
    ///
    /// This is a per-message timestamp, not the connection admission time.
    pub created_at: std::time::SystemTime,
    /// Timestamp at which the connection manager admitted the transport.
    connection_created_at: std::time::SystemTime,
    /// Current namespace the connection is in
    pub current_namespace: Option<String>,
    /// Immutable identity and transport data accepted during HTTP Upgrade.
    /// This is private so application payloads cannot forge trusted metadata.
    handshake: Option<Arc<crate::server::WsHandshakeContext>>,
    /// Current identity captured exactly once when this message entered the
    /// dispatch pipeline. Re-authentication cannot change it mid-message.
    principal: Option<lily_web_core::Principal>,
}

/// Connection state for WebSocket lifecycle management
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ConnectionState {
    /// Connection is being established (handshake in progress)
    #[default]
    Connecting,
    /// Connection is active and ready for messages
    Connected,
    /// Connection is being closed gracefully
    Closing,
    /// Connection has been closed
    Closed,
}

impl WsRequest {
    /// Create a new WebSocket request from incoming message
    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn new_from_message(
        connection_id: Uuid,
        message: Message,
        headers: WsHeaders,
        connection_info: RequestConnectionInfo,
        transport_security: WsTransportSecurity,
    ) -> Result<Self, WsRequestError> {
        let (wire_format, original_frame) = match &message {
            Message::Text(text) => (
                WsWireFormat::Text,
                OriginalWsFrame(Arc::from(text.as_bytes())),
            ),
            Message::Binary(bytes) => (
                WsWireFormat::Binary,
                OriginalWsFrame(Arc::from(bytes.as_slice())),
            ),
            _ => {
                return Err(WsRequestError::BodyParsing(
                    WsBodyError::UnsupportedMessageType,
                ));
            }
        };
        let message_body =
            WsMessageBody::from_message(&message).map_err(WsRequestError::BodyParsing)?;
        let created_at = std::time::SystemTime::now();

        Ok(Self {
            connection_id,
            message_body,
            wire_format,
            original_frame,
            connection_state: ConnectionState::Connected,
            headers,
            connection_info,
            transport_security,
            created_at,
            // Synthetic test/fuzz requests have no manager admission
            // authority. Treat request construction as their connection
            // boundary while keeping the two clocks structurally distinct.
            connection_created_at: created_at,
            current_namespace: None,
            handshake: None,
            principal: None,
        })
    }

    /// Bridges a validated custom/built-in frame-codec result into the
    /// existing immutable middleware/guard request view.
    pub(crate) fn new_from_decoded(
        connection_id: Uuid,
        connection_created_at: std::time::SystemTime,
        decoded: &DecodedWebSocketMessage,
        headers: WsHeaders,
        connection_info: RequestConnectionInfo,
        transport_security: WsTransportSecurity,
    ) -> Self {
        let payload = decoded.payload();
        let (content_kind, data) = match payload.data() {
            EncodedWebSocketPayloadData::Json(value) => (WsContentKind::Json, value.clone()),
            EncodedWebSocketPayloadData::Text(value) => {
                let kind = match payload.kind() {
                    WebSocketContentKind::Text => WsContentKind::Text,
                    WebSocketContentKind::Binary | WebSocketContentKind::Raw => {
                        WsContentKind::Binary
                    }
                    WebSocketContentKind::Json => WsContentKind::Json,
                };
                (kind, Value::String(value.clone()))
            }
            EncodedWebSocketPayloadData::Bytes(value) => (
                WsContentKind::Binary,
                Value::String(BASE64_STANDARD.encode(value)),
            ),
        };
        let encoding = match content_kind {
            WsContentKind::Binary => WsContentEncoding::Base64,
            WsContentKind::Json | WsContentKind::Text => WsContentEncoding::Identity,
        };
        let message_body = WsMessageBody {
            protocol_version: super::LILY_WEBSOCKET_PROTOCOL_VERSION,
            msg_type: match decoded.kind() {
                WebSocketInboundMessageKind::Event => super::WsMessageKind::Event,
                WebSocketInboundMessageKind::Ack => super::WsMessageKind::Ack,
            },
            event: decoded.route(),
            content_kind,
            content_type: payload.content_type().to_owned(),
            encoding,
            data,
            namespace: Some(decoded.namespace().to_owned()),
            room: decoded.room().map(str::to_owned),
            message_id: decoded.message_id().map(str::to_owned),
            ack_id: decoded.ack_id().map(str::to_owned),
            timestamp: decoded.timestamp_millis(),
            metadata: decoded
                .headers()
                .iter()
                .map(|(name, value)| (name.to_owned(), value.to_owned()))
                .collect(),
        };
        let (wire_format, original_frame) = match decoded.raw_envelope().kind() {
            WebSocketFrameKind::Text => (WsWireFormat::Text, decoded.raw_envelope().as_bytes()),
            WebSocketFrameKind::Binary => (WsWireFormat::Binary, decoded.raw_envelope().as_bytes()),
        };
        Self {
            connection_id,
            message_body,
            wire_format,
            original_frame: OriginalWsFrame(Arc::from(original_frame)),
            connection_state: ConnectionState::Connected,
            headers,
            connection_info,
            transport_security,
            created_at: std::time::SystemTime::now(),
            connection_created_at,
            current_namespace: Some(decoded.namespace().to_owned()),
            handshake: None,
            principal: None,
        }
    }

    pub(crate) fn with_handshake_context(
        mut self,
        handshake: Arc<crate::server::WsHandshakeContext>,
    ) -> Self {
        self.current_namespace = Some(handshake.namespace().to_string());
        self.connection_info = handshake.connection_info();
        self.transport_security = handshake.transport_security();
        self.principal = handshake.principal().cloned();
        self.handshake = Some(handshake);
        self
    }

    pub(crate) fn with_principal_snapshot(
        mut self,
        principal: Option<lily_web_core::Principal>,
    ) -> Self {
        self.principal = principal;
        self
    }

    /// Trusted, immutable context created by the server handshake callback.
    pub fn handshake(&self) -> Option<&crate::server::WsHandshakeContext> {
        self.handshake.as_deref()
    }

    /// Current authenticated identity captured for this message, when present.
    /// Use [`Self::handshake`] to inspect the immutable initial Upgrade
    /// identity.
    pub fn principal(&self) -> Option<&lily_web_core::Principal> {
        self.principal.as_ref()
    }

    /// Negotiated WebSocket subprotocol, when present.
    pub fn negotiated_subprotocol(&self) -> Option<&str> {
        self.handshake()
            .and_then(crate::server::WsHandshakeContext::subprotocol)
    }

    /// Get connection ID
    pub fn connection_id(&self) -> Uuid {
        self.connection_id
    }

    /// Get message body
    pub fn body(&self) -> &WsMessageBody {
        &self.message_body
    }

    /// Original Text or Binary frame representation.
    pub fn wire_format(&self) -> WsWireFormat {
        self.wire_format
    }

    /// Exact bytes of the original bounded Text or Binary frame.
    pub fn original_frame(&self) -> &[u8] {
        &self.original_frame.0
    }

    /// Immutable owned view of the exact original bounded frame.
    pub fn original_frame_owned(&self) -> Arc<[u8]> {
        Arc::clone(&self.original_frame.0)
    }

    /// Get connection state
    pub fn state(&self) -> &ConnectionState {
        &self.connection_state
    }

    /// Get headers
    pub fn headers(&self) -> &WsHeaders {
        &self.headers
    }

    /// Typed socket-peer and effective-client identity established by transport.
    pub const fn connection_info(&self) -> RequestConnectionInfo {
        self.connection_info
    }

    /// Effective client address after trusted-proxy processing.
    pub fn client_ip(&self) -> Option<std::net::IpAddr> {
        self.connection_info.client_ip()
    }

    /// Socket peer address before trusted-proxy processing.
    pub fn peer_ip(&self) -> Option<std::net::IpAddr> {
        self.connection_info.peer_ip()
    }

    /// Whether an explicitly trusted proxy supplied the effective client IP.
    pub fn via_trusted_proxy(&self) -> bool {
        self.connection_info.via_trusted_proxy()
    }

    /// Server-owned WS/WSS transport classification.
    pub const fn transport_security(&self) -> WsTransportSecurity {
        self.transport_security
    }

    /// Whether this request belongs to a Lily-terminated TLS connection.
    pub const fn is_secure(&self) -> bool {
        self.transport_security.is_secure()
    }

    /// Get event name from message body
    pub fn event(&self) -> &str {
        self.message_body.event()
    }

    /// Get message data as string
    pub fn data(&self) -> &Value {
        self.message_body.data()
    }

    /// Explicit application payload representation.
    pub fn content_kind(&self) -> WsContentKind {
        self.message_body.content_kind()
    }

    /// Validated application media type.
    pub fn content_type(&self) -> &str {
        self.message_body.content_type()
    }

    /// Explicit application payload encoding.
    pub fn content_encoding(&self) -> WsContentEncoding {
        self.message_body.encoding()
    }

    /// JSON payload authority for future typed extractors.
    pub fn json_payload(&self) -> Result<&Value, WsBodyError> {
        self.message_body.json_payload()
    }

    /// UTF-8 text payload authority for future typed extractors.
    pub fn text_payload(&self) -> Result<&str, WsBodyError> {
        self.message_body.text_payload()
    }

    /// Decoded application bytes for future binary extractors.
    pub fn binary_payload(&self) -> Result<Vec<u8>, WsBodyError> {
        self.message_body.binary_payload()
    }

    /// Get current namespace
    pub fn namespace(&self) -> Option<&str> {
        self.current_namespace
            .as_deref()
            .or_else(|| self.message_body.namespace())
    }

    /// Get target room from message or current rooms
    pub fn target_room(&self) -> Option<&str> {
        self.message_body.room()
    }

    /// Get metadata value
    pub fn get_metadata(&self, key: &str) -> Option<&String> {
        self.message_body.metadata().get(key)
    }

    /// Bounded envelope metadata, the sole per-message metadata authority.
    pub fn message_metadata(&self) -> &HashMap<String, String> {
        self.message_body.metadata()
    }

    /// Check if message expects acknowledgment
    pub fn expects_ack(&self) -> bool {
        self.message_body.expects_ack()
    }

    /// Get acknowledgment ID
    pub fn ack_id(&self) -> Option<&str> {
        self.message_body.ack_id()
    }

    /// Create acknowledgment response
    pub fn create_ack_response<T: Serialize>(
        &self,
        data: T,
    ) -> Result<Option<WsMessageBody>, serde_json::Error> {
        match self.ack_id() {
            Some(ack_id) => WsMessageBody::try_ack(self.event(), ack_id, data).map(Some),
            None => Ok(None),
        }
    }

    /// Check if connection is active
    pub fn is_active(&self) -> bool {
        matches!(self.connection_state, ConnectionState::Connected)
    }

    /// Check if connection is closing or closed
    pub fn is_closing(&self) -> bool {
        matches!(
            self.connection_state,
            ConnectionState::Closing | ConnectionState::Closed
        )
    }

    /// Elapsed time since this decoded message request was materialized.
    pub fn message_age(&self) -> std::time::Duration {
        self.created_at.elapsed().unwrap_or_default()
    }

    /// Elapsed time since the connection manager admitted the transport.
    pub fn connection_age(&self) -> std::time::Duration {
        self.connection_created_at.elapsed().unwrap_or_default()
    }
}

impl fmt::Display for WsRequest {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "<WebSocket Request {} {} [{}]>",
            self.connection_id,
            self.event(),
            match &self.connection_state {
                ConnectionState::Connecting => "CONNECTING",
                ConnectionState::Connected => "CONNECTED",
                ConnectionState::Closing => "CLOSING",
                ConnectionState::Closed => "CLOSED",
            }
        )
    }
}

/// WebSocket request parsing and validation errors
#[derive(Debug, thiserror::Error)]
pub enum WsRequestError {
    /// Upgrade headers violate the RFC 6455 boundary contract.
    #[error("Header validation failed: {0}")]
    HeaderValidation(#[from] WsHeaderError),
    /// Text or Binary application data is not a valid Lily envelope.
    #[error("Body parsing failed: {0}")]
    BodyParsing(#[from] WsBodyError),
    /// Application payload serialization or deserialization failed.
    #[error("Message serialization failed")]
    Serialization(#[from] serde_json::Error),
}

impl WsRequestError {
    /// Maps this failure to a stable public wire code.
    pub fn protocol_error_code(&self) -> super::WsProtocolErrorCode {
        match self {
            Self::BodyParsing(error) => error.protocol_error_code(),
            _ => super::WsProtocolErrorCode::InvalidEnvelope,
        }
    }

    /// Maps this failure to a bounded close reason.
    pub fn close_reason(&self) -> super::WsCloseReason {
        match self {
            Self::BodyParsing(error) => error.close_reason(),
            _ => super::WsCloseReason::InvalidEnvelope,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::{WsContentKind, WsMessageKind};

    #[test]
    fn exact_transport_frame_and_metadata_authority_are_preserved() {
        const SECRET: &str = "RAW_FRAME_SECRET_82D1";
        let envelope = WsMessageBody::new_text("chat:send", SECRET)
            .add_metadata("tenant".into(), "blue".into());
        let message = envelope.to_message().unwrap();
        let exact = match &message {
            Message::Text(text) => text.as_bytes().to_vec(),
            _ => unreachable!(),
        };

        let request = WsRequest::new_from_message(
            Uuid::nil(),
            message,
            WsHeaders::default(),
            RequestConnectionInfo::direct("127.0.0.1".parse().unwrap()),
            WsTransportSecurity::Plaintext,
        )
        .unwrap();

        assert_eq!(request.wire_format(), WsWireFormat::Text);
        assert_eq!(request.original_frame(), exact);
        assert_eq!(&*request.original_frame_owned(), exact);
        assert_eq!(request.content_kind(), WsContentKind::Text);
        assert_eq!(request.text_payload().unwrap(), SECRET);
        assert_eq!(
            request.get_metadata("tenant").map(String::as_str),
            Some("blue")
        );
        assert_eq!(request.message_metadata().len(), 1);
        assert!(!format!("{request:?}").contains(SECRET));
    }

    #[test]
    fn binary_transport_kind_is_independent_from_binary_payload_kind() {
        let envelope = WsMessageBody::new_binary("files:chunk", [0, 1, 2, 255]);
        let message = envelope.to_binary_message().unwrap();
        let exact = match &message {
            Message::Binary(bytes) => bytes.clone(),
            _ => unreachable!(),
        };
        let request = WsRequest::new_from_message(
            Uuid::nil(),
            message,
            WsHeaders::default(),
            RequestConnectionInfo::default(),
            WsTransportSecurity::Plaintext,
        )
        .unwrap();

        assert_eq!(request.wire_format(), WsWireFormat::Binary);
        assert_eq!(request.original_frame(), exact);
        assert_eq!(request.binary_payload().unwrap(), [0, 1, 2, 255]);
    }

    #[test]
    fn request_ack_helper_preserves_route_and_real_ack_kind() {
        let inbound = WsMessageBody::try_new("chat:send", serde_json::Value::Null)
            .unwrap()
            .with_ack_id("ack-7".into());
        let request = WsRequest::new_from_message(
            Uuid::nil(),
            inbound.to_message().unwrap(),
            WsHeaders::default(),
            RequestConnectionInfo::default(),
            WsTransportSecurity::Plaintext,
        )
        .unwrap();

        let ack = request
            .create_ack_response(serde_json::json!({ "accepted": true }))
            .unwrap()
            .unwrap();
        assert_eq!(ack.msg_type, WsMessageKind::Ack);
        assert_eq!(ack.event(), "chat:send");
        assert_eq!(ack.ack_id(), Some("ack-7"));
        assert_eq!(ack.validate_wire(), Ok(()));
    }

    #[test]
    fn inbound_ack_is_not_a_dispatchable_request() {
        let ack = WsMessageBody::try_ack("chat:send", "ack-7", serde_json::Value::Null).unwrap();
        let error = WsRequest::new_from_message(
            Uuid::nil(),
            ack.to_message().unwrap(),
            WsHeaders::default(),
            RequestConnectionInfo::default(),
            WsTransportSecurity::Plaintext,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            WsRequestError::BodyParsing(WsBodyError::UnsupportedMessageKind)
        ));
    }

    #[test]
    fn request_uses_typed_transport_authority_instead_of_origin() {
        let peer_addr = "10.2.3.4:43123".parse::<std::net::SocketAddr>().unwrap();
        let client_ip = "198.51.100.20".parse::<std::net::IpAddr>().unwrap();
        let connection_info = RequestConnectionInfo::from_trusted_transport(
            Some(peer_addr.ip()),
            Some(client_ip),
            true,
        );
        let mut headers = WsHeaders::default();
        headers.origin = Some("https://secure-origin.example".to_owned());
        let message = WsMessageBody::new_text("chat:send", "hello")
            .to_message()
            .unwrap();

        let plaintext = WsRequest::new_from_message(
            Uuid::nil(),
            message,
            headers.clone(),
            connection_info,
            WsTransportSecurity::Plaintext,
        )
        .unwrap();
        assert_eq!(plaintext.peer_ip(), Some(peer_addr.ip()));
        assert_eq!(plaintext.client_ip(), Some(client_ip));
        assert!(plaintext.via_trusted_proxy());
        assert!(!crate::request::WsRequestExt::is_secure(&plaintext));

        let handshake = Arc::new(crate::server::WsHandshakeContext::new(
            "chat".to_owned(),
            WsHeaders::default(),
            None,
            None,
            peer_addr,
            connection_info,
            WsTransportSecurity::Tls,
        ));
        let tls = plaintext.with_handshake_context(handshake);
        assert!(crate::request::WsRequestExt::is_secure(&tls));
        assert_eq!(tls.transport_security(), WsTransportSecurity::Tls);
    }

    #[test]
    fn request_separates_initial_handshake_identity_from_message_snapshot() {
        let peer_addr = "127.0.0.1:43123".parse::<std::net::SocketAddr>().unwrap();
        let connection_info = RequestConnectionInfo::direct(peer_addr.ip());
        let initial = lily_web_core::Principal::new(
            "initial-subject",
            ["initial-role".to_owned()],
            [],
            serde_json::Map::new(),
        );
        let refreshed = lily_web_core::Principal::new(
            "refreshed-subject",
            ["refreshed-role".to_owned()],
            [],
            serde_json::Map::new(),
        );
        let handshake = Arc::new(crate::server::WsHandshakeContext::new(
            "chat".to_owned(),
            WsHeaders::default(),
            Some(initial),
            None,
            peer_addr,
            connection_info,
            WsTransportSecurity::Plaintext,
        ));
        let request = WsRequest::new_from_message(
            Uuid::nil(),
            WsMessageBody::new_text("chat:send", "hello")
                .to_message()
                .unwrap(),
            WsHeaders::default(),
            connection_info,
            WsTransportSecurity::Plaintext,
        )
        .unwrap()
        .with_handshake_context(handshake)
        .with_principal_snapshot(Some(refreshed));

        assert_eq!(
            request.principal().map(lily_web_core::Principal::subject),
            Some("refreshed-subject")
        );
        assert_eq!(
            request
                .handshake()
                .and_then(crate::server::WsHandshakeContext::principal)
                .map(lily_web_core::Principal::subject),
            Some("initial-subject")
        );
    }

    #[test]
    fn message_and_connection_age_use_distinct_timestamps() {
        let mut request = WsRequest::new_from_message(
            Uuid::nil(),
            WsMessageBody::new_text("chat:send", "hello")
                .to_message()
                .unwrap(),
            WsHeaders::default(),
            RequestConnectionInfo::default(),
            WsTransportSecurity::Plaintext,
        )
        .unwrap();
        let now = std::time::SystemTime::now();
        request.connection_created_at = now.checked_sub(std::time::Duration::from_secs(5)).unwrap();
        request.created_at = now
            .checked_sub(std::time::Duration::from_millis(20))
            .unwrap();

        let message_age = request.message_age();
        let connection_age = request.connection_age();
        let uptime = crate::request::WsRequestExt::uptime(&request);

        assert!(message_age < std::time::Duration::from_secs(1));
        assert!(connection_age >= std::time::Duration::from_secs(5));
        assert!(connection_age > message_age);
        assert!(uptime >= connection_age);
        assert!(uptime < connection_age + std::time::Duration::from_secs(1));
    }
}
