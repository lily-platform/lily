use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde::{Deserialize, Deserializer, Serialize, de};
use serde_json::Value;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_tungstenite::tungstenite::{
    Message,
    protocol::{CloseFrame, frame::coding::CloseCode},
};

/// Current Lily WebSocket envelope protocol version.
pub const LILY_WEBSOCKET_PROTOCOL_VERSION: u16 = 2;
/// Canonical WebSocket subprotocol token for Lily v2 envelopes.
pub const LILY_WEBSOCKET_SUBPROTOCOL: &str = "lily.v2";
/// Canonical media type used by JSON payload constructors.
pub const JSON_CONTENT_TYPE: &str = "application/json";
/// Canonical media type used by UTF-8 text payload constructors.
pub const TEXT_CONTENT_TYPE: &str = "text/plain; charset=utf-8";
/// Canonical media type used by binary payload constructors.
pub const BINARY_CONTENT_TYPE: &str = "application/octet-stream";
/// Maximum UTF-8 byte length of a canonical event route.
pub const MAX_CANONICAL_EVENT_BYTES: usize = 256;
/// Maximum UTF-8 byte length of a canonical namespace token.
pub const MAX_CANONICAL_NAMESPACE_BYTES: usize = 128;
/// Maximum UTF-8 byte length of a public room token.
pub const MAX_CANONICAL_ROOM_BYTES: usize = 128;
/// Maximum UTF-8 byte length of a message or acknowledgement identifier.
pub const MAX_CANONICAL_ID_BYTES: usize = 128;
/// Maximum application metadata entries retained by one envelope.
pub const MAX_CANONICAL_METADATA_ENTRIES: usize = 32;
/// Maximum UTF-8 byte length of one application metadata key.
pub const MAX_CANONICAL_METADATA_KEY_BYTES: usize = 128;
/// Maximum UTF-8 byte length of one application metadata value.
pub const MAX_CANONICAL_METADATA_VALUE_BYTES: usize = 1024;
const MAX_CANONICAL_CONTENT_TYPE_BYTES: usize = 128;

/// Semantic kind carried inside both text and binary Lily v2 envelopes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WsMessageKind {
    /// Application event routed to a controller action.
    #[default]
    Event,
    /// Acknowledgement correlated by `ack_id`.
    Ack,
    /// Bounded protocol error response.
    Error,
    /// Reserved connection lifecycle envelope.
    Connect,
    /// Reserved disconnection lifecycle envelope.
    Disconnect,
}

/// Explicit application payload representation inside a Lily v2 envelope.
///
/// This value is independent from the WebSocket transport frame kind. Both a
/// Text frame and a Binary frame may carry the same versioned JSON envelope.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WsContentKind {
    /// `data` is a JSON value and uses [`WsContentEncoding::Identity`].
    #[default]
    Json,
    /// `data` is a JSON string containing UTF-8 text.
    Text,
    /// `data` is a JSON string containing canonical padded Base64.
    Binary,
}

/// Encoding applied to the `data` member of a Lily v2 envelope.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WsContentEncoding {
    /// The JSON value or UTF-8 string is represented directly.
    #[default]
    Identity,
    /// Binary application bytes are represented as canonical padded Base64.
    Base64,
}

/// WebSocket frame representation used for the canonical JSON envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsWireFormat {
    /// The versioned JSON envelope arrived in a WebSocket Text frame.
    Text,
    /// The versioned JSON envelope arrived in a WebSocket Binary frame.
    Binary,
}

/// Stable public error identifiers. Internal error strings and request
/// payloads are never copied into the wire response or close reason.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsProtocolErrorCode {
    /// The frame is not a valid strict Lily v2 envelope.
    InvalidEnvelope,
    /// `protocol_version` is unsupported.
    UnsupportedVersion,
    /// The envelope kind is not accepted at this boundary.
    UnsupportedMessageKind,
    /// The event route is malformed.
    InvalidRoute,
    /// The message targets a namespace unavailable to this connection.
    NamespaceViolation,
    /// No registered action matches the route.
    ActionNotFound,
    /// A guard denied the action.
    AuthorizationDenied,
    /// Message middleware rejected the action.
    MiddlewareRejected,
    /// The application action failed.
    HandlerFailed,
}

impl WsProtocolErrorCode {
    /// Stable wire identifier for this protocol error.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidEnvelope => "ws.invalid_envelope",
            Self::UnsupportedVersion => "ws.unsupported_version",
            Self::UnsupportedMessageKind => "ws.unsupported_message_kind",
            Self::InvalidRoute => "ws.invalid_route",
            Self::NamespaceViolation => "ws.namespace_violation",
            Self::ActionNotFound => "ws.action_not_found",
            Self::AuthorizationDenied => "ws.authorization_denied",
            Self::MiddlewareRejected => "ws.middleware_rejected",
            Self::HandlerFailed => "ws.handler_failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Bounded WebSocket close reason selected by the Lily runtime.
pub enum WsCloseReason {
    /// A typed application action deliberately closed the connection.
    Application,
    /// The peer sent a malformed strict envelope.
    InvalidEnvelope,
    /// The peer selected an unsupported Lily protocol version.
    UnsupportedVersion,
    /// The message kind is not allowed at this boundary.
    UnsupportedMessageKind,
    /// Authentication, authorization, or another policy rejected work.
    PolicyViolation,
    /// The application-provided identity deadline elapsed.
    IdentityExpired,
    /// Outbound application admission capacity did not recover before its deadline.
    SlowConsumer,
    /// The server could not safely continue processing.
    InternalFailure,
    /// Application shutdown is draining the listener.
    ServerShutdown,
    /// The application-message idle deadline elapsed.
    IdleTimeout,
}

impl WsCloseReason {
    /// RFC 6455 close code corresponding to this bounded reason.
    pub const fn code(self) -> CloseCode {
        match self {
            Self::Application => CloseCode::Normal,
            Self::InvalidEnvelope | Self::UnsupportedVersion | Self::UnsupportedMessageKind => {
                CloseCode::Protocol
            }
            Self::PolicyViolation | Self::IdentityExpired => CloseCode::Policy,
            Self::SlowConsumer => CloseCode::Again,
            Self::InternalFailure => CloseCode::Error,
            Self::ServerShutdown | Self::IdleTimeout => CloseCode::Away,
        }
    }

    /// Stable close-frame reason text.
    pub const fn reason(self) -> &'static str {
        match self {
            Self::Application => "lily.v2.application_close",
            Self::InvalidEnvelope => "lily.v2.invalid_envelope",
            Self::UnsupportedVersion => "lily.v2.unsupported_version",
            Self::UnsupportedMessageKind => "lily.v2.unsupported_message_kind",
            Self::PolicyViolation => "lily.v2.policy_violation",
            Self::IdentityExpired => "lily.v2.identity_expired",
            Self::SlowConsumer => "lily.v2.slow_consumer",
            Self::InternalFailure => "lily.v2.internal_failure",
            Self::ServerShutdown => "lily.v2.server_shutdown",
            Self::IdleTimeout => "lily.v2.idle_timeout",
        }
    }

    /// Builds the owned Tungstenite close frame.
    pub fn frame(self) -> CloseFrame<'static> {
        CloseFrame {
            code: self.code(),
            reason: self.reason().into(),
        }
    }
}

/// Lily WebSocket v2 envelope.
///
/// A Binary transport frame still contains the exact UTF-8 JSON bytes of this
/// schema. Application binary data lives in the `data` JSON string and is
/// explicitly marked `content_kind=binary, encoding=base64`.
#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct WsMessageBody {
    /// Required protocol discriminator. Missing or mismatched versions are
    /// never interpreted as a legacy format.
    pub protocol_version: u16,

    /// Semantic envelope kind.
    pub msg_type: WsMessageKind,

    /// Canonical `namespace:action` route for application events.
    pub event: String,

    /// Explicit semantic representation of the application payload.
    pub content_kind: WsContentKind,

    /// Bounded, printable media type supplied by the sender.
    pub content_type: String,

    /// Explicit encoding of the `data` member.
    pub encoding: WsContentEncoding,

    /// Application payload. Debug output always redacts this value.
    pub data: Value,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional explicit namespace; must agree with the selected connection
    /// namespace.
    pub namespace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional public-room target.
    pub room: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional application message identifier.
    pub message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional acknowledgement correlation identifier.
    pub ack_id: Option<String>,

    #[serde(default)]
    /// Non-negative Unix timestamp in milliseconds.
    pub timestamp: i64,

    #[serde(
        default,
        deserialize_with = "deserialize_metadata",
        skip_serializing_if = "HashMap::is_empty"
    )]
    /// Bounded application metadata excluded from Debug payload output.
    pub metadata: HashMap<String, String>,
}

impl std::fmt::Debug for WsMessageBody {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WsMessageBody")
            .field("protocol_version", &self.protocol_version)
            .field("msg_type", &self.msg_type)
            .field("event", &self.event)
            .field("content_kind", &self.content_kind)
            .field("content_type", &self.content_type)
            .field("encoding", &self.encoding)
            .field("data", &"[REDACTED]")
            .field("has_namespace", &self.namespace.is_some())
            .field("has_room", &self.room.is_some())
            .field("has_message_id", &self.message_id.is_some())
            .field("has_ack_id", &self.ack_id.is_some())
            .field("timestamp", &self.timestamp)
            .field("metadata_entries", &self.metadata.len())
            .finish()
    }
}

impl Default for WsMessageBody {
    fn default() -> Self {
        Self {
            protocol_version: LILY_WEBSOCKET_PROTOCOL_VERSION,
            msg_type: WsMessageKind::Event,
            event: "lily:message".into(),
            content_kind: WsContentKind::Json,
            content_type: JSON_CONTENT_TYPE.into(),
            encoding: WsContentEncoding::Identity,
            data: Value::Null,
            namespace: None,
            room: None,
            message_id: None,
            ack_id: None,
            timestamp: unix_timestamp_millis(),
            metadata: HashMap::new(),
        }
    }
}

impl WsMessageBody {
    /// Fallible constructor used by every canonical send path.
    pub fn try_new<T>(event: impl Into<String>, data: T) -> Result<Self, serde_json::Error>
    where
        T: Serialize,
    {
        Ok(Self {
            event: event.into(),
            data: serde_json::to_value(data)?,
            ..Default::default()
        })
    }

    /// Constructs a canonical UTF-8 text payload.
    pub fn new_text(event: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            event: event.into(),
            content_kind: WsContentKind::Text,
            content_type: TEXT_CONTENT_TYPE.into(),
            encoding: WsContentEncoding::Identity,
            data: Value::String(text.into()),
            ..Default::default()
        }
    }

    /// Constructs a canonical binary payload encoded as padded Base64.
    pub fn new_binary(event: impl Into<String>, bytes: impl AsRef<[u8]>) -> Self {
        Self {
            event: event.into(),
            content_kind: WsContentKind::Binary,
            content_type: BINARY_CONTENT_TYPE.into(),
            encoding: WsContentEncoding::Base64,
            data: Value::String(BASE64_STANDARD.encode(bytes.as_ref())),
            ..Default::default()
        }
    }

    /// Constructs a real acknowledgement envelope correlated by `ack_id`.
    pub fn try_ack<T>(
        event: impl Into<String>,
        ack_id: impl Into<String>,
        data: T,
    ) -> Result<Self, serde_json::Error>
    where
        T: Serialize,
    {
        let mut envelope = Self::try_new(event, data)?;
        envelope.msg_type = WsMessageKind::Ack;
        envelope.ack_id = Some(ack_id.into());
        Ok(envelope)
    }

    /// Builds the canonical bounded protocol-error envelope.
    pub fn protocol_error(code: WsProtocolErrorCode) -> Self {
        Self {
            msg_type: WsMessageKind::Error,
            event: "lily:error".into(),
            data: serde_json::json!({ "code": code.as_str() }),
            ..Default::default()
        }
    }

    /// Sets the explicit namespace.
    pub fn with_namespace(mut self, namespace: String) -> Self {
        self.namespace = Some(namespace);
        self
    }

    /// Sets the public-room target.
    pub fn with_room(mut self, room: String) -> Self {
        self.room = Some(room);
        self
    }

    /// Sets the acknowledgement correlation identifier.
    pub fn with_ack_id(mut self, ack_id: String) -> Self {
        self.ack_id = Some(ack_id);
        self
    }

    /// Adds one metadata entry. Validation remains deferred until conversion
    /// to a wire message.
    pub fn add_metadata(mut self, key: String, value: String) -> Self {
        self.metadata.insert(key, value);
        self
    }

    /// Parse either a text envelope or a binary frame containing the exact
    /// UTF-8 JSON representation of the same Lily v2 envelope.
    pub fn from_message(message: &Message) -> Result<Self, WsBodyError> {
        let envelope = match message {
            Message::Text(text) => serde_json::from_str(text),
            Message::Binary(bytes) => serde_json::from_slice(bytes),
            _ => return Err(WsBodyError::UnsupportedMessageType),
        }
        .map_err(|_| WsBodyError::InvalidFormat)?;
        Self::validate(envelope)
    }

    /// Validates a decoded envelope as an inbound dispatchable Lily v2 event.
    ///
    /// Acknowledgements have a separate correlation owner and are therefore
    /// rejected here instead of being routed to a normal controller action.
    pub fn validate(envelope: Self) -> Result<Self, WsBodyError> {
        envelope.validate_common()?;
        if envelope.msg_type != WsMessageKind::Event {
            return Err(WsBodyError::UnsupportedMessageKind);
        }
        Ok(envelope)
    }

    /// Validates all common v2 wire invariants without applying inbound
    /// dispatch-kind policy. Codecs and outbound materializers can use this
    /// read-only check before selecting their own kind authority.
    pub fn validate_wire(&self) -> Result<(), WsBodyError> {
        self.validate_common()
    }

    fn validate_common(&self) -> Result<(), WsBodyError> {
        if self.protocol_version != LILY_WEBSOCKET_PROTOCOL_VERSION {
            return Err(WsBodyError::UnsupportedVersion {
                actual: self.protocol_version,
                expected: LILY_WEBSOCKET_PROTOCOL_VERSION,
            });
        }
        if !is_canonical_event(&self.event) {
            return Err(WsBodyError::InvalidEvent);
        }
        if !is_valid_content_type(&self.content_type) {
            return Err(WsBodyError::InvalidContentType);
        }
        match self.content_kind {
            WsContentKind::Json => {
                if self.content_type != JSON_CONTENT_TYPE {
                    return Err(WsBodyError::InvalidContentType);
                }
                if self.encoding != WsContentEncoding::Identity {
                    return Err(WsBodyError::InvalidContentEncoding);
                }
            }
            WsContentKind::Text => {
                if self.content_type != TEXT_CONTENT_TYPE {
                    return Err(WsBodyError::InvalidContentType);
                }
                if self.encoding != WsContentEncoding::Identity || !self.data.is_string() {
                    return Err(WsBodyError::InvalidPayload);
                }
            }
            WsContentKind::Binary => {
                if self.content_type != BINARY_CONTENT_TYPE {
                    return Err(WsBodyError::InvalidContentType);
                }
                if self.encoding != WsContentEncoding::Base64 {
                    return Err(WsBodyError::InvalidContentEncoding);
                }
                let Some(encoded) = self.data.as_str() else {
                    return Err(WsBodyError::InvalidPayload);
                };
                let decoded = BASE64_STANDARD
                    .decode(encoded)
                    .map_err(|_| WsBodyError::InvalidPayload)?;
                if BASE64_STANDARD.encode(decoded) != encoded {
                    return Err(WsBodyError::InvalidPayload);
                }
            }
        }
        if self.namespace.as_deref().is_some_and(|value| {
            !is_canonical_namespace(value)
                || self.event.split_once(':').map(|(route, _)| route) != Some(value)
        }) {
            return Err(WsBodyError::InvalidNamespace);
        }
        if self
            .room
            .as_deref()
            .is_some_and(|value| !is_canonical_room(value, MAX_CANONICAL_ROOM_BYTES))
        {
            return Err(WsBodyError::InvalidRoom);
        }
        if self
            .message_id
            .as_deref()
            .is_some_and(|value| !is_bounded_identifier(value, MAX_CANONICAL_ID_BYTES))
        {
            return Err(WsBodyError::InvalidIdentifier);
        }
        if self
            .ack_id
            .as_deref()
            .is_some_and(|value| !is_bounded_identifier(value, MAX_CANONICAL_ID_BYTES))
        {
            return Err(WsBodyError::InvalidIdentifier);
        }
        if self.msg_type == WsMessageKind::Ack && self.ack_id.is_none() {
            return Err(WsBodyError::MissingAcknowledgementAuthority);
        }
        if self.timestamp < 0 {
            return Err(WsBodyError::InvalidTimestamp);
        }
        if self.metadata.len() > MAX_CANONICAL_METADATA_ENTRIES {
            return Err(WsBodyError::InvalidMetadata);
        }
        for (key, value) in &self.metadata {
            if !is_bounded_identifier(key, MAX_CANONICAL_METADATA_KEY_BYTES) {
                return Err(WsBodyError::InvalidMetadata);
            }
            if value.len() > MAX_CANONICAL_METADATA_VALUE_BYTES {
                return Err(WsBodyError::InvalidMetadata);
            }
            if value.bytes().any(|byte| byte == 0) {
                return Err(WsBodyError::InvalidMetadata);
            }
        }
        Ok(())
    }

    /// Encodes this envelope as a WebSocket Text message after validation.
    pub fn to_message(&self) -> Result<Message, WsBodyError> {
        self.validate_common()?;
        let text = serde_json::to_string(self).map_err(|_| WsBodyError::Serialization)?;
        Ok(Message::Text(text))
    }

    /// Encodes this envelope as a WebSocket Binary message containing the
    /// same UTF-8 JSON bytes.
    pub fn to_binary_message(&self) -> Result<Message, WsBodyError> {
        self.validate_common()?;
        let bytes = serde_json::to_vec(self).map_err(|_| WsBodyError::Serialization)?;
        Ok(Message::Binary(bytes))
    }

    /// Whether an explicit namespace is present.
    pub fn is_namespaced(&self) -> bool {
        self.namespace.is_some()
    }

    /// Whether an explicit public-room target is present.
    pub fn is_room_targeted(&self) -> bool {
        self.room.is_some()
    }

    /// Whether the sender requested an acknowledgement.
    pub fn expects_ack(&self) -> bool {
        self.ack_id.is_some()
    }

    /// Canonical event route.
    pub fn event(&self) -> &str {
        &self.event
    }

    /// Application payload.
    pub fn data(&self) -> &Value {
        &self.data
    }

    /// Explicit application payload representation.
    pub const fn content_kind(&self) -> WsContentKind {
        self.content_kind
    }

    /// Validated application media type.
    pub fn content_type(&self) -> &str {
        &self.content_type
    }

    /// Explicit envelope payload encoding.
    pub const fn encoding(&self) -> WsContentEncoding {
        self.encoding
    }

    /// Returns the JSON payload when this envelope declares JSON authority.
    pub fn json_payload(&self) -> Result<&Value, WsBodyError> {
        self.validate_common()?;
        (self.content_kind == WsContentKind::Json)
            .then_some(&self.data)
            .ok_or(WsBodyError::InvalidPayload)
    }

    /// Returns the UTF-8 payload when this envelope declares text authority.
    pub fn text_payload(&self) -> Result<&str, WsBodyError> {
        self.validate_common()?;
        if self.content_kind != WsContentKind::Text {
            return Err(WsBodyError::InvalidPayload);
        }
        self.data.as_str().ok_or(WsBodyError::InvalidPayload)
    }

    /// Decodes and returns application bytes for a binary envelope.
    pub fn binary_payload(&self) -> Result<Vec<u8>, WsBodyError> {
        self.validate_common()?;
        if self.content_kind != WsContentKind::Binary {
            return Err(WsBodyError::InvalidPayload);
        }
        BASE64_STANDARD
            .decode(self.data.as_str().ok_or(WsBodyError::InvalidPayload)?)
            .map_err(|_| WsBodyError::InvalidPayload)
    }

    /// Bounded message metadata. This is the sole message-metadata authority.
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// Optional sender-assigned message identifier.
    pub fn message_id(&self) -> Option<&str> {
        self.message_id.as_deref()
    }

    /// Optional acknowledgement correlation authority.
    pub fn ack_id(&self) -> Option<&str> {
        self.ack_id.as_deref()
    }

    /// Explicit namespace, if supplied.
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    /// Public-room target, if supplied.
    pub fn room(&self) -> Option<&str> {
        self.room.as_deref()
    }
}

fn unix_timestamp_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn is_canonical_event(event: &str) -> bool {
    if event.is_empty() {
        return false;
    }
    if event.len() > MAX_CANONICAL_EVENT_BYTES {
        return false;
    }
    let mut parts = event.split(':');
    let first = parts.next().unwrap_or_default();
    let Some(second) = parts.next() else {
        return false;
    };
    if parts.next().is_some() {
        return false;
    }
    is_bounded_token(first, MAX_CANONICAL_NAMESPACE_BYTES)
        && is_bounded_token(second, MAX_CANONICAL_EVENT_BYTES)
}

pub(crate) fn is_canonical_namespace(namespace: &str) -> bool {
    is_bounded_token(namespace, MAX_CANONICAL_NAMESPACE_BYTES)
}

pub(crate) fn is_canonical_route_token(value: &str, max_len: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_len
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

pub(crate) fn is_canonical_room(value: &str, configured_max_len: usize) -> bool {
    is_canonical_route_token(value, configured_max_len.min(MAX_CANONICAL_ROOM_BYTES))
}

fn is_bounded_token(value: &str, max_len: usize) -> bool {
    is_canonical_route_token(value, max_len)
}

fn is_bounded_identifier(value: &str, max_len: usize) -> bool {
    if value.is_empty() {
        return false;
    }
    if value.len() > max_len {
        return false;
    }
    !value.bytes().any(|byte| byte.is_ascii_control())
}

fn is_valid_content_type(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CANONICAL_CONTENT_TYPE_BYTES
        && value.trim() == value
        && value.bytes().all(|byte| matches!(byte, 0x20..=0x7e))
}

fn deserialize_metadata<'de, D>(deserializer: D) -> Result<HashMap<String, String>, D::Error>
where
    D: Deserializer<'de>,
{
    struct MetadataVisitor;

    impl<'de> de::Visitor<'de> for MetadataVisitor {
        type Value = HashMap<String, String>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a duplicate-free Lily WebSocket metadata object")
        }

        fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
        where
            A: de::MapAccess<'de>,
        {
            let mut metadata = HashMap::with_capacity(
                access
                    .size_hint()
                    .unwrap_or_default()
                    .min(MAX_CANONICAL_METADATA_ENTRIES),
            );
            while let Some((key, value)) = access.next_entry::<String, String>()? {
                if metadata.contains_key(&key) {
                    return Err(de::Error::custom("duplicate WebSocket metadata key"));
                }
                if metadata.len() >= MAX_CANONICAL_METADATA_ENTRIES {
                    return Err(de::Error::custom("too many WebSocket metadata entries"));
                }
                metadata.insert(key, value);
            }
            Ok(metadata)
        }
    }

    deserializer.deserialize_map(MetadataVisitor)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
/// Strict envelope parsing, validation, or serialization failure.
pub enum WsBodyError {
    /// Frame is neither Text nor Binary application data.
    #[error("Unsupported WebSocket message type")]
    UnsupportedMessageType,
    /// JSON is malformed or does not match the strict schema.
    #[error("Invalid Lily WebSocket envelope")]
    InvalidFormat,
    /// The envelope protocol version is unsupported.
    #[error("Unsupported Lily WebSocket protocol version {actual}; expected {expected}")]
    UnsupportedVersion {
        /// Version supplied by the peer.
        actual: u16,
        /// Version required by this runtime.
        expected: u16,
    },
    /// Envelope kind is not accepted for inbound application messages.
    #[error("Unsupported Lily WebSocket message kind")]
    UnsupportedMessageKind,
    /// Event route is malformed or exceeds its bound.
    #[error("Invalid Lily WebSocket event route")]
    InvalidEvent,
    /// Content type is empty, non-printable, or exceeds its byte bound.
    #[error("Invalid Lily WebSocket content type")]
    InvalidContentType,
    /// Content kind and encoding are not a supported canonical pair.
    #[error("Invalid Lily WebSocket content encoding")]
    InvalidContentEncoding,
    /// The JSON representation does not match the declared content kind.
    #[error("Invalid Lily WebSocket application payload")]
    InvalidPayload,
    /// Namespace is malformed or exceeds its bound.
    #[error("Invalid Lily WebSocket namespace")]
    InvalidNamespace,
    /// Public-room token is malformed or exceeds its bound.
    #[error("Invalid Lily WebSocket room")]
    InvalidRoom,
    /// Message or acknowledgement identifier is invalid.
    #[error("Invalid Lily WebSocket message identifier")]
    InvalidIdentifier,
    /// An acknowledgement envelope omitted its correlation authority.
    #[error("Lily WebSocket acknowledgement is missing its correlation identifier")]
    MissingAcknowledgementAuthority,
    /// Timestamp is negative.
    #[error("Invalid Lily WebSocket timestamp")]
    InvalidTimestamp,
    /// Metadata count, key, or value exceeds its bound.
    #[error("Invalid Lily WebSocket metadata")]
    InvalidMetadata,
    /// A validated envelope could not be serialized.
    #[error("Lily WebSocket envelope serialization failed")]
    Serialization,
}

impl WsBodyError {
    /// Maps this internal parsing failure to a stable public wire code.
    pub fn protocol_error_code(self) -> WsProtocolErrorCode {
        match self {
            Self::UnsupportedVersion { .. } => WsProtocolErrorCode::UnsupportedVersion,
            Self::UnsupportedMessageKind => WsProtocolErrorCode::UnsupportedMessageKind,
            Self::InvalidEvent => WsProtocolErrorCode::InvalidRoute,
            _ => WsProtocolErrorCode::InvalidEnvelope,
        }
    }

    /// Maps this internal parsing failure to a bounded close reason.
    pub fn close_reason(self) -> WsCloseReason {
        match self {
            Self::UnsupportedVersion { .. } => WsCloseReason::UnsupportedVersion,
            Self::UnsupportedMessageKind => WsCloseReason::UnsupportedMessageKind,
            _ => WsCloseReason::InvalidEnvelope,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct SerializationFailure;

    impl Serialize for SerializationFailure {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom(
                "deliberate serialization failure",
            ))
        }
    }

    const GOLDEN_V2: &str = r#"{"protocol_version":2,"msg_type":"event","event":"chat:send","content_kind":"json","content_type":"application/json","encoding":"identity","data":{"text":"hello"},"namespace":"chat","room":"general","message_id":"msg-1","ack_id":"ack-1","timestamp":42,"metadata":{"client":"v2"}}"#;

    #[test]
    fn text_and_binary_use_the_same_versioned_golden_envelope() {
        let text = WsMessageBody::from_message(&Message::Text(GOLDEN_V2.into())).unwrap();
        let binary =
            WsMessageBody::from_message(&Message::Binary(GOLDEN_V2.as_bytes().to_vec())).unwrap();
        assert_eq!(text, binary);
        assert_eq!(text.protocol_version, LILY_WEBSOCKET_PROTOCOL_VERSION);
        assert_eq!(text.msg_type, WsMessageKind::Event);
        assert_eq!(text.event, "chat:send");
    }

    #[test]
    fn constructor_propagates_serialization_failure() {
        assert!(WsMessageBody::try_new("chat:send", SerializationFailure).is_err());
    }

    #[test]
    fn json_text_and_binary_payloads_have_distinct_explicit_authority() {
        let json = WsMessageBody::try_new("chat:json", serde_json::json!({ "ok": true })).unwrap();
        assert_eq!(json.content_kind(), WsContentKind::Json);
        assert_eq!(json.encoding(), WsContentEncoding::Identity);
        assert_eq!(json.json_payload().unwrap()["ok"], true);
        assert_eq!(json.text_payload(), Err(WsBodyError::InvalidPayload));

        let text = WsMessageBody::new_text("chat:text", "merhaba");
        assert_eq!(text.content_kind(), WsContentKind::Text);
        assert_eq!(text.content_type(), TEXT_CONTENT_TYPE);
        assert_eq!(text.text_payload().unwrap(), "merhaba");

        let bytes = [0, 1, 2, 255];
        let binary = WsMessageBody::new_binary("chat:binary", bytes);
        assert_eq!(binary.content_kind(), WsContentKind::Binary);
        assert_eq!(binary.encoding(), WsContentEncoding::Base64);
        assert_eq!(binary.data(), &Value::String("AAEC/w==".into()));
        assert_eq!(binary.binary_payload().unwrap(), bytes);

        let from_text_transport =
            WsMessageBody::from_message(&binary.to_message().unwrap()).unwrap();
        let from_binary_transport =
            WsMessageBody::from_message(&binary.to_binary_message().unwrap()).unwrap();
        assert_eq!(from_text_transport, from_binary_transport);
        assert_eq!(from_binary_transport.binary_payload().unwrap(), bytes);
    }

    #[test]
    fn content_kind_encoding_and_payload_shape_fail_closed() {
        let cases = [
            WsMessageBody {
                content_kind: WsContentKind::Text,
                content_type: TEXT_CONTENT_TYPE.into(),
                data: serde_json::json!({ "not": "text" }),
                ..WsMessageBody::default()
            },
            WsMessageBody {
                content_kind: WsContentKind::Binary,
                content_type: BINARY_CONTENT_TYPE.into(),
                encoding: WsContentEncoding::Identity,
                data: Value::String("AA==".into()),
                ..WsMessageBody::default()
            },
            WsMessageBody {
                content_kind: WsContentKind::Binary,
                content_type: BINARY_CONTENT_TYPE.into(),
                encoding: WsContentEncoding::Base64,
                data: Value::String("not-base64".into()),
                ..WsMessageBody::default()
            },
        ];

        assert_eq!(cases[0].validate_wire(), Err(WsBodyError::InvalidPayload));
        assert_eq!(
            cases[1].validate_wire(),
            Err(WsBodyError::InvalidContentEncoding)
        );
        assert_eq!(cases[2].validate_wire(), Err(WsBodyError::InvalidPayload));

        let missing_content_fields = Message::Text(
            r#"{"protocol_version":2,"msg_type":"event","event":"chat:send","data":null}"#.into(),
        );
        assert_eq!(
            WsMessageBody::from_message(&missing_content_fields).unwrap_err(),
            WsBodyError::InvalidFormat
        );
    }

    #[test]
    fn acknowledgement_is_real_but_never_a_normal_inbound_action() {
        let ack = WsMessageBody::try_ack("chat:send", "ack-42", serde_json::json!({ "ok": true }))
            .unwrap();
        assert_eq!(ack.msg_type, WsMessageKind::Ack);
        assert_eq!(ack.ack_id(), Some("ack-42"));
        assert_eq!(ack.validate_wire(), Ok(()));
        assert_eq!(
            WsMessageBody::from_message(&ack.to_message().unwrap()).unwrap_err(),
            WsBodyError::UnsupportedMessageKind
        );

        let missing_authority = WsMessageBody {
            ack_id: None,
            ..ack
        };
        assert_eq!(
            missing_authority.validate_wire(),
            Err(WsBodyError::MissingAcknowledgementAuthority)
        );
        assert_eq!(
            missing_authority.to_message(),
            Err(WsBodyError::MissingAcknowledgementAuthority)
        );
    }

    #[test]
    fn duplicate_metadata_keys_are_rejected_before_hash_map_collapse() {
        let duplicate = Message::Text(
            r#"{"protocol_version":2,"msg_type":"event","event":"chat:send","content_kind":"json","content_type":"application/json","encoding":"identity","data":null,"timestamp":42,"metadata":{"tenant":"first","tenant":"second"}}"#
                .into(),
        );
        assert_eq!(
            WsMessageBody::from_message(&duplicate).unwrap_err(),
            WsBodyError::InvalidFormat
        );
    }

    #[test]
    fn missing_unknown_and_unsupported_versions_fail_closed() {
        let missing = Message::Text(r#"{"event":"chat:send"}"#.into());
        assert_eq!(
            WsMessageBody::from_message(&missing).unwrap_err(),
            WsBodyError::InvalidFormat
        );

        let unknown = Message::Text(
            r#"{"protocol_version":2,"msg_type":"event","event":"chat:send","content_kind":"json","content_type":"application/json","encoding":"identity","data":null,"unknown":true}"#
                .into(),
        );
        assert_eq!(
            WsMessageBody::from_message(&unknown).unwrap_err(),
            WsBodyError::InvalidFormat
        );

        let unsupported = Message::Text(
            r#"{"protocol_version":9,"msg_type":"event","event":"chat:send","content_kind":"json","content_type":"application/json","encoding":"identity","data":null}"#
                .into(),
        );
        assert_eq!(
            WsMessageBody::from_message(&unsupported).unwrap_err(),
            WsBodyError::UnsupportedVersion {
                actual: 9,
                expected: 2
            }
        );
    }

    #[test]
    fn route_namespace_and_kind_validation_are_deterministic() {
        for event in ["", ":send", "chat:", "chat:admin:send", "chat send"] {
            let mut envelope = WsMessageBody::try_new(event, Value::Null).unwrap();
            envelope.timestamp = 42;
            assert_eq!(
                WsMessageBody::validate(envelope).unwrap_err(),
                WsBodyError::InvalidEvent
            );
        }

        let mut namespace = WsMessageBody::try_new("chat:send", Value::Null).unwrap();
        namespace.namespace = Some("../../admin".into());
        assert_eq!(
            WsMessageBody::validate(namespace).unwrap_err(),
            WsBodyError::InvalidNamespace
        );

        let mismatched_namespace = WsMessageBody::try_new("chat:send", Value::Null)
            .unwrap()
            .with_namespace("orders".into());
        assert_eq!(
            WsMessageBody::validate(mismatched_namespace).unwrap_err(),
            WsBodyError::InvalidNamespace
        );

        let mut kind = WsMessageBody::try_new("chat:send", Value::Null).unwrap();
        kind.msg_type = WsMessageKind::Connect;
        assert_eq!(
            WsMessageBody::validate(kind).unwrap_err(),
            WsBodyError::UnsupportedMessageKind
        );
    }

    #[test]
    fn protocol_errors_and_close_contract_are_stable_and_redacted() {
        let error = WsMessageBody::protocol_error(WsProtocolErrorCode::AuthorizationDenied);
        let json = serde_json::to_value(error).unwrap();
        assert_eq!(json["protocol_version"], 2);
        assert_eq!(json["msg_type"], "error");
        assert_eq!(json["event"], "lily:error");
        assert_eq!(json["data"]["code"], "ws.authorization_denied");
        assert!(json["data"].get("message").is_none());

        assert_eq!(WsCloseReason::InvalidEnvelope.code(), CloseCode::Protocol);
        assert_eq!(
            WsCloseReason::InvalidEnvelope.reason(),
            "lily.v2.invalid_envelope"
        );
        assert_eq!(WsCloseReason::PolicyViolation.code(), CloseCode::Policy);
        assert!(WsCloseReason::PolicyViolation.reason().len() <= 123);

        let slow_consumer = WsCloseReason::SlowConsumer;
        assert_eq!(u16::from(slow_consumer.code()), 1013);
        assert!(slow_consumer.code().is_allowed());
        assert_eq!(slow_consumer.reason(), "lily.v2.slow_consumer");
        assert!(slow_consumer.reason().len() <= 123);
        let frame = slow_consumer.frame();
        assert_eq!(frame.code, CloseCode::Again);
        assert_eq!(frame.reason.as_ref(), "lily.v2.slow_consumer");
    }

    #[test]
    fn envelope_validation_observes_exact_event_identifier_and_timestamp_boundaries() {
        let exact_event = format!("{}:{}", "a".repeat(128), "b".repeat(127));
        assert_eq!(exact_event.len(), MAX_CANONICAL_EVENT_BYTES);
        let exact = WsMessageBody {
            event: exact_event,
            message_id: Some("m".repeat(MAX_CANONICAL_ID_BYTES)),
            ack_id: Some("a".repeat(MAX_CANONICAL_ID_BYTES)),
            timestamp: 0,
            ..WsMessageBody::default()
        };
        assert_eq!(exact.validate_common(), Ok(()));

        let mut above_event = exact.clone();
        above_event.event = format!("{}:{}", "a".repeat(128), "b".repeat(128));
        assert_eq!(
            above_event.validate_common(),
            Err(WsBodyError::InvalidEvent)
        );

        for (message_id, ack_id) in [
            (Some("m".repeat(MAX_CANONICAL_ID_BYTES + 1)), None),
            (None, Some("a".repeat(MAX_CANONICAL_ID_BYTES + 1))),
            (Some(String::new()), None),
            (None, Some("a\0b".to_string())),
        ] {
            let envelope = WsMessageBody {
                message_id,
                ack_id,
                timestamp: 0,
                ..WsMessageBody::default()
            };
            assert_eq!(
                envelope.validate_common(),
                Err(WsBodyError::InvalidIdentifier)
            );
        }

        for timestamp in [0, 1] {
            let envelope = WsMessageBody {
                timestamp,
                ..WsMessageBody::default()
            };
            assert_eq!(envelope.validate_common(), Ok(()));
        }
        let invalid_timestamp = WsMessageBody {
            timestamp: -1,
            ..WsMessageBody::default()
        };
        assert_eq!(
            invalid_timestamp.validate_common(),
            Err(WsBodyError::InvalidTimestamp)
        );
    }

    #[test]
    fn metadata_validation_observes_each_exact_and_adjacent_bound() {
        let exact_entries = (0..MAX_CANONICAL_METADATA_ENTRIES)
            .map(|index| (format!("key-{index}"), "value".to_string()))
            .collect();
        let exact = WsMessageBody {
            timestamp: 0,
            metadata: exact_entries,
            ..WsMessageBody::default()
        };
        assert_eq!(exact.validate_common(), Ok(()));

        let mut above_count = exact.clone();
        above_count
            .metadata
            .insert("one-too-many".to_string(), "value".to_string());
        assert_eq!(
            above_count.validate_common(),
            Err(WsBodyError::InvalidMetadata)
        );

        for (key, value, expected) in [
            (
                "k".repeat(MAX_CANONICAL_METADATA_KEY_BYTES),
                "v".repeat(MAX_CANONICAL_METADATA_VALUE_BYTES),
                Ok(()),
            ),
            (
                "k".repeat(MAX_CANONICAL_METADATA_KEY_BYTES + 1),
                "value".to_string(),
                Err(WsBodyError::InvalidMetadata),
            ),
            (
                "key".to_string(),
                "v".repeat(MAX_CANONICAL_METADATA_VALUE_BYTES + 1),
                Err(WsBodyError::InvalidMetadata),
            ),
            (
                "key".to_string(),
                "value\0".to_string(),
                Err(WsBodyError::InvalidMetadata),
            ),
        ] {
            let envelope = WsMessageBody {
                timestamp: 0,
                metadata: HashMap::from([(key, value)]),
                ..WsMessageBody::default()
            };
            assert_eq!(envelope.validate_common(), expected);
        }
    }

    #[test]
    fn body_errors_map_to_exact_protocol_codes_and_close_reasons() {
        let cases = [
            (
                WsBodyError::UnsupportedVersion {
                    actual: 1,
                    expected: 2,
                },
                WsProtocolErrorCode::UnsupportedVersion,
                WsCloseReason::UnsupportedVersion,
            ),
            (
                WsBodyError::UnsupportedMessageKind,
                WsProtocolErrorCode::UnsupportedMessageKind,
                WsCloseReason::UnsupportedMessageKind,
            ),
            (
                WsBodyError::InvalidEvent,
                WsProtocolErrorCode::InvalidRoute,
                WsCloseReason::InvalidEnvelope,
            ),
            (
                WsBodyError::InvalidMetadata,
                WsProtocolErrorCode::InvalidEnvelope,
                WsCloseReason::InvalidEnvelope,
            ),
        ];

        for (error, protocol_code, close_reason) in cases {
            assert_eq!(error.protocol_error_code(), protocol_code);
            assert_eq!(error.close_reason(), close_reason);
        }
    }
}
