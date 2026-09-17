//! Bounded WebSocket frame and payload codec contracts.
//!
//! Frame codecs own transport-envelope decoding and therefore run before
//! route lookup. Payload codecs run after the exact action is known. Keeping
//! these authorities separate makes controller-level custom wire protocols
//! compatible with action-level payload representations without probing a
//! frame with multiple codecs.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use lily_injection::Extensions;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_tungstenite::tungstenite::Message;

use crate::request::{
    BINARY_CONTENT_TYPE, JSON_CONTENT_TYPE, LILY_WEBSOCKET_PROTOCOL_VERSION,
    MAX_CANONICAL_EVENT_BYTES, MAX_CANONICAL_ID_BYTES, MAX_CANONICAL_METADATA_ENTRIES,
    MAX_CANONICAL_METADATA_KEY_BYTES, MAX_CANONICAL_METADATA_VALUE_BYTES,
    MAX_CANONICAL_NAMESPACE_BYTES, MAX_CANONICAL_ROOM_BYTES, TEXT_CONTENT_TYPE, WsBodyError,
    WsContentEncoding, WsContentKind, WsMessageBody, WsMessageKind, is_canonical_room,
};

/// Protocol version used by the typed Lily envelope.
pub const LILY_WEBSOCKET_TYPED_PROTOCOL_VERSION: u16 = LILY_WEBSOCKET_PROTOCOL_VERSION;
/// Maximum application metadata entries retained by one decoded message.
pub const MAX_WEBSOCKET_MESSAGE_HEADERS: usize = MAX_CANONICAL_METADATA_ENTRIES;
/// Maximum bytes retained for one application metadata key.
pub const MAX_WEBSOCKET_MESSAGE_HEADER_NAME_BYTES: usize = MAX_CANONICAL_METADATA_KEY_BYTES;
/// Maximum bytes retained for one application metadata value.
pub const MAX_WEBSOCKET_MESSAGE_HEADER_VALUE_BYTES: usize = MAX_CANONICAL_METADATA_VALUE_BYTES;
/// Maximum bytes retained for a content type or encoding token.
pub const MAX_WEBSOCKET_CONTENT_DESCRIPTOR_BYTES: usize = 128;
/// Maximum bytes retained for message, correlation, acknowledgement or room IDs.
pub const MAX_WEBSOCKET_IDENTIFIER_BYTES: usize = MAX_CANONICAL_ID_BYTES;

/// Physical WebSocket frame representation supplied to a frame codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WebSocketFrameKind {
    /// UTF-8 WebSocket text frame.
    Text,
    /// WebSocket binary frame.
    Binary,
}

/// An exact, size-checked application frame.
#[derive(Clone, PartialEq, Eq)]
pub struct RawEnvelope {
    kind: WebSocketFrameKind,
    bytes: Arc<[u8]>,
}

impl RawEnvelope {
    /// Converts a text or binary frame into an owned bounded envelope.
    pub fn try_from_message(
        message: Message,
        maximum_bytes: usize,
    ) -> Result<Self, WebSocketCodecError> {
        let (kind, bytes) = match message {
            Message::Text(text) => (WebSocketFrameKind::Text, text.into_bytes()),
            Message::Binary(bytes) => (WebSocketFrameKind::Binary, bytes),
            _ => return Err(WebSocketCodecError::unsupported_frame()),
        };
        if bytes.len() > maximum_bytes {
            return Err(WebSocketCodecError::frame_too_large());
        }
        Ok(Self {
            kind,
            bytes: Arc::from(bytes),
        })
    }

    /// Creates an envelope from already bounded bytes.
    pub fn try_new(
        kind: WebSocketFrameKind,
        bytes: impl Into<Vec<u8>>,
        maximum_bytes: usize,
    ) -> Result<Self, WebSocketCodecError> {
        let bytes = bytes.into();
        if bytes.len() > maximum_bytes {
            return Err(WebSocketCodecError::frame_too_large());
        }
        if kind == WebSocketFrameKind::Text && std::str::from_utf8(&bytes).is_err() {
            return Err(WebSocketCodecError::invalid_frame());
        }
        Ok(Self {
            kind,
            bytes: Arc::from(bytes),
        })
    }

    /// Original transport frame kind.
    #[must_use]
    pub const fn kind(&self) -> WebSocketFrameKind {
        self.kind
    }

    /// Exact frame payload bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Shared ownership of the exact frame payload bytes.
    #[must_use]
    pub fn into_bytes(self) -> Arc<[u8]> {
        self.bytes
    }

    /// Reconstitutes the corresponding Tungstenite application frame.
    pub fn to_message(&self) -> Result<Message, WebSocketCodecError> {
        match self.kind {
            WebSocketFrameKind::Text => std::str::from_utf8(&self.bytes)
                .map(|text| Message::Text(text.to_owned()))
                .map_err(|_| WebSocketCodecError::invalid_frame()),
            WebSocketFrameKind::Binary => Ok(Message::Binary(self.bytes.to_vec())),
        }
    }
}

impl fmt::Debug for RawEnvelope {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawEnvelope")
            .field("kind", &self.kind)
            .field("byte_length", &self.bytes.len())
            .finish()
    }
}

/// Semantic application payload representation declared by an envelope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WebSocketContentKind {
    /// JSON value.
    Json,
    /// UTF-8 text.
    Text,
    /// Binary application bytes.
    Binary,
    /// Codec-specific opaque bytes.
    Raw,
}

/// Payload representation returned by a frame codec before payload decoding.
#[derive(Clone, PartialEq)]
pub enum EncodedWebSocketPayloadData {
    /// JSON syntax tree retained without stringifying it.
    Json(Value),
    /// UTF-8 text or encoded textual representation.
    Text(String),
    /// Opaque encoded bytes.
    Bytes(Vec<u8>),
}

impl fmt::Debug for EncodedWebSocketPayloadData {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(_) => formatter.write_str("Json([REDACTED])"),
            Self::Text(value) => formatter
                .debug_struct("Text")
                .field("byte_length", &value.len())
                .finish(),
            Self::Bytes(value) => formatter
                .debug_struct("Bytes")
                .field("byte_length", &value.len())
                .finish(),
        }
    }
}

/// A bounded payload plus its explicit representation metadata.
#[derive(Debug, Clone, PartialEq)]
pub struct EncodedWebSocketPayload {
    kind: WebSocketContentKind,
    content_type: String,
    encoding: String,
    data: EncodedWebSocketPayloadData,
}

impl EncodedWebSocketPayload {
    /// Creates a validated encoded payload.
    pub fn try_new(
        kind: WebSocketContentKind,
        content_type: impl Into<String>,
        encoding: impl Into<String>,
        data: EncodedWebSocketPayloadData,
    ) -> Result<Self, WebSocketCodecError> {
        let content_type = content_type.into();
        let encoding = encoding.into();
        validate_descriptor(&content_type)?;
        validate_descriptor(&encoding)?;
        Ok(Self {
            kind,
            content_type,
            encoding,
            data,
        })
    }

    /// Declared content kind.
    #[must_use]
    pub const fn kind(&self) -> WebSocketContentKind {
        self.kind
    }

    /// Declared media type.
    #[must_use]
    pub fn content_type(&self) -> &str {
        &self.content_type
    }

    /// Declared transfer encoding.
    #[must_use]
    pub fn encoding(&self) -> &str {
        &self.encoding
    }

    /// Encoded application value.
    #[must_use]
    pub const fn data(&self) -> &EncodedWebSocketPayloadData {
        &self.data
    }

    /// Consumes the wrapper and returns its encoded application value.
    #[must_use]
    pub fn into_data(self) -> EncodedWebSocketPayloadData {
        self.data
    }
}

/// Codec-normalized payload consumed by typed payload extractors.
#[derive(Clone, PartialEq)]
pub enum DecodedWebSocketPayload {
    /// JSON value.
    Json(Value),
    /// Valid UTF-8 application text.
    Text(String),
    /// Decoded binary application bytes.
    Binary(Vec<u8>),
    /// Codec-specific opaque application bytes.
    Raw(Vec<u8>),
}

impl fmt::Debug for DecodedWebSocketPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Json(_) => formatter.write_str("Json([REDACTED])"),
            Self::Text(value) => formatter
                .debug_struct("Text")
                .field("byte_length", &value.len())
                .finish(),
            Self::Binary(value) => formatter
                .debug_struct("Binary")
                .field("byte_length", &value.len())
                .finish(),
            Self::Raw(value) => formatter
                .debug_struct("Raw")
                .field("byte_length", &value.len())
                .finish(),
        }
    }
}

impl DecodedWebSocketPayload {
    /// Semantic representation of this payload.
    #[must_use]
    pub const fn kind(&self) -> WebSocketContentKind {
        match self {
            Self::Json(_) => WebSocketContentKind::Json,
            Self::Text(_) => WebSocketContentKind::Text,
            Self::Binary(_) => WebSocketContentKind::Binary,
            Self::Raw(_) => WebSocketContentKind::Raw,
        }
    }
}

/// Semantic kind of a routed inbound message.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WebSocketInboundMessageKind {
    /// Application event routed to a controller action.
    Event,
    /// Acknowledgement owned by a correlation waiter rather than an action.
    Ack,
}

/// Validated application metadata carried by a message.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct WebSocketMessageHeaders {
    values: Arc<BTreeMap<String, String>>,
}

impl WebSocketMessageHeaders {
    /// Validates and owns a deterministic metadata map.
    pub fn try_new(
        values: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, WebSocketCodecError> {
        let mut normalized = BTreeMap::new();
        for (name, value) in values {
            if name.is_empty()
                || name.len() > MAX_WEBSOCKET_MESSAGE_HEADER_NAME_BYTES
                || name.bytes().any(|byte| byte.is_ascii_control())
                || value.len() > MAX_WEBSOCKET_MESSAGE_HEADER_VALUE_BYTES
                || value.bytes().any(|byte| byte == 0)
            {
                return Err(WebSocketCodecError::invalid_metadata());
            }
            if normalized.insert(name, value).is_some()
                || normalized.len() > MAX_WEBSOCKET_MESSAGE_HEADERS
            {
                return Err(WebSocketCodecError::invalid_metadata());
            }
        }
        Ok(Self {
            values: Arc::new(normalized),
        })
    }

    /// Returns a value by its exact application key.
    #[must_use]
    pub fn get(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }

    /// Deterministically ordered metadata entries.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = (&str, &str)> {
        self.values
            .iter()
            .map(|(name, value)| (name.as_str(), value.as_str()))
    }

    /// Number of metadata entries.
    #[must_use]
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Whether no application metadata is present.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl fmt::Debug for WebSocketMessageHeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebSocketMessageHeaders")
            .field("entry_count", &self.values.len())
            .finish()
    }
}

/// Frame-decoded message whose payload still belongs to the selected payload codec.
#[derive(Clone)]
pub struct DecodedWebSocketMessage {
    kind: WebSocketInboundMessageKind,
    namespace: String,
    event: String,
    payload: EncodedWebSocketPayload,
    message_id: Option<String>,
    ack_id: Option<String>,
    room: Option<String>,
    timestamp_millis: i64,
    headers: WebSocketMessageHeaders,
    raw_envelope: RawEnvelope,
}

/// Owned, zero-copy handoff from frame decode into the app-local dispatcher.
///
/// This is crate-private because applications extend frame codecs through
/// [`DecodedWebSocketMessage::try_new`], while only Lily's runtime needs to
/// split the accepted value into independently owned dispatch authorities.
pub(crate) struct DecodedWebSocketMessageParts {
    pub(crate) namespace: String,
    pub(crate) event: String,
    pub(crate) payload: EncodedWebSocketPayload,
    pub(crate) message_id: Option<String>,
    pub(crate) ack_id: Option<String>,
    pub(crate) room: Option<String>,
    pub(crate) timestamp_millis: i64,
    pub(crate) headers: WebSocketMessageHeaders,
    pub(crate) raw_envelope: RawEnvelope,
}

impl DecodedWebSocketMessage {
    /// Creates a decoded message while revalidating every route and bounded field.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        kind: WebSocketInboundMessageKind,
        namespace: impl Into<String>,
        event: impl Into<String>,
        payload: EncodedWebSocketPayload,
        message_id: Option<String>,
        ack_id: Option<String>,
        room: Option<String>,
        timestamp_millis: i64,
        headers: WebSocketMessageHeaders,
        raw_envelope: RawEnvelope,
    ) -> Result<Self, WebSocketCodecError> {
        let namespace = namespace.into();
        let event = event.into();
        validate_route_token(&namespace, MAX_CANONICAL_NAMESPACE_BYTES)?;
        validate_route_token(&event, MAX_CANONICAL_EVENT_BYTES)?;
        if namespace.len() + 1 + event.len() > MAX_CANONICAL_EVENT_BYTES {
            return Err(WebSocketCodecError::invalid_route());
        }
        for value in [message_id.as_deref(), ack_id.as_deref()]
            .into_iter()
            .flatten()
        {
            validate_identifier(value)?;
        }
        if let Some(room) = room.as_deref() {
            validate_room_token(room)?;
        }
        if kind == WebSocketInboundMessageKind::Ack && ack_id.is_none() {
            return Err(WebSocketCodecError::invalid_frame());
        }
        if timestamp_millis < 0 {
            return Err(WebSocketCodecError::invalid_frame());
        }
        Ok(Self {
            kind,
            namespace,
            event,
            payload,
            message_id,
            ack_id,
            room,
            timestamp_millis,
            headers,
            raw_envelope,
        })
    }

    /// Inbound semantic kind.
    #[must_use]
    pub const fn kind(&self) -> WebSocketInboundMessageKind {
        self.kind
    }

    /// Exact controller namespace.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Controller-local event name.
    #[must_use]
    pub fn event(&self) -> &str {
        &self.event
    }

    /// Canonical `namespace:event` route.
    #[must_use]
    pub fn route(&self) -> String {
        format!("{}:{}", self.namespace, self.event)
    }

    /// Payload awaiting the selected payload codec.
    #[must_use]
    pub const fn payload(&self) -> &EncodedWebSocketPayload {
        &self.payload
    }

    /// Consumes the message and returns its encoded payload.
    #[must_use]
    pub fn into_payload(self) -> EncodedWebSocketPayload {
        self.payload
    }

    /// Moves every decoded authority into the allocation-aware runtime handoff.
    #[must_use]
    pub(crate) fn into_runtime_parts(self) -> DecodedWebSocketMessageParts {
        DecodedWebSocketMessageParts {
            namespace: self.namespace,
            event: self.event,
            payload: self.payload,
            message_id: self.message_id,
            ack_id: self.ack_id,
            room: self.room,
            timestamp_millis: self.timestamp_millis,
            headers: self.headers,
            raw_envelope: self.raw_envelope,
        }
    }

    /// Optional application message identity.
    #[must_use]
    pub fn message_id(&self) -> Option<&str> {
        self.message_id.as_deref()
    }

    /// Optional acknowledgement authority supplied by the peer.
    #[must_use]
    pub fn ack_id(&self) -> Option<&str> {
        self.ack_id.as_deref()
    }

    /// Optional room token.
    #[must_use]
    pub fn room(&self) -> Option<&str> {
        self.room.as_deref()
    }

    /// Non-negative Unix timestamp in milliseconds.
    #[must_use]
    pub const fn timestamp_millis(&self) -> i64 {
        self.timestamp_millis
    }

    /// Bounded application metadata.
    #[must_use]
    pub const fn headers(&self) -> &WebSocketMessageHeaders {
        &self.headers
    }

    /// Exact original application frame.
    #[must_use]
    pub const fn raw_envelope(&self) -> &RawEnvelope {
        &self.raw_envelope
    }
}

impl fmt::Debug for DecodedWebSocketMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecodedWebSocketMessage")
            .field("kind", &self.kind)
            .field("namespace", &self.namespace)
            .field("event", &self.event)
            .field("payload_kind", &self.payload.kind)
            .field("has_message_id", &self.message_id.is_some())
            .field("has_ack_id", &self.ack_id.is_some())
            .field("has_room", &self.room.is_some())
            .field("timestamp_millis", &self.timestamp_millis)
            .field("header_count", &self.headers.len())
            .field("raw_byte_length", &self.raw_envelope.as_bytes().len())
            .finish()
    }
}

/// Semantic kind of an outbound typed result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WebSocketOutboundMessageKind {
    /// Event emitted to the current connection.
    Event,
    /// Acknowledgement correlated to an inbound request.
    Ack,
    /// Safe application or protocol error.
    Error,
}

/// Normalized outbound message supplied to the selected frame codec.
#[derive(Debug, Clone, PartialEq)]
pub struct EncodedWebSocketMessage {
    /// Outbound semantic kind.
    pub kind: WebSocketOutboundMessageKind,
    /// Canonical exact namespace.
    pub namespace: String,
    /// Controller-local event name.
    pub event: String,
    /// Payload encoded by the selected payload codec.
    pub payload: EncodedWebSocketPayload,
    /// Optional application message identity.
    pub message_id: Option<String>,
    /// Optional inbound acknowledgement/correlation authority.
    pub ack_id: Option<String>,
    /// Bounded application metadata.
    pub headers: WebSocketMessageHeaders,
    /// Preferred output transport representation.
    pub frame_kind: WebSocketFrameKind,
}

/// Secret-safe codec initialization failure category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum WebSocketCodecInitError {
    /// Required codec configuration is absent.
    #[error("WebSocket codec configuration is missing")]
    MissingConfiguration,
    /// Codec configuration is invalid.
    #[error("WebSocket codec configuration is invalid")]
    InvalidConfiguration,
    /// A codec dependency could not be initialized.
    #[error("WebSocket codec dependency initialization failed")]
    Dependency,
    /// Initialization failed internally.
    #[error("WebSocket codec initialization failed internally")]
    Internal,
}

impl WebSocketCodecInitError {
    /// Redacts a dependency failure to its stable category.
    pub fn dependency<E>(_source: E) -> Self {
        Self::Dependency
    }

    /// Redacts an implementation failure to its stable category.
    pub fn internal<E>(_source: E) -> Self {
        Self::Internal
    }
}

/// Stable codec failure category used for protocol mapping and telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum WebSocketCodecFailureKind {
    /// Frame type is not accepted by this codec.
    UnsupportedFrame,
    /// Frame exceeds its configured bound.
    FrameTooLarge,
    /// Envelope syntax or metadata is invalid.
    InvalidFrame,
    /// Protocol version is unsupported.
    UnsupportedVersion,
    /// Envelope kind is not accepted at the current boundary.
    UnsupportedMessageKind,
    /// Canonical route is invalid.
    InvalidRoute,
    /// Payload kind, content type or encoding is unsupported.
    UnsupportedContent,
    /// Payload cannot be decoded.
    InvalidPayload,
    /// Outbound value cannot be encoded.
    Encode,
    /// Codec implementation failed internally.
    Internal,
}

/// Source-preserving but value-redacting codec failure.
#[derive(Clone)]
pub struct WebSocketCodecError {
    kind: WebSocketCodecFailureKind,
    source: Option<Arc<dyn Error + Send + Sync>>,
}

impl WebSocketCodecError {
    /// Creates a stable codec failure without an internal source.
    #[must_use]
    pub const fn new(kind: WebSocketCodecFailureKind) -> Self {
        Self { kind, source: None }
    }

    /// Retains an internal source without exposing it through Display or Debug.
    pub fn with_source<E>(kind: WebSocketCodecFailureKind, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            kind,
            source: Some(Arc::new(source)),
        }
    }

    /// Stable failure category.
    #[must_use]
    pub const fn kind(&self) -> WebSocketCodecFailureKind {
        self.kind
    }

    /// Unsupported transport frame.
    #[must_use]
    pub const fn unsupported_frame() -> Self {
        Self::new(WebSocketCodecFailureKind::UnsupportedFrame)
    }

    /// Unsupported semantic envelope kind.
    #[must_use]
    pub const fn unsupported_message_kind() -> Self {
        Self::new(WebSocketCodecFailureKind::UnsupportedMessageKind)
    }

    /// Frame-size rejection.
    #[must_use]
    pub const fn frame_too_large() -> Self {
        Self::new(WebSocketCodecFailureKind::FrameTooLarge)
    }

    /// Invalid envelope rejection.
    #[must_use]
    pub const fn invalid_frame() -> Self {
        Self::new(WebSocketCodecFailureKind::InvalidFrame)
    }

    /// Invalid canonical route rejection.
    #[must_use]
    pub const fn invalid_route() -> Self {
        Self::new(WebSocketCodecFailureKind::InvalidRoute)
    }

    /// Invalid bounded metadata rejection.
    #[must_use]
    pub const fn invalid_metadata() -> Self {
        Self::new(WebSocketCodecFailureKind::InvalidFrame)
    }

    /// Unsupported content contract rejection.
    #[must_use]
    pub const fn unsupported_content() -> Self {
        Self::new(WebSocketCodecFailureKind::UnsupportedContent)
    }

    /// Invalid application payload rejection.
    #[must_use]
    pub const fn invalid_payload() -> Self {
        Self::new(WebSocketCodecFailureKind::InvalidPayload)
    }

    /// Outbound serialization failure.
    pub fn encode<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::with_source(WebSocketCodecFailureKind::Encode, source)
    }
}

impl fmt::Debug for WebSocketCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebSocketCodecError")
            .field("kind", &self.kind)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

impl fmt::Display for WebSocketCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "WebSocket codec {:?} failure", self.kind)
    }
}

impl Error for WebSocketCodecError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

/// Shared DI-aware construction contract for frame and payload codecs.
///
/// Custom codecs are trusted, cooperative application extensions. Lily isolates
/// construction in an abort-on-drop task so cancelling application build does
/// not detach the constructor, but it does not impose an internal construction
/// deadline. Implementations must not block a runtime worker and must arrange
/// their own bounded dependency I/O when they perform any.
#[async_trait]
pub trait WebSocketCodecFactory: Send + Sync + 'static {
    /// Constructs one app-owned codec instance.
    async fn new(extensions: Arc<Extensions>) -> Result<Self, WebSocketCodecInitError>
    where
        Self: Sized;
}

/// Controller-selected transport frame decoder/encoder.
///
/// Decode and encode calls run synchronously on the message task. Lily contains
/// panics and maps failures to typed protocol outcomes, but cannot preempt a
/// blocking implementation; custom codecs must keep both methods non-blocking
/// and bounded.
pub trait WebSocketFrameCodec: WebSocketCodecFactory {
    /// Decodes one exact bounded application frame and determines its route.
    fn decode_frame(
        &self,
        frame: RawEnvelope,
    ) -> Result<DecodedWebSocketMessage, WebSocketCodecError>;

    /// Encodes one normalized terminal message into one application frame.
    fn encode_frame(
        &self,
        message: EncodedWebSocketMessage,
    ) -> Result<Message, WebSocketCodecError>;
}

/// Action-selected payload decoder/encoder.
///
/// Decode and encode calls run synchronously on the message task. Lily contains
/// panics and maps failures to typed protocol outcomes, but cannot preempt a
/// blocking implementation; custom codecs must keep both methods non-blocking
/// and bounded.
pub trait WebSocketPayloadCodec: WebSocketCodecFactory {
    /// Converts frame-decoded content into one extractor-owned payload.
    fn decode_payload(
        &self,
        payload: EncodedWebSocketPayload,
    ) -> Result<DecodedWebSocketPayload, WebSocketCodecError>;

    /// Converts one typed output payload into frame-codec input.
    fn encode_payload(
        &self,
        payload: DecodedWebSocketPayload,
    ) -> Result<EncodedWebSocketPayload, WebSocketCodecError>;
}

/// Built-in strict Lily v2 frame and payload codec.
#[derive(Debug, Clone, Copy, Default)]
pub struct LilyEnvelopeCodec;

#[async_trait]
impl WebSocketCodecFactory for LilyEnvelopeCodec {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketCodecInitError> {
        Ok(Self)
    }
}

impl WebSocketFrameCodec for LilyEnvelopeCodec {
    fn decode_frame(
        &self,
        frame: RawEnvelope,
    ) -> Result<DecodedWebSocketMessage, WebSocketCodecError> {
        let envelope: WsMessageBody =
            serde_json::from_slice(frame.as_bytes()).map_err(|error| {
                WebSocketCodecError::with_source(WebSocketCodecFailureKind::InvalidFrame, error)
            })?;
        envelope.validate_wire().map_err(map_wire_body_error)?;
        let (namespace, event) = envelope
            .event
            .split_once(':')
            .ok_or_else(WebSocketCodecError::invalid_route)?;
        if envelope
            .namespace
            .as_deref()
            .is_some_and(|explicit| explicit != namespace)
        {
            return Err(WebSocketCodecError::invalid_route());
        }
        let kind = match envelope.msg_type {
            WsMessageKind::Event => WebSocketInboundMessageKind::Event,
            WsMessageKind::Ack => WebSocketInboundMessageKind::Ack,
            WsMessageKind::Error | WsMessageKind::Connect | WsMessageKind::Disconnect => {
                return Err(WebSocketCodecError::unsupported_message_kind());
            }
        };
        let data = match envelope.data {
            Value::String(value) if envelope.content_kind != WsContentKind::Json => {
                EncodedWebSocketPayloadData::Text(value)
            }
            value => EncodedWebSocketPayloadData::Json(value),
        };
        let payload = EncodedWebSocketPayload::try_new(
            content_kind_from_wire(envelope.content_kind),
            envelope.content_type,
            content_encoding_from_wire(envelope.encoding),
            data,
        )?;
        let headers = WebSocketMessageHeaders::try_new(envelope.metadata)?;
        DecodedWebSocketMessage::try_new(
            kind,
            namespace,
            event,
            payload,
            envelope.message_id,
            envelope.ack_id,
            envelope.room,
            envelope.timestamp,
            headers,
            frame,
        )
    }

    fn encode_frame(
        &self,
        message: EncodedWebSocketMessage,
    ) -> Result<Message, WebSocketCodecError> {
        let EncodedWebSocketMessage {
            kind,
            namespace,
            event,
            payload,
            message_id,
            ack_id,
            headers,
            frame_kind,
        } = message;
        validate_route_token(&namespace, MAX_CANONICAL_NAMESPACE_BYTES)?;
        validate_route_token(&event, MAX_CANONICAL_EVENT_BYTES)?;
        let route = format!("{namespace}:{event}");
        if route.len() > MAX_CANONICAL_EVENT_BYTES {
            return Err(WebSocketCodecError::invalid_route());
        }
        let data = match payload.data {
            EncodedWebSocketPayloadData::Json(value) => value,
            EncodedWebSocketPayloadData::Text(value) => Value::String(value),
            EncodedWebSocketPayloadData::Bytes(bytes) => Value::String(STANDARD.encode(bytes)),
        };
        let metadata = headers
            .iter()
            .map(|(name, value)| (name.to_owned(), value.to_owned()))
            .collect();
        let envelope = WsMessageBody {
            protocol_version: LILY_WEBSOCKET_TYPED_PROTOCOL_VERSION,
            msg_type: match kind {
                WebSocketOutboundMessageKind::Event => WsMessageKind::Event,
                WebSocketOutboundMessageKind::Ack => WsMessageKind::Ack,
                WebSocketOutboundMessageKind::Error => WsMessageKind::Error,
            },
            event: route,
            content_kind: content_kind_to_wire(payload.kind)?,
            content_type: payload.content_type,
            encoding: content_encoding_to_wire(&payload.encoding)?,
            data,
            namespace: Some(namespace),
            room: None,
            message_id,
            ack_id,
            timestamp: unix_timestamp_millis(),
            metadata,
        };
        match frame_kind {
            WebSocketFrameKind::Text => envelope.to_message().map_err(map_wire_body_error),
            WebSocketFrameKind::Binary => envelope.to_binary_message().map_err(map_wire_body_error),
        }
    }
}

impl WebSocketPayloadCodec for LilyEnvelopeCodec {
    fn decode_payload(
        &self,
        payload: EncodedWebSocketPayload,
    ) -> Result<DecodedWebSocketPayload, WebSocketCodecError> {
        match (
            payload.kind,
            payload.content_type.as_str(),
            payload.encoding.as_str(),
            payload.data,
        ) {
            (
                WebSocketContentKind::Json,
                JSON_CONTENT_TYPE,
                "identity",
                EncodedWebSocketPayloadData::Json(value),
            ) => Ok(DecodedWebSocketPayload::Json(value)),
            (
                WebSocketContentKind::Text,
                TEXT_CONTENT_TYPE,
                "identity",
                EncodedWebSocketPayloadData::Text(value),
            ) => Ok(DecodedWebSocketPayload::Text(value)),
            (
                WebSocketContentKind::Binary,
                BINARY_CONTENT_TYPE,
                "base64",
                EncodedWebSocketPayloadData::Text(value),
            ) => STANDARD
                .decode(value)
                .map(DecodedWebSocketPayload::Binary)
                .map_err(|error| {
                    WebSocketCodecError::with_source(
                        WebSocketCodecFailureKind::InvalidPayload,
                        error,
                    )
                }),
            _ => Err(WebSocketCodecError::unsupported_content()),
        }
    }

    fn encode_payload(
        &self,
        payload: DecodedWebSocketPayload,
    ) -> Result<EncodedWebSocketPayload, WebSocketCodecError> {
        match payload {
            DecodedWebSocketPayload::Json(value) => EncodedWebSocketPayload::try_new(
                WebSocketContentKind::Json,
                JSON_CONTENT_TYPE,
                "identity",
                EncodedWebSocketPayloadData::Json(value),
            ),
            DecodedWebSocketPayload::Text(value) => EncodedWebSocketPayload::try_new(
                WebSocketContentKind::Text,
                TEXT_CONTENT_TYPE,
                "identity",
                EncodedWebSocketPayloadData::Text(value),
            ),
            DecodedWebSocketPayload::Binary(value) => EncodedWebSocketPayload::try_new(
                WebSocketContentKind::Binary,
                BINARY_CONTENT_TYPE,
                "base64",
                EncodedWebSocketPayloadData::Text(STANDARD.encode(value)),
            ),
            DecodedWebSocketPayload::Raw(_) => Err(WebSocketCodecError::unsupported_content()),
        }
    }
}

const fn content_kind_from_wire(kind: WsContentKind) -> WebSocketContentKind {
    match kind {
        WsContentKind::Json => WebSocketContentKind::Json,
        WsContentKind::Text => WebSocketContentKind::Text,
        WsContentKind::Binary => WebSocketContentKind::Binary,
    }
}

fn content_kind_to_wire(kind: WebSocketContentKind) -> Result<WsContentKind, WebSocketCodecError> {
    match kind {
        WebSocketContentKind::Json => Ok(WsContentKind::Json),
        WebSocketContentKind::Text => Ok(WsContentKind::Text),
        WebSocketContentKind::Binary => Ok(WsContentKind::Binary),
        WebSocketContentKind::Raw => Err(WebSocketCodecError::unsupported_content()),
    }
}

const fn content_encoding_from_wire(encoding: WsContentEncoding) -> &'static str {
    match encoding {
        WsContentEncoding::Identity => "identity",
        WsContentEncoding::Base64 => "base64",
    }
}

fn content_encoding_to_wire(encoding: &str) -> Result<WsContentEncoding, WebSocketCodecError> {
    match encoding {
        "identity" => Ok(WsContentEncoding::Identity),
        "base64" => Ok(WsContentEncoding::Base64),
        _ => Err(WebSocketCodecError::unsupported_content()),
    }
}

fn map_wire_body_error(error: WsBodyError) -> WebSocketCodecError {
    let kind = match error {
        WsBodyError::UnsupportedMessageType => WebSocketCodecFailureKind::UnsupportedFrame,
        WsBodyError::InvalidFormat
        | WsBodyError::InvalidNamespace
        | WsBodyError::InvalidRoom
        | WsBodyError::InvalidIdentifier
        | WsBodyError::MissingAcknowledgementAuthority
        | WsBodyError::InvalidTimestamp
        | WsBodyError::InvalidMetadata => WebSocketCodecFailureKind::InvalidFrame,
        WsBodyError::UnsupportedVersion { .. } => WebSocketCodecFailureKind::UnsupportedVersion,
        WsBodyError::UnsupportedMessageKind => WebSocketCodecFailureKind::UnsupportedMessageKind,
        WsBodyError::InvalidEvent => WebSocketCodecFailureKind::InvalidRoute,
        WsBodyError::InvalidContentType | WsBodyError::InvalidContentEncoding => {
            WebSocketCodecFailureKind::UnsupportedContent
        }
        WsBodyError::InvalidPayload => WebSocketCodecFailureKind::InvalidPayload,
        WsBodyError::Serialization => WebSocketCodecFailureKind::Encode,
    };
    WebSocketCodecError::with_source(kind, error)
}

fn validate_descriptor(value: &str) -> Result<(), WebSocketCodecError> {
    if value.is_empty()
        || value.len() > MAX_WEBSOCKET_CONTENT_DESCRIPTOR_BYTES
        || value.trim() != value
        || !value.bytes().all(|byte| matches!(byte, 0x20..=0x7e))
    {
        return Err(WebSocketCodecError::unsupported_content());
    }
    Ok(())
}

fn validate_identifier(value: &str) -> Result<(), WebSocketCodecError> {
    if value.is_empty()
        || value.len() > MAX_WEBSOCKET_IDENTIFIER_BYTES
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(WebSocketCodecError::invalid_frame());
    }
    Ok(())
}

fn validate_route_token(value: &str, maximum_bytes: usize) -> Result<(), WebSocketCodecError> {
    if value.is_empty()
        || value.len() > maximum_bytes
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(WebSocketCodecError::invalid_route());
    }
    Ok(())
}

fn validate_room_token(value: &str) -> Result<(), WebSocketCodecError> {
    if !is_canonical_room(value, MAX_CANONICAL_ROOM_BYTES) {
        return Err(WebSocketCodecError::invalid_frame());
    }
    Ok(())
}

fn unix_timestamp_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy)]
    struct PipeCodec;

    #[async_trait::async_trait]
    impl WebSocketCodecFactory for PipeCodec {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketCodecInitError> {
            Ok(Self)
        }
    }

    impl WebSocketFrameCodec for PipeCodec {
        fn decode_frame(
            &self,
            frame: RawEnvelope,
        ) -> Result<DecodedWebSocketMessage, WebSocketCodecError> {
            let (namespace, event, payload) = {
                let input = std::str::from_utf8(frame.as_bytes()).map_err(|error| {
                    WebSocketCodecError::with_source(WebSocketCodecFailureKind::InvalidFrame, error)
                })?;
                let mut segments = input.splitn(3, '|');
                let namespace = segments
                    .next()
                    .ok_or_else(WebSocketCodecError::invalid_route)?;
                let event = segments
                    .next()
                    .ok_or_else(WebSocketCodecError::invalid_route)?;
                let payload = segments
                    .next()
                    .ok_or_else(WebSocketCodecError::invalid_payload)?;
                (
                    namespace.to_owned(),
                    event.to_owned(),
                    payload.as_bytes().to_vec(),
                )
            };
            DecodedWebSocketMessage::try_new(
                WebSocketInboundMessageKind::Event,
                namespace,
                event,
                EncodedWebSocketPayload::try_new(
                    WebSocketContentKind::Raw,
                    BINARY_CONTENT_TYPE,
                    "identity",
                    EncodedWebSocketPayloadData::Bytes(payload),
                )?,
                None,
                None,
                None,
                0,
                WebSocketMessageHeaders::default(),
                frame,
            )
        }

        fn encode_frame(
            &self,
            message: EncodedWebSocketMessage,
        ) -> Result<Message, WebSocketCodecError> {
            let EncodedWebSocketPayloadData::Bytes(payload) = message.payload.data else {
                return Err(WebSocketCodecError::unsupported_content());
            };
            let payload = std::str::from_utf8(&payload).map_err(|error| {
                WebSocketCodecError::with_source(WebSocketCodecFailureKind::InvalidPayload, error)
            })?;
            let frame = format!("{}|{}|{payload}", message.namespace, message.event);
            Ok(match message.frame_kind {
                WebSocketFrameKind::Text => Message::Text(frame),
                WebSocketFrameKind::Binary => Message::Binary(frame.into_bytes()),
            })
        }
    }

    impl WebSocketPayloadCodec for PipeCodec {
        fn decode_payload(
            &self,
            payload: EncodedWebSocketPayload,
        ) -> Result<DecodedWebSocketPayload, WebSocketCodecError> {
            match payload.data {
                EncodedWebSocketPayloadData::Bytes(value) => {
                    Ok(DecodedWebSocketPayload::Raw(value))
                }
                _ => Err(WebSocketCodecError::unsupported_content()),
            }
        }

        fn encode_payload(
            &self,
            payload: DecodedWebSocketPayload,
        ) -> Result<EncodedWebSocketPayload, WebSocketCodecError> {
            let DecodedWebSocketPayload::Raw(value) = payload else {
                return Err(WebSocketCodecError::unsupported_content());
            };
            EncodedWebSocketPayload::try_new(
                WebSocketContentKind::Raw,
                BINARY_CONTENT_TYPE,
                "identity",
                EncodedWebSocketPayloadData::Bytes(value),
            )
        }
    }

    fn raw(value: &str, kind: WebSocketFrameKind) -> RawEnvelope {
        RawEnvelope::try_new(kind, value.as_bytes(), 4096).unwrap()
    }

    fn encoded_payload_with_descriptors(
        content_type: impl Into<String>,
        encoding: impl Into<String>,
    ) -> Result<EncodedWebSocketPayload, WebSocketCodecError> {
        EncodedWebSocketPayload::try_new(
            WebSocketContentKind::Text,
            content_type,
            encoding,
            EncodedWebSocketPayloadData::Text("payload".to_owned()),
        )
    }

    #[test]
    fn encoded_payload_descriptors_require_bounded_trimmed_printable_ascii() {
        assert!(encoded_payload_with_descriptors("a", "b").is_ok());
        assert!(
            encoded_payload_with_descriptors(
                "a".repeat(MAX_WEBSOCKET_CONTENT_DESCRIPTOR_BYTES),
                "b".repeat(MAX_WEBSOCKET_CONTENT_DESCRIPTOR_BYTES),
            )
            .is_ok()
        );

        for invalid in [
            String::new(),
            "a".repeat(MAX_WEBSOCKET_CONTENT_DESCRIPTOR_BYTES + 1),
            " leading".to_owned(),
            "trailing ".to_owned(),
            "control\u{1f}".to_owned(),
            "non-ascii-é".to_owned(),
        ] {
            let content_type_error =
                encoded_payload_with_descriptors(invalid.clone(), "identity").unwrap_err();
            assert_eq!(
                content_type_error.kind(),
                WebSocketCodecFailureKind::UnsupportedContent,
                "content type descriptor unexpectedly accepted: {invalid:?}"
            );

            let encoding_error =
                encoded_payload_with_descriptors("text/plain", invalid.clone()).unwrap_err();
            assert_eq!(
                encoding_error.kind(),
                WebSocketCodecFailureKind::UnsupportedContent,
                "encoding descriptor unexpectedly accepted: {invalid:?}"
            );
        }
    }

    #[test]
    fn lily_v2_json_text_binary_and_original_frame_round_trip() {
        let codec = LilyEnvelopeCodec;
        for (payload, expected_kind) in [
            (
                DecodedWebSocketPayload::Json(serde_json::json!({"ok": true})),
                WebSocketContentKind::Json,
            ),
            (
                DecodedWebSocketPayload::Text("hello".to_owned()),
                WebSocketContentKind::Text,
            ),
            (
                DecodedWebSocketPayload::Binary(vec![0, 1, 2, 255]),
                WebSocketContentKind::Binary,
            ),
        ] {
            let encoded = codec.encode_payload(payload.clone()).unwrap();
            let message = codec
                .encode_frame(EncodedWebSocketMessage {
                    kind: WebSocketOutboundMessageKind::Event,
                    namespace: "chat".to_owned(),
                    event: "send".to_owned(),
                    payload: encoded,
                    message_id: Some("m-1".to_owned()),
                    ack_id: Some("a-1".to_owned()),
                    headers: WebSocketMessageHeaders::try_new([(
                        "tenant".to_owned(),
                        "north".to_owned(),
                    )])
                    .unwrap(),
                    frame_kind: WebSocketFrameKind::Text,
                })
                .unwrap();
            let exact = match &message {
                Message::Text(text) => text.clone(),
                _ => unreachable!(),
            };
            let decoded = codec
                .decode_frame(RawEnvelope::try_from_message(message, 4096).unwrap())
                .unwrap();
            assert_eq!(decoded.namespace(), "chat");
            assert_eq!(decoded.event(), "send");
            assert_eq!(decoded.raw_envelope().as_bytes(), exact.as_bytes());
            assert_eq!(decoded.payload().kind(), expected_kind);
            assert_eq!(
                codec.decode_payload(decoded.into_payload()).unwrap(),
                payload
            );
        }
    }

    #[test]
    fn malformed_version_route_metadata_and_base64_fail_closed() {
        let codec = LilyEnvelopeCodec;
        let unsupported = raw(
            r#"{"protocol_version":9,"msg_type":"event","event":"chat:send","content_kind":"json","content_type":"application/json","encoding":"identity","data":null,"timestamp":0}"#,
            WebSocketFrameKind::Text,
        );
        assert_eq!(
            codec.decode_frame(unsupported).unwrap_err().kind(),
            WebSocketCodecFailureKind::UnsupportedVersion
        );

        let invalid_route = raw(
            r#"{"protocol_version":2,"msg_type":"event","event":"chat:admin:send","content_kind":"json","content_type":"application/json","encoding":"identity","data":null,"timestamp":0}"#,
            WebSocketFrameKind::Text,
        );
        assert_eq!(
            codec.decode_frame(invalid_route).unwrap_err().kind(),
            WebSocketCodecFailureKind::InvalidRoute
        );

        let invalid_binary = EncodedWebSocketPayload::try_new(
            WebSocketContentKind::Binary,
            "application/octet-stream",
            "base64",
            EncodedWebSocketPayloadData::Text("%%%".to_owned()),
        )
        .unwrap();
        assert_eq!(
            codec.decode_payload(invalid_binary).unwrap_err().kind(),
            WebSocketCodecFailureKind::InvalidPayload
        );
        assert!(
            WebSocketMessageHeaders::try_new([
                ("tenant".to_owned(), "north".to_owned()),
                ("tenant".to_owned(), "south".to_owned()),
            ])
            .is_err()
        );
    }

    #[test]
    fn custom_frame_and_payload_codecs_preserve_the_public_runtime_contract() {
        let codec = PipeCodec;
        let original = DecodedWebSocketPayload::Raw(b"opaque".to_vec());
        let frame = codec
            .encode_frame(EncodedWebSocketMessage {
                kind: WebSocketOutboundMessageKind::Event,
                namespace: "chat".to_owned(),
                event: "send".to_owned(),
                payload: codec.encode_payload(original.clone()).unwrap(),
                message_id: None,
                ack_id: None,
                headers: WebSocketMessageHeaders::default(),
                frame_kind: WebSocketFrameKind::Binary,
            })
            .unwrap();
        let decoded = codec
            .decode_frame(RawEnvelope::try_from_message(frame, 128).unwrap())
            .unwrap();

        assert_eq!(decoded.route(), "chat:send");
        assert_eq!(
            codec.decode_payload(decoded.into_payload()).unwrap(),
            original
        );
    }

    #[test]
    fn debug_never_exposes_payload_or_internal_source() {
        let secret = "LILY_CODEC_SECRET";
        let envelope =
            RawEnvelope::try_new(WebSocketFrameKind::Text, secret.as_bytes(), 128).unwrap();
        assert!(!format!("{envelope:?}").contains(secret));
        let encoded = EncodedWebSocketPayloadData::Text(secret.to_owned());
        let decoded = DecodedWebSocketPayload::Text(secret.to_owned());
        assert!(!format!("{encoded:?}").contains(secret));
        assert!(!format!("{decoded:?}").contains(secret));

        let error = WebSocketCodecError::with_source(
            WebSocketCodecFailureKind::Internal,
            std::io::Error::other(secret),
        );
        assert!(!format!("{error:?}").contains(secret));
        assert!(!error.to_string().contains(secret));
        assert!(error.source().is_some());
    }
}
