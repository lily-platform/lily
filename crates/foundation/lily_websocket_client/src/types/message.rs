// =============================================================================
// Lily WebSocket v2 Message Types
// =============================================================================

use base64::{engine::general_purpose::STANDARD as BASE64_STANDARD, Engine as _};
use serde::{de, Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Current Lily message-envelope protocol version.
pub const LILY_WEBSOCKET_PROTOCOL_VERSION: u16 = 2;
/// WebSocket subprotocol token used for strict Lily v2 negotiation.
pub const LILY_WEBSOCKET_SUBPROTOCOL: &str = "lily.v2";
/// Canonical media type used by JSON payload constructors.
pub const JSON_CONTENT_TYPE: &str = "application/json";
/// Canonical media type used by UTF-8 text payload constructors.
pub const TEXT_CONTENT_TYPE: &str = "text/plain; charset=utf-8";
/// Canonical media type used by binary payload constructors.
pub const BINARY_CONTENT_TYPE: &str = "application/octet-stream";

const MAX_EVENT_BYTES: usize = 256;
const MAX_NAMESPACE_BYTES: usize = 128;
const MAX_ROOM_BYTES: usize = 128;
const MAX_IDENTIFIER_BYTES: usize = 128;
const MAX_METADATA_ENTRIES: usize = 32;
const MAX_METADATA_KEY_BYTES: usize = 128;
const MAX_METADATA_VALUE_BYTES: usize = 1024;
const MAX_CONTENT_TYPE_BYTES: usize = 128;

/// WebSocket message envelope shared byte-for-byte with the Lily v2 server.
///
/// The WebSocket Text/Binary transport-frame kind does not select the
/// application payload type. `content_kind` and `encoding` are the only
/// payload authorities.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WsMessage<T> {
    /// Required Lily envelope version.
    pub protocol_version: u16,
    /// Semantic envelope kind.
    pub msg_type: MessageType,
    /// Canonical `namespace:event` route.
    pub event: String,
    /// Explicit application payload representation.
    pub content_kind: ContentKind,
    /// Bounded printable application media type.
    pub content_type: String,
    /// Explicit encoding of the `data` member.
    pub encoding: ContentEncoding,
    /// Application payload representation.
    pub data: T,
    /// Optional Lily controller namespace selected for this envelope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
    /// Optional public Lily room target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub room: Option<String>,
    /// Optional id assigned by the sender for correlation/deduplication.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_id: Option<String>,
    /// Optional acknowledgement correlation authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ack_id: Option<String>,
    /// Message timestamp (Unix timestamp in milliseconds).
    #[serde(default)]
    pub timestamp: i64,
    /// Bounded transport metadata.
    #[serde(
        default,
        deserialize_with = "deserialize_metadata",
        skip_serializing_if = "HashMap::is_empty"
    )]
    pub metadata: HashMap<String, String>,
}

impl<T> WsMessage<T> {
    /// Creates a canonical JSON event message.
    pub fn new_event(event: String, data: T) -> Self {
        Self {
            protocol_version: LILY_WEBSOCKET_PROTOCOL_VERSION,
            msg_type: MessageType::Event,
            event,
            content_kind: ContentKind::Json,
            content_type: JSON_CONTENT_TYPE.into(),
            encoding: ContentEncoding::Identity,
            data,
            namespace: None,
            room: None,
            message_id: None,
            ack_id: None,
            timestamp: unix_timestamp_millis(),
            metadata: HashMap::new(),
        }
    }

    /// Creates a real acknowledgement correlated by `ack_id`.
    pub fn new_ack(event: String, ack_id: String, data: T) -> Self {
        let mut envelope = Self::new_event(event, data);
        envelope.msg_type = MessageType::Ack;
        envelope.ack_id = Some(ack_id);
        envelope
    }

    /// Sets the explicit namespace.
    pub fn with_namespace(mut self, namespace: String) -> Self {
        self.namespace = Some(namespace);
        self
    }

    /// Sets the public room.
    pub fn with_room(mut self, room: String) -> Self {
        self.room = Some(room);
        self
    }

    /// Sets acknowledgement correlation authority.
    pub fn with_ack_id(mut self, ack_id: String) -> Self {
        self.ack_id = Some(ack_id);
        self
    }

    /// Validates all bounded Lily v2 wire invariants.
    pub fn is_canonical(&self) -> bool
    where
        T: Serialize,
    {
        let Ok(data) = serde_json::to_value(&self.data) else {
            return false;
        };

        self.protocol_version == LILY_WEBSOCKET_PROTOCOL_VERSION
            && is_canonical_event(&self.event)
            && is_valid_content_type(&self.content_type)
            && content_type_matches(self.content_kind, &self.content_type)
            && content_is_canonical(self.content_kind, self.encoding, &data)
            && self.namespace.as_deref().is_none_or(|value| {
                is_bounded_token(value, MAX_NAMESPACE_BYTES)
                    && self.event.split_once(':').map(|(route, _)| route) == Some(value)
            })
            && self
                .room
                .as_deref()
                .is_none_or(|value| is_bounded_token(value, MAX_ROOM_BYTES))
            && self
                .message_id
                .as_deref()
                .is_none_or(|value| is_bounded_identifier(value, MAX_IDENTIFIER_BYTES))
            && self
                .ack_id
                .as_deref()
                .is_none_or(|value| is_bounded_identifier(value, MAX_IDENTIFIER_BYTES))
            && (self.msg_type != MessageType::Ack || self.ack_id.is_some())
            && self.timestamp >= 0
            && self.metadata.len() <= MAX_METADATA_ENTRIES
            && self.metadata.iter().all(|(key, value)| {
                is_bounded_identifier(key, MAX_METADATA_KEY_BYTES)
                    && value.len() <= MAX_METADATA_VALUE_BYTES
                    && !value.bytes().any(|byte| byte == 0)
            })
    }

    /// Returns whether this envelope may enter a normal event callback.
    /// Acknowledgements require a dedicated correlation owner.
    pub fn is_dispatchable_event(&self) -> bool
    where
        T: Serialize,
    {
        self.msg_type == MessageType::Event && self.is_canonical()
    }
}

impl WsMessage<Value> {
    /// Creates a canonical UTF-8 text event.
    pub fn new_text_event(event: String, text: String) -> Self {
        let mut envelope = Self::new_event(event, Value::String(text));
        envelope.content_kind = ContentKind::Text;
        envelope.content_type = TEXT_CONTENT_TYPE.into();
        envelope
    }

    /// Creates a canonical Base64-encoded binary event.
    pub fn new_binary_event(event: String, bytes: impl AsRef<[u8]>) -> Self {
        let mut envelope =
            Self::new_event(event, Value::String(BASE64_STANDARD.encode(bytes.as_ref())));
        envelope.content_kind = ContentKind::Binary;
        envelope.content_type = BINARY_CONTENT_TYPE.into();
        envelope.encoding = ContentEncoding::Base64;
        envelope
    }

    /// Decodes the explicit application payload without consulting the
    /// WebSocket transport-frame kind.
    pub fn decoded_payload(&self) -> Option<DecodedPayload> {
        if !self.is_canonical() {
            return None;
        }
        match self.content_kind {
            ContentKind::Json => Some(DecodedPayload::Json(self.data.clone())),
            ContentKind::Text => self
                .data
                .as_str()
                .map(|value| DecodedPayload::Text(value.to_owned())),
            ContentKind::Binary => BASE64_STANDARD
                .decode(self.data.as_str()?)
                .ok()
                .map(DecodedPayload::Binary),
        }
    }
}

/// Explicit semantic payload kind in the Lily v2 envelope.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContentKind {
    /// Arbitrary JSON value with identity encoding.
    #[default]
    Json,
    /// UTF-8 string with identity encoding.
    Text,
    /// Application bytes represented as padded Base64.
    Binary,
}

/// Encoding applied to the `data` member.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ContentEncoding {
    /// Value is represented directly.
    #[default]
    Identity,
    /// Binary bytes are represented as canonical padded Base64.
    Base64,
}

/// Decoded application payload delivered to client callbacks.
#[derive(Debug, Clone, PartialEq)]
pub enum DecodedPayload {
    /// Original JSON value.
    Json(Value),
    /// Decoded UTF-8 text.
    Text(String),
    /// Decoded application bytes.
    Binary(Vec<u8>),
}

/// Correlated terminal response to one client request.
///
/// Both variants carry the exact semantic payload selected by the server.
/// A server-side typed action error is a correlated rejection, while transport,
/// timeout, cancellation and protocol failures remain [`crate::WebSocketError`].
#[derive(Debug, Clone, PartialEq)]
pub enum WebSocketReply {
    /// The server emitted a real Lily `ack` envelope.
    Acknowledgement(DecodedPayload),
    /// The server emitted a correlated Lily `error` envelope.
    Rejection(DecodedPayload),
}

impl DecodedPayload {
    /// Converts the semantic payload into callback bytes.
    pub fn into_callback_bytes(self) -> Option<Vec<u8>> {
        match self {
            Self::Json(value) => serde_json::to_vec(&value).ok(),
            Self::Text(value) => Some(value.into_bytes()),
            Self::Binary(value) => Some(value),
        }
    }
}

/// Message type enumeration.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MessageType {
    /// Regular event message.
    #[default]
    Event,
    /// Acknowledgment owned by a correlation waiter, never a normal callback.
    Ack,
    /// Bounded protocol or application error.
    Error,
    /// Connection established lifecycle envelope.
    Connect,
    /// Connection closed lifecycle envelope.
    Disconnect,
}

impl std::fmt::Display for MessageType {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Event => write!(formatter, "event"),
            Self::Ack => write!(formatter, "ack"),
            Self::Error => write!(formatter, "error"),
            Self::Connect => write!(formatter, "connect"),
            Self::Disconnect => write!(formatter, "disconnect"),
        }
    }
}

fn unix_timestamp_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn content_is_canonical(kind: ContentKind, encoding: ContentEncoding, data: &Value) -> bool {
    match kind {
        ContentKind::Json => encoding == ContentEncoding::Identity,
        ContentKind::Text => encoding == ContentEncoding::Identity && data.is_string(),
        ContentKind::Binary => {
            if encoding != ContentEncoding::Base64 {
                return false;
            }
            let Some(encoded) = data.as_str() else {
                return false;
            };
            BASE64_STANDARD
                .decode(encoded)
                .is_ok_and(|decoded| BASE64_STANDARD.encode(decoded) == encoded)
        }
    }
}

fn content_type_matches(kind: ContentKind, content_type: &str) -> bool {
    match kind {
        ContentKind::Json => content_type == JSON_CONTENT_TYPE,
        ContentKind::Text => content_type == TEXT_CONTENT_TYPE,
        ContentKind::Binary => content_type == BINARY_CONTENT_TYPE,
    }
}

fn is_canonical_event(event: &str) -> bool {
    if event.is_empty() || event.len() > MAX_EVENT_BYTES {
        return false;
    }
    let mut parts = event.split(':');
    let first = parts.next().unwrap_or_default();
    let second = parts.next().unwrap_or_default();
    parts.next().is_none()
        && is_bounded_token(first, MAX_NAMESPACE_BYTES)
        && is_bounded_token(second, MAX_EVENT_BYTES)
}

fn is_bounded_token(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn is_bounded_identifier(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn is_valid_content_type(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_CONTENT_TYPE_BYTES
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
                    .min(MAX_METADATA_ENTRIES),
            );
            while let Some((key, value)) = access.next_entry::<String, String>()? {
                if metadata.contains_key(&key) {
                    return Err(de::Error::custom("duplicate WebSocket metadata key"));
                }
                if metadata.len() >= MAX_METADATA_ENTRIES {
                    return Err(de::Error::custom("too many WebSocket metadata entries"));
                }
                metadata.insert(key, value);
            }
            Ok(metadata)
        }
    }

    deserializer.deserialize_map(MetadataVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const SERVER_GOLDEN_V2: &str = r#"{"protocol_version":2,"msg_type":"event","event":"chat:send","content_kind":"json","content_type":"application/json","encoding":"identity","data":{"text":"hello"},"namespace":"chat","room":"general","message_id":"msg-1","ack_id":"ack-1","timestamp":42,"metadata":{"client":"canonical"}}"#;

    #[test]
    fn parses_and_re_emits_the_server_v2_golden_envelope() {
        let envelope: WsMessage<Value> = serde_json::from_str(SERVER_GOLDEN_V2).unwrap();
        assert_eq!(envelope.protocol_version, LILY_WEBSOCKET_PROTOCOL_VERSION);
        assert_eq!(envelope.content_kind, ContentKind::Json);
        assert!(envelope.is_dispatchable_event());
        assert_eq!(
            serde_json::to_value(envelope).unwrap(),
            serde_json::from_str::<Value>(SERVER_GOLDEN_V2).unwrap()
        );
    }

    #[test]
    fn missing_content_authority_and_unknown_fields_fail_closed() {
        assert!(serde_json::from_value::<WsMessage<Value>>(json!({
            "protocol_version": 2,
            "msg_type": "event",
            "event": "chat:send",
            "data": null
        }))
        .is_err());
        assert!(serde_json::from_value::<WsMessage<Value>>(json!({
            "protocol_version": 2,
            "msg_type": "event",
            "event": "chat:send",
            "content_kind": "json",
            "content_type": "application/json",
            "encoding": "identity",
            "data": null,
            "unknown": true
        }))
        .is_err());
    }

    #[test]
    fn json_text_and_binary_round_trip_with_distinct_callback_bytes() {
        let json = WsMessage::new_event("chat:json".into(), json!({ "ok": true }));
        assert_eq!(
            json.decoded_payload()
                .unwrap()
                .into_callback_bytes()
                .unwrap(),
            br#"{"ok":true}"#
        );

        let text = WsMessage::new_text_event("chat:text".into(), "hello".into());
        assert_eq!(
            text.decoded_payload()
                .unwrap()
                .into_callback_bytes()
                .unwrap(),
            b"hello"
        );

        let binary = WsMessage::new_binary_event("chat:binary".into(), [0, 1, 2, 255]);
        assert_eq!(binary.data, Value::String("AAEC/w==".into()));
        assert_eq!(
            binary
                .decoded_payload()
                .unwrap()
                .into_callback_bytes()
                .unwrap(),
            [0, 1, 2, 255]
        );
    }

    #[test]
    fn content_mismatches_and_transport_bounds_fail_closed() {
        let mut invalid_text = WsMessage::new_event("chat:text".into(), Value::Null);
        invalid_text.content_kind = ContentKind::Text;
        assert!(!invalid_text.is_canonical());

        let mut invalid_binary = WsMessage::new_binary_event("chat:binary".into(), [0]);
        invalid_binary.data = Value::String("AA".into());
        assert!(!invalid_binary.is_canonical());

        let invalid_route = WsMessage::new_event("chat:admin:send".into(), Value::Null);
        assert!(!invalid_route.is_canonical());

        let mismatched_namespace =
            WsMessage::new_event("chat:send".into(), Value::Null).with_namespace("orders".into());
        assert!(!mismatched_namespace.is_canonical());

        let mut invalid_metadata = WsMessage::new_event("chat:send".into(), Value::Null);
        invalid_metadata
            .metadata
            .insert("tenant".into(), "x".repeat(MAX_METADATA_VALUE_BYTES + 1));
        assert!(!invalid_metadata.is_canonical());
    }

    #[test]
    fn real_ack_never_has_normal_callback_authority() {
        let ack = WsMessage::new_ack("chat:send".into(), "ack-7".into(), Value::Null);
        assert!(ack.is_canonical());
        assert_eq!(ack.msg_type, MessageType::Ack);
        assert!(!ack.is_dispatchable_event());

        let missing_authority = WsMessage {
            ack_id: None,
            ..ack
        };
        assert!(!missing_authority.is_canonical());
    }

    #[test]
    fn duplicate_metadata_keys_are_rejected_before_hash_map_collapse() {
        let duplicate = r#"{"protocol_version":2,"msg_type":"event","event":"chat:send","content_kind":"json","content_type":"application/json","encoding":"identity","data":null,"timestamp":42,"metadata":{"tenant":"first","tenant":"second"}}"#;
        assert!(serde_json::from_str::<WsMessage<Value>>(duplicate).is_err());
    }
}
