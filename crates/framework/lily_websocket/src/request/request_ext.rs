use super::{
    BINARY_CONTENT_TYPE, JSON_CONTENT_TYPE, TEXT_CONTENT_TYPE, WsContentEncoding, WsContentKind,
    WsMessageBody, WsMessageKind, WsProtocolErrorCode, WsRequest, WsRequestError,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use serde::Serialize;
use serde_json::{Value, to_value};
use std::collections::HashMap;
use tokio_tungstenite::tungstenite::Message;

#[async_trait::async_trait]
/// Typed convenience accessors and response-envelope builders for
/// [`WsRequest`].
pub trait WsRequestExt {
    /// Returns a normalized handshake header by ASCII case-insensitive name.
    fn header(&self, name: &str) -> Option<&String>;

    /// Returns request metadata by its exact application key.
    fn metadata(&self, key: &str) -> Option<&String>;

    /// Parse JSON data from message body using ToJsonBytes
    async fn json<T>(&self) -> Result<T, WsRequestError>
    where
        T: serde::de::DeserializeOwned;

    /// Get message data as string
    fn text(&self) -> Result<String, WsRequestError>;

    /// Get message data as bytes
    fn bytes(&self) -> Result<Vec<u8>, WsRequestError>;

    /// Check if message is targeted to current connection's namespace
    fn is_for_current_namespace(&self) -> bool;

    /// Create response message for this request
    fn create_response<T: Serialize>(
        &self,
        event: &str,
        data: T,
    ) -> Result<WsMessageBody, serde_json::Error>;

    /// Create broadcast message to all connections in same namespace
    fn create_namespace_broadcast<T: Serialize>(
        &self,
        event: &str,
        data: T,
    ) -> Result<WsMessageBody, serde_json::Error>;

    /// Create broadcast message to all connections in a specific room
    fn create_room_broadcast<T: Serialize>(
        &self,
        room: &str,
        event: &str,
        data: T,
    ) -> Result<WsMessageBody, serde_json::Error>;

    /// Create error response
    fn create_error_response(&self, error_code: WsProtocolErrorCode) -> WsMessageBody;

    /// Whether `protocol` is the exact WebSocket subprotocol selected during
    /// the Upgrade.
    ///
    /// Client-offered protocols remain available through
    /// [`WsHeaders::get_protocols`](super::WsHeaders::get_protocols).
    fn supports_protocol(&self, protocol: &str) -> bool;

    /// Get WebSocket origin for CORS validation
    fn origin(&self) -> Option<&String>;

    /// Get WebSocket host
    fn host(&self) -> Option<&String>;

    /// Get user agent from custom headers
    fn user_agent(&self) -> Option<&String>;

    /// Check whether Lily accepted this connection through its TLS transport.
    fn is_secure(&self) -> bool;

    /// Elapsed time since the connection manager admitted the transport.
    ///
    /// This is an alias for [`WsRequest::connection_age`]. Per-message age is
    /// exposed separately by [`WsRequest::message_age`].
    fn uptime(&self) -> std::time::Duration;
}

#[async_trait::async_trait]
impl WsRequestExt for WsRequest {
    /// Get custom header by name from WebSocket headers
    fn header(&self, name: &str) -> Option<&String> {
        match name.to_lowercase().as_str() {
            "origin" => self.headers.origin.as_ref(),
            "host" => self.headers.host.as_ref(),
            "sec-websocket-version" => self.headers.sec_websocket_version.as_ref(),
            "sec-websocket-key" => self.headers.sec_websocket_key.as_ref(),
            "connection" => self.headers.connection.as_ref(),
            "upgrade" => self.headers.upgrade.as_ref(),
            _ => self.headers.get_custom_header(name),
        }
    }

    /// Get metadata by key with O(1) HashMap lookup
    fn metadata(&self, key: &str) -> Option<&String> {
        self.get_metadata(key)
    }

    /// Parse JSON data from message body
    async fn json<T>(&self) -> Result<T, WsRequestError>
    where
        T: serde::de::DeserializeOwned,
    {
        serde_json::from_value(self.json_payload()?.clone()).map_err(WsRequestError::Serialization)
    }

    /// Get message data as string
    fn text(&self) -> Result<String, WsRequestError> {
        self.text_payload()
            .map(str::to_owned)
            .map_err(WsRequestError::BodyParsing)
    }

    fn bytes(&self) -> Result<Vec<u8>, WsRequestError> {
        self.binary_payload().map_err(WsRequestError::BodyParsing)
    }

    /// Check if message is targeted to current connection's namespace
    fn is_for_current_namespace(&self) -> bool {
        match (self.namespace(), self.body().namespace()) {
            (Some(current), Some(target)) => current == target,
            (Some(_), None) => true, // Message without namespace goes to current namespace
            (None, None) => true,    // Both in default namespace
            (None, Some(_)) => false, // Connection in default, message targeted to specific namespace
        }
    }

    /// Create response message for this request
    fn create_response<T: Serialize>(
        &self,
        event: &str,
        data: T,
    ) -> Result<WsMessageBody, serde_json::Error> {
        let mut response = WsMessageBody::try_new(event, data)?;

        if let Some(ns) = self.namespace() {
            response = response.with_namespace(ns.to_string());
        }
        if let Some(room) = self.target_room() {
            response = response.with_room(room.to_string());
        }

        Ok(response)
    }

    /// Create broadcast message to all connections in same namespace
    fn create_namespace_broadcast<T: Serialize>(
        &self,
        event: &str,
        data: T,
    ) -> Result<WsMessageBody, serde_json::Error> {
        let mut builder = WsMessageBuilder::new(event).data(data)?;

        if let Some(namespace) = self.namespace() {
            builder = builder.namespace(namespace);
        }

        Ok(builder.build())
    }

    /// Create broadcast message to all connections in a specific room
    fn create_room_broadcast<T: Serialize>(
        &self,
        room: &str,
        event: &str,
        data: T,
    ) -> Result<WsMessageBody, serde_json::Error> {
        let mut builder = WsMessageBuilder::new(event).data(data)?.room(room);

        if let Some(namespace) = self.namespace() {
            builder = builder.namespace(namespace);
        }

        Ok(builder.build())
    }

    /// Create error response
    fn create_error_response(&self, error_code: WsProtocolErrorCode) -> WsMessageBody {
        let mut response = WsMessageBody::protocol_error(error_code);
        if let Some(room) = self.target_room() {
            response.room = Some(room.to_string());
        }
        response
    }

    /// Check the exact subprotocol selected during Upgrade.
    fn supports_protocol(&self, protocol: &str) -> bool {
        self.negotiated_subprotocol() == Some(protocol)
    }

    /// Get WebSocket origin for CORS validation
    fn origin(&self) -> Option<&String> {
        self.headers.origin.as_ref()
    }

    /// Get WebSocket host
    fn host(&self) -> Option<&String> {
        self.headers.host.as_ref()
    }

    /// Get user agent from custom headers
    fn user_agent(&self) -> Option<&String> {
        self.headers.get_custom_header("user-agent")
    }

    /// Returns only server-owned WS/WSS state; `Origin` is never trusted here.
    fn is_secure(&self) -> bool {
        WsRequest::is_secure(self)
    }

    /// Get connection uptime from the manager admission timestamp.
    fn uptime(&self) -> std::time::Duration {
        WsRequest::connection_age(self)
    }
}

/// WebSocket message builder for creating structured messages
#[derive(Debug, Clone)]
pub struct WsMessageBuilder {
    msg_type: WsMessageKind,
    event: String,
    content_kind: WsContentKind,
    content_type: String,
    encoding: WsContentEncoding,
    data: Value,
    namespace: Option<String>,
    room: Option<String>,
    ack_id: Option<String>,
    metadata: HashMap<String, String>,
}

impl WsMessageBuilder {
    /// Create new message builder
    pub fn new(event: &str) -> Self {
        Self {
            msg_type: WsMessageKind::Event,
            event: event.to_string(),
            content_kind: WsContentKind::Json,
            content_type: JSON_CONTENT_TYPE.to_string(),
            encoding: WsContentEncoding::Identity,
            data: Value::Null,
            namespace: None,
            room: None,
            ack_id: None,
            metadata: HashMap::new(),
        }
    }

    /// Set message data
    pub fn data<T: Serialize>(mut self, data: T) -> Result<Self, serde_json::Error> {
        self.data = to_value(data)?;
        self.content_kind = WsContentKind::Json;
        self.content_type = JSON_CONTENT_TYPE.to_string();
        self.encoding = WsContentEncoding::Identity;
        Ok(self)
    }

    /// Set a canonical UTF-8 text payload.
    pub fn text(mut self, text: impl Into<String>) -> Self {
        self.data = Value::String(text.into());
        self.content_kind = WsContentKind::Text;
        self.content_type = TEXT_CONTENT_TYPE.to_string();
        self.encoding = WsContentEncoding::Identity;
        self
    }

    /// Set a canonical Base64-encoded binary payload.
    pub fn binary(mut self, bytes: impl AsRef<[u8]>) -> Self {
        self.data = Value::String(BASE64_STANDARD.encode(bytes.as_ref()));
        self.content_kind = WsContentKind::Binary;
        self.content_type = BINARY_CONTENT_TYPE.to_string();
        self.encoding = WsContentEncoding::Base64;
        self
    }

    /// Set target namespace
    pub fn namespace(mut self, namespace: &str) -> Self {
        self.namespace = Some(namespace.to_string());
        self
    }

    /// Set target room
    pub fn room(mut self, room: &str) -> Self {
        self.room = Some(room.to_string());
        self
    }

    /// Set acknowledgment ID
    pub fn ack_id(mut self, ack_id: &str) -> Self {
        self.ack_id = Some(ack_id.to_string());
        self
    }

    /// Add metadata
    pub fn metadata(mut self, key: &str, value: String) -> Self {
        self.metadata.insert(key.to_string(), value);
        self
    }

    /// Build the message body
    pub fn build(self) -> WsMessageBody {
        let mut envelope = WsMessageBody {
            event: self.event,
            content_kind: self.content_kind,
            content_type: self.content_type,
            encoding: self.encoding,
            data: self.data,
            ..WsMessageBody::default()
        };
        envelope.msg_type = self.msg_type;
        envelope.namespace = self.namespace;
        envelope.room = self.room;
        envelope.ack_id = self.ack_id;
        envelope.metadata = self.metadata;
        envelope
    }

    /// Build and convert to WebSocket message
    pub fn build_message(self) -> Result<Message, WsRequestError> {
        self.build()
            .to_message()
            .map_err(WsRequestError::BodyParsing)
    }
}

/// Convenience functions for common message types
impl WsMessageBuilder {
    /// Create a simple text message
    pub fn text_message(text: &str) -> Result<Self, serde_json::Error> {
        Ok(Self::new("lily:message").text(text))
    }

    /// Create a JSON message
    pub fn json_message<T: Serialize>(data: T) -> Result<Self, serde_json::Error> {
        Self::new("lily:message").data(data)
    }

    /// Create an error message
    pub fn error_message(code: WsProtocolErrorCode) -> Result<Self, serde_json::Error> {
        let mut builder =
            Self::new("lily:error").data(serde_json::json!({ "code": code.as_str() }))?;
        builder.msg_type = WsMessageKind::Error;
        Ok(builder)
    }

    /// Create an acknowledgment message
    pub fn ack_message<T: Serialize>(ack_id: &str, data: T) -> Result<Self, serde_json::Error> {
        Self::ack_for("lily:ack", ack_id, data)
    }

    /// Create a true acknowledgement for the given canonical inbound event.
    pub fn ack_for<T: Serialize>(
        event: &str,
        ack_id: &str,
        data: T,
    ) -> Result<Self, serde_json::Error> {
        let mut builder = Self::new(event).data(data)?.ack_id(ack_id);
        builder.msg_type = WsMessageKind::Ack;
        Ok(builder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request::WsHeaders;
    use crate::server::WsTransportSecurity;
    use lily_web_core::RequestConnectionInfo;
    use std::sync::Arc;
    use uuid::Uuid;

    fn protocol_request(selected: Option<&str>) -> WsRequest {
        let mut headers = WsHeaders::default();
        headers.sec_websocket_protocol =
            Some(vec!["probe.unselected".to_owned(), "lily.v2".to_owned()]);
        let peer_addr = "127.0.0.1:43123".parse().unwrap();
        let connection_info = RequestConnectionInfo::direct("127.0.0.1".parse().unwrap());
        let request = WsRequest::new_from_message(
            Uuid::new_v4(),
            WsMessageBody::new_text("wire:echo", "payload")
                .to_message()
                .unwrap(),
            headers.clone(),
            connection_info,
            WsTransportSecurity::Plaintext,
        )
        .unwrap();
        request.with_handshake_context(Arc::new(crate::server::WsHandshakeContext::new(
            "wire".to_owned(),
            headers,
            None,
            selected.map(str::to_owned),
            peer_addr,
            connection_info,
            WsTransportSecurity::Plaintext,
        )))
    }

    #[test]
    fn supports_protocol_checks_only_the_negotiated_subprotocol() {
        let selected = protocol_request(Some("lily.v2"));

        assert_eq!(
            selected.headers().get_protocols().unwrap(),
            &["probe.unselected".to_owned(), "lily.v2".to_owned()]
        );
        assert!(!selected.supports_protocol("probe.unselected"));
        assert!(selected.supports_protocol("lily.v2"));
        assert!(!selected.supports_protocol("LILY.V2"));

        let unselected = protocol_request(None);
        assert!(!unselected.supports_protocol("probe.unselected"));
        assert!(!unselected.supports_protocol("lily.v2"));
    }

    #[test]
    fn error_response_keeps_canonical_protocol_namespace_and_round_trips_wire_validation() {
        let inbound = WsMessageBody::try_new("wire:echo", serde_json::json!({ "value": 7 }))
            .unwrap()
            .with_namespace("wire".into())
            .with_room("room-7".into());
        let request = WsRequest::new_from_message(
            Uuid::new_v4(),
            inbound.to_message().unwrap(),
            WsHeaders::default(),
            RequestConnectionInfo::default(),
            WsTransportSecurity::Plaintext,
        )
        .unwrap();

        assert_eq!(request.event(), "wire:echo");
        assert_eq!(request.namespace(), Some("wire"));

        let response = request.create_error_response(WsProtocolErrorCode::InvalidEnvelope);
        assert_eq!(response.event(), "lily:error");
        assert_eq!(response.namespace(), None);
        assert_eq!(response.room(), Some("room-7"));
        assert_eq!(response.validate_wire(), Ok(()));

        let serialized = response.to_message().unwrap();
        let Message::Text(serialized) = serialized else {
            panic!("protocol errors must use the canonical text envelope");
        };
        let decoded: WsMessageBody = serde_json::from_str(&serialized).unwrap();
        assert_eq!(decoded.event(), "lily:error");
        assert_eq!(decoded.namespace(), None);
        assert_eq!(decoded.validate_wire(), Ok(()));
    }
}
