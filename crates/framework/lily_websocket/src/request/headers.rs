use std::collections::HashMap;

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use tokio_tungstenite::tungstenite::http::{HeaderName, HeaderValue};

/// Maximum number of raw HTTP header fields accepted during one Upgrade.
pub const MAX_HANDSHAKE_HEADER_ENTRIES: usize = 64;
/// Maximum byte length of one HTTP header name accepted during Upgrade.
pub const MAX_HANDSHAKE_HEADER_NAME_BYTES: usize = 128;
/// Maximum byte length of one unmerged HTTP header value accepted during Upgrade.
pub const MAX_HANDSHAKE_HEADER_VALUE_BYTES: usize = 8 * 1024;
/// Maximum byte length after merging repeatable WebSocket list headers.
pub const MAX_HANDSHAKE_MERGED_HEADER_VALUE_BYTES: usize = 16 * 1024;

const SEC_WEBSOCKET_KEY_ENCODED_BYTES: usize = 24;
const SEC_WEBSOCKET_KEY_NONCE_BYTES: usize = 16;

/// WebSocket-specific headers for connection upgrade and protocol negotiation
#[derive(Clone, Default)]
pub struct WsHeaders {
    /// WebSocket protocol version (usually 13)
    pub sec_websocket_version: Option<String>,
    /// WebSocket key for handshake
    pub sec_websocket_key: Option<String>,
    /// Requested WebSocket protocols
    pub sec_websocket_protocol: Option<Vec<String>>,
    /// WebSocket extensions
    pub sec_websocket_extensions: Option<Vec<String>>,
    /// Origin header for CORS validation
    pub origin: Option<String>,
    /// Host header
    pub host: Option<String>,
    /// Connection upgrade header
    pub connection: Option<String>,
    /// Upgrade header (should be "websocket")
    pub upgrade: Option<String>,
    /// Custom headers for application-specific data
    custom_headers: HashMap<String, String>,
}

impl WsHeaders {
    /// Create new WebSocket headers
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse headers from raw HTTP headers during WebSocket handshake
    pub fn try_from_http_headers(headers: &HashMap<String, String>) -> Result<Self, WsHeaderError> {
        if headers.len() > MAX_HANDSHAKE_HEADER_ENTRIES {
            return Err(WsHeaderError::TooManyHeaders);
        }
        let mut ws_headers = Self::new();

        for (key, value) in headers {
            validate_header_name(key)?;
            let key_lower = key.to_lowercase();
            validate_collected_header_value(&key_lower, value)?;
            match key_lower.as_str() {
                "sec-websocket-version" => {
                    ws_headers.sec_websocket_version = Some(value.clone());
                }
                "sec-websocket-key" => {
                    ws_headers.sec_websocket_key = Some(value.clone());
                }
                "sec-websocket-protocol" => {
                    ws_headers.sec_websocket_protocol =
                        Some(value.split(',').map(|s| s.trim().to_string()).collect());
                }
                "sec-websocket-extensions" => {
                    ws_headers.sec_websocket_extensions =
                        Some(value.split(',').map(|s| s.trim().to_string()).collect());
                }
                "origin" => {
                    ws_headers.origin = Some(value.clone());
                }
                "host" => {
                    ws_headers.host = Some(value.clone());
                }
                "connection" => {
                    ws_headers.connection = Some(value.clone());
                }
                "upgrade" => {
                    ws_headers.upgrade = Some(value.clone());
                }
                _ => {
                    // Store custom headers
                    ws_headers.custom_headers.insert(key_lower, value.clone());
                }
            }
        }

        Ok(ws_headers)
    }

    /// Validate WebSocket handshake headers
    pub fn validate_handshake(&self) -> Result<(), WsHeaderError> {
        // Check required headers for WebSocket upgrade
        let key = self
            .sec_websocket_key
            .as_deref()
            .ok_or(WsHeaderError::MissingSecWebSocketKey)?;
        if !is_valid_sec_websocket_key(key) {
            return Err(WsHeaderError::InvalidSecWebSocketKey);
        }

        if self.sec_websocket_version.as_ref() != Some(&"13".to_string()) {
            return Err(WsHeaderError::UnsupportedVersion);
        }

        if !self.connection.as_ref().is_some_and(|value| {
            value
                .split(',')
                .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
        }) {
            return Err(WsHeaderError::InvalidConnection);
        }

        if self.upgrade.as_ref().map(|u| u.to_lowercase()) != Some("websocket".to_string()) {
            return Err(WsHeaderError::InvalidUpgrade);
        }

        Ok(())
    }

    /// Get custom header by name
    pub fn get_custom_header(&self, name: &str) -> Option<&String> {
        self.custom_headers.get(&name.to_ascii_lowercase())
    }

    /// Set custom header
    pub fn set_custom_header(
        &mut self,
        name: String,
        value: String,
    ) -> Result<Option<String>, WsHeaderError> {
        validate_header_name(&name)?;
        validate_header_value(&value)?;
        let name = name.to_ascii_lowercase();
        if is_reserved_websocket_header(&name) {
            return Err(WsHeaderError::ReservedCustomHeader);
        }
        if !self.custom_headers.contains_key(&name)
            && self.normalized_header_count() >= MAX_HANDSHAKE_HEADER_ENTRIES
        {
            return Err(WsHeaderError::TooManyHeaders);
        }
        Ok(self.custom_headers.insert(name, value))
    }

    fn normalized_header_count(&self) -> usize {
        self.custom_headers.len()
            + usize::from(self.sec_websocket_version.is_some())
            + usize::from(self.sec_websocket_key.is_some())
            + usize::from(self.sec_websocket_protocol.is_some())
            + usize::from(self.sec_websocket_extensions.is_some())
            + usize::from(self.origin.is_some())
            + usize::from(self.host.is_some())
            + usize::from(self.connection.is_some())
            + usize::from(self.upgrade.is_some())
    }

    /// Get requested protocols
    pub fn get_protocols(&self) -> Option<&Vec<String>> {
        self.sec_websocket_protocol.as_ref()
    }

    /// Get WebSocket key for handshake response generation
    pub fn get_websocket_key(&self) -> Option<&String> {
        self.sec_websocket_key.as_ref()
    }
}

impl std::fmt::Debug for WsHeaders {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut custom_header_names = self.custom_headers.keys().cloned().collect::<Vec<_>>();
        custom_header_names.sort();
        formatter
            .debug_struct("WsHeaders")
            .field("has_sec_websocket_key", &self.sec_websocket_key.is_some())
            .field(
                "has_sec_websocket_version",
                &self.sec_websocket_version.is_some(),
            )
            .field(
                "requested_protocol_count",
                &self.sec_websocket_protocol.as_ref().map_or(0, Vec::len),
            )
            .field(
                "extension_count",
                &self.sec_websocket_extensions.as_ref().map_or(0, Vec::len),
            )
            .field("has_origin", &self.origin.is_some())
            .field("has_host", &self.host.is_some())
            .field("has_connection", &self.connection.is_some())
            .field("has_upgrade", &self.upgrade.is_some())
            .field("custom_header_names", &custom_header_names)
            .finish()
    }
}

/// WebSocket header validation errors
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WsHeaderError {
    /// Required `Sec-WebSocket-Key` is absent.
    #[error("Missing Sec-WebSocket-Key header")]
    MissingSecWebSocketKey,
    /// `Sec-WebSocket-Key` is not canonical Base64 for one 16-byte nonce.
    #[error("Invalid Sec-WebSocket-Key header")]
    InvalidSecWebSocketKey,
    /// The peer did not request RFC 6455 version 13.
    #[error("Unsupported WebSocket version")]
    UnsupportedVersion,
    /// `Connection` does not contain the `upgrade` token.
    #[error("Invalid Connection header")]
    InvalidConnection,
    /// `Upgrade` is not `websocket`.
    #[error("Invalid Upgrade header")]
    InvalidUpgrade,
    /// The Upgrade contains more raw header fields than Lily admits.
    #[error("WebSocket handshake contains too many headers")]
    TooManyHeaders,
    /// A header name is invalid or exceeds Lily's byte bound.
    #[error("WebSocket handshake header name is invalid")]
    InvalidHeaderName,
    /// One unmerged header value is invalid or exceeds Lily's byte bound.
    #[error("WebSocket handshake header value is invalid")]
    InvalidHeaderValue,
    /// A repeatable list header exceeded Lily's bound after deterministic merge.
    #[error("WebSocket handshake merged header value exceeds its byte bound")]
    MergedHeaderValueTooLarge,
    /// A non-list handshake header appeared more than once.
    #[error("WebSocket handshake contains an ambiguous duplicate header")]
    DuplicateHeader,
    /// Standard WebSocket handshake headers cannot be inserted as custom headers.
    #[error("Standard WebSocket handshake header cannot be set as a custom header")]
    ReservedCustomHeader,
}

pub(crate) fn validate_raw_header<'a>(
    name: &HeaderName,
    value: &'a HeaderValue,
) -> Result<&'a str, WsHeaderError> {
    if name.as_str().len() > MAX_HANDSHAKE_HEADER_NAME_BYTES {
        return Err(WsHeaderError::InvalidHeaderName);
    }
    let value = value
        .to_str()
        .map_err(|_| WsHeaderError::InvalidHeaderValue)?;
    validate_header_value(value)?;
    Ok(value)
}

pub(crate) fn validate_merged_header_value(value: &str) -> Result<(), WsHeaderError> {
    if value.len() > MAX_HANDSHAKE_MERGED_HEADER_VALUE_BYTES {
        return Err(WsHeaderError::MergedHeaderValueTooLarge);
    }
    Ok(())
}

fn validate_header_name(name: &str) -> Result<(), WsHeaderError> {
    if name.is_empty()
        || name.len() > MAX_HANDSHAKE_HEADER_NAME_BYTES
        || HeaderName::from_bytes(name.as_bytes()).is_err()
    {
        return Err(WsHeaderError::InvalidHeaderName);
    }
    Ok(())
}

fn validate_header_value(value: &str) -> Result<(), WsHeaderError> {
    if value.len() > MAX_HANDSHAKE_HEADER_VALUE_BYTES || HeaderValue::try_from(value).is_err() {
        return Err(WsHeaderError::InvalidHeaderValue);
    }
    Ok(())
}

fn validate_collected_header_value(name: &str, value: &str) -> Result<(), WsHeaderError> {
    if is_repeatable_websocket_header(name) {
        if HeaderValue::try_from(value).is_err() {
            return Err(WsHeaderError::InvalidHeaderValue);
        }
        validate_merged_header_value(value)
    } else {
        validate_header_value(value)
    }
}

fn is_valid_sec_websocket_key(key: &str) -> bool {
    key.len() == SEC_WEBSOCKET_KEY_ENCODED_BYTES
        && BASE64_STANDARD
            .decode(key)
            .is_ok_and(|decoded| decoded.len() == SEC_WEBSOCKET_KEY_NONCE_BYTES)
}

pub(crate) fn is_repeatable_websocket_header(name: &str) -> bool {
    matches!(name, "sec-websocket-protocol" | "sec-websocket-extensions")
}

fn is_reserved_websocket_header(name: &str) -> bool {
    matches!(
        name,
        "sec-websocket-version"
            | "sec-websocket-key"
            | "sec-websocket-protocol"
            | "sec-websocket-extensions"
            | "origin"
            | "host"
            | "connection"
            | "upgrade"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn handshake_headers(key: Option<&str>) -> WsHeaders {
        WsHeaders {
            sec_websocket_key: key.map(str::to_owned),
            sec_websocket_version: Some("13".to_owned()),
            connection: Some("keep-alive, Upgrade".to_owned()),
            upgrade: Some("websocket".to_owned()),
            ..WsHeaders::default()
        }
    }

    #[test]
    fn handshake_requires_a_canonical_sixteen_byte_websocket_nonce() {
        assert_eq!(
            handshake_headers(None).validate_handshake(),
            Err(WsHeaderError::MissingSecWebSocketKey)
        );

        for invalid in [
            "",
            "dGhlIHNhbXBsZSBub25jZQ==AAAAAAAAAA",
            "dGhlIHNhbXBsZSBub25jZQ!!",
            "AAAAAAAAAAAAAAAAAAAAAAAA",
        ] {
            assert_eq!(
                handshake_headers(Some(invalid)).validate_handshake(),
                Err(WsHeaderError::InvalidSecWebSocketKey),
                "key: {invalid:?}"
            );
        }

        assert_eq!(
            handshake_headers(Some("dGhlIHNhbXBsZSBub25jZQ==")).validate_handshake(),
            Ok(())
        );
    }

    #[test]
    fn custom_header_lookup_is_case_insensitive_and_debug_is_redacted() {
        let headers = WsHeaders::try_from_http_headers(&HashMap::from([
            ("Authorization".into(), "Bearer top-secret".into()),
            ("Sec-WebSocket-Key".into(), "key-secret".into()),
        ]))
        .unwrap();
        assert_eq!(
            headers
                .get_custom_header("AUTHORIZATION")
                .map(String::as_str),
            Some("Bearer top-secret")
        );
        let debug = format!("{headers:?}");
        assert!(!debug.contains("top-secret"));
        assert!(!debug.contains("key-secret"));
        assert!(debug.contains("authorization"));
    }

    #[test]
    fn debug_exposes_only_structural_handshake_metadata() {
        const SENTINEL: &str = "LILY_WS_HEADER_SECRET_7F31";

        let headers = WsHeaders {
            sec_websocket_version: Some(SENTINEL.to_string()),
            sec_websocket_key: Some(SENTINEL.to_string()),
            sec_websocket_protocol: Some(vec!["lily.v2".to_string(), SENTINEL.to_string()]),
            sec_websocket_extensions: Some(vec![SENTINEL.to_string()]),
            origin: Some(SENTINEL.to_string()),
            host: Some(SENTINEL.to_string()),
            connection: Some(SENTINEL.to_string()),
            upgrade: Some(SENTINEL.to_string()),
            custom_headers: HashMap::from([("authorization".to_string(), SENTINEL.to_string())]),
        };

        let protocols = headers
            .get_protocols()
            .expect("protocol values must remain available to production policy code");
        assert_eq!(protocols.len(), 2);
        assert_eq!(protocols[0], "lily.v2");
        assert_eq!(protocols[1], SENTINEL);
        assert_eq!(
            headers
                .get_custom_header("Authorization")
                .map(String::as_str),
            Some(SENTINEL)
        );

        let debug = format!("{headers:?}");
        assert!(!debug.contains(SENTINEL));
        assert!(debug.contains("has_sec_websocket_key: true"));
        assert!(debug.contains("has_sec_websocket_version: true"));
        assert!(debug.contains("requested_protocol_count: 2"));
        assert!(debug.contains("extension_count: 1"));
        assert!(debug.contains("has_origin: true"));
        assert!(debug.contains("has_host: true"));
        assert!(debug.contains("has_connection: true"));
        assert!(debug.contains("has_upgrade: true"));
        assert!(debug.contains("authorization"));
    }

    #[test]
    fn custom_headers_enforce_name_value_and_count_bounds() {
        let mut headers = WsHeaders::default();
        assert_eq!(
            headers
                .set_custom_header("x-request-id".into(), "first".into())
                .unwrap(),
            None
        );
        assert_eq!(
            headers
                .set_custom_header("X-Request-ID".into(), "second".into())
                .unwrap(),
            Some("first".into())
        );
        assert_eq!(
            headers.set_custom_header("origin".into(), "https://example.com".into()),
            Err(WsHeaderError::ReservedCustomHeader)
        );
        assert_eq!(
            headers.set_custom_header(
                "x-oversized".into(),
                "a".repeat(MAX_HANDSHAKE_HEADER_VALUE_BYTES + 1),
            ),
            Err(WsHeaderError::InvalidHeaderValue)
        );

        for index in 1..MAX_HANDSHAKE_HEADER_ENTRIES {
            headers
                .set_custom_header(format!("x-bounded-{index}"), "ok".into())
                .unwrap();
        }
        assert_eq!(
            headers.set_custom_header("x-overflow".into(), "no".into()),
            Err(WsHeaderError::TooManyHeaders)
        );
    }

    #[test]
    fn normalized_headers_enforce_merged_and_entry_bounds() {
        let too_many = (0..=MAX_HANDSHAKE_HEADER_ENTRIES)
            .map(|index| (format!("x-{index}"), "ok".into()))
            .collect();
        assert_eq!(
            WsHeaders::try_from_http_headers(&too_many).unwrap_err(),
            WsHeaderError::TooManyHeaders
        );

        let oversized_merge = HashMap::from([(
            "sec-websocket-protocol".into(),
            "a".repeat(MAX_HANDSHAKE_MERGED_HEADER_VALUE_BYTES + 1),
        )]);
        assert_eq!(
            WsHeaders::try_from_http_headers(&oversized_merge).unwrap_err(),
            WsHeaderError::MergedHeaderValueTooLarge
        );
    }
}
