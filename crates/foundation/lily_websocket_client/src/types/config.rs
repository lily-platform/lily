// =============================================================================
// WebSocket Configuration Types
// =============================================================================

use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::time::Duration;
use tokio_tungstenite::tungstenite::http::header::{HeaderName, HeaderValue};
use url::Url;

/// WebSocket client configuration
#[derive(Clone, Serialize, Deserialize)]
pub struct WebSocketClientConfig {
    /// WebSocket server URL
    #[serde(serialize_with = "serialize_redacted_url")]
    pub url: String,

    /// Optional Lily controller namespace selected during the HTTP Upgrade.
    ///
    /// When present, the value must be a non-empty ASCII route token of at
    /// most 128 bytes. It is added as the `namespace` query parameter when the
    /// URL does not already contain one. Lily servers require an exact
    /// controller namespace; `/` is not a wildcard.
    pub namespace: Option<String>,

    /// Reconnection configuration
    pub reconnection: ReconnectionConfig,

    /// Ping interval in seconds (default: 30)
    #[serde(default = "default_ping_interval")]
    pub ping_interval_secs: u64,

    /// Pong timeout in seconds (default: 10)
    #[serde(default = "default_pong_timeout")]
    pub pong_timeout_secs: u64,

    /// Maximum message size in bytes (default: 1MB)
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,

    /// Maximum size of a single frame. Fragmented messages are still bounded
    /// by `max_message_size`.
    #[serde(default = "default_max_frame_size")]
    pub max_frame_size: usize,

    /// Capacity of the bounded application-to-socket queue.
    #[serde(default = "default_outbound_queue_capacity")]
    pub outbound_queue_capacity: usize,

    /// Maximum time allowed for the TCP/TLS/WebSocket handshake.
    #[serde(default = "default_connect_timeout_secs")]
    pub connect_timeout_secs: u64,

    /// Maximum time a queued send may wait for the socket writer.
    #[serde(default = "default_send_timeout_secs")]
    pub send_timeout_secs: u64,

    /// Maximum time allowed for the closing handshake and runtime join.
    #[serde(default = "default_shutdown_timeout_secs")]
    pub shutdown_timeout_secs: u64,

    /// Maximum time spent waiting for the peer's Close frame. This budget is
    /// part of, and therefore cannot exceed, `shutdown_timeout_secs`.
    #[serde(default = "default_close_timeout_secs")]
    pub close_timeout_secs: u64,

    /// Close a connection that has produced no inbound frame for this period.
    #[serde(default = "default_idle_timeout_secs")]
    pub idle_timeout_secs: u64,

    /// Static headers added to every handshake. Secrets that rotate should be
    /// supplied through `AuthHeaderProvider` instead.
    #[serde(default, skip_serializing)]
    pub headers: HashMap<String, String>,

    /// Ordered WebSocket subprotocol preferences.
    #[serde(default)]
    pub subprotocols: Vec<String>,

    /// Fail the handshake when the server does not select one of the offered
    /// subprotocols. Offering protocols remains optional by default.
    #[serde(default)]
    pub require_subprotocol: bool,

    /// Capacity of the bounded inbound callback dispatch queue.
    #[serde(default = "default_callback_queue_capacity")]
    pub callback_queue_capacity: usize,

    /// Maximum number of callbacks executing concurrently.
    #[serde(default = "default_callback_concurrency")]
    pub callback_concurrency: usize,

    /// Maximum time an asynchronous callback may run before it is cancelled.
    #[serde(default = "default_callback_timeout_secs")]
    pub callback_timeout_secs: u64,

    /// Optional public CA bundle appended to the normal public WebPKI roots.
    /// Hostname verification is never disabled.
    #[serde(default)]
    pub additional_ca_bundle: Option<PathBuf>,
}

fn default_ping_interval() -> u64 {
    30
}

fn serialize_redacted_url<S>(value: &str, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut redacted = value.to_string();
    if let Ok(mut url) = Url::parse(value) {
        if url.query().is_some() {
            url.set_query(Some("REDACTED"));
        }
        redacted = url.to_string();
    }
    serializer.serialize_str(&redacted)
}

fn default_pong_timeout() -> u64 {
    10
}

fn default_max_message_size() -> usize {
    1024 * 1024 // 1MB
}

fn default_max_frame_size() -> usize {
    256 * 1024
}

fn default_outbound_queue_capacity() -> usize {
    256
}

fn default_connect_timeout_secs() -> u64 {
    10
}

fn default_send_timeout_secs() -> u64 {
    10
}

fn default_shutdown_timeout_secs() -> u64 {
    10
}

fn default_close_timeout_secs() -> u64 {
    5
}

fn default_idle_timeout_secs() -> u64 {
    90
}

fn default_callback_queue_capacity() -> usize {
    256
}

fn default_callback_concurrency() -> usize {
    16
}

fn default_callback_timeout_secs() -> u64 {
    30
}

impl Default for WebSocketClientConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            namespace: None,
            reconnection: ReconnectionConfig::default(),
            ping_interval_secs: 30,
            pong_timeout_secs: 10,
            max_message_size: 1024 * 1024,
            max_frame_size: 256 * 1024,
            outbound_queue_capacity: 256,
            connect_timeout_secs: 10,
            send_timeout_secs: 10,
            shutdown_timeout_secs: 10,
            close_timeout_secs: 5,
            idle_timeout_secs: 90,
            headers: HashMap::new(),
            subprotocols: Vec::new(),
            require_subprotocol: false,
            callback_queue_capacity: 256,
            callback_concurrency: 16,
            callback_timeout_secs: 30,
            additional_ca_bundle: None,
        }
    }
}

impl WebSocketClientConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        let parsed =
            Url::parse(&self.url).map_err(|_| "url must be an absolute URL".to_string())?;
        if !matches!(parsed.scheme(), "ws" | "wss") {
            return Err("url must use ws:// or wss://".into());
        }
        if parsed.host_str().is_none() {
            return Err("url must include a host".into());
        }
        if !parsed.username().is_empty() || parsed.password().is_some() {
            return Err(
                "credentials in the WebSocket URL are not supported; use an AuthHeaderProvider"
                    .into(),
            );
        }
        if parsed.fragment().is_some() {
            return Err(
                "url fragments are not sent in a WebSocket handshake and are not supported".into(),
            );
        }
        let namespace_values = parsed
            .query_pairs()
            .filter_map(|(name, value)| (name == "namespace").then_some(value.into_owned()))
            .collect::<Vec<_>>();
        if namespace_values.len() > 1 {
            return Err("url cannot contain duplicate namespace query parameters".into());
        }
        let configured_namespace = self.namespace.as_deref();
        let url_namespace = namespace_values.first().map(String::as_str);
        if let (Some(configured), Some(in_url)) = (configured_namespace, url_namespace) {
            if configured != in_url {
                return Err("url namespace and configured namespace must match".into());
            }
        }
        let namespace = configured_namespace.or(url_namespace).ok_or_else(|| {
            "an exact namespace must be supplied in config or the WebSocket URL".to_string()
        })?;
        if !is_canonical_namespace(namespace) {
            return Err(
                "namespace must be a bounded ASCII route token of at most 128 bytes".into(),
            );
        }
        for (name, _) in parsed.query_pairs() {
            if matches!(
                name.to_ascii_lowercase().as_str(),
                "access_token"
                    | "api_key"
                    | "apikey"
                    | "authorization"
                    | "password"
                    | "secret"
                    | "sig"
                    | "signature"
                    | "token"
            ) {
                return Err(format!(
                    "sensitive URL query parameter `{name}` is not supported; use an AuthHeaderProvider"
                ));
            }
        }
        if self.max_message_size == 0 || self.max_frame_size == 0 {
            return Err("message and frame limits must be greater than zero".into());
        }
        if self.max_frame_size > self.max_message_size {
            return Err("max_frame_size cannot exceed max_message_size".into());
        }
        if self.outbound_queue_capacity == 0 {
            return Err("outbound_queue_capacity must be greater than zero".into());
        }
        if self.callback_queue_capacity == 0 || self.callback_concurrency == 0 {
            return Err("callback queue and concurrency limits must be greater than zero".into());
        }
        if self.ping_interval_secs == 0
            || self.pong_timeout_secs == 0
            || self.idle_timeout_secs == 0
            || self.connect_timeout_secs == 0
            || self.send_timeout_secs == 0
            || self.shutdown_timeout_secs == 0
            || self.close_timeout_secs == 0
            || self.callback_timeout_secs == 0
        {
            return Err("timeouts and heartbeat intervals must be greater than zero".into());
        }
        if self.pong_timeout_secs >= self.idle_timeout_secs {
            return Err("pong_timeout_secs must be smaller than idle_timeout_secs".into());
        }
        if self.close_timeout_secs > self.shutdown_timeout_secs {
            return Err("close_timeout_secs cannot exceed shutdown_timeout_secs".into());
        }
        if self.max_message_size > 64 * 1024 * 1024 {
            return Err("max_message_size cannot exceed the 64 MiB hard safety limit".into());
        }
        if self.max_frame_size > 16 * 1024 * 1024 {
            return Err("max_frame_size cannot exceed the 16 MiB hard safety limit".into());
        }
        if self.outbound_queue_capacity > 65_536 || self.callback_queue_capacity > 65_536 {
            return Err("queue capacities cannot exceed the 65,536 item hard safety limit".into());
        }
        if self.callback_concurrency > 1_024 {
            return Err(
                "callback_concurrency cannot exceed the 1,024 task hard safety limit".into(),
            );
        }
        if self.headers.len() > 64 {
            return Err("at most 64 static handshake headers are supported".into());
        }
        self.reconnection.validate()?;
        for name in self.headers.keys() {
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
                return Err(format!("reserved WebSocket handshake header `{name}`"));
            }
            HeaderName::from_bytes(name.as_bytes())
                .map_err(|error| format!("invalid handshake header `{name}`: {error}"))?;
            HeaderValue::from_str(&self.headers[name])
                .map_err(|error| format!("invalid value for handshake header `{name}`: {error}"))?;
            if self.headers[name].len() > 8 * 1024 {
                return Err(format!(
                    "handshake header `{name}` exceeds the 8 KiB hard safety limit"
                ));
            }
        }
        for protocol in &self.subprotocols {
            if protocol.is_empty() || !protocol.bytes().all(is_http_token_byte) {
                return Err(format!("invalid WebSocket subprotocol `{protocol}`"));
            }
        }
        if self.require_subprotocol && self.subprotocols.is_empty() {
            return Err("require_subprotocol needs at least one offered subprotocol".into());
        }
        Ok(())
    }
}

fn is_http_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

fn is_canonical_namespace(namespace: &str) -> bool {
    !namespace.is_empty()
        && namespace.len() <= 128
        && namespace
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

impl fmt::Debug for WebSocketClientConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut redacted_url = self.url.clone();
        if let Ok(mut url) = Url::parse(&self.url) {
            if url.query().is_some() {
                url.set_query(Some("REDACTED"));
            }
            redacted_url = url.to_string();
        }
        let mut header_names = self.headers.keys().cloned().collect::<Vec<_>>();
        header_names.sort();

        formatter
            .debug_struct("WebSocketClientConfig")
            .field("url", &redacted_url)
            .field("namespace", &self.namespace)
            .field("reconnection", &self.reconnection)
            .field("ping_interval_secs", &self.ping_interval_secs)
            .field("pong_timeout_secs", &self.pong_timeout_secs)
            .field("max_message_size", &self.max_message_size)
            .field("max_frame_size", &self.max_frame_size)
            .field("outbound_queue_capacity", &self.outbound_queue_capacity)
            .field("connect_timeout_secs", &self.connect_timeout_secs)
            .field("send_timeout_secs", &self.send_timeout_secs)
            .field("shutdown_timeout_secs", &self.shutdown_timeout_secs)
            .field("close_timeout_secs", &self.close_timeout_secs)
            .field("idle_timeout_secs", &self.idle_timeout_secs)
            .field("header_names", &header_names)
            .field("subprotocols", &self.subprotocols)
            .field("require_subprotocol", &self.require_subprotocol)
            .field("callback_queue_capacity", &self.callback_queue_capacity)
            .field("callback_concurrency", &self.callback_concurrency)
            .field("callback_timeout_secs", &self.callback_timeout_secs)
            .field(
                "has_additional_ca_bundle",
                &self.additional_ca_bundle.is_some(),
            )
            .finish()
    }
}

/// Reconnection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconnectionConfig {
    /// Enable automatic reconnection
    pub enabled: bool,

    /// Maximum number of reconnection attempts
    pub max_retries: usize,

    /// Initial delay before first reconnection attempt (seconds)
    pub initial_delay_secs: u64,

    /// Maximum delay between reconnection attempts (seconds)
    pub max_delay_secs: u64,

    /// Backoff multiplier for exponential backoff
    pub backoff_multiplier: f64,

    /// Random variation applied to a computed delay (0.0..=1.0). Jitter
    /// prevents a fleet of clients reconnecting in lock-step.
    #[serde(default = "default_jitter_ratio")]
    pub jitter_ratio: f64,
}

fn default_jitter_ratio() -> f64 {
    0.2
}

impl Default for ReconnectionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_retries: 5,
            initial_delay_secs: 1,
            max_delay_secs: 30,
            backoff_multiplier: 2.0,
            jitter_ratio: 0.2,
        }
    }
}

impl ReconnectionConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if !self.backoff_multiplier.is_finite() || self.backoff_multiplier < 1.0 {
            return Err("reconnection backoff_multiplier must be finite and at least 1.0".into());
        }
        if !self.jitter_ratio.is_finite() || !(0.0..=1.0).contains(&self.jitter_ratio) {
            return Err("reconnection jitter_ratio must be between 0.0 and 1.0".into());
        }
        if self.initial_delay_secs > self.max_delay_secs {
            return Err("reconnection initial_delay_secs cannot exceed max_delay_secs".into());
        }
        if self.max_retries > 1_000 {
            return Err(
                "reconnection max_retries cannot exceed the 1,000 attempt hard safety limit".into(),
            );
        }
        Ok(())
    }

    pub(crate) fn calculate_delay(&self, attempt: usize) -> Duration {
        let multiplier = if self.backoff_multiplier.is_finite() {
            self.backoff_multiplier.max(1.0)
        } else {
            1.0
        };
        let bounded_attempt = attempt.min(1_000) as i32;
        let delay_secs = (self.initial_delay_secs as f64 * multiplier.powi(bounded_attempt))
            .min(self.max_delay_secs as f64);

        Duration::from_secs(delay_secs as u64)
    }

    /// Calculate exponential backoff with bounded full-width jitter.
    pub(crate) fn calculate_delay_with_jitter(&self, attempt: usize) -> Duration {
        use rand::Rng;

        let base = self.calculate_delay(attempt).as_secs_f64();
        let ratio = if self.jitter_ratio.is_finite() {
            self.jitter_ratio.clamp(0.0, 1.0)
        } else {
            0.0
        };
        let factor = if ratio == 0.0 {
            1.0
        } else {
            rand::thread_rng().gen_range((1.0 - ratio)..=(1.0 + ratio))
        };
        Duration::from_secs_f64((base * factor).min(self.max_delay_secs as f64))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_and_serialization_do_not_expose_header_or_query_secrets() {
        let mut config = WebSocketClientConfig {
            url: "wss://example.test/socket?token=query-secret".into(),
            ..Default::default()
        };
        config
            .headers
            .insert("authorization".into(), "Bearer header-secret".into());

        let debug = format!("{config:?}");
        assert!(!debug.contains("query-secret"));
        assert!(!debug.contains("header-secret"));
        assert!(debug.contains("authorization"));

        let serialized = serde_json::to_string(&config).unwrap();
        assert!(!serialized.contains("query-secret"));
        assert!(!serialized.contains("header-secret"));
        assert!(config.validate().is_err());
    }

    #[test]
    fn invalid_reconnect_and_protocol_settings_fail_during_construction() {
        let mut config = WebSocketClientConfig {
            url: "ws://localhost:8080".into(),
            ..Default::default()
        };
        config.reconnection.jitter_ratio = f64::NAN;
        assert!(config.validate().is_err());

        config.reconnection.jitter_ratio = 0.2;
        config.subprotocols.push("contains space".into());
        assert!(config.validate().is_err());
    }

    #[test]
    fn unsafe_resource_and_handshake_overrides_fail_before_io() {
        let mut config = WebSocketClientConfig {
            url: "wss://example.test/socket#not-on-the-wire".into(),
            ..Default::default()
        };
        assert!(config.validate().is_err());

        config.url = "wss://example.test/socket".into();
        config.headers.insert("Host".into(), "other.test".into());
        assert!(config.validate().is_err());

        config.headers.clear();
        config.close_timeout_secs = config.shutdown_timeout_secs + 1;
        assert!(config.validate().is_err());

        config.close_timeout_secs = 5;
        config.max_message_size = 64 * 1024 * 1024 + 1;
        assert!(config.validate().is_err());

        config.max_message_size = 1024 * 1024;
        config.reconnection.max_retries = 1_001;
        assert!(config.validate().is_err());
    }

    #[test]
    fn namespace_validation_matches_the_server_handshake_contract() {
        for namespace in ["chat", "tenant-42.events"] {
            let config = WebSocketClientConfig {
                url: "wss://example.test/socket".into(),
                namespace: Some(namespace.into()),
                ..Default::default()
            };
            assert!(config.validate().is_ok(), "{namespace}");
        }

        for namespace in ["", "/", "tenant/admin", "contains space"] {
            let config = WebSocketClientConfig {
                url: "wss://example.test/socket".into(),
                namespace: Some(namespace.into()),
                ..Default::default()
            };
            assert!(config.validate().is_err(), "{namespace}");
        }

        let overlong = WebSocketClientConfig {
            url: "wss://example.test/socket".into(),
            namespace: Some("n".repeat(129)),
            ..Default::default()
        };
        assert!(overlong.validate().is_err());

        let invalid_url_namespace = WebSocketClientConfig {
            url: "wss://example.test/socket?namespace=tenant%2Fadmin".into(),
            ..Default::default()
        };
        assert!(invalid_url_namespace.validate().is_err());

        let url_only = WebSocketClientConfig {
            url: "wss://example.test/socket?namespace=chat".into(),
            ..Default::default()
        };
        assert!(url_only.validate().is_ok());

        let matching_sources = WebSocketClientConfig {
            url: "wss://example.test/socket?namespace=chat".into(),
            namespace: Some("chat".into()),
            ..Default::default()
        };
        assert!(matching_sources.validate().is_ok());

        let missing = WebSocketClientConfig {
            url: "wss://example.test/socket".into(),
            ..Default::default()
        };
        assert!(missing.validate().is_err());

        let mismatched_sources = WebSocketClientConfig {
            url: "wss://example.test/socket?namespace=orders".into(),
            namespace: Some("chat".into()),
            ..Default::default()
        };
        assert!(mismatched_sources.validate().is_err());

        let duplicate_url_namespace = WebSocketClientConfig {
            url: "wss://example.test/socket?namespace=chat&namespace=chat".into(),
            ..Default::default()
        };
        assert!(duplicate_url_namespace.validate().is_err());
    }
}
