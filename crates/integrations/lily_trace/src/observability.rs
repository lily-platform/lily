//! Canonical observability vocabulary shared by Lily runtime adapters.
//!
//! This module deliberately contains policy, not a second tracing runtime.
//! Product crates continue to use `tracing` and OpenTelemetry directly while
//! sharing stable span names and leak/cardinality checks from here.

/// Stable operation span names. Operation names never contain a URL, route,
/// queue, connection, event, or other runtime value.
pub mod span_names {
    /// Inbound HTTP server request.
    pub const HTTP_SERVER_REQUEST: &str = "http.server.request";
    /// Outbound HTTP client request.
    pub const HTTP_CLIENT_REQUEST: &str = "http.client.request";
    /// HTTP authentication decision.
    pub const HTTP_AUTHENTICATION: &str = "http.server.authentication";
    /// HTTP authorization decision.
    pub const HTTP_AUTHORIZATION: &str = "http.server.authorization";
    /// Application HTTP handler execution.
    pub const HTTP_HANDLER: &str = "http.server.handler";
    /// HTTP dependency call nested below an application operation.
    pub const HTTP_DEPENDENCY: &str = "http.client.dependency";

    /// Local socket lifetime, including TLS and the bounded Upgrade read.
    pub const WEBSOCKET_TRANSPORT: &str = "websocket.transport";
    /// TLS negotiation and bounded HTTP Upgrade header read, before propagation.
    pub const WEBSOCKET_UPGRADE_READ: &str = "websocket.upgrade.read";
    /// Parsed WebSocket Upgrade validation and application handshake.
    pub const WEBSOCKET_HANDSHAKE: &str = "websocket.handshake";
    /// Parsed WebSocket connection lifecycle with its incoming remote parent.
    pub const WEBSOCKET_CONNECTION: &str = "websocket.connection";
    /// Owned disconnect and connection middleware cleanup.
    pub const WEBSOCKET_CONNECTION_CLEANUP: &str = "websocket.connection.cleanup";
    /// Container-owned disposal associated with one scope generation.
    pub const DI_SCOPE_DISPOSE: &str = "di.scope.dispose";
    /// WebSocket message handling.
    pub const WEBSOCKET_MESSAGE: &str = "websocket.message";
    /// Outbound WebSocket client connection attempt.
    pub const WEBSOCKET_CLIENT_CONNECT: &str = "websocket.client.connect";
    /// Outbound WebSocket client reconnection attempt.
    pub const WEBSOCKET_CLIENT_RECONNECT: &str = "websocket.client.reconnect";

    /// Messaging publish operation.
    pub const MESSAGING_PUBLISH: &str = "messaging.publish";
    /// Messaging consume operation.
    pub const MESSAGING_CONSUME: &str = "messaging.consume";
    /// Publisher resource acquisition.
    pub const MESSAGING_ACQUIRE: &str = "messaging.publish.acquire";
    /// Publisher broker call.
    pub const MESSAGING_BROKER: &str = "messaging.publish.broker";
    /// Publisher confirmation wait.
    pub const MESSAGING_CONFIRM: &str = "messaging.publish.confirm";
    /// Consumer message decoding.
    pub const MESSAGING_DECODE: &str = "messaging.consume.decode";
    /// Consumer application handler execution.
    pub const MESSAGING_HANDLER: &str = "messaging.consume.handler";
    /// Consumer delivery handoff.
    pub const MESSAGING_HANDOFF: &str = "messaging.consume.handoff";
    /// Consumer retry operation.
    pub const MESSAGING_RETRY: &str = "messaging.consume.retry";
    /// Consumer acknowledgement operation.
    pub const MESSAGING_ACK: &str = "messaging.consume.ack";
    /// Consumer negative-acknowledgement operation.
    pub const MESSAGING_NACK: &str = "messaging.consume.nack";
}

/// Returns whether a canonical operation name is one of the bounded names
/// owned by the framework. Application spans may use other *static* names;
/// qualification tests use this allow-list for framework-created roots.
pub fn is_canonical_operation_span(name: &str) -> bool {
    use span_names::*;
    matches!(
        name,
        HTTP_SERVER_REQUEST
            | HTTP_CLIENT_REQUEST
            | HTTP_AUTHENTICATION
            | HTTP_AUTHORIZATION
            | HTTP_HANDLER
            | HTTP_DEPENDENCY
            | WEBSOCKET_HANDSHAKE
            | WEBSOCKET_CONNECTION
            | WEBSOCKET_MESSAGE
            | WEBSOCKET_CLIENT_CONNECT
            | WEBSOCKET_CLIENT_RECONNECT
            | MESSAGING_PUBLISH
            | MESSAGING_CONSUME
            | MESSAGING_ACQUIRE
            | MESSAGING_BROKER
            | MESSAGING_CONFIRM
            | MESSAGING_DECODE
            | MESSAGING_HANDLER
            | MESSAGING_HANDOFF
            | MESSAGING_RETRY
            | MESSAGING_ACK
            | MESSAGING_NACK
    )
}

/// Attribute names forbidden on framework spans because their values are
/// routinely secret-bearing, payload-bearing, or unbounded. This is kept as
/// a pure function so product-crate leak-negative tests can audit their field
/// declarations without installing an exporter.
pub fn is_forbidden_span_attribute(key: &str) -> bool {
    let key = key.to_ascii_lowercase();
    matches!(
        key.as_str(),
        "url.full"
            | "url.path"
            | "http.target"
            | "http.request.body"
            | "http.response.body"
            | "messaging.message.payload"
            | "websocket.message.payload"
            | "authorization"
            | "http.request.header.authorization"
            | "cookie"
            | "set-cookie"
            | "token"
            | "password"
            | "secret"
            | "connection_string"
            | "db.statement"
            | "db.query.text"
            | "bson"
            | "error.message"
            | "otel.status_message"
    ) || key.ends_with(".payload")
        || key.ends_with(".body")
        || key.ends_with(".token")
        || key.ends_with(".password")
        || key.ends_with(".secret")
}

/// Identifiers allowed for trace correlation but forbidden as metric labels.
/// Per-operation identifiers would create an unbounded metric time-series
/// count even though they are useful on sampled spans.
pub fn is_high_cardinality_metric_label(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "request_id"
            | "connection_id"
            | "message_id"
            | "event_id"
            | "lily.request_id"
            | "lily.connection_id"
            | "lily.message_id"
            | "lily.event_id"
            | "messaging.message.id"
            | "messaging.rabbitmq.message.delivery_tag"
    )
}

/// Safe dynamic attribute/label values are short printable tokens. This is
/// suitable for error codes, outcomes, protocol versions and configured route
/// templates; it is not a sanitizer for arbitrary user input.
pub fn is_bounded_telemetry_token(value: &str, max_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'.' | b'_' | b'-' | b'/' | b':' | b'{' | b'}')
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_names_are_static_and_low_cardinality() {
        for name in [
            span_names::HTTP_SERVER_REQUEST,
            span_names::HTTP_CLIENT_REQUEST,
            span_names::WEBSOCKET_HANDSHAKE,
            span_names::WEBSOCKET_CONNECTION,
            span_names::WEBSOCKET_MESSAGE,
            span_names::MESSAGING_PUBLISH,
            span_names::MESSAGING_CONSUME,
        ] {
            assert!(is_canonical_operation_span(name));
            assert!(is_bounded_telemetry_token(name, 64));
        }
        assert!(!is_canonical_operation_span("GET /users/123"));
    }

    #[test]
    fn secret_payload_and_dynamic_metric_keys_are_rejected() {
        for key in [
            "url.full",
            "http.request.body",
            "authorization",
            "cookie",
            "messaging.message.payload",
            "otel.status_message",
        ] {
            assert!(is_forbidden_span_attribute(key), "{key}");
        }
        for key in [
            "lily.request_id",
            "lily.connection_id",
            "lily.event_id",
            "messaging.rabbitmq.message.delivery_tag",
        ] {
            assert!(is_high_cardinality_metric_label(key), "{key}");
        }
        assert!(!is_high_cardinality_metric_label("http.route"));
    }

    #[test]
    fn bounded_tokens_reject_whitespace_control_and_long_values() {
        assert!(is_bounded_telemetry_token("handler_timeout", 32));
        assert!(is_bounded_telemetry_token("/users/{id}", 64));
        assert!(!is_bounded_telemetry_token("contains secret", 64));
        assert!(!is_bounded_telemetry_token(&"x".repeat(65), 64));
    }
}
