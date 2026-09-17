use crate::request::{MAX_CANONICAL_ROOM_BYTES, WsHeaders};
use lily_web_core::{Principal, RequestConnectionInfo};
use std::collections::HashSet;
use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::Semaphore;

const MIN_WRITE_TIMEOUT_MILLIS: u64 = 100;
const MAX_WRITE_TIMEOUT_MILLIS: u64 = 300_000;
const MIN_OUTBOUND_ADMISSION_TIMEOUT_MILLIS: u64 = 100;
const MAX_OUTBOUND_ADMISSION_TIMEOUT_MILLIS: u64 = 300_000;
const MAX_WRITE_BUFFER_THRESHOLD_BYTES: usize = 16 * 1024 * 1024;
const MAX_WRITE_BUFFER_CEILING_BYTES: usize = 64 * 1024 * 1024;

const fn server_frame_header_bytes(payload_len: usize) -> usize {
    if payload_len < 126 {
        2
    } else if payload_len <= u16::MAX as usize {
        4
    } else {
        10
    }
}
const MAX_ENDPOINT_PATH_BYTES: usize = 256;
pub(crate) const MAX_EXECUTION_TIMEOUT_SECS: u64 = 300;

/// Server-authenticated transport security for one WebSocket connection.
///
/// This value is selected by the listener's WS/WSS branch. Browser `Origin`
/// and other client-controlled headers never influence it.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WsTransportSecurity {
    /// Plain RFC 6455 over TCP (`ws://`).
    #[default]
    Plaintext,
    /// RFC 6455 over a Lily-terminated TLS connection (`wss://`).
    Tls,
}

impl WsTransportSecurity {
    /// Whether the accepted connection is protected by Lily-terminated TLS.
    #[must_use]
    pub const fn is_secure(self) -> bool {
        matches!(self, Self::Tls)
    }
}

/// Data accepted at the HTTP Upgrade boundary. All fields are immutable and
/// copied into message/lifecycle contexts; raw credentials are never
/// reconstructed from per-message input.
#[derive(Clone)]
pub struct WsHandshakeContext {
    namespace: String,
    headers: WsHeaders,
    principal: Option<Principal>,
    subprotocol: Option<String>,
    peer_addr: SocketAddr,
    connection_info: RequestConnectionInfo,
    transport_security: WsTransportSecurity,
    trace_context: Option<lily_trace::core::W3CTraceContext>,
}

impl WsHandshakeContext {
    pub(crate) fn new(
        namespace: String,
        headers: WsHeaders,
        principal: Option<Principal>,
        subprotocol: Option<String>,
        peer_addr: SocketAddr,
        connection_info: RequestConnectionInfo,
        transport_security: WsTransportSecurity,
    ) -> Self {
        let trace_context = headers
            .get_custom_header("traceparent")
            .and_then(|value| lily_trace::core::W3CTraceContext::from_traceparent(value).ok());
        Self {
            namespace,
            headers,
            principal,
            subprotocol,
            peer_addr,
            connection_info,
            transport_security,
            trace_context,
        }
    }

    /// Namespace accepted at the Upgrade boundary.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Normalized Upgrade headers.
    pub fn headers(&self) -> &WsHeaders {
        &self.headers
    }

    /// Initial principal accepted at the HTTP Upgrade boundary, when present.
    ///
    /// This immutable audit snapshot does not change after an application
    /// re-authentication. Message code should use
    /// [`crate::WebSocketContext::principal`] or the typed principal extractor
    /// for the current message snapshot.
    pub fn principal(&self) -> Option<&Principal> {
        self.principal.as_ref()
    }

    /// Negotiated WebSocket subprotocol, when selected.
    pub fn subprotocol(&self) -> Option<&str> {
        self.subprotocol.as_deref()
    }

    /// Peer socket address captured by the listener.
    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Typed peer/effective-client identity established by transport policy.
    pub const fn connection_info(&self) -> RequestConnectionInfo {
        self.connection_info
    }

    /// Effective client address after trusted-proxy processing.
    pub fn client_ip(&self) -> Option<std::net::IpAddr> {
        self.connection_info.client_ip()
    }

    /// Whether an explicitly trusted proxy supplied the effective client IP.
    pub fn via_trusted_proxy(&self) -> bool {
        self.connection_info.via_trusted_proxy()
    }

    /// Server-owned WS/WSS transport classification.
    pub const fn transport_security(&self) -> WsTransportSecurity {
        self.transport_security
    }

    /// Whether this connection uses Lily-terminated TLS.
    pub const fn is_secure(&self) -> bool {
        self.transport_security.is_secure()
    }

    /// Browser `Origin` accepted by the server policy, when supplied.
    pub fn browser_origin(&self) -> Option<&str> {
        self.headers.origin.as_deref()
    }

    /// Valid remote W3C parent captured during the HTTP Upgrade. Invalid
    /// tracing input is ignored and never reaches application routing.
    pub fn trace_context(&self) -> Option<&lily_trace::core::W3CTraceContext> {
        self.trace_context.as_ref()
    }
}

impl fmt::Debug for WsHandshakeContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WsHandshakeContext")
            .field("namespace", &self.namespace)
            .field("has_origin", &self.headers.origin.is_some())
            .field("has_principal", &self.principal.is_some())
            .field("subprotocol", &self.subprotocol)
            .field("peer_addr", &"[REDACTED]")
            .field("via_trusted_proxy", &self.via_trusted_proxy())
            .field("transport_security", &self.transport_security)
            .field("has_trace_context", &self.trace_context.is_some())
            .finish()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct HandshakeDecision {
    pub(crate) subprotocol: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HandshakeRejection {
    OriginRequired,
    OriginDenied,
    SubprotocolRequired,
    InvalidSubprotocol,
    InvalidNamespace,
    NamespaceNotFound,
}

impl HandshakeRejection {
    pub(crate) fn status(self) -> tokio_tungstenite::tungstenite::http::StatusCode {
        use tokio_tungstenite::tungstenite::http::StatusCode;
        match self {
            Self::OriginRequired | Self::OriginDenied => StatusCode::FORBIDDEN,
            Self::SubprotocolRequired | Self::InvalidSubprotocol => StatusCode::BAD_REQUEST,
            Self::InvalidNamespace => StatusCode::BAD_REQUEST,
            Self::NamespaceNotFound => StatusCode::NOT_FOUND,
        }
    }

    pub(crate) fn response_message(self) -> &'static str {
        match self {
            Self::OriginRequired => "WebSocket Origin header is required",
            Self::OriginDenied => "WebSocket origin is not allowed",
            Self::SubprotocolRequired => "A supported WebSocket subprotocol is required",
            Self::InvalidSubprotocol => "WebSocket subprotocol header is invalid",
            Self::InvalidNamespace => "WebSocket namespace is invalid",
            Self::NamespaceNotFound => "WebSocket namespace is not registered",
        }
    }
}

pub(crate) fn parse_handshake_namespace(query: Option<&str>) -> Result<String, HandshakeRejection> {
    let mut namespaces = query
        .into_iter()
        .flat_map(|query| url::form_urlencoded::parse(query.as_bytes()))
        .filter_map(|(key, value)| (key == "namespace").then(|| value.into_owned()));
    let Some(namespace) = namespaces.next() else {
        return Err(HandshakeRejection::InvalidNamespace);
    };
    if namespaces.next().is_some() || !crate::request::is_canonical_namespace(&namespace) {
        return Err(HandshakeRejection::InvalidNamespace);
    }
    Ok(namespace)
}

/// CIDR network allowed to attest `X-Forwarded-For` client identity.
pub type TrustedProxyNetwork = ipnet::IpNet;

pub(crate) const DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE_BYTES: usize = 1024 * 1024;
pub(crate) const DEFAULT_OUTBOUND_ADMISSION_TIMEOUT_MILLIS: u64 = 5_000;

/// Configuration for the canonical `WsAppBuilder` server runtime.
#[derive(Clone)]
pub struct ServerConfig {
    /// Exact HTTP Upgrade endpoint. Per-controller paths are not part of routing.
    pub endpoint_path: String,
    /// Maximum number of concurrent connections. This cannot exceed
    /// [`Semaphore::MAX_PERMITS`].
    pub max_connections: usize,
    /// Maximum reassembled inbound message size in bytes.
    pub max_message_size: usize,
    /// Maximum size of one WebSocket frame. Fragmented messages remain
    /// bounded by `max_message_size`. This size bound is not a frame-assembly
    /// deadline and does not make transport UTF-8 validation incremental
    /// across partial TCP reads; see the crate-level known transport
    /// limitation.
    pub max_frame_size: usize,
    /// Maximum canonical encoded outbound Text or Binary application message
    /// size in bytes.
    ///
    /// This excludes WebSocket framing, transport buffering, and an optional
    /// distributed backplane envelope.
    pub max_outbound_message_size: usize,
    /// Maximum time allowed for the HTTP Upgrade handshake.
    pub handshake_timeout_secs: u64,
    /// Maximum idle connection age in seconds.
    pub idle_timeout_secs: u64,
    /// Handshake/identity/admit/opened invocation cap and aggregate closed-chain cap.
    pub connection_middleware_timeout_secs: u64,
    /// One deadline for the whole message pipeline, from its first middleware
    /// through guards, extraction, action, response preparation and normal reverse exit.
    pub message_timeout_secs: u64,
    /// Aggregate cap for one message termination cleanup chain (default: 10 seconds).
    /// This is independent of the normal message execution deadline.
    pub message_cleanup_timeout_secs: u64,
    /// Default cap for a connected/disconnected controller invocation.
    pub connection_lifecycle_timeout_secs: u64,
    /// Supported WebSocket subprotocols in server preference order.
    pub supported_protocols: Vec<String>,
    /// Exact allowed browser origins.
    pub allowed_origins: Vec<String>,
    /// Explicit opt-in for accepting every Origin. It is never inferred from
    /// a wildcard string in `allowed_origins`.
    pub allow_any_origin: bool,
    /// Explicit non-browser profile switch. In browser mode a missing Origin
    /// is rejected before the WebSocket upgrade.
    pub allow_missing_origin: bool,
    /// Require a client to negotiate one of `supported_protocols`.
    pub require_subprotocol: bool,
    /// Maximum complete application messages retained behind the one active
    /// action for each connection. Retained messages do not create tasks or
    /// DI scopes.
    pub inbound_queue_capacity: usize,
    /// Maximum aggregate payload bytes retained in the per-connection inbound
    /// queue. This is independent from the per-message `max_message_size`.
    pub inbound_queue_max_bytes: usize,
    /// Bounded outbound queue for each connected peer. This cannot exceed
    /// [`Semaphore::MAX_PERMITS`].
    pub outbound_queue_capacity: usize,
    /// Maximum aggregate canonical application-message bytes admitted to one
    /// connection's outbound data path until each write completes. This is
    /// independent from the message-count capacity; the dequeued in-flight
    /// application frame remains charged, while protocol-control frames and
    /// transport-owned buffers do not belong to this budget.
    pub outbound_queue_max_bytes: usize,
    /// Maximum aggregate wait for one outbound application frame to acquire
    /// both connection-local message-count and byte capacity (default: 5000
    /// milliseconds; accepted range: 100..=300000). Expiry initiates Close
    /// `1013` with reason `lily.v2.slow_consumer`; it is independent from
    /// socket write/flush timing and provides no reconnect replay or delivery
    /// acknowledgement.
    pub outbound_admission_timeout_millis: u64,
    /// Maximum time for one socket send or flush. During graceful server
    /// shutdown, already-admitted terminal frames, the `1001 Going Away`
    /// frame, and the peer Close acknowledgement share this same bounded
    /// budget (and remain capped by the application shutdown deadline).
    pub write_timeout_millis: u64,
    /// Tungstenite write batching threshold.
    pub write_buffer_size_bytes: usize,
    /// Hard Tungstenite write-buffer ceiling. It must fit one maximum outbound
    /// application payload plus its unmasked server-frame header.
    pub max_write_buffer_size_bytes: usize,
    /// Maximum rooms one connection may join.
    pub max_rooms_per_connection: usize,
    /// Maximum UTF-8 byte length of a room name.
    pub max_room_name_length: usize,
    /// Socket networks allowed to supply `X-Forwarded-For`. Empty by default.
    pub trusted_proxy_cidrs: Vec<TrustedProxyNetwork>,
    /// Maximum comma-separated forwarding hops accepted from a trusted proxy.
    pub max_forwarded_hops: usize,
    /// Heartbeat interval.
    pub ping_interval_secs: u64,
    /// Maximum wait for Pong after a Ping.
    pub pong_timeout_secs: u64,
}

impl fmt::Debug for ServerConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerConfig")
            .field("endpoint_path", &self.endpoint_path)
            .field("max_connections", &self.max_connections)
            .field("max_message_size", &self.max_message_size)
            .field("max_frame_size", &self.max_frame_size)
            .field("max_outbound_message_size", &self.max_outbound_message_size)
            .field("handshake_timeout_secs", &self.handshake_timeout_secs)
            .field("idle_timeout_secs", &self.idle_timeout_secs)
            .field(
                "connection_middleware_timeout_secs",
                &self.connection_middleware_timeout_secs,
            )
            .field("message_timeout_secs", &self.message_timeout_secs)
            .field(
                "message_cleanup_timeout_secs",
                &self.message_cleanup_timeout_secs,
            )
            .field(
                "connection_lifecycle_timeout_secs",
                &self.connection_lifecycle_timeout_secs,
            )
            .field("supported_protocols", &self.supported_protocols)
            .field("allowed_origin_count", &self.allowed_origins.len())
            .field("allow_any_origin", &self.allow_any_origin)
            .field("allow_missing_origin", &self.allow_missing_origin)
            .field("require_subprotocol", &self.require_subprotocol)
            .field("inbound_queue_capacity", &self.inbound_queue_capacity)
            .field("inbound_queue_max_bytes", &self.inbound_queue_max_bytes)
            .field("outbound_queue_capacity", &self.outbound_queue_capacity)
            .field("outbound_queue_max_bytes", &self.outbound_queue_max_bytes)
            .field(
                "outbound_admission_timeout_millis",
                &self.outbound_admission_timeout_millis,
            )
            .field("write_timeout_millis", &self.write_timeout_millis)
            .field("write_buffer_size_bytes", &self.write_buffer_size_bytes)
            .field(
                "max_write_buffer_size_bytes",
                &self.max_write_buffer_size_bytes,
            )
            .field("max_rooms_per_connection", &self.max_rooms_per_connection)
            .field("max_room_name_length", &self.max_room_name_length)
            .field("trusted_proxy_count", &self.trusted_proxy_cidrs.len())
            .field("max_forwarded_hops", &self.max_forwarded_hops)
            .field("ping_interval_secs", &self.ping_interval_secs)
            .field("pong_timeout_secs", &self.pong_timeout_secs)
            .finish()
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            endpoint_path: "/ws".to_string(),
            max_connections: 1_000,
            max_message_size: 1024 * 1024,
            max_frame_size: 256 * 1024,
            max_outbound_message_size: DEFAULT_MAX_OUTBOUND_MESSAGE_SIZE_BYTES,
            handshake_timeout_secs: 10,
            idle_timeout_secs: 60,
            connection_middleware_timeout_secs: 10,
            message_timeout_secs: 30,
            message_cleanup_timeout_secs: 10,
            connection_lifecycle_timeout_secs: 30,
            supported_protocols: vec![crate::request::LILY_WEBSOCKET_SUBPROTOCOL.to_string()],
            allowed_origins: Vec::new(),
            allow_any_origin: false,
            allow_missing_origin: false,
            require_subprotocol: false,
            inbound_queue_capacity: 16,
            inbound_queue_max_bytes: 1024 * 1024,
            outbound_queue_capacity: 256,
            outbound_queue_max_bytes: 1024 * 1024,
            outbound_admission_timeout_millis: DEFAULT_OUTBOUND_ADMISSION_TIMEOUT_MILLIS,
            write_timeout_millis: 5_000,
            write_buffer_size_bytes: 128 * 1024,
            max_write_buffer_size_bytes: 2 * 1024 * 1024,
            max_rooms_per_connection: 128,
            max_room_name_length: 128,
            trusted_proxy_cidrs: Vec::new(),
            max_forwarded_hops: 16,
            ping_interval_secs: 30,
            pong_timeout_secs: 10,
        }
    }
}

impl ServerConfig {
    pub(crate) fn from_central(config: &lily_config::WebSocketConfig) -> Result<Self, ServerError> {
        let trusted_proxy_cidrs = config
            .trusted_proxy_cidrs
            .iter()
            .map(|network| {
                network.parse::<TrustedProxyNetwork>().map_err(|_| {
                    ServerError::configuration(
                        "trusted_proxy_cidrs contains an invalid CIDR network".to_owned(),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            endpoint_path: config.endpoint_path.clone(),
            max_connections: config.max_connections,
            max_message_size: config.max_message_size_bytes,
            max_frame_size: config.max_frame_size_bytes,
            max_outbound_message_size: config.max_outbound_message_size_bytes,
            handshake_timeout_secs: config.handshake_timeout_secs,
            idle_timeout_secs: config.idle_timeout_secs,
            connection_middleware_timeout_secs: config.connection_middleware_timeout_secs,
            message_timeout_secs: config.message_timeout_secs,
            message_cleanup_timeout_secs: config.message_cleanup_timeout_secs,
            connection_lifecycle_timeout_secs: config.connection_lifecycle_timeout_secs,
            supported_protocols: config.supported_protocols.clone(),
            allowed_origins: config.allowed_origins.clone(),
            allow_any_origin: config.allow_any_origin,
            allow_missing_origin: config.allow_missing_origin,
            require_subprotocol: config.require_subprotocol,
            inbound_queue_capacity: config.inbound_queue_capacity,
            inbound_queue_max_bytes: config.inbound_queue_max_bytes,
            outbound_queue_capacity: config.outbound_queue_capacity,
            outbound_queue_max_bytes: config.outbound_queue_max_bytes,
            outbound_admission_timeout_millis: config.outbound_admission_timeout_millis,
            write_timeout_millis: config.write_timeout_millis,
            write_buffer_size_bytes: config.write_buffer_size_bytes,
            max_write_buffer_size_bytes: config.max_write_buffer_size_bytes,
            max_rooms_per_connection: config.max_rooms_per_connection,
            max_room_name_length: config.max_room_name_length,
            trusted_proxy_cidrs,
            max_forwarded_hops: config.max_forwarded_hops,
            ping_interval_secs: config.ping_interval_secs,
            pong_timeout_secs: config.pong_timeout_secs,
        })
    }

    pub(crate) fn validate(&self) -> Result<(), ServerError> {
        if !is_canonical_endpoint_path(&self.endpoint_path) {
            return Err(ServerError::configuration(
                "endpoint_path must be an absolute canonical path of at most 256 bytes".into(),
            ));
        }
        for limit in [
            self.max_connections,
            self.max_message_size,
            self.max_frame_size,
            self.max_outbound_message_size,
            self.inbound_queue_capacity,
            self.inbound_queue_max_bytes,
            self.outbound_queue_capacity,
            self.outbound_queue_max_bytes,
            self.max_rooms_per_connection,
            self.max_room_name_length,
        ] {
            if limit == 0 {
                return Err(ServerError::configuration(
                    "connection, message, room and queue limits must be greater than zero".into(),
                ));
            }
        }
        for (name, limit) in [
            ("max_connections", self.max_connections),
            ("outbound_queue_capacity", self.outbound_queue_capacity),
        ] {
            if limit > Semaphore::MAX_PERMITS {
                return Err(ServerError::configuration(format!(
                    "{name} cannot exceed the runtime semaphore limit of {}",
                    Semaphore::MAX_PERMITS
                )));
            }
        }
        if self.max_frame_size > self.max_message_size {
            return Err(ServerError::configuration(
                "max_frame_size cannot exceed max_message_size".into(),
            ));
        }
        if self.outbound_queue_max_bytes < self.max_outbound_message_size {
            return Err(ServerError::configuration(
                "outbound_queue_max_bytes cannot be smaller than max_outbound_message_size".into(),
            ));
        }
        if self.max_room_name_length > MAX_CANONICAL_ROOM_BYTES {
            return Err(ServerError::configuration(format!(
                "max_room_name_length cannot exceed the canonical protocol limit of {MAX_CANONICAL_ROOM_BYTES} bytes"
            )));
        }
        if self.trusted_proxy_cidrs.len() > 64 {
            return Err(ServerError::configuration(
                "trusted_proxy_cidrs cannot contain more than 64 networks".to_owned(),
            ));
        }
        if self.max_forwarded_hops == 0 || self.max_forwarded_hops > 64 {
            return Err(ServerError::configuration(
                "max_forwarded_hops must be in 1..=64".to_owned(),
            ));
        }
        if !(MIN_WRITE_TIMEOUT_MILLIS..=MAX_WRITE_TIMEOUT_MILLIS)
            .contains(&self.write_timeout_millis)
        {
            return Err(ServerError::configuration(format!(
                "write_timeout_millis must be between {MIN_WRITE_TIMEOUT_MILLIS} and {MAX_WRITE_TIMEOUT_MILLIS}"
            )));
        }
        if !(MIN_OUTBOUND_ADMISSION_TIMEOUT_MILLIS..=MAX_OUTBOUND_ADMISSION_TIMEOUT_MILLIS)
            .contains(&self.outbound_admission_timeout_millis)
        {
            return Err(ServerError::configuration(format!(
                "outbound_admission_timeout_millis must be between {MIN_OUTBOUND_ADMISSION_TIMEOUT_MILLIS} and {MAX_OUTBOUND_ADMISSION_TIMEOUT_MILLIS}"
            )));
        }
        if self.write_buffer_size_bytes > MAX_WRITE_BUFFER_THRESHOLD_BYTES {
            return Err(ServerError::configuration(format!(
                "write_buffer_size_bytes must not exceed {MAX_WRITE_BUFFER_THRESHOLD_BYTES}"
            )));
        }
        if self.max_write_buffer_size_bytes > MAX_WRITE_BUFFER_CEILING_BYTES
            || self.max_write_buffer_size_bytes <= self.write_buffer_size_bytes
        {
            return Err(ServerError::configuration(format!(
                "max_write_buffer_size_bytes must be greater than write_buffer_size_bytes and at most {MAX_WRITE_BUFFER_CEILING_BYTES}"
            )));
        }
        let outbound_frame_header = server_frame_header_bytes(self.max_outbound_message_size);
        let minimum_outbound_write_buffer = self
            .max_outbound_message_size
            .checked_add(outbound_frame_header)
            .ok_or_else(|| {
                ServerError::configuration(
                    "max_outbound_message_size exceeds the transport write-buffer range".into(),
                )
            })?;
        if self.max_write_buffer_size_bytes < minimum_outbound_write_buffer {
            return Err(ServerError::configuration(format!(
                "max_write_buffer_size_bytes must be at least max_outbound_message_size plus {outbound_frame_header} bytes of server frame overhead"
            )));
        }
        for timeout in [self.handshake_timeout_secs, self.idle_timeout_secs] {
            if timeout == 0 {
                return Err(ServerError::configuration(
                    "handshake and idle timeouts must be greater than zero".into(),
                ));
            }
        }
        for (name, timeout) in [
            ("message_timeout_secs", self.message_timeout_secs),
            (
                "message_cleanup_timeout_secs",
                self.message_cleanup_timeout_secs,
            ),
            (
                "connection_middleware_timeout_secs",
                self.connection_middleware_timeout_secs,
            ),
            (
                "connection_lifecycle_timeout_secs",
                self.connection_lifecycle_timeout_secs,
            ),
        ] {
            if !(1..=MAX_EXECUTION_TIMEOUT_SECS).contains(&timeout) {
                return Err(ServerError::configuration(format!(
                    "{name} must be between 1 and {MAX_EXECUTION_TIMEOUT_SECS} seconds"
                )));
            }
        }
        for heartbeat in [self.ping_interval_secs, self.pong_timeout_secs] {
            if heartbeat == 0 {
                return Err(ServerError::configuration(
                    "heartbeat intervals must be greater than zero".into(),
                ));
            }
        }
        if self.require_subprotocol && self.supported_protocols.is_empty() {
            return Err(ServerError::configuration(
                "require_subprotocol needs at least one supported protocol".into(),
            ));
        }
        if self.allowed_origins.iter().any(|origin| origin == "*") {
            return Err(ServerError::configuration(
                "use allow_any_origin for the explicit wildcard policy".into(),
            ));
        }
        if self
            .allowed_origins
            .iter()
            .any(|origin| !is_canonical_browser_origin(origin))
        {
            return Err(ServerError::configuration(
                "allowed_origins must contain exact http(s) origins without credentials, path, query or fragment"
                    .into(),
            ));
        }
        for protocol in &self.supported_protocols {
            if protocol.is_empty() {
                return Err(ServerError::configuration(
                    "supported_protocols contains an invalid HTTP token".into(),
                ));
            }
            if !protocol.bytes().all(is_http_token_byte) {
                return Err(ServerError::configuration(
                    "supported_protocols contains an invalid HTTP token".into(),
                ));
            }
        }
        if contains_duplicates(&self.supported_protocols) {
            return Err(ServerError::configuration(
                "supported_protocols must not contain duplicates".into(),
            ));
        }
        if contains_duplicates(&self.allowed_origins) {
            return Err(ServerError::configuration(
                "allowed_origins must not contain duplicates".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn websocket_transport_config(
        &self,
    ) -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
        tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
            write_buffer_size: self.write_buffer_size_bytes,
            max_write_buffer_size: self.max_write_buffer_size_bytes,
            max_message_size: Some(self.max_message_size),
            max_frame_size: Some(self.max_frame_size),
            ..Default::default()
        }
    }

    pub(crate) fn evaluate_handshake(
        &self,
        headers: &WsHeaders,
    ) -> Result<HandshakeDecision, HandshakeRejection> {
        match headers.origin.as_deref() {
            Some(origin)
                if !self.allow_any_origin
                    && !self.allowed_origins.iter().any(|allowed| allowed == origin) =>
            {
                return Err(HandshakeRejection::OriginDenied);
            }
            None if !self.allow_missing_origin => {
                return Err(HandshakeRejection::OriginRequired);
            }
            _ => {}
        }

        let requested = headers
            .get_protocols()
            .map(Vec::as_slice)
            .unwrap_or_default();
        for protocol in requested {
            if protocol.is_empty() {
                return Err(HandshakeRejection::InvalidSubprotocol);
            }
            if !protocol.bytes().all(is_http_token_byte) {
                return Err(HandshakeRejection::InvalidSubprotocol);
            }
        }

        // Server preference is authoritative and therefore independent of the
        // order in an untrusted client header.
        let subprotocol = self
            .supported_protocols
            .iter()
            .find(|supported| requested.iter().any(|requested| requested == *supported))
            .cloned();
        if self.require_subprotocol && subprotocol.is_none() {
            return Err(HandshakeRejection::SubprotocolRequired);
        }

        Ok(HandshakeDecision { subprotocol })
    }
}

fn contains_duplicates(values: &[String]) -> bool {
    let mut unique = HashSet::with_capacity(values.len());
    values.iter().any(|value| !unique.insert(value))
}

fn is_canonical_endpoint_path(path: &str) -> bool {
    if path.is_empty() || path.len() > MAX_ENDPOINT_PATH_BYTES || !path.starts_with('/') {
        return false;
    }
    if path == "/" {
        return true;
    }
    if path.ends_with('/') {
        return false;
    }
    path[1..].split('/').all(|segment| {
        !segment.is_empty()
            && segment.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~')
            })
    })
}

fn is_canonical_browser_origin(origin: &str) -> bool {
    let Ok(parsed) = url::Url::parse(origin) else {
        return false;
    };
    if !matches!(parsed.scheme(), "http" | "https") {
        return false;
    }
    if parsed.host_str().is_none() {
        return false;
    }
    if !parsed.username().is_empty() {
        return false;
    }
    if parsed.password().is_some() {
        return false;
    }
    // URL parsing removes dot segments before exposing `path()`. Inspect the
    // configured spelling as well so `/.`, `/path/..`, and backslash path
    // variants cannot normalize into the authority-only `/` representation.
    let Some((_, authority_and_suffix)) = origin.split_once("://") else {
        return false;
    };
    if authority_and_suffix
        .bytes()
        .any(|byte| matches!(byte, b'/' | b'\\'))
    {
        return false;
    }
    if parsed.path() != "/" {
        return false;
    }
    if parsed.query().is_some() {
        return false;
    }
    parsed.fragment().is_none()
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

/// Invalid config-backed WebSocket listener or runtime configuration.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WsEffectiveConfigError {
    /// Config-backed listener host is empty.
    #[error("websocket.host must not be empty")]
    EmptyHost,
    /// Config-backed listener host is not a valid IP address or DNS name.
    #[error("websocket.host is not a valid listener host")]
    InvalidHost,
    /// Config-backed listener port is zero.
    #[error("websocket.port must be between 1 and 65535")]
    ZeroPort,
    /// Explicit listener address is not valid `host:port` syntax.
    #[error("explicit WebSocket listener address must be a valid host:port")]
    InvalidListenAddress,
    /// Explicit listener address omitted its port.
    #[error("explicit WebSocket listener address must include a port")]
    MissingListenPort,
    /// Resolved server policy failed runtime validation.
    #[error("effective WebSocket runtime configuration is invalid: {0}")]
    InvalidRuntime(WsRuntimeConfigValidationError),
}

/// Framework-owned detail for an invalid resolved runtime policy.
///
/// This wrapper has no public string constructor. Applications match the
/// typed [`WsEffectiveConfigError::InvalidRuntime`] category instead of
/// parsing or manufacturing diagnostic text.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{detail}")]
pub struct WsRuntimeConfigValidationError {
    detail: Box<str>,
}

impl WsRuntimeConfigValidationError {
    pub(crate) fn new(detail: String) -> Self {
        Self {
            detail: detail.into_boxed_str(),
        }
    }
}

/// Runtime/composition failure retained behind one typed public category.
///
/// The diagnostic is framework-owned and intentionally has no public string
/// constructor; applications should match [`ServerError::ConnectionError`]
/// instead of parsing its display text.
#[derive(Debug, thiserror::Error)]
#[error("{detail}")]
pub struct WsServerRuntimeError {
    detail: Box<str>,
}

impl WsServerRuntimeError {
    fn new(detail: String) -> Self {
        Self {
            detail: detail.into_boxed_str(),
        }
    }
}

/// Controller/handler failure retained behind one typed public category.
#[derive(Debug, thiserror::Error)]
#[error("{detail}")]
pub struct WsServerHandlerError {
    detail: Box<str>,
}

impl WsServerHandlerError {
    fn new(detail: String) -> Self {
        Self {
            detail: detail.into_boxed_str(),
        }
    }
}

/// Invalid server composition retained behind one typed public category.
#[derive(Debug, thiserror::Error)]
#[error("{detail}")]
pub struct WsServerConfigurationError {
    detail: Box<str>,
}

impl WsServerConfigurationError {
    fn new(detail: String) -> Self {
        Self {
            detail: detail.into_boxed_str(),
        }
    }
}

/// Canonical WebSocket server errors.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// A registered background worker could not be constructed.
    #[error("WebSocket background service initialization failed: {0}")]
    BackgroundService(#[from] lily_background_service::BackgroundServiceError),
    /// Listener or transport I/O failed.
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    /// Tungstenite protocol or transport operation failed.
    #[error("WebSocket error: {0}")]
    WebSocketError(#[source] Box<tokio_tungstenite::tungstenite::Error>),
    /// Connection manager operation failed.
    #[error("Connection error: {0}")]
    ConnectionError(#[source] WsServerRuntimeError),
    /// Controller lifecycle or action handler failed.
    #[error("Handler error: {0}")]
    HandlerError(#[source] WsServerHandlerError),
    /// Immutable application or server configuration is invalid.
    #[error("WebSocket server configuration error: {0}")]
    Configuration(#[source] WsServerConfigurationError),
    /// Central configuration explicitly disabled the listener.
    #[error("WebSocket server is disabled by central configuration")]
    DisabledByConfiguration,
    /// Listener address or resolved runtime configuration is invalid.
    #[error("WebSocket effective server configuration error: {0}")]
    EffectiveConfiguration(#[from] WsEffectiveConfigError),
    /// Static controller metadata could not be materialized safely for this app.
    #[error("WebSocket controller materialization failed: {0}")]
    ControllerMaterialization(#[from] crate::controller::WebSocketControllerMaterializationError),
    /// HTTP Upgrade did not finish within its deadline.
    #[error("WebSocket handshake timed out after {0:?}")]
    HandshakeTimeout(Duration),
    /// Rustls rejected a client handshake. Only the stable I/O category is
    /// retained so attacker-controlled negotiation details are not promoted
    /// into application errors or logs.
    #[error("WebSocket TLS handshake failed ({kind:?})")]
    TlsHandshakeFailed {
        /// Stable I/O category without attacker-controlled TLS detail.
        kind: std::io::ErrorKind,
    },
}

impl ServerError {
    pub(crate) fn connection_error(detail: String) -> Self {
        Self::ConnectionError(WsServerRuntimeError::new(detail))
    }

    pub(crate) fn handler_error(detail: String) -> Self {
        Self::HandlerError(WsServerHandlerError::new(detail))
    }

    pub(crate) fn configuration(detail: String) -> Self {
        Self::Configuration(WsServerConfigurationError::new(detail))
    }
}

impl From<tokio_tungstenite::tungstenite::Error> for ServerError {
    fn from(error: tokio_tungstenite::tungstenite::Error) -> Self {
        Self::WebSocketError(Box::new(error))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn browser_headers(origin: Option<&str>, protocols: &[&str]) -> WsHeaders {
        let mut headers = WsHeaders::default();
        headers.origin = origin.map(str::to_owned);
        headers.sec_websocket_protocol = (!protocols.is_empty())
            .then(|| protocols.iter().map(|value| value.to_string()).collect());
        headers
    }

    #[test]
    fn browser_origin_is_fail_closed_and_non_browser_is_explicit() {
        let config = ServerConfig::default();
        assert_eq!(
            config
                .evaluate_handshake(&browser_headers(None, &[]))
                .unwrap_err(),
            HandshakeRejection::OriginRequired
        );
        assert_eq!(
            config
                .evaluate_handshake(&browser_headers(Some("https://evil.example"), &[]))
                .unwrap_err(),
            HandshakeRejection::OriginDenied
        );

        let non_browser = ServerConfig {
            allow_missing_origin: true,
            ..ServerConfig::default()
        };
        assert!(
            non_browser
                .evaluate_handshake(&browser_headers(None, &[]))
                .is_ok()
        );
    }

    #[test]
    fn handshake_rejection_status_and_public_message_are_exact() {
        use tokio_tungstenite::tungstenite::http::StatusCode;

        for (rejection, status, message) in [
            (
                HandshakeRejection::OriginRequired,
                StatusCode::FORBIDDEN,
                "WebSocket Origin header is required",
            ),
            (
                HandshakeRejection::OriginDenied,
                StatusCode::FORBIDDEN,
                "WebSocket origin is not allowed",
            ),
            (
                HandshakeRejection::SubprotocolRequired,
                StatusCode::BAD_REQUEST,
                "A supported WebSocket subprotocol is required",
            ),
            (
                HandshakeRejection::InvalidSubprotocol,
                StatusCode::BAD_REQUEST,
                "WebSocket subprotocol header is invalid",
            ),
            (
                HandshakeRejection::InvalidNamespace,
                StatusCode::BAD_REQUEST,
                "WebSocket namespace is invalid",
            ),
            (
                HandshakeRejection::NamespaceNotFound,
                StatusCode::NOT_FOUND,
                "WebSocket namespace is not registered",
            ),
        ] {
            assert_eq!(rejection.status(), status);
            assert_eq!(rejection.response_message(), message);
        }
    }

    #[test]
    fn subprotocol_selection_uses_server_preference() {
        let config = ServerConfig {
            allowed_origins: vec!["https://app.example".into()],
            supported_protocols: vec![
                crate::request::LILY_WEBSOCKET_SUBPROTOCOL.into(),
                "legacy.chat".into(),
            ],
            require_subprotocol: true,
            ..ServerConfig::default()
        };
        config.validate().unwrap();

        let decision = config
            .evaluate_handshake(&browser_headers(
                Some("https://app.example"),
                &["legacy.chat", crate::request::LILY_WEBSOCKET_SUBPROTOCOL],
            ))
            .unwrap();
        assert_eq!(
            decision.subprotocol.as_deref(),
            Some(crate::request::LILY_WEBSOCKET_SUBPROTOCOL)
        );

        let legacy_only = config
            .evaluate_handshake(&browser_headers(
                Some("https://app.example"),
                &["legacy.chat"],
            ))
            .unwrap();
        assert_eq!(legacy_only.subprotocol.as_deref(), Some("legacy.chat"));
    }

    #[test]
    fn server_config_validates_each_limit_and_timeout_independently() {
        assert_eq!(
            ServerConfig::default().outbound_admission_timeout_millis,
            DEFAULT_OUTBOUND_ADMISSION_TIMEOUT_MILLIS
        );
        for config in [
            ServerConfig {
                max_connections: 0,
                ..ServerConfig::default()
            },
            ServerConfig {
                max_message_size: 0,
                ..ServerConfig::default()
            },
            ServerConfig {
                max_frame_size: 0,
                ..ServerConfig::default()
            },
            ServerConfig {
                max_outbound_message_size: 0,
                ..ServerConfig::default()
            },
            ServerConfig {
                inbound_queue_capacity: 0,
                ..ServerConfig::default()
            },
            ServerConfig {
                inbound_queue_max_bytes: 0,
                ..ServerConfig::default()
            },
            ServerConfig {
                outbound_queue_capacity: 0,
                ..ServerConfig::default()
            },
            ServerConfig {
                outbound_queue_max_bytes: 0,
                ..ServerConfig::default()
            },
            ServerConfig {
                max_outbound_message_size: 1025,
                outbound_queue_max_bytes: 1024,
                ..ServerConfig::default()
            },
            ServerConfig {
                max_rooms_per_connection: 0,
                ..ServerConfig::default()
            },
            ServerConfig {
                max_room_name_length: 0,
                ..ServerConfig::default()
            },
            ServerConfig {
                max_room_name_length: MAX_CANONICAL_ROOM_BYTES + 1,
                ..ServerConfig::default()
            },
            ServerConfig {
                handshake_timeout_secs: 0,
                ..ServerConfig::default()
            },
            ServerConfig {
                idle_timeout_secs: 0,
                ..ServerConfig::default()
            },
            ServerConfig {
                connection_middleware_timeout_secs: 0,
                ..ServerConfig::default()
            },
            ServerConfig {
                message_timeout_secs: 0,
                ..ServerConfig::default()
            },
            ServerConfig {
                message_timeout_secs: MAX_EXECUTION_TIMEOUT_SECS + 1,
                ..ServerConfig::default()
            },
            ServerConfig {
                message_cleanup_timeout_secs: 0,
                ..ServerConfig::default()
            },
            ServerConfig {
                message_cleanup_timeout_secs: MAX_EXECUTION_TIMEOUT_SECS + 1,
                ..ServerConfig::default()
            },
            ServerConfig {
                connection_lifecycle_timeout_secs: MAX_EXECUTION_TIMEOUT_SECS + 1,
                ..ServerConfig::default()
            },
            ServerConfig {
                ping_interval_secs: 0,
                ..ServerConfig::default()
            },
            ServerConfig {
                pong_timeout_secs: 0,
                ..ServerConfig::default()
            },
            ServerConfig {
                max_forwarded_hops: 0,
                ..ServerConfig::default()
            },
            ServerConfig {
                max_forwarded_hops: 65,
                ..ServerConfig::default()
            },
            ServerConfig {
                trusted_proxy_cidrs: (0..65)
                    .map(|index| format!("10.{index}.0.0/16").parse().unwrap())
                    .collect(),
                ..ServerConfig::default()
            },
            ServerConfig {
                write_timeout_millis: MIN_WRITE_TIMEOUT_MILLIS - 1,
                ..ServerConfig::default()
            },
            ServerConfig {
                write_timeout_millis: MAX_WRITE_TIMEOUT_MILLIS + 1,
                ..ServerConfig::default()
            },
            ServerConfig {
                outbound_admission_timeout_millis: MIN_OUTBOUND_ADMISSION_TIMEOUT_MILLIS - 1,
                ..ServerConfig::default()
            },
            ServerConfig {
                outbound_admission_timeout_millis: MAX_OUTBOUND_ADMISSION_TIMEOUT_MILLIS + 1,
                ..ServerConfig::default()
            },
            ServerConfig {
                write_buffer_size_bytes: MAX_WRITE_BUFFER_THRESHOLD_BYTES + 1,
                max_write_buffer_size_bytes: MAX_WRITE_BUFFER_CEILING_BYTES,
                ..ServerConfig::default()
            },
            ServerConfig {
                write_buffer_size_bytes: 1024,
                max_write_buffer_size_bytes: 1024,
                ..ServerConfig::default()
            },
            ServerConfig {
                max_write_buffer_size_bytes: MAX_WRITE_BUFFER_CEILING_BYTES + 1,
                ..ServerConfig::default()
            },
        ] {
            assert!(matches!(
                config.validate(),
                Err(ServerError::Configuration(_))
            ));
        }

        let exact_frame = ServerConfig {
            max_message_size: 1024,
            max_frame_size: 1024,
            ..ServerConfig::default()
        };
        assert!(exact_frame.validate().is_ok());
        ServerConfig {
            max_outbound_message_size: 1024,
            outbound_queue_max_bytes: 1024,
            ..ServerConfig::default()
        }
        .validate()
        .expect("one exact maximum-size outbound message must fit the byte budget");
        for (outbound_admission_timeout_millis, write_timeout_millis) in [
            (
                MIN_OUTBOUND_ADMISSION_TIMEOUT_MILLIS,
                MAX_WRITE_TIMEOUT_MILLIS,
            ),
            (
                MAX_OUTBOUND_ADMISSION_TIMEOUT_MILLIS,
                MIN_WRITE_TIMEOUT_MILLIS,
            ),
        ] {
            ServerConfig {
                outbound_admission_timeout_millis,
                write_timeout_millis,
                ..ServerConfig::default()
            }
            .validate()
            .expect("admission and write deadlines must have independent inclusive bounds");
        }
        ServerConfig {
            max_message_size: 64,
            max_frame_size: 64,
            inbound_queue_capacity: 1,
            inbound_queue_max_bytes: 1,
            ..ServerConfig::default()
        }
        .validate()
        .expect("queue byte budget is independent from the per-message limit");
        assert!(
            ServerConfig {
                max_room_name_length: MAX_CANONICAL_ROOM_BYTES,
                ..ServerConfig::default()
            }
            .validate()
            .is_ok()
        );
        assert!(matches!(
            ServerConfig {
                max_frame_size: 1025,
                ..exact_frame
            }
            .validate(),
            Err(ServerError::Configuration(_))
        ));
    }

    #[test]
    fn central_trusted_proxy_cidrs_are_parsed_without_echoing_invalid_input() {
        const SECRET_INVALID_CIDR: &str = "INVALID_PROXY_SECRET_5A91";
        let central = lily_config::WebSocketConfig {
            trusted_proxy_cidrs: vec!["10.0.0.0/8".to_owned()],
            max_outbound_message_size_bytes: 384 * 1024,
            outbound_queue_max_bytes: 768 * 1024,
            outbound_admission_timeout_millis: 7_500,
            ..lily_config::WebSocketConfig::default()
        };

        let resolved = ServerConfig::from_central(&central).unwrap();
        assert_eq!(resolved.trusted_proxy_cidrs.len(), 1);
        assert_eq!(resolved.max_outbound_message_size, 384 * 1024);
        assert_eq!(resolved.outbound_queue_max_bytes, 768 * 1024);
        assert_eq!(resolved.outbound_admission_timeout_millis, 7_500);
        let trusted_address = "10.2.3.4".parse::<std::net::IpAddr>().unwrap();
        assert!(resolved.trusted_proxy_cidrs[0].contains(&trusted_address));

        let invalid = lily_config::WebSocketConfig {
            trusted_proxy_cidrs: vec![SECRET_INVALID_CIDR.to_owned()],
            ..lily_config::WebSocketConfig::default()
        };
        let error = ServerConfig::from_central(&invalid).unwrap_err();
        assert!(matches!(error, ServerError::Configuration(_)));
        assert!(!error.to_string().contains(SECRET_INVALID_CIDR));
    }

    #[test]
    fn inbound_and_outbound_message_size_authorities_are_independent() {
        for (max_message_size, max_outbound_message_size) in [(64, 1024), (1024, 64)] {
            ServerConfig {
                max_message_size,
                max_frame_size: 64,
                max_outbound_message_size,
                outbound_queue_max_bytes: max_outbound_message_size,
                ..ServerConfig::default()
            }
            .validate()
            .expect("inbound and outbound message authorities must remain independent");
        }
    }

    #[test]
    fn outbound_message_limit_must_fit_the_transport_frame_buffer() {
        const OUTBOUND_LIMIT: usize = 6144;
        assert_eq!(server_frame_header_bytes(125), 2);
        assert_eq!(server_frame_header_bytes(126), 4);
        assert_eq!(server_frame_header_bytes(u16::MAX as usize), 4);
        assert_eq!(server_frame_header_bytes(u16::MAX as usize + 1), 10);

        let exact = ServerConfig {
            max_outbound_message_size: OUTBOUND_LIMIT,
            outbound_queue_max_bytes: OUTBOUND_LIMIT,
            write_buffer_size_bytes: 512,
            max_write_buffer_size_bytes: OUTBOUND_LIMIT + server_frame_header_bytes(OUTBOUND_LIMIT),
            ..ServerConfig::default()
        };
        exact
            .validate()
            .expect("one exact maximum outbound frame must fit an empty write buffer");

        assert!(matches!(
            ServerConfig {
                max_write_buffer_size_bytes: OUTBOUND_LIMIT
                    + server_frame_header_bytes(OUTBOUND_LIMIT)
                    - 1,
                ..exact.clone()
            }
            .validate(),
            Err(ServerError::Configuration(_))
        ));
        assert!(matches!(
            ServerConfig {
                max_outbound_message_size: usize::MAX,
                outbound_queue_max_bytes: usize::MAX,
                max_write_buffer_size_bytes: MAX_WRITE_BUFFER_CEILING_BYTES,
                ..ServerConfig::default()
            }
            .validate(),
            Err(ServerError::Configuration(_))
        ));
    }

    #[test]
    fn transport_security_is_server_owned_and_independent_from_origin() {
        let peer_addr = "127.0.0.1:43123".parse().unwrap();
        let plaintext = WsHandshakeContext::new(
            "chat".into(),
            browser_headers(Some("https://secure-origin.example"), &[]),
            None,
            None,
            peer_addr,
            RequestConnectionInfo::direct(peer_addr.ip()),
            WsTransportSecurity::Plaintext,
        );
        assert!(!plaintext.is_secure());

        let tls = WsHandshakeContext::new(
            "chat".into(),
            browser_headers(None, &[]),
            None,
            None,
            peer_addr,
            RequestConnectionInfo::direct(peer_addr.ip()),
            WsTransportSecurity::Tls,
        );
        assert!(tls.is_secure());
        assert_eq!(tls.transport_security(), WsTransportSecurity::Tls);
    }

    #[test]
    fn websocket_transport_config_uses_the_validated_bounded_write_policy() {
        let config = ServerConfig {
            max_message_size: 4096,
            max_frame_size: 2048,
            max_outbound_message_size: 6144,
            outbound_queue_max_bytes: 8192,
            write_buffer_size_bytes: 512,
            max_write_buffer_size_bytes: 8192,
            ..ServerConfig::default()
        };
        config.validate().unwrap();

        let transport = config.websocket_transport_config();
        assert_eq!(transport.write_buffer_size, 512);
        assert_eq!(transport.max_write_buffer_size, 8192);
        assert_eq!(transport.max_message_size, Some(4096));
        assert_eq!(transport.max_frame_size, Some(2048));
    }

    #[test]
    fn protocol_tokens_fail_closed_independently_from_origin_policy() {
        for protocol in ["", "bad protocol"] {
            let config = ServerConfig {
                supported_protocols: vec![protocol.to_string()],
                ..ServerConfig::default()
            };
            assert!(matches!(
                config.validate(),
                Err(ServerError::Configuration(_))
            ));

            let mut headers = browser_headers(Some("https://app.example"), &[]);
            headers.sec_websocket_protocol = Some(vec![protocol.to_string()]);
            let runtime = ServerConfig {
                allowed_origins: vec!["https://app.example".to_string()],
                ..ServerConfig::default()
            };
            assert_eq!(
                runtime.evaluate_handshake(&headers).unwrap_err(),
                HandshakeRejection::InvalidSubprotocol
            );
        }
    }

    #[test]
    fn server_config_rejects_runtime_semaphore_capacity_overflow() {
        for (name, config) in [
            (
                "max_connections",
                ServerConfig {
                    max_connections: Semaphore::MAX_PERMITS + 1,
                    ..ServerConfig::default()
                },
            ),
            (
                "outbound_queue_capacity",
                ServerConfig {
                    outbound_queue_capacity: Semaphore::MAX_PERMITS + 1,
                    ..ServerConfig::default()
                },
            ),
        ] {
            let error = config.validate().expect_err("capacity must fail closed");
            let message = error.to_string();
            assert!(message.contains(name), "unexpected error: {message}");
            assert!(
                message.contains(&Semaphore::MAX_PERMITS.to_string()),
                "unexpected error: {message}"
            );
        }

        ServerConfig {
            max_connections: Semaphore::MAX_PERMITS,
            outbound_queue_capacity: Semaphore::MAX_PERMITS,
            ..ServerConfig::default()
        }
        .validate()
        .expect("the exact runtime semaphore limit remains valid");
    }

    #[test]
    fn origin_and_http_token_validators_cover_exact_boundaries() {
        assert!(is_canonical_browser_origin("https://example.test"));
        for origin in [
            "ftp://example.test",
            "https://user@example.test",
            "https://:password@example.test",
            "https://example.test/path",
            "https://example.test?query",
            "https://example.test#fragment",
        ] {
            assert!(!is_canonical_browser_origin(origin), "origin: {origin}");
        }

        for byte in b"AZaz09!#$%&'*+-.^_`|~" {
            assert!(is_http_token_byte(*byte), "byte: {byte}");
        }
        for byte in [b' ', b'/', b',', 0, 127] {
            assert!(!is_http_token_byte(byte), "byte: {byte}");
        }
    }

    #[test]
    fn hsk_08_origin_validation_rejects_paths_before_url_normalization() {
        for origin in [
            "http://allowed.example",
            "https://allowed.example",
            "https://allowed.example:8443",
        ] {
            assert!(is_canonical_browser_origin(origin), "origin: {origin}");
            ServerConfig {
                allowed_origins: vec![origin.to_owned()],
                ..ServerConfig::default()
            }
            .validate()
            .expect("authority-only http(s) Origin remains valid");
        }

        for origin in [
            "https://allowed.example/",
            "https://allowed.example/.",
            "https://allowed.example/path/..",
            r"https://allowed.example\path\..",
        ] {
            assert_eq!(
                url::Url::parse(origin).expect("test Origin parses").path(),
                "/",
                "the reproducer depends on URL path normalization"
            );
            assert!(!is_canonical_browser_origin(origin), "origin: {origin}");
            assert!(matches!(
                ServerConfig {
                    allowed_origins: vec![origin.to_owned()],
                    ..ServerConfig::default()
                }
                .validate(),
                Err(ServerError::Configuration(_))
            ));
        }
    }

    #[test]
    fn handshake_context_preserves_the_common_principal_and_redacts_it() {
        let mut headers = browser_headers(None, &[]);
        headers
            .set_custom_header("authorization".into(), "Bearer redacted".into())
            .unwrap();
        let mut claims = serde_json::Map::new();
        claims.insert("tenant".into(), serde_json::json!("tenant-secret"));
        let principal = Principal::new(
            "account-42",
            ["operator".to_owned(), "reader".to_owned()],
            ["chat:read".to_owned(), "chat:write".to_owned()],
            claims,
        );
        let context = WsHandshakeContext::new(
            "chat".into(),
            headers,
            Some(principal.clone()),
            Some(crate::request::LILY_WEBSOCKET_SUBPROTOCOL.into()),
            "127.0.0.1:43123".parse().unwrap(),
            RequestConnectionInfo::direct("127.0.0.1".parse().unwrap()),
            WsTransportSecurity::Plaintext,
        );

        let accepted = context.principal().expect("principal must be retained");
        assert_eq!(accepted, &principal);
        assert_eq!(accepted.subject(), "account-42");
        assert!(accepted.has_role("operator"));
        assert!(accepted.has_scope("chat:write"));
        assert_eq!(
            accepted.claims().get("tenant"),
            Some(&serde_json::json!("tenant-secret"))
        );

        let debug = format!("{context:?}");
        assert!(!debug.contains("account-42"));
        assert!(!debug.contains("chat:write"));
        assert!(!debug.contains("tenant-secret"));
        assert!(!debug.contains("Bearer redacted"));
    }

    #[test]
    fn handshake_context_keeps_only_a_valid_redacted_trace_parent() {
        let mut headers = browser_headers(None, &[]);
        headers
            .set_custom_header(
                "traceparent".into(),
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".into(),
            )
            .unwrap();
        let context = WsHandshakeContext::new(
            "chat".into(),
            headers,
            None,
            None,
            "127.0.0.1:43123".parse().unwrap(),
            RequestConnectionInfo::direct("127.0.0.1".parse().unwrap()),
            WsTransportSecurity::Plaintext,
        );
        assert_eq!(
            context
                .trace_context()
                .map(lily_trace::core::W3CTraceContext::trace_id),
            Some("4bf92f3577b34da6a3ce929d0e0e4736")
        );
        let debug = format!("{context:?}");
        assert!(!debug.contains("4bf92f3577b34da6a3ce929d0e0e4736"));
        assert!(!debug.contains("127.0.0.1:43123"));

        let mut invalid_headers = browser_headers(None, &[]);
        invalid_headers
            .set_custom_header("traceparent".into(), "invalid".into())
            .unwrap();
        let invalid = WsHandshakeContext::new(
            "chat".into(),
            invalid_headers,
            None,
            None,
            "127.0.0.1:43123".parse().unwrap(),
            RequestConnectionInfo::direct("127.0.0.1".parse().unwrap()),
            WsTransportSecurity::Plaintext,
        );
        assert!(invalid.trace_context().is_none());
    }

    #[test]
    fn endpoint_path_is_single_exact_and_canonical() {
        for valid in ["/", "/ws", "/socket/v1", "/socket-v1"] {
            let config = ServerConfig {
                endpoint_path: valid.into(),
                ..ServerConfig::default()
            };
            config.validate().unwrap();
        }
        for invalid in [
            "",
            "ws",
            "/ws/",
            "//ws",
            "/ws?token=x",
            "/ws#fragment",
            "/w s",
        ] {
            let config = ServerConfig {
                endpoint_path: invalid.into(),
                ..ServerConfig::default()
            };
            assert!(matches!(
                config.validate(),
                Err(ServerError::Configuration(_))
            ));
        }
    }

    #[test]
    fn unsafe_implicit_origin_disable_and_ambiguous_config_are_rejected() {
        let zero_middleware_timeout = ServerConfig {
            connection_middleware_timeout_secs: 0,
            ..ServerConfig::default()
        };
        assert!(matches!(
            zero_middleware_timeout.validate(),
            Err(ServerError::Configuration(_))
        ));

        let wildcard = ServerConfig {
            allowed_origins: vec!["*".into()],
            ..ServerConfig::default()
        };
        assert!(matches!(
            wildcard.validate(),
            Err(ServerError::Configuration(_))
        ));

        let duplicate_protocol = ServerConfig {
            supported_protocols: vec!["lily.v1".into(), "lily.v1".into()],
            ..ServerConfig::default()
        };
        assert!(matches!(
            duplicate_protocol.validate(),
            Err(ServerError::Configuration(_))
        ));
    }

    #[test]
    fn namespace_query_is_single_bounded_and_canonical() {
        assert_eq!(
            parse_handshake_namespace(None).unwrap_err(),
            HandshakeRejection::InvalidNamespace
        );
        assert_eq!(
            parse_handshake_namespace(Some("namespace=chat-room")).unwrap(),
            "chat-room"
        );
        for query in [
            "namespace=",
            "namespace=../../admin",
            "namespace=chat&namespace=admin",
            "namespace=chat%2Fadmin",
        ] {
            assert_eq!(
                parse_handshake_namespace(Some(query)).unwrap_err(),
                HandshakeRejection::InvalidNamespace
            );
        }
    }
}
