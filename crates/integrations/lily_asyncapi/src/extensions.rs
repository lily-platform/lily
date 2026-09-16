use std::collections::BTreeMap;

use serde::Serialize;
use serde_json::Value;

use crate::{
    AsyncApiBuildError,
    contribution::TransportKind,
    limits,
    validation::{
        TextKind, canonical_json_bytes, normalize_mime, validate_count,
        validate_optional_description, validate_required, validate_required_text,
    },
};

const RUNTIME_IDENTITY_KEY: &str = "x-lily-runtime-identity";
const WEBSOCKET_PROTOCOL_KEY: &str = "x-lily-websocket-protocol";
const WEBSOCKET_OUTCOME_KEY: &str = "x-lily-websocket-outcome";
const RABBITMQ_TOPOLOGY_KEY: &str = "x-lily-rabbitmq-topology";
const SETTLEMENT_KEY: &str = "x-lily-settlement";
const DELIVERY_GUARANTEE_KEY: &str = "x-lily-delivery-guarantee";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExtensionScope {
    Channel,
    Message,
    Operation,
}

impl ExtensionScope {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Channel => "channel",
            Self::Message => "message",
            Self::Operation => "operation",
        }
    }
}

/// One framework-owned, typed Lily extension accepted by the private transport ABI.
///
/// The enum deliberately has no arbitrary JSON or arbitrary `x-*` variant.
#[doc(hidden)]
#[derive(Debug)]
pub enum LilyExtension {
    /// Lily v2 WebSocket envelope and nested content semantics.
    WebSocketProtocol(WebSocketProtocolExtension),
    /// Custom WebSocket codec protocol contract without Lily v2 assumptions.
    CustomWebSocketProtocol(CustomWebSocketProtocolExtension),
    /// Static WebSocket action outcome semantics.
    WebSocketOutcome(WebSocketOutcomeExtension),
    /// The accepted RabbitMQ topology projection.
    RabbitMqTopology(RabbitMqTopologyExtension),
    /// RabbitMQ terminal settlement behavior.
    Settlement(SettlementExtension),
    /// Per-handler delivery guarantee boundary.
    DeliveryGuarantee(DeliveryGuaranteeExtension),
}

impl LilyExtension {
    fn key(&self) -> &'static str {
        match self {
            Self::WebSocketProtocol(_) | Self::CustomWebSocketProtocol(_) => WEBSOCKET_PROTOCOL_KEY,
            Self::WebSocketOutcome(_) => WEBSOCKET_OUTCOME_KEY,
            Self::RabbitMqTopology(_) => RABBITMQ_TOPOLOGY_KEY,
            Self::Settlement(_) => SETTLEMENT_KEY,
            Self::DeliveryGuarantee(_) => DELIVERY_GUARANTEE_KEY,
        }
    }

    fn expected_scope(&self) -> ExtensionScope {
        match self {
            Self::WebSocketProtocol(_) | Self::CustomWebSocketProtocol(_) => {
                ExtensionScope::Message
            }
            Self::WebSocketOutcome(_) | Self::Settlement(_) | Self::DeliveryGuarantee(_) => {
                ExtensionScope::Operation
            }
            Self::RabbitMqTopology(_) => ExtensionScope::Channel,
        }
    }

    fn expected_transport(&self) -> TransportKind {
        match self {
            Self::WebSocketProtocol(_)
            | Self::CustomWebSocketProtocol(_)
            | Self::WebSocketOutcome(_) => TransportKind::WebSocket,
            Self::RabbitMqTopology(_) | Self::Settlement(_) | Self::DeliveryGuarantee(_) => {
                TransportKind::Amqp
            }
        }
    }

    fn value(&self) -> Result<Value, AsyncApiBuildError> {
        match self {
            Self::WebSocketProtocol(value) => value.value(),
            Self::CustomWebSocketProtocol(value) => value.value(),
            Self::WebSocketOutcome(value) => value.value(),
            Self::RabbitMqTopology(value) => value.value(),
            Self::Settlement(value) => value.value(),
            Self::DeliveryGuarantee(value) => value.value(),
        }
    }
}

/// WebSocket frame format carrying the Lily v2 JSON envelope.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WebSocketWireFormat {
    /// RFC 6455 text frame containing UTF-8 JSON.
    Text,
    /// RFC 6455 binary frame containing UTF-8 JSON bytes.
    Binary,
}

/// Nested payload kind declared by the Lily v2 envelope.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WebSocketContentKind {
    /// Typed JSON value.
    Json,
    /// UTF-8 text value.
    Text,
    /// Base64-encoded binary value.
    Binary,
}

impl WebSocketContentKind {
    fn normalized(&self) -> Result<&str, AsyncApiBuildError> {
        match self {
            Self::Json => Ok("json"),
            Self::Text => Ok("text"),
            Self::Binary => Ok("binary"),
        }
    }
}

/// Typed Lily v2 WebSocket protocol extension.
#[doc(hidden)]
#[derive(Debug)]
pub struct WebSocketProtocolExtension {
    wire_formats: Vec<WebSocketWireFormat>,
    namespace: String,
    event: String,
    content_kind: WebSocketContentKind,
    content_type: String,
    encoding: String,
}

impl WebSocketProtocolExtension {
    /// Creates the fixed Lily v2 envelope descriptor.
    pub fn new(
        namespace: impl Into<String>,
        event: impl Into<String>,
        content_kind: WebSocketContentKind,
        content_type: impl Into<String>,
        encoding: impl Into<String>,
    ) -> Self {
        Self {
            wire_formats: Vec::new(),
            namespace: namespace.into(),
            event: event.into(),
            content_kind,
            content_type: content_type.into(),
            encoding: encoding.into(),
        }
    }

    /// Adds one accepted RFC 6455 frame format.
    pub fn wire_format(mut self, wire_format: WebSocketWireFormat) -> Self {
        self.wire_formats.push(wire_format);
        self
    }

    fn value(&self) -> Result<Value, AsyncApiBuildError> {
        validate_route_token(
            "extension.websocket_protocol.namespace",
            &self.namespace,
            limits::WEBSOCKET_NAMESPACE_BYTES,
        )?;
        validate_event_route(&self.namespace, &self.event)?;
        validate_required(
            "extension.websocket_protocol.content_type",
            &self.content_type,
            limits::WEBSOCKET_CONTENT_TYPE_BYTES,
        )?;
        let content_type = normalize_mime(
            "extension.websocket_protocol.content_type",
            &self.content_type,
        )?;
        validate_route_token(
            "extension.websocket_protocol.encoding",
            &self.encoding,
            limits::CONTENT_ENCODING_BYTES,
        )?;
        let expected_encoding = match self.content_kind {
            WebSocketContentKind::Json | WebSocketContentKind::Text => "identity",
            WebSocketContentKind::Binary => "base64",
        };
        let expected_content_type = match self.content_kind {
            WebSocketContentKind::Json => "application/json",
            WebSocketContentKind::Text => "text/plain; charset=utf-8",
            WebSocketContentKind::Binary => "application/octet-stream",
        };
        if content_type != expected_content_type || self.content_type != expected_content_type {
            return Err(AsyncApiBuildError::validation(
                "extension.websocket_protocol.content_type",
                format!(
                    "built-in `{}` content must use exact `{expected_content_type}`",
                    self.content_kind.normalized()?
                ),
            ));
        }
        if self.encoding != expected_encoding {
            return Err(AsyncApiBuildError::validation(
                "extension.websocket_protocol.encoding",
                format!(
                    "built-in `{}` content must use `{expected_encoding}` encoding",
                    self.content_kind.normalized()?
                ),
            ));
        }
        if self.wire_formats.is_empty() {
            return Err(AsyncApiBuildError::validation(
                "extension.websocket_protocol.wire_formats",
                "must contain at least one accepted wire format",
            ));
        }
        validate_count(
            "extension.websocket_protocol.wire_formats",
            self.wire_formats.len(),
            limits::WEBSOCKET_WIRE_FORMAT_COUNT,
        )?;
        let mut wire_formats = self.wire_formats.clone();
        wire_formats.sort();
        if wire_formats.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(AsyncApiBuildError::validation(
                "extension.websocket_protocol.wire_formats",
                "duplicate entries are not allowed",
            ));
        }
        let value = WebSocketProtocolValue {
            codec: "lily_v2",
            envelope_version: 2,
            subprotocol: "lily.v2",
            wire_formats,
            namespace: &self.namespace,
            event: &self.event,
            content_kind: self.content_kind.normalized()?,
            content_type,
            encoding: &self.encoding,
        };
        bounded_value(WEBSOCKET_PROTOCOL_KEY, &value)
    }
}

#[derive(Serialize)]
struct WebSocketProtocolValue<'a> {
    codec: &'static str,
    envelope_version: u8,
    subprotocol: &'static str,
    wire_formats: Vec<WebSocketWireFormat>,
    namespace: &'a str,
    event: &'a str,
    content_kind: &'a str,
    content_type: String,
    encoding: &'a str,
}

/// Closed custom-codec error representation advertised by an accepted codec.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CustomWebSocketErrorRepresentation {
    /// A codec-owned application message described by a separate message/reply descriptor.
    ApplicationMessage,
    /// Errors are represented only by an RFC 6455 Close control frame.
    CloseFrame,
    /// The codec does not support a documented wire error representation.
    Unsupported,
}

/// Typed protocol contract for an accepted custom WebSocket codec.
///
/// This descriptor intentionally does not inherit Lily v2's version,
/// subprotocol, envelope, content, or error semantics.
#[doc(hidden)]
#[derive(Debug)]
pub struct CustomWebSocketProtocolExtension {
    protocol_name: String,
    protocol_version: String,
    compatible_subprotocols: Vec<String>,
    subprotocol_required: bool,
    wire_formats: Vec<WebSocketWireFormat>,
    namespace: String,
    event: String,
    content_kind: String,
    content_type: String,
    encoding: String,
    error_representation: CustomWebSocketErrorRepresentation,
}

impl CustomWebSocketProtocolExtension {
    /// Creates a custom codec protocol descriptor from its accepted registration.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        protocol_name: impl Into<String>,
        protocol_version: impl Into<String>,
        namespace: impl Into<String>,
        event: impl Into<String>,
        content_kind: impl Into<String>,
        content_type: impl Into<String>,
        encoding: impl Into<String>,
        error_representation: CustomWebSocketErrorRepresentation,
    ) -> Self {
        Self {
            protocol_name: protocol_name.into(),
            protocol_version: protocol_version.into(),
            compatible_subprotocols: Vec::new(),
            subprotocol_required: false,
            wire_formats: Vec::new(),
            namespace: namespace.into(),
            event: event.into(),
            content_kind: content_kind.into(),
            content_type: content_type.into(),
            encoding: encoding.into(),
            error_representation,
        }
    }

    /// Adds one compatible RFC 6455 subprotocol token.
    pub fn compatible_subprotocol(mut self, subprotocol: impl Into<String>) -> Self {
        self.compatible_subprotocols.push(subprotocol.into());
        self
    }

    /// Declares whether negotiation of one compatible subprotocol is mandatory.
    pub fn subprotocol_required(mut self, required: bool) -> Self {
        self.subprotocol_required = required;
        self
    }

    /// Adds one accepted RFC 6455 frame format.
    pub fn wire_format(mut self, wire_format: WebSocketWireFormat) -> Self {
        self.wire_formats.push(wire_format);
        self
    }

    fn value(&self) -> Result<Value, AsyncApiBuildError> {
        validate_route_token(
            "extension.websocket_protocol.protocol_name",
            &self.protocol_name,
            limits::WEBSOCKET_PROTOCOL_NAME_BYTES,
        )?;
        validate_route_token(
            "extension.websocket_protocol.protocol_version",
            &self.protocol_version,
            limits::WEBSOCKET_PROTOCOL_VERSION_BYTES,
        )?;
        validate_route_token(
            "extension.websocket_protocol.namespace",
            &self.namespace,
            limits::WEBSOCKET_NAMESPACE_BYTES,
        )?;
        validate_event_route(&self.namespace, &self.event)?;
        validate_route_token(
            "extension.websocket_protocol.content_kind",
            &self.content_kind,
            limits::IDENTIFIER_BYTES,
        )?;
        validate_required(
            "extension.websocket_protocol.content_type",
            &self.content_type,
            limits::WEBSOCKET_CONTENT_TYPE_BYTES,
        )?;
        let content_type = normalize_mime(
            "extension.websocket_protocol.content_type",
            &self.content_type,
        )?;
        validate_route_token(
            "extension.websocket_protocol.encoding",
            &self.encoding,
            limits::CONTENT_ENCODING_BYTES,
        )?;
        validate_count(
            "extension.websocket_protocol.compatible_subprotocols",
            self.compatible_subprotocols.len(),
            limits::WEBSOCKET_SUBPROTOCOL_COUNT,
        )?;
        let mut compatible_subprotocols = self.compatible_subprotocols.clone();
        for subprotocol in &compatible_subprotocols {
            validate_websocket_subprotocol(subprotocol)?;
        }
        compatible_subprotocols.sort();
        if compatible_subprotocols
            .windows(2)
            .any(|pair| pair[0] == pair[1])
        {
            return Err(AsyncApiBuildError::validation(
                "extension.websocket_protocol.compatible_subprotocols",
                "duplicate subprotocols are not allowed",
            ));
        }
        if self.subprotocol_required && compatible_subprotocols.is_empty() {
            return Err(AsyncApiBuildError::validation(
                "extension.websocket_protocol.compatible_subprotocols",
                "must not be empty when subprotocol negotiation is required",
            ));
        }
        if self.wire_formats.is_empty() {
            return Err(AsyncApiBuildError::validation(
                "extension.websocket_protocol.wire_formats",
                "must contain at least one accepted wire format",
            ));
        }
        validate_count(
            "extension.websocket_protocol.wire_formats",
            self.wire_formats.len(),
            limits::WEBSOCKET_WIRE_FORMAT_COUNT,
        )?;
        let mut wire_formats = self.wire_formats.clone();
        wire_formats.sort();
        if wire_formats.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(AsyncApiBuildError::validation(
                "extension.websocket_protocol.wire_formats",
                "duplicate entries are not allowed",
            ));
        }
        bounded_value(
            WEBSOCKET_PROTOCOL_KEY,
            &CustomWebSocketProtocolValue {
                codec: "custom",
                protocol_name: &self.protocol_name,
                protocol_version: &self.protocol_version,
                compatible_subprotocols,
                subprotocol_required: self.subprotocol_required,
                wire_formats,
                namespace: &self.namespace,
                event: &self.event,
                content_kind: &self.content_kind,
                content_type,
                encoding: &self.encoding,
                error_representation: self.error_representation,
            },
        )
    }
}

#[derive(Serialize)]
struct CustomWebSocketProtocolValue<'a> {
    codec: &'static str,
    protocol_name: &'a str,
    protocol_version: &'a str,
    compatible_subprotocols: Vec<String>,
    subprotocol_required: bool,
    wire_formats: Vec<WebSocketWireFormat>,
    namespace: &'a str,
    event: &'a str,
    content_kind: &'a str,
    content_type: String,
    encoding: &'a str,
    error_representation: CustomWebSocketErrorRepresentation,
}

/// WebSocket action outcome exposed by a documented operation.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WebSocketOutcomeKind {
    /// Correlated acknowledgement envelope.
    Ack,
    /// Successful action with no application reply.
    NoReply,
    /// Static, bounded application emit.
    Emit,
    /// Safe public KeepOpen error envelope.
    Error,
    /// RFC 6455 Close control frame.
    Close,
}

/// Closed target kinds supported by Lily's accepted WebSocket dispatch model.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WebSocketEmitTarget {
    /// The current action's connection.
    Direct,
    /// Every registered connection.
    All,
    /// Every connection in one namespace.
    Namespace,
    /// One room in one namespace.
    Room,
    /// The union of several rooms in one namespace.
    Rooms,
    /// Every connection for one principal in one namespace.
    Principal,
    /// One explicit connection.
    Connection,
    /// An explicit bounded connection set.
    Connections,
}

/// One statically declared WebSocket emit destination.
#[doc(hidden)]
#[derive(Debug, Serialize)]
pub struct WebSocketEmitDescriptor {
    target: WebSocketEmitTarget,
    event: String,
}

impl WebSocketEmitDescriptor {
    /// Creates a target/event pair from accepted action metadata.
    pub fn new(target: WebSocketEmitTarget, event: impl Into<String>) -> Self {
        Self {
            target,
            event: event.into(),
        }
    }

    fn validate(&self) -> Result<(), AsyncApiBuildError> {
        validate_event_route_any_namespace("extension.websocket_outcome.emit.event", &self.event)?;
        Ok(())
    }
}

/// One non-exhaustive, safe public WebSocket error catalog entry.
#[doc(hidden)]
#[derive(Debug, Serialize)]
pub struct WebSocketErrorDescriptor {
    code: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

impl WebSocketErrorDescriptor {
    /// Creates a bounded public error entry; internal sources are never accepted.
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            description: None,
        }
    }

    /// Adds bounded documentation prose to the public error entry.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    fn validate(&self) -> Result<(), AsyncApiBuildError> {
        validate_required(
            "extension.websocket_outcome.error.code",
            &self.code,
            limits::WEBSOCKET_ERROR_CODE_BYTES,
        )?;
        let mut bytes = self.code.bytes();
        if !bytes.next().is_some_and(|byte| byte.is_ascii_uppercase())
            || !bytes.all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(AsyncApiBuildError::validation(
                "extension.websocket_outcome.error.code",
                "must match ^[A-Z][A-Z0-9_]*$",
            ));
        }
        validate_required_text(
            "extension.websocket_outcome.error.message",
            &self.message,
            limits::WEBSOCKET_PUBLIC_MESSAGE_BYTES,
            TextKind::Token,
        )?;
        validate_optional_description(
            "extension.websocket_outcome.error.description",
            self.description.as_deref(),
            limits::WEBSOCKET_ERROR_DESCRIPTION_BYTES,
        )?;
        Ok(())
    }
}

/// Bounded RFC 6455 Close control-frame contract.
#[doc(hidden)]
#[derive(Debug)]
pub struct WebSocketCloseDescriptor {
    _private: (),
}

impl WebSocketCloseDescriptor {
    /// Creates a non-message close descriptor for the accepted code allowlist.
    pub const fn lily_application() -> Self {
        Self { _private: () }
    }

    fn value(&self) -> WebSocketCloseValue {
        WebSocketCloseValue {
            standard_codes: vec![
                1000, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014,
            ],
            application_code_range: WebSocketApplicationCloseCodeRange {
                minimum: 3000,
                maximum: 4999,
            },
            reason_max_bytes: limits::WEBSOCKET_CLOSE_REASON_BYTES,
            application_message: false,
        }
    }
}

/// Typed operation-level WebSocket result contract.
#[doc(hidden)]
#[derive(Debug)]
pub struct WebSocketOutcomeExtension {
    outcomes: Vec<WebSocketOutcomeKind>,
    emit: Option<WebSocketEmitDescriptor>,
    errors: Vec<WebSocketErrorDescriptor>,
    close: Option<WebSocketCloseDescriptor>,
}

impl WebSocketOutcomeExtension {
    /// Creates an outcome descriptor from the exact accepted outcome set.
    pub fn new(outcomes: Vec<WebSocketOutcomeKind>) -> Self {
        Self {
            outcomes,
            emit: None,
            errors: Vec::new(),
            close: None,
        }
    }

    /// Adds one static emit target/event contract.
    pub fn emit(mut self, emit: WebSocketEmitDescriptor) -> Self {
        self.emit = Some(emit);
        self
    }

    /// Adds one bounded, non-exhaustive public error entry.
    pub fn error(mut self, error: WebSocketErrorDescriptor) -> Self {
        self.errors.push(error);
        self
    }

    /// Adds the RFC 6455 Close control-frame contract.
    pub fn close(mut self, close: WebSocketCloseDescriptor) -> Self {
        self.close = Some(close);
        self
    }

    fn value(&self) -> Result<Value, AsyncApiBuildError> {
        if self.outcomes.is_empty() {
            return Err(AsyncApiBuildError::validation(
                "extension.websocket_outcome.outcomes",
                "must contain at least one accepted outcome",
            ));
        }
        validate_count(
            "extension.websocket_outcome.outcomes",
            self.outcomes.len(),
            limits::WEBSOCKET_OUTCOME_COUNT,
        )?;
        let mut outcomes = self.outcomes.clone();
        outcomes.sort();
        if outcomes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(AsyncApiBuildError::validation(
                "extension.websocket_outcome.outcomes",
                "duplicate entries are not allowed",
            ));
        }
        let has = |expected| outcomes.binary_search(&expected).is_ok();
        if self.emit.is_some() != has(WebSocketOutcomeKind::Emit) {
            return Err(AsyncApiBuildError::validation(
                "extension.websocket_outcome.emit",
                "must be present exactly when `emit` is an accepted outcome",
            ));
        }
        if !self.errors.is_empty() && !has(WebSocketOutcomeKind::Error) {
            return Err(AsyncApiBuildError::validation(
                "extension.websocket_outcome.errors",
                "cannot be present unless `error` is an accepted outcome",
            ));
        }
        if self.close.is_some() != has(WebSocketOutcomeKind::Close) {
            return Err(AsyncApiBuildError::validation(
                "extension.websocket_outcome.close",
                "must be present exactly when `close` is an accepted outcome",
            ));
        }
        if let Some(emit) = &self.emit {
            emit.validate()?;
        }
        validate_count(
            "extension.websocket_outcome.errors",
            self.errors.len(),
            limits::WEBSOCKET_ERROR_COUNT,
        )?;
        for error in &self.errors {
            error.validate()?;
        }
        let mut errors = self.errors.iter().collect::<Vec<_>>();
        errors.sort_by(|left, right| left.code.cmp(&right.code));
        let codes = errors.iter().map(|error| &error.code).collect::<Vec<_>>();
        if codes.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(AsyncApiBuildError::validation(
                "extension.websocket_outcome.errors",
                "duplicate error codes are not allowed",
            ));
        }
        let close = self.close.as_ref().map(WebSocketCloseDescriptor::value);
        let value = WebSocketOutcomeValue {
            outcomes,
            emit: self.emit.as_ref(),
            errors,
            error_catalog_exhaustive: false,
            error_code_max_bytes: limits::WEBSOCKET_ERROR_CODE_BYTES,
            public_message_max_bytes: limits::WEBSOCKET_PUBLIC_MESSAGE_BYTES,
            close,
        };
        bounded_value(WEBSOCKET_OUTCOME_KEY, &value)
    }
}

#[derive(Serialize)]
struct WebSocketOutcomeValue<'a> {
    outcomes: Vec<WebSocketOutcomeKind>,
    #[serde(skip_serializing_if = "Option::is_none")]
    emit: Option<&'a WebSocketEmitDescriptor>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    errors: Vec<&'a WebSocketErrorDescriptor>,
    error_catalog_exhaustive: bool,
    error_code_max_bytes: usize,
    public_message_max_bytes: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    close: Option<WebSocketCloseValue>,
}

#[derive(Serialize)]
struct WebSocketCloseValue {
    standard_codes: Vec<u16>,
    application_code_range: WebSocketApplicationCloseCodeRange,
    reason_max_bytes: usize,
    application_message: bool,
}

#[derive(Serialize)]
struct WebSocketApplicationCloseCodeRange {
    minimum: u16,
    maximum: u16,
}

/// RabbitMQ topology ownership accepted by the consumer plan.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RabbitMqTopologyOwnership {
    /// Lily declares the accepted topology during explicit bootstrap.
    FrameworkManaged,
    /// Lily only consumes topology created outside the application.
    External,
}

/// RabbitMQ physical queue type.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RabbitMqQueueType {
    /// RabbitMQ classic queue.
    Classic,
    /// RabbitMQ quorum queue.
    Quorum,
}

/// Exchange kind currently accepted by Lily's immutable RabbitMQ topology plan.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RabbitMqExchangeKind {
    /// Exact routing-key exchange.
    Direct,
}

/// One framework-owned retry bucket in an accepted topology plan.
#[doc(hidden)]
#[derive(Debug, Serialize)]
pub struct RabbitMqRetryBucketDescriptor {
    queue: String,
    routing_key: String,
    delay_ms: u64,
}

/// Main exchange/queue/routing tuple from one accepted RabbitMQ topology plan.
#[doc(hidden)]
#[derive(Debug)]
pub struct RabbitMqMainTopologyDescriptor {
    exchange: String,
    exchange_kind: RabbitMqExchangeKind,
    queue: String,
    routing_key: String,
}

impl RabbitMqMainTopologyDescriptor {
    /// Creates the accepted main exchange/queue/routing tuple.
    pub fn new(
        exchange: impl Into<String>,
        exchange_kind: RabbitMqExchangeKind,
        queue: impl Into<String>,
        routing_key: impl Into<String>,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            exchange_kind,
            queue: queue.into(),
            routing_key: routing_key.into(),
        }
    }

    fn validate(&self) -> Result<(), AsyncApiBuildError> {
        for (field, value) in [
            ("extension.rabbitmq_topology.exchange", &self.exchange),
            ("extension.rabbitmq_topology.queue", &self.queue),
            ("extension.rabbitmq_topology.routing_key", &self.routing_key),
        ] {
            validate_required(field, value, limits::RABBITMQ_NAME_BYTES)?;
        }
        Ok(())
    }
}

impl RabbitMqRetryBucketDescriptor {
    /// Creates a retry queue/routing/delay descriptor.
    pub fn new(queue: impl Into<String>, routing_key: impl Into<String>, delay_ms: u64) -> Self {
        Self {
            queue: queue.into(),
            routing_key: routing_key.into(),
            delay_ms,
        }
    }

    fn validate(&self) -> Result<(), AsyncApiBuildError> {
        validate_required(
            "extension.rabbitmq_topology.retry.queue",
            &self.queue,
            limits::RABBITMQ_NAME_BYTES,
        )?;
        validate_required(
            "extension.rabbitmq_topology.retry.routing_key",
            &self.routing_key,
            limits::RABBITMQ_NAME_BYTES,
        )?;
        if self.delay_ms == 0 {
            return Err(AsyncApiBuildError::validation(
                "extension.rabbitmq_topology.retry.delay_ms",
                "must be greater than zero",
            ));
        }
        Ok(())
    }
}

/// Retry delays eligible for one exact retry-attempt generation.
#[doc(hidden)]
#[derive(Debug)]
pub struct RabbitMqRetryAttemptDescriptor {
    attempt: u32,
    delay_ms: Vec<u64>,
}

impl RabbitMqRetryAttemptDescriptor {
    /// Creates an attempt-to-delay candidate mapping from the accepted topology plan.
    pub fn new(attempt: u32, delay_ms: Vec<u64>) -> Self {
        Self { attempt, delay_ms }
    }

    fn normalized(&self) -> Result<RabbitMqRetryAttemptValue, AsyncApiBuildError> {
        if self.attempt == 0
            || usize::try_from(self.attempt).map_or(true, |attempt| {
                attempt > limits::RABBITMQ_RETRY_ATTEMPT_COUNT
            })
        {
            return Err(AsyncApiBuildError::validation(
                "extension.rabbitmq_topology.retry_attempts.attempt",
                format!("must be in 1..={}", limits::RABBITMQ_RETRY_ATTEMPT_COUNT),
            ));
        }
        if self.delay_ms.is_empty() {
            return Err(AsyncApiBuildError::validation(
                "extension.rabbitmq_topology.retry_attempts.delay_ms",
                "must contain at least one eligible retry delay",
            ));
        }
        validate_count(
            "extension.rabbitmq_topology.retry_attempts.delay_ms",
            self.delay_ms.len(),
            limits::RABBITMQ_RETRY_CANDIDATE_COUNT,
        )?;
        let mut delay_ms = self.delay_ms.clone();
        delay_ms.sort_unstable();
        if delay_ms.contains(&0) {
            return Err(AsyncApiBuildError::validation(
                "extension.rabbitmq_topology.retry_attempts.delay_ms",
                "retry delay must be greater than zero",
            ));
        }
        if delay_ms.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(AsyncApiBuildError::validation(
                "extension.rabbitmq_topology.retry_attempts.delay_ms",
                "duplicate delay candidates are not allowed",
            ));
        }
        Ok(RabbitMqRetryAttemptValue {
            attempt: self.attempt,
            delay_ms,
        })
    }
}

#[derive(Debug, Serialize)]
struct RabbitMqRetryAttemptValue {
    attempt: u32,
    delay_ms: Vec<u64>,
}

/// Typed dead-letter exchange/queue/routing projection.
#[doc(hidden)]
#[derive(Debug, Serialize)]
pub struct RabbitMqDeadLetterDescriptor {
    exchange: String,
    queue: String,
    routing_key: String,
}

impl RabbitMqDeadLetterDescriptor {
    /// Creates an accepted dead-letter topology descriptor.
    pub fn new(
        exchange: impl Into<String>,
        queue: impl Into<String>,
        routing_key: impl Into<String>,
    ) -> Self {
        Self {
            exchange: exchange.into(),
            queue: queue.into(),
            routing_key: routing_key.into(),
        }
    }

    fn validate(&self) -> Result<(), AsyncApiBuildError> {
        validate_required(
            "extension.rabbitmq_topology.dead_letter.exchange",
            &self.exchange,
            limits::RABBITMQ_NAME_BYTES,
        )?;
        validate_required(
            "extension.rabbitmq_topology.dead_letter.queue",
            &self.queue,
            limits::RABBITMQ_NAME_BYTES,
        )?;
        validate_required(
            "extension.rabbitmq_topology.dead_letter.routing_key",
            &self.routing_key,
            limits::RABBITMQ_NAME_BYTES,
        )?;
        Ok(())
    }
}

/// Typed projection of one accepted RabbitMQ physical queue topology.
#[doc(hidden)]
#[derive(Debug)]
pub struct RabbitMqTopologyExtension {
    ownership: RabbitMqTopologyOwnership,
    queue_type: RabbitMqQueueType,
    main: RabbitMqMainTopologyDescriptor,
    retry_exchange: Option<String>,
    retry_buckets: Vec<RabbitMqRetryBucketDescriptor>,
    retry_attempts: Vec<RabbitMqRetryAttemptDescriptor>,
    dead_letter: RabbitMqDeadLetterDescriptor,
}

impl RabbitMqTopologyExtension {
    /// Creates an accepted main exchange/queue/routing projection.
    pub fn new(
        ownership: RabbitMqTopologyOwnership,
        queue_type: RabbitMqQueueType,
        main: RabbitMqMainTopologyDescriptor,
        retry_exchange: impl Into<String>,
        dead_letter: RabbitMqDeadLetterDescriptor,
    ) -> Self {
        Self {
            ownership,
            queue_type,
            main,
            retry_exchange: Some(retry_exchange.into()),
            retry_buckets: Vec::new(),
            retry_attempts: Vec::new(),
            dead_letter,
        }
    }

    /// Creates an accepted topology whose retry policy has no physical retry
    /// exchange or retry-delay queues.
    pub fn without_retry(
        ownership: RabbitMqTopologyOwnership,
        queue_type: RabbitMqQueueType,
        main: RabbitMqMainTopologyDescriptor,
        dead_letter: RabbitMqDeadLetterDescriptor,
    ) -> Self {
        Self {
            ownership,
            queue_type,
            main,
            retry_exchange: None,
            retry_buckets: Vec::new(),
            retry_attempts: Vec::new(),
            dead_letter,
        }
    }

    /// Adds one effective retry bucket from the immutable topology plan.
    pub fn retry_bucket(mut self, bucket: RabbitMqRetryBucketDescriptor) -> Self {
        self.retry_buckets.push(bucket);
        self
    }

    /// Adds one attempt-to-delay route from the accepted retry plan.
    pub fn retry_attempt(mut self, route: RabbitMqRetryAttemptDescriptor) -> Self {
        self.retry_attempts.push(route);
        self
    }

    fn value(&self) -> Result<Value, AsyncApiBuildError> {
        self.main.validate()?;
        if let Some(retry_exchange) = &self.retry_exchange {
            validate_required(
                "extension.rabbitmq_topology.retry_exchange",
                retry_exchange,
                limits::RABBITMQ_NAME_BYTES,
            )?;
        }
        validate_count(
            "extension.rabbitmq_topology.retry_buckets",
            self.retry_buckets.len(),
            limits::RABBITMQ_RETRY_BUCKET_COUNT,
        )?;
        for bucket in &self.retry_buckets {
            bucket.validate()?;
        }
        let mut retry_buckets = self.retry_buckets.iter().collect::<Vec<_>>();
        retry_buckets.sort_by(|left, right| {
            (left.delay_ms, &left.queue, &left.routing_key).cmp(&(
                right.delay_ms,
                &right.queue,
                &right.routing_key,
            ))
        });
        if retry_buckets
            .windows(2)
            .any(|pair| pair[0].delay_ms == pair[1].delay_ms)
        {
            return Err(AsyncApiBuildError::validation(
                "extension.rabbitmq_topology.retry_buckets",
                "each physical retry delay must have exactly one bucket",
            ));
        }
        validate_count(
            "extension.rabbitmq_topology.retry_attempts",
            self.retry_attempts.len(),
            limits::RABBITMQ_RETRY_ATTEMPT_COUNT,
        )?;
        let mut retry_attempts = self
            .retry_attempts
            .iter()
            .map(RabbitMqRetryAttemptDescriptor::normalized)
            .collect::<Result<Vec<_>, _>>()?;
        retry_attempts.sort_by_key(|route| route.attempt);
        if retry_attempts
            .iter()
            .enumerate()
            .any(|(index, route)| usize::try_from(route.attempt).ok() != Some(index + 1))
        {
            return Err(AsyncApiBuildError::validation(
                "extension.rabbitmq_topology.retry_attempts",
                "attempt numbers must be unique and contiguous from 1",
            ));
        }
        let bucket_delays = retry_buckets
            .iter()
            .map(|bucket| bucket.delay_ms)
            .collect::<Vec<_>>();
        if retry_attempts
            .iter()
            .flat_map(|route| route.delay_ms.iter())
            .any(|delay| bucket_delays.binary_search(delay).is_err())
        {
            return Err(AsyncApiBuildError::validation(
                "extension.rabbitmq_topology.retry_attempts.delay_ms",
                "every attempt delay must reference an accepted retry bucket",
            ));
        }
        let mapped_delays = retry_attempts
            .iter()
            .flat_map(|route| route.delay_ms.iter().copied())
            .collect::<std::collections::BTreeSet<_>>();
        if retry_buckets.is_empty() != retry_attempts.is_empty()
            || bucket_delays
                .iter()
                .any(|delay| !mapped_delays.contains(delay))
        {
            return Err(AsyncApiBuildError::validation(
                "extension.rabbitmq_topology.retry_attempts",
                "retry buckets and attempt routes must describe the same effective delay set",
            ));
        }
        if self.retry_exchange.is_some() == retry_buckets.is_empty() {
            return Err(AsyncApiBuildError::validation(
                "extension.rabbitmq_topology.retry_exchange",
                "must be present exactly when the accepted topology has retry buckets",
            ));
        }
        self.dead_letter.validate()?;
        let value = RabbitMqTopologyValue {
            ownership: self.ownership,
            queue_type: self.queue_type,
            exchange: &self.main.exchange,
            exchange_kind: self.main.exchange_kind,
            queue: &self.main.queue,
            routing_key: &self.main.routing_key,
            retry_exchange: self.retry_exchange.as_deref(),
            retry_buckets,
            retry_attempts,
            dead_letter: &self.dead_letter,
        };
        bounded_value_with_maximum(
            RABBITMQ_TOPOLOGY_KEY,
            &value,
            limits::RABBITMQ_TOPOLOGY_EXTENSION_BYTES,
        )
    }
}

#[derive(Serialize)]
struct RabbitMqTopologyValue<'a> {
    ownership: RabbitMqTopologyOwnership,
    queue_type: RabbitMqQueueType,
    exchange: &'a str,
    exchange_kind: RabbitMqExchangeKind,
    queue: &'a str,
    routing_key: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    retry_exchange: Option<&'a str>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    retry_buckets: Vec<&'a RabbitMqRetryBucketDescriptor>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    retry_attempts: Vec<RabbitMqRetryAttemptValue>,
    dead_letter: &'a RabbitMqDeadLetterDescriptor,
}

/// Framework-owned RabbitMQ terminal settlement contract.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Default)]
pub struct SettlementExtension;

impl SettlementExtension {
    /// Creates Lily's fixed manual-ACK settlement mapping.
    pub const fn manual_ack() -> Self {
        Self
    }

    fn value(self) -> Result<Value, AsyncApiBuildError> {
        bounded_value(
            SETTLEMENT_KEY,
            &SettlementValue {
                manual_ack: true,
                success: "ack",
                retryable: RetryableSettlementValue {
                    when_retry_budget_available: "retry_handoff_then_ack",
                    when_retry_budget_exhausted: "dead_letter_handoff_then_ack",
                },
                permanent: "dead_letter_handoff_then_ack",
                handoff_failure: "nack_requeue",
            },
        )
    }
}

#[derive(Serialize)]
struct SettlementValue {
    manual_ack: bool,
    success: &'static str,
    retryable: RetryableSettlementValue,
    permanent: &'static str,
    handoff_failure: &'static str,
}

#[derive(Serialize)]
struct RetryableSettlementValue {
    when_retry_budget_available: &'static str,
    when_retry_budget_exhausted: &'static str,
}

/// Transactional inbox backend owned by an accepted handler plan.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TransactionalInboxBackend {
    /// PostgreSQL local transaction boundary.
    Postgresql,
    /// MongoDB local transaction boundary.
    Mongodb,
}

/// Typed per-operation delivery guarantee boundary.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryGuaranteeExtension {
    /// Broker delivery and settlement are at-least-once.
    AtLeastOnce,
    /// Business effects and inbox/outbox state share one backend-local transaction.
    TransactionalInbox(TransactionalInboxBackend),
}

impl DeliveryGuaranteeExtension {
    fn value(self) -> Result<Value, AsyncApiBuildError> {
        let (delivery_guarantee, backend, local_transaction) = match self {
            Self::AtLeastOnce => ("at_least_once", None, false),
            Self::TransactionalInbox(backend) => ("transactional_inbox", Some(backend), true),
        };
        bounded_value(
            DELIVERY_GUARANTEE_KEY,
            &DeliveryGuaranteeValue {
                delivery_guarantee,
                backend,
                local_transaction,
                broker_database_2pc: false,
                global_exactly_once: false,
            },
        )
    }
}

#[derive(Serialize)]
struct DeliveryGuaranteeValue {
    delivery_guarantee: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    backend: Option<TransactionalInboxBackend>,
    local_transaction: bool,
    broker_database_2pc: bool,
    global_exactly_once: bool,
}

pub(crate) fn build_extensions(
    transport: TransportKind,
    scope: ExtensionScope,
    runtime_identity: &str,
    extensions: Vec<LilyExtension>,
) -> Result<BTreeMap<String, Value>, AsyncApiBuildError> {
    validate_required(
        "extension.runtime_identity.identity",
        runtime_identity,
        limits::RUNTIME_IDENTITY_BYTES,
    )?;
    validate_count(
        "extensions",
        extensions.len(),
        limits::LILY_EXTENSION_COUNT - 1,
    )?;
    let mut output = BTreeMap::new();
    output.insert(
        RUNTIME_IDENTITY_KEY.to_owned(),
        bounded_value(
            RUNTIME_IDENTITY_KEY,
            &RuntimeIdentityValue {
                identity: runtime_identity,
            },
        )?,
    );
    for extension in extensions {
        let key = extension.key();
        if extension.expected_scope() != scope {
            return Err(AsyncApiBuildError::validation(
                "extension.scope",
                format!("`{key}` cannot be attached to a {}", scope.as_str()),
            ));
        }
        if extension.expected_transport() != transport {
            return Err(AsyncApiBuildError::validation(
                "extension.transport",
                format!(
                    "`{key}` cannot be attached to a {} contribution",
                    transport.prefix()
                ),
            ));
        }
        if output.insert(key.to_owned(), extension.value()?).is_some() {
            return Err(AsyncApiBuildError::duplicate("extension", key));
        }
    }
    Ok(output)
}

#[derive(Serialize)]
struct RuntimeIdentityValue<'a> {
    identity: &'a str,
}

fn bounded_value<T: Serialize + ?Sized>(
    key: &'static str,
    value: &T,
) -> Result<Value, AsyncApiBuildError> {
    bounded_value_with_maximum(key, value, limits::LILY_EXTENSION_BYTES)
}

fn bounded_value_with_maximum<T: Serialize + ?Sized>(
    key: &'static str,
    value: &T,
    maximum: usize,
) -> Result<Value, AsyncApiBuildError> {
    let bytes = canonical_json_bytes("extension", value, maximum)?;
    serde_json::from_slice(&bytes).map_err(|error| AsyncApiBuildError::Serialization {
        detail: format!("canonical `{key}` extension could not be materialized: {error}"),
    })
}

fn validate_route_token(
    field: &'static str,
    value: &str,
    maximum: usize,
) -> Result<(), AsyncApiBuildError> {
    validate_required(field, value, maximum)?;
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(AsyncApiBuildError::validation(
            field,
            "must contain only ASCII letters, digits, '-', '_' and '.'",
        ));
    }
    Ok(())
}

fn validate_event_route(namespace: &str, event: &str) -> Result<(), AsyncApiBuildError> {
    validate_event_route_any_namespace("extension.websocket_protocol.event", event)?;
    let event_namespace = event.split_once(':').map(|(namespace, _)| namespace);
    if event_namespace != Some(namespace) {
        return Err(AsyncApiBuildError::validation(
            "extension.websocket_protocol.event",
            "route namespace must match the protocol namespace",
        ));
    }
    Ok(())
}

fn validate_event_route_any_namespace(
    field: &'static str,
    event: &str,
) -> Result<(), AsyncApiBuildError> {
    validate_required(field, event, limits::WEBSOCKET_EVENT_BYTES)?;
    let Some((namespace, action)) = event.split_once(':') else {
        return Err(AsyncApiBuildError::validation(
            field,
            "must be one canonical `namespace:event` route",
        ));
    };
    if action.contains(':') {
        return Err(AsyncApiBuildError::validation(
            field,
            "must contain exactly one ':' separator",
        ));
    }
    validate_route_token(field, namespace, limits::WEBSOCKET_NAMESPACE_BYTES)?;
    validate_route_token(field, action, limits::WEBSOCKET_EVENT_BYTES)?;
    Ok(())
}

fn validate_websocket_subprotocol(value: &str) -> Result<(), AsyncApiBuildError> {
    validate_required(
        "extension.websocket_protocol.compatible_subprotocol",
        value,
        limits::WEBSOCKET_SUBPROTOCOL_BYTES,
    )?;
    if !value.bytes().all(|byte| {
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
    }) {
        return Err(AsyncApiBuildError::validation(
            "extension.websocket_protocol.compatible_subprotocol",
            "must be an RFC 7230 token",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extension_values_are_bounded_and_canonical() {
        let extension = LilyExtension::WebSocketProtocol(
            WebSocketProtocolExtension::new(
                "orders",
                "orders:created",
                WebSocketContentKind::Json,
                "application/json",
                "identity",
            )
            .wire_format(WebSocketWireFormat::Text)
            .wire_format(WebSocketWireFormat::Binary),
        );
        let first = build_extensions(
            TransportKind::WebSocket,
            ExtensionScope::Message,
            "orders:created",
            vec![extension],
        )
        .expect("extension");
        assert_eq!(
            first[WEBSOCKET_PROTOCOL_KEY]["content_type"],
            "application/json"
        );
        assert_eq!(first[WEBSOCKET_PROTOCOL_KEY]["envelope_version"], 2);
        assert_eq!(first[WEBSOCKET_PROTOCOL_KEY]["subprotocol"], "lily.v2");

        let mismatched_content = LilyExtension::WebSocketProtocol(
            WebSocketProtocolExtension::new(
                "orders",
                "orders:created",
                WebSocketContentKind::Json,
                "text/plain",
                "identity",
            )
            .wire_format(WebSocketWireFormat::Text),
        );
        assert!(
            build_extensions(
                TransportKind::WebSocket,
                ExtensionScope::Message,
                "orders:created",
                vec![mismatched_content],
            )
            .is_err()
        );

        let oversized = LilyExtension::WebSocketProtocol(
            WebSocketProtocolExtension::new(
                "n".repeat(limits::IDENTIFIER_BYTES + 1),
                "orders:event",
                WebSocketContentKind::Json,
                "application/json",
                "identity",
            )
            .wire_format(WebSocketWireFormat::Text),
        );
        assert!(
            build_extensions(
                TransportKind::WebSocket,
                ExtensionScope::Message,
                "orders:created",
                vec![oversized],
            )
            .is_err()
        );
    }

    #[test]
    fn custom_codec_protocol_is_explicit_bounded_and_order_stable() {
        let custom = |reverse: bool| {
            let protocol = CustomWebSocketProtocolExtension::new(
                "application_codec",
                "v3",
                "orders",
                "orders:created",
                "protobuf",
                "application/protobuf",
                "identity",
                CustomWebSocketErrorRepresentation::ApplicationMessage,
            )
            .subprotocol_required(true);
            let protocol = if reverse {
                protocol
                    .compatible_subprotocol("orders.v3")
                    .compatible_subprotocol("orders.v2")
                    .wire_format(WebSocketWireFormat::Binary)
                    .wire_format(WebSocketWireFormat::Text)
            } else {
                protocol
                    .compatible_subprotocol("orders.v2")
                    .compatible_subprotocol("orders.v3")
                    .wire_format(WebSocketWireFormat::Text)
                    .wire_format(WebSocketWireFormat::Binary)
            };
            LilyExtension::CustomWebSocketProtocol(protocol)
        };
        let first = build_extensions(
            TransportKind::WebSocket,
            ExtensionScope::Message,
            "orders:created",
            vec![custom(false)],
        )
        .expect("first custom protocol");
        let second = build_extensions(
            TransportKind::WebSocket,
            ExtensionScope::Message,
            "orders:created",
            vec![custom(true)],
        )
        .expect("second custom protocol");
        assert_eq!(first, second);
        assert_eq!(first[WEBSOCKET_PROTOCOL_KEY]["codec"], "custom");
        assert!(
            first[WEBSOCKET_PROTOCOL_KEY]
                .as_object()
                .is_some_and(|value| !value.contains_key("envelope_version"))
        );

        let required_without_token = LilyExtension::CustomWebSocketProtocol(
            CustomWebSocketProtocolExtension::new(
                "application_codec",
                "v3",
                "orders",
                "orders:created",
                "protobuf",
                "application/protobuf",
                "identity",
                CustomWebSocketErrorRepresentation::Unsupported,
            )
            .subprotocol_required(true)
            .wire_format(WebSocketWireFormat::Binary),
        );
        assert!(
            build_extensions(
                TransportKind::WebSocket,
                ExtensionScope::Message,
                "orders:created",
                vec![required_without_token],
            )
            .is_err()
        );
    }

    #[test]
    fn scope_transport_and_duplicate_extension_keys_fail_closed() {
        let protocol = || {
            LilyExtension::WebSocketProtocol(
                WebSocketProtocolExtension::new(
                    "orders",
                    "orders:created",
                    WebSocketContentKind::Json,
                    "application/json",
                    "identity",
                )
                .wire_format(WebSocketWireFormat::Text),
            )
        };
        assert!(
            build_extensions(
                TransportKind::WebSocket,
                ExtensionScope::Channel,
                "socket",
                vec![protocol()],
            )
            .is_err()
        );
        assert!(
            build_extensions(
                TransportKind::Amqp,
                ExtensionScope::Message,
                "event",
                vec![protocol()],
            )
            .is_err()
        );
        assert!(matches!(
            build_extensions(
                TransportKind::WebSocket,
                ExtensionScope::Message,
                "event",
                vec![protocol(), protocol()],
            ),
            Err(AsyncApiBuildError::Duplicate { .. })
        ));
    }

    #[test]
    fn queue_extensions_expose_fixed_guarantee_and_settlement_boundaries() {
        let values = build_extensions(
            TransportKind::Amqp,
            ExtensionScope::Operation,
            "orders.consume",
            vec![
                LilyExtension::Settlement(SettlementExtension::manual_ack()),
                LilyExtension::DeliveryGuarantee(DeliveryGuaranteeExtension::TransactionalInbox(
                    TransactionalInboxBackend::Postgresql,
                )),
            ],
        )
        .expect("extensions");
        assert_eq!(values[SETTLEMENT_KEY]["manual_ack"], true);
        assert_eq!(
            values[SETTLEMENT_KEY]["retryable"]["when_retry_budget_available"],
            "retry_handoff_then_ack"
        );
        assert_eq!(
            values[SETTLEMENT_KEY]["retryable"]["when_retry_budget_exhausted"],
            "dead_letter_handoff_then_ack"
        );
        assert_eq!(
            values[SETTLEMENT_KEY]["permanent"],
            "dead_letter_handoff_then_ack"
        );
        assert_eq!(
            values[DELIVERY_GUARANTEE_KEY]["delivery_guarantee"],
            "transactional_inbox"
        );
        assert_eq!(values[DELIVERY_GUARANTEE_KEY]["broker_database_2pc"], false);
        assert_eq!(values[DELIVERY_GUARANTEE_KEY]["global_exactly_once"], false);

        let mongodb = build_extensions(
            TransportKind::Amqp,
            ExtensionScope::Operation,
            "orders.consume.mongodb",
            vec![LilyExtension::DeliveryGuarantee(
                DeliveryGuaranteeExtension::TransactionalInbox(TransactionalInboxBackend::Mongodb),
            )],
        )
        .expect("MongoDB guarantee");
        assert_eq!(mongodb[DELIVERY_GUARANTEE_KEY]["backend"], "mongodb");
        assert_eq!(
            mongodb[DELIVERY_GUARANTEE_KEY]["broker_database_2pc"],
            false
        );
    }

    #[test]
    fn retry_exchange_is_emitted_only_for_an_accepted_retry_topology() {
        let topology = RabbitMqTopologyExtension::without_retry(
            RabbitMqTopologyOwnership::External,
            RabbitMqQueueType::Classic,
            RabbitMqMainTopologyDescriptor::new(
                "orders",
                RabbitMqExchangeKind::Direct,
                "orders",
                "orders.created",
            ),
            RabbitMqDeadLetterDescriptor::new("orders.dlx", "orders.dlq", "orders.dead"),
        );
        let value = topology.value().expect("topology without retry");
        assert!(value.get("retry_exchange").is_none());
        assert!(value.get("retry_buckets").is_none());
        assert!(value.get("retry_attempts").is_none());

        let inconsistent = RabbitMqTopologyExtension::new(
            RabbitMqTopologyOwnership::External,
            RabbitMqQueueType::Classic,
            RabbitMqMainTopologyDescriptor::new(
                "orders",
                RabbitMqExchangeKind::Direct,
                "orders",
                "orders.created",
            ),
            "orders.retry.v2",
            RabbitMqDeadLetterDescriptor::new("orders.dlx", "orders.dlq", "orders.dead"),
        );
        assert!(inconsistent.value().is_err());
    }

    fn retry_topology(bucket_count: usize, attempt_count: usize) -> RabbitMqTopologyExtension {
        let mut topology = RabbitMqTopologyExtension::new(
            RabbitMqTopologyOwnership::FrameworkManaged,
            RabbitMqQueueType::Quorum,
            RabbitMqMainTopologyDescriptor::new(
                "orders",
                RabbitMqExchangeKind::Direct,
                "orders",
                "orders.created",
            ),
            "orders.retry.v1",
            RabbitMqDeadLetterDescriptor::new("orders.dlx", "orders.dlq", "orders.dead"),
        );
        for index in 1..=bucket_count {
            let delay = u64::try_from(index).expect("test index");
            topology = topology.retry_bucket(RabbitMqRetryBucketDescriptor::new(
                format!("orders.retry.{index}"),
                format!("orders.retry.{index}"),
                delay,
            ));
        }
        for index in 1..=attempt_count {
            let delay = u64::try_from(index.min(bucket_count)).expect("test index");
            topology = topology.retry_attempt(RabbitMqRetryAttemptDescriptor::new(
                u32::try_from(index).expect("test index"),
                vec![delay],
            ));
        }
        topology
    }

    #[test]
    fn rabbitmq_retry_limits_match_the_accepted_runtime_plan() {
        assert!(
            retry_topology(
                limits::RABBITMQ_RETRY_BUCKET_COUNT,
                limits::RABBITMQ_RETRY_BUCKET_COUNT,
            )
            .value()
            .is_ok()
        );
        assert!(
            retry_topology(
                limits::RABBITMQ_RETRY_BUCKET_COUNT + 1,
                limits::RABBITMQ_RETRY_BUCKET_COUNT + 1,
            )
            .value()
            .is_err()
        );

        assert!(
            retry_topology(1, limits::RABBITMQ_RETRY_ATTEMPT_COUNT)
                .value()
                .is_ok()
        );
        assert!(
            retry_topology(1, limits::RABBITMQ_RETRY_ATTEMPT_COUNT + 1)
                .value()
                .is_err()
        );

        assert!(
            RabbitMqRetryAttemptDescriptor::new(
                1,
                (1..=limits::RABBITMQ_RETRY_CANDIDATE_COUNT)
                    .map(|value| u64::try_from(value).expect("test index"))
                    .collect(),
            )
            .normalized()
            .is_ok()
        );
        assert!(
            RabbitMqRetryAttemptDescriptor::new(
                1,
                (1..=limits::RABBITMQ_RETRY_CANDIDATE_COUNT + 1)
                    .map(|value| u64::try_from(value).expect("test index"))
                    .collect(),
            )
            .normalized()
            .is_err()
        );
    }

    #[test]
    fn maximum_accepted_rabbitmq_topology_fits_its_dedicated_budget() {
        fn rabbit_name(prefix: char, index: usize) -> String {
            let suffix = format!("{index:03}");
            format!(
                "{}{}",
                prefix.to_string().repeat(255 - suffix.len()),
                suffix
            )
        }

        let mut topology = RabbitMqTopologyExtension::new(
            RabbitMqTopologyOwnership::FrameworkManaged,
            RabbitMqQueueType::Quorum,
            RabbitMqMainTopologyDescriptor::new(
                rabbit_name('e', 0),
                RabbitMqExchangeKind::Direct,
                rabbit_name('q', 0),
                rabbit_name('r', 0),
            ),
            rabbit_name('x', 0),
            RabbitMqDeadLetterDescriptor::new(
                rabbit_name('d', 0),
                rabbit_name('l', 0),
                rabbit_name('k', 0),
            ),
        );
        for index in 1..=limits::RABBITMQ_RETRY_BUCKET_COUNT {
            topology = topology.retry_bucket(RabbitMqRetryBucketDescriptor::new(
                rabbit_name('b', index),
                rabbit_name('t', index),
                u64::try_from(index).expect("test index"),
            ));
        }
        for index in 1..=limits::RABBITMQ_RETRY_ATTEMPT_COUNT {
            let delay_ms = if index <= limits::RABBITMQ_RETRY_BUCKET_COUNT {
                vec![u64::try_from(index).expect("test index")]
            } else {
                vec![1, 2, 3]
            };
            topology = topology.retry_attempt(RabbitMqRetryAttemptDescriptor::new(
                u32::try_from(index).expect("test index"),
                delay_ms,
            ));
        }

        let value = topology.value().expect("maximum accepted topology");
        let bytes = serde_json::to_vec(&value).expect("topology JSON");
        assert!(bytes.len() > limits::LILY_EXTENSION_BYTES);
        assert!(bytes.len() <= limits::RABBITMQ_TOPOLOGY_EXTENSION_BYTES);
    }

    #[test]
    fn websocket_outcome_requires_consistent_companion_descriptors() {
        let missing_emit = LilyExtension::WebSocketOutcome(WebSocketOutcomeExtension::new(vec![
            WebSocketOutcomeKind::Emit,
        ]));
        assert!(
            build_extensions(
                TransportKind::WebSocket,
                ExtensionScope::Operation,
                "orders.receive",
                vec![missing_emit],
            )
            .is_err()
        );

        let invalid_code = LilyExtension::WebSocketOutcome(
            WebSocketOutcomeExtension::new(vec![WebSocketOutcomeKind::Error])
                .error(WebSocketErrorDescriptor::new("bad-code", "safe")),
        );
        assert!(
            build_extensions(
                TransportKind::WebSocket,
                ExtensionScope::Operation,
                "orders.receive",
                vec![invalid_code],
            )
            .is_err()
        );

        let generic_error = LilyExtension::WebSocketOutcome(WebSocketOutcomeExtension::new(vec![
            WebSocketOutcomeKind::Error,
        ]));
        assert!(
            build_extensions(
                TransportKind::WebSocket,
                ExtensionScope::Operation,
                "orders.receive",
                vec![generic_error],
            )
            .is_ok()
        );
    }

    #[test]
    fn semantic_extension_sets_are_stable_across_registration_order() {
        let outcome = |reverse: bool| {
            let (outcomes, errors) = if reverse {
                (
                    vec![
                        WebSocketOutcomeKind::Close,
                        WebSocketOutcomeKind::Error,
                        WebSocketOutcomeKind::NoReply,
                    ],
                    vec![
                        WebSocketErrorDescriptor::new("Z_ERROR", "z"),
                        WebSocketErrorDescriptor::new("A_ERROR", "a"),
                    ],
                )
            } else {
                (
                    vec![
                        WebSocketOutcomeKind::NoReply,
                        WebSocketOutcomeKind::Error,
                        WebSocketOutcomeKind::Close,
                    ],
                    vec![
                        WebSocketErrorDescriptor::new("A_ERROR", "a"),
                        WebSocketErrorDescriptor::new("Z_ERROR", "z"),
                    ],
                )
            };
            LilyExtension::WebSocketOutcome(
                errors
                    .into_iter()
                    .fold(WebSocketOutcomeExtension::new(outcomes), |value, error| {
                        value.error(error)
                    })
                    .close(WebSocketCloseDescriptor::lily_application()),
            )
        };
        let first = build_extensions(
            TransportKind::WebSocket,
            ExtensionScope::Operation,
            "orders.receive",
            vec![outcome(false)],
        )
        .expect("first outcome");
        let second = build_extensions(
            TransportKind::WebSocket,
            ExtensionScope::Operation,
            "orders.receive",
            vec![outcome(true)],
        )
        .expect("second outcome");
        assert_eq!(first, second);

        let topology = |reverse: bool| {
            let first = RabbitMqRetryBucketDescriptor::new("retry.1", "retry.1", 1_000);
            let second = RabbitMqRetryBucketDescriptor::new("retry.2", "retry.2", 2_000);
            let topology = RabbitMqTopologyExtension::new(
                RabbitMqTopologyOwnership::FrameworkManaged,
                RabbitMqQueueType::Quorum,
                RabbitMqMainTopologyDescriptor::new(
                    "orders",
                    RabbitMqExchangeKind::Direct,
                    "orders",
                    "orders.created",
                ),
                "orders.retry.v1",
                RabbitMqDeadLetterDescriptor::new("orders.dlx", "orders.dlq", "orders.dead"),
            );
            let topology = if reverse {
                topology.retry_bucket(second).retry_bucket(first)
            } else {
                topology.retry_bucket(first).retry_bucket(second)
            };
            let first_route = RabbitMqRetryAttemptDescriptor::new(1, vec![1_000]);
            let second_route = RabbitMqRetryAttemptDescriptor::new(
                2,
                if reverse {
                    vec![2_000, 1_000]
                } else {
                    vec![1_000, 2_000]
                },
            );
            let topology = if reverse {
                topology
                    .retry_attempt(second_route)
                    .retry_attempt(first_route)
            } else {
                topology
                    .retry_attempt(first_route)
                    .retry_attempt(second_route)
            };
            LilyExtension::RabbitMqTopology(topology)
        };
        let first = build_extensions(
            TransportKind::Amqp,
            ExtensionScope::Channel,
            "orders.queue",
            vec![topology(false)],
        )
        .expect("first topology");
        let second = build_extensions(
            TransportKind::Amqp,
            ExtensionScope::Channel,
            "orders.queue",
            vec![topology(true)],
        )
        .expect("second topology");
        assert_eq!(first, second);
    }
}
