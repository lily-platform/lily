//! Typed WebSocket action outcomes and bounded application failures.

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use serde::Serialize;

use crate::codec::DecodedWebSocketPayload;
use crate::request::{MAX_CANONICAL_EVENT_BYTES, MAX_CANONICAL_NAMESPACE_BYTES};

/// Maximum bytes retained for a stable WebSocket application error code.
pub const MAX_WEBSOCKET_ERROR_CODE_BYTES: usize = 64;
/// Maximum bytes retained for a safe public WebSocket error message.
pub const MAX_WEBSOCKET_PUBLIC_ERROR_BYTES: usize = 1024;
/// RFC 6455 control-frame payload bound minus the two-byte close code.
pub const MAX_WEBSOCKET_CLOSE_REASON_BYTES: usize = 123;

/// Validated telemetry-safe application error identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WebSocketErrorCode(&'static str);

impl WebSocketErrorCode {
    /// Framework-owned payload extraction failure.
    pub const EXTRACTION_FAILED: Self = Self("WS_EXTRACTION_FAILED");
    /// Framework-owned action serialization failure.
    pub const OUTPUT_FAILED: Self = Self("WS_OUTPUT_FAILED");
    /// Framework-owned missing acknowledgement authority.
    pub const MISSING_ACK_AUTHORITY: Self = Self("WS_MISSING_ACK_AUTHORITY");
    /// Framework-owned internal action failure.
    pub const INTERNAL: Self = Self("WS_ACTION_INTERNAL");

    /// Validates a static application error code.
    pub fn new(code: &'static str) -> Result<Self, WebSocketOutcomeConfigError> {
        if code.is_empty() {
            return Err(WebSocketOutcomeConfigError::InvalidErrorCode);
        }
        if code.len() > MAX_WEBSOCKET_ERROR_CODE_BYTES {
            return Err(WebSocketOutcomeConfigError::ErrorCodeTooLong);
        }
        if !code.as_bytes()[0].is_ascii_uppercase()
            || !code
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(WebSocketOutcomeConfigError::InvalidErrorCode);
        }
        Ok(Self(code))
    }

    /// Validated static code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for WebSocketErrorCode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

/// Construction error for a typed action result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum WebSocketOutcomeConfigError {
    /// Error code does not use bounded uppercase ASCII grammar.
    #[error("WebSocket error code is invalid")]
    InvalidErrorCode,
    /// Error code exceeds its stable telemetry bound.
    #[error("WebSocket error code is too long")]
    ErrorCodeTooLong,
    /// Safe public message is unbounded or contains a forbidden control byte.
    #[error("WebSocket public error message is invalid")]
    InvalidPublicMessage,
    /// Close code is reserved or outside an application-usable range.
    #[error("WebSocket close code is invalid")]
    InvalidCloseCode,
    /// Close reason exceeds the RFC 6455 control-frame bound.
    #[error("WebSocket close reason is invalid")]
    InvalidCloseReason,
    /// Emit route is not one exact canonical `namespace:event` pair.
    #[error("WebSocket emit route is invalid")]
    InvalidEmitRoute,
}

/// Validated close decision returned by an action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloseConnection {
    code: u16,
    reason: String,
}

impl CloseConnection {
    /// Creates an RFC-compatible bounded close decision.
    pub fn try_new(
        code: u16,
        reason: impl Into<String>,
    ) -> Result<Self, WebSocketOutcomeConfigError> {
        if !is_application_close_code(code) {
            return Err(WebSocketOutcomeConfigError::InvalidCloseCode);
        }
        let reason = reason.into();
        if reason.len() > MAX_WEBSOCKET_CLOSE_REASON_BYTES
            || reason
                .bytes()
                .any(|byte| byte.is_ascii_control() && !matches!(byte, b'\t'))
        {
            return Err(WebSocketOutcomeConfigError::InvalidCloseReason);
        }
        Ok(Self { code, reason })
    }

    /// Creates a normal close (`1000`).
    pub fn normal(reason: impl Into<String>) -> Result<Self, WebSocketOutcomeConfigError> {
        Self::try_new(1000, reason)
    }

    /// Creates a policy close (`1008`).
    pub fn policy(reason: impl Into<String>) -> Result<Self, WebSocketOutcomeConfigError> {
        Self::try_new(1008, reason)
    }

    /// Wire close code.
    #[must_use]
    pub const fn code(&self) -> u16 {
        self.code
    }

    /// Bounded public close reason.
    #[must_use]
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

/// Terminal behavior selected by an application action error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WebSocketActionErrorDisposition {
    /// Encode one safe error frame and keep the connection open.
    KeepOpen,
    /// Emit only the supplied bounded close decision.
    Close(CloseConnection),
}

/// Source-preserving but wire-safe action failure.
#[derive(Clone)]
pub struct WebSocketActionError {
    code: WebSocketErrorCode,
    public_message: Arc<str>,
    disposition: WebSocketActionErrorDisposition,
    source: Option<Arc<dyn Error + Send + Sync>>,
}

impl WebSocketActionError {
    /// Creates an intentional safe application rejection.
    pub fn rejected(
        code: WebSocketErrorCode,
        public_message: impl Into<String>,
    ) -> Result<Self, WebSocketOutcomeConfigError> {
        Self::try_new(
            code,
            public_message,
            WebSocketActionErrorDisposition::KeepOpen,
        )
    }

    /// Creates an intentional safe application close.
    pub fn closing(
        code: WebSocketErrorCode,
        public_message: impl Into<String>,
        close: CloseConnection,
    ) -> Result<Self, WebSocketOutcomeConfigError> {
        Self::try_new(
            code,
            public_message,
            WebSocketActionErrorDisposition::Close(close),
        )
    }

    /// Creates a bounded failure with an explicit terminal disposition.
    pub fn try_new(
        code: WebSocketErrorCode,
        public_message: impl Into<String>,
        disposition: WebSocketActionErrorDisposition,
    ) -> Result<Self, WebSocketOutcomeConfigError> {
        let public_message = public_message.into();
        validate_public_message(&public_message)?;
        Ok(Self {
            code,
            public_message: Arc::from(public_message),
            disposition,
            source: None,
        })
    }

    /// Redacts an arbitrary implementation failure while retaining its source.
    pub fn internal<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            code: WebSocketErrorCode::INTERNAL,
            public_message: Arc::from("WebSocket action failed."),
            disposition: WebSocketActionErrorDisposition::KeepOpen,
            source: Some(Arc::new(source)),
        }
    }

    /// Creates the canonical output serialization failure.
    pub fn output<E>(source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            code: WebSocketErrorCode::OUTPUT_FAILED,
            public_message: Arc::from("WebSocket action output could not be encoded."),
            disposition: WebSocketActionErrorDisposition::KeepOpen,
            source: Some(Arc::new(source)),
        }
    }

    /// Stable wire and telemetry code.
    #[must_use]
    pub const fn code(&self) -> WebSocketErrorCode {
        self.code
    }

    /// Bounded user-safe message.
    #[must_use]
    pub fn public_message(&self) -> &str {
        &self.public_message
    }

    /// Terminal connection decision.
    #[must_use]
    pub const fn disposition(&self) -> &WebSocketActionErrorDisposition {
        &self.disposition
    }
}

impl fmt::Debug for WebSocketActionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebSocketActionError")
            .field("code", &self.code)
            .field("public_message_bytes", &self.public_message.len())
            .field("disposition", &self.disposition)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

impl fmt::Display for WebSocketActionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "WebSocket action failed with code {}", self.code)
    }
}

impl Error for WebSocketActionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

impl From<WebSocketOutcomeConfigError> for WebSocketActionError {
    fn from(error: WebSocketOutcomeConfigError) -> Self {
        Self::output(error)
    }
}

/// Successful action completion without an application frame.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoReply;

type PayloadEncoder<T> = fn(T) -> Result<DecodedWebSocketPayload, WebSocketActionError>;

/// Typed acknowledgement correlated to the inbound message's `ack_id`.
pub struct Ack<T> {
    value: T,
    encode: PayloadEncoder<T>,
}

impl<T> Ack<T>
where
    T: Serialize,
{
    /// Creates a JSON acknowledgement payload.
    #[must_use]
    pub fn new(value: T) -> Self {
        Self {
            value,
            encode: encode_json,
        }
    }
}

impl Ack<String> {
    /// Creates a UTF-8 text acknowledgement payload.
    #[must_use]
    pub fn text(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            encode: |value| Ok(DecodedWebSocketPayload::Text(value)),
        }
    }
}

impl Ack<Vec<u8>> {
    /// Creates a binary acknowledgement payload.
    #[must_use]
    pub fn binary(value: impl Into<Vec<u8>>) -> Self {
        Self {
            value: value.into(),
            encode: |value| Ok(DecodedWebSocketPayload::Binary(value)),
        }
    }

    /// Creates a codec-specific raw acknowledgement payload.
    #[must_use]
    pub fn raw(value: impl Into<Vec<u8>>) -> Self {
        Self {
            value: value.into(),
            encode: |value| Ok(DecodedWebSocketPayload::Raw(value)),
        }
    }
}

impl<T> fmt::Debug for Ack<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Ack")
            .field("payload_type", &std::any::type_name::<T>())
            .finish_non_exhaustive()
    }
}

/// Typed event emitted to the current connection.
pub struct Emit<T> {
    event: String,
    value: T,
    encode: PayloadEncoder<T>,
}

impl<T> Emit<T>
where
    T: Serialize,
{
    /// Creates a JSON event for one canonical `namespace:event` route.
    pub fn new(event: impl Into<String>, value: T) -> Result<Self, WebSocketOutcomeConfigError> {
        let event = validate_emit_route(event.into())?;
        Ok(Self {
            event,
            value,
            encode: encode_json,
        })
    }
}

impl Emit<String> {
    /// Creates a UTF-8 text event.
    pub fn text(
        event: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<Self, WebSocketOutcomeConfigError> {
        Ok(Self {
            event: validate_emit_route(event.into())?,
            value: value.into(),
            encode: |value| Ok(DecodedWebSocketPayload::Text(value)),
        })
    }
}

impl Emit<Vec<u8>> {
    /// Creates a binary event.
    pub fn binary(
        event: impl Into<String>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Self, WebSocketOutcomeConfigError> {
        Ok(Self {
            event: validate_emit_route(event.into())?,
            value: value.into(),
            encode: |value| Ok(DecodedWebSocketPayload::Binary(value)),
        })
    }

    /// Creates a codec-specific raw event.
    pub fn raw(
        event: impl Into<String>,
        value: impl Into<Vec<u8>>,
    ) -> Result<Self, WebSocketOutcomeConfigError> {
        Ok(Self {
            event: validate_emit_route(event.into())?,
            value: value.into(),
            encode: |value| Ok(DecodedWebSocketPayload::Raw(value)),
        })
    }
}

impl<T> fmt::Debug for Emit<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Emit")
            .field("event", &self.event)
            .field("payload_type", &std::any::type_name::<T>())
            .finish_non_exhaustive()
    }
}

/// Runtime-ready action result prior to payload/frame codec materialization.
#[derive(Debug)]
#[doc(hidden)]
pub enum PendingWebSocketActionOutcome {
    /// Complete without an application frame.
    NoReply,
    /// Correlated acknowledgement payload.
    Ack(DecodedWebSocketPayload),
    /// Explicit event route and payload.
    Emit {
        /// Canonical `namespace:event` route.
        event: String,
        /// Normalized application payload.
        payload: DecodedWebSocketPayload,
    },
    /// Bounded close decision.
    Close(CloseConnection),
}

mod sealed {
    pub trait Sealed {}
}

/// Sealed conversion implemented only by Lily's canonical action outcomes.
pub trait IntoWebSocketActionOutcome: sealed::Sealed {
    /// Converts the typed return into a runtime-ready terminal result.
    #[doc(hidden)]
    fn into_pending(self) -> Result<PendingWebSocketActionOutcome, WebSocketActionError>;
}

impl sealed::Sealed for NoReply {}
impl IntoWebSocketActionOutcome for NoReply {
    fn into_pending(self) -> Result<PendingWebSocketActionOutcome, WebSocketActionError> {
        Ok(PendingWebSocketActionOutcome::NoReply)
    }
}

impl<T> sealed::Sealed for Ack<T> {}
impl<T> IntoWebSocketActionOutcome for Ack<T> {
    fn into_pending(self) -> Result<PendingWebSocketActionOutcome, WebSocketActionError> {
        (self.encode)(self.value).map(PendingWebSocketActionOutcome::Ack)
    }
}

impl<T> sealed::Sealed for Emit<T> {}
impl<T> IntoWebSocketActionOutcome for Emit<T> {
    fn into_pending(self) -> Result<PendingWebSocketActionOutcome, WebSocketActionError> {
        let payload = (self.encode)(self.value)?;
        Ok(PendingWebSocketActionOutcome::Emit {
            event: self.event,
            payload,
        })
    }
}

impl sealed::Sealed for CloseConnection {}
impl IntoWebSocketActionOutcome for CloseConnection {
    fn into_pending(self) -> Result<PendingWebSocketActionOutcome, WebSocketActionError> {
        Ok(PendingWebSocketActionOutcome::Close(self))
    }
}

impl<O> sealed::Sealed for Result<O, WebSocketActionError> where O: IntoWebSocketActionOutcome {}
impl<O> IntoWebSocketActionOutcome for Result<O, WebSocketActionError>
where
    O: IntoWebSocketActionOutcome,
{
    fn into_pending(self) -> Result<PendingWebSocketActionOutcome, WebSocketActionError> {
        self?.into_pending()
    }
}

/// Hidden monomorphized conversion used by generated controller adapters.
#[doc(hidden)]
pub fn into_websocket_action_outcome<O>(
    outcome: O,
) -> Result<PendingWebSocketActionOutcome, WebSocketActionError>
where
    O: IntoWebSocketActionOutcome,
{
    outcome.into_pending()
}

fn encode_json<T>(value: T) -> Result<DecodedWebSocketPayload, WebSocketActionError>
where
    T: Serialize,
{
    serde_json::to_value(value)
        .map(DecodedWebSocketPayload::Json)
        .map_err(WebSocketActionError::output)
}

fn validate_public_message(value: &str) -> Result<(), WebSocketOutcomeConfigError> {
    if value.is_empty()
        || value.len() > MAX_WEBSOCKET_PUBLIC_ERROR_BYTES
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(WebSocketOutcomeConfigError::InvalidPublicMessage);
    }
    Ok(())
}

fn validate_emit_route(value: String) -> Result<String, WebSocketOutcomeConfigError> {
    if value.len() > MAX_CANONICAL_EVENT_BYTES {
        return Err(WebSocketOutcomeConfigError::InvalidEmitRoute);
    }
    let mut tokens = value.split(':');
    let namespace = tokens.next().unwrap_or_default();
    let event = tokens.next().unwrap_or_default();
    if tokens.next().is_some()
        || !valid_route_token(namespace, MAX_CANONICAL_NAMESPACE_BYTES)
        || !valid_route_token(event, MAX_CANONICAL_EVENT_BYTES)
    {
        return Err(WebSocketOutcomeConfigError::InvalidEmitRoute);
    }
    Ok(value)
}

fn valid_route_token(value: &str, maximum_bytes: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum_bytes
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn is_application_close_code(code: u16) -> bool {
    matches!(
        code,
        1000 | 1001 | 1002 | 1003 | 1007 | 1008 | 1009 | 1010 | 1011 | 1012 | 1013 | 1014
    ) || (3000..=4999).contains(&code)
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FailingSerialize;

    impl Serialize for FailingSerialize {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("LILY_OUTPUT_SECRET"))
        }
    }

    #[test]
    fn canonical_outcomes_preserve_payload_representation() {
        assert!(matches!(
            into_websocket_action_outcome(Ack::new(serde_json::json!({"ok": true}))).unwrap(),
            PendingWebSocketActionOutcome::Ack(DecodedWebSocketPayload::Json(_))
        ));
        assert!(matches!(
            into_websocket_action_outcome(Ack::text("hello")).unwrap(),
            PendingWebSocketActionOutcome::Ack(DecodedWebSocketPayload::Text(_))
        ));
        assert!(matches!(
            into_websocket_action_outcome(Emit::binary("chat:file", [1, 2, 3]).unwrap()).unwrap(),
            PendingWebSocketActionOutcome::Emit {
                payload: DecodedWebSocketPayload::Binary(_),
                ..
            }
        ));
        assert!(matches!(
            into_websocket_action_outcome(NoReply).unwrap(),
            PendingWebSocketActionOutcome::NoReply
        ));
    }

    #[test]
    fn errors_and_close_values_are_bounded_and_redacted() {
        assert!(WebSocketErrorCode::new("APPLICATION_DENIED").is_ok());
        assert!(WebSocketErrorCode::new("application.denied").is_err());
        assert!(CloseConnection::try_new(1005, "reserved").is_err());
        assert!(CloseConnection::normal("x".repeat(124)).is_err());

        let secret = "LILY_ACTION_SECRET";
        let error = WebSocketActionError::internal(std::io::Error::other(secret));
        assert!(!format!("{error:?}").contains(secret));
        assert!(!error.to_string().contains(secret));
        assert!(error.source().is_some());
    }

    #[test]
    fn emit_rejects_noncanonical_routes() {
        for route in ["send", ":send", "chat:", "chat:admin:send", "chat send"] {
            assert!(Emit::new(route, 1_u8).is_err());
        }
    }

    #[test]
    fn serialization_failure_is_typed_source_preserving_and_wire_safe() {
        let error = into_websocket_action_outcome(Ack::new(FailingSerialize)).unwrap_err();

        assert_eq!(error.code(), WebSocketErrorCode::OUTPUT_FAILED);
        assert!(error.source().is_some());
        assert!(!format!("{error:?}").contains("LILY_OUTPUT_SECRET"));
        assert!(!error.to_string().contains("LILY_OUTPUT_SECRET"));
    }
}
