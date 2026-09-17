use std::error::Error;
use std::fmt;
use std::future::{Future, ready};
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

use lily_web_core::Principal;
use uuid::Uuid;

use crate::codec::{WebSocketFrameKind, WebSocketMessageHeaders};
use crate::controller::WebSocketContext;
use crate::outcome::{WebSocketActionError, WebSocketErrorCode};

use super::{
    ExecutionCancellation, FromWebSocketMessageParts, OptionalFromWebSocketMessageParts,
    WebSocketMessageInvocation,
};

/// Stable identifier of the current WebSocket connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub Uuid);

/// Exact controller namespace selected at the Upgrade boundary.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Namespace(pub String);

impl Namespace {
    /// Returns the owned namespace.
    #[must_use]
    pub fn into_inner(self) -> String {
        self.0
    }
}

/// Controller-local routed event name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventName(pub String);

impl EventName {
    /// Returns the owned event name.
    #[must_use]
    pub fn into_inner(self) -> String {
        self.0
    }
}

/// Cheaply cloned bounded application metadata view.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MessageHeaders(pub WebSocketMessageHeaders);

impl MessageHeaders {
    /// Returns the shared bounded metadata map.
    #[must_use]
    pub fn into_inner(self) -> WebSocketMessageHeaders {
        self.0
    }
}

impl Deref for MessageHeaders {
    type Target = WebSocketMessageHeaders;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Owned clone of connection-lifetime typed state.
///
/// Store and extract `Arc<T>` when cloning the underlying value would be
/// expensive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConnectionLocal<T>(pub T)
where
    T: Clone + Send + Sync + 'static;

impl<T> ConnectionLocal<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Returns the owned connection-local value.
    #[must_use]
    pub fn into_inner(self) -> T {
        self.0
    }
}

/// Owned clone of state scoped to one message dispatch.
///
/// Store and extract `Arc<T>` when cloning the underlying value would be
/// expensive.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageLocal<T>(pub T)
where
    T: Clone + Send + Sync + 'static;

impl<T> MessageLocal<T>
where
    T: Clone + Send + Sync + 'static,
{
    /// Returns the owned message-local value.
    #[must_use]
    pub fn into_inner(self) -> T {
        self.0
    }
}

/// Absolute deadline of the complete normal message pipeline.
///
/// Message middleware, guards, extraction, the action and normal reverse exits
/// share this value, fixed before the first message middleware runs. For a
/// connected/disconnected extractor it describes that lifecycle invocation's
/// separate deadline instead.
///
/// Shutdown may shorten the owner's budget after extraction. This value does
/// not extend execution or cleanup authority; observe the phase's cancellation
/// signal as well. The owner enforces its current root limit independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageDeadline(pub tokio::time::Instant);

impl MessageDeadline {
    /// Returns the absolute Tokio deadline.
    #[must_use]
    pub const fn instant(self) -> tokio::time::Instant {
        self.0
    }

    /// Remaining budget, saturating at zero.
    #[must_use]
    pub fn remaining(self) -> Duration {
        self.0
            .saturating_duration_since(tokio::time::Instant::now())
    }
}

/// Read-only body-free escape hatch for uncommon action context needs.
#[derive(Clone)]
pub struct WebSocketMessageContext {
    context: Arc<WebSocketContext>,
    namespace: String,
    event: String,
    headers: WebSocketMessageHeaders,
    principal: Option<Principal>,
    cancellation: ExecutionCancellation,
    deadline: tokio::time::Instant,
    message_id: Option<String>,
    ack_id: Option<String>,
    room: Option<String>,
    timestamp_millis: i64,
    frame_kind: WebSocketFrameKind,
}

impl WebSocketMessageContext {
    /// Shared connection context.
    #[must_use]
    pub fn connection(&self) -> &WebSocketContext {
        &self.context
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

    /// Bounded message metadata.
    #[must_use]
    pub const fn headers(&self) -> &WebSocketMessageHeaders {
        &self.headers
    }

    /// Verified principal frozen when this message entered dispatch.
    #[must_use]
    pub const fn principal(&self) -> Option<&Principal> {
        self.principal.as_ref()
    }

    /// Read-only execution signal shared with this dispatch's token extractor.
    #[must_use]
    pub const fn cancellation(&self) -> &ExecutionCancellation {
        &self.cancellation
    }

    /// Absolute dispatch deadline.
    #[must_use]
    pub const fn deadline(&self) -> tokio::time::Instant {
        self.deadline
    }

    /// Optional application message identity.
    #[must_use]
    pub fn message_id(&self) -> Option<&str> {
        self.message_id.as_deref()
    }

    /// Optional acknowledgement authority.
    #[must_use]
    pub fn ack_id(&self) -> Option<&str> {
        self.ack_id.as_deref()
    }

    /// Optional public-room target declared by the decoded envelope.
    #[must_use]
    pub fn room(&self) -> Option<&str> {
        self.room.as_deref()
    }

    /// Non-negative sender timestamp from the decoded envelope.
    #[must_use]
    pub const fn timestamp_millis(&self) -> i64 {
        self.timestamp_millis
    }

    /// Original transport frame kind.
    #[must_use]
    pub const fn frame_kind(&self) -> WebSocketFrameKind {
        self.frame_kind
    }
}

impl fmt::Debug for WebSocketMessageContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebSocketMessageContext")
            .field("connection_id", &self.context.connection_id())
            .field("namespace", &self.namespace)
            .field("event", &self.event)
            .field("header_count", &self.headers.len())
            .field("has_principal", &self.principal.is_some())
            .field("has_message_id", &self.message_id.is_some())
            .field("has_ack_id", &self.ack_id.is_some())
            .field("has_room", &self.room.is_some())
            .field("timestamp_millis", &self.timestamp_millis)
            .field("frame_kind", &self.frame_kind)
            .finish()
    }
}

/// Stable typed extraction failure category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum WebSocketExtractionFailureKind {
    /// No verified principal is attached to the connection.
    MissingPrincipal,
    /// Required connection-local state is absent.
    MissingConnectionLocal,
    /// Required message-local state is absent.
    MissingMessageLocal,
    /// Payload authority was already consumed.
    PayloadConsumed,
    /// Declared content kind does not match the extractor.
    PayloadKindMismatch,
    /// The selected action payload codec rejected the encoded payload.
    PayloadCodec,
    /// JSON payload cannot be deserialized into the requested type.
    InvalidJson,
    /// Dependency resolution failed.
    Service,
    /// Custom extraction failed internally.
    Internal,
}

/// Source-preserving but payload-redacting extraction error.
#[derive(Clone)]
pub struct WebSocketExtractionError {
    kind: WebSocketExtractionFailureKind,
    source: Option<Arc<dyn Error + Send + Sync>>,
}

impl WebSocketExtractionError {
    /// Creates a source-free stable extraction failure.
    #[must_use]
    pub const fn new(kind: WebSocketExtractionFailureKind) -> Self {
        Self { kind, source: None }
    }

    /// Retains an internal source without exposing it in Debug or Display.
    pub fn with_source<E>(kind: WebSocketExtractionFailureKind, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self {
            kind,
            source: Some(Arc::new(source)),
        }
    }

    /// Stable extraction failure category.
    #[must_use]
    pub const fn kind(&self) -> WebSocketExtractionFailureKind {
        self.kind
    }

    pub(super) const fn missing_principal() -> Self {
        Self::new(WebSocketExtractionFailureKind::MissingPrincipal)
    }

    pub(super) const fn missing_connection_local() -> Self {
        Self::new(WebSocketExtractionFailureKind::MissingConnectionLocal)
    }

    pub(super) const fn missing_message_local() -> Self {
        Self::new(WebSocketExtractionFailureKind::MissingMessageLocal)
    }

    pub(super) const fn payload_consumed() -> Self {
        Self::new(WebSocketExtractionFailureKind::PayloadConsumed)
    }

    pub(super) const fn payload_kind_mismatch() -> Self {
        Self::new(WebSocketExtractionFailureKind::PayloadKindMismatch)
    }

    pub(super) fn invalid_json(source: serde_json::Error) -> Self {
        Self::with_source(WebSocketExtractionFailureKind::InvalidJson, source)
    }

    pub(super) fn payload_codec(source: crate::codec::WebSocketCodecError) -> Self {
        Self::with_source(WebSocketExtractionFailureKind::PayloadCodec, source)
    }
}

impl fmt::Debug for WebSocketExtractionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebSocketExtractionError")
            .field("kind", &self.kind)
            .field("has_source", &self.source.is_some())
            .finish()
    }
}

impl fmt::Display for WebSocketExtractionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "WebSocket extraction {:?} failure", self.kind)
    }
}

impl Error for WebSocketExtractionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn Error + 'static))
    }
}

impl From<WebSocketExtractionError> for WebSocketActionError {
    fn from(error: WebSocketExtractionError) -> Self {
        let message = match error.kind {
            WebSocketExtractionFailureKind::MissingPrincipal => {
                "A verified WebSocket principal is required."
            }
            WebSocketExtractionFailureKind::MissingConnectionLocal
            | WebSocketExtractionFailureKind::MissingMessageLocal => {
                "Required WebSocket context is unavailable."
            }
            WebSocketExtractionFailureKind::PayloadConsumed
            | WebSocketExtractionFailureKind::PayloadKindMismatch
            | WebSocketExtractionFailureKind::PayloadCodec
            | WebSocketExtractionFailureKind::InvalidJson => {
                "WebSocket message payload is invalid."
            }
            WebSocketExtractionFailureKind::Service | WebSocketExtractionFailureKind::Internal => {
                return WebSocketActionError::internal(error);
            }
        };
        WebSocketActionError::rejected(WebSocketErrorCode::EXTRACTION_FAILED, message)
            .unwrap_or_else(WebSocketActionError::output)
    }
}

impl FromWebSocketMessageParts for Arc<WebSocketContext> {
    type Rejection = WebSocketExtractionError;

    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(Arc::clone(invocation.context())))
    }
}

impl FromWebSocketMessageParts for WebSocketContext {
    type Rejection = WebSocketExtractionError;

    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(invocation.context().as_ref().clone()))
    }
}

impl FromWebSocketMessageParts for ConnectionId {
    type Rejection = WebSocketExtractionError;

    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(Self(invocation.context().connection_id())))
    }
}

impl FromWebSocketMessageParts for Namespace {
    type Rejection = WebSocketExtractionError;

    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(Self(invocation.namespace().to_owned())))
    }
}

impl FromWebSocketMessageParts for EventName {
    type Rejection = WebSocketExtractionError;

    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(Self(invocation.event().to_owned())))
    }
}

impl FromWebSocketMessageParts for MessageHeaders {
    type Rejection = WebSocketExtractionError;

    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(Self(invocation.headers().clone())))
    }
}

impl FromWebSocketMessageParts for Principal {
    type Rejection = WebSocketExtractionError;

    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(
            invocation
                .principal()
                .cloned()
                .ok_or_else(WebSocketExtractionError::missing_principal),
        )
    }
}

impl OptionalFromWebSocketMessageParts for Principal {
    type Rejection = WebSocketExtractionError;

    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Option<Self>, Self::Rejection>> + Send {
        ready(Ok(invocation.principal().cloned()))
    }
}

impl<T> FromWebSocketMessageParts for ConnectionLocal<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Rejection = WebSocketExtractionError;

    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(
            invocation
                .context()
                .connection_locals()
                .get::<T>()
                .cloned()
                .map(Self)
                .ok_or_else(WebSocketExtractionError::missing_connection_local),
        )
    }
}

impl<T> OptionalFromWebSocketMessageParts for ConnectionLocal<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Rejection = WebSocketExtractionError;

    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Option<Self>, Self::Rejection>> + Send {
        ready(Ok(invocation
            .context()
            .connection_locals()
            .get::<T>()
            .cloned()
            .map(Self)))
    }
}

impl<T> FromWebSocketMessageParts for MessageLocal<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Rejection = WebSocketExtractionError;

    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(
            invocation
                .message_local::<T>()
                .map(Self)
                .ok_or_else(WebSocketExtractionError::missing_message_local),
        )
    }
}

impl<T> OptionalFromWebSocketMessageParts for MessageLocal<T>
where
    T: Clone + Send + Sync + 'static,
{
    type Rejection = WebSocketExtractionError;

    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Option<Self>, Self::Rejection>> + Send {
        ready(Ok(invocation.message_local::<T>().map(Self)))
    }
}

impl FromWebSocketMessageParts for ExecutionCancellation {
    type Rejection = WebSocketExtractionError;

    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(invocation.cancellation().clone()))
    }
}

impl FromWebSocketMessageParts for MessageDeadline {
    type Rejection = WebSocketExtractionError;

    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(Self(invocation.deadline())))
    }
}

impl FromWebSocketMessageParts for WebSocketMessageContext {
    type Rejection = WebSocketExtractionError;

    fn from_message_parts(
        invocation: &mut WebSocketMessageInvocation,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        ready(Ok(Self {
            context: Arc::clone(invocation.context()),
            namespace: invocation.namespace().to_owned(),
            event: invocation.event().to_owned(),
            headers: invocation.headers().clone(),
            principal: invocation.principal().cloned(),
            cancellation: invocation.cancellation().clone(),
            deadline: invocation.deadline(),
            message_id: invocation.message_id().map(str::to_owned),
            ack_id: invocation.ack_id().map(str::to_owned),
            room: invocation.room().map(str::to_owned),
            timestamp_millis: invocation.timestamp_millis(),
            frame_kind: invocation.frame_kind(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadline_remaining_saturates() {
        let expired = MessageDeadline(tokio::time::Instant::now() - Duration::from_secs(1));
        assert_eq!(expired.remaining(), Duration::ZERO);
    }

    #[test]
    fn extraction_debug_redacts_sources() {
        let secret = "LILY_EXTRACTION_SECRET";
        let error = WebSocketExtractionError::with_source(
            WebSocketExtractionFailureKind::Internal,
            std::io::Error::other(secret),
        );
        assert!(!format!("{error:?}").contains(secret));
        assert!(!error.to_string().contains(secret));
        assert!(error.source().is_some());
    }
}
