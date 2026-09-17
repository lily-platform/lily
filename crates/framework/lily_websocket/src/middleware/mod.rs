use crate::{
    AuthenticatedWebSocketIdentity, CleanupCancellation, ExecutionCancellation, WebSocketContext,
    WebSocketIdentitySnapshot,
    lifecycle::{
        EnteredLifecycleLedger, LifecycleExitPath, LifecycleInterruption, LifecycleOutcome,
    },
    request::{WsCloseReason, WsHeaders, WsProtocolErrorCode, WsRequest},
    server::WsTransportSecurity,
};
use async_trait::async_trait;
use futures_util::FutureExt;
use lily_injection::{Extensions, InjectionError};
pub use lily_middleware::{
    MiddlewareConfigError, MiddlewareDescriptor, MiddlewareErrorCode, MiddlewareKind,
};
use lily_middleware::{validate_middleware_count, validate_middleware_descriptors};
use lily_web_core::{Principal, RequestConnectionInfo, RequestExtensions};
use opentelemetry::{
    KeyValue, global,
    metrics::{Counter, Histogram},
};
use std::{
    fmt,
    future::Future,
    net::SocketAddr,
    panic::AssertUnwindSafe,
    sync::{Arc, Mutex, PoisonError},
    time::{Duration, Instant},
};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::http::{
    HeaderName, HeaderValue, StatusCode, header::WWW_AUTHENTICATE,
};
use tokio_util::sync::CancellationToken;

use crate::extractor::{WebSocketMessageLocalError, WebSocketMessageLocals};

mod lifecycle;
mod message_termination;
pub(crate) use lifecycle::{
    ConnectionAccounting, ConnectionCleanupOutcome, ConnectionCleanupReceipts,
    ConnectionCleanupRegistry, ConnectionTerminalHook, ConnectionTerminalHookReport,
};
pub use message_termination::{
    WsMessageNormalExit, WsMessageTerminationContext, WsMessageTerminationReason,
};

/// Immutable, Lily-owned view of one HTTP Upgrade request.
///
/// The transport request type is deliberately not exposed. Policies receive
/// only the normalized namespace, Lily header model and peer address captured
/// at the trust boundary.
#[derive(Clone)]
pub struct WsHandshakeRequest {
    namespace: String,
    headers: WsHeaders,
    peer_addr: SocketAddr,
    connection_info: RequestConnectionInfo,
    transport_security: WsTransportSecurity,
}

impl WsHandshakeRequest {
    pub(crate) fn new(
        namespace: String,
        headers: WsHeaders,
        peer_addr: SocketAddr,
        connection_info: RequestConnectionInfo,
        transport_security: WsTransportSecurity,
    ) -> Self {
        Self {
            namespace,
            headers,
            peer_addr,
            connection_info,
            transport_security,
        }
    }

    #[must_use]
    /// Namespace selected by the `namespace` handshake query parameter.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    #[must_use]
    /// Normalized headers captured from the Upgrade request.
    pub fn headers(&self) -> &WsHeaders {
        &self.headers
    }

    #[must_use]
    /// Peer socket address captured by the listener.
    pub const fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    /// Typed peer/effective-client identity established by transport policy.
    #[must_use]
    pub const fn connection_info(&self) -> RequestConnectionInfo {
        self.connection_info
    }

    /// Effective client address after trusted-proxy processing.
    #[must_use]
    pub fn client_ip(&self) -> Option<std::net::IpAddr> {
        self.connection_info.client_ip()
    }

    /// Whether an explicitly trusted proxy supplied the effective client IP.
    #[must_use]
    pub fn via_trusted_proxy(&self) -> bool {
        self.connection_info.via_trusted_proxy()
    }

    /// Server-owned WS/WSS transport classification.
    #[must_use]
    pub const fn transport_security(&self) -> WsTransportSecurity {
        self.transport_security
    }

    /// Whether this Upgrade arrived over Lily-terminated TLS.
    #[must_use]
    pub const fn is_secure(&self) -> bool {
        self.transport_security.is_secure()
    }

    #[must_use]
    /// Requested WebSocket subprotocols in client preference order.
    pub fn requested_protocols(&self) -> &[String] {
        self.headers
            .get_protocols()
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
}

impl fmt::Debug for WsHandshakeRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WsHandshakeRequest")
            .field("namespace_bytes", &self.namespace.len())
            .field("has_origin", &self.headers.origin.is_some())
            .field(
                "requested_protocol_count",
                &self.requested_protocols().len(),
            )
            .field("peer_addr", &"[REDACTED]")
            .field("via_trusted_proxy", &self.via_trusted_proxy())
            .field("transport_security", &self.transport_security)
            .finish()
    }
}

const MAX_HANDSHAKE_REJECTION_BODY_BYTES: usize = 4 * 1024;
const MAX_HANDSHAKE_REJECTION_HEADERS: usize = 8;
const MAX_HANDSHAKE_REJECTION_HEADER_VALUE_BYTES: usize = 1024;
const MAX_HANDSHAKE_LOCAL_ENTRIES: usize = 32;
const MAX_CONNECTION_LOCAL_ENTRIES: usize = 32;

/// Validation failure while constructing a public pre-upgrade rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum WsHandshakeRejectionConfigError {
    /// Only HTTP client/server error statuses can terminate the handshake.
    #[error("WebSocket handshake rejection status must be between 400 and 599")]
    InvalidStatus,
    /// The status requires a dedicated constructor that establishes its
    /// mandatory HTTP response header.
    #[error("WebSocket handshake rejection status requires a typed header constructor")]
    RequiredHeaderMissing,
    /// Lily cannot safely construct the mandatory response headers for this
    /// HTTP status through the public handshake-rejection API.
    #[error("WebSocket handshake rejection status is not supported")]
    UnsupportedStatus,
    /// The public response body exceeded Lily's fixed admission bound.
    #[error("WebSocket handshake rejection body exceeds the public size bound")]
    BodyTooLarge,
    /// The public response body contains disallowed control bytes.
    #[error("WebSocket handshake rejection body is not safe public text")]
    InvalidBody,
    /// The response header is not in Lily's pre-upgrade allowlist.
    #[error("WebSocket handshake rejection header is not allowed")]
    HeaderNotAllowed,
    /// The rejection contains too many public response headers.
    #[error("WebSocket handshake rejection contains too many headers")]
    TooManyHeaders,
    /// A response header value exceeded Lily's fixed admission bound or was
    /// not visible HTTP text.
    #[error("WebSocket handshake rejection header value is invalid")]
    InvalidHeaderValue,
    /// The `WWW-Authenticate` value is not one valid HTTP authentication
    /// challenge.
    #[error("WebSocket authentication challenge is invalid")]
    InvalidAuthenticationChallenge,
}

/// Admission failure while publishing typed state between handshake stages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum WsHandshakeLocalError {
    /// The handshake-local map already contains Lily's maximum number of
    /// distinct concrete types.
    #[error("WebSocket handshake local-state entry limit was reached")]
    TooManyEntries,
}

/// Admission failure while constructing immutable connection identity state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum WebSocketIdentityError {
    /// The connection-local map already contains Lily's maximum number of
    /// distinct concrete types.
    #[error("WebSocket connection local-state entry limit was reached")]
    TooManyEntries,
}

/// Bounded, secret-safe HTTP response emitted before WebSocket Upgrade.
///
/// The body is public text and must never contain an internal error or raw
/// credential. Response headers are restricted to authentication, retry,
/// cookie invalidation and cache-control semantics; hop-by-hop headers remain
/// owned by Lily.
#[derive(Clone, PartialEq, Eq)]
pub struct WsHandshakeRejection {
    status: StatusCode,
    code: MiddlewareErrorCode,
    body: String,
    headers: Vec<(HeaderName, HeaderValue)>,
}

impl WsHandshakeRejection {
    /// Creates a rejection for one supported HTTP error status.
    ///
    /// HTTP `401` must be created with [`Self::unauthorized`] so a valid
    /// `WWW-Authenticate` challenge is present from construction. HTTP `405`,
    /// `407`, and `426` are rejected because their mandatory `Allow`,
    /// `Proxy-Authenticate`, and `Upgrade` headers are not owned by this API.
    pub fn try_new(
        status: u16,
        code: MiddlewareErrorCode,
    ) -> Result<Self, WsHandshakeRejectionConfigError> {
        let status = StatusCode::from_u16(status)
            .map_err(|_| WsHandshakeRejectionConfigError::InvalidStatus)?;
        if !status.is_client_error() && !status.is_server_error() {
            return Err(WsHandshakeRejectionConfigError::InvalidStatus);
        }
        match status {
            StatusCode::UNAUTHORIZED => {
                return Err(WsHandshakeRejectionConfigError::RequiredHeaderMissing);
            }
            StatusCode::METHOD_NOT_ALLOWED
            | StatusCode::PROXY_AUTHENTICATION_REQUIRED
            | StatusCode::UPGRADE_REQUIRED => {
                return Err(WsHandshakeRejectionConfigError::UnsupportedStatus);
            }
            _ => {}
        }
        Ok(Self::new_validated(status, code))
    }

    fn new_validated(status: StatusCode, code: MiddlewareErrorCode) -> Self {
        Self {
            status,
            code,
            body: default_handshake_rejection_message(status).to_owned(),
            headers: Vec::new(),
        }
    }

    #[must_use]
    /// Creates an HTTP `400 Bad Request` rejection.
    pub fn bad_request(code: MiddlewareErrorCode) -> Self {
        Self::try_new(StatusCode::BAD_REQUEST.as_u16(), code)
            .expect("400 is a valid handshake rejection status")
    }

    /// Creates an HTTP `401 Unauthorized` rejection with its mandatory
    /// `WWW-Authenticate` challenge.
    ///
    /// One challenge such as `Bearer`, `Basic realm="api"`, or
    /// `Bearer realm="api", error="invalid_token"` is accepted. Add another
    /// independently valid challenge with [`Self::try_with_header`] when a
    /// response advertises multiple authentication schemes.
    pub fn unauthorized(
        code: MiddlewareErrorCode,
        challenge: HeaderValue,
    ) -> Result<Self, WsHandshakeRejectionConfigError> {
        validate_public_header_value(&challenge)?;
        validate_authentication_challenge(&challenge)?;
        let mut rejection = Self::new_validated(StatusCode::UNAUTHORIZED, code);
        rejection.headers.push((WWW_AUTHENTICATE, challenge));
        Ok(rejection)
    }

    #[must_use]
    /// Creates an HTTP `403 Forbidden` rejection.
    pub fn forbidden(code: MiddlewareErrorCode) -> Self {
        Self::try_new(StatusCode::FORBIDDEN.as_u16(), code)
            .expect("403 is a valid handshake rejection status")
    }

    /// Replaces the default public response text.
    pub fn try_with_body(
        mut self,
        body: impl Into<String>,
    ) -> Result<Self, WsHandshakeRejectionConfigError> {
        let body = body.into();
        if body.len() > MAX_HANDSHAKE_REJECTION_BODY_BYTES {
            return Err(WsHandshakeRejectionConfigError::BodyTooLarge);
        }
        if body
            .bytes()
            .any(|byte| byte.is_ascii_control() && !matches!(byte, b'\t' | b'\n' | b'\r'))
        {
            return Err(WsHandshakeRejectionConfigError::InvalidBody);
        }
        self.body = body;
        Ok(self)
    }

    /// Adds one allowlisted public response header.
    pub fn try_with_header(
        mut self,
        name: HeaderName,
        value: HeaderValue,
    ) -> Result<Self, WsHandshakeRejectionConfigError> {
        if self.headers.len() >= MAX_HANDSHAKE_REJECTION_HEADERS {
            return Err(WsHandshakeRejectionConfigError::TooManyHeaders);
        }
        if !is_allowed_handshake_rejection_header(&name) {
            return Err(WsHandshakeRejectionConfigError::HeaderNotAllowed);
        }
        validate_public_header_value(&value)?;
        if name == WWW_AUTHENTICATE {
            validate_authentication_challenge(&value)?;
        }
        self.headers.push((name, value));
        Ok(self)
    }

    #[must_use]
    /// HTTP status emitted before Upgrade.
    pub const fn status(&self) -> StatusCode {
        self.status
    }

    #[must_use]
    /// Secret-safe diagnostic code retained by telemetry.
    pub const fn code(&self) -> MiddlewareErrorCode {
        self.code
    }

    #[must_use]
    /// Bounded public response text. This value is sent to the remote peer.
    pub fn public_body(&self) -> &str {
        &self.body
    }

    #[must_use]
    /// Allowlisted public response headers.
    pub fn headers(&self) -> &[(HeaderName, HeaderValue)] {
        &self.headers
    }
}

impl fmt::Debug for WsHandshakeRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WsHandshakeRejection")
            .field("status", &self.status)
            .field("code", &self.code)
            .field("body_bytes", &self.body.len())
            .field(
                "header_names",
                &self
                    .headers
                    .iter()
                    .map(|(name, _)| name.as_str())
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl fmt::Display for WsHandshakeRejection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "WebSocket handshake rejected with code {}",
            self.code
        )
    }
}

impl std::error::Error for WsHandshakeRejection {}

fn default_handshake_rejection_message(status: StatusCode) -> &'static str {
    match status {
        StatusCode::BAD_REQUEST => "The WebSocket upgrade request is invalid.",
        StatusCode::UNAUTHORIZED => "WebSocket authentication is required.",
        StatusCode::FORBIDDEN => "The WebSocket upgrade request is not allowed.",
        StatusCode::TOO_MANY_REQUESTS => "Too many WebSocket upgrade requests.",
        StatusCode::SERVICE_UNAVAILABLE => "WebSocket identity service is unavailable.",
        _ if status.is_client_error() => "The WebSocket upgrade request was rejected.",
        _ => "The WebSocket upgrade request could not be completed.",
    }
}

fn is_allowed_handshake_rejection_header(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "www-authenticate" | "retry-after" | "set-cookie" | "cache-control"
    )
}

fn validate_public_header_value(
    value: &HeaderValue,
) -> Result<&str, WsHandshakeRejectionConfigError> {
    let visible = value
        .to_str()
        .map_err(|_| WsHandshakeRejectionConfigError::InvalidHeaderValue)?;
    if visible.len() > MAX_HANDSHAKE_REJECTION_HEADER_VALUE_BYTES
        || visible.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(WsHandshakeRejectionConfigError::InvalidHeaderValue);
    }
    Ok(visible)
}

fn validate_authentication_challenge(
    value: &HeaderValue,
) -> Result<(), WsHandshakeRejectionConfigError> {
    let visible = value
        .to_str()
        .map_err(|_| WsHandshakeRejectionConfigError::InvalidAuthenticationChallenge)?;
    if is_authentication_challenge(visible.as_bytes()) {
        Ok(())
    } else {
        Err(WsHandshakeRejectionConfigError::InvalidAuthenticationChallenge)
    }
}

fn is_authentication_challenge(value: &[u8]) -> bool {
    let scheme_end = value
        .iter()
        .position(|byte| !is_http_token_byte(*byte))
        .unwrap_or(value.len());
    if scheme_end == 0 {
        return false;
    }
    if scheme_end == value.len() {
        return true;
    }
    if value[scheme_end] != b' ' {
        return false;
    }

    let mut data_start = scheme_end;
    while value.get(data_start) == Some(&b' ') {
        data_start += 1;
    }
    let Some(data) = value.get(data_start..) else {
        return false;
    };
    if data.is_empty() {
        return false;
    }
    is_token68(data) || is_auth_parameter_list(data)
}

fn is_token68(value: &[u8]) -> bool {
    let content_end = value
        .iter()
        .position(|byte| *byte == b'=')
        .unwrap_or(value.len());
    content_end > 0
        && value[..content_end].iter().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~' | b'+' | b'/')
        })
        && value[content_end..].iter().all(|byte| *byte == b'=')
}

fn is_auth_parameter_list(value: &[u8]) -> bool {
    let mut cursor = 0;
    loop {
        skip_optional_whitespace(value, &mut cursor);
        if !consume_http_token(value, &mut cursor) {
            return false;
        }
        skip_optional_whitespace(value, &mut cursor);
        if value.get(cursor) != Some(&b'=') {
            return false;
        }
        cursor += 1;
        skip_optional_whitespace(value, &mut cursor);
        if !consume_http_token(value, &mut cursor) && !consume_quoted_string(value, &mut cursor) {
            return false;
        }
        skip_optional_whitespace(value, &mut cursor);
        if cursor == value.len() {
            return true;
        }
        if value.get(cursor) != Some(&b',') {
            return false;
        }
        cursor += 1;
        if cursor == value.len() {
            return false;
        }
    }
}

fn skip_optional_whitespace(value: &[u8], cursor: &mut usize) {
    while value
        .get(*cursor)
        .is_some_and(|byte| matches!(byte, b' ' | b'\t'))
    {
        *cursor += 1;
    }
}

fn consume_http_token(value: &[u8], cursor: &mut usize) -> bool {
    let start = *cursor;
    while value
        .get(*cursor)
        .is_some_and(|byte| is_http_token_byte(*byte))
    {
        *cursor += 1;
    }
    *cursor > start
}

fn consume_quoted_string(value: &[u8], cursor: &mut usize) -> bool {
    if value.get(*cursor) != Some(&b'"') {
        return false;
    }
    *cursor += 1;
    while let Some(byte) = value.get(*cursor) {
        match byte {
            b'"' => {
                *cursor += 1;
                return true;
            }
            b'\\' => {
                *cursor += 1;
                if value
                    .get(*cursor)
                    .is_none_or(|escaped| escaped.is_ascii_control())
                {
                    return false;
                }
                *cursor += 1;
            }
            byte if byte.is_ascii_control() => return false,
            _ => *cursor += 1,
        }
    }
    false
}

const fn is_http_token_byte(byte: u8) -> bool {
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

/// One immutable async pre-upgrade execution context.
///
/// The context lives inside a Lily-owned DI scope. Middleware may resolve
/// scoped or transient services during the hook, but must not retain the
/// complete exchange or a scope-managed service after it returns. Derive an
/// owned immutable value before publishing connection-lifetime state.
pub struct WsHandshakeExchange {
    extensions: Arc<Extensions>,
    request: WsHandshakeRequest,
    locals: RequestExtensions,
    cancellation: ExecutionCancellation,
    deadline: tokio::time::Instant,
}

impl WsHandshakeExchange {
    pub(crate) fn with_shutdown_budget(mut self, budget: crate::shutdown::ShutdownBudget) -> Self {
        self.cancellation = self.cancellation.with_shutdown_budget(budget);
        self
    }
    pub(crate) fn new(
        extensions: Arc<Extensions>,
        request: WsHandshakeRequest,
        cancellation: CancellationToken,
        deadline: tokio::time::Instant,
    ) -> Self {
        Self {
            extensions,
            request,
            locals: RequestExtensions::new(),
            cancellation: ExecutionCancellation::new(cancellation),
            deadline,
        }
    }

    /// Normalized HTTP Upgrade request accepted by transport validation.
    #[must_use]
    pub const fn request(&self) -> &WsHandshakeRequest {
        &self.request
    }

    /// Resolves a concrete or derive-declared trait service from this
    /// handshake's DI scope.
    pub async fn service<T>(&self) -> Result<Arc<T>, InjectionError>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.extensions.get_service::<T>(None).await
    }

    /// Reads one value published by an earlier handshake middleware.
    #[must_use]
    pub fn local<T>(&self) -> Option<&T>
    where
        T: Send + Sync + 'static,
    {
        self.locals.get::<T>()
    }

    /// Publishes one typed value for later handshake middleware or identity.
    pub fn insert_local<T>(&mut self, value: T) -> Result<Option<T>, WsHandshakeLocalError>
    where
        T: Send + Sync + 'static,
    {
        if self.locals.get::<T>().is_none() && self.locals.len() >= MAX_HANDSHAKE_LOCAL_ENTRIES {
            return Err(WsHandshakeLocalError::TooManyEntries);
        }
        Ok(self.locals.insert(value))
    }

    /// Cooperative listener/shutdown cancellation for this handshake.
    #[must_use]
    pub const fn cancellation(&self) -> &ExecutionCancellation {
        &self.cancellation
    }

    /// Absolute total handshake deadline.
    #[must_use]
    pub const fn deadline(&self) -> tokio::time::Instant {
        self.deadline
    }
}

impl fmt::Debug for WsHandshakeExchange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WsHandshakeExchange")
            .field("request", &self.request)
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

/// Successful application-owned identity result.
///
/// `Principal` is optional so one implementation can support both public and
/// authenticated namespaces. Connection-local values are frozen after the
/// Upgrade and become available to middleware, guards and typed extractors.
pub struct WebSocketIdentity {
    snapshot: WebSocketIdentitySnapshot,
    connection_locals: RequestExtensions,
}

impl WebSocketIdentity {
    /// Creates an anonymous successful identity result.
    #[must_use]
    pub fn anonymous() -> Self {
        Self {
            snapshot: WebSocketIdentitySnapshot::anonymous(),
            connection_locals: RequestExtensions::new(),
        }
    }

    /// Creates a successful result carrying an application-verified bounded
    /// identity.
    #[must_use]
    pub fn authenticated(identity: AuthenticatedWebSocketIdentity) -> Self {
        Self {
            snapshot: WebSocketIdentitySnapshot::authenticated(identity),
            connection_locals: RequestExtensions::new(),
        }
    }

    /// Publishes one typed value for the accepted connection lifetime.
    pub fn insert_connection_local<T>(
        &mut self,
        value: T,
    ) -> Result<Option<T>, WebSocketIdentityError>
    where
        T: Send + Sync + 'static,
    {
        if self.connection_locals.get::<T>().is_none()
            && self.connection_locals.len() >= MAX_CONNECTION_LOCAL_ENTRIES
        {
            return Err(WebSocketIdentityError::TooManyEntries);
        }
        Ok(self.connection_locals.insert(value))
    }

    pub(crate) fn into_parts(self) -> (WebSocketIdentitySnapshot, Arc<RequestExtensions>) {
        (self.snapshot, Arc::new(self.connection_locals))
    }
}

impl Default for WebSocketIdentity {
    fn default() -> Self {
        Self::anonymous()
    }
}

impl fmt::Debug for WebSocketIdentity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WebSocketIdentity")
            .field("has_principal", &self.snapshot.principal().is_some())
            .field("has_expiry", &self.snapshot.expiry().is_some())
            .field("connection_local_count", &self.connection_locals.len())
            .finish()
    }
}

/// Async middleware executed after transport/origin validation and before
/// the optional identity slot.
#[async_trait]
pub trait WebSocketHandshakeMiddleware: Send + Sync + 'static {
    /// Constructs the one app-owned middleware instance.
    ///
    /// No Upgrade scope exists at app build. Retain only app-lived services
    /// here; resolve scoped or transient per-Upgrade services through
    /// [`WsHandshakeExchange::service`] inside [`Self::handle`].
    async fn new(extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError>
    where
        Self: Sized;

    /// Stable bounded middleware identity used for validation and telemetry.
    fn descriptor(&self) -> MiddlewareDescriptor;

    /// Validates immutable policy configuration during app build.
    fn validate(&self) -> Result<(), MiddlewareConfigError> {
        Ok(())
    }

    /// Evaluates one normalized Upgrade request inside its request DI scope.
    /// The read-only signal matches [`WsHandshakeExchange::cancellation`].
    async fn handle(
        &self,
        exchange: &mut WsHandshakeExchange,
        _cancellation: crate::ExecutionCancellation,
    ) -> Result<(), WsHandshakeRejection>;
}

/// Optional, application-owned identity verifier executed last before `101`.
///
/// Lily supplies no JWT, session, OIDC or database implementation. An app may
/// register at most one concrete identity middleware on [`crate::WsAppBuilder`].
#[async_trait]
pub trait WebSocketIdentityMiddleware: Send + Sync + 'static {
    /// Constructs the one app-owned identity middleware instance.
    ///
    /// No Upgrade scope exists at app build. Retain only app-lived services
    /// here; resolve scoped or transient per-Upgrade services through
    /// [`WsHandshakeExchange::service`] inside [`Self::identify`].
    async fn new(extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError>
    where
        Self: Sized;

    /// Stable bounded middleware identity used for validation and telemetry.
    fn descriptor(&self) -> MiddlewareDescriptor;

    /// Validates immutable middleware configuration during app build.
    fn validate(&self) -> Result<(), MiddlewareConfigError> {
        Ok(())
    }

    /// Verifies identity and produces immutable post-upgrade state.
    ///
    /// The returned state outlives the Upgrade DI scope. It must therefore
    /// contain owned values, not a scoped service whose disposal begins when
    /// this hook completes.
    /// The read-only signal matches [`WsHandshakeExchange::cancellation`].
    async fn identify(
        &self,
        exchange: &mut WsHandshakeExchange,
        _cancellation: crate::ExecutionCancellation,
    ) -> Result<WebSocketIdentity, WsHandshakeRejection>;
}

/// Stable runtime failure category for bounded diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WsMiddlewareFailureKind {
    /// Middleware intentionally rejected work.
    Rejected,
    /// Middleware exceeded its configured deadline.
    Timeout,
    /// Application shutdown cancelled the stage.
    Cancelled,
    /// Middleware failed without a safe public rejection.
    Internal,
}

/// Secret-safe middleware failure. Dynamic third-party error payloads are not
/// retained in this public representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WsMiddlewareError {
    kind: WsMiddlewareFailureKind,
    code: MiddlewareErrorCode,
}

impl WsMiddlewareError {
    #[must_use]
    /// Creates an intentional typed rejection.
    pub const fn rejected(code: MiddlewareErrorCode) -> Self {
        Self {
            kind: WsMiddlewareFailureKind::Rejected,
            code,
        }
    }

    #[must_use]
    /// Creates the canonical middleware deadline error.
    pub const fn timeout() -> Self {
        Self {
            kind: WsMiddlewareFailureKind::Timeout,
            code: MiddlewareErrorCode::TIMEOUT,
        }
    }

    #[must_use]
    /// Creates the canonical application-cancellation error.
    pub fn cancelled() -> Self {
        Self {
            kind: WsMiddlewareFailureKind::Cancelled,
            code: bounded_code("WS_MIDDLEWARE_CANCELLED"),
        }
    }

    #[must_use]
    /// Creates an internal failure with a bounded diagnostic code.
    pub const fn internal(code: MiddlewareErrorCode) -> Self {
        Self {
            kind: WsMiddlewareFailureKind::Internal,
            code,
        }
    }

    #[must_use]
    /// Stable terminal failure category.
    pub const fn kind(self) -> WsMiddlewareFailureKind {
        self.kind
    }

    #[must_use]
    /// Secret-safe diagnostic code suitable for logs and metrics.
    pub const fn diagnostic_code(self) -> MiddlewareErrorCode {
        self.code
    }
}

impl fmt::Display for WsMiddlewareError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "WebSocket middleware {:?} with code {}",
            self.kind, self.code
        )
    }
}

impl std::error::Error for WsMiddlewareError {}

/// Secret-safe failure returned while constructing one application-owned
/// WebSocket middleware instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum WsMiddlewareInitError {
    /// A required application dependency is unavailable.
    #[error("required WebSocket middleware dependency is unavailable")]
    MissingDependency,
    /// Middleware configuration is invalid.
    #[error("WebSocket middleware configuration is invalid")]
    InvalidConfiguration,
    /// A scoped dependency was requested during app-owned construction.
    #[error("WebSocket middleware construction cannot retain a scoped dependency")]
    ScopeRequired,
    /// Construction failed for another secret-safe reason.
    #[error("WebSocket middleware initialization failed internally")]
    Internal,
}

impl WsMiddlewareInitError {
    /// Redacts an arbitrary dependency failure to a stable category.
    pub fn dependency<E>(_source: E) -> Self {
        Self::MissingDependency
    }

    /// Redacts an arbitrary configuration failure to a stable category.
    pub fn invalid_configuration<E>(_source: E) -> Self {
        Self::InvalidConfiguration
    }

    /// Redacts an arbitrary implementation failure to a stable category.
    pub fn internal<E>(_source: E) -> Self {
        Self::Internal
    }

    /// Stable diagnostic code suitable for telemetry and build errors.
    #[must_use]
    pub const fn diagnostic_code(self) -> &'static str {
        match self {
            Self::MissingDependency => "WS_MIDDLEWARE_DEPENDENCY",
            Self::InvalidConfiguration => "WS_MIDDLEWARE_CONFIGURATION",
            Self::ScopeRequired => "WS_MIDDLEWARE_SCOPE_REQUIRED",
            Self::Internal => "WS_MIDDLEWARE_INITIALIZATION",
        }
    }
}

impl From<InjectionError> for WsMiddlewareInitError {
    fn from(error: InjectionError) -> Self {
        if matches!(error, InjectionError::ScopeRequired { .. }) {
            Self::ScopeRequired
        } else {
            Self::MissingDependency
        }
    }
}

/// Read-only message context shared by middleware and guards.
///
/// The exchange is created only after exact namespace/action lookup and while
/// the message DI scope is active. `ConnectionLocal` state and the verified
/// principal are read-only at this boundary. Middleware and guards may publish
/// message-local values for downstream middleware, guards and extractors.
/// [`Self::request`] permits immutable payload/raw-frame inspection; only typed
/// action extraction owns payload consumption authority.
pub struct WsMessageExchange {
    extensions: Option<Arc<Extensions>>,
    connection: Arc<WebSocketContext>,
    request: Arc<WsRequest>,
    message_locals: WebSocketMessageLocals,
    cancellation: ExecutionCancellation,
    deadline: tokio::time::Instant,
}

impl WsMessageExchange {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        extensions: Arc<Extensions>,
        connection: Arc<WebSocketContext>,
        request: Arc<WsRequest>,
        message_locals: WebSocketMessageLocals,
        cancellation: ExecutionCancellation,
        deadline: tokio::time::Instant,
    ) -> Self {
        Self {
            extensions: Some(extensions),
            connection,
            request,
            message_locals,
            cancellation,
            deadline,
        }
    }

    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn new_without_services(
        connection: Arc<WebSocketContext>,
        request: Arc<WsRequest>,
        message_locals: WebSocketMessageLocals,
        cancellation: CancellationToken,
        deadline: tokio::time::Instant,
    ) -> Self {
        let budget = connection.shutdown_budget().clone();
        Self {
            extensions: None,
            connection,
            request,
            message_locals,
            cancellation: ExecutionCancellation::with_budget(cancellation, budget),
            deadline,
        }
    }

    /// Immutable connection-scoped controller context.
    #[must_use]
    pub fn connection(&self) -> &WebSocketContext {
        &self.connection
    }

    /// Immutable decoded request view; inspecting it does not consume payload.
    #[must_use]
    pub fn request(&self) -> &WsRequest {
        &self.request
    }

    /// Current principal captured once for this message, if present.
    #[must_use]
    pub fn principal(&self) -> Option<&Principal> {
        self.request.principal()
    }

    /// Returns an owned clone of one connection-local value.
    #[must_use]
    pub fn connection_local<T>(&self) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.connection.connection_locals().get::<T>().cloned()
    }

    /// Returns an owned clone of one message-local value.
    #[must_use]
    pub fn message_local<T>(&self) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.message_locals.get_cloned::<T>()
    }

    /// Publishes one typed value for downstream message stages.
    pub fn insert_message_local<T>(
        &mut self,
        value: T,
    ) -> Result<Option<T>, WebSocketMessageLocalError>
    where
        T: Send + Sync + 'static,
    {
        self.message_locals.insert(value)
    }

    /// Removes one typed message-local value.
    pub fn remove_message_local<T>(&mut self) -> Option<T>
    where
        T: Send + Sync + 'static,
    {
        self.message_locals.remove::<T>()
    }

    /// Resolves a concrete or derive-declared trait service from the active
    /// message scope.
    pub async fn service<T>(&self) -> Result<Arc<T>, InjectionError>
    where
        T: ?Sized + Send + Sync + 'static,
    {
        match &self.extensions {
            Some(extensions) => extensions.get_service::<T>(None).await,
            None => Err(InjectionError::ServiceNotFound(
                "service resolution is unavailable in this framework test harness".to_owned(),
            )),
        }
    }

    /// Cooperative application shutdown signal for this dispatch.
    #[must_use]
    pub const fn cancellation(&self) -> &ExecutionCancellation {
        &self.cancellation
    }

    /// Absolute deadline shared by the complete normal message pipeline.
    ///
    /// It is fixed before the first message middleware and includes guards,
    /// extraction, the action, response preparation and normal reverse exit.
    /// Root shutdown can shorten execution authority without changing this
    /// snapshot. Termination cleanup has a separate deadline and authority.
    #[must_use]
    pub const fn deadline(&self) -> tokio::time::Instant {
        self.deadline
    }
}

impl fmt::Debug for WsMessageExchange {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WsMessageExchange")
            .field("connection_id", &self.connection.connection_id())
            .field("namespace", &self.connection.namespace())
            .field("event", &self.request.event())
            .field("has_principal", &self.principal().is_some())
            .field("deadline", &self.deadline)
            .finish_non_exhaustive()
    }
}

/// Bounded stage label used by the adapter's metrics and logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum WsMiddlewareStage {
    ConnectionAdmission,
    ConnectionOpened,
    ConnectionClosed,
    MessageBefore,
    MessageAfter,
    MessageTermination,
}

impl WsMiddlewareStage {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConnectionAdmission => "connection_admission",
            Self::ConnectionOpened => "connection_opened",
            Self::ConnectionClosed => "connection_closed",
            Self::MessageBefore => "message_before",
            Self::MessageAfter => "message_after",
            Self::MessageTermination => "message_termination",
        }
    }
}

/// Closed, bounded outcome set used by per-middleware telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WsMiddlewareObservationOutcome {
    Completed,
    Rejected,
    Closed,
    Timeout,
    Cancelled,
    Internal,
}

impl WsMiddlewareObservationOutcome {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Rejected => "rejected",
            Self::Closed => "closed",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::Internal => "internal",
        }
    }

    const fn from_error(error: WsMiddlewareError) -> Self {
        match error.kind() {
            WsMiddlewareFailureKind::Rejected => Self::Rejected,
            WsMiddlewareFailureKind::Timeout => Self::Timeout,
            WsMiddlewareFailureKind::Cancelled => Self::Cancelled,
            WsMiddlewareFailureKind::Internal => Self::Internal,
        }
    }

    const fn from_decision(decision: WsMessageDecision) -> Self {
        match decision {
            WsMessageDecision::Continue => Self::Completed,
            WsMessageDecision::Reject(_) => Self::Rejected,
            WsMessageDecision::Close(_) => Self::Closed,
        }
    }
}

pub(crate) trait WsMiddlewareObserver: Send + Sync {
    fn observe(
        &self,
        descriptor: MiddlewareDescriptor,
        outcome: WsMiddlewareObservationOutcome,
        duration: Duration,
    );
}

#[cfg(any(test, feature = "fuzzing"))]
struct NoopWsMiddlewareObserver;

#[cfg(any(test, feature = "fuzzing"))]
impl WsMiddlewareObserver for NoopWsMiddlewareObserver {
    fn observe(
        &self,
        _descriptor: MiddlewareDescriptor,
        _outcome: WsMiddlewareObservationOutcome,
        _duration: Duration,
    ) {
    }
}

/// Production observer shared by the three compiled WebSocket chains.
/// Attribute construction is closed over validated descriptor metadata and a
/// framework enum, so origin, peer address, namespace and payload data cannot
/// become metric labels.
pub(crate) struct OpenTelemetryWsMiddlewareObserver {
    duration: Histogram<f64>,
    outcomes: Counter<u64>,
}

impl OpenTelemetryWsMiddlewareObserver {
    pub(crate) fn new() -> Self {
        let meter = global::meter("lily_websocket");
        Self {
            duration: meter
                .f64_histogram("websocket.middleware.duration")
                .with_description("Per-middleware WebSocket execution duration")
                .with_unit("s")
                .build(),
            outcomes: meter
                .u64_counter("websocket.middleware.outcomes")
                .with_description("Per-middleware bounded WebSocket execution outcomes")
                .build(),
        }
    }
}

impl WsMiddlewareObserver for OpenTelemetryWsMiddlewareObserver {
    fn observe(
        &self,
        descriptor: MiddlewareDescriptor,
        outcome: WsMiddlewareObservationOutcome,
        duration: Duration,
    ) {
        let attributes = [
            KeyValue::new("lily.middleware.name", descriptor.name()),
            KeyValue::new("lily.outcome", outcome.as_str()),
        ];
        self.duration.record(duration.as_secs_f64(), &attributes);
        self.outcomes.add(1, &attributes);
    }
}

/// Canonical reason supplied to reverse connection cleanup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum WsConnectionCloseCategory {
    /// Peer completed a normal close handshake.
    NormalPeer,
    /// A typed application action deliberately closed the connection.
    Application,
    /// Transport reset or closed abruptly.
    Reset,
    /// A bounded socket write exceeded its deadline.
    WriteTimeout,
    /// No application message arrived before the idle deadline.
    IdleTimeout,
    /// Application shutdown terminated the connection.
    ServerShutdown,
    /// The peer violated the Lily wire protocol.
    ProtocolError,
    /// Connection or message policy rejected the peer.
    PolicyRejected,
    /// The application-provided connection identity deadline elapsed.
    IdentityExpired,
    /// Outbound application admission capacity did not recover before its deadline.
    SlowConsumer,
    /// Controller lifecycle or action code failed.
    HandlerError,
    /// Typed middleware failed.
    MiddlewareError,
    /// Application cancellation terminated the connection.
    Cancelled,
    /// An internal invariant failed.
    InternalError,
}

impl WsConnectionCloseCategory {
    #[must_use]
    /// Stable telemetry label for this close category.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NormalPeer => "normal_peer",
            Self::Application => "application",
            Self::Reset => "reset",
            Self::WriteTimeout => "write_timeout",
            Self::IdleTimeout => "idle_timeout",
            Self::ServerShutdown => "server_shutdown",
            Self::ProtocolError => "protocol_error",
            Self::PolicyRejected => "policy_rejected",
            Self::IdentityExpired => "identity_expired",
            Self::SlowConsumer => "slow_consumer",
            Self::HandlerError => "handler_error",
            Self::MiddlewareError => "middleware_error",
            Self::Cancelled => "cancelled",
            Self::InternalError => "internal_error",
        }
    }
}

/// Post-Upgrade connection admission and lifecycle middleware.
///
/// Successfully entered middleware is unwound in reverse order exactly once
/// when the connection terminates.
#[async_trait]
pub trait WsConnectionMiddleware: Send + Sync + 'static {
    /// Constructs the one app-owned middleware instance retained by every
    /// effective global/controller connection plan.
    async fn new(extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError>
    where
        Self: Sized;

    /// Stable bounded middleware identity used for validation and telemetry.
    fn descriptor(&self) -> MiddlewareDescriptor;

    /// Validates immutable middleware configuration during app build.
    fn validate(&self) -> Result<(), MiddlewareConfigError> {
        Ok(())
    }

    /// Runs after Upgrade but before the connection becomes visible.
    /// The framework retains ownership of the execution cancellation source.
    async fn admit(
        &self,
        _context: Arc<WebSocketContext>,
        _cancellation: crate::ExecutionCancellation,
    ) -> Result<(), WsMiddlewareError> {
        Ok(())
    }

    /// Runs after the connection manager publishes the connection.
    async fn opened(
        &self,
        _context: Arc<WebSocketContext>,
        _cancellation: crate::ExecutionCancellation,
    ) -> Result<(), WsMiddlewareError> {
        Ok(())
    }

    /// Runs during reverse lifecycle cleanup.
    /// The cleanup signal belongs to this invocation and is independent of
    /// execution cancellation and sibling cleanup invocations.
    async fn closed(
        &self,
        _context: Arc<WebSocketContext>,
        _category: WsConnectionCloseCategory,
        _cancellation: crate::CleanupCancellation,
    ) -> Result<(), WsMiddlewareError> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Decision returned by a pre- or post-message middleware hook.
pub enum WsMessageDecision {
    /// Continue to the next stage.
    Continue,
    /// Reject the message with a typed Lily protocol error.
    Reject(WsProtocolErrorCode),
    /// Close the connection with a bounded close reason.
    Close(WsCloseReason),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Terminal action outcome supplied to reverse message middleware unwind.
pub enum WsMessageOutcome {
    /// Action handler completed successfully.
    Handled,
    /// Guard, policy, or middleware rejected the message.
    Rejected(WsProtocolErrorCode),
    /// Processing selected a connection close.
    Close(WsCloseReason),
    /// Processing failed with a bounded middleware diagnostic code.
    Failed(MiddlewareErrorCode),
}

/// Per-message middleware with deterministic forward and reverse execution.
///
/// Effective route plans enter `global -> controller -> action`. After every
/// terminal path, Lily unwinds the successfully entered prefix in
/// `action -> controller -> global` order before publishing the terminal wire
/// outcome. Interrupted execution uses [`Self::on_message_termination`] for
/// outstanding exits. Exact route lookup happens before the first user hook.
#[async_trait]
pub trait WsMessageMiddleware: Send + Sync + 'static {
    /// Constructs the one app-owned middleware instance retained by every
    /// effective route plan.
    ///
    /// Retain only singleton DI services here. No message scope exists yet;
    /// resolve scoped and transient services from [`WsMessageExchange::service`]
    /// inside a hook and do not retain them on this application-lived instance.
    /// Directly constructed resources and raw spawned tasks are application-owned.
    async fn new(extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError>
    where
        Self: Sized;

    /// Stable bounded middleware identity used for validation and telemetry.
    fn descriptor(&self) -> MiddlewareDescriptor;

    /// Validates immutable middleware configuration during app build.
    fn validate(&self) -> Result<(), MiddlewareConfigError> {
        Ok(())
    }

    /// Runs before guards and the action handler.
    /// The read-only signal matches [`WsMessageExchange::cancellation`].
    async fn before_message(
        &self,
        _exchange: &mut WsMessageExchange,
        _cancellation: crate::ExecutionCancellation,
    ) -> Result<WsMessageDecision, WsMiddlewareError> {
        Ok(WsMessageDecision::Continue)
    }

    /// Runs in reverse order while normal message execution can continue.
    ///
    /// A Close already selected by the message transaction is terminal. Reverse
    /// hooks still observe that outcome, but a later
    /// outer Close decision cannot replace the winning close reason. Cleanup
    /// cancellation remains the explicit server-shutdown exception.
    /// The signal matches [`WsMessageExchange::cancellation`] and describes
    /// message execution. Cancellation allows bounded cooperation. If this
    /// future is interrupted, Lily observes its drop before invoking the
    /// separate termination callback; completed/failed/panicked normal exits
    /// are not invoked again as termination exits.
    async fn after_message(
        &self,
        _exchange: &mut WsMessageExchange,
        _outcome: WsMessageOutcome,
        _cancellation: crate::ExecutionCancellation,
    ) -> Result<WsMessageDecision, WsMiddlewareError> {
        Ok(WsMessageDecision::Continue)
    }

    /// Best-effort exit when framework interruption prevents normal unwind.
    ///
    /// Runs serially in reverse order, only for successfully entered middleware
    /// whose normal exit never started or was interrupted. A normal exit may
    /// have produced partial side effects; inspect `context.normal_exit()` and
    /// make termination safe for that state. This callback cannot rewrite the
    /// message response. The message DI scope stays open until unwind ends.
    ///
    /// Each invocation receives its own read-only cleanup signal, identical to
    /// `context.cancellation()`, independent of execution cancellation. Expired
    /// budgets can prevent invocation entirely; neither a first poll nor
    /// completion is guaranteed. Dropping this future must be safe.
    async fn on_message_termination(
        &self,
        _context: WsMessageTerminationContext<'_>,
        _cancellation: crate::CleanupCancellation,
    ) -> Result<(), WsMiddlewareError> {
        Ok(())
    }
}

fn bounded_code(value: &'static str) -> MiddlewareErrorCode {
    MiddlewareErrorCode::new(value).unwrap_or(MiddlewareErrorCode::INTERNAL)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WsMiddlewareExecutionError {
    descriptor: MiddlewareDescriptor,
    stage: WsMiddlewareStage,
    error: WsMiddlewareError,
}

impl WsMiddlewareExecutionError {
    fn new(
        descriptor: MiddlewareDescriptor,
        stage: WsMiddlewareStage,
        error: WsMiddlewareError,
    ) -> Self {
        Self {
            descriptor,
            stage,
            error,
        }
    }

    pub(crate) const fn descriptor(self) -> MiddlewareDescriptor {
        self.descriptor
    }

    pub(crate) const fn stage(self) -> WsMiddlewareStage {
        self.stage
    }

    pub(crate) const fn error(self) -> WsMiddlewareError {
        self.error
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BoundedHookFailure {
    error: WsMiddlewareError,
    panicked: bool,
    cancellation_observed: bool,
    interruption: Option<LifecycleInterruption>,
}

impl BoundedHookFailure {
    const fn regular(error: WsMiddlewareError) -> Self {
        Self {
            error,
            panicked: false,
            cancellation_observed: false,
            interruption: None,
        }
    }

    fn cancelled_by_token() -> Self {
        Self {
            error: WsMiddlewareError::cancelled(),
            panicked: false,
            cancellation_observed: true,
            interruption: Some(LifecycleInterruption::Cancelled),
        }
    }

    fn timed_out() -> Self {
        Self {
            error: WsMiddlewareError::timeout(),
            panicked: false,
            cancellation_observed: false,
            interruption: Some(LifecycleInterruption::TimedOut),
        }
    }

    fn panicked() -> Self {
        Self {
            error: WsMiddlewareError::internal(bounded_code("WS_MIDDLEWARE_PANICKED")),
            panicked: true,
            cancellation_observed: false,
            interruption: None,
        }
    }

    fn lifecycle_outcome(self) -> LifecycleOutcome {
        if self.panicked {
            LifecycleOutcome::Panicked
        } else if let Some(reason) = self.interruption {
            LifecycleOutcome::Interrupted(reason)
        } else {
            // An error returned by user code is a terminal return, even if
            // the user chose a timeout/cancelled diagnostic category.
            LifecycleOutcome::Failed
        }
    }
}

async fn execute_bounded_hook<T, O, F>(
    operation: O,
    stage_timeout: Duration,
    cancellation: &ExecutionCancellation,
) -> Result<T, BoundedHookFailure>
where
    O: FnOnce() -> F,
    F: Future<Output = Result<T, WsMiddlewareError>>,
{
    if cancellation.is_cancelled() {
        return Err(BoundedHookFailure::cancelled_by_token());
    }
    let execution = async {
        tokio::select! {
            biased;
            _ = cancellation.termination_requested() => {
                Err(BoundedHookFailure::cancelled_by_token())
            },
            result = timeout(stage_timeout, async { operation().await }) => match result {
                Ok(result) => result.map_err(BoundedHookFailure::regular),
                Err(_) => Err(BoundedHookFailure::timed_out()),
            },
        }
    };

    match AssertUnwindSafe(execution).catch_unwind().await {
        Ok(result) => result,
        Err(_) => Err(BoundedHookFailure::panicked()),
    }
}

/// Invocations are owned and serial. Root/owner limits can shorten a local cap
/// while the hook is pending; expired budgets do not poll a fresh user future.
async fn execute_bounded_cancellable_cleanup_hook<T, O, F>(
    operation: O,
    deadline: tokio::time::Instant,
    cancellation: &CancellationToken,
    budget: &crate::shutdown::ShutdownBudget,
) -> Result<T, BoundedHookFailure>
where
    O: FnOnce() -> F,
    F: Future<Output = Result<T, WsMiddlewareError>>,
{
    let execution = AssertUnwindSafe(async move { operation().await }).catch_unwind();
    tokio::select! {
        biased;
        () = cancellation.cancelled() => Err(BoundedHookFailure::cancelled_by_token()),
        result = budget.cleanup(Some(deadline), execution) => match result {
            Ok(Ok(result)) => result.map_err(BoundedHookFailure::regular),
            Ok(Err(_)) => Err(BoundedHookFailure::panicked()),
            Err(()) => Err(BoundedHookFailure::timed_out()),
        }
    }
}

/// Normal reverse callbacks belong to accepted execution. Keep polling during
/// cooperation, including when the signal preceded this callback, but never
/// start a fresh callback past its absolute execution boundary.
async fn execute_bounded_message_hook<T, O, F>(
    operation: O,
    deadline: tokio::time::Instant,
    cancellation: &ExecutionCancellation,
    _budget: &crate::shutdown::ShutdownBudget,
) -> Result<T, BoundedHookFailure>
where
    O: FnOnce() -> F,
    F: Future<Output = Result<T, WsMiddlewareError>>,
{
    let mut invocation = Box::pin(AssertUnwindSafe(async { operation().await }).catch_unwind());
    let result = tokio::select! {
        biased;
        () = cancellation.execution_stopped(Some(deadline)) => {
            let facts = cancellation.message_facts();
            if facts.and_then(|facts| facts.request)
                .is_some_and(|request| request.cause == crate::extractor::MessageCancellationCause::MessageTimeout)
                || (facts.is_none() && tokio::time::Instant::now() >= deadline) {
                Err(BoundedHookFailure::timed_out())
            } else {
                Err(BoundedHookFailure::cancelled_by_token())
            }
        },
        result = &mut invocation => match result {
            Ok(result) => result.map_err(BoundedHookFailure::regular),
            Err(_) => Err(BoundedHookFailure::panicked()),
        }
    };
    // Catch destructor panics as well as poll panics, before recording exit.
    match std::panic::catch_unwind(AssertUnwindSafe(|| drop(invocation))) {
        Ok(()) => result,
        Err(_) => Err(BoundedHookFailure::panicked()),
    }
}

struct CompiledWsHandshakeMiddleware {
    descriptor: MiddlewareDescriptor,
    middleware: Arc<dyn WebSocketHandshakeMiddleware>,
}

pub(crate) struct CompiledWsHandshakeChain {
    middlewares: Arc<[CompiledWsHandshakeMiddleware]>,
    observer: Arc<dyn WsMiddlewareObserver>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WsHandshakeExecutionFailureKind {
    Rejected(WsHandshakeRejection),
    Timeout,
    Cancelled,
    Panicked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WsHandshakeExecutionFailure {
    descriptor: MiddlewareDescriptor,
    kind: WsHandshakeExecutionFailureKind,
}

impl WsHandshakeExecutionFailure {
    pub(crate) const fn descriptor(&self) -> MiddlewareDescriptor {
        self.descriptor
    }

    pub(crate) const fn kind(&self) -> &WsHandshakeExecutionFailureKind {
        &self.kind
    }
}

impl CompiledWsHandshakeChain {
    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn compile(
        middlewares: Vec<Arc<dyn WebSocketHandshakeMiddleware>>,
    ) -> Result<Self, MiddlewareConfigError> {
        Self::compile_observed(middlewares, Arc::new(NoopWsMiddlewareObserver))
    }

    pub(crate) fn compile_observed(
        middlewares: Vec<Arc<dyn WebSocketHandshakeMiddleware>>,
        observer: Arc<dyn WsMiddlewareObserver>,
    ) -> Result<Self, MiddlewareConfigError> {
        validate_middleware_count(middlewares.len())?;
        let descriptors = middlewares
            .iter()
            .map(|middleware| middleware.descriptor())
            .collect::<Vec<_>>();
        validate_middleware_descriptors(&descriptors)?;
        for middleware in &middlewares {
            middleware.validate()?;
        }
        let middlewares = middlewares
            .into_iter()
            .zip(descriptors)
            .map(|(middleware, descriptor)| CompiledWsHandshakeMiddleware {
                descriptor,
                middleware,
            })
            .collect::<Vec<_>>();
        Ok(Self {
            middlewares: Arc::from(middlewares),
            observer,
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.middlewares.is_empty()
    }

    pub(crate) async fn execute(
        &self,
        exchange: &mut WsHandshakeExchange,
        stage_timeout: Duration,
    ) -> Result<(), WsHandshakeExecutionFailure> {
        for entry in self.middlewares.iter() {
            let started = Instant::now();
            let remaining = exchange
                .deadline()
                .saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                self.observer.observe(
                    entry.descriptor,
                    WsMiddlewareObservationOutcome::Timeout,
                    started.elapsed(),
                );
                return Err(WsHandshakeExecutionFailure {
                    descriptor: entry.descriptor,
                    kind: WsHandshakeExecutionFailureKind::Timeout,
                });
            }
            let budget = stage_timeout.min(remaining);
            let cancellation = exchange.cancellation().clone();
            let execution = async {
                if cancellation.is_cancelled() {
                    return Err(WsHandshakeExecutionFailureKind::Cancelled);
                }
                tokio::select! {
                    biased;
                    _ = cancellation.termination_requested() => Err(WsHandshakeExecutionFailureKind::Cancelled),
                    result = timeout(budget, entry.middleware.handle(exchange, cancellation.clone())) => match result {
                        Ok(Ok(())) => Ok(()),
                        Ok(Err(rejection)) => Err(WsHandshakeExecutionFailureKind::Rejected(rejection)),
                        Err(_) => Err(WsHandshakeExecutionFailureKind::Timeout),
                    }
                }
            };
            let evaluation = AssertUnwindSafe(execution).catch_unwind().await;
            let evaluation = match evaluation {
                Ok(result) => result,
                Err(_) => Err(WsHandshakeExecutionFailureKind::Panicked),
            };
            let outcome = match &evaluation {
                Ok(()) => WsMiddlewareObservationOutcome::Completed,
                Err(WsHandshakeExecutionFailureKind::Rejected(_)) => {
                    WsMiddlewareObservationOutcome::Rejected
                }
                Err(WsHandshakeExecutionFailureKind::Timeout) => {
                    WsMiddlewareObservationOutcome::Timeout
                }
                Err(WsHandshakeExecutionFailureKind::Cancelled) => {
                    WsMiddlewareObservationOutcome::Cancelled
                }
                Err(WsHandshakeExecutionFailureKind::Panicked) => {
                    WsMiddlewareObservationOutcome::Internal
                }
            };
            self.observer
                .observe(entry.descriptor, outcome, started.elapsed());
            if let Err(kind) = evaluation {
                return Err(WsHandshakeExecutionFailure {
                    descriptor: entry.descriptor,
                    kind,
                });
            }
        }
        Ok(())
    }
}

pub(crate) struct CompiledWsIdentityMiddleware {
    descriptor: MiddlewareDescriptor,
    middleware: Arc<dyn WebSocketIdentityMiddleware>,
    observer: Arc<dyn WsMiddlewareObserver>,
}

impl CompiledWsIdentityMiddleware {
    pub(crate) fn compile_observed(
        middleware: Arc<dyn WebSocketIdentityMiddleware>,
        observer: Arc<dyn WsMiddlewareObserver>,
    ) -> Result<Self, MiddlewareConfigError> {
        let descriptor = middleware.descriptor();
        validate_middleware_descriptors(&[descriptor])?;
        middleware.validate()?;
        Ok(Self {
            descriptor,
            middleware,
            observer,
        })
    }

    pub(crate) async fn execute(
        &self,
        exchange: &mut WsHandshakeExchange,
        stage_timeout: Duration,
    ) -> Result<WebSocketIdentity, WsHandshakeExecutionFailure> {
        let started = Instant::now();
        let remaining = exchange
            .deadline()
            .saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            self.observer.observe(
                self.descriptor,
                WsMiddlewareObservationOutcome::Timeout,
                started.elapsed(),
            );
            return Err(WsHandshakeExecutionFailure {
                descriptor: self.descriptor,
                kind: WsHandshakeExecutionFailureKind::Timeout,
            });
        }
        let budget = stage_timeout.min(remaining);
        let cancellation = exchange.cancellation().clone();
        let execution = async {
            if cancellation.is_cancelled() {
                return Err(WsHandshakeExecutionFailureKind::Cancelled);
            }
            tokio::select! {
                biased;
                _ = cancellation.termination_requested() => Err(WsHandshakeExecutionFailureKind::Cancelled),
                result = timeout(budget, self.middleware.identify(exchange, cancellation.clone())) => match result {
                    Ok(Ok(identity)) => Ok(identity),
                    Ok(Err(rejection)) => Err(WsHandshakeExecutionFailureKind::Rejected(rejection)),
                    Err(_) => Err(WsHandshakeExecutionFailureKind::Timeout),
                }
            }
        };
        let evaluation = match AssertUnwindSafe(execution).catch_unwind().await {
            Ok(result) => result,
            Err(_) => Err(WsHandshakeExecutionFailureKind::Panicked),
        };
        let outcome = match &evaluation {
            Ok(_) => WsMiddlewareObservationOutcome::Completed,
            Err(WsHandshakeExecutionFailureKind::Rejected(_)) => {
                WsMiddlewareObservationOutcome::Rejected
            }
            Err(WsHandshakeExecutionFailureKind::Timeout) => {
                WsMiddlewareObservationOutcome::Timeout
            }
            Err(WsHandshakeExecutionFailureKind::Cancelled) => {
                WsMiddlewareObservationOutcome::Cancelled
            }
            Err(WsHandshakeExecutionFailureKind::Panicked) => {
                WsMiddlewareObservationOutcome::Internal
            }
        };
        self.observer
            .observe(self.descriptor, outcome, started.elapsed());
        evaluation.map_err(|kind| WsHandshakeExecutionFailure {
            descriptor: self.descriptor,
            kind,
        })
    }
}

struct CompiledWsConnectionMiddleware {
    descriptor: MiddlewareDescriptor,
    middleware: Arc<dyn WsConnectionMiddleware>,
}

pub(crate) struct CompiledWsConnectionChain {
    middlewares: Arc<[CompiledWsConnectionMiddleware]>,
    observer: Arc<dyn WsMiddlewareObserver>,
}

/// Shared entered-prefix state. A server-owned cleanup lease clones this
/// handle before admission begins, so cancellation cannot discard the prefix.
#[derive(Clone, Default)]
pub(crate) struct WsConnectionLedger {
    state: Arc<Mutex<EnteredLifecycleLedger>>,
}

impl fmt::Debug for WsConnectionLedger {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WsConnectionLedger")
            .field("entered", &self.entered())
            .field("cleanup_started", &self.cleanup_started())
            .finish()
    }
}

impl WsConnectionLedger {
    pub(crate) fn accounting(&self) -> crate::reporting::LedgerCounts {
        self.with_state(crate::reporting::LedgerCounts::observe)
    }

    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn entered(&self) -> usize {
        self.with_state(|state| {
            if state.cleanup_claimed {
                0
            } else {
                state.entries.len()
            }
        })
    }

    pub(crate) fn cleanup_started(&self) -> bool {
        self.with_state(|state| state.cleanup_claimed)
    }

    fn record_entered(&self, next: usize) -> bool {
        self.with_state_mut(|state| state.record_entered(next))
    }

    fn claim_cleanup(&self) -> Option<usize> {
        self.with_state_mut(EnteredLifecycleLedger::claim_cleanup)
    }

    fn with_state<T>(&self, operation: impl FnOnce(&EnteredLifecycleLedger) -> T) -> T {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        operation(&state)
    }

    fn with_state_mut<T>(&self, operation: impl FnOnce(&mut EnteredLifecycleLedger) -> T) -> T {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        operation(&mut state)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct WsMiddlewareCleanupReport {
    claimed: bool,
    attempted: usize,
    completed: usize,
    failed: usize,
    timed_out: usize,
    cancelled: usize,
    cancellation_observed: usize,
    panicked: usize,
    first_failure: Option<WsMiddlewareExecutionError>,
}

impl WsMiddlewareCleanupReport {
    #[cfg(test)]
    pub(crate) const fn claimed(self) -> bool {
        self.claimed
    }

    pub(crate) const fn attempted(self) -> usize {
        self.attempted
    }

    #[cfg(test)]
    pub(crate) const fn completed(self) -> usize {
        self.completed
    }

    pub(crate) const fn failed(self) -> usize {
        self.failed
    }

    pub(crate) const fn timed_out(self) -> usize {
        self.timed_out
    }

    pub(crate) const fn cancelled(self) -> usize {
        self.cancelled
    }

    /// Failures produced by the framework cancellation token, as opposed to
    /// a middleware implementation returning `WsMiddlewareError::cancelled()`.
    pub(crate) const fn cancellation_observed(self) -> usize {
        self.cancellation_observed
    }

    pub(crate) const fn panicked(self) -> usize {
        self.panicked
    }

    pub(crate) const fn first_failure(self) -> Option<WsMiddlewareExecutionError> {
        self.first_failure
    }

    #[cfg(test)]
    pub(crate) const fn is_success(self) -> bool {
        self.claimed && self.failed == 0
    }
}

impl CompiledWsConnectionChain {
    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn compile(
        middlewares: Vec<Arc<dyn WsConnectionMiddleware>>,
    ) -> Result<Self, MiddlewareConfigError> {
        Self::compile_observed(middlewares, Arc::new(NoopWsMiddlewareObserver))
    }

    pub(crate) fn compile_observed(
        middlewares: Vec<Arc<dyn WsConnectionMiddleware>>,
        observer: Arc<dyn WsMiddlewareObserver>,
    ) -> Result<Self, MiddlewareConfigError> {
        validate_middleware_count(middlewares.len())?;
        let descriptors = middlewares
            .iter()
            .map(|middleware| middleware.descriptor())
            .collect::<Vec<_>>();
        validate_middleware_descriptors(&descriptors)?;
        for middleware in &middlewares {
            middleware.validate()?;
        }
        let middlewares = middlewares
            .into_iter()
            .zip(descriptors)
            .map(|(middleware, descriptor)| CompiledWsConnectionMiddleware {
                descriptor,
                middleware,
            })
            .collect::<Vec<_>>();
        Ok(Self {
            middlewares: Arc::from(middlewares),
            observer,
        })
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.middlewares.is_empty()
    }

    pub(crate) async fn admit(
        &self,
        context: Arc<WebSocketContext>,
        ledger: &WsConnectionLedger,
        stage_timeout: Duration,
        cancellation: &CancellationToken,
    ) -> Result<(), WsMiddlewareExecutionError> {
        let start = ledger.entered();
        let signal = ExecutionCancellation::with_budget(
            cancellation.clone(),
            context.shutdown_budget().clone(),
        );
        for (index, entry) in self.middlewares.iter().enumerate().skip(start) {
            let started = Instant::now();
            let result = execute_bounded_hook(
                || {
                    context.dispatcher().execution_scope(
                        signal.clone(),
                        context.shutdown_budget().clone(),
                        tokio::time::Instant::now() + stage_timeout,
                        async {
                            entry
                                .middleware
                                .admit(context.clone(), signal.clone())
                                .await
                        },
                    )
                },
                stage_timeout,
                &signal,
            )
            .await;
            let outcome = result.as_ref().map_or_else(
                |failure| WsMiddlewareObservationOutcome::from_error(failure.error),
                |_| WsMiddlewareObservationOutcome::Completed,
            );
            if result.is_ok() && !ledger.record_entered(index + 1) {
                return Err(WsMiddlewareExecutionError::new(
                    entry.descriptor,
                    WsMiddlewareStage::ConnectionAdmission,
                    WsMiddlewareError::internal(bounded_code("WS_MIDDLEWARE_LEDGER_CLOSED")),
                ));
            }
            self.observer
                .observe(entry.descriptor, outcome, started.elapsed());
            result.map_err(|failure| {
                WsMiddlewareExecutionError::new(
                    entry.descriptor,
                    WsMiddlewareStage::ConnectionAdmission,
                    failure.error,
                )
            })?;
        }
        Ok(())
    }

    pub(crate) async fn opened(
        &self,
        context: Arc<WebSocketContext>,
        ledger: &WsConnectionLedger,
        stage_timeout: Duration,
        cancellation: &CancellationToken,
    ) -> Result<(), WsMiddlewareExecutionError> {
        let signal = ExecutionCancellation::with_budget(
            cancellation.clone(),
            context.shutdown_budget().clone(),
        );
        for entry in self.middlewares.iter().take(ledger.entered()) {
            let started = Instant::now();
            let result = execute_bounded_hook(
                || {
                    context.dispatcher().execution_scope(
                        signal.clone(),
                        context.shutdown_budget().clone(),
                        tokio::time::Instant::now() + stage_timeout,
                        async {
                            entry
                                .middleware
                                .opened(context.clone(), signal.clone())
                                .await
                        },
                    )
                },
                stage_timeout,
                &signal,
            )
            .await;
            let outcome = result.as_ref().map_or_else(
                |failure| WsMiddlewareObservationOutcome::from_error(failure.error),
                |_| WsMiddlewareObservationOutcome::Completed,
            );
            self.observer
                .observe(entry.descriptor, outcome, started.elapsed());
            result.map_err(|failure| {
                WsMiddlewareExecutionError::new(
                    entry.descriptor,
                    WsMiddlewareStage::ConnectionOpened,
                    failure.error,
                )
            })?;
        }
        Ok(())
    }

    pub(crate) async fn cleanup(
        &self,
        context: Arc<WebSocketContext>,
        ledger: &WsConnectionLedger,
        category: WsConnectionCloseCategory,
        stage_timeout: Duration,
        cancellation: &CancellationToken,
    ) -> WsMiddlewareCleanupReport {
        let Some(entered) = ledger.claim_cleanup() else {
            return WsMiddlewareCleanupReport::default();
        };
        let mut report = WsMiddlewareCleanupReport {
            claimed: true,
            ..WsMiddlewareCleanupReport::default()
        };

        for (index, entry) in self.middlewares.iter().take(entered).enumerate().rev() {
            assert!(ledger.with_state_mut(|state| {
                state.claim_exit(index, LifecycleExitPath::Termination)
            }));
            report.attempted += 1;
            let started = Instant::now();
            let invocation_cancellation = cancellation.child_token();
            let _invocation_authority = invocation_cancellation.clone().drop_guard();
            let signal = CleanupCancellation::new(invocation_cancellation);
            let deadline = tokio::time::Instant::now() + stage_timeout;
            let result = execute_bounded_cancellable_cleanup_hook(
                || {
                    context.dispatcher().cleanup_scope(
                        signal.clone(),
                        context.shutdown_budget().clone(),
                        deadline,
                        async {
                            assert!(ledger.with_state_mut(|state| {
                                state.start_exit(index, LifecycleExitPath::Termination)
                            }));
                            entry
                                .middleware
                                .closed(context.clone(), category, signal.clone())
                                .await
                        },
                    )
                },
                deadline,
                cancellation,
                context.shutdown_budget(),
            )
            .await;
            let lifecycle_outcome = result.as_ref().map_or_else(
                |failure| failure.lifecycle_outcome(),
                |()| LifecycleOutcome::Completed,
            );
            assert!(ledger.with_state_mut(|state| {
                state.finish_exit(index, LifecycleExitPath::Termination, lifecycle_outcome)
            }));
            let outcome = result.as_ref().map_or_else(
                |failure| WsMiddlewareObservationOutcome::from_error(failure.error),
                |_| WsMiddlewareObservationOutcome::Completed,
            );
            self.observer
                .observe(entry.descriptor, outcome, started.elapsed());
            match result {
                Ok(()) => report.completed += 1,
                Err(failure) => {
                    let error = failure.error;
                    report.failed += 1;
                    if failure.cancellation_observed {
                        report.cancellation_observed += 1;
                    }
                    match error.kind() {
                        WsMiddlewareFailureKind::Timeout => report.timed_out += 1,
                        WsMiddlewareFailureKind::Cancelled => report.cancelled += 1,
                        WsMiddlewareFailureKind::Rejected | WsMiddlewareFailureKind::Internal => {}
                    }
                    if failure.panicked {
                        report.panicked += 1;
                    }
                    report.first_failure.get_or_insert_with(|| {
                        WsMiddlewareExecutionError::new(
                            entry.descriptor,
                            WsMiddlewareStage::ConnectionClosed,
                            error,
                        )
                    });
                }
            }
        }
        report
    }
}

struct CompiledWsMessageMiddleware {
    descriptor: MiddlewareDescriptor,
    middleware: Arc<dyn WsMessageMiddleware>,
}

pub(crate) struct CompiledWsMessageChain {
    middlewares: Arc<[CompiledWsMessageMiddleware]>,
    observer: Arc<dyn WsMiddlewareObserver>,
}

#[derive(Debug, PartialEq, Eq, Default)]
pub(crate) struct WsMessageLedger {
    lifecycle: EnteredLifecycleLedger,
    outcome: Option<WsMessageOutcome>,
    first_failure: Option<WsMiddlewareExecutionError>,
    termination_reason: Option<WsMessageTerminationReason>,
}

impl WsMessageLedger {
    pub(crate) fn accounting(&self) -> crate::reporting::LedgerCounts {
        crate::reporting::LedgerCounts::observe(&self.lifecycle)
    }

    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn entered(&self) -> usize {
        self.lifecycle.entries.len()
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct WsMessageAfterReport {
    outcome: WsMessageOutcome,
    attempted: usize,
    failed: usize,
    first_failure: Option<WsMiddlewareExecutionError>,
}

impl WsMessageAfterReport {
    pub(crate) const fn outcome(&self) -> WsMessageOutcome {
        self.outcome
    }

    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) const fn attempted(&self) -> usize {
        self.attempted
    }

    pub(crate) const fn failed(&self) -> usize {
        self.failed
    }

    pub(crate) const fn first_failure(&self) -> Option<WsMiddlewareExecutionError> {
        self.first_failure
    }
}

/// Merge one successful reverse-hook decision into the message terminal.
///
/// Reverse hooks run from the innermost entered middleware to the outermost, so
/// preserving an existing Close makes terminal selection first-writer-wins.
/// Reject remains a success-only transition; cleanup failures, including the
/// explicit server-shutdown cancellation mapping, are handled separately.
const fn merge_message_after_decision(
    outcome: WsMessageOutcome,
    decision: WsMessageDecision,
) -> WsMessageOutcome {
    match decision {
        WsMessageDecision::Continue => outcome,
        WsMessageDecision::Reject(code) => match outcome {
            WsMessageOutcome::Handled => WsMessageOutcome::Rejected(code),
            _ => outcome,
        },
        WsMessageDecision::Close(reason) => match outcome {
            WsMessageOutcome::Close(_) => outcome,
            _ => WsMessageOutcome::Close(reason),
        },
    }
}

impl CompiledWsMessageChain {
    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn compile(
        middlewares: Vec<Arc<dyn WsMessageMiddleware>>,
    ) -> Result<Self, MiddlewareConfigError> {
        Self::compile_observed(middlewares, Arc::new(NoopWsMiddlewareObserver))
    }

    pub(crate) fn compile_observed(
        middlewares: Vec<Arc<dyn WsMessageMiddleware>>,
        observer: Arc<dyn WsMiddlewareObserver>,
    ) -> Result<Self, MiddlewareConfigError> {
        validate_middleware_count(middlewares.len())?;
        let descriptors = middlewares
            .iter()
            .map(|middleware| middleware.descriptor())
            .collect::<Vec<_>>();
        validate_middleware_descriptors(&descriptors)?;
        for middleware in &middlewares {
            middleware.validate()?;
        }
        let middlewares = middlewares
            .into_iter()
            .zip(descriptors)
            .map(|(middleware, descriptor)| CompiledWsMessageMiddleware {
                descriptor,
                middleware,
            })
            .collect::<Vec<_>>();
        Ok(Self {
            middlewares: Arc::from(middlewares),
            observer,
        })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.middlewares.is_empty()
    }

    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) async fn before(
        &self,
        exchange: &mut WsMessageExchange,
    ) -> (
        WsMessageLedger,
        Result<WsMessageDecision, WsMiddlewareExecutionError>,
    ) {
        let mut ledger = WsMessageLedger::default();
        let result = self.before_entered(exchange, &mut ledger).await;
        (ledger, result)
    }

    /// The entered ledger belongs to the lifecycle owner, even while a later
    /// before hook is pending in an abortable execution slot.
    pub(crate) async fn before_entered(
        &self,
        exchange: &mut WsMessageExchange,
        ledger: &mut WsMessageLedger,
    ) -> Result<WsMessageDecision, WsMiddlewareExecutionError> {
        if self.middlewares.is_empty() {
            return Ok(WsMessageDecision::Continue);
        }

        let cancellation = exchange.cancellation().clone();
        for entry in self.middlewares.iter() {
            let dispatcher = exchange.connection().dispatcher().clone();
            let budget = exchange.connection().shutdown_budget().clone();
            let deadline = exchange.deadline();
            let started = Instant::now();
            let result = execute_bounded_message_hook(
                || {
                    dispatcher.execution_scope(
                        cancellation.clone(),
                        budget.clone(),
                        deadline,
                        async {
                            entry
                                .middleware
                                .before_message(exchange, cancellation.clone())
                                .await
                        },
                    )
                },
                deadline,
                &cancellation,
                &budget,
            )
            .await;
            // Commit successful entry before diagnostics can run user code.
            if matches!(result, Ok(WsMessageDecision::Continue)) {
                let next = ledger.lifecycle.entries.len() + 1;
                assert!(ledger.lifecycle.record_entered(next));
            }
            if let Err(failure) = &result
                && let Some(reason) = failure.interruption
            {
                ledger.request_termination(reason.into());
            }
            let observation_outcome = match &result {
                Ok(decision) => WsMiddlewareObservationOutcome::from_decision(*decision),
                Err(failure) => WsMiddlewareObservationOutcome::from_error(failure.error),
            };
            self.observer
                .observe(entry.descriptor, observation_outcome, started.elapsed());
            let decision = match result {
                Ok(decision) => decision,
                Err(failure) => {
                    return Err(WsMiddlewareExecutionError::new(
                        entry.descriptor,
                        WsMiddlewareStage::MessageBefore,
                        failure.error,
                    ));
                }
            };
            match decision {
                WsMessageDecision::Continue => {}
                decision => return Ok(decision),
            }
        }
        Ok(WsMessageDecision::Continue)
    }

    pub(crate) async fn after(
        &self,
        exchange: &mut WsMessageExchange,
        ledger: &mut WsMessageLedger,
        mut outcome: WsMessageOutcome,
    ) -> WsMessageAfterReport {
        ledger.outcome = Some(outcome);
        if ledger.termination_reason.is_some() {
            return ledger.report(outcome);
        }
        let entered = ledger
            .lifecycle
            .claim_cleanup()
            .expect("message unwind owns an unclaimed entered ledger");
        if entered == 0 {
            return ledger.report(outcome);
        }

        let budget = exchange.connection().shutdown_budget().clone();
        let deadline = exchange.deadline();
        for (index, entry) in self.middlewares.iter().take(entered).enumerate().rev() {
            assert!(
                ledger
                    .lifecycle
                    .claim_exit(index, LifecycleExitPath::Normal)
            );
            let started = Instant::now();
            let cancellation = exchange.cancellation().clone();
            let dispatcher = exchange.connection().dispatcher().clone();
            let result = execute_bounded_message_hook(
                || {
                    dispatcher.execution_scope(
                        cancellation.clone(),
                        budget.clone(),
                        deadline,
                        async {
                            assert!(
                                ledger
                                    .lifecycle
                                    .start_exit(index, LifecycleExitPath::Normal)
                            );
                            entry
                                .middleware
                                .after_message(exchange, outcome, cancellation.clone())
                                .await
                        },
                    )
                },
                deadline,
                &cancellation,
                &budget,
            )
            .await;
            let lifecycle_outcome = result.as_ref().map_or_else(
                |failure| failure.lifecycle_outcome(),
                |_| LifecycleOutcome::Completed,
            );
            assert!(ledger.lifecycle.finish_exit(
                index,
                LifecycleExitPath::Normal,
                lifecycle_outcome,
            ));
            let observation_outcome = match &result {
                Ok(decision) => WsMiddlewareObservationOutcome::from_decision(*decision),
                Err(failure) => WsMiddlewareObservationOutcome::from_error(failure.error),
            };
            match result {
                Ok(decision) => {
                    outcome = merge_message_after_decision(outcome, decision);
                }
                Err(failure) => {
                    let error = failure.error;
                    if let Some(reason) = failure.interruption {
                        ledger.request_termination(reason.into());
                    }
                    if matches!(outcome, WsMessageOutcome::Handled) {
                        outcome = WsMessageOutcome::Failed(error.diagnostic_code());
                    }
                    ledger.first_failure.get_or_insert_with(|| {
                        WsMiddlewareExecutionError::new(
                            entry.descriptor,
                            WsMiddlewareStage::MessageAfter,
                            error,
                        )
                    });
                }
            }
            ledger.outcome = Some(outcome);
            self.observer
                .observe(entry.descriptor, observation_outcome, started.elapsed());
            // Do not let outer normal exits overtake this interrupted inner
            // exit's termination obligation. The retained owner resumes here.
            if ledger.termination_reason.is_some() {
                break;
            }
        }
        ledger.report(outcome)
    }
}

#[cfg(test)]
mod typed_tests {
    mod cancellation;
    mod message_termination;

    use super::*;
    use crate::{
        connection::ConnectionManager,
        request::{WsHeaders, WsMessageBody},
    };
    use lily_injection::ApplicationContainer;
    use std::{
        future::pending,
        sync::{
            Mutex as StdMutex,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use uuid::Uuid;

    type Observation = (&'static str, WsMiddlewareObservationOutcome);

    #[derive(Default)]
    struct RecordingObserver {
        observations: StdMutex<Vec<Observation>>,
    }

    impl WsMiddlewareObserver for RecordingObserver {
        fn observe(
            &self,
            descriptor: MiddlewareDescriptor,
            outcome: WsMiddlewareObservationOutcome,
            _duration: Duration,
        ) {
            self.observations
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push((descriptor.name(), outcome));
        }
    }

    fn code(value: &'static str) -> MiddlewareErrorCode {
        MiddlewareErrorCode::new(value).unwrap()
    }

    fn unauthorized_rejection(code: MiddlewareErrorCode) -> WsHandshakeRejection {
        WsHandshakeRejection::unauthorized(code, HeaderValue::from_static("Bearer"))
            .expect("static authentication challenge is valid")
    }

    #[test]
    fn slow_consumer_close_category_has_a_stable_telemetry_label() {
        assert_eq!(
            WsConnectionCloseCategory::SlowConsumer.as_str(),
            "slow_consumer"
        );
    }

    fn context() -> Arc<WebSocketContext> {
        Arc::new(WebSocketContext::new(
            Uuid::new_v4(),
            Arc::new(ConnectionManager::new()),
            "test".to_string(),
        ))
    }

    fn request() -> Arc<WsRequest> {
        Arc::new(
            WsRequest::new_from_message(
                Uuid::new_v4(),
                WsMessageBody::new_text("test:event", "middleware-test")
                    .to_message()
                    .expect("static middleware test envelope is valid"),
                WsHeaders::default(),
                RequestConnectionInfo::default(),
                WsTransportSecurity::Plaintext,
            )
            .expect("static middleware test request is valid"),
        )
    }

    fn handshake_request(
        namespace: impl Into<String>,
        headers: WsHeaders,
        peer_addr: SocketAddr,
    ) -> WsHandshakeRequest {
        WsHandshakeRequest::new(
            namespace.into(),
            headers,
            peer_addr,
            RequestConnectionInfo::direct(peer_addr.ip()),
            WsTransportSecurity::Plaintext,
        )
    }

    fn message_exchange(cancellation: CancellationToken) -> WsMessageExchange {
        WsMessageExchange::new_without_services(
            context(),
            request(),
            WebSocketMessageLocals::default(),
            cancellation,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
    }

    async fn execute_handshake_chain(
        chain: &CompiledWsHandshakeChain,
        request: WsHandshakeRequest,
    ) -> Result<(), WsHandshakeExecutionFailure> {
        let container = ApplicationContainer::build()
            .await
            .expect("build handshake test container");
        let mut exchange = WsHandshakeExchange::new(
            container.services(),
            request,
            CancellationToken::new(),
            tokio::time::Instant::now() + Duration::from_secs(1),
        );
        let result = chain.execute(&mut exchange, Duration::from_secs(1)).await;
        container
            .close()
            .await
            .expect("close handshake test container");
        result
    }

    async fn execute_identity_middleware(
        behavior: IdentityBehavior,
        calls: Arc<AtomicUsize>,
        cancellation: CancellationToken,
        stage_timeout: Duration,
    ) -> (
        Result<WebSocketIdentity, WsHandshakeExecutionFailure>,
        Arc<RecordingObserver>,
    ) {
        const SECRET: &str = "LILY_SECRET_IDENTITY_CREDENTIAL";

        let observer = Arc::new(RecordingObserver::default());
        let observer_dyn: Arc<dyn WsMiddlewareObserver> = observer.clone();
        let compiled = CompiledWsIdentityMiddleware::compile_observed(
            Arc::new(IdentityProbe { behavior, calls }),
            observer_dyn,
        )
        .expect("compile identity middleware");
        let container = ApplicationContainer::build()
            .await
            .expect("build identity test container");
        let mut headers = WsHeaders::new();
        headers
            .set_custom_header("authorization".to_owned(), SECRET.to_owned())
            .unwrap();
        let mut exchange = WsHandshakeExchange::new(
            container.services(),
            handshake_request("identity-test", headers, "127.0.0.1:1".parse().unwrap()),
            cancellation,
            tokio::time::Instant::now() + Duration::from_secs(1),
        );
        let result = compiled.execute(&mut exchange, stage_timeout).await;
        container
            .close()
            .await
            .expect("close identity test container");
        (result, observer)
    }

    #[derive(Clone, Copy)]
    enum HookBehavior {
        Ok,
        Error,
        Pending,
        Panic,
    }

    async fn run_behavior(behavior: HookBehavior) -> Result<(), WsMiddlewareError> {
        match behavior {
            HookBehavior::Ok => Ok(()),
            HookBehavior::Error => Err(WsMiddlewareError::internal(code("TEST_HOOK_ERROR"))),
            HookBehavior::Pending => pending().await,
            HookBehavior::Panic => panic!("untrusted hook panic payload"),
        }
    }

    struct ConnectionProbe {
        name: &'static str,
        admit_label: &'static str,
        opened_label: &'static str,
        closed_label: &'static str,
        log: Arc<StdMutex<Vec<&'static str>>>,
        admit: HookBehavior,
        opened: HookBehavior,
        closed: HookBehavior,
    }

    #[async_trait]
    impl WsConnectionMiddleware for ConnectionProbe {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            Err(WsMiddlewareInitError::Internal)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(self.name, MiddlewareKind::Custom)
        }

        async fn admit(
            &self,
            _context: Arc<WebSocketContext>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), WsMiddlewareError> {
            self.log.lock().unwrap().push(self.admit_label);
            run_behavior(self.admit).await
        }

        async fn opened(
            &self,
            _context: Arc<WebSocketContext>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), WsMiddlewareError> {
            self.log.lock().unwrap().push(self.opened_label);
            run_behavior(self.opened).await
        }

        async fn closed(
            &self,
            _context: Arc<WebSocketContext>,
            _category: WsConnectionCloseCategory,
            _cancellation: crate::CleanupCancellation,
        ) -> Result<(), WsMiddlewareError> {
            self.log.lock().unwrap().push(self.closed_label);
            run_behavior(self.closed).await
        }
    }

    fn connection_probe(
        name: &'static str,
        labels: (&'static str, &'static str, &'static str),
        log: Arc<StdMutex<Vec<&'static str>>>,
        behavior: (HookBehavior, HookBehavior, HookBehavior),
    ) -> Arc<dyn WsConnectionMiddleware> {
        Arc::new(ConnectionProbe {
            name,
            admit_label: labels.0,
            opened_label: labels.1,
            closed_label: labels.2,
            log,
            admit: behavior.0,
            opened: behavior.1,
            closed: behavior.2,
        })
    }

    struct HandshakeProbe {
        name: &'static str,
        label: &'static str,
        log: Arc<StdMutex<Vec<&'static str>>>,
        rejection: Option<WsHandshakeRejection>,
    }

    #[async_trait]
    impl WebSocketHandshakeMiddleware for HandshakeProbe {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            Err(WsMiddlewareInitError::Internal)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(self.name, MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            _exchange: &mut WsHandshakeExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), WsHandshakeRejection> {
            self.log.lock().unwrap().push(self.label);
            self.rejection.clone().map_or(Ok(()), Err)
        }
    }

    struct CountingHandshakeMiddleware(Arc<AtomicUsize>);

    struct PanickingHandshakeMiddleware;

    #[derive(Clone, Copy)]
    enum IdentityBehavior {
        Anonymous,
        Pending,
    }

    struct IdentityProbe {
        behavior: IdentityBehavior,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl WebSocketHandshakeMiddleware for CountingHandshakeMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            Err(WsMiddlewareInitError::Internal)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            self.0.fetch_add(1, Ordering::Relaxed);
            MiddlewareDescriptor::new("counting", MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            _exchange: &mut WsHandshakeExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), WsHandshakeRejection> {
            Ok(())
        }
    }

    #[async_trait]
    impl WebSocketHandshakeMiddleware for PanickingHandshakeMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            Err(WsMiddlewareInitError::Internal)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("panicking_middleware", MiddlewareKind::WebSocketHandshake)
        }

        async fn handle(
            &self,
            _exchange: &mut WsHandshakeExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), WsHandshakeRejection> {
            panic!("LILY_SECRET_ASYNC_HANDSHAKE_PANIC")
        }
    }

    #[async_trait]
    impl WebSocketIdentityMiddleware for IdentityProbe {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            Err(WsMiddlewareInitError::Internal)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("identity_probe", MiddlewareKind::WebSocketHandshake)
        }

        async fn identify(
            &self,
            _exchange: &mut WsHandshakeExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<WebSocketIdentity, WsHandshakeRejection> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.behavior {
                IdentityBehavior::Anonymous => Ok(WebSocketIdentity::anonymous()),
                IdentityBehavior::Pending => pending().await,
            }
        }
    }

    struct MessageProbe {
        name: &'static str,
        before_label: &'static str,
        after_label: &'static str,
        log: Arc<StdMutex<Vec<&'static str>>>,
        before: WsMessageDecision,
        after: Result<WsMessageDecision, WsMiddlewareError>,
    }

    #[async_trait]
    impl WsMessageMiddleware for MessageProbe {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            Err(WsMiddlewareInitError::Internal)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(self.name, MiddlewareKind::Custom)
        }

        async fn before_message(
            &self,
            _exchange: &mut WsMessageExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<WsMessageDecision, WsMiddlewareError> {
            self.log.lock().unwrap().push(self.before_label);
            Ok(self.before)
        }

        async fn after_message(
            &self,
            _exchange: &mut WsMessageExchange,
            _outcome: WsMessageOutcome,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<WsMessageDecision, WsMiddlewareError> {
            self.log.lock().unwrap().push(self.after_label);
            self.after
        }
    }

    struct HangingAfter {
        name: &'static str,
    }

    #[async_trait]
    impl WsMessageMiddleware for HangingAfter {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
            Err(WsMiddlewareInitError::Internal)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(self.name, MiddlewareKind::Custom)
        }

        async fn before_message(
            &self,
            _exchange: &mut WsMessageExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<WsMessageDecision, WsMiddlewareError> {
            Ok(WsMessageDecision::Continue)
        }

        async fn after_message(
            &self,
            _exchange: &mut WsMessageExchange,
            _outcome: WsMessageOutcome,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<WsMessageDecision, WsMiddlewareError> {
            pending().await
        }
    }

    #[test]
    fn handshake_request_debug_is_secret_safe_and_rejections_are_bounded() {
        let mut headers = WsHeaders::new();
        headers
            .set_custom_header(
                "authorization".to_string(),
                "Bearer super-secret".to_string(),
            )
            .unwrap();
        headers.origin = Some("https://private.example".to_string());
        headers.sec_websocket_protocol = Some(vec!["lily.v1".to_string()]);
        let request = handshake_request(
            "/private-tenant",
            headers,
            "127.0.0.1:4321".parse().unwrap(),
        );
        let debug = format!("{request:?}");
        assert!(!debug.contains("super-secret"));
        assert!(!debug.contains("private.example"));
        assert!(!debug.contains("private-tenant"));
        assert!(!debug.contains("127.0.0.1"));
        assert_eq!(request.namespace(), "/private-tenant");
        assert_eq!(request.requested_protocols(), ["lily.v1"]);
        assert_eq!(request.peer_addr().port(), 4321);
        assert!(
            request
                .headers()
                .get_custom_header("authorization")
                .is_some()
        );

        assert_eq!(
            WsHandshakeRejection::try_new(403, code("ORIGIN_DENIED"))
                .unwrap()
                .public_body(),
            "The WebSocket upgrade request is not allowed."
        );
        assert_eq!(
            WsHandshakeRejection::try_new(500, code("INTERNAL"))
                .unwrap()
                .public_body(),
            "The WebSocket upgrade request could not be completed."
        );
        assert!(WsHandshakeRejection::try_new(200, code("NOT_A_REJECTION")).is_err());
        assert!(WsHandshakeRejection::try_new(600, code("INVALID_STATUS")).is_err());
        assert!(matches!(
            WsHandshakeRejection::try_new(401, code("AUTH_REQUIRED")),
            Err(WsHandshakeRejectionConfigError::RequiredHeaderMissing)
        ));
        for status in [405, 407, 426] {
            assert!(matches!(
                WsHandshakeRejection::try_new(status, code("UNSUPPORTED_SEMANTICS")),
                Err(WsHandshakeRejectionConfigError::UnsupportedStatus)
            ));
        }
        for status in [400, 403, 429, 500, 503] {
            assert!(WsHandshakeRejection::try_new(status, code("SUPPORTED_STATUS")).is_ok());
        }

        assert!(matches!(
            WsHandshakeRejection::unauthorized(code("AUTH_REQUIRED"), HeaderValue::from_static("")),
            Err(WsHandshakeRejectionConfigError::InvalidAuthenticationChallenge)
        ));
        for invalid in ["Bearer ???", "Basic realm=\"unterminated"] {
            assert!(matches!(
                WsHandshakeRejection::unauthorized(
                    code("AUTH_REQUIRED"),
                    HeaderValue::from_bytes(invalid.as_bytes()).unwrap(),
                ),
                Err(WsHandshakeRejectionConfigError::InvalidAuthenticationChallenge)
            ));
        }
        for valid in [
            "Bearer",
            "Bearer dG9rZW4=",
            "Basic realm=\"api\"",
            "Bearer realm=\"api\", error=\"invalid_token\"",
        ] {
            let rejection = WsHandshakeRejection::unauthorized(
                code("AUTH_REQUIRED"),
                HeaderValue::from_bytes(valid.as_bytes()).unwrap(),
            )
            .unwrap();
            assert_eq!(rejection.status(), StatusCode::UNAUTHORIZED);
            assert_eq!(
                rejection.headers(),
                [(
                    WWW_AUTHENTICATE,
                    HeaderValue::from_bytes(valid.as_bytes()).unwrap()
                )]
            );
        }

        assert!(matches!(
            WsHandshakeRejection::bad_request(code("BODY_TOO_LARGE"))
                .try_with_body("x".repeat(MAX_HANDSHAKE_REJECTION_BODY_BYTES + 1)),
            Err(WsHandshakeRejectionConfigError::BodyTooLarge)
        ));
        assert!(matches!(
            WsHandshakeRejection::bad_request(code("BODY_CONTROL")).try_with_body("public\0secret"),
            Err(WsHandshakeRejectionConfigError::InvalidBody)
        ));

        let allowed = unauthorized_rejection(code("AUTH_REQUIRED"))
            .try_with_header(
                HeaderName::from_static("www-authenticate"),
                HeaderValue::from_static("Basic realm=\"api\""),
            )
            .unwrap();
        assert_eq!(allowed.headers().len(), 2);
        let allowed_debug = format!("{allowed:?}");
        assert!(!allowed_debug.contains("Bearer"));
        assert!(!allowed_debug.contains("realm"));
        assert!(matches!(
            allowed.clone().try_with_header(
                HeaderName::from_static("content-type"),
                HeaderValue::from_static("application/json"),
            ),
            Err(WsHandshakeRejectionConfigError::HeaderNotAllowed)
        ));
        assert!(matches!(
            allowed.try_with_header(
                HeaderName::from_static("retry-after"),
                HeaderValue::from_bytes(b"1\t0").unwrap(),
            ),
            Err(WsHandshakeRejectionConfigError::InvalidHeaderValue)
        ));
        assert!(matches!(
            WsHandshakeRejection::forbidden(code("INVALID_CHALLENGE"))
                .try_with_header(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer ???"),),
            Err(WsHandshakeRejectionConfigError::InvalidAuthenticationChallenge)
        ));

        let mut maximum_headers = WsHandshakeRejection::forbidden(code("HEADER_BOUND"));
        for _ in 0..MAX_HANDSHAKE_REJECTION_HEADERS {
            maximum_headers = maximum_headers
                .try_with_header(
                    HeaderName::from_static("set-cookie"),
                    HeaderValue::from_static("session=; Max-Age=0"),
                )
                .unwrap();
        }
        assert!(matches!(
            maximum_headers.try_with_header(
                HeaderName::from_static("cache-control"),
                HeaderValue::from_static("no-store"),
            ),
            Err(WsHandshakeRejectionConfigError::TooManyHeaders)
        ));

        for (rejection, message) in [
            (
                WsHandshakeRejection::bad_request(code("INVALID_UPGRADE")),
                "The WebSocket upgrade request is invalid.",
            ),
            (
                unauthorized_rejection(code("AUTH_REQUIRED")),
                "WebSocket authentication is required.",
            ),
            (
                WsHandshakeRejection::forbidden(code("ORIGIN_DENIED")),
                "The WebSocket upgrade request is not allowed.",
            ),
        ] {
            assert_eq!(rejection.public_body(), message);
            assert!(rejection.to_string().contains(rejection.code().as_str()));
        }

        let error = WsMiddlewareError::internal(code("WS_INTERNAL"));
        assert_eq!(
            error.to_string(),
            "WebSocket middleware Internal with code WS_INTERNAL"
        );
    }

    #[test]
    fn connection_ledger_is_monotonic_and_cleanup_is_claimed_once() {
        let ledger = WsConnectionLedger::new();
        assert_eq!(ledger.entered(), 0);
        assert!(!ledger.cleanup_started());
        let debug = format!("{ledger:?}");
        assert!(debug.contains("entered: 0"));
        assert!(debug.contains("cleanup_started: false"));

        assert!(!ledger.record_entered(2));
        assert!(ledger.record_entered(1));
        assert_eq!(ledger.entered(), 1);
        assert!(!ledger.record_entered(1));
        assert_eq!(ledger.claim_cleanup(), Some(1));
        assert_eq!(ledger.entered(), 0);
        assert!(ledger.cleanup_started());
        assert!(!ledger.record_entered(1));
        assert_eq!(ledger.claim_cleanup(), None);
        assert_eq!(ledger.with_state(|state| state.entries.len()), 1);
    }

    #[test]
    fn shared_connection_ledger_has_one_cleanup_owner_under_racing_claims() {
        let ledger = WsConnectionLedger::new();
        assert!(ledger.record_entered(1));
        let barrier = Arc::new(std::sync::Barrier::new(8));
        let claims = std::thread::scope(|scope| {
            let workers = (0..8)
                .map(|_| {
                    let ledger = ledger.clone();
                    let barrier = Arc::clone(&barrier);
                    scope.spawn(move || {
                        barrier.wait();
                        ledger.claim_cleanup()
                    })
                })
                .collect::<Vec<_>>();
            workers
                .into_iter()
                .filter_map(|worker| worker.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(claims, [1]);
        assert_eq!(ledger.with_state(|state| state.entries.len()), 1);
        assert!(!ledger.record_entered(2));
    }

    #[tokio::test]
    async fn handshake_chain_is_ordered_and_stops_at_rejection() {
        let log = Arc::new(StdMutex::new(Vec::new()));
        let rejection = unauthorized_rejection(code("AUTH_REQUIRED"));
        let chain = CompiledWsHandshakeChain::compile(vec![
            Arc::new(HandshakeProbe {
                name: "first",
                label: "first",
                log: log.clone(),
                rejection: None,
            }),
            Arc::new(HandshakeProbe {
                name: "second",
                label: "second",
                log: log.clone(),
                rejection: Some(rejection.clone()),
            }),
            Arc::new(HandshakeProbe {
                name: "third",
                label: "third",
                log: log.clone(),
                rejection: None,
            }),
        ])
        .unwrap();
        assert!(!chain.is_empty());
        let request = handshake_request("test", WsHeaders::new(), "127.0.0.1:1".parse().unwrap());
        let failure = execute_handshake_chain(&chain, request).await.unwrap_err();
        assert_eq!(failure.descriptor().name(), "second");
        assert_eq!(
            failure.kind(),
            &WsHandshakeExecutionFailureKind::Rejected(rejection)
        );
        assert_eq!(*log.lock().unwrap(), ["first", "second"]);
    }

    #[tokio::test]
    async fn handshake_observations_exclude_dynamic_request_data() {
        let observer = Arc::new(RecordingObserver::default());
        let observer_dyn: Arc<dyn WsMiddlewareObserver> = observer.clone();
        let log = Arc::new(StdMutex::new(Vec::new()));
        let chain = CompiledWsHandshakeChain::compile_observed(
            vec![
                Arc::new(HandshakeProbe {
                    name: "origin_check",
                    label: "first",
                    log: log.clone(),
                    rejection: None,
                }),
                Arc::new(HandshakeProbe {
                    name: "auth_check",
                    label: "second",
                    log,
                    rejection: Some(unauthorized_rejection(code("AUTH_REQUIRED"))),
                }),
            ],
            observer_dyn,
        )
        .unwrap();
        let mut headers = WsHeaders::new();
        headers.origin = Some("https://private.example".to_string());
        headers
            .set_custom_header(
                "authorization".to_string(),
                "Bearer super-secret".to_string(),
            )
            .unwrap();
        let request = handshake_request(
            "/private-tenant",
            headers,
            "127.0.0.1:4321".parse().unwrap(),
        );

        assert!(execute_handshake_chain(&chain, request).await.is_err());

        let observations = observer
            .observations
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert_eq!(
            *observations,
            [
                ("origin_check", WsMiddlewareObservationOutcome::Completed),
                ("auth_check", WsMiddlewareObservationOutcome::Rejected),
            ]
        );
        let rendered = format!("{observations:?}");
        assert!(!rendered.contains("super-secret"));
        assert!(!rendered.contains("private.example"));
        assert!(!rendered.contains("private-tenant"));
        assert!(!rendered.contains("127.0.0.1"));
    }

    #[test]
    fn observation_outcomes_are_a_closed_bounded_label_set() {
        assert_eq!(
            [
                WsMiddlewareObservationOutcome::Completed,
                WsMiddlewareObservationOutcome::Rejected,
                WsMiddlewareObservationOutcome::Closed,
                WsMiddlewareObservationOutcome::Timeout,
                WsMiddlewareObservationOutcome::Cancelled,
                WsMiddlewareObservationOutcome::Internal,
            ]
            .map(WsMiddlewareObservationOutcome::as_str),
            [
                "completed",
                "rejected",
                "closed",
                "timeout",
                "cancelled",
                "internal",
            ]
        );
    }

    #[tokio::test]
    async fn identity_anonymous_success_is_completed_and_observed() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (result, observer) = execute_identity_middleware(
            IdentityBehavior::Anonymous,
            Arc::clone(&calls),
            CancellationToken::new(),
            Duration::from_secs(1),
        )
        .await;

        let identity = result.expect("anonymous identity succeeds");
        let (snapshot, connection_locals) = identity.into_parts();
        assert!(snapshot.principal().is_none());
        assert!(connection_locals.is_empty());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *observer
                .observations
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
            [("identity_probe", WsMiddlewareObservationOutcome::Completed)]
        );
    }

    #[test]
    fn identity_debug_redacts_principal_expiry_and_connection_local_values() {
        const SUBJECT: &str = "LILY_SECRET_IDENTITY_SUBJECT";
        const ROLE: &str = "LILY_SECRET_IDENTITY_ROLE";
        const SCOPE: &str = "LILY_SECRET_IDENTITY_SCOPE";
        const CLAIM: &str = "LILY_SECRET_IDENTITY_CLAIM";
        const LOCAL: &str = "LILY_SECRET_CONNECTION_LOCAL";

        let mut claims = serde_json::Map::new();
        claims.insert("tenant".to_owned(), serde_json::json!(CLAIM));
        let principal = Principal::new(SUBJECT, [ROLE.to_owned()], [SCOPE.to_owned()], claims);
        let authenticated = AuthenticatedWebSocketIdentity::try_new(principal)
            .expect("bounded fixture principal")
            .expires_at(tokio::time::Instant::now() + Duration::from_secs(30));
        let mut identity = WebSocketIdentity::authenticated(authenticated);
        identity
            .insert_connection_local(LOCAL.to_owned())
            .expect("one fixture local is within the bound");

        let rendered = format!("{identity:?}");
        for secret in [SUBJECT, ROLE, SCOPE, CLAIM, LOCAL] {
            assert!(!rendered.contains(secret));
        }
        assert!(rendered.contains("has_principal: true"));
        assert!(rendered.contains("has_expiry: true"));
        assert!(rendered.contains("connection_local_count: 1"));

        let (snapshot, connection_locals) = identity.into_parts();
        assert_eq!(snapshot.principal().map(Principal::subject), Some(SUBJECT));
        assert_eq!(
            connection_locals.get::<String>().map(String::as_str),
            Some(LOCAL)
        );
    }

    #[tokio::test]
    async fn identity_timeout_is_bounded_secret_safe_and_observed() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (result, observer) = execute_identity_middleware(
            IdentityBehavior::Pending,
            Arc::clone(&calls),
            CancellationToken::new(),
            Duration::from_millis(5),
        )
        .await;

        let failure = result.expect_err("pending identity must time out");
        assert_eq!(failure.kind(), &WsHandshakeExecutionFailureKind::Timeout);
        assert_eq!(failure.descriptor().name(), "identity_probe");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!format!("{failure:?}").contains("LILY_SECRET_IDENTITY_CREDENTIAL"));
        assert_eq!(
            *observer
                .observations
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
            [("identity_probe", WsMiddlewareObservationOutcome::Timeout)]
        );
    }

    #[tokio::test]
    async fn pre_cancelled_identity_is_not_polled_and_is_observed_as_cancelled() {
        let calls = Arc::new(AtomicUsize::new(0));
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let (result, observer) = execute_identity_middleware(
            IdentityBehavior::Pending,
            Arc::clone(&calls),
            cancellation,
            Duration::from_secs(1),
        )
        .await;

        let failure = result.expect_err("pre-cancelled identity must fail closed");
        assert_eq!(failure.kind(), &WsHandshakeExecutionFailureKind::Cancelled);
        assert_eq!(failure.descriptor().name(), "identity_probe");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(!format!("{failure:?}").contains("LILY_SECRET_IDENTITY_CREDENTIAL"));
        assert_eq!(
            *observer
                .observations
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
            [("identity_probe", WsMiddlewareObservationOutcome::Cancelled)]
        );
    }

    #[tokio::test]
    async fn async_handshake_middleware_panic_is_a_bounded_failure() {
        let chain = CompiledWsHandshakeChain::compile(vec![Arc::new(PanickingHandshakeMiddleware)])
            .unwrap();
        let request = handshake_request("test", WsHeaders::new(), "127.0.0.1:1".parse().unwrap());

        let failure = execute_handshake_chain(&chain, request).await.unwrap_err();

        assert_eq!(failure.kind(), &WsHandshakeExecutionFailureKind::Panicked);
        assert_eq!(failure.descriptor().name(), "panicking_middleware");
        assert!(!format!("{failure:?}").contains("LILY_SECRET_ASYNC_HANDSHAKE_PANIC"));
    }

    #[test]
    fn oversized_chain_is_rejected_before_user_descriptor_code() {
        let calls = Arc::new(AtomicUsize::new(0));
        let middleware: Arc<dyn WebSocketHandshakeMiddleware> =
            Arc::new(CountingHandshakeMiddleware(calls.clone()));
        let middlewares = (0..65).map(|_| middleware.clone()).collect();
        assert!(matches!(
            CompiledWsHandshakeChain::compile(middlewares),
            Err(MiddlewareConfigError::TooManyMiddlewares {
                limit: 64,
                actual: 65
            })
        ));
        assert_eq!(calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn connection_cleanup_is_reverse_exactly_once_and_best_effort() {
        let log = Arc::new(StdMutex::new(Vec::new()));
        let chain = CompiledWsConnectionChain::compile(vec![
            connection_probe(
                "a",
                ("a.admit", "a.opened", "a.closed"),
                log.clone(),
                (HookBehavior::Ok, HookBehavior::Ok, HookBehavior::Ok),
            ),
            connection_probe(
                "b",
                ("b.admit", "b.opened", "b.closed"),
                log.clone(),
                (HookBehavior::Ok, HookBehavior::Ok, HookBehavior::Error),
            ),
            connection_probe(
                "c",
                ("c.admit", "c.opened", "c.closed"),
                log.clone(),
                (HookBehavior::Ok, HookBehavior::Ok, HookBehavior::Panic),
            ),
        ])
        .unwrap();
        assert!(!chain.is_empty());
        let ledger = WsConnectionLedger::new();
        let cancellation = CancellationToken::new();
        let context = context();
        chain
            .admit(
                context.clone(),
                &ledger,
                Duration::from_secs(1),
                &cancellation,
            )
            .await
            .unwrap();
        assert_eq!(ledger.entered(), 3);
        chain
            .opened(
                context.clone(),
                &ledger,
                Duration::from_secs(1),
                &cancellation,
            )
            .await
            .unwrap();
        let report = chain
            .cleanup(
                context.clone(),
                &ledger,
                WsConnectionCloseCategory::Reset,
                Duration::from_secs(1),
                &cancellation,
            )
            .await;
        assert!(report.claimed());
        assert_eq!(report.attempted(), 3);
        assert_eq!(report.completed(), 1);
        assert_eq!(report.failed(), 2);
        assert_eq!(report.panicked(), 1);
        assert_eq!(report.timed_out(), 0);
        assert_eq!(report.cancelled(), 0);
        assert!(!report.is_success());
        assert_eq!(
            report.first_failure().unwrap().stage(),
            WsMiddlewareStage::ConnectionClosed
        );
        assert_eq!(
            *log.lock().unwrap(),
            [
                "a.admit", "b.admit", "c.admit", "a.opened", "b.opened", "c.opened", "c.closed",
                "b.closed", "a.closed"
            ]
        );

        let second = chain
            .cleanup(
                context,
                &ledger,
                WsConnectionCloseCategory::InternalError,
                Duration::from_secs(1),
                &cancellation,
            )
            .await;
        assert!(!second.claimed());
        assert_eq!(second.attempted(), 0);
    }

    #[tokio::test]
    async fn opened_failure_stops_forward_progress_and_cleans_every_admitted_entry() {
        let log = Arc::new(StdMutex::new(Vec::new()));
        let chain = CompiledWsConnectionChain::compile(vec![
            connection_probe(
                "a",
                ("a.admit", "a.opened", "a.closed"),
                log.clone(),
                (HookBehavior::Ok, HookBehavior::Ok, HookBehavior::Ok),
            ),
            connection_probe(
                "b",
                ("b.admit", "b.opened", "b.closed"),
                log.clone(),
                (HookBehavior::Ok, HookBehavior::Error, HookBehavior::Ok),
            ),
            connection_probe(
                "c",
                ("c.admit", "c.opened", "c.closed"),
                log.clone(),
                (HookBehavior::Ok, HookBehavior::Ok, HookBehavior::Ok),
            ),
        ])
        .unwrap();
        let ledger = WsConnectionLedger::new();
        let cancellation = CancellationToken::new();
        let context = context();
        chain
            .admit(
                context.clone(),
                &ledger,
                Duration::from_secs(1),
                &cancellation,
            )
            .await
            .unwrap();
        let failure = chain
            .opened(
                context.clone(),
                &ledger,
                Duration::from_secs(1),
                &cancellation,
            )
            .await
            .unwrap_err();
        assert_eq!(failure.descriptor().name(), "b");
        assert_eq!(failure.stage(), WsMiddlewareStage::ConnectionOpened);

        let cleanup = chain
            .cleanup(
                context,
                &ledger,
                WsConnectionCloseCategory::MiddlewareError,
                Duration::from_secs(1),
                &cancellation,
            )
            .await;
        assert_eq!(cleanup.completed(), 3);
        assert_eq!(
            *log.lock().unwrap(),
            [
                "a.admit", "b.admit", "c.admit", "a.opened", "b.opened", "c.closed", "b.closed",
                "a.closed"
            ]
        );
    }

    #[tokio::test]
    async fn admission_timeout_and_cancellation_preserve_shared_prefix() {
        let log = Arc::new(StdMutex::new(Vec::new()));
        let observer = Arc::new(RecordingObserver::default());
        let observer_dyn: Arc<dyn WsMiddlewareObserver> = observer.clone();
        let chain = CompiledWsConnectionChain::compile_observed(
            vec![
                connection_probe(
                    "entered",
                    ("entered.admit", "entered.opened", "entered.closed"),
                    log.clone(),
                    (HookBehavior::Ok, HookBehavior::Ok, HookBehavior::Ok),
                ),
                connection_probe(
                    "pending",
                    ("pending.admit", "pending.opened", "pending.closed"),
                    log,
                    (HookBehavior::Pending, HookBehavior::Ok, HookBehavior::Ok),
                ),
            ],
            observer_dyn,
        )
        .unwrap();
        let ledger = WsConnectionLedger::new();
        let failure = chain
            .admit(
                context(),
                &ledger,
                Duration::from_millis(10),
                &CancellationToken::new(),
            )
            .await
            .unwrap_err();
        assert_eq!(failure.stage(), WsMiddlewareStage::ConnectionAdmission);
        assert_eq!(failure.descriptor().name(), "pending");
        assert_eq!(failure.error().kind(), WsMiddlewareFailureKind::Timeout);
        assert_eq!(ledger.entered(), 1);

        let cancelled = CancellationToken::new();
        cancelled.cancel();
        let empty = WsConnectionLedger::new();
        let failure = chain
            .admit(context(), &empty, Duration::from_secs(1), &cancelled)
            .await
            .unwrap_err();
        assert_eq!(failure.error().kind(), WsMiddlewareFailureKind::Cancelled);
        assert_eq!(empty.entered(), 0);
        assert_eq!(
            *observer
                .observations
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
            [
                ("entered", WsMiddlewareObservationOutcome::Completed),
                ("pending", WsMiddlewareObservationOutcome::Timeout),
                ("entered", WsMiddlewareObservationOutcome::Cancelled),
            ]
        );
    }

    #[tokio::test]
    async fn message_rejection_unwinds_only_the_entered_prefix() {
        let log = Arc::new(StdMutex::new(Vec::new()));
        let make = |name, before_label, after_label, before| {
            Arc::new(MessageProbe {
                name,
                before_label,
                after_label,
                log: log.clone(),
                before,
                after: Ok(WsMessageDecision::Continue),
            }) as Arc<dyn WsMessageMiddleware>
        };
        let chain = CompiledWsMessageChain::compile(vec![
            make("a", "a.before", "a.after", WsMessageDecision::Continue),
            make(
                "b",
                "b.before",
                "b.after",
                WsMessageDecision::Reject(WsProtocolErrorCode::AuthorizationDenied),
            ),
            make("c", "c.before", "c.after", WsMessageDecision::Continue),
        ])
        .unwrap();
        assert!(!chain.is_empty());
        let cancellation = CancellationToken::new();
        let mut exchange = message_exchange(cancellation.clone());
        let (mut ledger, decision) = chain.before(&mut exchange).await;
        assert_eq!(ledger.entered(), 1);
        assert_eq!(
            decision.unwrap(),
            WsMessageDecision::Reject(WsProtocolErrorCode::AuthorizationDenied)
        );
        let report = chain
            .after(
                &mut exchange,
                &mut ledger,
                WsMessageOutcome::Rejected(WsProtocolErrorCode::AuthorizationDenied),
            )
            .await;
        assert_eq!(report.attempted(), 1);
        assert_eq!(report.failed(), 0);
        assert_eq!(
            report.outcome(),
            WsMessageOutcome::Rejected(WsProtocolErrorCode::AuthorizationDenied)
        );
        assert_eq!(*log.lock().unwrap(), ["a.before", "b.before", "a.after"]);
    }

    #[tokio::test]
    async fn message_after_is_reverse_best_effort_and_close_is_authoritative() {
        let log = Arc::new(StdMutex::new(Vec::new()));
        let chain = CompiledWsMessageChain::compile(vec![
            Arc::new(MessageProbe {
                name: "a",
                before_label: "a.before",
                after_label: "a.after",
                log: log.clone(),
                before: WsMessageDecision::Continue,
                after: Ok(WsMessageDecision::Close(WsCloseReason::InternalFailure)),
            }),
            Arc::new(MessageProbe {
                name: "b",
                before_label: "b.before",
                after_label: "b.after",
                log: log.clone(),
                before: WsMessageDecision::Continue,
                after: Ok(WsMessageDecision::Close(WsCloseReason::PolicyViolation)),
            }),
            Arc::new(MessageProbe {
                name: "c",
                before_label: "c.before",
                after_label: "c.after",
                log: log.clone(),
                before: WsMessageDecision::Continue,
                after: Err(WsMiddlewareError::internal(code("AFTER_FAILED"))),
            }),
        ])
        .unwrap();
        let cancellation = CancellationToken::new();
        let mut exchange = message_exchange(cancellation.clone());
        let (mut ledger, decision) = chain.before(&mut exchange).await;
        assert_eq!(decision.unwrap(), WsMessageDecision::Continue);
        let report = chain
            .after(&mut exchange, &mut ledger, WsMessageOutcome::Handled)
            .await;
        assert_eq!(report.failed(), 1);
        assert!(report.first_failure().is_some());
        assert_eq!(
            report.outcome(),
            WsMessageOutcome::Close(WsCloseReason::PolicyViolation)
        );
        assert_eq!(
            *log.lock().unwrap(),
            [
                "a.before", "b.before", "c.before", "c.after", "b.after", "a.after"
            ]
        );
    }

    #[test]
    fn message_after_decision_merge_is_first_close_wins() {
        let rejected = WsMessageOutcome::Rejected(WsProtocolErrorCode::AuthorizationDenied);
        let failed = WsMessageOutcome::Failed(MiddlewareErrorCode::INTERNAL);

        assert_eq!(
            merge_message_after_decision(
                WsMessageOutcome::Handled,
                WsMessageDecision::Reject(WsProtocolErrorCode::MiddlewareRejected),
            ),
            WsMessageOutcome::Rejected(WsProtocolErrorCode::MiddlewareRejected)
        );
        assert_eq!(
            merge_message_after_decision(
                rejected,
                WsMessageDecision::Reject(WsProtocolErrorCode::MiddlewareRejected),
            ),
            rejected
        );
        assert_eq!(
            merge_message_after_decision(failed, WsMessageDecision::Continue),
            failed
        );

        for outcome in [WsMessageOutcome::Handled, rejected, failed] {
            assert_eq!(
                merge_message_after_decision(
                    outcome,
                    WsMessageDecision::Close(WsCloseReason::PolicyViolation),
                ),
                WsMessageOutcome::Close(WsCloseReason::PolicyViolation)
            );
        }

        let first_close = WsMessageOutcome::Close(WsCloseReason::PolicyViolation);
        assert_eq!(
            merge_message_after_decision(
                first_close,
                WsMessageDecision::Close(WsCloseReason::InternalFailure),
            ),
            first_close
        );
        assert_eq!(
            merge_message_after_decision(first_close, WsMessageDecision::Continue),
            first_close
        );
        assert_eq!(
            merge_message_after_decision(
                first_close,
                WsMessageDecision::Reject(WsProtocolErrorCode::MiddlewareRejected),
            ),
            first_close
        );
    }

    #[tokio::test]
    async fn normal_after_user_cancellation_error_preserves_an_existing_policy_close() {
        let log = Arc::new(StdMutex::new(Vec::new()));
        let middleware = |name, after_label, after| {
            Arc::new(MessageProbe {
                name,
                before_label: "before",
                after_label,
                log: Arc::clone(&log),
                before: WsMessageDecision::Continue,
                after,
            }) as Arc<dyn WsMessageMiddleware>
        };
        let chain = CompiledWsMessageChain::compile(vec![
            middleware(
                "outer_close",
                "outer.close",
                Ok(WsMessageDecision::Close(WsCloseReason::InternalFailure)),
            ),
            middleware(
                "cancelled",
                "middle.cancelled",
                Err(WsMiddlewareError::cancelled()),
            ),
            middleware(
                "inner_close",
                "inner.close",
                Ok(WsMessageDecision::Close(WsCloseReason::PolicyViolation)),
            ),
        ])
        .unwrap();
        let cancellation = CancellationToken::new();
        let mut exchange = message_exchange(cancellation.clone());
        let (mut ledger, decision) = chain.before(&mut exchange).await;
        assert_eq!(decision.unwrap(), WsMessageDecision::Continue);

        let report = chain
            .after(&mut exchange, &mut ledger, WsMessageOutcome::Handled)
            .await;
        assert_eq!(report.attempted(), 3);
        assert_eq!(report.failed(), 1);
        assert_eq!(
            report.first_failure().unwrap().error().kind(),
            WsMiddlewareFailureKind::Cancelled
        );
        assert_eq!(
            report.outcome(),
            WsMessageOutcome::Close(WsCloseReason::PolicyViolation)
        );
        assert_eq!(
            *log.lock().unwrap(),
            [
                "before",
                "before",
                "before",
                "inner.close",
                "middle.cancelled",
                "outer.close",
            ]
        );
    }

    #[tokio::test]
    async fn normal_reverse_timeout_leaves_outer_exit_for_termination() {
        let chain = CompiledWsMessageChain::compile(vec![
            Arc::new(HangingAfter { name: "outer" }),
            Arc::new(HangingAfter { name: "inner" }),
        ])
        .unwrap();
        let cancellation = CancellationToken::new();
        let mut exchange = message_exchange(cancellation.clone());
        exchange.deadline = tokio::time::Instant::now() + Duration::from_millis(25);
        let (mut ledger, decision) = chain.before(&mut exchange).await;
        assert_eq!(decision.unwrap(), WsMessageDecision::Continue);

        let started = tokio::time::Instant::now();
        let report = chain
            .after(&mut exchange, &mut ledger, WsMessageOutcome::Handled)
            .await;
        assert_eq!(report.attempted(), 1);
        assert_eq!(report.failed(), 1);
        assert_eq!(
            ledger.lifecycle.entries[0].normal.state,
            crate::lifecycle::LifecycleInvocationState::Pending
        );
        assert!(matches!(
            ledger.lifecycle.entries[1].normal.state,
            crate::lifecycle::LifecycleInvocationState::Terminal {
                outcome: LifecycleOutcome::Interrupted(LifecycleInterruption::TimedOut),
                started: true,
            }
        ));
        assert!(
            started.elapsed() < Duration::from_millis(45),
            "reverse hooks received more than one aggregate budget"
        );
    }

    #[tokio::test]
    async fn returned_timeout_or_cancel_error_is_a_completed_invocation_with_failure() {
        for error in [WsMiddlewareError::timeout(), WsMiddlewareError::cancelled()] {
            let cancellation = CancellationToken::new();
            let chain = CompiledWsMessageChain::compile(vec![Arc::new(MessageProbe {
                name: "returned_error",
                before_label: "before",
                after_label: "after",
                log: Arc::new(StdMutex::new(Vec::new())),
                before: WsMessageDecision::Continue,
                after: Err(error),
            })])
            .unwrap();
            let mut exchange = message_exchange(cancellation.clone());
            let (mut ledger, decision) = chain.before(&mut exchange).await;
            assert_eq!(decision.unwrap(), WsMessageDecision::Continue);
            let report = chain
                .after(&mut exchange, &mut ledger, WsMessageOutcome::Handled)
                .await;
            assert_eq!(report.failed(), 1);
            let entry = &ledger.lifecycle.entries[0];
            assert_eq!(
                entry.normal.state,
                crate::lifecycle::LifecycleInvocationState::Terminal {
                    outcome: LifecycleOutcome::Failed,
                    started: true,
                }
            );
            assert!(!entry.normal.cancellation_requested);
            assert!(
                !ledger
                    .lifecycle
                    .claim_exit(0, LifecycleExitPath::Termination)
            );
        }
    }

    #[tokio::test]
    async fn execution_cancellation_keeps_cleanup_completion_and_timeout_evidence_independent() {
        let log = Arc::new(StdMutex::new(Vec::new()));
        let chain = CompiledWsConnectionChain::compile(vec![
            connection_probe(
                "outer",
                ("outer.admit", "outer.opened", "outer.closed"),
                Arc::clone(&log),
                (HookBehavior::Ok, HookBehavior::Ok, HookBehavior::Ok),
            ),
            connection_probe(
                "inner",
                ("inner.admit", "inner.opened", "inner.closed"),
                Arc::clone(&log),
                (HookBehavior::Ok, HookBehavior::Ok, HookBehavior::Pending),
            ),
        ])
        .unwrap();
        let ledger = WsConnectionLedger::new();
        let cancellation = CancellationToken::new();
        let context = context();
        chain
            .admit(
                Arc::clone(&context),
                &ledger,
                Duration::from_secs(1),
                &cancellation,
            )
            .await
            .unwrap();
        cancellation.cancel();
        let report = chain
            .cleanup(
                context,
                &ledger,
                WsConnectionCloseCategory::Cancelled,
                Duration::from_millis(5),
                &CancellationToken::new(),
            )
            .await;
        assert_eq!(report.completed(), 1);
        assert_eq!(report.cancelled(), 0);
        assert_eq!(report.timed_out(), 1);
        ledger.with_state(|state| {
            assert_eq!(state.entries.len(), 2);
            for (index, outcome) in [
                LifecycleOutcome::Completed,
                LifecycleOutcome::Interrupted(LifecycleInterruption::TimedOut),
            ]
            .into_iter()
            .enumerate()
            {
                assert_eq!(
                    state.entries[index].termination.state,
                    crate::lifecycle::LifecycleInvocationState::Terminal {
                        outcome,
                        started: true,
                    }
                );
            }
        });
        assert_eq!(
            *log.lock().unwrap(),
            ["outer.admit", "inner.admit", "inner.closed", "outer.closed"]
        );
    }

    #[tokio::test]
    async fn empty_message_chain_has_no_executor_work() {
        let chain = CompiledWsMessageChain::compile(Vec::new()).unwrap();
        assert!(chain.is_empty());
        let cancellation = CancellationToken::new();
        let mut exchange = message_exchange(cancellation.clone());
        let (mut ledger, decision) = chain.before(&mut exchange).await;
        assert_eq!(ledger.entered(), 0);
        assert_eq!(decision.unwrap(), WsMessageDecision::Continue);
        let report = chain
            .after(&mut exchange, &mut ledger, WsMessageOutcome::Handled)
            .await;
        assert_eq!(report.attempted(), 0);
        assert_eq!(report.outcome(), WsMessageOutcome::Handled);
    }
}
