//! Typed, transport-independent HTTP middleware contract.
//!
//! The HTTP adapter owns chain construction and execution. Middleware owns
//! only immutable metadata, startup validation and its around-style behavior.

use async_trait::async_trait;
use lily_injection::{Extensions, InjectionError};
pub use lily_web_core::{
    HttpErrorCode as MiddlewareErrorCode, HttpRejection as HttpMiddlewareRejection,
};
use lily_web_core::{Request, Response};
use std::time::Duration;
use std::{fmt, sync::Arc};

/// Maximum number of HTTP middleware entries accepted by one application.
///
/// Keeping this bound in the contract makes chain depth, startup work and
/// per-middleware metric cardinality deterministic.
pub const MAX_HTTP_MIDDLEWARES: usize = 64;

const MAX_DESCRIPTOR_LABEL_BYTES: usize = 64;

/// Stable category used for bounded diagnostics and metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum MiddlewareKind {
    /// Request-rate enforcement.
    RateLimit,
    /// Cross-site request forgery enforcement.
    Csrf,
    /// WebSocket HTTP Upgrade policy.
    WebSocketHandshake,
    /// WebSocket connection-lifecycle middleware.
    WebSocketConnection,
    /// WebSocket application-message middleware.
    WebSocketMessage,
    /// Application-defined middleware without a more specific category.
    Custom,
}

impl MiddlewareKind {
    /// Bounded metric label for this category.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RateLimit => "rate_limit",
            Self::Csrf => "csrf",
            Self::WebSocketHandshake => "websocket_handshake",
            Self::WebSocketConnection => "websocket_connection",
            Self::WebSocketMessage => "websocket_message",
            Self::Custom => "custom",
        }
    }
}

/// Immutable metadata collected once while building an application.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MiddlewareDescriptor {
    name: &'static str,
    kind: MiddlewareKind,
    exclusive_key: Option<&'static str>,
}

impl MiddlewareDescriptor {
    /// Creates descriptor metadata with no exclusivity constraint.
    #[must_use]
    pub const fn new(name: &'static str, kind: MiddlewareKind) -> Self {
        Self {
            name,
            kind,
            exclusive_key: None,
        }
    }

    /// Marks this middleware as mutually exclusive with another descriptor
    /// carrying the same key.
    #[must_use]
    pub const fn with_exclusive_key(mut self, exclusive_key: &'static str) -> Self {
        self.exclusive_key = Some(exclusive_key);
        self
    }

    /// Returns the bounded, application-selected middleware identity.
    #[must_use]
    pub const fn name(self) -> &'static str {
        self.name
    }

    /// Returns the stable middleware category.
    #[must_use]
    pub const fn kind(self) -> MiddlewareKind {
        self.kind
    }

    /// Returns the optional key used to reject mutually exclusive entries.
    #[must_use]
    pub const fn exclusive_key(self) -> Option<&'static str> {
        self.exclusive_key
    }
}

/// Startup-time middleware configuration failure.
///
/// This type retains only bounded codes, counts and indices. Descriptor text
/// and application data are never copied into `Debug` or `Display` output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum MiddlewareConfigError {
    /// The configured middleware chain exceeds its deterministic limit.
    TooManyMiddlewares {
        /// Maximum accepted entry count.
        limit: usize,
        /// Configured entry count.
        actual: usize,
    },
    /// A descriptor has no name.
    EmptyDescriptorName {
        /// Position in the validated descriptor sequence.
        index: usize,
    },
    /// A descriptor name exceeds the bounded-label byte limit.
    DescriptorNameTooLong {
        /// Position in the validated descriptor sequence.
        index: usize,
        /// Maximum accepted byte length.
        limit_bytes: usize,
        /// Configured byte length.
        actual_bytes: usize,
    },
    /// A descriptor name is not a valid bounded metric label.
    InvalidDescriptorName {
        /// Position in the validated descriptor sequence.
        index: usize,
    },
    /// A descriptor supplied an empty exclusivity key.
    EmptyExclusiveKey {
        /// Position in the validated descriptor sequence.
        index: usize,
    },
    /// An exclusivity key exceeds the bounded-label byte limit.
    ExclusiveKeyTooLong {
        /// Position in the validated descriptor sequence.
        index: usize,
        /// Maximum accepted byte length.
        limit_bytes: usize,
        /// Configured byte length.
        actual_bytes: usize,
    },
    /// An exclusivity key is not a valid bounded label.
    InvalidExclusiveKey {
        /// Position in the validated descriptor sequence.
        index: usize,
    },
    /// Two descriptors claim the same mutually exclusive capability.
    DuplicateExclusiveKey {
        /// Position of the first claimant.
        first_index: usize,
        /// Position of the duplicate claimant.
        duplicate_index: usize,
    },
    /// A transport-specific middleware policy selected an unsupported HTTP
    /// rejection status during startup validation.
    InvalidRejectionStatus {
        /// Unsupported status selected by the policy.
        status: u16,
    },
    /// A middleware-specific validation failure represented by a bounded code.
    Middleware {
        /// Secret-safe diagnostic code.
        code: MiddlewareErrorCode,
    },
}

impl MiddlewareConfigError {
    /// Creates a middleware-specific, bounded startup failure.
    #[must_use]
    pub const fn middleware(code: MiddlewareErrorCode) -> Self {
        Self::Middleware { code }
    }

    /// Returns a bounded diagnostic label suitable for startup telemetry.
    #[must_use]
    pub const fn diagnostic_code(self) -> &'static str {
        match self {
            Self::TooManyMiddlewares { .. } => "MIDDLEWARE_TOO_MANY",
            Self::EmptyDescriptorName { .. } => "MIDDLEWARE_NAME_EMPTY",
            Self::DescriptorNameTooLong { .. } => "MIDDLEWARE_NAME_TOO_LONG",
            Self::InvalidDescriptorName { .. } => "MIDDLEWARE_NAME_INVALID",
            Self::EmptyExclusiveKey { .. } => "MIDDLEWARE_EXCLUSIVE_KEY_EMPTY",
            Self::ExclusiveKeyTooLong { .. } => "MIDDLEWARE_EXCLUSIVE_KEY_TOO_LONG",
            Self::InvalidExclusiveKey { .. } => "MIDDLEWARE_EXCLUSIVE_KEY_INVALID",
            Self::DuplicateExclusiveKey { .. } => "MIDDLEWARE_EXCLUSIVE_KEY_DUPLICATE",
            Self::InvalidRejectionStatus { .. } => "MIDDLEWARE_STATUS_INVALID",
            Self::Middleware { code } => code.as_str(),
        }
    }
}

impl fmt::Display for MiddlewareConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooManyMiddlewares { limit, actual } => {
                write!(formatter, "middleware count {actual} exceeds limit {limit}")
            }
            Self::EmptyDescriptorName { index } => {
                write!(formatter, "middleware descriptor {index} has an empty name")
            }
            Self::DescriptorNameTooLong {
                index,
                limit_bytes,
                actual_bytes,
            } => write!(
                formatter,
                "middleware descriptor {index} name has {actual_bytes} bytes; limit is {limit_bytes}"
            ),
            Self::InvalidDescriptorName { index } => write!(
                formatter,
                "middleware descriptor {index} name is not a bounded label"
            ),
            Self::EmptyExclusiveKey { index } => write!(
                formatter,
                "middleware descriptor {index} has an empty exclusive key"
            ),
            Self::ExclusiveKeyTooLong {
                index,
                limit_bytes,
                actual_bytes,
            } => write!(
                formatter,
                "middleware descriptor {index} exclusive key has {actual_bytes} bytes; limit is {limit_bytes}"
            ),
            Self::InvalidExclusiveKey { index } => write!(
                formatter,
                "middleware descriptor {index} exclusive key is not a bounded label"
            ),
            Self::DuplicateExclusiveKey {
                first_index,
                duplicate_index,
            } => write!(
                formatter,
                "middleware descriptors {first_index} and {duplicate_index} use the same exclusive key"
            ),
            Self::InvalidRejectionStatus { status } => {
                write!(formatter, "middleware rejection status {status} is invalid")
            }
            Self::Middleware { code } => {
                write!(
                    formatter,
                    "middleware configuration failed with code {code}"
                )
            }
        }
    }
}

impl std::error::Error for MiddlewareConfigError {}

/// Bounded failure returned while constructing one application middleware.
///
/// Dynamic dependency, configuration and credential values are deliberately
/// not retained. Middleware authors can choose a stable code for actionable
/// startup diagnostics; the blanket [`InjectionError`] conversion retains
/// only framework-owned dependency/lifetime categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum HttpMiddlewareInitError {
    /// Required immutable application configuration is absent.
    ///
    /// The code identifies the missing category without retaining values.
    MissingConfiguration {
        /// Secret-safe category selected by the middleware author.
        code: MiddlewareErrorCode,
    },
    /// Immutable application configuration is invalid.
    InvalidConfiguration {
        /// Secret-safe category selected by the middleware author.
        code: MiddlewareErrorCode,
    },
    /// Dependency resolution or initialization failed.
    Dependency {
        /// Secret-safe dependency failure category.
        code: MiddlewareErrorCode,
    },
    /// Middleware construction failed for an internal reason.
    Internal {
        /// Secret-safe internal failure category.
        code: MiddlewareErrorCode,
    },
}

impl HttpMiddlewareInitError {
    /// Creates a missing-configuration failure with a bounded diagnostic code.
    #[must_use]
    pub const fn missing_configuration(code: MiddlewareErrorCode) -> Self {
        Self::MissingConfiguration { code }
    }

    /// Creates an invalid-configuration failure with a bounded diagnostic code.
    #[must_use]
    pub const fn invalid_configuration(code: MiddlewareErrorCode) -> Self {
        Self::InvalidConfiguration { code }
    }

    /// Creates a dependency failure with a bounded diagnostic code.
    #[must_use]
    pub const fn dependency(code: MiddlewareErrorCode) -> Self {
        Self::Dependency { code }
    }

    /// Creates an internal initialization failure with a bounded diagnostic code.
    #[must_use]
    pub const fn internal(code: MiddlewareErrorCode) -> Self {
        Self::Internal { code }
    }

    /// Returns the bounded diagnostic label selected at construction.
    #[must_use]
    pub const fn diagnostic_code(self) -> &'static str {
        match self {
            Self::MissingConfiguration { code }
            | Self::InvalidConfiguration { code }
            | Self::Dependency { code }
            | Self::Internal { code } => code.as_str(),
        }
    }
}

impl From<InjectionError> for HttpMiddlewareInitError {
    fn from(error: InjectionError) -> Self {
        let code = match error {
            InjectionError::ScopeRequired { .. } => "MIDDLEWARE_SCOPE_REQUIRED",
            InjectionError::LifetimeMismatch { .. } => "MIDDLEWARE_LIFETIME_MISMATCH",
            _ => "MIDDLEWARE_DEPENDENCY",
        };
        Self::Dependency {
            code: MiddlewareErrorCode::new(code)
                .expect("the built-in middleware dependency code is valid"),
        }
    }
}

impl fmt::Display for HttpMiddlewareInitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let category = match self {
            Self::MissingConfiguration { .. } => "missing configuration",
            Self::InvalidConfiguration { .. } => "invalid configuration",
            Self::Dependency { .. } => "dependency failure",
            Self::Internal { .. } => "internal failure",
        };
        write!(
            formatter,
            "middleware initialization {category} ({})",
            self.diagnostic_code()
        )
    }
}

impl std::error::Error for HttpMiddlewareInitError {}

/// Rejects an oversized chain before invoking user-provided descriptor code.
pub fn validate_middleware_count(actual: usize) -> Result<(), MiddlewareConfigError> {
    if actual > MAX_HTTP_MIDDLEWARES {
        return Err(MiddlewareConfigError::TooManyMiddlewares {
            limit: MAX_HTTP_MIDDLEWARES,
            actual,
        });
    }
    Ok(())
}

/// Validates immutable descriptor metadata before application publication.
pub fn validate_middleware_descriptors(
    descriptors: &[MiddlewareDescriptor],
) -> Result<(), MiddlewareConfigError> {
    validate_middleware_count(descriptors.len())?;

    for (index, descriptor) in descriptors.iter().copied().enumerate() {
        validate_descriptor_label(descriptor.name, index, false)?;
        let Some(exclusive_key) = descriptor.exclusive_key else {
            continue;
        };
        validate_descriptor_label(exclusive_key, index, true)?;
        if let Some(first_index) = descriptors[..index]
            .iter()
            .position(|other| other.exclusive_key == Some(exclusive_key))
        {
            return Err(MiddlewareConfigError::DuplicateExclusiveKey {
                first_index,
                duplicate_index: index,
            });
        }
    }
    Ok(())
}

fn validate_descriptor_label(
    label: &'static str,
    index: usize,
    exclusive_key: bool,
) -> Result<(), MiddlewareConfigError> {
    if label.is_empty() {
        return Err(if exclusive_key {
            MiddlewareConfigError::EmptyExclusiveKey { index }
        } else {
            MiddlewareConfigError::EmptyDescriptorName { index }
        });
    }
    if label.len() > MAX_DESCRIPTOR_LABEL_BYTES {
        return Err(if exclusive_key {
            MiddlewareConfigError::ExclusiveKeyTooLong {
                index,
                limit_bytes: MAX_DESCRIPTOR_LABEL_BYTES,
                actual_bytes: label.len(),
            }
        } else {
            MiddlewareConfigError::DescriptorNameTooLong {
                index,
                limit_bytes: MAX_DESCRIPTOR_LABEL_BYTES,
                actual_bytes: label.len(),
            }
        });
    }

    if !is_descriptor_label(label) {
        return Err(if exclusive_key {
            MiddlewareConfigError::InvalidExclusiveKey { index }
        } else {
            MiddlewareConfigError::InvalidDescriptorName { index }
        });
    }
    Ok(())
}

fn is_descriptor_label(label: &str) -> bool {
    label.len() <= MAX_DESCRIPTOR_LABEL_BYTES
        && label
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
}

/// Runtime failure returned by the typed HTTP middleware chain.
///
/// Its representation is private so future failure classes can be added
/// without exposing transport or third-party error types as public API.
#[derive(Debug, PartialEq, Eq)]
pub struct HttpMiddlewareError {
    kind: HttpMiddlewareErrorKind,
}

/// Stable runtime outcome category used by transport observability.
///
/// The category is intentionally independent of the diagnostic code. This
/// lets adapters distinguish rejection, timeout and internal failure without
/// turning a user-selected code or any request data into a metric label.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum HttpMiddlewareFailureKind {
    /// Middleware intentionally short-circuited with a public rejection.
    Rejected,
    /// Middleware exceeded its configured execution deadline.
    Timeout,
    /// Middleware failed without a safe application rejection.
    Internal,
}

impl HttpMiddlewareFailureKind {
    /// Bounded metric label for this failure category.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Rejected => "rejected",
            Self::Timeout => "timeout",
            Self::Internal => "internal",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum HttpMiddlewareErrorKind {
    Rejected(HttpMiddlewareRejection),
    Timeout { code: MiddlewareErrorCode },
    Internal { code: MiddlewareErrorCode },
}

impl HttpMiddlewareError {
    /// Creates an intentional, application-visible short-circuit.
    #[must_use]
    pub fn rejected(rejection: HttpMiddlewareRejection) -> Self {
        Self {
            kind: HttpMiddlewareErrorKind::Rejected(rejection),
        }
    }

    /// Creates a deadline failure with a secret-safe diagnostic code.
    #[must_use]
    pub const fn timeout(code: MiddlewareErrorCode) -> Self {
        Self {
            kind: HttpMiddlewareErrorKind::Timeout { code },
        }
    }

    /// Creates an internal runtime failure with a secret-safe diagnostic code.
    #[must_use]
    pub const fn internal(code: MiddlewareErrorCode) -> Self {
        Self {
            kind: HttpMiddlewareErrorKind::Internal { code },
        }
    }

    /// Returns the bounded failure category without exposing error payloads.
    #[doc(hidden)]
    #[must_use]
    pub const fn kind(&self) -> HttpMiddlewareFailureKind {
        match &self.kind {
            HttpMiddlewareErrorKind::Rejected(_) => HttpMiddlewareFailureKind::Rejected,
            HttpMiddlewareErrorKind::Timeout { .. } => HttpMiddlewareFailureKind::Timeout,
            HttpMiddlewareErrorKind::Internal { .. } => HttpMiddlewareFailureKind::Internal,
        }
    }

    /// Returns the bounded diagnostic code retained for telemetry.
    #[doc(hidden)]
    #[must_use]
    pub const fn diagnostic_code(&self) -> MiddlewareErrorCode {
        match &self.kind {
            HttpMiddlewareErrorKind::Rejected(rejection) => rejection.code(),
            HttpMiddlewareErrorKind::Timeout { code }
            | HttpMiddlewareErrorKind::Internal { code } => *code,
        }
    }

    /// Returns the HTTP status that the transport should publish.
    #[doc(hidden)]
    #[must_use]
    pub const fn http_status(&self) -> u16 {
        match &self.kind {
            HttpMiddlewareErrorKind::Rejected(rejection) => rejection.status(),
            HttpMiddlewareErrorKind::Timeout { .. } => 504,
            HttpMiddlewareErrorKind::Internal { .. } => 500,
        }
    }

    /// Returns an optional retry delay carried by a deliberate rejection.
    #[doc(hidden)]
    #[must_use]
    pub const fn retry_after(&self) -> Option<Duration> {
        match &self.kind {
            HttpMiddlewareErrorKind::Rejected(rejection) => rejection.retry_after(),
            HttpMiddlewareErrorKind::Timeout { .. } | HttpMiddlewareErrorKind::Internal { .. } => {
                None
            }
        }
    }

    /// Returns the bounded message safe to publish in an HTTP response.
    #[doc(hidden)]
    #[must_use]
    pub fn public_message(&self) -> &'static str {
        match &self.kind {
            HttpMiddlewareErrorKind::Rejected(rejection) => rejection.public_message(),
            HttpMiddlewareErrorKind::Timeout { .. } => "Middleware processing timed out.",
            HttpMiddlewareErrorKind::Internal { .. } => "An internal server error occurred.",
        }
    }

    /// Returns the application-defined rejection payload, if this is a
    /// deliberate middleware short-circuit.
    #[doc(hidden)]
    #[must_use]
    pub const fn rejection(&self) -> Option<&HttpMiddlewareRejection> {
        match &self.kind {
            HttpMiddlewareErrorKind::Rejected(rejection) => Some(rejection),
            HttpMiddlewareErrorKind::Timeout { .. } | HttpMiddlewareErrorKind::Internal { .. } => {
                None
            }
        }
    }
}

impl From<HttpMiddlewareRejection> for HttpMiddlewareError {
    fn from(rejection: HttpMiddlewareRejection) -> Self {
        Self::rejected(rejection)
    }
}

impl fmt::Display for HttpMiddlewareError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.kind {
            HttpMiddlewareErrorKind::Rejected(rejection) => rejection.fmt(formatter),
            HttpMiddlewareErrorKind::Timeout { code } => {
                write!(formatter, "HTTP middleware timed out with code {code}")
            }
            HttpMiddlewareErrorKind::Internal { code } => {
                write!(formatter, "HTTP middleware failed with code {code}")
            }
        }
    }
}

impl std::error::Error for HttpMiddlewareError {}

/// Request/response pair passed through the around-style middleware chain.
pub struct HttpExchange<'a> {
    request: &'a mut Request,
    response: &'a mut Response,
    termination_state: Option<&'a mut lily_web_core::RequestExtensions>,
    invocation_id: Option<usize>,
}

impl<'a> HttpExchange<'a> {
    /// Observes this request's normal execution cancellation. Middleware's
    /// explicit callback parameter refers to the same authority.
    pub fn execution_cancellation(&self) -> lily_cancellation::ExecutionCancellation {
        self.request.execution_cancellation()
    }

    /// Creates the executor-owned request/response exchange.
    #[doc(hidden)]
    #[must_use]
    pub fn new(request: &'a mut Request, response: &'a mut Response) -> Self {
        Self {
            request,
            response,
            termination_state: None,
            invocation_id: None,
        }
    }

    /// Framework seam for an owner-retained middleware invocation.
    #[doc(hidden)]
    pub fn with_termination_state(
        request: &'a mut Request,
        response: &'a mut Response,
        state: &'a mut lily_web_core::RequestExtensions,
        invocation_id: usize,
    ) -> Self {
        Self {
            request,
            response,
            termination_state: Some(state),
            invocation_id: Some(invocation_id),
        }
    }

    /// Identity of this middleware invocation within the managed request.
    /// Direct calls without an owner return `None`.
    pub fn invocation_id(&self) -> Option<usize> {
        self.invocation_id
    }

    /// State belonging only to this invocation, retained outside `handle` and
    /// independent of `Request.local`. Managed HTTP callbacks always have a
    /// store; a direct call without a request lifecycle owner returns `None`.
    ///
    /// Store cleanup data before cancellable work. Do not retain request body
    /// readers or response producers here; their release is a cleanup
    /// prerequisite. Normal return does not invoke abnormal cleanup.
    pub fn termination_state_mut(&mut self) -> Option<&mut lily_web_core::RequestExtensions> {
        self.termination_state.as_deref_mut()
    }

    /// Borrows the current request without allowing mutation.
    #[must_use]
    pub fn request(&self) -> &Request {
        self.request
    }

    /// Borrows the current request mutably.
    #[must_use]
    pub fn request_mut(&mut self) -> &mut Request {
        self.request
    }

    /// Borrows the response accumulated by the pipeline.
    #[must_use]
    pub fn response(&self) -> &Response {
        self.response
    }

    /// Borrows the response mutably for metadata or representation changes.
    #[must_use]
    pub fn response_mut(&mut self) -> &mut Response {
        self.response
    }

    /// Borrows both transport parts for the terminal HTTP service.
    ///
    /// This is an executor integration seam. Middleware should normally use
    /// the focused request/response accessors above.
    #[doc(hidden)]
    #[must_use]
    pub fn parts_mut(&mut self) -> (&mut Request, &mut Response) {
        (&mut *self.request, &mut *self.response)
    }
}

/// Adapter seam implemented by the HTTP executor for the remaining chain.
#[doc(hidden)]
#[async_trait]
pub trait HttpNextService: Send + Sync {
    /// Executes the remaining middleware chain and terminal action.
    async fn run(&self, exchange: &mut HttpExchange<'_>) -> Result<(), HttpMiddlewareError>;
}

/// Capability passed to middleware for invoking the remaining chain once.
pub struct HttpNext<'a> {
    service: &'a dyn HttpNextService,
}

impl<'a> HttpNext<'a> {
    #[doc(hidden)]
    #[must_use]
    pub const fn new(service: &'a dyn HttpNextService) -> Self {
        Self { service }
    }

    /// Executes the remaining chain exactly once.
    ///
    /// Middleware may return without calling this method to short-circuit.
    pub async fn run(self, exchange: &mut HttpExchange<'_>) -> Result<(), HttpMiddlewareError> {
        self.service.run(exchange).await
    }
}

/// Around-style HTTP middleware contract.
///
/// The HTTP application calls [`HttpMiddleware::new`] exactly once after its
/// immutable DI graph is available and before a listener is opened. The
/// returned value is retained as one application-wide instance and shared by
/// every request.
///
/// Resolving a scoped service in `new` fails because no request
/// [`lily_injection::ProcessContext`] is active. Resolving a transient in
/// `new` and retaining it in `self` makes that particular value effectively
/// application-lived. Middleware that needs request-scoped or per-request
/// transient state should retain the provider and resolve from `handle` while
/// the request context is active. Lily deliberately does not inspect or
/// rewrite application lifetime choices.
///
/// Calling `next.run(exchange).await` continues the chain. Returning without
/// calling it is an explicit short-circuit; [`HttpMiddlewareError::rejected`]
/// carries the bounded HTTP response contract. Code before and after
/// `next.run` naturally provides forward-enter/reverse-exit ordering.
#[async_trait]
pub trait HttpMiddleware: Send + Sync + 'static {
    /// Constructs this application's single middleware instance from the
    /// application-owned service provider.
    async fn new(extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError>
    where
        Self: Sized;

    /// Returns immutable identity and exclusivity metadata for this instance.
    fn descriptor(&self) -> MiddlewareDescriptor;

    /// Validates immutable middleware configuration during application build.
    fn validate(&self) -> Result<(), MiddlewareConfigError> {
        Ok(())
    }

    /// Processes one request and optionally invokes the remaining chain.
    ///
    /// `cancellation` is the same read-only authority exposed by
    /// [`HttpExchange::execution_cancellation`]. Both before and normal after
    /// code belong to execution. The owner keeps polling signalled execution
    /// within a bounded window; this signal never cancels DI cleanup.
    async fn handle(
        &self,
        exchange: &mut HttpExchange<'_>,
        next: HttpNext<'_>,
        cancellation: lily_cancellation::ExecutionCancellation,
    ) -> Result<(), HttpMiddlewareError>;

    /// Best-effort cleanup of a first-polled invocation that did not return
    /// normally (including a contained panic). Typed errors count as normal
    /// returns. Eligible invocations run serially in reverse order after the
    /// execution/body/input prerequisites terminate, before request DI closes.
    ///
    /// The default is a no-op. Implementations must tolerate partial effects
    /// and interruption of this callback itself. It is invoked at most once,
    /// never started after its budget expires, and never retried after failure.
    /// The signal is independent of execution and sibling invocations.
    async fn on_request_termination(
        &self,
        _context: &mut crate::HttpRequestTerminationContext<'_>,
        _cancellation: lily_web_core::CleanupCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lily_web_core::{HttpRejectionError, MAX_HTTP_ERROR_CODE_BYTES};
    use std::sync::{Arc, Mutex};

    #[test]
    fn codes_are_static_bounded_labels() {
        let auth_required = MiddlewareErrorCode::new("AUTH_REQUIRED").unwrap();
        assert_eq!(auth_required.as_str(), "AUTH_REQUIRED");
        assert_eq!(auth_required.to_string(), "AUTH_REQUIRED");
        assert_eq!(
            MiddlewareErrorCode::new("").unwrap_err(),
            HttpRejectionError::EmptyErrorCode
        );
        assert_eq!(
            MiddlewareErrorCode::new("lower-case").unwrap_err(),
            HttpRejectionError::InvalidErrorCode
        );
        assert_eq!(
            MiddlewareErrorCode::new("1INVALID").unwrap_err(),
            HttpRejectionError::InvalidErrorCode
        );
        let invalid = MiddlewareErrorCode::new("AUTH\nTOKEN").unwrap_err();
        assert_eq!(invalid, HttpRejectionError::InvalidErrorCode);
        assert!(!format!("{invalid:?} {invalid}").contains("TOKEN"));
        let maximum = "A".repeat(MAX_HTTP_ERROR_CODE_BYTES).leak();
        assert_eq!(MiddlewareErrorCode::new(maximum).unwrap().as_str(), maximum);
        let too_long = "A".repeat(MAX_HTTP_ERROR_CODE_BYTES + 1).leak();
        assert!(matches!(
            MiddlewareErrorCode::new(too_long),
            Err(HttpRejectionError::ErrorCodeTooLong { .. })
        ));
    }

    #[test]
    fn initialization_errors_preserve_only_bounded_dependency_categories() {
        let scoped: HttpMiddlewareInitError = InjectionError::ScopeRequired {
            service: "credential=secret".to_string(),
        }
        .into();
        assert_eq!(scoped.diagnostic_code(), "MIDDLEWARE_SCOPE_REQUIRED");
        assert!(!format!("{scoped:?} {scoped}").contains("secret"));

        let dependency: HttpMiddlewareInitError =
            InjectionError::InitError("credential=secret".to_string()).into();
        assert_eq!(dependency.diagnostic_code(), "MIDDLEWARE_DEPENDENCY");
        assert!(!format!("{dependency:?} {dependency}").contains("secret"));
    }

    #[test]
    fn descriptor_validation_bounds_count_labels_and_exclusivity() {
        assert_eq!(validate_middleware_count(0), Ok(()));
        let valid = [
            MiddlewareDescriptor::new("request_trace", MiddlewareKind::Custom),
            MiddlewareDescriptor::new("exclusive", MiddlewareKind::Custom)
                .with_exclusive_key("response:cors"),
        ];
        assert_eq!(validate_middleware_descriptors(&valid), Ok(()));
        assert_eq!(valid[0].name(), "request_trace");
        assert_eq!(valid[0].exclusive_key(), None);
        assert_eq!(valid[1].name(), "exclusive");
        assert_eq!(valid[1].exclusive_key(), Some("response:cors"));

        let duplicate_names_without_exclusive_keys = [
            MiddlewareDescriptor::new("trace", MiddlewareKind::Custom),
            MiddlewareDescriptor::new("trace", MiddlewareKind::Custom),
        ];
        assert_eq!(
            validate_middleware_descriptors(&duplicate_names_without_exclusive_keys),
            Ok(())
        );

        let maximum_count =
            vec![MiddlewareDescriptor::new("noop", MiddlewareKind::Custom); MAX_HTTP_MIDDLEWARES];
        assert_eq!(validate_middleware_descriptors(&maximum_count), Ok(()));

        let maximum_name = "a".repeat(MAX_DESCRIPTOR_LABEL_BYTES).leak();
        assert_eq!(
            validate_middleware_descriptors(&[MiddlewareDescriptor::new(
                maximum_name,
                MiddlewareKind::Custom,
            )]),
            Ok(())
        );
        let oversized_name = "a".repeat(MAX_DESCRIPTOR_LABEL_BYTES + 1).leak();
        assert_eq!(
            validate_middleware_descriptors(&[MiddlewareDescriptor::new(
                oversized_name,
                MiddlewareKind::Custom,
            )]),
            Err(MiddlewareConfigError::DescriptorNameTooLong {
                index: 0,
                limit_bytes: MAX_DESCRIPTOR_LABEL_BYTES,
                actual_bytes: MAX_DESCRIPTOR_LABEL_BYTES + 1,
            })
        );

        let duplicate = [
            MiddlewareDescriptor::new("first", MiddlewareKind::Custom)
                .with_exclusive_key("response:cors"),
            MiddlewareDescriptor::new("second", MiddlewareKind::Custom)
                .with_exclusive_key("response:cors"),
        ];
        assert_eq!(
            validate_middleware_descriptors(&duplicate),
            Err(MiddlewareConfigError::DuplicateExclusiveKey {
                first_index: 0,
                duplicate_index: 1,
            })
        );

        let too_many = vec![
            MiddlewareDescriptor::new("noop", MiddlewareKind::Custom);
            MAX_HTTP_MIDDLEWARES + 1
        ];
        assert_eq!(
            validate_middleware_descriptors(&too_many),
            Err(MiddlewareConfigError::TooManyMiddlewares {
                limit: MAX_HTTP_MIDDLEWARES,
                actual: MAX_HTTP_MIDDLEWARES + 1,
            })
        );
        assert_eq!(
            validate_middleware_count(MAX_HTTP_MIDDLEWARES + 1),
            Err(MiddlewareConfigError::TooManyMiddlewares {
                limit: MAX_HTTP_MIDDLEWARES,
                actual: MAX_HTTP_MIDDLEWARES + 1,
            })
        );

        for (descriptor, expected) in [
            (
                MiddlewareDescriptor::new("", MiddlewareKind::Custom),
                MiddlewareConfigError::EmptyDescriptorName { index: 0 },
            ),
            (
                MiddlewareDescriptor::new("contains space", MiddlewareKind::Custom),
                MiddlewareConfigError::InvalidDescriptorName { index: 0 },
            ),
            (
                MiddlewareDescriptor::new(":invalid", MiddlewareKind::Custom),
                MiddlewareConfigError::InvalidDescriptorName { index: 0 },
            ),
            (
                MiddlewareDescriptor::new("trace\nsecret", MiddlewareKind::Custom),
                MiddlewareConfigError::InvalidDescriptorName { index: 0 },
            ),
            (
                MiddlewareDescriptor::new("valid", MiddlewareKind::Custom).with_exclusive_key(""),
                MiddlewareConfigError::EmptyExclusiveKey { index: 0 },
            ),
        ] {
            assert_eq!(
                validate_middleware_descriptors(&[descriptor]),
                Err(expected)
            );
        }
    }

    #[test]
    fn rejection_status_retry_after_and_public_message_are_bounded() {
        let code = MiddlewareErrorCode::new("RATE_LIMITED").unwrap();
        let rejection = HttpMiddlewareRejection::new(429, code)
            .unwrap()
            .with_retry_after(Duration::from_millis(1_001))
            .unwrap();
        assert_eq!(rejection.status(), 429);
        assert_eq!(rejection.retry_after(), Some(Duration::from_millis(1_001)));
        assert_eq!(rejection.retry_after_seconds(), Some(2));
        assert_eq!(rejection.public_message(), "Too many requests.");
        assert_eq!(
            rejection.to_string(),
            "HTTP request rejected with status 429 and code RATE_LIMITED"
        );

        for retry_after in [Duration::from_millis(1), Duration::from_secs(86_400)] {
            assert_eq!(
                HttpMiddlewareRejection::new(429, code)
                    .unwrap()
                    .with_retry_after(retry_after)
                    .unwrap()
                    .retry_after(),
                Some(retry_after)
            );
        }

        assert_eq!(
            HttpMiddlewareRejection::new(429, code)
                .unwrap()
                .with_retry_after(Duration::ZERO)
                .unwrap_err(),
            HttpRejectionError::InvalidRetryAfter
        );
        assert_eq!(
            HttpMiddlewareRejection::new(503, code)
                .unwrap()
                .with_retry_after(Duration::from_secs(86_400) + Duration::from_millis(1))
                .unwrap_err(),
            HttpRejectionError::InvalidRetryAfter
        );

        assert_eq!(
            HttpMiddlewareRejection::new(200, code).unwrap_err(),
            HttpRejectionError::InvalidStatus { status: 200 }
        );
        assert_eq!(
            HttpMiddlewareRejection::new(403, code)
                .unwrap()
                .with_retry_after(Duration::from_secs(1))
                .unwrap_err(),
            HttpRejectionError::RetryAfterNotSupported { status: 403 }
        );

        for (status, message) in [
            (400, "The request is invalid."),
            (401, "Authentication is required."),
            (403, "You are not allowed to perform this operation."),
            (408, "The request timed out."),
            (413, "The request payload is too large."),
            (415, "The request media type is not supported."),
            (429, "Too many requests."),
            (503, "The service is temporarily unavailable."),
            (418, "The request was rejected."),
        ] {
            let rejection = if status == 401 {
                HttpMiddlewareRejection::unauthorized(code, "Bearer realm=test").unwrap()
            } else {
                HttpMiddlewareRejection::new(status, code).unwrap()
            };
            assert_eq!(rejection.public_message(), message, "status: {status}");
        }
    }

    struct Terminal {
        events: Arc<Mutex<Vec<&'static str>>>,
    }

    #[async_trait]
    impl HttpNextService for Terminal {
        async fn run(&self, exchange: &mut HttpExchange<'_>) -> Result<(), HttpMiddlewareError> {
            let (_request, response) = exchange.parts_mut();
            response.status(201, "Created");
            self.events.lock().unwrap().push("handler");
            Ok(())
        }
    }

    struct Around {
        events: Arc<Mutex<Vec<&'static str>>>,
        before: &'static str,
        after: &'static str,
    }

    #[async_trait]
    impl HttpMiddleware for Around {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            Ok(Self {
                events: Arc::new(Mutex::new(Vec::new())),
                before: "before",
                after: "after",
            })
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("around", MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: HttpNext<'_>,
            _cancellation: lily_cancellation::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            self.events.lock().unwrap().push(self.before);
            next.run(exchange).await?;
            self.events.lock().unwrap().push(self.after);
            Ok(())
        }
    }

    struct ChainNode<'a> {
        middleware: &'a dyn HttpMiddleware,
        next: &'a dyn HttpNextService,
    }

    #[async_trait]
    impl HttpNextService for ChainNode<'_> {
        async fn run(&self, exchange: &mut HttpExchange<'_>) -> Result<(), HttpMiddlewareError> {
            let cancellation = exchange.execution_cancellation();
            self.middleware
                .handle(exchange, HttpNext::new(self.next), cancellation)
                .await
        }
    }

    #[tokio::test]
    async fn around_contract_has_explicit_next_and_reverse_exit_semantics() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let first: Arc<dyn HttpMiddleware> = Arc::new(Around {
            events: Arc::clone(&events),
            before: "first.before",
            after: "first.after",
        });
        let second: Arc<dyn HttpMiddleware> = Arc::new(Around {
            events: Arc::clone(&events),
            before: "second.before",
            after: "second.after",
        });
        let terminal = Terminal {
            events: Arc::clone(&events),
        };
        let second_node = ChainNode {
            middleware: second.as_ref(),
            next: &terminal,
        };
        let first_node = ChainNode {
            middleware: first.as_ref(),
            next: &second_node,
        };
        let mut request =
            Request::from_transport_parts("GET".to_string(), "/".to_string(), Vec::new(), &[])
                .await
                .unwrap();
        let mut response = Response::new().await.unwrap();
        let mut exchange = HttpExchange::new(&mut request, &mut response);

        first_node.run(&mut exchange).await.unwrap();

        assert_eq!(
            *events.lock().unwrap(),
            [
                "first.before",
                "second.before",
                "handler",
                "second.after",
                "first.after"
            ]
        );
        assert_eq!(exchange.response().status_code_value(), 201);
    }

    #[test]
    fn runtime_errors_expose_only_bounded_codes_and_generic_messages() {
        let code = MiddlewareErrorCode::new("AUTH_REQUIRED").unwrap();
        let rejection = HttpMiddlewareRejection::unauthorized(code, "Bearer realm=test").unwrap();
        let rejected = HttpMiddlewareError::rejected(rejection);
        assert_eq!(rejected.http_status(), 401);
        assert_eq!(rejected.kind(), HttpMiddlewareFailureKind::Rejected);
        assert_eq!(rejected.kind().as_str(), "rejected");
        assert_eq!(rejected.diagnostic_code(), code);
        assert_eq!(rejected.retry_after(), None);
        assert_eq!(rejected.public_message(), "Authentication is required.");
        assert_eq!(
            rejected.to_string(),
            "HTTP request rejected with status 401 and code AUTH_REQUIRED"
        );

        let rate_limited = HttpMiddlewareError::rejected(
            HttpMiddlewareRejection::new(429, code)
                .unwrap()
                .with_retry_after(Duration::from_secs(2))
                .unwrap(),
        );
        assert_eq!(rate_limited.retry_after(), Some(Duration::from_secs(2)));

        let internal = HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL);
        assert_eq!(internal.http_status(), 500);
        assert_eq!(internal.kind(), HttpMiddlewareFailureKind::Internal);
        assert_eq!(
            internal.public_message(),
            "An internal server error occurred."
        );
        assert_eq!(
            internal.to_string(),
            "HTTP middleware failed with code MIDDLEWARE_INTERNAL"
        );

        let timeout = HttpMiddlewareError::timeout(MiddlewareErrorCode::TIMEOUT);
        assert_eq!(timeout.kind(), HttpMiddlewareFailureKind::Timeout);
        assert_eq!(timeout.kind().as_str(), "timeout");
        assert_eq!(
            timeout.to_string(),
            "HTTP middleware timed out with code MIDDLEWARE_TIMEOUT"
        );
    }
}
