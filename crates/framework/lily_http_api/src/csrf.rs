//! Application-owned CSRF integration installed through [`crate::AppBuilder`].
//!
//! The application owns authentication and session rotation. Lily only binds
//! a signed or stateful token to the opaque identifier returned by its configured
//! [`crate::CsrfSessionBinding`]. A typical token endpoint resolves this
//! singleton from `Extensions`, calls [`CsrfService::issue`], and returns
//! [`crate::CsrfToken::as_str`] to the frontend. Signed double-submit mode also
//! writes its protected cookie; synchronizer mode persists the token only via
//! the application-supplied [`crate::CsrfTokenStore`]. Unsafe requests return
//! the public token through the configured header (or explicitly enabled
//! URL-encoded form field).
//!
//! With [`crate::CsrfRequestLocalBinding`], global mode reads the exact
//! [`crate::CsrfSessionId`] published by the registered session middleware.
//! Route-scoped mode can use that same middleware or an earlier application
//! guard that validates/loads the session and writes the typed value through
//! [`crate::Request::local_mut`]. Missing state, a different concrete DTO, or a
//! token signed for another session fails closed.
//!
//! On login or privilege-boundary changes, signed mode rotates the application
//! session identifier, publishes the new request-local [`crate::CsrfSessionId`],
//! then calls [`CsrfService::rotate`]. Synchronizer mode first calls
//! [`CsrfService::clear`] while the old binding is still available, rotates and
//! republishes the application session identifier, then issues/rotates the new
//! token. Logout follows the same preserve-old-binding-until-clear rule. Lily
//! does not provide or infer a session/token store. Because the signed profile
//! is stateless, a stolen cookie+token pair cannot be revoked before expiry
//! unless the application also changes or invalidates its session binding.

use crate::guard::{GuardInitError, GuardRejection, GuardTrait};
use async_trait::async_trait;
use lily_injection::Injectable;
use lily_injection::{Extensions, ServiceTrait};
use lily_middleware::__private::{CompiledCsrfPolicy, CsrfOperationError, CsrfRuntimeRejection};
use lily_middleware::{
    CsrfToken, HttpExchange, HttpMiddleware, HttpMiddlewareError, HttpMiddlewareInitError,
    HttpNext, MiddlewareDescriptor, MiddlewareErrorCode, MiddlewareKind,
};
use lily_web_core::{HttpRejection, Request, Response, ResponseCookieWriteError};
use std::sync::{Arc, OnceLock};

fn csrf_rejected_code() -> MiddlewareErrorCode {
    MiddlewareErrorCode::new("CSRF_REJECTED").expect("the built-in CSRF code is valid")
}

fn csrf_internal_code() -> MiddlewareErrorCode {
    MiddlewareErrorCode::new("CSRF_INTERNAL").expect("the built-in CSRF code is valid")
}

fn csrf_store_unavailable_code() -> MiddlewareErrorCode {
    MiddlewareErrorCode::new("CSRF_STORE_UNAVAILABLE")
        .expect("the built-in CSRF store code is valid")
}

fn csrf_store_timeout_code() -> MiddlewareErrorCode {
    MiddlewareErrorCode::new("CSRF_STORE_TIMEOUT")
        .expect("the built-in CSRF store timeout code is valid")
}

/// Singleton CSRF capability available through the application DI graph.
///
/// An application normally resolves this service only from the endpoint that
/// issues or rotates a signed token. [`crate::AppBuilder::csrf`] installs
/// global enforcement; [`crate::AppBuilder::csrf_route_scoped`] attaches
/// the same service for routes that declare [`CsrfGuard`].
#[derive(Injectable)]
#[service(lifetime = "Singleton")]
pub struct CsrfService {
    runtime: OnceLock<Arc<CompiledCsrfPolicy>>,
}

impl Default for CsrfService {
    fn default() -> Self {
        Self {
            runtime: OnceLock::new(),
        }
    }
}

impl ServiceTrait for CsrfService {}

impl CsrfService {
    /// Issues the current session-bound token.
    ///
    /// The configured binding must already be available on `request`. For a
    /// request-local binding this means the session middleware has published a
    /// verified [`crate::CsrfSessionId`] before the endpoint is invoked. Signed
    /// mode appends its protected cookie. Synchronizer mode atomically
    /// load-or-inserts through the configured store and does not write a CSRF
    /// cookie.
    pub async fn issue(
        &self,
        request: &Request,
        response: &mut Response,
    ) -> Result<CsrfToken, CsrfServiceError> {
        let (token, cookie) = self.runtime()?.issue(request).await?;
        if let Some(cookie) = cookie {
            response.set_cookie(&cookie)?;
        }
        Ok(token)
    }

    /// Rotates the token for the current session binding.
    ///
    /// Signed mode creates a fresh signed token/cookie pair. Synchronizer mode
    /// atomically replaces the stored value. After an application session
    /// rotation, callers using request-local binding must replace the local
    /// [`crate::CsrfSessionId`] before this call.
    pub async fn rotate(
        &self,
        request: &Request,
        response: &mut Response,
    ) -> Result<CsrfToken, CsrfServiceError> {
        let (token, cookie) = self.runtime()?.rotate(request).await?;
        if let Some(cookie) = cookie {
            response.set_cookie(&cookie)?;
        }
        Ok(token)
    }

    /// Clears token state for the current session binding.
    ///
    /// Signed mode appends the exact cookie removal; synchronizer mode
    /// idempotently revokes the stored token. The application keeps the old
    /// binding available to this call, then completes its own session
    /// invalidation. Lily does not infer logout or session validity.
    pub async fn clear(
        &self,
        request: &Request,
        response: &mut Response,
    ) -> Result<(), CsrfServiceError> {
        if let Some(removal) = self.runtime()?.clear(request).await? {
            response.delete_cookie(&removal)?;
        }
        Ok(())
    }

    pub(crate) fn attach(
        &self,
        runtime: Arc<CompiledCsrfPolicy>,
    ) -> Result<(), CsrfServiceAttachmentError> {
        self.runtime
            .set(runtime)
            .map_err(|_| CsrfServiceAttachmentError::AlreadyAttached)
    }

    fn runtime(&self) -> Result<&CompiledCsrfPolicy, CsrfServiceError> {
        self.runtime
            .get()
            .map(Arc::as_ref)
            .ok_or(CsrfServiceError::NotConfigured)
    }

    async fn enforce(&self, request: &Request) -> Result<(), CsrfEnforcementFailure> {
        let runtime = self.runtime.get().ok_or(CsrfEnforcementFailure::Internal)?;
        match runtime.enforce(request).await {
            Ok(()) => Ok(()),
            Err(CsrfRuntimeRejection::StoreUnavailable) => {
                Err(CsrfEnforcementFailure::StoreUnavailable)
            }
            Err(CsrfRuntimeRejection::StoreTimeout) => Err(CsrfEnforcementFailure::StoreTimeout),
            Err(CsrfRuntimeRejection::ClockInvalid | CsrfRuntimeRejection::StoreInvalid) => {
                Err(CsrfEnforcementFailure::Internal)
            }
            Err(_) => Err(CsrfEnforcementFailure::Rejected),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CsrfEnforcementFailure {
    Rejected,
    Internal,
    StoreUnavailable,
    StoreTimeout,
}

fn csrf_forbidden_rejection() -> HttpRejection {
    HttpRejection::forbidden(csrf_rejected_code()).expect("the built-in CSRF rejection is valid")
}

fn csrf_internal_rejection() -> HttpRejection {
    HttpRejection::new(500, csrf_internal_code())
        .expect("the built-in CSRF internal rejection is valid")
}

fn csrf_store_unavailable_rejection() -> HttpRejection {
    HttpRejection::new(500, csrf_store_unavailable_code())
        .expect("the built-in CSRF store rejection is valid")
}

fn csrf_store_timeout_rejection() -> HttpRejection {
    HttpRejection::new(500, csrf_store_timeout_code())
        .expect("the built-in CSRF store timeout rejection is valid")
}

/// Route-level CSRF enforcement backed by the application-wide
/// [`CsrfService`] singleton.
///
/// Register the policy with [`crate::AppBuilder::csrf_route_scoped`] and
/// list this guard only on protected routes. Guard declaration order remains
/// application-controlled: a guard that validates a session and publishes a
/// [`crate::CsrfSessionId`] may precede `CsrfGuard`, followed by authorization
/// guards. The guard never creates or validates an application session.
pub struct CsrfGuard {
    service: Arc<CsrfService>,
}

#[async_trait]
impl GuardTrait for CsrfGuard {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, GuardInitError> {
        let service = extensions
            .get_service::<CsrfService>(None)
            .await
            .map_err(|_| GuardInitError::Dependency {
                guard: "csrf",
                reason: "CSRF service is unavailable".to_string(),
            })?;
        if service.runtime.get().is_none() {
            return Err(GuardInitError::MissingConfiguration {
                guard: "csrf",
                setting: "AppBuilder::csrf_route_scoped(CsrfPolicy)",
            });
        }
        Ok(Self { service })
    }

    async fn can_activate(
        &self,
        request: &mut Request,
        _cancellation: crate::ExecutionCancellation,
    ) -> Result<(), GuardRejection> {
        match self.service.enforce(request).await {
            Ok(()) => Ok(()),
            Err(CsrfEnforcementFailure::Rejected) => Err(csrf_forbidden_rejection()),
            Err(CsrfEnforcementFailure::Internal) => Err(csrf_internal_rejection()),
            Err(CsrfEnforcementFailure::StoreUnavailable) => {
                Err(csrf_store_unavailable_rejection())
            }
            Err(CsrfEnforcementFailure::StoreTimeout) => Err(csrf_store_timeout_rejection()),
        }
    }

    fn name(&self) -> &'static str {
        "csrf"
    }
}

/// Internal enforcement adapter around the same injectable service used by
/// application token endpoints.
pub(crate) struct CsrfEnforcementMiddleware {
    service: Arc<CsrfService>,
}

impl CsrfEnforcementMiddleware {
    pub(crate) const fn middleware_descriptor() -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("csrf", MiddlewareKind::Csrf).with_exclusive_key("lily.csrf")
    }
}

#[async_trait]
impl HttpMiddleware for CsrfEnforcementMiddleware {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
        Ok(Self {
            service: extensions.get_service::<CsrfService>(None).await?,
        })
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        Self::middleware_descriptor()
    }

    async fn handle(
        &self,
        exchange: &mut HttpExchange<'_>,
        next: HttpNext<'_>,
        _cancellation: crate::ExecutionCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        match self.service.enforce(exchange.request()).await {
            Ok(()) => next.run(exchange).await,
            Err(CsrfEnforcementFailure::Internal) => {
                Err(HttpMiddlewareError::internal(csrf_internal_code()))
            }
            Err(CsrfEnforcementFailure::StoreUnavailable) => {
                Err(HttpMiddlewareError::internal(csrf_store_unavailable_code()))
            }
            Err(CsrfEnforcementFailure::StoreTimeout) => {
                Err(HttpMiddlewareError::internal(csrf_store_timeout_code()))
            }
            Err(CsrfEnforcementFailure::Rejected) => {
                Err(HttpMiddlewareError::rejected(csrf_forbidden_rejection()))
            }
        }
    }
}

/// Token lifecycle failure returned to the application endpoint.
#[derive(Debug)]
#[non_exhaustive]
pub enum CsrfServiceError {
    /// No compiled CSRF policy has been attached to this application service.
    NotConfigured,
    /// The configured policy does not enable a token-based CSRF mode.
    TokenModeRequired,
    /// The request does not contain the configured application session binding.
    BindingMissing,
    /// The request's session binding is malformed or otherwise unusable.
    BindingInvalid,
    /// A required clock or randomness source was unavailable.
    RuntimeUnavailable,
    /// Lily could not construct the configured CSRF response cookie.
    CookieInvalid,
    /// The application-provided synchronizer token store was unavailable.
    StoreUnavailable,
    /// A synchronizer token-store operation exceeded its bounded timeout.
    StoreTimeout,
    /// The synchronizer token store returned invalid state.
    StoreInvalid,
    /// Writing the typed CSRF cookie or removal to the response failed.
    CookieWrite(
        /// Underlying bounded response-cookie write failure.
        ResponseCookieWriteError,
    ),
}

impl std::fmt::Display for CsrfServiceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotConfigured => formatter.write_str("CSRF token mode is not configured"),
            Self::TokenModeRequired => formatter.write_str("CSRF token mode is not configured"),
            Self::BindingMissing => formatter.write_str("CSRF session binding is unavailable"),
            Self::BindingInvalid => formatter.write_str("CSRF session binding is invalid"),
            Self::RuntimeUnavailable => {
                formatter.write_str("CSRF runtime dependency is unavailable")
            }
            Self::CookieInvalid => formatter.write_str("CSRF cookie construction failed"),
            Self::StoreUnavailable => formatter.write_str("CSRF token store is unavailable"),
            Self::StoreTimeout => formatter.write_str("CSRF token store operation timed out"),
            Self::StoreInvalid => formatter.write_str("CSRF token store returned invalid data"),
            Self::CookieWrite(error) => write!(formatter, "CSRF cookie write failed: {error}"),
        }
    }
}

impl std::error::Error for CsrfServiceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NotConfigured
            | Self::TokenModeRequired
            | Self::BindingMissing
            | Self::BindingInvalid
            | Self::RuntimeUnavailable
            | Self::CookieInvalid
            | Self::StoreUnavailable
            | Self::StoreTimeout
            | Self::StoreInvalid => None,
            Self::CookieWrite(error) => Some(error),
        }
    }
}

impl From<CsrfOperationError> for CsrfServiceError {
    fn from(error: CsrfOperationError) -> Self {
        match error {
            CsrfOperationError::TokenModeRequired => Self::TokenModeRequired,
            CsrfOperationError::BindingMissing => Self::BindingMissing,
            CsrfOperationError::BindingInvalid => Self::BindingInvalid,
            CsrfOperationError::ClockInvalid | CsrfOperationError::RandomUnavailable => {
                Self::RuntimeUnavailable
            }
            CsrfOperationError::CookieInvalid => Self::CookieInvalid,
            CsrfOperationError::StoreUnavailable => Self::StoreUnavailable,
            CsrfOperationError::StoreTimeout => Self::StoreTimeout,
            CsrfOperationError::StoreInvalid => Self::StoreInvalid,
        }
    }
}

impl From<ResponseCookieWriteError> for CsrfServiceError {
    fn from(error: ResponseCookieWriteError) -> Self {
        Self::CookieWrite(error)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CsrfServiceAttachmentError {
    AlreadyAttached,
}

impl std::fmt::Display for CsrfServiceAttachmentError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("the application container is already attached to a CSRF policy")
    }
}

impl std::error::Error for CsrfServiceAttachmentError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CsrfPolicy, CsrfSecret, CsrfSessionCookieBinding, CsrfStoreKey, CsrfTokenStore,
        CsrfTokenStoreError, StoredCsrfToken,
    };
    use lily_core::RawHeader;
    use lily_injection::ApplicationContainer;
    use lily_middleware::__private::HttpNextService;
    use std::sync::atomic::{AtomicBool, Ordering};

    async fn request(headers: &[(&str, &str)]) -> Request {
        request_with_method("GET", headers).await
    }

    async fn request_with_method(method: &str, headers: &[(&str, &str)]) -> Request {
        Request::from_transport_parts(
            method.to_string(),
            "/csrf".to_string(),
            headers
                .iter()
                .enumerate()
                .map(|(line_number, (name, value))| RawHeader {
                    name: (*name).to_string(),
                    value: (*value).to_string(),
                    line_number,
                    raw_line: String::new(),
                })
                .collect(),
            b"",
        )
        .await
        .unwrap()
    }

    struct UnavailableTokenStore;

    #[async_trait]
    impl CsrfTokenStore for UnavailableTokenStore {
        async fn load_or_insert(
            &self,
            _key: &CsrfStoreKey,
            _candidate: StoredCsrfToken,
            _now_unix_seconds: u64,
        ) -> Result<StoredCsrfToken, CsrfTokenStoreError> {
            Err(CsrfTokenStoreError::Unavailable)
        }

        async fn load(
            &self,
            _key: &CsrfStoreKey,
        ) -> Result<Option<StoredCsrfToken>, CsrfTokenStoreError> {
            Err(CsrfTokenStoreError::Unavailable)
        }

        async fn replace(
            &self,
            _key: &CsrfStoreKey,
            _replacement: StoredCsrfToken,
        ) -> Result<(), CsrfTokenStoreError> {
            Err(CsrfTokenStoreError::Unavailable)
        }

        async fn revoke(&self, _key: &CsrfStoreKey) -> Result<(), CsrfTokenStoreError> {
            Err(CsrfTokenStoreError::Unavailable)
        }
    }

    #[tokio::test]
    async fn guard_requires_the_attached_application_csrf_service() {
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let error = CsrfGuard::new(container.services())
            .await
            .err()
            .expect("an unattached service must fail guard construction");
        assert_eq!(
            error,
            GuardInitError::MissingConfiguration {
                guard: "csrf",
                setting: "AppBuilder::csrf_route_scoped(CsrfPolicy)",
            }
        );
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn guard_uses_the_attached_singleton_and_canonical_enforcement() {
        let policy = CsrfPolicy::signed_double_submit(
            CsrfSecret::new([9_u8; 32]).unwrap(),
            CsrfSessionCookieBinding::new("session").unwrap(),
        );
        let runtime = Arc::new(CompiledCsrfPolicy::try_new_route_scoped(&policy).unwrap());
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let service = container.resolve::<CsrfService>(None).await.unwrap();
        service.attach(runtime).unwrap();
        let guard = CsrfGuard::new(container.services()).await.unwrap();
        assert!(Arc::ptr_eq(&guard.service, &service));
        assert_eq!(guard.name(), "csrf");

        let mut safe = request_with_method("GET", &[]).await;
        let cancellation = safe.execution_cancellation();
        assert_eq!(guard.can_activate(&mut safe, cancellation).await, Ok(()));

        let mut missing = request_with_method("POST", &[]).await;
        let cancellation = missing.execution_cancellation();
        let rejection = guard
            .can_activate(&mut missing, cancellation)
            .await
            .unwrap_err();
        assert_eq!(rejection.status(), 403);
        assert_eq!(rejection.error_code(), "CSRF_REJECTED");

        let issue_request = request(&[("Cookie", "session=session-a")]).await;
        let mut issue_response = Response::new().await.unwrap();
        let token = service
            .issue(&issue_request, &mut issue_response)
            .await
            .unwrap();
        let csrf_cookie = issue_response
            .header_values("set-cookie")
            .unwrap()
            .next()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned();
        let cookies = format!("session=session-a; {csrf_cookie}");
        let mut valid = request_with_method(
            "POST",
            &[("Cookie", &cookies), ("X-CSRF-Token", token.as_str())],
        )
        .await;
        let cancellation = valid.execution_cancellation();
        assert_eq!(guard.can_activate(&mut valid, cancellation).await, Ok(()));

        drop(guard);
        drop(service);
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn service_owns_issue_rotate_and_clear_cookie_lifecycle() {
        let policy = CsrfPolicy::signed_double_submit(
            CsrfSecret::new([7_u8; 32]).unwrap(),
            CsrfSessionCookieBinding::new("session").unwrap(),
        );
        let runtime = Arc::new(CompiledCsrfPolicy::try_new(&policy).unwrap());
        let service = CsrfService::default();
        service.attach(runtime.clone()).unwrap();
        assert_eq!(
            service.attach(runtime).unwrap_err(),
            CsrfServiceAttachmentError::AlreadyAttached
        );

        let request = request(&[("Cookie", "session=opaque-session")]).await;
        let mut response = Response::new().await.unwrap();
        let first = service.issue(&request, &mut response).await.unwrap();
        let second = service.rotate(&request, &mut response).await.unwrap();
        assert_ne!(first, second);
        service.clear(&request, &mut response).await.unwrap();

        let cookies: Vec<_> = response.header_values("set-cookie").unwrap().collect();
        assert_eq!(cookies.len(), 3);
        assert!(cookies[0].contains("HttpOnly"));
        assert!(cookies[0].contains("SameSite=Strict"));
        assert!(cookies[0].contains("Secure"));
        assert!(cookies[2].contains("Max-Age=0"));
        let debug = format!("{first:?} {second:?}");
        assert!(!debug.contains(first.as_str()));
        assert!(!debug.contains(second.as_str()));
    }

    #[tokio::test]
    async fn unconfigured_service_fails_without_writing_a_cookie() {
        let service = CsrfService::default();
        let request = request(&[("Cookie", "session=opaque-session")]).await;
        let mut response = Response::new().await.unwrap();
        assert!(matches!(
            service.issue(&request, &mut response).await,
            Err(CsrfServiceError::NotConfigured)
        ));
        assert_eq!(response.header_values("set-cookie").unwrap().count(), 0);
    }

    struct Terminal(AtomicBool);

    #[async_trait]
    impl HttpNextService for Terminal {
        async fn run(&self, _exchange: &mut HttpExchange<'_>) -> Result<(), HttpMiddlewareError> {
            self.0.store(true, Ordering::Relaxed);
            Ok(())
        }
    }

    #[tokio::test]
    async fn enforcement_maps_private_causes_to_one_canonical_forbidden_rejection() {
        let runtime = Arc::new(
            CompiledCsrfPolicy::try_new(&CsrfPolicy::signed_double_submit(
                CsrfSecret::new([8_u8; 32]).unwrap(),
                CsrfSessionCookieBinding::new("session").unwrap(),
            ))
            .unwrap(),
        );
        let service = Arc::new(CsrfService::default());
        service.attach(runtime).unwrap();
        let middleware = CsrfEnforcementMiddleware {
            service: Arc::clone(&service),
        };
        let mut request = Request::from_transport_parts(
            "POST".to_string(),
            "/transfer".to_string(),
            Vec::new(),
            b"",
        )
        .await
        .unwrap();
        let mut response = Response::new().await.unwrap();
        let terminal = Terminal(AtomicBool::new(false));
        let mut exchange = HttpExchange::new(&mut request, &mut response);

        let cancellation = exchange.execution_cancellation();
        let error = middleware
            .handle(&mut exchange, HttpNext::new(&terminal), cancellation)
            .await
            .unwrap_err();
        assert_eq!(error.http_status(), 403);
        assert_eq!(error.diagnostic_code().as_str(), "CSRF_REJECTED");
        assert!(!terminal.0.load(Ordering::Relaxed));
        let debug = format!("{error:?} {error}");
        assert!(!debug.contains("session"));
        assert!(!debug.contains("Cookie"));
    }

    #[tokio::test]
    async fn synchronizer_store_failures_are_not_reported_as_client_rejections() {
        let policy = CsrfPolicy::synchronizer(
            CsrfSecret::new([4_u8; 32]).unwrap(),
            CsrfSessionCookieBinding::new("session").unwrap(),
            Arc::new(UnavailableTokenStore),
        );
        let runtime = Arc::new(CompiledCsrfPolicy::try_new_route_scoped(&policy).unwrap());
        let service = Arc::new(CsrfService::default());
        service.attach(runtime).unwrap();

        let issue_request = request(&[("Cookie", "session=session-a")]).await;
        let mut issue_response = Response::new().await.unwrap();
        assert!(matches!(
            service.issue(&issue_request, &mut issue_response).await,
            Err(CsrfServiceError::StoreUnavailable)
        ));
        assert_eq!(
            issue_response.header_values("set-cookie").unwrap().count(),
            0
        );

        let mut protected = request_with_method(
            "POST",
            &[
                ("Cookie", "session=session-a"),
                (
                    "X-CSRF-Token",
                    "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
                ),
            ],
        )
        .await;
        let guard = CsrfGuard {
            service: Arc::clone(&service),
        };
        let cancellation = protected.execution_cancellation();
        let rejection = guard
            .can_activate(&mut protected, cancellation)
            .await
            .unwrap_err();
        assert_eq!(rejection.status(), 500);
        assert_eq!(rejection.error_code(), "CSRF_STORE_UNAVAILABLE");

        let middleware = CsrfEnforcementMiddleware { service };
        let mut response = Response::new().await.unwrap();
        let terminal = Terminal(AtomicBool::new(false));
        let mut exchange = HttpExchange::new(&mut protected, &mut response);
        let cancellation = exchange.execution_cancellation();
        let error = middleware
            .handle(&mut exchange, HttpNext::new(&terminal), cancellation)
            .await
            .unwrap_err();
        assert_eq!(error.http_status(), 500);
        assert_eq!(error.diagnostic_code().as_str(), "CSRF_STORE_UNAVAILABLE");
        assert_eq!(
            error.kind(),
            lily_middleware::HttpMiddlewareFailureKind::Internal
        );
        assert!(!terminal.0.load(Ordering::Relaxed));
    }
}
