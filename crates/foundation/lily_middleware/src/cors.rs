use crate::{MiddlewareConfigError, MiddlewareErrorCode};
use async_trait::async_trait;
use http::{
    HeaderMap, HeaderName, HeaderValue, Method, Request, Response,
    header::{
        ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS,
        ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS,
        ACCESS_CONTROL_MAX_AGE, ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD,
        ORIGIN, VARY,
    },
};
use lily_injection::{Extensions, InjectionError};
use pin_project_lite::pin_project;
use std::{
    any::{Any as StdAny, TypeId},
    collections::HashMap,
    fmt,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tower_http::cors::{AllowHeaders, AllowMethods, AllowOrigin, Any, Cors, CorsLayer, Vary};
use tower_layer::Layer;
use tower_service::Service;
use url::Url;

/// Maximum number of exact origins accepted by one CORS policy.
const MAX_CORS_ORIGINS: usize = 64;
/// Maximum number of methods accepted by one CORS policy.
const MAX_CORS_METHODS: usize = 32;
/// Maximum number of allowed, exposed or additional `Vary` header names.
const MAX_CORS_HEADER_NAMES: usize = 64;
/// Maximum retained CORS policy metadata.
const MAX_CORS_POLICY_BYTES: usize = 32 * 1024;

const MAX_ORIGIN_BYTES: usize = 2 * 1024;
const MAX_TOKEN_BYTES: usize = 256;
const MAX_VARY_VALUE_BYTES: usize = 8 * 1024;
const MAX_CORS_MAX_AGE: Duration = Duration::from_secs(86_400);
const ACCESS_CONTROL_ALLOW_PRIVATE_NETWORK: HeaderName =
    HeaderName::from_static("access-control-allow-private-network");
const ACCESS_CONTROL_REQUEST_PRIVATE_NETWORK: HeaderName =
    HeaderName::from_static("access-control-request-private-network");

/// Bounded failure returned while constructing one dynamic CORS origin resolver.
///
/// Application dependency messages and configuration values are deliberately not retained.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CorsOriginResolverInitError {
    /// Required immutable application configuration is absent.
    MissingConfiguration {
        /// Secret-safe failure category.
        code: MiddlewareErrorCode,
    },
    /// Resolver configuration is invalid.
    InvalidConfiguration {
        /// Secret-safe failure category.
        code: MiddlewareErrorCode,
    },
    /// A dependency could not be resolved or initialized.
    Dependency {
        /// Secret-safe dependency failure category.
        code: MiddlewareErrorCode,
    },
    /// Resolver construction failed internally.
    Internal {
        /// Secret-safe internal failure category.
        code: MiddlewareErrorCode,
    },
}

impl CorsOriginResolverInitError {
    /// Creates a missing-configuration failure.
    #[must_use]
    pub const fn missing_configuration(code: MiddlewareErrorCode) -> Self {
        Self::MissingConfiguration { code }
    }

    /// Creates an invalid-configuration failure.
    #[must_use]
    pub const fn invalid_configuration(code: MiddlewareErrorCode) -> Self {
        Self::InvalidConfiguration { code }
    }

    /// Creates a dependency failure.
    #[must_use]
    pub const fn dependency(code: MiddlewareErrorCode) -> Self {
        Self::Dependency { code }
    }

    /// Creates an internal resolver-construction failure.
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

impl From<InjectionError> for CorsOriginResolverInitError {
    fn from(error: InjectionError) -> Self {
        let code = match error {
            InjectionError::ScopeRequired { .. } => "CORS_ORIGIN_RESOLVER_SCOPE_REQUIRED",
            InjectionError::LifetimeMismatch { .. } => "CORS_ORIGIN_RESOLVER_LIFETIME_MISMATCH",
            _ => "CORS_ORIGIN_RESOLVER_DEPENDENCY",
        };
        Self::Dependency {
            code: MiddlewareErrorCode::new(code)
                .expect("built-in CORS resolver dependency code is valid"),
        }
    }
}

impl fmt::Display for CorsOriginResolverInitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let category = match self {
            Self::MissingConfiguration { .. } => "missing configuration",
            Self::InvalidConfiguration { .. } => "invalid configuration",
            Self::Dependency { .. } => "dependency failure",
            Self::Internal { .. } => "internal failure",
        };
        write!(
            formatter,
            "CORS origin resolver initialization {category} ({})",
            self.diagnostic_code()
        )
    }
}

impl std::error::Error for CorsOriginResolverInitError {}

/// Secret-safe runtime failure returned by a dynamic CORS origin resolver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CorsOriginResolverError {
    /// The resolver's backing authority is temporarily unavailable.
    Unavailable {
        /// Secret-safe availability category.
        code: MiddlewareErrorCode,
    },
    /// The resolver failed without a safe availability interpretation.
    Internal {
        /// Secret-safe internal failure category.
        code: MiddlewareErrorCode,
    },
}

impl CorsOriginResolverError {
    /// Creates a temporary-unavailability failure.
    #[must_use]
    pub const fn unavailable(code: MiddlewareErrorCode) -> Self {
        Self::Unavailable { code }
    }

    /// Creates an internal runtime failure.
    #[must_use]
    pub const fn internal(code: MiddlewareErrorCode) -> Self {
        Self::Internal { code }
    }

    /// Returns the bounded diagnostic label selected at construction.
    #[must_use]
    pub const fn diagnostic_code(self) -> &'static str {
        match self {
            Self::Unavailable { code } | Self::Internal { code } => code.as_str(),
        }
    }
}

impl fmt::Display for CorsOriginResolverError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let category = match self {
            Self::Unavailable { .. } => "unavailable",
            Self::Internal { .. } => "internal failure",
        };
        write!(
            formatter,
            "CORS origin resolver {category} ({})",
            self.diagnostic_code()
        )
    }
}

impl std::error::Error for CorsOriginResolverError {}

/// Immutable, request-head-only input for a dynamic origin decision.
///
/// CORS is evaluated before application middleware, session state, guards and the request body.
/// Implementations should retain application-lived services or bounded caches obtained in
/// [`CorsOriginResolver::new`].
pub struct CorsOriginContext<'a> {
    origin: &'a str,
    method: Method,
    path: &'a str,
    host: Option<&'a str>,
    preflight: bool,
    cancellation: lily_cancellation::ExecutionCancellation,
}

impl fmt::Debug for CorsOriginContext<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CorsOriginContext")
            .field("origin", &"<redacted>")
            .field("method", &self.method)
            .field("path", &"<redacted>")
            .field("host", &self.host.map(|_| "<redacted>"))
            .field("preflight", &self.preflight)
            .finish()
    }
}

impl CorsOriginContext<'_> {
    /// Observes cancellation of this accepted policy evaluation. It shares
    /// the request's execution authority and cannot cancel its source.
    pub fn execution_cancellation(&self) -> lily_cancellation::ExecutionCancellation {
        self.cancellation.clone()
    }

    /// Returns the validated browser `Origin` serialization.
    #[must_use]
    pub const fn origin(&self) -> &str {
        self.origin
    }

    /// Returns the application request method.
    #[must_use]
    pub const fn method(&self) -> &Method {
        &self.method
    }

    /// Returns the request target path used for the origin decision.
    #[must_use]
    pub const fn path(&self) -> &str {
        self.path
    }

    /// Returns the validated `Host` value when one was supplied.
    #[must_use]
    pub const fn host(&self) -> Option<&str> {
        self.host
    }

    /// Reports whether this is a structurally valid CORS preflight.
    #[must_use]
    pub const fn is_preflight(&self) -> bool {
        self.preflight
    }
}

/// Application-defined dynamic origin authority.
///
/// Lily creates one resolver instance after the immutable DI graph is available and before the
/// listener opens. The runtime method can only allow or deny Lily's already validated request
/// origin; it cannot supply response header material or mutate the rest of the CORS policy.
#[async_trait]
pub trait CorsOriginResolver: Send + Sync + 'static {
    /// Constructs one application-lived resolver from the immutable DI graph.
    async fn new(extensions: Arc<Extensions>) -> Result<Self, CorsOriginResolverInitError>
    where
        Self: Sized;

    /// Allows or denies the validated request origin.
    async fn allows(
        &self,
        context: &CorsOriginContext<'_>,
    ) -> Result<bool, CorsOriginResolverError>;
}

/// Stateless build-time provider used by controller and action CORS metadata.
///
/// Lily invokes each provider type at most once while building an application. The returned
/// policy is validated and compiled before the listener opens. Runtime requests never call this
/// trait.
pub trait CorsPolicyProvider: Send + Sync + 'static {
    /// Returns the immutable policy contributed by this provider type.
    fn policy() -> CorsPolicy;
}

/// Explicit controller/action marker that cuts inherited/global CORS authority.
///
/// Use this as the policy provider in controller metadata when a route must remain CORS-disabled.
#[derive(Debug, Clone, Copy, Default)]
pub struct CorsDisabled;

impl CorsPolicyProvider for CorsDisabled {
    fn policy() -> CorsPolicy {
        CorsPolicy::new()
    }
}

type CorsOriginResolverInitializationFuture = Pin<
    Box<
        dyn Future<Output = Result<Arc<dyn CorsOriginResolver>, CorsOriginResolverInitError>>
            + Send
            + 'static,
    >,
>;

#[derive(Clone, Copy)]
struct CorsOriginResolverRegistration {
    type_id: TypeId,
    initialize: fn(Arc<Extensions>) -> CorsOriginResolverInitializationFuture,
}

/// Build-local resolver cache shared by all compiled CORS policy plans.
///
/// This type is reserved for Lily's HTTP composition root. It guarantees that two policy
/// providers selecting the same resolver type share one application-lived instance.
#[doc(hidden)]
#[derive(Default)]
pub struct CorsOriginResolverRegistry {
    resolvers: HashMap<TypeId, Arc<dyn CorsOriginResolver>>,
}

impl PartialEq for CorsOriginResolverRegistration {
    fn eq(&self, other: &Self) -> bool {
        self.type_id == other.type_id
    }
}

impl Eq for CorsOriginResolverRegistration {}

impl CorsOriginResolverRegistration {
    fn of<R>() -> Self
    where
        R: CorsOriginResolver,
    {
        Self {
            type_id: TypeId::of::<R>(),
            initialize: initialize_cors_origin_resolver::<R>,
        }
    }

    async fn instantiate(
        self,
        extensions: Arc<Extensions>,
    ) -> Result<Arc<dyn CorsOriginResolver>, CorsOriginResolverInitError> {
        (self.initialize)(extensions).await
    }
}

fn initialize_cors_origin_resolver<R>(
    extensions: Arc<Extensions>,
) -> CorsOriginResolverInitializationFuture
where
    R: CorsOriginResolver,
{
    Box::pin(async move {
        match CatchUnwindFuture::new(R::new(extensions)).await {
            Ok(Ok(resolver)) => Ok(Arc::new(resolver) as Arc<dyn CorsOriginResolver>),
            Ok(Err(error)) => Err(error),
            Err(_) => Err(CorsOriginResolverInitError::internal(
                MiddlewareErrorCode::new("CORS_ORIGIN_RESOLVER_INIT_PANICKED")
                    .expect("built-in CORS resolver initialization code is valid"),
            )),
        }
    })
}

pin_project! {
    struct CatchUnwindFuture<F> {
        #[pin]
        inner: F,
    }
}

impl<F> CatchUnwindFuture<F> {
    const fn new(inner: F) -> Self {
        Self { inner }
    }
}

impl<F> Future for CatchUnwindFuture<F>
where
    F: Future,
{
    type Output = Result<F::Output, Box<dyn StdAny + Send>>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let inner = self.project().inner;
        match catch_unwind(AssertUnwindSafe(|| inner.poll(context))) {
            Ok(Poll::Ready(output)) => Poll::Ready(Ok(output)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(payload) => Poll::Ready(Err(payload)),
        }
    }
}

fn cors_code(code: &'static str) -> MiddlewareErrorCode {
    MiddlewareErrorCode::new(code).expect("built-in CORS codes are valid bounded labels")
}

fn cors_error(code: &'static str) -> MiddlewareConfigError {
    MiddlewareConfigError::middleware(cors_code(code))
}

/// Stable, transport-independent CORS configuration.
///
/// `CorsPolicy::new()` is deliberately restrictive: no origin receives CORS
/// response permission by default. Simple responses receive CORS headers only
/// for an allowed origin; preflight permission additionally requires the
/// requested method and headers to be selected. CORS is not request
/// authentication or CSRF protection.
/// The browser protocol implementation is delegated to `tower-http` behind a
/// Lily-private adapter; no `tower-http` type is part of this public contract.
#[derive(Clone, PartialEq, Eq)]
pub struct CorsPolicy {
    allowed_origins: Vec<String>,
    origin_resolver: Option<CorsOriginResolverRegistration>,
    allowed_methods: Vec<String>,
    allowed_headers: Vec<String>,
    exposed_headers: Vec<String>,
    additional_vary_headers: Vec<String>,
    allow_credentials: bool,
    allow_private_network: bool,
    max_age: Option<Duration>,
    invalid: Option<MiddlewareErrorCode>,
}

impl CorsPolicy {
    /// Creates a deny-by-default CORS policy.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            allowed_origins: Vec::new(),
            origin_resolver: None,
            allowed_methods: Vec::new(),
            allowed_headers: Vec::new(),
            exposed_headers: Vec::new(),
            additional_vary_headers: Vec::new(),
            allow_credentials: false,
            allow_private_network: false,
            max_age: None,
            invalid: None,
        }
    }

    /// Creates an intentionally permissive policy for local development.
    ///
    /// Credentials remain disabled. This helper should not be used as a
    /// production default because every origin, method and request header is
    /// accepted and every response header is exposed to browser code.
    #[must_use]
    pub fn permissive_for_development() -> Self {
        Self::new()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .expose_any_header()
    }

    /// Replaces the exact browser origins accepted by this policy.
    ///
    /// Values must use canonical browser serialization, for example
    /// `https://api.example.com` (without a trailing slash). Use
    /// [`Self::allow_null_origin`] for the opaque `null` origin and
    /// [`Self::allow_any_origin`] for a credential-free wildcard policy. A
    /// policy accepts at most 64 origins, each at most 2 KiB; all configured
    /// origin, method and header strings together are bounded to 32 KiB.
    #[must_use]
    pub fn allow_origins<I, S>(mut self, origins: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if self.invalid.is_some() {
            return self;
        }
        if self.origin_resolver.is_some() {
            return self.with_error(cors_error("CORS_ORIGIN_AUTHORITY_CONFLICT"));
        }
        let remaining = self.replacement_budget(&self.allowed_origins);
        let values = match collect_bounded(
            origins,
            MAX_CORS_ORIGINS,
            MAX_ORIGIN_BYTES,
            remaining,
            "CORS_ORIGIN_LIMIT",
        ) {
            Ok(values) => values,
            Err(error) => return self.with_error(error),
        };
        if let Err(error) = validate_wildcard(&values, "CORS_ORIGIN_WILDCARD_MIXED")
            .and_then(|any| {
                if self.allow_credentials && any {
                    Err(cors_error("CORS_CREDENTIALS_WILDCARD"))
                } else {
                    Ok(())
                }
            })
            .and_then(|()| validate_origins(&values))
        {
            return self.with_error(error);
        }
        self.allowed_origins = values;
        self
    }

    /// Allows the opaque browser origin `null` explicitly.
    #[must_use]
    pub fn allow_null_origin(mut self) -> Self {
        if self.invalid.is_some() {
            return self;
        }
        if self.origin_resolver.is_some() {
            return self.with_error(cors_error("CORS_ORIGIN_AUTHORITY_CONFLICT"));
        }
        if is_wildcard(&self.allowed_origins) {
            return self;
        }
        if !self.allowed_origins.iter().any(|origin| origin == "null") {
            if self.replacement_budget(&self.allowed_origins)
                < self
                    .allowed_origins
                    .iter()
                    .map(String::len)
                    .sum::<usize>()
                    .saturating_add(4)
                || self.allowed_origins.len() == MAX_CORS_ORIGINS
            {
                return self.with_error(cors_error("CORS_ORIGIN_LIMIT"));
            }
            self.allowed_origins.push("null".to_owned());
        }
        self
    }

    /// Allows every origin. This cannot be combined with credentials.
    #[must_use]
    pub fn allow_any_origin(self) -> Self {
        if self.allow_credentials {
            return self.with_error(cors_error("CORS_CREDENTIALS_WILDCARD"));
        }
        self.allow_origins(["*"])
    }

    /// Selects one DI-aware dynamic authority for request-origin decisions.
    ///
    /// This is mutually exclusive with exact, `null` and wildcard origins. Lily constructs one
    /// resolver instance per application; the resolver can only allow or deny the structurally
    /// validated request origin.
    #[must_use]
    pub fn resolve_origins_with<R>(mut self) -> Self
    where
        R: CorsOriginResolver,
    {
        if self.invalid.is_some() {
            return self;
        }
        if !self.allowed_origins.is_empty() || self.origin_resolver.is_some() {
            return self.with_error(cors_error("CORS_ORIGIN_AUTHORITY_CONFLICT"));
        }
        self.origin_resolver = Some(CorsOriginResolverRegistration::of::<R>());
        self
    }

    /// Replaces the exact HTTP methods accepted by preflight requests.
    ///
    /// At most 32 method tokens are retained and each token is limited to 256
    /// bytes. These values share the policy-wide 32 KiB metadata budget.
    #[must_use]
    pub fn allow_methods<I, S>(mut self, methods: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if self.invalid.is_some() {
            return self;
        }
        let values = match collect_bounded(
            methods,
            MAX_CORS_METHODS,
            MAX_TOKEN_BYTES,
            self.replacement_budget(&self.allowed_methods),
            "CORS_METHOD_LIMIT",
        ) {
            Ok(values) => values,
            Err(error) => return self.with_error(error),
        };
        if let Err(error) = validate_wildcard(&values, "CORS_METHOD_WILDCARD_MIXED")
            .and_then(|any| {
                if self.allow_credentials && any {
                    Err(cors_error("CORS_CREDENTIALS_WILDCARD"))
                } else {
                    Ok(())
                }
            })
            .and_then(|()| validate_methods(&values))
        {
            return self.with_error(error);
        }
        self.allowed_methods = values;
        self
    }

    /// Allows every method. This cannot be combined with credentials.
    #[must_use]
    pub fn allow_any_method(self) -> Self {
        if self.allow_credentials {
            return self.with_error(cors_error("CORS_CREDENTIALS_WILDCARD"));
        }
        self.allow_methods(["*"])
    }

    /// Replaces the exact request header names accepted by preflight requests.
    /// Trace propagation headers are never inserted implicitly. At most 64
    /// names are retained, each at most 256 bytes, inside the shared 32 KiB
    /// policy budget.
    #[must_use]
    pub fn allow_headers<I, S>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if self.invalid.is_some() {
            return self;
        }
        let values = match collect_bounded(
            headers,
            MAX_CORS_HEADER_NAMES,
            MAX_TOKEN_BYTES,
            self.replacement_budget(&self.allowed_headers),
            "CORS_HEADER_LIMIT",
        ) {
            Ok(values) => values,
            Err(error) => return self.with_error(error),
        };
        if let Err(error) = validate_wildcard(&values, "CORS_HEADER_WILDCARD_MIXED")
            .and_then(|any| {
                if self.allow_credentials && any {
                    Err(cors_error("CORS_CREDENTIALS_WILDCARD"))
                } else {
                    Ok(())
                }
            })
            .and_then(|()| validate_header_names(&values, "CORS_HEADER_INVALID"))
        {
            return self.with_error(error);
        }
        self.allowed_headers = values;
        self
    }

    /// Allows every request header. This cannot be combined with credentials.
    #[must_use]
    pub fn allow_any_header(self) -> Self {
        if self.allow_credentials {
            return self.with_error(cors_error("CORS_CREDENTIALS_WILDCARD"));
        }
        self.allow_headers(["*"])
    }

    /// Replaces the response header names exposed to browser code.
    ///
    /// At most 64 names are retained, each at most 256 bytes, inside the shared
    /// 32 KiB policy budget.
    #[must_use]
    pub fn expose_headers<I, S>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if self.invalid.is_some() {
            return self;
        }
        let values = match collect_bounded(
            headers,
            MAX_CORS_HEADER_NAMES,
            MAX_TOKEN_BYTES,
            self.replacement_budget(&self.exposed_headers),
            "CORS_EXPOSE_HEADER_LIMIT",
        ) {
            Ok(values) => values,
            Err(error) => return self.with_error(error),
        };
        if let Err(error) = validate_wildcard(&values, "CORS_EXPOSE_WILDCARD_MIXED")
            .and_then(|any| {
                if self.allow_credentials && any {
                    Err(cors_error("CORS_CREDENTIALS_WILDCARD"))
                } else {
                    Ok(())
                }
            })
            .and_then(|()| validate_header_names(&values, "CORS_EXPOSE_HEADER_INVALID"))
        {
            return self.with_error(error);
        }
        self.exposed_headers = values;
        self
    }

    /// Exposes every response header. This cannot be combined with credentials.
    #[must_use]
    pub fn expose_any_header(self) -> Self {
        if self.allow_credentials {
            return self.with_error(cors_error("CORS_CREDENTIALS_WILDCARD"));
        }
        self.expose_headers(["*"])
    }

    /// Enables or disables credentialed browser requests.
    #[must_use]
    pub fn allow_credentials(mut self, allow: bool) -> Self {
        if self.invalid.is_some() {
            return self;
        }
        if allow
            && [
                &self.allowed_origins,
                &self.allowed_methods,
                &self.allowed_headers,
                &self.exposed_headers,
            ]
            .into_iter()
            .any(|values| is_wildcard(values))
        {
            return self.with_error(cors_error("CORS_CREDENTIALS_WILDCARD"));
        }
        self.allow_credentials = allow;
        self
    }

    /// Enables Private Network Access responses for valid, allowed preflight requests.
    ///
    /// The default is disabled. Application-produced PNA control headers are never trusted.
    #[must_use]
    pub fn allow_private_network(mut self, allow: bool) -> Self {
        if self.invalid.is_some() {
            return self;
        }
        self.allow_private_network = allow;
        self
    }

    /// Sets the preflight cache duration.
    ///
    /// The value must use whole-second precision and must not exceed 24 hours.
    #[must_use]
    pub fn max_age(mut self, max_age: Duration) -> Self {
        if self.invalid.is_some() {
            return self;
        }
        if max_age > MAX_CORS_MAX_AGE || max_age.subsec_nanos() != 0 {
            return self.with_error(cors_error("CORS_MAX_AGE_INVALID"));
        }
        self.max_age = Some(max_age);
        self
    }

    /// Disables preflight caching.
    #[must_use]
    pub fn without_max_age(mut self) -> Self {
        if self.invalid.is_some() {
            return self;
        }
        self.max_age = None;
        self
    }

    /// Adds cache-key header names to the protocol-derived `Vary` set.
    ///
    /// Required CORS vary fields cannot be removed through this API. At most 64
    /// additional names of at most 256 bytes are retained. The final canonical
    /// `Vary` value is additionally bounded to 8 KiB.
    #[must_use]
    pub fn vary_by<I, S>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        if self.invalid.is_some() {
            return self;
        }
        let values = match collect_bounded(
            headers,
            MAX_CORS_HEADER_NAMES,
            MAX_TOKEN_BYTES,
            self.replacement_budget(&self.additional_vary_headers),
            "CORS_VARY_HEADER_LIMIT",
        ) {
            Ok(values) => values,
            Err(error) => return self.with_error(error),
        };
        if values.iter().any(|value| value == "*") {
            return self.with_error(cors_error("CORS_VARY_HEADER_INVALID"));
        }
        if let Err(error) = validate_header_names(&values, "CORS_VARY_HEADER_INVALID") {
            return self.with_error(error);
        }
        self.additional_vary_headers = values;
        self
    }

    /// Validates all user-controlled values before a Tower layer is built.
    pub fn validate(&self) -> Result<(), MiddlewareConfigError> {
        if let Some(code) = self.invalid {
            return Err(MiddlewareConfigError::middleware(code));
        }
        validate_collection_bounds(
            &self.allowed_origins,
            MAX_CORS_ORIGINS,
            MAX_ORIGIN_BYTES,
            "CORS_ORIGIN_LIMIT",
        )?;
        validate_collection_bounds(
            &self.allowed_methods,
            MAX_CORS_METHODS,
            MAX_TOKEN_BYTES,
            "CORS_METHOD_LIMIT",
        )?;
        validate_collection_bounds(
            &self.allowed_headers,
            MAX_CORS_HEADER_NAMES,
            MAX_TOKEN_BYTES,
            "CORS_HEADER_LIMIT",
        )?;
        validate_collection_bounds(
            &self.exposed_headers,
            MAX_CORS_HEADER_NAMES,
            MAX_TOKEN_BYTES,
            "CORS_EXPOSE_HEADER_LIMIT",
        )?;
        validate_collection_bounds(
            &self.additional_vary_headers,
            MAX_CORS_HEADER_NAMES,
            MAX_TOKEN_BYTES,
            "CORS_VARY_HEADER_LIMIT",
        )?;

        let retained_bytes = self
            .allowed_origins
            .iter()
            .chain(&self.allowed_methods)
            .chain(&self.allowed_headers)
            .chain(&self.exposed_headers)
            .chain(&self.additional_vary_headers)
            .fold(0usize, |total, value| total.saturating_add(value.len()));
        if retained_bytes > MAX_CORS_POLICY_BYTES {
            return Err(cors_error("CORS_POLICY_BYTES_EXCEEDED"));
        }

        if self.origin_resolver.is_some() && !self.allowed_origins.is_empty() {
            return Err(cors_error("CORS_ORIGIN_AUTHORITY_CONFLICT"));
        }

        let any_origin = validate_wildcard(&self.allowed_origins, "CORS_ORIGIN_WILDCARD_MIXED")?;
        let any_method = validate_wildcard(&self.allowed_methods, "CORS_METHOD_WILDCARD_MIXED")?;
        let any_header = validate_wildcard(&self.allowed_headers, "CORS_HEADER_WILDCARD_MIXED")?;
        let expose_any = validate_wildcard(&self.exposed_headers, "CORS_EXPOSE_WILDCARD_MIXED")?;
        if self.allow_credentials && (any_origin || any_method || any_header || expose_any) {
            return Err(cors_error("CORS_CREDENTIALS_WILDCARD"));
        }

        validate_origins(&self.allowed_origins)?;
        validate_methods(&self.allowed_methods)?;
        validate_header_names(&self.allowed_headers, "CORS_HEADER_INVALID")?;
        validate_header_names(&self.exposed_headers, "CORS_EXPOSE_HEADER_INVALID")?;
        validate_header_names(&self.additional_vary_headers, "CORS_VARY_HEADER_INVALID")?;
        validate_compiled_vary(
            &self.allowed_origins,
            self.origin_resolver.is_some(),
            self.allow_private_network,
            &self.additional_vary_headers,
        )?;

        if let Some(max_age) = self.max_age
            && (max_age > MAX_CORS_MAX_AGE || max_age.subsec_nanos() != 0)
        {
            return Err(cors_error("CORS_MAX_AGE_INVALID"));
        }
        Ok(())
    }

    fn retained_bytes(&self) -> usize {
        self.allowed_origins
            .iter()
            .chain(&self.allowed_methods)
            .chain(&self.allowed_headers)
            .chain(&self.exposed_headers)
            .chain(&self.additional_vary_headers)
            .map(String::len)
            .sum()
    }

    fn replacement_budget(&self, replaced: &[String]) -> usize {
        let replaced_bytes = replaced.iter().map(String::len).sum::<usize>();
        MAX_CORS_POLICY_BYTES.saturating_sub(self.retained_bytes().saturating_sub(replaced_bytes))
    }

    fn with_error(mut self, error: MiddlewareConfigError) -> Self {
        self.invalid = Some(cors_code(error.diagnostic_code()));
        self
    }
}

impl Default for CorsPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for CorsPolicy {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CorsPolicy")
            .field("origin_count", &self.allowed_origins.len())
            .field("dynamic_origin", &self.origin_resolver.is_some())
            .field("method_count", &self.allowed_methods.len())
            .field("allowed_header_count", &self.allowed_headers.len())
            .field("exposed_header_count", &self.exposed_headers.len())
            .field("additional_vary_count", &self.additional_vary_headers.len())
            .field("allow_credentials", &self.allow_credentials)
            .field("allow_private_network", &self.allow_private_network)
            .field("max_age", &self.max_age)
            .field("is_invalid", &self.invalid.is_some())
            .finish()
    }
}

fn validate_collection_bounds(
    values: &[String],
    max_count: usize,
    max_item_bytes: usize,
    error_code: &'static str,
) -> Result<(), MiddlewareConfigError> {
    if values.len() > max_count
        || values
            .iter()
            .any(|value| value.is_empty() || value.len() > max_item_bytes)
    {
        return Err(cors_error(error_code));
    }
    Ok(())
}

fn collect_bounded<I, S>(
    values: I,
    max_count: usize,
    max_item_bytes: usize,
    max_retained_bytes: usize,
    error_code: &'static str,
) -> Result<Vec<String>, MiddlewareConfigError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut retained = Vec::new();
    let mut retained_bytes = 0usize;
    for value in values {
        if retained.len() == max_count {
            return Err(cors_error(error_code));
        }
        let value = value.as_ref();
        if value.is_empty()
            || value.len() > max_item_bytes
            || retained_bytes.saturating_add(value.len()) > max_retained_bytes
        {
            return Err(cors_error(error_code));
        }
        retained_bytes += value.len();
        retained.push(value.to_owned());
    }
    Ok(retained)
}

fn validate_wildcard(
    values: &[String],
    mixed_error_code: &'static str,
) -> Result<bool, MiddlewareConfigError> {
    let has_wildcard = values.iter().any(|value| value == "*");
    if has_wildcard && values.len() != 1 {
        return Err(cors_error(mixed_error_code));
    }
    Ok(has_wildcard)
}

fn is_wildcard(values: &[String]) -> bool {
    values.len() == 1 && values[0] == "*"
}

fn validate_origins(origins: &[String]) -> Result<(), MiddlewareConfigError> {
    for (index, origin) in origins.iter().enumerate() {
        if origins[..index].contains(origin) {
            return Err(cors_error("CORS_ORIGIN_DUPLICATE"));
        }
        if origin == "*" || origin == "null" {
            continue;
        }
        let parsed = Url::parse(origin).map_err(|_| cors_error("CORS_ORIGIN_INVALID"))?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(cors_error("CORS_ORIGIN_INVALID"));
        }
        if !parsed.username().is_empty() {
            return Err(cors_error("CORS_ORIGIN_INVALID"));
        }
        if parsed.password().is_some() {
            return Err(cors_error("CORS_ORIGIN_INVALID"));
        }
        if parsed.query().is_some() {
            return Err(cors_error("CORS_ORIGIN_INVALID"));
        }
        if parsed.fragment().is_some() {
            return Err(cors_error("CORS_ORIGIN_INVALID"));
        }
        if parsed.path() != "/" {
            return Err(cors_error("CORS_ORIGIN_INVALID"));
        }
        if parsed.origin().ascii_serialization() != *origin {
            return Err(cors_error("CORS_ORIGIN_INVALID"));
        }
        if HeaderValue::from_str(origin).is_err() {
            return Err(cors_error("CORS_ORIGIN_INVALID"));
        }
    }
    Ok(())
}

fn validate_methods(methods: &[String]) -> Result<(), MiddlewareConfigError> {
    for (index, method) in methods.iter().enumerate() {
        if method == "*" {
            continue;
        }
        if Method::from_bytes(method.as_bytes()).is_err() {
            return Err(cors_error("CORS_METHOD_INVALID"));
        }
        if methods[..index].contains(method) {
            return Err(cors_error("CORS_METHOD_DUPLICATE"));
        }
    }
    Ok(())
}

fn validate_header_names(
    headers: &[String],
    invalid_error_code: &'static str,
) -> Result<(), MiddlewareConfigError> {
    for (index, header) in headers.iter().enumerate() {
        if header == "*" {
            continue;
        }
        if HeaderName::from_bytes(header.as_bytes()).is_err() {
            return Err(cors_error(invalid_error_code));
        }
        if headers[..index]
            .iter()
            .any(|existing| existing.eq_ignore_ascii_case(header))
        {
            return Err(cors_error("CORS_HEADER_DUPLICATE"));
        }
    }
    Ok(())
}

/// Bounds the exact canonical `Vary` value produced by a validated policy.
///
/// Runtime canonicalization may still select `Vary: *` for an untrusted inner
/// response, but Lily-owned configuration must never rely on that fallback.
fn validate_compiled_vary(
    allowed_origins: &[String],
    dynamic_origin: bool,
    private_network: bool,
    additional_headers: &[String],
) -> Result<(), MiddlewareConfigError> {
    let derives_origin =
        dynamic_origin || (!allowed_origins.is_empty() && !is_wildcard(allowed_origins));
    let preflight_varies = dynamic_origin || private_network;
    let derived_headers = [
        (derives_origin, ORIGIN.as_str()),
        (preflight_varies, ACCESS_CONTROL_REQUEST_METHOD.as_str()),
        (preflight_varies, ACCESS_CONTROL_REQUEST_HEADERS.as_str()),
        (private_network, "access-control-request-private-network"),
    ];
    let derived_headers = derived_headers
        .into_iter()
        .filter(|(required, header)| {
            *required
                && !additional_headers
                    .iter()
                    .any(|configured| configured.eq_ignore_ascii_case(header))
        })
        .map(|(_, header)| header)
        .collect::<Vec<_>>();
    let derived_count = derived_headers.len();
    let field_count = additional_headers.len().saturating_add(derived_count);
    if field_count > MAX_CORS_HEADER_NAMES {
        return Err(cors_error("CORS_VARY_HEADER_LIMIT"));
    }

    let field_bytes = additional_headers
        .iter()
        .fold(0usize, |total, header| total.saturating_add(header.len()))
        .saturating_add(
            derived_headers
                .iter()
                .fold(0usize, |total, header| total.saturating_add(header.len())),
        );
    let separator_bytes = field_count.saturating_sub(1).saturating_mul(2);
    if field_bytes.saturating_add(separator_bytes) > MAX_VARY_VALUE_BYTES {
        return Err(cors_error("CORS_VARY_HEADER_LIMIT"));
    }
    Ok(())
}

/// Allocation-free structural classification used before a request body is
/// consumed or a Tower preflight response is selected.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorsRequestKind {
    NonCors,
    Simple,
    Preflight,
}

/// Bounded structural rejection produced before Tower sees the request.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CorsRuntimeRejection {
    AmbiguousHeaders,
    InvalidOrigin,
    InvalidPreflight,
    InvalidRequestedHeaders,
    InvalidPrivateNetwork,
    OriginResolverUnavailable { code: MiddlewareErrorCode },
    OriginResolverInternal { code: MiddlewareErrorCode },
    OriginResolverPanicked,
}

impl CorsRuntimeRejection {
    #[must_use]
    pub const fn diagnostic_code(self) -> &'static str {
        match self {
            Self::AmbiguousHeaders => "CORS_HEADERS_AMBIGUOUS",
            Self::InvalidOrigin => "CORS_ORIGIN_INVALID",
            Self::InvalidPreflight => "CORS_PREFLIGHT_INVALID",
            Self::InvalidRequestedHeaders => "CORS_REQUEST_HEADERS_INVALID",
            Self::InvalidPrivateNetwork => "CORS_PRIVATE_NETWORK_INVALID",
            Self::OriginResolverUnavailable { code } | Self::OriginResolverInternal { code } => {
                code.as_str()
            }
            Self::OriginResolverPanicked => "CORS_ORIGIN_RESOLVER_PANICKED",
        }
    }

    #[must_use]
    pub const fn status(self) -> http::StatusCode {
        match self {
            Self::AmbiguousHeaders
            | Self::InvalidOrigin
            | Self::InvalidPreflight
            | Self::InvalidRequestedHeaders
            | Self::InvalidPrivateNetwork => http::StatusCode::BAD_REQUEST,
            Self::OriginResolverUnavailable { .. } => http::StatusCode::SERVICE_UNAVAILABLE,
            Self::OriginResolverInternal { .. } | Self::OriginResolverPanicked => {
                http::StatusCode::INTERNAL_SERVER_ERROR
            }
        }
    }
}

#[derive(Clone)]
struct CorsResponsePolicy {
    any_origin: bool,
    dynamic_origin: bool,
    origins: Vec<HeaderValue>,
    allow_credentials: bool,
    allow_private_network: bool,
    expose_headers: Option<HeaderValue>,
    vary_headers: Vec<HeaderName>,
}

impl CorsResponsePolicy {
    fn decorate<ReqBody, ResBody>(
        &self,
        request: &Request<ReqBody>,
        response: &mut Response<ResBody>,
    ) {
        remove_policy_owned_headers(response.headers_mut());

        let request_kind = classify_cors_request(request.method(), request.headers()).ok();
        let structurally_valid = request_kind.is_some();
        let allowed_origin = if self.dynamic_origin && structurally_valid {
            request
                .extensions()
                .get::<DynamicCorsOriginDecision>()
                .filter(|decision| decision.allowed)
                .and_then(|_| request.headers().get(ORIGIN))
                .cloned()
        } else if self.any_origin && structurally_valid {
            Some(HeaderValue::from_static("*"))
        } else if structurally_valid {
            request
                .headers()
                .get_all(ORIGIN)
                .iter()
                .next()
                .filter(|_| request.headers().get_all(ORIGIN).iter().count() == 1)
                .filter(|origin| self.origins.contains(origin))
                .cloned()
        } else {
            None
        };
        if let Some(origin) = allowed_origin {
            response
                .headers_mut()
                .insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin);
            if self.allow_credentials {
                response.headers_mut().insert(
                    ACCESS_CONTROL_ALLOW_CREDENTIALS,
                    HeaderValue::from_static("true"),
                );
            }
            if let Some(expose_headers) = &self.expose_headers {
                response
                    .headers_mut()
                    .insert(ACCESS_CONTROL_EXPOSE_HEADERS, expose_headers.clone());
            }
            if self.allow_private_network
                && request_kind == Some(CorsRequestKind::Preflight)
                && request
                    .headers()
                    .get(ACCESS_CONTROL_REQUEST_PRIVATE_NETWORK)
                    == Some(&HeaderValue::from_static("true"))
            {
                response.headers_mut().insert(
                    ACCESS_CONTROL_ALLOW_PRIVATE_NETWORK,
                    HeaderValue::from_static("true"),
                );
            }
        }
        for vary in &self.vary_headers {
            response
                .headers_mut()
                .append(VARY, HeaderValue::from(vary.clone()));
        }
        finalize_cors_response(response.headers_mut());
    }
}

/// Lily-private wrapper around the selected `tower-http` CORS layer.
#[doc(hidden)]
#[derive(Clone)]
pub struct CorsLayerAdapter {
    inner: CorsLayer,
    response_policy: CorsResponsePolicy,
    resolver_registration: Option<CorsOriginResolverRegistration>,
    origin_resolver: Option<Arc<dyn CorsOriginResolver>>,
}

impl CorsLayerAdapter {
    /// Validates and compiles a stable Lily policy without exposing Tower.
    pub fn try_new(policy: &CorsPolicy) -> Result<Self, MiddlewareConfigError> {
        policy.validate()?;
        let any_origin = is_wildcard(&policy.allowed_origins);
        let dynamic_origin = policy.origin_resolver.is_some();
        let any_method = is_wildcard(&policy.allowed_methods);
        let any_header = is_wildcard(&policy.allowed_headers);
        let expose_any = is_wildcard(&policy.exposed_headers);

        let origins = policy
            .allowed_origins
            .iter()
            .filter(|origin| origin.as_str() != "*")
            .map(|origin| HeaderValue::from_str(origin).expect("validated CORS origin"))
            .collect::<Vec<_>>();
        let mut layer = CorsLayer::new().allow_credentials(policy.allow_credentials);
        layer = if dynamic_origin {
            layer.allow_origin(AllowOrigin::predicate(|_, parts| {
                parts
                    .extensions
                    .get::<DynamicCorsOriginDecision>()
                    .is_some_and(|decision| decision.allowed)
            }))
        } else if any_origin {
            layer.allow_origin(Any)
        } else {
            layer.allow_origin(AllowOrigin::list(origins.clone()))
        };
        layer = if any_method {
            layer.allow_methods(Any)
        } else {
            layer.allow_methods(AllowMethods::list(policy.allowed_methods.iter().map(
                |method| Method::from_bytes(method.as_bytes()).expect("validated method"),
            )))
        };
        layer = if any_header {
            layer.allow_headers(Any)
        } else {
            layer.allow_headers(AllowHeaders::list(policy.allowed_headers.iter().map(
                |header| HeaderName::from_bytes(header.as_bytes()).expect("validated header"),
            )))
        };
        layer = if expose_any {
            layer.expose_headers(Any)
        } else {
            layer.expose_headers(
                policy
                    .exposed_headers
                    .iter()
                    .map(|header| {
                        HeaderName::from_bytes(header.as_bytes()).expect("validated exposed header")
                    })
                    .collect::<Vec<_>>(),
            )
        };
        if let Some(max_age) = policy.max_age {
            layer = layer.max_age(max_age);
        }
        layer = layer.allow_private_network(policy.allow_private_network);

        let mut vary_headers = policy
            .additional_vary_headers
            .iter()
            .map(|header| HeaderName::from_bytes(header.as_bytes()).expect("validated vary header"))
            .collect::<Vec<_>>();
        if (dynamic_origin || (!any_origin && !origins.is_empty()))
            && !vary_headers.contains(&ORIGIN)
        {
            vary_headers.push(ORIGIN);
        }
        if (dynamic_origin || policy.allow_private_network)
            && !vary_headers.contains(&ACCESS_CONTROL_REQUEST_METHOD)
        {
            vary_headers.push(ACCESS_CONTROL_REQUEST_METHOD);
        }
        if (dynamic_origin || policy.allow_private_network)
            && !vary_headers.contains(&ACCESS_CONTROL_REQUEST_HEADERS)
        {
            vary_headers.push(ACCESS_CONTROL_REQUEST_HEADERS);
        }
        if policy.allow_private_network
            && !vary_headers.contains(&ACCESS_CONTROL_REQUEST_PRIVATE_NETWORK)
        {
            vary_headers.push(ACCESS_CONTROL_REQUEST_PRIVATE_NETWORK.clone());
        }
        if dynamic_origin
            || policy.allow_private_network
            || !policy.additional_vary_headers.is_empty()
            || (!any_origin && origins.is_empty())
        {
            layer = layer.vary(Vary::list(vary_headers.clone()));
        }

        let expose_headers = if expose_any {
            Some(HeaderValue::from_static("*"))
        } else {
            comma_separated_header_names(&policy.exposed_headers)
        };
        Ok(Self {
            inner: layer,
            response_policy: CorsResponsePolicy {
                any_origin,
                dynamic_origin,
                origins,
                allow_credentials: policy.allow_credentials,
                allow_private_network: policy.allow_private_network,
                expose_headers,
                vary_headers,
            },
            resolver_registration: policy.origin_resolver,
            origin_resolver: None,
        })
    }

    /// Initializes the optional dynamic origin authority once from the application DI graph.
    pub async fn initialize_origin_resolver(
        self,
        extensions: Arc<Extensions>,
    ) -> Result<Self, CorsOriginResolverInitError> {
        let mut registry = CorsOriginResolverRegistry::default();
        self.initialize_origin_resolver_with(extensions, &mut registry)
            .await
    }

    /// Initializes or reuses the optional resolver through one application build-local cache.
    pub async fn initialize_origin_resolver_with(
        mut self,
        extensions: Arc<Extensions>,
        registry: &mut CorsOriginResolverRegistry,
    ) -> Result<Self, CorsOriginResolverInitError> {
        if let Some(registration) = self.resolver_registration {
            let resolver = if let Some(resolver) = registry.resolvers.get(&registration.type_id) {
                Arc::clone(resolver)
            } else {
                let resolver = registration.instantiate(extensions).await?;
                registry
                    .resolvers
                    .insert(registration.type_id, Arc::clone(&resolver));
                resolver
            };
            self.origin_resolver = Some(resolver);
        }
        Ok(self)
    }

    #[must_use]
    pub const fn has_dynamic_origin(&self) -> bool {
        self.resolver_registration.is_some()
    }

    /// Classifies a request once and records the trusted result for the
    /// compiled CORS service to consume after transport-level body checks.
    ///
    /// Both acceptance and bounded rejection are recorded. Repeating this
    /// operation replaces the previous private marker with a result derived
    /// from the request's current method and headers.
    pub fn classify_request<Body>(
        request: &mut Request<Body>,
    ) -> Result<CorsRequestKind, CorsRuntimeRejection> {
        let classification = classify_cors_request(request.method(), request.headers());
        request
            .extensions_mut()
            .insert(ClassifiedCorsRequest(classification));
        classification
    }

    /// Resolves one dynamic origin decision and stores only the bounded decision in private
    /// request state. Repeated calls reuse that state and never invoke the application resolver
    /// again.
    pub async fn resolve_origin<Body>(
        &self,
        request: &mut Request<Body>,
    ) -> Result<CorsRequestKind, CorsRuntimeRejection> {
        let classification = if let Some(classified) =
            request.extensions().get::<ClassifiedCorsRequest>().copied()
        {
            classified.0
        } else {
            Self::classify_request(request)
        };
        let request_kind = classification?;
        if !self.has_dynamic_origin() || request_kind == CorsRequestKind::NonCors {
            return Ok(request_kind);
        }
        if request
            .extensions()
            .get::<DynamicCorsOriginDecision>()
            .is_some()
        {
            return Ok(request_kind);
        }

        let Some(resolver) = self.origin_resolver.as_ref() else {
            let rejection = CorsRuntimeRejection::OriginResolverInternal {
                code: MiddlewareErrorCode::new("CORS_ORIGIN_RESOLVER_UNINITIALIZED")
                    .expect("built-in CORS resolver state code is valid"),
            };
            request
                .extensions_mut()
                .insert(ClassifiedCorsRequest(Err(rejection)));
            return Err(rejection);
        };

        let origin = request
            .headers()
            .get(ORIGIN)
            .and_then(|origin| origin.to_str().ok())
            .expect("classified CORS request has one canonical origin");
        let method = if request_kind == CorsRequestKind::Preflight {
            Method::from_bytes(
                request
                    .headers()
                    .get(ACCESS_CONTROL_REQUEST_METHOD)
                    .expect("classified preflight has a target method")
                    .as_bytes(),
            )
            .expect("classified preflight target method is valid")
        } else {
            request.method().clone()
        };
        let host = request
            .headers()
            .get(http::header::HOST)
            .and_then(|host| host.to_str().ok());
        let context = CorsOriginContext {
            origin,
            method,
            path: request.uri().path(),
            host,
            preflight: request_kind == CorsRequestKind::Preflight,
            cancellation: request
                .extensions()
                .get::<lily_cancellation::ExecutionCancellation>()
                .cloned()
                .unwrap_or_else(|| {
                    lily_cancellation::__private::ExecutionCancellationSource::default().view()
                }),
        };
        let resolution = CatchUnwindFuture::new(resolver.allows(&context)).await;
        match resolution {
            Ok(Ok(allowed)) => {
                request
                    .extensions_mut()
                    .insert(DynamicCorsOriginDecision { allowed });
                Ok(request_kind)
            }
            Ok(Err(CorsOriginResolverError::Unavailable { code })) => {
                let rejection = CorsRuntimeRejection::OriginResolverUnavailable { code };
                request
                    .extensions_mut()
                    .insert(ClassifiedCorsRequest(Err(rejection)));
                Err(rejection)
            }
            Ok(Err(CorsOriginResolverError::Internal { code })) => {
                let rejection = CorsRuntimeRejection::OriginResolverInternal { code };
                request
                    .extensions_mut()
                    .insert(ClassifiedCorsRequest(Err(rejection)));
                Err(rejection)
            }
            Err(_) => {
                let rejection = CorsRuntimeRejection::OriginResolverPanicked;
                request
                    .extensions_mut()
                    .insert(ClassifiedCorsRequest(Err(rejection)));
                Err(rejection)
            }
        }
    }

    /// Copies private classification and dynamic decision state into a bounded request-head
    /// snapshot used to decorate transport-generated responses.
    pub fn copy_request_state<SourceBody, TargetBody>(
        source: &Request<SourceBody>,
        target: &mut Request<TargetBody>,
    ) {
        if let Some(classification) = source.extensions().get::<ClassifiedCorsRequest>() {
            target.extensions_mut().insert(*classification);
        }
        if let Some(decision) = source.extensions().get::<DynamicCorsOriginDecision>() {
            target.extensions_mut().insert(*decision);
        }
    }

    /// Applies normal-response CORS semantics to a response produced outside
    /// the inner application service, without turning OPTIONS into preflight.
    pub fn decorate_response<ReqBody, ResBody>(
        &self,
        request: &Request<ReqBody>,
        response: &mut Response<ResBody>,
    ) {
        self.response_policy.decorate(request, response);
    }

    /// Wraps one standard HTTP service without leaking a Tower type.
    ///
    /// This is a service-construction operation and must not be called per
    /// request. The returned cloneable service is reused for request calls.
    #[must_use]
    pub fn layer<S>(&self, inner: S) -> CorsService<S> {
        CorsService {
            inner: self.inner.layer(RestoreOptionsAndSanitize::new(inner)),
            response_policy: self.response_policy.clone(),
        }
    }
}

impl fmt::Debug for CorsLayerAdapter {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CorsLayerAdapter")
            .finish_non_exhaustive()
    }
}

fn comma_separated_header_names(headers: &[String]) -> Option<HeaderValue> {
    if headers.is_empty() {
        return None;
    }
    let mut value = String::new();
    for header in headers {
        if !value.is_empty() {
            value.push(',');
        }
        value.push_str(header);
    }
    Some(HeaderValue::from_str(&value).expect("validated CORS header list"))
}

#[derive(Clone, Copy)]
struct RestoreOptionsMethod;

/// Trusted result produced by the Lily transport's one structural CORS
/// classification pass. The private extension type prevents application code
/// from forging a classification before [`CorsService`] consumes it.
#[derive(Clone, Copy)]
struct ClassifiedCorsRequest(Result<CorsRequestKind, CorsRuntimeRejection>);

#[derive(Clone, Copy)]
struct DynamicCorsOriginDecision {
    allowed: bool,
}

#[derive(Clone)]
struct RestoreOptionsAndSanitize<S> {
    inner: S,
}

impl<S> RestoreOptionsAndSanitize<S> {
    const fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for RestoreOptionsAndSanitize<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = SanitizedResponseFuture<S::Future>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, mut request: Request<ReqBody>) -> Self::Future {
        request
            .extensions_mut()
            .remove::<DynamicCorsOriginDecision>();
        if request
            .extensions_mut()
            .remove::<RestoreOptionsMethod>()
            .is_some()
        {
            *request.method_mut() = Method::OPTIONS;
        }
        SanitizedResponseFuture {
            inner: self.inner.call(request),
        }
    }
}

pin_project! {
    struct SanitizedResponseFuture<F> {
        #[pin]
        inner: F,
    }
}

impl<F, ResBody, Error> Future for SanitizedResponseFuture<F>
where
    F: Future<Output = Result<Response<ResBody>, Error>>,
{
    type Output = Result<Response<ResBody>, Error>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().inner.poll(context) {
            Poll::Ready(Ok(mut response)) => {
                remove_policy_owned_headers(response.headers_mut());
                Poll::Ready(Ok(response))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Lily-private CORS service. Its representation intentionally hides
/// `tower_http::cors::Cors` from downstream signatures.
#[doc(hidden)]
#[derive(Clone)]
pub struct CorsService<S> {
    inner: Cors<RestoreOptionsAndSanitize<S>>,
    response_policy: CorsResponsePolicy,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for CorsService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    ResBody: Default,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = CorsResponseFuture<S::Future, ResBody>;

    fn poll_ready(&mut self, context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(context)
    }

    fn call(&mut self, mut request: Request<ReqBody>) -> Self::Future {
        let classification = request
            .extensions_mut()
            .remove::<ClassifiedCorsRequest>()
            .map_or_else(
                || classify_cors_request(request.method(), request.headers()),
                |classified| classified.0,
            );
        let request_kind = match classification {
            Ok(kind) => kind,
            Err(rejection) => {
                let mut response = Response::new(ResBody::default());
                *response.status_mut() = rejection.status();
                response.extensions_mut().insert(rejection);
                self.response_policy.decorate(&request, &mut response);
                return CorsResponseFuture::ready(response);
            }
        };

        if request.method() == Method::OPTIONS && request_kind != CorsRequestKind::Preflight {
            request.extensions_mut().insert(RestoreOptionsMethod);
            // tower-http treats every OPTIONS request as a preflight. This
            // marker selects its normal-response path while the Lily app still
            // receives the original OPTIONS method.
            *request.method_mut() = Method::GET;
        }
        CorsResponseFuture::tower(self.inner.call(request))
    }
}

fn classify_cors_request(
    method: &Method,
    headers: &HeaderMap,
) -> Result<CorsRequestKind, CorsRuntimeRejection> {
    let origin_values = headers.get_all(ORIGIN);
    let request_method_values = headers.get_all(ACCESS_CONTROL_REQUEST_METHOD);
    let request_header_values = headers.get_all(ACCESS_CONTROL_REQUEST_HEADERS);
    let private_network_values = headers.get_all(&ACCESS_CONTROL_REQUEST_PRIVATE_NETWORK);
    let origin_count = origin_values.iter().count();
    let request_method_count = request_method_values.iter().count();
    let request_header_count = request_header_values.iter().count();
    let private_network_count = private_network_values.iter().count();

    if origin_count > 1
        || request_method_count > 1
        || request_header_count > 1
        || private_network_count > 1
    {
        return Err(CorsRuntimeRejection::AmbiguousHeaders);
    }
    if let Some(origin) = origin_values.iter().next()
        && !valid_request_origin(origin)
    {
        return Err(CorsRuntimeRejection::InvalidOrigin);
    }
    if *method != Method::OPTIONS {
        if private_network_count != 0 {
            return Err(CorsRuntimeRejection::InvalidPrivateNetwork);
        }
        return Ok(if origin_count == 1 {
            CorsRequestKind::Simple
        } else {
            CorsRequestKind::NonCors
        });
    }
    if request_method_count == 1 {
        if origin_count != 1
            || request_method_values.iter().next().is_none_or(|method| {
                method.as_bytes().len() > MAX_TOKEN_BYTES
                    || Method::from_bytes(method.as_bytes()).is_err()
            })
        {
            return Err(CorsRuntimeRejection::InvalidPreflight);
        }
        if request_header_values
            .iter()
            .next()
            .is_some_and(|headers| !valid_requested_header_list(headers))
        {
            return Err(CorsRuntimeRejection::InvalidRequestedHeaders);
        }
        if private_network_values
            .iter()
            .next()
            .is_some_and(|value| value.as_bytes() != b"true")
        {
            return Err(CorsRuntimeRejection::InvalidPrivateNetwork);
        }
        return Ok(CorsRequestKind::Preflight);
    }
    if request_header_count != 0 || private_network_count != 0 {
        return Err(CorsRuntimeRejection::InvalidPreflight);
    }
    if origin_count == 1 {
        return Err(CorsRuntimeRejection::InvalidPreflight);
    }
    Ok(CorsRequestKind::NonCors)
}

fn valid_request_origin(value: &HeaderValue) -> bool {
    let Ok(value) = value.to_str() else {
        return false;
    };
    if value == "null" {
        return true;
    }
    if value.len() > MAX_ORIGIN_BYTES {
        return false;
    }
    let Ok(parsed) = Url::parse(value) else {
        return false;
    };
    if !matches!(parsed.scheme(), "http" | "https") {
        return false;
    }
    if !parsed.username().is_empty() {
        return false;
    }
    if parsed.password().is_some() {
        return false;
    }
    if parsed.query().is_some() {
        return false;
    }
    if parsed.fragment().is_some() {
        return false;
    }
    if parsed.path() != "/" {
        return false;
    }
    parsed.origin().ascii_serialization() == value
}

fn valid_requested_header_list(value: &HeaderValue) -> bool {
    let Ok(value) = value.to_str() else {
        return false;
    };
    if value.len() > MAX_VARY_VALUE_BYTES {
        return false;
    }
    let mut count = 0usize;
    for item in value.split(',') {
        let item = item.trim();
        count = count.saturating_add(1);
        if item.is_empty()
            || item.len() > MAX_TOKEN_BYTES
            || count > MAX_CORS_HEADER_NAMES
            || HeaderName::from_bytes(item.as_bytes()).is_err()
        {
            return false;
        }
    }
    true
}

pin_project! {
    #[project = CorsResponseFutureKindProj]
    enum CorsResponseFutureKind<F, ResBody> {
        Tower {
            #[pin]
            future: tower_http::cors::ResponseFuture<SanitizedResponseFuture<F>>,
        },
        Ready {
            response: Option<Response<ResBody>>,
        },
    }
}

pin_project! {
    /// Lily-private future returned by [`CorsService`].
    #[doc(hidden)]
    pub struct CorsResponseFuture<F, ResBody> {
        #[pin]
        kind: CorsResponseFutureKind<F, ResBody>,
    }
}

impl<F, ResBody> CorsResponseFuture<F, ResBody> {
    fn tower(future: tower_http::cors::ResponseFuture<SanitizedResponseFuture<F>>) -> Self {
        Self {
            kind: CorsResponseFutureKind::Tower { future },
        }
    }

    fn ready(response: Response<ResBody>) -> Self {
        Self {
            kind: CorsResponseFutureKind::Ready {
                response: Some(response),
            },
        }
    }
}

impl<F, ResBody, Error> Future for CorsResponseFuture<F, ResBody>
where
    F: Future<Output = Result<Response<ResBody>, Error>>,
    ResBody: Default,
{
    type Output = Result<Response<ResBody>, Error>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().kind.project() {
            CorsResponseFutureKindProj::Tower { future } => match future.poll(context) {
                Poll::Ready(Ok(mut response)) => {
                    finalize_cors_response(response.headers_mut());
                    Poll::Ready(Ok(response))
                }
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Pending => Poll::Pending,
            },
            CorsResponseFutureKindProj::Ready { response } => Poll::Ready(Ok(response
                .take()
                .expect("CORS response future polled after completion"))),
        }
    }
}

fn remove_policy_owned_headers(headers: &mut HeaderMap) {
    for name in [
        &ACCESS_CONTROL_ALLOW_ORIGIN,
        &ACCESS_CONTROL_ALLOW_CREDENTIALS,
        &ACCESS_CONTROL_ALLOW_HEADERS,
        &ACCESS_CONTROL_ALLOW_METHODS,
        &ACCESS_CONTROL_EXPOSE_HEADERS,
        &ACCESS_CONTROL_MAX_AGE,
        &ACCESS_CONTROL_ALLOW_PRIVATE_NETWORK,
    ] {
        headers.remove(name);
    }
}

fn finalize_cors_response(headers: &mut HeaderMap) {
    if !headers.contains_key(ACCESS_CONTROL_ALLOW_ORIGIN) {
        for name in [
            &ACCESS_CONTROL_ALLOW_CREDENTIALS,
            &ACCESS_CONTROL_ALLOW_HEADERS,
            &ACCESS_CONTROL_ALLOW_METHODS,
            &ACCESS_CONTROL_EXPOSE_HEADERS,
            &ACCESS_CONTROL_MAX_AGE,
            &ACCESS_CONTROL_ALLOW_PRIVATE_NETWORK,
        ] {
            headers.remove(name);
        }
    }
    canonicalize_vary(headers);
}

fn canonicalize_vary(headers: &mut HeaderMap) {
    let mut names = Vec::<HeaderName>::new();
    let mut invalid_or_unbounded = false;
    let mut wildcard = false;

    'values: for value in headers.get_all(VARY).iter() {
        let Ok(value) = value.to_str() else {
            invalid_or_unbounded = true;
            break;
        };
        for token in value.split(',').map(str::trim) {
            if token.is_empty() {
                continue;
            }
            if token == "*" {
                wildcard = true;
                break 'values;
            }
            let Ok(name) = HeaderName::from_bytes(token.as_bytes()) else {
                invalid_or_unbounded = true;
                break 'values;
            };
            if !names.contains(&name) {
                names.push(name);
                if names.len() > MAX_CORS_HEADER_NAMES {
                    invalid_or_unbounded = true;
                    break 'values;
                }
            }
        }
    }

    headers.remove(VARY);
    if wildcard || invalid_or_unbounded {
        headers.insert(VARY, HeaderValue::from_static("*"));
        return;
    }
    if names.is_empty() {
        return;
    }

    let mut value = String::new();
    for name in names {
        let additional_bytes = name.as_str().len() + usize::from(!value.is_empty()) * 2;
        if value.len().saturating_add(additional_bytes) > MAX_VARY_VALUE_BYTES {
            headers.insert(VARY, HeaderValue::from_static("*"));
            return;
        }
        if !value.is_empty() {
            value.push_str(", ");
        }
        value.push_str(name.as_str());
    }
    headers.insert(
        VARY,
        HeaderValue::from_str(&value).expect("validated header-name list is a valid Vary value"),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use lily_injection::ApplicationContainer;
    use std::{
        convert::Infallible,
        future::{Ready, ready},
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    static DYNAMIC_RESOLVER_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static DYNAMIC_RESOLVER_DECISIONS: AtomicUsize = AtomicUsize::new(0);
    static DYNAMIC_RESOLVER_TEST_SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct TestDynamicOriginResolver;

    #[async_trait]
    impl CorsOriginResolver for TestDynamicOriginResolver {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, CorsOriginResolverInitError> {
            DYNAMIC_RESOLVER_INITIALIZATIONS.fetch_add(1, Ordering::SeqCst);
            Ok(Self)
        }

        async fn allows(
            &self,
            context: &CorsOriginContext<'_>,
        ) -> Result<bool, CorsOriginResolverError> {
            DYNAMIC_RESOLVER_DECISIONS.fetch_add(1, Ordering::SeqCst);
            assert_eq!(context.method(), &Method::GET);
            assert_eq!(context.host(), Some("api.example"));
            match context.path() {
                "/resolver-unavailable" => Err(CorsOriginResolverError::unavailable(
                    MiddlewareErrorCode::new("TENANT_ORIGIN_STORE_UNAVAILABLE").unwrap(),
                )),
                "/resolver-internal" => Err(CorsOriginResolverError::internal(
                    MiddlewareErrorCode::new("TENANT_ORIGIN_INTERNAL").unwrap(),
                )),
                "/resolver-panic" => panic!("test CORS resolver panic"),
                _ => Ok(context.origin() == "https://allowed.example"),
            }
        }
    }

    #[derive(Clone)]
    struct TestService {
        calls: Arc<AtomicUsize>,
        seen_methods: Arc<Mutex<Vec<Method>>>,
        response_headers: HeaderMap,
    }

    impl TestService {
        fn new(calls: Arc<AtomicUsize>) -> Self {
            Self {
                calls,
                seen_methods: Arc::new(Mutex::new(Vec::new())),
                response_headers: HeaderMap::new(),
            }
        }

        fn with_headers(mut self, headers: HeaderMap) -> Self {
            self.response_headers = headers;
            self
        }
    }

    impl Service<Request<()>> for TestService {
        type Response = Response<()>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _context: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, request: Request<()>) -> Self::Future {
            assert!(
                request
                    .extensions()
                    .get::<ClassifiedCorsRequest>()
                    .is_none(),
                "private CORS classification marker leaked to the application service"
            );
            assert!(
                request
                    .extensions()
                    .get::<DynamicCorsOriginDecision>()
                    .is_none(),
                "private CORS origin decision leaked to the application service"
            );
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.seen_methods
                .lock()
                .unwrap()
                .push(request.method().clone());
            let mut response = Response::new(());
            *response.headers_mut() = self.response_headers.clone();
            ready(Ok(response))
        }
    }

    fn production_policy() -> CorsPolicy {
        CorsPolicy::new()
            .allow_origins(["https://client.example"])
            .allow_methods(["GET", "POST"])
            .allow_headers(["content-type", "authorization"])
            .expose_headers(["x-request-id"])
            .max_age(Duration::from_secs(600))
    }

    fn service(policy: &CorsPolicy, inner: TestService) -> CorsService<TestService> {
        CorsLayerAdapter::try_new(policy).unwrap().layer(inner)
    }

    fn header_name_with_len(index: usize, len: usize) -> String {
        let suffix = format!("{index:x}");
        assert!(len > suffix.len());
        format!("x{}{suffix}", "a".repeat(len - suffix.len() - 1))
    }

    fn header_names_with_len(count: usize, len: usize) -> Vec<String> {
        (0..count)
            .map(|index| header_name_with_len(index, len))
            .collect()
    }

    #[test]
    fn restrictive_default_and_debug_are_secret_safe() {
        let policy = CorsPolicy::new().allow_origins(["https://internal-secret.example"]);
        policy.validate().unwrap();
        let debug = format!("{policy:?}");
        assert!(!debug.contains("internal-secret"));
        assert!(policy.allowed_methods.is_empty());
        assert!(policy.allowed_headers.is_empty());
        assert!(policy.exposed_headers.is_empty());
    }

    #[test]
    fn policy_validation_rejects_invalid_and_unbounded_values_before_tower() {
        for policy in [
            CorsPolicy::new().allow_origins(["https://example.com/"]),
            CorsPolicy::new().allow_origins(["HTTPS://EXAMPLE.COM"]),
            CorsPolicy::new().allow_origins(["https://example.com", "*"]),
            CorsPolicy::new().allow_origins(["null", "null"]),
            CorsPolicy::new().allow_methods(["BAD METHOD"]),
            CorsPolicy::new().allow_headers(["bad header"]),
            CorsPolicy::new().allow_origins(
                (0..=MAX_CORS_ORIGINS).map(|index| format!("https://{index}.example")),
            ),
        ] {
            assert!(policy.validate().is_err());
        }
        for policy in [
            CorsPolicy::new()
                .allow_origins(["https://client.example"])
                .vary_by((0..MAX_CORS_HEADER_NAMES).map(|index| format!("x-vary-{index}"))),
            CorsPolicy::new()
                .allow_any_origin()
                .vary_by((0..34).map(|index| format!("x-{index}-{}", "a".repeat(245)))),
        ] {
            assert_eq!(
                policy.validate().unwrap_err().diagnostic_code(),
                "CORS_VARY_HEADER_LIMIT"
            );
            assert_eq!(
                CorsLayerAdapter::try_new(&policy)
                    .unwrap_err()
                    .diagnostic_code(),
                "CORS_VARY_HEADER_LIMIT"
            );
        }
        let error = CorsPolicy::permissive_for_development()
            .allow_credentials(true)
            .validate()
            .unwrap_err();
        assert_eq!(error.diagnostic_code(), "CORS_CREDENTIALS_WILDCARD");
        assert!(
            std::panic::catch_unwind(|| {
                CorsLayerAdapter::try_new(&CorsPolicy::permissive_for_development())
            })
            .is_ok()
        );
    }

    #[test]
    fn policy_validation_observes_exact_aggregate_and_max_age_boundaries() {
        let exact_headers = header_names_with_len(MAX_CORS_HEADER_NAMES, MAX_TOKEN_BYTES);
        let exact_policy = CorsPolicy {
            allowed_headers: exact_headers.clone(),
            exposed_headers: exact_headers,
            ..CorsPolicy::new()
        };
        assert_eq!(exact_policy.retained_bytes(), MAX_CORS_POLICY_BYTES);
        assert!(exact_policy.validate().is_ok());

        let above_policy = CorsPolicy {
            allowed_origins: vec!["null".to_string()],
            ..exact_policy
        };
        assert_eq!(
            above_policy.validate().unwrap_err().diagnostic_code(),
            "CORS_POLICY_BYTES_EXCEEDED"
        );

        let exact_age = CorsPolicy {
            max_age: Some(MAX_CORS_MAX_AGE),
            ..CorsPolicy::new()
        };
        assert!(exact_age.validate().is_ok());
        for max_age in [
            MAX_CORS_MAX_AGE + Duration::from_secs(1),
            Duration::from_nanos(1),
        ] {
            let policy = CorsPolicy {
                max_age: Some(max_age),
                ..CorsPolicy::new()
            };
            assert_eq!(
                policy.validate().unwrap_err().diagnostic_code(),
                "CORS_MAX_AGE_INVALID"
            );
        }
    }

    #[test]
    fn policy_validation_checks_each_credentials_wildcard_source_directly() {
        for policy in [
            CorsPolicy {
                allowed_origins: vec!["*".to_string()],
                allow_credentials: true,
                ..CorsPolicy::new()
            },
            CorsPolicy {
                allowed_methods: vec!["*".to_string()],
                allow_credentials: true,
                ..CorsPolicy::new()
            },
            CorsPolicy {
                allowed_headers: vec!["*".to_string()],
                allow_credentials: true,
                ..CorsPolicy::new()
            },
            CorsPolicy {
                exposed_headers: vec!["*".to_string()],
                allow_credentials: true,
                ..CorsPolicy::new()
            },
        ] {
            assert_eq!(
                policy.validate().unwrap_err().diagnostic_code(),
                "CORS_CREDENTIALS_WILDCARD"
            );
        }
    }

    #[test]
    fn collection_validation_accepts_exact_bounds_and_rejects_each_violation() {
        let exact = header_names_with_len(2, MAX_TOKEN_BYTES);
        assert!(validate_collection_bounds(&exact, 2, MAX_TOKEN_BYTES, "TEST").is_ok());

        let cases = [
            vec!["x".to_string(), "y".to_string(), "z".to_string()],
            vec![String::new()],
            vec!["x".repeat(MAX_TOKEN_BYTES + 1)],
        ];
        for values in cases {
            assert_eq!(
                validate_collection_bounds(&values, 2, MAX_TOKEN_BYTES, "TEST")
                    .unwrap_err()
                    .diagnostic_code(),
                "TEST"
            );
        }
    }

    #[test]
    fn configured_origins_reject_every_forbidden_url_component() {
        for origin in [
            "ftp://example.test",
            "https://user@example.test",
            "https://:password@example.test",
            "https://example.test?query",
            "https://example.test#fragment",
            "https://example.test/path",
            "HTTPS://EXAMPLE.TEST",
        ] {
            assert_eq!(
                validate_origins(&[origin.to_string()])
                    .unwrap_err()
                    .diagnostic_code(),
                "CORS_ORIGIN_INVALID",
                "origin: {origin}"
            );
        }
    }

    #[test]
    fn compiled_vary_accepts_exact_field_and_wire_byte_limits() {
        let exact_count = header_names_with_len(MAX_CORS_HEADER_NAMES, 4);
        assert!(validate_compiled_vary(&[], false, false, &exact_count).is_ok());
        assert_eq!(
            validate_compiled_vary(
                &["https://client.example".to_string()],
                false,
                false,
                &exact_count,
            )
            .unwrap_err()
            .diagnostic_code(),
            "CORS_VARY_HEADER_LIMIT"
        );

        let mut origin_already_present = exact_count;
        origin_already_present[0] = ORIGIN.as_str().to_string();
        assert!(
            validate_compiled_vary(
                &["https://client.example".to_string()],
                false,
                false,
                &origin_already_present
            )
            .is_ok()
        );

        let mut exact_bytes = header_names_with_len(32, MAX_TOKEN_BYTES);
        exact_bytes[31] = header_name_with_len(31, 186);
        assert!(
            validate_compiled_vary(
                &["https://client.example".to_string()],
                false,
                false,
                &exact_bytes,
            )
            .is_ok()
        );

        exact_bytes[31].push('a');
        assert_eq!(
            validate_compiled_vary(
                &["https://client.example".to_string()],
                false,
                false,
                &exact_bytes,
            )
            .unwrap_err()
            .diagnostic_code(),
            "CORS_VARY_HEADER_LIMIT"
        );
    }

    #[test]
    fn fluent_builders_are_bounded_and_invalid_state_is_sticky() {
        let yielded = Arc::new(AtomicUsize::new(0));
        let yielded_by_iterator = yielded.clone();
        let unbounded = std::iter::from_fn(move || {
            let index = yielded_by_iterator.fetch_add(1, Ordering::SeqCst);
            Some(format!("https://{index}.example"))
        });
        let policy = CorsPolicy::new()
            .allow_origins(unbounded)
            .allow_origins(["https://later-valid.example"]);
        assert_eq!(yielded.load(Ordering::SeqCst), MAX_CORS_ORIGINS + 1);
        assert_eq!(
            policy.validate().unwrap_err().diagnostic_code(),
            "CORS_ORIGIN_LIMIT"
        );
        assert!(policy.allowed_origins.is_empty());

        let oversized = "a".repeat(MAX_ORIGIN_BYTES + 1);
        let policy = CorsPolicy::new()
            .allow_origins([oversized.as_str()])
            .allow_origins(["https://later-valid.example"]);
        assert_eq!(
            policy.validate().unwrap_err().diagnostic_code(),
            "CORS_ORIGIN_LIMIT"
        );
        assert!(policy.allowed_origins.is_empty());
        assert!(!format!("{policy:?}").contains(&oversized));
    }

    #[test]
    fn every_credentials_wildcard_combination_fails_before_tower_assertions() {
        for policy in [
            CorsPolicy::new().allow_any_origin().allow_credentials(true),
            CorsPolicy::new().allow_any_method().allow_credentials(true),
            CorsPolicy::new().allow_any_header().allow_credentials(true),
            CorsPolicy::new()
                .expose_any_header()
                .allow_credentials(true),
            CorsPolicy::new().allow_credentials(true).allow_any_origin(),
            CorsPolicy::new().allow_credentials(true).allow_any_method(),
            CorsPolicy::new().allow_credentials(true).allow_any_header(),
            CorsPolicy::new()
                .allow_credentials(true)
                .expose_any_header(),
        ] {
            assert_eq!(
                policy.validate().unwrap_err().diagnostic_code(),
                "CORS_CREDENTIALS_WILDCARD"
            );
            assert!(std::panic::catch_unwind(|| CorsLayerAdapter::try_new(&policy)).is_ok());
        }
    }

    #[test]
    fn trace_headers_are_never_implicit_and_null_is_explicit() {
        let policy = CorsPolicy::new()
            .allow_origins(["https://client.example"])
            .allow_null_origin()
            .allow_headers(["content-type"]);
        policy.validate().unwrap();
        assert_eq!(policy.allowed_headers, ["content-type"]);
        assert_eq!(policy.allowed_origins, ["https://client.example", "null"]);
    }

    #[test]
    fn request_classification_checks_method_syntax_and_inclusive_token_length() {
        let mut headers = HeaderMap::new();
        headers.insert(ORIGIN, HeaderValue::from_static("https://client.example"));

        let exact_method = "A".repeat(MAX_TOKEN_BYTES);
        headers.insert(
            ACCESS_CONTROL_REQUEST_METHOD,
            HeaderValue::from_str(&exact_method).unwrap(),
        );
        assert_eq!(
            classify_cors_request(&Method::OPTIONS, &headers),
            Ok(CorsRequestKind::Preflight)
        );

        let oversized_method = "A".repeat(MAX_TOKEN_BYTES + 1);
        headers.insert(
            ACCESS_CONTROL_REQUEST_METHOD,
            HeaderValue::from_str(&oversized_method).unwrap(),
        );
        assert_eq!(
            classify_cors_request(&Method::OPTIONS, &headers),
            Err(CorsRuntimeRejection::InvalidPreflight)
        );

        headers.insert(
            ACCESS_CONTROL_REQUEST_METHOD,
            HeaderValue::from_static("BAD METHOD"),
        );
        assert_eq!(
            classify_cors_request(&Method::OPTIONS, &headers),
            Err(CorsRuntimeRejection::InvalidPreflight)
        );
    }

    #[test]
    fn runtime_origin_validation_is_canonical_and_fail_closed() {
        assert!(valid_request_origin(&HeaderValue::from_static("null")));
        assert!(valid_request_origin(&HeaderValue::from_static(
            "https://client.example"
        )));

        let exact_length = format!("https://{}", "a".repeat(MAX_ORIGIN_BYTES - 8));
        assert_eq!(exact_length.len(), MAX_ORIGIN_BYTES);
        assert!(valid_request_origin(
            &HeaderValue::from_str(&exact_length).unwrap()
        ));

        for origin in [
            "ftp://example.test",
            "https://user@example.test",
            "https://:password@example.test",
            "https://example.test?query",
            "https://example.test#fragment",
            "https://example.test/path",
            "HTTPS://EXAMPLE.TEST",
        ] {
            assert!(!valid_request_origin(
                &HeaderValue::from_str(origin).unwrap()
            ));
        }

        let oversized = format!("https://{}", "a".repeat(MAX_ORIGIN_BYTES - 7));
        assert_eq!(oversized.len(), MAX_ORIGIN_BYTES + 1);
        assert!(!valid_request_origin(
            &HeaderValue::from_str(&oversized).unwrap()
        ));
    }

    #[test]
    fn requested_header_list_accepts_exact_limits_and_rejects_each_invalid_dimension() {
        let exact_count = header_names_with_len(MAX_CORS_HEADER_NAMES, 4).join(",");
        assert!(valid_requested_header_list(
            &HeaderValue::from_str(&exact_count).unwrap()
        ));

        let above_count = header_names_with_len(MAX_CORS_HEADER_NAMES + 1, 4).join(",");
        assert!(!valid_requested_header_list(
            &HeaderValue::from_str(&above_count).unwrap()
        ));

        let exact_item = header_name_with_len(0, MAX_TOKEN_BYTES);
        assert!(valid_requested_header_list(
            &HeaderValue::from_str(&exact_item).unwrap()
        ));
        let above_item = header_name_with_len(0, MAX_TOKEN_BYTES + 1);
        assert!(!valid_requested_header_list(
            &HeaderValue::from_str(&above_item).unwrap()
        ));

        for invalid in ["", "x,,y", "bad header"] {
            assert!(!valid_requested_header_list(
                &HeaderValue::from_str(invalid).unwrap()
            ));
        }

        let mut exact_bytes = header_names_with_len(32, MAX_TOKEN_BYTES);
        exact_bytes[31] = header_name_with_len(31, 225);
        let exact_bytes = exact_bytes.join(",");
        assert_eq!(exact_bytes.len(), MAX_VARY_VALUE_BYTES);
        assert!(valid_requested_header_list(
            &HeaderValue::from_str(&exact_bytes).unwrap()
        ));

        let above_bytes = format!("{exact_bytes}a");
        assert!(!valid_requested_header_list(
            &HeaderValue::from_str(&above_bytes).unwrap()
        ));
    }

    #[test]
    fn vary_canonicalization_observes_exact_count_and_wire_byte_limits() {
        let mut exact_count = HeaderMap::new();
        for name in header_names_with_len(MAX_CORS_HEADER_NAMES, 4) {
            exact_count.append(VARY, HeaderValue::from_str(&name).unwrap());
        }
        canonicalize_vary(&mut exact_count);
        assert_ne!(exact_count.get(VARY).unwrap(), "*");

        let mut above_count = HeaderMap::new();
        for name in header_names_with_len(MAX_CORS_HEADER_NAMES + 1, 4) {
            above_count.append(VARY, HeaderValue::from_str(&name).unwrap());
        }
        canonicalize_vary(&mut above_count);
        assert_eq!(above_count.get(VARY).unwrap(), "*");

        let mut exact_names = header_names_with_len(32, MAX_TOKEN_BYTES);
        exact_names[31] = header_name_with_len(31, 194);
        let mut exact_bytes = HeaderMap::new();
        for name in &exact_names {
            exact_bytes.append(VARY, HeaderValue::from_str(name).unwrap());
        }
        canonicalize_vary(&mut exact_bytes);
        let exact_value = exact_bytes.get(VARY).unwrap();
        assert_ne!(exact_value, "*");
        assert_eq!(exact_value.as_bytes().len(), MAX_VARY_VALUE_BYTES);

        exact_names[31].push('a');
        let mut above_bytes = HeaderMap::new();
        for name in &exact_names {
            above_bytes.append(VARY, HeaderValue::from_str(name).unwrap());
        }
        canonicalize_vary(&mut above_bytes);
        assert_eq!(above_bytes.get(VARY).unwrap(), "*");
    }

    #[tokio::test]
    async fn simple_request_allowed_and_denied_origin_semantics() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut service = service(&production_policy(), TestService::new(calls.clone()));
        let allowed = service
            .call(
                Request::builder()
                    .header(ORIGIN, "https://client.example")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            allowed.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
            "https://client.example"
        );
        assert_eq!(
            allowed
                .headers()
                .get(ACCESS_CONTROL_EXPOSE_HEADERS)
                .unwrap(),
            "x-request-id"
        );

        let denied = service
            .call(
                Request::builder()
                    .header(ORIGIN, "https://denied.example")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(!denied.headers().contains_key(ACCESS_CONTROL_ALLOW_ORIGIN));
        assert!(!denied.headers().contains_key(ACCESS_CONTROL_EXPOSE_HEADERS));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn request_classification_marker_is_private_single_use_state() {
        let mut request = Request::builder()
            .header(ORIGIN, "https://client.example")
            .body(())
            .unwrap();
        assert_eq!(
            CorsLayerAdapter::classify_request(&mut request),
            Ok(CorsRequestKind::Simple)
        );
        assert!(
            request
                .extensions()
                .get::<ClassifiedCorsRequest>()
                .is_some()
        );

        let calls = Arc::new(AtomicUsize::new(0));
        let mut service = service(&production_policy(), TestService::new(calls.clone()));
        let response = service.call(request).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let mut invalid = Request::builder()
            .header(ORIGIN, "https://client.example")
            .body(())
            .unwrap();
        CorsLayerAdapter::classify_request(&mut invalid).unwrap();
        invalid
            .headers_mut()
            .append(ORIGIN, HeaderValue::from_static("https://second.example"));
        assert_eq!(
            CorsLayerAdapter::classify_request(&mut invalid),
            Err(CorsRuntimeRejection::AmbiguousHeaders)
        );
        assert_eq!(
            invalid
                .extensions()
                .get::<ClassifiedCorsRequest>()
                .map(|classified| classified.0),
            Some(Err(CorsRuntimeRejection::AmbiguousHeaders))
        );
        let rejected = service.call(invalid).await.unwrap();
        assert_eq!(rejected.status(), http::StatusCode::BAD_REQUEST);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn preflight_is_tower_owned_and_browser_denial_is_fail_closed() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut service = service(&production_policy(), TestService::new(calls.clone()));
        let allowed = Request::builder()
            .method(Method::OPTIONS)
            .header(ORIGIN, "https://client.example")
            .header(ACCESS_CONTROL_REQUEST_METHOD, "POST")
            .header(ACCESS_CONTROL_REQUEST_HEADERS, "content-type")
            .body(())
            .unwrap();
        let allowed = service.call(allowed).await.unwrap();
        assert_eq!(allowed.status(), 200);
        assert_eq!(
            allowed.headers().get(ACCESS_CONTROL_ALLOW_METHODS).unwrap(),
            "GET,POST"
        );
        assert_eq!(
            allowed.headers().get(ACCESS_CONTROL_ALLOW_HEADERS).unwrap(),
            "content-type,authorization"
        );
        assert_eq!(
            allowed.headers().get(ACCESS_CONTROL_MAX_AGE).unwrap(),
            "600"
        );

        let denied = Request::builder()
            .method(Method::OPTIONS)
            .header(ORIGIN, "https://client.example")
            .header(ACCESS_CONTROL_REQUEST_METHOD, "DELETE")
            .header(ACCESS_CONTROL_REQUEST_HEADERS, "x-not-allowed")
            .body(())
            .unwrap();
        let denied = service.call(denied).await.unwrap();
        assert_eq!(
            denied.headers().get(ACCESS_CONTROL_ALLOW_METHODS).unwrap(),
            "GET,POST"
        );
        assert_eq!(
            denied.headers().get(ACCESS_CONTROL_ALLOW_HEADERS).unwrap(),
            "content-type,authorization"
        );

        let denied_origin = Request::builder()
            .method(Method::OPTIONS)
            .header(ORIGIN, "https://denied.example")
            .header(ACCESS_CONTROL_REQUEST_METHOD, "POST")
            .header(ACCESS_CONTROL_REQUEST_HEADERS, "content-type")
            .body(())
            .unwrap();
        let denied_origin = service.call(denied_origin).await.unwrap();
        assert!(
            !denied_origin
                .headers()
                .contains_key(ACCESS_CONTROL_ALLOW_ORIGIN)
        );
        assert!(
            !denied_origin
                .headers()
                .contains_key(ACCESS_CONTROL_ALLOW_CREDENTIALS)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn ambiguous_and_malformed_preflight_is_bounded_rejection() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut service = service(&production_policy(), TestService::new(calls.clone()));
        let mut duplicate_origin = Request::builder()
            .method(Method::OPTIONS)
            .header(ORIGIN, "https://client.example")
            .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
            .body(())
            .unwrap();
        duplicate_origin
            .headers_mut()
            .append(ORIGIN, HeaderValue::from_static("https://second.example"));
        let response = service.call(duplicate_origin).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
        assert_eq!(
            response.extensions().get::<CorsRuntimeRejection>().copied(),
            Some(CorsRuntimeRejection::AmbiguousHeaders)
        );

        for duplicate_name in [
            ACCESS_CONTROL_REQUEST_METHOD,
            ACCESS_CONTROL_REQUEST_HEADERS,
        ] {
            let mut request = Request::builder()
                .method(Method::OPTIONS)
                .header(ORIGIN, "https://client.example")
                .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
                .header(ACCESS_CONTROL_REQUEST_HEADERS, "content-type")
                .body(())
                .unwrap();
            request
                .headers_mut()
                .append(duplicate_name, HeaderValue::from_static("authorization"));
            let response = service.call(request).await.unwrap();
            assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
            assert_eq!(
                response.extensions().get::<CorsRuntimeRejection>().copied(),
                Some(CorsRuntimeRejection::AmbiguousHeaders)
            );
        }

        for request in [
            Request::builder()
                .method(Method::OPTIONS)
                .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
                .body(())
                .unwrap(),
            Request::builder()
                .method(Method::OPTIONS)
                .header(ORIGIN, "https://client.example")
                .body(())
                .unwrap(),
            Request::builder()
                .method(Method::OPTIONS)
                .header(ORIGIN, "https://client.example")
                .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
                .header(
                    ACCESS_CONTROL_REQUEST_HEADERS,
                    "content-type,,authorization",
                )
                .body(())
                .unwrap(),
        ] {
            assert_eq!(
                service.call(request).await.unwrap().status(),
                http::StatusCode::BAD_REQUEST
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn wildcard_policy_does_not_decorate_structurally_invalid_request() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut service = service(
            &CorsPolicy::permissive_for_development(),
            TestService::new(calls.clone()),
        );
        let mut request = Request::builder()
            .header(ORIGIN, "https://client.example")
            .body(())
            .unwrap();
        request
            .headers_mut()
            .append(ORIGIN, HeaderValue::from_static("https://second.example"));
        let response = service.call(request).await.unwrap();
        assert_eq!(response.status(), http::StatusCode::BAD_REQUEST);
        assert!(!response.headers().contains_key(ACCESS_CONTROL_ALLOW_ORIGIN));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn null_origin_is_denied_by_default_and_covered_by_explicit_wildcard() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut restrictive = service(&CorsPolicy::new(), TestService::new(calls.clone()));
        let denied = restrictive
            .call(Request::builder().header(ORIGIN, "null").body(()).unwrap())
            .await
            .unwrap();
        assert!(!denied.headers().contains_key(ACCESS_CONTROL_ALLOW_ORIGIN));

        let mut wildcard = service(
            &CorsPolicy::new().allow_any_origin().allow_null_origin(),
            TestService::new(calls.clone()),
        );
        let allowed = wildcard
            .call(Request::builder().header(ORIGIN, "null").body(()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            allowed.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
            "*"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn null_origin_credentials_and_ordinary_options_are_explicit() {
        let policy = CorsPolicy::new()
            .allow_null_origin()
            .allow_methods(["GET"])
            .allow_credentials(true);
        let calls = Arc::new(AtomicUsize::new(0));
        let inner = TestService::new(calls.clone());
        let seen_methods = inner.seen_methods.clone();
        let mut service = service(&policy, inner);
        let response = service
            .call(Request::builder().header(ORIGIN, "null").body(()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
            "null"
        );
        assert_eq!(
            response
                .headers()
                .get(ACCESS_CONTROL_ALLOW_CREDENTIALS)
                .unwrap(),
            "true"
        );
        service
            .call(Request::builder().method(Method::OPTIONS).body(()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            *seen_methods.lock().unwrap(),
            [Method::GET, Method::OPTIONS]
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn stale_headers_are_replaced_and_vary_is_canonical() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut headers = HeaderMap::new();
        headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
        headers.insert(
            ACCESS_CONTROL_ALLOW_PRIVATE_NETWORK,
            HeaderValue::from_static("true"),
        );
        headers.append(VARY, HeaderValue::from_static("Accept-Encoding, ORIGIN"));
        headers.append(VARY, HeaderValue::from_static("origin, User-Agent"));
        let mut service = service(
            &production_policy(),
            TestService::new(calls).with_headers(headers),
        );
        let response = service
            .call(
                Request::builder()
                    .header(ORIGIN, "https://client.example")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
            "https://client.example"
        );
        assert!(
            !response
                .headers()
                .contains_key(ACCESS_CONTROL_ALLOW_PRIVATE_NETWORK)
        );
        let vary = response.headers().get_all(VARY).iter().collect::<Vec<_>>();
        assert_eq!(vary.len(), 1);
        assert_eq!(vary[0], "accept-encoding, origin, user-agent");
    }

    #[tokio::test]
    async fn vary_wildcard_is_authoritative_and_additional_vary_is_deduplicated() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut wildcard_headers = HeaderMap::new();
        wildcard_headers.append(VARY, HeaderValue::from_static("Accept-Encoding"));
        wildcard_headers.append(VARY, HeaderValue::from_static("*"));
        let mut wildcard_service = service(
            &production_policy(),
            TestService::new(calls.clone()).with_headers(wildcard_headers),
        );
        let wildcard_response = wildcard_service
            .call(
                Request::builder()
                    .header(ORIGIN, "https://client.example")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wildcard_response.headers().get(VARY).unwrap(), "*");

        let policy = production_policy().vary_by(["accept-encoding", "user-agent"]);
        let mut exact_service = service(&policy, TestService::new(calls));
        let response = exact_service
            .call(
                Request::builder()
                    .header(ORIGIN, "https://client.example")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.headers().get(VARY).unwrap(),
            "accept-encoding, user-agent, origin"
        );

        let any_policy = CorsPolicy::new()
            .allow_any_origin()
            .allow_any_method()
            .allow_any_header()
            .vary_by(["accept-encoding"]);
        let mut any_service = service(&any_policy, TestService::new(Arc::new(AtomicUsize::new(0))));
        let response = any_service
            .call(
                Request::builder()
                    .method(Method::OPTIONS)
                    .header(ORIGIN, "https://client.example")
                    .header(ACCESS_CONTROL_REQUEST_METHOD, "POST")
                    .header(ACCESS_CONTROL_REQUEST_HEADERS, "x-custom")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.headers().get(VARY).unwrap(), "accept-encoding");
    }

    #[test]
    fn dynamic_origin_is_one_exclusive_secret_safe_authority() {
        for policy in [
            CorsPolicy::new()
                .allow_origins(["https://client.example"])
                .resolve_origins_with::<TestDynamicOriginResolver>(),
            CorsPolicy::new()
                .resolve_origins_with::<TestDynamicOriginResolver>()
                .allow_origins(["https://client.example"]),
            CorsPolicy::new()
                .resolve_origins_with::<TestDynamicOriginResolver>()
                .resolve_origins_with::<TestDynamicOriginResolver>(),
        ] {
            assert_eq!(
                policy.validate().unwrap_err().diagnostic_code(),
                "CORS_ORIGIN_AUTHORITY_CONFLICT"
            );
        }

        let policy = CorsPolicy::new().resolve_origins_with::<TestDynamicOriginResolver>();
        policy.validate().unwrap();
        let debug = format!("{policy:?}");
        assert!(debug.contains("dynamic_origin: true"));
        assert!(!debug.contains("TestDynamicOriginResolver"));

        let scoped = CorsOriginResolverInitError::from(InjectionError::ScopeRequired {
            service: "tenant-secret-service".to_owned(),
        });
        assert_eq!(
            scoped.diagnostic_code(),
            "CORS_ORIGIN_RESOLVER_SCOPE_REQUIRED"
        );
        assert!(!format!("{scoped:?} {scoped}").contains("tenant-secret"));
    }

    #[tokio::test]
    async fn dynamic_origin_is_initialized_once_resolved_once_and_removed_before_inner_service() {
        let _serial = DYNAMIC_RESOLVER_TEST_SERIAL.lock().await;
        let initial_initializations = DYNAMIC_RESOLVER_INITIALIZATIONS.load(Ordering::SeqCst);
        let initial_decisions = DYNAMIC_RESOLVER_DECISIONS.load(Ordering::SeqCst);
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let policy = CorsPolicy::new()
            .resolve_origins_with::<TestDynamicOriginResolver>()
            .allow_methods(["GET"])
            .expose_headers(["x-request-id"])
            .allow_credentials(true);
        let adapter = CorsLayerAdapter::try_new(&policy)
            .unwrap()
            .initialize_origin_resolver(container.services())
            .await
            .unwrap();
        assert_eq!(
            DYNAMIC_RESOLVER_INITIALIZATIONS.load(Ordering::SeqCst),
            initial_initializations + 1
        );

        let mut allowed_request = Request::builder()
            .uri("/allowed")
            .header(ORIGIN, "https://allowed.example")
            .header(http::header::HOST, "api.example")
            .body(())
            .unwrap();
        assert_eq!(
            CorsLayerAdapter::classify_request(&mut allowed_request),
            Ok(CorsRequestKind::Simple)
        );
        assert_eq!(
            adapter.resolve_origin(&mut allowed_request).await,
            Ok(CorsRequestKind::Simple)
        );
        assert_eq!(
            adapter.resolve_origin(&mut allowed_request).await,
            Ok(CorsRequestKind::Simple)
        );
        assert_eq!(
            DYNAMIC_RESOLVER_DECISIONS.load(Ordering::SeqCst),
            initial_decisions + 1
        );

        let calls = Arc::new(AtomicUsize::new(0));
        let mut cors_service = adapter.layer(TestService::new(calls.clone()));
        let allowed_response = cors_service.call(allowed_request).await.unwrap();
        assert_eq!(allowed_response.status(), http::StatusCode::OK);
        assert_eq!(
            allowed_response.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN),
            Some(&HeaderValue::from_static("https://allowed.example"))
        );
        assert_eq!(
            allowed_response
                .headers()
                .get(ACCESS_CONTROL_ALLOW_CREDENTIALS),
            Some(&HeaderValue::from_static("true"))
        );
        assert_eq!(
            allowed_response.headers().get(VARY).unwrap(),
            "origin, access-control-request-method, access-control-request-headers"
        );

        let mut denied_request = Request::builder()
            .uri("/denied")
            .header(ORIGIN, "https://denied.example")
            .header(http::header::HOST, "api.example")
            .body(())
            .unwrap();
        CorsLayerAdapter::classify_request(&mut denied_request).unwrap();
        adapter.resolve_origin(&mut denied_request).await.unwrap();
        let denied_response = cors_service.call(denied_request).await.unwrap();
        assert!(
            !denied_response
                .headers()
                .contains_key(ACCESS_CONTROL_ALLOW_ORIGIN)
        );
        assert!(
            !denied_response
                .headers()
                .contains_key(ACCESS_CONTROL_ALLOW_CREDENTIALS)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn dynamic_resolver_error_and_panic_are_bounded_fail_closed_rejections() {
        let _serial = DYNAMIC_RESOLVER_TEST_SERIAL.lock().await;
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let policy = CorsPolicy::new()
            .resolve_origins_with::<TestDynamicOriginResolver>()
            .allow_methods(["GET"]);
        let adapter = CorsLayerAdapter::try_new(&policy)
            .unwrap()
            .initialize_origin_resolver(container.services())
            .await
            .unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut cors_service = adapter.layer(TestService::new(calls.clone()));

        for (path, status, code) in [
            (
                "/resolver-unavailable",
                http::StatusCode::SERVICE_UNAVAILABLE,
                "TENANT_ORIGIN_STORE_UNAVAILABLE",
            ),
            (
                "/resolver-internal",
                http::StatusCode::INTERNAL_SERVER_ERROR,
                "TENANT_ORIGIN_INTERNAL",
            ),
            (
                "/resolver-panic",
                http::StatusCode::INTERNAL_SERVER_ERROR,
                "CORS_ORIGIN_RESOLVER_PANICKED",
            ),
        ] {
            let mut request = Request::builder()
                .uri(path)
                .header(ORIGIN, "https://allowed.example")
                .header(http::header::HOST, "api.example")
                .body(())
                .unwrap();
            CorsLayerAdapter::classify_request(&mut request).unwrap();
            let rejection = adapter.resolve_origin(&mut request).await.unwrap_err();
            assert_eq!(rejection.diagnostic_code(), code);
            let response = cors_service.call(request).await.unwrap();
            assert_eq!(response.status(), status);
            assert_eq!(
                response
                    .extensions()
                    .get::<CorsRuntimeRejection>()
                    .map(|rejection| rejection.diagnostic_code()),
                Some(code)
            );
            assert!(!response.headers().contains_key(ACCESS_CONTROL_ALLOW_ORIGIN));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn private_network_access_is_explicit_preflight_only_and_fail_closed() {
        let policy = production_policy().allow_private_network(true);
        let mut cors_service = service(&policy, TestService::new(Arc::new(AtomicUsize::new(0))));

        let allowed = cors_service
            .call(
                Request::builder()
                    .method(Method::OPTIONS)
                    .header(ORIGIN, "https://client.example")
                    .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .header(&ACCESS_CONTROL_REQUEST_PRIVATE_NETWORK, "true")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(allowed.status(), http::StatusCode::OK);
        assert_eq!(
            allowed.headers().get(&ACCESS_CONTROL_ALLOW_PRIVATE_NETWORK),
            Some(&HeaderValue::from_static("true"))
        );
        assert_eq!(
            allowed.headers().get(VARY).unwrap(),
            "origin, access-control-request-method, access-control-request-headers, access-control-request-private-network"
        );

        let denied = cors_service
            .call(
                Request::builder()
                    .method(Method::OPTIONS)
                    .header(ORIGIN, "https://denied.example")
                    .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .header(&ACCESS_CONTROL_REQUEST_PRIVATE_NETWORK, "true")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(!denied.headers().contains_key(ACCESS_CONTROL_ALLOW_ORIGIN));
        assert!(
            !denied
                .headers()
                .contains_key(&ACCESS_CONTROL_ALLOW_PRIVATE_NETWORK)
        );

        let mut stale_headers = HeaderMap::new();
        stale_headers.insert(
            ACCESS_CONTROL_ALLOW_PRIVATE_NETWORK,
            HeaderValue::from_static("true"),
        );
        let mut stale_service = service(
            &policy,
            TestService::new(Arc::new(AtomicUsize::new(0))).with_headers(stale_headers),
        );
        let sanitized_normal_response = stale_service
            .call(
                Request::builder()
                    .header(ORIGIN, "https://client.example")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            !sanitized_normal_response
                .headers()
                .contains_key(&ACCESS_CONTROL_ALLOW_PRIVATE_NETWORK)
        );

        let invalid_value = cors_service
            .call(
                Request::builder()
                    .method(Method::OPTIONS)
                    .header(ORIGIN, "https://client.example")
                    .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .header(&ACCESS_CONTROL_REQUEST_PRIVATE_NETWORK, "false")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid_value.status(), http::StatusCode::BAD_REQUEST);
        assert_eq!(
            invalid_value
                .extensions()
                .get::<CorsRuntimeRejection>()
                .copied(),
            Some(CorsRuntimeRejection::InvalidPrivateNetwork)
        );
        assert!(
            !invalid_value
                .headers()
                .contains_key(&ACCESS_CONTROL_ALLOW_PRIVATE_NETWORK)
        );

        let normal_request = cors_service
            .call(
                Request::builder()
                    .header(ORIGIN, "https://client.example")
                    .header(&ACCESS_CONTROL_REQUEST_PRIVATE_NETWORK, "true")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(normal_request.status(), http::StatusCode::BAD_REQUEST);
        assert!(
            !normal_request
                .headers()
                .contains_key(&ACCESS_CONTROL_ALLOW_PRIVATE_NETWORK)
        );

        let mut duplicate = Request::builder()
            .method(Method::OPTIONS)
            .header(ORIGIN, "https://client.example")
            .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
            .header(&ACCESS_CONTROL_REQUEST_PRIVATE_NETWORK, "true")
            .body(())
            .unwrap();
        duplicate.headers_mut().append(
            ACCESS_CONTROL_REQUEST_PRIVATE_NETWORK.clone(),
            HeaderValue::from_static("true"),
        );
        let duplicate_response = cors_service.call(duplicate).await.unwrap();
        assert_eq!(duplicate_response.status(), http::StatusCode::BAD_REQUEST);
        assert_eq!(
            duplicate_response
                .extensions()
                .get::<CorsRuntimeRejection>()
                .copied(),
            Some(CorsRuntimeRejection::AmbiguousHeaders)
        );

        let disabled_policy = production_policy().allow_private_network(false);
        let mut disabled_service = service(
            &disabled_policy,
            TestService::new(Arc::new(AtomicUsize::new(0))),
        );
        let disabled = disabled_service
            .call(
                Request::builder()
                    .method(Method::OPTIONS)
                    .header(ORIGIN, "https://client.example")
                    .header(ACCESS_CONTROL_REQUEST_METHOD, "GET")
                    .header(&ACCESS_CONTROL_REQUEST_PRIVATE_NETWORK, "true")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            !disabled
                .headers()
                .contains_key(&ACCESS_CONTROL_ALLOW_PRIVATE_NETWORK)
        );
    }

    #[test]
    fn response_only_seam_is_lily_owned() {
        let policy = production_policy().allow_credentials(true);
        let adapter = CorsLayerAdapter::try_new(&policy).unwrap();
        let request = Request::builder()
            .header(ORIGIN, "https://client.example")
            .body(())
            .unwrap();
        let mut response = Response::builder()
            .status(http::StatusCode::SERVICE_UNAVAILABLE)
            .header(ACCESS_CONTROL_ALLOW_ORIGIN, "https://stale.example")
            .body(())
            .unwrap();
        adapter.decorate_response(&request, &mut response);
        assert_eq!(response.status(), http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            response.headers().get(ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
            "https://client.example"
        );
        assert_eq!(
            response
                .headers()
                .get(ACCESS_CONTROL_ALLOW_CREDENTIALS)
                .unwrap(),
            "true"
        );
    }
}
