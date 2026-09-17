use crate::{
    controller::ControllerMaterializationError,
    guard::GuardInitError,
    openapi::{OpenApiSecurityValidationError, OpenApiServiceError},
    registry::{OpenApiRouteBuildError, OpenApiRouteRegistryError},
    route_table::{
        AmbiguousRouteError, DuplicateRouteError, InvalidRouteError, RouteTableBuildError,
    },
};
use lily_error::injection::InjectionError;
use lily_middleware::{
    CorsOriginResolverInitError, HttpMiddlewareInitError, MiddlewareConfigError,
};
use lily_web_core::TlsConfigError;
use std::any::TypeId;

/// Failures while selecting or loading the HTTP server's TLS configuration.
///
/// Paths and private-key material are deliberately absent so the error is safe
/// to retain in startup logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpTlsConfigError {
    /// TLS was enabled in `lily_config`, but no certificate chain was supplied.
    CertificatePathRequired,
    /// TLS was enabled in `lily_config`, but no private key was supplied.
    PrivateKeyPathRequired,
    /// Certificate material was configured while TLS was not explicitly enabled.
    CertificateMaterialWhileDisabled,
    /// The shared bounded PEM loader or Rustls validation rejected the identity.
    Loader(TlsConfigError),
}

impl std::fmt::Display for HttpTlsConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CertificatePathRequired => {
                formatter.write_str("server.tls_cert_path is required when TLS is enabled")
            }
            Self::PrivateKeyPathRequired => {
                formatter.write_str("server.tls_key_path is required when TLS is enabled")
            }
            Self::CertificateMaterialWhileDisabled => formatter
                .write_str("server TLS certificate material requires server.tls_enabled=true"),
            Self::Loader(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for HttpTlsConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Loader(error) => Some(error),
            Self::CertificatePathRequired
            | Self::PrivateKeyPathRequired
            | Self::CertificateMaterialWhileDisabled => None,
        }
    }
}

impl From<TlsConfigError> for HttpTlsConfigError {
    fn from(error: TlsConfigError) -> Self {
        Self::Loader(error)
    }
}

/// Invalid effective HTTP listener or transport configuration.
///
/// These errors are produced while [`super::AppBuilder`] freezes the final
/// server snapshot, before the listener is opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpServerConfigError {
    /// `server.host` is empty or contains only whitespace.
    EmptyHost,
    /// `server.host` is not a valid IP address or DNS host name.
    InvalidHost,
    /// `server.port = 0` is not valid for a config-backed production listener.
    ZeroPort,
    /// An explicit builder address is not a valid `host:port` listener address.
    InvalidListenAddress,
    /// An explicit builder address has no port.
    MissingListenPort,
    /// A bounded transport invariant was violated.
    InvalidTransport(String),
}

impl std::fmt::Display for HttpServerConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyHost => formatter.write_str("server.host must not be empty"),
            Self::InvalidHost => formatter.write_str("server.host is not a valid listener host"),
            Self::ZeroPort => formatter.write_str("server.port must be between 1 and 65535"),
            Self::InvalidListenAddress => {
                formatter.write_str("explicit HTTP listener address must be a valid host:port")
            }
            Self::MissingListenPort => {
                formatter.write_str("explicit HTTP listener address must include a port")
            }
            Self::InvalidTransport(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for HttpServerConfigError {}

/// A route declares a guard for which the application has no live instance.
///
/// This is validated while [`super::AppBuilder`] is building the immutable
/// route state, before a listener can accept traffic. The same error is also
/// used by the request path as a fail-closed invariant check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingRouteGuardError {
    /// Normalized HTTP method of the affected route.
    pub method: String,
    /// Registered route template that requires the guard.
    pub path: String,
    /// Controller action or handler name attached to the route.
    pub handler: String,
    /// Concrete guard type that could not be resolved during graph validation.
    pub guard_type_id: TypeId,
}

/// Build-time failure while freezing one route's middleware plan.
#[derive(Debug, Clone, PartialEq)]
pub struct RouteMiddlewareBuildError {
    /// Normalized HTTP method of the affected route.
    pub method: String,
    /// Registered route template whose middleware plan could not be frozen.
    pub path: String,
    /// Controller action or handler name attached to the route.
    pub handler: String,
    /// Rust middleware type name, or the bounded `<route-chain>` aggregate label.
    pub middleware: &'static str,
    /// Stable category and typed source of the plan failure.
    pub cause: RouteMiddlewareBuildErrorCause,
}

/// Why one route-specific middleware plan could not be frozen.
#[derive(Debug, Clone, PartialEq)]
pub enum RouteMiddlewareBuildErrorCause {
    /// The same concrete middleware type was declared more than once on a route.
    DuplicateType,
    /// Constructing the middleware singleton from the application DI graph failed.
    Initialization(HttpMiddlewareInitError),
    /// Middleware validation or whole-chain descriptor validation failed.
    InvalidConfiguration(MiddlewareConfigError),
}

impl std::fmt::Display for RouteMiddlewareBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "route {} {} (handler '{}') middleware '{}' failed: ",
            self.method, self.path, self.handler, self.middleware
        )?;
        match &self.cause {
            RouteMiddlewareBuildErrorCause::DuplicateType => {
                formatter.write_str("the same middleware type is declared more than once")
            }
            RouteMiddlewareBuildErrorCause::Initialization(error) => error.fmt(formatter),
            RouteMiddlewareBuildErrorCause::InvalidConfiguration(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for RouteMiddlewareBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.cause {
            RouteMiddlewareBuildErrorCause::DuplicateType => None,
            RouteMiddlewareBuildErrorCause::Initialization(error) => Some(error),
            RouteMiddlewareBuildErrorCause::InvalidConfiguration(error) => Some(error),
        }
    }
}

impl std::fmt::Display for MissingRouteGuardError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "route {} {} (handler '{}') requires unavailable guard {:?}",
            self.method, self.path, self.handler, self.guard_type_id
        )
    }
}

impl std::error::Error for MissingRouteGuardError {}

/// Typed failures produced before an HTTP application is published.
#[derive(Debug, Clone, PartialEq)]
pub enum AppBuildError {
    /// Constructing one registered background service failed.
    BackgroundService(lily_background_service::BackgroundServiceError),
    /// Building or resolving the application DI composition root failed.
    DependencyInjection(InjectionError),
    /// The effective listener address or bounded transport configuration is invalid.
    InvalidServerConfiguration(HttpServerConfigError),
    /// Selecting, loading, or validating the listener TLS identity failed.
    TlsConfiguration(HttpTlsConfigError),
    /// An application-wide middleware or the effective middleware chain is invalid.
    InvalidMiddlewareConfiguration {
        /// Registration index when one middleware is responsible, or `None` for a
        /// whole-chain invariant.
        index: Option<usize>,
        /// Typed bounded configuration failure.
        error: MiddlewareConfigError,
    },
    /// Constructing one application-wide middleware singleton failed.
    MiddlewareInitialization {
        /// Zero-based registration index in the effective application chain.
        index: usize,
        /// Secret-safe initialization failure returned by the middleware.
        error: HttpMiddlewareInitError,
    },
    /// More than one application session-preparation middleware was registered.
    DuplicateSessionMiddleware,
    /// More than one application-wide CORS policy was registered.
    DuplicateCorsPolicy,
    /// The selected CORS policy violates a bounded policy invariant.
    InvalidCorsConfiguration(MiddlewareConfigError),
    /// Constructing a DI-aware dynamic CORS origin resolver failed.
    CorsOriginResolverInitialization(CorsOriginResolverInitError),
    /// More than one CSRF policy was registered in the same mode.
    DuplicateCsrfPolicy,
    /// Global and route-scoped CSRF enforcement were configured together.
    ConflictingCsrfModes,
    /// A route declared [`crate::CsrfGuard`] without route-scoped CSRF mode.
    CsrfGuardRequiresRouteScopedPolicy,
    /// Route-scoped CSRF mode was enabled but no route declared [`crate::CsrfGuard`].
    CsrfRouteScopedPolicyRequiresGuard,
    /// One route declared [`crate::CsrfGuard`] more than once.
    DuplicateCsrfGuardOnRoute,
    /// A ready [`crate::CsrfGuard`] instance was supplied instead of allowing
    /// Lily to bind it to this application's [`crate::CsrfService`].
    ExplicitCsrfGuardUnsupported,
    /// The selected CSRF policy violates a bounded policy invariant.
    InvalidCsrfConfiguration(MiddlewareConfigError),
    /// The application CSRF runtime could not be attached to its injectable service.
    CsrfInitialization,
    /// The explicit localization catalog source is unavailable or malformed.
    InvalidLocalizationConfiguration(String),
    /// An owned tracing configuration source is unavailable or malformed.
    InvalidTracingConfiguration(String),
    /// A secret resolver and caller-owned container were supplied together.
    ConflictingContainerBootstrap,
    /// Struct-controller metadata could not be materialized for this application.
    ControllerMaterialization(ControllerMaterializationError),
    /// OpenAPI route metadata is incomplete, conflicting, or otherwise invalid.
    OpenApiRouteRegistry(Box<OpenApiRouteRegistryError>),
    /// OpenAPI security declarations do not match the configured scheme registry.
    OpenApiSecurity(Box<OpenApiSecurityValidationError>),
    /// The immutable OpenAPI service snapshot could not be attached.
    OpenApiInitialization(OpenApiServiceError),
    /// Two controller actions registered the same normalized method and route.
    DuplicateRoute(DuplicateRouteError),
    /// A controller action registered an invalid route template or method.
    InvalidRoute(InvalidRouteError),
    /// Two parameterized routes have equal precedence for at least one request path.
    AmbiguousRoute(AmbiguousRouteError),
    /// A route requires a guard singleton that is absent from the frozen graph.
    MissingRouteGuard(MissingRouteGuardError),
    /// A route/controller middleware plan could not be initialized or validated.
    RouteMiddleware(RouteMiddlewareBuildError),
    /// Constructing one required guard singleton from application DI failed.
    GuardInitialization(GuardInitError),
    /// Installing the application-owned tracing runtime failed.
    TracingInitialization(String),
    /// Attaching the injectable HTTP lifecycle health snapshot failed.
    MonitoringInitialization(String),
    /// Startup failed and one or more owned-resource rollback steps also failed.
    StartupRollback {
        /// Failure that caused startup rollback to begin.
        original: Box<AppBuildError>,
        /// Bounded descriptions of cleanup operations that did not complete.
        cleanup_failures: Vec<String>,
    },
}

impl std::fmt::Display for AppBuildError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BackgroundService(error) => error.fmt(formatter),
            Self::DependencyInjection(error) => write!(formatter, "DI startup failed: {error}"),
            Self::InvalidServerConfiguration(error) => {
                write!(formatter, "HTTP server configuration is invalid: {error}")
            }
            Self::TlsConfiguration(error) => {
                write!(formatter, "HTTP TLS configuration is invalid: {error}")
            }
            Self::InvalidMiddlewareConfiguration { index, error } => {
                if let Some(index) = index {
                    write!(formatter, "HTTP middleware {index} is invalid: {error}")
                } else {
                    write!(formatter, "HTTP middleware chain is invalid: {error}")
                }
            }
            Self::MiddlewareInitialization { index, error } => {
                write!(formatter, "HTTP middleware {index} initialization failed: {error}")
            }
            Self::DuplicateSessionMiddleware => {
                formatter.write_str("HTTP session middleware was registered more than once")
            }
            Self::DuplicateCorsPolicy => {
                formatter.write_str("HTTP CORS policy was registered more than once")
            }
            Self::InvalidCorsConfiguration(error) => {
                write!(formatter, "HTTP CORS policy is invalid: {error}")
            }
            Self::CorsOriginResolverInitialization(error) => {
                write!(formatter, "HTTP CORS origin resolver initialization failed: {error}")
            }
            Self::DuplicateCsrfPolicy => {
                formatter.write_str("HTTP CSRF policy was registered more than once")
            }
            Self::ConflictingCsrfModes => formatter.write_str(
                "global and route-scoped HTTP CSRF modes cannot be combined",
            ),
            Self::CsrfGuardRequiresRouteScopedPolicy => formatter.write_str(
                "CsrfGuard requires AppBuilder::csrf_route_scoped and cannot be used with global or missing CSRF policy",
            ),
            Self::CsrfRouteScopedPolicyRequiresGuard => formatter.write_str(
                "route-scoped HTTP CSRF policy requires at least one route with CsrfGuard",
            ),
            Self::DuplicateCsrfGuardOnRoute => formatter.write_str(
                "one route cannot declare CsrfGuard more than once",
            ),
            Self::ExplicitCsrfGuardUnsupported => formatter.write_str(
                "CsrfGuard cannot be supplied as a ready instance; declare it on a route so it resolves the attached application CSRF service during build",
            ),
            Self::InvalidCsrfConfiguration(error) => {
                write!(formatter, "HTTP CSRF policy is invalid: {error}")
            }
            Self::CsrfInitialization => formatter.write_str("HTTP CSRF initialization failed"),
            Self::InvalidLocalizationConfiguration(error) => {
                write!(formatter, "localization configuration is invalid: {error}")
            }
            Self::InvalidTracingConfiguration(error) => {
                write!(formatter, "tracing configuration is invalid: {error}")
            }
            Self::ConflictingContainerBootstrap => formatter.write_str(
                "secret_resolver cannot be combined with a caller-owned DI container; seed ConfigService while building that container",
            ),
            Self::ControllerMaterialization(error) => error.fmt(formatter),
            Self::OpenApiRouteRegistry(error) => error.fmt(formatter),
            Self::OpenApiSecurity(error) => error.fmt(formatter),
            Self::OpenApiInitialization(error) => error.fmt(formatter),
            Self::DuplicateRoute(error) => error.fmt(formatter),
            Self::InvalidRoute(error) => error.fmt(formatter),
            Self::AmbiguousRoute(error) => error.fmt(formatter),
            Self::MissingRouteGuard(error) => error.fmt(formatter),
            Self::RouteMiddleware(error) => error.fmt(formatter),
            Self::GuardInitialization(error) => error.fmt(formatter),
            Self::TracingInitialization(error) => {
                write!(formatter, "tracing initialization failed: {error}")
            }
            Self::MonitoringInitialization(error) => {
                write!(formatter, "health initialization failed: {error}")
            }
            Self::StartupRollback {
                original,
                cleanup_failures,
            } => write!(
                formatter,
                "{original}; startup rollback also failed: {}",
                cleanup_failures.join("; ")
            ),
        }
    }
}

impl std::error::Error for AppBuildError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::BackgroundService(error) => Some(error),
            Self::DependencyInjection(error) => Some(error),
            Self::ControllerMaterialization(error) => Some(error),
            Self::OpenApiRouteRegistry(error) => Some(error.as_ref()),
            Self::OpenApiSecurity(error) => Some(error.as_ref()),
            Self::OpenApiInitialization(error) => Some(error),
            Self::DuplicateRoute(error) => Some(error),
            Self::InvalidRoute(error) => Some(error),
            Self::AmbiguousRoute(error) => Some(error),
            Self::MissingRouteGuard(error) => Some(error),
            Self::RouteMiddleware(error) => Some(error),
            Self::GuardInitialization(error) => Some(error),
            Self::TlsConfiguration(error) => Some(error),
            Self::InvalidMiddlewareConfiguration { error, .. } => Some(error),
            Self::MiddlewareInitialization { error, .. } => Some(error),
            Self::InvalidCorsConfiguration(error) => Some(error),
            Self::CorsOriginResolverInitialization(error) => Some(error),
            Self::InvalidCsrfConfiguration(error) => Some(error),
            Self::StartupRollback { original, .. } => Some(original.as_ref()),
            Self::InvalidServerConfiguration(error) => Some(error),
            Self::DuplicateSessionMiddleware
            | Self::DuplicateCorsPolicy
            | Self::DuplicateCsrfPolicy
            | Self::ConflictingCsrfModes
            | Self::CsrfGuardRequiresRouteScopedPolicy
            | Self::CsrfRouteScopedPolicyRequiresGuard
            | Self::DuplicateCsrfGuardOnRoute
            | Self::ExplicitCsrfGuardUnsupported
            | Self::CsrfInitialization
            | Self::InvalidLocalizationConfiguration(_)
            | Self::InvalidTracingConfiguration(_)
            | Self::ConflictingContainerBootstrap
            | Self::TracingInitialization(_)
            | Self::MonitoringInitialization(_) => None,
        }
    }
}

impl From<HttpServerConfigError> for AppBuildError {
    fn from(error: HttpServerConfigError) -> Self {
        Self::InvalidServerConfiguration(error)
    }
}

impl From<HttpTlsConfigError> for AppBuildError {
    fn from(error: HttpTlsConfigError) -> Self {
        Self::TlsConfiguration(error)
    }
}

impl From<InjectionError> for AppBuildError {
    fn from(error: InjectionError) -> Self {
        Self::DependencyInjection(error)
    }
}

impl From<DuplicateRouteError> for AppBuildError {
    fn from(error: DuplicateRouteError) -> Self {
        Self::DuplicateRoute(error)
    }
}

impl From<ControllerMaterializationError> for AppBuildError {
    fn from(error: ControllerMaterializationError) -> Self {
        Self::ControllerMaterialization(error)
    }
}

impl From<OpenApiRouteRegistryError> for AppBuildError {
    fn from(error: OpenApiRouteRegistryError) -> Self {
        Self::OpenApiRouteRegistry(Box::new(error))
    }
}

impl From<OpenApiRouteBuildError> for AppBuildError {
    fn from(error: OpenApiRouteBuildError) -> Self {
        match error {
            OpenApiRouteBuildError::Route(error) => error.into(),
            OpenApiRouteBuildError::Registry(error) => Self::OpenApiRouteRegistry(error),
        }
    }
}

impl From<OpenApiSecurityValidationError> for AppBuildError {
    fn from(error: OpenApiSecurityValidationError) -> Self {
        Self::OpenApiSecurity(Box::new(error))
    }
}

impl From<OpenApiServiceError> for AppBuildError {
    fn from(error: OpenApiServiceError) -> Self {
        Self::OpenApiInitialization(error)
    }
}

impl From<RouteTableBuildError> for AppBuildError {
    fn from(error: RouteTableBuildError) -> Self {
        match error {
            RouteTableBuildError::Duplicate(error) => Self::DuplicateRoute(error),
            RouteTableBuildError::Invalid(error) => Self::InvalidRoute(error),
            RouteTableBuildError::Ambiguous(error) => Self::AmbiguousRoute(error),
        }
    }
}

impl From<MissingRouteGuardError> for AppBuildError {
    fn from(error: MissingRouteGuardError) -> Self {
        Self::MissingRouteGuard(error)
    }
}

impl From<GuardInitError> for AppBuildError {
    fn from(error: GuardInitError) -> Self {
        Self::GuardInitialization(error)
    }
}
