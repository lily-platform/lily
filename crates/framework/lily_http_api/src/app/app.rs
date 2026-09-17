use crate::{
    app::{
        middleware_executor::{
            CompiledHttpMiddleware, HttpMiddlewareChain, HttpMiddlewareErrorWriter,
            HttpMiddlewareObservationOutcome, HttpMiddlewareObserver, MiddlewareChainOutcome,
            MiddlewareChainOutcomeSlot,
        },
        AppBuildError, HttpServerConfigError, HttpTlsConfigError, MissingRouteGuardError,
        RouteMiddlewareBuildError, RouteMiddlewareBuildErrorCause,
    },
    csrf::{CsrfEnforcementMiddleware, CsrfGuard, CsrfService},
    guard::GuardTrait,
    health::HttpHealthService,
    openapi::{OpenApiConfig, OpenApiPreparedDocument, OpenApiService},
    private::{CorsPolicyProviderRegistration, HttpMiddlewareRegistration},
    registry::RouteInfo,
    request_lifecycle::{RequestExecutionContext, RequestRegistry},
    route_table::{RouteResolution, RouteTable},
    server::server::{
        http_method_metric_label, HttpServer, HttpTransportConfig, ManagedHttpServer,
    },
    shutdown::{ShutdownBudget, ShutdownStage},
    tasks::{HttpTaskInventory, TaskReceipt, TaskRegistry},
    telemetry::{http_status_error_code, http_status_outcome},
};
use futures::FutureExt;
use lily_background_service::{BackgroundServiceTrait, BackgroundServices};
use lily_config::{ConfigService, SecretResolver};
use lily_error::{application::http_api::HttpApiError, LocalizationCatalog};
use lily_injection::{ApplicationContainer, Extensions, DEFAULT_SHUTDOWN_TIMEOUT};
use lily_middleware::__private::{
    CompiledCsrfPolicy, CorsLayerAdapter, CorsOriginResolverRegistry, HttpNextService,
};
use lily_middleware::{
    validate_middleware_count, validate_middleware_descriptors, CorsPolicy, CsrfPolicy,
    HttpExchange, HttpMiddleware, HttpMiddlewareError, HttpMiddlewareInitError,
    MiddlewareDescriptor, MiddlewareErrorCode,
};
use lily_shutdown::{
    FrameworkShutdownComponent, FrameworkShutdownCoordinator, FrameworkShutdownPhase,
    ShutdownError, ShutdownSignal, ShutdownState, SignalHandler, SignalMonitor,
};
use lily_trace::prelude::*;
use lily_web_core::{
    enums::HttpProtocol, write_error_response, IntoResponse, Request, Response,
    ResponseFailureKind, ResponseWriteError, ResponseWriteOutcome, RustlsConfig,
};
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{global, KeyValue};
use std::{
    any::TypeId,
    collections::{HashMap, HashSet},
    io,
    net::{IpAddr, SocketAddr},
    panic::{catch_unwind, AssertUnwindSafe},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};
use tokio_util::sync::CancellationToken;

#[path = "build_lifecycle.rs"]
mod build_lifecycle;
use build_lifecycle::HttpBuildGuard;

#[path = "dependencies.rs"]
mod dependencies;
use dependencies::{HttpDependencies, HttpDependencyHandle, HttpReconciliationHandle};

#[path = "report.rs"]
mod report;
use crate::shutdown_report::FrameworkSnapshot;
use report::HttpFrozenAttempt;

#[cfg(test)]
#[path = "lifecycle_tests.rs"]
mod lifecycle_tests;

#[cfg(test)]
#[path = "qualification_tests.rs"]
mod qualification_tests;

#[cfg(test)]
#[path = "background_tests.rs"]
mod background_tests;

#[derive(Clone, Default)]
enum LocalizationSource {
    #[default]
    Disabled,
    Catalog(Arc<LocalizationCatalog>),
    Path(PathBuf),
}

/// Selection policy for the HTTP listener's transport security.
#[derive(Clone, Default)]
enum HttpTlsSource {
    /// Apply the canonical `lily_config.server` TLS fields.
    #[default]
    Config,
    /// Intentionally publish a plaintext listener.
    Disabled,
    /// Use an application-supplied complete Rustls configuration.
    Explicit(RustlsConfig),
}

#[derive(Clone, Default)]
enum HttpAddressSource {
    #[default]
    Config,
    Explicit(String),
}

#[derive(Clone, Default)]
enum HttpTransportSource {
    #[default]
    Config,
    Explicit(Box<HttpTransportConfig>),
}

/// Immutable listener configuration frozen by [`AppBuilder::build`].
///
/// Builder overrides and config-backed values are resolved into this single
/// authority before a socket is opened. Every connection observes the same
/// address, protocol, transport limits, and optional TLS identity. Its
/// [`Debug`] representation reports only whether TLS is enabled and never
/// renders certificate or private-key material.
#[derive(Clone)]
pub struct EffectiveHttpServerConfig {
    listen_address: String,
    protocol: HttpProtocol,
    transport: HttpTransportConfig,
    tls_config: Option<RustlsConfig>,
}

impl std::fmt::Debug for EffectiveHttpServerConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EffectiveHttpServerConfig")
            .field("listen_address", &self.listen_address)
            .field("protocol", &self.protocol)
            .field("transport", &self.transport)
            .field("tls_enabled", &self.tls_config.is_some())
            .finish()
    }
}

impl EffectiveHttpServerConfig {
    async fn resolve(
        address_source: &HttpAddressSource,
        protocol: HttpProtocol,
        transport_source: &HttpTransportSource,
        tls_source: &HttpTlsSource,
        config: &lily_config::ServerConfig,
    ) -> Result<Self, AppBuildError> {
        let listen_address = Self::resolve_address(address_source, config)?;
        let transport = Self::resolve_transport(transport_source, config)?;
        let tls_config = tls_source.resolve(config).await?;
        Ok(Self {
            listen_address,
            protocol,
            transport,
            tls_config,
        })
    }

    fn resolve_address(
        source: &HttpAddressSource,
        config: &lily_config::ServerConfig,
    ) -> Result<String, HttpServerConfigError> {
        match source {
            HttpAddressSource::Explicit(address) => {
                let address = address.trim();
                if address.is_empty() {
                    return Err(HttpServerConfigError::InvalidListenAddress);
                }
                let parsed = url::Url::parse(&format!("tcp://{address}"))
                    .map_err(|_| HttpServerConfigError::InvalidListenAddress)?;
                if parsed.host().is_none()
                    || !parsed.username().is_empty()
                    || parsed.password().is_some()
                    || !parsed.path().is_empty()
                    || parsed.query().is_some()
                    || parsed.fragment().is_some()
                {
                    return Err(HttpServerConfigError::InvalidListenAddress);
                }
                if parsed.port().is_none() {
                    return Err(HttpServerConfigError::MissingListenPort);
                }
                Ok(address.to_string())
            }
            HttpAddressSource::Config => {
                let host = config.host.trim();
                if host.is_empty() {
                    return Err(HttpServerConfigError::EmptyHost);
                }
                if config.port == 0 {
                    return Err(HttpServerConfigError::ZeroPort);
                }
                let rendered_host = match host.parse::<IpAddr>() {
                    Ok(IpAddr::V4(address)) => address.to_string(),
                    Ok(IpAddr::V6(address)) => format!("[{address}]"),
                    Err(_) => match url::Host::parse(host)
                        .map_err(|_| HttpServerConfigError::InvalidHost)?
                    {
                        url::Host::Domain(domain) => domain,
                        url::Host::Ipv4(address) => address.to_string(),
                        url::Host::Ipv6(address) => format!("[{address}]"),
                    },
                };
                Ok(format!("{rendered_host}:{}", config.port))
            }
        }
    }

    fn resolve_transport(
        source: &HttpTransportSource,
        config: &lily_config::ServerConfig,
    ) -> Result<HttpTransportConfig, HttpServerConfigError> {
        let transport = match source {
            HttpTransportSource::Explicit(transport) => transport.as_ref().clone(),
            HttpTransportSource::Config => {
                let mut transport = HttpTransportConfig::default();
                if let Some(max_connections) = config.max_connections {
                    transport.max_connections = max_connections;
                }
                if let Some(max_part_bytes) = config.max_multipart_part_bytes {
                    transport.max_multipart_part_bytes = max_part_bytes;
                }
                if let Some(max_parts) = config.max_multipart_parts {
                    transport.max_multipart_parts = max_parts;
                }
                if let Some(max_metadata_bytes) = config.max_multipart_metadata_bytes {
                    transport.max_multipart_metadata_bytes = max_metadata_bytes;
                }
                if let Some(timeout_secs) = config.connection_idle_timeout_secs {
                    transport.connection_idle_timeout = Duration::from_secs(timeout_secs);
                }
                if let Some(timeout_secs) = config.request_timeout_secs {
                    transport.request_timeout = Duration::from_secs(timeout_secs);
                }
                transport
            }
        };
        transport
            .validate()
            .map_err(HttpServerConfigError::InvalidTransport)?;
        Ok(transport)
    }

    /// Returns the validated `host:port` address used when binding the listener.
    pub fn listen_address(&self) -> &str {
        &self.listen_address
    }

    /// Returns the HTTP protocol policy selected for every accepted connection.
    pub fn protocol(&self) -> HttpProtocol {
        self.protocol
    }

    /// Returns the validated transport limits used by the managed server.
    pub fn transport(&self) -> &HttpTransportConfig {
        &self.transport
    }

    /// Reports whether the effective listener uses TLS.
    pub fn tls_enabled(&self) -> bool {
        self.tls_config.is_some()
    }

    /// Returns the validated Rustls configuration when listener TLS is enabled.
    ///
    /// The value is shared read-only; protocol-specific ALPN values are applied
    /// to an internal clone when the listener starts.
    pub fn rustls_config(&self) -> Option<&RustlsConfig> {
        self.tls_config.as_ref()
    }
}

#[derive(serde::Serialize)]
struct MiddlewarePublicErrorBody {
    error: bool,
    code: &'static str,
    message: &'static str,
    status: u16,
}

/// Private terminal metadata retained alongside an already-materialized
/// response. Final status classifies a completed HTTP response; an actual
/// writer failure retains technical failure even if middleware changes status.
/// The optional diagnostic is bounded and static. Application error origin is
/// retained independently of later status changes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AppCallOutcome {
    status: u16,
    outcome: &'static str,
    error_code: Option<&'static str>,
    application_error: bool,
    application_error_code: Option<&'static str>,
    application_failure: Option<ResponseFailureKind>,
}

impl AppCallOutcome {
    pub(crate) fn from_final_response(
        response: &Response,
        error_code: Option<&'static str>,
    ) -> Self {
        let status = response.status_code_value();
        let outcome = http_status_outcome(status);
        let error_code = match outcome {
            "rejected" => Some(error_code.unwrap_or("HTTP_CLIENT_ERROR")),
            "error" => Some(error_code.unwrap_or("HTTP_SERVER_ERROR")),
            _ => None,
        };
        Self {
            status,
            outcome,
            error_code,
            application_error: false,
            application_error_code: None,
            application_failure: None,
        }
    }

    fn from_terminal_response(response: &Response, metadata: TerminalResponseMetadata) -> Self {
        let mut outcome = Self::from_final_response(response, metadata.response_error_code);
        outcome.application_failure = metadata.application_failure;
        if metadata.application_error {
            outcome = outcome.with_application_error(metadata.application_error_code);
        }
        // A failed writer is a technical failure even if outer middleware
        // changes the fallback status. Successfully rendering an application
        // error, on the other hand, does not imply a failed writer.
        if metadata.response_write_error {
            outcome.outcome = "error";
            outcome.error_code = Some("RESPONSE_ENCODING_ERROR");
        }
        outcome
    }

    pub(crate) fn with_application_error(mut self, code: Option<&'static str>) -> Self {
        self.application_error = true;
        self.application_error_code = Some(code.unwrap_or("APPLICATION_ERROR"));
        self
    }

    pub(crate) const fn application_error(self) -> bool {
        self.application_error
    }

    pub(crate) const fn application_error_code(self) -> Option<&'static str> {
        self.application_error_code
    }

    pub(crate) const fn application_failure(self) -> Option<ResponseFailureKind> {
        self.application_failure
    }

    pub(crate) const fn status(self) -> u16 {
        self.status
    }

    pub(crate) const fn outcome(self) -> &'static str {
        self.outcome
    }

    pub(crate) const fn error_code(self) -> Option<&'static str> {
        self.error_code
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct TerminalResponseMetadata {
    application_error: bool,
    application_error_code: Option<&'static str>,
    response_error_code: Option<&'static str>,
    application_failure: Option<ResponseFailureKind>,
    response_write_error: bool,
}

#[derive(Default)]
struct TerminalResponseStateData {
    metadata: TerminalResponseMetadata,
    write_error: Option<ResponseWriteError>,
    fallback_attempted: bool,
    fatal_write_error: Option<ResponseWriteError>,
}

/// Shared only by the stack-owned terminal and middleware adapters. No lock is
/// held over an await. A failed fallback cannot be retried by an outer chain.
#[derive(Default)]
struct TerminalResponseState(Mutex<TerminalResponseStateData>);

impl TerminalResponseState {
    fn snapshot(&self) -> TerminalResponseMetadata {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .metadata
    }

    fn record_response_code(&self, code: &'static str) {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .metadata
            .response_error_code = Some(code);
    }

    fn record_action(
        &self,
        outcome: ResponseWriteOutcome,
        status: u16,
    ) -> (&'static str, Option<&'static str>) {
        if !outcome.is_error_response() {
            return (http_status_outcome(status), http_status_error_code(status));
        }
        let failure = outcome.failure_kind().or(match status {
            400..=499 => Some(ResponseFailureKind::Rejected),
            500..=599 => Some(ResponseFailureKind::Error),
            _ => None,
        });
        let code = outcome
            .error_code()
            .map(|code| code.as_str())
            .unwrap_or(match status {
                400..=499 => "HTTP_CLIENT_ERROR",
                500..=599 => "HTTP_SERVER_ERROR",
                _ => "APPLICATION_ERROR",
            });
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.metadata.application_error = true;
        state.metadata.application_error_code = Some(code);
        state.metadata.application_failure = failure;
        state.metadata.response_error_code = Some(code);
        (
            failure
                .map(ResponseFailureKind::as_str)
                .unwrap_or("success"),
            Some(code),
        )
    }

    fn record_write_error(&self, error: ResponseWriteError) {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .write_error = Some(error);
    }

    fn write_error(&self) -> ResponseWriteError {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .write_error
            .unwrap_or(ResponseWriteError::InvalidOutcome)
    }

    fn fatal_write_error(&self) -> Option<ResponseWriteError> {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .fatal_write_error
    }

    fn begin_fallback(&self, error: ResponseWriteError) -> Result<(), ResponseWriteError> {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if state.fallback_attempted {
            let error = *state.fatal_write_error.get_or_insert(error);
            return Err(error);
        }
        state.fallback_attempted = true;
        state.metadata.response_write_error = true;
        state.write_error = Some(error);
        Ok(())
    }

    fn fail_fallback(&self, error: ResponseWriteError) {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .fatal_write_error = Some(error);
    }
}

struct AppTerminal<'app, 'outcome> {
    app: &'app App,
    state: &'outcome TerminalResponseState,
}

struct MatchedRouteTerminal<'app, 'route, 'outcome> {
    app: &'app App,
    route: &'route RouteInfo,
    method: &'route str,
    method_metric: &'static str,
    state: &'outcome TerminalResponseState,
}

struct AppErrorWriter<'app, 'outcome> {
    app: &'app App,
    state: &'outcome TerminalResponseState,
}

impl HttpTlsSource {
    async fn resolve(
        &self,
        config: &lily_config::ServerConfig,
    ) -> Result<Option<RustlsConfig>, HttpTlsConfigError> {
        match self {
            Self::Disabled => Ok(None),
            Self::Explicit(config) => Ok(Some(config.clone())),
            Self::Config if config.tls_enabled == Some(true) => {
                let certificate_path = config
                    .tls_cert_path
                    .as_deref()
                    .ok_or(HttpTlsConfigError::CertificatePathRequired)?;
                let private_key_path = config
                    .tls_key_path
                    .as_deref()
                    .ok_or(HttpTlsConfigError::PrivateKeyPathRequired)?;
                RustlsConfig::from_pem_file(certificate_path, private_key_path)
                    .await
                    .map(Some)
                    .map_err(HttpTlsConfigError::from)
            }
            Self::Config if config.tls_cert_path.is_some() || config.tls_key_path.is_some() => {
                Err(HttpTlsConfigError::CertificateMaterialWhileDisabled)
            }
            Self::Config => Ok(None),
        }
    }
}

impl LocalizationSource {
    async fn resolve(&self) -> Result<Option<Arc<LocalizationCatalog>>, AppBuildError> {
        match self {
            Self::Disabled => Ok(None),
            Self::Catalog(catalog) => Ok(Some(Arc::clone(catalog))),
            Self::Path(path) => LocalizationCatalog::load(path)
                .await
                .map(Arc::new)
                .map(Some)
                .map_err(|error| {
                    AppBuildError::InvalidLocalizationConfiguration(error.to_string())
                }),
        }
    }
}

// Build failure has no App root yet. The process inventory retains its actual
// rollback join, including when the build caller drops its rollback waiter.
static BUILD_ROLLBACK_TASKS: OnceLock<TaskRegistry> = OnceLock::new();

async fn close_build_dependencies(
    dependencies: Arc<HttpDependencies>,
    budget: ShutdownBudget,
) -> Vec<String> {
    let mut failures = Vec::new();
    if let Err(error) = dependencies.close_background(&budget).await {
        failures.push(error.to_string());
    }
    if let Err(error) = dependencies.close_di(&budget).await {
        failures.push(error.to_string());
    }
    if let Err(error) = dependencies.close_tracing(&budget).await {
        failures.push(error.to_string());
    }
    if let Err(error) = dependencies.reconcile(&budget).await {
        failures.push(error.to_string());
    }
    if !failures.is_empty() {
        tracing::warn!(?failures, "HTTP build rollback incomplete");
    }
    failures
}

async fn run_build_rollback(
    original: AppBuildError,
    dependencies: Arc<HttpDependencies>,
    budget: ShutdownBudget,
) -> AppBuildError {
    budget.begin();
    dependencies.retain();
    let receipt = BUILD_ROLLBACK_TASKS
        .get_or_init(TaskRegistry::default)
        .spawn(close_build_dependencies(dependencies, budget.clone()));
    let cleanup_failures = match budget.wait_for_receipt(ShutdownStage::Final, receipt).await {
        Ok(Ok(failures)) => failures.as_ref().clone(),
        Ok(Err(error)) => vec![format!("HTTP build rollback join failed: {error}")],
        Err(()) => vec!["HTTP build rollback join outstanding; receipt retained".into()],
    };
    if cleanup_failures.is_empty() {
        original
    } else {
        AppBuildError::StartupRollback {
            original: Box::new(original),
            cleanup_failures,
        }
    }
}

async fn rollback_failed_build(
    original: AppBuildError,
    container: &Arc<ApplicationContainer>,
    owns_container: bool,
    shutdown_timeout: Duration,
    tracing_owner: Option<TracingRuntimeOwner>,
) -> AppBuildError {
    let budget = ShutdownBudget::from_started(shutdown_timeout, tokio::time::Instant::now());
    let dependencies = HttpDependencies::new(container.clone(), owns_container, tracing_owner);
    run_build_rollback(original, dependencies, budget).await
}

async fn rollback_tracing_only(
    original: AppBuildError,
    budget: ShutdownBudget,
    tracing_owner: Option<TracingRuntimeOwner>,
    di_quiescent: bool,
) -> AppBuildError {
    let dependencies = HttpDependencies::with_container(None, false, tracing_owner);
    if !di_quiescent {
        dependencies.retain();
        return AppBuildError::StartupRollback {
            original: Box::new(original),
            cleanup_failures: vec!["HTTP telemetry blocked by outstanding DI build users".into()],
        };
    }
    run_build_rollback(original, dependencies, budget).await
}

fn middleware_initialization_internal(code: &'static str) -> HttpMiddlewareInitError {
    HttpMiddlewareInitError::internal(
        MiddlewareErrorCode::new(code)
            .expect("framework middleware initialization codes are valid"),
    )
}

async fn instantiate_registered_middleware(
    registration: HttpMiddlewareRegistration,
    extensions: Arc<Extensions>,
    index: usize,
) -> Result<(Arc<dyn HttpMiddleware>, MiddlewareDescriptor), AppBuildError> {
    let middleware = registration
        .instantiate(extensions)
        .await
        .map_err(|error| AppBuildError::MiddlewareInitialization { index, error })?;

    catch_unwind(AssertUnwindSafe(|| middleware.validate()))
        .map_err(|_| AppBuildError::MiddlewareInitialization {
            index,
            error: middleware_initialization_internal("MIDDLEWARE_VALIDATE_PANICKED"),
        })?
        .map_err(|error| AppBuildError::InvalidMiddlewareConfiguration {
            index: Some(index),
            error,
        })?;

    let descriptor = catch_unwind(AssertUnwindSafe(|| middleware.descriptor())).map_err(|_| {
        AppBuildError::MiddlewareInitialization {
            index,
            error: middleware_initialization_internal("MIDDLEWARE_DESCRIPTOR_PANICKED"),
        }
    })?;

    Ok((middleware, descriptor))
}

async fn build_route_middleware_plans(
    route_table: &RouteTable,
    application_middlewares: &[CompiledHttpMiddleware],
    mut middleware_by_type: HashMap<TypeId, CompiledHttpMiddleware>,
    extensions: Arc<Extensions>,
) -> Result<Vec<Arc<[CompiledHttpMiddleware]>>, AppBuildError> {
    let mut plans = vec![Arc::<[CompiledHttpMiddleware]>::from([]); route_table.route_count()];
    let mut routes = route_table.routes().collect::<Vec<_>>();
    routes.sort_unstable_by_key(|route| route.route_plan_id);

    for route in routes {
        validate_middleware_count(
            application_middlewares
                .len()
                .saturating_add(route.middleware_registrations.len()),
        )
        .map_err(|error| {
            AppBuildError::RouteMiddleware(RouteMiddlewareBuildError {
                method: route.method.clone(),
                path: route.path.clone(),
                handler: route.handler_name.clone(),
                middleware: "<route-chain>",
                cause: RouteMiddlewareBuildErrorCause::InvalidConfiguration(error),
            })
        })?;

        let mut seen = HashSet::with_capacity(route.middleware_registrations.len());
        let mut plan = Vec::with_capacity(route.middleware_registrations.len());
        for registration in &route.middleware_registrations {
            if !seen.insert(registration.type_id()) {
                return Err(AppBuildError::RouteMiddleware(RouteMiddlewareBuildError {
                    method: route.method.clone(),
                    path: route.path.clone(),
                    handler: route.handler_name.clone(),
                    middleware: registration.type_name(),
                    cause: RouteMiddlewareBuildErrorCause::DuplicateType,
                }));
            }

            let compiled = if let Some(compiled) = middleware_by_type.get(&registration.type_id()) {
                compiled.clone()
            } else {
                let middleware = registration
                    .instantiate(Arc::clone(&extensions))
                    .await
                    .map_err(|error| {
                        AppBuildError::RouteMiddleware(RouteMiddlewareBuildError {
                            method: route.method.clone(),
                            path: route.path.clone(),
                            handler: route.handler_name.clone(),
                            middleware: registration.type_name(),
                            cause: RouteMiddlewareBuildErrorCause::Initialization(error),
                        })
                    })?;
                catch_unwind(AssertUnwindSafe(|| middleware.validate()))
                    .map_err(|_| {
                        AppBuildError::RouteMiddleware(RouteMiddlewareBuildError {
                            method: route.method.clone(),
                            path: route.path.clone(),
                            handler: route.handler_name.clone(),
                            middleware: registration.type_name(),
                            cause: RouteMiddlewareBuildErrorCause::Initialization(
                                middleware_initialization_internal("MIDDLEWARE_VALIDATE_PANICKED"),
                            ),
                        })
                    })?
                    .map_err(|error| {
                        AppBuildError::RouteMiddleware(RouteMiddlewareBuildError {
                            method: route.method.clone(),
                            path: route.path.clone(),
                            handler: route.handler_name.clone(),
                            middleware: registration.type_name(),
                            cause: RouteMiddlewareBuildErrorCause::InvalidConfiguration(error),
                        })
                    })?;
                let descriptor = catch_unwind(AssertUnwindSafe(|| middleware.descriptor()))
                    .map_err(|_| {
                        AppBuildError::RouteMiddleware(RouteMiddlewareBuildError {
                            method: route.method.clone(),
                            path: route.path.clone(),
                            handler: route.handler_name.clone(),
                            middleware: registration.type_name(),
                            cause: RouteMiddlewareBuildErrorCause::Initialization(
                                middleware_initialization_internal(
                                    "MIDDLEWARE_DESCRIPTOR_PANICKED",
                                ),
                            ),
                        })
                    })?;
                let compiled = CompiledHttpMiddleware::new(middleware, descriptor);
                middleware_by_type.insert(registration.type_id(), compiled.clone());
                compiled
            };
            plan.push(compiled);
        }

        let effective_descriptors: Vec<_> = application_middlewares
            .iter()
            .chain(plan.iter())
            .map(CompiledHttpMiddleware::descriptor)
            .collect();
        validate_middleware_descriptors(&effective_descriptors).map_err(|error| {
            AppBuildError::RouteMiddleware(RouteMiddlewareBuildError {
                method: route.method.clone(),
                path: route.path.clone(),
                handler: route.handler_name.clone(),
                middleware: "<route-chain>",
                cause: RouteMiddlewareBuildErrorCause::InvalidConfiguration(error),
            })
        })?;
        plans[route.route_plan_id] = plan.into();
    }

    Ok(plans)
}

const MAX_CORS_POLICY_PLANS: usize = 64;

#[derive(Clone)]
struct CorsApplicationState {
    adapters: Arc<[Arc<CorsLayerAdapter>]>,
    fallback_plan_id: usize,
    route_plan_ids: Option<Arc<[usize]>>,
}

impl CorsApplicationState {
    fn adapter(&self, plan_id: usize) -> &Arc<CorsLayerAdapter> {
        &self.adapters[plan_id]
    }

    fn adapters(&self) -> &[Arc<CorsLayerAdapter>] {
        &self.adapters
    }

    fn plan_for_route(&self, route: &RouteInfo) -> usize {
        self.route_plan_ids
            .as_ref()
            .map_or(self.fallback_plan_id, |plans| plans[route.route_plan_id])
    }
}

async fn compile_cors_adapter(
    policy: &CorsPolicy,
    extensions: Arc<Extensions>,
    resolver_registry: &mut CorsOriginResolverRegistry,
) -> Result<Arc<CorsLayerAdapter>, AppBuildError> {
    let adapter = CorsLayerAdapter::try_new(policy)
        .map_err(AppBuildError::InvalidCorsConfiguration)?
        .initialize_origin_resolver_with(extensions, resolver_registry)
        .await
        .map_err(AppBuildError::CorsOriginResolverInitialization)?;
    Ok(Arc::new(adapter))
}

async fn build_cors_application_state(
    global_policy: Option<&CorsPolicy>,
    route_table: &RouteTable,
    extensions: Arc<Extensions>,
) -> Result<Option<Arc<CorsApplicationState>>, AppBuildError> {
    let mut providers = HashMap::<TypeId, CorsPolicyProviderRegistration>::new();
    for route in route_table.routes() {
        if let Some(registration) = route.cors_policy_registration.provider_registration() {
            providers
                .entry(registration.type_id())
                .or_insert(registration);
        }
    }

    if global_policy.is_none() && providers.is_empty() {
        return Ok(None);
    }

    let needs_fallback_deny = global_policy.is_none();
    let plan_count = providers
        .len()
        .saturating_add(usize::from(global_policy.is_some()))
        .saturating_add(usize::from(needs_fallback_deny));
    if plan_count > MAX_CORS_POLICY_PLANS {
        return Err(AppBuildError::InvalidCorsConfiguration(
            lily_middleware::MiddlewareConfigError::middleware(
                MiddlewareErrorCode::new("CORS_POLICY_PLAN_LIMIT")
                    .expect("built-in CORS plan limit code is valid"),
            ),
        ));
    }

    let mut resolver_registry = CorsOriginResolverRegistry::default();
    let mut adapters = Vec::with_capacity(plan_count);
    let global_plan_id = if let Some(policy) = global_policy {
        let plan_id = adapters.len();
        adapters.push(
            compile_cors_adapter(policy, Arc::clone(&extensions), &mut resolver_registry).await?,
        );
        Some(plan_id)
    } else {
        None
    };

    let mut providers = providers.into_values().collect::<Vec<_>>();
    providers.sort_unstable_by_key(|registration| registration.type_name());
    let mut provider_plan_ids = HashMap::with_capacity(providers.len());
    for registration in providers {
        let policy = registration
            .policy()
            .map_err(AppBuildError::InvalidCorsConfiguration)?;
        let plan_id = adapters.len();
        adapters.push(
            compile_cors_adapter(&policy, Arc::clone(&extensions), &mut resolver_registry).await?,
        );
        provider_plan_ids.insert(registration.type_id(), plan_id);
    }

    let fallback_plan_id = if let Some(global_plan_id) = global_plan_id {
        global_plan_id
    } else {
        let plan_id = adapters.len();
        adapters.push(
            compile_cors_adapter(&CorsPolicy::new(), extensions, &mut resolver_registry).await?,
        );
        plan_id
    };

    let route_plan_ids = if provider_plan_ids.is_empty() {
        None
    } else {
        let mut plans = vec![fallback_plan_id; route_table.route_count()];
        for route in route_table.routes() {
            if let Some(registration) = route.cors_policy_registration.provider_registration() {
                plans[route.route_plan_id] = provider_plan_ids[&registration.type_id()];
            }
        }
        Some(plans.into())
    };

    Ok(Some(Arc::new(CorsApplicationState {
        adapters: adapters.into(),
        fallback_plan_id,
        route_plan_ids,
    })))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CsrfApplicationMode {
    Global,
    RouteScoped,
}

/// Composition root for one immutable HTTP application.
///
/// `Default` reads the listener address and supported transport overrides from
/// the application's [`ConfigService`]. [`Self::new`] instead supplies an
/// explicit address while retaining config-backed TLS and transport defaults.
/// Calling [`Self::build`] validates and initializes DI, controller, middleware,
/// guard, CORS, CSRF, localization, OpenAPI, health, and telemetry state without
/// opening a listener. The returned [`App`] owns the resulting runtime graph.
pub struct AppBuilder {
    background_services: BackgroundServices,
    address_source: HttpAddressSource,
    protocol: HttpProtocol,
    transport_source: HttpTransportSource,
    middlewares: Vec<HttpMiddlewareRegistration>,
    session_middleware: Option<HttpMiddlewareRegistration>,
    duplicate_session_middleware: bool,
    cors_policy: Option<CorsPolicy>,
    duplicate_cors_policy: bool,
    csrf_policy: Option<CsrfPolicy>,
    csrf_mode: Option<CsrfApplicationMode>,
    duplicate_csrf_policy: bool,
    conflicting_csrf_modes: bool,
    explicit_csrf_guard: bool,
    guard_instances: HashMap<TypeId, Arc<dyn GuardTrait + Send + Sync>>,
    container: Option<Arc<ApplicationContainer>>,
    secret_resolver: Option<Arc<dyn SecretResolver>>,
    localization_source: LocalizationSource,
    tracing_mode: TracingMode,
    tls_source: HttpTlsSource,
    openapi_config: Option<OpenApiConfig>,
}

impl Default for AppBuilder {
    fn default() -> Self {
        Self::config_backed()
    }
}

impl AppBuilder {
    /// Creates a builder with an explicit listener address.
    ///
    /// `address` must be a `host:port` value. It overrides the configured
    /// `server.host` and `server.port`; other listener settings remain
    /// config-backed until their corresponding builder methods are called.
    pub fn new(address: &str) -> Self {
        let mut builder = Self::config_backed();
        builder.address_source = HttpAddressSource::Explicit(address.to_string());
        builder
    }

    fn config_backed() -> Self {
        Self {
            background_services: BackgroundServices::default(),
            address_source: HttpAddressSource::Config,
            protocol: HttpProtocol::Auto,
            transport_source: HttpTransportSource::Config,
            middlewares: Vec::new(),
            session_middleware: None,
            duplicate_session_middleware: false,
            cors_policy: None,
            duplicate_cors_policy: false,
            csrf_policy: None,
            csrf_mode: None,
            duplicate_csrf_policy: false,
            conflicting_csrf_modes: false,
            explicit_csrf_guard: false,
            guard_instances: HashMap::new(),
            container: None,
            secret_resolver: None,
            localization_source: LocalizationSource::Disabled,
            tracing_mode: TracingMode::Disabled,
            tls_source: HttpTlsSource::Config,
            openapi_config: None,
        }
    }

    /// Register one background worker per concrete type. Construction happens
    /// during build; execution starts once after successful listener bind.
    /// Unhandled worker errors/panics initiate controlled application shutdown.
    pub fn add_background_service<T: BackgroundServiceTrait>(mut self) -> Self {
        self.background_services.add::<T>();
        self
    }

    /// Selects the HTTP protocol policy for accepted connections.
    ///
    /// [`HttpProtocol::Auto`] is the default. TLS listeners advertise the
    /// selected policy with ALPN; plaintext listeners apply the same policy to
    /// Hyper's connection codec.
    pub fn protocol(mut self, protocol: HttpProtocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Replaces all bounded HTTP/1.1 and HTTP/2 transport limits.
    ///
    /// This explicit snapshot takes precedence over transport values obtained
    /// from [`ConfigService`]. Invalid or unbounded values fail during
    /// [`Self::build`], before the listener is opened.
    pub fn transport_config(mut self, config: HttpTransportConfig) -> Self {
        self.transport_source = HttpTransportSource::Explicit(Box::new(config));
        self
    }

    /// Publishes this application with a complete Rustls server configuration.
    ///
    /// This explicit composition-root value takes precedence over
    /// `lily_config.server.tls_*`. The HTTP adapter sets ALPN to match the
    /// selected [`HttpProtocol`] when it binds the listener.
    pub fn rustls_config(mut self, config: impl Into<RustlsConfig>) -> Self {
        self.tls_source = HttpTlsSource::Explicit(config.into());
        self
    }

    /// Explicitly disables listener TLS, overriding `lily_config.server.tls_*`.
    pub fn tls_disabled(mut self) -> Self {
        self.tls_source = HttpTlsSource::Disabled;
        self
    }

    /// Registers one application middleware type.
    ///
    /// The middleware is constructed exactly once through
    /// [`HttpMiddleware::new`] after the application DI graph is available.
    /// Registrations execute in call order outside route/controller middleware,
    /// guards, and the action. Ready-made middleware instances are deliberately
    /// not accepted.
    #[must_use]
    pub fn middleware<M>(mut self) -> Self
    where
        M: HttpMiddleware,
    {
        self.middlewares.push(HttpMiddlewareRegistration::of::<M>());
        self
    }

    /// Registers the single application-owned session preparation middleware.
    ///
    /// This slot always runs before global CSRF enforcement and ordinary
    /// application middleware. Lily owns only construction and ordering; the
    /// session model, storage, expiry and cookie behavior remain application
    /// responsibilities. When [`crate::CsrfRequestLocalBinding`] is selected,
    /// this middleware publishes the verified [`crate::CsrfSessionId`] before
    /// calling `next`. A second
    /// registration fails during [`Self::build`].
    #[must_use]
    pub fn session_middleware<M>(mut self) -> Self
    where
        M: HttpMiddleware,
    {
        if self.session_middleware.is_some() {
            self.duplicate_session_middleware = true;
        } else {
            self.session_middleware = Some(HttpMiddlewareRegistration::of::<M>());
        }
        self
    }

    /// Installs the canonical, application-wide browser CORS policy.
    ///
    /// CORS is exclusive: registering this method more than once fails during
    /// [`Self::build`] before the listener is opened. The compiled policy runs
    /// at the managed transport boundary. Valid preflight requests retain
    /// admission, timeout and response-write accounting but short-circuit
    /// before generic application middleware, guards and route dispatch.
    /// Controller/action CORS metadata may select another compiled policy for a
    /// matched route; this policy remains the fallback for inherited and
    /// unmatched routes.
    #[must_use]
    pub fn cors(mut self, policy: CorsPolicy) -> Self {
        if self.cors_policy.is_some() {
            self.duplicate_cors_policy = true;
        } else {
            self.cors_policy = Some(policy);
        }
        self
    }

    /// Installs one application-wide CSRF policy at the start of the managed
    /// middleware chain.
    ///
    /// Signed and synchronizer-token modes also configure the injectable
    /// [`CsrfService`] used by application endpoints to issue, rotate, and
    /// clear tokens. Synchronizer policies use only their explicit
    /// application-supplied token store. Registering this method more than once
    /// fails during [`Self::build`]. CSRF is never enabled implicitly. A
    /// request-local session binding requires a
    /// [`Self::session_middleware`] registration that publishes its verified
    /// session identifier before forwarding the request.
    #[must_use]
    pub fn csrf(mut self, policy: CsrfPolicy) -> Self {
        match self.csrf_mode {
            Some(CsrfApplicationMode::Global) => self.duplicate_csrf_policy = true,
            Some(CsrfApplicationMode::RouteScoped) => self.conflicting_csrf_modes = true,
            None => {
                self.csrf_policy = Some(policy);
                self.csrf_mode = Some(CsrfApplicationMode::Global);
            }
        }
        self
    }

    /// Installs one application-wide CSRF runtime without adding global
    /// enforcement middleware.
    ///
    /// Only routes declaring [`CsrfGuard`] are protected. A route-scoped
    /// policy with no such route, a `CsrfGuard` without this mode, or combining
    /// this registration with [`Self::csrf`] fails during [`Self::build`].
    /// Exact global bypass entries are not accepted in this mode because route
    /// guard selection is already the opt-in boundary.
    #[must_use]
    pub fn csrf_route_scoped(mut self, policy: CsrfPolicy) -> Self {
        match self.csrf_mode {
            Some(CsrfApplicationMode::RouteScoped) => self.duplicate_csrf_policy = true,
            Some(CsrfApplicationMode::Global) => self.conflicting_csrf_modes = true,
            None => {
                self.csrf_policy = Some(policy);
                self.csrf_mode = Some(CsrfApplicationMode::RouteScoped);
            }
        }
        self
    }

    /// Enable one immutable OpenAPI 3.1 document for this application.
    ///
    /// The generated document is attached to [`OpenApiService`] before this
    /// build can publish a listener. Lily does not register a JSON or UI route;
    /// applications may retain the service from a controller constructor or
    /// resolve it as an action `Service<OpenApiService>` parameter and expose
    /// it explicitly. The document is attached only after route materialization,
    /// so a constructor must not call `snapshot` or `json`; those reads belong
    /// in an action after [`Self::build`] has completed.
    #[must_use]
    pub fn openapi(mut self, config: OpenApiConfig) -> Self {
        self.openapi_config = Some(config);
        self
    }

    /// Supplies an application-defined guard instance to this composition
    /// root. Use this path when the guard needs policy or dependencies that
    /// cannot be obtained by its [`GuardTrait::new`] implementation. The
    /// framework-owned [`CsrfGuard`] is deliberately excluded because it must
    /// resolve this application's attached [`CsrfService`] during build. When
    /// the same concrete type is supplied repeatedly, the last instance is the
    /// application authority for that type.
    pub fn guard<G>(mut self, guard: G) -> Self
    where
        G: GuardTrait + Send + Sync + 'static,
    {
        if TypeId::of::<G>() == TypeId::of::<CsrfGuard>() {
            self.explicit_csrf_guard = true;
            return self;
        }
        self.guard_instances
            .insert(TypeId::of::<G>(), Arc::new(guard));
        self
    }

    /// Uses a caller-owned DI container.
    ///
    /// When omitted, `build` creates a dedicated container owned by the HTTP
    /// application and closes it during managed shutdown. Supplying one lets
    /// different application adapters share an explicit composition root; the
    /// caller retains shutdown authority and Lily will not close it.
    ///
    /// One container may host at most one HTTP [`App`] lifecycle because its
    /// injectable [`HttpHealthService`] is the single owner of that listener's
    /// health snapshot. After one successful HTTP build, another HTTP build
    /// with the same container therefore fails before it can publish a listener.
    pub fn container(mut self, container: Arc<ApplicationContainer>) -> Self {
        self.container = Some(container);
        self
    }

    /// Supplies this application's secret provider before configuration is
    /// loaded. Lily defines only the resolver contract; provider integration
    /// and credentials remain owned by the application.
    ///
    /// This cannot be combined with [`Self::container`]. When adopting a
    /// caller-owned container, seed its [`ConfigService`] while building that
    /// container instead.
    pub fn secret_resolver<R>(mut self, resolver: R) -> Self
    where
        R: SecretResolver + 'static,
    {
        self.secret_resolver = Some(Arc::new(resolver));
        self
    }

    /// Disables response localization and uses stable built-in public error
    /// messages. This is the default and performs no localization filesystem
    /// I/O or process-global lookup.
    pub fn localization_disabled(mut self) -> Self {
        self.localization_source = LocalizationSource::Disabled;
        self
    }

    /// Uses an already validated immutable catalog owned by this application.
    pub fn localization_catalog(mut self, catalog: impl Into<Arc<LocalizationCatalog>>) -> Self {
        self.localization_source = LocalizationSource::Catalog(catalog.into());
        self
    }

    /// Loads one immutable catalog snapshot from an explicit directory during
    /// `build`. Missing, empty, or malformed directories fail before DI or
    /// listener startup. Runtime reload is intentionally unsupported.
    pub fn localization_path(mut self, path: impl AsRef<Path>) -> Self {
        self.localization_source = LocalizationSource::Path(path.as_ref().to_path_buf());
        self
    }

    /// Disables adapter-owned tracing. This is the default and never probes the
    /// current working directory for a configuration file.
    ///
    /// Instrumentation sites still emit through Rust's tracing and
    /// OpenTelemetry globals; without an externally installed provider those
    /// emissions are no-ops.
    pub fn tracing_disabled(mut self) -> Self {
        self.tracing_mode = TracingMode::Disabled;
        self
    }

    /// Installs and owns a validated tracing runtime for this HTTP application.
    ///
    /// Installation occurs before eager DI initialization so services created
    /// during build bind their instruments to the selected provider. Managed
    /// shutdown flushes and stops the owned exporters.
    pub fn tracing_config(mut self, config: TraceConfig) -> Self {
        self.tracing_mode = TracingMode::owned_config(config);
        self
    }

    /// Strictly loads, installs, and owns tracing from an explicit path.
    ///
    /// Missing, malformed, or unsupported configuration fails
    /// [`Self::build`]; it never silently falls back to disabled tracing.
    pub fn tracing_config_path(mut self, path: impl AsRef<Path>) -> Self {
        self.tracing_mode = TracingMode::owned_path(path.as_ref().to_path_buf());
        self
    }

    /// Declares that a higher-level process composition root owns tracing.
    ///
    /// The external tracing and OpenTelemetry providers must be installed
    /// **before** [`Self::build`], because meters created during DI and HTTP
    /// initialization permanently bind to the provider active at that time.
    /// This application neither initializes nor shuts down those providers.
    pub fn tracing_external(mut self) -> Self {
        self.tracing_mode = TracingMode::External;
        self
    }

    /// Builds and freezes the application runtime graph.
    ///
    /// This initializes owned tracing, DI services, controllers, middleware,
    /// guards, policy plans, health, and optional OpenAPI state, but does not
    /// bind a socket. Any failure rolls back resources owned by this builder;
    /// resources in a caller-owned container remain the caller's authority.
    /// Consume the successful result with [`App::start`] or
    /// [`App::start_with_cancellation`] to run the managed lifecycle.
    pub async fn build(self) -> Result<App, AppBuildError> {
        debug!("Building HTTP application");

        let csrf_mode = self.csrf_mode;
        let openapi_config = self.openapi_config;

        if self.container.is_some() && self.secret_resolver.is_some() {
            return Err(AppBuildError::ConflictingContainerBootstrap);
        }

        let middleware_count = self
            .middlewares
            .len()
            .saturating_add(usize::from(self.session_middleware.is_some()))
            .saturating_add(usize::from(matches!(
                csrf_mode,
                Some(CsrfApplicationMode::Global)
            )));
        validate_middleware_count(middleware_count).map_err(|error| {
            AppBuildError::InvalidMiddlewareConfiguration { index: None, error }
        })?;
        if self.duplicate_session_middleware {
            return Err(AppBuildError::DuplicateSessionMiddleware);
        }
        if self.duplicate_cors_policy {
            return Err(AppBuildError::DuplicateCorsPolicy);
        }
        if self.conflicting_csrf_modes {
            return Err(AppBuildError::ConflictingCsrfModes);
        }
        if self.duplicate_csrf_policy {
            return Err(AppBuildError::DuplicateCsrfPolicy);
        }
        if self.explicit_csrf_guard {
            return Err(AppBuildError::ExplicitCsrfGuardUnsupported);
        }
        let cors_policy = self.cors_policy.as_ref();
        if let Some(policy) = cors_policy {
            policy
                .validate()
                .map_err(AppBuildError::InvalidCorsConfiguration)?;
        }
        if let Some(policy) = &self.csrf_policy {
            policy
                .validate()
                .map_err(AppBuildError::InvalidCsrfConfiguration)?;
        }

        let csrf_runtime = match (self.csrf_policy.as_ref(), csrf_mode) {
            (None, None) => None,
            (Some(policy), Some(CsrfApplicationMode::Global)) => Some(
                CompiledCsrfPolicy::try_new(policy)
                    .map_err(AppBuildError::InvalidCsrfConfiguration)?,
            ),
            (Some(policy), Some(CsrfApplicationMode::RouteScoped)) => Some(
                CompiledCsrfPolicy::try_new_route_scoped(policy)
                    .map_err(AppBuildError::InvalidCsrfConfiguration)?,
            ),
            _ => return Err(AppBuildError::CsrfInitialization),
        }
        .map(Arc::new);

        let localization_catalog = self.localization_source.resolve().await?;

        // Resolve an explicitly selected tracing source before any DI constructor
        // or listener work. Disabled/external modes perform no filesystem I/O.
        let trace_config = self
            .tracing_mode
            .resolve_owned_config()
            .map_err(|error| AppBuildError::InvalidTracingConfiguration(error.to_string()))?;

        debug!("HTTP application configuration loaded and validated");

        // Freeze the complete struct-controller inventory before creating owned runtime
        // resources. Live controller instances remain App-owned and are materialized only
        // after the application container is ready.
        let controller_registrations = crate::registry::get_struct_controller_registrations();
        let pending_controller_routes = crate::registry::get_pending_controller_routes();

        // OpenTelemetry meters bind permanently to the provider active when
        // they are created. Install an owned provider before eager DI service
        // initialization so queue/client instruments cannot become no-op
        // meters for the lifetime of the process.
        let tracing_owner = match trace_config {
            Some(config) => match TracingRuntimeOwner::install(&config) {
                Ok(TraceInstallOutcome::Disabled) => None,
                Ok(TraceInstallOutcome::Owned(owner)) => Some(owner),
                Err(error) => {
                    return Err(AppBuildError::TracingInitialization(error.to_string()));
                }
            },
            None => None,
        };

        let owns_container = self.container.is_none();
        let owns_tracing = tracing_owner.is_some();
        let mut build_guard = HttpBuildGuard::new(tracing_owner, owns_container);
        let container = match self.container {
            Some(container) => container,
            None => {
                let builder = match self.secret_resolver {
                    Some(resolver) => ApplicationContainer::builder()
                        .seed_singleton(ConfigService::with_shared_secret_resolver(resolver)),
                    None => ApplicationContainer::builder(),
                };
                let mut build =
                    lily_injection::__private::begin_application_container_build(builder);
                // DI rollback and outer telemetry share a source-anchored H.
                build.reserve_rollback_tail(5);
                build_guard.di_build = Some(build);
                let build = build_guard.di_build.as_mut().unwrap();
                match build.wait().await {
                    lily_injection::__private::ApplicationContainerBuildOutcome::Built(
                        container,
                    ) => Arc::new(container),
                    outcome => {
                        let error = match outcome {
                            lily_injection::__private::ApplicationContainerBuildOutcome::Failed(error) => error,
                            lily_injection::__private::ApplicationContainerBuildOutcome::Cancelled(Err(error)) => error,
                            _ => lily_error::injection::InjectionError::General("HTTP DI build cancelled".into()),
                        };
                        let budget = ShutdownBudget::from_started(
                            build.rollback_timeout(),
                            build
                                .rollback_started_at()
                                .unwrap_or_else(tokio::time::Instant::now),
                        );
                        let quiescent = build.rollback_quiescent();
                        return Err(rollback_tracing_only(
                            error.into(),
                            budget,
                            build_guard.handoff(),
                            quiescent,
                        )
                        .await);
                    }
                }
            }
        };
        build_guard.di_build = None;
        build_guard.container = Some(container.clone());
        let lily_config = match container.resolve::<ConfigService>(None).await {
            Ok(config_service) => config_service.get_lily_config().await,
            Err(error) => {
                return Err(rollback_failed_build(
                    error.into(),
                    &container,
                    owns_container,
                    DEFAULT_SHUTDOWN_TIMEOUT,
                    build_guard.handoff(),
                )
                .await);
            }
        };
        let shutdown_timeout = Duration::from_secs(lily_config.lifecycle.shutdown_timeout_secs);
        build_guard.timeout = shutdown_timeout;
        let effective_server = match EffectiveHttpServerConfig::resolve(
            &self.address_source,
            self.protocol,
            &self.transport_source,
            &self.tls_source,
            &lily_config.server,
        )
        .await
        {
            Ok(config) => config,
            Err(error) => {
                return Err(rollback_failed_build(
                    error,
                    &container,
                    owns_container,
                    shutdown_timeout,
                    build_guard.handoff(),
                )
                .await);
            }
        };

        let app_extensions = container.services();
        let (openapi_service, openapi_attachment) = if openapi_config.is_some() {
            let service = Arc::new(OpenApiService::new());
            let attachment = match lily_injection::__private::attach_framework_singleton(
                &app_extensions,
                Arc::clone(&service),
            ) {
                Ok(attachment) => attachment,
                Err(error) => {
                    return Err(rollback_failed_build(
                        error.into(),
                        &container,
                        owns_container,
                        shutdown_timeout,
                        build_guard.handoff(),
                    )
                    .await);
                }
            };
            (Some(service), Some(attachment))
        } else {
            (None, None)
        };
        let controllers = match crate::registry::materialize_controller_routes(
            controller_registrations,
            pending_controller_routes,
            Arc::clone(&app_extensions),
        )
        .await
        {
            Ok(routes) => routes,
            Err(error) => {
                return Err(rollback_failed_build(
                    error.into(),
                    &container,
                    owns_container,
                    shutdown_timeout,
                    build_guard.handoff(),
                )
                .await);
            }
        };
        if let Err(error) = validate_csrf_guard_contract(csrf_mode, controllers.routes()) {
            return Err(rollback_failed_build(
                error,
                &container,
                owns_container,
                shutdown_timeout,
                build_guard.handoff(),
            )
            .await);
        }

        let mut middleware_registrations = Vec::with_capacity(middleware_count);
        if let Some(session) = self.session_middleware {
            middleware_registrations.push(session);
        }
        if matches!(csrf_mode, Some(CsrfApplicationMode::Global)) {
            middleware_registrations
                .push(HttpMiddlewareRegistration::of::<CsrfEnforcementMiddleware>());
        }
        middleware_registrations.extend(self.middlewares);

        let mut initialized_middlewares = Vec::with_capacity(middleware_registrations.len());
        let mut middleware_descriptors = Vec::with_capacity(middleware_registrations.len());
        let mut application_middleware_types = Vec::with_capacity(middleware_registrations.len());
        for (index, registration) in middleware_registrations.into_iter().enumerate() {
            application_middleware_types.push(registration.type_id());
            let (middleware, descriptor) = match instantiate_registered_middleware(
                registration,
                Arc::clone(&app_extensions),
                index,
            )
            .await
            {
                Ok(initialized) => initialized,
                Err(error) => {
                    return Err(rollback_failed_build(
                        error,
                        &container,
                        owns_container,
                        shutdown_timeout,
                        build_guard.handoff(),
                    )
                    .await);
                }
            };
            initialized_middlewares.push(middleware);
            middleware_descriptors.push(descriptor);
        }

        if let Err(error) = validate_middleware_descriptors(&middleware_descriptors) {
            return Err(rollback_failed_build(
                AppBuildError::InvalidMiddlewareConfiguration { index: None, error },
                &container,
                owns_container,
                shutdown_timeout,
                build_guard.handoff(),
            )
            .await);
        }

        if let Some(runtime) = csrf_runtime {
            let csrf = match container.resolve::<CsrfService>(None).await {
                Ok(csrf) => csrf,
                Err(error) => {
                    return Err(rollback_failed_build(
                        error.into(),
                        &container,
                        owns_container,
                        shutdown_timeout,
                        build_guard.handoff(),
                    )
                    .await);
                }
            };
            if csrf.attach(runtime).is_err() {
                return Err(rollback_failed_build(
                    AppBuildError::CsrfInitialization,
                    &container,
                    owns_container,
                    shutdown_timeout,
                    build_guard.handoff(),
                )
                .await);
            }
        }

        let compiled_middlewares: Vec<CompiledHttpMiddleware> = initialized_middlewares
            .into_iter()
            .zip(middleware_descriptors)
            .map(|(middleware, descriptor)| CompiledHttpMiddleware::new(middleware, descriptor))
            .collect();
        let middleware_by_type: HashMap<_, _> = application_middleware_types
            .into_iter()
            .zip(compiled_middlewares.iter().cloned())
            .collect();

        // Step 6: Get registered guards
        let _guards = crate::guard::guard_registry::get_all_guards();

        // Build the authoritative runtime route table. OpenAPI factories are
        // never invoked for a disabled application.
        let route_build = if let Some(config) = openapi_config.as_ref() {
            controllers
                .into_openapi_route_table()
                .map_err(AppBuildError::from)
                .and_then(|(route_table, mut registry)| {
                    registry.apply_security_config(config)?;
                    let document = config
                        .build_document(registry.paths().clone(), registry.components().clone());
                    let prepared = OpenApiService::prepare(document)?;
                    Ok((route_table, Some(prepared)))
                })
        } else {
            controllers
                .into_route_table()
                .map(|route_table| (route_table, None))
                .map_err(AppBuildError::from)
        };
        let (route_table, openapi_document): (RouteTable, Option<OpenApiPreparedDocument>) =
            match route_build {
                Ok(route_build) => route_build,
                Err(error) => {
                    return Err(rollback_failed_build(
                        error,
                        &container,
                        owns_container,
                        shutdown_timeout,
                        build_guard.handoff(),
                    )
                    .await);
                }
            };
        let stats = route_table.stats();
        let cors_state = match build_cors_application_state(
            cors_policy,
            &route_table,
            Arc::clone(&app_extensions),
        )
        .await
        {
            Ok(state) => state,
            Err(error) => {
                return Err(rollback_failed_build(
                    error,
                    &container,
                    owns_container,
                    shutdown_timeout,
                    build_guard.handoff(),
                )
                .await);
            }
        };
        let route_middleware_plans = match build_route_middleware_plans(
            &route_table,
            &compiled_middlewares,
            middleware_by_type,
            Arc::clone(&app_extensions),
        )
        .await
        {
            Ok(plans) => plans,
            Err(error) => {
                return Err(rollback_failed_build(
                    error,
                    &container,
                    owns_container,
                    shutdown_timeout,
                    build_guard.handoff(),
                )
                .await);
            }
        };

        // Step 7: Instantiate only guards required by this immutable route
        // graph. Explicitly configured instances win over metadata factories.
        let required_guard_ids: HashSet<TypeId> = route_table
            .routes()
            .flat_map(|route| route.guard_type_ids.iter().copied())
            .collect();
        let mut guard_instances = self.guard_instances;
        let guard_metadata = crate::guard::guard_registry::get_all_guards();
        for guard_meta in guard_metadata {
            if !required_guard_ids.contains(&guard_meta.type_id)
                || guard_instances.contains_key(&guard_meta.type_id)
            {
                continue;
            }
            let guard_instance = match (guard_meta.factory_fn)(app_extensions.clone()).await {
                Ok(guard) => guard,
                Err(error) => {
                    return Err(rollback_failed_build(
                        error.into(),
                        &container,
                        owns_container,
                        shutdown_timeout,
                        build_guard.handoff(),
                    )
                    .await);
                }
            };
            guard_instances.insert(guard_meta.type_id, Arc::from(guard_instance));
            debug!(guard = guard_meta.type_name, "HTTP guard instantiated");
        }

        debug!(
            guard_count = guard_instances.len(),
            "HTTP guard inventory materialized"
        );

        let route_state = match AppRouteState::with_route_middlewares(
            route_table,
            guard_instances,
            route_middleware_plans,
        ) {
            Ok(route_state) => Arc::new(route_state),
            Err(error) => {
                return Err(rollback_failed_build(
                    error.into(),
                    &container,
                    owns_container,
                    shutdown_timeout,
                    build_guard.handoff(),
                )
                .await);
            }
        };
        if let (Some(service), Some(document)) = (openapi_service.as_ref(), openapi_document) {
            if let Err(error) = service.attach(document) {
                return Err(rollback_failed_build(
                    error.into(),
                    &container,
                    owns_container,
                    shutdown_timeout,
                    build_guard.handoff(),
                )
                .await);
            }
        }

        // Initialize metrics
        let meter = global::meter("lily_http_api");
        let route_lookup_duration = meter
            .f64_histogram("http.route.lookup.duration")
            .with_description("Route lookup duration")
            .with_unit("s")
            .build();
        let middleware_duration = meter
            .f64_histogram("http.middleware.duration")
            .with_description("Per-middleware execution duration")
            .with_unit("s")
            .build();
        let middleware_outcome_counter = meter
            .u64_counter("http.middleware.outcomes")
            .with_description("Per-middleware bounded execution outcomes")
            .build();
        let guard_duration = meter
            .f64_histogram("http.guard.duration")
            .with_description("Guard check duration")
            .with_unit("s")
            .build();
        let handler_duration = meter
            .f64_histogram("http.handler.duration")
            .with_description("Handler execution duration")
            .with_unit("s")
            .build();
        let route_match_counter = meter
            .u64_counter("http.route.matches.total")
            .with_description("Total route matches")
            .build();
        let route_miss_counter = meter
            .u64_counter("http.route.misses.total")
            .with_description("Total route misses (404s)")
            .build();
        let guard_outcome_counter = meter
            .u64_counter("http.server.guard.outcomes")
            .with_description("Authorization guard terminal outcomes")
            .build();
        let handler_outcome_counter = meter
            .u64_counter("http.server.handler.outcomes")
            .with_description("HTTP handler terminal outcomes")
            .build();

        debug!(
            total_routes = stats.total_routes,
            exact_routes = stats.exact_routes,
            param_routes = stats.param_routes,
            "[BUILD] APP: Application built successfully with route table"
        );

        let health = match container.resolve::<HttpHealthService>(None).await {
            Ok(health) => health,
            Err(error) => {
                return Err(rollback_failed_build(
                    error.into(),
                    &container,
                    owns_container,
                    shutdown_timeout,
                    build_guard.handoff(),
                )
                .await);
            }
        };
        if !self.background_services.is_empty() {
            let background = self.background_services.into_runtime(&container);
            build_guard.background = Some(background.clone());
            if let Err(error) = background.initialize().await {
                let dependencies =
                    HttpDependencies::new(container.clone(), owns_container, build_guard.handoff());
                dependencies.attach_background(background);
                return Err(run_build_rollback(
                    AppBuildError::BackgroundService(error),
                    dependencies,
                    ShutdownBudget::from_started(shutdown_timeout, tokio::time::Instant::now()),
                )
                .await);
            }
        }
        if let Err(error) = health.attach_http_lifecycle(owns_tracing) {
            let dependencies =
                HttpDependencies::new(container.clone(), owns_container, build_guard.handoff());
            if let Some(background) = build_guard.background.take() {
                dependencies.attach_background(background);
            }
            return Err(run_build_rollback(
                AppBuildError::MonitoringInitialization(error.to_string()),
                dependencies,
                ShutdownBudget::from_started(shutdown_timeout, tokio::time::Instant::now()),
            )
            .await);
        }
        if let Some(attachment) = openapi_attachment {
            attachment.commit();
        }
        let shutdown_state = health.shutdown_state();
        let dependencies =
            HttpDependencies::new(container.clone(), owns_container, build_guard.handoff());
        if let Some(background) = build_guard.background.take() {
            dependencies.attach_background(background);
        }
        Ok(App {
            effective_server: Arc::new(effective_server),
            localization_catalog,
            route_state,
            container,
            owns_container,
            shutdown_timeout,
            lifecycle: Arc::new(AppLifecycleState {
                root: std::sync::Mutex::new(None),
                root_tasks: TaskRegistry::default(),
                terminal: Arc::new(OnceLock::new()),
                pre_join_report: Arc::new(OnceLock::new()),
                framework_report: OnceLock::new(),
                root_join_observed: AtomicBool::new(false),
                tasks: HttpTaskInventory::default(),
                requests: RequestRegistry::default(),
                budget: ShutdownBudget::for_state(shutdown_timeout, shutdown_state.clone()),
                admission: CancellationToken::new(),
                force: CancellationToken::new(),
                bound_address: OnceLock::new(),
                dependencies,
                shutdown_state,
                health,
            }),
            middlewares: compiled_middlewares.into(),
            cors_state,
            route_lookup_duration,
            middleware_duration,
            middleware_outcome_counter,
            guard_duration,
            handler_duration,
            route_match_counter,
            route_miss_counter,
            guard_outcome_counter,
            handler_outcome_counter,
        })
    }
}

#[cfg(test)]
fn build_route_table(routes: Vec<RouteInfo>) -> Result<RouteTable, AppBuildError> {
    RouteTable::from_routes(routes).map_err(AppBuildError::from)
}

fn validate_csrf_guard_contract<'route>(
    mode: Option<CsrfApplicationMode>,
    routes: impl IntoIterator<Item = &'route RouteInfo>,
) -> Result<(), AppBuildError> {
    let csrf_guard = TypeId::of::<CsrfGuard>();
    let mut guarded_route_count = 0usize;
    for route in routes {
        let count = route
            .guard_type_ids
            .iter()
            .filter(|type_id| **type_id == csrf_guard)
            .count();
        if count > 1 {
            return Err(AppBuildError::DuplicateCsrfGuardOnRoute);
        }
        guarded_route_count = guarded_route_count.saturating_add(count);
    }

    match (mode, guarded_route_count) {
        (Some(CsrfApplicationMode::RouteScoped), 0) => {
            Err(AppBuildError::CsrfRouteScopedPolicyRequiresGuard)
        }
        (Some(CsrfApplicationMode::Global) | None, 1..) => {
            Err(AppBuildError::CsrfGuardRequiresRouteScopedPolicy)
        }
        _ => Ok(()),
    }
}

type GuardInstance = Arc<dyn GuardTrait + Send + Sync>;

fn missing_route_guard_http_error(error: &MissingRouteGuardError) -> HttpApiError {
    error!(
        error = %error,
        "[RUNTIME] APP: Required route guard is unavailable; request denied"
    );
    HttpApiError::InitializationError("required HTTP route guard is unavailable".to_string())
}

async fn write_guard_rejection(
    response: &mut Response,
    rejection: crate::guard::GuardRejection,
) -> Result<(), ResponseWriteError> {
    rejection.write_to_response(response).await
}

fn guard_observation_outcome(
    guard_type_id: TypeId,
    result: &Result<(), crate::guard::GuardRejection>,
) -> &'static str {
    if guard_type_id == TypeId::of::<CsrfGuard>() {
        return match result {
            Ok(()) => "completed",
            Err(rejection) if rejection.status() >= 500 => "internal",
            Err(_) => "rejected",
        };
    }
    match result {
        Ok(()) => "allowed",
        Err(rejection) => http_status_outcome(rejection.status()),
    }
}

/// Immutable router and guard graph shared by every clone of one application.
struct AppRouteState {
    route_table: RouteTable,
    guard_instances: HashMap<TypeId, GuardInstance>,
    middleware_plans: Vec<Arc<[CompiledHttpMiddleware]>>,
}

impl AppRouteState {
    #[cfg(any(test, feature = "fuzzing"))]
    fn new(
        route_table: RouteTable,
        guard_instances: HashMap<TypeId, GuardInstance>,
    ) -> Result<Self, MissingRouteGuardError> {
        let middleware_plans =
            vec![Arc::<[CompiledHttpMiddleware]>::from([]); route_table.route_count()];
        Self::with_route_middlewares(route_table, guard_instances, middleware_plans)
    }

    fn with_route_middlewares(
        route_table: RouteTable,
        guard_instances: HashMap<TypeId, GuardInstance>,
        middleware_plans: Vec<Arc<[CompiledHttpMiddleware]>>,
    ) -> Result<Self, MissingRouteGuardError> {
        let state = Self {
            route_table,
            guard_instances,
            middleware_plans,
        };

        for route in state.route_table.routes() {
            for guard_type_id in &route.guard_type_ids {
                state.guard_for_route(route, guard_type_id)?;
            }
        }

        Ok(state)
    }

    fn middleware_for_route(&self, route: &RouteInfo) -> &[CompiledHttpMiddleware] {
        &self.middleware_plans[route.route_plan_id]
    }

    fn guard_for_route(
        &self,
        route: &RouteInfo,
        guard_type_id: &TypeId,
    ) -> Result<&GuardInstance, MissingRouteGuardError> {
        self.guard_instances
            .get(guard_type_id)
            .ok_or_else(|| MissingRouteGuardError {
                method: route.method.clone(),
                path: route.path.clone(),
                handler: route.handler_name.clone(),
                guard_type_id: *guard_type_id,
            })
    }
}

/// HTTP Application with centralized routing and dependency injection
struct AppLifecycleState {
    root: std::sync::Mutex<Option<TaskReceipt<HttpLifecycleOutcome>>>,
    root_tasks: TaskRegistry,
    terminal: Arc<OnceLock<HttpFrozenAttempt>>,
    pre_join_report: Arc<OnceLock<crate::shutdown_report::HttpShutdownReport>>,
    framework_report: OnceLock<FrameworkSnapshot>,
    root_join_observed: AtomicBool,
    tasks: HttpTaskInventory,
    requests: RequestRegistry,
    budget: ShutdownBudget,
    admission: CancellationToken,
    force: CancellationToken,
    bound_address: OnceLock<SocketAddr>,
    dependencies: Arc<HttpDependencies>,
    shutdown_state: Arc<ShutdownState>,
    health: Arc<HttpHealthService>,
}

impl AppLifecycleState {
    fn begin_shutdown(&self) {
        self.requests.close_admission();
        let _ = self
            .shutdown_state
            .initiate_shutdown(ShutdownSignal::Manual);
        self.budget.begin();
        self.admission.cancel();
        self.dependencies.begin_background_shutdown(&self.budget);
        if let Some(background) = self.dependencies.background.get() {
            self.dependencies.background_force_bridge.get_or_init(|| {
                let background = background.clone();
                let budget = self.budget.clone();
                let force = self.force.clone();
                self.tasks.monitors.spawn(async move {
                    tokio::select! {
                        biased;
                        _ = budget.wait_for_force() => background.force_stop(),
                        _ = force.cancelled() => background.force_stop(),
                        _ = background.wait_stopped_before(budget.begin().at(ShutdownStage::Reconcile)) => {},
                    }
                    Ok::<(), io::Error>(())
                })
            });
        }
    }
}

impl Drop for AppLifecycleState {
    fn drop(&mut self) {
        // A built-but-never-started App may be abandoned. Prepared worker
        // tasks still own dependencies, so hand them to the retained rollback
        // path before the last application/container handles can disappear.
        if self
            .root
            .get_mut()
            .unwrap_or_else(|p| p.into_inner())
            .is_some()
        {
            return;
        }
        let Some(background) = self.dependencies.background.get() else {
            return;
        };
        if background.snapshot().is_terminal() {
            return;
        }
        self.dependencies.retain();
        let _ = self
            .shutdown_state
            .initiate_shutdown(ShutdownSignal::Manual);
        self.dependencies.begin_background_shutdown(&self.budget);
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let _entered = runtime.enter();
            BUILD_ROLLBACK_TASKS
                .get_or_init(TaskRegistry::default)
                .spawn(close_build_dependencies(
                    self.dependencies.clone(),
                    self.budget.clone(),
                ));
        } else {
            tracing::error!(
                "Background shutdown dependencies retained: no available Tokio runtime"
            );
        }
    }
}

#[derive(Debug, Clone)]
enum HttpLifecycleOutcome {
    Completed,
    Failed {
        kind: io::ErrorKind,
        message: String,
    },
}

impl HttpLifecycleOutcome {
    fn from_result(result: io::Result<()>) -> Self {
        match result {
            Ok(()) => Self::Completed,
            Err(error) => Self::Failed {
                kind: error.kind(),
                message: error.to_string(),
            },
        }
    }

    fn as_result(&self) -> io::Result<()> {
        match self {
            Self::Completed => Ok(()),
            Self::Failed { kind, message } => Err(io::Error::new(*kind, message.clone())),
        }
    }
}

enum HttpRootMode {
    Signals,
    Cancellation(CancellationToken),
    Close,
}

struct HttpStartWaiter {
    lifecycle: Arc<AppLifecycleState>,
    armed: bool,
}

impl Drop for HttpStartWaiter {
    fn drop(&mut self) {
        if self.armed {
            self.lifecycle.begin_shutdown();
        }
    }
}

struct HttpLifecycleTrigger {
    state: Arc<ShutdownState>,
    budget: ShutdownBudget,
    task: Option<TaskReceipt<io::Result<()>>>,
}

impl HttpLifecycleTrigger {
    fn cancellation(lifecycle: &Arc<AppLifecycleState>, cancellation: CancellationToken) -> Self {
        if cancellation.is_cancelled() {
            lifecycle.begin_shutdown();
            return Self::passive(lifecycle);
        }

        let owner = lifecycle.clone();
        let task = lifecycle.tasks.monitors.spawn(async move {
            cancellation.cancelled().await;
            owner.begin_shutdown();
            Ok(())
        });
        let mut trigger = Self::passive(lifecycle);
        trigger.task = Some(task);
        trigger
    }

    fn passive(lifecycle: &AppLifecycleState) -> Self {
        Self {
            state: lifecycle.shutdown_state.clone(),
            budget: lifecycle.budget.clone(),
            task: None,
        }
    }

    fn signals(lifecycle: &AppLifecycleState, monitor: SignalMonitor) -> Self {
        let mut trigger = Self::passive(lifecycle);
        trigger.task = monitor
            .into_task()
            .map(|task| lifecycle.tasks.monitors.adopt(task));
        trigger
    }

    async fn wait_for_first(&mut self) -> io::Result<ShutdownSignal> {
        let mut receiver = self.state.subscribe();
        let result = tokio::select! {
            biased;
            signal = receiver.recv() => signal.map_err(|error| {
                io::Error::new(io::ErrorKind::BrokenPipe, error)
            }),
            result = async {
                match &self.task {
                    Some(task) => task.clone().await,
                    None => std::future::pending().await,
                }
            } => match result {
                Ok(result) => match result.as_ref() {
                    Ok(()) => self.state.initial_signal().ok_or_else(|| {
                        io::Error::other("HTTP lifecycle monitor stopped without a signal")
                    }),
                    Err(error) => Err(io::Error::new(error.kind(), error.to_string())),
                },
                Err(error) => Err(io::Error::other(format!("HTTP lifecycle monitor failed: {error}"))),
            },
        };
        self.budget.begin();
        result
    }

    async fn stop(&self) -> io::Result<()> {
        let Some(task) = &self.task else {
            return Ok(());
        };
        task.abort();
        match self
            .budget
            .wait_for_receipt(ShutdownStage::Reconcile, task.clone())
            .await
        {
            Ok(Ok(result)) => result
                .as_ref()
                .as_ref()
                .map(|_| ())
                .map_err(|error| io::Error::new(error.kind(), error.to_string())),
            Ok(Err(error)) if error.is_cancelled() => Ok(()),
            Ok(Err(error)) => Err(io::Error::other(format!(
                "HTTP lifecycle monitor failed: {error}"
            ))),
            Err(()) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "HTTP lifecycle monitor join is outstanding",
            )),
        }
    }
}

impl Drop for HttpLifecycleTrigger {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

struct HttpServerLifecycleHandle {
    phase: FrameworkShutdownPhase,
    server: Arc<tokio::sync::Mutex<ManagedHttpServer>>,
    admission: tokio_util::sync::CancellationToken,
    force: tokio_util::sync::CancellationToken,
    timeout: Duration,
    budget: ShutdownBudget,
    tasks: HttpTaskInventory,
    requests: RequestRegistry,
}

impl HttpServerLifecycleHandle {
    async fn wait_for_drain(&self, accept_forced: bool) -> Result<(), ShutdownError> {
        let stage = if accept_forced {
            ShutdownStage::Reconcile
        } else {
            ShutdownStage::Graceful
        };
        let report = self
            .budget
            .wait_for_receipt(stage, async { self.server.lock().await.wait().await })
            .await
            .map_err(|()| {
                ShutdownError::Component(
                    "HTTP transport joins remain outstanding at the phase cutoff".into(),
                )
            })?
            .map_err(|error| ShutdownError::Component(error.to_string()))?;
        HttpServer::log_drain_report(&report);

        if !self.tasks.transport_is_terminal() {
            return Err(ShutdownError::Component(
                "HTTP transport producers are still active".into(),
            ));
        }
        self.requests.seal();
        let _requests = self
            .budget
            .wait_for_receipt(stage, self.requests.wait())
            .await
            .map_err(|()| {
                ShutdownError::Component("HTTP request owner joins remain outstanding".into())
            })?;

        let accounting_is_complete = if accept_forced {
            report.shutdown_is_terminal()
        } else {
            report.shutdown_is_clean()
        };
        let requests = self.requests.attempt_snapshot();
        if accounting_is_complete
            && self.tasks.transport_is_terminal()
            && !self.tasks.transport_panicked()
            && requests.is_terminal()
            && requests.cleanup_failed == 0
            && requests.owner_failed == 0
        {
            Ok(())
        } else {
            let expected_state = if accept_forced { "terminal" } else { "clean" };
            Err(ShutdownError::Component(format!(
                "HTTP task accounting was not {expected_state}: {report:?}"
            )))
        }
    }
}

#[async_trait::async_trait]
impl FrameworkShutdownComponent for HttpServerLifecycleHandle {
    async fn shutdown(&mut self) -> Result<(), ShutdownError> {
        match self.phase {
            FrameworkShutdownPhase::StopAdmission => {
                self.requests.close_admission();
                self.admission.cancel();
                Ok(())
            }
            FrameworkShutdownPhase::DrainInFlight => self.wait_for_drain(false).await,
            _ => unreachable!("HTTP lifecycle handle registered in an invalid phase"),
        }
    }

    fn name(&self) -> &str {
        match self.phase {
            FrameworkShutdownPhase::StopAdmission => "http-connection-admission",
            FrameworkShutdownPhase::DrainInFlight => "http-connection-drain",
            _ => "invalid-http-lifecycle-phase",
        }
    }

    fn phase(&self) -> FrameworkShutdownPhase {
        self.phase
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }

    fn force_shutdown(&mut self) -> Option<lily_shutdown::FrameworkForceShutdownFuture<'_>> {
        let reason = if !self.budget.force_requested()
            && tokio::time::Instant::now() >= self.budget.begin().at(ShutdownStage::Graceful)
        {
            crate::lifecycle::ExecutionStopReason::GracefulDeadline
        } else {
            crate::lifecycle::ExecutionStopReason::ForcedShutdown
        };
        self.requests.cancel_executions(reason);
        self.admission.cancel();
        self.force.cancel();
        Some(Box::pin(async move {
            match self.phase {
                FrameworkShutdownPhase::StopAdmission => Ok(()),
                // The coordinator retains the graceful timeout as the primary
                // status; this future only proves forced teardown terminated.
                FrameworkShutdownPhase::DrainInFlight => self.wait_for_drain(true).await,
                _ => unreachable!("HTTP lifecycle handle registered in an invalid phase"),
            }
        }))
    }

    fn force_deadline(&self) -> Option<tokio::time::Instant> {
        Some(self.budget.begin().at(ShutdownStage::Reconcile))
    }
}

/// Built HTTP application and owner of one managed listener lifecycle.
///
/// The controller, middleware, guard, route, policy, localization, health, and
/// server configuration graphs are immutable after build. Clones share those
/// graphs and the same lifecycle root; at most one clone can enter
/// [`Self::start`] or [`Self::start_with_cancellation`]. [`Self::close`] can also
/// install that root for an unstarted application without binding a listener.
/// Start and close waiters observe the same bounded shutdown attempt.
#[must_use = "a built HTTP application must be started or asynchronously closed"]
pub struct App {
    effective_server: Arc<EffectiveHttpServerConfig>,
    localization_catalog: Option<Arc<LocalizationCatalog>>,
    route_state: Arc<AppRouteState>,
    container: Arc<ApplicationContainer>,
    owns_container: bool,
    shutdown_timeout: Duration,
    lifecycle: Arc<AppLifecycleState>,
    middlewares: Arc<[CompiledHttpMiddleware]>,
    cors_state: Option<Arc<CorsApplicationState>>,
    // Metrics for request processing
    route_lookup_duration: Histogram<f64>,
    middleware_duration: Histogram<f64>,
    middleware_outcome_counter: Counter<u64>,
    guard_duration: Histogram<f64>,
    handler_duration: Histogram<f64>,
    route_match_counter: Counter<u64>,
    route_miss_counter: Counter<u64>,
    guard_outcome_counter: Counter<u64>,
    handler_outcome_counter: Counter<u64>,
}

impl Clone for App {
    fn clone(&self) -> Self {
        Self {
            effective_server: Arc::clone(&self.effective_server),
            localization_catalog: self.localization_catalog.clone(),
            route_state: Arc::clone(&self.route_state),
            container: Arc::clone(&self.container),
            owns_container: self.owns_container,
            shutdown_timeout: self.shutdown_timeout,
            lifecycle: Arc::clone(&self.lifecycle),
            middlewares: self.middlewares.clone(),
            cors_state: self.cors_state.clone(),
            route_lookup_duration: self.route_lookup_duration.clone(),
            middleware_duration: self.middleware_duration.clone(),
            middleware_outcome_counter: self.middleware_outcome_counter.clone(),
            guard_duration: self.guard_duration.clone(),
            handler_duration: self.handler_duration.clone(),
            route_match_counter: self.route_match_counter.clone(),
            route_miss_counter: self.route_miss_counter.clone(),
            guard_outcome_counter: self.guard_outcome_counter.clone(),
            handler_outcome_counter: self.handler_outcome_counter.clone(),
        }
    }
}

impl App {
    async fn finish_failed_start(
        original: io::Error,
        lifecycle: &Arc<AppLifecycleState>,
    ) -> io::Error {
        let kind = original.kind();
        let mut failures = vec![original.to_string()];
        lifecycle.begin_shutdown();
        if let Err(error) = lifecycle.health.record_listener_bind_failure() {
            failures.push(format!(
                "HTTP listener health publication also failed: {error}"
            ));
        }
        failures.extend(Self::close_failed_root_dependencies(lifecycle).await);
        io::Error::new(kind, failures.join("; "))
    }

    async fn close_failed_root_dependencies(lifecycle: &Arc<AppLifecycleState>) -> Vec<String> {
        let mut failures = Vec::new();
        if let Err(error) = lifecycle
            .dependencies
            .close_background(&lifecycle.budget)
            .await
        {
            failures.push(error.to_string());
        }
        if let Some(bridge) = lifecycle.dependencies.background_force_bridge.get() {
            let _ = lifecycle
                .budget
                .wait_for_receipt(ShutdownStage::Reconcile, bridge.clone())
                .await;
        }
        // Failed start and panic recovery share the normal root obligations.
        lifecycle.budget.begin();
        for phase in [
            FrameworkShutdownPhase::DisposeDependencies,
            FrameworkShutdownPhase::FlushTelemetry,
        ] {
            let mut handle = HttpDependencyHandle {
                lifecycle: lifecycle.clone(),
                phase,
                timeout: Duration::ZERO,
            };
            if let Err(error) = handle.shutdown().await {
                failures.push(error.to_string());
            }
        }
        if let Err(error) = lifecycle.dependencies.reconcile(&lifecycle.budget).await {
            failures.push(error.to_string());
        }
        failures
    }

    /// Returns the DI composition root used by this HTTP application.
    ///
    /// The application owns and closes this container only when it was created
    /// by [`AppBuilder::build`]. A container supplied through
    /// [`AppBuilder::container`] remains caller-owned.
    pub fn container(&self) -> &Arc<ApplicationContainer> {
        &self.container
    }

    /// Returns a read-only service-provider handle for application adapters.
    ///
    /// Controllers normally receive this handle during construction and typed
    /// actions resolve request-scoped services with `Service<T>`.
    pub fn extensions(&self) -> Arc<Extensions> {
        self.container.services()
    }

    /// Returns the effective HTTP protocol policy frozen during build.
    pub fn protocol(&self) -> HttpProtocol {
        self.effective_server.protocol()
    }

    /// Returns the effective listener address frozen during build.
    pub fn listen_address(&self) -> &str {
        self.effective_server.listen_address()
    }

    /// Returns the socket address selected by the operating system after the
    /// managed listener binds.
    ///
    /// This is `None` before a successful bind. It is published once and shared
    /// by every [`App`] clone without changing the immutable configured address
    /// returned by [`Self::listen_address`].
    pub fn bound_address(&self) -> Option<SocketAddr> {
        self.lifecycle.bound_address.get().copied()
    }

    /// Returns the complete immutable listener snapshot used by the runtime.
    pub fn effective_server_config(&self) -> &EffectiveHttpServerConfig {
        &self.effective_server
    }

    /// Returns the effective bounded transport configuration.
    pub fn transport_config(&self) -> &HttpTransportConfig {
        self.effective_server.transport()
    }

    /// Private transport handoff for all immutable compiled CORS plans.
    pub(crate) fn cors_adapters(&self) -> Option<&[Arc<CorsLayerAdapter>]> {
        self.cors_state.as_ref().map(|state| state.adapters())
    }

    pub(crate) fn cors_fallback_plan_id(&self) -> Option<usize> {
        self.cors_state.as_ref().map(|state| state.fallback_plan_id)
    }

    pub(crate) fn cors_plan_id(&self, method: &str, path: &str) -> Option<usize> {
        let state = self.cors_state.as_ref()?;
        if state.route_plan_ids.is_none() {
            return Some(state.fallback_plan_id);
        }
        Some(match self.route_state.route_table.resolve(method, path) {
            RouteResolution::Found(route) => state.plan_for_route(route),
            RouteResolution::MethodNotAllowed { .. } | RouteResolution::NotFound => {
                state.fallback_plan_id
            }
        })
    }

    pub(crate) fn cors_adapter(&self, plan_id: usize) -> Option<&Arc<CorsLayerAdapter>> {
        self.cors_state.as_ref().map(|state| state.adapter(plan_id))
    }

    #[cfg(test)]
    pub(crate) async fn replace_routes_and_cors_for_test(
        &mut self,
        routes: Vec<RouteInfo>,
        global_policy: Option<&CorsPolicy>,
    ) -> Result<(), AppBuildError> {
        let route_table = build_route_table(routes)?;
        let cors_state =
            build_cors_application_state(global_policy, &route_table, self.container.services())
                .await?;
        self.route_state = Arc::new(AppRouteState::new(route_table, HashMap::new())?);
        self.cors_state = cors_state;
        Ok(())
    }

    /// Returns the immutable TLS configuration selected during build.
    ///
    /// `None` means Lily publishes a plaintext listener; TLS terminated by an
    /// upstream proxy is outside this listener's authority.
    pub fn rustls_config(&self) -> Option<&RustlsConfig> {
        self.effective_server.rustls_config()
    }

    /// Returns this application's immutable localization snapshot.
    ///
    /// `None` means framework errors use their stable built-in public messages.
    pub fn localization_catalog(&self) -> Option<&LocalizationCatalog> {
        self.localization_catalog.as_deref()
    }

    /// Returns bounded statistics for the immutable route table.
    pub fn route_stats(&self) -> crate::route_table::RouteTableStats {
        self.route_state.route_table.stats()
    }

    /// Returns the number of live guard singletons required by the route graph.
    pub fn guard_count(&self) -> usize {
        self.route_state.guard_instances.len()
    }

    pub(crate) fn task_inventory(&self) -> &HttpTaskInventory {
        &self.lifecycle.tasks
    }

    pub(crate) fn request_registry(&self) -> &RequestRegistry {
        &self.lifecycle.requests
    }

    pub(crate) fn request_admission_stopping(&self) -> bool {
        self.lifecycle.admission.is_cancelled()
            || self.lifecycle.force.is_cancelled()
            || self.lifecycle.budget.force_requested()
            || self.lifecycle.shutdown_state.is_shutdown_initiated()
            || self.lifecycle.budget.deadlines().is_some()
    }

    pub(crate) fn shutdown_budget(&self) -> &ShutdownBudget {
        &self.lifecycle.budget
    }

    pub(crate) fn transport_shutdown(&self) -> CancellationToken {
        self.lifecycle.admission.clone()
    }

    pub(crate) fn transport_force(&self) -> CancellationToken {
        self.lifecycle.force.clone()
    }

    fn install_root(
        &self,
        mode: HttpRootMode,
        require_new: bool,
    ) -> io::Result<TaskReceipt<HttpLifecycleOutcome>> {
        // Claim and receipt publication are synchronous under the same lock.
        // Concurrent close cannot observe a claimed-but-unregistered root.
        let mut root = self
            .lifecycle
            .root
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(root) = root.as_ref() {
            return if require_new {
                Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "this HTTP application lifecycle has already been started or closed",
                ))
            } else {
                Ok(root.clone())
            };
        }
        let runtime = self.clone();
        let receipt = self.lifecycle.root_tasks.spawn(async move {
            let result = AssertUnwindSafe(runtime.run_owned_root(mode))
                .catch_unwind()
                .await;
            let mut result = match result {
                Ok(result) => result,
                Err(_) => {
                    let recovery = runtime.recover_transport_after_root_panic().await;
                    runtime.lifecycle.tasks.monitors.seal();
                    let monitors = runtime.lifecycle.budget.wait_for_receipt(
                        ShutdownStage::Reconcile,
                        runtime.lifecycle.tasks.monitors.wait(),
                    ).await;
                    let dependencies = Self::close_failed_root_dependencies(
                        &runtime.lifecycle).await;
                    let _ = runtime.lifecycle.health.record_listener_failure();
                    Err(io::Error::other(format!(
                        "HTTP lifecycle root panicked; transport recovery: {recovery:?}; monitor recovery: {monitors:?}; dependency cleanup: {dependencies:?}"
                    )))
                }
            };
            if runtime
                .lifecycle
                .budget
                .deadlines()
                .is_some_and(|deadlines| {
                    tokio::time::Instant::now() > deadlines.at(ShutdownStage::Final)
                })
                && result.is_ok()
            {
                result = Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "HTTP lifecycle root completed after its absolute shutdown deadline",
                ));
            }
            let _ = runtime.lifecycle.pre_join_report.set(runtime.lifecycle.observe_report(
                crate::tasks::TaskSnapshot { registered: 1, outstanding: 1, ..Default::default() },
                result.is_ok(),
            ));
            HttpLifecycleOutcome::from_result(result)
        });
        *root = Some(receipt.clone());
        self.lifecycle.install_report_observer(receipt.clone());
        Ok(receipt)
    }

    async fn await_root(&self, receipt: TaskReceipt<HttpLifecycleOutcome>) -> io::Result<()> {
        if let Some(terminal) = self.lifecycle.terminal.get() {
            // Reap a late join without revising the frozen attempt result.
            if receipt.clone().now_or_never().is_some() {
                self.lifecycle
                    .root_join_observed
                    .store(true, Ordering::Release);
            }
            // A late child release may make the retained finalizer safe to
            // retire. Observe it under the ORIGINAL cutoffs, without replacing
            // this outcome or starting another dependency-close attempt.
            let _ = self.reconcile_transport().await;
            let _ = self
                .lifecycle
                .dependencies
                .reconcile(&self.lifecycle.budget)
                .await;
            return terminal.as_result();
        }
        let observation_receipt = receipt.clone();
        let observed = {
            let join = self
                .lifecycle
                .budget
                .wait_for_receipt(ShutdownStage::Final, receipt);
            tokio::pin!(join);
            let mut stopping = self.lifecycle.shutdown_state.subscribe();
            // Also observe the durable native signal outside the root worker.
            // A stalled root must not delay installation of its own deadline.
            tokio::select! {
                biased;
                signal = stopping.recv() => {
                    if signal.is_ok() { self.lifecycle.begin_shutdown(); }
                    join.await
                },
                result = &mut join => result,
            }
        };
        let root_evidence = if observed.is_ok() {
            observation_receipt.snapshot()
        } else {
            // The wait timed out. Do not replace this fact with a join that
            // becomes ready while assembling the remainder of the report.
            crate::tasks::TaskSnapshot {
                registered: 1,
                outstanding: 1,
                ..Default::default()
            }
        };
        let outcome = match observed {
            Ok(Ok(outcome)) => {
                self.lifecycle
                    .root_join_observed
                    .store(true, Ordering::Release);
                outcome.as_ref().clone()
            }
            Ok(Err(error)) => {
                self.lifecycle
                    .root_join_observed
                    .store(true, Ordering::Release);
                HttpLifecycleOutcome::from_result(Err(io::Error::other(format!(
                    "HTTP lifecycle root join failed: {error}"
                ))))
            }
            Err(()) => HttpLifecycleOutcome::from_result(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "HTTP lifecycle root join remains outstanding at the absolute shutdown deadline",
            ))),
        };
        self.lifecycle
            .freeze_attempt(outcome, root_evidence)
            .as_result()
    }

    async fn run_owned_root(&self, mode: HttpRootMode) -> io::Result<()> {
        let trigger = match mode {
            HttpRootMode::Signals => {
                match SignalHandler::install(self.lifecycle.shutdown_state.clone()).await {
                    Ok(monitor) => HttpLifecycleTrigger::signals(&self.lifecycle, monitor),
                    Err(error) => {
                        return Err(Self::finish_failed_start(error, &self.lifecycle).await)
                    }
                }
            }
            HttpRootMode::Cancellation(token) => {
                HttpLifecycleTrigger::cancellation(&self.lifecycle, token)
            }
            HttpRootMode::Close => HttpLifecycleTrigger::passive(&self.lifecycle),
        };
        self.run_http_lifecycle(trigger).await
    }

    async fn recover_transport_after_root_panic(&self) -> io::Result<()> {
        self.lifecycle.begin_shutdown();
        self.lifecycle
            .requests
            .cancel_executions(crate::lifecycle::ExecutionStopReason::ForcedShutdown);
        self.lifecycle.force.cancel();
        if let Some(background) = self.lifecycle.dependencies.background.get() {
            background.force_stop();
        }
        self.lifecycle.tasks.monitors.abort_all();
        // A root panic does not authorize cutting accepted response work before
        // its cooperative/finalization window. Actual owners remain retained.
        let transport = &self.lifecycle.tasks;
        let drained = self
            .lifecycle
            .budget
            .wait_for_receipt(ShutdownStage::TransportStop, async {
                // Producer joins precede observing the next inventory empty.
                transport.listener.wait().await;
                transport.connections.wait().await;
                transport.protocol.wait().await;
            })
            .await;
        if drained.is_err() {
            transport.abort_transport();
        }
        // Cancellation requests above are not join evidence. The original R
        // reserve and exact scope receipts still guard parent disposal.
        self.reconcile_transport().await
    }

    async fn reconcile_transport(&self) -> io::Result<()> {
        let tasks = &self.lifecycle.tasks;
        // Joining the producer before sealing its children closes the empty
        // snapshot / late registration race. Timeout keeps every receipt.
        for registry in [&tasks.listener, &tasks.connections, &tasks.protocol] {
            registry.seal();
            let snapshot = self
                .lifecycle
                .budget
                .wait_for_receipt(ShutdownStage::Reconcile, registry.wait())
                .await;
            if snapshot.is_err() || !registry.snapshot().is_terminal() {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "HTTP transport task inventory is outstanding",
                ));
            }
        }
        // All request producers have joined; now seal and reconcile the owners
        // that survive their service/connection waiters. Never abort this ledger.
        self.lifecycle.requests.seal();
        let requests = self
            .lifecycle
            .budget
            .wait_for_receipt(ShutdownStage::Reconcile, self.lifecycle.requests.wait())
            .await;
        if requests.is_err() || !self.lifecycle.requests.snapshot().is_terminal() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "HTTP request owners or exact scope receipts remain outstanding",
            ));
        }
        let requests = self.lifecycle.requests.attempt_snapshot();
        if requests.cleanup_failed != 0 || requests.owner_failed != 0 {
            return Err(io::Error::other(format!(
                "HTTP request lifecycle cleanup failed: {requests:?}"
            )));
        }
        if tasks.transport_panicked() {
            return Err(io::Error::other(
                "HTTP transport tasks terminated with a panic",
            ));
        }
        Ok(())
    }

    async fn reconcile_monitors(&self) -> io::Result<()> {
        let monitors = &self.lifecycle.tasks.monitors;
        monitors.seal();
        monitors.abort_all();
        let result = self
            .lifecycle
            .budget
            .wait_for_receipt(ShutdownStage::Reconcile, monitors.wait())
            .await;
        if result.is_err() || !monitors.snapshot().is_terminal() {
            return Err(io::Error::other("HTTP monitor join is outstanding"));
        }
        if monitors.snapshot().panicked != 0 {
            return Err(io::Error::other("HTTP monitor joined with a panic"));
        }
        Ok(())
    }

    /// Starts the listener with adapter-owned platform signal handling.
    ///
    /// The method consumes this application, accepts only one start across all
    /// clones, and observes the retained root's actual join. After its first
    /// poll registers the root, dropping this waiter requests shutdown; the
    /// canonical root continues owning cleanup.
    /// The result is replayed by [`Self::close`], including incomplete shutdown.
    pub async fn start(self) -> io::Result<()> {
        let root = self.install_root(HttpRootMode::Signals, true)?;
        let mut waiter = HttpStartWaiter {
            lifecycle: self.lifecycle.clone(),
            armed: true,
        };
        let result = self.await_root(root).await;
        waiter.armed = false;
        result
    }

    /// Starts the HTTP server with a caller-owned cancellation source and no
    /// platform signal handler. Cancellation enters the same bounded lifecycle
    /// as a manual shutdown request.
    ///
    /// The method consumes this application and waits for the complete shutdown
    /// result. After its first poll registers the root, dropping the waiter
    /// requests shutdown without destroying the root; another App clone can
    /// call [`Self::close`] to observe its join.
    pub async fn start_with_cancellation(self, cancellation: CancellationToken) -> io::Result<()> {
        let root = self.install_root(HttpRootMode::Cancellation(cancellation), true)?;
        let mut waiter = HttpStartWaiter {
            lifecycle: self.lifecycle.clone(),
            armed: true,
        };
        let result = self.await_root(root).await;
        waiter.armed = false;
        result
    }

    /// Requests shutdown and observes the same retained lifecycle root as start.
    ///
    /// A built but unstarted App closes without binding a listener. Concurrent
    /// or repeated calls share one root, one absolute deadline and one result.
    /// Once first polled, dropping a close waiter does not cancel cleanup.
    /// As with start, an entirely unpolled future does not install a root.
    /// Caller-owned DI and
    /// external tracing remain caller-owned. Outstanding work is an error,
    /// never confirmation that cleanup succeeded.
    pub async fn close(&self) -> io::Result<()> {
        self.lifecycle.begin_shutdown();
        let root = self.install_root(HttpRootMode::Close, false)?;
        self.await_root(root).await
    }

    async fn run_http_lifecycle(&self, mut trigger: HttpLifecycleTrigger) -> io::Result<()> {
        enum ListenerStart {
            Bound(Box<ManagedHttpServer>),
            Triggered(io::Result<ShutdownSignal>),
        }

        // Store values before moving self
        let address = self.listen_address().to_string();
        let stats = self.route_state.route_table.stats();
        let shutdown_timeout = self.shutdown_timeout;
        let lifecycle = Arc::clone(&self.lifecycle);
        let scheme = if self.rustls_config().is_some() {
            "https"
        } else {
            "http"
        };

        info!(listen_address = %address, "Starting HTTP listener");

        let shutdown_state = Arc::clone(&lifecycle.shutdown_state);
        let listener_start = {
            let start = HttpServer(Arc::new(self.clone())).start_managed();
            tokio::pin!(start);

            tokio::select! {
                biased;
                triggered = trigger.wait_for_first() => ListenerStart::Triggered(triggered),
                result = &mut start => match result {
                    Ok(server) => ListenerStart::Bound(Box::new(server)),
                    Err(error) => {
                        lifecycle.begin_shutdown();
                        let error = match trigger.stop().await {
                            Ok(()) => error,
                            Err(trigger_error) => io::Error::new(
                                error.kind(),
                                format!("{error}; HTTP lifecycle trigger cleanup failed: {trigger_error}"),
                            ),
                        };
                        return Err(Self::finish_failed_start(
                            error,
                            &lifecycle,
                        )
                        .await);
                    }
                },
            }
        };
        let mut health_failure = None;
        let mut initial_trigger = None;
        let server = match listener_start {
            ListenerStart::Triggered(triggered) => {
                if triggered.is_err() {
                    if let Err(error) = lifecycle.health.record_listener_failure() {
                        health_failure = Some(error.to_string());
                    }
                    let _ = shutdown_state.initiate_shutdown(ShutdownSignal::Manual);
                }
                initial_trigger = Some(triggered);
                None
            }
            ListenerStart::Bound(server) => {
                let server = *server;
                let bound_address = server.bound_address();
                if shutdown_state.is_shutdown_initiated() {
                    initial_trigger = Some(Ok(shutdown_state
                        .initial_signal()
                        .unwrap_or(ShutdownSignal::Manual)));
                } else if lifecycle.bound_address.set(bound_address).is_err() {
                    let runtime_error = io::Error::other(
                        "HTTP listener bound address was published more than once",
                    );
                    let _ = lifecycle.health.record_listener_failure();
                    let _ = shutdown_state.initiate_shutdown(ShutdownSignal::Manual);
                    initial_trigger = Some(Err(runtime_error));
                } else if let Err(error) = lifecycle.health.record_listener_listening() {
                    let runtime_error = io::Error::other(format!(
                        "HTTP listener readiness publication failed: {error}"
                    ));
                    let _ = lifecycle.health.record_listener_failure();
                    let _ = shutdown_state.initiate_shutdown(ShutdownSignal::Manual);
                    initial_trigger = Some(Err(runtime_error));
                } else if let Err(error) = lifecycle.dependencies.start_background() {
                    let _ = shutdown_state.initiate_shutdown(ShutdownSignal::Manual);
                    initial_trigger = Some(Err(io::Error::other(error.to_string())));
                } else if let Err(error) = shutdown_state.publish_ready() {
                    if shutdown_state.is_shutdown_initiated() {
                        initial_trigger = Some(Ok(shutdown_state
                            .initial_signal()
                            .unwrap_or(ShutdownSignal::Manual)));
                    } else {
                        let runtime_error = io::Error::other(format!(
                            "HTTP listener readiness publication failed: {error}"
                        ));
                        let _ = lifecycle.health.record_listener_failure();
                        let _ = shutdown_state.initiate_shutdown(ShutdownSignal::Manual);
                        initial_trigger = Some(Err(runtime_error));
                    }
                } else {
                    info!(
                        scheme,
                        listen_address = %bound_address,
                        route_count = stats.total_routes,
                        exact_route_count = stats.exact_routes,
                        parameterized_route_count = stats.param_routes,
                        "HTTP listener ready"
                    );
                }
                let admission = server.admission_token();
                let force = server.force_token();
                Some((Arc::new(tokio::sync::Mutex::new(server)), admission, force))
            }
        };

        let (shutdown_signal, runtime_result) = if let Some(triggered) = initial_trigger {
            match triggered {
                Ok(signal) => (signal, Ok(())),
                Err(error) => (ShutdownSignal::Manual, Err(error)),
            }
        } else {
            let (server, _, _) = server
                .as_ref()
                .expect("a published listener must retain its managed owner");
            tokio::select! {
                biased;
                _ = lifecycle.dependencies.wait_for_background_failure() => {
                    let _ = shutdown_state.initiate_shutdown(ShutdownSignal::Manual);
                    (ShutdownSignal::Manual, Err(io::Error::other("Background service failed; stopping HTTP host")))
                },
                triggered = trigger.wait_for_first() => match triggered {
                    Ok(signal) => (signal, Ok(())),
                    Err(error) => {
                        if let Err(health_error) = lifecycle.health.record_listener_failure() {
                            health_failure = Some(health_error.to_string());
                        }
                        let _ = shutdown_state.initiate_shutdown(ShutdownSignal::Manual);
                        (ShutdownSignal::Manual, Err(error))
                    }
                },
                result = async { server.lock().await.wait().await } => {
                    let error = match result {
                        Ok(report) => {
                            HttpServer::log_drain_report(&report);
                            io::Error::other(format!(
                                "HTTP listener stopped before a lifecycle trigger: {report:?}"
                            ))
                        }
                        Err(error) => error,
                    };
                    if let Err(health_error) = lifecycle.health.record_listener_failure() {
                        health_failure = Some(health_error.to_string());
                    }
                    let _ = shutdown_state.initiate_shutdown(ShutdownSignal::Manual);
                    (ShutdownSignal::Manual, Err(error))
                }
            }
        };

        lifecycle.begin_shutdown();
        if runtime_result.is_ok() {
            if let Err(error) = lifecycle.health.record_listener_shutting_down() {
                health_failure = Some(error.to_string());
            }
        }

        let deadlines = lifecycle.budget.begin();
        let mut coordinator = FrameworkShutdownCoordinator::before(
            Arc::clone(&shutdown_state),
            shutdown_timeout,
            deadlines.at(ShutdownStage::Dependencies),
            deadlines.at(ShutdownStage::Telemetry),
        );
        coordinator.register(HttpReconciliationHandle(self.clone()));
        if let Some((server, server_admission, server_force)) = server {
            coordinator.register(HttpServerLifecycleHandle {
                phase: FrameworkShutdownPhase::StopAdmission,
                server: Arc::clone(&server),
                admission: server_admission.clone(),
                force: server_force.clone(),
                timeout: shutdown_timeout,
                budget: lifecycle.budget.clone(),
                tasks: lifecycle.tasks.clone(),
                requests: lifecycle.requests.clone(),
            });
            coordinator.register(HttpServerLifecycleHandle {
                phase: FrameworkShutdownPhase::DrainInFlight,
                server,
                admission: server_admission,
                force: server_force,
                timeout: shutdown_timeout,
                budget: lifecycle.budget.clone(),
                tasks: lifecycle.tasks.clone(),
                requests: lifecycle.requests.clone(),
            });
        }
        for phase in [
            FrameworkShutdownPhase::DisposeDependencies,
            FrameworkShutdownPhase::FlushTelemetry,
        ] {
            coordinator.register(HttpDependencyHandle {
                lifecycle: lifecycle.clone(),
                phase,
                timeout: shutdown_timeout,
            });
        }

        let report = coordinator.execute_report(shutdown_signal).await;
        let _ = lifecycle
            .framework_report
            .set(FrameworkSnapshot::from(&report));
        let dependency_cleanup = lifecycle.dependencies.reconcile(&lifecycle.budget).await;
        let trace_report = lifecycle
            .dependencies
            .trace_evidence
            .get()
            .and_then(|e| e.report());
        if let Some(trace_report) = trace_report {
            let file_dropped = trace_report.file_metrics().map_or(0, |metrics| {
                u64::try_from(metrics.total_dropped()).unwrap_or(u64::MAX)
            });
            let dropped = trace_report
                .log_metrics
                .map_or(0, |metrics| metrics.dropped)
                .saturating_add(
                    trace_report
                        .span_metrics
                        .map_or(0, |metrics| metrics.dropped),
                )
                .saturating_add(file_dropped);
            let rejected = trace_report
                .log_metrics
                .map_or(0, |metrics| metrics.rejected)
                .saturating_add(
                    trace_report
                        .span_metrics
                        .map_or(0, |metrics| metrics.rejected),
                );
            if let Err(error) = lifecycle.health.record_exporter_loss(dropped, rejected) {
                health_failure = Some(error.to_string());
            }
        }
        lifecycle.health.record_shutdown_report(&report);
        let trigger_cleanup = trigger.stop().await;
        lifecycle.tasks.monitors.seal();
        let transport_cleanup = self.reconcile_transport().await;
        let lifecycle_result = if let Err(error) = trigger_cleanup {
            let _ = lifecycle.health.record_listener_failure();
            Err(io::Error::other(format!(
                "HTTP lifecycle trigger cleanup failed: {error}"
            )))
        } else if let Err(error) = transport_cleanup {
            Err(error)
        } else if let Err(error) = dependency_cleanup {
            Err(io::Error::other(error.to_string()))
        } else if let Some(error) = health_failure {
            Err(io::Error::other(format!(
                "HTTP health shutdown update failed: {error}"
            )))
        } else if report.is_terminal_complete() && report.reconciles() {
            Ok(())
        } else {
            Err(io::Error::other(format!(
                "HTTP framework shutdown was incomplete: {report:?}"
            )))
        };
        match (runtime_result, lifecycle_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(runtime_error), Ok(())) => Err(runtime_error),
            (Ok(()), Err(lifecycle_error)) => Err(lifecycle_error),
            (Err(runtime_error), Err(lifecycle_error)) => Err(io::Error::new(
                runtime_error.kind(),
                format!("{runtime_error}; {lifecycle_error}"),
            )),
        }
    }
}

impl App {
    /// Internal service seam used by transports that retain bounded outcome
    /// metadata alongside an already-materialized response.
    pub(crate) async fn call_with_outcome(
        &self,
        req: Request,
        rsp: Response,
        owner: &RequestExecutionContext,
    ) -> Result<(Response, AppCallOutcome), HttpApiError> {
        let call_start = std::time::Instant::now();

        info!(
            method = %req.method(),
            "[RUNTIME] APP: Processing request"
        );

        let mut resources = owner.prepare_scope(&self.container, req, rsp).await?;
        let resources = resources
            .as_mut()
            .expect("request resources were published");
        let result = match resources
            .scope
            .as_ref()
            .expect("request scope is live")
            .run(self.handle_request_with_outcome(
                resources.request.as_mut().expect("request is retained"),
                resources.response.as_mut().expect("response is retained"),
            ))
            .await
        {
            Ok(result) => result.map_err(HttpApiError::from),
            Err(error) => Err(HttpApiError::from(error)),
        };
        let call_duration = call_start.elapsed();

        match &result {
            Ok(outcome) if outcome.outcome() == "success" => info!(
                status = outcome.status(),
                outcome = outcome.outcome(),
                duration_ms = call_duration.as_secs_f64() * 1_000.0,
                "[RUNTIME] APP: Request completed successfully"
            ),
            Ok(outcome) if outcome.outcome() == "rejected" => warn!(
                status = outcome.status(),
                outcome = outcome.outcome(),
                error_code = outcome.error_code().unwrap_or("HTTP_CLIENT_ERROR"),
                duration_ms = call_duration.as_secs_f64() * 1_000.0,
                "[RUNTIME] APP: Request completed with a rejection response"
            ),
            Ok(outcome) => error!(
                status = outcome.status(),
                outcome = outcome.outcome(),
                error_code = outcome.error_code().unwrap_or("none"),
                duration_ms = call_duration.as_secs_f64() * 1_000.0,
                "[RUNTIME] APP: Request completed with an error response"
            ),
            Err(error) => error!(
                outcome = "error",
                error_code = error.error_code(),
                duration_ms = call_duration.as_secs_f64() * 1_000.0,
                "[RUNTIME] APP: Request execution failed before a response was materialized"
            ),
        }

        let outcome = result?;
        Ok((
            resources
                .response
                .take()
                .expect("completed response is retained"),
            outcome,
        ))
    }
}

impl App {
    /// Executes one request through the immutable HTTP middleware chain.
    ///
    /// With no configured middleware this calls the terminal directly; the
    /// hot path does not construct an exchange or recursive chain adapter.
    #[cfg(test)]
    pub(crate) async fn handle_request(
        &self,
        req: &mut Request,
        rsp: &mut Response,
    ) -> Result<(), HttpApiError> {
        self.handle_request_with_outcome(req, rsp)
            .await
            .map(|_| ())
            .map_err(HttpApiError::from)
    }

    /// Internal transport seam retaining bounded terminal metadata after a
    /// handler or middleware error has already become a safe response.
    pub(crate) async fn handle_request_with_outcome(
        &self,
        req: &mut Request,
        rsp: &mut Response,
    ) -> Result<AppCallOutcome, ResponseWriteError> {
        req.clear_local();
        req.set_localization_catalog(self.localization_catalog.clone());
        let state = TerminalResponseState::default();

        if self.middlewares.is_empty() {
            self.handle_terminal_response(req, rsp, &state).await?;
            return Ok(AppCallOutcome::from_terminal_response(
                rsp,
                state.snapshot(),
            ));
        }

        let middleware_start = std::time::Instant::now();
        let mut exchange = HttpExchange::new(req, rsp);
        let outcome_slot = MiddlewareChainOutcomeSlot::default();
        let terminal = AppTerminal {
            app: self,
            state: &state,
        };
        let error_writer = AppErrorWriter {
            app: self,
            state: &state,
        };
        let result = HttpMiddlewareChain::new(
            &self.middlewares,
            &terminal,
            &error_writer,
            &outcome_slot,
            self,
        )
        .run(&mut exchange)
        .await;
        self.complete_middleware_chain(&mut exchange, result, outcome_slot.take(), &state)
            .await?;

        trace!(
            middleware_count = self.middlewares.len(),
            middleware_duration_us = middleware_start.elapsed().as_secs_f64() * 1_000_000.0,
            "[RUNTIME] APP: Around middleware chain completed"
        );
        Ok(AppCallOutcome::from_terminal_response(
            exchange.response(),
            state.snapshot(),
        ))
    }

    async fn complete_middleware_chain(
        &self,
        exchange: &mut HttpExchange<'_>,
        result: Result<(), HttpMiddlewareError>,
        chain_outcome: MiddlewareChainOutcome,
        state: &TerminalResponseState,
    ) -> Result<(), ResponseWriteError> {
        if let Some(error) = state.fatal_write_error() {
            return Err(error);
        }
        let result = match (result, chain_outcome) {
            (_, MiddlewareChainOutcome::WriterFailed(failure)) => {
                error!(
                    middleware = failure.origin(),
                    error_code = "RESPONSE_ENCODING_ERROR",
                    outcome = "writer_error",
                    "[RUNTIME] APP: Middleware error response writer failed"
                );
                Err(state.write_error())
            }
            (Ok(()), MiddlewareChainOutcome::Materialized(failure)) => {
                // The chain owns the first materialized middleware failure.
                state.record_response_code(failure.code().as_str());
                // The final request event owns HTTP severity after outer
                // middleware has had its opportunity to recover the response.
                // A materialized refusal is not a response writer failure.
                debug!(
                    middleware = failure.origin(),
                    error_code = failure.code().as_str(),
                    status = i64::from(failure.status()),
                    outcome = http_status_outcome(failure.status()),
                    "[RUNTIME] APP: Typed middleware response materialized"
                );
                Ok(())
            }
            (
                Err(error),
                MiddlewareChainOutcome::None | MiddlewareChainOutcome::Materialized(_),
            ) => {
                let result = self.write_middleware_error_response(exchange, &error).await;
                if result.is_ok() {
                    state.record_response_code(error.diagnostic_code().as_str());
                }
                result
            }
            (Ok(()), MiddlewareChainOutcome::None) => Ok(()),
        };
        let (request, response) = exchange.parts_mut();
        Self::finish_response_write(request, response, state, result).await
    }

    async fn handle_terminal_response(
        &self,
        req: &mut Request,
        rsp: &mut Response,
        state: &TerminalResponseState,
    ) -> Result<(), ResponseWriteError> {
        let result = self.handle_terminal(req, rsp, state).await;
        Self::finish_response_write(req, rsp, state, result).await
    }

    async fn finish_response_write(
        req: &mut Request,
        rsp: &mut Response,
        state: &TerminalResponseState,
        result: Result<(), ResponseWriteError>,
    ) -> Result<(), ResponseWriteError> {
        if let Some(error) = state.fatal_write_error() {
            return Err(error);
        }
        let error = match result {
            Ok(()) => match rsp.body_failure() {
                None => return Ok(()),
                Some(error) => error.into(),
            },
            Err(error) => error,
        };
        state.begin_fallback(error)?;
        error!(
            error = ?error,
            error_code = "RESPONSE_ENCODING_ERROR",
            outcome = "writer_error",
            "[RUNTIME] APP: Response construction failed"
        );
        match Self::write_safe_response_encoding_error(req, rsp, error).await {
            Ok(()) => {
                state.record_response_code("RESPONSE_ENCODING_ERROR");
                Ok(())
            }
            Err(error) => {
                state.fail_fallback(error);
                Err(error)
            }
        }
    }

    async fn write_safe_response_encoding_error(
        req: &mut Request,
        rsp: &mut Response,
        error: ResponseWriteError,
    ) -> Result<(), ResponseWriteError> {
        write_error_response(HttpApiError::from(error), rsp, req)
            .await
            .map(|_| ())
    }

    async fn write_middleware_error_response(
        &self,
        exchange: &mut HttpExchange<'_>,
        error: &HttpMiddlewareError,
    ) -> Result<(), ResponseWriteError> {
        if let Some(rejection) = error.rejection() {
            return rejection.write_to_response(exchange.response_mut()).await;
        }

        let status = error.http_status();
        let status_reason = http::StatusCode::from_u16(status)
            .ok()
            .and_then(|status| status.canonical_reason())
            .unwrap_or("Error");
        let mut response = Response::from_limits(exchange.response().limits());
        let mut builder = Response::builder().status(status, status_reason);
        if let Some(retry_after) = error.retry_after() {
            let seconds = retry_after
                .as_secs()
                .saturating_add(u64::from(retry_after.subsec_nanos() != 0));
            builder = builder.header("Retry-After", &seconds.to_string());
        }
        let builder = builder.json(MiddlewarePublicErrorBody {
            error: true,
            code: error.diagnostic_code().as_str(),
            message: error.public_message(),
            status,
        });
        let (request, current_response) = exchange.parts_mut();
        builder.write_to_response(&mut response, request).await?;
        *current_response = response;
        Ok(())
    }

    async fn execute_matched_route(
        &self,
        route: &RouteInfo,
        method: &str,
        method_metric: &'static str,
        req: &mut Request,
        rsp: &mut Response,
        state: &TerminalResponseState,
    ) -> Result<(), ResponseWriteError> {
        let guard_start = std::time::Instant::now();
        let guard_count = route.guard_type_ids.len();

        if guard_count > 0 {
            trace!(guard_count, "[RUNTIME] APP: Executing route guards");
        }

        for (idx, guard_type_id) in route.guard_type_ids.iter().enumerate() {
            let guard_instance = match self.route_state.guard_for_route(route, guard_type_id) {
                Ok(guard) => guard,
                Err(error) => {
                    error!(guard_index = idx, route = %route.path,
                        "[RUNTIME] APP: Route guard invariant failed");
                    let error = missing_route_guard_http_error(&error);
                    let code = error.error_code();
                    write_error_response(error, rsp, req).await?;
                    state.record_response_code(code);
                    return Ok(());
                }
            };
            let single_guard_start = std::time::Instant::now();
            let guard_span = tracing::info_span!(
                "http.server.authorization",
                http.route = %route.path,
                lily.guard = guard_instance.name(),
                lily.outcome = tracing::field::Empty,
                lily.error_code = tracing::field::Empty,
                otel.status_code = tracing::field::Empty,
            );
            let cancellation = req.execution_cancellation();
            let guard_result = guard_instance
                .can_activate(req, cancellation)
                .instrument(guard_span.clone())
                .await;
            let single_guard_duration = single_guard_start.elapsed();
            let observation_outcome = guard_observation_outcome(*guard_type_id, &guard_result);
            match &guard_result {
                Ok(()) => {
                    guard_span.record("lily.outcome", observation_outcome);
                    self.guard_outcome_counter.add(
                        1,
                        &[
                            KeyValue::new("lily.guard.name", guard_instance.name()),
                            KeyValue::new("lily.outcome", observation_outcome),
                            KeyValue::new("lily.error_code", "none"),
                        ],
                    );
                }
                Err(rejection) => {
                    guard_span.record("lily.outcome", observation_outcome);
                    guard_span.record("lily.error_code", rejection.error_code());
                    if http_status_outcome(rejection.status()) == "error" {
                        guard_span.record("otel.status_code", "ERROR");
                    }
                    self.guard_outcome_counter.add(
                        1,
                        &[
                            KeyValue::new("lily.guard.name", guard_instance.name()),
                            KeyValue::new("lily.outcome", observation_outcome),
                            KeyValue::new("lily.error_code", rejection.error_code()),
                        ],
                    );
                }
            }

            trace!(
                guard_index = idx,
                passed = guard_result.is_ok(),
                duration_us = single_guard_duration.as_secs_f64() * 1_000_000.0,
                "[RUNTIME] APP: Guard executed"
            );

            if let Err(rejection) = guard_result {
                let (status, _) = rejection.http_status();
                warn!(
                    guard_index = idx,
                    route = %route.path,
                    status,
                    "[RUNTIME] APP: Guard denied access"
                );
                let code = rejection.error_code();
                write_guard_rejection(rsp, rejection).await?;
                state.record_response_code(code);
                info!(status, "[RUNTIME] APP: Access denied by guard");
                return Ok(());
            }
        }

        if guard_count > 0 {
            let guard_total_duration = guard_start.elapsed();
            self.guard_duration.record(
                guard_total_duration.as_secs_f64(),
                &[KeyValue::new("http.route", route.path.clone())],
            );
            debug!(
                guard_count,
                total_duration_us = guard_total_duration.as_secs_f64() * 1_000_000.0,
                "[RUNTIME] APP: All guards passed"
            );
        }

        let handler_start = std::time::Instant::now();
        info!(route = %route.path, method, "[RUNTIME] APP: Executing action handler");
        let handler_span = tracing::info_span!(
            "http.server.handler",
            http.route = %route.path,
            http.request.method = %method,
            lily.outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let handler_result = route
            .handler
            .call(self.container.services(), req, rsp)
            .instrument(handler_span.clone())
            .await
            .and_then(|outcome| match rsp.body_failure() {
                Some(error) => Err(error.into()),
                None => Ok(outcome),
            });
        let handler_duration = handler_start.elapsed();
        self.handler_duration.record(
            handler_duration.as_secs_f64(),
            &[
                KeyValue::new("http.route", route.path.clone()),
                KeyValue::new("http.request.method", method_metric),
            ],
        );

        let (outcome_label, error_code) = match &handler_result {
            Ok(outcome) => state.record_action(*outcome, rsp.status_code_value()),
            Err(_) => ("writer_error", Some("RESPONSE_ENCODING_ERROR")),
        };
        handler_span.record("lily.outcome", outcome_label);
        if let Some(code) = error_code {
            handler_span.record("lily.error_code", code);
        }
        if matches!(outcome_label, "error" | "writer_error") {
            handler_span.record("otel.status_code", "ERROR");
        }
        self.handler_outcome_counter.add(
            1,
            &[
                KeyValue::new("http.route", route.path.clone()),
                KeyValue::new("lily.outcome", outcome_label),
                KeyValue::new("lily.error_code", error_code.unwrap_or("none")),
            ],
        );
        if matches!(outcome_label, "error" | "writer_error") {
            error!(
                parent: &handler_span,
                route = %route.path,
                duration_ms = handler_duration.as_secs_f64() * 1_000.0,
                outcome = outcome_label,
                error_code = error_code.unwrap_or("none"),
                "[RUNTIME] APP: Handler completed with an error outcome"
            );
        } else if outcome_label == "rejected" {
            warn!(
                parent: &handler_span,
                route = %route.path,
                duration_ms = handler_duration.as_secs_f64() * 1_000.0,
                outcome = outcome_label,
                error_code = error_code.unwrap_or("HTTP_CLIENT_ERROR"),
                "[RUNTIME] APP: Handler completed with a rejection outcome"
            );
        } else {
            info!(
                parent: &handler_span,
                route = %route.path,
                duration_ms = handler_duration.as_secs_f64() * 1_000.0,
                outcome = outcome_label,
                "[RUNTIME] APP: Handler response conversion completed"
            );
        }
        handler_result.map(|_| ())
    }

    async fn execute_matched_route_response(
        &self,
        route: &RouteInfo,
        method: &str,
        method_metric: &'static str,
        req: &mut Request,
        rsp: &mut Response,
        state: &TerminalResponseState,
    ) -> Result<(), ResponseWriteError> {
        let result = self
            .execute_matched_route(route, method, method_metric, req, rsp, state)
            .await;
        Self::finish_response_write(req, rsp, state, result).await
    }

    async fn handle_terminal(
        &self,
        req: &mut Request,
        rsp: &mut Response,
        state: &TerminalResponseState,
    ) -> Result<(), ResponseWriteError> {
        let method = req.method().to_string();
        let method_metric = http_method_metric_label(&method);
        let full_path = req.path().to_string();

        // ✅ FIX: Strip query string from path for route matching
        // Routes are registered without query strings (e.g., "/v1/crates")
        // but requests may include them (e.g., "/v1/crates?q=test&per_page=20")
        let path = if let Some(query_start) = full_path.find('?') {
            &full_path[..query_start]
        } else {
            &full_path
        };

        let lookup_start = std::time::Instant::now();

        trace!(method = %method, "[RUNTIME] APP: Starting route lookup");

        let (route_option, method_not_allowed) =
            match self.route_state.route_table.resolve(&method, path) {
                RouteResolution::Found(route) => (Some(route), None),
                RouteResolution::MethodNotAllowed { allow } => (None, Some(allow)),
                RouteResolution::NotFound => (None, None),
            };
        let lookup_duration = lookup_start.elapsed();

        self.route_lookup_duration.record(
            lookup_duration.as_secs_f64(),
            &[KeyValue::new("http.request.method", method_metric)],
        );

        debug!(
            method = %method,
            lookup_duration_us = lookup_duration.as_secs_f64() * 1_000_000.0,
            matched = route_option.is_some(),
            "[RUNTIME] APP: Route lookup completed"
        );

        if let Some(route) = route_option {
            self.route_match_counter.add(
                1,
                &[
                    KeyValue::new("http.request.method", method_metric),
                    KeyValue::new("http.route", route.path.clone()),
                ],
            );
            tracing::Span::current().record("route_matched", true);
            tracing::Span::current().record("http.route", route.path.as_str());

            info!(
                route_path = %route.path,
                method = %method,
                "[RUNTIME] APP: Route matched successfully"
            );
            // Extract parameters if the route has them
            if route.path.contains([':', '*']) {
                let param_start = std::time::Instant::now();
                let params = RouteTable::extract_params(&route.path, path);
                let param_duration = param_start.elapsed();

                debug!(
                    param_count = params.len(),
                    extraction_duration_us = param_duration.as_secs_f64() * 1_000_000.0,
                    "[RUNTIME] APP: Route parameters extracted"
                );

                req.set_params(params);
            }

            let route_middlewares = self.route_state.middleware_for_route(route);
            if !route_middlewares.is_empty() {
                let mut exchange = HttpExchange::new(req, rsp);
                let outcome_slot = MiddlewareChainOutcomeSlot::default();
                let terminal = MatchedRouteTerminal {
                    app: self,
                    route,
                    method: &method,
                    method_metric,
                    state,
                };
                let error_writer = AppErrorWriter { app: self, state };
                let result = HttpMiddlewareChain::new(
                    route_middlewares,
                    &terminal,
                    &error_writer,
                    &outcome_slot,
                    self,
                )
                .run(&mut exchange)
                .await;
                return self
                    .complete_middleware_chain(&mut exchange, result, outcome_slot.take(), state)
                    .await;
            }
            return self
                .execute_matched_route_response(route, &method, method_metric, req, rsp, state)
                .await;
        } else {
            self.route_miss_counter.add(
                1,
                &[
                    KeyValue::new("http.request.method", method_metric),
                    KeyValue::new("http.route", "<unmatched>"),
                ],
            );
            tracing::Span::current().record("http.route", "<unmatched>");

            let mut safe_response = Response::from_limits(rsp.limits());
            let (status, reason, body) = if let Some(allow) = method_not_allowed {
                warn!(
                    method = %method,
                    allow = %allow.join(", "),
                    "[RUNTIME] APP: Path matched with another method - returning 405"
                );
                safe_response.try_insert_header("Allow", &allow.join(", "))?;
                (
                    405,
                    "Method Not Allowed",
                    b"{\"error\":\"Method not allowed\"}".as_slice(),
                )
            } else {
                warn!(
                    method = %method,
                    "[RUNTIME] APP: No route matched - returning 404 Not Found"
                );
                (
                    404,
                    "Not Found",
                    b"{\"error\":\"Route not found\"}".as_slice(),
                )
            };
            safe_response.status_code(status, reason);
            safe_response.try_insert_header("Content-Type", "application/json")?;
            safe_response.write_body(body)?;
            *rsp = safe_response;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl HttpNextService for AppTerminal<'_, '_> {
    async fn run(&self, exchange: &mut HttpExchange<'_>) -> Result<(), HttpMiddlewareError> {
        let (request, response) = exchange.parts_mut();
        self.app
            .handle_terminal_response(request, response, self.state)
            .await
            .map_err(|_| HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL))
    }
}

#[async_trait::async_trait]
impl HttpNextService for MatchedRouteTerminal<'_, '_, '_> {
    async fn run(&self, exchange: &mut HttpExchange<'_>) -> Result<(), HttpMiddlewareError> {
        let (request, response) = exchange.parts_mut();
        self.app
            .execute_matched_route_response(
                self.route,
                self.method,
                self.method_metric,
                request,
                response,
                self.state,
            )
            .await
            .map_err(|_| HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL))
    }
}

#[async_trait::async_trait]
impl HttpMiddlewareErrorWriter for AppErrorWriter<'_, '_> {
    async fn write_error_response(
        &self,
        exchange: &mut HttpExchange<'_>,
        error: &HttpMiddlewareError,
    ) -> Result<(), HttpMiddlewareError> {
        if self.state.fatal_write_error().is_some() {
            return Err(HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL));
        }
        match self
            .app
            .write_middleware_error_response(exchange, error)
            .await
        {
            Ok(()) => {
                self.state
                    .record_response_code(error.diagnostic_code().as_str());
                Ok(())
            }
            Err(error) => {
                self.state.record_write_error(error);
                Err(HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL))
            }
        }
    }
}

impl HttpMiddlewareObserver for App {
    fn observe(
        &self,
        descriptor: MiddlewareDescriptor,
        outcome: HttpMiddlewareObservationOutcome,
        duration: Duration,
    ) {
        // Descriptor names have already passed the static, bounded label
        // validator during AppBuilder::build. The outcome comes exclusively
        // from the closed framework enum above; request and error data cannot
        // enter this attribute set.
        let attributes = [
            KeyValue::new("lily.middleware.name", descriptor.name()),
            KeyValue::new("lily.outcome", outcome.as_str()),
        ];
        self.middleware_duration
            .record(duration.as_secs_f64(), &attributes);
        self.middleware_outcome_counter.add(1, &attributes);
    }
}

/// Feature-gated entry points used by the repository's cargo-fuzz targets.
///
/// These adapters deliberately call the production request dispatcher and
/// middleware executor. They are absent from normal builds and do not create
/// a second routing or unwind implementation.
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing {
    use super::*;
    use crate::app::middleware_executor::NoopHttpMiddlewareObserver;
    use crate::{handler::Handler, registry::RouteInfo};
    use async_trait::async_trait;
    use lily_core::structs::RawHeader;
    use lily_middleware::{HttpNext, MiddlewareKind};
    use std::{
        future::Future,
        pin::Pin,
        sync::{
            atomic::{AtomicUsize, Ordering as AtomicOrdering},
            Mutex,
        },
    };
    use tokio::sync::{OnceCell, Semaphore};

    const MAX_METHOD_BYTES: usize = 32;
    const MAX_TARGET_BYTES: usize = 4 * 1024;
    const MAX_HEADERS: usize = 32;
    const MAX_HEADER_NAME_BYTES: usize = 128;
    const MAX_HEADER_VALUE_BYTES: usize = 4 * 1024;
    const MAX_BODY_BYTES: usize = 64 * 1024;
    const CHAIN_DEADLINE: Duration = Duration::from_millis(50);

    /// Owned, bounded request components consumed by the real App dispatcher.
    #[derive(Debug)]
    pub struct AppDispatchInput {
        pub method: String,
        pub target: String,
        pub headers: Vec<(String, String)>,
        pub body: Vec<u8>,
    }

    /// A bounded result category; expected request rejection is not a crash.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum AppDispatchResult {
        InputRejected,
        Completed { status: u16, body_bytes: usize },
    }

    struct DenyGuard;

    #[async_trait]
    impl GuardTrait for DenyGuard {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, crate::guard::GuardInitError> {
            Ok(Self)
        }

        async fn can_activate(
            &self,
            _request: &mut Request,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::guard::GuardRejection> {
            Err(crate::guard::GuardRejection::forbidden(
                lily_web_core::HttpErrorCode::new("FORBIDDEN").expect("static guard code is valid"),
            )
            .expect("403 guard rejection is valid"))
        }
    }

    fn success_handler<'a>(
        _extensions: Arc<Extensions>,
        _request: &'a mut Request,
        response: &'a mut Response,
    ) -> Pin<Box<dyn Future<Output = Result<(), HttpApiError>> + Send + 'a>> {
        Box::pin(async move {
            response.status_code(200, "OK");
            response
                .try_insert_header("Content-Type", "text/plain; charset=utf-8")
                .map_err(|_| {
                    HttpApiError::ResponseEncodingError(
                        "fuzz dispatch response header rejected".to_string(),
                    )
                })?;
            response.write_body(b"ok")?;
            Ok(())
        })
    }

    fn failing_handler<'a>(
        _extensions: Arc<Extensions>,
        _request: &'a mut Request,
        _response: &'a mut Response,
    ) -> Pin<Box<dyn Future<Output = Result<(), HttpApiError>> + Send + 'a>> {
        Box::pin(async {
            Err(HttpApiError::InternalError(
                "bounded fuzz handler failure".to_string(),
            ))
        })
    }

    async fn dispatch_app() -> Result<&'static Arc<App>, ()> {
        static APP: OnceCell<Arc<App>> = OnceCell::const_new();
        APP.get_or_try_init(|| async {
            let guard_type_id = TypeId::of::<DenyGuard>();
            let routes = vec![
                RouteInfo {
                    method: "GET".to_string(),
                    path: "/ok".to_string(),
                    handler: Handler::new(Arc::new(success_handler), false),
                    handler_name: "fuzz_success".to_string(),
                    guard_type_ids: Vec::new(),
                    middleware_registrations: Vec::new(),
                    cors_policy_registration: crate::private::CorsRoutePolicyRegistration::inherit(
                    ),
                    route_plan_id: 0,
                },
                RouteInfo {
                    method: "GET".to_string(),
                    path: "/items/:id".to_string(),
                    handler: Handler::new(Arc::new(success_handler), false),
                    handler_name: "fuzz_parameter".to_string(),
                    guard_type_ids: Vec::new(),
                    middleware_registrations: Vec::new(),
                    cors_policy_registration: crate::private::CorsRoutePolicyRegistration::inherit(
                    ),
                    route_plan_id: 0,
                },
                RouteInfo {
                    method: "GET".to_string(),
                    path: "/guarded".to_string(),
                    handler: Handler::new(Arc::new(success_handler), false),
                    handler_name: "fuzz_guarded".to_string(),
                    guard_type_ids: vec![guard_type_id],
                    middleware_registrations: Vec::new(),
                    cors_policy_registration: crate::private::CorsRoutePolicyRegistration::inherit(
                    ),
                    route_plan_id: 0,
                },
                RouteInfo {
                    method: "GET".to_string(),
                    path: "/error".to_string(),
                    handler: Handler::new(Arc::new(failing_handler), false),
                    handler_name: "fuzz_error".to_string(),
                    guard_type_ids: Vec::new(),
                    middleware_registrations: Vec::new(),
                    cors_policy_registration: crate::private::CorsRoutePolicyRegistration::inherit(
                    ),
                    route_plan_id: 0,
                },
            ];
            let route_table = RouteTable::from_routes(routes).map_err(|_| ())?;
            let route_state = Arc::new(
                AppRouteState::new(
                    route_table,
                    HashMap::from([(guard_type_id, Arc::new(DenyGuard) as GuardInstance)]),
                )
                .map_err(|_| ())?,
            );
            let mut app = AppBuilder::new("127.0.0.1:0")
                .build()
                .await
                .map_err(|_| ())?;
            app.route_state = route_state;
            Ok(Arc::new(app))
        })
        .await
    }

    /// Runs normalized request components through Request construction, route
    /// lookup, guards and the terminal handler/error materializer.
    pub async fn exercise_app_dispatch(input: AppDispatchInput) -> AppDispatchResult {
        if input.method.len() > MAX_METHOD_BYTES
            || input.target.len() > MAX_TARGET_BYTES
            || input.headers.len() > MAX_HEADERS
            || input.body.len() > MAX_BODY_BYTES
            || input.headers.iter().any(|(name, value)| {
                name.len() > MAX_HEADER_NAME_BYTES || value.len() > MAX_HEADER_VALUE_BYTES
            })
        {
            return AppDispatchResult::InputRejected;
        }

        let headers = input
            .headers
            .into_iter()
            .enumerate()
            .map(|(index, (name, value))| RawHeader {
                name,
                value,
                line_number: index.saturating_add(1),
                raw_line: "<fuzz-header-redacted>".to_string(),
            })
            .collect();
        let Ok(mut request) =
            Request::from_transport_parts(input.method, input.target, headers, &input.body).await
        else {
            return AppDispatchResult::InputRejected;
        };
        let Ok(mut response) = Response::new().await else {
            return AppDispatchResult::InputRejected;
        };
        let Ok(app) = dispatch_app().await else {
            return AppDispatchResult::InputRejected;
        };
        let Ok(outcome) = app
            .handle_request_with_outcome(&mut request, &mut response)
            .await
        else {
            return AppDispatchResult::InputRejected;
        };

        assert!(request.local().is_empty(), "request-local state leaked");
        let parts = response.into_transport_parts().unwrap();
        assert_eq!(outcome.status(), parts.status);
        assert!((100..=599).contains(&parts.status));
        AppDispatchResult::Completed {
            status: outcome.status(),
            body_bytes: parts.body.len(),
        }
    }

    /// Synthetic decisions that drive the production middleware executor.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum MiddlewareDecision {
        Continue,
        ShortCircuit,
        Reject,
        ErrorBeforeNext,
        ErrorAfterNext,
        Pending,
    }

    /// Bounded observations returned after executor invariants are checked.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct MiddlewareChainReport {
        pub timed_out: bool,
        pub terminal_calls: usize,
        pub error_writes: usize,
        pub status: u16,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum ChainEvent {
        Before(usize),
        After(usize),
        Stop(usize),
        Terminal,
    }

    struct FuzzMiddleware {
        index: usize,
        decision: MiddlewareDecision,
        events: Arc<Mutex<Vec<ChainEvent>>>,
        semaphore: Arc<Semaphore>,
        active: Arc<AtomicUsize>,
    }

    struct ActiveFrame(Arc<AtomicUsize>);

    impl Drop for ActiveFrame {
        fn drop(&mut self) {
            self.0.fetch_sub(1, AtomicOrdering::AcqRel);
        }
    }

    #[async_trait]
    impl HttpMiddleware for FuzzMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            Ok(Self {
                index: 0,
                decision: MiddlewareDecision::Continue,
                events: Arc::new(Mutex::new(Vec::new())),
                semaphore: Arc::new(Semaphore::new(1)),
                active: Arc::new(AtomicUsize::new(0)),
            })
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("http_fuzz_layer", MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            self.events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(ChainEvent::Before(self.index));
            match self.decision {
                MiddlewareDecision::Continue => {
                    next.run(exchange).await?;
                    self.events
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .push(ChainEvent::After(self.index));
                    Ok(())
                }
                MiddlewareDecision::ShortCircuit => {
                    exchange.response_mut().status_code(204, "No Content");
                    self.events
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .push(ChainEvent::Stop(self.index));
                    Ok(())
                }
                MiddlewareDecision::Reject => {
                    self.events
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .push(ChainEvent::Stop(self.index));
                    Err(lily_middleware::HttpMiddlewareRejection::new(
                        403,
                        MiddlewareErrorCode::new("HTTP_FUZZ_REJECTED")
                            .expect("static fuzz code is valid"),
                    )
                    .expect("static fuzz rejection is valid")
                    .into())
                }
                MiddlewareDecision::ErrorBeforeNext => {
                    self.events
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .push(ChainEvent::Stop(self.index));
                    Err(HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL))
                }
                MiddlewareDecision::ErrorAfterNext => {
                    next.run(exchange).await?;
                    self.events
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .push(ChainEvent::After(self.index));
                    Err(HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL))
                }
                MiddlewareDecision::Pending => {
                    let _permit = Arc::clone(&self.semaphore)
                        .acquire_owned()
                        .await
                        .expect("fuzz semaphore is never closed");
                    self.active.fetch_add(1, AtomicOrdering::AcqRel);
                    let _active = ActiveFrame(Arc::clone(&self.active));
                    std::future::pending::<()>().await;
                    Ok(())
                }
            }
        }
    }

    struct FuzzTerminal {
        calls: Arc<AtomicUsize>,
        events: Arc<Mutex<Vec<ChainEvent>>>,
    }

    #[async_trait]
    impl HttpNextService for FuzzTerminal {
        async fn run(&self, _exchange: &mut HttpExchange<'_>) -> Result<(), HttpMiddlewareError> {
            self.calls.fetch_add(1, AtomicOrdering::AcqRel);
            self.events
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(ChainEvent::Terminal);
            Ok(())
        }
    }

    struct FuzzErrorWriter(Arc<AtomicUsize>);

    #[async_trait]
    impl HttpMiddlewareErrorWriter for FuzzErrorWriter {
        async fn write_error_response(
            &self,
            exchange: &mut HttpExchange<'_>,
            error: &HttpMiddlewareError,
        ) -> Result<(), HttpMiddlewareError> {
            self.0.fetch_add(1, AtomicOrdering::AcqRel);
            let mut response = Response::new()
                .await
                .map_err(|_| HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL))?;
            response.status_code(usize::from(error.http_status()), "Error");
            *exchange.response_mut() = response;
            Ok(())
        }
    }

    async fn run_chain(
        decisions: Vec<MiddlewareDecision>,
        semaphore: Arc<Semaphore>,
        active: Arc<AtomicUsize>,
    ) -> MiddlewareChainReport {
        let events = Arc::new(Mutex::new(Vec::new()));
        let terminal_calls = Arc::new(AtomicUsize::new(0));
        let error_writes = Arc::new(AtomicUsize::new(0));
        let middlewares: Vec<_> = decisions
            .into_iter()
            .enumerate()
            .map(|(index, decision)| {
                let middleware: Arc<dyn HttpMiddleware> = Arc::new(FuzzMiddleware {
                    index,
                    decision,
                    events: Arc::clone(&events),
                    semaphore: Arc::clone(&semaphore),
                    active: Arc::clone(&active),
                });
                CompiledHttpMiddleware::new(Arc::clone(&middleware), middleware.descriptor())
            })
            .collect();
        let terminal = FuzzTerminal {
            calls: Arc::clone(&terminal_calls),
            events: Arc::clone(&events),
        };
        let writer = FuzzErrorWriter(Arc::clone(&error_writes));
        let outcome = MiddlewareChainOutcomeSlot::default();
        let mut request =
            Request::from_transport_parts("GET".to_string(), "/fuzz".to_string(), Vec::new(), &[])
                .await
                .expect("static fuzz request is valid");
        let mut response = Response::new()
            .await
            .expect("static fuzz response is valid");
        let mut exchange = HttpExchange::new(&mut request, &mut response);
        let _ = HttpMiddlewareChain::new(
            &middlewares,
            &terminal,
            &writer,
            &outcome,
            &NoopHttpMiddlewareObserver,
        )
        .run(&mut exchange)
        .await;

        let terminal_calls = terminal_calls.load(AtomicOrdering::Acquire);
        let error_writes = error_writes.load(AtomicOrdering::Acquire);
        assert!(terminal_calls <= 1, "terminal ran more than once");
        assert!(
            error_writes <= 1,
            "middleware error materialized more than once"
        );
        let captured = events
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        let mut entered = Vec::new();
        for event in captured {
            match event {
                ChainEvent::Before(index) => entered.push(index),
                ChainEvent::After(index) | ChainEvent::Stop(index) => {
                    assert_eq!(entered.pop(), Some(index), "middleware unwind was not LIFO");
                }
                ChainEvent::Terminal => {}
            }
        }
        assert!(entered.is_empty(), "entered middleware was not reconciled");
        assert!((100..=599).contains(&exchange.response().status_code_value()));
        MiddlewareChainReport {
            timed_out: false,
            terminal_calls,
            error_writes,
            status: exchange.response().status_code_value(),
        }
    }

    /// Runs a bounded decision vector through the production around-chain.
    /// A pending frame is hard-cancelled, awaited and checked for permit and
    /// active-frame reconciliation before the iteration returns.
    pub async fn exercise_middleware_chain(
        decisions: Vec<MiddlewareDecision>,
    ) -> MiddlewareChainReport {
        if decisions.len() > lily_middleware::MAX_HTTP_MIDDLEWARES {
            return MiddlewareChainReport {
                timed_out: false,
                terminal_calls: 0,
                error_writes: 0,
                status: 400,
            };
        }
        let semaphore = Arc::new(Semaphore::new(1));
        let active = Arc::new(AtomicUsize::new(0));
        let task_semaphore = Arc::clone(&semaphore);
        let task_active = Arc::clone(&active);
        let mut task =
            tokio::spawn(async move { run_chain(decisions, task_semaphore, task_active).await });

        match tokio::time::timeout(CHAIN_DEADLINE, &mut task).await {
            Ok(Ok(report)) => report,
            Ok(Err(join_error)) if join_error.is_panic() => {
                std::panic::resume_unwind(join_error.into_panic())
            }
            Ok(Err(_)) => panic!("middleware fuzz task was cancelled unexpectedly"),
            Err(_) => {
                task.abort();
                let _ = task.await;
                assert_eq!(active.load(AtomicOrdering::Acquire), 0);
                assert_eq!(semaphore.available_permits(), 1);
                MiddlewareChainReport {
                    timed_out: true,
                    terminal_calls: 0,
                    error_writes: 0,
                    status: 408,
                }
            }
        }
    }
}

#[cfg(test)]
mod route_state_tests {
    use super::*;
    use crate::controller::{ControllerBindingError, ControllerInitError, ControllerTrait};
    use crate::handler::{Handler, HttpAction, HttpActionFuture};
    use crate::private::{
        downcast_controller, ControllerActionRegistration, ControllerRegistration,
        CorsRoutePolicyRegistration, ErasedController,
    };
    use crate::registry::{materialize_controller_routes, PendingControllerRoute};
    use lily_injection::Injectable;
    use lily_injection::ServiceTrait;
    use lily_monitoring::HealthStatus;
    use lily_web_core::{BodyBudget, IntoResponse, RequestExt, ResponseLimits};
    use rcgen::generate_simple_self_signed;
    use serde::Serializer;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;
    use tempfile::TempDir;

    static DI_MIDDLEWARE_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static DI_MIDDLEWARE_REQUESTS: AtomicUsize = AtomicUsize::new(0);
    static SESSION_MIDDLEWARE_EVENTS: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());
    static ROUTE_MIDDLEWARE_EVENTS: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());
    static ROUTE_MIDDLEWARE_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static REJECTED_ROUTE_TERMINAL_CALLS: AtomicUsize = AtomicUsize::new(0);
    static CONTROLLER_PROVIDER_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static CONTROLLER_SCOPED_INITIALIZATIONS: AtomicUsize = AtomicUsize::new(0);
    static CONTROLLER_SCOPED_IDS: Mutex<Vec<usize>> = Mutex::new(Vec::new());

    fn middleware_init_test_code(code: &'static str) -> MiddlewareErrorCode {
        MiddlewareErrorCode::new(code).expect("test middleware code is valid")
    }

    struct DiAwareMiddleware {
        _config: Arc<ConfigService>,
    }

    #[async_trait::async_trait]
    impl HttpMiddleware for DiAwareMiddleware {
        async fn new(extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            DI_MIDDLEWARE_INITIALIZATIONS.fetch_add(1, Ordering::AcqRel);
            Ok(Self {
                _config: extensions.get_service::<ConfigService>(None).await?,
            })
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("di_aware", lily_middleware::MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: lily_middleware::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            DI_MIDDLEWARE_REQUESTS.fetch_add(1, Ordering::AcqRel);
            next.run(exchange).await
        }
    }

    struct SessionPreparationMiddleware;

    #[async_trait::async_trait]
    impl HttpMiddleware for SessionPreparationMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            SESSION_MIDDLEWARE_EVENTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("session_init");
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("session", lily_middleware::MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: lily_middleware::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            SESSION_MIDDLEWARE_EVENTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("session_before");
            next.run(exchange).await?;
            SESSION_MIDDLEWARE_EVENTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("session_after");
            Ok(())
        }
    }

    struct OrderedApplicationMiddleware;

    #[async_trait::async_trait]
    impl HttpMiddleware for OrderedApplicationMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            SESSION_MIDDLEWARE_EVENTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("application_init");
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("application", lily_middleware::MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: lily_middleware::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            SESSION_MIDDLEWARE_EVENTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("application_before");
            next.run(exchange).await?;
            SESSION_MIDDLEWARE_EVENTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("application_after");
            Ok(())
        }
    }

    struct OrderedRouteMiddleware {
        _config: Arc<ConfigService>,
    }

    struct RouteApplicationMiddleware;

    #[async_trait::async_trait]
    impl HttpMiddleware for RouteApplicationMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(
                "route_test_application",
                lily_middleware::MiddlewareKind::Custom,
            )
        }

        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: lily_middleware::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            ROUTE_MIDDLEWARE_EVENTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("application_before");
            next.run(exchange).await?;
            ROUTE_MIDDLEWARE_EVENTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("application_after");
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl HttpMiddleware for OrderedRouteMiddleware {
        async fn new(extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            ROUTE_MIDDLEWARE_INITIALIZATIONS.fetch_add(1, Ordering::AcqRel);
            Ok(Self {
                _config: extensions.get_service::<ConfigService>(None).await?,
            })
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("route", lily_middleware::MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: lily_middleware::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            ROUTE_MIDDLEWARE_EVENTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("route_before");
            next.run(exchange).await?;
            ROUTE_MIDDLEWARE_EVENTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("route_after");
            Ok(())
        }
    }

    struct OrderingGuard;

    struct RejectingRouteMiddleware;

    struct OrderedActionRouteMiddleware;

    #[async_trait::async_trait]
    impl HttpMiddleware for OrderedActionRouteMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("route_action", lily_middleware::MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: lily_middleware::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            ROUTE_MIDDLEWARE_EVENTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("action_before");
            next.run(exchange).await?;
            ROUTE_MIDDLEWARE_EVENTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("action_after");
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl HttpMiddleware for RejectingRouteMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("rejecting_route", lily_middleware::MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            _exchange: &mut HttpExchange<'_>,
            _next: lily_middleware::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            let code = MiddlewareErrorCode::new("ROUTE_REJECTED").unwrap();
            Err(HttpMiddlewareError::rejected(
                lily_middleware::HttpMiddlewareRejection::new(429, code).unwrap(),
            ))
        }
    }

    #[async_trait::async_trait]
    impl GuardTrait for OrderingGuard {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, crate::guard::GuardInitError> {
            Ok(Self)
        }

        async fn can_activate(
            &self,
            _request: &mut Request,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::guard::GuardRejection> {
            ROUTE_MIDDLEWARE_EVENTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("guard");
            Ok(())
        }
    }

    fn ordered_handler<'a>(
        _extensions: Arc<Extensions>,
        _request: &'a mut Request,
        _response: &'a mut Response,
    ) -> Pin<Box<dyn Future<Output = Result<(), HttpApiError>> + Send + 'a>> {
        Box::pin(async move {
            ROUTE_MIDDLEWARE_EVENTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push("handler");
            Ok(())
        })
    }

    fn rejected_route_handler<'a>(
        _extensions: Arc<Extensions>,
        _request: &'a mut Request,
        _response: &'a mut Response,
    ) -> Pin<Box<dyn Future<Output = Result<(), HttpApiError>> + Send + 'a>> {
        Box::pin(async move {
            REJECTED_ROUTE_TERMINAL_CALLS.fetch_add(1, Ordering::AcqRel);
            Ok(())
        })
    }

    struct RequestLocalSessionMiddleware;

    #[async_trait::async_trait]
    impl HttpMiddleware for RequestLocalSessionMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(
                "request_local_session",
                lily_middleware::MiddlewareKind::Custom,
            )
        }

        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: lily_middleware::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            if let Some(value) = exchange.request().header_value("x-test-session") {
                if let Ok(session_id) = lily_middleware::CsrfSessionId::new(value) {
                    exchange.request_mut().local_mut().insert(session_id);
                }
            }
            next.run(exchange).await
        }
    }

    struct FailingInitializationMiddleware;

    #[async_trait::async_trait]
    impl HttpMiddleware for FailingInitializationMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            Err(HttpMiddlewareInitError::invalid_configuration(
                middleware_init_test_code("TEST_MIDDLEWARE_CONFIG_INVALID"),
            ))
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            unreachable!("a failed constructor cannot publish a descriptor")
        }

        async fn handle(
            &self,
            _exchange: &mut HttpExchange<'_>,
            _next: lily_middleware::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            unreachable!("a failed constructor cannot handle a request")
        }
    }

    struct PanickingInitializationMiddleware;

    #[async_trait::async_trait]
    impl HttpMiddleware for PanickingInitializationMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            panic!("private middleware initialization detail")
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            unreachable!("a panicked constructor cannot publish a descriptor")
        }

        async fn handle(
            &self,
            _exchange: &mut HttpExchange<'_>,
            _next: lily_middleware::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            unreachable!("a panicked constructor cannot handle a request")
        }
    }

    #[derive(Default, Injectable)]
    #[service(lifetime = "Scoped")]
    struct ScopedMiddlewareDependency;

    impl ServiceTrait for ScopedMiddlewareDependency {}

    #[derive(Default, Injectable)]
    #[service(lifetime = "Scoped")]
    struct ControllerScopedDependency {
        sequence: usize,
    }

    #[async_trait::async_trait]
    impl ServiceTrait for ControllerScopedDependency {
        async fn initialize(&mut self) -> Result<(), lily_error::injection::InjectionError> {
            self.sequence = CONTROLLER_SCOPED_INITIALIZATIONS.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(())
        }
    }

    struct ProviderController {
        extensions: Arc<Extensions>,
    }

    #[async_trait::async_trait]
    impl ControllerTrait for ProviderController {
        async fn new(extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
            CONTROLLER_PROVIDER_INITIALIZATIONS.fetch_add(1, Ordering::SeqCst);
            Ok(Self { extensions })
        }
    }

    struct ProviderControllerAction {
        controller: Arc<ProviderController>,
    }

    impl HttpAction for ProviderControllerAction {
        fn call<'a>(
            &'a self,
            _extensions: Arc<Extensions>,
            request: &'a mut Request,
            response: &'a mut Response,
        ) -> HttpActionFuture<'a> {
            Box::pin(async move {
                let result: Result<(), HttpApiError> = async {
                    let first = self
                        .controller
                        .extensions
                        .get_service::<ControllerScopedDependency>(None)
                        .await?;
                    let second = self
                        .controller
                        .extensions
                        .get_service::<ControllerScopedDependency>(None)
                        .await?;
                    if first.sequence != second.sequence {
                        return Err(HttpApiError::InternalError(
                            "request-scoped controller dependency was not reused".to_string(),
                        ));
                    }
                    CONTROLLER_SCOPED_IDS
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .push(first.sequence);
                    response.status_code(204, "No Content");
                    Ok(())
                }
                .await;
                result.write_to_response(response, request).await
            })
        }
    }

    fn bind_provider_controller(
        controller: ErasedController,
    ) -> Result<Handler, ControllerBindingError> {
        Ok(Handler::from_action(
            ProviderControllerAction {
                controller: downcast_controller::<ProviderController>(controller)?,
            },
            false,
        ))
    }

    struct ScopedDependencyMiddleware;
    struct ScopedDependencyCorsOriginResolver;

    #[async_trait::async_trait]
    impl HttpMiddleware for ScopedDependencyMiddleware {
        async fn new(extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            let _dependency = extensions
                .get_service::<ScopedMiddlewareDependency>(None)
                .await?;
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("scoped_dependency", lily_middleware::MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: lily_middleware::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            next.run(exchange).await
        }
    }

    #[async_trait::async_trait]
    impl lily_middleware::CorsOriginResolver for ScopedDependencyCorsOriginResolver {
        async fn new(
            extensions: Arc<Extensions>,
        ) -> Result<Self, lily_middleware::CorsOriginResolverInitError> {
            let _dependency = extensions
                .get_service::<ScopedMiddlewareDependency>(None)
                .await?;
            Ok(Self)
        }

        async fn allows(
            &self,
            _context: &lily_middleware::CorsOriginContext<'_>,
        ) -> Result<bool, lily_middleware::CorsOriginResolverError> {
            Ok(false)
        }
    }

    struct TestSecretResolver;

    #[async_trait::async_trait]
    impl SecretResolver for TestSecretResolver {
        async fn resolve(&self, _key: &str) -> Result<String, lily_config::ConfigError> {
            unreachable!("container/bootstrap conflict must be rejected before config loading")
        }
    }

    #[tokio::test]
    async fn caller_owned_container_cannot_be_combined_with_secret_resolver() {
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let error = AppBuilder::new("127.0.0.1:0")
            .container(Arc::clone(&container))
            .secret_resolver(TestSecretResolver)
            .build()
            .await
            .err()
            .expect("ambiguous bootstrap ownership must fail");
        assert!(matches!(
            error,
            AppBuildError::ConflictingContainerBootstrap
        ));
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn typed_middleware_is_constructed_once_from_the_caller_owned_graph() {
        DI_MIDDLEWARE_INITIALIZATIONS.store(0, Ordering::Release);
        DI_MIDDLEWARE_REQUESTS.store(0, Ordering::Release);
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let app = AppBuilder::new("127.0.0.1:0")
            .container(Arc::clone(&container))
            .middleware::<DiAwareMiddleware>()
            .build()
            .await
            .unwrap();

        assert_eq!(DI_MIDDLEWARE_INITIALIZATIONS.load(Ordering::Acquire), 1);
        for _ in 0..2 {
            let mut request = Request::from_transport_parts(
                "GET".to_string(),
                "/missing".to_string(),
                Vec::new(),
                &[],
            )
            .await
            .unwrap();
            let mut response = Response::new().await.unwrap();
            app.handle_request(&mut request, &mut response)
                .await
                .unwrap();
        }
        assert_eq!(DI_MIDDLEWARE_INITIALIZATIONS.load(Ordering::Acquire), 1);
        assert_eq!(DI_MIDDLEWARE_REQUESTS.load(Ordering::Acquire), 2);

        drop(app);
        assert!(container.resolve::<ConfigService>(None).await.is_ok());
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn openapi_disabled_app_does_not_attach_an_openapi_service() {
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let app = AppBuilder::new("127.0.0.1:0")
            .container(Arc::clone(&container))
            .build()
            .await
            .unwrap();

        assert!(matches!(
            container.resolve::<OpenApiService>(None).await,
            Err(lily_error::injection::InjectionError::ServiceNotFound(_))
        ));

        drop(app);
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn failed_openapi_build_rolls_back_the_framework_handle_attachment() {
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let config = OpenApiConfig::new("Rollback API", "1.0.0").unwrap();
        let error = AppBuilder::new("127.0.0.1:0")
            .container(Arc::clone(&container))
            .openapi(config)
            .middleware::<FailingInitializationMiddleware>()
            .build()
            .await
            .err()
            .expect("middleware initialization must fail");
        assert!(matches!(
            error,
            AppBuildError::MiddlewareInitialization { .. }
        ));
        assert!(matches!(
            container.resolve::<OpenApiService>(None).await,
            Err(lily_error::injection::InjectionError::ServiceNotFound(_))
        ));
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn route_middleware_is_singleton_and_wraps_guards_and_handler_inside_application_chain() {
        ROUTE_MIDDLEWARE_INITIALIZATIONS.store(0, Ordering::Release);
        let mut app = AppBuilder::new("127.0.0.1:0")
            .middleware::<RouteApplicationMiddleware>()
            .build()
            .await
            .unwrap();
        ROUTE_MIDDLEWARE_EVENTS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();

        let guard_type_id = TypeId::of::<OrderingGuard>();
        let registration = HttpMiddlewareRegistration::of::<OrderedRouteMiddleware>();
        let action_registration = HttpMiddlewareRegistration::of::<OrderedActionRouteMiddleware>();
        let routes = vec![
            RouteInfo {
                method: "GET".to_string(),
                path: "/ordered".to_string(),
                handler: Handler::new(Arc::new(ordered_handler), false),
                handler_name: "ordered_handler".to_string(),
                guard_type_ids: vec![guard_type_id],
                middleware_registrations: vec![registration, action_registration],
                cors_policy_registration: CorsRoutePolicyRegistration::inherit(),
                route_plan_id: 0,
            },
            RouteInfo {
                method: "GET".to_string(),
                path: "/also-ordered".to_string(),
                handler: Handler::new(Arc::new(ordered_handler), false),
                handler_name: "also_ordered_handler".to_string(),
                guard_type_ids: Vec::new(),
                middleware_registrations: vec![registration],
                cors_policy_registration: CorsRoutePolicyRegistration::inherit(),
                route_plan_id: 0,
            },
        ];
        let route_table = RouteTable::from_routes(routes).unwrap();
        let plans = build_route_middleware_plans(
            &route_table,
            &app.middlewares,
            HashMap::new(),
            app.container.services(),
        )
        .await
        .unwrap();
        app.route_state = Arc::new(
            AppRouteState::with_route_middlewares(
                route_table,
                HashMap::from([(guard_type_id, Arc::new(OrderingGuard) as GuardInstance)]),
                plans,
            )
            .unwrap(),
        );

        assert_eq!(ROUTE_MIDDLEWARE_INITIALIZATIONS.load(Ordering::Acquire), 1);
        let mut request = Request::from_transport_parts(
            "GET".to_string(),
            "/ordered".to_string(),
            Vec::new(),
            &[],
        )
        .await
        .unwrap();
        let mut response = Response::new().await.unwrap();
        app.handle_request(&mut request, &mut response)
            .await
            .unwrap();
        assert_eq!(
            *ROUTE_MIDDLEWARE_EVENTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            vec![
                "application_before",
                "route_before",
                "action_before",
                "guard",
                "handler",
                "action_after",
                "route_after",
                "application_after",
            ]
        );

        ROUTE_MIDDLEWARE_EVENTS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        let mut missing = Request::from_transport_parts(
            "GET".to_string(),
            "/missing".to_string(),
            Vec::new(),
            &[],
        )
        .await
        .unwrap();
        let mut missing_response = Response::new().await.unwrap();
        app.handle_request(&mut missing, &mut missing_response)
            .await
            .unwrap();
        assert_eq!(
            *ROUTE_MIDDLEWARE_EVENTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            vec!["application_before", "application_after"]
        );
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn duplicate_route_middleware_type_is_a_typed_startup_failure() {
        let app = AppBuilder::new("127.0.0.1:0").build().await.unwrap();
        let registration = HttpMiddlewareRegistration::of::<OrderedRouteMiddleware>();
        let route_table = RouteTable::from_routes(vec![RouteInfo {
            method: "GET".to_string(),
            path: "/duplicate".to_string(),
            handler: Handler::new(Arc::new(ordered_handler), false),
            handler_name: "ordered_handler".to_string(),
            guard_type_ids: Vec::new(),
            middleware_registrations: vec![registration, registration],
            cors_policy_registration: CorsRoutePolicyRegistration::inherit(),
            route_plan_id: 0,
        }])
        .unwrap();
        let error = build_route_middleware_plans(
            &route_table,
            &app.middlewares,
            HashMap::new(),
            app.container.services(),
        )
        .await
        .err()
        .expect("duplicate route middleware must fail build");
        assert!(matches!(
            error,
            AppBuildError::RouteMiddleware(RouteMiddlewareBuildError {
                cause: RouteMiddlewareBuildErrorCause::DuplicateType,
                ..
            })
        ));
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn route_middleware_rejection_short_circuits_and_preserves_its_bounded_error_code() {
        REJECTED_ROUTE_TERMINAL_CALLS.store(0, Ordering::Release);
        let mut app = AppBuilder::new("127.0.0.1:0").build().await.unwrap();
        let route_table = RouteTable::from_routes(vec![RouteInfo {
            method: "GET".to_string(),
            path: "/limited".to_string(),
            handler: Handler::new(Arc::new(rejected_route_handler), false),
            handler_name: "rejected_route_handler".to_string(),
            guard_type_ids: Vec::new(),
            middleware_registrations: vec![HttpMiddlewareRegistration::of::<
                RejectingRouteMiddleware,
            >()],
            cors_policy_registration: CorsRoutePolicyRegistration::inherit(),
            route_plan_id: 0,
        }])
        .unwrap();
        let plans = build_route_middleware_plans(
            &route_table,
            &app.middlewares,
            HashMap::new(),
            app.container.services(),
        )
        .await
        .unwrap();
        app.route_state = Arc::new(
            AppRouteState::with_route_middlewares(route_table, HashMap::new(), plans).unwrap(),
        );

        let mut request = Request::from_transport_parts(
            "GET".to_string(),
            "/limited".to_string(),
            Vec::new(),
            &[],
        )
        .await
        .unwrap();
        let mut response = Response::new().await.unwrap();
        let outcome = app
            .handle_request_with_outcome(&mut request, &mut response)
            .await
            .unwrap();
        assert_eq!(outcome.status(), 429);
        assert_eq!(outcome.error_code(), Some("ROUTE_REJECTED"));
        assert_eq!(REJECTED_ROUTE_TERMINAL_CALLS.load(Ordering::Acquire), 0);
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn session_slot_brackets_csrf_before_application_middleware() {
        SESSION_MIDDLEWARE_EVENTS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        let app = AppBuilder::new("127.0.0.1:0")
            .middleware::<OrderedApplicationMiddleware>()
            .session_middleware::<SessionPreparationMiddleware>()
            .csrf(CsrfPolicy::cross_origin())
            .build()
            .await
            .unwrap();

        let mut request = Request::from_transport_parts(
            "GET".to_string(),
            "/missing".to_string(),
            Vec::new(),
            &[],
        )
        .await
        .unwrap();
        let mut response = Response::new().await.unwrap();
        app.handle_request(&mut request, &mut response)
            .await
            .unwrap();

        assert_eq!(
            *SESSION_MIDDLEWARE_EVENTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            vec![
                "session_init",
                "application_init",
                "session_before",
                "application_before",
                "application_after",
                "session_after",
            ]
        );

        SESSION_MIDDLEWARE_EVENTS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        let mut request = Request::from_transport_parts(
            "POST".to_string(),
            "/missing".to_string(),
            vec![
                lily_core::RawHeader {
                    name: "Host".to_string(),
                    value: "api.example".to_string(),
                    line_number: 1,
                    raw_line: String::new(),
                },
                lily_core::RawHeader {
                    name: "Origin".to_string(),
                    value: "https://evil.example".to_string(),
                    line_number: 2,
                    raw_line: String::new(),
                },
                lily_core::RawHeader {
                    name: "Sec-Fetch-Site".to_string(),
                    value: "cross-site".to_string(),
                    line_number: 3,
                    raw_line: String::new(),
                },
            ],
            &[],
        )
        .await
        .unwrap();
        let mut response = Response::new().await.unwrap();
        app.handle_request(&mut request, &mut response)
            .await
            .unwrap();
        assert_eq!(response.status_code_value(), 403);
        assert_eq!(
            *SESSION_MIDDLEWARE_EVENTS
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
            vec!["session_before", "session_after"]
        );
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn duplicate_session_middleware_fails_before_composition_root_build() {
        let error = AppBuilder::new("127.0.0.1:0")
            .session_middleware::<SessionPreparationMiddleware>()
            .session_middleware::<OrderedApplicationMiddleware>()
            .build()
            .await
            .err()
            .expect("the second session slot registration must fail startup");
        assert_eq!(error, AppBuildError::DuplicateSessionMiddleware);
    }

    #[tokio::test]
    async fn middleware_constructor_failure_is_typed_and_keeps_external_container_open() {
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let error = AppBuilder::new("127.0.0.1:0")
            .container(Arc::clone(&container))
            .middleware::<FailingInitializationMiddleware>()
            .build()
            .await
            .err()
            .expect("middleware initialization must fail");
        assert_eq!(
            error,
            AppBuildError::MiddlewareInitialization {
                index: 0,
                error: HttpMiddlewareInitError::invalid_configuration(middleware_init_test_code(
                    "TEST_MIDDLEWARE_CONFIG_INVALID"
                ),),
            }
        );
        assert!(container.resolve::<ConfigService>(None).await.is_ok());
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn middleware_constructor_panic_becomes_a_bounded_startup_error() {
        let error = AppBuilder::new("127.0.0.1:0")
            .middleware::<PanickingInitializationMiddleware>()
            .build()
            .await
            .err()
            .expect("middleware panic must not escape application build");
        assert_eq!(
            error,
            AppBuildError::MiddlewareInitialization {
                index: 0,
                error: HttpMiddlewareInitError::internal(middleware_init_test_code(
                    "MIDDLEWARE_INIT_PANICKED",
                )),
            }
        );
    }

    #[tokio::test]
    async fn scoped_dependency_resolution_in_constructor_fails_closed_at_build_time() {
        let error = AppBuilder::new("127.0.0.1:0")
            .middleware::<ScopedDependencyMiddleware>()
            .build()
            .await
            .err()
            .expect("a scoped dependency has no active request context during build");
        assert_eq!(
            error,
            AppBuildError::MiddlewareInitialization {
                index: 0,
                error: HttpMiddlewareInitError::dependency(middleware_init_test_code(
                    "MIDDLEWARE_SCOPE_REQUIRED",
                )),
            }
        );
    }

    #[tokio::test]
    async fn scoped_cors_resolver_dependency_fails_before_listener_publication() {
        let error = AppBuilder::new("127.0.0.1:0")
            .cors(
                CorsPolicy::new()
                    .resolve_origins_with::<ScopedDependencyCorsOriginResolver>()
                    .allow_methods(["GET"]),
            )
            .build()
            .await
            .err()
            .expect("a dynamic CORS resolver cannot resolve a scoped dependency at build time");
        let AppBuildError::CorsOriginResolverInitialization(error) = error else {
            panic!("unexpected dynamic CORS build error: {error}");
        };
        assert_eq!(
            error.diagnostic_code(),
            "CORS_ORIGIN_RESOLVER_SCOPE_REQUIRED"
        );
    }

    #[test]
    fn metric_method_labels_are_closed_over_standard_methods() {
        for method in [
            "CONNECT", "DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT", "TRACE",
        ] {
            assert_eq!(http_method_metric_label(method), method);
        }
        for extension in ["PURGE", "SEARCH", "get", "PRIVATE-METHOD-SENTINEL"] {
            assert_eq!(http_method_metric_label(extension), "_OTHER");
        }
    }

    fn noop_handler<'a>(
        _extensions: Arc<Extensions>,
        _request: &'a mut Request,
        _response: &'a mut Response,
    ) -> Pin<Box<dyn Future<Output = Result<(), HttpApiError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    struct TypedTestAction(
        for<'a> fn(Arc<Extensions>, &'a mut Request, &'a mut Response) -> HttpActionFuture<'a>,
    );

    impl HttpAction for TypedTestAction {
        fn call<'a>(
            &'a self,
            extensions: Arc<Extensions>,
            request: &'a mut Request,
            response: &'a mut Response,
        ) -> HttpActionFuture<'a> {
            (self.0)(extensions, request, response)
        }
    }

    fn written_unit_handler<'a>(
        _extensions: Arc<Extensions>,
        request: &'a mut Request,
        response: &'a mut Response,
    ) -> HttpActionFuture<'a> {
        Box::pin(async move {
            response.status(201, "Created");
            response.try_insert_header("X-Handler", "retained")?;
            response.write_body(b"created")?;
            Result::<(), HttpApiError>::Ok(())
                .write_to_response(response, request)
                .await
        })
    }

    struct FailingHandlerSerialize;

    impl serde::Serialize for FailingHandlerSerialize {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            Err(serde::ser::Error::custom(
                "credential=secret path=/srv/private",
            ))
        }
    }

    fn serialization_failure_handler<'a>(
        _extensions: Arc<Extensions>,
        request: &'a mut Request,
        response: &'a mut Response,
    ) -> HttpActionFuture<'a> {
        Box::pin(async move {
            response.status(201, "Created");
            response.try_insert_header("X-Partial", "credential=secret")?;
            response.write_body(b"credential=secret path=/srv/private")?;
            Result::<FailingHandlerSerialize, HttpApiError>::Ok(FailingHandlerSerialize)
                .write_to_response(response, request)
                .await
        })
    }

    fn ignored_body_limit_handler<'a>(
        _extensions: Arc<Extensions>,
        _request: &'a mut Request,
        response: &'a mut Response,
    ) -> Pin<Box<dyn Future<Output = Result<(), HttpApiError>> + Send + 'a>> {
        Box::pin(async move {
            response.status(201, "Created");
            let oversized = vec![b'x'; 257];
            let _ = response.write_body(&oversized);
            Ok(())
        })
    }

    fn localized_error_handler<'a>(
        _extensions: Arc<Extensions>,
        request: &'a mut Request,
        response: &'a mut Response,
    ) -> HttpActionFuture<'a> {
        Box::pin(async move {
            let result: Result<String, HttpApiError> = Err(HttpApiError::BadRequest(
                "internal detail must remain private".to_string(),
            ));
            result.write_to_response(response, request).await
        })
    }

    fn test_catalog(message: &str) -> LocalizationCatalog {
        LocalizationCatalog::from_translations(HashMap::from([(
            "tr".to_string(),
            HashMap::from([("BAD_REQUEST".to_string(), message.to_string())]),
        )]))
        .unwrap()
    }

    async fn close_test_app(app: App) {
        let container = Arc::clone(&app.container);
        drop(app);
        container
            .close_with_timeout(Duration::from_secs(1))
            .await
            .expect("test composition root must close cleanly");
    }

    fn tls_identity_files(directory: &TempDir) -> (PathBuf, PathBuf) {
        let certified = generate_simple_self_signed(["localhost".to_owned()])
            .expect("generate test TLS identity");
        let certificate_path = directory.path().join("server-cert.pem");
        let private_key_path = directory.path().join("server-key.pem");
        std::fs::write(&certificate_path, certified.cert.pem()).expect("write test certificate");
        std::fs::write(&private_key_path, certified.signing_key.serialize_pem())
            .expect("write test private key");
        (certificate_path, private_key_path)
    }

    fn failing_handler<'a>(
        _extensions: Arc<Extensions>,
        _request: &'a mut Request,
        _response: &'a mut Response,
    ) -> Pin<Box<dyn Future<Output = Result<(), HttpApiError>> + Send + 'a>> {
        Box::pin(async {
            Err(HttpApiError::HandlerError(
                "sensitive internal handler detail".to_string(),
            ))
        })
    }

    fn route(
        method: &str,
        path: &str,
        handler_name: &str,
        guard_type_ids: Vec<TypeId>,
    ) -> RouteInfo {
        RouteInfo {
            method: method.to_string(),
            path: path.to_string(),
            handler: Handler::new(Arc::new(noop_handler), false),
            handler_name: handler_name.to_string(),
            guard_type_ids,
            middleware_registrations: Vec::new(),
            cors_policy_registration: CorsRoutePolicyRegistration::inherit(),
            route_plan_id: 0,
        }
    }

    struct TestGuard;

    #[async_trait::async_trait]
    impl GuardTrait for TestGuard {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, crate::guard::GuardInitError> {
            Ok(Self)
        }

        async fn can_activate(
            &self,
            _request: &mut Request,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::guard::GuardRejection> {
            Ok(())
        }
    }

    struct MissingGuard;

    struct RouteSessionGuard;

    #[async_trait::async_trait]
    impl GuardTrait for RouteSessionGuard {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, crate::guard::GuardInitError> {
            Ok(Self)
        }

        async fn can_activate(
            &self,
            request: &mut Request,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::guard::GuardRejection> {
            request
                .local_mut()
                .insert(lily_middleware::CsrfSessionId::new("session-a").unwrap());
            Ok(())
        }
    }

    struct DenyGuard;

    #[async_trait::async_trait]
    impl GuardTrait for DenyGuard {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, crate::guard::GuardInitError> {
            Ok(Self)
        }

        async fn can_activate(
            &self,
            _request: &mut Request,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::guard::GuardRejection> {
            Err(crate::guard::GuardRejection::forbidden(
                lily_web_core::HttpErrorCode::new("FORBIDDEN").expect("static guard code is valid"),
            )
            .expect("403 guard rejection is valid"))
        }
    }

    struct ResponseMarker;

    #[async_trait::async_trait]
    impl HttpMiddleware for ResponseMarker {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("response_marker", lily_middleware::MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: lily_middleware::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            next.run(exchange).await?;
            exchange
                .response_mut()
                .try_insert_header("X-Lily-Middleware", "unwound")
                .map_err(|_| HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL))?;
            Ok(())
        }
    }

    struct PrewriteSecret;

    #[async_trait::async_trait]
    impl HttpMiddleware for PrewriteSecret {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("prewrite_secret", lily_middleware::MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: lily_middleware::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            exchange
                .response_mut()
                .try_insert_header("X-Untrusted-Prefix", "credential=secret")
                .map_err(|_| HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL))?;
            exchange
                .response_mut()
                .write_body(b"credential=secret")
                .map_err(|_| HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL))?;
            next.run(exchange).await
        }
    }

    struct SuccessStatusOverride;

    #[async_trait::async_trait]
    impl HttpMiddleware for SuccessStatusOverride {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(
                "success_status_override",
                lily_middleware::MiddlewareKind::Custom,
            )
        }

        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: lily_middleware::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            next.run(exchange).await?;
            exchange.response_mut().status_code(200, "OK");
            Ok(())
        }
    }

    struct RequestContext(String);

    struct RequestContextMiddleware;

    #[async_trait::async_trait]
    impl HttpMiddleware for RequestContextMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("request_context", lily_middleware::MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: lily_middleware::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            let value = exchange
                .request()
                .header_value("X-Request-Context")
                .unwrap_or("missing")
                .to_owned();
            exchange
                .request_mut()
                .local_mut()
                .insert(RequestContext(value));
            next.run(exchange).await
        }
    }

    fn request_context_handler<'a>(
        _extensions: Arc<Extensions>,
        request: &'a mut Request,
        response: &'a mut Response,
    ) -> Pin<Box<dyn Future<Output = Result<(), HttpApiError>> + Send + 'a>> {
        Box::pin(async move {
            let value = request
                .local()
                .get::<RequestContext>()
                .ok_or_else(|| {
                    HttpApiError::HandlerError(
                        "request-local middleware context was unavailable".to_string(),
                    )
                })?
                .0
                .clone();
            response.status_code(200, "OK");
            response
                .try_insert_header("Content-Type", "text/plain; charset=utf-8")
                .map_err(|_| {
                    HttpApiError::ResponseEncodingError(
                        "request-local response header was rejected".to_string(),
                    )
                })?;
            response.write_body(value.as_bytes())?;
            Ok(())
        })
    }

    #[derive(Clone, Copy)]
    enum FailureStage {
        Rejection,
        Before,
        After,
    }

    struct FailingMiddleware<const STAGE: u8>;

    #[async_trait::async_trait]
    impl<const STAGE: u8> HttpMiddleware for FailingMiddleware<STAGE> {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("failing", lily_middleware::MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: lily_middleware::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            let error = || {
                HttpMiddlewareError::internal(
                    MiddlewareErrorCode::new("TEST_MIDDLEWARE_FAILED").unwrap(),
                )
            };
            match STAGE {
                0 => Err(lily_middleware::HttpMiddlewareRejection::new(
                    403,
                    MiddlewareErrorCode::new("TEST_MIDDLEWARE_REJECTED").unwrap(),
                )
                .unwrap()
                .into()),
                1 => Err(error()),
                2 => {
                    next.run(exchange).await?;
                    Err(error())
                }
                _ => unreachable!("test middleware stage is bounded by its registrations"),
            }
        }
    }

    static COUNTING_DESCRIPTOR_CALLS: AtomicUsize = AtomicUsize::new(0);

    struct CountingDescriptorMiddleware;

    #[async_trait::async_trait]
    impl HttpMiddleware for CountingDescriptorMiddleware {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            COUNTING_DESCRIPTOR_CALLS.fetch_add(1, Ordering::AcqRel);
            MiddlewareDescriptor::new("counted", lily_middleware::MiddlewareKind::Custom)
        }

        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: lily_middleware::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            next.run(exchange).await
        }
    }

    struct TestRateLimitRejection;

    #[async_trait::async_trait]
    impl HttpMiddleware for TestRateLimitRejection {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(
                "test_rate_limit",
                lily_middleware::MiddlewareKind::RateLimit,
            )
        }

        async fn handle(
            &self,
            _exchange: &mut HttpExchange<'_>,
            _next: lily_middleware::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            let rejection = lily_middleware::HttpMiddlewareRejection::new(
                429,
                MiddlewareErrorCode::new("RATE_LIMIT_EXCEEDED").unwrap(),
            )
            .and_then(|rejection| rejection.with_retry_after(Duration::from_secs(3600)))
            .and_then(|rejection| {
                rejection.with_problem_detail("The application rate limit was exceeded.")
            })
            .and_then(|rejection| rejection.with_header("X-RateLimit-Limit", "100"))
            .map_err(|_| HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL))?;
            Err(rejection.into())
        }
    }

    #[test]
    fn duplicate_route_is_a_typed_build_error() {
        let error = build_route_table(vec![
            route("GET", "/users/:id", "first", Vec::new()),
            route("GET", "/users/:user_id", "duplicate", Vec::new()),
        ])
        .expect_err("equivalent parameter routes must fail application startup");

        match error {
            AppBuildError::DuplicateRoute(error) => {
                assert_eq!(error.method, "GET");
                assert_eq!(error.first_handler, "first");
                assert_eq!(error.duplicate_handler, "duplicate");
            }
            other => panic!("expected typed duplicate-route error, got {other:?}"),
        }
    }

    #[test]
    fn invalid_and_equal_precedence_routes_are_typed_build_errors() {
        assert!(matches!(
            build_route_table(vec![route("GET", "users/:id", "invalid", Vec::new())]),
            Err(AppBuildError::InvalidRoute(_))
        ));

        assert!(matches!(
            build_route_table(vec![
                route("GET", "/a/:x", "left", Vec::new()),
                route("GET", "/:y/b", "right", Vec::new()),
            ]),
            Err(AppBuildError::AmbiguousRoute(_))
        ));
    }

    #[test]
    fn effective_http_server_config_applies_central_values_without_builder_overrides() {
        let config = lily_config::ServerConfig {
            host: "0.0.0.0".to_string(),
            port: 9090,
            max_connections: Some(321),
            max_multipart_part_bytes: Some(4096),
            max_multipart_parts: Some(17),
            max_multipart_metadata_bytes: Some(2048),
            connection_idle_timeout_secs: Some(17),
            request_timeout_secs: Some(23),
            ..lily_config::ServerConfig::default()
        };

        let address =
            EffectiveHttpServerConfig::resolve_address(&HttpAddressSource::Config, &config)
                .expect("central listener address");
        let transport =
            EffectiveHttpServerConfig::resolve_transport(&HttpTransportSource::Config, &config)
                .expect("central transport");

        assert_eq!(address, "0.0.0.0:9090");
        assert_eq!(transport.max_connections, 321);
        assert_eq!(transport.max_multipart_part_bytes, 4096);
        assert_eq!(transport.max_multipart_parts, 17);
        assert_eq!(transport.max_multipart_metadata_bytes, 2048);
        assert_eq!(transport.connection_idle_timeout, Duration::from_secs(17));
        assert_eq!(transport.request_timeout, Duration::from_secs(23));
    }

    #[test]
    fn explicit_builder_values_win_while_unspecified_values_still_use_central_config() {
        let config = lily_config::ServerConfig {
            host: "0.0.0.0".to_string(),
            port: 9090,
            max_connections: Some(321),
            max_multipart_part_bytes: Some(4096),
            max_multipart_parts: Some(17),
            max_multipart_metadata_bytes: Some(2048),
            connection_idle_timeout_secs: Some(17),
            request_timeout_secs: Some(23),
            ..lily_config::ServerConfig::default()
        };
        let builder = AppBuilder::new("127.0.0.1:0");

        let address = EffectiveHttpServerConfig::resolve_address(&builder.address_source, &config)
            .expect("explicit listener address");
        let central_transport =
            EffectiveHttpServerConfig::resolve_transport(&builder.transport_source, &config)
                .expect("central transport remains active");
        assert_eq!(address, "127.0.0.1:0");
        assert_eq!(central_transport.max_connections, 321);
        assert_eq!(central_transport.max_multipart_part_bytes, 4096);
        assert_eq!(central_transport.max_multipart_parts, 17);
        assert_eq!(central_transport.max_multipart_metadata_bytes, 2048);

        let explicit_transport = HttpTransportConfig {
            max_connections: 7,
            connection_idle_timeout: Duration::from_secs(41),
            request_timeout: Duration::from_secs(43),
            ..HttpTransportConfig::default()
        };
        let builder = builder.transport_config(explicit_transport.clone());
        let resolved =
            EffectiveHttpServerConfig::resolve_transport(&builder.transport_source, &config)
                .expect("explicit transport");
        assert_eq!(resolved.max_connections, 7);
        assert_eq!(resolved.connection_idle_timeout, Duration::from_secs(41));
        assert_eq!(resolved.request_timeout, Duration::from_secs(43));
    }

    #[test]
    fn invalid_effective_listener_and_transport_values_are_typed() {
        let empty_host = lily_config::ServerConfig {
            host: "  ".to_string(),
            ..lily_config::ServerConfig::default()
        };
        assert_eq!(
            EffectiveHttpServerConfig::resolve_address(&HttpAddressSource::Config, &empty_host,),
            Err(HttpServerConfigError::EmptyHost)
        );

        let zero_port = lily_config::ServerConfig {
            port: 0,
            ..lily_config::ServerConfig::default()
        };
        assert_eq!(
            EffectiveHttpServerConfig::resolve_address(&HttpAddressSource::Config, &zero_port,),
            Err(HttpServerConfigError::ZeroPort)
        );
        assert_eq!(
            EffectiveHttpServerConfig::resolve_address(
                &HttpAddressSource::Explicit("127.0.0.1:8080".to_string()),
                &zero_port,
            ),
            Ok("127.0.0.1:8080".to_string())
        );

        let invalid_transport = lily_config::ServerConfig {
            max_connections: Some(0),
            ..lily_config::ServerConfig::default()
        };
        assert!(matches!(
            EffectiveHttpServerConfig::resolve_transport(
                &HttpTransportSource::Config,
                &invalid_transport,
            ),
            Err(HttpServerConfigError::InvalidTransport(_))
        ));
    }

    #[tokio::test]
    async fn canonical_server_tls_fields_are_applied_fail_closed() {
        let disabled = lily_config::ServerConfig::default();
        assert!(HttpTlsSource::Config
            .resolve(&disabled)
            .await
            .expect("default server config is plaintext")
            .is_none());

        let invalid_configured_tls = lily_config::ServerConfig {
            tls_enabled: Some(true),
            tls_cert_path: Some("must-not-be-read.pem".into()),
            ..lily_config::ServerConfig::default()
        };
        assert!(HttpTlsSource::Disabled
            .resolve(&invalid_configured_tls)
            .await
            .expect("explicit TLS disable must bypass configured TLS material")
            .is_none());

        let enabled_without_identity = lily_config::ServerConfig {
            tls_enabled: Some(true),
            ..lily_config::ServerConfig::default()
        };
        assert_eq!(
            HttpTlsSource::Config
                .resolve(&enabled_without_identity)
                .await
                .expect_err("enabled TLS requires an identity"),
            HttpTlsConfigError::CertificatePathRequired
        );

        let directory = TempDir::new().expect("create TLS fixture directory");
        let (certificate_path, private_key_path) = tls_identity_files(&directory);
        let material_while_disabled = lily_config::ServerConfig {
            tls_cert_path: Some(certificate_path.clone()),
            tls_key_path: Some(private_key_path.clone()),
            ..lily_config::ServerConfig::default()
        };
        assert_eq!(
            HttpTlsSource::Config
                .resolve(&material_while_disabled)
                .await
                .expect_err("dormant TLS material must not be ignored"),
            HttpTlsConfigError::CertificateMaterialWhileDisabled
        );

        let enabled = lily_config::ServerConfig {
            tls_enabled: Some(true),
            tls_cert_path: Some(certificate_path),
            tls_key_path: Some(private_key_path),
            ..lily_config::ServerConfig::default()
        };
        assert!(HttpTlsSource::Config
            .resolve(&enabled)
            .await
            .expect("valid canonical TLS identity must load")
            .is_some());
    }

    #[tokio::test]
    async fn explicit_rustls_config_is_owned_by_the_built_application() {
        let directory = TempDir::new().expect("create TLS fixture directory");
        let (certificate_path, private_key_path) = tls_identity_files(&directory);
        let tls = RustlsConfig::from_pem_file(certificate_path, private_key_path)
            .await
            .expect("load explicit TLS identity");
        let invalid_central_tls = lily_config::ServerConfig {
            tls_enabled: Some(true),
            ..lily_config::ServerConfig::default()
        };
        assert!(HttpTlsSource::Explicit(tls.clone())
            .resolve(&invalid_central_tls)
            .await
            .expect("explicit Rustls must bypass incomplete central TLS fields")
            .is_some());

        let app = AppBuilder::new("127.0.0.1:0")
            .rustls_config(tls)
            .build()
            .await
            .expect("explicit Rustls configuration must build");

        assert!(app.rustls_config().is_some());
        close_test_app(app).await;
    }

    #[test]
    fn missing_guard_rejects_route_state_publication() {
        let guard_type_id = TypeId::of::<MissingGuard>();
        let route_table = RouteTable::from_routes(vec![route(
            "POST",
            "/admin",
            "admin_handler",
            vec![guard_type_id],
        )])
        .expect("route itself is valid");

        let error = AppRouteState::new(route_table, HashMap::new())
            .err()
            .expect("missing guard must fail before the app is published");

        assert_eq!(error.method, "POST");
        assert_eq!(error.path, "/admin");
        assert_eq!(error.handler, "admin_handler");
        assert_eq!(error.guard_type_id, guard_type_id);
    }

    #[tokio::test]
    async fn cloned_application_route_state_retains_routes_and_guards() {
        let guard_type_id = TypeId::of::<TestGuard>();
        let route_table = RouteTable::from_routes(vec![route(
            "GET",
            "/guarded",
            "guarded_handler",
            vec![guard_type_id],
        )])
        .expect("route itself is valid");
        let mut guards = HashMap::new();
        let guard: GuardInstance = Arc::new(TestGuard);
        guards.insert(guard_type_id, guard);

        let route_state = Arc::new(
            AppRouteState::new(route_table, guards).expect("guard graph must be complete"),
        );
        let mut app = AppBuilder::new("127.0.0.1:0")
            .build()
            .await
            .expect("test composition root must build without opening a listener");
        app.route_state = route_state;
        let cloned_app = app.clone();

        assert!(Arc::ptr_eq(&app.route_state, &cloned_app.route_state));
        let guarded_route = cloned_app
            .route_state
            .route_table
            .find_route("GET", "/guarded")
            .expect("cloned state must retain the route");
        assert!(
            cloned_app
                .route_state
                .guard_for_route(guarded_route, &guard_type_id)
                .is_ok(),
            "cloned state must retain the validated guard instance"
        );

        let container = Arc::clone(&app.container);
        drop(cloned_app);
        drop(app);
        container
            .close_with_timeout(Duration::from_secs(1))
            .await
            .expect("test composition root must close cleanly");
    }

    #[test]
    fn runtime_missing_guard_fallback_is_a_deterministic_server_error() {
        let guard_type_id = TypeId::of::<MissingGuard>();
        let guarded_route = route("GET", "/guarded", "guarded_handler", vec![guard_type_id]);
        let invalid_state = AppRouteState {
            route_table: RouteTable::default(),
            guard_instances: HashMap::new(),
            middleware_plans: Vec::new(),
        };
        let missing = invalid_state
            .guard_for_route(&guarded_route, &guard_type_id)
            .err()
            .expect("corrupt runtime state must not skip a configured guard");
        let error = missing_route_guard_http_error(&missing);

        assert_eq!(error.http_status(), (500, "Internal Server Error"));
        assert_eq!(
            error,
            HttpApiError::InitializationError(
                "required HTTP route guard is unavailable".to_string()
            )
        );
    }

    #[tokio::test]
    async fn guard_rejections_use_the_shared_problem_details_contract() {
        let code = |value| lily_web_core::HttpErrorCode::new(value).unwrap();
        let cases = [
            (
                crate::guard::GuardRejection::unauthorized(
                    code("UNAUTHENTICATED"),
                    "Bearer realm=api",
                )
                .unwrap(),
                401,
                Some("Bearer realm=api"),
                "UNAUTHENTICATED",
            ),
            (
                crate::guard::GuardRejection::forbidden(code("FORBIDDEN")).unwrap(),
                403,
                None,
                "FORBIDDEN",
            ),
            (
                crate::guard::GuardRejection::service_unavailable(code(
                    "GUARD_DEPENDENCY_UNAVAILABLE",
                ))
                .unwrap(),
                503,
                None,
                "GUARD_DEPENDENCY_UNAVAILABLE",
            ),
        ];

        for (rejection, expected_status, expected_challenge, expected_code) in cases {
            let mut response = Response::new().await.unwrap();
            write_guard_rejection(&mut response, rejection)
                .await
                .unwrap();
            let parts = response.into_transport_parts().unwrap();
            assert_eq!(parts.status, expected_status);
            assert_eq!(
                parts
                    .headers
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case("www-authenticate"))
                    .map(|(_, value)| value.as_str()),
                expected_challenge
            );
            assert!(parts.headers.iter().any(|(name, value)| {
                name.eq_ignore_ascii_case("content-type") && value == "application/problem+json"
            }));
            let body: serde_json::Value = serde_json::from_slice(&parts.body).unwrap();
            assert_eq!(body["status"], expected_status);
            assert_eq!(body["code"], expected_code);
        }
    }

    #[test]
    fn guard_initialization_failure_is_a_typed_build_error() {
        let error = crate::guard::GuardInitError::MissingConfiguration {
            guard: "ConfiguredGuard",
            setting: "AppBuilder::guard(ConfiguredGuard::new(...))",
        };
        assert_eq!(
            AppBuildError::from(error.clone()),
            AppBuildError::GuardInitialization(error)
        );
    }

    #[tokio::test]
    async fn unsafe_cors_configuration_fails_before_composition_root_build() {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reservation.local_addr().unwrap();
        drop(reservation);

        let error = AppBuilder::new(&address.to_string())
            .cors(
                lily_middleware::CorsPolicy::new()
                    .allow_any_origin()
                    .allow_credentials(true),
            )
            .build()
            .await
            .err()
            .expect("wildcard plus credentials must fail startup");
        assert_eq!(
            error,
            AppBuildError::InvalidCorsConfiguration(
                lily_middleware::MiddlewareConfigError::middleware(
                    lily_middleware::MiddlewareErrorCode::new("CORS_CREDENTIALS_WILDCARD").unwrap(),
                ),
            )
        );
        let rebound = std::net::TcpListener::bind(address)
            .expect("invalid middleware policy must fail before opening the listener");
        drop(rebound);
    }

    #[tokio::test]
    async fn invalid_and_duplicate_csrf_policies_fail_before_publication() {
        let invalid = AppBuilder::new("127.0.0.1:0")
            .csrf(CsrfPolicy::cross_origin().trust_origins(["https://example.com/path"]))
            .build()
            .await
            .err()
            .expect("a non-origin URL must fail startup");
        assert!(matches!(
            invalid,
            AppBuildError::InvalidCsrfConfiguration(_)
        ));

        let duplicate = AppBuilder::new("127.0.0.1:0")
            .csrf(CsrfPolicy::cross_origin())
            .csrf(CsrfPolicy::cross_origin())
            .build()
            .await
            .err()
            .expect("a second canonical CSRF policy must fail startup");
        assert_eq!(duplicate, AppBuildError::DuplicateCsrfPolicy);
    }

    #[test]
    fn csrf_route_guard_contract_rejects_every_inert_or_duplicate_combination() {
        let guard_id = TypeId::of::<CsrfGuard>();
        let open = route("POST", "/open", "open", Vec::new());
        let guarded = route("POST", "/protected", "protected", vec![guard_id]);
        let duplicate = route("POST", "/duplicate", "duplicate", vec![guard_id, guard_id]);

        assert_eq!(
            validate_csrf_guard_contract(None, std::slice::from_ref(&open)),
            Ok(())
        );
        assert_eq!(
            validate_csrf_guard_contract(None, std::slice::from_ref(&guarded)),
            Err(AppBuildError::CsrfGuardRequiresRouteScopedPolicy)
        );
        assert_eq!(
            validate_csrf_guard_contract(
                Some(CsrfApplicationMode::Global),
                std::slice::from_ref(&guarded),
            ),
            Err(AppBuildError::CsrfGuardRequiresRouteScopedPolicy)
        );
        assert_eq!(
            validate_csrf_guard_contract(Some(CsrfApplicationMode::RouteScoped), &[open]),
            Err(AppBuildError::CsrfRouteScopedPolicyRequiresGuard)
        );
        assert_eq!(
            validate_csrf_guard_contract(Some(CsrfApplicationMode::RouteScoped), &[guarded]),
            Ok(())
        );
        assert_eq!(
            validate_csrf_guard_contract(Some(CsrfApplicationMode::RouteScoped), &[duplicate]),
            Err(AppBuildError::DuplicateCsrfGuardOnRoute)
        );
    }

    #[test]
    fn csrf_guard_observation_uses_the_global_enforcement_outcome_vocabulary() {
        let csrf = TypeId::of::<CsrfGuard>();
        let application = TypeId::of::<TestGuard>();
        let rejected = crate::guard::GuardRejection::forbidden(
            lily_web_core::HttpErrorCode::new("CSRF_REJECTED").unwrap(),
        )
        .unwrap();
        let internal = crate::guard::GuardRejection::new(
            500,
            lily_web_core::HttpErrorCode::new("CSRF_INTERNAL").unwrap(),
        )
        .unwrap();

        assert_eq!(guard_observation_outcome(csrf, &Ok(())), "completed");
        assert_eq!(guard_observation_outcome(csrf, &Err(rejected)), "rejected");
        assert_eq!(guard_observation_outcome(csrf, &Err(internal)), "internal");
        assert_eq!(guard_observation_outcome(application, &Ok(())), "allowed");
    }

    #[tokio::test]
    async fn csrf_mode_registration_is_exclusive_and_route_bypass_is_not_inert() {
        let conflict = AppBuilder::new("127.0.0.1:0")
            .csrf(CsrfPolicy::cross_origin())
            .csrf_route_scoped(CsrfPolicy::cross_origin())
            .build()
            .await
            .err()
            .expect("global and route-scoped policies must conflict");
        assert_eq!(conflict, AppBuildError::ConflictingCsrfModes);

        let duplicate = AppBuilder::new("127.0.0.1:0")
            .csrf_route_scoped(CsrfPolicy::cross_origin())
            .csrf_route_scoped(CsrfPolicy::cross_origin())
            .build()
            .await
            .err()
            .expect("a second route-scoped policy must fail");
        assert_eq!(duplicate, AppBuildError::DuplicateCsrfPolicy);

        let bypass = AppBuilder::new("127.0.0.1:0")
            .csrf_route_scoped(CsrfPolicy::cross_origin().bypass("POST", "/webhook"))
            .build()
            .await
            .err()
            .expect("a global bypass is inert in route-scoped mode");
        assert!(matches!(
            bypass,
            AppBuildError::InvalidCsrfConfiguration(error)
                if error.diagnostic_code() == "CSRF_ROUTE_BYPASS_UNSUPPORTED"
        ));
    }

    #[tokio::test]
    async fn ready_made_csrf_guard_cannot_capture_another_application_service() {
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let service = container.resolve::<CsrfService>(None).await.unwrap();
        service
            .attach(Arc::new(
                CompiledCsrfPolicy::try_new_route_scoped(&CsrfPolicy::cross_origin()).unwrap(),
            ))
            .unwrap();
        let guard = CsrfGuard::new(container.services()).await.unwrap();

        let error = AppBuilder::new("127.0.0.1:0")
            .guard(guard)
            .build()
            .await
            .err()
            .expect("ready-made CsrfGuard must not cross composition roots");
        assert_eq!(error, AppBuildError::ExplicitCsrfGuardUnsupported);

        drop(service);
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn csrf_builder_attaches_the_same_injectable_service_used_for_tokens() {
        let policy = CsrfPolicy::signed_double_submit(
            lily_middleware::CsrfSecret::new([5_u8; 32]).unwrap(),
            lily_middleware::CsrfSessionCookieBinding::new("session").unwrap(),
        );
        let app = AppBuilder::new("127.0.0.1:0")
            .csrf(policy)
            .build()
            .await
            .unwrap();
        assert_eq!(app.middlewares.len(), 1);

        let service = app.container().resolve::<CsrfService>(None).await.unwrap();
        let request = Request::from_transport_parts(
            "GET".to_string(),
            "/csrf".to_string(),
            vec![lily_core::RawHeader {
                name: "Cookie".to_string(),
                value: "session=opaque-session".to_string(),
                line_number: 0,
                raw_line: String::new(),
            }],
            b"",
        )
        .await
        .unwrap();
        let mut response = Response::new().await.unwrap();
        assert!(service.issue(&request, &mut response).await.is_ok());
        assert_eq!(response.header_values("set-cookie").unwrap().count(), 1);

        let container = Arc::clone(app.container());
        drop(service);
        drop(app);
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn verified_request_local_session_binds_global_csrf_before_dispatch() {
        let policy = CsrfPolicy::signed_double_submit(
            lily_middleware::CsrfSecret::new([6_u8; 32]).unwrap(),
            lily_middleware::CsrfRequestLocalBinding::new(),
        );
        let app = AppBuilder::new("127.0.0.1:0")
            .session_middleware::<RequestLocalSessionMiddleware>()
            .csrf(policy)
            .build()
            .await
            .unwrap();

        let service = app.container().resolve::<CsrfService>(None).await.unwrap();
        let mut issue_request =
            Request::from_transport_parts("GET".to_string(), "/csrf".to_string(), Vec::new(), b"")
                .await
                .unwrap();
        issue_request
            .local_mut()
            .insert(lily_middleware::CsrfSessionId::new("session-a").unwrap());
        let mut issue_response = Response::new().await.unwrap();
        let token = service
            .issue(&issue_request, &mut issue_response)
            .await
            .unwrap();
        let cookie_pair = issue_response
            .header_values("set-cookie")
            .unwrap()
            .next()
            .unwrap()
            .split(';')
            .next()
            .unwrap()
            .to_owned();

        let protected_request = |session: Option<&str>| {
            let mut headers = vec![
                lily_core::RawHeader {
                    name: "Cookie".to_string(),
                    value: cookie_pair.clone(),
                    line_number: 0,
                    raw_line: String::new(),
                },
                lily_core::RawHeader {
                    name: "X-CSRF-Token".to_string(),
                    value: token.as_str().to_owned(),
                    line_number: 1,
                    raw_line: String::new(),
                },
            ];
            if let Some(session) = session {
                headers.push(lily_core::RawHeader {
                    name: "X-Test-Session".to_string(),
                    value: session.to_owned(),
                    line_number: 2,
                    raw_line: String::new(),
                });
            }
            headers
        };

        let mut valid = Request::from_transport_parts(
            "POST".to_string(),
            "/missing".to_string(),
            protected_request(Some("session-a")),
            b"",
        )
        .await
        .unwrap();
        let mut valid_response = Response::new().await.unwrap();
        app.handle_request(&mut valid, &mut valid_response)
            .await
            .unwrap();
        assert_eq!(valid_response.status_code_value(), 404);
        let expected_session = lily_middleware::CsrfSessionId::new("session-a").unwrap();
        assert_eq!(
            valid.local().get::<lily_middleware::CsrfSessionId>(),
            Some(&expected_session)
        );

        for session in [None, Some("session-b")] {
            let mut rejected = Request::from_transport_parts(
                "POST".to_string(),
                "/missing".to_string(),
                protected_request(session),
                b"",
            )
            .await
            .unwrap();
            let mut rejected_response = Response::new().await.unwrap();
            app.handle_request(&mut rejected, &mut rejected_response)
                .await
                .unwrap();
            assert_eq!(rejected_response.status_code_value(), 403);
        }

        drop(service);
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn csrf_guard_protects_only_selected_routes_and_keeps_safe_methods_safe() {
        let mut app = AppBuilder::new("127.0.0.1:0").build().await.unwrap();
        let policy = CsrfPolicy::signed_double_submit(
            lily_middleware::CsrfSecret::new([7_u8; 32]).unwrap(),
            lily_middleware::CsrfSessionCookieBinding::new("session").unwrap(),
        );
        let runtime = Arc::new(CompiledCsrfPolicy::try_new_route_scoped(&policy).unwrap());
        let service = app.container().resolve::<CsrfService>(None).await.unwrap();
        service.attach(runtime).unwrap();
        let csrf_guard = CsrfGuard::new(app.container.services()).await.unwrap();
        let guard_id = TypeId::of::<CsrfGuard>();
        let guard: GuardInstance = Arc::new(csrf_guard);
        app.route_state = Arc::new(
            AppRouteState::new(
                RouteTable::from_routes(vec![
                    route("POST", "/open", "open", Vec::new()),
                    route("POST", "/protected", "protected_post", vec![guard_id]),
                    route("GET", "/protected", "protected_get", vec![guard_id]),
                ])
                .unwrap(),
                HashMap::from([(guard_id, guard)]),
            )
            .unwrap(),
        );

        let mut open =
            Request::from_transport_parts("POST".to_string(), "/open".to_string(), Vec::new(), b"")
                .await
                .unwrap();
        let mut open_response = Response::new().await.unwrap();
        app.handle_request(&mut open, &mut open_response)
            .await
            .unwrap();
        assert_eq!(open_response.status_code_value(), 200);

        let mut missing = Request::from_transport_parts(
            "POST".to_string(),
            "/protected".to_string(),
            Vec::new(),
            b"",
        )
        .await
        .unwrap();
        let mut missing_response = Response::new().await.unwrap();
        app.handle_request(&mut missing, &mut missing_response)
            .await
            .unwrap();
        assert_eq!(missing_response.status_code_value(), 403);

        let mut safe = Request::from_transport_parts(
            "GET".to_string(),
            "/protected".to_string(),
            Vec::new(),
            b"",
        )
        .await
        .unwrap();
        let mut safe_response = Response::new().await.unwrap();
        app.handle_request(&mut safe, &mut safe_response)
            .await
            .unwrap();
        assert_eq!(safe_response.status_code_value(), 200);

        let issue_request = Request::from_transport_parts(
            "GET".to_string(),
            "/csrf".to_string(),
            vec![lily_core::RawHeader {
                name: "Cookie".to_string(),
                value: "session=session-a".to_string(),
                line_number: 0,
                raw_line: String::new(),
            }],
            b"",
        )
        .await
        .unwrap();
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
        let mut valid = Request::from_transport_parts(
            "POST".to_string(),
            "/protected".to_string(),
            vec![
                lily_core::RawHeader {
                    name: "Cookie".to_string(),
                    value: format!("session=session-a; {csrf_cookie}"),
                    line_number: 0,
                    raw_line: String::new(),
                },
                lily_core::RawHeader {
                    name: "X-CSRF-Token".to_string(),
                    value: token.as_str().to_owned(),
                    line_number: 1,
                    raw_line: String::new(),
                },
            ],
            b"",
        )
        .await
        .unwrap();
        let mut valid_response = Response::new().await.unwrap();
        app.handle_request(&mut valid, &mut valid_response)
            .await
            .unwrap();
        assert_eq!(valid_response.status_code_value(), 200);

        drop(service);
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn application_session_guard_must_precede_request_local_csrf_guard() {
        let mut app = AppBuilder::new("127.0.0.1:0").build().await.unwrap();
        let policy = CsrfPolicy::signed_double_submit(
            lily_middleware::CsrfSecret::new([8_u8; 32]).unwrap(),
            lily_middleware::CsrfRequestLocalBinding::new(),
        );
        let runtime = Arc::new(CompiledCsrfPolicy::try_new_route_scoped(&policy).unwrap());
        let service = app.container().resolve::<CsrfService>(None).await.unwrap();
        service.attach(runtime).unwrap();

        let csrf_guard_id = TypeId::of::<CsrfGuard>();
        let session_guard_id = TypeId::of::<RouteSessionGuard>();
        let csrf_guard: GuardInstance =
            Arc::new(CsrfGuard::new(app.container.services()).await.unwrap());
        let session_guard: GuardInstance = Arc::new(RouteSessionGuard);
        app.route_state = Arc::new(
            AppRouteState::new(
                RouteTable::from_routes(vec![
                    route(
                        "POST",
                        "/ordered",
                        "ordered",
                        vec![session_guard_id, csrf_guard_id],
                    ),
                    route(
                        "POST",
                        "/reversed",
                        "reversed",
                        vec![csrf_guard_id, session_guard_id],
                    ),
                ])
                .unwrap(),
                HashMap::from([
                    (csrf_guard_id, csrf_guard),
                    (session_guard_id, session_guard),
                ]),
            )
            .unwrap(),
        );

        let mut issue_request =
            Request::from_transport_parts("GET".to_string(), "/csrf".to_string(), Vec::new(), b"")
                .await
                .unwrap();
        issue_request
            .local_mut()
            .insert(lily_middleware::CsrfSessionId::new("session-a").unwrap());
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
        let request_headers = || {
            vec![
                lily_core::RawHeader {
                    name: "Cookie".to_string(),
                    value: csrf_cookie.clone(),
                    line_number: 0,
                    raw_line: String::new(),
                },
                lily_core::RawHeader {
                    name: "X-CSRF-Token".to_string(),
                    value: token.as_str().to_owned(),
                    line_number: 1,
                    raw_line: String::new(),
                },
            ]
        };

        let mut ordered = Request::from_transport_parts(
            "POST".to_string(),
            "/ordered".to_string(),
            request_headers(),
            b"",
        )
        .await
        .unwrap();
        let mut ordered_response = Response::new().await.unwrap();
        app.handle_request(&mut ordered, &mut ordered_response)
            .await
            .unwrap();
        assert_eq!(ordered_response.status_code_value(), 200);

        let mut reversed = Request::from_transport_parts(
            "POST".to_string(),
            "/reversed".to_string(),
            request_headers(),
            b"",
        )
        .await
        .unwrap();
        let mut reversed_response = Response::new().await.unwrap();
        app.handle_request(&mut reversed, &mut reversed_response)
            .await
            .unwrap();
        assert_eq!(reversed_response.status_code_value(), 403);

        drop(service);
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn canonical_cors_is_exclusive_and_sticky() {
        let first = lily_middleware::CorsPolicy::new()
            .allow_origins(["https://first.example"])
            .allow_methods(["GET"]);
        let second = lily_middleware::CorsPolicy::new()
            .allow_origins(["https://second.example"])
            .allow_methods(["POST"]);
        let builder = AppBuilder::new("127.0.0.1:0")
            .cors(first.clone())
            .cors(second);
        assert_eq!(builder.cors_policy, Some(first));
        assert!(builder.duplicate_cors_policy);
        assert!(matches!(
            builder.build().await,
            Err(AppBuildError::DuplicateCorsPolicy)
        ));
    }

    #[tokio::test]
    async fn middleware_count_is_rejected_before_any_descriptor_runs() {
        COUNTING_DESCRIPTOR_CALLS.store(0, Ordering::Release);
        let mut builder = AppBuilder::new("127.0.0.1:0");
        for _ in 0..=lily_middleware::MAX_HTTP_MIDDLEWARES {
            builder = builder.middleware::<CountingDescriptorMiddleware>();
        }

        let error = builder
            .build()
            .await
            .err()
            .expect("65 middleware entries must fail before composition startup");
        assert_eq!(COUNTING_DESCRIPTOR_CALLS.load(Ordering::Acquire), 0);
        assert_eq!(
            error,
            AppBuildError::InvalidMiddlewareConfiguration {
                index: None,
                error: lily_middleware::MiddlewareConfigError::TooManyMiddlewares {
                    limit: lily_middleware::MAX_HTTP_MIDDLEWARES,
                    actual: lily_middleware::MAX_HTTP_MIDDLEWARES + 1,
                },
            }
        );
    }

    #[tokio::test]
    async fn rate_limit_rejection_is_429_with_retry_after() {
        let app = AppBuilder::new("127.0.0.1:0")
            .middleware::<TestRateLimitRejection>()
            .build()
            .await
            .unwrap();
        let mut request = Request::from_transport_parts(
            "GET".to_string(),
            "/missing".to_string(),
            Vec::new(),
            &[],
        )
        .await
        .unwrap();
        let mut response = Response::new().await.unwrap();
        app.handle_request(&mut request, &mut response)
            .await
            .unwrap();
        let parts = response.into_transport_parts().unwrap();
        assert_eq!(parts.status, 429);
        assert_eq!(
            parts
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("retry-after"))
                .map(|(_, value)| value.as_str()),
            Some("3600")
        );
        assert_eq!(
            parts
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("x-ratelimit-limit"))
                .map(|(_, value)| value.as_str()),
            Some("100")
        );
        let body: serde_json::Value = serde_json::from_slice(&parts.body).unwrap();
        assert_eq!(body["type"], "about:blank");
        assert_eq!(body["title"], "Too Many Requests");
        assert_eq!(body["code"], "RATE_LIMIT_EXCEEDED");
        assert_eq!(body["status"], 429);
        assert_eq!(body["detail"], "The application rate limit was exceeded.");
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn rejection_materialization_failure_becomes_a_safe_500() {
        let app = AppBuilder::new("127.0.0.1:0")
            .middleware::<TestRateLimitRejection>()
            .build()
            .await
            .unwrap();
        let mut request = Request::from_transport_parts(
            "GET".to_string(),
            "/missing".to_string(),
            Vec::new(),
            &[],
        )
        .await
        .unwrap();
        let limits = ResponseLimits::new(BodyBudget::new(1024).unwrap(), 1, 1024).unwrap();
        let mut response = Response::with_limits(limits).await.unwrap();

        let outcome = app
            .handle_request_with_outcome(&mut request, &mut response)
            .await
            .unwrap();
        let parts = response.into_transport_parts().unwrap();
        assert_eq!(parts.status, 500);
        assert_eq!(outcome.error_code(), Some("RESPONSE_ENCODING_ERROR"));
        assert!(!parts
            .headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("x-ratelimit-limit")));
        assert!(!parts
            .body
            .windows("The application rate limit was exceeded.".len())
            .any(|window| window == b"The application rate limit was exceeded."));
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn middleware_context_reaches_handlers_without_cross_request_leakage() {
        let route_table = RouteTable::from_routes(vec![RouteInfo {
            method: "GET".to_string(),
            path: "/context".to_string(),
            handler: Handler::new(Arc::new(request_context_handler), false),
            handler_name: "request_context_handler".to_string(),
            guard_type_ids: Vec::new(),
            middleware_registrations: Vec::new(),
            cors_policy_registration: CorsRoutePolicyRegistration::inherit(),
            route_plan_id: 0,
        }])
        .unwrap();
        let mut app = AppBuilder::new("127.0.0.1:0")
            .middleware::<RequestContextMiddleware>()
            .build()
            .await
            .unwrap();
        app.route_state = Arc::new(AppRouteState::new(route_table, HashMap::new()).unwrap());
        let app = Arc::new(app);

        let mut tasks = Vec::with_capacity(32);
        for index in 0..32 {
            let app = Arc::clone(&app);
            tasks.push(tokio::spawn(async move {
                let expected = format!("request-{index}");
                let mut request = Request::from_transport_parts(
                    "GET".to_string(),
                    "/context".to_string(),
                    vec![lily_core::structs::RawHeader {
                        name: "X-Request-Context".to_string(),
                        value: expected.clone(),
                        line_number: 1,
                        raw_line: "X-Request-Context: <redacted>".to_string(),
                    }],
                    &[],
                )
                .await
                .unwrap();
                let mut response = Response::new().await.unwrap();
                app.handle_request(&mut request, &mut response)
                    .await
                    .unwrap();
                let body = response.into_transport_parts().unwrap().body;
                assert_eq!(body.as_ref(), expected.as_bytes());
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }

        let app = match Arc::try_unwrap(app) {
            Ok(app) => app,
            Err(_) => panic!("all request tasks must release the application"),
        };
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn handler_failure_is_materialized_as_a_safe_terminal_response() {
        let route_table = RouteTable::from_routes(vec![RouteInfo {
            method: "GET".to_string(),
            path: "/fails".to_string(),
            handler: Handler::new(Arc::new(failing_handler), false),
            handler_name: "failing_handler".to_string(),
            guard_type_ids: Vec::new(),
            middleware_registrations: Vec::new(),
            cors_policy_registration: CorsRoutePolicyRegistration::inherit(),
            route_plan_id: 0,
        }])
        .expect("test route must be valid");
        let route_state = Arc::new(
            AppRouteState::new(route_table, HashMap::new())
                .expect("unguarded test route must build"),
        );
        let mut app = AppBuilder::new("127.0.0.1:0")
            .build()
            .await
            .expect("test composition root must build without opening a listener");
        app.route_state = route_state;

        let mut request =
            Request::from_transport_parts("GET".to_string(), "/fails".to_string(), Vec::new(), &[])
                .await
                .unwrap();
        let mut response = Response::new().await.unwrap();
        let outcome = app
            .handle_request_with_outcome(&mut request, &mut response)
            .await
            .expect("handler failure must become a response before middleware unwind");
        assert_eq!(outcome.status(), 500);
        assert_eq!(outcome.outcome(), "error");
        assert_eq!(outcome.error_code(), Some("HANDLER_ERROR"));
        let parts = response.into_transport_parts().unwrap();
        assert_eq!(parts.status, 500);
        let body = String::from_utf8(parts.body.to_vec()).unwrap();
        assert!(body.contains("HANDLER_ERROR"));
        assert!(!body.contains("sensitive internal handler detail"));

        let container = Arc::clone(&app.container);
        drop(app);
        container
            .close_with_timeout(Duration::from_secs(1))
            .await
            .expect("test composition root must close cleanly");
    }

    #[tokio::test]
    async fn unit_handler_result_preserves_the_handler_terminal_response() {
        let route_table = RouteTable::from_routes(vec![RouteInfo {
            method: "POST".to_string(),
            path: "/created".to_string(),
            handler: Handler::from_action(TypedTestAction(written_unit_handler), false),
            handler_name: "written_unit_handler".to_string(),
            guard_type_ids: Vec::new(),
            middleware_registrations: Vec::new(),
            cors_policy_registration: CorsRoutePolicyRegistration::inherit(),
            route_plan_id: 0,
        }])
        .unwrap();
        let mut app = AppBuilder::new("127.0.0.1:0").build().await.unwrap();
        app.route_state = Arc::new(AppRouteState::new(route_table, HashMap::new()).unwrap());
        let mut request = Request::from_transport_parts(
            "POST".to_string(),
            "/created".to_string(),
            Vec::new(),
            &[],
        )
        .await
        .unwrap();
        let mut response = Response::new().await.unwrap();

        let outcome = app
            .handle_request_with_outcome(&mut request, &mut response)
            .await
            .unwrap();

        assert_eq!(outcome.status(), 201);
        assert_eq!(outcome.outcome(), "success");
        let parts = response.into_transport_parts().unwrap();
        assert_eq!(parts.status, 201);
        assert_eq!(
            parts.headers,
            vec![("x-handler".to_string(), "retained".to_string())]
        );
        assert_eq!(parts.body.as_ref(), b"created");
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn ignored_body_limit_failure_becomes_a_redacted_500() {
        let route_table = RouteTable::from_routes(vec![RouteInfo {
            method: "GET".to_string(),
            path: "/oversized".to_string(),
            handler: Handler::new(Arc::new(ignored_body_limit_handler), false),
            handler_name: "ignored_body_limit_handler".to_string(),
            guard_type_ids: Vec::new(),
            middleware_registrations: Vec::new(),
            cors_policy_registration: CorsRoutePolicyRegistration::inherit(),
            route_plan_id: 0,
        }])
        .unwrap();
        let mut app = AppBuilder::new("127.0.0.1:0").build().await.unwrap();
        app.route_state = Arc::new(AppRouteState::new(route_table, HashMap::new()).unwrap());
        let mut request = Request::from_transport_parts(
            "GET".to_string(),
            "/oversized".to_string(),
            Vec::new(),
            &[],
        )
        .await
        .unwrap();
        let limits = ResponseLimits::new(BodyBudget::new(256).unwrap(), 64, 64 * 1024).unwrap();
        let mut response = Response::with_limits(limits).await.unwrap();

        let outcome = app
            .handle_request_with_outcome(&mut request, &mut response)
            .await
            .unwrap();

        assert_eq!(outcome.status(), 500);
        assert_eq!(outcome.outcome(), "error");
        assert_eq!(outcome.error_code(), Some("RESPONSE_ENCODING_ERROR"));
        let parts = response.into_transport_parts().unwrap();
        assert_eq!(parts.status, 500);
        let body = String::from_utf8(parts.body.to_vec()).unwrap();
        assert!(body.contains("RESPONSE_ENCODING_ERROR"));
        assert!(!body.contains(&"x".repeat(32)));

        close_test_app(app).await;
    }

    #[tokio::test]
    async fn head_uses_get_route_and_method_miss_returns_405_with_allow() {
        let route_table = RouteTable::from_routes(vec![
            RouteInfo {
                method: "GET".to_string(),
                path: "/items/:id".to_string(),
                handler: Handler::from_action(TypedTestAction(written_unit_handler), false),
                handler_name: "get_item".to_string(),
                guard_type_ids: Vec::new(),
                middleware_registrations: Vec::new(),
                cors_policy_registration: CorsRoutePolicyRegistration::inherit(),
                route_plan_id: 0,
            },
            route("POST", "/items/:id", "post_item", Vec::new()),
        ])
        .unwrap();
        let mut app = AppBuilder::new("127.0.0.1:0").build().await.unwrap();
        app.route_state = Arc::new(AppRouteState::new(route_table, HashMap::new()).unwrap());

        let mut head_request = Request::from_transport_parts(
            "HEAD".to_string(),
            "/items/1".to_string(),
            Vec::new(),
            &[],
        )
        .await
        .unwrap();
        let mut head_response = Response::new().await.unwrap();
        app.handle_request(&mut head_request, &mut head_response)
            .await
            .unwrap();
        assert_eq!(head_response.status_code_value(), 201);
        assert_eq!(head_request.param("id").map(|id| id.as_str()), Some("1"));

        let mut delete_request = Request::from_transport_parts(
            "DELETE".to_string(),
            "/items/1".to_string(),
            Vec::new(),
            &[],
        )
        .await
        .unwrap();
        let mut delete_response = Response::new().await.unwrap();
        app.handle_request(&mut delete_request, &mut delete_response)
            .await
            .unwrap();
        assert_eq!(delete_response.status_code_value(), 405);
        assert_eq!(delete_response.header("allow"), Some("GET, HEAD, POST"));
        let body = delete_response.into_transport_parts().unwrap().body;
        assert_eq!(body.as_ref(), b"{\"error\":\"Method not allowed\"}");

        close_test_app(app).await;
    }

    #[tokio::test]
    async fn serialization_failure_replaces_partial_success_with_typed_safe_500() {
        let route_table = RouteTable::from_routes(vec![RouteInfo {
            method: "GET".to_string(),
            path: "/serialization-fails".to_string(),
            handler: Handler::from_action(TypedTestAction(serialization_failure_handler), false),
            handler_name: "serialization_failure_handler".to_string(),
            guard_type_ids: Vec::new(),
            middleware_registrations: Vec::new(),
            cors_policy_registration: CorsRoutePolicyRegistration::inherit(),
            route_plan_id: 0,
        }])
        .unwrap();
        let mut app = AppBuilder::new("127.0.0.1:0").build().await.unwrap();
        app.route_state = Arc::new(AppRouteState::new(route_table, HashMap::new()).unwrap());
        let mut request = Request::from_transport_parts(
            "GET".to_string(),
            "/serialization-fails".to_string(),
            Vec::new(),
            &[],
        )
        .await
        .unwrap();
        let mut response = Response::new().await.unwrap();

        let outcome = app
            .handle_request_with_outcome(&mut request, &mut response)
            .await
            .unwrap();

        assert_eq!(outcome.status(), 500);
        assert_eq!(outcome.outcome(), "error");
        assert_eq!(outcome.error_code(), Some("RESPONSE_ENCODING_ERROR"));
        assert_eq!(response.header("x-partial"), None);
        let parts = response.into_transport_parts().unwrap();
        assert_eq!(parts.status, 500);
        let body = String::from_utf8(parts.body.to_vec()).unwrap();
        assert!(body.contains("RESPONSE_ENCODING_ERROR"));
        assert!(body.contains("An internal server error occurred."));
        assert!(!body.contains("credential=secret"));
        assert!(!body.contains("/srv/private"));
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn guard_404_and_handler_error_responses_all_reverse_unwind() {
        let guard_type_id = TypeId::of::<DenyGuard>();
        let route_table = RouteTable::from_routes(vec![
            RouteInfo {
                method: "GET".to_string(),
                path: "/fails".to_string(),
                handler: Handler::new(Arc::new(failing_handler), false),
                handler_name: "failing_handler".to_string(),
                guard_type_ids: Vec::new(),
                middleware_registrations: Vec::new(),
                cors_policy_registration: CorsRoutePolicyRegistration::inherit(),
                route_plan_id: 0,
            },
            route("GET", "/guarded", "guarded_handler", vec![guard_type_id]),
        ])
        .unwrap();
        let guard: GuardInstance = Arc::new(DenyGuard);
        let mut app = AppBuilder::new("127.0.0.1:0")
            .middleware::<PrewriteSecret>()
            .middleware::<ResponseMarker>()
            .build()
            .await
            .unwrap();
        app.route_state = Arc::new(
            AppRouteState::new(route_table, HashMap::from([(guard_type_id, guard)])).unwrap(),
        );

        for (path, expected_status) in [("/guarded", 403_u16), ("/missing", 404), ("/fails", 500)] {
            let mut request =
                Request::from_transport_parts("GET".to_string(), path.to_string(), Vec::new(), &[])
                    .await
                    .unwrap();
            let mut response = Response::new().await.unwrap();
            let outcome = app
                .handle_request_with_outcome(&mut request, &mut response)
                .await
                .unwrap();

            assert_eq!(response.status_code_value(), expected_status);
            assert_eq!(outcome.status(), expected_status);
            if path == "/fails" {
                assert_eq!(outcome.outcome(), "error");
                assert_eq!(outcome.error_code(), Some("HANDLER_ERROR"));
            }
            assert_eq!(response.header("x-lily-middleware"), Some("unwound"));
            assert_eq!(response.header("x-untrusted-prefix"), None);
            let body = response.into_transport_parts().unwrap().body;
            assert!(!body
                .windows(b"credential=secret".len())
                .any(|window| window == b"credential=secret"));
            assert!(!body
                .windows(b"sensitive internal handler detail".len())
                .any(|window| window == b"sensitive internal handler detail"));
        }

        close_test_app(app).await;
    }

    #[tokio::test]
    async fn typed_rejection_before_and_after_errors_materialize_before_question_mark_unwind() {
        for (stage, expected_status, expected_code) in [
            (FailureStage::Rejection, 403_u16, "TEST_MIDDLEWARE_REJECTED"),
            (FailureStage::Before, 500, "TEST_MIDDLEWARE_FAILED"),
            (FailureStage::After, 500, "TEST_MIDDLEWARE_FAILED"),
        ] {
            let builder = AppBuilder::new("127.0.0.1:0").middleware::<ResponseMarker>();
            let app = match stage {
                FailureStage::Rejection => builder
                    .middleware::<FailingMiddleware<0>>()
                    .build()
                    .await
                    .unwrap(),
                FailureStage::Before => builder
                    .middleware::<FailingMiddleware<1>>()
                    .build()
                    .await
                    .unwrap(),
                FailureStage::After => builder
                    .middleware::<FailingMiddleware<2>>()
                    .build()
                    .await
                    .unwrap(),
            };
            let mut request = Request::from_transport_parts(
                "GET".to_string(),
                "/missing".to_string(),
                Vec::new(),
                &[],
            )
            .await
            .unwrap();
            let mut response = Response::new().await.unwrap();

            let outcome = app
                .handle_request_with_outcome(&mut request, &mut response)
                .await
                .unwrap();

            assert_eq!(response.status_code_value(), expected_status);
            assert_eq!(outcome.status(), expected_status);
            assert_eq!(
                outcome.outcome(),
                if expected_status == 403 {
                    "rejected"
                } else {
                    "error"
                }
            );
            assert_eq!(outcome.error_code(), Some(expected_code));
            assert_eq!(response.header("x-lily-middleware"), Some("unwound"));
            let body =
                String::from_utf8(response.into_transport_parts().unwrap().body.to_vec()).unwrap();
            assert!(body.contains(expected_code));
            close_test_app(app).await;
        }
    }

    #[tokio::test]
    async fn final_response_status_is_authoritative_for_private_call_outcome() {
        let app = AppBuilder::new("127.0.0.1:0")
            .middleware::<SuccessStatusOverride>()
            .middleware::<FailingMiddleware<1>>()
            .build()
            .await
            .unwrap();
        let mut request = Request::from_transport_parts(
            "GET".to_string(),
            "/missing".to_string(),
            Vec::new(),
            &[],
        )
        .await
        .unwrap();
        let mut response = Response::new().await.unwrap();

        let outcome = app
            .handle_request_with_outcome(&mut request, &mut response)
            .await
            .unwrap();

        assert_eq!(response.status_code_value(), 200);
        assert_eq!(outcome.status(), 200);
        assert_eq!(outcome.outcome(), "success");
        assert_eq!(outcome.error_code(), None);
        close_test_app(app).await;
    }

    #[derive(Clone)]
    struct TestApplicationError {
        status: u16,
        code: Option<crate::HttpErrorCode>,
        conversions: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl IntoResponse for TestApplicationError {
        async fn write_to_response(
            self,
            response: &mut Response,
            request: &mut Request,
        ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
            self.conversions.fetch_add(1, Ordering::SeqCst);
            let outcome = Response::builder()
                .status(
                    self.status,
                    if self.status == 200 {
                        "OK"
                    } else {
                        "Not Found"
                    },
                )
                .text("application failure")
                .write_to_response(response, request)
                .await?;
            Ok(match self.code {
                Some(code) => ResponseWriteOutcome::error(Some(code)),
                None => outcome,
            })
        }
    }

    struct TestErrorAction(TestApplicationError);

    impl HttpAction for TestErrorAction {
        fn call<'a>(
            &'a self,
            _extensions: Arc<Extensions>,
            request: &'a mut Request,
            response: &'a mut Response,
        ) -> HttpActionFuture<'a> {
            Box::pin(async move {
                Result::<serde_json::Value, _>::Err(self.0.clone())
                    .write_to_response(response, request)
                    .await
            })
        }
    }

    async fn app_with_custom_error(
        builder: AppBuilder,
        status: u16,
        code: Option<crate::HttpErrorCode>,
        route_middleware: bool,
    ) -> (App, Arc<AtomicUsize>) {
        let mut app = builder.build().await.unwrap();
        let conversions = Arc::new(AtomicUsize::new(0));
        let mut custom_route = route("GET", "/custom-error", "custom_error", Vec::new());
        custom_route.handler = Handler::from_action(
            TestErrorAction(TestApplicationError {
                status,
                code,
                conversions: conversions.clone(),
            }),
            false,
        );
        if route_middleware {
            custom_route
                .middleware_registrations
                .push(HttpMiddlewareRegistration::of::<ResponseMarker>());
        }
        let route_table = RouteTable::from_routes(vec![custom_route]).unwrap();
        let plans = build_route_middleware_plans(
            &route_table,
            &app.middlewares,
            HashMap::new(),
            app.container.services(),
        )
        .await
        .unwrap();
        app.route_state = Arc::new(
            AppRouteState::with_route_middlewares(route_table, HashMap::new(), plans).unwrap(),
        );
        (app, conversions)
    }

    async fn custom_error_request() -> Request {
        Request::from_transport_parts("GET".into(), "/custom-error".into(), Vec::new(), &[])
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn custom_error_metadata_survives_direct_and_nested_middleware_paths() {
        for nested in [false, true] {
            for status in [200, 404] {
                for code in [
                    None,
                    Some(crate::HttpErrorCode::new("USER_MISSING").unwrap()),
                ] {
                    let builder = AppBuilder::new("127.0.0.1:0");
                    let builder = if nested {
                        builder.middleware::<PrewriteSecret>()
                    } else {
                        builder
                    };
                    let (app, conversions) =
                        app_with_custom_error(builder, status, code, nested).await;
                    let mut request = custom_error_request().await;
                    let mut response = Response::new().await.unwrap();
                    let outcome = app
                        .handle_request_with_outcome(&mut request, &mut response)
                        .await
                        .unwrap();
                    let expected_code =
                        code.map(|code| code.as_str()).unwrap_or(if status == 200 {
                            "APPLICATION_ERROR"
                        } else {
                            "HTTP_CLIENT_ERROR"
                        });
                    assert!(outcome.application_error());
                    assert_eq!(outcome.application_error_code(), Some(expected_code));
                    assert_eq!(outcome.status(), status);
                    assert_eq!(
                        outcome.outcome(),
                        if status == 200 { "success" } else { "rejected" }
                    );
                    assert_eq!(
                        outcome.error_code(),
                        if status == 200 {
                            None
                        } else {
                            Some(expected_code)
                        }
                    );
                    assert_eq!(conversions.load(Ordering::SeqCst), 1);
                    assert_eq!(response.header("x-untrusted-prefix"), None);
                    assert_eq!(
                        response.header("x-lily-middleware"),
                        nested.then_some("unwound")
                    );
                    assert_eq!(
                        response.into_transport_parts().unwrap().body.as_ref(),
                        b"application failure"
                    );
                    close_test_app(app).await;
                }
            }
        }
    }

    #[tokio::test]
    async fn middleware_replacement_keeps_application_origin_and_updates_final_http_metadata() {
        for replaces_with_error in [false, true] {
            let builder = AppBuilder::new("127.0.0.1:0");
            let builder = if replaces_with_error {
                builder.middleware::<FailingMiddleware<2>>()
            } else {
                builder.middleware::<SuccessStatusOverride>()
            };
            let (app, conversions) = app_with_custom_error(
                builder,
                404,
                Some(crate::HttpErrorCode::new("USER_MISSING").unwrap()),
                true,
            )
            .await;
            let mut request = custom_error_request().await;
            let mut response = Response::new().await.unwrap();
            let outcome = app
                .handle_request_with_outcome(&mut request, &mut response)
                .await
                .unwrap();
            assert!(outcome.application_error());
            assert_eq!(outcome.application_error_code(), Some("USER_MISSING"));
            assert_eq!(conversions.load(Ordering::SeqCst), 1);
            if replaces_with_error {
                assert_eq!(outcome.status(), 500);
                assert_eq!(outcome.error_code(), Some("TEST_MIDDLEWARE_FAILED"));
                assert!(
                    String::from_utf8_lossy(&response.into_transport_parts().unwrap().body)
                        .contains("TEST_MIDDLEWARE_FAILED")
                );
            } else {
                assert_eq!(outcome.status(), 200);
                assert_eq!(outcome.outcome(), "success");
                assert_eq!(outcome.error_code(), None);
            }
            close_test_app(app).await;
        }
    }

    struct PoisonAfter;

    #[async_trait::async_trait]
    impl HttpMiddleware for PoisonAfter {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
            Ok(Self)
        }
        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("poison_after", lily_middleware::MiddlewareKind::Custom)
        }
        async fn handle(
            &self,
            exchange: &mut HttpExchange<'_>,
            next: crate::HttpNext<'_>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), HttpMiddlewareError> {
            next.run(exchange).await?;
            let _ = exchange.response_mut().write_body(&[b'x'; 257]);
            Ok(())
        }
    }

    #[tokio::test]
    async fn technical_fallback_replaces_response_code_without_erasing_application_origin() {
        let (app, conversions) = app_with_custom_error(
            AppBuilder::new("127.0.0.1:0").middleware::<PoisonAfter>(),
            404,
            Some(crate::HttpErrorCode::new("USER_MISSING").unwrap()),
            true,
        )
        .await;
        let limits =
            crate::ResponseLimits::new(crate::BodyBudget::new(256).unwrap(), 8, 1024).unwrap();
        let mut response = Response::from_limits(limits);
        let mut request = custom_error_request().await;
        let outcome = app
            .handle_request_with_outcome(&mut request, &mut response)
            .await
            .unwrap();
        assert_eq!(outcome.status(), 500);
        assert_eq!(outcome.error_code(), Some("RESPONSE_ENCODING_ERROR"));
        assert!(outcome.application_error());
        assert_eq!(outcome.application_error_code(), Some("USER_MISSING"));
        assert_eq!(conversions.load(Ordering::SeqCst), 1);
        assert_eq!(response.limits(), limits);
        let body = response.into_transport_parts().unwrap().body;
        assert!(String::from_utf8_lossy(&body).contains("RESPONSE_ENCODING_ERROR"));
        assert!(!String::from_utf8_lossy(&body).contains("application failure"));
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn an_unwritable_error_and_fallback_leave_through_the_technical_channel() {
        let (app, conversions) = app_with_custom_error(
            AppBuilder::new("127.0.0.1:0").middleware::<SuccessStatusOverride>(),
            404,
            None,
            true,
        )
        .await;
        let limits =
            crate::ResponseLimits::new(crate::BodyBudget::new(1).unwrap(), 8, 1024).unwrap();
        let mut response = Response::from_limits(limits);
        response.status(202, "Accepted");
        response
            .try_insert_header("X-Original", "retained")
            .unwrap();
        let mut request = custom_error_request().await;
        let error = tokio::time::timeout(
            Duration::from_secs(1),
            app.handle_request_with_outcome(&mut request, &mut response),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert_eq!(
            error,
            ResponseWriteError::Body(crate::ResponseBodyError::LimitExceeded { limit_bytes: 1 })
        );
        assert_eq!(conversions.load(Ordering::SeqCst), 1);
        assert_eq!(response.status_code_value(), 202);
        assert_eq!(response.header("x-original"), Some("retained"));
        assert_eq!(response.limits(), limits);
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn localization_is_disabled_by_default_and_can_be_selected_explicitly() {
        let app = AppBuilder::new("127.0.0.1:0")
            .build()
            .await
            .expect("default localization mode must not require a locales directory");
        assert!(app.localization_catalog().is_none());
        close_test_app(app).await;

        let explicit = AppBuilder::new("127.0.0.1:0")
            .localization_catalog(test_catalog("unused"))
            .localization_disabled()
            .build()
            .await
            .expect("explicit disabled mode must perform no localization I/O");
        assert!(explicit.localization_catalog().is_none());
        close_test_app(explicit).await;
    }

    #[tokio::test]
    async fn injectable_health_service_owns_the_application_snapshot() {
        let app = AppBuilder::new("127.0.0.1:0").build().await.unwrap();
        let health = app
            .extensions()
            .get_service::<HttpHealthService>(None)
            .await
            .unwrap();
        let snapshot = health.snapshot().unwrap();
        assert!(snapshot.live);
        assert!(!snapshot.ready);
        assert!(!snapshot.accepting_new_work);
        assert_eq!(snapshot.checks.len(), 3);
        let listener = snapshot
            .checks
            .iter()
            .find(|check| check.name == "http.listener")
            .unwrap();
        assert_eq!(listener.status, HealthStatus::Unhealthy);
        assert_eq!(listener.reason_code, "not_started");
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn one_container_cannot_publish_two_http_lifecycle_snapshots() {
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let first = AppBuilder::new("127.0.0.1:0")
            .container(Arc::clone(&container))
            .build()
            .await
            .unwrap();

        let error = AppBuilder::new("127.0.0.1:0")
            .container(Arc::clone(&container))
            .build()
            .await
            .err()
            .expect("a DI health singleton cannot represent two HTTP applications");
        assert!(matches!(error, AppBuildError::MonitoringInitialization(_)));

        drop(first);
        container
            .close_with_timeout(Duration::from_secs(1))
            .await
            .expect("caller-owned test container must close cleanly");
    }

    #[tokio::test]
    async fn caller_cancellation_tracks_bound_ready_and_shutdown_states() {
        let app = AppBuilder::new("127.0.0.1:0").build().await.unwrap();
        assert_eq!(app.listen_address(), "127.0.0.1:0");
        assert_eq!(app.bound_address(), None);
        let observer = app.clone();
        let health = app
            .extensions()
            .get_service::<HttpHealthService>(None)
            .await
            .unwrap();
        let cancellation = CancellationToken::new();
        let cancel = cancellation.clone();
        let task = tokio::spawn(async move { app.start_with_cancellation(cancellation).await });

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if health.snapshot().unwrap().ready {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("bound listener must publish readiness");
        let ready = health.snapshot().unwrap();
        assert!(ready.live);
        assert!(ready.ready);
        assert!(ready.accepting_new_work);
        let bound_address = observer
            .bound_address()
            .expect("ready listener must publish its bound address");
        assert_ne!(bound_address.port(), 0);
        assert_eq!(observer.listen_address(), "127.0.0.1:0");
        let connection = tokio::time::timeout(
            Duration::from_secs(5),
            tokio::net::TcpStream::connect(bound_address),
        )
        .await
        .expect("bound-address connection must be bounded")
        .expect("published bound address must accept a connection");
        drop(connection);
        let listener = ready
            .checks
            .iter()
            .find(|check| check.name == "http.listener")
            .unwrap();
        assert_eq!(listener.reason_code, "listening");

        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .expect("programmatic shutdown must be bounded")
            .expect("HTTP lifecycle task must not panic")
            .expect("clean programmatic shutdown must succeed");

        let stopped = health.snapshot().unwrap();
        assert!(stopped.live);
        assert!(!stopped.ready);
        assert!(!stopped.accepting_new_work);
        assert_eq!(observer.bound_address(), Some(bound_address));
        let listener = stopped
            .checks
            .iter()
            .find(|check| check.name == "http.listener")
            .unwrap();
        assert_eq!(listener.reason_code, "shutting_down");
    }

    #[tokio::test]
    async fn bind_failure_is_fatal_and_visible_through_the_injectable_service() {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reservation.local_addr().unwrap();
        let app = AppBuilder::new(&address.to_string()).build().await.unwrap();
        let health = app
            .extensions()
            .get_service::<HttpHealthService>(None)
            .await
            .unwrap();

        let error = app
            .start_with_cancellation(CancellationToken::new())
            .await
            .expect_err("an occupied listener address must fail startup");
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);
        let snapshot = health.snapshot().unwrap();
        assert!(!snapshot.live);
        assert!(!snapshot.ready);
        assert!(!snapshot.accepting_new_work);
        let listener = snapshot
            .checks
            .iter()
            .find(|check| check.name == "http.listener")
            .unwrap();
        assert_eq!(listener.reason_code, "bind_failed");
        drop(reservation);
    }

    #[tokio::test]
    async fn pre_cancelled_start_never_finishes_as_ready() {
        let app = AppBuilder::new("127.0.0.1:0").build().await.unwrap();
        let observer = app.clone();
        let health = app
            .extensions()
            .get_service::<HttpHealthService>(None)
            .await
            .unwrap();
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        app.start_with_cancellation(cancellation)
            .await
            .expect("pre-cancelled lifecycle must close owned resources cleanly");
        assert_eq!(observer.bound_address(), None);
        let snapshot = health.snapshot().unwrap();
        assert!(snapshot.live);
        assert!(!snapshot.ready);
        assert!(!snapshot.accepting_new_work);
        let listener = snapshot
            .checks
            .iter()
            .find(|check| check.name == "http.listener")
            .unwrap();
        assert_eq!(listener.reason_code, "shutting_down");
    }

    #[tokio::test]
    async fn explicit_localization_path_is_loaded_once_during_build() {
        let directory = TempDir::new().unwrap();
        tokio::fs::write(
            directory.path().join("tr.json"),
            r#"{"BAD_REQUEST":"Dosyadan yüklendi"}"#,
        )
        .await
        .unwrap();

        let app = AppBuilder::new("127.0.0.1:0")
            .localization_path(directory.path())
            .build()
            .await
            .expect("an explicit valid localization path must build");
        assert_eq!(
            app.localization_catalog()
                .and_then(|catalog| catalog.translate("tr-TR", "BAD_REQUEST")),
            Some("Dosyadan yüklendi")
        );
        close_test_app(app).await;
    }

    #[tokio::test]
    async fn explicit_missing_or_malformed_localization_path_fails_build_typed() {
        let directory = TempDir::new().unwrap();
        let missing = directory.path().join("missing");
        let missing_error = AppBuilder::new("127.0.0.1:0")
            .localization_path(&missing)
            .build()
            .await
            .err()
            .expect("an explicit missing localization path must fail build");
        assert!(matches!(
            missing_error,
            AppBuildError::InvalidLocalizationConfiguration(_)
        ));

        tokio::fs::write(directory.path().join("en.json"), r#"{"BAD_REQUEST":42}"#)
            .await
            .unwrap();
        let malformed_error = AppBuilder::new("127.0.0.1:0")
            .localization_path(directory.path())
            .build()
            .await
            .err()
            .expect("a malformed localization snapshot must fail build");
        assert!(matches!(
            malformed_error,
            AppBuildError::InvalidLocalizationConfiguration(_)
        ));
    }

    #[tokio::test]
    async fn two_applications_localize_from_isolated_request_catalogs() {
        let route_table = || {
            RouteTable::from_routes(vec![RouteInfo {
                method: "GET".to_string(),
                path: "/localized".to_string(),
                handler: Handler::from_action(TypedTestAction(localized_error_handler), false),
                handler_name: "localized_error_handler".to_string(),
                guard_type_ids: Vec::new(),
                middleware_registrations: Vec::new(),
                cors_policy_registration: CorsRoutePolicyRegistration::inherit(),
                route_plan_id: 0,
            }])
            .unwrap()
        };

        let mut first = AppBuilder::new("127.0.0.1:0")
            .localization_catalog(test_catalog("Birinci uygulama"))
            .build()
            .await
            .unwrap();
        first.route_state = Arc::new(
            AppRouteState::new(route_table(), HashMap::new())
                .expect("first route state must build"),
        );
        let mut second = AppBuilder::new("127.0.0.1:0")
            .localization_catalog(test_catalog("İkinci uygulama"))
            .build()
            .await
            .unwrap();
        second.route_state = Arc::new(
            AppRouteState::new(route_table(), HashMap::new())
                .expect("second route state must build"),
        );

        let request = || async {
            Request::from_transport_parts(
                "GET".to_string(),
                "/localized".to_string(),
                vec![lily_core::structs::RawHeader {
                    name: "Accept-Language".to_string(),
                    value: "tr-TR".to_string(),
                    line_number: 1,
                    raw_line: "Accept-Language: <redacted>".to_string(),
                }],
                &[],
            )
            .await
            .unwrap()
        };
        let mut first_request = request().await;
        let mut second_request = request().await;
        let mut first_response = Response::new().await.unwrap();
        let mut second_response = Response::new().await.unwrap();

        first
            .handle_request(&mut first_request, &mut first_response)
            .await
            .unwrap();
        second
            .handle_request(&mut second_request, &mut second_response)
            .await
            .unwrap();
        let first_json: serde_json::Value =
            serde_json::from_slice(&first_response.into_transport_parts().unwrap().body).unwrap();
        let second_json: serde_json::Value =
            serde_json::from_slice(&second_response.into_transport_parts().unwrap().body).unwrap();
        assert_eq!(first_json["message"], "Birinci uygulama");
        assert_eq!(second_json["message"], "İkinci uygulama");

        close_test_app(first).await;
        close_test_app(second).await;
    }

    #[tokio::test]
    async fn controller_provider_resolves_one_scoped_service_per_request_scope() {
        CONTROLLER_PROVIDER_INITIALIZATIONS.store(0, Ordering::SeqCst);
        CONTROLLER_SCOPED_INITIALIZATIONS.store(0, Ordering::SeqCst);
        CONTROLLER_SCOPED_IDS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();

        let mut app = AppBuilder::new("127.0.0.1:0").build().await.unwrap();
        let routes = materialize_controller_routes(
            vec![ControllerRegistration::of::<ProviderController>()],
            vec![PendingControllerRoute::new(
                "GET",
                "/controller-provider",
                "fixture::ProviderController::action",
                Vec::new(),
                Vec::new(),
                CorsRoutePolicyRegistration::inherit(),
                ControllerActionRegistration::of::<ProviderController>(bind_provider_controller),
            )],
            app.extensions(),
        )
        .await
        .expect("provider controller route materializes");
        app.replace_routes_and_cors_for_test(routes.routes().cloned().collect(), None)
            .await
            .expect("provider controller route table builds");

        for _ in 0..2 {
            let request = Request::from_transport_parts(
                "GET".to_string(),
                "/controller-provider".to_string(),
                Vec::new(),
                &[],
            )
            .await
            .unwrap();
            let response = Response::new().await.unwrap();
            let service = Arc::new(app.clone());
            let (_, outcome) = app
                .request_registry()
                .spawn(service.clone(), move |owner| async move {
                    owner
                        .execute_for_test(
                            Duration::from_secs(2),
                            service.call_with_outcome(request, response, &owner),
                        )
                        .await
                        .unwrap()
                        .unwrap()
                })
                .unwrap()
                .wait()
                .await
                .expect("provider controller action succeeds");
            assert_eq!(outcome.status(), 204);
        }

        assert_eq!(
            CONTROLLER_PROVIDER_INITIALIZATIONS.load(Ordering::SeqCst),
            1
        );
        assert_eq!(CONTROLLER_SCOPED_INITIALIZATIONS.load(Ordering::SeqCst), 2);
        let scoped_ids = CONTROLLER_SCOPED_IDS
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        assert_eq!(scoped_ids.len(), 2);
        assert_ne!(scoped_ids[0], scoped_ids[1]);

        close_test_app(app).await;
    }
}
