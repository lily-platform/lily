mod background;
mod build_transaction;
mod message_reporting;
mod ownership;
mod reconciliation;
mod reporting;

use ownership::{
    ConnectionSession, ExecutionExit, ExecutionSlot, MessageDispatchRegistry, ScopeCleanupRegistry,
};

use self::build_transaction::WsAppBuildTransaction;
use crate::backplane::{
    WS_BACKPLANE_PUBLISHER_HEALTH_CHECK, WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
    WebSocketBackplaneIngressTask, WebSocketBackplaneRegistration,
};
use crate::codec::{
    DecodedWebSocketMessage, DecodedWebSocketPayload, EncodedWebSocketMessage, LilyEnvelopeCodec,
    RawEnvelope, WebSocketCodecFailureKind, WebSocketFrameCodec, WebSocketInboundMessageKind,
    WebSocketMessageHeaders, WebSocketOutboundMessageKind, WebSocketPayloadCodec,
};
use crate::connection::{
    CloseRequestOutcome, ConnectionControlFrame, ConnectionControlReceiver, ConnectionManager,
    IdentityExpiryClaim, QueuedApplicationFrame, connection_control_channel,
};
#[cfg(test)]
use crate::controller::materialize_websocket_controllers;
use crate::controller::{
    MaterializedWebSocketAction, WebSocketActionTable, WebSocketConnectionMiddlewareRegistration,
    WebSocketContext, WebSocketFrameCodecRegistration, WebSocketGuardRegistration,
    WebSocketHandshakeMiddlewareRegistration, WebSocketIdentityMiddlewareRegistration,
    WebSocketLifecycleHandler, WebSocketLifecycleHandlers, WebSocketMessageMiddlewareRegistration,
    WebSocketPayloadCodecRegistration, get_pending_websocket_operations,
    get_websocket_controller_registrations, materialize_websocket_controllers_with_pipeline,
};
use crate::extractor::{
    DisconnectReason, WebSocketLifecycleInvocation, WebSocketMessageInvocation,
    WebSocketMessageLocals,
};
use crate::middleware::{
    ConnectionCleanupOutcome, ConnectionCleanupReceipts, ConnectionCleanupRegistry,
    ConnectionTerminalHook, ConnectionTerminalHookReport, OpenTelemetryWsMiddlewareObserver,
    WebSocketHandshakeMiddleware, WebSocketIdentityMiddleware, WsConnectionMiddleware,
    WsMessageDecision, WsMessageExchange, WsMessageMiddleware, WsMessageOutcome,
    WsMiddlewareObserver,
};
use crate::request::{
    MAX_HANDSHAKE_HEADER_ENTRIES, MAX_HANDSHAKE_MERGED_HEADER_VALUE_BYTES, WsHeaderError,
    WsRequest, is_repeatable_websocket_header, validate_raw_header,
};
use crate::server::{
    ServerConfig, ServerError, WsEffectiveConfigError, WsHandshakeContext, WsTransportSecurity,
};
use crate::tasks::{OwnedTaskSet, TaskReceipt, TaskRegistry};
use crate::{
    BackplaneRequirement, PendingWebSocketActionOutcome, WebSocketActionError,
    WebSocketActionErrorDisposition, WebSocketBackplane, WebSocketDispatcher, WebSocketErrorCode,
};
use futures_util::{FutureExt, Sink, SinkExt, StreamExt, stream::FuturesUnordered};
use lily_background_service::{BackgroundServiceTrait, BackgroundServices};
use lily_config::{ConfigService, SecretResolver};
use lily_injection::{ApplicationContainer, DEFAULT_SHUTDOWN_TIMEOUT, Extensions, ProcessContext};
use lily_monitoring::{
    HealthCheckKind, HealthCriticality, HealthRegistry, HealthRegistryError, HealthSnapshot,
    HealthStatus,
};
use lily_shutdown::{
    FrameworkShutdownComponent, FrameworkShutdownCoordinator, FrameworkShutdownPhase,
    ShutdownError, ShutdownSignal, ShutdownState, SignalHandler,
};
use lily_trace::{TraceConfig, TraceInstallOutcome, TracingMode, TracingRuntimeOwner};
use lily_web_core::RustlsConfig;
use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc};
use tokio::time::{Duration, Instant, interval, timeout, timeout_at};
use tokio_rustls::{TlsAcceptor, server::TlsStream};
use tokio_tungstenite::{
    WebSocketStream,
    tungstenite::{
        Message,
        protocol::{CloseFrame, Role},
    },
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::Instrument;
use uuid::Uuid;

const WS_APP_BUILT: u8 = 0;
const WS_APP_RUNNING: u8 = 1;
const WS_APP_CLOSING: u8 = 2;
const WS_APP_TERMINAL: u8 = 3;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InboundQueueSaturation {
    MessageCapacity,
    ByteCapacity,
}

impl InboundQueueSaturation {
    const fn as_str(self) -> &'static str {
        match self {
            Self::MessageCapacity => "message_capacity",
            Self::ByteCapacity => "byte_capacity",
        }
    }
}

/// Complete application frames waiting behind the one active action.
///
/// Count bounds allocation overhead from many tiny messages while the byte
/// bound caps retained remote-controlled payload. Queued frames intentionally
/// own neither an execution task nor a DI scope.
struct InboundApplicationQueue {
    messages: VecDeque<Message>,
    retained_bytes: usize,
    max_messages: usize,
    max_bytes: usize,
}

impl InboundApplicationQueue {
    fn new(max_messages: usize, max_bytes: usize) -> Self {
        Self {
            messages: VecDeque::new(),
            retained_bytes: 0,
            max_messages,
            max_bytes,
        }
    }

    fn try_push(&mut self, message: Message) -> Result<(), InboundQueueSaturation> {
        debug_assert!(matches!(message, Message::Text(_) | Message::Binary(_)));
        if self.messages.len() >= self.max_messages {
            return Err(InboundQueueSaturation::MessageCapacity);
        }

        let retained_bytes = self
            .retained_bytes
            .checked_add(message.len())
            .ok_or(InboundQueueSaturation::ByteCapacity)?;
        if retained_bytes > self.max_bytes {
            return Err(InboundQueueSaturation::ByteCapacity);
        }

        self.messages.push_back(message);
        self.retained_bytes = retained_bytes;
        Ok(())
    }

    fn pop_front(&mut self) -> Option<Message> {
        let message = self.messages.pop_front()?;
        self.retained_bytes -= message.len();
        Some(message)
    }

    fn clear(&mut self) {
        self.messages.clear();
        self.retained_bytes = 0;
    }

    fn is_empty(&self) -> bool {
        self.messages.is_empty()
    }
}

const WS_DI_HEALTH_CHECK: &str = "di.container";
const WS_LISTENER_HEALTH_CHECK: &str = "transport.listener";
const WS_CONTROLLER_REGISTRY_HEALTH_CHECK: &str = "controller.registry";
const WS_DISPATCHER_HEALTH_CHECK: &str = "message.dispatcher";
const WS_DEPENDENCY_HEALTH_PREFIX: &str = "dependency.";

#[derive(Clone, Default)]
enum WsAddressSource {
    #[default]
    Config,
    Explicit(String),
}

#[derive(Clone, Default)]
enum WsServerConfigSource {
    #[default]
    Config,
    Explicit(Box<ServerConfig>),
}

/// Immutable WebSocket listener and runtime configuration frozen at build.
#[derive(Clone)]
pub struct EffectiveWsServerConfig {
    enabled: bool,
    listen_address: String,
    server: ServerConfig,
    tls_config: Option<RustlsConfig>,
}

impl std::fmt::Debug for EffectiveWsServerConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EffectiveWsServerConfig")
            .field("enabled", &self.enabled)
            .field("listen_address", &self.listen_address)
            .field("server", &self.server)
            .field("tls_enabled", &self.tls_config.is_some())
            .finish()
    }
}

impl EffectiveWsServerConfig {
    fn resolve(
        address_source: &WsAddressSource,
        config_source: &WsServerConfigSource,
        tls_config: Option<&RustlsConfig>,
        central: &lily_config::WebSocketConfig,
    ) -> Result<Self, ServerError> {
        let enabled = matches!(address_source, WsAddressSource::Explicit(_)) || central.enabled;
        if !enabled {
            return Err(ServerError::DisabledByConfiguration);
        }

        let listen_address = Self::resolve_address(address_source, central)?;
        let server = match config_source {
            WsServerConfigSource::Config => ServerConfig::from_central(central)?,
            WsServerConfigSource::Explicit(config) => config.as_ref().clone(),
        };
        server.validate().map_err(|error| {
            WsEffectiveConfigError::InvalidRuntime(
                crate::server::WsRuntimeConfigValidationError::new(error.to_string()),
            )
        })?;

        Ok(Self {
            enabled,
            listen_address,
            server,
            tls_config: tls_config.cloned(),
        })
    }

    fn resolve_address(
        source: &WsAddressSource,
        central: &lily_config::WebSocketConfig,
    ) -> Result<String, WsEffectiveConfigError> {
        match source {
            WsAddressSource::Explicit(address) => {
                let address = address.trim();
                if address.is_empty() {
                    return Err(WsEffectiveConfigError::InvalidListenAddress);
                }
                let parsed = url::Url::parse(&format!("tcp://{address}"))
                    .map_err(|_| WsEffectiveConfigError::InvalidListenAddress)?;
                if parsed.host().is_none()
                    || !parsed.username().is_empty()
                    || parsed.password().is_some()
                    || !parsed.path().is_empty()
                    || parsed.query().is_some()
                    || parsed.fragment().is_some()
                {
                    return Err(WsEffectiveConfigError::InvalidListenAddress);
                }
                if parsed.port().is_none() {
                    return Err(WsEffectiveConfigError::MissingListenPort);
                }
                Ok(address.to_string())
            }
            WsAddressSource::Config => {
                let host = central.host.trim();
                if host.is_empty() {
                    return Err(WsEffectiveConfigError::EmptyHost);
                }
                if central.port == 0 {
                    return Err(WsEffectiveConfigError::ZeroPort);
                }
                let rendered_host = match host.parse::<IpAddr>() {
                    Ok(IpAddr::V4(address)) => address.to_string(),
                    Ok(IpAddr::V6(address)) => format!("[{address}]"),
                    Err(_) => match url::Host::parse(host)
                        .map_err(|_| WsEffectiveConfigError::InvalidHost)?
                    {
                        url::Host::Domain(domain) => domain,
                        url::Host::Ipv4(address) => address.to_string(),
                        url::Host::Ipv6(address) => format!("[{address}]"),
                    },
                };
                Ok(format!("{rendered_host}:{}", central.port))
            }
        }
    }

    /// Whether the central WebSocket listener is enabled.
    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// Effective socket address used by the listener.
    pub fn listen_address(&self) -> &str {
        &self.listen_address
    }

    /// Validated server policy used by the runtime.
    pub fn server(&self) -> &ServerConfig {
        &self.server
    }

    /// Whether the listener terminates TLS itself.
    pub fn tls_enabled(&self) -> bool {
        self.tls_config.is_some()
    }

    /// Caller-supplied Rustls configuration, when direct WSS is enabled.
    pub fn rustls_config(&self) -> Option<&RustlsConfig> {
        self.tls_config.as_ref()
    }
}

/// WebSocket Application Builder (lily_http_api pattern)
///
/// Similar to AppBuilder in lily_http_api, provides a fluent API for building
/// WebSocket applications with struct controllers, DI, and configuration.
///
/// # Example
/// ```rust,no_run
/// use lily_websocket::{ServerConfig, WsAppBuilder};
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let app = WsAppBuilder::new("127.0.0.1:8080")
///     .config(ServerConfig::default())
///     .build()
///     .await?;
///
/// app.start().await?;
/// # Ok(())
/// }
/// ```
pub struct WsAppBuilder {
    background_services: BackgroundServices,
    address_source: WsAddressSource,
    config_source: WsServerConfigSource,
    tls_config: Option<RustlsConfig>,
    handshake_middlewares: Vec<WebSocketHandshakeMiddlewareRegistration>,
    connection_middlewares: Vec<WebSocketConnectionMiddlewareRegistration>,
    identity_middleware: Option<WebSocketIdentityMiddlewareRegistration>,
    duplicate_identity_middleware: bool,
    message_middlewares: Vec<WebSocketMessageMiddlewareRegistration>,
    guards: Vec<WebSocketGuardRegistration>,
    container: Option<Arc<ApplicationContainer>>,
    secret_resolver: Option<Arc<dyn SecretResolver>>,
    tracing_mode: TracingMode,
    frame_codec: WebSocketFrameCodecRegistration,
    payload_codec: WebSocketPayloadCodecRegistration,
    backplane: Option<WebSocketBackplaneRegistration>,
    duplicate_backplane: bool,
}

impl WsAppBuilder {
    /// Create new WebSocket app builder
    pub fn new(address: &str) -> Self {
        let mut builder = Self::config_backed();
        builder.address_source = WsAddressSource::Explicit(address.to_string());
        builder
    }

    fn config_backed() -> Self {
        Self {
            background_services: BackgroundServices::default(),
            address_source: WsAddressSource::Config,
            config_source: WsServerConfigSource::Config,
            tls_config: None,
            handshake_middlewares: Vec::new(),
            connection_middlewares: Vec::new(),
            identity_middleware: None,
            duplicate_identity_middleware: false,
            message_middlewares: Vec::new(),
            guards: Vec::new(),
            container: None,
            secret_resolver: None,
            tracing_mode: TracingMode::Disabled,
            frame_codec: WebSocketFrameCodecRegistration::of::<LilyEnvelopeCodec>(),
            payload_codec: WebSocketPayloadCodecRegistration::of::<LilyEnvelopeCodec>(),
            backplane: None,
            duplicate_backplane: false,
        }
    }

    /// Register one worker per concrete type. Constructors run during build;
    /// execution starts once after bind and required backplane readiness.
    /// Unhandled worker errors or panics stop this application.
    pub fn add_background_service<T: BackgroundServiceTrait>(mut self) -> Self {
        self.background_services.add::<T>();
        self
    }

    /// Set server configuration
    pub fn config(mut self, config: ServerConfig) -> Self {
        self.config_source = WsServerConfigSource::Explicit(Box::new(config));
        self
    }

    /// Publishes this listener as WSS with a complete Rustls server
    /// configuration. The WebSocket adapter selects HTTP/1.1 ALPN while
    /// preserving the caller's certificate resolver and client verifier.
    ///
    /// ```rust,no_run
    /// use lily_websocket::{RustlsConfig, WsAppBuilder};
    ///
    /// # async fn build() -> Result<(), Box<dyn std::error::Error>> {
    /// let tls = RustlsConfig::from_pem_file("server-chain.pem", "server-key.pem").await?;
    /// let app = WsAppBuilder::new("0.0.0.0:8443")
    ///     .rustls_config(tls)
    ///     .build()
    ///     .await?;
    /// app.start().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn rustls_config(mut self, config: impl Into<RustlsConfig>) -> Self {
        self.tls_config = Some(config.into());
        self
    }

    /// Explicitly selects the existing plaintext `ws://` transport.
    ///
    /// Plaintext remains the default, so existing applications do not change
    /// behavior when upgrading Lily.
    pub fn tls_disabled(mut self) -> Self {
        self.tls_config = None;
        self
    }

    /// Adds one global async middleware type executed before HTTP `101`.
    pub fn handshake_middleware<M>(mut self) -> Self
    where
        M: WebSocketHandshakeMiddleware,
    {
        self.handshake_middlewares
            .push(WebSocketHandshakeMiddlewareRegistration::of::<M>());
        self
    }

    /// Adds one global post-upgrade connection lifecycle middleware type.
    pub fn connection_middleware<M>(mut self) -> Self
    where
        M: WsConnectionMiddleware,
    {
        self.connection_middlewares
            .push(WebSocketConnectionMiddlewareRegistration::of::<M>());
        self
    }

    /// Selects the application's optional async identity middleware.
    ///
    /// Lily constructs this type once and invokes it after origin and all
    /// global/controller handshake middleware. Calling this method more than
    /// once is rejected during app build; no verifier implementation is
    /// supplied by the framework.
    pub fn identity_middleware<M>(mut self) -> Self
    where
        M: WebSocketIdentityMiddleware,
    {
        if self.identity_middleware.is_some() {
            self.duplicate_identity_middleware = true;
        } else {
            self.identity_middleware = Some(WebSocketIdentityMiddlewareRegistration::of::<M>());
        }
        self
    }

    /// Adds one global asynchronous per-message middleware type.
    ///
    /// Lily constructs `M` once after the application DI container exists and
    /// prepends it to every routed action's controller/action middleware plan.
    pub fn message_middleware<M>(mut self) -> Self
    where
        M: WsMessageMiddleware,
    {
        self.message_middlewares
            .push(WebSocketMessageMiddlewareRegistration::of::<M>());
        self
    }

    /// Adds one global message guard type.
    ///
    /// The guard is constructed once at app build and runs after effective
    /// message middleware but before payload extractors and the action.
    pub fn guard<G>(mut self) -> Self
    where
        G: crate::guard::WsGuard,
    {
        self.guards.push(WebSocketGuardRegistration::of::<G>());
        self
    }

    /// Selects the app-default transport frame codec.
    ///
    /// An exact controller-level `#[frame_codec(...)]` overrides this value.
    /// Action-level frame codecs are deliberately unsupported because the
    /// route must be decoded before an action can be selected.
    pub fn frame_codec<C>(mut self) -> Self
    where
        C: WebSocketFrameCodec,
    {
        self.frame_codec = WebSocketFrameCodecRegistration::of::<C>();
        self
    }

    /// Selects the app-default payload codec used after exact route lookup.
    /// Controller and action `#[payload_codec(...)]` metadata override it.
    pub fn payload_codec<C>(mut self) -> Self
    where
        C: WebSocketPayloadCodec,
    {
        self.payload_codec = WebSocketPayloadCodecRegistration::of::<C>();
        self
    }

    /// Selects one app-scoped distributed dispatch implementation.
    ///
    /// Lily constructs `B` exactly once after the application DI container is
    /// ready and passes that container's [`Extensions`] to
    /// [`WebSocketBackplane::new`]. Ready instances and separate builder
    /// configuration objects are deliberately unsupported. Calling this more
    /// than once is rejected during app build.
    pub fn backplane<B>(mut self, requirement: BackplaneRequirement) -> Self
    where
        B: WebSocketBackplane,
    {
        if self.backplane.is_some() {
            self.duplicate_backplane = true;
        } else {
            self.backplane = Some(WebSocketBackplaneRegistration::of::<B>(requirement));
        }
        self
    }

    /// Use an explicit application-owned DI container.
    pub fn container(mut self, container: Arc<ApplicationContainer>) -> Self {
        self.container = Some(container);
        self
    }

    /// Supplies this application's secret provider before configuration is
    /// loaded. Lily does not provide concrete secret-provider integrations.
    ///
    /// This cannot be combined with [`Self::container`]. Seed `ConfigService`
    /// while constructing a caller-owned container instead.
    pub fn secret_resolver<R>(mut self, resolver: R) -> Self
    where
        R: SecretResolver + 'static,
    {
        self.secret_resolver = Some(Arc::new(resolver));
        self
    }

    /// Do not install or probe tracing configuration. This is the default.
    pub fn tracing_disabled(mut self) -> Self {
        self.tracing_mode = TracingMode::Disabled;
        self
    }

    /// Install and own the supplied tracing configuration.
    pub fn tracing_config(mut self, config: TraceConfig) -> Self {
        self.tracing_mode = TracingMode::owned_config(config);
        self
    }

    /// Strictly load, install, and own tracing from the supplied path.
    pub fn tracing_config_path(mut self, path: impl AsRef<Path>) -> Self {
        self.tracing_mode = TracingMode::owned_path(path.as_ref().to_path_buf());
        self
    }

    /// Use a tracing runtime installed by the outer process composition root.
    pub fn tracing_external(mut self) -> Self {
        self.tracing_mode = TracingMode::External;
        self
    }

    /// Build the WebSocket application
    pub async fn build(self) -> Result<WsApp, ServerError> {
        tracing::debug!("Building WebSocket application");

        if self.container.is_some() && self.secret_resolver.is_some() {
            return Err(ServerError::configuration(
                "secret_resolver cannot be combined with a caller-owned DI container; seed ConfigService while building that container"
                    .to_string(),
            ));
        }
        if self.duplicate_identity_middleware {
            return Err(ServerError::configuration(
                "only one WebSocket identity middleware may be configured".to_owned(),
            ));
        }
        if self.duplicate_backplane {
            return Err(ServerError::configuration(
                "only one WebSocket backplane may be configured".to_owned(),
            ));
        }

        let shutdown_budget = crate::shutdown::ShutdownBudget::default();
        let connection_cleanup_registry = ConnectionCleanupRegistry::new().map_err(|error| {
            ServerError::configuration(format!(
                "WebSocket connection cleanup registry initialization failed: {error:?}"
            ))
        })?;

        // Freeze immutable link-time descriptors before creating any owned
        // application resources. Live controller instances are never stored
        // globally; they are materialized only after this App's Extensions
        // become available.
        let controller_registrations = get_websocket_controller_registrations()?;
        let pending_operations = get_pending_websocket_operations()?;

        // Resolve and install an explicitly selected owner before building a
        // framework-owned DI container. OpenTelemetry instruments bind to the
        // provider that exists when eager services are constructed.
        let trace_config = self.tracing_mode.resolve_owned_config().map_err(|error| {
            ServerError::configuration(format!("invalid tracing configuration: {error}"))
        })?;
        let tracing_owner = match trace_config {
            Some(config) => match TracingRuntimeOwner::install(&config) {
                Ok(TraceInstallOutcome::Disabled) => None,
                Ok(TraceInstallOutcome::Owned(owner)) => Some(owner),
                Err(error) => {
                    return Err(ServerError::configuration(format!(
                        "tracing initialization failed: {error}"
                    )));
                }
            },
            None => None,
        };
        let mut build_transaction = WsAppBuildTransaction::new(
            connection_cleanup_registry.runtime_handle(),
            tracing_owner,
            DEFAULT_SHUTDOWN_TIMEOUT,
        );

        // OpenTelemetry instruments bind to the provider that exists when
        // they are created. Build every middleware observer only after the
        // optional Lily-owned tracing runtime has been installed.
        let middleware_observer: Arc<dyn WsMiddlewareObserver> =
            Arc::new(OpenTelemetryWsMiddlewareObserver::new());
        // Step 2: Create or adopt this application's explicit DI owner.
        let owns_container = self.container.is_none();
        let container = match self.container {
            Some(container) => container,
            None => {
                let container_builder = match self.secret_resolver {
                    Some(resolver) => ApplicationContainer::builder()
                        .seed_singleton(ConfigService::with_shared_secret_resolver(resolver)),
                    None => ApplicationContainer::builder(),
                };
                match build_transaction
                    .build_owned_container(container_builder)
                    .await
                {
                    Ok(container) => container,
                    Err(error) => {
                        return Err(build_transaction.rollback(error).await);
                    }
                }
            }
        };
        let lily_config = match build_transaction
            .track(async {
                let config = container.resolve::<ConfigService>(None).await?;
                Ok::<_, lily_injection::InjectionError>(config.get_lily_config().await)
            })
            .await
        {
            Ok(config) => config,
            Err(error) => {
                return Err(build_transaction
                    .rollback(ServerError::connection_error(format!(
                        "failed to resolve DI lifecycle configuration: {error}"
                    )))
                    .await);
            }
        };
        let shutdown_timeout = Duration::from_secs(lily_config.lifecycle.shutdown_timeout_secs);
        build_transaction.set_shutdown_timeout(shutdown_timeout);
        let central_websocket = lily_config.websocket.clone().unwrap_or_default();
        let effective_server = match EffectiveWsServerConfig::resolve(
            &self.address_source,
            &self.config_source,
            self.tls_config.as_ref(),
            &central_websocket,
        ) {
            Ok(config) => Arc::new(config),
            Err(error) => {
                return Err(build_transaction.rollback(error).await);
            }
        };
        let server_config = effective_server.server();

        // Materialize every required controller exactly once for this App and
        // bind all of its operations to the same Arc-owned instance. Any
        // constructor, metadata or binder failure participates in the normal
        // startup rollback path.
        let action_table = match build_transaction
            .track(materialize_websocket_controllers_with_pipeline(
                controller_registrations,
                pending_operations,
                container.services(),
                self.frame_codec,
                self.payload_codec,
                self.handshake_middlewares,
                self.connection_middlewares,
                self.identity_middleware,
                self.message_middlewares,
                self.guards,
                middleware_observer,
            ))
            .await
        {
            Ok(table) => Arc::new(table),
            Err(error) => {
                return Err(build_transaction.rollback(error.into()).await);
            }
        };
        tracing::debug!(
            lily.websocket.namespace_count = action_table.namespaces().count(),
            lily.websocket.action_count = action_table.action_count(),
            "WebSocket controller registry materialized"
        );

        // Create connection manager
        let identity_index_enabled = action_table.identity_middleware().is_some();
        let connection_manager = Arc::new(
            ConnectionManager::with_registered_namespaces_and_identity_and_outbound_policy(
                server_config.max_rooms_per_connection,
                server_config.max_room_name_length,
                action_table.namespaces().map(str::to_owned),
                identity_index_enabled,
                server_config.max_outbound_message_size,
                server_config.outbound_queue_max_bytes,
                Duration::from_millis(server_config.outbound_admission_timeout_millis),
            ),
        );

        let backplane_registration: Option<WebSocketBackplaneRegistration> = self.backplane;
        let connection_permits = Arc::new(Semaphore::new(server_config.max_connections));
        let shutdown_state = Arc::new(ShutdownState::new_not_ready());
        let health = HealthRegistry::new(Arc::clone(&shutdown_state));
        let health_initialization = (|| -> Result<(), HealthRegistryError> {
            if !self.background_services.is_empty() {
                health.register(
                    background::HEALTH_CHECK,
                    HealthCheckKind::Lifecycle,
                    HealthCriticality::Critical,
                )?;
                health.update(
                    background::HEALTH_CHECK,
                    HealthStatus::Healthy,
                    "initialized",
                )?;
            }
            health.register(
                WS_DI_HEALTH_CHECK,
                HealthCheckKind::Lifecycle,
                HealthCriticality::Critical,
            )?;
            health.update(WS_DI_HEALTH_CHECK, HealthStatus::Healthy, "initialized")?;
            health.register(
                WS_LISTENER_HEALTH_CHECK,
                HealthCheckKind::Resource,
                HealthCriticality::Critical,
            )?;
            health.update(
                WS_LISTENER_HEALTH_CHECK,
                HealthStatus::Unhealthy,
                "not_started",
            )?;
            health.register(
                WS_CONTROLLER_REGISTRY_HEALTH_CHECK,
                HealthCheckKind::Lifecycle,
                HealthCriticality::Critical,
            )?;
            health.update(
                WS_CONTROLLER_REGISTRY_HEALTH_CHECK,
                HealthStatus::Healthy,
                "materialized",
            )?;
            health.register(
                WS_DISPATCHER_HEALTH_CHECK,
                HealthCheckKind::Resource,
                HealthCriticality::Critical,
            )?;
            health.update(
                WS_DISPATCHER_HEALTH_CHECK,
                HealthStatus::Healthy,
                "accepting_messages",
            )?;
            if let Some(registration) = backplane_registration {
                let criticality = match registration.requirement() {
                    BackplaneRequirement::Required => HealthCriticality::Critical,
                    BackplaneRequirement::Optional => HealthCriticality::NonCritical,
                };
                for check in [
                    WS_BACKPLANE_PUBLISHER_HEALTH_CHECK,
                    WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
                ] {
                    health.register(check, HealthCheckKind::Dependency, criticality)?;
                    health.update(check, HealthStatus::Unhealthy, "initializing")?;
                }
            }
            Ok(())
        })();
        if let Err(error) = health_initialization {
            return Err(build_transaction
                .rollback(ServerError::configuration(format!(
                    "health initialization failed: {error}"
                )))
                .await);
        }

        let dispatcher = match backplane_registration {
            None => Arc::new(WebSocketDispatcher::local(Arc::clone(&connection_manager))),
            Some(registration) => match build_transaction
                .track(registration.materialize(container.services(), shutdown_timeout))
                .await
            {
                Ok(backplane) => {
                    let dispatcher = Arc::new(WebSocketDispatcher::active(
                        Arc::clone(&connection_manager),
                        registration.requirement(),
                        backplane,
                        Duration::from_millis(server_config.write_timeout_millis),
                        health.clone(),
                    ));
                    build_transaction.retain_dispatcher(Arc::clone(&dispatcher));
                    let backplane_health = health
                        .update(
                            WS_BACKPLANE_PUBLISHER_HEALTH_CHECK,
                            HealthStatus::Healthy,
                            "initialized",
                        )
                        .and_then(|()| {
                            health.update(
                                WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
                                HealthStatus::Unhealthy,
                                "awaiting_ready",
                            )
                        });
                    if let Err(error) = backplane_health {
                        return Err(build_transaction
                            .rollback(ServerError::configuration(format!(
                                "backplane health initialization failed: {error}"
                            )))
                            .await);
                    }
                    tracing::debug!(
                        lily.websocket.backplane_type = registration.type_name(),
                        lily.websocket.backplane_requirement = ?registration.requirement(),
                        "WebSocket backplane materialized"
                    );
                    dispatcher
                }
                Err(_error) if registration.requirement() == BackplaneRequirement::Optional => {
                    let optional_health = health
                        .update(
                            WS_BACKPLANE_PUBLISHER_HEALTH_CHECK,
                            HealthStatus::Degraded,
                            "initialization_failed",
                        )
                        .and_then(|()| {
                            health.update(
                                WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
                                HealthStatus::Degraded,
                                "initialization_failed",
                            )
                        });
                    if let Err(health_error) = optional_health {
                        return Err(build_transaction
                            .rollback(ServerError::configuration(format!(
                                "optional backplane health publication failed: {health_error}"
                            )))
                            .await);
                    }
                    tracing::warn!(
                        lily.websocket.backplane_type = registration.type_name(),
                        "Optional WebSocket backplane initialization failed; local-only dispatch remains available"
                    );
                    Arc::new(WebSocketDispatcher::configured_unavailable(
                        Arc::clone(&connection_manager),
                        registration.requirement(),
                        Duration::from_millis(server_config.write_timeout_millis),
                        health.clone(),
                    ))
                }
                Err(error) => {
                    for check in [
                        WS_BACKPLANE_PUBLISHER_HEALTH_CHECK,
                        WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
                    ] {
                        let _ =
                            health.update(check, HealthStatus::Unhealthy, "initialization_failed");
                    }
                    return Err(build_transaction
                        .rollback(ServerError::configuration(format!(
                            "required WebSocket backplane `{}` failed to initialize ({:?})",
                            registration.type_name(),
                            error.kind(),
                        )))
                        .await);
                }
            },
        };
        let background = if self.background_services.is_empty() {
            None
        } else {
            let runtime = self.background_services.into_runtime(&container);
            // The supervisor owns constructors independently of this await.
            // Rollback must retain it before initialization can be cancelled.
            build_transaction.retain_background(runtime.clone());
            if let Err(error) = runtime.initialize().await {
                return Err(build_transaction.rollback(error.into()).await);
            }
            Some(background::BackgroundHost::new(runtime))
        };
        let dependency_registration_open = Arc::new(StdMutex::new(true));
        let tracing_owner = build_transaction.commit();
        Ok(WsApp {
            effective_server,
            action_table,
            connection_manager,
            dispatcher,
            container,
            owns_container,
            shutdown_timeout,
            connection_cleanup_registry,
            message_dispatch_registry: MessageDispatchRegistry::with_budget(
                shutdown_budget.clone(),
            ),
            scope_cleanup_registry: ScopeCleanupRegistry::with_budget(shutdown_budget),
            connection_permits,
            lifecycle: Arc::new(WsAppLifecycle {
                background,
                phase: AtomicU8::new(WS_APP_BUILT),
                close_cancellation: CancellationToken::new(),
                terminal_result: tokio::sync::Mutex::new(None),
                terminal_notify: Notify::new(),
                root_task: std::sync::OnceLock::new(),
                root_budget: Default::default(),
                execution_force: std::sync::OnceLock::new(),
                signal_tasks: TaskRegistry::default(),
                reconciliation_failed: AtomicBool::new(false),
                tracing_task: std::sync::OnceLock::new(),
                owns_tracing: tracing_owner.is_some(),
                reporting_started: std::sync::OnceLock::new(),
                coordinator_report: std::sync::OnceLock::new(),
                shutdown_report: std::sync::OnceLock::new(),
                root_join_observed: AtomicBool::new(false),
                server_tasks: TaskRegistry::default(),
                connection_tasks: TaskRegistry::default(),
                maintenance_tasks: TaskRegistry::default(),
                dependency_registration_open,
                tracing_owner: tokio::sync::Mutex::new(tracing_owner),
                shutdown_state,
                health,
            }),
        })
    }
}

impl Default for WsAppBuilder {
    fn default() -> Self {
        Self::config_backed()
    }
}

/// Error returned by the restricted required-dependency health facade.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WsDependencyHealthError {
    /// Required dependencies may only be registered before application start
    /// or close claims the application lifecycle.
    #[error("required dependency registration is closed")]
    RegistrationClosed,
    /// The bounded health registry rejected the name, reason, or transition.
    #[error(transparent)]
    Registry(#[from] HealthRegistryError),
}

/// Restricted health facade for application-owned required dependencies.
///
/// Lily owns listener, controller, dispatcher, and DI health entries. This
/// facade deliberately prefixes application dependency names so callers
/// cannot mutate those framework-owned checks. Register every required
/// dependency after [`WsAppBuilder::build`] and before [`WsApp::start`], then
/// retain a clone to publish its bounded runtime status.
#[derive(Clone)]
pub struct WsDependencyHealth {
    registry: HealthRegistry,
    registration_open: Arc<StdMutex<bool>>,
}

impl WsDependencyHealth {
    fn check_name(name: &str) -> Result<String, WsDependencyHealthError> {
        if name.is_empty() {
            return Err(HealthRegistryError::InvalidName.into());
        }
        Ok(format!("{WS_DEPENDENCY_HEALTH_PREFIX}{name}"))
    }

    /// Registers one critical dependency in the initial fail-closed state.
    pub fn register_required(&self, name: &str) -> Result<(), WsDependencyHealthError> {
        let registration_open = self
            .registration_open
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !*registration_open {
            return Err(WsDependencyHealthError::RegistrationClosed);
        }
        self.registry.register(
            Self::check_name(name)?,
            HealthCheckKind::Dependency,
            HealthCriticality::Critical,
        )?;
        Ok(())
    }

    /// Marks one registered dependency healthy with a bounded reason code.
    pub fn mark_healthy(
        &self,
        name: &str,
        reason_code: impl Into<String>,
    ) -> Result<(), WsDependencyHealthError> {
        self.update(name, HealthStatus::Healthy, reason_code)
    }

    /// Marks one registered dependency degraded with a bounded reason code.
    pub fn mark_degraded(
        &self,
        name: &str,
        reason_code: impl Into<String>,
    ) -> Result<(), WsDependencyHealthError> {
        self.update(name, HealthStatus::Degraded, reason_code)
    }

    /// Marks one registered dependency unhealthy with a bounded reason code.
    pub fn mark_unhealthy(
        &self,
        name: &str,
        reason_code: impl Into<String>,
    ) -> Result<(), WsDependencyHealthError> {
        self.update(name, HealthStatus::Unhealthy, reason_code)
    }

    fn update(
        &self,
        name: &str,
        status: HealthStatus,
        reason_code: impl Into<String>,
    ) -> Result<(), WsDependencyHealthError> {
        self.registry
            .update(&Self::check_name(name)?, status, reason_code)?;
        Ok(())
    }
}

type WebSocketDisconnectLedger = Arc<StdMutex<reporting::DisconnectLedger>>;

/// WebSocket Application
///
/// Main application struct that manages the WebSocket controller runtime.
/// Similar to App in lily_http_api.
pub struct WsApp {
    effective_server: Arc<EffectiveWsServerConfig>,
    action_table: Arc<WebSocketActionTable>,
    connection_manager: Arc<ConnectionManager>,
    dispatcher: Arc<WebSocketDispatcher>,
    container: Arc<ApplicationContainer>,
    /// True only when this composition root created the container.
    owns_container: bool,
    shutdown_timeout: Duration,
    connection_cleanup_registry: ConnectionCleanupRegistry,
    message_dispatch_registry: MessageDispatchRegistry,
    scope_cleanup_registry: ScopeCleanupRegistry,
    connection_permits: Arc<Semaphore>,
    lifecycle: Arc<WsAppLifecycle>,
}

struct WsAppLifecycle {
    background: Option<Arc<background::BackgroundHost>>,
    phase: AtomicU8,
    close_cancellation: CancellationToken,
    terminal_result: tokio::sync::Mutex<Option<Result<(), String>>>,
    terminal_notify: Notify,
    root_task: std::sync::OnceLock<TaskReceipt<()>>,
    root_budget: crate::shutdown::ShutdownBudget,
    execution_force: std::sync::OnceLock<CancellationToken>,
    signal_tasks: TaskRegistry,
    reconciliation_failed: AtomicBool,
    tracing_task: std::sync::OnceLock<TaskReceipt<lily_trace::TraceShutdownReport>>,
    owns_tracing: bool,
    reporting_started: std::sync::OnceLock<Instant>,
    coordinator_report: std::sync::OnceLock<reporting::CoordinatorSummary>,
    shutdown_report: std::sync::OnceLock<reporting::ShutdownReport>,
    root_join_observed: AtomicBool,
    server_tasks: TaskRegistry,
    connection_tasks: TaskRegistry,
    maintenance_tasks: TaskRegistry,
    dependency_registration_open: Arc<StdMutex<bool>>,
    tracing_owner: tokio::sync::Mutex<Option<TracingRuntimeOwner>>,
    shutdown_state: Arc<ShutdownState>,
    health: HealthRegistry,
}

struct WsStartWaiterGuard {
    cancellation: CancellationToken,
    armed: bool,
}

impl WsStartWaiterGuard {
    fn new(cancellation: CancellationToken) -> Self {
        Self {
            cancellation,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for WsStartWaiterGuard {
    fn drop(&mut self) {
        if self.armed {
            self.cancellation.cancel();
        }
    }
}

pub(crate) fn has_ambiguous_handshake_headers(
    headers: &tokio_tungstenite::tungstenite::http::HeaderMap,
) -> bool {
    headers.keys().any(|name| {
        !is_repeatable_websocket_header(name.as_str()) && headers.get_all(name).iter().count() > 1
    })
}

pub(crate) fn collect_handshake_headers(
    headers: &tokio_tungstenite::tungstenite::http::HeaderMap,
) -> Result<HashMap<String, String>, WsHeaderError> {
    let mut collected = HashMap::<String, String>::new();
    for (index, (name, value)) in headers.into_iter().enumerate() {
        if index >= MAX_HANDSHAKE_HEADER_ENTRIES {
            return Err(WsHeaderError::TooManyHeaders);
        }
        let value = validate_raw_header(name, value)?;
        let name = name.as_str().to_ascii_lowercase();
        if let Some(existing) = collected.get_mut(&name) {
            if !is_repeatable_websocket_header(&name) {
                return Err(WsHeaderError::DuplicateHeader);
            }
            let merged_len = existing
                .len()
                .checked_add(1)
                .and_then(|len| len.checked_add(value.len()))
                .ok_or(WsHeaderError::MergedHeaderValueTooLarge)?;
            if merged_len > MAX_HANDSHAKE_MERGED_HEADER_VALUE_BYTES {
                return Err(WsHeaderError::MergedHeaderValueTooLarge);
            }
            existing.push(',');
            existing.push_str(value);
        } else {
            collected.insert(name, value.to_owned());
        }
    }
    Ok(collected)
}

/// Immutable per-connection dependencies copied from the application
/// composition root. Grouping them keeps connection admission explicit while
/// avoiding an error-prone positional argument list.
#[derive(Clone)]
struct ConnectionRuntime {
    connection_manager: Arc<ConnectionManager>,
    dispatcher: Arc<WebSocketDispatcher>,
    action_table: Arc<WebSocketActionTable>,
    container: Arc<ApplicationContainer>,
    connection_cleanup_registry: ConnectionCleanupRegistry,
    message_dispatch_registry: MessageDispatchRegistry,
    scope_cleanup_registry: ScopeCleanupRegistry,
    config: ServerConfig,
    shutdown_timeout: Duration,
}

#[derive(Clone)]
struct MessageRuntime {
    message_timeout: Duration,
    cleanup_timeout: Duration,
    cancellation: CancellationToken,
    metrics: Arc<crate::connection::WebSocketServerMetrics>,
}

/// A terminal decision prepared inside the message scope but not published
/// until reverse middleware unwind and deterministic DI cleanup both finish.
#[derive(Debug)]
enum PreparedWebSocketTerminal {
    ApplicationFrame(Message),
    Close {
        frame: CloseFrame<'static>,
        category: crate::middleware::WsConnectionCloseCategory,
    },
}

#[derive(Debug)]
struct ScopedWebSocketDispatch {
    outcome: WsMessageOutcome,
    terminal: Option<PreparedWebSocketTerminal>,
    output: Option<message_reporting::OutputObservation>,
}

/// Retained outside the replaceable execution future. Neither the entered
/// prefix nor the exchange is owned by a pending before/guard/action callback.
struct MessageLifecycleOwner {
    exchange: WsMessageExchange,
    ledger: crate::middleware::WsMessageLedger,
    cleanup_authority: CancellationToken,
}

/// Keep lifecycle state in the scoped operation's output while its caller
/// awaits DI close. Only the surrounding owner may release it after close.
struct OwnedMessageDispatch {
    dispatch: ScopedWebSocketDispatch,
    owner: MessageLifecycleOwner,
}

impl OwnedMessageDispatch {
    fn into_dispatch(self) -> ScopedWebSocketDispatch {
        let Self { dispatch, owner } = self;
        drop(owner);
        dispatch
    }
}

#[derive(Debug)]
struct WebSocketActionResponseContext {
    namespace: String,
    event: String,
    ack_id: Option<String>,
    frame_kind: crate::codec::WebSocketFrameKind,
}

/// Private provenance for failures while normalizing a successful typed
/// action outcome. Invalid typed outcomes remain recoverable application
/// errors, while failures in the selected output codecs make the connection
/// unsafe to continue.
#[derive(Debug)]
enum ActionOutcomePreparationError {
    Recoverable(WebSocketActionError),
    InternalOutput(WebSocketActionError),
}

impl ActionOutcomePreparationError {
    const fn action_error(&self) -> &WebSocketActionError {
        match self {
            Self::Recoverable(error) | Self::InternalOutput(error) => error,
        }
    }
}

impl ScopedWebSocketDispatch {
    const fn new(outcome: WsMessageOutcome, terminal: Option<PreparedWebSocketTerminal>) -> Self {
        Self {
            outcome,
            terminal,
            output: None,
        }
    }

    fn close_category(&self) -> Option<crate::middleware::WsConnectionCloseCategory> {
        match &self.terminal {
            Some(PreparedWebSocketTerminal::Close { category, .. }) => Some(*category),
            _ => None,
        }
    }
}

#[derive(Debug)]
enum OutboundCommand {
    PeerClose,
    Close {
        message: Message,
        category: crate::middleware::WsConnectionCloseCategory,
    },
    Protocol(Message),
    FlushAutomaticControl,
    Heartbeat,
    Data(QueuedApplicationFrame),
    ChannelClosed,
}

struct OutboundMultiplexer {
    data: mpsc::Receiver<QueuedApplicationFrame>,
    control: ConnectionControlReceiver,
}

impl OutboundMultiplexer {
    async fn next(
        &mut self,
        peer_close: &CancellationToken,
        automatic_control_flush: &Notify,
        heartbeat: &mut tokio::time::Interval,
    ) -> OutboundCommand {
        tokio::select! {
            biased;
            _ = peer_close.cancelled() => OutboundCommand::PeerClose,
            control = self.control.next() => match control {
                Ok(ConnectionControlFrame::Close(request)) => {
                    let (message, category) = request.into_parts();
                    OutboundCommand::Close { message, category }
                }
                Ok(ConnectionControlFrame::Protocol(message)) => {
                    OutboundCommand::Protocol(message)
                }
                Err(()) => OutboundCommand::ChannelClosed,
            },
            _ = automatic_control_flush.notified() => OutboundCommand::FlushAutomaticControl,
            _ = heartbeat.tick() => OutboundCommand::Heartbeat,
            data = self.data.recv() => match data {
                Some(message) => OutboundCommand::Data(message),
                None => OutboundCommand::ChannelClosed,
            },
        }
    }
}

#[derive(Debug)]
enum WriterIoFailure {
    TimedOut,
    Transport(tokio_tungstenite::tungstenite::Error),
}

fn tungstenite_error_kind(error: &tokio_tungstenite::tungstenite::Error) -> &'static str {
    use tokio_tungstenite::tungstenite::Error;

    match error {
        Error::ConnectionClosed => "connection_closed",
        Error::AlreadyClosed => "already_closed",
        Error::Io(_) => "io",
        Error::Tls(_) => "tls",
        Error::Capacity(_) => "capacity",
        Error::Protocol(_) => "protocol",
        Error::WriteBufferFull(_) => "write_buffer_full",
        Error::Utf8 => "utf8",
        Error::AttackAttempt => "attack_attempt",
        Error::Url(_) => "url",
        Error::Http(_) => "http",
        Error::HttpFormat(_) => "http_format",
    }
}

fn reader_transport_error_close_frame(
    error: &tokio_tungstenite::tungstenite::Error,
) -> Option<CloseFrame<'static>> {
    use tokio_tungstenite::tungstenite::{
        Error,
        error::{CapacityError, ProtocolError},
        protocol::frame::coding::CloseCode,
    };

    let code = match error {
        Error::Protocol(
            ProtocolError::NonZeroReservedBits
            | ProtocolError::UnmaskedFrameFromClient
            | ProtocolError::MaskedFrameFromServer
            | ProtocolError::FragmentedControlFrame
            | ProtocolError::ControlFrameTooBig
            | ProtocolError::UnknownControlFrameType(_)
            | ProtocolError::UnknownDataFrameType(_)
            | ProtocolError::UnexpectedContinueFrame
            | ProtocolError::ExpectedFragment(_)
            | ProtocolError::InvalidOpcode(_)
            | ProtocolError::InvalidCloseSequence,
        ) => CloseCode::Protocol,
        Error::Utf8 => CloseCode::Invalid,
        Error::Capacity(CapacityError::MessageTooLong { .. }) => CloseCode::Size,
        _ => return None,
    };

    Some(CloseFrame {
        code,
        reason: "".into(),
    })
}

async fn queue_reader_transport_error_close(
    connection_manager: &ConnectionManager,
    connection_id: Uuid,
    error: &tokio_tungstenite::tungstenite::Error,
) -> Result<bool, crate::connection::ConnectionError> {
    let Some(frame) = reader_transport_error_close_frame(error) else {
        return Ok(false);
    };
    connection_manager
        .request_close_frame(
            connection_id,
            Some(frame),
            crate::middleware::WsConnectionCloseCategory::ProtocolError,
        )
        .await
        .map(|_| true)
}

async fn queue_heartbeat_timeout_close(
    connection_manager: &ConnectionManager,
    connection_id: Uuid,
) -> Result<CloseRequestOutcome, crate::connection::ConnectionError> {
    connection_manager
        .request_close_frame(
            connection_id,
            Some(crate::request::WsCloseReason::IdleTimeout.frame()),
            crate::middleware::WsConnectionCloseCategory::IdleTimeout,
        )
        .await
}

impl WriterIoFailure {
    fn kind(&self) -> &'static str {
        match self {
            Self::TimedOut => "timeout",
            Self::Transport(error) => tungstenite_error_kind(error),
        }
    }

    const fn close_category(&self) -> crate::middleware::WsConnectionCloseCategory {
        match self {
            Self::TimedOut => crate::middleware::WsConnectionCloseCategory::WriteTimeout,
            Self::Transport(_) => crate::middleware::WsConnectionCloseCategory::Reset,
        }
    }

    fn record_timeout(&self, metrics: &crate::connection::WebSocketServerMetrics) {
        if matches!(self, Self::TimedOut) {
            metrics.timeout("write");
        }
    }
}

impl std::fmt::Display for WriterIoFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TimedOut => formatter.write_str("WebSocket write deadline exceeded"),
            Self::Transport(error) => {
                write!(formatter, "WebSocket transport write failed: {error}")
            }
        }
    }
}

async fn send_frame_with_deadline<W>(
    writer: &mut W,
    message: Message,
    write_timeout: Duration,
) -> Result<(), WriterIoFailure>
where
    W: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    timeout(write_timeout, writer.send(message))
        .await
        .map_err(|_| WriterIoFailure::TimedOut)?
        .map_err(WriterIoFailure::Transport)
}

async fn send_queued_application_frame_with_deadline<W>(
    writer: &mut W,
    frame: QueuedApplicationFrame,
    write_timeout: Duration,
) -> Result<(), WriterIoFailure>
where
    W: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    // The guard intentionally lives across `SinkExt::send`; that future
    // includes both queueing into the transport sink and flushing it.
    let (message, _reservation) = frame.into_parts();
    send_frame_with_deadline(writer, message, write_timeout).await
}

async fn send_frame_before_deadline<W>(
    writer: &mut W,
    message: Message,
    deadline: Instant,
) -> Result<(), WriterIoFailure>
where
    W: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    timeout_at(deadline, writer.send(message))
        .await
        .map_err(|_| WriterIoFailure::TimedOut)?
        .map_err(WriterIoFailure::Transport)
}

async fn send_queued_application_frame_before_deadline<W>(
    writer: &mut W,
    frame: QueuedApplicationFrame,
    deadline: Instant,
) -> Result<(), WriterIoFailure>
where
    W: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    let (message, _reservation) = frame.into_parts();
    send_frame_before_deadline(writer, message, deadline).await
}

async fn flush_with_deadline<W>(
    writer: &mut W,
    write_timeout: Duration,
) -> Result<(), WriterIoFailure>
where
    W: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    timeout(write_timeout, writer.flush())
        .await
        .map_err(|_| WriterIoFailure::TimedOut)?
        .map_err(WriterIoFailure::Transport)
}

async fn flush_before_deadline<W>(writer: &mut W, deadline: Instant) -> Result<(), WriterIoFailure>
where
    W: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    timeout_at(deadline, writer.flush())
        .await
        .map_err(|_| WriterIoFailure::TimedOut)?
        .map_err(WriterIoFailure::Transport)
}

async fn writer_drain_category<F>(
    writer: F,
    write_timeout: Duration,
) -> Option<crate::middleware::WsConnectionCloseCategory>
where
    F: std::future::Future<Output = crate::middleware::WsConnectionCloseCategory>,
{
    timeout(write_timeout, writer).await.ok()
}

fn record_connection_terminal_category(
    span: &tracing::Span,
    category: crate::middleware::WsConnectionCloseCategory,
) {
    span.record("lily.close_category", category.as_str());
}

fn connection_error_is_identity_expired(error: &crate::connection::ConnectionError) -> bool {
    matches!(
        error,
        crate::connection::ConnectionError::InvalidOperation(
            crate::connection::ConnectionOperationError::IdentityExpired
        )
    )
}

const fn disconnect_reason_from_category(
    category: crate::middleware::WsConnectionCloseCategory,
) -> DisconnectReason {
    use crate::middleware::WsConnectionCloseCategory as Category;
    match category {
        Category::NormalPeer | Category::Reset => DisconnectReason::Peer,
        Category::Application => DisconnectReason::Application,
        Category::ProtocolError => DisconnectReason::Protocol,
        Category::PolicyRejected => DisconnectReason::Policy,
        Category::IdentityExpired => DisconnectReason::IdentityExpired,
        Category::SlowConsumer => DisconnectReason::SlowConsumer,
        Category::IdleTimeout => DisconnectReason::IdleTimeout,
        Category::ServerShutdown | Category::Cancelled => DisconnectReason::ServerShutdown,
        Category::WriteTimeout
        | Category::HandlerError
        | Category::MiddlewareError
        | Category::InternalError => DisconnectReason::Internal,
    }
}

const fn codec_protocol_error(
    kind: WebSocketCodecFailureKind,
) -> crate::request::WsProtocolErrorCode {
    use crate::request::WsProtocolErrorCode as Code;
    match kind {
        WebSocketCodecFailureKind::UnsupportedVersion => Code::UnsupportedVersion,
        WebSocketCodecFailureKind::UnsupportedMessageKind => Code::UnsupportedMessageKind,
        WebSocketCodecFailureKind::InvalidRoute => Code::InvalidRoute,
        WebSocketCodecFailureKind::UnsupportedFrame
        | WebSocketCodecFailureKind::FrameTooLarge
        | WebSocketCodecFailureKind::InvalidFrame
        | WebSocketCodecFailureKind::UnsupportedContent
        | WebSocketCodecFailureKind::InvalidPayload
        | WebSocketCodecFailureKind::Encode
        | WebSocketCodecFailureKind::Internal => Code::InvalidEnvelope,
    }
}

const fn codec_close_reason(kind: WebSocketCodecFailureKind) -> crate::request::WsCloseReason {
    use crate::request::WsCloseReason as Reason;
    match kind {
        WebSocketCodecFailureKind::UnsupportedVersion => Reason::UnsupportedVersion,
        WebSocketCodecFailureKind::UnsupportedMessageKind => Reason::UnsupportedMessageKind,
        WebSocketCodecFailureKind::UnsupportedFrame
        | WebSocketCodecFailureKind::FrameTooLarge
        | WebSocketCodecFailureKind::InvalidFrame
        | WebSocketCodecFailureKind::InvalidRoute
        | WebSocketCodecFailureKind::UnsupportedContent
        | WebSocketCodecFailureKind::InvalidPayload
        | WebSocketCodecFailureKind::Encode
        | WebSocketCodecFailureKind::Internal => Reason::InvalidEnvelope,
    }
}

fn decode_inbound_application_frame(
    frame_codec: &dyn WebSocketFrameCodec,
    message: Message,
    maximum_bytes: usize,
) -> Result<DecodedWebSocketMessage, crate::codec::WebSocketCodecError> {
    catch_unwind(AssertUnwindSafe(|| {
        RawEnvelope::try_from_message(message, maximum_bytes)
            .and_then(|frame| frame_codec.decode_frame(frame))
    }))
    .unwrap_or_else(|_| {
        Err(crate::codec::WebSocketCodecError::new(
            WebSocketCodecFailureKind::Internal,
        ))
    })
}

fn default_terminal_for_non_rejected_outcome(
    outcome: WsMessageOutcome,
) -> Option<PreparedWebSocketTerminal> {
    match outcome {
        WsMessageOutcome::Handled => None,
        WsMessageOutcome::Rejected(_) => {
            unreachable!("routed rejection requires the selected frame and payload codecs")
        }
        WsMessageOutcome::Close(reason) => Some(PreparedWebSocketTerminal::Close {
            frame: reason.frame(),
            category: WsApp::close_category_from_reason(reason),
        }),
        WsMessageOutcome::Failed(_) => Some(PreparedWebSocketTerminal::Close {
            frame: crate::request::WsCloseReason::InternalFailure.frame(),
            category: crate::middleware::WsConnectionCloseCategory::MiddlewareError,
        }),
    }
}

struct WebSocketConnectRuntime<'a> {
    container: &'a Arc<ApplicationContainer>,
    extensions: &'a Arc<Extensions>,
    context: &'a Arc<WebSocketContext>,
    lifecycle: &'a WebSocketLifecycleHandlers,
    stage_timeout: Duration,
    cancellation: &'a CancellationToken,
    scopes: &'a ScopeCleanupRegistry,
    disconnect_ledger: &'a WebSocketDisconnectLedger,
}

enum PublishedConnectionStageOutcome<T> {
    Completed(T),
    IdentityExpired,
    CloseAlreadySelected,
    IdentityStateFailed(crate::connection::ConnectionError),
}

enum PublishedConnectionStageClose {
    Request(CloseFrame<'static>),
    AlreadySelected,
}

async fn run_published_connection_stage<F>(
    connection_id: Uuid,
    connection_manager: &ConnectionManager,
    context: &WebSocketContext,
    cancellation: &CancellationToken,
    stage: F,
) -> PublishedConnectionStageOutcome<F::Output>
where
    F: std::future::Future,
{
    tokio::pin!(stage);
    loop {
        tokio::select! {
            biased;
            revision = context.wait_for_identity_expiry() => {
                match connection_manager
                    .claim_identity_expiry(connection_id, revision)
                    .await
                {
                    Ok(IdentityExpiryClaim::Superseded) => continue,
                    Ok(IdentityExpiryClaim::Closing(CloseRequestOutcome::Requested)) => {
                        cancellation.cancel();
                        let _ = (&mut stage).await;
                        return PublishedConnectionStageOutcome::IdentityExpired;
                    }
                    Ok(IdentityExpiryClaim::Closing(CloseRequestOutcome::AlreadyClosing)) => {
                        cancellation.cancel();
                        let _ = (&mut stage).await;
                        return PublishedConnectionStageOutcome::CloseAlreadySelected;
                    }
                    Err(error) => {
                        cancellation.cancel();
                        let _ = (&mut stage).await;
                        return PublishedConnectionStageOutcome::IdentityStateFailed(error);
                    }
                }
            }
            result = &mut stage => {
                return PublishedConnectionStageOutcome::Completed(result);
            }
        }
    }
}

async fn materialize_published_stage_close(
    connection_id: Uuid,
    connection_manager: &ConnectionManager,
    control: &mut ConnectionControlReceiver,
    selection: PublishedConnectionStageClose,
    requested_category: crate::middleware::WsConnectionCloseCategory,
    wait_timeout: Duration,
) -> Result<
    (Message, crate::middleware::WsConnectionCloseCategory),
    crate::connection::ConnectionError,
> {
    if let PublishedConnectionStageClose::Request(frame) = selection {
        let _ = connection_manager
            .request_close_frame(connection_id, Some(frame), requested_category)
            .await?;
    }
    match timeout(wait_timeout, control.next()).await {
        Ok(Ok(ConnectionControlFrame::Close(request))) => Ok(request.into_parts()),
        Ok(Ok(ConnectionControlFrame::Protocol(_))) | Ok(Err(())) | Err(_) => {
            Err(crate::connection::ConnectionError::ConnectionClosed { connection_id })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WebSocketConnectFailure {
    Cancelled,
    TimedOut,
    Panicked,
    Handler,
    Scope,
}

impl WebSocketConnectFailure {
    const fn close_category(self) -> crate::middleware::WsConnectionCloseCategory {
        match self {
            Self::Cancelled => crate::middleware::WsConnectionCloseCategory::ServerShutdown,
            Self::TimedOut | Self::Panicked | Self::Handler | Self::Scope => {
                crate::middleware::WsConnectionCloseCategory::HandlerError
            }
        }
    }

    fn into_server_error(self) -> ServerError {
        let message = match self {
            Self::Cancelled => "WebSocket on_connected lifecycle was cancelled",
            Self::TimedOut => "WebSocket on_connected lifecycle timed out",
            Self::Panicked => "WebSocket on_connected lifecycle panicked",
            Self::Handler => "WebSocket on_connected lifecycle failed",
            Self::Scope => "WebSocket connect scope cleanup failed",
        };
        ServerError::handler_error(message.to_owned())
    }
}

pub(crate) const fn connection_middleware_terminal(
    kind: crate::middleware::WsMiddlewareFailureKind,
) -> (
    crate::middleware::WsConnectionCloseCategory,
    crate::request::WsCloseReason,
) {
    match kind {
        crate::middleware::WsMiddlewareFailureKind::Rejected => (
            crate::middleware::WsConnectionCloseCategory::PolicyRejected,
            crate::request::WsCloseReason::PolicyViolation,
        ),
        crate::middleware::WsMiddlewareFailureKind::Cancelled => (
            crate::middleware::WsConnectionCloseCategory::ServerShutdown,
            crate::request::WsCloseReason::ServerShutdown,
        ),
        crate::middleware::WsMiddlewareFailureKind::Timeout
        | crate::middleware::WsMiddlewareFailureKind::Internal => (
            crate::middleware::WsConnectionCloseCategory::MiddlewareError,
            crate::request::WsCloseReason::InternalFailure,
        ),
    }
}

struct AcceptedConnection {
    addr: SocketAddr,
    connection_id: Uuid,
    permit: OwnedSemaphorePermit,
    span: tracing::Span,
}

struct ConnectionHandshake {
    cancellation: CancellationToken,
    started: Instant,
    deadline: Instant,
    timeout: Duration,
    transport_security: WsTransportSecurity,
    span: tracing::Span,
}

#[derive(Debug, Default)]
struct ConnectionTaskDrain {
    completed: usize,
    pending_at_deadline: usize,
    abort_requested: usize,
    unjoined: usize,
    cancelled: usize,
    timed_out: bool,
    forced: bool,
    join_errors: Vec<String>,
}

struct ManagedWsServer {
    admission: CancellationToken,
    message_admission: CancellationToken,
    force: CancellationToken,
    task: TaskReceipt<Result<(), ServerError>>,
    outcome: Option<Result<(), String>>,
    runtime_result_observed: bool,
}

fn replayable_server_error_detail(error: &ServerError) -> String {
    match error {
        // `ManagedWsServer` and the application terminal slot replay an
        // already-observed result through the stable ConnectionError
        // category. Retain only its inner detail so each replay does not add
        // another `Connection error:` prefix.
        ServerError::ConnectionError(detail) => detail.to_string(),
        _ => error.to_string(),
    }
}

impl ManagedWsServer {
    async fn wait_for_runtime(&mut self) -> Result<(), ServerError> {
        let result = self.wait().await;
        self.runtime_result_observed = true;
        result
    }

    async fn wait(&mut self) -> Result<(), ServerError> {
        if let Some(outcome) = &self.outcome {
            return outcome.clone().map_err(ServerError::connection_error);
        }
        let outcome = match self.task.clone().await {
            Ok(result) => result
                .as_ref()
                .as_ref()
                .map(|_| ())
                .map_err(replayable_server_error_detail),
            Err(error) => {
                let message = if error.is_cancelled() {
                    "WebSocket server task was cancelled"
                } else if error.is_panic() {
                    "WebSocket server task panicked"
                } else {
                    "WebSocket server task failed"
                };
                Err(message.to_string())
            }
        };
        self.outcome = Some(outcome.clone());
        outcome.map_err(ServerError::connection_error)
    }

    async fn drain(&mut self) -> Result<(), ServerError> {
        // Only the main lifecycle select separately retains a runtime failure.
        // A failure first observed by drain (including cleanup timeout) must
        // remain a failure when the coordinator subsequently retries via force.
        if self.runtime_result_observed {
            return Ok(());
        }
        self.wait().await
    }
}

impl Drop for ManagedWsServer {
    fn drop(&mut self) {
        self.admission.cancel();
        self.message_admission.cancel();
        self.force.cancel();
        // Drop requests the existing cooperative protocol. The application
        // task registry retains the join and owns the final abort decision.
    }
}

struct WsServerLifecycleHandle {
    phase: FrameworkShutdownPhase,
    server: Arc<tokio::sync::Mutex<ManagedWsServer>>,
    admission: CancellationToken,
    message_admission: CancellationToken,
    force: CancellationToken,
    timeout: Duration,
    budget: crate::shutdown::ShutdownBudget,
}

struct WsDispatcherLifecycleHandle {
    phase: FrameworkShutdownPhase,
    dispatcher: Arc<WebSocketDispatcher>,
    timeout: Duration,
}

#[async_trait::async_trait]
impl FrameworkShutdownComponent for WsDispatcherLifecycleHandle {
    async fn shutdown(&mut self) -> Result<(), ShutdownError> {
        match self.phase {
            FrameworkShutdownPhase::StopAdmission => {
                self.dispatcher.begin_drain();
                Ok(())
            }
            FrameworkShutdownPhase::DrainInFlight => {
                self.dispatcher.wait_drained().await;
                Ok(())
            }
            _ => unreachable!("WebSocket dispatcher registered in an invalid shutdown phase"),
        }
    }

    fn name(&self) -> &str {
        match self.phase {
            FrameworkShutdownPhase::StopAdmission => "websocket-dispatch-admission",
            FrameworkShutdownPhase::DrainInFlight => "websocket-dispatch-drain",
            _ => "invalid-websocket-dispatch-phase",
        }
    }

    fn phase(&self) -> FrameworkShutdownPhase {
        self.phase
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }

    fn set_shutdown_deadlines(&mut self, graceful: Instant, hard: Instant) {
        self.dispatcher.set_shutdown_deadlines(graceful, hard);
    }

    fn set_force_deadline(&mut self, deadline: Instant) {
        self.dispatcher.set_force_deadline(deadline);
    }

    fn force_shutdown(&mut self) -> Option<lily_shutdown::FrameworkForceShutdownFuture<'_>> {
        self.dispatcher.force_drain();
        Some(Box::pin(async move { self.shutdown().await }))
    }
}

#[async_trait::async_trait]
impl FrameworkShutdownComponent for WsServerLifecycleHandle {
    async fn shutdown(&mut self) -> Result<(), ShutdownError> {
        match self.phase {
            FrameworkShutdownPhase::StopAdmission => {
                self.admission.cancel();
                self.message_admission.cancel();
                Ok(())
            }
            FrameworkShutdownPhase::DrainInFlight => {
                self.message_admission.cancel();
                self.budget
                    .observe_cleanup(async { self.server.lock().await.drain().await })
                    .await
                    .map_err(|_| {
                        ShutdownError::Component(
                            "server cleanup cutoff elapsed; final joins remain owned".into(),
                        )
                    })?
                    .map_err(|error| ShutdownError::Component(error.to_string()))
            }
            _ => unreachable!("WebSocket lifecycle handle registered in an invalid phase"),
        }
    }

    fn name(&self) -> &str {
        match self.phase {
            FrameworkShutdownPhase::StopAdmission => "websocket-connection-admission",
            FrameworkShutdownPhase::DrainInFlight => "websocket-connection-drain",
            _ => "invalid-websocket-lifecycle-phase",
        }
    }

    fn phase(&self) -> FrameworkShutdownPhase {
        self.phase
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }

    fn set_shutdown_deadlines(&mut self, graceful: Instant, hard: Instant) {
        self.budget
            .configure(graceful, reconciliation::users_deadline(graceful, hard));
    }

    fn set_force_deadline(&mut self, deadline: Instant) {
        self.budget.force_before(deadline);
    }

    fn force_shutdown(&mut self) -> Option<lily_shutdown::FrameworkForceShutdownFuture<'_>> {
        self.admission.cancel();
        self.message_admission.cancel();
        self.force.cancel();
        Some(Box::pin(async move { self.shutdown().await }))
    }
}

impl WsApp {
    async fn stop_backplane_ingress(
        task: &mut Option<WebSocketBackplaneIngressTask>,
    ) -> Result<(), String> {
        let Some(task) = task.take() else {
            return Ok(());
        };
        task.stop().await.map_err(|error| {
            if error.is_panic() {
                "WebSocket backplane ingress task panicked".to_owned()
            } else {
                "WebSocket backplane ingress task join failed".to_owned()
            }
        })
    }

    fn close_dependency_registration(&self) {
        *self
            .lifecycle
            .dependency_registration_open
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = false;
    }

    async fn complete_lifecycle(lifecycle: &Arc<WsAppLifecycle>, result: Result<(), String>) {
        let mut terminal = lifecycle.terminal_result.lock().await;
        if terminal.is_none() {
            *terminal = Some(result);
            lifecycle.phase.store(WS_APP_TERMINAL, Ordering::Release);
        }
        drop(terminal);
        lifecycle.terminal_notify.notify_waiters();
    }

    async fn await_terminal(&self) -> Result<(), ServerError> {
        loop {
            let notified = self.lifecycle.terminal_notify.notified();
            tokio::pin!(notified);
            // `notify_waiters` has no stored permit. Enabling the waiter before
            // reading terminal state closes the publish-between-check-and-await
            // race while the terminal result remains the replay source.
            notified.as_mut().enable();
            if let Some(result) = self.lifecycle.terminal_result.lock().await.clone() {
                if let Some(root) = self.lifecycle.root_task.get() {
                    root.clone().await.map_err(|_| {
                        ServerError::connection_error(
                            "WebSocket lifecycle root task failed to join normally".to_owned(),
                        )
                    })?;
                    if !self
                        .lifecycle
                        .root_join_observed
                        .swap(true, Ordering::AcqRel)
                    {
                        reporting::diagnostic(|| {
                            tracing::info!(target: "lily_websocket::shutdown",
                            root_join_observed = true, "WebSocket lifecycle root joined")
                        });
                    }
                }
                return result.map_err(ServerError::connection_error);
            }
            notified.as_mut().await;
        }
    }

    async fn close_never_started(runtime: Self) -> Result<(), String> {
        runtime.begin_shutdown_reporting();
        let lifecycle = &runtime.lifecycle;
        let _ = lifecycle.health.update(
            WS_LISTENER_HEALTH_CHECK,
            HealthStatus::Unhealthy,
            "closed_without_start",
        );
        let mut coordinator = FrameworkShutdownCoordinator::new(
            Arc::clone(&lifecycle.shutdown_state),
            runtime.shutdown_timeout,
        );
        coordinator.register(WsDispatcherLifecycleHandle {
            phase: FrameworkShutdownPhase::StopAdmission,
            dispatcher: Arc::clone(&runtime.dispatcher),
            timeout: runtime.shutdown_timeout,
        });
        coordinator.register(WsDispatcherLifecycleHandle {
            phase: FrameworkShutdownPhase::DrainInFlight,
            dispatcher: Arc::clone(&runtime.dispatcher),
            timeout: runtime.shutdown_timeout,
        });
        runtime.register_reconciliation(&mut coordinator);
        runtime.register_dependencies(&mut coordinator);
        let report = coordinator.execute_report(ShutdownSignal::Manual).await;
        runtime.record_coordinator(&report);
        if report.is_terminal_complete() && report.reconciles() {
            let _ = lifecycle.health.update(
                WS_DISPATCHER_HEALTH_CHECK,
                HealthStatus::Degraded,
                "resources_closed",
            );
            if runtime.owns_container {
                let _ = lifecycle.health.update(
                    WS_DI_HEALTH_CHECK,
                    HealthStatus::Degraded,
                    "resources_closed",
                );
            }
            Ok(())
        } else {
            Err(format!(
                "WebSocket application close was incomplete: {report:?}"
            ))
        }
    }

    /// Closes this application even when it was built but never started.
    ///
    /// The first concurrent `start` or `close` call owns lifecycle cleanup.
    /// When `start` already owns it, `close` requests the canonical graceful
    /// shutdown and awaits the same terminal result. Repeated or concurrent
    /// close calls are idempotent. A caller-supplied DI container is never
    /// disposed by the application.
    pub async fn close(&self) -> Result<(), ServerError> {
        loop {
            match self.lifecycle.phase.load(Ordering::Acquire) {
                WS_APP_BUILT => {
                    if self
                        .lifecycle
                        .phase
                        .compare_exchange(
                            WS_APP_BUILT,
                            WS_APP_CLOSING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    self.close_dependency_registration();
                    let runtime = self.runtime_clone();
                    let (root_registered, registered) = tokio::sync::oneshot::channel();
                    let root = tokio::spawn(async move {
                        // Publish the join owner before this worker can publish
                        // a result observed by another concurrent close caller.
                        let _ = registered.await;
                        let result = match AssertUnwindSafe(Self::close_never_started(
                            runtime.runtime_clone(),
                        ))
                        .catch_unwind()
                        .await
                        {
                            Ok(result) => result,
                            Err(_) => Err(format!(
                                "WebSocket application close supervisor panicked; {}",
                                runtime.recover_root_failure().await
                            )),
                        };
                        runtime.finish_shutdown(result).await;
                    });
                    assert!(
                        self.lifecycle
                            .root_task
                            .set(TaskReceipt::from(root))
                            .is_ok()
                    );
                    let _ = root_registered.send(());
                    return self.await_terminal().await;
                }
                WS_APP_RUNNING => {
                    self.lifecycle.close_cancellation.cancel();
                    return self.await_terminal().await;
                }
                WS_APP_CLOSING | WS_APP_TERMINAL => return self.await_terminal().await,
                _ => unreachable!("WebSocket application lifecycle phase is invalid"),
            }
        }
    }

    /// Returns a restricted updater for application-owned critical dependencies.
    ///
    /// Dependency registration closes atomically when either [`Self::start`] or
    /// [`Self::close`] claims the lifecycle. Status updates for already
    /// registered dependencies remain available to application health probes.
    pub fn required_dependency_health(&self) -> WsDependencyHealth {
        WsDependencyHealth {
            registry: self.lifecycle.health.clone(),
            registration_open: Arc::clone(&self.lifecycle.dependency_registration_open),
        }
    }

    /// Captures the latest bounded application health snapshot.
    pub fn health_snapshot(&self) -> Result<HealthSnapshot, HealthRegistryError> {
        self.lifecycle.health.snapshot()
    }

    /// Return a payload-free, low-cardinality snapshot of the server's
    /// terminal connection and message accounting.
    pub fn metrics_snapshot(&self) -> crate::connection::WebSocketServerMetricSnapshot {
        self.connection_manager.metrics_snapshot()
    }

    /// Return the number of published connections across all of this application's
    /// namespaces on this node. Closing connections count until registry removal.
    ///
    /// For one controller namespace, use [`crate::WebSocketClients::count`].
    /// Neither count aggregates connections on other backplane nodes.
    ///
    /// Qualification and health reporters use this after shutdown to verify
    /// that no connection remains registered; it does not expose connection
    /// identifiers or transport handles.
    pub async fn active_connection_count(&self) -> usize {
        self.connection_manager.connection_count().await
    }

    /// Returns the immutable TLS configuration selected by the composition
    /// root, or `None` for the existing plaintext listener.
    pub fn rustls_config(&self) -> Option<&RustlsConfig> {
        self.effective_server.rustls_config()
    }

    /// Effective socket address used by [`Self::start`].
    pub fn listen_address(&self) -> &str {
        self.effective_server.listen_address()
    }

    /// Validated WebSocket server policy.
    pub fn server_config(&self) -> &ServerConfig {
        self.effective_server.server()
    }

    /// Complete resolved listener configuration, including TLS state.
    pub fn effective_server_config(&self) -> &EffectiveWsServerConfig {
        &self.effective_server
    }

    fn runtime_clone(&self) -> Self {
        Self {
            effective_server: Arc::clone(&self.effective_server),
            action_table: Arc::clone(&self.action_table),
            connection_manager: Arc::clone(&self.connection_manager),
            dispatcher: Arc::clone(&self.dispatcher),
            container: Arc::clone(&self.container),
            owns_container: self.owns_container,
            shutdown_timeout: self.shutdown_timeout,
            connection_cleanup_registry: self.connection_cleanup_registry.clone(),
            message_dispatch_registry: self.message_dispatch_registry.clone(),
            scope_cleanup_registry: self.scope_cleanup_registry.clone(),
            connection_permits: Arc::clone(&self.connection_permits),
            lifecycle: Arc::clone(&self.lifecycle),
        }
    }

    fn connection_runtime(&self) -> ConnectionRuntime {
        ConnectionRuntime {
            connection_manager: Arc::clone(&self.connection_manager),
            dispatcher: Arc::clone(&self.dispatcher),
            action_table: Arc::clone(&self.action_table),
            container: Arc::clone(&self.container),
            connection_cleanup_registry: self.connection_cleanup_registry.clone(),
            message_dispatch_registry: self.message_dispatch_registry.clone(),
            scope_cleanup_registry: self.scope_cleanup_registry.clone(),
            config: self.server_config().clone(),
            shutdown_timeout: self.shutdown_timeout,
        }
    }

    /// Start the WebSocket server
    pub async fn start(&self) -> Result<(), ServerError> {
        self.start_with_cancellation(CancellationToken::new()).await
    }

    /// Start the server with an application-owned cancellation source. This
    /// is the canonical path for orchestrators and tests. Platform signals and
    /// caller cancellation both enter the same ordered lifecycle report.
    pub async fn start_with_cancellation(
        &self,
        cancellation: CancellationToken,
    ) -> Result<(), ServerError> {
        if self
            .lifecycle
            .phase
            .compare_exchange(
                WS_APP_BUILT,
                WS_APP_RUNNING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err(ServerError::connection_error(
                "this WebSocket application lifecycle has already been started or closed"
                    .to_string(),
            ));
        }
        self.close_dependency_registration();

        let mut waiter = WsStartWaiterGuard::new(self.lifecycle.close_cancellation.clone());
        let runtime = self.runtime_clone();
        let (root_registered, registered) = tokio::sync::oneshot::channel();
        let supervisor = tokio::spawn(async move {
            let _ = registered.await;
            let result = AssertUnwindSafe(runtime.run_started_with_cancellation(cancellation))
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    Err(ServerError::connection_error(
                        "WebSocket application lifecycle supervisor panicked".to_string(),
                    ))
                });
            let terminal = match &result {
                Ok(()) => Ok(()),
                Err(error) => Err(replayable_server_error_detail(error)),
            };
            runtime.finish_shutdown(terminal).await;
        });
        assert!(
            self.lifecycle
                .root_task
                .set(TaskReceipt::from(supervisor))
                .is_ok()
        );
        let _ = root_registered.send(());
        let result = self.await_terminal().await;
        waiter.disarm();
        result
    }

    async fn run_started_with_cancellation(
        &self,
        cancellation: CancellationToken,
    ) -> Result<(), ServerError> {
        let shutdown_state = Arc::clone(&self.lifecycle.shutdown_state);
        let mut signal_monitor = match SignalHandler::install(Arc::clone(&shutdown_state)).await {
            Ok(monitor) => monitor,
            Err(error) => {
                let _ = shutdown_state.initiate_shutdown(ShutdownSignal::Manual);
                let _ = self
                    .lifecycle
                    .health
                    .record_lifecycle_failure(WS_LISTENER_HEALTH_CHECK, "signal_install_failed");
                let _ = self.lifecycle.health.update(
                    WS_DISPATCHER_HEALTH_CHECK,
                    HealthStatus::Degraded,
                    "resources_closed",
                );
                let mut failures =
                    vec![format!("OS signal handler initialization failed: {error}")];
                if let Err(cleanup) = Self::close_never_started(self.runtime_clone()).await {
                    failures.push(cleanup);
                }
                return Err(ServerError::connection_error(failures.join("; ")));
            }
        };

        let runtime_result =
            match AssertUnwindSafe(self.run_with_signal_monitor(&mut signal_monitor, cancellation))
                .catch_unwind()
                .await
            {
                Ok(result) => result,
                Err(_) => {
                    let recovery = self.recover_root_failure().await;
                    Err(ServerError::connection_error(format!(
                        "WebSocket lifecycle supervisor panicked; {recovery}"
                    )))
                }
            };
        let signal_cleanup = match signal_monitor.into_stop_task() {
            Some(task) => {
                let receipt = self.lifecycle.signal_tasks.track(task);
                // SignalMonitor already requested stop; retain the request
                // separately from its subsequently observed cancelled join.
                receipt.abort();
                match self.lifecycle.root_budget.reconcile(receipt).await {
                    Ok(Ok(result)) if result.is_ok() => Ok(()),
                    Ok(Err(error)) if error.is_cancelled() => Ok(()),
                    _ => Err("signal monitor termination unconfirmed or failed"),
                }
            }
            None => Ok(()),
        };
        match signal_cleanup {
            Ok(()) => runtime_result,
            Err(error) => Err(ServerError::connection_error(format!(
                "{error}; lifecycle result: {runtime_result:?}"
            ))),
        }
    }

    async fn run_with_signal_monitor(
        &self,
        signal_monitor: &mut lily_shutdown::SignalMonitor,
        cancellation: CancellationToken,
    ) -> Result<(), ServerError> {
        if cancellation.is_cancelled()
            || self.lifecycle.close_cancellation.is_cancelled()
            || self.lifecycle.shutdown_state.is_shutdown_initiated()
        {
            return Self::close_never_started(self.runtime_clone())
                .await
                .map_err(ServerError::connection_error);
        }
        let shutdown_state = Arc::clone(&self.lifecycle.shutdown_state);
        let admission = CancellationToken::new();
        let message_admission = CancellationToken::new();
        let force = CancellationToken::new();
        assert!(self.lifecycle.execution_force.set(force.clone()).is_ok());
        let runtime = self.runtime_clone();
        let task_admission = admission.clone();
        let task_message_admission = message_admission.clone();
        let task_force = force.clone();
        let task: tokio::task::JoinHandle<Result<(), ServerError>> =
            lily_trace::spawn(async move {
                runtime
                    .serve_until_shutdown(task_admission, task_message_admission, task_force)
                    .await
            });
        let server = Arc::new(tokio::sync::Mutex::new(ManagedWsServer {
            admission: admission.clone(),
            message_admission: message_admission.clone(),
            force: force.clone(),
            task: self.lifecycle.server_tasks.track(task),
            outcome: None,
            runtime_result_observed: false,
        }));

        let (shutdown_signal, runtime_result) = tokio::select! {
            _ = self.wait_for_background_failure() => {
                let _ = self.lifecycle.health.record_lifecycle_failure(
                    background::HEALTH_CHECK, "execution_failed",
                );
                (ShutdownSignal::Manual, Err(ServerError::connection_error(
                    "Background service failed; stopping WebSocket host".into(),
                )))
            },
            result = async { server.lock().await.wait_for_runtime().await } => {
                (ShutdownSignal::Manual, result)
            }
            signal = signal_monitor.wait_for_first() => match signal {
                Ok(signal) => (signal, Ok(())),
                Err(error) => (ShutdownSignal::Manual, Err(ServerError::IoError(error))),
            },
            _ = self.lifecycle.close_cancellation.cancelled() => {
                (ShutdownSignal::Manual, Ok(()))
            },
            _ = cancellation.cancelled() => (ShutdownSignal::Manual, Ok(())),
        };
        // Stop producers before exposing drain continuation to accepted work.
        self.begin_shutdown_reporting();
        // These signals do not cancel an already-admitted user execution.
        admission.cancel();
        message_admission.cancel();
        self.dispatcher.begin_drain();

        let mut coordinator =
            FrameworkShutdownCoordinator::new(Arc::clone(&shutdown_state), self.shutdown_timeout);
        coordinator.register(WsDispatcherLifecycleHandle {
            phase: FrameworkShutdownPhase::StopAdmission,
            dispatcher: Arc::clone(&self.dispatcher),
            timeout: self.shutdown_timeout,
        });
        coordinator.register(WsServerLifecycleHandle {
            phase: FrameworkShutdownPhase::StopAdmission,
            server: Arc::clone(&server),
            admission: admission.clone(),
            message_admission: message_admission.clone(),
            force: force.clone(),
            timeout: self.shutdown_timeout,
            budget: self.scope_cleanup_registry.budget.clone(),
        });
        coordinator.register(WsDispatcherLifecycleHandle {
            phase: FrameworkShutdownPhase::DrainInFlight,
            dispatcher: Arc::clone(&self.dispatcher),
            timeout: self.shutdown_timeout,
        });
        self.register_reconciliation(&mut coordinator);
        coordinator.register(WsServerLifecycleHandle {
            phase: FrameworkShutdownPhase::DrainInFlight,
            server,
            admission,
            message_admission,
            force,
            timeout: self.shutdown_timeout,
            budget: self.scope_cleanup_registry.budget.clone(),
        });
        self.register_dependencies(&mut coordinator);

        let report = coordinator.execute_report(shutdown_signal).await;
        self.record_coordinator(&report);
        if self.owns_container
            && report.is_terminal_complete()
            && report.reconciles()
            && let Err(error) = self.lifecycle.health.update(
                WS_DI_HEALTH_CHECK,
                HealthStatus::Degraded,
                "resources_closed",
            )
        {
            tracing::error!(%error, "WebSocket DI terminal health publication failed");
        }
        let lifecycle_result = if report.is_terminal_complete() && report.reconciles() {
            Ok(())
        } else {
            Err(ServerError::connection_error(format!(
                "WebSocket framework shutdown was incomplete: {report:?}"
            )))
        };
        match (runtime_result, lifecycle_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(runtime_error), Ok(())) => Err(runtime_error),
            (Ok(()), Err(lifecycle_error)) => Err(lifecycle_error),
            (Err(runtime_error), Err(lifecycle_error)) => Err(ServerError::connection_error(
                format!("{runtime_error}; {lifecycle_error}"),
            )),
        }
    }

    fn maintenance_task_failure(
        &self,
        completed: Option<Result<(), Arc<tokio::task::JoinError>>>,
    ) -> ServerError {
        let failure = match completed {
            Some(Ok(())) => "WebSocket maintenance task stopped unexpectedly".to_string(),
            Some(Err(error)) if error.is_panic() => {
                "WebSocket maintenance task panicked".to_string()
            }
            Some(Err(_)) => "WebSocket maintenance task join failed".to_string(),
            None => "WebSocket maintenance task registry closed unexpectedly".to_string(),
        };
        let _ = self
            .lifecycle
            .health
            .record_lifecycle_failure(WS_LISTENER_HEALTH_CHECK, "maintenance_failed");
        ServerError::connection_error(failure)
    }

    /// Run the listener until cancellation or an admission failure. Dropping the
    /// listener when this future returns stops new WebSocket connections before
    /// the owned DI container begins draining scopes.
    async fn serve_until_shutdown(
        &self,
        cancellation: CancellationToken,
        message_admission: CancellationToken,
        force: CancellationToken,
    ) -> Result<(), ServerError> {
        tracing::debug!("Binding WebSocket listener");
        let listener = match TcpListener::bind(self.listen_address()).await {
            Ok(listener) => listener,
            Err(error) => {
                let _ = self
                    .lifecycle
                    .health
                    .record_lifecycle_failure(WS_LISTENER_HEALTH_CHECK, "bind_failed");
                let _ = self.lifecycle.health.update(
                    WS_DISPATCHER_HEALTH_CHECK,
                    HealthStatus::Degraded,
                    "resources_closed",
                );
                return Err(ServerError::IoError(error));
            }
        };
        let tls_acceptor = self.rustls_config().map(Self::tls_acceptor);

        // The subscription task exists before process readiness. Required
        // backplanes must prove that their inbound subscription is active;
        // optional backplanes remain observable but never gate local traffic.
        let mut backplane_ingress_task = self.dispatcher.spawn_ingress();
        if self.dispatcher.has_active_backplane()
            && self.dispatcher.backplane_requirement() == Some(BackplaneRequirement::Required)
        {
            let subscription_ready = tokio::select! {
                result = timeout(
                    self.shutdown_timeout,
                    self.dispatcher.wait_for_subscription_ready(),
                ) => Some(result),
                _ = cancellation.cancelled() => None,
                _ = force.cancelled() => None,
            };
            match subscription_ready {
                None => {
                    let stop = Self::stop_backplane_ingress(&mut backplane_ingress_task).await;
                    if let Err(error) = stop {
                        return Err(ServerError::connection_error(error));
                    }
                    return Ok(());
                }
                Some(Err(_)) => {
                    let _ = self.lifecycle.health.update(
                        WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
                        HealthStatus::Unhealthy,
                        "startup_timeout",
                    );
                    let mut failure =
                        "required WebSocket backplane subscription readiness timed out".to_owned();
                    if let Err(stop_error) =
                        Self::stop_backplane_ingress(&mut backplane_ingress_task).await
                    {
                        failure.push_str("; ");
                        failure.push_str(&stop_error);
                    }
                    return Err(ServerError::connection_error(failure));
                }
                Some(Ok(Err(error))) => {
                    let reason = error.kind().as_str();
                    let _ = self.lifecycle.health.update(
                        WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
                        HealthStatus::Unhealthy,
                        reason,
                    );
                    let mut failure = format!(
                        "required WebSocket backplane subscription failed before readiness ({reason})"
                    );
                    if let Err(stop_error) =
                        Self::stop_backplane_ingress(&mut backplane_ingress_task).await
                    {
                        failure.push_str("; ");
                        failure.push_str(&stop_error);
                    }
                    return Err(ServerError::connection_error(failure));
                }
                Some(Ok(Ok(()))) => {}
            }
        }

        if let Err(error) = self.lifecycle.health.update(
            WS_LISTENER_HEALTH_CHECK,
            HealthStatus::Healthy,
            "accepting_connections",
        ) {
            let mut failure = format!("listener readiness publication failed: {error}");
            if let Err(stop_error) = Self::stop_backplane_ingress(&mut backplane_ingress_task).await
            {
                failure.push_str("; ");
                failure.push_str(&stop_error);
            }
            return Err(ServerError::configuration(failure));
        }
        // A shutdown racing subscription readiness must not release prepared
        // workers. The background runtime also serializes start with stop.
        if cancellation.is_cancelled() || self.lifecycle.shutdown_state.is_shutdown_initiated() {
            return Self::stop_backplane_ingress(&mut backplane_ingress_task)
                .await
                .map_err(ServerError::connection_error);
        }
        if let Some(background) = &self.lifecycle.background
            && let Err(error) = background.runtime.start()
        {
            let _ = Self::stop_backplane_ingress(&mut backplane_ingress_task).await;
            if cancellation.is_cancelled() || self.lifecycle.shutdown_state.is_shutdown_initiated()
            {
                return Ok(());
            }
            return Err(error.into());
        }
        if let Err(error) = self.lifecycle.shutdown_state.publish_ready() {
            if cancellation.is_cancelled()
                || self.lifecycle.shutdown_state.is_shutdown_initiated()
                || !self.lifecycle.shutdown_state.is_accepting_connections()
            {
                let _ = self.lifecycle.health.update(
                    WS_LISTENER_HEALTH_CHECK,
                    HealthStatus::Degraded,
                    "closed_before_ready",
                );
                let _ = self.lifecycle.health.update(
                    WS_DISPATCHER_HEALTH_CHECK,
                    HealthStatus::Degraded,
                    "resources_closed",
                );
                return match Self::stop_backplane_ingress(&mut backplane_ingress_task).await {
                    Ok(()) => Ok(()),
                    Err(error) => Err(ServerError::connection_error(error)),
                };
            }
            let _ = self
                .lifecycle
                .health
                .record_lifecycle_failure(WS_LISTENER_HEALTH_CHECK, "readiness_publication_failed");
            let mut failure = format!("WebSocket listener readiness publication failed: {error}");
            if let Err(stop_error) = Self::stop_backplane_ingress(&mut backplane_ingress_task).await
            {
                failure.push_str("; ");
                failure.push_str(&stop_error);
            }
            return Err(ServerError::connection_error(failure));
        }

        tracing::info!(
            tls.enabled = tls_acceptor.is_some(),
            lily.websocket.max_connections =
                i64::try_from(self.server_config().max_connections).unwrap_or(i64::MAX),
            lily.websocket.max_message_size_bytes =
                i64::try_from(self.server_config().max_message_size).unwrap_or(i64::MAX),
            lily.websocket.max_outbound_message_size_bytes =
                i64::try_from(self.server_config().max_outbound_message_size).unwrap_or(i64::MAX),
            lily.websocket.outbound_queue_capacity =
                i64::try_from(self.server_config().outbound_queue_capacity).unwrap_or(i64::MAX),
            lily.websocket.outbound_queue_max_bytes =
                i64::try_from(self.server_config().outbound_queue_max_bytes).unwrap_or(i64::MAX),
            lily.websocket.outbound_admission_timeout_millis =
                i64::try_from(self.server_config().outbound_admission_timeout_millis)
                    .unwrap_or(i64::MAX),
            lily.websocket.action_count =
                i64::try_from(self.action_table.action_count()).unwrap_or(i64::MAX),
            "WebSocket listener is ready"
        );

        // Both maintenance and accepted connections remain owned by this
        // composition root. Neither class of task may outlive `start` and race
        // the DI container's root disposal.
        let mut cleanup_tasks =
            OwnedTaskSet::with_registry(self.lifecycle.maintenance_tasks.clone());
        let mut connection_tasks =
            OwnedTaskSet::with_registry(self.lifecycle.connection_tasks.clone());
        let mut connection_task_errors = Vec::new();
        self.start_cleanup_task(&mut cleanup_tasks);

        let serve_result = loop {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => break Ok(()),
                completed = cleanup_tasks.join_next(), if !cleanup_tasks.is_empty() => {
                    break Err(self.maintenance_task_failure(completed));
                }
                completed = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                    if let Some(Err(error)) = completed
                        && !error.is_cancelled()
                    {
                        connection_task_errors.push(if error.is_panic() {
                            "WebSocket connection task panicked".to_string()
                        } else {
                            "WebSocket connection task join failed".to_string()
                        });
                    }
                }
                accepted = listener.accept() => {
                    let (stream, addr) = match accepted {
                        Ok(connection) => connection,
                        Err(error) => {
                            let _ = self.lifecycle.health.record_lifecycle_failure(
                                WS_LISTENER_HEALTH_CHECK,
                                "listener_failed",
                            );
                            break Err(ServerError::IoError(error));
                        }
                    };

                    // The source may be cancelled between select's readiness
                    // checks and accept completing on another worker. This is
                    // the admission decision: no permit/user task after stop.
                    if cancellation.is_cancelled() {
                        break Ok(());
                    }

                    // WebSocket envelopes are small latency-sensitive writes.
                    // Configure the accepted side before the HTTP Upgrade so
                    // Nagle cannot couple the first message to delayed ACK.
                    if let Err(error) = stream.set_nodelay(true) {
                        tracing::warn!(%error, "failed to enable TCP_NODELAY for WebSocket connection");
                        continue;
                    }

                    let admission_started = Instant::now();
                    let permit = match self.connection_permits.clone().try_acquire_owned() {
                        Ok(permit) => {
                            self.connection_manager
                                .metrics()
                                .connection_admission("accepted", admission_started.elapsed());
                            permit
                        }
                        Err(_) => {
                            self.connection_manager
                                .metrics()
                                .connection_admission("rejected", admission_started.elapsed());
                            tracing::debug!(
                                lily.outcome = "capacity_rejected",
                                "WebSocket connection admission rejected"
                            );
                            continue;
                        }
                    };

                    let connection_id = Uuid::new_v4();

                    let runtime = self.connection_runtime();
                    let tls_acceptor = tls_acceptor.clone();
                    // A connection may cancel its own execution path without
                    // ever cancelling the process-wide listener barrier.
                    let handshake_cancellation = force.child_token();
                    let connection_message_admission = message_admission.clone();
                    let connection_execution_cancellation = force.child_token();

                    let connection_span = tracing::info_span!(
                        "websocket.transport",
                        otel.kind = "server",
                        tls.enabled = tls_acceptor.is_some(),
                        lily.connection_id = %connection_id,
                        lily.outcome = tracing::field::Empty,
                        lily.close_category = tracing::field::Empty,
                        lily.error_code = tracing::field::Empty,
                        otel.status_code = tracing::field::Empty,
                    );
                    let task_span = connection_span.clone();
                    let accepted_connection = AcceptedConnection {
                        addr,
                        connection_id,
                        permit,
                        span: connection_span.clone(),
                    };
                    connection_tasks.spawn(
                        async move {
                            match Self::handle_connection(
                                stream,
                                runtime,
                                accepted_connection,
                                tls_acceptor,
                                handshake_cancellation,
                                connection_message_admission,
                                connection_execution_cancellation,
                            )
                            .await
                            {
                                Ok(()) => {
                                    connection_span.record("lily.outcome", "closed");
                                }
                                Err(_) => {
                                    connection_span.record("lily.outcome", "error");
                                    connection_span.record("lily.error_code", "CONNECTION_ERROR");
                                    connection_span.record("otel.status_code", "ERROR");
                                }
                            }
                        }
                        .instrument(task_span),
                    );
                }
            }
        };

        // Leaving the accept loop is the admission barrier. Explicitly release
        // the listener before asking established connections to close.
        message_admission.cancel();
        self.dispatcher.begin_drain();
        drop(listener);
        let mut cleanup_errors = Vec::new();
        if let Err(error) = Self::stop_backplane_ingress(&mut backplane_ingress_task).await {
            cleanup_errors.push(error);
        }
        if let Err(error) = self.lifecycle.health.update(
            WS_LISTENER_HEALTH_CHECK,
            HealthStatus::Degraded,
            "draining_connections",
        ) {
            cleanup_errors.push(format!("listener drain health publication failed: {error}"));
        }
        if let Err(error) = self.lifecycle.health.update(
            WS_DISPATCHER_HEALTH_CHECK,
            HealthStatus::Degraded,
            "draining_messages",
        ) {
            cleanup_errors.push(format!(
                "dispatcher drain health publication failed: {error}"
            ));
        }
        // Coordinated shutdown receives its graceful cutoff exclusively from
        // the composition-root force token. A local deadline exists only when
        // this server entered cleanup because its own listener/maintenance
        // loop failed before the outer coordinator could start.
        let autonomous_failure_deadline = serve_result.is_err().then(|| {
            let now = Instant::now();
            let hard = now + self.shutdown_timeout;
            let graceful = hard - (self.shutdown_timeout / 4).min(Duration::from_secs(2));
            self.scope_cleanup_registry.budget.configure(graceful, hard);
            graceful
        });
        // Establish the cleanup receipt barrier before aborting any
        // connection owner. A cleanup that finishes during the following
        // drains remains replayable to the server-wide reconciliation pass.
        self.connection_cleanup_registry
            .begin_shutdown_reconciliation();
        // Stop per-connection message admission independently from transport
        // cancellation. A connection already dispatching an action keeps that
        // action alive; once it reaches the reader boundary it requests the
        // canonical shutdown Close instead of starting another action.
        message_admission.cancel();

        cleanup_tasks.abort_all();
        while let Ok(Some(result)) = self
            .scope_cleanup_registry
            .budget
            .reconcile(cleanup_tasks.join_next())
            .await
        {
            if let Err(error) = result
                && !error.is_cancelled()
            {
                cleanup_errors.push(if error.is_panic() {
                    "WebSocket maintenance task panicked".to_string()
                } else {
                    "WebSocket maintenance task join failed".to_string()
                });
            }
        }

        let connection_drain = Self::drain_connection_tasks_with_force(
            &mut connection_tasks,
            autonomous_failure_deadline,
            &force,
            &self.scope_cleanup_registry.budget,
        )
        .await;
        for _ in 0..connection_drain.completed {
            self.connection_manager.metrics().close("graceful");
        }
        for _ in 0..connection_drain.cancelled {
            self.connection_manager
                .metrics()
                .close(if connection_drain.forced {
                    "forced"
                } else {
                    "cancelled"
                });
        }
        connection_task_errors.extend(connection_drain.join_errors);
        if connection_drain.timed_out {
            connection_task_errors.push(format!(
                "connection drain timed out with {} task(s) pending; {} task(s) were cancelled and joined",
                connection_drain.pending_at_deadline, connection_drain.cancelled
            ));
        }

        // Message dispatch is tracked independently from its connection task,
        // so dropping that waiter does not cancel ordinary reverse unwind.
        // Root force stops only execution slots. Their lifecycle owners retain
        // reverse unwind, scope receipts, and their independently observed join.
        let message_dispatch_drain = self
            .message_dispatch_registry
            .drain_with_force(autonomous_failure_deadline, &force)
            .await;
        if message_dispatch_drain.timed_out {
            connection_task_errors.push(format!(
                "message execution drain reached its deadline with {} lifecycle owner(s) pending; {} slot abort request(s), {} confirmed slot abort(s)",
                message_dispatch_drain.pending,
                message_dispatch_drain.abort_requested,
                message_dispatch_drain.aborted
            ));
        }
        if message_dispatch_drain.panicked > 0 {
            connection_task_errors.push(format!(
                "{} WebSocket message dispatch task(s) panicked",
                message_dispatch_drain.panicked
            ));
        }
        if message_dispatch_drain.cleanup_failures > 0 {
            connection_task_errors.push(format!(
                "{} WebSocket message cleanup failure(s) during shutdown",
                message_dispatch_drain.cleanup_failures
            ));
        }
        if message_dispatch_drain.timed_out
            || message_dispatch_drain.panicked > 0
            || message_dispatch_drain.cleanup_failures > 0
        {
            if let Err(error) = self
                .lifecycle
                .health
                .record_lifecycle_failure(WS_DISPATCHER_HEALTH_CHECK, "drain_failed")
            {
                cleanup_errors.push(format!(
                    "dispatcher failure health publication failed: {error}"
                ));
            }
        } else if let Err(error) = self.lifecycle.health.update(
            WS_DISPATCHER_HEALTH_CHECK,
            HealthStatus::Degraded,
            "resources_closed",
        ) {
            cleanup_errors.push(format!(
                "dispatcher terminal health publication failed: {error}"
            ));
        }

        // Reconcile middleware-owned manager entries before the final manager
        // sweep. Cleanup hooks observe a Closing connection and the registry
        // removes it only after reverse unwind completes.
        let cleanup_category =
            if force.is_cancelled() || connection_drain.forced || connection_drain.timed_out {
                crate::middleware::WsConnectionCloseCategory::Cancelled
            } else {
                crate::middleware::WsConnectionCloseCategory::ServerShutdown
            };
        let middleware_cleanup = self
            .connection_cleanup_registry
            .finalize_all_with_force(cleanup_category, autonomous_failure_deadline, &force)
            .await;
        let scope_drain = self.scope_cleanup_registry.drain().await;
        if scope_drain.outstanding > 0
            || scope_drain.deadline_failures > 0
            || message_dispatch_drain.outstanding > 0
        {
            connection_task_errors.push(format!(
                "WebSocket cleanup has {} outstanding message owner(s), {} outstanding DI receipt(s), {} DI disposal timeout(s)",
                message_dispatch_drain.outstanding, scope_drain.outstanding, scope_drain.deadline_failures,
            ));
        }
        if message_dispatch_drain.owner_join_cancelled > 0 {
            connection_task_errors.push(format!(
                "{} WebSocket message lifecycle owner(s) were cancelled before confirmed cleanup",
                message_dispatch_drain.owner_join_cancelled,
            ));
        }
        if middleware_cleanup.panicked > 0 {
            connection_task_errors.push(format!(
                "{} WebSocket middleware cleanup task(s) panicked while reconciling {} connection(s)",
                middleware_cleanup.panicked, middleware_cleanup.connections
            ));
        }
        if middleware_cleanup.deadline_elapsed {
            connection_task_errors.push(
                "WebSocket cleanup reached its autonomous server-failure deadline".to_string(),
            );
        }
        if middleware_cleanup.task_join_cancelled > 0
            || middleware_cleanup.task_join_panicked > 0
            || middleware_cleanup.registry_entries_remaining > 0
            || middleware_cleanup.prerequisites_incomplete > 0
        {
            connection_task_errors.push(format!(
                "WebSocket cleanup left {} cancelled task join(s), {} panicked task join(s), {} registry entry/entries and {} connection(s) with unconfirmed prerequisites ({} unconfirmed session/transport receipt(s), {} eligible controller hook(s) not started)",
                middleware_cleanup.task_join_cancelled,
                middleware_cleanup.task_join_panicked,
                middleware_cleanup.registry_entries_remaining,
                middleware_cleanup.prerequisites_incomplete,
                middleware_cleanup.session_incomplete,
                middleware_cleanup.terminal_not_started,
            ));
        }
        let effective_hooks_failed = middleware_cleanup.unreconciled_hook_failures();
        let effective_terminal_failed = middleware_cleanup.unreconciled_terminal_failures();
        if effective_hooks_failed > 0
            || effective_terminal_failed > 0
            || middleware_cleanup.manager_cleanup_failed > 0
        {
            connection_task_errors.push(format!(
                "WebSocket cleanup reported {} unreconciled middleware failure(s), {} middleware timeout(s), {} middleware cancellation(s), {} framework-reconciled middleware cancellation(s), {} middleware panic(s), {} unreconciled controller lifecycle failure(s), {} controller timeout(s), {} framework-reconciled controller cancellation(s), {} controller panic(s), and {} manager cleanup failure(s)",
                effective_hooks_failed,
                middleware_cleanup.hooks_timed_out,
                middleware_cleanup.hooks_cancelled,
                middleware_cleanup.hooks_cancellation_observed,
                middleware_cleanup.hooks_panicked,
                effective_terminal_failed,
                middleware_cleanup.terminal_timed_out,
                middleware_cleanup.terminal_cancelled,
                middleware_cleanup.terminal_panicked,
                middleware_cleanup.manager_cleanup_failed,
            ));
        }

        // Every admitted connection is registry-owned. The final sweep is a
        // defensive reconciliation for internal ownership-transfer failures.
        let removed_connections = if connection_tasks.is_empty()
            && cleanup_tasks.is_empty()
            && self.message_dispatch_registry.is_terminal()
            && self.connection_cleanup_registry.tasks_terminal()
            && self.scope_cleanup_registry.is_terminal()
        {
            self.scope_cleanup_registry
                .budget
                .reconcile(self.connection_manager.remove_all_connections())
                .await
                .unwrap_or(0)
        } else {
            0
        };
        if removed_connections > 0 {
            tracing::debug!(
                lily.websocket.connection_count = removed_connections,
                "Reconciled remaining WebSocket connection records after task drain"
            );
        }

        let mut shutdown_errors = cleanup_errors;
        shutdown_errors.extend(connection_task_errors);
        if shutdown_errors.is_empty() {
            serve_result
        } else {
            let shutdown_errors = shutdown_errors.join("; ");
            match serve_result {
                Ok(()) => Err(ServerError::connection_error(format!(
                    "WebSocket task shutdown failed: {shutdown_errors}"
                ))),
                Err(server_error) => Err(ServerError::connection_error(format!(
                    "WebSocket server stopped with an error: {server_error}; task shutdown also failed: {shutdown_errors}"
                ))),
            }
        }
    }

    /// Drain every accepted connection within one operational deadline. If the
    /// deadline expires, all remaining futures are aborted and then joined so
    /// no connection work can race the subsequent DI container close.
    #[cfg(test)]
    async fn drain_connection_tasks(
        tasks: &mut OwnedTaskSet,
        deadline: Instant,
    ) -> ConnectionTaskDrain {
        Self::drain_connection_tasks_with_force(
            tasks,
            Some(deadline),
            &CancellationToken::new(),
            &crate::shutdown::ShutdownBudget::default(),
        )
        .await
    }

    async fn drain_connection_tasks_with_force(
        tasks: &mut OwnedTaskSet,
        autonomous_failure_deadline: Option<Instant>,
        force: &CancellationToken,
        budget: &crate::shutdown::ShutdownBudget,
    ) -> ConnectionTaskDrain {
        let mut drain = ConnectionTaskDrain::default();
        let mut deadline_signal: futures_util::future::BoxFuture<'static, ()> =
            match autonomous_failure_deadline {
                Some(deadline) => tokio::time::sleep_until(deadline).boxed(),
                None => std::future::pending().boxed(),
            };

        while !tasks.is_empty() {
            enum DrainEvent<T> {
                Joined(T),
                Forced,
                FallbackDeadlineElapsed,
            }
            let event = tokio::select! {
                biased;
                _ = force.cancelled() => DrainEvent::Forced,
                () = &mut deadline_signal => DrainEvent::FallbackDeadlineElapsed,
                result = tasks.join_next() => DrainEvent::Joined(result),
            };
            match event {
                DrainEvent::Joined(Some(Ok(()))) => drain.completed += 1,
                DrainEvent::Joined(Some(Err(error))) if error.is_cancelled() => {
                    drain.cancelled += 1;
                }
                DrainEvent::Joined(Some(Err(error))) => {
                    drain.join_errors.push(if error.is_panic() {
                        "WebSocket connection task panicked".to_string()
                    } else {
                        "WebSocket connection task join failed".to_string()
                    });
                }
                DrainEvent::Joined(None) => break,
                DrainEvent::Forced => {
                    drain.forced = true;
                    drain.pending_at_deadline = tasks.len();
                    Self::cancel_then_abort_connection_tasks(tasks, &mut drain, force, budget)
                        .await;
                    break;
                }
                DrainEvent::FallbackDeadlineElapsed => {
                    drain.timed_out = true;
                    drain.pending_at_deadline = tasks.len();
                    Self::cancel_then_abort_connection_tasks(tasks, &mut drain, force, budget)
                        .await;
                    break;
                }
            }
        }

        drain
    }

    async fn cancel_then_abort_connection_tasks(
        tasks: &mut OwnedTaskSet,
        drain: &mut ConnectionTaskDrain,
        force: &CancellationToken,
        budget: &crate::shutdown::ShutdownBudget,
    ) {
        if !budget.is_forced() {
            budget.force_before(
                budget
                    .hard_deadline()
                    .unwrap_or_else(|| Instant::now() + Duration::from_millis(500)),
            );
        }
        // Signal every accepted connection's execution before dropping any
        // connection slot. The same absolute cutoff applies to all connections.
        force.cancel();
        loop {
            tokio::select! {
                biased;
                // Message slots enforce the earlier cooperative cutoff. Keep
                // the connection and writer alive for retained message cleanup
                // and terminal output before final transport abort.
                () = budget.transport_expired() => break,
                joined = tasks.join_next() => match joined {
                    None => return,
                    Some(Ok(())) => drain.completed += 1,
                    Some(Err(error)) if error.is_cancelled() => drain.cancelled += 1,
                    Some(Err(_)) => drain.join_errors.push("WebSocket connection task panicked during cooperative cancellation".into()),
                }
            }
        }
        drain.abort_requested = tasks.len();
        tasks.abort_all();
        while !tasks.is_empty() {
            let Ok(Some(result)) = budget.reconcile(tasks.join_next()).await else {
                break;
            };
            match result {
                Ok(()) => drain.completed += 1,
                Err(error) if error.is_cancelled() => drain.cancelled += 1,
                Err(error) => drain.join_errors.push(if error.is_panic() {
                    "WebSocket connection task panicked".to_string()
                } else {
                    "WebSocket connection task join failed".to_string()
                }),
            }
        }
        drain.unjoined = tasks.len();
        if drain.unjoined > 0 {
            drain.join_errors.push(format!(
                "{} connection abort request(s) have no observed join",
                drain.unjoined
            ));
        }
    }

    fn tls_acceptor(config: &RustlsConfig) -> TlsAcceptor {
        let mut config = (*config.server_config()).clone();
        // RFC 6455 uses an HTTP/1.1 Upgrade. HTTP/2 extended CONNECT is a
        // separate protocol and is deliberately not advertised here.
        config.alpn_protocols = vec![b"http/1.1".to_vec()];
        TlsAcceptor::from(Arc::new(config))
    }

    async fn accept_tls<I>(
        stream: I,
        acceptor: TlsAcceptor,
        handshake_deadline: Instant,
        handshake_timeout: Duration,
        cancellation: &CancellationToken,
        handshake_span: &tracing::Span,
    ) -> Result<Option<TlsStream<I>>, ServerError>
    where
        I: AsyncRead + AsyncWrite + Unpin,
    {
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => Ok(None),
            result = timeout_at(handshake_deadline, acceptor.accept(stream))
                .instrument(handshake_span.clone()) => match result {
                    Ok(Ok(stream)) => Ok(Some(stream)),
                    Ok(Err(error)) => Err(ServerError::TlsHandshakeFailed { kind: error.kind() }),
                    Err(_) => Err(ServerError::HandshakeTimeout(handshake_timeout)),
                }
        }
    }

    /// Handle individual WebSocket connection (similar to handle_request in HTTP API)
    async fn handle_connection<I>(
        stream: I,
        runtime: ConnectionRuntime,
        connection: AcceptedConnection,
        tls_acceptor: Option<TlsAcceptor>,
        handshake_cancellation: CancellationToken,
        message_admission: CancellationToken,
        execution_cancellation: CancellationToken,
    ) -> Result<(), ServerError>
    where
        I: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let handshake_started = Instant::now();
        let handshake_timeout = Duration::from_secs(runtime.config.handshake_timeout_secs);
        let handshake_deadline = handshake_started + handshake_timeout;
        let transport_security = if tls_acceptor.is_some() {
            WsTransportSecurity::Tls
        } else {
            WsTransportSecurity::Plaintext
        };
        let handshake_span = tracing::info_span!(
            "websocket.upgrade.read",
            otel.kind = "server",
            tls.enabled = tls_acceptor.is_some(),
            lily.connection_id = %connection.connection_id,
            lily.outcome = tracing::field::Empty,
            lily.identity_outcome = tracing::field::Empty,
            lily.error_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let handshake = ConnectionHandshake {
            cancellation: handshake_cancellation,
            started: handshake_started,
            deadline: handshake_deadline,
            timeout: handshake_timeout,
            transport_security,
            span: handshake_span,
        };

        if let Some(acceptor) = tls_acceptor {
            let tls_stream = match Self::accept_tls(
                stream,
                acceptor,
                handshake.deadline,
                handshake.timeout,
                &handshake.cancellation,
                &handshake.span,
            )
            .await
            {
                Ok(Some(stream)) => stream,
                Ok(None) => {
                    handshake.span.record("lily.outcome", "cancelled");
                    runtime
                        .connection_manager
                        .metrics()
                        .handshake("cancelled", handshake.started.elapsed());
                    return Ok(());
                }
                Err(error) => {
                    if matches!(&error, ServerError::HandshakeTimeout(_)) {
                        handshake.span.record("lily.outcome", "timeout");
                        handshake
                            .span
                            .record("lily.error_code", "HANDSHAKE_TIMEOUT");
                        runtime
                            .connection_manager
                            .metrics()
                            .handshake("timeout", handshake.started.elapsed());
                        runtime.connection_manager.metrics().timeout("handshake");
                    } else {
                        handshake.span.record("lily.outcome", "tls_rejected");
                        handshake
                            .span
                            .record("lily.error_code", "TLS_HANDSHAKE_REJECTED");
                        runtime
                            .connection_manager
                            .metrics()
                            .handshake("tls_rejected", handshake.started.elapsed());
                    }
                    handshake.span.record("otel.status_code", "ERROR");
                    return Err(error);
                }
            };
            Self::handle_websocket_connection(
                tls_stream,
                runtime,
                connection,
                handshake,
                message_admission,
                execution_cancellation,
            )
            .await
        } else {
            Self::handle_websocket_connection(
                stream,
                runtime,
                connection,
                handshake,
                message_admission,
                execution_cancellation,
            )
            .await
        }
    }

    async fn handle_websocket_connection<S>(
        mut stream: S,
        runtime: ConnectionRuntime,
        connection: AcceptedConnection,
        handshake_runtime: ConnectionHandshake,
        message_admission: CancellationToken,
        execution_cancellation: CancellationToken,
    ) -> Result<(), ServerError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let AcceptedConnection {
            addr,
            connection_id,
            permit: _connection_permit,
            span: transport_span,
        } = connection;
        let ConnectionHandshake {
            cancellation: handshake_cancellation,
            started: handshake_started,
            deadline: handshake_deadline,
            timeout: handshake_timeout,
            transport_security,
            span: handshake_span,
        } = handshake_runtime;
        let ConnectionRuntime {
            connection_manager,
            dispatcher,
            action_table,
            container,
            connection_cleanup_registry,
            message_dispatch_registry,
            scope_cleanup_registry,
            config,
            shutdown_timeout,
        } = runtime;

        // Validate and publish the HTTP Upgrade around an async application
        // boundary. No user identity code runs before protocol/path/origin.
        let read = read_pre_upgrade(
            &mut stream,
            &handshake_cancellation,
            handshake_deadline,
            handshake_timeout,
        )
        .instrument(handshake_span.clone())
        .await;
        let (pre_upgrade, handshake_span, connection_span) = match read {
            Ok(UpgradeRequestOutcome::Parsed(request)) => {
                handshake_span.record("lily.outcome", "parsed");
                drop(handshake_span);
                // No child may sample an identity until the bounded HTTP read
                // has supplied the remote parent. The transport remains local.
                let connection_span = tracing::info_span!(
                    parent: None,
                    "websocket.connection",
                    otel.kind = "server",
                    tls.enabled = matches!(transport_security, WsTransportSecurity::Tls),
                    lily.connection_id = %connection_id,
                    lily.outcome = tracing::field::Empty,
                    lily.close_category = tracing::field::Empty,
                    lily.error_code = tracing::field::Empty,
                    otel.status_code = tracing::field::Empty,
                );
                lily_trace::set_parent(&connection_span, request_trace_context(&request));
                let handshake_span = tracing::info_span!(
                    parent: &connection_span,
                    "websocket.handshake",
                    otel.kind = "internal",
                    tls.enabled = matches!(transport_security, WsTransportSecurity::Tls),
                    lily.connection_id = %connection_id,
                    lily.outcome = tracing::field::Empty,
                    lily.identity_outcome = tracing::field::Empty,
                    lily.error_code = tracing::field::Empty,
                    otel.status_code = tracing::field::Empty,
                );
                let outcome = perform_parsed_upgrade(
                    request,
                    &mut stream,
                    addr,
                    &action_table,
                    &container,
                    &config,
                    transport_security,
                    &handshake_cancellation,
                    handshake_deadline,
                    handshake_timeout,
                    &scope_cleanup_registry,
                    connection_id,
                )
                .instrument(handshake_span.clone())
                .await;
                (outcome, handshake_span, connection_span)
            }
            Ok(UpgradeRequestOutcome::Terminal(outcome)) => {
                (Ok(outcome), handshake_span, transport_span)
            }
            Err(error) => (Err(error), handshake_span, transport_span),
        };
        let handshake = match pre_upgrade {
            Ok(PreUpgradeOutcome::Accepted(handshake)) => {
                handshake_span.record("lily.outcome", "success");
                handshake_span.record(
                    "lily.identity_outcome",
                    if handshake
                        .identity
                        .as_ref()
                        .and_then(crate::WebSocketIdentitySnapshot::principal)
                        .is_some()
                    {
                        "authenticated"
                    } else {
                        "anonymous"
                    },
                );
                connection_manager
                    .metrics()
                    .handshake("success", handshake_started.elapsed());
                handshake
            }
            Ok(PreUpgradeOutcome::Rejected(code)) => {
                handshake_span.record("lily.outcome", "rejected");
                handshake_span.record("lily.identity_outcome", "not_accepted");
                handshake_span.record("lily.error_code", code.as_str());
                handshake_span.record("otel.status_code", "ERROR");
                connection_manager
                    .metrics()
                    .handshake("rejected", handshake_started.elapsed());
                return Ok(());
            }
            Ok(PreUpgradeOutcome::Cancelled) => {
                handshake_span.record("lily.outcome", "cancelled");
                connection_manager
                    .metrics()
                    .handshake("cancelled", handshake_started.elapsed());
                return Ok(());
            }
            Ok(PreUpgradeOutcome::Disconnected) => {
                handshake_span.record("lily.outcome", "disconnected");
                connection_manager
                    .metrics()
                    .handshake("disconnected", handshake_started.elapsed());
                return Ok(());
            }
            Err(error @ ServerError::HandshakeTimeout(_)) => {
                handshake_span.record("lily.outcome", "timeout");
                handshake_span.record("lily.error_code", "HANDSHAKE_TIMEOUT");
                handshake_span.record("otel.status_code", "ERROR");
                connection_manager
                    .metrics()
                    .handshake("timeout", handshake_started.elapsed());
                connection_manager.metrics().timeout("handshake");
                return Err(error);
            }
            Err(error) => {
                handshake_span.record("lily.outcome", "error");
                handshake_span.record("lily.error_code", "HANDSHAKE_ERROR");
                handshake_span.record("otel.status_code", "ERROR");
                connection_manager
                    .metrics()
                    .handshake("error", handshake_started.elapsed());
                return Err(error);
            }
        };
        drop(handshake_span);
        let result = Self::run_upgraded_connection(
            stream,
            ConnectionRuntime {
                connection_manager,
                dispatcher,
                action_table,
                container,
                connection_cleanup_registry,
                message_dispatch_registry,
                scope_cleanup_registry,
                config,
                shutdown_timeout,
            },
            AcceptedConnection {
                addr,
                connection_id,
                permit: _connection_permit,
                span: connection_span.clone(),
            },
            *handshake,
            message_admission,
            execution_cancellation,
        )
        .instrument(connection_span.clone())
        .await;
        match &result {
            Ok(()) => {
                connection_span.record("lily.outcome", "closed");
            }
            Err(_) => {
                connection_span.record("lily.outcome", "error");
                connection_span.record("lily.error_code", "CONNECTION_ERROR");
                connection_span.record("otel.status_code", "ERROR");
            }
        }
        result
    }

    async fn run_upgraded_connection<S>(
        stream: S,
        runtime: ConnectionRuntime,
        connection: AcceptedConnection,
        handshake: handshake::PreparedHandshake,
        message_admission: CancellationToken,
        execution_cancellation: CancellationToken,
    ) -> Result<(), ServerError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let AcceptedConnection {
            addr,
            connection_id,
            permit: _connection_permit,
            span: connection_span,
        } = connection;
        let ConnectionRuntime {
            connection_manager,
            dispatcher,
            action_table,
            container,
            connection_cleanup_registry,
            message_dispatch_registry,
            scope_cleanup_registry,
            config,
            shutdown_timeout,
        } = runtime;
        let namespace = handshake.namespace.clone();
        let websocket_config = config.websocket_transport_config();
        let ws_stream =
            WebSocketStream::from_raw_socket(stream, Role::Server, Some(websocket_config)).await;
        let extensions = container.services();

        // Freeze the data accepted by the HTTP Upgrade boundary once. The
        // same Arc is observed by lifecycle middleware, guards and handlers.
        let initial_principal = handshake
            .identity
            .as_ref()
            .and_then(crate::WebSocketIdentitySnapshot::principal)
            .cloned();
        let handshake_context = Arc::new(WsHandshakeContext::new(
            namespace.clone(),
            handshake.headers.clone(),
            initial_principal,
            handshake.subprotocol.clone(),
            addr,
            handshake.connection_info,
            handshake.transport_security,
        ));

        let mut context = WebSocketContext::new_with_dispatcher(
            connection_id,
            connection_manager.clone(),
            dispatcher,
            namespace.clone(),
        )
        .with_handshake_context(handshake_context.clone())
        .with_identity_snapshot(handshake.identity.clone());
        if let Some(connection_locals) = handshake.connection_locals.as_ref() {
            context = context.with_connection_locals(Arc::clone(connection_locals));
        }
        let context = Arc::new(context.with_shutdown_budget(scope_cleanup_registry.budget.clone()));
        let connection_middleware_chain = action_table
            .connection_middleware(&namespace)
            .ok_or_else(|| {
                ServerError::connection_error(
                    "accepted namespace has no materialized connection middleware plan".into(),
                )
            })?;
        let middleware_timeout = Duration::from_secs(config.connection_middleware_timeout_secs);
        let lifecycle_timeout = Duration::from_secs(config.connection_lifecycle_timeout_secs);
        let controller_lifecycle = action_table
            .lifecycle_for_namespace(&namespace)
            .ok_or_else(|| {
                ServerError::connection_error(
                    "accepted namespace has no materialized WebSocket controller".into(),
                )
            })?;
        let disconnect_ledger: WebSocketDisconnectLedger = Default::default();
        let terminal_hook = controller_lifecycle.disconnected().is_some().then(|| {
            let container = Arc::clone(&container);
            let extensions = Arc::clone(&extensions);
            let context = Arc::clone(&context);
            let ledger = Arc::clone(&disconnect_ledger);
            let scopes = scope_cleanup_registry.clone();
            Arc::new(move |category, cancellation| {
                let container = Arc::clone(&container);
                let extensions = Arc::clone(&extensions);
                let context = Arc::clone(&context);
                let ledger = Arc::clone(&ledger);
                let scopes = scopes.clone();
                Box::pin(Self::run_websocket_disconnect_ledger(
                    connection_id,
                    container,
                    extensions,
                    context,
                    ledger,
                    category,
                    cancellation,
                    scopes,
                ))
                    as futures_util::future::BoxFuture<'static, ConnectionTerminalHookReport>
            }) as ConnectionTerminalHook
        });
        // Every accepted connection uses the same owned cleanup receipt,
        // including an empty middleware chain with no disconnect handler.
        // The previous zero-hook `ConnectionLease` fast path spawned an
        // untracked manager-removal task from Drop, so server force could
        // report completion before that task had joined.
        let owned_messages = message_dispatch_registry.clone();
        let owned_scopes = scope_cleanup_registry.clone();
        let pending_disconnects = Arc::clone(&disconnect_ledger);
        let disconnect_evidence = Arc::clone(&disconnect_ledger);
        let (session_ended, session_receipt) = tokio::sync::oneshot::channel();
        let receipts = ConnectionCleanupReceipts {
            session: async move { session_receipt.await.is_ok() }
                .boxed()
                .shared(),
            children: Arc::new(move || {
                let messages = owned_messages.clone();
                let scopes = owned_scopes.clone();
                Box::pin(async move {
                    messages.wait_connection(connection_id).await;
                    scopes.wait_connection(connection_id).await;
                })
            }),
            terminal_obligations: Arc::new(move || {
                pending_disconnects
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .obligations()
            }),
            terminal_accounting: Some(Arc::new(move || {
                disconnect_evidence
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .accounting()
            })),
        };
        let connection_cleanup = connection_cleanup_registry
            .register_with_receipts(
                connection_id,
                Arc::clone(&connection_middleware_chain),
                Arc::clone(&context),
                terminal_hook,
                middleware_timeout,
                Some(receipts),
            )
            .map_err(|error| {
                ServerError::connection_error(format!(
                    "WebSocket connection cleanup registration failed: {error:?}"
                ))
            })?;
        let connection_ledger = connection_cleanup.ledger();
        let mut manager_transfer_failed = false;
        let session = ConnectionSession::new(
            async {
                // Move the complete transport into the session, including early
                // admission/open/connected error paths and both later split halves.
                let mut ws_stream = ws_stream;

                // Post-upgrade asynchronous admission runs before the connection is
                // visible to application sends, namespaces or active gauges.
                {
                    // Identity is already authoritative after HTTP 101, even though the
                    // connection is not published to the manager yet. Race a potentially
                    // slow application admission hook against the current versioned
                    // deadline so expired credentials cannot enter application code.
                    let admission = connection_middleware_chain.admit(
                        Arc::clone(&context),
                        &connection_ledger,
                        middleware_timeout,
                        &execution_cancellation,
                    );
                    tokio::pin!(admission);
                    let admission = tokio::select! {
                        biased;
                        _revision = context.wait_for_identity_expiry() => {
                            execution_cancellation.cancel();
                            let _ = (&mut admission).await;
                            None
                        },
                        result = &mut admission => Some(result),
                    };

                    match admission {
                        Some(Ok(())) => {}
                        Some(Err(error)) => {
                            let (category, close_reason) =
                                connection_middleware_terminal(error.error().kind());
                            tracing::warn!(
                                lily.middleware = error.descriptor().name(),
                                lily.middleware_stage = error.stage().as_str(),
                                lily.error_code = error.error().diagnostic_code().as_str(),
                                "WebSocket connection middleware rejected admission"
                            );
                            if let Err(close_error) = send_frame_with_deadline(
                                &mut ws_stream,
                                Message::Close(Some(close_reason.frame())),
                                Duration::from_millis(config.write_timeout_millis),
                            )
                            .await
                            {
                                close_error.record_timeout(connection_manager.metrics());
                                tracing::warn!(
                                    %connection_id,
                                    lily.transport.error_kind = close_error.kind(),
                                    "Failed to send policy close frame"
                                );
                            }
                            return (
                                category,
                                Err(ServerError::connection_error(
                                    "WebSocket connection middleware rejected admission".into(),
                                )),
                            );
                        }
                        None => {
                            tracing::info!(
                                %connection_id,
                                lily.close_category = "identity_expired",
                                "WebSocket identity expired during connection admission"
                            );
                            if let Err(close_error) = send_frame_with_deadline(
                                &mut ws_stream,
                                Message::Close(Some(
                                    crate::request::WsCloseReason::IdentityExpired.frame(),
                                )),
                                Duration::from_millis(config.write_timeout_millis),
                            )
                            .await
                            {
                                close_error.record_timeout(connection_manager.metrics());
                                tracing::warn!(
                                    %connection_id,
                                    lily.transport.error_kind = close_error.kind(),
                                    "Failed to send identity-expiry close frame"
                                );
                            }
                            return (
                                crate::middleware::WsConnectionCloseCategory::IdentityExpired,
                                Err(ServerError::connection_error(
                                    "WebSocket identity expired during connection admission".into(),
                                )),
                            );
                        }
                    }
                }

                // Create the bounded writer channel only after every admission policy
                // has succeeded, then publish the manager entry atomically.
                let (data_tx, data_rx) = mpsc::channel(config.outbound_queue_capacity);
                let (control_tx, mut control_rx) = connection_control_channel();
                let connection_created_at = match connection_manager
                    .add_connection_with_control_and_identity(
                        connection_id,
                        data_tx,
                        control_tx,
                        handshake.connection_info,
                        handshake.transport_security,
                        Some(namespace.clone()),
                        context.identity_handle(),
                    )
                    .await
                {
                    Ok(created_at) => created_at,
                    Err(error) => {
                        let identity_expired = matches!(
                            &error,
                            crate::connection::ConnectionError::InvalidOperation(
                                crate::connection::ConnectionOperationError::IdentityExpired
                            )
                        );
                        if identity_expired {
                            let _ = send_frame_with_deadline(
                                &mut ws_stream,
                                Message::Close(Some(
                                    crate::request::WsCloseReason::IdentityExpired.frame(),
                                )),
                                Duration::from_millis(config.write_timeout_millis),
                            )
                            .await;
                        }
                        return (
                            if identity_expired {
                                crate::middleware::WsConnectionCloseCategory::IdentityExpired
                            } else {
                                crate::middleware::WsConnectionCloseCategory::InternalError
                            },
                            Err(ServerError::connection_error(error.to_string())),
                        );
                    }
                };
                if let Err(error) =
                    connection_cleanup.attach_manager(Arc::clone(&connection_manager))
                {
                    // Keep lifecycle context available while bounded cleanup runs.
                    // This branch is an internal ownership-transfer failure, so it
                    // must preserve the same Closing -> hooks -> removal ordering as
                    // the registry-owned finalizer.
                    manager_transfer_failed = true;
                    return (
                        crate::middleware::WsConnectionCloseCategory::InternalError,
                        Err(ServerError::connection_error(format!(
                            "WebSocket manager cleanup ownership transfer failed: {error:?}"
                        ))),
                    );
                }

                // Publication transfers framework termination ownership. User
                // disconnected is armed separately by an observed connected success.

                {
                    let opened = run_published_connection_stage(
                        connection_id,
                        connection_manager.as_ref(),
                        context.as_ref(),
                        &execution_cancellation,
                        connection_middleware_chain.opened(
                            Arc::clone(&context),
                            &connection_ledger,
                            middleware_timeout,
                            &execution_cancellation,
                        ),
                    )
                    .await;
                    let terminal = match opened {
                        PublishedConnectionStageOutcome::Completed(Ok(())) => None,
                        PublishedConnectionStageOutcome::Completed(Err(error)) => {
                            let (category, close_reason) =
                                connection_middleware_terminal(error.error().kind());
                            tracing::warn!(
                                lily.middleware = error.descriptor().name(),
                                lily.middleware_stage = error.stage().as_str(),
                                lily.error_code = error.error().diagnostic_code().as_str(),
                                "WebSocket connection middleware failed after manager admission"
                            );
                            Some((
                                PublishedConnectionStageClose::Request(close_reason.frame()),
                                category,
                                ServerError::handler_error(
                                    "WebSocket connection middleware opened hook failed".into(),
                                ),
                            ))
                        }
                        PublishedConnectionStageOutcome::IdentityExpired => {
                            tracing::info!(
                                %connection_id,
                                lily.close_category = "identity_expired",
                                "WebSocket identity expired during connection opened hooks"
                            );
                            Some((
                                PublishedConnectionStageClose::AlreadySelected,
                                crate::middleware::WsConnectionCloseCategory::IdentityExpired,
                                ServerError::connection_error(
                                    "WebSocket identity expired during connection opened hooks"
                                        .into(),
                                ),
                            ))
                        }
                        PublishedConnectionStageOutcome::CloseAlreadySelected => Some((
                            PublishedConnectionStageClose::AlreadySelected,
                            crate::middleware::WsConnectionCloseCategory::InternalError,
                            ServerError::connection_error(
                                "WebSocket connection closed during connection opened hooks".into(),
                            ),
                        )),
                        PublishedConnectionStageOutcome::IdentityStateFailed(error) => {
                            tracing::warn!(
                                %connection_id,
                                %error,
                                "WebSocket identity state failed during connection opened hooks"
                            );
                            Some((
                        PublishedConnectionStageClose::Request(
                            crate::request::WsCloseReason::InternalFailure.frame(),
                        ),
                        crate::middleware::WsConnectionCloseCategory::InternalError,
                        ServerError::connection_error(
                            "WebSocket identity state failed during connection opened hooks".into(),
                        ),
                    ))
                        }
                    };
                    if let Some((close_selection, fallback_category, terminal_error)) = terminal {
                        let selected_close = materialize_published_stage_close(
                            connection_id,
                            connection_manager.as_ref(),
                            &mut control_rx,
                            close_selection,
                            fallback_category,
                            Duration::from_millis(config.write_timeout_millis),
                        )
                        .await;
                        let mut category = fallback_category;
                        match selected_close {
                            Ok((close_message, selected_category)) => {
                                category = selected_category;
                                if let Err(close_error) = send_frame_with_deadline(
                                    &mut ws_stream,
                                    close_message,
                                    Duration::from_millis(config.write_timeout_millis),
                                )
                                .await
                                {
                                    category = close_error.close_category();
                                    close_error.record_timeout(connection_manager.metrics());
                                    tracing::warn!(
                                        %connection_id,
                                        lily.transport.error_kind = close_error.kind(),
                                        "Failed to send lifecycle failure close frame"
                                    );
                                }
                            }
                            Err(error) => tracing::warn!(
                                %connection_id,
                                %error,
                                "Selected WebSocket close frame was unavailable during connection opened hooks"
                            ),
                        }
                        return (category, Err(terminal_error));
                    }
                }

                let connected = run_published_connection_stage(
                    connection_id,
                    connection_manager.as_ref(),
                    context.as_ref(),
                    &execution_cancellation,
                    Self::run_connect_hooks(
                        connection_id,
                        WebSocketConnectRuntime {
                            container: &container,
                            extensions: &extensions,
                            context: &context,
                            lifecycle: &controller_lifecycle,
                            stage_timeout: lifecycle_timeout,
                            cancellation: &execution_cancellation,
                            scopes: &scope_cleanup_registry,
                            disconnect_ledger: &disconnect_ledger,
                        },
                    ),
                )
                .await;
                let terminal = match connected {
                    PublishedConnectionStageOutcome::Completed(Ok(())) => None,
                    PublishedConnectionStageOutcome::Completed(Err(failure)) => {
                        let category = failure.close_category();
                        let close_reason = if matches!(
                            category,
                            crate::middleware::WsConnectionCloseCategory::ServerShutdown
                        ) {
                            crate::request::WsCloseReason::ServerShutdown
                        } else {
                            crate::request::WsCloseReason::InternalFailure
                        };
                        Some((
                            PublishedConnectionStageClose::Request(close_reason.frame()),
                            category,
                            failure.into_server_error(),
                        ))
                    }
                    PublishedConnectionStageOutcome::IdentityExpired => {
                        tracing::info!(
                            %connection_id,
                            lily.close_category = "identity_expired",
                            "WebSocket identity expired during controller connect hook"
                        );
                        Some((
                            PublishedConnectionStageClose::AlreadySelected,
                            crate::middleware::WsConnectionCloseCategory::IdentityExpired,
                            ServerError::connection_error(
                                "WebSocket identity expired during controller connect hook".into(),
                            ),
                        ))
                    }
                    PublishedConnectionStageOutcome::CloseAlreadySelected => Some((
                        PublishedConnectionStageClose::AlreadySelected,
                        crate::middleware::WsConnectionCloseCategory::InternalError,
                        ServerError::connection_error(
                            "WebSocket connection closed during controller connect hook".into(),
                        ),
                    )),
                    PublishedConnectionStageOutcome::IdentityStateFailed(error) => {
                        tracing::warn!(
                            %connection_id,
                            %error,
                            "WebSocket identity state failed during controller connect hook"
                        );
                        Some((
                            PublishedConnectionStageClose::Request(
                                crate::request::WsCloseReason::InternalFailure.frame(),
                            ),
                            crate::middleware::WsConnectionCloseCategory::InternalError,
                            ServerError::connection_error(
                                "WebSocket identity state failed during controller connect hook"
                                    .into(),
                            ),
                        ))
                    }
                };
                if let Some((close_selection, fallback_category, terminal_error)) = terminal {
                    let selected_close = materialize_published_stage_close(
                        connection_id,
                        connection_manager.as_ref(),
                        &mut control_rx,
                        close_selection,
                        fallback_category,
                        Duration::from_millis(config.write_timeout_millis),
                    )
                    .await;
                    let mut category = fallback_category;
                    match selected_close {
                        Ok((close_message, selected_category)) => {
                            category = selected_category;
                            if let Err(close_error) = send_frame_with_deadline(
                                &mut ws_stream,
                                close_message,
                                Duration::from_millis(config.write_timeout_millis),
                            )
                            .await
                            {
                                category = close_error.close_category();
                                close_error.record_timeout(connection_manager.metrics());
                                tracing::warn!(
                                    connection_id = %connection_id,
                                    lily.transport.error_kind = close_error.kind(),
                                    "Failed to send lifecycle failure close frame"
                                );
                            }
                        }
                        Err(error) => tracing::warn!(
                            %connection_id,
                            %error,
                            "Selected WebSocket close frame was unavailable during controller connect hook"
                        ),
                    }
                    return (category, Err(terminal_error));
                }

                // Split WebSocket stream
                let (mut ws_sender, mut ws_receiver) = ws_stream.split();
                let pending_pong = Arc::new(StdMutex::new(None::<Instant>));
                // Tungstenite automatically queues the close acknowledgement when its
                // read half receives a peer Close frame. The write half must flush that
                // queued frame, but must not send a second Close frame: doing so fails
                // with `Sending after closing is not allowed` and turns an otherwise
                // graceful client shutdown into a transport reset.
                let peer_close = CancellationToken::new();
                let writer_stopped = CancellationToken::new();
                // Reading a Ping makes Tungstenite own exactly one automatic Pong.
                // Wake the split writer so that queued control frame is flushed
                // promptly without synthesizing a second Pong in Lily.
                let automatic_control_flush = Arc::new(Notify::new());

                // Incoming and outgoing work are child futures of the tracked
                // connection task, not independently detached Tokio tasks. Aborting the
                // outer task at the application deadline therefore cancels both halves.
                let connection_close_category = {
                    let pending_pong_for_writer = pending_pong.clone();
                    let peer_close_for_writer = peer_close.clone();
                    let automatic_control_flush_for_writer = Arc::clone(&automatic_control_flush);
                    let ping_interval = Duration::from_secs(config.ping_interval_secs);
                    let pong_timeout = Duration::from_secs(config.pong_timeout_secs);
                    let write_timeout = Duration::from_millis(config.write_timeout_millis);
                    let writer_metrics = Arc::clone(connection_manager.metrics());
                    let connection_manager_for_writer = Arc::clone(&connection_manager);
                    let mut outbound = OutboundMultiplexer {
                        data: data_rx,
                        control: control_rx,
                    };
                    let outgoing = async move {
                        let check_interval = ping_interval.min(pong_timeout);
                        let mut heartbeat = interval(check_interval);
                        heartbeat.tick().await;
                        let mut last_ping = Instant::now();

                        loop {
                            match outbound
                                .next(
                                    &peer_close_for_writer,
                                    automatic_control_flush_for_writer.as_ref(),
                                    &mut heartbeat,
                                )
                                .await
                            {
                                OutboundCommand::PeerClose => {
                                    match flush_with_deadline(&mut ws_sender, write_timeout).await {
                                        Ok(())
                                        | Err(WriterIoFailure::Transport(
                                            tokio_tungstenite::tungstenite::Error::ConnectionClosed
                                            | tokio_tungstenite::tungstenite::Error::AlreadyClosed,
                                        )) => {
                                            break crate::middleware::WsConnectionCloseCategory::NormalPeer;
                                        }
                                        Err(error) => {
                                            tracing::warn!(
                                                connection_id = %connection_id,
                                                lily.transport.error_kind = error.kind(),
                                                "Peer close acknowledgement could not be flushed"
                                            );
                                            error.record_timeout(&writer_metrics);
                                            break error.close_category();
                                        }
                                    }
                                }
                                OutboundCommand::Close { message, category } => {
                                    let is_server_shutdown = matches!(
                                category,
                                crate::middleware::WsConnectionCloseCategory::ServerShutdown
                            );
                                    let close_budget = if is_server_shutdown {
                                        write_timeout.min(shutdown_timeout)
                                    } else {
                                        write_timeout
                                    };
                                    let close_deadline = Instant::now() + close_budget;
                                    // A Ping read may have left Tungstenite's automatic
                                    // Pong queued. Flush it before buffering a local
                                    // Close so no control frame is emitted after Close.
                                    if let Err(error) =
                                        flush_before_deadline(&mut ws_sender, close_deadline).await
                                    {
                                        tracing::warn!(
                                            connection_id = %connection_id,
                                            lily.transport.error_kind = error.kind(),
                                            "WebSocket pending control frame flush before Close failed"
                                        );
                                        error.record_timeout(&writer_metrics);
                                        break error.close_category();
                                    }
                                    if is_server_shutdown {
                                        // A completed in-flight action publishes its
                                        // terminal frame before requesting shutdown.
                                        // The Close control slot has higher priority
                                        // than the data queue, so explicitly drain the
                                        // already-admitted data before sending 1001.
                                        // `request_close` marks the connection Closing
                                        // before filling the control slot, preventing
                                        // any later application-frame admission.
                                        let mut drain_failure = None;
                                        while let Ok(frame) = outbound.data.try_recv() {
                                            if let Err(error) =
                                                send_queued_application_frame_before_deadline(
                                                    &mut ws_sender,
                                                    frame,
                                                    close_deadline,
                                                )
                                                .await
                                            {
                                                drain_failure = Some(error);
                                                break;
                                            }
                                        }
                                        if let Some(error) = drain_failure {
                                            tracing::warn!(
                                                connection_id = %connection_id,
                                                lily.transport.error_kind = error.kind(),
                                                "WebSocket shutdown terminal frame drain failed"
                                            );
                                            error.record_timeout(&writer_metrics);
                                            break error.close_category();
                                        }
                                        if let Err(error) = send_frame_before_deadline(
                                            &mut ws_sender,
                                            message,
                                            close_deadline,
                                        )
                                        .await
                                        {
                                            tracing::warn!(
                                                connection_id = %connection_id,
                                                lily.transport.error_kind = error.kind(),
                                                "WebSocket close frame write failed"
                                            );
                                            error.record_timeout(&writer_metrics);
                                            break error.close_category();
                                        }
                                        if timeout_at(
                                            close_deadline,
                                            peer_close_for_writer.cancelled(),
                                        )
                                        .await
                                        .is_err()
                                        {
                                            writer_metrics.timeout("close_handshake");
                                            tracing::warn!(
                                                connection_id = %connection_id,
                                                "WebSocket peer did not acknowledge the shutdown Close before the deadline"
                                            );
                                            break crate::middleware::WsConnectionCloseCategory::WriteTimeout;
                                        }
                                        break category;
                                    }
                                    if let Err(error) = send_frame_before_deadline(
                                        &mut ws_sender,
                                        message,
                                        close_deadline,
                                    )
                                    .await
                                    {
                                        tracing::warn!(
                                            connection_id = %connection_id,
                                            lily.transport.error_kind = error.kind(),
                                            "WebSocket close frame write failed"
                                        );
                                        error.record_timeout(&writer_metrics);
                                        break error.close_category();
                                    }
                                    break category;
                                }
                                OutboundCommand::Protocol(message) => {
                                    if let Err(error) = send_frame_with_deadline(
                                        &mut ws_sender,
                                        message,
                                        write_timeout,
                                    )
                                    .await
                                    {
                                        tracing::warn!(
                                            connection_id = %connection_id,
                                            lily.transport.error_kind = error.kind(),
                                            "WebSocket protocol frame write failed"
                                        );
                                        error.record_timeout(&writer_metrics);
                                        break error.close_category();
                                    }
                                }
                                OutboundCommand::FlushAutomaticControl => {
                                    if let Err(error) =
                                        flush_with_deadline(&mut ws_sender, write_timeout).await
                                    {
                                        tracing::warn!(
                                            connection_id = %connection_id,
                                            lily.transport.error_kind = error.kind(),
                                            "WebSocket automatic control frame flush failed"
                                        );
                                        error.record_timeout(&writer_metrics);
                                        break error.close_category();
                                    }
                                }
                                OutboundCommand::Data(frame) => {
                                    if let Err(error) = send_queued_application_frame_with_deadline(
                                        &mut ws_sender,
                                        frame,
                                        write_timeout,
                                    )
                                    .await
                                    {
                                        tracing::warn!(
                                            connection_id = %connection_id,
                                            lily.transport.error_kind = error.kind(),
                                            "WebSocket application frame write failed"
                                        );
                                        error.record_timeout(&writer_metrics);
                                        break error.close_category();
                                    }
                                }
                                OutboundCommand::Heartbeat => {
                                    let pong_expired = pending_pong_for_writer
                                        .lock()
                                        .ok()
                                        .and_then(|pending| *pending)
                                        .is_some_and(|sent| sent.elapsed() >= pong_timeout);
                                    if pong_expired {
                                        match queue_heartbeat_timeout_close(
                                            connection_manager_for_writer.as_ref(),
                                            connection_id,
                                        )
                                        .await
                                        {
                                            Ok(CloseRequestOutcome::Requested)
                                            | Ok(CloseRequestOutcome::AlreadyClosing) => {
                                                // The biased control branch consumes whichever
                                                // first-writer request won, preserving its frame and
                                                // lifecycle category as one terminal decision.
                                                continue;
                                            }
                                            Err(error) => {
                                                tracing::warn!(
                                                    connection_id = %connection_id,
                                                    %error,
                                                    "Heartbeat timeout Close could not be queued"
                                                );
                                                break crate::middleware::WsConnectionCloseCategory::InternalError;
                                            }
                                        }
                                    }
                                    let awaiting_pong = pending_pong_for_writer
                                        .lock()
                                        .map(|pending| pending.is_some())
                                        .unwrap_or(true);
                                    if !awaiting_pong && last_ping.elapsed() >= ping_interval {
                                        if let Err(error) = send_frame_with_deadline(
                                            &mut ws_sender,
                                            Message::Ping(Vec::new()),
                                            write_timeout,
                                        )
                                        .await
                                        {
                                            tracing::warn!(
                                                connection_id = %connection_id,
                                                lily.transport.error_kind = error.kind(),
                                                "WebSocket heartbeat write failed"
                                            );
                                            error.record_timeout(&writer_metrics);
                                            break error.close_category();
                                        }
                                        last_ping = Instant::now();
                                        if let Ok(mut pending) = pending_pong_for_writer.lock() {
                                            *pending = Some(last_ping);
                                        }
                                    }
                                }
                                OutboundCommand::ChannelClosed => {
                                    break crate::middleware::WsConnectionCloseCategory::ServerShutdown;
                                }
                            }
                        }
                    };

                    let connection_manager_clone = connection_manager.clone();
                    let context_clone = context.clone();
                    let extensions_clone = extensions.clone();
                    let action_table_clone = action_table.clone();
                    let message_runtime = MessageRuntime {
                        message_timeout: Duration::from_secs(config.message_timeout_secs),
                        cleanup_timeout: Duration::from_secs(config.message_cleanup_timeout_secs),
                        cancellation: execution_cancellation.clone(),
                        metrics: Arc::clone(connection_manager.metrics()),
                    };
                    let container_clone = Arc::clone(&container);
                    let message_dispatch_registry = message_dispatch_registry.clone();
                    let request_headers = handshake.headers.clone();
                    let request_handshake = handshake_context.clone();
                    let pending_pong_for_reader = pending_pong.clone();
                    let peer_close_for_reader = peer_close.clone();
                    let automatic_control_flush_for_reader = Arc::clone(&automatic_control_flush);
                    let incoming_cancellation = execution_cancellation.clone();
                    let incoming_writer_stopped = writer_stopped.clone();
                    let incoming_message_admission = message_admission.clone();

                    let incoming = async move {
                        let mut drain_close = false;
                        let mut exit_category =
                            crate::middleware::WsConnectionCloseCategory::NormalPeer;
                        let mut awaiting_shutdown_peer_close = false;
                        // Retain a count-and-byte bounded set of complete application
                        // frames outside Tungstenite while one action owns the
                        // per-connection executor. This read-ahead keeps Ping, Pong and
                        // Close observable without creating parallel tasks, DI scopes,
                        // or an unbounded remote-controlled allocation. Identity is
                        // intentionally not frozen here: each raw frame is resolved
                        // against the authoritative snapshot only when its sequential
                        // dispatch begins.
                        let mut deferred_application_messages = InboundApplicationQueue::new(
                            config.inbound_queue_capacity,
                            config.inbound_queue_max_bytes,
                        );
                        let mut shutdown_requested_while_dispatching = false;
                        loop {
                            if incoming_writer_stopped.is_cancelled() {
                                exit_category =
                                    crate::middleware::WsConnectionCloseCategory::Cancelled;
                                break;
                            }
                            if !awaiting_shutdown_peer_close
                                && (incoming_message_admission.is_cancelled()
                                    || incoming_cancellation.is_cancelled())
                            {
                                deferred_application_messages.clear();
                                exit_category =
                                    crate::middleware::WsConnectionCloseCategory::ServerShutdown;
                                drain_close = true;
                                match context_clone
                            .request_close(
                                crate::request::WsCloseReason::ServerShutdown,
                                crate::middleware::WsConnectionCloseCategory::ServerShutdown,
                            )
                            .await
                        {
                            Ok(()) => {
                                awaiting_shutdown_peer_close = true;
                                continue;
                            }
                            Err(error) => {
                                tracing::debug!(
                                    connection_id = %connection_id,
                                    %error,
                                    "WebSocket shutdown Close could not be queued"
                                );
                                break;
                            }
                        }
                            }

                            let message_was_read_ahead = !deferred_application_messages.is_empty();
                            let message_result = if let Some(message) =
                                deferred_application_messages.pop_front()
                            {
                                Some(Ok(message))
                            } else if awaiting_shutdown_peer_close {
                                // Execution cancellation stops application
                                // work, not protocol Close acknowledgement.
                                // The connection owner bounds this transport
                                // wait by the shared force transport cutoff.
                                tokio::select! {
                                    biased;
                                    () = incoming_writer_stopped.cancelled() => break,
                                    message = ws_receiver.next() => message,
                                }
                            } else {
                                tokio::select! {
                                    biased;
                                    _ = incoming_cancellation.cancelled() => {
                                        continue;
                                    }
                                    _ = incoming_message_admission.cancelled() => {
                                        deferred_application_messages.clear();
                                        exit_category = crate::middleware::WsConnectionCloseCategory::ServerShutdown;
                                        drain_close = true;
                                        match context_clone
                                            .request_close(
                                                crate::request::WsCloseReason::ServerShutdown,
                                                crate::middleware::WsConnectionCloseCategory::ServerShutdown,
                                            )
                                            .await
                                        {
                                            Ok(()) => {
                                                awaiting_shutdown_peer_close = true;
                                                continue;
                                            }
                                            Err(error) => {
                                                tracing::debug!(
                                                    connection_id = %connection_id,
                                                    %error,
                                                    "WebSocket shutdown Close could not be queued"
                                                );
                                                break;
                                            }
                                        }
                                    }
                                    message = ws_receiver.next() => message,
                                }
                            };
                            let Some(message_result) = message_result else {
                                break;
                            };
                            match message_result {
                                Ok(message) => {
                                    if awaiting_shutdown_peer_close {
                                        if matches!(message, Message::Close(_)) {
                                            peer_close_for_reader.cancel();
                                            exit_category = crate::middleware::WsConnectionCloseCategory::ServerShutdown;
                                            break;
                                        }
                                        // RFC 6455 allows data already in flight to be
                                        // observed after our Close was sent. Lily must
                                        // not turn those frames into new application
                                        // dispatches while awaiting the peer Close.
                                        continue;
                                    }
                                    if context_clone.has_identity_lifecycle() {
                                        match connection_manager_clone
                                            .expired_identity_revision(connection_id)
                                            .await
                                        {
                                            Ok(Some(revision)) => {
                                                match connection_manager_clone
                                                    .claim_identity_expiry(connection_id, revision)
                                                    .await
                                                {
                                                    Ok(IdentityExpiryClaim::Superseded) => {}
                                                    Ok(IdentityExpiryClaim::Closing(_)) => {
                                                        drain_close = true;
                                                        exit_category = crate::middleware::WsConnectionCloseCategory::IdentityExpired;
                                                        break;
                                                    }
                                                    Err(error) => {
                                                        tracing::warn!(
                                                            connection_id = %connection_id,
                                                            %error,
                                                            "WebSocket identity expiry admission failed"
                                                        );
                                                        drain_close = true;
                                                        exit_category = crate::middleware::WsConnectionCloseCategory::InternalError;
                                                        break;
                                                    }
                                                }
                                            }
                                            Ok(None) => {}
                                            Err(error) => {
                                                tracing::warn!(
                                                    connection_id = %connection_id,
                                                    %error,
                                                    "WebSocket identity state could not be read"
                                                );
                                                drain_close = true;
                                                exit_category =
                                            crate::middleware::WsConnectionCloseCategory::InternalError;
                                                break;
                                            }
                                        }
                                    }
                                    // Read-ahead records arrival time while the active
                                    // action is running. Do not record it again when the
                                    // deferred frame reaches sequential dispatch.
                                    if !message_was_read_ahead {
                                        // Every frame proves transport liveness. Only
                                        // Text/Binary frames extend application-idle.
                                        if let Err(error) = connection_manager_clone
                                            .record_liveness(connection_id)
                                            .await
                                        {
                                            tracing::warn!(
                                                connection_id = %connection_id,
                                                %error,
                                                "WebSocket activity update failed; ending the connection task"
                                            );
                                            drain_close = true;
                                            exit_category =
                                        crate::middleware::WsConnectionCloseCategory::InternalError;
                                            break;
                                        }
                                    }

                                    // Handle different message types
                                    match message {
                                        Message::Text(_) | Message::Binary(_) => {
                                            if !message_was_read_ahead
                                                && let Err(error) = connection_manager_clone
                                                    .record_application_activity(connection_id)
                                                    .await
                                            {
                                                tracing::warn!(
                                                    connection_id = %connection_id,
                                                    %error,
                                                    "WebSocket application activity update failed"
                                                );
                                                drain_close = true;
                                                exit_category = crate::middleware::WsConnectionCloseCategory::InternalError;
                                                break;
                                            }
                                            let message_size = message.len();
                                            connection_manager_clone.metrics().inbound_message();
                                            let message_span = tracing::info_span!(
                                                "websocket.message",
                                                otel.kind = "server",
                                                lily.connection_id = %connection_id,
                                                lily.message_id = tracing::field::Empty,
                                                lily.protocol_version = tracing::field::Empty,
                                                lily.namespace = tracing::field::Empty,
                                                lily.action = tracing::field::Empty,
                                                lily.direction = "inbound",
                                                lily.message_size = i64::try_from(message_size).unwrap_or(i64::MAX),
                                                lily.outcome = tracing::field::Empty,
                                                lily.error_code = tracing::field::Empty,
                                                otel.status_code = tracing::field::Empty,
                                            );
                                            // The handshake already selected one exact controller, so its
                                            // frame codec is authoritative before route lookup. No action
                                            // codec probing or payload sniffing is performed.
                                            let frame_codec = action_table_clone
                                        .frame_codec(request_handshake.namespace())
                                        .expect(
                                            "accepted namespace has a materialized frame codec",
                                        );
                                            let payload_codec = action_table_clone
                                        .payload_codec(request_handshake.namespace())
                                        .expect(
                                            "accepted namespace has a materialized payload codec",
                                        );
                                            let decoded = {
                                                let _entered = message_span.enter();
                                                decode_inbound_application_frame(
                                                    frame_codec.as_ref(),
                                                    message,
                                                    config.max_message_size,
                                                )
                                            };
                                            let decoded = match decoded {
                                                Ok(decoded)
                                                    if decoded.kind()
                                                        == WebSocketInboundMessageKind::Event =>
                                                {
                                                    decoded
                                                }
                                                Ok(decoded) => {
                                                    let error_code = crate::request::WsProtocolErrorCode::UnsupportedMessageKind;
                                                    message_span
                                                        .record("lily.outcome", "protocol_error");
                                                    message_span.record(
                                                        "lily.error_code",
                                                        error_code.as_str(),
                                                    );
                                                    message_span
                                                        .record("otel.status_code", "ERROR");
                                                    let terminal = Self::prepare_protocol_rejection(
                                                        error_code,
                                                        decoded.raw_envelope().kind(),
                                                        payload_codec.as_ref(),
                                                        frame_codec.as_ref(),
                                                    );
                                                    let close_category = terminal.close_category();
                                                    if let Err(error) =
                                                        Self::materialize_message_terminal(
                                                            connection_manager_clone.as_ref(),
                                                            request_handshake.namespace(),
                                                            connection_id,
                                                            terminal.terminal,
                                                            context_clone.shutdown_budget(),
                                                            terminal.output,
                                                        )
                                                        .await
                                                    {
                                                        tracing::warn!(connection_id = %connection_id, %error, "Inbound acknowledgement rejection could not be queued");
                                                        if connection_error_is_identity_expired(
                                                            &error,
                                                        ) {
                                                            drain_close = true;
                                                            exit_category = crate::middleware::WsConnectionCloseCategory::IdentityExpired;
                                                            break;
                                                        }
                                                        Self::request_middleware_close(
                                                    &context_clone,
                                                    crate::request::WsCloseReason::InternalFailure,
                                                    crate::middleware::WsConnectionCloseCategory::HandlerError,
                                                )
                                                .await;
                                                        drain_close = true;
                                                        exit_category = crate::middleware::WsConnectionCloseCategory::HandlerError;
                                                        break;
                                                    }
                                                    if let Some(category) = close_category {
                                                        drain_close = true;
                                                        exit_category = category;
                                                        break;
                                                    }
                                                    continue;
                                                }
                                                Err(error) => {
                                                    let error_code =
                                                        codec_protocol_error(error.kind());
                                                    message_span
                                                        .record("lily.outcome", "protocol_error");
                                                    message_span.record(
                                                        "lily.error_code",
                                                        error_code.as_str(),
                                                    );
                                                    message_span
                                                        .record("otel.status_code", "ERROR");
                                                    let close_reason =
                                                        codec_close_reason(error.kind());
                                                    tracing::warn!(
                                                        connection_id = %connection_id,
                                                        error_code = error_code.as_str(),
                                                        "Rejected invalid WebSocket envelope"
                                                    );
                                                    if let Err(error) = connection_manager_clone
                                                .request_close_frame(
                                                    connection_id,
                                                    Some(close_reason.frame()),
                                                    crate::middleware::WsConnectionCloseCategory::ProtocolError,
                                                )
                                                .await
                                            {
                                                tracing::warn!(
                                                    connection_id = %connection_id,
                                                    %error,
                                                    "Protocol close frame could not be queued"
                                                );
                                            }
                                                    drain_close = true;
                                                    exit_category = crate::middleware::WsConnectionCloseCategory::ProtocolError;
                                                    break;
                                                }
                                            };
                                            if decoded.namespace() != request_handshake.namespace()
                                            {
                                                let error_code =
                                            crate::request::WsProtocolErrorCode::NamespaceViolation;
                                                message_span
                                                    .record("lily.outcome", "protocol_error");
                                                message_span
                                                    .record("lily.error_code", error_code.as_str());
                                                message_span.record("otel.status_code", "ERROR");
                                                let terminal = Self::prepare_protocol_rejection(
                                                    error_code,
                                                    decoded.raw_envelope().kind(),
                                                    payload_codec.as_ref(),
                                                    frame_codec.as_ref(),
                                                );
                                                let close_category = terminal.close_category();
                                                if let Err(error) =
                                                    Self::materialize_message_terminal(
                                                        connection_manager_clone.as_ref(),
                                                        request_handshake.namespace(),
                                                        connection_id,
                                                        terminal.terminal,
                                                        context_clone.shutdown_budget(),
                                                        terminal.output,
                                                    )
                                                    .await
                                                {
                                                    tracing::warn!(connection_id = %connection_id, %error, "Namespace rejection could not be queued");
                                                    if connection_error_is_identity_expired(&error)
                                                    {
                                                        drain_close = true;
                                                        exit_category = crate::middleware::WsConnectionCloseCategory::IdentityExpired;
                                                        break;
                                                    }
                                                    Self::request_middleware_close(
                                                &context_clone,
                                                crate::request::WsCloseReason::InternalFailure,
                                                crate::middleware::WsConnectionCloseCategory::HandlerError,
                                            )
                                            .await;
                                                    drain_close = true;
                                                    exit_category = crate::middleware::WsConnectionCloseCategory::HandlerError;
                                                    break;
                                                }
                                                if let Some(category) = close_category {
                                                    drain_close = true;
                                                    exit_category = category;
                                                    break;
                                                }
                                                continue;
                                            }
                                            let (message_principal, message_connection_context) =
                                                if context_clone.has_identity_lifecycle() {
                                                    let identity =
                                                        context_clone.identity_snapshot();
                                                    let principal = identity
                                                .as_ref()
                                                .and_then(crate::connection::WebSocketIdentitySnapshot::principal)
                                                .cloned();
                                                    (
                                                        principal,
                                                        Arc::new(
                                                            context_clone
                                                                .as_ref()
                                                                .clone()
                                                                .with_message_identity_snapshot(
                                                                    identity,
                                                                ),
                                                        ),
                                                    )
                                                } else {
                                                    // Preserve the zero-identity fast path: no
                                                    // versioned snapshot exists to freeze, so the
                                                    // immutable connection context can be shared.
                                                    (None, Arc::clone(&context_clone))
                                                };
                                            let ws_request = Arc::new(
                                                WsRequest::new_from_decoded(
                                                    connection_id,
                                                    connection_created_at,
                                                    &decoded,
                                                    request_headers.clone(),
                                                    request_handshake.connection_info(),
                                                    request_handshake.transport_security(),
                                                )
                                                .with_handshake_context(request_handshake.clone())
                                                .with_principal_snapshot(message_principal),
                                            );
                                            message_span.record(
                                                "lily.protocol_version",
                                                u64::from(
                                                    crate::request::LILY_WEBSOCKET_PROTOCOL_VERSION,
                                                ),
                                            );
                                            message_span.record(
                                                "lily.namespace",
                                                request_handshake.namespace(),
                                            );
                                            message_span.record("lily.action", ws_request.event());
                                            if let Some(message_id) =
                                                ws_request.message_body.message_id.as_deref()
                                            {
                                                message_span.record("lily.message_id", message_id);
                                            }

                                            // Resolve exact route authority before creating a DI scope.
                                            // Unknown/invalid routes therefore cannot allocate scoped
                                            // application services or enter user middleware.
                                            let message_context = ProcessContext::new()
                                                .with_metadata(
                                                    "transport".to_string(),
                                                    "websocket".to_string(),
                                                )
                                                .with_metadata(
                                                    "phase".to_string(),
                                                    "message".to_string(),
                                                )
                                                .with_metadata(
                                                    "connection_id".to_string(),
                                                    connection_id.to_string(),
                                                );
                                            let inbound_frame_kind = decoded.raw_envelope().kind();
                                            let mut read_ahead_terminal = None::<(
                                                crate::middleware::WsConnectionCloseCategory,
                                                bool,
                                            )>;
                                            let route_result = async {
                                        match Self::resolve_message_route(
                                            action_table_clone.as_ref(),
                                            message_connection_context.as_ref(),
                                            ws_request.as_ref(),
                                        ) {
                                            Ok((action, frame_codec)) => {
                                                let dispatch_span = message_span.clone();
                                                let dispatch_container =
                                                    Arc::clone(&container_clone);
                                                let dispatch_extensions =
                                                    Arc::clone(&extensions_clone);
                                                let dispatch_context =
                                                    Arc::clone(&message_connection_context);
                                                let dispatch_runtime = message_runtime.clone();
                                                #[cfg(test)]
                                                let dispatch_scope_registry =
                                                    message_dispatch_registry.clone();
                                                let dispatch_scopes = scope_cleanup_registry.clone();
                                                let mut dispatch = message_dispatch_registry.spawn_owner_with_cancellation(
                                                    connection_id, dispatch_runtime.cancellation.clone(), move |slot| async move {
                                                        #[cfg(test)]
                                                        dispatch_scope_registry
                                                            .record_scope_execution_attempt();
                                                        dispatch_scopes
                                                            .run_scoped(
                                                                connection_id, &dispatch_container, message_context,
                                                                Self::dispatch_message_owner(
                                                                    action.as_ref(),
                                                                    frame_codec,
                                                                    dispatch_extensions,
                                                                    dispatch_context,
                                                                    ws_request,
                                                                    decoded,
                                                                    dispatch_runtime,
                                                                    slot,
                                                                ),
                                                            )
                                                            .await
                                                            .map(OwnedMessageDispatch::into_dispatch)
                                                    }.instrument(dispatch_span),
                                                );

                                                // The action remains the sole sequential
                                                // application dispatch for this connection, but
                                                // it no longer owns socket reads. Continue
                                                // consuming protocol controls while retaining a
                                                // count-and-byte bounded raw application queue for
                                                // later sequential dispatch. The tracked receiver is always awaited
                                                // after cancellation so its DI scope completes its
                                                // reverse unwind before connection cleanup.
                                                let mut execution_cancel_seen = false;
                                                loop {
                                                    if read_ahead_terminal.is_some() || incoming_writer_stopped.is_cancelled() {
                                                        read_ahead_terminal.get_or_insert((
                                                            crate::middleware::WsConnectionCloseCategory::Cancelled,
                                                            false,
                                                        ));
                                                        break match (&mut dispatch).await {
                                                            Ok(result) => result,
                                                            Err(_) => Ok(Self::internal_action_close()),
                                                        };
                                                    }
                                                    tokio::select! {
                                                        biased;
                                                        dispatch_result = &mut dispatch => {
                                                            break match dispatch_result {
                                                                Ok(result) => result,
                                                                Err(_) => Ok(Self::internal_action_close()),
                                                            };
                                                        }
                                                        _ = incoming_cancellation.cancelled(), if !execution_cancel_seen => {
                                                            execution_cancel_seen = true;
                                                            if incoming_writer_stopped.is_cancelled() {
                                                                read_ahead_terminal.get_or_insert((
                                                                    crate::middleware::WsConnectionCloseCategory::Cancelled,
                                                                    false,
                                                                ));
                                                            } else {
                                                                // Retain a result returned inside cooperation.
                                                                // Reader/peer failures already have their own
                                                                // read_ahead_terminal evidence and still win.
                                                                shutdown_requested_while_dispatching = true;
                                                                deferred_application_messages.clear();
                                                            }
                                                        }
                                                        _ = incoming_message_admission.cancelled(),
                                                            if !shutdown_requested_while_dispatching =>
                                                        {
                                                            shutdown_requested_while_dispatching = true;
                                                            deferred_application_messages.clear();
                                                        }
                                                        message_result = ws_receiver.next() => {
                                                            let Some(message_result) = message_result else {
                                                                read_ahead_terminal = Some((
                                                                    crate::middleware::WsConnectionCloseCategory::Reset,
                                                                    false,
                                                                ));
                                                                deferred_application_messages.clear();
                                                                incoming_cancellation.cancel();
                                                                continue;
                                                            };
                                                            let message = match message_result {
                                                                Ok(message) => message,
                                                                Err(error) => {
                                                                    tracing::debug!(
                                                                        lily.transport.error_kind = tungstenite_error_kind(&error),
                                                                        "WebSocket transport reader stopped during action dispatch"
                                                                    );
                                                                    let close_queued = match queue_reader_transport_error_close(
                                                                        connection_manager_clone.as_ref(),
                                                                        connection_id,
                                                                        &error,
                                                                    )
                                                                    .await
                                                                    {
                                                                        Ok(queued) => queued,
                                                                        Err(close_error) => {
                                                                            tracing::warn!(
                                                                                connection_id = %connection_id,
                                                                                %close_error,
                                                                                "RFC error Close could not be queued during action dispatch"
                                                                            );
                                                                            false
                                                                        }
                                                                    };
                                                                    read_ahead_terminal = Some((
                                                                        if close_queued {
                                                                            crate::middleware::WsConnectionCloseCategory::ProtocolError
                                                                        } else {
                                                                            crate::middleware::WsConnectionCloseCategory::Reset
                                                                        },
                                                                        close_queued,
                                                                    ));
                                                                    deferred_application_messages.clear();
                                                                    incoming_cancellation.cancel();
                                                                    continue;
                                                                }
                                                            };

                                                            if let Err(error) = connection_manager_clone
                                                                .record_liveness(connection_id)
                                                                .await
                                                            {
                                                                tracing::warn!(
                                                                    connection_id = %connection_id,
                                                                    %error,
                                                                    "WebSocket read-ahead liveness update failed"
                                                                );
                                                                read_ahead_terminal = Some((
                                                                    crate::middleware::WsConnectionCloseCategory::InternalError,
                                                                    false,
                                                                ));
                                                                deferred_application_messages.clear();
                                                                incoming_cancellation.cancel();
                                                                continue;
                                                            }

                                                            match message {
                                                                application_message @ Message::Text(_)
                                                                | application_message @ Message::Binary(_) => {
                                                                    if shutdown_requested_while_dispatching {
                                                                        // Admission is already down. Consume bounded
                                                                        // wire input only to expose later controls; no
                                                                        // new application dispatch may be retained.
                                                                        continue;
                                                                    }
                                                                    if let Err(error) = connection_manager_clone
                                                                        .record_application_activity(connection_id)
                                                                        .await
                                                                    {
                                                                        tracing::warn!(
                                                                            connection_id = %connection_id,
                                                                            %error,
                                                                            "WebSocket read-ahead application activity update failed"
                                                                        );
                                                                        read_ahead_terminal = Some((
                                                                            crate::middleware::WsConnectionCloseCategory::InternalError,
                                                                            false,
                                                                        ));
                                                                        deferred_application_messages.clear();
                                                                        incoming_cancellation.cancel();
                                                                        continue;
                                                                    }
                                                                    match deferred_application_messages
                                                                        .try_push(application_message)
                                                                    {
                                                                        Ok(()) => continue,
                                                                        Err(saturation) => {
                                                                            tracing::warn!(
                                                                                connection_id = %connection_id,
                                                                                saturation = saturation.as_str(),
                                                                                queued_messages = deferred_application_messages.messages.len(),
                                                                                retained_bytes = deferred_application_messages.retained_bytes,
                                                                                max_messages = deferred_application_messages.max_messages,
                                                                                max_bytes = deferred_application_messages.max_bytes,
                                                                                "WebSocket inbound queue saturated; closing the connection"
                                                                            );
                                                                            let _ = context_clone
                                                                                .request_close(
                                                                                    crate::request::WsCloseReason::PolicyViolation,
                                                                                    crate::middleware::WsConnectionCloseCategory::PolicyRejected,
                                                                                )
                                                                                .await;
                                                                            read_ahead_terminal = Some((
                                                                                crate::middleware::WsConnectionCloseCategory::PolicyRejected,
                                                                                true,
                                                                            ));
                                                                            deferred_application_messages.clear();
                                                                            incoming_cancellation.cancel();
                                                                        }
                                                                    }
                                                                }
                                                                Message::Close(_) => {
                                                                    // Reading the peer Close queues Tungstenite's
                                                                    // acknowledgement. Wake the writer immediately,
                                                                    // cancel the active action, and await its tracked
                                                                    // DI-scope cleanup before final connection cleanup.
                                                                    peer_close_for_reader.cancel();
                                                                    read_ahead_terminal = Some((
                                                                        crate::middleware::WsConnectionCloseCategory::NormalPeer,
                                                                        true,
                                                                    ));
                                                                    deferred_application_messages.clear();
                                                                    incoming_cancellation.cancel();
                                                                }
                                                                Message::Ping(_) => {
                                                                    automatic_control_flush_for_reader
                                                                        .notify_one();
                                                                }
                                                                Message::Pong(_) => {
                                                                    if let Ok(mut pending) =
                                                                        pending_pong_for_reader.lock()
                                                                    {
                                                                        *pending = None;
                                                                    }
                                                                }
                                                                Message::Frame(_) => {}
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                            Err(code) => {
                                                let frame_codec = action_table_clone
                                                    .frame_codec(
                                                        message_connection_context.namespace(),
                                                    )
                                                    .expect("accepted namespace has a frame codec");
                                                let payload_codec = action_table_clone
                                                    .payload_codec(
                                                        message_connection_context.namespace(),
                                                    )
                                                    .expect(
                                                        "accepted namespace has a payload codec",
                                                    );
                                                Ok(Self::prepare_protocol_rejection(
                                                    code,
                                                    inbound_frame_kind,
                                                    payload_codec.as_ref(),
                                                    frame_codec.as_ref(),
                                                ))
                                            }
                                        }
                                    }
                                    .instrument(message_span.clone())
                                    .await;
                                            if let Some((category, should_drain)) =
                                                read_ahead_terminal
                                            {
                                                message_span.record(
                                                    "lily.outcome",
                                                    "cancelled_by_transport",
                                                );
                                                message_span.record(
                                                    "lily.error_code",
                                                    "TRANSPORT_TERMINAL",
                                                );
                                                exit_category = category;
                                                drain_close = should_drain;
                                                break;
                                            }
                                            match route_result {
                                                Ok(dispatch) => {
                                                    let outcome = dispatch.outcome;
                                                    let close_category = dispatch.close_category();
                                                    if let Err(error) =
                                                        Self::materialize_message_terminal(
                                                            connection_manager_clone.as_ref(),
                                                            request_handshake.namespace(),
                                                            connection_id,
                                                            dispatch.terminal,
                                                            context_clone.shutdown_budget(),
                                                            dispatch.output,
                                                        )
                                                        .await
                                                    {
                                                        let identity_expired =
                                                            connection_error_is_identity_expired(
                                                                &error,
                                                            );
                                                        message_span.record(
                                                            "lily.outcome",
                                                            if identity_expired {
                                                                "identity_expired"
                                                            } else {
                                                                "terminal_error"
                                                            },
                                                        );
                                                        message_span.record(
                                                            "lily.error_code",
                                                            if identity_expired {
                                                                "IDENTITY_EXPIRED"
                                                            } else {
                                                                "TERMINAL_MATERIALIZATION_FAILED"
                                                            },
                                                        );
                                                        message_span
                                                            .record("otel.status_code", "ERROR");
                                                        tracing::warn!(
                                                            connection_id = %connection_id,
                                                            %error,
                                                            "WebSocket terminal decision could not be queued"
                                                        );
                                                        if identity_expired {
                                                            drain_close = true;
                                                            exit_category = crate::middleware::WsConnectionCloseCategory::IdentityExpired;
                                                            break;
                                                        }
                                                        Self::request_middleware_close(
                                                    &context_clone,
                                                    crate::request::WsCloseReason::InternalFailure,
                                                    crate::middleware::WsConnectionCloseCategory::HandlerError,
                                                )
                                                .await;
                                                        drain_close = true;
                                                        exit_category = crate::middleware::WsConnectionCloseCategory::HandlerError;
                                                        break;
                                                    }
                                                    message_span.record(
                                                        "lily.outcome",
                                                        Self::message_outcome_label(outcome),
                                                    );
                                                    if !matches!(outcome, WsMessageOutcome::Handled)
                                                    {
                                                        message_span
                                                            .record("otel.status_code", "ERROR");
                                                    }
                                                    match outcome {
                                                        WsMessageOutcome::Close(reason) => {
                                                            drain_close = true;
                                                            exit_category =
                                                        close_category.unwrap_or_else(|| {
                                                            Self::close_category_from_reason(reason)
                                                        });
                                                            break;
                                                        }
                                                        WsMessageOutcome::Failed(_) => {
                                                            drain_close = true;
                                                            exit_category = crate::middleware::WsConnectionCloseCategory::MiddlewareError;
                                                            break;
                                                        }
                                                        WsMessageOutcome::Handled
                                                        | WsMessageOutcome::Rejected(_) => {}
                                                    }
                                                }
                                                Err(_) => {
                                                    message_span.record(
                                                        "lily.outcome",
                                                        "scope_cleanup_error",
                                                    );
                                                    message_span.record(
                                                        "lily.error_code",
                                                        "SCOPE_CLEANUP_ERROR",
                                                    );
                                                    message_span
                                                        .record("otel.status_code", "ERROR");
                                                    Self::request_middleware_close(
                                                &context_clone,
                                                crate::request::WsCloseReason::InternalFailure,
                                                crate::middleware::WsConnectionCloseCategory::HandlerError,
                                            )
                                            .await;
                                                    drain_close = true;
                                                    exit_category = crate::middleware::WsConnectionCloseCategory::HandlerError;
                                                    break;
                                                }
                                            }
                                            if shutdown_requested_while_dispatching {
                                                // The action and its scoped DI cleanup have completed.
                                                // Preserve its terminal frame ordering, discard the
                                                // bounded read-ahead application queue, and only now
                                                // publish the server shutdown Close.
                                                deferred_application_messages.clear();
                                                exit_category = crate::middleware::WsConnectionCloseCategory::ServerShutdown;
                                                drain_close = true;
                                                match context_clone
                                            .request_close(
                                                crate::request::WsCloseReason::ServerShutdown,
                                                crate::middleware::WsConnectionCloseCategory::ServerShutdown,
                                            )
                                            .await
                                        {
                                            Ok(()) => {
                                                awaiting_shutdown_peer_close = true;
                                                continue;
                                            }
                                            Err(error) => {
                                                tracing::debug!(
                                                    connection_id = %connection_id,
                                                    %error,
                                                    "WebSocket shutdown Close could not be queued after action drain"
                                                );
                                                break;
                                            }
                                        }
                                            }
                                        }
                                        Message::Close(_) => {
                                            // Reading a Close frame already queued the
                                            // protocol acknowledgement inside
                                            // Tungstenite. Wake the writer so it can
                                            // flush that frame without sending a second
                                            // Close message.
                                            peer_close_for_reader.cancel();
                                            drain_close = true;
                                            exit_category =
                                        crate::middleware::WsConnectionCloseCategory::NormalPeer;
                                            break;
                                        }
                                        Message::Ping(_) => {
                                            automatic_control_flush_for_reader.notify_one();
                                        }
                                        Message::Pong(_) => {
                                            if let Ok(mut pending) = pending_pong_for_reader.lock()
                                            {
                                                *pending = None;
                                            }
                                        }
                                        Message::Frame(_) => {
                                            // Raw frames not handled
                                        }
                                    }
                                }
                                Err(error) => {
                                    tracing::debug!(
                                        lily.transport.error_kind = tungstenite_error_kind(&error),
                                        "WebSocket transport reader stopped"
                                    );
                                    match queue_reader_transport_error_close(
                                        connection_manager_clone.as_ref(),
                                        connection_id,
                                        &error,
                                    )
                                    .await
                                    {
                                        Ok(true) => {
                                            drain_close = true;
                                            exit_category =
                                        crate::middleware::WsConnectionCloseCategory::ProtocolError;
                                        }
                                        Ok(false) => {
                                            exit_category =
                                                crate::middleware::WsConnectionCloseCategory::Reset;
                                        }
                                        Err(close_error) => {
                                            tracing::warn!(
                                                connection_id = %connection_id,
                                                %close_error,
                                                "RFC error Close could not be queued"
                                            );
                                            exit_category =
                                                crate::middleware::WsConnectionCloseCategory::Reset;
                                        }
                                    }
                                    break;
                                }
                            }
                        }
                        (exit_category, drain_close)
                    };

                    let expiry_context = Arc::clone(&context);
                    let expiry_manager = Arc::clone(&connection_manager);
                    let identity_expiry = async move {
                        loop {
                            let revision = expiry_context.wait_for_identity_expiry().await;
                            match expiry_manager
                                .claim_identity_expiry(connection_id, revision)
                                .await?
                            {
                                IdentityExpiryClaim::Superseded => continue,
                                IdentityExpiryClaim::Closing(_outcome) => {
                                    return Ok::<(), crate::connection::ConnectionError>(());
                                }
                            }
                        }
                    };

                    tokio::pin!(outgoing);
                    tokio::pin!(incoming);
                    tokio::pin!(identity_expiry);
                    context.shutdown_budget().transport(async {
                    tokio::select! {
                        category = &mut outgoing => {
                            // The writer may terminate while a message action, guard,
                            // or middleware hook is still pending in the reader. Do
                            // not drop that route future: cancellation drives it to
                            // its executor-owned reverse unwind, then the reader exits
                            // through its cancellation branch.
                            writer_stopped.cancel();
                            execution_cancellation.cancel();
                            let _ = (&mut incoming).await;
                            category
                        },
                        (category, drain_close) = &mut incoming => {
                            if drain_close {
                                let close_budget = Duration::from_millis(config.write_timeout_millis);
                                match writer_drain_category(&mut outgoing, close_budget).await {
                                    Some(writer_category) => writer_category,
                                    None => {
                                        connection_manager.metrics().timeout("write");
                                        tracing::warn!(
                                            connection_id = %connection_id,
                                            "WebSocket close drain reached its deadline"
                                        );
                                        crate::middleware::WsConnectionCloseCategory::WriteTimeout
                                    }
                                }
                            } else {
                                category
                            }
                        },
                        expiry = &mut identity_expiry => {
                            if let Err(error) = expiry {
                                tracing::warn!(
                                    connection_id = %connection_id,
                                    %error,
                                    "WebSocket identity expiry claim failed"
                                );
                                let _ = context
                                    .request_close(
                                        crate::request::WsCloseReason::InternalFailure,
                                        crate::middleware::WsConnectionCloseCategory::InternalError,
                                    )
                                    .await;
                            }
                            // Expiry is a connection terminal event, not merely an
                            // inbound-loop check. Cancel a pending middleware, guard,
                            // action, and its DI scope, then let the writer flush the
                            // first-writer close frame selected by the manager claim.
                            execution_cancellation.cancel();
                            let close_budget = Duration::from_millis(config.write_timeout_millis);
                            let (_, category) = tokio::join!(
                                &mut incoming,
                                async {
                                    let category = writer_drain_category(&mut outgoing, close_budget).await;
                                    writer_stopped.cancel();
                                    category
                                },
                            );
                            category.unwrap_or(crate::middleware::WsConnectionCloseCategory::WriteTimeout)
                        },
                    }
                    }).await.unwrap_or(crate::middleware::WsConnectionCloseCategory::Cancelled)
                };

                (connection_close_category, Ok(()))
            },
            session_ended,
        );
        let (connection_close_category, session_result) = session.await;
        // The session dropped all startup execution and transport futures
        // before publishing its receipt. Message owners and DI are separately
        // reconciled by the cleanup owner's prerequisite barrier.
        if let Err(error) = connection_manager.mark_closing(connection_id).await {
            tracing::debug!(%connection_id, %error, "WebSocket connection was already absent while closing");
        }
        record_connection_terminal_category(&connection_span, connection_close_category);
        Self::record_connection_cleanup(
            connection_id,
            connection_cleanup.finalize(connection_close_category).await,
        );

        if manager_transfer_failed {
            let _ = connection_manager.remove_connection(connection_id).await;
        }
        session_result
    }

    fn record_connection_cleanup(connection_id: Uuid, cleanup: ConnectionCleanupOutcome) {
        if cleanup.prerequisites_incomplete() {
            tracing::warn!(
                %connection_id,
                lily.error_code = "WS_CONNECTION_TERMINATION_UNCONFIRMED",
                lily.session_incomplete = cleanup.session_incomplete(),
                lily.controller_hooks_not_started = cleanup.terminal_report().not_started(),
                "WebSocket user cleanup skipped because termination prerequisites were unconfirmed"
            );
        }
        if cleanup.manager_cleanup_failed() {
            tracing::error!(
                %connection_id,
                lily.error_code = "WS_CONNECTION_MANAGER_CLEANUP_FAILED",
                "WebSocket connection manager cleanup failed"
            );
        }
        let terminal = cleanup.terminal_report();
        if terminal.failed() > 0 {
            tracing::warn!(
                %connection_id,
                lily.error_code = "WS_CONTROLLER_LIFECYCLE_CLEANUP_FAILED",
                lily.controller_hooks_attempted = terminal.attempted(),
                lily.controller_hooks_failed = terminal.failed(),
                lily.controller_hooks_timed_out = terminal.timed_out(),
                lily.controller_hooks_cancelled = terminal.cancelled(),
                lily.controller_hooks_panicked = terminal.panicked(),
                lily.controller_hooks_not_started = terminal.not_started(),
                "WebSocket controller lifecycle cleanup completed with bounded failures"
            );
        }
        let Some(report) = cleanup.report() else {
            tracing::error!(
                %connection_id,
                lily.error_code = "WS_MIDDLEWARE_CLEANUP_PANICKED",
                "WebSocket middleware cleanup task panicked"
            );
            return;
        };
        if report.failed() > 0 {
            let first_error_code = report
                .first_failure()
                .map(|failure| failure.error().diagnostic_code().as_str())
                .unwrap_or("WS_MIDDLEWARE_CLEANUP_FAILED");
            tracing::warn!(
                %connection_id,
                lily.error_code = first_error_code,
                lily.middleware_hooks_attempted = report.attempted(),
                lily.middleware_hooks_failed = report.failed(),
                lily.middleware_hooks_timed_out = report.timed_out(),
                lily.middleware_hooks_cancelled = report.cancelled(),
                lily.middleware_hooks_panicked = report.panicked(),
                "WebSocket middleware cleanup completed with bounded failures"
            );
        }
    }

    fn arm_disconnect_hook(
        lifecycle: &WebSocketLifecycleHandlers,
        disconnect_ledger: &WebSocketDisconnectLedger,
        fallback_timeout: Duration,
    ) -> Result<(), WebSocketConnectFailure> {
        let Some(disconnected) = lifecycle.disconnected() else {
            return Ok(());
        };
        let stage_timeout = lifecycle
            .disconnected_operation()
            .and_then(|operation| operation.timeout())
            .unwrap_or(fallback_timeout);
        disconnect_ledger
            .lock()
            .map_err(|_| WebSocketConnectFailure::Handler)?
            .arm(disconnected, stage_timeout);
        Ok(())
    }

    async fn run_connect_hooks(
        connection_id: Uuid,
        runtime: WebSocketConnectRuntime<'_>,
    ) -> Result<(), WebSocketConnectFailure> {
        let connect_context = ProcessContext::new()
            .with_metadata("transport".to_string(), "websocket".to_string())
            .with_metadata("phase".to_string(), "connect".to_string())
            .with_metadata("connection_id".to_string(), connection_id.to_string());
        runtime
            .scopes
            .run_scoped(connection_id, runtime.container, connect_context, async {
                if let Some(handler) = runtime.lifecycle.connected() {
                    if runtime.cancellation.is_cancelled() {
                        return Err(WebSocketConnectFailure::Cancelled);
                    }
                    let stage_timeout = runtime
                        .lifecycle
                        .connected_operation()
                        .and_then(|operation| operation.timeout())
                        .unwrap_or(runtime.stage_timeout);
                    let invocation = AssertUnwindSafe(async {
                        let invocation = WebSocketLifecycleInvocation::connected(
                            Arc::clone(runtime.extensions),
                            Arc::clone(runtime.context),
                            runtime.cancellation.clone(),
                            Instant::now() + stage_timeout,
                        );
                        runtime
                            .context
                            .dispatcher()
                            .execution_scope(
                                invocation
                                    .execution_cancellation()
                                    .expect("connected authority")
                                    .clone(),
                                runtime.context.shutdown_budget().clone(),
                                invocation.deadline(),
                                async { handler.call(invocation).await },
                            )
                            .await
                    })
                    .catch_unwind();
                    let execution_signal = crate::ExecutionCancellation::with_budget(
                        runtime.cancellation.clone(),
                        runtime.context.shutdown_budget().clone(),
                    );
                    let result = tokio::select! {
                        biased;
                        _ = execution_signal.termination_requested() => {
                            return Err(WebSocketConnectFailure::Cancelled);
                        },
                        result = timeout(stage_timeout, invocation) => result,
                    };
                    match result {
                        Ok(Ok(Ok(()))) => {
                            // Arm before awaiting connect-scope disposal. A
                            // scope-close timeout or a concurrent close must
                            // not erase an observed successful user callback.
                            Self::arm_disconnect_hook(
                                runtime.lifecycle,
                                runtime.disconnect_ledger,
                                runtime.stage_timeout,
                            )?;
                        }
                        Ok(Ok(Err(_))) => return Err(WebSocketConnectFailure::Handler),
                        Ok(Err(_)) => return Err(WebSocketConnectFailure::Panicked),
                        Err(_) => return Err(WebSocketConnectFailure::TimedOut),
                    }
                }
                Ok(())
            })
            .await
            .map_err(|_| WebSocketConnectFailure::Scope)?
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "private disconnect owner receives its retained scope registry"
    )]
    async fn run_websocket_disconnect_ledger(
        connection_id: Uuid,
        container: Arc<ApplicationContainer>,
        extensions: Arc<Extensions>,
        context: Arc<WebSocketContext>,
        disconnect_ledger: WebSocketDisconnectLedger,
        category: crate::middleware::WsConnectionCloseCategory,
        cancellation: CancellationToken,
        scopes: ScopeCleanupRegistry,
    ) -> ConnectionTerminalHookReport {
        let handlers = disconnect_ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take_handlers();
        let mut attempted = 0_usize;
        let mut completed = 0_usize;
        let mut failed = 0_usize;
        let mut timed_out = 0_usize;
        let mut panicked = 0_usize;

        for (handler, handler_timeout, record) in handlers.into_iter().rev() {
            let observation = reporting::DisconnectObservation::new(record);
            let invocation_cancellation = cancellation.child_token();
            let _invocation_authority = invocation_cancellation.clone().drop_guard();
            attempted = attempted.saturating_add(1);
            let disconnect_context = ProcessContext::new()
                .with_metadata("transport".to_string(), "websocket".to_string())
                .with_metadata("phase".to_string(), "disconnect".to_string())
                .with_metadata("connection_id".to_string(), connection_id.to_string())
                .with_metadata("close_category".to_string(), category.as_str().to_string());
            let deadline = scopes
                .budget
                .cleanup_deadline(Instant::now() + handler_timeout);
            let invocation = AssertUnwindSafe(async {
                let lifecycle_invocation = WebSocketLifecycleInvocation::disconnected(
                    Arc::clone(&extensions),
                    Arc::clone(&context),
                    invocation_cancellation,
                    deadline,
                    disconnect_reason_from_category(category),
                );
                let signal = lifecycle_invocation
                    .cleanup_cancellation()
                    .expect("disconnect authority")
                    .clone();
                let handler_future = context.dispatcher().cleanup_scope(
                    signal,
                    scopes.budget.clone(),
                    deadline,
                    async {
                        observation.start();
                        match AssertUnwindSafe(async { handler.call(lifecycle_invocation).await })
                            .catch_unwind()
                            .await
                        {
                            Ok(result) => {
                                observation.finish(if result.is_ok() {
                                    crate::lifecycle::LifecycleOutcome::Completed
                                } else {
                                    crate::lifecycle::LifecycleOutcome::Failed
                                });
                                result
                            }
                            Err(panic) => {
                                observation.finish(crate::lifecycle::LifecycleOutcome::Panicked);
                                std::panic::resume_unwind(panic)
                            }
                        }
                    },
                );
                scopes
                    .run_scoped(
                        connection_id,
                        &container,
                        disconnect_context,
                        handler_future,
                    )
                    .await
            })
            .catch_unwind();
            match scopes.budget.cleanup(Some(deadline), invocation).await {
                Ok(Ok(Ok(Ok(())))) => completed = completed.saturating_add(1),
                Ok(Ok(Ok(Err(_)))) | Ok(Ok(Err(_))) => {
                    failed = failed.saturating_add(1);
                }
                Ok(Err(_)) => {
                    failed = failed.saturating_add(1);
                    panicked = panicked.saturating_add(1);
                }
                Err(_) => {
                    observation.finish(crate::lifecycle::LifecycleOutcome::Interrupted(
                        crate::lifecycle::LifecycleInterruption::TimedOut,
                    ));
                    failed = failed.saturating_add(1);
                    timed_out = timed_out.saturating_add(1);
                }
            }
        }
        ConnectionTerminalHookReport::new(attempted, completed, failed, timed_out, panicked)
    }

    /// Resolve exact route and frame-codec authority without opening a DI scope.
    fn resolve_message_route(
        action_table: &WebSocketActionTable,
        context: &WebSocketContext,
        request: &WsRequest,
    ) -> Result<
        (
            Arc<MaterializedWebSocketAction>,
            Arc<dyn WebSocketFrameCodec>,
        ),
        crate::request::WsProtocolErrorCode,
    > {
        let event = request.event();
        let Some((namespace, event_name)) = event.split_once(':') else {
            return Err(crate::request::WsProtocolErrorCode::InvalidRoute);
        };
        if request
            .message_body
            .namespace()
            .is_some_and(|request_namespace| request_namespace != context.namespace())
            || !action_table.contains_namespace(namespace)
            || context.namespace() != namespace
        {
            return Err(crate::request::WsProtocolErrorCode::NamespaceViolation);
        }
        let action = action_table
            .find_action(namespace, event_name)
            .ok_or(crate::request::WsProtocolErrorCode::ActionNotFound)?;
        let frame_codec = action_table
            .frame_codec(namespace)
            .expect("materialized action has a controller frame codec");
        Ok((action, frame_codec))
    }

    #[cfg(test)]
    async fn route_decoded_message(
        action_table: Arc<WebSocketActionTable>,
        extensions: Arc<Extensions>,
        context: Arc<WebSocketContext>,
        request: Arc<WsRequest>,
        decoded: DecodedWebSocketMessage,
        runtime: MessageRuntime,
    ) -> ScopedWebSocketDispatch {
        match Self::resolve_message_route(&action_table, &context, &request) {
            Ok((action, frame_codec)) => {
                Self::dispatch_resolved_message(
                    action.as_ref(),
                    frame_codec,
                    extensions,
                    context,
                    request,
                    decoded,
                    runtime,
                )
                .await
            }
            Err(code) => {
                let frame_codec = action_table
                    .frame_codec(context.namespace())
                    .expect("accepted namespace has a frame codec");
                let payload_codec = action_table
                    .payload_codec(context.namespace())
                    .expect("accepted namespace has a payload codec");
                Self::prepare_protocol_rejection(
                    code,
                    decoded.raw_envelope().kind(),
                    payload_codec.as_ref(),
                    frame_codec.as_ref(),
                )
            }
        }
    }

    #[cfg(test)]
    async fn dispatch_resolved_message(
        action: &MaterializedWebSocketAction,
        frame_codec: Arc<dyn WebSocketFrameCodec>,
        extensions: Arc<Extensions>,
        context: Arc<WebSocketContext>,
        request: Arc<WsRequest>,
        decoded: DecodedWebSocketMessage,
        runtime: MessageRuntime,
    ) -> ScopedWebSocketDispatch {
        let slot = ExecutionSlot::with_cancellation(
            runtime.cancellation.clone(),
            context.shutdown_budget().clone(),
        )
        .1;
        Self::dispatch_message_owner(
            action,
            frame_codec,
            extensions,
            context,
            request,
            decoded,
            runtime,
            slot,
        )
        .await
        .into_dispatch()
    }

    /// Execute one pre-resolved app-local controller action inside its scope.
    #[allow(
        clippy::too_many_arguments,
        reason = "private owner receives the resolved invocation and its execution slot"
    )]
    async fn dispatch_message_owner(
        action: &MaterializedWebSocketAction,
        frame_codec: Arc<dyn WebSocketFrameCodec>,
        extensions: Arc<Extensions>,
        context: Arc<WebSocketContext>,
        request: Arc<WsRequest>,
        decoded: DecodedWebSocketMessage,
        runtime: MessageRuntime,
        slot: ExecutionSlot,
    ) -> OwnedMessageDispatch {
        let event = request.event();
        let (namespace, event_name) = event
            .split_once(':')
            .expect("pre-resolved WebSocket route is canonical");

        // Route metadata is already resolved before any message user code runs.
        // All normal stages observe this exact Instant; cleanup owns a separate cap.
        let message_deadline = Instant::now() + action.timeout().unwrap_or(runtime.message_timeout);
        let execution_signal = slot.message_cancellation(message_deadline);
        let payload_codec = action.payload_codec();
        let parts = decoded.into_runtime_parts();
        let response_context = WebSocketActionResponseContext {
            namespace: parts.namespace.clone(),
            event: parts.event.clone(),
            ack_id: parts.ack_id.clone(),
            frame_kind: parts.raw_envelope.kind(),
        };
        let message_locals = WebSocketMessageLocals::default();
        let exchange = WsMessageExchange::new(
            Arc::clone(&extensions),
            Arc::clone(&context),
            Arc::clone(&request),
            message_locals.clone(),
            execution_signal.clone(),
            message_deadline,
        );
        let middleware_chain = action.middleware_chain();
        let mut owner = MessageLifecycleOwner {
            exchange,
            ledger: Default::default(),
            cleanup_authority: CancellationToken::new(),
        };
        let cleanup_observation = slot.cleanup_observation();
        // Retain in-flight diagnostics across slot destruction too. A handler
        // interrupted by the owner must still report its duration and failure.
        let mut active_stage: Option<(&'static str, tracing::Span, Instant)> = None;
        let execution = slot
            .run_until(message_deadline, async {
                let exchange = &mut owner.exchange;
                let (mut may_invoke, mut outcome) = if middleware_chain.is_empty() {
                    (true, WsMessageOutcome::Handled)
                } else {
                    let before_result = middleware_chain
                        .before_entered(exchange, &mut owner.ledger)
                        .await;
                    match before_result {
                        Ok(WsMessageDecision::Continue) => (true, WsMessageOutcome::Handled),
                        Ok(WsMessageDecision::Reject(code)) => {
                            (false, WsMessageOutcome::Rejected(code))
                        }
                        Ok(WsMessageDecision::Close(reason)) => {
                            (false, WsMessageOutcome::Close(reason))
                        }
                        Err(error) => {
                            tracing::warn!(
                                lily.middleware = error.descriptor().name(),
                                lily.middleware_stage = error.stage().as_str(),
                                lily.error_code = error.error().diagnostic_code().as_str(),
                                "WebSocket message middleware failed before action dispatch"
                            );
                            let outcome = match error.error().kind() {
                                crate::middleware::WsMiddlewareFailureKind::Rejected => {
                                    WsMessageOutcome::Rejected(
                                        crate::request::WsProtocolErrorCode::MiddlewareRejected,
                                    )
                                }
                                crate::middleware::WsMiddlewareFailureKind::Cancelled => {
                                    WsMessageOutcome::Close(
                                        crate::request::WsCloseReason::ServerShutdown,
                                    )
                                }
                                crate::middleware::WsMiddlewareFailureKind::Timeout
                                | crate::middleware::WsMiddlewareFailureKind::Internal => {
                                    WsMessageOutcome::Close(
                                        crate::request::WsCloseReason::InternalFailure,
                                    )
                                }
                            };
                            (false, outcome)
                        }
                    }
                };
                let mut terminal = None;

                if may_invoke && !action.guards().is_empty() {
                    use crate::guard::GuardChain;
                    let guard_chain = GuardChain::new(action.guards());
                    let guard_span = tracing::info_span!(
                        "websocket.message.guard",
                        lily.namespace = %namespace,
                        lily.action = %event_name,
                        lily.outcome = tracing::field::Empty,
                        lily.error_code = tracing::field::Empty,
                        otel.status_code = tracing::field::Empty,
                    );
                    let guard_deadline = exchange.deadline();
                    active_stage = Some(("guard", guard_span.clone(), Instant::now()));
                    let guard_invocation = AssertUnwindSafe(context.dispatcher().execution_scope(
                        exchange.cancellation().clone(),
                        context.shutdown_budget().clone(),
                        guard_deadline,
                        async { guard_chain.execute(exchange).await },
                    ))
                    .catch_unwind()
                    .instrument(guard_span.clone());
                    let guard_result = guard_invocation.await;
                    match guard_result {
                        Err(_) => {
                            guard_span.record("lily.outcome", "panic");
                            guard_span.record("lily.error_code", "GUARD_PANICKED");
                            guard_span.record("otel.status_code", "ERROR");
                            outcome = WsMessageOutcome::Close(
                                crate::request::WsCloseReason::InternalFailure,
                            );
                            may_invoke = false;
                        }
                        Ok(Err(rejection)) => {
                            guard_span.record("lily.outcome", "denied");
                            guard_span.record("lily.error_code", rejection.code().as_str());
                            guard_span.record("otel.status_code", "ERROR");
                            let prepared = Self::prepare_guard_rejection(
                                rejection,
                                &response_context,
                                payload_codec.as_ref(),
                                frame_codec.as_ref(),
                            );
                            outcome = prepared.outcome;
                            terminal = prepared.terminal;
                            may_invoke = false;
                        }
                        Ok(Ok(())) => {
                            guard_span.record("lily.outcome", "allowed");
                        }
                    }
                    let _ = active_stage.take();
                }

                if may_invoke {
                    let action_deadline = exchange.deadline();
                    let handler_span = tracing::info_span!(
                        "websocket.message.handler",
                        lily.namespace = %namespace,
                        lily.action = %event_name,
                        lily.outcome = tracing::field::Empty,
                        lily.error_code = tracing::field::Empty,
                        otel.status_code = tracing::field::Empty,
                    );
                    let handler_started = Instant::now();
                    active_stage = Some(("handler", handler_span.clone(), handler_started));
                    let invocation = WebSocketMessageInvocation::new(
                        extensions,
                        Arc::clone(&context),
                        request.principal().cloned(),
                        parts.namespace,
                        parts.event,
                        parts.headers,
                        parts.payload,
                        Arc::clone(&payload_codec),
                        parts.raw_envelope,
                        execution_signal.clone(),
                        action_deadline,
                        parts.message_id,
                        parts.ack_id,
                        parts.room,
                        parts.timestamp_millis,
                        message_locals,
                    );
                    let handler_invocation =
                        AssertUnwindSafe(context.dispatcher().execution_scope(
                            invocation.cancellation().clone(),
                            context.shutdown_budget().clone(),
                            action_deadline,
                            async { action.invoke(invocation).await },
                        ))
                        .catch_unwind()
                        .instrument(handler_span.clone());
                    let handler_result = handler_invocation.await;
                    match handler_result {
                        Err(_) => {
                            handler_span.record("lily.outcome", "error");
                            handler_span.record("lily.error_code", "HANDLER_PANICKED");
                            handler_span.record("otel.status_code", "ERROR");
                            runtime.metrics.handler("error", handler_started.elapsed());
                            tracing::error!(
                                lily.action = request.event(),
                                "WebSocket action handler panicked"
                            );
                            outcome = WsMessageOutcome::Rejected(
                                crate::request::WsProtocolErrorCode::HandlerFailed,
                            );
                        }
                        Ok(Err(error)) => {
                            handler_span.record("lily.outcome", "error");
                            handler_span.record("lily.error_code", error.code().as_str());
                            handler_span.record("otel.status_code", "ERROR");
                            runtime.metrics.handler("error", handler_started.elapsed());
                            tracing::error!(
                                lily.action = request.event(),
                                lily.error_code = error.code().as_str(),
                                "WebSocket action handler failed"
                            );
                            let prepared = Self::prepare_action_error_caught(
                                error,
                                &response_context,
                                payload_codec.as_ref(),
                                frame_codec.as_ref(),
                            );
                            outcome = prepared.outcome;
                            terminal = prepared.terminal;
                        }
                        Ok(Ok(pending)) => {
                            match Self::prepare_action_outcome_caught(
                                pending,
                                &response_context,
                                payload_codec.as_ref(),
                                frame_codec.as_ref(),
                            ) {
                                Ok(Ok(prepared_terminal)) => {
                                    handler_span.record("lily.outcome", "success");
                                    runtime
                                        .metrics
                                        .handler("success", handler_started.elapsed());
                                    terminal = prepared_terminal;
                                    if matches!(
                                        terminal.as_ref(),
                                        Some(PreparedWebSocketTerminal::Close { .. })
                                    ) {
                                        outcome = WsMessageOutcome::Close(
                                            crate::request::WsCloseReason::Application,
                                        );
                                    }
                                }
                                Ok(Err(error)) => {
                                    handler_span.record("lily.outcome", "error");
                                    handler_span.record(
                                        "lily.error_code",
                                        error.action_error().code().as_str(),
                                    );
                                    handler_span.record("otel.status_code", "ERROR");
                                    runtime.metrics.handler("error", handler_started.elapsed());
                                    let prepared = Self::prepare_action_outcome_failure(
                                        error,
                                        &response_context,
                                        payload_codec.as_ref(),
                                        frame_codec.as_ref(),
                                    );
                                    outcome = prepared.outcome;
                                    terminal = prepared.terminal;
                                }
                                Err(()) => {
                                    handler_span.record("lily.outcome", "error");
                                    handler_span.record("lily.error_code", "CODEC_PANICKED");
                                    handler_span.record("otel.status_code", "ERROR");
                                    runtime.metrics.handler("error", handler_started.elapsed());
                                    let prepared = Self::internal_action_close();
                                    outcome = prepared.outcome;
                                    terminal = prepared.terminal;
                                }
                            }
                        }
                    }
                }

                let _ = active_stage.take();
                if terminal.is_none() {
                    let prepared = Self::prepare_routed_default_terminal(
                        outcome,
                        response_context.frame_kind,
                        payload_codec.as_ref(),
                        frame_codec.as_ref(),
                    );
                    outcome = prepared.outcome;
                    terminal = prepared.terminal;
                }
                let mut dispatch = ScopedWebSocketDispatch::new(outcome, terminal);
                if !middleware_chain.is_empty() {
                    // Normal reverse callbacks are accepted execution too.
                    // Their ledger stays outside the replaceable slot, so its
                    // observed interruption can resume as termination cleanup.
                    let after_report = middleware_chain
                        .after(&mut owner.exchange, &mut owner.ledger, dispatch.outcome)
                        .await;
                    if dispatch.outcome != after_report.outcome() {
                        dispatch = Self::prepare_routed_default_terminal(
                            after_report.outcome(),
                            response_context.frame_kind,
                            payload_codec.as_ref(),
                            frame_codec.as_ref(),
                        );
                    }
                }
                dispatch
            })
            .await;
        cleanup_observation.pipeline_result(match &execution {
            ExecutionExit::Completed(dispatch) => Some(dispatch.outcome),
            _ => None,
        });
        if let Some((stage, span, started)) = active_stage {
            let (outcome, code) = match &execution {
                ExecutionExit::TimedOut => ("timeout", "MESSAGE_TIMEOUT"),
                ExecutionExit::Aborted => ("cancelled", "EXECUTION_CANCELLED"),
                ExecutionExit::Panicked => ("error", "EXECUTION_PANICKED"),
                ExecutionExit::Completed(_) => {
                    unreachable!("completed pipeline clears its active stage")
                }
            };
            span.record("lily.outcome", outcome);
            span.record("lily.error_code", code);
            span.record("otel.status_code", "ERROR");
            if stage == "handler" {
                runtime.metrics.handler(outcome, started.elapsed());
            }
        }
        let facts = execution_signal
            .message_facts()
            .expect("message cancellation facts");
        if facts.deadline_exceeded {
            runtime.metrics.timeout("message");
        }
        reporting::diagnostic(|| {
            tracing::debug!(
                lily.message.deadline_exceeded = facts.deadline_exceeded,
                lily.message.cancellation_cause = ?facts.request.map(|request| request.cause),
                lily.message.cancellation_at = ?facts.request.map(|request| request.at),
                lily.message.pipeline_returned = matches!(&execution, ExecutionExit::Completed(_)),
                "WebSocket message execution terminated"
            )
        });
        let mut dispatch = match execution {
            ExecutionExit::Completed(dispatch) => dispatch,
            ExecutionExit::TimedOut => {
                middleware_chain.execution_stopped(
                    &mut owner.ledger,
                    crate::middleware::WsMessageTerminationReason::TimedOut,
                );
                Self::prepare_action_error_caught(
                    WebSocketActionError::rejected(
                        WebSocketErrorCode::new("MESSAGE_TIMEOUT").expect("static code"),
                        "The message execution timed out.",
                    )
                    .expect("static timeout response"),
                    &response_context,
                    payload_codec.as_ref(),
                    frame_codec.as_ref(),
                )
            }
            ExecutionExit::Aborted => {
                middleware_chain.execution_stopped(
                    &mut owner.ledger,
                    crate::middleware::WsMessageTerminationReason::Aborted,
                );
                Self::prepare_routed_default_terminal(
                    WsMessageOutcome::Close(crate::request::WsCloseReason::ServerShutdown),
                    response_context.frame_kind,
                    payload_codec.as_ref(),
                    frame_codec.as_ref(),
                )
            }
            ExecutionExit::Panicked => {
                middleware_chain.execution_stopped(
                    &mut owner.ledger,
                    crate::middleware::WsMessageTerminationReason::Panicked,
                );
                Self::internal_action_close()
            }
        };

        if !middleware_chain.is_empty() {
            let termination = AssertUnwindSafe(middleware_chain.terminate(
                &mut owner.exchange,
                &mut owner.ledger,
                dispatch.outcome,
                runtime.cleanup_timeout,
                &owner.cleanup_authority,
            ))
            .catch_unwind()
            .await;
            if termination.is_err() {
                cleanup_observation.failed(1);
                tracing::error!("WebSocket message termination executor panicked");
                dispatch = Self::internal_action_close();
            }
            let after_report = owner.ledger.report(dispatch.outcome);
            cleanup_observation.failed(after_report.failed());
            if let Some(error) = after_report.first_failure() {
                tracing::warn!(
                    lily.middleware = error.descriptor().name(),
                    lily.middleware_stage = error.stage().as_str(),
                    lily.error_code = error.error().diagnostic_code().as_str(),
                    "WebSocket message middleware reverse unwind reported a failure"
                );
            }
        }
        let ledger = owner.ledger.accounting();
        if ledger.termination.failed
            + ledger.termination.timed_out
            + ledger.termination.panicked
            + ledger.termination.cancelled
            + ledger.termination.aborted
            + ledger.termination.outstanding
            + ledger.exits_not_started
            > 0
        {
            // A healthy connection may continue only after the required message
            // unwind is confirmed. Cleanup failure is independent of timeout.
            dispatch = Self::internal_action_close();
        }
        cleanup_observation.record(ledger);
        if let Some(output) = &cleanup_observation.output {
            output.prepared(&dispatch.terminal);
            dispatch.output = Some(output.clone());
        }
        OwnedMessageDispatch { dispatch, owner }
    }

    fn prepare_routed_default_terminal(
        outcome: WsMessageOutcome,
        frame_kind: crate::codec::WebSocketFrameKind,
        payload_codec: &dyn WebSocketPayloadCodec,
        frame_codec: &dyn WebSocketFrameCodec,
    ) -> ScopedWebSocketDispatch {
        match outcome {
            WsMessageOutcome::Rejected(code) => {
                Self::prepare_protocol_rejection(code, frame_kind, payload_codec, frame_codec)
            }
            _ => ScopedWebSocketDispatch::new(
                outcome,
                default_terminal_for_non_rejected_outcome(outcome),
            ),
        }
    }

    fn prepare_protocol_rejection(
        code: crate::request::WsProtocolErrorCode,
        frame_kind: crate::codec::WebSocketFrameKind,
        payload_codec: &dyn WebSocketPayloadCodec,
        frame_codec: &dyn WebSocketFrameCodec,
    ) -> ScopedWebSocketDispatch {
        let encoded = catch_unwind(AssertUnwindSafe(|| {
            Self::encode_action_frame(
                WebSocketOutboundMessageKind::Error,
                "lily",
                "error",
                DecodedWebSocketPayload::Json(serde_json::json!({
                    "code": code.as_str(),
                })),
                None,
                frame_kind,
                payload_codec,
                frame_codec,
            )
        }));
        match encoded {
            Ok(Ok(frame)) => ScopedWebSocketDispatch::new(
                WsMessageOutcome::Rejected(code),
                Some(PreparedWebSocketTerminal::ApplicationFrame(frame)),
            ),
            Ok(Err(_)) | Err(_) => Self::internal_action_close(),
        }
    }

    fn prepare_guard_rejection(
        rejection: crate::guard::WebSocketGuardRejection,
        inbound: &WebSocketActionResponseContext,
        payload_codec: &dyn WebSocketPayloadCodec,
        frame_codec: &dyn WebSocketFrameCodec,
    ) -> ScopedWebSocketDispatch {
        use crate::guard::WebSocketGuardRejectionDisposition as Disposition;

        let disposition = rejection.disposition().clone();
        match disposition {
            Disposition::Error => {
                let error = rejection
                    .into_action_result()
                    .expect_err("error guard disposition has a typed action error");
                Self::classify_guard_frame(Self::prepare_action_error_caught(
                    error,
                    inbound,
                    payload_codec,
                    frame_codec,
                ))
            }
            Disposition::Ack => {
                let pending = rejection
                    .into_action_result()
                    .expect("ack guard disposition has a typed acknowledgement");
                match Self::prepare_action_outcome_caught(
                    pending,
                    inbound,
                    payload_codec,
                    frame_codec,
                ) {
                    Ok(Ok(terminal)) => ScopedWebSocketDispatch::new(
                        WsMessageOutcome::Rejected(
                            crate::request::WsProtocolErrorCode::AuthorizationDenied,
                        ),
                        terminal,
                    ),
                    Ok(Err(error)) => {
                        Self::classify_guard_frame(Self::prepare_action_outcome_failure(
                            error,
                            inbound,
                            payload_codec,
                            frame_codec,
                        ))
                    }
                    Err(()) => Self::internal_action_close(),
                }
            }
            Disposition::Close(close) => ScopedWebSocketDispatch::new(
                WsMessageOutcome::Close(crate::request::WsCloseReason::PolicyViolation),
                Some(PreparedWebSocketTerminal::Close {
                    frame: CloseFrame {
                        code: close.code().into(),
                        reason: close.reason().to_owned().into(),
                    },
                    category: crate::middleware::WsConnectionCloseCategory::PolicyRejected,
                }),
            ),
        }
    }

    fn classify_guard_frame(mut dispatch: ScopedWebSocketDispatch) -> ScopedWebSocketDispatch {
        if matches!(
            dispatch.terminal,
            Some(PreparedWebSocketTerminal::ApplicationFrame(_))
        ) {
            dispatch.outcome = WsMessageOutcome::Rejected(
                crate::request::WsProtocolErrorCode::AuthorizationDenied,
            );
        }
        dispatch
    }

    fn prepare_action_outcome_caught(
        pending: PendingWebSocketActionOutcome,
        inbound: &WebSocketActionResponseContext,
        payload_codec: &dyn WebSocketPayloadCodec,
        frame_codec: &dyn WebSocketFrameCodec,
    ) -> Result<Result<Option<PreparedWebSocketTerminal>, ActionOutcomePreparationError>, ()> {
        catch_unwind(AssertUnwindSafe(|| {
            Self::prepare_action_outcome(pending, inbound, payload_codec, frame_codec)
        }))
        .map_err(|_| ())
    }

    fn prepare_action_outcome(
        pending: PendingWebSocketActionOutcome,
        inbound: &WebSocketActionResponseContext,
        payload_codec: &dyn WebSocketPayloadCodec,
        frame_codec: &dyn WebSocketFrameCodec,
    ) -> Result<Option<PreparedWebSocketTerminal>, ActionOutcomePreparationError> {
        match pending {
            PendingWebSocketActionOutcome::NoReply => Ok(None),
            PendingWebSocketActionOutcome::Ack(payload) => {
                let ack_id = inbound
                    .ack_id
                    .as_deref()
                    .ok_or_else(|| {
                        WebSocketActionError::rejected(
                            WebSocketErrorCode::MISSING_ACK_AUTHORITY,
                            "The inbound WebSocket message did not request an acknowledgement.",
                        )
                        .unwrap_or_else(WebSocketActionError::output)
                    })
                    .map_err(ActionOutcomePreparationError::Recoverable)?;
                let frame = Self::encode_action_frame(
                    WebSocketOutboundMessageKind::Ack,
                    &inbound.namespace,
                    &inbound.event,
                    payload,
                    Some(ack_id.to_owned()),
                    inbound.frame_kind,
                    payload_codec,
                    frame_codec,
                )
                .map_err(ActionOutcomePreparationError::InternalOutput)?;
                Ok(Some(PreparedWebSocketTerminal::ApplicationFrame(frame)))
            }
            PendingWebSocketActionOutcome::Emit { event, payload } => {
                let (namespace, event_name) = event
                    .split_once(':')
                    .ok_or_else(|| {
                        WebSocketActionError::output(std::io::Error::other(
                            "typed WebSocket emit route is invalid",
                        ))
                    })
                    .map_err(ActionOutcomePreparationError::Recoverable)?;
                if namespace != inbound.namespace {
                    return Err(ActionOutcomePreparationError::Recoverable(
                        WebSocketActionError::output(std::io::Error::other(
                            "typed WebSocket emit cannot change the connection namespace",
                        )),
                    ));
                }
                let frame = Self::encode_action_frame(
                    WebSocketOutboundMessageKind::Event,
                    namespace,
                    event_name,
                    payload,
                    None,
                    inbound.frame_kind,
                    payload_codec,
                    frame_codec,
                )
                .map_err(ActionOutcomePreparationError::InternalOutput)?;
                Ok(Some(PreparedWebSocketTerminal::ApplicationFrame(frame)))
            }
            PendingWebSocketActionOutcome::Close(close) => {
                Ok(Some(PreparedWebSocketTerminal::Close {
                    frame: CloseFrame {
                        code: close.code().into(),
                        reason: close.reason().to_owned().into(),
                    },
                    category: crate::middleware::WsConnectionCloseCategory::Application,
                }))
            }
        }
    }

    fn prepare_action_outcome_failure(
        error: ActionOutcomePreparationError,
        inbound: &WebSocketActionResponseContext,
        payload_codec: &dyn WebSocketPayloadCodec,
        frame_codec: &dyn WebSocketFrameCodec,
    ) -> ScopedWebSocketDispatch {
        match error {
            ActionOutcomePreparationError::Recoverable(error) => {
                Self::prepare_action_error_caught(error, inbound, payload_codec, frame_codec)
            }
            ActionOutcomePreparationError::InternalOutput(error) => {
                tracing::error!(
                    lily.error_code = error.code().as_str(),
                    %error,
                    "WebSocket action output codec failed"
                );
                Self::internal_action_close()
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn encode_action_frame(
        kind: WebSocketOutboundMessageKind,
        namespace: &str,
        event: &str,
        payload: DecodedWebSocketPayload,
        ack_id: Option<String>,
        frame_kind: crate::codec::WebSocketFrameKind,
        payload_codec: &dyn WebSocketPayloadCodec,
        frame_codec: &dyn WebSocketFrameCodec,
    ) -> Result<Message, WebSocketActionError> {
        let payload = payload_codec
            .encode_payload(payload)
            .map_err(WebSocketActionError::output)?;
        let frame = frame_codec
            .encode_frame(EncodedWebSocketMessage {
                kind,
                namespace: namespace.to_owned(),
                event: event.to_owned(),
                payload,
                message_id: None,
                ack_id,
                headers: WebSocketMessageHeaders::default(),
                frame_kind,
            })
            .map_err(WebSocketActionError::output)?;
        if !matches!(frame, Message::Text(_) | Message::Binary(_)) {
            return Err(WebSocketActionError::output(std::io::Error::other(
                "WebSocket frame codec produced a non-application frame",
            )));
        }
        Ok(frame)
    }

    fn prepare_action_error(
        error: WebSocketActionError,
        inbound: &WebSocketActionResponseContext,
        payload_codec: &dyn WebSocketPayloadCodec,
        frame_codec: &dyn WebSocketFrameCodec,
    ) -> ScopedWebSocketDispatch {
        match error.disposition() {
            WebSocketActionErrorDisposition::KeepOpen => {
                let payload = DecodedWebSocketPayload::Json(serde_json::json!({
                    "code": error.code().as_str(),
                    "message": error.public_message(),
                }));
                match Self::encode_action_frame(
                    WebSocketOutboundMessageKind::Error,
                    &inbound.namespace,
                    &inbound.event,
                    payload,
                    inbound.ack_id.clone(),
                    inbound.frame_kind,
                    payload_codec,
                    frame_codec,
                ) {
                    Ok(frame) => ScopedWebSocketDispatch::new(
                        WsMessageOutcome::Rejected(
                            crate::request::WsProtocolErrorCode::HandlerFailed,
                        ),
                        Some(PreparedWebSocketTerminal::ApplicationFrame(frame)),
                    ),
                    Err(_) => Self::internal_action_close(),
                }
            }
            WebSocketActionErrorDisposition::Close(close) => ScopedWebSocketDispatch::new(
                WsMessageOutcome::Close(crate::request::WsCloseReason::Application),
                Some(PreparedWebSocketTerminal::Close {
                    frame: CloseFrame {
                        code: close.code().into(),
                        reason: close.reason().to_owned().into(),
                    },
                    category: crate::middleware::WsConnectionCloseCategory::Application,
                }),
            ),
        }
    }

    fn prepare_action_error_caught(
        error: WebSocketActionError,
        inbound: &WebSocketActionResponseContext,
        payload_codec: &dyn WebSocketPayloadCodec,
        frame_codec: &dyn WebSocketFrameCodec,
    ) -> ScopedWebSocketDispatch {
        catch_unwind(AssertUnwindSafe(|| {
            Self::prepare_action_error(error, inbound, payload_codec, frame_codec)
        }))
        .unwrap_or_else(|_| Self::internal_action_close())
    }

    fn internal_action_close() -> ScopedWebSocketDispatch {
        let reason = crate::request::WsCloseReason::InternalFailure;
        ScopedWebSocketDispatch::new(
            WsMessageOutcome::Close(reason),
            Some(PreparedWebSocketTerminal::Close {
                frame: reason.frame(),
                category: crate::middleware::WsConnectionCloseCategory::HandlerError,
            }),
        )
    }

    async fn materialize_message_terminal(
        connection_manager: &ConnectionManager,
        namespace: &str,
        connection_id: Uuid,
        terminal: Option<PreparedWebSocketTerminal>,
        budget: &crate::shutdown::ShutdownBudget,
        output: Option<message_reporting::OutputObservation>,
    ) -> Result<(), crate::connection::ConnectionError> {
        // Private terminal publication has independent authority after the
        // execution slot and DI owner have ended. Queue admission has its own
        // local cap; the shared transport cutoff can only shorten it.
        let attempt = output.map(message_reporting::OutputObservation::begin);
        let result = budget
            .transport(async {
                match terminal {
                    None => Ok(()),
                    Some(PreparedWebSocketTerminal::ApplicationFrame(frame)) => {
                        connection_manager
                            .send_application_frame(namespace, connection_id, frame)
                            .await
                    }
                    Some(PreparedWebSocketTerminal::Close { frame, category }) => {
                        connection_manager
                            .request_close_frame(connection_id, Some(frame), category)
                            .await
                            .map(|_| ())
                    }
                }
            })
            .await
            .unwrap_or(Err(crate::connection::ConnectionError::DispatchInterrupted));
        if let Some(attempt) = attempt {
            attempt.finish(&result);
        }
        result
    }

    const fn message_outcome_label(outcome: WsMessageOutcome) -> &'static str {
        match outcome {
            WsMessageOutcome::Handled => "success",
            WsMessageOutcome::Rejected(_) => "rejected",
            WsMessageOutcome::Close(_) => "close",
            WsMessageOutcome::Failed(_) => "middleware_error",
        }
    }

    pub(crate) const fn close_category_from_reason(
        reason: crate::request::WsCloseReason,
    ) -> crate::middleware::WsConnectionCloseCategory {
        match reason {
            crate::request::WsCloseReason::Application => {
                crate::middleware::WsConnectionCloseCategory::Application
            }
            crate::request::WsCloseReason::ServerShutdown => {
                crate::middleware::WsConnectionCloseCategory::ServerShutdown
            }
            crate::request::WsCloseReason::IdleTimeout => {
                crate::middleware::WsConnectionCloseCategory::IdleTimeout
            }
            crate::request::WsCloseReason::InvalidEnvelope
            | crate::request::WsCloseReason::UnsupportedVersion
            | crate::request::WsCloseReason::UnsupportedMessageKind => {
                crate::middleware::WsConnectionCloseCategory::ProtocolError
            }
            crate::request::WsCloseReason::PolicyViolation => {
                crate::middleware::WsConnectionCloseCategory::PolicyRejected
            }
            crate::request::WsCloseReason::IdentityExpired => {
                crate::middleware::WsConnectionCloseCategory::IdentityExpired
            }
            crate::request::WsCloseReason::SlowConsumer => {
                crate::middleware::WsConnectionCloseCategory::SlowConsumer
            }
            crate::request::WsCloseReason::InternalFailure => {
                crate::middleware::WsConnectionCloseCategory::MiddlewareError
            }
        }
    }

    async fn request_middleware_close(
        context: &WebSocketContext,
        reason: crate::request::WsCloseReason,
        category: crate::middleware::WsConnectionCloseCategory,
    ) {
        if context.request_close(reason, category).await.is_err() {
            tracing::warn!(
                close_category = reason.reason(),
                "WebSocket middleware close frame could not be queued"
            );
        }
    }

    /// Start cleanup task for inactive connections
    fn start_cleanup_task(&self, tasks: &mut OwnedTaskSet) {
        let connection_manager = self.connection_manager.clone();
        let cleanup_registry = self.connection_cleanup_registry.clone();
        let timeout = std::time::Duration::from_secs(self.server_config().idle_timeout_secs);

        tasks.spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));

            loop {
                interval.tick().await;
                let claim = connection_manager
                    .cleanup_inactive_connections(timeout)
                    .await;
                let claimed = claim.claimed_ids().len();
                let mut failed_queue_cleanups = FuturesUnordered::new();
                for connection_id in claim.close_not_queued_ids().iter().copied() {
                    let cleanup_registry = cleanup_registry.clone();
                    failed_queue_cleanups.push(async move {
                        let cleanup = cleanup_registry
                            .finalize_connection(
                                connection_id,
                                crate::middleware::WsConnectionCloseCategory::IdleTimeout,
                            )
                            .await;
                        (connection_id, cleanup)
                    });
                }
                while let Some((connection_id, cleanup)) = failed_queue_cleanups.next().await {
                    if let Some(cleanup) = cleanup {
                        Self::record_connection_cleanup(connection_id, cleanup);
                    } else if let Err(error) =
                        connection_manager.remove_connection(connection_id).await
                    {
                        tracing::warn!(
                            %connection_id,
                            %error,
                            "Inactive zero-middleware WebSocket cleanup failed"
                        );
                    }
                }
                if claimed > 0 {
                    tracing::debug!(
                        lily.websocket.connection_count = claimed,
                        "Claimed inactive WebSocket connections for cleanup"
                    );
                }
            }
        });
    }

    /// Get connection manager
    pub fn connection_manager(&self) -> &Arc<ConnectionManager> {
        &self.connection_manager
    }

    /// Returns the app-scoped dispatcher used by controller client proxies.
    ///
    /// Direct [`ConnectionManager::broadcast`] calls are deliberately
    /// node-local. Use this dispatcher when application code outside a
    /// controller action must preserve configured backplane semantics.
    pub fn dispatcher(&self) -> &Arc<WebSocketDispatcher> {
        &self.dispatcher
    }

    /// Get the DI composition root owned by this WebSocket application.
    pub fn container(&self) -> &Arc<ApplicationContainer> {
        &self.container
    }

    /// Get a service-provider handle for controller adapters.
    pub fn extensions(&self) -> Arc<Extensions> {
        self.container.services()
    }
}

#[cfg(test)]
mod message_scope_qualification_tests;

#[cfg(test)]
mod wire_qualification_tests;

#[cfg(test)]
mod tests {
    mod connection_shutdown;
    mod message_cooperation;
    mod message_deadline;
    mod outbound_shutdown;
    mod shutdown_qualification;
    use super::handshake::test_support::{
        CONTROLLED_WRITE_ERROR, EXPECTED_SWITCHING_PROTOCOLS_RESPONSE, ExactKHandshakeStream,
        exact_size_upgrade_request, valid_upgrade_request,
    };
    use super::*;
    use crate::controller::{
        BoundWebSocketOperation, ErasedWebSocketController, PendingWebSocketOperation,
        WebSocketActionFuture, WebSocketActionRegistration, WebSocketAsyncApiRegistration,
        WebSocketConnectionMiddlewareRegistration, WebSocketControllerBindingError,
        WebSocketControllerDefinition, WebSocketControllerInitError,
        WebSocketControllerRegistration, WebSocketGuardRegistration,
        WebSocketHandshakeMiddlewareRegistration, WebSocketLifecycleAction,
        WebSocketLifecycleFuture, WebSocketMessageAction, WebSocketMessageMiddlewareRegistration,
        WebSocketOperationKind, WebSocketOperationMetadata, WebSocketPayloadCodecRegistration,
        downcast_websocket_controller,
    };
    use crate::guard::{GuardInitializationError, WebSocketGuardRejection, WsGuard};
    use crate::request::WsHeaders;
    use async_trait::async_trait;
    use lily_middleware::{MiddlewareDescriptor, MiddlewareErrorCode, MiddlewareKind};
    use lily_web_core::Principal;
    use rcgen::{
        BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa,
        KeyPair, KeyUsagePurpose, generate_simple_self_signed,
    };
    use rustls::{
        ClientConfig, RootCertStore, ServerConfig as RustlsServerConfig,
        pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName},
        server::WebPkiClientVerifier,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::io::{AsyncWriteExt, duplex};
    use tokio_rustls::TlsConnector;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    struct TestSecretResolver;

    struct CloseCategorySubscriber {
        next_span: AtomicUsize,
        values: Arc<StdMutex<Vec<String>>>,
    }

    struct CloseCategoryVisitor<'a> {
        values: &'a StdMutex<Vec<String>>,
    }

    impl tracing::field::Visit for CloseCategoryVisitor<'_> {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "lily.close_category" {
                self.values.lock().unwrap().push(format!("{value:?}"));
            }
        }

        fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
            if field.name() == "lily.close_category" {
                self.values.lock().unwrap().push(value.to_owned());
            }
        }
    }

    impl tracing::Subscriber for CloseCategorySubscriber {
        fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
            true
        }

        fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            let id = self.next_span.fetch_add(1, Ordering::Relaxed) + 1;
            tracing::span::Id::from_u64(u64::try_from(id).unwrap())
        }

        fn record(&self, _span: &tracing::span::Id, values: &tracing::span::Record<'_>) {
            values.record(&mut CloseCategoryVisitor {
                values: self.values.as_ref(),
            });
        }

        fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

        fn event(&self, _event: &tracing::Event<'_>) {}

        fn enter(&self, _span: &tracing::span::Id) {}

        fn exit(&self, _span: &tracing::span::Id) {}
    }

    #[async_trait]
    impl SecretResolver for TestSecretResolver {
        async fn resolve(&self, _key: &str) -> Result<String, lily_config::ConfigError> {
            unreachable!("container/bootstrap conflict must be rejected before config loading")
        }
    }

    #[test]
    fn accepted_connection_span_records_the_exact_terminal_category() {
        let values = Arc::new(StdMutex::new(Vec::new()));
        let subscriber = CloseCategorySubscriber {
            next_span: AtomicUsize::new(0),
            values: Arc::clone(&values),
        };

        tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!(
                "test.websocket.connection_terminal",
                lily.close_category = tracing::field::Empty,
            );
            record_connection_terminal_category(
                &span,
                crate::middleware::WsConnectionCloseCategory::SlowConsumer,
            );
        });

        assert_eq!(values.lock().unwrap().as_slice(), ["slow_consumer"]);
    }

    fn required_subscription_ready_sender() -> &'static tokio::sync::watch::Sender<bool> {
        static READY: std::sync::OnceLock<tokio::sync::watch::Sender<bool>> =
            std::sync::OnceLock::new();
        READY.get_or_init(|| tokio::sync::watch::channel(false).0)
    }

    struct RequiredReadyGateBackplane {
        ready: tokio::sync::Mutex<tokio::sync::watch::Receiver<bool>>,
        emitted: AtomicBool,
    }

    #[async_trait]
    impl WebSocketBackplane for RequiredReadyGateBackplane {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::WebSocketBackplaneInitError> {
            Ok(Self {
                ready: tokio::sync::Mutex::new(required_subscription_ready_sender().subscribe()),
                emitted: AtomicBool::new(false),
            })
        }

        async fn publish(
            &self,
            _frame: crate::WebSocketBackplaneFrame,
        ) -> Result<crate::WebSocketBackplanePublishReceipt, crate::WebSocketBackplaneError>
        {
            Ok(crate::WebSocketBackplanePublishReceipt::accepted())
        }

        async fn receive(
            &self,
            _admission: crate::WebSocketBackplaneInboundAdmission,
        ) -> Result<crate::WebSocketBackplaneEvent, crate::WebSocketBackplaneError> {
            if self.emitted.load(Ordering::Acquire) {
                return std::future::pending().await;
            }
            let mut ready = self.ready.lock().await;
            loop {
                if *ready.borrow_and_update() && !self.emitted.swap(true, Ordering::AcqRel) {
                    return Ok(crate::WebSocketBackplaneEvent::SubscriptionReady);
                }
                if ready.changed().await.is_err() {
                    return Err(crate::WebSocketBackplaneError::new(
                        crate::WebSocketBackplaneErrorKind::Receive,
                    ));
                }
            }
        }
    }

    struct NeverReadyOptionalBackplane;

    #[async_trait]
    impl WebSocketBackplane for NeverReadyOptionalBackplane {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::WebSocketBackplaneInitError> {
            Ok(Self)
        }

        async fn publish(
            &self,
            _frame: crate::WebSocketBackplaneFrame,
        ) -> Result<crate::WebSocketBackplanePublishReceipt, crate::WebSocketBackplaneError>
        {
            Ok(crate::WebSocketBackplanePublishReceipt::accepted())
        }

        async fn receive(
            &self,
            _admission: crate::WebSocketBackplaneInboundAdmission,
        ) -> Result<crate::WebSocketBackplaneEvent, crate::WebSocketBackplaneError> {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn dispatcher_shutdown_handle_rejects_retained_local_dispatcher_clones() {
        let dispatcher = Arc::new(WebSocketDispatcher::local(Arc::new(
            ConnectionManager::new(),
        )));
        let retained = Arc::clone(&dispatcher);
        let mut stop = WsDispatcherLifecycleHandle {
            phase: FrameworkShutdownPhase::StopAdmission,
            dispatcher: Arc::clone(&dispatcher),
            timeout: Duration::from_secs(1),
        };

        stop.shutdown().await.unwrap();

        let error = retained
            .dispatch(crate::connection::BroadcastMessage {
                target: crate::connection::BroadcastTarget::Namespace("orders".to_owned()),
                message: crate::request::WsMessageBody::try_new(
                    "test:shutdown",
                    serde_json::Value::Null,
                )
                .unwrap(),
                wire_format: crate::request::WsWireFormat::Text,
                exclude: Vec::new(),
            })
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            crate::ConnectionError::InvalidOperation(
                crate::connection::ConnectionOperationError::DispatcherNotAccepting
            )
        ));

        let mut drain = WsDispatcherLifecycleHandle {
            phase: FrameworkShutdownPhase::DrainInFlight,
            dispatcher: Arc::clone(&dispatcher),
            timeout: Duration::from_secs(1),
        };
        timeout(Duration::from_secs(1), drain.shutdown())
            .await
            .unwrap()
            .unwrap();
        dispatcher.close_backplane().await.unwrap();
    }

    struct PanickingOutputCodec;

    #[async_trait]
    impl crate::codec::WebSocketCodecFactory for PanickingOutputCodec {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::codec::WebSocketCodecInitError> {
            Ok(Self)
        }
    }

    impl WebSocketPayloadCodec for PanickingOutputCodec {
        fn decode_payload(
            &self,
            payload: crate::codec::EncodedWebSocketPayload,
        ) -> Result<DecodedWebSocketPayload, crate::codec::WebSocketCodecError> {
            LilyEnvelopeCodec.decode_payload(payload)
        }

        fn encode_payload(
            &self,
            _payload: DecodedWebSocketPayload,
        ) -> Result<crate::codec::EncodedWebSocketPayload, crate::codec::WebSocketCodecError>
        {
            panic!("untrusted output codec panic payload")
        }
    }

    struct ErroringPayloadEncoder;

    #[async_trait]
    impl crate::codec::WebSocketCodecFactory for ErroringPayloadEncoder {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::codec::WebSocketCodecInitError> {
            Ok(Self)
        }
    }

    impl WebSocketPayloadCodec for ErroringPayloadEncoder {
        fn decode_payload(
            &self,
            payload: crate::codec::EncodedWebSocketPayload,
        ) -> Result<DecodedWebSocketPayload, crate::codec::WebSocketCodecError> {
            LilyEnvelopeCodec.decode_payload(payload)
        }

        fn encode_payload(
            &self,
            _payload: DecodedWebSocketPayload,
        ) -> Result<crate::codec::EncodedWebSocketPayload, crate::codec::WebSocketCodecError>
        {
            Err(crate::codec::WebSocketCodecError::new(
                WebSocketCodecFailureKind::Encode,
            ))
        }
    }

    struct ErroringFrameEncoder;
    struct FixedApplicationFrameEncoder(Message);
    struct ControlFrameEncoder;

    #[async_trait]
    impl crate::codec::WebSocketCodecFactory for ErroringFrameEncoder {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::codec::WebSocketCodecInitError> {
            Ok(Self)
        }
    }

    impl WebSocketFrameCodec for ErroringFrameEncoder {
        fn decode_frame(
            &self,
            frame: RawEnvelope,
        ) -> Result<DecodedWebSocketMessage, crate::codec::WebSocketCodecError> {
            LilyEnvelopeCodec.decode_frame(frame)
        }

        fn encode_frame(
            &self,
            _message: EncodedWebSocketMessage,
        ) -> Result<Message, crate::codec::WebSocketCodecError> {
            Err(crate::codec::WebSocketCodecError::new(
                WebSocketCodecFailureKind::Encode,
            ))
        }
    }

    #[async_trait]
    impl crate::codec::WebSocketCodecFactory for FixedApplicationFrameEncoder {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::codec::WebSocketCodecInitError> {
            Ok(Self(Message::Text(String::new())))
        }
    }

    impl WebSocketFrameCodec for FixedApplicationFrameEncoder {
        fn decode_frame(
            &self,
            frame: RawEnvelope,
        ) -> Result<DecodedWebSocketMessage, crate::codec::WebSocketCodecError> {
            LilyEnvelopeCodec.decode_frame(frame)
        }

        fn encode_frame(
            &self,
            _message: EncodedWebSocketMessage,
        ) -> Result<Message, crate::codec::WebSocketCodecError> {
            Ok(self.0.clone())
        }
    }

    #[async_trait]
    impl crate::codec::WebSocketCodecFactory for ControlFrameEncoder {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::codec::WebSocketCodecInitError> {
            Ok(Self)
        }
    }

    impl WebSocketFrameCodec for ControlFrameEncoder {
        fn decode_frame(
            &self,
            frame: RawEnvelope,
        ) -> Result<DecodedWebSocketMessage, crate::codec::WebSocketCodecError> {
            LilyEnvelopeCodec.decode_frame(frame)
        }

        fn encode_frame(
            &self,
            _message: EncodedWebSocketMessage,
        ) -> Result<Message, crate::codec::WebSocketCodecError> {
            Ok(Message::Ping(b"invalid-application-output".to_vec()))
        }
    }

    struct PanickingFrameDecoder;
    static CUSTOM_PROTOCOL_ENCODINGS: AtomicUsize = AtomicUsize::new(0);

    struct RecordingFrameCodec;

    #[async_trait]
    impl crate::codec::WebSocketCodecFactory for PanickingFrameDecoder {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::codec::WebSocketCodecInitError> {
            Ok(Self)
        }
    }

    impl WebSocketFrameCodec for PanickingFrameDecoder {
        fn decode_frame(
            &self,
            _frame: RawEnvelope,
        ) -> Result<DecodedWebSocketMessage, crate::codec::WebSocketCodecError> {
            panic!("untrusted frame decoder panic payload")
        }

        fn encode_frame(
            &self,
            message: EncodedWebSocketMessage,
        ) -> Result<Message, crate::codec::WebSocketCodecError> {
            LilyEnvelopeCodec.encode_frame(message)
        }
    }

    #[async_trait]
    impl crate::codec::WebSocketCodecFactory for RecordingFrameCodec {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::codec::WebSocketCodecInitError> {
            Ok(Self)
        }
    }

    impl WebSocketFrameCodec for RecordingFrameCodec {
        fn decode_frame(
            &self,
            frame: RawEnvelope,
        ) -> Result<DecodedWebSocketMessage, crate::codec::WebSocketCodecError> {
            LilyEnvelopeCodec.decode_frame(frame)
        }

        fn encode_frame(
            &self,
            message: EncodedWebSocketMessage,
        ) -> Result<Message, crate::codec::WebSocketCodecError> {
            CUSTOM_PROTOCOL_ENCODINGS.fetch_add(1, Ordering::SeqCst);
            LilyEnvelopeCodec.encode_frame(message)
        }
    }

    #[tokio::test]
    async fn caller_owned_container_cannot_be_combined_with_secret_resolver() {
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let error = WsAppBuilder::new("127.0.0.1:0")
            .container(Arc::clone(&container))
            .secret_resolver(TestSecretResolver)
            .build()
            .await
            .err()
            .expect("ambiguous bootstrap ownership must fail");
        assert!(matches!(error, ServerError::Configuration(_)));
        assert!(error.to_string().contains("caller-owned DI container"));
        container.close().await.unwrap();
    }

    #[test]
    fn inbound_application_queue_enforces_message_and_byte_boundaries() {
        let mut count_bounded = InboundApplicationQueue::new(2, 64);
        count_bounded
            .try_push(Message::Text("a".into()))
            .expect("first message fits");
        count_bounded
            .try_push(Message::Binary(vec![1, 2]))
            .expect("exact message capacity fits");
        assert_eq!(count_bounded.messages.len(), 2);
        assert_eq!(count_bounded.retained_bytes, 3);
        assert_eq!(
            count_bounded.try_push(Message::Text("b".into())),
            Err(InboundQueueSaturation::MessageCapacity)
        );
        assert_eq!(count_bounded.messages.len(), 2);
        assert_eq!(count_bounded.retained_bytes, 3);
        assert_eq!(count_bounded.pop_front(), Some(Message::Text("a".into())));
        assert_eq!(count_bounded.pop_front(), Some(Message::Binary(vec![1, 2])));

        let mut byte_bounded = InboundApplicationQueue::new(4, 3);
        byte_bounded
            .try_push(Message::Text("ab".into()))
            .expect("first payload fits");
        byte_bounded
            .try_push(Message::Binary(vec![3]))
            .expect("exact byte capacity fits");
        assert_eq!(byte_bounded.retained_bytes, 3);
        assert_eq!(
            byte_bounded.try_push(Message::Text("c".into())),
            Err(InboundQueueSaturation::ByteCapacity)
        );
        assert_eq!(byte_bounded.messages.len(), 2);
        assert_eq!(byte_bounded.retained_bytes, 3);
    }

    #[test]
    fn inbound_application_queue_releases_accounting_on_pop_and_clear() {
        let mut queue = InboundApplicationQueue::new(2, 4);
        queue
            .try_push(Message::Text("abc".into()))
            .expect("initial payload fits");
        assert_eq!(queue.pop_front(), Some(Message::Text("abc".into())));
        assert_eq!(queue.retained_bytes, 0);
        assert!(queue.is_empty());

        queue
            .try_push(Message::Binary(vec![1, 2, 3, 4]))
            .expect("released bytes can be reused");
        queue.clear();
        assert_eq!(queue.retained_bytes, 0);
        assert!(queue.is_empty());
    }

    #[test]
    fn central_websocket_config_produces_the_canonical_effective_snapshot() {
        let central = lily_config::WebSocketConfig {
            enabled: true,
            host: "0.0.0.0".into(),
            port: 9091,
            endpoint_path: "/socket".into(),
            max_connections: 77,
            max_message_size_bytes: 512 * 1024,
            max_frame_size_bytes: 128 * 1024,
            max_outbound_message_size_bytes: 256 * 1024,
            ping_interval_secs: 31,
            idle_timeout_secs: 61,
            message_timeout_secs: 7,
            message_cleanup_timeout_secs: 13,
            connection_middleware_timeout_secs: 11,
            connection_lifecycle_timeout_secs: 19,
            inbound_queue_capacity: 12,
            inbound_queue_max_bytes: 384 * 1024,
            outbound_queue_max_bytes: 768 * 1024,
            outbound_admission_timeout_millis: 2_500,
            write_timeout_millis: 7_500,
            write_buffer_size_bytes: 64 * 1024,
            max_write_buffer_size_bytes: 1024 * 1024,
            ..lily_config::WebSocketConfig::default()
        };
        let effective = EffectiveWsServerConfig::resolve(
            &WsAddressSource::Config,
            &WsServerConfigSource::Config,
            None,
            &central,
        )
        .expect("central WebSocket config");

        assert!(effective.enabled());
        assert_eq!(effective.listen_address(), "0.0.0.0:9091");
        assert_eq!(effective.server().endpoint_path, "/socket");
        assert_eq!(effective.server().max_connections, 77);
        assert_eq!(effective.server().max_message_size, 512 * 1024);
        assert_eq!(effective.server().max_frame_size, 128 * 1024);
        assert_eq!(effective.server().max_outbound_message_size, 256 * 1024);
        assert_eq!(effective.server().ping_interval_secs, 31);
        assert_eq!(effective.server().idle_timeout_secs, 61);
        assert_eq!(effective.server().message_timeout_secs, 7);
        assert_eq!(effective.server().message_cleanup_timeout_secs, 13);
        assert_eq!(effective.server().connection_middleware_timeout_secs, 11);
        assert_eq!(effective.server().connection_lifecycle_timeout_secs, 19);
        assert_eq!(effective.server().inbound_queue_capacity, 12);
        assert_eq!(effective.server().inbound_queue_max_bytes, 384 * 1024);
        assert_eq!(effective.server().outbound_queue_max_bytes, 768 * 1024);
        assert_eq!(effective.server().outbound_admission_timeout_millis, 2_500);
        assert_eq!(effective.server().write_timeout_millis, 7_500);
        assert_eq!(effective.server().write_buffer_size_bytes, 64 * 1024);
        assert_eq!(effective.server().max_write_buffer_size_bytes, 1024 * 1024);
    }

    #[test]
    fn central_websocket_config_rejects_an_outbound_budget_below_one_maximum_message() {
        let central = lily_config::WebSocketConfig {
            enabled: true,
            max_outbound_message_size_bytes: 1025,
            outbound_queue_max_bytes: 1024,
            ..lily_config::WebSocketConfig::default()
        };

        assert!(matches!(
            EffectiveWsServerConfig::resolve(
                &WsAddressSource::Config,
                &WsServerConfigSource::Config,
                None,
                &central,
            ),
            Err(ServerError::EffectiveConfiguration(
                WsEffectiveConfigError::InvalidRuntime(_)
            ))
        ));
    }

    #[tokio::test]
    async fn application_composition_wires_outbound_authority_independently_from_inbound() {
        const INBOUND_LIMIT: usize = 512;
        const OUTBOUND_LIMIT: usize = 1024;

        fn exact_outbound(size: usize) -> crate::connection::BroadcastMessage {
            let mut message =
                crate::request::WsMessageBody::try_new("orders:created", serde_json::json!(""))
                    .expect("bounded fixture");
            message.timestamp = 123;
            let empty_size = message.to_message().unwrap().len();
            assert!(size >= empty_size);
            message.data = serde_json::json!("x".repeat(size - empty_size));
            assert_eq!(message.to_message().unwrap().len(), size);
            crate::connection::BroadcastMessage {
                target: crate::connection::BroadcastTarget::Namespace("orders".to_owned()),
                message,
                wire_format: crate::request::WsWireFormat::Text,
                exclude: Vec::new(),
            }
        }

        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                max_message_size: INBOUND_LIMIT,
                max_frame_size: INBOUND_LIMIT / 2,
                max_outbound_message_size: OUTBOUND_LIMIT,
                ..ServerConfig::default()
            })
            .build()
            .await
            .expect("divergent inbound/outbound limits must compose");

        assert_eq!(
            app.connection_manager().max_outbound_message_size(),
            OUTBOUND_LIMIT
        );
        app.dispatcher()
            .dispatch(exact_outbound(OUTBOUND_LIMIT))
            .await
            .expect("the exact outbound limit is independent from inbound admission");
        assert!(matches!(
            app.dispatcher()
                .dispatch(exact_outbound(OUTBOUND_LIMIT + 1))
                .await,
            Err(crate::connection::ConnectionError::InvalidOperation(
                crate::connection::ConnectionOperationError::OutboundMessageTooLarge
            ))
        ));

        app.close().await.expect("close composed application");
    }

    struct PendingWriter;

    impl Sink<Message> for PendingWriter {
        type Error = tokio_tungstenite::tungstenite::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn start_send(self: Pin<&mut Self>, _item: Message) -> Result<(), Self::Error> {
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }

        fn poll_close(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Pending
        }
    }

    const GATED_FLUSH_PENDING: usize = 0;
    const GATED_FLUSH_SUCCESS: usize = 1;
    const GATED_FLUSH_ERROR: usize = 2;

    struct FlushGate {
        state: AtomicUsize,
        started: AtomicBool,
        messages: StdMutex<Vec<Message>>,
        waker: StdMutex<Option<std::task::Waker>>,
    }

    impl FlushGate {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                state: AtomicUsize::new(GATED_FLUSH_PENDING),
                started: AtomicBool::new(false),
                messages: StdMutex::new(Vec::new()),
                waker: StdMutex::new(None),
            })
        }

        fn finish(&self, state: usize) {
            self.state.store(state, Ordering::Release);
            if let Some(waker) = self.waker.lock().unwrap().take() {
                waker.wake();
            }
        }

        fn messages(&self) -> Vec<Message> {
            self.messages.lock().unwrap().clone()
        }
    }

    struct FlushGatedWriter {
        gate: Arc<FlushGate>,
    }

    impl Sink<Message> for FlushGatedWriter {
        type Error = tokio_tungstenite::tungstenite::Error;

        fn poll_ready(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
            self.gate.started.store(true, Ordering::Release);
            self.gate.messages.lock().unwrap().push(item);
            Ok(())
        }

        fn poll_flush(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            match self.gate.state.load(Ordering::Acquire) {
                GATED_FLUSH_SUCCESS => Poll::Ready(Ok(())),
                GATED_FLUSH_ERROR => Poll::Ready(Err(tokio_tungstenite::tungstenite::Error::Io(
                    std::io::Error::new(std::io::ErrorKind::BrokenPipe, "controlled flush failure"),
                ))),
                _ => {
                    *self.gate.waker.lock().unwrap() = Some(context.waker().clone());
                    Poll::Pending
                }
            }
        }

        fn poll_close(
            self: Pin<&mut Self>,
            context: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.poll_flush(context)
        }
    }

    async fn wait_for_flush_start(gate: &FlushGate) {
        timeout(Duration::from_secs(1), async {
            while !gate.started.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("controlled writer must reach flush");
    }

    #[tokio::test]
    async fn queued_application_bytes_remain_reserved_through_transport_flush() {
        let payload = Arc::new(Message::Text("reserved-through-flush".into()));
        let expected = payload.as_ref().clone();
        let sibling = Arc::clone(&payload);
        let bytes = payload.len();
        let (frame, budget) = QueuedApplicationFrame::for_test_shared_with_budget(payload);
        let gate = FlushGate::new();
        let writer_gate = Arc::clone(&gate);

        let writer = tokio::spawn(async move {
            let mut writer = FlushGatedWriter { gate: writer_gate };
            send_queued_application_frame_with_deadline(&mut writer, frame, Duration::from_secs(1))
                .await
        });

        wait_for_flush_start(&gate).await;
        assert_eq!(budget.used_bytes(), bytes);
        gate.finish(GATED_FLUSH_SUCCESS);
        writer.await.unwrap().unwrap();
        assert_eq!(budget.used_bytes(), 0);
        assert_eq!(gate.messages(), [expected]);
        assert_eq!(
            Arc::strong_count(&sibling),
            1,
            "the writer must release its shared queue reference after materializing the owned frame"
        );
    }

    #[tokio::test]
    async fn queued_application_bytes_release_on_timeout_error_and_cancellation() {
        let timeout_frame = Message::Text("timeout".into());
        let (timeout_frame, timeout_budget) =
            QueuedApplicationFrame::for_test_with_budget(timeout_frame);
        let mut timeout_writer = FlushGatedWriter {
            gate: FlushGate::new(),
        };
        assert!(matches!(
            send_queued_application_frame_with_deadline(
                &mut timeout_writer,
                timeout_frame,
                Duration::from_millis(5),
            )
            .await,
            Err(WriterIoFailure::TimedOut)
        ));
        assert_eq!(timeout_budget.used_bytes(), 0);

        let error_frame = Message::Text("error".into());
        let (error_frame, error_budget) = QueuedApplicationFrame::for_test_with_budget(error_frame);
        let error_gate = FlushGate::new();
        let writer_gate = Arc::clone(&error_gate);
        let error_writer = tokio::spawn(async move {
            let mut writer = FlushGatedWriter { gate: writer_gate };
            send_queued_application_frame_with_deadline(
                &mut writer,
                error_frame,
                Duration::from_secs(1),
            )
            .await
        });
        wait_for_flush_start(&error_gate).await;
        error_gate.finish(GATED_FLUSH_ERROR);
        assert!(matches!(
            error_writer.await.unwrap(),
            Err(WriterIoFailure::Transport(_))
        ));
        assert_eq!(error_budget.used_bytes(), 0);

        let cancelled_frame = Message::Text("cancelled".into());
        let (cancelled_frame, cancelled_budget) =
            QueuedApplicationFrame::for_test_with_budget(cancelled_frame);
        let cancelled_gate = FlushGate::new();
        let writer_gate = Arc::clone(&cancelled_gate);
        let cancelled_writer = tokio::spawn(async move {
            let mut writer = FlushGatedWriter { gate: writer_gate };
            send_queued_application_frame_with_deadline(
                &mut writer,
                cancelled_frame,
                Duration::from_secs(1),
            )
            .await
        });
        wait_for_flush_start(&cancelled_gate).await;
        assert!(cancelled_budget.used_bytes() > 0);
        cancelled_writer.abort();
        let _ = cancelled_writer.await;
        assert_eq!(cancelled_budget.used_bytes(), 0);
    }

    #[test]
    fn reader_transport_errors_map_only_actionable_rfc_failures_to_close_frames() {
        use tokio_tungstenite::tungstenite::{
            Error,
            error::{CapacityError, ProtocolError},
            protocol::frame::coding::CloseCode,
        };

        for (error, expected_code) in [
            (
                Error::Protocol(ProtocolError::UnmaskedFrameFromClient),
                CloseCode::Protocol,
            ),
            (Error::Utf8, CloseCode::Invalid),
            (
                Error::Capacity(CapacityError::MessageTooLong {
                    size: 513,
                    max_size: 512,
                }),
                CloseCode::Size,
            ),
        ] {
            let frame = reader_transport_error_close_frame(&error)
                .expect("actionable RFC reader error must select a Close frame");
            assert_eq!(frame.code, expected_code);
            assert!(frame.reason.is_empty());
        }

        for error in [
            Error::ConnectionClosed,
            Error::AlreadyClosed,
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "peer reset",
            )),
            Error::Protocol(ProtocolError::ResetWithoutClosingHandshake),
            Error::Protocol(ProtocolError::ReceivedAfterClosing),
            Error::Capacity(CapacityError::TooManyHeaders),
        ] {
            assert!(
                reader_transport_error_close_frame(&error).is_none(),
                "non-actionable transport terminal must not attempt a Close: {error}"
            );
        }
    }

    #[tokio::test]
    async fn every_writer_operation_has_a_bounded_deadline() {
        let mut writer = PendingWriter;
        let send_failure = send_frame_with_deadline(
            &mut writer,
            Message::Text("blocked".into()),
            Duration::from_millis(5),
        )
        .await
        .unwrap_err();
        assert!(matches!(send_failure, WriterIoFailure::TimedOut));
        assert_eq!(
            send_failure.close_category(),
            crate::middleware::WsConnectionCloseCategory::WriteTimeout
        );
        let metrics = ConnectionManager::new();
        send_failure.record_timeout(metrics.metrics());
        assert_eq!(metrics.metrics_snapshot().timeouts, 1);
        assert!(matches!(
            flush_with_deadline(&mut writer, Duration::from_millis(5)).await,
            Err(WriterIoFailure::TimedOut)
        ));

        let saturation = WriterIoFailure::Transport(
            tokio_tungstenite::tungstenite::Error::WriteBufferFull(Message::Text("bounded".into())),
        );
        assert_eq!(
            saturation.close_category(),
            crate::middleware::WsConnectionCloseCategory::Reset
        );
    }

    #[tokio::test]
    async fn timed_out_writer_task_releases_its_connection_permit() {
        let permits = Arc::new(Semaphore::new(1));
        let owned_permit = Arc::clone(&permits).acquire_owned().await.unwrap();

        let writer_task = tokio::spawn(async move {
            let _owned_permit = owned_permit;
            let mut writer = PendingWriter;
            send_frame_with_deadline(
                &mut writer,
                Message::Text("peer-does-not-read".into()),
                Duration::from_millis(5),
            )
            .await
            .unwrap_err()
            .close_category()
        });

        assert_eq!(
            timeout(Duration::from_millis(100), writer_task)
                .await
                .expect("writer deadline must bound the connection task")
                .unwrap(),
            crate::middleware::WsConnectionCloseCategory::WriteTimeout
        );
        assert_eq!(permits.available_permits(), 1);
    }

    #[tokio::test]
    async fn close_drain_preserves_writer_terminal_category_and_bounds_pending_writer() {
        assert_eq!(
            writer_drain_category(
                std::future::ready(crate::middleware::WsConnectionCloseCategory::Reset),
                Duration::from_millis(50),
            )
            .await,
            Some(crate::middleware::WsConnectionCloseCategory::Reset)
        );
        assert_eq!(
            writer_drain_category(std::future::pending(), Duration::from_millis(5),).await,
            None
        );
    }

    #[tokio::test]
    async fn outbound_mux_prioritizes_close_and_preserves_explicit_provenance() {
        let (data_sender, data) = mpsc::channel(1);
        data_sender
            .try_send(QueuedApplicationFrame::for_test(Message::Text(
                "application".into(),
            )))
            .unwrap();
        let (control_sender, control) = connection_control_channel();
        control_sender
            .send_protocol(Message::Pong(vec![2]))
            .unwrap();
        control_sender
            .request_close(
                Message::Close(Some(crate::request::WsCloseReason::IdentityExpired.frame())),
                crate::middleware::WsConnectionCloseCategory::Application,
            )
            .unwrap();

        let mut outbound = OutboundMultiplexer { data, control };
        let peer_close = CancellationToken::new();
        let automatic_control_flush = Notify::new();
        automatic_control_flush.notify_one();
        let mut heartbeat = interval(Duration::from_secs(60));
        tokio::time::sleep(Duration::from_millis(1)).await;

        assert!(matches!(
            outbound
                .next(&peer_close, &automatic_control_flush, &mut heartbeat)
                .await,
            OutboundCommand::Close {
                message: Message::Close(Some(frame)),
                category: crate::middleware::WsConnectionCloseCategory::Application,
            } if frame.reason == crate::request::WsCloseReason::IdentityExpired.reason()
        ));
        assert!(matches!(
            outbound
                .next(&peer_close, &automatic_control_flush, &mut heartbeat)
                .await,
            OutboundCommand::Protocol(Message::Pong(payload)) if payload == vec![2]
        ));
        assert!(matches!(
            outbound
                .next(&peer_close, &automatic_control_flush, &mut heartbeat)
                .await,
            OutboundCommand::FlushAutomaticControl
        ));
        assert!(matches!(
            outbound
                .next(&peer_close, &automatic_control_flush, &mut heartbeat)
                .await,
            OutboundCommand::Heartbeat
        ));
        assert!(matches!(
            outbound
                .next(&peer_close, &automatic_control_flush, &mut heartbeat)
                .await,
            OutboundCommand::Data(frame)
                if matches!(frame.message(), Message::Text(payload) if payload == "application")
        ));
    }

    async fn heartbeat_close_race_fixture() -> (
        Arc<ConnectionManager>,
        WebSocketContext,
        ConnectionControlReceiver,
    ) {
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            128,
            128,
            ["orders".into()],
        ));
        let connection_id = Uuid::new_v4();
        let (data_sender, _data_receiver) = mpsc::channel(1);
        let (control_sender, control_receiver) = connection_control_channel();
        manager
            .add_connection_with_control(
                connection_id,
                data_sender,
                control_sender,
                lily_web_core::RequestConnectionInfo::default(),
                WsTransportSecurity::Plaintext,
                Some("orders".into()),
            )
            .await
            .unwrap();
        let context = WebSocketContext::new(connection_id, Arc::clone(&manager), "orders".into());
        (manager, context, control_receiver)
    }

    #[tokio::test]
    async fn heartbeat_timeout_close_and_application_close_share_one_first_writer_slot() {
        let (manager, context, mut control) = heartbeat_close_race_fixture().await;
        let application_reason = crate::request::WsCloseReason::IdentityExpired.reason();
        assert_eq!(
            context
                .close(crate::CloseConnection::policy(application_reason).unwrap())
                .await
                .unwrap(),
            CloseRequestOutcome::Requested
        );
        assert_eq!(
            queue_heartbeat_timeout_close(manager.as_ref(), context.connection_id())
                .await
                .unwrap(),
            CloseRequestOutcome::AlreadyClosing
        );
        let selected = timeout(Duration::from_secs(1), control.next())
            .await
            .expect("application winner must be observable within the test deadline")
            .expect("control channel remains open");
        let ConnectionControlFrame::Close(request) = selected else {
            panic!("application winner must occupy the Close control slot");
        };
        assert_eq!(
            request.category(),
            crate::middleware::WsConnectionCloseCategory::Application
        );
        assert!(matches!(
            request.message(),
            Message::Close(Some(frame)) if frame.reason == application_reason
        ));
        assert!(
            timeout(Duration::from_millis(20), control.next())
                .await
                .is_err(),
            "the losing heartbeat timeout must not publish a second terminal request"
        );

        let (manager, context, mut control) = heartbeat_close_race_fixture().await;
        assert_eq!(
            queue_heartbeat_timeout_close(manager.as_ref(), context.connection_id())
                .await
                .unwrap(),
            CloseRequestOutcome::Requested
        );
        assert_eq!(
            context
                .close(crate::CloseConnection::policy("late_application_close").unwrap())
                .await
                .unwrap(),
            CloseRequestOutcome::AlreadyClosing
        );
        let selected = timeout(Duration::from_secs(1), control.next())
            .await
            .expect("heartbeat winner must be observable within the test deadline")
            .expect("control channel remains open");
        let ConnectionControlFrame::Close(request) = selected else {
            panic!("heartbeat winner must occupy the Close control slot");
        };
        assert_eq!(
            request.category(),
            crate::middleware::WsConnectionCloseCategory::IdleTimeout
        );
        assert!(matches!(
            request.message(),
            Message::Close(Some(frame))
                if frame.code == crate::request::WsCloseReason::IdleTimeout.code()
                    && frame.reason == crate::request::WsCloseReason::IdleTimeout.reason()
        ));
        assert!(
            timeout(Duration::from_millis(20), control.next())
                .await
                .is_err(),
            "the losing application close must not publish a second terminal request"
        );
    }

    #[tokio::test]
    async fn startup_expiry_preserves_an_already_selected_close_frame() {
        let connection_id = Uuid::new_v4();
        let manager = ConnectionManager::new();
        let (control_sender, mut control) = connection_control_channel();
        let selected = Message::Close(Some(crate::request::WsCloseReason::ServerShutdown.frame()));
        assert_eq!(
            control_sender
                .request_close(
                    selected.clone(),
                    crate::middleware::WsConnectionCloseCategory::ServerShutdown,
                )
                .unwrap(),
            CloseRequestOutcome::Requested
        );
        assert_eq!(
            control_sender
                .request_close(
                    Message::Close(Some(crate::request::WsCloseReason::IdentityExpired.frame(),)),
                    crate::middleware::WsConnectionCloseCategory::IdentityExpired,
                )
                .unwrap(),
            CloseRequestOutcome::AlreadyClosing
        );

        let received = materialize_published_stage_close(
            connection_id,
            &manager,
            &mut control,
            PublishedConnectionStageClose::AlreadySelected,
            crate::middleware::WsConnectionCloseCategory::InternalError,
            Duration::from_millis(50),
        )
        .await
        .expect("the first-writer close remains available to the startup transport");
        assert_eq!(received.0, selected);
        assert_eq!(
            received.1,
            crate::middleware::WsConnectionCloseCategory::ServerShutdown
        );
    }

    #[test]
    fn identity_expiry_has_one_exact_wire_cleanup_and_lifecycle_category() {
        use crate::middleware::WsConnectionCloseCategory;
        use crate::request::WsCloseReason;
        use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;

        let reason = WsCloseReason::IdentityExpired;

        assert_eq!(reason.code(), CloseCode::Policy);
        assert_eq!(reason.reason(), "lily.v2.identity_expired");
        assert_eq!(
            WsApp::close_category_from_reason(reason),
            WsConnectionCloseCategory::IdentityExpired
        );
        assert_eq!(
            disconnect_reason_from_category(WsConnectionCloseCategory::IdentityExpired),
            DisconnectReason::IdentityExpired
        );
        assert_eq!(
            WsConnectionCloseCategory::IdentityExpired.as_str(),
            "identity_expired"
        );
        let send_error: crate::connection::ConnectionError =
            crate::connection::ConnectionOperationError::IdentityExpired.into();
        assert!(connection_error_is_identity_expired(&send_error));
    }

    #[test]
    fn explicit_builder_sources_override_only_the_values_they_own() {
        let central = lily_config::WebSocketConfig {
            enabled: false,
            max_connections: 77,
            ..lily_config::WebSocketConfig::default()
        };
        let explicit_address = WsAddressSource::Explicit("127.0.0.1:0".into());
        let effective = EffectiveWsServerConfig::resolve(
            &explicit_address,
            &WsServerConfigSource::Config,
            None,
            &central,
        )
        .expect("explicit builder creation enables the central runtime profile");
        assert_eq!(effective.listen_address(), "127.0.0.1:0");
        assert_eq!(effective.server().max_connections, 77);

        let explicit_config = ServerConfig {
            max_connections: 9,
            ..ServerConfig::default()
        };
        let effective = EffectiveWsServerConfig::resolve(
            &explicit_address,
            &WsServerConfigSource::Explicit(Box::new(explicit_config)),
            None,
            &central,
        )
        .expect("whole explicit ServerConfig wins");
        assert_eq!(effective.server().max_connections, 9);
    }

    #[test]
    fn config_backed_disabled_and_invalid_listener_states_are_typed() {
        let disabled = lily_config::WebSocketConfig::default();
        assert!(matches!(
            EffectiveWsServerConfig::resolve(
                &WsAddressSource::Config,
                &WsServerConfigSource::Config,
                None,
                &disabled,
            ),
            Err(ServerError::DisabledByConfiguration)
        ));

        let zero_port = lily_config::WebSocketConfig {
            enabled: true,
            port: 0,
            ..lily_config::WebSocketConfig::default()
        };
        assert!(matches!(
            EffectiveWsServerConfig::resolve(
                &WsAddressSource::Config,
                &WsServerConfigSource::Config,
                None,
                &zero_port,
            ),
            Err(ServerError::EffectiveConfiguration(
                WsEffectiveConfigError::ZeroPort
            ))
        ));
    }

    #[test]
    fn websocket_builder_exposes_the_same_explicit_tracing_modes_as_other_roots() {
        let disabled = WsAppBuilder::default();
        assert!(matches!(disabled.tracing_mode, TracingMode::Disabled));

        let configured = WsAppBuilder::default().tracing_config(TraceConfig::default());
        assert!(matches!(
            configured.tracing_mode,
            TracingMode::OwnedConfig(_)
        ));

        let path = WsAppBuilder::default().tracing_config_path("trace.toml");
        assert!(matches!(path.tracing_mode, TracingMode::OwnedPath(_)));

        let external = WsAppBuilder::default().tracing_external();
        assert!(matches!(external.tracing_mode, TracingMode::External));
    }

    #[tokio::test]
    async fn duplicate_identity_registration_fails_before_component_initialization() {
        let error = WsAppBuilder::new("127.0.0.1:0")
            .identity_middleware::<IdentityProbe>()
            .identity_middleware::<IdentityProbe>()
            .build()
            .await
            .err()
            .expect("the application owns exactly one identity middleware slot");

        assert!(matches!(error, ServerError::Configuration(_)));
        assert!(error.to_string().contains("only one WebSocket identity"));
    }

    struct DropFlag(Arc<AtomicBool>);

    struct RejectHandshakeMiddleware<const STATUS: u16>;

    #[derive(Default, lily_injection::Injectable)]
    #[service(lifetime = "Scoped")]
    struct Dec009UpgradeScopeProbe;

    struct Dec009ScopedHandshakeProbe;

    static DEC009_SCOPE_INITIALIZED: AtomicUsize = AtomicUsize::new(0);
    static DEC009_SCOPE_RESOLVED: AtomicUsize = AtomicUsize::new(0);
    static DEC009_SCOPE_DISPOSED: AtomicUsize = AtomicUsize::new(0);

    #[derive(Clone)]
    enum IdentityProbeDecision {
        Accept,
        AcceptExpiring(Duration),
        Reject(crate::middleware::WsHandshakeRejection),
    }

    #[derive(Clone)]
    struct IdentityProbeConfig {
        calls: Arc<AtomicUsize>,
        started: Arc<tokio::sync::Notify>,
        release: Option<Arc<tokio::sync::Notify>>,
        decision: IdentityProbeDecision,
        principal_observed: Arc<AtomicBool>,
        connection_local_observed: Arc<AtomicBool>,
    }

    struct IdentityProbe(IdentityProbeConfig);
    struct IdentityContextProbe(IdentityProbeConfig);
    struct PendingIdentityAdmissionProbe;
    struct PendingIdentityOpenedProbe;

    #[derive(Debug, PartialEq, Eq)]
    struct IdentityConnectionLocal(&'static str);

    #[derive(Clone)]
    struct ConnectionLifecycleProbeConfig {
        reject_admission: bool,
        admit_calls: Arc<AtomicUsize>,
        opened_calls: Arc<AtomicUsize>,
        closed_calls: Arc<AtomicUsize>,
        close_categories: Arc<StdMutex<Vec<crate::middleware::WsConnectionCloseCategory>>>,
    }

    struct ConnectionLifecycleProbe(ConnectionLifecycleProbeConfig);
    struct OrderedOpenProbe<const INDEX: usize>;

    static CONNECTION_PROBE_CONFIG: StdMutex<Option<ConnectionLifecycleProbeConfig>> =
        StdMutex::new(None);
    static CONNECTION_PROBE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    static IDENTITY_PROBE_CONFIG: StdMutex<Option<IdentityProbeConfig>> = StdMutex::new(None);
    static IDENTITY_PROBE_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    static PENDING_IDENTITY_ADMISSION_STARTED: tokio::sync::Notify =
        tokio::sync::Notify::const_new();
    static PENDING_IDENTITY_OPENED_STARTED: tokio::sync::Notify = tokio::sync::Notify::const_new();
    static PENDING_IDENTITY_OPENED_CLOSE_CATEGORIES: StdMutex<
        Vec<crate::middleware::WsConnectionCloseCategory>,
    > = StdMutex::new(Vec::new());

    static MESSAGE_REJECTION_EVENTS: StdMutex<Vec<&'static str>> = StdMutex::new(Vec::new());
    static PANIC_UNWIND_EVENTS: StdMutex<Vec<&'static str>> = StdMutex::new(Vec::new());
    static PANIC_UNWIND_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    struct MessageLifecycleProbe<const KIND: u8>;

    #[async_trait]
    impl lily_injection::ServiceTrait for Dec009UpgradeScopeProbe {
        async fn initialize(&mut self) -> Result<(), lily_error::injection::InjectionError> {
            assert!(ProcessContext::current().is_some());
            DEC009_SCOPE_INITIALIZED.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn dispose(&self) -> Result<(), lily_error::injection::InjectionError> {
            assert!(ProcessContext::current().is_some());
            DEC009_SCOPE_DISPOSED.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl WebSocketHandshakeMiddleware for Dec009ScopedHandshakeProbe {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::middleware::WsMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(
                "dec_009_scoped_handshake_probe",
                MiddlewareKind::WebSocketHandshake,
            )
        }

        async fn handle(
            &self,
            exchange: &mut crate::middleware::WsHandshakeExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::middleware::WsHandshakeRejection> {
            let _probe = exchange
                .service::<Dec009UpgradeScopeProbe>()
                .await
                .expect("DEC-009 Upgrade scope resolves its scoped probe");
            DEC009_SCOPE_RESOLVED.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl<const STATUS: u16> WebSocketHandshakeMiddleware for RejectHandshakeMiddleware<STATUS> {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::middleware::WsMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(
                "test_handshake_middleware",
                MiddlewareKind::WebSocketHandshake,
            )
        }

        async fn handle(
            &self,
            _exchange: &mut crate::middleware::WsHandshakeExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::middleware::WsHandshakeRejection> {
            let code = MiddlewareErrorCode::new("TEST_HANDSHAKE_REJECTED").unwrap();
            let rejection = if STATUS == 401 {
                crate::middleware::WsHandshakeRejection::unauthorized(
                    code,
                    crate::HeaderValue::from_static("Bearer"),
                )
                .expect("static authentication challenge is valid")
            } else {
                crate::middleware::WsHandshakeRejection::try_new(STATUS, code)
                    .expect("test status is a supported handshake rejection")
            };
            Err(rejection)
        }
    }

    #[async_trait]
    impl WebSocketIdentityMiddleware for IdentityProbe {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::middleware::WsMiddlewareInitError> {
            IDENTITY_PROBE_CONFIG
                .lock()
                .unwrap()
                .clone()
                .map(Self)
                .ok_or(crate::middleware::WsMiddlewareInitError::InvalidConfiguration)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("test_identity", MiddlewareKind::WebSocketHandshake)
        }

        async fn identify(
            &self,
            _exchange: &mut crate::middleware::WsHandshakeExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<crate::middleware::WebSocketIdentity, crate::middleware::WsHandshakeRejection>
        {
            self.0.calls.fetch_add(1, Ordering::SeqCst);
            self.0.started.notify_one();
            if let Some(release) = self.0.release.as_ref() {
                release.notified().await;
            }

            match &self.0.decision {
                IdentityProbeDecision::Accept | IdentityProbeDecision::AcceptExpiring(_) => {
                    let principal = Principal::new(
                        "identity-subject",
                        ["operator".to_owned()],
                        ["websocket:connect".to_owned()],
                        serde_json::Map::new(),
                    );
                    let mut authenticated =
                        crate::AuthenticatedWebSocketIdentity::try_new(principal)
                            .expect("identity fixture subject is bounded");
                    if let IdentityProbeDecision::AcceptExpiring(ttl) = &self.0.decision {
                        authenticated =
                            authenticated.expires_at(tokio::time::Instant::now() + *ttl);
                    }
                    let mut identity =
                        crate::middleware::WebSocketIdentity::authenticated(authenticated);
                    identity
                        .insert_connection_local(IdentityConnectionLocal("tenant-42"))
                        .expect("one test connection-local value is within the bound");
                    Ok(identity)
                }
                IdentityProbeDecision::Reject(rejection) => Err(rejection.clone()),
            }
        }
    }

    #[async_trait]
    impl WsConnectionMiddleware for PendingIdentityAdmissionProbe {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::middleware::WsMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(
                "pending_identity_admission",
                MiddlewareKind::WebSocketConnection,
            )
        }

        async fn admit(
            &self,
            _context: Arc<WebSocketContext>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::middleware::WsMiddlewareError> {
            PENDING_IDENTITY_ADMISSION_STARTED.notify_one();
            std::future::pending().await
        }
    }

    #[async_trait]
    impl WsConnectionMiddleware for PendingIdentityOpenedProbe {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::middleware::WsMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(
                "pending_identity_opened",
                MiddlewareKind::WebSocketConnection,
            )
        }

        async fn admit(
            &self,
            _context: Arc<WebSocketContext>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::middleware::WsMiddlewareError> {
            Ok(())
        }

        async fn opened(
            &self,
            _context: Arc<WebSocketContext>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::middleware::WsMiddlewareError> {
            PENDING_IDENTITY_OPENED_STARTED.notify_one();
            std::future::pending().await
        }

        async fn closed(
            &self,
            _context: Arc<WebSocketContext>,
            category: crate::middleware::WsConnectionCloseCategory,
            _cancellation: crate::CleanupCancellation,
        ) -> Result<(), crate::middleware::WsMiddlewareError> {
            PENDING_IDENTITY_OPENED_CLOSE_CATEGORIES
                .lock()
                .unwrap()
                .push(category);
            Ok(())
        }
    }

    #[async_trait]
    impl WsConnectionMiddleware for IdentityContextProbe {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::middleware::WsMiddlewareInitError> {
            IDENTITY_PROBE_CONFIG
                .lock()
                .unwrap()
                .clone()
                .map(Self)
                .ok_or(crate::middleware::WsMiddlewareInitError::InvalidConfiguration)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("test_identity_context", MiddlewareKind::WebSocketConnection)
        }

        async fn admit(
            &self,
            context: Arc<WebSocketContext>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::middleware::WsMiddlewareError> {
            self.0.principal_observed.store(
                context.principal().is_some_and(|principal| {
                    principal.subject() == "identity-subject"
                        && principal.has_role("operator")
                        && principal.has_scope("websocket:connect")
                }),
                Ordering::SeqCst,
            );
            self.0.connection_local_observed.store(
                context.connection_locals().get::<IdentityConnectionLocal>()
                    == Some(&IdentityConnectionLocal("tenant-42")),
                Ordering::SeqCst,
            );

            // End the real transport test immediately after the post-101
            // context observation; the rejection itself is not under test.
            Err(crate::middleware::WsMiddlewareError::rejected(
                MiddlewareErrorCode::new("TEST_IDENTITY_CONTEXT_OBSERVED").unwrap(),
            ))
        }
    }

    #[async_trait]
    impl WsConnectionMiddleware for ConnectionLifecycleProbe {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::middleware::WsMiddlewareInitError> {
            CONNECTION_PROBE_CONFIG
                .lock()
                .unwrap()
                .clone()
                .map(Self)
                .ok_or(crate::middleware::WsMiddlewareInitError::InvalidConfiguration)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(
                "test_connection_lifecycle",
                MiddlewareKind::WebSocketConnection,
            )
        }

        async fn admit(
            &self,
            _context: Arc<WebSocketContext>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::middleware::WsMiddlewareError> {
            self.0.admit_calls.fetch_add(1, Ordering::SeqCst);
            if self.0.reject_admission {
                Err(crate::middleware::WsMiddlewareError::rejected(
                    MiddlewareErrorCode::new("TEST_CONNECTION_REJECTED").unwrap(),
                ))
            } else {
                Ok(())
            }
        }

        async fn opened(
            &self,
            _context: Arc<WebSocketContext>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::middleware::WsMiddlewareError> {
            self.0.opened_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn closed(
            &self,
            _context: Arc<WebSocketContext>,
            category: crate::middleware::WsConnectionCloseCategory,
            _cancellation: crate::CleanupCancellation,
        ) -> Result<(), crate::middleware::WsMiddlewareError> {
            self.0.closed_calls.fetch_add(1, Ordering::SeqCst);
            self.0.close_categories.lock().unwrap().push(category);
            Ok(())
        }
    }

    #[async_trait]
    impl<const INDEX: usize> WsConnectionMiddleware for OrderedOpenProbe<INDEX> {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::middleware::WsMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new(
                if INDEX == 0 {
                    "open_prefix"
                } else {
                    "open_rejecting"
                },
                MiddlewareKind::WebSocketConnection,
            )
        }

        async fn admit(
            &self,
            _context: Arc<WebSocketContext>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::middleware::WsMiddlewareError> {
            OPEN_ORDER_EVENTS.lock().unwrap().push(if INDEX == 0 {
                "prefix.admit"
            } else {
                "rejecting.admit"
            });
            Ok(())
        }

        async fn opened(
            &self,
            _context: Arc<WebSocketContext>,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), crate::middleware::WsMiddlewareError> {
            OPEN_ORDER_EVENTS.lock().unwrap().push(if INDEX == 0 {
                "prefix.opened"
            } else {
                "rejecting.opened"
            });
            if INDEX == 1 {
                Err(crate::middleware::WsMiddlewareError::rejected(
                    MiddlewareErrorCode::new("TEST_CONNECTION_OPEN_REJECTED").unwrap(),
                ))
            } else {
                Ok(())
            }
        }

        async fn closed(
            &self,
            _context: Arc<WebSocketContext>,
            _category: crate::middleware::WsConnectionCloseCategory,
            _cancellation: crate::CleanupCancellation,
        ) -> Result<(), crate::middleware::WsMiddlewareError> {
            OPEN_ORDER_EVENTS.lock().unwrap().push(if INDEX == 0 {
                "prefix.closed"
            } else {
                "rejecting.closed"
            });
            Ok(())
        }
    }

    #[async_trait]
    impl<const KIND: u8> WsMessageMiddleware for MessageLifecycleProbe<KIND> {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::middleware::WsMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            let name = match KIND {
                0 => "test_outer_message_middleware",
                1 => "test_rejecting_message_middleware",
                2 => "test_unentered_message_middleware",
                3 => "test_panic_unwind_message_middleware",
                4 => "test_graceful_drain_message_middleware",
                5 => "test_competing_close_message_middleware",
                _ => "test_message_middleware",
            };
            MiddlewareDescriptor::new(name, MiddlewareKind::WebSocketMessage)
        }

        async fn before_message(
            &self,
            _exchange: &mut WsMessageExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<WsMessageDecision, crate::middleware::WsMiddlewareError> {
            match KIND {
                0 => {
                    MESSAGE_REJECTION_EVENTS
                        .lock()
                        .unwrap()
                        .push("outer.before");
                    Ok(WsMessageDecision::Continue)
                }
                1 => {
                    MESSAGE_REJECTION_EVENTS
                        .lock()
                        .unwrap()
                        .push("rejecting.before");
                    Ok(WsMessageDecision::Reject(
                        crate::request::WsProtocolErrorCode::MiddlewareRejected,
                    ))
                }
                2 => {
                    MESSAGE_REJECTION_EVENTS
                        .lock()
                        .unwrap()
                        .push("unentered.before");
                    Ok(WsMessageDecision::Continue)
                }
                3 => {
                    PANIC_UNWIND_EVENTS.lock().unwrap().push("entered.before");
                    Ok(WsMessageDecision::Continue)
                }
                4 => {
                    GRACEFUL_DRAIN_MIDDLEWARE_BEFORE.store(true, Ordering::SeqCst);
                    Ok(WsMessageDecision::Continue)
                }
                _ => Ok(WsMessageDecision::Continue),
            }
        }

        async fn after_message(
            &self,
            _exchange: &mut WsMessageExchange,
            _outcome: WsMessageOutcome,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<WsMessageDecision, crate::middleware::WsMiddlewareError> {
            match KIND {
                0 => {
                    MESSAGE_REJECTION_EVENTS.lock().unwrap().push("outer.after");
                    Ok(WsMessageDecision::Close(
                        crate::request::WsCloseReason::PolicyViolation,
                    ))
                }
                1 => {
                    MESSAGE_REJECTION_EVENTS
                        .lock()
                        .unwrap()
                        .push("rejecting.after");
                    Ok(WsMessageDecision::Continue)
                }
                2 => {
                    MESSAGE_REJECTION_EVENTS
                        .lock()
                        .unwrap()
                        .push("unentered.after");
                    Ok(WsMessageDecision::Continue)
                }
                3 => {
                    PANIC_UNWIND_EVENTS.lock().unwrap().push("entered.after");
                    Ok(WsMessageDecision::Continue)
                }
                4 => {
                    GRACEFUL_DRAIN_MIDDLEWARE_AFTER.store(true, Ordering::SeqCst);
                    Ok(WsMessageDecision::Continue)
                }
                5 => Ok(WsMessageDecision::Close(
                    crate::request::WsCloseReason::PolicyViolation,
                )),
                _ => Ok(WsMessageDecision::Continue),
            }
        }

        async fn on_message_termination(
            &self,
            _context: crate::middleware::WsMessageTerminationContext<'_>,
            _cancellation: crate::CleanupCancellation,
        ) -> Result<(), crate::middleware::WsMiddlewareError> {
            if KIND == 3 {
                PANIC_UNWIND_EVENTS
                    .lock()
                    .unwrap()
                    .push("entered.termination");
            }
            Ok(())
        }
    }

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct AppTestController;

    #[async_trait]
    impl crate::controller::WebSocketControllerTrait for AppTestController {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
            Ok(Self)
        }
    }

    impl WebSocketControllerDefinition for AppTestController {
        fn namespace() -> &'static str {
            "orders"
        }

        fn handshake_middleware_registrations() -> Vec<WebSocketHandshakeMiddlewareRegistration> {
            Vec::new()
        }

        fn connection_middleware_registrations() -> Vec<WebSocketConnectionMiddlewareRegistration> {
            Vec::new()
        }

        fn message_middleware_registrations() -> Vec<WebSocketMessageMiddlewareRegistration> {
            Vec::new()
        }

        fn guard_registrations() -> Vec<WebSocketGuardRegistration> {
            Vec::new()
        }

        fn timeout() -> Option<Duration> {
            None
        }

        fn asyncapi_registration() -> WebSocketAsyncApiRegistration {
            WebSocketAsyncApiRegistration::unspecified()
        }
    }

    fn app_test_controller_registration() -> WebSocketControllerRegistration {
        WebSocketControllerRegistration::of::<AppTestController>()
    }

    #[linkme::distributed_slice(crate::controller::WEBSOCKET_CONTROLLER_REGISTRATIONS)]
    static APP_TEST_CONTROLLER_REGISTRATION: crate::controller::WebSocketControllerRegistrationFn =
        app_test_controller_registration;

    static TRUSTED_ACTION_OBSERVED: AtomicBool = AtomicBool::new(false);
    static DENIED_ACTION_INVOKED: AtomicBool = AtomicBool::new(false);
    static MIDDLEWARE_ACTION_INVOKED: AtomicBool = AtomicBool::new(false);
    static TIMEOUT_ACTION_DROPPED: AtomicBool = AtomicBool::new(false);
    static SHUTDOWN_ACTION_DROPPED: AtomicBool = AtomicBool::new(false);
    static GRACEFUL_DRAIN_ACTION_STARTED: AtomicBool = AtomicBool::new(false);
    static GRACEFUL_DRAIN_ACTION_COMPLETED: AtomicBool = AtomicBool::new(false);
    static GRACEFUL_DRAIN_ACTION_DROPPED: AtomicBool = AtomicBool::new(false);
    static GRACEFUL_DRAIN_MIDDLEWARE_BEFORE: AtomicBool = AtomicBool::new(false);
    static GRACEFUL_DRAIN_MIDDLEWARE_AFTER: AtomicBool = AtomicBool::new(false);
    static POST_SHUTDOWN_ACTION_INVOKED: AtomicBool = AtomicBool::new(false);
    static GRACEFUL_DRAIN_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    static CONTROL_READER_ACTION_STARTED: AtomicBool = AtomicBool::new(false);
    static CONTROL_READER_ACTION_COMPLETED: AtomicBool = AtomicBool::new(false);
    static CONTROL_READER_ACTION_DROPPED: AtomicBool = AtomicBool::new(false);
    static CONTROL_READER_DEFERRED_ACTION_INVOKED: AtomicBool = AtomicBool::new(false);
    static CONTROL_READER_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    static MESSAGE_LOCAL_ACTION_OBSERVED: AtomicBool = AtomicBool::new(false);

    struct StaticDropFlag(&'static AtomicBool);

    impl Drop for StaticDropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    struct AppTestMessageAction {
        _controller: Arc<AppTestController>,
    }

    impl WebSocketMessageAction for AppTestMessageAction {
        fn call(&self, invocation: WebSocketMessageInvocation) -> WebSocketActionFuture {
            let route = format!("{}:{}", invocation.namespace(), invocation.event());
            match route.as_str() {
                "orders:panic" => panic!("synchronous test action panic"),
                "orders:trusted" => {
                    TRUSTED_ACTION_OBSERVED
                        .store(invocation.context().handshake().is_some(), Ordering::SeqCst);
                    Box::pin(async { Ok(PendingWebSocketActionOutcome::NoReply) })
                }
                "orders:denied" => {
                    DENIED_ACTION_INVOKED.store(true, Ordering::SeqCst);
                    Box::pin(async { Ok(PendingWebSocketActionOutcome::NoReply) })
                }
                "orders:middleware-reject" => {
                    MIDDLEWARE_ACTION_INVOKED.store(true, Ordering::SeqCst);
                    Box::pin(async { Ok(PendingWebSocketActionOutcome::NoReply) })
                }
                "orders:pending-timeout" => Box::pin(async {
                    let _drop = StaticDropFlag(&TIMEOUT_ACTION_DROPPED);
                    std::future::pending::<()>().await;
                    Ok(PendingWebSocketActionOutcome::NoReply)
                }),
                "orders:pending-shutdown" => Box::pin(async {
                    let _drop = StaticDropFlag(&SHUTDOWN_ACTION_DROPPED);
                    std::future::pending::<()>().await;
                    Ok(PendingWebSocketActionOutcome::NoReply)
                }),
                "orders:shutdown-drain" => Box::pin(async {
                    let _drop = StaticDropFlag(&GRACEFUL_DRAIN_ACTION_DROPPED);
                    GRACEFUL_DRAIN_ACTION_STARTED.store(true, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    GRACEFUL_DRAIN_ACTION_COMPLETED.store(true, Ordering::SeqCst);
                    Ok(PendingWebSocketActionOutcome::Emit {
                        event: "orders:shutdown-drained".into(),
                        payload: DecodedWebSocketPayload::Json(
                            serde_json::json!({"drained": true}),
                        ),
                    })
                }),
                "orders:after-shutdown" => {
                    POST_SHUTDOWN_ACTION_INVOKED.store(true, Ordering::SeqCst);
                    Box::pin(async { Ok(PendingWebSocketActionOutcome::NoReply) })
                }
                "orders:long-control-pong" => Box::pin(async {
                    CONTROL_READER_ACTION_STARTED.store(true, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_millis(2_500)).await;
                    CONTROL_READER_ACTION_COMPLETED.store(true, Ordering::SeqCst);
                    Ok(PendingWebSocketActionOutcome::Emit {
                        event: "orders:long-control-complete".into(),
                        payload: DecodedWebSocketPayload::Json(
                            serde_json::json!({"completed": true}),
                        ),
                    })
                }),
                "orders:long-control-pending" => Box::pin(async {
                    let _drop = StaticDropFlag(&CONTROL_READER_ACTION_DROPPED);
                    CONTROL_READER_ACTION_STARTED.store(true, Ordering::SeqCst);
                    std::future::pending::<()>().await;
                    Ok(PendingWebSocketActionOutcome::NoReply)
                }),
                "orders:after-long-control" => {
                    CONTROL_READER_DEFERRED_ACTION_INVOKED.store(true, Ordering::SeqCst);
                    Box::pin(async { Ok(PendingWebSocketActionOutcome::NoReply) })
                }
                "orders:failure" => Box::pin(async {
                    Err(WebSocketActionError::internal(std::io::Error::other(
                        "test action failure",
                    )))
                }),
                "orders:close" => Box::pin(async {
                    Ok(PendingWebSocketActionOutcome::Close(
                        crate::CloseConnection::try_new(4001, "application complete")
                            .expect("test close is bounded"),
                    ))
                }),
                "orders:codec-panic" => Box::pin(async {
                    Ok(PendingWebSocketActionOutcome::Emit {
                        event: "orders:codec-result".into(),
                        payload: DecodedWebSocketPayload::Json(serde_json::json!({"ok": true})),
                    })
                }),
                "orders:message-local" => {
                    MESSAGE_LOCAL_ACTION_OBSERVED.store(
                        invocation.message_local::<PipelineMessageLocal>()
                            == Some(PipelineMessageLocal(2)),
                        Ordering::SeqCst,
                    );
                    Box::pin(async { Ok(PendingWebSocketActionOutcome::NoReply) })
                }
                _ => Box::pin(async { Ok(PendingWebSocketActionOutcome::NoReply) }),
            }
        }
    }

    fn bind_app_test_message(
        controller: ErasedWebSocketController,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
        let controller = downcast_websocket_controller::<AppTestController>(controller)?;
        Ok(BoundWebSocketOperation::Message(Arc::new(
            AppTestMessageAction {
                _controller: controller,
            },
        )))
    }

    fn pending_action(
        event: &'static str,
        guards: Vec<WebSocketGuardRegistration>,
        timeout: Option<Duration>,
    ) -> PendingWebSocketOperation {
        pending_action_with_middlewares(event, Vec::new(), guards, timeout)
    }

    fn pending_action_with_middlewares(
        event: &'static str,
        message_middlewares: Vec<WebSocketMessageMiddlewareRegistration>,
        guards: Vec<WebSocketGuardRegistration>,
        timeout: Option<Duration>,
    ) -> PendingWebSocketOperation {
        PendingWebSocketOperation::new(
            WebSocketOperationKind::Message,
            Some(event),
            "app::tests::AppTestController::message",
            WebSocketActionRegistration::of::<AppTestController>(bind_app_test_message),
            WebSocketOperationMetadata::new(
                message_middlewares,
                guards,
                None,
                timeout,
                WebSocketAsyncApiRegistration::unspecified(),
            ),
        )
    }

    fn app_test_operation_registration() -> PendingWebSocketOperation {
        pending_action("noop", Vec::new(), None)
    }

    #[linkme::distributed_slice(crate::controller::PENDING_WEBSOCKET_OPERATION_REGISTRATIONS)]
    static APP_TEST_OPERATION_REGISTRATION:
        crate::controller::PendingWebSocketOperationRegistrationFn =
        app_test_operation_registration;

    fn app_test_shutdown_drain_registration() -> PendingWebSocketOperation {
        pending_action_with_middlewares(
            "shutdown-drain",
            vec![WebSocketMessageMiddlewareRegistration::of::<
                MessageLifecycleProbe<4>,
            >()],
            Vec::new(),
            None,
        )
    }

    #[linkme::distributed_slice(crate::controller::PENDING_WEBSOCKET_OPERATION_REGISTRATIONS)]
    static APP_TEST_SHUTDOWN_DRAIN_REGISTRATION:
        crate::controller::PendingWebSocketOperationRegistrationFn =
        app_test_shutdown_drain_registration;

    fn app_test_after_shutdown_registration() -> PendingWebSocketOperation {
        pending_action("after-shutdown", Vec::new(), None)
    }

    #[linkme::distributed_slice(crate::controller::PENDING_WEBSOCKET_OPERATION_REGISTRATIONS)]
    static APP_TEST_AFTER_SHUTDOWN_REGISTRATION:
        crate::controller::PendingWebSocketOperationRegistrationFn =
        app_test_after_shutdown_registration;

    fn app_test_long_control_pong_registration() -> PendingWebSocketOperation {
        pending_action("long-control-pong", Vec::new(), None)
    }

    #[linkme::distributed_slice(crate::controller::PENDING_WEBSOCKET_OPERATION_REGISTRATIONS)]
    static APP_TEST_LONG_CONTROL_PONG_REGISTRATION:
        crate::controller::PendingWebSocketOperationRegistrationFn =
        app_test_long_control_pong_registration;

    fn app_test_long_control_pending_registration() -> PendingWebSocketOperation {
        pending_action("long-control-pending", Vec::new(), None)
    }

    #[linkme::distributed_slice(crate::controller::PENDING_WEBSOCKET_OPERATION_REGISTRATIONS)]
    static APP_TEST_LONG_CONTROL_PENDING_REGISTRATION:
        crate::controller::PendingWebSocketOperationRegistrationFn =
        app_test_long_control_pending_registration;

    fn app_test_after_long_control_registration() -> PendingWebSocketOperation {
        pending_action("after-long-control", Vec::new(), None)
    }

    #[linkme::distributed_slice(crate::controller::PENDING_WEBSOCKET_OPERATION_REGISTRATIONS)]
    static APP_TEST_AFTER_LONG_CONTROL_REGISTRATION:
        crate::controller::PendingWebSocketOperationRegistrationFn =
        app_test_after_long_control_registration;

    struct OpenOrderController;

    #[async_trait]
    impl crate::controller::WebSocketControllerTrait for OpenOrderController {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
            Ok(Self)
        }
    }

    impl WebSocketControllerDefinition for OpenOrderController {
        fn namespace() -> &'static str {
            "open-order"
        }

        fn handshake_middleware_registrations() -> Vec<WebSocketHandshakeMiddlewareRegistration> {
            Vec::new()
        }

        fn connection_middleware_registrations() -> Vec<WebSocketConnectionMiddlewareRegistration> {
            Vec::new()
        }

        fn message_middleware_registrations() -> Vec<WebSocketMessageMiddlewareRegistration> {
            Vec::new()
        }

        fn guard_registrations() -> Vec<WebSocketGuardRegistration> {
            Vec::new()
        }

        fn timeout() -> Option<Duration> {
            None
        }

        fn asyncapi_registration() -> WebSocketAsyncApiRegistration {
            WebSocketAsyncApiRegistration::unspecified()
        }
    }

    fn open_order_controller_registration() -> WebSocketControllerRegistration {
        WebSocketControllerRegistration::of::<OpenOrderController>()
    }

    #[linkme::distributed_slice(crate::controller::WEBSOCKET_CONTROLLER_REGISTRATIONS)]
    static OPEN_ORDER_CONTROLLER_REGISTRATION:
        crate::controller::WebSocketControllerRegistrationFn = open_order_controller_registration;

    static OPEN_ORDER_CONNECTED_CALLS: AtomicUsize = AtomicUsize::new(0);
    static OPEN_ORDER_DISCONNECTED_CALLS: AtomicUsize = AtomicUsize::new(0);
    static OPEN_ORDER_EVENTS: StdMutex<Vec<&'static str>> = StdMutex::new(Vec::new());
    static OPEN_ORDER_FORCE_PENDING_DISCONNECT: AtomicBool = AtomicBool::new(false);
    static OPEN_ORDER_FORCE_DISCONNECT_DROPPED: AtomicBool = AtomicBool::new(false);
    static OPEN_ORDER_LIFECYCLE_TEST_LOCK: tokio::sync::Mutex<()> =
        tokio::sync::Mutex::const_new(());

    fn reset_dec_009_lifecycle_probes() {
        DEC009_SCOPE_INITIALIZED.store(0, Ordering::SeqCst);
        DEC009_SCOPE_RESOLVED.store(0, Ordering::SeqCst);
        DEC009_SCOPE_DISPOSED.store(0, Ordering::SeqCst);
        OPEN_ORDER_CONNECTED_CALLS.store(0, Ordering::SeqCst);
        OPEN_ORDER_DISCONNECTED_CALLS.store(0, Ordering::SeqCst);
        OPEN_ORDER_EVENTS.lock().unwrap().clear();
        OPEN_ORDER_FORCE_PENDING_DISCONNECT.store(false, Ordering::SeqCst);
        OPEN_ORDER_FORCE_DISCONNECT_DROPPED.store(false, Ordering::SeqCst);
    }

    async fn dec_009_lifecycle_app() -> WsApp {
        WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .handshake_middleware::<Dec009ScopedHandshakeProbe>()
            .connection_middleware::<OrderedOpenProbe<0>>()
            .build()
            .await
            .expect("build DEC-009 lifecycle application")
    }

    async fn hsk_04_no_handshake_pipeline_app() -> WsApp {
        WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .connection_middleware::<OrderedOpenProbe<0>>()
            .build()
            .await
            .expect("build HSK-04 no-handshake-pipeline application")
    }

    fn spawn_handshake_test_connection(
        app: &WsApp,
        stream: ExactKHandshakeStream,
        execution_cancellation: CancellationToken,
    ) -> tokio_util::task::AbortOnDropHandle<Result<(), ServerError>> {
        let permit = app
            .connection_permits
            .clone()
            .try_acquire_owned()
            .expect("handshake test connection permit");
        tokio_util::task::AbortOnDropHandle::new(tokio::spawn(WsApp::handle_connection(
            stream,
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: Uuid::new_v4(),
                permit,
                span: tracing::info_span!("test.websocket.handshake_boundary"),
            },
            None,
            CancellationToken::new(),
            CancellationToken::new(),
            execution_cancellation,
        )))
    }

    async fn wait_for_dec_009_publication(app: &WsApp) {
        timeout(Duration::from_secs(2), async {
            loop {
                let connection_count = app.active_connection_count().await;
                let connected = OPEN_ORDER_CONNECTED_CALLS.load(Ordering::SeqCst);
                let event_count = OPEN_ORDER_EVENTS.lock().unwrap().len();
                assert!(
                    connection_count <= 1,
                    "manager publication must be exact-once"
                );
                assert!(connected <= 1, "connected hook must be exact-once");
                if connection_count == 1 && connected == 1 && event_count >= 2 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("DEC-009 manager/hook publication deadline");
    }

    struct OpenOrderForceDisconnectDrop;

    struct OpenOrderForcePendingGuard;

    impl OpenOrderForcePendingGuard {
        fn enable() -> Self {
            OPEN_ORDER_FORCE_PENDING_DISCONNECT.store(true, Ordering::SeqCst);
            Self
        }
    }

    impl Drop for OpenOrderForcePendingGuard {
        fn drop(&mut self) {
            OPEN_ORDER_FORCE_PENDING_DISCONNECT.store(false, Ordering::SeqCst);
        }
    }

    impl Drop for OpenOrderForceDisconnectDrop {
        fn drop(&mut self) {
            OPEN_ORDER_FORCE_DISCONNECT_DROPPED.store(true, Ordering::SeqCst);
            OPEN_ORDER_EVENTS
                .lock()
                .unwrap()
                .push("controller.disconnected_dropped");
        }
    }

    struct OpenOrderLifecycleAction {
        _controller: Arc<OpenOrderController>,
        connected: bool,
    }

    impl WebSocketLifecycleAction for OpenOrderLifecycleAction {
        fn call(&self, _invocation: WebSocketLifecycleInvocation) -> WebSocketLifecycleFuture {
            if let Some(future) = connection_shutdown::lifecycle(self.connected, &_invocation) {
                return future;
            }
            if self.connected {
                OPEN_ORDER_CONNECTED_CALLS.fetch_add(1, Ordering::SeqCst);
                return Box::pin(async { Ok(()) });
            }
            if OPEN_ORDER_FORCE_PENDING_DISCONNECT.load(Ordering::SeqCst) {
                return Box::pin(async {
                    OPEN_ORDER_DISCONNECTED_CALLS.fetch_add(1, Ordering::SeqCst);
                    OPEN_ORDER_EVENTS
                        .lock()
                        .unwrap()
                        .push("controller.disconnected_started");
                    let _drop = OpenOrderForceDisconnectDrop;
                    std::future::pending::<()>().await;
                    Ok(())
                });
            }
            OPEN_ORDER_DISCONNECTED_CALLS.fetch_add(1, Ordering::SeqCst);
            OPEN_ORDER_EVENTS
                .lock()
                .unwrap()
                .push("controller.disconnected");
            Box::pin(async { Ok(()) })
        }
    }

    fn bind_open_order_connected(
        controller: ErasedWebSocketController,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
        let controller = downcast_websocket_controller::<OpenOrderController>(controller)?;
        Ok(BoundWebSocketOperation::Connected(Arc::new(
            OpenOrderLifecycleAction {
                _controller: controller,
                connected: true,
            },
        )))
    }

    fn bind_open_order_disconnected(
        controller: ErasedWebSocketController,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
        let controller = downcast_websocket_controller::<OpenOrderController>(controller)?;
        Ok(BoundWebSocketOperation::Disconnected(Arc::new(
            OpenOrderLifecycleAction {
                _controller: controller,
                connected: false,
            },
        )))
    }

    fn open_order_operation(
        kind: WebSocketOperationKind,
        handler_name: &'static str,
        binder: fn(
            ErasedWebSocketController,
        ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError>,
    ) -> PendingWebSocketOperation {
        PendingWebSocketOperation::new(
            kind,
            None,
            handler_name,
            WebSocketActionRegistration::of::<OpenOrderController>(binder),
            WebSocketOperationMetadata::new(
                Vec::new(),
                Vec::new(),
                None,
                None,
                WebSocketAsyncApiRegistration::unspecified(),
            ),
        )
    }

    fn open_order_connected_registration() -> PendingWebSocketOperation {
        open_order_operation(
            WebSocketOperationKind::Connected,
            "app::tests::OpenOrderController::connected",
            bind_open_order_connected,
        )
    }

    fn open_order_disconnected_registration() -> PendingWebSocketOperation {
        open_order_operation(
            WebSocketOperationKind::Disconnected,
            "app::tests::OpenOrderController::disconnected",
            bind_open_order_disconnected,
        )
    }

    #[linkme::distributed_slice(crate::controller::PENDING_WEBSOCKET_OPERATION_REGISTRATIONS)]
    static OPEN_ORDER_CONNECTED_REGISTRATION:
        crate::controller::PendingWebSocketOperationRegistrationFn =
        open_order_connected_registration;

    #[linkme::distributed_slice(crate::controller::PENDING_WEBSOCKET_OPERATION_REGISTRATIONS)]
    static OPEN_ORDER_DISCONNECTED_REGISTRATION:
        crate::controller::PendingWebSocketOperationRegistrationFn =
        open_order_disconnected_registration;

    #[derive(Clone, Copy)]
    enum AppTestLifecycleBehavior {
        ConnectFailure,
        ConnectPanic,
        ConnectPending,
        DisconnectFailure,
        DisconnectSafe,
        DisconnectPanic,
    }

    static DISCONNECT_FAILURE_CALLS: AtomicUsize = AtomicUsize::new(0);
    static DISCONNECT_SAFE_CALLS: AtomicUsize = AtomicUsize::new(0);

    struct AppTestLifecycleAction {
        _controller: Arc<AppTestController>,
        behavior: AppTestLifecycleBehavior,
    }

    impl WebSocketLifecycleAction for AppTestLifecycleAction {
        fn call(&self, _invocation: WebSocketLifecycleInvocation) -> WebSocketLifecycleFuture {
            match self.behavior {
                AppTestLifecycleBehavior::ConnectFailure => {
                    Box::pin(async { Err(crate::controller::WebSocketLifecycleError::Internal) })
                }
                AppTestLifecycleBehavior::ConnectPanic
                | AppTestLifecycleBehavior::DisconnectPanic => {
                    panic!("synchronous test lifecycle panic")
                }
                AppTestLifecycleBehavior::ConnectPending => Box::pin(std::future::pending()),
                AppTestLifecycleBehavior::DisconnectFailure => Box::pin(async {
                    DISCONNECT_FAILURE_CALLS.fetch_add(1, Ordering::SeqCst);
                    Err(crate::controller::WebSocketLifecycleError::Internal)
                }),
                AppTestLifecycleBehavior::DisconnectSafe => Box::pin(async {
                    DISCONNECT_SAFE_CALLS.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }),
            }
        }
    }

    fn bind_lifecycle(
        controller: ErasedWebSocketController,
        kind: WebSocketOperationKind,
        behavior: AppTestLifecycleBehavior,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
        let controller = downcast_websocket_controller::<AppTestController>(controller)?;
        let handler = Arc::new(AppTestLifecycleAction {
            _controller: controller,
            behavior,
        });
        Ok(match kind {
            WebSocketOperationKind::Connected => BoundWebSocketOperation::Connected(handler),
            WebSocketOperationKind::Disconnected => BoundWebSocketOperation::Disconnected(handler),
            WebSocketOperationKind::Message => unreachable!("lifecycle binder kind"),
        })
    }

    fn bind_connect_failure(
        controller: ErasedWebSocketController,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
        bind_lifecycle(
            controller,
            WebSocketOperationKind::Connected,
            AppTestLifecycleBehavior::ConnectFailure,
        )
    }

    fn bind_connect_panic(
        controller: ErasedWebSocketController,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
        bind_lifecycle(
            controller,
            WebSocketOperationKind::Connected,
            AppTestLifecycleBehavior::ConnectPanic,
        )
    }

    fn bind_connect_pending(
        controller: ErasedWebSocketController,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
        bind_lifecycle(
            controller,
            WebSocketOperationKind::Connected,
            AppTestLifecycleBehavior::ConnectPending,
        )
    }

    fn bind_disconnect_failure(
        controller: ErasedWebSocketController,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
        bind_lifecycle(
            controller,
            WebSocketOperationKind::Disconnected,
            AppTestLifecycleBehavior::DisconnectFailure,
        )
    }

    fn bind_disconnect_safe(
        controller: ErasedWebSocketController,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
        bind_lifecycle(
            controller,
            WebSocketOperationKind::Disconnected,
            AppTestLifecycleBehavior::DisconnectSafe,
        )
    }

    fn pending_lifecycle(
        kind: WebSocketOperationKind,
        binder: fn(
            ErasedWebSocketController,
        ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError>,
    ) -> PendingWebSocketOperation {
        pending_lifecycle_with_timeout(kind, binder, None)
    }

    fn pending_lifecycle_with_timeout(
        kind: WebSocketOperationKind,
        binder: fn(
            ErasedWebSocketController,
        ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError>,
        timeout: Option<Duration>,
    ) -> PendingWebSocketOperation {
        PendingWebSocketOperation::new(
            kind,
            None,
            "app::tests::AppTestController::lifecycle",
            WebSocketActionRegistration::of::<AppTestController>(binder),
            WebSocketOperationMetadata::new(
                Vec::new(),
                Vec::new(),
                None,
                timeout,
                WebSocketAsyncApiRegistration::unspecified(),
            ),
        )
    }

    async fn test_action_table(
        operations: Vec<PendingWebSocketOperation>,
        extensions: Arc<Extensions>,
    ) -> Arc<WebSocketActionTable> {
        Arc::new(
            materialize_websocket_controllers(
                vec![WebSocketControllerRegistration::of::<AppTestController>()],
                operations,
                extensions,
            )
            .await
            .expect("test controller operations materialize"),
        )
    }

    #[tokio::test]
    async fn invalid_middleware_timeout_is_visible_before_listener_publication() {
        let config = ServerConfig {
            connection_middleware_timeout_secs: 0,
            ..ServerConfig::default()
        };

        let error = WsAppBuilder::new("127.0.0.1:0")
            .config(config)
            .build()
            .await
            .err()
            .expect("zero middleware timeout must fail application construction");

        assert!(matches!(
            error,
            ServerError::EffectiveConfiguration(WsEffectiveConfigError::InvalidRuntime(_))
        ));
    }

    #[tokio::test]
    async fn hsk_08_dot_segment_origins_fail_during_application_build() {
        timeout(Duration::from_secs(10), async {
            for origin in [
                "https://allowed.example/.",
                "https://allowed.example/path/..",
            ] {
                let result = WsAppBuilder::new("127.0.0.1:0")
                    .config(ServerConfig {
                        allowed_origins: vec![origin.to_owned()],
                        ..ServerConfig::default()
                    })
                    .build()
                    .await;

                match result {
                    Err(error) => assert!(
                        matches!(
                            error,
                            ServerError::EffectiveConfiguration(
                                WsEffectiveConfigError::InvalidRuntime(_)
                            )
                        ),
                        "dot-segment Origin returned the wrong build error: {error}"
                    ),
                    Ok(app) => {
                        app.close()
                            .await
                            .expect("close unexpectedly built HSK-08 application");
                        panic!("dot-segment Origin unexpectedly built: {origin}");
                    }
                }
            }
        })
        .await
        .expect("HSK-08 application build regression exceeded its bounded deadline");
    }

    fn reserve_loopback_address() -> std::net::SocketAddr {
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a loopback test address");
        let address = listener.local_addr().expect("read loopback test address");
        drop(listener);
        address
    }

    async fn connect_loopback_when_ready(address: std::net::SocketAddr) -> tokio::net::TcpStream {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        loop {
            match tokio::net::TcpStream::connect(address).await {
                Ok(stream) => return stream,
                Err(error) if tokio::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    let _ = error;
                }
                Err(error) => panic!("loopback listener did not become ready: {error}"),
            }
        }
    }

    async fn wait_for_accepting_health(app: &WsApp) -> HealthSnapshot {
        timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = app.health_snapshot().expect("health snapshot");
                if snapshot.accepting_new_work {
                    return snapshot;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("WebSocket listener readiness deadline")
    }

    #[test]
    fn connection_middleware_cancellation_preserves_server_shutdown_category() {
        let (category, reason) =
            connection_middleware_terminal(crate::middleware::WsMiddlewareFailureKind::Cancelled);
        assert_eq!(
            category,
            crate::middleware::WsConnectionCloseCategory::ServerShutdown
        );
        assert_eq!(reason, crate::request::WsCloseReason::ServerShutdown);
    }

    fn message_runtime(manager: &Arc<ConnectionManager>) -> MessageRuntime {
        MessageRuntime {
            message_timeout: Duration::from_secs(1),
            cleanup_timeout: Duration::from_secs(1),
            cancellation: CancellationToken::new(),
            metrics: Arc::clone(manager.metrics()),
        }
    }

    fn test_request(connection_id: Uuid, event: &str) -> WsRequest {
        let namespace = event
            .split_once(':')
            .expect("test event is a canonical route")
            .0;
        let frame = crate::request::WsMessageBody::try_new(event, serde_json::Value::Null)
            .unwrap()
            .with_namespace(namespace.to_owned())
            .to_message()
            .unwrap();
        WsRequest::new_from_message(
            connection_id,
            frame,
            WsHeaders::default(),
            lily_web_core::RequestConnectionInfo::default(),
            WsTransportSecurity::Plaintext,
        )
        .unwrap()
    }

    fn decode_test_request(request: &WsRequest) -> DecodedWebSocketMessage {
        let kind = match request.wire_format() {
            crate::request::WsWireFormat::Text => crate::codec::WebSocketFrameKind::Text,
            crate::request::WsWireFormat::Binary => crate::codec::WebSocketFrameKind::Binary,
        };
        let raw = RawEnvelope::try_new(kind, request.original_frame().to_vec(), usize::MAX)
            .expect("test request carries one bounded application frame");
        LilyEnvelopeCodec
            .decode_frame(raw)
            .expect("test request carries a strict Lily v2 envelope")
    }

    async fn route_test_message(
        table: Arc<WebSocketActionTable>,
        extensions: Arc<Extensions>,
        context: Arc<WebSocketContext>,
        request: Arc<WsRequest>,
        runtime: MessageRuntime,
    ) -> WsMessageOutcome {
        let connection_id = request.connection_id();
        let decoded = decode_test_request(&request);
        let manager = Arc::clone(context.connection_manager());
        let namespace = context.namespace().to_owned();
        let dispatch =
            WsApp::route_decoded_message(table, extensions, context, request, decoded, runtime)
                .await;
        let outcome = dispatch.outcome;
        let _ = WsApp::materialize_message_terminal(
            manager.as_ref(),
            &namespace,
            connection_id,
            dispatch.terminal,
            &crate::shutdown::ShutdownBudget::default(),
            dispatch.output,
        )
        .await;
        outcome
    }

    fn typed_response_context(ack_id: Option<&str>) -> WebSocketActionResponseContext {
        WebSocketActionResponseContext {
            namespace: "orders".into(),
            event: "write".into(),
            ack_id: ack_id.map(str::to_owned),
            frame_kind: crate::codec::WebSocketFrameKind::Text,
        }
    }

    fn application_frame(terminal: Option<PreparedWebSocketTerminal>) -> Message {
        match terminal {
            Some(PreparedWebSocketTerminal::ApplicationFrame(frame)) => frame,
            other => panic!("expected one application frame, got {other:?}"),
        }
    }

    fn outbound_wire_body(
        terminal: Option<PreparedWebSocketTerminal>,
    ) -> crate::request::WsMessageBody {
        let body: crate::request::WsMessageBody = match application_frame(terminal) {
            Message::Text(text) => serde_json::from_str(&text),
            Message::Binary(bytes) => serde_json::from_slice(&bytes),
            other => panic!("expected an outbound application frame, got {other:?}"),
        }
        .expect("typed terminal must encode one Lily v2 envelope");
        body.validate_wire()
            .expect("typed terminal must satisfy the Lily v2 wire contract");
        body
    }

    #[tokio::test]
    async fn custom_codec_controller_terminal_cannot_bypass_the_outbound_message_limit() {
        const LIMIT: usize = 8;

        let response = typed_response_context(Some("ack-size"));

        for binary in [false, true] {
            let manager = Arc::new(
                ConnectionManager::with_registered_namespaces_and_identity_and_outbound_limit(
                    8,
                    128,
                    ["orders".to_owned()],
                    false,
                    LIMIT,
                ),
            );
            let connection_id = Uuid::new_v4();
            let (sender, mut receiver) = mpsc::channel(4);
            let (control_sender, mut control_receiver) = connection_control_channel();
            manager
                .add_connection_with_control(
                    connection_id,
                    sender,
                    control_sender,
                    lily_web_core::RequestConnectionInfo::default(),
                    WsTransportSecurity::Plaintext,
                    Some("orders".to_owned()),
                )
                .await
                .unwrap();
            let context =
                WebSocketContext::new(connection_id, Arc::clone(&manager), "orders".to_owned());

            let exact = if binary {
                Message::Binary(vec![b'x'; LIMIT])
            } else {
                Message::Text("x".repeat(LIMIT))
            };
            let exact_terminal = WsApp::prepare_action_outcome(
                PendingWebSocketActionOutcome::Emit {
                    event: "orders:changed".into(),
                    payload: DecodedWebSocketPayload::Json(serde_json::json!({"ok": true})),
                },
                &response,
                &LilyEnvelopeCodec,
                &FixedApplicationFrameEncoder(exact),
            )
            .expect("fixed codec output is a valid application frame");
            WsApp::materialize_message_terminal(
                manager.as_ref(),
                "orders",
                connection_id,
                exact_terminal,
                &crate::shutdown::ShutdownBudget::default(),
                None,
            )
            .await
            .unwrap();
            let admitted = timeout(Duration::from_secs(1), receiver.recv())
                .await
                .expect("exact codec output must be admitted within the test deadline")
                .expect("application data channel remains open");
            assert_eq!(admitted.len(), LIMIT);
            assert_eq!(matches!(admitted, Message::Binary(_)), binary);

            let oversized = if binary {
                Message::Binary(vec![b'x'; LIMIT + 1])
            } else {
                Message::Text("x".repeat(LIMIT + 1))
            };
            let oversized_terminal = WsApp::prepare_action_outcome(
                PendingWebSocketActionOutcome::Emit {
                    event: "orders:changed".into(),
                    payload: DecodedWebSocketPayload::Json(serde_json::json!({"ok": true})),
                },
                &response,
                &LilyEnvelopeCodec,
                &FixedApplicationFrameEncoder(oversized),
            )
            .expect("size authority belongs to the terminal sink");
            let error = WsApp::materialize_message_terminal(
                manager.as_ref(),
                "orders",
                connection_id,
                oversized_terminal,
                &crate::shutdown::ShutdownBudget::default(),
                None,
            )
            .await
            .unwrap_err();
            assert!(matches!(
                error,
                crate::connection::ConnectionError::InvalidOperation(
                    crate::connection::ConnectionOperationError::OutboundMessageTooLarge
                )
            ));
            assert!(matches!(
                receiver.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));

            // This is the same fail-closed branch used by the live message
            // loop after terminal materialization fails.
            WsApp::request_middleware_close(
                &context,
                crate::request::WsCloseReason::InternalFailure,
                crate::middleware::WsConnectionCloseCategory::HandlerError,
            )
            .await;
            let request = timeout(Duration::from_secs(1), control_receiver.next())
                .await
                .expect("internal Close must be selected within the test deadline")
                .expect("control channel remains open");
            let ConnectionControlFrame::Close(request) = request else {
                panic!("internal failure must select the Close control slot");
            };
            assert_eq!(
                request.category(),
                crate::middleware::WsConnectionCloseCategory::HandlerError
            );
            assert!(matches!(
                request.message(),
                Message::Close(Some(frame))
                    if u16::from(frame.code) == 1011
                        && frame.reason == "lily.v2.internal_failure"
            ));
            assert!(
                timeout(Duration::from_millis(20), control_receiver.next())
                    .await
                    .is_err(),
                "oversized codec output must select exactly one internal close"
            );
        }
    }

    #[test]
    fn typed_terminal_table_is_exact_and_ack_authority_is_not_invented() {
        let codec = LilyEnvelopeCodec;
        let with_ack = typed_response_context(Some("ack-42"));

        assert!(
            WsApp::prepare_action_outcome(
                PendingWebSocketActionOutcome::NoReply,
                &with_ack,
                &codec,
                &codec,
            )
            .unwrap()
            .is_none()
        );

        let ack = WsApp::prepare_action_outcome(
            PendingWebSocketActionOutcome::Ack(DecodedWebSocketPayload::Json(
                serde_json::json!({"accepted": true}),
            )),
            &with_ack,
            &codec,
            &codec,
        )
        .unwrap();
        let ack = outbound_wire_body(ack);
        assert_eq!(ack.msg_type, crate::request::WsMessageKind::Ack);
        assert_eq!(ack.event(), "orders:write");
        assert_eq!(ack.ack_id(), Some("ack-42"));

        let missing_ack = WsApp::prepare_action_outcome(
            PendingWebSocketActionOutcome::Ack(DecodedWebSocketPayload::Json(
                serde_json::Value::Null,
            )),
            &typed_response_context(None),
            &codec,
            &codec,
        )
        .unwrap_err();
        assert_eq!(
            missing_ack.action_error().code(),
            WebSocketErrorCode::MISSING_ACK_AUTHORITY
        );

        let emitted = WsApp::prepare_action_outcome(
            PendingWebSocketActionOutcome::Emit {
                event: "orders:changed".into(),
                payload: DecodedWebSocketPayload::Text("ready".into()),
            },
            &with_ack,
            &codec,
            &codec,
        )
        .unwrap();
        let emitted = outbound_wire_body(emitted);
        assert_eq!(emitted.msg_type, crate::request::WsMessageKind::Event);
        assert_eq!(emitted.event(), "orders:changed");
        assert_eq!(emitted.text_payload().unwrap(), "ready");

        let close = crate::CloseConnection::try_new(4001, "application complete").unwrap();
        let terminal = WsApp::prepare_action_outcome(
            PendingWebSocketActionOutcome::Close(close),
            &with_ack,
            &codec,
            &codec,
        )
        .unwrap();
        assert!(matches!(
            terminal,
            Some(PreparedWebSocketTerminal::Close {
                frame: CloseFrame { code, .. },
                category: crate::middleware::WsConnectionCloseCategory::Application,
            }) if u16::from(code) == 4001
        ));
    }

    #[test]
    fn custom_output_codec_failures_close_while_typed_validation_stays_recoverable() {
        let context = typed_response_context(Some("ack-output"));
        let lily_codec = LilyEnvelopeCodec;
        let payload_error = ErroringPayloadEncoder;
        let frame_error = ErroringFrameEncoder;
        let control_frame = ControlFrameEncoder;
        let output_failures: [(&dyn WebSocketPayloadCodec, &dyn WebSocketFrameCodec); 3] = [
            (&payload_error, &lily_codec),
            (&lily_codec, &frame_error),
            (&lily_codec, &control_frame),
        ];

        for (payload_codec, frame_codec) in output_failures {
            let failure = WsApp::prepare_action_outcome_caught(
                PendingWebSocketActionOutcome::Emit {
                    event: "orders:changed".into(),
                    payload: DecodedWebSocketPayload::Json(serde_json::json!({"ok": true})),
                },
                &context,
                payload_codec,
                frame_codec,
            )
            .expect("codec Err and output revalidation must not unwind")
            .expect_err("the output must not be publishable");
            assert!(matches!(
                &failure,
                ActionOutcomePreparationError::InternalOutput(error)
                    if error.code() == WebSocketErrorCode::OUTPUT_FAILED
            ));

            let dispatch = WsApp::prepare_action_outcome_failure(
                failure,
                &context,
                payload_codec,
                frame_codec,
            );
            assert_eq!(
                dispatch.outcome,
                WsMessageOutcome::Close(crate::request::WsCloseReason::InternalFailure)
            );
            assert!(matches!(
                dispatch.terminal,
                Some(PreparedWebSocketTerminal::Close {
                    frame: CloseFrame { code, ref reason },
                    category: crate::middleware::WsConnectionCloseCategory::HandlerError,
                }) if u16::from(code) == 1011 && reason == "lily.v2.internal_failure"
            ));
        }

        let recoverable = WsApp::prepare_action_outcome_caught(
            PendingWebSocketActionOutcome::Emit {
                event: "other:not-authoritative".into(),
                payload: DecodedWebSocketPayload::Json(serde_json::Value::Null),
            },
            &context,
            &lily_codec,
            &lily_codec,
        )
        .expect("typed validation must not unwind")
        .expect_err("cross-namespace emit must be rejected");
        assert!(matches!(
            &recoverable,
            ActionOutcomePreparationError::Recoverable(error)
                if error.code() == WebSocketErrorCode::OUTPUT_FAILED
        ));

        let dispatch =
            WsApp::prepare_action_outcome_failure(recoverable, &context, &lily_codec, &lily_codec);
        assert_eq!(
            dispatch.outcome,
            WsMessageOutcome::Rejected(crate::request::WsProtocolErrorCode::HandlerFailed)
        );
        let error = outbound_wire_body(dispatch.terminal);
        assert_eq!(error.msg_type, crate::request::WsMessageKind::Error);
        assert_eq!(
            error.data()["code"],
            WebSocketErrorCode::OUTPUT_FAILED.as_str()
        );
    }

    #[test]
    fn guard_error_ack_and_close_keep_policy_terminal_classification() {
        let codec = LilyEnvelopeCodec;
        let response = typed_response_context(Some("ack-guard"));
        let code = WebSocketErrorCode::new("TEST_GUARD_POLICY").unwrap();

        let error = WsApp::prepare_guard_rejection(
            WebSocketGuardRejection::error(code, "Policy denied the message.").unwrap(),
            &response,
            &codec,
            &codec,
        );
        assert_eq!(
            error.outcome,
            WsMessageOutcome::Rejected(crate::request::WsProtocolErrorCode::AuthorizationDenied)
        );
        let error = outbound_wire_body(error.terminal);
        assert_eq!(error.msg_type, crate::request::WsMessageKind::Error);
        assert_eq!(error.data()["code"], "TEST_GUARD_POLICY");

        let ack = WsApp::prepare_guard_rejection(
            WebSocketGuardRejection::ack(code, "Policy denied the message.").unwrap(),
            &response,
            &codec,
            &codec,
        );
        assert_eq!(
            ack.outcome,
            WsMessageOutcome::Rejected(crate::request::WsProtocolErrorCode::AuthorizationDenied)
        );
        let ack = outbound_wire_body(ack.terminal);
        assert_eq!(ack.msg_type, crate::request::WsMessageKind::Ack);
        assert_eq!(ack.ack_id(), Some("ack-guard"));
        assert_eq!(ack.data()["code"], "TEST_GUARD_POLICY");

        let missing_ack = WsApp::prepare_guard_rejection(
            WebSocketGuardRejection::ack(code, "Policy denied the message.").unwrap(),
            &typed_response_context(None),
            &codec,
            &codec,
        );
        assert_eq!(
            missing_ack.outcome,
            WsMessageOutcome::Rejected(crate::request::WsProtocolErrorCode::AuthorizationDenied)
        );
        let missing_ack = outbound_wire_body(missing_ack.terminal);
        assert_eq!(missing_ack.msg_type, crate::request::WsMessageKind::Error);
        assert_eq!(
            missing_ack.data()["code"],
            WebSocketErrorCode::MISSING_ACK_AUTHORITY.as_str()
        );

        let close = WsApp::prepare_guard_rejection(
            WebSocketGuardRejection::close(
                code,
                "Policy denied the message.",
                crate::CloseConnection::policy("Policy denied the message.").unwrap(),
            )
            .unwrap(),
            &response,
            &codec,
            &codec,
        );
        assert_eq!(
            close.outcome,
            WsMessageOutcome::Close(crate::request::WsCloseReason::PolicyViolation)
        );
        assert!(matches!(
            close.terminal,
            Some(PreparedWebSocketTerminal::Close {
                category: crate::middleware::WsConnectionCloseCategory::PolicyRejected,
                ..
            })
        ));
    }

    #[test]
    fn custom_codec_panics_are_bounded_and_protocol_errors_use_selected_frame_codec() {
        let frame = crate::request::WsMessageBody::try_new("orders:write", serde_json::Value::Null)
            .unwrap()
            .with_namespace("orders".into())
            .to_message()
            .unwrap();
        let error = decode_inbound_application_frame(&PanickingFrameDecoder, frame, 64 * 1024)
            .expect_err("decoder panic becomes one typed codec failure");
        assert_eq!(error.kind(), WebSocketCodecFailureKind::Internal);

        CUSTOM_PROTOCOL_ENCODINGS.store(0, Ordering::SeqCst);
        let dispatch = WsApp::prepare_protocol_rejection(
            crate::request::WsProtocolErrorCode::MiddlewareRejected,
            crate::codec::WebSocketFrameKind::Text,
            &LilyEnvelopeCodec,
            &RecordingFrameCodec,
        );
        assert_eq!(
            dispatch.outcome,
            WsMessageOutcome::Rejected(crate::request::WsProtocolErrorCode::MiddlewareRejected)
        );
        assert_eq!(CUSTOM_PROTOCOL_ENCODINGS.load(Ordering::SeqCst), 1);
        let error = outbound_wire_body(dispatch.terminal);
        assert_eq!(error.msg_type, crate::request::WsMessageKind::Error);
        assert_eq!(error.data()["code"], "ws.middleware_rejected");
    }

    #[test]
    fn typed_action_error_uses_safe_selected_codec_frame() {
        let codec = LilyEnvelopeCodec;
        let code = WebSocketErrorCode::new("ORDER_REJECTED").unwrap();
        let error = WebSocketActionError::rejected(code, "Order was rejected.").unwrap();
        let dispatch = WsApp::prepare_action_error(
            error,
            &typed_response_context(Some("ack-7")),
            &codec,
            &codec,
        );
        assert_eq!(
            dispatch.outcome,
            WsMessageOutcome::Rejected(crate::request::WsProtocolErrorCode::HandlerFailed)
        );
        let error = outbound_wire_body(dispatch.terminal);
        assert_eq!(error.msg_type, crate::request::WsMessageKind::Error);
        assert_eq!(error.data()["code"], "ORDER_REJECTED");
        assert_eq!(error.data()["message"], "Order was rejected.");
    }

    #[tokio::test]
    async fn typed_close_remains_the_exact_terminal_when_reverse_middleware_also_closes() {
        let connection_id = Uuid::new_v4();
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            128,
            128,
            ["orders".into()],
        ));
        let (sender, mut receiver) = mpsc::channel(2);
        manager
            .add_connection(connection_id, sender, None, Some("orders".into()))
            .await
            .unwrap();
        let container = ApplicationContainer::build().await.unwrap();
        let table = test_action_table(
            vec![pending_action_with_middlewares(
                "close",
                vec![WebSocketMessageMiddlewareRegistration::of::<
                    MessageLifecycleProbe<5>,
                >()],
                Vec::new(),
                None,
            )],
            container.services(),
        )
        .await;

        let outcome = route_test_message(
            table,
            container.services(),
            Arc::new(WebSocketContext::new(
                connection_id,
                Arc::clone(&manager),
                "orders".into(),
            )),
            Arc::new(test_request(connection_id, "orders:close")),
            message_runtime(&manager),
        )
        .await;

        assert_eq!(
            outcome,
            WsMessageOutcome::Close(crate::request::WsCloseReason::Application)
        );
        let Some(Message::Close(Some(frame))) = receiver.recv().await else {
            panic!("typed close must queue one close frame");
        };
        assert_eq!(u16::from(frame.code), 4001);
        assert_eq!(frame.reason, "application complete");
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));

        manager.remove_connection(connection_id).await.unwrap();
        container.close().await.unwrap();
    }

    fn test_tls_configs() -> (RustlsConfig, Arc<ClientConfig>) {
        let certified = generate_simple_self_signed(["localhost".to_owned()])
            .expect("generate test TLS identity");
        let certificate = certified.cert.der().clone();
        let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            certified.signing_key.serialize_der(),
        ));
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut server = RustlsServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .expect("server protocol versions")
            .with_no_client_auth()
            .with_single_cert(vec![certificate.clone()], private_key)
            .expect("server identity");
        // The adapter must replace a caller-supplied protocol list because
        // RFC 6455 is an HTTP/1.1 Upgrade.
        server.alpn_protocols = vec![b"h2".to_vec()];

        let mut roots = RootCertStore::empty();
        roots.add(certificate).expect("client trust anchor");
        let mut client = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .expect("client protocol versions")
            .with_root_certificates(roots)
            .with_no_client_auth();
        client.alpn_protocols = vec![b"http/1.1".to_vec()];
        (RustlsConfig::from(server), Arc::new(client))
    }

    fn test_mtls_configs() -> (RustlsConfig, Arc<ClientConfig>, Arc<ClientConfig>) {
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("CA parameters");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::DigitalSignature,
            KeyUsagePurpose::CrlSign,
        ];
        let ca =
            CertifiedIssuer::self_signed(ca_params, KeyPair::generate().expect("generate CA key"))
                .expect("generate CA");

        let server_key = KeyPair::generate().expect("generate server key");
        let mut server_params =
            CertificateParams::new(["localhost".to_owned()]).expect("server parameters");
        server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        let server_certificate = server_params
            .signed_by(&server_key, &ca)
            .expect("sign server certificate");

        let client_key = KeyPair::generate().expect("generate client key");
        let mut client_params = CertificateParams::new(["lily-websocket-client".to_owned()])
            .expect("client parameters");
        client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_certificate = client_params
            .signed_by(&client_key, &ca)
            .expect("sign client certificate");

        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut client_roots = RootCertStore::empty();
        client_roots.add(ca.der().clone()).expect("client root");
        let mut authenticated_client = ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .expect("client protocol versions")
            .with_root_certificates(client_roots.clone())
            .with_client_auth_cert(
                vec![client_certificate.der().clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(client_key.serialize_der())),
            )
            .expect("client identity");
        authenticated_client.alpn_protocols = vec![b"http/1.1".to_vec()];
        let mut anonymous_client = ClientConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .expect("client protocol versions")
            .with_root_certificates(client_roots)
            .with_no_client_auth();
        anonymous_client.alpn_protocols = vec![b"http/1.1".to_vec()];

        let mut server_roots = RootCertStore::empty();
        server_roots.add(ca.der().clone()).expect("server root");
        let verifier =
            WebPkiClientVerifier::builder_with_provider(Arc::new(server_roots), provider)
                .build()
                .expect("required client verifier");
        let server = RustlsServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("server protocol versions")
        .with_client_cert_verifier(verifier)
        .with_single_cert(
            vec![server_certificate.der().clone()],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(server_key.serialize_der())),
        )
        .expect("server identity");

        (
            RustlsConfig::from(server),
            Arc::new(authenticated_client),
            Arc::new(anonymous_client),
        )
    }

    static TRUSTED_GUARD_OBSERVED: AtomicBool = AtomicBool::new(false);

    struct ContextGuard;

    #[async_trait]
    impl WsGuard for ContextGuard {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, GuardInitializationError> {
            Ok(Self)
        }

        async fn can_activate(
            &self,
            exchange: &mut WsMessageExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), WebSocketGuardRejection> {
            let context = exchange.connection();
            let request = exchange.request();
            let trusted = context.principal().is_some_and(|principal| {
                principal.subject() == "subject-1" && principal.has_scope("orders:write")
            }) && request.principal().is_some_and(|principal| {
                principal.subject() == "subject-1" && principal.has_role("operator")
            }) && context.subprotocol()
                == Some(crate::request::LILY_WEBSOCKET_SUBPROTOCOL)
                && request.negotiated_subprotocol()
                    == Some(crate::request::LILY_WEBSOCKET_SUBPROTOCOL);
            TRUSTED_GUARD_OBSERVED.store(trusted, Ordering::SeqCst);
            if trusted {
                Ok(())
            } else {
                Err(WebSocketGuardRejection::error(
                    WebSocketErrorCode::new("TEST_CONTEXT_DENIED").unwrap(),
                    "The trusted context is unavailable.",
                )
                .unwrap())
            }
        }
    }

    struct PendingGuard;

    #[async_trait]
    impl WsGuard for PendingGuard {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, GuardInitializationError> {
            Ok(Self)
        }

        async fn can_activate(
            &self,
            _exchange: &mut WsMessageExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), WebSocketGuardRejection> {
            std::future::pending::<Result<(), WebSocketGuardRejection>>().await
        }
    }

    struct DenyGuard;

    #[async_trait]
    impl WsGuard for DenyGuard {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, GuardInitializationError> {
            Ok(Self)
        }

        async fn can_activate(
            &self,
            _exchange: &mut WsMessageExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), WebSocketGuardRejection> {
            Err(WebSocketGuardRejection::error(
                WebSocketErrorCode::new("TEST_GUARD_DENIED").unwrap(),
                "The message is not allowed.",
            )
            .unwrap())
        }
    }

    struct MissingGuard;

    #[async_trait]
    impl WsGuard for MissingGuard {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, GuardInitializationError> {
            Err(GuardInitializationError::MissingDependency)
        }

        async fn can_activate(
            &self,
            _exchange: &mut WsMessageExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), WebSocketGuardRejection> {
            Ok(())
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct PipelineMessageLocal(u8);

    static MESSAGE_LOCAL_GUARD_OBSERVED: AtomicBool = AtomicBool::new(false);
    static MESSAGE_LOCAL_AFTER_OBSERVED: AtomicBool = AtomicBool::new(false);

    struct MessageLocalGuard;

    #[async_trait]
    impl WsGuard for MessageLocalGuard {
        async fn new(_extensions: Arc<Extensions>) -> Result<Self, GuardInitializationError> {
            Ok(Self)
        }

        async fn can_activate(
            &self,
            exchange: &mut WsMessageExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<(), WebSocketGuardRejection> {
            let observed =
                exchange.message_local::<PipelineMessageLocal>() == Some(PipelineMessageLocal(1));
            MESSAGE_LOCAL_GUARD_OBSERVED.store(observed, Ordering::SeqCst);
            exchange
                .insert_message_local(PipelineMessageLocal(2))
                .expect("replacement does not consume another local slot");
            Ok(())
        }
    }

    struct MessageLocalMiddleware;

    #[async_trait]
    impl WsMessageMiddleware for MessageLocalMiddleware {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::middleware::WsMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("test_message_local", MiddlewareKind::WebSocketMessage)
        }

        async fn before_message(
            &self,
            exchange: &mut WsMessageExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<WsMessageDecision, crate::middleware::WsMiddlewareError> {
            exchange
                .insert_message_local(PipelineMessageLocal(1))
                .expect("test dispatch has local-state capacity");
            Ok(WsMessageDecision::Continue)
        }

        async fn after_message(
            &self,
            exchange: &mut WsMessageExchange,
            _outcome: WsMessageOutcome,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<WsMessageDecision, crate::middleware::WsMiddlewareError> {
            MESSAGE_LOCAL_AFTER_OBSERVED.store(
                exchange.message_local::<PipelineMessageLocal>() == Some(PipelineMessageLocal(2)),
                Ordering::SeqCst,
            );
            Ok(WsMessageDecision::Continue)
        }
    }

    static CONTEXT_MIDDLEWARE_OBSERVED: AtomicBool = AtomicBool::new(false);

    struct ContextMiddleware;

    #[async_trait]
    impl WsMessageMiddleware for ContextMiddleware {
        async fn new(
            _extensions: Arc<Extensions>,
        ) -> Result<Self, crate::middleware::WsMiddlewareInitError> {
            Ok(Self)
        }

        fn descriptor(&self) -> MiddlewareDescriptor {
            MiddlewareDescriptor::new("test_context_middleware", MiddlewareKind::WebSocketMessage)
        }

        async fn before_message(
            &self,
            exchange: &mut WsMessageExchange,
            _cancellation: crate::ExecutionCancellation,
        ) -> Result<WsMessageDecision, crate::middleware::WsMiddlewareError> {
            let context = exchange.connection();
            let request = exchange.request();
            let same_context = match (context.handshake(), request.handshake()) {
                (Some(context), Some(request)) => std::ptr::eq(context, request),
                _ => false,
            };
            CONTEXT_MIDDLEWARE_OBSERVED.store(same_context, Ordering::SeqCst);
            if same_context {
                Ok(WsMessageDecision::Continue)
            } else {
                Ok(WsMessageDecision::Reject(
                    crate::request::WsProtocolErrorCode::MiddlewareRejected,
                ))
            }
        }
    }

    #[tokio::test]
    async fn controller_table_rejects_prefixed_events_and_duplicate_routes() {
        let container = ApplicationContainer::build().await.unwrap();
        let prefixed = materialize_websocket_controllers(
            vec![WebSocketControllerRegistration::of::<AppTestController>()],
            vec![pending_action("orders:created", Vec::new(), None)],
            container.services(),
        )
        .await;
        assert!(matches!(
            prefixed,
            Err(crate::controller::WebSocketControllerMaterializationError::InvalidEvent {
                event,
                ..
            }) if event == "orders:created"
        ));

        let duplicate = materialize_websocket_controllers(
            vec![WebSocketControllerRegistration::of::<AppTestController>()],
            vec![
                pending_action("created", Vec::new(), None),
                pending_action("created", Vec::new(), None),
            ],
            container.services(),
        )
        .await;
        assert!(matches!(
            duplicate,
            Err(crate::controller::WebSocketControllerMaterializationError::DuplicateOperation {
                namespace,
                event,
            }) if namespace == "orders" && event == "created"
        ));
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn message_routing_rejects_unknown_and_mismatched_namespaces() {
        let container = ApplicationContainer::build().await.unwrap();
        let table = test_action_table(
            vec![pending_action("write", Vec::new(), None)],
            container.services(),
        )
        .await;
        assert!(!table.contains_namespace("/"));
        assert!(table.contains_namespace("orders"));
        assert!(!table.contains_namespace("missing"));

        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            128,
            128,
            ["orders".into()],
        ));
        let connection_id = Uuid::new_v4();
        for (namespace, event, expected) in [
            (
                "orders",
                "missing:write",
                crate::request::WsProtocolErrorCode::NamespaceViolation,
            ),
            (
                "orders",
                "orders:missing",
                crate::request::WsProtocolErrorCode::ActionNotFound,
            ),
        ] {
            let request = Arc::new(test_request(connection_id, event));
            let outcome = route_test_message(
                Arc::clone(&table),
                container.services(),
                Arc::new(WebSocketContext::new(
                    connection_id,
                    Arc::clone(&manager),
                    namespace.into(),
                )),
                request,
                message_runtime(&manager),
            )
            .await;
            assert_eq!(outcome, WsMessageOutcome::Rejected(expected));
        }
        container.close().await.unwrap();
    }

    #[test]
    fn handshake_headers_reject_duplicate_custom_credentials_without_silent_overwrite() {
        use tokio_tungstenite::tungstenite::http::{HeaderMap, HeaderValue};

        let mut headers = HeaderMap::new();
        headers.append("x-api-key", HeaderValue::from_static("first-secret"));
        headers.append("x-api-key", HeaderValue::from_static("second-secret"));
        assert!(has_ambiguous_handshake_headers(&headers));
        assert!(collect_handshake_headers(&headers).is_err());
    }

    #[test]
    fn handshake_headers_reject_duplicate_connection_and_upgrade() {
        use tokio_tungstenite::tungstenite::http::{HeaderMap, HeaderValue};

        for name in ["connection", "upgrade"] {
            let mut headers = HeaderMap::new();
            headers.append(name, HeaderValue::from_static("first"));
            headers.append(name, HeaderValue::from_static("second"));
            assert!(has_ambiguous_handshake_headers(&headers), "{name}");
            assert!(collect_handshake_headers(&headers).is_err(), "{name}");
        }
    }

    #[test]
    fn handshake_headers_normalize_names_to_lowercase() {
        use tokio_tungstenite::tungstenite::http::{HeaderMap, HeaderName, HeaderValue};

        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_bytes(b"X-API-Key").unwrap(),
            HeaderValue::from_static("credential"),
        );

        let collected = collect_handshake_headers(&headers).unwrap();
        assert_eq!(collected.len(), 1);
        assert_eq!(
            collected.get("x-api-key").map(String::as_str),
            Some("credential")
        );
        assert!(!collected.contains_key("X-API-Key"));
    }

    #[test]
    fn handshake_headers_only_merge_websocket_list_fields() {
        use tokio_tungstenite::tungstenite::http::{HeaderMap, HeaderValue};

        let mut protocols = HeaderMap::new();
        protocols.append(
            "sec-websocket-protocol",
            HeaderValue::from_static(crate::request::LILY_WEBSOCKET_SUBPROTOCOL),
        );
        protocols.append(
            "sec-websocket-protocol",
            HeaderValue::from_static("legacy.chat"),
        );
        protocols.append(
            "sec-websocket-extensions",
            HeaderValue::from_static("permessage-deflate"),
        );
        protocols.append(
            "sec-websocket-extensions",
            HeaderValue::from_static("x-lily-extension"),
        );
        assert!(!has_ambiguous_handshake_headers(&protocols));
        let collected = collect_handshake_headers(&protocols).unwrap();
        assert_eq!(collected["sec-websocket-protocol"], "lily.v2,legacy.chat");
        assert_eq!(
            collected["sec-websocket-extensions"],
            "permessage-deflate,x-lily-extension"
        );
    }

    #[test]
    fn handshake_headers_reject_non_text_values() {
        use tokio_tungstenite::tungstenite::http::{HeaderMap, HeaderValue};

        let mut invalid = HeaderMap::new();
        invalid.insert("x-invalid", HeaderValue::from_bytes(&[0xff]).unwrap());
        assert!(collect_handshake_headers(&invalid).is_err());
    }

    #[test]
    fn handshake_headers_enforce_count_name_value_and_merge_bounds() {
        use crate::request::{
            MAX_HANDSHAKE_HEADER_ENTRIES, MAX_HANDSHAKE_HEADER_NAME_BYTES,
            MAX_HANDSHAKE_HEADER_VALUE_BYTES, WsHeaderError,
        };
        use tokio_tungstenite::tungstenite::http::{HeaderMap, HeaderName, HeaderValue};

        let mut too_many = HeaderMap::new();
        for index in 0..=MAX_HANDSHAKE_HEADER_ENTRIES {
            too_many.insert(
                HeaderName::from_bytes(format!("x-bounded-{index}").as_bytes()).unwrap(),
                HeaderValue::from_static("ok"),
            );
        }
        assert_eq!(
            collect_handshake_headers(&too_many).unwrap_err(),
            WsHeaderError::TooManyHeaders
        );

        let mut long_name = HeaderMap::new();
        long_name.insert(
            HeaderName::from_bytes(
                format!("x-{}", "a".repeat(MAX_HANDSHAKE_HEADER_NAME_BYTES)).as_bytes(),
            )
            .unwrap(),
            HeaderValue::from_static("ok"),
        );
        assert_eq!(
            collect_handshake_headers(&long_name).unwrap_err(),
            WsHeaderError::InvalidHeaderName
        );

        let mut long_value = HeaderMap::new();
        long_value.insert(
            "x-large",
            HeaderValue::from_str(&"a".repeat(MAX_HANDSHAKE_HEADER_VALUE_BYTES + 1)).unwrap(),
        );
        assert_eq!(
            collect_handshake_headers(&long_value).unwrap_err(),
            WsHeaderError::InvalidHeaderValue
        );

        let mut merged = HeaderMap::new();
        let maximum = HeaderValue::from_str(&"a".repeat(MAX_HANDSHAKE_HEADER_VALUE_BYTES)).unwrap();
        merged.append("sec-websocket-protocol", maximum.clone());
        merged.append("sec-websocket-protocol", maximum);
        assert_eq!(
            collect_handshake_headers(&merged).unwrap_err(),
            WsHeaderError::MergedHeaderValueTooLarge
        );
    }

    #[tokio::test]
    async fn trusted_handshake_context_reaches_middleware_guard_and_action_immutably() {
        CONTEXT_MIDDLEWARE_OBSERVED.store(false, Ordering::SeqCst);
        TRUSTED_GUARD_OBSERVED.store(false, Ordering::SeqCst);
        TRUSTED_ACTION_OBSERVED.store(false, Ordering::SeqCst);
        let principal = Principal::new(
            "subject-1",
            ["operator".to_owned()],
            ["orders:read".to_owned(), "orders:write".to_owned()],
            serde_json::Map::new(),
        );
        let mut headers = WsHeaders::default();
        headers.origin = Some("https://app.example".into());
        let peer_addr = "127.0.0.1:43123".parse().unwrap();
        let handshake = Arc::new(WsHandshakeContext::new(
            "orders".into(),
            headers.clone(),
            Some(principal),
            Some(crate::request::LILY_WEBSOCKET_SUBPROTOCOL.into()),
            peer_addr,
            lily_web_core::RequestConnectionInfo::direct(peer_addr.ip()),
            WsTransportSecurity::Plaintext,
        ));
        let connection_id = Uuid::new_v4();
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            128,
            128,
            ["orders".into()],
        ));
        let context = Arc::new(
            WebSocketContext::new(connection_id, manager.clone(), "orders".into())
                .with_handshake_context(handshake.clone()),
        );
        let request =
            test_request(connection_id, "orders:trusted").with_handshake_context(handshake);

        let container = ApplicationContainer::build().await.unwrap();
        let table = test_action_table(
            vec![pending_action_with_middlewares(
                "trusted",
                vec![WebSocketMessageMiddlewareRegistration::of::<
                    ContextMiddleware,
                >()],
                vec![WebSocketGuardRegistration::of::<ContextGuard>()],
                None,
            )],
            container.services(),
        )
        .await;

        let outcome = route_test_message(
            table,
            container.services(),
            context,
            Arc::new(request),
            message_runtime(&manager),
        )
        .await;

        assert_eq!(outcome, WsMessageOutcome::Handled);
        assert!(CONTEXT_MIDDLEWARE_OBSERVED.load(Ordering::SeqCst));
        assert!(TRUSTED_GUARD_OBSERVED.load(Ordering::SeqCst));
        assert!(TRUSTED_ACTION_OBSERVED.load(Ordering::SeqCst));
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn message_local_is_shared_from_middleware_through_guard_action_and_reverse_unwind() {
        MESSAGE_LOCAL_GUARD_OBSERVED.store(false, Ordering::SeqCst);
        MESSAGE_LOCAL_ACTION_OBSERVED.store(false, Ordering::SeqCst);
        MESSAGE_LOCAL_AFTER_OBSERVED.store(false, Ordering::SeqCst);
        let container = ApplicationContainer::build().await.unwrap();
        let table = test_action_table(
            vec![pending_action_with_middlewares(
                "message-local",
                vec![WebSocketMessageMiddlewareRegistration::of::<
                    MessageLocalMiddleware,
                >()],
                vec![WebSocketGuardRegistration::of::<MessageLocalGuard>()],
                None,
            )],
            container.services(),
        )
        .await;
        let connection_id = Uuid::new_v4();
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            128,
            128,
            ["orders".into()],
        ));
        let outcome = route_test_message(
            table,
            container.services(),
            Arc::new(WebSocketContext::new(
                connection_id,
                Arc::clone(&manager),
                "orders".into(),
            )),
            Arc::new(test_request(connection_id, "orders:message-local")),
            message_runtime(&manager),
        )
        .await;

        assert_eq!(outcome, WsMessageOutcome::Handled);
        assert!(MESSAGE_LOCAL_GUARD_OBSERVED.load(Ordering::SeqCst));
        assert!(MESSAGE_LOCAL_ACTION_OBSERVED.load(Ordering::SeqCst));
        assert!(MESSAGE_LOCAL_AFTER_OBSERVED.load(Ordering::SeqCst));
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn fallible_guard_factory_aborts_startup_instead_of_removing_guard() {
        let container = ApplicationContainer::build().await.unwrap();
        let result = materialize_websocket_controllers(
            vec![WebSocketControllerRegistration::of::<AppTestController>()],
            vec![pending_action(
                "write",
                vec![WebSocketGuardRegistration::of::<MissingGuard>()],
                None,
            )],
            container.services(),
        )
        .await;
        assert!(matches!(
            result,
            Err(crate::controller::WebSocketControllerMaterializationError::GuardInitialization {
                operation: "app::tests::AppTestController::message",
                guard,
                source: GuardInitializationError::MissingDependency
            }) if guard.ends_with("MissingGuard")
        ));
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn denied_guard_runs_before_and_never_invokes_handler() {
        DENIED_ACTION_INVOKED.store(false, Ordering::SeqCst);
        let container = ApplicationContainer::build().await.unwrap();
        let table = test_action_table(
            vec![pending_action(
                "denied",
                vec![WebSocketGuardRegistration::of::<DenyGuard>()],
                None,
            )],
            container.services(),
        )
        .await;
        let connection_id = Uuid::new_v4();
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            128,
            128,
            ["orders".into()],
        ));
        let (sender, mut receiver) = mpsc::channel(1);
        manager
            .add_connection(connection_id, sender, None, Some("orders".into()))
            .await
            .unwrap();
        let context = Arc::new(WebSocketContext::new(
            connection_id,
            manager.clone(),
            "orders".into(),
        ));
        let request = Arc::new(test_request(connection_id, "orders:denied"));

        let outcome = route_test_message(
            table,
            container.services(),
            context,
            request,
            message_runtime(&manager),
        )
        .await;

        assert_eq!(
            outcome,
            WsMessageOutcome::Rejected(crate::request::WsProtocolErrorCode::AuthorizationDenied)
        );
        assert!(!DENIED_ACTION_INVOKED.load(Ordering::SeqCst));
        let Message::Text(error) = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("guard denial response must be bounded")
            .unwrap()
        else {
            panic!("guard denial must emit a text error envelope");
        };
        let error: serde_json::Value = serde_json::from_str(&error).unwrap();
        assert_eq!(error["protocol_version"], 2);
        assert_eq!(error["msg_type"], "error");
        assert_eq!(error["data"]["code"], "TEST_GUARD_DENIED");
        assert_eq!(error["data"]["message"], "The message is not allowed.");
        manager.remove_connection(connection_id).await.unwrap();
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn message_deadline_stops_pending_guard_and_action_futures() {
        TIMEOUT_ACTION_DROPPED.store(false, Ordering::SeqCst);
        let connection_id = Uuid::new_v4();
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            128,
            128,
            ["orders".into()],
        ));
        let (sender, mut receiver) = mpsc::channel(4);
        manager
            .add_connection(connection_id, sender, None, Some("orders".into()))
            .await
            .unwrap();
        let context = Arc::new(WebSocketContext::new(
            connection_id,
            Arc::clone(&manager),
            "orders".into(),
        ));
        let request = Arc::new(test_request(connection_id, "orders:pending-timeout"));
        let container = ApplicationContainer::build().await.unwrap();

        let guard_table = test_action_table(
            vec![pending_action(
                "pending-timeout",
                vec![WebSocketGuardRegistration::of::<PendingGuard>()],
                None,
            )],
            container.services(),
        )
        .await;
        let mut runtime = message_runtime(&manager);
        runtime.message_timeout = Duration::from_millis(10);
        assert_eq!(
            route_test_message(
                guard_table,
                container.services(),
                Arc::clone(&context),
                Arc::clone(&request),
                runtime,
            )
            .await,
            WsMessageOutcome::Rejected(crate::request::WsProtocolErrorCode::HandlerFailed)
        );
        assert!(matches!(
            receiver.recv().await,
            Some(Message::Text(ref text)) if text.contains("MESSAGE_TIMEOUT")
        ));

        let action_table = test_action_table(
            vec![pending_action("pending-timeout", Vec::new(), None)],
            container.services(),
        )
        .await;
        let mut runtime = message_runtime(&manager);
        runtime.message_timeout = Duration::from_millis(10);
        let decoded = decode_test_request(&request);
        let dispatch = WsApp::route_decoded_message(
            action_table,
            container.services(),
            context,
            request,
            decoded,
            runtime,
        )
        .await;
        assert_eq!(
            dispatch.outcome,
            WsMessageOutcome::Rejected(crate::request::WsProtocolErrorCode::HandlerFailed)
        );
        assert!(matches!(
            dispatch.terminal,
            Some(PreparedWebSocketTerminal::ApplicationFrame(Message::Text(ref text)))
                if text.contains("MESSAGE_TIMEOUT")
        ));
        assert!(TIMEOUT_ACTION_DROPPED.load(Ordering::SeqCst));
        assert_eq!(manager.metrics_snapshot().timeouts, 2);
        assert_eq!(manager.metrics_snapshot().handler_failed, 1);

        manager.remove_connection(connection_id).await.unwrap();
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn shutdown_cancels_a_pending_action_and_preserves_shutdown_reason() {
        let connection_id = Uuid::new_v4();
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            128,
            128,
            ["orders".into()],
        ));
        let (sender, mut receiver) = mpsc::channel(2);
        manager
            .add_connection(connection_id, sender, None, Some("orders".into()))
            .await
            .unwrap();
        let context = Arc::new(WebSocketContext::new(
            connection_id,
            Arc::clone(&manager),
            "orders".into(),
        ));
        let request = Arc::new(test_request(connection_id, "orders:pending-shutdown"));
        let container = ApplicationContainer::build().await.unwrap();
        let table = test_action_table(
            vec![pending_action("pending-shutdown", Vec::new(), None)],
            container.services(),
        )
        .await;
        let runtime = message_runtime(&manager);
        runtime.cancellation.cancel();

        assert_eq!(
            route_test_message(table, container.services(), context, request, runtime).await,
            WsMessageOutcome::Close(crate::request::WsCloseReason::ServerShutdown)
        );
        let Some(Message::Close(Some(frame))) = receiver.recv().await else {
            panic!("shutdown cancellation must queue a close frame");
        };
        assert_eq!(
            frame.reason,
            crate::request::WsCloseReason::ServerShutdown.reason()
        );
        manager.remove_connection(connection_id).await.unwrap();
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn connection_cancellation_wins_but_later_deadline_expiry_remains_observable() {
        let connection_id = Uuid::new_v4();
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            128,
            128,
            ["orders".into()],
        ));
        let (sender, mut receiver) = mpsc::channel(2);
        manager
            .add_connection(connection_id, sender, None, Some("orders".into()))
            .await
            .unwrap();
        let context = Arc::new(WebSocketContext::new(
            connection_id,
            Arc::clone(&manager),
            "orders".into(),
        ));
        let request = Arc::new(test_request(connection_id, "orders:pending-shutdown"));
        let container = ApplicationContainer::build().await.unwrap();
        let table = test_action_table(
            vec![pending_action("pending-shutdown", Vec::new(), None)],
            container.services(),
        )
        .await;
        let mut runtime = message_runtime(&manager);
        runtime.message_timeout = Duration::from_millis(10);
        runtime.cancellation.cancel();

        assert_eq!(
            route_test_message(table, container.services(), context, request, runtime).await,
            WsMessageOutcome::Close(crate::request::WsCloseReason::ServerShutdown)
        );
        let Some(Message::Close(Some(frame))) = receiver.recv().await else {
            panic!("the cancellation winner must queue one close frame");
        };
        assert_eq!(
            frame.reason,
            crate::request::WsCloseReason::ServerShutdown.reason()
        );
        assert_eq!(manager.metrics_snapshot().timeouts, 1);
        assert!(receiver.try_recv().is_err());

        manager.remove_connection(connection_id).await.unwrap();
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn dropped_connection_waiter_does_not_skip_tracked_message_reverse_unwind() {
        let _test_lock = PANIC_UNWIND_TEST_LOCK.lock().await;
        PANIC_UNWIND_EVENTS.lock().unwrap().clear();
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let table = test_action_table(
            vec![pending_action_with_middlewares(
                "pending-shutdown",
                vec![WebSocketMessageMiddlewareRegistration::of::<
                    MessageLifecycleProbe<3>,
                >()],
                Vec::new(),
                None,
            )],
            container.services(),
        )
        .await;
        let action = table
            .find_action("orders", "pending-shutdown")
            .expect("tracked action exists");
        let frame_codec = table.frame_codec("orders").unwrap();
        let connection_id = Uuid::new_v4();
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            128,
            128,
            ["orders".into()],
        ));
        let context = Arc::new(WebSocketContext::new(
            connection_id,
            Arc::clone(&manager),
            "orders".into(),
        ));
        let request = Arc::new(test_request(connection_id, "orders:pending-shutdown"));
        let decoded = decode_test_request(&request);
        let runtime = message_runtime(&manager);
        let cancellation = runtime.cancellation.clone();
        let registry = MessageDispatchRegistry::default();
        let dispatch_container = Arc::clone(&container);
        let dispatch_extensions = container.services();
        let waiter = registry.spawn_owner_with_cancellation(
            connection_id,
            cancellation.clone(),
            move |slot| async move {
                dispatch_container
                    .run_scoped(
                        ProcessContext::new(),
                        WsApp::dispatch_message_owner(
                            action.as_ref(),
                            frame_codec,
                            dispatch_extensions,
                            context,
                            request,
                            decoded,
                            runtime,
                            slot,
                        ),
                    )
                    .await
            },
        );

        for _ in 0..100 {
            if PANIC_UNWIND_EVENTS
                .lock()
                .unwrap()
                .contains(&"entered.before")
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(*PANIC_UNWIND_EVENTS.lock().unwrap(), ["entered.before"]);
        drop(waiter);
        cancellation.cancel();
        let drain = registry
            .drain_until(
                Instant::now() + Duration::from_secs(1),
                &CancellationToken::new(),
            )
            .await;

        assert!(!drain.timed_out);
        assert_eq!(
            drain.aborted, 1,
            "only the uncooperative execution slot is stopped"
        );
        assert_eq!(drain.owner_join_cancelled, 0);
        assert_eq!(drain.outstanding, 0);
        assert_eq!(
            *PANIC_UNWIND_EVENTS.lock().unwrap(),
            ["entered.before", "entered.termination"]
        );
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn message_dispatch_drain_uses_force_or_the_autonomous_failure_fallback() {
        let empty_registry = MessageDispatchRegistry::default();
        let empty_force = CancellationToken::new();
        empty_force.cancel();
        let empty = empty_registry
            .drain_until(Instant::now() + Duration::from_secs(60), &empty_force)
            .await;
        assert!(!empty.forced);
        assert!(!empty.timed_out);
        assert_eq!(empty.pending, 0);
        assert_eq!(empty.aborted, 0);
        assert_eq!(empty.panicked, 0);

        let forced_registry = MessageDispatchRegistry::default();
        let _forced_waiter =
            forced_registry.spawn_owner(Uuid::nil(), |slot| slot.run(std::future::pending::<()>()));
        let force = CancellationToken::new();
        force.cancel();
        let forced = forced_registry.drain_with_force(None, &force).await;
        assert!(forced.forced);
        assert!(!forced.timed_out);
        assert_eq!(forced.pending, 1);
        assert_eq!(forced.aborted, 1);
        assert_eq!(forced.panicked, 0);

        let timed_registry = MessageDispatchRegistry::default();
        let _timed_waiter =
            timed_registry.spawn_owner(Uuid::nil(), |slot| slot.run(std::future::pending::<()>()));
        let timed = timed_registry
            .drain_until(Instant::now(), &CancellationToken::new())
            .await;
        assert!(!timed.forced);
        assert!(timed.timed_out);
        assert_eq!(timed.pending, 1);
        assert_eq!(timed.aborted, 1);
        assert_eq!(timed.panicked, 0);

        let panicked_registry = MessageDispatchRegistry::default();
        let panicked_waiter = panicked_registry.spawn_owner(Uuid::nil(), |_slot| async {
            panic!("test-only message dispatch panic");
        });
        let panicked = panicked_registry
            .drain_with_force(None, &CancellationToken::new())
            .await;
        assert!(!panicked.forced);
        assert!(!panicked.timed_out);
        assert_eq!(panicked.aborted, 0);
        assert_eq!(panicked.panicked, 1);
        assert!(panicked_waiter.await.is_err());
    }

    #[tokio::test]
    async fn message_rejection_unwinds_entered_prefix_and_materializes_one_close_frame() {
        MIDDLEWARE_ACTION_INVOKED.store(false, Ordering::SeqCst);
        MESSAGE_REJECTION_EVENTS.lock().unwrap().clear();
        let container = ApplicationContainer::build().await.unwrap();
        let table = test_action_table(
            vec![pending_action_with_middlewares(
                "middleware-reject",
                vec![
                    WebSocketMessageMiddlewareRegistration::of::<MessageLifecycleProbe<0>>(),
                    WebSocketMessageMiddlewareRegistration::of::<MessageLifecycleProbe<1>>(),
                    WebSocketMessageMiddlewareRegistration::of::<MessageLifecycleProbe<2>>(),
                ],
                Vec::new(),
                None,
            )],
            container.services(),
        )
        .await;
        let connection_id = Uuid::new_v4();
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            128,
            128,
            ["orders".into()],
        ));
        let (sender, mut receiver) = mpsc::channel(2);
        manager
            .add_connection(connection_id, sender, None, Some("orders".into()))
            .await
            .unwrap();
        let context = Arc::new(WebSocketContext::new(
            connection_id,
            Arc::clone(&manager),
            "orders".into(),
        ));
        let request = Arc::new(test_request(connection_id, "orders:middleware-reject"));

        let outcome = route_test_message(
            table,
            container.services(),
            context,
            request,
            message_runtime(&manager),
        )
        .await;

        assert_eq!(
            outcome,
            WsMessageOutcome::Close(crate::request::WsCloseReason::PolicyViolation)
        );
        assert_eq!(
            *MESSAGE_REJECTION_EVENTS.lock().unwrap(),
            ["outer.before", "rejecting.before", "outer.after"]
        );
        assert!(!MIDDLEWARE_ACTION_INVOKED.load(Ordering::SeqCst));
        let Message::Close(Some(actual)) = receiver.recv().await.unwrap() else {
            panic!("reverse unwind must materialize one close frame");
        };
        let expected = crate::request::WsCloseReason::PolicyViolation.frame();
        assert_eq!(actual.code, expected.code);
        assert_eq!(actual.reason, expected.reason);
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        manager.remove_connection(connection_id).await.unwrap();
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn synchronous_action_panic_is_bounded_and_still_unwinds_message_middleware() {
        let _test_lock = PANIC_UNWIND_TEST_LOCK.lock().await;
        PANIC_UNWIND_EVENTS.lock().unwrap().clear();
        let container = ApplicationContainer::build().await.unwrap();
        let table = test_action_table(
            vec![pending_action_with_middlewares(
                "panic",
                vec![WebSocketMessageMiddlewareRegistration::of::<
                    MessageLifecycleProbe<3>,
                >()],
                Vec::new(),
                None,
            )],
            container.services(),
        )
        .await;
        let connection_id = Uuid::new_v4();
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            128,
            128,
            ["orders".into()],
        ));
        let (sender, mut receiver) = mpsc::channel(2);
        manager
            .add_connection(connection_id, sender, None, Some("orders".into()))
            .await
            .unwrap();
        let request = Arc::new(test_request(connection_id, "orders:panic"));

        let outcome = route_test_message(
            table,
            container.services(),
            Arc::new(WebSocketContext::new(
                connection_id,
                Arc::clone(&manager),
                "orders".into(),
            )),
            request,
            message_runtime(&manager),
        )
        .await;

        assert_eq!(
            outcome,
            WsMessageOutcome::Rejected(crate::request::WsProtocolErrorCode::HandlerFailed)
        );
        assert_eq!(
            *PANIC_UNWIND_EVENTS.lock().unwrap(),
            ["entered.before", "entered.after"]
        );
        let Message::Text(error) = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("panicking handler response must be bounded")
            .unwrap()
        else {
            panic!("panicking handler must emit one canonical protocol error");
        };
        assert!(error.contains("ws.handler_failed"));
        assert!(!error.contains("LILY_SECRET_SYNC_ACTION_PANIC"));
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        manager.remove_connection(connection_id).await.unwrap();
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn output_codec_panic_is_bounded_and_still_unwinds_message_middleware() {
        let _test_lock = PANIC_UNWIND_TEST_LOCK.lock().await;
        PANIC_UNWIND_EVENTS.lock().unwrap().clear();
        let container = ApplicationContainer::build().await.unwrap();
        let operation = PendingWebSocketOperation::new(
            WebSocketOperationKind::Message,
            Some("codec-panic"),
            "app::tests::AppTestController::codec_panic",
            WebSocketActionRegistration::of::<AppTestController>(bind_app_test_message),
            WebSocketOperationMetadata::new(
                vec![WebSocketMessageMiddlewareRegistration::of::<
                    MessageLifecycleProbe<3>,
                >()],
                Vec::new(),
                Some(WebSocketPayloadCodecRegistration::of::<PanickingOutputCodec>()),
                None,
                WebSocketAsyncApiRegistration::unspecified(),
            ),
        );
        let table = test_action_table(vec![operation], container.services()).await;
        let connection_id = Uuid::new_v4();
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            128,
            128,
            ["orders".into()],
        ));
        let (sender, mut receiver) = mpsc::channel(2);
        manager
            .add_connection(connection_id, sender, None, Some("orders".into()))
            .await
            .unwrap();
        let outcome = route_test_message(
            table,
            container.services(),
            Arc::new(WebSocketContext::new(
                connection_id,
                Arc::clone(&manager),
                "orders".into(),
            )),
            Arc::new(test_request(connection_id, "orders:codec-panic")),
            message_runtime(&manager),
        )
        .await;

        assert_eq!(
            outcome,
            WsMessageOutcome::Close(crate::request::WsCloseReason::InternalFailure)
        );
        assert_eq!(
            *PANIC_UNWIND_EVENTS.lock().unwrap(),
            ["entered.before", "entered.after"]
        );
        assert!(matches!(
            receiver.recv().await,
            Some(Message::Close(Some(_)))
        ));
        assert!(matches!(
            receiver.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
        manager.remove_connection(connection_id).await.unwrap();
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn handler_failure_is_a_canonical_error_not_a_success() {
        let connection_id = Uuid::new_v4();
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            128,
            128,
            ["orders".into()],
        ));
        let (sender, mut receiver) = mpsc::channel(1);
        manager
            .add_connection(connection_id, sender, None, Some("orders".into()))
            .await
            .unwrap();
        let container = ApplicationContainer::build().await.unwrap();
        let table = test_action_table(
            vec![pending_action("failure", Vec::new(), None)],
            container.services(),
        )
        .await;
        let request = Arc::new(test_request(connection_id, "orders:failure"));
        route_test_message(
            table,
            container.services(),
            Arc::new(WebSocketContext::new(
                connection_id,
                manager.clone(),
                "orders".into(),
            )),
            request,
            message_runtime(&manager),
        )
        .await;

        let Message::Text(error) = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .expect("handler failure response must be bounded")
            .unwrap()
        else {
            panic!("handler failure must emit a canonical error envelope");
        };
        assert!(error.contains(WebSocketErrorCode::INTERNAL.as_str()));
        assert!(!error.contains("secret detail"));
        manager.remove_connection(connection_id).await.unwrap();
        container.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn connected_callback_can_yield_after_execution_cancellation_and_return() {
        struct CooperativeConnected {
            _controller: Arc<AppTestController>,
        }
        impl WebSocketLifecycleAction for CooperativeConnected {
            fn call(&self, invocation: WebSocketLifecycleInvocation) -> WebSocketLifecycleFuture {
                Box::pin(async move {
                    invocation
                        .execution_cancellation()
                        .unwrap()
                        .cancelled()
                        .await;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    Ok(())
                })
            }
        }
        fn bind(
            controller: ErasedWebSocketController,
        ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
            Ok(BoundWebSocketOperation::Connected(Arc::new(
                CooperativeConnected {
                    _controller: downcast_websocket_controller::<AppTestController>(controller)?,
                },
            )))
        }
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let extensions = container.services();
        let table = test_action_table(
            vec![pending_lifecycle(WebSocketOperationKind::Connected, bind)],
            Arc::clone(&extensions),
        )
        .await;
        let lifecycle = table.lifecycle_for_namespace("orders").unwrap();
        let source = CancellationToken::new();
        let budget = crate::shutdown::ShutdownBudget::default();
        let scopes = ScopeCleanupRegistry::with_budget(budget.clone());
        let connection_id = Uuid::new_v4();
        let context = Arc::new(
            WebSocketContext::new(
                connection_id,
                Arc::new(ConnectionManager::new()),
                "orders".into(),
            )
            .with_shutdown_budget(budget.clone()),
        );
        let start = Instant::now();
        let disconnect_ledger = Default::default();
        let execution = WsApp::run_connect_hooks(
            connection_id,
            WebSocketConnectRuntime {
                container: &container,
                extensions: &extensions,
                context: &context,
                lifecycle: &lifecycle,
                stage_timeout: Duration::from_secs(1),
                cancellation: &source,
                scopes: &scopes,
                disconnect_ledger: &disconnect_ledger,
            },
        );
        tokio::pin!(execution);
        assert!(matches!(
            futures_util::poll!(execution.as_mut()),
            std::task::Poll::Pending
        ));
        budget.force_before(start + Duration::from_millis(200));
        source.cancel();
        assert_eq!(execution.await, Ok(()));
        assert!(start.elapsed() <= Duration::from_millis(6));
        assert_eq!(scopes.drain().await.outstanding, 0);
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn lifecycle_operation_timeout_overrides_the_server_action_fallback() {
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let extensions = container.services();
        let table = test_action_table(
            vec![
                pending_lifecycle_with_timeout(
                    WebSocketOperationKind::Connected,
                    bind_connect_pending,
                    Some(Duration::from_millis(10)),
                ),
                pending_lifecycle_with_timeout(
                    WebSocketOperationKind::Disconnected,
                    bind_disconnect_safe,
                    Some(Duration::from_millis(17)),
                ),
            ],
            Arc::clone(&extensions),
        )
        .await;
        let lifecycle = table.lifecycle_for_namespace("orders").unwrap();
        let ledger: WebSocketDisconnectLedger = Default::default();
        assert_eq!(
            lifecycle.disconnected_operation().unwrap().timeout(),
            Some(Duration::from_millis(17))
        );

        let connection_id = Uuid::new_v4();
        let context = Arc::new(WebSocketContext::new(
            connection_id,
            Arc::new(ConnectionManager::new()),
            "orders".into(),
        ));
        let started = Instant::now();
        let result = WsApp::run_connect_hooks(
            connection_id,
            WebSocketConnectRuntime {
                container: &container,
                extensions: &extensions,
                context: &context,
                lifecycle: &lifecycle,
                stage_timeout: Duration::from_secs(1),
                cancellation: &CancellationToken::new(),
                scopes: &ScopeCleanupRegistry::default(),
                disconnect_ledger: &ledger,
            },
        )
        .await;
        assert_eq!(result, Err(WebSocketConnectFailure::TimedOut));
        assert!(ledger.lock().unwrap().is_empty());
        assert!(started.elapsed() < Duration::from_millis(500));
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn failed_connected_never_arms_user_disconnected() {
        DISCONNECT_FAILURE_CALLS.store(0, Ordering::SeqCst);
        let connection_id = Uuid::new_v4();
        let manager = Arc::new(ConnectionManager::new());
        let context = Arc::new(WebSocketContext::new(
            connection_id,
            manager,
            "orders".into(),
        ));
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let extensions = container.services();
        let table = test_action_table(
            vec![
                pending_lifecycle(WebSocketOperationKind::Connected, bind_connect_failure),
                pending_lifecycle(
                    WebSocketOperationKind::Disconnected,
                    bind_disconnect_failure,
                ),
            ],
            Arc::clone(&extensions),
        )
        .await;
        let lifecycle = table.lifecycle_for_namespace("orders").unwrap();
        let ledger: WebSocketDisconnectLedger = Default::default();
        let connect = WsApp::run_connect_hooks(
            connection_id,
            WebSocketConnectRuntime {
                container: &container,
                extensions: &extensions,
                context: &context,
                lifecycle: &lifecycle,
                stage_timeout: Duration::from_secs(1),
                cancellation: &CancellationToken::new(),
                scopes: &ScopeCleanupRegistry::default(),
                disconnect_ledger: &ledger,
            },
        )
        .await
        .unwrap_err();
        assert_eq!(connect, WebSocketConnectFailure::Handler);
        assert_eq!(
            connect.into_server_error().to_string(),
            "Handler error: WebSocket on_connected lifecycle failed"
        );

        let report = WsApp::run_websocket_disconnect_ledger(
            connection_id,
            Arc::clone(&container),
            extensions,
            context,
            ledger,
            crate::middleware::WsConnectionCloseCategory::HandlerError,
            CancellationToken::new(),
            ScopeCleanupRegistry::default(),
        )
        .await;
        assert_eq!(report.attempted(), 0);
        assert_eq!(report.failed(), 0);
        assert_eq!(DISCONNECT_FAILURE_CALLS.load(Ordering::SeqCst), 0);
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn panicking_connect_hook_is_contained_as_a_bounded_lifecycle_failure() {
        let connection_id = Uuid::new_v4();
        let manager = Arc::new(ConnectionManager::new());
        let context = Arc::new(WebSocketContext::new(
            connection_id,
            manager,
            "orders".into(),
        ));
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let extensions = container.services();
        let table = test_action_table(
            vec![pending_lifecycle(
                WebSocketOperationKind::Connected,
                bind_connect_panic,
            )],
            Arc::clone(&extensions),
        )
        .await;
        let lifecycle = table.lifecycle_for_namespace("orders").unwrap();

        let error = WsApp::run_connect_hooks(
            connection_id,
            WebSocketConnectRuntime {
                container: &container,
                extensions: &extensions,
                context: &context,
                lifecycle: &lifecycle,
                stage_timeout: Duration::from_secs(1),
                cancellation: &CancellationToken::new(),
                scopes: &ScopeCleanupRegistry::default(),
                disconnect_ledger: &Default::default(),
            },
        )
        .await
        .unwrap_err();

        assert_eq!(error, WebSocketConnectFailure::Panicked);
        let diagnostic = error.into_server_error().to_string();
        assert_eq!(
            diagnostic,
            "Handler error: WebSocket on_connected lifecycle panicked"
        );
        assert!(!diagnostic.contains("LILY_SECRET_CONNECT_HOOK_PANIC"));
        assert!(lifecycle.disconnected().is_none());
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn synchronous_disconnect_panic_does_not_skip_remaining_reverse_cleanup() {
        DISCONNECT_SAFE_CALLS.store(0, Ordering::SeqCst);
        let connection_id = Uuid::new_v4();
        let manager = Arc::new(ConnectionManager::new());
        let context = Arc::new(WebSocketContext::new(
            connection_id,
            manager,
            "orders".into(),
        ));
        let controller = Arc::new(AppTestController);
        let safe: WebSocketLifecycleHandler = Arc::new(AppTestLifecycleAction {
            _controller: Arc::clone(&controller),
            behavior: AppTestLifecycleBehavior::DisconnectSafe,
        });
        let panicking: WebSocketLifecycleHandler = Arc::new(AppTestLifecycleAction {
            _controller: controller,
            behavior: AppTestLifecycleBehavior::DisconnectPanic,
        });
        let timeout = Duration::from_secs(1);
        let ledger: WebSocketDisconnectLedger = Arc::new(StdMutex::new(
            vec![(safe, timeout), (panicking, timeout)].into(),
        ));
        let container = Arc::new(ApplicationContainer::build().await.unwrap());

        let report = WsApp::run_websocket_disconnect_ledger(
            connection_id,
            Arc::clone(&container),
            container.services(),
            context,
            ledger,
            crate::middleware::WsConnectionCloseCategory::NormalPeer,
            CancellationToken::new(),
            ScopeCleanupRegistry::default(),
        )
        .await;

        assert_eq!(report.attempted(), 2);
        assert_eq!(report.completed(), 1);
        assert_eq!(report.failed(), 1);
        assert_eq!(report.panicked(), 1);
        assert_eq!(DISCONNECT_SAFE_CALLS.load(Ordering::SeqCst), 1);
        container.close().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn disconnected_adapter_uses_cleanup_parent_and_isolates_handler_timeout() {
        struct SignalHandler {
            pending: bool,
            views: Arc<StdMutex<Vec<(crate::CleanupCancellation, bool)>>>,
        }
        impl WebSocketLifecycleAction for SignalHandler {
            fn call(
                &self,
                mut invocation: WebSocketLifecycleInvocation,
            ) -> WebSocketLifecycleFuture {
                let views = Arc::clone(&self.views);
                let pending = self.pending;
                Box::pin(async move {
                    let (signal,) = crate::extract_disconnected_arguments::<(
                        crate::CleanupCancellation,
                    )>(&mut invocation)
                    .await?;
                    let initially_cancelled = signal.is_cancelled();
                    assert_eq!(
                        invocation.cleanup_cancellation().unwrap().is_cancelled(),
                        initially_cancelled
                    );
                    assert!(invocation.execution_cancellation().is_none());
                    views.lock().unwrap().push((signal, initially_cancelled));
                    if pending {
                        std::future::pending().await
                    } else {
                        Ok(())
                    }
                })
            }
        }
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        for parent_cancelled in [false, true] {
            let connection_id = Uuid::new_v4();
            let context = Arc::new(WebSocketContext::new(
                connection_id,
                Arc::new(ConnectionManager::new()),
                "signal-test".to_owned(),
            ));
            let cleanup_parent = CancellationToken::new();
            if parent_cancelled {
                cleanup_parent.cancel();
            }
            let views = Arc::new(StdMutex::new(Vec::new()));
            let handlers: Vec<(WebSocketLifecycleHandler, Duration)> = vec![
                (
                    Arc::new(SignalHandler {
                        pending: false,
                        views: Arc::clone(&views),
                    }),
                    Duration::from_millis(10),
                ),
                (
                    Arc::new(SignalHandler {
                        pending: true,
                        views: Arc::clone(&views),
                    }),
                    Duration::from_millis(10),
                ),
            ];
            let report = WsApp::run_websocket_disconnect_ledger(
                connection_id,
                Arc::clone(&container),
                container.services(),
                context,
                Arc::new(StdMutex::new(handlers.into())),
                crate::middleware::WsConnectionCloseCategory::NormalPeer,
                cleanup_parent.clone(),
                ScopeCleanupRegistry::default(),
            )
            .await;
            assert_eq!(report.attempted(), 2);
            assert_eq!(report.completed(), 1);
            assert_eq!(report.timed_out(), 1);
            let views = views.lock().unwrap();
            assert_eq!(views.len(), 2);
            assert!(
                views
                    .iter()
                    .all(|(_, initially_cancelled)| *initially_cancelled == parent_cancelled)
            );
            assert!(views.iter().all(|(signal, _)| signal.is_cancelled()));
            assert_eq!(cleanup_parent.is_cancelled(), parent_cancelled);
        }
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn connection_task_drain_joins_success_and_aborts_stragglers_at_deadline() {
        let mut completed_tasks = OwnedTaskSet::new();
        completed_tasks.spawn(async {});
        let completed = WsApp::drain_connection_tasks(
            &mut completed_tasks,
            Instant::now() + Duration::from_secs(1),
        )
        .await;
        assert_eq!(completed.completed, 1);
        assert!(!completed.timed_out);
        assert!(completed_tasks.is_empty());

        let mut panicked_tasks = OwnedTaskSet::new();
        panicked_tasks.spawn(async { panic!("test-only connection task panic") });
        let panicked = WsApp::drain_connection_tasks(
            &mut panicked_tasks,
            Instant::now() + Duration::from_secs(1),
        )
        .await;
        assert_eq!(panicked.join_errors.len(), 1);
        assert!(panicked.join_errors[0].contains("panicked"));
        assert!(panicked_tasks.is_empty());

        let dropped = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let mut blocked_tasks = OwnedTaskSet::new();
        let task_dropped = Arc::clone(&dropped);
        blocked_tasks.spawn(async move {
            let _drop_flag = DropFlag(task_dropped);
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        started_rx.await.unwrap();

        let timed_out = WsApp::drain_connection_tasks(
            &mut blocked_tasks,
            Instant::now() + Duration::from_millis(10),
        )
        .await;
        assert!(timed_out.timed_out);
        assert_eq!(timed_out.pending_at_deadline, 1);
        assert_eq!(timed_out.cancelled, 1);
        assert!(timed_out.join_errors.is_empty());
        assert!(blocked_tasks.is_empty());
        assert!(dropped.load(Ordering::SeqCst));

        let dropped = Arc::new(AtomicBool::new(false));
        let mut forced_tasks = OwnedTaskSet::new();
        let task_dropped = Arc::clone(&dropped);
        forced_tasks.spawn(async move {
            let _drop_flag = DropFlag(task_dropped);
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;
        let force = CancellationToken::new();
        force.cancel();
        let forced = WsApp::drain_connection_tasks_with_force(
            &mut forced_tasks,
            None,
            &force,
            &crate::shutdown::ShutdownBudget::default(),
        )
        .await;
        assert!(forced.forced);
        assert_eq!(forced.pending_at_deadline, 1);
        assert_eq!(forced.cancelled, 1);
        assert!(forced_tasks.is_empty());
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn managed_websocket_server_replays_its_terminal_outcome() {
        let mut server = ManagedWsServer {
            admission: CancellationToken::new(),
            message_admission: CancellationToken::new(),
            force: CancellationToken::new(),
            task: tokio::spawn(async { Ok(()) }).into(),
            outcome: None,
            runtime_result_observed: false,
        };
        server.wait().await.unwrap();
        server.wait().await.unwrap();
    }

    #[tokio::test]
    async fn managed_websocket_server_drain_does_not_reclassify_observed_runtime_failure() {
        let mut server = ManagedWsServer {
            admission: CancellationToken::new(),
            message_admission: CancellationToken::new(),
            force: CancellationToken::new(),
            task: tokio::spawn(async {
                Err(ServerError::connection_error(
                    "required subscription readiness timed out".to_owned(),
                ))
            })
            .into(),
            outcome: None,
            runtime_result_observed: false,
        };

        let error = server.wait_for_runtime().await.unwrap_err().to_string();
        assert_eq!(
            error,
            "Connection error: required subscription readiness timed out"
        );
        server
            .drain()
            .await
            .expect("an observed runtime failure is already terminal evidence");
    }

    #[tokio::test]
    async fn managed_websocket_server_drain_reports_an_unobserved_runtime_failure() {
        let mut server = ManagedWsServer {
            admission: CancellationToken::new(),
            message_admission: CancellationToken::new(),
            force: CancellationToken::new(),
            task: tokio::spawn(async {
                Err(ServerError::connection_error(
                    "listener failed while shutdown was pending".to_owned(),
                ))
            })
            .into(),
            outcome: None,
            runtime_result_observed: false,
        };

        let error = server.drain().await.unwrap_err().to_string();
        assert_eq!(
            error,
            "Connection error: listener failed while shutdown was pending"
        );
        assert_eq!(
            server.drain().await.unwrap_err().to_string(),
            error,
            "retrying a failed drain must not reinterpret a cached error as completed cleanup"
        );
    }

    #[tokio::test]
    async fn coordinator_force_retry_cannot_turn_a_failed_server_drain_into_completion() {
        let admission = CancellationToken::new();
        let message_admission = CancellationToken::new();
        let force = CancellationToken::new();
        let server = Arc::new(tokio::sync::Mutex::new(ManagedWsServer {
            admission: admission.clone(),
            message_admission: message_admission.clone(),
            force: force.clone(),
            task: tokio::spawn(async {
                Err(ServerError::connection_error(
                    "message cleanup timed out".to_owned(),
                ))
            })
            .into(),
            outcome: None,
            runtime_result_observed: false,
        }));
        let mut coordinator = FrameworkShutdownCoordinator::new(
            Arc::new(ShutdownState::new()),
            Duration::from_secs(1),
        );
        coordinator.register(WsServerLifecycleHandle {
            phase: FrameworkShutdownPhase::DrainInFlight,
            server: Arc::clone(&server),
            admission,
            message_admission,
            force,
            timeout: Duration::from_secs(1),
            budget: Default::default(),
        });
        let report = coordinator.execute_report(ShutdownSignal::Manual).await;
        assert!(report.forced);
        assert_eq!(
            report.completion,
            lily_shutdown::FrameworkShutdownCompletion::Incomplete
        );
        assert!(!report.is_terminal_complete());
        assert!(matches!(server.lock().await.outcome, Some(Err(_))));
    }

    #[tokio::test]
    async fn managed_websocket_server_does_not_expose_join_panic_payloads() {
        let mut server = ManagedWsServer {
            admission: CancellationToken::new(),
            message_admission: CancellationToken::new(),
            force: CancellationToken::new(),
            task: tokio::spawn(async {
                panic!("LILY_SECRET_SERVER_TASK_PANIC");
                #[allow(unreachable_code)]
                Ok(())
            })
            .into(),
            outcome: None,
            runtime_result_observed: false,
        };

        let error = server.wait().await.unwrap_err().to_string();
        assert_eq!(error, "Connection error: WebSocket server task panicked");
        assert!(!error.contains("LILY_SECRET_SERVER_TASK_PANIC"));
    }

    #[tokio::test]
    async fn hsk_04_exact_limit_early_data_never_reaches_connection_publication_or_hooks() {
        timeout(Duration::from_secs(10), async {
            let _test_lock = OPEN_ORDER_LIFECYCLE_TEST_LOCK.lock().await;
            reset_dec_009_lifecycle_probes();
            let app = hsk_04_no_handshake_pipeline_app().await;
            let mut request = exact_size_upgrade_request("open-order", 64 * 1024);
            request.push(0x81);
            let (stream, capture) = ExactKHandshakeStream::complete(request);
            let server_task =
                spawn_handshake_test_connection(&app, stream, CancellationToken::new());

            timeout(Duration::from_secs(2), server_task)
                .await
                .expect("HSK-04 connection deadline")
                .expect("HSK-04 connection task did not panic")
                .expect("the early-data rejection must terminate cleanly");

            let response = String::from_utf8(capture.bytes()).expect("ASCII HTTP rejection");
            assert!(response.starts_with("HTTP/1.1 400 Bad Request\r\n"));
            assert!(!response.contains("101 Switching Protocols"));
            assert!(
                response.ends_with("Data cannot be sent before the WebSocket upgrade completes.")
            );
            assert_eq!(capture.flush_count(), 1);
            assert!(capture.response_fully_flushed());
            assert_eq!(app.active_connection_count().await, 0);
            assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
            assert_eq!(OPEN_ORDER_CONNECTED_CALLS.load(Ordering::SeqCst), 0);
            assert_eq!(OPEN_ORDER_DISCONNECTED_CALLS.load(Ordering::SeqCst), 0);
            assert!(OPEN_ORDER_EVENTS.lock().unwrap().is_empty());
            assert_eq!(DEC009_SCOPE_INITIALIZED.load(Ordering::SeqCst), 0);
            assert_eq!(DEC009_SCOPE_RESOLVED.load(Ordering::SeqCst), 0);
            assert_eq!(DEC009_SCOPE_DISPOSED.load(Ordering::SeqCst), 0);
            assert_eq!(app.container.active_scope_count(), 0);
            assert_eq!(
                app.connection_permits.available_permits(),
                app.server_config().max_connections
            );

            app.container.close().await.expect("close HSK-04 container");
        })
        .await
        .expect("HSK-04 application regression exceeded its bounded deadline");
    }

    #[tokio::test]
    async fn dec_009_partial_101_never_publishes_manager_or_hooks_and_cleans_scope_once() {
        timeout(Duration::from_secs(10), async {
            let _test_lock = OPEN_ORDER_LIFECYCLE_TEST_LOCK.lock().await;
            reset_dec_009_lifecycle_probes();
            let app = dec_009_lifecycle_app().await;
            let response_len = EXPECTED_SWITCHING_PROTOCOLS_RESPONSE.len();

            for (case_index, accepted_bytes) in [0, 1, response_len - 1].into_iter().enumerate() {
                let (stream, capture) = ExactKHandshakeStream::fail_after(
                    valid_upgrade_request("open-order"),
                    accepted_bytes,
                );
                let server_task =
                    spawn_handshake_test_connection(&app, stream, CancellationToken::new());
                let result = timeout(Duration::from_secs(2), server_task)
                    .await
                    .expect("partial HTTP 101 connection deadline")
                    .expect("partial HTTP 101 task did not panic");

                assert!(
                    matches!(
                        result,
                        Err(ServerError::IoError(ref error))
                            if error.kind() == std::io::ErrorKind::Other
                                && error.to_string() == CONTROLLED_WRITE_ERROR
                    ),
                    "the exact-K stream must terminate the handshake with its controlled I/O error"
                );
                assert_eq!(
                    capture.bytes(),
                    EXPECTED_SWITCHING_PROTOCOLS_RESPONSE[..accepted_bytes],
                    "the stream records the exact accepted TCP prefix without claiming rollback"
                );
                assert_eq!(capture.flush_count(), 0);
                assert!(!capture.response_fully_flushed());
                assert_eq!(app.active_connection_count().await, 0);
                assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
                assert_eq!(OPEN_ORDER_CONNECTED_CALLS.load(Ordering::SeqCst), 0);
                assert_eq!(OPEN_ORDER_DISCONNECTED_CALLS.load(Ordering::SeqCst), 0);
                assert!(OPEN_ORDER_EVENTS.lock().unwrap().is_empty());

                let expected_scope_count = case_index + 1;
                assert_eq!(
                    DEC009_SCOPE_INITIALIZED.load(Ordering::SeqCst),
                    expected_scope_count
                );
                assert_eq!(
                    DEC009_SCOPE_RESOLVED.load(Ordering::SeqCst),
                    expected_scope_count
                );
                assert_eq!(
                    DEC009_SCOPE_DISPOSED.load(Ordering::SeqCst),
                    expected_scope_count
                );
                assert_eq!(app.container.active_scope_count(), 0);
                assert_eq!(
                    app.connection_permits.available_permits(),
                    app.server_config().max_connections
                );
            }

            app.container
                .close()
                .await
                .expect("close DEC-009 test container");
        })
        .await
        .expect("DEC-009 partial-write matrix exceeded its bounded deadline");
    }

    #[tokio::test]
    async fn dec_009_full_101_publishes_manager_and_hooks_only_after_flush() {
        timeout(Duration::from_secs(10), async {
            let _test_lock = OPEN_ORDER_LIFECYCLE_TEST_LOCK.lock().await;
            reset_dec_009_lifecycle_probes();
            let app = dec_009_lifecycle_app().await;
            let execution_cancellation = CancellationToken::new();
            let (stream, capture) =
                ExactKHandshakeStream::gated_complete(valid_upgrade_request("open-order"));
            let server_task =
                spawn_handshake_test_connection(&app, stream, execution_cancellation.clone());

            timeout(Duration::from_secs(2), capture.wait_for_flush_started())
                .await
                .expect("complete HTTP 101 reached its gated flush");
            assert_eq!(capture.bytes(), EXPECTED_SWITCHING_PROTOCOLS_RESPONSE);
            assert!(!capture.response_fully_flushed());
            assert_eq!(capture.flush_count(), 0);
            assert_eq!(DEC009_SCOPE_INITIALIZED.load(Ordering::SeqCst), 1);
            assert_eq!(DEC009_SCOPE_RESOLVED.load(Ordering::SeqCst), 1);
            assert_eq!(DEC009_SCOPE_DISPOSED.load(Ordering::SeqCst), 1);
            assert_eq!(app.container.active_scope_count(), 0);
            assert_eq!(app.active_connection_count().await, 0);
            assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
            assert_eq!(OPEN_ORDER_CONNECTED_CALLS.load(Ordering::SeqCst), 0);
            assert_eq!(OPEN_ORDER_DISCONNECTED_CALLS.load(Ordering::SeqCst), 0);
            assert!(OPEN_ORDER_EVENTS.lock().unwrap().is_empty());
            tokio::task::yield_now().await;
            assert_eq!(
                app.active_connection_count().await,
                0,
                "recording every 101 byte is not publication until flush completes"
            );

            capture.release_flush();
            wait_for_dec_009_publication(&app).await;

            assert!(capture.response_fully_flushed());
            assert_eq!(capture.flush_count(), 1);
            assert_eq!(capture.bytes(), EXPECTED_SWITCHING_PROTOCOLS_RESPONSE);
            assert_eq!(app.active_connection_count().await, 1);
            assert_eq!(OPEN_ORDER_CONNECTED_CALLS.load(Ordering::SeqCst), 1);
            assert_eq!(OPEN_ORDER_DISCONNECTED_CALLS.load(Ordering::SeqCst), 0);
            assert_eq!(
                OPEN_ORDER_EVENTS.lock().unwrap().as_slice(),
                ["prefix.admit", "prefix.opened"]
            );

            app.scope_cleanup_registry
                .budget
                .force_before(Instant::now() + Duration::from_millis(400));
            execution_cancellation.cancel();
            timeout(Duration::from_secs(2), server_task)
                .await
                .expect("complete HTTP 101 connection cleanup deadline")
                .expect("complete HTTP 101 task did not panic")
                .expect("execution cancellation must cleanly stop the published connection");

            assert_eq!(app.active_connection_count().await, 0);
            assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
            assert_eq!(OPEN_ORDER_CONNECTED_CALLS.load(Ordering::SeqCst), 1);
            assert_eq!(OPEN_ORDER_DISCONNECTED_CALLS.load(Ordering::SeqCst), 1);
            assert_eq!(
                OPEN_ORDER_EVENTS.lock().unwrap().as_slice(),
                [
                    "prefix.admit",
                    "prefix.opened",
                    "controller.disconnected",
                    "prefix.closed",
                ]
            );
            assert_eq!(DEC009_SCOPE_INITIALIZED.load(Ordering::SeqCst), 1);
            assert_eq!(DEC009_SCOPE_RESOLVED.load(Ordering::SeqCst), 1);
            assert_eq!(DEC009_SCOPE_DISPOSED.load(Ordering::SeqCst), 1);
            assert_eq!(app.container.active_scope_count(), 0);
            assert_eq!(
                app.connection_permits.available_permits(),
                app.server_config().max_connections
            );

            app.container
                .close()
                .await
                .expect("close DEC-009 test container");
        })
        .await
        .expect("DEC-009 complete-write control exceeded its bounded deadline");
    }

    #[tokio::test]
    async fn origin_rejection_does_not_invoke_application_identity() {
        let _test_lock = IDENTITY_PROBE_TEST_LOCK.lock().await;
        let calls = Arc::new(AtomicUsize::new(0));
        *IDENTITY_PROBE_CONFIG.lock().unwrap() = Some(IdentityProbeConfig {
            calls: Arc::clone(&calls),
            started: Arc::new(tokio::sync::Notify::new()),
            release: None,
            decision: IdentityProbeDecision::Accept,
            principal_observed: Arc::new(AtomicBool::new(false)),
            connection_local_observed: Arc::new(AtomicBool::new(false)),
        });
        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allowed_origins: vec!["https://trusted.example".to_owned()],
                allow_missing_origin: false,
                ..ServerConfig::default()
            })
            .identity_middleware::<IdentityProbe>()
            .build()
            .await
            .expect("build origin-before-identity application");
        let (server_io, client_io) = duplex(16 * 1024);
        let permit = app
            .connection_permits
            .clone()
            .try_acquire_owned()
            .expect("connection permit");
        let server_task = tokio::spawn(WsApp::handle_connection(
            server_io,
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: Uuid::new_v4(),
                permit,
                span: tracing::info_span!("test.websocket.origin_before_identity"),
            },
            None,
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
        ));
        let mut request = "ws://localhost/ws?namespace=orders"
            .into_client_request()
            .expect("valid WebSocket client request");
        request.headers_mut().insert(
            tokio_tungstenite::tungstenite::http::header::ORIGIN,
            tokio_tungstenite::tungstenite::http::HeaderValue::from_static(
                "https://untrusted.example",
            ),
        );

        let client_error = tokio_tungstenite::client_async(request, client_io)
            .await
            .expect_err("Origin policy must reject before identity");
        let tokio_tungstenite::tungstenite::Error::Http(response) = client_error else {
            panic!("expected an HTTP Origin rejection");
        };
        assert_eq!(response.status().as_u16(), 403);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            server_task
                .await
                .expect("server task did not panic")
                .is_ok()
        );
        assert_eq!(app.active_connection_count().await, 0);
        app.container.close().await.expect("close test container");
        *IDENTITY_PROBE_CONFIG.lock().unwrap() = None;
    }

    #[tokio::test]
    async fn malformed_trusted_forwarding_data_returns_400_before_identity() {
        let _test_lock = IDENTITY_PROBE_TEST_LOCK.lock().await;
        let calls = Arc::new(AtomicUsize::new(0));
        *IDENTITY_PROBE_CONFIG.lock().unwrap() = Some(IdentityProbeConfig {
            calls: Arc::clone(&calls),
            started: Arc::new(tokio::sync::Notify::new()),
            release: None,
            decision: IdentityProbeDecision::Accept,
            principal_observed: Arc::new(AtomicBool::new(false)),
            connection_local_observed: Arc::new(AtomicBool::new(false)),
        });
        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                trusted_proxy_cidrs: vec!["127.0.0.0/8".parse().unwrap()],
                ..ServerConfig::default()
            })
            .identity_middleware::<IdentityProbe>()
            .build()
            .await
            .expect("build trusted-proxy application");
        let (server_io, client_io) = duplex(16 * 1024);
        let permit = app
            .connection_permits
            .clone()
            .try_acquire_owned()
            .expect("connection permit");
        let server_task = tokio::spawn(WsApp::handle_connection(
            server_io,
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: Uuid::new_v4(),
                permit,
                span: tracing::info_span!("test.websocket.forwarding_before_identity"),
            },
            None,
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
        ));
        let mut request = "ws://localhost/ws?namespace=orders"
            .into_client_request()
            .expect("valid WebSocket client request");
        request.headers_mut().insert(
            tokio_tungstenite::tungstenite::http::HeaderName::from_static("x-forwarded-for"),
            tokio_tungstenite::tungstenite::http::HeaderValue::from_static("not-an-ip"),
        );

        let client_error = tokio_tungstenite::client_async(request, client_io)
            .await
            .expect_err("malformed trusted forwarding metadata must fail closed");
        let tokio_tungstenite::tungstenite::Error::Http(response) = client_error else {
            panic!("expected an HTTP forwarding rejection");
        };
        assert_eq!(response.status().as_u16(), 400);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(
            server_task
                .await
                .expect("server task did not panic")
                .is_ok()
        );
        assert_eq!(app.active_connection_count().await, 0);
        app.container.close().await.expect("close test container");
        *IDENTITY_PROBE_CONFIG.lock().unwrap() = None;
    }

    #[tokio::test]
    async fn identity_rejection_preserves_bounded_http_response() {
        let _test_lock = IDENTITY_PROBE_TEST_LOCK.lock().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let rejection = crate::middleware::WsHandshakeRejection::try_new(
            429,
            MiddlewareErrorCode::new("TEST_IDENTITY_RATE_LIMITED").unwrap(),
        )
        .unwrap()
        .try_with_body("Identity verification is temporarily rate limited.")
        .unwrap()
        .try_with_header(
            tokio_tungstenite::tungstenite::http::header::RETRY_AFTER,
            tokio_tungstenite::tungstenite::http::HeaderValue::from_static("7"),
        )
        .unwrap();
        *IDENTITY_PROBE_CONFIG.lock().unwrap() = Some(IdentityProbeConfig {
            calls: Arc::clone(&calls),
            started: Arc::new(tokio::sync::Notify::new()),
            release: None,
            decision: IdentityProbeDecision::Reject(rejection),
            principal_observed: Arc::new(AtomicBool::new(false)),
            connection_local_observed: Arc::new(AtomicBool::new(false)),
        });
        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .identity_middleware::<IdentityProbe>()
            .build()
            .await
            .expect("build identity-rejection application");
        let (server_io, client_io) = duplex(16 * 1024);
        let permit = app
            .connection_permits
            .clone()
            .try_acquire_owned()
            .expect("connection permit");
        let server_task = tokio::spawn(WsApp::handle_connection(
            server_io,
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: Uuid::new_v4(),
                permit,
                span: tracing::info_span!("test.websocket.identity_rejection"),
            },
            None,
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
        ));

        let client_error =
            tokio_tungstenite::client_async("ws://localhost/ws?namespace=orders", client_io)
                .await
                .expect_err("identity middleware must reject the HTTP Upgrade");
        let tokio_tungstenite::tungstenite::Error::Http(response) = client_error else {
            panic!("expected an HTTP identity rejection");
        };
        assert_eq!(response.status().as_u16(), 429);
        assert_eq!(
            response
                .headers()
                .get(tokio_tungstenite::tungstenite::http::header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("7")
        );
        assert_eq!(
            response.body().as_deref(),
            Some(&b"Identity verification is temporarily rate limited."[..])
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            server_task
                .await
                .expect("server task did not panic")
                .is_ok()
        );
        assert_eq!(app.active_connection_count().await, 0);
        app.container.close().await.expect("close test container");
        *IDENTITY_PROBE_CONFIG.lock().unwrap() = None;
    }

    #[tokio::test]
    async fn async_identity_blocks_upgrade_and_publishes_principal_and_connection_local() {
        let _test_lock = IDENTITY_PROBE_TEST_LOCK.lock().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let principal_observed = Arc::new(AtomicBool::new(false));
        let connection_local_observed = Arc::new(AtomicBool::new(false));
        *IDENTITY_PROBE_CONFIG.lock().unwrap() = Some(IdentityProbeConfig {
            calls: Arc::clone(&calls),
            started: Arc::clone(&started),
            release: Some(Arc::clone(&release)),
            decision: IdentityProbeDecision::Accept,
            principal_observed: Arc::clone(&principal_observed),
            connection_local_observed: Arc::clone(&connection_local_observed),
        });
        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .identity_middleware::<IdentityProbe>()
            .connection_middleware::<IdentityContextProbe>()
            .build()
            .await
            .expect("build asynchronous identity application");
        let (server_io, client_io) = duplex(16 * 1024);
        let permit = app
            .connection_permits
            .clone()
            .try_acquire_owned()
            .expect("connection permit");
        let server_task = tokio::spawn(WsApp::handle_connection(
            server_io,
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: Uuid::new_v4(),
                permit,
                span: tracing::info_span!("test.websocket.async_identity"),
            },
            None,
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
        ));
        let mut client_task = tokio::spawn(async move {
            tokio_tungstenite::client_async("ws://localhost/ws?namespace=orders", client_io).await
        });

        timeout(Duration::from_secs(1), started.notified())
            .await
            .expect("identity middleware start deadline");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            timeout(Duration::from_millis(20), &mut client_task)
                .await
                .is_err(),
            "the client must not observe HTTP 101 while identity is pending"
        );

        release.notify_one();
        let (mut client, response) = timeout(Duration::from_secs(1), client_task)
            .await
            .expect("identity release Upgrade deadline")
            .expect("client task did not panic")
            .expect("identity accepted the HTTP Upgrade");
        assert_eq!(response.status().as_u16(), 101);
        let close = timeout(Duration::from_secs(1), client.next())
            .await
            .expect("post-identity connection close deadline")
            .expect("post-identity connection close frame")
            .expect("read post-identity connection close frame");
        assert!(matches!(close, Message::Close(Some(_))));
        assert!(principal_observed.load(Ordering::SeqCst));
        assert!(connection_local_observed.load(Ordering::SeqCst));
        assert!(
            server_task
                .await
                .expect("server task did not panic")
                .is_err()
        );
        assert_eq!(app.active_connection_count().await, 0);
        app.container.close().await.expect("close test container");
        *IDENTITY_PROBE_CONFIG.lock().unwrap() = None;
    }

    #[tokio::test]
    async fn identity_expiry_interrupts_pending_post_upgrade_admission_before_publication() {
        let _test_lock = IDENTITY_PROBE_TEST_LOCK.lock().await;
        *IDENTITY_PROBE_CONFIG.lock().unwrap() = Some(IdentityProbeConfig {
            calls: Arc::new(AtomicUsize::new(0)),
            started: Arc::new(tokio::sync::Notify::new()),
            release: None,
            decision: IdentityProbeDecision::AcceptExpiring(Duration::from_secs(1)),
            principal_observed: Arc::new(AtomicBool::new(false)),
            connection_local_observed: Arc::new(AtomicBool::new(false)),
        });
        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .identity_middleware::<IdentityProbe>()
            .connection_middleware::<PendingIdentityAdmissionProbe>()
            .build()
            .await
            .expect("build expiring admission application");
        let (server_io, client_io) = duplex(16 * 1024);
        let permit = app
            .connection_permits
            .clone()
            .try_acquire_owned()
            .expect("connection permit");
        let server_task = tokio::spawn(WsApp::handle_connection(
            server_io,
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: Uuid::new_v4(),
                permit,
                span: tracing::info_span!("test.websocket.identity_expiry_during_admission"),
            },
            None,
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
        ));

        let (mut client, response) =
            tokio_tungstenite::client_async("ws://localhost/ws?namespace=orders", client_io)
                .await
                .expect("identity succeeds before HTTP Upgrade");
        assert_eq!(response.status().as_u16(), 101);
        timeout(
            Duration::from_secs(1),
            PENDING_IDENTITY_ADMISSION_STARTED.notified(),
        )
        .await
        .expect("post-upgrade admission starts");
        let close = timeout(Duration::from_secs(2), client.next())
            .await
            .expect("identity-expiry close deadline")
            .expect("identity-expiry close frame")
            .expect("read identity-expiry close frame");
        let Message::Close(Some(frame)) = close else {
            panic!("identity expiry must emit one close frame");
        };
        assert_eq!(
            frame.code,
            crate::request::WsCloseReason::IdentityExpired.code()
        );
        assert_eq!(
            frame.reason.as_ref(),
            crate::request::WsCloseReason::IdentityExpired.reason()
        );
        assert!(
            server_task
                .await
                .expect("server task did not panic")
                .is_err()
        );
        assert_eq!(app.active_connection_count().await, 0);
        app.container.close().await.expect("close test container");
        *IDENTITY_PROBE_CONFIG.lock().unwrap() = None;
    }

    #[tokio::test]
    async fn identity_expiry_interrupts_opened_hook_and_runs_reverse_cleanup() {
        let _test_lock = IDENTITY_PROBE_TEST_LOCK.lock().await;
        PENDING_IDENTITY_OPENED_CLOSE_CATEGORIES
            .lock()
            .unwrap()
            .clear();
        *IDENTITY_PROBE_CONFIG.lock().unwrap() = Some(IdentityProbeConfig {
            calls: Arc::new(AtomicUsize::new(0)),
            started: Arc::new(tokio::sync::Notify::new()),
            release: None,
            decision: IdentityProbeDecision::AcceptExpiring(Duration::from_secs(1)),
            principal_observed: Arc::new(AtomicBool::new(false)),
            connection_local_observed: Arc::new(AtomicBool::new(false)),
        });
        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .identity_middleware::<IdentityProbe>()
            .connection_middleware::<PendingIdentityOpenedProbe>()
            .build()
            .await
            .expect("build expiring opened-hook application");
        let (server_io, client_io) = duplex(16 * 1024);
        let permit = app
            .connection_permits
            .clone()
            .try_acquire_owned()
            .expect("connection permit");
        let server_task = tokio::spawn(WsApp::handle_connection(
            server_io,
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: Uuid::new_v4(),
                permit,
                span: tracing::info_span!("test.websocket.identity_expiry_during_opened"),
            },
            None,
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
        ));

        let (mut client, response) =
            tokio_tungstenite::client_async("ws://localhost/ws?namespace=orders", client_io)
                .await
                .expect("identity succeeds before HTTP Upgrade");
        assert_eq!(response.status().as_u16(), 101);
        timeout(
            Duration::from_secs(1),
            PENDING_IDENTITY_OPENED_STARTED.notified(),
        )
        .await
        .expect("opened hook starts");
        let close = timeout(Duration::from_secs(2), client.next())
            .await
            .expect("identity-expiry close deadline")
            .expect("identity-expiry close frame")
            .expect("read identity-expiry close frame");
        let Message::Close(Some(frame)) = close else {
            panic!("identity expiry must emit one close frame");
        };
        assert_eq!(
            frame.reason.as_ref(),
            crate::request::WsCloseReason::IdentityExpired.reason()
        );
        assert!(
            server_task
                .await
                .expect("server task did not panic")
                .is_err()
        );
        assert_eq!(app.active_connection_count().await, 0);
        assert_eq!(
            *PENDING_IDENTITY_OPENED_CLOSE_CATEGORIES.lock().unwrap(),
            [crate::middleware::WsConnectionCloseCategory::IdentityExpired]
        );
        app.container.close().await.expect("close test container");
        *IDENTITY_PROBE_CONFIG.lock().unwrap() = None;
    }

    #[tokio::test]
    async fn custom_handshake_middleware_rejects_before_upgrade_and_manager_admission() {
        async fn assert_rejection<const STATUS: u16>() {
            let app = WsAppBuilder::new("127.0.0.1:0")
                .config(ServerConfig {
                    allow_missing_origin: true,
                    ..ServerConfig::default()
                })
                .handshake_middleware::<RejectHandshakeMiddleware<STATUS>>()
                .build()
                .await
                .expect("build handshake-policy application");
            let (server_io, client_io) = duplex(16 * 1024);
            let permit = app
                .connection_permits
                .clone()
                .try_acquire_owned()
                .expect("connection permit");
            let server_task = tokio::spawn(WsApp::handle_connection(
                server_io,
                app.connection_runtime(),
                AcceptedConnection {
                    addr: "127.0.0.1:12345".parse().unwrap(),
                    connection_id: Uuid::new_v4(),
                    permit,
                    span: tracing::info_span!("test.websocket.rejected_handshake"),
                },
                None,
                CancellationToken::new(),
                CancellationToken::new(),
                CancellationToken::new(),
            ));

            let client_error =
                tokio_tungstenite::client_async("ws://localhost/ws?namespace=orders", client_io)
                    .await
                    .expect_err("policy must reject the HTTP Upgrade");
            let tokio_tungstenite::tungstenite::Error::Http(response) = client_error else {
                panic!("expected an HTTP rejection");
            };
            assert_eq!(response.status().as_u16(), STATUS);
            let server_result = server_task.await.expect("server task did not panic");
            assert!(server_result.is_ok());
            assert_eq!(app.active_connection_count().await, 0);
            assert_eq!(
                app.connection_permits.available_permits(),
                app.server_config().max_connections
            );
            app.container.close().await.expect("close test container");
        }

        assert_rejection::<400>().await;
        assert_rejection::<401>().await;
        assert_rejection::<403>().await;
    }

    #[tokio::test]
    async fn endpoint_and_unknown_namespace_are_rejected_before_upgrade() {
        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                endpoint_path: "/socket".into(),
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .build()
            .await
            .expect("build endpoint-policy application");

        for (url, expected_status) in [
            ("ws://localhost/ws?namespace=%2F", 404),
            ("ws://localhost/socket?namespace=%2F", 400),
            (
                "ws://localhost/socket?namespace=definitely_missing_namespace",
                404,
            ),
        ] {
            let (server_io, client_io) = duplex(16 * 1024);
            let permit = app
                .connection_permits
                .clone()
                .try_acquire_owned()
                .expect("connection permit");
            let server_task = tokio::spawn(WsApp::handle_connection(
                server_io,
                app.connection_runtime(),
                AcceptedConnection {
                    addr: "127.0.0.1:12345".parse().unwrap(),
                    connection_id: Uuid::new_v4(),
                    permit,
                    span: tracing::info_span!("test.websocket.rejected_target"),
                },
                None,
                CancellationToken::new(),
                CancellationToken::new(),
                CancellationToken::new(),
            ));

            let client_error = tokio_tungstenite::client_async(url, client_io)
                .await
                .expect_err("target must reject the HTTP Upgrade");
            let tokio_tungstenite::tungstenite::Error::Http(response) = client_error else {
                panic!("expected an HTTP rejection");
            };
            assert_eq!(response.status().as_u16(), expected_status);
            assert!(
                server_task
                    .await
                    .expect("server task did not panic")
                    .is_ok()
            );
        }

        assert_eq!(app.active_connection_count().await, 0);
        app.container.close().await.expect("close test container");
    }

    #[tokio::test]
    async fn async_admission_rejection_never_enters_manager_or_rejecting_layer_cleanup() {
        let _test_lock = CONNECTION_PROBE_TEST_LOCK.lock().await;
        let admit_calls = Arc::new(AtomicUsize::new(0));
        let opened_calls = Arc::new(AtomicUsize::new(0));
        let closed_calls = Arc::new(AtomicUsize::new(0));
        let close_categories = Arc::new(StdMutex::new(Vec::new()));
        *CONNECTION_PROBE_CONFIG.lock().unwrap() = Some(ConnectionLifecycleProbeConfig {
            reject_admission: true,
            admit_calls: Arc::clone(&admit_calls),
            opened_calls: Arc::clone(&opened_calls),
            closed_calls: Arc::clone(&closed_calls),
            close_categories: Arc::clone(&close_categories),
        });
        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .connection_middleware::<ConnectionLifecycleProbe>()
            .build()
            .await
            .expect("build admission-policy application");
        let (server_io, client_io) = duplex(16 * 1024);
        let permit = app
            .connection_permits
            .clone()
            .try_acquire_owned()
            .expect("connection permit");
        let server_task = tokio::spawn(WsApp::handle_connection(
            server_io,
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: Uuid::new_v4(),
                permit,
                span: tracing::info_span!("test.websocket.rejected_admission"),
            },
            None,
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
        ));

        let (mut client, response) =
            tokio_tungstenite::client_async("ws://localhost/ws?namespace=orders", client_io)
                .await
                .expect("HTTP Upgrade succeeds before async admission");
        assert_eq!(response.status().as_u16(), 101);
        let close = timeout(Duration::from_secs(1), client.next())
            .await
            .expect("policy close deadline")
            .expect("policy close frame")
            .expect("read policy close frame");
        assert!(matches!(close, Message::Close(Some(_))));
        assert!(
            server_task
                .await
                .expect("server task did not panic")
                .is_err()
        );
        assert_eq!(app.active_connection_count().await, 0);
        assert_eq!(admit_calls.load(Ordering::SeqCst), 1);
        assert_eq!(opened_calls.load(Ordering::SeqCst), 0);
        // A rejecting middleware did not successfully enter; only a prior
        // successful prefix would receive reverse cleanup.
        assert_eq!(closed_calls.load(Ordering::SeqCst), 0);
        assert!(close_categories.lock().unwrap().is_empty());
        app.container.close().await.expect("close test container");
        *CONNECTION_PROBE_CONFIG.lock().unwrap() = None;
    }

    #[tokio::test]
    async fn opened_failure_skips_user_lifecycle_but_runs_framework_reverse_cleanup() {
        let _test_lock = OPEN_ORDER_LIFECYCLE_TEST_LOCK.lock().await;
        OPEN_ORDER_CONNECTED_CALLS.store(0, Ordering::SeqCst);
        OPEN_ORDER_DISCONNECTED_CALLS.store(0, Ordering::SeqCst);
        OPEN_ORDER_EVENTS.lock().unwrap().clear();
        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .connection_middleware::<OrderedOpenProbe<0>>()
            .connection_middleware::<OrderedOpenProbe<1>>()
            .build()
            .await
            .expect("build opened-failure application");
        let (server_io, client_io) = duplex(16 * 1024);
        let permit = app
            .connection_permits
            .clone()
            .try_acquire_owned()
            .expect("connection permit");
        let server_task = tokio::spawn(WsApp::handle_connection(
            server_io,
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: Uuid::new_v4(),
                permit,
                span: tracing::info_span!("test.websocket.rejected_open"),
            },
            None,
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
        ));

        let (mut client, response) =
            tokio_tungstenite::client_async("ws://localhost/ws?namespace=open-order", client_io)
                .await
                .expect("HTTP Upgrade succeeds before on_open");
        assert_eq!(response.status().as_u16(), 101);
        let close = timeout(Duration::from_secs(1), client.next())
            .await
            .expect("opened rejection close deadline")
            .expect("opened rejection close frame")
            .expect("read opened rejection close frame");
        assert!(matches!(close, Message::Close(Some(_))));
        assert!(
            server_task
                .await
                .expect("server task did not panic")
                .is_err()
        );

        assert_eq!(OPEN_ORDER_CONNECTED_CALLS.load(Ordering::SeqCst), 0);
        assert_eq!(OPEN_ORDER_DISCONNECTED_CALLS.load(Ordering::SeqCst), 0);
        assert_eq!(
            OPEN_ORDER_EVENTS.lock().unwrap().as_slice(),
            [
                "prefix.admit",
                "rejecting.admit",
                "prefix.opened",
                "rejecting.opened",
                "rejecting.closed",
                "prefix.closed",
            ]
        );
        assert_eq!(app.active_connection_count().await, 0);
        assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
        app.container.close().await.expect("close test container");
    }

    #[tokio::test]
    async fn long_action_keeps_heartbeat_pong_responsive_and_dispatches_read_ahead_sequentially() {
        let _test_lock = CONTROL_READER_TEST_LOCK.lock().await;
        CONTROL_READER_ACTION_STARTED.store(false, Ordering::SeqCst);
        CONTROL_READER_ACTION_COMPLETED.store(false, Ordering::SeqCst);
        CONTROL_READER_DEFERRED_ACTION_INVOKED.store(false, Ordering::SeqCst);

        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                ping_interval_secs: 1,
                pong_timeout_secs: 1,
                ..ServerConfig::default()
            })
            .build()
            .await
            .expect("build long-action heartbeat application");
        let (server_io, client_io) = duplex(64 * 1024);
        let permit = app
            .connection_permits
            .clone()
            .try_acquire_owned()
            .expect("connection permit");
        let server_task = tokio::spawn(WsApp::handle_connection(
            server_io,
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: Uuid::new_v4(),
                permit,
                span: tracing::info_span!("test.websocket.long_action_heartbeat"),
            },
            None,
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
        ));
        let (mut client, response) =
            tokio_tungstenite::client_async("ws://localhost/ws?namespace=orders", client_io)
                .await
                .expect("long-action heartbeat Upgrade");
        assert_eq!(response.status().as_u16(), 101);

        client
            .send(
                crate::request::WsMessageBody::try_new(
                    "orders:long-control-pong",
                    serde_json::Value::Null,
                )
                .unwrap()
                .with_namespace("orders".into())
                .to_message()
                .unwrap(),
            )
            .await
            .expect("send long action");
        timeout(Duration::from_secs(1), async {
            while !CONTROL_READER_ACTION_STARTED.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("long action start deadline");
        client
            .send(
                crate::request::WsMessageBody::try_new(
                    "orders:after-long-control",
                    serde_json::Value::Null,
                )
                .unwrap()
                .with_namespace("orders".into())
                .to_message()
                .unwrap(),
            )
            .await
            .expect("send one bounded read-ahead action");

        let terminal = timeout(Duration::from_secs(4), async {
            loop {
                let frame = client
                    .next()
                    .await
                    .expect("server remains connected")
                    .expect("read server frame");
                match frame {
                    Message::Ping(payload) => {
                        client
                            .send(Message::Pong(payload))
                            .await
                            .expect("answer heartbeat while action is active");
                    }
                    Message::Text(text) => {
                        let envelope: serde_json::Value = serde_json::from_str(&text).unwrap();
                        if envelope["event"] == "orders:long-control-complete" {
                            break envelope;
                        }
                    }
                    Message::Close(frame) => {
                        panic!("healthy Pong was not observed during the long action: {frame:?}");
                    }
                    Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
                }
            }
        })
        .await
        .expect("long action terminal deadline");
        assert_eq!(terminal["data"]["completed"], true);
        assert!(CONTROL_READER_ACTION_COMPLETED.load(Ordering::SeqCst));
        timeout(Duration::from_secs(1), async {
            while !CONTROL_READER_DEFERRED_ACTION_INVOKED.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("deferred sequential action deadline");

        client
            .send(Message::Close(None))
            .await
            .expect("close heartbeat test client");
        let close = timeout(Duration::from_secs(1), client.next())
            .await
            .expect("peer Close acknowledgement deadline");
        assert!(matches!(close, Some(Ok(Message::Close(_)))));
        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("heartbeat server deadline")
            .expect("heartbeat server task panicked")
            .expect("heartbeat connection failed");
        assert_eq!(app.active_connection_count().await, 0);
        assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
        app.container.close().await.expect("close test container");
    }

    #[tokio::test]
    async fn client_ping_during_pending_action_emits_exactly_one_pong() {
        let _test_lock = CONTROL_READER_TEST_LOCK.lock().await;
        CONTROL_READER_ACTION_STARTED.store(false, Ordering::SeqCst);
        CONTROL_READER_ACTION_DROPPED.store(false, Ordering::SeqCst);

        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                ping_interval_secs: 60,
                ..ServerConfig::default()
            })
            .build()
            .await
            .expect("build active-action Pong application");
        let (server_io, client_io) = duplex(64 * 1024);
        let permit = app
            .connection_permits
            .clone()
            .try_acquire_owned()
            .expect("connection permit");
        let server_task = tokio::spawn(WsApp::handle_connection(
            server_io,
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: Uuid::new_v4(),
                permit,
                span: tracing::info_span!("test.websocket.active_action_client_ping"),
            },
            None,
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
        ));
        let (mut client, response) =
            tokio_tungstenite::client_async("ws://localhost/ws?namespace=orders", client_io)
                .await
                .expect("active-action Pong Upgrade");
        assert_eq!(response.status().as_u16(), 101);
        client
            .send(
                crate::request::WsMessageBody::try_new(
                    "orders:long-control-pending",
                    serde_json::Value::Null,
                )
                .unwrap()
                .with_namespace("orders".into())
                .to_message()
                .unwrap(),
            )
            .await
            .expect("send pending action");
        timeout(Duration::from_secs(1), async {
            while !CONTROL_READER_ACTION_STARTED.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pending action start deadline");

        let payload = b"active-action-ping".to_vec();
        client
            .send(Message::Ping(payload.clone()))
            .await
            .expect("send Ping while action is pending");
        let pong = timeout(Duration::from_millis(500), client.next())
            .await
            .expect("automatic Pong deadline")
            .expect("connection remains open for Pong")
            .expect("read automatic Pong");
        assert!(matches!(pong, Message::Pong(observed) if observed == payload));
        assert!(
            timeout(Duration::from_millis(200), client.next())
                .await
                .is_err(),
            "a client Ping must not leave a second Pong queued"
        );

        client
            .send(Message::Close(None))
            .await
            .expect("close active-action Pong client");
        let close = timeout(Duration::from_millis(500), client.next())
            .await
            .expect("peer Close acknowledgement deadline");
        assert!(matches!(close, Some(Ok(Message::Close(_)))));
        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("active-action Pong server deadline")
            .expect("active-action Pong server task panicked")
            .expect("active-action Pong connection failed");
        assert!(CONTROL_READER_ACTION_DROPPED.load(Ordering::SeqCst));
        assert_eq!(app.active_connection_count().await, 0);
        assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
        app.container.close().await.expect("close test container");
    }

    #[tokio::test]
    async fn rfc_violation_during_pending_action_emits_1002_and_cleans_scope() {
        let _control_lock = CONTROL_READER_TEST_LOCK.lock().await;
        let _connection_lock = CONNECTION_PROBE_TEST_LOCK.lock().await;
        CONTROL_READER_ACTION_STARTED.store(false, Ordering::SeqCst);
        CONTROL_READER_ACTION_DROPPED.store(false, Ordering::SeqCst);
        let admit_calls = Arc::new(AtomicUsize::new(0));
        let opened_calls = Arc::new(AtomicUsize::new(0));
        let closed_calls = Arc::new(AtomicUsize::new(0));
        let close_categories = Arc::new(StdMutex::new(Vec::new()));
        *CONNECTION_PROBE_CONFIG.lock().unwrap() = Some(ConnectionLifecycleProbeConfig {
            reject_admission: false,
            admit_calls: Arc::clone(&admit_calls),
            opened_calls: Arc::clone(&opened_calls),
            closed_calls: Arc::clone(&closed_calls),
            close_categories: Arc::clone(&close_categories),
        });

        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                ping_interval_secs: 60,
                ..ServerConfig::default()
            })
            .connection_middleware::<ConnectionLifecycleProbe>()
            .build()
            .await
            .expect("build active-action RFC violation application");
        let (server_io, client_io) = duplex(64 * 1024);
        let permit = app
            .connection_permits
            .clone()
            .try_acquire_owned()
            .expect("connection permit");
        let server_task = tokio::spawn(WsApp::handle_connection(
            server_io,
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: Uuid::new_v4(),
                permit,
                span: tracing::info_span!("test.websocket.active_action_rfc_violation"),
            },
            None,
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
        ));
        let (mut client, response) =
            tokio_tungstenite::client_async("ws://localhost/ws?namespace=orders", client_io)
                .await
                .expect("active-action RFC violation Upgrade");
        assert_eq!(response.status().as_u16(), 101);
        client
            .send(
                crate::request::WsMessageBody::try_new(
                    "orders:long-control-pending",
                    serde_json::Value::Null,
                )
                .unwrap()
                .with_namespace("orders".into())
                .to_message()
                .unwrap(),
            )
            .await
            .expect("send pending action");
        timeout(Duration::from_secs(1), async {
            while !CONTROL_READER_ACTION_STARTED.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pending action start deadline");

        // Bypass the client encoder so the server receives an unmasked Text
        // frame, which is forbidden for a client endpoint by RFC 6455.
        client
            .get_mut()
            .write_all(&[0x81, 0x00])
            .await
            .expect("write unmasked frame during action dispatch");
        let close = timeout(Duration::from_millis(500), client.next())
            .await
            .expect("RFC 1002 Close deadline")
            .expect("server must emit an explicit Close")
            .expect("read RFC 1002 Close");
        assert!(matches!(
            close,
            Message::Close(Some(frame))
                if frame.code
                    == tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Protocol
                    && frame.reason.is_empty()
        ));
        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("active-action RFC server deadline")
            .expect("active-action RFC server task panicked")
            .expect("active-action RFC connection failed");

        assert!(CONTROL_READER_ACTION_DROPPED.load(Ordering::SeqCst));
        assert_eq!(admit_calls.load(Ordering::SeqCst), 1);
        assert_eq!(opened_calls.load(Ordering::SeqCst), 1);
        assert_eq!(closed_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            close_categories.lock().unwrap().as_slice(),
            [crate::middleware::WsConnectionCloseCategory::ProtocolError]
        );
        assert_eq!(app.active_connection_count().await, 0);
        assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
        app.container.close().await.expect("close test container");
        *CONNECTION_PROBE_CONFIG.lock().unwrap() = None;
    }

    #[tokio::test]
    async fn peer_close_during_long_action_is_acked_and_cleans_the_action_scope() {
        let _control_lock = CONTROL_READER_TEST_LOCK.lock().await;
        let _connection_lock = CONNECTION_PROBE_TEST_LOCK.lock().await;
        CONTROL_READER_ACTION_STARTED.store(false, Ordering::SeqCst);
        CONTROL_READER_ACTION_DROPPED.store(false, Ordering::SeqCst);
        let admit_calls = Arc::new(AtomicUsize::new(0));
        let opened_calls = Arc::new(AtomicUsize::new(0));
        let closed_calls = Arc::new(AtomicUsize::new(0));
        let close_categories = Arc::new(StdMutex::new(Vec::new()));
        *CONNECTION_PROBE_CONFIG.lock().unwrap() = Some(ConnectionLifecycleProbeConfig {
            reject_admission: false,
            admit_calls: Arc::clone(&admit_calls),
            opened_calls: Arc::clone(&opened_calls),
            closed_calls: Arc::clone(&closed_calls),
            close_categories: Arc::clone(&close_categories),
        });

        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .connection_middleware::<ConnectionLifecycleProbe>()
            .build()
            .await
            .expect("build long-action peer-close application");
        let (server_io, client_io) = duplex(64 * 1024);
        let permit = app
            .connection_permits
            .clone()
            .try_acquire_owned()
            .expect("connection permit");
        let server_task = tokio::spawn(WsApp::handle_connection(
            server_io,
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: Uuid::new_v4(),
                permit,
                span: tracing::info_span!("test.websocket.long_action_peer_close"),
            },
            None,
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
        ));
        let (mut client, response) =
            tokio_tungstenite::client_async("ws://localhost/ws?namespace=orders", client_io)
                .await
                .expect("long-action peer-close Upgrade");
        assert_eq!(response.status().as_u16(), 101);
        client
            .send(
                crate::request::WsMessageBody::try_new(
                    "orders:long-control-pending",
                    serde_json::Value::Null,
                )
                .unwrap()
                .with_namespace("orders".into())
                .to_message()
                .unwrap(),
            )
            .await
            .expect("send pending action");
        timeout(Duration::from_secs(1), async {
            while !CONTROL_READER_ACTION_STARTED.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("pending action start deadline");

        client
            .send(Message::Close(None))
            .await
            .expect("send peer Close during action");
        let close = timeout(Duration::from_millis(500), client.next())
            .await
            .expect("peer Close must be acknowledged while action is pending");
        assert!(matches!(close, Some(Ok(Message::Close(_)))));
        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("peer-close server deadline")
            .expect("peer-close server task panicked")
            .expect("peer-close connection failed");

        assert!(CONTROL_READER_ACTION_DROPPED.load(Ordering::SeqCst));
        assert_eq!(admit_calls.load(Ordering::SeqCst), 1);
        assert_eq!(opened_calls.load(Ordering::SeqCst), 1);
        assert_eq!(closed_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            close_categories.lock().unwrap().as_slice(),
            [crate::middleware::WsConnectionCloseCategory::NormalPeer]
        );
        assert_eq!(app.active_connection_count().await, 0);
        assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
        app.container.close().await.expect("close test container");
        *CONNECTION_PROBE_CONFIG.lock().unwrap() = None;
    }

    #[tokio::test]
    async fn long_action_read_ahead_saturation_fails_closed_without_dispatching_queued_work() {
        let _test_lock = CONTROL_READER_TEST_LOCK.lock().await;
        CONTROL_READER_ACTION_STARTED.store(false, Ordering::SeqCst);
        CONTROL_READER_ACTION_DROPPED.store(false, Ordering::SeqCst);
        CONTROL_READER_DEFERRED_ACTION_INVOKED.store(false, Ordering::SeqCst);

        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                inbound_queue_capacity: 1,
                ..ServerConfig::default()
            })
            .build()
            .await
            .expect("build read-ahead saturation application");
        let (server_io, client_io) = duplex(64 * 1024);
        let permit = app
            .connection_permits
            .clone()
            .try_acquire_owned()
            .expect("connection permit");
        let server_task = tokio::spawn(WsApp::handle_connection(
            server_io,
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: Uuid::new_v4(),
                permit,
                span: tracing::info_span!("test.websocket.read_ahead_saturation"),
            },
            None,
            CancellationToken::new(),
            CancellationToken::new(),
            CancellationToken::new(),
        ));
        let (mut client, response) =
            tokio_tungstenite::client_async("ws://localhost/ws?namespace=orders", client_io)
                .await
                .expect("read-ahead saturation Upgrade");
        assert_eq!(response.status().as_u16(), 101);

        for event in [
            "orders:long-control-pending",
            "orders:after-long-control",
            "orders:after-long-control",
        ] {
            client
                .send(
                    crate::request::WsMessageBody::try_new(event, serde_json::Value::Null)
                        .unwrap()
                        .with_namespace("orders".into())
                        .to_message()
                        .unwrap(),
                )
                .await
                .expect("send saturation fixture");
            if event == "orders:long-control-pending" {
                timeout(Duration::from_secs(1), async {
                    while !CONTROL_READER_ACTION_STARTED.load(Ordering::SeqCst) {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("pending action start deadline");
            }
        }

        let close = timeout(Duration::from_secs(1), client.next())
            .await
            .expect("saturated read-ahead close deadline")
            .expect("saturated read-ahead close frame")
            .expect("read saturated read-ahead close frame");
        assert!(matches!(
            close,
            Message::Close(Some(frame))
                if frame.code == crate::request::WsCloseReason::PolicyViolation.code()
                    && frame.reason == crate::request::WsCloseReason::PolicyViolation.reason()
        ));
        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("saturation server deadline")
            .expect("saturation server task panicked")
            .expect("saturation connection failed");
        assert!(CONTROL_READER_ACTION_DROPPED.load(Ordering::SeqCst));
        assert!(!CONTROL_READER_DEFERRED_ACTION_INVOKED.load(Ordering::SeqCst));
        assert_eq!(app.active_connection_count().await, 0);
        assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
        app.container.close().await.expect("close test container");
    }

    #[tokio::test]
    async fn graceful_shutdown_drains_started_action_before_close_and_rejects_next_message() {
        let _test_lock = GRACEFUL_DRAIN_TEST_LOCK.lock().await;
        GRACEFUL_DRAIN_ACTION_STARTED.store(false, Ordering::SeqCst);
        GRACEFUL_DRAIN_ACTION_COMPLETED.store(false, Ordering::SeqCst);
        GRACEFUL_DRAIN_ACTION_DROPPED.store(false, Ordering::SeqCst);
        GRACEFUL_DRAIN_MIDDLEWARE_BEFORE.store(false, Ordering::SeqCst);
        GRACEFUL_DRAIN_MIDDLEWARE_AFTER.store(false, Ordering::SeqCst);
        POST_SHUTDOWN_ACTION_INVOKED.store(false, Ordering::SeqCst);

        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .build()
            .await
            .expect("build graceful-drain application");
        let (server_io, client_io) = duplex(64 * 1024);
        let permit = app
            .connection_permits
            .clone()
            .try_acquire_owned()
            .expect("connection permit");
        let message_admission = CancellationToken::new();
        let server_message_admission = message_admission.clone();
        let server_task = tokio::spawn(WsApp::handle_connection(
            server_io,
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: Uuid::new_v4(),
                permit,
                span: tracing::info_span!("test.websocket.graceful_drain"),
            },
            None,
            CancellationToken::new(),
            server_message_admission,
            CancellationToken::new(),
        ));
        let (mut client, response) =
            tokio_tungstenite::client_async("ws://localhost/ws?namespace=orders", client_io)
                .await
                .expect("graceful-drain Upgrade");
        assert_eq!(response.status().as_u16(), 101);

        client
            .send(
                crate::request::WsMessageBody::try_new(
                    "orders:shutdown-drain",
                    serde_json::Value::Null,
                )
                .unwrap()
                .with_namespace("orders".into())
                .to_message()
                .unwrap(),
            )
            .await
            .expect("send started action");
        timeout(Duration::from_secs(1), async {
            while !GRACEFUL_DRAIN_ACTION_STARTED.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("started action observation deadline");
        client
            .send(
                crate::request::WsMessageBody::try_new(
                    "orders:after-shutdown",
                    serde_json::Value::Null,
                )
                .unwrap()
                .with_namespace("orders".into())
                .to_message()
                .unwrap(),
            )
            .await
            .expect("queue a second message before the admission barrier");

        message_admission.cancel();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!GRACEFUL_DRAIN_ACTION_COMPLETED.load(Ordering::SeqCst));
        assert!(!GRACEFUL_DRAIN_ACTION_DROPPED.load(Ordering::SeqCst));

        let terminal = timeout(Duration::from_secs(2), client.next())
            .await
            .expect("drained action terminal deadline")
            .expect("drained action terminal frame")
            .expect("read drained action terminal frame");
        let Message::Text(terminal) = terminal else {
            panic!("the completed action terminal must precede shutdown Close");
        };
        let terminal: serde_json::Value = serde_json::from_str(&terminal).unwrap();
        assert_eq!(terminal["event"], "orders:shutdown-drained");
        assert_eq!(terminal["data"]["drained"], true);

        let close = timeout(Duration::from_secs(2), client.next())
            .await
            .expect("shutdown Close deadline")
            .expect("shutdown Close frame")
            .expect("read shutdown Close frame");
        assert!(matches!(
            close,
            Message::Close(Some(frame))
                if frame.code == crate::request::WsCloseReason::ServerShutdown.code()
                    && frame.reason == crate::request::WsCloseReason::ServerShutdown.reason()
        ));
        assert!(GRACEFUL_DRAIN_ACTION_COMPLETED.load(Ordering::SeqCst));
        assert!(GRACEFUL_DRAIN_ACTION_DROPPED.load(Ordering::SeqCst));
        assert!(GRACEFUL_DRAIN_MIDDLEWARE_BEFORE.load(Ordering::SeqCst));
        assert!(GRACEFUL_DRAIN_MIDDLEWARE_AFTER.load(Ordering::SeqCst));
        assert!(!POST_SHUTDOWN_ACTION_INVOKED.load(Ordering::SeqCst));
        assert!(
            !server_task.is_finished(),
            "server must keep the transport until the peer acknowledges Close"
        );
        client
            .flush()
            .await
            .expect("flush shutdown Close acknowledgement");
        timeout(Duration::from_secs(2), server_task)
            .await
            .expect("graceful-drain server deadline")
            .expect("graceful-drain task panicked")
            .expect("graceful-drain connection failed");

        assert_eq!(app.active_connection_count().await, 0);
        assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
        assert_eq!(
            app.connection_permits.available_permits(),
            app.server_config().max_connections
        );
        app.container.close().await.expect("close test container");
    }

    #[tokio::test]
    async fn shutdown_close_handshake_is_bounded_when_peer_does_not_acknowledge() {
        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                write_timeout_millis: 100,
                ..ServerConfig::default()
            })
            .build()
            .await
            .expect("build close-timeout application");
        let (server_io, client_io) = duplex(64 * 1024);
        let permit = app
            .connection_permits
            .clone()
            .try_acquire_owned()
            .expect("connection permit");
        let message_admission = CancellationToken::new();
        let server_message_admission = message_admission.clone();
        let server_task = tokio::spawn(WsApp::handle_connection(
            server_io,
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: Uuid::new_v4(),
                permit,
                span: tracing::info_span!("test.websocket.close_timeout"),
            },
            None,
            CancellationToken::new(),
            server_message_admission,
            CancellationToken::new(),
        ));
        let (mut client, response) =
            tokio_tungstenite::client_async("ws://localhost/ws?namespace=orders", client_io)
                .await
                .expect("close-timeout Upgrade");
        assert_eq!(response.status().as_u16(), 101);

        message_admission.cancel();
        let close = timeout(Duration::from_secs(1), client.next())
            .await
            .expect("shutdown Close deadline")
            .expect("shutdown Close frame")
            .expect("read shutdown Close frame");
        assert!(matches!(
            close,
            Message::Close(Some(frame))
                if frame.code == crate::request::WsCloseReason::ServerShutdown.code()
                    && frame.reason == crate::request::WsCloseReason::ServerShutdown.reason()
        ));
        // Deliberately do not flush Tungstenite's automatically queued Close
        // response. The server must force-drop this one transport after the
        // existing bounded write/close budget.
        timeout(Duration::from_secs(1), server_task)
            .await
            .expect("non-acknowledging peer must not hold the server indefinitely")
            .expect("close-timeout task panicked")
            .expect("close-timeout connection failed");
        assert_eq!(app.metrics_snapshot().timeouts, 1);
        assert_eq!(app.active_connection_count().await, 0);
        assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
        assert_eq!(
            app.connection_permits.available_permits(),
            app.server_config().max_connections
        );
        drop(client);
        app.container.close().await.expect("close test container");
    }

    #[tokio::test]
    async fn server_force_reports_incomplete_disconnect_and_reconciles_manager_registry_and_permit()
    {
        let _test_lock = OPEN_ORDER_LIFECYCLE_TEST_LOCK.lock().await;
        OPEN_ORDER_CONNECTED_CALLS.store(0, Ordering::SeqCst);
        OPEN_ORDER_DISCONNECTED_CALLS.store(0, Ordering::SeqCst);
        OPEN_ORDER_FORCE_DISCONNECT_DROPPED.store(false, Ordering::SeqCst);
        OPEN_ORDER_EVENTS.lock().unwrap().clear();
        let _force_mode = OpenOrderForcePendingGuard::enable();

        let address = reserve_loopback_address();
        let app = WsAppBuilder::new(&address.to_string())
            .config(ServerConfig {
                allow_missing_origin: true,
                max_connections: 1,
                ping_interval_secs: 60,
                ..ServerConfig::default()
            })
            .build()
            .await
            .expect("build force-reconciliation WS application");
        let admission = CancellationToken::new();
        let message_admission = CancellationToken::new();
        let force = CancellationToken::new();
        let runtime = app.runtime_clone();
        let server_admission = admission.clone();
        let server_message_admission = message_admission.clone();
        let server_force = force.clone();
        let task = tokio::spawn(async move {
            runtime
                .serve_until_shutdown(server_admission, server_message_admission, server_force)
                .await
        });
        let server = Arc::new(tokio::sync::Mutex::new(ManagedWsServer {
            admission: admission.clone(),
            message_admission: message_admission.clone(),
            force: force.clone(),
            task: task.into(),
            outcome: None,
            runtime_result_observed: false,
        }));
        let mut force_handle = WsServerLifecycleHandle {
            phase: FrameworkShutdownPhase::DrainInFlight,
            server,
            admission,
            message_admission,
            force,
            timeout: Duration::from_secs(2),
            budget: Default::default(),
        };

        let tcp = connect_loopback_when_ready(address).await;
        let (client, response) =
            tokio_tungstenite::client_async(format!("ws://{address}/ws?namespace=open-order"), tcp)
                .await
                .expect("force-reconciliation WS Upgrade");
        assert_eq!(
            response.status(),
            tokio_tungstenite::tungstenite::http::StatusCode::SWITCHING_PROTOCOLS
        );
        timeout(Duration::from_secs(1), async {
            loop {
                if OPEN_ORDER_CONNECTED_CALLS.load(Ordering::SeqCst) == 1
                    && app.active_connection_count().await == 1
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("published force-reconciliation connection deadline");

        let force_future = force_handle
            .force_shutdown()
            .expect("WebSocket server drain exposes a force handle");
        let failure = timeout(Duration::from_secs(2), force_future)
            .await
            .expect("WebSocket server force handle remained pending")
            .expect_err("joined owners cannot turn an incomplete user cleanup into success");
        assert!(failure.to_string().contains("controller lifecycle failure"));

        assert_eq!(OPEN_ORDER_DISCONNECTED_CALLS.load(Ordering::SeqCst), 1);
        assert!(OPEN_ORDER_FORCE_DISCONNECT_DROPPED.load(Ordering::SeqCst));
        assert_eq!(
            OPEN_ORDER_EVENTS.lock().unwrap().as_slice(),
            [
                "controller.disconnected_started",
                "controller.disconnected_dropped"
            ]
        );
        assert_eq!(app.active_connection_count().await, 0);
        assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
        assert_eq!(app.connection_permits.available_permits(), 1);

        drop(client);
        if let Err(error) = app.container.close().await {
            // A disposer whose first scheduling opportunity is beyond the
            // cleanup cutoff can also be interrupted. DI retains that failure.
            assert!(error.to_string().contains("ScopeCleanupTimedOut"));
        }
    }

    #[tokio::test]
    async fn route_misses_never_spawn_message_dispatch_or_attempt_message_scope() {
        let address = reserve_loopback_address();
        let app = WsAppBuilder::new(&address.to_string())
            .config(ServerConfig {
                allow_missing_origin: true,
                ping_interval_secs: 60,
                ..ServerConfig::default()
            })
            .build()
            .await
            .expect("build route-miss regression application");
        let admission = CancellationToken::new();
        let runtime = app.runtime_clone();
        let server_admission = admission.clone();
        let server_task = tokio::spawn(async move {
            runtime
                .serve_until_shutdown(
                    server_admission,
                    CancellationToken::new(),
                    CancellationToken::new(),
                )
                .await
        });

        let tcp = connect_loopback_when_ready(address).await;
        let (mut client, response) =
            tokio_tungstenite::client_async(format!("ws://{address}/ws?namespace=orders"), tcp)
                .await
                .expect("route-miss regression WS Upgrade");
        assert_eq!(
            response.status(),
            tokio_tungstenite::tungstenite::http::StatusCode::SWITCHING_PROTOCOLS
        );

        let baseline = app.message_dispatch_registry.test_snapshot();
        assert_eq!(baseline.dispatch_admissions, 0);
        assert_eq!(baseline.scope_execution_attempts, 0);
        assert_eq!(baseline.active_tasks, 0);
        assert_eq!(baseline.registered_abort_handles, 0);
        assert_eq!(app.container.active_scope_count(), 0);

        for (event, expected_code) in [
            (
                "other:pipeline",
                crate::request::WsProtocolErrorCode::NamespaceViolation,
            ),
            (
                "orders:unknown",
                crate::request::WsProtocolErrorCode::ActionNotFound,
            ),
            (
                "orders:noop.extra",
                crate::request::WsProtocolErrorCode::ActionNotFound,
            ),
        ] {
            let namespace = event.split_once(':').expect("test route is canonical").0;
            client
                .send(
                    crate::request::WsMessageBody::try_new(event, serde_json::Value::Null)
                        .expect("build route-miss envelope")
                        .with_namespace(namespace.to_owned())
                        .to_message()
                        .expect("encode route-miss envelope"),
                )
                .await
                .expect("send route-miss envelope");

            let terminal = timeout(Duration::from_secs(1), client.next())
                .await
                .expect("route-miss terminal deadline")
                .expect("route miss keeps the connection open")
                .expect("read route-miss terminal");
            let encoded = match terminal {
                Message::Text(text) => text.into_bytes(),
                Message::Binary(bytes) => bytes,
                other => panic!("route miss returned a non-application terminal: {other:?}"),
            };
            let terminal: serde_json::Value =
                serde_json::from_slice(&encoded).expect("decode route-miss terminal");
            assert_eq!(terminal["data"]["code"], expected_code.as_str(), "{event}");

            let observed = app.message_dispatch_registry.test_snapshot();
            assert_eq!(
                observed.dispatch_admissions, baseline.dispatch_admissions,
                "route miss admitted a message-dispatch task: {event}"
            );
            assert_eq!(
                observed.scope_execution_attempts, baseline.scope_execution_attempts,
                "route miss attempted a message DI scope: {event}"
            );
            assert_eq!(
                observed.active_tasks, baseline.active_tasks,
                "route miss left a tracked message-dispatch task: {event}"
            );
            assert_eq!(
                observed.registered_abort_handles, baseline.registered_abort_handles,
                "route miss left a message-dispatch abort handle: {event}"
            );
            assert_eq!(
                app.container.active_scope_count(),
                0,
                "route miss left an active DI scope: {event}"
            );
        }

        // Prove that the private cumulative probes are connected to the real
        // production path: one valid route must admit exactly one dispatch and
        // attempt exactly one message scope, even though both finish quickly.
        client
            .send(
                crate::request::WsMessageBody::try_new("orders:noop", serde_json::Value::Null)
                    .expect("build positive-control envelope")
                    .with_namespace("orders".to_owned())
                    .to_message()
                    .expect("encode positive-control envelope"),
            )
            .await
            .expect("send positive-control envelope");
        let positive = timeout(Duration::from_secs(1), async {
            loop {
                let observed = app.message_dispatch_registry.test_snapshot();
                if observed.dispatch_admissions > baseline.dispatch_admissions
                    && observed.scope_execution_attempts > baseline.scope_execution_attempts
                    && observed.active_tasks == 0
                    && observed.registered_abort_handles == 0
                    && app.container.active_scope_count() == 0
                {
                    break observed;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("positive-control dispatch cleanup deadline");
        assert_eq!(
            positive.dispatch_admissions,
            baseline.dispatch_admissions + 1
        );
        assert_eq!(
            positive.scope_execution_attempts,
            baseline.scope_execution_attempts + 1
        );

        client
            .send(Message::Close(None))
            .await
            .expect("close route-miss regression client");
        let close = timeout(Duration::from_secs(1), client.next())
            .await
            .expect("route-miss regression Close deadline");
        assert!(matches!(close, Some(Ok(Message::Close(_)))));
        drop(client);

        admission.cancel();
        timeout(Duration::from_secs(2), server_task)
            .await
            .expect("route-miss regression server shutdown deadline")
            .expect("route-miss regression server task panicked")
            .expect("route-miss regression server shutdown");
        assert_eq!(app.active_connection_count().await, 0);
        assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
        assert_eq!(app.container.active_scope_count(), 0);
        app.container.close().await.expect("close test container");
    }

    #[tokio::test]
    async fn real_loopback_ws_reconciles_connections_cleanup_tasks_and_permits() {
        let address = reserve_loopback_address();
        let app = WsAppBuilder::new(&address.to_string())
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .build()
            .await
            .expect("build real WS application");
        let admission = CancellationToken::new();
        let message_admission = CancellationToken::new();
        let force = CancellationToken::new();
        let runtime = app.runtime_clone();
        let server_admission = admission.clone();
        let server_task = tokio::spawn(async move {
            runtime
                .serve_until_shutdown(server_admission, message_admission, force)
                .await
        });
        let first_tcp = connect_loopback_when_ready(address).await;
        let (mut first_client, first_response) = tokio_tungstenite::client_async(
            format!("ws://{address}/ws?namespace=orders"),
            first_tcp,
        )
        .await
        .expect("first real WS Upgrade");
        assert_eq!(
            first_response.status(),
            tokio_tungstenite::tungstenite::http::StatusCode::SWITCHING_PROTOCOLS
        );
        first_client
            .send(Message::Close(None))
            .await
            .expect("send first peer Close");
        let first_close = timeout(Duration::from_secs(2), first_client.next())
            .await
            .expect("first peer Close acknowledgement deadline");
        assert!(matches!(first_close, Some(Ok(Message::Close(_)))));
        drop(first_client);

        // Closing one connection must never cancel the process-wide listener
        // admission token. A second connection proves the tokens are isolated.
        let tcp = connect_loopback_when_ready(address).await;
        let (mut client, response) =
            tokio_tungstenite::client_async(format!("ws://{address}/ws?namespace=orders"), tcp)
                .await
                .expect("real WS Upgrade");
        assert_eq!(
            response.status(),
            tokio_tungstenite::tungstenite::http::StatusCode::SWITCHING_PROTOCOLS
        );

        admission.cancel();
        let close = timeout(Duration::from_secs(2), client.next())
            .await
            .expect("real WS shutdown close deadline")
            .expect("real WS shutdown close frame")
            .expect("read real WS shutdown close frame");
        assert!(matches!(
            close,
            Message::Close(Some(frame))
                if frame.code == crate::request::WsCloseReason::ServerShutdown.code()
                    && frame.reason == crate::request::WsCloseReason::ServerShutdown.reason()
        ));
        tokio::task::yield_now().await;
        assert!(
            !server_task.is_finished(),
            "server must await the peer Close acknowledgement"
        );
        client
            .flush()
            .await
            .expect("flush real WS close acknowledgement");
        drop(client);
        timeout(Duration::from_secs(2), server_task)
            .await
            .expect("real WS server shutdown deadline")
            .expect("real WS server task panicked")
            .expect("real WS server shutdown");

        assert_eq!(app.active_connection_count().await, 0);
        assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
        assert_eq!(
            app.connection_permits.available_permits(),
            app.server_config().max_connections
        );
        app.container.close().await.expect("close test container");
    }

    #[tokio::test]
    async fn real_loopback_wss_reconciles_connections_cleanup_tasks_and_permits() {
        let address = reserve_loopback_address();
        let (server_tls, client_tls) = test_tls_configs();
        let app = WsAppBuilder::new(&address.to_string())
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .rustls_config(server_tls)
            .build()
            .await
            .expect("build real WSS application");
        let admission = CancellationToken::new();
        let message_admission = CancellationToken::new();
        let force = CancellationToken::new();
        let runtime = app.runtime_clone();
        let server_admission = admission.clone();
        let server_task = tokio::spawn(async move {
            runtime
                .serve_until_shutdown(server_admission, message_admission, force)
                .await
        });
        let tcp = connect_loopback_when_ready(address).await;
        let connector = TlsConnector::from(client_tls);
        let tls = connector
            .connect(ServerName::try_from("localhost").expect("server name"), tcp)
            .await
            .expect("real WSS TLS handshake");
        let (mut client, response) =
            tokio_tungstenite::client_async(format!("wss://{address}/ws?namespace=orders"), tls)
                .await
                .expect("real WSS Upgrade");
        assert_eq!(
            response.status(),
            tokio_tungstenite::tungstenite::http::StatusCode::SWITCHING_PROTOCOLS
        );

        admission.cancel();
        let close = timeout(Duration::from_secs(2), client.next())
            .await
            .expect("real WSS shutdown close deadline")
            .expect("real WSS shutdown close frame")
            .expect("read real WSS shutdown close frame");
        assert!(matches!(
            close,
            Message::Close(Some(frame))
                if frame.code == crate::request::WsCloseReason::ServerShutdown.code()
                    && frame.reason == crate::request::WsCloseReason::ServerShutdown.reason()
        ));
        tokio::task::yield_now().await;
        assert!(
            !server_task.is_finished(),
            "server must await the peer Close acknowledgement"
        );
        client
            .flush()
            .await
            .expect("flush real WSS close acknowledgement");
        drop(client);
        timeout(Duration::from_secs(2), server_task)
            .await
            .expect("real WSS server shutdown deadline")
            .expect("real WSS server task panicked")
            .expect("real WSS server shutdown");

        assert_eq!(app.active_connection_count().await, 0);
        assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
        assert_eq!(
            app.connection_permits.available_permits(),
            app.server_config().max_connections
        );
        app.container.close().await.expect("close test container");
    }

    async fn wait_for_abrupt_disconnect_cleanup(app: &WsApp, closed_calls: &AtomicUsize) {
        timeout(Duration::from_secs(2), async {
            loop {
                if closed_calls.load(Ordering::SeqCst) == 1
                    && app.active_connection_count().await == 0
                    && app.connection_cleanup_registry.entry_count() == 0
                    && app.connection_permits.available_permits()
                        == app.server_config().max_connections
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("abrupt disconnect cleanup deadline");
    }

    #[tokio::test]
    async fn abrupt_real_ws_disconnect_runs_exactly_one_cleanup_and_reuses_capacity() {
        let _test_lock = CONNECTION_PROBE_TEST_LOCK.lock().await;
        let admit_calls = Arc::new(AtomicUsize::new(0));
        let opened_calls = Arc::new(AtomicUsize::new(0));
        let closed_calls = Arc::new(AtomicUsize::new(0));
        let close_categories = Arc::new(StdMutex::new(Vec::new()));
        *CONNECTION_PROBE_CONFIG.lock().unwrap() = Some(ConnectionLifecycleProbeConfig {
            reject_admission: false,
            admit_calls: Arc::clone(&admit_calls),
            opened_calls: Arc::clone(&opened_calls),
            closed_calls: Arc::clone(&closed_calls),
            close_categories: Arc::clone(&close_categories),
        });

        let address = reserve_loopback_address();
        let app = WsAppBuilder::new(&address.to_string())
            .config(ServerConfig {
                allow_missing_origin: true,
                max_connections: 1,
                ping_interval_secs: 60,
                ..ServerConfig::default()
            })
            .connection_middleware::<ConnectionLifecycleProbe>()
            .build()
            .await
            .expect("build abrupt WS application");
        let admission = CancellationToken::new();
        let runtime = app.runtime_clone();
        let server_admission = admission.clone();
        let server_task = tokio::spawn(async move {
            runtime
                .serve_until_shutdown(
                    server_admission,
                    CancellationToken::new(),
                    CancellationToken::new(),
                )
                .await
        });

        let tcp = connect_loopback_when_ready(address).await;
        let (client, response) =
            tokio_tungstenite::client_async(format!("ws://{address}/ws?namespace=orders"), tcp)
                .await
                .expect("real WS Upgrade");
        assert_eq!(
            response.status(),
            tokio_tungstenite::tungstenite::http::StatusCode::SWITCHING_PROTOCOLS
        );
        timeout(Duration::from_secs(1), async {
            while opened_calls.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("connection middleware opened deadline");

        // Dropping the transport without a WebSocket Close frame is the
        // portable abrupt-peer case (EOF instead of the RFC close handshake).
        drop(client);
        wait_for_abrupt_disconnect_cleanup(&app, closed_calls.as_ref()).await;
        assert_eq!(admit_calls.load(Ordering::SeqCst), 1);
        assert_eq!(opened_calls.load(Ordering::SeqCst), 1);
        assert_eq!(closed_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            close_categories.lock().unwrap().as_slice(),
            [crate::middleware::WsConnectionCloseCategory::Reset]
        );

        // The one connection permit must be usable by a fresh real client.
        let tcp = connect_loopback_when_ready(address).await;
        let (mut recovered, _) =
            tokio_tungstenite::client_async(format!("ws://{address}/ws?namespace=orders"), tcp)
                .await
                .expect("capacity-recovery WS Upgrade");
        recovered
            .send(Message::Close(None))
            .await
            .expect("send recovery Close");
        let _ = timeout(Duration::from_secs(1), recovered.next())
            .await
            .expect("recovery Close acknowledgement deadline");
        drop(recovered);

        timeout(Duration::from_secs(2), async {
            while closed_calls.load(Ordering::SeqCst) != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("recovery connection cleanup deadline");
        admission.cancel();
        timeout(Duration::from_secs(2), server_task)
            .await
            .expect("abrupt WS server shutdown deadline")
            .expect("abrupt WS server task panicked")
            .expect("abrupt WS server shutdown");
        assert_eq!(app.active_connection_count().await, 0);
        assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
        assert_eq!(app.connection_permits.available_permits(), 1);
        app.container.close().await.expect("close test container");
        *CONNECTION_PROBE_CONFIG.lock().unwrap() = None;
    }

    #[tokio::test]
    async fn abrupt_real_wss_disconnect_does_not_leave_a_pending_tls_payload() {
        let _test_lock = CONNECTION_PROBE_TEST_LOCK.lock().await;
        let (server_tls, client_tls) = test_tls_configs();
        let admit_calls = Arc::new(AtomicUsize::new(0));
        let opened_calls = Arc::new(AtomicUsize::new(0));
        let closed_calls = Arc::new(AtomicUsize::new(0));
        let close_categories = Arc::new(StdMutex::new(Vec::new()));
        *CONNECTION_PROBE_CONFIG.lock().unwrap() = Some(ConnectionLifecycleProbeConfig {
            reject_admission: false,
            admit_calls: Arc::clone(&admit_calls),
            opened_calls: Arc::clone(&opened_calls),
            closed_calls: Arc::clone(&closed_calls),
            close_categories: Arc::clone(&close_categories),
        });

        let address = reserve_loopback_address();
        let app = WsAppBuilder::new(&address.to_string())
            .config(ServerConfig {
                allow_missing_origin: true,
                max_connections: 1,
                ping_interval_secs: 60,
                ..ServerConfig::default()
            })
            .connection_middleware::<ConnectionLifecycleProbe>()
            .rustls_config(server_tls)
            .build()
            .await
            .expect("build abrupt WSS application");
        let admission = CancellationToken::new();
        let runtime = app.runtime_clone();
        let server_admission = admission.clone();
        let server_task = tokio::spawn(async move {
            runtime
                .serve_until_shutdown(
                    server_admission,
                    CancellationToken::new(),
                    CancellationToken::new(),
                )
                .await
        });

        let tcp = connect_loopback_when_ready(address).await;
        let tls = TlsConnector::from(client_tls)
            .connect(ServerName::try_from("localhost").expect("server name"), tcp)
            .await
            .expect("real WSS TLS handshake");
        let (client, response) =
            tokio_tungstenite::client_async(format!("wss://{address}/ws?namespace=orders"), tls)
                .await
                .expect("real WSS Upgrade");
        assert_eq!(
            response.status(),
            tokio_tungstenite::tungstenite::http::StatusCode::SWITCHING_PROTOCOLS
        );
        timeout(Duration::from_secs(1), async {
            while opened_calls.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("WSS connection middleware opened deadline");

        drop(client);
        wait_for_abrupt_disconnect_cleanup(&app, closed_calls.as_ref()).await;
        assert_eq!(admit_calls.load(Ordering::SeqCst), 1);
        assert_eq!(opened_calls.load(Ordering::SeqCst), 1);
        assert_eq!(closed_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            close_categories.lock().unwrap().as_slice(),
            [crate::middleware::WsConnectionCloseCategory::Reset]
        );

        admission.cancel();
        timeout(Duration::from_secs(2), server_task)
            .await
            .expect("abrupt WSS server shutdown deadline")
            .expect("abrupt WSS server task panicked")
            .expect("abrupt WSS server shutdown");
        assert_eq!(app.active_connection_count().await, 0);
        assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
        assert_eq!(app.connection_permits.available_permits(), 1);
        app.container.close().await.expect("close test container");
        *CONNECTION_PROBE_CONFIG.lock().unwrap() = None;
    }

    #[tokio::test]
    async fn wss_connection_reaches_the_existing_websocket_pipeline() {
        let _test_lock = CONNECTION_PROBE_TEST_LOCK.lock().await;
        let (server_tls, client_tls) = test_tls_configs();
        let admit_calls = Arc::new(AtomicUsize::new(0));
        let opened_calls = Arc::new(AtomicUsize::new(0));
        let closed_calls = Arc::new(AtomicUsize::new(0));
        let close_categories = Arc::new(StdMutex::new(Vec::new()));
        *CONNECTION_PROBE_CONFIG.lock().unwrap() = Some(ConnectionLifecycleProbeConfig {
            reject_admission: false,
            admit_calls: Arc::clone(&admit_calls),
            opened_calls: Arc::clone(&opened_calls),
            closed_calls: Arc::clone(&closed_calls),
            close_categories: Arc::clone(&close_categories),
        });
        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .connection_middleware::<ConnectionLifecycleProbe>()
            .rustls_config(server_tls)
            .build()
            .await
            .expect("build WSS application");
        let acceptor = WsApp::tls_acceptor(app.rustls_config().expect("TLS configuration"));

        let (server_io, client_io) = duplex(64 * 1024);
        let peer_addr = "127.0.0.1:12345".parse().expect("peer address");

        let runtime = app.connection_runtime();
        let permit = app
            .connection_permits
            .clone()
            .try_acquire_owned()
            .expect("connection permit");
        let cancellation = CancellationToken::new();
        let connection_id = Uuid::new_v4();
        let connection_span = tracing::info_span!("test.websocket.connection");
        let server_task = tokio::spawn(WsApp::handle_connection(
            server_io,
            runtime,
            AcceptedConnection {
                addr: peer_addr,
                connection_id,
                permit,
                span: connection_span,
            },
            Some(acceptor),
            cancellation,
            CancellationToken::new(),
            CancellationToken::new(),
        ));

        let connector = TlsConnector::from(client_tls);
        let client_tls = connector
            .connect(
                ServerName::try_from("localhost").expect("server name"),
                client_io,
            )
            .await
            .expect("TLS handshake");
        assert_eq!(
            client_tls.get_ref().1.alpn_protocol(),
            Some(b"http/1.1".as_slice())
        );
        let (mut client, response) =
            tokio_tungstenite::client_async("wss://localhost/ws?namespace=orders", client_tls)
                .await
                .expect("WebSocket upgrade");
        assert_eq!(
            response.status(),
            tokio_tungstenite::tungstenite::http::StatusCode::SWITCHING_PROTOCOLS
        );

        client
            .send(Message::Close(None))
            .await
            .expect("send client close");
        let close = timeout(Duration::from_secs(2), client.next())
            .await
            .expect("close acknowledgement timed out");
        assert!(matches!(close, Some(Ok(Message::Close(_)))));
        timeout(Duration::from_secs(2), server_task)
            .await
            .expect("WSS server task timed out")
            .expect("WSS server task panicked")
            .expect("WSS connection failed");

        let metrics = app.metrics_snapshot();
        assert_eq!(metrics.handshakes_succeeded, 1);
        assert_eq!(metrics.handshakes_failed, 0);
        assert_eq!(app.active_connection_count().await, 0);
        assert_eq!(admit_calls.load(Ordering::SeqCst), 1);
        assert_eq!(opened_calls.load(Ordering::SeqCst), 1);
        assert_eq!(closed_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            close_categories.lock().unwrap().as_slice(),
            [crate::middleware::WsConnectionCloseCategory::NormalPeer]
        );
        app.container.close().await.expect("close test container");
        *CONNECTION_PROBE_CONFIG.lock().unwrap() = None;
    }

    #[tokio::test]
    async fn websocket_tls_adapter_preserves_required_client_certificate_verification() {
        let (server_tls, authenticated_client, anonymous_client) = test_mtls_configs();
        let cancellation = CancellationToken::new();

        let (server_io, client_io) = duplex(16 * 1024);
        let acceptor = WsApp::tls_acceptor(&server_tls);
        let connector = TlsConnector::from(authenticated_client);
        let server_span = tracing::info_span!("test.websocket.tls");
        let (server_result, client_result) = tokio::join!(
            WsApp::accept_tls(
                server_io,
                acceptor,
                Instant::now() + Duration::from_secs(1),
                Duration::from_secs(1),
                &cancellation,
                &server_span,
            ),
            connector.connect(
                ServerName::try_from("localhost").expect("server name"),
                client_io,
            )
        );
        let server_stream = server_result
            .expect("authenticated server handshake")
            .expect("handshake was not cancelled");
        assert!(client_result.is_ok());
        assert_eq!(
            server_stream.get_ref().1.alpn_protocol(),
            Some(b"http/1.1".as_slice())
        );

        let (server_io, client_io) = duplex(16 * 1024);
        let acceptor = WsApp::tls_acceptor(&server_tls);
        let connector = TlsConnector::from(anonymous_client);
        let server_span = tracing::info_span!("test.websocket.tls");
        let (server_result, _client_result) = tokio::join!(
            WsApp::accept_tls(
                server_io,
                acceptor,
                Instant::now() + Duration::from_secs(1),
                Duration::from_secs(1),
                &cancellation,
                &server_span,
            ),
            connector.connect(
                ServerName::try_from("localhost").expect("server name"),
                client_io,
            )
        );
        assert!(matches!(
            server_result,
            Err(ServerError::TlsHandshakeFailed { .. })
        ));
    }

    #[tokio::test]
    async fn websocket_tls_handshake_obeys_cancellation_and_deadline() {
        let (server_tls, _) = test_tls_configs();

        let (server_io, _client_io) = duplex(1024);
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let span = tracing::info_span!("test.websocket.tls");
        let cancelled = WsApp::accept_tls(
            server_io,
            WsApp::tls_acceptor(&server_tls),
            Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
            &cancellation,
            &span,
        )
        .await
        .expect("cancellation is not a transport failure");
        assert!(cancelled.is_none());

        let (server_io, _client_io) = duplex(1024);
        let cancellation = CancellationToken::new();
        let span = tracing::info_span!("test.websocket.tls");
        let result = WsApp::accept_tls(
            server_io,
            WsApp::tls_acceptor(&server_tls),
            Instant::now() + Duration::from_millis(20),
            Duration::from_millis(20),
            &cancellation,
            &span,
        )
        .await;
        assert!(matches!(
            result,
            Err(ServerError::HandshakeTimeout(duration))
                if duration == Duration::from_millis(20)
        ));
    }

    #[tokio::test]
    async fn plaintext_remains_default_and_can_override_an_explicit_tls_value() {
        let plaintext = WsAppBuilder::new("127.0.0.1:0")
            .build()
            .await
            .expect("build plaintext application");
        assert!(plaintext.rustls_config().is_none());
        plaintext
            .container
            .close()
            .await
            .expect("close plaintext test container");

        let (server_tls, _) = test_tls_configs();
        let explicitly_plaintext = WsAppBuilder::new("127.0.0.1:0")
            .rustls_config(server_tls)
            .tls_disabled()
            .build()
            .await
            .expect("build explicitly plaintext application");
        assert!(explicitly_plaintext.rustls_config().is_none());
        explicitly_plaintext
            .container
            .close()
            .await
            .expect("close explicitly plaintext test container");
    }

    #[tokio::test]
    async fn peer_close_acknowledgement_is_flushed_without_a_second_close_send() {
        use tokio_tungstenite::{WebSocketStream, tungstenite::protocol::Role};

        let (client_io, server_io) = tokio::io::duplex(4 * 1024);
        let mut client = WebSocketStream::from_raw_socket(client_io, Role::Client, None).await;
        let server = WebSocketStream::from_raw_socket(server_io, Role::Server, None).await;
        let (mut server_sender, mut server_receiver) = server.split();

        client
            .send(Message::Close(Some(CloseFrame {
                code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
                reason: "client shutdown".into(),
            })))
            .await
            .unwrap();

        assert!(matches!(
            server_receiver.next().await,
            Some(Ok(Message::Close(_)))
        ));

        // Reading the peer Close queued Tungstenite's acknowledgement. The
        // production writer follows this same flush-only path; sending another
        // Close here would produce `Sending after closing is not allowed`.
        server_sender.flush().await.unwrap();
        assert!(matches!(client.next().await, Some(Ok(Message::Close(_)))));
    }

    #[tokio::test]
    async fn connection_admission_permits_never_exceed_the_configured_limit() {
        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                max_connections: 2,
                ..ServerConfig::default()
            })
            .build()
            .await
            .unwrap();

        let first = app.connection_permits.clone().try_acquire_owned().unwrap();
        let second = app.connection_permits.clone().try_acquire_owned().unwrap();
        assert!(app.connection_permits.clone().try_acquire_owned().is_err());
        assert_eq!(app.connection_permits.available_permits(), 0);

        drop(first);
        assert!(app.connection_permits.clone().try_acquire_owned().is_ok());
        drop(second);
        app.container.close().await.unwrap();
    }

    #[tokio::test]
    async fn health_snapshot_is_tied_to_the_application_lifecycle() {
        let app = WsAppBuilder::new("127.0.0.1:0").build().await.unwrap();
        let snapshot = app.health_snapshot().unwrap();
        assert!(snapshot.live);
        assert!(!snapshot.ready);
        assert!(!snapshot.accepting_new_work);
        assert_eq!(snapshot.checks.len(), 4);
        assert_eq!(snapshot.checks[0].name, WS_CONTROLLER_REGISTRY_HEALTH_CHECK);
        assert_eq!(snapshot.checks[1].name, WS_DI_HEALTH_CHECK);
        assert_eq!(snapshot.checks[2].name, WS_DISPATCHER_HEALTH_CHECK);
        assert_eq!(snapshot.checks[3].name, WS_LISTENER_HEALTH_CHECK);
        assert_eq!(snapshot.checks[0].status, HealthStatus::Healthy);
        assert_eq!(snapshot.checks[1].status, HealthStatus::Healthy);
        assert_eq!(snapshot.checks[2].status, HealthStatus::Healthy);
        assert_eq!(snapshot.checks[3].status, HealthStatus::Unhealthy);
        app.close().await.unwrap();
    }

    #[tokio::test]
    async fn required_backplane_subscription_gates_listener_readiness_until_ready_event() {
        required_subscription_ready_sender().send_replace(false);
        let address = reserve_loopback_address().to_string();
        let app = Arc::new(
            WsAppBuilder::new(&address)
                .backplane::<RequiredReadyGateBackplane>(BackplaneRequirement::Required)
                .build()
                .await
                .unwrap(),
        );
        let start_app = Arc::clone(&app);
        let start = tokio::spawn(async move { start_app.start().await });

        let waiting = timeout(Duration::from_secs(2), async {
            loop {
                let snapshot = app.health_snapshot().unwrap();
                let subscriber = snapshot
                    .checks
                    .iter()
                    .find(|check| check.name == WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK)
                    .unwrap();
                if subscriber.reason_code == "starting" {
                    return snapshot;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("required backplane subscription did not enter its startup gate");
        assert!(!waiting.ready);
        assert!(!waiting.accepting_new_work);

        required_subscription_ready_sender().send_replace(true);
        let ready = wait_for_accepting_health(&app).await;
        assert!(ready.ready);
        let subscriber = ready
            .checks
            .iter()
            .find(|check| check.name == WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK)
            .unwrap();
        assert_eq!(subscriber.status, HealthStatus::Healthy);
        assert_eq!(subscriber.reason_code, "connected");

        timeout(Duration::from_secs(3), app.close())
            .await
            .expect("required backplane shutdown deadline")
            .unwrap();
        start.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn optional_backplane_subscription_does_not_gate_listener_readiness() {
        let address = reserve_loopback_address().to_string();
        let app = Arc::new(
            WsAppBuilder::new(&address)
                .backplane::<NeverReadyOptionalBackplane>(BackplaneRequirement::Optional)
                .build()
                .await
                .unwrap(),
        );
        let start_app = Arc::clone(&app);
        let start = tokio::spawn(async move { start_app.start().await });

        let ready = wait_for_accepting_health(&app).await;
        assert!(ready.ready);
        let subscriber = ready
            .checks
            .iter()
            .find(|check| check.name == WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK)
            .unwrap();
        assert_eq!(subscriber.criticality, HealthCriticality::NonCritical);
        assert_eq!(subscriber.status, HealthStatus::Unhealthy);

        timeout(Duration::from_secs(3), app.close())
            .await
            .expect("optional backplane shutdown deadline")
            .unwrap();
        start.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn terminal_wait_replays_and_observes_completion() {
        let replay = WsAppBuilder::new("127.0.0.1:0").build().await.unwrap();
        WsApp::complete_lifecycle(&replay.lifecycle, Ok(())).await;
        timeout(Duration::from_secs(1), replay.await_terminal())
            .await
            .expect("terminal result published before wait must be replayed")
            .expect("successful terminal result must remain successful");
        replay.container.close().await.unwrap();

        let observed = Arc::new(WsAppBuilder::new("127.0.0.1:0").build().await.unwrap());
        let terminal_waiter = Arc::clone(&observed);
        let waiter = tokio::spawn(async move { terminal_waiter.await_terminal().await });
        tokio::task::yield_now().await;
        WsApp::complete_lifecycle(&observed.lifecycle, Ok(())).await;
        timeout(Duration::from_secs(1), waiter)
            .await
            .expect("registered terminal waiter must observe completion")
            .expect("terminal waiter task must not panic")
            .expect("successful terminal result must remain successful");
        observed.container.close().await.unwrap();
    }

    #[tokio::test]
    async fn never_started_close_is_concurrent_idempotent_and_owns_only_its_container() {
        let app = Arc::new(WsAppBuilder::new("127.0.0.1:0").build().await.unwrap());
        let container = Arc::clone(app.container());
        let dependencies = app.required_dependency_health();
        dependencies.register_required("redis.primary").unwrap();

        let (first, second) = tokio::join!(app.close(), app.close());
        first.unwrap();
        second.unwrap();
        assert!(container.resolve::<ConfigService>(None).await.is_err());
        assert!(matches!(
            dependencies.register_required("postgres.primary"),
            Err(WsDependencyHealthError::RegistrationClosed)
        ));
        assert!(
            app.start()
                .await
                .unwrap_err()
                .to_string()
                .contains("closed")
        );

        let snapshot = app.health_snapshot().unwrap();
        assert!(snapshot.live);
        assert!(!snapshot.ready);
        assert!(!snapshot.accepting_new_work);
        let listener = snapshot
            .checks
            .iter()
            .find(|check| check.name == WS_LISTENER_HEALTH_CHECK)
            .unwrap();
        assert_eq!(listener.status, HealthStatus::Unhealthy);
        assert_eq!(listener.reason_code, "closed_without_start");
        let dispatcher = snapshot
            .checks
            .iter()
            .find(|check| check.name == WS_DISPATCHER_HEALTH_CHECK)
            .unwrap();
        assert_eq!(dispatcher.status, HealthStatus::Degraded);
        assert_eq!(dispatcher.reason_code, "resources_closed");
    }

    #[tokio::test]
    async fn never_started_close_preserves_a_caller_owned_container() {
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let app = WsAppBuilder::new("127.0.0.1:0")
            .container(Arc::clone(&container))
            .build()
            .await
            .unwrap();

        app.close().await.unwrap();
        app.close().await.unwrap();
        assert!(container.resolve::<ConfigService>(None).await.is_ok());
        container.close().await.unwrap();
    }

    #[tokio::test]
    async fn running_close_and_a_dropped_start_waiter_share_one_supervised_terminal() {
        let address = reserve_loopback_address().to_string();
        let app = Arc::new(WsAppBuilder::new(&address).build().await.unwrap());
        let start_app = Arc::clone(&app);
        let waiter = tokio::spawn(async move { start_app.start().await });
        let ready = wait_for_accepting_health(&app).await;
        assert!(ready.ready);

        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        timeout(Duration::from_secs(3), app.close())
            .await
            .expect("detached lifecycle supervisor cleanup deadline")
            .unwrap();

        let snapshot = app.health_snapshot().unwrap();
        assert!(!snapshot.ready);
        assert!(!snapshot.accepting_new_work);
        let dispatcher = snapshot
            .checks
            .iter()
            .find(|check| check.name == WS_DISPATCHER_HEALTH_CHECK)
            .unwrap();
        assert_eq!(dispatcher.status, HealthStatus::Degraded);
        assert_eq!(dispatcher.reason_code, "resources_closed");
    }

    #[tokio::test]
    async fn required_dependencies_gate_bound_listener_readiness_and_freeze_at_start() {
        let address = reserve_loopback_address().to_string();
        let app = Arc::new(WsAppBuilder::new(&address).build().await.unwrap());
        let dependencies = app.required_dependency_health();
        dependencies.register_required("redis.primary").unwrap();
        let start_app = Arc::clone(&app);
        let start = tokio::spawn(async move { start_app.start().await });

        let listening = wait_for_accepting_health(&app).await;
        assert!(!listening.ready);
        assert!(matches!(
            dependencies.register_required("postgres.primary"),
            Err(WsDependencyHealthError::RegistrationClosed)
        ));
        dependencies
            .mark_healthy("redis.primary", "connected")
            .unwrap();
        assert!(app.health_snapshot().unwrap().ready);

        app.close().await.unwrap();
        start.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn maintenance_failure_is_fatal_and_revokes_listener_readiness() {
        let app = WsAppBuilder::new("127.0.0.1:0").build().await.unwrap();
        let completed = tokio::spawn(async { panic!("maintenance probe") }).await;
        let error = app.maintenance_task_failure(Some(completed.map_err(Arc::new)));
        assert!(error.to_string().contains("maintenance task panicked"));
        let snapshot = app.health_snapshot().unwrap();
        assert!(!snapshot.live);
        assert!(!snapshot.ready);
        let listener = snapshot
            .checks
            .iter()
            .find(|check| check.name == WS_LISTENER_HEALTH_CHECK)
            .unwrap();
        assert_eq!(listener.status, HealthStatus::Unhealthy);
        assert_eq!(listener.reason_code, "maintenance_failed");
        app.close().await.unwrap();
    }

    #[tokio::test]
    async fn bind_failure_closes_a_builder_owned_container() {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reservation.local_addr().unwrap();
        let app = WsAppBuilder::new(&address.to_string())
            .build()
            .await
            .unwrap();
        let container = Arc::clone(app.container());

        assert!(app.owns_container);
        assert_eq!(app.shutdown_timeout, DEFAULT_SHUTDOWN_TIMEOUT);
        assert!(app.start().await.is_err());
        drop(reservation);
        assert!(container.resolve::<ConfigService>(None).await.is_err());
        let snapshot = app.health_snapshot().unwrap();
        assert!(!snapshot.live);
        let listener = snapshot
            .checks
            .iter()
            .find(|check| check.name == WS_LISTENER_HEALTH_CHECK)
            .unwrap();
        assert_eq!(listener.status, HealthStatus::Unhealthy);
        assert_eq!(listener.reason_code, "bind_failed");
        assert!(
            app.start()
                .await
                .unwrap_err()
                .to_string()
                .contains("already")
        );
    }

    #[tokio::test]
    async fn bind_failure_does_not_close_a_caller_supplied_container() {
        let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = reservation.local_addr().unwrap();
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let app = WsAppBuilder::new(&address.to_string())
            .container(Arc::clone(&container))
            .build()
            .await
            .unwrap();

        assert!(!app.owns_container);
        assert!(app.start().await.is_err());
        drop(reservation);
        assert!(container.resolve::<ConfigService>(None).await.is_ok());

        container.close().await.unwrap();
    }

    #[test]
    fn canonical_server_spans_do_not_declare_payload_secret_or_error_text_fields() {
        let source = include_str!("mod.rs")
            .split("\n#[cfg(test)]\nmod tests")
            .next()
            .expect("production source");
        for forbidden in [
            "otel.status_message =",
            "websocket.message.payload =",
            "authorization =",
            "cookie =",
            "token =",
            "password =",
        ] {
            assert!(
                !source.contains(forbidden),
                "forbidden telemetry field: {forbidden}"
            );
        }
        assert!(source.contains("\"websocket.handshake\""));
        assert!(source.contains("handshake_span.record(\"lily.error_code\", code.as_str())"));
        assert!(source.contains("\"websocket.connection\""));
        assert!(source.contains("\"websocket.message\""));
    }
}
mod handshake;

use handshake::{
    PreUpgradeOutcome, UpgradeRequestOutcome, perform_parsed_upgrade, read_pre_upgrade,
    request_trace_context,
};
