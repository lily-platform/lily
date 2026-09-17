//! Cargo-fuzz-only adapters over Lily's production decision and lifecycle code.
//!
//! This module is compiled only with the non-default `fuzzing` feature. It is
//! intentionally doc-hidden: the types below are not a supported application
//! API and exist solely so the external cargo-fuzz crate can exercise
//! crate-private production executors without copying their implementation.

use crate::{
    app::{
        WsApp, collect_handshake_headers, connection_middleware_terminal,
        has_ambiguous_handshake_headers,
    },
    backplane::{
        BackplaneRequirement, WS_BACKPLANE_PUBLISHER_HEALTH_CHECK,
        WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK, WebSocketBackplane, WebSocketBackplaneError,
        WebSocketBackplaneEvent, WebSocketBackplaneFrame, WebSocketBackplaneInboundAdmission,
        WebSocketBackplaneInitError, WebSocketBackplanePublishReceipt, WebSocketDispatcher,
    },
    connection::ConnectionManager,
    controller::WebSocketContext,
    extractor::WebSocketMessageLocals,
    middleware::{
        CompiledWsConnectionChain, CompiledWsHandshakeChain, CompiledWsIdentityMiddleware,
        CompiledWsMessageChain, ConnectionCleanupOutcome, ConnectionCleanupRegistry,
        WebSocketHandshakeMiddleware, WebSocketIdentity, WebSocketIdentityMiddleware,
        WsConnectionCloseCategory, WsConnectionMiddleware, WsHandshakeExchange,
        WsHandshakeExecutionFailureKind, WsHandshakeRejection, WsHandshakeRequest,
        WsMessageDecision, WsMessageExchange, WsMessageMiddleware, WsMessageOutcome,
        WsMiddlewareError, WsMiddlewareFailureKind, WsMiddlewareInitError,
        WsMiddlewareObservationOutcome, WsMiddlewareObserver,
    },
    request::{WsCloseReason, WsHeaders, WsMessageBody, WsProtocolErrorCode, WsRequest},
    server::{HandshakeRejection, ServerConfig, WsTransportSecurity, parse_handshake_namespace},
};
use async_trait::async_trait;
use futures_util::future::pending;
use lily_injection::ApplicationContainer;
use lily_middleware::{MiddlewareDescriptor, MiddlewareErrorCode, MiddlewareKind};
use lily_monitoring::{HealthCheckKind, HealthCriticality, HealthRegistry};
use lily_shutdown::ShutdownState;
use lily_web_core::{Principal, RequestConnectionInfo};
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::atomic::{AtomicUsize, Ordering},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
    time::Duration,
};
use tokio::sync::{OnceCell, Semaphore, mpsc};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::http::{HeaderMap, HeaderName, HeaderValue};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

pub const HANDSHAKE_MAX_HEADERS: usize = 32;
pub const HANDSHAKE_MAX_HEADER_NAME_BYTES: usize = 64;
pub const HANDSHAKE_MAX_HEADER_VALUE_BYTES: usize = 1024;
pub const HANDSHAKE_MAX_QUERY_BYTES: usize = 512;
pub const HANDSHAKE_MAX_MIDDLEWARES: usize = 16;
pub const MESSAGE_MAX_MIDDLEWARES: usize = 32;
pub const LIFECYCLE_MAX_MIDDLEWARES: usize = 32;
pub const LIFECYCLE_MAX_EVENTS: usize = 64;
pub const BACKPLANE_MAX_INPUT_BYTES: usize = 512 * 1024;
pub const SECRET_SENTINEL: &str = "LILY_WS_FUZZ_SECRET_7F31";

const STAGE_TIMEOUT: Duration = Duration::from_millis(2);
const BACKPLANE_MESSAGE_BYTES: usize = 64 * 1024;
const BACKPLANE_STAGE_TIMEOUT: Duration = Duration::from_millis(500);

struct FuzzBackplaneEnvelopeProvider {
    frame: Mutex<Option<Vec<u8>>>,
    receive_step: AtomicUsize,
    processed: Semaphore,
}

#[async_trait]
impl WebSocketBackplane for FuzzBackplaneEnvelopeProvider {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, WebSocketBackplaneInitError> {
        Err(WebSocketBackplaneInitError::dependency(
            "fuzz providers are materialized directly",
        ))
    }

    async fn publish(
        &self,
        _frame: WebSocketBackplaneFrame,
    ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
        Ok(WebSocketBackplanePublishReceipt::accepted())
    }

    async fn receive(
        &self,
        _admission: WebSocketBackplaneInboundAdmission,
    ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
        match self.receive_step.fetch_add(1, Ordering::AcqRel) {
            0 => Ok(WebSocketBackplaneEvent::SubscriptionReady),
            1 => Ok(WebSocketBackplaneEvent::Frame(
                self.frame
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .take()
                    .unwrap_or_default(),
            )),
            _ => {
                self.processed.add_permits(1);
                pending().await
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuzzBackplaneEnvelopeSnapshot {
    pub invalid_frames: u64,
    pub duplicates_suppressed: u64,
    pub origin_loops_suppressed: u64,
}

#[must_use]
pub async fn exercise_backplane_envelope(input: Vec<u8>) -> FuzzBackplaneEnvelopeSnapshot {
    if input.len() > BACKPLANE_MAX_INPUT_BYTES {
        return FuzzBackplaneEnvelopeSnapshot {
            invalid_frames: 0,
            duplicates_suppressed: 0,
            origin_loops_suppressed: 0,
        };
    }

    let manager = Arc::new(
        ConnectionManager::with_registered_namespaces_and_identity_and_outbound_limit(
            8,
            128,
            std::iter::empty(),
            true,
            BACKPLANE_MESSAGE_BYTES,
        ),
    );
    let provider = Arc::new(FuzzBackplaneEnvelopeProvider {
        frame: Mutex::new(Some(input)),
        receive_step: AtomicUsize::new(0),
        processed: Semaphore::new(0),
    });
    let health = HealthRegistry::new(Arc::new(ShutdownState::new()));
    for check in [
        WS_BACKPLANE_PUBLISHER_HEALTH_CHECK,
        WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
    ] {
        health
            .register(
                check,
                HealthCheckKind::Dependency,
                HealthCriticality::Critical,
            )
            .expect("bounded static backplane health check must register");
    }
    let dispatcher = WebSocketDispatcher::active(
        Arc::clone(&manager),
        BackplaneRequirement::Required,
        Arc::clone(&provider) as Arc<dyn WebSocketBackplane>,
        BACKPLANE_STAGE_TIMEOUT,
        health,
    );
    let ingress = dispatcher
        .spawn_ingress()
        .expect("one active fuzz ingress must start");

    tokio::time::timeout(BACKPLANE_STAGE_TIMEOUT, provider.processed.acquire())
        .await
        .expect("backplane envelope exercise exceeded its absolute deadline")
        .expect("backplane processing semaphore remains open")
        .forget();

    ingress
        .stop()
        .await
        .expect("fuzz ingress task must stop without panicking");
    dispatcher
        .close_backplane()
        .await
        .expect("fuzz backplane must close cleanly");

    let metrics = manager.metrics_snapshot();
    FuzzBackplaneEnvelopeSnapshot {
        invalid_frames: metrics.backplane_invalid_frames,
        duplicates_suppressed: metrics.backplane_duplicates_suppressed,
        origin_loops_suppressed: metrics.backplane_origin_loops_suppressed,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzHandshakeHeader {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuzzHandshakeIdentityAction {
    Disabled,
    Anonymous,
    Authenticated,
    CredentialRequired,
    RejectUnauthorized,
    Unavailable,
    Panic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuzzHandshakeMiddlewareAction {
    Accept,
    RejectBadRequest,
    RejectUnauthorized,
    RejectForbidden,
    Panic,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzHandshakePlan {
    pub headers: Vec<FuzzHandshakeHeader>,
    pub query: Option<String>,
    pub allowed_origins: Vec<String>,
    pub allow_any_origin: bool,
    pub allow_missing_origin: bool,
    pub supported_protocols: Vec<String>,
    pub require_subprotocol: bool,
    pub identity: FuzzHandshakeIdentityAction,
    pub middlewares: Vec<FuzzHandshakeMiddlewareAction>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuzzHandshakeCode {
    Accepted,
    InputLimit,
    InvalidHeader,
    AmbiguousHeader,
    InvalidNamespace,
    InvalidConfiguration,
    CustomBadRequest,
    CustomUnauthorized,
    CustomForbidden,
    CustomMiddlewarePanicked,
    OriginRequired,
    OriginDenied,
    IdentityRejected,
    IdentityRequired,
    IdentityUnavailable,
    IdentityPanicked,
    SubprotocolRequired,
    InvalidSubprotocol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuzzHandshakeSnapshot {
    pub status: u16,
    pub code: FuzzHandshakeCode,
    pub selected_subprotocol: bool,
    pub has_principal: bool,
    pub has_connection_local: bool,
    pub secret_safe: bool,
}

impl FuzzHandshakeSnapshot {
    const fn rejected(status: u16, code: FuzzHandshakeCode) -> Self {
        Self {
            status,
            code,
            selected_subprotocol: false,
            has_principal: false,
            has_connection_local: false,
            secret_safe: true,
        }
    }
}

#[derive(Debug)]
struct FuzzConnectionLocal;

struct FuzzIdentityMiddleware(FuzzHandshakeIdentityAction);

#[async_trait]
impl WebSocketIdentityMiddleware for FuzzIdentityMiddleware {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, WsMiddlewareInitError> {
        Err(WsMiddlewareInitError::Internal)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("ws_fuzz_identity", MiddlewareKind::WebSocketHandshake)
    }

    async fn identify(
        &self,
        exchange: &mut WsHandshakeExchange,
        _cancellation: crate::ExecutionCancellation,
    ) -> Result<WebSocketIdentity, WsHandshakeRejection> {
        let authenticated = || {
            let principal = Principal::new(
                "fuzz-subject",
                ["fuzz-role".to_string()],
                ["fuzz:scope".to_string()],
                Default::default(),
            );
            let mut identity = WebSocketIdentity::authenticated(
                crate::AuthenticatedWebSocketIdentity::try_new(principal)
                    .expect("bounded fuzz principal must be valid"),
            );
            identity
                .insert_connection_local(FuzzConnectionLocal)
                .expect("one fuzz connection-local entry is within the production bound");
            identity
        };
        match self.0 {
            FuzzHandshakeIdentityAction::Disabled | FuzzHandshakeIdentityAction::Anonymous => {
                Ok(WebSocketIdentity::anonymous())
            }
            FuzzHandshakeIdentityAction::Authenticated => Ok(authenticated()),
            FuzzHandshakeIdentityAction::CredentialRequired => {
                let has_credential = ["authorization", "cookie"].into_iter().any(|name| {
                    exchange
                        .request()
                        .headers()
                        .get_custom_header(name)
                        .is_some_and(|value| !value.is_empty())
                });
                if has_credential {
                    Ok(authenticated())
                } else {
                    Err(WsHandshakeRejection::unauthorized(
                        bounded_code("WS_FUZZ_IDENTITY_REQUIRED"),
                        HeaderValue::from_static("Bearer"),
                    )
                    .expect("static fuzz authentication challenge is valid"))
                }
            }
            FuzzHandshakeIdentityAction::RejectUnauthorized => {
                Err(WsHandshakeRejection::unauthorized(
                    bounded_code("WS_FUZZ_IDENTITY_REJECTED"),
                    HeaderValue::from_static("Bearer"),
                )
                .expect("static fuzz authentication challenge is valid"))
            }
            FuzzHandshakeIdentityAction::Unavailable => Err(WsHandshakeRejection::try_new(
                503,
                bounded_code("WS_FUZZ_IDENTITY_UNAVAILABLE"),
            )
            .expect("503 is a valid production handshake rejection")),
            FuzzHandshakeIdentityAction::Panic => panic!("fuzz identity middleware panic"),
        }
    }
}

struct FuzzHandshakeMiddlewareEntry {
    index: usize,
    action: FuzzHandshakeMiddlewareAction,
}

#[async_trait]
impl WebSocketHandshakeMiddleware for FuzzHandshakeMiddlewareEntry {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, WsMiddlewareInitError> {
        Err(WsMiddlewareInitError::Internal)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new(
            fuzz_descriptor_name(self.index),
            MiddlewareKind::WebSocketHandshake,
        )
    }

    async fn handle(
        &self,
        exchange: &mut WsHandshakeExchange,
        _cancellation: crate::ExecutionCancellation,
    ) -> Result<(), WsHandshakeRejection> {
        exchange
            .insert_local(self.index)
            .expect("one fuzz handshake-local type is within the production bound");
        let code = bounded_code("WS_FUZZ_HANDSHAKE_REJECTED");
        match self.action {
            FuzzHandshakeMiddlewareAction::Accept => Ok(()),
            FuzzHandshakeMiddlewareAction::RejectBadRequest => {
                Err(WsHandshakeRejection::bad_request(code))
            }
            FuzzHandshakeMiddlewareAction::RejectUnauthorized => Err(
                WsHandshakeRejection::unauthorized(code, HeaderValue::from_static("Bearer"))
                    .expect("static fuzz authentication challenge is valid"),
            ),
            FuzzHandshakeMiddlewareAction::RejectForbidden => {
                Err(WsHandshakeRejection::forbidden(code))
            }
            FuzzHandshakeMiddlewareAction::Panic => panic!("fuzz handshake middleware panic"),
        }
    }
}

struct FuzzHandshakeObserver;

impl WsMiddlewareObserver for FuzzHandshakeObserver {
    fn observe(
        &self,
        _descriptor: MiddlewareDescriptor,
        _outcome: WsMiddlewareObservationOutcome,
        _duration: Duration,
    ) {
    }
}

async fn fuzz_container() -> Arc<ApplicationContainer> {
    static CONTAINER: OnceCell<Arc<ApplicationContainer>> = OnceCell::const_new();
    Arc::clone(
        CONTAINER
            .get_or_init(|| async {
                Arc::new(
                    ApplicationContainer::build()
                        .await
                        .expect("fuzz DI container must build"),
                )
            })
            .await,
    )
}

#[must_use]
pub async fn exercise_handshake(plan: FuzzHandshakePlan) -> FuzzHandshakeSnapshot {
    if plan.headers.len() > HANDSHAKE_MAX_HEADERS
        || plan.headers.iter().any(|header| {
            header.name.len() > HANDSHAKE_MAX_HEADER_NAME_BYTES
                || header.value.len() > HANDSHAKE_MAX_HEADER_VALUE_BYTES
        })
        || plan
            .query
            .as_ref()
            .is_some_and(|query| query.len() > HANDSHAKE_MAX_QUERY_BYTES)
        || plan.middlewares.len() > HANDSHAKE_MAX_MIDDLEWARES
    {
        return FuzzHandshakeSnapshot::rejected(400, FuzzHandshakeCode::InputLimit);
    }

    let mut raw_headers = HeaderMap::new();
    for header in plan.headers {
        let Ok(name) = HeaderName::from_bytes(&header.name) else {
            return FuzzHandshakeSnapshot::rejected(400, FuzzHandshakeCode::InvalidHeader);
        };
        let Ok(value) = HeaderValue::from_bytes(&header.value) else {
            return FuzzHandshakeSnapshot::rejected(400, FuzzHandshakeCode::InvalidHeader);
        };
        raw_headers.append(name, value);
    }

    if has_ambiguous_handshake_headers(&raw_headers) {
        return FuzzHandshakeSnapshot::rejected(400, FuzzHandshakeCode::AmbiguousHeader);
    }
    let Ok(collected) = collect_handshake_headers(&raw_headers) else {
        return FuzzHandshakeSnapshot::rejected(400, FuzzHandshakeCode::InvalidHeader);
    };
    let Ok(headers) = WsHeaders::try_from_http_headers(&collected) else {
        return FuzzHandshakeSnapshot::rejected(400, FuzzHandshakeCode::InvalidHeader);
    };
    let namespace = match parse_handshake_namespace(plan.query.as_deref()) {
        Ok(namespace) => namespace,
        Err(_) => {
            return FuzzHandshakeSnapshot::rejected(400, FuzzHandshakeCode::InvalidNamespace);
        }
    };
    let handshake_request = WsHandshakeRequest::new(
        namespace,
        headers.clone(),
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 443),
        RequestConnectionInfo::direct(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        WsTransportSecurity::Plaintext,
    );
    let secret_safe = !format!("{headers:?}{handshake_request:?}").contains(SECRET_SENTINEL);

    let config = ServerConfig {
        allowed_origins: plan.allowed_origins,
        allow_any_origin: plan.allow_any_origin,
        allow_missing_origin: plan.allow_missing_origin,
        supported_protocols: plan.supported_protocols,
        require_subprotocol: plan.require_subprotocol,
        ..ServerConfig::default()
    };
    if config.validate().is_err() {
        return FuzzHandshakeSnapshot {
            secret_safe,
            ..FuzzHandshakeSnapshot::rejected(400, FuzzHandshakeCode::InvalidConfiguration)
        };
    }

    let decision = match config.evaluate_handshake(&headers) {
        Ok(decision) => decision,
        Err(rejection) => {
            let code = match rejection {
                HandshakeRejection::OriginRequired => FuzzHandshakeCode::OriginRequired,
                HandshakeRejection::OriginDenied => FuzzHandshakeCode::OriginDenied,
                HandshakeRejection::SubprotocolRequired => FuzzHandshakeCode::SubprotocolRequired,
                HandshakeRejection::InvalidSubprotocol => FuzzHandshakeCode::InvalidSubprotocol,
                HandshakeRejection::InvalidNamespace => FuzzHandshakeCode::InvalidNamespace,
                HandshakeRejection::NamespaceNotFound => FuzzHandshakeCode::InvalidNamespace,
            };
            return FuzzHandshakeSnapshot {
                secret_safe,
                ..FuzzHandshakeSnapshot::rejected(rejection.status().as_u16(), code)
            };
        }
    };

    let middlewares = plan
        .middlewares
        .into_iter()
        .enumerate()
        .map(|(index, action)| {
            Arc::new(FuzzHandshakeMiddlewareEntry { index, action })
                as Arc<dyn WebSocketHandshakeMiddleware>
        })
        .collect::<Vec<_>>();
    let chain = CompiledWsHandshakeChain::compile(middlewares)
        .expect("bounded fuzz descriptors must compile");
    let container = fuzz_container().await;
    let mut exchange = WsHandshakeExchange::new(
        container.services(),
        handshake_request,
        CancellationToken::new(),
        Instant::now() + Duration::from_millis(25),
    );
    if let Err(failure) = chain.execute(&mut exchange, STAGE_TIMEOUT).await {
        let (status, code) = match failure.kind() {
            WsHandshakeExecutionFailureKind::Rejected(rejection) => {
                match rejection.status().as_u16() {
                    400 => (400, FuzzHandshakeCode::CustomBadRequest),
                    401 => (401, FuzzHandshakeCode::CustomUnauthorized),
                    403 => (403, FuzzHandshakeCode::CustomForbidden),
                    status => (status, FuzzHandshakeCode::CustomMiddlewarePanicked),
                }
            }
            WsHandshakeExecutionFailureKind::Timeout
            | WsHandshakeExecutionFailureKind::Cancelled
            | WsHandshakeExecutionFailureKind::Panicked => {
                (500, FuzzHandshakeCode::CustomMiddlewarePanicked)
            }
        };
        return FuzzHandshakeSnapshot {
            secret_safe,
            ..FuzzHandshakeSnapshot::rejected(status, code)
        };
    }

    let identity = match plan.identity {
        FuzzHandshakeIdentityAction::Disabled => WebSocketIdentity::anonymous(),
        action => {
            let identity = CompiledWsIdentityMiddleware::compile_observed(
                Arc::new(FuzzIdentityMiddleware(action)),
                Arc::new(FuzzHandshakeObserver),
            )
            .expect("bounded fuzz identity descriptor must compile");
            match identity.execute(&mut exchange, STAGE_TIMEOUT).await {
                Ok(identity) => identity,
                Err(failure) => {
                    let (status, code) = match failure.kind() {
                        WsHandshakeExecutionFailureKind::Rejected(rejection) => {
                            let status = rejection.status().as_u16();
                            let code = match status {
                                401 => FuzzHandshakeCode::IdentityRequired,
                                503 => FuzzHandshakeCode::IdentityUnavailable,
                                _ => FuzzHandshakeCode::IdentityRejected,
                            };
                            (status, code)
                        }
                        WsHandshakeExecutionFailureKind::Timeout
                        | WsHandshakeExecutionFailureKind::Cancelled => {
                            (503, FuzzHandshakeCode::IdentityUnavailable)
                        }
                        WsHandshakeExecutionFailureKind::Panicked => {
                            (500, FuzzHandshakeCode::IdentityPanicked)
                        }
                    };
                    return FuzzHandshakeSnapshot {
                        secret_safe,
                        ..FuzzHandshakeSnapshot::rejected(status, code)
                    };
                }
            }
        }
    };
    let (identity, connection_locals) = identity.into_parts();
    FuzzHandshakeSnapshot {
        status: 101,
        code: FuzzHandshakeCode::Accepted,
        selected_subprotocol: decision.subprotocol.is_some(),
        has_principal: identity.principal().is_some(),
        has_connection_local: connection_locals.get::<FuzzConnectionLocal>().is_some(),
        secret_safe,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuzzMessageAction {
    Continue,
    Reject,
    Close,
    Error,
    Pending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuzzMessageMiddlewarePlan {
    pub before: FuzzMessageAction,
    pub after: FuzzMessageAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuzzInitialMessageOutcome {
    Handled,
    Rejected,
    Close,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzMessageChainPlan {
    pub middlewares: Vec<FuzzMessageMiddlewarePlan>,
    pub initial_outcome: FuzzInitialMessageOutcome,
    pub cancel_before: bool,
    pub cancel_after: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuzzMessageTerminal {
    Handled,
    Rejected,
    Close,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzMessageChainSnapshot {
    pub before_order: Vec<u8>,
    pub after_order: Vec<u8>,
    pub termination_order: Vec<u8>,
    pub exit_order: Vec<u8>,
    pub entered: usize,
    pub after_attempted: usize,
    pub after_failed: usize,
    pub terminal: FuzzMessageTerminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FuzzMessageEvent {
    Before(u8),
    After(u8),
    Termination(u8),
}

struct FuzzMessageMiddlewareEntry {
    index: u8,
    before: FuzzMessageAction,
    after: FuzzMessageAction,
    events: Arc<Mutex<Vec<FuzzMessageEvent>>>,
}

#[async_trait]
impl WsMessageMiddleware for FuzzMessageMiddlewareEntry {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, WsMiddlewareInitError> {
        Err(WsMiddlewareInitError::Internal)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new(
            fuzz_descriptor_name(usize::from(self.index)),
            MiddlewareKind::WebSocketMessage,
        )
    }

    async fn before_message(
        &self,
        _exchange: &mut WsMessageExchange,
        _cancellation: crate::ExecutionCancellation,
    ) -> Result<WsMessageDecision, WsMiddlewareError> {
        mutex_guard(&self.events).push(FuzzMessageEvent::Before(self.index));
        run_message_action(self.before).await
    }

    async fn after_message(
        &self,
        _exchange: &mut WsMessageExchange,
        _outcome: WsMessageOutcome,
        _cancellation: crate::ExecutionCancellation,
    ) -> Result<WsMessageDecision, WsMiddlewareError> {
        mutex_guard(&self.events).push(FuzzMessageEvent::After(self.index));
        run_message_action(self.after).await
    }

    async fn on_message_termination(
        &self,
        _context: crate::middleware::WsMessageTerminationContext<'_>,
        _cancellation: crate::CleanupCancellation,
    ) -> Result<(), WsMiddlewareError> {
        mutex_guard(&self.events).push(FuzzMessageEvent::Termination(self.index));
        run_message_action(self.after).await.map(|_| ())
    }
}

async fn run_message_action(
    action: FuzzMessageAction,
) -> Result<WsMessageDecision, WsMiddlewareError> {
    match action {
        FuzzMessageAction::Continue => Ok(WsMessageDecision::Continue),
        FuzzMessageAction::Reject => Ok(WsMessageDecision::Reject(
            WsProtocolErrorCode::MiddlewareRejected,
        )),
        FuzzMessageAction::Close => Ok(WsMessageDecision::Close(WsCloseReason::PolicyViolation)),
        FuzzMessageAction::Error => Err(WsMiddlewareError::internal(bounded_code(
            "WS_FUZZ_MESSAGE_ERROR",
        ))),
        FuzzMessageAction::Pending => pending().await,
    }
}

pub async fn exercise_message_chain(plan: FuzzMessageChainPlan) -> FuzzMessageChainSnapshot {
    let events = Arc::new(Mutex::new(Vec::new()));
    let middlewares = plan
        .middlewares
        .into_iter()
        .take(MESSAGE_MAX_MIDDLEWARES)
        .enumerate()
        .map(|(index, middleware)| {
            Arc::new(FuzzMessageMiddlewareEntry {
                index: u8::try_from(index).expect("fuzz middleware bound fits u8"),
                before: middleware.before,
                after: middleware.after,
                events: Arc::clone(&events),
            }) as Arc<dyn WsMessageMiddleware>
        })
        .collect::<Vec<_>>();
    let chain = CompiledWsMessageChain::compile(middlewares)
        .expect("bounded fuzz descriptors must compile");
    let manager = Arc::new(ConnectionManager::with_registered_namespaces(
        128,
        128,
        ["fuzz".into()],
    ));
    let connection_id = Uuid::nil();
    let context = Arc::new(WebSocketContext::new(connection_id, manager, "fuzz".into()));
    let request = Arc::new(
        WsRequest::new_from_message(
            connection_id,
            WsMessageBody::try_new("fuzz:event", serde_json::Value::Null)
                .expect("static fuzz envelope serializes")
                .to_message()
                .expect("static fuzz envelope is valid"),
            WsHeaders::default(),
            RequestConnectionInfo::default(),
            WsTransportSecurity::Plaintext,
        )
        .expect("static fuzz request is valid"),
    );
    let cancellation = CancellationToken::new();
    if plan.cancel_before {
        cancellation.cancel();
    }
    let mut exchange = WsMessageExchange::new_without_services(
        context,
        request,
        WebSocketMessageLocals::default(),
        cancellation.clone(),
        Instant::now() + STAGE_TIMEOUT,
    );
    let (mut ledger, before_result) = chain.before(&mut exchange).await;
    let entered = ledger.entered();
    let outcome = match before_result {
        Ok(WsMessageDecision::Continue) => initial_message_outcome(plan.initial_outcome),
        Ok(WsMessageDecision::Reject(code)) => WsMessageOutcome::Rejected(code),
        Ok(WsMessageDecision::Close(reason)) => WsMessageOutcome::Close(reason),
        Err(error) => match error.error().kind() {
            WsMiddlewareFailureKind::Rejected => {
                WsMessageOutcome::Rejected(WsProtocolErrorCode::MiddlewareRejected)
            }
            WsMiddlewareFailureKind::Cancelled => {
                WsMessageOutcome::Close(WsCloseReason::ServerShutdown)
            }
            WsMiddlewareFailureKind::Timeout | WsMiddlewareFailureKind::Internal => {
                WsMessageOutcome::Failed(error.error().diagnostic_code())
            }
        },
    };
    if plan.cancel_after {
        cancellation.cancel();
    }
    let report = chain.after(&mut exchange, &mut ledger, outcome).await;
    chain
        .terminate(
            &mut exchange,
            &mut ledger,
            report.outcome(),
            STAGE_TIMEOUT,
            &CancellationToken::new(),
        )
        .await;
    assert_eq!(
        ledger.report(outcome).outcome(),
        report.outcome(),
        "termination cannot rewrite the response"
    );
    let captured = mutex_guard(&events).clone();
    let before_order = captured
        .iter()
        .filter_map(|event| match event {
            FuzzMessageEvent::Before(index) => Some(*index),
            FuzzMessageEvent::After(_) | FuzzMessageEvent::Termination(_) => None,
        })
        .collect();
    let after_order = captured
        .iter()
        .filter_map(|event| match event {
            FuzzMessageEvent::After(index) => Some(*index),
            FuzzMessageEvent::Before(_) | FuzzMessageEvent::Termination(_) => None,
        })
        .collect();
    FuzzMessageChainSnapshot {
        before_order,
        after_order,
        termination_order: captured
            .iter()
            .filter_map(|event| match event {
                FuzzMessageEvent::Termination(index) => Some(*index),
                _ => None,
            })
            .collect(),
        exit_order: captured
            .iter()
            .filter_map(|event| match event {
                FuzzMessageEvent::After(index) | FuzzMessageEvent::Termination(index) => {
                    Some(*index)
                }
                FuzzMessageEvent::Before(_) => None,
            })
            .collect(),
        entered,
        after_attempted: report.attempted(),
        after_failed: report.failed(),
        terminal: fuzz_message_terminal(report.outcome()),
    }
}

fn initial_message_outcome(outcome: FuzzInitialMessageOutcome) -> WsMessageOutcome {
    match outcome {
        FuzzInitialMessageOutcome::Handled => WsMessageOutcome::Handled,
        FuzzInitialMessageOutcome::Rejected => {
            WsMessageOutcome::Rejected(WsProtocolErrorCode::AuthorizationDenied)
        }
        FuzzInitialMessageOutcome::Close => WsMessageOutcome::Close(WsCloseReason::PolicyViolation),
        FuzzInitialMessageOutcome::Failed => {
            WsMessageOutcome::Failed(bounded_code("WS_FUZZ_HANDLER_FAILED"))
        }
    }
}

fn fuzz_message_terminal(outcome: WsMessageOutcome) -> FuzzMessageTerminal {
    match outcome {
        WsMessageOutcome::Handled => FuzzMessageTerminal::Handled,
        WsMessageOutcome::Rejected(_) => FuzzMessageTerminal::Rejected,
        WsMessageOutcome::Close(_) => FuzzMessageTerminal::Close,
        WsMessageOutcome::Failed(_) => FuzzMessageTerminal::Failed,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuzzConnectionAction {
    Continue,
    Reject,
    Error,
    Pending,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FuzzConnectionMiddlewarePlan {
    pub admit: FuzzConnectionAction,
    pub opened: FuzzConnectionAction,
    pub closed: FuzzConnectionAction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FuzzLifecycleEvent {
    Connect,
    Opened,
    MessageContinue,
    MessageReject,
    MessageClose,
    ClientClose,
    Reset,
    IdleTimeout,
    ServerShutdown,
    Cancellation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzLifecyclePlan {
    pub middlewares: Vec<FuzzConnectionMiddlewarePlan>,
    pub events: Vec<FuzzLifecycleEvent>,
    pub cancel_before_admission: bool,
    pub cancel_before_opened: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FuzzLifecycleSnapshot {
    pub admit_order: Vec<u8>,
    pub opened_order: Vec<u8>,
    pub closed_order: Vec<u8>,
    pub entered: usize,
    pub cleanup_attempted: usize,
    pub cleanup_failed: usize,
    pub manager_connections: usize,
    pub registry_entries: usize,
    pub available_permits: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FuzzConnectionEvent {
    Admit(u8),
    Opened(u8),
    Closed(u8),
}

struct FuzzConnectionMiddlewareEntry {
    index: u8,
    plan: FuzzConnectionMiddlewarePlan,
    events: Arc<Mutex<Vec<FuzzConnectionEvent>>>,
}

#[async_trait]
impl WsConnectionMiddleware for FuzzConnectionMiddlewareEntry {
    async fn new(
        _extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, WsMiddlewareInitError> {
        Err(WsMiddlewareInitError::Internal)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new(
            fuzz_descriptor_name(usize::from(self.index)),
            MiddlewareKind::WebSocketConnection,
        )
    }

    async fn admit(
        &self,
        _context: Arc<WebSocketContext>,
        _cancellation: crate::ExecutionCancellation,
    ) -> Result<(), WsMiddlewareError> {
        mutex_guard(&self.events).push(FuzzConnectionEvent::Admit(self.index));
        run_connection_action(self.plan.admit).await
    }

    async fn opened(
        &self,
        _context: Arc<WebSocketContext>,
        _cancellation: crate::ExecutionCancellation,
    ) -> Result<(), WsMiddlewareError> {
        mutex_guard(&self.events).push(FuzzConnectionEvent::Opened(self.index));
        run_connection_action(self.plan.opened).await
    }

    async fn closed(
        &self,
        _context: Arc<WebSocketContext>,
        _category: WsConnectionCloseCategory,
        _cancellation: crate::CleanupCancellation,
    ) -> Result<(), WsMiddlewareError> {
        mutex_guard(&self.events).push(FuzzConnectionEvent::Closed(self.index));
        run_connection_action(self.plan.closed).await
    }
}

async fn run_connection_action(action: FuzzConnectionAction) -> Result<(), WsMiddlewareError> {
    match action {
        FuzzConnectionAction::Continue => Ok(()),
        FuzzConnectionAction::Reject => Err(WsMiddlewareError::rejected(bounded_code(
            "WS_FUZZ_CONNECTION_REJECTED",
        ))),
        FuzzConnectionAction::Error => Err(WsMiddlewareError::internal(bounded_code(
            "WS_FUZZ_CONNECTION_ERROR",
        ))),
        FuzzConnectionAction::Pending => pending().await,
    }
}

pub async fn exercise_lifecycle(plan: FuzzLifecyclePlan) -> FuzzLifecycleSnapshot {
    let events = Arc::new(Mutex::new(Vec::new()));
    let middlewares = plan
        .middlewares
        .into_iter()
        .take(LIFECYCLE_MAX_MIDDLEWARES)
        .enumerate()
        .map(|(index, plan)| {
            Arc::new(FuzzConnectionMiddlewareEntry {
                index: u8::try_from(index).expect("fuzz middleware bound fits u8"),
                plan,
                events: Arc::clone(&events),
            }) as Arc<dyn WsConnectionMiddleware>
        })
        .collect::<Vec<_>>();
    let chain = Arc::new(
        CompiledWsConnectionChain::compile(middlewares)
            .expect("bounded fuzz descriptors must compile"),
    );
    let manager = Arc::new(ConnectionManager::with_registered_namespaces(
        128,
        128,
        ["fuzz".into()],
    ));
    let connection_id = Uuid::nil();
    let context = Arc::new(WebSocketContext::new(
        connection_id,
        Arc::clone(&manager),
        "fuzz".into(),
    ));
    let registry = ConnectionCleanupRegistry::new().expect("fuzz runtime is active");
    let mut cleanup = Some(
        registry
            .register(
                connection_id,
                Arc::clone(&chain),
                Arc::clone(&context),
                None,
                STAGE_TIMEOUT,
            )
            .expect("one bounded fuzz connection registers"),
    );
    let ledger = cleanup.as_ref().expect("cleanup lease exists").ledger();
    let cancellation = CancellationToken::new();
    let permits = Arc::new(Semaphore::new(1));
    let permit = Arc::clone(&permits)
        .acquire_owned()
        .await
        .expect("fuzz semaphore remains open");
    let (sender, receiver) = mpsc::channel(2);
    let (control_sender, _control_receiver) = crate::connection::connection_control_channel();
    let mut receiver = Some(receiver);
    let mut admission_attempted = false;
    let mut admitted = false;
    let mut opened = false;
    let mut entered = 0_usize;
    let mut cleanup_attempted = 0_usize;
    let mut cleanup_failed = 0_usize;
    let mut terminal = false;

    for event in plan.events.into_iter().take(LIFECYCLE_MAX_EVENTS) {
        if terminal {
            break;
        }
        match event {
            FuzzLifecycleEvent::Connect if !admission_attempted => {
                admission_attempted = true;
                if plan.cancel_before_admission {
                    cancellation.cancel();
                }
                match chain
                    .admit(Arc::clone(&context), &ledger, STAGE_TIMEOUT, &cancellation)
                    .await
                {
                    Ok(()) => {
                        entered = ledger.entered();
                        manager
                            .add_connection_with_control(
                                connection_id,
                                sender.clone(),
                                control_sender.clone(),
                                RequestConnectionInfo::default(),
                                WsTransportSecurity::Plaintext,
                                Some("fuzz".into()),
                            )
                            .await
                            .expect("one fuzz connection is admitted once");
                        cleanup
                            .as_ref()
                            .expect("cleanup lease exists")
                            .attach_manager(Arc::clone(&manager))
                            .expect("manager ownership transfers once");
                        admitted = true;
                    }
                    Err(error) => {
                        let (category, _) = connection_middleware_terminal(error.error().kind());
                        let outcome = cleanup
                            .take()
                            .expect("cleanup lease exists")
                            .finalize(category)
                            .await;
                        add_cleanup_outcome(outcome, &mut cleanup_attempted, &mut cleanup_failed);
                        terminal = true;
                    }
                }
            }
            FuzzLifecycleEvent::Opened if admitted && !opened => {
                opened = true;
                if plan.cancel_before_opened {
                    cancellation.cancel();
                }
                if let Err(error) = chain
                    .opened(Arc::clone(&context), &ledger, STAGE_TIMEOUT, &cancellation)
                    .await
                {
                    let (category, _) = connection_middleware_terminal(error.error().kind());
                    let outcome = cleanup
                        .take()
                        .expect("cleanup lease exists")
                        .finalize(category)
                        .await;
                    add_cleanup_outcome(outcome, &mut cleanup_attempted, &mut cleanup_failed);
                    terminal = true;
                }
            }
            FuzzLifecycleEvent::MessageContinue | FuzzLifecycleEvent::MessageReject => {}
            FuzzLifecycleEvent::MessageClose => {
                let category = WsApp::close_category_from_reason(WsCloseReason::PolicyViolation);
                let outcome = cleanup
                    .take()
                    .expect("cleanup lease exists")
                    .finalize(category)
                    .await;
                add_cleanup_outcome(outcome, &mut cleanup_attempted, &mut cleanup_failed);
                terminal = true;
            }
            FuzzLifecycleEvent::ClientClose => {
                let outcome = cleanup
                    .take()
                    .expect("cleanup lease exists")
                    .finalize(WsConnectionCloseCategory::NormalPeer)
                    .await;
                add_cleanup_outcome(outcome, &mut cleanup_attempted, &mut cleanup_failed);
                terminal = true;
            }
            FuzzLifecycleEvent::Reset => {
                let outcome = cleanup
                    .take()
                    .expect("cleanup lease exists")
                    .finalize(WsConnectionCloseCategory::Reset)
                    .await;
                add_cleanup_outcome(outcome, &mut cleanup_attempted, &mut cleanup_failed);
                terminal = true;
            }
            FuzzLifecycleEvent::IdleTimeout => {
                if let Some(outcome) = registry
                    .finalize_connection(connection_id, WsConnectionCloseCategory::IdleTimeout)
                    .await
                {
                    add_cleanup_outcome(outcome, &mut cleanup_attempted, &mut cleanup_failed);
                }
                drop(cleanup.take());
                terminal = true;
            }
            FuzzLifecycleEvent::ServerShutdown => {
                let report = registry
                    .finalize_all(
                        WsConnectionCloseCategory::ServerShutdown,
                        Instant::now() + STAGE_TIMEOUT,
                    )
                    .await;
                cleanup_attempted = cleanup_attempted.saturating_add(report.hooks_attempted);
                cleanup_failed = cleanup_failed.saturating_add(report.hooks_failed);
                drop(cleanup.take());
                terminal = true;
            }
            FuzzLifecycleEvent::Cancellation => {
                cancellation.cancel();
                drop(cleanup.take());
                let report = registry
                    .finalize_all(
                        WsConnectionCloseCategory::Cancelled,
                        Instant::now() + STAGE_TIMEOUT,
                    )
                    .await;
                cleanup_attempted = cleanup_attempted.saturating_add(report.hooks_attempted);
                cleanup_failed = cleanup_failed.saturating_add(report.hooks_failed);
                terminal = true;
            }
            FuzzLifecycleEvent::Connect | FuzzLifecycleEvent::Opened => {}
        }
    }

    if !terminal {
        drop(cleanup.take());
        let report = registry
            .finalize_all(
                WsConnectionCloseCategory::Cancelled,
                Instant::now() + STAGE_TIMEOUT,
            )
            .await;
        cleanup_attempted = cleanup_attempted.saturating_add(report.hooks_attempted);
        cleanup_failed = cleanup_failed.saturating_add(report.hooks_failed);
    }
    drop(receiver.take());
    drop(permit);

    let captured = mutex_guard(&events).clone();
    let admit_order = captured
        .iter()
        .filter_map(|event| match event {
            FuzzConnectionEvent::Admit(index) => Some(*index),
            FuzzConnectionEvent::Opened(_) | FuzzConnectionEvent::Closed(_) => None,
        })
        .collect();
    let opened_order = captured
        .iter()
        .filter_map(|event| match event {
            FuzzConnectionEvent::Opened(index) => Some(*index),
            FuzzConnectionEvent::Admit(_) | FuzzConnectionEvent::Closed(_) => None,
        })
        .collect();
    let closed_order = captured
        .iter()
        .filter_map(|event| match event {
            FuzzConnectionEvent::Closed(index) => Some(*index),
            FuzzConnectionEvent::Admit(_) | FuzzConnectionEvent::Opened(_) => None,
        })
        .collect();
    FuzzLifecycleSnapshot {
        admit_order,
        opened_order,
        closed_order,
        entered,
        cleanup_attempted,
        cleanup_failed,
        manager_connections: manager.connection_count().await,
        registry_entries: registry.entry_count(),
        available_permits: permits.available_permits(),
    }
}

fn add_cleanup_outcome(
    outcome: ConnectionCleanupOutcome,
    attempted: &mut usize,
    failed: &mut usize,
) {
    if let Some(report) = outcome.report() {
        *attempted = attempted.saturating_add(report.attempted());
        *failed = failed.saturating_add(report.failed());
    } else {
        *failed = failed.saturating_add(1);
    }
    if outcome.manager_cleanup_failed() {
        *failed = failed.saturating_add(1);
    }
}

fn bounded_code(value: &'static str) -> MiddlewareErrorCode {
    MiddlewareErrorCode::new(value).unwrap_or(MiddlewareErrorCode::INTERNAL)
}

fn fuzz_descriptor_name(index: usize) -> &'static str {
    const NAMES: [&str; 32] = [
        "ws_fuzz_00",
        "ws_fuzz_01",
        "ws_fuzz_02",
        "ws_fuzz_03",
        "ws_fuzz_04",
        "ws_fuzz_05",
        "ws_fuzz_06",
        "ws_fuzz_07",
        "ws_fuzz_08",
        "ws_fuzz_09",
        "ws_fuzz_10",
        "ws_fuzz_11",
        "ws_fuzz_12",
        "ws_fuzz_13",
        "ws_fuzz_14",
        "ws_fuzz_15",
        "ws_fuzz_16",
        "ws_fuzz_17",
        "ws_fuzz_18",
        "ws_fuzz_19",
        "ws_fuzz_20",
        "ws_fuzz_21",
        "ws_fuzz_22",
        "ws_fuzz_23",
        "ws_fuzz_24",
        "ws_fuzz_25",
        "ws_fuzz_26",
        "ws_fuzz_27",
        "ws_fuzz_28",
        "ws_fuzz_29",
        "ws_fuzz_30",
        "ws_fuzz_31",
    ];
    NAMES[index.min(NAMES.len() - 1)]
}

fn mutex_guard<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn backplane_adapter_exercises_valid_production_ingress_seeds() {
        let seeds: &[&[u8]] = &[
            include_bytes!("../fuzz/corpus/websocket_backplane_envelope/valid-namespace-text.json"),
            include_bytes!("../fuzz/corpus/websocket_backplane_envelope/valid-principal-text.json"),
            include_bytes!(
                "../fuzz/corpus/websocket_backplane_envelope/valid-namespace-connections.json"
            ),
            include_bytes!(
                "../fuzz/corpus/websocket_backplane_envelope/valid-rooms-binary-trace.json"
            ),
        ];

        for seed in seeds {
            let snapshot = exercise_backplane_envelope(seed.to_vec()).await;
            assert_eq!(snapshot.invalid_frames, 0);
            assert_eq!(snapshot.duplicates_suppressed, 0);
            assert_eq!(snapshot.origin_loops_suppressed, 0);
        }
    }

    #[tokio::test]
    async fn backplane_adapter_observes_one_bounded_rejection_for_each_invalid_seed() {
        let seeds: &[&[u8]] = &[
            include_bytes!("../fuzz/corpus/websocket_backplane_envelope/duplicate-exclusion.json"),
            include_bytes!("../fuzz/corpus/websocket_backplane_envelope/duplicate-room.json"),
            include_bytes!(
                "../fuzz/corpus/websocket_backplane_envelope/invalid-namespace-connections.json"
            ),
            include_bytes!(
                "../fuzz/corpus/websocket_backplane_envelope/invalid-embedded-message.json"
            ),
            include_bytes!(
                "../fuzz/corpus/websocket_backplane_envelope/invalid-principal-empty.json"
            ),
            include_bytes!("../fuzz/corpus/websocket_backplane_envelope/invalid-traceparent.json"),
            include_bytes!("../fuzz/corpus/websocket_backplane_envelope/invalid-wire-format.json"),
            include_bytes!("../fuzz/corpus/websocket_backplane_envelope/malformed-json.seed"),
            include_bytes!("../fuzz/corpus/websocket_backplane_envelope/nil-identifiers.json"),
            include_bytes!("../fuzz/corpus/websocket_backplane_envelope/unknown-field.json"),
            include_bytes!(
                "../fuzz/corpus/websocket_backplane_envelope/unsupported-version-v3.json"
            ),
            include_bytes!("../fuzz/corpus/websocket_backplane_envelope/invalid-unscoped-all.json"),
            include_bytes!(
                "../fuzz/corpus/websocket_backplane_envelope/invalid-unscoped-connections.json"
            ),
            include_bytes!(
                "../fuzz/corpus/websocket_backplane_envelope/invalid-unknown-target-field.json"
            ),
            include_bytes!("../fuzz/corpus/websocket_backplane_envelope/unsupported-version.json"),
            include_bytes!(
                "../fuzz/corpus/websocket_backplane_envelope/unsupported-version-v1.json"
            ),
            include_bytes!(
                "../fuzz/corpus/websocket_backplane_envelope/unsupported-version-v2.json"
            ),
        ];

        for seed in seeds {
            let snapshot = exercise_backplane_envelope(seed.to_vec()).await;
            assert_eq!(snapshot.invalid_frames, 1);
            assert_eq!(snapshot.duplicates_suppressed, 0);
            assert_eq!(snapshot.origin_loops_suppressed, 0);
        }
    }

    #[tokio::test]
    async fn handshake_adapter_uses_production_origin_and_subprotocol_policy() {
        let snapshot = exercise_handshake(FuzzHandshakePlan {
            headers: vec![
                FuzzHandshakeHeader {
                    name: b"origin".to_vec(),
                    value: b"https://app.example".to_vec(),
                },
                FuzzHandshakeHeader {
                    name: b"sec-websocket-protocol".to_vec(),
                    value: b"lily.v1".to_vec(),
                },
            ],
            query: Some("namespace=orders".into()),
            allowed_origins: vec!["https://app.example".into()],
            allow_any_origin: false,
            allow_missing_origin: false,
            supported_protocols: vec!["lily.v1".into()],
            require_subprotocol: true,
            identity: FuzzHandshakeIdentityAction::Authenticated,
            middlewares: vec![FuzzHandshakeMiddlewareAction::Accept],
        })
        .await;
        assert_eq!(snapshot.status, 101);
        assert!(snapshot.selected_subprotocol);
        assert!(snapshot.has_principal);
        assert!(snapshot.has_connection_local);
        assert!(snapshot.secret_safe);
    }

    #[tokio::test]
    async fn message_adapter_preserves_entered_prefix_reverse_order() {
        let snapshot = exercise_message_chain(FuzzMessageChainPlan {
            middlewares: vec![
                FuzzMessageMiddlewarePlan {
                    before: FuzzMessageAction::Continue,
                    after: FuzzMessageAction::Continue,
                },
                FuzzMessageMiddlewarePlan {
                    before: FuzzMessageAction::Reject,
                    after: FuzzMessageAction::Continue,
                },
                FuzzMessageMiddlewarePlan {
                    before: FuzzMessageAction::Continue,
                    after: FuzzMessageAction::Continue,
                },
            ],
            initial_outcome: FuzzInitialMessageOutcome::Handled,
            cancel_before: false,
            cancel_after: false,
        })
        .await;
        assert_eq!(snapshot.before_order, [0, 1]);
        assert_eq!(snapshot.entered, 1);
        assert_eq!(snapshot.after_order, [0]);
    }

    #[tokio::test(start_paused = true)]
    async fn message_adapter_exercises_interrupted_normal_and_termination_order_together() {
        let snapshot = exercise_message_chain(FuzzMessageChainPlan {
            middlewares: [
                FuzzMessageAction::Continue,
                FuzzMessageAction::Pending,
                FuzzMessageAction::Continue,
            ]
            .into_iter()
            .map(|after| FuzzMessageMiddlewarePlan {
                before: FuzzMessageAction::Continue,
                after,
            })
            .collect(),
            initial_outcome: FuzzInitialMessageOutcome::Handled,
            cancel_before: false,
            cancel_after: false,
        })
        .await;
        assert_eq!(snapshot.entered, 3);
        assert_eq!(snapshot.after_order, [2, 1]);
        assert_eq!(snapshot.termination_order.first(), Some(&1));
        // The intentionally tiny fuzz budget may expire before the outer hook.
        assert!(
            snapshot
                .exit_order
                .windows(2)
                .all(|pair| pair[0] >= pair[1])
        );
        assert_eq!(snapshot.after_failed, 1);
        assert_eq!(snapshot.terminal, FuzzMessageTerminal::Failed);
    }

    #[tokio::test]
    async fn lifecycle_adapter_reconciles_manager_registry_and_permit() {
        let snapshot = exercise_lifecycle(FuzzLifecyclePlan {
            middlewares: vec![FuzzConnectionMiddlewarePlan {
                admit: FuzzConnectionAction::Continue,
                opened: FuzzConnectionAction::Continue,
                closed: FuzzConnectionAction::Continue,
            }],
            events: vec![
                FuzzLifecycleEvent::Connect,
                FuzzLifecycleEvent::Opened,
                FuzzLifecycleEvent::ServerShutdown,
            ],
            cancel_before_admission: false,
            cancel_before_opened: false,
        })
        .await;
        assert_eq!(snapshot.closed_order, [0]);
        assert_eq!(snapshot.manager_connections, 0);
        assert_eq!(snapshot.registry_entries, 0);
        assert_eq!(snapshot.available_permits, 1);
    }
}
