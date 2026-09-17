use super::*;

use crate::controller::{
    BoundWebSocketOperation, ErasedWebSocketController, PendingWebSocketOperation,
    WebSocketActionFuture, WebSocketActionRegistration, WebSocketAsyncApiRegistration,
    WebSocketConnectionMiddlewareRegistration, WebSocketControllerBindingError,
    WebSocketControllerDefinition, WebSocketControllerInitError, WebSocketControllerRegistration,
    WebSocketGuardRegistration, WebSocketHandshakeMiddlewareRegistration, WebSocketMessageAction,
    WebSocketMessageMiddlewareRegistration, WebSocketOperationKind, WebSocketOperationMetadata,
    downcast_websocket_controller,
};
use crate::guard::{GuardInitializationError, WebSocketGuardRejection, WsGuard};
use crate::request::WsHeaders;
use async_trait::async_trait;
use lily_error::injection::InjectionError;
use lily_injection::Injectable;
use lily_injection::ServiceTrait;
use lily_middleware::{MiddlewareDescriptor, MiddlewareKind};
use std::future::pending;
use std::sync::atomic::{AtomicUsize, Ordering};

static MESSAGE_SCOPE_QUALIFICATION_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
static PROBE_INITIALIZED: AtomicUsize = AtomicUsize::new(0);
static PROBE_RESOLVED: AtomicUsize = AtomicUsize::new(0);
static PROBE_DISPOSED: AtomicUsize = AtomicUsize::new(0);
static PROBE_DISPOSING: AtomicUsize = AtomicUsize::new(0);
static PROBE_DISPOSE_GATE: StdMutex<Option<Arc<Semaphore>>> = StdMutex::new(None);
static OWNER_EVENTS: StdMutex<Vec<&'static str>> = StdMutex::new(Vec::new());
static MESSAGE_STATE_DROPPED: AtomicUsize = AtomicUsize::new(0);
static OWNER_COOPERATIVE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static OWNER_PENDING_AFTER: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);
static OWNER_PENDING_TERMINATION: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

async fn cooperative_return(signal: crate::ExecutionCancellation) {
    signal.cancelled().await;
    tokio::time::sleep(Duration::from_millis(5)).await;
    OWNER_EVENTS.lock().unwrap().push("cooperative.return");
}

struct RetainedMessageState;
impl Drop for RetainedMessageState {
    fn drop(&mut self) {
        MESSAGE_STATE_DROPPED.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct MessageScopeProbe;

#[async_trait]
impl ServiceTrait for MessageScopeProbe {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        assert!(
            ProcessContext::current().is_some(),
            "a scoped message service must initialize inside its message context"
        );
        PROBE_INITIALIZED.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        assert!(
            ProcessContext::current().is_some(),
            "a scoped message service must dispose inside the same message context"
        );
        PROBE_DISPOSING.fetch_add(1, Ordering::SeqCst);
        let gate = PROBE_DISPOSE_GATE.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.acquire().await.unwrap().forget();
        }
        PROBE_DISPOSED.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct MessageScopeController;

#[async_trait]
impl crate::controller::WebSocketControllerTrait for MessageScopeController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

impl WebSocketControllerDefinition for MessageScopeController {
    fn namespace() -> &'static str {
        "message-scope"
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

struct MessageScopeAction {
    _controller: Arc<MessageScopeController>,
}

impl WebSocketMessageAction for MessageScopeAction {
    fn call(&self, invocation: WebSocketMessageInvocation) -> WebSocketActionFuture {
        let event = invocation.event().to_owned();
        let signal = invocation.cancellation().clone();
        let extensions = invocation.extensions();
        Box::pin(async move {
            let _probe = extensions
                .get_service::<MessageScopeProbe>(None)
                .await
                .expect("typed action must resolve from its active message scope");
            PROBE_RESOLVED.fetch_add(1, Ordering::SeqCst);
            OWNER_EVENTS.lock().unwrap().push("action");

            match event.as_str() {
                "success" => Ok(PendingWebSocketActionOutcome::NoReply),
                "timeout" | "disconnect" | "shutdown" => {
                    if OWNER_COOPERATIVE.load(Ordering::SeqCst) {
                        cooperative_return(signal).await;
                        return Ok(PendingWebSocketActionOutcome::NoReply);
                    }
                    pending::<()>().await;
                    unreachable!("pending qualification action completed")
                }
                "panic" => panic!("intentional message-scope qualification panic"),
                other => panic!("unexpected message-scope qualification event: {other}"),
            }
        })
    }
}

fn bind_message_scope_action(
    controller: ErasedWebSocketController,
) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
    let controller = downcast_websocket_controller::<MessageScopeController>(controller)?;
    Ok(BoundWebSocketOperation::Message(Arc::new(
        MessageScopeAction {
            _controller: controller,
        },
    )))
}

struct RejectingScopeMiddleware;

#[async_trait]
impl WsMessageMiddleware for RejectingScopeMiddleware {
    async fn new(
        _extensions: Arc<Extensions>,
    ) -> Result<Self, crate::middleware::WsMiddlewareInitError> {
        Ok(Self)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new(
            "qualification_message_scope_rejection",
            MiddlewareKind::WebSocketMessage,
        )
    }

    async fn before_message(
        &self,
        exchange: &mut WsMessageExchange,
        _cancellation: crate::ExecutionCancellation,
    ) -> Result<WsMessageDecision, crate::middleware::WsMiddlewareError> {
        let _probe = exchange
            .service::<MessageScopeProbe>()
            .await
            .expect("middleware must resolve from its active message scope");
        PROBE_RESOLVED.fetch_add(1, Ordering::SeqCst);
        Ok(WsMessageDecision::Reject(
            crate::request::WsProtocolErrorCode::MiddlewareRejected,
        ))
    }
}

struct RejectingScopeGuard;

#[async_trait]
impl WsGuard for RejectingScopeGuard {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, GuardInitializationError> {
        Ok(Self)
    }

    async fn can_activate(
        &self,
        exchange: &mut WsMessageExchange,
        _cancellation: crate::ExecutionCancellation,
    ) -> Result<(), WebSocketGuardRejection> {
        let _probe = exchange
            .service::<MessageScopeProbe>()
            .await
            .expect("guard must resolve from its active message scope");
        PROBE_RESOLVED.fetch_add(1, Ordering::SeqCst);
        Err(WebSocketGuardRejection::error(
            crate::WebSocketErrorCode::new("MESSAGE_SCOPE_GUARD_REJECTED").unwrap(),
            "The qualification guard rejected this message.",
        )
        .unwrap())
    }
}

fn operation(
    event: &'static str,
    middleware: Vec<WebSocketMessageMiddlewareRegistration>,
    guards: Vec<WebSocketGuardRegistration>,
    timeout: Option<Duration>,
) -> PendingWebSocketOperation {
    PendingWebSocketOperation::new(
        WebSocketOperationKind::Message,
        Some(event),
        "app::message_scope_qualification_tests::MessageScopeController::message",
        WebSocketActionRegistration::of::<MessageScopeController>(bind_message_scope_action),
        WebSocketOperationMetadata::new(
            middleware,
            guards,
            None,
            timeout,
            WebSocketAsyncApiRegistration::unspecified(),
        ),
    )
}

async fn action_table(
    container: &ApplicationContainer,
    operation: PendingWebSocketOperation,
) -> Arc<WebSocketActionTable> {
    Arc::new(
        materialize_websocket_controllers(
            vec![WebSocketControllerRegistration::of::<MessageScopeController>()],
            vec![operation],
            container.services(),
        )
        .await
        .expect("message-scope qualification action must materialize"),
    )
}

fn qualification_request(connection_id: Uuid, event: &str) -> WsRequest {
    let route = format!("message-scope:{event}");
    let frame = crate::request::WsMessageBody::try_new(&route, serde_json::Value::Null)
        .unwrap()
        .with_namespace("message-scope".to_owned())
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

fn decode_qualification_request(request: &WsRequest) -> DecodedWebSocketMessage {
    let raw = RawEnvelope::try_new(
        crate::codec::WebSocketFrameKind::Text,
        request.original_frame().to_vec(),
        usize::MAX,
    )
    .expect("qualification request is bounded");
    LilyEnvelopeCodec
        .decode_frame(raw)
        .expect("qualification request uses the canonical Lily envelope")
}

async fn execute_scoped_message(
    container: Arc<ApplicationContainer>,
    table: Arc<WebSocketActionTable>,
    event: &'static str,
    action_timeout: Duration,
    cancellation: CancellationToken,
) -> ScopedWebSocketDispatch {
    let slot = ExecutionSlot::with_cancellation(cancellation.clone(), Default::default()).1;
    execute_scoped_message_with_slot(container, table, event, action_timeout, cancellation, slot)
        .await
}

async fn execute_scoped_message_with_slot(
    container: Arc<ApplicationContainer>,
    table: Arc<WebSocketActionTable>,
    event: &'static str,
    action_timeout: Duration,
    cancellation: CancellationToken,
    slot: ExecutionSlot,
) -> ScopedWebSocketDispatch {
    let connection_id = Uuid::new_v4();
    let manager = Arc::new(ConnectionManager::with_registered_namespaces(
        8,
        8,
        ["message-scope".to_owned()],
    ));
    let budget = slot.test_shutdown_budget();
    let context = Arc::new(
        WebSocketContext::new(
            connection_id,
            Arc::clone(&manager),
            "message-scope".to_owned(),
        )
        .with_shutdown_budget(budget.clone()),
    );
    let request = Arc::new(qualification_request(connection_id, event));
    let decoded = decode_qualification_request(request.as_ref());
    let runtime = MessageRuntime {
        message_timeout: action_timeout,
        cleanup_timeout: Duration::from_secs(1),
        cancellation,
        metrics: Arc::clone(manager.metrics()),
    };

    let (action, frame_codec) = WsApp::resolve_message_route(&table, &context, &request).unwrap();
    ScopeCleanupRegistry::with_budget(budget)
        .run_scoped(
            connection_id,
            &container,
            ProcessContext::new()
                .with_metadata("transport".to_owned(), "websocket".to_owned())
                .with_metadata("phase".to_owned(), "message".to_owned()),
            WsApp::dispatch_message_owner(
                action.as_ref(),
                frame_codec,
                container.services(),
                context,
                request,
                decoded,
                runtime,
                slot,
            ),
        )
        .await
        .expect("message scope cleanup must succeed")
        .into_dispatch()
}

fn reset_probe() {
    PROBE_INITIALIZED.store(0, Ordering::SeqCst);
    PROBE_RESOLVED.store(0, Ordering::SeqCst);
    PROBE_DISPOSED.store(0, Ordering::SeqCst);
    PROBE_DISPOSING.store(0, Ordering::SeqCst);
    *PROBE_DISPOSE_GATE.lock().unwrap() = None;
    OWNER_EVENTS.lock().unwrap().clear();
    MESSAGE_STATE_DROPPED.store(0, Ordering::SeqCst);
    OWNER_COOPERATIVE.store(false, Ordering::SeqCst);
    OWNER_PENDING_AFTER.store(false, Ordering::SeqCst);
    OWNER_PENDING_TERMINATION.store(false, Ordering::SeqCst);
}

struct OwnerMiddleware<const INDEX: usize>;

#[async_trait]
impl<const INDEX: usize> WsMessageMiddleware for OwnerMiddleware<INDEX> {
    async fn new(_: Arc<Extensions>) -> Result<Self, crate::middleware::WsMiddlewareInitError> {
        Ok(Self)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new(
            match INDEX {
                0 => "owner_outer",
                1 => "owner_inner",
                _ => "owner_pending",
            },
            MiddlewareKind::WebSocketMessage,
        )
    }

    async fn before_message(
        &self,
        exchange: &mut WsMessageExchange,
        signal: crate::ExecutionCancellation,
    ) -> Result<WsMessageDecision, crate::middleware::WsMiddlewareError> {
        exchange.service::<MessageScopeProbe>().await.unwrap();
        if INDEX == 0 {
            exchange.insert_message_local(RetainedMessageState).unwrap();
        }
        OWNER_EVENTS.lock().unwrap().push(match INDEX {
            0 => "outer.before",
            1 => "inner.before",
            _ => "pending.before",
        });
        if INDEX == 2 {
            if OWNER_COOPERATIVE.load(Ordering::SeqCst) {
                cooperative_return(signal).await;
                return Ok(WsMessageDecision::Continue);
            }
            pending::<()>().await;
        }
        Ok(WsMessageDecision::Continue)
    }

    async fn after_message(
        &self,
        exchange: &mut WsMessageExchange,
        _: WsMessageOutcome,
        signal: crate::ExecutionCancellation,
    ) -> Result<WsMessageDecision, crate::middleware::WsMiddlewareError> {
        assert_eq!(
            PROBE_DISPOSING.load(Ordering::SeqCst),
            0,
            "DI cleanup cannot race unwind"
        );
        exchange.service::<MessageScopeProbe>().await.unwrap();
        OWNER_EVENTS.lock().unwrap().push(match INDEX {
            0 => "outer.after",
            1 => "inner.after",
            _ => "pending.after",
        });
        if INDEX == 1 && OWNER_PENDING_AFTER.load(Ordering::SeqCst) {
            if OWNER_COOPERATIVE.load(Ordering::SeqCst) {
                cooperative_return(signal).await;
                return Ok(WsMessageDecision::Continue);
            }
            pending::<()>().await;
        }
        Ok(WsMessageDecision::Continue)
    }

    async fn on_message_termination(
        &self,
        context: crate::middleware::WsMessageTerminationContext<'_>,
        signal: crate::CleanupCancellation,
    ) -> Result<(), crate::middleware::WsMiddlewareError> {
        assert_eq!(
            PROBE_DISPOSING.load(Ordering::SeqCst),
            0,
            "DI cleanup cannot race termination unwind"
        );
        assert!(!signal.is_cancelled());
        assert!(!context.cancellation().is_cancelled());
        context.service::<MessageScopeProbe>().await.unwrap();
        OWNER_EVENTS.lock().unwrap().push(match INDEX {
            0 => "outer.termination",
            1 => "inner.termination",
            _ => "pending.termination",
        });
        if INDEX == 1 && OWNER_PENDING_TERMINATION.load(Ordering::SeqCst) {
            pending::<()>().await;
        }
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn timed_out_termination_is_retained_after_outer_cleanup_di_and_owner_join() {
    let _lock = MESSAGE_SCOPE_QUALIFICATION_LOCK.lock().await;
    reset_probe();
    OWNER_PENDING_TERMINATION.store(true, Ordering::SeqCst);
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let table = action_table(
        &container,
        operation(
            "shutdown",
            vec![
                WebSocketMessageMiddlewareRegistration::of::<OwnerMiddleware<0>>(),
                WebSocketMessageMiddlewareRegistration::of::<OwnerMiddleware<1>>(),
            ],
            Vec::new(),
            None,
        ),
    )
    .await;
    let budget = crate::shutdown::ShutdownBudget::default();
    let registry = MessageDispatchRegistry::with_budget(budget.clone());
    let source = CancellationToken::new();
    let task_container = Arc::clone(&container);
    let output =
        registry.spawn_owner_with_cancellation(Uuid::new_v4(), source.clone(), move |slot| {
            execute_scoped_message_with_slot(
                task_container,
                table,
                "shutdown",
                Duration::from_secs(60),
                source,
                slot,
            )
        });
    while !OWNER_EVENTS.lock().unwrap().contains(&"action") {
        tokio::task::yield_now().await;
    }
    let start = Instant::now();
    budget.force_before(start + Duration::from_millis(200));
    let force = CancellationToken::new();
    force.cancel();
    let report = registry.drain_with_force(None, &force).await;
    output.await.unwrap();
    assert!(start.elapsed() < Duration::from_millis(200));
    assert_eq!(report.aborted, 1);
    assert_eq!(report.outstanding, 0);
    assert_eq!(report.owner_join_cancelled, 0);
    assert_eq!(report.cleanup_failures, 1);
    assert_eq!(
        *OWNER_EVENTS.lock().unwrap(),
        [
            "outer.before",
            "inner.before",
            "action",
            "inner.termination",
            "outer.termination"
        ]
    );
    assert_eq!(PROBE_DISPOSED.load(Ordering::SeqCst), 1);
    assert_eq!(MESSAGE_STATE_DROPPED.load(Ordering::SeqCst), 1);
    assert_eq!(registry.test_snapshot().registered_abort_handles, 0);
    assert_eq!(
        registry
            .drain_with_force(None, &force)
            .await
            .cleanup_failures,
        1
    );
    close_without_replaying_disposal(&container).await;
    reset_probe();
}

struct OwnerPendingGuard;

#[async_trait]
impl WsGuard for OwnerPendingGuard {
    async fn new(_: Arc<Extensions>) -> Result<Self, GuardInitializationError> {
        Ok(Self)
    }
    async fn can_activate(
        &self,
        _: &mut WsMessageExchange,
        signal: crate::ExecutionCancellation,
    ) -> Result<(), WebSocketGuardRejection> {
        OWNER_EVENTS.lock().unwrap().push("guard");
        if OWNER_COOPERATIVE.load(Ordering::SeqCst) {
            cooperative_return(signal).await;
            return Ok(());
        }
        pending().await
    }
}

#[tokio::test(start_paused = true)]
async fn message_cleanup_timeout_remains_reported_after_successful_owner_join() {
    let _lock = MESSAGE_SCOPE_QUALIFICATION_LOCK.lock().await;
    reset_probe();
    OWNER_PENDING_AFTER.store(true, Ordering::SeqCst);
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let table = action_table(
        &container,
        operation(
            "success",
            vec![
                WebSocketMessageMiddlewareRegistration::of::<OwnerMiddleware<0>>(),
                WebSocketMessageMiddlewareRegistration::of::<OwnerMiddleware<1>>(),
            ],
            Vec::new(),
            None,
        ),
    )
    .await;
    let budget = crate::shutdown::ShutdownBudget::default();
    budget.configure(
        Instant::now() + Duration::from_millis(1500),
        Instant::now() + Duration::from_secs(2),
    );
    let registry = MessageDispatchRegistry::with_budget(budget);
    let task_container = Arc::clone(&container);
    let output = registry.spawn_owner(Uuid::new_v4(), move |slot| {
        execute_scoped_message_with_slot(
            task_container,
            table,
            "success",
            Duration::from_secs(1),
            CancellationToken::new(),
            slot,
        )
    });
    let report = registry
        .drain_with_force(None, &CancellationToken::new())
        .await;
    output.await.unwrap();
    assert_eq!(
        report.cleanup_failures, 1,
        "interrupted normal inner exit stays incomplete after successful termination"
    );
    assert_eq!(report.outstanding, 0);
    assert_eq!(report.owner_join_cancelled, 0);
    assert_eq!(registry.test_snapshot().registered_abort_handles, 0);
    assert_eq!(
        registry
            .drain_with_force(None, &CancellationToken::new())
            .await
            .cleanup_failures,
        1
    );
    close_without_replaying_disposal(&container).await;
    reset_probe();
}

#[tokio::test]
async fn middleware_guard_and_action_observe_execution_cancellation_before_return_and_reverse_cleanup()
 {
    let _lock = MESSAGE_SCOPE_QUALIFICATION_LOCK.lock().await;
    for blocked in ["pending.before", "guard", "action", "inner.after"] {
        reset_probe();
        OWNER_COOPERATIVE.store(true, Ordering::SeqCst);
        OWNER_PENDING_AFTER.store(blocked == "inner.after", Ordering::SeqCst);
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let mut middleware = vec![
            WebSocketMessageMiddlewareRegistration::of::<OwnerMiddleware<0>>(),
            WebSocketMessageMiddlewareRegistration::of::<OwnerMiddleware<1>>(),
        ];
        if blocked == "pending.before" {
            middleware.push(WebSocketMessageMiddlewareRegistration::of::<
                OwnerMiddleware<2>,
            >());
        }
        let guards = if blocked == "guard" {
            vec![WebSocketGuardRegistration::of::<OwnerPendingGuard>()]
        } else {
            Vec::new()
        };
        let event = if blocked == "action" {
            "shutdown"
        } else {
            "success"
        };
        let table = action_table(&container, operation(event, middleware, guards, None)).await;
        let registry = MessageDispatchRegistry::default();
        let source = CancellationToken::new();
        let task_container = Arc::clone(&container);
        let output =
            registry.spawn_owner_with_cancellation(Uuid::new_v4(), source.clone(), move |slot| {
                execute_scoped_message_with_slot(
                    task_container,
                    table,
                    event,
                    Duration::from_secs(60),
                    source,
                    slot,
                )
            });
        timeout(Duration::from_secs(2), async {
            while !OWNER_EVENTS.lock().unwrap().contains(&blocked) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let force = CancellationToken::new();
        force.cancel();
        let report = registry.drain_with_force(None, &force).await;
        output.await.unwrap();
        assert_eq!(
            report.abort_requested, 0,
            "{blocked} should return during the cooperative window"
        );
        assert_eq!(report.aborted, 0);
        assert_eq!(report.outstanding, 0);
        assert_eq!(report.owner_join_cancelled, 0);
        let mut expected = vec!["outer.before", "inner.before"];
        if blocked == "inner.after" {
            expected.push("action");
        }
        expected.extend([blocked, "cooperative.return"]);
        if matches!(blocked, "pending.before" | "guard") {
            expected.push("action");
        }
        if blocked == "pending.before" {
            expected.push("pending.after");
        }
        if blocked != "inner.after" {
            expected.push("inner.after");
        }
        expected.push("outer.after");
        assert_eq!(*OWNER_EVENTS.lock().unwrap(), expected);
        assert_eq!(PROBE_DISPOSED.load(Ordering::SeqCst), 1);
        assert_eq!(MESSAGE_STATE_DROPPED.load(Ordering::SeqCst), 1);
        close_without_replaying_disposal(&container).await;
    }
    reset_probe();
}

#[tokio::test]
async fn force_at_before_guard_action_or_after_retains_reverse_termination_and_di_receipt() {
    let _lock = MESSAGE_SCOPE_QUALIFICATION_LOCK.lock().await;
    for blocked in ["pending.before", "guard", "action", "inner.after"] {
        reset_probe();
        OWNER_PENDING_AFTER.store(blocked == "inner.after", Ordering::SeqCst);
        let container = Arc::new(ApplicationContainer::build().await.unwrap());
        let mut middleware = vec![
            WebSocketMessageMiddlewareRegistration::of::<OwnerMiddleware<0>>(),
            WebSocketMessageMiddlewareRegistration::of::<OwnerMiddleware<1>>(),
        ];
        if blocked == "pending.before" {
            middleware.push(WebSocketMessageMiddlewareRegistration::of::<
                OwnerMiddleware<2>,
            >());
        }
        let guards = if blocked == "guard" {
            vec![WebSocketGuardRegistration::of::<OwnerPendingGuard>()]
        } else {
            Vec::new()
        };
        let event = if blocked == "action" {
            "shutdown"
        } else {
            "success"
        };
        let table = action_table(&container, operation(event, middleware, guards, None)).await;
        let registry = MessageDispatchRegistry::default();
        let task_container = Arc::clone(&container);
        let waiter = registry.spawn_owner(Uuid::new_v4(), move |slot| {
            execute_scoped_message_with_slot(
                task_container,
                table,
                event,
                Duration::from_secs(60),
                CancellationToken::new(),
                slot,
            )
        });
        timeout(Duration::from_secs(2), async {
            while !OWNER_EVENTS.lock().unwrap().contains(&blocked) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let gate = Arc::new(Semaphore::new(0));
        *PROBE_DISPOSE_GATE.lock().unwrap() = Some(Arc::clone(&gate));
        drop(waiter);
        let force = CancellationToken::new();
        force.cancel();
        let draining = registry.clone();
        let task = tokio::spawn(async move { draining.drain_with_force(None, &force).await });
        timeout(Duration::from_secs(2), async {
            while PROBE_DISPOSING.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let mut expected = vec!["outer.before", "inner.before"];
        if blocked == "inner.after" {
            expected.push("action");
        }
        expected.extend([blocked, "inner.termination", "outer.termination"]);
        assert_eq!(*OWNER_EVENTS.lock().unwrap(), expected);
        assert_eq!(PROBE_DISPOSED.load(Ordering::SeqCst), 0);
        assert_eq!(
            MESSAGE_STATE_DROPPED.load(Ordering::SeqCst),
            0,
            "message owner state must survive pending DI cleanup"
        );
        assert!(!task.is_finished(), "owner join cannot precede DI disposal");
        let evidence = registry.accounting().0;
        assert_eq!(evidence.joined, 0);
        assert_eq!(evidence.output.outstanding, 1);
        assert_eq!(
            evidence.output.attempts, 0,
            "DI must close before any terminal attempt"
        );
        assert_eq!(evidence.output.close_requests_completed, 0);
        assert!(evidence.reconciles());
        gate.add_permits(1);
        let report = timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.abort_requested, 1);
        assert_eq!(report.aborted, 1);
        assert_eq!(report.owner_join_cancelled, 0);
        assert_eq!(
            report.cleanup_failures,
            usize::from(blocked == "inner.after")
        );
        assert_eq!(registry.test_snapshot().active_tasks, 0);
        assert_eq!(registry.test_snapshot().registered_abort_handles, 0);
        assert_eq!(PROBE_INITIALIZED.load(Ordering::SeqCst), 1);
        assert_eq!(PROBE_DISPOSED.load(Ordering::SeqCst), 1);
        assert_eq!(MESSAGE_STATE_DROPPED.load(Ordering::SeqCst), 1);
        close_without_replaying_disposal(&container).await;
    }
}

#[tokio::test(start_paused = true)]
async fn root_cleanup_deadline_aborts_pending_di_disposal_and_observes_its_receipt() {
    let _lock = MESSAGE_SCOPE_QUALIFICATION_LOCK.lock().await;
    reset_probe();
    *PROBE_DISPOSE_GATE.lock().unwrap() = Some(Arc::new(Semaphore::new(0)));
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let budget = crate::shutdown::ShutdownBudget::default();
    let start = Instant::now();
    budget.force_before(start + Duration::from_millis(100));
    let scopes = ScopeCleanupRegistry::with_budget(budget);
    let extensions = container.services();
    let result = scopes
        .run_scoped(
            Uuid::new_v4(),
            &container,
            ProcessContext::new(),
            async move {
                extensions
                    .get_service::<MessageScopeProbe>(None)
                    .await
                    .unwrap();
                RetainedMessageState
            },
        )
        .await;
    assert!(matches!(
        result,
        Err(InjectionError::ScopeCleanupTimedOut { .. })
    ));
    let report = scopes.drain().await;
    assert_eq!(report.outstanding, 0);
    assert_eq!(report.deadline_failures, 1);
    assert_eq!(PROBE_DISPOSING.load(Ordering::SeqCst), 1);
    assert_eq!(
        PROBE_DISPOSED.load(Ordering::SeqCst),
        0,
        "termination is not successful disposal"
    );
    assert_eq!(MESSAGE_STATE_DROPPED.load(Ordering::SeqCst), 1);
    assert!(Instant::now() <= start + Duration::from_millis(100));
    let failure = container
        .close()
        .await
        .expect_err("DI must retain its disposal timeout");
    assert!(failure.to_string().contains("ScopeCleanupTimedOut"));
    reset_probe();
}

#[tokio::test(start_paused = true)]
async fn disconnected_timeout_retains_di_termination_receipt_after_callback_future_drops() {
    struct PendingDisconnect;
    impl crate::controller::WebSocketLifecycleAction for PendingDisconnect {
        fn call(
            &self,
            invocation: WebSocketLifecycleInvocation,
        ) -> crate::controller::WebSocketLifecycleFuture {
            Box::pin(async move {
                invocation
                    .extensions()
                    .get_service::<MessageScopeProbe>(None)
                    .await
                    .unwrap();
                pending().await
            })
        }
    }
    let _lock = MESSAGE_SCOPE_QUALIFICATION_LOCK.lock().await;
    reset_probe();
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let scopes = ScopeCleanupRegistry::default();
    let gate = Arc::new(Semaphore::new(0));
    *PROBE_DISPOSE_GATE.lock().unwrap() = Some(Arc::clone(&gate));
    let connection_id = Uuid::new_v4();
    let context = Arc::new(WebSocketContext::new(
        connection_id,
        Arc::new(ConnectionManager::new()),
        "message-scope".into(),
    ));
    let handler: WebSocketLifecycleHandler = Arc::new(PendingDisconnect);
    let ledger = Arc::new(StdMutex::new(
        vec![(handler, Duration::from_millis(10))].into(),
    ));
    let report = WsApp::run_websocket_disconnect_ledger(
        connection_id,
        Arc::clone(&container),
        container.services(),
        context,
        ledger,
        crate::middleware::WsConnectionCloseCategory::Cancelled,
        CancellationToken::new(),
        scopes.clone(),
    )
    .await;
    assert_eq!(report.completed(), 0);
    assert_eq!(report.timed_out(), 1);
    let receipt = scopes.wait_connection(connection_id);
    tokio::pin!(receipt);
    assert!(matches!(
        futures_util::poll!(receipt.as_mut()),
        std::task::Poll::Pending
    ));
    assert_eq!(PROBE_DISPOSED.load(Ordering::SeqCst), 0);
    gate.add_permits(1);
    receipt.await;
    scopes.drain().await;
    assert_eq!(PROBE_DISPOSED.load(Ordering::SeqCst), 1);
    close_without_replaying_disposal(&container).await;
}

async fn wait_for_resolution() {
    tokio::time::timeout(Duration::from_secs(2), async {
        while PROBE_RESOLVED.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("qualification service resolution deadline");
}

#[tokio::test]
async fn connected_success_arms_disconnect_before_pending_scope_close_and_survives_drop() {
    struct ScopeLifecycle(bool);
    impl crate::controller::WebSocketLifecycleAction for ScopeLifecycle {
        fn call(
            &self,
            invocation: WebSocketLifecycleInvocation,
        ) -> crate::controller::WebSocketLifecycleFuture {
            let connected = self.0;
            Box::pin(async move {
                if connected {
                    invocation
                        .extensions()
                        .get_service::<MessageScopeProbe>(None)
                        .await
                        .unwrap();
                } else {
                    assert_eq!(PROBE_DISPOSED.load(Ordering::SeqCst), 1);
                    OWNER_EVENTS.lock().unwrap().push("disconnected");
                }
                Ok(())
            })
        }
    }
    fn connected(
        _: ErasedWebSocketController,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
        Ok(BoundWebSocketOperation::Connected(Arc::new(
            ScopeLifecycle(true),
        )))
    }
    fn disconnected(
        _: ErasedWebSocketController,
    ) -> Result<BoundWebSocketOperation, WebSocketControllerBindingError> {
        Ok(BoundWebSocketOperation::Disconnected(Arc::new(
            ScopeLifecycle(false),
        )))
    }

    let _lock = MESSAGE_SCOPE_QUALIFICATION_LOCK.lock().await;
    reset_probe();
    let gate = Arc::new(Semaphore::new(0));
    *PROBE_DISPOSE_GATE.lock().unwrap() = Some(gate.clone());
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let operations = [
        (
            WebSocketOperationKind::Connected,
            connected
                as fn(
                    ErasedWebSocketController,
                )
                    -> Result<BoundWebSocketOperation, WebSocketControllerBindingError>,
        ),
        (
            WebSocketOperationKind::Disconnected,
            disconnected
                as fn(
                    ErasedWebSocketController,
                )
                    -> Result<BoundWebSocketOperation, WebSocketControllerBindingError>,
        ),
    ]
    .into_iter()
    .map(|(kind, binder)| {
        PendingWebSocketOperation::new(
            kind,
            None,
            "app::message_scope_qualification_tests::ScopeLifecycle",
            WebSocketActionRegistration::of::<MessageScopeController>(binder),
            WebSocketOperationMetadata::new(
                Vec::new(),
                Vec::new(),
                None,
                None,
                WebSocketAsyncApiRegistration::unspecified(),
            ),
        )
    })
    .collect();
    let table = materialize_websocket_controllers(
        vec![WebSocketControllerRegistration::of::<MessageScopeController>()],
        operations,
        container.services(),
    )
    .await
    .unwrap();
    let lifecycle = table.lifecycle_for_namespace("message-scope").unwrap();
    let id = Uuid::new_v4();
    let context = Arc::new(WebSocketContext::new(
        id,
        Arc::new(ConnectionManager::new()),
        "message-scope".into(),
    ));
    let ledger: WebSocketDisconnectLedger = Default::default();
    let scopes = ScopeCleanupRegistry::default();
    let extensions = container.services();
    let cancellation = CancellationToken::new();
    let mut connect = Box::pin(WsApp::run_connect_hooks(
        id,
        WebSocketConnectRuntime {
            container: &container,
            extensions: &extensions,
            context: &context,
            lifecycle: &lifecycle,
            stage_timeout: Duration::from_secs(1),
            cancellation: &cancellation,
            scopes: &scopes,
            disconnect_ledger: &ledger,
        },
    ));
    tokio::select! {
        result = &mut connect => panic!("connect scope must still be closing: {result:?}"),
        _ = async {
            timeout(Duration::from_secs(1), async {
                while PROBE_DISPOSING.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            }).await.unwrap();
        } => {}
    }
    assert_eq!(
        ledger.lock().unwrap().len(),
        1,
        "successful callback arms before DI close returns"
    );
    drop(connect);
    let cleanup = ConnectionCleanupRegistry::new().unwrap();
    let terminal_container = container.clone();
    let terminal_scopes = scopes.clone();
    let terminal_context = context.clone();
    let terminal_ledger = ledger.clone();
    let terminal: ConnectionTerminalHook = Arc::new(move |category, signal| {
        WsApp::run_websocket_disconnect_ledger(
            id,
            terminal_container.clone(),
            terminal_container.services(),
            terminal_context.clone(),
            terminal_ledger.clone(),
            category,
            signal,
            terminal_scopes.clone(),
        )
        .boxed()
    });
    let lease = cleanup
        .register(
            id,
            Arc::new(crate::middleware::CompiledWsConnectionChain::compile(Vec::new()).unwrap()),
            context,
            Some(terminal),
            Duration::from_secs(1),
        )
        .unwrap();
    let receipt_scopes = scopes.clone();
    lease.attach_prerequisites(Arc::new(move || {
        let scopes = receipt_scopes.clone();
        async move { scopes.wait_connection(id).await }.boxed()
    }));
    let finalizer =
        tokio::spawn(lease.finalize(crate::middleware::WsConnectionCloseCategory::Cancelled));
    tokio::task::yield_now().await;
    assert!(!finalizer.is_finished());
    assert!(OWNER_EVENTS.lock().unwrap().is_empty());
    gate.add_permits(1);
    let report = finalizer.await.unwrap();
    assert_eq!(report.terminal_report().completed(), 1);
    assert_eq!(*OWNER_EVENTS.lock().unwrap(), vec!["disconnected"]);
    assert!(ledger.lock().unwrap().is_empty());
    assert_eq!(scopes.drain().await.outstanding, 0);
    assert_eq!(cleanup.entry_count(), 0);
    close_without_replaying_disposal(&container).await;
    reset_probe();
}

#[tokio::test]
async fn connection_cleanup_waits_for_session_late_message_owner_and_di_termination() {
    let _lock = MESSAGE_SCOPE_QUALIFICATION_LOCK.lock().await;
    reset_probe();
    OWNER_COOPERATIVE.store(true, Ordering::SeqCst);
    let gate = Arc::new(Semaphore::new(0));
    *PROBE_DISPOSE_GATE.lock().unwrap() = Some(gate.clone());
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let table = action_table(
        &container,
        operation(
            "shutdown",
            vec![
                WebSocketMessageMiddlewareRegistration::of::<OwnerMiddleware<0>>(),
                WebSocketMessageMiddlewareRegistration::of::<OwnerMiddleware<1>>(),
            ],
            Vec::new(),
            None,
        ),
    )
    .await;
    let messages = MessageDispatchRegistry::default();
    let cleanup = ConnectionCleanupRegistry::new().unwrap();
    let id = Uuid::new_v4();
    let disconnected = Arc::new(AtomicUsize::new(0));
    let calls = disconnected.clone();
    let terminal: ConnectionTerminalHook = Arc::new(move |_, _| {
        let calls = calls.clone();
        async move {
            assert_eq!(PROBE_DISPOSED.load(Ordering::SeqCst), 1);
            assert_eq!(MESSAGE_STATE_DROPPED.load(Ordering::SeqCst), 1);
            calls.fetch_add(1, Ordering::SeqCst);
            ConnectionTerminalHookReport::new(1, 1, 0, 0, 0)
        }
        .boxed()
    });
    let lease = cleanup
        .register(
            id,
            Arc::new(crate::middleware::CompiledWsConnectionChain::compile(Vec::new()).unwrap()),
            Arc::new(WebSocketContext::new(
                id,
                Arc::new(ConnectionManager::new()),
                "message-scope".into(),
            )),
            Some(terminal),
            Duration::from_secs(1),
        )
        .unwrap();
    let (session_tx, session_rx) = tokio::sync::oneshot::channel();
    lease.attach_session(async move { session_rx.await.is_ok() }.boxed().shared());
    let children = messages.clone();
    lease.attach_prerequisites(Arc::new(move || {
        let children = children.clone();
        async move { children.wait_connection(id).await }.boxed()
    }));
    let finalizer =
        tokio::spawn(lease.finalize(crate::middleware::WsConnectionCloseCategory::Cancelled));
    tokio::task::yield_now().await;
    assert_eq!(disconnected.load(Ordering::SeqCst), 0);
    // Register after the finalizer started: it must not snapshot an empty
    // message registry while the session could still accept this dispatch.
    let source = CancellationToken::new();
    let execution_source = source.clone();
    let task_container = container.clone();
    let output = messages.spawn_owner_with_cancellation(id, source.clone(), move |slot| {
        execute_scoped_message_with_slot(
            task_container,
            table,
            "shutdown",
            Duration::from_secs(60),
            execution_source,
            slot,
        )
    });
    wait_for_resolution().await;
    session_tx.send(()).unwrap();
    tokio::task::yield_now().await;
    source.cancel();
    timeout(Duration::from_secs(1), async {
        while PROBE_DISPOSING.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(disconnected.load(Ordering::SeqCst), 0);
    assert!(!finalizer.is_finished());
    let evidence = messages.accounting().0;
    assert_eq!(evidence.output.outstanding, 1);
    assert_eq!(evidence.output.attempts, 0);
    assert_eq!(evidence.joined, 0);
    gate.add_permits(1);
    output.await.unwrap();
    let result = finalizer.await.unwrap();
    assert_eq!(result.terminal_report().completed(), 1);
    assert_eq!(disconnected.load(Ordering::SeqCst), 1);
    assert_eq!(cleanup.entry_count(), 0);
    let evidence = messages.accounting().0;
    assert_eq!(evidence.output.outstanding, 0);
    assert_eq!(
        evidence.output.suppressed, 1,
        "dropped terminal decision is not a successful send"
    );
    assert!(evidence.reconciles());
    assert_eq!(
        messages
            .drain_with_force(None, &CancellationToken::new())
            .await
            .outstanding,
        0
    );
    close_without_replaying_disposal(&container).await;
    reset_probe();
}

async fn assert_exactly_once_cleanup(container: &ApplicationContainer) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while container.active_scope_count() != 0 || PROBE_DISPOSED.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("message-scope disposal deadline");

    assert_eq!(PROBE_INITIALIZED.load(Ordering::SeqCst), 1);
    assert_eq!(PROBE_RESOLVED.load(Ordering::SeqCst), 1);
    assert_eq!(PROBE_DISPOSED.load(Ordering::SeqCst), 1);
    assert_eq!(container.active_scope_count(), 0);
}

async fn close_without_replaying_disposal(container: &ApplicationContainer) {
    container.close().await.unwrap();
    assert_eq!(
        PROBE_DISPOSED.load(Ordering::SeqCst),
        1,
        "container shutdown must not dispose a completed message scope twice"
    );
}

#[tokio::test]
async fn successful_action_disposes_its_scoped_service_exactly_once() {
    let _lock = MESSAGE_SCOPE_QUALIFICATION_LOCK.lock().await;
    reset_probe();
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let table = action_table(
        container.as_ref(),
        operation("success", Vec::new(), Vec::new(), None),
    )
    .await;

    let dispatch = execute_scoped_message(
        Arc::clone(&container),
        table,
        "success",
        Duration::from_secs(1),
        CancellationToken::new(),
    )
    .await;

    assert_eq!(dispatch.outcome, WsMessageOutcome::Handled);
    assert_exactly_once_cleanup(container.as_ref()).await;
    close_without_replaying_disposal(container.as_ref()).await;
}

#[tokio::test]
async fn middleware_rejection_disposes_its_scoped_service_exactly_once() {
    let _lock = MESSAGE_SCOPE_QUALIFICATION_LOCK.lock().await;
    reset_probe();
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let table = action_table(
        container.as_ref(),
        operation(
            "success",
            vec![WebSocketMessageMiddlewareRegistration::of::<
                RejectingScopeMiddleware,
            >()],
            Vec::new(),
            None,
        ),
    )
    .await;

    let dispatch = execute_scoped_message(
        Arc::clone(&container),
        table,
        "success",
        Duration::from_secs(1),
        CancellationToken::new(),
    )
    .await;

    assert_eq!(
        dispatch.outcome,
        WsMessageOutcome::Rejected(crate::request::WsProtocolErrorCode::MiddlewareRejected)
    );
    assert_exactly_once_cleanup(container.as_ref()).await;
    close_without_replaying_disposal(container.as_ref()).await;
}

#[tokio::test]
async fn guard_rejection_disposes_its_scoped_service_exactly_once() {
    let _lock = MESSAGE_SCOPE_QUALIFICATION_LOCK.lock().await;
    reset_probe();
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let table = action_table(
        container.as_ref(),
        operation(
            "success",
            Vec::new(),
            vec![WebSocketGuardRegistration::of::<RejectingScopeGuard>()],
            None,
        ),
    )
    .await;

    let dispatch = execute_scoped_message(
        Arc::clone(&container),
        table,
        "success",
        Duration::from_secs(1),
        CancellationToken::new(),
    )
    .await;

    assert_eq!(
        dispatch.outcome,
        WsMessageOutcome::Rejected(crate::request::WsProtocolErrorCode::AuthorizationDenied)
    );
    assert_exactly_once_cleanup(container.as_ref()).await;
    close_without_replaying_disposal(container.as_ref()).await;
}

#[tokio::test]
async fn action_timeout_disposes_its_scoped_service_exactly_once() {
    let _lock = MESSAGE_SCOPE_QUALIFICATION_LOCK.lock().await;
    reset_probe();
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let table = action_table(
        container.as_ref(),
        operation(
            "timeout",
            Vec::new(),
            Vec::new(),
            Some(Duration::from_millis(10)),
        ),
    )
    .await;

    let dispatch = execute_scoped_message(
        Arc::clone(&container),
        table,
        "timeout",
        Duration::from_secs(1),
        CancellationToken::new(),
    )
    .await;

    assert_eq!(
        dispatch.outcome,
        WsMessageOutcome::Rejected(crate::request::WsProtocolErrorCode::HandlerFailed)
    );
    assert_exactly_once_cleanup(container.as_ref()).await;
    close_without_replaying_disposal(container.as_ref()).await;
}

#[tokio::test]
async fn action_panic_disposes_its_scoped_service_exactly_once() {
    let _lock = MESSAGE_SCOPE_QUALIFICATION_LOCK.lock().await;
    reset_probe();
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let table = action_table(
        container.as_ref(),
        operation("panic", Vec::new(), Vec::new(), None),
    )
    .await;

    let dispatch = execute_scoped_message(
        Arc::clone(&container),
        table,
        "panic",
        Duration::from_secs(1),
        CancellationToken::new(),
    )
    .await;

    assert_eq!(
        dispatch.outcome,
        WsMessageOutcome::Rejected(crate::request::WsProtocolErrorCode::HandlerFailed)
    );
    assert_exactly_once_cleanup(container.as_ref()).await;
    close_without_replaying_disposal(container.as_ref()).await;
}

#[tokio::test]
async fn dropped_connection_waiter_cannot_orphan_its_message_scope() {
    let _lock = MESSAGE_SCOPE_QUALIFICATION_LOCK.lock().await;
    reset_probe();
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let table = action_table(
        container.as_ref(),
        operation("disconnect", Vec::new(), Vec::new(), None),
    )
    .await;
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task_container = Arc::clone(&container);
    let registry = MessageDispatchRegistry::default();
    let waiter = registry.spawn_owner_with_cancellation(
        Uuid::new_v4(),
        cancellation.clone(),
        move |slot| async move {
            let _ = execute_scoped_message_with_slot(
                task_container,
                table,
                "disconnect",
                Duration::from_secs(10),
                task_cancellation,
                slot,
            )
            .await;
        },
    );

    wait_for_resolution().await;
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
    assert_exactly_once_cleanup(container.as_ref()).await;
    close_without_replaying_disposal(container.as_ref()).await;
}

#[tokio::test]
async fn server_shutdown_cancellation_disposes_pending_action_scope_exactly_once() {
    let _lock = MESSAGE_SCOPE_QUALIFICATION_LOCK.lock().await;
    reset_probe();
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let table = action_table(
        container.as_ref(),
        operation("shutdown", Vec::new(), Vec::new(), None),
    )
    .await;
    let cancellation = CancellationToken::new();
    let task_cancellation = cancellation.clone();
    let task_container = Arc::clone(&container);
    let task = tokio::spawn(async move {
        execute_scoped_message(
            task_container,
            table,
            "shutdown",
            Duration::from_secs(10),
            task_cancellation,
        )
        .await
        .outcome
    });

    wait_for_resolution().await;
    cancellation.cancel();
    let outcome = tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("shutdown dispatch completion deadline")
        .unwrap();

    assert_eq!(
        outcome,
        WsMessageOutcome::Close(crate::request::WsCloseReason::ServerShutdown)
    );
    assert_exactly_once_cleanup(container.as_ref()).await;
    close_without_replaying_disposal(container.as_ref()).await;
}
