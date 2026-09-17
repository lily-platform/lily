use super::*;
use crate::app::middleware_executor::{
    CompiledHttpMiddleware, HttpMiddlewareChain, HttpMiddlewareErrorWriter,
    MiddlewareChainOutcomeSlot, NoopHttpMiddlewareObserver,
};
use crate::{
    CleanupCancellation, HttpMiddlewareStage, HttpRequestInterruption,
    HttpRequestTerminationContext,
};
use lily_middleware::__private::HttpNextService;

#[derive(Clone, Copy, Default)]
enum Normal {
    #[default]
    Around,
    BeforePending,
    BeforePanic,
    Short,
    Error,
    AfterPending,
    AfterError,
    DropNext,
}

#[derive(Clone, Copy, Default)]
enum Cleanup {
    #[default]
    Complete,
    Error,
    Pending,
    Panic,
    Cooperative,
    DropPanic,
    FileHelper,
}

#[derive(Default)]
struct State {
    normal: [Normal; 3],
    cleanup: [Cleanup; 3],
    next_id: AtomicUsize,
    events: Mutex<Vec<(usize, &'static str)>>,
    changed: Notify,
    views: Mutex<Vec<(usize, CleanupCancellation)>>,
    stages: Mutex<Vec<(usize, HttpMiddlewareStage)>>,
    reasons: Mutex<Vec<HttpRequestInterruption>>,
    clear_local: bool,
    terminal_pending: bool,
    body: Mutex<Option<oneshot::Sender<BodyBridge>>>,
    route_probe: OnceLock<Arc<ProbeState>>,
    constructors: AtomicUsize,
    by_request: Mutex<Vec<(u64, usize)>>,
}

impl State {
    fn record(&self, id: usize, label: &'static str) {
        lock(&self.events).push((id, label));
        self.changed.notify_waiters();
    }
    async fn until(&self, id: usize, label: &'static str) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if lock(&self.events).contains(&(id, label)) {
                return;
            }
            changed.await;
        }
    }
    fn terminations(&self) -> Vec<usize> {
        lock(&self.events)
            .iter()
            .filter_map(|(id, label)| (*label == "termination").then_some(*id))
            .collect()
    }
}

struct Retained {
    id: usize,
    state: Arc<State>,
}
impl Drop for Retained {
    fn drop(&mut self) {
        assert!(ProcessContext::current().is_some());
        self.state.record(self.id, "state-released");
    }
}

struct StackValue(usize, Arc<State>, bool);
impl Drop for StackValue {
    fn drop(&mut self) {
        self.1.record(self.0, "stack-released");
        assert!(!self.2, "contained cleanup future destructor panic");
    }
}

struct Middleware(Arc<State>);

#[async_trait::async_trait]
impl HttpMiddleware for Middleware {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
        let probe = extensions.get_service::<RouteProbe>(None).await?;
        let owner = extensions.get_service::<OwnerProbe>(None).await?;
        let _ = probe.state.route_probe.set(owner.state.clone());
        probe.state.constructors.fetch_add(1, Ordering::AcqRel);
        Ok(Self(probe.state.clone()))
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("termination_probe", crate::MiddlewareKind::Custom)
    }
    async fn handle(
        &self,
        exchange: &mut HttpExchange<'_>,
        next: HttpNext<'_>,
        _: crate::ExecutionCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        self.0.next_id.fetch_add(1, Ordering::AcqRel);
        let id = exchange
            .invocation_id()
            .expect("managed invocation identity");
        if id == 0 {
            if let Some(probe) = self.0.route_probe.get() {
                exchange.request_mut().local_mut().insert(RequestCapture(
                    probe.clone(),
                    ProcessContext::current().unwrap().process_id,
                ));
            }
        }
        exchange
            .termination_state_mut()
            .expect("managed invocation has retained storage")
            .insert(Retained {
                id,
                state: self.0.clone(),
            });
        let _stack = StackValue(id, self.0.clone(), false);
        self.0.record(id, "before");
        if self.0.clear_local {
            exchange.request_mut().local_mut().clear();
        }
        match self.0.normal[id] {
            Normal::BeforePending => std::future::pending::<()>().await,
            Normal::BeforePanic => panic!("contained middleware poll panic"),
            Normal::Short => return Ok(()),
            Normal::Error => {
                return Err(HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL))
            }
            Normal::DropNext => {
                let mut next = Box::pin(next.run(exchange));
                assert!(next.as_mut().now_or_never().is_none());
                drop(next);
                return Ok(());
            }
            _ => {}
        }
        next.run(exchange).await?;
        self.0.record(id, "after");
        match self.0.normal[id] {
            Normal::AfterPending => std::future::pending::<()>().await,
            Normal::AfterError => {
                return Err(HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL))
            }
            _ => {}
        }
        Ok(())
    }

    async fn on_request_termination(
        &self,
        context: &mut HttpRequestTerminationContext<'_>,
        signal: CleanupCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        let id = context.invocation_id();
        assert_eq!(context.state().get::<Retained>().unwrap().id, id);
        assert!(lock(&self.0.events).contains(&(id, "stack-released")));
        assert_eq!(context.metadata().method(), "GET");
        assert_eq!(context.metadata().path(), "/termination");
        assert_eq!(
            context.metadata().request_id(),
            ProcessContext::current().unwrap().process_id
        );
        let service = context
            .extensions()
            .get_service::<ScopedProbe>(None)
            .await
            .unwrap();
        assert_eq!(service.context_id, context.metadata().request_id());
        assert!(!lock(&service.owner.state.events)
            .iter()
            .any(|(request, event)| *request == service.context_id && *event == "dispose"));
        assert_eq!(signal.is_cancelled(), context.cancellation().is_cancelled());
        assert_eq!(signal.deadline(), context.cancellation().deadline());
        lock(&self.0.stages).push((id, context.stage()));
        lock(&self.0.reasons).push(context.interruption());
        lock(&self.0.by_request).push((context.metadata().request_id(), id));
        lock(&self.0.views).push((id, signal.clone()));
        self.0.record(id, "termination");
        let _stack = StackValue(
            id,
            self.0.clone(),
            matches!(self.0.cleanup[id], Cleanup::DropPanic),
        );
        match self.0.cleanup[id] {
            Cleanup::Complete => {}
            Cleanup::Error => {
                return Err(HttpMiddlewareError::internal(MiddlewareErrorCode::INTERNAL))
            }
            Cleanup::Panic => panic!("contained termination hook panic"),
            Cleanup::Pending | Cleanup::DropPanic => std::future::pending::<()>().await,
            Cleanup::Cooperative => {
                signal.cancelled().await;
                assert!(context.cancellation().is_cancelled());
                assert_eq!(signal.deadline(), context.cancellation().deadline());
                self.0.record(id, "cleanup-signal");
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Cleanup::FileHelper => {
                // Real framework-owned blocking mount open, registered under
                // cleanup authority even though execution resources are sealed.
                let _mount = crate::StaticFileMount::new(std::env::temp_dir(), "/")
                    .await
                    .unwrap();
            }
        }
        self.0.record(id, "termination-returned");
        Ok(())
    }
}

struct Terminal(Arc<State>);

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct RouteProbe {
    state: Arc<State>,
}
impl ServiceTrait for RouteProbe {}

struct RouteInner(Middleware);
#[async_trait::async_trait]
impl HttpMiddleware for RouteInner {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
        Ok(Self(Middleware::new(extensions).await?))
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        self.0.descriptor()
    }
    async fn handle(
        &self,
        exchange: &mut HttpExchange<'_>,
        next: HttpNext<'_>,
        signal: crate::ExecutionCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        self.0.handle(exchange, next, signal).await
    }
    async fn on_request_termination(
        &self,
        context: &mut HttpRequestTerminationContext<'_>,
        signal: CleanupCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        self.0.on_request_termination(context, signal).await
    }
}

#[derive(crate::Controller)]
#[base_path("/")]
#[middleware(Middleware)]
struct RoutedController(Arc<Extensions>);

#[async_trait::async_trait]
impl crate::ControllerTrait for RoutedController {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, crate::ControllerInitError> {
        Ok(Self(extensions))
    }
}

#[crate::controller]
impl RoutedController {
    #[get("/termination")]
    #[middleware(RouteInner)]
    async fn pending(&self) -> Result<crate::Json<&'static str>, HttpApiError> {
        let probe = self.0.get_service::<RouteProbe>(None).await.unwrap();
        probe.state.record(3, "terminal");
        std::future::pending().await
    }
}

#[tokio::test(start_paused = true)]
async fn actual_application_controller_action_chains_share_one_ledger_per_concurrent_request() {
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let app = Arc::new(
        AppBuilder::new("127.0.0.1:0")
            .container(container.clone())
            .middleware::<Middleware>()
            .build()
            .await
            .unwrap(),
    );
    let probe = app
        .extensions()
        .get_service::<RouteProbe>(None)
        .await
        .unwrap();
    // The same Middleware is shared by application and controller positions;
    // only it and the distinct action wrapper were constructed.
    assert_eq!(probe.state.constructors.load(Ordering::Acquire), 2);
    let mut waiters = Vec::new();
    for _ in 0..8 {
        let service = app.clone();
        waiters.push(
            app.request_registry()
                .spawn(app.clone(), move |owner| async move {
                    let request = Request::from_transport_parts(
                        "GET".into(),
                        "/termination".into(),
                        vec![],
                        &[],
                    )
                    .await
                    .unwrap();
                    owner
                        .execute_for_test(
                            Duration::from_secs(10),
                            service.call_with_outcome(
                                request,
                                Response::new().await.unwrap(),
                                &owner,
                            ),
                        )
                        .await
                })
                .unwrap(),
        );
    }
    loop {
        let changed = probe.state.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if lock(&probe.state.events)
            .iter()
            .filter(|event| **event == (3, "terminal"))
            .count()
            == 8
        {
            break;
        }
        changed.await;
    }
    for waiter in &waiters {
        waiter.context.stop(ExecutionStopReason::ForcedShutdown);
    }
    for waiter in waiters {
        assert!(matches!(
            waiter.wait().await.unwrap(),
            Err(ExecutionInterrupted::Stopped)
        ));
    }
    let snapshot = app.request_registry().wait().await;
    assert!(snapshot.is_terminal());
    assert_eq!(snapshot.retired, 8);
    assert_eq!(snapshot.middleware.entered, 24);
    assert_eq!(snapshot.middleware.completed, 24);
    assert_eq!(snapshot.middleware.resources_outstanding, 0);
    let mut requests = HashMap::<u64, Vec<usize>>::new();
    for (id, invocation) in lock(&probe.state.by_request).iter() {
        requests.entry(*id).or_default().push(*invocation);
    }
    assert_eq!(requests.len(), 8);
    assert!(requests.values().all(|order| order == &[2, 1, 0]));
    close_case(&app, false).await;
}
#[async_trait::async_trait]
impl HttpNextService for Terminal {
    async fn run(&self, _: &mut HttpExchange<'_>) -> Result<(), HttpMiddlewareError> {
        self.0.record(3, "terminal");
        if self.0.terminal_pending {
            std::future::pending::<()>().await;
        }
        Ok(())
    }
}

struct Writer;
#[async_trait::async_trait]
impl HttpMiddlewareErrorWriter for Writer {
    async fn write_error_response(
        &self,
        exchange: &mut HttpExchange<'_>,
        error: &HttpMiddlewareError,
    ) -> Result<(), HttpMiddlewareError> {
        exchange
            .response_mut()
            .status_code(usize::from(error.http_status()), "Error");
        Ok(())
    }
}

async fn case(
    state: State,
    timeout: Duration,
) -> (
    Arc<App>,
    Arc<State>,
    RequestWaiter<Result<Result<(), HttpApiError>, ExecutionInterrupted>>,
) {
    let (app, probe) = application(true).await;
    let state = Arc::new(state);
    // Intentionally the exact same application-wide object at all positions.
    // A TypeId- or instance-keyed ledger would collapse these obligations.
    let instance: Arc<dyn HttpMiddleware> = Arc::new(Middleware(state.clone()));
    let middlewares = (0..3)
        .map(|_| CompiledHttpMiddleware::new(instance.clone(), instance.descriptor()))
        .collect::<Vec<_>>();
    let terminal = Terminal(state.clone());
    let application = app.clone();
    let runtime_state = state.clone();
    let waiter = app
        .request_registry()
        .spawn(app.clone(), move |owner| async move {
            let result = owner
                .execute_for_test(timeout, async {
                    let request = Request::from_transport_parts(
                        "GET".into(),
                        "/termination".into(),
                        vec![],
                        &[],
                    )
                    .await
                    .unwrap();
                    let mut resources = owner
                        .prepare_scope(
                            application.container(),
                            request,
                            Response::new().await.unwrap(),
                        )
                        .await?;
                    let resources = resources.as_mut().unwrap();
                    resources
                        .scope
                        .as_ref()
                        .unwrap()
                        .run(async {
                            let _ = application
                                .extensions()
                                .get_service::<ScopedProbe>(None)
                                .await
                                .unwrap();
                            let request = resources.request.as_mut().unwrap();
                            request.local_mut().insert(RequestCapture(
                                probe.clone(),
                                ProcessContext::current().unwrap().process_id,
                            ));
                            let mut exchange =
                                HttpExchange::new(request, resources.response.as_mut().unwrap());
                            HttpMiddlewareChain::new(
                                &middlewares,
                                &terminal,
                                &Writer,
                                &MiddlewareChainOutcomeSlot::default(),
                                &NoopHttpMiddlewareObserver,
                            )
                            .run(&mut exchange)
                            .await
                            .map_err(|_| HttpApiError::StateError("test writer failed".into()))
                        })
                        .await
                        .map_err(HttpApiError::from)?
                })
                .await;
            let body = lock(&runtime_state.body).take();
            if let Some(body) = body {
                use lily_web_core::{IntoResponse, TransportResponseBody};
                let mut resources = owner.0.resources.lock().await;
                let resources = resources.as_mut().unwrap();
                let response = resources.response.as_mut().unwrap();
                let request = resources.request.as_mut().unwrap();
                resources
                    .scope
                    .as_ref()
                    .unwrap()
                    .run(async {
                        crate::streaming(PendingBody(runtime_state.clone()))
                            .write_to_response(response, request)
                            .await
                            .unwrap();
                    })
                    .await
                    .unwrap();
                let response = resources
                    .response
                    .take()
                    .unwrap()
                    .into_transport_parts()
                    .unwrap();
                let TransportResponseBody::Stream(source) = response.into_body() else {
                    unreachable!()
                };
                let bridge = owner.own_response_stream(source, 64, None);
                assert!(body.send(bridge).is_ok());
            }
            result
        })
        .unwrap();
    (app, state, waiter)
}

struct PendingBody(Arc<State>);
impl futures::Stream for PendingBody {
    type Item = Result<bytes::Bytes, lily_web_core::ResponseBodyError>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Pending
    }
}
impl Drop for PendingBody {
    fn drop(&mut self) {
        self.0.record(3, "body-released");
    }
}

struct Input;
#[async_trait::async_trait]
impl lily_web_core::RequestBodyStream for Input {
    async fn next_chunk(
        &mut self,
    ) -> Result<Option<bytes::Bytes>, lily_web_core::RequestBodyError> {
        std::future::pending().await
    }
    fn size_hint(&self) -> (u64, Option<u64>) {
        (0, None)
    }
}

#[tokio::test(start_paused = true)]
async fn retained_input_receipt_blocks_all_termination_hooks_until_actual_release() {
    let (app, state, waiter) = case(
        State {
            terminal_pending: true,
            ..Default::default()
        },
        Duration::from_secs(10),
    )
    .await;
    let context = waiter.context.clone();
    state.until(3, "terminal").await;
    let input = context.track_input(Box::new(Input));
    context.stop(ExecutionStopReason::ForcedShutdown);
    state.until(0, "stack-released").await;
    assert!(state.terminations().is_empty());
    assert_eq!(app.container().active_scope_count(), 1);
    drop(input);
    let _ = waiter.wait().await.unwrap();
    assert_eq!(state.terminations(), [2, 1, 0]);
    close_case(&app, false).await;
}

#[tokio::test(start_paused = true)]
async fn response_source_release_is_required_before_eligible_inner_cleanup() {
    let (send, receive) = oneshot::channel();
    let (app, state, waiter) = case(
        State {
            normal: [Normal::DropNext, Normal::BeforePending, Normal::Around],
            body: Mutex::new(Some(send)),
            ..Default::default()
        },
        Duration::from_secs(10),
    )
    .await;
    let context = waiter.context.clone();
    let bridge = receive.await.unwrap();
    assert!(context.close_scope().await.is_err());
    assert!(state.terminations().is_empty());
    assert_eq!(app.container().active_scope_count(), 1);
    drop(bridge);
    waiter.wait().await.unwrap().unwrap().unwrap();
    assert_eq!(state.terminations(), [1]);
    let events = lock(&state.events).clone();
    assert!(
        events.iter().position(|e| *e == (3, "body-released"))
            < events.iter().position(|e| *e == (1, "termination"))
    );
    drop(events);
    close_case(&app, false).await;
}

#[tokio::test(start_paused = true)]
async fn later_body_interruption_never_rearms_normally_returned_middleware() {
    let (send, receive) = oneshot::channel();
    let (app, state, waiter) = case(
        State {
            body: Mutex::new(Some(send)),
            ..Default::default()
        },
        Duration::from_secs(10),
    )
    .await;
    let context = waiter.context.clone();
    let bridge = receive.await.unwrap();
    assert_eq!(context.0.middleware.snapshot().normal_returned, 3);
    assert!(!context.0.middleware.snapshot().is_terminal()); // retained state still lives with body
    context.stop(ExecutionStopReason::ForcedShutdown);
    waiter.wait().await.unwrap().unwrap().unwrap();
    assert!(state.terminations().is_empty());
    assert_eq!(context.0.middleware.snapshot().termination_started, 0);
    assert!(context.0.middleware.snapshot().is_terminal());
    drop(bridge);
    close_case(&app, false).await;
}

#[tokio::test]
async fn cleanup_file_helpers_have_retained_joins_outside_the_sealed_execution_inventory() {
    let (app, state, waiter) = case(
        State {
            terminal_pending: true,
            cleanup: [Cleanup::FileHelper; 3],
            ..Default::default()
        },
        Duration::from_secs(10),
    )
    .await;
    let context = waiter.context.clone();
    state.until(3, "terminal").await;
    context.stop(ExecutionStopReason::ForcedShutdown);
    let _ = waiter.wait().await.unwrap();
    let snapshot = context.0.middleware.snapshot();
    assert_eq!(snapshot.helpers_registered, 3);
    assert_eq!(snapshot.helpers_joined, 3);
    assert_eq!(snapshot.completed, 3);
    assert_eq!(state.terminations(), [2, 1, 0]);
    close_case(&app, false).await;
}

async fn close_case(app: &App, failed: bool) {
    let snapshot = app.request_registry().wait().await;
    assert!(snapshot.is_terminal(), "{snapshot:?}");
    assert_eq!(snapshot.cleanup_failed > 0, failed);
    // These request-local failures retired before application shutdown.
    app.close().await.unwrap();
    assert_eq!(app.request_registry().attempt_snapshot().cleanup_failed, 0);
    let result = app.container().close().await;
    if !failed {
        result.unwrap();
    }
    assert_eq!(app.container().active_scope_count(), 0);
}

#[tokio::test(start_paused = true)]
async fn force_unwinds_same_instance_invocations_in_reverse_after_actual_execution_drop() {
    let (app, state, waiter) = case(
        State {
            terminal_pending: true,
            clear_local: true,
            ..Default::default()
        },
        Duration::from_secs(10),
    )
    .await;
    let context = waiter.context.clone();
    state.until(3, "terminal").await;
    context.stop(ExecutionStopReason::ForcedShutdown);
    assert!(matches!(
        waiter.wait().await.unwrap(),
        Err(ExecutionInterrupted::Stopped)
    ));
    assert_eq!(state.terminations(), [2, 1, 0]);
    assert_eq!(
        *lock(&state.stages),
        [
            (2, HttpMiddlewareStage::DelegatingToNext),
            (1, HttpMiddlewareStage::DelegatingToNext),
            (0, HttpMiddlewareStage::DelegatingToNext)
        ]
    );
    assert!(lock(&state.reasons)
        .iter()
        .all(|reason| *reason == HttpRequestInterruption::ForcedShutdown));
    assert_eq!(context.0.middleware.snapshot().completed, 3);
    assert!(context.0.middleware.snapshot().is_terminal());
    context.close_scope().await.unwrap();
    assert_eq!(state.terminations(), [2, 1, 0]);
    close_case(&app, false).await;
}

#[tokio::test(start_paused = true)]
async fn interrupted_before_only_arms_the_entered_prefix() {
    let (app, state, waiter) = case(
        State {
            normal: [Normal::Around, Normal::BeforePending, Normal::Around],
            ..Default::default()
        },
        Duration::from_secs(10),
    )
    .await;
    let context = waiter.context.clone();
    state.until(1, "before").await;
    context.stop(ExecutionStopReason::PeerDisconnect);
    let _ = waiter.wait().await.unwrap();
    assert_eq!(state.terminations(), [1, 0]);
    assert_eq!(context.0.middleware.snapshot().entered, 2);
    assert_eq!(
        *lock(&state.stages),
        [
            (1, HttpMiddlewareStage::Before),
            (0, HttpMiddlewareStage::DelegatingToNext)
        ]
    );
    close_case(&app, false).await;
}

#[tokio::test(start_paused = true)]
async fn partial_normal_after_does_not_reopen_the_returned_inner_frame() {
    let (app, state, waiter) = case(
        State {
            normal: [Normal::Around, Normal::AfterPending, Normal::Around],
            ..Default::default()
        },
        Duration::from_secs(10),
    )
    .await;
    let context = waiter.context.clone();
    state.until(1, "after").await;
    app.request_registry().close_admission();
    assert_eq!(context.0.middleware.snapshot().normal_exit_interrupted, 0);
    context.stop(ExecutionStopReason::ForcedShutdown);
    let _ = waiter.wait().await.unwrap();
    assert_eq!(state.terminations(), [1, 0]);
    assert_eq!(context.0.middleware.snapshot().normal_returned, 1);
    assert_eq!(lock(&state.stages)[0], (1, HttpMiddlewareStage::After));
    close_case(&app, false).await;
    let report = app.shutdown_report().unwrap();
    assert_eq!(report.requests.middleware.normal_exit_interrupted, 1);
    assert_eq!(report.requests.middleware.normal_returned, 1);
    assert_eq!(report.requests.middleware.delegation_interrupted, 1);
    assert_eq!(report.requests.middleware.completed, 2);
    assert!(report.reconciles() && report.succeeded());
}

#[tokio::test(start_paused = true)]
async fn typed_errors_short_circuit_and_normal_completion_do_not_invoke_termination() {
    for behavior in [
        Normal::Around,
        Normal::Short,
        Normal::Error,
        Normal::AfterError,
    ] {
        let (app, state, waiter) = case(
            State {
                normal: [Normal::Around, behavior, Normal::Around],
                ..Default::default()
            },
            Duration::from_secs(10),
        )
        .await;
        let context = waiter.context.clone();
        waiter.wait().await.unwrap().unwrap().unwrap();
        assert!(state.terminations().is_empty());
        let snapshot = context.0.middleware.snapshot();
        assert_eq!(snapshot.entered, snapshot.normal_returned);
        assert!(snapshot.is_terminal());
        close_case(&app, false).await;
    }
}

#[tokio::test(start_paused = true)]
async fn contained_execution_panic_keeps_its_entered_obligations() {
    let (app, state, waiter) = case(
        State {
            normal: [Normal::Around, Normal::BeforePanic, Normal::Around],
            ..Default::default()
        },
        Duration::from_secs(10),
    )
    .await;
    assert!(matches!(
        waiter.wait().await.unwrap(),
        Err(ExecutionInterrupted::Panicked)
    ));
    assert_eq!(state.terminations(), [1, 0]);
    assert!(lock(&state.reasons)
        .iter()
        .all(|reason| *reason == HttpRequestInterruption::ExecutionPanicked));
    close_case(&app, false).await;
}

#[tokio::test(start_paused = true)]
async fn application_abandoned_next_is_not_mistaken_for_an_inner_normal_return() {
    let (app, state, waiter) = case(
        State {
            normal: [Normal::DropNext, Normal::BeforePending, Normal::Around],
            ..Default::default()
        },
        Duration::from_secs(10),
    )
    .await;
    waiter.wait().await.unwrap().unwrap().unwrap();
    assert_eq!(state.terminations(), [1]);
    assert_eq!(
        *lock(&state.reasons),
        [HttpRequestInterruption::ExecutionInterrupted]
    );
    let events = lock(&state.events).clone();
    let inner = events
        .iter()
        .position(|event| *event == (1, "termination-returned"))
        .unwrap();
    let outer = events
        .iter()
        .position(|event| *event == (0, "state-released"))
        .unwrap();
    assert!(
        inner < outer,
        "framework-retained parent state must outlive inner cleanup"
    );
    drop(events);
    close_case(&app, false).await;
}

#[tokio::test(start_paused = true)]
async fn hook_timeout_panic_and_error_do_not_cancel_or_skip_outer_invocations() {
    for behavior in [Cleanup::Pending, Cleanup::Panic, Cleanup::Error] {
        let (app, state, waiter) = case(
            State {
                terminal_pending: true,
                cleanup: [Cleanup::Complete, Cleanup::Complete, behavior],
                ..Default::default()
            },
            Duration::from_secs(10),
        )
        .await;
        let context = waiter.context.clone();
        state.until(3, "terminal").await;
        context.stop(ExecutionStopReason::ForcedShutdown);
        let _ = waiter.wait().await.unwrap();
        assert_eq!(state.terminations(), [2, 1, 0]);
        let views = lock(&state.views).clone();
        assert!(!views[1].1.is_cancelled());
        assert!(!views[2].1.is_cancelled());
        drop(views);
        let snapshot = context.0.middleware.snapshot();
        assert_eq!(snapshot.completed, 2);
        assert_eq!(snapshot.timed_out + snapshot.panicked + snapshot.failed, 1);
        assert!(snapshot.is_terminal());
        close_case(&app, true).await;
    }
}

#[tokio::test(start_paused = true)]
async fn cleanup_signal_has_a_bounded_cooperative_tail_and_matching_context_authority() {
    let (app, state, waiter) = case(
        State {
            terminal_pending: true,
            cleanup: [Cleanup::Complete, Cleanup::Complete, Cleanup::Cooperative],
            ..Default::default()
        },
        Duration::from_secs(10),
    )
    .await;
    let context = waiter.context.clone();
    state.until(3, "terminal").await;
    context.stop(ExecutionStopReason::ForcedShutdown);
    let started = Instant::now();
    let _ = waiter.wait().await.unwrap();
    assert!(lock(&state.events).contains(&(2, "cleanup-signal")));
    assert!(lock(&state.events).contains(&(2, "termination-returned")));
    assert!(
        Instant::now() < started + COOPERATIVE_CANCELLATION_CAP + middleware::TERMINATION_HOOK_CAP
    );
    assert_eq!(context.0.middleware.snapshot().completed, 3);
    close_case(&app, false).await;
}

#[tokio::test(start_paused = true)]
async fn exhausted_root_cleanup_budget_never_polls_hooks_or_renews_sibling_budgets() {
    let (app, state, waiter) = case(
        State {
            terminal_pending: true,
            cleanup: [Cleanup::Pending; 3],
            ..Default::default()
        },
        Duration::from_secs(120),
    )
    .await;
    let context = waiter.context.clone();
    state.until(3, "terminal").await;
    let root = app.shutdown_budget().begin();
    tokio::time::advance(root.at(ShutdownStage::Cleanup) - Instant::now()).await;
    let _ = waiter.wait().await.unwrap();
    assert!(state.terminations().is_empty());
    let snapshot = context.0.middleware.snapshot();
    assert_eq!(snapshot.not_started, 3);
    assert_eq!(snapshot.termination_started, 0);
    assert!(snapshot.is_terminal());
    assert!(Instant::now() <= root.at(ShutdownStage::Reconcile));
    close_case(&app, true).await;
}

#[tokio::test(start_paused = true)]
async fn request_timeout_keeps_cleanup_independent_and_waiter_loss_does_not_abort_hooks() {
    let (app, state, waiter) = case(
        State {
            terminal_pending: true,
            cleanup: [Cleanup::Complete, Cleanup::Complete, Cleanup::Cooperative],
            ..Default::default()
        },
        Duration::from_secs(1),
    )
    .await;
    let context = waiter.context.clone();
    state.until(2, "termination").await;
    drop(waiter);
    assert!(context.cancellation().is_cancelled());
    assert_eq!(
        context.cancellation_reason(),
        Some(ExecutionStopReason::RequestTimeout)
    );
    let snapshot = app.request_registry().wait().await;
    assert!(snapshot.is_terminal());
    assert_eq!(state.terminations(), [2, 1, 0]);
    assert!(lock(&state.events).contains(&(2, "termination-returned")));
    close_case(&app, false).await;
}

#[tokio::test(start_paused = true)]
async fn pending_hooks_share_one_owner_cutoff_and_report_unstarted_outer_frame() {
    let (app, state, waiter) = case(
        State {
            terminal_pending: true,
            cleanup: [Cleanup::Pending; 3],
            ..Default::default()
        },
        Duration::from_millis(400),
    )
    .await;
    let context = waiter.context.clone();
    let started = Instant::now();
    let _ = waiter.wait().await.unwrap();
    assert_eq!(state.terminations(), [2, 1]);
    let snapshot = context.0.middleware.snapshot();
    assert_eq!(snapshot.timed_out, 2);
    assert_eq!(snapshot.not_started, 1);
    assert!(
        Instant::now() <= started + Duration::from_millis(400) * 2 + COOPERATIVE_CANCELLATION_CAP
    );
    close_case(&app, true).await;
}

#[tokio::test(start_paused = true)]
async fn cleanup_future_drop_panic_blocks_parent_hooks_and_scope_disposal() {
    let (app, state, waiter) = case(
        State {
            terminal_pending: true,
            cleanup: [Cleanup::Complete, Cleanup::Complete, Cleanup::DropPanic],
            ..Default::default()
        },
        Duration::from_secs(10),
    )
    .await;
    let context = waiter.context.clone();
    state.until(3, "terminal").await;
    context.stop(ExecutionStopReason::ForcedShutdown);
    let _ = waiter.wait().await.unwrap();
    assert_eq!(state.terminations(), [2]);
    assert!(!context.0.middleware.snapshot().is_terminal());
    assert_eq!(app.container().active_scope_count(), 1);
    assert!(context.close_scope().await.is_err());
    assert_eq!(state.terminations(), [2]);
    assert!(app.close().await.is_err());
    assert_eq!(app.request_registry().snapshot().outstanding, 1);
    // Caller-owned container remains caller-owned even in an incomplete HTTP
    // shutdown. This explicit external close is not an HTTP success claim.
    assert!(app.container().close().await.is_err());
}
