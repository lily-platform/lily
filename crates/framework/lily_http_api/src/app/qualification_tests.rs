//! Managed listener -> generated callbacks -> body -> scope -> dependency ->
//! actual root join. Faults are in application callbacks, never synthetic
//! request counters. Every client/observer task is explicitly joined.

use super::*;
use bytes::Bytes;
use futures::{stream, StreamExt};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::{TokioExecutor, TokioIo};
use lily_injection::Injectable;
use lily_injection::{InjectionError, ProcessContext, ServiceTrait};
use std::sync::{atomic::AtomicUsize, Mutex};
use tokio::{io::AsyncWriteExt, sync::Notify, task::JoinHandle};

use crate::shutdown_report::{HttpShutdownCompletion, HttpShutdownReport};
use crate::{
    CleanupCancellation, ControllerInitError, ControllerTrait, ExecutionCancellation,
    FromRequestParts, GuardInitError, GuardRejection, HttpNext, HttpRequestTerminationContext,
    PlainText, ResponseBodyError, ResponseWriteOutcome,
};

const OTHER: usize = usize::MAX;
type Event = (u64, usize, &'static str);

#[derive(Default)]
struct State {
    events: Mutex<Vec<Event>>,
    changed: Notify,
    release: CancellationToken,
    disposer_release: CancellationToken,
    cleanup_mode: AtomicUsize,
    disposal_mode: AtomicUsize,
    active_scopes: AtomicUsize,
    views: Mutex<Vec<ExecutionCancellation>>,
    file: Mutex<Option<PathBuf>>,
}

impl State {
    fn record(&self, id: usize, label: &'static str) {
        let request = ProcessContext::current().map_or(0, |context| context.process_id);
        self.events.lock().unwrap().push((request, id, label));
        self.changed.notify_waiters();
    }

    async fn until(&self, label: &'static str, count: usize) {
        bounded(async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self
                    .events
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|e| e.2 == label)
                    .count()
                    >= count
                {
                    return;
                }
                changed.await;
            }
        })
        .await;
    }

    fn ids(&self, label: &str) -> Vec<usize> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| (event.2 == label).then_some(event.1))
            .collect()
    }

    fn before(&self, first: &str, second: &str) {
        let events = self.events.lock().unwrap();
        let first = events.iter().rposition(|event| event.2 == first).unwrap();
        let second = events.iter().position(|event| event.2 == second).unwrap();
        assert!(first < second, "{events:?}");
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct QualificationProbe {
    state: Arc<State>,
}

#[async_trait::async_trait]
impl ServiceTrait for QualificationProbe {
    async fn dispose(&self) -> Result<(), InjectionError> {
        assert_eq!(self.state.active_scopes.load(Ordering::Acquire), 0);
        self.state.record(OTHER, "application-disposed");
        Ok(())
    }
}

#[derive(Default, Injectable)]
#[service(lifetime = "Scoped")]
struct QualificationScope {
    #[inject]
    probe: Arc<QualificationProbe>,
    request_id: u64,
}

#[async_trait::async_trait]
impl ServiceTrait for QualificationScope {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.request_id = ProcessContext::current().unwrap().process_id;
        self.probe
            .state
            .active_scopes
            .fetch_add(1, Ordering::AcqRel);
        self.probe.state.record(OTHER, "scope-created");
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        assert_eq!(
            ProcessContext::current().unwrap().process_id,
            self.request_id
        );
        let state = &self.probe.state;
        let _use_guard = DisposerUse(state.clone());
        state.record(OTHER, "scope-disposing");
        if state.disposal_mode.load(Ordering::Acquire) == 1 {
            state.disposer_release.cancelled().await;
        }
        state.record(OTHER, "scope-disposed");
        Ok(())
    }
}

struct DisposerUse(Arc<State>);
impl Drop for DisposerUse {
    fn drop(&mut self) {
        self.0.active_scopes.fetch_sub(1, Ordering::AcqRel);
        self.0.record(OTHER, "disposer-released");
    }
}

struct Capture(Arc<State>, usize, &'static str);
impl Drop for Capture {
    fn drop(&mut self) {
        self.0.record(self.1, self.2);
    }
}

struct QualificationAround(Arc<Extensions>, Arc<State>);
#[async_trait::async_trait]
impl HttpMiddleware for QualificationAround {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
        let state = extensions
            .get_service::<QualificationProbe>(None)
            .await?
            .state
            .clone();
        Ok(Self(extensions, state))
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("managed-qualification", crate::MiddlewareKind::Custom)
    }

    async fn handle(
        &self,
        exchange: &mut HttpExchange<'_>,
        next: HttpNext<'_>,
        cancellation: ExecutionCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        let id = exchange.invocation_id().unwrap();
        self.0
            .get_service::<QualificationScope>(None)
            .await
            .unwrap();
        let _capture = Capture(self.1.clone(), id, "execution-released");
        exchange.termination_state_mut().unwrap().insert(Capture(
            self.1.clone(),
            id,
            "ledger-released",
        ));
        self.1.views.lock().unwrap().push(cancellation.clone());
        self.1.record(id, "before");
        if id == 1 && exchange.request().path().ends_with("/before") {
            self.1.record(id, "blocked");
            cancellation.cancelled().await;
            self.1.record(id, "cancel-observed");
            std::future::pending::<()>().await;
        }
        next.run(exchange).await?;
        if id == 1 && exchange.request().path().ends_with("/after") {
            self.1.record(id, "blocked");
            cancellation.cancelled().await;
            self.1.record(id, "cancel-observed");
            std::future::pending::<()>().await;
        }
        self.1.record(id, "normal-after");
        Ok(())
    }

    async fn on_request_termination(
        &self,
        context: &mut HttpRequestTerminationContext<'_>,
        cancellation: CleanupCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        let id = context.invocation_id();
        assert_eq!(context.state().get::<Capture>().unwrap().1, id);
        assert_eq!(context.cancellation().deadline(), cancellation.deadline());
        assert!(
            !cancellation.is_cancelled(),
            "another invocation cannot cancel this child"
        );
        assert!(self
            .1
            .views
            .lock()
            .unwrap()
            .iter()
            .all(ExecutionCancellation::is_cancelled));
        assert!(self.1.ids("execution-released").contains(&id));
        assert!(self.1.ids("scope-disposing").is_empty());
        let scope = context
            .extensions()
            .get_service::<QualificationScope>(None)
            .await
            .unwrap();
        assert_eq!(scope.request_id, context.metadata().request_id());
        self.1.record(id, "termination");
        if id == 2 {
            match self.1.cleanup_mode.load(Ordering::Acquire) {
                1 => std::future::pending::<()>().await,
                2 => panic!("qualification: contained user cleanup panic"),
                _ => {}
            }
        }
        self.1.record(id, "termination-returned");
        Ok(())
    }
}

// Controller/action attributes deliberately reject a duplicated middleware
// type in their merged plan. Application + controller reuse is supported;
// the action uses a distinct type with the same observed callback behavior.
struct QualificationActionAround(QualificationAround);
#[async_trait::async_trait]
impl HttpMiddleware for QualificationActionAround {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
        Ok(Self(QualificationAround::new(extensions).await?))
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new(
            "managed-action-qualification",
            crate::MiddlewareKind::Custom,
        )
    }
    async fn handle(
        &self,
        exchange: &mut HttpExchange<'_>,
        next: HttpNext<'_>,
        cancellation: ExecutionCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        self.0.handle(exchange, next, cancellation).await
    }
    async fn on_request_termination(
        &self,
        context: &mut HttpRequestTerminationContext<'_>,
        cancellation: CleanupCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        self.0.on_request_termination(context, cancellation).await
    }
}

struct QualificationGuard(Arc<State>);
#[async_trait::async_trait]
impl GuardTrait for QualificationGuard {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, GuardInitError> {
        Ok(Self(
            extensions
                .get_service::<QualificationProbe>(None)
                .await
                .unwrap()
                .state
                .clone(),
        ))
    }
    async fn can_activate(
        &self,
        request: &mut Request,
        cancellation: ExecutionCancellation,
    ) -> Result<(), GuardRejection> {
        if request.path().ends_with("/guard") {
            self.0.record(OTHER, "blocked");
            cancellation.cancelled().await;
            self.0.record(OTHER, "cancel-observed");
            std::future::pending().await
        }
        Ok(())
    }
}

struct QualificationParts;
impl FromRequestParts for QualificationParts {
    type Rejection = HttpApiError;
    async fn from_request_parts(
        request: &mut Request,
        extensions: &Extensions,
    ) -> Result<Self, Self::Rejection> {
        if request.path().ends_with("/extractor") {
            let probe = extensions
                .get_service::<QualificationProbe>(None)
                .await
                .unwrap();
            probe.state.record(OTHER, "blocked");
            request.execution_cancellation().cancelled().await;
            probe.state.record(OTHER, "cancel-observed");
            std::future::pending::<()>().await;
        }
        Ok(Self)
    }
}

#[derive(crate::Controller)]
#[base_path("/qualification")]
#[middleware(QualificationAround)]
struct QualificationController(Arc<Extensions>);
#[async_trait::async_trait]
impl ControllerTrait for QualificationController {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self(extensions))
    }
}

#[crate::controller]
impl QualificationController {
    #[get("/:stage")]
    #[middleware(QualificationActionAround)]
    #[guard(QualificationGuard)]
    async fn execute(
        &self,
        _parts: QualificationParts,
        request: &mut Request,
        cancellation: ExecutionCancellation,
    ) -> QualificationResponse {
        let scope = self
            .0
            .get_service::<QualificationScope>(None)
            .await
            .unwrap();
        let state = scope.probe.state.clone();
        state.record(OTHER, "action");
        let stage = request.path().rsplit('/').next().unwrap().to_owned();
        match stage.as_str() {
            "graceful" => {
                state.record(OTHER, "blocked");
                state.release.cancelled().await;
                assert!(!cancellation.is_cancelled());
            }
            "cooperative" | "ignore" => {
                state.record(OTHER, "blocked");
                cancellation.cancelled().await;
                state.record(OTHER, "cancel-observed");
                if stage == "ignore" {
                    std::future::pending::<()>().await;
                }
            }
            "input" => {
                state.record(OTHER, "blocked");
                let _ = request.next_body_chunk().await;
            }
            _ => {}
        }
        QualificationResponse {
            scope,
            stage,
            cancellation,
        }
    }
}

struct QualificationResponse {
    scope: Arc<QualificationScope>,
    stage: String,
    cancellation: ExecutionCancellation,
}

#[async_trait::async_trait]
impl IntoResponse for QualificationResponse {
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        let state = self.scope.probe.state.clone();
        if self.stage == "file" {
            let directory = state.file.lock().unwrap().clone().unwrap();
            let file = async {
                crate::StaticFileMount::new(directory, "/qualification")
                    .await?
                    .serve(request)
                    .await
            }
            .await;
            return file
                .map_err(HttpApiError::from)
                .write_to_response(response, request)
                .await;
        }
        if matches!(self.stage.as_str(), "stream" | "sse" | "echo") {
            let capture = Capture(state.clone(), OTHER, "source-released");
            let reader = if self.stage == "echo" {
                Some(match request.take_body_reader() {
                    Ok(reader) => reader,
                    Err(error) => {
                        return HttpApiError::from(error)
                            .write_to_response(response, request)
                            .await
                    }
                })
            } else {
                None
            };
            let source = stream::unfold(
                (false, self.scope, capture, reader),
                move |(sent, scope, capture, mut reader)| async move {
                    let state = &scope.probe.state;
                    if sent {
                        state.record(OTHER, "body-pending");
                        if let Some(reader) = reader.as_mut() {
                            let _ = reader.next_chunk().await;
                        } else {
                            state.release.cancelled().await;
                        }
                        return None;
                    }
                    Some((
                        Ok::<_, ResponseBodyError>(Bytes::from_static(b"first")),
                        (true, scope, capture, reader),
                    ))
                },
            );
            if self.stage == "sse" {
                return crate::SseResponse::new(
                    source.map(|item| item.map(|_| crate::SseEvent::new("first").unwrap())),
                )
                .keep_alive(Duration::from_secs(1))
                .unwrap()
                .write_to_response(response, request)
                .await;
            }
            return crate::streaming(source)
                .write_to_response(response, request)
                .await;
        }
        // Keep this view available through custom response conversion too.
        assert_eq!(
            self.cancellation.is_cancelled(),
            request.execution_cancellation().is_cancelled()
        );
        PlainText("qualified".into())
            .write_to_response(response, request)
            .await
    }
}

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("qualification barrier timed out")
}

async fn application(protocol: HttpProtocol) -> (App, Arc<State>) {
    application_with_container(protocol, None).await
}

async fn application_with_container(
    protocol: HttpProtocol,
    container: Option<Arc<ApplicationContainer>>,
) -> (App, Arc<State>) {
    let mut builder = AppBuilder::new("127.0.0.1:0")
        .protocol(protocol)
        .tracing_disabled()
        .middleware::<QualificationAround>();
    if let Some(container) = container {
        builder = builder.container(container);
    }
    let mut app = builder.build().await.unwrap();
    let lifecycle = Arc::get_mut(&mut app.lifecycle).unwrap();
    lifecycle.budget =
        ShutdownBudget::for_state(Duration::from_secs(3), lifecycle.shutdown_state.clone());
    let state = app
        .container()
        .resolve::<QualificationProbe>(None)
        .await
        .unwrap()
        .state
        .clone();
    (app, state)
}

async fn start(app: &App) -> JoinHandle<io::Result<()>> {
    let runtime = app.clone();
    let root = tokio::spawn(async move {
        runtime
            .start_with_cancellation(CancellationToken::new())
            .await
    });
    bounded(async {
        while app.bound_address().is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    root
}

async fn close(app: &App, force: bool) -> JoinHandle<io::Result<()>> {
    let observer = app.clone();
    let close = tokio::spawn(async move { observer.close().await });
    bounded(async {
        while !app.request_registry().admission_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    if force {
        app.lifecycle.shutdown_state.request_force();
    }
    close
}

type Sender = hyper::client::conn::http1::SendRequest<Full<Bytes>>;
async fn http1(app: &App) -> (Sender, JoinHandle<Result<(), hyper::Error>>) {
    let socket = tokio::net::TcpStream::connect(app.bound_address().unwrap())
        .await
        .unwrap();
    let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
        .await
        .unwrap();
    (sender, tokio::spawn(connection))
}

fn request(stage: &str) -> hyper::Request<Full<Bytes>> {
    hyper::Request::builder()
        .uri(format!("/qualification/{stage}"))
        .header("host", "lily.test")
        .body(Full::new(Bytes::new()))
        .unwrap()
}

fn send(mut sender: Sender, stage: &'static str) -> JoinHandle<Option<(u16, Bytes)>> {
    tokio::spawn(async move {
        let response = sender.send_request(request(stage)).await.ok()?;
        let status = response.status().as_u16();
        let body = response.into_body().collect().await.ok()?.to_bytes();
        Some((status, body))
    })
}

async fn finished(
    app: &App,
    root: JoinHandle<io::Result<()>>,
    close: JoinHandle<io::Result<()>>,
    success: bool,
) -> HttpShutdownReport {
    let close = bounded(close).await.unwrap();
    let root = bounded(root).await.unwrap();
    assert_eq!(close.is_ok(), success, "close: {close:?}");
    assert_eq!(root.is_ok(), success, "root: {root:?}");
    let report = app.shutdown_report().unwrap();
    assert!(report.reconciles(), "{report:#?}");
    assert_eq!(app.close().await.is_ok(), success);
    assert_eq!(
        app.shutdown_report().unwrap(),
        report,
        "replay cannot rewrite evidence"
    );
    report
}

fn terminal(report: HttpShutdownReport) {
    assert!(report.terminal(), "{report:#?}");
    assert_eq!(report.root.completed, 1);
    assert_eq!(report.listener.completed, 1);
    assert_eq!(
        report.requests_lifetime.resources.helpers_registered,
        report.requests_lifetime.resources.helpers_joined
    );
    assert_eq!(
        report.requests_lifetime.scopes.created,
        report.requests_lifetime.scopes.succeeded
    );
    assert!(report.connections.is_terminal() && report.protocol.is_terminal());
    assert_eq!(report.requests_lifetime.owners.outstanding, 0);
}

#[tokio::test]
async fn managed_idle_http1_and_http2_reconcile_actual_root_and_owned_di() {
    for protocol in [HttpProtocol::Http1_1, HttpProtocol::Http2] {
        let (app, state) = application(protocol).await;
        let root = start(&app).await;
        let closing = close(&app, false).await;
        let report = finished(&app, root, closing, true).await;
        terminal(report);
        assert_eq!(report.completion, HttpShutdownCompletion::GracefulCompleted);
        assert_eq!(report.requests_lifetime.registered, 0);
        assert_eq!(state.ids("application-disposed"), [OTHER]);
    }
}

#[tokio::test]
async fn managed_graceful_action_drains_buffered_response_before_application_di() {
    let (app, state) = application(HttpProtocol::Http1_1).await;
    let root = start(&app).await;
    let (sender, client) = http1(&app).await;
    let response = send(sender, "graceful");
    state.until("blocked", 1).await;
    let closing = close(&app, false).await;
    assert!(state
        .views
        .lock()
        .unwrap()
        .iter()
        .all(|view| !view.is_cancelled()));
    assert_eq!(app.request_registry().attempt_snapshot().outstanding, 1);
    state.release.cancel();
    assert_eq!(
        bounded(response).await.unwrap(),
        Some((200, Bytes::from_static(b"qualified")))
    );
    bounded(client).await.unwrap().unwrap();
    let report = finished(&app, root, closing, true).await;
    terminal(report);
    assert_eq!(report.completion, HttpShutdownCompletion::GracefulCompleted);
    assert_eq!(report.requests.execution.returned, 1);
    assert_eq!(report.requests.response.completed, 1);
    assert_eq!(state.ids("normal-after"), [2, 1, 0]);
    assert!(state.ids("termination").is_empty());
    state.before("ledger-released", "scope-disposing");
    state.before("scope-disposed", "application-disposed");
}

#[tokio::test]
async fn managed_graceful_deadline_polls_the_same_cooperative_action_to_return() {
    let (app, state) = application(HttpProtocol::Http1_1).await;
    let root = start(&app).await;
    let (sender, client) = http1(&app).await;
    let response = send(sender, "cooperative");
    state.until("blocked", 1).await;
    let closing = close(&app, false).await;
    let report = finished(&app, root, closing, true).await;
    let _ = bounded(response).await.unwrap();
    let _ = bounded(client).await.unwrap();
    terminal(report);
    assert_eq!(report.completion, HttpShutdownCompletion::ForcedCompleted);
    assert_eq!(report.requests.stop_reasons.graceful_deadline, 1);
    assert_eq!(report.requests.execution.returned, 1);
    assert_eq!(report.requests.execution.dropped, 0);
    assert_eq!(state.ids("cancel-observed"), [OTHER]);
    assert_eq!(state.ids("normal-after"), [2, 1, 0]);
    assert!(state.ids("termination").is_empty());
    assert!(report.observed_at <= report.deadline);
}

#[tokio::test]
async fn managed_force_preserves_generated_before_guard_extractor_action_and_after_ledgers() {
    for stage in ["before", "guard", "extractor", "ignore", "after"] {
        let (app, state) = application(HttpProtocol::Http1_1).await;
        let root = start(&app).await;
        let (sender, client) = http1(&app).await;
        let response = send(sender, stage);
        state.until("blocked", 1).await;
        let closing = close(&app, true).await;
        let report = finished(&app, root, closing, true).await;
        let _ = bounded(response).await.unwrap();
        let _ = bounded(client).await.unwrap();
        terminal(report);
        assert_eq!(
            report.completion,
            HttpShutdownCompletion::ForcedCompleted,
            "{stage}"
        );
        assert_eq!(report.requests.execution.dropped, 1, "{stage}");
        let expected = if matches!(stage, "before" | "after") {
            vec![1, 0]
        } else {
            vec![2, 1, 0]
        };
        assert_eq!(state.ids("termination"), expected, "{stage}");
        assert_eq!(report.requests.middleware.completed, expected.len());
        assert_eq!(
            state.ids("normal-after"),
            if stage == "after" { vec![2] } else { vec![] }
        );
        let events = state.events.lock().unwrap().clone();
        assert!(
            events
                .iter()
                .rposition(|e| e.2 == "cancel-observed")
                .unwrap()
                < events
                    .iter()
                    .rposition(|e| e.2 == "execution-released")
                    .unwrap()
        );
        state.before("execution-released", "termination");
        state.before("ledger-released", "scope-disposing");
        state.before("scope-disposed", "application-disposed");
    }
}

#[tokio::test]
async fn managed_cleanup_timeout_and_panic_keep_outer_authority_and_report_terminal_failure() {
    for mode in [1, 2] {
        let (app, state) = application(HttpProtocol::Http1_1).await;
        state.cleanup_mode.store(mode, Ordering::Release);
        let root = start(&app).await;
        let (sender, client) = http1(&app).await;
        let response = send(sender, "ignore");
        state.until("blocked", 1).await;
        let closing = close(&app, true).await;
        let report = finished(&app, root, closing, false).await;
        let _ = bounded(response).await.unwrap();
        let _ = bounded(client).await.unwrap();
        terminal(report);
        assert_eq!(report.completion, HttpShutdownCompletion::TerminalFailed);
        assert_eq!(state.ids("termination"), [2, 1, 0]);
        assert_eq!(report.requests.middleware.completed, 2);
        assert_eq!(report.requests.middleware.timed_out, usize::from(mode == 1));
        assert_eq!(report.requests.middleware.panicked, usize::from(mode == 2));
        assert_eq!(report.requests.cleanup_failed, 1);
        state.before("termination-returned", "scope-disposing");
    }
}

#[tokio::test]
async fn managed_finite_stream_keeps_scope_after_headers_then_drains_normally() {
    let (app, state) = application(HttpProtocol::Http1_1).await;
    let root = start(&app).await;
    let (mut sender, client) = http1(&app).await;
    let response = bounded(sender.send_request(request("stream")))
        .await
        .unwrap();
    let mut body = response.into_body();
    assert_eq!(
        bounded(body.frame())
            .await
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap(),
        "first"
    );
    state.until("body-pending", 1).await;
    let closing = close(&app, false).await;
    let live = app.request_registry().attempt_snapshot();
    assert_eq!(live.execution.returned, 1);
    assert_eq!(live.response.streaming_outstanding, 1);
    assert_eq!(live.scopes.outstanding, 1);
    state.release.cancel();
    assert!(bounded(body.collect()).await.unwrap().to_bytes().is_empty());
    drop(sender);
    bounded(client).await.unwrap().unwrap();
    let report = finished(&app, root, closing, true).await;
    terminal(report);
    assert_eq!(report.completion, HttpShutdownCompletion::GracefulCompleted);
    assert_eq!(report.requests.response.completed, 1);
    state.before("source-released", "scope-disposing");
}

#[tokio::test]
async fn managed_force_truncates_stream_and_sse_without_replaying_normal_middleware() {
    for stage in ["stream", "sse"] {
        let (app, state) = application(HttpProtocol::Http1_1).await;
        let root = start(&app).await;
        let (mut sender, client) = http1(&app).await;
        let response = bounded(sender.send_request(request(stage))).await.unwrap();
        assert_eq!(response.status(), 200);
        let mut body = response.into_body();
        assert!(bounded(body.frame()).await.unwrap().unwrap().is_data());
        state.until("body-pending", 1).await;
        let closing = close(&app, true).await;
        assert!(
            bounded(body.collect()).await.is_err(),
            "the client sees truncation, not a replacement response"
        );
        drop(sender);
        let _ = bounded(client).await.unwrap();
        let report = finished(&app, root, closing, true).await;
        terminal(report);
        assert_eq!(report.completion, HttpShutdownCompletion::ForcedCompleted);
        assert_eq!(report.requests.execution.returned, 1);
        assert_eq!(report.requests.response.head_handed_off, 1);
        assert_eq!(report.requests.response.interrupted, 1);
        assert_eq!(report.requests.response.completed, 0);
        assert_eq!(report.requests.response.streaming_outstanding, 0);
        assert!(state.ids("termination").is_empty());
        state.before("source-released", "scope-disposing");
    }
}

#[tokio::test]
async fn managed_http2_concurrent_actions_drain_and_late_stream_cannot_enter() {
    let (app, state) = application(HttpProtocol::Http2).await;
    let root = start(&app).await;
    let socket = tokio::net::TcpStream::connect(app.bound_address().unwrap())
        .await
        .unwrap();
    let (sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(socket))
            .await
            .unwrap();
    let client = tokio::spawn(connection);
    let mut requests = Vec::new();
    for _ in 0..16 {
        let mut sender = sender.clone();
        requests.push(tokio::spawn(async move {
            sender
                .send_request(request("graceful"))
                .await
                .unwrap()
                .into_body()
                .collect()
                .await
                .unwrap()
        }));
    }
    state.until("blocked", 16).await;
    let closing = close(&app, false).await;
    let admitted = app.request_registry().attempt_snapshot().admitted;
    let mut late = sender.clone();
    if let Ok(response) = bounded(late.send_request(request("ignore"))).await {
        assert_eq!(response.status(), 503);
        assert!(!response.headers().contains_key("connection"));
        bounded(response.into_body().collect()).await.unwrap();
    }
    assert_eq!(app.request_registry().attempt_snapshot().admitted, admitted);
    assert_eq!(admitted, 16);
    state.release.cancel();
    for request in requests {
        assert_eq!(bounded(request).await.unwrap().to_bytes(), "qualified");
    }
    drop(late);
    drop(sender);
    bounded(client).await.unwrap().unwrap();
    let report = finished(&app, root, closing, true).await;
    terminal(report);
    assert_eq!(report.requests.scopes.succeeded, 16);
    assert_eq!(report.requests.middleware.normal_returned, 48);
    assert!(
        report.protocol.registered >= 16,
        "actual Hyper executor receipts are required"
    );
    assert_eq!(report.connections.registered, 1);
    assert!(state.ids("termination").is_empty());
}

#[tokio::test]
async fn managed_pending_input_and_input_moved_into_output_release_before_scope() {
    for stage in ["input", "echo"] {
        let (app, state) = application(HttpProtocol::Http1_1).await;
        let root = start(&app).await;
        let mut socket = tokio::net::TcpStream::connect(app.bound_address().unwrap())
            .await
            .unwrap();
        socket.write_all(format!("GET /qualification/{stage} HTTP/1.1\r\nHost: lily.test\r\nContent-Length: 20\r\n\r\n").as_bytes()).await.unwrap();
        state
            .until(
                if stage == "input" {
                    "blocked"
                } else {
                    "body-pending"
                },
                1,
            )
            .await;
        let closing = close(&app, true).await;
        let report = finished(&app, root, closing, true).await;
        drop(socket);
        terminal(report);
        assert_eq!(report.requests.resources.inputs_outstanding, 0);
        assert_eq!(report.requests.scopes.succeeded, 1);
        if stage == "echo" {
            state.before("source-released", "scope-disposing");
        }
    }
}

#[tokio::test]
async fn managed_pending_scope_disposer_times_out_with_confirmed_release_and_terminal_failure() {
    let (app, state) = application(HttpProtocol::Http1_1).await;
    state.disposal_mode.store(1, Ordering::Release);
    let root = start(&app).await;
    let (sender, client) = http1(&app).await;
    let response = send(sender, "graceful");
    state.until("blocked", 1).await;
    let closing = close(&app, false).await;
    state.release.cancel();
    state.until("scope-disposing", 1).await;
    assert!(state.ids("application-disposed").is_empty());
    let report = finished(&app, root, closing, false).await;
    // The DI owner itself enforces U and destroys the still-pending disposer.
    // Its exact receipt proves termination, but cannot invent completion.
    assert_eq!(report.completion, HttpShutdownCompletion::TerminalFailed);
    assert!(report.terminal());
    assert_eq!(report.requests.scopes.outstanding, 0);
    assert_eq!(report.requests.scopes.timed_out, 1);
    assert_eq!(report.requests.scopes.succeeded, 0);
    assert!(state.ids("scope-disposed").is_empty());
    state.before("disposer-released", "application-disposed");
    state.disposer_release.cancel();
    assert!(app.close().await.is_err());
    assert_eq!(app.shutdown_report().unwrap(), report);
    let _ = bounded(response).await.unwrap();
    let _ = bounded(client).await.unwrap();
}

#[tokio::test]
async fn managed_caller_container_tracks_only_http_scopes_and_leaves_unrelated_scope_open() {
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let mut unrelated = container.create_scope(ProcessContext::new()).unwrap();
    let (app, state) =
        application_with_container(HttpProtocol::Http1_1, Some(container.clone())).await;
    let root = start(&app).await;
    let (sender, client) = http1(&app).await;
    let response = send(sender, "graceful");
    state.until("blocked", 1).await;
    let closing = close(&app, false).await;
    state.release.cancel();
    bounded(response).await.unwrap().unwrap();
    bounded(client).await.unwrap().unwrap();
    let report = finished(&app, root, closing, true).await;
    terminal(report);
    assert_eq!(report.requests.scopes.created, 1);
    assert_eq!(
        report.dependencies.di.disposition,
        crate::shutdown_report::DependencyDisposition::NotOwned
    );
    assert_eq!(container.active_scope_count(), 1);
    assert!(!lily_injection::__private::container_shutdown_started(
        &container
    ));
    assert!(state.ids("application-disposed").is_empty());
    unrelated.close().await.unwrap();
    container.close().await.unwrap();
    assert_eq!(state.ids("application-disposed"), [OTHER]);
}

#[tokio::test]
async fn managed_static_file_http2_drain_reconciles_real_open_and_read_helpers() {
    let directory = tempfile::tempdir().unwrap();
    let contents = vec![b'x'; 1024 * 1024];
    std::fs::write(directory.path().join("file"), &contents).unwrap();
    let (app, state) = application(HttpProtocol::Http2).await;
    *state.file.lock().unwrap() = Some(directory.path().to_owned());
    let root = start(&app).await;
    let socket = tokio::net::TcpStream::connect(app.bound_address().unwrap())
        .await
        .unwrap();
    // Keep the file source live at admission closure using actual HTTP/2 flow
    // control, rather than a sleep or assumptions about kernel send buffers.
    let (mut sender, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .initial_stream_window_size(4096)
        .initial_connection_window_size(4096)
        .handshake(TokioIo::new(socket))
        .await
        .unwrap();
    let client = tokio::spawn(connection);
    let response = bounded(sender.send_request(request("file"))).await.unwrap();
    assert_eq!(response.status(), 200);
    let closing = close(&app, false).await;
    assert_eq!(
        app.request_registry()
            .attempt_snapshot()
            .response
            .streaming_outstanding,
        1
    );
    assert_eq!(
        bounded(response.into_body().collect())
            .await
            .unwrap()
            .to_bytes(),
        contents
    );
    drop(sender);
    bounded(client).await.unwrap().unwrap();
    let report = finished(&app, root, closing, true).await;
    terminal(report);
    assert_eq!(report.completion, HttpShutdownCompletion::GracefulCompleted);
    assert!(report.requests.resources.helpers_registered >= 3);
    assert_eq!(
        report.requests.resources.helpers_registered,
        report.requests.resources.helpers_joined
    );
    assert_eq!(report.requests.resources.helpers_failed, 0);
    assert_eq!(report.requests.response.completed, 1);
    state.before("scope-disposed", "application-disposed");
}

#[tokio::test]
async fn managed_peer_disconnect_racing_shutdown_finalizes_each_entered_frame_once() {
    let (app, state) = application(HttpProtocol::Http1_1).await;
    let root = start(&app).await;
    let mut socket = tokio::net::TcpStream::connect(app.bound_address().unwrap())
        .await
        .unwrap();
    socket
        .write_all(b"GET /qualification/ignore HTTP/1.1\r\nHost: lily.test\r\n\r\n")
        .await
        .unwrap();
    state.until("blocked", 1).await;
    let closing = close(&app, false).await;
    drop(socket);
    let report = finished(&app, root, closing, true).await;
    terminal(report);
    assert_eq!(report.requests.registered, 1);
    assert_eq!(report.requests.scopes.succeeded, 1);
    assert_eq!(state.ids("termination"), [2, 1, 0]);
    assert_eq!(report.requests.cancellation_requested, 1);
    state.before("execution-released", "termination");
    state.before("scope-disposed", "application-disposed");
}
