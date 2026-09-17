use super::response_control_tests::{FlushGate, GatedIo};
use super::*;
use crate::{AppBuilder, ExecutionCancellation};
use futures::{Stream, StreamExt};
use http_body_util::Full;
use lily_web_core::{streaming, IntoResponse};
use tokio::time::Instant as Clock;

#[derive(Default, lily_injection::Injectable)]
#[service(lifetime = "Singleton")]
struct Probe {
    entered: Notify,
    signal: Mutex<Option<(Clock, ExecutionCancellation)>>,
    sources_dropped: AtomicUsize,
}
impl lily_injection::ServiceTrait for Probe {}

struct Pipeline(Arc<Probe>);

struct Source {
    stream: Pin<Box<dyn Stream<Item = Result<Bytes, ResponseBodyError>> + Send>>,
    probe: Arc<Probe>,
}
impl Stream for Source {
    type Item = Result<Bytes, ResponseBodyError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.stream.as_mut().poll_next(cx)
    }
}
impl Drop for Source {
    fn drop(&mut self) {
        self.probe.sources_dropped.fetch_add(1, Ordering::AcqRel);
    }
}

#[async_trait::async_trait]
impl crate::HttpMiddleware for Pipeline {
    async fn new(
        extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, crate::HttpMiddlewareInitError> {
        Ok(Self(extensions.get_service::<Probe>(None).await.unwrap()))
    }
    fn descriptor(&self) -> crate::MiddlewareDescriptor {
        crate::MiddlewareDescriptor::new("shared-response-deadline", crate::MiddlewareKind::Custom)
    }
    async fn handle(
        &self,
        exchange: &mut crate::HttpExchange<'_>,
        _: crate::HttpNext<'_>,
        cancellation: ExecutionCancellation,
    ) -> Result<(), crate::HttpMiddlewareError> {
        let path = exchange.request().path().to_owned();
        *self.0.signal.lock().unwrap() = Some((Clock::now(), cancellation.clone()));
        self.0.entered.notify_one();
        if path == "/ignore" {
            std::future::pending::<()>().await;
        }
        if matches!(path.as_str(), "/cooperative-pipeline" | "/spent-window") {
            cancellation.cancelled().await;
            tokio::time::sleep(Duration::from_millis(if path == "/spent-window" {
                140
            } else {
                40
            }))
            .await;
        } else if path == "/buffered" || path == "/stream" {
            tokio::time::sleep(Duration::from_millis(60)).await;
        }
        if matches!(
            path.as_str(),
            "/cooperative-pipeline" | "/spent-window" | "/stream" | "/pending" | "/sse"
        ) {
            let ignore = matches!(path.as_str(), "/pending" | "/sse");
            let delay = if path == "/spent-window" { 140 } else { 40 };
            let source = Source {
                stream: Box::pin(futures::stream::once(async move {
                    if ignore {
                        std::future::pending::<()>().await;
                    }
                    cancellation.cancelled().await;
                    tokio::time::sleep(Duration::from_millis(delay)).await;
                    Ok(Bytes::from_static(b"original result"))
                })),
                probe: self.0.clone(),
            };
            let (request, response) = exchange.parts_mut();
            if path == "/sse" {
                crate::SseResponse::new(
                    source.map(|item| item.map(|_| crate::SseEvent::new("event").unwrap())),
                )
                .write_to_response(response, request)
                .await
                .unwrap();
            } else {
                streaming(source)
                    .write_to_response(response, request)
                    .await
                    .unwrap();
            }
        } else if path == "/buffered" {
            exchange
                .response_mut()
                .write_body(&vec![7; 512 * 1024])
                .unwrap();
        } else {
            exchange.response_mut().write_body(b"healthy").unwrap();
        }
        Ok(())
    }
}

enum Sender {
    Http1(hyper::client::conn::http1::SendRequest<Full<Bytes>>),
    Http2(hyper::client::conn::http2::SendRequest<Full<Bytes>>),
}
impl Sender {
    async fn send(&mut self, path: &str) -> Result<HyperResponse<Incoming>, hyper::Error> {
        let request = HyperRequest::builder()
            .uri(format!("http://lily.test{path}"))
            .body(Full::new(Bytes::new()))
            .unwrap();
        match self {
            Self::Http1(sender) => sender.send_request(request).await,
            Self::Http2(sender) => sender.send_request(request).await,
        }
    }
}
struct Pair {
    app: Arc<App>,
    probe: Arc<Probe>,
    sender: Sender,
    client: tokio::task::JoinHandle<Result<(), hyper::Error>>,
    driver: TaskReceipt<io::Result<ConnectionCloseReason>>,
    stop: CancellationToken,
    flush: Arc<FlushGate>,
}

async fn pair(protocol: HttpProtocol, window: u32) -> Pair {
    let transport = HttpTransportConfig {
        request_timeout: Duration::from_millis(100),
        ..Default::default()
    };
    let app = Arc::new(
        AppBuilder::new("127.0.0.1:0")
            .protocol(protocol)
            .middleware::<Pipeline>()
            .build()
            .await
            .unwrap(),
    );
    let probe = app.container().resolve::<Probe>(None).await.unwrap();
    let (client_io, server_io) = tokio::io::duplex(64 * 1024);
    let flush = Arc::new(FlushGate::default());
    let io = GatedIo {
        inner: server_io,
        gate: flush.clone(),
    };
    let stop = CancellationToken::new();
    let stopped = stop.clone();
    let runtime = HttpConnectionRuntime {
        service: app.clone(),
        transport,
        request_admission: Arc::new(Semaphore::new(8)),
        telemetry: HttpServerTelemetry::new(),
        cors_services: None,
    };
    let driver = app
        .task_inventory()
        .connections
        .try_spawn_with_receipt(
            move |receipt| {
                CONNECTION_TASK.scope(
                    receipt,
                    HttpServer::serve_connection(
                        io,
                        "127.0.0.1:12345".parse().unwrap(),
                        runtime,
                        stopped,
                    ),
                )
            },
            None,
        )
        .unwrap();
    let (sender, client) = if protocol == HttpProtocol::Http2 {
        let (sender, client) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .initial_stream_window_size(window)
            .initial_connection_window_size(1024 * 1024)
            .handshake(TokioIo::new(client_io))
            .await
            .unwrap();
        (Sender::Http2(sender), tokio::spawn(client))
    } else {
        let (sender, client) = hyper::client::conn::http1::handshake(TokioIo::new(client_io))
            .await
            .unwrap();
        (Sender::Http1(sender), tokio::spawn(client))
    };
    Pair {
        app,
        probe,
        sender,
        client,
        driver,
        stop,
        flush,
    }
}

async fn close(pair: Pair) {
    pair.flush.release();
    pair.stop.cancel();
    drop(pair.sender);
    let _ = tokio::time::timeout(Duration::from_secs(5), pair.client)
        .await
        .unwrap()
        .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), pair.driver)
        .await
        .unwrap()
        .unwrap();
    assert!(pair.app.request_registry().wait().await.is_terminal());
    assert_eq!(pair.app.container().active_scope_count(), 0);
    pair.app.close().await.unwrap();
    assert!(pair.app.task_inventory().transport_is_terminal());
}

fn signal(probe: &Probe) -> (Clock, ExecutionCancellation) {
    probe
        .signal
        .lock()
        .unwrap()
        .clone()
        .expect("pipeline entered")
}

#[tokio::test(start_paused = true)]
async fn a_cooperative_pipeline_can_start_and_finish_its_returned_stream_in_the_same_window() {
    for protocol in [HttpProtocol::Http1_1, HttpProtocol::Http2] {
        for force in [false, true] {
            let mut pair = pair(protocol, 64 * 1024).await;
            let app = pair.app.clone();
            let probe = pair.probe.clone();
            let reply = async { pair.sender.send("/cooperative-pipeline").await.unwrap() };
            let control = async {
                probe.entered.notified().await;
                if force {
                    app.shutdown_budget().begin();
                    app.request_registry()
                        .cancel_executions(ExecutionStopReason::ForcedShutdown);
                }
            };
            let (response, ()) = tokio::join!(reply, control);
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                "original result"
            );
            assert!(signal(&pair.probe).1.is_cancelled());
            assert_eq!(pair.probe.sources_dropped.load(Ordering::Acquire), 1);
            assert_eq!(
                pair.app
                    .task_inventory()
                    .protocol
                    .snapshot()
                    .abort_requested,
                0
            );
            close(pair).await;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn lazy_production_uses_the_spent_handler_budget_and_keeps_its_cooperative_result() {
    for protocol in [HttpProtocol::Http1_1, HttpProtocol::Http2] {
        let mut pair = pair(protocol, 64 * 1024).await;
        let response = pair.sender.send("/stream").await.unwrap();
        let (started, cancellation) = signal(&pair.probe);
        assert_eq!(Clock::now(), started + Duration::from_millis(60));
        let body = response.into_body().collect();
        let observe = async {
            cancellation.cancelled().await;
            assert_eq!(Clock::now(), started + Duration::from_millis(100));
        };
        let (body, ()) = tokio::join!(body, observe);
        assert_eq!(body.unwrap().to_bytes(), "original result");
        assert_eq!(Clock::now(), started + Duration::from_millis(140));
        close(pair).await;
    }
}

#[tokio::test(start_paused = true)]
async fn incomplete_committed_stream_and_sse_are_truncated_with_protocol_local_scope() {
    for protocol in [HttpProtocol::Http1_1, HttpProtocol::Http2] {
        for path in ["/pending", "/sse"] {
            let mut pair = pair(protocol, 64 * 1024).await;
            let response = pair.sender.send(path).await.unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            if path == "/sse" {
                assert_eq!(
                    response.headers()[http::header::CONTENT_TYPE],
                    "text/event-stream"
                );
            }
            let (started, cancellation) = signal(&pair.probe);
            assert!(response.into_body().collect().await.is_err());
            assert!(cancellation.is_cancelled());
            assert_eq!(Clock::now(), started + Duration::from_millis(350));
            assert!(pair.app.request_registry().wait().await.is_terminal());
            assert_eq!(pair.probe.sources_dropped.load(Ordering::Acquire), 1);
            let later = pair.sender.send("/healthy").await;
            if protocol == HttpProtocol::Http2 {
                assert_eq!(
                    later
                        .unwrap()
                        .into_body()
                        .collect()
                        .await
                        .unwrap()
                        .to_bytes(),
                    "healthy"
                );
            } else {
                assert!(later.is_err());
            }
            close(pair).await;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn buffered_response_clock_survives_owner_retirement_and_zero_h2_capacity() {
    let mut pair = pair(HttpProtocol::Http2, 0).await;
    let response = pair.sender.send("/buffered").await.unwrap();
    let (started, cancellation) = signal(&pair.probe);
    assert!(pair.app.request_registry().wait().await.is_terminal());
    assert_eq!(pair.app.container().active_scope_count(), 0);
    cancellation.cancelled().await;
    assert_eq!(Clock::now(), started + Duration::from_millis(100));
    assert_eq!(
        pair.app
            .task_inventory()
            .protocol
            .snapshot()
            .abort_requested,
        0
    );
    let tasks = pair.app.task_inventory().protocol.wait().await;
    assert_eq!(Clock::now(), started + Duration::from_millis(350));
    assert_eq!(tasks.cancelled, 1);
    assert!(response.into_body().collect().await.is_err());
    close(pair).await;
}

#[tokio::test(start_paused = true)]
async fn buffered_response_flush_can_finish_after_signal_without_losing_success() {
    for protocol in [HttpProtocol::Http1_1, HttpProtocol::Http2] {
        let mut pair = pair(protocol, 1024 * 1024).await;
        // Complete protocol setup before blocking the response's flush. A gate on
        // the initial HTTP/2 handshake would prevent request admission altogether.
        pair.sender
            .send("/healthy")
            .await
            .unwrap()
            .into_body()
            .collect()
            .await
            .unwrap();
        pair.flush.block();
        let response = pair.sender.send("/buffered").await.unwrap();
        let (started, cancellation) = signal(&pair.probe);
        let collect = response.into_body().collect();
        let release = async {
            cancellation.cancelled().await;
            assert_eq!(Clock::now(), started + Duration::from_millis(100));
            tokio::time::sleep(Duration::from_millis(20)).await;
            pair.flush.release();
        };
        let (body, ()) = tokio::join!(collect, release);
        assert_eq!(body.unwrap().to_bytes().len(), 512 * 1024);
        assert_eq!(
            pair.app
                .task_inventory()
                .protocol
                .snapshot()
                .abort_requested,
            0
        );
        tokio::time::sleep(Duration::from_millis(400)).await;
        let healthy = pair.sender.send("/healthy").await.unwrap();
        assert_eq!(
            healthy.into_body().collect().await.unwrap().to_bytes(),
            "healthy"
        );
        close(pair).await;
    }
}

#[tokio::test(start_paused = true)]
async fn body_cannot_restart_the_window_already_spent_in_dispatch() {
    for protocol in [HttpProtocol::Http1_1, HttpProtocol::Http2] {
        let mut pair = pair(protocol, 64 * 1024).await;
        let response = pair.sender.send("/spent-window").await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let (started, _) = signal(&pair.probe);
        assert_eq!(Clock::now(), started + Duration::from_millis(240));
        assert!(response.into_body().collect().await.is_err());
        assert_eq!(Clock::now(), started + Duration::from_millis(350));
        assert!(pair.app.request_registry().wait().await.is_terminal());
        assert_eq!(pair.probe.sources_dropped.load(Ordering::Acquire), 1);
        close(pair).await;
    }
}

#[tokio::test(start_paused = true)]
async fn uncommitted_fallback_is_clipped_to_root_and_reselection_cannot_renew_it() {
    use crate::request_lifecycle::deadline::RequestDeadline;
    use futures::FutureExt;
    let started = Clock::now();
    let root = ShutdownBudget::from_started(Duration::from_millis(100), started);
    let clock = RequestDeadline::new(root.clone(), CancellationToken::new());
    clock.start(Duration::from_secs(10));
    let control = ResponseTransportControl::bind(Version::HTTP_11, ConnectionReceipt::current());
    control.bind_deadline(clock.clone());
    let watch = control.enforce_deadline();
    tokio::pin!(watch);
    assert!(watch.as_mut().now_or_never().is_none());
    tokio::time::advance(Duration::from_millis(60)).await;
    assert!(watch.as_mut().now_or_never().is_none());
    assert_eq!(clock.reason(), Some(ExecutionStopReason::GracefulDeadline));
    tokio::time::advance(Duration::from_millis(10)).await;
    assert!(watch.as_mut().now_or_never().is_none());
    assert!(matches!(
        control.commit(),
        Err(ResponseStopped::FallbackRequired(
            StatusCode::SERVICE_UNAVAILABLE
        ))
    ));
    tokio::time::advance(Duration::from_millis(10)).await;
    control.begin_finalization();
    control.commit_fallback().unwrap();
    assert!(watch.as_mut().now_or_never().is_none());
    // Finalization ends before R, leaving the original root's join reserve.
    tokio::time::advance(Duration::from_millis(8)).await;
    watch.await;
    assert_eq!(
        Clock::now(),
        root.deadlines().unwrap().at(ShutdownStage::TransportStop)
    );
    assert_eq!(
        control.snapshot().stop_requested,
        Some(ExecutionStopReason::GracefulDeadline)
    );
    assert!(control.commit_fallback().is_err());
    assert!(
        !control.snapshot().is_terminal(),
        "stop is not driver-release evidence"
    );
}

#[tokio::test(start_paused = true)]
async fn a_later_shutdown_root_shortens_the_same_pending_response_window() {
    use crate::request_lifecycle::deadline::RequestDeadline;
    use futures::FutureExt;
    let started = Clock::now();
    let root = ShutdownBudget::new(Duration::from_millis(100));
    let clock = RequestDeadline::new(root.clone(), CancellationToken::new());
    clock.start(Duration::from_millis(100));
    let control = ResponseTransportControl::bind(Version::HTTP_11, ConnectionReceipt::current());
    control.bind_deadline(clock.clone());
    control.commit().unwrap();
    let watch = control.enforce_deadline();
    tokio::pin!(watch);
    assert!(watch.as_mut().now_or_never().is_none());
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(watch.as_mut().now_or_never().is_none());
    assert_eq!(clock.reason(), Some(ExecutionStopReason::RequestTimeout));
    tokio::time::advance(Duration::from_millis(50)).await;
    let cutoffs = root.begin();
    clock.cancel(ExecutionStopReason::ForcedShutdown);
    assert!(watch.as_mut().now_or_never().is_none());
    tokio::time::advance(Duration::from_millis(70)).await;
    watch.await;
    assert_eq!(Clock::now(), cutoffs.at(ShutdownStage::Cooperative));
    assert_eq!(Clock::now(), started + Duration::from_millis(220));
    assert_eq!(
        control.snapshot().stop_requested,
        Some(ExecutionStopReason::RequestTimeout)
    );
}

#[tokio::test(start_paused = true)]
async fn incomplete_uncommitted_request_gets_504_or_shutdown_503_and_can_finish_writing() {
    for protocol in [HttpProtocol::Http1_1, HttpProtocol::Http2] {
        for force in [false, true] {
            let mut pair = pair(protocol, 64 * 1024).await;
            let app = pair.app.clone();
            let probe = pair.probe.clone();
            let reply = async { pair.sender.send("/ignore").await.unwrap() };
            let control = async {
                probe.entered.notified().await;
                if force {
                    app.shutdown_budget().begin();
                    app.request_registry()
                        .cancel_executions(ExecutionStopReason::ForcedShutdown);
                }
            };
            let (response, ()) = tokio::join!(reply, control);
            assert_eq!(
                response.status(),
                if force {
                    StatusCode::SERVICE_UNAVAILABLE
                } else {
                    StatusCode::GATEWAY_TIMEOUT
                }
            );
            assert!(!response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .is_empty());
            assert!(pair.app.request_registry().wait().await.is_terminal());
            if !force {
                let response = pair.sender.send("/healthy").await.unwrap();
                assert_eq!(
                    response.into_body().collect().await.unwrap().to_bytes(),
                    "healthy"
                );
            }
            close(pair).await;
        }
    }
}

#[tokio::test(start_paused = true)]
async fn late_uncommitted_selection_keeps_resolved_cors_and_replaces_the_original_response() {
    use crate::request_lifecycle::deadline::RequestDeadline;
    let clock = RequestDeadline::new(
        ShutdownBudget::new(Duration::from_secs(5)),
        CancellationToken::new(),
    );
    clock.start(Duration::from_millis(100));
    let activity = ConnectionActivity::new();
    let mut guard = activity.enter();
    let control = guard.bind_transport(Version::HTTP_11);
    control.bind_deadline(clock.clone());
    // This response is handed off, but its service future has not selected it
    // for Hyper yet. The encoder must not commit it after the shared cutoff.
    let response = HyperResponse::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/json")
        .header(http::header::CONTENT_LENGTH, "8")
        .header("x-application-result", "original")
        .header(
            http::header::ACCESS_CONTROL_ALLOW_ORIGIN,
            "https://example.test",
        )
        .header(http::header::VARY, "Origin")
        .body(BoundedResponseBody::new(
            Bytes::from_static(b"original"),
            1024,
            guard,
        ))
        .unwrap();
    tokio::time::advance(Duration::from_millis(100)).await;
    clock.refresh();
    tokio::time::advance(Duration::from_millis(250)).await;
    let response = HttpServer::commit_response(response, &control).unwrap();
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(response.headers().len(), 2);
    assert_eq!(response.headers()[http::header::VARY], "Origin");
    assert_eq!(
        response.headers()[http::header::ACCESS_CONTROL_ALLOW_ORIGIN],
        "https://example.test"
    );
    assert!(response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .is_empty());
    assert_eq!(
        control.snapshot().commit,
        super::super::response_control::ResponseCommit::CommittedToProtocol
    );
    assert!(
        !control.snapshot().is_terminal(),
        "commit and EOF are not I/O flush"
    );
}

#[tokio::test(start_paused = true)]
async fn fallback_body_also_has_one_bounded_finalization_attempt() {
    let mut pair = pair(HttpProtocol::Http2, 0).await;
    let response = pair.sender.send("/ignore").await.unwrap();
    let (started, _) = signal(&pair.probe);
    assert_eq!(response.status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(Clock::now(), started + Duration::from_millis(350));
    let tasks = pair.app.task_inventory().protocol.wait().await;
    assert_eq!(tasks.cancelled, 1);
    assert_eq!(Clock::now(), started + Duration::from_millis(450));
    assert!(response.into_body().collect().await.is_err());
    close(pair).await;
}
