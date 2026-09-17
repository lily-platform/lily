//! Real managed request/DI owners and Hyper codecs, driven through the same
//! listener drain function as production. Gates hold actual I/O or user work;
//! no counter or task-completion evidence is injected.
use super::response_control_tests::{FlushGate, GatedIo};
use super::*;
use crate::{AppBuilder, ExecutionCancellation};
use futures::Stream;
use http_body_util::Full;
use lily_injection::{Extensions, InjectionError, ServiceTrait};
use lily_web_core::{streaming, IntoResponse};
use std::sync::atomic::AtomicBool;
use tokio::time::Instant as Clock;

#[derive(Default, lily_injection::Injectable)]
#[service(lifetime = "Singleton")]
struct Probe {
    tasks: std::sync::OnceLock<crate::tasks::HttpTaskInventory>,
    signal: Mutex<Option<ExecutionCancellation>>,
    entered: Notify,
    events: Mutex<Vec<&'static str>>,
    scopes: AtomicUsize,
    sources: AtomicUsize,
    disposed: AtomicUsize,
    hold_disposal: AtomicBool,
    disposing: Notify,
    release_disposal: CancellationToken,
    release_source: CancellationToken,
}
impl Probe {
    fn record(&self, event: &'static str) {
        self.events.lock().unwrap().push(event);
    }
}
#[async_trait::async_trait]
impl ServiceTrait for Probe {
    async fn dispose(&self) -> Result<(), InjectionError> {
        // Injectable registrations are process-wide and singleton construction
        // is eager, including unrelated tests' containers. Only this harness
        // arms the transport probe; every harness also asserts disposed == 1.
        let Some(tasks) = self.tasks.get() else {
            return Ok(());
        };
        assert_eq!(self.scopes.load(Ordering::Acquire), 0);
        assert_eq!(self.sources.load(Ordering::Acquire), 0);
        assert!(tasks.transport_is_terminal());
        self.record("application-disposed");
        self.disposed.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

#[derive(Default, lily_injection::Injectable)]
#[service(lifetime = "Scoped")]
struct Scope {
    #[inject]
    probe: Arc<Probe>,
    source_live: AtomicBool,
}
#[async_trait::async_trait]
impl ServiceTrait for Scope {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.probe.scopes.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
    async fn dispose(&self) -> Result<(), InjectionError> {
        assert!(!self.source_live.load(Ordering::Acquire));
        self.probe.record("scope-disposing");
        self.probe.disposing.notify_one();
        if self.probe.hold_disposal.load(Ordering::Acquire) {
            self.probe.release_disposal.cancelled().await;
        }
        self.probe.scopes.fetch_sub(1, Ordering::AcqRel);
        self.probe.record("scope-disposed");
        Ok(())
    }
}

struct Pipeline(Arc<Extensions>, Arc<Probe>);
#[async_trait::async_trait]
impl crate::HttpMiddleware for Pipeline {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, crate::HttpMiddlewareInitError> {
        let probe = extensions.get_service::<Probe>(None).await?;
        Ok(Self(extensions, probe))
    }
    fn descriptor(&self) -> crate::MiddlewareDescriptor {
        crate::MiddlewareDescriptor::new(
            "shutdown-response-qualification",
            crate::MiddlewareKind::Custom,
        )
    }
    async fn handle(
        &self,
        exchange: &mut crate::HttpExchange<'_>,
        _: crate::HttpNext<'_>,
        cancellation: ExecutionCancellation,
    ) -> Result<(), crate::HttpMiddlewareError> {
        let scope = self.0.get_service::<Scope>(None).await.unwrap();
        *self.1.signal.lock().unwrap() = Some(cancellation.clone());
        self.1.entered.notify_one();
        match exchange.request().path() {
            "/ignore" => {
                cancellation.cancelled().await;
                self.1.record("execution-notified");
                std::future::pending::<()>().await;
            }
            "/stream" | "/finite" => {
                let finite = exchange.request().path() == "/finite";
                let release = self.1.release_source.clone();
                self.1.sources.fetch_add(1, Ordering::AcqRel);
                scope.source_live.store(true, Ordering::Release);
                let source = Source {
                    inner: Box::pin(futures::stream::once(async move {
                        if finite {
                            release.cancelled().await;
                        } else {
                            cancellation.cancelled().await;
                            tokio::time::sleep(Duration::from_millis(40)).await;
                        }
                        Ok(Bytes::from_static(b"cooperative source"))
                    })),
                    probe: self.1.clone(),
                    scope,
                };
                let (request, response) = exchange.parts_mut();
                streaming(source)
                    .write_to_response(response, request)
                    .await
                    .unwrap();
            }
            "/buffered" => {
                exchange
                    .response_mut()
                    .write_body(&vec![7; 512 * 1024])
                    .unwrap();
            }
            _ => {
                exchange.response_mut().write_body(b"ready").unwrap();
            }
        }
        self.1.record("normal-return");
        Ok(())
    }
    async fn on_request_termination(
        &self,
        _: &mut crate::HttpRequestTerminationContext<'_>,
        cancellation: crate::CleanupCancellation,
    ) -> Result<(), crate::HttpMiddlewareError> {
        assert!(!cancellation.is_cancelled());
        self.1.record("termination");
        Ok(())
    }
}
struct Source {
    inner: Pin<Box<dyn Stream<Item = Result<Bytes, ResponseBodyError>> + Send>>,
    probe: Arc<Probe>,
    scope: Arc<Scope>,
}
impl Stream for Source {
    type Item = Result<Bytes, ResponseBodyError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}
impl Drop for Source {
    fn drop(&mut self) {
        self.scope.source_live.store(false, Ordering::Release);
        self.probe.sources.fetch_sub(1, Ordering::AcqRel);
        self.probe.record("source-released");
    }
}

enum Sender {
    Http1(hyper::client::conn::http1::SendRequest<Full<Bytes>>),
    Http2(hyper::client::conn::http2::SendRequest<Full<Bytes>>),
}
impl Sender {
    async fn send(&mut self, path: &str) -> HyperResponse<Incoming> {
        let request = HyperRequest::builder()
            .uri(format!("http://lily.test{path}"))
            .body(Full::new(Bytes::new()))
            .unwrap();
        match self {
            Self::Http1(sender) => sender.send_request(request).await.unwrap(),
            Self::Http2(sender) => sender.send_request(request).await.unwrap(),
        }
    }
}
struct Harness {
    app: Arc<App>,
    probe: Arc<Probe>,
    sender: Sender,
    client: tokio::task::JoinHandle<Result<(), hyper::Error>>,
    listener: TaskReceipt<ConnectionTaskReport>,
    gate: Arc<FlushGate>,
    others: Vec<Peer>,
    drivers: Vec<TaskReceipt<io::Result<ConnectionCloseReason>>>,
}
struct Peer {
    sender: Sender,
    client: tokio::task::JoinHandle<Result<(), hyper::Error>>,
    gate: Arc<FlushGate>,
}

async fn harness(protocol: HttpProtocol, timeout: Duration) -> Harness {
    harness_with_peers(protocol, timeout, 1, 1024 * 1024).await
}

async fn harness_with_peers(
    protocol: HttpProtocol,
    timeout: Duration,
    count: usize,
    window: u32,
) -> Harness {
    let transport = HttpTransportConfig {
        request_timeout: timeout,
        ..Default::default()
    };
    let app = Arc::new(
        AppBuilder::new("127.0.0.1:0")
            .protocol(protocol)
            .transport_config(transport.clone())
            .tracing_disabled()
            .middleware::<Pipeline>()
            .build()
            .await
            .unwrap(),
    );
    let probe = app.container().resolve::<Probe>(None).await.unwrap();
    assert!(probe.tasks.set(app.task_inventory().clone()).is_ok());
    let mut clients = Vec::new();
    let mut servers = Vec::new();
    for _ in 0..count {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let gate = Arc::new(FlushGate::default());
        clients.push((client, gate.clone()));
        servers.push(GatedIo {
            inner: server,
            gate,
        });
    }
    let (publish, mut receipts) = tokio::sync::mpsc::unbounded_channel();
    let server = app.clone();
    let listener = app.task_inventory().listener.spawn(async move {
        let stop = server.transport_shutdown();
        let mut tasks = TaskSet::new(server.task_inventory().connections.clone());
        let admission = Arc::new(Semaphore::new(8));
        for server_io in servers {
            let connection_stop = stop.clone();
            let runtime = HttpConnectionRuntime {
                service: server.clone(),
                transport: transport.clone(),
                request_admission: admission.clone(),
                telemetry: HttpServerTelemetry::new(),
                cors_services: None,
            };
            let publish = publish.clone();
            tasks.spawn_with_receipt(move |receipt| {
                publish.send(receipt.clone()).unwrap();
                CONNECTION_TASK.scope(
                    receipt,
                    HttpServer::serve_connection(
                        server_io,
                        "127.0.0.1:12345".parse().unwrap(),
                        runtime,
                        connection_stop,
                    ),
                )
            });
        }
        stop.cancelled().await;
        server.shutdown_budget().begin();
        server.task_inventory().connections.seal();
        let mut report = ConnectionTaskReport {
            accepted: count,
            ..Default::default()
        };
        HttpServer::drain_connection_tasks_before(
            &mut tasks,
            server.shutdown_budget(),
            &server.transport_force(),
            &mut report,
            Some(server.request_registry()),
        )
        .await;
        report
    });
    let mut peers = Vec::new();
    for (client_io, gate) in clients {
        let (sender, client) = if protocol == HttpProtocol::Http2 {
            let (sender, connection) =
                hyper::client::conn::http2::Builder::new(TokioExecutor::new())
                    .initial_stream_window_size(window)
                    .initial_connection_window_size(1024 * 1024)
                    .handshake(TokioIo::new(client_io))
                    .await
                    .unwrap();
            (Sender::Http2(sender), tokio::spawn(connection))
        } else {
            let (sender, connection) =
                hyper::client::conn::http1::handshake(TokioIo::new(client_io))
                    .await
                    .unwrap();
            (Sender::Http1(sender), tokio::spawn(connection))
        };
        peers.push(Peer {
            sender,
            client,
            gate,
        });
    }
    let mut drivers = Vec::new();
    for _ in 0..count {
        drivers.push(receipts.recv().await.unwrap());
    }
    let Peer {
        sender,
        client,
        gate,
    } = peers.remove(0);
    Harness {
        app,
        probe,
        sender,
        client,
        listener,
        gate,
        others: peers,
        drivers,
    }
}

// Starting shutdown and observing its final join are separate test operations.
struct Closing {
    task: tokio::task::JoinHandle<io::Result<()>>,
}

async fn shutdown(app: &Arc<App>, force: bool) -> Closing {
    let observer = app.clone();
    let close = tokio::spawn(async move { observer.close().await });
    while !app.request_registry().admission_closed() {
        tokio::task::yield_now().await;
    }
    if force {
        app.transport_force().cancel();
    }
    Closing { task: close }
}

async fn finished(h: Harness, close: Closing) {
    close.task.await.unwrap().unwrap();
    let report = h.listener.await.unwrap();
    assert_eq!(
        report.forced, 0,
        "response drain must not use the connection abort fallback: {report:?}"
    );
    assert_eq!(report.cancelled, 0);
    assert_eq!(report.outstanding, 0);
    assert_eq!(report.accepted, h.drivers.len());
    assert_eq!(report.completed, h.drivers.len());
    for receipt in h.drivers {
        assert!(receipt.await.unwrap().is_ok());
    }
    assert_eq!(h.probe.disposed.load(Ordering::Acquire), 1);
    assert_eq!(h.probe.scopes.load(Ordering::Acquire), 0);
    assert!(h.app.task_inventory().transport_is_terminal());
    assert!(h.app.request_registry().snapshot().is_terminal());
    assert_eq!(
        h.app
            .task_inventory()
            .connections
            .snapshot()
            .abort_requested,
        0
    );
    drop(h.sender);
    h.client.await.unwrap().unwrap();
    for peer in h.others {
        drop(peer.sender);
        peer.client.await.unwrap().unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn forced_drain_preserves_buffered_tail_after_request_scope_retirement() {
    for protocol in [HttpProtocol::Http1_1, HttpProtocol::Http2] {
        let mut h = harness(protocol, Duration::from_secs(60)).await;
        assert_eq!(
            h.sender
                .send("/ready")
                .await
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes(),
            "ready"
        );
        h.gate.block();
        let response = h.sender.send("/buffered").await;
        assert!(h.app.request_registry().wait().await.is_terminal());
        assert_eq!(h.probe.scopes.load(Ordering::Acquire), 0);
        let signal = h.probe.signal.lock().unwrap().clone().unwrap();
        let started = Clock::now();
        let close = shutdown(&h.app, true).await;
        let release = async {
            signal.cancelled().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert_eq!(h.probe.disposed.load(Ordering::Acquire), 0);
            assert!(!lily_injection::__private::container_shutdown_started(
                h.app.container()
            ));
            assert_eq!(h.app.task_inventory().connections.snapshot().outstanding, 1);
            assert_eq!(
                h.app
                    .task_inventory()
                    .connections
                    .snapshot()
                    .abort_requested,
                0
            );
            h.gate.release();
        };
        let (body, ()) = tokio::join!(response.into_body().collect(), release);
        assert_eq!(body.unwrap().to_bytes(), Bytes::from(vec![7; 512 * 1024]));
        assert_eq!(Clock::now(), started + Duration::from_millis(20));
        finished(h, close).await;
    }
}

#[tokio::test(start_paused = true)]
async fn graceful_deadline_and_late_force_preserve_source_and_dependency_order() {
    for protocol in [HttpProtocol::Http1_1, HttpProtocol::Http2] {
        // 0: drain without cancellation; 1: G expires; 2: force arrives while
        // an already accepted response is streaming during graceful drain.
        for mode in 0..3 {
            let mut h = harness(protocol, Duration::from_secs(60)).await;
            let response = h
                .sender
                .send(if mode == 0 { "/finite" } else { "/stream" })
                .await;
            let signal = h.probe.signal.lock().unwrap().clone().unwrap();
            let started = Clock::now();
            let close = shutdown(&h.app, false).await;
            let root = h.app.shutdown_budget().deadlines().unwrap();
            assert!(!signal.is_cancelled());
            let drive = async {
                if mode == 1 {
                    signal.cancelled().await;
                    assert_eq!(Clock::now(), root.at(ShutdownStage::Graceful));
                } else {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    assert!(!signal.is_cancelled());
                    if mode == 0 {
                        h.probe.release_source.cancel();
                    } else {
                        h.app.transport_force().cancel();
                    }
                }
                assert_eq!(h.probe.disposed.load(Ordering::Acquire), 0);
                assert_eq!(h.probe.scopes.load(Ordering::Acquire), 1);
                assert!(!lily_injection::__private::container_shutdown_started(
                    h.app.container()
                ));
            };
            let (body, ()) = tokio::join!(response.into_body().collect(), drive);
            assert_eq!(body.unwrap().to_bytes(), "cooperative source");
            assert_eq!(
                Clock::now(),
                match mode {
                    0 => started + Duration::from_millis(20),
                    1 => root.at(ShutdownStage::Graceful) + Duration::from_millis(40),
                    _ => started + Duration::from_millis(60),
                }
            );
            assert_eq!(signal.is_cancelled(), mode != 0);
            let probe = h.probe.clone();
            finished(h, close).await;
            let events = probe.events.lock().unwrap();
            assert_eq!(
                *events,
                [
                    "normal-return",
                    "source-released",
                    "scope-disposing",
                    "scope-disposed",
                    "application-disposed"
                ]
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn forced_uncommitted_fallback_keeps_its_write_window_and_original_timeout_reason() {
    for protocol in [HttpProtocol::Http1_1, HttpProtocol::Http2] {
        for local_first in [false, true] {
            let mut h = harness(
                protocol,
                if local_first {
                    Duration::from_millis(100)
                } else {
                    Duration::from_secs(60)
                },
            )
            .await;
            let app = h.app.clone();
            let probe = h.probe.clone();
            let gate = h.gate.clone();
            let started = Clock::now();
            let cutoff = started + Duration::from_millis(if local_first { 350 } else { 250 });
            let (response, close) = tokio::join!(h.sender.send("/ignore"), async {
                probe.entered.notified().await;
                // Protocol setup has already completed before this gate.
                // Hold actual writes, not only a flush after every byte has
                // already reached a client which could legitimately disconnect.
                gate.block_writes();
                if local_first {
                    let signal = probe.signal.lock().unwrap().clone().unwrap();
                    signal.cancelled().await;
                    assert_eq!(Clock::now(), started + Duration::from_millis(100));
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                let close = shutdown(&app, true).await;
                tokio::time::sleep_until(cutoff + Duration::from_millis(40)).await;
                assert_eq!(app.task_inventory().connections.snapshot().outstanding, 1);
                assert_eq!(
                    app.task_inventory().connections.snapshot().abort_requested,
                    0
                );
                assert_eq!(app.task_inventory().protocol.snapshot().abort_requested, 0);
                assert_eq!(probe.disposed.load(Ordering::Acquire), 0);
                assert!(!lily_injection::__private::container_shutdown_started(
                    app.container()
                ));
                gate.release();
                close
            });
            assert_eq!(
                response.status(),
                if local_first {
                    StatusCode::GATEWAY_TIMEOUT
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            );
            let body = response.into_body().collect().await;
            let body = String::from_utf8(body.unwrap().to_bytes().to_vec()).unwrap();
            assert!(
                body.contains(if local_first {
                    "GATEWAY_TIMEOUT"
                } else {
                    "SERVICE_UNAVAILABLE"
                }),
                "{body}"
            );
            assert_eq!(Clock::now(), cutoff + Duration::from_millis(40));
            let registry = h.app.request_registry().clone();
            finished(h, close).await;
            let reasons = registry.snapshot().stop_reasons;
            assert_eq!(reasons.request_timeout, usize::from(local_first));
            assert_eq!(reasons.forced_shutdown, usize::from(!local_first));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn completed_fallback_transport_cannot_bypass_a_pending_exact_scope_receipt() {
    for protocol in [HttpProtocol::Http1_1, HttpProtocol::Http2] {
        let mut h = harness(protocol, Duration::from_secs(60)).await;
        h.probe.hold_disposal.store(true, Ordering::Release);
        let app = h.app.clone();
        let probe = h.probe.clone();
        let (response, close) = tokio::join!(h.sender.send("/ignore"), async {
            probe.entered.notified().await;
            shutdown(&app, true).await
        });
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(!response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty());
        h.listener.clone().await.unwrap();
        h.probe.disposing.notified().await;
        assert!(h.app.task_inventory().transport_is_terminal());
        assert_eq!(h.app.request_registry().snapshot().scopes.outstanding, 1);
        assert_eq!(h.probe.disposed.load(Ordering::Acquire), 0);
        assert!(!lily_injection::__private::container_shutdown_started(
            h.app.container()
        ));
        assert!(!close.task.is_finished());
        h.probe.release_disposal.cancel();
        let probe = h.probe.clone();
        finished(h, close).await;
        assert_eq!(
            *probe.events.lock().unwrap(),
            [
                "execution-notified",
                "termination",
                "scope-disposing",
                "scope-disposed",
                "application-disposed"
            ]
        );
    }
}

#[tokio::test]
async fn dropped_managed_server_signals_but_does_not_abort_the_response_owner() {
    for protocol in [HttpProtocol::Http1_1, HttpProtocol::Http2] {
        let app = Arc::new(
            AppBuilder::new("127.0.0.1:0")
                .protocol(protocol)
                .tracing_disabled()
                .middleware::<Pipeline>()
                .build()
                .await
                .unwrap(),
        );
        let probe = app.container().resolve::<Probe>(None).await.unwrap();
        assert!(probe.tasks.set(app.task_inventory().clone()).is_ok());
        let server = HttpServer(app.clone()).start_managed().await.unwrap();
        let listener = server.task.clone();
        let io = tokio::net::TcpStream::connect(server.bound_address())
            .await
            .unwrap();
        let (mut sender, client) = if protocol == HttpProtocol::Http2 {
            let (sender, connection) =
                hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(io))
                    .await
                    .unwrap();
            (Sender::Http2(sender), tokio::spawn(connection))
        } else {
            let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io))
                .await
                .unwrap();
            (Sender::Http1(sender), tokio::spawn(connection))
        };
        let response = sender.send("/stream").await;
        assert_eq!(response.status(), StatusCode::OK);
        drop(server);
        assert_eq!(listener.snapshot().abort_requested, 0);
        let body = tokio::time::timeout(Duration::from_secs(2), response.into_body().collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(body.to_bytes(), "cooperative source");
        let result = tokio::time::timeout(Duration::from_secs(2), listener)
            .await
            .unwrap()
            .unwrap();
        let report = result.as_ref().as_ref().unwrap();
        assert_eq!(report.forced, 0);
        assert_eq!(report.cancelled, 0);
        assert_eq!(app.task_inventory().listener.snapshot().abort_requested, 0);
        assert_eq!(
            app.task_inventory().connections.snapshot().abort_requested,
            0
        );
        assert_eq!(probe.disposed.load(Ordering::Acquire), 0);
        app.close().await.unwrap();
        assert_eq!(probe.disposed.load(Ordering::Acquire), 1);
        assert!(app.task_inventory().transport_is_terminal());
        drop(sender);
        tokio::time::timeout(Duration::from_secs(2), client)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn three_connections_must_all_join_before_application_dependency_disposal() {
    for protocol in [HttpProtocol::Http1_1, HttpProtocol::Http2] {
        let mut h = harness_with_peers(protocol, Duration::from_secs(60), 3, 1024 * 1024).await;
        let mut responses = Vec::new();
        h.sender
            .send("/ready")
            .await
            .into_body()
            .collect()
            .await
            .unwrap();
        h.gate.block();
        responses.push(h.sender.send("/buffered").await);
        for peer in &mut h.others {
            peer.sender
                .send("/ready")
                .await
                .into_body()
                .collect()
                .await
                .unwrap();
            peer.gate.block();
            responses.push(peer.sender.send("/buffered").await);
        }
        assert!(h.app.request_registry().wait().await.is_terminal());
        assert_eq!(h.probe.scopes.load(Ordering::Acquire), 0);
        let signal = h.probe.signal.lock().unwrap().clone().unwrap();
        let started = Clock::now();
        let close = shutdown(&h.app, true).await;
        let bodies =
            futures::future::join_all(responses.into_iter().map(|r| r.into_body().collect()));
        let progress = async {
            signal.cancelled().await;
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert_eq!(h.app.task_inventory().connections.snapshot().outstanding, 3);
            assert_eq!(
                h.app
                    .task_inventory()
                    .connections
                    .snapshot()
                    .abort_requested,
                0
            );
            h.gate.release();
            h.others[0].gate.release();
            h.drivers[0]
                .clone()
                .await
                .unwrap()
                .as_ref()
                .as_ref()
                .unwrap();
            h.drivers[1]
                .clone()
                .await
                .unwrap()
                .as_ref()
                .as_ref()
                .unwrap();
            let connections = h.app.task_inventory().connections.snapshot();
            assert_eq!(connections.completed, 2);
            assert_eq!(connections.outstanding, 1);
            assert_eq!(connections.abort_requested, 0);
            assert_eq!(h.probe.disposed.load(Ordering::Acquire), 0);
            assert!(!lily_injection::__private::container_shutdown_started(
                h.app.container()
            ));
            assert!(!close.task.is_finished());
            tokio::time::sleep(Duration::from_millis(40)).await;
            h.others[1].gate.release();
        };
        let (bodies, ()) = tokio::join!(bodies, progress);
        assert_eq!(bodies.len(), 3);
        for body in bodies {
            assert_eq!(body.unwrap().to_bytes(), Bytes::from(vec![7; 512 * 1024]));
        }
        assert_eq!(Clock::now(), started + Duration::from_millis(60));
        finished(h, close).await;
    }
}

#[tokio::test(start_paused = true)]
async fn exhausted_fallback_and_blocked_connection_stop_before_the_root_join_reserve() {
    let mut h = harness_with_peers(HttpProtocol::Http2, Duration::from_secs(60), 1, 0).await;
    let app = h.app.clone();
    let probe = h.probe.clone();
    let gate = h.gate.clone();
    let started = Clock::now();
    let (response, close) = tokio::join!(h.sender.send("/ignore"), async {
        probe.entered.notified().await;
        gate.block();
        shutdown(&app, true).await
    });
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(Clock::now(), started + Duration::from_millis(250));
    let root = h.app.shutdown_budget().deadlines().unwrap();
    tokio::time::sleep(Duration::from_millis(99)).await;
    let before = h.app.task_inventory().protocol.snapshot();
    assert_eq!(before.registered, 1);
    assert_eq!(before.abort_requested, 0);
    assert_eq!(before.outstanding, 1);
    assert_eq!(h.probe.disposed.load(Ordering::Acquire), 0);
    let protocol = h.app.task_inventory().protocol.wait().await;
    assert_eq!(Clock::now(), started + Duration::from_millis(350));
    assert_eq!(protocol.abort_requested, 1);
    assert_eq!(protocol.cancelled, 1);
    assert_eq!(protocol.outstanding, 0);
    // The per-response abort has joined, but the actual connection cannot
    // flush its framing/close yet. It is still a dependency user.
    assert_eq!(h.app.task_inventory().connections.snapshot().outstanding, 1);
    assert!(!lily_injection::__private::container_shutdown_started(
        h.app.container()
    ));
    let remaining = root.at(ShutdownStage::TransportStop) - Clock::now() - Duration::from_nanos(1);
    tokio::time::sleep(remaining).await;
    assert_eq!(
        h.app
            .task_inventory()
            .connections
            .snapshot()
            .abort_requested,
        0
    );
    assert_eq!(h.probe.disposed.load(Ordering::Acquire), 0);
    close.task.await.unwrap().unwrap();
    assert_eq!(Clock::now(), root.at(ShutdownStage::TransportStop));
    assert!(Clock::now() < root.at(ShutdownStage::Reconcile));
    let report = h.listener.await.unwrap();
    assert_eq!(report.forced, 1);
    assert_eq!(report.cancelled, 1);
    assert_eq!(report.outstanding, 0);
    assert_eq!(h.app.task_inventory().connections.snapshot().cancelled, 1);
    assert!(h.app.task_inventory().transport_is_terminal());
    assert_eq!(h.probe.disposed.load(Ordering::Acquire), 1);
    // No complete error body can be delivered through a zero stream window.
    assert!(response.into_body().collect().await.is_err());
    drop(h.sender);
    // A closed connection may surface as a protocol error or clean EOF to
    // this client driver. Its actual join, not either wire interpretation,
    // is the client-task cleanup obligation.
    let _ = h.client.await.unwrap();
}
