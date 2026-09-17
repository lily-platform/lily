use super::*;
use crate::server::response_control::{ResponseCommit, ResponseFrames};
use crate::AppBuilder;
use http_body_util::Full;
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, oneshot};

#[derive(Default)]
pub(super) struct FlushGate {
    blocked: std::sync::atomic::AtomicBool,
    writes: std::sync::atomic::AtomicBool,
    waker: futures::task::AtomicWaker,
}

impl FlushGate {
    pub(super) fn block(&self) {
        self.blocked.store(true, Ordering::Release);
    }

    pub(super) fn release(&self) {
        self.blocked.store(false, Ordering::Release);
        self.waker.wake();
    }

    pub(super) fn block_writes(&self) {
        self.writes.store(true, Ordering::Release);
        self.block();
    }
}

pub(super) struct GatedIo {
    pub(super) inner: tokio::io::DuplexStream,
    pub(super) gate: Arc<FlushGate>,
}

impl AsyncRead for GatedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for GatedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.gate.waker.register(cx.waker());
        if self.gate.writes.load(Ordering::Acquire) && self.gate.blocked.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.gate.waker.register(cx.waker());
        if self.gate.blocked.load(Ordering::Acquire) {
            return Poll::Pending;
        }
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // AsyncWrite::shutdown includes flushing. Do not let connection-close
        // bypass the pending write gate used by the transport qualification.
        std::task::ready!(self.as_mut().poll_flush(cx))?;
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

struct H2Pair {
    app: Arc<App>,
    sender: hyper::client::conn::http2::SendRequest<Full<Bytes>>,
    client: tokio::task::JoinHandle<Result<(), hyper::Error>>,
    driver: TaskReceipt<io::Result<ConnectionCloseReason>>,
    controls: mpsc::UnboundedReceiver<(String, ResponseTransportControl)>,
    activity: Arc<ConnectionActivity>,
    stop: CancellationToken,
    flush_gate: Arc<FlushGate>,
}

async fn h2_pair(window: u32) -> H2Pair {
    let app = Arc::new(AppBuilder::new("127.0.0.1:0").build().await.unwrap());
    let runtime = app.clone();
    let (client_io, server_io) = duplex(512 * 1024);
    let flush_gate = Arc::new(FlushGate::default());
    let server_io = GatedIo {
        inner: server_io,
        gate: flush_gate.clone(),
    };
    let (entered, controls) = mpsc::unbounded_channel();
    let (publish, published) = oneshot::channel();
    let stop = CancellationToken::new();
    let server_stop = stop.clone();
    let driver = app
        .task_inventory()
        .connections
        .try_spawn_with_receipt(
            move |receipt| {
                CONNECTION_TASK.scope(receipt, async move {
                    let activity = ConnectionActivity::new();
                    publish.send(activity.clone()).unwrap();
                    let request_activity = activity.clone();
                    let service = service_fn(move |request: HyperRequest<Incoming>| {
                        let activity = request_activity.clone();
                        let entered = entered.clone();
                        async move {
                            let path = request.uri().path().to_owned();
                            let mut guard = activity.enter();
                            let control = guard.bind_transport(request.version());
                            entered.send((path.clone(), control.clone())).unwrap();
                            if path == "/before" {
                                std::future::pending::<()>().await;
                            }
                            let bytes = match path.as_str() {
                                "/slow" | "/sibling" => Bytes::from(vec![7; 512 * 1024]),
                                "/one-frame" => Bytes::from_static(b"last frame"),
                                "/empty" | "/empty-timeout" => Bytes::new(),
                                _ => Bytes::from_static(b"healthy"),
                            };
                            let timeout = if matches!(
                                path.as_str(),
                                "/slow" | "/one-frame" | "/empty-timeout"
                            ) {
                                Duration::from_millis(100)
                            } else {
                                Duration::from_secs(5)
                            };
                            let response = HyperResponse::new(BoundedResponseBody::new_for_test(
                                bytes,
                                16 * 1024,
                                timeout,
                                guard,
                            ));
                            control.commit()?;
                            Ok::<_, ResponseStopped>(response)
                        }
                    });
                    let local = TaskRegistry::default();
                    let owner = ConnectionProtocolOwner {
                        tasks: local.clone(),
                    };
                    let config = HttpTransportConfig::default();
                    let builder = HttpServer::connection_builder_with_executor(
                        &config,
                        HttpProtocol::Http2,
                        TrackedHttpExecutor {
                            local: local.clone(),
                            parent: runtime.task_inventory().protocol.clone(),
                            keep_alive: runtime.clone(),
                        },
                    );
                    let result = HttpServer::drive_connection(
                        server_io,
                        builder,
                        service,
                        server_stop,
                        activity,
                        config.connection_idle_timeout,
                    )
                    .await;
                    drop(owner);
                    assert!(local.wait().await.is_terminal());
                    result
                })
            },
            None,
        )
        .unwrap();
    let activity = published.await.unwrap();
    let (sender, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .initial_stream_window_size(window)
        .initial_connection_window_size(1024 * 1024)
        .handshake(TokioIo::new(client_io))
        .await
        .unwrap();
    H2Pair {
        app,
        sender,
        client: tokio::spawn(connection),
        driver,
        controls,
        activity,
        stop,
        flush_gate,
    }
}

#[tokio::test]
async fn h2_response_worker_stays_abortable_while_final_io_flush_is_pending() {
    for path in ["/one-frame", "/empty-timeout"] {
        let mut pair = h2_pair(16 * 1024).await;
        pair.flush_gate.block();
        let response = pair.sender.send_request(request(path)).await.unwrap();
        let control = next_control(&mut pair, path).await;
        assert_eq!(control.snapshot().frames, ResponseFrames::Completed);
        assert!(!control.snapshot().is_terminal());
        tokio::time::timeout(Duration::from_secs(5), control.worker().unwrap())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(control.snapshot().task.unwrap().cancelled, 1);
        pair.flush_gate.release();
        assert!(response.into_body().collect().await.is_err());
        let healthy = pair.sender.send_request(request("/healthy")).await.unwrap();
        assert_eq!(
            healthy.into_body().collect().await.unwrap().to_bytes(),
            "healthy"
        );
        close(pair).await;
    }
}

fn request(path: &str) -> HyperRequest<Full<Bytes>> {
    HyperRequest::builder()
        .uri(format!("http://lily.test{path}"))
        .body(Full::new(Bytes::new()))
        .unwrap()
}

async fn next_control(pair: &mut H2Pair, path: &str) -> ResponseTransportControl {
    let (actual, control) = tokio::time::timeout(Duration::from_secs(5), pair.controls.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(actual, path);
    control
}

async fn close(pair: H2Pair) {
    pair.stop.cancel();
    let result = tokio::time::timeout(Duration::from_secs(5), pair.driver)
        .await
        .unwrap()
        .unwrap();
    assert!(result.is_ok(), "{result:?}");
    drop(pair.sender);
    tokio::time::timeout(Duration::from_secs(5), pair.client)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(pair.app.task_inventory().transport_is_terminal());
    pair.app.close().await.unwrap();
}

#[tokio::test]
async fn h2_write_timeout_aborts_only_its_worker_and_preserves_active_and_later_siblings() {
    let mut pair = h2_pair(16 * 1024).await;
    let slow = pair.sender.send_request(request("/slow")).await.unwrap();
    let slow_control = next_control(&mut pair, "/slow").await;
    let sibling = pair.sender.send_request(request("/sibling")).await.unwrap();
    let sibling_control = next_control(&mut pair, "/sibling").await;
    assert_eq!(
        slow_control.snapshot().commit,
        ResponseCommit::CommittedToProtocol
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(5), slow_control.worker().unwrap())
            .await
            .unwrap()
            .unwrap_err()
            .is_cancelled()
    );
    assert_eq!(slow_control.snapshot().task.unwrap().cancelled, 1);
    assert_eq!(sibling_control.snapshot().stop_requested, None);
    assert_eq!(sibling_control.snapshot().task.unwrap().outstanding, 1);
    assert!(slow.into_body().collect().await.is_err());
    assert_eq!(
        sibling
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .len(),
        512 * 1024
    );
    sibling_control.worker().unwrap().await.unwrap();
    let healthy = pair.sender.send_request(request("/healthy")).await.unwrap();
    assert_eq!(
        healthy.into_body().collect().await.unwrap().to_bytes(),
        "healthy"
    );
    let stats = pair.activity.transport_snapshot();
    assert_eq!(stats.stop_requested, 1);
    assert_eq!(stats.tasks_aborted, 1);
    close(pair).await;
}

#[tokio::test]
async fn h2_eof_handoff_does_not_remove_the_watchdog_while_the_last_frame_waits_for_capacity() {
    for window in [0, 1] {
        let mut pair = h2_pair(window).await;
        let response = pair
            .sender
            .send_request(request("/one-frame"))
            .await
            .unwrap();
        let control = next_control(&mut pair, "/one-frame").await;
        let initial = control.snapshot();
        assert_eq!(initial.frames, ResponseFrames::Completed);
        assert_eq!(initial.task.unwrap().outstanding, 1);
        assert!(!initial.is_terminal());
        tokio::time::timeout(Duration::from_secs(5), control.worker().unwrap())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(
            control.snapshot().stop_requested,
            Some(ExecutionStopReason::ResponseFinalizationTimeout)
        );
        assert!(response.into_body().collect().await.is_err());
        // Even with a zero DATA window, an unrelated empty response can finish.
        let response = pair.sender.send_request(request("/empty")).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert!(response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty());
        close(pair).await;
    }
}

#[tokio::test]
async fn h2_stop_before_commit_cannot_send_a_second_response_or_claim_an_abort_join() {
    let mut pair = h2_pair(16 * 1024).await;
    let response = pair.sender.send_request(request("/before"));
    let request_task = tokio::spawn(response);
    let control = next_control(&mut pair, "/before").await;
    assert_eq!(control.snapshot().commit, ResponseCommit::Uncommitted);
    assert!(control.request_stop(ExecutionStopReason::ForcedShutdown));
    assert!(!control.request_stop(ExecutionStopReason::RequestTimeout));
    assert!(control.commit().is_err());
    let requested = control.snapshot();
    assert_eq!(requested.task.unwrap().abort_requested, 1);
    assert_eq!(requested.task.unwrap().cancelled, 0);
    assert!(!requested.is_terminal());
    tokio::time::timeout(Duration::from_secs(5), control.worker().unwrap())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(control.snapshot().task.unwrap().cancelled, 1);
    assert!(request_task.await.unwrap().is_err());
    let healthy = pair.sender.send_request(request("/healthy")).await.unwrap();
    assert_eq!(
        healthy.into_body().collect().await.unwrap().to_bytes(),
        "healthy"
    );
    close(pair).await;
}

#[tokio::test]
async fn h2_graceful_drain_keeps_local_stop_isolated_from_accepted_siblings() {
    let mut pair = h2_pair(16 * 1024).await;
    let slow = pair.sender.send_request(request("/slow")).await.unwrap();
    let control = next_control(&mut pair, "/slow").await;
    let sibling = pair.sender.send_request(request("/sibling")).await.unwrap();
    let other = next_control(&mut pair, "/sibling").await;
    pair.stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), control.worker().unwrap())
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(other.snapshot().stop_requested, None);
    assert!(slow.into_body().collect().await.is_err());
    assert_eq!(
        sibling
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .len(),
        512 * 1024
    );
    close(pair).await;
}

#[tokio::test]
async fn http1_last_frame_still_needs_flush_and_a_stop_releases_the_actual_connection() {
    for stop_reason in [
        None,
        Some(ExecutionStopReason::RequestTimeout),
        Some(ExecutionStopReason::ForcedShutdown),
    ] {
        let app = Arc::new(AppBuilder::new("127.0.0.1:0").build().await.unwrap());
        let (mut client, server_io) = duplex(32);
        let (publish, published) = oneshot::channel();
        let publish = Arc::new(Mutex::new(Some(publish)));
        let driver = app
            .task_inventory()
            .connections
            .try_spawn_with_receipt(
                move |receipt| {
                    CONNECTION_TASK.scope(receipt, async move {
                        let activity = ConnectionActivity::new();
                        let request_activity = activity.clone();
                        let service = service_fn(move |request: HyperRequest<Incoming>| {
                            let activity = request_activity.clone();
                            let publish = publish.clone();
                            async move {
                                let mut guard = activity.enter();
                                let control = guard.bind_transport(request.version());
                                let response =
                                    HyperResponse::new(BoundedResponseBody::new_for_test(
                                        Bytes::from(vec![1; 2048]),
                                        4096,
                                        Duration::from_millis(100),
                                        guard,
                                    ));
                                control.commit().unwrap();
                                publish
                                    .lock()
                                    .unwrap()
                                    .take()
                                    .unwrap()
                                    .send(control)
                                    .unwrap();
                                Ok::<_, Infallible>(response)
                            }
                        });
                        let config = HttpTransportConfig::default();
                        HttpServer::drive_connection(
                            server_io,
                            HttpServer::connection_builder(&config, HttpProtocol::Http1_1),
                            service,
                            CancellationToken::new(),
                            activity,
                            config.connection_idle_timeout,
                        )
                        .await
                    })
                },
                None,
            )
            .unwrap();
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: lily.test\r\n\r\n")
            .await
            .unwrap();
        let control = published.await.unwrap();
        assert_eq!(control.snapshot().frames, ResponseFrames::Completed);
        assert!(!control.snapshot().http1_flushed);
        assert!(!control.snapshot().is_terminal());
        if let Some(reason) = stop_reason {
            assert!(control.request_stop(reason));
        }
        let result = tokio::time::timeout(Duration::from_secs(5), driver)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            *result.as_ref().as_ref().unwrap(),
            stop_reason.map_or(
                ConnectionCloseReason::ResponseFinalizationTimeout,
                ConnectionCloseReason::ResponseStopped
            )
        );
        let mut report = ConnectionTaskReport {
            accepted: 1,
            ..ConnectionTaskReport::default()
        };
        report.record_join(Ok(result));
        assert!(report.reconciles());
        assert_eq!(
            report.response_finalization_timeout,
            usize::from(stop_reason.is_none())
        );
        assert_eq!(report.response_stopped, usize::from(stop_reason.is_some()));
        let stopped = control.snapshot();
        assert!(stopped.driver_released);
        assert_eq!(stopped.task.unwrap().outstanding, 0);
        assert!(!stopped.http1_flushed);
        let mut partial = Vec::new();
        client.read_to_end(&mut partial).await.unwrap();
        assert!(partial.len() < 2048);
        app.close().await.unwrap();
    }
}

#[tokio::test]
async fn http1_completed_flush_retires_watchdog_without_closing_keep_alive() {
    let app = Arc::new(AppBuilder::new("127.0.0.1:0").build().await.unwrap());
    let (client_io, server_io) = duplex(4096);
    let (publish, mut published) = mpsc::unbounded_channel();
    let driver = app
        .task_inventory()
        .connections
        .try_spawn_with_receipt(
            move |receipt| {
                CONNECTION_TASK.scope(receipt, async move {
                    let activity = ConnectionActivity::new();
                    let request_activity = activity.clone();
                    let service = service_fn(move |request: HyperRequest<Incoming>| {
                        let activity = request_activity.clone();
                        let publish = publish.clone();
                        async move {
                            let mut guard = activity.enter();
                            let control = guard.bind_transport(request.version());
                            let response = HyperResponse::new(BoundedResponseBody::new_for_test(
                                Bytes::from_static(b"healthy"),
                                4096,
                                Duration::from_millis(50),
                                guard,
                            ));
                            control.commit()?;
                            publish.send((control, activity)).unwrap();
                            Ok::<_, ResponseStopped>(response)
                        }
                    });
                    let config = HttpTransportConfig::default();
                    HttpServer::drive_connection(
                        server_io,
                        HttpServer::connection_builder(&config, HttpProtocol::Http1_1),
                        service,
                        CancellationToken::new(),
                        activity,
                        config.connection_idle_timeout,
                    )
                    .await
                })
            },
            None,
        )
        .unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(client_io))
        .await
        .unwrap();
    let client = tokio::spawn(connection);
    let response = sender.send_request(request("/first")).await.unwrap();
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "healthy"
    );
    let (control, activity) = published.recv().await.unwrap();
    let complete = control.snapshot();
    assert!(complete.http1_flushed);
    assert!(complete.is_terminal());
    assert_eq!(complete.task.unwrap().outstanding, 1);
    assert!(!control.request_stop(ExecutionStopReason::ResponseFinalizationTimeout));
    tokio::time::sleep(Duration::from_millis(100)).await;
    let response = sender.send_request(request("/second")).await.unwrap();
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "healthy"
    );
    let stats = activity.transport_snapshot();
    assert_eq!(stats.registered, 2);
    assert_eq!(stats.stop_requested, 0);
    assert_eq!(stats.outstanding, 0);
    drop(sender);
    tokio::time::timeout(Duration::from_secs(5), client)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .unwrap()
        .unwrap()
        .is_ok());
    app.close().await.unwrap();
}

#[derive(Default, lily_injection::Injectable)]
#[service(lifetime = "Singleton")]
struct ManagedProbe {
    entered: Notify,
    observed: Notify,
    release: CancellationToken,
}
impl lily_injection::ServiceTrait for ManagedProbe {}

struct ManagedMiddleware(Arc<ManagedProbe>);

#[async_trait::async_trait]
impl crate::HttpMiddleware for ManagedMiddleware {
    async fn new(
        extensions: Arc<lily_injection::Extensions>,
    ) -> Result<Self, crate::HttpMiddlewareInitError> {
        Ok(Self(
            extensions.get_service::<ManagedProbe>(None).await.unwrap(),
        ))
    }
    fn descriptor(&self) -> crate::MiddlewareDescriptor {
        crate::MiddlewareDescriptor::new("response-control-managed", crate::MiddlewareKind::Custom)
    }
    async fn handle(
        &self,
        exchange: &mut crate::HttpExchange<'_>,
        _: crate::HttpNext<'_>,
        cancellation: crate::ExecutionCancellation,
    ) -> Result<(), crate::HttpMiddlewareError> {
        if exchange.request().path() == "/held" {
            self.0.entered.notify_one();
            cancellation.cancelled().await;
            self.0.observed.notify_one();
            self.0.release.cancelled().await;
        }
        exchange
            .response_mut()
            .write_body(b"managed response")
            .unwrap();
        Ok(())
    }
}

#[tokio::test]
async fn managed_h2_worker_abort_keeps_execution_owner_and_scope_alive_until_cooperative_return() {
    let app = Arc::new(
        AppBuilder::new("127.0.0.1:0")
            .protocol(HttpProtocol::Http2)
            .middleware::<ManagedMiddleware>()
            .build()
            .await
            .unwrap(),
    );
    let probe = app.container().resolve::<ManagedProbe>(None).await.unwrap();
    let runtime = app.as_ref().clone();
    let root = tokio::spawn(async move {
        runtime
            .start_with_cancellation(CancellationToken::new())
            .await
    });
    let address = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(address) = app.bound_address() {
                break address;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let socket = tokio::net::TcpStream::connect(address).await.unwrap();
    let (mut sender, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(socket))
            .await
            .unwrap();
    let client = tokio::spawn(connection);
    let held = tokio::spawn(sender.send_request(request("/held")));
    tokio::time::timeout(Duration::from_secs(5), probe.entered.notified())
        .await
        .unwrap();
    let controls = app.request_registry().response_transports();
    assert_eq!(controls.len(), 1);
    let control = &controls[0];
    assert_eq!(control.snapshot().commit, ResponseCommit::Uncommitted);
    control.request_stop(ExecutionStopReason::TransportFailure);
    tokio::time::timeout(Duration::from_secs(5), control.worker().unwrap())
        .await
        .unwrap()
        .unwrap_err();
    probe.observed.notified().await;
    assert_eq!(app.container().active_scope_count(), 1);
    assert_eq!(app.request_registry().snapshot().execution.outstanding, 1);
    probe.release.cancel();
    assert!(held.await.unwrap().is_err());
    tokio::time::timeout(Duration::from_secs(5), app.request_registry().wait())
        .await
        .unwrap();
    assert_eq!(app.container().active_scope_count(), 0);
    let healthy = sender.send_request(request("/healthy")).await.unwrap();
    assert_eq!(
        healthy.into_body().collect().await.unwrap().to_bytes(),
        "managed response"
    );
    drop(sender);
    app.close().await.unwrap();
    root.await.unwrap().unwrap();
    client.await.unwrap().unwrap();
    assert!(app.task_inventory().transport_is_terminal());
}
