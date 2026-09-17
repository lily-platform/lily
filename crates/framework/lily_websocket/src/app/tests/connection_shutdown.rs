use super::*;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};

static PROBES: std::sync::LazyLock<StdMutex<HashMap<Uuid, Arc<Probe>>>> =
    std::sync::LazyLock::new(|| StdMutex::new(HashMap::new()));

#[derive(Clone, Copy)]
enum Connected {
    Ready,
    Error,
    Panic,
    Pending,
    Cooperative,
}

struct Probe {
    connected: Connected,
    reject_opened: bool,
    connected_started: AtomicBool,
    connected_dropped: AtomicBool,
    transport_dropped: AtomicBool,
    disconnected_checked: AtomicUsize,
    closed_checked: AtomicUsize,
    other: Uuid,
}

struct StartupDrop(Arc<Probe>);
impl Drop for StartupDrop {
    fn drop(&mut self) {
        self.0.connected_dropped.store(true, Ordering::SeqCst);
    }
}

pub(super) fn lifecycle(
    connected: bool,
    invocation: &WebSocketLifecycleInvocation,
) -> Option<WebSocketLifecycleFuture> {
    let context = invocation.context().clone();
    let probe = PROBES
        .lock()
        .unwrap()
        .get(&context.connection_id())?
        .clone();
    let signal = invocation.execution_cancellation().cloned();
    Some(Box::pin(async move {
        if connected {
            let _drop = StartupDrop(probe.clone());
            probe.connected_started.store(true, Ordering::SeqCst);
            match probe.connected {
                Connected::Ready => Ok(()),
                Connected::Error => Err(crate::controller::WebSocketLifecycleError::Internal),
                Connected::Panic => panic!("phase6 connected panic"),
                Connected::Pending => std::future::pending().await,
                Connected::Cooperative => {
                    signal.unwrap().cancelled().await;
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    Ok(())
                }
            }
        } else {
            assert!(probe.connected_dropped.load(Ordering::SeqCst));
            assert!(
                probe.transport_dropped.load(Ordering::SeqCst),
                "disconnected cannot precede physical transport drop"
            );
            assert!(
                context
                    .clients()
                    .caller()
                    .send("open-order:left", serde_json::Value::Null)
                    .await
                    .is_err(),
                "the closing caller is not a live outbound target"
            );
            context
                .clients()
                .client(probe.other)
                .send("open-order:left", serde_json::Value::Null)
                .await
                .unwrap();
            probe.disconnected_checked.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }))
}

struct ClosedProbe;
#[async_trait]
impl WsConnectionMiddleware for ClosedProbe {
    async fn new(_: Arc<Extensions>) -> Result<Self, crate::middleware::WsMiddlewareInitError> {
        Ok(Self)
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("phase6_closed", MiddlewareKind::WebSocketConnection)
    }
    async fn opened(
        &self,
        context: Arc<WebSocketContext>,
        _: crate::ExecutionCancellation,
    ) -> Result<(), crate::middleware::WsMiddlewareError> {
        if PROBES.lock().unwrap()[&context.connection_id()].reject_opened {
            Err(crate::middleware::WsMiddlewareError::internal(
                MiddlewareErrorCode::INTERNAL,
            ))
        } else {
            Ok(())
        }
    }
    async fn closed(
        &self,
        context: Arc<WebSocketContext>,
        _: crate::middleware::WsConnectionCloseCategory,
        _: crate::CleanupCancellation,
    ) -> Result<(), crate::middleware::WsMiddlewareError> {
        let probe = PROBES.lock().unwrap()[&context.connection_id()].clone();
        assert!(probe.transport_dropped.load(Ordering::SeqCst));
        if probe.connected_started.load(Ordering::SeqCst) {
            assert!(probe.connected_dropped.load(Ordering::SeqCst));
        }
        probe.closed_checked.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

struct ObservedTransport {
    inner: Option<DuplexStream>,
    probe: Arc<Probe>,
}
impl Drop for ObservedTransport {
    fn drop(&mut self) {
        drop(self.inner.take());
        self.probe.transport_dropped.store(true, Ordering::SeqCst);
    }
}
impl AsyncRead for ObservedTransport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(self.get_mut().inner.as_mut().unwrap()).poll_read(cx, buffer)
    }
}
impl AsyncWrite for ObservedTransport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(self.get_mut().inner.as_mut().unwrap()).poll_write(cx, buffer)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(self.get_mut().inner.as_mut().unwrap()).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(self.get_mut().inner.as_mut().unwrap()).poll_shutdown(cx)
    }
}

async fn wait_until(check: impl Fn() -> bool) {
    timeout(Duration::from_secs(2), async {
        while !check() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn real_connection_startup_failures_force_and_peer_close_obey_terminal_barriers() {
    for (connected, reject_opened, abort_task, shutdown_race) in [
        (Connected::Ready, false, false, false),
        (Connected::Ready, false, false, true),
        (Connected::Ready, true, false, false),
        (Connected::Error, false, false, false),
        (Connected::Panic, false, false, false),
        (Connected::Pending, false, false, false),
        (Connected::Pending, false, true, false),
        (Connected::Cooperative, false, false, false),
    ] {
        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .connection_middleware::<ClosedProbe>()
            .build()
            .await
            .unwrap();
        let id = Uuid::new_v4();
        let other = Uuid::new_v4();
        let (other_tx, mut other_rx) = mpsc::channel(2);
        app.connection_manager
            .add_connection(other, other_tx, None, Some("open-order".into()))
            .await
            .unwrap();
        let probe = Arc::new(Probe {
            connected,
            reject_opened,
            connected_started: AtomicBool::new(false),
            connected_dropped: AtomicBool::new(false),
            transport_dropped: AtomicBool::new(false),
            disconnected_checked: AtomicUsize::new(0),
            closed_checked: AtomicUsize::new(0),
            other,
        });
        PROBES.lock().unwrap().insert(id, probe.clone());
        let (server_io, client_io) = duplex(16 * 1024);
        let source = CancellationToken::new();
        let admission = CancellationToken::new();
        let task = tokio::spawn(WsApp::handle_connection(
            ObservedTransport {
                inner: Some(server_io),
                probe: probe.clone(),
            },
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: id,
                permit: app.connection_permits.clone().try_acquire_owned().unwrap(),
                span: tracing::info_span!("test.websocket.phase6"),
            },
            None,
            CancellationToken::new(),
            admission.clone(),
            source.clone(),
        ));
        let (mut client, _) =
            tokio_tungstenite::client_async("ws://localhost/ws?namespace=open-order", client_io)
                .await
                .unwrap();
        if !reject_opened {
            wait_until(|| probe.connected_started.load(Ordering::SeqCst)).await;
        }
        if abort_task {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            match connected {
                Connected::Pending | Connected::Cooperative => {
                    app.scope_cleanup_registry
                        .budget
                        .force_before(Instant::now() + Duration::from_millis(300));
                    source.cancel();
                }
                Connected::Ready if !reject_opened => {
                    if shutdown_race {
                        admission.cancel();
                    }
                    let _ = client.close(None).await;
                }
                _ => {}
            }
            let _ = timeout(Duration::from_secs(1), client.next()).await;
            let result = timeout(Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                result.is_ok(),
                !reject_opened && matches!(connected, Connected::Ready | Connected::Cooperative)
            );
        }
        wait_until(|| probe.closed_checked.load(Ordering::SeqCst) == 1).await;
        let report = app
            .connection_cleanup_registry
            .finalize_all(
                crate::middleware::WsConnectionCloseCategory::Cancelled,
                Instant::now() + Duration::from_secs(1),
            )
            .await;
        assert_eq!(report.prerequisites_incomplete, 0);
        assert_eq!(report.session_incomplete, 0);
        let expected_disconnect = usize::from(
            !reject_opened && matches!(connected, Connected::Ready | Connected::Cooperative),
        );
        assert_eq!(
            probe.disconnected_checked.load(Ordering::SeqCst),
            expected_disconnect
        );
        assert_eq!(other_rx.try_recv().is_ok(), expected_disconnect == 1);
        assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
        let (accounting, drivers) = app.connection_cleanup_registry.accounting();
        assert_eq!(accounting.owners, 1);
        assert_eq!(accounting.workers.completed, 1);
        assert_eq!(accounting.middleware.termination.completed, 1);
        assert_eq!(accounting.stages.terminal_completed, expected_disconnect);
        assert_eq!(accounting.disconnected.total, expected_disconnect);
        assert_eq!(accounting.disconnected.completed, expected_disconnect);
        assert_eq!(drivers.outstanding, 0);
        assert_eq!(
            app.connection_cleanup_registry.accounting(),
            (accounting, drivers)
        );
        assert_eq!(
            app.active_connection_count().await,
            1,
            "only the unrelated live target remains"
        );
        app.connection_manager
            .remove_connection(other)
            .await
            .unwrap();
        assert_eq!(
            app.connection_permits.available_permits(),
            app.server_config().max_connections
        );
        app.container.close().await.unwrap();
        PROBES.lock().unwrap().remove(&id);
    }
}

#[tokio::test]
async fn disconnected_only_controller_is_not_implicitly_armed() {
    let container = Arc::new(ApplicationContainer::build().await.unwrap());
    let extensions = container.services();
    let table = test_action_table(
        vec![pending_lifecycle(
            WebSocketOperationKind::Disconnected,
            bind_disconnect_safe,
        )],
        extensions.clone(),
    )
    .await;
    let lifecycle = table.lifecycle_for_namespace("orders").unwrap();
    let id = Uuid::new_v4();
    let context = Arc::new(WebSocketContext::new(
        id,
        Arc::new(ConnectionManager::new()),
        "orders".into(),
    ));
    let ledger = Default::default();
    let result = WsApp::run_connect_hooks(
        id,
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
    assert_eq!(result, Ok(()));
    assert!(ledger.lock().unwrap().is_empty());
    container.close().await.unwrap();
}
