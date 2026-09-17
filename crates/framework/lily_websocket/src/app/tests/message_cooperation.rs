use super::*;
use crate::middleware::{WsMessageTerminationContext, WsMiddlewareError, WsMiddlewareInitError};
use crate::{CleanupCancellation, Emit, ExecutionCancellation, Payload};

struct Probe {
    started: Semaphore,
    entered: AtomicUsize,
    normal: AtomicUsize,
    termination: AtomicUsize,
    observed: AtomicBool,
    cooperative_release: StdMutex<Option<Arc<Semaphore>>>,
}

impl Default for Probe {
    fn default() -> Self {
        Self {
            started: Semaphore::new(0),
            entered: AtomicUsize::new(0),
            normal: AtomicUsize::new(0),
            termination: AtomicUsize::new(0),
            observed: AtomicBool::new(false),
            cooperative_release: StdMutex::new(None),
        }
    }
}

static PROBES: std::sync::LazyLock<StdMutex<HashMap<Uuid, Arc<Probe>>>> =
    std::sync::LazyLock::new(|| StdMutex::new(HashMap::new()));

fn probe(context: &WebSocketContext) -> Arc<Probe> {
    PROBES.lock().unwrap()[&context.connection_id()].clone()
}

struct Middleware;

#[async_trait]
impl WsMessageMiddleware for Middleware {
    async fn new(_: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Ok(Self)
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("cooperation_wire", MiddlewareKind::WebSocketMessage)
    }
    async fn before_message(
        &self,
        exchange: &mut WsMessageExchange,
        _: ExecutionCancellation,
    ) -> Result<WsMessageDecision, WsMiddlewareError> {
        probe(exchange.connection())
            .entered
            .fetch_add(1, Ordering::SeqCst);
        Ok(WsMessageDecision::Continue)
    }
    async fn after_message(
        &self,
        exchange: &mut WsMessageExchange,
        _: WsMessageOutcome,
        _: ExecutionCancellation,
    ) -> Result<WsMessageDecision, WsMiddlewareError> {
        probe(exchange.connection())
            .normal
            .fetch_add(1, Ordering::SeqCst);
        Ok(WsMessageDecision::Continue)
    }
    async fn on_message_termination(
        &self,
        context: WsMessageTerminationContext<'_>,
        cancellation: CleanupCancellation,
    ) -> Result<(), WsMiddlewareError> {
        assert!(!cancellation.is_cancelled());
        probe(context.connection())
            .termination
            .fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[derive(crate::WebSocketController)]
#[namespace("cooperation-wire")]
#[message_middleware(Middleware)]
struct Controller;

#[async_trait]
impl crate::WebSocketControllerTrait for Controller {
    async fn new(_: Arc<Extensions>) -> Result<Self, crate::WebSocketControllerInitError> {
        Ok(Self)
    }
}

#[crate::websocket_controller]
impl Controller {
    #[message("run")]
    async fn run(
        &self,
        context: WebSocketContext,
        cancellation: ExecutionCancellation,
        Payload(mode): Payload<String>,
    ) -> Result<Emit<&'static str>, WebSocketActionError> {
        let probe = probe(&context);
        probe.started.add_permits(1);
        match mode.as_str() {
            "pending" => std::future::pending::<()>().await,
            "cooperate" | "error" | "cooperate-no-send" => {
                cancellation.cancelled().await;
                probe.observed.store(true, Ordering::SeqCst);
                let gate = probe.cooperative_release.lock().unwrap().clone();
                if let Some(gate) = gate {
                    gate.acquire().await.unwrap().forget();
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
                if mode != "cooperate-no-send" {
                    context
                        .clients()
                        .caller()
                        .send("cooperation-wire:side-effect", "accepted")
                        .await
                        .unwrap();
                }
                if mode == "error" {
                    return Err(WebSocketActionError::rejected(
                        WebSocketErrorCode::new("ACTUAL_ERROR").unwrap(),
                        "Application result.",
                    )
                    .unwrap());
                }
            }
            "normal" => assert!(
                !cancellation.is_cancelled(),
                "next message has an independent live signal"
            ),
            _ => panic!("unexpected mode"),
        }
        Ok(Emit::new("cooperation-wire:result", "actual-result").unwrap())
    }
}

#[tokio::test(start_paused = true)]
async fn local_timeout_keeps_healthy_connection_and_forced_cooperative_result_precedes_close() {
    for (mode, force) in [
        ("pending", false),
        ("cooperate", false),
        ("error", false),
        ("cooperate", true),
        ("error", true),
    ] {
        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                message_timeout_secs: 1,
                ..ServerConfig::default()
            })
            .build()
            .await
            .unwrap();
        let id = Uuid::new_v4();
        let probe = Arc::new(Probe::default());
        PROBES.lock().unwrap().insert(id, probe.clone());
        let (server_io, client_io) = duplex(32 * 1024);
        let source = CancellationToken::new();
        let admission = CancellationToken::new();
        let task = tokio::spawn(WsApp::handle_connection(
            server_io,
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: id,
                permit: app.connection_permits.clone().try_acquire_owned().unwrap(),
                span: tracing::info_span!("test.websocket.message_cooperation"),
            },
            None,
            CancellationToken::new(),
            admission.clone(),
            source.clone(),
        ));
        let (mut client, _) = tokio_tungstenite::client_async(
            "ws://localhost/ws?namespace=cooperation-wire",
            client_io,
        )
        .await
        .unwrap();
        client
            .send(
                crate::WsMessageBody::try_new("cooperation-wire:run", mode)
                    .unwrap()
                    .to_message()
                    .unwrap(),
            )
            .await
            .unwrap();
        probe.started.acquire().await.unwrap().forget();
        if force {
            let deadline = Instant::now() + Duration::from_millis(400);
            app.scope_cleanup_registry.budget.force_before(deadline);
            app.dispatcher.set_force_deadline(deadline);
            admission.cancel();
            source.cancel();
            app.dispatcher.force_drain();
        }
        let mut frames = Vec::new();
        let expected_frames = if mode == "pending" { 1 } else { 2 };
        timeout(Duration::from_secs(2), async {
            while frames.len() < expected_frames {
                let Some(Ok(Message::Text(text))) = client.next().await else {
                    panic!("message reply must precede any Close: {mode} force={force}");
                };
                frames.push(serde_json::from_str::<serde_json::Value>(&text).unwrap());
            }
        })
        .await
        .unwrap();
        let terminal = frames.last().unwrap();
        match mode {
            "pending" => assert_eq!(terminal["data"]["code"], "MESSAGE_TIMEOUT"),
            "error" => assert_eq!(terminal["data"]["code"], "ACTUAL_ERROR"),
            _ => assert_eq!(terminal["data"], "actual-result"),
        }
        if mode != "pending" {
            assert_eq!(frames[0]["event"], "cooperation-wire:side-effect");
        }
        assert_eq!(probe.observed.load(Ordering::SeqCst), mode != "pending");
        let (first, _) = app.message_dispatch_registry.accounting();
        assert_eq!(
            first.joined, 1,
            "a reply requires the real message owner join"
        );
        assert_eq!(first.execution.timed_out, usize::from(mode == "pending"));
        assert!(first.reconciles());
        assert_eq!(first.timeout_cancellation, usize::from(!force));
        assert_eq!(first.connection_cancellation, usize::from(force));
        assert_eq!(
            first.output.queued_frames, 1,
            "side-effect sends are not terminal replies"
        );
        assert_eq!(first.output.outstanding, 0);
        assert_eq!(first.pipeline.rejected, usize::from(mode == "error"));
        assert_eq!(first.pipeline.not_returned, usize::from(mode == "pending"));
        assert_eq!(
            first.completed_after_cancellation,
            usize::from(mode != "pending")
        );
        assert_eq!(
            first.completed_after_deadline,
            usize::from(!force && mode != "pending")
        );
        if force {
            assert!(matches!(client.next().await, Some(Ok(Message::Close(_)))));
            let _ = client.flush().await;
        } else {
            assert!(
                !source.is_cancelled(),
                "local timeout cannot cancel its connection"
            );
            client
                .send(
                    crate::WsMessageBody::try_new("cooperation-wire:run", "normal")
                        .unwrap()
                        .to_message()
                        .unwrap(),
                )
                .await
                .unwrap();
            let Some(Ok(Message::Text(text))) = client.next().await else {
                panic!("next message on the same socket must succeed");
            };
            let reply: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(
                reply["data"], "actual-result",
                "no duplicate prior terminal reply"
            );
            let _ = client.close(None).await;
        }
        timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        app.message_dispatch_registry.reconcile().await;
        assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
        assert_eq!(app.container.active_scope_count(), 0);
        let (messages, tasks) = app.message_dispatch_registry.accounting();
        assert_eq!(messages.outstanding, 0);
        assert_eq!(tasks.outstanding, 0);
        assert_eq!(
            probe.entered.load(Ordering::SeqCst),
            if force { 1 } else { 2 }
        );
        assert_eq!(
            probe.termination.load(Ordering::SeqCst),
            usize::from(mode == "pending")
        );
        assert_eq!(
            probe.normal.load(Ordering::SeqCst),
            if force || mode == "pending" { 1 } else { 2 }
        );
        app.container.close().await.unwrap();
        PROBES.lock().unwrap().remove(&id);
    }
}

type WireClient =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn root_app(
    message_timeout_secs: u64,
    shutdown_timeout: Duration,
) -> (
    Arc<WsApp>,
    SocketAddr,
    CancellationToken,
    tokio::task::JoinHandle<Result<(), ServerError>>,
) {
    let address = reserve_loopback_address();
    let mut app = WsAppBuilder::new(&address.to_string())
        .config(ServerConfig {
            allow_missing_origin: true,
            message_timeout_secs,
            ..ServerConfig::default()
        })
        .build()
        .await
        .unwrap();
    app.shutdown_timeout = shutdown_timeout;
    let app = Arc::new(app);
    let source = CancellationToken::new();
    let runtime = app.clone();
    let cancel = source.clone();
    let root = tokio::spawn(async move { runtime.start_with_cancellation(cancel).await });
    wait_for_accepting_health(&app).await;
    (app, address, source, root)
}

async fn open_three(app: &WsApp, address: SocketAddr) -> Vec<(WireClient, Uuid, Arc<Probe>)> {
    let mut clients = Vec::new();
    for _ in 0..3 {
        let before = app
            .connection_manager
            .get_namespace_connections("cooperation-wire")
            .await;
        let (client, _) = tokio_tungstenite::connect_async(format!(
            "ws://{address}/ws?namespace=cooperation-wire"
        ))
        .await
        .unwrap();
        let id = timeout(Duration::from_secs(2), async {
            loop {
                if let Some(id) = app
                    .connection_manager
                    .get_namespace_connections("cooperation-wire")
                    .await
                    .into_iter()
                    .find(|id| !before.contains(id))
                {
                    break id;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let probe = Arc::new(Probe::default());
        PROBES.lock().unwrap().insert(id, probe.clone());
        clients.push((client, id, probe));
    }
    clients
}

async fn send(client: &mut WireClient, mode: &str) {
    client
        .send(
            crate::WsMessageBody::try_new("cooperation-wire:run", mode)
                .unwrap()
                .to_message()
                .unwrap(),
        )
        .await
        .unwrap();
}

async fn read_result(client: &mut WireClient, mode: &str) {
    timeout(Duration::from_secs(3), async {
        if matches!(mode, "cooperate" | "error") {
            let Some(Ok(Message::Text(text))) = client.next().await else {
                panic!("expected cooperative side effect");
            };
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            assert_eq!(value["event"], "cooperation-wire:side-effect");
        }
        let Some(Ok(Message::Text(text))) = client.next().await else {
            panic!("expected preserved terminal result for {mode}");
        };
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        match mode {
            "pending" => assert_eq!(value["data"]["code"], "MESSAGE_TIMEOUT"),
            "error" => assert_eq!(value["data"]["code"], "ACTUAL_ERROR"),
            _ => assert_eq!(value["data"], "actual-result"),
        }
    })
    .await
    .unwrap();
}

async fn read_close(client: &mut WireClient) {
    assert!(
        matches!(
            timeout(Duration::from_secs(3), client.next())
                .await
                .unwrap(),
            Some(Ok(Message::Close(_)))
        ),
        "no duplicate or late message reply may precede Close"
    );
    let _ = client.flush().await;
}

fn assert_root_terminal(app: &WsApp, owners: usize) {
    let report = app.lifecycle.shutdown_report.get().unwrap();
    assert!(report.evidence.quiescent(), "{report:?}");
    assert!(report.evidence.reconciles(), "{report:?}");
    assert_eq!(report.evidence.messages.owners, owners);
    assert_eq!(report.evidence.messages.joined, owners);
    assert_eq!(report.evidence.connections.owners, 3);
    assert_eq!(report.evidence.scope_receipts_outstanding, 0);
    assert_eq!(report.evidence.messages.output.outstanding, 0);
    assert_eq!(report.evidence.dispatches_outstanding, 0);
    assert!(
        report
            .evidence
            .tasks
            .iter()
            .all(|(_, tasks)| tasks.outstanding == 0)
    );
    assert_eq!(
        app.connection_permits.available_permits(),
        app.server_config().max_connections
    );
    assert!(app.lifecycle.root_join_observed.load(Ordering::Acquire));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn three_connections_nine_messages_keep_local_timeout_isolated_and_later_shutdown_graceful() {
    let (app, address, source, root) = root_app(1, Duration::from_secs(3)).await;
    let mut clients = open_three(&app, address).await;
    send(&mut clients[0].0, "pending").await;
    clients[0].2.started.acquire().await.unwrap().forget();
    for (client, _, _) in clients.iter_mut().skip(1) {
        for _ in 0..3 {
            send(client, "normal").await;
            read_result(client, "normal").await;
        }
    }
    read_result(&mut clients[0].0, "pending").await;
    for mode in ["normal", "cooperate"] {
        send(&mut clients[0].0, mode).await;
        read_result(&mut clients[0].0, mode).await;
    }
    source.cancel();
    for (client, _, _) in &mut clients {
        read_close(client).await;
    }
    timeout(Duration::from_secs(4), root)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_root_terminal(&app, 9);
    let report = app.lifecycle.shutdown_report.get().unwrap().clone();
    assert_eq!(
        report.completion,
        lily_shutdown::FrameworkShutdownCompletion::GracefulCompleted
    );
    assert!(!report.forced);
    let messages = report.evidence.messages;
    assert_eq!(messages.execution.completed, 8);
    assert_eq!(messages.execution.timed_out, 1);
    assert_eq!(messages.timeout_cancellation, 2);
    assert_eq!(messages.connection_cancellation, 0);
    assert_eq!(messages.deadline_exceeded, 2);
    assert_eq!(messages.completed_after_deadline, 1);
    assert_eq!(messages.pipeline.handled, 8);
    assert_eq!(messages.pipeline.not_returned, 1);
    assert_eq!(messages.output.queued_frames, 9);
    assert_eq!(messages.middleware.normal.completed, 8);
    assert_eq!(messages.middleware.termination.completed, 1);
    app.close().await.unwrap();
    assert_eq!(app.lifecycle.shutdown_report.get().unwrap(), &report);
    for (_, id, _) in clients {
        PROBES.lock().unwrap().remove(&id);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_root_explicit_force_and_graceful_expiry_preserve_results_and_reject_late_dispatch() {
    for explicit_force in [true, false] {
        let (app, address, source, root) = root_app(10, Duration::from_millis(1200)).await;
        let mut clients = open_three(&app, address).await;
        for (client, _, _) in &mut clients {
            for _ in 0..2 {
                send(client, "normal").await;
                read_result(client, "normal").await;
            }
        }
        for ((client, _, probe), mode) in clients.iter_mut().zip(["cooperate", "error", "pending"])
        {
            probe.started.acquire_many(2).await.unwrap().forget();
            send(client, mode).await;
            probe.started.acquire().await.unwrap().forget();
        }
        let started = Instant::now();
        source.cancel();
        timeout(Duration::from_secs(1), async {
            while app.health_snapshot().unwrap().accepting_new_work {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(
            clients
                .iter()
                .all(|(_, _, probe)| !probe.observed.load(Ordering::SeqCst)),
            "admission closure cannot cancel accepted executions"
        );
        for (client, _, _) in &mut clients {
            send(client, "normal").await;
        }
        if explicit_force {
            app.lifecycle.shutdown_state.request_force();
        }
        for ((client, _, _), mode) in clients.iter_mut().zip(["cooperate", "error", "pending"]) {
            if mode != "pending" {
                read_result(client, mode).await;
            }
            read_close(client).await;
        }
        timeout(Duration::from_secs(2), root)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "phases cannot add fresh shutdown budgets"
        );
        assert_root_terminal(&app, 9);
        let report = app.lifecycle.shutdown_report.get().unwrap();
        assert_eq!(
            report.completion,
            lily_shutdown::FrameworkShutdownCompletion::ForcedCompleted
        );
        let messages = report.evidence.messages;
        assert_eq!(messages.execution.completed, 8);
        assert_eq!(messages.execution.aborted, 1);
        assert_eq!(messages.connection_cancellation, 3);
        assert_eq!(messages.timeout_cancellation, 0);
        assert_eq!(messages.deadline_exceeded, 0);
        assert_eq!(messages.completed_after_cancellation, 2);
        assert_eq!(messages.pipeline.handled, 7);
        assert_eq!(messages.pipeline.rejected, 1);
        assert_eq!(messages.output.queued_frames, 8);
        assert_eq!(messages.output.close_requests_completed, 1);
        assert_eq!(messages.middleware.normal.completed, 8);
        assert_eq!(messages.middleware.termination.completed, 1);
        for (_, id, _) in clients {
            PROBES.lock().unwrap().remove(&id);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn local_timeout_then_graceful_shutdown_preserves_result_but_peer_close_suppresses_delivery()
{
    for peer_close in [false, true] {
        let (app, address, source, root) = root_app(1, Duration::from_secs(3)).await;
        let mut clients = open_three(&app, address).await;
        let gate = Arc::new(Semaphore::new(0));
        *clients[0].2.cooperative_release.lock().unwrap() = Some(gate.clone());
        send(
            &mut clients[0].0,
            if peer_close {
                "cooperate-no-send"
            } else {
                "cooperate"
            },
        )
        .await;
        timeout(Duration::from_secs(2), async {
            while !clients[0].2.observed.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        // The local cancellation was observed, but the same execution future
        // remains held in its cooperative window while application drain begins.
        if peer_close {
            clients[0].0.close(None).await.unwrap();
            read_close(&mut clients[0].0).await;
        }
        source.cancel();
        timeout(Duration::from_secs(1), async {
            while app.health_snapshot().unwrap().accepting_new_work {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        gate.add_permits(1);
        if !peer_close {
            read_result(&mut clients[0].0, "cooperate").await;
            read_close(&mut clients[0].0).await;
        }
        for (client, _, _) in clients.iter_mut().skip(1) {
            read_close(client).await;
        }
        timeout(Duration::from_secs(4), root)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_root_terminal(&app, 1);
        let report = app.lifecycle.shutdown_report.get().unwrap();
        assert_eq!(
            report.completion,
            lily_shutdown::FrameworkShutdownCompletion::GracefulCompleted
        );
        let messages = report.evidence.messages;
        assert_eq!(messages.timeout_cancellation, 1);
        assert_eq!(messages.connection_cancellation, 0);
        assert_eq!(messages.completed_after_deadline, 1);
        assert_eq!(messages.pipeline.handled, 1);
        assert_eq!(messages.output.prepared_frames, 1);
        assert_eq!(messages.output.queued_frames, usize::from(!peer_close));
        assert_eq!(messages.output.suppressed, usize::from(peer_close));
        assert_eq!(messages.output.attempts, usize::from(!peer_close));
        assert_eq!(messages.middleware.normal.completed, 1);
        assert_eq!(messages.middleware.termination.total, 0);
        for (_, id, _) in clients {
            PROBES.lock().unwrap().remove(&id);
        }
    }
}
