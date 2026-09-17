use super::*;
use crate::middleware::{WsMessageTerminationContext, WsMiddlewareError, WsMiddlewareInitError};
use crate::{CleanupCancellation, ExecutionCancellation, NoReply};

static PROBES: std::sync::LazyLock<StdMutex<HashMap<Uuid, Arc<Probe>>>> =
    std::sync::LazyLock::new(|| StdMutex::new(HashMap::new()));

struct Probe {
    gate: &'static str,
    started: Semaphore,
    release: Semaphore,
    force: bool,
    cancellation_observed: AtomicBool,
    other: Uuid,
    events: StdMutex<Vec<&'static str>>,
}

fn probe(context: &WebSocketContext) -> Arc<Probe> {
    PROBES.lock().unwrap()[&context.connection_id()].clone()
}

async fn visit(
    context: &WebSocketContext,
    stage: &'static str,
    signal: Option<ExecutionCancellation>,
) {
    let probe = probe(context);
    if probe.gate == stage {
        probe.started.add_permits(1);
        if probe.force {
            signal.unwrap().cancelled().await;
            let result = context
                .clients()
                .client(probe.other)
                .send("outbound-phase7:probe", stage)
                .await;
            result.expect("accepted execution can send during cooperation");
            probe.cancellation_observed.store(true, Ordering::SeqCst);
            std::future::pending::<()>().await;
        } else {
            probe.release.acquire().await.unwrap().forget();
        }
    }
    context
        .clients()
        .client(probe.other)
        .send("outbound-phase7:probe", stage)
        .await
        .unwrap();
    probe.events.lock().unwrap().push(stage);
}

struct ConnectionOutbound;
#[async_trait]
impl WsConnectionMiddleware for ConnectionOutbound {
    async fn new(_: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Ok(Self)
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("phase7_connection", MiddlewareKind::WebSocketConnection)
    }
    async fn admit(
        &self,
        context: Arc<WebSocketContext>,
        signal: ExecutionCancellation,
    ) -> Result<(), WsMiddlewareError> {
        visit(&context, "admit", Some(signal)).await;
        Ok(())
    }
    async fn opened(
        &self,
        context: Arc<WebSocketContext>,
        signal: ExecutionCancellation,
    ) -> Result<(), WsMiddlewareError> {
        visit(&context, "opened", Some(signal)).await;
        Ok(())
    }
    async fn closed(
        &self,
        context: Arc<WebSocketContext>,
        _: crate::middleware::WsConnectionCloseCategory,
        signal: CleanupCancellation,
    ) -> Result<(), WsMiddlewareError> {
        assert!(!signal.is_cancelled());
        visit(&context, "closed", None).await;
        Ok(())
    }
}

struct MessageOutbound;
#[async_trait]
impl WsMessageMiddleware for MessageOutbound {
    async fn new(_: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Ok(Self)
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("phase7_message", MiddlewareKind::WebSocketMessage)
    }
    async fn before_message(
        &self,
        exchange: &mut WsMessageExchange,
        signal: ExecutionCancellation,
    ) -> Result<WsMessageDecision, WsMiddlewareError> {
        visit(exchange.connection(), "before", Some(signal)).await;
        Ok(WsMessageDecision::Continue)
    }
    async fn after_message(
        &self,
        exchange: &mut WsMessageExchange,
        _: WsMessageOutcome,
        signal: ExecutionCancellation,
    ) -> Result<WsMessageDecision, WsMiddlewareError> {
        visit(exchange.connection(), "after", Some(signal)).await;
        Ok(WsMessageDecision::Continue)
    }
    async fn on_message_termination(
        &self,
        context: WsMessageTerminationContext<'_>,
        signal: CleanupCancellation,
    ) -> Result<(), WsMiddlewareError> {
        assert!(!signal.is_cancelled());
        visit(context.connection(), "termination", None).await;
        Ok(())
    }
}

struct GuardOutbound;
#[async_trait]
impl crate::guard::WsGuard for GuardOutbound {
    async fn new(_: Arc<Extensions>) -> Result<Self, crate::guard::GuardInitializationError> {
        Ok(Self)
    }
    async fn can_activate(
        &self,
        exchange: &mut WsMessageExchange,
        signal: ExecutionCancellation,
    ) -> Result<(), crate::guard::WebSocketGuardRejection> {
        visit(exchange.connection(), "guard", Some(signal)).await;
        Ok(())
    }
}

#[derive(crate::WebSocketController)]
#[namespace("outbound-phase7")]
#[connection_middleware(ConnectionOutbound)]
#[message_middleware(MessageOutbound)]
#[guard(GuardOutbound)]
struct OutboundController;

#[async_trait]
impl crate::WebSocketControllerTrait for OutboundController {
    async fn new(_: Arc<Extensions>) -> Result<Self, crate::WebSocketControllerInitError> {
        Ok(Self)
    }
}

#[crate::websocket_controller]
impl OutboundController {
    #[connected]
    async fn connected(
        &self,
        context: WebSocketContext,
        signal: ExecutionCancellation,
    ) -> Result<(), crate::WebSocketLifecycleError> {
        visit(&context, "connected", Some(signal)).await;
        Ok(())
    }
    #[message("run")]
    async fn run(
        &self,
        context: WebSocketContext,
        signal: ExecutionCancellation,
    ) -> Result<NoReply, WebSocketActionError> {
        visit(&context, "action", Some(signal)).await;
        context
            .clients()
            .caller()
            .send("outbound-phase7:reply", "completed")
            .await
            .unwrap();
        Ok(NoReply)
    }
    #[disconnected]
    async fn disconnected(
        &self,
        context: WebSocketContext,
        signal: CleanupCancellation,
    ) -> Result<(), crate::WebSocketLifecycleError> {
        assert!(!signal.is_cancelled());
        assert!(
            context
                .clients()
                .caller()
                .send("outbound-phase7:left", "bye")
                .await
                .is_err()
        );
        visit(&context, "disconnected", None).await;
        Ok(())
    }
}

#[tokio::test]
async fn admitted_callbacks_send_during_drain_and_forced_termination_uses_cleanup_authority() {
    for (stage, force) in [
        ("admit", false),
        ("opened", false),
        ("connected", false),
        ("before", false),
        ("guard", false),
        ("action", false),
        ("after", false),
        ("action", true),
    ] {
        let app = WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .build()
            .await
            .unwrap();
        let id = Uuid::new_v4();
        let other = Uuid::new_v4();
        let (tx, mut rx) = mpsc::channel(16);
        app.connection_manager
            .add_connection(other, tx, None, Some("outbound-phase7".into()))
            .await
            .unwrap();
        let probe = Arc::new(Probe {
            gate: stage,
            started: Semaphore::new(0),
            release: Semaphore::new(0),
            force,
            cancellation_observed: AtomicBool::new(false),
            other,
            events: StdMutex::new(Vec::new()),
        });
        PROBES.lock().unwrap().insert(id, probe.clone());
        let (server_io, client_io) = duplex(32 * 1024);
        let execution_cancel = CancellationToken::new();
        let message_admission = CancellationToken::new();
        let server = tokio::spawn(WsApp::handle_connection(
            server_io,
            app.connection_runtime(),
            AcceptedConnection {
                addr: "127.0.0.1:12345".parse().unwrap(),
                connection_id: id,
                permit: app.connection_permits.clone().try_acquire_owned().unwrap(),
                span: tracing::info_span!("test.websocket.phase7"),
            },
            None,
            CancellationToken::new(),
            message_admission.clone(),
            execution_cancel.clone(),
        ));
        let (mut client, _) = tokio_tungstenite::client_async(
            "ws://localhost/ws?namespace=outbound-phase7",
            client_io,
        )
        .await
        .unwrap();
        let has_message = !matches!(stage, "admit" | "opened" | "connected");
        if has_message {
            client
                .send(
                    crate::WsMessageBody::try_new("outbound-phase7:run", serde_json::Value::Null)
                        .unwrap()
                        .to_message()
                        .unwrap(),
                )
                .await
                .unwrap();
        }
        timeout(Duration::from_secs(2), probe.started.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        if has_message {
            // A queued later message must not borrow the admitted invocation's
            // continuation when the dispatcher switches to drain.
            client
                .send(
                    crate::WsMessageBody::try_new("outbound-phase7:run", serde_json::Value::Null)
                        .unwrap()
                        .to_message()
                        .unwrap(),
                )
                .await
                .unwrap();
        }
        app.dispatcher.begin_drain();
        message_admission.cancel();
        // External work must be rejected without hitting any target queue.
        assert!(
            app.dispatcher
                .dispatch(crate::BroadcastMessage {
                    target: crate::BroadcastTarget::NamespaceConnections {
                        namespace: "outbound-phase7".to_owned(),
                        connection_ids: vec![other]
                    },
                    message: crate::WsMessageBody::try_new("outbound-phase7:external", ()).unwrap(),
                    wire_format: crate::WsWireFormat::Text,
                    exclude: Vec::new(),
                })
                .await
                .is_err()
        );
        if force {
            app.scope_cleanup_registry
                .budget
                .force_before(Instant::now() + Duration::from_millis(400));
            execution_cancel.cancel();
            app.dispatcher.force_drain();
        } else {
            probe.release.add_permits(1);
        }
        let mut reply = false;
        timeout(Duration::from_secs(2), async {
            while let Some(frame) = client.next().await {
                match frame {
                    Ok(Message::Text(text)) => {
                        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                        if value["event"] == "outbound-phase7:reply" {
                            reply = true;
                        }
                    }
                    Ok(Message::Close(_)) => {
                        let _ = client.flush().await;
                        break;
                    }
                    Err(_) => break,
                    _ => {}
                }
            }
        })
        .await
        .unwrap();
        timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let expected: &[&str] = if force {
            &[
                "admit",
                "opened",
                "connected",
                "before",
                "guard",
                "termination",
                "disconnected",
                "closed",
            ]
        } else if has_message {
            &[
                "admit",
                "opened",
                "connected",
                "before",
                "guard",
                "action",
                "after",
                "disconnected",
                "closed",
            ]
        } else {
            &["admit", "opened", "connected", "disconnected", "closed"]
        };
        assert_eq!(
            *probe.events.lock().unwrap(),
            expected,
            "stage={stage} force={force}"
        );
        assert_eq!(probe.cancellation_observed.load(Ordering::SeqCst), force);
        assert_eq!(
            reply,
            has_message && !force,
            "admitted reply precedes transport close"
        );
        let mut delivered = Vec::new();
        while let Ok(Message::Text(text)) = rx.try_recv() {
            let value: serde_json::Value = serde_json::from_str(&text).unwrap();
            delivered.push(value["data"].as_str().unwrap().to_owned());
        }
        let mut expected_deliveries = expected.to_vec();
        if force {
            let at = expected_deliveries
                .iter()
                .position(|value| *value == "termination")
                .unwrap();
            expected_deliveries.insert(at, stage);
        }
        assert_eq!(delivered, expected_deliveries);
        assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
        assert_eq!(
            app.message_dispatch_registry.test_snapshot().active_tasks,
            0
        );
        app.dispatcher.wait_drained().await;
        app.connection_manager
            .remove_connection(other)
            .await
            .unwrap();
        app.dispatcher.close_backplane().await.unwrap();
        app.container.close().await.unwrap();
        PROBES.lock().unwrap().remove(&id);
    }
}
