use super::*;

struct CleanupProbe;
#[async_trait]
impl WsConnectionMiddleware for CleanupProbe {
    async fn new(_: Arc<Extensions>) -> Result<Self, crate::middleware::WsMiddlewareInitError> {
        Ok(Self)
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new(
            "shutdown_qualification",
            MiddlewareKind::WebSocketConnection,
        )
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn root_shutdown_reconciles_multiple_connections_peer_race_and_late_admission() {
    for _ in 0..8 {
        qualify_root_shutdown_round().await;
    }
}

async fn qualify_root_shutdown_round() {
    let address = reserve_loopback_address();
    let app = Arc::new(
        WsAppBuilder::new(&address.to_string())
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .connection_middleware::<CleanupProbe>()
            .build()
            .await
            .unwrap(),
    );
    let cancel = CancellationToken::new();
    let runtime = app.clone();
    let source = cancel.clone();
    let root = tokio::spawn(async move { runtime.start_with_cancellation(source).await });
    wait_for_accepting_health(&app).await;
    let mut clients = Vec::new();
    for _ in 0..8 {
        let (client, _) =
            tokio_tungstenite::connect_async(format!("ws://{address}/ws?namespace=orders"))
                .await
                .unwrap();
        clients.push(client);
    }
    timeout(Duration::from_secs(2), async {
        while app.active_connection_count().await != 8 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // One peer begins closing concurrently with application drain.
    let mut peer = clients.pop().unwrap();
    let peer_close = async {
        let _ = peer.close(None).await;
        let _ = timeout(Duration::from_secs(2), peer.next()).await;
    };
    let shutdown = async {
        cancel.cancel();
        timeout(Duration::from_secs(2), async {
            while app.health_snapshot().unwrap().accepting_new_work {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // Readiness/admission has stopped: a new upgrade cannot become work.
        let late = timeout(
            Duration::from_millis(250),
            tokio_tungstenite::connect_async(format!("ws://{address}/ws?namespace=orders")),
        )
        .await;
        assert!(!matches!(late, Ok(Ok(_))));
        for mut client in clients {
            assert!(matches!(
                timeout(Duration::from_secs(2), client.next())
                    .await
                    .unwrap(),
                Some(Ok(Message::Close(_)))
            ));
            let _ = client.flush().await;
        }
    };
    tokio::join!(peer_close, shutdown);
    timeout(Duration::from_secs(3), root)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let report = app.lifecycle.shutdown_report.get().unwrap().clone();
    assert_eq!(
        report.completion,
        lily_shutdown::FrameworkShutdownCompletion::GracefulCompleted
    );
    assert!(report.evidence.quiescent());
    assert!(report.evidence.reconciles());
    assert_eq!(report.evidence.connections.owners, 8);
    assert_eq!(report.evidence.connections.published, 8);
    assert_eq!(report.evidence.connections.workers.completed, 8);
    assert_eq!(
        report.evidence.connections.middleware.termination.completed,
        8
    );
    assert_eq!(report.evidence.connections.middleware.exits_incomplete, 0);
    assert_eq!(app.active_connection_count().await, 0);
    assert_eq!(app.connection_cleanup_registry.entry_count(), 0);
    assert_eq!(
        app.connection_permits.available_permits(),
        app.server_config().max_connections
    );
    assert!(app.lifecycle.root_join_observed.load(Ordering::Acquire));
    app.close().await.unwrap();
    assert_eq!(app.lifecycle.shutdown_report.get().unwrap(), &report);
}
