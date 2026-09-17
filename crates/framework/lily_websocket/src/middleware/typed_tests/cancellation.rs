use super::*;
use crate::guard::{GuardChain, GuardInitializationError, WebSocketGuardRejection, WsGuard};

#[derive(Default)]
struct ExecutionProbe {
    cooperative: bool,
    views: StdMutex<Vec<ExecutionCancellation>>,
}

impl ExecutionProbe {
    async fn cooperate(&self, signal: &ExecutionCancellation) {
        if self.cooperative {
            signal.cancelled().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn observe(&self, parameter: ExecutionCancellation, context: Option<&ExecutionCancellation>) {
        let mut views = self.views.lock().unwrap();
        views.push(parameter);
        if let Some(context) = context {
            views.push(context.clone());
        }
    }
}

#[async_trait]
impl WebSocketHandshakeMiddleware for ExecutionProbe {
    async fn new(_: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Ok(Self::default())
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("execution_probe", MiddlewareKind::WebSocketHandshake)
    }
    async fn handle(
        &self,
        exchange: &mut WsHandshakeExchange,
        cancellation: ExecutionCancellation,
    ) -> Result<(), WsHandshakeRejection> {
        self.observe(cancellation.clone(), Some(exchange.cancellation()));
        self.cooperate(&cancellation).await;
        Ok(())
    }
}

#[async_trait]
impl WebSocketIdentityMiddleware for ExecutionProbe {
    async fn new(_: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Ok(Self::default())
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("identity_probe", MiddlewareKind::WebSocketHandshake)
    }
    async fn identify(
        &self,
        exchange: &mut WsHandshakeExchange,
        cancellation: ExecutionCancellation,
    ) -> Result<WebSocketIdentity, WsHandshakeRejection> {
        self.observe(cancellation.clone(), Some(exchange.cancellation()));
        self.cooperate(&cancellation).await;
        Ok(WebSocketIdentity::anonymous())
    }
}

#[async_trait]
impl WsConnectionMiddleware for ExecutionProbe {
    async fn new(_: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Ok(Self::default())
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("connection_probe", MiddlewareKind::Custom)
    }
    async fn admit(
        &self,
        _: Arc<WebSocketContext>,
        cancellation: ExecutionCancellation,
    ) -> Result<(), WsMiddlewareError> {
        self.observe(cancellation.clone(), None);
        self.cooperate(&cancellation).await;
        Ok(())
    }
    async fn opened(
        &self,
        _: Arc<WebSocketContext>,
        cancellation: ExecutionCancellation,
    ) -> Result<(), WsMiddlewareError> {
        self.observe(cancellation.clone(), None);
        self.cooperate(&cancellation).await;
        Ok(())
    }
}

#[async_trait]
impl WsMessageMiddleware for ExecutionProbe {
    async fn new(_: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Ok(Self::default())
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("message_probe", MiddlewareKind::Custom)
    }
    async fn before_message(
        &self,
        exchange: &mut WsMessageExchange,
        cancellation: ExecutionCancellation,
    ) -> Result<WsMessageDecision, WsMiddlewareError> {
        self.observe(cancellation.clone(), Some(exchange.cancellation()));
        self.cooperate(&cancellation).await;
        Ok(WsMessageDecision::Continue)
    }
    async fn after_message(
        &self,
        exchange: &mut WsMessageExchange,
        _: WsMessageOutcome,
        cancellation: ExecutionCancellation,
    ) -> Result<WsMessageDecision, WsMiddlewareError> {
        self.observe(cancellation.clone(), Some(exchange.cancellation()));
        self.cooperate(&cancellation).await;
        Ok(WsMessageDecision::Continue)
    }
}

#[async_trait]
impl WsGuard for ExecutionProbe {
    async fn new(_: Arc<Extensions>) -> Result<Self, GuardInitializationError> {
        Ok(Self::default())
    }
    async fn can_activate(
        &self,
        exchange: &mut WsMessageExchange,
        cancellation: ExecutionCancellation,
    ) -> Result<(), WebSocketGuardRejection> {
        self.observe(cancellation.clone(), Some(exchange.cancellation()));
        self.cooperate(&cancellation).await;
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn accepted_handshake_identity_admit_and_opened_can_return_cooperatively() {
    let container = ApplicationContainer::build().await.unwrap();
    for stage in 0..4 {
        let source = CancellationToken::new();
        let callback_source = source.clone();
        let probe = Arc::new(ExecutionProbe {
            cooperative: true,
            ..Default::default()
        });
        let callback = Arc::clone(&probe);
        let services = container.services();
        let operation = async move {
            let mut handshake = WsHandshakeExchange::new(
                services,
                handshake_request("test", WsHeaders::new(), "127.0.0.1:1".parse().unwrap()),
                callback_source.clone(),
                tokio::time::Instant::now() + Duration::from_secs(1),
            );
            match stage {
                0 => CompiledWsHandshakeChain::compile(vec![callback])
                    .unwrap()
                    .execute(&mut handshake, Duration::from_secs(1))
                    .await
                    .unwrap(),
                1 => {
                    CompiledWsIdentityMiddleware::compile_observed(
                        callback,
                        Arc::new(NoopWsMiddlewareObserver),
                    )
                    .unwrap()
                    .execute(&mut handshake, Duration::from_secs(1))
                    .await
                    .unwrap();
                }
                _ => {
                    let chain = CompiledWsConnectionChain::compile(vec![callback]).unwrap();
                    let ledger = WsConnectionLedger::new();
                    if stage == 2 {
                        chain
                            .admit(context(), &ledger, Duration::from_secs(1), &callback_source)
                            .await
                            .unwrap();
                    } else {
                        assert!(ledger.record_entered(1));
                        chain
                            .opened(context(), &ledger, Duration::from_secs(1), &callback_source)
                            .await
                            .unwrap();
                    }
                }
            }
        };
        tokio::pin!(operation);
        assert!(matches!(
            futures_util::poll!(operation.as_mut()),
            std::task::Poll::Pending
        ));
        assert!(!probe.views.lock().unwrap().is_empty());
        source.cancel();
        assert!(
            matches!(
                futures_util::poll!(operation.as_mut()),
                std::task::Poll::Pending
            ),
            "signal must not drop accepted callback at stage {stage}"
        );
        let start = tokio::time::Instant::now();
        operation.await;
        assert!(start.elapsed() <= Duration::from_millis(6));
    }
    container.close().await.unwrap();
}

#[tokio::test]
async fn callback_parameters_and_contexts_observe_the_same_execution_source() {
    let source = CancellationToken::new();
    let probe = Arc::new(ExecutionProbe::default());
    let container = ApplicationContainer::build().await.unwrap();
    let mut handshake = WsHandshakeExchange::new(
        container.services(),
        handshake_request("test", WsHeaders::new(), "127.0.0.1:1".parse().unwrap()),
        source.clone(),
        tokio::time::Instant::now() + Duration::from_secs(1),
    );
    CompiledWsHandshakeChain::compile(vec![probe.clone()])
        .unwrap()
        .execute(&mut handshake, Duration::from_secs(1))
        .await
        .unwrap();
    CompiledWsIdentityMiddleware::compile_observed(
        probe.clone(),
        Arc::new(NoopWsMiddlewareObserver),
    )
    .unwrap()
    .execute(&mut handshake, Duration::from_secs(1))
    .await
    .unwrap();
    let connection_chain = CompiledWsConnectionChain::compile(vec![probe.clone()]).unwrap();
    let connection_ledger = WsConnectionLedger::new();
    connection_chain
        .admit(
            context(),
            &connection_ledger,
            Duration::from_secs(1),
            &source,
        )
        .await
        .unwrap();
    connection_chain
        .opened(
            context(),
            &connection_ledger,
            Duration::from_secs(1),
            &source,
        )
        .await
        .unwrap();
    let message_chain = CompiledWsMessageChain::compile(vec![probe.clone()]).unwrap();
    let mut exchange = message_exchange(source.clone());
    let (mut ledger, decision) = message_chain.before(&mut exchange).await;
    assert_eq!(decision.unwrap(), WsMessageDecision::Continue);
    let guards: Vec<Arc<dyn WsGuard>> = vec![probe.clone()];
    GuardChain::new(&guards)
        .execute(&mut exchange)
        .await
        .unwrap();
    let report = message_chain
        .after(&mut exchange, &mut ledger, WsMessageOutcome::Handled)
        .await;
    assert_eq!(report.outcome(), WsMessageOutcome::Handled);
    {
        let views = probe.views.lock().unwrap();
        assert_eq!(views.len(), 12);
        assert!(views.iter().all(|view| !view.is_cancelled()));
        source.cancel();
        assert!(views.iter().all(ExecutionCancellation::is_cancelled));
    }
    connection_chain
        .cleanup(
            context(),
            &connection_ledger,
            WsConnectionCloseCategory::NormalPeer,
            Duration::from_secs(1),
            &CancellationToken::new(),
        )
        .await;
    container.close().await.unwrap();
}

#[tokio::test]
async fn message_execution_uses_the_exchange_source_for_its_cancellation_boundary() {
    let source = CancellationToken::new();
    let probe = Arc::new(ExecutionProbe::default());
    let chain = CompiledWsMessageChain::compile(vec![probe.clone()]).unwrap();
    let mut exchange = message_exchange(source.clone());
    source.cancel();
    let (ledger, result) = chain.before(&mut exchange).await;
    assert_eq!(result.unwrap(), WsMessageDecision::Continue);
    assert_eq!(ledger.entered(), 1);
    let views = probe.views.lock().unwrap();
    assert_eq!(views.len(), 2);
    assert!(views.iter().all(ExecutionCancellation::is_cancelled));
}

struct CleanupProbe {
    name: &'static str,
    pending: bool,
    views: Arc<StdMutex<Vec<(CleanupCancellation, bool)>>>,
}

#[async_trait]
impl WsConnectionMiddleware for CleanupProbe {
    async fn new(_: Arc<Extensions>) -> Result<Self, WsMiddlewareInitError> {
        Err(WsMiddlewareInitError::Internal)
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new(self.name, MiddlewareKind::Custom)
    }
    async fn closed(
        &self,
        _: Arc<WebSocketContext>,
        _: WsConnectionCloseCategory,
        cancellation: CleanupCancellation,
    ) -> Result<(), WsMiddlewareError> {
        let initially_cancelled = cancellation.is_cancelled();
        self.views
            .lock()
            .unwrap()
            .push((cancellation, initially_cancelled));
        if self.pending {
            pending().await
        } else {
            Ok(())
        }
    }
}

#[tokio::test(start_paused = true)]
async fn cleanup_local_timeout_isolated_from_siblings_and_execution_with_root_propagation() {
    for force in [false, true] {
        let execution = CancellationToken::new();
        let cleanup_root = CancellationToken::new();
        let views = Arc::new(StdMutex::new(Vec::new()));
        let chain = CompiledWsConnectionChain::compile(vec![
            Arc::new(CleanupProbe {
                name: "outer",
                pending: false,
                views: Arc::clone(&views),
            }),
            Arc::new(CleanupProbe {
                name: "inner",
                pending: true,
                views: Arc::clone(&views),
            }),
        ])
        .unwrap();
        let ledger = WsConnectionLedger::new();
        chain
            .admit(context(), &ledger, Duration::from_secs(1), &execution)
            .await
            .unwrap();
        execution.cancel();
        if force {
            cleanup_root.cancel();
        }
        let report = chain
            .cleanup(
                context(),
                &ledger,
                WsConnectionCloseCategory::ServerShutdown,
                Duration::from_millis(10),
                &cleanup_root,
            )
            .await;
        if force {
            assert_eq!(report.completed(), 0);
            assert_eq!(report.cancelled(), 2);
            assert!(
                views.lock().unwrap().is_empty(),
                "expired cleanup authority cannot start fresh hooks"
            );
            continue;
        }
        assert_eq!(report.attempted(), 2);
        assert_eq!(report.completed(), 1);
        assert_eq!(report.cancelled(), usize::from(force));
        assert_eq!(report.timed_out(), usize::from(!force));
        let views = views.lock().unwrap();
        assert_eq!(views.len(), 2);
        assert!(
            views
                .iter()
                .all(|(_, initially_cancelled)| *initially_cancelled == force)
        );
        assert!(views.iter().all(|(view, _)| view.is_cancelled()));
        assert_eq!(cleanup_root.is_cancelled(), force);
    }
}
