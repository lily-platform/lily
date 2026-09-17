use super::*;

use std::collections::VecDeque;
use std::future::pending;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use lily_monitoring::{HealthCheckKind, HealthCriticality};
use lily_shutdown::ShutdownState;
use serde_json::json;
use tokio::sync::{Mutex, Notify, mpsc};
use tokio::time::{Duration, timeout};

fn test_health(requirement: BackplaneRequirement) -> HealthRegistry {
    let health = HealthRegistry::new(Arc::new(ShutdownState::new()));
    let criticality = match requirement {
        BackplaneRequirement::Required => HealthCriticality::Critical,
        BackplaneRequirement::Optional => HealthCriticality::NonCritical,
    };
    for name in [
        WS_BACKPLANE_PUBLISHER_HEALTH_CHECK,
        WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
    ] {
        health
            .register(name, HealthCheckKind::Dependency, criticality)
            .unwrap();
        health
            .update(name, HealthStatus::Healthy, "available")
            .unwrap();
    }
    health
}

fn active_dispatcher(
    manager: Arc<ConnectionManager>,
    backplane: Arc<dyn WebSocketBackplane>,
    health: HealthRegistry,
) -> WebSocketDispatcher {
    WebSocketDispatcher::active(
        manager,
        BackplaneRequirement::Required,
        backplane,
        Duration::from_secs(1),
        health,
    )
}

fn message() -> BroadcastMessage {
    BroadcastMessage {
        target: BroadcastTarget::Namespace("orders".to_owned()),
        message: WsMessageBody::try_new("orders:created", json!({ "id": "42" })).unwrap(),
        wire_format: WsWireFormat::Text,
        exclude: Vec::new(),
    }
}

fn health_check(health: &HealthRegistry, name: &str) -> lily_monitoring::HealthCheckSnapshot {
    health
        .snapshot()
        .unwrap()
        .checks
        .into_iter()
        .find(|check| check.name == name)
        .expect("registered health check")
}

async fn wait_for_health(health: &HealthRegistry, name: &str, status: HealthStatus, reason: &str) {
    timeout(Duration::from_secs(1), async {
        loop {
            let check = health_check(health, name);
            if check.status == status && check.reason_code == reason {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("health transition timed out");
}

struct ErrorPublishBackplane {
    kind: WebSocketBackplaneErrorKind,
}

#[async_trait]
impl WebSocketBackplane for ErrorPublishBackplane {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
        unreachable!()
    }

    async fn publish(
        &self,
        _frame: WebSocketBackplaneFrame,
    ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
        Err(WebSocketBackplaneError::new(self.kind))
    }

    async fn receive(
        &self,
        _admission: WebSocketBackplaneInboundAdmission,
    ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
        pending().await
    }
}

struct PanickingPublishBackplane;

#[async_trait]
impl WebSocketBackplane for PanickingPublishBackplane {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
        unreachable!()
    }

    async fn publish(
        &self,
        _frame: WebSocketBackplaneFrame,
    ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
        panic!("intentional publish panic")
    }

    async fn receive(
        &self,
        _admission: WebSocketBackplaneInboundAdmission,
    ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
        pending().await
    }
}

struct RecoveringPublishBackplane {
    outcomes: Mutex<VecDeque<Result<(), WebSocketBackplaneErrorKind>>>,
}

#[async_trait]
impl WebSocketBackplane for RecoveringPublishBackplane {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
        unreachable!()
    }

    async fn publish(
        &self,
        _frame: WebSocketBackplaneFrame,
    ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
        match self
            .outcomes
            .lock()
            .await
            .pop_front()
            .expect("scripted publish outcome")
        {
            Ok(()) => Ok(WebSocketBackplanePublishReceipt::accepted()),
            Err(kind) => Err(WebSocketBackplaneError::new(kind)),
        }
    }

    async fn receive(
        &self,
        _admission: WebSocketBackplaneInboundAdmission,
    ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
        pending().await
    }
}

struct PendingPublishBackplane {
    started: Notify,
    dropped: AtomicBool,
}

impl PendingPublishBackplane {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            started: Notify::new(),
            dropped: AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl WebSocketBackplane for PendingPublishBackplane {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
        unreachable!()
    }

    async fn publish(
        &self,
        _frame: WebSocketBackplaneFrame,
    ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
        struct DropEvidence<'a>(&'a AtomicBool);
        impl Drop for DropEvidence<'_> {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let _evidence = DropEvidence(&self.dropped);
        self.started.notify_one();
        pending().await
    }

    async fn receive(
        &self,
        _admission: WebSocketBackplaneInboundAdmission,
    ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
        pending().await
    }
}

struct ScriptedReceiveBackplane {
    events: Mutex<mpsc::Receiver<Result<WebSocketBackplaneEvent, WebSocketBackplaneError>>>,
}

impl ScriptedReceiveBackplane {
    fn channel() -> (
        Arc<Self>,
        mpsc::Sender<Result<WebSocketBackplaneEvent, WebSocketBackplaneError>>,
    ) {
        let (sender, receiver) = mpsc::channel(8);
        (
            Arc::new(Self {
                events: Mutex::new(receiver),
            }),
            sender,
        )
    }
}

#[async_trait]
impl WebSocketBackplane for ScriptedReceiveBackplane {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
        unreachable!()
    }

    async fn publish(
        &self,
        _frame: WebSocketBackplaneFrame,
    ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
        Ok(WebSocketBackplanePublishReceipt::accepted())
    }

    async fn receive(
        &self,
        _admission: WebSocketBackplaneInboundAdmission,
    ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
        self.events.lock().await.recv().await.unwrap_or_else(|| {
            Err(WebSocketBackplaneError::new(
                WebSocketBackplaneErrorKind::Receive,
            ))
        })
    }
}

struct PanickingReceiveBackplane;

#[async_trait]
impl WebSocketBackplane for PanickingReceiveBackplane {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
        unreachable!()
    }

    async fn publish(
        &self,
        _frame: WebSocketBackplaneFrame,
    ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
        Ok(WebSocketBackplanePublishReceipt::accepted())
    }

    async fn receive(
        &self,
        _admission: WebSocketBackplaneInboundAdmission,
    ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
        panic!("intentional receive panic")
    }
}

struct CloseFaultBackplane {
    panic: bool,
    close_calls: AtomicUsize,
    close_started: Notify,
    close_release: Notify,
}

impl CloseFaultBackplane {
    fn new(panic: bool) -> Arc<Self> {
        Arc::new(Self {
            panic,
            close_calls: AtomicUsize::new(0),
            close_started: Notify::new(),
            close_release: Notify::new(),
        })
    }
}

#[async_trait]
impl WebSocketBackplane for CloseFaultBackplane {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
        unreachable!()
    }

    async fn publish(
        &self,
        _frame: WebSocketBackplaneFrame,
    ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
        Ok(WebSocketBackplanePublishReceipt::accepted())
    }

    async fn receive(
        &self,
        _admission: WebSocketBackplaneInboundAdmission,
    ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
        pending().await
    }

    async fn close(&self) -> Result<(), WebSocketBackplaneError> {
        self.close_calls.fetch_add(1, Ordering::AcqRel);
        self.close_started.notify_one();
        self.close_release.notified().await;
        if self.panic {
            panic!("intentional close panic");
        }
        Err(WebSocketBackplaneError::new(
            WebSocketBackplaneErrorKind::Shutdown,
        ))
    }
}

#[tokio::test]
async fn publish_error_kinds_preserve_local_report_metrics_and_health() {
    for kind in [
        WebSocketBackplaneErrorKind::Unavailable,
        WebSocketBackplaneErrorKind::Publish,
        WebSocketBackplaneErrorKind::Transport,
    ] {
        let manager = Arc::new(ConnectionManager::new());
        let health = test_health(BackplaneRequirement::Required);
        let dispatcher = active_dispatcher(
            Arc::clone(&manager),
            Arc::new(ErrorPublishBackplane { kind }),
            health.clone(),
        );

        let error = dispatcher.dispatch(message()).await.unwrap_err();
        let ConnectionError::BackplanePublish { local, source } = error else {
            panic!("expected typed backplane publish error");
        };
        assert_eq!(local, BroadcastReport::default());
        assert_eq!(source.kind(), kind);

        let metrics = manager.metrics_snapshot();
        if kind == WebSocketBackplaneErrorKind::Unavailable {
            assert_eq!(metrics.backplane_publish_unavailable, 1);
            assert_eq!(metrics.backplane_publish_failed, 0);
        } else {
            assert_eq!(metrics.backplane_publish_unavailable, 0);
            assert_eq!(metrics.backplane_publish_failed, 1);
        }
        let publisher = health_check(&health, WS_BACKPLANE_PUBLISHER_HEALTH_CHECK);
        assert_eq!(publisher.status, HealthStatus::Unhealthy);
        assert_eq!(publisher.reason_code, kind.as_str());
    }
}

#[tokio::test]
async fn publish_panic_is_contained_and_counted_as_a_failure() {
    let manager = Arc::new(ConnectionManager::new());
    let health = test_health(BackplaneRequirement::Required);
    let dispatcher = active_dispatcher(
        Arc::clone(&manager),
        Arc::new(PanickingPublishBackplane),
        health.clone(),
    );

    let error = dispatcher.dispatch(message()).await.unwrap_err();
    let ConnectionError::BackplanePublish { source, .. } = error else {
        panic!("expected typed backplane publish error");
    };
    assert_eq!(source.kind(), WebSocketBackplaneErrorKind::Panicked);
    assert_eq!(manager.metrics_snapshot().backplane_publish_failed, 1);
    let publisher = health_check(&health, WS_BACKPLANE_PUBLISHER_HEALTH_CHECK);
    assert_eq!(publisher.status, HealthStatus::Unhealthy);
    assert_eq!(publisher.reason_code, "panicked");
}

#[tokio::test]
async fn caller_cancellation_drops_publish_and_releases_dispatch_lease() {
    let manager = Arc::new(ConnectionManager::with_registered_namespaces(
        8,
        128,
        ["orders".to_owned()],
    ));
    let connection_id = Uuid::new_v4();
    let (sender, mut receiver) = mpsc::channel(1);
    manager
        .add_connection(connection_id, sender, None, Some("orders".to_owned()))
        .await
        .unwrap();
    let health = test_health(BackplaneRequirement::Required);
    let backplane = PendingPublishBackplane::new();
    let dispatcher = active_dispatcher(
        Arc::clone(&manager),
        Arc::clone(&backplane) as Arc<dyn WebSocketBackplane>,
        health.clone(),
    );
    let task_dispatcher = dispatcher.clone();
    let task = tokio::spawn(async move { task_dispatcher.dispatch(message()).await });
    backplane.started.notified().await;
    assert!(receiver.recv().await.is_some());

    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(backplane.dropped.load(Ordering::Acquire));
    timeout(Duration::from_secs(1), dispatcher.wait_drained())
        .await
        .expect("cancelled publish retained its dispatch lease");
    let metrics = manager.metrics_snapshot();
    assert_eq!(metrics.backplane_publish_accepted, 0);
    assert_eq!(metrics.backplane_publish_failed, 0);
    let publisher = health_check(&health, WS_BACKPLANE_PUBLISHER_HEALTH_CHECK);
    assert_eq!(publisher.status, HealthStatus::Healthy);
    assert_eq!(publisher.reason_code, "available");
}

#[tokio::test]
async fn force_drain_interrupts_pending_publish_and_preserves_completed_local_delivery() {
    let manager = Arc::new(ConnectionManager::with_registered_namespaces(
        8,
        128,
        ["orders".to_owned()],
    ));
    let connection_id = Uuid::new_v4();
    let (sender, mut receiver) = mpsc::channel(2);
    manager
        .add_connection(connection_id, sender, None, Some("orders".to_owned()))
        .await
        .unwrap();
    let backplane = PendingPublishBackplane::new();
    let health = test_health(BackplaneRequirement::Required);
    let dispatcher = active_dispatcher(
        Arc::clone(&manager),
        Arc::clone(&backplane) as Arc<dyn WebSocketBackplane>,
        health.clone(),
    );
    let task_dispatcher = dispatcher.clone();
    let task = tokio::spawn(async move { task_dispatcher.dispatch(message()).await });
    backplane.started.notified().await;
    assert!(receiver.recv().await.is_some());

    dispatcher.force_drain();
    let error = task.await.unwrap().unwrap_err();
    let ConnectionError::BackplanePublish { local, source } = error else {
        panic!("expected interrupted publish");
    };
    assert_eq!(local.sent, 1);
    assert_eq!(source.kind(), WebSocketBackplaneErrorKind::Interrupted);
    assert!(backplane.dropped.load(Ordering::Acquire));
    timeout(Duration::from_secs(1), dispatcher.wait_drained())
        .await
        .expect("forced publish retained its dispatch lease");
    assert_eq!(manager.metrics_snapshot().backplane_publish_failed, 1);
    let publisher = health_check(&health, WS_BACKPLANE_PUBLISHER_HEALTH_CHECK);
    assert_eq!(publisher.status, HealthStatus::Unhealthy);
    assert_eq!(publisher.reason_code, "interrupted");
}

#[tokio::test]
async fn successful_publish_after_failure_recovers_publisher_health() {
    let manager = Arc::new(ConnectionManager::new());
    let health = test_health(BackplaneRequirement::Required);
    let backplane = Arc::new(RecoveringPublishBackplane {
        outcomes: Mutex::new(VecDeque::from([
            Err(WebSocketBackplaneErrorKind::Transport),
            Ok(()),
        ])),
    });
    let dispatcher = active_dispatcher(manager.clone(), backplane, health.clone());

    dispatcher.dispatch(message()).await.unwrap_err();
    let failed = health_check(&health, WS_BACKPLANE_PUBLISHER_HEALTH_CHECK);
    assert_eq!(failed.status, HealthStatus::Unhealthy);
    assert_eq!(failed.reason_code, "transport_failed");

    dispatcher.dispatch(message()).await.unwrap();
    let recovered = health_check(&health, WS_BACKPLANE_PUBLISHER_HEALTH_CHECK);
    assert_eq!(recovered.status, HealthStatus::Healthy);
    assert_eq!(recovered.reason_code, "available");
    let metrics = manager.metrics_snapshot();
    assert_eq!(metrics.backplane_publish_failed, 1);
    assert_eq!(metrics.backplane_publish_accepted, 1);
}

#[tokio::test]
async fn receive_error_before_ready_stops_required_subscription() {
    let (backplane, events) = ScriptedReceiveBackplane::channel();
    let health = test_health(BackplaneRequirement::Required);
    let dispatcher = active_dispatcher(
        Arc::new(ConnectionManager::new()),
        backplane,
        health.clone(),
    );
    let ingress = dispatcher.spawn_ingress().unwrap();
    events
        .send(Err(WebSocketBackplaneError::new(
            WebSocketBackplaneErrorKind::Unavailable,
        )))
        .await
        .unwrap();

    wait_for_health(
        &health,
        WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
        HealthStatus::Unhealthy,
        "unavailable",
    )
    .await;
    let error = dispatcher.wait_for_subscription_ready().await.unwrap_err();
    assert_eq!(error.kind(), WebSocketBackplaneErrorKind::Receive);
    ingress.stop().await.unwrap();
    dispatcher.close_backplane().await.unwrap();
}

#[tokio::test]
async fn receive_error_after_ready_revokes_subscription_health() {
    let (backplane, events) = ScriptedReceiveBackplane::channel();
    let health = test_health(BackplaneRequirement::Required);
    let dispatcher = active_dispatcher(
        Arc::new(ConnectionManager::new()),
        backplane,
        health.clone(),
    );
    let ingress = dispatcher.spawn_ingress().unwrap();
    events
        .send(Ok(WebSocketBackplaneEvent::SubscriptionReady))
        .await
        .unwrap();
    dispatcher.wait_for_subscription_ready().await.unwrap();
    events
        .send(Err(WebSocketBackplaneError::new(
            WebSocketBackplaneErrorKind::Transport,
        )))
        .await
        .unwrap();

    wait_for_health(
        &health,
        WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
        HealthStatus::Unhealthy,
        "transport_failed",
    )
    .await;
    ingress.stop().await.unwrap();
    dispatcher.close_backplane().await.unwrap();
}

#[tokio::test]
async fn receive_panic_is_contained_and_stops_required_subscription() {
    let health = test_health(BackplaneRequirement::Required);
    let dispatcher = active_dispatcher(
        Arc::new(ConnectionManager::new()),
        Arc::new(PanickingReceiveBackplane),
        health.clone(),
    );
    let ingress = dispatcher.spawn_ingress().unwrap();

    wait_for_health(
        &health,
        WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
        HealthStatus::Unhealthy,
        "receive_panicked",
    )
    .await;
    let error = dispatcher.wait_for_subscription_ready().await.unwrap_err();
    assert_eq!(error.kind(), WebSocketBackplaneErrorKind::Receive);
    ingress.stop().await.unwrap();
    dispatcher.close_backplane().await.unwrap();
}

async fn assert_close_fault(panic: bool, expected: WebSocketBackplaneErrorKind) {
    let backplane = CloseFaultBackplane::new(panic);
    let health = test_health(BackplaneRequirement::Required);
    let dispatcher = active_dispatcher(
        Arc::new(ConnectionManager::new()),
        Arc::clone(&backplane) as Arc<dyn WebSocketBackplane>,
        health.clone(),
    );
    let first_dispatcher = dispatcher.clone();
    let first = tokio::spawn(async move { first_dispatcher.close_backplane().await });
    timeout(Duration::from_secs(1), backplane.close_started.notified())
        .await
        .expect("first close did not reach the provider");
    let second_dispatcher = dispatcher.clone();
    let second = tokio::spawn(async move { second_dispatcher.close_backplane().await });
    backplane.close_release.notify_one();

    let first = timeout(Duration::from_secs(1), first)
        .await
        .expect("first close waiter did not finish")
        .unwrap();
    let second = timeout(Duration::from_secs(1), second)
        .await
        .expect("second close waiter did not finish")
        .unwrap();
    for result in [first, second] {
        assert_eq!(result.unwrap_err().kind(), expected);
    }
    assert_eq!(
        timeout(Duration::from_secs(1), dispatcher.close_backplane())
            .await
            .expect("terminal close result was not replayed")
            .unwrap_err()
            .kind(),
        expected
    );
    assert_eq!(backplane.close_calls.load(Ordering::Acquire), 1);
    for name in [
        WS_BACKPLANE_PUBLISHER_HEALTH_CHECK,
        WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
    ] {
        let check = health_check(&health, name);
        assert_eq!(check.status, HealthStatus::Unhealthy);
        assert_eq!(check.reason_code, "shutdown_failed");
    }
}

#[tokio::test]
async fn close_error_is_called_once_and_replayed_to_concurrent_callers() {
    assert_close_fault(false, WebSocketBackplaneErrorKind::Shutdown).await;
}

#[tokio::test]
async fn close_panic_is_called_once_and_replayed_as_a_typed_failure() {
    assert_close_fault(true, WebSocketBackplaneErrorKind::Panicked).await;
}

#[tokio::test]
async fn configured_optional_unavailable_preserves_local_delivery_and_observability() {
    let manager = Arc::new(ConnectionManager::with_registered_namespaces(
        8,
        128,
        ["orders".to_owned()],
    ));
    let connection_id = Uuid::new_v4();
    let (sender, mut receiver) = mpsc::channel(1);
    manager
        .add_connection(connection_id, sender, None, Some("orders".to_owned()))
        .await
        .unwrap();
    let health = test_health(BackplaneRequirement::Optional);
    health
        .update(
            WS_BACKPLANE_PUBLISHER_HEALTH_CHECK,
            HealthStatus::Degraded,
            "initialization_failed",
        )
        .unwrap();
    let dispatcher = WebSocketDispatcher::configured_unavailable(
        Arc::clone(&manager),
        BackplaneRequirement::Optional,
        Duration::from_secs(1),
        health.clone(),
    );

    let error = dispatcher.dispatch(message()).await.unwrap_err();
    let ConnectionError::BackplaneUnavailable { local } = error else {
        panic!("expected optional backplane unavailable result");
    };
    assert_eq!(local.sent, 1);
    assert!(receiver.recv().await.is_some());
    assert_eq!(manager.metrics_snapshot().backplane_publish_unavailable, 1);
    let publisher = health_check(&health, WS_BACKPLANE_PUBLISHER_HEALTH_CHECK);
    assert_eq!(publisher.status, HealthStatus::Degraded);
    assert_eq!(publisher.reason_code, "initialization_failed");
}
