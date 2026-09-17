//! Backplane state-machine and concurrency tests.

use super::*;

use std::future::pending;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use lily_monitoring::{HealthCheckKind, HealthCriticality};
use lily_shutdown::ShutdownState;
use serde_json::json;
use tokio::sync::{Mutex, Notify, Semaphore, mpsc};
use tokio::time::{Duration, timeout};
use tokio_tungstenite::tungstenite::Message;

const TEST_TIMEOUT: Duration = Duration::from_secs(2);

fn test_health() -> HealthRegistry {
    let health = HealthRegistry::new(Arc::new(ShutdownState::new()));
    for name in [
        WS_BACKPLANE_PUBLISHER_HEALTH_CHECK,
        WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
    ] {
        health
            .register(
                name,
                HealthCheckKind::Dependency,
                HealthCriticality::Critical,
            )
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
        Duration::from_secs(10),
        health,
    )
}

fn message(target: BroadcastTarget) -> BroadcastMessage {
    BroadcastMessage {
        target,
        message: WsMessageBody::try_new("orders:created", json!({ "id": "42" })).unwrap(),
        wire_format: WsWireFormat::Text,
        exclude: Vec::new(),
    }
}

fn foreign_frame(dispatcher: &WebSocketDispatcher, message_id: Uuid) -> Vec<u8> {
    let mut origin_node_id = Uuid::from_u128(dispatcher.0.node_id.as_u128().wrapping_add(1));
    if origin_node_id.is_nil() {
        origin_node_id = Uuid::from_u128(1);
    }
    debug_assert_ne!(origin_node_id, dispatcher.0.node_id);

    serde_json::to_vec(&BackplaneEnvelope {
        protocol_version: BACKPLANE_PROTOCOL_VERSION,
        message_id,
        origin_node_id,
        target: BackplaneTarget::Namespace {
            namespace: "orders".to_owned(),
        },
        exclusions: Vec::new(),
        message: WsMessageBody::try_new("orders:created", json!({ "id": "42" })).unwrap(),
        wire_format: BackplaneWireFormat::Text,
        traceparent: None,
    })
    .unwrap()
}

async fn wait_until(mut predicate: impl FnMut() -> bool, context: &'static str) {
    timeout(TEST_TIMEOUT, async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for {context}"));
}

async fn wait_for_health(health: &HealthRegistry, name: &str, status: HealthStatus, reason: &str) {
    wait_until(
        || {
            health.snapshot().unwrap().checks.iter().any(|check| {
                check.name == name && check.status == status && check.reason_code == reason
            })
        },
        "backplane health transition",
    )
    .await;
}

struct EventBackplane {
    events: Mutex<mpsc::Receiver<Result<WebSocketBackplaneEvent, WebSocketBackplaneError>>>,
    close_calls: AtomicUsize,
}

impl EventBackplane {
    fn channel() -> (
        Arc<Self>,
        mpsc::Sender<Result<WebSocketBackplaneEvent, WebSocketBackplaneError>>,
    ) {
        let (sender, receiver) = mpsc::channel(16);
        (
            Arc::new(Self {
                events: Mutex::new(receiver),
                close_calls: AtomicUsize::new(0),
            }),
            sender,
        )
    }
}

#[async_trait]
impl WebSocketBackplane for EventBackplane {
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

    async fn close(&self) -> Result<(), WebSocketBackplaneError> {
        self.close_calls.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

async fn manager_with_order_connection() -> (Arc<ConnectionManager>, mpsc::Receiver<Message>) {
    let manager = Arc::new(ConnectionManager::with_registered_namespaces(
        8,
        128,
        ["orders".to_owned()],
    ));
    let (sender, receiver) = mpsc::channel(8);
    manager
        .add_connection(Uuid::new_v4(), sender, None, Some("orders".to_owned()))
        .await
        .unwrap();
    (manager, receiver)
}

#[tokio::test]
async fn frame_before_ready_is_rejected_but_the_same_frame_is_accepted_after_ready() {
    let (manager, mut receiver) = manager_with_order_connection().await;
    let (backplane, events) = EventBackplane::channel();
    let health = test_health();
    let dispatcher = active_dispatcher(
        Arc::clone(&manager),
        Arc::clone(&backplane) as Arc<dyn WebSocketBackplane>,
        health.clone(),
    );
    let ingress = dispatcher.spawn_ingress().unwrap();
    let frame = foreign_frame(&dispatcher, Uuid::new_v4());

    events
        .send(Ok(WebSocketBackplaneEvent::Frame(frame.clone())))
        .await
        .unwrap();
    wait_until(
        || manager.metrics_snapshot().backplane_invalid_frames == 1,
        "pre-ready frame rejection",
    )
    .await;
    assert!(receiver.try_recv().is_err());

    events
        .send(Ok(WebSocketBackplaneEvent::SubscriptionReady))
        .await
        .unwrap();
    wait_for_health(
        &health,
        WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
        HealthStatus::Healthy,
        "connected",
    )
    .await;
    events
        .send(Ok(WebSocketBackplaneEvent::Frame(frame)))
        .await
        .unwrap();
    assert!(matches!(
        timeout(TEST_TIMEOUT, receiver.recv()).await.unwrap(),
        Some(Message::Text(_))
    ));
    assert_eq!(manager.metrics_snapshot().backplane_invalid_frames, 1);

    ingress.stop().await.unwrap();
    dispatcher.close_backplane().await.unwrap();
    assert_eq!(backplane.close_calls.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn reconnecting_gates_frames_and_ready_recovery_accepts_them_again() {
    let (manager, mut receiver) = manager_with_order_connection().await;
    let (backplane, events) = EventBackplane::channel();
    let health = test_health();
    let dispatcher = active_dispatcher(Arc::clone(&manager), backplane, health.clone());
    let ingress = dispatcher.spawn_ingress().unwrap();
    let frame = foreign_frame(&dispatcher, Uuid::new_v4());

    events
        .send(Ok(WebSocketBackplaneEvent::SubscriptionReady))
        .await
        .unwrap();
    wait_for_health(
        &health,
        WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
        HealthStatus::Healthy,
        "connected",
    )
    .await;
    events
        .send(Ok(WebSocketBackplaneEvent::SubscriptionReconnecting))
        .await
        .unwrap();
    wait_for_health(
        &health,
        WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
        HealthStatus::Degraded,
        "reconnecting",
    )
    .await;
    events
        .send(Ok(WebSocketBackplaneEvent::Frame(frame.clone())))
        .await
        .unwrap();
    wait_until(
        || manager.metrics_snapshot().backplane_invalid_frames == 1,
        "reconnecting frame rejection",
    )
    .await;
    assert!(receiver.try_recv().is_err());

    events
        .send(Ok(WebSocketBackplaneEvent::SubscriptionReady))
        .await
        .unwrap();
    wait_for_health(
        &health,
        WS_BACKPLANE_SUBSCRIBER_HEALTH_CHECK,
        HealthStatus::Healthy,
        "connected",
    )
    .await;
    events
        .send(Ok(WebSocketBackplaneEvent::Frame(frame)))
        .await
        .unwrap();
    assert!(matches!(
        timeout(TEST_TIMEOUT, receiver.recv()).await.unwrap(),
        Some(Message::Text(_))
    ));

    ingress.stop().await.unwrap();
    dispatcher.close_backplane().await.unwrap();
}

#[tokio::test]
async fn ingress_is_single_owner_and_terminal_or_closed_dispatchers_cannot_restart_it() {
    let (backplane, events) = EventBackplane::channel();
    let dispatcher =
        active_dispatcher(Arc::new(ConnectionManager::new()), backplane, test_health());
    let ingress = dispatcher.spawn_ingress().unwrap();
    assert!(dispatcher.spawn_ingress().is_none());

    events
        .send(Err(WebSocketBackplaneError::new(
            WebSocketBackplaneErrorKind::Receive,
        )))
        .await
        .unwrap();
    wait_until(
        || dispatcher.0.ingress_running.load(Ordering::Acquire) == 0,
        "terminal ingress completion",
    )
    .await;
    assert_eq!(
        dispatcher.0.subscription_state.load(Ordering::Acquire),
        SUBSCRIPTION_STOPPED
    );
    assert!(dispatcher.spawn_ingress().is_none());
    ingress.stop().await.unwrap();
    dispatcher.close_backplane().await.unwrap();

    let (backplane, _events) = EventBackplane::channel();
    let closed = active_dispatcher(Arc::new(ConnectionManager::new()), backplane, test_health());
    let ingress = closed.spawn_ingress().unwrap();
    closed.close_backplane().await.unwrap();
    assert!(closed.spawn_ingress().is_none());
    ingress.stop().await.unwrap();
}

#[test]
fn dedupe_window_honors_exact_ttl_and_capacity_boundaries() {
    let now = Instant::now();
    let first = Uuid::from_u128(1);
    let mut ttl = DedupeWindow::new();
    assert!(!ttl.seen_or_insert(first, now));
    assert!(ttl.seen_or_insert(
        first,
        now + DEDUPE_TTL.checked_sub(Duration::from_nanos(1)).unwrap()
    ));
    assert!(!ttl.seen_or_insert(first, now + DEDUPE_TTL));

    let mut bounded = DedupeWindow::new();
    let ids = (1..=DEDUPE_CAPACITY)
        .map(|value| Uuid::from_u128(value as u128))
        .collect::<Vec<_>>();
    for id in &ids {
        assert!(!bounded.seen_or_insert(*id, now));
    }
    assert_eq!(bounded.order.len(), DEDUPE_CAPACITY);
    assert_eq!(bounded.ids.len(), DEDUPE_CAPACITY);

    let overflow = Uuid::from_u128((DEDUPE_CAPACITY + 1) as u128);
    assert!(!bounded.seen_or_insert(overflow, now));
    assert_eq!(bounded.order.len(), DEDUPE_CAPACITY);
    assert_eq!(bounded.ids.len(), DEDUPE_CAPACITY);
    assert!(bounded.seen_or_insert(*ids.last().unwrap(), now));
    assert!(!bounded.seen_or_insert(ids[0], now));
}

struct DrainBackplane {
    entered: AtomicUsize,
    entered_notify: Notify,
    release: Semaphore,
}

impl DrainBackplane {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: AtomicUsize::new(0),
            entered_notify: Notify::new(),
            release: Semaphore::new(0),
        })
    }
}

#[async_trait]
impl WebSocketBackplane for DrainBackplane {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
        unreachable!()
    }

    async fn publish(
        &self,
        _frame: WebSocketBackplaneFrame,
    ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
        self.entered.fetch_add(1, Ordering::AcqRel);
        self.entered_notify.notify_waiters();
        self.release.acquire().await.unwrap().forget();
        Ok(WebSocketBackplanePublishReceipt::accepted())
    }

    async fn receive(
        &self,
        _admission: WebSocketBackplaneInboundAdmission,
    ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
        pending().await
    }
}

#[tokio::test]
async fn concurrent_publish_drain_rejects_new_work_and_waits_for_every_admitted_publish() {
    const PUBLISHES: usize = 32;

    let backplane = DrainBackplane::new();
    let dispatcher = active_dispatcher(
        Arc::new(ConnectionManager::new()),
        Arc::clone(&backplane) as Arc<dyn WebSocketBackplane>,
        test_health(),
    );
    let mut dispatches = Vec::with_capacity(PUBLISHES);
    for _ in 0..PUBLISHES {
        let dispatcher = dispatcher.clone();
        dispatches.push(tokio::spawn(async move {
            dispatcher
                .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
                .await
        }));
    }
    wait_until(
        || backplane.entered.load(Ordering::Acquire) == PUBLISHES,
        "all concurrent publishes to enter the provider",
    )
    .await;

    dispatcher.begin_drain();
    assert!(matches!(
        dispatcher
            .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
            .await
            .unwrap_err(),
        ConnectionError::InvalidOperation(
            crate::connection::ConnectionOperationError::DispatcherNotAccepting
        )
    ));
    let drain_dispatcher = dispatcher.clone();
    let drained = tokio::spawn(async move { drain_dispatcher.wait_drained().await });
    tokio::task::yield_now().await;
    assert!(!drained.is_finished());

    backplane.release.add_permits(PUBLISHES);
    for dispatch in dispatches {
        timeout(TEST_TIMEOUT, dispatch)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    timeout(TEST_TIMEOUT, drained).await.unwrap().unwrap();
    dispatcher.close_backplane().await.unwrap();
}

struct ActiveOperationGuard<'a>(&'a AtomicUsize);

impl Drop for ActiveOperationGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

struct OrderedCloseBackplane {
    publish_active: AtomicUsize,
    receive_active: AtomicUsize,
    publish_started: Notify,
    receive_started: Notify,
    publish_release: Semaphore,
    close_calls: AtomicUsize,
    close_raced_operation: AtomicBool,
}

impl OrderedCloseBackplane {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            publish_active: AtomicUsize::new(0),
            receive_active: AtomicUsize::new(0),
            publish_started: Notify::new(),
            receive_started: Notify::new(),
            publish_release: Semaphore::new(0),
            close_calls: AtomicUsize::new(0),
            close_raced_operation: AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl WebSocketBackplane for OrderedCloseBackplane {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
        unreachable!()
    }

    async fn publish(
        &self,
        _frame: WebSocketBackplaneFrame,
    ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
        self.publish_active.fetch_add(1, Ordering::AcqRel);
        let _guard = ActiveOperationGuard(&self.publish_active);
        self.publish_started.notify_one();
        self.publish_release.acquire().await.unwrap().forget();
        Ok(WebSocketBackplanePublishReceipt::accepted())
    }

    async fn receive(
        &self,
        _admission: WebSocketBackplaneInboundAdmission,
    ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
        self.receive_active.fetch_add(1, Ordering::AcqRel);
        let _guard = ActiveOperationGuard(&self.receive_active);
        self.receive_started.notify_one();
        pending().await
    }

    async fn close(&self) -> Result<(), WebSocketBackplaneError> {
        self.close_calls.fetch_add(1, Ordering::AcqRel);
        if self.publish_active.load(Ordering::Acquire) != 0
            || self.receive_active.load(Ordering::Acquire) != 0
        {
            self.close_raced_operation.store(true, Ordering::Release);
        }
        Ok(())
    }
}

#[tokio::test]
async fn close_waits_for_publish_drain_and_receive_future_drop_before_provider_close() {
    let backplane = OrderedCloseBackplane::new();
    let dispatcher = active_dispatcher(
        Arc::new(ConnectionManager::new()),
        Arc::clone(&backplane) as Arc<dyn WebSocketBackplane>,
        test_health(),
    );
    let ingress = dispatcher.spawn_ingress().unwrap();
    timeout(TEST_TIMEOUT, backplane.receive_started.notified())
        .await
        .unwrap();

    let dispatching = dispatcher.clone();
    let dispatch = tokio::spawn(async move {
        dispatching
            .dispatch(message(BroadcastTarget::Namespace("orders".to_owned())))
            .await
    });
    timeout(TEST_TIMEOUT, backplane.publish_started.notified())
        .await
        .unwrap();

    let closing_dispatcher = dispatcher.clone();
    let closing = tokio::spawn(async move { closing_dispatcher.close_backplane().await });
    wait_until(
        || dispatcher.0.dispatch_state.load(Ordering::Acquire) == DISPATCHER_CLOSING,
        "dispatcher close seals continuation admission before draining",
    )
    .await;
    assert_eq!(backplane.close_calls.load(Ordering::Acquire), 0);
    assert!(!closing.is_finished());

    backplane.publish_release.add_permits(1);
    timeout(TEST_TIMEOUT, dispatch)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    timeout(TEST_TIMEOUT, closing)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(backplane.publish_active.load(Ordering::Acquire), 0);
    assert_eq!(backplane.receive_active.load(Ordering::Acquire), 0);
    assert_eq!(backplane.close_calls.load(Ordering::Acquire), 1);
    assert!(!backplane.close_raced_operation.load(Ordering::Acquire));
    ingress.stop().await.unwrap();
}

static DI_AVAILABLE_DURING_BACKPLANE_CLOSE: AtomicBool = AtomicBool::new(false);

struct DiAwareBackplane {
    extensions: Arc<Extensions>,
}

#[async_trait]
impl WebSocketBackplane for DiAwareBackplane {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
        Ok(Self { extensions })
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
        let available = self
            .extensions
            .get_service::<lily_config::ConfigService>(None)
            .await
            .is_ok();
        DI_AVAILABLE_DURING_BACKPLANE_CLOSE.store(available, Ordering::Release);
        Ok(())
    }
}

#[tokio::test]
async fn app_closes_backplane_while_owned_di_is_available_then_disposes_the_container() {
    DI_AVAILABLE_DURING_BACKPLANE_CLOSE.store(false, Ordering::Release);
    let app = crate::WsAppBuilder::new("127.0.0.1:0")
        .backplane::<DiAwareBackplane>(BackplaneRequirement::Required)
        .build()
        .await
        .unwrap();
    let container = Arc::clone(app.container());

    app.close().await.unwrap();

    assert!(DI_AVAILABLE_DURING_BACKPLANE_CLOSE.load(Ordering::Acquire));
    assert!(
        container
            .resolve::<lily_config::ConfigService>(None)
            .await
            .is_err()
    );
}
