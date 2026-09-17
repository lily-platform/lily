//! Transport-neutral multi-node backplane contract tests.

use super::*;

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use lily_monitoring::{HealthCheckKind, HealthCriticality};
use lily_shutdown::ShutdownState;
use lily_web_core::Principal;
use serde_json::json;
use tokio::sync::{Notify, mpsc};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

use crate::WebSocketServerMetricSnapshot;

const TEST_DEADLINE: Duration = Duration::from_secs(2);
const TEST_MESSAGE_LIMIT: usize = 1024 * 1024;
const TEST_ROOM_LIMIT: usize = 128;

struct TestInbox {
    events: VecDeque<WebSocketBackplaneEvent>,
    notify: Arc<Notify>,
}

struct TestBusState {
    next_endpoint_id: usize,
    endpoints: BTreeMap<usize, TestInbox>,
    last_frame: Option<Vec<u8>>,
}

/// A bounded, deterministic, all-or-none test transport.
///
/// Publications are echoed to their source endpoint so a broker-style pub/sub
/// path exercises Lily's origin-loop suppression. Capacity is checked for all
/// endpoints before any queue is mutated, preventing partial test fan-out.
struct BoundedInMemoryBus {
    capacity: usize,
    state: StdMutex<TestBusState>,
}

impl BoundedInMemoryBus {
    fn new(capacity: usize) -> Arc<Self> {
        assert!(capacity > 0);
        Arc::new(Self {
            capacity,
            state: StdMutex::new(TestBusState {
                next_endpoint_id: 0,
                endpoints: BTreeMap::new(),
                last_frame: None,
            }),
        })
    }

    fn attach(self: &Arc<Self>) -> Arc<InMemoryBackplane> {
        let notify = Arc::new(Notify::new());
        let endpoint_id = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let endpoint_id = state.next_endpoint_id;
            state.next_endpoint_id += 1;
            state.endpoints.insert(
                endpoint_id,
                TestInbox {
                    events: VecDeque::new(),
                    notify: Arc::clone(&notify),
                },
            );
            endpoint_id
        };
        Arc::new(InMemoryBackplane {
            bus: Arc::clone(self),
            endpoint_id,
            notify,
            closed: AtomicBool::new(false),
            close_calls: AtomicUsize::new(0),
        })
    }

    fn publish(&self, bytes: &[u8]) -> Result<(), WebSocketBackplaneError> {
        let notifies = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if state
                .endpoints
                .values()
                .any(|inbox| inbox.events.len() >= self.capacity)
            {
                return Err(WebSocketBackplaneError::new(
                    WebSocketBackplaneErrorKind::Saturated,
                ));
            }

            let notifies = state
                .endpoints
                .values()
                .map(|inbox| Arc::clone(&inbox.notify))
                .collect::<Vec<_>>();
            for inbox in state.endpoints.values_mut() {
                inbox
                    .events
                    .push_back(WebSocketBackplaneEvent::Frame(bytes.to_vec()));
            }
            state.last_frame = Some(bytes.to_vec());
            notifies
        };
        for notify in notifies {
            notify.notify_one();
        }
        Ok(())
    }

    fn enqueue(
        &self,
        endpoint_id: usize,
        event: WebSocketBackplaneEvent,
    ) -> Result<(), WebSocketBackplaneError> {
        let notify = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let inbox = state.endpoints.get_mut(&endpoint_id).ok_or_else(|| {
                WebSocketBackplaneError::new(WebSocketBackplaneErrorKind::Unavailable)
            })?;
            if inbox.events.len() >= self.capacity {
                return Err(WebSocketBackplaneError::new(
                    WebSocketBackplaneErrorKind::Saturated,
                ));
            }
            inbox.events.push_back(event);
            Arc::clone(&inbox.notify)
        };
        notify.notify_one();
        Ok(())
    }

    fn replay_last_to(&self, endpoint_id: usize) -> Result<(), WebSocketBackplaneError> {
        let notify = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let bytes = state.last_frame.clone().expect("published test frame");
            let inbox = state.endpoints.get_mut(&endpoint_id).ok_or_else(|| {
                WebSocketBackplaneError::new(WebSocketBackplaneErrorKind::Unavailable)
            })?;
            if inbox.events.len() >= self.capacity {
                return Err(WebSocketBackplaneError::new(
                    WebSocketBackplaneErrorKind::Saturated,
                ));
            }
            inbox
                .events
                .push_back(WebSocketBackplaneEvent::Frame(bytes));
            Arc::clone(&inbox.notify)
        };
        notify.notify_one();
        Ok(())
    }

    fn pop(
        &self,
        endpoint_id: usize,
    ) -> Result<Option<WebSocketBackplaneEvent>, WebSocketBackplaneError> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .endpoints
            .get_mut(&endpoint_id)
            .ok_or_else(|| WebSocketBackplaneError::new(WebSocketBackplaneErrorKind::Receive))
            .map(|inbox| inbox.events.pop_front())
    }

    fn remove(&self, endpoint_id: usize) {
        let notify = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .endpoints
            .remove(&endpoint_id)
            .map(|inbox| inbox.notify);
        if let Some(notify) = notify {
            notify.notify_waiters();
        }
    }

    fn queued(&self, endpoint_id: usize) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .endpoints
            .get(&endpoint_id)
            .map_or(0, |inbox| inbox.events.len())
    }
}

struct InMemoryBackplane {
    bus: Arc<BoundedInMemoryBus>,
    endpoint_id: usize,
    notify: Arc<Notify>,
    closed: AtomicBool,
    close_calls: AtomicUsize,
}

impl InMemoryBackplane {
    fn emit(&self, event: WebSocketBackplaneEvent) {
        self.bus
            .enqueue(self.endpoint_id, event)
            .expect("test control event admitted");
    }
}

#[async_trait]
impl WebSocketBackplane for InMemoryBackplane {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
        unreachable!("the contract fixture is attached to its in-memory bus explicitly")
    }

    async fn publish(
        &self,
        frame: WebSocketBackplaneFrame,
    ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
        self.bus.publish(frame.as_bytes())?;
        Ok(WebSocketBackplanePublishReceipt::accepted())
    }

    async fn receive(
        &self,
        admission: WebSocketBackplaneInboundAdmission,
    ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
        loop {
            let notified = self.notify.notified();
            if let Some(event) = self.bus.pop(self.endpoint_id)? {
                if let WebSocketBackplaneEvent::Frame(bytes) = &event
                    && bytes.len() > admission.maximum_frame_bytes()
                {
                    return Err(WebSocketBackplaneError::new(
                        WebSocketBackplaneErrorKind::Receive,
                    ));
                }
                return Ok(event);
            }
            notified.await;
        }
    }

    async fn close(&self) -> Result<(), WebSocketBackplaneError> {
        self.close_calls.fetch_add(1, Ordering::AcqRel);
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.bus.remove(self.endpoint_id);
        }
        Ok(())
    }
}

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
            .update(name, HealthStatus::Healthy, "connected")
            .unwrap();
    }
    health
}

struct TestNode {
    manager: Arc<ConnectionManager>,
    dispatcher: WebSocketDispatcher,
    provider: Arc<InMemoryBackplane>,
    ingress: Option<WebSocketBackplaneIngressTask>,
}

impl TestNode {
    fn start(bus: &Arc<BoundedInMemoryBus>) -> Self {
        Self::start_with_identity(bus, true)
    }

    fn start_with_identity(bus: &Arc<BoundedInMemoryBus>, identity_enabled: bool) -> Self {
        let manager = Arc::new(
            ConnectionManager::with_registered_namespaces_and_identity_and_outbound_policy(
                16,
                TEST_ROOM_LIMIT,
                ["orders".to_owned(), "billing".to_owned()],
                identity_enabled,
                TEST_MESSAGE_LIMIT,
                TEST_MESSAGE_LIMIT,
                Duration::from_millis(50),
            ),
        );
        let provider = bus.attach();
        let dispatcher = WebSocketDispatcher::active(
            Arc::clone(&manager),
            BackplaneRequirement::Required,
            Arc::clone(&provider) as Arc<dyn WebSocketBackplane>,
            Duration::from_secs(1),
            test_health(),
        );
        let ingress = dispatcher.spawn_ingress().expect("first ingress owner");
        Self {
            manager,
            dispatcher,
            provider,
            ingress: Some(ingress),
        }
    }

    fn without_ingress(bus: &Arc<BoundedInMemoryBus>) -> Self {
        let manager = Arc::new(
            ConnectionManager::with_registered_namespaces_and_identity_and_outbound_policy(
                16,
                TEST_ROOM_LIMIT,
                ["orders".to_owned(), "billing".to_owned()],
                true,
                TEST_MESSAGE_LIMIT,
                TEST_MESSAGE_LIMIT,
                Duration::from_millis(50),
            ),
        );
        let provider = bus.attach();
        let dispatcher = WebSocketDispatcher::active(
            Arc::clone(&manager),
            BackplaneRequirement::Required,
            Arc::clone(&provider) as Arc<dyn WebSocketBackplane>,
            Duration::from_secs(1),
            test_health(),
        );
        Self {
            manager,
            dispatcher,
            provider,
            ingress: None,
        }
    }

    async fn ready(&self) {
        self.provider
            .emit(WebSocketBackplaneEvent::SubscriptionReady);
        timeout(TEST_DEADLINE, self.dispatcher.wait_for_subscription_ready())
            .await
            .expect("subscription readiness timed out")
            .expect("subscription stopped before readiness");
    }

    async fn shutdown(mut self) {
        self.dispatcher.close_backplane().await.unwrap();
        if let Some(ingress) = self.ingress.take() {
            ingress.stop().await.unwrap();
        }
        assert_eq!(self.provider.close_calls.load(Ordering::Acquire), 1);
    }
}

struct TestClient {
    id: Uuid,
    receiver: mpsc::Receiver<Message>,
}

async fn add_client(
    node: &TestNode,
    namespace: &str,
    rooms: &[&str],
    capacity: usize,
) -> TestClient {
    let id = Uuid::new_v4();
    let (sender, receiver) = mpsc::channel(capacity);
    node.manager
        .add_connection(id, sender, None, Some(namespace.to_owned()))
        .await
        .unwrap();
    for room in rooms {
        node.manager.join_room(namespace, id, room).await.unwrap();
    }
    TestClient { id, receiver }
}

async fn add_authenticated_client(
    node: &TestNode,
    namespace: &str,
    principal_id: &str,
    capacity: usize,
) -> TestClient {
    let id = Uuid::new_v4();
    let (sender, receiver) = mpsc::channel(capacity);
    let principal = Principal::new(
        principal_id,
        Vec::<String>::new(),
        Vec::<String>::new(),
        serde_json::Map::new(),
    );
    let identity = crate::connection::AuthenticatedWebSocketIdentity::try_new(principal).unwrap();
    node.manager
        .add_authenticated_connection(id, sender, Some(namespace.to_owned()), identity)
        .await
        .unwrap();
    TestClient { id, receiver }
}

async fn add_full_client(node: &TestNode, namespace: &str) -> TestClient {
    let id = Uuid::new_v4();
    let (sender, receiver) = mpsc::channel(1);
    node.manager
        .add_connection(id, sender.clone(), None, Some(namespace.to_owned()))
        .await
        .unwrap();
    sender
        .try_send(Message::Text("fixture-prefill".into()))
        .unwrap();
    TestClient { id, receiver }
}

fn broadcast(
    target: BroadcastTarget,
    wire_format: WsWireFormat,
    sequence: u64,
) -> BroadcastMessage {
    let mut message =
        WsMessageBody::try_new("orders:created", json!({ "sequence": sequence })).unwrap();
    message.timestamp = i64::try_from(sequence).unwrap();
    BroadcastMessage {
        target,
        message,
        wire_format,
        exclude: Vec::new(),
    }
}

async fn expect_message(
    client: &mut TestClient,
    expected: &WsMessageBody,
    wire_format: WsWireFormat,
) {
    let frame = timeout(TEST_DEADLINE, client.receiver.recv())
        .await
        .expect("client receive timed out")
        .expect("client queue closed before delivery");
    match wire_format {
        WsWireFormat::Text => assert!(matches!(frame, Message::Text(_))),
        WsWireFormat::Binary => assert!(matches!(frame, Message::Binary(_))),
    }
    assert_eq!(WsMessageBody::from_message(&frame).unwrap(), *expected);
}

fn assert_no_message(client: &mut TestClient) {
    assert!(matches!(
        client.receiver.try_recv(),
        Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected)
    ));
}

async fn wait_for_metric(
    manager: &ConnectionManager,
    expected: u64,
    select: impl Fn(WebSocketServerMetricSnapshot) -> u64,
) {
    timeout(TEST_DEADLINE, async {
        loop {
            if select(manager.metrics_snapshot()) >= expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("metric transition timed out");
}

async fn wait_for_origin_loop(node: &TestNode) {
    wait_for_metric(&node.manager, 1, |snapshot| {
        snapshot.backplane_origin_loops_suppressed
    })
    .await;
}

#[tokio::test]
async fn namespace_text_reaches_both_nodes_once_and_suppresses_source_echo() {
    let bus = BoundedInMemoryBus::new(16);
    let source = TestNode::start(&bus);
    let remote = TestNode::start(&bus);
    source.ready().await;
    remote.ready().await;

    let mut source_orders = add_client(&source, "orders", &[], 4).await;
    let mut remote_orders = add_client(&remote, "orders", &[], 4).await;
    let mut remote_billing = add_client(&remote, "billing", &[], 4).await;
    let command = broadcast(
        BroadcastTarget::Namespace("orders".to_owned()),
        WsWireFormat::Text,
        1,
    );
    let expected = command.message.clone();

    let receipt = source.dispatcher.dispatch(command).await.unwrap();
    assert_eq!(receipt.local().sent, 1);
    assert!(matches!(
        receipt.backplane(),
        WebSocketBackplaneDispatchReceipt::Accepted(_)
    ));
    expect_message(&mut source_orders, &expected, WsWireFormat::Text).await;
    expect_message(&mut remote_orders, &expected, WsWireFormat::Text).await;
    assert_no_message(&mut remote_billing);
    wait_for_origin_loop(&source).await;
    assert_no_message(&mut source_orders);

    source.shutdown().await;
    remote.shutdown().await;
}

#[tokio::test]
async fn namespace_binary_isolated_across_nodes() {
    let bus = BoundedInMemoryBus::new(16);
    let source = TestNode::start(&bus);
    let remote = TestNode::start(&bus);
    source.ready().await;
    remote.ready().await;

    let mut source_orders = add_client(&source, "orders", &[], 4).await;
    let mut source_billing = add_client(&source, "billing", &[], 4).await;
    let mut remote_orders = add_client(&remote, "orders", &[], 4).await;
    let mut remote_billing = add_client(&remote, "billing", &[], 4).await;
    let command = broadcast(
        BroadcastTarget::Namespace("orders".to_owned()),
        WsWireFormat::Binary,
        2,
    );
    let expected = command.message.clone();

    source.dispatcher.dispatch(command).await.unwrap();
    expect_message(&mut source_orders, &expected, WsWireFormat::Binary).await;
    expect_message(&mut remote_orders, &expected, WsWireFormat::Binary).await;
    wait_for_origin_loop(&source).await;
    assert_no_message(&mut source_orders);
    assert_no_message(&mut source_billing);
    assert_no_message(&mut remote_billing);

    source.shutdown().await;
    remote.shutdown().await;
}

#[tokio::test]
async fn client_count_stays_namespace_scoped_and_node_local_with_backplane() {
    let bus = BoundedInMemoryBus::new(16);
    let source = TestNode::start(&bus);
    let remote = TestNode::start(&bus);
    source.ready().await;
    remote.ready().await;
    let source_orders = add_client(&source, "orders", &[], 4).await;
    let _source_billing = add_client(&source, "billing", &[], 4).await;
    let remote_orders = add_client(&remote, "orders", &[], 4).await;
    let remote_peer = add_client(&remote, "orders", &[], 4).await;
    let _remote_billing = add_client(&remote, "billing", &[], 4).await;
    let source_clients = crate::WebSocketContext::new_with_dispatcher(
        source_orders.id,
        Arc::clone(&source.manager),
        Arc::new(source.dispatcher.clone()),
        "orders".to_owned(),
    )
    .clients();
    let remote_clients = crate::WebSocketContext::new_with_dispatcher(
        remote_orders.id,
        Arc::clone(&remote.manager),
        Arc::new(remote.dispatcher.clone()),
        "orders".to_owned(),
    )
    .clients();

    assert_eq!(source_clients.count().await, 1);
    assert_eq!(remote_clients.count().await, 2);
    assert_eq!(source.manager.connection_count().await, 2);
    assert_eq!(remote.manager.connection_count().await, 3);

    remote
        .manager
        .remove_connection(remote_peer.id)
        .await
        .unwrap();
    assert_eq!(source_clients.count().await, 1);
    assert_eq!(remote_clients.count().await, 1);
    assert_eq!(source.manager.connection_count().await, 2);
    assert_eq!(remote.manager.connection_count().await, 2);

    source.shutdown().await;
    remote.shutdown().await;
}

#[tokio::test]
async fn principal_text_reaches_only_matching_namespace_and_identity_across_nodes() {
    let bus = BoundedInMemoryBus::new(16);
    let source = TestNode::start(&bus);
    let remote = TestNode::start(&bus);
    source.ready().await;
    remote.ready().await;

    let mut source_match = add_authenticated_client(&source, "orders", "account-42", 4).await;
    let mut source_other = add_authenticated_client(&source, "orders", "account-73", 4).await;
    let mut remote_match = add_authenticated_client(&remote, "orders", "account-42", 4).await;
    let mut remote_other = add_authenticated_client(&remote, "orders", "account-73", 4).await;
    let mut remote_other_namespace =
        add_authenticated_client(&remote, "billing", "account-42", 4).await;
    let command = broadcast(
        BroadcastTarget::Principal {
            namespace: "orders".to_owned(),
            principal_id: PrincipalId::try_new("account-42").unwrap(),
        },
        WsWireFormat::Text,
        10,
    );
    let expected = command.message.clone();

    let receipt = source.dispatcher.dispatch(command).await.unwrap();
    assert_eq!(receipt.local().targeted, 1);
    assert_eq!(receipt.local().sent, 1);
    assert!(matches!(
        receipt.backplane(),
        WebSocketBackplaneDispatchReceipt::Accepted(_)
    ));
    expect_message(&mut source_match, &expected, WsWireFormat::Text).await;
    expect_message(&mut remote_match, &expected, WsWireFormat::Text).await;
    wait_for_origin_loop(&source).await;
    assert_no_message(&mut source_match);
    assert_no_message(&mut source_other);
    assert_no_message(&mut remote_other);
    assert_no_message(&mut remote_other_namespace);

    source.shutdown().await;
    remote.shutdown().await;
}

#[tokio::test]
async fn room_text_isolated_across_nodes() {
    let bus = BoundedInMemoryBus::new(16);
    let source = TestNode::start(&bus);
    let remote = TestNode::start(&bus);
    source.ready().await;
    remote.ready().await;

    let mut source_red = add_client(&source, "orders", &["red"], 4).await;
    let mut source_blue = add_client(&source, "orders", &["blue"], 4).await;
    let mut remote_red = add_client(&remote, "orders", &["red"], 4).await;
    let mut remote_blue = add_client(&remote, "orders", &["blue"], 4).await;
    let command = broadcast(
        BroadcastTarget::Room {
            namespace: "orders".to_owned(),
            room: "red".to_owned(),
        },
        WsWireFormat::Text,
        3,
    );
    let expected = command.message.clone();

    source.dispatcher.dispatch(command).await.unwrap();
    expect_message(&mut source_red, &expected, WsWireFormat::Text).await;
    expect_message(&mut remote_red, &expected, WsWireFormat::Text).await;
    wait_for_origin_loop(&source).await;
    assert_no_message(&mut source_red);
    assert_no_message(&mut source_blue);
    assert_no_message(&mut remote_blue);

    source.shutdown().await;
    remote.shutdown().await;
}

#[tokio::test]
async fn rooms_binary_uses_canonical_union_semantics() {
    let bus = BoundedInMemoryBus::new(16);
    let source = TestNode::start(&bus);
    let remote = TestNode::start(&bus);
    source.ready().await;
    remote.ready().await;

    let mut source_both = add_client(&source, "orders", &["red", "blue"], 4).await;
    let mut remote_red = add_client(&remote, "orders", &["red"], 4).await;
    let mut remote_blue = add_client(&remote, "orders", &["blue"], 4).await;
    let mut remote_both = add_client(&remote, "orders", &["red", "blue"], 4).await;
    let mut remote_none = add_client(&remote, "orders", &[], 4).await;
    let command = broadcast(
        BroadcastTarget::Rooms {
            namespace: "orders".to_owned(),
            rooms: vec!["red".to_owned(), "blue".to_owned(), "red".to_owned()],
        },
        WsWireFormat::Binary,
        4,
    );
    let expected = command.message.clone();

    source.dispatcher.dispatch(command).await.unwrap();
    expect_message(&mut source_both, &expected, WsWireFormat::Binary).await;
    expect_message(&mut remote_red, &expected, WsWireFormat::Binary).await;
    expect_message(&mut remote_blue, &expected, WsWireFormat::Binary).await;
    expect_message(&mut remote_both, &expected, WsWireFormat::Binary).await;
    wait_for_origin_loop(&source).await;
    assert_no_message(&mut source_both);
    assert_no_message(&mut remote_both);
    assert_no_message(&mut remote_none);

    source.shutdown().await;
    remote.shutdown().await;
}

#[tokio::test]
async fn explicit_connections_route_remote_ids_without_fake_global_counts() {
    let bus = BoundedInMemoryBus::new(16);
    let source = TestNode::start(&bus);
    let remote = TestNode::start(&bus);
    source.ready().await;
    remote.ready().await;

    let mut source_target = add_client(&source, "orders", &[], 4).await;
    let mut source_other = add_client(&source, "orders", &[], 4).await;
    let mut remote_target = add_client(&remote, "orders", &[], 4).await;
    let mut remote_other = add_client(&remote, "orders", &[], 4).await;
    let missing = Uuid::new_v4();
    let command = broadcast(
        BroadcastTarget::NamespaceConnections {
            namespace: "orders".to_owned(),
            connection_ids: vec![
                remote_target.id,
                source_target.id,
                remote_target.id,
                missing,
            ],
        },
        WsWireFormat::Text,
        5,
    );
    let expected = command.message.clone();

    let receipt = source.dispatcher.dispatch(command).await.unwrap();
    assert_eq!(receipt.local().targeted, 3);
    assert_eq!(receipt.local().sent, 1);
    assert_eq!(receipt.local().missing, 2);
    assert!(receipt.is_fully_accepted());
    expect_message(&mut source_target, &expected, WsWireFormat::Text).await;
    expect_message(&mut remote_target, &expected, WsWireFormat::Text).await;
    wait_for_origin_loop(&source).await;
    assert_no_message(&mut source_target);
    assert_no_message(&mut source_other);
    assert_no_message(&mut remote_other);

    source.shutdown().await;
    remote.shutdown().await;
}

#[tokio::test]
async fn facade_explicit_connections_keep_namespace_across_nodes_and_wire_formats() {
    let bus = BoundedInMemoryBus::new(16);
    let source = TestNode::start(&bus);
    let remote = TestNode::start(&bus);
    source.ready().await;
    remote.ready().await;
    let mut source_target = add_client(&source, "orders", &[], 4).await;
    let mut source_foreign = add_client(&source, "billing", &[], 4).await;
    let mut source_excluded = add_client(&source, "orders", &[], 4).await;
    let mut remote_target = add_client(&remote, "orders", &[], 4).await;
    let mut remote_foreign = add_client(&remote, "billing", &[], 4).await;
    let mut remote_excluded = add_client(&remote, "orders", &[], 4).await;
    let context = crate::WebSocketContext::new_with_dispatcher(
        source_target.id,
        Arc::clone(&source.manager),
        Arc::new(source.dispatcher.clone()),
        "orders".to_owned(),
    );
    for wire_format in [WsWireFormat::Text, WsWireFormat::Binary] {
        let proxy = context
            .clients()
            .clients(vec![
                source_target.id,
                source_foreign.id,
                source_excluded.id,
                remote_target.id,
                remote_foreign.id,
                remote_excluded.id,
                remote_target.id,
                Uuid::new_v4(),
            ])
            .except(vec![
                source_excluded.id,
                remote_excluded.id,
                remote_excluded.id,
            ]);
        let receipt = match wire_format {
            WsWireFormat::Text => {
                proxy
                    .send_with_receipt("orders:created", json!({"sequence": 77}))
                    .await
            }
            WsWireFormat::Binary => {
                proxy
                    .send_binary_with_receipt("orders:created", json!({"sequence": 77}))
                    .await
            }
        }
        .unwrap();
        assert_eq!(
            *receipt.local(),
            BroadcastReport {
                targeted: 5,
                sent: 1,
                missing: 4,
                ..Default::default()
            }
        );
        assert!(receipt.is_fully_accepted());
        let frame = bus.state.lock().unwrap().last_frame.clone().unwrap();
        let envelope = remote.dispatcher.decode(&frame).unwrap();
        assert!(matches!(&envelope.target,
            BackplaneTarget::NamespaceConnections { namespace, connection_ids }
                if namespace == "orders" && connection_ids.len() == 7
        ));
        expect_message(&mut source_target, &envelope.message, wire_format).await;
        expect_message(&mut remote_target, &envelope.message, wire_format).await;
        wait_for_origin_loop(&source).await;
        for client in [
            &mut source_target,
            &mut source_foreign,
            &mut source_excluded,
            &mut remote_target,
            &mut remote_foreign,
            &mut remote_excluded,
        ] {
            assert_no_message(client);
        }
    }
    source.shutdown().await;
    remote.shutdown().await;
}

#[tokio::test]
async fn exclusions_apply_on_origin_and_remote_nodes() {
    let bus = BoundedInMemoryBus::new(16);
    let source = TestNode::start(&bus);
    let remote = TestNode::start(&bus);
    source.ready().await;
    remote.ready().await;

    let mut source_excluded = add_client(&source, "orders", &[], 4).await;
    let mut source_included = add_client(&source, "orders", &[], 4).await;
    let mut remote_excluded = add_client(&remote, "orders", &[], 4).await;
    let mut remote_included = add_client(&remote, "orders", &[], 4).await;
    let mut command = broadcast(
        BroadcastTarget::Namespace("orders".to_owned()),
        WsWireFormat::Binary,
        6,
    );
    command.exclude = vec![remote_excluded.id, source_excluded.id, remote_excluded.id];
    let expected = command.message.clone();

    source.dispatcher.dispatch(command).await.unwrap();
    expect_message(&mut source_included, &expected, WsWireFormat::Binary).await;
    expect_message(&mut remote_included, &expected, WsWireFormat::Binary).await;
    wait_for_origin_loop(&source).await;
    assert_no_message(&mut source_excluded);
    assert_no_message(&mut source_included);
    assert_no_message(&mut remote_excluded);

    source.shutdown().await;
    remote.shutdown().await;
}

#[tokio::test]
async fn duplicate_remote_replay_is_suppressed_after_first_delivery() {
    let bus = BoundedInMemoryBus::new(16);
    let source = TestNode::start(&bus);
    let remote = TestNode::start(&bus);
    source.ready().await;
    remote.ready().await;

    let mut remote_client = add_client(&remote, "orders", &[], 4).await;
    let command = broadcast(
        BroadcastTarget::Namespace("orders".to_owned()),
        WsWireFormat::Text,
        7,
    );
    let expected = command.message.clone();
    source.dispatcher.dispatch(command).await.unwrap();
    expect_message(&mut remote_client, &expected, WsWireFormat::Text).await;

    bus.replay_last_to(remote.provider.endpoint_id).unwrap();
    bus.replay_last_to(remote.provider.endpoint_id).unwrap();
    wait_for_metric(&remote.manager, 2, |snapshot| {
        snapshot.backplane_duplicates_suppressed
    })
    .await;
    assert_no_message(&mut remote_client);

    source.shutdown().await;
    remote.shutdown().await;
}

mod ingress_error_tests {
    use super::*;
    use crate::connection::ConnectionOperationError;
    use opentelemetry::{Value, trace::TracerProvider as _};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use tracing::{Event, Subscriber, field, span};
    use tracing_subscriber::{Layer, layer::Context, prelude::*};

    #[derive(Debug, Default)]
    struct Fields(BTreeMap<String, String>);

    impl field::Visit for Fields {
        fn record_debug(&mut self, field: &field::Field, value: &dyn fmt::Debug) {
            self.0.insert(field.name().to_owned(), format!("{value:?}"));
        }

        fn record_str(&mut self, field: &field::Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }
    }

    #[derive(Debug, Default)]
    struct Diagnostics {
        spans: BTreeMap<u64, (String, Fields)>,
        events: Vec<Fields>,
    }

    #[derive(Clone, Default)]
    struct Capture(Arc<StdMutex<Diagnostics>>);

    impl<S: Subscriber> Layer<S> for Capture {
        fn on_new_span(&self, attributes: &span::Attributes<'_>, id: &span::Id, _: Context<'_, S>) {
            let mut diagnostics = self.0.lock().unwrap();
            let mut fields = Fields::default();
            attributes.record(&mut fields);
            diagnostics.spans.insert(
                id.into_u64(),
                (attributes.metadata().name().to_owned(), fields),
            );
        }

        fn on_record(&self, id: &span::Id, values: &span::Record<'_>, _: Context<'_, S>) {
            let mut diagnostics = self.0.lock().unwrap();
            values.record(&mut diagnostics.spans.get_mut(&id.into_u64()).unwrap().1);
        }

        fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
            let mut fields = Fields::default();
            event.record(&mut fields);
            self.0.lock().unwrap().events.push(fields);
        }
    }

    fn principal_command(wire: WsWireFormat) -> BroadcastMessage {
        let mut command = broadcast(
            BroadcastTarget::Principal {
                namespace: "orders".to_owned(),
                principal_id: PrincipalId::try_new("principal-secret-canary").unwrap(),
            },
            wire,
            201,
        );
        command.message = WsMessageBody::try_new(
            "orders:created",
            json!({ "private": "payload-secret-canary" }),
        )
        .unwrap();
        command
    }

    async fn barrier(
        source: &TestNode,
        client: &mut TestClient,
        wire: WsWireFormat,
        sequence: u64,
    ) {
        let command = broadcast(
            BroadcastTarget::NamespaceConnections {
                namespace: "orders".to_owned(),
                connection_ids: vec![client.id],
            },
            wire,
            sequence,
        );
        let expected = command.message.clone();
        source.dispatcher.dispatch(command).await.unwrap();
        expect_message(client, &expected, wire).await;
    }

    #[tokio::test]
    async fn ingress_preserves_typed_local_error_and_redacts_its_debug() {
        let bus = BoundedInMemoryBus::new(4);
        let source = TestNode::without_ingress(&bus);
        let manager = Arc::new(ConnectionManager::with_registered_namespaces(
            16,
            TEST_ROOM_LIMIT,
            ["orders".to_owned()],
        ));
        let remote = WebSocketDispatcher::local(manager);
        source
            .dispatcher
            .dispatch(principal_command(WsWireFormat::Text))
            .await
            .unwrap();
        let bytes = bus.state.lock().unwrap().last_frame.clone().unwrap();
        remote
            .decode(&bytes)
            .expect("the source frame passes protocol validation");
        let error = remote.ingest(bytes).await.unwrap_err();
        assert!(matches!(
            error,
            WebSocketBackplaneIngressError::LocalDispatch(ConnectionError::InvalidOperation(
                ConnectionOperationError::IdentityLifecycleUnavailable
            ))
        ));
        assert_eq!(
            format!("{error:?}"),
            "LocalDispatch { reason: \"identity_lifecycle_unavailable\" }"
        );

        // Future local failures must not expose identifiers through Debug either.
        let error =
            WebSocketBackplaneIngressError::LocalDispatch(ConnectionError::ConnectionNotFound {
                connection_id: Uuid::new_v4(),
            });
        assert_eq!(format!("{error:?}"), "LocalDispatch { reason: \"other\" }");
        source.shutdown().await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn local_failure_is_separate_from_invalid_frames_and_replay_preserves_accounting() {
        // Other tests can first register this callsite without a subscriber.
        // Keep both dispatchers registered so tracing combines their interests
        // instead of using its single-dispatcher/current-thread fast path.
        let _unsubscribed = tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default());
        for wire in [WsWireFormat::Text, WsWireFormat::Binary] {
            let capture = Capture::default();
            let exports = InMemorySpanExporter::default();
            let provider = SdkTracerProvider::builder()
                .with_simple_exporter(exports.clone())
                .build();
            // This runtime and its ingress stay on this thread; no process-global
            // subscriber or extra production instrumentation is required.
            let _subscriber = tracing::subscriber::set_default(
                tracing_subscriber::registry().with(capture.clone()).with(
                    tracing_opentelemetry::layer()
                        .with_tracer(provider.tracer("backplane-contract")),
                ),
            );
            let bus = BoundedInMemoryBus::new(16);
            let source = TestNode::without_ingress(&bus);
            let remote = TestNode::start_with_identity(&bus, false);
            remote.ready().await;
            let mut healthy = add_client(&remote, "orders", &[], 4).await;
            let command = principal_command(wire);
            source.dispatcher.dispatch(command.clone()).await.unwrap();
            let original = bus.state.lock().unwrap().last_frame.clone().unwrap();
            remote
                .dispatcher
                .decode(&original)
                .expect("valid principal frame");

            // The ordered barrier proves ingress completed the failing dispatch
            // and its warning before any assertion observes the result.
            barrier(&source, &mut healthy, wire, 202).await;
            wait_for_metric(&remote.manager, 1, |m| m.outbound_messages).await;
            let mut expected = WebSocketServerMetricSnapshot {
                outbound_messages: 1,
                backplane_local_dispatch_failed: 1,
                ..Default::default()
            };
            assert_eq!(remote.manager.metrics_snapshot(), expected);

            remote
                .provider
                .emit(WebSocketBackplaneEvent::Frame(original));
            remote
                .provider
                .emit(WebSocketBackplaneEvent::Frame(b"{".to_vec()));
            barrier(&source, &mut healthy, wire, 203).await;
            wait_for_metric(&remote.manager, 2, |m| m.outbound_messages).await;
            expected.outbound_messages = 2;
            expected.backplane_duplicates_suppressed = 1;
            expected.backplane_invalid_frames = 1;
            assert_eq!(remote.manager.metrics_snapshot(), expected);

            // A fresh dispatch of the same command must produce another local
            // failure. Neither dedupe suppression nor an error stops ingress.
            source.dispatcher.dispatch(command).await.unwrap();
            barrier(&source, &mut healthy, wire, 204).await;
            wait_for_metric(&remote.manager, 3, |m| m.outbound_messages).await;
            expected.outbound_messages = 3;
            expected.backplane_local_dispatch_failed = 2;
            assert_eq!(remote.manager.metrics_snapshot(), expected);
            assert!(matches!(
                healthy.receiver.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            assert_eq!(
                bus.queued(source.provider.endpoint_id),
                5,
                "ingress must not republish"
            );
            let health = remote
                .dispatcher
                .0
                .health
                .as_ref()
                .unwrap()
                .snapshot()
                .unwrap();
            assert!(
                health
                    .checks
                    .iter()
                    .all(|check| check.status == HealthStatus::Healthy)
            );

            {
                let diagnostics = capture.0.lock().unwrap();
                let consumer_spans = diagnostics
                    .spans
                    .values()
                    .filter(|(name, _)| name == "websocket.backplane.dispatch")
                    .map(|(_, fields)| &fields.0)
                    .collect::<Vec<_>>();
                assert_eq!(consumer_spans.len(), 5, "{diagnostics:?}");
                let failed = consumer_spans
                    .iter()
                    .filter(|fields| {
                        fields.get("lily.outcome").map(String::as_str)
                            == Some("local_dispatch_failed")
                    })
                    .collect::<Vec<_>>();
                assert_eq!(failed.len(), 2);
                for fields in failed {
                    assert_eq!(
                        fields.get("lily.error_category").map(String::as_str),
                        Some("identity_lifecycle_unavailable")
                    );
                }
                assert_eq!(
                    consumer_spans
                        .iter()
                        .filter(|fields| {
                            fields.get("lily.outcome").map(String::as_str) == Some("delivered")
                        })
                        .count(),
                    3
                );
                let local_warnings = diagnostics
                    .events
                    .iter()
                    .filter(|fields| {
                        fields.0.get("message").map(String::as_str)
                            == Some("WebSocket backplane local dispatch failed")
                    })
                    .collect::<Vec<_>>();
                assert_eq!(local_warnings.len(), 2);
                for fields in local_warnings {
                    assert_eq!(
                        fields.0.get("lily.error_category").map(String::as_str),
                        Some("identity_lifecycle_unavailable")
                    );
                }
                assert_eq!(
                    diagnostics
                        .events
                        .iter()
                        .filter(|fields| {
                            fields.0.get("message").map(String::as_str)
                                == Some("Rejected invalid WebSocket backplane frame")
                        })
                        .count(),
                    1
                );
                let rendered = format!("{diagnostics:?}");
                assert!(!rendered.contains("principal-secret-canary"));
                assert!(!rendered.contains("payload-secret-canary"));
            }
            remote.manager.remove_connection(healthy.id).await.unwrap();
            source.shutdown().await;
            remote.shutdown().await;
            provider.force_flush().unwrap();
            let spans = exports.get_finished_spans().unwrap();
            let consumers: Vec<_> = spans
                .iter()
                .filter(|s| s.name == "websocket.backplane.dispatch")
                .collect();
            assert_eq!(consumers.len(), 5);
            let mut outcomes = BTreeMap::new();
            for span in consumers {
                assert!(span.span_context.is_valid());
                let mut keys = std::collections::HashSet::new();
                for kv in &span.attributes {
                    assert!(
                        keys.insert(kv.key.as_str()),
                        "raw export contains duplicate {}",
                        kv.key
                    );
                }
                let field = |key| {
                    span.attributes
                        .iter()
                        .find(|kv| kv.key.as_str() == key)
                        .map(|kv| &kv.value)
                };
                let Some(Value::String(outcome)) = field("lily.outcome") else {
                    panic!("string outcome required")
                };
                *outcomes.entry(outcome.as_str().to_owned()).or_insert(0) += 1;
                for key in ["lily.backplane.message_id", "lily.backplane.origin_node_id"] {
                    let Some(Value::String(id)) = field(key) else {
                        panic!("UUID string required")
                    };
                    assert!(!Uuid::parse_str(id.as_str()).unwrap().is_nil());
                }
                assert_eq!(
                    field("lily.error_category"),
                    (outcome.as_str() == "local_dispatch_failed")
                        .then_some(&Value::from("identity_lifecycle_unavailable"))
                );
            }
            assert_eq!(
                outcomes,
                BTreeMap::from([("delivered".into(), 3), ("local_dispatch_failed".into(), 2)])
            );
            assert!(!format!("{spans:?}").contains("principal-secret-canary"));
            assert!(!format!("{spans:?}").contains("payload-secret-canary"));
            provider.shutdown().unwrap();
        }
    }
}

// RSK-002 needs the production managed queue, not add_connection's legacy
// test sender: the writer can close while lifecycle still owns a Connected
// registry entry. Only the backplane transport and writer endpoints are fixtures.
mod retained_closed_receiver {
    use super::*;
    use crate::connection::{
        ConnectionControlReceiver, ConnectionIdentityHandle, QueuedApplicationFrame,
        WebSocketIdentitySnapshot, connection_control_channel,
    };
    use crate::{ConnectionState, RequestConnectionInfo, WsTransportSecurity};

    struct ManagedClient {
        id: Uuid,
        receiver: mpsc::Receiver<QueuedApplicationFrame>,
        control: ConnectionControlReceiver,
    }

    impl ManagedClient {
        async fn add(node: &TestNode) -> Self {
            let id = Uuid::new_v4();
            let (sender, receiver) = mpsc::channel(4);
            let (control_sender, control) = connection_control_channel();
            node.manager
                .add_connection_with_control_and_identity(
                    id,
                    sender,
                    control_sender,
                    RequestConnectionInfo::default(),
                    WsTransportSecurity::Plaintext,
                    Some("orders".to_owned()),
                    Some(ConnectionIdentityHandle::new(
                        WebSocketIdentitySnapshot::anonymous(),
                    )),
                )
                .await
                .unwrap();
            Self {
                id,
                receiver,
                control,
            }
        }

        async fn assert_connected(&mut self, node: &TestNode) {
            let connection = node.manager.get_connection(self.id).await.unwrap();
            assert_eq!(connection.state, ConnectionState::Connected);
            assert_eq!(connection.namespace, "orders");
            assert!(self.control.next().now_or_never().is_none());
        }

        async fn expect(&mut self, expected: &WsMessageBody, wire: WsWireFormat) {
            let queued = timeout(TEST_DEADLINE, self.receiver.recv())
                .await
                .expect("managed queue receive timed out")
                .expect("managed queue closed before delivery");
            match wire {
                WsWireFormat::Text => assert!(matches!(queued.message(), Message::Text(_))),
                WsWireFormat::Binary => assert!(matches!(queued.message(), Message::Binary(_))),
            }
            assert_eq!(
                WsMessageBody::from_message(queued.message()).unwrap(),
                *expected
            );
            // Dropping the frame also releases its real outbound byte reservation.
        }

        fn assert_empty(&mut self) {
            assert!(matches!(
                self.receiver.try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
        }
    }

    async fn publish_barrier(
        source: &TestNode,
        recipients: Vec<Uuid>,
        wire: WsWireFormat,
        sequence: u64,
    ) -> WsMessageBody {
        let barrier = broadcast(
            BroadcastTarget::NamespaceConnections {
                namespace: "orders".to_owned(),
                connection_ids: recipients,
            },
            wire,
            sequence,
        );
        let expected = barrier.message.clone();
        let receipt = source.dispatcher.dispatch(barrier).await.unwrap();
        assert!(matches!(
            receipt.backplane(),
            WebSocketBackplaneDispatchReceipt::Accepted(_)
        ));
        expected
    }

    async fn partial_delivery_then_replay(wire: WsWireFormat) {
        let bus = BoundedInMemoryBus::new(16);
        // Keeping the source inbox unconsumed also counts every publication,
        // including any erroneous re-publication by the remote ingress.
        let source = TestNode::without_ingress(&bus);
        let remote = TestNode::start(&bus);
        remote.ready().await;
        let mut healthy = ManagedClient::add(&remote).await;
        let mut closed = ManagedClient::add(&remote).await;
        closed.receiver.close();
        assert!(matches!(
            closed.receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
        closed.assert_connected(&remote).await;

        let command = broadcast(BroadcastTarget::Namespace("orders".to_owned()), wire, 101);
        let receipt = source.dispatcher.dispatch(command.clone()).await.unwrap();
        assert_eq!(*receipt.local(), BroadcastReport::default());
        assert!(matches!(
            receipt.backplane(),
            WebSocketBackplaneDispatchReceipt::Accepted(_)
        ));
        let original = bus.state.lock().unwrap().last_frame.clone().unwrap();
        let envelope = remote.dispatcher.decode(&original).unwrap();
        assert_eq!(envelope.origin_node_id, source.dispatcher.0.node_id);
        assert_ne!(envelope.origin_node_id, remote.dispatcher.0.node_id);

        // Ingress awaits each local broadcast before consuming the next frame.
        // Receiving this barrier proves the partial attempt has completed; the
        // healthy write alone would not establish that the closed peer was tried.
        let barrier = publish_barrier(&source, vec![healthy.id], wire, 102).await;
        healthy.expect(&command.message, wire).await;
        healthy.expect(&barrier, wire).await;
        wait_for_metric(&remote.manager, 2, |m| m.outbound_messages).await;
        // This internal admission fixture does not execute HTTP handshake
        // accounting. Every unrelated counter, including invalid frames, is zero.
        let mut expected = WebSocketServerMetricSnapshot {
            outbound_messages: 2,
            channel_closed: 1,
            ..Default::default()
        };
        assert_eq!(remote.manager.metrics_snapshot(), expected);
        assert_eq!(bus.queued(source.provider.endpoint_id), 2);
        closed.assert_connected(&remote).await;
        healthy.assert_empty();

        // A new member makes re-running namespace selection observable even if
        // an implementation tried to skip only recipients that succeeded before.
        let mut late = ManagedClient::add(&remote).await;
        remote
            .provider
            .emit(WebSocketBackplaneEvent::Frame(original));
        let barrier = publish_barrier(&source, vec![healthy.id, late.id], wire, 103).await;
        healthy.expect(&barrier, wire).await;
        late.expect(&barrier, wire).await;
        wait_for_metric(&remote.manager, 4, |m| m.outbound_messages).await;
        expected.outbound_messages = 4;
        expected.backplane_duplicates_suppressed = 1;
        assert_eq!(remote.manager.metrics_snapshot(), expected);
        assert_eq!(bus.queued(source.provider.endpoint_id), 3);
        closed.assert_connected(&remote).await;
        healthy.assert_empty();
        late.assert_empty();

        // Positive control: the identical application command with a new
        // backplane ID must select current members and attempt the closed peer
        // again. This rules out a stopped consumer or payload-based suppression.
        let receipt = source.dispatcher.dispatch(command.clone()).await.unwrap();
        assert!(matches!(
            receipt.backplane(),
            WebSocketBackplaneDispatchReceipt::Accepted(_)
        ));
        let fresh = bus.state.lock().unwrap().last_frame.clone().unwrap();
        let fresh_envelope = remote.dispatcher.decode(&fresh).unwrap();
        assert_ne!(fresh_envelope.message_id, envelope.message_id);
        assert_eq!(fresh_envelope.message, envelope.message);
        let barrier = publish_barrier(&source, vec![healthy.id, late.id], wire, 104).await;
        for client in [&mut healthy, &mut late] {
            client.expect(&command.message, wire).await;
            client.expect(&barrier, wire).await;
        }
        wait_for_metric(&remote.manager, 8, |m| m.outbound_messages).await;
        expected.outbound_messages = 8;
        expected.channel_closed = 2;
        assert_eq!(remote.manager.metrics_snapshot(), expected);
        assert_eq!(bus.queued(source.provider.endpoint_id), 5);
        assert_eq!(
            remote.dispatcher.0.ingress_running.load(Ordering::Acquire),
            1
        );
        assert!(matches!(
            closed.receiver.try_recv(),
            Err(mpsc::error::TryRecvError::Disconnected)
        ));
        for client in [&mut healthy, &mut closed, &mut late] {
            client.assert_connected(&remote).await;
        }
        healthy.assert_empty();
        late.assert_empty();
        for id in [healthy.id, closed.id, late.id] {
            remote.manager.remove_connection(id).await.unwrap();
            assert!(remote.manager.get_connection(id).await.is_none());
        }
        source.shutdown().await;
        remote.shutdown().await;
    }

    #[tokio::test]
    async fn text_partial_delivery_retains_closed_recipient_and_suppresses_replay() {
        partial_delivery_then_replay(WsWireFormat::Text).await;
    }

    #[tokio::test]
    async fn binary_partial_delivery_retains_closed_recipient_and_suppresses_replay() {
        partial_delivery_then_replay(WsWireFormat::Binary).await;
    }
}

#[tokio::test]
async fn remote_disconnect_cleans_room_membership_without_affecting_peer() {
    let bus = BoundedInMemoryBus::new(16);
    let source = TestNode::start(&bus);
    let remote = TestNode::start(&bus);
    source.ready().await;
    remote.ready().await;

    let mut disconnected = add_client(&remote, "orders", &["red"], 4).await;
    let mut healthy = add_client(&remote, "orders", &["red"], 4).await;
    remote
        .manager
        .remove_connection(disconnected.id)
        .await
        .unwrap();
    assert_eq!(
        remote.manager.get_room_connections("orders", "red").await,
        vec![healthy.id]
    );

    let command = broadcast(
        BroadcastTarget::Room {
            namespace: "orders".to_owned(),
            room: "red".to_owned(),
        },
        WsWireFormat::Text,
        8,
    );
    let expected = command.message.clone();
    source.dispatcher.dispatch(command).await.unwrap();
    expect_message(&mut healthy, &expected, WsWireFormat::Text).await;
    assert_no_message(&mut disconnected);

    source.shutdown().await;
    remote.shutdown().await;
}

#[tokio::test]
async fn remote_closed_and_full_queues_do_not_block_healthy_recipient() {
    let bus = BoundedInMemoryBus::new(16);
    let source = TestNode::start(&bus);
    let remote = TestNode::start(&bus);
    source.ready().await;
    remote.ready().await;

    let mut healthy = add_client(&remote, "orders", &[], 4).await;
    let closed = add_client(&remote, "orders", &[], 1).await;
    let mut full = add_full_client(&remote, "orders").await;
    drop(closed.receiver);

    let command = broadcast(
        BroadcastTarget::Namespace("orders".to_owned()),
        WsWireFormat::Binary,
        9,
    );
    let expected = command.message.clone();
    source.dispatcher.dispatch(command).await.unwrap();
    expect_message(&mut healthy, &expected, WsWireFormat::Binary).await;
    wait_for_metric(&remote.manager, 1, |snapshot| snapshot.channel_closed).await;
    wait_for_metric(&remote.manager, 1, |snapshot| snapshot.backpressured).await;
    assert!(matches!(
        full.receiver.try_recv(),
        Ok(Message::Text(text)) if text == "fixture-prefill"
    ));
    assert_no_message(&mut full);
    assert_eq!(
        remote.dispatcher.0.ingress_running.load(Ordering::Acquire),
        1
    );

    source.shutdown().await;
    remote.shutdown().await;
}

#[tokio::test]
async fn bounded_bus_saturation_is_atomic_across_endpoints() {
    let bus = BoundedInMemoryBus::new(1);
    let source = TestNode::without_ingress(&bus);
    let remote = TestNode::without_ingress(&bus);

    source
        .dispatcher
        .dispatch(broadcast(
            BroadcastTarget::Namespace("orders".to_owned()),
            WsWireFormat::Text,
            10,
        ))
        .await
        .unwrap();
    assert_eq!(bus.queued(source.provider.endpoint_id), 1);
    assert_eq!(bus.queued(remote.provider.endpoint_id), 1);

    let error = source
        .dispatcher
        .dispatch(broadcast(
            BroadcastTarget::Namespace("orders".to_owned()),
            WsWireFormat::Text,
            11,
        ))
        .await
        .unwrap_err();
    let ConnectionError::BackplanePublish { source: error, .. } = error else {
        panic!("expected bounded backplane publish failure");
    };
    assert_eq!(error.kind(), WebSocketBackplaneErrorKind::Saturated);
    assert_eq!(bus.queued(source.provider.endpoint_id), 1);
    assert_eq!(bus.queued(remote.provider.endpoint_id), 1);

    source.shutdown().await;
    remote.shutdown().await;
}
