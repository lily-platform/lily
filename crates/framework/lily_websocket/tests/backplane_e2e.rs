use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use lily_injection::Injectable;
use lily_injection::{ApplicationContainer, InjectionError, ServiceTrait};
use lily_websocket::{
    BackplaneRequirement, Extensions, Message, NoReply, Payload, ServerConfig,
    WebSocketActionError, WebSocketBackplane, WebSocketBackplaneError, WebSocketBackplaneErrorKind,
    WebSocketBackplaneEvent, WebSocketBackplaneFrame, WebSocketBackplaneInboundAdmission,
    WebSocketBackplaneInitError, WebSocketBackplaneInitErrorKind, WebSocketBackplanePublishReceipt,
    WebSocketContext, WebSocketController, WebSocketControllerInitError, WebSocketControllerTrait,
    WsApp, WsAppBuilder, WsMessageBody, async_trait, websocket_controller,
};
use serde::{Deserialize, Serialize};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc};
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;

const TEST_DEADLINE: Duration = Duration::from_secs(3);
const TEST_BUS_CAPACITY: usize = 16;
const TEST_NAMESPACE: &str = "cap09-e2e";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct FanoutPayload {
    sequence: u64,
}

static ACTION_CALLS: AtomicUsize = AtomicUsize::new(0);

#[derive(WebSocketController)]
#[namespace("cap09-e2e")]
struct BackplaneE2eController;

#[async_trait]
impl WebSocketControllerTrait for BackplaneE2eController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

#[websocket_controller]
impl BackplaneE2eController {
    #[message("fanout")]
    async fn fanout(
        &self,
        context: WebSocketContext,
        Payload(input): Payload<FanoutPayload>,
    ) -> Result<NoReply, WebSocketActionError> {
        ACTION_CALLS.fetch_add(1, Ordering::AcqRel);
        context
            .clients()
            .all()
            .send("cap09-e2e:delivered", &input)
            .await
            .map_err(WebSocketActionError::internal)?;
        Ok(NoReply)
    }
}

struct TestBusState {
    next_endpoint_id: usize,
    endpoints: BTreeMap<usize, mpsc::Sender<WebSocketBackplaneEvent>>,
}

/// Bounded all-node transport used only by this integration-test binary.
///
/// The source endpoint receives its own publication, matching broker pub/sub
/// behavior and exercising Lily's origin-node suppression path.
struct BoundedTestBus {
    capacity: usize,
    state: StdMutex<TestBusState>,
    publications: AtomicUsize,
}

impl BoundedTestBus {
    fn new(capacity: usize) -> Arc<Self> {
        assert!(capacity > 1, "one slot is reserved for SubscriptionReady");
        Arc::new(Self {
            capacity,
            state: StdMutex::new(TestBusState {
                next_endpoint_id: 0,
                endpoints: BTreeMap::new(),
            }),
            publications: AtomicUsize::new(0),
        })
    }

    fn attach(&self) -> (usize, mpsc::Receiver<WebSocketBackplaneEvent>) {
        let (sender, receiver) = mpsc::channel(self.capacity);
        sender
            .try_send(WebSocketBackplaneEvent::SubscriptionReady)
            .expect("the new endpoint has a reserved readiness slot");

        let endpoint_id = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let endpoint_id = state.next_endpoint_id;
            state.next_endpoint_id += 1;
            state.endpoints.insert(endpoint_id, sender);
            endpoint_id
        };
        (endpoint_id, receiver)
    }

    fn publish(&self, bytes: &[u8]) -> Result<(), WebSocketBackplaneError> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.endpoints.is_empty() {
            return Err(WebSocketBackplaneError::new(
                WebSocketBackplaneErrorKind::Unavailable,
            ));
        }
        if state
            .endpoints
            .values()
            .any(|sender| sender.capacity() == 0)
        {
            return Err(WebSocketBackplaneError::new(
                WebSocketBackplaneErrorKind::Saturated,
            ));
        }

        for sender in state.endpoints.values() {
            sender
                .try_send(WebSocketBackplaneEvent::Frame(bytes.to_vec()))
                .map_err(|error| match error {
                    mpsc::error::TrySendError::Full(_) => {
                        WebSocketBackplaneError::new(WebSocketBackplaneErrorKind::Saturated)
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        WebSocketBackplaneError::new(WebSocketBackplaneErrorKind::Unavailable)
                    }
                })?;
        }
        self.publications.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    fn detach(&self, endpoint_id: usize) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .endpoints
            .remove(&endpoint_id);
    }

    fn endpoint_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .endpoints
            .len()
    }
}

#[derive(Default)]
struct LifecycleEvidence {
    service_initialized: AtomicUsize,
    service_disposed: AtomicUsize,
    backplane_closed: AtomicBool,
    disposed_before_backplane_close: AtomicBool,
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct TestBackplaneBusService {
    bus: Option<Arc<BoundedTestBus>>,
    evidence: Arc<LifecycleEvidence>,
}

#[async_trait]
impl ServiceTrait for TestBackplaneBusService {
    async fn initialize(&mut self) -> Result<(), InjectionError> {
        self.evidence
            .service_initialized
            .fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    async fn dispose(&self) -> Result<(), InjectionError> {
        if !self.evidence.backplane_closed.load(Ordering::Acquire) {
            self.evidence
                .disposed_before_backplane_close
                .store(true, Ordering::Release);
        }
        self.evidence
            .service_disposed
            .fetch_add(1, Ordering::AcqRel);
        Ok(())
    }
}

struct TestInMemoryBackplane {
    bus: Arc<BoundedTestBus>,
    evidence: Arc<LifecycleEvidence>,
    endpoint_id: usize,
    events: Mutex<mpsc::Receiver<WebSocketBackplaneEvent>>,
    closed: AtomicBool,
}

#[async_trait]
impl WebSocketBackplane for TestInMemoryBackplane {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, WebSocketBackplaneInitError> {
        let service = extensions
            .get_service::<TestBackplaneBusService>(None)
            .await
            .map_err(WebSocketBackplaneInitError::dependency)?;
        let bus = service.bus.clone().ok_or_else(|| {
            WebSocketBackplaneInitError::new(WebSocketBackplaneInitErrorKind::Configuration)
        })?;
        let (endpoint_id, events) = bus.attach();
        Ok(Self {
            bus,
            evidence: Arc::clone(&service.evidence),
            endpoint_id,
            events: Mutex::new(events),
            closed: AtomicBool::new(false),
        })
    }

    async fn publish(
        &self,
        frame: WebSocketBackplaneFrame,
    ) -> Result<WebSocketBackplanePublishReceipt, WebSocketBackplaneError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(WebSocketBackplaneError::new(
                WebSocketBackplaneErrorKind::Unavailable,
            ));
        }
        self.bus.publish(frame.as_bytes())?;
        Ok(WebSocketBackplanePublishReceipt::accepted())
    }

    async fn receive(
        &self,
        admission: WebSocketBackplaneInboundAdmission,
    ) -> Result<WebSocketBackplaneEvent, WebSocketBackplaneError> {
        let event =
            self.events.lock().await.recv().await.ok_or_else(|| {
                WebSocketBackplaneError::new(WebSocketBackplaneErrorKind::Receive)
            })?;
        if let WebSocketBackplaneEvent::Frame(bytes) = &event
            && bytes.len() > admission.maximum_frame_bytes()
        {
            return Err(WebSocketBackplaneError::new(
                WebSocketBackplaneErrorKind::Receive,
            ));
        }
        Ok(event)
    }

    async fn close(&self) -> Result<(), WebSocketBackplaneError> {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.bus.detach(self.endpoint_id);
            self.evidence
                .backplane_closed
                .store(true, Ordering::Release);
        }
        Ok(())
    }
}

fn reserve_loopback_address() -> (std::net::TcpListener, std::net::SocketAddr) {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").expect("reserve a loopback test address");
    let address = listener.local_addr().expect("read loopback test address");
    (listener, address)
}

async fn wait_for_accepting_health(app: &WsApp) {
    timeout(TEST_DEADLINE, async {
        loop {
            let snapshot = app.health_snapshot().expect("health snapshot");
            if snapshot.accepting_new_work {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("WebSocket listener and required backplane readiness deadline");
}

async fn wait_for_connection_count(app: &WsApp, expected: usize) {
    timeout(TEST_DEADLINE, async {
        loop {
            if app.active_connection_count().await == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("WebSocket connection-count deadline");
}

async fn wait_for_delivery_evidence(app_a: &WsApp, app_b: &WsApp, expected: u64) {
    timeout(TEST_DEADLINE, async {
        loop {
            let a = app_a.metrics_snapshot();
            let b = app_b.metrics_snapshot();
            if a.backplane_publish_accepted == expected
                && a.backplane_origin_loops_suppressed == expected
                && b.outbound_messages == expected
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("distributed delivery evidence deadline");
}

async fn connect_client(address: std::net::SocketAddr) -> WebSocketStream<TcpStream> {
    let tcp = timeout(TEST_DEADLINE, TcpStream::connect(address))
        .await
        .expect("loopback connect deadline")
        .expect("connect to ready loopback listener");
    let (client, response) = tokio_tungstenite::client_async(
        format!("ws://{address}/ws?namespace={TEST_NAMESPACE}"),
        tcp,
    )
    .await
    .expect("complete real WebSocket Upgrade");
    assert_eq!(
        response.status(),
        tokio_tungstenite::tungstenite::http::StatusCode::SWITCHING_PROTOCOLS
    );
    client
}

async fn next_application_envelope(client: &mut WebSocketStream<TcpStream>) -> WsMessageBody {
    loop {
        let frame = timeout(TEST_DEADLINE, client.next())
            .await
            .expect("application frame deadline")
            .expect("WebSocket stream ended before the application frame")
            .expect("read application frame");
        match frame {
            Message::Text(_) | Message::Binary(_) => {
                return WsMessageBody::from_message(&frame)
                    .expect("server emitted a canonical Lily envelope");
            }
            Message::Ping(payload) => {
                client
                    .send(Message::Pong(payload))
                    .await
                    .expect("answer server heartbeat");
            }
            Message::Pong(_) => {}
            Message::Close(frame) => panic!("server closed before delivery: {frame:?}"),
            Message::Frame(_) => panic!("Tungstenite exposed an unexpected raw frame"),
        }
    }
}

async fn send_fanout(client: &mut WebSocketStream<TcpStream>, sequence: u64) {
    client
        .send(
            WsMessageBody::try_new("cap09-e2e:fanout", FanoutPayload { sequence })
                .expect("serialize test payload")
                .with_namespace(TEST_NAMESPACE.to_owned())
                .to_message()
                .expect("encode canonical inbound envelope"),
        )
        .await
        .expect("send action envelope to node A");
}

async fn assert_delivery(client: &mut WebSocketStream<TcpStream>, sequence: u64) {
    let envelope = next_application_envelope(client).await;
    assert_eq!(envelope.event(), "cap09-e2e:delivered");
    assert_eq!(
        envelope.data(),
        &serde_json::json!({ "sequence": sequence })
    );
}

async fn close_client(client: &mut WebSocketStream<TcpStream>) {
    client
        .send(Message::Close(None))
        .await
        .expect("send peer Close");
    loop {
        let frame = timeout(TEST_DEADLINE, client.next())
            .await
            .expect("peer Close acknowledgement deadline")
            .expect("stream ended before peer Close acknowledgement")
            .expect("read peer Close acknowledgement");
        match frame {
            Message::Close(_) => return,
            Message::Ping(payload) => {
                client
                    .send(Message::Pong(payload))
                    .await
                    .expect("answer heartbeat while closing");
            }
            _ => {}
        }
    }
}

#[tokio::test]
async fn two_real_apps_route_a_struct_controller_fanout_through_the_backplane() {
    ACTION_CALLS.store(0, Ordering::Release);
    let bus = BoundedTestBus::new(TEST_BUS_CAPACITY);
    let evidence_a = Arc::new(LifecycleEvidence::default());
    let evidence_b = Arc::new(LifecycleEvidence::default());

    let container_a = Arc::new(
        ApplicationContainer::builder()
            .seed_singleton(TestBackplaneBusService {
                bus: Some(Arc::clone(&bus)),
                evidence: Arc::clone(&evidence_a),
            })
            .build()
            .await
            .expect("build node A DI container"),
    );
    let container_b = Arc::new(
        ApplicationContainer::builder()
            .seed_singleton(TestBackplaneBusService {
                bus: Some(Arc::clone(&bus)),
                evidence: Arc::clone(&evidence_b),
            })
            .build()
            .await
            .expect("build node B DI container"),
    );
    assert_eq!(evidence_a.service_initialized.load(Ordering::Acquire), 1);
    assert_eq!(evidence_b.service_initialized.load(Ordering::Acquire), 1);

    let (reservation_a, address_a) = reserve_loopback_address();
    let (reservation_b, address_b) = reserve_loopback_address();
    let server_config = ServerConfig {
        allow_missing_origin: true,
        ..ServerConfig::default()
    };
    let app_a = Arc::new(
        WsAppBuilder::new(&address_a.to_string())
            .config(server_config.clone())
            .container(Arc::clone(&container_a))
            .backplane::<TestInMemoryBackplane>(BackplaneRequirement::Required)
            .build()
            .await
            .expect("build node A WebSocket app"),
    );
    let app_b = Arc::new(
        WsAppBuilder::new(&address_b.to_string())
            .config(server_config)
            .container(Arc::clone(&container_b))
            .backplane::<TestInMemoryBackplane>(BackplaneRequirement::Required)
            .build()
            .await
            .expect("build node B WebSocket app"),
    );
    assert_eq!(bus.endpoint_count(), 2);

    drop(reservation_a);
    let start_a = {
        let app = Arc::clone(&app_a);
        tokio::spawn(async move { app.start().await })
    };
    wait_for_accepting_health(&app_a).await;

    drop(reservation_b);
    let start_b = {
        let app = Arc::clone(&app_b);
        tokio::spawn(async move { app.start().await })
    };
    wait_for_accepting_health(&app_b).await;

    let mut client_a = connect_client(address_a).await;
    let mut client_b = connect_client(address_b).await;
    wait_for_connection_count(&app_a, 1).await;
    wait_for_connection_count(&app_b, 1).await;

    // Two ordered messages act as a deterministic sentinel: a duplicate of
    // sequence 1 would be observed before sequence 2 on either client.
    send_fanout(&mut client_a, 1).await;
    assert_delivery(&mut client_a, 1).await;
    assert_delivery(&mut client_b, 1).await;
    wait_for_delivery_evidence(&app_a, &app_b, 1).await;

    send_fanout(&mut client_a, 2).await;
    assert_delivery(&mut client_a, 2).await;
    assert_delivery(&mut client_b, 2).await;
    wait_for_delivery_evidence(&app_a, &app_b, 2).await;

    assert_eq!(ACTION_CALLS.load(Ordering::Acquire), 2);
    assert_eq!(bus.publications.load(Ordering::Acquire), 2);
    assert_eq!(app_a.metrics_snapshot().backplane_duplicates_suppressed, 0);
    assert_eq!(app_b.metrics_snapshot().backplane_duplicates_suppressed, 0);

    close_client(&mut client_a).await;
    close_client(&mut client_b).await;
    drop(client_a);
    drop(client_b);
    wait_for_connection_count(&app_a, 0).await;
    wait_for_connection_count(&app_b, 0).await;

    timeout(TEST_DEADLINE, app_a.close())
        .await
        .expect("node A app close deadline")
        .expect("close node A app");
    assert_eq!(bus.endpoint_count(), 1);
    timeout(TEST_DEADLINE, app_b.close())
        .await
        .expect("node B app close deadline")
        .expect("close node B app");
    assert_eq!(bus.endpoint_count(), 0);

    timeout(TEST_DEADLINE, start_a)
        .await
        .expect("node A start task deadline")
        .expect("node A start task panicked")
        .expect("node A server lifecycle failed");
    timeout(TEST_DEADLINE, start_b)
        .await
        .expect("node B start task deadline")
        .expect("node B start task panicked")
        .expect("node B server lifecycle failed");
    assert!(evidence_a.backplane_closed.load(Ordering::Acquire));
    assert!(evidence_b.backplane_closed.load(Ordering::Acquire));
    assert_eq!(evidence_a.service_disposed.load(Ordering::Acquire), 0);
    assert_eq!(evidence_b.service_disposed.load(Ordering::Acquire), 0);

    timeout(TEST_DEADLINE, container_a.close())
        .await
        .expect("node A DI close deadline")
        .expect("close node A DI container");
    timeout(TEST_DEADLINE, container_b.close())
        .await
        .expect("node B DI close deadline")
        .expect("close node B DI container");
    assert_eq!(evidence_a.service_disposed.load(Ordering::Acquire), 1);
    assert_eq!(evidence_b.service_disposed.load(Ordering::Acquire), 1);
    assert!(
        !evidence_a
            .disposed_before_backplane_close
            .load(Ordering::Acquire)
    );
    assert!(
        !evidence_b
            .disposed_before_backplane_close
            .load(Ordering::Acquire)
    );
}
