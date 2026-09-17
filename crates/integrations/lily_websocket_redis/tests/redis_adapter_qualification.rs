use std::fs;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use lily_config::{ConfigOptions, ConfigService};
use lily_injection::ApplicationContainer;
use lily_websocket::{
    BackplaneRequirement, Extensions, NoReply, ServerConfig, ServerError, WebSocketActionError,
    WebSocketController, WebSocketControllerInitError, WebSocketControllerTrait, WsApp,
    WsAppBuilder, WsMessageBody, async_trait, websocket_controller,
};
use lily_websocket_redis::RedisWebSocketBackplane;
use tempfile::{TempDir, tempdir};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

const CONFIGURED_CONNECTION_DEADLINE: Duration = Duration::from_millis(150);
const BUILD_ASSERTION_DEADLINE: Duration = Duration::from_millis(750);
const CLOSE_ASSERTION_DEADLINE: Duration = Duration::from_secs(2);
const REAL_REDIS_ASSERTION_DEADLINE: Duration = Duration::from_secs(5);
const FRAME_STABILIZATION_WINDOW: Duration = Duration::from_millis(250);
const DELIVERY_QUIET_WINDOW: Duration = Duration::from_secs(1);
const QUALIFICATION_NAMESPACE: &str = "cap10-qualification";
const SUBSCRIBER_HEALTH_CHECK: &str = "backplane.subscriber";

#[derive(WebSocketController)]
#[namespace("cap10-qualification")]
struct QualificationController;

#[async_trait]
impl WebSocketControllerTrait for QualificationController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

#[websocket_controller]
impl QualificationController {
    #[message("qualification-noop")]
    async fn qualification_noop(&self) -> Result<NoReply, WebSocketActionError> {
        Ok(NoReply)
    }
}

struct HalfOpenEndpoint {
    address: SocketAddr,
    accepted: Arc<AtomicUsize>,
    shutdown: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl HalfOpenEndpoint {
    async fn bind() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind half-open Redis endpoint");
        let address = listener.local_addr().expect("read half-open address");
        let accepted = Arc::new(AtomicUsize::new(0));
        let task_accepted = Arc::clone(&accepted);
        let (shutdown, mut shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut held_connections = Vec::new();
            loop {
                tokio::select! {
                    biased;
                    _ = &mut shutdown_rx => break,
                    accepted = listener.accept() => match accepted {
                        Ok((stream, _)) => {
                            task_accepted.fetch_add(1, Ordering::AcqRel);
                            held_connections.push(stream);
                        }
                        Err(_) => break,
                    }
                }
            }
        });
        Self {
            address,
            accepted,
            shutdown: Some(shutdown),
            task: Some(task),
        }
    }

    fn redis_url(&self) -> String {
        format!("redis://{}/0", self.address)
    }

    fn accepted_connections(&self) -> usize {
        self.accepted.load(Ordering::Acquire)
    }

    async fn close(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            timeout(CLOSE_ASSERTION_DEADLINE, task)
                .await
                .expect("half-open endpoint close deadline")
                .expect("half-open endpoint task panicked");
        }
    }
}

impl Drop for HalfOpenEndpoint {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct LoopbackReservation {
    listener: Option<StdTcpListener>,
    address: SocketAddr,
}

impl LoopbackReservation {
    fn new() -> Self {
        let listener = StdTcpListener::bind("127.0.0.1:0").expect("reserve loopback address");
        let address = listener.local_addr().expect("read loopback address");
        Self {
            listener: Some(listener),
            address,
        }
    }

    const fn address(&self) -> SocketAddr {
        self.address
    }

    fn release(mut self) -> SocketAddr {
        drop(self.listener.take());
        self.address
    }
}

fn write_backplane_config(
    root: &TempDir,
    redis_url: &str,
    application_namespace: &str,
    connection_timeout: Duration,
) -> std::path::PathBuf {
    assert!(
        !redis_url
            .chars()
            .any(|character| matches!(character, '\n' | '\r' | '"'))
    );
    let path = root.path().join("lily.toml");
    let use_tls = redis_url.starts_with("rediss://");
    fs::write(
        &path,
        format!(
            r#"
[websocket.backplane]
redis_url = "{redis_url}"
use_tls = {use_tls}
application_namespace = "{application_namespace}"
environment_namespace = "test"
channel_namespace = "qualification"
publish_capacity = 8
ingress_capacity = 16
connection_timeout_millis = {}
operation_timeout_millis = 150
reconnect_initial_delay_millis = 10
reconnect_max_delay_millis = 50
reconnect_jitter_ratio = 0.0
"#,
            connection_timeout.as_millis()
        ),
    )
    .expect("write Redis qualification config");
    path
}

async fn test_container(config_path: &std::path::Path) -> Arc<ApplicationContainer> {
    Arc::new(
        ApplicationContainer::builder()
            .seed_singleton(ConfigService::new(ConfigOptions::test(config_path)))
            .build()
            .await
            .expect("build qualification DI container"),
    )
}

fn qualification_builder(
    container: Arc<ApplicationContainer>,
    requirement: BackplaneRequirement,
    address: SocketAddr,
) -> WsAppBuilder {
    WsAppBuilder::new(&address.to_string())
        .config(ServerConfig {
            allow_missing_origin: true,
            max_message_size: 1024,
            max_frame_size: 1024,
            ..ServerConfig::default()
        })
        .container(container)
        .backplane::<RedisWebSocketBackplane>(requirement)
}

async fn close_container(container: Arc<ApplicationContainer>) {
    timeout(CLOSE_ASSERTION_DEADLINE, container.close())
        .await
        .expect("qualification container close deadline")
        .expect("close qualification container");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn required_build_fails_within_connection_deadline_for_half_open_endpoint() {
    let endpoint = HalfOpenEndpoint::bind().await;
    let config_root = tempdir().expect("create required qualification directory");
    let config_path = write_backplane_config(
        &config_root,
        &endpoint.redis_url(),
        "cap10-required",
        CONFIGURED_CONNECTION_DEADLINE,
    );
    let container = test_container(&config_path).await;
    let application_address = LoopbackReservation::new();

    let started = Instant::now();
    let result = timeout(
        BUILD_ASSERTION_DEADLINE,
        qualification_builder(
            Arc::clone(&container),
            BackplaneRequirement::Required,
            application_address.address(),
        )
        .build(),
    )
    .await
    .expect("required build exceeded its configured connection deadline");
    let error = match result {
        Ok(_) => panic!("required Redis backplane must fail closed for a half-open endpoint"),
        Err(error) => error,
    };
    assert!(
        matches!(&error, ServerError::Configuration(_)),
        "half-open Redis must retain the stable server configuration category: {error}"
    );
    let diagnostic = error.to_string();
    assert!(
        diagnostic.contains("required WebSocket backplane")
            && diagnostic.contains("failed to initialize")
            && diagnostic.contains("(Transport)"),
        "half-open Redis produced an unrelated initialization error: {diagnostic}"
    );
    assert!(
        started.elapsed() <= BUILD_ASSERTION_DEADLINE,
        "required build was not bounded"
    );
    assert!(
        endpoint.accepted_connections() >= 1,
        "qualification endpoint never accepted the Redis TCP connection"
    );

    close_container(container).await;
    endpoint.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn optional_build_degrades_and_close_is_bounded_for_half_open_endpoint() {
    let endpoint = HalfOpenEndpoint::bind().await;
    let config_root = tempdir().expect("create optional qualification directory");
    let config_path = write_backplane_config(
        &config_root,
        &endpoint.redis_url(),
        "cap10-optional",
        CONFIGURED_CONNECTION_DEADLINE,
    );
    let container = test_container(&config_path).await;
    let application_address = LoopbackReservation::new();

    let app = timeout(
        BUILD_ASSERTION_DEADLINE,
        qualification_builder(
            Arc::clone(&container),
            BackplaneRequirement::Optional,
            application_address.address(),
        )
        .build(),
    )
    .await
    .expect("optional build exceeded its configured connection deadline")
    .expect("optional Redis backplane must degrade to local-only dispatch");
    assert!(
        endpoint.accepted_connections() >= 1,
        "qualification endpoint never accepted the Redis TCP connection"
    );
    let backplane_checks = app
        .health_snapshot()
        .expect("optional qualification health snapshot")
        .checks
        .into_iter()
        .filter(|check| check.name.starts_with("backplane."))
        .collect::<Vec<_>>();
    assert_eq!(backplane_checks.len(), 2);
    assert!(
        backplane_checks
            .iter()
            .all(|check| check.reason_code == "initialization_failed")
    );

    timeout(CLOSE_ASSERTION_DEADLINE, app.close())
        .await
        .expect("optional local-only app close deadline")
        .expect("close optional local-only app");
    close_container(container).await;
    endpoint.close().await;
}

fn subscriber_generation(app: &WsApp) -> (u64, String) {
    let snapshot = app
        .health_snapshot()
        .expect("Redis qualification health snapshot");
    let subscriber = snapshot
        .checks
        .iter()
        .find(|check| check.name == SUBSCRIBER_HEALTH_CHECK)
        .expect("subscriber health check");
    (subscriber.generation, subscriber.reason_code.clone())
}

async fn wait_for_ready(app: &WsApp) {
    timeout(REAL_REDIS_ASSERTION_DEADLINE, async {
        loop {
            if app
                .health_snapshot()
                .expect("Redis qualification health snapshot")
                .ready
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("required Redis app readiness deadline");
}

async fn wait_for_subscriber_recovery(app: &WsApp, previous_generation: u64) {
    timeout(REAL_REDIS_ASSERTION_DEADLINE, async {
        loop {
            let (generation, reason) = subscriber_generation(app);
            if generation >= previous_generation.saturating_add(3) && reason == "connected" {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("subscriber fail-closed reconnect cycle deadline");
}

async fn wait_for_invalid_frame_count(app: &WsApp, expected: u64) {
    timeout(REAL_REDIS_ASSERTION_DEADLINE, async {
        loop {
            if app.metrics_snapshot().backplane_invalid_frames >= expected {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("malformed Redis frame rejection deadline");
}

async fn assert_invalid_frame_count_stable(app: &WsApp, expected: u64) {
    let deadline = Instant::now() + FRAME_STABILIZATION_WINDOW;
    loop {
        assert_eq!(
            app.metrics_snapshot().backplane_invalid_frames,
            expected,
            "one malformed Redis publication must increment the invalid-frame metric exactly once"
        );
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        tokio::time::sleep(remaining.min(Duration::from_millis(10))).await;
    }
}

async fn publish_raw(
    connection: &mut redis::aio::MultiplexedConnection,
    channel: &str,
    payload: &[u8],
) {
    let subscribers = timeout(
        REAL_REDIS_ASSERTION_DEADLINE,
        redis::cmd("PUBLISH")
            .arg(channel)
            .arg(payload)
            .query_async::<usize>(connection),
    )
    .await
    .expect("raw Redis publish deadline")
    .expect("raw Redis publish");
    assert!(subscribers >= 1, "qualification subscriber was not active");
}

async fn connect_client(address: SocketAddr) -> WebSocketStream<TcpStream> {
    let tcp = timeout(REAL_REDIS_ASSERTION_DEADLINE, TcpStream::connect(address))
        .await
        .expect("qualification client TCP deadline")
        .expect("connect qualification client");
    let (client, response) = timeout(
        REAL_REDIS_ASSERTION_DEADLINE,
        tokio_tungstenite::client_async(
            format!("ws://{address}/ws?namespace={QUALIFICATION_NAMESPACE}"),
            tcp,
        ),
    )
    .await
    .expect("qualification WebSocket upgrade deadline")
    .expect("qualification WebSocket upgrade");
    assert_eq!(
        response.status(),
        tokio_tungstenite::tungstenite::http::StatusCode::SWITCHING_PROTOCOLS
    );
    client
}

async fn next_application_envelope(client: &mut WebSocketStream<TcpStream>) -> WsMessageBody {
    timeout(REAL_REDIS_ASSERTION_DEADLINE, async {
        loop {
            let frame = client
                .next()
                .await
                .expect("qualification WebSocket stream ended")
                .expect("read qualification WebSocket frame");
            match frame {
                Message::Text(_) | Message::Binary(_) => {
                    return WsMessageBody::from_message(&frame)
                        .expect("canonical qualification envelope");
                }
                Message::Ping(payload) => client
                    .send(Message::Pong(payload))
                    .await
                    .expect("answer qualification heartbeat"),
                Message::Pong(_) => {}
                Message::Close(frame) => panic!("qualification app closed early: {frame:?}"),
                Message::Frame(_) => panic!("unexpected raw WebSocket frame"),
            }
        }
    })
    .await
    .expect("valid Redis frame delivery deadline")
}

async fn assert_no_application_envelope(client: &mut WebSocketStream<TcpStream>) {
    let deadline = Instant::now() + DELIVERY_QUIET_WINDOW;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        let frame = match timeout(remaining, client.next()).await {
            Err(_) => return,
            Ok(Some(Ok(frame))) => frame,
            Ok(Some(Err(error))) => panic!("qualification WebSocket read failed: {error}"),
            Ok(None) => panic!("qualification WebSocket stream ended during duplicate check"),
        };
        match frame {
            Message::Ping(payload) => timeout(remaining, client.send(Message::Pong(payload)))
                .await
                .expect("duplicate-check heartbeat response deadline")
                .expect("answer qualification heartbeat during duplicate check"),
            Message::Pong(_) => {}
            Message::Text(_) | Message::Binary(_) => {
                let envelope = WsMessageBody::from_message(&frame)
                    .expect("decode duplicate qualification envelope");
                panic!(
                    "valid Redis publication was delivered more than once: {}",
                    envelope.event()
                );
            }
            Message::Close(frame) => {
                panic!("qualification app closed during duplicate check: {frame:?}")
            }
            Message::Frame(_) => panic!("unexpected raw WebSocket frame"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires LILY_CAP10_QUALIFICATION_REDIS_URL pointing to a dedicated real Redis"]
async fn raw_invalid_frames_fail_closed_and_valid_frame_recovers_on_real_redis() {
    let redis_url = std::env::var("LILY_CAP10_QUALIFICATION_REDIS_URL")
        .expect("LILY_CAP10_QUALIFICATION_REDIS_URL is required");
    let application_namespace = format!("cap10-{}", Uuid::new_v4());
    let channel = format!("lily.websocket.v4.test.{application_namespace}.qualification");
    let config_root = tempdir().expect("create real Redis qualification directory");
    let config_path = write_backplane_config(
        &config_root,
        &redis_url,
        &application_namespace,
        Duration::from_millis(500),
    );
    let container = test_container(&config_path).await;
    let address = LoopbackReservation::new();
    let app = Arc::new(
        timeout(
            REAL_REDIS_ASSERTION_DEADLINE,
            qualification_builder(
                Arc::clone(&container),
                BackplaneRequirement::Required,
                address.address(),
            )
            .build(),
        )
        .await
        .expect("real Redis qualification app build deadline")
        .expect("build real Redis qualification app"),
    );
    let address = address.release();
    let start = {
        let app = Arc::clone(&app);
        tokio::spawn(async move { app.start().await })
    };
    wait_for_ready(&app).await;
    let mut client = connect_client(address).await;
    let raw_client = redis::Client::open(redis_url).expect("open raw Redis client");
    let mut raw = timeout(
        REAL_REDIS_ASSERTION_DEADLINE,
        raw_client.get_multiplexed_async_connection(),
    )
    .await
    .expect("raw Redis publisher connection deadline")
    .expect("connect raw Redis publisher");

    let invalid_before_transport_rejection = app.metrics_snapshot().backplane_invalid_frames;
    let (empty_generation, _) = subscriber_generation(&app);
    publish_raw(&mut raw, &channel, &[]).await;
    wait_for_subscriber_recovery(&app, empty_generation).await;
    wait_for_ready(&app).await;

    let (oversized_generation, _) = subscriber_generation(&app);
    let oversized = vec![0_u8; 2 * 1024 * 1024];
    publish_raw(&mut raw, &channel, &oversized).await;
    wait_for_subscriber_recovery(&app, oversized_generation).await;
    wait_for_ready(&app).await;
    assert_eq!(
        app.metrics_snapshot().backplane_invalid_frames,
        invalid_before_transport_rejection,
        "empty and oversized payloads must not cross the adapter admission boundary"
    );

    let invalid_before = app.metrics_snapshot().backplane_invalid_frames;
    publish_raw(&mut raw, &channel, b"not-a-lily-backplane-envelope").await;
    wait_for_invalid_frame_count(&app, invalid_before + 1).await;
    assert_invalid_frame_count_stable(&app, invalid_before + 1).await;
    assert!(app.health_snapshot().expect("post-malformed health").ready);

    let valid = serde_json::to_vec(&serde_json::json!({
        "protocol_version": 4,
        "message_id": Uuid::new_v4(),
        "origin_node_id": Uuid::new_v4(),
        "target": { "kind": "namespace", "namespace": "cap10-qualification" },
        "exclusions": [],
        "message": {
            "protocol_version": 2,
            "msg_type": "event",
            "event": "cap10-qualification:recovered",
            "content_kind": "json",
            "content_type": "application/json",
            "encoding": "identity",
            "data": { "sequence": 7 },
            "timestamp": 123
        },
        "wire_format": "text"
    }))
    .expect("encode valid raw Lily frame");
    publish_raw(&mut raw, &channel, &valid).await;
    let envelope = next_application_envelope(&mut client).await;
    assert_eq!(envelope.event(), "cap10-qualification:recovered");
    assert_eq!(envelope.data(), &serde_json::json!({ "sequence": 7 }));
    assert_no_application_envelope(&mut client).await;

    timeout(REAL_REDIS_ASSERTION_DEADLINE, client.close(None))
        .await
        .expect("qualification client close deadline")
        .expect("close qualification client");
    timeout(REAL_REDIS_ASSERTION_DEADLINE, app.close())
        .await
        .expect("real Redis qualification app close deadline")
        .expect("close real Redis qualification app");
    timeout(REAL_REDIS_ASSERTION_DEADLINE, start)
        .await
        .expect("real Redis qualification start task deadline")
        .expect("real Redis qualification start task panicked")
        .expect("real Redis qualification lifecycle failed");
    close_container(container).await;
}
