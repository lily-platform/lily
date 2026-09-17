use std::fs::{self, File};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use lily_config::{ConfigOptions, ConfigService};
use lily_injection::ApplicationContainer;
use lily_websocket::{
    BackplaneRequirement, ConnectionError, Extensions, NoReply, Payload, ServerConfig, ServerError,
    WebSocketActionError, WebSocketContext, WebSocketController, WebSocketControllerInitError,
    WebSocketControllerTrait, WsApp, WsAppBuilder, WsMessageBody, async_trait,
    websocket_controller,
};
use lily_websocket_redis::RedisWebSocketBackplane;
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

const CHILD_ROLE: &str = "LILY_CAP10_NODE_ROLE";
const CHILD_CONFIG: &str = "LILY_CAP10_CONFIG_PATH";
const CHILD_ADDRESS: &str = "LILY_CAP10_NODE_ADDRESS";
const CHILD_STATE: &str = "LILY_CAP10_NODE_STATE";
const CHILD_STOP: &str = "LILY_CAP10_NODE_STOP";
const TEST_NAMESPACE: &str = "cap10-e2e";
const ISOLATED_NAMESPACE: &str = "cap10-isolated";
const REDIS_IMAGE: &str =
    "redis:7.4.2-alpine@sha256:02419de7eddf55aa5bcf49efb74e88fa8d931b4d77c07eff8a6b2144472b6952";
const STARTUP_DEADLINE: Duration = Duration::from_secs(15);
const IO_DEADLINE: Duration = Duration::from_secs(5);
const REDIS_PROBE_DEADLINE: Duration = Duration::from_millis(750);
const FAIL_FAST_DEADLINE: Duration = Duration::from_secs(2);
// A quarter-second quiet period made negative delivery assertions vulnerable to
// scheduler and Redis transport jitter on slower CI hosts. Keep this longer
// than the normal operation timeout so a late duplicate cannot pass unnoticed.
const QUIET_WINDOW: Duration = Duration::from_secs(1);
const DOCKER_COMMAND_DEADLINE: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Delivery {
    sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Identity {
    connection_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct DirectDelivery {
    connection_id: Uuid,
    sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RoomDelivery {
    room: String,
    sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RoomsDelivery {
    rooms: Vec<String>,
    sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct PublishOutcome {
    sequence: u64,
    result: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct HealthCheckEvidence {
    status: String,
    reason_code: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct NodeHealthEvidence {
    ready: bool,
    publisher: HealthCheckEvidence,
    subscriber: HealthCheckEvidence,
}

#[derive(WebSocketController)]
#[namespace("cap10-e2e")]
struct RedisBackplaneE2eController;

#[async_trait]
impl WebSocketControllerTrait for RedisBackplaneE2eController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

#[websocket_controller]
impl RedisBackplaneE2eController {
    #[message("identity")]
    async fn identity(&self, context: WebSocketContext) -> Result<NoReply, WebSocketActionError> {
        context
            .clients()
            .caller()
            .send(
                "cap10-e2e:identity",
                Identity {
                    connection_id: context.connection_id(),
                },
            )
            .await
            .map_err(WebSocketActionError::internal)?;
        Ok(NoReply)
    }

    #[message("fanout")]
    async fn fanout(
        &self,
        context: WebSocketContext,
        Payload(input): Payload<Delivery>,
    ) -> Result<NoReply, WebSocketActionError> {
        context
            .clients()
            .all()
            .send("cap10-e2e:delivered", input)
            .await
            .map_err(WebSocketActionError::internal)?;
        Ok(NoReply)
    }

    #[message("direct")]
    async fn direct(
        &self,
        context: WebSocketContext,
        Payload(input): Payload<DirectDelivery>,
    ) -> Result<NoReply, WebSocketActionError> {
        context
            .clients()
            .client(input.connection_id)
            .send(
                "cap10-e2e:direct-delivered",
                Delivery {
                    sequence: input.sequence,
                },
            )
            .await
            .map_err(WebSocketActionError::internal)?;
        Ok(NoReply)
    }

    #[message("join")]
    async fn join(
        &self,
        context: WebSocketContext,
        Payload(input): Payload<RoomDelivery>,
    ) -> Result<NoReply, WebSocketActionError> {
        context
            .rooms()
            .join(&input.room)
            .await
            .map_err(WebSocketActionError::internal)?;
        context
            .clients()
            .caller()
            .send(
                "cap10-e2e:join-ack",
                Delivery {
                    sequence: input.sequence,
                },
            )
            .await
            .map_err(WebSocketActionError::internal)?;
        Ok(NoReply)
    }

    #[message("room")]
    async fn room(
        &self,
        context: WebSocketContext,
        Payload(input): Payload<RoomDelivery>,
    ) -> Result<NoReply, WebSocketActionError> {
        context
            .clients()
            .room(&input.room)
            .send(
                "cap10-e2e:room-delivered",
                Delivery {
                    sequence: input.sequence,
                },
            )
            .await
            .map_err(WebSocketActionError::internal)?;
        Ok(NoReply)
    }

    #[message("rooms")]
    async fn rooms(
        &self,
        context: WebSocketContext,
        Payload(input): Payload<RoomsDelivery>,
    ) -> Result<NoReply, WebSocketActionError> {
        context
            .clients()
            .rooms(input.rooms)
            .send(
                "cap10-e2e:rooms-delivered",
                Delivery {
                    sequence: input.sequence,
                },
            )
            .await
            .map_err(WebSocketActionError::internal)?;
        Ok(NoReply)
    }

    #[message("except")]
    async fn except(
        &self,
        context: WebSocketContext,
        Payload(input): Payload<DirectDelivery>,
    ) -> Result<NoReply, WebSocketActionError> {
        context
            .clients()
            .all()
            .except(vec![input.connection_id])
            .send(
                "cap10-e2e:except-delivered",
                Delivery {
                    sequence: input.sequence,
                },
            )
            .await
            .map_err(WebSocketActionError::internal)?;
        Ok(NoReply)
    }

    #[message("binary-fanout")]
    async fn binary_fanout(
        &self,
        context: WebSocketContext,
        Payload(input): Payload<Delivery>,
    ) -> Result<NoReply, WebSocketActionError> {
        context
            .clients()
            .all()
            .send_binary("cap10-e2e:binary-delivered", input)
            .await
            .map_err(WebSocketActionError::internal)?;
        Ok(NoReply)
    }

    #[message("publish-probe")]
    async fn publish_probe(
        &self,
        context: WebSocketContext,
        Payload(input): Payload<Delivery>,
    ) -> Result<NoReply, WebSocketActionError> {
        let sequence = input.sequence;
        let result = context
            .clients()
            .all()
            .send_with_receipt("cap10-e2e:probe-delivered", input)
            .await;
        let result = match &result {
            Ok(_) => "accepted",
            Err(ConnectionError::BackplaneUnavailable { .. }) => "unavailable",
            Err(ConnectionError::BackplanePublish { source, .. }) => source.kind().as_str(),
            Err(_) => "dispatch_failed",
        };

        // A failed backplane publish still performs node-local delivery. This
        // second event makes the terminal publish result observable without
        // relying on timing or server logs. Its own provider result is
        // intentionally ignored because the event is already delivered
        // locally before the provider attempt.
        let _ = context
            .clients()
            .caller()
            .send_with_receipt(
                "cap10-e2e:publish-outcome",
                PublishOutcome {
                    sequence,
                    result: result.to_owned(),
                },
            )
            .await;
        Ok(NoReply)
    }
}

#[derive(WebSocketController)]
#[namespace("cap10-isolated")]
struct RedisBackplaneIsolationController;

#[async_trait]
impl WebSocketControllerTrait for RedisBackplaneIsolationController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

#[websocket_controller]
impl RedisBackplaneIsolationController {
    #[message("identity")]
    async fn identity(&self, context: WebSocketContext) -> Result<NoReply, WebSocketActionError> {
        context
            .clients()
            .caller()
            .send(
                "cap10-isolated:identity",
                Identity {
                    connection_id: context.connection_id(),
                },
            )
            .await
            .map_err(WebSocketActionError::internal)?;
        Ok(NoReply)
    }
}

struct DockerRedis {
    name: String,
    port: u16,
    port_reservation: Option<TcpListener>,
    created: bool,
    running: bool,
}

impl DockerRedis {
    fn new() -> Self {
        // Docker may assign a different host port when a container created
        // with `127.0.0.1::6379` is restarted. The child nodes intentionally
        // keep one immutable Redis URL while exercising outage recovery, so
        // reserve one loopback port and publish that exact port for the whole
        // container lifetime.
        let port_reservation =
            TcpListener::bind("127.0.0.1:0").expect("reserve Redis qualification port");
        let port = port_reservation
            .local_addr()
            .expect("read Redis qualification port")
            .port();
        Self {
            name: format!("lily-cap10-{}-{}", std::process::id(), Uuid::new_v4()),
            port,
            port_reservation: Some(port_reservation),
            created: false,
            running: false,
        }
    }

    fn start(&mut self) {
        assert!(!self.running, "Redis test container is already running");
        let output = if self.created {
            docker_output(["start", self.name.as_str()])
        } else {
            // Release immediately before Docker claims the explicitly chosen
            // port. Once the container exists, the fixed binding survives
            // kill/start and remains compatible with the nodes' immutable
            // configuration.
            drop(self.port_reservation.take());
            let port_binding = format!("127.0.0.1:{}:6379", self.port);
            docker_output([
                "run",
                "--detach",
                "--name",
                self.name.as_str(),
                "--publish",
                port_binding.as_str(),
                REDIS_IMAGE,
                "redis-server",
                "--save",
                "",
                "--appendonly",
                "no",
            ])
        };
        assert!(
            output.status.success(),
            "start Docker Redis: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        if !self.created {
            self.created = true;
        }
        self.running = true;
    }

    fn port(&self) -> u16 {
        self.port
    }

    fn interrupt(&mut self) {
        if !self.running {
            return;
        }
        let output = docker_output(["kill", self.name.as_str()]);
        assert!(
            output.status.success(),
            "interrupt Docker Redis: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        self.running = false;
    }
}

impl Drop for DockerRedis {
    fn drop(&mut self) {
        // The name is unique and owned before `docker run` begins. Always
        // attempt removal: Docker can create the container and then fail a
        // later startup/readiness step before `created` is committed.
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.name])
            .output();
    }
}

fn docker_output<const N: usize>(args: [&str; N]) -> std::process::Output {
    let mut child = Command::new("docker")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn Docker command");
    let deadline = Instant::now() + DOCKER_COMMAND_DEADLINE;
    loop {
        if child.try_wait().expect("poll Docker command").is_some() {
            return child.wait_with_output().expect("collect Docker output");
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let output = child
                .wait_with_output()
                .expect("collect timed-out Docker output");
            panic!(
                "Docker command exceeded {DOCKER_COMMAND_DEADLINE:?}: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

struct NodeProcess {
    role: &'static str,
    child: Child,
    state_path: PathBuf,
    stop_path: PathBuf,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
}

impl NodeProcess {
    fn spawn(role: &'static str, config_path: &Path, address: SocketAddr, evidence: &Path) -> Self {
        let state_path = evidence.join(format!("node-{role}.state"));
        let stop_path = evidence.join(format!("node-{role}.stop"));
        let stdout_path = evidence.join(format!("node-{role}.stdout.log"));
        let stderr_path = evidence.join(format!("node-{role}.stderr.log"));
        let executable = std::env::current_exe().expect("locate integration-test binary");
        let child = Command::new(executable)
            .args([
                "--exact",
                "redis_backplane_multi_process_e2e",
                "--ignored",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(CHILD_ROLE, role)
            .env(CHILD_CONFIG, config_path)
            .env(CHILD_ADDRESS, address.to_string())
            .env(CHILD_STATE, &state_path)
            .env(CHILD_STOP, &stop_path)
            .stdout(Stdio::from(
                File::create(&stdout_path).expect("create node stdout log"),
            ))
            .stderr(Stdio::from(
                File::create(&stderr_path).expect("create node stderr log"),
            ))
            .spawn()
            .expect("spawn WebSocket node process");
        Self {
            role,
            child,
            state_path,
            stop_path,
            stdout_path,
            stderr_path,
        }
    }

    async fn wait_for_health(
        &mut self,
        expectation: &str,
        matches: impl Fn(&NodeHealthEvidence) -> bool,
    ) -> NodeHealthEvidence {
        let deadline = Instant::now() + STARTUP_DEADLINE;
        let mut last_evidence = None;
        loop {
            if let Ok(state) = fs::read_to_string(&self.state_path)
                && let Ok(evidence) = serde_json::from_str::<NodeHealthEvidence>(&state)
            {
                if matches(&evidence) {
                    return evidence;
                }
                last_evidence = Some(evidence);
            }
            if let Some(status) = self.child.try_wait().expect("poll node process") {
                panic!(
                    "node {} exited before {expectation} with {status}; last health: {last_evidence:?}; stdout:\n{}\nstderr:\n{}",
                    self.role,
                    read_log(&self.stdout_path),
                    read_log(&self.stderr_path)
                );
            }
            assert!(
                Instant::now() < deadline,
                "node {} did not reach {expectation}; last health: {last_evidence:?}; stdout:\n{}\nstderr:\n{}",
                self.role,
                read_log(&self.stdout_path),
                read_log(&self.stderr_path)
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn stop(&mut self) {
        fs::write(&self.stop_path, b"stop").expect("signal node stop");
        let deadline = Instant::now() + STARTUP_DEADLINE;
        loop {
            if let Some(status) = self.child.try_wait().expect("poll stopping node") {
                assert!(
                    status.success(),
                    "node {} shutdown failed with {status}; stdout:\n{}\nstderr:\n{}",
                    self.role,
                    read_log(&self.stdout_path),
                    read_log(&self.stderr_path)
                );
                return;
            }
            assert!(
                Instant::now() < deadline,
                "node {} did not stop; stdout:\n{}\nstderr:\n{}",
                self.role,
                read_log(&self.stdout_path),
                read_log(&self.stderr_path)
            );
            sleep(Duration::from_millis(25)).await;
        }
    }
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn read_log(path: &Path) -> String {
    fs::read_to_string(path).unwrap_or_else(|_| "<unavailable>".to_owned())
}

struct LoopbackReservation {
    listener: Option<TcpListener>,
    address: SocketAddr,
}

impl LoopbackReservation {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback address");
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

fn write_config(root: &Path, redis_port: u16) -> PathBuf {
    write_named_config(root, "lily.toml", redis_port, "two-process")
}

fn write_named_config(
    root: &Path,
    file_name: &str,
    redis_port: u16,
    channel_namespace: &str,
) -> PathBuf {
    let path = root.join(file_name);
    fs::write(
        &path,
        format!(
            r#"
[websocket.backplane]
redis_url = "redis://127.0.0.1:{redis_port}/0"
use_tls = false
application_namespace = "cap10"
environment_namespace = "test"
channel_namespace = "{channel_namespace}"
publish_capacity = 16
ingress_capacity = 32
connection_timeout_millis = 500
operation_timeout_millis = 500
reconnect_initial_delay_millis = 25
reconnect_max_delay_millis = 200
reconnect_jitter_ratio = 0.0
"#
        ),
    )
    .expect("write CAP-10 test config");
    path
}

async fn wait_for_redis(redis_port: u16) {
    let deadline = Instant::now() + STARTUP_DEADLINE;
    loop {
        let result = timeout(REDIS_PROBE_DEADLINE, async {
            let client = redis::Client::open(format!("redis://127.0.0.1:{redis_port}/0"))?;
            let mut connection = client.get_multiplexed_async_connection().await?;
            redis::cmd("PING")
                .query_async::<String>(&mut connection)
                .await
        })
        .await;
        if matches!(result, Ok(Ok(ref response)) if response == "PONG") {
            return;
        }
        assert!(Instant::now() < deadline, "Redis did not become ready");
        sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_ready_health(app: &WsApp) {
    timeout(STARTUP_DEADLINE, async {
        loop {
            if app.health_snapshot().expect("capture node health").ready {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("node listener and required Redis backplane readiness deadline");
}

fn node_health_evidence(app: &WsApp) -> NodeHealthEvidence {
    let snapshot = app.health_snapshot().expect("capture child health");
    let check = |name: &str| {
        let check = snapshot
            .checks
            .iter()
            .find(|check| check.name == name)
            .unwrap_or_else(|| panic!("missing {name} health check"));
        HealthCheckEvidence {
            status: serde_json::to_value(check.status)
                .expect("serialize health status")
                .as_str()
                .expect("health status is a string")
                .to_owned(),
            reason_code: check.reason_code.clone(),
        }
    };

    NodeHealthEvidence {
        ready: snapshot.ready,
        publisher: check("backplane.publisher"),
        subscriber: check("backplane.subscriber"),
    }
}

fn is_initially_ready(evidence: &NodeHealthEvidence) -> bool {
    evidence.ready
        && evidence.publisher.status == "healthy"
        && matches!(
            evidence.publisher.reason_code.as_str(),
            "initialized" | "available"
        )
        && evidence.subscriber.status == "healthy"
        && evidence.subscriber.reason_code == "connected"
}

fn has_observed_idle_outage(evidence: &NodeHealthEvidence) -> bool {
    !evidence.ready
        && evidence.publisher.status != "healthy"
        && matches!(
            evidence.publisher.reason_code.as_str(),
            "reconnecting" | "unavailable"
        )
        && evidence.subscriber.status != "healthy"
        && matches!(
            evidence.subscriber.reason_code.as_str(),
            "reconnecting" | "unavailable"
        )
}

fn has_observed_publish_outage(evidence: &NodeHealthEvidence) -> bool {
    !evidence.ready
        && evidence.publisher.status != "healthy"
        && matches!(
            evidence.publisher.reason_code.as_str(),
            "reconnecting" | "unavailable" | "timed_out"
        )
        && evidence.subscriber.status != "healthy"
        && matches!(
            evidence.subscriber.reason_code.as_str(),
            "reconnecting" | "unavailable"
        )
}

fn has_recovered(evidence: &NodeHealthEvidence) -> bool {
    evidence.ready
        && evidence.publisher.status == "healthy"
        && evidence.publisher.reason_code == "available"
        && evidence.subscriber.status == "healthy"
        && evidence.subscriber.reason_code == "connected"
}

fn publish_health_evidence(path: &Path, evidence: &NodeHealthEvidence) {
    let temporary = path.with_extension("state.tmp");
    fs::write(
        &temporary,
        serde_json::to_vec(evidence).expect("serialize child health evidence"),
    )
    .expect("write child health evidence");
    fs::rename(temporary, path).expect("atomically publish child health evidence");
}

async fn run_child_node() {
    let config_path = PathBuf::from(std::env::var_os(CHILD_CONFIG).expect("child config path"));
    let address = std::env::var(CHILD_ADDRESS).expect("child listener address");
    let state_path = PathBuf::from(std::env::var_os(CHILD_STATE).expect("child state path"));
    let stop_path = PathBuf::from(std::env::var_os(CHILD_STOP).expect("child stop path"));

    let container = Arc::new(
        ApplicationContainer::builder()
            .seed_singleton(ConfigService::new(ConfigOptions::test(config_path)))
            .build()
            .await
            .expect("build child DI container"),
    );
    let app = Arc::new(
        WsAppBuilder::new(&address)
            .config(ServerConfig {
                allow_missing_origin: true,
                ping_interval_secs: 60,
                idle_timeout_secs: 120,
                ..ServerConfig::default()
            })
            .container(Arc::clone(&container))
            .backplane::<RedisWebSocketBackplane>(BackplaneRequirement::Required)
            .build()
            .await
            .expect("build child WebSocket app"),
    );
    let start = {
        let app = Arc::clone(&app);
        tokio::spawn(async move { app.start().await })
    };
    wait_for_ready_health(&app).await;

    let mut last_evidence = None;
    loop {
        if stop_path.exists() {
            break;
        }
        assert!(
            !start.is_finished(),
            "child WebSocket lifecycle terminated early"
        );
        let evidence = node_health_evidence(&app);
        if last_evidence.as_ref() != Some(&evidence) {
            publish_health_evidence(&state_path, &evidence);
            last_evidence = Some(evidence);
        }
        sleep(Duration::from_millis(25)).await;
    }

    timeout(STARTUP_DEADLINE, app.close())
        .await
        .expect("child app close deadline")
        .expect("close child app");
    timeout(STARTUP_DEADLINE, start)
        .await
        .expect("child start task deadline")
        .expect("child start task panicked")
        .expect("child WebSocket lifecycle failed");
    timeout(STARTUP_DEADLINE, container.close())
        .await
        .expect("child DI close deadline")
        .expect("close child DI container");
}

async fn connect_client(address: SocketAddr) -> WebSocketStream<TcpStream> {
    connect_client_to_namespace(address, TEST_NAMESPACE).await
}

async fn connect_client_to_namespace(
    address: SocketAddr,
    namespace: &str,
) -> WebSocketStream<TcpStream> {
    let tcp = timeout(IO_DEADLINE, TcpStream::connect(address))
        .await
        .expect("client connect deadline")
        .expect("connect to WebSocket node");
    let (client, response) = timeout(
        IO_DEADLINE,
        tokio_tungstenite::client_async(format!("ws://{address}/ws?namespace={namespace}"), tcp),
    )
    .await
    .expect("WebSocket upgrade deadline")
    .expect("complete WebSocket upgrade");
    assert_eq!(
        response.status(),
        tokio_tungstenite::tungstenite::http::StatusCode::SWITCHING_PROTOCOLS
    );
    client
}

async fn send_action<T: Serialize>(
    client: &mut WebSocketStream<TcpStream>,
    action: &str,
    payload: T,
) {
    timeout(
        IO_DEADLINE,
        client.send(
            WsMessageBody::try_new(format!("{TEST_NAMESPACE}:{action}"), payload)
                .expect("serialize action payload")
                .with_namespace(TEST_NAMESPACE.to_owned())
                .to_message()
                .expect("encode Lily action envelope"),
        ),
    )
    .await
    .expect("action send deadline")
    .expect("send action envelope");
}

async fn next_application_envelope(client: &mut WebSocketStream<TcpStream>) -> WsMessageBody {
    next_application_frame(client).await.0
}

async fn next_application_frame(client: &mut WebSocketStream<TcpStream>) -> (WsMessageBody, bool) {
    loop {
        let frame = timeout(IO_DEADLINE, client.next())
            .await
            .expect("application frame deadline")
            .expect("WebSocket stream ended")
            .expect("read WebSocket frame");
        match frame {
            Message::Text(_) => {
                return (
                    WsMessageBody::from_message(&frame).expect("canonical Lily text envelope"),
                    false,
                );
            }
            Message::Binary(_) => {
                return (
                    WsMessageBody::from_message(&frame).expect("canonical Lily binary envelope"),
                    true,
                );
            }
            Message::Ping(payload) => timeout(IO_DEADLINE, client.send(Message::Pong(payload)))
                .await
                .expect("heartbeat response deadline")
                .expect("answer heartbeat"),
            Message::Pong(_) => {}
            Message::Close(frame) => panic!("node closed before delivery: {frame:?}"),
            Message::Frame(_) => panic!("unexpected raw Tungstenite frame"),
        }
    }
}

async fn expect_event(client: &mut WebSocketStream<TcpStream>, event: &str, sequence: u64) {
    let envelope = next_application_envelope(client).await;
    assert_eq!(envelope.event(), event);
    assert_eq!(
        envelope.data(),
        &serde_json::json!({ "sequence": sequence })
    );
}

async fn expect_binary_event(client: &mut WebSocketStream<TcpStream>, event: &str, sequence: u64) {
    let (envelope, binary) = next_application_frame(client).await;
    assert!(binary, "{event} must use a binary WebSocket frame");
    assert_eq!(envelope.event(), event);
    assert_eq!(
        envelope.data(),
        &serde_json::json!({ "sequence": sequence })
    );
}

async fn expect_publish_outcome(
    client: &mut WebSocketStream<TcpStream>,
    sequence: u64,
    expected: &[&str],
) -> String {
    let envelope = next_application_envelope(client).await;
    assert_eq!(envelope.event(), "cap10-e2e:publish-outcome");
    let outcome = serde_json::from_value::<PublishOutcome>(envelope.data().clone())
        .expect("decode publish outcome");
    assert_eq!(outcome.sequence, sequence);
    assert!(
        expected.contains(&outcome.result.as_str()),
        "unexpected publish outcome {}; expected one of {expected:?}",
        outcome.result
    );
    outcome.result
}

async fn assert_quiet(client: &mut WebSocketStream<TcpStream>, context: &str) {
    let deadline = Instant::now() + QUIET_WINDOW;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return;
        }
        let frame = match timeout(remaining, client.next()).await {
            Err(_) => return,
            Ok(Some(Ok(frame))) => frame,
            Ok(Some(Err(error))) => panic!("{context}: WebSocket read failed: {error}"),
            Ok(None) => panic!("{context}: WebSocket stream ended"),
        };
        match frame {
            Message::Ping(payload) => timeout(remaining, client.send(Message::Pong(payload)))
                .await
                .expect("quiet-window heartbeat response deadline")
                .expect("answer heartbeat during quiet window"),
            Message::Pong(_) => {}
            Message::Text(_) | Message::Binary(_) => {
                let envelope = WsMessageBody::from_message(&frame)
                    .expect("decode unexpected application envelope");
                panic!(
                    "{context}: unexpected application event {} with data {}",
                    envelope.event(),
                    envelope.data()
                );
            }
            Message::Close(frame) => panic!("{context}: unexpected Close frame: {frame:?}"),
            Message::Frame(_) => panic!("{context}: unexpected raw Tungstenite frame"),
        }
    }
}

async fn identity(client: &mut WebSocketStream<TcpStream>) -> Uuid {
    send_action(client, "identity", serde_json::json!({})).await;
    let envelope = next_application_envelope(client).await;
    assert_eq!(envelope.event(), "cap10-e2e:identity");
    serde_json::from_value::<Identity>(envelope.data().clone())
        .expect("decode connection identity")
        .connection_id
}

async fn close_client(client: &mut WebSocketStream<TcpStream>) {
    timeout(IO_DEADLINE, client.send(Message::Close(None)))
        .await
        .expect("client Close send deadline")
        .expect("send client Close");
    loop {
        let frame = timeout(IO_DEADLINE, client.next())
            .await
            .expect("Close acknowledgement deadline")
            .expect("stream ended before Close acknowledgement")
            .expect("read Close acknowledgement");
        match frame {
            Message::Close(_) => return,
            Message::Ping(payload) => timeout(IO_DEADLINE, client.send(Message::Pong(payload)))
                .await
                .expect("closing heartbeat response deadline")
                .expect("answer heartbeat while closing"),
            Message::Pong(_) => {}
            Message::Text(_) | Message::Binary(_) => {
                let envelope = WsMessageBody::from_message(&frame)
                    .expect("decode application frame observed while closing");
                panic!(
                    "application event {} remained unread before close: {}",
                    envelope.event(),
                    envelope.data()
                );
            }
            Message::Frame(_) => panic!("unexpected raw Tungstenite frame while closing"),
        }
    }
}

async fn test_container(config_path: &Path) -> Arc<ApplicationContainer> {
    Arc::new(
        ApplicationContainer::builder()
            .seed_singleton(ConfigService::new(ConfigOptions::test(config_path)))
            .build()
            .await
            .expect("build CAP-10 test DI container"),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn redis_backplane_required_and_optional_startup_contracts_are_bounded() {
    let evidence = TempDir::new().expect("create startup-contract evidence directory");
    // Retaining this listener makes the selected port deterministic: it is
    // reachable but cannot accidentally become a Redis endpoint between port
    // selection and the two initialization attempts.
    let unavailable_redis = LoopbackReservation::new();
    let unavailable_redis_port = unavailable_redis.address().port();
    let config_path = write_config(evidence.path(), unavailable_redis_port);

    let required_container = test_container(&config_path).await;
    let required_result = timeout(
        FAIL_FAST_DEADLINE,
        WsAppBuilder::new("127.0.0.1:0")
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .container(Arc::clone(&required_container))
            .backplane::<RedisWebSocketBackplane>(BackplaneRequirement::Required)
            .build(),
    )
    .await
    .expect("required Redis initialization exceeded its fail-fast deadline");
    let required_error = match required_result {
        Ok(_) => panic!("required Redis backplane must fail closed when Redis is unavailable"),
        Err(error) => error,
    };
    assert!(
        matches!(&required_error, ServerError::Configuration(_)),
        "required Redis failure must retain the stable server configuration category: {required_error}"
    );
    let required_error = required_error.to_string();
    assert!(
        required_error.contains("required WebSocket backplane")
            && required_error.contains("failed to initialize")
            && (required_error.contains("(Transport)") || required_error.contains("(TimedOut)")),
        "required Redis failure did not retain the stable backplane initialization reason: {required_error}"
    );
    timeout(STARTUP_DEADLINE, required_container.close())
        .await
        .expect("required container close deadline")
        .expect("close required container");

    let optional_container = test_container(&config_path).await;
    let optional_address = LoopbackReservation::new();
    let app = Arc::new(
        WsAppBuilder::new(&optional_address.address().to_string())
            .config(ServerConfig {
                allow_missing_origin: true,
                ..ServerConfig::default()
            })
            .container(Arc::clone(&optional_container))
            .backplane::<RedisWebSocketBackplane>(BackplaneRequirement::Optional)
            .build()
            .await
            .expect("optional Redis backplane falls back to local dispatch"),
    );
    let snapshot = app.health_snapshot().expect("optional pre-start health");
    let backplane_checks = snapshot
        .checks
        .iter()
        .filter(|check| check.name.starts_with("backplane."))
        .collect::<Vec<_>>();
    assert_eq!(backplane_checks.len(), 2);
    assert!(
        backplane_checks
            .iter()
            .all(|check| check.reason_code == "initialization_failed")
    );

    let optional_address = optional_address.release();
    let start = {
        let app = Arc::clone(&app);
        tokio::spawn(async move { app.start().await })
    };
    wait_for_ready_health(&app).await;
    let mut client = connect_client(optional_address).await;
    send_action(&mut client, "publish-probe", Delivery { sequence: 1 }).await;
    expect_event(&mut client, "cap10-e2e:probe-delivered", 1).await;
    expect_publish_outcome(&mut client, 1, &["unavailable"]).await;
    assert_quiet(
        &mut client,
        "optional local-only publish must not duplicate",
    )
    .await;
    close_client(&mut client).await;

    timeout(STARTUP_DEADLINE, app.close())
        .await
        .expect("optional app close deadline")
        .expect("close optional app");
    timeout(STARTUP_DEADLINE, start)
        .await
        .expect("optional start task deadline")
        .expect("optional start task panicked")
        .expect("optional WebSocket lifecycle failed");
    timeout(STARTUP_DEADLINE, optional_container.close())
        .await
        .expect("optional container close deadline")
        .expect("close optional container");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker and the pinned Redis image"]
async fn redis_backplane_multi_process_e2e() {
    if std::env::var_os(CHILD_ROLE).is_some() {
        run_child_node().await;
        return;
    }

    let evidence = TempDir::new().expect("create CAP-10 evidence directory");
    // Hold the listener sockets at once so the OS cannot return the same port
    // for two child processes. Each reservation is released immediately
    // before the process which owns that address starts. Docker itself owns a
    // dynamically allocated host port, avoiding a Redis port-selection race.
    let address_a = LoopbackReservation::new();
    let address_b = LoopbackReservation::new();
    let address_c = LoopbackReservation::new();
    let mut redis = DockerRedis::new();
    redis.start();
    let redis_port = redis.port();
    let config_path = write_config(evidence.path(), redis_port);
    let isolated_config_path = write_named_config(
        evidence.path(),
        "isolated-channel.toml",
        redis_port,
        "isolated-process",
    );
    wait_for_redis(redis_port).await;

    let address_a = address_a.release();
    let mut node_a = NodeProcess::spawn("a", &config_path, address_a, evidence.path());
    let address_b = address_b.release();
    let mut node_b = NodeProcess::spawn("b", &config_path, address_b, evidence.path());
    let address_c = address_c.release();
    let mut node_c = NodeProcess::spawn("c", &isolated_config_path, address_c, evidence.path());
    node_a
        .wait_for_health("initial publisher/subscriber readiness", is_initially_ready)
        .await;
    node_b
        .wait_for_health("initial publisher/subscriber readiness", is_initially_ready)
        .await;
    node_c
        .wait_for_health(
            "isolated-channel publisher/subscriber readiness",
            is_initially_ready,
        )
        .await;

    let mut client_a = connect_client(address_a).await;
    let mut client_b = connect_client(address_b).await;
    let mut channel_isolated_client = connect_client(address_c).await;
    let mut isolated_client = connect_client_to_namespace(address_b, ISOLATED_NAMESPACE).await;
    let _connection_a = identity(&mut client_a).await;
    let connection_b = identity(&mut client_b).await;
    let _connection_c = identity(&mut channel_isolated_client).await;

    send_action(&mut client_a, "fanout", Delivery { sequence: 1 }).await;
    expect_event(&mut client_a, "cap10-e2e:delivered", 1).await;
    expect_event(&mut client_b, "cap10-e2e:delivered", 1).await;
    tokio::join!(
        assert_quiet(&mut client_a, "fanout duplicate on node A"),
        assert_quiet(&mut client_b, "fanout duplicate on node B"),
        assert_quiet(
            &mut isolated_client,
            "fanout crossed the controller namespace boundary"
        ),
        assert_quiet(
            &mut channel_isolated_client,
            "fanout crossed the Redis channel boundary"
        ),
    );

    send_action(
        &mut channel_isolated_client,
        "fanout",
        Delivery { sequence: 10 },
    )
    .await;
    expect_event(&mut channel_isolated_client, "cap10-e2e:delivered", 10).await;
    tokio::join!(
        assert_quiet(&mut client_a, "isolated Redis channel reached node A"),
        assert_quiet(&mut client_b, "isolated Redis channel reached node B"),
        assert_quiet(
            &mut channel_isolated_client,
            "isolated Redis channel delivery duplicated locally"
        ),
    );
    close_client(&mut channel_isolated_client).await;
    node_c.stop().await;

    send_action(
        &mut client_a,
        "direct",
        DirectDelivery {
            connection_id: connection_b,
            sequence: 2,
        },
    )
    .await;
    expect_event(&mut client_b, "cap10-e2e:direct-delivered", 2).await;
    tokio::join!(
        assert_quiet(&mut client_a, "direct delivery reached the source"),
        assert_quiet(&mut client_b, "direct delivery duplicated"),
        assert_quiet(
            &mut isolated_client,
            "direct delivery crossed the controller namespace boundary"
        ),
    );

    send_action(
        &mut client_b,
        "join",
        RoomDelivery {
            room: "blue".to_owned(),
            sequence: 20,
        },
    )
    .await;
    expect_event(&mut client_b, "cap10-e2e:join-ack", 20).await;
    send_action(
        &mut client_a,
        "room",
        RoomDelivery {
            room: "blue".to_owned(),
            sequence: 3,
        },
    )
    .await;
    expect_event(&mut client_b, "cap10-e2e:room-delivered", 3).await;
    tokio::join!(
        assert_quiet(&mut client_a, "room delivery reached a non-member"),
        assert_quiet(&mut client_b, "room delivery duplicated"),
        assert_quiet(
            &mut isolated_client,
            "room delivery crossed the controller namespace boundary"
        ),
    );

    send_action(
        &mut client_b,
        "join",
        RoomDelivery {
            room: "green".to_owned(),
            sequence: 21,
        },
    )
    .await;
    expect_event(&mut client_b, "cap10-e2e:join-ack", 21).await;
    send_action(
        &mut client_a,
        "rooms",
        RoomsDelivery {
            rooms: vec!["green".to_owned(), "blue".to_owned(), "green".to_owned()],
            sequence: 4,
        },
    )
    .await;
    expect_event(&mut client_b, "cap10-e2e:rooms-delivered", 4).await;
    tokio::join!(
        assert_quiet(&mut client_a, "room union reached a non-member"),
        assert_quiet(&mut client_b, "overlapping room union duplicated delivery"),
        assert_quiet(
            &mut isolated_client,
            "room union crossed the controller namespace boundary"
        ),
    );

    send_action(
        &mut client_a,
        "except",
        DirectDelivery {
            connection_id: connection_b,
            sequence: 5,
        },
    )
    .await;
    expect_event(&mut client_a, "cap10-e2e:except-delivered", 5).await;
    tokio::join!(
        assert_quiet(&mut client_a, "excluded fanout duplicated locally"),
        assert_quiet(&mut client_b, "excluded connection received fanout"),
        assert_quiet(
            &mut isolated_client,
            "excluded fanout crossed the controller namespace boundary"
        ),
    );

    send_action(&mut client_a, "binary-fanout", Delivery { sequence: 6 }).await;
    expect_binary_event(&mut client_a, "cap10-e2e:binary-delivered", 6).await;
    expect_binary_event(&mut client_b, "cap10-e2e:binary-delivered", 6).await;
    tokio::join!(
        assert_quiet(&mut client_a, "binary fanout duplicated on node A"),
        assert_quiet(&mut client_b, "binary fanout duplicated on node B"),
        assert_quiet(
            &mut isolated_client,
            "binary fanout crossed the controller namespace boundary"
        ),
    );

    // No action is sent after the interruption until both subscribers have
    // independently observed the idle transport outage.
    redis.interrupt();
    node_a
        .wait_for_health("idle subscriber outage on node A", has_observed_idle_outage)
        .await;
    node_b
        .wait_for_health("idle subscriber outage on node B", has_observed_idle_outage)
        .await;

    let fail_fast_started = Instant::now();
    send_action(&mut client_a, "publish-probe", Delivery { sequence: 7 }).await;
    expect_event(&mut client_a, "cap10-e2e:probe-delivered", 7).await;
    expect_publish_outcome(&mut client_a, 7, &["unavailable", "timed_out"]).await;
    assert!(
        fail_fast_started.elapsed() <= FAIL_FAST_DEADLINE,
        "node A publish rejection exceeded fail-fast deadline: {:?}",
        fail_fast_started.elapsed()
    );
    assert_quiet(
        &mut client_b,
        "failed node A publish reached the remote Redis subscriber",
    )
    .await;
    node_a
        .wait_for_health("publisher outage on node A", has_observed_publish_outage)
        .await;

    let fail_fast_started = Instant::now();
    send_action(&mut client_b, "publish-probe", Delivery { sequence: 71 }).await;
    expect_event(&mut client_b, "cap10-e2e:probe-delivered", 71).await;
    expect_publish_outcome(&mut client_b, 71, &["unavailable", "timed_out"]).await;
    assert!(
        fail_fast_started.elapsed() <= FAIL_FAST_DEADLINE,
        "node B publish rejection exceeded fail-fast deadline: {:?}",
        fail_fast_started.elapsed()
    );
    assert_quiet(
        &mut client_a,
        "failed node B publish reached the remote Redis subscriber",
    )
    .await;
    node_b
        .wait_for_health("publisher outage on node B", has_observed_publish_outage)
        .await;

    redis.start();
    wait_for_redis(redis_port).await;
    node_a
        .wait_for_health("publisher/subscriber recovery on node A", has_recovered)
        .await;
    node_b
        .wait_for_health("publisher/subscriber recovery on node B", has_recovered)
        .await;

    // The first application publish from each recovered publisher must be
    // accepted and observed exactly once on both nodes.
    send_action(&mut client_a, "publish-probe", Delivery { sequence: 8 }).await;
    expect_event(&mut client_a, "cap10-e2e:probe-delivered", 8).await;
    expect_event(&mut client_b, "cap10-e2e:probe-delivered", 8).await;
    expect_publish_outcome(&mut client_a, 8, &["accepted"]).await;
    tokio::join!(
        assert_quiet(&mut client_a, "recovered node A publish duplicated locally"),
        assert_quiet(
            &mut client_b,
            "recovered node A publish duplicated remotely"
        ),
        assert_quiet(
            &mut isolated_client,
            "recovered node A publish crossed the namespace boundary"
        ),
    );

    send_action(&mut client_b, "publish-probe", Delivery { sequence: 9 }).await;
    expect_event(&mut client_b, "cap10-e2e:probe-delivered", 9).await;
    expect_event(&mut client_a, "cap10-e2e:probe-delivered", 9).await;
    expect_publish_outcome(&mut client_b, 9, &["accepted"]).await;
    tokio::join!(
        assert_quiet(
            &mut client_a,
            "recovered node B publish duplicated remotely"
        ),
        assert_quiet(&mut client_b, "recovered node B publish duplicated locally"),
        assert_quiet(
            &mut isolated_client,
            "recovered node B publish crossed the namespace boundary"
        ),
    );

    close_client(&mut client_a).await;
    close_client(&mut client_b).await;
    close_client(&mut isolated_client).await;
    node_a.stop().await;
    node_b.stop().await;
}
