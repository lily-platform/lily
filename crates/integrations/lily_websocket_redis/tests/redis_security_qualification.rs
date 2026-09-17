#![cfg(unix)]

use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use lily_config::{ConfigOptions, ConfigService};
use lily_injection::ApplicationContainer;
use lily_websocket::{
    BackplaneRequirement, ConnectionError, Extensions, NoReply, Payload, ServerConfig, ServerError,
    WebSocketActionError, WebSocketContext, WebSocketController, WebSocketControllerInitError,
    WebSocketControllerTrait, WsAppBuilder, WsMessageBody, async_trait, websocket_controller,
};
use lily_websocket_redis::RedisWebSocketBackplane;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, DnType, ExtendedKeyUsagePurpose, IsCa,
    KeyPair, KeyUsagePurpose,
};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;
use uuid::Uuid;

const REDIS_IMAGE: &str =
    "redis:7.4.2-alpine@sha256:02419de7eddf55aa5bcf49efb74e88fa8d931b4d77c07eff8a6b2144472b6952";
const FULL_USER: &str = "full";
const FULL_PASSWORD: &str = "cap10-full-secret";
const PUBLISH_ONLY_USER: &str = "publishonly";
const PUBLISH_ONLY_PASSWORD: &str = "cap10-publish-secret";
const COMMAND_DEADLINE: Duration = Duration::from_secs(15);
const REDIS_STARTUP_DEADLINE: Duration = Duration::from_secs(15);
const ASSERTION_DEADLINE: Duration = Duration::from_secs(5);
const SECURITY_NAMESPACE: &str = "cap10-security";

fn bounded_command(program: &str, args: &[&str]) -> std::process::Output {
    bounded_command_with_deadline(program, args, COMMAND_DEADLINE)
}

fn bounded_command_with_deadline(
    program: &str,
    args: &[&str],
    command_deadline: Duration,
) -> std::process::Output {
    match try_command_with_deadline(program, args, command_deadline) {
        Ok(output) => output,
        Err(output) => panic!(
            "{program} exceeded {command_deadline:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        ),
    }
}

fn try_command_with_deadline(
    program: &str,
    args: &[&str],
    command_deadline: Duration,
) -> Result<std::process::Output, std::process::Output> {
    let mut child = Command::new(program)
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("spawn {program}: {error}"));
    let deadline = Instant::now() + command_deadline;
    loop {
        if child
            .try_wait()
            .unwrap_or_else(|error| panic!("poll {program}: {error}"))
            .is_some()
        {
            return Ok(child
                .wait_with_output()
                .unwrap_or_else(|error| panic!("collect {program} output: {error}")));
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let output = child
                .wait_with_output()
                .unwrap_or_else(|error| panic!("collect timed-out {program} output: {error}"));
            return Err(output);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct SecurityDelivery {
    sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct SecurityPublishOutcome {
    sequence: u64,
    result: String,
}

#[derive(WebSocketController)]
#[namespace("cap10-security")]
struct RedisSecurityController;

#[async_trait]
impl WebSocketControllerTrait for RedisSecurityController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

#[websocket_controller]
impl RedisSecurityController {
    #[message("publish-probe")]
    async fn publish_probe(
        &self,
        context: WebSocketContext,
        Payload(input): Payload<SecurityDelivery>,
    ) -> Result<NoReply, WebSocketActionError> {
        let sequence = input.sequence;
        let result = context
            .clients()
            .all()
            .send_with_receipt("cap10-security:delivered", input)
            .await;
        let result = match result {
            Ok(_) => "accepted",
            Err(ConnectionError::BackplaneUnavailable { .. }) => "unavailable",
            Err(ConnectionError::BackplanePublish { source, .. }) => source.kind().as_str(),
            Err(_) => "dispatch_failed",
        };

        // The outcome is intentionally sent to the caller after the provider
        // receipt is known. Receiving only the first, node-local event would
        // not prove that the Redis PUBLISH command was accepted.
        let _ = context
            .clients()
            .caller()
            .send_with_receipt(
                "cap10-security:publish-outcome",
                SecurityPublishOutcome {
                    sequence,
                    result: result.to_owned(),
                },
            )
            .await;
        Ok(NoReply)
    }
}

struct TlsRedis {
    name: String,
    port: Option<u16>,
}

impl TlsRedis {
    fn start(fixtures: &Path) -> Self {
        let name = format!(
            "lily-cap10-security-{}-{}",
            std::process::id(),
            Uuid::new_v4()
        );
        // Establish the cleanup owner before invoking Docker. Any assertion or
        // readiness failure after container creation still removes the
        // partially initialized qualification resource.
        let mut redis = Self { name, port: None };
        let fixture_mount = format!("{}:/fixtures:ro", fixtures.display());
        let output = bounded_command(
            "docker",
            &[
                "run",
                "--detach",
                "--name",
                &redis.name,
                "--publish",
                "127.0.0.1::6379",
                "--volume",
                &fixture_mount,
                REDIS_IMAGE,
                "redis-server",
                "/fixtures/redis.conf",
            ],
        );
        assert!(
            output.status.success(),
            "start TLS Redis: {}",
            String::from_utf8_lossy(&output.stderr)
        );

        let mapping = bounded_command("docker", &["port", &redis.name, "6379/tcp"]);
        assert!(
            mapping.status.success(),
            "read TLS Redis port: {}",
            String::from_utf8_lossy(&mapping.stderr)
        );
        let mapping = String::from_utf8(mapping.stdout).expect("Docker port is UTF-8");
        redis.port = Some(
            mapping
                .trim()
                .rsplit_once(':')
                .and_then(|(_, port)| port.parse().ok())
                .expect("Docker returned a loopback TLS Redis port"),
        );

        redis.wait_until_ready();
        redis
    }

    fn port(&self) -> u16 {
        self.port
            .expect("TLS Redis qualification container has no published port")
    }

    fn wait_until_ready(&self) {
        let deadline = Instant::now() + REDIS_STARTUP_DEADLINE;
        let mut last_probe = "Redis readiness probe has not run".to_owned();
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                let inspect = bounded_command_with_deadline(
                    "docker",
                    &[
                        "inspect",
                        "--format",
                        "status={{.State.Status}} exit={{.State.ExitCode}} error={{.State.Error}}",
                        &self.name,
                    ],
                    Duration::from_secs(2),
                );
                let logs = bounded_command_with_deadline(
                    "docker",
                    &["logs", "--tail", "100", &self.name],
                    Duration::from_secs(2),
                );
                panic!(
                    "TLS Redis did not become ready; last probe: {last_probe}; container: {}; logs:\n{}{}",
                    String::from_utf8_lossy(&inspect.stdout).trim(),
                    String::from_utf8_lossy(&logs.stdout),
                    String::from_utf8_lossy(&logs.stderr),
                );
            }
            let output = try_command_with_deadline(
                "docker",
                &[
                    "exec",
                    &self.name,
                    "redis-cli",
                    "--tls",
                    "--cacert",
                    "/fixtures/ca.pem",
                    "--user",
                    FULL_USER,
                    "--pass",
                    FULL_PASSWORD,
                    "PING",
                ],
                remaining.min(Duration::from_millis(500)),
            );
            let output = match output {
                Ok(output) => output,
                Err(output) => {
                    last_probe = format!(
                        "redis-cli timed out: {}",
                        String::from_utf8_lossy(&output.stderr).trim()
                    );
                    continue;
                }
            };
            if output.status.success() && output.stdout.as_slice() == b"PONG\n" {
                return;
            }
            last_probe = format!(
                "redis-cli exit={:?}, stdout={:?}, stderr={:?}",
                output.status.code(),
                String::from_utf8_lossy(&output.stdout).trim(),
                String::from_utf8_lossy(&output.stderr).trim(),
            );
            std::thread::sleep(Duration::from_millis(25));
        }
    }
}

impl Drop for TlsRedis {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.name])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
    }
}

fn write_tls_fixtures(root: &Path) -> PathBuf {
    fs::set_permissions(root, fs::Permissions::from_mode(0o755))
        .expect("make TLS fixture directory container-readable");

    let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("CA parameters");
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "Lily CAP-10 Qualification Root CA");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::CrlSign,
    ];
    let ca = CertifiedIssuer::self_signed(ca_params, KeyPair::generate().expect("generate CA key"))
        .expect("generate test CA");

    let server_key = KeyPair::generate().expect("generate Redis server key");
    let mut server_params =
        CertificateParams::new(["localhost".to_owned(), "127.0.0.1".to_owned()])
            .expect("Redis server certificate parameters");
    server_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_certificate = server_params
        .signed_by(&server_key, &ca)
        .expect("sign Redis server certificate");

    let ca_path = root.join("ca.pem");
    let files = [
        (ca_path.clone(), ca.pem()),
        (root.join("server.pem"), server_certificate.pem()),
        (root.join("server.key"), server_key.serialize_pem()),
        (
            root.join("redis.conf"),
            format!(
                r#"port 0
tls-port 6379
tls-cert-file /fixtures/server.pem
tls-key-file /fixtures/server.key
tls-ca-cert-file /fixtures/ca.pem
tls-auth-clients no
save ""
appendonly no
user default off
user {FULL_USER} on >{FULL_PASSWORD} &lily.websocket.v4.qualification.cap10-security.events +hello +client|setinfo +ping +publish +subscribe +unsubscribe
user {PUBLISH_ONLY_USER} on >{PUBLISH_ONLY_PASSWORD} &lily.websocket.v4.qualification.cap10-security.events +hello +client|setinfo +ping +publish
"#
            ),
        ),
    ];
    for (path, contents) in files {
        fs::write(&path, contents).expect("write Redis TLS fixture");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644))
            .expect("make Redis TLS fixture container-readable");
    }
    fs::canonicalize(ca_path).expect("canonical test CA path")
}

fn write_app_config(
    root: &Path,
    file_name: &str,
    port: u16,
    ca_path: &Path,
    username: &str,
    password: &str,
) -> PathBuf {
    let ca_path = ca_path.display().to_string();
    assert!(
        !ca_path
            .chars()
            .any(|character| matches!(character, '\n' | '\r' | '"'))
    );
    let path = root.join(file_name);
    fs::write(
        &path,
        format!(
            r#"[lifecycle]
shutdown_timeout_secs = 1

[websocket.backplane]
redis_url = "rediss://{username}:{password}@127.0.0.1:{port}/0"
use_tls = true
custom_ca_bundle = "{ca_path}"
application_namespace = "cap10-security"
environment_namespace = "qualification"
channel_namespace = "events"
publish_capacity = 8
ingress_capacity = 16
connection_timeout_millis = 500
operation_timeout_millis = 500
reconnect_initial_delay_millis = 25
reconnect_max_delay_millis = 100
reconnect_jitter_ratio = 0.0
"#
        ),
    )
    .expect("write security qualification config");
    path
}

async fn container(config_path: &Path) -> Arc<ApplicationContainer> {
    Arc::new(
        ApplicationContainer::builder()
            .seed_singleton(ConfigService::new(ConfigOptions::production(config_path)))
            .build()
            .await
            .expect("build security qualification container"),
    )
}

fn builder(container: Arc<ApplicationContainer>, address: &str) -> WsAppBuilder {
    WsAppBuilder::new(address)
        .config(ServerConfig {
            allow_missing_origin: true,
            ..ServerConfig::default()
        })
        .container(container)
        .backplane::<RedisWebSocketBackplane>(BackplaneRequirement::Required)
}

struct LoopbackReservation {
    listener: Option<TcpListener>,
    address: SocketAddr,
}

impl LoopbackReservation {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve security listener");
        let address = listener
            .local_addr()
            .expect("read reserved security listener address");
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

async fn connect_client(address: SocketAddr) -> WebSocketStream<TcpStream> {
    let stream = timeout(ASSERTION_DEADLINE, TcpStream::connect(address))
        .await
        .expect("security client connect deadline")
        .expect("connect security WebSocket client");
    let (client, response) = timeout(
        ASSERTION_DEADLINE,
        tokio_tungstenite::client_async(
            format!("ws://{address}/ws?namespace={SECURITY_NAMESPACE}"),
            stream,
        ),
    )
    .await
    .expect("security WebSocket upgrade deadline")
    .expect("complete security WebSocket upgrade");
    assert_eq!(
        response.status(),
        tokio_tungstenite::tungstenite::http::StatusCode::SWITCHING_PROTOCOLS
    );
    client
}

async fn send_publish_probe(client: &mut WebSocketStream<TcpStream>, sequence: u64) {
    let message = WsMessageBody::try_new(
        format!("{SECURITY_NAMESPACE}:publish-probe"),
        SecurityDelivery { sequence },
    )
    .expect("serialize security publish probe")
    .with_namespace(SECURITY_NAMESPACE.to_owned())
    .to_message()
    .expect("encode security publish probe");
    timeout(ASSERTION_DEADLINE, client.send(message))
        .await
        .expect("security publish probe deadline")
        .expect("send security publish probe");
}

async fn next_application_event(client: &mut WebSocketStream<TcpStream>) -> WsMessageBody {
    loop {
        let frame = timeout(ASSERTION_DEADLINE, client.next())
            .await
            .expect("security application event deadline")
            .expect("security WebSocket stream ended")
            .expect("read security WebSocket frame");
        match frame {
            Message::Text(_) | Message::Binary(_) => {
                return WsMessageBody::from_message(&frame)
                    .expect("decode canonical security application envelope");
            }
            Message::Ping(payload) => {
                timeout(ASSERTION_DEADLINE, client.send(Message::Pong(payload)))
                    .await
                    .expect("security heartbeat response deadline")
                    .expect("answer security heartbeat")
            }
            Message::Pong(_) => {}
            Message::Close(frame) => panic!("security server closed before evidence: {frame:?}"),
            Message::Frame(_) => panic!("unexpected raw security WebSocket frame"),
        }
    }
}

async fn close_container(container: Arc<ApplicationContainer>) {
    timeout(ASSERTION_DEADLINE, container.close())
        .await
        .expect("security qualification container close deadline")
        .expect("close security qualification container");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires Docker and the pinned TLS Redis image"]
async fn tls_custom_ca_credentials_and_subscriber_acl_are_fail_closed() {
    let fixtures = TempDir::new().expect("create TLS Redis fixture directory");
    let ca_path = write_tls_fixtures(fixtures.path());
    let redis = TlsRedis::start(fixtures.path());

    let wrong_config = write_app_config(
        fixtures.path(),
        "wrong-credential.toml",
        redis.port(),
        &ca_path,
        FULL_USER,
        "wrong-secret",
    );
    let wrong_container = container(&wrong_config).await;
    let wrong_result = timeout(
        ASSERTION_DEADLINE,
        builder(Arc::clone(&wrong_container), "127.0.0.1:0").build(),
    )
    .await
    .expect("wrong-credential build deadline");
    let wrong_error = match wrong_result {
        Err(error) => error,
        Ok(app) => {
            let _ = app.close().await;
            panic!("wrong Redis credential unexpectedly initialized the required backplane");
        }
    };
    let diagnostic = format!("{wrong_error:?} {wrong_error}");
    assert!(diagnostic.contains("required WebSocket backplane"));
    assert!(diagnostic.contains("Transport"));
    assert!(!diagnostic.contains("wrong-secret"));
    assert!(!diagnostic.contains(FULL_PASSWORD));
    close_container(wrong_container).await;

    let full_config = write_app_config(
        fixtures.path(),
        "full-access.toml",
        redis.port(),
        &ca_path,
        FULL_USER,
        FULL_PASSWORD,
    );
    let full_container = container(&full_config).await;
    let full_address = LoopbackReservation::new();
    let full_app = Arc::new(
        timeout(
            ASSERTION_DEADLINE,
            builder(
                Arc::clone(&full_container),
                &full_address.address().to_string(),
            )
            .build(),
        )
        .await
        .expect("TLS/custom-CA build deadline")
        .expect("TLS/custom-CA Redis backplane initialization"),
    );
    let full_address = full_address.release();
    let full_start = {
        let app = Arc::clone(&full_app);
        tokio::spawn(async move { app.start().await })
    };
    timeout(ASSERTION_DEADLINE, async {
        loop {
            if full_app
                .health_snapshot()
                .expect("TLS Redis health snapshot")
                .ready
            {
                return;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("TLS/custom-CA subscriber readiness deadline");
    let mut full_client = connect_client(full_address).await;
    send_publish_probe(&mut full_client, 1).await;
    let delivered = next_application_event(&mut full_client).await;
    assert_eq!(delivered.event(), "cap10-security:delivered");
    assert_eq!(delivered.data(), &serde_json::json!({ "sequence": 1 }));
    let outcome = next_application_event(&mut full_client).await;
    assert_eq!(outcome.event(), "cap10-security:publish-outcome");
    assert_eq!(
        serde_json::from_value::<SecurityPublishOutcome>(outcome.data().clone())
            .expect("decode security publish result"),
        SecurityPublishOutcome {
            sequence: 1,
            result: "accepted".to_owned(),
        }
    );
    timeout(ASSERTION_DEADLINE, full_client.send(Message::Close(None)))
        .await
        .expect("security client close deadline")
        .expect("close security WebSocket client");
    timeout(ASSERTION_DEADLINE, full_app.close())
        .await
        .expect("TLS Redis app close deadline")
        .expect("close TLS Redis app");
    timeout(ASSERTION_DEADLINE, full_start)
        .await
        .expect("TLS Redis lifecycle deadline")
        .expect("TLS Redis lifecycle task panicked")
        .expect("TLS Redis lifecycle failed");
    close_container(full_container).await;

    let publish_only_config = write_app_config(
        fixtures.path(),
        "publish-only.toml",
        redis.port(),
        &ca_path,
        PUBLISH_ONLY_USER,
        PUBLISH_ONLY_PASSWORD,
    );
    let publish_only_container = container(&publish_only_config).await;
    let publish_only_app = timeout(
        ASSERTION_DEADLINE,
        builder(Arc::clone(&publish_only_container), "127.0.0.1:0").build(),
    )
    .await
    .expect("publish-only build deadline")
    .expect("publish-only credential may initialize the publisher");
    let start_error = timeout(ASSERTION_DEADLINE, publish_only_app.start())
        .await
        .expect("publish-only subscription readiness deadline")
        .expect_err("subscriber ACL denial must reject required listener readiness");
    let start_diagnostic = format!("{start_error:?} {start_error}");
    assert!(!start_diagnostic.contains(PUBLISH_ONLY_PASSWORD));
    assert!(matches!(&start_error, ServerError::ConnectionError(_)));
    assert!(start_diagnostic.contains("required WebSocket backplane subscription"));
    assert!(start_diagnostic.contains("readiness timed out"));
    let close_error = timeout(ASSERTION_DEADLINE, publish_only_app.close())
        .await
        .expect("publish-only app close deadline")
        .expect_err("close must replay the observed required-backplane startup failure");
    let close_diagnostic = format!("{close_error:?} {close_error}");
    assert!(!close_diagnostic.contains(PUBLISH_ONLY_PASSWORD));
    assert!(matches!(&close_error, ServerError::ConnectionError(_)));
    assert!(close_diagnostic.contains("required WebSocket backplane subscription"));
    assert!(close_diagnostic.contains("readiness timed out"));
    assert!(!close_diagnostic.contains("framework shutdown was incomplete"));
    assert!(!close_diagnostic.contains("Connection error: Connection error:"));
    close_container(publish_only_container).await;
}
