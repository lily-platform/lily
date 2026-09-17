use super::*;

use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::{Duration, timeout};
use tokio_util::sync::CancellationToken;

struct RunningApp {
    app: WsApp,
    admission: CancellationToken,
    server_task: JoinHandle<Result<(), ServerError>>,
    address: std::net::SocketAddr,
}

impl RunningApp {
    async fn start(config: ServerConfig) -> Self {
        let reservation =
            std::net::TcpListener::bind("127.0.0.1:0").expect("reserve loopback address");
        let address = reservation.local_addr().expect("read loopback address");
        drop(reservation);

        let app = WsAppBuilder::new(&address.to_string())
            .config(config)
            .build()
            .await
            .expect("build qualification application");
        let admission = CancellationToken::new();
        let runtime = app.runtime_clone();
        let server_admission = admission.clone();
        let server_task = tokio::spawn(async move {
            runtime
                .serve_until_shutdown(
                    server_admission,
                    CancellationToken::new(),
                    CancellationToken::new(),
                )
                .await
        });

        Self {
            app,
            admission,
            server_task,
            address,
        }
    }

    async fn stop(self) {
        self.admission.cancel();
        timeout(Duration::from_secs(3), self.server_task)
            .await
            .expect("qualification server shutdown deadline")
            .expect("qualification server task panicked")
            .expect("qualification server shutdown");
        assert_reconciled(&self.app).await;
        self.app
            .container
            .close()
            .await
            .expect("close qualification container");
    }
}

#[derive(Debug)]
struct ServerFrame {
    fin: bool,
    opcode: u8,
    payload: Vec<u8>,
}

const VALID_WEBSOCKET_KEY: &str = "dGhlIHNhbXBsZSBub25jZQ==";

async fn connect_when_ready(address: std::net::SocketAddr) -> TcpStream {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        match TcpStream::connect(address).await {
            Ok(stream) => return stream,
            Err(error) if tokio::time::Instant::now() < deadline => {
                let _ = error;
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(error) => panic!("qualification listener did not become ready: {error}"),
        }
    }
}

async fn raw_upgrade(address: std::net::SocketAddr) -> BufReader<TcpStream> {
    let request = raw_upgrade_request(
        address,
        "GET",
        "HTTP/1.1",
        Some("Upgrade"),
        Some("websocket"),
        Some("13"),
        Some(VALID_WEBSOCKET_KEY),
    );
    let (stream, response) = raw_http_exchange(address, &request).await;
    assert!(
        response.starts_with("HTTP/1.1 101 "),
        "expected HTTP 101, received {response:?}"
    );
    stream
}

fn raw_upgrade_request(
    address: std::net::SocketAddr,
    method: &str,
    http_version: &str,
    connection: Option<&str>,
    upgrade: Option<&str>,
    websocket_version: Option<&str>,
    websocket_key: Option<&str>,
) -> String {
    let mut lines = vec![
        format!("{method} /ws?namespace=orders {http_version}"),
        format!("Host: {address}"),
    ];
    if let Some(value) = upgrade {
        lines.push(format!("Upgrade: {value}"));
    }
    if let Some(value) = connection {
        lines.push(format!("Connection: {value}"));
    }
    if let Some(value) = websocket_key {
        lines.push(format!("Sec-WebSocket-Key: {value}"));
    }
    if let Some(value) = websocket_version {
        lines.push(format!("Sec-WebSocket-Version: {value}"));
    }
    lines.push("Sec-WebSocket-Protocol: lily.v2".to_owned());
    format!("{}\r\n\r\n", lines.join("\r\n"))
}

async fn raw_http_exchange(
    address: std::net::SocketAddr,
    request: &str,
) -> (BufReader<TcpStream>, String) {
    let stream = connect_when_ready(address).await;
    let mut stream = BufReader::new(stream);
    stream
        .get_mut()
        .write_all(request.as_bytes())
        .await
        .expect("write raw Upgrade request");

    let mut response = Vec::new();
    timeout(Duration::from_secs(2), async {
        loop {
            let read = stream
                .read_until(b'\n', &mut response)
                .await
                .expect("read raw Upgrade response");
            assert!(
                read > 0,
                "server closed before completing its HTTP response"
            );
            assert!(
                response.len() <= 16 * 1024,
                "Upgrade response is unexpectedly large"
            );
            if response.ends_with(b"\r\n\r\n") {
                break;
            }
        }
    })
    .await
    .expect("raw Upgrade response deadline");
    let response = String::from_utf8(response).expect("HTTP response is UTF-8");
    (stream, response)
}

fn client_frame(fin: bool, rsv1: bool, opcode: u8, masked: bool, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(payload.len() + 14);
    frame.push((u8::from(fin) << 7) | (u8::from(rsv1) << 6) | (opcode & 0x0f));
    let mask_bit = u8::from(masked) << 7;
    match payload.len() {
        length @ 0..=125 => frame.push(mask_bit | u8::try_from(length).unwrap()),
        length @ 126..=65_535 => {
            frame.push(mask_bit | 126);
            frame.extend_from_slice(&u16::try_from(length).unwrap().to_be_bytes());
        }
        length => {
            frame.push(mask_bit | 127);
            frame.extend_from_slice(&u64::try_from(length).unwrap().to_be_bytes());
        }
    }
    if masked {
        const MASK: [u8; 4] = [0x11, 0x22, 0x33, 0x44];
        frame.extend_from_slice(&MASK);
        frame.extend(
            payload
                .iter()
                .enumerate()
                .map(|(index, byte)| byte ^ MASK[index % 4]),
        );
    } else {
        frame.extend_from_slice(payload);
    }
    frame
}

async fn write_client_frame(
    stream: &mut BufReader<TcpStream>,
    fin: bool,
    rsv1: bool,
    opcode: u8,
    masked: bool,
    payload: &[u8],
) {
    stream
        .get_mut()
        .write_all(&client_frame(fin, rsv1, opcode, masked, payload))
        .await
        .expect("write raw WebSocket frame");
}

async fn read_server_frame(
    stream: &mut BufReader<TcpStream>,
) -> std::io::Result<Option<ServerFrame>> {
    let mut header = [0_u8; 2];
    match stream.read_exact(&mut header).await {
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error),
    }
    let fin = header[0] & 0x80 != 0;
    let opcode = header[0] & 0x0f;
    let masked = header[1] & 0x80 != 0;
    let mut length = u64::from(header[1] & 0x7f);
    if length == 126 {
        let mut extended = [0_u8; 2];
        stream.read_exact(&mut extended).await?;
        length = u64::from(u16::from_be_bytes(extended));
    } else if length == 127 {
        let mut extended = [0_u8; 8];
        stream.read_exact(&mut extended).await?;
        length = u64::from_be_bytes(extended);
    }
    assert!(!masked, "server frames must not be masked");
    let length = usize::try_from(length)
        .map_err(|_| std::io::Error::other("server frame does not fit in memory"))?;
    assert!(
        length <= 2 * 1024 * 1024,
        "server frame exceeds qualification bound"
    );
    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload).await?;
    Ok(Some(ServerFrame {
        fin,
        opcode,
        payload,
    }))
}

async fn next_server_frame(stream: &mut BufReader<TcpStream>) -> Option<ServerFrame> {
    timeout(Duration::from_secs(2), read_server_frame(stream))
        .await
        .expect("server frame deadline")
        .expect("read server frame")
}

async fn close_raw_client(stream: &mut BufReader<TcpStream>) {
    write_client_frame(stream, true, false, 0x8, true, &1000_u16.to_be_bytes()).await;
    loop {
        let frame = next_server_frame(stream)
            .await
            .expect("server must acknowledge client Close");
        if frame.opcode == 0x8 {
            assert!(frame.fin, "Close acknowledgement must not be fragmented");
            return;
        }
    }
}

async fn assert_server_close_code(stream: &mut BufReader<TcpStream>, expected_code: u16) {
    let frame = next_server_frame(stream)
        .await
        .expect("RFC violation must receive an explicit Close frame");
    assert!(frame.fin, "Close frame must not be fragmented");
    assert_eq!(
        frame.opcode, 0x8,
        "RFC violation must not end with silent EOF"
    );
    assert!(
        frame.payload.len() >= 2,
        "RFC error Close must carry an explicit status code"
    );
    assert_eq!(
        u16::from_be_bytes([frame.payload[0], frame.payload[1]]),
        expected_code
    );
    assert!(
        frame.payload.len() <= 125,
        "Close payload must satisfy the control-frame limit"
    );
    std::str::from_utf8(&frame.payload[2..]).expect("Close reason must be valid UTF-8");
}

async fn assert_reconciled(app: &WsApp) {
    timeout(Duration::from_secs(2), async {
        loop {
            if app.active_connection_count().await == 0
                && app.connection_cleanup_registry.entry_count() == 0
                && app.connection_permits.available_permits() == app.server_config().max_connections
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("connection state did not reconcile");
}

fn noop_envelope() -> Vec<u8> {
    serde_json::to_vec(
        &crate::request::WsMessageBody::try_new("orders:noop", json!({"qualification": "wire"}))
            .expect("construct qualification envelope"),
    )
    .expect("serialize qualification envelope")
}

#[tokio::test]
async fn raw_upgrade_rejects_invalid_http_and_websocket_handshake_matrix() {
    let running = RunningApp::start(ServerConfig {
        allow_missing_origin: true,
        max_connections: 1,
        ping_interval_secs: 60,
        ..ServerConfig::default()
    })
    .await;
    let cases = [
        (
            "method",
            "POST",
            "HTTP/1.1",
            Some("Upgrade"),
            Some("websocket"),
            Some("13"),
            Some(VALID_WEBSOCKET_KEY),
        ),
        (
            "HTTP version",
            "GET",
            "HTTP/1.0",
            Some("Upgrade"),
            Some("websocket"),
            Some("13"),
            Some(VALID_WEBSOCKET_KEY),
        ),
        (
            "missing Connection",
            "GET",
            "HTTP/1.1",
            None,
            Some("websocket"),
            Some("13"),
            Some(VALID_WEBSOCKET_KEY),
        ),
        (
            "invalid Connection",
            "GET",
            "HTTP/1.1",
            Some("keep-alive"),
            Some("websocket"),
            Some("13"),
            Some(VALID_WEBSOCKET_KEY),
        ),
        (
            "missing Upgrade",
            "GET",
            "HTTP/1.1",
            Some("Upgrade"),
            None,
            Some("13"),
            Some(VALID_WEBSOCKET_KEY),
        ),
        (
            "invalid Upgrade",
            "GET",
            "HTTP/1.1",
            Some("Upgrade"),
            Some("h2c"),
            Some("13"),
            Some(VALID_WEBSOCKET_KEY),
        ),
        (
            "missing WebSocket version",
            "GET",
            "HTTP/1.1",
            Some("Upgrade"),
            Some("websocket"),
            None,
            Some(VALID_WEBSOCKET_KEY),
        ),
        (
            "unsupported WebSocket version",
            "GET",
            "HTTP/1.1",
            Some("Upgrade"),
            Some("websocket"),
            Some("12"),
            Some(VALID_WEBSOCKET_KEY),
        ),
        (
            "missing WebSocket key",
            "GET",
            "HTTP/1.1",
            Some("Upgrade"),
            Some("websocket"),
            Some("13"),
            None,
        ),
        (
            "empty WebSocket key",
            "GET",
            "HTTP/1.1",
            Some("Upgrade"),
            Some("websocket"),
            Some("13"),
            Some(""),
        ),
        (
            "oversized WebSocket key",
            "GET",
            "HTTP/1.1",
            Some("Upgrade"),
            Some("websocket"),
            Some("13"),
            Some("dGhlIHNhbXBsZSBub25jZQ==AAAAAAAAAA"),
        ),
        (
            "non-Base64 WebSocket key",
            "GET",
            "HTTP/1.1",
            Some("Upgrade"),
            Some("websocket"),
            Some("13"),
            Some("dGhlIHNhbXBsZSBub25jZQ!!"),
        ),
        (
            "wrong WebSocket nonce length",
            "GET",
            "HTTP/1.1",
            Some("Upgrade"),
            Some("websocket"),
            Some("13"),
            Some("AAAAAAAAAAAAAAAAAAAAAAAA"),
        ),
    ];

    for (name, method, http_version, connection, upgrade, version, key) in cases {
        let request = raw_upgrade_request(
            running.address,
            method,
            http_version,
            connection,
            upgrade,
            version,
            key,
        );
        let (stream, response) = raw_http_exchange(running.address, &request).await;
        assert!(
            response.starts_with("HTTP/1.1 400 "),
            "{name} unexpectedly published an Upgrade: {response:?}"
        );
        assert!(
            !response
                .to_ascii_lowercase()
                .contains("sec-websocket-accept"),
            "{name} rejection must not publish WebSocket acceptance headers"
        );
        drop(stream);
        assert_reconciled(&running.app).await;
    }

    assert_eq!(
        running.app.metrics_snapshot().handshakes_failed,
        u64::try_from(cases.len()).unwrap()
    );

    let mut recovered = raw_upgrade(running.address).await;
    close_raw_client(&mut recovered).await;
    drop(recovered);
    assert_reconciled(&running.app).await;
    assert_eq!(running.app.metrics_snapshot().handshakes_succeeded, 1);
    running.stop().await;
}

#[tokio::test]
async fn fragmented_message_with_interleaved_ping_reaches_the_real_listener() {
    let running = RunningApp::start(ServerConfig {
        allow_missing_origin: true,
        ping_interval_secs: 60,
        ..ServerConfig::default()
    })
    .await;
    let mut client = raw_upgrade(running.address).await;
    let message = noop_envelope();
    let split = message.len() / 2;

    write_client_frame(&mut client, false, false, 0x1, true, &message[..split]).await;
    write_client_frame(&mut client, true, false, 0x9, true, b"qualification-ping").await;
    write_client_frame(&mut client, true, false, 0x0, true, &message[split..]).await;

    let pong = next_server_frame(&mut client)
        .await
        .expect("server Pong response");
    assert_eq!(pong.opcode, 0xA);
    assert_eq!(pong.payload, b"qualification-ping");
    assert!(pong.fin);
    write_client_frame(&mut client, true, false, 0x8, true, &1000_u16.to_be_bytes()).await;
    assert_server_close_code(&mut client, 1000).await;
    drop(client);
    assert_reconciled(&running.app).await;
    assert_eq!(running.app.metrics_snapshot().inbound_messages, 1);
    running.stop().await;
}

#[tokio::test]
async fn real_wire_enforces_exact_message_and_frame_limits() {
    let exact = noop_envelope();
    let running = RunningApp::start(ServerConfig {
        allow_missing_origin: true,
        max_message_size: exact.len(),
        max_frame_size: exact.len(),
        ping_interval_secs: 60,
        ..ServerConfig::default()
    })
    .await;

    let mut accepted = raw_upgrade(running.address).await;
    write_client_frame(&mut accepted, true, false, 0x1, true, &exact).await;
    write_client_frame(&mut accepted, true, false, 0x9, true, b"exact").await;
    let pong = next_server_frame(&mut accepted)
        .await
        .expect("exact-bound message keeps the connection open");
    assert_eq!(
        (pong.opcode, pong.payload.as_slice()),
        (0xA, b"exact".as_slice())
    );
    close_raw_client(&mut accepted).await;
    drop(accepted);
    assert_reconciled(&running.app).await;

    let mut oversized_frame = raw_upgrade(running.address).await;
    let mut maximum_plus_one = exact.clone();
    maximum_plus_one.push(b' ');
    write_client_frame(
        &mut oversized_frame,
        true,
        false,
        0x1,
        true,
        &maximum_plus_one,
    )
    .await;
    assert_server_close_code(&mut oversized_frame, 1009).await;
    drop(oversized_frame);
    assert_reconciled(&running.app).await;

    let mut oversized_fragmented = raw_upgrade(running.address).await;
    let split = maximum_plus_one.len() / 2;
    write_client_frame(
        &mut oversized_fragmented,
        false,
        false,
        0x1,
        true,
        &maximum_plus_one[..split],
    )
    .await;
    write_client_frame(
        &mut oversized_fragmented,
        true,
        false,
        0x0,
        true,
        &maximum_plus_one[split..],
    )
    .await;
    assert_server_close_code(&mut oversized_fragmented, 1009).await;
    drop(oversized_fragmented);
    assert_reconciled(&running.app).await;

    assert_eq!(running.app.metrics_snapshot().inbound_messages, 1);
    running.stop().await;
}

#[tokio::test]
async fn representative_raw_protocol_violations_terminate_and_reconcile() {
    let running = RunningApp::start(ServerConfig {
        allow_missing_origin: true,
        ping_interval_secs: 60,
        ..ServerConfig::default()
    })
    .await;
    let valid = noop_envelope();

    for (fin, rsv1, opcode, masked, payload, expected_close_code) in [
        (true, false, 0x1, false, valid.as_slice(), 1002),
        (true, true, 0x1, true, valid.as_slice(), 1002),
        (true, false, 0x1, true, &[0xff][..], 1007),
        (
            false,
            false,
            0x9,
            true,
            b"fragmented-control".as_slice(),
            1002,
        ),
    ] {
        let mut client = raw_upgrade(running.address).await;
        write_client_frame(&mut client, fin, rsv1, opcode, masked, payload).await;
        assert_server_close_code(&mut client, expected_close_code).await;
        drop(client);
        assert_reconciled(&running.app).await;
    }

    assert_eq!(running.app.metrics_snapshot().inbound_messages, 0);
    running.stop().await;
}

#[tokio::test]
async fn incomplete_raw_upgrade_times_out_releases_capacity_and_recovers() {
    let running = RunningApp::start(ServerConfig {
        allow_missing_origin: true,
        max_connections: 1,
        handshake_timeout_secs: 1,
        ping_interval_secs: 60,
        ..ServerConfig::default()
    })
    .await;
    let mut stalled = connect_when_ready(running.address).await;
    stalled
        .write_all(b"GET /ws?namespace=orders HTTP/1.1\r\nHost: stalled\r\n")
        .await
        .expect("write incomplete Upgrade");

    timeout(Duration::from_secs(1), async {
        loop {
            if running.app.connection_permits.available_permits() == 0 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("incomplete Upgrade never consumed its connection permit");

    let mut byte = [0_u8; 1];
    let terminal = timeout(Duration::from_secs(2), stalled.read(&mut byte))
        .await
        .expect("incomplete Upgrade timeout deadline");
    assert!(matches!(terminal, Ok(0) | Err(_)));
    drop(stalled);
    assert_reconciled(&running.app).await;
    assert_eq!(running.app.metrics_snapshot().timeouts, 1);

    let mut recovered = raw_upgrade(running.address).await;
    close_raw_client(&mut recovered).await;
    drop(recovered);
    assert_reconciled(&running.app).await;
    running.stop().await;
}

#[tokio::test]
async fn real_slow_reader_backpressure_does_not_block_a_healthy_peer() {
    let running = RunningApp::start(ServerConfig {
        allow_missing_origin: true,
        max_message_size: 256 * 1024,
        max_frame_size: 256 * 1024,
        max_outbound_message_size: 128 * 1024,
        outbound_queue_capacity: 4,
        outbound_queue_max_bytes: 512 * 1024,
        outbound_admission_timeout_millis: 100,
        write_timeout_millis: 1_000,
        write_buffer_size_bytes: 1024,
        max_write_buffer_size_bytes: 256 * 1024,
        ping_interval_secs: 60,
        ..ServerConfig::default()
    })
    .await;

    let slow_tcp = connect_when_ready(running.address).await;
    let (slow_client, _) = tokio_tungstenite::client_async(
        format!("ws://{}/ws?namespace=orders", running.address),
        slow_tcp,
    )
    .await
    .expect("slow-reader Upgrade");

    let healthy_tcp = connect_when_ready(running.address).await;
    let (mut healthy_client, _) = tokio_tungstenite::client_async(
        format!("ws://{}/ws?namespace=orders", running.address),
        healthy_tcp,
    )
    .await
    .expect("healthy-reader Upgrade");

    timeout(Duration::from_secs(2), async {
        while running.app.active_connection_count().await != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("both real clients must enter the connection manager");

    let healthy_reader = tokio::spawn(async move {
        while let Some(message) = healthy_client.next().await {
            match message.expect("healthy client frame") {
                tokio_tungstenite::tungstenite::Message::Text(bytes) => {
                    let body: crate::request::WsMessageBody =
                        serde_json::from_str(bytes.as_str()).expect("decode healthy text envelope");
                    if body.event == "orders:healthy-marker" {
                        return;
                    }
                }
                tokio_tungstenite::tungstenite::Message::Binary(bytes) => {
                    let body: crate::request::WsMessageBody =
                        serde_json::from_slice(&bytes).expect("decode healthy binary envelope");
                    if body.event == "orders:healthy-marker" {
                        return;
                    }
                }
                tokio_tungstenite::tungstenite::Message::Close(frame) => {
                    panic!("healthy client closed before marker: {frame:?}")
                }
                _ => {}
            }
        }
        panic!("healthy client ended before marker");
    });

    let load = crate::request::WsMessageBody::try_new(
        "orders:slow-reader-load",
        json!({"payload": "x".repeat(96 * 1024)}),
    )
    .expect("construct bounded load envelope");
    let mut observed_slow_terminal = false;
    for _ in 0..512 {
        let report = running
            .app
            .connection_manager()
            .broadcast(crate::BroadcastMessage {
                target: crate::BroadcastTarget::Namespace("orders".to_owned()),
                message: load.clone(),
                wire_format: crate::request::WsWireFormat::Binary,
                exclude: Vec::new(),
            })
            .await
            .expect("broadcast load frame");
        if report.backpressured > 0 || report.closed > 0 {
            observed_slow_terminal = true;
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        observed_slow_terminal,
        "a non-reading real peer must eventually exert bounded pressure"
    );
    assert!(running.app.metrics_snapshot().backpressured > 0);

    let marker =
        crate::request::WsMessageBody::try_new("orders:healthy-marker", json!({"delivered": true}))
            .expect("construct healthy marker");
    for _ in 0..100 {
        running
            .app
            .connection_manager()
            .broadcast(crate::BroadcastMessage {
                target: crate::BroadcastTarget::Namespace("orders".to_owned()),
                message: marker.clone(),
                wire_format: crate::request::WsWireFormat::Text,
                exclude: Vec::new(),
            })
            .await
            .expect("broadcast healthy marker");
        if healthy_reader.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    timeout(Duration::from_secs(3), healthy_reader)
        .await
        .expect("healthy peer marker deadline")
        .expect("healthy peer task panicked");

    drop(slow_client);
    assert_reconciled(&running.app).await;
    running.stop().await;
}

/// Runs the external Autobahn server conformance suite against the exact
/// Tungstenite transport configuration used by Lily. The normal raw-wire
/// tests above cover Lily's custom pre-upgrade and cleanup boundaries; this
/// opt-in gate deliberately supplies Autobahn's required raw echo behavior
/// instead of weakening the production Lily v2 application protocol.
///
/// The pinned Docker image and host networking follow Autobahn's recommended
/// reproducible execution model. Run explicitly on Linux with Docker:
///
/// `cargo test --locked -p lily_websocket --all-features autobahn_server_conformance -- --ignored --nocapture`
#[tokio::test]
#[ignore = "requires Docker and the pinned Autobahn Testsuite image"]
async fn autobahn_server_conformance() {
    const AGENT: &str = "Lily-WebSocket";
    const EXPECTED_CASES: usize = 247;
    const IMAGE: &str = "crossbario/autobahn-testsuite:25.10.1";

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind Autobahn echo listener");
    let address = listener
        .local_addr()
        .expect("read Autobahn listener address");
    let transport_config = ServerConfig {
        max_message_size: 16 * 1024 * 1024,
        max_frame_size: 16 * 1024 * 1024,
        ..ServerConfig::default()
    }
    .websocket_transport_config();
    let cancellation = CancellationToken::new();
    let server_cancellation = cancellation.clone();
    let server_task = tokio::spawn(async move {
        let mut connections = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                _ = server_cancellation.cancelled() => break,
                accepted = listener.accept() => {
                    let (stream, _) = accepted.expect("accept Autobahn connection");
                    let config = transport_config;
                    connections.spawn(async move {
                        let mut socket = tokio_tungstenite::accept_async_with_config(
                            stream,
                            Some(config),
                        )
                        .await
                        .expect("Autobahn WebSocket Upgrade");
                        while let Some(frame) = socket.next().await {
                            let frame = match frame {
                                Ok(frame) => frame,
                                Err(_) => break,
                            };
                            let closing = matches!(
                                &frame,
                                tokio_tungstenite::tungstenite::Message::Close(_)
                            );
                            match &frame {
                                tokio_tungstenite::tungstenite::Message::Text(_)
                                | tokio_tungstenite::tungstenite::Message::Binary(_)
                                | tokio_tungstenite::tungstenite::Message::Close(_) => {
                                    if socket.send(frame).await.is_err() || closing {
                                        break;
                                    }
                                }
                                tokio_tungstenite::tungstenite::Message::Ping(payload) => {
                                    if socket
                                        .send(tokio_tungstenite::tungstenite::Message::Pong(
                                            payload.clone(),
                                        ))
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                                tokio_tungstenite::tungstenite::Message::Pong(_)
                                | tokio_tungstenite::tungstenite::Message::Frame(_) => {}
                            }
                        }
                    });
                }
            }
        }
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    });

    let evidence = std::env::temp_dir().join(format!(
        "lily-websocket-autobahn-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4()
    ));
    let reports = evidence.join("reports");
    std::fs::create_dir_all(&reports).expect("create Autobahn report directory");
    let specification = evidence.join("fuzzingclient.json");
    let specification_json = serde_json::json!({
        "outdir": "/reports",
        "servers": [{
            "agent": AGENT,
            "url": format!("ws://{address}")
        }],
        "cases": ["*"],
        "exclude-cases": ["9.*", "12.*", "13.*"],
        "exclude-agent-cases": {}
    });
    std::fs::write(
        &specification,
        serde_json::to_vec_pretty(&specification_json).expect("serialize Autobahn spec"),
    )
    .expect("write Autobahn spec");

    let specification_mount = format!("{}:/config/fuzzingclient.json:ro", specification.display());
    let reports_mount = format!("{}:/reports", reports.display());
    let output = timeout(
        Duration::from_secs(10 * 60),
        tokio::process::Command::new("docker")
            .args([
                "run",
                "--rm",
                "--network",
                "host",
                "-v",
                &specification_mount,
                "-v",
                &reports_mount,
                IMAGE,
                "wstest",
                "-m",
                "fuzzingclient",
                "-s",
                "/config/fuzzingclient.json",
            ])
            .output(),
    )
    .await
    .expect("Autobahn execution deadline")
    .expect("execute Docker Autobahn client");
    cancellation.cancel();
    timeout(Duration::from_secs(2), server_task)
        .await
        .expect("Autobahn echo server shutdown deadline")
        .expect("Autobahn echo server task panicked");
    assert!(
        output.status.success(),
        "Autobahn runner failed:\nstdout:\n{}\nstderr:\n{}\nevidence: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
        evidence.display()
    );

    let report_path = reports.join("index.json");
    let report: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&report_path).unwrap_or_else(|error| {
            panic!(
                "read Autobahn report {}: {error}; evidence: {}",
                report_path.display(),
                evidence.display()
            )
        }))
        .expect("parse Autobahn index.json");
    let cases = report
        .get(AGENT)
        .and_then(serde_json::Value::as_object)
        .unwrap_or_else(|| {
            panic!(
                "Autobahn report has no {AGENT} agent: {}",
                evidence.display()
            )
        });
    assert_eq!(
        cases.len(),
        EXPECTED_CASES,
        "Autobahn did not execute the complete selected case set; evidence: {}",
        evidence.display()
    );
    let failures: Vec<_> = cases
        .iter()
        .filter_map(|(case, result)| {
            let behavior = result.get("behavior").and_then(serde_json::Value::as_str);
            (!matches!(behavior, Some("OK" | "NON-STRICT" | "INFORMATIONAL")))
                .then(|| format!("{case}: {}", behavior.unwrap_or("missing behavior")))
        })
        .collect();
    assert!(
        failures.is_empty(),
        "Autobahn conformance failures:\n{}\nevidence: {}",
        failures.join("\n"),
        evidence.display()
    );
    std::fs::remove_dir_all(&evidence).expect("remove passing Autobahn evidence");
}
