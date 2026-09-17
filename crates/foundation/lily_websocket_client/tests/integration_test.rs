use futures_util::{SinkExt, StreamExt};
use lily_websocket_client::{
    AuthHeaderProvider, ConnectionState, DecodedPayload, ReconnectionConfig, TokioWsClient,
    WebSocketClientConfig, WebSocketError, WebSocketReply, WsClient, WsMessage,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio::sync::{oneshot, Barrier};
use tokio::time::{sleep, timeout, Duration};
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

fn local_config(url: String) -> WebSocketClientConfig {
    WebSocketClientConfig {
        url,
        namespace: Some("test".into()),
        reconnection: ReconnectionConfig {
            enabled: false,
            ..Default::default()
        },
        ping_interval_secs: 30,
        pong_timeout_secs: 5,
        idle_timeout_secs: 60,
        connect_timeout_secs: 2,
        send_timeout_secs: 2,
        shutdown_timeout_secs: 2,
        close_timeout_secs: 1,
        ..Default::default()
    }
}

async fn echo_server() -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        while let Some(frame) = socket.next().await {
            match frame.unwrap() {
                Message::Text(text) => socket.send(Message::Text(text)).await.unwrap(),
                Message::Binary(data) => socket.send(Message::Binary(data)).await.unwrap(),
                Message::Ping(data) => socket.send(Message::Pong(data)).await.unwrap(),
                Message::Close(_) => {
                    // Tungstenite queues the mandatory Close response while
                    // reading the peer frame. Drive that queued response to
                    // the wire instead of attempting a second Close send.
                    let _ = socket.flush().await;
                    break;
                }
                Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    });
    (format!("ws://{address}"), task)
}

#[tokio::test]
#[ignore = "requires a host environment that permits IPv4 loopback sockets"]
async fn real_text_binary_callbacks_and_bounded_close() {
    let (url, server) = echo_server().await;
    let mut config = local_config(url);
    config.namespace = Some("inventory".into());
    let client = TokioWsClient::with_config(config).unwrap();

    let (event_tx, event_rx) = oneshot::channel();
    let event_tx = Arc::new(Mutex::new(Some(event_tx)));
    client.on("inventory:updated", move |event, data| {
        if let Some(sender) = event_tx.lock().unwrap().take() {
            let _ = sender.send((event, data));
        }
    });
    let (binary_tx, binary_rx) = oneshot::channel();
    let binary_tx = Arc::new(Mutex::new(Some(binary_tx)));
    client.on("inventory:binary", move |_, data| {
        if let Some(sender) = binary_tx.lock().unwrap().take() {
            let _ = sender.send(data);
        }
    });

    client.connect(CancellationToken::new()).await.unwrap();
    assert!(client.is_connected());
    client
        .send("inventory:updated", serde_json::json!({"id": 42}))
        .await
        .unwrap();
    let (event, data) = timeout(Duration::from_secs(2), event_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event, "inventory:updated");
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&data).unwrap()["id"],
        42
    );

    client
        .send_binary_event("inventory:binary", vec![0, 1, 2, 255])
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), binary_rx)
            .await
            .unwrap()
            .unwrap(),
        [0, 1, 2, 255]
    );

    client.disconnect().await.unwrap();
    assert!(!client.is_connected());
    timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = "requires a host environment that permits IPv4 loopback sockets"]
async fn request_uses_one_wire_ack_authority_and_returns_the_typed_reply() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        while let Some(frame) = socket.next().await {
            match frame.unwrap() {
                Message::Text(text) => {
                    let request: WsMessage<serde_json::Value> =
                        serde_json::from_str(&text).unwrap();
                    let acknowledgement_id = request
                        .ack_id
                        .expect("request envelope carries one acknowledgement authority");
                    let response = WsMessage::new_ack(
                        request.event,
                        acknowledgement_id,
                        serde_json::json!({ "saved": true }),
                    );
                    socket
                        .send(Message::Text(serde_json::to_string(&response).unwrap()))
                        .await
                        .unwrap();
                }
                Message::Ping(data) => socket.send(Message::Pong(data)).await.unwrap(),
                Message::Close(_) => {
                    let _ = socket.flush().await;
                    break;
                }
                Message::Binary(_) | Message::Pong(_) | Message::Frame(_) => {}
            }
        }
    });

    let client = TokioWsClient::with_config(local_config(format!("ws://{address}"))).unwrap();
    client.connect(CancellationToken::new()).await.unwrap();
    let reply = client
        .request(
            "test:save",
            serde_json::json!({ "id": 42 }),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert_eq!(
        reply,
        WebSocketReply::Acknowledgement(DecodedPayload::Json(serde_json::json!({ "saved": true })))
    );

    client.disconnect().await.unwrap();
    timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = "requires a host environment that permits IPv4 loopback sockets"]
async fn cancellation_stops_the_single_owned_runtime() {
    let (url, server) = echo_server().await;
    let client = TokioWsClient::with_config(local_config(url)).unwrap();
    let cancellation = CancellationToken::new();
    client.connect(cancellation.clone()).await.unwrap();
    cancellation.cancel();

    timeout(Duration::from_secs(2), async {
        while client.is_connected() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    client.disconnect().await.unwrap();
    timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = "requires a host environment that permits IPv4 loopback sockets"]
async fn wss_uses_the_url_hostname_as_tls_sni() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (sni_sender, sni_receiver) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let acceptor =
            tokio_rustls::LazyConfigAcceptor::new(rustls::server::Acceptor::default(), stream);
        let handshake = acceptor.await.unwrap();
        let _ = sni_sender.send(
            handshake
                .client_hello()
                .server_name()
                .map(ToOwned::to_owned),
        );
        // Dropping before ServerHello deliberately makes the client fail; this
        // qualification fixture observes the ClientHello/SNI contract only.
    });

    let client = TokioWsClient::with_config(local_config(format!(
        "wss://localhost:{}/socket",
        address.port()
    )))
    .unwrap();
    assert!(client.connect(CancellationToken::new()).await.is_err());
    assert_eq!(sni_receiver.await.unwrap().as_deref(), Some("localhost"));
    server.await.unwrap();
}

#[derive(Default)]
struct RotatingAuthorization {
    calls: AtomicUsize,
}

#[async_trait::async_trait]
impl AuthHeaderProvider for RotatingAuthorization {
    async fn headers(&self) -> Result<Vec<(String, String)>, WebSocketError> {
        let value = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        Ok(vec![(
            "authorization".into(),
            format!("Bearer refreshed-{value}"),
        )])
    }
}

#[tokio::test]
#[ignore = "requires a host environment that permits IPv4 loopback sockets"]
#[allow(clippy::result_large_err)]
async fn reconnect_refreshes_auth_and_does_not_duplicate_the_runtime() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_by_server = seen.clone();
    let server = tokio::spawn(async move {
        for attempt in 0..2 {
            let (stream, _) = listener.accept().await.unwrap();
            let seen = seen_by_server.clone();
            let mut socket = tokio_tungstenite::accept_hdr_async(
                stream,
                move |request: &tokio_tungstenite::tungstenite::handshake::server::Request,
                      response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                    seen.lock().unwrap().push(
                        request
                            .headers()
                            .get("authorization")
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .to_string(),
                    );
                    Ok(response)
                },
            )
            .await
            .unwrap();
            if attempt == 0 {
                socket.send(Message::Close(None)).await.unwrap();
            } else {
                while let Some(frame) = socket.next().await {
                    match frame.unwrap() {
                        Message::Close(_) => {
                            // Reading Close already queued the peer response.
                            let _ = socket.flush().await;
                            break;
                        }
                        Message::Ping(data) => socket.send(Message::Pong(data)).await.unwrap(),
                        _ => {}
                    }
                }
            }
        }
    });

    let mut config = local_config(format!("ws://{address}"));
    config.reconnection = ReconnectionConfig {
        enabled: true,
        max_retries: 5,
        initial_delay_secs: 0,
        max_delay_secs: 1,
        backoff_multiplier: 2.0,
        jitter_ratio: 0.0,
    };
    let client = TokioWsClient::with_config(config).unwrap();
    let provider = Arc::new(RotatingAuthorization::default());
    client.set_auth_header_provider(provider.clone()).await;

    let connects = Arc::new(AtomicUsize::new(0));
    let connects_from_callback = connects.clone();
    client.on("on_connect", move |_, _| {
        connects_from_callback.fetch_add(1, Ordering::SeqCst);
    });
    client.connect(CancellationToken::new()).await.unwrap();
    timeout(Duration::from_secs(3), async {
        while connects.load(Ordering::SeqCst) < 2 {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        seen.lock().unwrap().as_slice(),
        ["Bearer refreshed-1", "Bearer refreshed-2"]
    );
    client.connect(CancellationToken::new()).await.unwrap();
    assert_eq!(connects.load(Ordering::SeqCst), 2);
    client.disconnect().await.unwrap();
    timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
#[ignore = "requires a host environment that permits IPv4 loopback sockets"]
async fn bounded_outbound_queue_reports_backpressure() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let _socket = accept_async(stream).await.unwrap();
        sleep(Duration::from_secs(3)).await;
    });

    let mut config = local_config(format!("ws://{address}"));
    config.outbound_queue_capacity = 1;
    config.max_message_size = 1024 * 1024;
    config.max_frame_size = 1024 * 1024;
    config.send_timeout_secs = 1;
    let client = Arc::new(TokioWsClient::with_config(config).unwrap());
    client.connect(CancellationToken::new()).await.unwrap();

    let workers = 32;
    let barrier = Arc::new(Barrier::new(workers + 1));
    let mut tasks = Vec::new();
    for _ in 0..workers {
        let client = client.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            client.send_binary(vec![7; 1024 * 1024]).await
        }));
    }
    barrier.wait().await;
    let mut backpressured = 0;
    for task in tasks {
        if matches!(
            task.await.unwrap(),
            Err(WebSocketError::Backpressure { .. })
        ) {
            backpressured += 1;
        }
    }
    assert!(backpressured > 0, "bounded queue never exposed saturation");

    let _ = client.disconnect().await;
    server.abort();
    let _ = server.await;
}

#[test]
fn configuration_and_state_are_explicit() {
    let client = TokioWsClient::new("ws://localhost:8080?namespace=test").unwrap();
    assert_eq!(client.connection_state(), ConnectionState::Disconnected);
    assert!(!client.is_connected());

    let invalid = TokioWsClient::new("http://localhost:8080");
    assert!(matches!(
        invalid,
        Err(WebSocketError::InvalidConfiguration(_))
    ));
}
