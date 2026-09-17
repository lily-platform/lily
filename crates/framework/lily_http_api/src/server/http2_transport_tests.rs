// Raw codec conformance services below deliberately bypass managed request
// receipt binding. Their write watchdog exercises the unbound connection
// fallback. Managed request-local stop/isolation is qualified separately in
// server/response_control_tests.rs using the registered executor.
use std::{convert::Infallible, error::Error as StdError, sync::Arc, time::Duration};

use bytes::Bytes;
use http::{Request, Response, StatusCode, Version};
use http_body_util::{BodyExt, Full};
use hyper::{
    body::{Body, Incoming},
    client::conn::http2::{self, SendRequest},
    service::{service_fn, Service},
};
use hyper_util::rt::{TokioExecutor, TokioIo};
use lily_core::enums::HttpProtocol;
use tokio::{
    io::duplex,
    sync::{mpsc, Notify},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use super::server::{
    BoundedResponseBody, ConnectionActivity, ConnectionCloseReason, HttpServer, HttpTransportConfig,
};

struct H2Pair {
    sender: SendRequest<Full<Bytes>>,
    client_driver: JoinHandle<Result<(), String>>,
    server_driver: JoinHandle<Result<ConnectionCloseReason, String>>,
    shutdown: CancellationToken,
}

async fn h2_pair<S, B>(config: HttpTransportConfig, service: S) -> (H2Pair, Arc<ConnectionActivity>)
where
    S: Service<Request<Incoming>, Response = Response<B>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    h2_pair_with_activity(config, service, ConnectionActivity::new()).await
}

async fn h2_pair_with_activity<S, B>(
    config: HttpTransportConfig,
    service: S,
    activity: Arc<ConnectionActivity>,
) -> (H2Pair, Arc<ConnectionActivity>)
where
    S: Service<Request<Incoming>, Response = Response<B>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    config.validate().expect("test transport config is valid");
    let (client_io, server_io) = duplex(512 * 1024);
    let shutdown = CancellationToken::new();
    let server_shutdown = shutdown.clone();
    let server_activity = Arc::clone(&activity);
    let idle_timeout = config.connection_idle_timeout;
    let builder = HttpServer::connection_builder(&config, HttpProtocol::Auto);
    let server_driver = tokio::spawn(async move {
        HttpServer::drive_connection(
            server_io,
            builder,
            service,
            server_shutdown,
            server_activity,
            idle_timeout,
        )
        .await
        .map_err(|error| format!("{error:?}"))
    });

    let mut client_builder = http2::Builder::new(TokioExecutor::new());
    client_builder
        .initial_stream_window_size(64 * 1024)
        .initial_connection_window_size(128 * 1024)
        .max_frame_size(16 * 1024);
    let (sender, connection) = client_builder
        .handshake(TokioIo::new(client_io))
        .await
        .expect("the in-memory HTTP/2 handshake must succeed");
    let client_driver =
        tokio::spawn(async move { connection.await.map_err(|error| error.to_string()) });
    (
        H2Pair {
            sender,
            client_driver,
            server_driver,
            shutdown,
        },
        activity,
    )
}

fn request(path: &'static str, body: impl Into<Bytes>) -> Request<Full<Bytes>> {
    Request::builder()
        .method("POST")
        .uri(path)
        .header(http::header::HOST, "lily.test")
        .body(Full::new(body.into()))
        .expect("the static HTTP/2 request is valid")
}

async fn cleanup(pair: H2Pair) {
    pair.shutdown.cancel();
    let server = tokio::time::timeout(Duration::from_secs(2), pair.server_driver)
        .await
        .expect("the HTTP/2 server driver must stop within the test deadline")
        .expect("the HTTP/2 server task must not panic");
    if let Err(error) = server {
        assert!(
            error.contains("BrokenPipe"),
            "server driver failed unexpectedly: {error}"
        );
    }
    drop(pair.sender);
    let client = tokio::time::timeout(Duration::from_secs(2), pair.client_driver)
        .await
        .expect("the HTTP/2 client driver must stop within the test deadline")
        .expect("the HTTP/2 client task must not panic");
    assert!(client.is_ok(), "client driver failed: {client:?}");
}

#[test]
fn http2_resource_limits_reject_zero_and_pathological_values() {
    let mut config = HttpTransportConfig::default();
    assert!(config.validate().is_ok());

    config.http2_max_concurrent_streams = 0;
    assert!(config.validate().is_err());
    config.http2_max_concurrent_streams = 10_001;
    assert!(config.validate().is_err());
    config.http2_max_concurrent_streams = 100;

    config.http2_initial_stream_window_bytes = 0;
    assert!(config.validate().is_err());
    config.http2_initial_stream_window_bytes = 1_u32 << 31;
    assert!(config.validate().is_err());
    config.http2_initial_stream_window_bytes = 65_535;

    config.http2_initial_connection_window_bytes = 1_u32 << 31;
    assert!(config.validate().is_err());
    config.http2_initial_connection_window_bytes = 65_535;

    config.http2_max_frame_bytes = 16_383;
    assert!(config.validate().is_err());
    config.http2_max_frame_bytes = 16_384;
    config.http2_max_send_buffer_bytes = 16 * 1024 * 1024 + 1;
    assert!(config.validate().is_err());
    config.http2_max_send_buffer_bytes = 16_384;

    config.http2_max_pending_accept_reset_streams = 0;
    assert!(config.validate().is_err());
    config.http2_max_pending_accept_reset_streams = 20;
    config.http2_max_local_error_reset_streams = 0;
    assert!(config.validate().is_err());
    config.http2_max_local_error_reset_streams = 128;

    config.connection_idle_timeout = Duration::ZERO;
    assert!(config.validate().is_err());
    config.connection_idle_timeout = Duration::from_secs(1);
    config.request_timeout = Duration::ZERO;
    assert!(config.validate().is_err());
}

#[tokio::test]
async fn production_builder_negotiates_h2_and_multiplexes_concurrent_streams() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let release = Arc::new(Notify::new());
    let service = service_fn({
        let release = Arc::clone(&release);
        move |request: Request<Incoming>| {
            let started_tx = started_tx.clone();
            let release = Arc::clone(&release);
            async move {
                started_tx
                    .send(request.uri().path().to_string())
                    .expect("request observer remains alive");
                release.notified().await;
                Ok::<_, Infallible>(
                    Response::builder()
                        .body(Full::new(Bytes::from_static(b"ok")))
                        .unwrap(),
                )
            }
        }
    });
    let config = HttpTransportConfig {
        connection_idle_timeout: Duration::from_secs(5),
        ..HttpTransportConfig::default()
    };
    let (pair, _activity) = h2_pair(config, service).await;

    let mut first_sender = pair.sender.clone();
    let first = tokio::spawn(async move {
        first_sender.ready().await.unwrap();
        first_sender
            .send_request(request("/one", Bytes::new()))
            .await
    });
    let mut second_sender = pair.sender.clone();
    let second = tokio::spawn(async move {
        second_sender.ready().await.unwrap();
        second_sender
            .send_request(request("/two", Bytes::new()))
            .await
    });

    let first_path = tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let second_path = tokio::time::timeout(Duration::from_secs(1), started_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_ne!(first_path, second_path);
    release.notify_waiters();

    for response in [
        first.await.unwrap().unwrap(),
        second.await.unwrap().unwrap(),
    ] {
        assert_eq!(response.version(), Version::HTTP_2);
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.into_body().collect().await.unwrap().to_bytes(),
            "ok"
        );
    }
    cleanup(pair).await;
}

#[tokio::test]
async fn max_concurrent_streams_defers_the_next_stream_until_capacity_returns() {
    let (started_tx, mut started_rx) = mpsc::unbounded_channel();
    let release_first = Arc::new(Notify::new());
    let service = service_fn({
        let release_first = Arc::clone(&release_first);
        move |request: Request<Incoming>| {
            let started_tx = started_tx.clone();
            let release_first = Arc::clone(&release_first);
            async move {
                let path = request.uri().path().to_string();
                started_tx.send(path.clone()).unwrap();
                if path == "/first" {
                    release_first.notified().await;
                }
                Ok::<_, Infallible>(Response::new(Full::new(Bytes::from(path))))
            }
        }
    });
    let config = HttpTransportConfig {
        http2_max_concurrent_streams: 1,
        connection_idle_timeout: Duration::from_secs(5),
        ..HttpTransportConfig::default()
    };
    let (pair, _activity) = h2_pair(config, service).await;

    let mut first_sender = pair.sender.clone();
    let first = tokio::spawn(async move {
        first_sender.ready().await.unwrap();
        first_sender
            .send_request(request("/first", Bytes::new()))
            .await
    });
    assert_eq!(started_rx.recv().await.as_deref(), Some("/first"));

    let mut second_sender = pair.sender.clone();
    let second = tokio::spawn(async move {
        second_sender.ready().await.unwrap();
        second_sender
            .send_request(request("/second", Bytes::new()))
            .await
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), started_rx.recv())
            .await
            .is_err(),
        "the second stream entered before the configured stream slot was returned"
    );

    release_first.notify_waiters();
    first.await.unwrap().unwrap();
    assert_eq!(started_rx.recv().await.as_deref(), Some("/second"));
    second.await.unwrap().unwrap();
    cleanup(pair).await;
}

#[tokio::test]
async fn header_list_limit_rejects_the_stream_before_application_dispatch() {
    let admitted = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let service = service_fn({
        let admitted = Arc::clone(&admitted);
        move |request: Request<Incoming>| {
            let admitted = Arc::clone(&admitted);
            async move {
                let status =
                    if HttpServer::convert_request_headers(request.headers(), 64, 128).is_ok() {
                        admitted.store(true, std::sync::atomic::Ordering::SeqCst);
                        StatusCode::OK
                    } else {
                        StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
                    };
                Ok::<_, Infallible>(
                    Response::builder()
                        .status(status)
                        .body(Full::new(Bytes::new()))
                        .unwrap(),
                )
            }
        }
    });
    let config = HttpTransportConfig {
        max_request_header_bytes: 128,
        connection_idle_timeout: Duration::from_secs(5),
        ..HttpTransportConfig::default()
    };
    let (pair, _activity) = h2_pair(config, service).await;
    let mut sender = pair.sender.clone();
    sender.ready().await.unwrap();
    let oversized = Request::builder()
        .uri("/headers")
        .header(http::header::HOST, "lily.test")
        .header("x-oversized", "x".repeat(1024))
        .body(Full::new(Bytes::new()))
        .unwrap();

    match sender.send_request(oversized).await {
        Ok(response) => assert_eq!(
            response.status(),
            StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE
        ),
        Err(error) => assert!(
            error.to_string().contains("connection error")
                || format!("{error:?}").contains("header_list_way_too_large")
        ),
    }
    assert!(
        !admitted.load(std::sync::atomic::Ordering::SeqCst),
        "an oversized header list reached application dispatch"
    );
    drop(sender);
    let H2Pair {
        sender,
        client_driver,
        server_driver,
        shutdown: _,
    } = pair;
    drop(sender);
    let server_error = tokio::time::timeout(Duration::from_secs(1), server_driver)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(server_error.contains("ENHANCE_YOUR_CALM"));
    let _ = tokio::time::timeout(Duration::from_secs(1), client_driver)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn idle_h2_connection_closes_with_a_typed_outcome() {
    let service = service_fn(move |_request: Request<Incoming>| async move {
        Ok::<_, Infallible>(Response::new(Full::new(Bytes::new())))
    });
    let config = HttpTransportConfig {
        connection_idle_timeout: Duration::from_millis(25),
        ..HttpTransportConfig::default()
    };
    let (pair, _activity) = h2_pair(config, service).await;
    let H2Pair {
        mut sender,
        client_driver,
        server_driver,
        shutdown: _,
    } = pair;

    let reason = tokio::time::timeout(Duration::from_secs(1), server_driver)
        .await
        .expect("idle connection shutdown must be bounded")
        .unwrap()
        .unwrap();
    assert_eq!(reason, ConnectionCloseReason::IdleTimeout);
    let client = tokio::time::timeout(Duration::from_secs(1), client_driver)
        .await
        .unwrap()
        .unwrap();
    assert!(client.is_ok());
    assert!(sender
        .send_request(request("/closed", Bytes::new()))
        .await
        .is_err());
}

#[tokio::test]
async fn oversized_body_is_reset_without_poisoning_the_h2_connection() {
    let service = service_fn(move |request: Request<Incoming>| async move {
        let body_limit = if request.uri().path() == "/healthy" {
            256 * 1024
        } else {
            1024
        };
        let result =
            HttpServer::collect_request_body(request.into_body(), body_limit, 8, 1024).await;
        let status = if result.is_ok() {
            StatusCode::OK
        } else {
            StatusCode::PAYLOAD_TOO_LARGE
        };
        Ok::<_, Infallible>(
            Response::builder()
                .status(status)
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
    });
    let config = HttpTransportConfig {
        connection_idle_timeout: Duration::from_secs(5),
        http2_initial_stream_window_bytes: 16 * 1024,
        http2_initial_connection_window_bytes: 32 * 1024,
        ..HttpTransportConfig::default()
    };
    let (pair, _activity) = h2_pair(config, service).await;
    let mut sender = pair.sender.clone();

    sender.ready().await.unwrap();
    let oversized = sender
        .send_request(request("/too-large", Bytes::from(vec![7_u8; 2048])))
        .await
        .unwrap();
    assert_eq!(oversized.status(), StatusCode::PAYLOAD_TOO_LARGE);
    drop(oversized);

    sender.ready().await.unwrap();
    let healthy = sender
        .send_request(request("/healthy", Bytes::from(vec![9_u8; 128 * 1024])))
        .await
        .unwrap();
    assert_eq!(healthy.status(), StatusCode::OK);
    cleanup(pair).await;
}

#[tokio::test]
async fn unbound_service_write_deadline_uses_connection_fallback_without_leaking_the_stream() {
    let response_activity = ConnectionActivity::new();
    let service = service_fn({
        let response_activity = Arc::clone(&response_activity);
        move |request: Request<Incoming>| {
            let response_activity = Arc::clone(&response_activity);
            async move {
                let body = if request.uri().path() == "/slow-reader" {
                    Bytes::from(vec![5_u8; 512 * 1024])
                } else {
                    Bytes::from_static(b"healthy")
                };
                Ok::<_, Infallible>(Response::new(BoundedResponseBody::new_for_test(
                    body,
                    16 * 1024,
                    Duration::from_millis(25),
                    response_activity.enter(),
                )))
            }
        }
    });
    let config = HttpTransportConfig {
        connection_idle_timeout: Duration::from_secs(5),
        http2_max_send_buffer_bytes: 16 * 1024,
        ..HttpTransportConfig::default()
    };
    let (pair, _connection_activity) =
        h2_pair_with_activity(config, service, Arc::clone(&response_activity)).await;
    let mut sender = pair.sender.clone();

    sender.ready().await.unwrap();
    let slow_response = sender
        .send_request(request("/slow-reader", Bytes::new()))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while response_activity.snapshot().timed_out == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the slow reader must hit the bounded response write deadline");
    assert_eq!(response_activity.snapshot().active, 0);
    assert!(slow_response.into_body().collect().await.is_err());
    assert!(sender
        .send_request(request("/closed", Bytes::new()))
        .await
        .is_err());

    let H2Pair {
        sender: pair_sender,
        client_driver,
        server_driver,
        shutdown: _,
    } = pair;
    drop(sender);
    drop(pair_sender);
    let reason = server_driver.await.unwrap().unwrap();
    assert_eq!(reason, ConnectionCloseReason::ResponseFinalizationTimeout);
    let _ = client_driver.await.unwrap();
}

#[tokio::test]
async fn unbound_graceful_drain_keeps_connection_fallback_and_accounts_for_cancelled_h2_sibling() {
    let activity = ConnectionActivity::new();
    let service = service_fn({
        let activity = activity.clone();
        move |request: Request<Incoming>| {
            let activity = activity.clone();
            async move {
                let timeout = if request.uri().path() == "/slow" {
                    Duration::from_millis(50)
                } else {
                    Duration::from_secs(5)
                };
                Ok::<_, Infallible>(Response::new(BoundedResponseBody::new_for_test(
                    Bytes::from(vec![1; 512 * 1024]),
                    16 * 1024,
                    timeout,
                    activity.enter(),
                )))
            }
        }
    });
    let (pair, _) =
        h2_pair_with_activity(HttpTransportConfig::default(), service, activity.clone()).await;
    let mut a = pair.sender.clone();
    let mut b = pair.sender.clone();
    let (a, b) = tokio::join!(
        a.send_request(request("/slow", Bytes::new())),
        b.send_request(request("/sibling", Bytes::new()))
    );
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(activity.snapshot().active, 2);
    pair.shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(1), async {
        while activity.snapshot().active != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("write watchdog must remain polled inside graceful drain");
    let snapshot = activity.snapshot();
    assert_eq!(snapshot.started, 2);
    assert_eq!(snapshot.completed, 0);
    assert_eq!(snapshot.timed_out, 1);
    assert_eq!(snapshot.cancelled, 1);
    assert!(a.into_body().collect().await.is_err());
    assert!(b.into_body().collect().await.is_err());
    drop(pair.sender);
    assert_eq!(
        pair.server_driver.await.unwrap().unwrap(),
        ConnectionCloseReason::ResponseFinalizationTimeout
    );
    let _ = pair.client_driver.await.unwrap();
}

#[tokio::test]
async fn rst_stream_cancels_the_service_future_and_preserves_other_streams() {
    struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    let (slow_started_tx, slow_started_rx) = tokio::sync::oneshot::channel();
    let slow_started_tx = Arc::new(std::sync::Mutex::new(Some(slow_started_tx)));
    let (slow_dropped_tx, slow_dropped_rx) = tokio::sync::oneshot::channel();
    let slow_dropped_tx = Arc::new(std::sync::Mutex::new(Some(slow_dropped_tx)));
    let never_release = Arc::new(Notify::new());
    let service = service_fn({
        let never_release = Arc::clone(&never_release);
        move |request: Request<Incoming>| {
            let path = request.uri().path().to_string();
            let never_release = Arc::clone(&never_release);
            let slow_started_tx = Arc::clone(&slow_started_tx);
            let slow_dropped_tx = Arc::clone(&slow_dropped_tx);
            async move {
                if path == "/slow" {
                    if let Some(sender) = slow_started_tx.lock().unwrap().take() {
                        let _ = sender.send(());
                    }
                    let _drop_signal = DropSignal(slow_dropped_tx.lock().unwrap().take());
                    never_release.notified().await;
                }
                Ok::<_, Infallible>(Response::new(Full::new(Bytes::from(path))))
            }
        }
    });
    let config = HttpTransportConfig {
        connection_idle_timeout: Duration::from_secs(5),
        ..HttpTransportConfig::default()
    };
    let (pair, _activity) = h2_pair(config, service).await;

    let mut slow_sender = pair.sender.clone();
    let slow = tokio::spawn(async move {
        slow_sender.ready().await.unwrap();
        slow_sender
            .send_request(request("/slow", Bytes::new()))
            .await
    });
    slow_started_rx.await.unwrap();
    slow.abort();
    let _ = slow.await;
    tokio::time::timeout(Duration::from_secs(1), slow_dropped_rx)
        .await
        .expect("RST_STREAM must cancel and drop the server service future")
        .unwrap();

    let mut healthy_sender = pair.sender.clone();
    healthy_sender.ready().await.unwrap();
    let healthy = healthy_sender
        .send_request(request("/healthy", Bytes::new()))
        .await
        .unwrap();
    assert_eq!(healthy.status(), StatusCode::OK);
    cleanup(pair).await;
}

#[tokio::test]
async fn graceful_shutdown_sends_goaway_and_drains_the_accepted_stream() {
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let started_tx = Arc::new(std::sync::Mutex::new(Some(started_tx)));
    let release = Arc::new(Notify::new());
    let service = service_fn({
        let release = Arc::clone(&release);
        move |_request: Request<Incoming>| {
            let release = Arc::clone(&release);
            let started_tx = Arc::clone(&started_tx);
            async move {
                if let Some(sender) = started_tx.lock().unwrap().take() {
                    let _ = sender.send(());
                }
                release.notified().await;
                Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"drained"))))
            }
        }
    });
    let config = HttpTransportConfig {
        connection_idle_timeout: Duration::from_secs(5),
        ..HttpTransportConfig::default()
    };
    let (mut pair, _activity) = h2_pair(config, service).await;

    let mut accepted_sender = pair.sender.clone();
    let accepted = tokio::spawn(async move {
        accepted_sender.ready().await.unwrap();
        accepted_sender
            .send_request(request("/accepted", Bytes::new()))
            .await
    });
    started_rx.await.unwrap();
    pair.shutdown.cancel();

    release.notify_waiters();
    let response = accepted.await.unwrap().unwrap();
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        "drained"
    );

    let reason = tokio::time::timeout(Duration::from_secs(2), pair.server_driver)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(reason, ConnectionCloseReason::GracefulShutdown);
    let client = tokio::time::timeout(Duration::from_secs(2), pair.client_driver)
        .await
        .unwrap()
        .unwrap();
    assert!(client.is_ok());
    assert!(pair.sender.is_closed());
    assert!(pair
        .sender
        .send_request(request("/after-goaway", Bytes::new()))
        .await
        .is_err());
}
