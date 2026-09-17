use std::{
    convert::Infallible,
    future::{ready, Ready},
    io,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use http::{Request as HttpRequest, Response as HttpResponse, Version};
use http_body_util::{Full, StreamBody as HyperStreamBody};
use hyper::{body::Frame, body::Incoming, client::conn::http2, service::service_fn};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tokio::{
    io::duplex,
    sync::{oneshot, Barrier},
};
use url::Url;

use hyper_util::client::legacy::{
    connect::{Connected, Connection},
    Client as HyperClient,
};
use tower_service::Service;

use super::{
    ClientConfig, HttpClient, OriginKey, PerOriginAdmission, ProtocolPreference,
    TransportBuildSettings,
};
use crate::{
    body::{BinaryBody, Body},
    error::HttpClientError,
    header::HeaderMap,
};

#[derive(Debug)]
struct DeclaredLengthBody {
    declared: usize,
    bytes: Bytes,
}

#[async_trait::async_trait]
impl Body for DeclaredLengthBody {
    fn content_type(&self) -> Option<&str> {
        None
    }

    fn content_length(&self) -> Option<usize> {
        Some(self.declared)
    }

    async fn to_bytes(&mut self) -> crate::Result<Bytes> {
        Ok(self.bytes.clone())
    }
}

struct MockConnection(TokioIo<tokio::io::DuplexStream>);

impl hyper::rt::Read for MockConnection {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buffer)
    }
}

impl hyper::rt::Write for MockConnection {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buffer)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }

    fn is_write_vectored(&self) -> bool {
        self.0.is_write_vectored()
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffers: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.0).poll_write_vectored(cx, buffers)
    }
}

impl Connection for MockConnection {
    fn connected(&self) -> Connected {
        Connected::new().negotiated_h2()
    }
}

#[derive(Clone)]
struct MockH2Connector {
    connections: Arc<AtomicUsize>,
    goaway_after_request: bool,
    response_header: Option<(&'static str, &'static str)>,
}

impl Service<http::Uri> for MockH2Connector {
    type Response = MockConnection;
    type Error = io::Error;
    type Future = Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _destination: http::Uri) -> Self::Future {
        self.connections.fetch_add(1, Ordering::SeqCst);
        let (client_io, server_io) = duplex(64 * 1024);
        let response_header = self.response_header;
        if self.goaway_after_request {
            let (request_seen_tx, request_seen_rx) = oneshot::channel();
            let (shutdown_started_tx, shutdown_started_rx) = oneshot::channel();
            let request_seen_tx = Arc::new(Mutex::new(Some(request_seen_tx)));
            let shutdown_started_rx = Arc::new(Mutex::new(Some(shutdown_started_rx)));
            let service = service_fn(move |request: HttpRequest<Incoming>| {
                let request_seen_tx = Arc::clone(&request_seen_tx);
                let shutdown_started_rx = Arc::clone(&shutdown_started_rx);
                async move {
                    if let Some(sender) = request_seen_tx
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .take()
                    {
                        let _ = sender.send(());
                    }
                    let shutdown_started = shutdown_started_rx
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .take();
                    if let Some(shutdown_started) = shutdown_started {
                        let _ = shutdown_started.await;
                    }
                    let mut response = HttpResponse::new(Full::new(Bytes::copy_from_slice(
                        request.uri().path().as_bytes(),
                    )));
                    if let Some((name, value)) = response_header {
                        response.headers_mut().insert(
                            http::HeaderName::from_static(name),
                            http::HeaderValue::from_static(value),
                        );
                    }
                    Ok::<_, Infallible>(response)
                }
            });
            tokio::spawn(async move {
                let connection = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(server_io), service);
                tokio::pin!(connection);
                tokio::select! {
                    _ = &mut connection => {},
                    _ = request_seen_rx => {
                        connection.as_mut().graceful_shutdown();
                        let _ = shutdown_started_tx.send(());
                        let _ = connection.await;
                    }
                }
            });
        } else {
            let service = service_fn(move |request: HttpRequest<Incoming>| async move {
                let mut response = HttpResponse::new(Full::new(Bytes::copy_from_slice(
                    request.uri().path().as_bytes(),
                )));
                if let Some((name, value)) = response_header {
                    response.headers_mut().insert(
                        http::HeaderName::from_static(name),
                        http::HeaderValue::from_static(value),
                    );
                }
                Ok::<_, Infallible>(response)
            });
            tokio::spawn(async move {
                let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(server_io), service)
                    .await;
            });
        }
        ready(Ok(MockConnection(TokioIo::new(client_io))))
    }
}

fn h2_request(path: &'static str) -> HttpRequest<Full<Bytes>> {
    HttpRequest::builder()
        .version(Version::HTTP_2)
        .uri(format!("https://example.test{path}"))
        .header("host", "example.test")
        .body(Full::new(Bytes::new()))
        .unwrap()
}

#[tokio::test]
async fn one_socketless_h2_connection_multiplexes_binary_responses() {
    let (client_io, server_io) = duplex(64 * 1024);
    let rendezvous = Arc::new(Barrier::new(2));
    let service = service_fn({
        let rendezvous = Arc::clone(&rendezvous);
        move |request: HttpRequest<Incoming>| {
            let rendezvous = Arc::clone(&rendezvous);
            async move {
                assert_eq!(request.version(), Version::HTTP_2);
                rendezvous.wait().await;
                let body = if request.uri().path() == "/one" {
                    Bytes::from_static(&[0, 0xff, 1, 0x80])
                } else {
                    Bytes::from_static(&[2, 0xfe, 3, 0x81])
                };
                Ok::<_, Infallible>(HttpResponse::new(Full::new(body)))
            }
        }
    });
    let server = tokio::spawn(async move {
        hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .max_concurrent_streams(2)
            .serve_connection(TokioIo::new(server_io), service)
            .await
    });

    let (sender, connection) = http2::Builder::new(TokioExecutor::new())
        .handshake(TokioIo::new(client_io))
        .await
        .unwrap();
    let client_driver = tokio::spawn(connection);
    let mut first_sender = sender.clone();
    let mut second_sender = sender;
    let (first, second) = tokio::time::timeout(Duration::from_secs(2), async move {
        tokio::join!(
            first_sender.send_request(h2_request("/one")),
            second_sender.send_request(h2_request("/two"))
        )
    })
    .await
    .expect("both streams must make progress on the same connection");

    let first = HttpClient::collect_body(first.unwrap().into_body(), 16)
        .await
        .unwrap();
    let second = HttpClient::collect_body(second.unwrap().into_body(), 16)
        .await
        .unwrap();
    assert_eq!(first, Bytes::from_static(&[0, 0xff, 1, 0x80]));
    assert_eq!(second, Bytes::from_static(&[2, 0xfe, 3, 0x81]));

    client_driver.abort();
    server.abort();
}

#[tokio::test]
async fn hyper_pool_reuses_one_h2_connection_per_origin() {
    let connections = Arc::new(AtomicUsize::new(0));
    let connector = MockH2Connector {
        connections: Arc::clone(&connections),
        goaway_after_request: false,
        response_header: None,
    };
    let mut builder = HyperClient::builder(TokioExecutor::new());
    builder.http2_only(true).retry_canceled_requests(true);
    let client = builder.build::<_, Full<Bytes>>(connector);

    for uri in [
        "http://one.example.test/first",
        "http://one.example.test/second",
    ] {
        let response = client
            .request(
                HttpRequest::builder()
                    .uri(uri)
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = HttpClient::collect_body(response.into_body(), 64)
            .await
            .unwrap();
        assert!(!body.is_empty());
    }
    assert_eq!(connections.load(Ordering::SeqCst), 1);

    let response = client
        .request(
            HttpRequest::builder()
                .uri("http://two.example.test/third")
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    let _body = HttpClient::collect_body(response.into_body(), 64)
        .await
        .unwrap();
    assert_eq!(connections.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn h2_upstream_limit_preserves_the_public_header_byte_budget() {
    const HEADER_NAME: &str = "x-budget";
    const HEADER_VALUE: &str =
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    let config = ClientConfig::default()
        .with_max_header_count(4)
        .with_max_header_bytes(192);
    config.validate().unwrap();
    let settings = TransportBuildSettings::from_config(
        &config,
        Duration::from_secs(1),
        ProtocolPreference::Http2Only,
    );
    assert!(
        HEADER_NAME.len() + HEADER_VALUE.len() <= config.max_header_bytes,
        "the response is inside Lily's public name-plus-value byte budget"
    );
    assert!(
        32 + 7 + 3 + 32 + HEADER_NAME.len() + HEADER_VALUE.len() > config.max_header_bytes,
        "the same response would exceed the old raw h2 parser setting"
    );

    let connections = Arc::new(AtomicUsize::new(0));
    let connector = MockH2Connector {
        connections,
        goaway_after_request: false,
        response_header: Some((HEADER_NAME, HEADER_VALUE)),
    };
    let mut builder = HyperClient::builder(TokioExecutor::new());
    builder
        .http2_only(true)
        .http2_max_header_list_size(settings.http2_max_header_list_bytes);
    let client = builder.build::<_, Full<Bytes>>(connector);

    let response = client
        .request(
            HttpRequest::builder()
                .uri("http://header-budget.example.test/bounded-head")
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    let headers = HttpClient::convert_response_headers(
        response.headers(),
        config.max_header_count,
        config.max_header_bytes,
    )
    .unwrap();

    assert_eq!(headers.get(HEADER_NAME), Some(HEADER_VALUE));
}

#[tokio::test]
async fn hyper_pool_reconnects_after_peer_goaway_without_replaying_an_accepted_stream() {
    let connections = Arc::new(AtomicUsize::new(0));
    let connector = MockH2Connector {
        connections: Arc::clone(&connections),
        goaway_after_request: true,
        response_header: None,
    };
    let mut builder = HyperClient::builder(TokioExecutor::new());
    builder.http2_only(true).retry_canceled_requests(true);
    let client = builder.build::<_, Full<Bytes>>(connector);

    for uri in [
        "http://one.example.test/accepted-once",
        "http://one.example.test/new-connection",
    ] {
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            client.request(
                HttpRequest::builder()
                    .uri(uri)
                    .body(Full::new(Bytes::new()))
                    .unwrap(),
            ),
        )
        .await
        .expect("the pool must make progress after GOAWAY")
        .unwrap();
        let body = HttpClient::collect_body(response.into_body(), 64)
            .await
            .unwrap();
        assert!(!body.is_empty());
    }

    assert_eq!(
        connections.load(Ordering::SeqCst),
        2,
        "the accepted request must not be replayed; the next request uses one fresh connection"
    );
}

#[tokio::test]
async fn cancelling_one_h2_stream_keeps_the_shared_connection_usable() {
    let (client_io, server_io) = duplex(64 * 1024);
    let (cancel_seen_tx, cancel_seen_rx) = oneshot::channel();
    let cancel_seen_tx = Arc::new(Mutex::new(Some(cancel_seen_tx)));
    let service = service_fn({
        let cancel_seen_tx = Arc::clone(&cancel_seen_tx);
        move |request: HttpRequest<Incoming>| {
            let cancel_seen_tx = Arc::clone(&cancel_seen_tx);
            async move {
                if request.uri().path() == "/cancel" {
                    if let Some(sender) = cancel_seen_tx
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .take()
                    {
                        let _ = sender.send(());
                    }
                    std::future::pending::<Result<HttpResponse<Full<Bytes>>, Infallible>>().await
                } else {
                    Ok(HttpResponse::new(Full::new(Bytes::from_static(b"alive"))))
                }
            }
        }
    });
    let server = tokio::spawn(async move {
        hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .serve_connection(TokioIo::new(server_io), service)
            .await
    });

    let (sender, connection) = http2::Builder::new(TokioExecutor::new())
        .handshake(TokioIo::new(client_io))
        .await
        .unwrap();
    let client_driver = tokio::spawn(connection);
    let mut canceled_sender = sender.clone();
    let canceled =
        tokio::spawn(async move { canceled_sender.send_request(h2_request("/cancel")).await });
    cancel_seen_rx.await.unwrap();
    canceled.abort();
    let _ = canceled.await;

    let mut surviving_sender = sender;
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        surviving_sender.send_request(h2_request("/alive")),
    )
    .await
    .expect("cancelling one stream must not stall the connection")
    .unwrap();
    let body = HttpClient::collect_body(response.into_body(), 16)
        .await
        .unwrap();
    assert_eq!(body, Bytes::from_static(b"alive"));

    client_driver.abort();
    server.abort();
}

#[tokio::test]
async fn goaway_closes_the_old_sender_and_returns_an_unstarted_request() {
    let (client_io, server_io) = duplex(64 * 1024);
    let service = service_fn(|_request: HttpRequest<Incoming>| async {
        Ok::<_, Infallible>(HttpResponse::new(Full::new(Bytes::from_static(b"done"))))
    });
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let connection = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .serve_connection(TokioIo::new(server_io), service);
        tokio::pin!(connection);
        tokio::select! {
            result = &mut connection => result,
            _ = shutdown_rx => {
                connection.as_mut().graceful_shutdown();
                connection.await
            }
        }
    });

    let (mut sender, connection) = http2::Builder::new(TokioExecutor::new())
        .handshake(TokioIo::new(client_io))
        .await
        .unwrap();
    let client_driver = tokio::spawn(connection);
    let response = sender.send_request(h2_request("/first")).await.unwrap();
    assert_eq!(
        HttpClient::collect_body(response.into_body(), 16)
            .await
            .unwrap(),
        Bytes::from_static(b"done")
    );

    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !sender.is_closed() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the peer GOAWAY must close this sender");

    let mut error = sender
        .try_send_request(h2_request("/after-goaway"))
        .await
        .unwrap_err();
    assert!(
        error.take_message().is_some(),
        "a request rejected before serialization is the only request class the pool may retry"
    );

    let _ = server.await.unwrap();
    let _ = client_driver.await.unwrap();
}

#[tokio::test]
async fn socketless_h2_stream_is_stopped_at_the_response_body_limit() {
    let config = ClientConfig::default().with_max_response_body_bytes(4);
    config.validate().unwrap();
    let (client_io, server_io) = duplex(64 * 1024);
    let service = service_fn(|_request: HttpRequest<Incoming>| async {
        let chunks = futures::stream::iter([
            Ok::<_, Infallible>(Frame::data(Bytes::from_static(b"1234"))),
            Ok(Frame::data(Bytes::from_static(b"5678"))),
        ]);
        Ok::<_, Infallible>(HttpResponse::new(HyperStreamBody::new(chunks)))
    });
    let server = tokio::spawn(async move {
        hyper::server::conn::http2::Builder::new(TokioExecutor::new())
            .initial_stream_window_size(16)
            .initial_connection_window_size(32)
            .serve_connection(TokioIo::new(server_io), service)
            .await
    });

    let (mut sender, connection) = http2::Builder::new(TokioExecutor::new())
        .initial_stream_window_size(16)
        .initial_connection_window_size(32)
        .handshake(TokioIo::new(client_io))
        .await
        .unwrap();
    let client_driver = tokio::spawn(connection);
    let response = sender.send_request(h2_request("/bounded")).await.unwrap();
    let error = HttpClient::collect_body(response.into_body(), config.max_response_body_bytes)
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        HttpClientError::LimitExceeded { resource, limit }
            if resource == "response body" && limit == 4
    ));

    client_driver.abort();
    server.abort();
}

#[tokio::test]
async fn per_origin_admission_is_shared_but_does_not_block_another_origin() {
    let admission = PerOriginAdmission::new(1);
    let first_origin =
        OriginKey::from_url(&Url::parse("https://api.example.test/a").unwrap()).unwrap();
    let same_origin =
        OriginKey::from_url(&Url::parse("https://api.example.test:443/b").unwrap()).unwrap();
    let other_origin =
        OriginKey::from_url(&Url::parse("https://other.example.test/a").unwrap()).unwrap();

    let first = admission.acquire(first_origin).await.unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(20),
            admission.acquire(same_origin.clone())
        )
        .await
        .is_err(),
        "the second request for one origin must wait for its permit"
    );
    let other = tokio::time::timeout(Duration::from_millis(20), admission.acquire(other_origin))
        .await
        .expect("a different origin has an independent admission budget")
        .unwrap();
    drop(other);
    drop(first);
    let _released_origin_permit = admission.acquire(same_origin).await.unwrap();
}

#[test]
fn explicit_protocol_policy_rejects_unadvertised_fallbacks() {
    let plain = Url::parse("http://example.test/").unwrap();
    let tls = Url::parse("https://example.test/").unwrap();

    assert!(HttpClient::validate_negotiated_protocol(
        ProtocolPreference::Auto,
        &plain,
        Version::HTTP_11
    )
    .is_ok());
    assert!(matches!(
        HttpClient::validate_negotiated_protocol(ProtocolPreference::Auto, &plain, Version::HTTP_2),
        Err(HttpClientError::ProtocolNegotiation { .. })
    ));
    assert!(HttpClient::validate_negotiated_protocol(
        ProtocolPreference::Auto,
        &tls,
        Version::HTTP_2
    )
    .is_ok());
    assert!(matches!(
        HttpClient::validate_negotiated_protocol(
            ProtocolPreference::Http2Only,
            &tls,
            Version::HTTP_11
        ),
        Err(HttpClientError::ProtocolNegotiation { .. })
    ));
}

#[test]
fn protocol_rejections_report_the_exact_requested_and_negotiated_versions() {
    let plain = Url::parse("http://example.test/").unwrap();
    let tls = Url::parse("https://example.test/").unwrap();
    let cases = [
        (
            ProtocolPreference::Auto,
            &tls,
            Version::HTTP_09,
            "h2 or http/1.1",
            "http/0.9",
        ),
        (
            ProtocolPreference::Auto,
            &plain,
            Version::HTTP_10,
            "http/1.1",
            "http/1.0",
        ),
        (
            ProtocolPreference::Http2Only,
            &tls,
            Version::HTTP_11,
            "h2",
            "http/1.1",
        ),
        (
            ProtocolPreference::Http1Only,
            &tls,
            Version::HTTP_2,
            "http/1.1",
            "h2",
        ),
        (
            ProtocolPreference::Auto,
            &tls,
            Version::HTTP_3,
            "h2 or http/1.1",
            "h3",
        ),
    ];

    for (preference, url, negotiated_version, requested, negotiated) in cases {
        let error = HttpClient::validate_negotiated_protocol(preference, url, negotiated_version)
            .unwrap_err();
        assert!(matches!(
            error,
            HttpClientError::ProtocolNegotiation {
                requested: actual_requested,
                negotiated: actual_negotiated,
            } if actual_requested == requested && actual_negotiated == negotiated
        ));
    }
}

#[test]
fn request_header_count_and_bytes_are_bounded_before_transport() {
    let count_config = ClientConfig::default().with_max_header_count(1);
    count_config.validate().unwrap();
    let byte_config = ClientConfig::default().with_max_header_bytes(64);
    byte_config.validate().unwrap();

    let mut counted_headers = HeaderMap::new();
    counted_headers.append("x-test", "one").unwrap();
    counted_headers.append("x-test", "two").unwrap();
    let oversized_value = "x".repeat(byte_config.max_header_bytes);
    let mut oversized_headers = HeaderMap::new();
    oversized_headers
        .append("x-test", &oversized_value)
        .unwrap();

    assert!(matches!(
        HttpClient::validate_request_header_limits(
            &counted_headers,
            count_config.max_header_count,
            count_config.max_header_bytes,
        ),
        Err(HttpClientError::LimitExceeded { resource, .. })
            if resource == "request header count"
    ));
    assert!(matches!(
        HttpClient::validate_request_header_limits(
            &oversized_headers,
            byte_config.max_header_count,
            byte_config.max_header_bytes,
        ),
        Err(HttpClientError::LimitExceeded { resource, .. })
            if resource == "request headers"
    ));

    let mut counted_response_headers = http::HeaderMap::new();
    counted_response_headers.append("x-test", "one".parse().unwrap());
    counted_response_headers.append("x-test", "two".parse().unwrap());
    let mut oversized_response_headers = http::HeaderMap::new();
    oversized_response_headers.append("x-test", oversized_value.parse().unwrap());
    assert!(matches!(
        HttpClient::convert_response_headers(
            &counted_response_headers,
            count_config.max_header_count,
            count_config.max_header_bytes,
        ),
        Err(HttpClientError::LimitExceeded { resource, .. })
            if resource == "response header count"
    ));
    assert!(matches!(
        HttpClient::convert_response_headers(
            &oversized_response_headers,
            byte_config.max_header_count,
            byte_config.max_header_bytes,
        ),
        Err(HttpClientError::LimitExceeded { resource, .. })
            if resource == "response headers"
    ));
}

#[test]
fn request_header_byte_limit_is_inclusive() {
    let mut headers = HeaderMap::new();
    headers.append("x", "y").unwrap();

    assert!(HttpClient::validate_request_header_limits(&headers, 1, 2).is_ok());
}

#[tokio::test]
async fn buffered_request_body_is_bounded_before_transport() {
    let config = ClientConfig::default().with_max_request_body_bytes(4);
    config.validate().unwrap();
    let mut oversized = BinaryBody::new(Bytes::from_static(b"12345"));
    assert!(matches!(
        HttpClient::buffer_request_body(&mut oversized, config.max_request_body_bytes).await,
        Err(HttpClientError::LimitExceeded { resource, limit })
            if resource == "request body" && limit == 4
    ));

    let mut at_limit = BinaryBody::new(Bytes::from_static(b"1234"));
    assert_eq!(
        HttpClient::buffer_request_body(&mut at_limit, config.max_request_body_bytes)
            .await
            .unwrap(),
        Bytes::from_static(b"1234")
    );

    let mut below_limit = BinaryBody::new(Bytes::from_static(b"123"));
    assert_eq!(
        HttpClient::buffer_request_body(&mut below_limit, config.max_request_body_bytes)
            .await
            .unwrap(),
        Bytes::from_static(b"123")
    );
}

#[tokio::test]
async fn response_body_limit_is_inclusive_for_size_hint_and_streamed_data() {
    let body = Full::new(Bytes::from_static(b"1234"));
    assert_eq!(
        HttpClient::collect_body(body, 4).await.unwrap(),
        Bytes::from_static(b"1234")
    );
}

#[tokio::test]
async fn buffered_request_body_rejects_both_declared_length_mismatch_directions() {
    const SECRET: &[u8] = b"LILY_SECRET_BODY_LENGTH_MISMATCH";

    for declared in [SECRET.len() - 1, SECRET.len() + 1] {
        let mut body = DeclaredLengthBody {
            declared,
            bytes: Bytes::from_static(SECRET),
        };
        let error = HttpClient::buffer_request_body(&mut body, usize::MAX)
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            HttpClientError::UnsupportedConfiguration(_)
        ));
        assert_eq!(error.diagnostic_code(), "REQUEST_BODY_LENGTH_MISMATCH");
        assert!(!format!("{error:?}").contains("LILY_SECRET_BODY_LENGTH_MISMATCH"));
        assert!(!error
            .to_string()
            .contains("LILY_SECRET_BODY_LENGTH_MISMATCH"));
    }
}
