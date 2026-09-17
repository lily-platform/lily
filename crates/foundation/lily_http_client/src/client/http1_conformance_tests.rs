use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use bytes::Bytes;
use http::StatusCode as HttpStatusCode;
use http_body_util::Full;
use hyper::client::conn::http1;
use hyper_util::rt::TokioIo;
use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt, DuplexStream};
use url::Url;

use super::{
    ClientConfig, HttpClient, ProtocolPreference, TransportBuildSettings,
    HYPER_HTTP1_DEFAULT_MAX_HEADERS,
};
use crate::{header::HeaderMap, request::Method};

async fn read_header_block(peer: &mut DuplexStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let byte = peer
                .read_u8()
                .await
                .expect("the client closed before completing its request headers");
            bytes.push(byte);
            if bytes.ends_with(b"\r\n\r\n") {
                break;
            }
        }
    })
    .await
    .expect("request headers must be emitted within the test deadline");
    bytes
}

async fn client_connection() -> (
    http1::SendRequest<Full<Bytes>>,
    DuplexStream,
    tokio::task::JoinHandle<Result<(), String>>,
) {
    let (client, peer) = duplex(64 * 1024);
    let (sender, connection) = http1::Builder::new()
        .handshake(TokioIo::new(client))
        .await
        .expect("the in-memory client handshake must succeed");
    let driver = tokio::spawn(async move { connection.await.map_err(|error| error.to_string()) });
    (sender, peer, driver)
}

#[tokio::test]
async fn full_request_uses_one_content_length_and_decodes_1xx_plus_chunked_response() {
    let (mut sender, mut peer, driver) = client_connection().await;
    let wire_peer = tokio::spawn(async move {
        let mut request = read_header_block(&mut peer).await;
        let mut body = [0_u8; 4];
        peer.read_exact(&mut body).await.unwrap();
        request.extend_from_slice(&body);
        peer.write_all(
            b"HTTP/1.1 103 Early Hints\r\nLink: </style.css>; rel=preload\r\n\r\n\
              HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n\
              3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n",
        )
        .await
        .unwrap();
        peer.shutdown().await.unwrap();
        request
    });

    let mut user_headers = HeaderMap::new();
    user_headers.insert("Content-Length", "999").unwrap();
    user_headers.insert("Transfer-Encoding", "chunked").unwrap();
    user_headers.insert("Connection", "keep-alive").unwrap();
    let mut request = HttpClient::build_request(
        &Method::Post,
        &Url::parse("http://example.test/upload").unwrap(),
        &user_headers,
        Bytes::from_static(b"body"),
    )
    .unwrap();
    let informational = Arc::new(Mutex::new(Vec::new()));
    let observed_informational = Arc::clone(&informational);
    hyper::ext::on_informational(&mut request, move |response| {
        let observed_informational = Arc::clone(&observed_informational);
        observed_informational
            .lock()
            .expect("the informational observation lock must not be poisoned")
            .push(response.status());
    });

    let response = sender.send_request(request).await.unwrap();
    assert_eq!(response.status(), HttpStatusCode::OK);
    let body = HttpClient::collect_body(response.into_body(), 1024)
        .await
        .unwrap();
    assert_eq!(body, Bytes::from_static(b"abcde"));
    assert_eq!(
        informational
            .lock()
            .expect("the informational observation lock must not be poisoned")
            .as_slice(),
        &[HttpStatusCode::EARLY_HINTS]
    );

    let wire_request = String::from_utf8(wire_peer.await.unwrap()).unwrap();
    let normalized = wire_request.to_ascii_lowercase();
    assert_eq!(normalized.matches("content-length: 4").count(), 1);
    assert!(!normalized.contains("content-length: 999"));
    assert!(!normalized.contains("transfer-encoding:"));
    assert!(!normalized.contains("connection:"));
    assert!(wire_request.ends_with("\r\n\r\nbody"));
    driver.await.unwrap().unwrap();
}

#[tokio::test]
async fn close_delimited_response_is_read_to_eof() {
    let (mut sender, mut peer, driver) = client_connection().await;
    let wire_peer = tokio::spawn(async move {
        let request = read_header_block(&mut peer).await;
        peer.write_all(b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nclose-body")
            .await
            .unwrap();
        peer.shutdown().await.unwrap();
        request
    });

    let request = HttpClient::build_request(
        &Method::Get,
        &Url::parse("http://example.test/close").unwrap(),
        &HeaderMap::new(),
        Bytes::new(),
    )
    .unwrap();
    let response = sender.send_request(request).await.unwrap();
    let body = HttpClient::collect_body(response.into_body(), 1024)
        .await
        .unwrap();

    assert_eq!(body, Bytes::from_static(b"close-body"));
    assert!(!wire_peer.await.unwrap().is_empty());
    driver.await.unwrap().unwrap();
}

#[tokio::test]
async fn conflicting_response_content_lengths_are_rejected() {
    let (mut sender, mut peer, driver) = client_connection().await;
    let wire_peer = tokio::spawn(async move {
        let _ = read_header_block(&mut peer).await;
        peer.write_all(
            b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nContent-Length: 5\r\nConnection: close\r\n\r\nabcde",
        )
        .await
        .unwrap();
        peer.shutdown().await.unwrap();
    });

    let request = HttpClient::build_request(
        &Method::Get,
        &Url::parse("http://example.test/ambiguous").unwrap(),
        &HeaderMap::new(),
        Bytes::new(),
    )
    .unwrap();
    let error = sender.send_request(request).await.unwrap_err();
    assert!(
        error.to_string().contains("invalid content-length"),
        "unexpected codec error: {error}"
    );

    wire_peer.await.unwrap();
    // Hyper reports the parse failure through `send_request`; after delivering
    // it, the connection driver may itself complete cleanly.
    let _ = driver.await.unwrap();
}

#[tokio::test]
async fn configured_header_buffer_accepts_a_large_h1_head_within_the_lily_limit() {
    const HEADER_VALUE_BYTES: usize = 450 * 1024;

    let config = ClientConfig::default().with_max_header_bytes(512 * 1024);
    config.validate().unwrap();
    let settings = TransportBuildSettings::from_config(
        &config,
        Duration::from_secs(1),
        ProtocolPreference::Http1Only,
    );

    let (client, mut peer) = duplex(2 * 1024 * 1024);
    let mut builder = http1::Builder::new();
    builder.max_buf_size(settings.http1_max_buffer_bytes);
    if settings.http1_max_headers != HYPER_HTTP1_DEFAULT_MAX_HEADERS {
        builder.max_headers(settings.http1_max_headers);
    }
    let (mut sender, connection) = builder
        .handshake(TokioIo::new(client))
        .await
        .expect("the bounded in-memory client handshake must succeed");
    let driver = tokio::spawn(async move { connection.await.map_err(|error| error.to_string()) });

    let wire_peer = tokio::spawn(async move {
        let _ = read_header_block(&mut peer).await;
        let mut response = Vec::with_capacity(HEADER_VALUE_BYTES + 128);
        response.extend_from_slice(b"HTTP/1.1 200 OK\r\nX-Large: ");
        response.resize(response.len() + HEADER_VALUE_BYTES, b'a');
        response.extend_from_slice(b"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        peer.write_all(&response).await.unwrap();
        peer.shutdown().await.unwrap();
    });

    let request = HttpClient::build_request(
        &Method::Get,
        &Url::parse("http://example.test/large-head").unwrap(),
        &HeaderMap::new(),
        Bytes::new(),
    )
    .unwrap();
    let response = sender.send_request(request).await.unwrap();
    let headers = HttpClient::convert_response_headers(
        response.headers(),
        config.max_header_count,
        config.max_header_bytes,
    )
    .unwrap();
    assert_eq!(headers.get("X-Large").unwrap().len(), HEADER_VALUE_BYTES);

    wire_peer.await.unwrap();
    let _ = driver.await.unwrap();
}
