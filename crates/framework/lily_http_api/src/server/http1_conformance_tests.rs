use std::{convert::Infallible, time::Duration};

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{body::Incoming, service::service_fn, Request, Response};
use hyper_util::rt::TokioIo;
use lily_core::enums::HttpProtocol;
use tokio::{
    io::{duplex, AsyncReadExt, AsyncWriteExt, DuplexStream},
    sync::mpsc,
    task::JoinHandle,
};

use super::server::{HttpServer, HttpTransportConfig};

#[derive(Debug, PartialEq, Eq)]
struct ObservedRequest {
    path: String,
    body: Bytes,
}

fn spawn_recording_server() -> (
    DuplexStream,
    mpsc::UnboundedReceiver<ObservedRequest>,
    JoinHandle<Result<(), String>>,
) {
    let (client, server) = duplex(64 * 1024);
    let (observed_tx, observed_rx) = mpsc::unbounded_channel();
    let service = service_fn(move |request: Request<Incoming>| {
        let observed_tx = observed_tx.clone();
        async move {
            let path = request.uri().path().to_string();
            let body = request
                .into_body()
                .collect()
                .await
                .expect("the conformance request body must be valid")
                .to_bytes();
            observed_tx
                .send(ObservedRequest { path, body })
                .expect("the observation receiver must remain alive");
            Ok::<_, Infallible>(
                Response::builder()
                    .body(Full::new(Bytes::from_static(b"ok")))
                    .expect("the static response is valid"),
            )
        }
    });
    let builder =
        HttpServer::connection_builder(&HttpTransportConfig::default(), HttpProtocol::Auto);
    let task = tokio::spawn(async move {
        builder
            .serve_connection(TokioIo::new(server), service)
            .await
            .map_err(|error| error.to_string())
    });
    (client, observed_rx, task)
}

async fn read_to_eof(client: &mut DuplexStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), client.read_to_end(&mut bytes))
        .await
        .expect("the HTTP/1 connection must close deterministically")
        .expect("the in-memory transport must remain readable");
    bytes
}

async fn read_header_block(client: &mut DuplexStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let byte = client
                .read_u8()
                .await
                .expect("the HTTP/1 connection closed before its header block");
            bytes.push(byte);
            if bytes.ends_with(b"\r\n\r\n") {
                break;
            }
        }
    })
    .await
    .expect("the HTTP/1 header block must be bounded");
    bytes
}

#[tokio::test]
async fn content_length_requests_reuse_one_keep_alive_connection() {
    let (mut client, mut observed, task) = spawn_recording_server();
    client
        .write_all(
            b"POST /length HTTP/1.1\r\nHost: test\r\nContent-Length: 5\r\n\r\nhello\
              GET /second HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();

    assert_eq!(
        observed.recv().await.unwrap(),
        ObservedRequest {
            path: "/length".to_string(),
            body: Bytes::from_static(b"hello"),
        }
    );
    assert_eq!(
        observed.recv().await.unwrap(),
        ObservedRequest {
            path: "/second".to_string(),
            body: Bytes::new(),
        }
    );

    let response = String::from_utf8(read_to_eof(&mut client).await).unwrap();
    let normalized = response.to_ascii_lowercase();
    assert_eq!(normalized.matches("http/1.1 200 ok").count(), 2);
    assert_eq!(normalized.matches("content-length: 2").count(), 2);
    assert!(normalized.contains("connection: close"));
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn transfer_encoding_with_content_length_closes_before_a_pipelined_request() {
    let (mut client, mut observed, task) = spawn_recording_server();
    client
        .write_all(
            b"POST /chunked HTTP/1.1\r\nHost: test\r\nContent-Length: 99\r\nTransfer-Encoding: chunked\r\n\r\n\
              5\r\nhello\r\n0\r\nX-Checksum: accepted\r\n\r\n\
              GET /after-chunk HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();

    assert_eq!(
        observed.recv().await.unwrap(),
        ObservedRequest {
            path: "/chunked".to_string(),
            body: Bytes::from_static(b"hello"),
        }
    );
    let response = String::from_utf8(read_to_eof(&mut client).await).unwrap();
    assert_eq!(response.matches("HTTP/1.1 200 OK").count(), 1);
    assert!(response.to_ascii_lowercase().contains("connection: close"));
    task.await.unwrap().unwrap();
    assert_eq!(observed.recv().await, None);
}

#[tokio::test]
async fn expect_continue_is_sent_before_the_service_reads_the_body() {
    let (mut client, mut observed, task) = spawn_recording_server();
    client
        .write_all(
            b"POST /expect HTTP/1.1\r\nHost: test\r\nExpect: 100-continue\r\nContent-Length: 4\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();

    let informational = read_header_block(&mut client).await;
    assert_eq!(informational, b"HTTP/1.1 100 Continue\r\n\r\n");

    client.write_all(b"body").await.unwrap();
    assert_eq!(
        observed.recv().await.unwrap(),
        ObservedRequest {
            path: "/expect".to_string(),
            body: Bytes::from_static(b"body"),
        }
    );
    let final_response = String::from_utf8(read_to_eof(&mut client).await).unwrap();
    assert!(final_response.starts_with("HTTP/1.1 200 OK\r\n"));
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn malformed_or_ambiguous_length_headers_fail_before_dispatch() {
    let cases: &[&[u8]] = &[
        b"POST / HTTP/1.1\r\nHost: test\r\nContent-Length: four\r\nConnection: close\r\n\r\n",
        b"POST / HTTP/1.1\r\nHost: test\r\nContent-Length: 4\r\nContent-Length: 5\r\nConnection: close\r\n\r\nabcde",
        b"POST / HTTP/1.1\r\nHost: test\r\nTransfer-Encoding: gzip\r\nConnection: close\r\n\r\nbody",
    ];

    for wire_request in cases {
        let (mut client, mut observed, task) = spawn_recording_server();
        client.write_all(wire_request).await.unwrap();

        let response = String::from_utf8(read_to_eof(&mut client).await).unwrap();
        assert!(
            response.starts_with("HTTP/1.1 400 Bad Request\r\n"),
            "unexpected response: {response:?}"
        );
        assert!(
            observed.try_recv().is_err(),
            "malformed input was dispatched"
        );

        // A parser rejection can finish the connection as either a clean 400
        // or a codec error after that response has been flushed. Both are a
        // closed, non-dispatched fail-closed outcome.
        let _ = task.await.unwrap();
    }
}

#[tokio::test]
async fn incomplete_header_is_closed_by_the_production_header_deadline_without_dispatch() {
    let (mut client, server) = duplex(8 * 1024);
    let (observed_tx, mut observed_rx) = mpsc::unbounded_channel();
    let service = service_fn(move |_request: Request<Incoming>| {
        let observed_tx = observed_tx.clone();
        async move {
            let _ = observed_tx.send(());
            Ok::<_, Infallible>(Response::new(Full::new(Bytes::new())))
        }
    });
    let config = HttpTransportConfig {
        header_read_timeout: Duration::from_millis(10),
        ..HttpTransportConfig::default()
    };
    let builder = HttpServer::connection_builder(&config, HttpProtocol::Http1_1);
    let task = tokio::spawn(async move {
        builder
            .serve_connection(TokioIo::new(server), service)
            .await
    });

    client
        .write_all(b"GET /slow HTTP/1.1\r\nHost: test")
        .await
        .unwrap();
    let mut terminal_bytes = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(1),
        client.read_to_end(&mut terminal_bytes),
    )
    .await
    .expect("slow header must reach a bounded terminal outcome")
    .expect("duplex read must remain valid");

    assert!(
        observed_rx.try_recv().is_err(),
        "incomplete headers must never reach application dispatch"
    );
    let _ = task.await.unwrap();
}
