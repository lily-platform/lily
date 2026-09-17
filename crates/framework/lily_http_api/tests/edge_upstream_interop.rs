//! Socketless edge-to-Lily upstream contract.
//!
//! The public TLS handshake belongs to the edge profile. These tests start at
//! the private upstream byte stream and prove that the production auto builder
//! accepts both the declared HTTP/1.1 and h2c prior-knowledge modes.

use std::convert::Infallible;
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response, Version};
use http_body_util::{BodyExt, Empty, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use lily_core::HttpProtocol;
use lily_http_api::__private::HttpServer;
use lily_http_api::HttpTransportConfig;
use tokio::io::duplex;

fn upstream_service(
    request: Request<Incoming>,
) -> impl std::future::Future<Output = Result<Response<Full<Bytes>>, Infallible>> {
    let version = match request.version() {
        Version::HTTP_11 => "http/1.1",
        Version::HTTP_2 => "h2c",
        _ => "unsupported",
    };
    async move {
        Ok(Response::builder()
            .header("x-lily-upstream-protocol", version)
            .body(Full::new(Bytes::from_static(b"healthy")))
            .expect("static fixture response"))
    }
}

#[tokio::test]
async fn private_edge_upstream_accepts_http_1_1() {
    let (edge_io, lily_io) = duplex(64 * 1024);
    let builder =
        HttpServer::connection_builder(&HttpTransportConfig::default(), HttpProtocol::Auto);
    let server = tokio::spawn(async move {
        builder
            .serve_connection(TokioIo::new(lily_io), service_fn(upstream_service))
            .await
    });

    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(edge_io))
        .await
        .expect("HTTP/1.1 upstream handshake");
    let driver = tokio::spawn(connection);
    let response = sender
        .send_request(
            Request::builder()
                .version(Version::HTTP_11)
                .uri("http://lily.internal/health")
                .header(http::header::CONNECTION, "close")
                .body(Empty::<Bytes>::new())
                .expect("valid H1 request"),
        )
        .await
        .expect("HTTP/1.1 upstream response");
    assert_eq!(response.version(), Version::HTTP_11);
    assert_eq!(response.headers()["x-lily-upstream-protocol"], "http/1.1");
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        Bytes::from_static(b"healthy")
    );

    timeout_join(driver).await;
    timeout_join(server).await;
}

#[tokio::test]
async fn private_edge_upstream_accepts_h2c_prior_knowledge() {
    let (edge_io, lily_io) = duplex(64 * 1024);
    let builder =
        HttpServer::connection_builder(&HttpTransportConfig::default(), HttpProtocol::Auto);
    let server = tokio::spawn(async move {
        builder
            .serve_connection(TokioIo::new(lily_io), service_fn(upstream_service))
            .await
    });

    let (mut sender, connection) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
        .handshake(TokioIo::new(edge_io))
        .await
        .expect("h2c prior-knowledge upstream handshake");
    let driver = tokio::spawn(connection);
    let response = sender
        .send_request(
            Request::builder()
                .version(Version::HTTP_2)
                .uri("http://lily.internal/health")
                .body(Empty::<Bytes>::new())
                .expect("valid H2 request"),
        )
        .await
        .expect("h2c upstream response");
    assert_eq!(response.version(), Version::HTTP_2);
    assert_eq!(response.headers()["x-lily-upstream-protocol"], "h2c");
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        Bytes::from_static(b"healthy")
    );

    drop(sender);
    driver.abort();
    server.abort();
    let _ = driver.await;
    let _ = server.await;
}

async fn timeout_join<T>(mut task: tokio::task::JoinHandle<T>) {
    if tokio::time::timeout(Duration::from_secs(1), &mut task)
        .await
        .is_err()
    {
        task.abort();
        let _ = task.await;
    }
}
