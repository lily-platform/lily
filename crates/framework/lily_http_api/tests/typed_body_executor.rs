use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use lily_http_api::AppBuilder;
use lily_http_api::{
    controller, BodyStream, CancellationToken, Controller, ControllerInitError, ControllerTrait,
    Extensions, Form, HttpApiError, HttpHealthService, HttpTransportConfig, Json, PlainText,
    RawBody, Request,
};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Debug, Deserialize)]
struct JsonInput {
    name: String,
    count: u32,
}

#[derive(Debug, Deserialize)]
struct FormInput {
    enabled: bool,
    #[serde(default)]
    tag: Vec<String>,
}

#[derive(Controller)]
#[base_path("/typed-body")]
struct TypedBodyController;

#[lily_http_api::async_trait::async_trait]
impl ControllerTrait for TypedBodyController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl TypedBodyController {
    #[post("/json")]
    async fn json(
        &self,
        Json(input): Json<JsonInput>,
        request: &mut Request,
    ) -> Result<PlainText, HttpApiError> {
        let retained = request
            .body_bytes()
            .ok_or_else(|| HttpApiError::InternalError("buffered body is missing".to_string()))?;
        Ok(PlainText(format!(
            "{}|{}|{}",
            input.name,
            input.count,
            retained.len()
        )))
    }

    #[post("/form")]
    async fn form(&self, Form(input): Form<FormInput>) -> Result<PlainText, HttpApiError> {
        Ok(PlainText(format!(
            "{}|{}",
            input.enabled,
            input.tag.join(",")
        )))
    }

    #[post("/raw")]
    async fn raw(&self, body: RawBody) -> Result<PlainText, HttpApiError> {
        Ok(PlainText(format!("{}", body.len())))
    }

    #[post("/stream")]
    async fn stream(&self, mut body: BodyStream) -> Result<PlainText, HttpApiError> {
        let mut collected = Vec::new();
        while let Some(chunk) = body.next_chunk().await? {
            collected.extend_from_slice(&chunk);
        }
        Ok(PlainText(format!(
            "{}|{}",
            body.bytes_read(),
            String::from_utf8_lossy(&collected)
        )))
    }
}

fn request(path: &str, headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    )
    .into_bytes();
    for (name, value) in headers {
        request.extend_from_slice(name.as_bytes());
        request.extend_from_slice(b": ");
        request.extend_from_slice(value.as_bytes());
        request.extend_from_slice(b"\r\n");
    }
    request.extend_from_slice(b"\r\n");
    request.extend_from_slice(body);
    request
}

async fn send(address: SocketAddr, request: &[u8]) -> String {
    let mut connection = tokio::net::TcpStream::connect(address)
        .await
        .expect("typed body listener accepts a connection");
    connection
        .write_all(request)
        .await
        .expect("HTTP request is written");
    let mut response = Vec::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        connection.read_to_end(&mut response),
    )
    .await
    .expect("HTTP response is bounded")
    .expect("HTTP response is readable");
    String::from_utf8(response).expect("fixture response is UTF-8")
}

fn assert_status(response: &str, status: u16) {
    assert!(
        response.starts_with(&format!("HTTP/1.1 {status}")),
        "{response}"
    );
}

#[tokio::test]
async fn generated_adapter_enforces_tae04_body_contracts_on_real_http() {
    let reservation = TcpListener::bind("127.0.0.1:0").expect("reserve a loopback address");
    let address = reservation
        .local_addr()
        .expect("reserved listener has an address");
    drop(reservation);

    let transport = HttpTransportConfig {
        max_request_body_bytes: 128,
        max_multipart_part_bytes: 128,
        ..HttpTransportConfig::default()
    };
    let app = AppBuilder::new(&address.to_string())
        .transport_config(transport)
        .build()
        .await
        .expect("typed body application builds");
    let health = app
        .extensions()
        .get_service::<HttpHealthService>(None)
        .await
        .expect("HTTP health service resolves");
    let cancellation = CancellationToken::new();
    let cancel = cancellation.clone();
    let server = tokio::spawn(async move { app.start_with_cancellation(cancellation).await });

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if health
                .snapshot()
                .expect("health snapshot remains readable")
                .ready
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("typed body listener becomes ready");

    let json_wire = br#"{"name":"lily","count":7}"#;
    let response = send(
        address,
        &request(
            "/typed-body/json",
            &[("Content-Type", "application/problem+json; charset=utf-8")],
            json_wire,
        ),
    )
    .await;
    assert_status(&response, 200);
    assert!(response.ends_with(&format!("lily|7|{}", json_wire.len())));

    let form_wire = b"enabled=true&tag=first+value&tag=%2Bsecond";
    let response = send(
        address,
        &request(
            "/typed-body/form",
            &[(
                "Content-Type",
                "application/x-www-form-urlencoded; charset=UTF-8",
            )],
            form_wire,
        ),
    )
    .await;
    assert_status(&response, 200);
    assert!(response.ends_with("true|first value,+second"));

    let response = send(address, &request("/typed-body/raw", &[], b"exact-raw-body")).await;
    assert_status(&response, 200);
    assert!(response.ends_with("14"));

    let response = send(
        address,
        &request("/typed-body/stream", &[], b"pull-stream-body"),
    )
    .await;
    assert_status(&response, 200);
    assert!(response.ends_with("16|pull-stream-body"));

    for request in [
        request(
            "/typed-body/json",
            &[("Content-Type", "text/plain")],
            json_wire,
        ),
        request(
            "/typed-body/json",
            &[
                ("Content-Type", "application/json"),
                ("Content-Type", "application/problem+json"),
            ],
            json_wire,
        ),
        request(
            "/typed-body/form",
            &[("Content-Type", "application/json")],
            b"enabled=true",
        ),
    ] {
        assert_status(&send(address, &request).await, 415);
    }

    assert_status(
        &send(
            address,
            &request(
                "/typed-body/json",
                &[("Content-Type", "application/json")],
                br#"{"name":"broken","count":}"#,
            ),
        )
        .await,
        400,
    );
    assert_status(
        &send(
            address,
            &request(
                "/typed-body/json",
                &[("Content-Type", "application/json")],
                b"",
            ),
        )
        .await,
        400,
    );
    assert_status(
        &send(address, &request("/typed-body/raw", &[], &[b'x'; 129])).await,
        413,
    );

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("typed body server shutdown is bounded")
        .expect("typed body server task does not panic")
        .expect("typed body server shuts down cleanly");
}
