use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;
use std::time::Duration;

use lily_http_api::AppBuilder;
use lily_http_api::{
    controller, Accepted, CancellationToken, Controller, ControllerInitError, ControllerTrait,
    Created, Extensions, HttpApiError, HttpHealthService, IntoResponse, NoContent,
    PassthroughResponseContext, PassthroughResponseError, Query, Request, Response,
    ResponseBuilder, ResponseCookie, ResponseWriteError, ResponseWriteOutcome, Service,
    ServiceTrait,
};
use serde::{Deserialize, Serialize, Serializer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Deserialize, Serialize)]
struct CreatedResource {
    id: u64,
}

// A single application converter works for JSON, manual and passthrough actions.
// Cell intentionally makes this error Send but not Sync.
enum CustomError {
    Missing,
    SuccessStatus(std::cell::Cell<u16>),
    BrokenBody,
    Preserved,
}

#[lily_http_api::async_trait::async_trait]
impl IntoResponse for CustomError {
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        let status = match self {
            Self::Missing => 404,
            Self::SuccessStatus(status) => status.get(),
            Self::BrokenBody => {
                return ResponseBuilder::new()
                    .json(FailingSerialize)
                    .write_to_response(response, request)
                    .await
            }
            Self::Preserved => return Ok(ResponseWriteOutcome::Preserved),
        };
        ResponseBuilder::new()
            .status(status, if status == 200 { "OK" } else { "Not Found" })
            .header("X-Custom-Error", "written")
            .json(serde_json::json!({"detail": "resource missing"}))
            .write_to_response(response, request)
            .await
    }
}

struct UnregisteredService;
impl ServiceTrait for UnregisteredService {}

static EXTRACTOR_ACTION_CALLS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

struct FailingSerialize;

impl Serialize for FailingSerialize {
    fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        Err(serde::ser::Error::custom("private serialization detail"))
    }
}

#[derive(Controller)]
#[base_path("/typed-response")]
struct TypedResponseController;

#[lily_http_api::async_trait::async_trait]
impl ControllerTrait for TypedResponseController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl TypedResponseController {
    #[post("/created")]
    async fn created(&self) -> Result<Created<CreatedResource>, CustomError> {
        Ok(Created::new("/resources/42", CreatedResource { id: 42 }))
    }

    #[post("/accepted")]
    async fn accepted(&self) -> Accepted<CreatedResource> {
        Accepted::new("/jobs/42", CreatedResource { id: 42 })
    }

    #[post("/created-empty")]
    async fn created_empty(&self) -> Created {
        Created::empty().with_location("/resources/42")
    }

    #[post("/accepted-empty")]
    async fn accepted_empty(&self) -> Result<Accepted, CustomError> {
        Ok(Accepted::empty())
    }

    #[post("/created-error")]
    async fn created_error(&self) -> Result<Created<CreatedResource>, CustomError> {
        Err(CustomError::Missing)
    }

    #[post("/accepted-error")]
    async fn accepted_error(&self) -> Result<Accepted, CustomError> {
        Err(CustomError::Missing)
    }

    #[post("/created-invalid-location")]
    async fn created_invalid_location(&self) -> Created<CreatedResource> {
        Created::new("/private\r\nX-Injected: yes", CreatedResource { id: 42 })
    }

    #[post("/accepted-serialization-error")]
    async fn accepted_serialization_error(
        &self,
    ) -> Result<Accepted<FailingSerialize>, CustomError> {
        Ok(Accepted::new("/jobs/42", FailingSerialize))
    }

    #[post("/accepted-status-override")]
    async fn accepted_status_override(
        &self,
        mut response: PassthroughResponseContext<'_>,
    ) -> Accepted {
        response.status(200).expect("valid staged status");
        Accepted::empty().with_location("/jobs/42")
    }

    #[get("/custom-json")]
    async fn custom_json(&self) -> Result<CreatedResource, CustomError> {
        Ok(CreatedResource { id: 7 })
    }

    #[get("/custom-error")]
    async fn custom_error(&self) -> Result<CreatedResource, CustomError> {
        Err(CustomError::Missing)
    }

    #[get("/custom-broken")]
    async fn custom_broken(&self) -> Result<CreatedResource, CustomError> {
        Err(CustomError::BrokenBody)
    }

    #[get("/custom-preserved")]
    async fn custom_preserved(&self) -> Result<CreatedResource, CustomError> {
        Err(CustomError::Preserved)
    }

    #[get("/custom-manual-error")]
    async fn custom_manual_error(&self, response: &mut Response) -> Result<(), CustomError> {
        response.try_insert_header("X-Partial", "discard").unwrap();
        response.write_body(b"partial").unwrap();
        Err(CustomError::Missing)
    }

    #[get("/custom-manual-success")]
    async fn custom_manual_success(&self, response: &mut Response) -> Result<(), CustomError> {
        response.status(201, "Created");
        response.write_body(b"manual custom").unwrap();
        Ok(())
    }

    #[get("/custom-passthrough")]
    async fn custom_passthrough(
        &self,
        mut response: PassthroughResponseContext<'_>,
    ) -> Result<CreatedResource, CustomError> {
        response.status(201).unwrap();
        response.insert_header("X-Staged", "discard").unwrap();
        response
            .set_cookie(&ResponseCookie::new("session", "discard").unwrap())
            .unwrap();
        Err(CustomError::SuccessStatus(std::cell::Cell::new(200)))
    }

    #[get("/custom-service")]
    async fn custom_service(
        &self,
        _service: Service<UnregisteredService>,
    ) -> Result<CreatedResource, CustomError> {
        EXTRACTOR_ACTION_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(CreatedResource { id: 7 })
    }

    #[get("/custom-query")]
    async fn custom_query(
        &self,
        Query(value): Query<CreatedResource>,
    ) -> Result<CreatedResource, CustomError> {
        EXTRACTOR_ACTION_CALLS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(value)
    }

    #[get("/unit")]
    async fn unit(&self) -> Result<(), HttpApiError> {
        Ok(())
    }

    #[delete("/no-content")]
    async fn no_content(&self) -> NoContent {
        NoContent
    }

    #[delete("/no-content-result")]
    async fn no_content_result(&self) -> Result<NoContent, HttpApiError> {
        Ok(NoContent)
    }

    #[post("/manual")]
    async fn manual(&self, response: &mut Response) -> Result<(), HttpApiError> {
        response.status(201, "Created");
        response
            .try_insert_header("X-Manual", "preserved")
            .map_err(|_| {
                HttpApiError::ResponseEncodingError("manual response header failed".to_string())
            })?;
        response
            .write_body(b"manual-body")
            .map_err(HttpApiError::from)?;
        Ok(())
    }

    #[post("/passthrough-created")]
    async fn passthrough_created(
        &self,
        mut response: PassthroughResponseContext<'_>,
    ) -> Result<CreatedResource, HttpApiError> {
        let cookie = ResponseCookie::new("refresh", "opaque")
            .map_err(PassthroughResponseError::from)?
            .http_only(true)
            .secure(true);
        response.set_cookie(&cookie)?;
        response.insert_header("X-Resource", "created")?;
        response.status(201)?;
        Ok(CreatedResource { id: 42 })
    }

    #[post("/passthrough-error")]
    async fn passthrough_error(
        &self,
        mut response: PassthroughResponseContext<'_>,
    ) -> Result<CreatedResource, HttpApiError> {
        response.insert_header("X-Staged", "must-not-commit")?;
        Err(HttpApiError::BadRequest(
            "private action detail".to_string(),
        ))
    }

    #[post("/passthrough-serialization-error")]
    async fn passthrough_serialization_error(
        &self,
        mut response: PassthroughResponseContext<'_>,
    ) -> Result<FailingSerialize, HttpApiError> {
        response.insert_header("X-Staged", "must-not-commit")?;
        Ok(FailingSerialize)
    }

    #[delete("/passthrough-no-content")]
    async fn passthrough_no_content(
        &self,
        mut response: PassthroughResponseContext<'_>,
    ) -> Result<NoContent, HttpApiError> {
        response.insert_header("X-Deleted", "true")?;
        Ok(NoContent)
    }

    #[delete("/passthrough-no-content-status")]
    async fn passthrough_no_content_status(
        &self,
        mut response: PassthroughResponseContext<'_>,
    ) -> Result<NoContent, HttpApiError> {
        response.insert_header("X-Staged", "must-not-commit")?;
        response.status(201)?;
        Ok(NoContent)
    }
}

fn request(method: &str, path: &str) -> Vec<u8> {
    format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
    )
    .into_bytes()
}

async fn send(address: SocketAddr, request: &[u8]) -> String {
    let mut connection = tokio::net::TcpStream::connect(address)
        .await
        .expect("typed response listener accepts a connection");
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

fn response_parts(response: &str) -> (&str, &str) {
    response
        .split_once("\r\n\r\n")
        .expect("HTTP response contains a header terminator")
}

#[tokio::test]
async fn generated_adapter_keeps_unit_no_content_and_manual_response_modes_distinct() {
    let reservation = TcpListener::bind("127.0.0.1:0").expect("reserve a loopback address");
    let address = reservation
        .local_addr()
        .expect("reserved listener has an address");
    drop(reservation);

    let app = AppBuilder::new(&address.to_string())
        .build()
        .await
        .expect("typed response application builds");
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
    .expect("typed response listener becomes ready");

    for (path, status, location, expected_body) in [
        ("created", 201, Some("/resources/42"), Some(r#"{"id":42}"#)),
        ("accepted", 202, Some("/jobs/42"), Some(r#"{"id":42}"#)),
        ("created-empty", 201, Some("/resources/42"), None),
        ("accepted-empty", 202, None, None),
    ] {
        let response = send(
            address,
            &request("POST", &format!("/typed-response/{path}")),
        )
        .await;
        assert_status(&response, status);
        let (headers, body) = response_parts(&response);
        let headers = headers.to_ascii_lowercase();
        match location {
            Some(location) => assert!(headers.contains(&format!("location: {location}\r\n"))),
            None => assert!(!headers.contains("location:")),
        }
        assert_eq!(
            headers.contains("content-type: application/json"),
            expected_body.is_some()
        );
        assert_eq!(body, expected_body.unwrap_or_default());
    }

    for path in ["created-error", "accepted-error"] {
        let response = send(
            address,
            &request("POST", &format!("/typed-response/{path}")),
        )
        .await;
        assert_status(&response, 404);
        let (headers, body) = response_parts(&response);
        assert!(!headers.to_ascii_lowercase().contains("location:"));
        assert_eq!(body, r#"{"detail":"resource missing"}"#);
    }

    for path in [
        "created-invalid-location",
        "accepted-serialization-error",
        "accepted-status-override",
    ] {
        let response = send(
            address,
            &request("POST", &format!("/typed-response/{path}")),
        )
        .await;
        assert_status(&response, 500);
        let (headers, body) = response_parts(&response);
        assert!(!headers.to_ascii_lowercase().contains("location:"));
        assert!(!headers.to_ascii_lowercase().contains("x-injected:"));
        assert!(body.contains("RESPONSE_ENCODING_ERROR"));
        assert!(!body.contains("private"));
    }

    let response = send(address, &request("GET", "/typed-response/unit")).await;
    assert_status(&response, 200);
    let (headers, body) = response_parts(&response);
    assert!(!headers.to_ascii_lowercase().contains("content-type:"));
    assert!(body.is_empty());

    for path in [
        "/typed-response/no-content",
        "/typed-response/no-content-result",
    ] {
        let response = send(address, &request("DELETE", path)).await;
        assert_status(&response, 204);
        let (headers, body) = response_parts(&response);
        assert!(!headers.to_ascii_lowercase().contains("content-type:"));
        assert!(body.is_empty());
    }

    let response = send(address, &request("POST", "/typed-response/manual")).await;
    assert_status(&response, 201);
    let (headers, body) = response_parts(&response);
    assert!(headers.to_ascii_lowercase().contains("x-manual: preserved"));
    assert_eq!(body, "manual-body");

    let response = send(
        address,
        &request("POST", "/typed-response/passthrough-created"),
    )
    .await;
    assert_status(&response, 201);
    let (headers, body) = response_parts(&response);
    let headers = headers.to_ascii_lowercase();
    assert!(headers.contains("content-type: application/json"));
    assert!(headers.contains("x-resource: created"));
    assert!(headers.contains("set-cookie: refresh=opaque"));
    assert!(headers.contains("httponly"));
    assert!(headers.contains("secure"));
    assert_eq!(body, r#"{"id":42}"#);

    let response = send(
        address,
        &request("POST", "/typed-response/passthrough-error"),
    )
    .await;
    assert_status(&response, 400);
    let (headers, _) = response_parts(&response);
    assert!(!headers.to_ascii_lowercase().contains("x-staged:"));

    let response = send(
        address,
        &request("POST", "/typed-response/passthrough-serialization-error"),
    )
    .await;
    assert_status(&response, 500);
    let (headers, body) = response_parts(&response);
    assert!(!headers.to_ascii_lowercase().contains("x-staged:"));
    assert!(!body.contains("private serialization detail"));

    let response = send(
        address,
        &request("DELETE", "/typed-response/passthrough-no-content"),
    )
    .await;
    assert_status(&response, 204);
    let (headers, body) = response_parts(&response);
    assert!(headers.to_ascii_lowercase().contains("x-deleted: true"));
    assert!(body.is_empty());

    let response = send(
        address,
        &request("DELETE", "/typed-response/passthrough-no-content-status"),
    )
    .await;
    assert_status(&response, 500);
    let (headers, _) = response_parts(&response);
    assert!(!headers.to_ascii_lowercase().contains("x-staged:"));

    let response = send(address, &request("GET", "/typed-response/custom-json")).await;
    assert_status(&response, 200);
    assert_eq!(response_parts(&response).1, r#"{"id":7}"#);

    for path in ["custom-error", "custom-manual-error"] {
        let response = send(address, &request("GET", &format!("/typed-response/{path}"))).await;
        assert_status(&response, 404);
        let (headers, body) = response_parts(&response);
        assert!(headers
            .to_ascii_lowercase()
            .contains("x-custom-error: written"));
        assert!(!headers.to_ascii_lowercase().contains("x-partial:"));
        assert_eq!(body, r#"{"detail":"resource missing"}"#);
    }

    for path in ["custom-broken", "custom-preserved"] {
        let response = send(address, &request("GET", &format!("/typed-response/{path}"))).await;
        assert_status(&response, 500);
        let (headers, body) = response_parts(&response);
        assert!(!headers.to_ascii_lowercase().contains("x-custom-error:"));
        assert!(body.contains("RESPONSE_ENCODING_ERROR"));
        assert!(!body.contains("private serialization detail"));
    }

    let response = send(
        address,
        &request("GET", "/typed-response/custom-manual-success"),
    )
    .await;
    assert_status(&response, 201);
    assert_eq!(response_parts(&response).1, "manual custom");

    let response = send(
        address,
        &request("GET", "/typed-response/custom-passthrough"),
    )
    .await;
    assert_status(&response, 200);
    let (headers, body) = response_parts(&response);
    assert!(!headers.to_ascii_lowercase().contains("x-staged:"));
    assert!(!headers.to_ascii_lowercase().contains("set-cookie:"));
    assert_eq!(body, r#"{"detail":"resource missing"}"#);

    for (path, status) in [("custom-service", 500), ("custom-query", 400)] {
        let response = send(address, &request("GET", &format!("/typed-response/{path}"))).await;
        assert_status(&response, status);
        assert!(!response_parts(&response)
            .0
            .to_ascii_lowercase()
            .contains("x-custom-error:"));
    }
    assert_eq!(
        EXTRACTOR_ACTION_CALLS.load(std::sync::atomic::Ordering::SeqCst),
        0
    );

    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("typed response server shutdown is bounded")
        .expect("typed response server task does not panic")
        .expect("typed response server shuts down cleanly");
}
