//! Real HTTP requests against the SDK span exporter. Attribute lists stay raw:
//! collecting them into a map would hide the duplicate-field regression.
use std::{
    collections::{BTreeMap, HashSet},
    sync::Arc,
    time::Duration,
};

use lily_http_api::{
    async_trait::async_trait, controller, AppBuilder, CancellationToken, Controller,
    ControllerInitError, ControllerTrait, CorsOriginContext, CorsOriginResolver,
    CorsOriginResolverError, CorsOriginResolverInitError, CorsPolicy, ExecutionCancellation,
    Extensions, GuardInitError, GuardRejection, GuardTrait, HttpErrorCode, HttpExchange,
    HttpMiddleware, HttpMiddlewareError, HttpMiddlewareInitError, HttpMiddlewareRejection,
    HttpNext, IntoResponse, MiddlewareDescriptor, MiddlewareErrorCode, MiddlewareKind,
    PassthroughResponseContext, Request, Response, ResponseBuilder, ResponseFailureKind,
    ResponseWriteError, ResponseWriteOutcome,
};
use opentelemetry::{
    global,
    trace::{SpanId, Status, TraceId, TracerProvider as _},
    KeyValue, Value,
};
use opentelemetry_sdk::{
    metrics::{
        data::{AggregatedMetrics, MetricData},
        InMemoryMetricExporter, PeriodicReader, SdkMeterProvider,
    },
    propagation::TraceContextPropagator,
    trace::{InMemorySpanExporter, SdkTracerProvider, SpanData},
};
use serde::{Serialize, Serializer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing_subscriber::prelude::*;

static RELEASE_STREAM_FAILURE: tokio::sync::Notify = tokio::sync::Notify::const_new();
static STATIC_FILES: std::sync::OnceLock<lily_http_api::StaticFileMount> =
    std::sync::OnceLock::new();

struct StaticFixtureResponse;

#[async_trait]
impl IntoResponse for StaticFixtureResponse {
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        STATIC_FILES
            .get()
            .unwrap()
            .serve(request)
            .await
            .unwrap()
            .write_to_response(response, request)
            .await
    }
}

struct AppError(u16, Option<ResponseFailureKind>);
impl AppError {
    fn legacy(status: u16) -> Self {
        Self(status, None)
    }
    fn rejected() -> Self {
        Self(409, Some(ResponseFailureKind::Rejected))
    }
}

#[async_trait]
impl IntoResponse for AppError {
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        ResponseBuilder::new()
            .status(self.0, "Fixture")
            .json(serde_json::json!({"failure": true}))
            .write_to_response(response, request)
            .await?;
        let code = Some(HttpErrorCode::new("APP_FAILURE").unwrap());
        Ok(match self.1 {
            Some(kind) => ResponseWriteOutcome::classified_error(kind, code),
            None => ResponseWriteOutcome::error(code),
        })
    }
}

struct Broken;
impl Serialize for Broken {
    fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("private-serialization-canary"))
    }
}

struct Admission;
#[async_trait]
impl GuardTrait for Admission {
    async fn new(_: Arc<Extensions>) -> Result<Self, GuardInitError> {
        Ok(Self)
    }
    async fn can_activate(
        &self,
        request: &mut Request,
        _: ExecutionCancellation,
    ) -> Result<(), GuardRejection> {
        let status = if request.header_value("x-technical").is_some() {
            503
        } else {
            403
        };
        Err(GuardRejection::new(status, HttpErrorCode::new("GUARD_FAILURE").unwrap()).unwrap())
    }
}

struct Recovery;
#[async_trait]
impl HttpMiddleware for Recovery {
    async fn new(_: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
        Ok(Self)
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("telemetry-recovery", MiddlewareKind::Custom)
    }
    async fn handle(
        &self,
        exchange: &mut HttpExchange<'_>,
        next: HttpNext<'_>,
        _: ExecutionCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        let recover = exchange.request().header_value("x-recover").is_some();
        next.run(exchange).await?;
        if recover {
            exchange.response_mut().status(200, "Recovered");
        }
        Ok(())
    }
}

struct Origins;
#[async_trait]
impl CorsOriginResolver for Origins {
    async fn new(_: Arc<Extensions>) -> Result<Self, CorsOriginResolverInitError> {
        Ok(Self)
    }
    async fn allows(
        &self,
        context: &CorsOriginContext<'_>,
    ) -> Result<bool, CorsOriginResolverError> {
        if context.origin() == "https://unavailable.test" {
            return Err(CorsOriginResolverError::unavailable(
                MiddlewareErrorCode::new("CORS_UNAVAILABLE").unwrap(),
            ));
        }
        Ok(context.origin() == "https://telemetry.test")
    }
}

struct Gate;
#[async_trait]
impl HttpMiddleware for Gate {
    async fn new(_: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
        Ok(Self)
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("telemetry-gate", MiddlewareKind::Custom)
    }
    async fn handle(
        &self,
        exchange: &mut HttpExchange<'_>,
        next: HttpNext<'_>,
        _: ExecutionCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        if let Some(status) = exchange.request().header_value("x-middleware-status") {
            return Err(HttpMiddlewareRejection::new(
                status.parse().unwrap(),
                MiddlewareErrorCode::new("MIDDLEWARE_FAILURE").unwrap(),
            )
            .unwrap()
            .into());
        }
        next.run(exchange).await
    }
}

#[derive(Controller)]
#[base_path("/telemetry")]
struct Endpoints;
#[async_trait]
impl ControllerTrait for Endpoints {
    async fn new(_: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}
#[controller]
impl Endpoints {
    #[get("/ok")]
    async fn ok(&self) -> &'static str {
        "ok"
    }
    #[get("/rejected")]
    async fn rejected(&self) -> Result<(), AppError> {
        Err(AppError::legacy(409))
    }
    #[get("/error")]
    async fn error(&self) -> Result<(), AppError> {
        Err(AppError::legacy(503))
    }
    #[get("/success-status")]
    async fn success_status(&self) -> Result<(), AppError> {
        Err(AppError::legacy(200))
    }
    #[get("/broken")]
    async fn broken(&self) -> lily_http_api::Json<Broken> {
        lily_http_api::Json(Broken)
    }
    #[get("/stream-error")]
    async fn stream_error(&self) -> lily_http_api::StreamingResponse {
        use futures::StreamExt;
        lily_http_api::streaming(
            futures::stream::iter([Ok::<_, std::io::Error>(lily_http_api::Bytes::from_static(
                b"first",
            ))])
            .chain(futures::stream::once(async {
                // Fail only after the client has received the committed
                // headers and first chunk; no timing-dependent sleep.
                RELEASE_STREAM_FAILURE.notified().await;
                Err(std::io::Error::other("private-stream-canary"))
            })),
        )
    }
    #[get("/stream-exact")]
    async fn stream_exact(&self) -> lily_http_api::StreamingResponse {
        use futures::StreamExt;
        lily_http_api::streaming(
            futures::stream::iter([
                Ok::<_, std::io::Error>(lily_http_api::Bytes::from_static(b"first")),
                Ok(lily_http_api::Bytes::from_static(b"last")),
            ])
            // An exact-length response must not require a final source poll.
            .chain(futures::stream::pending()),
        )
        .content_length(9)
    }
    #[get("/stream-empty")]
    async fn stream_empty(&self) -> lily_http_api::StreamingResponse {
        lily_http_api::streaming(futures::stream::pending::<
            Result<lily_http_api::Bytes, std::io::Error>,
        >())
        .content_length(0)
    }
    #[get("/static-file")]
    async fn static_file(&self) -> StaticFixtureResponse {
        StaticFixtureResponse
    }
    #[get("/static-empty")]
    async fn static_empty(&self) -> StaticFixtureResponse {
        StaticFixtureResponse
    }
    #[get("/classified")]
    async fn classified(&self) -> Result<(), AppError> {
        Err(AppError::rejected())
    }
    #[get("/explicit-error-200")]
    async fn explicit_error(&self) -> Result<(), AppError> {
        Err(AppError(200, Some(ResponseFailureKind::Error)))
    }
    #[get("/direct")]
    async fn direct(&self) -> AppError {
        AppError::rejected()
    }
    #[get("/manual")]
    async fn manual(&self, response: &mut Response) -> Result<(), AppError> {
        response.try_insert_header("X-Partial", "discard").unwrap();
        response.write_body(b"partial-canary").unwrap();
        Err(AppError::rejected())
    }
    #[get("/passthrough")]
    async fn passthrough(
        &self,
        mut response: PassthroughResponseContext<'_>,
    ) -> Result<serde_json::Value, AppError> {
        response.status(201).unwrap();
        response.insert_header("X-Partial", "discard").unwrap();
        Err(AppError::rejected())
    }
    #[get("/created-error")]
    async fn created_error(&self) -> Result<lily_http_api::Created<serde_json::Value>, AppError> {
        Err(AppError::rejected())
    }
    #[get("/accepted-error")]
    async fn accepted_error(&self) -> Result<lily_http_api::Accepted, AppError> {
        Err(AppError::rejected())
    }
    #[get("/raw-status")]
    async fn raw_status(&self) -> ResponseBuilder {
        ResponseBuilder::new()
            .status(409, "Conflict")
            .text("raw refusal")
    }
    #[get("/header-error")]
    async fn header_error(&self) -> ResponseBuilder {
        ResponseBuilder::new().header("X-Invalid", "private-header-canary\r\n")
    }
    #[head("/head")]
    async fn head(&self) -> &'static str {
        "head payload"
    }
    #[get("/empty")]
    async fn empty(&self) -> lily_http_api::NoContent {
        lily_http_api::NoContent
    }
    #[get("/guard")]
    #[guard(Admission)]
    async fn guarded(&self) -> &'static str {
        panic!("rejected guard must not execute handler")
    }
}

#[derive(Clone, Copy, Debug)]
struct Case {
    path: &'static str,
    method: &'static str,
    request_body: &'static str,
    headers: &'static str,
    status: u16,
    outcome: &'static str,
    application_error: bool,
    application_outcome: Option<&'static str>,
    handler: Option<&'static str>,
    error_code: &'static str,
}

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(15), future)
        .await
        .expect("bounded HTTP test")
}

fn one<'a>(attributes: &'a [KeyValue], key: &str) -> &'a Value {
    let found: Vec<_> = attributes
        .iter()
        .filter(|item| item.key.as_str() == key)
        .collect();
    assert_eq!(found.len(), 1, "{key} must be present once: {attributes:?}");
    &found[0].value
}

fn unique(span: &SpanData) {
    let keys: HashSet<_> = span
        .attributes
        .iter()
        .map(|pair| pair.key.as_str())
        .collect();
    assert_eq!(
        keys.len(),
        span.attributes.len(),
        "{}: {:?}",
        span.name,
        span.attributes
    );
    assert_eq!(span.dropped_attributes_count, 0);
    assert_eq!(span.events.dropped_count, 0);
    for event in span.events.iter() {
        for field in &event.attributes {
            if field.key.as_str().ends_with("duration_ms")
                || field.key.as_str().ends_with("duration_us")
            {
                let Value::F64(value) = field.value else {
                    panic!("duration must be numeric: {field:?}")
                };
                assert!(value.is_finite() && value >= 0.0);
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_outcomes_have_consistent_status_severity_types_and_terminal_ownership() {
    let exports = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exports.clone())
        .build();
    tracing::subscriber::set_global_default(
        tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("http-regression"))),
    )
    .unwrap();
    global::set_text_map_propagator(TraceContextPropagator::new());
    let metrics = InMemoryMetricExporter::default();
    let meters = SdkMeterProvider::builder()
        .with_reader(
            PeriodicReader::builder(metrics.clone())
                .with_interval(Duration::from_secs(3600))
                .build(),
        )
        .build();
    global::set_meter_provider(meters.clone());
    let files = tempfile::tempdir().unwrap();
    std::fs::write(files.path().join("static-file"), b"static-file-bytes").unwrap();
    std::fs::write(files.path().join("static-empty"), b"").unwrap();
    STATIC_FILES
        .set(
            lily_http_api::StaticFileMount::new(files.path(), "/telemetry")
                .await
                .unwrap(),
        )
        .unwrap();
    let app = AppBuilder::new("127.0.0.1:0")
        .tls_disabled()
        .tracing_external()
        .middleware::<Recovery>()
        .middleware::<Gate>()
        .cors(
            CorsPolicy::new()
                .resolve_origins_with::<Origins>()
                .allow_methods(["GET", "HEAD"]),
        )
        .build()
        .await
        .unwrap();
    let running = app.clone();
    let server = tokio::spawn(async move {
        running
            .start_with_cancellation(CancellationToken::new())
            .await
    });
    bounded(async {
        while app.bound_address().is_none() {
            assert!(!server.is_finished(), "server failed before readiness");
            tokio::task::yield_now().await;
        }
    })
    .await;
    let address = app.bound_address().unwrap();
    // Explicit expectations are independent of the production classifier.
    let mut cases = vec![
        (
            "ok",
            "",
            200,
            "success",
            false,
            None,
            Some("success"),
            "none",
        ),
        (
            "empty",
            "",
            204,
            "success",
            false,
            None,
            Some("success"),
            "none",
        ),
        (
            "rejected",
            "",
            409,
            "rejected",
            true,
            Some("rejected"),
            Some("rejected"),
            "APP_FAILURE",
        ),
        (
            "error",
            "",
            503,
            "error",
            true,
            Some("error"),
            Some("error"),
            "APP_FAILURE",
        ),
        (
            "success-status",
            "",
            200,
            "success",
            true,
            None,
            Some("success"),
            "none",
        ),
        (
            "rejected",
            "x-recover: yes\r\n",
            200,
            "success",
            true,
            Some("rejected"),
            Some("rejected"),
            "none",
        ),
        (
            "error",
            "x-recover: yes\r\n",
            200,
            "success",
            true,
            Some("error"),
            Some("error"),
            "none",
        ),
        (
            "guard",
            "",
            403,
            "rejected",
            false,
            None,
            None,
            "GUARD_FAILURE",
        ),
        (
            "guard",
            "x-technical: yes\r\n",
            503,
            "error",
            false,
            None,
            None,
            "GUARD_FAILURE",
        ),
        (
            "missing",
            "",
            404,
            "rejected",
            false,
            None,
            None,
            "HTTP_CLIENT_ERROR",
        ),
        (
            "broken",
            "",
            500,
            "error",
            false,
            None,
            Some("writer_error"),
            "RESPONSE_ENCODING_ERROR",
        ),
        (
            "broken",
            "x-recover: yes\r\n",
            200,
            "error",
            false,
            None,
            Some("writer_error"),
            "RESPONSE_ENCODING_ERROR",
        ),
        (
            "header-error",
            "",
            500,
            "error",
            false,
            None,
            Some("writer_error"),
            "RESPONSE_ENCODING_ERROR",
        ),
        (
            "classified",
            "",
            409,
            "rejected",
            true,
            Some("rejected"),
            Some("rejected"),
            "APP_FAILURE",
        ),
        (
            "explicit-error-200",
            "",
            200,
            "success",
            true,
            Some("error"),
            Some("error"),
            "none",
        ),
        (
            "direct",
            "",
            409,
            "rejected",
            true,
            Some("rejected"),
            Some("rejected"),
            "APP_FAILURE",
        ),
        (
            "manual",
            "",
            409,
            "rejected",
            true,
            Some("rejected"),
            Some("rejected"),
            "APP_FAILURE",
        ),
        (
            "passthrough",
            "",
            409,
            "rejected",
            true,
            Some("rejected"),
            Some("rejected"),
            "APP_FAILURE",
        ),
        (
            "created-error",
            "",
            409,
            "rejected",
            true,
            Some("rejected"),
            Some("rejected"),
            "APP_FAILURE",
        ),
        (
            "accepted-error",
            "",
            409,
            "rejected",
            true,
            Some("rejected"),
            Some("rejected"),
            "APP_FAILURE",
        ),
        (
            "raw-status",
            "",
            409,
            "rejected",
            false,
            None,
            Some("rejected"),
            "HTTP_CLIENT_ERROR",
        ),
        (
            "ok",
            "x-middleware-status: 429\r\n",
            429,
            "rejected",
            false,
            None,
            None,
            "MIDDLEWARE_FAILURE",
        ),
        (
            "ok",
            "x-middleware-status: 503\r\n",
            503,
            "error",
            false,
            None,
            None,
            "MIDDLEWARE_FAILURE",
        ),
        (
            "ok",
            "x-middleware-status: 503\r\nx-recover: yes\r\n",
            200,
            "success",
            false,
            None,
            None,
            "none",
        ),
    ]
    .into_iter()
    .map(
        |(
            path,
            headers,
            status,
            outcome,
            application_error,
            application_outcome,
            handler,
            error_code,
        )| Case {
            path,
            method: "GET",
            request_body: "",
            headers,
            status,
            outcome,
            application_error,
            application_outcome,
            handler,
            error_code,
        },
    )
    .collect::<Vec<_>>();
    cases.extend([
        Case {
            path: "stream-exact",
            method: "GET",
            request_body: "",
            headers: "",
            status: 200,
            outcome: "success",
            application_error: false,
            application_outcome: None,
            handler: Some("success"),
            error_code: "none",
        },
        Case {
            path: "stream-empty",
            method: "GET",
            request_body: "",
            headers: "",
            status: 200,
            outcome: "success",
            application_error: false,
            application_outcome: None,
            handler: Some("success"),
            error_code: "none",
        },
        Case {
            path: "stream-error",
            method: "GET",
            request_body: "",
            headers: "",
            status: 200,
            outcome: "error",
            application_error: false,
            application_outcome: None,
            handler: Some("success"),
            error_code: "RESPONSE_STREAM_SOURCE_FAILED",
        },
        Case {
            path: "head",
            method: "HEAD",
            request_body: "",
            headers: "",
            status: 200,
            outcome: "success",
            application_error: false,
            application_outcome: None,
            handler: Some("success"),
            error_code: "none",
        },
        Case {
            path: "ok",
            method: "OPTIONS",
            request_body: "",
            headers: "Origin: https://telemetry.test\r\nAccess-Control-Request-Method: GET\r\n",
            status: 200,
            outcome: "success",
            application_error: false,
            application_outcome: None,
            handler: None,
            error_code: "none",
        },
        Case {
            path: "ok",
            method: "GET",
            request_body: "",
            headers: "Origin: invalid-origin\r\n",
            status: 400,
            outcome: "rejected",
            application_error: false,
            application_outcome: None,
            handler: None,
            error_code: "CORS_ORIGIN_INVALID",
        },
        Case {
            path: "ok",
            method: "GET",
            request_body: "",
            headers: "Origin: https://unavailable.test\r\n",
            status: 503,
            outcome: "error",
            application_error: false,
            application_outcome: None,
            handler: None,
            error_code: "CORS_UNAVAILABLE",
        },
        Case {
            path: "ok",
            method: "OPTIONS",
            request_body: "x",
            headers: "Origin: https://telemetry.test\r\nAccess-Control-Request-Method: GET\r\n",
            status: 400,
            outcome: "rejected",
            application_error: false,
            application_outcome: None,
            handler: None,
            error_code: "INVALID_REQUEST_BODY",
        },
    ]);
    for (path, headers, status) in [
        ("static-file", "", 200),
        ("static-file", "Range: bytes=2-5\r\n", 206),
        ("static-empty", "", 200),
    ] {
        cases.push(Case {
            path,
            headers,
            status,
            method: "GET",
            request_body: "",
            outcome: "success",
            application_error: false,
            application_outcome: None,
            handler: Some("success"),
            error_code: "none",
        });
    }
    let responses = futures::future::join_all(cases.iter().enumerate().map(|(index, case)| async move {
        let id = index as u128 + 1;
        let trace = format!("{id:032x}");
        let parent = format!("{:016x}", index + 100);
        let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
        socket.write_all(format!("{} /telemetry/{} HTTP/1.1\r\nHost: localhost\r\ntraceparent: 00-{trace}-{parent}-01\r\n{}Content-Length: {}\r\nConnection: close\r\n\r\n{}", case.method, case.path, case.headers, case.request_body.len(), case.request_body).as_bytes()).await.unwrap();
        let mut response = Vec::new();
        if case.path == "stream-error" {
            bounded(async {
                while !response.ends_with(b"\r\n\r\n") {
                    assert!(response.len() < 8192, "response head must be bounded");
                    response.push(socket.read_u8().await.unwrap());
                }
                let mut chunk = [0; 10];
                socket.read_exact(&mut chunk).await.unwrap();
                assert_eq!(&chunk, b"5\r\nfirst\r\n");
                response.extend_from_slice(&chunk);
                RELEASE_STREAM_FAILURE.notify_one();
            }).await;
        }
        bounded(socket.read_to_end(&mut response)).await.unwrap();
        let text = String::from_utf8(response).unwrap();
        assert!(text.starts_with(&format!("HTTP/1.1 {} ", case.status)), "{text}");
        for secret in ["private-serialization-canary", "private-header-canary", "private-stream-canary", "partial-canary", "x-partial"] {
            assert!(!text.to_lowercase().contains(secret), "partial/private response leaked: {text}");
        }
        let (_, body) = text.split_once("\r\n\r\n").unwrap();
        if case.path == "stream-exact" { assert_eq!(body, "firstlast"); }
        if case.path == "stream-empty" { assert!(body.is_empty()); }
        if case.path == "static-file" { assert_eq!(body, if case.status == 206 { "atic" } else { "static-file-bytes" }); }
        if case.path == "static-empty" { assert!(body.is_empty()); }
        if case.application_error { assert_eq!(body, r#"{"failure":true}"#); }
        if case.status == 204 || case.method == "HEAD" || case.method == "OPTIONS" && case.status == 200 { assert!(body.is_empty()); }
        let body_bytes = if case.path == "stream-error" {
            assert_eq!(body, "5\r\nfirst\r\n", "failed chunk stream must not have a successful EOF");
            5
        } else { body.len() as u64 };
        (TraceId::from_hex(&trace).unwrap(), SpanId::from_hex(&parent).unwrap(), body_bytes)
    })).await;
    bounded(app.close()).await.unwrap();
    bounded(server).await.unwrap().unwrap();
    provider.force_flush().unwrap();
    meters.force_flush().unwrap();
    let spans = exports.get_finished_spans().unwrap();
    for (case, (trace, parent, body_bytes)) in cases.iter().zip(&responses) {
        let requests: Vec<_> = spans
            .iter()
            .filter(|span| {
                span.name == "http.server.request" && span.span_context.trace_id() == *trace
            })
            .collect();
        assert_eq!(requests.len(), 1, "one terminal request per caller trace");
        let request = requests[0];
        assert_eq!(request.parent_span_id, *parent);
        assert_ne!(request.span_context.span_id(), *parent);
        assert_eq!(
            matches!(request.status, Status::Error { .. }),
            case.outcome == "error",
            "{} {:?}: {:?}",
            case.path,
            case.headers,
            request.status
        );
        if case.outcome != "error" {
            assert_eq!(request.status, Status::Unset);
        }
        unique(request);
        assert_eq!(
            one(&request.attributes, "http.request.body.size"),
            &Value::I64(0)
        );
        assert_eq!(
            one(&request.attributes, "http.response.body.size"),
            &Value::I64(*body_bytes as i64)
        );
        match case.application_outcome {
            Some(outcome) => assert_eq!(
                one(&request.attributes, "lily.application_outcome"),
                &Value::from(outcome)
            ),
            None => assert!(!request
                .attributes
                .iter()
                .any(|p| p.key.as_str() == "lily.application_outcome")),
        }
        if case.application_error {
            assert_eq!(
                one(&request.attributes, "lily.application_error_code"),
                &Value::from("APP_FAILURE")
            );
        }
        if case.error_code != "none" {
            assert_eq!(
                one(&request.attributes, "lily.error_code"),
                &Value::from(case.error_code)
            );
        } else {
            assert!(!request
                .attributes
                .iter()
                .any(|p| p.key.as_str() == "lily.error_code"));
        }
        assert_eq!(
            one(&request.attributes, "http.response.status_code"),
            &Value::I64(i64::from(case.status))
        );
        assert_eq!(
            one(&request.attributes, "lily.outcome"),
            &Value::from(case.outcome)
        );
        assert_eq!(
            one(&request.attributes, "lily.application_error"),
            &Value::Bool(case.application_error)
        );
        assert_eq!(
            request
                .events
                .iter()
                .filter(|event| event
                    .attributes
                    .iter()
                    .any(|pair| pair.key.as_str() == "lily.event"
                        && pair.value == Value::from("http.server.terminal")))
                .count(),
            1
        );
        let terminal = request
            .events
            .iter()
            .find(|event| {
                event.attributes.iter().any(|p| {
                    p.key.as_str() == "lily.event" && p.value == Value::from("http.server.terminal")
                })
            })
            .unwrap();
        assert_eq!(
            one(&terminal.attributes, "lily.outcome"),
            &Value::from(case.outcome)
        );
        assert_eq!(
            one(&terminal.attributes, "lily.error_code"),
            &Value::from(case.error_code)
        );
        let app_completions: Vec<_> = request
            .events
            .iter()
            .filter(|event| event.name.starts_with("[RUNTIME] APP: Request completed"))
            .collect();
        let dispatched_to_app = case.handler.is_some()
            || matches!(case.path, "guard" | "missing")
            || case.headers.contains("x-middleware-status");
        assert_eq!(
            app_completions.len(),
            usize::from(dispatched_to_app),
            "{case:?}"
        );
        if dispatched_to_app {
            let event = app_completions[0];
            // Lazy stream failure occurs after successful response preparation.
            let prepared_outcome = if case.path == "stream-error" {
                "success"
            } else {
                case.outcome
            };
            assert_eq!(
                one(&event.attributes, "outcome"),
                &Value::from(prepared_outcome)
            );
            assert_eq!(
                one(&event.attributes, "level"),
                &Value::from(match prepared_outcome {
                    "success" => "INFO",
                    "rejected" => "WARN",
                    _ => "ERROR",
                })
            );
            assert!(matches!(
                one(&event.attributes, "duration_ms"),
                Value::F64(_)
            ));
        }
        let guards: Vec<_> = spans
            .iter()
            .filter(|span| {
                span.name == "http.server.authorization" && span.span_context.trace_id() == *trace
            })
            .collect();
        assert_eq!(guards.len(), usize::from(case.path == "guard"));
        if case.path == "guard" {
            let guard = guards[0];
            assert_eq!(guard.parent_span_id, request.span_context.span_id());
            assert_eq!(
                one(&guard.attributes, "lily.outcome"),
                &Value::from(case.outcome)
            );
            assert_eq!(
                matches!(guard.status, Status::Error { .. }),
                case.status == 503
            );
        }
        let handlers: Vec<_> = spans
            .iter()
            .filter(|span| {
                span.name == "http.server.handler" && span.span_context.trace_id() == *trace
            })
            .collect();
        assert_eq!(handlers.len(), usize::from(case.handler.is_some()));
        if let Some(expected) = case.handler {
            let handler = handlers[0];
            unique(handler);
            assert_eq!(handler.parent_span_id, request.span_context.span_id());
            assert_eq!(
                one(&handler.attributes, "lily.outcome"),
                &Value::from(expected)
            );
            assert_eq!(
                matches!(handler.status, Status::Error { .. }),
                matches!(expected, "error" | "writer_error")
            );
            let completions: Vec<_> = handler
                .events
                .iter()
                .filter(|event| event.attributes.iter().any(|p| p.key.as_str() == "outcome"))
                .collect();
            assert_eq!(
                completions.len(),
                1,
                "one handler completion event: {case:?}"
            );
            let completion = completions[0];
            assert_eq!(
                one(&completion.attributes, "outcome"),
                &Value::from(expected)
            );
            assert_eq!(
                one(&completion.attributes, "level"),
                &Value::from(match expected {
                    "rejected" => "WARN",
                    "error" | "writer_error" => "ERROR",
                    _ => "INFO",
                })
            );
            assert!(matches!(
                one(&completion.attributes, "duration_ms"),
                Value::F64(_)
            ));
        }
        for span in spans
            .iter()
            .filter(|span| span.span_context.trace_id() == *trace)
        {
            unique(span);
        }
    }
    // Verify the exact metric series/counts, including recovery and 2xx Err.
    // Merely checking that a metric exists would miss duplicate completion.
    let mut expected_terminals = BTreeMap::new();
    let mut expected_bodies = BTreeMap::new();
    let mut expected_handlers = BTreeMap::new();
    for (case, (_, _, bytes)) in cases.iter().zip(&responses) {
        let key = format!("{}|{}|{}", case.status, case.outcome, case.error_code);
        *expected_terminals.entry(key.clone()).or_insert(0_u64) += 1;
        *expected_bodies.entry(key).or_insert(0_u64) += bytes;
        if let Some(handler) = case.handler {
            let code = if case.application_error {
                "APP_FAILURE"
            } else {
                match handler {
                    "writer_error" => "RESPONSE_ENCODING_ERROR",
                    "rejected" => "HTTP_CLIENT_ERROR",
                    _ => "none",
                }
            };
            *expected_handlers
                .entry(format!("/telemetry/{}|{handler}|{code}", case.path))
                .or_insert(0_u64) += 1;
        }
    }
    let batches = metrics.get_finished_metrics().unwrap();
    assert_eq!(
        batches.len(),
        1,
        "only the explicit flush should export metrics"
    );
    let mut verified = HashSet::new();
    for metric in batches
        .iter()
        .flat_map(|r| r.scope_metrics())
        .flat_map(|s| s.metrics())
    {
        match (metric.name(), metric.data()) {
            ("http.server.guard.outcomes", AggregatedMetrics::U64(MetricData::Sum(sum))) => {
                let actual: BTreeMap<_, _> = sum
                    .data_points()
                    .map(|point| {
                        let attrs: Vec<_> = point.attributes().cloned().collect();
                        assert_eq!(
                            one(&attrs, "lily.guard.name"),
                            &Value::from(std::any::type_name::<Admission>())
                        );
                        assert_eq!(
                            one(&attrs, "lily.error_code"),
                            &Value::from("GUARD_FAILURE")
                        );
                        (
                            one(&attrs, "lily.outcome").as_str().into_owned(),
                            point.value(),
                        )
                    })
                    .collect();
                assert_eq!(
                    actual,
                    BTreeMap::from([("rejected".to_string(), 1), ("error".to_string(), 1)])
                );
                verified.insert(metric.name());
            }
            ("http.server.handler.outcomes", AggregatedMetrics::U64(MetricData::Sum(sum))) => {
                let actual: BTreeMap<_, _> = sum
                    .data_points()
                    .map(|p| {
                        let attrs: Vec<_> = p.attributes().cloned().collect();
                        (
                            format!(
                                "{}|{}|{}",
                                one(&attrs, "http.route").as_str(),
                                one(&attrs, "lily.outcome").as_str(),
                                one(&attrs, "lily.error_code").as_str()
                            ),
                            p.value(),
                        )
                    })
                    .collect();
                assert_eq!(actual, expected_handlers);
                verified.insert(metric.name());
            }
            (
                "http.server.request.duration",
                AggregatedMetrics::F64(MetricData::Histogram(hist)),
            ) => {
                let actual: BTreeMap<_, _> = hist
                    .data_points()
                    .map(|p| {
                        assert!(p.sum().is_finite() && p.sum() > 0.0);
                        (terminal_metric_key(p.attributes()), p.count())
                    })
                    .collect();
                assert_eq!(actual, expected_terminals);
                verified.insert(metric.name());
            }
            (
                name @ ("http.server.request.body.size" | "http.server.response.body.size"),
                AggregatedMetrics::U64(MetricData::Histogram(hist)),
            ) => {
                let actual: BTreeMap<_, _> = hist
                    .data_points()
                    .map(|p| (terminal_metric_key(p.attributes()), p.count()))
                    .collect();
                assert_eq!(actual, expected_terminals);
                for p in hist.data_points() {
                    let expected = if name == "http.server.request.body.size" {
                        0
                    } else {
                        expected_bodies[&terminal_metric_key(p.attributes())]
                    };
                    assert_eq!(p.sum(), expected);
                }
                verified.insert(name);
            }
            ("http.server.requests", AggregatedMetrics::U64(MetricData::Sum(sum))) => {
                assert_eq!(
                    sum.data_points().map(|p| p.value()).sum::<u64>(),
                    cases.len() as u64
                );
                verified.insert(metric.name());
            }
            (
                name @ ("http.server.requests.in_flight" | "http.server.connections.active"),
                AggregatedMetrics::I64(MetricData::Sum(sum)),
            ) => {
                assert!(
                    sum.data_points().all(|p| p.value() == 0),
                    "{name} leaked at shutdown"
                );
                verified.insert(name);
            }
            _ => {}
        }
    }
    assert_eq!(
        verified,
        HashSet::from([
            "http.server.guard.outcomes",
            "http.server.handler.outcomes",
            "http.server.request.duration",
            "http.server.request.body.size",
            "http.server.response.body.size",
            "http.server.requests",
            "http.server.requests.in_flight",
            "http.server.connections.active"
        ])
    );
    meters.shutdown().unwrap();
    provider.shutdown().unwrap();
}

fn terminal_metric_key<'a>(attributes: impl Iterator<Item = &'a KeyValue>) -> String {
    let attributes: Vec<_> = attributes.cloned().collect();
    assert_eq!(
        one(&attributes, "network.protocol.version"),
        &Value::from("1.1")
    );
    format!(
        "{}|{}|{}",
        one(&attributes, "http.response.status_code").as_str(),
        one(&attributes, "lily.outcome").as_str(),
        one(&attributes, "lily.error_code").as_str()
    )
}
