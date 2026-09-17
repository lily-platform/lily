use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use lily_cancellation::ExecutionCancellation;
use lily_http_api::*;
use lily_injection::Injectable;
use serde::Deserialize;

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct Probe {
    views: Mutex<Vec<ExecutionCancellation>>,
    events: Mutex<Vec<&'static str>>,
}
impl ServiceTrait for Probe {}

impl Probe {
    fn view(&self, view: ExecutionCancellation) {
        self.views.lock().unwrap().push(view);
    }
    fn event(&self, event: &'static str) {
        self.events.lock().unwrap().push(event);
    }
}

struct Around(Arc<Probe>);
#[async_trait::async_trait]
impl HttpMiddleware for Around {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
        Ok(Self(extensions.get_service::<Probe>(None).await.unwrap()))
    }
    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("phase4-around", MiddlewareKind::Custom)
    }
    async fn handle(
        &self,
        exchange: &mut HttpExchange<'_>,
        next: HttpNext<'_>,
        cancellation: ExecutionCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        self.0.view(cancellation.clone());
        self.0.view(exchange.execution_cancellation());
        self.0.event("before");
        if exchange.request().path().ends_with("/before") {
            cancellation.cancelled().await;
            self.0.event("before-cancelled");
        }
        next.run(exchange).await?;
        if exchange.request().path().ends_with("/after") {
            cancellation.cancelled().await;
            self.0.event("after-cancelled");
            exchange
                .response_mut()
                .try_insert_header("x-cooperative-after", "completed")
                .unwrap();
        }
        self.0.event("after");
        Ok(())
    }
}

struct RouteGuard(Arc<Probe>);
#[async_trait::async_trait]
impl GuardTrait for RouteGuard {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, GuardInitError> {
        Ok(Self(extensions.get_service::<Probe>(None).await.unwrap()))
    }
    async fn can_activate(
        &self,
        request: &mut Request,
        cancellation: ExecutionCancellation,
    ) -> Result<(), GuardRejection> {
        self.0.view(cancellation.clone());
        self.0.view(request.execution_cancellation());
        self.0.event("guard");
        if request.path().ends_with("/guard") {
            cancellation.cancelled().await;
            self.0.event("guard-cancelled");
        }
        Ok(())
    }
}

struct OriginResolver(Arc<Probe>);
#[async_trait::async_trait]
impl CorsOriginResolver for OriginResolver {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, CorsOriginResolverInitError> {
        Ok(Self(extensions.get_service::<Probe>(None).await.unwrap()))
    }
    async fn allows(
        &self,
        context: &CorsOriginContext<'_>,
    ) -> Result<bool, CorsOriginResolverError> {
        let view = context.execution_cancellation();
        assert!(
            !view.is_cancelled(),
            "a new request cannot inherit a previous request's signal"
        );
        self.0.view(view.clone());
        self.0.event("cors");
        if context.path().ends_with("/cors") {
            view.cancelled().await;
            self.0.event("cors-cancelled");
        }
        Ok(true)
    }
}

#[derive(Deserialize)]
struct Stage {
    stage: String,
}

struct PartsProbe;
impl FromRequestParts for PartsProbe {
    type Rejection = HttpApiError;
    async fn from_request_parts(
        request: &mut Request,
        extensions: &Extensions,
    ) -> Result<Self, Self::Rejection> {
        if request.path().ends_with("/extractor") {
            let probe = extensions.get_service::<Probe>(None).await.unwrap();
            let view = request.execution_cancellation();
            probe.view(view.clone());
            view.cancelled().await;
            probe.event("extractor-cancelled");
        }
        Ok(Self)
    }
}

struct ConvertedResponse(Arc<Probe>, bool);
#[async_trait::async_trait]
impl IntoResponse for ConvertedResponse {
    async fn write_to_response(
        self,
        response: &mut Response,
        request: &mut Request,
    ) -> Result<ResponseWriteOutcome, ResponseWriteError> {
        if self.1 {
            let view = request.execution_cancellation();
            self.0.view(view.clone());
            view.cancelled().await;
            self.0.event("response-cancelled");
        }
        PlainText("normal action returned".into())
            .write_to_response(response, request)
            .await
    }
}

#[derive(Controller)]
#[base_path("/phase4")]
struct CancellationController {
    probe: Arc<Probe>,
}
#[async_trait::async_trait]
impl ControllerTrait for CancellationController {
    async fn new(extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self {
            probe: extensions.get_service::<Probe>(None).await.unwrap(),
        })
    }
}

#[controller]
impl CancellationController {
    #[get("/:stage")]
    #[guard(RouteGuard)]
    async fn execute(
        &self,
        _parts: PartsProbe,
        Path(stage): Path<Stage>,
        cancellation: ExecutionCancellation,
        request: &mut Request,
    ) -> ConvertedResponse {
        self.probe.view(cancellation.clone());
        self.probe.view(request.execution_cancellation());
        self.probe.event("action");
        request.local_mut().clear();
        if matches!(stage.stage.as_str(), "action" | "ignore") {
            cancellation.cancelled().await;
            assert!(request.execution_cancellation().is_cancelled());
            self.probe.event("action-cancelled");
        }
        if stage.stage == "ignore" {
            std::future::pending::<()>().await;
        }
        ConvertedResponse(self.probe.clone(), stage.stage == "response")
    }

    #[get("/error")]
    #[guard(RouteGuard)]
    async fn error(&self, cancellation: ExecutionCancellation) -> Result<String, HttpApiError> {
        self.probe.view(cancellation.clone());
        self.probe.event("action");
        cancellation.cancelled().await;
        self.probe.event("action-cancelled");
        Err(HttpApiError::Conflict("application conflict".into()))
    }

    #[post("/body")]
    async fn body(&self, body: RawBody) -> PlainText {
        self.probe.event("body-action");
        PlainText(String::from_utf8(body.into_inner().to_vec()).unwrap())
    }
}

async fn request(address: std::net::SocketAddr, stage: &str) -> (http::response::Parts, Bytes) {
    let socket = tokio::net::TcpStream::connect(address).await.unwrap();
    let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(socket))
        .await
        .unwrap();
    let client = tokio::spawn(connection);
    let response = sender
        .send_request(
            hyper::Request::builder()
                .uri(format!("/phase4/{stage}"))
                .header("host", "lily.test")
                .header("origin", "https://phase4.example")
                .header("connection", "close")
                .body(Full::new(Bytes::new()))
                .unwrap(),
        )
        .await
        .unwrap();
    let (head, body) = response.into_parts();
    let body = body.collect().await.unwrap().to_bytes();
    drop(sender);
    client.await.unwrap().unwrap();
    (head, body)
}

#[tokio::test]
async fn generated_action_guard_middleware_and_dynamic_cors_share_read_only_cancellation() {
    let app = AppBuilder::new("127.0.0.1:0")
        .middleware::<Around>()
        .cors(
            CorsPolicy::new()
                .resolve_origins_with::<OriginResolver>()
                .allow_methods(["GET"]),
        )
        .transport_config(HttpTransportConfig {
            request_timeout: Duration::from_millis(50),
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let probe = app.container().resolve::<Probe>(None).await.unwrap();
    let runtime = app.clone();
    let root = tokio::spawn(async move {
        runtime
            .start_with_cancellation(CancellationToken::new())
            .await
    });
    let address = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(address) = app.bound_address() {
                break address;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for stage in [
        "before",
        "guard",
        "action",
        "after",
        "cors",
        "extractor",
        "response",
        "error",
        "ignore",
    ] {
        probe.views.lock().unwrap().clear();
        probe.events.lock().unwrap().clear();
        let (head, body) = tokio::time::timeout(Duration::from_secs(5), request(address, stage))
            .await
            .unwrap();
        match stage {
            "ignore" => {
                assert_eq!(head.status, 504);
                assert!(String::from_utf8_lossy(&body).contains("GATEWAY_TIMEOUT"));
            }
            "error" => {
                assert_eq!(head.status, 409);
                assert!(String::from_utf8_lossy(&body).contains("CONFLICT"));
            }
            _ => {
                assert_eq!(head.status, 200, "cooperative return must survive: {stage}");
                assert_eq!(body, "normal action returned", "{stage}");
            }
        }
        if stage == "after" {
            assert_eq!(head.headers["x-cooperative-after"], "completed");
        }
        assert!(
            probe
                .views
                .lock()
                .unwrap()
                .iter()
                .all(ExecutionCancellation::is_cancelled),
            "{stage}"
        );
        let events = probe.events.lock().unwrap().clone();
        assert!(events.contains(&"action"), "{stage}: {events:?}");
        assert_eq!(
            events.contains(&"after"),
            stage != "ignore",
            "{stage}: {events:?}"
        );
        if stage != "ignore" {
            assert!(events.contains(&match stage {
                "before" => "before-cancelled",
                "guard" => "guard-cancelled",
                "action" | "error" => "action-cancelled",
                "after" => "after-cancelled",
                "cors" => "cors-cancelled",
                "extractor" => "extractor-cancelled",
                "response" => "response-cancelled",
                _ => unreachable!(),
            }));
        }
    }
    assert_eq!(request(address, "normal").await.0.status, 200);
    app.close().await.unwrap();
    root.await.unwrap().unwrap();
}

#[tokio::test]
async fn pending_request_body_shares_the_deadline_and_can_finish_in_the_cooperative_window() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let app = AppBuilder::new("127.0.0.1:0")
        .middleware::<Around>()
        .transport_config(HttpTransportConfig {
            request_timeout: Duration::from_millis(100),
            ..Default::default()
        })
        .build()
        .await
        .unwrap();
    let probe = app.container().resolve::<Probe>(None).await.unwrap();
    let runtime = app.clone();
    let root = tokio::spawn(async move {
        runtime
            .start_with_cancellation(CancellationToken::new())
            .await
    });
    let address = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(address) = app.bound_address() {
                break address;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    for complete in [true, false] {
        probe.views.lock().unwrap().clear();
        probe.events.lock().unwrap().clear();
        tokio::time::timeout(Duration::from_secs(5), async {
            let mut socket = tokio::net::TcpStream::connect(address).await.unwrap();
            socket.write_all(
                b"POST /phase4/body HTTP/1.1\r\nHost: lily.test\r\nContent-Length: 10\r\nConnection: close\r\n\r\nfirst"
            ).await.unwrap();
            let view = loop {
                let view = probe.views.lock().unwrap().first().cloned();
                if let Some(view) = view {
                    break view;
                }
                tokio::task::yield_now().await;
            };
            // The typed RawBody extractor is still consuming the upload when
            // the owner signals. A frame reader must not return its own 408.
            view.cancelled().await;
            assert!(!probe.events.lock().unwrap().contains(&"body-action"));
            if complete {
                socket.write_all(b" last").await.unwrap();
            }
            let mut response = Vec::new();
            socket.read_to_end(&mut response).await.unwrap();
            let response = String::from_utf8(response).unwrap();
            if complete {
                assert!(response.starts_with("HTTP/1.1 200"), "{response}");
                assert!(response.ends_with("first last"), "{response}");
            } else {
                assert!(response.starts_with("HTTP/1.1 504"), "{response}");
                assert!(response.contains("GATEWAY_TIMEOUT"), "{response}");
            }
            let events = probe.events.lock().unwrap();
            assert_eq!(events.contains(&"body-action"), complete);
            assert_eq!(events.contains(&"after"), complete);
        })
        .await
        .unwrap();
    }
    app.close().await.unwrap();
    root.await.unwrap().unwrap();
}
