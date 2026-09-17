use lily_http_api::utoipa::{self, ToSchema};
use lily_http_api::{
    controller, AppBuilder, ConfigError, Controller, ControllerInitError, ControllerTrait,
    CorsPolicy, Extensions, GuardInitError, GuardRejection, GuardTrait, HealthRegistryError,
    HealthSnapshot, HttpExchange, HttpHealthService, HttpMiddleware, HttpMiddlewareError,
    HttpMiddlewareInitError, HttpNext, HttpProtocol, HttpTransportConfig, MiddlewareDescriptor,
    MiddlewareKind, OpenApiConfig, Request, ResolvedSecret, SecretResolver, TraceConfig,
    TraceConfigLoadError,
};
use lily_http_client::{
    client::{ClientConfig, HttpClient, HttpClientBuilder, ProtocolPreference},
    request::RequestConfig,
    LilyHttpClientFactory,
};
use std::{sync::Arc, time::Duration};

#[derive(serde::Serialize, ToSchema)]
struct FixtureHealthView {
    status: String,
}

#[derive(Controller)]
#[base_path("/")]
#[openapi(tag = "Fixture")]
struct FixtureController;

#[lily_http_api::async_trait::async_trait]
impl ControllerTrait for FixtureController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl FixtureController {
    #[get("/health")]
    #[openapi(
        operation_id = "fixture.health",
        responses((
            status = 503,
            description = "Fixture unavailable",
            schema = FixtureHealthView,
            example = "unavailable"
        ))
    )]
    async fn fixture_health(&self) -> Result<FixtureHealthView, lily_http_api::HttpApiError> {
        Ok(FixtureHealthView {
            status: "ok".to_owned(),
        })
    }
}

struct DownstreamGuard;

struct DownstreamMiddleware;

struct DownstreamSecretResolver;

#[lily_http_api::async_trait::async_trait]
impl SecretResolver for DownstreamSecretResolver {
    async fn resolve(&self, key: &str) -> Result<String, ConfigError> {
        Ok(format!("resolved:{key}"))
    }

    async fn resolve_versioned(&self, key: &str) -> Result<ResolvedSecret, ConfigError> {
        Ok(ResolvedSecret::versioned(
            format!("resolved:{key}"),
            "fixture-generation",
            None,
        ))
    }
}

#[lily_http_api::async_trait::async_trait]
impl HttpMiddleware for DownstreamMiddleware {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, HttpMiddlewareInitError> {
        Ok(Self)
    }

    fn descriptor(&self) -> MiddlewareDescriptor {
        MiddlewareDescriptor::new("downstream_fixture", MiddlewareKind::Custom)
    }

    async fn handle(
        &self,
        exchange: &mut HttpExchange<'_>,
        next: HttpNext<'_>,
        _cancellation: lily_http_api::ExecutionCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        if let Some(state) = exchange.termination_state_mut() {
            state.insert(String::from("fixture cleanup state"));
        }
        next.run(exchange).await
    }

    async fn on_request_termination(
        &self,
        context: &mut lily_http_api::HttpRequestTerminationContext<'_>,
        cancellation: lily_http_api::CleanupCancellation,
    ) -> Result<(), HttpMiddlewareError> {
        let _retained = context.state_mut().remove::<String>();
        let _remaining = cancellation.deadline();
        let _same_authority = context.cancellation();
        Ok(())
    }
}

#[lily_http_api::async_trait::async_trait]
impl GuardTrait for DownstreamGuard {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, GuardInitError> {
        Ok(Self)
    }

    async fn can_activate(
        &self,
        _request: &mut Request,
        _cancellation: lily_http_api::ExecutionCancellation,
    ) -> Result<(), GuardRejection> {
        Ok(())
    }
}

/// Type-check the public HTTP application composition path without loading
/// configuration, constructing DI state, or opening a listener.
#[allow(dead_code)]
fn compile_http_application() {
    let openapi = OpenApiConfig::new("Downstream Fixture API", "1.0.0")
        .expect("fixture OpenAPI identity is valid");
    let mut tracing = TraceConfig::default();
    tracing.enabled = true;
    tracing.service_name = "downstream-fixture".to_owned();

    let build = AppBuilder::new("127.0.0.1:8080")
        .protocol(HttpProtocol::Auto)
        .transport_config(HttpTransportConfig::default())
        .cors(
            CorsPolicy::new()
                .allow_origins(["https://fixture.example"])
                .allow_methods(["GET"]),
        )
        .middleware::<DownstreamMiddleware>()
        .guard(DownstreamGuard)
        .secret_resolver(DownstreamSecretResolver)
        .tracing_config(tracing)
        .openapi(openapi)
        .build();

    // An async function does not execute until polled. Dropping this future is
    // intentional: this fixture is a downstream API compile contract.
    drop(build);
}

#[allow(dead_code)]
fn compile_health_snapshot(
    health: &HttpHealthService,
) -> Result<HealthSnapshot, HealthRegistryError> {
    health.snapshot()
}

#[allow(dead_code)]
fn compile_trace_file_loading(path: &str) -> Result<TraceConfig, TraceConfigLoadError> {
    TraceConfig::try_load_from_path(path)
}

/// Type-check the configured factory lookup and the canonical request
/// build/execute path without initializing DI or performing network I/O.
#[allow(dead_code)]
fn compile_http_client_factory(
    factory: &LilyHttpClientFactory,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = factory.get("fixture_api")?;
    let request = client.get("/health")?.build()?;
    let execute = client.execute(request);
    drop(execute);
    Ok(())
}

/// Freeze the fail-fast, typed direct-client construction paths from a crate
/// that cannot access Lily's private config representation.
#[allow(dead_code)]
fn compile_direct_http_client() -> Result<(), Box<dyn std::error::Error>> {
    let config = ClientConfig::default()
        .with_base_address("https://fixture.example/api")
        .with_connect_timeout(Duration::from_secs(5))
        .with_request_timeout(Duration::from_secs(15))
        .with_max_redirects(2)
        .with_protocol(ProtocolPreference::Auto)
        .with_max_in_flight_requests(64)
        .with_max_in_flight_requests_per_origin(16)
        .with_max_request_body_bytes(1024 * 1024)
        .with_max_response_body_bytes(2 * 1024 * 1024)
        .with_max_header_count(64)
        .with_max_header_bytes(16 * 1024)
        .with_pool_idle_timeout(Duration::from_secs(30))
        .with_pool_max_idle_per_host(8)
        .with_http2_initial_stream_window_bytes(64 * 1024)
        .with_http2_initial_connection_window_bytes(128 * 1024)
        .with_http2_max_frame_bytes(16 * 1024)
        .with_http2_keep_alive_interval(Duration::from_secs(20))
        .with_http2_keep_alive_timeout(Duration::from_secs(5))
        .with_retry_unstarted_requests(true)
        .try_with_user_agent("lily-downstream-fixture/1.0")?
        .try_with_default_header("X-Fixture", "enabled")?;
    let client = HttpClient::try_with_config(config)?;

    let request_config = RequestConfig::new()
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(10))
        .max_redirects(1)
        .follow_redirects(true)
        .protocol(ProtocolPreference::Http1Only);
    let mut request = client.get("/health")?;
    request.config(request_config);
    let request = request.build()?;
    drop(client.execute(request));

    let _builder_client = HttpClientBuilder::new()
        .base_address("https://fixture.example/api")
        .protocol(ProtocolPreference::Http1Only)
        .user_agent("lily-downstream-fixture/1.0")?
        .try_build()?;
    Ok(())
}

fn main() {}

/// A successful build retains an async application-lifecycle obligation even
/// when the host never starts listening. The facade exposes the close handle.
#[allow(dead_code)]
async fn compile_managed_close(app: &lily_http_api::App) -> std::io::Result<()> {
    app.close().await
}

#[allow(dead_code)]
async fn compile_managed_start(
    app: lily_http_api::App,
    stop: lily_http_api::CancellationToken,
) -> std::io::Result<()> {
    app.start_with_cancellation(stop).await
}
