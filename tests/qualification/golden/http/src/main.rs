use lily_http_api::{
    controller, AppBuilder, Controller, ControllerInitError, ControllerTrait, Extensions,
    HttpApiError, HttpProtocol,
};
use lily_trace::runtime::TraceConfig;
use serde::Serialize;
use std::{env, io, sync::Arc};

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    profile: &'static str,
}

#[derive(Controller)]
#[base_path("/api/qualification")]
struct QualificationController;

#[lily_http_api::async_trait::async_trait]
impl ControllerTrait for QualificationController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, ControllerInitError> {
        Ok(Self)
    }
}

#[controller]
impl QualificationController {
    #[get("/qualification/health")]
    async fn health(&self) -> Result<HealthResponse, HttpApiError> {
        Ok(HealthResponse {
            status: "ok",
            profile: "http-v1",
        })
    }
}

fn required_env(name: &str) -> io::Result<String> {
    env::var(name).map_err(|_| io::Error::other(format!("required environment {name} is missing")))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let address = required_env("LILY_GOLDEN_HTTP_ADDRESS")?;
    let trace_path = required_env("LILY_GOLDEN_TRACE_CONFIG")?;
    let trace_config = TraceConfig::try_load_from_path(trace_path)?;
    if !trace_config.is_enabled() {
        return Err(io::Error::other("golden-path telemetry must be enabled").into());
    }

    AppBuilder::new(&address)
        .protocol(HttpProtocol::Auto)
        .tracing_config(trace_config)
        .build()
        .await?
        .start()
        .await?;
    Ok(())
}
