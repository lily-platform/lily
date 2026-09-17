use lily_trace::runtime::{TraceConfig, TraceInstallOutcome, TracingRuntimeOwner};
use lily_websocket::{
    async_trait, websocket_controller, Extensions, NoReply, Payload, ServerConfig,
    WebSocketActionError, WebSocketController, WebSocketControllerInitError,
    WebSocketControllerTrait, WsAppBuilder,
};
use lily_websocket_client::{
    TokioWsClient, WebSocketClientConfig, WsClient, LILY_WEBSOCKET_SUBPROTOCOL,
};
use serde::{Deserialize, Serialize};
use std::{env, io, sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

#[derive(WebSocketController)]
#[namespace("qualification")]
struct QualificationController;

#[async_trait]
impl WebSocketControllerTrait for QualificationController {
    async fn new(_extensions: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}

#[websocket_controller]
impl QualificationController {
    #[message("get_status")]
    async fn get_status(
        &self,
        Payload(_request): Payload<StatusRequest>,
    ) -> Result<NoReply, WebSocketActionError> {
        Ok(NoReply)
    }
}

#[derive(Deserialize, Serialize)]
struct StatusRequest {
    request_id: String,
}

fn required_env(name: &str) -> io::Result<String> {
    env::var(name).map_err(|_| io::Error::other(format!("required environment {name} is missing")))
}

async fn shutdown_tracing(owner: TracingRuntimeOwner) -> io::Result<()> {
    let report = owner.shutdown(Duration::from_secs(15)).await;
    if report.is_success() {
        Ok(())
    } else {
        Err(io::Error::other(format!(
            "golden-path telemetry shutdown did not reconcile: {report:?}"
        )))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let address = required_env("LILY_GOLDEN_WS_ADDRESS")?;
    let url = required_env("LILY_GOLDEN_WS_URL")?;
    let trace_path = required_env("LILY_GOLDEN_TRACE_CONFIG")?;
    let trace_config = TraceConfig::try_load_from_path(trace_path)?;
    let tracing_owner = match TracingRuntimeOwner::install(&trace_config)? {
        TraceInstallOutcome::Owned(owner) => owner,
        TraceInstallOutcome::Disabled => {
            return Err(io::Error::other("golden-path telemetry must be enabled").into());
        }
    };

    let server_config = ServerConfig {
        max_connections: 128,
        max_message_size: 1024 * 1024,
        max_frame_size: 256 * 1024,
        allow_missing_origin: true,
        require_subprotocol: true,
        outbound_queue_capacity: 128,
        ..ServerConfig::default()
    };
    let server = WsAppBuilder::new(&address)
        .config(server_config)
        .build()
        .await?;
    let server_cancellation = CancellationToken::new();
    let server_stop = server_cancellation.clone();
    let server_task =
        tokio::spawn(async move { server.start_with_cancellation(server_stop).await });

    let mut client_config = WebSocketClientConfig {
        url,
        namespace: Some("qualification".to_string()),
        require_subprotocol: true,
        ..WebSocketClientConfig::default()
    };
    client_config
        .subprotocols
        .push(LILY_WEBSOCKET_SUBPROTOCOL.to_string());
    let client = TokioWsClient::with_config(client_config)?;
    let client_cancellation = CancellationToken::new();

    let connect_deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        match client.connect(client_cancellation.clone()).await {
            Ok(()) => break,
            Err(error) if tokio::time::Instant::now() < connect_deadline => {
                eprintln!("waiting for golden WebSocket server readiness: {error}");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(error) => {
                server_cancellation.cancel();
                let _ = server_task.await;
                shutdown_tracing(tracing_owner).await?;
                return Err(error.into());
            }
        }
    }

    let interaction = client
        .send(
            "qualification:get_status",
            StatusRequest {
                request_id: "golden-1".to_owned(),
            },
        )
        .await;
    let disconnect = client.disconnect().await;
    client_cancellation.cancel();
    server_cancellation.cancel();
    let server_result = server_task
        .await
        .map_err(|error| io::Error::other(format!("WebSocket server task failed: {error}")))?;

    interaction?;
    disconnect?;
    server_result?;
    shutdown_tracing(tracing_owner).await?;
    Ok(())
}
