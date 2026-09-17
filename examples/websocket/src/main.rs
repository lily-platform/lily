use lily::websocket::{
    Ack, Extensions, Payload, ServerConfig, Service, WebSocketActionError, WebSocketController,
    WebSocketControllerInitError, WebSocketControllerTrait, WebSocketErrorCode, WsAppBuilder,
    async_trait, websocket_controller,
};
use lily_example_models::{GetJob, JobView};
use lily_example_shared::{DemoError, JobOperations, SummaryWorker, tracing_config};
use std::sync::Arc;

fn action_error(error: DemoError) -> WebSocketActionError {
    if error.is_rejected() {
        WebSocketActionError::rejected(
            WebSocketErrorCode::new(error.code()).expect("static error code"),
            error.to_string(),
        )
        .expect("bounded public message")
    } else {
        WebSocketActionError::internal(error)
    }
}

#[derive(WebSocketController)]
#[namespace("jobs")]
struct JobsController;
#[async_trait]
impl WebSocketControllerTrait for JobsController {
    async fn new(_: Arc<Extensions>) -> Result<Self, WebSocketControllerInitError> {
        Ok(Self)
    }
}
#[websocket_controller]
impl JobsController {
    #[message("get")]
    async fn get(
        &self,
        service: Service<dyn JobOperations>,
        Payload(query): Payload<GetJob>,
    ) -> Result<Ack<JobView>, WebSocketActionError> {
        Ok(Ack::new(service.get(query.id).await.map_err(action_error)?))
    }
    #[message("ping")]
    async fn ping(&self) -> Result<Ack<&'static str>, WebSocketActionError> {
        Ok(Ack::new("pong"))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let address = std::env::var("EXAMPLE_WS_BIND").unwrap_or_else(|_| "127.0.0.1:58101".into());
    WsAppBuilder::new(&address)
        .config(ServerConfig {
            endpoint_path: "/ws".into(),
            allow_missing_origin: true,
            ..Default::default()
        })
        .tracing_config(tracing_config("websocket")?)
        .add_background_service::<SummaryWorker>()
        .build()
        .await?
        .start()
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn application_rejections_satisfy_websocket_transport_contract() {
        for error in [
            DemoError::InvalidInput,
            DemoError::NotFound,
            DemoError::Conflict,
        ] {
            assert!(WebSocketErrorCode::new(error.code()).is_ok());
            let _ = action_error(error);
        }
    }
}
