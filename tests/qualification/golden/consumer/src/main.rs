use lily_consumer::ConsumerBuilder;
use lily_injectable_derive::Injectable;
use lily_injection::ServiceTrait;
use lily_queue::{Json, QueueHandlerError, queue, queue_service};
use lily_trace::runtime::TraceConfig;
use serde::{Deserialize, Serialize};
use std::{env, io};

#[derive(Debug, Deserialize, Serialize)]
struct QualificationEvent {
    event_id: String,
}

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct QualificationConsumer;

impl ServiceTrait for QualificationConsumer {}

#[queue_service]
impl QualificationConsumer {
    #[queue("qualification.events", version = 1, content = "json")]
    async fn handle(
        &self,
        Json(_message): Json<QualificationEvent>,
    ) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

fn required_env(name: &str) -> io::Result<String> {
    env::var(name).map_err(|_| io::Error::other(format!("required environment {name} is missing")))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let trace_path = required_env("LILY_GOLDEN_TRACE_CONFIG")?;
    let trace_config = TraceConfig::try_load_from_path(trace_path)?;
    if !trace_config.is_enabled() {
        return Err(io::Error::other("golden-path telemetry must be enabled").into());
    }

    ConsumerBuilder::new()
        .tracing_config(trace_config)
        .run()
        .await?;
    Ok(())
}
