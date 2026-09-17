use lily::{
    consumer::{Consumer, Injectable, ServiceTrait},
    error::application::QueueHandlerError,
    queue::{Json, Service, asyncapi, queue, queue_service},
};
use lily_example_models::JobRequested;
use lily_example_shared::{DemoError, JobOperations, tracing_config};

#[derive(Default, Injectable)]
#[service(lifetime = "Singleton")]
struct JobHandler;
impl ServiceTrait for JobHandler {}

#[queue_service]
#[asyncapi(documented)]
impl JobHandler {
    #[queue("lily.examples.jobs", version = 1, content = "json")]
    async fn process(
        &self,
        service: Service<dyn JobOperations>,
        Json(job): Json<JobRequested>,
    ) -> Result<(), QueueHandlerError> {
        service.process(job).await.map_err(|error: DemoError| {
            if error.is_rejected() {
                QueueHandlerError::permanent_with_source(error.code(), error)
            } else {
                QueueHandlerError::retryable_with_source(error.code(), error)
            }
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    Consumer::builder()
        .tracing_config(tracing_config("consumer")?)
        .run()
        .await?;
    Ok(())
}
