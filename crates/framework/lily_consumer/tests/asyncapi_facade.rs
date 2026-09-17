#![cfg(feature = "asyncapi")]

use lily_consumer::schemars::{self, JsonSchema};
use lily_queue::{asyncapi, queue, queue_service, Json, QueueHandlerError};
use serde::Deserialize;

#[allow(dead_code)]
#[derive(Deserialize, JsonSchema)]
struct FacadeEvent {
    event_id: String,
}

struct FacadeWorker;

#[queue_service]
#[asyncapi(tag = "facade")]
impl FacadeWorker {
    #[queue("asyncapi.facade", version = 1, content = "json")]
    #[asyncapi(summary = "Compile through the Consumer schema facade")]
    async fn consume(&self, Json(_event): Json<FacadeEvent>) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

#[test]
fn consumer_facade_supplies_the_exact_schema_trait_and_derive_runtime() {
    fn assert_schema<T: lily_consumer::schemars::JsonSchema>() {}
    assert_schema::<FacadeEvent>();
}
