use lily_error::application::QueueHandlerError;
use queue_runtime::{queue, queue_service, BinaryPayload, Json};

struct VersionedOrderConsumer;

#[queue_service]
impl VersionedOrderConsumer {
    #[queue("orders.created", version = 1, content = "json")]
    async fn created_v1(&self, _message: Json<String>) -> Result<(), QueueHandlerError> {
        Ok(())
    }

    #[queue("orders.created", version = 2, content = "json")]
    async fn created_v2(&self, _message: Json<String>) -> Result<(), QueueHandlerError> {
        Ok(())
    }

    #[queue("orders.created", version = 65535, content = "binary")]
    async fn created_max(&self, _message: BinaryPayload) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

fn main() {
    let handlers = queue_runtime::__private::get_all_queue_handlers();

    assert!(handlers.iter().any(|handler| {
        let key = handler.dispatch_key();
        key.queue_name() == "orders.created"
            && key.schema_version() == 1
            && key.content_kind() == "json"
    }));
    assert!(handlers.iter().any(|handler| {
        let key = handler.dispatch_key();
        key.queue_name() == "orders.created"
            && key.schema_version() == 2
            && key.content_kind() == "json"
    }));
    assert!(handlers.iter().any(|handler| {
        let key = handler.dispatch_key();
        key.queue_name() == "orders.created"
            && key.schema_version() == u16::MAX
            && key.content_kind() == "binary"
    }));
}
