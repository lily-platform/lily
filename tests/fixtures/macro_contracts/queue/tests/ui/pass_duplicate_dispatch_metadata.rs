use lily_error::application::QueueHandlerError;
use queue_runtime::{queue, queue_service, Json};

struct FirstOrderConsumer;
struct SecondOrderConsumer;

#[queue_service]
impl FirstOrderConsumer {
    #[queue("orders.created", version = 2, content = "json")]
    async fn created(&self, _message: Json<String>) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

#[queue_service]
impl SecondOrderConsumer {
    #[queue("orders.created", version = 2, content = "json")]
    async fn created(&self, _message: Json<String>) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

fn main() {
    let duplicate_count = queue_runtime::__private::get_all_queue_handlers()
        .into_iter()
        .filter(|handler| {
            let key = handler.dispatch_key();
            key.queue_name() == "orders.created"
                && key.schema_version() == 2
                && key.content_kind() == "json"
        })
        .count();

    // The link-time ABI must preserve every record. The consumer's global
    // startup plan is the sole authority that rejects this exact collision.
    assert_eq!(duplicate_count, 2);
}
