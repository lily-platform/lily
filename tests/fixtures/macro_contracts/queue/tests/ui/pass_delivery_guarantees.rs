use lily_error::application::QueueHandlerError;
use queue_runtime::{queue, queue_service, DeliveryGuarantee};

struct DeliveryGuaranteeConsumer;

#[queue_service]
impl DeliveryGuaranteeConsumer {
    #[queue("delivery.default", version = 1, content = "none")]
    async fn default_at_least_once(&self) -> Result<(), QueueHandlerError> {
        Ok(())
    }

    #[queue(
        "delivery.transactional",
        version = 1,
        content = "none",
        delivery_guarantee = "transactional_inbox"
    )]
    async fn transactional_inbox(&self) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

fn main() {
    let handlers = queue_runtime::__private::get_all_queue_handlers();

    let default = handlers
        .iter()
        .find(|handler| handler.queue_name == "delivery.default")
        .expect("default handler metadata must be linked");
    assert_eq!(default.delivery_guarantee, DeliveryGuarantee::AtLeastOnce);

    let transactional = handlers
        .iter()
        .find(|handler| handler.queue_name == "delivery.transactional")
        .expect("transactional handler metadata must be linked");
    assert_eq!(
        transactional.delivery_guarantee,
        DeliveryGuarantee::TransactionalInbox
    );
}
