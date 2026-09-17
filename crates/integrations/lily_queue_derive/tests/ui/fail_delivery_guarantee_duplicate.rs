use lily_queue_derive::queue_service;

struct DuplicateGuaranteeConsumer;

#[queue_service]
impl DuplicateGuaranteeConsumer {
    #[queue(
        "delivery.duplicate",
        version = 1,
        content = "none",
        delivery_guarantee = "at_least_once",
        delivery_guarantee = "transactional_inbox"
    )]
    async fn handle(&self) -> Result<(), String> {
        Ok(())
    }
}

fn main() {}
