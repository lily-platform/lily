use lily_queue_derive::queue_service;

struct InvalidGuaranteeConsumer;

#[queue_service]
impl InvalidGuaranteeConsumer {
    #[queue(
        "delivery.invalid",
        version = 1,
        content = "none",
        delivery_guarantee = "exactly_once"
    )]
    async fn handle(&self) -> Result<(), String> {
        Ok(())
    }
}

fn main() {}
