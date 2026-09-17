struct Handler;
#[lily::queue::queue_service]
#[lily::queue::asyncapi(documented)]
impl Handler {
    #[lily::queue::queue("events", version = 1, content = "text")]
    async fn handle(
        &self,
        _text: lily::queue::TextPayload,
    ) -> Result<(), lily::queue::QueueHandlerError> {
        Ok(())
    }
}
fn main() {}
