struct Handler;
#[lilyrs::queue::queue_service]
#[lilyrs::queue::asyncapi(documented)]
impl Handler {
    #[lilyrs::queue::queue("events", version = 1, content = "text")]
    async fn handle(
        &self,
        _text: lilyrs::queue::TextPayload,
    ) -> Result<(), lilyrs::queue::QueueHandlerError> {
        Ok(())
    }
}
fn main() {}
