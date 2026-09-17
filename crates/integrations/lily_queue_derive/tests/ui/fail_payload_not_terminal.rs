use lily_error::application::QueueHandlerError;
use queue_runtime::{queue, queue_service, DeliveryContext, Json};

struct AuditService;

#[queue_service]
impl AuditService {
    #[queue("audit.created", version = 1, content = "json")]
    async fn handle(
        &self,
        _payload: Json<String>,
        _context: DeliveryContext,
    ) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

fn main() {}
