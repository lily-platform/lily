use queue_runtime::queue_service;

struct AuditService;

#[queue_service]
#[asyncapi(documented)]
impl AuditService {
    #[queue("audit.created", version = 1, content = "json")]
    async fn handle(
        &self,
        _message: queue_runtime::Json<String>,
    ) -> Result<(), lily_error::application::QueueHandlerError> {
        Ok(())
    }
}

fn main() {}
