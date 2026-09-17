use lily_error::application::QueueHandlerError;
use queue_runtime::{middleware, queue, queue_service};

struct NotMiddleware;
struct AuditService;

#[queue_service]
#[middleware(NotMiddleware)]
impl AuditService {
    #[queue("audit.pipeline", version = 1, content = "json")]
    async fn handle(&self) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

fn main() {}
