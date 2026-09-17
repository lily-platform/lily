use lily_error::application::QueueHandlerError;
use queue_runtime::{guard, queue, queue_service};

struct NotGuard;
struct AuditService;

#[queue_service]
#[guard(NotGuard)]
impl AuditService {
    #[queue("audit.pipeline", version = 1, content = "json")]
    async fn handle(&self) -> Result<(), QueueHandlerError> {
        Ok(())
    }
}

fn main() {}
