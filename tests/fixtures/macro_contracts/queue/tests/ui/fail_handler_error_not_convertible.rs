use queue_runtime::{queue, queue_service};

struct AuditService;

#[queue_service]
impl AuditService {
    #[queue("audit.created", version = 1, content = "json")]
    async fn handle(&self) -> Result<(), String> {
        Err("application error".into())
    }
}

fn main() {}
