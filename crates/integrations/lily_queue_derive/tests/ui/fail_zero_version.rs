use lily_queue_derive::queue_service;

struct AuditService;

#[queue_service]
impl AuditService {
    #[queue("audit.created", version = 0, content = "json")]
    async fn handle(&self) -> Result<(), String> {
        Ok(())
    }
}

fn main() {}
