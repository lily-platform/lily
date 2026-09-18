use lily_queue_derive::queue_service;

struct AuditService;

#[queue_service]
impl AuditService {
    #[queue("audit.created", version = 1, content = "json", unknown = true)]
    async fn handle(&self, _message: String) -> Result<(), String> {
        Ok(())
    }
}

fn main() {}
