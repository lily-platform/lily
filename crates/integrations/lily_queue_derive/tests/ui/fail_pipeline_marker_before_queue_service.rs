#![allow(unused_imports)]

use queue_runtime::{middleware, queue, queue_service};

struct Audit;
struct AuditService;

#[middleware(Audit)]
#[queue_service]
impl AuditService {
    #[queue("audit.pipeline", version = 1, content = "json")]
    async fn handle(&self) -> Result<(), String> {
        Ok(())
    }
}

fn main() {}
