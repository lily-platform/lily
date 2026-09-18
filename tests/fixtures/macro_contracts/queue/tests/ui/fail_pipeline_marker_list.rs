#![allow(unused_imports)]

use queue_runtime::{middleware, queue, queue_service};

struct First;
struct Second;
struct AuditService;

#[queue_service]
#[middleware(First, Second)]
impl AuditService {
    #[queue("audit.pipeline", version = 1, content = "json")]
    async fn handle(&self) -> Result<(), String> {
        Ok(())
    }
}

fn main() {}
