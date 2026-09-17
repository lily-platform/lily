#![allow(unused_imports)]

use queue_runtime::{middleware, queue_service};

struct Audit;
struct AuditService;

#[queue_service]
impl AuditService {
    #[middleware(Audit)]
    async fn helper(&self) -> Result<(), String> {
        Ok(())
    }
}

fn main() {}
