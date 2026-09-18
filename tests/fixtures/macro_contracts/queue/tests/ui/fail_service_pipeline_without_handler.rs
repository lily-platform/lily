#![allow(unused_imports)]

use queue_runtime::{middleware, queue_service};

struct Audit;
struct AuditService;

#[queue_service]
#[middleware(Audit)]
impl AuditService {
    async fn helper(&self) {}
}

fn main() {}
