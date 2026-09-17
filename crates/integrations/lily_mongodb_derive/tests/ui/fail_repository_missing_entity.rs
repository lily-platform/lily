use lily_mongodb_derive::Repository;
use std::sync::Arc;

struct AuditCollection;

#[derive(Repository)]
#[collection_type(AuditCollection)]
struct AuditRepository {
    collection: Arc<AuditCollection>,
}

fn main() {}
