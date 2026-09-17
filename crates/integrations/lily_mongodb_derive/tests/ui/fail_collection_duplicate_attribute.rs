use lily_mongodb_derive::MongoCollection;

#[derive(MongoCollection)]
#[collection("audit")]
#[collection("audit_again")]
#[collection_type(())]
struct AuditCollection {
    db: (),
    collection: (),
}

fn main() {}
