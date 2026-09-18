use lily_mongodb_derive::MongoCollection;

#[derive(MongoCollection)]
#[collection("audit")]
#[collection_type(())]
#[index(field = "$unsafe")]
struct AuditCollection {
    db: (),
    collection: (),
}

fn main() {}
