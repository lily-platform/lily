use lily_mongodb_derive::MongoCollection;

#[derive(MongoCollection)]
struct AuditCollection {
    db: (),
    collection: (),
}

fn main() {}
